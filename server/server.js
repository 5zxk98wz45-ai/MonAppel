const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, '..')));

// --- Fichiers de persistance (voir note de fiabilité dans le README) ---
const USERS_FILE = path.join(__dirname, 'users.json');
const SERVERS_FILE = path.join(__dirname, 'servers.json');
const FRIENDS_FILE = path.join(__dirname, 'friends.json');
const DMS_FILE = path.join(__dirname, 'dms.json');

function loadJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
}
function saveJSON(file, data) {
  try { fs.writeFileSync(file, JSON.stringify(data, null, 2)); } catch (e) { console.error('Erreur sauvegarde:', e); }
}
function hashPassword(pw) { return crypto.createHash('sha256').update(pw).digest('hex'); }
function shortId() { return crypto.randomBytes(4).toString('hex'); }
function genInviteCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // sans caractères ambigus
  let code;
  do {
    code = Array.from({ length: 6 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (Object.values(servers).some((s) => s.code === code));
  return code;
}
function dmKey(a, b) { return [a, b].sort().join('|'); }

let users = loadJSON(USERS_FILE, {});
// servers = { serverId: { name, code, members: [pseudo...], channels: { channelId: { name } } } }
let servers = loadJSON(SERVERS_FILE, {});
// friendsData = { pseudo: { friends: [...], incoming: [...], outgoing: [...] } }
let friendsData = loadJSON(FRIENDS_FILE, {});
// dmsData = { "pseudoA|pseudoB": [ { from, msg, ts }, ... ] }
let dmsData = loadJSON(DMS_FILE, {});

function ensureFriendData(pseudo) {
  if (!friendsData[pseudo]) friendsData[pseudo] = { friends: [], incoming: [], outgoing: [] };
}

// socket.id -> { pseudo, serverId, channelId }
const connected = {};
// pseudo -> socket.id (pour retrouver quelqu'un et l'appeler directement)
const pseudoToSocket = {};
// "serverId:channelId" -> Set(socket.id) en appel de groupe
const callRooms = {};

function roomKey(serverId, channelId) { return `${serverId}:${channelId}`; }

function usersInRoom(serverId, channelId) {
  const key = roomKey(serverId, channelId);
  return Object.values(connected)
    .filter((u) => roomKey(u.serverId, u.channelId) === key)
    .map((u) => u.pseudo);
}

function serversForUser(pseudo) {
  return Object.entries(servers)
    .filter(([id, s]) => (s.members || []).includes(pseudo))
    .map(([id, s]) => ({
      id, name: s.name, code: s.code,
      channels: Object.entries(s.channels).map(([cid, c]) => ({ id: cid, name: c.name }))
    }));
}

io.on('connection', (socket) => {

  // --- Comptes ---
  socket.on('register', ({ pseudo, password }, cb) => {
    if (!pseudo || !password) return cb({ ok: false, error: 'Pseudo et mot de passe requis.' });
    if (users[pseudo]) return cb({ ok: false, error: 'Ce pseudo est déjà pris.' });
    users[pseudo] = { password: hashPassword(password) };
    saveJSON(USERS_FILE, users);
    cb({ ok: true });
  });

  socket.on('login', ({ pseudo, password }, cb) => {
    const u = users[pseudo];
    if (!u || u.password !== hashPassword(password)) {
      return cb({ ok: false, error: 'Pseudo ou mot de passe incorrect.' });
    }
    cb({ ok: true });
  });

  // --- Arrivée sur le site (après connexion) ---
  socket.on('join', (pseudo) => {
    connected[socket.id] = { pseudo, serverId: null, channelId: null };
    pseudoToSocket[pseudo] = socket.id;
    ensureFriendData(pseudo);
    // rejoint les "rooms" de mise à jour temps réel de chaque serveur dont il est membre
    Object.entries(servers).forEach(([id, s]) => {
      if ((s.members || []).includes(pseudo)) socket.join(`server:${id}`);
    });
    socket.emit('servers-list', serversForUser(pseudo));
    socket.emit('friends-data', friendsData[pseudo]);
    socket.emit('profile', { avatar: users[pseudo]?.avatar || null });
  });

  // --- Profil : photo de profil ---
  socket.on('update-avatar', (dataUrl, cb) => {
    const u = connected[socket.id];
    if (!u || !users[u.pseudo]) return cb({ ok: false, error: 'Non connecté.' });
    if (dataUrl && dataUrl.length > 300000) return cb({ ok: false, error: 'Image trop grande.' });
    users[u.pseudo].avatar = dataUrl || null;
    saveJSON(USERS_FILE, users);
    cb({ ok: true });
  });

  socket.on('get-avatars', (pseudoList, cb) => {
    const map = {};
    (pseudoList || []).forEach((p) => { map[p] = users[p]?.avatar || null; });
    cb(map);
  });

  // --- Demandes d'ami ---
  socket.on('send-friend-request', (toPseudo, cb) => {
    const u = connected[socket.id];
    if (!u) return cb({ ok: false, error: 'Non connecté.' });
    const from = u.pseudo;
    if (toPseudo === from) return cb({ ok: false, error: "Tu ne peux pas t'ajouter toi-même." });
    if (!users[toPseudo]) return cb({ ok: false, error: "Ce pseudo n'existe pas." });
    ensureFriendData(from); ensureFriendData(toPseudo);
    if (friendsData[from].friends.includes(toPseudo)) return cb({ ok: false, error: 'Vous êtes déjà amis.' });
    if (friendsData[toPseudo].incoming.includes(from)) return cb({ ok: false, error: 'Demande déjà envoyée.' });
    friendsData[toPseudo].incoming.push(from);
    if (!friendsData[from].outgoing.includes(toPseudo)) friendsData[from].outgoing.push(toPseudo);
    saveJSON(FRIENDS_FILE, friendsData);
    const targetSocket = pseudoToSocket[toPseudo];
    if (targetSocket) io.to(targetSocket).emit('friend-request-received', { from });
    cb({ ok: true });
  });

  socket.on('accept-friend-request', (fromPseudo, cb) => {
    const u = connected[socket.id];
    if (!u) return cb({ ok: false });
    const me = u.pseudo;
    ensureFriendData(me); ensureFriendData(fromPseudo);
    friendsData[me].incoming = friendsData[me].incoming.filter((p) => p !== fromPseudo);
    friendsData[fromPseudo].outgoing = friendsData[fromPseudo].outgoing.filter((p) => p !== me);
    if (!friendsData[me].friends.includes(fromPseudo)) friendsData[me].friends.push(fromPseudo);
    if (!friendsData[fromPseudo].friends.includes(me)) friendsData[fromPseudo].friends.push(me);
    saveJSON(FRIENDS_FILE, friendsData);
    const targetSocket = pseudoToSocket[fromPseudo];
    if (targetSocket) io.to(targetSocket).emit('friend-request-accepted', { by: me });
    cb({ ok: true, friends: friendsData[me].friends });
  });

  socket.on('decline-friend-request', (fromPseudo) => {
    const u = connected[socket.id];
    if (!u) return;
    const me = u.pseudo;
    ensureFriendData(me); ensureFriendData(fromPseudo);
    friendsData[me].incoming = friendsData[me].incoming.filter((p) => p !== fromPseudo);
    friendsData[fromPseudo].outgoing = friendsData[fromPseudo].outgoing.filter((p) => p !== me);
    saveJSON(FRIENDS_FILE, friendsData);
  });

  // --- Créer un serveur (génère un code d'invitation) ---
  socket.on('create-server', (name, cb) => {
    const u = connected[socket.id];
    if (!u) return cb({ ok: false, error: 'Non connecté.' });
    if (!name || !name.trim()) return cb({ ok: false, error: 'Nom invalide.' });
    const id = shortId();
    const code = genInviteCode();
    servers[id] = { name: name.trim(), code, members: [u.pseudo], channels: {} };
    saveJSON(SERVERS_FILE, servers);
    socket.join(`server:${id}`);
    socket.emit('servers-list', serversForUser(u.pseudo));
    cb({ ok: true, id, code });
  });

  // --- Rejoindre un serveur avec un code d'invitation ---
  socket.on('join-server-by-code', (code, cb) => {
    const u = connected[socket.id];
    if (!u) return cb({ ok: false, error: 'Non connecté.' });
    const entry = Object.entries(servers).find(([id, s]) => s.code === (code || '').trim().toUpperCase());
    if (!entry) return cb({ ok: false, error: 'Code invalide.' });
    const [id, s] = entry;
    if (!s.members.includes(u.pseudo)) {
      s.members.push(u.pseudo);
      saveJSON(SERVERS_FILE, servers);
    }
    socket.join(`server:${id}`);
    socket.to(`server:${id}`).emit('system-message-server', { serverId: id, text: `${u.pseudo} a rejoint le serveur.` });
    socket.emit('servers-list', serversForUser(u.pseudo));
    cb({ ok: true, id });
  });

  // --- Créer un salon dans un serveur (notifie tous les membres en temps réel) ---
  socket.on('create-channel', ({ serverId, name }, cb) => {
    const s = servers[serverId];
    if (!s) return cb({ ok: false, error: 'Serveur introuvable.' });
    if (!name || !name.trim()) return cb({ ok: false, error: 'Nom invalide.' });
    const id = shortId();
    s.channels[id] = { name: name.trim() };
    saveJSON(SERVERS_FILE, servers);
    io.to(`server:${serverId}`).emit('channel-added', { serverId, channel: { id, name: s.channels[id].name } });
    cb({ ok: true, id });
  });

  // --- Rejoindre un salon précis ---
  socket.on('join-channel', ({ serverId, channelId }) => {
    const u = connected[socket.id];
    if (!u || !servers[serverId] || !servers[serverId].channels[channelId]) return;

    // Quitte l'ancien salon
    if (u.serverId && u.channelId) {
      const oldKey = roomKey(u.serverId, u.channelId);
      socket.leave(oldKey);
      io.to(oldKey).emit('user-list', usersInRoom(u.serverId, u.channelId));
      leaveCallRoom(socket, u.serverId, u.channelId);
    }

    u.serverId = serverId;
    u.channelId = channelId;
    const key = roomKey(serverId, channelId);
    socket.join(key);
    io.to(key).emit('user-list', usersInRoom(serverId, channelId));
    socket.emit('channel-joined', { serverId, channelId });
  });

  // --- Chat texte (scopé au salon courant) ---
  socket.on('chat-message', (msg) => {
    const u = connected[socket.id];
    if (!u || !u.serverId) return;
    io.to(roomKey(u.serverId, u.channelId)).emit('chat-message', { pseudo: u.pseudo, msg });
  });

  // --- Appel de groupe (mesh WebRTC) dans le salon courant ---
  socket.on('join-call', () => {
    const u = connected[socket.id];
    if (!u || !u.serverId) return;
    const key = roomKey(u.serverId, u.channelId);
    if (!callRooms[key]) callRooms[key] = new Set();
    const existingPeers = Array.from(callRooms[key]).map((id) => ({ id, pseudo: connected[id]?.pseudo }));
    callRooms[key].add(socket.id);
    socket.emit('existing-call-peers', existingPeers);
    socket.to(key).emit('call-peer-joined', { id: socket.id, pseudo: u.pseudo });
  });

  socket.on('call-offer', ({ to, offer }) => io.to(to).emit('call-offer', { from: socket.id, offer }));
  socket.on('call-answer', ({ to, answer }) => io.to(to).emit('call-answer', { from: socket.id, answer }));
  socket.on('ice-candidate', ({ to, candidate }) => io.to(to).emit('ice-candidate', { from: socket.id, candidate }));

  socket.on('leave-call', () => {
    const u = connected[socket.id];
    if (u && u.serverId) leaveCallRoom(socket, u.serverId, u.channelId);
  });

  function leaveCallRoom(socket, serverId, channelId) {
    const key = roomKey(serverId, channelId);
    const room = callRooms[key];
    if (room && room.has(socket.id)) {
      room.delete(socket.id);
      socket.to(key).emit('call-peer-left', { id: socket.id });
    }
  }

  // --- Messages privés (DM) entre amis ---
  socket.on('send-dm', ({ toPseudo, msg }, cb) => {
    const u = connected[socket.id];
    if (!u) return cb && cb({ ok: false });
    ensureFriendData(u.pseudo);
    if (!friendsData[u.pseudo].friends.includes(toPseudo)) return cb && cb({ ok: false, error: "Vous n'êtes pas amis." });
    const key = dmKey(u.pseudo, toPseudo);
    if (!dmsData[key]) dmsData[key] = [];
    const entry = { from: u.pseudo, msg, ts: Date.now() };
    dmsData[key].push(entry);
    if (dmsData[key].length > 200) dmsData[key] = dmsData[key].slice(-200);
    saveJSON(DMS_FILE, dmsData);
    const targetId = pseudoToSocket[toPseudo];
    if (targetId) io.to(targetId).emit('dm-message', entry);
    cb && cb({ ok: true });
  });

  socket.on('get-dm-history', (withPseudo, cb) => {
    const u = connected[socket.id];
    if (!u) return cb([]);
    const key = dmKey(u.pseudo, withPseudo);
    cb(dmsData[key] || []);
  });

  // --- Appel direct à un contact (par pseudo), indépendant des salons ---
  socket.on('direct-call-user', ({ toPseudo, offer }, cb) => {
    const targetId = pseudoToSocket[toPseudo];
    const u = connected[socket.id];
    if (!targetId) { if (cb) cb({ ok: false }); return; }
    io.to(targetId).emit('direct-incoming-call', { from: socket.id, offer, pseudo: u?.pseudo });
    if (cb) cb({ ok: true, targetId });
  });
  socket.on('direct-answer-call', ({ to, answer }) => io.to(to).emit('direct-call-answered', { from: socket.id, answer }));
  socket.on('direct-ice-candidate', ({ to, candidate }) => io.to(to).emit('direct-ice-candidate', { from: socket.id, candidate }));
  socket.on('direct-end-call', ({ to }) => io.to(to).emit('direct-call-ended'));

  // --- Vérifier si un pseudo est en ligne (pour les points de statut des contacts) ---
  socket.on('check-online', (pseudoList, cb) => {
    cb(pseudoList.filter((p) => !!pseudoToSocket[p]));
  });

  // --- Déconnexion ---
  socket.on('disconnect', () => {
    const u = connected[socket.id];
    if (u) {
      if (u.serverId) {
        io.to(roomKey(u.serverId, u.channelId)).emit('user-list', usersInRoom(u.serverId, u.channelId).filter(p => p !== u.pseudo));
        leaveCallRoom(socket, u.serverId, u.channelId);
      }
      delete pseudoToSocket[u.pseudo];
      delete connected[socket.id];
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Serveur lancé sur le port ${PORT}`));
