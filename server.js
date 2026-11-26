const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT = __dirname;
const rooms = new Map();
const clients = new Map();
const MAX_PLAYERS = 12;

function send(ws, data) {
  if (ws.readyState === 1) ws.send(JSON.stringify(data));
}
function roomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do { code = Array.from({ length: 5 }, () => chars[Math.floor(Math.random() * chars.length)]).join(''); }
  while (rooms.has(code));
  return code;
}
function publicPlayer(p) {
  return { id: p.id, name: p.name, skin: p.skin, x: p.x, y: p.y, z: p.z, yaw: p.yaw };
}
function broadcast(room, data, except) {
  for (const p of room.players.values()) if (p.ws !== except) send(p.ws, data);
}
function leave(ws) {
  const p = clients.get(ws);
  if (!p) return;
  clients.delete(ws);
  const room = rooms.get(p.room);
  if (!room) return;
  room.players.delete(p.id);
  broadcast(room, { type: 'playerLeft', id: p.id });
  if (!room.players.size) rooms.delete(p.room);
  else if (room.hostId === p.id) {
    const next = room.players.values().next().value;
    room.hostId = next.id;
    send(next.ws, { type: 'hostChanged' });
  }
}

const server = http.createServer((req, res) => {
  const pathname = decodeURIComponent(new URL(req.url, `http://${req.headers.host || 'localhost'}`).pathname);
  let file = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const full = path.resolve(ROOT, file);
  if (!full.startsWith(ROOT + path.sep) && full !== path.join(ROOT, 'index.html')) {
    res.writeHead(403); return res.end('Forbidden');
  }
  fs.readFile(full, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(full).toLowerCase();
    const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8' };
    res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream', 'Cache-Control': 'no-store' });
    res.end(data);
  });
});
const wss = new WebSocketServer({ noServer: true, maxPayload: 4096 });
server.on('upgrade', (req, socket, head) => {
  if (new URL(req.url, 'http://localhost').pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

wss.on('connection', ws => {
  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', message: 'Mensagem inválida.' }); }
    if (!m || typeof m.type !== 'string') return;

    if (m.type === 'create' || m.type === 'join') {
      if (clients.has(ws)) return send(ws, { type: 'error', message: 'Você já está em uma sala.' });
      let room;
      if (m.type === 'create') {
        const code = roomCode();
        room = { code, hostId: null, players: new Map(), started: false };
        rooms.set(code, room);
      } else {
        const code = String(m.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 5);
        room = rooms.get(code);
        if (!room) return send(ws, { type: 'error', message: 'Sala não encontrada. Confira o código.' });
      }
      if (room.players.size >= MAX_PLAYERS) return send(ws, { type: 'error', message: 'Sala cheia (máximo de 12 jogadores).' });
      const id = Math.random().toString(36).slice(2, 11);
      const p = { id, ws, room: room.code, name: String(m.name || 'Jogador').slice(0, 16), skin: null, x: 0, y: 0, z: 0, yaw: 0, lastState: 0 };
      room.players.set(id, p);
      if (!room.hostId) room.hostId = id;
      clients.set(ws, p);
      send(ws, { type: 'joined', id, room: room.code, host: room.hostId === id, players: [...room.players.values()].filter(q => q.id !== id).map(publicPlayer), started: room.started });
      broadcast(room, { type: 'playerJoined', player: publicPlayer(p) }, ws);
      if (room.started) send(ws, { type: 'start' });
      return;
    }

    const p = clients.get(ws);
    if (!p) return send(ws, { type: 'error', message: 'Crie ou entre em uma sala primeiro.' });
    const room = rooms.get(p.room);
    if (!room) return leave(ws);

    if (m.type === 'state') {
      const now = Date.now();
      if (now - p.lastState < 45) return;
      p.lastState = now;
      p.x = Number.isFinite(+m.x) ? Math.max(-120, Math.min(120, +m.x)) : p.x;
      p.y = Number.isFinite(+m.y) ? Math.max(0, Math.min(220, +m.y)) : p.y;
      p.z = Number.isFinite(+m.z) ? Math.max(-120, Math.min(120, +m.z)) : p.z;
      p.yaw = Number.isFinite(+m.yaw) ? +m.yaw : p.yaw;
      p.name = String(m.name || p.name).slice(0, 16);
      if (m.skin && typeof m.skin === 'object') {
        const keys = ['shirt', 'hair', 'acc', 'pants', 'tone', 'sty', 'gender', 'tr'];
        p.skin = {};
        for (const k of keys) p.skin[k] = Math.max(0, Math.min(20, Number(m.skin[k]) || 0));
      }
      broadcast(room, { type: 'state', ...publicPlayer(p) }, ws);
    } else if (m.type === 'start') {
      if (room.hostId !== p.id) return send(ws, { type: 'error', message: 'Somente o anfitrião pode iniciar.' });
      room.started = true;
      broadcast(room, { type: 'start' });
    }
  });
  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

server.listen(PORT, HOST, () => {
  console.log(`FIRE BLOCK Online rodando em http://localhost:${PORT}`);
});
