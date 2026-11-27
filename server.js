'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT = path.resolve(__dirname);
const MAX_PLAYERS = 12;
const MAX_MESSAGE_BYTES = 16 * 1024;
const STATE_INTERVAL_MS = 50;
const ROOM_CODE_LENGTH = 5;
const rooms = new Map();
const clients = new Map();

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch (_) {}
  }
}

function safeName(value) {
  return String(value || 'Jogador').replace(/[<>]/g, '').trim().slice(0, 16) || 'Jogador';
}

function randomSeed() {
  return crypto.randomBytes(4).readUInt32BE(0) || 1;
}

function roomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do {
    const bytes = crypto.randomBytes(ROOM_CODE_LENGTH);
    code = Array.from(bytes, b => chars[b % chars.length]).join('');
  } while (rooms.has(code));
  return code;
}

function cleanSkin(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const keys = ['shirt', 'hair', 'acc', 'pants', 'tone', 'sty', 'gender', 'tr'];
  const skin = {};
  for (const key of keys) {
    const n = Number(value[key]);
    skin[key] = Number.isFinite(n) ? Math.max(0, Math.min(20, Math.trunc(n))) : 0;
  }
  return skin;
}

function publicPlayer(p) {
  return {
    id: p.id,
    name: p.name,
    skin: p.skin,
    x: p.x,
    y: p.y,
    z: p.z,
    yaw: p.yaw
  };
}

function roomSnapshot(room) {
  return {
    code: room.code,
    seed: room.mapSeed,
    map: room.map,
    quality: room.quality,
    world: room.world,
    started: room.started
  };
}

function broadcast(room, data, except = null) {
  for (const p of room.players.values()) {
    if (p.ws !== except) send(p.ws, data);
  }
}

function validNumber(value, min, max, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
}

function cleanWorld(m, room) {
  // O servidor conserva uma configuração pequena e consistente para a sala.
  // A geração visual precisa usar a mesma seed no cliente.
  const seed = Number(m.seed) >>> 0;
  const map = Number(m.map) ? 1 : 0;
  const quality = Math.max(0, Math.min(2, Math.trunc(Number(m.quality) || 0)));
  const houses = Array.isArray(m.houses)
    ? m.houses.slice(0, 100)
        .filter(item => Array.isArray(item) && item.length >= 2 &&
          Number.isFinite(Number(item[0])) && Number.isFinite(Number(item[1])))
        .map(item => [
          validNumber(item[0], -120, 120, 0),
          validNumber(item[1], -120, 120, 0)
        ])
    : [];
  return {
    seed: seed || room.mapSeed || randomSeed(),
    map,
    quality,
    houses
  };
}

function leave(ws) {
  const p = clients.get(ws);
  if (!p) return;
  clients.delete(ws);

  const room = rooms.get(p.room);
  if (!room) return;

  room.players.delete(p.id);
  broadcast(room, { type: 'playerLeft', id: p.id });

  if (room.players.size === 0) {
    rooms.delete(room.code);
    return;
  }

  if (room.hostId === p.id) {
    const next = room.players.values().next().value;
    room.hostId = next.id;
    send(next.ws, { type: 'hostChanged' });
  }
}

function createRoom() {
  const code = roomCode();
  const seed = randomSeed();
  const room = {
    code,
    hostId: null,
    players: new Map(),
    started: false,
    mapSeed: seed,
    map: 0,
    quality: 2,
    houses: [],
    world: { seed, map: 0, quality: 2, houses: [] },
    createdAt: Date.now()
  };
  rooms.set(code, room);
  return room;
}

function joinRoom(ws, message) {
  if (clients.has(ws)) {
    send(ws, { type: 'error', message: 'Você já está em uma sala.' });
    return;
  }

  let room;
  if (message.type === 'create') {
    room = createRoom();
  } else {
    const code = String(message.room || '')
      .toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, ROOM_CODE_LENGTH);
    room = rooms.get(code);
    if (!room) {
      send(ws, { type: 'error', message: 'Sala não encontrada. Confira o código.' });
      return;
    }
  }

  if (room.players.size >= MAX_PLAYERS) {
    if (message.type === 'create') rooms.delete(room.code);
    send(ws, { type: 'error', message: 'Sala cheia (máximo de 12 jogadores).' });
    return;
  }

  if (room.started) {
    send(ws, { type: 'error', message: 'A partida já começou. Entre em outra sala.' });
    return;
  }

  const id = crypto.randomBytes(8).toString('hex');
  const player = {
    id,
    ws,
    room: room.code,
    name: safeName(message.name),
    skin: null,
    x: 0,
    y: 0,
    z: 0,
    yaw: 0,
    lastState: 0
  };

  room.players.set(id, player);
  if (!room.hostId) room.hostId = id;
  clients.set(ws, player);

  send(ws, {
    type: 'joined',
    id,
    room: room.code,
    host: room.hostId === id,
    players: [...room.players.values()]
      .filter(other => other.id !== id)
      .map(publicPlayer),
    started: room.started,
    world: roomSnapshot(room)
  });

  broadcast(room, { type: 'playerJoined', player: publicPlayer(player) }, ws);
}

function handleState(ws, player, room, message) {
  const now = Date.now();
  if (now - player.lastState < STATE_INTERVAL_MS) return;
  player.lastState = now;

  player.x = validNumber(message.x, -120, 120, player.x);
  player.y = validNumber(message.y, 0, 220, player.y);
  player.z = validNumber(message.z, -120, 120, player.z);
  player.yaw = validNumber(message.yaw, -Math.PI * 100, Math.PI * 100, player.yaw);
  player.name = safeName(message.name || player.name);

  if (message.skin) player.skin = cleanSkin(message.skin);

  broadcast(room, { type: 'state', ...publicPlayer(player) }, ws);
}

function handleStart(ws, player, room, message) {
  if (room.hostId !== player.id) {
    send(ws, { type: 'error', message: 'Somente o anfitrião pode iniciar.' });
    return;
  }
  if (room.started) {
    send(ws, { type: 'error', message: 'A partida já foi iniciada.' });
    return;
  }

  const world = cleanWorld(message, room);
  room.mapSeed = world.seed;
  room.map = world.map;
  room.quality = world.quality;
  room.houses = world.houses;
  room.world = world;
  room.started = true;

  // Todos recebem exatamente a mesma configuração de mundo.
  broadcast(room, {
    type: 'start',
    seed: room.mapSeed,
    map: room.map,
    quality: room.quality,
    houses: room.houses,
    world: room.world
  });
}

function handleMessage(ws, raw) {
  let message;
  try {
    message = JSON.parse(raw.toString());
  } catch (_) {
    send(ws, { type: 'error', message: 'Mensagem inválida.' });
    return;
  }

  if (!message || typeof message.type !== 'string') return;

  if (message.type === 'create' || message.type === 'join') {
    joinRoom(ws, message);
    return;
  }

  const player = clients.get(ws);
  if (!player) {
    send(ws, { type: 'error', message: 'Crie ou entre em uma sala primeiro.' });
    return;
  }

  const room = rooms.get(player.room);
  if (!room) {
    leave(ws);
    return;
  }

  if (message.type === 'state') {
    handleState(ws, player, room, message);
  } else if (message.type === 'start') {
    handleStart(ws, player, room, message);
  } else if (message.type === 'ping') {
    send(ws, { type: 'pong', at: Date.now() });
  } else if (message.type === 'leave') {
    leave(ws);
    try { ws.close(1000, 'Saiu da sala'); } catch (_) {}
  }
}

const server = http.createServer((req, res) => {
  let pathname;
  try {
    pathname = decodeURIComponent(new URL(req.url, `http://${req.headers.host || 'localhost'}`).pathname);
  } catch (_) {
    res.writeHead(400);
    res.end('Bad Request');
    return;
  }

  const requested = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const fullPath = path.resolve(ROOT, requested);

  if (fullPath !== ROOT && !fullPath.startsWith(ROOT + path.sep)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.stat(fullPath, (statError, stat) => {
    if (statError || !stat.isFile()) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }

    fs.readFile(fullPath, (readError, data) => {
      if (readError) {
        res.writeHead(500);
        res.end('Could not read file');
        return;
      }

      const ext = path.extname(fullPath).toLowerCase();
      const types = {
        '.html': 'text/html; charset=utf-8',
        '.js': 'text/javascript; charset=utf-8',
        '.css': 'text/css; charset=utf-8',
        '.json': 'application/json; charset=utf-8',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.webp': 'image/webp',
        '.svg': 'image/svg+xml'
      };

      res.writeHead(200, {
        'Content-Type': types[ext] || 'application/octet-stream',
        'Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff'
      });
      res.end(data);
    });
  });
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_MESSAGE_BYTES,
  perMessageDeflate: false
});

server.on('upgrade', (req, socket, head) => {
  let pathname;
  try {
    pathname = new URL(req.url, 'http://localhost').pathname;
  } catch (_) {
    socket.destroy();
    return;
  }

  if (pathname !== '/ws') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', ws => {
  ws.on('message', raw => handleMessage(ws, raw));
  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

server.listen(PORT, HOST, () => {
  console.log(`FIRE BLOCK Online rodando em http://${HOST}:${PORT}`);
});

function shutdown() {
  for (const ws of wss.clients) {
    try { ws.close(1001, 'Servidor encerrando'); } catch (_) {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
