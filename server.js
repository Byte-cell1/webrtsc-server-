const WebSocket = require('ws');
const http = require('http');
const url = require('url');

const server = http.createServer((req, res) => {
  res.writeHead(200, {'Content-Type': 'text/plain'});
  res.end('WebRTC Signaling Server Running');
});

const wss = new WebSocket.Server({ server });
const rooms = new Map(); // roomId -> {host, monitor, users: Map}

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {host: null, monitor: null, users: new Map()});
  }
  return rooms.get(roomId);
}

wss.on('connection', (ws, req) => {
  const params = new URLSearchParams(url.parse(req.url, true).query);
  ws.roomId = params.get('room') || 'default';
  ws.role = null;
  ws.id = null;
  ws.isAlive = true;

  ws.on('pong', () => ws.isAlive = true);

  ws.on('message', raw => {
    try {
      const data = JSON.parse(raw);
      const room = getRoom(ws.roomId);

      if (data.type === 'register') {
        ws.role = data.role;
        ws.id = data.id || Math.random().toString(36).substr(2,9);

        if (data.role === 'host') {
          room.host = ws;
          log(`Host joined room ${ws.roomId}`);
        } else if (data.role === 'monitor') {
          room.monitor = ws;
          log(`Monitor joined room ${ws.roomId}`);
        } else if (data.role === 'user') {
          room.users.set(ws.id, ws);
          log(`User ${ws.id} joined room ${ws.roomId}`);
          // Notify host + monitor
          broadcast(room, {type:'user-joined', userId:ws.id}, [ws]);
        }
      }

      // Route signaling
      const target = data.to;
      if (target === 'host' && room.host && room.host.readyState === WebSocket.OPEN) {
        room.host.send(raw);
      } else if (target === 'monitor' && room.monitor && room.monitor.readyState === WebSocket.OPEN) {
        room.monitor.send(raw);
      } else if (target === 'all') {
        if (room.host) room.host.send(raw);
        if (room.monitor) room.monitor.send(raw);
      } else if (room.users.has(target)) {
        room.users.get(target).send(raw);
      }
    } catch (e) {
      log(`Error parsing message: ${e.message}`);
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.roomId);
    if (!room) return;

    if (ws.role === 'host') {
      room.host = null;
      broadcast(room, {type:'host-left'});
      log(`Host left room ${ws.roomId}`);
    } else if (ws.role === 'monitor') {
      room.monitor = null;
      log(`Monitor left room ${ws.roomId}`);
    } else if (ws.role === 'user') {
      room.users.delete(ws.id);
      broadcast(room, {type:'user-left', userId:ws.id});
      log(`User ${ws.id} left room ${ws.roomId}`);
    }

    if (!room.host &&!room.monitor && room.users.size === 0) {
      rooms.delete(ws.roomId);
    }
  });

  ws.on('error', err => log(`WS Error: ${err.message}`));
});

// Heartbeat to kill dead connections
const interval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

function broadcast(room, msg, exclude = []) {
  const str = JSON.stringify(msg);
  if (room.host &&!exclude.includes(room.host)) room.host.send(str);
  if (room.monitor &&!exclude.includes(room.monitor)) room.monitor.send(str);
  room.users.forEach(u => { if (!exclude.includes(u)) u.send(str); });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => log(`Signaling server running on port ${PORT}`));