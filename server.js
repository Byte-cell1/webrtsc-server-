const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const rooms = {};
const clients = new Map();
const groups = {};

wss.on('connection', (ws) => {
  ws.id = Math.random().toString(36).substring(2, 9);

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'join') {
        ws.role = data.role;
        ws.name = data.name || 'Guest';
        ws.roomId = data.roomId || 'public';

        if (!rooms[ws.roomId]) rooms[ws.roomId] = { host: null, users: {}, monitors: [] };

        if (ws.role === 'host') rooms[ws.roomId].host = ws;
        if (ws.role === 'user') rooms[ws.roomId].users[ws.id] = ws;
        if (ws.role === 'monitor') rooms[ws.roomId].monitors.push(ws);

        clients.set(ws.id, ws);

        if (ws.role === 'user' && rooms[ws.roomId].host) {
          rooms[ws.roomId].host.send(JSON.stringify({
            type: 'user-joined',
            userId: ws.id,
            name: ws.name
          }));
        }

        rooms[ws.roomId].monitors.forEach(m => {
          m.send(JSON.stringify({
            type: 'user-joined',
            userId: ws.id,
            name: ws.name
          }));
        });
      }

      if (data.type === 'offer' || data.type === 'answer' || data.type === 'ice') {
        const target = clients.get(data.target);
        if (target) target.send(JSON.stringify({...data, from: ws.id }));
      }

      if (data.type === 'approve' || data.type === 'reject') {
        const target = clients.get(data.target);
        if (target) target.send(JSON.stringify({ type: data.type }));
      }

      if (data.type === 'chat') {
        if (data.target && data.target.startsWith('g_')) {
          const group = groups[data.target];
          group?.members.forEach(uid => {
            const client = clients.get(uid);
            if (client) client.send(JSON.stringify({ type: 'chat', from: ws.id, msg: data.msg }));
          });
        } else if (data.target) {
          const target = clients.get(data.target);
          if (target) target.send(JSON.stringify({ type: 'chat', from: ws.id, msg: data.msg }));
        } else {
          const room = rooms[ws.roomId];
          if (room.host) room.host.send(JSON.stringify({ type: 'chat', from: ws.id, msg: data.msg }));
        }
      }

      if (data.type === 'file') {
        if (data.target && data.target.startsWith('g_')) {
          const group = groups[data.target];
          group?.members.forEach(uid => {
            const client = clients.get(uid);
            if (client) client.send(JSON.stringify({...data, from: ws.id }));
          });
        } else {
          const target = clients.get(data.target);
          if (target) target.send(JSON.stringify({...data, from: ws.id }));
        }
      }

    } catch (e) {
      console.log('Error:', e.message);
    }
  });

  ws.on('close', () => {
    clients.delete(ws.id);
    if (ws.roomId && rooms[ws.roomId]) {
      delete rooms[ws.roomId].users[ws.id];
      if (ws.role === 'host') rooms[ws.roomId].host = null;
      if (ws.role === 'monitor') {
        rooms[ws.roomId].monitors = rooms[ws.roomId].monitors.filter(m => m.id!== ws.id);
      }
      const room = rooms[ws.roomId];
      if (room.host) room.host.send(JSON.stringify({ type: 'user-left', userId: ws.id }));
      room.monitors.forEach(m => m.send(JSON.stringify({ type: 'user-left', userId: ws.id })));
    }
  });
});

app.get('/', (req, res) => res.send('WebRTC Server Running'));

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => console.log('Server on port', PORT));