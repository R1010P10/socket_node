const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

const server = http.createServer();

const wss = new WebSocket.Server({ server });

const rooms = new Map();

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

wss.on('connection', (ws, req) => {
  console.log('Client Connected');
  console.log(req.url);

  ws.roomId = null;
  ws.userId = null;

  ws.on('message', (message) => {
    const msg = JSON.parse(message);

    switch (msg.type) {
      case 'join':
        ws.roomId = msg.roomId;
        ws.userId = msg.userId;

        if (!rooms.has(msg.roomId)) {
          rooms.set(msg.roomId, new Set());
        }

        const peers = rooms.get(msg.roomId);

        peers.forEach((peer) => {
          send(peer, {
            type: 'peer-joined',
            userId: ws.userId,
          });
        });

        peers.add(ws);

        send(ws, {
          type: 'joined',
          peerCount: peers.size - 1,
        });

        console.log(ws.userId + ' joined ' + ws.roomId);

        break;

      case 'offer':
      case 'answer':
      case 'candidate':
      case 'hangup':
        const room = rooms.get(ws.roomId);

        if (!room) return;

        room.forEach((peer) => {
          if (peer !== ws) {
            send(peer, msg);
          }
        });

        break;
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.roomId);

    if (room) {
      room.delete(ws);

      room.forEach((peer) => {
        send(peer, {
          type: 'peer-left',
          userId: ws.userId,
        });
      });

      if (room.size === 0) {
        rooms.delete(ws.roomId);
      }
    }

    console.log('Client disconnected');
  });
});

server.listen(PORT, () => {
  console.log('Server Running on Port ' + PORT);
});
