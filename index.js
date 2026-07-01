// const WebSocket = require('ws');
// const http = require('http');

// const PORT = process.env.PORT || 8080;

// const server = http.createServer();

// const wss = new WebSocket.Server({ server });

// const rooms = new Map();

// function send(ws, data) {
//   if (ws.readyState === WebSocket.OPEN) {
//     ws.send(JSON.stringify(data));
//   }
// }

// wss.on('connection', (ws, req) => {
//   console.log('Client Connected');
//   console.log(req.url);

//   ws.roomId = null;
//   ws.userId = null;

//   ws.on('message', (message) => {
//     const msg = JSON.parse(message);

//     switch (msg.type) {
//       case 'join':
//         ws.roomId = msg.roomId;
//         ws.userId = msg.userId;
//         ws.callType = msg.callType || 'video';

//         if (!rooms.has(msg.roomId)) {
//           rooms.set(msg.roomId, new Set());
//         }

//         const peers = rooms.get(msg.roomId);

//         peers.forEach((peer) => {
//           send(peer, {
//             type: 'peer-joined',
//             userId: ws.userId,
//             callType: ws.callType,
//           });
//         });

//         send(ws, {
//           type: 'joined',
//           peerCount: peers.size,
//         });

//         peers.add(ws);

//         console.log(ws.userId + ' joined ' + ws.roomId + ' as ' + ws.callType);

//         break;

//       case 'offer':
//       case 'answer':
//       case 'candidate':
//       case 'hangup':
//       case 'camera-state':
//         const room = rooms.get(ws.roomId);

//         if (!room) return;

//         room.forEach((peer) => {
//           if (peer !== ws) {
//             send(peer, msg);
//           }
//         });

//         break;
//     }
//   });

//   ws.on('close', () => {
//     const room = rooms.get(ws.roomId);

//     if (room) {
//       room.delete(ws);

//       room.forEach((peer) => {
//         send(peer, {
//           type: 'peer-left',
//           userId: ws.userId,
//         });
//       });

//       if (room.size === 0) {
//         rooms.delete(ws.roomId);
//       }
//     }

//     console.log('Client disconnected');
//   });
// });

// server.listen(PORT, () => {
//   console.log('Server Running on Port ' + PORT);
// });

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
    let msg;
    try {
      msg = JSON.parse(message);
    } catch (e) {
      console.error('Bad message', e);
      return;
    }

    switch (msg.type) {
      case 'join': {
        ws.roomId = msg.roomId;
        ws.userId = msg.userId;
        ws.callType = msg.callType || 'video';

        if (!rooms.has(msg.roomId)) {
          rooms.set(msg.roomId, new Set());
        }

        const peers = rooms.get(msg.roomId);
        const existingPeerIds = Array.from(peers).map((p) => p.userId);

        // Let everyone already in the room know a new user showed up
        peers.forEach((peer) => {
          send(peer, {
            type: 'peer-joined',
            userId: ws.userId,
            callType: ws.callType,
          });
        });

        // Tell the new user who's already here, so THEY can offer to each one.
        // (This is what makes mesh group calls work - the joiner always initiates.)
        send(ws, {
          type: 'joined',
          peers: existingPeerIds,
        });

        peers.add(ws);

        console.log(ws.userId + ' joined ' + ws.roomId + ' as ' + ws.callType);
        break;
      }

      // Pairwise signaling - must be routed to one specific peer, not broadcast,
      // otherwise in a 3+ person room every peer connection gets confused
      // about whose offer/answer/candidate it's looking at.
      case 'offer':
      case 'answer':
      case 'candidate': {
        const room = rooms.get(ws.roomId);
        if (!room) return;

        const targetPeer = Array.from(room).find(
          (p) => p.userId === msg.target
        );
        if (targetPeer) {
          send(targetPeer, { ...msg, from: ws.userId });
        }
        break;
      }

      // Room-wide notifications
      case 'hangup':
      case 'camera-state':
      case 'mic-state': {
        const room = rooms.get(ws.roomId);
        if (!room) return;

        room.forEach((peer) => {
          if (peer !== ws) {
            send(peer, { ...msg, from: ws.userId });
          }
        });
        break;
      }
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
