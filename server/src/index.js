// Triad Summons online server (Cloudflare Worker + Durable Objects).
//   /room/<CODE>  a two-seat relay: whatever one player sends goes to the other. The game rules run on the phones.
//   /queue        quick match: players send {t:"find", rating, name}; two are paired and told a private room code.
// Nothing is stored: rooms and the queue live only while players are connected.
const MAX_MSG = 32768;

export default {
  async fetch(req, env) {
    const url = new URL(req.url), parts = url.pathname.split("/").filter(Boolean);
    if (parts[0] === "room" && /^[A-Z0-9]{4,8}$/.test(parts[1] || "")) return env.ROOM.get(env.ROOM.idFromName(parts[1])).fetch(req);
    if (parts[0] === "queue") return env.LOBBY.get(env.LOBBY.idFromName("main")).fetch(req);
    return new Response("triad server ok", { headers: { "content-type": "text/plain", "access-control-allow-origin": "*" } });
  },
};

const upgrade = (req) => {
  if (req.headers.get("Upgrade") !== "websocket") return null;
  const pair = new WebSocketPair(); return pair;
};

export class Room {
  constructor(state) { this.state = state; }
  async fetch(req) {
    const pair = upgrade(req); if (!pair) return new Response("expected websocket", { status: 426 });
    const [client, server] = Object.values(pair), live = this.state.getWebSockets();
    this.state.acceptWebSocket(server);
    if (live.length >= 2) { server.send(JSON.stringify({ t: "full" })); server.close(1008, "full"); return new Response(null, { status: 101, webSocket: client }); }
    const seat = live.length === 0 ? 0 : (live[0].deserializeAttachment()?.seat === 0 ? 1 : 0);
    server.serializeAttachment({ seat }); server.send(JSON.stringify({ t: "hello", seat }));
    if (live.length === 1) { const msg = JSON.stringify({ t: "peer" }); live[0].send(msg); server.send(msg); }
    return new Response(null, { status: 101, webSocket: client });
  }
  webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > MAX_MSG) return;
    for (const o of this.state.getWebSockets()) if (o !== ws) { try { o.send(msg); } catch (_) {} }
  }
  webSocketClose(ws) {
    if (ws.deserializeAttachment()?.seat === undefined) return;   // a rejected third visitor leaving is not news
    for (const o of this.state.getWebSockets()) if (o !== ws) { try { o.send(JSON.stringify({ t: "left" })); } catch (_) {} }
    try { ws.close(); } catch (_) {}
  }
  webSocketError(ws) { this.webSocketClose(ws); }
}

export class Lobby {
  constructor() { this.waiting = []; }
  async fetch(req) {
    const pair = upgrade(req); if (!pair) return new Response("expected websocket", { status: 426 });
    const [client, server] = Object.values(pair); server.accept();
    let me = null;
    server.addEventListener("message", (e) => {
      let m; try { m = JSON.parse(e.data); } catch (_) { return; }
      if (m.t !== "find" || me) return;
      me = { ws: server, rating: Math.max(0, Math.min(9999, +m.rating || 800)), at: Date.now() };
      let best = -1, gap = 1e9;
      this.waiting.forEach((w, i) => { const d = Math.abs(w.rating - me.rating); if (d < gap) { gap = d; best = i; } });
      if (best >= 0) {
        const other = this.waiting.splice(best, 1)[0], code = Array.from({ length: 6 }, () => "ABCDEFGHJKMNPQRSTUVWXYZ23456789"[Math.floor(Math.random() * 31)]).join("");
        for (const p of [other, me]) { try { p.ws.send(JSON.stringify({ t: "match", room: code })); p.ws.close(1000, "matched"); } catch (_) {} }
        me = null;
      } else { this.waiting.push(me); server.send(JSON.stringify({ t: "wait" })); }
    });
    const drop = () => { this.waiting = this.waiting.filter((w) => w.ws !== server); };
    server.addEventListener("close", drop); server.addEventListener("error", drop);
    return new Response(null, { status: 101, webSocket: client });
  }
}
