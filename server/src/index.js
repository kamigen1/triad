// Triad Summons online server (Cloudflare Worker + Durable Objects).
//   /room/<CODE>   a two-seat relay: whatever one player sends goes to the other. The game rules run on the phones.
//   /queue         ranked quick match: players send {t:"find", rating}; two are paired and told a private room code.
//   /auth/signup, /auth/login, /auth/logout   accounts (a name and a password; the password is stored only as a salted PBKDF2 hash)
//   /hub?token=    one live connection per signed-in player: friends, who is online, and match invites that pop up on the other phone.
// The Hub keeps accounts, friends and pending invites in the Durable Object's SQLite storage. Rooms and the queue keep nothing.
const MAX_MSG = 32768;
const CORS = { "access-control-allow-origin": "*", "access-control-allow-headers": "content-type", "access-control-allow-methods": "GET,POST,OPTIONS" };
const json = (o, status = 200) => new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json", ...CORS } });

export default {
  async fetch(req, env) {
    const url = new URL(req.url), parts = url.pathname.split("/").filter(Boolean);
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    if (parts[0] === "room" && /^[A-Z0-9]{4,8}$/.test(parts[1] || "")) return env.ROOM.get(env.ROOM.idFromName(parts[1])).fetch(req);
    if (parts[0] === "queue") return env.LOBBY.get(env.LOBBY.idFromName("main")).fetch(req);
    if (parts[0] === "auth" || parts[0] === "hub") return env.HUB.get(env.HUB.idFromName("main")).fetch(req);
    return new Response("triad server ok v2", { headers: { "content-type": "text/plain", ...CORS } });
  },
};

const upgrade = (req) => {
  if (req.headers.get("Upgrade") !== "websocket") return null;
  return new WebSocketPair();
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
        const other = this.waiting.splice(best, 1)[0], code = roomCode(6);
        for (const p of [other, me]) { try { p.ws.send(JSON.stringify({ t: "match", room: code })); p.ws.close(1000, "matched"); } catch (_) {} }
        me = null;
      } else { this.waiting.push(me); server.send(JSON.stringify({ t: "wait" })); }
    });
    const drop = () => { this.waiting = this.waiting.filter(w => w.ws !== server); };
    server.addEventListener("close", drop); server.addEventListener("error", drop);
    return new Response(null, { status: 101, webSocket: client });
  }
}

const roomCode = (n) => Array.from({ length: n }, () => "ABCDEFGHJKMNPQRSTUVWXYZ23456789"[Math.floor(Math.random() * 31)]).join("");
const hex = (buf) => [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
const NAME_RE = /^[A-Za-z0-9_]{3,14}$/;
const INVITE_TTL = 45000;

/* Sign in with Google / Apple: the phone gets an ID token from the provider; the server checks its signature against the provider's published keys, that it was issued for OUR client id, and that it has not expired. */
const PROVIDERS = {
  google: { keys: "https://www.googleapis.com/oauth2/v3/certs", iss: ["https://accounts.google.com", "accounts.google.com"] },
  apple: { keys: "https://appleid.apple.com/auth/keys", iss: ["https://appleid.apple.com"] },
};
const b64u = (s) => Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(s.length / 4) * 4, "=")), (c) => c.charCodeAt(0));
async function verifyIdToken(provider, token, aud) {
  const [h, p, sg] = String(token).split("."); if (!sg) throw new Error("shape");
  const head = JSON.parse(new TextDecoder().decode(b64u(h))), pay = JSON.parse(new TextDecoder().decode(b64u(p))), cfg = PROVIDERS[provider];
  const jwk = (await (await fetch(cfg.keys)).json()).keys.find((k) => k.kid === head.kid); if (!jwk || head.alg !== "RS256") throw new Error("key");
  const key = await crypto.subtle.importKey("jwk", jwk, { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["verify"]);
  if (!(await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, b64u(sg), new TextEncoder().encode(h + "." + p)))) throw new Error("sig");
  if (!cfg.iss.includes(pay.iss) || pay.aud !== aud || !(pay.exp * 1000 > Date.now()) || !pay.sub) throw new Error("claims");
  return pay;
}

async function hashPw(pw, salt) {
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(pw), "PBKDF2", false, ["deriveBits"]);
  return hex(await crypto.subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: new TextEncoder().encode(salt), iterations: 100000 }, key, 256));
}
async function sha(s) { return hex(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s))); }

export class Hub {
  constructor(state, env) {
    this.state = state; this.env = env || {}; this.sql = state.storage.sql; this.tries = new Map(); this.lastPut = new Map();
    this.sql.exec(`CREATE TABLE IF NOT EXISTS accounts (lname TEXT PRIMARY KEY, name TEXT, salt TEXT, pw TEXT, created INTEGER);
      CREATE TABLE IF NOT EXISTS sessions (th TEXT PRIMARY KEY, lname TEXT, created INTEGER);
      CREATE TABLE IF NOT EXISTS friends (a TEXT, b TEXT, PRIMARY KEY (a, b));
      CREATE TABLE IF NOT EXISTS reqs (frm TEXT, too TEXT, PRIMARY KEY (frm, too));
      CREATE TABLE IF NOT EXISTS profiles (lname TEXT PRIMARY KEY, rev INTEGER, data TEXT, at INTEGER);
      CREATE TABLE IF NOT EXISTS pbak (lname TEXT, rev INTEGER, data TEXT, at INTEGER, PRIMARY KEY (lname, rev));
      CREATE TABLE IF NOT EXISTS oauth (provider TEXT, sub TEXT, lname TEXT, PRIMARY KEY (provider, sub));
      CREATE TABLE IF NOT EXISTS invites (frm TEXT, too TEXT, at INTEGER, PRIMARY KEY (frm, too));`);
  }
  one(q, ...a) { const r = [...this.sql.exec(q, ...a)]; return r[0] || null; }
  all(q, ...a) { return [...this.sql.exec(q, ...a)]; }
  limited(ip) {   // a few tries per minute per address, for sign-up and log-in
    const now = Date.now(), t = (this.tries.get(ip) || []).filter(x => now - x < 60000); t.push(now); this.tries.set(ip, t);
    if (this.tries.size > 500) this.tries.clear(); return t.length > 8;
  }
  async fetch(req) {
    const url = new URL(req.url), p = url.pathname;
    if (p === "/hub") return this.connect(req, url);
    if (p === "/auth/config") return json({ google: this.env.GOOGLE_CLIENT_ID || "", apple: this.env.APPLE_CLIENT_ID || "" });
    if (req.method !== "POST") return json({ ok: false, err: "post only" }, 405);
    let b = {}; try { b = await req.json(); } catch (_) {}
    if (p === "/auth/logout") { if (typeof b.token === "string") this.sql.exec("DELETE FROM sessions WHERE th=?", await sha(b.token)); return json({ ok: true }); }
    if (p === "/profile/get" || p === "/profile/put") {   // the saved game: the server keeps it, the phone keeps a cache
      const s = this.one("SELECT lname FROM sessions WHERE th=?", await sha(String(b.token || ""))); if (!s) return json({ ok: false, auth: true, err: "Please sign in again." }, 401);
      const cur = this.one("SELECT rev, data FROM profiles WHERE lname=?", s.lname);
      if (p === "/profile/get") return json({ ok: true, rev: cur ? cur.rev : 0, data: cur ? cur.data : null });
      const now = Date.now(), last = this.lastPut.get(s.lname) || 0; if (now - last < 1500) return json({ ok: false, slow: true });
      const data = typeof b.data === "string" ? b.data : ""; if (!data || data.length > 400000) return json({ ok: false, err: "Save too large." }, 413);
      let o; try { o = JSON.parse(data); } catch (_) { return json({ ok: false, err: "Bad save." }, 400); }
      if (!o || typeof o !== "object" || Array.isArray(o) || typeof o.own !== "object") return json({ ok: false, err: "Bad save." }, 400);
      const have = cur ? cur.rev : 0; if ((+b.base || 0) !== have && !b.force) return json({ ok: false, conflict: true, rev: have });
      const rev = have + 1; this.lastPut.set(s.lname, now); if (this.lastPut.size > 500) this.lastPut.clear();
      if (cur) { this.sql.exec("INSERT OR REPLACE INTO pbak VALUES (?,?,?,?)", s.lname, have, cur.data, now); for (const o2 of this.all("SELECT rev FROM pbak WHERE lname=? ORDER BY rev DESC", s.lname).slice(8)) this.sql.exec("DELETE FROM pbak WHERE lname=? AND rev=?", s.lname, o2.rev); }
      this.sql.exec("INSERT OR REPLACE INTO profiles VALUES (?,?,?,?)", s.lname, rev, data, now);
      return json({ ok: true, rev });
    }
    if (p === "/auth/oauth") {
      if (this.limited(req.headers.get("cf-connecting-ip") || "x")) return json({ ok: false, err: "Too many tries. Wait a minute." }, 429);
      const prov = String(b.provider || ""), aud = prov === "google" ? this.env.GOOGLE_CLIENT_ID : prov === "apple" ? this.env.APPLE_CLIENT_ID : "";
      if (!aud) return json({ ok: false, err: "That sign-in is not set up yet." });
      let pay; try { pay = await verifyIdToken(prov, b.idToken, aud); } catch (_) { return json({ ok: false, err: "Could not verify that sign-in. Try again." }); }
      const sb = String(pay.sub); let row = this.one("SELECT lname FROM oauth WHERE provider=? AND sub=?", prov, sb);
      if (!row) {   // first time with this Google / Apple account: it needs a game name
        const name = String(b.name || "").trim(); if (!name) return json({ ok: false, needName: true });
        if (!NAME_RE.test(name)) return json({ ok: false, needName: true, err: "Names are 3 to 14 letters, numbers or underscores." });
        const ln = name.toLowerCase(); if (this.one("SELECT 1 AS x FROM accounts WHERE lname=?", ln)) return json({ ok: false, needName: true, err: "That name is taken." });
        this.sql.exec("INSERT INTO accounts VALUES (?,?,?,?,?)", ln, name, hex(crypto.getRandomValues(new Uint8Array(16))), "!", Date.now());   // no password: "!" never matches a hash
        this.sql.exec("INSERT INTO oauth VALUES (?,?,?)", prov, sb, ln); row = { lname: ln };
      }
      return json({ ok: true, name: this.nameOf(row.lname), token: await this.newToken(row.lname) });
    }
    if (p !== "/auth/signup" && p !== "/auth/login") return json({ ok: false, err: "not found" }, 404);
    if (this.limited(req.headers.get("cf-connecting-ip") || "x")) return json({ ok: false, err: "Too many tries. Wait a minute." }, 429);
    const name = String(b.name || "").trim(), pw = String(b.pass || "");
    if (!NAME_RE.test(name)) return json({ ok: false, err: "Names are 3 to 14 letters, numbers or underscores." });
    if (pw.length < 6 || pw.length > 64) return json({ ok: false, err: "Passwords are 6 to 64 characters." });
    const lname = name.toLowerCase();
    if (p === "/auth/signup") {
      if (this.one("SELECT 1 AS x FROM accounts WHERE lname=?", lname)) return json({ ok: false, err: "That name is taken." });
      const salt = hex(crypto.getRandomValues(new Uint8Array(16)));
      this.sql.exec("INSERT INTO accounts VALUES (?,?,?,?,?)", lname, name, salt, await hashPw(pw, salt), Date.now());
      return json({ ok: true, name, token: await this.newToken(lname) });
    }
    const a = this.one("SELECT * FROM accounts WHERE lname=?", lname);
    if (!a || a.pw !== await hashPw(pw, a.salt)) return json({ ok: false, err: "Wrong name or password." });
    return json({ ok: true, name: a.name, token: await this.newToken(lname) });
  }
  async newToken(lname) {
    const t = hex(crypto.getRandomValues(new Uint8Array(24)));
    this.sql.exec("INSERT INTO sessions VALUES (?,?,?)", await sha(t), lname, Date.now());
    const old = this.all("SELECT th FROM sessions WHERE lname=? ORDER BY created DESC", lname);   // keep the six newest devices
    for (const o of old.slice(6)) this.sql.exec("DELETE FROM sessions WHERE th=?", o.th);
    return t;
  }
  async connect(req, url) {
    const pair = upgrade(req); if (!pair) return new Response("expected websocket", { status: 426 });
    const s = this.one("SELECT lname FROM sessions WHERE th=?", await sha(url.searchParams.get("token") || ""));
    const [client, server] = Object.values(pair);
    if (!s) { server.accept(); server.send(JSON.stringify({ t: "auth" })); server.close(4001, "auth"); return new Response(null, { status: 101, webSocket: client }); }
    const a = this.one("SELECT name FROM accounts WHERE lname=?", s.lname);
    this.state.acceptWebSocket(server); server.serializeAttachment({ lname: s.lname, name: a.name });
    for (const o of this.state.getWebSockets()) if (o !== server && o.deserializeAttachment()?.lname === s.lname) { try { o.send(JSON.stringify({ t: "other" })); o.close(1000, "other device"); } catch (_) {} }
    this.sendState(server, s.lname); this.presence(s.lname);
    return new Response(null, { status: 101, webSocket: client });
  }
  socketsOf(lname) { return this.state.getWebSockets().filter(w => { try { return w.readyState === 1 && w.deserializeAttachment()?.lname === lname; } catch (_) { return false; } }); }
  online(lname) { return this.socketsOf(lname).length > 0; }
  nameOf(lname) { const a = this.one("SELECT name FROM accounts WHERE lname=?", lname); return a ? a.name : lname; }
  sendTo(lname, o) { const m = JSON.stringify(o); for (const w of this.socketsOf(lname)) { try { w.send(m); } catch (_) {} } }
  sendState(ws, lname) {
    const friends = this.all("SELECT b FROM friends WHERE a=?", lname).map(f => ({ name: this.nameOf(f.b), online: this.online(f.b) })).sort((x, y) => (y.online - x.online) || x.name.localeCompare(y.name));
    const reqs = this.all("SELECT frm FROM reqs WHERE too=?", lname).map(r => this.nameOf(r.frm));
    const now = Date.now(), inv = this.all("SELECT frm FROM invites WHERE too=? AND at>?", lname, now - INVITE_TTL).map(r => this.nameOf(r.frm));
    try { ws.send(JSON.stringify({ t: "state", me: this.nameOf(lname), friends, reqs, invites: inv })); } catch (_) {}
  }
  pushState(lname) { for (const w of this.socketsOf(lname)) this.sendState(w, lname); }
  presence(lname) { for (const f of this.all("SELECT b FROM friends WHERE a=?", lname)) this.pushState(f.b); }
  webSocketMessage(ws, raw) {
    if (typeof raw !== "string" || raw.length > 2048) return;
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    const me = ws.deserializeAttachment(); if (!me) return;
    const lname = me.lname, other = String(m.name || m.to || m.from || "").toLowerCase(), tell = (msg) => { try { ws.send(JSON.stringify({ t: "msg", m: msg })); } catch (_) {} };
    const exists = other && other !== lname && this.one("SELECT 1 AS x FROM accounts WHERE lname=?", other), friends = (a, b) => !!this.one("SELECT 1 AS x FROM friends WHERE a=? AND b=?", a, b);
    switch (m.t) {
      case "ping": try { ws.send('{"t":"pong"}'); } catch (_) {} return;
      case "friend_add":
        if (!exists) return tell("No player with that name.");
        if (friends(lname, other)) return tell("Already friends.");
        if (this.one("SELECT 1 AS x FROM reqs WHERE frm=? AND too=?", other, lname)) { this.makeFriends(lname, other); return tell("You are now friends with " + this.nameOf(other) + "."); }
        this.sql.exec("INSERT OR IGNORE INTO reqs VALUES (?,?)", lname, other); this.pushState(other); return tell("Friend request sent to " + this.nameOf(other) + ".");
      case "friend_accept":
        if (this.one("SELECT 1 AS x FROM reqs WHERE frm=? AND too=?", other, lname)) this.makeFriends(lname, other); return;
      case "friend_decline": this.sql.exec("DELETE FROM reqs WHERE frm=? AND too=?", other, lname); this.pushState(lname); return;
      case "friend_remove": this.sql.exec("DELETE FROM friends WHERE (a=? AND b=?) OR (a=? AND b=?)", lname, other, other, lname); this.pushState(lname); this.pushState(other); return;
      case "invite": {
        if (!exists || !friends(lname, other)) return tell("You can only challenge friends.");
        if (!this.online(other)) return this.sendTo(lname, { t: "invite_fail", name: this.nameOf(other), why: "offline" });
        this.sql.exec("INSERT OR REPLACE INTO invites VALUES (?,?,?)", lname, other, Date.now());
        this.sendTo(other, { t: "invite", from: me.name }); return;
      }
      case "invite_cancel": this.sql.exec("DELETE FROM invites WHERE frm=? AND too=?", lname, other); this.sendTo(other, { t: "invite_gone", from: me.name }); return;
      case "invite_decline": case "invite_busy": {
        const had = this.one("SELECT 1 AS x FROM invites WHERE frm=? AND too=?", other, lname); this.sql.exec("DELETE FROM invites WHERE frm=? AND too=?", other, lname);
        if (had) this.sendTo(other, { t: "invite_fail", name: me.name, why: m.t === "invite_busy" ? "busy" : "declined" }); return;
      }
      case "invite_accept": {
        const inv = this.one("SELECT at FROM invites WHERE frm=? AND too=?", other, lname); this.sql.exec("DELETE FROM invites WHERE frm=? AND too=?", other, lname);
        if (!inv || Date.now() - inv.at > INVITE_TTL || !this.online(other)) return this.sendTo(lname, { t: "invite_fail", name: this.nameOf(other), why: "expired" });
        const room = roomCode(6); this.sendTo(other, { t: "go", room, with: me.name }); this.sendTo(lname, { t: "go", room, with: this.nameOf(other) }); return;
      }
    }
  }
  makeFriends(a, b) {
    this.sql.exec("INSERT OR IGNORE INTO friends VALUES (?,?)", a, b); this.sql.exec("INSERT OR IGNORE INTO friends VALUES (?,?)", b, a);
    this.sql.exec("DELETE FROM reqs WHERE (frm=? AND too=?) OR (frm=? AND too=?)", a, b, b, a); this.pushState(a); this.pushState(b);
  }
  webSocketClose(ws) { this.gone(ws); }
  webSocketError(ws) { this.gone(ws); }
  gone(ws) {
    try { ws.close(); } catch (_) {}
    let me = null; try { me = ws.deserializeAttachment(); } catch (_) {}
    if (me && !this.online(me.lname)) { this.sql.exec("DELETE FROM invites WHERE frm=?", me.lname); this.presence(me.lname); }
  }
}
