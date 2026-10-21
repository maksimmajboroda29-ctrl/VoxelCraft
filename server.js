'use strict';
/* VOXELCRAFT multiplayer server — Node.js >= 18, no dependencies.
   HTTP: serves public/index.html (the game) + /health.  WebSocket: rooms, players, world edits. */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = +process.env.PORT || 8080;
const MAX_PLAYERS = 8, CH = 64, DAY = 300, MAXMSG = 20000, REJOIN_MS = 60000, MAX_EDITS = 300000;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms = new Map();
const now = () => Date.now();

/* ---------- validation helpers ---------- */
const isInt = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
const isNum = (v, lo, hi) => typeof v === 'number' && Number.isFinite(v) && v >= lo && v <= hi;
const blockOk = (id, gm) => id === 0 || (id >= 1 && id <= 11 && (id !== 9 || gm === 0)) || (id >= 40 && id <= 59) || (id >= 60 && id <= 72) || id === 80;
const itemOk = id => (id >= 1 && id <= 11) || (id >= 20 && id <= 27) || (id >= 40 && id <= 80) || (id >= 100 && id <= 119) || (id >= 130 && id <= 138);
const cleanName = n => String(n == null ? '' : n).replace(/[^\p{L}\p{N} _\-]/gu, '').trim().slice(0, 16);
const KEYRE = /^-?\d{1,8},\d{1,3},-?\d{1,8}$/;
function bucket(p, k, perSec, burst) {            // token bucket rate limiter
  const t = now(), b = p.rate[k] || (p.rate[k] = { n: burst, t });
  b.n = Math.min(burst, b.n + (t - b.t) / 1000 * perSec); b.t = t;
  if (b.n < 1) return false; b.n -= 1; return true;
}

/* ---------- minimal RFC6455 websocket ---------- */
function frame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) { h = Buffer.alloc(2); h[1] = len; }
  else if (len < 65536) { h = Buffer.alloc(4); h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  h[0] = 0x80 | op; return Buffer.concat([h, payload]);
}
function wsSend(c, obj) {
  if (!c || c.closed) return;
  try { if (c.sock.writableLength > 2e6) return c.sock.destroy(); c.sock.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (e) { /* ignore */ }
}
function wsClose(c, code) {
  if (c.closed) return; c.closed = true;
  try { const b = Buffer.alloc(2); b.writeUInt16BE(code || 1000); c.sock.write(frame(8, b)); c.sock.end(); } catch (e) { /* ignore */ }
  setTimeout(() => { try { c.sock.destroy(); } catch (e) { /* */ } }, 500);
  onClose(c);
}
function parse(c) {
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80); let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > BigInt(MAXMSG)) return wsClose(c, 1009); len = Number(big); off = 10; }
    if (len > MAXMSG) return wsClose(c, 1009);
    if (!masked) return wsClose(c, 1002);
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4), pl = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < pl.length; i++) pl[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len); c.seen = now();
    if (op === 8) return wsClose(c, 1000);
    if (op === 9) { c.sock.write(frame(10, pl)); continue; }
    if (op === 10) continue;
    if (op === 1 || op === 0) {
      if (op === 1) { c.frag = []; c.fl = 0; }
      if (!c.frag) return wsClose(c, 1002);
      c.fl += pl.length; if (c.fl > MAXMSG) return wsClose(c, 1009); c.frag.push(pl);
      if (fin) { const txt = Buffer.concat(c.frag).toString('utf8'); c.frag = null; onMessage(c, txt); }
    } else return wsClose(c, 1003);
    if (c.closed) return;
  }
}

/* ---------- rooms ---------- */
function newCode() { for (;;) { let s = ''; for (let i = 0; i < 5; i++) s += ALPHA[crypto.randomInt(ALPHA.length)]; if (!rooms.has(s)) return s; } }
function roomTime(r) { return ((r.t0 + (now() - r.ts) / 1000 / DAY) % 1 + 1) % 1; }
function setTime(r, t) { r.t0 = t; r.ts = now(); }
function bcast(r, obj, except) { for (const p of r.players.values()) if (p !== except && p.conn) wsSend(p.conn, obj); }
function pub(p) { return { id: p.id, name: p.name, x: p.x, y: p.y, z: p.z, yaw: p.yaw, pitch: p.pitch }; }
function welcome(r, p, re) {
  wsSend(p.conn, { t: 'welcome', re: !!re, id: p.id, code: r.code, token: p.token, seed: r.seed, gm: r.gm, time: roomTime(r),
    edits: r.edits, chests: Object.fromEntries(r.chests), players: [...r.players.values()].filter(q => q !== p).map(pub) });
}
function removePlayer(r, p, notify) {
  r.players.delete(p.id); if (p.conn) { p.conn.player = null; p.conn.room = null; }
  if (notify) bcast(r, { t: 'leave', id: p.id });
}
function onClose(c) {
  const p = c.player, r = c.room;
  if (p && r && p.conn === c) { p.conn = null; p.dropAt = now() + REJOIN_MS; }
}
function handleJoin(c, m) {
  if (c.player) return wsSend(c, { t: 'err', msg: 'Уже в комнате' });
  const name0 = cleanName(m.name);
  if (m.t === 'rejoin') {
    const r = rooms.get(String(m.code || '').toUpperCase()), p = r && r.players.get(m.id);
    if (!p || typeof m.token !== 'string' || m.token.length !== 32 || !crypto.timingSafeEqual(Buffer.from(m.token), Buffer.from(p.token)))
      return wsSend(c, { t: 'err', msg: 'Сессия устарела, подключитесь заново' });
    if (p.conn && p.conn !== c) wsClose(p.conn, 4000);
    p.conn = c; p.dropAt = 0; c.player = p; c.room = r; welcome(r, p, true); return;
  }
  let r;
  if (m.t === 'create') {
    if (rooms.size >= 200) return wsSend(c, { t: 'err', msg: 'Сервер перегружен' });
    r = { code: newCode(), seed: 1 + crypto.randomInt(1e9 - 1), gm: m.gm === 0 ? 0 : 1, edits: {}, editN: 0, chests: new Map(), players: new Map(), nid: 1, t0: 0.28, ts: now(), created: now() };
    rooms.set(r.code, r);
  } else {
    r = rooms.get(String(m.code || '').toUpperCase());
    if (!r) return wsSend(c, { t: 'err', msg: 'Комната не найдена' });
    if (r.players.size >= MAX_PLAYERS) return wsSend(c, { t: 'err', msg: 'Комната заполнена' });
  }
  const id = r.nid++; let name = name0 || ('Игрок' + id);
  for (const q of r.players.values()) if (q.name.toLowerCase() === name.toLowerCase()) { name = name.slice(0, 12) + '#' + id; break; }
  const p = { id, name, token: crypto.randomBytes(16).toString('hex'), conn: c, dropAt: 0, x: 0, y: 80, z: 0, yaw: 0, pitch: 0, rate: {}, known: false };
  r.players.set(id, p); c.player = p; c.room = r;
  welcome(r, p, false);
  bcast(r, { t: 'join', id, name, x: p.x, y: p.y, z: p.z, yaw: 0, pitch: 0 }, p);
  console.log(`[${r.code}] ${name} joined (${r.players.size})`);
}
function onMessage(c, txt) {
  let m; try { m = JSON.parse(txt); } catch (e) { return; }
  if (!m || typeof m !== 'object' || typeof m.t !== 'string') return;
  if (!c.player) { if (['create', 'join', 'rejoin'].includes(m.t)) { if (now() - (c.lastJoin || 0) < 500) return; c.lastJoin = now(); handleJoin(c, m); } return; }
  const p = c.player, r = c.room;
  switch (m.t) {
    case 'ping': wsSend(c, { t: 'pong' }); break;
    case 'bye': removePlayer(r, p, true); wsClose(c, 1000); break;
    case 'p': {
      if (!bucket(p, 'p', 25, 40)) return;
      if (!isNum(m.x, -1e6, 1e6) || !isNum(m.y, -64, 300) || !isNum(m.z, -1e6, 1e6) || !isNum(m.yaw, -1e3, 1e3) || !isNum(m.pitch, -4, 4)) return;
      p.x = m.x; p.y = m.y; p.z = m.z; p.yaw = m.yaw; p.pitch = m.pitch; p.known = true;
      bcast(r, { t: 'p', id: p.id, x: m.x, y: m.y, z: m.z, yaw: m.yaw, pitch: m.pitch, fl: m.fl ? 1 : 0 }, p); break;
    }
    case 'b': {   // block change — server validates before it is applied and relayed
      const rej = () => wsSend(c, { t: 'rej', x: m.x | 0, y: m.y | 0, z: m.z | 0 });
      if (!bucket(p, 'b', 20, 30)) return rej();
      if (!isInt(m.x, -1e6, 1e6) || !isInt(m.y, 1, CH - 1) || !isInt(m.z, -1e6, 1e6) || !isInt(m.id, 0, 255) || !blockOk(m.id, r.gm)) return rej();
      if (!p.known || Math.hypot(m.x + .5 - p.x, m.y + .5 - (p.y + 1.6), m.z + .5 - p.z) > 9) return rej();
      const key = m.x + ',' + m.y + ',' + m.z, ch = r.chests.get(key);
      if (m.id === 0 && ch) { if (ch.some(s => s)) return rej(); r.chests.delete(key); }
      const ck = (m.x >> 4) + ',' + (m.z >> 4), idx = (m.y * 16 + (m.z & 15)) * 16 + (m.x & 15), e = r.edits[ck] || (r.edits[ck] = {});
      if (!(idx in e)) { if (r.editN >= MAX_EDITS) return rej(); r.editN++; }
      e[idx] = m.id; bcast(r, { t: 'b', x: m.x, y: m.y, z: m.z, id: m.id }, p); break;
    }
    case 'co': {
      if (!bucket(p, 'c', 5, 10) || typeof m.key !== 'string' || !KEYRE.test(m.key)) return;
      if (!r.chests.has(m.key)) { if (r.chests.size >= 2000) return; r.chests.set(m.key, new Array(27).fill(null)); }
      wsSend(c, { t: 'cd', key: m.key, s: r.chests.get(m.key) }); break;
    }
    case 'cs': {
      if (!bucket(p, 'c', 5, 10) || typeof m.key !== 'string' || !KEYRE.test(m.key) || !r.chests.has(m.key) || !Array.isArray(m.s) || m.s.length !== 27) return;
      const [cx, cy, cz] = m.key.split(',').map(Number);
      if (Math.hypot(cx + .5 - p.x, cy + .5 - p.y, cz + .5 - p.z) > 10) return;
      const out = [];
      for (const s of m.s) {
        if (s === null) { out.push(null); continue; }
        if (!s || typeof s !== 'object' || !isInt(s.id, 1, 255) || !itemOk(s.id) || !isInt(s.n, 1, 64) || !isInt(s.d == null ? 0 : s.d, 0, 2000)) return;
        out.push({ id: s.id, n: s.n, d: s.d | 0 });
      }
      r.chests.set(m.key, out); bcast(r, { t: 'cd', key: m.key, s: out }, p); break;
    }
    case 'sleep': {
      if (!bucket(p, 's', 0.2, 1)) return;
      const t = roomTime(r); if (t >= 0.5) { setTime(r, 0.05); bcast(r, { t: 'time', time: 0.05 }); bcast(r, { t: 'sys', msg: p.name + ' проспал ночь' }, p); }
      break;
    }
  }
}

/* ---------- http + upgrade ---------- */
const FILES = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, '..', 'index.html')];
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok rooms=' + rooms.size); }
  if (req.url === '/' || req.url.startsWith('/index.html') || req.url.startsWith('/?')) {
    const f = FILES.find(x => fs.existsSync(x));
    if (f) { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); return fs.createReadStream(f).pipe(res); }
  }
  res.writeHead(404); res.end('VOXELCRAFT server. Connect via WebSocket.');
});
server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket' || req.headers['sec-websocket-version'] !== '13') { sock.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { sock, buf: Buffer.alloc(0), closed: false, seen: now(), player: null, room: null, frag: null, fl: 0 };
  sock.on('data', d => { if (c.closed) return; c.buf = Buffer.concat([c.buf, d]); if (c.buf.length > MAXMSG * 2 + 100) return wsClose(c, 1009); try { parse(c); } catch (e) { wsClose(c, 1011); } });
  sock.on('error', () => {}); sock.on('close', () => { c.closed = true; onClose(c); });
});

/* ---------- housekeeping ---------- */
const conns = () => { const a = []; for (const r of rooms.values()) for (const p of r.players.values()) if (p.conn) a.push(p.conn); return a; };
setInterval(() => {
  const t = now();
  for (const c of conns()) { if (t - c.seen > 50000) wsClose(c, 4001); else try { c.sock.write(frame(9, Buffer.alloc(0))); } catch (e) { /* */ } }
  for (const [code, r] of rooms) {
    for (const p of [...r.players.values()]) if (!p.conn && p.dropAt && t > p.dropAt) { removePlayer(r, p, true); console.log(`[${code}] ${p.name} timed out`); }
    if (r.players.size === 0 && t - r.created > 120000) { rooms.delete(code); console.log(`[${code}] room closed`); }
  }
}, 10000).unref();
setInterval(() => { for (const r of rooms.values()) bcast(r, { t: 'time', time: roomTime(r) }); }, 5000).unref();

if (require.main === module) server.listen(PORT, () => console.log('VOXELCRAFT server on :' + PORT));
module.exports = { server, rooms };
