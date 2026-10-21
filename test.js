'use strict';
// Protocol test: two WebSocket clients against a locally started server (Node >= 22 has global WebSocket).
const { server } = require('./server.js');
const assert = require('assert');
const sleep = ms => new Promise(r => setTimeout(r, ms));
function client(url) {
  return new Promise((res, rej) => { const ws = new WebSocket(url), q = []; ws.onmessage = e => q.push(JSON.parse(e.data));
    ws.onopen = () => res({ ws, q, send: o => ws.send(JSON.stringify(o)), raw: s => ws.send(s), find: t => q.find(m => m.t === t), all: t => q.filter(m => m.t === t) }); ws.onerror = rej; });
}
(async () => {
  await new Promise(r => server.listen(0, r)); const url = 'ws://127.0.0.1:' + server.address().port;
  const A = await client(url); A.send({ t: 'create', name: 'Alice', gm: 1 }); await sleep(150);
  const wa = A.find('welcome'); assert(wa && wa.code.length === 5 && wa.seed > 0 && wa.gm === 1, 'create/welcome'); console.log('OK create room', wa.code, 'seed', wa.seed);
  const B = await client(url); B.send({ t: 'join', code: wa.code.toLowerCase(), name: 'Bob' }); await sleep(150);
  const wb = B.find('welcome'); assert(wb && wb.seed === wa.seed && wb.players.length === 1 && wb.players[0].name === 'Alice', 'join'); console.log('OK join + same seed + sees Alice');
  assert(A.find('join') && A.find('join').name === 'Bob', 'alice notified'); console.log('OK Alice notified of Bob');
  const C = await client(url); C.send({ t: 'join', code: 'ZZZZZ', name: 'X' }); await sleep(100); assert(C.find('err'), 'bad code'); console.log('OK unknown code rejected');
  A.send({ t: 'p', x: 10, y: 40, z: 5, yaw: 1.5, pitch: .2, fl: 1 }); await sleep(100);
  const pp = B.find('p'); assert(pp && pp.id === wa.id && pp.x === 10 && pp.fl === 1, 'position relay'); console.log('OK position relayed to Bob');
  A.send({ t: 'b', x: 11, y: 40, z: 5, id: 0 }); await sleep(100);
  assert(B.find('b') && B.find('b').x === 11 && B.find('b').id === 0, 'block relay'); console.log('OK valid block change relayed');
  A.send({ t: 'b', x: 500, y: 40, z: 5, id: 3 }); await sleep(100); assert(A.find('rej'), 'reach'); console.log('OK out-of-reach edit rejected');
  A.send({ t: 'b', x: 11, y: 0, z: 5, id: 0 }); A.send({ t: 'b', x: 11, y: 40, z: 5, id: 999 }); A.send({ t: 'b', x: 'a', y: 40, z: 5, id: 3 }); await sleep(100);
  assert(A.all('rej').length >= 4, 'invalid'); console.log('OK bedrock / bad id / bad type rejected');
  A.send({ t: 'b', x: 11, y: 40, z: 5, id: 59 }); await sleep(60); A.send({ t: 'co', key: '11,40,5' }); await sleep(100);
  const cd = A.find('cd'); assert(cd && cd.s.length === 27, 'chest open'); A.send({ t: 'cs', key: '11,40,5', s: cd.s.map((_, i) => i === 0 ? { id: 135, n: 3, d: 0 } : null) }); await sleep(100);
  assert(B.find('cd') && B.find('cd').s[0].n === 3, 'chest sync'); console.log('OK chest stored and synced');
  A.send({ t: 'cs', key: '11,40,5', s: cd.s.map((_, i) => i === 0 ? { id: 7, n: 9999, d: 0 } : null) }); await sleep(80);
  A.send({ t: 'b', x: 11, y: 40, z: 5, id: 0 }); await sleep(100); assert(A.all('rej').length >= 5, 'nonempty chest break'); console.log('OK invalid chest data ignored, non-empty chest cannot be broken');
  A.raw('this is not json'); A.raw('{"t":42}'); A.raw('[]'); await sleep(80); A.send({ t: 'ping' }); await sleep(80); assert(A.find('pong'), 'still alive'); console.log('OK malformed messages ignored');
  A.ws.close(); await sleep(150); assert(!B.find('leave'), 'grace period'); console.log('OK dropped player kept for reconnect grace period');
  const A2 = await client(url); A2.send({ t: 'rejoin', code: wa.code, id: wa.id, token: wa.token, name: 'Alice' }); await sleep(150);
  const wr = A2.find('welcome'); assert(wr && wr.re && wr.id === wa.id && wr.edits && Object.keys(wr.edits).length > 0, 'rejoin'); console.log('OK reconnect restored same player id + world edits');
  const A3 = await client(url); A3.send({ t: 'rejoin', code: wa.code, id: wa.id, token: 'x'.repeat(32), name: 'Evil' }); await sleep(100); assert(A3.find('err'), 'bad token'); console.log('OK rejoin with wrong token refused');
  B.send({ t: 'bye' }); await sleep(100); assert(A2.find('leave'), 'leave'); console.log('OK clean leave broadcast');
  const big = await client(url); let closed = false; big.ws.onclose = () => closed = true; big.raw(JSON.stringify({ t: 'create', pad: 'x'.repeat(30000) })); await sleep(200); assert(closed, 'oversize'); console.log('OK oversize message closes connection');
  console.log('\nALL TESTS PASSED'); process.exit(0);
})().catch(e => { console.error('TEST FAILED', e); process.exit(1); });
