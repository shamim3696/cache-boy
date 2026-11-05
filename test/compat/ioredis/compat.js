// Runs common ioredis usage against cache-boy and reports any mismatch.
const Redis = require('ioredis');
const assert = require('assert/strict');

const port = Number(process.argv[2] || 16384);
const results = [];
async function check(name, fn) {
  try {
    await fn();
    results.push(['PASS', name]);
  } catch (e) {
    results.push(['FAIL', name, e.message.split('\n')[0]]);
  }
}

(async () => {
  const r = new Redis({ port, lazyConnect: true, maxRetriesPerRequest: 1 });
  await r.connect();
  await r.flushall();

  await check('strings', async () => {
    assert.equal(await r.set('s', 'v', 'EX', 100), 'OK');
    assert.equal(await r.get('s'), 'v');
    assert.ok((await r.ttl('s')) > 90);
    assert.equal(await r.incrby('n', 5), 5);
    assert.equal(await r.incrbyfloat('f', 1.5), '1.5');
    assert.deepEqual(await r.mget('s', 'missing'), ['v', null]);
    assert.equal(await r.set('s', 'x', 'NX'), null);
    assert.equal(await r.getdel('s'), 'v');
  });
  await check('binary values (Buffer)', async () => {
    const buf = Buffer.from([0, 255, 13, 10, 1]);
    await r.set('bin', buf);
    assert.deepEqual(await r.getBuffer('bin'), buf);
  });
  await check('hashes', async () => {
    await r.hset('h', { a: '1', b: '2' });
    assert.deepEqual(await r.hgetall('h'), { a: '1', b: '2' });
    assert.equal(await r.hincrby('h', 'a', 10), 11);
  });
  await check('lists + blocking pop', async () => {
    await r.rpush('l', 'a', 'b', 'c');
    assert.deepEqual(await r.lrange('l', 0, -1), ['a', 'b', 'c']);
    const b = new Redis({ port });
    const pending = b.blpop('waitq', 2);
    await new Promise((res) => setTimeout(res, 50));
    await r.rpush('waitq', 'job');
    assert.deepEqual(await pending, ['waitq', 'job']);
    b.disconnect();
  });
  await check('sets', async () => {
    await r.sadd('s1', 'a', 'b', 'c');
    await r.sadd('s2', 'b', 'c', 'd');
    assert.deepEqual((await r.sinter('s1', 's2')).sort(), ['b', 'c']);
  });
  await check('sorted sets', async () => {
    await r.zadd('z', 1, 'a', 2, 'b', 3, 'c');
    assert.deepEqual(await r.zrange('z', 0, -1, 'WITHSCORES'), ['a', '1', 'b', '2', 'c', '3']);
    assert.deepEqual(await r.zrangebyscore('z', '(1', '+inf'), ['b', 'c']);
    assert.equal(await r.zscore('z', 'b'), '2');
    assert.equal(await r.zrank('z', 'c'), 2);
  });
  await check('pipeline', async () => {
    const res = await r.pipeline().set('p1', '1').incr('p1').get('p1').exec();
    assert.deepEqual(res, [[null, 'OK'], [null, 2], [null, '2']]);
  });
  await check('multi/exec', async () => {
    const res = await r.multi().set('m', '1').incr('m').exec();
    assert.deepEqual(res, [[null, 'OK'], [null, 2]]);
  });
  await check('watch abort', async () => {
    const other = new Redis({ port });
    await r.watch('w');
    await other.set('w', 'changed');
    assert.equal(await r.multi().set('w', 'mine').exec(), null);
    other.disconnect();
  });
  await check('pub/sub', async () => {
    const sub = new Redis({ port });
    const got = new Promise((res) => sub.on('message', (ch, msg) => res([ch, msg])));
    await sub.subscribe('news');
    await r.publish('news', 'hello');
    assert.deepEqual(await got, ['news', 'hello']);
    sub.disconnect();
  });
  await check('pattern pub/sub', async () => {
    const sub = new Redis({ port });
    const got = new Promise((res) => sub.on('pmessage', (p, ch, msg) => res([p, ch, msg])));
    await sub.psubscribe('ev:*');
    await r.publish('ev:1', 'x');
    assert.deepEqual(await got, ['ev:*', 'ev:1', 'x']);
    sub.disconnect();
  });
  await check('scanStream', async () => {
    for (let i = 0; i < 50; i++) await r.set(`scan:${i}`, i);
    const keys = new Set();
    await new Promise((res, rej) => {
      r.scanStream({ match: 'scan:*', count: 10 }).on('data', (ks) => ks.forEach((k) => keys.add(k))).on('end', res).on('error', rej);
    });
    assert.equal(keys.size, 50);
  });
  await check('custom Lua-free script via call()', async () => {
    assert.equal(await r.call('EVAL', "return cb.call('INCRBY', KEYS[0], ARGV[0])", 1, 'ev', 7), 7);
  });
  await check('feature commands via call()', async () => {
    assert.equal(await r.call('SET', 'tagged', '1', 'TAGS', 'grp'), 'OK');
    assert.equal(await r.call('TAG.INVALIDATE', 'grp'), 1);
    const lim = await r.call('RATELIMIT', 'rl', 10, '1m');
    assert.equal(lim[0], 1);
  });
  await check('info + config', async () => {
    assert.match(await r.info('server'), /redis_version:7\./);
    assert.deepEqual(await r.config('GET', 'maxmemory-policy'), ['maxmemory-policy', 'noeviction']);
  });
  await check('expire / persist / type', async () => {
    await r.set('e', '1');
    assert.equal(await r.expire('e', 100), 1);
    assert.equal(await r.persist('e'), 1);
    assert.equal(await r.ttl('e'), -1);
    assert.equal(await r.type('h'), 'hash');
  });
  await check('errors map to ReplyError', async () => {
    await assert.rejects(r.lpush('h', 'x'), (e) => e.name === 'ReplyError' && /WRONGTYPE/.test(e.message));
  });

  r.disconnect();
  for (const row of results) console.log(row.join('  '));
  const failed = results.filter((x) => x[0] === 'FAIL').length;
  console.log(`\n${results.length - failed}/${results.length} passed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => {
  console.error(e);
  process.exit(2);
});
