/**
 * Integration tests against a real cache-boy server. Skipped unless
 * CACHEBOY_URL is set, e.g.:
 *
 *   CACHEBOY_URL=redis://127.0.0.1:6380 npm test
 *   CACHEBOY_HTTP_URL=http://127.0.0.1:8080 (optional, enables the HTTP tests)
 *   CACHEBOY_TOKEN=... (optional, for the HTTP API)
 */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, describe, it } from 'node:test';
import { CacheBoy } from '../src/client.js';
import { CacheBoyError } from '../src/errors.js';
import { CacheBoyHttp } from '../src/http.js';
import { delay, until } from './helpers.js';

const URL_ = process.env['CACHEBOY_URL'];
const HTTP_URL = process.env['CACHEBOY_HTTP_URL'];
const skip = URL_ ? false : 'set CACHEBOY_URL (e.g. redis://127.0.0.1:6380) to run integration tests';
const p = `sdkit:${randomUUID().slice(0, 8)}:`; // unique prefix per run

describe('integration (TCP)', { skip }, () => {
  let c: CacheBoy;
  let other: CacheBoy;

  before(async () => {
    c = new CacheBoy(URL_!, { clientName: 'sdk-it', commandTimeoutMs: 10_000 });
    other = new CacheBoy(URL_!, { clientName: 'sdk-it-2', commandTimeoutMs: 10_000 });
    await Promise.all([c.connect(), other.connect()]);
  });

  after(async () => {
    const keys = await c.keys(`${p}*`).catch(() => [] as string[]);
    if (keys.length) await c.del(...keys).catch(() => 0);
    await Promise.all([c.quit(), other.quit()]);
  });

  it('HELLO 3 handshake', () => {
    assert.equal(Number(c.serverInfo?.['proto']), 3);
  });

  it('get/set/del, JSON values, options', async () => {
    assert.equal(await c.set(`${p}a`, 'hello'), 'OK');
    assert.equal(await c.get(`${p}a`), 'hello');
    await c.set(`${p}obj`, { n: 1, s: 'x' }, { ex: 60 });
    assert.deepEqual(await c.getJSON(`${p}obj`), { n: 1, s: 'x' });
    assert.ok((await c.ttl(`${p}obj`)) > 0);
    assert.equal(await c.set(`${p}a`, 'nope', { nx: true }), null);
    assert.equal(await c.set(`${p}a`, 'new', { get: true }), 'hello');
    assert.equal(await c.del(`${p}a`, `${p}obj`), 2);
    assert.equal(await c.get(`${p}a`), null);
  });

  it('binary-safe values', async () => {
    const bin = Buffer.from([0, 1, 2, 13, 10, 255]);
    await c.set(`${p}bin`, bin);
    assert.deepEqual(await c.getBuffer(`${p}bin`), bin);
  });

  it('errors carry the code', async () => {
    await c.set(`${p}str`, 'x');
    await assert.rejects(c.lpush(`${p}str`, 'y'), (e: unknown) => e instanceof CacheBoyError && e.code === 'WRONGTYPE');
  });

  it('auto-pipelining: many concurrent commands', async () => {
    const n = 500;
    const res = await Promise.all(Array.from({ length: n }, () => c.incr(`${p}ctr`)));
    assert.equal(Math.max(...res), n);
  });

  it('pipeline and multi', async () => {
    const r = await c.pipeline().set(`${p}p1`, '1').incr(`${p}p1`).get(`${p}p1`).exec();
    assert.deepEqual(r, [[null, 'OK'], [null, 2], [null, '2']]);
    const m = await c.multi().set(`${p}m1`, 'a').append(`${p}m1`, 'b').get(`${p}m1`).exec();
    assert.deepEqual(m[2], [null, 'ab']);
  });

  it('client-side caching: invalidation when another client writes', async () => {
    const cached = new CacheBoy(URL_!, { clientCache: { maxEntries: 1000 } });
    try {
      await cached.connect();
      await other.set(`${p}cc`, 'v1');
      assert.equal(await cached.get(`${p}cc`), 'v1');
      assert.equal(await cached.get(`${p}cc`), 'v1');
      assert.equal(cached.cacheStats().hits, 1);
      await other.set(`${p}cc`, 'v2');
      await until(() => cached.cacheStats().invalidations >= 1, 3000);
      assert.equal(await cached.get(`${p}cc`), 'v2');
    } finally {
      await cached.quit();
    }
  });

  it('wrap(): one computation for concurrent callers', async () => {
    let computes = 0;
    const fn = async (): Promise<{ at: number }> => {
      computes++;
      await delay(100);
      return { at: 1 };
    };
    const results = await Promise.all(
      Array.from({ length: 5 }, (_, i) => (i % 2 ? c : other).wrap(`${p}wrap`, fn, { ttl: 30, tags: [`${p}tag`] })),
    );
    for (const r of results) assert.deepEqual(r, { at: 1 });
    assert.equal(computes, 1);
    assert.deepEqual(await c.getJSON(`${p}wrap`), { at: 1 });
    // Cached now.
    assert.deepEqual(await c.wrap(`${p}wrap`, fn, { ttl: 30 }), { at: 1 });
    assert.equal(computes, 1);
  });

  it('wrap(): fn errors release the compute lock', async () => {
    await assert.rejects(c.wrap(`${p}wrap-err`, () => Promise.reject(new Error('db down')), { ttl: 30, waitMs: 200 }), /db down/);
    assert.equal(await c.wrap(`${p}wrap-err`, () => 'ok', { ttl: 30, waitMs: 200 }), 'ok');
  });

  it('tags', async () => {
    await c.set(`${p}t1`, '1', { tags: [`${p}red`, `${p}blue`] });
    await c.set(`${p}t2`, '2', { tags: [`${p}red`] });
    assert.deepEqual((await c.tags.keys(`${p}red`)).sort(), [`${p}t1`, `${p}t2`]);
    assert.deepEqual((await c.tags.of(`${p}t1`)).sort(), [`${p}blue`, `${p}red`]);
    assert.equal(await c.tags.add(`${p}t2`, `${p}green`), 1);
    assert.equal(await c.tags.remove(`${p}t2`, `${p}green`), 1);
    assert.ok((await c.tags.list()).includes(`${p}red`));
    assert.equal(await c.tags.invalidate(`${p}red`), 2);
    assert.equal(await c.exists(`${p}t1`, `${p}t2`), 0);
  });

  it('lock: fencing tokens, exclusivity, release, extend', async () => {
    const l1 = await c.lock(`${p}lock`, 5000, { owner: 'a' });
    assert.ok(l1);
    assert.equal(await other.lock(`${p}lock`, 5000), null);
    assert.equal(await l1.extend(10_000), true);
    const info = await c.lockInfo(`${p}lock`);
    assert.equal(info?.owner, 'a');
    assert.equal(await l1.release(), true);
    const l2 = await other.lock(`${p}lock`, 5000);
    assert.ok(l2 && l2.token > l1.token);
    await l2.release();
    assert.equal(await c.withLock(`${p}lock`, 1000, () => 'done'), 'done');
  });

  it('lock: WAIT acquires after the holder releases', async () => {
    const l1 = await c.lock(`${p}lockw`, 5000);
    assert.ok(l1);
    setTimeout(() => void l1.release(), 100);
    const l2 = await other.lock(`${p}lockw`, 5000, { waitMs: 2000 });
    assert.ok(l2);
    await l2.release();
  });

  it('semanticCache: similar prompts hit, wrap computes once', async () => {
    // Toy embedding: bag of words hashed into 16 buckets (stands in for a real model).
    const embed = (text: string): number[] => {
      const v = new Array<number>(16).fill(0);
      for (const w of text.toLowerCase().match(/[a-z]+/g) ?? []) {
        let h = 0;
        for (const ch of w) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
        v[h % 16] += 1;
      }
      return v;
    };
    const llm = c.semanticCache(`${p}llm`, { embed, threshold: 0.8, ttl: '1h' });
    let calls = 0;
    const ask = (q: string) => llm.wrap(q, () => { calls++; return `answer to: ${q}`; });
    assert.equal(await ask('what is the capital of france'), 'answer to: what is the capital of france');
    assert.equal(await ask('What is the capital of France?'), 'answer to: what is the capital of france');
    assert.equal(calls, 1);
    const miss = await llm.get('how do I bake bread');
    assert.equal(miss.hit, false);
    const info = await llm.info();
    assert.equal(info.entries, 1);
    assert.ok(info.hits >= 1);
    assert.equal(await llm.drop(), 1);
  });

  it('rateLimit', async () => {
    const k = `${p}rl`;
    const a = await c.rateLimit(k, 2, '1m');
    const b = await c.rateLimit(k, 2, '1m');
    const d = await c.rateLimit(k, 2, '1m');
    assert.equal(a.allowed, true);
    assert.equal(b.allowed, true);
    assert.equal(d.allowed, false);
    assert.ok(d.retryAfterMs > 0);
    assert.equal(a.remaining, 1);
    await c.rateLimitReset(k);
    assert.equal((await c.rateLimit(k, 2, '1m')).allowed, true);
  });

  it('queue: push, pop, ack, nack to dead letter, retryDead, purge', async () => {
    const q = c.queue<{ n: number }>(`${p}q`);
    const id = await q.push({ n: 1 }, { retries: 0 });
    assert.ok(id);
    assert.equal((await q.len()).ready, 1);
    const job = await q.pop({ visibilityMs: 5000 });
    assert.ok(job);
    assert.deepEqual(job.payload, { n: 1 });
    assert.equal((await q.len()).inflight, 1);
    await job.nack(); // retries 0 → dead
    assert.equal((await q.len()).dead, 1);
    assert.equal((await q.dead())[0]?.id, id);
    assert.equal(await q.retryDead(), 1);
    const again = await q.pop();
    assert.ok(again);
    assert.equal(await again.ack(), true);
    await q.push({ n: 2 }, { delayMs: 60_000 });
    assert.equal((await q.len()).delayed, 1);
    assert.equal(await q.purge(), 1);
  });

  it('queue: duplicate ids are ignored', async () => {
    const q = c.queue(`${p}qd`);
    const a = await q.push('x', { id: 'same' });
    const b = await q.push('y', { id: 'same' });
    assert.equal(a, b);
    assert.equal((await q.len()).ready, 1);
    await q.purge();
  });

  it('queue: worker processes jobs with concurrency and stops gracefully', async () => {
    const q = c.queue<number>(`${p}qw`);
    for (let i = 0; i < 10; i++) await q.push(i);
    const seen: number[] = [];
    const worker = q.process(async (job) => {
      seen.push(job.payload);
      await delay(10);
    }, { concurrency: 3, blockMs: 200 });
    await until(() => seen.length === 10, 5000);
    await worker.stop();
    assert.deepEqual([...seen].sort((a, b) => a - b), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    const len = await q.len();
    assert.equal(len.ready + len.inflight, 0);
  });

  it('pub/sub', async () => {
    const got: string[] = [];
    const unsub = await c.subscribe(`${p}ch`, (m) => got.push(m));
    await other.publish(`${p}ch`, 'hi');
    await until(() => got.length === 1);
    assert.deepEqual(got, ['hi']);
    await unsub();
  });

  it('namespaces isolate keys', async () => {
    const ns = c.namespace(`${p}tenant`);
    await ns.set('k', 'inside');
    assert.equal(await ns.get('k'), 'inside');
    assert.equal(await c.get('k'), null);
    await ns.del('k');
  });
});

describe('integration (HTTP)', { skip: URL_ && HTTP_URL ? false : 'set CACHEBOY_URL and CACHEBOY_HTTP_URL to run HTTP integration tests' }, () => {
  it('get/set, pipeline, multi', async () => {
    const h = new CacheBoyHttp({ url: HTTP_URL!, ...(process.env['CACHEBOY_TOKEN'] ? { token: process.env['CACHEBOY_TOKEN'] } : {}) });
    await h.set(`${p}h`, { a: 1 });
    assert.deepEqual(await h.getJSON(`${p}h`), { a: 1 });
    const r = await h.pipeline().incr(`${p}hc`).incr(`${p}hc`).exec();
    assert.deepEqual(r, [[null, 1], [null, 2]]);
    const m = await h.multi().incr(`${p}hc`).get(`${p}hc`).exec();
    assert.deepEqual(m, [[null, 3], [null, '3']]);
    await h.del(`${p}h`, `${p}hc`);
  });
});
