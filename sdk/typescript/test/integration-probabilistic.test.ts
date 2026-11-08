/**
 * Integration tests for bloom filters, HyperLogLog and wrap()'s cache
 * penetration options. Skipped unless CACHEBOY_URL is set (see integration.test.ts).
 */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, describe, it } from 'node:test';
import { CacheBoy } from '../src/client.js';
import { CacheBoyError } from '../src/errors.js';

const URL_ = process.env['CACHEBOY_URL'];
const skip = URL_ ? false : 'set CACHEBOY_URL (e.g. redis://127.0.0.1:6380) to run integration tests';
const p = `sdkit:${randomUUID().slice(0, 8)}:`;

describe('integration: bloom, hyperloglog, cache penetration', { skip }, () => {
  let c: CacheBoy;

  before(async () => {
    c = new CacheBoy(URL_!, { clientName: 'sdk-it-prob', commandTimeoutMs: 10_000 });
    await c.connect();
  });

  after(async () => {
    const keys = await c.keys(`${p}*`).catch(() => [] as string[]);
    if (keys.length) await c.del(...keys).catch(() => 0);
    await c.quit();
  });

  it('bloom filter', async () => {
    await c.bloom.reserve(`${p}bf`, 0.01, 1000);
    await assert.rejects(c.bloom.reserve(`${p}bf`, 0.01, 1000), CacheBoyError);
    assert.equal(await c.bloom.add(`${p}bf`, 'a'), true);
    assert.equal(await c.bloom.add(`${p}bf`, 'a'), false);
    assert.deepEqual(await c.bloom.madd(`${p}bf`, ['b', 'c']), [true, true]);
    assert.equal(await c.bloom.exists(`${p}bf`, 'b'), true);
    assert.deepEqual(await c.bloom.mexists(`${p}bf`, ['a', 'never-added']), [true, false]);
    assert.equal(await c.bloom.card(`${p}bf`), 3);
    const info = await c.bloom.info(`${p}bf`);
    assert.equal(info.capacity, 1000);
    assert.equal(info.items, 3);
    assert.equal(info.filters, 1);
    assert.equal(info.expansionRate, 2);
    assert.equal(await c.type(`${p}bf`), 'MBbloom--');

    await c.bloom.reserve(`${p}ns`, 0.01, 1, { nonScaling: true });
    await c.bloom.add(`${p}ns`, 'x');
    await assert.rejects(c.bloom.add(`${p}ns`, 'y'), /full/);
  });

  it('hyperloglog', async () => {
    const a = Array.from({ length: 1000 }, (_, i) => `a${i}`);
    assert.equal(await c.pfAdd(`${p}h1`, ...a), true);
    assert.equal(await c.pfAdd(`${p}h1`, 'a1'), false);
    const n = await c.pfCount(`${p}h1`);
    assert.ok(Math.abs(n - 1000) <= 20, `estimate ${n}`);
    await c.pfAdd(`${p}h2`, 'a1', 'b1', 'b2');
    assert.equal(await c.pfCount(`${p}h1`, `${p}h2`), (await c.pfCount(`${p}h1`)) + 2);
    await c.pfMerge(`${p}m`, `${p}h1`, `${p}h2`);
    assert.equal(await c.pfCount(`${p}m`), await c.pfCount(`${p}h1`, `${p}h2`));
    assert.equal(await c.type(`${p}m`), 'hyperloglog');
  });

  it('wrap(): negativeTtl caches misses briefly', async () => {
    let calls = 0;
    const load = (): null => {
      calls++;
      return null;
    };
    assert.equal(await c.wrap(`${p}user:404`, load, { ttl: 300, negativeTtl: 30 }), null);
    assert.equal(await c.wrap(`${p}user:404`, load, { ttl: 300, negativeTtl: 30 }), null);
    assert.equal(calls, 1);
    const ttl = await c.ttl(`${p}user:404`);
    assert.ok(ttl > 0 && ttl <= 30, `ttl ${ttl}`);
  });

  it('wrap(): bloom guard skips ids that were never added', async () => {
    await c.bloom.add(`${p}ids`, '1');
    let calls = 0;
    const load = (): { id: number } => {
      calls++;
      return { id: 1 };
    };
    assert.equal(await c.wrap(`${p}user:2`, load, { ttl: 60, bloom: { key: `${p}ids`, item: '2' } }), null);
    assert.equal(calls, 0);
    assert.equal(await c.exists(`${p}user:2`), 0);
    assert.deepEqual(await c.wrap(`${p}user:1`, load, { ttl: 60, bloom: { key: `${p}ids`, item: '1' } }), { id: 1 });
    assert.equal(calls, 1);
  });
});
