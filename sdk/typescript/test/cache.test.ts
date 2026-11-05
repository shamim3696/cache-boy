import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { ClientCache, LruCache } from '../src/cache.js';

describe('LruCache', () => {
  it('evicts the least recently used entry', () => {
    const c = new LruCache<string>(3);
    c.set('a', '1');
    c.set('b', '2');
    c.set('c', '3');
    assert.equal(c.get('a'), '1'); // a is now most recent
    c.set('d', '4'); // evicts b
    assert.equal(c.get('b'), undefined);
    assert.deepEqual([...c.keys()], ['c', 'a', 'd']);
    assert.equal(c.size, 3);
  });

  it('overwriting refreshes recency without growing', () => {
    const c = new LruCache<string>(2);
    c.set('a', '1');
    c.set('b', '2');
    c.set('a', '3');
    c.set('c', '4'); // evicts b
    assert.equal(c.get('a'), '3');
    assert.equal(c.get('b'), undefined);
  });

  it('expires entries after ttlMs', () => {
    let now = 1000;
    const c = new LruCache<string>(10, 50, () => now);
    c.set('a', '1');
    now += 49;
    assert.equal(c.get('a'), '1');
    now += 2;
    assert.equal(c.get('a'), undefined);
    assert.equal(c.size, 0);
  });

  it('rejects a non-positive capacity', () => {
    assert.throws(() => new LruCache(0), RangeError);
  });
});

describe('ClientCache', () => {
  it('caches a completed read and counts hits and misses', () => {
    const c = new ClientCache({ maxEntries: 10 });
    assert.equal(c.lookup('k'), undefined);
    const t = c.beginRead('k');
    assert.equal(c.endRead(t, 'v'), true);
    assert.equal(c.lookup('k'), 'v');
    assert.deepEqual(c.stats(), { hits: 1, misses: 1, size: 1, invalidations: 0 });
  });

  it('does not cache null (missing keys)', () => {
    const c = new ClientCache();
    const t = c.beginRead('k');
    assert.equal(c.endRead(t, null), false);
    assert.equal(c.stats().size, 0);
  });

  it('evicts keys on invalidation', () => {
    const c = new ClientCache();
    c.endRead(c.beginRead('a'), '1');
    c.endRead(c.beginRead('b'), '2');
    c.invalidate(['a']);
    assert.equal(c.lookup('a'), undefined);
    assert.equal(c.lookup('b'), '2');
    assert.equal(c.stats().invalidations, 1);
  });

  it('clears everything on invalidate(null)', () => {
    const c = new ClientCache();
    c.endRead(c.beginRead('a'), '1');
    c.endRead(c.beginRead('b'), '2');
    c.invalidate(null);
    assert.equal(c.stats().size, 0);
    assert.equal(c.stats().invalidations, 1);
  });

  it('race: an invalidation during an in-flight read prevents caching the stale reply', () => {
    const c = new ClientCache();
    const t = c.beginRead('k');
    c.invalidate(['k']); // arrives before the GET reply
    assert.equal(c.endRead(t, 'stale'), false);
    assert.equal(c.lookup('k'), undefined);
    // The next read is cached normally.
    assert.equal(c.endRead(c.beginRead('k'), 'fresh'), true);
    assert.equal(c.lookup('k'), 'fresh');
  });

  it('race: overlapping reads of the same key, invalidated between them', () => {
    const c = new ClientCache();
    const t1 = c.beginRead('k');
    c.invalidate(['k']);
    const t2 = c.beginRead('k'); // started after the invalidation: its reply is fresh
    assert.equal(c.endRead(t1, 'old'), false);
    assert.equal(c.endRead(t2, 'new'), true);
    assert.equal(c.lookup('k'), 'new');
  });

  it('race: a full flush (or reconnect) during a read prevents caching', () => {
    const c = new ClientCache();
    const t = c.beginRead('k');
    c.clear();
    assert.equal(c.endRead(t, 'v'), false);
    const t2 = c.beginRead('k');
    c.invalidate(null);
    assert.equal(c.endRead(t2, 'v'), false);
  });

  it('an invalidation of another key does not affect the read', () => {
    const c = new ClientCache();
    const t = c.beginRead('k');
    c.invalidate(['other']);
    assert.equal(c.endRead(t, 'v'), true);
  });

  it('abortRead releases in-flight state', () => {
    const c = new ClientCache();
    const t = c.beginRead('k');
    c.abortRead(t);
    const t2 = c.beginRead('k');
    assert.equal(c.endRead(t2, 'v'), true);
  });

  it('local evictions are not counted as server invalidations', () => {
    const c = new ClientCache();
    c.endRead(c.beginRead('a'), '1');
    c.invalidate(['a'], false);
    assert.equal(c.stats().invalidations, 0);
    assert.equal(c.lookup('a'), undefined);
  });

  it('respects maxEntries', () => {
    const c = new ClientCache({ maxEntries: 2 });
    for (const k of ['a', 'b', 'c']) c.endRead(c.beginRead(k), k);
    assert.equal(c.stats().size, 2);
    assert.equal(c.lookup('a'), undefined);
  });
});
