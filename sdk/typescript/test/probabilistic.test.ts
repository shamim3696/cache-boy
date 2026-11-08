import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { CacheBoyError } from '../src/errors.js';
import { NEGATIVE_SENTINEL } from '../src/features/wrap.js';
import { MockClient } from './helpers.js';

describe('bloom and hyperloglog (mock transport)', () => {
  it('bloom commands encode arguments and map replies', async () => {
    const m = new MockClient((a) => {
      switch (a[0]) {
        case 'BF.ADD':
        case 'BF.EXISTS':
          return 1;
        case 'BF.MADD':
        case 'BF.MEXISTS':
          return [1, 0];
        case 'BF.CARD':
          return 7;
        case 'BF.INFO':
          return { Capacity: 100, Size: 272, 'Number of filters': 1, 'Number of items inserted': 7, 'Expansion rate': 2 };
      }
      return 'OK';
    });
    await m.bloom.reserve('b', 0.001, 1000, { expansion: 4 });
    await m.bloom.reserve('n', 0.01, 10, { nonScaling: true });
    assert.equal(await m.bloom.add('b', 'x'), true);
    assert.deepEqual(await m.bloom.madd('b', ['x', 'y']), [true, false]);
    assert.equal(await m.bloom.exists('b', 'x'), true);
    assert.deepEqual(await m.bloom.mexists('b', ['x', 'z']), [true, false]);
    assert.equal(await m.bloom.card('b'), 7);
    assert.deepEqual(await m.bloom.info('b'), { capacity: 100, size: 272, filters: 1, items: 7, expansionRate: 2 });
    assert.deepEqual(m.calls.slice(0, 4), [
      ['BF.RESERVE', 'b', '0.001', '1000', 'EXPANSION', '4'],
      ['BF.RESERVE', 'n', '0.01', '10', 'NONSCALING'],
      ['BF.ADD', 'b', 'x'],
      ['BF.MADD', 'b', 'x', 'y'],
    ]);
  });

  it('bloom info accepts RESP2 flat arrays and a null expansion rate', async () => {
    const m = new MockClient(() => ['Capacity', 10, 'Size', 200, 'Number of filters', 1, 'Number of items inserted', 2, 'Expansion rate', null]);
    assert.deepEqual(await m.bloom.info('n'), { capacity: 10, size: 200, filters: 1, items: 2, expansionRate: null });
  });

  it('bloom madd rethrows a per-item error', async () => {
    const m = new MockClient(() => [1, new CacheBoyError('ERR non scaling filter is full')]);
    await assert.rejects(m.bloom.madd('n', ['a', 'b']), /full/);
  });

  it('pfAdd, pfCount, pfMerge', async () => {
    const m = new MockClient((a) => (a[0] === 'PFADD' ? 1 : a[0] === 'PFCOUNT' ? 42 : 'OK'));
    assert.equal(await m.pfAdd('h', 'a', 'b'), true);
    assert.equal(await m.pfCount('h', 'h2'), 42);
    await m.pfMerge('dest', 'h', 'h2');
    assert.deepEqual(m.calls, [
      ['PFADD', 'h', 'a', 'b'],
      ['PFCOUNT', 'h', 'h2'],
      ['PFMERGE', 'dest', 'h', 'h2'],
    ]);
    assert.equal(await new MockClient(() => 0).pfAdd('h', 'a'), false);
  });

  it('pfAdd and bloom work in pipelines', async () => {
    const m = new MockClient((a) => (a[0] === 'PFCOUNT' ? 3 : 1));
    const res = await m.pipeline().pfAdd('h', 'a').pfCount('h').exec();
    assert.deepEqual(res, [[null, true], [null, 3]]);
  });
});

describe('wrap() cache penetration (mock transport)', () => {
  it('negativeTtl: a null result is cached as the sentinel with the short TTL', async () => {
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['compute', null] : 'OK'));
    const v = await m.wrap('user:404', async () => null, { ttl: 300, negativeTtl: 30 });
    assert.equal(v, null);
    assert.deepEqual(m.calls[1], ['SET', 'user:404', NEGATIVE_SENTINEL, 'EX', '30']);
  });

  it('negativeTtl: undefined is treated like null', async () => {
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['timeout', null] : 'OK'));
    assert.equal(await m.wrap('k', () => undefined, { ttl: 300, negativeTtl: 5 }), null);
    assert.deepEqual(m.calls[1], ['SET', 'k', NEGATIVE_SENTINEL, 'EX', '5']);
  });

  it('negativeTtl: a sentinel hit resolves to null without calling fn or parse', async () => {
    const m = new MockClient(() => ['hit', NEGATIVE_SENTINEL]);
    let called = false;
    const v = await m.wrap('k', () => {
      called = true;
      return 1;
    }, { ttl: 60, negativeTtl: 10, parse: () => assert.fail('parse must not run') });
    assert.equal(v, null);
    assert.equal(called, false);
    assert.deepEqual(m.names(), ['CACHE.GET']);
  });

  it('real values are never confused with the sentinel', async () => {
    // JSON text of any value, even a string that spells the sentinel, differs from it.
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['compute', null] : 'OK'));
    for (const value of [NEGATIVE_SENTINEL, 'null', 0, false, '', []]) {
      m.calls.length = 0;
      assert.deepEqual(await m.wrap('k', () => value, { ttl: 60, negativeTtl: 10 }), value);
      assert.notEqual(m.calls[1]![2], NEGATIVE_SENTINEL);
      assert.equal(m.calls[1]![4], '60');
    }
    const hit = new MockClient(() => ['hit', JSON.stringify(NEGATIVE_SENTINEL)]);
    assert.equal(await hit.wrap('k', () => 'x', { ttl: 60, negativeTtl: 10 }), NEGATIVE_SENTINEL);
  });

  it('a custom serializer that produces the sentinel is rejected and releases the lock', async () => {
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['compute', null] : 1));
    await assert.rejects(m.wrap('k', () => 'x', { ttl: 60, negativeTtl: 10, serialize: () => NEGATIVE_SENTINEL }), TypeError);
    assert.deepEqual(m.names(), ['CACHE.GET', 'CACHE.RELEASE']);
  });

  it('without negativeTtl, null is cached as JSON null for the full TTL (unchanged)', async () => {
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['compute', null] : 'OK'));
    assert.equal(await m.wrap('k', () => null, { ttl: 60 }), null);
    assert.deepEqual(m.calls[1], ['SET', 'k', 'null', 'EX', '60']);
  });

  it('bloom: a definite miss returns null without touching the cache or fn', async () => {
    const m = new MockClient((a) => (a[0] === 'BF.EXISTS' ? 0 : ['hit', '1']));
    let called = false;
    const v = await m.wrap('user:9', () => {
      called = true;
      return 1;
    }, { ttl: 60, bloom: { key: 'users:ids', item: '9' } });
    assert.equal(v, null);
    assert.equal(called, false);
    assert.deepEqual(m.calls, [['BF.EXISTS', 'users:ids', '9']]);
  });

  it('bloom: a possible hit continues with the normal protocol; item defaults to the cache key', async () => {
    const m = new MockClient((a) => (a[0] === 'BF.EXISTS' ? 1 : ['hit', '{"id":9}']));
    assert.deepEqual(await m.wrap('user:9', () => null, { ttl: 60, bloom: { key: 'users' } }), { id: 9 });
    assert.deepEqual(m.calls, [['BF.EXISTS', 'users', 'user:9'], ['CACHE.GET', 'user:9']]);
  });
});
