import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { CacheBoyError } from '../src/errors.js';
import type { Job } from '../src/features/queue.js';
import { MockClient, delay, until } from './helpers.js';

describe('typed commands (mock transport)', () => {
  it('set encodes options in order with TAGS last, objects as JSON', async () => {
    const m = new MockClient(() => 'OK');
    await m.set('k', { a: 1 }, { ex: 10, nx: true, get: true, tags: ['t1', 't2'] });
    await m.set('n', 5, { px: 100, xx: true });
    await m.set('b', Buffer.from('raw'), { keepTtl: true });
    assert.deepEqual(m.calls, [
      ['SET', 'k', '{"a":1}', 'EX', '10', 'NX', 'GET', 'TAGS', 't1', 't2'],
      ['SET', 'n', '5', 'PX', '100', 'XX'],
      ['SET', 'b', 'raw', 'KEEPTTL'],
    ]);
  });

  it('getJSON parses and runs the parse hook', async () => {
    const m = new MockClient(() => '{"id":1,"name":"x"}');
    const v = await m.getJSON<{ id: number }>('k', {
      parse: (x) => {
        const o = x as { id: unknown };
        if (typeof o.id !== 'number') throw new Error('bad');
        return { id: o.id };
      },
    });
    assert.deepEqual(v, { id: 1 });
    const none = new MockClient(() => null);
    assert.equal(await none.getJSON('k'), null);
  });

  it('hgetall accepts RESP3 maps and RESP2 flat arrays', async () => {
    assert.deepEqual(await new MockClient(() => ({ a: '1', b: '2' })).hgetall('h'), { a: '1', b: '2' });
    assert.deepEqual(await new MockClient(() => ['a', '1', 'b', '2']).hgetall('h'), { a: '1', b: '2' });
  });

  it('zscore and zrangeWithScores handle doubles and strings', async () => {
    assert.equal(await new MockClient(() => 1.5).zscore('z', 'm'), 1.5);
    assert.equal(await new MockClient(() => '2.5').zscore('z', 'm'), 2.5);
    assert.equal(await new MockClient(() => null).zscore('z', 'm'), null);
    assert.deepEqual(await new MockClient(() => [['a', 1], ['b', 2]]).zrangeWithScores('z', 0, -1), [
      { member: 'a', score: 1 },
      { member: 'b', score: 2 },
    ]);
    assert.deepEqual(await new MockClient(() => ['a', '1', 'b', '2']).zrangeWithScores('z', 0, -1), [
      { member: 'a', score: 1 },
      { member: 'b', score: 2 },
    ]);
  });

  it('rateLimit maps the reply array', async () => {
    const m = new MockClient(() => [1, 99, 0, 600]);
    assert.deepEqual(await m.rateLimit('api:1', 100, '1m', { cost: 2 }), { allowed: true, remaining: 99, retryAfterMs: 0, resetAfterMs: 600 });
    assert.deepEqual(m.calls[0], ['RATELIMIT', 'api:1', '100', '1m', 'COST', '2']);
    const denied = new MockClient(() => [0, 0, 250, 60000]);
    assert.equal((await denied.rateLimit('k', 1, 60000)).allowed, false);
  });

  it('lock returns a Lock with release/extend, or null', async () => {
    const m = new MockClient((a) => (a[0] === 'LOCK' ? 7 : 1));
    const lock = await m.lock('job', 5000, { waitMs: 100, owner: 'w1' });
    assert.ok(lock);
    assert.equal(lock.token, 7);
    assert.equal(await lock.extend(10000), true);
    assert.equal(await lock.release(), true);
    assert.equal(await lock.release(), false); // only once
    assert.deepEqual(m.calls, [
      ['LOCK', 'job', '5000', 'WAIT', '100', 'OWNER', 'w1'],
      ['LOCK.EXTEND', 'job', '7', '10000'],
      ['UNLOCK', 'job', '7'],
    ]);
    assert.equal(await new MockClient(() => null).lock('job', 1000), null);
  });

  it('withLock releases after fn, even if it throws; LOCKED when not acquired', async () => {
    const m = new MockClient((a) => (a[0] === 'LOCK' ? 3 : 1));
    assert.equal(await m.withLock('n', 1000, (l) => l.token * 2), 6);
    await assert.rejects(m.withLock('n', 1000, () => Promise.reject(new Error('boom'))), /boom/);
    assert.equal(m.names().filter((n) => n === 'UNLOCK').length, 2);
    await assert.rejects(new MockClient(() => null).withLock('n', 1000, () => 1), (e: unknown) => e instanceof CacheBoyError && e.code === 'LOCKED');
  });

  it('lockInfo maps the map reply', async () => {
    const m = new MockClient(() => ({ token: 4, owner: 'me', ttl_ms: 900 }));
    assert.deepEqual(await m.lockInfo('n'), { token: 4, owner: 'me', ttlMs: 900 });
  });

  it('tags', async () => {
    const m = new MockClient((a) => (a[0] === 'TAG.KEYS' || a[0] === 'TAG.OF' || a[0] === 'TAG.LIST' ? ['x', 'y'] : 2));
    assert.equal(await m.tags.add('k', 'a', 'b'), 2);
    assert.equal(await m.tags.remove('k', 'a'), 2);
    assert.deepEqual(await m.tags.keys('a'), ['x', 'y']);
    assert.deepEqual(await m.tags.of('k'), ['x', 'y']);
    assert.equal(await m.tags.invalidate('a', 'b'), 2);
    assert.deepEqual(await m.tags.list(), ['x', 'y']);
    assert.deepEqual(m.calls.map((c) => c[0]), ['TAG.ADD', 'TAG.REMOVE', 'TAG.KEYS', 'TAG.OF', 'TAG.INVALIDATE', 'TAG.LIST']);
  });

  it('json', async () => {
    const m = new MockClient((a) => {
      switch (a[0]) {
        case 'JSON.SET':
          return 'OK';
        case 'JSON.GET':
          return a.length === 2 ? '{"a":[1,2]}' : '[[1,2]]';
        case 'JSON.NUMINCRBY':
          return '[3]';
        case 'JSON.ARRAPPEND':
          return [3];
        case 'JSON.DEL':
          return 1;
      }
      return null;
    });
    assert.equal(await m.json.set('doc', '$', { a: [1, 2] }, { nx: true }), true);
    assert.deepEqual(await m.json.get('doc'), { a: [1, 2] });
    assert.deepEqual(await m.json.get('doc', '.a'), [[1, 2]]);
    assert.deepEqual(await m.json.numIncrBy('doc', '.n', 2), [3]);
    assert.deepEqual(await m.json.arrAppend('doc', '.a', 3), [3]);
    assert.equal(await m.json.del('doc', '.a'), 1);
    assert.deepEqual(m.calls[0], ['JSON.SET', 'doc', '$', '{"a":[1,2]}', 'NX']);
    assert.deepEqual(m.calls[4], ['JSON.ARRAPPEND', 'doc', '.a', '3']);
    assert.equal(await new MockClient(() => null).json.set('d', '$', 1, { xx: true }), false);
  });

  it('time series', async () => {
    const m = new MockClient((a) => {
      if (a[0] === 'TS.ADD') return 1000;
      if (a[0] === 'TS.GET') return [1000, 1.5];
      if (a[0] === 'TS.RANGE') return [[1000, '1.5'], [2000, 2]];
      return 'OK';
    });
    await m.ts.create('t', { retentionMs: 60000, labels: { host: 'a' } });
    assert.equal(await m.ts.add('t', '*', 1.5), 1000);
    assert.deepEqual(await m.ts.get('t'), { timestamp: 1000, value: 1.5 });
    assert.deepEqual(await m.ts.range('t', '-', '+', { aggregation: { type: 'avg', bucketMs: 1000 } }), [
      { timestamp: 1000, value: 1.5 },
      { timestamp: 2000, value: 2 },
    ]);
    assert.deepEqual(m.calls[0], ['TS.CREATE', 't', 'RETENTION', '60000', 'LABELS', 'host', 'a']);
    assert.deepEqual(m.calls[3], ['TS.RANGE', 't', '-', '+', 'AGGREGATION', 'avg', '1000']);
    assert.equal(await new MockClient(() => []).ts.get('t'), null);
  });

  it('vector', async () => {
    const m = new MockClient((a) => {
      if (a[0] === 'VSEARCH') return [['a', 0.99], ['b', '0.5']];
      if (a[0] === 'VGET') return ['0.1', '0.2'];
      return 1;
    });
    assert.equal(await m.vector.add('v', 'a', [0.1, 0.2], { attr: { lang: 'en' } }), true);
    assert.deepEqual(await m.vector.search('v', 2, [0.1, 0.2], { filter: { lang: 'en' } }), [
      { id: 'a', score: 0.99 },
      { id: 'b', score: 0.5 },
    ]);
    assert.deepEqual(await m.vector.get('v', 'a'), [0.1, 0.2]);
    assert.equal(await m.vector.del('v', 'a'), true);
    assert.deepEqual(m.calls[0], ['VADD', 'v', 'a', '0.1', '0.2', 'ATTR', '{"lang":"en"}']);
    assert.deepEqual(m.calls[1], ['VSEARCH', 'v', '2', '0.1', '0.2', 'FILTER', 'lang=en']);
  });

  it('history and scripting', async () => {
    const m = new MockClient((a) => {
      if (a[0] === 'HISTORY') return [[2, 2000, 'b'], [1, 1000, null]];
      if (a[0] === 'GETVERSION') return 'a';
      if (a[0] === 'SCRIPT') return 'abc123';
      if (a[0] === 'EVALSHA') return new CacheBoyError('NOSCRIPT No matching script');
      return 5;
    });
    assert.deepEqual(await m.history('k', { count: 2 }), [
      { version: 2, timestampMs: 2000, value: 'b' },
      { version: 1, timestampMs: 1000, value: null },
    ]);
    assert.equal(await m.getVersion('k', 1), 'a');
    assert.equal(await m.eval("return cb.call('INCRBY', KEYS[0], ARGV[0])", ['c'], [5]), 5);
    assert.deepEqual(m.calls[2], ['EVAL', "return cb.call('INCRBY', KEYS[0], ARGV[0])", '1', 'c', '5']);
    assert.equal(await m.scriptLoad('return 1'), 'abc123');
    await assert.rejects(m.evalsha('abc', [], []), (e: unknown) => e instanceof CacheBoyError && e.code === 'NOSCRIPT');
  });

  it('pipeline chains typed methods and returns [err, typedResult]', async () => {
    const m = new MockClient((a) => {
      if (a[0] === 'GET') return '{"x":1}';
      if (a[0] === 'INCR') return 2;
      if (a[0] === 'RATELIMIT') return [1, 9, 0, 100];
      return new CacheBoyError('WRONGTYPE nope');
    });
    const res = await m.pipeline().getJSON('a').incr('n').rateLimit('r', 10, 1000).hget('h', 'f').exec();
    assert.deepEqual(res[0], [null, { x: 1 }]);
    assert.deepEqual(res[1], [null, 2]);
    assert.deepEqual(res[2], [null, { allowed: true, remaining: 9, retryAfterMs: 0, resetAfterMs: 100 }]);
    assert.ok(res[3]![0] instanceof CacheBoyError);
    assert.equal(res[3]![0]!.code, 'WRONGTYPE');
  });

  it('pipeline rejects multi-round-trip helpers', () => {
    const m = new MockClient();
    const p = m.pipeline() as unknown as { wrap: (...a: unknown[]) => unknown };
    assert.throws(() => p.wrap('k', () => 1, { ttl: 1 }), (e: unknown) => e instanceof CacheBoyError && e.code === 'UNSUPPORTED');
  });
});

describe('wrap() (mock transport)', () => {
  it('hit: parses the cached JSON without calling fn', async () => {
    const m = new MockClient(() => ['hit', '{"v":42}']);
    let called = false;
    const v = await m.wrap('k', async () => {
      called = true;
      return { v: 0 };
    }, { ttl: 60 });
    assert.deepEqual(v, { v: 42 });
    assert.equal(called, false);
    assert.deepEqual(m.calls, [['CACHE.GET', 'k']]);
  });

  it('hit: runs the parse hook', async () => {
    const m = new MockClient(() => ['hit', '{"v":42}']);
    const v = await m.wrap('k', async () => 0, { ttl: 60, parse: (x) => (x as { v: number }).v + 1 });
    assert.equal(v, 43);
  });

  it('compute: runs fn, then SET with EX and TAGS', async () => {
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['compute', null] : 'OK'));
    const v = await m.wrap('k', async () => ({ fresh: true }), { ttl: 30, tags: ['user:1'], lockMs: 2000, waitMs: 500 });
    assert.deepEqual(v, { fresh: true });
    assert.deepEqual(m.calls, [
      ['CACHE.GET', 'k', 'LOCK', '2000', 'WAIT', '500'],
      ['SET', 'k', '{"fresh":true}', 'EX', '30', 'TAGS', 'user:1'],
    ]);
  });

  it('compute + fn throws: CACHE.RELEASE, then rethrow', async () => {
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['compute', null] : 1));
    await assert.rejects(m.wrap('k', async () => {
      throw new Error('db down');
    }, { ttl: 30 }), /db down/);
    assert.deepEqual(m.names(), ['CACHE.GET', 'CACHE.RELEASE']);
  });

  it('timeout: runs fn and SETs', async () => {
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['timeout', null] : 'OK'));
    assert.equal(await m.wrap('k', () => 'x', { ttl: 5 }), 'x');
    assert.deepEqual(m.calls[1], ['SET', 'k', '"x"', 'EX', '5']);
  });

  it('timeout + fn throws: no CACHE.RELEASE (we never held the lock)', async () => {
    const m = new MockClient(() => ['timeout', null]);
    await assert.rejects(m.wrap('k', () => Promise.reject(new Error('x')), { ttl: 5 }));
    assert.deepEqual(m.names(), ['CACHE.GET']);
  });

  it('a failed SET releases the lock; onSetError swallows it', async () => {
    const m = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['compute', null] : a[0] === 'SET' ? new CacheBoyError('OOM full') : 1));
    let seen: unknown;
    assert.equal(await m.wrap('k', () => 1, { ttl: 5, onSetError: (e) => (seen = e) }), 1);
    assert.ok(seen instanceof CacheBoyError);
    assert.deepEqual(m.names(), ['CACHE.GET', 'SET', 'CACHE.RELEASE']);
    const m2 = new MockClient((a) => (a[0] === 'CACHE.GET' ? ['compute', null] : a[0] === 'SET' ? new CacheBoyError('OOM full') : 1));
    await assert.rejects(m2.wrap('k', () => 1, { ttl: 5 }), /OOM/);
  });

  it('stampede: concurrent callers, one computes, the others get the hit', async () => {
    // Simulates the server: the first CACHE.GET computes, the others wait for the SET.
    let value: string | null = null;
    let computing = false;
    const waiters: Array<() => void> = [];
    const m = new MockClient(async (a) => {
      if (a[0] === 'CACHE.GET') {
        if (value !== null) return ['hit', value];
        if (!computing) {
          computing = true;
          return ['compute', null];
        }
        await new Promise<void>((r) => waiters.push(r));
        return ['hit', value];
      }
      if (a[0] === 'SET') {
        value = a[2]!;
        for (const w of waiters.splice(0)) w();
        return 'OK';
      }
      return null;
    });
    let computes = 0;
    const fn = async (): Promise<number> => {
      computes++;
      await delay(20);
      return 99;
    };
    const results = await Promise.all(Array.from({ length: 10 }, () => m.wrap('hot', fn, { ttl: 60 })));
    assert.deepEqual(results, Array(10).fill(99));
    assert.equal(computes, 1);
  });
});

describe('queue (mock transport)', () => {
  it('push JSON-encodes payloads with options; pop decodes', async () => {
    const m = new MockClient((a) => (a[0] === 'QPUSH' ? 'job-1' : ['job-1', '{"email":"a@b.c"}', 1]));
    const q = m.queue<{ email: string }>('mail');
    assert.equal(await q.push({ email: 'a@b.c' }, { delayMs: 1000, retries: 5, id: 'welcome:1' }), 'job-1');
    const job = await q.pop({ visibilityMs: 5000 });
    assert.ok(job);
    assert.deepEqual(job.payload, { email: 'a@b.c' });
    assert.equal(job.attempt, 1);
    assert.deepEqual(m.calls, [
      ['QPUSH', 'mail', '{"email":"a@b.c"}', 'DELAY', '1000', 'RETRIES', '5', 'ID', 'welcome:1'],
      ['QPOP', 'mail', 'VISIBILITY', '5000'],
    ]);
  });

  it('pop returns null when empty; non-JSON payloads come back raw', async () => {
    assert.equal(await new MockClient(() => null).queue('q').pop(), null);
    const job = await new MockClient(() => ['1', 'plain text', 2]).queue('q').pop();
    assert.equal(job!.payload, 'plain text');
    assert.equal(job!.attempt, 2);
  });

  it('ack/nack send QACK/QNACK once', async () => {
    const m = new MockClient((a) => (a[0] === 'QPOP' ? ['7', '1', 1] : 1));
    const q = m.queue('q');
    const j1 = (await q.pop())!;
    assert.equal(await j1.ack(), true);
    assert.equal(await j1.nack(), false);
    const j2 = (await q.pop())!;
    await j2.nack(500);
    assert.deepEqual(m.calls.slice(1).filter((c) => c[0] !== 'QPOP'), [
      ['QACK', 'q', '7'],
      ['QNACK', 'q', '7', 'DELAY', '500'],
    ]);
  });

  it('len, dead, retryDead, purge', async () => {
    const m = new MockClient((a) => {
      if (a[0] === 'QLEN') return { ready: 1, delayed: 2, inflight: 3, dead: 4 };
      if (a[0] === 'QDEAD') return [['9', '{"x":1}', 4]];
      return 4;
    });
    const q = m.queue('q');
    assert.deepEqual(await q.len(), { ready: 1, delayed: 2, inflight: 3, dead: 4 });
    assert.deepEqual(await q.dead(), [{ id: '9', payload: { x: 1 }, attempts: 4 }]);
    assert.equal(await q.retryDead(), 4);
    assert.equal(await q.purge(), 4);
  });

  /** An in-memory queue server for worker tests. */
  function queueServer(jobs: string[]): MockClient & { acked: string[]; nacked: string[] } {
    const ready = jobs.map((p, i) => ({ id: String(i + 1), payload: p }));
    const acked: string[] = [];
    const nacked: string[] = [];
    const m = new MockClient(async (a) => {
      switch (a[0]) {
        case 'QPOP': {
          const j = ready.shift();
          if (!j) {
            await delay(5); // short "block"
            return null;
          }
          return [j.id, j.payload, 1];
        }
        case 'QACK':
          acked.push(a[2]!);
          return 1;
        case 'QNACK':
          nacked.push(a[2]!);
          return 1;
      }
      return null;
    }) as MockClient & { acked: string[]; nacked: string[] };
    m.acked = acked;
    m.nacked = nacked;
    return m;
  }

  it('process: acks on success, nacks on throw, reports errors', async () => {
    const m = queueServer(['{"n":1}', '{"n":2}', '{"n":3}', '{"n":4}']);
    const errors: unknown[] = [];
    const worker = m.queue<{ n: number }>('q').process(
      async (job) => {
        if (job.payload.n % 2 === 0) throw new Error(`fail ${job.payload.n}`);
      },
      { blockMs: 10, onError: (e) => errors.push(e), nackDelayMs: 250 },
    );
    await until(() => m.acked.length + m.nacked.length === 4);
    await worker.stop();
    assert.deepEqual(m.acked.sort(), ['1', '3']);
    assert.deepEqual(m.nacked.sort(), ['2', '4']);
    assert.equal(errors.length, 2);
    assert.equal(worker.processed, 2);
    assert.equal(worker.failed, 2);
    assert.ok(m.calls.some((c) => c[0] === 'QNACK' && c[3] === 'DELAY' && c[4] === '250'));
    assert.ok(m.calls.filter((c) => c[0] === 'QPOP').every((c) => c.includes('BLOCK')));
  });

  it('process: respects concurrency', async () => {
    const m = queueServer(Array.from({ length: 12 }, (_, i) => String(i)));
    let running = 0;
    let peak = 0;
    const worker = m.queue('q').process(
      async () => {
        running++;
        peak = Math.max(peak, running);
        await delay(15);
        running--;
      },
      { concurrency: 3, blockMs: 10 },
    );
    await until(() => m.acked.length === 12);
    await worker.stop();
    assert.equal(peak, 3);
  });

  it('process: stop() waits for in-flight handlers and takes no new jobs', async () => {
    const m = queueServer(['a', 'b', 'c']);
    let finished = 0;
    let started = 0;
    const worker = m.queue('q').process(
      async () => {
        started++;
        await delay(40);
        finished++;
      },
      { blockMs: 10 },
    );
    await until(() => started === 1);
    await worker.stop();
    assert.equal(finished, started);
    assert.equal(worker.running, false);
    const pops = m.calls.filter((c) => c[0] === 'QPOP').length;
    await delay(30);
    assert.equal(m.calls.filter((c) => c[0] === 'QPOP').length, pops);
    assert.ok(started < 3);
  });

  it('process: a handler that acks itself is not acked twice', async () => {
    const m = queueServer(['x']);
    const worker = m.queue('q').process(async (job: Job<unknown>) => {
      await job.ack();
    }, { blockMs: 10 });
    await until(() => m.acked.length === 1);
    await delay(10);
    await worker.stop();
    assert.equal(m.acked.length, 1);
  });

  it('process: fetch errors are reported and retried', async () => {
    let fails = 2;
    const acked: string[] = [];
    const m = new MockClient(async (a) => {
      if (a[0] === 'QPOP') {
        if (fails-- > 0) return new CacheBoyError('ERR temporary');
        if (fails === -1) return ['1', '"p"', 1];
        await delay(5);
        return null;
      }
      if (a[0] === 'QACK') acked.push(a[2]!);
      return 1;
    });
    const errors: unknown[] = [];
    const worker = m.queue('q').process(() => undefined, { blockMs: 10, onError: (e) => errors.push(e) });
    await until(() => acked.length === 1, 3000);
    await worker.stop();
    assert.equal(errors.length, 2);
  });
});
