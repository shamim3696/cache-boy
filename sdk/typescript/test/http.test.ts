import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { CacheBoyError } from '../src/errors.js';
import { CacheBoyHttp, type FetchLike } from '../src/http.js';

interface Req {
  url: string;
  headers: Record<string, string>;
  body: unknown;
}

function mockFetch(respond: (req: Req) => { status?: number; body: unknown } | Promise<{ status?: number; body: unknown }>): {
  fetch: (url: string, init: { headers: Record<string, string>; body: string }) => Promise<{ ok: boolean; status: number; text(): Promise<string> }>;
  reqs: Req[];
} {
  const reqs: Req[] = [];
  return {
    reqs,
    fetch: async (url, init) => {
      const req = { url, headers: init.headers, body: JSON.parse(init.body) as unknown };
      reqs.push(req);
      const r = await respond(req);
      const status = r.status ?? 200;
      const text = typeof r.body === 'string' ? r.body : JSON.stringify(r.body);
      return { ok: status >= 200 && status < 300, status, text: async () => text };
    },
  };
}

describe('CacheBoyHttp', () => {
  it('sends commands to /v1/command with auth and namespace headers', async () => {
    const f = mockFetch(() => ({ body: { result: 'OK' } }));
    const c = new CacheBoyHttp({ url: 'https://cache.example.com/', token: 's3cret', namespace: 'shop', fetch: f.fetch });
    assert.equal(await c.set('a', 1, { ex: 5 }), 'OK');
    assert.equal(f.reqs[0]!.url, 'https://cache.example.com/v1/command');
    assert.deepEqual(f.reqs[0]!.body, ['SET', 'a', '1', 'EX', '5']);
    assert.equal(f.reqs[0]!.headers['authorization'], 'Bearer s3cret');
    assert.equal(f.reqs[0]!.headers['x-cacheboy-namespace'], 'shop');
    assert.equal(f.reqs[0]!.headers['content-type'], 'application/json');
  });

  it('decodes results through the typed API (maps as objects)', async () => {
    const f = mockFetch((r) => {
      const cmd = (r.body as string[])[0];
      if (cmd === 'GET') return { body: { result: (r.body as string[])[1] === 'k' ? '{"a":1}' : null } };
      if (cmd === 'HGETALL') return { body: { result: { f: 'v' } } };
      if (cmd === 'QLEN') return { body: { result: { ready: 1, delayed: 0, inflight: 0, dead: 0 } } };
      if (cmd === 'RATELIMIT') return { body: { result: [1, 4, 0, 200] } };
      return { body: { result: null } };
    });
    const c = new CacheBoyHttp({ url: 'http://x', fetch: f.fetch });
    assert.deepEqual(await c.getJSON('k'), { a: 1 });
    assert.deepEqual(await c.hgetall('h'), { f: 'v' });
    assert.deepEqual(await c.queue('q').len(), { ready: 1, delayed: 0, inflight: 0, dead: 0 });
    assert.equal((await c.rateLimit('r', 5, '1s')).remaining, 4);
    assert.equal(await c.get('missing'), null);
    assert.equal(f.reqs[0]!.headers['authorization'], undefined);
  });

  it('turns {"error"} into CacheBoyError with the code', async () => {
    const f = mockFetch(() => ({ status: 200, body: { error: 'WRONGTYPE Operation against a key holding the wrong kind of value' } }));
    const c = new CacheBoyHttp({ url: 'http://x', fetch: f.fetch });
    await assert.rejects(c.get('k'), (e: unknown) => e instanceof CacheBoyError && e.code === 'WRONGTYPE' && e.command === 'GET');
  });

  it('handles errors sent with a non-2xx status', async () => {
    const f = mockFetch(() => ({ status: 400, body: { error: 'ERR syntax error' } }));
    const c = new CacheBoyHttp({ url: 'http://x', fetch: f.fetch });
    await assert.rejects(c.call('SET'), (e: unknown) => e instanceof CacheBoyError && e.code === 'ERR');
  });

  it('401 becomes NOAUTH', async () => {
    const c1 = new CacheBoyHttp({ url: 'http://x', fetch: mockFetch(() => ({ status: 401, body: '' })).fetch });
    await assert.rejects(c1.get('k'), (e: unknown) => e instanceof CacheBoyError && e.code === 'NOAUTH');
    const c2 = new CacheBoyHttp({ url: 'http://x', fetch: mockFetch(() => ({ status: 401, body: { error: 'unauthorized' } })).fetch });
    await assert.rejects(c2.get('k'), (e: unknown) => e instanceof CacheBoyError && e.code === 'NOAUTH');
    const c3 = new CacheBoyHttp({ url: 'http://x', fetch: mockFetch(() => ({ status: 401, body: { error: 'WRONGPASS invalid password' } })).fetch });
    await assert.rejects(c3.get('k'), (e: unknown) => e instanceof CacheBoyError && e.code === 'WRONGPASS');
  });

  it('non-JSON responses become HTTP errors; fetch failures too', async () => {
    const c = new CacheBoyHttp({ url: 'http://x', fetch: mockFetch(() => ({ status: 502, body: '<html>bad gateway</html>' })).fetch });
    await assert.rejects(c.get('k'), (e: unknown) => e instanceof CacheBoyError && e.code === 'HTTP');
    const c2 = new CacheBoyHttp({
      url: 'http://x',
      fetch: async () => {
        throw new TypeError('fetch failed');
      },
    });
    await assert.rejects(c2.get('k'), (e: unknown) => e instanceof CacheBoyError && e.code === 'HTTP');
  });

  it('pipeline() posts to /v1/pipeline and maps per-command errors', async () => {
    const f = mockFetch(() => ({ body: [{ result: 'OK' }, { result: 2 }, { error: 'WRONGTYPE nope' }] }));
    const c = new CacheBoyHttp({ url: 'http://x', fetch: f.fetch });
    const res = await c.pipeline().set('a', 'b').incr('n').hget('a', 'f').exec();
    assert.equal(f.reqs.length, 1);
    assert.equal(f.reqs[0]!.url, 'http://x/v1/pipeline');
    assert.deepEqual(f.reqs[0]!.body, [['SET', 'a', 'b'], ['INCR', 'n'], ['HGET', 'a', 'f']]);
    assert.deepEqual(res[0], [null, 'OK']);
    assert.deepEqual(res[1], [null, 2]);
    assert.equal(res[2]![0]?.code, 'WRONGTYPE');
  });

  it('multi() posts to /v1/multi; a whole-transaction error rejects exec()', async () => {
    const ok = mockFetch(() => ({ body: { result: ['OK', 3] } }));
    const c = new CacheBoyHttp({ url: 'http://x', fetch: ok.fetch });
    assert.deepEqual(await c.multi().set('a', 'b').incr('n').exec(), [[null, 'OK'], [null, 3]]);
    assert.equal(ok.reqs[0]!.url, 'http://x/v1/multi');

    const bad = mockFetch(() => ({ body: { error: 'EXECABORT Transaction discarded' } }));
    const c2 = new CacheBoyHttp({ url: 'http://x', fetch: bad.fetch });
    await assert.rejects(c2.multi().set('a', 'b').exec(), (e: unknown) => e instanceof CacheBoyError && e.code === 'EXECABORT');
  });

  it('rejects pub/sub, tracking and blocking-forever commands with UNSUPPORTED', async () => {
    const f = mockFetch(() => ({ body: { result: null } }));
    const c = new CacheBoyHttp({ url: 'http://x', fetch: f.fetch });
    const isUnsupported = (e: unknown): boolean => e instanceof CacheBoyError && e.code === 'UNSUPPORTED';
    await assert.rejects(c.subscribe('ch', () => undefined), isUnsupported);
    await assert.rejects(c.psubscribe('ch*', () => undefined), isUnsupported);
    await assert.rejects(c.keyWatch('user:*', () => undefined), isUnsupported);
    await assert.rejects(c.call('SUBSCRIBE', 'x'), isUnsupported);
    await assert.rejects(c.call('CLIENT', 'TRACKING', 'ON'), isUnsupported);
    await assert.rejects(c.call('MULTI'), isUnsupported);
    await assert.rejects(c.call('NAMESPACE', 'USE', 'x'), isUnsupported);
    await assert.rejects(c.blpop('list', 0), isUnsupported);
    await assert.rejects(c.queue('q').pop({ blockMs: 0 }), isUnsupported);
    assert.equal(f.reqs.length, 0);
    // Finite blocking is fine.
    assert.equal(await c.blpop('list', 1), null);
    assert.equal(await c.queue('q').pop({ blockMs: 500 }), null);
    assert.deepEqual(f.reqs[1]!.body, ['QPOP', 'q', 'BLOCK', '500']);
  });

  it('namespace() returns a client with another namespace header', async () => {
    const f = mockFetch(() => ({ body: { result: 'v' } }));
    const c = new CacheBoyHttp({ url: 'http://x', namespace: 'a', fetch: f.fetch });
    await c.namespace('b').get('k');
    assert.equal(f.reqs[0]!.headers['x-cacheboy-namespace'], 'b');
  });

  it('wrap() works over HTTP', async () => {
    const f = mockFetch((r) => ((r.body as string[])[0] === 'CACHE.GET' ? { body: { result: ['compute', null] } } : { body: { result: 'OK' } }));
    const c = new CacheBoyHttp({ url: 'http://x', fetch: f.fetch });
    assert.deepEqual(await c.wrap('k', async () => [1, 2], { ttl: 10, tags: ['t'] }), [1, 2]);
    assert.deepEqual(f.reqs[1]!.body, ['SET', 'k', '[1,2]', 'EX', '10', 'TAGS', 't']);
  });

  it('times out with TIMEOUT', async () => {
    const c = new CacheBoyHttp({
      url: 'http://x',
      timeoutMs: 20,
      fetch: ((_u, init) =>
        new Promise<never>((_res, rej) => {
          init.signal?.addEventListener('abort', () => rej(init.signal!.reason));
        })) as FetchLike,
    });
    await assert.rejects(c.get('k'), (e: unknown) => e instanceof CacheBoyError && e.code === 'TIMEOUT');
  });
});
