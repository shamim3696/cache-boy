import assert from 'node:assert/strict';
import { after, describe, it } from 'node:test';
import { CacheBoy, parseUrl } from '../src/client.js';
import { Connection, backoff } from '../src/connection.js';
import { CacheBoyError } from '../src/errors.js';
import { FakeServer, R, delay, handshake, until } from './helpers.js';

const servers: FakeServer[] = [];
async function server(handler: ConstructorParameters<typeof FakeServer>[0]): Promise<FakeServer> {
  const s = new FakeServer(handler);
  await s.listen();
  servers.push(s);
  return s;
}
after(async () => {
  await Promise.all(servers.map((s) => s.close()));
});

/** A small in-memory key/value server with tracking (enough for client tests). */
function kvHandler(store: Map<string, string>) {
  return (args: string[]): string | undefined => {
    const h = handshake(args);
    if (h !== undefined) return h;
    switch (args[0]!.toUpperCase()) {
      case 'GET':
        return R.bulk(store.get(args[1]!) ?? null);
      case 'SET':
        store.set(args[1]!, args[2]!);
        return R.ok;
      case 'PING':
        return R.simple('PONG');
      case 'INCR': {
        const n = Number(store.get(args[1]!) ?? '0') + 1;
        store.set(args[1]!, String(n));
        return R.int(n);
      }
    }
    return R.err(`ERR unknown command '${args[0]}'`);
  };
}

describe('Connection', () => {
  it('handshakes with HELLO 3 AUTH SETNAME, then NAMESPACE USE, before user commands', async () => {
    const s = await server(kvHandler(new Map()));
    const c = new Connection({ port: s.port, password: 'pw', username: 'app', clientName: 'svc', namespace: 'shop' });
    assert.equal(await c.send(['PING']), 'PONG');
    assert.deepEqual(s.commands.slice(0, 3), [
      ['HELLO', '3', 'AUTH', 'app', 'pw', 'SETNAME', 'svc'],
      ['NAMESPACE', 'USE', 'shop'],
      ['PING'],
    ]);
    assert.equal(c.serverInfo?.['server'], 'cache-boy');
    c.disconnect();
  });

  it('uses user "default" when only a password is given', async () => {
    const s = await server(kvHandler(new Map()));
    const c = new Connection({ port: s.port, password: 'pw' });
    await c.send(['PING']);
    assert.deepEqual(s.commands[0], ['HELLO', '3', 'AUTH', 'default', 'pw']);
    c.disconnect();
  });

  it('auto-pipelines commands issued in the same tick into one write', async () => {
    const s = await server(kvHandler(new Map()));
    const c = new Connection({ port: s.port });
    await c.connect();
    await until(() => s.dataEvents[0] === 1); // handshake
    const replies = await Promise.all(Array.from({ length: 50 }, (_, i) => c.send(['SET', `k${i}`, String(i)])));
    assert.equal(replies.length, 50);
    assert.equal(s.dataEvents[0], 2); // one more write for all 50 commands
    c.disconnect();
  });

  it('matches replies FIFO and rejects error replies with CacheBoyError', async () => {
    const s = await server(kvHandler(new Map([['a', '1']])));
    const c = new Connection({ port: s.port });
    const [a, b, e] = await Promise.allSettled([c.send(['GET', 'a']), c.send(['INCR', 'n']), c.send(['NOPE'])]);
    assert.deepEqual(a, { status: 'fulfilled', value: '1' });
    assert.deepEqual(b, { status: 'fulfilled', value: 1 });
    assert.equal(e.status, 'rejected');
    const err = (e as PromiseRejectedResult).reason as CacheBoyError;
    assert.ok(err instanceof CacheBoyError);
    assert.equal(err.code, 'ERR');
    assert.equal(err.command, 'NOPE');
    c.disconnect();
  });

  it('per-command timeout rejects with TIMEOUT and keeps later replies aligned', async () => {
    let first = true;
    const s = await server((args, conn) => {
      const h = handshake(args);
      if (h !== undefined) return h;
      if (args[0] === 'SLOW' && first) {
        first = false;
        setTimeout(() => conn.write(R.simple('late')), 80);
        return;
      }
      if (args[0] === 'GET') {
        // Reply only after SLOW replied, to keep FIFO order.
        setTimeout(() => conn.write(R.bulk('v')), 120);
        return;
      }
      return R.ok;
    });
    const c = new Connection({ port: s.port });
    await c.connect();
    const slow = c.send(['SLOW'], { timeoutMs: 20 });
    const get = c.send(['GET', 'k']);
    await assert.rejects(slow, (e: unknown) => e instanceof CacheBoyError && e.code === 'TIMEOUT');
    assert.equal(await get, 'v');
    c.disconnect();
  });

  it('queues commands while connecting and rejects them when the offline queue is disabled', async () => {
    const s = await server(kvHandler(new Map([['k', 'v']])));
    const c = new Connection({ port: s.port, lazyConnect: true });
    assert.equal(c.status, 'wait');
    assert.equal(await c.send(['GET', 'k']), 'v');
    c.disconnect();
    const c2 = new Connection({ port: s.port, enableOfflineQueue: false });
    await assert.rejects(c2.send(['GET', 'k']), (e: unknown) => e instanceof CacheBoyError && e.code === 'CONNECTION_CLOSED');
    c2.disconnect();
  });

  it('reconnects, replays safe in-flight commands and rejects unsafe ones', async () => {
    let drop = true;
    const s = await server((args, conn) => {
      const h = handshake(args);
      if (h !== undefined) return h;
      if (drop && (args[0] === 'GET' || args[0] === 'INCR')) {
        // Swallow the command, then kill the connection.
        if (args[0] === 'INCR') {
          drop = false;
          setTimeout(() => conn.socket.destroy(), 10);
        }
        return;
      }
      if (args[0] === 'GET') return R.bulk('after-reconnect');
      return R.int(1);
    });
    const events: string[] = [];
    const c = new Connection({ port: s.port, reconnect: { baseDelayMs: 5, maxDelayMs: 20 } });
    c.on('reconnecting', () => events.push('reconnecting'));
    c.on('ready', () => events.push('ready'));
    c.on('close', () => events.push('close'));
    await c.connect();
    const get = c.send(['GET', 'k']);
    const incr = c.send(['INCR', 'n']);
    await assert.rejects(incr, (e: unknown) => e instanceof CacheBoyError && e.code === 'CONNECTION_LOST' && /not safe to retry/.test(e.message));
    assert.equal(await get, 'after-reconnect');
    assert.deepEqual(events, ['ready', 'close', 'reconnecting', 'ready']);
    // Handshake ran again on the new connection.
    assert.equal(s.conns.length, 2);
    assert.equal(s.conns[1]!.commands[0]![0], 'HELLO');
    c.disconnect();
  });

  it("inflightOnReconnect: 'reject' rejects even safe commands", async () => {
    let dropped = false;
    const s = await server((args, conn) => {
      const h = handshake(args);
      if (h !== undefined) return h;
      if (!dropped) {
        dropped = true;
        setTimeout(() => conn.socket.destroy(), 5);
        return;
      }
      return R.bulk('x');
    });
    const c = new Connection({ port: s.port, inflightOnReconnect: 'reject', reconnect: { baseDelayMs: 5 } });
    await assert.rejects(c.send(['GET', 'k']), (e: unknown) => e instanceof CacheBoyError && e.code === 'CONNECTION_LOST');
    assert.equal(await c.send(['GET', 'k']), 'x');
    c.disconnect();
  });

  it('gives up after maxAttempts and rejects queued commands', async () => {
    const s = await server(() => undefined);
    const port = s.port;
    await s.close();
    servers.splice(servers.indexOf(s), 1);
    const c = new Connection({ port, reconnect: { baseDelayMs: 1, maxDelayMs: 5, maxAttempts: 2 } });
    const ended = new Promise((r) => c.once('end', r));
    await assert.rejects(c.send(['PING']), (e: unknown) => e instanceof CacheBoyError && e.code === 'CONNECTION_CLOSED');
    await ended;
    assert.equal(c.status, 'closed');
  });

  it('fails queued commands fast when the handshake is rejected (WRONGPASS)', async () => {
    const s = await server((args) => (args[0] === 'HELLO' ? R.err('WRONGPASS invalid username-password pair') : R.ok));
    const c = new Connection({ port: s.port, password: 'bad', reconnect: { baseDelayMs: 50 } });
    c.on('error', () => undefined);
    await assert.rejects(c.send(['PING']), (e: unknown) => e instanceof CacheBoyError && e.code === 'WRONGPASS');
    c.disconnect();
  });

  it('quit() sends QUIT and closes without reconnecting', async () => {
    const s = await server(kvHandler(new Map()));
    const c = new Connection({ port: s.port });
    await c.connect();
    const ended = new Promise((r) => c.once('end', r));
    // The fake server doesn't close on QUIT; the client ends the socket itself.
    await c.quit();
    await ended;
    assert.equal(c.status, 'closed');
    await assert.rejects(c.send(['PING']), (e: unknown) => e instanceof CacheBoyError && e.code === 'CONNECTION_CLOSED');
  });

  it('backoff grows exponentially with jitter, capped', () => {
    for (let i = 0; i < 100; i++) {
      const d1 = backoff(1, 100, 2000);
      assert.ok(d1 >= 50 && d1 <= 100);
      const d5 = backoff(5, 100, 2000);
      assert.ok(d5 >= 800 && d5 <= 1600);
      const d20 = backoff(20, 100, 2000);
      assert.ok(d20 >= 1000 && d20 <= 2000);
    }
  });
});

describe('CacheBoy client over a fake server', () => {
  it('parses redis:// URLs', () => {
    assert.deepEqual(parseUrl('redis://u:p%40ss@example.com:6380/2?namespace=shop&name=api'), {
      host: 'example.com',
      port: 6380,
      username: 'u',
      password: 'p@ss',
      db: 2,
      namespace: 'shop',
      clientName: 'api',
    });
    assert.deepEqual(parseUrl('rediss://:secret@h'), { tls: true, host: 'h', password: 'secret' });
  });

  it('client-side caching: CLIENT TRACKING ON, local hits, push invalidation', async () => {
    const store = new Map([['k', 'v1']]);
    let tracker: ((s: string) => void) | undefined;
    const kv = kvHandler(store);
    const s = await server((args, conn) => {
      if (args[0] === 'CLIENT') {
        tracker = (x) => conn.write(x);
        return R.ok;
      }
      return kv(args);
    });
    const c = new CacheBoy({ port: s.port, clientCache: { maxEntries: 100 } });
    assert.equal(await c.get('k'), 'v1');
    assert.equal(await c.get('k'), 'v1');
    assert.equal(await c.get('k'), 'v1');
    assert.deepEqual(s.commands.map((x) => x[0]), ['HELLO', 'CLIENT', 'GET']);
    assert.deepEqual(s.commands[1], ['CLIENT', 'TRACKING', 'ON']);
    assert.deepEqual(c.cacheStats(), { hits: 2, misses: 1, size: 1, invalidations: 0 });

    store.set('k', 'v2');
    tracker!(R.push(R.bulk('invalidate'), R.arr(R.bulk('k'))));
    await until(() => c.cacheStats().invalidations === 1);
    assert.equal(await c.get('k'), 'v2');
    assert.equal(c.cacheStats().size, 1);

    tracker!(R.push(R.bulk('invalidate'), R.nil));
    await until(() => c.cacheStats().size === 0);
    c.disconnect();
  });

  it('client-side caching: an invalidation racing an in-flight GET is not cached', async () => {
    const s = await server((args, conn) => {
      const h = handshake(args);
      if (h !== undefined) return h;
      if (args[0] === 'GET') {
        // The invalidation reaches the client before the (stale) GET reply.
        conn.write(R.push(R.bulk('invalidate'), R.arr(R.bulk('k'))));
        return R.bulk('stale');
      }
      return R.ok;
    });
    const c = new CacheBoy({ port: s.port, clientCache: true });
    assert.equal(await c.get('k'), 'stale');
    assert.equal(c.cacheStats().size, 0);
    await c.get('k');
    assert.equal(s.commands.filter((x) => x[0] === 'GET').length, 2);
    c.disconnect();
  });

  it('client-side caching: namespaced invalidation keys are mapped back', async () => {
    let push: ((s: string) => void) | undefined;
    const s = await server((args, conn) => {
      push = (x) => conn.write(x);
      return handshake(args) ?? (args[0] === 'GET' ? R.bulk('v') : R.ok);
    });
    const c = new CacheBoy({ port: s.port, namespace: 'shop', clientCache: true });
    await c.get('cart');
    assert.equal(c.cacheStats().size, 1);
    push!(R.push(R.bulk('invalidate'), R.arr(R.bulk('\x00shop\x00cart'))));
    await until(() => c.cacheStats().size === 0);
    c.disconnect();
  });

  it('client-side caching: own writes evict immediately; reconnect clears', async () => {
    const store = new Map([['k', 'v1']]);
    const s = await server(kvHandler(store));
    const c = new CacheBoy({ port: s.port, clientCache: true, reconnect: { baseDelayMs: 5 } });
    await c.get('k');
    await c.set('k', 'v2');
    assert.equal(c.cacheStats().size, 0);
    assert.equal(await c.get('k'), 'v2');
    await c.call('INCR', 'k2');
    await c.get('k');
    assert.equal(c.cacheStats().size, 1);
    s.dropAll();
    await until(() => c.cacheStats().size === 0);
    c.disconnect();
  });

  it('multi() wraps commands in MULTI/EXEC', async () => {
    const s = await server((args) => {
      const h = handshake(args);
      if (h !== undefined) return h;
      if (args[0] === 'MULTI') return R.ok;
      if (args[0] === 'EXEC') return R.arr(R.ok, R.int(5), R.err('WRONGTYPE bad'));
      return R.simple('QUEUED');
    });
    const c = new CacheBoy({ port: s.port });
    const res = await c.multi().set('a', 'b').incr('n').hget('a', 'f').exec();
    assert.deepEqual(s.commands.slice(1).map((x) => x[0]), ['MULTI', 'SET', 'INCR', 'HGET', 'EXEC']);
    assert.deepEqual(res[0], [null, 'OK']);
    assert.deepEqual(res[1], [null, 5]);
    assert.equal(res[2]![0]?.code, 'WRONGTYPE');
    c.disconnect();
  });

  it('subscribe/psubscribe/keyWatch use a dedicated connection and dispatch pushes', async () => {
    const s = await server((args, conn) => {
      const h = handshake(args);
      if (h !== undefined) return h;
      const kind = args[0]!.toLowerCase();
      if (['subscribe', 'psubscribe', 'keywatch', 'unsubscribe'].includes(kind)) {
        return args
          .slice(1)
          .map((n, i) => R.push(R.bulk(kind), R.bulk(n), R.int(i + 1)))
          .join('');
      }
      if (args[0] === 'FIRE') {
        for (const c of s.conns) {
          if (c.commands.some((x) => x[0] === 'SUBSCRIBE')) {
            c.write(R.push(R.bulk('message'), R.bulk('news'), R.bulk('hello')));
            c.write(R.push(R.bulk('pmessage'), R.bulk('ev:*'), R.bulk('ev:1'), R.bulk('payload')));
            c.write(R.push(R.bulk('keyevent'), R.bulk('user:7'), R.bulk('set')));
            c.write(R.push(R.bulk('keyevent'), R.bulk('order:1'), R.bulk('del')));
          }
        }
        return R.ok;
      }
      void conn;
      return R.ok;
    });
    const c = new CacheBoy({ port: s.port });
    const got: string[] = [];
    const unsub = await c.subscribe('news', (m, ch) => got.push(`${ch}:${m}`));
    await c.psubscribe('ev:*', (m, ch, p) => got.push(`${p}|${ch}:${m}`));
    await c.keyWatch('user:*', (k, op) => got.push(`key ${k} ${op}`));
    await c.call('FIRE');
    await until(() => got.length === 3);
    assert.deepEqual(got, ['news:hello', 'ev:*|ev:1:payload', 'key user:7 set']);
    // Main connection and subscriber connection are separate.
    assert.equal(s.conns.length, 2);
    await unsub();
    assert.ok(s.commands.some((x) => x[0] === 'UNSUBSCRIBE' && x[1] === 'news'));
    c.disconnect();
  });

  it('namespace() returns a view with its own NAMESPACE USE connection', async () => {
    const s = await server(kvHandler(new Map()));
    const c = new CacheBoy({ port: s.port });
    const tenant = c.namespace('tenant1');
    assert.equal(c.namespace('tenant1'), tenant);
    await tenant.ping();
    assert.ok(s.commands.some((x) => x[0] === 'NAMESPACE' && x[2] === 'tenant1'));
    c.disconnect();
    await delay(5);
  });
});
