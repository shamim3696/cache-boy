# cache-boy-client

The TypeScript/Node.js client for [cache-boy](../../README.md), a Redis-compatible in-memory data store.

It works as a drop-in alternative to ioredis for everyday commands. It also covers the things you normally build yourself on top of Redis: client-side caching, typed JSON, stampede-safe cache-aside, locks with fencing tokens, rate limiting, job queues, tag invalidation, and an HTTP mode for serverless and edge runtimes.

- No runtime dependencies. ESM, with `.d.ts` types. Node ≥ 18.
- RESP3 by default: one streaming parser that handles every RESP2 and RESP3 type.
- Auto-pipelining: commands issued in the same tick are sent in one socket write.
- Auto-reconnect with exponential backoff and jitter. In-flight reads are replayed. Writes that might have run are rejected with a clear error, so they never run twice by accident.

## Install

```sh
npm install cache-boy-client
```

## Quick start

```ts
import { CacheBoy } from 'cache-boy-client';

const cb = new CacheBoy('redis://:secret@localhost:6379', {
  clientCache: { maxEntries: 10_000 }, // local LRU kept fresh by the server
});

await cb.set('user:1', { id: 1, name: 'Ada' }, { ex: 3600, tags: ['users'] });
const user = await cb.getJSON<{ id: number; name: string }>('user:1');

await cb.quit();
```

Connection options: `host`, `port`, `path` (unix socket), `tls` (`true` or `node:tls` options; `rediss://` URLs turn it on), `username`, `password`, `clientName`, `namespace`, `db`, `protocol` (3 by default), `connectTimeoutMs`, `commandTimeoutMs`, `autoPipelining`, `enableOfflineQueue`, `reconnect: { baseDelayMs, maxDelayMs, maxAttempts } | false`, `inflightOnReconnect: 'retry-safe' | 'retry-all' | 'reject'`, `lazyConnect`, and `clientCache`.

URL form: `redis://[user:pass@]host[:port][/db][?namespace=ns&name=client&protocol=2]`.

Events: `connect`, `ready`, `error`, `close`, `reconnecting` (`{ attempt, delayMs, error }`), `end`, and `push`.

```ts
cb.on('reconnecting', ({ attempt, delayMs }) => console.warn(`reconnect #${attempt} in ${delayMs}ms`));
cb.on('error', (err) => console.error(err)); // without a listener, connection errors are silent; commands still reject
```

## Feature tour

### Client-side caching

ioredis doesn't do this. With `clientCache`, the client sends `CLIENT TRACKING ON` after `HELLO 3`. GET results are then kept in a local LRU, and repeated reads never touch the network. When any client changes a key, the server pushes `["invalidate", [key]]` and the entry is dropped.

```ts
const cb = new CacheBoy({ host: 'cache', clientCache: { maxEntries: 10_000, ttlMs: 60_000 } });

await cb.get('config:flags'); // network
await cb.get('config:flags'); // local, ~0 µs
cb.cacheStats(); // { hits: 1, misses: 1, size: 1, invalidations: 0 }
```

How it stays correct:

- **Invalidation pushes** evict single keys. `["invalidate", null]` (FLUSHALL) clears everything.
- **Reconnects** clear the cache, because invalidations sent while the connection was down are lost.
- **In-flight race.** If an invalidation for a key arrives while a GET for that key is in flight, that GET's reply may be stale. It is returned to the caller but not cached.
- **Read-your-writes.** Writes made through this client (`set`, `del`, `incr`, `call('SET', ...)` and so on) evict the local entry right away, before the server push arrives.
- Missing keys (`null`) are not cached.

### Typed JSON

```ts
await cb.set('order:7', { total: 42, items: ['a', 'b'] }); // objects are JSON-encoded
const order = await cb.getJSON<Order>('order:7');

// Plug in any validator, e.g. zod (no dependency needed):
const safe = await cb.getJSON('order:7', { parse: OrderSchema.parse });
```

`set(key, value, opts)` accepts `string | number | Buffer | object`. The options are `ex`, `px`, `exat`, `pxat`, `keepTtl`, `nx`, `xx`, `get` and `tags`. Binary values round-trip with `getBuffer(key)`.

### `wrap()`: cache-aside with stampede protection

```ts
const product = await cb.wrap(
  `product:${id}`,
  () => db.products.find(id), // runs on ONE client only, even if 1000 ask at once
  { ttl: 300, tags: [`product:${id}`, 'catalog'], lockMs: 10_000, waitMs: 5_000, parse: ProductSchema.parse },
);
```

`wrap()` uses the server's `CACHE.GET` protocol:

- `hit`: the cached JSON is parsed and returned.
- `compute`: this client holds the compute lock. It runs `fn`, stores the result with `SET ... EX ttl TAGS ...`, and that write wakes every waiting client. If `fn` throws, it sends `CACHE.RELEASE` and rethrows.
- `timeout`: another client's computation took longer than `waitMs`. This client runs `fn` itself and stores the result.

#### Cache penetration

Stampede protection handles keys that exist. Cache *penetration* is the opposite problem: lookups of ids that don't exist (a typo, a deleted row, a bot scanning `user:1..∞`). Nothing gets cached for them, so every request goes to the database. `wrap()` has two defences:

```ts
const user = await cb.wrap(`user:${id}`, () => db.users.find(id), {
  ttl: 300,
  negativeTtl: 30,                   // cache "not found" for 30 s
  bloom: { key: 'users:ids', item: id }, // skip ids that were never created
});
// user is null for a missing id
```

- `negativeTtl` (seconds): when `fn` returns `null` or `undefined`, `wrap()` stores a sentinel for `negativeTtl` seconds instead of `ttl`. Lookups of that key resolve to `null` until the sentinel expires, without calling `fn`. The sentinel contains NUL characters, which JSON text never does, so a real cached value can't be mistaken for it. A custom `serialize` that returns the sentinel is rejected with a `TypeError`. Without `negativeTtl`, `null` is cached as JSON `null` for the full `ttl`, as before.
- `bloom: { key, item? }`: before touching the cache, `wrap()` sends `BF.EXISTS key item` (`item` defaults to the cache key). If the filter says the id was definitely never added, `wrap()` resolves to `null` without calling the cache or `fn`. A Bloom filter has no false negatives, so real ids always get through. A small share of missing ids (the filter's error rate) still pass and are handled by `negativeTtl`. Add every id when it's created (`cb.bloom.add('users:ids', id)`) and backfill existing ones first: a missing filter counts as empty and lets nothing through.

### Bloom filters and HyperLogLog

```ts
await cb.bloom.reserve('users:ids', 0.001, 1_000_000); // error rate, capacity [, { expansion, nonScaling }]
await cb.bloom.add('users:ids', '42');                 // true if new
await cb.bloom.madd('users:ids', ['43', '44']);        // [true, true]
await cb.bloom.exists('users:ids', '99');              // false = definitely never added
await cb.bloom.mexists('users:ids', ['42', '99']);     // [true, false]
await cb.bloom.card('users:ids');                      // 3
await cb.bloom.info('users:ids');                      // { capacity, size, filters, items, expansionRate }

await cb.pfAdd('visitors:today', 'ip1', 'ip2');        // true if the estimate changed
await cb.pfCount('visitors:today');                    // ≈ distinct count (0.81% standard error)
await cb.pfCount('visitors:mon', 'visitors:tue');      // distinct over the union
await cb.pfMerge('visitors:week', 'visitors:mon', 'visitors:tue');
```

### Tags

```ts
await cb.set('product:1', p1, { tags: ['catalog', 'brand:acme'] });
await cb.tags.add('product:2', 'catalog');
await cb.tags.keys('catalog');        // ['product:1', 'product:2']
await cb.tags.of('product:1');        // ['catalog', 'brand:acme']
await cb.tags.invalidate('brand:acme'); // deletes every key tagged brand:acme → 1
await cb.tags.list();
await cb.tags.remove('product:2', 'catalog');
```

### Rate limiting (GCRA)

```ts
const r = await cb.rateLimit(`api:${userId}`, 100, '1m', { cost: 1 });
if (!r.allowed) return new Response('slow down', { status: 429, headers: { 'retry-after': String(Math.ceil(r.retryAfterMs / 1000)) } });
// r = { allowed, remaining, retryAfterMs, resetAfterMs }
await cb.rateLimitPeek(`api:${userId}`, 100, '1m'); // doesn't consume
```

### Distributed locks with fencing tokens

```ts
const lock = await cb.lock('invoice:42', 10_000, { waitMs: 2_000, owner: 'worker-1' });
if (lock) {
  try {
    await db.invoices.update(42, data, { fencingToken: lock.token }); // DB rejects stale tokens
    await lock.extend(10_000);
  } finally {
    await lock.release();
  }
}

// Or, simpler:
await cb.withLock('report', 30_000, async (lock) => buildReport(lock.token)); // throws CacheBoyError('LOCKED') if busy
```

### Job queue

```ts
const emails = cb.queue<{ to: string; template: string }>('emails');

await emails.push({ to: 'ada@example.com', template: 'welcome' }, { delayMs: 60_000, retries: 5, id: 'welcome:ada' });

const worker = emails.process(
  async (job) => {
    await sendEmail(job.payload); // resolve → ack, throw → nack (retry, then dead-letter)
  },
  { concurrency: 10, visibilityMs: 30_000, nackDelayMs: (job) => 1000 * 2 ** job.attempt, onError: console.error },
);

process.on('SIGTERM', () => worker.stop()); // finishes running jobs, takes no new ones

// Manual control:
const job = await emails.pop({ visibilityMs: 30_000, blockMs: 5_000 });
if (job) { /* ... */ await job.ack(); /* or */ await job.nack(5_000); }
await emails.len();       // { ready, delayed, inflight, dead }
await emails.dead();      // [{ id, payload, attempts }]
await emails.retryDead(); // move dead jobs back to ready
await emails.purge();
```

Payloads are JSON-encoded, strings included, so they round-trip. Buffers are sent raw. Blocking pops run on a separate pooled connection, so they never stall other commands.

### Pub/sub and key change streams

```ts
const unsubscribe = await cb.subscribe('news', (message, channel) => console.log(channel, message));
await cb.psubscribe('orders:*', (message, channel, pattern) => {});
await cb.keyWatch('user:*', (key, op) => console.log(`${key} was ${op}`)); // op: set, del, expired, evicted...
await cb.publish('news', 'hello');
await unsubscribe();
```

Subscriptions use a dedicated connection, created on first use. They are restored automatically after a reconnect.

### JSON documents, time series, vectors, history, scripting

```ts
await cb.json.set('doc', '$', { user: { name: 'Ada', visits: 1 }, tags: [] });
await cb.json.get('doc');                      // the whole document
await cb.json.get('doc', '.user.name');        // ['Ada']
await cb.json.numIncrBy('doc', '.user.visits', 1); // [2]
await cb.json.arrAppend('doc', '.tags', 'vip');    // [1]
await cb.json.del('doc', '.tags');

await cb.ts.create('cpu', { retentionMs: 86_400_000, labels: { host: 'a' } });
await cb.ts.add('cpu', '*', 0.42);
await cb.ts.get('cpu');                        // { timestamp, value }
await cb.ts.range('cpu', '-', '+', { aggregation: { type: 'avg', bucketMs: 60_000 } });

await cb.vector.add('docs', 'doc:1', embedding, { attr: { lang: 'en' } });
await cb.vector.search('docs', 5, queryEmbedding, { filter: { lang: 'en' } }); // [{ id, score }]
await cb.vector.get('docs', 'doc:1');
await cb.vector.del('docs', 'doc:1');

await cb.history('config');                    // [{ version, timestampMs, value }] (needs history-depth)
await cb.getVersion('config', 3);
await cb.restoreVersion('config', 3);

await cb.eval("return cb.call('INCRBY', KEYS[0], ARGV[0])", ['counter'], [5]);
const sha = await cb.scriptLoad(script);
await cb.evalsha(sha, ['k'], []);
await cb.evalCached(script, ['k'], []); // EVALSHA with automatic SCRIPT LOAD on NOSCRIPT
```

### Namespaces (multi-tenancy)

```ts
const tenant = cb.namespace('acme'); // same API, keys isolated in "acme"
await tenant.set('settings', { theme: 'dark' });
```

On TCP, a namespace view gets its own connection, which runs `NAMESPACE USE` on every reconnect. It is created lazily and closed together with the parent client.

### Pipelines and transactions

```ts
const results = await cb.pipeline().set('a', 1).incr('a').getJSON('cfg').exec();
// [[null, 'OK'], [null, 2], [null, {...}]]: each entry is [error, typedResult]

await cb.multi().decrBy('stock:1', 1).incrBy('sold:1', 1).exec(); // MULTI/EXEC
```

You rarely need `pipeline()` on TCP, because auto-pipelining already batches concurrent commands.

### Anything else

```ts
await cb.call('OBJECT', 'ENCODING', 'big-key');
```

The typed API also covers strings (`get`, `set`, `mget`, `mset`, `incr`, `incrBy`, `incrByFloat`, `decr`, `append`, `getdel`, `strlen`), keys (`del`, `unlink`, `exists`, `expire`, `pexpire`, `ttl`, `pttl`, `persist`, `type`, `rename`, `keys`, `scan`, `scanAll`), hashes, lists (including `blpop` and `brpop`), sets, sorted sets (`zadd`, `zrange`, `zrangeWithScores`, `zscore`...), `publish` and `ping`.

## HTTP / edge mode

`CacheBoyHttp` exposes the same API over cache-boy's HTTP JSON endpoints (`/v1/command`, `/v1/pipeline`, `/v1/multi`). Use it in Vercel/Cloudflare/Lambda functions where TCP connections are unavailable or too expensive. It needs only `fetch`.

```ts
import { CacheBoyHttp } from 'cache-boy-client/http';

const cb = new CacheBoyHttp({ url: process.env.CACHEBOY_HTTP_URL!, token: process.env.CACHEBOY_TOKEN, namespace: 'shop' });

await cb.wrap('home', renderHome, { ttl: 60 });
const limited = await cb.rateLimit(ip, 20, '10s');
await cb.pipeline().incr('views').expire('views', 3600).exec(); // one request → /v1/pipeline
await cb.multi().set('a', 1).set('b', 2).exec();                // atomic → /v1/multi
```

Some features are not available over HTTP and throw `CacheBoyError` with code `UNSUPPORTED`: pub/sub, `keyWatch`, client-side caching (`CLIENT TRACKING`), `MULTI`/`WATCH` as single commands (use `multi()`), `NAMESPACE USE` (use the `namespace` option), and commands that block forever (`BLPOP k 0`, `QPOP ... BLOCK 0`). Finite blocking timeouts work. Binary values are sent as UTF-8 text, because JSON has no byte type.

## ioredis vs cache-boy-client

| | ioredis | cache-boy-client |
|---|---|---|
| Client-side caching (RESP3 tracking, local LRU, race-safe invalidation) | ✗ | ✓ `clientCache` |
| Typed JSON values (`set(obj)`, `getJSON<T>`, validator hook) | ✗ (manual `JSON.parse`) | ✓ |
| Cache-aside with stampede protection | ✗ (DIY with SETNX + polling) | ✓ `wrap()` |
| Distributed locks with fencing tokens | ✗ (Redlock as a separate library, no fencing) | ✓ `lock()` / `withLock()` |
| Rate limiting | ✗ (Lua scripts) | ✓ GCRA `rateLimit()` |
| Job queue (delays, visibility timeout, retries, dead-letter, worker) | ✗ (BullMQ, a separate library) | ✓ `queue().process()` |
| Tag-based invalidation | ✗ | ✓ `set(..., { tags })`, `tags.invalidate()` |
| HTTP / edge mode with the same API | ✗ | ✓ `CacheBoyHttp` |
| Auto-pipelining | opt-in | on by default |
| Safe replay on reconnect | retries everything | reads replayed, unsafe writes rejected (configurable) |
| RESP3 | ✗ (RESP2 only) | default (maps, sets, doubles, pushes...) |
| Runtime dependencies | several | none |

## Errors

Every error is a `CacheBoyError`. Its `code` is the first word of the server error (`WRONGTYPE`, `NOPERM`, `MOVED`, `NOSCRIPT`...) or one of these client-side codes:

| code | meaning |
|---|---|
| `TIMEOUT` | no reply within `commandTimeoutMs` (or the per-call timeout) |
| `CONNECTION_LOST` | the connection dropped after the command was sent, and the command isn't safe to retry. It may or may not have run |
| `CONNECTION_CLOSED` | the client was closed, or gave up reconnecting |
| `UNSUPPORTED` | not available on this transport (e.g. SUBSCRIBE over HTTP) |
| `LOCKED` | `withLock()` couldn't acquire the lock |
| `HTTP` / `PROTOCOL` / `BAD_REPLY` | transport or decoding problems |

```ts
try {
  await cb.lpush('a-string', 'x');
} catch (e) {
  if (e instanceof CacheBoyError && e.code === 'WRONGTYPE') { /* ... */ }
}
```

## Development

```sh
npm install
npm run build      # → dist/ (ESM + .d.ts)
npm test           # build + node --test (unit tests; integration tests are skipped)

# Integration tests against a running server:
CACHEBOY_URL=redis://127.0.0.1:6380 npm test
CACHEBOY_URL=redis://127.0.0.1:6380 CACHEBOY_HTTP_URL=http://127.0.0.1:8080 CACHEBOY_TOKEN=secret npm test
```

## Cluster

```ts
import { CacheBoyCluster } from 'cache-boy-client';

const cluster = new CacheBoyCluster({ nodes: ['redis://10.0.0.1:6380', 'redis://10.0.0.2:6380'] });
await cluster.set('user:1', 'ana');                 // sent to the node owning the key's slot
await cluster.mget('{user:1}:a', '{user:1}:b');     // hashtags keep related keys on one node
await cluster.tags.invalidate('user:1');            // broadcast to every node, counts summed
```

The cluster client computes each key's hash slot (CRC16 + `{hashtag}`), sends the command straight to the owning node, follows `MOVED` redirects and refreshes the slot map. `DBSIZE`, `KEYS`, `FLUSHALL` and `TAG.*` queries are sent to every node and the replies merged. `subscribe` and `keyWatch` listen on every node. Multi-key commands and `multi()` need all their keys on one node, so use a hashtag.
