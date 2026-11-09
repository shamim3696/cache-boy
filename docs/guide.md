# cache-boy user guide

This guide takes you from installing cache-boy to running it in production. It covers every way to connect and every built-in feature, with working examples.

- Command reference for everything Redis doesn't have: [features.md](features.md)
- TypeScript SDK reference: [sdk/typescript/README.md](../sdk/typescript/README.md)
- Performance numbers: [benchmarks.md](benchmarks.md)

## Contents

1. [What cache-boy is](#1-what-cache-boy-is)
2. [Install](#2-install)
3. [Run the server](#3-run-the-server)
4. [The dashboard (web UI)](#4-the-dashboard-web-ui)
5. [Connect](#5-connect)
6. [Caching patterns](#6-caching-patterns)
7. [Rate limiting](#7-rate-limiting)
8. [Distributed locks](#8-distributed-locks)
9. [Job queues](#9-job-queues)
10. [Reacting to changes: pub/sub, key watch, webhooks](#10-reacting-to-changes)
11. [AI apps: semantic cache and vector search](#11-ai-apps-semantic-cache-and-vector-search)
12. [Data types](#12-data-types)
13. [Multi-tenancy with namespaces](#13-multi-tenancy-with-namespaces)
14. [Server-side JavaScript](#14-server-side-javascript)
15. [Persistence](#15-persistence)
16. [Memory limits and eviction](#16-memory-limits-and-eviction)
17. [Security](#17-security)
18. [Replication, failover and cluster](#18-replication-failover-and-cluster)
19. [Monitoring](#19-monitoring)
20. [Deploying](#20-deploying)
21. [Migrating from Redis and ioredis](#21-migrating-from-redis-and-ioredis)
22. [Troubleshooting](#22-troubleshooting)

---

## 1. What cache-boy is

cache-boy is an in-memory data store that speaks the Redis protocol. Any Redis client (redis-cli, ioredis, go-redis, redis-py, Jedis) can connect to it and use the usual commands: strings, hashes, lists, sets, sorted sets, expiry, pub/sub, transactions.

On top of that, it has features that usually need extra libraries or services:

| You want to... | Usually you add | In cache-boy |
|---|---|---|
| Stop 1000 requests hitting the DB when a hot key expires | Custom locking code | `wrap()` / `CACHE.GET` |
| Delete every cached entry about one user | Your own key tracking | Tags: `TAG.INVALIDATE user:1` |
| Rate-limit an API | Lua scripts or a library | `RATELIMIT` |
| Make sure only one worker runs a job | Redlock | `LOCK` with fencing tokens |
| Run background jobs | BullMQ | `QPUSH` / `QPOP` |
| Avoid paying an LLM twice for similar questions | A vector DB plus glue code | Semantic cache `SCACHE.*` |
| Use it from Vercel or Cloudflare Workers | A paid HTTP proxy | Built-in HTTP API |
| See what's in the cache | A separate GUI | Built-in dashboard |

It runs on every CPU core (the keyspace is split into shards with their own locks), ships as one binary with no dependencies, and runs on Windows, Linux and macOS.

---

## 2. Install

### Windows

Open PowerShell (no admin rights needed) and run:

```powershell
irm https://raw.githubusercontent.com/shamim3696/cache-boy/main/scripts/install.ps1 | iex
```

This downloads the latest release, checks its SHA-256 checksum, installs `cache-boy.exe`, `cache-boy-cli.exe` and `cache-boy-bench.exe` to `%LOCALAPPDATA%\cache-boy\bin`, and adds that folder to your user PATH. Open a new terminal afterwards.

### Linux and macOS

```bash
curl -fsSL https://raw.githubusercontent.com/shamim3696/cache-boy/main/scripts/install.sh | sh
```

Binaries go to `/usr/local/bin` if it's writable, otherwise to `~/.local/bin`.

Both scripts accept these environment variables:

| Variable | Effect |
|---|---|
| `CACHE_BOY_VERSION=v0.1.0` | Install a specific version instead of the latest |
| `CACHE_BOY_INSTALL_DIR=...` | Install somewhere else |
| `CACHE_BOY_NO_MODIFY_PATH=1` | Windows only: don't touch your PATH |

### Manual download

Download the archive for your platform from the [Releases page](https://github.com/shamim3696/cache-boy/releases) (`cache-boy_windows_amd64.zip`, `cache-boy_linux_arm64.tar.gz`, ...), check it against `checksums.txt`, and extract it anywhere.

### With Go

```bash
go install github.com/shamim3696/cache-boy/cmd/...@latest
```

### From source

```bash
git clone https://github.com/shamim3696/cache-boy.git
cd cache-boy
go build -o bin/ ./cmd/...
```

Requires Go 1.26 or newer.

### Docker

```bash
docker build -f deployments/docker/Dockerfile -t cache-boy .
docker run -d --name cache-boy -p 6380:6380 -p 6381:6381 -v cache-boy-data:/data cache-boy
```

Release builds also publish a prebuilt image to `ghcr.io/shamim3696/cache-boy`.

### Client libraries

```bash
npm install cache-boy-client                # Node.js / TypeScript / edge runtimes
go get github.com/shamim3696/cache-boy/pkg/client   # Go
```

Any other language: use its regular Redis client.

### Check the install

```bash
cache-boy --version
```

---

## 3. Run the server

```bash
cache-boy
```

```
HTTP API and dashboard on http://127.0.0.1:6381/
cache-boy 0.1.0 listening on 127.0.0.1:6380
```

- Port **6380** is the Redis-protocol port (Redis uses 6379, so both can run side by side).
- Port **6381** serves the dashboard, the HTTP API and Prometheus metrics. Open http://127.0.0.1:6381 in a browser.

Try it from another terminal:

```bash
cache-boy-cli SET greeting hello EX 60
cache-boy-cli GET greeting
cache-boy-cli                # interactive shell
```

On Windows, run the interactive shell in PowerShell or cmd. In Git Bash, use `winpty cache-boy-cli`.

### Configuration

Every option can go in a config file, as a flag, or both (flags win):

```bash
cache-boy cache-boy.conf
cache-boy --port 7000 --maxmemory 256mb --maxmemory-policy allkeys-tinylfu
cache-boy cache-boy.conf --appendonly yes
```

Most options can also be changed at runtime with `CONFIG SET`. The common ones:

| Option | Default | Meaning |
|---|---|---|
| `bind` | `127.0.0.1` | Address to listen on. Use `0.0.0.0` to accept remote connections (set a password first) |
| `port` | `6380` | Redis-protocol port |
| `http-port` | `6381` | Dashboard, HTTP API, metrics. `0` turns it off |
| `requirepass` | none | Password for the `default` user |
| `maxclients` | `10000` | Connection limit |
| `timeout` | `0` | Close idle clients after N seconds (0 = never) |
| `maxmemory` | none | Memory limit, e.g. `512mb`, `2gb` |
| `maxmemory-policy` | `noeviction` | What to delete when full (see [section 16](#16-memory-limits-and-eviction)) |
| `dir` | `.` | Where snapshot and AOF files go |
| `save` | `3600 1 300 100 60 10000` | Snapshot rules: after N seconds if at least M writes |
| `appendonly` | `no` | Log every write for durability |
| `appendfsync` | `everysec` | `always`, `everysec` or `no` |
| `history-depth` | `0` | Versions kept per string key |
| `compression-threshold` | `0` | Compress string values at least this many bytes |
| `slowlog-log-slower-than` | `10000` | Log commands slower than N microseconds |

The full annotated file is [configs/cache-boy.conf](../configs/cache-boy.conf). Run `cache-boy --help` for usage.

---

## 4. The dashboard (web UI)

cache-boy has a built-in web dashboard, with nothing extra to install. While the server is running, open this address in any browser:

```
http://127.0.0.1:6381
```

If you started the server with `--http-port 8080`, use `http://127.0.0.1:8080` instead. On another machine or in Docker, use that host's address and the published port.

If the server has a password (`requirepass`), the dashboard first shows **Authentication required**. Enter the password, or `user:password` for an ACL user, and click **Sign in**. You stay signed in until you close the browser tab.

The dot in the header shows whether the dashboard can reach the server. The **namespace** box next to it switches every tab to one tenant's keys (see [Multi-tenancy](#13-multi-tenancy-with-namespaces)). Leave it empty for the default namespace.

The dashboard has seven tabs:

### Overview

The server's health at a glance. It refreshes every 2 seconds.

| Card | Shows |
|---|---|
| Version | cache-boy version and the Redis version it reports to clients |
| Uptime | How long the server has been running |
| Ops/sec | Commands per second, with a live sparkline chart |
| Memory | Memory used, a bar against `maxmemory` (if set), and a sparkline |
| Keys | Number of keys, plus how many have expired or been evicted |
| Hit rate | Share of reads that found their key: a quick check that your cache is working |
| Clients | Connected clients |

Below the cards, a table lists the main server details from `INFO`: role (leader or replica), persistence state, eviction policy and so on.

### Keys

Browse and inspect what's stored.

- **Search** with a pattern (`user:*`, `session:*`, `*`), optionally filtered by **type** (string, hash, list, set, zset, json, ...), then click **Load more** for the next page. It uses `SCAN`, so it's safe on a large production keyspace.
- **Click a key** to see its value (JSON is pretty-printed; hashes, lists and sets are shown as structures), its type, its remaining TTL and its size.
- **Reload** re-reads the value. **Delete** removes the key after asking for confirmation.

### Console

Run any command from the browser, like `cache-boy-cli`: `SET greeting hello`, `TAG.KEYS user:1`, `RATELIMIT.PEEK api:42 100 1m`, `INFO memory`. Results are shown formatted. Press ↑/↓ to go through earlier commands; history is kept in your browser.

### Events

A live feed of key changes. Enter a key pattern (default `*`), click **Start**, and every matching `set`, `del`, `expired`, `evicted` and so on appears as it happens, with the time and the key. This is the easiest way to watch what your app is actually doing to the cache. **Clear** empties the list; click **Stop** when you're done, since a busy server produces a lot of events.

### Slowlog

Commands that took longer than `slowlog-log-slower-than` (10 ms by default), with the time, how long each took, the full command and the client that sent it. Look here first when the app feels slow.

### Clients

Every open connection: id, address, kind (normal, pub/sub, replica...), client name, ACL user, namespace and the last command it ran. Use it to find which service is connected, or which one is sending unexpected traffic. Name your connections (`clientName` in the SDK, `CLIENT SETNAME` elsewhere) so they're easy to spot.

### Commands

Statistics for every command the server has run: how many times it was called, total time and average time per call. It shows which commands dominate your workload and which ones are expensive.

### Keep the dashboard private

The dashboard can read and delete any key, so treat port 6381 like the database port: set `requirepass`, keep `bind` on a private address, and put a TLS reverse proxy in front if you open it beyond your network. To turn the dashboard and HTTP API off completely, run with `--http-port 0`.

---

## 5. Connect

### Node.js / TypeScript: cache-boy-client (recommended)

```ts
import { CacheBoy } from 'cache-boy-client';

const cb = new CacheBoy('redis://127.0.0.1:6380');
// with a password:   new CacheBoy('redis://:secret@127.0.0.1:6380')
// with an ACL user:  new CacheBoy('redis://app:secret@cache.internal:6380')
// TLS:               new CacheBoy('rediss://...')

await cb.set('user:1', { id: 1, name: 'Ada' }, { ex: 3600 });  // objects are stored as JSON
const user = await cb.getJSON<{ id: number; name: string }>('user:1');

cb.on('error', (err) => console.error('cache-boy:', err));
await cb.quit();
```

The client has no runtime dependencies, uses RESP3, pipelines concurrent commands automatically and reconnects with backoff. It never silently re-sends a write that might already have run. Create one client per process and share it. The full option list is in the [SDK README](../sdk/typescript/README.md).

### Node.js: ioredis or node-redis

Existing code works unchanged; only the port is different:

```js
import Redis from 'ioredis';
const redis = new Redis(6380);
await redis.set('a', '1');
```

For the extra features, call the commands directly: `await redis.call('RATELIMIT', 'api:42', 100, '1m')`.

### Serverless and edge (Vercel, Cloudflare Workers, Lambda)

These runtimes often can't keep TCP connections open. Use the HTTP client, which only needs `fetch`:

```ts
import { CacheBoyHttp } from 'cache-boy-client/http';

const cb = new CacheBoyHttp({
  url: process.env.CACHEBOY_HTTP_URL!,   // e.g. https://cache.example.com (port 6381 behind TLS)
  token: process.env.CACHEBOY_TOKEN,     // the password, or "user:password"
});

const page = await cb.wrap('home', renderHome, { ttl: 60 });
```

Same API as `CacheBoy`, except features that need a long-lived connection: pub/sub, key watch and client-side caching.

### Go

```go
import "github.com/shamim3696/cache-boy/pkg/client"

c, err := client.Dial("127.0.0.1:6380")
if err != nil { log.Fatal(err) }
defer c.Close()

ctx := context.Background()
c.Set(ctx, "greeting", "hello")
v, _ := c.Get(ctx, "greeting")

// Anything else:
reply, err := c.Do(ctx, "TAG.INVALIDATE", "user:1")
```

The Go client also has `Wrap`, `Lock`, `RateLimit`, `QPush`/`QPop`, `TagAdd`/`TagInvalidate`, `Subscribe` and `KeyWatch`.

### Python, Java, PHP, Ruby, .NET...

Use the language's standard Redis client with port 6380. Send cache-boy commands with its generic command method:

```python
import redis
r = redis.Redis(port=6380, decode_responses=True)
r.set('a', 1)
allowed, remaining, retry_ms, reset_ms = r.execute_command('RATELIMIT', 'api:42', 100, '1m')
```

### Plain HTTP (any language, curl)

```bash
curl -X POST localhost:6381/v1/command -d '["SET","a","1"]'                 # {"result":"OK"}
curl -X POST localhost:6381/v1/pipeline -d '[["INCR","n"],["GET","n"]]'     # several commands, one request
curl -X POST localhost:6381/v1/multi -d '[["SET","x","1"],["SET","y","2"]]' # atomic (MULTI/EXEC)
curl localhost:6381/v1/keys/a                                               # value as typed JSON
curl -X PUT 'localhost:6381/v1/keys/a?ttl=60' -d 'hello'                    # set with a 60 s TTL
curl -X DELETE localhost:6381/v1/keys/a
curl 'localhost:6381/v1/scan?pattern=user:*&count=100'
```

With a password, add `-H "Authorization: Bearer <password>"`. To pick a namespace, add `-H "X-CacheBoy-Namespace: <name>"`.

### WebSocket

Connect to `ws://host:6381/v1/ws` (add `?token=<password>` if needed) and send JSON:

```json
{"id": 1, "cmd": ["GET", "a"]}
```

Replies come back as `{"id": 1, "result": ...}` or `{"id": 1, "error": "..."}`. Pub/sub messages and key events arrive as `{"push": ...}`, so a browser can subscribe to live changes.

---

## 6. Caching patterns

### Cache-aside with stampede protection: `wrap()`

The usual cache-aside code ("get; if missing, load from DB and set") has a problem. When a popular key expires, every request misses at the same moment and they all hit the database. `wrap()` makes sure only **one** caller runs the loader; the rest wait and get its result.

```ts
const product = await cb.wrap(
  `product:${id}`,
  () => db.products.findById(id),   // runs once, even under 1000 concurrent calls
  { ttl: 300 },                      // seconds
);
```

Options:

| Option | Meaning |
|---|---|
| `ttl` | Seconds to cache the value |
| `tags` | Tags for group invalidation (next section) |
| `lockMs` | How long the loader may take before others stop waiting (default 10 000) |
| `waitMs` | How long waiters wait before loading it themselves (default 5 000) |
| `parse` | Validate or convert the cached value, e.g. a zod schema's `parse` |
| `negativeTtl` | Cache "not found" (a `null`/`undefined` result) for this many seconds |
| `bloom` | `{ key, item? }`: skip ids that were never added to a Bloom filter |

If the loader throws, the lock is released at once and the error is passed to the caller.

### Missing ids hammering the database (cache penetration)

Requests for ids that don't exist (`user:999999`, bots scanning ids) are never cached, so each one reaches the database. There are two defences:

```ts
// When a user is created:
await cb.bloom.add('users:ids', String(user.id));

// When reading:
const user = await cb.wrap(`user:${id}`, () => db.users.findById(id), {
  ttl: 300,
  negativeTtl: 30,                          // remember "not found" for 30 s
  bloom: { key: 'users:ids', item: String(id) }, // ids never created skip the cache and the DB entirely
});
```

Create the filter once with the expected size (`await cb.bloom.reserve('users:ids', 0.001, 1_000_000)`) and add existing ids before turning on the `bloom` option.

### Invalidating groups of keys with tags

```ts
await cb.set(`user:${id}:profile`, profile, { ex: 3600, tags: [`user:${id}`] });
await cb.set(`user:${id}:orders`,  orders,  { ex: 600,  tags: [`user:${id}`, 'orders'] });
await cb.wrap(`feed:${id}`, loadFeed, { ttl: 60, tags: [`user:${id}`] });

// The user changed something: drop everything cached about them.
await cb.tags.invalidate(`user:${id}`);   // returns how many keys were deleted
```

Other tag calls: `cb.tags.add(key, ...tags)`, `cb.tags.remove(key, ...tags)`, `cb.tags.keys(tag)`, `cb.tags.of(key)`, `cb.tags.list()`. Over raw Redis protocol: `SET k v EX 60 TAGS t1 t2` (TAGS must be the last option) and `TAG.INVALIDATE t1`.

### Client-side caching: reads with no network round trip

The SDK can keep recently read keys in process memory. The server tells it the moment any of them changes, so the local copy is never stale for long.

```ts
const cb = new CacheBoy('redis://127.0.0.1:6380', {
  clientCache: { maxEntries: 10_000, ttlMs: 60_000 },
});

await cb.get('config:flags');   // from the server
await cb.get('config:flags');   // from local memory
cb.cacheStats();                // { hits, misses, size, invalidations }
```

Use it for data that is read far more often than it changes: configuration, feature flags, product details, user profiles.

### Keeping old versions

```bash
cache-boy-cli CONFIG SET history-depth 10
cache-boy-cli SET config '{"theme":"dark"}'
cache-boy-cli SET config '{"theme":"light"}'
cache-boy-cli HISTORY config            # every version, newest first
cache-boy-cli RESTOREVERSION config 1   # undo
```

SDK: `cb.history(key)`, `cb.getVersion(key, n)`, `cb.restoreVersion(key, n)`.

---

## 7. Rate limiting

cache-boy uses GCRA, the algorithm behind many API gateways. It gives a smooth rate with bursts up to the limit and uses constant memory per key.

```ts
// 100 requests per minute per user
const r = await cb.rateLimit(`api:${userId}`, 100, '1m');
if (!r.allowed) {
  res.setHeader('Retry-After', Math.ceil(r.retryAfterMs / 1000));
  return res.status(429).send('Too many requests');
}
// r = { allowed, remaining, retryAfterMs, resetAfterMs }
```

- `cost`: expensive endpoints can take more than one unit: `cb.rateLimit(key, 100, '1m', { cost: 5 })`.
- `cb.rateLimitPeek(key, limit, period)` checks without consuming.
- Periods: `500ms`, `10s`, `1m`, `1h`, or a number of milliseconds.

Express middleware example:

```ts
app.use(async (req, res, next) => {
  const r = await cb.rateLimit(`ip:${req.ip}`, 60, '1m');
  if (r.allowed) return next();
  res.set('Retry-After', String(Math.ceil(r.retryAfterMs / 1000))).status(429).end();
});
```

Raw command: `RATELIMIT key limit period [COST n]` → `[allowed, remaining, retry_after_ms, reset_after_ms]`.

---

## 8. Distributed locks

Use a lock when only one process at a time may do something: charge a card, run a nightly report, migrate a record.

```ts
await cb.withLock(`invoice:${id}`, 30_000, async (lock) => {
  await chargeInvoice(id, { fencingToken: lock.token });
});
// throws CacheBoyError with code 'LOCKED' if someone else holds it
```

For more control:

```ts
const lock = await cb.lock('nightly-report', 60_000, { waitMs: 5_000, owner: 'worker-3' });
if (!lock) return;                  // someone else is running it
try {
  await step1();
  await lock.extend(60_000);        // need more time
  await step2();
} finally {
  await lock.release();
}
```

**Fencing tokens.** Every lock acquisition gets a number that only goes up. A lock can expire while its holder is paused (for example by a long GC pause), and another worker then takes it. If your database stores the highest token it has seen and rejects writes with a lower one, the paused worker can't overwrite newer data when it wakes up. Plain Redis locks (and Redlock) don't provide this.

---

## 9. Job queues

Queues support delayed jobs, retries with backoff, a visibility timeout (a job whose worker crashes goes back to the queue) and a dead-letter list.

**Producer:**

```ts
const emails = cb.queue<{ to: string; template: string }>('emails');

await emails.push({ to: 'ada@example.com', template: 'welcome' });
await emails.push(payload, {
  delayMs: 60_000,        // run in a minute
  retries: 5,             // attempts before dead-letter (default 3)
  id: `welcome:${userId}` // de-duplicate: pushing the same id twice does nothing
});
```

**Worker:**

```ts
const worker = emails.process(
  async (job) => {
    await sendEmail(job.payload);    // return → done; throw → retry later
  },
  {
    concurrency: 10,
    visibilityMs: 30_000,                          // a job not finished in 30 s is retried
    nackDelayMs: (job) => 1000 * 2 ** job.attempt, // exponential backoff
    onError: (err) => console.error(err),
  },
);

process.on('SIGTERM', () => worker.stop());        // finish running jobs, take no new ones
```

**Operations:**

```ts
await emails.len();        // { ready, delayed, inflight, dead }
await emails.dead();       // jobs that ran out of retries
await emails.retryDead();  // put them back in the queue
await emails.purge();      // delete everything
```

Producers and workers can be in different languages; the commands are `QPUSH`, `QPOP`, `QACK`, `QNACK` ([reference](features.md#job-queue)). Queues are saved with persistence turned on.

---

## 10. Reacting to changes

### Pub/sub

```ts
const unsubscribe = await cb.subscribe('chat:room1', (message) => console.log(message));
await cb.publish('chat:room1', 'hello');
await cb.psubscribe('chat:*', (message, channel) => {});
```

### Watching keys

Get told whenever matching keys change, without the publisher doing anything:

```ts
await cb.keyWatch('order:*', (key, op) => {
  console.log(`${key} was ${op}`);   // op: set, del, expired, evicted, hset, lpush...
});
```

Unlike Redis keyspace notifications, nothing needs to be enabled in the server config.

### Webhooks

Have the server POST to a URL on every matching change, so another service can react without holding a connection:

```bash
cache-boy-cli WEBHOOK.ADD orders 'order:*' https://api.example.com/hooks/cache SECRET s3cret
```

Each change is sent as `{"key": "...", "op": "...", "ts": ...}`. With `SECRET`, the request carries `X-CacheBoy-Signature: sha256=<HMAC of the body>`. Verify it before trusting the call. Failed deliveries are retried up to 3 times. `WEBHOOK.LIST` shows delivery counts and `WEBHOOK.DEL` removes a hook.

---

## 11. AI apps: semantic cache and vector search

### Semantic cache

LLM calls are slow and paid. Users ask the same thing in different words: "What's the capital of France?" and "capital of france?". A semantic cache stores answers by the *meaning* of the prompt (its embedding), so a similar question returns the earlier answer.

```ts
import OpenAI from 'openai';
const openai = new OpenAI();

const embed = async (text: string) =>
  (await openai.embeddings.create({ model: 'text-embedding-3-small', input: text })).data[0].embedding;

const answers = cb.semanticCache('answers', { embed, threshold: 0.92, ttl: '1d' });

const reply = await answers.wrap(question, async () => {
  const r = await openai.chat.completions.create({ model: 'gpt-4o-mini', messages: [{ role: 'user', content: question }] });
  return r.choices[0].message.content!;
});
```

- `threshold` is the cosine similarity a cached prompt needs to count as a hit (0 to 1). Start around 0.9 to 0.95 and tune it on real traffic. Too low returns wrong answers; too high rarely hits.
- Exact repeats of a prompt hit without a vector search.
- Any embedding model works; the server only stores vectors and never calls a model.
- `SCACHE.INFO answers` reports entries, hits, misses and the hit ratio. `SCACHE.CONFIG answers MAXENTRIES 50000` limits the size; the least recently used entries are evicted first.

### Vector search

Store embeddings with optional JSON attributes and find the nearest ones:

```ts
await cb.vector.add('docs', 'doc:1', embedding, { attr: { lang: 'en', team: 'billing' } });

const hits = await cb.vector.search('docs', 5, queryEmbedding, { filter: { lang: 'en' } });
// [{ id: 'doc:1', score: 0.93 }, ...]   score = cosine similarity, best first
```

Small sets are searched exactly. Larger ones use an HNSW index (about 0.3 ms per query at 100 000 vectors in our benchmark), which is approximate. If a filter leaves fewer than k matches, the search falls back to an exact scan so no result is lost.

---

## 12. Data types

All the Redis types work as in Redis: strings, hashes, lists, sets, sorted sets, plus expiry, `SCAN`, `MULTI/EXEC/WATCH` and blocking pops. Small hashes and sets are stored in compact encodings automatically.

Extra types:

**JSON documents.** Update one field without rewriting the whole value:

```ts
await cb.json.set('cart:7', '$', { items: [], total: 0 });
await cb.json.arrAppend('cart:7', '.items', { sku: 'A1', qty: 2 });
await cb.json.numIncrBy('cart:7', '.total', 19.98);
await cb.json.get('cart:7', '.items');
```

**Time series.** Metrics with retention and downsampling, compressed to about 1–2 bytes per sample:

```ts
await cb.ts.create('cpu:web1', { retentionMs: 7 * 86_400_000, labels: { host: 'web1' } });
await cb.ts.add('cpu:web1', '*', 0.42);    // '*' = now
await cb.ts.range('cpu:web1', '-', '+', { aggregation: { type: 'avg', bucketMs: 60_000 } });
```

**Bloom filters.** "Have I seen this before?" in a fraction of the memory of a set. Answers are "definitely not" or "probably yes":

```ts
await cb.bloom.reserve('seen:emails', 0.001, 1_000_000);
await cb.bloom.add('seen:emails', email);
await cb.bloom.exists('seen:emails', email);
```

**HyperLogLog.** Count distinct items (unique visitors) in 12 KB, about 0.81% error:

```ts
await cb.pfAdd('visitors:2026-11-09', ip);
await cb.pfCount('visitors:2026-11-09');
```

Command syntax for all of these is in [features.md](features.md).

---

## 13. Multi-tenancy with namespaces

Give each tenant (customer, app, environment) its own isolated keyspace on one server:

```ts
const acme = cb.namespace('acme');
const globex = cb.namespace('globex');

await acme.set('settings', { theme: 'dark' });
await globex.get('settings');   // null: different namespace
```

Over the protocol: `NAMESPACE USE acme` on a connection. Over HTTP: the `X-CacheBoy-Namespace` header. `NAMESPACE LIST` shows key counts per namespace and `NAMESPACE DROP acme` deletes one tenant's data. Combine namespaces with ACL users to stop tenants from switching namespaces themselves.

---

## 14. Server-side JavaScript

Run several commands atomically on the server, in JavaScript instead of Lua:

```ts
const script = `
  const stock = Number(cb.call('GET', KEYS[0]) || 0);
  if (stock < Number(ARGV[0])) return {err: 'OUT_OF_STOCK'};
  cb.call('DECRBY', KEYS[0], ARGV[0]);
  return cb.call('INCRBY', KEYS[1], ARGV[0]);
`;
await cb.evalCached(script, ['stock:42', 'sold:42'], [2]);
```

Inside a script you have `KEYS`, `ARGV`, `cb.call()` (throws on error), `cb.pcall()` (returns `{err}`) and `cb.log()`. Scripts are stopped after `script-timeout` ms (default 5000). Nothing else runs while a script runs, so keep scripts short.

---

## 15. Persistence

By default data is kept in memory and snapshotted to `dump.cbdump` in `dir` according to the `save` rules. Choose what fits your data:

| Setup | Config | What you can lose on a crash |
|---|---|---|
| Pure cache | `save ""` | Everything (it's a cache; it refills) |
| Snapshots (default) | `save 3600 1 300 100 60 10000` | Writes since the last snapshot |
| Append-only file | `appendonly yes` + `appendfsync everysec` | About one second of writes |
| Safest | `appendonly yes` + `appendfsync always` | Nothing, but writes are slower |

```bash
cache-boy --dir /var/lib/cache-boy --appendonly yes
```

Commands: `SAVE` / `BGSAVE` take a snapshot now, `BGREWRITEAOF` compacts the AOF, `LASTSAVE` shows the last snapshot time. Data is loaded automatically on start. Back up by copying the snapshot file. With AOF on, run `BGSAVE` first.

---

## 16. Memory limits and eviction

Set a limit and a policy so the server deletes old data instead of growing without bound:

```bash
cache-boy --maxmemory 1gb --maxmemory-policy allkeys-tinylfu
```

| Policy | Deletes | Use for |
|---|---|---|
| `noeviction` (default) | Nothing; writes fail when full | Data you must not lose (queues, locks) |
| `allkeys-tinylfu` | Keys accessed least often recently | **Most caches.** Resists one-off scans better than LRU (74.6% vs 65.9% hit ratio in our scan benchmark) |
| `allkeys-lru` | Least recently used keys | Caches where recency matters most |
| `allkeys-lfu` | Least frequently used keys | Stable popularity patterns |
| `allkeys-random` | Random keys | Uniform access |
| `volatile-*` | Same rules, only keys with a TTL | Mixing cache and permanent data on one server |
| `volatile-ttl` | Keys closest to expiring | |

Check memory with `INFO memory`, `MEMORY USAGE key`, or the dashboard. `CONFIG SET compression-threshold 4096` compresses large string values transparently.

---

## 17. Security

The default config listens on `127.0.0.1` only. Before exposing the server on a network:

1. **Set a password:** `requirepass <long random string>` in the config file.
2. **Create users with limited rights** for each app:
   ```bash
   cache-boy-cli -a <admin password> ACL SETUSER web on '>web-password' '~cache:*' '+@read' '+@write' '-@dangerous'
   cache-boy-cli -a <admin password> ACL SETUSER reporting on '>rpt-password' '~*' '+@read'
   ```
   Clients log in as that user: `redis://web:web-password@host:6380`. `ACL LIST` and `ACL WHOAMI` show users.
3. **Bind to a private interface** (`bind 10.0.0.5`), or `0.0.0.0` behind a firewall that only lets your app servers through.
4. **Protect the HTTP port.** It accepts the same password (as a Bearer token). Put it behind a TLS reverse proxy (nginx, Caddy) before exposing it to the internet, or set `http-port 0` if you don't need it.
5. **Use a secret for webhooks** and verify the signature on the receiving side.

---

## 18. Replication, failover and cluster

### Replicas

A replica keeps a live copy of a leader and serves reads:

```bash
# on the replica
cache-boy --port 6380 --replicaof 10.0.0.1 6380 --masterauth <leader password>
```

After a short disconnect a replica resyncs only what it missed, from the leader's backlog (`repl-backlog-size`, 1 MB by default), instead of copying everything. `INFO replication` shows the state. `WAIT n timeout` blocks until n replicas have a write.

### Automatic failover

With three or more nodes, cache-boy can elect a new leader when the current one dies (Raft consensus). Give each node the same peer list:

```
raft-port 7380
raft-peer node-a 10.0.0.1:7380 10.0.0.1:6380
raft-peer node-b 10.0.0.2:7380 10.0.0.2:6380
raft-peer node-c 10.0.0.3:7380 10.0.0.3:6380
```

### Cluster (sharding)

To hold more data than one machine, split the 16 384 hash slots across nodes:

```
cluster-enabled yes
cluster-node-id node-a
cluster-node node-a 10.0.0.1:6380 0-8191
cluster-node node-b 10.0.0.2:6380 8192-16383
```

Clients are redirected with `MOVED`, as in Redis Cluster. In Node.js use the cluster client:

```ts
import { CacheBoyCluster } from 'cache-boy-client';
const cluster = new CacheBoyCluster({ nodes: ['redis://10.0.0.1:6380', 'redis://10.0.0.2:6380'] });
await cluster.set('user:1', 'ana');
```

Commands touching several keys need the keys on one node. Use a hashtag: `{user:1}:profile` and `{user:1}:orders` land in the same slot.

---

## 19. Monitoring

- **Dashboard:** http://host:6381 shows keys and their values, memory, clients, throughput, live key events and the slow log. See [The dashboard](#4-the-dashboard-web-ui) for a tour.
- **Prometheus:** scrape `http://host:6381/metrics`.
- **Health check:** `GET /health` returns 200 when the server is up. Use it for load balancers and Kubernetes probes.
- **Commands:** `INFO` (all stats), `SLOWLOG GET 10`, `CLIENT LIST`, `MONITOR` (every command live; debugging only), `DBSIZE`, `MEMORY USAGE key`, `OBJECT ENCODING key`.
- **Benchmark your own setup:** `cache-boy-bench -t set,get -n 100000 -c 50 -P 16`.

---

## 20. Deploying

### Docker Compose

```yaml
services:
  cache:
    image: ghcr.io/shamim3696/cache-boy:latest   # or build: { context: ., dockerfile: deployments/docker/Dockerfile }
    # appended to the image's entrypoint (cache-boy /etc/cache-boy.conf --bind 0.0.0.0 --dir /data)
    command: ["--requirepass", "${CACHEBOY_PASSWORD}", "--maxmemory", "512mb",
              "--maxmemory-policy", "allkeys-tinylfu", "--appendonly", "yes"]
    ports: ["6380:6380", "6381:6381"]
    volumes: ["cache-data:/data"]
    restart: unless-stopped
volumes:
  cache-data:
```

The image runs as a non-root user and stores data in `/data`. [deployments/docker/docker-compose.yml](../deployments/docker/docker-compose.yml) has a leader and replica example.

### Linux service (systemd)

```ini
# /etc/systemd/system/cache-boy.service
[Unit]
Description=cache-boy
After=network.target

[Service]
ExecStart=/usr/local/bin/cache-boy /etc/cache-boy.conf
User=cacheboy
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
sudo useradd --system --home /var/lib/cache-boy cacheboy
sudo mkdir -p /var/lib/cache-boy && sudo chown cacheboy /var/lib/cache-boy
sudo systemctl enable --now cache-boy
```

Set `dir /var/lib/cache-boy` in `/etc/cache-boy.conf`.

### Windows service

Run it in the background with the Task Scheduler ("At startup", program `cache-boy.exe`, argument the config file path), or wrap it as a service with [NSSM](https://nssm.cc): `nssm install cache-boy "%LOCALAPPDATA%\cache-boy\bin\cache-boy.exe" C:\cache-boy\cache-boy.conf`.

### Production checklist

- [ ] `requirepass` set, and an ACL user per application
- [ ] `bind` to a private address; firewall in front of 6380 and 6381
- [ ] `maxmemory` below the machine's RAM (leave room for the OS), with an eviction policy
- [ ] Persistence chosen deliberately (section 15) and `dir` on a persistent disk
- [ ] Prometheus scraping `/metrics`, alerts on memory and evictions
- [ ] A replica, or three nodes with Raft, if downtime matters

---

## 21. Migrating from Redis and ioredis

1. Start cache-boy next to Redis (it uses port 6380 by default).
2. Point your app at it. With ioredis, change only the port: `new Redis(6380)`. The ioredis compatibility suite passes against cache-boy.
3. Move features over one at a time: replace Lua rate limiters with `rateLimit()`, Redlock with `lock()`, hand-written cache-aside with `wrap()`, and BullMQ queues with `queue()`.
4. Optionally switch to `cache-boy-client` for typed JSON, client-side caching and the HTTP mode.

**Differences from Redis to know about:**

- One database: `SELECT` with a non-zero index isn't supported. Use namespaces instead.
- HyperLogLog is its own type (`TYPE` says `hyperloglog`), so `GET` on a HyperLogLog key fails, unlike Redis.
- Snapshot and AOF files use cache-boy's own format; you can't load a Redis `dump.rdb`. For a cache, just let it refill. To copy data, run a small script that `SCAN`s Redis and writes each key to cache-boy.
- Redis modules aren't supported. JSON, time series, Bloom filters and vectors are built in, with their own command syntax (see [features.md](features.md)).
- Under heavy pipelining against **one** very hot key, Redis is faster. For normal workloads across many keys, cache-boy is faster (see [benchmarks.md](benchmarks.md)).

---

## 22. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `Could not connect to cache-boy at 127.0.0.1:6380` | The server isn't running, or runs on another port. Start `cache-boy`, or pass `-p <port>` to the CLI |
| Other machines can't connect | The default `bind` is `127.0.0.1`. Set `bind 0.0.0.0` (with a password) and open the firewall port |
| `NOAUTH Authentication required` | Set the password in the URL: `redis://:password@host:6380` |
| `NOPERM` | The ACL user lacks that command or key pattern. Check with `ACL GETUSER <user>` |
| `OOM command not allowed` | `maxmemory` reached with `noeviction`. Raise the limit or pick an eviction policy |
| `MOVED 1234 10.0.0.2:6380` | Cluster mode: use a cluster-aware client (`CacheBoyCluster`) |
| `CROSSSLOT` | Keys in one command live on different cluster nodes. Use a `{hashtag}` |
| Windows: `cache-boy` not found after install | Open a new terminal so it picks up the updated PATH |
| Windows: firewall prompt on first start | Allow "Private networks" for local use |
| Git Bash: CLI shell hangs | Use `winpty cache-boy-cli`, or PowerShell |
| Slow responses | Check `SLOWLOG GET`, look for large `KEYS`/`SMEMBERS` calls (use `SCAN` instead), and check `INFO` for evictions |
| Semantic cache returns wrong answers | Raise `threshold` (e.g. 0.95). Returns nothing useful? Lower it slightly |

Still stuck? [Open an issue](https://github.com/shamim3696/cache-boy/issues) with your OS, `cache-boy --version` and the error message.
