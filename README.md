# cache-boy

A Redis-compatible in-memory data store written in Go. It speaks the Redis protocol (RESP2 and RESP3), so existing Redis clients and tools work with it. It adds features that usually need extra libraries or services:

| Problem | What you usually do | cache-boy |
|---|---|---|
| Invalidate every cache entry for a user | Track keys yourself | `SET k v TAGS user:1` + `TAG.INVALIDATE user:1` |
| Cache stampede when a hot key expires | Hand-written locks | `CACHE.GET` (one client computes, the others wait) |
| Rate limiting | Lua scripts | `RATELIMIT key 100 1m` |
| Distributed locks | Redlock library | `LOCK name 10s` with fencing tokens |
| Background jobs | BullMQ | `QPUSH` / `QPOP` / `QACK` with retries, delay, dead-letter |
| Reacting to key changes | Keyspace notifications config | `KEYWATCH user:*`, `WEBHOOK.ADD` |
| Calling it from serverless / edge | Paid HTTP proxies | Built-in REST + WebSocket API |
| Seeing what's in the cache | Third-party GUIs | Built-in web dashboard |
| JSON, time series, vectors | Redis Stack modules | Built in |
| Multi-tenant apps | Key prefixes by convention | `NAMESPACE USE tenant42` |
| Reads hammering the network | Nothing in ioredis | Client-side caching (RESP3 tracking) in the TS SDK |
| Paying for the same LLM answer twice | Separate vector DB + glue code | Semantic cache: `SCACHE.GET` hits on *similar* prompts |

It also uses every CPU core: the keyspace is split into shards with their own locks, while Redis runs commands on a single thread.

## Install

**Windows** (PowerShell, no admin needed):
```powershell
irm https://raw.githubusercontent.com/shamim3696/cache-boy/main/scripts/install.ps1 | iex
```

**Linux / macOS**:
```bash
curl -fsSL https://raw.githubusercontent.com/shamim3696/cache-boy/main/scripts/install.sh | sh
```

**Docker**:
```bash
docker run -d -p 6380:6380 -p 6381:6381 -v cache-boy-data:/data ghcr.io/shamim3696/cache-boy
```

**Go**:
```bash
go install github.com/shamim3696/cache-boy/cmd/...@latest
```

Or download an archive for your platform from [Releases](https://github.com/shamim3696/cache-boy/releases). Each one holds `cache-boy`, `cache-boy-cli` and `cache-boy-bench`. Both install scripts check the SHA-256 checksum before installing; set `CACHE_BOY_VERSION=v0.1.0` to pin a version.

**Node.js SDK**:
```bash
npm install cache-boy-client
```

Then start the server with `cache-boy` and open `http://127.0.0.1:6381` for the dashboard.

## Quick start

From source:
```bash
go build -o bin/ ./cmd/...

./bin/cache-boy                          # 127.0.0.1:6380, dashboard on http://127.0.0.1:6381
./bin/cache-boy configs/cache-boy.conf   # with a config file
./bin/cache-boy --port 7000 --maxmemory 256mb --maxmemory-policy allkeys-lru --appendonly yes

./bin/cache-boy-cli                      # interactive shell (use cmd/PowerShell on Windows, or winpty in Git Bash)
./bin/cache-boy-cli SET greeting hello EX 60
./bin/cache-boy-bench -t set,get -n 100000 -P 16
```

Docker: `docker compose -f deployments/docker/docker-compose.yml up` starts a leader and a replica.

## Using it

**From any Redis client** (redis-cli, ioredis, go-redis, redis-py): connect to port 6380.

**From Node.js, with the TypeScript SDK** in `sdk/typescript`:
```ts
import { CacheBoy } from "cache-boy-client";
const cache = new CacheBoy({ url: "redis://127.0.0.1:6380", clientCache: { maxEntries: 10_000 } });

const user = await cache.wrap(`user:${id}`, () => db.users.find(id), { ttl: 60, tags: [`user:${id}`] });
await cache.tags.invalidate(`user:${id}`);
const { allowed } = await cache.rateLimit(`api:${ip}`, 100, "1m");
```

**From Go**: `import "github.com/shamim3696/cache-boy/pkg/client"`. It includes helpers like `Wrap`, `Lock`, `RateLimit` and `QPush`.

**Over HTTP** (serverless, edge, any language):
```bash
curl -X POST localhost:6381/v1/command -d '["SET","a","1"]'      # {"result":"OK"}
curl localhost:6381/v1/keys/a                                     # typed JSON
curl -X POST localhost:6381/v1/pipeline -d '[["INCR","n"],["GET","n"]]'
```
If `requirepass` is set, add `Authorization: Bearer <password>`. Prometheus metrics are at `/metrics` and the WebSocket API at `/v1/ws`.

Every non-Redis command is documented in [docs/features.md](docs/features.md).

## What's supported

- **Data types**: strings, lists, hashes, sets, sorted sets (skip list), JSON documents, time series (Gorilla-compressed), vector sets (HNSW index), Bloom filters, HyperLogLog
- **Compact encodings**: small hashes and sets are stored as listpacks and intsets, and integer strings as int64, as in Redis
- **Keys**: TTLs (lazy and active expiry), SCAN, RENAME, COPY, OBJECT
- **Memory**: `maxmemory` with LRU, LFU, random, TTL and **W-TinyLFU** eviction (`allkeys-tinylfu`: a Count-Min Sketch plus a doorkeeper Bloom filter, which resists scans), an eviction pool, timing-wheel active expiry, and optional compression of large values
- **Transactions**: MULTI / EXEC / WATCH, plus atomic server-side JavaScript (`EVAL`)
- **AI**: semantic cache for LLM responses, vector similarity search
- **Pub/Sub**: channels and patterns, key-change streams, webhooks
- **Persistence**: binary snapshots (`SAVE`, `BGSAVE`, save rules) and an append-only file with fsync policies and rewrite
- **Replication**: leader → replicas with full sync, partial resync from a backlog after short disconnects, `WAIT`, read-only replicas, `masterauth`
- **High availability**: Raft leader election for automatic failover
- **Cluster**: 16384 hash slots with a static slot map, `MOVED` / `CROSSSLOT` redirects; the TS SDK's `CacheBoyCluster` routes by slot and follows redirects
- **Security**: `requirepass`, ACL users with command, category and key-pattern rules
- **Observability**: INFO, SLOWLOG, MONITOR, per-command latency histograms, Prometheus, a dashboard

## Project layout

```
cmd/cache-boy            server
cmd/cache-boy-cli        CLI (like redis-cli)
cmd/cache-boy-bench      benchmark (like redis-benchmark)
internal/protocol/resp   RESP2/RESP3 reader and writer
internal/server          TCP server: one goroutine per client, plus a writer goroutine
internal/command         command table and dispatcher, one file per command group
internal/store           sharded keyspace, entries, expiry sampling
internal/datatype        deque, skiplist, jsondoc, timeseries, vector
internal/expiry, eviction, compression, glob
internal/persistence     snapshot (binary format) and AOF
internal/replication     leader/follower, Raft failover
internal/consensus/raft  leader election
internal/cluster         hash slots and routing
internal/auth            users, passwords, ACL
internal/pubsub, transaction, blocking, tracking
internal/feature/*       tags, ratelimit, lock, queue, watch, singleflight, history, scripting, namespace
internal/api             REST and WebSocket API
internal/observability   metrics, slowlog, monitor
web/dashboard            embedded web UI
pkg/client               Go client
sdk/typescript           Node.js / TypeScript SDK
```

## How a command runs

1. `server/conn.go` reads a RESP array from the socket (`resp.Reader`).
2. `command.Engine.Execute` looks up the command, then checks arity, authentication and ACL, MULTI queuing and pub/sub mode.
3. `run` adds the namespace prefix to key arguments and checks cluster slots, read-only replicas and `maxmemory`. It then locks only the shards that hold the command's keys, which is why commands on different keys run in parallel.
4. The handler runs. Write commands then emit key events (WATCH, tracking, BLPOP wake-ups, key watchers, history and tags all listen to these) and are propagated to the AOF and replicas, still under the lock, so the order on disk matches the order in memory.
5. The reply goes to the client's writer goroutine, which batches pipelined replies into one network write.

## Testing

```bash
go test ./...                                    # Go unit and integration tests
go test -fuzz FuzzReadCommand ./internal/protocol/resp   # fuzz the protocol parser
cd test/compat/ioredis && npm install && npm test -- 6380  # real ioredis against a running server
cd sdk/typescript && npm install && npm test     # TypeScript SDK
CACHEBOY_URL=redis://127.0.0.1:6380 npm test     # SDK against a running server
```

## Performance

With many keys and no pipelining, cache-boy is about 2x faster than Redis 7 on the same 4 CPUs. With pipelining the two are roughly even. With one extremely hot key under heavy pipelining, Redis is faster. See [docs/benchmarks.md](docs/benchmarks.md).

The test suite passes under the race detector (`go test -race ./...`).

## Known limitations

- After a long disconnect (more writes than `repl-backlog-size`), a replica falls back to a full resync. A replica that restarts always does a full resync, because the replication ID is not persisted.
- Failover can lose writes the new leader hadn't received yet, because replication is asynchronous (as in Redis).
- Cluster slots are a static map from the config; there's no resharding or gossip.
- Snapshots and AOF rewrites briefly lock all shards and need a temporary in-memory copy of the dataset, because Go can't `fork()` like Redis.
- Locks, rate limits and compute locks live only on the node that handles them; they aren't replicated.
- `HSCAN` / `SSCAN` / `ZSCAN` return all elements in one page.
- Vector search is exact (brute force), which is fine up to about 100k vectors per key.
