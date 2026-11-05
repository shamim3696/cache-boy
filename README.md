# cache-boy

A Redis-compatible in-memory data store written in Go. It uses all CPU cores and adds features developers usually need extra libraries for.

## Project layout

```
cmd/
  cache-boy/            server binary
  cache-boy-cli/        interactive CLI
  cache-boy-bench/      benchmark tool
internal/
  config/               config file + flags
  server/               TCP listener, per-client connection
  protocol/resp/        RESP2 + RESP3 reader/writer
  command/              command registry + one file per command group
  store/                keyspace, sharding (multi-core), entries
  datatype/             deque, skiplist, jsondoc, timeseries, vector
  expiry/               lazy + active expiry
  eviction/             LRU, LFU, random policies
  persistence/aof/      append-only file + rewrite
  persistence/snapshot/ binary snapshot format
  pubsub/               channels + pattern subscriptions
  transaction/          MULTI/EXEC/WATCH
  blocking/             BLPOP-style wait queues
  tracking/             client-side caching invalidation
  replication/          leader → follower
  cluster/              hash slots + routing
  consensus/raft/       leader election
  auth/                 AUTH + ACL users
  feature/
    tags/               tag-based invalidation
    ratelimit/          built-in rate limiter
    lock/               distributed locks with fencing tokens
    queue/              job queue: retry, delay, dead-letter
    watch/              key-change streams + webhooks
    singleflight/       cache stampede protection
    history/            key versioning
    namespace/          multi-tenancy + per-namespace limits
    scripting/          server-side scripting
  api/httpapi/          REST API (serverless / edge)
  api/ws/               WebSocket API
  observability/        metrics, slowlog, tracing
pkg/client/             Go client SDK
sdk/typescript/         TypeScript SDK (ioredis alternative)
web/dashboard/          web UI
test/integration/       end-to-end tests (unit tests live next to code as *_test.go)
configs/                sample config files
deployments/docker/     Dockerfile, compose
docs/                   command reference, architecture
scripts/                dev / release scripts
```

## Roadmap

- [x] Phase 0: Project structure
- [ ] Phase 1: TCP server, RESP2, basic commands
- [ ] Phase 2: Lists, hashes, sets, sorted sets (skip list)
- [ ] Phase 3: Expiry and eviction
- [ ] Phase 4: Persistence (AOF and snapshots)
- [ ] Phase 5: Pub/Sub, transactions, blocking ops, replication
- [ ] Phase 6: RESP3, client tracking, tags, rate limit, locks, queue, singleflight
- [ ] Phase 7: HTTP/WS API, dashboard, metrics, auth, SDKs, CLI, Docker
- [ ] Phase 8: Cluster and Raft
