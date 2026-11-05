# Benchmarks: cache-boy vs Redis 7

**Setup.** Both servers ran in identical Linux containers (`--cpus 4`) on one Docker network, on a Windows 11 laptop running Docker Desktop. The same client, `cache-boy-bench` in a third container, measured both: 50 clients, 300k requests, 3-byte values. Redis 7 (`redis:7-alpine`) used its default config. Treat these as relative numbers, not absolute ones.

Reproduce:
```bash
GOOS=linux CGO_ENABLED=0 go build -o bin/linux/ ./cmd/cache-boy ./cmd/cache-boy-bench
docker network create cbbench
docker run -d --name cbredis --network cbbench --cpus 4 redis:7-alpine
docker run -d --name cbserver --network cbbench --cpus 4 -v $PWD/bin/linux:/app alpine:3.20 /app/cache-boy --bind 0.0.0.0 --http-port 0 --save ""
docker run --rm --network cbbench -v $PWD/bin/linux:/app alpine:3.20 /app/cache-boy-bench -h cbserver -p 6380 -r 100000 -P 16 -t set,get,incr -q
```

## Many keys (`-r 100000`), the typical cache workload

| requests/sec | Redis 7, no pipeline | cache-boy, no pipeline | Redis 7, pipeline 16 | cache-boy, pipeline 16 |
|---|---|---|---|---|
| SET  | 54.8k | **112.6k** | **479k** | 441k |
| GET  | 55.9k | **107.4k** | 496k | **569k** |
| INCR | 57.5k | **126.1k** | 493k | **507k** |

Without pipelining, cache-boy is about **2x faster**: parsing, networking and commands on different keys run on all cores. Redis executes commands on one thread. With pipelining, network overhead disappears and the two are roughly even.

## One hot key (HSET/ZADD on a single key, as redis-benchmark does)

| requests/sec | Redis 7, no pipeline | cache-boy, no pipeline | Redis 7, pipeline 16 | cache-boy, pipeline 16 |
|---|---|---|---|---|
| HSET (one hash) | 52.3k | **110.3k** | **458k** | 217k |
| ZADD (one zset) | 44.9k | 48.5k | **255k** | 62k |

When every client hammers **the same key** under heavy pipelining, Redis wins. cache-boy serializes those writes on one shard mutex, and handing the lock between 50 goroutines costs more than Redis' lock-free single thread. If your workload has one extremely hot key (a global counter or leaderboard), spread it over several keys (e.g. `counter:{0..15}`) or use a client-side batch.

## What was optimized

Every write emits a key event, and seven subsystems listen to them: WATCH, BLPOP wake-ups, client tracking, tags, key watch, history and pub/sub. Each one used to take a global mutex on every write, even when the feature was unused. Each listener now checks an atomic counter first and returns without locking when nobody uses it. This removed the global serialization point for write commands.
