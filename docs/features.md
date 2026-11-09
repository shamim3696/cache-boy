# cache-boy features beyond Redis

This is the contract for every command that Redis doesn't have. The server, the Go client, the CLI, the HTTP API and the TypeScript SDK must all follow it.

Conventions:
- `ms` is milliseconds and `s` is seconds. Durations given as `<duration>` accept a plain number of milliseconds, or a number with a unit: `500ms`, `10s`, `5m`, `1h`.
- "map" is a RESP3 map. Under RESP2 it arrives as a flat array of key/value pairs.
- Keys are namespaced automatically (see Namespaces).

---

## Tag-based invalidation

Attach tags to keys, then delete every key with a tag in one call. Tags are removed automatically when a key is deleted, expires or is evicted.

| Command | Reply |
|---|---|
| `SET key value [EX s\|PX ms\|EXAT\|PXAT\|KEEPTTL] [NX\|XX] [GET] [TAGS tag [tag ...]]` | as Redis SET. `TAGS` must be the last option. |
| `TAG.ADD key tag [tag ...]` | integer: tags newly attached. 0 if the key doesn't exist. |
| `TAG.REMOVE key tag [tag ...]` | integer: tags removed |
| `TAG.KEYS tag` | array of keys with the tag |
| `TAG.OF key` | array of the key's tags |
| `TAG.INVALIDATE tag [tag ...]` | integer: number of keys deleted |
| `TAG.LIST` | array of every tag in use |

Tags are not namespaced in the index. Keys are stored namespaced, and `TAG.KEYS` returns them with the caller's namespace removed (keys from other namespaces are hidden).

## Rate limiting

Uses GCRA (Generic Cell Rate Algorithm): smooth limiting, with bursts allowed up to the limit, and O(1) memory per key.

| Command | Reply |
|---|---|
| `RATELIMIT key limit period [COST n]` | array `[allowed (1/0), remaining, retry_after_ms, reset_after_ms]` |
| `RATELIMIT.PEEK key limit period` | same shape; consumes nothing |
| `RATELIMIT.RESET key` | integer 1/0 |

`period` is a `<duration>`. Example: `RATELIMIT api:user:42 100 1m` allows 100 requests per minute. `retry_after_ms` is 0 when the request is allowed. Rate-limit keys live outside the keyspace; they don't show up in KEYS and are not persisted.

## Distributed locks with fencing tokens

| Command | Reply |
|---|---|
| `LOCK name ttl [WAIT ms] [OWNER id]` | integer fencing token (increases monotonically per server), or null if the lock wasn't acquired (after waiting up to WAIT) |
| `UNLOCK name token` | integer 1 if released, 0 if the token doesn't match or the lock expired |
| `LOCK.EXTEND name token ttl` | integer 1/0 |
| `LOCK.INFO name` | map `{token, owner, ttl_ms}` or null |

`ttl` is a `<duration>`. Pass the fencing token along with every write to your database; the database should reject a token lower than the last one it saw.

## Job queue

Supports delayed jobs, a visibility timeout, retries and a dead-letter queue.

| Command | Reply |
|---|---|
| `QPUSH queue payload [DELAY ms] [RETRIES n] [ID id]` | job id (bulk string). Default RETRIES 3. A duplicate ID is ignored, and the existing id is returned. |
| `QPOP queue [VISIBILITY ms] [BLOCK ms]` | null, or array `[id, payload, attempt]`. The job stays in flight until QACK. If it isn't acked within VISIBILITY (default 30000), it becomes ready again. `BLOCK 0` waits forever. |
| `QACK queue id` | integer 1/0 |
| `QNACK queue id [DELAY ms]` | integer 1/0. Retries the job, or moves it to the dead-letter list when attempts > RETRIES. |
| `QLEN queue` | map `{ready, delayed, inflight, dead}` |
| `QDEAD queue [count]` | array of `[id, payload, attempts]` |
| `QRETRYDEAD queue` | integer: jobs moved back to ready |
| `QPURGE queue` | integer: jobs removed (all states) |

Queues are persisted through AOF/snapshots (the module implements `Dump`).

## Cache-aside with stampede protection

When a hot key expires, only one client recomputes it. The others wait for the result.

| Command | Reply |
|---|---|
| `CACHE.GET key [LOCK ms] [WAIT ms]` | array `[status, value]` |
| `CACHE.RELEASE key` | integer 1/0: give up the compute lock (e.g. the DB call failed) |

`status` is one of:
- `hit`: value is the cached string.
- `compute`: value is null. You hold the compute lock (default LOCK 10000 ms), so compute the value and store it with `SET` (any write to the key releases waiters).
- `timeout`: value is null. The WAIT ran out (default WAIT 5000 ms) while another client was computing. Fall back to computing it yourself.

## Key change streams and webhooks

| Command | Reply |
|---|---|
| `KEYWATCH pattern [pattern ...]` | like PSUBSCRIBE. Then push messages `["keyevent", key, op]` arrive for every key matching the pattern. `op` is the command name (`set`, `del`, `lpush`...) or `expired` / `evicted`. |
| `KEYUNWATCH [pattern ...]` | like PUNSUBSCRIBE |
| `WEBHOOK.ADD id pattern url [SECRET s]` | OK. POSTs `{"key","op","ts"}` as JSON to the url for every matching change. With SECRET, the header `X-CacheBoy-Signature: sha256=<hex hmac of body>` is added. |
| `WEBHOOK.DEL id` | integer 1/0 |
| `WEBHOOK.LIST` | array of `[id, pattern, url, delivered, failed]` |

Under the hood KEYWATCH is pub/sub on the channel `__key__:<key>`. Webhooks are delivered asynchronously, with up to 3 attempts and backoff. A bounded queue drops events when full.

## Key history (versioning)

Enable it with `CONFIG SET history-depth N`. The last N versions of every string key are kept, including after the key is deleted.

| Command | Reply |
|---|---|
| `HISTORY key [COUNT n]` | array of `[version, timestamp_ms, value]`, newest first. A deletion is recorded with value null. |
| `GETVERSION key version` | bulk string or null |
| `RESTOREVERSION key version` | OK, or an error if the version is unknown |
| `HISTORY.CLEAR key` | integer 1/0 |

## Server-side JavaScript

Scripts run atomically: every other command waits until the script finishes.

| Command | Reply |
|---|---|
| `EVAL script numkeys [key ...] [arg ...]` | the script's return value |
| `EVALSHA sha1 numkeys [key ...] [arg ...]` | same; `NOSCRIPT` error if unknown |
| `SCRIPT LOAD script` | sha1 |
| `SCRIPT EXISTS sha1 [sha1 ...]` | array of 1/0 |
| `SCRIPT FLUSH` | OK |

Inside the script you have `KEYS` and `ARGV` (arrays of strings), plus:
- `cb.call(cmd, ...args)`: runs the command and returns its reply. Throws on an error reply.
- `cb.pcall(cmd, ...args)`: like `cb.call`, but returns `{err: "message"}` instead of throwing.
- `cb.log(...)`: writes to the server log.

Return values are converted like this: string → bulk; integer number → integer; non-integer number → bulk string; `true` → 1; `false` / `null` / `undefined` → null; array → array; `{ok: "s"}` → status; `{err: "s"}` → error. Scripts time out after `script-timeout` ms (default 5000).

Example: `EVAL "return cb.call('INCRBY', KEYS[0], ARGV[0])" 1 counter 5`

## Namespaces (multi-tenancy)

| Command | Reply |
|---|---|
| `NAMESPACE USE name` | OK. All key arguments on this connection are now isolated in `name`. Use `default` to go back. |
| `NAMESPACE CURRENT` | bulk string |
| `NAMESPACE LIST` | map of namespace → key count |
| `NAMESPACE DROP name` | integer: keys deleted |

## Native JSON documents

The path syntax is `$`, `.field`, `["field"]`, `[index]` (negative indexes allowed) and `[*]`.

| Command | Reply |
|---|---|
| `JSON.SET key path json [NX\|XX]` | OK, or null if NX/XX wasn't met |
| `JSON.GET key [path ...]` | One path: JSON text of the matches as an array, e.g. `[1]`. With the root path `$` or no path: the document itself. Several paths: a JSON object of path → matches. |
| `JSON.DEL key [path]` | integer: values deleted |
| `JSON.TYPE key [path]` | array of type names |
| `JSON.NUMINCRBY key path n` | JSON array text of the new values, e.g. `[3]` |
| `JSON.ARRAPPEND key path json [json ...]` | array of new lengths |
| `JSON.ARRLEN key [path]` | array of lengths |
| `JSON.OBJKEYS key [path]` | array of arrays of keys |
| `JSON.MGET key [key ...] path` | array of JSON texts or nulls |

## Time series

| Command | Reply |
|---|---|
| `TS.CREATE key [RETENTION ms] [LABELS k v ...]` | OK |
| `TS.ADD key timestamp\|* value [RETENTION ms]` | integer timestamp. Creates the key if needed. |
| `TS.GET key` | array `[timestamp, value]` or empty array |
| `TS.RANGE key from\|- to\|+ [AGGREGATION avg\|sum\|min\|max\|count\|first\|last\|range bucket_ms]` | array of `[timestamp, value]`. Values are doubles. |
| `TS.INFO key` | map `{totalSamples, retentionTime, firstTimestamp, lastTimestamp, labels}` |

Samples are stored in chunks of 256. The newest chunk is a plain array, and older ones are Gorilla-compressed (delta-of-delta timestamps, XOR-encoded values). A regular series with slowly changing values takes about 1.2 bytes per sample instead of 16. Writing an older timestamp recompresses the one chunk it falls in.

## Vector similarity search

| Command | Reply |
|---|---|
| `VADD key id v1 v2 ... vn [ATTR json]` | integer 1 if new, 0 if replaced. The first vector sets the dimension. |
| `VSEARCH key K v1 ... vn [FILTER attr=value]` | array of `[id, score]`. The score is cosine similarity as a double, best first. |
| `VGET key id` | array of floats as bulk strings, or null |
| `VDEL key id` | integer 1/0 |
| `VCARD key` / `VDIM key` | integer |

`FILTER attr=value` keeps vectors whose ATTR JSON has that top-level field with that string or number value. Below 1000 vectors the search is exact. Larger sets are indexed with HNSW, built incrementally as vectors are added, and answered approximately (about 0.3 ms per query at 100k vectors). When a filter leaves fewer than K matches in the graph, the search falls back to an exact scan.

## Client-side caching

Send `HELLO 3`, then `CLIENT TRACKING ON [BCAST] [PREFIX p ...] [NOLOOP]`. Every key you read is remembered. When it changes, you receive the push `["invalidate", [key]]`. FLUSHALL sends `["invalidate", null]`.

## Compression

`CONFIG SET compression-threshold 4096` stores string values of 4 KB or more DEFLATE-compressed, which is transparent to clients. `OBJECT ENCODING key` returns `compressed` for such values.

## Semantic cache (for LLM apps)

This cache answers a prompt with the cached response of a *similar* earlier prompt. For example, "What's the capital of France?" can hit the cached answer of "capital of france?", which saves a slow, paid model call. The server never calls a model: clients send embedding vectors, so any embedding model works. Vectors are written as a JSON array (`[0.1,0.2,...]`) or comma-separated values.

| Command | Reply |
|---|---|
| `SCACHE.SET name vector response [PROMPT text] [EX s\|PX <duration>\|PXAT ms] [ID id]` | entry id |
| `SCACHE.GET name vector [PROMPT text] [THRESHOLD t]` | `[status, response, score, id]`. `status` is `hit` or `miss`. On a miss, `score` is the best similarity found, or null if the cache is empty. |
| `SCACHE.DEL name id` | integer 1/0 |
| `SCACHE.CONFIG name [THRESHOLD t] [MAXENTRIES n]` | OK. The default threshold is 0.92 (cosine) and MAXENTRIES defaults to 10000; when full, the least recently used entries are evicted. |
| `SCACHE.INFO name` | map `{entries, dim, threshold, maxentries, hits, misses, hit_ratio}` |
| `SCACHE.DROP name` | integer: entries deleted |

`PROMPT` stores the exact text, so a repeated identical prompt hits without a vector search. Cache names are namespaced. Caches are persisted (snapshot/AOF) and replicated. LRU eviction is decided by the leader and propagated as `SCACHE.DEL`.

TypeScript SDK:
```ts
const llm = cache.semanticCache("answers", { embed: (t) => openai.embed(t), threshold: 0.9, ttl: "1d" });
const answer = await llm.wrap(question, () => openai.chat(question));
```

## Bloom filters

RedisBloom-compatible syntax and replies. A filter answers "definitely not added" or "probably added". It never gives a false negative.

| Command | Reply |
|---|---|
| `BF.RESERVE key error_rate capacity [EXPANSION n] [NONSCALING]` | OK. Errors if the key exists. `0 < error_rate < 1`, `capacity > 0`, default EXPANSION 2. |
| `BF.ADD key item` | integer 1 if the item was added, 0 if it may already have been there. A missing key gets a filter with error 0.01 and capacity 100. |
| `BF.MADD key item [item ...]` | array of 1/0 (or an error per item, e.g. a full NONSCALING filter) |
| `BF.EXISTS key item` | integer 1 if the item may exist, 0 if it surely doesn't (also 0 for a missing key) |
| `BF.MEXISTS key item [item ...]` | array of 1/0 |
| `BF.CARD key` | integer: items added (0 for a missing key) |
| `BF.INFO key [CAPACITY\|SIZE\|FILTERS\|ITEMS\|EXPANSION]` | map `{Capacity, Size, Number of filters, Number of items inserted, Expansion rate}`, or a one-element array with the requested field. `Size` is bytes of memory. Expansion rate is null for NONSCALING filters. |

`TYPE` returns `MBbloom--`, RedisBloom's type name. The filter is scalable: when the newest sub-filter is full, one `EXPANSION` times larger is added with half the error rate. The first sub-filter uses `error_rate / 2`, so the compound false-positive rate stays below `error_rate` however far the filter grows. RedisBloom uses `error_rate` for the first sub-filter and lets the compound rate drift above it. A `NONSCALING` filter uses `error_rate` as is, and `BF.ADD` fails with `ERR non scaling filter is full` once it holds `capacity` items. Hashing is deterministic (FNV-1a with murmur finalisers, double hashing), so the filters are persisted and replicated bit for bit.

## HyperLogLog

Redis's algorithm and commands: 16384 6-bit registers, MurmurHash64A, and Ertl's improved estimator (standard error 0.81%). Small sketches use a sparse representation and switch to the dense 12 KB one at about 3 KB.

| Command | Reply |
|---|---|
| `PFADD key [element ...]` | integer 1 if a register changed (or the key was created), else 0 |
| `PFCOUNT key [key ...]` | integer: estimated cardinality. With several keys, the cardinality of their union. Missing keys count as empty. |
| `PFMERGE destkey [sourcekey ...]` | OK. destkey becomes the union of itself and the sources. |

**Deviation from Redis:** Redis stores a HyperLogLog as a specially encoded string, so `GET`/`SET` work on it and `TYPE` says `string`. Here it is its own kind: `TYPE` returns `hyperloglog`, string commands on it fail with WRONGTYPE, and you can't build one by `SET`ting Redis's binary format.
