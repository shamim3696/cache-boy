/**
 * The typed command API, shared by the TCP client (CacheBoy), the HTTP client
 * (CacheBoyHttp) and pipelines. Everything is implemented over one abstract
 * method: `send(args)`.
 */
import { CacheBoyError, ErrorCodes } from './errors.js';
import { type LockInfo, type LockOptions, Lock, acquireLock, lockInfo, withLock } from './features/lock.js';
import { JsonDocs } from './features/json.js';
import { Queue } from './features/queue.js';
import { type RateLimitResult, rateLimit, rateLimitPeek, rateLimitReset } from './features/ratelimit.js';
import { SemanticCache, type SemanticCacheOptions } from './features/semcache.js';
import { Tags } from './features/tags.js';
import { TimeSeries } from './features/timeseries.js';
import { Vectors } from './features/vector.js';
import { type WrapOptions, wrap } from './features/wrap.js';
import {
  type Arg,
  type Duration,
  type Executor,
  type SendOptions,
  type StorableValue,
  encodeValue,
  toArr,
  toBool,
  toNum,
  toNumOrNull,
  toRecord,
  toStr,
  toStrArr,
  toStrRecord,
} from './types.js';

export interface SetOptions {
  /** Expire after this many seconds. */
  ex?: number;
  /** Expire after this many milliseconds. */
  px?: number;
  /** Expire at this unix time (seconds). */
  exat?: number;
  /** Expire at this unix time (milliseconds). */
  pxat?: number;
  /** Only set if the key doesn't exist. */
  nx?: boolean;
  /** Only set if the key exists. */
  xx?: boolean;
  /** Keep the current TTL. */
  keepTtl?: boolean;
  /** Return the old value instead of OK. */
  get?: boolean;
  /** Attach tags (cache-boy TAGS option). */
  tags?: string[];
}

export interface GetJSONOptions<T> {
  /** Validates/transforms the parsed JSON, e.g. a zod schema's `parse`. */
  parse?: (v: unknown) => T;
}

export interface HistoryEntry {
  version: number;
  timestampMs: number;
  /** null records a deletion. */
  value: string | null;
}

export interface ScoredMember {
  member: string;
  score: number;
}

/** Callback-based unsubscribe returned by subscribe/psubscribe/keyWatch. */
export type Unsubscribe = () => Promise<void>;

/** [error, result] for each command of a pipeline or transaction, ioredis-style. */
export type BatchResult = [CacheBoyError | null, unknown];
export type BatchItem = { ok: true; value: unknown } | { ok: false; error: CacheBoyError };

/** Methods that need several round trips (or a live connection) and can't be queued in a pipeline. */
type NonBatchable =
  | 'wrap'
  | 'withLock'
  | 'subscribe'
  | 'psubscribe'
  | 'keyWatch'
  | 'scanAll'
  | 'evalCached'
  | 'send'
  | 'pipeline'
  | 'multi'
  | 'namespace'
  | 'queue';

/**
 * A chainable batch: call any typed command, then `exec()`.
 *
 *     const [[, a], [, n]] = await client.pipeline().get('a').incr('n').exec();
 */
export type Pipeline = {
  [K in Exclude<keyof CommandsBase, NonBatchable> as CommandsBase[K] extends (...a: never[]) => Promise<unknown> ? K : never]: CommandsBase[K] extends (
    ...a: infer A
  ) => Promise<unknown>
    ? (...args: A) => Pipeline
    : never;
} & {
  /** Runs the queued commands. Each entry is [error, typedResult]. */
  exec(): Promise<BatchResult[]>;
  /** Number of commands queued so far. */
  readonly length: number;
};

export abstract class CommandsBase implements Executor {
  /** Sends one command and resolves with its decoded reply (rejects with CacheBoyError). */
  abstract send(args: Arg[], opts?: SendOptions): Promise<unknown>;

  /** A view of this client bound to another namespace. */
  abstract namespace(name: string): CommandsBase;

  /** Runs a batch: atomic = MULTI/EXEC semantics. */
  protected abstract execBatch(cmds: Arg[][], atomic: boolean): Promise<BatchItem[]>;

  /** Executor for commands that block (QPOP BLOCK, BLPOP...). TCP uses a dedicated connection. */
  protected blockingExecutor(): Executor {
    return this;
  }

  /** Hook for local caches: called before a write to these keys is sent. */
  protected beforeWrite(_keys: readonly string[]): void {}

  // ---------------------------------------------------------------- generic

  /** Runs any command: `call('OBJECT', 'ENCODING', 'k')`. */
  call(...args: Arg[]): Promise<unknown> {
    return this.send(args);
  }

  /** A batch sent in one round trip (not atomic). */
  pipeline(): Pipeline {
    return makeBatch(this, (cmds) => this.execBatch(cmds, false));
  }

  /** A transaction (MULTI/EXEC; /v1/multi over HTTP). */
  multi(): Pipeline {
    return makeBatch(this, (cmds) => this.execBatch(cmds, true));
  }

  async ping(message?: string): Promise<string> {
    return toStr(await this.send(message === undefined ? ['PING'] : ['PING', message])) ?? '';
  }

  async echo(message: string): Promise<string> {
    return toStr(await this.send(['ECHO', message])) ?? '';
  }

  // ---------------------------------------------------------------- strings

  async get(key: string): Promise<string | null> {
    return toStr(await this.send(['GET', key]));
  }

  /** GET returning raw bytes. */
  async getBuffer(key: string): Promise<Buffer | null> {
    const v = await this.send(['GET', key], { buffers: true });
    if (v === null || v === undefined) return null;
    return Buffer.isBuffer(v) ? v : v instanceof Uint8Array ? Buffer.from(v) : Buffer.from(String(v), 'utf8');
  }

  /** GET + JSON.parse (+ optional validation). Null if the key doesn't exist. */
  async getJSON<T = unknown>(key: string, opts: GetJSONOptions<T> = {}): Promise<T | null> {
    const s = await this.get(key);
    if (s === null) return null;
    const v: unknown = JSON.parse(s);
    return opts.parse ? opts.parse(v) : (v as T);
  }

  /**
   * SET. Objects (and booleans) are JSON-encoded; Buffers are stored as-is.
   * Resolves to "OK", null when NX/XX wasn't met, or the old value with `get`.
   */
  async set(key: string, value: StorableValue, opts: SetOptions = {}): Promise<string | null> {
    const args: Arg[] = ['SET', key, encodeValue(value)];
    if (opts.ex !== undefined) args.push('EX', opts.ex);
    else if (opts.px !== undefined) args.push('PX', opts.px);
    else if (opts.exat !== undefined) args.push('EXAT', opts.exat);
    else if (opts.pxat !== undefined) args.push('PXAT', opts.pxat);
    else if (opts.keepTtl) args.push('KEEPTTL');
    if (opts.nx) args.push('NX');
    else if (opts.xx) args.push('XX');
    if (opts.get) args.push('GET');
    if (opts.tags && opts.tags.length > 0) args.push('TAGS', ...opts.tags);
    this.beforeWrite([key]);
    return toStr(await this.send(args));
  }

  /** SET with JSON.stringify, whatever the value type (strings get quoted). */
  setJSON(key: string, value: unknown, opts: SetOptions = {}): Promise<string | null> {
    return this.set(key, JSON.stringify(value), opts);
  }

  async mget(...keys: string[]): Promise<(string | null)[]> {
    return toArr(await this.send(['MGET', ...keys])).map(toStr);
  }

  async mset(entries: Record<string, StorableValue>): Promise<void> {
    const args: Arg[] = ['MSET'];
    const keys = Object.keys(entries);
    for (const k of keys) args.push(k, encodeValue(entries[k]!));
    this.beforeWrite(keys);
    await this.send(args);
  }

  async getdel(key: string): Promise<string | null> {
    this.beforeWrite([key]);
    return toStr(await this.send(['GETDEL', key]));
  }

  async incr(key: string): Promise<number> {
    this.beforeWrite([key]);
    return toNum(await this.send(['INCR', key]));
  }

  async incrBy(key: string, by: number): Promise<number> {
    this.beforeWrite([key]);
    return toNum(await this.send(['INCRBY', key, by]));
  }

  async incrByFloat(key: string, by: number): Promise<number> {
    this.beforeWrite([key]);
    return toNum(await this.send(['INCRBYFLOAT', key, by]));
  }

  async decr(key: string): Promise<number> {
    this.beforeWrite([key]);
    return toNum(await this.send(['DECR', key]));
  }

  async decrBy(key: string, by: number): Promise<number> {
    this.beforeWrite([key]);
    return toNum(await this.send(['DECRBY', key, by]));
  }

  async append(key: string, value: string | Buffer): Promise<number> {
    this.beforeWrite([key]);
    return toNum(await this.send(['APPEND', key, value]));
  }

  async strlen(key: string): Promise<number> {
    return toNum(await this.send(['STRLEN', key]));
  }

  // ---------------------------------------------------------------- keys

  async del(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    this.beforeWrite(keys);
    return toNum(await this.send(['DEL', ...keys]));
  }

  async unlink(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    this.beforeWrite(keys);
    return toNum(await this.send(['UNLINK', ...keys]));
  }

  async exists(...keys: string[]): Promise<number> {
    return toNum(await this.send(['EXISTS', ...keys]));
  }

  async expire(key: string, seconds: number): Promise<boolean> {
    return toBool(await this.send(['EXPIRE', key, seconds]));
  }

  async pexpire(key: string, ms: number): Promise<boolean> {
    return toBool(await this.send(['PEXPIRE', key, ms]));
  }

  async persist(key: string): Promise<boolean> {
    return toBool(await this.send(['PERSIST', key]));
  }

  /** Seconds to live; -1 no expiry, -2 missing key. */
  async ttl(key: string): Promise<number> {
    return toNum(await this.send(['TTL', key]));
  }

  async pttl(key: string): Promise<number> {
    return toNum(await this.send(['PTTL', key]));
  }

  async type(key: string): Promise<string> {
    return toStr(await this.send(['TYPE', key])) ?? 'none';
  }

  async rename(key: string, newKey: string): Promise<void> {
    this.beforeWrite([key, newKey]);
    await this.send(['RENAME', key, newKey]);
  }

  async keys(pattern: string): Promise<string[]> {
    return toStrArr(await this.send(['KEYS', pattern]));
  }

  /** One SCAN step. */
  async scan(cursor: string | number, opts: { match?: string; count?: number; type?: string } = {}): Promise<[string, string[]]> {
    const args: Arg[] = ['SCAN', cursor];
    if (opts.match !== undefined) args.push('MATCH', opts.match);
    if (opts.count !== undefined) args.push('COUNT', opts.count);
    if (opts.type !== undefined) args.push('TYPE', opts.type);
    const a = toArr(await this.send(args));
    return [toStr(a[0]) ?? '0', toStrArr(a[1])];
  }

  /** Iterates every key matching `match` with SCAN. */
  async *scanAll(opts: { match?: string; count?: number; type?: string } = {}): AsyncGenerator<string> {
    let cursor = '0';
    do {
      const [next, keys] = await this.scan(cursor, opts);
      cursor = next;
      yield* keys;
    } while (cursor !== '0');
  }

  async dbsize(): Promise<number> {
    return toNum(await this.send(['DBSIZE']));
  }

  async flushdb(): Promise<void> {
    await this.send(['FLUSHDB']);
  }

  // ---------------------------------------------------------------- hashes

  /** HSET key field value, or HSET key {field: value, ...}. Returns fields added. */
  async hset(key: string, fieldOrEntries: string | Record<string, StorableValue>, value?: StorableValue): Promise<number> {
    const args: Arg[] = ['HSET', key];
    if (typeof fieldOrEntries === 'string') {
      if (value === undefined) throw new TypeError('hset: value is required');
      args.push(fieldOrEntries, encodeValue(value));
    } else {
      for (const [f, v] of Object.entries(fieldOrEntries)) args.push(f, encodeValue(v));
    }
    return toNum(await this.send(args));
  }

  async hget(key: string, field: string): Promise<string | null> {
    return toStr(await this.send(['HGET', key, field]));
  }

  async hmget(key: string, ...fields: string[]): Promise<(string | null)[]> {
    return toArr(await this.send(['HMGET', key, ...fields])).map(toStr);
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return toStrRecord(await this.send(['HGETALL', key]));
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    return toNum(await this.send(['HDEL', key, ...fields]));
  }

  async hexists(key: string, field: string): Promise<boolean> {
    return toBool(await this.send(['HEXISTS', key, field]));
  }

  async hincrBy(key: string, field: string, by: number): Promise<number> {
    return toNum(await this.send(['HINCRBY', key, field, by]));
  }

  async hkeys(key: string): Promise<string[]> {
    return toStrArr(await this.send(['HKEYS', key]));
  }

  async hvals(key: string): Promise<string[]> {
    return toStrArr(await this.send(['HVALS', key]));
  }

  async hlen(key: string): Promise<number> {
    return toNum(await this.send(['HLEN', key]));
  }

  // ---------------------------------------------------------------- lists

  async lpush(key: string, ...values: StorableValue[]): Promise<number> {
    return toNum(await this.send(['LPUSH', key, ...values.map(encodeValue)]));
  }

  async rpush(key: string, ...values: StorableValue[]): Promise<number> {
    return toNum(await this.send(['RPUSH', key, ...values.map(encodeValue)]));
  }

  async lpop(key: string): Promise<string | null> {
    return toStr(await this.send(['LPOP', key]));
  }

  async rpop(key: string): Promise<string | null> {
    return toStr(await this.send(['RPOP', key]));
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    return toStrArr(await this.send(['LRANGE', key, start, stop]));
  }

  async llen(key: string): Promise<number> {
    return toNum(await this.send(['LLEN', key]));
  }

  async lindex(key: string, index: number): Promise<string | null> {
    return toStr(await this.send(['LINDEX', key, index]));
  }

  async lrem(key: string, count: number, value: string): Promise<number> {
    return toNum(await this.send(['LREM', key, count, value]));
  }

  async ltrim(key: string, start: number, stop: number): Promise<void> {
    await this.send(['LTRIM', key, start, stop]);
  }

  /** BLPOP with a timeout in seconds (0 = forever, TCP only). Returns [key, value] or null. */
  async blpop(keys: string | string[], timeoutSec: number): Promise<[string, string] | null> {
    return this.bpop('BLPOP', keys, timeoutSec);
  }

  async brpop(keys: string | string[], timeoutSec: number): Promise<[string, string] | null> {
    return this.bpop('BRPOP', keys, timeoutSec);
  }

  private async bpop(cmd: string, keys: string | string[], timeoutSec: number): Promise<[string, string] | null> {
    const ks = Array.isArray(keys) ? keys : [keys];
    const v = await this.blockingExecutor().send([cmd, ...ks, timeoutSec], {
      timeoutMs: timeoutSec === 0 ? 0 : timeoutSec * 1000 + 10_000,
      retryable: false,
    });
    if (v === null || v === undefined) return null;
    const a = toArr(v);
    return [toStr(a[0]) ?? '', toStr(a[1]) ?? ''];
  }

  // ---------------------------------------------------------------- sets

  async sadd(key: string, ...members: string[]): Promise<number> {
    return toNum(await this.send(['SADD', key, ...members]));
  }

  async srem(key: string, ...members: string[]): Promise<number> {
    return toNum(await this.send(['SREM', key, ...members]));
  }

  async smembers(key: string): Promise<string[]> {
    return toStrArr(await this.send(['SMEMBERS', key]));
  }

  async sismember(key: string, member: string): Promise<boolean> {
    return toBool(await this.send(['SISMEMBER', key, member]));
  }

  async scard(key: string): Promise<number> {
    return toNum(await this.send(['SCARD', key]));
  }

  async sinter(...keys: string[]): Promise<string[]> {
    return toStrArr(await this.send(['SINTER', ...keys]));
  }

  async sunion(...keys: string[]): Promise<string[]> {
    return toStrArr(await this.send(['SUNION', ...keys]));
  }

  // ---------------------------------------------------------------- sorted sets

  /** ZADD key score member, or ZADD key [{score, member}, ...]. Returns members added. */
  async zadd(key: string, scoreOrMembers: number | ScoredMember[], member?: string): Promise<number> {
    const args: Arg[] = ['ZADD', key];
    if (typeof scoreOrMembers === 'number') {
      if (member === undefined) throw new TypeError('zadd: member is required');
      args.push(scoreOrMembers, member);
    } else {
      for (const m of scoreOrMembers) args.push(m.score, m.member);
    }
    return toNum(await this.send(args));
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    return toNum(await this.send(['ZREM', key, ...members]));
  }

  async zscore(key: string, member: string): Promise<number | null> {
    return toNumOrNull(await this.send(['ZSCORE', key, member]));
  }

  async zincrBy(key: string, by: number, member: string): Promise<number> {
    return toNum(await this.send(['ZINCRBY', key, by, member]));
  }

  async zcard(key: string): Promise<number> {
    return toNum(await this.send(['ZCARD', key]));
  }

  async zrank(key: string, member: string): Promise<number | null> {
    return toNumOrNull(await this.send(['ZRANK', key, member]));
  }

  async zrange(key: string, start: number, stop: number, opts: { rev?: boolean } = {}): Promise<string[]> {
    const args: Arg[] = ['ZRANGE', key, start, stop];
    if (opts.rev) args.push('REV');
    return toStrArr(await this.send(args));
  }

  async zrangeWithScores(key: string, start: number, stop: number, opts: { rev?: boolean } = {}): Promise<ScoredMember[]> {
    const args: Arg[] = ['ZRANGE', key, start, stop];
    if (opts.rev) args.push('REV');
    args.push('WITHSCORES');
    const a = toArr(await this.send(args));
    const out: ScoredMember[] = [];
    if (a.length > 0 && Array.isArray(a[0])) {
      // RESP3: [[member, score], ...]
      for (const e of a as unknown[][]) out.push({ member: toStr(e[0]) ?? '', score: toNum(e[1]) });
    } else {
      for (let i = 0; i + 1 < a.length; i += 2) out.push({ member: toStr(a[i]) ?? '', score: toNum(a[i + 1]) });
    }
    return out;
  }

  // ---------------------------------------------------------------- pub/sub

  async publish(channel: string, message: string | Buffer): Promise<number> {
    return toNum(await this.send(['PUBLISH', channel, message]));
  }

  /** Subscribes to channels. Resolves to an unsubscribe function. (TCP only.) */
  subscribe(_channels: string | string[], _cb: (message: string, channel: string) => void): Promise<Unsubscribe> {
    return Promise.reject(unsupported('SUBSCRIBE', 'pub/sub needs a persistent connection'));
  }

  /** Subscribes to glob patterns. (TCP only.) */
  psubscribe(_patterns: string | string[], _cb: (message: string, channel: string, pattern: string) => void): Promise<Unsubscribe> {
    return Promise.reject(unsupported('PSUBSCRIBE', 'pub/sub needs a persistent connection'));
  }

  /** Streams key changes matching the pattern: cb(key, op). (TCP only.) */
  keyWatch(_patterns: string | string[], _cb: (key: string, op: string) => void): Promise<Unsubscribe> {
    return Promise.reject(unsupported('KEYWATCH', 'key change streams need a persistent connection (use webhooks for serverless)'));
  }

  // ---------------------------------------------------------------- cache-boy features

  /** Tag-based invalidation. */
  get tags(): Tags {
    return new Tags(this);
  }

  /** Native JSON documents. */
  get json(): JsonDocs {
    return new JsonDocs(this);
  }

  /** Time series. */
  get ts(): TimeSeries {
    return new TimeSeries(this);
  }

  /** Vector similarity search. */
  get vector(): Vectors {
    return new Vectors(this);
  }

  /** Cache-aside with stampede protection (CACHE.GET). See WrapOptions. */
  wrap<T>(key: string, fn: () => Promise<T> | T, opts: WrapOptions<T>): Promise<T> {
    this.beforeWrite([key]);
    return wrap(this, key, fn, opts, undefined, this.blockingExecutor());
  }

  /** Releases a compute lock taken by CACHE.GET. */
  async cacheRelease(key: string): Promise<boolean> {
    return toNum(await this.send(['CACHE.RELEASE', key])) === 1;
  }

  /** Semantic cache for LLM responses: hits on *similar* prompts. Bring your own embedding function. */
  semanticCache(name: string, opts: SemanticCacheOptions): SemanticCache {
    return new SemanticCache(this, name, opts);
  }

  /** GCRA rate limit: `limit` requests per `period` (ms, or "1m", "10s"...). */
  rateLimit(key: string, limit: number, period: Duration, opts: { cost?: number } = {}): Promise<RateLimitResult> {
    return rateLimit(this, key, limit, period, opts);
  }

  /** Like rateLimit, without consuming anything. */
  rateLimitPeek(key: string, limit: number, period: Duration): Promise<RateLimitResult> {
    return rateLimitPeek(this, key, limit, period);
  }

  rateLimitReset(key: string): Promise<boolean> {
    return rateLimitReset(this, key);
  }

  /** Acquires a distributed lock with a fencing token, or resolves to null. */
  lock(name: string, ttlMs: Duration, opts: LockOptions = {}): Promise<Lock | null> {
    return acquireLock(this, name, ttlMs, opts);
  }

  /** Runs fn under the lock; throws CacheBoyError(LOCKED) if it can't be acquired. */
  withLock<T>(name: string, ttlMs: Duration, fn: (lock: Lock) => Promise<T> | T, opts: LockOptions = {}): Promise<T> {
    return withLock(this, name, ttlMs, fn, opts);
  }

  lockInfo(name: string): Promise<LockInfo | null> {
    return lockInfo(this, name);
  }

  /** A job queue handle. T is the payload type. */
  queue<T = unknown>(name: string): Queue<T> {
    return new Queue<T>(this, name, () => this.blockingExecutor());
  }

  /** Versions of a string key, newest first (needs `CONFIG SET history-depth N`). */
  async history(key: string, opts: { count?: number } = {}): Promise<HistoryEntry[]> {
    const args: Arg[] = ['HISTORY', key];
    if (opts.count !== undefined) args.push('COUNT', opts.count);
    return toArr(await this.send(args)).map((e) => {
      const a = toArr(e);
      return { version: toNum(a[0]), timestampMs: toNum(a[1]), value: toStr(a[2]) };
    });
  }

  async getVersion(key: string, version: number): Promise<string | null> {
    return toStr(await this.send(['GETVERSION', key, version]));
  }

  async restoreVersion(key: string, version: number): Promise<void> {
    this.beforeWrite([key]);
    await this.send(['RESTOREVERSION', key, version]);
  }

  async clearHistory(key: string): Promise<boolean> {
    return toNum(await this.send(['HISTORY.CLEAR', key])) === 1;
  }

  /** Runs server-side JavaScript. `KEYS` and `ARGV` are available in the script; use cb.call(). */
  eval(script: string, keys: string[] = [], args: Arg[] = []): Promise<unknown> {
    return this.send(['EVAL', script, keys.length, ...keys, ...args], { retryable: false });
  }

  evalsha(sha1: string, keys: string[] = [], args: Arg[] = []): Promise<unknown> {
    return this.send(['EVALSHA', sha1, keys.length, ...keys, ...args], { retryable: false });
  }

  /** Loads a script and returns its sha1. */
  async scriptLoad(script: string): Promise<string> {
    return toStr(await this.send(['SCRIPT', 'LOAD', script])) ?? '';
  }

  async scriptExists(...sha1s: string[]): Promise<boolean[]> {
    return toArr(await this.send(['SCRIPT', 'EXISTS', ...sha1s])).map(toBool);
  }

  /** EVALSHA, falling back to SCRIPT LOAD + EVALSHA on NOSCRIPT. */
  async evalCached(script: string, keys: string[] = [], args: Arg[] = []): Promise<unknown> {
    const sha = await sha1Hex(script);
    try {
      return await this.evalsha(sha, keys, args);
    } catch (err) {
      if (!(err instanceof CacheBoyError) || err.code !== 'NOSCRIPT') throw err;
      await this.scriptLoad(script);
      return this.evalsha(sha, keys, args);
    }
  }

  // ---------------------------------------------------------------- namespaces (admin)

  async namespaceCurrent(): Promise<string> {
    return toStr(await this.send(['NAMESPACE', 'CURRENT'])) ?? 'default';
  }

  /** Namespace → key count. */
  async namespaceList(): Promise<Record<string, number>> {
    const r = toRecord(await this.send(['NAMESPACE', 'LIST']));
    const out: Record<string, number> = {};
    for (const k of Object.keys(r)) out[k] = toNum(r[k]);
    return out;
  }

  async namespaceDrop(name: string): Promise<number> {
    return toNum(await this.send(['NAMESPACE', 'DROP', name]));
  }
}

async function sha1Hex(s: string): Promise<string> {
  const { createHash } = await import('node:crypto');
  return createHash('sha1').update(s).digest('hex');
}

export function unsupported(command: string, why: string): CacheBoyError {
  return new CacheBoyError(`${command} is not supported by this client: ${why}`, ErrorCodes.UNSUPPORTED, { command });
}

// ---------------------------------------------------------------------------
// Pipelines / transactions

const NON_BATCHABLE = new Set<string>([
  'wrap', 'withLock', 'subscribe', 'psubscribe', 'keyWatch', 'scanAll', 'evalCached', 'pipeline', 'multi', 'namespace',
  'blpop', 'brpop', 'queue', 'semanticCache',
]);

class BatchRecorder extends CommandsBase {
  readonly cmds: { args: Arg[]; resolve: (v: unknown) => void; reject: (e: Error) => void }[] = [];
  readonly results: Promise<BatchResult>[] = [];
  closed = false;

  send(args: Arg[]): Promise<unknown> {
    if (this.closed) {
      return Promise.reject(new CacheBoyError('This pipeline was already executed; this method needs more than one round trip and cannot be used in a pipeline', ErrorCodes.UNSUPPORTED));
    }
    return new Promise((resolve, reject) => this.cmds.push({ args, resolve, reject }));
  }

  namespace(): CommandsBase {
    throw new CacheBoyError('namespace() is not available inside a pipeline', ErrorCodes.UNSUPPORTED);
  }

  protected execBatch(): Promise<BatchItem[]> {
    return Promise.reject(new CacheBoyError('pipelines cannot be nested', ErrorCodes.UNSUPPORTED));
  }
}

function toCbError(e: unknown): CacheBoyError {
  return e instanceof CacheBoyError ? e : new CacheBoyError(e instanceof Error ? e.message : String(e), undefined, { cause: e });
}

function makeBatch(_parent: CommandsBase, run: (cmds: Arg[][]) => Promise<BatchItem[]>): Pipeline {
  const rec = new BatchRecorder();
  let executed = false;
  const exec = async (): Promise<BatchResult[]> => {
    if (executed) throw new CacheBoyError('pipeline already executed', ErrorCodes.UNSUPPORTED);
    executed = true;
    // Let typed methods that do sync work before send() finish queuing.
    await Promise.resolve();
    rec.closed = true;
    const cmds = rec.cmds.slice();
    if (cmds.length > 0) {
      let items: BatchItem[];
      try {
        items = await run(cmds.map((c) => c.args));
      } catch (err) {
        const e = toCbError(err);
        for (const c of cmds) c.reject(e);
        await Promise.all(rec.results);
        throw e;
      }
      cmds.forEach((c, i) => {
        const it = items[i];
        if (!it) c.reject(new CacheBoyError('Missing reply in batch', ErrorCodes.BAD_REPLY));
        else if (it.ok) c.resolve(it.value);
        else c.reject(it.error);
      });
    }
    return Promise.all(rec.results);
  };
  const proxy: Pipeline = new Proxy(rec, {
    get(target, prop) {
      if (prop === 'exec') return exec;
      if (prop === 'length') return target.results.length;
      if (prop === 'then') return undefined; // not a thenable
      const v: unknown = Reflect.get(target, prop, target);
      if (typeof v !== 'function' || typeof prop !== 'string') return v;
      if (NON_BATCHABLE.has(prop) || prop === 'send' || prop.startsWith('_')) {
        return () => {
          throw new CacheBoyError(`${prop}() cannot be used in a pipeline`, ErrorCodes.UNSUPPORTED);
        };
      }
      return (...args: unknown[]) => {
        if (executed) throw new CacheBoyError('pipeline already executed', ErrorCodes.UNSUPPORTED);
        let p: Promise<unknown>;
        try {
          p = Promise.resolve((v as (...a: unknown[]) => unknown).apply(target, args));
        } catch (err) {
          p = Promise.reject(err);
        }
        target.results.push(p.then((r): BatchResult => [null, r], (e: unknown): BatchResult => [toCbError(e), null]));
        return proxy;
      };
    },
  }) as unknown as Pipeline;
  return proxy;
}
