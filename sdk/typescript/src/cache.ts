/**
 * Client-side caching: a local LRU of GET results, kept correct by the
 * server's invalidation pushes (CLIENT TRACKING ON over RESP3).
 *
 * The in-flight race: a GET is sent, the key changes, the invalidation push
 * arrives before the GET's reply. The reply may hold the old value, so it must
 * not be cached. Every read takes a token (key version + global epoch) when it
 * starts; an invalidation of that key (or a full flush/reconnect) bumps the
 * version, and endRead() only stores the value if the token is still current.
 */

export interface ClientCacheOptions {
  /** Maximum number of cached keys (LRU eviction). Default 10_000. */
  maxEntries?: number;
  /** Optional local expiry, on top of server invalidation. */
  ttlMs?: number;
}

export interface CacheStats {
  hits: number;
  misses: number;
  size: number;
  /** Keys invalidated by the server (a full flush counts as one). */
  invalidations: number;
}

interface Entry<V> {
  value: V;
  expiresAt: number;
}

/** A Map-based LRU: Map iteration order is insertion order, so the first key is the oldest. */
export class LruCache<V> {
  private readonly map = new Map<string, Entry<V>>();

  constructor(
    readonly maxEntries: number,
    private readonly ttlMs = 0,
    private readonly now: () => number = Date.now,
  ) {
    if (!(maxEntries > 0)) throw new RangeError('maxEntries must be > 0');
  }

  get size(): number {
    return this.map.size;
  }

  get(key: string): V | undefined {
    const e = this.map.get(key);
    if (!e) return undefined;
    if (e.expiresAt !== 0 && e.expiresAt <= this.now()) {
      this.map.delete(key);
      return undefined;
    }
    // Move to the most-recently-used end.
    this.map.delete(key);
    this.map.set(key, e);
    return e.value;
  }

  has(key: string): boolean {
    return this.get(key) !== undefined;
  }

  set(key: string, value: V): void {
    this.map.delete(key);
    this.map.set(key, { value, expiresAt: this.ttlMs > 0 ? this.now() + this.ttlMs : 0 });
    while (this.map.size > this.maxEntries) {
      const oldest = this.map.keys().next().value as string;
      this.map.delete(oldest);
    }
  }

  delete(key: string): boolean {
    return this.map.delete(key);
  }

  clear(): void {
    this.map.clear();
  }

  keys(): IterableIterator<string> {
    return this.map.keys();
  }
}

export interface ReadToken {
  readonly key: string;
  readonly epoch: number;
  readonly version: number;
}

export class ClientCache {
  private readonly lru: LruCache<string>;
  private epoch = 0;
  private readonly inflight = new Map<string, { count: number; version: number }>();
  private hits = 0;
  private misses = 0;
  private invalidations = 0;

  constructor(opts: ClientCacheOptions = {}, now?: () => number) {
    this.lru = new LruCache<string>(opts.maxEntries ?? 10_000, opts.ttlMs ?? 0, now);
  }

  /** Returns the cached value, counting a hit or a miss. */
  lookup(key: string): string | undefined {
    const v = this.lru.get(key);
    if (v === undefined) this.misses++;
    else this.hits++;
    return v;
  }

  /** Call before sending a GET whose result may be cached. */
  beginRead(key: string): ReadToken {
    let s = this.inflight.get(key);
    if (!s) {
      s = { count: 0, version: 0 };
      this.inflight.set(key, s);
    }
    s.count++;
    return { key, epoch: this.epoch, version: s.version };
  }

  /**
   * Call with the GET reply. Caches it unless the key was invalidated (or the
   * cache was flushed) while the read was in flight. Only strings are cached:
   * a missing key isn't, because tracking may not cover keys that don't exist.
   * Returns true if the value was cached.
   */
  endRead(token: ReadToken, value: unknown): boolean {
    const s = this.inflight.get(token.key);
    const fresh = token.epoch === this.epoch && s !== undefined && s.version === token.version;
    this.release(token.key, s);
    if (fresh && typeof value === 'string') {
      this.lru.set(token.key, value);
      return true;
    }
    return false;
  }

  /** Call when the GET failed. */
  abortRead(token: ReadToken): void {
    this.release(token.key, this.inflight.get(token.key));
  }

  /**
   * Handles an invalidation: a list of keys, or null for "everything"
   * (FLUSHALL). `fromServer` controls whether it counts in the stats.
   */
  invalidate(keys: readonly string[] | null, fromServer = true): void {
    if (keys === null) {
      if (fromServer) this.invalidations++;
      this.clear();
      return;
    }
    for (const k of keys) {
      if (fromServer) this.invalidations++;
      this.lru.delete(k);
      const s = this.inflight.get(k);
      if (s) s.version++;
    }
  }

  /** Drops everything and makes every in-flight read stale (used on reconnect). */
  clear(): void {
    this.lru.clear();
    this.epoch++;
  }

  stats(): CacheStats {
    return { hits: this.hits, misses: this.misses, size: this.lru.size, invalidations: this.invalidations };
  }

  private release(key: string, s: { count: number; version: number } | undefined): void {
    if (s && --s.count <= 0) this.inflight.delete(key);
  }
}
