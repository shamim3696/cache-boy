/**
 * CacheBoy: the TCP client (RESP3, auto-pipelining, client-side caching, pub/sub).
 */
import { type CacheStats, type ClientCacheOptions, ClientCache } from './cache.js';
import { type BatchItem, CommandsBase, type Unsubscribe } from './commands.js';
import { Connection, type ConnectionOptions, type ConnectionStatus } from './connection.js';
import { CacheBoyError, ErrorCodes } from './errors.js';
import { globMatch } from './glob.js';
import type { Arg } from './resp.js';
import { type Executor, type SendOptions, commandName, toArr, toStr } from './types.js';

export interface CacheBoyOptions extends ConnectionOptions {
  /** `redis://[user:pass@]host[:port][/db][?namespace=ns&name=client]`, or `rediss://` for TLS. */
  url?: string;
  /** Database index (SELECT) sent after the handshake. Usually set through the URL path. */
  db?: number;
  /**
   * Client-side caching of GET results, kept fresh by server invalidation
   * pushes (CLIENT TRACKING ON). Needs protocol 3.
   */
  clientCache?: ClientCacheOptions | boolean;
}

export type ClientEvent = 'connect' | 'ready' | 'error' | 'close' | 'reconnecting' | 'end' | 'push';

type MessageCb = (message: string, channel: string) => void;
type PMessageCb = (message: string, channel: string, pattern: string) => void;
type KeyEventCb = (key: string, op: string) => void;

/** Commands that change string keys whose GET result may be cached locally. */
const WRITES_FIRST_KEY = new Set([
  'SET', 'SETEX', 'PSETEX', 'SETNX', 'GETSET', 'GETDEL', 'GETEX', 'APPEND', 'SETRANGE',
  'INCR', 'INCRBY', 'INCRBYFLOAT', 'DECR', 'DECRBY', 'RESTOREVERSION', 'EXPIRE', 'PEXPIRE',
  'EXPIREAT', 'PEXPIREAT', 'PERSIST', 'MOVE', 'RESTORE', 'LMOVE',
]);
const WRITES_ALL_KEYS = new Set(['DEL', 'UNLINK', 'RENAME', 'RENAMENX', 'COPY']);

/** Parses a redis:// or rediss:// URL into connection options. */
export function parseUrl(url: string): CacheBoyOptions {
  const u = new URL(url);
  const out: CacheBoyOptions = {};
  if (u.protocol === 'rediss:' || u.protocol === 'cacheboys:') out.tls = true;
  else if (u.protocol !== 'redis:' && u.protocol !== 'cacheboy:') throw new TypeError(`Unsupported URL scheme ${u.protocol}`);
  if (u.hostname) out.host = u.hostname.replace(/^\[(.*)\]$/, '$1');
  if (u.port) out.port = Number(u.port);
  if (u.username) out.username = decodeURIComponent(u.username);
  if (u.password) out.password = decodeURIComponent(u.password);
  // redis://:password@host means "password only".
  if (u.username === '' && u.password) delete out.username;
  const db = u.pathname.replace(/^\//, '');
  if (db) out.db = Number(db);
  const ns = u.searchParams.get('namespace');
  if (ns) out.namespace = ns;
  const name = u.searchParams.get('name');
  if (name) out.clientName = name;
  const proto = u.searchParams.get('protocol');
  if (proto === '2' || proto === '3') out.protocol = Number(proto) as 2 | 3;
  return out;
}

export class CacheBoy extends CommandsBase {
  readonly options: Readonly<CacheBoyOptions>;
  private readonly conn: Connection;
  private readonly cache: ClientCache | undefined;
  private readonly nsPrefix: string;
  private sub: Connection | undefined;
  private readonly blockingPool: Connection[] = [];
  private readonly blockingIdle: Connection[] = [];
  private readonly views = new Map<string, CacheBoy>();
  private readonly channelCbs = new Map<string, Set<MessageCb>>();
  private readonly patternCbs = new Map<string, Set<PMessageCb>>();
  private readonly watchCbs = new Map<string, Set<KeyEventCb>>();

  /**
   * new CacheBoy()                                  → 127.0.0.1:6379
   * new CacheBoy('redis://:secret@host:6380?namespace=shop')
   * new CacheBoy({ host, port, password, clientCache: { maxEntries: 10_000 } })
   */
  constructor(urlOrOptions?: string | CacheBoyOptions, options: CacheBoyOptions = {}) {
    super();
    const base = typeof urlOrOptions === 'string' ? { ...options, url: urlOrOptions } : { ...(urlOrOptions ?? {}), ...options };
    const opts: CacheBoyOptions = base.url ? { ...parseUrl(base.url), ...stripUndefined(base) } : base;
    this.options = opts;
    const ns = opts.namespace && opts.namespace !== 'default' ? opts.namespace : '';
    this.nsPrefix = ns ? `\x00${ns}\x00` : '';

    if (opts.clientCache) {
      if (opts.protocol === 2) throw new CacheBoyError('clientCache needs protocol 3 (invalidations arrive as RESP3 pushes)', ErrorCodes.UNSUPPORTED);
      this.cache = new ClientCache(opts.clientCache === true ? {} : opts.clientCache);
    }
    const cache = this.cache;
    this.conn = new Connection(opts, {
      onPush: (data) => this.onMainPush(data),
      init: () => {
        const cmds: Array<{ args: Arg[] }> = [];
        if (opts.db) cmds.push({ args: ['SELECT', opts.db] });
        if (cache) cmds.push({ args: ['CLIENT', 'TRACKING', 'ON'] });
        return cmds;
      },
    });
    // Invalidations sent while we were disconnected are lost: start from scratch.
    if (cache) this.conn.on('close', () => cache.clear());
  }

  /** Connection state of the main connection. */
  get status(): ConnectionStatus {
    return this.conn.status;
  }

  /** The HELLO reply (server, version, proto, id, mode, role...). */
  get serverInfo(): Record<string, unknown> | undefined {
    return this.conn.serverInfo;
  }

  /** Resolves when the main connection is ready (useful with lazyConnect). */
  connect(): Promise<void> {
    return this.conn.connect();
  }

  on(event: ClientEvent, listener: (...args: any[]) => void): this {
    this.conn.on(event, listener);
    return this;
  }

  once(event: ClientEvent, listener: (...args: any[]) => void): this {
    this.conn.once(event, listener);
    return this;
  }

  off(event: ClientEvent, listener: (...args: any[]) => void): this {
    this.conn.off(event, listener);
    return this;
  }

  send(args: Arg[], opts?: SendOptions): Promise<unknown> {
    return this.conn.send(args, opts);
  }

  /** Runs any command. Local cache entries of keys it writes are dropped first. */
  override call(...args: Arg[]): Promise<unknown> {
    this.noteWrite(args);
    return this.send(args);
  }

  // ---------------------------------------------------------------- client-side caching

  override async get(key: string): Promise<string | null> {
    const c = this.cache;
    if (!c) return super.get(key);
    const hit = c.lookup(key);
    if (hit !== undefined) return hit;
    const token = c.beginRead(key);
    try {
      const v = toStr(await this.send(['GET', key]));
      c.endRead(token, v);
      return v;
    } catch (err) {
      c.abortRead(token);
      throw err;
    }
  }

  /** Hits, misses, size and server invalidations of the local cache (all 0 when disabled). */
  cacheStats(): CacheStats {
    return this.cache?.stats() ?? { hits: 0, misses: 0, size: 0, invalidations: 0 };
  }

  /** Drops the local cache. */
  clearLocalCache(): void {
    this.cache?.clear();
  }

  protected override beforeWrite(keys: readonly string[]): void {
    this.cache?.invalidate(keys, false);
  }

  private noteWrite(args: readonly Arg[]): void {
    if (!this.cache || args.length < 2) {
      if (this.cache && args.length >= 1 && /^FLUSH(ALL|DB)$/.test(commandName(args))) this.cache.clear();
      return;
    }
    const name = commandName(args);
    if (WRITES_FIRST_KEY.has(name)) this.cache.invalidate([String(args[1])], false);
    else if (WRITES_ALL_KEYS.has(name)) this.cache.invalidate(args.slice(1).map(String), false);
    else if (name === 'MSET' || name === 'MSETNX') this.cache.invalidate(args.filter((_, i) => i % 2 === 1).map(String), false);
    else if (name === 'FLUSHALL' || name === 'FLUSHDB' || name === 'TAG.INVALIDATE' || name === 'NAMESPACE') {
      // We can't tell which keys these touch; the server pushes the invalidations, but read-your-writes needs a clear.
      if (name !== 'NAMESPACE' || String(args[1]).toUpperCase() === 'DROP') this.cache.clear();
    }
  }

  private onMainPush(data: unknown[]): void {
    const kind = toStr(data[0])?.toLowerCase();
    if (kind === 'invalidate' && this.cache) {
      const keys = data[1];
      if (keys === null || keys === undefined) {
        this.cache.invalidate(null);
      } else {
        this.cache.invalidate(toArr(keys).map((k) => this.userKey(toStr(k) ?? '')));
      }
    }
  }

  /** Invalidation keys are stored keys ("\0ns\0key"); map them back to what we cached. */
  private userKey(stored: string): string {
    if (this.nsPrefix && stored.startsWith(this.nsPrefix)) return stored.slice(this.nsPrefix.length);
    return stored;
  }

  // ---------------------------------------------------------------- batches

  protected async execBatch(cmds: Arg[][], atomic: boolean): Promise<BatchItem[]> {
    for (const c of cmds) this.noteWrite(c);
    if (!atomic) {
      const settled = await Promise.allSettled(cmds.map((c) => this.send(c)));
      return settled.map((s) => (s.status === 'fulfilled' ? { ok: true, value: s.value } : { ok: false, error: asCbError(s.reason) }));
    }
    const o = { retryable: false };
    const multi = this.send(['MULTI'], o);
    const queued = cmds.map((c) => this.send(c, o).then(() => null, (e: unknown) => asCbError(e)));
    const exec = this.send(['EXEC'], o);
    await multi.catch(() => undefined);
    const queueErrors = await Promise.all(queued);
    let res: unknown;
    try {
      res = await exec;
    } catch (err) {
      const first = queueErrors.find((e) => e !== null);
      throw first ? new CacheBoyError(`${(err as Error).message} (${first.message})`, asCbError(err).code, { cause: first }) : err;
    }
    if (res === null) throw new CacheBoyError('Transaction aborted: a WATCHed key changed', 'EXECABORT');
    return toArr(res).map((v) => (v instanceof CacheBoyError ? { ok: false, error: v } : { ok: true, value: v }));
  }

  // ---------------------------------------------------------------- namespaces

  /** A client bound to another namespace (its own connection, created lazily and closed with this one). */
  namespace(name: string): CacheBoy {
    let v = this.views.get(name);
    if (!v) {
      v = new CacheBoy({ ...this.options, namespace: name, lazyConnect: true });
      this.views.set(name, v);
    }
    return v;
  }

  // ---------------------------------------------------------------- blocking commands

  /** Each concurrent blocking command gets its own connection from a small pool. */
  protected override blockingExecutor(): Executor {
    return {
      send: (args, opts) => {
        const conn = this.blockingIdle.pop() ?? this.newBlockingConn();
        return conn.send(args, opts).finally(() => {
          if (conn.status !== 'closed') this.blockingIdle.push(conn);
        });
      },
    };
  }

  private newBlockingConn(): Connection {
    const { clientCache: _cc, ...opts } = this.options;
    const c = new Connection({ ...opts, clientName: opts.clientName ? `${opts.clientName}:blocking` : undefined }, {
      init: () => (this.options.db ? [{ args: ['SELECT', this.options.db] }] : []),
    });
    c.on('error', () => undefined);
    this.blockingPool.push(c);
    return c;
  }

  // ---------------------------------------------------------------- pub/sub

  private subscriber(): Connection {
    if (!this.sub) {
      const { clientCache: _cc, ...opts } = this.options;
      this.sub = new Connection(
        { ...opts, clientName: opts.clientName ? `${opts.clientName}:sub` : undefined, inflightOnReconnect: 'retry-safe' },
        {
          subscriber: true,
          onPush: (d) => this.onSubPush(d),
          init: () => {
            // Re-subscribe everything after a reconnect.
            const cmds: Array<{ args: Arg[]; opts: SendOptions }> = [];
            if (this.options.db) cmds.push({ args: ['SELECT', this.options.db], opts: {} });
            const ch = [...this.channelCbs.keys()];
            const pt = [...this.patternCbs.keys()];
            const kw = [...this.watchCbs.keys()];
            if (ch.length) cmds.push({ args: ['SUBSCRIBE', ...ch], opts: { confirmations: ch.length } });
            if (pt.length) cmds.push({ args: ['PSUBSCRIBE', ...pt], opts: { confirmations: pt.length } });
            if (kw.length) cmds.push({ args: ['KEYWATCH', ...kw], opts: { confirmations: kw.length } });
            return cmds;
          },
        },
      );
      this.sub.on('error', (e: Error) => {
        if (this.conn.listenerCount('error') > 0) this.conn.emit('error', e);
      });
    }
    return this.sub;
  }

  override subscribe(channels: string | string[], cb: MessageCb): Promise<Unsubscribe> {
    return this.addSubs('SUBSCRIBE', 'UNSUBSCRIBE', this.channelCbs, channels, cb);
  }

  override psubscribe(patterns: string | string[], cb: PMessageCb): Promise<Unsubscribe> {
    return this.addSubs('PSUBSCRIBE', 'PUNSUBSCRIBE', this.patternCbs, patterns, cb);
  }

  /** KEYWATCH: cb(key, op) for every change of a key matching the pattern (op: set, del, expired...). */
  override keyWatch(patterns: string | string[], cb: KeyEventCb): Promise<Unsubscribe> {
    return this.addSubs('KEYWATCH', 'KEYUNWATCH', this.watchCbs, patterns, cb);
  }

  private async addSubs<F>(cmd: string, uncmd: string, map: Map<string, Set<F>>, names: string | string[], cb: F): Promise<Unsubscribe> {
    const list = [...new Set(Array.isArray(names) ? names : [names])];
    if (list.length === 0) throw new TypeError(`${cmd.toLowerCase()}: at least one name is required`);
    const fresh = list.filter((n) => !map.has(n));
    for (const n of list) {
      let s = map.get(n);
      if (!s) map.set(n, (s = new Set()));
      s.add(cb);
    }
    const remove = (): string[] => {
      const empty: string[] = [];
      for (const n of list) {
        const s = map.get(n);
        if (!s) continue;
        s.delete(cb);
        if (s.size === 0) {
          map.delete(n);
          empty.push(n);
        }
      }
      return empty;
    };
    const sub = this.subscriber();
    if (fresh.length > 0) {
      try {
        await sub.send([cmd, ...fresh], { confirmations: fresh.length });
      } catch (err) {
        remove();
        throw err;
      }
    }
    let done = false;
    return async () => {
      if (done) return;
      done = true;
      const empty = remove();
      if (empty.length > 0 && sub.status !== 'closed') await sub.send([uncmd, ...empty], { confirmations: empty.length });
    };
  }

  private onSubPush(data: unknown[]): void {
    const kind = toStr(data[0])?.toLowerCase();
    if (kind === 'message' || kind === 'smessage') {
      const ch = toStr(data[1]) ?? '';
      const msg = toStr(data[2]) ?? '';
      for (const cb of this.channelCbs.get(ch) ?? []) safe(() => cb(msg, ch));
    } else if (kind === 'pmessage') {
      const pat = toStr(data[1]) ?? '';
      const ch = toStr(data[2]) ?? '';
      const msg = toStr(data[3]) ?? '';
      for (const cb of this.patternCbs.get(pat) ?? []) safe(() => cb(msg, ch, pat));
      // KEYWATCH may be implemented as PSUBSCRIBE __key__:<pattern>.
      if (pat.startsWith('__key__:') && ch.startsWith('__key__:')) {
        for (const cb of this.watchCbs.get(pat.slice(8)) ?? []) safe(() => cb(this.userKey(ch.slice(8)), msg));
      }
    } else if (kind === 'keyevent') {
      const key = this.userKey(toStr(data[1]) ?? '');
      const op = toStr(data[2]) ?? '';
      for (const [pattern, cbs] of this.watchCbs) {
        if (globMatch(pattern, key)) for (const cb of cbs) safe(() => cb(key, op));
      }
    }
  }

  // ---------------------------------------------------------------- lifecycle

  /** Sends QUIT on every connection and waits for them to close. */
  async quit(): Promise<void> {
    await Promise.all([
      ...[...this.views.values()].map((v) => v.quit()),
      ...(this.sub ? [this.sub.quit()] : []),
      ...this.blockingPool.map((c) => c.quit()),
      this.conn.quit(),
    ]);
  }

  /** Closes every connection immediately; pending commands are rejected. */
  disconnect(): void {
    for (const v of this.views.values()) v.disconnect();
    this.sub?.disconnect();
    for (const c of this.blockingPool) c.disconnect();
    this.conn.disconnect();
  }

  /** Alias of quit(). */
  close(): Promise<void> {
    return this.quit();
  }
}

function asCbError(e: unknown): CacheBoyError {
  return e instanceof CacheBoyError ? e : new CacheBoyError(e instanceof Error ? e.message : String(e), undefined, { cause: e });
}

function safe(fn: () => void): void {
  try {
    fn();
  } catch (err) {
    queueMicrotask(() => {
      throw err;
    });
  }
}

function stripUndefined<T extends object>(o: T): Partial<T> {
  const out: Partial<T> = {};
  for (const [k, v] of Object.entries(o)) if (v !== undefined) (out as Record<string, unknown>)[k] = v;
  return out;
}
