import { CacheBoy, type CacheBoyOptions, parseUrl } from './client.js';
import { type BatchItem, CommandsBase } from './commands.js';
import { CacheBoyError, ErrorCodes } from './errors.js';
import { type Arg, type Executor, type SendOptions, toArr, toNum, toStr, toStrArr } from './types.js';

export const SLOTS = 16384;

export interface CacheBoyClusterOptions extends Omit<CacheBoyOptions, 'url' | 'host' | 'port'> {
  /** Seed nodes: "redis://host:port" or {host, port}. One reachable node is enough. */
  nodes: Array<string | { host: string; port: number }>;
  /** How many MOVED redirects to follow for one command (default 5). */
  maxRedirects?: number;
}

type Unsubscribe = () => Promise<void>;

// Commands without a key: sent to any node.
const KEYLESS = new Set([
  'PING', 'ECHO', 'INFO', 'TIME', 'CONFIG', 'CLIENT', 'COMMAND', 'HELLO', 'AUTH', 'SELECT', 'CLUSTER',
  'PUBLISH', 'PUBSUB', 'SLOWLOG', 'ACL', 'MEMORY', 'DEBUG', 'ROLE', 'WAIT', 'SCRIPT', 'NAMESPACE', 'QUIT',
  'WEBHOOK.ADD', 'WEBHOOK.DEL', 'WEBHOOK.LIST', 'SAVE', 'BGSAVE', 'BGREWRITEAOF', 'LASTSAVE',
]);

// Commands whose effect spans every node; replies are merged.
const BROADCAST: Record<string, 'sum' | 'concat' | 'union' | 'first'> = {
  DBSIZE: 'sum',
  FLUSHALL: 'first',
  FLUSHDB: 'first',
  KEYS: 'concat',
  'TAG.INVALIDATE': 'sum',
  'TAG.KEYS': 'concat',
  'TAG.LIST': 'union',
  'SCRIPT': 'first', // LOAD/FLUSH: every node needs the script
};

/**
 * A cluster client: routes each command to the node that owns its key's hash
 * slot (CRC16 + {hashtag}, like the server), follows MOVED redirects and
 * refreshes the slot map. Every node gets its own CacheBoy client, so
 * auto-pipelining and client-side caching work per node.
 *
 * ```ts
 * const cluster = new CacheBoyCluster({ nodes: ['redis://10.0.0.1:6380', 'redis://10.0.0.2:6380'] });
 * await cluster.set('user:1', 'ana');
 * ```
 *
 * Multi-key commands and MULTI need all keys on one node: use a hashtag,
 * e.g. `{user:1}:profile` and `{user:1}:cart`.
 */
export class CacheBoyCluster extends CommandsBase {
  readonly options: Readonly<CacheBoyClusterOptions>;
  private readonly clients = new Map<string, CacheBoy>();
  private readonly slots: Array<string | undefined> = new Array(SLOTS);
  private readonly seeds: string[];
  private refreshing: Promise<void> | undefined;
  private ready: Promise<void> | undefined;
  private readonly views = new Map<string, CacheBoyCluster>();

  constructor(options: CacheBoyClusterOptions) {
    super();
    if (!options.nodes?.length) throw new TypeError('CacheBoyCluster needs at least one seed node');
    this.options = options;
    this.seeds = options.nodes.map((n) => {
      if (typeof n !== 'string') return `${n.host}:${n.port}`;
      const o = parseUrl(n.includes('://') ? n : `redis://${n}`);
      return `${o.host ?? '127.0.0.1'}:${o.port ?? 6379}`;
    });
  }

  // ---------------------------------------------------------------- routing

  async send(args: Arg[], opts?: SendOptions): Promise<unknown> {
    await this.init();
    const name = String(args[0]).toUpperCase();
    const mode = BROADCAST[name];
    if (mode) return this.broadcast(args, mode, opts);
    const key = keyOf(name, args);
    return this.route(key, (c) => c.send(args, opts));
  }

  /** Runs fn on the node owning key (any node if undefined), following MOVED. */
  private async route<T>(key: string | undefined, fn: (c: CacheBoy) => Promise<T>): Promise<T> {
    let addr = key === undefined ? this.anyNode() : (this.slots[keySlot(key)] ?? this.anyNode());
    const max = this.options.maxRedirects ?? 5;
    for (let attempt = 0; ; attempt++) {
      try {
        return await fn(this.node(addr));
      } catch (err) {
        if (!(err instanceof CacheBoyError) || attempt >= max) throw err;
        if (err.code === 'MOVED') {
          // "MOVED <slot> <host:port>": go there now, refresh the whole map in the background.
          const [, slot, target] = err.message.split(' ');
          if (!slot || !target) throw err;
          this.slots[Number(slot)] = target;
          addr = target;
          void this.refresh().catch(() => undefined);
          continue;
        }
        if (err.code === 'CLUSTERDOWN' || err.code === ErrorCodes.CONNECTION_LOST || err.code === ErrorCodes.CONNECTION_CLOSED) {
          await this.refresh().catch(() => undefined);
          addr = key === undefined ? this.anyNode() : (this.slots[keySlot(key)] ?? this.anyNode());
          continue;
        }
        throw err;
      }
    }
  }

  private async broadcast(args: Arg[], mode: 'sum' | 'concat' | 'union' | 'first', opts?: SendOptions): Promise<unknown> {
    const replies = await Promise.all(this.masters().map((a) => this.node(a).send(args, opts)));
    switch (mode) {
      case 'sum':
        return replies.reduce<number>((s, r) => s + toNum(r), 0);
      case 'concat':
        return replies.flatMap((r) => toArr(r));
      case 'union':
        return [...new Set(replies.flatMap((r) => toStrArr(r)))];
      default:
        return replies[0];
    }
  }

  /** Fetches CLUSTER SLOTS from the first node that answers. */
  refresh(): Promise<void> {
    this.refreshing ??= (async () => {
      const candidates = [...new Set([...this.masters(), ...this.seeds])];
      let lastErr: unknown;
      for (const addr of candidates) {
        try {
          const ranges = toArr(await this.node(addr).send(['CLUSTER', 'SLOTS']));
          this.slots.fill(undefined);
          for (const r of ranges) {
            const [start, end, owner] = toArr(r);
            const [host, port] = toArr(owner);
            const target = `${toStr(host)}:${toNum(port)}`;
            for (let s = toNum(start); s <= toNum(end); s++) this.slots[s] = target;
          }
          return;
        } catch (err) {
          lastErr = err;
        }
      }
      throw lastErr instanceof Error ? lastErr : new CacheBoyError('no cluster node reachable', ErrorCodes.CONNECTION_CLOSED);
    })().finally(() => {
      this.refreshing = undefined;
    });
    return this.refreshing;
  }

  private init(): Promise<void> {
    this.ready ??= this.refresh().catch((err: unknown) => {
      this.ready = undefined; // retry on the next command
      throw err;
    });
    return this.ready;
  }

  /** Addresses that own at least one slot (or the seeds before the first refresh). */
  masters(): string[] {
    const owners = new Set(this.slots.filter((a): a is string => a !== undefined));
    return owners.size ? [...owners] : this.seeds;
  }

  private anyNode(): string {
    const m = this.masters();
    return m[Math.floor(Math.random() * m.length)] ?? this.seeds[0]!;
  }

  /** The client for one node (created on first use). */
  node(addr: string): CacheBoy {
    let c = this.clients.get(addr);
    if (!c) {
      const i = addr.lastIndexOf(':');
      const { nodes: _n, maxRedirects: _m, ...rest } = this.options;
      c = new CacheBoy({ ...rest, host: addr.slice(0, i), port: Number(addr.slice(i + 1)) });
      // Errors surface through command promises; don't crash on unhandled 'error' events.
      c.on('error', () => undefined);
      this.clients.set(addr, c);
    }
    return c;
  }

  // ---------------------------------------------------------------- CommandsBase hooks

  protected async execBatch(cmds: Arg[][], atomic: boolean): Promise<BatchItem[]> {
    await this.init();
    if (!atomic) {
      // Each node client auto-pipelines the commands routed to it.
      const settled = await Promise.allSettled(cmds.map((c) => this.send(c)));
      return settled.map((s) => (s.status === 'fulfilled' ? { ok: true, value: s.value } : { ok: false, error: asError(s.reason) }));
    }
    // MULTI runs on one node: the one owning the first key. Keys on other
    // nodes fail with MOVED/CROSSSLOT from the server.
    const first = cmds.map((c) => keyOf(String(c[0]).toUpperCase(), c)).find((k) => k !== undefined);
    return this.route(first, (c) => (c as unknown as BatchRunner).execBatch(cmds, true));
  }

  protected override blockingExecutor(): Executor {
    return {
      send: async (args, opts) => {
        await this.init();
        const key = keyOf(String(args[0]).toUpperCase(), args);
        return this.route(key, (c) => (c as unknown as BlockingSource).blockingExecutor().send(args, opts));
      },
    };
  }

  namespace(name: string): CacheBoyCluster {
    let v = this.views.get(name);
    if (!v) {
      v = new CacheBoyCluster({ ...this.options, namespace: name });
      this.views.set(name, v);
    }
    return v;
  }

  // Pub/sub and key events are per node, so subscribe on every node.
  override subscribe(channels: string | string[], cb: (message: string, channel: string) => void): Promise<Unsubscribe> {
    return this.onEveryNode((c) => c.subscribe(channels, cb));
  }

  override psubscribe(patterns: string | string[], cb: (message: string, channel: string, pattern: string) => void): Promise<Unsubscribe> {
    return this.onEveryNode((c) => c.psubscribe(patterns, cb));
  }

  override keyWatch(patterns: string | string[], cb: (key: string, op: string) => void): Promise<Unsubscribe> {
    return this.onEveryNode((c) => c.keyWatch(patterns, cb));
  }

  /** SCAN every node in turn. */
  override async *scanAll(opts: { match?: string; count?: number; type?: string } = {}): AsyncGenerator<string> {
    await this.init();
    for (const addr of this.masters()) yield* this.node(addr).scanAll(opts);
  }

  private async onEveryNode(fn: (c: CacheBoy) => Promise<Unsubscribe>): Promise<Unsubscribe> {
    await this.init();
    const offs = await Promise.all(this.masters().map((a) => fn(this.node(a))));
    return async () => {
      await Promise.all(offs.map((off) => off()));
    };
  }

  async quit(): Promise<void> {
    await Promise.all([...this.views.values()].map((v) => v.quit()));
    await Promise.all([...this.clients.values()].map((c) => c.quit()));
  }

  disconnect(): void {
    for (const v of this.views.values()) v.disconnect();
    for (const c of this.clients.values()) c.disconnect();
  }
}

interface BatchRunner {
  execBatch(cmds: Arg[][], atomic: boolean): Promise<BatchItem[]>;
}
interface BlockingSource {
  blockingExecutor(): Executor;
}

function asError(e: unknown): CacheBoyError {
  return e instanceof CacheBoyError ? e : new CacheBoyError(e instanceof Error ? e.message : String(e), 'ERR');
}

/** The routing key of a command: usually the first argument. */
export function keyOf(name: string, args: readonly Arg[]): string | undefined {
  if (KEYLESS.has(name) || args.length < 2) return undefined;
  switch (name) {
    case 'EVAL':
    case 'EVALSHA':
      return toNum(String(args[2])) > 0 && args.length > 3 ? String(args[3]) : undefined;
    case 'OBJECT':
    case 'XINFO':
      return args.length > 2 ? String(args[2]) : undefined;
    case 'BLMPOP':
    case 'BZMPOP':
      return args.length > 3 ? String(args[3]) : undefined;
    case 'LMPOP':
    case 'ZMPOP':
    case 'SINTERCARD':
    case 'ZUNION':
    case 'ZINTER':
    case 'ZDIFF':
      return args.length > 2 ? String(args[2]) : undefined;
  }
  const k = args[1];
  return typeof k === 'string' ? k : Buffer.isBuffer(k) ? k.toString('utf8') : String(k);
}

// CRC16-XMODEM, the hash Redis Cluster uses.
const CRC16_TABLE = (() => {
  const t = new Uint16Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i << 8;
    for (let j = 0; j < 8; j++) c = c & 0x8000 ? ((c << 1) ^ 0x1021) & 0xffff : (c << 1) & 0xffff;
    t[i] = c;
  }
  return t;
})();

function crc16(buf: Uint8Array): number {
  let crc = 0;
  for (const b of buf) crc = ((crc << 8) & 0xffff) ^ CRC16_TABLE[((crc >> 8) ^ b) & 0xff]!;
  return crc;
}

/** The hash slot of a key; only the part inside the first non-empty {...} counts. */
export function keySlot(key: string): number {
  let k = key;
  const open = key.indexOf('{');
  if (open >= 0) {
    const close = key.indexOf('}', open + 1);
    if (close > open + 1) k = key.slice(open + 1, close);
  }
  return crc16(new TextEncoder().encode(k)) % SLOTS;
}
