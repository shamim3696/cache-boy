/**
 * One TCP (or TLS) connection to a cache-boy server, with:
 * - HELLO 3 handshake (AUTH + SETNAME), NAMESPACE USE and extra init commands on every (re)connect
 * - a FIFO command queue matched with replies
 * - auto-pipelining: every command issued in the same tick goes out in one socket write
 * - per-command timeouts
 * - auto-reconnect with exponential backoff and jitter, and replay of in-flight
 *   commands that are safe to retry
 *
 * Events: 'connect' (socket open), 'ready' (handshake done), 'error' (err),
 * 'close' (socket closed), 'reconnecting' ({attempt, delayMs, error}),
 * 'end' (gave up / closed for good), 'push' (data: unknown[]).
 */
import { EventEmitter } from 'node:events';
import * as net from 'node:net';
import * as tls from 'node:tls';
import { CacheBoyError, ErrorCodes } from './errors.js';
import { type Arg, RespParser, encodeCommand } from './resp.js';
import { commandName } from './types.js';

export interface RetryOptions {
  /** First reconnect delay before jitter. Default 50 ms. */
  baseDelayMs?: number;
  /** Cap for the exponential delay. Default 2000 ms. */
  maxDelayMs?: number;
  /** Give up after this many consecutive failed attempts. Default Infinity. */
  maxAttempts?: number;
}

/**
 * What to do with commands that were written but got no reply when the
 * connection dropped:
 * - 'retry-safe' (default): replay read-only/idempotent commands, reject the others
 * - 'retry-all': replay everything (a write may then run twice)
 * - 'reject': reject all of them with CONNECTION_LOST
 * Commands that were never written are always sent after reconnecting.
 */
export type InflightPolicy = 'retry-safe' | 'retry-all' | 'reject';

export interface ConnectionOptions {
  host?: string;
  port?: number;
  /** Unix socket path (overrides host/port). */
  path?: string;
  /** true or node:tls options to connect over TLS. */
  tls?: boolean | tls.ConnectionOptions;
  username?: string;
  password?: string;
  /** CLIENT SETNAME, sent with HELLO. */
  clientName?: string;
  /** NAMESPACE USE <ns> after every (re)connect. */
  namespace?: string;
  /** RESP version. Default 3 (required for client-side caching and pushes). */
  protocol?: 2 | 3;
  /** Default 10000 ms. */
  connectTimeoutMs?: number;
  /** Default timeout for each command. 0 or undefined: no timeout. */
  commandTimeoutMs?: number;
  /** Batch commands issued in the same tick into one write. Default true. */
  autoPipelining?: boolean;
  /** Queue commands while disconnected. Default true; false rejects them immediately. */
  enableOfflineQueue?: boolean;
  /** false disables auto-reconnect. */
  reconnect?: false | RetryOptions;
  inflightOnReconnect?: InflightPolicy;
  /** Don't connect until the first command (or connect()). Default false. */
  lazyConnect?: boolean;
}

export interface SendOptions {
  /** Overrides the default command timeout. 0 = none. */
  timeoutMs?: number;
  /** Return bulk strings as Buffers. */
  buffers?: boolean;
  /** Force whether this command may be replayed after a reconnect. */
  retryable?: boolean;
  /** Internal: number of subscribe-style confirmation pushes that complete this command. */
  confirmations?: number;
}

export interface ConnectionHooks {
  /** Push messages (RESP3 `>`), and pub/sub messages on a RESP2 subscriber connection. */
  onPush?: (data: unknown[]) => void;
  /** Extra commands to run after the handshake and before queued commands (e.g. CLIENT TRACKING ON). */
  init?: () => Array<{ args: Arg[]; opts?: SendOptions }>;
  /** True for a dedicated pub/sub connection. */
  subscriber?: boolean;
}

export type ConnectionStatus = 'wait' | 'connecting' | 'handshake' | 'ready' | 'reconnecting' | 'closed';

interface Command {
  args: Arg[];
  name: string;
  resolve: (v: unknown) => void;
  reject: (e: Error) => void;
  timer: NodeJS.Timeout | undefined;
  done: boolean;
  written: boolean;
  buffers: boolean;
  retryable: boolean;
  internal: boolean;
  confirmations: number;
  expected: number;
  lastConfirm: unknown;
}

/** Commands that read only, or are idempotent, and can be replayed after a reconnect. */
const SAFE_TO_RETRY = new Set([
  'GET', 'MGET', 'GETRANGE', 'STRLEN', 'EXISTS', 'TYPE', 'TTL', 'PTTL', 'EXPIRETIME', 'PEXPIRETIME',
  'KEYS', 'SCAN', 'DBSIZE', 'RANDOMKEY', 'OBJECT', 'DUMP', 'PING', 'ECHO', 'TIME', 'INFO',
  'HGET', 'HMGET', 'HGETALL', 'HEXISTS', 'HLEN', 'HKEYS', 'HVALS', 'HSTRLEN', 'HSCAN',
  'LRANGE', 'LLEN', 'LINDEX', 'LPOS',
  'SMEMBERS', 'SISMEMBER', 'SMISMEMBER', 'SCARD', 'SINTER', 'SUNION', 'SDIFF', 'SSCAN', 'SRANDMEMBER',
  'ZRANGE', 'ZREVRANGE', 'ZRANGEBYSCORE', 'ZREVRANGEBYSCORE', 'ZSCORE', 'ZMSCORE', 'ZCARD', 'ZCOUNT',
  'ZRANK', 'ZREVRANK', 'ZSCAN',
  'TAG.KEYS', 'TAG.OF', 'TAG.LIST', 'RATELIMIT.PEEK', 'LOCK.INFO', 'QLEN', 'QDEAD',
  'HISTORY', 'GETVERSION', 'JSON.GET', 'JSON.TYPE', 'JSON.ARRLEN', 'JSON.OBJKEYS', 'JSON.MGET',
  'TS.GET', 'TS.RANGE', 'TS.INFO', 'VSEARCH', 'VGET', 'VCARD', 'VDIM',
  'NAMESPACE', 'SCRIPT', 'PUBSUB',
  'SUBSCRIBE', 'PSUBSCRIBE', 'UNSUBSCRIBE', 'PUNSUBSCRIBE', 'KEYWATCH', 'KEYUNWATCH',
  'TAG.ADD', 'TAG.REMOVE',
]);

/** Commands whose replies arrive as confirmation pushes. */
export const CONFIRM_KINDS = new Set(['subscribe', 'psubscribe', 'unsubscribe', 'punsubscribe', 'keywatch', 'keyunwatch']);
const MESSAGE_KINDS = new Set(['message', 'pmessage', 'keyevent', 'smessage']);

export function isSafeToRetry(args: readonly Arg[]): boolean {
  return SAFE_TO_RETRY.has(commandName(args));
}

export { commandName };

/** Minimal FIFO with O(1) shift. */
class Deque<T> {
  private items: T[] = [];
  private head = 0;
  get length(): number {
    return this.items.length - this.head;
  }
  push(v: T): void {
    this.items.push(v);
  }
  peek(): T | undefined {
    return this.items[this.head];
  }
  shift(): T | undefined {
    if (this.head >= this.items.length) return undefined;
    const v = this.items[this.head];
    this.items[this.head++] = undefined as T;
    if (this.head > 1024 && this.head * 2 > this.items.length) {
      this.items = this.items.slice(this.head);
      this.head = 0;
    }
    return v;
  }
  drain(): T[] {
    const out = this.items.slice(this.head);
    this.items = [];
    this.head = 0;
    return out;
  }
}

export class Connection extends EventEmitter {
  readonly options: ConnectionOptions;
  /** HELLO reply from the last handshake (server, version, proto, id...). */
  serverInfo: Record<string, unknown> | undefined;

  private _status: ConnectionStatus = 'wait';
  private socket: net.Socket | undefined;
  private readonly parser: RespParser;
  private readonly sent = new Deque<Command>();
  private offline: Command[] = [];
  private writeQueue: Buffer[] = [];
  private writeCmds: Command[] = [];
  private flushScheduled = false;
  private attempt = 0;
  private manualClose = false;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private lastError: Error | undefined;

  constructor(options: ConnectionOptions = {}, private readonly hooks: ConnectionHooks = {}) {
    super();
    this.options = options;
    this.parser = new RespParser({
      onReply: (v) => this.onReply(v),
      onPush: (d) => this.onPush(d),
    });
    this.parser.returnBuffers = () => this.sent.peek()?.buffers ?? false;
    if (!options.lazyConnect) this.startConnect();
  }

  get status(): ConnectionStatus {
    return this._status;
  }

  /** Number of commands awaiting a reply or a connection. */
  get pendingCount(): number {
    return this.sent.length + this.offline.length + this.writeCmds.length;
  }

  /** Connects (if lazy) and resolves once the handshake is done. */
  connect(): Promise<void> {
    if (this._status === 'ready') return Promise.resolve();
    if (this._status === 'closed') return Promise.reject(closedError());
    if (this._status === 'wait') this.startConnect();
    return new Promise((resolve, reject) => {
      const onReady = (): void => {
        this.off('end', onEnd);
        resolve();
      };
      const onEnd = (): void => {
        this.off('ready', onReady);
        reject(this.lastError ?? closedError());
      };
      this.once('ready', onReady);
      this.once('end', onEnd);
    });
  }

  send(args: Arg[], opts: SendOptions = {}): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      const cmd: Command = {
        args,
        name: commandName(args),
        resolve,
        reject,
        timer: undefined,
        done: false,
        written: false,
        buffers: opts.buffers ?? false,
        retryable: false,
        internal: false,
        confirmations: opts.confirmations ?? 0,
        expected: opts.confirmations ?? 0,
        lastConfirm: undefined,
      };
      cmd.retryable = opts.retryable ?? SAFE_TO_RETRY.has(cmd.name);
      if (this._status === 'closed') {
        reject(closedError(cmd.name));
        return;
      }
      const timeout = opts.timeoutMs ?? this.options.commandTimeoutMs ?? 0;
      if (timeout > 0) cmd.timer = setTimeout(() => this.onTimeout(cmd, timeout), timeout);
      if (this._status === 'ready') {
        this.write(cmd);
      } else if (this.options.enableOfflineQueue === false) {
        this.settle(cmd, new CacheBoyError(`Connection is not ready (${this._status}) and the offline queue is disabled`, ErrorCodes.CONNECTION_CLOSED, { command: cmd.name }));
      } else {
        this.offline.push(cmd);
        if (this._status === 'wait') this.startConnect();
      }
    });
  }

  /** Sends QUIT and closes once the server replies. No reconnect. */
  async quit(): Promise<void> {
    this.manualClose = true;
    if (this._status === 'ready') {
      const closed = new Promise<void>((resolve) => this.once('end', () => resolve()));
      await this.send(['QUIT'], { retryable: false }).catch(() => undefined);
      this.socket?.end();
      await closed;
    } else {
      this.disconnect();
    }
  }

  /** Closes immediately. Pending commands are rejected. No reconnect. */
  disconnect(): void {
    this.manualClose = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    if (this.socket) {
      this.socket.destroy();
    } else if (this._status !== 'closed') {
      this.end(closedError());
    }
  }

  // ---------------------------------------------------------------------------

  private startConnect(): void {
    this._status = 'connecting';
    const o = this.options;
    const connectTimeout = o.connectTimeoutMs ?? 10_000;
    let sock: net.Socket;
    if (o.tls) {
      const tlsOpts: tls.ConnectionOptions = typeof o.tls === 'object' ? { ...o.tls } : {};
      if (o.path) tlsOpts.path = o.path;
      else {
        tlsOpts.host ??= o.host ?? '127.0.0.1';
        tlsOpts.port ??= o.port ?? 6379;
        if (tlsOpts.servername === undefined && !net.isIP(tlsOpts.host!)) tlsOpts.servername = tlsOpts.host;
      }
      sock = tls.connect(tlsOpts);
    } else if (o.path) {
      sock = net.connect({ path: o.path });
    } else {
      sock = net.connect({ host: o.host ?? '127.0.0.1', port: o.port ?? 6379 });
    }
    this.socket = sock;
    sock.setNoDelay(true);
    sock.setKeepAlive(true, 30_000);
    if (connectTimeout > 0) {
      sock.setTimeout(connectTimeout);
      sock.once('timeout', () => sock.destroy(new CacheBoyError(`Connect timeout after ${connectTimeout} ms`, ErrorCodes.CONNECTION_LOST)));
    }
    sock.once(o.tls ? 'secureConnect' : 'connect', () => {
      sock.setTimeout(0);
      if (sock === this.socket) this.onConnected(sock);
    });
    sock.on('data', (chunk: Buffer) => {
      if (sock !== this.socket) return;
      try {
        this.parser.feed(chunk);
      } catch (err) {
        this.emitError(err as Error);
        sock.destroy(err as Error);
      }
    });
    sock.on('error', (err) => {
      this.lastError = err;
      this.emitError(err);
    });
    sock.on('close', () => this.onClose(sock));
  }

  private onConnected(sock: net.Socket): void {
    this._status = 'handshake';
    this.parser.reset();
    this.emit('connect');
    const o = this.options;
    const steps: Promise<unknown>[] = [];
    if ((o.protocol ?? 3) === 3) {
      const hello: Arg[] = ['HELLO', '3'];
      if (o.password !== undefined) hello.push('AUTH', o.username ?? 'default', o.password);
      if (o.clientName) hello.push('SETNAME', o.clientName);
      steps.push(
        this.writeInternal(hello).then((info) => {
          if (info && typeof info === 'object' && !Array.isArray(info)) this.serverInfo = info as Record<string, unknown>;
          else if (Array.isArray(info)) this.serverInfo = pairsToObject(info);
        }),
      );
    } else {
      if (o.password !== undefined) {
        steps.push(this.writeInternal(o.username ? ['AUTH', o.username, o.password] : ['AUTH', o.password]));
      }
      if (o.clientName) steps.push(this.writeInternal(['CLIENT', 'SETNAME', o.clientName]));
    }
    if (o.namespace && o.namespace !== 'default') steps.push(this.writeInternal(['NAMESPACE', 'USE', o.namespace]));
    for (const c of this.hooks.init?.() ?? []) steps.push(this.writeInternal(c.args, c.opts));
    this.flush();
    Promise.all(steps).then(
      () => {
        if (sock !== this.socket) return;
        this._status = 'ready';
        this.attempt = 0;
        this.lastError = undefined;
        const queued = this.offline;
        this.offline = [];
        for (const cmd of queued) if (!cmd.done) this.write(cmd);
        this.emit('ready');
      },
      (err: Error) => {
        if (sock !== this.socket) return;
        const e = err instanceof CacheBoyError ? err : new CacheBoyError(String(err?.message ?? err), undefined, { cause: err });
        this.lastError = e;
        this.emitError(e);
        // Fail queued commands fast instead of leaving them hanging (e.g. WRONGPASS).
        const queued = this.offline;
        this.offline = [];
        for (const cmd of queued) this.settle(cmd, e);
        sock.destroy();
      },
    );
  }

  private writeInternal(args: Arg[], opts: SendOptions = {}): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const cmd: Command = {
        args,
        name: commandName(args),
        resolve,
        reject,
        timer: undefined,
        done: false,
        written: false,
        buffers: false,
        retryable: false,
        internal: true,
        confirmations: opts.confirmations ?? 0,
        expected: opts.confirmations ?? 0,
        lastConfirm: undefined,
      };
      this.sent.push(cmd);
      this.writeQueue.push(encodeCommand(args));
      this.writeCmds.push(cmd);
    });
  }

  private write(cmd: Command): void {
    this.sent.push(cmd);
    this.writeQueue.push(encodeCommand(cmd.args));
    this.writeCmds.push(cmd);
    if (this.options.autoPipelining === false) {
      this.flush();
    } else if (!this.flushScheduled) {
      this.flushScheduled = true;
      process.nextTick(() => this.flush());
    }
  }

  private flush(): void {
    this.flushScheduled = false;
    if (this.writeQueue.length === 0 || !this.socket) return;
    const data = this.writeQueue.length === 1 ? this.writeQueue[0]! : Buffer.concat(this.writeQueue);
    for (const c of this.writeCmds) c.written = true;
    this.writeQueue = [];
    this.writeCmds = [];
    this.socket.write(data);
  }

  private onReply(v: unknown): void {
    const cmd = this.sent.peek();
    if (!cmd) {
      this.emitError(new CacheBoyError('Received a reply with no pending command', ErrorCodes.PROTOCOL));
      return;
    }
    if (Array.isArray(v) && (this.options.protocol ?? 3) === 2 && this.hooks.subscriber) {
      // RESP2 pub/sub: confirmations and messages are plain arrays.
      const kind = kindOf(v);
      if (MESSAGE_KINDS.has(kind)) {
        this.deliverPush(v);
        return;
      }
      if (CONFIRM_KINDS.has(kind) && cmd.confirmations > 0) {
        this.confirm(cmd, v);
        return;
      }
    }
    this.sent.shift();
    this.settle(cmd, v);
  }

  private onPush(data: unknown[]): void {
    const kind = kindOf(data);
    if (CONFIRM_KINDS.has(kind)) {
      const head = this.sent.peek();
      if (head && head.confirmations > 0) {
        this.confirm(head, data);
        return;
      }
    }
    this.deliverPush(data);
  }

  private confirm(cmd: Command, data: unknown): void {
    cmd.lastConfirm = data;
    if (--cmd.confirmations <= 0) {
      this.sent.shift();
      this.settle(cmd, data);
    }
  }

  private deliverPush(data: unknown[]): void {
    try {
      this.hooks.onPush?.(data);
    } catch (err) {
      this.emitError(err as Error);
    }
    if (this.listenerCount('push') > 0) this.emit('push', data);
  }

  private settle(cmd: Command, v: unknown): void {
    if (cmd.timer) clearTimeout(cmd.timer);
    cmd.timer = undefined;
    if (cmd.done) return;
    cmd.done = true;
    if (v instanceof Error) {
      if (v instanceof CacheBoyError && v.command === undefined) v.command = cmd.name;
      cmd.reject(v);
    } else {
      cmd.resolve(v);
    }
  }

  private onTimeout(cmd: Command, ms: number): void {
    cmd.timer = undefined;
    if (cmd.done) return;
    const i = this.offline.indexOf(cmd);
    if (i !== -1) this.offline.splice(i, 1);
    // A command already sent stays in the FIFO (marked done) so later replies still line up.
    this.settle(cmd, new CacheBoyError(`Command ${cmd.name} timed out after ${ms} ms`, ErrorCodes.TIMEOUT, { command: cmd.name }));
  }

  private onClose(sock: net.Socket): void {
    if (sock !== this.socket) return;
    this.socket = undefined;
    this.flushScheduled = false;
    const wasReady = this._status === 'ready';
    this.emit('close', this.lastError);

    // Commands that were queued for writing but never hit the socket are safe to resend.
    const unwritten = new Set(this.writeCmds);
    this.writeQueue = [];
    this.writeCmds = [];
    const policy = this.options.inflightOnReconnect ?? 'retry-safe';
    const giveUp = this.manualClose || this.options.reconnect === false;
    const requeue: Command[] = [];
    for (const cmd of this.sent.drain()) {
      if (cmd.done) continue;
      if (cmd.internal) {
        this.settle(cmd, new CacheBoyError('Connection closed during handshake', ErrorCodes.CONNECTION_LOST));
        continue;
      }
      const neverSent = unwritten.has(cmd) || !cmd.written;
      if (!giveUp && (neverSent || policy === 'retry-all' || (policy === 'retry-safe' && cmd.retryable))) {
        cmd.written = false;
        cmd.confirmations = cmd.expected;
        requeue.push(cmd);
      } else {
        this.settle(
          cmd,
          new CacheBoyError(
            giveUp
              ? `Connection closed before ${cmd.name} got a reply`
              : `Connection lost before ${cmd.name} got a reply. It may or may not have run on the server, and it was not retried because it is not safe to retry (inflightOnReconnect: '${policy}')`,
            giveUp ? ErrorCodes.CONNECTION_CLOSED : ErrorCodes.CONNECTION_LOST,
            { command: cmd.name, cause: this.lastError },
          ),
        );
      }
    }
    this.offline = requeue.concat(this.offline);

    if (giveUp) {
      this.end(closedError());
      return;
    }
    if (wasReady) this.attempt = 0;
    this.attempt++;
    const r = this.options.reconnect || {};
    const maxAttempts = r.maxAttempts ?? Infinity;
    if (this.attempt > maxAttempts) {
      this.end(new CacheBoyError(`Gave up reconnecting after ${maxAttempts} attempts`, ErrorCodes.CONNECTION_CLOSED, { cause: this.lastError }));
      return;
    }
    const delayMs = backoff(this.attempt, r.baseDelayMs ?? 50, r.maxDelayMs ?? 2000);
    this._status = 'reconnecting';
    this.emit('reconnecting', { attempt: this.attempt, delayMs, error: this.lastError });
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      if (!this.manualClose) this.startConnect();
    }, delayMs);
  }

  private end(err: Error): void {
    this._status = 'closed';
    const queued = this.offline;
    this.offline = [];
    for (const cmd of queued) this.settle(cmd, err);
    this.emit('end', err);
  }

  private emitError(err: Error): void {
    // Without listeners, EventEmitter would throw; commands are rejected anyway.
    if (this.listenerCount('error') > 0) this.emit('error', err);
  }
}

/** Exponential backoff with "equal jitter": half fixed, half random. */
export function backoff(attempt: number, baseMs: number, maxMs: number): number {
  const exp = Math.min(maxMs, baseMs * 2 ** Math.min(attempt - 1, 30));
  return Math.round(exp / 2 + Math.random() * (exp / 2));
}

function kindOf(data: unknown[]): string {
  const k = data[0];
  return typeof k === 'string' ? k.toLowerCase() : Buffer.isBuffer(k) ? k.toString().toLowerCase() : '';
}

function closedError(command?: string): CacheBoyError {
  return new CacheBoyError('Connection is closed', ErrorCodes.CONNECTION_CLOSED, command !== undefined ? { command } : undefined);
}

export function pairsToObject(arr: unknown[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (let i = 0; i + 1 < arr.length; i += 2) out[String(arr[i])] = arr[i + 1];
  return out;
}
