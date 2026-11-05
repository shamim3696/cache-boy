/**
 * CacheBoyHttp: the same typed API over cache-boy's HTTP JSON API, for
 * serverless and edge runtimes (anything with fetch).
 *
 *   POST {url}/v1/command   ["SET","a","1"]          → {"result": ...} | {"error": "..."}
 *   POST {url}/v1/pipeline  [[...],[...]]            → [{"result"}|{"error"}, ...]
 *   POST {url}/v1/multi     [[...],[...]] (atomic)   → {"result": [...]} | {"error": "..."}
 *   Authorization: Bearer <token>, X-CacheBoy-Namespace: <ns>
 */
import { type BatchItem, CommandsBase, unsupported } from './commands.js';
import { CacheBoyError, ErrorCodes } from './errors.js';
import type { Arg } from './resp.js';
import { type SendOptions, bytesToString, commandName } from './types.js';

export type FetchLike = (input: string, init: {
  method: string;
  headers: Record<string, string>;
  body: string;
  signal?: AbortSignal;
}) => Promise<{ ok: boolean; status: number; statusText?: string; text(): Promise<string> }>;

export interface CacheBoyHttpOptions {
  /** Base URL of the HTTP API, e.g. https://cache.example.com */
  url: string;
  /** Sent as `Authorization: Bearer <token>`. */
  token?: string;
  /** Sent as `X-CacheBoy-Namespace`. */
  namespace?: string;
  /** Custom fetch (defaults to the global one). */
  fetch?: FetchLike | typeof fetch;
  /** Extra headers for every request. */
  headers?: Record<string, string>;
  /** Abort each request after this long. Default: none. */
  timeoutMs?: number;
}

/** Commands that need a persistent connection. */
const CONNECTION_ONLY: Record<string, string> = {
  SUBSCRIBE: 'pub/sub needs a persistent connection',
  PSUBSCRIBE: 'pub/sub needs a persistent connection',
  SSUBSCRIBE: 'pub/sub needs a persistent connection',
  UNSUBSCRIBE: 'pub/sub needs a persistent connection',
  PUNSUBSCRIBE: 'pub/sub needs a persistent connection',
  KEYWATCH: 'key change streams need a persistent connection (use WEBHOOK.ADD instead)',
  KEYUNWATCH: 'key change streams need a persistent connection',
  MONITOR: 'MONITOR needs a persistent connection',
  HELLO: 'the HTTP API has no connection handshake',
  MULTI: 'use client.multi() (POST /v1/multi) instead',
  EXEC: 'use client.multi() (POST /v1/multi) instead',
  DISCARD: 'use client.multi() (POST /v1/multi) instead',
  WATCH: 'WATCH needs a persistent connection',
  UNWATCH: 'WATCH needs a persistent connection',
  SELECT: 'every HTTP request is independent; SELECT would not stick',
  QUIT: 'there is no connection to quit',
  RESET: 'there is no connection to reset',
};

/** Blocking commands, with the index of their timeout argument (-1 = last). */
const BLOCKING_TIMEOUT_ARG: Record<string, number> = {
  BLPOP: -1, BRPOP: -1, BLMOVE: -1, BRPOPLPUSH: -1, BZPOPMIN: -1, BZPOPMAX: -1, BLMPOP: 1, BZMPOP: 1,
};

function argToJson(a: Arg): string {
  if (typeof a === 'string') return a;
  if (typeof a === 'number' || typeof a === 'bigint') return String(a);
  // JSON has no binary type: bytes are sent as UTF-8 text.
  return bytesToString(a);
}

/** Throws CacheBoyError(UNSUPPORTED) for commands that can't work over stateless HTTP. */
export function assertHttpCompatible(args: readonly Arg[]): void {
  const name = commandName(args);
  const why = CONNECTION_ONLY[name];
  if (why) throw unsupported(name, `not available over HTTP: ${why}`);
  if (name === 'CLIENT' && args.length > 1 && String(args[1]).toUpperCase() === 'TRACKING') {
    throw unsupported('CLIENT TRACKING', 'client-side caching needs a persistent RESP3 connection');
  }
  if (name === 'NAMESPACE' && args.length > 1 && String(args[1]).toUpperCase() === 'USE') {
    throw unsupported('NAMESPACE USE', 'set the namespace option (X-CacheBoy-Namespace header) or use client.namespace(name)');
  }
  const ti = BLOCKING_TIMEOUT_ARG[name];
  if (ti !== undefined) {
    const t = args[ti < 0 ? args.length + ti : ti];
    if (t !== undefined && Number(argToJson(t)) === 0) throw unsupported(name, 'blocking forever (timeout 0) is not available over HTTP; pass a timeout');
  }
  if (name === 'QPOP' || name === 'XREAD' || name === 'XREADGROUP') {
    for (let i = 2; i + 1 < args.length; i++) {
      if (String(args[i]).toUpperCase() === 'BLOCK' && Number(argToJson(args[i + 1]!)) === 0) {
        throw unsupported(name, 'BLOCK 0 (wait forever) is not available over HTTP; pass a finite BLOCK');
      }
    }
  }
}

export class CacheBoyHttp extends CommandsBase {
  readonly options: Readonly<CacheBoyHttpOptions>;
  private readonly base: string;
  private readonly fetchFn: FetchLike;

  constructor(options: CacheBoyHttpOptions) {
    super();
    if (!options?.url) throw new TypeError('CacheBoyHttp: url is required');
    this.options = { ...options };
    this.base = options.url.replace(/\/+$/, '');
    const f = (options.fetch ?? (globalThis as { fetch?: FetchLike }).fetch) as FetchLike | undefined;
    if (!f) throw new TypeError('CacheBoyHttp: no fetch available; pass options.fetch');
    this.fetchFn = f;
  }

  async send(args: Arg[], opts: SendOptions = {}): Promise<unknown> {
    assertHttpCompatible(args);
    const body = await this.post('/v1/command', args.map(argToJson), opts.timeoutMs);
    const v = unwrap(body, commandName(args));
    if (opts.buffers && typeof v === 'string' && typeof Buffer !== 'undefined') return Buffer.from(v, 'utf8');
    return v;
  }

  /** The same client bound to another namespace (X-CacheBoy-Namespace). */
  namespace(name: string): CacheBoyHttp {
    return new CacheBoyHttp({ ...this.options, namespace: name });
  }

  protected async execBatch(cmds: Arg[][], atomic: boolean): Promise<BatchItem[]> {
    for (const c of cmds) assertHttpCompatible(c);
    const payload = cmds.map((c) => c.map(argToJson));
    if (atomic) {
      const body = await this.post('/v1/multi', payload);
      const res = unwrap(body, 'MULTI');
      if (!Array.isArray(res)) throw new CacheBoyError('Unexpected /v1/multi reply: expected an array result', ErrorCodes.BAD_REPLY);
      return res.map((v, i) => {
        // Defensive: an individual error encoded as {"error": "..."}.
        if (isErrorObject(v)) return { ok: false, error: CacheBoyError.fromServer(v.error, commandName(cmds[i]!)) };
        return { ok: true, value: v };
      });
    }
    const body = await this.post('/v1/pipeline', payload);
    if (!Array.isArray(body)) {
      if (isErrorObject(body)) throw CacheBoyError.fromServer(body.error);
      throw new CacheBoyError('Unexpected /v1/pipeline reply: expected an array', ErrorCodes.BAD_REPLY);
    }
    return cmds.map((c, i) => {
      const item: unknown = body[i];
      if (isErrorObject(item)) return { ok: false, error: CacheBoyError.fromServer(item.error, commandName(c)) };
      if (item && typeof item === 'object' && 'result' in item) return { ok: true, value: (item as { result: unknown }).result };
      return { ok: false, error: new CacheBoyError(`Missing reply for ${commandName(c)} in /v1/pipeline response`, ErrorCodes.BAD_REPLY) };
    });
  }

  private async post(path: string, payload: unknown, timeoutMs?: number): Promise<unknown> {
    const headers: Record<string, string> = { 'content-type': 'application/json', ...(this.options.headers ?? {}) };
    if (this.options.token) headers['authorization'] = `Bearer ${this.options.token}`;
    if (this.options.namespace) headers['x-cacheboy-namespace'] = this.options.namespace;
    const t = timeoutMs ?? this.options.timeoutMs;
    const init: Parameters<FetchLike>[1] = { method: 'POST', headers, body: JSON.stringify(payload) };
    if (t && t > 0) init.signal = AbortSignal.timeout(t);
    let res: Awaited<ReturnType<FetchLike>>;
    try {
      res = await this.fetchFn(this.base + path, init);
    } catch (err) {
      const aborted = (err as Error)?.name === 'TimeoutError' || (err as Error)?.name === 'AbortError';
      throw new CacheBoyError(
        aborted ? `HTTP request to ${path} timed out after ${t} ms` : `HTTP request to ${path} failed: ${(err as Error)?.message ?? err}`,
        aborted ? ErrorCodes.TIMEOUT : ErrorCodes.HTTP,
        { cause: err },
      );
    }
    const text = await res.text();
    let body: unknown;
    try {
      body = text === '' ? undefined : JSON.parse(text);
    } catch {
      body = undefined;
    }
    if (res.status === 401) {
      const msg = isErrorObject(body) ? body.error : 'NOAUTH Authentication required';
      const e = CacheBoyError.fromServer(msg);
      throw e.code === 'ERR' ? new CacheBoyError(msg, 'NOAUTH') : e;
    }
    if (body === undefined) {
      throw new CacheBoyError(`HTTP ${res.status} ${res.statusText ?? ''} from ${path}: ${text.slice(0, 200)}`.trim(), ErrorCodes.HTTP);
    }
    if (!res.ok && !isErrorObject(body)) {
      throw new CacheBoyError(`HTTP ${res.status} from ${path}: ${text.slice(0, 200)}`, ErrorCodes.HTTP);
    }
    return body;
  }
}

function isErrorObject(v: unknown): v is { error: string } {
  return v !== null && typeof v === 'object' && !Array.isArray(v) && typeof (v as { error?: unknown }).error === 'string' && !('result' in v);
}

function unwrap(body: unknown, command: string): unknown {
  if (isErrorObject(body)) throw CacheBoyError.fromServer(body.error, command);
  if (body !== null && typeof body === 'object' && 'result' in body) return (body as { result: unknown }).result;
  throw new CacheBoyError(`Unexpected HTTP reply for ${command}: expected {"result"} or {"error"}`, ErrorCodes.BAD_REPLY);
}
