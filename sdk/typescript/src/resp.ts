/**
 * RESP2/RESP3 wire format: a streaming reply parser and a command encoder.
 *
 * Reply mapping (what the parser hands you):
 *
 * | RESP type                 | JavaScript                                   |
 * |---------------------------|----------------------------------------------|
 * | `+` simple string         | string                                       |
 * | `-` error, `!` bulk error | CacheBoyError (as a value, see below)        |
 * | `:` integer               | number (bigint if outside the safe range)    |
 * | `$` bulk string           | string (utf8), or Buffer in buffer mode      |
 * | `=` verbatim string       | string without the `txt:` prefix             |
 * | `*` array, `~` set        | array                                        |
 * | `%` map                   | plain object (keys converted to strings)     |
 * | `,` double                | number (`inf`, `-inf`, `nan` supported)      |
 * | `#` boolean               | boolean                                      |
 * | `(` big number            | bigint                                       |
 * | `_`, `$-1`, `*-1` null    | null                                         |
 * | `>` push                  | delivered to `onPush` instead of `onReply`   |
 * | `\|` attribute            | parsed and dropped                           |
 *
 * A top-level error is delivered to `onReply` as a CacheBoyError instance, so the
 * connection can reject the matching command. Errors nested inside arrays
 * (EXEC results) stay in the array as values.
 */
import { CacheBoyError, ErrorCodes } from './errors.js';

export type Arg = string | number | bigint | Buffer | Uint8Array;

export interface ParserHandlers {
  /** A reply to a command (anything that isn't a push). */
  onReply(value: unknown): void;
  /** An out-of-band push message (RESP3 `>`): pub/sub, invalidations... */
  onPush(data: unknown[]): void;
}

const INCOMPLETE: unique symbol = Symbol('incomplete');
type Incomplete = typeof INCOMPLETE;
const EMPTY = Buffer.alloc(0);

const CR = 13;
const LF = 10;

export class RespParser {
  /**
   * Called at the start of each top-level reply (never for pushes). When it
   * returns true, bulk strings in that reply are returned as Buffers.
   */
  returnBuffers: () => boolean = () => false;

  private buf: Buffer = EMPTY;
  private pending: Buffer[] = [];
  private pendingLen = 0;
  /** Don't try again until the buffer holds at least this many bytes. */
  private need = 0;
  private pos = 0;

  constructor(private readonly handlers: ParserHandlers) {}

  /** Bytes received but not yet parsed into a complete value. */
  get bufferedBytes(): number {
    return this.buf.length + this.pendingLen;
  }

  /** Drops any partial data (call after a reconnect). */
  reset(): void {
    this.buf = EMPTY;
    this.pending = [];
    this.pendingLen = 0;
    this.need = 0;
  }

  /**
   * Feeds a chunk of bytes. Complete values are dispatched synchronously.
   * Throws CacheBoyError(PROTOCOL) on malformed input; the stream is then
   * unusable and should be reset.
   */
  feed(chunk: Buffer): void {
    if (this.buf.length === 0 && this.pendingLen === 0) {
      this.buf = chunk;
    } else {
      this.pending.push(chunk);
      this.pendingLen += chunk.length;
    }
    if (this.buf.length + this.pendingLen < this.need) return; // large bulk still arriving
    if (this.pendingLen > 0) {
      this.buf = Buffer.concat([this.buf, ...this.pending], this.buf.length + this.pendingLen);
      this.pending = [];
      this.pendingLen = 0;
    }
    const buf = this.buf;
    let offset = 0;
    try {
      while (offset < buf.length) {
        this.pos = offset;
        this.need = 0;
        const isPush = buf[offset] === 0x3e; /* '>' */
        const v = this.parse(buf, !isPush && this.returnBuffers());
        if (v === INCOMPLETE) break;
        offset = this.pos;
        if (isPush) this.handlers.onPush(v as unknown[]);
        else this.handlers.onReply(v);
      }
    } finally {
      // Keep the unparsed tail (also if a handler threw, so nothing is parsed twice).
      this.buf = offset >= buf.length ? EMPTY : buf.subarray(offset);
      if (this.need > 0) this.need -= offset;
    }
  }

  private line(buf: Buffer): string | Incomplete {
    const start = this.pos;
    const cr = buf.indexOf(CR, start);
    if (cr === -1 || cr + 1 >= buf.length) {
      this.need = Math.max(buf.length + 1, cr + 2);
      return INCOMPLETE;
    }
    if (buf[cr + 1] !== LF) throw protocolError('expected CRLF');
    this.pos = cr + 2;
    return buf.toString(isAscii(buf, start, cr) ? 'latin1' : 'utf8', start, cr);
  }

  private int(buf: Buffer): number | Incomplete {
    // Fast path for the common case: a short non-negative integer.
    let p = this.pos;
    let n = 0;
    let neg = false;
    if (buf[p] === 0x2d) {
      neg = true;
      p++;
    }
    const start = p;
    while (p < buf.length) {
      const c = buf[p]!;
      if (c === CR) {
        if (p + 1 >= buf.length) break;
        if (buf[p + 1] !== LF || p === start) throw protocolError('bad length');
        this.pos = p + 2;
        return neg ? -n : n;
      }
      if (c < 0x30 || c > 0x39) throw protocolError('bad length');
      n = n * 10 + (c - 0x30);
      p++;
    }
    this.need = buf.length + 1;
    return INCOMPLETE;
  }

  private parse(buf: Buffer, asBuffer: boolean): unknown {
    if (this.pos >= buf.length) {
      this.need = this.pos + 1;
      return INCOMPLETE;
    }
    const type = buf[this.pos++]!;
    switch (type) {
      case 0x2b: /* + */
        return this.line(buf);
      case 0x2d: /* - */ {
        const s = this.line(buf);
        return s === INCOMPLETE ? s : CacheBoyError.fromServer(s);
      }
      case 0x3a: /* : */ {
        const s = this.line(buf);
        if (s === INCOMPLETE) return s;
        const n = Number(s);
        if (Number.isSafeInteger(n)) return n;
        try {
          return BigInt(s);
        } catch {
          throw protocolError(`bad integer ${JSON.stringify(s)}`);
        }
      }
      case 0x24: /* $ */
      case 0x3d: /* = */
      case 0x21: /* ! */ {
        const len = this.int(buf);
        if (len === INCOMPLETE) return len;
        if (len < 0) return null;
        const start = this.pos;
        const end = start + len;
        if (end + 2 > buf.length) {
          this.need = end + 2;
          return INCOMPLETE;
        }
        if (buf[end] !== CR || buf[end + 1] !== LF) throw protocolError('bulk string not terminated by CRLF');
        this.pos = end + 2;
        if (type === 0x21) return CacheBoyError.fromServer(buf.toString('utf8', start, end));
        if (type === 0x3d) {
          // "txt:" / "mkd:" prefix
          const from = len >= 4 && buf[start + 3] === 0x3a ? start + 4 : start;
          return asBuffer ? Buffer.from(buf.subarray(from, end)) : buf.toString('utf8', from, end);
        }
        // Copy so we never pin the (possibly large) socket buffer.
        return asBuffer ? Buffer.from(buf.subarray(start, end)) : buf.toString('utf8', start, end);
      }
      case 0x2a: /* * */
      case 0x7e: /* ~ */
      case 0x3e: /* > */ {
        const n = this.int(buf);
        if (n === INCOMPLETE) return n;
        if (n < 0) return null;
        const out = new Array<unknown>(n);
        for (let i = 0; i < n; i++) {
          const v = this.parse(buf, asBuffer);
          if (v === INCOMPLETE) return v;
          out[i] = v;
        }
        return out;
      }
      case 0x25: /* % */ {
        const n = this.int(buf);
        if (n === INCOMPLETE) return n;
        if (n < 0) return null;
        const out: Record<string, unknown> = {};
        for (let i = 0; i < n; i++) {
          const k = this.parse(buf, false);
          if (k === INCOMPLETE) return k;
          const v = this.parse(buf, asBuffer);
          if (v === INCOMPLETE) return v;
          setKey(out, k, v);
        }
        return out;
      }
      case 0x7c: /* | attribute: parse it, drop it, return the value that follows */ {
        const n = this.int(buf);
        if (n === INCOMPLETE) return n;
        for (let i = 0; i < n * 2; i++) {
          const v = this.parse(buf, false);
          if (v === INCOMPLETE) return v;
        }
        return this.parse(buf, asBuffer);
      }
      case 0x5f: /* _ */ {
        const s = this.line(buf);
        return s === INCOMPLETE ? s : null;
      }
      case 0x23: /* # */ {
        const s = this.line(buf);
        if (s === INCOMPLETE) return s;
        if (s === 't') return true;
        if (s === 'f') return false;
        throw protocolError(`bad boolean ${JSON.stringify(s)}`);
      }
      case 0x2c: /* , */ {
        const s = this.line(buf);
        if (s === INCOMPLETE) return s;
        return parseDouble(s);
      }
      case 0x28: /* ( */ {
        const s = this.line(buf);
        if (s === INCOMPLETE) return s;
        try {
          return BigInt(s);
        } catch {
          throw protocolError(`bad big number ${JSON.stringify(s)}`);
        }
      }
      default:
        throw protocolError(`unknown type byte 0x${type.toString(16)} (${JSON.stringify(String.fromCharCode(type))})`);
    }
  }
}

function isAscii(buf: Buffer, start: number, end: number): boolean {
  for (let i = start; i < end; i++) if (buf[i]! > 0x7f) return false;
  return true;
}

function setKey(obj: Record<string, unknown>, k: unknown, v: unknown): void {
  const key = Buffer.isBuffer(k) ? k.toString('utf8') : String(k);
  if (key === '__proto__') {
    Object.defineProperty(obj, key, { value: v, enumerable: true, writable: true, configurable: true });
  } else {
    obj[key] = v;
  }
}

function parseDouble(s: string): number {
  switch (s.toLowerCase()) {
    case 'inf':
    case '+inf':
      return Infinity;
    case '-inf':
      return -Infinity;
    case 'nan':
      return NaN;
  }
  const n = Number(s);
  if (Number.isNaN(n)) throw protocolError(`bad double ${JSON.stringify(s)}`);
  return n;
}

function protocolError(msg: string): CacheBoyError {
  return new CacheBoyError(`Protocol error: ${msg}`, ErrorCodes.PROTOCOL);
}

// ---------------------------------------------------------------------------
// Encoder

const CRLF = '\r\n';

function argToBytes(a: Arg): string | Uint8Array {
  if (typeof a === 'string') return a;
  if (typeof a === 'number' || typeof a === 'bigint') return String(a);
  if (a instanceof Uint8Array) return a; // Buffer is a Uint8Array
  throw new TypeError(`cache-boy: invalid command argument of type ${typeof a}`);
}

/**
 * Encodes a command as a RESP array of bulk strings. Binary-safe: Buffers and
 * Uint8Arrays are copied byte for byte; strings are encoded as UTF-8.
 * One allocation per command.
 */
export function encodeCommand(args: readonly Arg[]): Buffer {
  const n = args.length;
  const parts = new Array<string | Uint8Array>(n);
  const lens = new Array<number>(n);
  let total = 1 + digits(n) + 2;
  for (let i = 0; i < n; i++) {
    const p = argToBytes(args[i]!);
    const len = typeof p === 'string' ? Buffer.byteLength(p, 'utf8') : p.byteLength;
    parts[i] = p;
    lens[i] = len;
    total += 1 + digits(len) + 2 + len + 2;
  }
  const out = Buffer.allocUnsafe(total);
  let o = out.write(`*${n}${CRLF}`, 0, 'latin1');
  for (let i = 0; i < n; i++) {
    const p = parts[i]!;
    const len = lens[i]!;
    o += out.write(`$${len}${CRLF}`, o, 'latin1');
    if (typeof p === 'string') o += out.write(p, o, len, 'utf8');
    else {
      out.set(p, o);
      o += len;
    }
    out[o++] = CR;
    out[o++] = LF;
  }
  return out;
}

/** Encodes several commands into one buffer (used for pipelines). */
export function encodeCommands(cmds: readonly (readonly Arg[])[]): Buffer {
  return Buffer.concat(cmds.map(encodeCommand));
}

function digits(n: number): number {
  if (n < 10) return 1;
  if (n < 100) return 2;
  if (n < 1000) return 3;
  if (n < 10000) return 4;
  return String(n).length;
}
