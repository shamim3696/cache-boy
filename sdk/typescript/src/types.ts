import type { SendOptions } from './connection.js';
import { CacheBoyError, ErrorCodes } from './errors.js';
import type { Arg } from './resp.js';

export type { Arg } from './resp.js';
export type { SendOptions } from './connection.js';

/** Anything that can run one command. Both clients, pipelines and test mocks implement it. */
export interface Executor {
  send(args: Arg[], opts?: SendOptions): Promise<unknown>;
}

/** A value you can store: objects (and booleans) are JSON-encoded. */
export type StorableValue = string | number | bigint | Buffer | Uint8Array | boolean | object;

/** `<duration>` per docs/features.md: milliseconds, or a string such as "500ms", "10s", "5m", "1h". */
export type Duration = number | string;

const HAS_BUFFER = typeof Buffer !== 'undefined';

/** Buffer.isBuffer that also works where Buffer doesn't exist (edge runtimes). */
export function isBuffer(v: unknown): v is Buffer {
  return HAS_BUFFER && Buffer.isBuffer(v);
}

/** UTF-8 decode without depending on Buffer. */
export function bytesToString(b: Uint8Array): string {
  return HAS_BUFFER ? Buffer.from(b.buffer, b.byteOffset, b.byteLength).toString('utf8') : new TextDecoder().decode(b);
}

/** Upper-case command name of an argument list. */
export function commandName(args: readonly Arg[]): string {
  const a = args[0];
  if (a === undefined) return '';
  return (typeof a === 'string' ? a : a instanceof Uint8Array ? bytesToString(a) : String(a)).toUpperCase();
}

export function encodeValue(v: StorableValue): Arg {
  if (typeof v === 'string' || typeof v === 'number' || typeof v === 'bigint') return v;
  if (v instanceof Uint8Array) return v;
  return JSON.stringify(v);
}

export function durationArg(d: Duration): string {
  if (typeof d === 'number') {
    if (!Number.isFinite(d) || d < 0) throw new RangeError(`invalid duration ${d}`);
    return String(Math.round(d));
  }
  return d;
}

export function badReply(what: string, v: unknown): CacheBoyError {
  let shown: string;
  try {
    shown = JSON.stringify(v, (_k, x: unknown) => (typeof x === 'bigint' ? x.toString() : x));
  } catch {
    shown = String(v);
  }
  return new CacheBoyError(`Unexpected reply for ${what}: ${shown?.slice(0, 200)}`, ErrorCodes.BAD_REPLY);
}

/** A string reply (or null). Buffers are decoded as UTF-8; numbers are stringified. */
export function toStr(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  if (typeof v === 'string') return v;
  if (v instanceof Uint8Array) return bytesToString(v);
  if (typeof v === 'number' || typeof v === 'bigint' || typeof v === 'boolean') return String(v);
  return String(v);
}

export function toNum(v: unknown): number {
  if (typeof v === 'number') return v;
  if (typeof v === 'bigint') return Number(v);
  if (typeof v === 'boolean') return v ? 1 : 0;
  if (typeof v === 'string' || v instanceof Uint8Array) {
    const s = (typeof v === 'string' ? v : bytesToString(v)).toLowerCase();
    if (s === 'inf' || s === '+inf') return Infinity;
    if (s === '-inf') return -Infinity;
    if (s === 'nan') return NaN;
    return Number(s);
  }
  if (v === null || v === undefined) return NaN;
  throw badReply('number', v);
}

export function toNumOrNull(v: unknown): number | null {
  return v === null || v === undefined ? null : toNum(v);
}

export function toBool(v: unknown): boolean {
  return v === true || v === 1 || v === '1' || v === 'OK' || (typeof v === 'bigint' && v !== 0n);
}

export function toArr(v: unknown): unknown[] {
  if (Array.isArray(v)) return v;
  if (v === null || v === undefined) return [];
  throw badReply('array', v);
}

export function toStrArr(v: unknown): string[] {
  return toArr(v).map((x) => toStr(x) ?? '');
}

/** A RESP3 map (object), a RESP2 flat [k, v, k, v] array, or a JSON object → Record. */
export function toRecord(v: unknown): Record<string, unknown> {
  if (v === null || v === undefined) return {};
  if (Array.isArray(v)) {
    const out: Record<string, unknown> = {};
    for (let i = 0; i + 1 < v.length; i += 2) out[toStr(v[i]) ?? ''] = v[i + 1];
    return out;
  }
  if (typeof v === 'object' && !(v instanceof Uint8Array)) return v as Record<string, unknown>;
  throw badReply('map', v);
}

export function toStrRecord(v: unknown): Record<string, string> {
  const r = toRecord(v);
  const out: Record<string, string> = {};
  for (const k of Object.keys(r)) out[k] = toStr(r[k]) ?? '';
  return out;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
