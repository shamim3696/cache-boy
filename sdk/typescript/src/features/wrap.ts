import { type Arg, type Executor, badReply, toArr, toBool, toStr } from '../types.js';

/**
 * What wrap() stores for "fn found nothing" when `negativeTtl` is set. JSON
 * text never contains a raw NUL character, so no JSON-serialized value can be
 * mistaken for it; a custom `serialize` that produces it is rejected.
 */
export const NEGATIVE_SENTINEL = '\u0000cache-boy:absent\u0000';

export interface WrapOptions<T> {
  /** TTL of the cached value, in seconds (SET ... EX ttl). */
  ttl: number;
  /** Tags attached to the cached value (SET ... TAGS ...). */
  tags?: string[];
  /** How long this client may hold the compute lock (CACHE.GET LOCK). Server default 10000. */
  lockMs?: number;
  /** How long to wait for another client's compute (CACHE.GET WAIT). Server default 5000. */
  waitMs?: number;
  /** Validates/transforms the cached JSON, e.g. a zod schema's `parse`. */
  parse?: (v: unknown) => T;
  /** Serializer for the computed value. Default JSON.stringify. */
  serialize?: (v: T) => string;
  /**
   * Called if storing the computed value fails; wrap() then still returns the
   * value. Without it, the SET error is thrown (after CACHE.RELEASE).
   */
  onSetError?: (err: unknown) => void;
  /**
   * Cache-penetration protection: when fn returns null or undefined, remember
   * that for this many seconds (instead of `ttl`), so repeated lookups of
   * missing data don't reach the database. Those hits resolve to null.
   */
  negativeTtl?: number;
  /**
   * Bloom-filter guard: if the filter at `key` says `item` (default: the
   * cache key) was never added, resolve to null without calling fn or the
   * cache. The filter must hold every id that exists (BF.ADD on create); a
   * missing filter counts as empty.
   */
  bloom?: { key: string; item?: string };
}

/** `absent`: the bloom filter ruled the key out, so neither the cache nor fn was used. */
export type WrapStatus = 'hit' | 'compute' | 'timeout' | 'absent';

/**
 * Cache-aside with stampede protection (CACHE.GET protocol):
 * - hit: the cached JSON is parsed and returned
 * - compute: we hold the compute lock; run fn, SET the result (EX + TAGS),
 *   which wakes the waiting clients. If fn throws, CACHE.RELEASE and rethrow.
 * - timeout: another client took too long; run fn ourselves and SET.
 * With `bloom`, a BF.EXISTS miss short-circuits to null first. With
 * `negativeTtl`, a null result is cached as NEGATIVE_SENTINEL.
 */
export async function wrap<T>(
  x: Executor,
  key: string,
  fn: () => Promise<T> | T,
  opts: WrapOptions<T>,
  onStatus?: (s: WrapStatus) => void,
  /** Runs CACHE.GET. It may wait for another client's compute, so the TCP
   * client passes a dedicated connection: our own SET must not queue behind it. */
  waiter: Executor = x,
): Promise<T> {
  if (opts.bloom && !toBool(await x.send(['BF.EXISTS', opts.bloom.key, opts.bloom.item ?? key]))) {
    onStatus?.('absent');
    return null as T;
  }

  const args: Arg[] = ['CACHE.GET', key];
  if (opts.lockMs !== undefined) args.push('LOCK', Math.round(opts.lockMs));
  if (opts.waitMs !== undefined) args.push('WAIT', Math.round(opts.waitMs));
  const waitMs = opts.waitMs ?? 5000;
  const reply = toArr(await waiter.send(args, { timeoutMs: waitMs + 10_000, retryable: false }));
  const status = (toStr(reply[0]) ?? '').toLowerCase();
  onStatus?.(status as WrapStatus);

  if (status === 'hit') {
    const raw = toStr(reply[1]);
    if (raw === NEGATIVE_SENTINEL) return null as T;
    if (raw !== null) {
      const v: unknown = JSON.parse(raw);
      return opts.parse ? opts.parse(v) : (v as T);
    }
    // A hit without a value shouldn't happen; fall through and compute.
  } else if (status !== 'compute' && status !== 'timeout') {
    throw badReply('CACHE.GET', reply);
  }

  let value: T;
  let serialized: string;
  let absent = false;
  try {
    value = await fn();
    absent = opts.negativeTtl !== undefined && (value === null || value === undefined);
    if (absent) {
      serialized = NEGATIVE_SENTINEL;
    } else {
      serialized = opts.serialize ? opts.serialize(value) : JSON.stringify(value === undefined ? null : value);
      if (serialized === NEGATIVE_SENTINEL) throw new TypeError('wrap(): serialize() produced the negative-cache sentinel');
    }
  } catch (err) {
    if (status === 'compute') await x.send(['CACHE.RELEASE', key]).catch(() => undefined);
    throw err;
  }
  const ttl = absent ? opts.negativeTtl! : opts.ttl;
  const set: Arg[] = ['SET', key, serialized, 'EX', Math.max(1, Math.ceil(ttl))];
  if (opts.tags && opts.tags.length > 0) set.push('TAGS', ...opts.tags);
  try {
    await x.send(set);
  } catch (err) {
    // The value is computed; storing it is best effort. Free waiters, then report via onSetError.
    if (status === 'compute') await x.send(['CACHE.RELEASE', key]).catch(() => undefined);
    if (opts.onSetError) opts.onSetError(err);
    else throw err;
  }
  return absent ? (null as T) : value;
}
