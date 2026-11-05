import { type Arg, type Executor, badReply, toArr, toStr } from '../types.js';

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
}

export type WrapStatus = 'hit' | 'compute' | 'timeout';

/**
 * Cache-aside with stampede protection (CACHE.GET protocol):
 * - hit: the cached JSON is parsed and returned
 * - compute: we hold the compute lock; run fn, SET the result (EX + TAGS),
 *   which wakes the waiting clients. If fn throws, CACHE.RELEASE and rethrow.
 * - timeout: another client took too long; run fn ourselves and SET.
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
  const args: Arg[] = ['CACHE.GET', key];
  if (opts.lockMs !== undefined) args.push('LOCK', Math.round(opts.lockMs));
  if (opts.waitMs !== undefined) args.push('WAIT', Math.round(opts.waitMs));
  const waitMs = opts.waitMs ?? 5000;
  const reply = toArr(await waiter.send(args, { timeoutMs: waitMs + 10_000, retryable: false }));
  const status = (toStr(reply[0]) ?? '').toLowerCase();
  onStatus?.(status as WrapStatus);

  if (status === 'hit') {
    const raw = toStr(reply[1]);
    if (raw !== null) {
      const v: unknown = JSON.parse(raw);
      return opts.parse ? opts.parse(v) : (v as T);
    }
    // A hit without a value shouldn't happen; fall through and compute.
  } else if (status !== 'compute' && status !== 'timeout') {
    throw badReply('CACHE.GET', reply);
  }

  let value: T;
  try {
    value = await fn();
  } catch (err) {
    if (status === 'compute') await x.send(['CACHE.RELEASE', key]).catch(() => undefined);
    throw err;
  }
  const serialized = opts.serialize ? opts.serialize(value) : JSON.stringify(value === undefined ? null : value);
  const set: Arg[] = ['SET', key, serialized, 'EX', Math.max(1, Math.ceil(opts.ttl))];
  if (opts.tags && opts.tags.length > 0) set.push('TAGS', ...opts.tags);
  try {
    await x.send(set);
  } catch (err) {
    // The value is computed; storing it is best effort. Free waiters, then report via onSetError.
    if (status === 'compute') await x.send(['CACHE.RELEASE', key]).catch(() => undefined);
    if (opts.onSetError) opts.onSetError(err);
    else throw err;
  }
  return value;
}
