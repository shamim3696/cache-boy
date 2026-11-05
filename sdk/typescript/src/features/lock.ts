import { CacheBoyError, ErrorCodes } from '../errors.js';
import { type Arg, type Duration, type Executor, durationArg, toNum, toRecord, toStr } from '../types.js';

export interface LockOptions {
  /** Wait up to this long for the lock (server-side). Default: don't wait. */
  waitMs?: number;
  /** Free-form owner id, visible in LOCK.INFO. */
  owner?: string;
}

export interface LockInfo {
  token: number;
  owner: string | null;
  ttlMs: number;
}

/** A held lock. Pass `token` (the fencing token) along with every write to your database. */
export class Lock {
  private released = false;

  constructor(
    private readonly x: Executor,
    readonly name: string,
    /** Fencing token: increases monotonically per server. */
    readonly token: number,
  ) {}

  /** Releases the lock. Returns false if it had already expired or been taken over. */
  async release(): Promise<boolean> {
    if (this.released) return false;
    this.released = true;
    return toNum(await this.x.send(['UNLOCK', this.name, this.token])) === 1;
  }

  /** Resets the lock's TTL. Returns false if the lock is no longer ours. */
  async extend(ttl: Duration): Promise<boolean> {
    return toNum(await this.x.send(['LOCK.EXTEND', this.name, this.token, durationArg(ttl)])) === 1;
  }
}

/** Tries to acquire `name` for `ttl`. Resolves to null if it couldn't (after waitMs). */
export async function acquireLock(x: Executor, name: string, ttl: Duration, opts: LockOptions = {}): Promise<Lock | null> {
  const args: Arg[] = ['LOCK', name, durationArg(ttl)];
  if (opts.waitMs !== undefined) args.push('WAIT', Math.max(0, Math.round(opts.waitMs)));
  if (opts.owner !== undefined) args.push('OWNER', opts.owner);
  // LOCK WAIT blocks server-side; give the client timeout some slack.
  const reply = await x.send(args, opts.waitMs !== undefined && opts.waitMs > 0 ? { timeoutMs: opts.waitMs + 10_000 } : undefined);
  if (reply === null || reply === undefined) return null;
  return new Lock(x, name, toNum(reply));
}

/** Runs fn while holding the lock, then releases it. Throws CacheBoyError(LOCKED) if not acquired. */
export async function withLock<T>(
  x: Executor,
  name: string,
  ttl: Duration,
  fn: (lock: Lock) => Promise<T> | T,
  opts: LockOptions = {},
): Promise<T> {
  const lock = await acquireLock(x, name, ttl, opts);
  if (!lock) throw new CacheBoyError(`Could not acquire lock "${name}"`, ErrorCodes.LOCKED);
  try {
    return await fn(lock);
  } finally {
    await lock.release().catch(() => false);
  }
}

export async function lockInfo(x: Executor, name: string): Promise<LockInfo | null> {
  const v = await x.send(['LOCK.INFO', name]);
  if (v === null || v === undefined) return null;
  const r = toRecord(v);
  return { token: toNum(r['token']), owner: toStr(r['owner']), ttlMs: toNum(r['ttl_ms']) };
}
