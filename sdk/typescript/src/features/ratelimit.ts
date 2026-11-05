import { type Arg, type Duration, type Executor, badReply, durationArg, toArr, toNum } from '../types.js';

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  /** 0 when allowed; otherwise how long until one more request would be allowed. */
  retryAfterMs: number;
  /** How long until the bucket is full again. */
  resetAfterMs: number;
}

export function parseRateLimit(v: unknown): RateLimitResult {
  const a = toArr(v);
  if (a.length < 4) throw badReply('RATELIMIT', v);
  return {
    allowed: toNum(a[0]) === 1 || a[0] === true,
    remaining: toNum(a[1]),
    retryAfterMs: toNum(a[2]),
    resetAfterMs: toNum(a[3]),
  };
}

/** GCRA rate limiting: `limit` requests per `period` (ms number, or "1m", "10s"...). */
export async function rateLimit(
  x: Executor,
  key: string,
  limit: number,
  period: Duration,
  opts: { cost?: number } = {},
): Promise<RateLimitResult> {
  const args: Arg[] = ['RATELIMIT', key, limit, durationArg(period)];
  if (opts.cost !== undefined) args.push('COST', opts.cost);
  return parseRateLimit(await x.send(args));
}

export async function rateLimitPeek(x: Executor, key: string, limit: number, period: Duration): Promise<RateLimitResult> {
  return parseRateLimit(await x.send(['RATELIMIT.PEEK', key, limit, durationArg(period)]));
}

export async function rateLimitReset(x: Executor, key: string): Promise<boolean> {
  return toNum(await x.send(['RATELIMIT.RESET', key])) === 1;
}
