import { CacheBoyError } from '../errors.js';
import { type Arg, type Executor, type SendOptions, badReply, sleep, toArr, toNum, toRecord, toStr } from '../types.js';

export interface PushOptions {
  /** Make the job ready only after this delay. */
  delayMs?: number;
  /** Attempts allowed before the job moves to the dead-letter list (server default 3). */
  retries?: number;
  /** Idempotency id: pushing a duplicate id is ignored and returns the existing id. */
  id?: string;
}

export interface PopOptions {
  /** The job comes back if it isn't acked within this time (server default 30000). */
  visibilityMs?: number;
  /** Wait up to this long for a job. 0 waits forever (TCP only). */
  blockMs?: number;
}

export interface QueueLength {
  ready: number;
  delayed: number;
  inflight: number;
  dead: number;
}

export interface DeadJob<T> {
  id: string;
  payload: T;
  attempts: number;
}

export interface ProcessOptions {
  /** Jobs handled at the same time. Default 1. */
  concurrency?: number;
  visibilityMs?: number;
  /** How long each fetch blocks waiting for a job. Default 1000 (also bounds how long stop() waits). */
  blockMs?: number;
  /** Delay before a failed job is retried. A function gets the job and the error. */
  nackDelayMs?: number | ((job: Job<unknown>, err: unknown) => number);
  /** Called for handler errors and fetch errors. Default: ignore (the job is nacked). */
  onError?: (err: unknown, job?: Job<unknown>) => void;
}

/**
 * Payloads are JSON-encoded on push (strings too, so they round-trip), except
 * Buffers which are sent raw. On pop the payload is JSON-decoded; if it isn't
 * valid JSON (pushed by another client), the raw string is returned.
 */
export function encodePayload(p: unknown): Arg {
  if (p instanceof Uint8Array) return p;
  const s = JSON.stringify(p);
  if (s === undefined) throw new TypeError('queue payload is not JSON-serializable');
  return s;
}

export function decodePayload(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

export class Job<T = unknown> {
  private settled = false;

  constructor(
    private readonly x: Executor,
    readonly queue: string,
    readonly id: string,
    readonly payload: T,
    /** The payload as stored on the server. */
    readonly raw: string,
    /** 1 on the first delivery. */
    readonly attempt: number,
  ) {}

  /** True once ack() or nack() was called. */
  get done(): boolean {
    return this.settled;
  }

  /** Marks the job as done. */
  async ack(): Promise<boolean> {
    if (this.settled) return false;
    this.settled = true;
    return toNum(await this.x.send(['QACK', this.queue, this.id])) === 1;
  }

  /** Puts the job back (after delayMs), or moves it to the dead-letter list if out of retries. */
  async nack(delayMs?: number): Promise<boolean> {
    if (this.settled) return false;
    this.settled = true;
    const args: Arg[] = ['QNACK', this.queue, this.id];
    if (delayMs !== undefined && delayMs > 0) args.push('DELAY', Math.round(delayMs));
    return toNum(await this.x.send(args)) === 1;
  }
}

export class Queue<T = unknown> {
  constructor(
    private readonly x: Executor,
    readonly name: string,
    /** Executor used for blocking pops (a dedicated connection on TCP). */
    private readonly blocking: () => Executor = () => x,
  ) {}

  /** Adds a job. Returns its id. */
  async push(payload: T, opts: PushOptions = {}): Promise<string> {
    const args: Arg[] = ['QPUSH', this.name, encodePayload(payload)];
    if (opts.delayMs !== undefined && opts.delayMs > 0) args.push('DELAY', Math.round(opts.delayMs));
    if (opts.retries !== undefined) args.push('RETRIES', opts.retries);
    if (opts.id !== undefined) args.push('ID', opts.id);
    return toStr(await this.x.send(args)) ?? '';
  }

  /**
   * Takes the next ready job, or null. With blockMs, waits on the blocking
   * executor (so other commands on the main connection aren't held up).
   */
  async pop(opts: PopOptions = {}): Promise<Job<T> | null> {
    const args: Arg[] = ['QPOP', this.name];
    if (opts.visibilityMs !== undefined) args.push('VISIBILITY', Math.round(opts.visibilityMs));
    let ex = this.x;
    let sendOpts: SendOptions | undefined;
    if (opts.blockMs !== undefined) {
      args.push('BLOCK', Math.round(opts.blockMs));
      ex = this.blocking();
      sendOpts = { timeoutMs: opts.blockMs === 0 ? 0 : opts.blockMs + 10_000, retryable: false };
    }
    const v = await ex.send(args, sendOpts);
    if (v === null || v === undefined) return null;
    const a = toArr(v);
    if (a.length < 3) throw badReply('QPOP', v);
    const raw = toStr(a[1]) ?? '';
    return new Job<T>(this.x, this.name, toStr(a[0]) ?? '', decodePayload(raw) as T, raw, toNum(a[2]));
  }

  async len(): Promise<QueueLength> {
    const r = toRecord(await this.x.send(['QLEN', this.name]));
    return { ready: toNum(r['ready'] ?? 0), delayed: toNum(r['delayed'] ?? 0), inflight: toNum(r['inflight'] ?? 0), dead: toNum(r['dead'] ?? 0) };
  }

  /** Jobs in the dead-letter list. */
  async dead(count?: number): Promise<DeadJob<T>[]> {
    const args: Arg[] = ['QDEAD', this.name];
    if (count !== undefined) args.push(count);
    return toArr(await this.x.send(args)).map((e) => {
      const a = toArr(e);
      return { id: toStr(a[0]) ?? '', payload: decodePayload(toStr(a[1]) ?? '') as T, attempts: toNum(a[2]) };
    });
  }

  /** Moves dead jobs back to ready. Returns how many. */
  async retryDead(): Promise<number> {
    return toNum(await this.x.send(['QRETRYDEAD', this.name]));
  }

  /** Removes every job in every state. Returns how many. */
  async purge(): Promise<number> {
    return toNum(await this.x.send(['QPURGE', this.name]));
  }

  /**
   * Starts a worker loop: pops jobs, runs handler, acks on success and nacks on
   * throw. Call `stop()` on the result to finish gracefully (in-flight handlers
   * complete, no new jobs are taken).
   */
  process(handler: (job: Job<T>) => Promise<unknown> | unknown, opts: ProcessOptions = {}): Worker<T> {
    return new Worker<T>(this, handler, opts);
  }
}

export class Worker<T = unknown> {
  private stopped = false;
  private readonly active = new Set<Promise<void>>();
  private readonly loop: Promise<void>;
  /** Jobs completed successfully / failed, since start. */
  processed = 0;
  failed = 0;

  constructor(
    private readonly queue: Queue<T>,
    private readonly handler: (job: Job<T>) => Promise<unknown> | unknown,
    private readonly opts: ProcessOptions,
  ) {
    this.loop = this.run();
  }

  get running(): boolean {
    return !this.stopped;
  }

  /** Number of handlers running right now. */
  get activeCount(): number {
    return this.active.size;
  }

  /** Stops taking jobs and waits for running handlers (and the current fetch) to finish. */
  async stop(): Promise<void> {
    this.stopped = true;
    await this.loop;
  }

  private async run(): Promise<void> {
    const concurrency = Math.max(1, this.opts.concurrency ?? 1);
    const blockMs = this.opts.blockMs ?? 1000;
    let errorDelay = 100;
    while (!this.stopped) {
      if (this.active.size >= concurrency) {
        await Promise.race(this.active);
        continue;
      }
      const started = Date.now();
      let job: Job<T> | null;
      try {
        job = await this.queue.pop({ blockMs, ...(this.opts.visibilityMs !== undefined ? { visibilityMs: this.opts.visibilityMs } : {}) });
        errorDelay = 100;
      } catch (err) {
        this.report(err);
        if (!this.stopped) await sleep(errorDelay);
        errorDelay = Math.min(errorDelay * 2, 5000);
        continue;
      }
      if (!job) {
        // A transport that doesn't block (or a server that returned early): don't spin.
        if (!this.stopped && Date.now() - started < Math.min(50, blockMs || 50)) await sleep(Math.min(100, blockMs || 100));
        continue;
      }
      const p: Promise<void> = this.handle(job).finally(() => this.active.delete(p));
      this.active.add(p);
    }
    await Promise.allSettled([...this.active]);
  }

  private async handle(job: Job<T>): Promise<void> {
    try {
      await this.handler(job);
      this.processed++;
      if (!job.done) await job.ack();
    } catch (err) {
      this.failed++;
      this.report(err, job);
      if (!job.done) {
        const d = this.opts.nackDelayMs;
        const delay = typeof d === 'function' ? d(job as Job<unknown>, err) : d;
        await job.nack(delay).catch((e: unknown) => this.report(e, job));
      }
    }
  }

  private report(err: unknown, job?: Job<T>): void {
    try {
      this.opts.onError?.(err instanceof Error ? err : new CacheBoyError(String(err)), job as Job<unknown> | undefined);
    } catch {
      /* ignore */
    }
  }
}
