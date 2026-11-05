import { type Arg, type Executor, toArr, toNum, toRecord, toStr } from '../types.js';

export interface Sample {
  timestamp: number;
  value: number;
}

export type Aggregation = 'avg' | 'sum' | 'min' | 'max' | 'count' | 'first' | 'last' | 'range';

function toSample(v: unknown): Sample {
  const a = toArr(v);
  return { timestamp: toNum(a[0]), value: toNum(a[1]) };
}

/** Time series (TS.*). */
export class TimeSeries {
  constructor(private readonly x: Executor) {}

  async create(key: string, opts: { retentionMs?: number; labels?: Record<string, string> } = {}): Promise<void> {
    const args: Arg[] = ['TS.CREATE', key];
    if (opts.retentionMs !== undefined) args.push('RETENTION', opts.retentionMs);
    if (opts.labels) {
      args.push('LABELS');
      for (const [k, v] of Object.entries(opts.labels)) args.push(k, v);
    }
    await this.x.send(args);
  }

  /** Adds a sample (timestamp "*" or omitted = server time). Returns the timestamp. */
  async add(key: string, timestamp: number | '*' | Date, value: number, opts: { retentionMs?: number } = {}): Promise<number> {
    const ts = timestamp instanceof Date ? timestamp.getTime() : timestamp;
    const args: Arg[] = ['TS.ADD', key, ts, value];
    if (opts.retentionMs !== undefined) args.push('RETENTION', opts.retentionMs);
    return toNum(await this.x.send(args));
  }

  /** The latest sample, or null if the series is empty. */
  async get(key: string): Promise<Sample | null> {
    const a = toArr(await this.x.send(['TS.GET', key]));
    return a.length < 2 ? null : toSample(a);
  }

  async range(
    key: string,
    from: number | '-' | Date = '-',
    to: number | '+' | Date = '+',
    opts: { aggregation?: { type: Aggregation; bucketMs: number } } = {},
  ): Promise<Sample[]> {
    const args: Arg[] = ['TS.RANGE', key, from instanceof Date ? from.getTime() : from, to instanceof Date ? to.getTime() : to];
    if (opts.aggregation) args.push('AGGREGATION', opts.aggregation.type, opts.aggregation.bucketMs);
    return toArr(await this.x.send(args)).map(toSample);
  }

  async info(key: string): Promise<{
    totalSamples: number;
    retentionTime: number;
    firstTimestamp: number;
    lastTimestamp: number;
    labels: Record<string, string>;
  }> {
    const r = toRecord(await this.x.send(['TS.INFO', key]));
    const labelsRaw = r['labels'];
    const labels: Record<string, string> = {};
    if (Array.isArray(labelsRaw)) {
      // [[k, v], ...] or [k, v, k, v]
      if (labelsRaw.every((e) => Array.isArray(e))) for (const e of labelsRaw as unknown[][]) labels[toStr(e[0]) ?? ''] = toStr(e[1]) ?? '';
      else Object.assign(labels, Object.fromEntries(Object.entries(toRecord(labelsRaw)).map(([k, v]) => [k, toStr(v) ?? ''])));
    } else if (labelsRaw && typeof labelsRaw === 'object') {
      for (const [k, v] of Object.entries(labelsRaw)) labels[k] = toStr(v) ?? '';
    }
    return {
      totalSamples: toNum(r['totalSamples'] ?? 0),
      retentionTime: toNum(r['retentionTime'] ?? 0),
      firstTimestamp: toNum(r['firstTimestamp'] ?? 0),
      lastTimestamp: toNum(r['lastTimestamp'] ?? 0),
      labels,
    };
  }
}
