import { type Arg, type Executor, toArr, toBool, toNum, toNumOrNull, toRecord } from '../types.js';

export interface BloomReserveOptions {
  /** Each new sub-filter is this many times larger (default 2). */
  expansion?: number;
  /** Never grow: BF.ADD fails once `capacity` items are in. */
  nonScaling?: boolean;
}

export interface BloomInfo {
  capacity: number;
  /** Bytes of memory. */
  size: number;
  filters: number;
  items: number;
  /** null for a non-scaling filter. */
  expansionRate: number | null;
}

/** Element-wise replies may contain errors (e.g. a full non-scaling filter): rethrow the first. */
function toBools(v: unknown): boolean[] {
  return toArr(v).map((x) => {
    if (x instanceof Error) throw x;
    return toBool(x);
  });
}

/** Scalable Bloom filters (BF.*, RedisBloom syntax). */
export class Bloom {
  constructor(private readonly x: Executor) {}

  /** Creates a filter for `capacity` items at `errorRate` false positives (0 < errorRate < 1). */
  async reserve(key: string, errorRate: number, capacity: number, opts: BloomReserveOptions = {}): Promise<void> {
    const args: Arg[] = ['BF.RESERVE', key, errorRate, capacity];
    if (opts.expansion !== undefined) args.push('EXPANSION', opts.expansion);
    if (opts.nonScaling) args.push('NONSCALING');
    await this.x.send(args);
  }

  /** true if the item was added, false if it may already have been there. Creates a default filter if needed. */
  async add(key: string, item: Arg): Promise<boolean> {
    return toBool(await this.x.send(['BF.ADD', key, item]));
  }

  async madd(key: string, items: Arg[]): Promise<boolean[]> {
    return toBools(await this.x.send(['BF.MADD', key, ...items]));
  }

  /** false means the item was definitely never added. */
  async exists(key: string, item: Arg): Promise<boolean> {
    return toBool(await this.x.send(['BF.EXISTS', key, item]));
  }

  async mexists(key: string, items: Arg[]): Promise<boolean[]> {
    return toBools(await this.x.send(['BF.MEXISTS', key, ...items]));
  }

  /** Number of items added (0 for a missing key). */
  async card(key: string): Promise<number> {
    return toNum(await this.x.send(['BF.CARD', key]));
  }

  async info(key: string): Promise<BloomInfo> {
    const r = toRecord(await this.x.send(['BF.INFO', key]));
    return {
      capacity: toNum(r['Capacity']),
      size: toNum(r['Size']),
      filters: toNum(r['Number of filters']),
      items: toNum(r['Number of items inserted']),
      expansionRate: toNumOrNull(r['Expansion rate']),
    };
  }
}
