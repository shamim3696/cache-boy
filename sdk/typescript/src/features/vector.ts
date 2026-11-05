import { type Arg, type Executor, toArr, toNum, toStr } from '../types.js';

export interface VectorMatch {
  id: string;
  /** Cosine similarity, best first. */
  score: number;
}

/** Vector similarity search (VADD/VSEARCH/VGET/VDEL). */
export class Vectors {
  constructor(private readonly x: Executor) {}

  /** Adds or replaces a vector. Returns true if it is new. `attr` is stored as JSON for FILTER. */
  async add(key: string, id: string, vector: ArrayLike<number>, opts: { attr?: Record<string, unknown> } = {}): Promise<boolean> {
    const args: Arg[] = ['VADD', key, id, ...Array.from(vector, (n) => n)];
    if (opts.attr !== undefined) args.push('ATTR', JSON.stringify(opts.attr));
    return toNum(await this.x.send(args)) === 1;
  }

  /** The k nearest vectors. `filter` keeps vectors whose attr has that top-level field and value. */
  async search(
    key: string,
    k: number,
    vector: ArrayLike<number>,
    opts: { filter?: Record<string, string | number> | string } = {},
  ): Promise<VectorMatch[]> {
    const args: Arg[] = ['VSEARCH', key, k, ...Array.from(vector, (n) => n)];
    if (opts.filter !== undefined) {
      if (typeof opts.filter === 'string') args.push('FILTER', opts.filter);
      else for (const [f, v] of Object.entries(opts.filter)) args.push('FILTER', `${f}=${v}`);
    }
    return toArr(await this.x.send(args)).map((e) => {
      const a = toArr(e);
      return { id: toStr(a[0]) ?? '', score: toNum(a[1]) };
    });
  }

  async get(key: string, id: string): Promise<number[] | null> {
    const v = await this.x.send(['VGET', key, id]);
    if (v === null || v === undefined) return null;
    return toArr(v).map((n) => toNum(n));
  }

  async del(key: string, id: string): Promise<boolean> {
    return toNum(await this.x.send(['VDEL', key, id])) === 1;
  }

  async card(key: string): Promise<number> {
    return toNum(await this.x.send(['VCARD', key]));
  }

  async dim(key: string): Promise<number> {
    return toNum(await this.x.send(['VDIM', key]));
  }
}
