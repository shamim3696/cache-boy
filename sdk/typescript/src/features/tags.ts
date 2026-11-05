import { type Executor, toNum, toStrArr } from '../types.js';

/** Tag-based invalidation (TAG.*). Attach tags with `set(key, v, { tags })` or `tags.add`. */
export class Tags {
  constructor(private readonly x: Executor) {}

  /** Attaches tags to an existing key. Returns how many were newly attached (0 if the key doesn't exist). */
  async add(key: string, ...tags: string[]): Promise<number> {
    return toNum(await this.x.send(['TAG.ADD', key, ...tags]));
  }

  /** Detaches tags. Returns how many were removed. */
  async remove(key: string, ...tags: string[]): Promise<number> {
    return toNum(await this.x.send(['TAG.REMOVE', key, ...tags]));
  }

  /** Keys carrying the tag (in the caller's namespace). */
  async keys(tag: string): Promise<string[]> {
    return toStrArr(await this.x.send(['TAG.KEYS', tag]));
  }

  /** Tags of a key. */
  async of(key: string): Promise<string[]> {
    return toStrArr(await this.x.send(['TAG.OF', key]));
  }

  /** Deletes every key with any of the tags. Returns the number of keys deleted. */
  async invalidate(...tags: string[]): Promise<number> {
    if (tags.length === 0) return 0;
    return toNum(await this.x.send(['TAG.INVALIDATE', ...tags]));
  }

  /** Every tag in use. */
  async list(): Promise<string[]> {
    return toStrArr(await this.x.send(['TAG.LIST']));
  }
}
