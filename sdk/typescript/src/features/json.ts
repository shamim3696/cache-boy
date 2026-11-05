import { type Arg, type Executor, toArr, toNum, toStr, toStrArr } from '../types.js';

function parseText(v: unknown): unknown {
  const s = toStr(v);
  return s === null ? null : JSON.parse(s);
}

/** Native JSON documents (JSON.*). Paths: `$`, `.field`, `["field"]`, `[index]`, `[*]`. */
export class JsonDocs {
  constructor(private readonly x: Executor) {}

  /** Sets the value at path (use "$" for the whole document). Returns false if NX/XX wasn't met. */
  async set(key: string, path: string, value: unknown, opts: { nx?: boolean; xx?: boolean } = {}): Promise<boolean> {
    const args: Arg[] = ['JSON.SET', key, path, JSON.stringify(value)];
    if (opts.nx) args.push('NX');
    else if (opts.xx) args.push('XX');
    const r = await this.x.send(args);
    return r !== null && r !== undefined;
  }

  /**
   * Without a path (or with "$"): the document itself, or null if the key doesn't exist.
   * With one path: an array of the matches, e.g. `[1]`.
   */
  async get<T = unknown>(key: string, path?: string): Promise<T | null> {
    const args: Arg[] = ['JSON.GET', key];
    if (path !== undefined) args.push(path);
    return parseText(await this.x.send(args)) as T | null;
  }

  /** Several paths: an object of path → matches. */
  async getPaths(key: string, ...paths: string[]): Promise<Record<string, unknown[]> | null> {
    return parseText(await this.x.send(['JSON.GET', key, ...paths])) as Record<string, unknown[]> | null;
  }

  /** Deletes the value at path (whole key if omitted). Returns the number of values deleted. */
  async del(key: string, path?: string): Promise<number> {
    const args: Arg[] = ['JSON.DEL', key];
    if (path !== undefined) args.push(path);
    return toNum(await this.x.send(args));
  }

  async type(key: string, path?: string): Promise<string[]> {
    const args: Arg[] = ['JSON.TYPE', key];
    if (path !== undefined) args.push(path);
    return toStrArr(await this.x.send(args));
  }

  /** Increments the numbers at path. Returns the new values. */
  async numIncrBy(key: string, path: string, by: number): Promise<number[]> {
    const r = parseText(await this.x.send(['JSON.NUMINCRBY', key, path, by]));
    return (Array.isArray(r) ? r : [r]).map((n) => toNum(n));
  }

  /** Appends values to the arrays at path. Returns the new lengths (null where the match isn't an array). */
  async arrAppend(key: string, path: string, ...values: unknown[]): Promise<(number | null)[]> {
    const r = await this.x.send(['JSON.ARRAPPEND', key, path, ...values.map((v) => JSON.stringify(v))]);
    return (Array.isArray(r) ? r : [r]).map((n) => (n === null ? null : toNum(n)));
  }

  async arrLen(key: string, path?: string): Promise<(number | null)[]> {
    const args: Arg[] = ['JSON.ARRLEN', key];
    if (path !== undefined) args.push(path);
    const r = await this.x.send(args);
    return (Array.isArray(r) ? r : [r]).map((n) => (n === null ? null : toNum(n)));
  }

  async objKeys(key: string, path?: string): Promise<(string[] | null)[]> {
    const args: Arg[] = ['JSON.OBJKEYS', key];
    if (path !== undefined) args.push(path);
    return toArr(await this.x.send(args)).map((e) => (e === null ? null : toStrArr(e)));
  }

  /** The value at path in each key (null for missing keys). */
  async mget<T = unknown>(keys: string[], path: string): Promise<(T | null)[]> {
    return toArr(await this.x.send(['JSON.MGET', ...keys, path])).map((e) => parseText(e) as T | null);
  }
}
