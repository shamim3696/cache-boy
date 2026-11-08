import { type Arg, type Executor, toNum } from '../types.js';

/** PFADD: true if the estimate may have changed (a register changed or the key was created). */
export async function pfAdd(x: Executor, key: string, elements: Arg[]): Promise<boolean> {
  return toNum(await x.send(['PFADD', key, ...elements])) === 1;
}

/** PFCOUNT: estimated distinct elements; with several keys, of their union. */
export async function pfCount(x: Executor, keys: string[]): Promise<number> {
  return toNum(await x.send(['PFCOUNT', ...keys]));
}

/** PFMERGE: dest becomes the union of itself and the sources. */
export async function pfMerge(x: Executor, dest: string, sources: string[]): Promise<void> {
  await x.send(['PFMERGE', dest, ...sources]);
}
