import { type Arg, type Duration, type Executor, badReply, durationArg, toArr, toNum, toNumOrNull, toRecord, toStr } from '../types.js';

/** Turns text into an embedding vector (OpenAI, a local model, ...). */
export type EmbedFn = (text: string) => Promise<number[]> | number[];

export interface SemanticCacheOptions {
  /** Required for get/set/wrap with text: computes the prompt's embedding. */
  embed: EmbedFn;
  /** Cosine similarity needed for a hit (0..1). Server default 0.92. */
  threshold?: number;
  /** Default time to live of stored responses (ms number or "1h", "30m"...). */
  ttl?: Duration;
}

export interface SemanticHit {
  hit: boolean;
  /** The cached response (null on a miss). */
  response: string | null;
  /** Similarity of the closest entry, if any (1 = identical). */
  score: number | null;
  id: string | null;
}

export interface SemanticCacheInfo {
  entries: number;
  dim: number;
  threshold: number;
  maxEntries: number;
  hits: number;
  misses: number;
  hitRatio: number;
}

/**
 * Semantic cache for LLM calls: answers a prompt from the cached response of
 * a *similar* earlier prompt ("capital of France?" ≈ "What's France's
 * capital?"). The server stores vectors; you bring the embedding model.
 *
 * ```ts
 * const llm = cache.semanticCache('answers', { embed: (t) => openai.embed(t), ttl: '1d' });
 * const answer = await llm.wrap(question, () => openai.chat(question));
 * ```
 */
export class SemanticCache {
  constructor(
    private readonly x: Executor,
    readonly name: string,
    private readonly opts: SemanticCacheOptions,
  ) {}

  /** Looks up the most similar cached prompt. */
  async get(prompt: string, opts: { threshold?: number } = {}): Promise<SemanticHit> {
    return this.getByVector(await this.opts.embed(prompt), { ...opts, prompt });
  }

  /** Stores a response for a prompt. Returns the entry id. */
  async set(prompt: string, response: string, opts: { ttl?: Duration } = {}): Promise<string> {
    return this.setByVector(await this.opts.embed(prompt), response, { ...opts, prompt });
  }

  /**
   * Returns the cached response of a similar prompt, or calls `fn`, caches
   * its result and returns it. The embedding is computed once per call.
   */
  async wrap(prompt: string, fn: () => Promise<string> | string, opts: { ttl?: Duration; threshold?: number } = {}): Promise<string> {
    const vec = await this.opts.embed(prompt);
    const found = await this.getByVector(vec, { prompt, threshold: opts.threshold });
    if (found.hit && found.response !== null) return found.response;
    const response = await fn();
    await this.setByVector(vec, response, { prompt, ttl: opts.ttl });
    return response;
  }

  /** Lookup with a precomputed embedding. */
  async getByVector(vec: readonly number[], opts: { prompt?: string; threshold?: number } = {}): Promise<SemanticHit> {
    const args: Arg[] = ['SCACHE.GET', this.name, vectorArg(vec)];
    if (opts.prompt !== undefined) args.push('PROMPT', opts.prompt);
    const threshold = opts.threshold ?? this.opts.threshold;
    if (threshold !== undefined) args.push('THRESHOLD', threshold);
    const a = toArr(await this.x.send(args));
    if (a.length < 4) throw badReply('SCACHE.GET', a);
    return { hit: toStr(a[0]) === 'hit', response: toStr(a[1]), score: toNumOrNull(a[2]), id: toStr(a[3]) };
  }

  /** Store with a precomputed embedding. */
  async setByVector(vec: readonly number[], response: string, opts: { prompt?: string; ttl?: Duration; id?: string } = {}): Promise<string> {
    const args: Arg[] = ['SCACHE.SET', this.name, vectorArg(vec), response];
    if (opts.prompt !== undefined) args.push('PROMPT', opts.prompt);
    if (opts.id !== undefined) args.push('ID', opts.id);
    const ttl = opts.ttl ?? this.opts.ttl;
    if (ttl !== undefined) args.push('PX', durationArg(ttl));
    return toStr(await this.x.send(args)) ?? '';
  }

  async delete(id: string): Promise<boolean> {
    return toNum(await this.x.send(['SCACHE.DEL', this.name, id])) === 1;
  }

  /** Sets the server-side default threshold and/or the maximum number of entries (LRU). */
  async configure(opts: { threshold?: number; maxEntries?: number }): Promise<void> {
    const args: Arg[] = ['SCACHE.CONFIG', this.name];
    if (opts.threshold !== undefined) args.push('THRESHOLD', opts.threshold);
    if (opts.maxEntries !== undefined) args.push('MAXENTRIES', opts.maxEntries);
    await this.x.send(args);
  }

  async info(): Promise<SemanticCacheInfo> {
    const m = toRecord(await this.x.send(['SCACHE.INFO', this.name]));
    return {
      entries: toNum(m.entries),
      dim: toNum(m.dim),
      threshold: toNum(m.threshold),
      maxEntries: toNum(m.maxentries),
      hits: toNum(m.hits),
      misses: toNum(m.misses),
      hitRatio: toNum(m.hit_ratio),
    };
  }

  /** Deletes the whole cache; returns how many entries it had. */
  async drop(): Promise<number> {
    return toNum(await this.x.send(['SCACHE.DROP', this.name]));
  }
}

function vectorArg(vec: readonly number[]): string {
  if (vec.length === 0) throw new TypeError('embedding vector is empty');
  return '[' + vec.join(',') + ']';
}
