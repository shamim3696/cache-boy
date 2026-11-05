/** Test helpers: a mock transport, a scriptable fake RESP server, and RESP reply builders. */
import * as net from 'node:net';
import { type BatchItem, CommandsBase } from '../src/commands.js';
import { CacheBoyError } from '../src/errors.js';
import { RespParser, type Arg } from '../src/resp.js';
import type { SendOptions } from '../src/types.js';

export function argStr(a: Arg): string {
  return typeof a === 'string' ? a : a instanceof Uint8Array ? Buffer.from(a).toString() : String(a);
}

/** A transport that records commands and answers from a handler. */
export class MockClient extends CommandsBase {
  readonly calls: string[][] = [];
  readonly opts: (SendOptions | undefined)[] = [];
  constructor(public handler: (args: string[]) => unknown | Promise<unknown> = () => null) {
    super();
  }

  async send(args: Arg[], opts?: SendOptions): Promise<unknown> {
    const a = args.map(argStr);
    this.calls.push(a);
    this.opts.push(opts);
    await Promise.resolve();
    const v = await this.handler(a);
    if (v instanceof Error) throw v;
    return v;
  }

  namespace(): MockClient {
    return this;
  }

  protected async execBatch(cmds: Arg[][]): Promise<BatchItem[]> {
    const out: BatchItem[] = [];
    for (const c of cmds) {
      try {
        out.push({ ok: true, value: await this.send(c) });
      } catch (e) {
        out.push({ ok: false, error: e as CacheBoyError });
      }
    }
    return out;
  }

  names(): string[] {
    return this.calls.map((c) => c[0]!.toUpperCase());
  }
}

// RESP builders ---------------------------------------------------------------
export const R = {
  ok: '+OK\r\n',
  simple: (s: string) => `+${s}\r\n`,
  err: (s: string) => `-${s}\r\n`,
  int: (n: number) => `:${n}\r\n`,
  bulk: (s: string | null) => (s === null ? '$-1\r\n' : `$${Buffer.byteLength(s)}\r\n${s}\r\n`),
  nil: '_\r\n',
  arr: (...items: string[]) => `*${items.length}\r\n${items.join('')}`,
  push: (...items: string[]) => `>${items.length}\r\n${items.join('')}`,
  map: (...kv: string[]) => `%${kv.length / 2}\r\n${kv.join('')}`,
  dbl: (n: string) => `,${n}\r\n`,
};

export interface FakeConn {
  socket: net.Socket;
  commands: string[][];
  write(s: string | Buffer): void;
}

/**
 * A scriptable RESP server. The handler gets each command and returns the raw
 * reply to write (string), or undefined to stay silent.
 */
export class FakeServer {
  readonly server: net.Server;
  readonly conns: FakeConn[] = [];
  readonly commands: string[][] = [];
  /** Number of 'data' events per connection (to check pipelining). */
  readonly dataEvents: number[] = [];
  port = 0;

  constructor(public handler: (args: string[], conn: FakeConn) => string | undefined | void) {
    this.server = net.createServer((socket) => {
      const conn: FakeConn = { socket, commands: [], write: (s) => socket.write(s) };
      const idx = this.conns.push(conn) - 1;
      this.dataEvents[idx] = 0;
      const parser = new RespParser({
        onReply: (v) => {
          const args = (v as unknown[]).map((x) => String(x));
          conn.commands.push(args);
          this.commands.push(args);
          const out = this.handler(args, conn);
          if (typeof out === 'string' && out.length > 0) socket.write(out);
        },
        onPush: () => undefined,
      });
      socket.on('data', (chunk) => {
        this.dataEvents[idx]!++;
        parser.feed(chunk);
      });
      socket.on('error', () => undefined);
    });
  }

  async listen(): Promise<number> {
    await new Promise<void>((r) => this.server.listen(0, '127.0.0.1', () => r()));
    this.port = (this.server.address() as net.AddressInfo).port;
    return this.port;
  }

  /** Kills every client connection (simulates a network drop). */
  dropAll(): void {
    for (const c of this.conns) c.socket.destroy();
  }

  async close(): Promise<void> {
    this.dropAll();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}

/** Default handshake answers: HELLO → map, NAMESPACE/CLIENT/SELECT → OK. */
export function handshake(args: string[]): string | undefined {
  switch (args[0]!.toUpperCase()) {
    case 'HELLO':
      return R.map(R.bulk('server'), R.bulk('cache-boy'), R.bulk('proto'), R.int(3));
    case 'NAMESPACE':
    case 'CLIENT':
    case 'SELECT':
    case 'AUTH':
      return R.ok;
    case 'QUIT':
      return R.ok;
  }
  return undefined;
}

export function until(cond: () => boolean, timeoutMs = 2000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = (): void => {
      if (cond()) resolve();
      else if (Date.now() - start > timeoutMs) reject(new Error('until: timed out'));
      else setTimeout(tick, 5);
    };
    tick();
  });
}

export const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
