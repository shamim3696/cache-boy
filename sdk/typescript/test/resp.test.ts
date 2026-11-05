import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { CacheBoyError } from '../src/errors.js';
import { RespParser, encodeCommand, encodeCommands } from '../src/resp.js';

interface Event {
  t: 'reply' | 'push';
  v: unknown;
}

function collect(): { parser: RespParser; events: Event[] } {
  const events: Event[] = [];
  const parser = new RespParser({
    onReply: (v) => events.push({ t: 'reply', v }),
    onPush: (v) => events.push({ t: 'push', v }),
  });
  return { parser, events };
}

function parseWhole(input: string | Buffer): Event[] {
  const { parser, events } = collect();
  parser.feed(Buffer.isBuffer(input) ? input : Buffer.from(input));
  return events;
}

function parseByteByByte(input: string | Buffer): Event[] {
  const { parser, events } = collect();
  const buf = Buffer.isBuffer(input) ? input : Buffer.from(input);
  for (let i = 0; i < buf.length; i++) parser.feed(buf.subarray(i, i + 1));
  assert.equal(parser.bufferedBytes, 0);
  return events;
}

function parseRandomChunks(input: string | Buffer, seed: number): Event[] {
  const { parser, events } = collect();
  const buf = Buffer.isBuffer(input) ? input : Buffer.from(input);
  let s = seed;
  let i = 0;
  while (i < buf.length) {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    const n = 1 + (s % 17);
    parser.feed(Buffer.from(buf.subarray(i, i + n)));
    i += n;
  }
  return events;
}

/** Normalizes errors so deepEqual can compare them. */
function norm(events: Event[]): unknown[] {
  const fix = (v: unknown): unknown => {
    if (v instanceof CacheBoyError) return { error: v.message, code: v.code };
    if (Array.isArray(v)) return v.map(fix);
    if (v && typeof v === 'object' && !Buffer.isBuffer(v)) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, fix(x)]));
    return v;
  };
  return events.map((e) => ({ t: e.t, v: fix(e.v) }));
}

const ALL_TYPES =
  '+OK\r\n' +
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' +
  ':42\r\n' +
  ':-7\r\n' +
  ':9007199254740993\r\n' +
  '$5\r\nhello\r\n' +
  '$0\r\n\r\n' +
  '$-1\r\n' +
  '*-1\r\n' +
  '_\r\n' +
  '#t\r\n' +
  '#f\r\n' +
  ',3.14\r\n' +
  ',inf\r\n' +
  ',-inf\r\n' +
  ',1e3\r\n' +
  '(3492890328409238509324850943850943825024385\r\n' +
  '=15\r\ntxt:Some string\r\n' +
  '!21\r\nSYNTAX invalid syntax\r\n' +
  '*3\r\n:1\r\n$1\r\na\r\n_\r\n' +
  '~2\r\n$1\r\nx\r\n$1\r\ny\r\n' +
  '%2\r\n$3\r\nkey\r\n$5\r\nvalue\r\n+n\r\n:1\r\n' +
  '>2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n' +
  '|1\r\n+ttl\r\n:3600\r\n$3\r\nbar\r\n' +
  '$6\r\nhéllo\r\n'; // "héllo" is 6 bytes in UTF-8

const EXPECTED: unknown[] = [
  { t: 'reply', v: 'OK' },
  { t: 'reply', v: { error: 'WRONGTYPE Operation against a key holding the wrong kind of value', code: 'WRONGTYPE' } },
  { t: 'reply', v: 42 },
  { t: 'reply', v: -7 },
  { t: 'reply', v: 9007199254740993n },
  { t: 'reply', v: 'hello' },
  { t: 'reply', v: '' },
  { t: 'reply', v: null },
  { t: 'reply', v: null },
  { t: 'reply', v: null },
  { t: 'reply', v: true },
  { t: 'reply', v: false },
  { t: 'reply', v: 3.14 },
  { t: 'reply', v: Infinity },
  { t: 'reply', v: -Infinity },
  { t: 'reply', v: 1000 },
  { t: 'reply', v: 3492890328409238509324850943850943825024385n },
  { t: 'reply', v: 'Some string' },
  { t: 'reply', v: { error: 'SYNTAX invalid syntax', code: 'SYNTAX' } },
  { t: 'reply', v: [1, 'a', null] },
  { t: 'reply', v: ['x', 'y'] },
  { t: 'reply', v: { key: 'value', n: 1 } },
  { t: 'push', v: ['invalidate', ['foo']] },
  { t: 'reply', v: 'bar' },
  { t: 'reply', v: 'héllo' },
];

describe('RespParser', () => {
  it('parses every RESP2/RESP3 type in one chunk', () => {
    assert.deepEqual(norm(parseWhole(ALL_TYPES)), EXPECTED);
  });

  it('parses every type fed one byte at a time', () => {
    assert.deepEqual(norm(parseByteByByte(ALL_TYPES)), EXPECTED);
  });

  it('parses with random chunk splits', () => {
    for (let seed = 1; seed < 50; seed++) assert.deepEqual(norm(parseRandomChunks(ALL_TYPES, seed)), EXPECTED);
  });

  it('parses NaN doubles', () => {
    const [e] = parseWhole(',nan\r\n');
    assert.ok(Number.isNaN(e!.v));
  });

  it('handles deeply nested aggregates', () => {
    const input = '*2\r\n%1\r\n+a\r\n*2\r\n~1\r\n#t\r\n%1\r\n+b\r\n*0\r\n*1\r\n*1\r\n*1\r\n:5\r\n';
    const expected = [{ t: 'reply', v: [{ a: [[true], { b: [] }] }, [[[5]]]] }];
    assert.deepEqual(norm(parseWhole(input)), expected);
    assert.deepEqual(norm(parseByteByByte(input)), expected);
  });

  it('keeps pushes out of the reply stream, in order', () => {
    const input =
      '+r1\r\n' +
      '>3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n' +
      ':2\r\n' +
      '>2\r\n$10\r\ninvalidate\r\n_\r\n' +
      '>2\r\n$10\r\ninvalidate\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n' +
      '$2\r\nr3\r\n';
    const expected = [
      { t: 'reply', v: 'r1' },
      { t: 'push', v: ['message', 'ch', 'hi'] },
      { t: 'reply', v: 2 },
      { t: 'push', v: ['invalidate', null] },
      { t: 'push', v: ['invalidate', ['a', 'b']] },
      { t: 'reply', v: 'r3' },
    ];
    assert.deepEqual(norm(parseWhole(input)), expected);
    assert.deepEqual(norm(parseByteByByte(input)), expected);
  });

  it('handles a large bulk string arriving in many chunks', () => {
    const big = 'x'.repeat(1_000_000);
    const input = Buffer.from(`$${big.length}\r\n${big}\r\n+after\r\n`);
    const { parser, events } = collect();
    for (let i = 0; i < input.length; i += 4096) parser.feed(input.subarray(i, i + 4096));
    assert.equal(events.length, 2);
    assert.equal((events[0]!.v as string).length, big.length);
    assert.equal(events[1]!.v, 'after');
  });

  it('is binary-safe in buffer mode', () => {
    const bytes = Buffer.from([0, 13, 10, 255, 128, 36]);
    const { parser, events } = collect();
    parser.returnBuffers = () => true;
    parser.feed(Buffer.concat([Buffer.from(`$${bytes.length}\r\n`), bytes, Buffer.from('\r\n*1\r\n$1\r\nz\r\n')]));
    assert.ok(Buffer.isBuffer(events[0]!.v));
    assert.deepEqual(events[0]!.v, bytes);
    assert.deepEqual(events[1]!.v, [Buffer.from('z')]);
  });

  it('never returns buffers for pushes', () => {
    const { parser, events } = collect();
    parser.returnBuffers = () => true;
    parser.feed(Buffer.from('>2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n'));
    assert.deepEqual(events[0]!.v, ['invalidate', ['k']]);
  });

  it('parses bulk errors and error codes', () => {
    const [a, b, c] = parseWhole('-ERR unknown command\r\n-MOVED 3999 127.0.0.1:6381\r\n-something lowercase\r\n');
    assert.ok(a!.v instanceof CacheBoyError);
    assert.equal((a!.v as CacheBoyError).code, 'ERR');
    assert.equal((b!.v as CacheBoyError).code, 'MOVED');
    assert.equal((c!.v as CacheBoyError).code, 'ERR');
  });

  it('keeps errors nested in arrays as values (EXEC)', () => {
    const [e] = parseWhole('*2\r\n+OK\r\n-WRONGTYPE bad\r\n');
    const arr = e!.v as unknown[];
    assert.equal(arr[0], 'OK');
    assert.ok(arr[1] instanceof CacheBoyError);
  });

  it('maps keys named __proto__ safely', () => {
    const [e] = parseWhole('%1\r\n$9\r\n__proto__\r\n:1\r\n');
    const obj = e!.v as Record<string, unknown>;
    assert.equal(Object.getPrototypeOf(obj), Object.prototype);
    assert.deepEqual(Object.keys(obj), ['__proto__']);
  });

  it('throws a PROTOCOL error on garbage', () => {
    const { parser } = collect();
    assert.throws(() => parser.feed(Buffer.from('?what\r\n')), (e: unknown) => e instanceof CacheBoyError && e.code === 'PROTOCOL');
  });

  it('can be reset mid-value', () => {
    const { parser, events } = collect();
    parser.feed(Buffer.from('$10\r\nabc'));
    parser.reset();
    parser.feed(Buffer.from('+OK\r\n'));
    assert.deepEqual(events, [{ t: 'reply', v: 'OK' }]);
  });
});

describe('encodeCommand', () => {
  it('encodes strings as a RESP array of bulk strings', () => {
    assert.equal(encodeCommand(['SET', 'key', 'value']).toString(), '*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n');
  });

  it('uses byte lengths for UTF-8', () => {
    assert.equal(encodeCommand(['SET', 'k', 'héllo→']).toString(), '*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$9\r\nhéllo→\r\n');
  });

  it('encodes numbers and bigints', () => {
    assert.equal(encodeCommand(['EXPIRE', 'k', 10, 5n]).toString(), '*4\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\n10\r\n$1\r\n5\r\n');
  });

  it('is binary-safe for Buffers and Uint8Arrays', () => {
    const bin = Buffer.from([0, 13, 10, 255]);
    const out = encodeCommand(['SET', bin, new Uint8Array([1, 2])]);
    const expected = Buffer.concat([Buffer.from('*3\r\n$3\r\nSET\r\n$4\r\n'), bin, Buffer.from('\r\n$2\r\n'), Buffer.from([1, 2]), Buffer.from('\r\n')]);
    assert.deepEqual(out, expected);
  });

  it('handles empty strings and long arguments', () => {
    const long = 'y'.repeat(12345);
    assert.equal(encodeCommand(['SET', '', long]).toString(), `*3\r\n$3\r\nSET\r\n$0\r\n\r\n$12345\r\n${long}\r\n`);
  });

  it('round-trips through the parser', () => {
    const args = ['HSET', 'h', 'f1', 'v1', 'ünï', 'çødé'];
    const { parser, events } = collect();
    parser.feed(encodeCommands([args, ['PING']]));
    assert.deepEqual(events.map((e) => e.v), [args, ['PING']]);
  });

  it('rejects invalid argument types', () => {
    assert.throws(() => encodeCommand(['SET', {} as unknown as string]), TypeError);
  });
});
