/**
 * Slot hashing unit tests, plus cluster integration tests that run only when
 * CACHEBOY_CLUSTER lists the nodes of a running cluster, e.g.:
 *   CACHEBOY_CLUSTER=redis://127.0.0.1:7001,redis://127.0.0.1:7002 npm test
 */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, describe, it } from 'node:test';
import { CacheBoy } from '../src/client.js';
import { CacheBoyCluster, keyOf, keySlot } from '../src/cluster.js';
import { CacheBoyError } from '../src/errors.js';

describe('keySlot', () => {
  it('matches Redis Cluster CRC16 slots', () => {
    assert.equal(keySlot('foo'), 12182);
    assert.equal(keySlot('123456789'), 12739); // CRC16-XMODEM check value 0x31C3
    assert.equal(keySlot(''), 0);
  });
  it('honours {hashtags}', () => {
    assert.equal(keySlot('{user1}.profile'), keySlot('user1'));
    assert.equal(keySlot('{user1}.profile'), keySlot('{user1}.cart'));
    assert.equal(keySlot('{}foo'), keySlot('{}foo')); // empty tag: whole key
    assert.notEqual(keySlot('{}foo'), keySlot('foo'));
  });
  it('finds the routing key', () => {
    assert.equal(keyOf('GET', ['GET', 'k']), 'k');
    assert.equal(keyOf('PING', ['PING']), undefined);
    assert.equal(keyOf('EVAL', ['EVAL', 'return 1', '1', 'k']), 'k');
    assert.equal(keyOf('EVAL', ['EVAL', 'return 1', '0']), undefined);
    assert.equal(keyOf('OBJECT', ['OBJECT', 'ENCODING', 'k']), 'k');
  });
});

const NODES = process.env['CACHEBOY_CLUSTER']?.split(',');
const skip = NODES ? false : 'set CACHEBOY_CLUSTER to run cluster integration tests';

describe('cluster (integration)', { skip }, () => {
  const c = new CacheBoyCluster({ nodes: NODES ?? [] });
  const p = `cl:${randomUUID().slice(0, 8)}:`;
  after(() => c.quit());

  it('routes keys to the node that owns their slot', async () => {
    const keys = Array.from({ length: 200 }, (_, i) => `${p}k${i}`);
    await Promise.all(keys.map((k, i) => c.set(k, String(i))));
    const got = await Promise.all(keys.map((k) => c.get(k)));
    assert.deepEqual(got, keys.map((_, i) => String(i)));
    assert.ok(c.masters().length >= 2, 'expected a multi-node cluster');
    // Each node holds only its own keys, so writes really were spread out.
    const sizes = await Promise.all(c.masters().map((a) => c.node(a).dbsize()));
    assert.ok(sizes.every((n) => n > 0), `keys on every node: ${sizes}`);
    assert.equal(await c.dbsize(), sizes.reduce((a, b) => a + b, 0)); // broadcast + sum
  });

  it('follows MOVED when the slot map is stale', async () => {
    const key = `${p}moved`;
    await c.set(key, 'v');
    // Point the slot at the wrong node, as after a reshard.
    const slots = (c as unknown as { slots: string[] }).slots;
    const right = slots[keySlot(key)]!;
    slots[keySlot(key)] = c.masters().find((a) => a !== right)!;
    assert.equal(await c.get(key), 'v');
    assert.equal(slots[keySlot(key)], right);
  });

  it('a plain client gets MOVED for a foreign key', async () => {
    const addr = c.masters()[0]!;
    const plain = new CacheBoy({ host: addr.split(':')[0], port: Number(addr.split(':')[1]) });
    try {
      let moved = 0;
      for (let i = 0; i < 20; i++) {
        await plain.get(`${p}probe${i}`).catch((e: unknown) => {
          if (e instanceof CacheBoyError && e.code === 'MOVED') moved++;
        });
      }
      assert.ok(moved > 0);
    } finally {
      await plain.quit();
    }
  });

  it('hashtags keep multi-key commands and MULTI on one node', async () => {
    await c.set(`{${p}u1}.a`, '1');
    await c.set(`{${p}u1}.b`, '2');
    assert.deepEqual(await c.mget(`{${p}u1}.a`, `{${p}u1}.b`), ['1', '2']);
    const res = await c.multi().incr(`{${p}u1}.n`).incr(`{${p}u1}.n`).exec();
    assert.deepEqual(res.map(([err, v]) => [err, v]), [[null, 1], [null, 2]]);
  });

  it('tags invalidate across nodes', async () => {
    const keys = Array.from({ length: 30 }, (_, i) => `${p}t${i}`);
    await Promise.all(keys.map((k) => c.set(k, 'x', { tags: [`${p}grp`] })));
    assert.equal(await c.tags.invalidate(`${p}grp`), 30);
    assert.deepEqual(await Promise.all(keys.map((k) => c.get(k))), keys.map(() => null));
  });

  it('keyWatch sees changes on every node', async () => {
    const seen = new Set<string>();
    const off = await c.keyWatch(`${p}w*`, (key) => seen.add(key));
    const keys = Array.from({ length: 20 }, (_, i) => `${p}w${i}`);
    await Promise.all(keys.map((k) => c.set(k, '1')));
    for (let i = 0; i < 50 && seen.size < keys.length; i++) await new Promise((r) => setTimeout(r, 20));
    await off();
    assert.equal(seen.size, keys.length);
  });
});
