# Automatic cache invalidation from your database

> **The cache that never serves stale data.**

cache-boy can watch your database and invalidate cache entries the moment the rows behind them change. You write no cache-deletion code, and the cache stays correct even when data changes outside your app: an admin panel, another service, a migration, someone running SQL by hand.

It works with **PostgreSQL**, **MySQL** (and MariaDB) and **MongoDB**, and it is **off unless you turn it on**.

```
UPDATE products SET price = 24999 WHERE id = 777;   -- from any app, script or SQL console
        │
        ▼  the database's change stream (WAL / binlog / change stream)
db-sync         rule: products → tag "product:{id}"
        │
        ▼
cache-boy       TAG.INVALIDATE product:777   → every key tagged product:777 is gone
```

## Contents

- [On or off: the auto_invalidate switch](#on-or-off-the-auto_invalidate-switch)
- [Why](#why)
- [A real example](#a-real-example)
- [How it works](#how-it-works)
- [Setup](#setup)
- [Rules](#rules)
- [PostgreSQL](#postgresql)
- [MySQL and MariaDB](#mysql-and-mariadb)
- [MongoDB](#mongodb)
- [Configuration reference](#configuration-reference)
- [Running in production](#running-in-production)
- [Limitations](#limitations)

## On or off: the auto_invalidate switch

Whether the cache follows the database is a decision you make in the config:

```yaml
auto_invalidate: true    # database changes invalidate the cache automatically
```

| Config | What happens |
|---|---|
| `auto_invalidate: true` | Changes to the configured tables invalidate the matching cache entries, within milliseconds of the commit |
| `auto_invalidate: false`, or the line is missing | **Nothing is automatic.** cache-boy logs `auto invalidation is off` and doesn't connect to the databases. Your application invalidates the cache itself (`cb.tags.invalidate(...)`, `TAG.INVALIDATE`, `DEL`), as without this feature |
| No `db-sync` config at all | Same as off |

Because the default is off, adding a config file never changes behaviour by surprise; flipping one line turns it on or off.

Invalidation means the stale entry is **deleted**. The next read loads fresh data, for example through `wrap()`, which loads only that one entry (one database query, even if a thousand requests arrive at once). Nothing is preloaded and the whole table is never read.

## Why

The usual approach is to delete cache keys in application code after every write:

```ts
await db.products.update(777, { price: 24999 });
await cb.tags.invalidate('product:777');   // easy to forget somewhere
```

This breaks in predictable ways: someone forgets a call, a write happens in a different service or an admin tool, a bulk `UPDATE` runs from a script. Users then see stale data (an old price, an old name) until the TTL runs out. With auto invalidation, if the database changed, the cache is invalidated; nobody needs to remember.

## A real example

An online shop on PostgreSQL with 500 000 products, 2 million reviews and 200 000 visitors a day.

`cache-boy.conf`:
```
maxmemory 1gb
maxmemory-policy allkeys-tinylfu
db-sync shop-sync.yaml
```

`shop-sync.yaml`:
```yaml
auto_invalidate: true
sources:
  - type: postgres
    url: postgres://shop:${PG_PASSWORD}@db:5432/shop
    rules:
      - table: products
        tags: ["product:{id}", "category:{category_id}"]
      - table: reviews
        tags: ["product:{product_id}"]      # a new review changes the product page
```

Application code (no cache deletion anywhere):
```ts
app.get('/product/:id', async (req, res) => {
  const id = req.params.id;
  const page = await cb.wrap(`product-page:${id}`, async () => ({
    product: await db.query('SELECT * FROM products WHERE id = $1', [id]),
    reviews: await db.query('SELECT * FROM reviews WHERE product_id = $1 LIMIT 20', [id]),
  }), { ttl: 3600, tags: [`product:${id}`] });
  res.json(page);
});
```

What happens during the day:

| Time | Event | Result |
|---|---|---|
| 10:00 | First visitor opens product 777 | 2 queries (≈45 ms); page cached with tag `product:777` |
| 10:00–12:00 | 5 000 more visitors | Served from cache in under 1 ms; 0 queries |
| 12:00 | Flash sale: 10 000 visitors at once, right as the TTL ends | `wrap()` lets one request query the database; the rest wait a few ms for its result |
| 12:15 | An admin changes the price in a separate admin app | Within milliseconds `product:777` is invalidated; the next visitor gets the new price. Nobody sees the old one |
| 14:00 | A customer posts a review | The `reviews` rule invalidates `product:777`; the new review shows at once |
| 02:00 | A nightly script updates stock for 100 products | Exactly those 100 products and `category:5` are invalidated; the other 499 900 stay cached |

Only pages people actually visit are in memory (≈150 MB here), capped by `maxmemory`.

## How it works

1. You tag cache entries when you store them, as usual in cache-boy (`tags: ['product:777']`).
2. Rules tell db-sync which tags a table's rows map to (`product:{id}`).
3. db-sync reads the database's change stream. For each **committed** transaction it collects the tags and keys of every changed row, removes duplicates, sends `TAG.INVALIDATE` / `DEL`, and only then records that the change was handled.

Properties:

- **Nothing is missed.** The stream position is stored (a PostgreSQL replication slot, or a state file for MySQL and MongoDB). If db-sync or cache-boy is down, the database keeps the changes and they are applied when both are back. Delivery is *at least once*: after a crash the last changes may be invalidated twice, which is harmless.
- **Only committed data counts.** Rolled-back transactions invalidate nothing.
- **One command per transaction.** A transaction that updates 10 000 rows of the same user sends `user:42` once.
- **No load on your tables.** It reads the log the database writes anyway: no triggers, no polling queries.
- **Several databases at once.** One config can list PostgreSQL, MySQL and MongoDB sources together.

## Setup

### 1. Prepare the database

Each database needs its change stream turned on and a user allowed to read it. See [PostgreSQL](#postgresql), [MySQL](#mysql-and-mariadb) or [MongoDB](#mongodb).

### 2. Write the config

```yaml
# shop-sync.yaml
auto_invalidate: true
sources:
  - type: postgres
    url: postgres://cache_sync:${PG_PASSWORD}@db:5432/shop
    rules:
      - table: users
        tags: ["user:{id}", "users:list"]
  - type: mysql
    url: mysql://cache_sync:${MYSQL_PASSWORD}@mysql:3306/catalog
    rules:
      - table: products
        tags: ["product:{id}"]
  - type: mongodb
    url: mongodb://mongo:27017/cms?replicaSet=rs0
    rules:
      - collection: posts
        tags: ["post:{_id}", "author:{author.id}"]
```

`${NAME}` is replaced with the environment variable `NAME`, so passwords stay out of the file. An annotated example is in [configs/cache-boy-sync.yaml](../configs/cache-boy-sync.yaml).

### 3. Run it: inside the server or as its own process

**Inside cache-boy (simplest).** Add one line to `cache-boy.conf` and restart the server:
```
db-sync /etc/cache-boy/shop-sync.yaml
```
The server runs db-sync itself and invalidates through its normal command path, so invalidations also reach replicas and the AOF. State files go to the server's `dir`. Replicas don't run it; the leader does. The option is read at startup.

**As a separate process.** Run `cache-boy-sync` next to your app or database (it connects to cache-boy over the network, using `cache.addr`, `cache.password` and so on from the config):
```bash
cache-boy-sync -config shop-sync.yaml
```
Choose this when the server can't reach the database, or to restart the sync independently.

Either way, check it first without touching the cache:
```bash
cache-boy-sync -config shop-sync.yaml -dry-run
```
```
postgres: streaming changes from slot "cache_boy" (publication "cache_boy", 1 rules)
dry run: would invalidate tags [user:42 users:list] and delete keys []
```

## Rules

```yaml
rules:
  - table: orders             # "name", or "schema.name" / "database.name"; MongoDB: collection: name
    tags: ["user:{user_id}", "order:{id}", "orders:recent"]
    keys: ["order:{id}:summary"]
    on: [insert, update, delete, truncate]   # optional; default all four
```

- **tags** are invalidated with `TAG.INVALIDATE`, which deletes every key carrying that tag.
- **keys** are deleted directly with `DEL`, for single keys you don't tag.
- **`{column}`** is replaced with the changed row's value. A template may use several: `"{tenant_id}:user:{id}"`. MongoDB fields can be dotted paths: `{author.id}`, `{tags.0}`. If a value is NULL or unknown, that one tag is skipped.
- **Static tags** (no placeholders) are invalidated on every change to the table. Use them for lists, counts and search results: `"users:list"`.
- **Updates count twice.** Both the old and the new row produce tags, so an order moving from user 1 to user 2 invalidates both `user:1` and `user:2`. This needs the database to send the old row; see "old row" under each database.
- **TRUNCATE** (and dropping a MongoDB collection) carries no rows, so only static tags and keys are invalidated, with a warning in the log.
- Several rules may name the same table.

How values become text: numbers in plain decimal (`42`, `12.50` for a DECIMAL with scale 2, whole MongoDB doubles as `42`), MongoDB ObjectIds as hex, booleans `true`/`false`, UUIDs in the usual 8-4-4-4-12 form, timestamps in UTC.

## PostgreSQL

**Requirements**
- PostgreSQL 10+ with `wal_level = logical` (restart after changing it). Managed services: `rds.logical_replication = 1` on AWS RDS/Aurora, the `cloudsql.logical_decoding` flag on Google Cloud SQL, logical replication on Supabase and Neon, "Replication support: Logical" on Azure.
- A user with the `REPLICATION` attribute that owns the watched tables (or a superuser):
  ```sql
  CREATE ROLE cache_sync WITH LOGIN REPLICATION PASSWORD '...';
  ```
  If it can't own the tables, have an owner create the publication once and db-sync will use it: `CREATE PUBLICATION cache_boy FOR TABLE users, orders;`

**What it creates**: a publication and a replication slot, both named `cache_boy` by default (`publication:` and `slot:` in the source). The slot stores the position, so no state file is needed.

**Old row.** By default Postgres sends only the old row's primary key on UPDATE and DELETE. For tags built from other columns (`user:{user_id}` on `orders`):
```sql
ALTER TABLE orders REPLICA IDENTITY FULL;
```
db-sync checks this at startup and logs the exact command for each table that needs it.

**Uninstall.** A slot nobody reads makes Postgres keep WAL forever. When you stop using db-sync for good:
```bash
cache-boy-sync -config shop-sync.yaml -uninstall    # drops the slot and the publication
```

## MySQL and MariaDB

**Requirements**
- Binary logging on with `binlog_format = ROW` (the default on MySQL 8). `binlog_row_image = FULL` (also the default) is needed for tags from non-key columns, and `binlog_row_metadata = FULL` is recommended so column names come from the binlog itself.
- A user with replication rights:
  ```sql
  CREATE USER 'cache_sync'@'%' IDENTIFIED BY '...';
  GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'cache_sync'@'%';
  ```
- Managed services: on AWS RDS/Aurora set `binlog_format = ROW` in the parameter group and keep binlogs long enough (`CALL mysql.rds_set_configuration('binlog retention hours', 24);`); Google Cloud SQL and Azure enable binlogs with point-in-time recovery.

**URL**: `mysql://user:pass@host:3306/db` or the Go DSN form `user:pass@tcp(host:3306)/db`. Add `?tls=true` (or `skip-verify`) for TLS. Tables in rules default to the database in the URL; use `db.table` for others.

**Position**: db-sync connects like a replica (with `server_id`, default 1673; change it if that id is taken) and stores its binlog position, plus the GTID set when `gtid_mode = ON`, in a state file (`mysql.cbsync-state` by default). The first start begins at the current position. The position is saved only after invalidation succeeds.

**Old row**: with `binlog_row_image = FULL` the whole old row is always there. With `MINIMAL` or `NOBLOB`, db-sync warns which tags can't be computed.

**Keep binlogs long enough**: MySQL deletes binlogs after `binlog_expire_logs_seconds` (30 days by default). If db-sync is down longer than that, it can't catch up and tells you to clear the cache and delete the state file.

## MongoDB

**Requirements**
- A replica set or sharded cluster (change streams don't work on a standalone server). A single server can run as a one-member replica set: start `mongod --replSet rs0` and run `rs.initiate()` once. MongoDB Atlas works as is.
- A user that can read the watched databases (the `read` role is enough).

**URL**: a normal connection string, `mongodb://user:pass@host:27017/shop?replicaSet=rs0` or `mongodb+srv://...`. Collections in rules default to `database:` in the source, else the database in the URL path; use `db.collection` for others.

**Old document.** Updates and deletes carry only `_id` unless pre-images are on (MongoDB 6.0+). For tags from other fields (`author:{author.id}`):
```js
db.runCommand({ collMod: "posts", changeStreamPreAndPostImages: { enabled: true } })
```
db-sync checks this at startup and logs the exact command for each collection that needs it.

**Position**: the change stream's resume token, kept in a state file (`mongodb.cbsync-state` by default) and saved after each invalidated batch. It is also refreshed while idle, so it stays inside the oplog. If db-sync is down longer than the oplog window, it can't resume and tells you to delete the state file.

## Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `auto_invalidate` | `false` | The switch. Must be `true` for anything to happen |
| `cache.addr` | `127.0.0.1:6380` | cache-boy address (separate process only) |
| `cache.username` / `cache.password` | none | cache-boy credentials. The ACL user needs `TAG.INVALIDATE` and `DEL` |
| `cache.namespace` | none | Run the invalidations in this namespace |
| `sources[].type` | (required) | `postgres`, `mysql` or `mongodb` |
| `sources[].name` | the type | Name in logs and in the default state file name |
| `sources[].url` | (required) | Connection string |
| `sources[].slot` / `publication` | `cache_boy` | PostgreSQL replication slot and publication |
| `sources[].server_id` | `1673` + index | MySQL replica id; must be unique among the server's replicas |
| `sources[].database` | from the URL | MongoDB database |
| `sources[].state_file` | `<name>.cbsync-state` | MySQL/MongoDB position file (relative paths go to the server's `dir`, or `-state-dir`) |
| `sources[].rules[]` | (required) | `table` or `collection`, `tags`, `keys`, `on` |

`cache-boy-sync` flags:

| Flag | Meaning |
|---|---|
| `-config file` | Config file (default `cache-boy-sync.yaml`) |
| `-v` | Log every invalidation |
| `-dry-run` | Log what would be invalidated; don't touch the cache |
| `-state-dir dir` | Where state files go |
| `-uninstall` | Drop the PostgreSQL slot and publication and delete state files, then exit |
| `-version` | Print the version |

## Running in production

- **Run one instance per database.** Two readers on the same PostgreSQL slot can't both stream, and two MySQL readers with the same `server_id` disconnect each other. Use a supervisor that restarts it, rather than two copies.
- **Embedded or separate**: embedded is one less thing to deploy; a separate process (systemd unit, container, one-replica Kubernetes Deployment) can be restarted on its own. It needs little memory and CPU.
- **Monitor the lag.** While db-sync is stopped the database retains its log. On PostgreSQL:
  ```sql
  SELECT slot_name, active, pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn) AS behind
  FROM pg_replication_slots;
  ```
  (`max_slot_wal_keep_size` caps how much WAL a slot may hold on Postgres 13+.)
- **Keep TTLs.** Automatic invalidation removes stale data within milliseconds of a commit, but a TTL is still a good safety net.
- **Uninstall properly** when you stop using it: `-uninstall` drops the PostgreSQL slot, which otherwise keeps WAL forever.

## Limitations

- Changes made before db-sync first started (or while it couldn't catch up) aren't replayed. Start with an empty or flushed cache, or let existing entries expire.
- After an outage, entries can be stale until db-sync catches up; short TTLs bound how stale.
- Large transactions are invalidated after they commit. A MongoDB transaction with more than 500 changes may be invalidated in several batches.
- MySQL XA transactions are invalidated at `XA PREPARE`. Dropping a watched MySQL table invalidates nothing (TRUNCATE does).
- MongoDB before 6.0 has no pre-images, so deletes can only use `_id`.
