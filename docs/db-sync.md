# Automatic cache invalidation from PostgreSQL

`cache-boy-sync` watches your PostgreSQL database and invalidates cache entries the moment the rows behind them change. You don't write a single line of cache-deletion code, and the cache stays correct even when data changes outside your app: an admin panel, another service, a migration, someone running SQL by hand.

```
UPDATE users SET name = 'Karim' WHERE id = 42;     -- in any client, any service
        │
        ▼  Postgres change stream (logical replication)
cache-boy-sync      rule: users → tag "user:{id}"
        │
        ▼
cache-boy           TAG.INVALIDATE user:42   → every key tagged user:42 is gone
```

## Why

The usual approach is to delete cache keys in application code after every write:

```ts
await db.users.update(42, { name: 'Karim' });
await cache.del('user:42');
await cache.del('user:42:profile');   // easy to forget one...
```

This breaks in predictable ways. Someone forgets a key, a write happens in a different service, a bulk `UPDATE` runs from a script, or two writes race. Users then see stale data until the TTL runs out. `cache-boy-sync` moves invalidation next to the source of truth: if the database changed, the cache is invalidated. Nothing else needs to remember.

## How it works

1. You tag cache entries when you store them, as usual in cache-boy:
   ```ts
   await cb.wrap(`user:${id}:profile`, () => loadProfile(id), { ttl: 3600, tags: [`user:${id}`] });
   ```
2. You tell `cache-boy-sync` which tags a table's rows map to:
   ```yaml
   rules:
     - table: users
       tags: ["user:{id}"]
   ```
3. `cache-boy-sync` reads Postgres's logical replication stream. For each **committed** transaction it collects the tags and keys of every changed row, removes duplicates, sends `TAG.INVALIDATE` / `DEL` to cache-boy, and only then tells Postgres the transaction is handled.

Properties:

- **Nothing is missed.** Progress is stored in a Postgres *replication slot*. If `cache-boy-sync` or cache-boy is down, Postgres keeps the changes and they are applied when both are back. (Delivery is at least once: after a crash the last transactions may be invalidated twice, which is harmless.)
- **Only committed data counts.** Rolled-back transactions invalidate nothing.
- **One command per transaction.** A transaction that updates 10 000 rows of the same user sends one `user:42`.
- **No load on your tables.** It reads the WAL that Postgres writes anyway; no triggers, no polling queries.

## Requirements

- PostgreSQL 10 or newer with `wal_level = logical`. Managed services have a setting for it: `rds.logical_replication = 1` on AWS RDS/Aurora, the `cloudsql.logical_decoding` flag on Google Cloud SQL, logical replication in Supabase and Neon (on by default in Supabase), "Replication support: Logical" on Azure.
- A database user with the `REPLICATION` attribute that owns the tables (or a superuser):
  ```sql
  CREATE ROLE cache_sync WITH LOGIN REPLICATION PASSWORD '...';
  ALTER TABLE users OWNER TO cache_sync;   -- or create the publication yourself, see below
  ```
- A cache-boy server it can reach.

## Quick start

1. Turn on logical replication in `postgresql.conf` and restart Postgres:
   ```
   wal_level = logical
   ```
2. Write `cache-boy-sync.yaml` (an annotated example is in [configs/cache-boy-sync.yaml](../configs/cache-boy-sync.yaml)):
   ```yaml
   postgres:
     url: postgres://cache_sync:${PG_PASSWORD}@db.internal:5432/shop
   cache:
     addr: 127.0.0.1:6380
   rules:
     - table: users
       tags: ["user:{id}", "users:list"]
     - table: orders
       tags: ["user:{user_id}", "order:{id}"]
       keys: ["order:{id}:summary"]
   ```
3. Check what it would do without touching the cache:
   ```bash
   PG_PASSWORD=... cache-boy-sync -config cache-boy-sync.yaml -dry-run
   ```
   Change some rows and watch the log:
   ```
   dry run: would invalidate tags [user:42 users:list] and delete keys []
   ```
4. Run it for real (`-v` logs every invalidation):
   ```bash
   PG_PASSWORD=... cache-boy-sync -config cache-boy-sync.yaml
   ```
   On first start it creates the publication and the replication slot:
   ```
   created publication "cache_boy" for "public"."orders", "public"."users"
   created replication slot "cache_boy"; changes from now on are tracked
   streaming changes from slot "cache_boy" (publication "cache_boy", 2 rules)
   ```

## Rules

```yaml
rules:
  - table: orders             # "name" (schema public) or "schema.name"
    tags: ["user:{user_id}", "order:{id}", "orders:recent"]
    keys: ["order:{id}:summary"]
    on: [insert, update, delete, truncate]   # optional; default all four
```

- **tags** are invalidated with `TAG.INVALIDATE`, which deletes every key carrying that tag.
- **keys** are deleted directly with `DEL`. Use them for single keys you don't tag.
- **`{column}`** is replaced with the changed row's value of that column. A tag can use several: `"{tenant_id}:user:{id}"`. If a value is `NULL` or unknown, that one tag is skipped.
- **Static tags** (no placeholders) are invalidated on every change to the table. Use them for lists, counts and search results: `"users:list"`.
- **Updates count twice.** Both the old and the new row produce tags. When an order moves from user 1 to user 2, both `user:1` and `user:2` are invalidated. (This needs the old row; see the next section.)
- Several rules may name the same table.

### REPLICA IDENTITY: tags from non-key columns

On `UPDATE` and `DELETE`, Postgres only sends the old row's primary key by default. A tag built from another column, such as `user:{user_id}` on `orders`, can then be computed for the new row of an update but not for a deleted row or for the old row of an update. Tell Postgres to send the whole old row:

```sql
ALTER TABLE orders REPLICA IDENTITY FULL;
```

`cache-boy-sync` checks this at startup and logs a warning with this exact command for each table that needs it. Tags that only use primary-key columns (`user:{id}` on `users`) work with the default.

### TRUNCATE

`TRUNCATE` carries no rows, so only static tags and keys of the table's rules are invalidated, and a warning is logged. If you truncate tables that have per-row caches, give them a static tag too (`"orders:all"`) and add it to every cached entry, or flush the cache yourself.

## Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `postgres.url` | (required) | `postgres://user:pass@host:port/db?sslmode=...` or a `key=value` connection string. `replication=database` is added automatically |
| `postgres.slot` | `cache_boy` | Replication slot name. Use a different one per `cache-boy-sync` instance |
| `postgres.publication` | `cache_boy` | Publication name. Created at startup if missing; tables from new rules are added to it |
| `cache.addr` | `127.0.0.1:6380` | cache-boy address |
| `cache.username` / `cache.password` | none | cache-boy credentials. The ACL user needs `TAG.INVALIDATE` and `DEL` |
| `cache.namespace` | none | Run the `DEL`s in this namespace |
| `rules[].table` | (required) | Table to watch |
| `rules[].tags` / `rules[].keys` | at least one | Templates as described above |
| `rules[].on` | all | Subset of `insert`, `update`, `delete`, `truncate` |

`${NAME}` anywhere in the file is replaced with the environment variable `NAME`, so secrets can stay out of the file.

Command-line flags:

| Flag | Meaning |
|---|---|
| `-config file` | Config file (default `cache-boy-sync.yaml`) |
| `-v` | Log every transaction and what it invalidated |
| `-dry-run` | Log what would be invalidated; don't touch the cache |
| `-uninstall` | Drop the replication slot and publication, then exit |
| `-version` | Print the version |

## Running in production

- **Run one instance per database.** Two instances on the same slot can't both stream (Postgres allows one reader per slot). If you want a standby, use a supervisor that restarts it, rather than two copies.
- **Deploy it like any other service:** a systemd unit, a container next to your app, or a Kubernetes Deployment with one replica. It needs little memory and CPU.
- **Monitor the slot.** While `cache-boy-sync` is stopped, Postgres keeps WAL for the slot and disk usage grows. Watch the lag:
  ```sql
  SELECT slot_name, active, pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn) AS behind
  FROM pg_replication_slots;
  ```
  On Postgres 13+, `max_slot_wal_keep_size` caps how much WAL a slot may hold.
- **Uninstall properly.** When you stop using it for good, run `cache-boy-sync -config ... -uninstall`. A forgotten slot keeps WAL forever and can fill the disk.
- **If you can't grant table ownership**, have a table owner create the publication once, and `cache-boy-sync` will use it as is:
  ```sql
  CREATE PUBLICATION cache_boy FOR TABLE users, orders;
  ```
- **Keep TTLs.** Automatic invalidation removes stale data within milliseconds of a commit, but a TTL is still a good safety net.

## Limitations

- PostgreSQL only for now. MySQL (binlog) support is planned.
- Large transactions are invalidated after they commit, as one batch.
- After an outage, invalidations arrive when `cache-boy-sync` catches up; until then, cached entries can be stale. Short TTLs bound how stale.
- Changes made before the slot was created are not replayed. Start with an empty or freshly flushed cache, or accept that existing entries live until their TTL.
