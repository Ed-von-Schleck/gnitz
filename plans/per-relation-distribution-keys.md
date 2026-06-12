# Per-relation distribution keys

## 1. Summary

Every table is hash-partitioned by its primary key: routing funnels through one
function, `partition_for_pk_bytes`, fed by `schema.pk_indices()` in schema order,
at every layer (DML scatter, SEEK, exchange relay, storage ingest, SAL replay).
That one hardwired choice forces a full repartition (exchange) on every join,
because no two tables are co-partitioned unless their join key *is* both PKs.

This plan makes data distribution a **per-relation choice**:

- **`DISTRIBUTE BY (cols)`** — hash-partition a base table by a chosen column
  list (which must be a subset of its PK columns; see §4.1) instead of the full
  PK.
- **`REPLICATED`** — store a full copy of a (small) table on every worker.
- **Co-partitioned local joins** — when both join sides are already distributed
  by the join key, the existing exchange-skip path fires and the join runs
  locally with no network shuffle. A join against a replicated table is always
  local.

## 2. Scope and non-goals

**In scope**
- Persisting and threading a per-table distribution key (column subset of the PK)
  through routing, SEEK, storage ingest, and SAL replay.
- Replicated tables: broadcast writes, per-worker full copy, local-join treatment.
- Widening the co-partition detector from "shard key == PK" to "shard key ==
  table's distribution key", and emitting local joins when both sides match.
- The DDL surface (`DISTRIBUTE BY` / `REPLICATED`) and its validation.

**Non-goals**
- **Range partitioning.** Ordered/range distribution (split points, rebalancing,
  range-aware routing, ordered cross-worker merge) is a separable and far larger
  effort. This plan is hash + replicated only. Range predicates and ordered output
  remain broadcast-and-gather.
- **Re-distributing an existing table.** Distribution is fixed at `CREATE TABLE`;
  there is no `ALTER … DISTRIBUTE BY`. (Pre-production: no migration concern.)
- **Distributing views or secondary indexes** by a chosen key — base tables only.
- **Changing the 256-bucket / contiguous-worker-range model.** Hash distribution
  still uses 256 buckets and `worker_for_partition`; only the *bytes hashed* and
  the *columns chosen* change. Replicated bypasses the bucket model entirely.

## 3. Current architecture (anchors)

- **Storage**: `PartitionedTable` (`crates/gnitz-engine/src/storage/partitioned_table.rs:29-91`)
  holds one child `Table` per live partition; `num_partitions ∈ {1, 256}` (hard
  assert `:53-56`); a worker owns the contiguous range `[part_start, part_end)`,
  `part_offset = part_start` (`:87`). Ingest routes each row by
  `partition_for_pk_bytes(mb.get_pk_bytes(i))` (`:138-140`); `mix` takes the top 8
  bits → 0..=255 (`:435-442`). The struct knows only `schema`, not any key choice.
- **Partition → worker**: `worker_for_partition(partition, num_workers)`
  (`ops/exchange.rs:73-77`); `partition_range(worker_id, num_workers)`
  (`runtime/bootstrap.rs:26-35`).
- **DML scatter**: `MasterDispatcher::write_commit_group` (`runtime/master.rs:2335-2356`)
  routes by `schema.pk_indices()` via `with_worker_indices` →
  `fill_worker_indices` (`ops/exchange.rs:261-294`). The router gate is
  `is_pk_routing = col_indices == schema.pk_indices()` (**strict sequence
  equality**, `:281`); true ⇒ `partition_for_pk_bytes`, else
  `hash_row_for_partition(.., RouteMode::GroupKey)`.
- **SEEK routing**: `master.rs:790-811` —
  `worker_for_partition(partition_for_pk_bytes(opk[..stride]), num_workers)`.
- **Repartition relay**: `op_repartition_batches_mode` /
  `op_relay_scatter_consolidated_mode` (`exchange.rs:416-437, 547-582`) — three
  route modes: PK bytes, `JoinPromote` (compound/promoted key via `ReindexPacker`),
  else `GroupKey`. Only `JoinPromote` packs a compound OPK key that co-partitions
  byte-for-byte with `partition_for_pk_bytes`.
- **Co-partition detection**: `compute_co_partitioned`
  (`compiler.rs:539-564`) marks a source exchange-skippable iff (a) no slot
  carries a promotion (`tc != 0` ⇒ exchange, `:549-551`) and (b)
  `ext.schema.shard_cols_match_pk(&col_indices)` — the shard key equals the PK
  **exactly, in schema order** (`schema.rs:455-458`). Consumed by
  `get_exchange_info` → `ExchangeInfo { is_trivial, is_co_partitioned }`
  (`dag.rs:356-359, 690-738`); the skip is `skip_exchange = is_trivial &&
  is_co_partitioned` (`dag.rs:1188-1192`).
- **DDL**: `client.create_table` (`gnitz-core/src/client.rs:401-440`) packs
  `pack_pk_cols(pk_cols)` and writes a `TABLE_TAB` row;
  `execute_create_table` (`planner.rs:474`). `hook_table_register`
  (`catalog/hooks.rs:140-202`) decodes `pk`/`flags` and builds storage via
  `build_partitioned_storage` (always `NUM_PARTITIONS = 256`). `TABLE_TAB` schema
  = `[u64 id(PK), u64, str name, str dir, u64 pk_packed, u64 created_lsn, u64
  flags]` (`sys_tables.rs:184-186`); only flag bit today is
  `TABLETAB_FLAG_UNIQUE_PK = 1` (`sys_tables.rs:43`).
- **Recovery**: per-worker SAL replay (`bootstrap.rs:160-166`) replays the
  pre-scattered `FLAG_PUSH` groups; `raw_store_ingest` → `ingest_owned_batch`
  **re-hashes by PK bytes** (`store.rs:1037-1041`, `partitioned_table.rs:138-140`).
- **System tables**: `num_partitions = 1`, master-only, durable
  (`RelationKind::SystemCatalog`, `dag.rs:248-250`); held as
  `StoreHandle::Borrowed` (`store.rs:1075-1083`); DDL via `FLAG_DDL_SYNC`.
- **Uniqueness / FK co-location**: unique-PK enforcement is per-partition
  (`enforce_unique_pk_partitioned`, `store.rs:1043-1051`) — a row and its
  retraction must co-locate, which PK routing guarantees today. FK existence
  checks scatter when the parent key is the lone PK, else **broadcast**
  (`master.rs:1538-1554`).

## 4. Design

### 4.1 The distribution key must be a subset of the PK

A row and its retraction (identical PK) must land on the same worker, or
per-partition PK-uniqueness enforcement (`enforce_unique_pk_partitioned`) and
retraction matching break. This is guaranteed iff the distribution columns are a
**subset of the PK columns**: two rows with an identical full PK necessarily
agree on every PK column, hence on the distribution subset, hence hash to the
same partition.

**Decision**: `DISTRIBUTE BY (cols)` requires `cols ⊆ pk_indices` (each named
column is a PK column; order is the declared distribution order). This is the
standard distributed-relational rule (a unique key must contain the distribution
key). The default (no clause) is `cols = pk_indices` — exactly today's behavior.

Consequence for the headline use case: to co-locate `orders` with `customers` on
`customer_id`, `customer_id` must be part of `orders`' PK (e.g.
`PRIMARY KEY (customer_id, order_id)`). That composite-PK requirement is the
deliberate, well-trodden cost of distribution-by-non-PK-leading-key.

### 4.2 Routing by the distribution key

Routing changes from "PK columns" to "the table's distribution columns" at every
site, packed as a **compound OPK key via the `ReindexPacker` / `JoinPromote`
path**, not the `GroupKey` path (only the former co-partitions byte-for-byte with
`partition_for_pk_bytes`):

- `write_commit_group` (`master.rs:2335-2356`) passes the table's `dist_cols`
  instead of `schema.pk_indices()`.
- `fill_worker_indices` (`exchange.rs:261-294`): the `is_pk_routing` fast path
  stays for `dist_cols == pk_indices` (the common default — a single
  `partition_for_pk_bytes` loop); otherwise route by packing `dist_cols` through
  the `JoinPromote` packer and hashing the packed leading bytes (replacing the
  `GroupKey` fallback, which does not co-partition).
- SEEK routing (`master.rs:790-811`): pack the seek's `dist_cols` values into OPK
  bytes and route by `partition_for_pk_bytes` of that key. A SEEK that constrains
  only a subset of `dist_cols` (cannot identify one worker) broadcasts.
- `PartitionedTable` carries `dist_cols` and routes `ingest_owned_batch`
  (`:138-140`) by the distribution-key bytes, not `get_pk_bytes`. Since
  `dist_cols ⊆ pk_indices`, the bytes are extracted from the row's PK region
  (no payload read).

### 4.3 Replicated tables

A replicated table has a full copy on every worker:

- Storage: a single-partition `Table` (`num_partitions = 1`, reusing the
  `partitioned_table.rs:114-116, 172-174, 197-199` single-partition branches) on
  **every** worker — not the master-only `SystemCatalog` shape.
- Writes **broadcast** to all workers (every worker ingests the full row set),
  reusing the all-workers fan-out convention (`unicast_worker = -1` /
  `CheckPayload::Broadcast`, `master.rs:48-52`). No PK-hash scatter.
- Uniqueness is enforced **locally** on each worker's full copy (every worker sees
  every row), so no distributed preflight/broadcast check is needed for a
  replicated table's unique constraints.
- Recovery replays the broadcast `FLAG_PUSH` groups on every worker (each worker's
  SAL holds the full row set).

### 4.4 Co-partitioned local joins

Widen the co-partition detector to read the table's distribution key:

- `shard_cols_match_pk` → `shard_cols_match_dist_key`, comparing the reindex shard
  key against the table's declared `dist_cols` (ordered) instead of `pk_indices`.
- The `tc != 0` promotion guard stays (a promoted slot has a different width than
  the native partition key, so it still requires the exchange).
- A **join** is local when **both** sides' reindex keys equal their respective
  `dist_cols`; the per-source `compute_co_partitioned` already evaluates each
  source independently and both sources skipping yields a local join with no new
  node type (`dag.rs:1188-1192`).
- A join against a **replicated** side is always local: the replicated side is
  present in full on every worker, so the partitioned side is joined locally with
  no exchange on either side.

### 4.5 FK existence checks

FK checks route to the worker(s) that could hold the parent row. Generalize the
existing scatter-vs-broadcast choice (`master.rs:1538-1554`) to route by the
**parent table's distribution key**: if the child's referenced columns are a
superset of the parent's `dist_cols`, the check unicasts/scatters to the
parent's owning worker; otherwise it broadcasts. A replicated parent is always a
local check on the child's worker.

## 5. Data-model / persistence changes

- **`TABLE_TAB`** (`sys_tables.rs:184-186`) gains one column: a packed
  `dist_cols` U64 (`pack_pk_cols` form; for the default it equals the packed PK).
  The `REPLICATED` choice is a new flag bit alongside `TABLETAB_FLAG_UNIQUE_PK`
  (`sys_tables.rs:43`); when set, `dist_cols` is ignored. Both `client.create_table`
  (`client.rs:430-438`) and `hook_table_register` (`hooks.rs:169-202`) decode rows
  positionally, so the new column is appended in both, and a `validate_dist_cols`
  (modeled on `validate_pk_cols`, `sys_tables.rs:64-92`) enforces `dist_cols ⊆ pk`.
- **`TableEntry`** carries the decoded `dist_cols` (and a `replicated` bool) as a
  sibling field — `RelationKind` stays 1 byte (`dag.rs:281`); the distribution key
  is data, not a kind. Routing (§4.2) and co-partition (§4.4) read it from here.
- **`PartitionedTable`** gains a `dist_cols` field, set at construction from the
  decoded value.

## 6. Implementation (sites)

| Concern | Site | Change |
|---|---|---|
| DDL parse | `gnitz-sql` CREATE TABLE | parse `DISTRIBUTE BY (cols)` / `REPLICATED` table options |
| DDL persist | `client.rs:401-440`, `planner.rs:474` | pass + pack `dist_cols`/`replicated` |
| DDL apply + validate | `hooks.rs:140-202`, `sys_tables.rs` | decode; `validate_dist_cols` (⊆ PK); build replicated vs partitioned storage |
| Sys-table schema | `sys_tables.rs:184-186, 43` | add `dist_cols` column + `REPLICATED` flag bit |
| Storage routing | `partitioned_table.rs:29-91, 138-140` | carry `dist_cols`; route ingest by it |
| Replicated storage | `partitioned_table.rs` single-partition branches + new per-worker copy | full copy on every worker |
| DML scatter | `master.rs:2335-2356`, `exchange.rs:261-294` | route by `dist_cols` via `JoinPromote` packer; broadcast for replicated |
| SEEK routing | `master.rs:790-811` | route by `dist_cols` OPK; broadcast on partial key |
| Co-partition | `compiler.rs:539-564`, `schema.rs:455-458`, `dag.rs:690-738` | `shard_cols_match_dist_key`; replicated ⇒ always local |
| FK checks | `master.rs:1538-1554`, `validation.rs:15-45` | route by parent `dist_cols`; replicated ⇒ local |
| Recovery | `store.rs:1037-1041`, `bootstrap.rs:160-166` | `ingest`/replay route by `dist_cols`; replicated replays on all workers |

## 7. Recovery

- A partitioned table's re-hash on replay (`raw_store_ingest` →
  `ingest_owned_batch`) must use `dist_cols`, not `get_pk_bytes`. Because the SAL
  `FLAG_PUSH` groups were already scattered by `dist_cols` at write time, a worker
  only ever replays its own partitions; the re-hash on ingest must agree, which it
  does once `PartitionedTable` carries `dist_cols`.
- A replicated table replays its full broadcast row set on every worker.
- The min-flushed-LSN watermark (`partitioned_table.rs:333-358`) is unchanged.

## 8. Limits

- `dist_cols ⊆ pk_indices` (§4.1); a distribution key that is not a PK subset is
  rejected at DDL.
- Hash distribution still uses 256 buckets and contiguous worker ranges; a STRING
  distribution column is impossible (PK columns are never STRING, and `dist_cols ⊆
  pk`).
- Replicated tables are intended for small dimensions; every worker stores the
  full copy and every write broadcasts. No size cap is enforced; the cost is the
  user's to bound.

## 9. Testing

- **Routing**: a table `DISTRIBUTE BY (c)` with `c ⊆ pk` routes a row and its
  retraction to the same worker; a `DISTRIBUTE BY` that is a *permutation* of the
  PK still co-locates same-PK rows.
- **Validation**: `DISTRIBUTE BY` a non-PK column is rejected at DDL.
- **Local join**: `customers(id PK) DISTRIBUTE BY (id)` joined to
  `orders(customer_id, order_id PK) DISTRIBUTE BY (customer_id)` on
  `id = customer_id` skips the exchange on both sides (assert no relay scatter)
  and returns correct results; a non-aligned join still exchanges.
- **Replicated**: a replicated dimension joined to a partitioned fact runs locally
  on every worker; writes to the replicated table appear on all workers; reboot
  restores the full copy on every worker.
- **Uniqueness / FK**: PK uniqueness holds under `DISTRIBUTE BY` a PK subset;
  FK existence checks against a distributed parent route correctly; against a
  replicated parent run locally.
- **Default parity**: a table with no clause behaves byte-identically to today
  (routes by full PK; joins exchange unless PK-aligned).

## 10. Invariants preserved

- **Same-PK co-location** for uniqueness/retraction — guaranteed by `dist_cols ⊆
  pk` (§4.1).
- **OPK byte-equality of routing keys** — distribution keys are packed through the
  same `JoinPromote`/`ReindexPacker` OPK path as join keys, so a co-partitioned
  join's two sides agree byte-for-byte (`exchange.rs:33-60`,
  `partitioned_table.rs:449-457`).
- **256-bucket / contiguous-worker model** unchanged for partitioned tables;
  `worker_for_partition`/`partition_range` are key-agnostic and untouched.
- **Source of truth on durable base tables**; replay re-derives partition
  placement from `dist_cols` deterministically.
