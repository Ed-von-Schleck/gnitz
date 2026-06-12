# Value-partitioned covering secondary index

## 1. Summary

A secondary index today is (1) **partitioned by source PK** — the index is a
child table of the base table, populated locally during the base-table integrate,
so a given indexed value's entries scatter across every worker and a lookup-by-value
must **broadcast-and-merge** — and (2) **non-covering** — each entry is
`[promoted key | src_pk]` with zero payload, so a lookup must then **heap-fetch**
the row by `src_pk`.

This plan adds an opt-in index mode, **`COVERING`**, that is both:

- **Value-partitioned**: every entry for an indexed value lives on the single
  worker owning `hash(value)`, so a point lookup **unicasts** (no broadcast) and a
  unique check has a single deterministic holder; and
- **Covering**: each entry carries the full source-row payload, so a lookup
  returns the row directly (no heap fetch).

These two properties together turn the index into a value-keyed, covering store:
read-by-value is one unicast with no second fetch, a unique check is one unicast,
and an equi-join on the indexed columns can probe the index directly instead of
building a private join trace.

## 2. Scope and non-goals

**In scope**
- A `COVERING` index flag (persisted) that makes an index value-partitioned and
  covering. It applies to unique and non-unique indexes.
- Value-partitioned maintenance: the projected covering index batch is exchanged
  by its leading-value bytes and integrated into the value-owning worker's shard.
- Unicast read-by-value returning covering rows (no broadcast, no heap fetch).
- Unicast unique enforcement for `COVERING UNIQUE` indexes (single deterministic
  holder).
- An equi-join access path: a join whose key equals a `COVERING` index's columns
  binds its trace port to the index table instead of building a private trace.
- Backfill and recovery for value-partitioned covering indexes.

**Non-goals**
- **Range / ordered scans** over the index. Value-partitioning hashes the OPK
  leading bytes (equality-correct at any sign); ordered access across workers is
  not provided here.
- **Changing the default (PK-partitioned, non-covering) index.** `COVERING` is
  opt-in; absent the flag, indexes behave exactly as today.
- **Base-table distribution.** The base table stays PK-partitioned; only the index
  is value-partitioned.
- **Composite-FK / FK use** of the covering index.

## 3. Current architecture (anchors)

- **Index entry / circuit**: `IndexCircuitEntry { col_indices: PkColList,
  index_id, index_table: UnsafeCell<Box<Table>>, index_schema, is_unique }`
  (`crates/gnitz-engine/src/dag.rs:242-256`), on
  `TableEntry.index_circuits` (`dag.rs:316`). Schema = `[promoted key cols…,
  src_pk cols…]`, **all PK, zero payload**, built by `make_index_schema`
  (`catalog/utils.rs:162-193`); arity/stride capped by `index_key_types`
  (`gnitz-wire/src/types.rs:199-221`; `MAX_PK_COLUMNS = 5`, `MAX_PK_BYTES = 80`,
  `PK_LIST_MAX_COLS = 4`).
- **Creation**: `hook_index_register` (`catalog/hooks.rs:393-499`) —
  `make_index_schema` (`:450`), `Table::new(… Ephemeral)` (`:462-465`),
  `backfill_index` (`:470`), `dag.add_index_circuit` (`:472`). The `IDX_TAB` row
  packs `col_indices` (`ddl.rs:300`).
- **Population (write path)**: `DagEngine::ingest_store_and_indices`
  (`dag.rs:947-961`) projects each circuit's batch via `batch_project_index`
  (`dag.rs:1850-1904`) and ingests it **locally** into `ic.table_mut()` — no
  exchange. `batch_project_index` packs `[OPK(value) | src_pk]` per row; the
  src_pk suffix is copied verbatim from `src.get_pk_bytes(row)` (`:1889-1890`);
  NULL in any indexed column skips the row (`:1885`); the entry has **no payload
  region** (`:1893-1897`, zero null-bmp only).
- **Read-by-value**: `seek_by_index(table_id, col_indices, natives)`
  (`catalog/store.rs:921-973`) OPK-encodes the prefix via
  `index_opk_prefix_composite` (`schema.rs:896-911`), prefix-scans the index
  table, extracts the `src_pk` suffix (`:963-967`), and **heap-fetches** via
  `seek_family_bytes` (`:968`, `:843-869`). The master fans out: unique ⇒
  `fan_out_seek_by_index_async` (unicast, `master.rs:879-933`); else
  `fan_out_seek_by_index_collect_async` (broadcast-and-merge,
  `master.rs:935+`). Routing cache `PartitionRouter.index_routing:
  FxHashMap<(u32,u32,u128),u32>` (`exchange.rs:80-91`) is populated by
  `record_index_routing` for **single-column unique circuits only**
  (`master.rs:800-831`). Planner index pick:
  `collect_index_seek_candidates` (`gnitz-sql/src/dml.rs:871-941`).
- **Repartition primitive (reusable)**: the join reindex scatter —
  `ReindexPacker` (`ops/linear.rs:124-145`) + `op_repartition_batches_mode` with
  `RouteMode::JoinPromote` (`exchange.rs:416-437`) + `partition_for_pk_bytes` —
  routes a row to the worker owning its packed leading-key bytes. The projected
  index batch already leads with `OPK(value)`, so this routes a value's entries to
  one worker with no new key logic. Today this primitive fires only in the **view
  relay path** (`master.rs:667-698`), not at base-table integrate time.
- **Join operator**: `op_join_delta_trace(delta, cursor: &mut ReadCursor,
  left_schema, right_schema)` (`ops/join.rs:226-255`) merge-walks `delta` against
  a `ReadCursor` over the trace. A `ReadCursor` is produced by `Table::open_cursor`
  (`storage/table.rs:665`); an index circuit **is** a `Table`, so it can present
  the same cursor shape.
- **Uniqueness machinery**: master broadcast-skip `UniqueFilter`
  (`master.rs:140-164`), create-time preflight (`worker.rs:1209-1219`, master
  k-way merge `master.rs:2762-2777`), insert-time distributed check
  (`worker.rs:1280-1307`), in-batch validator (`validation.rs:143-261`). All keyed
  on the OPK leading-key span (`PkBuf`), all assuming a value's entries are
  scattered (hence the broadcast).
- **Recovery**: index tables are ephemeral and **rebuilt** at boot —
  `replay_catalog` re-fires `hook_index_register` → `backfill_index`
  (`hooks.rs:470`), which projects from the durable base table. Backfill streams
  chunks via `stream_index_projection` (`ddl.rs:432-454`) and dedups in the live
  phase (`ddl.rs:447`).

## 4. Design

### 4.1 The `COVERING` index schema

A covering index schema is `[promoted key cols (PK)…, src_pk cols (PK)…, source
payload cols (payload)…]`: the existing key+src_pk PK region (unchanged), plus the
source's payload columns appended as a **payload region**.

- `make_index_schema_covering(source_cols, source)` extends `make_index_schema`
  (`utils.rs:162-193`) by appending every source payload column as a non-PK
  `SchemaColumn`. The PK region (and therefore the key arity/stride caps,
  `index_key_types`) is unchanged; the payload columns do not consume PK budget.
- The leading PK key is still `OPK(value)`, so value-routing and prefix seeks work
  identically to the non-covering index.

### 4.2 Value-partitioned maintenance

A `COVERING` index's projected batch must land on the worker owning
`hash(value)`, not the worker owning `hash(src_pk)`. The projection is extended to
covering (copy payload too) and then **exchanged by its leading-value bytes**
before ingest:

- `batch_project_index_covering` mirrors `batch_project_index` (`dag.rs:1850-1904`)
  but copies each row's payload slots into the entry's payload region instead of
  writing only a zero null-bmp.
- In `ingest_store_and_indices` (`dag.rs:947-961`), for a `COVERING` circuit, the
  projected batch is routed by `partition_for_pk_bytes` of its leading
  `idx_key_size` bytes through the `JoinPromote` scatter primitive
  (`op_repartition_batches_mode`, `exchange.rs:416-437`) and the per-worker
  sub-batches are exchanged to their owning workers, which ingest into their shard
  of the index table. This is a new invocation of an existing primitive at
  base-table integrate time.

Because the leading key is `OPK(value)`, all entries for a value (and all
retractions of those entries) route identically — a value's entries and its
covering payload co-locate on one worker.

### 4.3 Unicast read-by-value

For a `COVERING` index the holder worker is **deterministic**:
`worker_for_partition(partition_for_pk_bytes(OPK(value)), num_workers)`. The read
path therefore:

- Unicasts the seek to that one worker — for unique **and** non-unique covering
  indexes (a non-unique covering seek returns all matching rows from the one
  worker; no cross-worker merge).
- Returns the entry's **covering payload** directly; `seek_family_bytes`
  (`store.rs:843-869`) is **not** called for a covering index.
- Needs **no `PartitionRouter` cache** (`exchange.rs:80-91`): the holder is a pure
  function of the value, so `record_index_routing` is skipped for covering indexes.

`handle_seek_by_index` (`runtime/executor.rs:860-887`) branches on the covering
flag: covering ⇒ compute holder + unicast + return rows; non-covering ⇒ the
existing unique-unicast / non-unique-broadcast paths unchanged.

### 4.4 Unicast unique enforcement

A `COVERING UNIQUE` index's sole holder for a value lives on the one worker owning
`hash(value)`. The distributed uniqueness machinery (§3) collapses for this case:

- The insert-time check unicasts the candidate to the value's owning worker, which
  checks its local shard — replacing the broadcast check
  (`worker.rs:1280-1307` invoked per all workers).
- The create-time preflight scatters by value (the backfill already does, §4.6),
  so each value's duplicates land together and the per-worker scan detects them
  locally — no k-way cross-worker merge for the duplicate verdict.
- The master `UniqueFilter` broadcast-skip is unnecessary for covering uniques and
  is bypassed (the unicast check is already one round-trip).

### 4.5 Index-backed equi-join

A join whose key columns equal a `COVERING` index's `col_indices` binds the join's
trace port to the index table instead of building a private `integrate_trace`:

- The index table leads its PK with `OPK(value)` and carries the covering payload,
  so its `ReadCursor` (`Table::open_cursor`, `table.rs:665`) presents exactly the
  shape `op_join_delta_trace` expects (`ops/join.rs:226-255`): `current_pk_bytes()`
  leads with the join key; the payload is readable from the cursor.
- The planner, when a `COVERING` index exists on a join side's key, emits that
  side's trace port as a read of the index table (the existing
  `ScanTrace(table_id)` external-trace binding, `compiler.rs:1109-1149`) and omits
  the per-view `integrate_trace` for that side.
- This requires the join's other side to be co-partitioned by the same value —
  the join's delta side is reindexed and exchanged by the join key, matching the
  index's value-partitioning, so delta and index shard co-partition per worker.

### 4.6 Backfill and recovery

`backfill_index` / `stream_index_projection` (`ddl.rs:432-454`) currently project
**locally** per worker. For a `COVERING` index the streamed projected chunks must
be **scattered by value** (the §4.2 exchange) so each worker rebuilds only its
value-shard, not a PK-partitioned copy. The live-phase duplicate check
(`ddl.rs:447`) for a `COVERING UNIQUE` index runs per value-shard (duplicates of a
value land together), so it detects cross-chunk duplicates without a global merge.
At boot, the rebuild uses the identical value-scatter path, so the recovered index
is value-partitioned exactly as steady-state.

## 5. Data-model / persistence changes

- **`IDX_TAB`** gains a `COVERING` flag bit alongside the unique flag (the index
  sys-table row that `create_index` writes, `ddl.rs:254-338`). `hook_index_register`
  decodes it and selects `make_index_schema_covering` + value-partitioned storage.
- **`IndexCircuitEntry`** gains a `covering: bool` field (`dag.rs:242-256`), read
  by the maintenance (§4.2), read (§4.3), uniqueness (§4.4), and join-binding
  (§4.5) paths.
- **DDL**: `CREATE [UNIQUE] INDEX name ON t (cols) COVERING` — the `COVERING`
  keyword sets the flag. `execute_create_index` (`planner.rs`) plumbs it to
  `client.create_index`.
- **Index schema now has payload**: `index_schema.num_payload_cols() > 0` for
  covering indexes; the index `Table`'s flush/validate paths (`dag.rs:975-999`)
  and `ZSetBatch` wide-PK decode (`pk_count >= 2`,
  `gnitz-core/src/protocol/wal_block.rs:403-419`) now apply to index entries that
  flow over the wire to a SQL client.

## 6. Implementation (sites)

| Concern | Site | Change |
|---|---|---|
| Covering schema | `catalog/utils.rs:162-193` | `make_index_schema_covering`: append source payload cols as payload |
| Covering projection | `dag.rs:1850-1904` | `batch_project_index_covering`: copy payload region |
| Value-partitioned maintenance | `dag.rs:947-961` | covering ⇒ scatter projected batch by leading-value bytes (`JoinPromote`) + per-worker ingest |
| Circuit entry flag | `dag.rs:242-256` | add `covering: bool` |
| Read path | `runtime/executor.rs:860-887`, `catalog/store.rs:921-973` | covering ⇒ unicast to holder, return covering rows, skip `seek_family_bytes` |
| Holder function | `master.rs` (near `fan_out_seek_by_index_async`) | `worker_for_partition(partition_for_pk_bytes(OPK(value)))`; no cache |
| Unique check | `worker.rs:1280-1307`, `master.rs:140-164, 2762-2777` | covering unique ⇒ unicast check; bypass broadcast filter |
| Join binding | `gnitz-sql/src/planner.rs` (join builder), `compiler.rs:1109-1149` | covering index on join key ⇒ bind trace port via `ScanTrace(index_table_id)`, omit `integrate_trace` |
| Backfill scatter | `catalog/ddl.rs:432-454` | covering ⇒ scatter projected chunks by value |
| DDL + persist | `planner.rs` (`execute_create_index`), `catalog/ddl.rs:254-338`, `hooks.rs:393-499` | `COVERING` keyword; flag bit; covering schema + value storage |
| Sys-table flag | the `IDX_TAB` schema | add `COVERING` flag bit |

## 7. Recovery

Covering indexes are ephemeral and rebuilt at boot like every index. The rebuild
projects from the durable base table and **scatters by value** (§4.6), producing a
value-partitioned covering index identical to steady-state. No index state is read
from disk; the base table is the sole source of truth. The covering payload is
re-derived from the base row during projection, so a stale covering payload cannot
survive a reboot.

## 8. Limits

- The key arity/stride caps (`index_key_types`: `MAX_PK_COLUMNS = 5`,
  `MAX_PK_BYTES = 80`, `PK_LIST_MAX_COLS = 4`) are unchanged; the covering payload
  is unconstrained by them (it is payload, not PK).
- No range/ordered access (§2). Value-partitioning hashes OPK leading bytes;
  equality is correct at any sign, ordering is not provided.
- A covering index duplicates the source row payload (one copy per indexed value
  occurrence), so it costs roughly a full extra copy of the indexed-and-covered
  columns — the same order of cost a join trace pays. This is the user's opt-in
  trade for unicast covering reads and trace-free joins.

## 9. Testing

- **Schema/projection**: a covering projection reproduces the leading
  `[OPK(value) | src_pk]` of the non-covering projection and additionally carries
  the correct payload; NULL-in-any-indexed-column still skips the row.
- **Value-partitioning**: all entries for a given value (and their retractions)
  land on the one worker owning `hash(value)`; two distinct values whose low 16
  OPK bytes collide but whose full spans differ route independently.
- **Unicast read**: a covering point lookup hits exactly one worker (assert no
  broadcast) and returns the row with no `seek_family_bytes` call; a non-unique
  covering lookup returns all matches from the one worker.
- **Unique**: a `COVERING UNIQUE` duplicate inserted on any worker is rejected via
  a single unicast check; create-time preflight rejects a pre-existing duplicate
  via the value-scatter.
- **Index-backed join**: a join on a covering index's columns produces results
  byte-identical to the private-trace baseline and builds **no** `integrate_trace`
  for that side (assert the trace table is absent).
- **Backfill/recovery**: cross-chunk duplicate detection for a covering unique
  index; reboot rebuilds a value-partitioned covering index with correct contents
  and covering payload.
- **Default unaffected**: a non-`COVERING` index behaves byte-identically to today
  (PK-partitioned, non-covering, broadcast-and-merge read, heap fetch).

## 10. Invariants preserved

- **OPK-at-rest**: the index PK region remains order-preserving big-endian; the
  projected entry, the value-route key, and the seek prefix encode identically
  (`schema.rs:871-876`, `dag.rs:1879-1888`).
- **NULL-distinctness**: a row NULL in any indexed column is not indexed and never
  collides — unchanged.
- **Filter is never the source of truth**: covering uniqueness relies on the
  unicast per-value check, not the broadcast filter; the filter is bypassed, not
  trusted.
- **Index rebuilds from the durable base table**; covering payload is re-derived,
  never persisted as the authority.
- **Co-partition requirement** for the index-backed join is the same equality the
  exchange-skip path already enforces: the join's delta side is reindexed and
  exchanged by the join key to match the index's value shard per worker.
