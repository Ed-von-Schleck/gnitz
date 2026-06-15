# Per-relation distribution keys

## 1. Summary

Every table is hash-partitioned by its full primary key: routing funnels through
one function, `partition_for_pk_bytes`, fed by the full OPK region
(`schema.pk_indices()` in schema order) at every write-side layer (DML scatter,
SEEK, storage ingest, SAL replay). That one hardwired choice forces a full
repartition (exchange) on every join, because no two tables co-partition unless
their join key *is* both full PKs.

This plan makes the hash distribution key a **per-table choice**, constrained to
a **leading prefix of the PK**:

- **`CLUSTER BY <cols>`** — hash-partition a base table by the first *k* PK
  columns (the named columns must be exactly the PK's first *k*, in order; see
  §4.1) instead of all of them. The default (no clause) is *k = |PK|* — exactly
  today's behavior, byte-for-byte. `CLUSTER BY` (not `DISTRIBUTE BY`) is the
  surface syntax because it is the only distribution-style CREATE TABLE clause the
  parser already produces an AST node for (§6).
- **Co-partitioned local joins** — when both join sides are already distributed
  by the (common-width) join key, the existing exchange-skip path fires and the
  join runs locally with no network shuffle.

The whole feature reduces to threading one small integer *k* (the distribution
prefix length, materialized as a byte width `dist_stride`) into a small, **closed**
set of byte-slice sites, plus widening the co-partition detector:

1. the **write-side table-key routers** — storage ingest, the per-partition PK
   probe, DML scatter (`fill_worker_indices`), and SEEK;
2. the **co-partition detectors** (there are **two**, §3, §4.3), widened from
   "reindex/shard key == PK" to "reindex/shard key == the table's distribution
   prefix."

For a clustered table, the routers become a single leading slice of the PK
region — `partition_for_pk_bytes(&pk_bytes[..dist_stride])` — with no new packer,
gather, or hash path, and zero added allocation or copy (a slice is a
pointer/length adjustment; see §11). The join-relay scatter functions
(`op_repartition_batches_mode`, `relay_scatter_merge_walk`,
`op_relay_scatter_consolidated_mode`) route by the *join* key, not the table key,
and are **deliberately untouched** (§4.2); the discriminator is a function's role,
not whether it happens to call `get_pk_bytes`.

Replicated tables are a separate mechanism (full copy per worker, broadcast
writes, single-source reads) with their own recovery and gather concerns; they
are designed in `plans/replicated-tables.md`, not here (§2).

## 2. Scope and non-goals

**In scope**
- Persisting and threading a per-table distribution prefix length *k* through
  storage ingest, the PK probe, DML scatter, SEEK, and SAL replay.
- Widening **both** co-partition detectors (`compute_co_partitioned` for join
  sources; `compute_view_skips_exchange` for single-source `ExchangeShard` views)
  from "key == PK" to "key == table's distribution prefix," yielding local joins —
  and local GROUP BY/reduce — when the key matches.
- The DDL surface (`CLUSTER BY <cols>`) and its validation (cols == PK[..k]).

**Non-goals**
- **Replicated tables.** Broadcast-write / per-worker-full-copy / single-source-read
  distribution is a separate mechanism — see `plans/replicated-tables.md`. It
  shares the *goal* (local joins against small dimensions) but no *machinery*
  with hash distribution, and it has open problems (read gather, data-write
  broadcast path, per-worker full replay) that this plan does not touch.
- **Non-prefix distribution keys.** The distribution key is a leading PK prefix,
  not an arbitrary PK subset (§4.1). A user who wants to distribute by a non-leading
  PK column reorders the PK so that column leads. This keeps write-side routing a
  pure byte-slice and the persisted state a single integer.
- **Range partitioning.** Ordered/range distribution (split points, rebalancing,
  range-aware routing, ordered cross-worker merge) is a far larger, separable
  effort. Hash only. Range predicates and ordered output remain broadcast-and-gather.
- **Re-distributing an existing table.** Distribution is fixed at `CREATE TABLE`;
  no `ALTER … CLUSTER BY`. (Pre-production: no migration concern.)
- **Distributing views or secondary indexes** by a chosen key — base tables only.
  Views still call `build_partitioned_storage` and must pass the full-PK default
  (`k = |PK|`), §5.
- **Changing the 256-bucket / contiguous-worker-range model.** Only the *bytes
  hashed* change (a leading prefix of the OPK instead of the whole OPK);
  `worker_for_partition` / `partition_range` are untouched.

## 3. Current architecture (anchors)

Line numbers are anchors cross-checked against the tree at audit time (it drifts;
confirm by symbol).

- **Storage**: `PartitionedTable` struct (`storage/partitioned_table.rs:29-35`)
  holds one child `Table` per live partition (`tables: Vec<Table>`);
  `num_partitions ∈ {1, 256}` (hard `assert!` at `:53-56`, fires in release); a
  worker owns the contiguous range `[part_start, part_end)`, with
  `part_offset = part_start` (`:87`) and the range end implicit in `tables.len()`
  (there is no `part_end` field). The struct knows `schema` but no key choice.
  Ingest routes each row by `partition_for_pk_bytes(mb.get_pk_bytes(i))`
  (`:138-140`). The **same struct also probes a single partition** for PK
  existence/retraction via `local_index_bytes(key)` (`:410-418`) →
  `partition_for_pk_bytes(key)`, used by `has_pk_bytes`/`retract_pk_bytes`
  (`:240-272`). This probe hashes the **full** key today; it is the site §4.4's bug
  lives at. The in-partition match itself is on the full `(PK, payload)` identity,
  not the partition-selection key (regression-tested at `table.rs:1523-1563`:
  prefix-twins `(1,1,100)`/`(1,1,200)` coexist and an absent twin is not found).
- **Hash**: `partition_for_pk_bytes(bytes)` (`partitioned_table.rs:457-464`) — for
  `len ≤ 16`, `mix(widen_pk_be(bytes, len))`; else `xxh::checksum(bytes) >> 56`.
  `mix` (`:435-442`) takes the top 8 bits → 0..=255. `widen_pk_be`
  (`gnitz-wire/src/pk.rs:123-129`, re-exported as `gnitz_wire::widen_pk_be`)
  right-aligns OPK bytes into a u128 and **left-zero-pads** (`buf[16 - stride..]`),
  so the **same logical value at any width ≤ 16 hashes to the same partition**.
  Zero-extension is correct: signedness is already baked into the OPK bytes
  (signed columns are encoded big-endian sign-flipped), so the OPK is monotone in
  the unsigned range. It is always called with the full PK region today.
- **OPK encoding**: `encode_order_preserving_pk` (`storage/columnar.rs:288-303`)
  walks `pk_columns()` in PK-list order, encoding each column big-endian at a
  running offset that advances by exactly `col.size()` — **tight, no length
  prefixes, no reordering, no inter-column padding**. So the leading `dist_stride`
  bytes of a row's PK region *are* the OPK of its first *k* PK columns. This is the
  load-bearing fact that makes write-side routing a pure byte-slice.
- **Partition → worker**: `worker_for_partition(partition, num_workers)`
  (`ops/exchange.rs:74`, `#[inline]` at `:73`); `partition_range(worker_id,
  num_workers)` (`runtime/bootstrap.rs:27-36`). Both are pure arithmetic on
  partition index and worker count over the 256-partition space — key-agnostic.
- **DML scatter (write-side)**: `MasterDispatcher::write_commit_group`
  (`runtime/master.rs:2597-2617`) routes by `schema.pk_indices()` (`:2604`) via
  `with_worker_indices` (`exchange.rs:345`) → `fill_worker_indices`
  (`exchange.rs:306-338`). The gate is `is_pk_routing = (col_indices ==
  schema.pk_indices())` — **strict ordered slice equality** (`:325`); true ⇒
  `partition_for_pk_bytes(get_pk_bytes(i))` (`:328`), else a `RouteMode::GroupKey`
  hash-fold fallback (`:331-337`) used for non-PK routing (e.g. index maintenance).
  The only function that re-implements this gate, `op_repartition_batch`
  (`exchange.rs:380`, gate `:393`), is `#[cfg(test)]`-only — there is **no
  production single-source DML scatter** distinct from `fill_worker_indices`.
- **Join-relay (route by join key, not table key)**: `op_repartition_batches_mode`
  (`exchange.rs:414`, gate `:455`), `relay_scatter_merge_walk` (`:584`, gate
  `:634`), and `op_relay_scatter_consolidated_mode` (`:892`) gate `is_pk_routing =
  col_indices == pk_indices && !key_is_promoted(target_tcs)`; the promoted/compound
  case routes by a packed `_join_pk` (`ReindexPacker` via `compound_join_packer`,
  `:296-304`). `RouteMode` (`:254-257`) has exactly two variants — `GroupKey`,
  `JoinPromote`; the raw-PK path is the `is_pk_routing` gate, not a third variant.
  **These functions route by the join key and are unchanged by this plan**, even on
  their raw-`get_pk_bytes` fast-path: that path fires only when the join key *is*
  the full PK, where both sides already route at full-PK width (slicing would
  desync the delta from its reindexed trace).
- **SEEK routing**: `master.rs:953-967` (in `fan_out_seek_async`, `:942`) —
  `worker_for_partition(partition_for_pk_bytes(opk[..stride]), num_workers)`, on the
  assembled OPK for wide PKs (`:957`) and the encoded narrow key otherwise (`:964`),
  then a single unicast (`:966`). A `FLAG_SEEK` point-lookup always carries the
  **full** PK and always unicasts — there is no prefix-or-broadcast branch (prefix
  scans live on the secondary-index `FLAG_SEEK_BY_INDEX*` path, not here).
- **Co-partition detection — two sites**:
  - `compute_co_partitioned` (`compiler.rs:597-622`) marks a **join** source
    skippable iff (a) no slot carries a promotion (`tc != 0` ⇒ exchange, `:607`)
    and (b) `ext.schema.shard_cols_match_pk(&col_indices)` — the reindex key
    (resolved *through* Filter nodes via `reindex_cols_through_filters`) equals the
    PK **exactly, in order** (`schema.rs:455-458`: `cols.len()==pk.len() &&
    all(c==p)`). It reads each source's `SchemaDescriptor` off `ExternalTable`
    (built in `compile_view_internal`, carries `{table_id, schema}`).
  - `compute_view_skips_exchange` (`dag.rs:757-789`, memoized via
    `view_skips_exchange` `:748-755`, cache `skip_exchange_cache` `:403`) governs
    the exchange-skip for **every single-source `ExchangeShard` view** (joins'
    output shard, GROUP BY/reduce, DISTINCT, set-op sides). It finds the view's
    output `ExchangeShard` (`:761-765`), requires its sole input be a **bare scan**
    (exactly one incoming edge from a node with none, `:767-776` — it does *not*
    walk through Filters), and returns `entry.schema.shard_cols_match_pk(&shard_cols)`
    (`:788`), reading the schema from `self.tables[&tid].schema`. The skip is one
    bit: `dag.rs:1342` `if self.view_skips_exchange(view_id) { pre_result } else {
    do_exchange(...) }`. (`ExchangeInfo { is_trivial, is_co_partitioned }` was
    collapsed into this single bool; the struct no longer exists.)
- **Reindex vs exchange (load-bearing)**: `map_reindex` is a *separate* circuit
  node (`OpNode::Map(MapKind::Expression { reindex_cols })`, built at
  `planner.rs:1155` (`reindex_b`) and `:1188` (`reindex_a`)) upstream of the join.
  It is compiled into the pre-phase and **always executes locally**;
  `view_skips_exchange`/`copart_join` elides only the network shuffle
  (`dag.rs:1330-1336,1342-1346,1353-1355`), never the PK-region relabel. So a
  co-partitioned join whose distribution prefix is a *proper* prefix of the PK
  still receives rows correctly re-keyed to the join key before `cogroup`
  (`ops/cogroup.rs:111-138`, memcmp on `get_pk_bytes` via `compare_pk_bytes`,
  `columnar.rs:146-148`). This is why §4.3 needs no new relabel.
- **DDL**: `client.create_table` (`gnitz-core/src/client.rs:486-532`) packs
  `pack_pk_cols(pk_cols)` (`:513`) and writes a `TABLE_TAB` row, writing `flags` as
  `unique_pk as u64` (`:528`). `execute_create_table`
  (`gnitz-sql/src/planner.rs:239`) consumes a `sqlparser::ast::CreateTable`.
  `hook_table_register` (`catalog/hooks.rs:164-243`) decodes `pk`
  (`unpack_pk_cols(read_batch_u64(batch,i,3))`, `:177`) and `flags`
  (`read_batch_u64(batch,i,5)`, `:178`; `is_unique = (flags & TABLETAB_FLAG_UNIQUE_PK)
  != 0` at `:179`) and builds storage via `build_partitioned_storage` (`:140-162`,
  always `NUM_PARTITIONS = 256`, `sys_tables.rs:30`). It fires on the master **and
  every worker** (workers receive DDL deltas and re-fire hooks via `ddl_sync`,
  `store.rs:1184-1210`), so storage is built identically everywhere. `TABLE_TAB`
  (engine `sys_tables.rs:184-186`; core `types.rs:39-53`) = PK `id:u64` + payload
  `[schema_id u64, name str, dir str, pk_packed u64, created_lsn u64, flags u64]`;
  both sites decode **positionally**. The only flag bit is `TABLETAB_FLAG_UNIQUE_PK
  = 1` (`sys_tables.rs:43`); bits 1..63 are free. `pack_pk_cols`
  (`gnitz-wire/src/catalog.rs:251-262`) packs ≤ `PK_LIST_MAX_COLS = 4` columns
  (`:154`), each index ≤127, into one u64 (bit 63 = packed flag; bits 32..62 free) —
  this is the **separate** `pk_packed` column, not `flags`. `validate_pk_cols`
  (engine `sys_tables.rs:64-112` — authoritative type/null/width model; lighter wire
  copy at core `types.rs:149-163`) is the validation pattern.
- **Catalog in-memory**: `TableEntry` struct (`dag.rs:310-317`) holds `handle,
  schema, kind, depth, directory, index_circuits`. `RelationKind` enum
  (`dag.rs:268-278`) is asserted exactly 1 byte (`:283`): `SystemCatalog |
  BaseTable{unique_pk} | View`.
- **Recovery**: `CatalogEngine::open` replays the system catalog (`replay_catalog`,
  `catalog/bootstrap.rs:342-364`) and then committed-but-unflushed DDL
  (`recover_system_tables_from_sal`, `bootstrap.rs:321`) — **both before `fork()`
  (`bootstrap.rs:409`)**. Per-worker user-data SAL replay (`recover_from_sal`,
  `bootstrap.rs:164-189`) runs **after** the fork in each child, replaying the
  pre-scattered `FLAG_PUSH` groups (`:176`) via `replay_ingest`
  (`store.rs:1231-1241`). For **user tables** that routes through
  `dag.ingest_returning_effective` → `enforce_unique_pk` → `ingest_owned_batch`,
  which re-hashes each row by `partition_for_pk_bytes` (`partitioned_table.rs:138-140`).
  (`raw_store_ingest`, `store.rs:1213-1217`, is the system-table-only direct path.)
  The SAL is a single shared FIFO; each worker reads all of it but drops the child
  `Table` for every foreign partition first (`trim_worker_partitions` →
  `close_partitions_outside`, `partitioned_table.rs:365-390`) and silently skips
  foreign-partition rows during re-scatter (`:148-151`), so it materializes only
  partitions it owns. **Ordering guarantee** (§7): every `PartitionedTable` — including
  one built only from a SAL-committed-but-unflushed CREATE — exists with its final
  construction before any user row is re-ingested.
- **System tables**: `num_partitions = 1`, master-only, durable
  (`RelationKind::SystemCatalog` ⇒ `Persistence::Durable`, `dag.rs:291`); held as
  `StoreHandle::Borrowed`; DDL via `FLAG_DDL_SYNC`.
- **Uniqueness / FK co-location**: unique-PK enforcement is per-partition —
  `enforce_unique_pk` (`dag.rs:1820-1890`) calls `ptable.retract_pk_bytes(pkb)`
  **once per row** at `:1858` (a single probe shared by the insert and delete paths,
  `:1855-1857`), which routes through `local_index_bytes` →
  `partition_for_pk_bytes(full_key)`. Co-location of a row and its retraction is
  *necessary but not sufficient*: the **probe** must hash the same bytes the row was
  ingested under, or it lands in a different partition (§4.4). FK existence checks
  (`master.rs:1782-1822`; pre-create `catalog/validation.rs:8-46`) reference a
  **single** parent column and **scatter** only when it is the parent's lone PK
  (`is_parent_pk = pk.len()==1 && pk[0]==ref_col`, `:1786` ⇒
  `CheckPayload::ScatterSource` at `:1799`, routed by the parent PK via
  `with_worker_indices` at `:1282`), else **broadcast** (`CheckPayload::Broadcast`,
  `:1818`).
- **`with_worker_indices` is write-side, table-key-only**: `with_worker_indices` →
  `fill_worker_indices` (`exchange.rs:306-360`) has exactly **three** production
  callers, each routing by *a table's own PK* (never a join key): DML commit
  (`master.rs:2606`), FK check (`:1282`), and FK parent gather (`:1382`). The
  `is_pk_routing = col_indices == schema.pk_indices()` gate (`:325`) selects the
  PK-bytes path; the `GroupKey` fallback (index maintenance, routed by the index
  key) is a different path and is untouched.

## 4. Design

### 4.1 The distribution key is a leading prefix of the PK

A row and its retraction (identical PK) must land on the same worker *and the same
partition within it*, or per-partition PK-uniqueness (`enforce_unique_pk`) and
retraction matching break. Routing both by a PK subset co-locates them: identical
full PKs agree on every PK column, hence on any distribution subset, hence hash to
the same partition. **But co-location is not the whole story** — the per-partition
*probe* (`local_index_bytes`, used by `retract_pk_bytes`/`has_pk_bytes`) currently
hashes the full key to pick the partition, so it must be sliced to the same prefix
the row was ingested under or it probes a different partition (§4.4, §6). The
in-partition match stays on the full key (prefix-twins coexist in one partition and
are distinguished by their full PK); only the partition *selection* uses the prefix.

**Decision**: `CLUSTER BY <cols>` requires `cols == pk_indices[..k]` — the PK's
first *k* columns, in PK order, for some `1 ≤ k ≤ |PK|`. The distribution key is
therefore fully described by the single integer *k*. The default (no clause) is
*k = |PK|* — exactly today's full-PK routing.

Restricting to a **prefix** (rather than an arbitrary PK subset) is what keeps the
feature small:

- **Write-side routing is a byte-slice, not a gather.** The OPK region is the PK
  columns concatenated big-endian in PK order with no padding (`columnar.rs:288-303`),
  so the leading `dist_stride` bytes of any row's PK region *are* the OPK of its
  first *k* PK columns. Routing a clustered table is therefore
  `partition_for_pk_bytes(&get_pk_bytes(i)[..dist_stride])` — one slice, the same
  `mix`/`xxh3` hash, no new packer and no per-column gather. A non-leading subset
  would need a gather (skip an interior PK column's bytes) and a separate packer,
  reintroducing the `ReindexPacker`-vs-slice byte-equality hazard for no real gain.
- **Storage stays sort-aligned with the distribution key.** Shards are sorted by
  the full PK; a prefix distribution key is also a sort prefix, so a partition's
  rows for a given distribution-key value are contiguous (useful later; not relied
  on here).
- **Persisted state is one small integer**, not a packed column list (§5).

`dist_stride` is the encoded byte width of the first *k* PK columns:
`schema.pk_byte_offset(pk_indices()[k])` for `k < |PK|`, else `pk_stride`.
`pk_byte_offset` (`schema.rs:463-471`) takes a **column index** and returns that
column's byte offset within the PK region by accumulating `col.size()` over
`pk_columns()` in PK-list order — so it must be passed `pk_indices()[k]` (the column
index of the *k*-th PK column), **never the bare position `k`**. The same
`col.size()` widths drive the OPK encoder, so the offset matches the physical layout
exactly. `pk_byte_offset` `debug_assert!`s `is_pk_col`; in release that assert
compiles out, so passing `k` instead of `pk_indices()[k]` would silently misbehave.

Consequence for the headline use case: to co-locate `orders` with `customers` on
`customer_id`, declare `orders` `PRIMARY KEY (customer_id, order_id)` and
`CLUSTER BY customer_id`. The composite-PK-with-the-join-key-leading requirement is
the deliberate, standard cost of distribution-by-non-unique-key.

### 4.2 Write-side routing by the prefix slice

Routing changes at the **write-side table-key** sites only — where rows are placed,
or probed for uniqueness/retraction, by the table's own key. The discriminator is a
function's *role*, not whether it calls `get_pk_bytes`: the join-relay scatters
(`op_repartition_batches_mode`, `relay_scatter_merge_walk`,
`op_relay_scatter_consolidated_mode`) route by the *join* key and are untouched,
even where they call `partition_for_pk_bytes(get_pk_bytes(i))` on their raw-PK
fast-path — that path fires only when the join key equals the full PK, where the
delta and its reindexed trace both partition at full-PK width and a prefix slice
would desync them.

A table carries `dist_stride` (the encoded byte width of its first *k* PK columns;
`dist_stride = pk_stride` for the default). The **complete, closed** set of
table-key routers, all sliced to the same `dist_stride`:

- **Storage ingest** (`partitioned_table.rs:138-140`): `PartitionedTable` carries
  `dist_stride`; route by `partition_for_pk_bytes(&mb.get_pk_bytes(i)[..dist_stride])`.
  For the default this is byte-identical to today's `get_pk_bytes(i)`.
- **Per-partition PK probe** (`partitioned_table.rs:410-418`): `local_index_bytes`
  slices to `[..dist_stride]` before `partition_for_pk_bytes`. This one edit fixes
  `has_pk_bytes`, `retract_pk_bytes`, and therefore `enforce_unique_pk`, because all
  three funnel through it. **Omitting this site is a silent-corruption bug**: a
  prefix-distributed row ingests into the prefix-partition but its
  uniqueness/retraction probe lands in the full-key partition, so the old row is
  never retracted (UPSERT duplicates it; DELETE is dropped) and base-table
  positivity / PK-uniqueness break with no error.
- **DML scatter** (`write_commit_group` → `fill_worker_indices`,
  `exchange.rs:326-330`): pass the table's `dist_stride`; in the `is_pk_routing`
  branch slice to `[..dist_stride]` before `partition_for_pk_bytes`. The non-PK
  `GroupKey` fallback (index maintenance) is unchanged. No `RouteMode` or
  `ReindexPacker` change is needed.
- **SEEK routing** (`master.rs:953-967`): a point-lookup always carries the **full**
  PK (both the narrow `opk_key` and the wide `assemble_wide_pk` paths consume the
  full `stride`) and always unicasts. The only change is to slice the
  assembled/encoded OPK to its leading `dist_stride` bytes before
  `partition_for_pk_bytes`. Since `dist ⊆ pk`, a full-PK seek still pins exactly one
  worker, so no broadcast clause is needed. (Pinning a *prefix scan* to a single
  worker is a separate scan-planner opportunity, not in this plan; such scans
  broadcast as today.)

This set is complete: an enumeration of every `partition_for_pk_bytes` caller
classifies each as table-key (the four above) or join-key (the three relay
scatters plus the packed-`_join_pk` and pure-range trace-ownership filters, all left
alone). A **partial rollout is unsafe** — if the ingest router slices but the probe
or seek does not (or vice-versa), a table's base rows and its lookups partition
inconsistently and rows are silently lost. Treat the four table-key sites as one
atomic change.

Width robustness depends on the prefix width, and `dist_stride` (not `pk_stride`)
now selects the `partition_for_pk_bytes` hash branch. For a distribution prefix ≤ 16
bytes (the common case — a single u64/i64 leading column), `partition_for_pk_bytes`
widens via `widen_pk_be`, so two tables distributed by the same logical value
co-locate **even at different column widths**. For a prefix > 16 bytes it hashes the
raw OPK bytes via `xxh3` (no widening), so co-location then requires byte-identical
prefix encodings on both sides — which the `tc == 0` skip gate (§4.3) already
guarantees for any join that actually elides its exchange. Because all four
table-key routers slice to the *same* `dist_stride`, they always agree on the branch
and the partition. The only code that still hashes the **full** PK is the
per-shard XOR8 membership filter (`shard_file.rs:175-206`) — and that is correct
and intended: it fingerprints the in-partition *match* identity (the full PK), which
§4.1 keeps full-width; only partition *selection* moved to the prefix.

### 4.3 Co-partitioned local joins (and local GROUP BY)

Widen **both** detectors to read the table's distribution prefix:

- Add `shard_cols_match_dist_key(cols, k)`: `cols == pk_indices[..k]` for the
  table's *k*. (`shard_cols_match_pk` is the special case `k = |PK|`.) Use it in
  `compute_co_partitioned` (`compiler.rs:616`, join sources) **and**
  `compute_view_skips_exchange` (`dag.rs:788`, single-source `ExchangeShard` views)
  in place of `shard_cols_match_pk`. Each detector needs the table's *k*: the join
  detector reads schema off `ExternalTable`, the view detector off
  `self.tables[&tid]`, so *k* must reach both (§5 threads it onto `ExternalTable` and
  `TableEntry` — or, more simply, onto `SchemaDescriptor`, which both already hold).
- **The view detector is not join-specific.** `compute_view_skips_exchange` governs
  the exchange-skip for every single-source `ExchangeShard` view, so widening it also
  lets a **GROUP BY / reduce** whose grouping key equals the table's distribution
  prefix run **locally with no shuffle**: every row for a group value already lives
  on one worker, the reduce runs per-worker, and its output is partitioned by the
  group key = the view's declared PK (`build_reduce_output_schema` emits the PK in
  source order, `planner.rs:2446`), so the multi-worker gather stays correct. This is
  not speculative: the full-PK case is already live and tested
  (`test_group_by_compound_pk_multiworker`, `planner_group_by.rs:305-345`, runs at
  `GNITZ_WORKERS=4`; `g_full = GROUP BY a,b` co-partitions, `g_part = GROUP BY a`
  exchanges). The prefix case is the natural generalization from `k = |PK|` to
  `k ≤ |PK|`, gated only on base rows now being physically distributed by the prefix
  (which this plan introduces).
  - **DISTINCT and set-op sides do not benefit.** Both reindex to a *synthetic*
    content-hash PK (`map_hash_row` → shard col `[0]`, `planner.rs:3208-3211`) before
    their shard, so neither `shard_cols_match_pk` nor `shard_cols_match_dist_key` can
    ever match a real PK prefix, and the shard never reads a bare scan. They always
    exchange, unaffected by the widening. Only GROUP BY/reduce gains locality.
  - **The view detector requires a bare scan** (`dag.rs:767-776`) — it does not walk
    through Filters — so the GROUP BY bonus fires only for an *unfiltered* `GROUP BY
    prefix`. `compute_co_partitioned` (joins) is more permissive: it resolves the
    reindex key *through* Filters (`reindex_cols_through_filters`), so a filtered join
    still co-partitions. Relaxing `compute_view_skips_exchange` to walk Filters would
    extend the GROUP BY bonus to `GROUP BY prefix … WHERE …` and unify the two
    detectors (§11); it is an optimization, not required for correctness.
- The `tc != 0` promotion guard stays (`compiler.rs:607`). A promoted reindex slot
  is at a different width than the native distribution key, so the cogroup memcmp
  would not match even though the rows co-locate; such joins keep exchanging. The
  skip therefore fires only for **common-native-width** join keys — exactly the case
  where co-located rows are also byte-equal. `shard_cols_match_dist_key` must retain
  this guard.
- A **join** is local when **both** sides' reindex keys equal their respective
  distribution prefixes. The per-source detector already evaluates each source
  independently; both skipping yields a local join with **no new node type**.
- **The range-join arm never co-partitions.** `evaluate_dag` checks the
  `Join(DeltaTraceRange)` arm *first* (`dag.rs:1280-1319`, gated by
  `view_range_join_n_eq`, `dag.rs:839-847`); it always relays input and exchanges
  output and never consults `view_skips_exchange`, so widening the detector cannot
  affect it.
- **Correctness of the local relabel** is already guaranteed: `map_reindex` is a
  standalone pre-phase node that always runs locally, re-keying each side's rows to
  the join key (e.g. `orders`' full PK `(customer_id, order_id)` → `customer_id`)
  before `cogroup`. The skip elides only the cross-worker shuffle
  (`dag.rs:1330-1336,1353-1355`). So a distribution prefix that is a *proper* prefix
  of the PK joins correctly without any added relabel.

The detector is **conservative but never wrong**: it fires only on an exact
`cols == pk_indices[..k]` match. A key that is a *super-prefix* of the distribution
key (e.g. `GROUP BY (customer_id, order_id)` on a `CLUSTER BY customer_id` table)
also co-locates — each finer group is wholly inside one prefix-partition — yet is
not detected, so it exchanges unnecessarily. Recognizing super-prefixes is a
possible future refinement; it cannot cause a false skip.

### 4.4 The probe-routing invariant, and FK checks

**The load-bearing fix is the per-partition probe, not FK.** As §4.1–4.2 establish,
the one mandatory correctness change beyond write-side placement is slicing
`local_index_bytes` to `dist_stride` so uniqueness/retraction probes the partition
the row actually lives in. State it as an invariant: *any code that locates a row's
partition by hashing a PK must hash the table's distribution prefix, not its full
PK.* `local_index_bytes` (`partitioned_table.rs:410-418`) is the single funnel for
`has_pk_bytes`/`retract_pk_bytes`/`enforce_unique_pk` (one `retract_pk_bytes` call,
`dag.rs:1858`), so it is the only probe site the core feature must touch.

**FK existence checks stay correct with no change.** A FK references a single parent
column (`parent_col_idx`). `is_parent_pk = pk.len()==1 && pk[0]==parent_col_idx`
(`master.rs:1786`) fires *only* for a **lone-PK** parent — and a lone-PK parent has
`k = |PK| = 1`, so it is always default-distributed and its `ScatterSource` routing
(by the full parent PK, which *is* its distribution prefix) already hashes the right
bytes. Every other parent (a column of a compound PK, or a unique-indexed non-PK
column) **broadcasts** (`:1818`), and broadcast is correct under *any* distribution
(each worker filters its own partitions via the same per-partition probe). So a
prefix-distributed parent needs no FK change to remain correct; FK checks just
broadcast more than strictly necessary.

**Scatter-by-prefix is a deferrable optimization, and is not "one boolean."** To
turn a broadcast into a scatter when the child's referenced columns cover the
parent's first-*k* PK columns: `ScatterSource` currently passes the full parent
schema, and `build_check_batch` (`master.rs:3314-3338`) fills exactly the lone PK
column's bytes (`:3336`). Scattering by a prefix requires (a) filling the parent's
first-*k* PK columns' OPK bytes into the check batch in PK order, and (b) routing the
`with_worker_indices` call at `:1282` by `dist_stride` (via a synthetic prefix-PK
schema or a prefix-sliced router). The parent gather at `:1382` routes by the parent
PK identically and would need the same treatment. This is real, multi-site work for
a broadcast-elision win on compound-PK parents; defer it unless that broadcast is
shown to matter.

## 5. Data-model / persistence changes

- **Persist *k* in the existing `flags` column** (`TABLE_TAB`,
  `sys_tables.rs:184-186`): a 3-bit field above `TABLETAB_FLAG_UNIQUE_PK` (bits
  `[1..4)`), with `0` meaning "default = full PK." Only bit 0 of `flags` is in use
  today (verified exhaustively: `TABLETAB_FLAG` has exactly three sites — the
  constant `sys_tables.rs:43`, the read `(flags & TABLETAB_FLAG_UNIQUE_PK)` at
  `hooks.rs:179`, and a `#[cfg(test)]` write at `ddl.rs:180`; the production write is
  `client.rs:528` `unique_pk as u64`; the `drop_table` round-trip reads `flags`
  (`client.rs:934`) and re-writes it **verbatim** (`:556`), so packed bits survive a
  DROP). Concretely: the writer becomes `flags = (k_field << 1) | (unique_pk as u64)`
  and the reader adds `let k = (flags >> 1) & 0x7;`. **Both** write sites must change
  — `client.rs:528` (production) and `ddl.rs:195` (test-only) — or catalog
  round-trip tests diverge. Two bits actually suffice (an explicit prefix is always
  `< |PK| ≤ 4`, since `k = |PK|` is the default `0`), so the third bit is headroom.
  This adds no `TABLE_TAB` column and no change to the positional decode at either
  site — `flags` is already read there. Guard against future code that reconstructs
  a `TABLE_TAB` row from `unique_pk` alone: it would silently zero *k*, since *k* now
  rides the same column.
- **Validate** with a small `validate_dist_prefix(pk, cols) -> k`: the named columns
  must equal `pk_indices[..cols.len()]` and `1 ≤ cols.len() ≤ |pk|`. No type/null
  re-checks — those are inherited from `validate_pk_cols`
  (`sys_tables.rs:64-112`: eligibility, non-null, no-dups, width), since `dist ⊆
  pk`. A prefix variant of `shard_cols_match_pk` (`schema.rs:455-458`) is the natural
  sequence-equality primitive.
- **`TableEntry`** (`dag.rs:310-317`) gains `dist_prefix_len: u8` (decoded from
  flags in `hook_table_register`, threaded through `register_table`; `0` normalized
  to `|pk|`). `RelationKind` stays 1 byte — the distribution key is data on
  `TableEntry`, not a `RelationKind` variant (widening `BaseTable{unique_pk}` would
  break the `:283` 1-byte niche assert).
- **`dist_stride`** = the encoded byte width of the first *k* PK columns =
  `schema.pk_byte_offset(pk_indices()[k])` for `k < |PK|`, else `pk_stride`. It is a
  pure function of `schema` + *k* (so it can live on the schema, §11 simplification).
- **`PartitionedTable`** gains `dist_stride: usize`, **computed in
  `PartitionedTable::new` at construction** (from the schema + decoded *k*), so the
  per-row ingest and probe loops read a field, and — critically for recovery (§7) —
  every `PartitionedTable` carries its final `dist_stride` the moment it exists. This
  threads a new argument through `PartitionedTable::new` (already
  `#[allow(too_many_arguments)]`), `build_partitioned_storage` (`hooks.rs:140`), and
  `register_table`; the **view** registration path also calls
  `build_partitioned_storage` and must pass the full-PK default (`dist_stride =
  pk_stride`).
- **`ExternalTable`** (compiler-side) carries *k* so `compute_co_partitioned` can
  call `shard_cols_match_dist_key`.

## 6. Implementation (sites)

Sites are grouped **mandatory** (core correctness) vs **optional** (further
optimization).

**Mandatory:**

| Concern | Site | Change |
|---|---|---|
| DDL parse | `gnitz-sql/src/planner.rs:239` (`execute_create_table`) | read `create.cluster_by` — `CLUSTER BY a, b` (unparenthesized comma list) is parsed into `cluster_by` by `GenericDialect` (sqlparser 0.56 `parser/mod.rs:7162-7168`); resolve idents like the PK pass. `DISTRIBUTE BY` has **no** AST node, so `CLUSTER BY` (or a `WITH (...)` option) is the surface. |
| DDL validate | `gnitz-sql` + `sys_tables.rs` | `validate_dist_prefix(pk, cols) -> k` (cols == PK[..k]) |
| DDL persist | `client.rs:528` **and** `ddl.rs:195` | `flags = (k << 1) \| unique_pk` at **both** write sites |
| DDL apply | `hooks.rs:164-243` (`:178-179`) | decode `k = (flags>>1)&0x7`; compute `dist_stride`; pass to `build_partitioned_storage` → `PartitionedTable::new` |
| Sys-table flags | `sys_tables.rs:43` | reserve flag bits `[1..4)` for *k* |
| Storage ingest routing | `partitioned_table.rs:29-35,138-140` | carry cached `dist_stride`; ingest routes by `pk_bytes[..dist_stride]` |
| **PK probe (uniqueness/retraction)** | `partitioned_table.rs:410-418` | `local_index_bytes` slices to `[..dist_stride]`. **Omitting this silently loses retractions / duplicates PKs (§4.4).** |
| DML scatter | `master.rs:2606`, `exchange.rs:325-330` | pass the *committed table's* `dist_stride`; slice in the `is_pk_routing` branch only (relay scatters untouched) |
| SEEK routing | `master.rs:953-967` | slice the assembled OPK to `[..dist_stride]` (still pins one worker; no broadcast clause) |
| Co-partition (joins) | `compiler.rs:597-622`, `schema.rs:455-458` | `shard_cols_match_dist_key`; thread *k* onto `ExternalTable` |
| Co-partition (single-source views) | `dag.rs:757-789` | same `shard_cols_match_dist_key` (`:788`); read *k* from `TableEntry`. Governs GROUP BY/reduce skips (§4.3). |
| Catalog | `dag.rs:310-317` | `TableEntry.dist_prefix_len`; set from hook; thread through `register_table` |
| Recovery | (none) | ingest re-hash and probe read the cached `dist_stride`; ordering already safe (§7) |

**Optional (deferrable optimization, §4.4):**

| Concern | Site | Change |
|---|---|---|
| FK scatter-by-prefix | `master.rs:1282,1382,3314-3338`, `catalog/validation.rs` | fill the parent's first-*k* PK columns into the check/gather batch and route by the parent's `dist_stride`; until done, compound-PK parents broadcast (correct, slower) |
| Filtered GROUP BY locality | `dag.rs:767-776` | relax the bare-scan restriction to walk Filters, unifying with `compute_co_partitioned`; extends the §4.3 bonus to `GROUP BY prefix … WHERE …` |

## 7. Recovery

- The system catalog is fully replayed and applied **before `fork()`**:
  `replay_catalog` (`catalog/bootstrap.rs:342-364`, from shard storage) and
  `recover_system_tables_from_sal` (`bootstrap.rs:321`, committed-but-unflushed DDL)
  both run in the master pre-fork. User-data SAL replay (`recover_from_sal`,
  `bootstrap.rs:164-189`) runs only in each forked child. Both catalog paths fire the
  same `hook_table_register` → `build_partitioned_storage` (`hooks.rs:140-162`), so a
  single decode-and-plumb edit covers both. **Precondition:** `dist_stride` must be
  materialized *inside* `PartitionedTable::new` at construction (§5). Given that,
  every `PartitionedTable` carries its final `dist_stride` before any user row is
  re-ingested — so re-ingest re-hashes by the correct prefix and there is **no change
  to `replay_ingest` / `bootstrap.rs`**.
- A table's re-hash on replay flows through `ingest_owned_batch`
  (`partitioned_table.rs:138-140`), and the per-partition probe through
  `local_index_bytes` (`:410-418`); both read the cached `dist_stride`. The SAL
  `FLAG_PUSH` groups were scattered by the prefix at write time; each worker reads the
  shared SAL but materializes only partitions it owns (`trim_worker_partitions` drops
  foreign child tables; the re-scatter skips foreign-partition rows), so the re-hash
  and the probe agree because they hash the same prefix slice.
- The min-flushed-LSN watermark (`min_flushed_lsn`, `partitioned_table.rs:357-359`,
  a `min` over child `current_lsn`) is key-agnostic and unchanged.

## 8. Limits

- The distribution key is `pk_indices[..k]`, `1 ≤ k ≤ |PK| ≤ 4`. A key that is not
  a leading PK prefix is rejected at DDL with a message pointing at PK column order.
- Hash distribution still uses 256 buckets and contiguous worker ranges; a STRING
  distribution column is impossible (PK columns are never STRING, and `dist ⊆ pk`).
- A table has one distribution key, so a fact table co-partitions with at most one
  dimension; other joins still exchange (or use a replicated dimension — separate
  plan).
- The co-partition detector matches an *exact* prefix, not a super-prefix (§4.3), so
  some safely-co-located joins/GROUP BYs still exchange.
- **Distribution-prefix skew is the dominant cost (§11).** The full-PK default
  spreads rows by a (near-)unique key. A short, low-cardinality prefix (e.g.
  `customer_id` when a few customers hold most rows) concentrates rows onto a few of
  the 256 partitions, hence a few workers — a hot-partition / load-imbalance hazard
  with no analog in the default. This is the deliberate trade for local joins, and it
  is the user's to weigh per table: co-partitioning a fact table by a skewed
  dimension key can cost more in imbalance than it saves in shuffle. The engine does
  not detect or rebalance skew (range/adaptive partitioning is a non-goal).

## 9. Testing

- **Routing**: a table `CLUSTER BY c` with `c = pk_indices[..1]` routes a row and
  its retraction to the same worker; a multi-column prefix likewise.
- **Validation**: `CLUSTER BY` a non-leading PK column, a non-PK column, or a column
  set that is not a contiguous PK prefix is rejected at DDL.
- **Local join**: `customers(id PK)` (default `CLUSTER BY id`) joined to
  `orders(PRIMARY KEY (customer_id, order_id)) CLUSTER BY customer_id` on
  `id = customer_id` skips the exchange on both sides (assert no relay scatter) and
  returns correct results; a cross-width or non-aligned join still exchanges.
- **Proper-prefix correctness**: the `orders` side above (distribution prefix ⊊ PK)
  returns rows correctly grouped by `customer_id` (verifies the local `map_reindex`
  relabel runs under exchange-skip).
- **Uniqueness / retraction under a prefix key (the §4.4 regression)**: on a
  `CLUSTER BY prefix` table with a *compound* PK, insert two rows that share the
  distribution prefix but differ in the PK suffix, then UPSERT one and DELETE the
  other; assert the old payload is retracted exactly once and no duplicate PK
  survives. This is the test that would fail if `local_index_bytes` is not sliced
  (the probe would miss the existing row). Include prefix-twins that hash their full
  PK to a *different* partition than their prefix to make the bug observable.
- **All-sites-or-none**: a test that ingests, then seeks and deletes by full PK on a
  prefix-distributed table — exercising ingest, probe, and SEEK together — to catch a
  partial rollout where one router slices and another does not.
- **FK**: existence checks against a prefix-distributed parent stay correct
  (compound-PK parents broadcast; lone-PK parents scatter); if the optional
  scatter-by-prefix is built, assert it routes to the owning worker.
- **GROUP BY co-partition (§4.3)**: an unfiltered `GROUP BY prefix` on a
  prefix-distributed table skips its exchange and returns results identical to the
  exchanged path; a `GROUP BY` on a non-prefix column still exchanges; a filtered
  `GROUP BY prefix … WHERE …` still exchanges (bare-scan limit) unless the §6 optional
  relaxation lands. (DISTINCT and set-ops always exchange — they reindex to a
  synthetic PK — so they are *not* expected to skip.) Run under `GNITZ_WORKERS=4`.
- **Skew sanity (§11)**: ingesting a low-cardinality-prefix dataset lands rows on a
  strict subset of partitions/workers (documents the hazard; not a correctness
  assertion).
- **Default parity**: a table with no clause behaves byte-identically to today
  (routes by full PK; joins exchange unless full-PK-aligned). Assert the routed
  partition for representative keys is unchanged from the pre-change build.

## 10. Invariants preserved

- **Same-PK co-location** for uniqueness/retraction — guaranteed by `dist =
  pk_indices[..k] ⊆ pk` (§4.1).
- **Probe routing matches ingest routing** — every site that locates a row's
  partition by hashing a PK (ingest, the `local_index_bytes` probe, DML scatter, and
  SEEK) hashes the same `dist_stride` prefix, so the uniqueness/retraction probe
  always reaches the partition the row was ingested into (§4.4). The in-partition
  match — and the XOR8 membership filter — remain on the full PK.
- **Join-key routing is separate from table-key routing** — the three join-relay
  scatters route by the join key / packed `_join_pk` and are untouched; the
  `tc == 0` co-partition gate keeps a skipped local join's two sides byte-equal at
  native width, and `widen_pk_be` keeps co-location robust across widths ≤ 16.
- **256-bucket / contiguous-worker model** unchanged; `worker_for_partition` /
  `partition_range` are key-agnostic and untouched.
- **Source of truth on durable base tables** — replay re-derives partition placement
  from the table's construction-time `dist_stride` deterministically, with catalog
  replay ordered before user-data replay (§7).
- **The probe slice (§4.4) is the only mandatory correctness change** — the
  recovery, FK, and routing paths need no other fix, and the feature introduces no
  latent bug beyond the partial-rollout hazard (§4.2), which the all-sites-or-none
  contract and its test (§9) close.

## 11. Performance: allocation, cache, skew, and a simplification

**The routing change adds zero allocation and zero copy.** `&pk_bytes[..dist_stride]`
is a pointer/length narrowing of the existing `get_pk_bytes(i)` slice — no heap
traffic, no memcpy, no new buffer. The TLS index pools (`PARTITION_INDICES` in
`ingest_owned_batch`, `SCATTER_INDICES` in `with_worker_indices`) are untouched and
keep their clear-retaining-capacity discipline. No new packer/gather is introduced
(the whole point of the prefix-only restriction, §4.1), so the `ReindexPacker`'s
per-row scratch buffer is not in play on these paths.

**Cache behavior is neutral-to-favorable.** A row's PK region is contiguous, so
slicing to a leading prefix hashes a subset of bytes already on the touched cache
line(s); a narrow prefix (≤ 16 B) reads *fewer* bytes and stays on the `widen_pk_be`
fast path (a prefix of exactly 16 B is meaningfully cheaper to route than 17 B, which
falls to `xxh3`). There is no pointer-chasing or extra indirection.

**Hoist `dist_stride` into a local before the hot loop.** Cache it on
`PartitionedTable` (§5), store it as `usize` once (`pk_byte_offset`/`pk_stride` return
`u8`), and bind `let dist_stride = self.dist_stride;` outside the per-row loops in
`ingest_owned_batch` and `local_index_bytes` so the width is a register, not a field
reload through `&self`, and `partition_for_pk_bytes`'s `len <= 16` branch stays
predictable. Same for the `dist_stride` argument threaded into `fill_worker_indices`.

**Skew has a compounding cost, not just an imbalance (§8).** A low-cardinality
distribution prefix concentrates rows onto a few of the 256 partitions. Because each
partition is an independent `Table` with a **fixed** memtable arena
(`partition_arena_size` = 256 KiB for user tables, `partitioned_table.rs:466-472`) and
its own L0/compaction, a hot partition fills its arena faster → flushes more often →
emits more L0 shards → triggers compaction sooner, all on the same worker. So prefix
skew shows up twice: once as cross-worker load imbalance, and again as
flush/compaction amplification on the hot worker's hot partitions. The full-PK default
avoids both by spreading on a near-unique key. Weigh this per table against the
shuffle it eliminates.

**Simplification — carry the prefix on `SchemaDescriptor`.** `dist_stride` is a pure
function of `(schema, k)`. If `SchemaDescriptor` carried `dist_prefix_len` (or the
derived `dist_stride`), every consumer — storage, the two detectors, the routers —
would read one field instead of *k* being threaded separately onto `ExternalTable`,
`TableEntry`, and `PartitionedTable::new`. Both detectors already hold a
`SchemaDescriptor`, and `dist_stride` would then be derivable wherever the schema is.
Weigh against `SchemaDescriptor` being widely shared/immutable; if it is the wrong
home, the per-struct threading in §5 stands.

**The win it buys.** A co-partitioned **join** drops straight to `execute_pre_phase`
(`copart_join`, `dag.rs:1353-1355`): no per-row IPC scatter, no wire
serialize/deserialize, **and** no post-exchange `consolidate_exchanged` — only the
always-local `map_reindex` relabel remains. For a large fact-table delta joined to a
co-located dimension, that removes the dominant network and re-consolidation cost of
the tick. A co-partitioned **single-source** view (unfiltered GROUP BY/reduce,
`dag.rs:1330-1336,1342-1346`) elides the IPC round-trip but still consolidates its
local pre-phase output before the post phase — the network shuffle is saved, the local
sort/merge is not.
