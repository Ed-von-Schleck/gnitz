# LRU views: capacity-bounded materialization via skeleton shards + scan-time rehydration

## Problem

A view's output family is a full materialization of its result — every row,
every payload byte, forever. For views over wide/TEXT-heavy rows whose old
data is rarely read, that is the wrong point on the space/latency curve: the
user should be able to bound how much materialized payload a view keeps, the
way a cache has a capacity, without ever risking a wrong answer.

This plan adds **LRU views**: the view is maintained exactly as today (every
delta, every tick — correctness is never a function of what is resident), but
its family stores, beyond a user-set capacity, only **skeleton rows** — the PK
region and the weight, with all payload dropped. A scan or seek that touches a
skeleton row recomputes that key's output rows on the worker, from the view's
own operator traces, which remain fully materialized and are the hydration
ground truth. Cold data costs `pk_stride + ~8` bytes per row instead of the
full payload + blob heap; reads of cold data pay a key-local recompute.

Two properties make this sound with zero correctness risk:

1. **The write path never reads family payloads.** `drive_dag` only appends
   output deltas into the family (`ingest_store_and_indices` →
   `ingest_borrowed_batch`, `query/dag/mod.rs:1100-1131`, ingest sites at
   `dag/mod.rs:1331,1334`; `table/mod.rs:350-379` — consolidate + memtable
   append only). Payloads are read exclusively by client scans/seeks and by
   compaction; the only other family reader, `gather_family_bytes`, is issued
   solely by DML preflight against base tables (`master/preflight.rs:464,647`).
   Dehydration therefore never touches delta processing.
2. **Every SQL view output is a positive Z-set** (each (PK, payload) at net
   weight ≥ 0 at tick boundaries): every net-negative-capable construction is
   clamped — the only `negate` call sites reachable from SQL are internal
   subtrahends of `positive_part`/bag-min/threshold differences
   (`plan/view/set_op.rs:341`, `exists.rs:495,717`, `join.rs:1402`). Hence a
   PK whose summed weight is 0 at a tick boundary has no rows at all.

## SQL surface (committed)

```sql
CREATE LRU VIEW hot_orders CAPACITY 256 MB AS
  SELECT o.id, o.note, c.name FROM orders o JOIN customers c ON o.cid = c.id;

CREATE LRU VIEW recent CAPACITY 1000000 ROWS AS
  SELECT id, body FROM messages WHERE kind = 3;
```

- Grammar: `CREATE LRU VIEW <name> CAPACITY <uint> {KB | MB | GB | ROWS} AS
  <query>`. The CAPACITY clause is **mandatory** for LRU views; `LRU` without
  `CAPACITY` is an error, and `CAPACITY` on a plain `CREATE VIEW` gets its own
  targeted error. Byte units are binary (KB = 2^10). `<uint>` must be a plain
  unsuffixed integer token; the unit must be a separate unquoted word.
- **The capacity contract**: capacity bounds the view's **on-disk hydrated
  payload** per partition (byte or row accounting of non-skeleton shards),
  enforced whenever the view flushes or compacts, at **guard granularity**
  with **write-recency** victim ordering. It is not a per-row residency
  guarantee: hash skew across the 256 partition shares, guard boundaries, and
  physical row duplication across levels all make "exactly the newest N rows"
  unachievable, and dehydrated guards never re-hydrate (post-shrink hydration
  does not recover; fresh deltas for their keys stay hydrated only until they
  sink). The in-memory young head behaves exactly as for every view today
  (existing `INMEM_CEILING = 4 MiB` per partition `Table`, `table/mod.rs:47`)
  and is not counted — LRU changes the disk tier only.
- **LRU views are leaf views**: `CREATE VIEW` (of any kind) referencing an LRU
  view anywhere in its body is rejected. `SELECT`/scan/seek work normally.
- **Eligible shapes** (the final user-named segment's own circuit; hidden
  chain segments are ordinary views and are unaffected):
  1. **Linear** (`ViewShape::Simple`): filter/projection over one relation.
     Allowed sources: a base table whose distribution key is its full PK or
     which is replicated (a `CLUSTER BY`-prefixed table is ineligible — its
     rows live on the prefix-hash worker, the view family on the full-PK-hash
     worker, so hydration would not be worker-local); a registered non-LRU
     view; or a chain-local hidden segment (CTE/derived table — always
     eligible, never LRU, resolves to no catalog row).
  2. **Inner equi-join final step** (`ViewShape::Join` whose last chain step
     is INNER with equality keys only): residual ON conjuncts, WHERE, and
     projection are all replayable. A range/band conjunct on the final step,
     or a preserved side (LEFT/RIGHT/FULL final step), is ineligible.
  Everything else — grouped, DISTINCT, set-ops, and every subquery flavor
  (conjunct EXISTS/IN, mark, scalar) as the final shape — is rejected with
  `Unsupported("CREATE LRU VIEW: <shape> is not supported")`.
  The rule behind the list: the segment downstream of its integrated traces
  must be **stateless-replayable** (JoinDT/Filter/Map/Union-of-the-two-join-
  terms only — no WeightClamp, no Reduce, no JoinDTRange, no Delay).

## Ground facts this design builds on (verified against the live tree)

- **The compiled inner equi-join circuit has TWO JoinDT terms.** The planner
  emits `join_ab = ΔA ⋈ trace_b` and `join_ba = ΔB ⋈ trace_a`
  (`plan/view/join.rs:803-804`, traces built at `join.rs:801-802` over the
  reindexes at `:772-800`), each followed by a normalization Map, merged by
  `cb.union(..)` in `normalize_to_ab` (`join.rs:104-127`, called at `:808`),
  then optional residual/WHERE `Filter` (`join.rs:957-958`) and projection
  `Map` (`join.rs:984`) into the sink. No SQL join view emits a `ScanTrace`
  side today.
- **The terminal LSM level is L2.** The only production vertical call is
  `compact_guard_vertical(1)` from `run_compact`
  (`shard_index/index.rs:208-213`); the code's own terminal predicate is
  `level_num == MAX_LEVELS - 1` (`index.rs:272`, `:423`). Levels deeper than
  L2 never hold data.
- **Every skeleton-visible fold is a per-key time-prefix at a tick boundary.**
  `run_compact` consumes all of L0 (`index.rs:179-216`, cleared on commit at
  `commit_l0_to_l1`, `:255-268`); `compact_one_guard` folds all entries of a
  guard (`index.rs:297-327`); verticals fold the whole source guard plus every
  overlapping destination guard (`index.rs:329-428`, destination folds at
  `:368-384`); the ephemeral spill consolidates and writes the entire
  in-memory set atomically at flush time (`table/flush.rs:376-396`, atomic
  write + register in `persist_l0_run`, `flush.rs:231-273`). Level movement
  always takes everything at a level for a key range downward, so per key the
  level split is a time-prefix split.
- **The recency signal already exists.** `current_lsn` bumps on **every**
  ingest, not just Durable (`table/mod.rs:356-360`; pinned by
  `current_lsn_bumps_on_every_ingest_including_rederive`,
  `table/mod.rs:1951-1964`), and the ephemeral spill registers real LSNs —
  `persist_l0_run` → `add_shard(&final_full, lsn_max)` with
  `lsn_max = current_lsn − 1` (`flush.rs:109-114, 261`; pinned at
  `table/mod.rs:1836-1887`). Compaction propagates entry LSN bounds through
  every fold.
- Family write path: `drive_dag` ingests at `query/dag/mod.rs:1331/1334` and
  flushes each dirty view once per tick (`dag/mod.rs:1358-1361`,
  `flush_view_or_abort` at `:1154`). Ephemeral flush pushes to `in_memory_l0`
  (`flush_to_ram`, `flush.rs:95-98`, push at `:85`); disk shards appear via
  spill past the ceiling, which then runs `compact_if_needed` (`flush.rs:394`,
  fn at `table/mod.rs:660-669`). Rederive tables publish no manifest on the
  ingest path (`flush.rs:154-158`; view stores persist manifests only in the
  checkpoint's ephemeral round) and there is no per-table WAL — durability is
  the fsynced SAL. The in-memory ceiling override is `#[cfg(test)]`-only today
  (field `table/mod.rs:246`, setter `:470-473`, getter `flush.rs:308-316`).
- Scan: master `handle_scan` drains all pending ticks before dispatch
  (`executor.rs:1297`, drain loop `:1313-1328`); workers materialize the whole
  family atomically inside one dispatch (`scan_family`, `store_io.rs:27-37` →
  `StoreHandle::full_scan`, `query/dag/store_handle.rs:101-112` →
  `materialize`, `read_cursor/output.rs:154-169`); the chunked wire train
  slices an immutable `Rc<Batch>` (`worker/reply.rs:293-303`). Seek: the
  worker primitive is `seek_family` → `seek_entry_bytes`
  (`store_io.rs:39-74`), which copies the single positioned row; master
  `handle_seek` (`executor.rs:1034-1068`) does **not** drain ticks today.
- Consolidation identity is uniformly (PK, payload); the single pending-group
  drain owner is `drive_merge` (`repr/heap.rs:232-301`, boundary
  `same_pk && eq_payload` at `:290`, emit gate `net_weight != 0` at `:297`,
  comparison against the group exemplar); the comparator builders are
  `merge_same_pk`/`merge_eq_payload` (`repr/merge.rs:523-581`), seated by
  `run_merge_body` (`merge.rs:591`), and the cursor drives live at
  `read_cursor/mod.rs:519-660` (`drive` `:519`, `heap_less_with` `:132`,
  `drive_with_inner` `:615`, `drive_pair_inner` `:652`). Every payload
  comparison a family cursor or compaction can perform funnels through these
  sites.
- Shard format: header FLAGS byte at `OFF_FLAGS = 56`
  (`storage/repr/layout.rs:29`), only `SHARD_FLAG_PK_UNIQUE = 0x01`
  (`layout.rs:34`) used; the reader derives the region count from the schema
  (`strides_from_schema`, `shard_reader/open.rs:41-48`) and indexes the
  directory positionally (`open.rs:59, 99-228`); per-region encodings
  (RAW/CONSTANT/TWO_VALUE) are self-describing. Zero-payload (all-PK)
  `SchemaDescriptor`s already exist and work end-to-end — every secondary
  index schema is one (`make_index_schema`; `batch_project_index` appends
  index rows with no payload regions, `query/dag/mod.rs:2079-2083`).
  `Batch::write_as_shard(path, schema, ShardWriteOpts)` carries the header
  flags in `ShardWriteOpts.flags` (`repr/batch_wire.rs:28-36`).
  `ShardIndex::add_shard` (`index.rs:42-47`) is the registration chokepoint
  the durable flush and the spill both route through (`persist_l0_run`,
  `flush.rs:261`); compaction outputs install via `open_outputs` →
  `ShardEntry::open` (`index.rs:163-177`).
- Plans and traces at scan time: the scan path is `&mut CatalogEngine`;
  `self.dag` (`catalog/mod.rs:104`) reaches the private plan cache
  `cache: FxHashMap<i64, CachedPlan>` and `tables` (`dag/mod.rs:323, 326`).
  Join views own their input traces as `_int_{view}_{nid}` child tables in
  `VmHandle.owned_tables` / `owned_trace_regs` (`compiler/emit.rs:634-641`,
  `vm/mod.rs:168, 179`). There is exactly one compile site
  (`compile_view_internal`, `dag/mod.rs:929-967`) serving live CREATE, boot,
  and worker sync; `CompileOutput` is constructed in three arms
  (`compiler/mod.rs:324, 408, 558`). `ensure_compiled` returns `false` on
  compile failure (`dag/mod.rs:797-808`, warn-only log at `:963`) and
  `hook_view_register` today merely skips the backfill on `false`
  (`hooks.rs:445`).
- VM: `execute_epoch(&Program, &mut RegisterFile, input_batch, input_reg,
  output_reg, cursor_handles, owned_trace_reg_ids)` (`vm/exec.rs:39-47`);
  arbitrary registers are seedable (`execute_epoch_multi`, `exec.rs:66-73`,
  taking `(u16, Batch)` pairs); `JoinDT` is pure (delta × trace cursor →
  `[left_PK | left payload | trace payload]` at products of net weights,
  `ops/join/delta_trace.rs:23-57`), as are `Filter`/`Map`/`Union`; the only
  Table writers are `Integrate` with a non-null table and `WeightClamp` with
  a history table (`exec.rs:349-356, 264-281`). `Program.funcs` holds raw
  `*const ScalarFunc` pointers (`vm/mod.rs:284`); `RegisterFile::new` takes
  `&[RegisterMeta]` (`vm/mod.rs:322`). `ReadCursor` owns its sources via `Rc`
  with no borrow lifetime (`CursorSource`, `read_cursor/source.rs:19-25`);
  `Table::open_cursor` is `&self` (`table/mod.rs:420`);
  `for_each_pk_group_row` (`read_cursor/mod.rs:418-443`) walks one PK group;
  `copy_current_row_into` (`read_cursor/output.rs:101-115`) relocates
  German-string blobs via `Batch::append_row_from_source_bytes`.
- Co-location: the join exchange scatter and the family ingest route by
  `partition_for_pk_bytes` (`schema/key.rs:138-144`) over the identical
  packed, width-promoted `_join_pk` OPK bytes (`ScatterKey::partition`,
  `ops/exchange/router.rs:145-156`; `ReindexPacker::pack_into` /
  `promote_into`, `ops/reindex.rs:301-318`; OPK encode `opk_key`,
  `schema/key.rs:96-106`) — worker-local by the OPK hash invariant. Simple
  views force-prepend the source PK verbatim (`plan/lp.rs:83`,
  `codec/project_schema.rs:74-84, 144`), so view PK = source PK
  byte-identically. NULL join keys are excluded by planner `IS NOT NULL`
  filters upstream of the reindex/traces (`join.rs:765-767, 787-789`), so
  traces, family, and hydration see the identical post-filter universe.
- Parser: `sqlparser` 0.56 / `GenericDialect`; the single statement entry is
  `SqlPlanner::execute`, which parses N statements per string
  (`gnitz-sql/src/lib.rs:48-57`) and is the route for capi
  (`gnitz-capi/src/lib.rs:1332,1438`) and py (`gnitz-py/src/lib.rs:1744`);
  the only other `Parser::new` sites are `#[cfg(test)]` helpers.
  `Parser::parse_sql` = `Tokenizer::tokenize_with_location` →
  `with_tokens_with_locations` → `parse_statements` (sqlparser
  `parser/mod.rs:438-510`); the parser skips whitespace tokens itself, and
  comments are whitespace variants (`tokenizer.rs:452-467`); string literals
  tokenize as `SingleQuotedString`, never `Word`; `GenericDialect` has
  `supports_numeric_prefix = false` so `256MB` tokenizes as
  `Number("256")` + `Word("MB")`.
- Planner shape: `ViewShape` has exactly 8 variants (SetOp, Distinct, Join,
  Subquery, MarkSubquery, ScalarSubquery, GroupBy, Simple —
  `plan/view/dispatch.rs:161-200`, `classify` at `:226`); every arm,
  subquery flavors included, flows into the shared match and the common tail
  (`create_view_chain` at `dispatch.rs:150`). DISTINCT/GROUP BY over a join
  classify as `Distinct`/`GroupBy` and compile the join to a hidden view via
  `resolve_operator_input`. CTEs/derived tables compile to hidden segments
  *before* classify (`inline_ctes`/`compile_derived_tables`,
  `dispatch.rs:55-57`), and those chain-local vids have no VIEW_TAB row until
  `create_view_chain` commits (`dispatch.rs:141-150`). INNER-ness of a join
  step is AST-visible (`join_on_and_type`, `join.rs:160`) but range conjuncts
  are known only inside `plan_join_chain` after `extract_join_predicates` per
  step (`join.rs:483-484`, final step at `is_last`, `join.rs:486,509`).
- Catalog: VIEW_TAB is `[view_id(pk), schema_id, name, sql_definition,
  cache_directory, created_lsn, pk_col_idx]` — no flags column — defined
  **once** as `gnitz_wire::VIEW_TAB_COLS` (`gnitz-wire/src/catalog.rs:80-90`),
  from which the engine schema (`SYS_FAMILIES` entry at
  `catalog/sys_tables.rs:315`, `SCHEMAS` derivation), the engine's positional
  `VIEWTAB_PAY_*` constants (`sys_tables.rs:171-173`, via `col_index_in`),
  bootstrap's COL_TAB self-description rows (`catalog/bootstrap.rs:138-166`,
  iterating the slices), and the client `Schema`
  (`gnitz-core/src/types.rs:65-67`) all derive — so client and engine cannot
  drift and `Batch::decode_from_wal_block`'s region-count check is satisfied
  by construction. Row writers/readers: `append_view_row` is the **single**
  VIEW_TAB row writer, shared by `create_view_chain`'s `+1`
  (`client.rs:995-1007`) and `drop_view`'s full `-1` retraction
  (`client.rs:1063-1067`; `ViewRecord` at `client.rs:132-140`,
  `decode_view_record` at `client.rs:1259`), so the retraction byte-matches
  the insert by construction; `resolve_table_or_view_id`
  (`client.rs:1085`). `hook_view_register` (`hooks.rs:353`) decodes the row
  and builds storage via `build_partitioned_storage` → `PartitionedTable::new`
  (`hooks.rs:148-206`; replicated views get one partition per worker,
  `hooks.rs:180-186`). `Circuit::dependencies()` returns only `ScanDelta`
  sources — `ScanTrace` tids are deliberately excluded
  (`gnitz-core/src/circuit.rs:49-56`), and `ScanTrace` is full wire
  vocabulary usable by the circuit-builder API (`circuit.rs:146`, builder
  method `trace_scan` at `:266`). The client-side dist-prefix accessor mirror
  exists (`table_flags_dist_prefix`, `gnitz-wire/src/catalog.rs:496`).
  `RelationKind` is 1-byte niche-packed with one dataful variant and a size
  assert (`dag/mod.rs:109-124`) — adding a second bool-carrying variant makes
  it 2 bytes, so LRU-ness must not ride on the enum.

## Design

### 1. Skeleton shards

A **skeleton schema** is the PK-only projection of the view schema:
`SchemaDescriptor` over the same PK columns (same `pk_stride`), zero payload
columns. A skeleton shard is a standard shard file serialized under the
skeleton schema — regions `[pk, weight, null, blob]` where the null region is
all-zero (collapses to `ENCODING_CONSTANT`) and the blob region is empty —
with a new header flag (carried via `ShardWriteOpts.flags`):

```rust
// storage/repr/layout.rs
pub const SHARD_FLAG_SKELETON: u8 = 0x02;
```

`MappedShard::open` reads the flags byte **before** deriving the region
count: when `SHARD_FLAG_SKELETON` is set, region derivation uses the skeleton
projection (`num_non_pk = 0`) and the struct records `skeleton: true`. The
positional decode is otherwise unchanged. `MappedShard` additionally exposes
`payload_cost_bytes()` = Σ(null + payload + blob region sizes) for capacity
accounting (region offsets/sizes are already parsed at open); 0 for skeleton
shards.

**Skeleton weights are non-negative, and coarse-zero ghosting is exact.**
This rests on a named invariant this plan makes load-bearing:

> **Fold totality.** Every merge that can see a skeleton row folds a per-key
> *time-prefix* of the family's history, cut at a tick boundary: L0 is
> consumed whole, a guard fold takes all of the guard's entries, a vertical
> takes the whole source guard plus every overlapping destination guard, and
> the spill that creates disk shards writes the entire in-memory set
> atomically at flush time (anchors in Ground facts). Level movement always
> takes everything at a level for a key range downward, so deeper = strictly
> older per key.

A fold's per-PK coarse sum is therefore the PK-projection of the view
integral at some tick boundary restricted to that prefix — ≥ 0 by positivity
(a retraction's balancing insertion is always older, i.e. inside the prefix,
never younger). Consequently: skeleton rows never carry negative weights, and
a coarse-zero group inside a fold means every (PK, payload) in the prefix
cancelled exactly, so the ghost gate (`net_weight != 0`) dropping it is
exact — younger rows above the prefix are untouched and consolidate
independently at read time. A `debug_assert!(w >= 0)` on every skeleton row a
fold emits is the tripwire: any future *partial* compaction (size-tiered
shard subsets, partial spill) that breaks fold totality trips it instead of
silently corrupting LRU views.

### 2. Coarsened merging

Sources gain `is_skeleton(&self) -> bool` (`CursorSource::Shard` reads the
`MappedShard` flag; `CursorSource::Batch` reads a new `Batch` skeleton bit
set only by skeleton materialization below; memtable runs are never
skeleton). Two comparator amendments, applied **unconditionally** at all
comparator sites (the `merge_same_pk`/`merge_eq_payload` builders
`run_merge_body` seats, `repr/merge.rs:523-591`; `heap_less_with`,
`drive_with_inner`, `drive_pair_inner` in `read_cursor/mod.rs:132, 615, 652`)
— they are no-ops when no source is skeleton, so non-LRU tables are
byte-identical:

- **Ordering** (PK ties): a skeleton row sorts **before** any hydrated row;
  two skeleton rows tie (stable by source index).
- **Grouping**: `eq_payload` returns `true` whenever `same_pk` holds and
  either row is skeleton.

That is the whole coarsening mechanism: `drive_merge` compares against the
group exemplar (`heap.rs:290`), the exemplar is the skeleton row (ordering),
and every same-PK row folds into it regardless of payload (grouping) —
PK groups containing a skeleton row collapse to one skeleton row with the
summed weight; hydrated-only groups fold by (PK, payload) exactly as today.
The pending-group drain itself is untouched, preserving the single-drain-
owner rule. Materializing a group whose exemplar is skeleton emits PK +
weight only; a stray payload read on a skeleton shard panics loudly
(`get_col_ptr`'s slice bounds, `shard_reader/access.rs:118-143`;
`col_ptr_by_logical` returns null on OOB, `access.rs:148-172`), and a
`debug_assert!(!source.is_skeleton())` lands at both as the tripwire.

**Output representation is per destination guard.** `compact_routed`
(`compact/merge.rs:63`) already routes each surviving group to its
destination guard (`find_guard_for_key`, `compact/merge.rs:43`, routed at
`:102`) before materializing per-guard slices (`:110-160`). It gains a
per-guard representation lookup instead of a single mode: a slice destined
for a **dehydrated** guard is materialized under the skeleton schema (PK +
weight scatter only — this also strips hydrated-only groups sinking into the
guard, which is exactly the intended aging) and written with
`SHARD_FLAG_SKELETON`; slices for hydrated guards materialize as today.
Guards within a level are disjoint key ranges and skeleton shards exist only
inside dehydrated terminal-level guards, so a hydrated guard's slice can
never contain a skeleton input row — verticals over mixed destination guards
(`index.rs:368-384`) are handled correctly by construction, with the
unconditional comparator doing the grouping.

Uniformity invariants: L0 and every non-terminal level are always hydrated;
only guards of the **terminal level** (the code's own predicate
`level_num == MAX_LEVELS - 1`, i.e. L2 — `index.rs:272, 423`) can be
dehydrated; `LevelGuard::dehydrated()` is **derived** from its entries'
shard flags (no separately persisted bit, so the state survives any future
manifest reload unchanged); a guard is uniformly skeleton or uniformly
hydrated (folds and routed slices always rewrite whole guards).

### 3. Recency (the LRU signal)

Already present in storage, no change needed: `current_lsn` bumps on every
ingest and the ephemeral spill registers each disk shard with
`max_lsn = current_lsn − 1` (anchors and pinned tests in Ground facts), and
compaction propagates entry LSN bounds through every fold. Guard recency =
max over entries of `max_lsn`; "oldest guard" is well-defined.

### 4. Capacity accounting and the dehydration sweep

`Table` gains `lru_capacity: Option<LruCapacity>`:

```rust
// storage/lsm/table/mod.rs
#[derive(Clone, Copy)]
pub enum LruCapacity { Bytes(u64), Rows(u64) }
```

threaded exactly like `persistence`: catalog decode →
`build_partitioned_storage` → `PartitionedTable::new` → `Table::new` →
stored on `Table` and handed to `ShardIndex`. Per-partition share =
`capacity / NUM_PARTITIONS` for `Routing::Hashed` (hash skew tolerated and
documented) and the full capacity per partition table for
`Routing::Replicated` (one partition per worker, `hooks.rs:180-186` — a
replicated LRU view spends up to capacity × workers cluster-wide;
documented).

`ShardIndex` keeps a cached `resident_cost: u64` — Σ over non-skeleton shard
entries of `payload_cost_bytes()` (Bytes) or row count (Rows; physical rows,
so a key transiently duplicated across levels counts per copy — documented)
— maintained at both entry-install chokepoints (`ShardIndex::add_shard`,
`index.rs:42-47`, which the durable flush and the spill route through via
`persist_l0_run`, `flush.rs:261`; and the compaction-output install,
`open_outputs`, `index.rs:163-177`) and at every entry-removal site
(`commit_l0_to_l1`, guard folds, verticals, deferred deletions).

**The sweep creates its own pressure.** File-count compaction thresholds
(`shard_index/mod.rs:34-37`) never fire on byte volume, so over-capacity
data can otherwise sit in L0/L1 indefinitely. `enforce_lru_capacity`:

```rust
// storage/lsm/shard_index/index.rs
/// While resident_cost exceeds the share:
///  1. if a hydrated terminal-level guard exists, dehydrate the one whose
///     recency (max over entries of max_lsn) is oldest: fold its entries
///     through the skeleton representation into one skeleton shard;
///  2. else if L0 is non-empty, run the L0→L1 merge-and-route;
///  3. else if a non-terminal guard exists, vertical the one with the
///     largest resident payload bytes toward the terminal level;
///  4. else stop (only the in-memory head remains — out of scope).
pub fn enforce_lru_capacity(&mut self, cap: LruCapacity) { ... }
```

Steps 2/3 reuse `merge_and_route` / `compact_guard_vertical` with explicit
levels — no new merge machinery. Triggers: the end of `Table::flush` (which
`drive_dag` runs once per dirty view per tick, `dag/mod.rs:1358-1361` — so
the moment a tick pushes the view over budget is itself an enforcement
point) and inside `compact_if_needed` after `run_compact`
(`table/mod.rs:660-669`, reached from the spill, `flush.rs:394`). Both are
cheap cached-counter compares when under budget. A view that is over budget
and then never ticks again was enforced on its last tick; there is no
starvation window.

**Testability**: the in-memory ceiling override becomes runtime-configurable
— `GNITZ_EPHEMERAL_CEILING_BYTES` env var read where the constant is
consumed (`INMEM_CEILING`, `table/mod.rs:47`; the `#[cfg(test)]` override —
field `table/mod.rs:246`, setter `:470-473`, getter `flush.rs:308-316` — is
deleted in favor of it) — so E2E tests reach the disk regime with small
data. It applies to all ephemeral tables uniformly.

### 5. The hydration plan (compiled once per LRU view)

The engine compiler, when the view is LRU, constructs a companion
`HydrationPlan` after emitting the main program, stored on the cached plan
(`CachedPlan` gains the field; all three `CompileOutput` construction arms —
`compiler/mod.rs:324, 408, 558` — thread it):

```rust
// query/compiler/mod.rs
pub struct HydrationPlan {
    pub program: Program,            // JoinDT? → Map/Filter chain → Halt; tables: empty
    pub owned_funcs: Vec<ScalarFunc>,      // backing store; program.funcs points here
    pub owned_expr_progs: Vec<ResolvedProgram>,
    pub in_reg: u16,                 // seeded with the σ_K batch
    pub trace_bind: Option<(u16, usize)>,  // join: probe-side owned_tables idx
    pub seed_bind: HydraBind,        // where σ_K rows are collected from
    pub out_reg: u16,
}
pub enum HydraBind {
    OwnedTrace(usize),   // VmHandle.owned_tables[idx] (join seed side)
    Relation(i64),       // dag.tables[tid] store (linear seed)
}
```

Construction is **structural**, matching the circuit the planner actually
emits:

- **Join final**: walk back from the sink through the post-Union chain
  (`Filter`s and `Map`s only) to a `Union` whose two input branches are each
  `JoinDT` optionally followed by one normalization `Map`, the two JoinDTs
  cross-wired over the same two `IntegrateTrace` nodes. Pick the branch whose
  JoinDT's **delta** input descends from the left source. The hydration
  program is: that branch's `JoinDT` (delta register = `in_reg`, trace
  register = `trace_bind`), that branch's normalization `Map` if present,
  then the post-Union chain to the sink — the `Union` itself and the entire
  other branch are omitted. `seed_bind` = the `IntegrateTrace` feeding the
  *other* branch's JoinDT trace port resolved to its `owned_tables` index —
  i.e. the trace integrating the **left** reindex; `trace_bind` = the right
  trace's index. (One product suffices: hydration computes *state*, not a
  delta — the two-term Union exists only for incremental maintenance.)
- **Linear final**: `seed_bind = Relation(source_tid)`; the program is the
  `Filter`/`Map` chain downstream of `ScanDelta`. The filter **is** replayed:
  over a multi-row-per-key source only a subset of a key's rows may pass, and
  replay must reproduce exactly that subset.
- Registers renumber densely; `ScalarFunc`s / expression programs are cloned
  into the owned vecs and `program.funcs` rebuilt over them; `reg_meta`
  carries the schemas; `tables` stays empty (no `Integrate` — pure by
  construction).
- Any structure mismatch (a `WeightClamp`, `Reduce`, `JoinDTRange`, `Delay`,
  a Union not of the two-JoinDT shape, an unexpected extra node) →
  **compile error**, and for LRU views compile errors are fatal to the DDL:
  `hook_view_register` compiles LRU views eagerly and returns `Err` on
  failure (today `ensure_compiled` failures only skip the backfill,
  `dag/mod.rs:797-808, 963`; `hooks.rs:445` — that stays true for plain
  views). This is the trust-boundary re-check: planner under-rejection
  becomes a loud DDL failure, never a silently-empty view.

**Execution** — `DagEngine::hydrate_keys(view_id, keys: &[(PkRef, i64)]) ->
Result<Batch>` (`pub(crate)`; a DagEngine method, so the private cache needs
no external accessor):

1. Open non-compacting cursors (`Table::open_cursor` — the read path must
   not mutate shard state; the compacting variant stays reserved for epoch
   trace refresh) on `seed_bind` and `trace_bind`.
2. Collect σ_K: keys arrive PK-sorted (they come from an ordered family
   walk); one forward pass, per key `for_each_pk_group_row` copying each
   consolidated row via `copy_current_row_into` into the seed batch (schema
   = the seed register's `reg_meta` schema).
3. Seed `in_reg`, bind the trace cursor, run `vm::execute_epoch` on the
   hydration program with a fresh `RegisterFile`, consolidate the output.
4. Debug tripwire: group the output by PK and assert Σ weights per PK equals
   the family's coarse skeleton weight passed in with each key.

Borrow story (verified): `ReadCursor` owns its sources via `Rc`
(`CursorSource`), `open_cursor` is `&self`, `execute_epoch` takes `&Program`
+ a fresh local `RegisterFile`, and `cache`/`tables` are sibling fields of
the same `&mut DagEngine` — no unsafe, no restructure. `JoinDT`'s cogroup
accepts multi-key deltas, so hydration is chunked only to bound the seed
batch (`HYDRATE_CHUNK_KEYS = 4096`).

### 6. Scan and seek integration

**Scan** (`scan_family`, `store_io.rs:27-37`): for an LRU family, replace
the blind `StoreHandle::full_scan` delegate with a hydrating walk over the
same merged cursor. Because of §2, the consolidated cursor already yields,
per PK: either normal hydrated rows, or exactly one skeleton row (net coarse
weight, dropped by the ghost gate when zero) whenever any source row of that
PK was skeleton. The walk copies hydrated rows verbatim into batch `H` and
pushes skeleton (PK, weight) pairs onto list `K` (the positioned row's
skeleton-ness is exposed from the committed source index, `commit_emitted`,
`read_cursor/mod.rs:528-538`); afterwards `K` is hydrated in chunks via
`hydrate_keys` into batch `R`; `H` and `R` are both (PK, payload)-sorted and
a final two-way sort-merge produces the reply batch. The whole walk runs
inside the single Scan dispatch on the single-threaded worker — the snapshot
the wire train slices is as atomic as today's. Everything downstream
(`send_scan_response`, chunk train) is untouched.

**Seek**: `seek_entry_bytes` (`store_io.rs:65-74`), on an LRU family, after
`seek_exact_live(pk)` lands on a row: if the positioned row is skeleton,
`hydrate_keys(view_id, [(pk, w)])` and return the first recomputed row in
(PK, payload) order (matching today's first-positioned-row semantics for
multi-row keys). Master side: `handle_seek` (`executor.rs:1034-1068`) gains
the drain-ticks loop `handle_scan` runs (`executor.rs:1313-1328`) for
**every view-targeted seek** — not just LRU ones. Uniform semantics (a read
sees all committed writes, exactly like scans), no LRU-vs-plain freshness
fork, and the parity tests below are sound. Base-table seeks stay
drain-free (base stores are push-aligned and already fresh). For LRU views
the drain is what makes hydration sources (including a linear view's base
store) tick-aligned with the family; worker-side, family and owned traces
are additionally updated in the same `evaluate_dag` pass, so they are
mutually consistent at every instant regardless.

### 7. Parser: token-level preprocessing, per statement

`sqlparser`/`GenericDialect` cannot parse `LRU` or `CAPACITY`. The statement
entry (`SqlPlanner::execute`, `gnitz-sql/src/lib.rs:48-57` — the single
route for SQL, capi, and py) changes from `Parser::parse_sql` to:

1. `Tokenizer::new(&dialect, sql).tokenize_with_location()?`.
2. Split the token stream into statement segments at top-level
   `Token::SemiColon` (semicolons inside string literals are inside single
   tokens, so token-level splitting is exact). Keep each segment's raw
   source span.
3. Per segment (whitespace/comment tokens skipped; all keyword matches
   require `Word { quote_style: None, .. }` and `eq_ignore_ascii_case`):
   - If the segment starts `CREATE LRU VIEW`: remove the `LRU` token; scan
     forward, before the first unquoted `AS` keyword, for
     `CAPACITY <Number> <unit>` — the number must be a plain unsuffixed
     integer, the unit an unquoted word in {KB, MB, GB, ROWS}; remove the
     three tokens and record `LruSpec { capacity: LruCapacity }` for this
     segment. Missing/malformed clause → targeted `GnitzSqlError::Parse`.
   - If the segment starts `CREATE [LRU] VIEW` **without** a valid LRU
     prefix but contains an unquoted `CAPACITY` word before `AS` → targeted
     error ("CAPACITY requires CREATE LRU VIEW").
4. Parse each segment with
   `Parser::new(&dialect).with_tokens_with_locations(segment)` →
   `parse_statement()`; non-LRU statements are byte-identical in behavior to
   today's `parse_sql`.
5. Each statement carries its own `Option<LruSpec>` into `dispatch.rs`; the
   **original** (unstripped) segment text is what flows into VIEW_TAB's
   `sql_definition`, so introspection shows the real DDL.

Quoted identifiers named `"LRU"`/`"CAPACITY"`/`"AS"` are untouched
(quote-style check); `lru`/`capacity`/`rows` remain ordinary identifiers
everywhere else; the rewrite never reaches into the `AS <query>` body.

### 8. Planner: eligibility, leaf rule, threading

- **Coarse gate** in `execute_create_view`, right after `classify`
  (`dispatch.rs:226`, called at `:67`): LRU + shape ∉ {Simple, Join} →
  `Unsupported` — one check covers SetOp, Distinct, GroupBy, and all three
  subquery flavors (Subquery, MarkSubquery, ScalarSubquery) uniformly, since
  every arm flows through the shared match. For Simple, classify the source:
  base table → require dist prefix = full PK (via the client mirror
  `table_flags_dist_prefix`) or replicated; registered view → require not
  LRU; unresolved as either (a chain-local hidden segment vid — no
  VIEW_TAB/TABLE_TAB row exists yet) → eligible (hidden segments are never
  LRU and their families hash by full PK, so co-location holds).
- **Join final-step check** lives inside `plan_join_chain` at `is_last`
  (`join.rs:486,509`), where `extract_join_predicates` has classified the
  conjuncts: `lru.is_some() && (join_type != Inner || n_range > 0)` →
  `Unsupported`. `plan_join_chain` gains the `lru: Option<LruCapacity>`
  parameter; only the final segment sees it. (INNER-ness alone is
  AST-visible earlier, but range-conjunct knowledge exists only here —
  duplicating conjunct classification at dispatch would be drift by
  construction.)
- **Leaf rule, client side**: scoped to CREATE VIEW planning, not the shared
  `Binder::resolve` (which also serves plain SELECT and must keep resolving
  LRU views). The view-planning paths that resolve FROM relations (linear
  lowering, join alias maps, exists inners, set-op sides, CTE/derived
  compilation) check each resolved **registered** view against a new
  `GnitzClient::view_lru(vid)` (reads the new VIEW_TAB flags column; a
  VIEW_TAB miss — hidden chain-local vid — is not LRU) and reject:
  `"'{name}' is an LRU view; views cannot be created over LRU views"`.
- **Threading**: only the final `PlannedView` of the chain carries
  `lru: Option<LruCapacity>`; `create_view_chain` writes it into the final
  view's VIEW_TAB row. Hidden segments never carry it.

### 9. Catalog and wire plumbing

- **VIEW_TAB gains two columns** (pre-alpha, no migration): col 7
  `flags: u64` (bit0 = LRU, bit1 = capacity-unit-is-rows), col 8
  `capacity: u64` — appended **once** to `gnitz_wire::VIEW_TAB_COLS`
  (`gnitz-wire/src/catalog.rs:80-90`). The engine `SchemaDescriptor`,
  bootstrap's COL_TAB self-description rows, the client `Schema`, and the
  wal-block region count all derive from that slice, so they update
  automatically and cannot drift. Hand-touched sites: two new derived
  constants `VIEWTAB_PAY_FLAGS` / `VIEWTAB_PAY_CAPACITY` next to the
  existing `VIEWTAB_PAY_*` (`catalog/sys_tables.rs:171-173`); the single
  client row writer `append_view_row` + `ViewRecord` (shared by the `+1`
  create at `client.rs:995-1007` and the `-1` drop retraction at
  `client.rs:1063-1067`, which therefore stay byte-matched by construction;
  `ViewRecord` at `:132-140`) with `decode_view_record` (`client.rs:1259`)
  and `resolve_table_or_view_id`'s decode (`client.rs:1085`);
  circuit-builder-API views write `flags = 0`; and the test builders
  (`catalog/tests/mod.rs:124-138` `build_view_tab_row`;
  `runtime/protocol/wire.rs:1555-1566`).
- **Engine registration**: `hook_view_register` decodes flags + capacity.
  `RelationKind` is **unchanged** (a second dataful variant would break the
  1-byte niche assert, `dag/mod.rs:124`); LRU-ness and capacity live on
  `TableEntry` as `lru_capacity: Option<LruCapacity>`, threaded as a
  parameter through `build_partitioned_storage` → `PartitionedTable::new` →
  `Table::new`, mirroring `persistence`. Trust-boundary re-checks in the
  hook (pattern of the REPLICATED×dist-prefix guard, `hooks.rs:242-248`): an
  LRU flag on a hidden-prefixed name, or a capacity without the flag,
  rejects the DDL; and — per §5 — an LRU view that fails to compile rejects
  the DDL.
- **Leaf rule, engine side** (two independent backstops closing both source
  kinds):
  1. The view-dep precheck (`write_path.rs:498-516` neighborhood) rejects
     any DEP_TAB `+1` row whose `dep_table_id` resolves to a registered
     LRU-view `TableEntry`. (Covers every `ScanDelta` source of every
     circuit, including subquery inners; within-bundle hidden deps are
     unresolvable at precheck time and are never LRU.)
  2. `ScanTrace` sources produce **no** DEP_TAB row (`circuit.rs:49-56`), so
     additionally: the same precheck walks the batch's circuit-node `+1`
     rows and rejects a `ScanTrace` opcode whose tid is a registered LRU
     view, and the engine compiler's ext-trace resolution
     (`build_ext_cursors` / the `ScanTrace` emit arm) fails compilation when
     the tid is an LRU view — closing the circuit-builder-API path
     (`trace_scan`, `circuit.rs:266`) for views compiled later.
- **DROP / boot / backfill**: unchanged mechanics. DROP of an LRU view is
  never dep-blocked (leaf). Boot re-derives the family hydrated (backfill
  replays *sources* through the circuit — `backfill_view` opens source
  stores only, `ddl.rs:386-409`; a skeleton family is never replayed as a
  source, by leaf-ness) and the sweep re-dehydrates as ticks/spills run —
  transiently over-capacity during backfill, bounded by the per-flush
  enforcement.

## Why the recompute is exact

For any eligible view, output-at-key `k` equals the replay:

- Restriction to a key is linear and time-invariant, so it commutes with
  integration: σ_k(I(s)) = I(σ_k(s)). The traces are integrals of everything
  upstream of the join (reindex, IS-NOT-NULL and pushed filters), so
  σ_k(trace) is the complete upstream history at `k`.
- The inner equi-join is per-key: σ_k(A ⋈ B) = σ_k(A) ⋈ σ_k(B), computed as
  **one** product over the two trace integrals — the compiled circuit's
  two-term Union exists only to produce *deltas* incrementally; the state
  they integrate to is the single product, which is what hydration computes.
  Filter/Map commute with σ_k pointwise. Hence
  `family|_k = σ_k(Q(inputs)) = hydration(k)` whenever family and traces are
  aligned at the same tick — which the scan drain guarantees and the
  view-seek drain extends to seeks.
- Weights: `JoinDT` emits products of the net trace weights the
  consolidating cursors expose; recomputed rows carry their true
  multiplicities independently of the skeleton, whose coarse weight is only
  a consistency witness (Σ recomputed = coarse, by linearity of the PK
  projection — debug-asserted).
- The family's coarse per-PK bookkeeping is exact because PK-projection is
  linear, and its partial-fold behavior is exact by fold totality +
  positivity (§1).

## Files touched

| File | Change |
|---|---|
| `crates/gnitz-sql/src/lib.rs` | per-statement token preprocessor |
| `crates/gnitz-sql/src/dispatch.rs`, `plan/view/dispatch.rs`, `plan/view/join.rs` | `LruSpec` threading; coarse gate; final-step check in `plan_join_chain` |
| `crates/gnitz-sql/src/plan/view/*` (view-planning resolution sites) | leaf-rule rejection of LRU sources |
| `crates/gnitz-wire/src/catalog.rs` | VIEW_TAB_COLS +2 columns (single source for engine + client + bootstrap) |
| `crates/gnitz-core/src/types.rs`, `client.rs` | `append_view_row`/`ViewRecord`/decoders; `view_lru` + dist-prefix accessors; `PlannedView.lru` |
| `crates/gnitz-engine/src/catalog/{hooks,sys_tables,write_path}.rs` | new `VIEWTAB_PAY_*` constants; decode + re-validation; eager LRU compile with `Err`; DEP_TAB + circuit-node ScanTrace leaf prechecks |
| `crates/gnitz-engine/src/query/dag/mod.rs` | `TableEntry.lru_capacity`; `hydrate_keys`; ext-trace LRU compile rejection |
| `crates/gnitz-engine/src/query/compiler/{mod,emit}.rs` | structural `HydrationPlan` construction; field on `CachedPlan` through all three `CompileOutput` arms |
| `crates/gnitz-engine/src/storage/repr/{layout,batch_wire}.rs`, `storage/lsm/shard_reader/*` | `SHARD_FLAG_SKELETON`; skeleton open; `payload_cost_bytes` |
| `crates/gnitz-engine/src/storage/repr/{merge,heap}.rs`, `storage/lsm/read_cursor/*` | unconditional skeleton-aware ordering + grouping; skeleton exposure on the committed row |
| `crates/gnitz-engine/src/storage/lsm/compact/merge.rs`, `shard_index/*` | per-dest-guard representation; derived `dehydrated()`; `resident_cost`; `enforce_lru_capacity` with push-down |
| `crates/gnitz-engine/src/storage/lsm/{table/mod.rs,table/flush.rs,partitioned_table.rs}` | `LruCapacity` threading; sweep triggers; `GNITZ_EPHEMERAL_CEILING_BYTES` |
| `crates/gnitz-engine/src/catalog/store_io.rs` | hydrating scan walk in `scan_family`; skeleton-aware `seek_entry_bytes` |
| `crates/gnitz-engine/src/runtime/orchestration/executor.rs` | view-targeted seeks drain ticks |
| `CLAUDE.md` | record the LRU view surface, the capacity contract, and the fold-totality invariant |

## Testing

- **Storage units**: skeleton shard write/open round-trip (flag, region
  derivation, `payload_cost_bytes = 0`); coarsening merge across all three
  consolidation paths — mixed skeleton/hydrated inputs, coarse-zero group
  ghosted, the non-negative-skeleton `debug_assert` exercised; per-dest-guard
  representation in a vertical spanning one dehydrated and one hydrated
  destination guard (hydrated guard output byte-identical to today);
  `enforce_lru_capacity` victim ordering on the real spill-stamped LSNs
  (already pinned: `current_lsn_bumps_on_every_ingest_including_rederive`,
  `max_lsn == current_lsn - 1`), push-down steps 2/3 firing when the terminal
  level is empty, and cached `resident_cost` accuracy for both units across
  add/fold/vertical/removal.
- **Compiler units**: structural `HydrationPlan` construction on a real
  planner-emitted equi-join circuit (two JoinDTs + Union) and a linear
  circuit; clean compile failure on every ineligible node kind and on a
  malformed Union shape.
- **Planner units**: preprocessor (LRU stripped; CAPACITY parsed for all
  four units; mandatory-clause error; `CAPACITY` without `LRU` targeted
  error; quoted `"LRU"`/`"CAPACITY"` identifiers untouched; multi-statement
  strings with the LRU statement in each position; `lru`/`capacity` as
  ordinary identifiers; suffixed/decimal number rejected; original text
  stored in `sql_definition`); eligibility matrix (each rejected shape,
  covering all three subquery flavors at the coarse gate; CLUSTER BY source;
  LRU-over-LRU; range conjunct on the final join step rejected inside
  `plan_join_chain`; Simple over a derived table accepted); VIEW_TAB row
  carries flags + capacity and DROP retraction cancels it.
- **E2E** (`GNITZ_WORKERS=4` and `WORKERS=1`, with
  `GNITZ_EPHEMERAL_CEILING_BYTES` set small so families reach disk):
  - **Parity**: an LRU join view (`CAPACITY 1 KB`) and its plain twin over
    the same churn stream (inserts, updates, deletes on both sides, enough
    volume to spill and compact) — scans identical at every step; point
    seeks on cold and hot keys identical (sound now that all view seeks
    drain).
  - **Parity, linear**: LRU filter/projection view over a TEXT-heavy table;
    also over a non-LRU view source.
  - **Capacity semantics**: `CAPACITY <huge>` never dehydrates (on-disk
    bytes match the plain twin); a small byte capacity drives
    `resident_cost` under the per-partition share after sustained ingest
    (asserted via directory size — a fully-dehydrated TEXT view's family is
    a small fraction of the plain twin's); ROWS unit enforces the same way
    on row accounting. No "newest N rows stay hydrated" assertion — the
    contract is the bound, not per-row residency.
  - **Leaf rule**: CREATE VIEW over an LRU view rejected (direct FROM, join
    side, subquery inner); DROP works; a hand-built circuit-API view with
    `trace_scan(lru_view)` rejected at DDL.
  - **Seek drain**: push rows, seek an LRU view and a plain view immediately
    (no intervening scan) — both reflect the push.
  - **Boot**: restart re-derives, contents equal; capacity re-enforced after
    post-boot ticks.
  - **Backfill**: CREATE LRU VIEW over pre-populated tables equals the
    fresh-data run.

## Invariants preserved

- Element identity stays (PK, payload) everywhere except inside PK groups
  containing a skeleton row, where identity coarsens to PK — sound because
  the coarse weight is the linear PK-projection of the exact Z-set, partial
  folds are per-key time-prefixes at tick boundaries (fold totality, §1,
  debug-asserted), and the payload breakdown is re-derivable from the
  traces, which remain exact.
- The write path (push → tick → family ingest) is byte-identical for LRU and
  plain views; dehydration happens only in compaction/sweep, hydration only
  in scan/seek.
- Non-LRU views and all base tables: no behavioral change except
  view-targeted seeks now draining ticks (a strict freshness improvement
  aligning seeks with scans).
- `drive_merge` remains the sole pending-group drain owner; coarsening is
  expressed entirely through the boundary predicates it already consumes.
- Guard dehydration state is derivable from shard headers alone — no new
  manifest or catalog state, so future durability changes to ephemeral
  tables inherit it for free.

## Sequencing

1. **Storage core**: skeleton schema/flag/open + `payload_cost_bytes`;
   unconditional comparator amendments; per-dest-guard representation in
   `compact_routed`; the fold-totality tripwire (+ units). Inert: no table
   carries a capacity.
2. **Capacity**: `LruCapacity` threading to `Table`/`ShardIndex`;
   `resident_cost`; `enforce_lru_capacity` with push-down; flush/compact
   triggers; `GNITZ_EPHEMERAL_CEILING_BYTES` (+ units). Still inert.
3. **Catalog plumbing**: VIEW_TAB columns end-to-end (two columns in
   `VIEW_TAB_COLS` + the derived `VIEWTAB_PAY_*` constants + client
   writer/decoders + test builders — engine schema and bootstrap COL_TAB
   derive automatically); `TableEntry.lru_capacity` threading; DEP_TAB +
   circuit-node ScanTrace leaf prechecks; hook re-validation (+ units).
   Still inert: no SQL surface, circuit-API writes flags = 0.
4. **Hydration**: structural `HydrationPlan` + `hydrate_keys`; hydrating
   scan walk; skeleton-aware seek; view-seek drain; eager LRU compile with
   DDL rejection; ext-trace LRU compile rejection (+ units). Still inert.
5. **SQL surface**: per-statement preprocessor; coarse gate + final-step
   check + leaf rule; `PlannedView.lru` → `create_view_chain`; the full E2E
   suite.

Each step compiles and passes `make verify` + `make e2e` on its own.
