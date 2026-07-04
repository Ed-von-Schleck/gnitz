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
   `ingest_borrowed_batch`, `query/dag/mod.rs:1003-1015`,
   `table/mod.rs:331-359` — consolidate + memtable append only). Payloads are
   read exclusively by client scans/seeks and by compaction; the only other
   family reader, `gather_family_bytes`, is issued solely by DML preflight
   against base tables (`master/preflight.rs:469,659`). Dehydration therefore
   never touches delta processing.
2. **Every SQL view output is a positive Z-set** (each (PK, payload) at net
   weight ≥ 0 at tick boundaries): every net-negative-capable construction is
   clamped — the only `negate` call sites reachable from SQL are internal
   subtrahends of `positive_part`/bag-min/threshold differences
   (`plan/view/set_op.rs:283-292,311`, `exists.rs:391-395,593`,
   `join.rs:559,1107,1157,1178`). Hence a PK whose summed weight is 0 at a
   tick boundary has no rows at all.

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
  (existing `EPHEMERAL_INMEM_CEILING = 4 MiB` per partition, `table/mod.rs:29`)
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
  Everything else — grouped, DISTINCT, set-ops, EXISTS/IN (semi/anti) finals —
  is rejected with `Unsupported("CREATE LRU VIEW: <shape> is not supported")`.
  The rule behind the list: the segment downstream of its integrated traces
  must be **stateless-replayable** (JoinDT/Filter/Map/Union-of-the-two-join-
  terms only — no WeightClamp, no Reduce, no JoinDTRange, no Delay).

## Ground facts this design builds on (verified against the live tree)

- **The compiled inner equi-join circuit has TWO JoinDT terms.** The planner
  emits `join_ab = ΔA ⋈ trace_b` and `join_ba = ΔB ⋈ trace_a`
  (`plan/view/join.rs:524-525`, traces built at `join.rs:493-523`), each
  followed by a normalization Map, merged by `cb.union(..)` in
  `normalize_to_ab` (`join.rs:73-96`), then optional residual/WHERE `Filter`
  (`join.rs:676-685`) and projection `Map` (`join.rs:705-709`) into the sink.
  No SQL join view emits a `ScanTrace` side today.
- **The terminal LSM level is L2.** The only production vertical call is
  `compact_guard_vertical(1)` from `run_compact`
  (`shard_index/index.rs:158-163`); the code's own terminal predicate is
  `level_num == MAX_LEVELS - 1` (`index.rs:225-234`). Levels deeper than L2
  never hold data.
- **Every skeleton-visible fold is a per-key time-prefix at a tick boundary.**
  `run_compact` consumes all of L0 (`index.rs:129-152`); `compact_one_guard`
  folds all entries of a guard (`index.rs:252-289`); verticals fold the whole
  source guard plus every overlapping destination guard
  (`index.rs:291-410, 324-346`); the ephemeral spill consolidates and writes
  the entire in-memory set atomically at flush time (`table/flush.rs:338-396`).
  Level movement always takes everything at a level for a key range downward,
  so per key the level split is a time-prefix split.
- **Ephemeral shards carry no LSNs today**: the spill registers
  `add_shard(&final_full, 0, 0)` (`table/flush.rs:380`) and `current_lsn`
  bumps only for Durable ingest (`table/mod.rs:343-345`) — there is no
  recency signal on family shards yet.
- Family write path: `drive_dag` ingests at `query/dag/mod.rs:1177/1180` and
  flushes each dirty view once per tick (`dag/mod.rs:1205-1207`). Ephemeral
  flush pushes to `in_memory_l0` (`flush.rs:90-99`); disk shards appear via
  spill past the ceiling, which then runs `compact_if_needed` (`flush.rs:394`).
  Ephemeral tables have no manifest (`table/mod.rs:267-276`) and no WAL. The
  in-memory ceiling override is `#[cfg(test)]`-only today
  (`table/mod.rs:220-221,446-447`).
- Scan: master `handle_scan` drains all pending ticks before dispatch
  (`executor.rs:1206-1221`); workers materialize the whole family atomically
  inside one dispatch (`store_io.rs:487-493` → `materialize`,
  `read_cursor/output.rs:154-166`); the chunked wire train slices an immutable
  `Rc<Batch>` (`worker/reply.rs:294-302`). Seek: `seek_family_bytes`
  (`store_io.rs:55-76`) copies the single positioned row; master `handle_seek`
  (`executor.rs:927-961`) does **not** drain ticks today.
- Consolidation identity is uniformly (PK, payload); the single pending-group
  drain owner is `drive_merge` (`repr/heap.rs:232-301`, boundary
  `same_pk && eq_payload` at `:290`, emit gate `net_weight != 0` at `:297`,
  comparison against the group exemplar); comparators live in
  `run_merge_body` (`repr/merge.rs:574-588`) and the cursor drives
  (`read_cursor/mod.rs:143-158, 628-655, 669-743`). Every payload comparison
  a family cursor or compaction can perform funnels through these sites.
- Shard format: header FLAGS byte at `OFF_FLAGS = 56`
  (`storage/lsm/layout.rs:23`), only `SHARD_FLAG_PK_UNIQUE = 0x01` used;
  the reader derives `num_regions = 3 + num_payload + 1` from the schema and
  indexes the directory positionally (`shard_reader/open.rs:37-43, 144-209`);
  per-region encodings (RAW/CONSTANT/TWO_VALUE) are self-describing.
  Zero-payload (all-PK) `SchemaDescriptor`s already exist and work
  (`shard_index/mod.rs:981-1002`). `write_as_shard_with_flags` exists
  (`repr/batch_wire.rs:33`). `add_opened_shard` (`index.rs:105-109`) is the
  chokepoint both `add_shard` and durable `flush_commit` route through.
- Plans and traces at scan time: the scan path is `&mut CatalogEngine`;
  `self.dag` (`catalog/mod.rs:101`) reaches the private plan cache
  `cache: FxHashMap<i64, CachedPlan>` and `tables` (`dag/mod.rs:283-288`).
  Join views own their input traces as `_int_{view}_{nid}` child tables in
  `VmHandle.owned_tables` / `owned_trace_regs` (`compiler/emit.rs:530-550`,
  `vm/mod.rs:169,180`). There is exactly one compile site
  (`compile_view_internal`, `dag/mod.rs:845-861`) serving live CREATE, boot,
  and worker sync; `CompileOutput` is constructed in three arms
  (`compiler/mod.rs:323, 407, 557`). `ensure_compiled` returns `false` on
  compile failure and `hook_view_register` today only logs it
  (`dag/mod.rs:712-723, 879`; `hooks.rs:500-507`).
- VM: `execute_epoch(&Program, &mut RegisterFile, input_batch, input_reg,
  output_reg, cursor_handles, owned_trace_reg_ids)` (`vm/exec.rs:20-28`);
  arbitrary registers are seedable (`execute_epoch_multi`, `exec.rs:47-83`);
  `JoinDT` is pure (delta × trace cursor → `[left_PK | left payload | trace
  payload]` at products of net weights, `ops/join/delta_trace.rs:23-57`), as
  are `Filter`/`Map`/`Union`; the only Table writers are `Integrate` with a
  non-null table and `WeightClamp` with a history table (`exec.rs:268-291,
  354-381`). `Program.funcs` holds raw `*const ScalarFunc` pointers
  (`vm/mod.rs:286`); `RegisterFile::new` takes `&[RegisterMeta]`
  (`vm/mod.rs:321-339`). `CursorHandle` is Rc-owning with no borrow lifetime
  (`read_cursor/source.rs:20-26`); `Table::open_cursor` is `&self`
  (`table/mod.rs:383-393`); `for_each_pk_group_row`
  (`read_cursor/mod.rs:431-456`) walks one PK group; `copy_current_row_into`
  (`read_cursor/output.rs:101-115`) relocates German-string blobs.
- Co-location: the join exchange scatter and the family ingest route by
  `partition_for_pk_bytes` over the identical packed, width-promoted
  `_join_pk` OPK bytes (`ops/exchange/router.rs:295-310`,
  `schema/key.rs:116-144`, `ops/reindex.rs:323-399`) — worker-local by the
  OPK hash invariant. Simple views force-prepend the source PK verbatim
  (`plan/lp.rs:83`, `codec/project_schema.rs:74-96`), so view PK = source PK
  byte-identically. NULL join keys are excluded by planner `IS NOT NULL`
  filters upstream of the reindex/traces (`join.rs:474-514`), so traces,
  family, and hydration see the identical post-filter universe.
- Parser: `sqlparser` 0.56 / `GenericDialect`; the single statement entry is
  `SqlPlanner::execute`, which parses N statements per string
  (`gnitz-sql/src/lib.rs:48-57`) and is the route for capi
  (`gnitz-capi/src/lib.rs:1358,1464`) and py (`gnitz-py/src/lib.rs:1714`);
  the only other `Parser::new` sites are `#[cfg(test)]` helpers.
  `Parser::parse_sql` = `Tokenizer::tokenize_with_location` →
  `with_tokens_with_locations` → `parse_statements` (sqlparser
  `parser/mod.rs:438-510`); the parser skips whitespace tokens itself, and
  comments are whitespace variants (`tokenizer.rs:452-467`); string literals
  tokenize as `SingleQuotedString`, never `Word`; `GenericDialect` has
  `supports_numeric_prefix = false` so `256MB` tokenizes as
  `Number("256")` + `Word("MB")`.
- Planner shape: `ViewShape::classify` has exactly 8 variants (Simple, Join,
  GroupBy, Distinct, SetOp, DistinctOverJoin, GroupedOverJoin, Subquery —
  `plan/view/dispatch.rs:141-172`); `Subquery` early-returns at
  `dispatch.rs:62-85` before the common tail; CTEs/derived tables compile to
  hidden segments *before* classify (`dispatch.rs:53-61`), and those
  chain-local vids have no VIEW_TAB row until `create_view_chain` commits
  (`dispatch.rs:130-132`). INNER-ness of a join step is AST-visible
  (`join_on_and_type`, `join.rs:129-145`) but range conjuncts are known only
  inside `plan_join_chain` after `extract_join_predicates` per step
  (`join.rs:318-321`, final step at `is_last`, `join.rs:321,349`).
- Catalog: VIEW_TAB is `[view_id(pk), schema_id, name, sql_definition,
  cache_directory, created_lsn, pk_col_idx]` — no flags column
  (`gnitz-core/src/types.rs:97-114`). Row writers/readers: `create_view_chain`
  (`client.rs:1005-1018`), `drop_view` rebuilds the full `-1` retraction row
  with exactly today's payload arity (`client.rs:1073-1084`; `ViewRecord` at
  `client.rs:114-122`, `decode_view_record` at `client.rs:1257-1266`),
  `resolve_table_or_view_id` (`client.rs:1115-1156`); engine side:
  `view_tab_schema()` descriptor (`catalog/sys_tables.rs:199-214`, static at
  `:269`, dispatch at `:378`) — `Batch::decode_from_wal_block` hard-rejects
  on region-count mismatch (`repr/batch_wire.rs:163-193`); bootstrap's
  self-describing COL_TAB rows for VIEW_TAB (`catalog/bootstrap.rs:170-181`);
  test builders (`catalog/tests/mod.rs:175-186`,
  `runtime/protocol/wire.rs:1546-1558`). `hook_view_register`
  (`hooks.rs:388-531`) decodes the row and builds storage via
  `build_partitioned_storage` → `PartitionedTable::new` (`hooks.rs:160-209`;
  replicated views get one partition per worker, `hooks.rs:192-198`).
  `Circuit::dependencies()` returns only `ScanDelta` sources — `ScanTrace`
  tids are deliberately excluded (`gnitz-core/src/circuit.rs:45-63`), and
  `ScanTrace` is full wire vocabulary usable by the circuit-builder API
  (`circuit.rs:133,260`). The client-side dist-prefix accessor mirror exists
  (`table_flags_dist_prefix`, `gnitz-wire/src/catalog.rs:417`).
  `RelationKind` is 1-byte niche-packed with one dataful variant
  (`dag/mod.rs:74-90`) — adding a second bool-carrying variant makes it
  2 bytes, so LRU-ness must not ride on the enum.

## Design

### 1. Skeleton shards

A **skeleton schema** is the PK-only projection of the view schema:
`SchemaDescriptor` over the same PK columns (same `pk_stride`), zero payload
columns. A skeleton shard is a standard shard file serialized under the
skeleton schema — regions `[pk, weight, null, blob]` where the null region is
all-zero (collapses to `ENCODING_CONSTANT`) and the blob region is empty —
with a new header flag:

```rust
// storage/lsm/layout.rs
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
comparator sites (`run_merge_body`'s `less`/`eq_payload`,
`repr/merge.rs:574-588`; `heap_less_with`, `drive_with_inner`,
`drive_pair_inner` in `read_cursor/mod.rs`) — they are no-ops when no source
is skeleton, so non-LRU tables are byte-identical:

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
(existing slice-index bounds, `shard_reader/access.rs:115-131,159-171`), and
a `debug_assert!(!source.is_skeleton())` lands there as the tripwire.

**Output representation is per destination guard.** `compact_routed` already
routes each surviving group to its destination guard (`find_guard_for_key`,
`compact/merge.rs:41-43`) before materializing per-guard slices
(`:100-138`). It gains a per-guard representation lookup instead of a single
mode: a slice destined for a **dehydrated** guard is materialized under the
skeleton schema (PK + weight scatter only — this also strips hydrated-only
groups sinking into the guard, which is exactly the intended aging) and
written with `SHARD_FLAG_SKELETON`; slices for hydrated guards materialize
as today. Guards within a level are disjoint key ranges and skeleton shards
exist only inside dehydrated terminal-level guards, so a hydrated guard's
slice can never contain a skeleton input row — verticals over mixed
destination guards (`index.rs:324-346`) are handled correctly by
construction, with the unconditional comparator doing the grouping.

Uniformity invariants: L0 and every non-terminal level are always hydrated;
only guards of the **terminal level** (the code's own predicate
`level_num == MAX_LEVELS - 1`, i.e. L2 — `index.rs:225-234`) can be
dehydrated; `LevelGuard::dehydrated()` is **derived** from its entries'
shard flags (no separately persisted bit, so the state survives any future
manifest reload unchanged); a guard is uniformly skeleton or uniformly
hydrated (folds and routed slices always rewrite whole guards).

### 3. Recency plumbing (the LRU signal)

Ephemeral shards carry `max_lsn = 0` today, so no victim ordering is
possible. Committed change, all tables: `current_lsn` bumps on **every**
ingest (drop the `== Durable` guard at `table/mod.rs:343-345`), and the
ephemeral spill registers real bounds — `add_shard(&final_full,
spill_min_lsn, current_lsn - 1)` where `spill_min_lsn` is the previous
spill's `current_lsn` watermark (`table/flush.rs:380`). Ephemeral tables
have no manifest or recovery path reading `current_lsn`, and durable tables'
LSNs only become denser, so nothing else changes behavior. Compaction
already propagates entry LSN bounds (`index.rs:129-132, 204-222, 331-336`).
Guard recency = max over entries of `max_lsn`; "oldest guard" is now
well-defined.

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
`Routing::Replicated` (one partition per worker, `hooks.rs:192-198` — a
replicated LRU view spends up to capacity × workers cluster-wide;
documented).

`ShardIndex` keeps a cached `resident_cost: u64` — Σ over non-skeleton shard
entries of `payload_cost_bytes()` (Bytes) or row count (Rows; physical rows,
so a key transiently duplicated across levels counts per copy — documented)
— maintained in `add_opened_shard` (`index.rs:105-109`, the chokepoint both
`add_shard` and `flush_commit` route through) and at every entry-removal
site (L0→L1 commit, guard folds, verticals, deferred deletions).

**The sweep creates its own pressure.** File-count compaction thresholds
(`shard_index/mod.rs:34-38`) never fire on byte volume, so over-capacity
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
`drive_dag` runs once per dirty view per tick, `dag/mod.rs:1205-1207` — so
the moment a tick pushes the view over budget is itself an enforcement
point) and inside `compact_if_needed` after `run_compact`
(`table/mod.rs:616-637`, reached from the spill, `flush.rs:394`). Both are
cheap cached-counter compares when under budget. A view that is over budget
and then never ticks again was enforced on its last tick; there is no
starvation window.

**Testability**: the in-memory ceiling override becomes runtime-configurable
— `GNITZ_EPHEMERAL_CEILING_BYTES` env var read where the constant is
consumed (`table/mod.rs:29` users; the `#[cfg(test)]` override at
`table/mod.rs:220-221,446-447` is deleted in favor of it) — so E2E tests
reach the disk regime with small data. It applies to all ephemeral tables
uniformly.

### 5. The hydration plan (compiled once per LRU view)

The engine compiler, when the view is LRU, constructs a companion
`HydrationPlan` after emitting the main program, stored on the cached plan
(`CachedPlan` gains the field; all three `CompileOutput` construction arms —
`compiler/mod.rs:323, 407, 557` — thread it):

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
  failure (today `ensure_compiled` failures are only logged,
  `dag/mod.rs:712-723, 879` — that stays true for plain views). This is the
  trust-boundary re-check: planner under-rejection becomes a loud DDL
  failure, never a silently-empty view.

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

Borrow story (verified): `CursorHandle` is Rc-owning, `open_cursor` is
`&self`, `execute_epoch` takes `&Program` + a fresh local `RegisterFile`,
and `cache`/`tables` are sibling fields of the same `&mut DagEngine` — no
unsafe, no restructure. `JoinDT`'s cogroup accepts multi-key deltas, so
hydration is chunked only to bound the seed batch
(`HYDRATE_CHUNK_KEYS = 4096`).

### 6. Scan and seek integration

**Scan** (`scan_store`, `store_io.rs:487-493`): for an LRU family, replace
the blind `materialize()` with a hydrating walk over the same cursor.
Because of §2, the consolidated cursor already yields, per PK: either normal
hydrated rows, or exactly one skeleton row (net coarse weight, dropped by
the ghost gate when zero) whenever any source row of that PK was skeleton.
The walk copies hydrated rows verbatim into batch `H` and pushes skeleton
(PK, weight) pairs onto list `K` (the positioned row's skeleton-ness is
exposed from the committed source index, `commit_emitted`,
`read_cursor/mod.rs:541-551`); afterwards `K` is hydrated in chunks via
`hydrate_keys` into batch `R`; `H` and `R` are both (PK, payload)-sorted and
a final two-way sort-merge produces the reply batch. The whole walk runs
inside the single Scan dispatch on the single-threaded worker — the snapshot
the wire train slices is as atomic as today's. Everything downstream
(`send_scan_response`, chunk train) is untouched.

**Seek**: `seek_family_bytes` (`store_io.rs:55-76`), on an LRU family, after
`seek_exact_live(pk)` lands on a row: if the positioned row is skeleton,
`hydrate_keys(view_id, [(pk, w)])` and return the first recomputed row in
(PK, payload) order (matching today's first-positioned-row semantics for
multi-row keys). Master side: `handle_seek` (`executor.rs:927-961`) gains
the drain-ticks loop `handle_scan` runs (`executor.rs:1206-1221`) for
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

- **Coarse gate** in `execute_create_view`, **before** the `Subquery`
  early-return (`dispatch.rs:62-85`): LRU + shape ∉ {Simple, Join} →
  `Unsupported`. For Simple, classify the source: base table → require dist
  prefix = full PK (via the new client mirror of `table_flags_dist_prefix`)
  or replicated; registered view → require not LRU; unresolved as either
  (a chain-local hidden segment vid — no VIEW_TAB/TABLE_TAB row exists yet)
  → eligible (hidden segments are never LRU and their families hash by full
  PK, so co-location holds).
- **Join final-step check** lives inside `plan_join_chain` at `is_last`
  (`join.rs:321,349`), where `extract_join_predicates` has classified the
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
  `capacity: u64`. Updated in lockstep — the full sweep:
  `gnitz-core/src/types.rs:97-114` (schema); `create_view_chain`
  (`client.rs:1005-1018`) and the single-view writers (circuit-builder API
  views write `flags = 0`); `drop_view`'s rebuilt `-1` retraction row plus
  `ViewRecord`/`decode_view_record` (`client.rs:1073-1084, 114-122,
  1257-1266` — the retraction must byte-match the `+1` row or it ghosts);
  `resolve_table_or_view_id`'s VIEW_TAB decode (`client.rs:1115-1156`); the
  engine's authoritative `view_tab_schema()` descriptor
  (`catalog/sys_tables.rs:199-214, 269, 378` — without it every CREATE VIEW
  fails the region-count check in `decode_from_wal_block`); bootstrap's
  self-describing COL_TAB rows for VIEW_TAB (`catalog/bootstrap.rs:170-181`);
  the `VIEWTAB_*` payload-index constants (`sys_tables.rs:111-114`); and the
  test builders (`catalog/tests/mod.rs:175-186`,
  `runtime/protocol/wire.rs:1546-1558`).
- **Engine registration**: `hook_view_register` decodes flags + capacity.
  `RelationKind` is **unchanged** (a second dataful variant would break the
  1-byte niche assert); LRU-ness and capacity live on `TableEntry` as
  `lru_capacity: Option<LruCapacity>`, threaded as a parameter through
  `build_partitioned_storage` → `PartitionedTable::new` → `Table::new`,
  mirroring `persistence`. Trust-boundary re-checks in the hook (pattern at
  `hooks.rs:255-261`): an LRU flag on a hidden-prefixed name, or a capacity
  without the flag, rejects the DDL; and — per §5 — an LRU view that fails
  to compile rejects the DDL.
- **Leaf rule, engine side** (two independent backstops closing both source
  kinds):
  1. The view-dep precheck (`write_path.rs:522-539` neighborhood) rejects
     any DEP_TAB `+1` row whose `dep_table_id` resolves to a registered
     LRU-view `TableEntry`. (Covers every `ScanDelta` source of every
     circuit, including subquery inners; within-bundle hidden deps are
     unresolvable at precheck time and are never LRU.)
  2. `ScanTrace` sources produce **no** DEP_TAB row (`circuit.rs:45-63`), so
     additionally: the same precheck walks the batch's circuit-node `+1`
     rows and rejects a `ScanTrace` opcode whose tid is a registered LRU
     view, and the engine compiler's ext-trace resolution
     (`build_ext_cursors` / the `ScanTrace` emit arm) fails compilation when
     the tid is an LRU view — closing the circuit-builder-API path for
     views compiled later.
- **DROP / boot / backfill**: unchanged mechanics. DROP of an LRU view is
  never dep-blocked (leaf). Boot re-derives the family hydrated (backfill
  replays *sources* through the circuit — `backfill_view` opens source
  stores only, `ddl.rs:397-406`; a skeleton family is never replayed as a
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
| `crates/gnitz-core/src/types.rs`, `client.rs` | VIEW_TAB +2 columns; all row writers/readers/`ViewRecord`; `view_lru` + dist-prefix accessors; `PlannedView.lru` |
| `crates/gnitz-engine/src/catalog/{hooks,sys_tables,write_path,bootstrap}.rs` | descriptor + decode + re-validation; eager LRU compile with `Err`; DEP_TAB + circuit-node ScanTrace leaf prechecks |
| `crates/gnitz-engine/src/query/dag/mod.rs` | `TableEntry.lru_capacity`; `hydrate_keys`; ext-trace LRU compile rejection |
| `crates/gnitz-engine/src/query/compiler/{mod,emit}.rs` | structural `HydrationPlan` construction; field on `CachedPlan` through all three `CompileOutput` arms |
| `crates/gnitz-engine/src/storage/lsm/layout.rs`, `shard_file.rs`, `shard_reader/*` | `SHARD_FLAG_SKELETON`; skeleton open; `payload_cost_bytes` |
| `crates/gnitz-engine/src/storage/repr/{merge,heap}.rs`, `storage/lsm/read_cursor/*` | unconditional skeleton-aware ordering + grouping; skeleton exposure on the committed row |
| `crates/gnitz-engine/src/storage/lsm/compact/merge.rs`, `shard_index/*` | per-dest-guard representation; derived `dehydrated()`; `resident_cost`; `enforce_lru_capacity` with push-down |
| `crates/gnitz-engine/src/storage/lsm/{table/mod.rs,table/flush.rs,partitioned_table.rs}` | `LruCapacity` threading; ingest LSN bump for all tables; real spill LSNs; sweep triggers; `GNITZ_EPHEMERAL_CEILING_BYTES` |
| `crates/gnitz-engine/src/catalog/store_io.rs` | hydrating scan walk; skeleton-aware seek |
| `crates/gnitz-engine/src/runtime/orchestration/executor.rs` | view-targeted seeks drain ticks |
| `CLAUDE.md` | record the LRU view surface, the capacity contract, and the fold-totality invariant |

## Testing

- **Storage units**: skeleton shard write/open round-trip (flag, region
  derivation, `payload_cost_bytes = 0`); coarsening merge across all three
  consolidation paths — mixed skeleton/hydrated inputs, coarse-zero group
  ghosted, the non-negative-skeleton `debug_assert` exercised; per-dest-guard
  representation in a vertical spanning one dehydrated and one hydrated
  destination guard (hydrated guard output byte-identical to today);
  `enforce_lru_capacity` victim ordering on real spill-stamped LSNs,
  push-down steps 2/3 firing when the terminal level is empty, and cached
  `resident_cost` accuracy for both units across add/fold/vertical/removal;
  ingest LSN bump + real spill bounds.
- **Compiler units**: structural `HydrationPlan` construction on a real
  planner-emitted equi-join circuit (two JoinDTs + Union) and a linear
  circuit; clean compile failure on every ineligible node kind and on a
  malformed Union shape.
- **Planner units**: preprocessor (LRU stripped; CAPACITY parsed for all
  four units; mandatory-clause error; `CAPACITY` without `LRU` targeted
  error; quoted `"LRU"`/`"CAPACITY"` identifiers untouched; multi-statement
  strings with the LRU statement in each position; `lru`/`capacity` as
  ordinary identifiers; suffixed/decimal number rejected; original text
  stored in `sql_definition`); eligibility matrix (each rejected shape;
  CLUSTER BY source; LRU-over-LRU; Subquery-with-LRU rejected before the
  early return; range conjunct on the final join step rejected inside
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
    `ScanTrace(lru_view)` rejected at DDL.
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
- Non-LRU views and all base tables: no behavioral change except (a) the
  ingest LSN bump (dense LSNs on ephemeral shards, read by nothing else) and
  (b) view-targeted seeks now draining ticks (a strict freshness improvement
  aligning seeks with scans).
- `drive_merge` remains the sole pending-group drain owner; coarsening is
  expressed entirely through the boundary predicates it already consumes.
- Guard dehydration state is derivable from shard headers alone — no new
  manifest or catalog state, so future durability changes to ephemeral
  tables inherit it for free.

## Sequencing

1. **Storage core**: skeleton schema/flag/open + `payload_cost_bytes`;
   unconditional comparator amendments; per-dest-guard representation in
   `compact_routed`; ingest LSN bump + real spill LSNs; the fold-totality
   tripwire (+ units). Inert: no table carries a capacity.
2. **Capacity**: `LruCapacity` threading to `Table`/`ShardIndex`;
   `resident_cost`; `enforce_lru_capacity` with push-down; flush/compact
   triggers; `GNITZ_EPHEMERAL_CEILING_BYTES` (+ units). Still inert.
3. **Catalog plumbing**: VIEW_TAB columns end-to-end (full sweep incl.
   engine descriptor, drop_view retraction, `ViewRecord`, bootstrap COL_TAB,
   test builders); `TableEntry.lru_capacity` threading; DEP_TAB +
   circuit-node ScanTrace leaf prechecks; hook re-validation (+ units).
   Still inert: no SQL surface, circuit-API writes flags = 0.
4. **Hydration**: structural `HydrationPlan` + `hydrate_keys`; hydrating
   scan walk; skeleton-aware seek; view-seek drain; eager LRU compile with
   DDL rejection; ext-trace LRU compile rejection (+ units). Still inert.
5. **SQL surface**: per-statement preprocessor; coarse gate + final-step
   check + leaf rule; `PlannedView.lru` → `create_view_chain`; the full E2E
   suite.

Each step compiles and passes `make verify` + `make e2e` on its own.
