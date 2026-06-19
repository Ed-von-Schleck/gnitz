# Order-preserving range exchange for pure-range INNER joins

Replace the **broadcast** input relay of a pure-range **INNER** join (`n_eq == 0`,
no null-fill) with an **order-preserving range exchange**: both sides' traces are
range-partitioned by the (single) range-column OPK into `W` contiguous intervals,
and each input-delta row is routed only to the contiguous interval of workers whose
trace interval can satisfy the predicate. A probe then touches only
boundary-overlapping workers instead of every worker, and the delta is no longer
cloned `W` times.

The partition **boundaries are data-driven but sealed once**, at server start, from
a sample of the range column — and held fixed for the process lifetime. This keeps
the design free of any live re-partitioning: trace partitioning and delta routing
always agree because bounds never move while traffic flows. The cost is that
imbalance which accrues when the live distribution drifts away from the sealed
bounds is **not corrected until the next restart re-seals**. The worst case is an
append-mostly key (a timestamp / sequence) whose hot working set migrates past the
top split: new rows pile onto the top interval's worker, and the exchange degrades
toward broadcast cost *on that one worker* (never incorrect, never worse than
broadcast was for correctness). Live adaptive rebalancing — recomputing bounds from
the running distribution and physically re-partitioning the existing traces — is a
separate, larger effort and is **out of scope here**.

Scope is **pure-range INNER joins** only. Three join shapes broadcast today; this
work changes exactly one of them:

- **Pure-range INNER (`n_eq == 0`, no null-fill)** — *the target.* Broadcast →
  range exchange.
- **Pure-range LEFT (`n_eq == 0`, with null-fill)** — **stays on broadcast,
  untouched.** Its threshold null-fill computes `m = MAX/MIN(b.range_col)` with an
  inline `reduce_multi_local` over the **broadcast** `reindex_b` (planner.rs,
  `build_range_join_view`): a global extremum over a fully-broadcast input is
  identical on every worker, with no scatter/gather. Range-scattering `b` would give
  each worker only a slice of `b`, so its local reduce would compute a *local*
  extremum and the null-fill would be wrong. The relay decision distinguishes the
  two by circuit structure (below), so LEFT keeps its broadcast relay and its hash
  `PartitionFilter` verbatim.
- **Band join (`n_eq ≥ 1`)** — already scatters by the equality prefix (hash),
  untouched.

Equi-joins are untouched. This is an engine + master change; the SQL planner changes
only the integrate-path filter node it emits for the INNER pure-range case.

A write-once broadcast — all workers reading one shared SAL copy instead of `W`
per-worker clones — is an independent option, out of scope here: it would cut the
master's relay write from `D·W` to `D` for every broadcast (pure-range INNER and
LEFT, index-seek fan-out, replicated writes), but it is a change to the SAL's
per-worker-slot layout (each worker reads its own offset/size slot today) and does
not narrow the per-worker probe set. The range exchange narrows the clone to each
delta row's matching worker interval and lets non-matching workers skip the (empty)
probe. Both are constant-factor optimizations of *input* movement: the join's
dominant term — producing the half-space match output — is already distributed
`W`-way by the pair-PK output exchange (`cb.shard`, planner.rs) and is unchanged by
either.

---

## 1. Current state (what the code does today)

A pure-range join is built in `build_range_join_view`
(`crates/gnitz-sql/src/planner.rs`). For `n_eq == 0`, `k = n_eq + 1 = 1`: the
reindex key (`_join_pk`) is the **single range column**, stored order-preserving
(OPK), at the pair's common reindex type `T` (`reindex_output_type`). The trace is
sorted by that OPK. A single range column is one scalar, so `stride_T ≤ 16` always
(no compound key, no wide-PK split).

- **Broadcast decision** — `crates/gnitz-engine/src/runtime/orchestration/master/dispatch.rs:585-594`:
  ```rust
  let range_n_eq = if is_join { cat.dag.view_range_join_n_eq(view_id) } else { None };
  let dest_batches = if range_n_eq == Some(0) {
      // Pure range join: broadcast the full delta to every worker.
      let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();
      op_relay_broadcast(&sources, &schema, self.num_workers)
  } else { /* band/equi/group scatter */ };
  ```
  `op_relay_broadcast` (`crates/gnitz-engine/src/ops/exchange/relay.rs:534-555`)
  concatenates the per-source slices into one batch and `clone_batch`es it to every
  worker — `W` physical copies serialized over the SAL. `view_range_join_n_eq`
  returns `Some(0)` for **both** INNER and LEFT pure-range joins, so today both
  broadcast through this one branch.

- **Hash trace partition** — `planner.rs:1728-1732`:
  ```rust
  let (int_a, int_b) = if n_eq == 0 {
      (cb.partition_filter(reindex_a), cb.partition_filter(reindex_b))
  } else { (reindex_a, reindex_b) };
  let trace_a = cb.integrate_trace(int_a);
  let trace_b = cb.integrate_trace(int_b);
  let join_ab = cb.join_with_trace_range_node(reindex_a, trace_b, n_eq as u8, rel_ab); // ΔA ⋈θ I(B)
  let join_ba = cb.join_with_trace_range_node(reindex_b, trace_a, n_eq as u8, rel_ba); // ΔB ⋈θ I(A)
  ```
  `op_partition_filter` (`ops/exchange/router.rs:82-107`) keeps row `i` iff
  `worker_for_partition(partition_for_pk_bytes(pk_i), nw) == wid` — a **hash**
  partition of the OPK bytes. So each worker's trace holds a hash-random `1/W`
  slice. The two terms probe the **unfiltered** `reindex_*` (the full broadcast
  delta) against the other side's trace; the union over workers is the full join.

- **`PartitionFilter` is the sole consumer of the hash partition.** It carries no
  payload — worker identity is a compile-time constant (`circuit.rs:421-430`,
  `Instr::PartitionFilter { worker_id, num_workers }` in `query/vm/exec.rs`, wire
  `OPCODE_PARTITION_FILTER = 33`). Band joins omit it.

- **Reindexed deltas are unsorted and unconsolidated.** `map_reindex` overwrites
  the PK region with the join/range key and sets `output.sorted = false;
  output.consolidated = false` (`ops/linear.rs:150-151`) — the new PK order differs
  from the input row order. The range probe re-establishes order itself:
  `op_join_delta_trace_range` (`ops/join/delta_trace.rs:42-43`) calls
  `Batch::consolidate_if_needed`, which keys off the **`consolidated`** flag
  (`batch.rs:1472-1484`) — *not* `sorted` — and sort-consolidates when it is false.
  So the join always sees a correctly (PK, payload)-sorted delta regardless of the
  `sorted` flag, as long as nothing falsely marks an unconsolidated batch
  `consolidated`.

- **The exact range probe** — `ops/join/range.rs` (`range_per_row_seek`,
  `range_merge_walk`) seeks the trace by OPK and walks the matching interval
  computed by `range_cut_points`/`range_group_cut_points`
  (`storage/repr/range_key.rs`). The probe is exact and **unchanged** by this work.

- **`rel` per term** — `planner.rs`: the range is canonicalized to
  `left_col OP right_col`; term AB probes `trace_b` with `rel_ab = converse_rel(op)`,
  term BA probes `trace_a` with `rel_ba = op`.

- **Pure-range LEFT null-fill depends on broadcast.** When `is_left_join &&
  n_eq == 0`, `build_range_join_view` additionally builds `A − (A ⋈ {m})` with
  `m = MAX/MIN(b.range_col)` from `cb.reduce_multi_local(map_hash_row(reindex_b), …)`
  over the **broadcast** `reindex_b` (not the partition-filtered `int_b`), plus the
  `cb.null_extend` tail. `OpNode::NullExtend` (`OPCODE_NULL_EXTEND`) is emitted only
  by LEFT joins. INNER pure-range has **no** `Reduce` and **no** `NullExtend` node.

- **What the master knows at dispatch.** `shard_cols` (the reindex key columns,
  `len == n_eq + 1`) and `target_tcs` (the per-slot reindex type `T`) are in hand at
  `dispatch.rs:599-608` via `get_join_shard_cols` → `reindex_cols_through_filters`;
  for `n_eq == 0`, `shard_cols[0]` is the range column (in **base-table** schema,
  pre-reindex) and `target_tcs[0]` is its carried `T`. `target_tcs[0]` is `0` when
  the source column is already type `T` (same-type join) — the existing
  `JoinPromote` scatter resolves a `0` slot by self-deriving the column's type
  through `ReindexPacker`, so a concrete `T` is **not** needed at dispatch. The
  master holds a `*mut CatalogEngine` (`master/mod.rs`), so it can resolve the
  range column's promoted **width** once at view setup if it needs it for cold-start
  bounds. `self.num_workers` is set.

- **View traces are ephemeral and replay-rebuilt.** `IntegrateTrace` scratch tables
  are `Persistence::Ephemeral` (`query/compiler/emit.rs:87-95`, `create_child_table`),
  as is the view's output sink (`RelationKind::View ⇒ Persistence::Ephemeral`,
  `query/dag/mod.rs:95`). On open, `Table::new` runs `erase_stale_shards` for
  ephemeral tables (`storage/lsm/table/mod.rs:228`), so every trace and the sink
  start genuinely empty; they are then rebuilt by replaying base-table data through
  the view circuit. The **only** point at which a range-join view's traces are built
  from existing base data is the boot `backfill_exchange_views`
  (`runtime/bootstrap.rs`); it calls `fan_out_backfill` per `(view, source)` and is
  **boot-only** (the live CREATE path's `hook_view_register` skips backfill for
  exchange views, `catalog/hooks.rs:462`, and `fan_out_backfill` has no other call
  site). `backfill_view` performs **no clear** before replaying — it relies on the
  open-time erase (`catalog/ddl.rs:382-391`), and there is **no live primitive** to
  clear a registered ephemeral table's stored rows. This is exactly why bounds are
  sealed at the boot backfill (§3.6): it is the one moment a view's traces are
  (re)built from scratch, so building them under freshly-sealed bounds needs no
  re-partition, and a live reseal would have no correct way to re-derive the traces.

---

## 2. Committed design

A single set of **shared boundaries** — `W-1` OPK byte-strings of width
`stride_T` (the range column's reindex-type encoded width) — splits the range-column
OPK domain into `W` contiguous intervals. Worker `w` owns
`[bounds[w-1], bounds[w])` (with `bounds[-1] = 0x00…`, `bounds[W-1] = 0xFF…`).
Because both traces share `T` and the same bounds, one boundary array governs
*both* sides' partition filters *and* both terms' delta routing.

`bounds` is **non-decreasing** under `compare_pk_bytes`, not strictly ascending:
heavy ties may produce equal adjacent split keys, which leave the in-between workers
empty. `owner_of`'s `partition_point` handles a non-decreasing array correctly, so
no successor-nudging is done — see §3.2.

Five pieces:

1. **`RangePartitionFilter` op** is emitted on the integrate path of an INNER
   pure-range join *instead of* the hash `PartitionFilter` (LEFT keeps the hash
   filter): keep row `i` iff `owner(opk_i, bounds) == wid`, where `opk_i` is the
   reindex PK bytes (already the OPK `_join_pk`) and `owner` is a binary search over
   `bounds`. The hash `PartitionFilter` is **retained** for the pure-range LEFT case.

2. **Range-interval scatter** replaces broadcast on the input relay for the INNER
   case: route each delta row to the contiguous worker interval that its predicate
   side can match (a *bounded multi-destination* scatter — a row reaches `1..=W`
   workers), skipping NULL range-key rows. It writes through the existing one-copy
   `scatter_wire_group` SAL path (§3.5), not a materialized per-worker `Vec<Batch>`.

3. **Bounds** are computed from a range-column sample by even quantiles, **sealed
   once** at the boot backfill (§3.6) before the traces are rebuilt; an empty sample
   (no base data yet, or a live-created view) falls back to even-width type-domain
   bounds. Owned by the master, cached per-view on each worker. Fixed thereafter.

4. **Bounds transport**: a control message broadcasts `bounds` to all workers once,
   right after the boot seal; workers cache them in compiled-program state and
   `RangePartitionFilter` reads the cache. Cold-start even-width bounds need no
   transport (a pure function of `stride_T` and `W`, computed identically on both
   sides).

5. **Relay discriminator**: the master picks range scatter vs. broadcast by circuit
   structure (`view_range_join_shape`, §3.1). `Some(shape)` ⟺ the view's circuit
   carries a `RangePartitionFilter` node ⟺ INNER pure-range ⟹ range scatter. A
   pure-range LEFT join (hash `PartitionFilter`, has `NullExtend`) yields `None` and
   stays on `op_relay_broadcast`. Band/equi keep their existing scatter.

### Correctness

- **Conservative routing + exact probe.** Routing need only deliver a delta row to
  a *superset* of the workers holding a truly-matching trace row; the per-worker
  range probe (`range.rs`, unchanged) then matches exactly. So routing ignores
  open/closed (`<` vs `<=`) inclusivity and uses whole-worker intervals:
  - canonical `op ∈ {<, <=}`: term AB (`ΔA` value `x`, matches `b.y > x`) → suffix
    `[owner(x), W-1]`; term BA (`ΔB` value `y`, matches `a.x < y`) → prefix
    `[0, owner(y)]`.
  - canonical `op ∈ {>, >=}`: ΔA → prefix `[0, owner(x)]`; ΔB → suffix
    `[owner(y), W-1]`.

  The integrate owner `owner(x)` is always an **endpoint** of the routed interval,
  so the delta row reaches the one worker that integrates it (its
  `RangePartitionFilter` keeps it) *and* every worker whose trace can match it.

- **Union = full join.** Each truly-matching `(a, b)` pair has `b` on exactly one
  worker (`owner(b.y)`); `a`'s routed interval includes that worker; the term runs
  there and emits the pair once. Summing the per-worker term outputs (`cb.union` of
  `proj_ab`/`proj_ba`, unchanged) reproduces the broadcast result exactly — the
  partition is disjoint, so no pair duplicates and none is missed.

- **No new cross-term.** Single-source-per-epoch is preserved: the relay still
  carries one source's delta per epoch; routing only narrows *which workers* see it.
  `ΔA ⋈ ΔB` is still zero (CLAUDE.md §3).

- **Delta order/consolidation unchanged.** The range scatter copies base-delta rows
  by index and the integrate-path `RangePartitionFilter` keeps a *contiguous* OPK
  sub-run, so each propagates the source's `sorted`/`consolidated` flags **verbatim**
  — never fabricating `consolidated = true`. Because reindexed deltas arrive
  `consolidated = false` (§1), the range probe's `consolidate_if_needed` sort-merges
  them exactly as on the broadcast path today. No path is misled into skipping the
  sort.

- **Bounds are set once, before traffic.** Bounds are sealed during the boot
  backfill, before any steady-state epoch runs, and never change while the process
  is live. So no in-flight probe ever straddles two boundary versions, and trace
  partitioning is always consistent with delta routing because both read the same
  immutable bounds.

- **Restart re-seals; bounds need no durability.** View traces are ephemeral and
  replay-rebuilt under whatever bounds are active during replay (§1). On restart,
  bounds are re-sealed from a fresh sample of the (recovered) base data before the
  traces rebuild, and new deltas route under them too — trace partitioning and delta
  routing always agree at any instant. No on-disk partition is ever frozen against a
  stale boundary set.

---

## 3. Implementation

### 3.1 Range metadata lookup (`crates/gnitz-engine/src/query/compiler/load.rs` + `query/dag`)

`view_range_join_n_eq` exists (`circuit_range_join_n_eq` scans for a
`JoinKind::DeltaTraceRange` node). Add a sibling that returns the full shape the
router needs **and** gates on range-exchange eligibility:

```rust
/// Range-exchange routing shape for the input relay. `Some` IFF the view is an
/// INNER pure-range join eligible for the range exchange — detected by the
/// presence of a `RangePartitionFilter` node (the planner emits it only for that
/// case). A pure-range LEFT join carries the hash `PartitionFilter` + a
/// `NullExtend` and yields `None` (stays on broadcast); band/equi yield `None`.
pub(crate) fn circuit_range_join_shape(loaded: &LoadedCircuit) -> Option<RangeShape>;

pub(crate) struct RangeShape {
    pub rel: RangeRel,   // canonical `left_col OP right_col` — recovered as below
    pub range_tc: u8,    // carried reindex tc of the range slot; 0 = self-derive (see §3.5)
}
```

It confirms a `RangePartitionFilter` node is present, reads the reindex Map's
`reindex_target_tcs` last slot for `range_tc` (carried — may be `0`), and recovers the
**canonical `op`** robustly. The two `DeltaTraceRange` terms carry *different* rels
(`join_ab` probes `trace_b` with `rel_ab = converse(op)`; `join_ba` probes `trace_a`
with `rel_ba = op`), so reading an arbitrary node's rel is wrong. Instead, identify the
**right-delta** term — the `DeltaTraceRange` whose delta-port (`PORT_IN`) input traces
back through reindex/filter to the view's **right** `input_delta_tagged` tid — and take
`rel = op = rel_ba` directly (the left-delta term would give `converse(op)`). The view's
left/right input tids are the first/second `input_delta_tagged` nodes. `range_tc == 0`
is intentional and **not** resolved here: the scatter's key packer self-derives the
concrete type from the delta column (§3.5), so no catalog/base-schema context is needed.
`n_eq` is fixed at `0` for this path, so it is not returned.

Expose `DagEngine::view_range_join_shape(view_id)` (memoized like
`view_range_join_n_eq`). The dispatcher uses `Some(shape)` ⇒ range exchange, `None +
view_range_join_n_eq == Some(0)` ⇒ broadcast (pure-range LEFT), else the existing
band/equi/group scatter. `rel` picks suffix/prefix per side; the source's left/right
role is already known at the relay site (`source_id` maps to the left vs. right
input).

### 3.2 Boundaries: representation and computation

`bounds: Vec<PkBuf>` — `W-1` OPK split keys of width `stride_T`, **non-decreasing**
under `compare_pk_bytes`. Helpers in a new `ops/exchange/range_route.rs`:

```rust
#[derive(Clone, Copy)]
pub(crate) enum RouteDir { Suffix, Prefix }

/// Worker owning `opk` under `bounds`: the count of split keys ≤ opk. `bounds` is
/// non-decreasing; `partition_point` needs only a monotone predicate, so duplicate
/// split keys are fine — they leave the in-between workers empty. Result in 0..W.
pub(crate) fn owner_of(opk: &[u8], bounds: &[PkBuf]) -> usize {
    bounds.partition_point(|b| compare_pk_bytes(b.pk_bytes(), opk).is_le())
}

/// Routed inclusive worker interval [lo, hi] for a delta row, given its owner and
/// the direction for this (side, rel). `owner` is always an endpoint.
pub(crate) fn route_interval(owner: usize, dir: RouteDir, nw: usize) -> (usize, usize) {
    match dir {
        RouteDir::Suffix => (owner, nw - 1),
        RouteDir::Prefix => (0, owner),
    }
}

/// Direction per (side, canonical rel) — see §2. `op ∈ {<,<=}`: left⇒Suffix,
/// right⇒Prefix. `op ∈ {>,>=}`: left⇒Prefix, right⇒Suffix.
pub(crate) fn route_dir(rel: RangeRel, source_is_left: bool) -> RouteDir {
    let suffix_for_left = matches!(rel, RangeRel::Lt | RangeRel::Le);
    if source_is_left == suffix_for_left { RouteDir::Suffix } else { RouteDir::Prefix }
}
```

`compare_pk_bytes` is the canonical OPK comparator (CLAUDE.md §2). Because OPK is
order-preserving big-endian, even-width splits of the OPK byte domain are even splits
of the *value* domain for unsigned `T`, and — because signed columns are sign-flipped
into OPK (§6) — also for signed `T`.

**Quantile computation** (master, in §3.6's seal):

```rust
/// W-1 evenly-spaced quantile split keys from a sorted OPK sample. Empty sample
/// (no base data) ⇒ even-width type-domain splits over [0x00…, 0xFF…] at stride_t.
/// The result is NON-DECREASING; an all-equal sample yields equal adjacent splits
/// (a tie run lands wholly on one worker, leaving its siblings empty — correct,
/// just skewed). NO successor-nudging: `increment_key_in_place` on a 0xFF…FF split
/// wraps to 0x00 (it is `wrapping_add` with a carry-out flag the nudge would ignore),
/// breaking the non-decreasing invariant `owner_of` relies on.
fn compute_bounds(sample_sorted: &[PkBuf], nw: usize, stride_t: usize) -> Vec<PkBuf> {
    // bounds[i] = sample[(i+1) * m / W] for i in 0..W-1, clamped; empty ⇒ even-width.
}
```

Cold-start even-width splits are a pure function of `(stride_t, W)`, so the master
and workers agree before any data exists with no transport. `stride_T` is the
range column's promoted-`T` encoded width — a single one-time lookup the master
resolves from the range column's type via the catalog at view setup (not per-row).

### 3.3 `RangePartitionFilter` op (added alongside the hash `PartitionFilter`)

The INNER pure-range integrate path uses the new op; the hash `PartitionFilter` and
`op_relay_broadcast` are **kept** for the pure-range LEFT path. (Pre-alpha: both
coexist because they serve different join shapes, not for back-compat.)

- **Wire** (`crates/gnitz-wire/src/circuit.rs`): add `OpNode::RangePartitionFilter`
  (new opcode, no key payload — worker rank is a runtime constant, bounds are runtime
  program state). `OpNode::PartitionFilter` (opcode 33) stays.
- **Circuit builder** (`crates/gnitz-core/src/circuit.rs`): add
  `CircuitBuilder::range_partition_filter`; keep `partition_filter`.
- **Engine op** (`ops/exchange/router.rs`): add `op_range_partition_filter` next to
  `op_partition_filter`. The input is the **reindexed** batch, whose PK region is
  already the OPK `_join_pk`, so the filter reads it directly — no type or `range_tc`
  needed:
  ```rust
  pub(crate) fn op_range_partition_filter(
      batch: &Batch, schema: &SchemaDescriptor, worker_id: u32, num_workers: u32, bounds: &[PkBuf],
  ) -> Batch {
      if num_workers <= 1 || batch.count == 0 { return batch.clone_batch(); }
      let (nw, wid) = (num_workers as usize, worker_id as usize);
      let mb = batch.as_mem_batch();
      let mut indices = Vec::with_capacity(batch.count / nw + 1);
      for i in 0..batch.count {
          if owner_of(mb.get_pk_bytes(i), bounds) == wid { indices.push(i as u32); }
      }
      let mut out = Batch::from_indexed_rows(&mb, &indices, schema);
      out.sorted = batch.sorted;          // propagate verbatim (reindex set both false)
      out.consolidated = batch.consolidated; // never fabricate `consolidated = true`
      out
  }
  ```
  The kept rows form a *contiguous* OPK run (the input is OPK-sorted only if the
  upstream marked it so; reindexed deltas are not), so `sorted` carries through
  honestly. If `bounds` is empty (single worker / not yet transported), `nw <= 1`
  short-circuits to identity.
- **VM** (`query/vm/exec.rs`): `Instr::RangePartitionFilter { in_reg, out_reg,
  worker_id, num_workers, bounds_idx }` reads `program.range_bounds[bounds_idx]` and
  calls the op. `worker_id`/`num_workers` are baked at compile time exactly as for
  `PartitionFilter` (via `worker_rank()`/`num_workers()` at emit). Per-view bounds
  must reach the handler through the `Program`, because `execute_epoch` receives only
  `&Program` (not the owning `VmHandle`/`SubPlan`):
  ```rust
  // query/vm/mod.rs — Program gains one field; no execute_epoch signature change.
  pub(crate) struct Program { /* … */ pub range_bounds: Vec<Vec<u8>> }
  ```
  `range_bounds` is the flattened/`PkBuf`-decoded live bounds, overwritten in place by
  the `FLAG_SET_RANGE_BOUNDS` handler (§3.4) on the owning `SubPlan`'s `Program`. The
  instruction carries a `bounds_idx` (a view has one range join, so `0`) for
  forward-compat with multiple range joins per view.
- **Planner** (`planner.rs`): emit `range_partition_filter` for the INNER pure-range
  case, `partition_filter` for LEFT:
  ```rust
  let (int_a, int_b) = if n_eq == 0 {
      if is_left_join {
          (cb.partition_filter(reindex_a), cb.partition_filter(reindex_b)) // hash, broadcast
      } else {
          (cb.range_partition_filter(reindex_a), cb.range_partition_filter(reindex_b))
      }
  } else { (reindex_a, reindex_b) };
  ```
  The LEFT null-fill's `anull_owned` keeps `cb.partition_filter` (it rides the
  broadcast). The probe terms still read the unfiltered `reindex_a`/`reindex_b`.

### 3.4 Per-view bounds cache (worker) + bounds transport

- **Worker program state.** `Program::range_bounds` (§3.3) holds the live bounds,
  initialized to the cold-start even-width splits (computed from `stride_T`,
  `num_workers()`) when the view compiles, so `RangePartitionFilter` is correct
  before the seal arrives.
- **Transport.** Add a control SAL message `FLAG_SET_RANGE_BOUNDS`
  (`runtime/protocol/sal.rs`) carrying `(view_id, Vec<PkBuf> bounds)`. The master
  broadcasts it once, right after the boot seal (§3.6). The worker decodes it, looks
  up the view's `SubPlan`, and overwrites `Program::range_bounds`. It is not a data
  write — no fdatasync (re-derivable, CLAUDE.md SAL durability contract) — and is
  modeled on `FLAG_TICK` (broadcast, no data batch, ACK per worker): add a
  `SalMessageKind` variant, a `classify` arm, an `is_broadcast` entry, and a worker
  `dispatch` arm. Because it is processed at the worker's top-level `drain_sal`
  boundary (no epoch in flight) and the seal happens before steady-state traffic, no
  probe is mid-flight when it lands.
- **Master copy.** The master keeps the same `bounds` per view (in the view's
  dispatch metadata) for the scatter (§3.5). Master and workers hold byte-identical
  bounds from the seal onward.

### 3.5 Range-interval scatter (replaces broadcast for the INNER case)

At `dispatch.rs`, when `view_range_join_shape` is `Some(shape)`, replace the
`op_relay_broadcast` branch with a one-copy range scatter; the `None + Some(0)` case
keeps broadcast:

```rust
let range_shape = if is_join { cat.dag.view_range_join_shape(view_id) } else { None };
// … in the relay body:
if let Some(shape) = range_shape {
    // INNER pure-range: bounded multi-destination range scatter, one-copy to SAL.
    // dir + bounds + the range column are all the scatter needs; no Vec<Batch>.
    // (computed + written under the SAL lock — see below)
} else if range_n_eq == Some(0) {
    // pure-range LEFT: keep broadcast (threshold null-fill needs global b).
    let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();
    op_relay_broadcast(&sources, &schema, self.num_workers)   // → Vec<Batch> → emit_relay
} else { /* existing band/equi/group scatter, unchanged */ }
```

The range scatter computes per-worker row-index lists and writes them through the
**existing one-copy `scatter_wire_group`** (`runtime/protocol/sal.rs`) — the same path
PUSH already uses — carrying `FLAG_EXCHANGE_RELAY` and the relay's `source_id` (echoed
via `seek_pk`) and backfill `decision` (via `seek_col_idx`), exactly as `emit_relay`
does today. New index builder in `ops/exchange/range_route.rs`:

```rust
/// Per-worker row-index lists for a bounded multi-destination range scatter over a
/// base-table delta. For each row: skip it if its range column is NULL (cannot match
/// under SQL 3VL — see below); else encode the range column (col `range_col`, carried
/// type `range_tc`, 0 ⇒ self-derive from the column) to the SAME stride_T OPK the
/// worker's reindex packs as `_join_pk` — via the existing `ReindexPacker`
/// (`compound_join_packer` machinery), so routing and trace partitioning agree
/// byte-for-byte (CLAUDE.md §6, plan §7). Then owner = owner_of(opk, bounds), expand
/// to [lo, hi] = route_interval(owner, dir, nw), and push the row index to every
/// worker in that interval. A row index may appear in several lists (1..=W
/// destinations). The TLS pool / borrow contract mirrors `with_worker_indices`.
fn fill_range_indices(
    batch: &Batch, range_col: usize, range_tc: u8, bounds: &[PkBuf], dir: RouteDir,
    schema: &SchemaDescriptor, num_workers: usize, out: &mut Vec<Vec<u32>>,
);
pub(crate) fn with_range_indices<F, R>(/* … */, f: F) -> R where F: FnOnce(&[Vec<u32>]) -> R;
```

**NULL range-key pruning at the master.** A row whose range column is NULL can never
satisfy a pure-range INNER predicate (SQL 3VL), and the integrate path's pre-reindex
NULL gate (`multi_null_filter_prog`) would drop it on the worker anyway. Pruning it at
the master removes it from the SAL write and from every worker's CPU. Inside the
`fill_range_indices` row loop, before encoding the key:

```rust
// A PK range column is non-nullable (sentinel ⇒ skip the check); a nullable payload
// range column is dropped when NULL. `mb` is the source delta's MemBatch.
let pidx = schema.payload_mapping_byte(range_col);
if pidx != PAYLOAD_MAPPING_PK_SENTINEL && (mb.get_null_word(row) >> pidx) & 1 != 0 {
    continue;
}
```

`scatter_wire_group` then copies each row directly from the source delta into every
destination worker's SAL slot — a row in `k` lists is copied `k` times into `k` slots,
exactly as `with_broadcast_indices` already does for a replicated write (`k = W`). The
caller builds only the index lists (no caller-side `Vec<Batch>`); the one-copy fast
path applies to fixed-width (`wire_safe`) schemas, while a base delta carrying
STRING/BLOB columns takes `scatter_wire_group`'s internal Batch-rebuild fallback.
Because the index computation must co-locate with the SAL write under
`sal_writer_excl`, the range case computes indices and calls `scatter_wire_group`
inside the emit half of the relay (the `prepare`/`emit` split passes through the base
delta + shape + live bounds for this case rather than a pre-built `dest_batches`).

At `W = 1` every interval is `[0, 0]`: identity, matching today's single-process
behavior. The cross-width / cross-sign correctness is inherited from `ReindexPacker`:
`range_tc != 0` packs at the wider `T`; `range_tc == 0` packs at the column's native
OPK width — both byte-identical to the integrate-path reindex.

### 3.6 Boot bounds seal

Bounds are sealed once, at server start, in `backfill_exchange_views`
(`runtime/bootstrap.rs`) — immediately **before** a range-join view's
`fan_out_backfill` rebuilds its traces (§1: the only point those traces are built
from existing base data, and there is no live re-derivation). For each view with
`view_range_join_shape == Some`:

1. The master gathers a bounded OPK reservoir of the range column from **both** base
   inputs feeding the range slot (left `a.range_col` and right `b.range_col`), reusing
   the scan fan-out, early-stopping once the reservoir fills (cap ~4096 keys, so boot
   memory is O(1) and I/O is bounded). Each sampled value is packed to the `stride_T`
   OPK with the same `ReindexPacker` the integrate path uses, so the sample lives in
   the same key space as the trace keys it will partition, and quantiles over the
   combined sample balance the shared key space both traces use.
2. `compute_bounds(sample_sorted, num_workers, stride_t)` → `W-1` quantile split keys
   (empty sample ⇒ even-width type-domain).
3. The master records `bounds` in the view's dispatch metadata (for the §3.5 scatter)
   and broadcasts `FLAG_SET_RANGE_BOUNDS(view_id, bounds)`; workers overwrite
   `Program::range_bounds`.
4. `fan_out_backfill` then runs; the backfill's deltas route (§3.5) and partition
   (§3.3) under the sealed bounds, so the rebuilt traces are partitioned consistently
   with them.

A view created live (after start), or over not-yet-populated tables, runs on the
cold-start even-width bounds (the compile-time default, a pure function of `stride_T`
and `W`); its traces accrue under those bounds and are re-sealed from the
now-populated base tables at the next restart. There is no per-tick stats collection
and no live reseal: the master holds its sealed copy for the scatter and the workers
hold the same copy for the filter, set once and never diverging for the process
lifetime.

---

## 4. Edge cases

- **`W = 1`** — every op degenerates to identity: `route_interval` is `[0, 0]`,
  `RangePartitionFilter` keeps all, no scatter clones. Matches today.
- **Empty trace / cold start** — even-width type-domain bounds; correct (interval
  routing + exact probe), just unbalanced until the next restart seals a sample.
- **`W` > distinct keys / heavy ties** — `compute_bounds` may emit equal adjacent
  splits; `owner_of`'s `partition_point` leaves the in-between workers empty (a tie
  run lands wholly on one worker). Correct, skewed; no worse than broadcast. No
  successor-nudging (§3.2).
- **Range key width** — a single range column is one scalar (`stride_T ≤ 16`); all
  routing uses raw OPK bytes via `compare_pk_bytes`/`partition_point`, never the
  narrow `u128` hash path, so the key routes by `memcmp` exactly (no `pk_is_wide`
  split — CLAUDE.md §3 invariant).
- **NULL range key** — dropped twice over: the master's `fill_range_indices` skips it
  (§3.5), and the existing pre-reindex NULL-key gate (`multi_null_filter_prog`) drops
  any that reach a worker, so no NULL OPK reaches routing or the integrate filter.
- **Signed / cross-sign / cross-width `T`** — OPK sign-flips signed columns and the
  scatter promotes to `T` via the existing `ReindexPacker` key path (resolving
  `range_tc == 0` from the column), so value order equals OPK byte order for every
  `T` the range classifier admits (CLAUDE.md §6).
- **Pure-range LEFT join** — out of scope for the exchange; the relay discriminator
  returns `None` (the circuit carries a hash `PartitionFilter` + `NullExtend`, no
  `RangePartitionFilter`), so it keeps `op_relay_broadcast` and its global-`b`
  threshold null-fill unchanged.
- **Restart** — bounds re-seal from a fresh sample of the recovered base data, then
  traces replay-rebuild under them; trace partitioning and delta routing agree at
  every instant. Bounds carry no durability.
- **Post-seal drift (the accepted limitation)** — once sealed, bounds do not chase a
  drifting distribution. An append-mostly key (timestamp / sequence) whose hot
  working set migrates past the top split concentrates new rows on the top interval's
  worker, degrading toward broadcast cost on that worker until the next restart
  re-seals. Never incorrect. Correcting this online is the separate adaptive-rebalance
  effort, out of scope here.

---

## 5. Tests

### 5.1 Unit — routing math (`ops/exchange/range_route.rs`, inline `mod tests`)
- `owner_of` over a 3-split (`W = 4`) non-decreasing bounds array: keys below all
  splits → 0, between → exact interval index, at/above the top split → `W-1`;
  exact-on-split keys (`is_le` boundary) land in the upper worker, deterministically.
- `owner_of` over a bounds array **with duplicates** (incl. a duplicate at the
  maximal `0xFF…FF` split): the in-between workers own nothing, no key is dropped, no
  wraparound (regression for the removed nudging).
- `route_interval` / `route_dir`: `Suffix`/`Prefix` × owner at `0`, mid, `W-1` give
  the right inclusive ranges; owner is always an endpoint; all four `rel` × left/right
  combinations.
- `compute_bounds`: uniform sample → near-even splits; all-equal sample → equal
  adjacent splits with no panic/wraparound; empty sample → even-width type-domain
  splits identical to the worker cold-start computation (same function).
- Signed `T`: `owner_of` orders by `compare_pk_bytes`, matching numeric order of the
  decoded values.

### 5.2 Unit — `op_range_partition_filter` / `fill_range_indices`
- A 4-worker bounds array over a known OPK column: the filter keeps exactly the rows
  in each worker's interval, propagates the input `sorted`/`consolidated` flags
  verbatim, and the union of all four filtered batches equals the input (disjoint,
  complete).
- `fill_range_indices` for `op = <`, left side (`Suffix`): a row with key `x` appears
  in destinations `[owner(x), W-1]` and nowhere else; the owner endpoint is present;
  the produced OPK equals the reindex-path `_join_pk` for the same value (cross-width
  and same-width); `W = 1` is identity.
- `fill_range_indices` NULL pruning: a row whose nullable range column is NULL is
  pushed to **no** worker's list; a non-NULL row in the same batch routes normally; a
  PK (non-nullable) range column is never tested for NULL.

### 5.3 Engine — multi-worker equivalence (`crates/gnitz-engine` integration test)
- Build a pure-range INNER join circuit at `W = 4`; drive a mixed insert/delete
  workload and assert the consolidated output equals the same circuit run under the
  **old broadcast** path (capture broadcast output via a `W = 1` reference run).
  Covers both `range_per_row_seek` and `range_merge_walk` (force via delta size).
- Seal-then-run: pre-seal bounds from a known sample, broadcast `FLAG_SET_RANGE_BOUNDS`,
  then drive deltas and assert output matches the broadcast reference and that the two
  traces are partitioned under the sealed bounds (each worker's trace holds only its
  interval's keys).

### 5.4 E2E (`crates/gnitz-py/tests/`, `GNITZ_WORKERS=4`)
Add `test_range_join_exchange.py`:
- Pure-range INNER (`ON a.x < b.y`) over tables seeded so matches straddle worker
  boundaries; assert result == brute-force after inserts/deletes/updates.
- All four `rel` directions (`<`, `<=`, `>`, `>=`) — confirm suffix/prefix routing per
  side is correct.
- Skewed keys (all in a narrow band) — assert correctness (not balance); the seal
  lands them on one worker and the result is still exact.
- Cross-width / cross-sign range column (`a.i32 < b.i64`, unsigned-vs-signed within
  the admitted set) — routes and matches correctly.
- A residual on a pure-range INNER join (`ON a.x < b.y AND a.r <> b.s`) still works —
  the residual filter sits over the merged output, after the exchange.
- Seal from existing data: populate base tables, restart the server (so
  `backfill_exchange_views` re-seals from the recovered data), and assert the view is
  correct and its traces are range-partitioned (not hash).
- **Pure-range LEFT join regression** (`LEFT JOIN … ON a.x < b.y`): result ==
  brute-force, including rows that null-fill, across multi-worker — proves the
  exchange did **not** disturb the broadcast/threshold null-fill path. Include an
  empty-`b` case (every left row null-fills) and a case where `b`'s extremum changes.

### 5.5 Gate
`make verify` (fmt + clippy-as-errors + unit) and
`make e2e K='range' WORKERS=4`. Run the engine equivalence + seal tests a few times
to rule out a routing race.

---

## 6. File-change summary

| File | Change |
|------|--------|
| `crates/gnitz-wire/src/circuit.rs` | Add `OpNode::RangePartitionFilter` (new opcode, no payload); add `FLAG_SET_RANGE_BOUNDS` control flag. Keep `PartitionFilter` (opcode 33) |
| `crates/gnitz-core/src/circuit.rs` | Add `CircuitBuilder::range_partition_filter`; keep `partition_filter` |
| `crates/gnitz-sql/src/planner.rs` | `build_range_join_view`: emit `range_partition_filter` for INNER pure-range, `partition_filter` for LEFT pure-range |
| `crates/gnitz-engine/src/ops/exchange/range_route.rs` | New: `owner_of`, `route_interval`, `route_dir`, `compute_bounds`, `fill_range_indices` / `with_range_indices` (incl. NULL pruning), `RouteDir` |
| `crates/gnitz-engine/src/ops/exchange/router.rs` | Add `op_range_partition_filter(.., bounds)` next to `op_partition_filter` |
| `crates/gnitz-engine/src/query/vm/{mod,exec}.rs` | `Program::range_bounds`; `Instr::RangePartitionFilter { …, bounds_idx }` reads worker rank + `program.range_bounds[bounds_idx]` |
| `crates/gnitz-engine/src/query/compiler/load.rs` + `query/dag` | `circuit_range_join_shape` / `DagEngine::view_range_join_shape` |
| `crates/gnitz-engine/src/runtime/orchestration/master/dispatch.rs` | Range scatter (via `scatter_wire_group`) for `Some(shape)`; broadcast for pure-range LEFT |
| `crates/gnitz-engine/src/runtime/bootstrap.rs` | Boot bounds seal: sample the range column, `compute_bounds`, broadcast `FLAG_SET_RANGE_BOUNDS`, before each range view's `fan_out_backfill` |
| `crates/gnitz-engine/src/runtime/orchestration/worker/mod.rs` | Handle `FLAG_SET_RANGE_BOUNDS` (overwrite `Program::range_bounds` for the view's `SubPlan`) |
| `crates/gnitz-engine/src/runtime/protocol/sal.rs` | New control flag + bounds (de)serialization; route `FLAG_EXCHANGE_RELAY` range scatter through `scatter_wire_group` |
| `crates/gnitz-engine` tests + `crates/gnitz-py/tests/test_range_join_exchange.py` | §5 |

`op_relay_broadcast`, `op_partition_filter`, and the hash `PartitionFilter` opcode
all **remain** — they serve the pure-range LEFT path.

---

## 7. Invariants preserved (CLAUDE.md)

- **OPK / no narrow-PK guards.** All routing, ownership, and boundary math use raw
  OPK bytes through `compare_pk_bytes` (`memcmp`), at every width — no `pk_is_wide`
  dispatch, no `u128`/`pk_cache` shortcut on the routing path.
- **One key per producer/consumer.** The range column is promoted to `T` by the same
  `ReindexPacker`/`extract_col_key` path for routing as the reindex packer uses for
  the trace key (resolving `range_tc == 0` identically), so delta routing and trace
  partitioning agree on the OPK byte-for-byte.
- **Single-source-per-epoch / no `ΔA ⋈ ΔB`.** The relay still carries one source per
  epoch; range routing only narrows the worker set.
- **(PK, payload) sort / consolidation.** `RangePartitionFilter` and the range
  scatter propagate `sorted`/`consolidated` verbatim and never fabricate
  `consolidated = true`; reindexed deltas stay `consolidated = false`, so the range
  probe's `consolidate_if_needed` sort-merges them as on the broadcast path. No merge
  path sees PK-only ordering.
- **Positivity / trace semantics.** Bounds and routing never alter weights or the
  z⁻¹ trace snapshot; the exact range probe (`range.rs`) is unchanged.
- **SAL durability.** Bounds are re-derivable control traffic — no fdatasync;
  recovery replays to the last checkpoint, ephemeral traces rebuild under freshly
  re-sealed bounds, and routing stays self-consistent.
