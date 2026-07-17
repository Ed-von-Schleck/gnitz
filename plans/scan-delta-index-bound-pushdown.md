# Index-bound pushdown into circuit `ScanDelta` source scans

Both circuit backfill drivers open `open_store_cursor(source_id)` — an unbounded `ReadCursor`
over the whole local partition slice — and pull fixed chunks through the circuit. So a
selective `WHERE indexed_col = 5` that a GROUP BY forces through the circuit reads every row of
the source and drops all but a handful in the leading `Filter`, even though a secondary index
on `indexed_col` exists and the thin keyseek path would have used it.

Push the index bound into the source scan: the SQL planner attaches the best equality/range
index candidate to the `ScanDelta` node as a **backfill-scan hint**, and the source drive opens
an index-bounded cursor that **adaptively abandons back to a full scan** once the range proves
unselective. The circuit's `Filter` is emitted **verbatim**, so query semantics and incremental
maintenance are untouched.

## 0. Honest scope and value — read before building

This is a **narrow** optimization, and the plan commits to saying so rather than implying more:

- **It serves exactly two shapes**, both single-base-table: a linear view/transient
  (`plan/view/simple.rs`) and a **single-table** GROUP BY (`plan/view/group_by.rs`).
- **GROUP BY over a join never fires.** `resolve_operator_input` clears the WHERE before
  compiling the join to a hidden segment — `op_select.selection = None` (`plan/view/dispatch.rs:614`)
  — and the GROUP BY segment then scans that hidden view. `emit_group_by_pieces` sees
  `(base table, Some(where))` only for a plain single-table FROM.
- **No downstream chain segment can ever carry a bound**: a view cannot own an index (rejected
  at bind, `plan/ddl.rs:652-657` — "indexing a snapshot of derived data has no defined
  semantics"; again at `catalog/write_path.rs:241-248`). Only a leaf segment scanning a base
  table qualifies.
- **`SELECT * FROM t WHERE indexed = 5` never reaches this code** — the thin keyseek path
  serves it (`dml/select.rs:126-189`). `simple.rs` gets a bound only for a CREATE VIEW backfill
  (one-time), an ad-hoc select with a non-thin projection (`select.rs:71`), or a non-thin-evaluable
  residual (`select.rs:120-122`).
- **A PK predicate never produces a bound.** Both collectors funnel through
  `collect_eq_conjuncts` (`dml/plan.rs:373`), which gates on `!schema.is_pk_col(col) &&
  tc.is_pk_eligible()` — PK columns are skipped unconditionally, whatever index circuits exist.
  So `WHERE pk = k` always full-scans. A bound therefore always names a **non-PK secondary
  index**.

**Demand is unevidenced.** Across 168 executed `CREATE INDEX` sites in the repo, **zero** pair
an index with a GROUP BY on the same table, and `benchmarks/combined/` contains **no**
`CREATE INDEX` at all. The nearest miss is `benchmarks/features/test_reads.py:115` —
`CREATE VIEW v{i} AS SELECT g, SUM(v) AS s FROM t WHERE v >= {i} GROUP BY g`, exactly the target
shape, on an unindexed `t`. The shape is natural; nothing in-tree exercises it. §7 therefore
makes indexing that benchmark step 1, so the feature ships with a number attached.

## 1. Why a hint, not a plan rewrite (correctness)

- **Steady-state maintenance is untouched.** A maintained view's `ScanDelta` processes the
  per-push *delta* — it never opens the source cursor. The bound is consulted **only** by the
  two backfill drivers. A push of a row failing the index predicate still flows through the
  unchanged `Filter` and is dropped there. If the bound instead *replaced* the index conjuncts
  in the `Filter`, a steady-state push failing the index predicate but passing the residual
  would leak — so the `Filter` keeps the full predicate.
- **The bounded scan reads a superset of the Filter-passing rows.** The bound is one index
  candidate's range, derived from the `Filter`'s own conjuncts, so it reads exactly the rows
  matching those conjuncts — a superset of what the full `Filter` passes (which also requires
  the residual). The unchanged `Filter` drops the over-read remainder.

## 2. The load-bearing invariant: an index circuit is a local shadow of the local base slice

This is what makes a bounded scan sound at `W > 1`, and it is established at write time, not by
a routing decision that could drift:

- The base table is a `PartitionedTable` (256 buckets, `partition_for_pk_bytes` over the PK's
  distribution prefix); each worker owns a contiguous range
  (`partition_range(worker_id, num_workers)`, `storage/lsm/partitioned_table.rs:53`) and closes
  the rest (`trim_worker_partitions`).
- The **index table is not partitioned at all** — `IndexCircuitEntry::index_table:
  UnsafeCell<Box<Table>>` (`query/dag/mod.rs:43`), a plain `Table`. Nothing hashes an index key;
  there is no exchange or scatter anywhere on the index write path.
- Both index write paths are strictly local: steady state projects the index batch from *the
  same* batch being ingested into this process's base store (`ingest_store_and_indices`,
  `query/dag/ingest.rs:144-149` → `batch_project_index`, which copies the source PK verbatim into
  the key suffix, `ingest.rs:438`); CREATE INDEX backfill reads the owner's **local** store
  cursor (`catalog/ddl.rs:582-585`).

So: **if a process holds an in-range index entry with source-PK suffix `P`, that process holds
row `P` in its own base slice.** The gather needs no exchange; a miss in `advance_to_exact_live`
means the row is genuinely absent/retracted, never "it lives on another worker". (Index key
layout is `[promoted indexed cols… ‖ source PK cols…]`, all-PK, zero payload —
`make_index_schema`, `schema.rs:1468-1497`.)

**The master is excluded, and must stay excluded.** The master's index copies are registered but
stay **permanently empty** (`catalog/hooks.rs:643` skips `backfill_index` on `is_master()`;
distributed probes union the workers' local copies). A bounded cursor there would return **zero
rows, not an error** — §6's missing-circuit fallback would not catch it and the view would
silently fill empty. The master is excluded today because it owns no partitions: post-fork the
parent runs `close_user_table_partitions()` + `set_active_partitions(0, 0)`
(`runtime/bootstrap.rs:653`), so `backfill_view`'s `active_part_start != active_part_end` guard
(`catalog/hooks.rs:531`) is false there, and `handle_backfill` lives in
`runtime/orchestration/worker/mod.rs` — workers only. Standalone is `is_worker()`, not
`is_master()`, and backfills its own slice.

> **Invariant to preserve: a bounded cursor may only be opened in a process that owns base
> partitions.** Both drivers already satisfy it. `open_source_cursor` (§6a) enforces it with a
> **hard** `assert!(self.active_part_start != self.active_part_end)` — not `debug_assert!`.
> Release is a supported deployment (`make release-server`, `GNITZ_SERVER_BIN`), and a violation
> here yields a **silently empty view**, so the guard must not compile out. It costs one compare
> per backfill, not per row.

## 3. The adaptive cap — why no cost model is needed

A bounded scan is **not** unconditionally cheaper. For a range matching `M` of `N` rows it costs
an index walk of `M` entries, an `M log M` sort, and `M` galloping probes into the base merge
cursor; a full scan is one sequential columnar drain of `N`. Bounded wins for small `M/N` and
**loses badly as `M → N`** — and `WHERE indexed_col > 0` (an in-tree shape:
`test_catalog.py:61-64`) matches everything.

The project has no selectivity estimator and has explicitly deferred one
(`research/optimizations.md:522`; the word `selectivity` appears in zero source files), and
`IndexMeta { cols, is_unique }` (`gnitz-core/src/client.rs:157-160`) carries no cardinality. So
the planner **cannot** know `M`. Attaching the bound blind — as an earlier draft did — admits an
unbounded pessimization.

Resolve it at the only place `M` is observable, without stats: **the drive counts as it walks
and abandons when the range proves unselective.**

```
cap = max(chunk_rows, src.estimated_length() / ABANDON_RATIO)   // ABANDON_RATIO = 16
```
`ReadCursor::estimated_length()` (`storage/lsm/read_cursor/mod.rs:754`) gives `N` for the base
snapshot at construction. `BoundedIndexCursor` tracks a running `collected` total across chunks;
the moment it exceeds `cap`, it **abandons**: it drops the index cursor and degrades to draining
its already-held base `src` cursor as a plain full scan, from the beginning
(`src.rewind()`, `read_cursor/mod.rs:228`).

This makes the cost profile **decided, not gated**:
- **Worst case** ≈ `1 + 1/ABANDON_RATIO` ≈ **1.06×** a full scan — the abandoned index walk
  touched at most `N/16` entries, and no base row was gathered twice because abandonment happens
  before any chunk is emitted (see below).
- **Best case** — `N/M`× faster.
- No stats, no estimator, no planner cost model.

**Abandonment must precede the first emitted chunk**, or a partial bounded prefix would be
double-counted against the full re-scan. Since `cap >= chunk_rows`, the first `drain_chunk` may
collect up to `cap` PKs before deciding; it emits nothing until the walk either ends (bounded
path wins) or crosses `cap` (abandon, then drain `src` from `rewind()`). Chunks after the first
cannot abandon — by then `collected <= cap` held at every emit, and the remaining walk is
already known to be within budget. Concretely: the cap check runs **only** while
`emitted_any == false`; once a bounded chunk is emitted, the cursor is committed to the bounded
path for its lifetime.

## 4. What the current code provides (verified)

- **Bound extraction exists client-side, over the raw AST.**
  `collect_index_range_candidates(expr: &'e Expr, schema: &Schema, fetch_indexes: impl FnOnce()
  -> Result<Arc<Vec<IndexMeta>>, ClientError>) -> Result<Vec<IndexRangeCandidate<'e>>, ClientError>`
  (`gnitz-sql/src/dml/plan.rs:587`) and the pure-equality sibling `collect_index_seek_candidates`
  (`plan.rs:319`, returning `type IndexSeekCandidate<'e> = (PkColList, Vec<u128>, Vec<&'e Expr>)`).
  Both are **sync**, take a **lazy closure** (invoked only after the conjunct walk proves ≥1
  usable conjunct, so a non-servable WHERE costs zero wire traffic), and return a **best-first
  `Vec`** — primary key = covered/eq-pinned leading columns **descending**, tiebreak = the
  index's own column count **ascending** (`plan.rs:358-364` seek, `plan.rs:680-687` range).
- **`RangeDescriptor`** (`gnitz-wire/src/range.rs:98`) is a self-contained `Copy` wire type:
  `eq: [u128; PK_LIST_MAX_COLS]` (**native LE-packed values, not OPK bytes** — the worker is the
  sole OPK encoder), `n_eq`, `start`/`end: Cut`, `encode`/`decode`. `PkColList`
  (`gnitz-wire/src/catalog.rs:349`) is `Copy` and holds up to `PK_LIST_MAX_COLS = 4` **column
  indices**; `as_slice()` returns `&[u32]` (`catalog.rs:399`).
- **The engine compiler cannot decide the bound.** `ExtTables = HashMap<i64, SchemaDescriptor>`
  (`query/compiler/mod.rs:104`) is schema-only, and the layering forbids `query` (L5) from
  reaching `catalog` (L6) where `index_circuits` live. Server-side extraction from the compiled
  `Filter` blob is also a dead end: `ExprProgram` registers are reusable, not SSA
  (`expr/program.rs:858-860` forbids only intra-instruction aliasing), so recovering `col OP
  const` conjuncts needs last-write dataflow plus 3VL null reasoning — a second extractor that
  can drift from the AST one. The planner decides; the bound travels on the node.
- **The server can serve a bounded scan today.** `seek_by_index_range` (`catalog/store_io.rs:296`)
  walks the index table's ordered range (`ic.key_spec.prefix(n_eq+1)` cut keys, `open_cursor()` +
  `seek_bytes(start)` + break at `end`), collects the source-PK suffix of each positive-weight
  in-range key, sorts, and gathers via `resolve_source_pks` (`store_io.rs:241`). The missing
  piece is a **chunked** variant fitting the `drain_chunk` loop.

## 5. Wire: `ScanDelta` carries an optional bound

`OpNode::ScanDelta(TableId)` (`gnitz-wire/src/circuit.rs:314`) carries only the source id and
emits zero param rows (`encode_op_node`, `circuit.rs:440`). Make it a struct variant:

```rust
/// `OPCODE_SCAN_DELTA`. Delta input for `source`. `bound` is a backfill-scan hint only
/// (steady-state deltas ignore it); a non-`None` bound narrows the initial full-source scan
/// to a secondary-index range, leaving the downstream `Filter` unchanged.
ScanDelta { source: TableId, bound: Option<ScanBound> },

/// A secondary-index range bound: the index's declared column list and the half-open range
/// over its leading columns. Resolved server-side to the source's `IndexCircuitEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanBound {
    pub idx_cols: PkColList,
    pub desc: RangeDescriptor,
}
```

`ScanBound` is `Copy` (both fields are) — load-bearing for §6, which would otherwise hit a
borrowck conflict between `self.dag.source_scan_bound(..)` and `self.open_*_cursor(..)`.

Add `pub const NODE_COL_KIND_SCAN_BOUND: u64 = 10;` (`circuit.rs:66-75`; 0–9 taken, 10 free).
`encode_col_list` is `fn encode_col_list<I, T>(kind: u64, iter: I) -> Vec<NodeColumnPayload>
where I: IntoIterator<Item = T>, T: Into<u64>` (`circuit.rs:410`) and
`NodeColumnPayload = (u64, u16, u64, u64)` (`circuit.rs:408`) — the `u16` is the **position**;
the column index rides in `value1: u64`. Since `u32: Into<u64>`, pass the slice iterator
directly (**no `collect()`, no cast** — `collect()` there is `E0283`, `I` is only bound by
`IntoIterator`):

```rust
OpNode::ScanDelta { source, bound: None } => ((OPCODE_SCAN_DELTA, Some(source), None), Vec::new()),
OpNode::ScanDelta { source, bound: Some(b) } => (
    (OPCODE_SCAN_DELTA, Some(source), Some(b.desc.encode())),
    encode_col_list(NODE_COL_KIND_SCAN_BOUND, b.idx_cols.as_slice().iter().copied()),
),
```

`decode_op_node` (`circuit.rs:538`) reconstructs `bound: None` when no `SCAN_BOUND` rows are
present. Its existing `collect_cols` closure returns `Vec<u16>` (`circuit.rs:515`), but
`PkColList::from_slice` takes `&[u32]` (`catalog.rs:365`) and **panics** outside
`1..=PK_LIST_MAX_COLS`. This decode is a trust boundary over catalog bytes, so collect to
`Vec<u32>` explicitly and validate before constructing:

```rust
OPCODE_SCAN_DELTA => {
    let source = src_tab.ok_or_else(|| "SCAN_DELTA missing source_table".to_string())?;
    let cols: Vec<u32> = cols.iter()
        .filter(|c| c.kind == NODE_COL_KIND_SCAN_BOUND)
        .map(|c| c.value1 as u32)
        .collect();
    let bound = if cols.is_empty() {
        None
    } else {
        if cols.len() > gnitz_wire::PK_LIST_MAX_COLS {
            return Err(format!("SCAN_DELTA bound has {} index columns, max {}",
                               cols.len(), gnitz_wire::PK_LIST_MAX_COLS));
        }
        let blob = expr_blob.ok_or_else(|| "SCAN_DELTA bound missing range descriptor".to_string())?;
        Some(ScanBound { idx_cols: PkColList::from_slice(&cols), desc: RangeDescriptor::decode(&blob)? })
    };
    OpNode::ScanDelta { source, bound }
}
```
An unbounded `ScanDelta` is byte-identical to today (no param rows, no blob), so every stored
view circuit round-trips unchanged. **A zero-length `SCAN_BOUND` col-list is not representable**
— `encode_col_list` emits one row per element, so "absent" and "length 0" are the same bytes and
can only decode to `None`.

**Match-site churn — 10 real sites**, all reading `.source` or constructing `bound: None`:
`gnitz-core/src/circuit.rs:48` (`Circuit::dependencies`), `:157` (`input_delta`), `:169`
(`input_delta_tagged` — a tuple construction, so it **must** change even though its behavior
does not); `compiler/load.rs:232` (`scan_source_ids`), `:432` (`scan_tid_through_filters`);
`compiler/emit.rs:282` (`emit_node`), `:987` (`all_sources_replicated`); `compiler/optimize.rs:16`
(`compute_join_shard_map`); `gnitz-wire/src/circuit.rs:440`, `:541`. Plus **47** mechanical
fixture edits: 46 in `query/compiler/mod.rs` (inside `#[cfg(test)] mod tests`, line 474) and 1 at
`query/dag/mod.rs:1179`. (`catalog/tests/mod.rs:96` builds rows from the raw `OPCODE_SCAN_DELTA`
and is unaffected. `scan_delta_targets`, named by an earlier draft, does not exist.)

## 6. Engine: thread the bound to both source drives

`CompileOutput` (`query/compiler/mod.rs:187`) gains `pub source_bounds: HashMap<i64, ScanBound>`.

**It cannot be populated from `emit_node`.** `EmitCtx::source_reg_map` (`emit.rs:139`) flows into
`SubPlan` (`emit.rs:1104`/`1115`, field at `compiler/mod.rs:129`), never into `CompileOutput`,
which is built by a single closure capturing only four values:
```rust
let annotated = |shape: PlanShape| CompileOutput {
    shape, co_partitioned, join_shard_map, range_join_n_eq, skips_exchange,
};                                          // compiler/mod.rs:356-362
```
Worse, an `Exchanged` shape runs `build_plan` once per side plus post (`compiler/mod.rs:374-400`)
— exactly the GROUP BY shape this plan targets — so an `EmitCtx`-sourced map would need a
cross-sub-plan merge. Instead sweep `loaded.nodes` once in `compile_loaded`, sibling to the
existing `let range_join_n_eq = circuit_range_join_n_eq(&loaded);` (`compiler/mod.rs:353`), and
add the field to `annotated`'s captures — the same shape `all_sources_replicated` (`emit.rs:987`)
and `compute_join_shard_map` (`optimize.rs:16`) already use.

`DagEngine.cache: FxHashMap<i64, CompileOutput>` (`query/dag/mod.rs:246`) is keyed by view id
**and** transient tid (`compile_transient` inserts at `mod.rs:443`), so one accessor serves both:

```rust
/// The backfill-scan bound for `source` under `view_id`, if the compiled plan pushed one down.
/// By value (`ScanBound: Copy`) so callers can open a cursor on `self` without holding a borrow.
pub fn source_scan_bound(&self, view_id: i64, source: i64) -> Option<ScanBound> {
    self.cache.get(&view_id).and_then(|co| co.source_bounds.get(&source).copied())
}
```

### 6a. `SourceCursor` + `BoundedIndexCursor` (`catalog/store_io.rs`)

`open_store_cursor` stays (its other callers — CREATE INDEX backfill `ddl.rs:582`, unique
pre-flight `worker/mod.rs:1471` — must keep full-scanning). Add one sibling both drivers call:

```rust
/// The source cursor for driving `source` through `view_id`'s circuit: an index-bounded
/// cursor when the compiled plan pushed a bound down and the index circuit resolves, else the
/// full-scan cursor. Falling back is a soundness backstop, never wrong — the circuit's
/// `Filter` is authoritative, so a stale bound costs speed, not correctness.
pub(crate) fn open_source_cursor(&mut self, view_id: i64, source: i64) -> Option<SourceCursor>;

pub(crate) enum SourceCursor { Full(ReadCursor), Bounded(BoundedIndexCursor) }
impl SourceCursor { pub(crate) fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch>; }
```
`ReadCursor::drain_chunk` returns `Option<Batch>` (`read_cursor/output.rs:178`), so
`handle.as_mut().and_then(|h| h.drain_chunk(chunk_rows))` (`worker/mod.rs:1200`) works unchanged
against `Option<SourceCursor>`.

**`open_source_cursor` must ensure the plan is compiled first.** `backfill_view` guards with
`if !self.dag.ensure_compiled(vid) { return; }` before its loop (`ddl.rs:389`), but
`handle_backfill` does **not** — lines 1178-1193 run the `invalid_views` reset → `has_id` →
`get_schema_desc` → `open_store_cursor`, and `ensure_compiled` is reached only later, inside
`backfill_view_step` → `backfill_view_step_multi_worker` (`exec.rs:52`/`:316`). Without this,
`self.dag.cache` has no entry for a freshly created exchange view, `source_scan_bound` returns
`None`, and the **motivating GROUP BY case silently full-scans** — invisible, because the
fallback is by design never wrong. `ensure_compiled` is cache-first and idempotent
(`dag/mod.rs:495-497`), so it is also correct for a transient, whose cache entry
`compile_transient` already seeded (`worker/mod.rs:1295`, before the `handle_backfill` at `:1302`)
and whose sys tables hold no circuit rows. So `open_source_cursor` opens with
`self.dag.ensure_compiled(view_id)` and only then reads `source_scan_bound`.

**`open_source_cursor` returns `None` iff the source is absent** — byte-identical to
`open_store_cursor`'s contract (`self.dag.tables.get(&table_id).map(...)`, `store_io.rs:442-443`),
which `backfill_view`'s `else { continue; }` (`ddl.rs:401`) relies on. A *provably empty* range
(the `start.is_none()` or `start >= end` short-circuits — reachable via an inverted bound the
planner does not pre-reject, e.g. `x > 5 AND x < 3`, named at `store_io.rs:357-358`) must **not**
surface as `None`; it is a `Bounded` cursor whose first `drain_chunk` returns `None`. Collapsing
the two would make `backfill_view` skip the source entirely.

**The source must be `unique_pk`; otherwise fall back to `Full`.** The gather this cursor reuses
(`resolve_source_pks`) resolves by **source PK alone** and emits one `(PK, payload)` group per PK
— which is wrong on a `unique_pk = false` base table, where one PK can carry several live payloads
differing in the indexed column: rows are lost and weights doubled. That is a **pre-existing bug
in the thin index path** (`seek_by_index` / `seek_by_index_range`), tracked and fixed separately;
this plan must not promote it into durably persisted, incrementally maintained views. So
`open_source_cursor` gates on `entry.unique_pk()` (`query/dag/mod.rs:~192`) and full-scans
otherwise — consistent with "falling back is a soundness backstop, never wrong". Every
SQL-created table is `unique_pk`, so the gate costs nothing on the SQL path; only binary clients
(`gnitz-py`'s `create_table(..., unique_pk=False)`) hit it.

`BoundedIndexCursor` fields, resolved once at construction from the `IndexCircuitEntry` that
`table_and_index(source, bound.idx_cols.as_slice())` returns (exact ordered-list equality on
`col_indices`; `Err` → fall back to `Full`):

- `idx: Option<ReadCursor>` — the index cursor, `seek_bytes(&start[..idx_pk_stride])`; `None`
  once abandoned (§3).
- `src: ReadCursor` — the base cursor, **held for the cursor's whole life**.
- `end: Option<PkBuf>` — exclusive end key (`None` = run to the end of the index).
- `idx_key_size`, `src_pk_stride`, `src_schema`, `idx_pk_stride`, a reusable `Vec<PkBuf>`,
  `cap: usize`, `collected: usize`, `emitted_any: bool`.

Construction reuses `seek_by_index_range`'s cut math **verbatim** (`store_io.rs:333-360`:
`ic.key_spec.prefix(n_eq + 1)`, the `cut_key` closure over `spec.seek_prefix` +
`increment_key_in_place`, the `start.is_none()` → empty and `start >= end` → empty
short-circuits), so range edges and OPK ordering are byte-identical to the seek path. Extract
that block into a shared `fn index_range_keys(ic: &IndexCircuitEntry, range: &RangeDescriptor)
-> Option<(PkBuf, Option<PkBuf>)>` called by both — one definition of the range edges, not two.

`drain_chunk(n)`, bounded path:
1. Advance `idx` collecting up to `n` **positive-weight** in-range source-PK suffixes
   (`cur_pk[idx_key_size..idx_key_size + src_pk_stride]`), breaking at `end`. While
   `!emitted_any`, keep walking past `n` up to `cap`; if `collected > cap`, **abandon**: set
   `idx = None`, `src.rewind()`, and fall through to the full path (§3).
2. **Exhaustion is the index walk's verdict, never the gather's.** `drain_chunk` returns `None`
   **iff** the index cursor is past `end`/invalid; otherwise `Some(batch)` — *including
   `Some(empty)`* when the walk collected PKs but the gather resolved nothing. This is not
   pedantry: the reused `resolve_source_pks` returns `None` at `acc.count == 0`
   (`store_io.rs:263`), and both drivers read `None` as exhaustion — `let pad = drained.is_none();`
   → `break` (`worker/mod.rs:1201,1217`) and `None => break` (`ddl.rs:407-422`). Letting the
   gather's `None` escape would **silently truncate the view mid-range**. A bounded cursor is
   monotone — once it returns `None` it never yields data again — so the pad/termination protocol
   is otherwise unaffected, and an empty-range worker pads alongside its peers exactly as a worker
   whose partition slice drained early already does.
3. `sort_unstable()` — a range spans many duplicate groups, so collected PKs interleave;
   `resolve_source_pks` requires ascending order for its monotone sweep (it `debug_assert!`s it).
4. Gather with `resolve_source_pks_with(&mut self.src, src_schema, &pks)`, set `emitted_any`.

`drain_chunk(n)`, abandoned/full path: `self.src.drain_chunk(n)`.

Step 4 needs **no re-seek between chunks**: `advance_to_exact_live` calls `advance_to`, which is
explicitly **backward-capable** — "an out-of-order key only forfeits the speedup, never
correctness", and an unordered batch sharing one cursor already uses it via "the bounded
`[0, position)` backward branch" (`read_cursor/mod.rs:366-378`). Chunk N+1's first PK may sort
below chunk N's last; the cursor absorbs it.

`src` is **held** rather than re-opened per chunk because `open_store_cursor` returns an **`Rc`
snapshot** of the store's runs, and the transient drive runs concurrently with live traffic
(`yield_between_chunks = true`, `worker/mod.rs:1302`). Re-opening per chunk would give successive
chunks **different base snapshots** — a torn read no full scan can produce. One held snapshot
preserves the existing single-snapshot contract, and it is what makes `rewind()`-on-abandon free.

**Both cursors are opened inside `open_source_cursor`, one synchronous step with no reactor
yield.** The worker is single-threaded and `ingest_store_and_indices` writes base-then-index
non-atomically, so a yield between the two opens could snapshot a base row whose index entry is
not yet visible — losing a row a full scan would have returned. No yield, no interleave, so the
pair is consistent by construction.

**The gather is weight-identical to the full scan it replaces.** Both emit only positive-weight
rows: `drain_to_batch` yields consolidated merge groups (ghost elimination drops net-zero) and
base-table weights are non-negative by the positivity invariant, while `advance_to_exact_live`
gates on `self.valid && current_pk_eq(key) && current_weight > 0` (`read_cursor/mod.rs:376-380`)
and `copy_current_row_into(&mut acc, w)` carries that same consolidated weight. No source PK is
collected twice from one range: one index entry per (row, index), and a bound only ever exists
over a base table (§7b's gate), where every SQL-created table is registered `unique_pk`.

**Refactor** `resolve_source_pks` — real signature `fn resolve_source_pks(handle: &StoreHandle,
src_schema: SchemaDescriptor, pks: &[crate::storage::PkBuf]) -> Option<Batch>`, an **associated
fn** (`Self::resolve_source_pks`, `store_io.rs:241`) that opens its own cursor via
`handle.open_cursor()` — into `resolve_source_pks_with(cursor: &mut ReadCursor, src_schema, pks)`.
`seek_by_index_range` keeps its behavior by hoisting `entry.handle.open_cursor()` itself; hoist it
**before** the index walk builds `pks`, since `entry` is a `&self` borrow from `table_and_index`
that is live across the `ic.table_mut().open_cursor()` walk.

### 6b. Both drivers swap one line

There are **two** drivers, not one — an earlier draft patched only the one its own attachment
site does not feed:

| Shape | Driver | Site |
|---|---|---|
| **Live CREATE** of a plain linear view (no exchange) | `backfill_view`, per-worker over its local slice | `catalog/ddl.rs:401` |
| Live CREATE of an exchange view (GROUP BY, join); every ad-hoc transient; **and the post-recovery rebuild of _every_ invalid view, exchange or not** | `handle_backfill` | `runtime/orchestration/worker/mod.rs:1190` |

`backfill_view` drives **live CREATE only** — its call site is gated on `self.ctx.is_live()`
(`catalog/hooks.rs:530`, and `:511-514`: *"Boot never backfills views inline either: valid views
resume from their checkpoint and the master drives the recovery tick sweep + invalid-view
rebuild"*). Recovery instead runs `rebuild_invalid_views` → `fan_out_backfill(vid, src)` →
`handle_backfill`, *"for both exchange and non-exchange views"* (`runtime/bootstrap.rs:308-329`).
(`ddl.rs:415-417`'s comment — *"backfill_view drives BOTH live CREATE and the post-recovery
rebuild"* — is **stale**; do not reason from it.) Live routing is
`!dag.view_seeds_exchange_backfill(vid)` → `backfill_view` (`catalog/hooks.rs:528-534`);
otherwise the live DDL handler's `fan_out_backfill` → `handle_backfill`. The transient drive calls
`handle_backfill(target_id, tid, request_id, true)` (`worker/mod.rs:1302`). Both sites have the
view/transient id and the source id in scope (`vid` is a fn param at `ddl.rs:401`, used at `:389`;
`handle_backfill(&mut self, source_tid: i64, view_id: i64, request_id: u64,
yield_between_chunks: bool)`):

```rust
// catalog/ddl.rs:401, inside `for source_id in source_ids`
let Some(mut handle) = self.open_source_cursor(vid, source_id) else { continue; };

// runtime/orchestration/worker/mod.rs:1189-1193
let mut handle = if has { self.cat().open_source_cursor(view_id, source_tid) } else { None };
```

Everything downstream is untouched: pad rounds, exchange, `backfill_signal` gating, and the flush
tails all keep their exact semantics.

**The global-aggregate ground row is not at risk, and it is worth saying why.**
`backfill_view`'s `first`-round empty-epoch seed fires a global aggregate's `n == 0` row only
when the source is dry on the **first** iteration (`ddl.rs:406-423`, the `None if first =>` arm
at `:418`), so an empty bounded range changes when it fires. Neither in-scope shape can lose it:
a GROUP BY compiles to a reduce → `needs_exchange` → `view_seeds_exchange_backfill` is true
(`dag/meta.rs:375-377`) → it is driven by `handle_backfill`, which **always** feeds a chunk
(`drained.unwrap_or_else(|| Batch::empty_with_schema(&schema))`, `worker/mod.rs:1202`); and a
linear view has no reduce and no ground row. This holds **because of §7e's scope restriction** and
re-arms the moment a third attachment site appears — so §8 pins
`SELECT COUNT(*) FROM t WHERE indexed = <no-match>` as a live test rather than an argument.

## 7. SQL planner: attach the bound, keep the `Filter`

### 7a. `CircuitBuilder` gets one method

```rust
/// A delta input over the builder's primary source whose backfill scan is bounded to a
/// secondary-index range. Identical to `input_delta` except the initial full-source scan
/// reads only the index range; the caller still emits the full `Filter` downstream.
pub fn input_delta_bounded(&mut self, bound: Option<ScanBound>) -> NodeId {
    self.alloc_node(OpNode::ScanDelta { source: self.primary_source_id, bound })
}
```
`input_delta()` becomes `self.input_delta_bounded(None)`. Both attachment sites use
`CircuitBuilder::new(view_id, source_tid)` + bare `input_delta()`, so this one method serves both.

### 7b. The base-table gate

`Binder::resolve`'s cache "holds base-table resolutions *and* CTE/derived-table aliases (**which
never reach the catalog**)" (`bind/resolve.rs`, `resolve`'s own doc). So a resolved id may be a
**chain-minted provisional id**, and a transient chain mints them `1, 2, 3, …`
(`ViewChain::new_transient`, `plan/view/mod.rs:61-67`, `mint_id` at `:71-80`). Those alias real
relation ids: `SCHEMA_TAB = 1`, `TABLE_TAB = 2`, `VIEW_TAB = 3`, `COL_TAB = 4`, `IDX_TAB = 5`,
`FIRST_USER_TABLE_ID = 16` (`gnitz-wire/src/catalog.rs:173-200`), and `MAX_CHAIN_SEGMENTS = 64`.
So a ≥16-segment bundle mints ids that collide with **real user tables** — and
`consume_leading_eq_prefix` (`dml/plan.rs:392-405`) matches a foreign table's index **column
indices** against the segment's schema column indices by pure numeric coincidence, producing a
bogus bound and burning a `GET_INDICES` round-trip per segment.

That bogus bound does not currently corrupt results — the master remaps the id to a transient tid,
whose store has no index circuits, so `table_and_index` errors (`store_io.rs:142-146`) and
`open_source_cursor` falls back to `Full`. **Do not rely on that.** It is an accident of two
unrelated mechanisms, and the fallback is not designed for this case.

**Gate extraction on catalog provenance.** Add a `from_catalog: bool` to the binder's resolution
cache entry — `true` only on the catalog-probe path, `false` where a CTE/derived alias is inserted
— and expose `Binder::is_catalog_relation(&self, name: &str) -> bool`. `scan_bound_for` is called
only for a source that resolved from the catalog. A durable hidden segment id is harmless either
way (it is a real view id, and `GET_INDICES` on a view returns an empty list — a view cannot own
an index, §0), but the gate removes both the provisional-id hazard and a pointless round-trip.

### 7c. One shared extractor — `gnitz-sql/src/plan/index_bound.rs` (new)

```rust
/// The best index range/equality bound for `where_expr` over base table `tid`, or `None` when
/// no index covers a leading conjunct. Range candidates first (a range candidate extends an
/// equality prefix, so it is never less selective than the bare prefix it extends), then the
/// best equality candidate as a degenerate point range. Both collectors are lazy — a WHERE
/// with no usable conjunct fetches no indexes. The residual is discarded: the caller emits the
/// FULL predicate as the `Filter`, so unlike the thin path there is no `bind_thin_residuals`
/// pre-filter and no `first_index_hit` probe — the best candidate is taken outright.
/// Callers MUST gate on `Binder::is_catalog_relation` (§7b).
pub(crate) fn scan_bound_for(
    client: &mut GnitzClient,
    tid: u64,
    where_expr: &sqlparser::ast::Expr,
    schema: &Schema,
) -> Result<Option<ScanBound>, GnitzSqlError> {
    // One GET_INDICES round-trip serves both collectors: `table_indexes` ALWAYS hits the wire
    // (its epoch cache only skips re-decoding), so the range→equality fall-through must not
    // fetch the same list twice (the `select.rs:145-168` idiom, which borrow-checks because
    // the first closure's borrows die with the `let ranges = ...;` temporary).
    let mut idx_memo: Option<Arc<Vec<gnitz_core::IndexMeta>>> = None;
    let ranges = collect_index_range_candidates(where_expr, schema, || {
        let list = client.table_indexes(tid)?;
        idx_memo = Some(Arc::clone(&list));
        Ok(list)
    })
    .map_err(GnitzSqlError::Exec)?;
    if let Some(c) = ranges.into_iter().next() {
        return Ok(Some(ScanBound { idx_cols: c.idx_cols, desc: c.desc }));
    }
    let seeks = collect_index_seek_candidates(where_expr, schema, || match idx_memo {
        Some(list) => Ok(list),
        None => client.table_indexes(tid),
    })
    .map_err(GnitzSqlError::Exec)?;
    Ok(seeks.into_iter().next().map(|(idx_cols, vals, _residual)| {
        // A pure n-column equality has NO range column, so `RangeDescriptor` cannot express it
        // directly. Encode the LAST equality as a degenerate point range over that column:
        // eq = vals[..n-1], start = Before(v_last), end = After(v_last). `seek_by_index_range`
        // then cuts [seek_prefix(vals), seek_prefix(vals)+1) — exactly that key group, which is
        // the algebra `range.rs:17-22` already documents. `n >= 1` (the collector only emits
        // covered candidates) and `consume_leading_eq_prefix` caps `n` at
        // `idx_cols.len() <= PK_LIST_MAX_COLS = 4`, so `n-1 <= 3 < 4` satisfies
        // `RangeDescriptor::new`'s assert and `seek_by_index_range`'s `n_eq < col_indices.len()`.
        let n = vals.len();
        let last = vals[n - 1];
        let desc = RangeDescriptor::new(&vals[..n - 1], Cut::Before(last), Cut::After(last));
        ScanBound { idx_cols, desc }
    }))
}
```

> **The degenerate-range conversion is mandatory, not a nicety.** For the motivating
> `WHERE indexed_col = 5`, `collect_index_range_candidates` returns **empty** — it bails at
> `if ends.is_empty() { return Ok(Vec::new()); }` (`plan.rs:609-611`) because a pure equality
> contributes no range end. Range candidates alone leave the headline case unbounded.

`gnitz-sql` depends on `gnitz-wire` directly, and `gnitz-core` re-exports `Cut`/`PkColList`/
`RangeDescriptor`; `ScanBound` is wire-only (`gnitz_wire::ScanBound`).

### 7d. Two attachment sites

The bound must come from the **raw `sqlparser::ast::Expr`** — that is what the collectors take.
This rules out `simple.rs`: `emit_linear(view_id, rel)` is **sync and client-free**, and its
`Rel::Filter { pred }` carries a `BoundExpr` (`plan/lp.rs:31`), not an `Expr`.

- **Linear** — `plan/lp.rs:41 lower_linear` has `client`, the raw `select.selection`, and the
  resolved `source_schema`. Carry the result on the IR: `Rel::Source { tid, schema, bound:
  Option<ScanBound> }`. `Rel::Source` has exactly **3 sites**: `lp.rs:60` (construct — set the
  bound), `lp.rs:103` (`passthrough_rel` — `None`), `simple.rs:51` (destructure). **Hoist the
  extraction above line 60**: `Rel::Source` is built at `lp.rs:60`, *before* the `if let
  Some(where_expr) = &select.selection` block at `:66` (which only wraps `rel` in a
  `Rel::Filter`), so there is no `Source` to mutate inside that block.
  ```rust
  let bound = match &select.selection {
      Some(e) if binder.is_catalog_relation(&table_name) =>
          scan_bound_for(client, source_tid, e, &source_schema)?,
      _ => None,
  };
  let mut rel = Rel::Source { tid: source_tid, schema: Rc::clone(&source_schema), bound };
  ```
  `emit_linear_opts` destructures the new field and calls `cb.input_delta_bounded(bound)` at
  `simple.rs:78` — **still no client, no signature change**.
- **GROUP BY** — `plan/view/group_by.rs:163 emit_group_by_pieces` already takes
  `client: &mut GnitzClient` and holds `select.selection` + `source_schema`. Call `scan_bound_for`
  (behind the same gate) before `CircuitBuilder::new` and use `cb.input_delta_bounded(bound)` at
  `group_by.rs:382`. Its own WHERE emit (`group_by.rs:385-391`) is **unchanged**.

The `Filter` is emitted verbatim at both sites — the full predicate, index conjuncts included.

### 7e. Non-goals (committed scope)

- **Joins are out of scope, and the reason is not the one an earlier draft gave.** It is *not*
  that `join.rs` lacks a client — `plan_join_chain(client: &mut GnitzClient, ...)` (`join.rs:491-497`)
  has the client, the raw `select.selection` (`join.rs:636`), and both side schemas
  (`join.rs:641-654`), and `input_delta_tagged` builds the identical node. The real blockers are
  two:
  1. **A join's WHERE is a post-join filter**, not a per-side one:
     `let post_filter: Vec<Expr> = residual.into_iter().chain(where_filter).collect();`
     (`join.rs:1054`) → `cb.filter(merged, Some(prog))` (`join.rs:1062-1063`); for INNER,
     `classify_join_where` folds the whole WHERE into `residual` (`join.rs:346-348`).
  2. **The collectors discard the qualifier.** `single_relation_col_name` returns
     `Expr::CompoundIdentifier(p) if p.len() == 2 => Some(&p[1].value)` (`ast_util.rs:532`) and
     backs `try_col_eq_literal` → `collect_eq_conjuncts` (`dml/plan.rs:165, 327, 379`). Calling
     `scan_bound_for(client, left_tid, where_expr, &left_schema)` on a join WHERE would resolve
     `small.indexed = 5` against `left_schema` whenever the left side also has a column named
     `indexed` — **bounding the left scan by the right side's predicate. Silent wrong rows, and
     §6a's fallback does not catch it** (the index resolves fine).

  Covering joins therefore requires partitioning `post_filter` conjuncts by alias via
  `collect_column_refs` + `alias_map` (`join.rs:389-395`, `:610-628`), keeping only conjuncts whose
  refs all resolve to one side, and restricting to `join_type == Inner` (an outer null-fill's
  `ν_P = positive_part(P − π_P(inner))` reads the **raw** preserved input, `join.rs:960-977`, so a
  bounded preserved-side scan changes what null-fills and needs its own proof). That is a separate
  plan, not a follow-on to this one.
- **`set_op.rs:84` and `exists.rs:442/447`** stay unbounded: `emit_distinct_pieces`
  (`set_op.rs:407`) and `exists.rs::open_circuit` have no client.
- **Non-`unique_pk` sources full-scan** (§6a's gate), until the pre-existing PK-only gather bug in
  the thin index path is fixed. Not reachable from SQL — only from binary clients.
- **One index per source.** The single best candidate. No multi-index intersection/union.
- **The `Filter` is never eliminated or trimmed.** No residual-into-scan.

## 8. Tests

- **Benchmark first.** Add `CREATE INDEX` on the filtered column to
  `benchmarks/features/test_reads.py:115` (`CREATE VIEW v{i} AS SELECT g, SUM(v) AS s FROM t
  WHERE v >= {i} GROUP BY g` — already the exact target shape, just unindexed) and record a
  baseline. Without it the feature has no number and §3's abandon ratio is unverifiable.
- **Rust unit** (`gnitz-wire`): a bounded `ScanDelta` round-trips `encode_op_node` →
  `decode_op_node` preserving `idx_cols` order and the `RangeDescriptor`; an unbounded
  `ScanDelta` encodes byte-identically to the pre-change form; a `SCAN_BOUND` col-list longer
  than `PK_LIST_MAX_COLS`, and a bounded node with no blob, each decode to `Err` (no panic).
- **Rust unit** (`gnitz-sql`): `scan_bound_for` maps `WHERE a = 5` on a 1-col index to
  `n_eq == 0, start == Before(5), end == After(5)`; `WHERE a = 5 AND b = 7` on `(a,b)` to
  `n_eq == 1, eq == [5]` with a point range on `b`; `WHERE a = 5 AND b > 10` on `(a,b)` to the
  range candidate, not the seek; `WHERE pk = k` → `None` (PK columns are never eq conjuncts);
  no index → `None` with **zero** `table_indexes` calls.
- **Rust unit** (`gnitz-engine`): `BoundedIndexCursor` yields exactly the in-range
  positive-weight rows across multiple `drain_chunk` calls with a chunk boundary mid-range, and
  equals full-scan + the equivalent `Filter`; a ghost index entry and an index entry with no live
  base row are both skipped; the degenerate point range from a pure equality returns exactly that
  key group; **a range exceeding `cap` abandons and returns the full scan's exact rows, with no
  row emitted twice** (§3); a bound whose index circuit is absent falls back to `Full`; a
  `unique_pk = false` source falls back to `Full` (§6a's gate) and returns full-scan-identical
  **weights** with two live payloads under one PK; an in-range PK whose gather resolves nothing
  yields `Some(empty)` and the walk continues to the range end (§6a's exhaustion rule); an
  inverted bound (`x > 5 AND x < 3`) yields a cursor returning `None`, **not**
  `open_source_cursor -> None`.
- **Rust unit** (`gnitz-engine`): drive `open_source_cursor` on `handle_backfill`'s ordering for a
  **freshly created** exchange view and assert it returns `Bounded` — the `ensure_compiled` guard
  (§6a). Without this the motivating case ships silently dead: every other test passes with the
  bound never consulted.
- **Integration** (`gnitz-sql`): `SELECT indexed_col, COUNT(*) FROM t WHERE indexed_col = 5 GROUP
  BY indexed_col` (routed to the executor by the GROUP BY — `select.rs:69,73`) and the same as a
  `CREATE VIEW` both compile a `ScanDelta` carrying the bound; a `BETWEEN` range and an
  equality-prefix-plus-range over a compound index likewise; a WHERE over an unindexed column, and
  a GROUP BY **over a join**, both compile `bound: None` (§0).
- **E2E** (`gnitz-py`, `GNITZ_WORKERS=4`): over a partitioned base with a secondary index, an
  ad-hoc `WHERE indexed = k GROUP BY …` and a `CREATE VIEW` with the same WHERE return
  **identical rows and weights** to the unbounded build — the W=4 run is what guards §2's
  co-location claim; `SELECT COUNT(*) FROM t WHERE indexed = <no-match>` and its CREATE VIEW twin
  return one row with count 0 (§6b's ground-row seed); a steady-state push of a row failing the
  index predicate to a maintained bounded view is dropped by the `Filter`; dropping the index
  mid-life falls back to a full scan and still returns correct results.

**No timing assertion in E2E** — the suite's variance cannot resolve it. Cost evidence is the
benchmark above; row-count evidence is the deterministic cursor unit test.

## 9. Sequencing

Each step compiles and is callable on its own. `make clippy` is `--all-targets … -D warnings`
(`Makefile:51`) and `--all-targets` builds the lib **without** `cfg(test)`, so a `pub(crate)` item
with no non-test caller trips `dead_code` — and `#[allow(dead_code)]` band-aids are forbidden.
That is why the cursor, the compiler field, and both driver swaps are **one** commit, and the
extractor ships **with** its call sites.

1. **Wire** — `ScanBound`, the `ScanDelta` struct variant, `NODE_COL_KIND_SCAN_BOUND`,
   encode/decode + validation, the 10 real match sites, the 47 fixture edits; wire unit tests.
2. **Client** — `CircuitBuilder::input_delta_bounded`; `input_delta` delegates to it.
3. **Engine** — `index_range_keys` extraction, the `resolve_source_pks_with` refactor,
   `BoundedIndexCursor` (incl. the §3 cap/abandon), `SourceCursor`, `open_source_cursor` (incl.
   `ensure_compiled`), `CompileOutput::source_bounds` computed in `compile_loaded`,
   `DagEngine::source_scan_bound`, **and both driver swaps**; cursor + `open_source_cursor` unit
   tests.
4. **Planner** — `Binder::is_catalog_relation` (§7b), `plan/index_bound.rs`, `Rel::Source.bound`
   + the `lower_linear` hoist, the `emit_group_by_pieces` attachment; planner unit tests.
5. **Tests** — benchmark index + baseline, integration, E2E; `make verify` + `make e2e WORKERS=4`
   green.
