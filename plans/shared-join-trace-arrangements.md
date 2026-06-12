# Shared join-trace arrangements

## 1. Summary

Every materialized join view compiles its own `integrate_trace` backing tables.
Two views that both `JOIN orders ON orders.customer_id = …` each maintain a
private, full copy of *orders reindexed by customer_id* — same bytes, same
per-tick integrate write, stored N times for N views. This plan factors the
**integrated trace** out of individual view circuits into a per-worker,
reference-counted **arrangement registry** keyed by `(source table, key columns,
promoted key types)`. The arrangement is maintained **once per source push**
regardless of how many views consume it; consumer joins bind their trace port to
the shared arrangement table.

What is shared is the *persistent integrated state* (the trace table and its
once-per-tick maintenance write). The transient per-tick delta reindex that
feeds a join's **delta** port stays per-consumer (it is cheap, not stored, and
sharing it would require restructuring every consumer's delta plumbing — see
§2). Net effect: trace storage and trace-maintenance CPU stop scaling with view
count and start scaling with *distinct (source, key)* pairs.

## 2. Scope and non-goals

**In scope**
- A per-worker arrangement registry: `(source table_id, ordered key columns,
  per-slot promoted target type codes, trace kind) → refcounted backing
  `Table` + internal table_id`.
- Maintaining each live arrangement exactly once per source push (project +
  exchange-by-key + integrate), independent of any view.
- Binding a view join's trace port to a shared arrangement instead of a private
  `integrate_trace`.
- Refcount lifecycle: an arrangement outlives any single consuming view; it is
  dropped only when its last consumer drops.
- The exact equality predicate that decides two views may share an arrangement.

**Non-goals**
- **Sharing the delta-side reindex.** Each consumer continues to compute its own
  per-tick `map_reindex` of the source delta for the join's delta input. Only the
  integrated arrangement (stored state + its integrate write) is shared. The
  delta reindex is transient and not stored, so it is not the cost driver.
- **Cross-worker sharing.** Arrangements are per-worker structures (each worker
  holds only its own key-shard). The registry is per-worker; "shared" means
  shared *within a worker*, across that worker's view plans.
- **Cross-view operator (compute) CSE** beyond the trace — filters, maps, and
  joins remain per-view.
- **Persisting arrangements across reboot.** Like today's traces, arrangements
  are ephemeral and rebuilt from durable sources at boot (§7).

## 3. Current architecture (anchors)

A SQL join view builds, per side, in `execute_create_join_view`
(`crates/gnitz-sql/src/planner.rs:1136-1184`):

```rust
let reindex_b = cb.map_reindex(input_b, &right_join_cols, &right_target_tcs, right_reindex_prog);
let reindex_a = cb.map_reindex(input_a_match, &left_join_cols, &left_target_tcs, left_reindex_prog);
let trace_a   = cb.integrate_trace(reindex_a);
let trace_b   = cb.integrate_trace(reindex_b);
let join_ab   = cb.join_with_trace_node(reindex_a, trace_b);   // ΔA ⋈ I(B)
let join_ba   = cb.join_with_trace_node(reindex_b, trace_a);   // ΔB ⋈ I(A)
```

- **Trace creation**: compiling `OpNode::IntegrateTrace`
  (`crates/gnitz-engine/src/compiler.rs:1444-1463`) calls `create_child_table`
  (`compiler.rs:1018-1030`) — a fresh **ephemeral** `Table` whose schema is the
  *reindexed* input schema (PK = the packed `_join_pk`). The table carries the
  owning **view's** `view_table_id`, not a distinct id; it is identified only by
  its directory `_int_{view_id}_{node_id}` and lives in the VM's `owned_tables`,
  tracked by `owned_trace_regs: Vec<(reg_id, table_idx)>` (`compiler.rs:1450-1455`).
  **Trace tables are not in `dag.tables` and have no catalog table_id.**
- **Two trace-binding surfaces**:
  1. `join_with_trace_node` (`crates/gnitz-core/src/circuit.rs:352-382`) reads
     the `IntegrateTrace` node directly over `PORT_TRACE`; no table_id, no
     `ScanTrace`. This is the SQL-join surface.
  2. `ScanTrace(table_id)` (`OPCODE_SCAN_TRACE_TABLE = 31`, compiler.rs:1109-1149)
     binds a trace register to an **external table** found in `ext_tables` by
     `table_id`, recorded in `ext_trace_regs`; when it feeds a join's
     `PORT_TRACE` (`is_join_trace_side`, compiler.rs:1036-1055) no `add_scan_trace`
     instruction is emitted — the join reads the external table's cursor. Cursors
     for `ext_trace_regs` are opened **compacting** at `dag.rs:1375-1399`.
     This surface **already addresses a trace by a real table_id** and is the
     binding mechanism this plan reuses.
- **Maintenance**: there is no background compactor; traces are compacted on the
  read path (`vm.rs:562-575` owned, `dag.rs:1375-1399` external). The
  `IntegrateTrace` VM op accumulates the input delta into the trace each tick;
  trace registers (`reg_kinds == 1`) are *preserved* across epochs while delta
  registers are cleared (`vm.rs:1225-1254`, `ddl.rs:381-386`).
- **Serialization**: a view circuit persists as rows in `CircuitNodes` /
  `CircuitEdges` / `CircuitNodeColumns` (`catalog/sys_tables.rs:281-319`);
  `Circuit::into_rows`/`from_rows` (`gnitz-core/src/circuit.rs`). Reindex columns
  persist as `NODE_COL_KIND_REINDEX` rows (`value1 = source col`,
  `value2 = promoted target tc`).
- **Build**: `build_plan` (`compiler.rs:1884-1917`) (re)creates every trace table
  on each compile. Views are `RelationKind::View` ⇒ ephemeral, rebuilt from
  sources at open (`dag.rs:245-266`); `backfill_view` refills them.
- **Co-partition / reindex annotation**: `reindex_cols_through_filters`
  (`compiler.rs:488-524`), `compute_join_shard_cols` (`dag.rs:651-673`).

## 4. Design

### 4.1 Arrangement identity

An arrangement is identified by:

```rust
struct ArrangementKey {
    source_table_id: i64,
    key_cols: PkColList,      // ordered reindex key columns
    target_tcs: [u8; PK_LIST_MAX_COLS], // per-slot PROMOTED type codes (resolved)
    kind: ArrangementKind,    // Payload | DistinctKeyOnly
}
enum ArrangementKind { Payload, DistinctKeyOnly }
```

The equality predicate is the **whole** of "two views may share":

- `source_table_id` equal.
- `key_cols` equal **as an ordered sequence** (`a.x = b.p AND a.x = b.q` reindexes
  `[x, x]` verbatim — the duplicate-preserving sequence from
  `reindex_cols_through_filters`, `compiler.rs:488-524` — and must match
  verbatim, not set-deduplicated).
- `target_tcs` equal **using the resolved/promoted type codes**, not the raw
  source column types. Cross-width / cross-sign join-key promotion is *pairwise*
  (`side_tcs` → `validate_join_key_pair` → `join_key_common_type`,
  `planner.rs:1093-1099, 1439-1451`), so the same source column joined against
  differently-typed counter-keys resolves to different slot widths and is a
  **different** arrangement (its trace stride differs).
- `kind` equal: a full-payload join trace (`Payload`) is never shared with a
  key-only trace (`DistinctKeyOnly`) — the LEFT/anti/semi path builds
  `integrate_trace(distinct(reindex))` whose schema has no payload
  (`planner.rs:1206-1212`) and must not alias a `Payload` arrangement of the same
  `(source, key)`.

Only `MapKind::Expression` equijoin reindexes are eligible. `MapKind::HashRow`
(set-op full-row identity, carries a `branch_id`) and `MapKind::KeyOnly` are not
arrangement sources and are excluded by the predicate.

### 4.2 The registry

A per-worker registry on the `DagEngine`:

```rust
struct ArrangementEntry {
    table_id: i64,            // reserved internal id, for ScanTrace binding
    table: Box<Table>,        // ephemeral, schema = reindex_output_schema
    refcount: u32,
}
arrangements: HashMap<ArrangementKey, ArrangementEntry>
```

Backing tables are registered in `dag.tables` under a reserved internal
table_id range with a new `RelationKind::Arrangement` (a unit variant; the
niche-optimized `size_of::<RelationKind>() == 1` assertion at `dag.rs:281` still
holds — 5 inhabitants fit in one byte). Registering them in `dag.tables` lets the
existing `ScanTrace` resolution (which searches the `ext_tables` passed to
`build_plan`) and the compacting-cursor path (`dag.rs:1375-1399`) work unchanged.
The reserved id range is allocated below `FIRST_USER_TABLE_ID` -adjacent space
(a dedicated band, distinct from system tables) so it never collides with user
tables or views.

### 4.3 Arrangement maintenance (once per source push)

The integrated arrangement is maintained by a dedicated per-arrangement
maintenance step, **not** by any view circuit. The maintenance pipeline for
`ArrangementKey { source, key_cols, target_tcs, Payload }` is exactly the prefix
that view circuits emit today:

```
ScanDelta(source) → [exchange by key_cols] → map_reindex(key_cols, target_tcs) → integrate_trace(arrangement.table)
```

For `DistinctKeyOnly` it is the same with a `distinct` before `integrate_trace`.

This is compiled once per arrangement into a small `Program` stored on the
`ArrangementEntry`. On a source push, the worker runs every arrangement whose
`source_table_id` matches the pushed table **before** running consumer view
plans for that tick, so consumers read an arrangement already updated for the
tick. The exchange repartitions the source delta by `key_cols` across workers
(reusing the existing `ExchangeShard` / `RouteMode::JoinPromote` relay), so each
worker's arrangement table holds only its own key-shard — byte-identical to the
per-worker trace partitioning today.

Because maintenance runs once, the per-tick integrate write that N views each
performed today collapses to one.

### 4.4 Consumer binding

The SQL planner stops emitting `integrate_trace` for a shareable side. Instead:

- It registers/refcounts the arrangement for that `(source, key, target_tcs,
  kind)` (allocating the backing table + maintenance program on first use).
- It emits the join's trace port as `ScanTrace(arrangement.table_id)` bound to
  the shared table (surface 2, §3), replacing `join_with_trace_node(delta,
  integrate_trace_node)` with `join` reading the arrangement.
- It still emits the consumer's own `map_reindex` of the source delta for the
  join's **delta** port (transient, per-consumer, unchanged).

`build_plan` must include every arrangement a view consumes in the `ext_tables`
it is compiled against, so `ScanTrace` resolves (`compiler.rs:1109-1149`).

### 4.5 Scheduling correctness

Today the planner allocates each `integrate_trace` a lower node id than its join
readers so the Kahn schedule (ascending node-id tie-break, `build_plan` rejects a
bad order at `compiler.rs:3502-3516`) updates the trace before any destructive
co-consumer drains the shared register
(`planner.rs:2472-2521`). With maintenance factored out, this ordering becomes a
**cross-step** guarantee: the arrangement-maintenance step for a source must run
before any consumer view plan that reads it, within the same tick. The
per-tick driver enforces this by a two-phase tick: (1) run all arrangement
maintenance for changed sources, (2) run consumer view plans. Within a consumer
plan there is no `integrate_trace` for the shared side, so the intra-plan
ordering hazard for that side disappears.

### 4.6 Lifecycle

- **Bind** (view create / compile): `refcount += 1`; create the backing table +
  maintenance program if absent.
- **Unbind** (view drop / recompile that no longer consumes it): `refcount -= 1`;
  at zero, drop the maintenance program and the backing table and unregister the
  internal table_id. A view's own drop path
  (`hook_view_register` weight<0, `hooks.rs:345-371`) must unbind every
  arrangement the view consumed; the compiled-plan cache must record which
  arrangements each view bound so recompile can rebind idempotently.
- An arrangement therefore survives the drop of any single consuming view as long
  as another consumer remains.

## 5. Data-model / persistence changes

- **`RelationKind::Arrangement`** (unit variant) added in `dag.rs:245-266`;
  ephemeral persistence; the `size_of == 1` assertion at `dag.rs:281` is rechecked
  (holds).
- **No new sys-tables.** Arrangements are derived, per-worker, and rebuilt at
  boot, so they are not serialized. The view→arrangement binding is *recomputed*
  from the view's persisted circuit at compile time (the same `(source, key,
  target_tcs, kind)` the planner derived), so no binding table is persisted.
- **Internal table_id band**: reserve a fixed id range for arrangement tables,
  documented alongside `FIRST_USER_TABLE_ID`.

## 6. Implementation (sites)

| Concern | Site | Change |
|---|---|---|
| Registry struct + map | `dag.rs` (DagEngine) | add `arrangements` map, `ArrangementKey`/`ArrangementEntry`, intern/refcount/drop methods |
| RelationKind | `dag.rs:245-281` | add `Arrangement`; recheck size assert |
| Arrangement maintenance program | new, near `compiler.rs:1884` (`build_plan`) | compile the `ScanDelta→Exchange→reindex→integrate` (and `+distinct`) prefix into a standalone `Program` |
| Two-phase tick | the per-source-push driver (`ddl.rs` epoch driver / worker push handler `worker.rs:1189`) | run matching arrangement maintenance before consumer view plans |
| Planner: emit binding | `planner.rs:1136-1212` (`execute_create_join_view`) | replace per-side `integrate_trace`+`join_with_trace_node` with intern-arrangement + `ScanTrace(arr.table_id)`-bound join; keep delta-side `map_reindex` |
| Equality predicate | new helper (planner + dag) | `ArrangementKey` derive from `(source, reindex_cols, resolved target_tcs, kind)` |
| ext_tables plumbing | `build_plan` caller (`dag.rs` compile path) | include consumed arrangements in `ext_tables` |
| Lifecycle unbind | `hooks.rs:345-371` (view drop) + compiled-plan cache (`dag.rs:804-837`) | record bound arrangements; unbind on drop/recompile |
| Boot rebuild | recovery (`bootstrap.rs` go-live) | re-intern arrangements while recompiling views; maintenance refills from durable sources |

## 7. Recovery

Arrangements are ephemeral. At boot, views recompile from their persisted
circuits; re-deriving each view's `ArrangementKey`s re-interns the arrangements
and rebuilds the maintenance programs. The arrangement tables are refilled by
replaying durable source data through the maintenance pipeline (the same path
`backfill_view` uses for view output today). No arrangement state is read from
disk; the durable base tables are the sole source of truth, identical to the
current per-view trace rebuild.

## 8. Limits

- Sharing is per-worker; an arrangement is bounded by the source relation's
  key-shard on that worker, exactly as a private trace is today.
- Maximum key arity/stride is the reindex key's existing bound
  (`reindex_output_schema` / `ReindexPacker`, ≤ `MAX_PK_BYTES`).
- An arrangement is shared only across `MapKind::Expression` equijoin reindexes
  with identical resolved `(key_cols, target_tcs, kind)`; any divergence yields a
  separate arrangement.

## 9. Testing

- **Identity**: two join views over the same source/key/promotion intern the
  **same** `ArrangementKey`; differing promoted `target_tcs` (same column, different
  counter-key type) intern **distinct** keys; a `Payload` and a `DistinctKeyOnly`
  trace over the same `(source, key)` never alias.
- **Maintenance-once**: with two views sharing an arrangement, a source push
  performs exactly one integrate into the shared table (instrument the integrate
  write count) and both views produce correct output.
- **Correctness parity**: a battery of join views (inner / left / anti / semi /
  multi-join `t ⋈ t1 ⋈ t2`, overlapping key `[x, x]`, nullable key with the
  pre-reindex null filter) produce byte-identical results to the per-view-trace
  baseline.
- **Lifecycle**: dropping one of two sharing views leaves the arrangement alive
  and the surviving view correct; dropping the last consumer drops the table and
  frees the internal id; recompiling a view rebinds idempotently (no refcount
  leak).
- **Scheduling**: an anti-join whose trace-absent branch drains a register still
  observes a fully-updated arrangement (two-phase tick ordering).
- **Recovery**: reboot rebuilds shared arrangements from durable sources; all
  consuming views recover correct contents.

## 10. Invariants preserved

- **Per-worker key-shard partitioning** of the trace is unchanged — maintenance
  exchanges the source delta by the join key exactly as the per-view circuit did,
  so delta and trace co-partition byte-for-byte.
- **Uncompacted-trace tolerance**: a key is "present" iff some entry has positive
  net weight (`ops/join.rs:1-31`); compaction stays a space optimization, run on
  the read path. Sharing does not change this.
- **Maintain-before-read ordering** that the node-id tie-break guaranteed
  intra-plan is preserved as the two-phase tick (maintenance before consumers).
- **Source of truth** is the durable base table; arrangements are derived and
  rebuilt at boot, never persisted.
