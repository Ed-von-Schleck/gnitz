# On-demand query executor: ad-hoc SELECT as a run-once DBSP circuit

## 1. Problem

gnitz has **two query engines that share nothing**:

- **Ad-hoc SELECT** (`crates/gnitz-sql/src/dml/select.rs`) — a thin, client-resident
  keyseek engine: an access-path ladder (PK seek → `pk IN` multi-seek → ordered
  range index → equality index → hard error, `select.rs:103-178`), gather the whole
  reply into one client `ZSetBatch`, project client-side. It has **no relational
  operator**. Joins, GROUP BY, DISTINCT, aggregation, set-ops, and every non-indexed
  WHERE are rejected with "belongs in a CREATE VIEW". Its residual filter is
  **i64-only** (`exec/eval.rs`) — it cannot filter a `TEXT`/`F64` column even when that
  column *is* indexed.
- **CREATE VIEW** (`crates/gnitz-sql/src/plan/view/`) — the full DBSP compiler: every
  relational shape, compiled to a circuit, registered, materialized, maintained
  incrementally, checkpointed, recovered.

Every relational capability is therefore built **twice** (a client keyseek wart and an
engine circuit) or forced into a **persistent, maintained view** even for a one-shot
answer.

The fix is to **unify the compute, not the dispatch**. The relational *compute* — join,
reduce, distinct, set-ops — already exists once, in the operator kernels and the view
compiler. What ad-hoc SELECT lacks is a way to *run that compute once and discard it*.
So: compile a SELECT with the **existing view planner** into the same circuit a CREATE
VIEW builds, run it **once** over the committed base snapshot as a **transient**
(unregistered, non-durable, unmaintained) circuit, stream the result, discard all state.
This is sound in the engine's own theory (CLAUDE.md §3): every operator is "first a
scalar function on Z-Sets, then lifted"; feeding the whole base as deltas from an empty
integral accumulates exactly `Q(base)` (the sum telescopes for linear, non-linear, and
bilinear operators alike) — the scalar form the engine already runs in `backfill_view`.

**The drive is non-blocking.** The engine already drives distributed circuits with
exchange/relay non-blockingly, coexisting with the checkpoint committer, on **every
push** — the tick/maintenance path. The transient reuses **that steady-state SAL
discipline** (§9), not the stop-the-world DDL backfill. So an ad-hoc join or GROUP BY
runs while pushes, seeks, and scans stay live; no cluster freeze. This is the design's
load-bearing property — it is why a transient beats `CREATE VIEW _tmp; scan; DROP`
(which takes the reactor-parked write lock and freezes ingestion for the query's whole
duration).

**Crucially, the dispatch stays split.** A point/index read is one unicast seek with
zero allocations; a circuit is a compile + trace-table allocation + epoch drive. Forcing
the cheap read through the expensive machine is a pure regression. So the **thin keyseek
path is kept, unchanged**, for exactly the reads it serves cheapest; the executor picks
up the shapes the thin path *rejects today*.

**Scope: single-segment circuits.** The view planner does **not** always emit one circuit
— it emits a `ViewChain` *bundle* (`plan/view/dispatch.rs:138-150`, `create_view_chain`,
`MAX_CHAIN_SEGMENTS = 64`). A body compiles to **one** segment iff `chain.segments` stays
empty and only the final view is pushed. From `ViewShape::classify` + the emit functions
(`dispatch.rs:66-136`), the single-segment shapes are: `Simple` filter/map (incl.
non-indexed / typed WHERE); `SetOp`; `Distinct`/`GroupBy` over a **plain** FROM; a **2-way
non-self join** (`plan_join_chain` pushes no intermediate for two distinct relations); and
`EXISTS`/`IN` semi/anti-joins over base tables (`emit_exists_pieces`/`emit_mark_pieces`
take no `&mut chain`). Multi-segment (rejected here, served by a separate chain-driver plan):
3+-way joins, self-joins (a pass-through wrapper segment), `DISTINCT`/`GROUP BY` *over a
join*, correlated scalar subqueries / `ANY`/`ALL`, non-pass-through CTEs, derived tables in
FROM. The routing guard is mechanical: after compile, `segments.len() > 1` is a clean
`Unsupported` error ("use CREATE VIEW"). This boundary is self-enforcing — no
multi-segment query reports `len == 1`, and no single-segment shape is wrongly rejected.

Payoff (single-segment): a **server-side, fully-typed** WHERE (dissolving the i64-only and
non-indexed-error limits), single-table GROUP BY / HAVING / aggregation / DISTINCT,
set-ops, 2-way joins, `EXISTS`/`IN` semi/anti-joins — all **non-blocking** at W>1. No
point-lookup regression; no engine deleted. Ordering/pagination (ORDER BY / OFFSET / LIMIT)
is a separate client-side concern (a self-contained sink over any ad-hoc result); the
executor streams unordered `(PK, payload)` entries.

## 2. The lifecycle model

A view today is *literally* "compile + register + `backfill_view` once + then keep
ticking." An ad-hoc query is that same sequence **truncated before maintenance**: compile,
register (in-memory only), run once, drop. The two are not parallel engines — the query is
a **prefix of the view lifecycle**.

`RelationKind` (`query/dag/mod.rs:90`) is a "property-bundle" enum ("Bundles every per-kind
property... so the nonsense combinations are unconstructable") with three variants:
`SystemCatalog` / `BaseTable { unique_pk }` / `View`. `TableEntry.kind` (`mod.rs:147`) is
non-nullable and `register_table` (`mod.rs:243`) is uncallable without it. Add a **fourth**,
`RelationKind::Transient` — a unit variant (the 1-byte niche assertion at `mod.rs:105` still
holds; only `BaseTable`'s bool consumes niche space). Named for its **storage policy** (never
checkpointed, never recovered, RAM-only scratch), not an execution count — the same policy a
future streaming subscription would carry.

Why a new variant rather than `View { transient: bool }`: `recovery_source()` (`mod.rs:113-120`)
is a **non-wildcarded** match, so adding a variant is a **compile error** until every such
match gets an explicit arm — correctness by construction. A bool field would not force that
(Rust does not require re-destructuring a struct variant's fields at existing `match`/`!=`
sites), and the checkpoint-collector bug below (`ingest.rs:267`, a whole-variant `!=`
comparison) would silently persist.

**The checkpoint-collector restructure — a latent bug this design introduces.**
`collect_ephemeral_flush_tables` (`query/dag/ingest.rs:253`) has **two asymmetric halves**:
the **trace** half iterates `self.cache.values_mut()` with **no `kind` filter** (and
`CompileOutput`, the `cache` value, carries no `kind`/`tid` to filter on); the **output**
half iterates `self.tables.values()` filtering `entry.kind != RelationKind::View`. Today
`cache` holds only views (`ensure_compiled` is the sole inserter and matches zero rows for a
non-view), so the trace half is *incidentally* correct. A transient's compiled plan **must**
live in `cache[tid]` to run, and checkpoint coordination runs on a path **independent of the
`catalog_rwlock`** every read takes (`do_checkpoint`/`sync_flush_round`/`handle_flush_all_ephemeral`
never lock it), so a `FLAG_FLUSH_EPH` round can interleave with an in-flight transient.
`flush_prepare_ephemeral` (`storage/lsm/table/flush.rs:213`) stages a generation-stamped
manifest **unconditionally**, so any `*mut Table` the collector returns is durably persisted —
leaking the transient's operator traces.

**Fix — restructure the trace half.** Iterate `self.tables` first (the source of `kind`),
keep entries where `entry.kind.checkpointed()`, and pull each survivor's plan via
`self.cache.get_mut(&tid)`. Sound because every `cache` entry has a matching `tables` entry
(`ensure_compiled` requires `self.tables.get(&view_id)` first; `unregister_table` removes both
together). Change the output half's predicate to `entry.kind.checkpointed()` as well.

**Add exactly one accessor**, `RelationKind::checkpointed(self) -> bool` (`View => true`, all
others `=> false`). It has two callers: the two `collect_ephemeral_flush_tables` halves, and
the `flush_view_or_abort` gate (§9). Do **not** add `maintained()`/`durable()`/`drains_ticks()`:
a transient is never in the DepTab (§9 drives `circuit.sources` directly), so the
maintenance/dependency walkers never see a Transient id; recovery is handled by the forced
`recovery_source()` arm; the read-drain is gated on circuit shape (§6), not kind. Those three
accessors have no caller under this plan's scope.

`recovery_source()` gets an explicit `Transient => RecoverySource::Rederive` arm (the
non-checkpointed rederive variant — a transient is never persisted, so this is only reached
defensively; it must not claim a committed generation). Trace tables created by
`create_child_table` (`compiler/emit.rs:187`) stay labeled `RederiveCheckpointed` — that label
is never consulted (the sole enforcement point is the flush-collector filter), so
`create_child_table` needs no change.

## 3. Routing — thin path unchanged, executor by fall-through

The thin path keeps its runtime access-path ladder verbatim. The routing decision is made
**before any seek/scan I/O**, from schema + AST + index metadata only, so a query never pays
for a thin round trip it then discards, and routing is a pure function of query shape (not of
which rows a seek happens to return).

A query is **thin-serviceable** iff *all* hold (else route to the executor):
1. single-table FROM, no JOIN (`select.rs:88`, `:94`);
2. no DISTINCT / GROUP BY / HAVING (checked directly against `select.distinct` /
   `select.group_by` / `select.having`, as JOIN / multi-table already are — **not** via
   `reject_unhonored_select_clauses`, whose generic `Unsupported` can't distinguish these from
   TOP/QUALIFY/etc.);
3. body is a plain `SetExpr::Select`, not a set operation (`select.rs:47`);
4. projection is thin-resolvable: every item is a bare column reference (`exec/batch.rs:73`)
   including ≥1 PK column (`exec/batch.rs:100`) — both pure schema/AST checks, **hoisted before**
   the WHERE ladder's first fetch (today they run after `client.scan`, so a PK-less projection
   full-scans then rejects);
5. the WHERE is served by an index **and** its residual references **only i64-typed columns**
   — decided statically from the residual's column types (the only kind `exec/eval.rs` evaluates).

The ladder's own error sites — `select.rs:172` "WHERE on non-indexed column", the
non-i64-residual rejections in `exec/eval.rs`, the two `resolve_projection` rejections, and the
JOIN / DISTINCT / GROUP BY / HAVING / set-op / multi-table up-front rejections — all become
**route-to-executor** decisions taken before I/O. Genuine failures (`GnitzSqlError::Exec`
transport errors, `GnitzSqlError::Bind` ambiguous/unknown-column errors) are **not** routing
signals and propagate as hard errors with their exact messages. **`make e2e` adds a drift
cross-check test**: every fixture the router sends thin must also classify thin on the compiled
circuit IR (single `ScanDelta` + index-servable-bound + i64-residual + PK-bearing bare-column
projection), so a future planner change that moves a shape can't silently misroute.

`SELECT non_pk_col FROM t` (dropping all PK columns) stays on the **thin path**: teach its
projection resolver to hidden-prepend the source PK (the `place_pk_front` logic already used by
the view compiler, `codec/project_schema.rs:112`). Do **not** route it to the executor — it is
structurally a keyed read, and paying a full circuit compile + drive for a projection-resolution
gap is the exact "cheap read through the expensive machine" regression this plan forbids.

## 4. Request lifecycle

```
CLIENT (gnitz-sql / gnitz-core)
  execute_select(query):
    if thin_serviceable(query): serve via thin path (unchanged)
    else:
      segments = compile_query_to_circuit(query)          // reuse the view planner (§5), LOCAL ids
      if segments.len() != 1: return Err(Unsupported "use CREATE VIEW")   // multi-segment (out of scope)
      circuit, out_schema = segments[0].{circuit, output schema}
      return client.run_query(circuit, out_schema)         // FLAG_RUN_TRANSIENT; streamed (PK,payload) entries

MASTER
  decode transient frame -> CircuitRows + out_schema
  tid = allocate_transient_id()                            // monotonic in-RAM, reserved range (§5)
  loaded = build_loaded_circuit(CircuitRows); topo_sort(&mut loaded)   // NO compile, no scratch tables
  register tables[tid] (out schema, RelationKind::Transient)
  meta[tid] = ViewMeta::from_loaded(&loaded)               // §9 — master-only, memo pre-injection
  TickTrigger::Quiesce                                     // brief: every worker opens cursors at one SAL prefix
  catalog_rwlock.read()                                    // excludes DDL; pushes/seeks/scans stay live
  for source_id in get_source_ids(circuit):                // ScanDelta sources only, in dependency order
     emit RunTransient(tid, source_id) as a tick-shaped SAL group   // §9: sal_writer_excl per write, NO maybe_checkpoint
     await all-worker ACKs via reply futures               // async (run_tick shape), NOT the sync collect
     // relay_loop drives this source's exchange rounds; on all_pad it stamps BACKFILL_DECISION_STOP
  if shape is Single/linear: the streamed per-chunk batches ARE the result   // no output store
  else:                     scan the accumulated RAM output store, stream    // join/agg/distinct/setop/semijoin
  emit_drop_transient(tid)  // .await'ed SAL broadcast at handler tail, every exit path (§10); free master maps

WORKER
  dispatch_inner(RunTransient, tid, source_id, req_id):    // new arm — reuses handle_backfill's chunked loop
     if first sight of tid: build LoadedCircuit from cached frame; compile_loaded under tid (scratch dir) -> cache[tid]
     drive this source over local partition cursors in DDL_SCAN_CHUNK_ROWS chunks, one epoch/chunk;
       stamp the pad bit on outbound FLAG_EXCHANGE; obey the relay decision (consume_backfill_decision);
     Single/linear shape: stream_batch_response each chunk's output (ReplySchema::OneOff(out_schema), req_id)
     accumulate shape: ingest_to_family(tid, ...) each chunk; final source ACK carries a routable request_id
  dispatch_inner(DropTransient, tid, ...):                 // new arm, idempotent
     free tables[tid]/cache[tid]/meta[tid] (map-remove no-op if absent) + scratch dir (ignore ENOENT)
```

`meta[tid]` is **master-only** (only `prepare_relay` reads the meta getters); `cache[tid]` is
**worker-only** (`execute_multi_worker_step`). The master never compiles — it only decodes the
frame into a `LoadedCircuit` to synthesize `ViewMeta`. **No transient-drive mutex**: SAL total
order + serial per-worker execution serialize concurrent transients naturally (§10).

## 5. Component changes — the executor

Reused verbatim: the operator kernels (`ops/`), the VM, the compiler internals after
`load_circuit` (`topo_sort`/`annotate`/`build_plan`), the exchange transport
(`ExchangeCallback::do_exchange`; `ExchangeAccumulator`), the per-node circuit codec
(`encode_op_node`/`decode_op_node`, `gnitz-wire/src/circuit.rs`), `Circuit::into_rows`
(`gnitz-core/src/circuit.rs:59`) + the `encode_ddl_txn` family framing, the streamed-reply path
(`stream_batch_response` + `ReplySchema::OneOff`), `unregister_table` (`query/dag/mod.rs:265`),
and **the worker-side chunked drive loop** `handle_backfill` (`worker/mod.rs:969-1031`, see §9).

- **gnitz-sql** — factor `compile_query_to_circuit(query) -> Result<Vec<PlannedView>>` out of
  `execute_create_view` (`plan/view/dispatch.rs:38-147`): everything from `inline_ctes` through
  building the final `PlannedView`, stopping **before** `client.create_view_chain`. It must mint
  segment ids from a **local** provisional source (sequential from 1), **not** `client.alloc_table_id()`
  (a server round trip that durably advances `SEQ_ID_TABLES` and whose ids the master discards
  anyway). CREATE VIEW keeps its `alloc_table_id` path; the transient path remaps the provisional
  ids server-side (§below). `execute_select`'s fall-through calls it, applies the `segments.len() == 1`
  guard, ships `segments[0]`. **No new SQL planning.**
- **gnitz-core** — `GnitzClient::run_query(circuit_rows, out_schema)`: encode a **`FLAG_RUN_TRANSIENT`**
  frame (the `encode_ddl_txn` family-block layout carrying the 3 circuit families + the output-schema
  column family; **no** VIEW_TAB/COL_TAB/DEP rows, no fsync), reassemble the streamed reply as
  `recv_scan` does.
- **gnitz-wire** — add `FLAG_RUN_TRANSIENT` and `FLAG_DROP_TRANSIENT` flag bits.
- **gnitz-engine**:
  - `SalMessageKind::RunTransient` + `DropTransient` (`runtime/protocol/sal.rs:229`): `classify`,
    `ALL_KINDS`, top-level `dispatch`, worker `dispatch_inner` arms, and the **dispatch-matrix cell**
    (`worker/mod.rs:465-481`): `RunTransient` **defers** inside an exchange wait (mirroring `Tick`,
    **not** inline like `Gather`/`Backfill`), preventing a re-entrant `do_exchange`; `DropTransient`
    is inline (frees maps). Match exhaustiveness forces both cells explicitly.
  - **In-memory `LoadedCircuit` builder + `compile_loaded` split.** `load_circuit` (`compiler/load.rs:40`)
    is the **only** sys-table-reading step in `compile_view` (verified: `topo_sort`/`annotate`/
    `compute_skip_nodes`/`circuit_range_join_n_eq`/`compute_skips_exchange`/`build_plan`/`finalize_side`
    all operate on the in-memory `LoadedCircuit` + the resident `ext_tables` map). Split it out and add
    `compile_loaded(loaded, view_dir, view_schema, ext_tables) -> Result<CompileOutput>` (the body from
    `mod.rs:310`). A builder constructs `LoadedCircuit` from the delivered `CircuitRows` (same
    `decode_op_node`, same `(kind, position)` node-column sort, same edge-validity check). Client node
    ids are `u64`, engine keys are `i32`: the frame is **fresh untrusted per-request wire input**, so the
    builder must **reject** (clean `CompileError`) any id outside the positive `i32` range, not truncate.
  - **`register_transient_plan(tid, CompileOutput, out_schema, Transient)`** — a new inserter seeding
    `cache[tid]` + `tables[tid]` directly, because `ensure_compiled` reads sys tables → zero rows for a
    transient → `CompileError::EmptyCircuit` → no insert.
  - **Server-allocated Transient ids** — a **monotonic, in-RAM, never-recycled** counter over a reserved
    range distinct from the durable table-id space, that must **not** advance `SEQ_ID_TABLES`. Distinct ids
    give concurrent transients distinct `tid` **and** distinct scratch `view_dir` (scratch tables are
    `_{hist,int,reduce}_{tid}_{nid}`, `emit.rs:590` — a collision corrupts a concurrent query). Never a
    bounded ring: a delayed/re-issued `DropTransient` for an old `tid` must never free a newer query's live
    state. Cross-restart reuse is safe because boot GC (§10) clears pre-crash scratch before the first serve.
  - **The one-shot driver** enumerates the circuit's source ids via **`get_source_ids`**
    (`gnitz-core/src/circuit.rs:36` — **ScanDelta only**; `ScanTrace` are read-only lookups, not driven as
    deltas; deduped), and for a source that drains dry on the first iteration feeds **one empty epoch** so a
    global aggregate mints its ground row (`COUNT(*)=0`, `ops/reduce/emit.rs:86`) — mirroring `backfill_view`.
  - **Stream-per-chunk vs accumulate.** A `Single`/linear shape's output is emitted **one delta per drained
    source chunk** (`DDL_SCAN_CHUNK_ROWS`), each disjoint over `unique_pk` base rows — stream **every** chunk's
    batch via `stream_batch_response` (streaming only the terminal epoch would silently drop all but the last
    chunk). A multi-source/non-linear shape (join/reduce/distinct/set-op/semi-join) ingests each chunk's output
    into an unregistered RAM output `Table` (`tables[tid]`, carrying the hidden output key), scanned once at the
    end via `scan_family(tid)` (consolidated cursors; **never flushed to shards**, §9).

## 6. Visibility & consistency

The executor reads a **per-statement snapshot on the workers**: the brief `TickTrigger::Quiesce`
(§4) makes every worker open its source cursors at the identical SAL prefix, and cursors are
MemTable-snapshot + shard-set (immune to inline pushes). The catalog read lock excludes concurrent
DDL. Consistency is **read-committed, per-source** — a multi-source join opens each source's cursor
at drive time, so there is no single consistent cut across a join's two sources. This is inherent to
a non-blocking multi-source one-shot and matches the freshness a view scan gets today.

It does **not** reflect the calling transaction's own uncommitted buffered writes — **byte-for-byte
today's direct SELECT visibility** (`execute_select` reads through raw `client.scan`/`seek` with no
overlay; the read-your-own-writes overlay is DML-only and base-PK-keyed). Read-your-own-writes for
ad-hoc SELECT is a **non-goal** (§8).

**Freshness drain is gated on circuit shape.** `drain_pending_ticks` (`executor.rs:1705`) gates on a
system-wide LSN; a base-table read never calls it today. Draining unconditionally would newly
latency-couple a pure base-table transient to *unrelated* views' tick backlog, so drain **iff** the
compiled circuit contains a `ScanTrace` (view) source.

## 7. Result model

One entry per `(PK, payload)`, weight surfaced as a per-row attribute, never expanded
(`gnitz-py/src/lib.rs:1388`). Shapes without a natural key carry a hidden synthetic slot
(`_group_pk`/`_join_pk`/`_set_pk`), stripped by `visible_columns()` — identical to a view scan.
DISTINCT and aggregation collapse multiplicity **server-side inside the circuit**, so `SELECT DISTINCT`
returns true distinct rows (the projection-PK limitation that blocks client-side DISTINCT does not
apply — the circuit owns the output key).

## 8. Non-goals

- **Deleting the thin keyseek path** — kept permanently; it serves point/index reads optimally.
- **Ordering / pagination (ORDER BY / OFFSET / LIMIT)** — a self-contained client-side sink over any
  ad-hoc result (thin or executor), a separate concern. Until it lands, ORDER BY/OFFSET stay rejected as
  today; the executor streams unordered `(PK, payload)` entries.
- **Multi-segment chains** — 3+-way joins, self-joins, DISTINCT/GROUP BY over a join, correlated scalar
  subqueries / ANY / ALL, non-pass-through CTEs, derived tables in FROM (a `ViewChain` bundle). Rejected
  here with a clean `Unsupported` (`segments.len() > 1`); a separate plan drives transient chains.
- **Read-your-own-writes for buffered txn writes** (§6) — deferred; feeding the buffered write-set as a
  final circuit epoch (PK identity intact against the transient's own traces) is the Z-set-native
  mechanism, out of scope here.
- **Bounded circuit sources** — the drive full-scans its base sources (a non-indexed filter has no index
  to bound; relational shapes read whole sources anyway).
- **Plan caching** — each query compiles fresh.
- **STRING/BLOB result chunking** — one-frame limit inherited from the view-scan path (`worker/reply.rs:182`).
- **Standing/incremental queries** — `CREATE VIEW` remains the maintained preset.

## 9. Distributed correctness — the non-blocking drive

The transient reuses the **steady-state tick discipline**, not the stop-the-world DDL backfill. The
worker-side full-base drive is **already consumer-agnostic**: `handle_backfill` (`worker/mod.rs:969-1031`)
streams a source in `DDL_SCAN_CHUNK_ROWS` chunks through view-scoped epochs, stamps a pad bit on every
outbound `FLAG_EXCHANGE` (reset at `worker/mod.rs:1016`), and obeys the relay decision
(`consume_backfill_decision`, `worker/mod.rs:1064` — a no-op outside a backfill). The async exchange path
already computes `all_pad` per round (`reactor/exchange.rs:90`), and `Backfill` already ACKs with a
routable `request_id` (`worker/mod.rs:723`). So the transient drive is a **bounded delta** on machinery
that already coexists with the committer on every tick — not a new subsystem:

1. **`relay_loop` stamps `BACKFILL_DECISION_STOP` when `relay.all_pad`** (via `emit_relay_with_decision`) —
   ~3 lines, provably inert for tick rounds (steady-state exchanges always report `all_pad = false`).
2. **`RunTransient` is emitted like a tick, not like `fan_out_backfill`** — per-worker req_ids, written
   under `sal_writer_excl` **for the synchronous write window only**, catalog **read** lock held, ACKs
   awaited via **reply futures** (the `run_tick` shape, `executor.rs:713-758`). It **does not** call
   `maybe_checkpoint` (`dispatch.rs:449` — whose own invariant forbids the async path from calling it); low
   SAL space is handled by the existing Reclaim barrier, which works mid-drive because `Flush` is inline
   inside exchange waits. It **does not** use the synchronous `collect_acks_and_relay` (which reads the W2M
   rings raw and would mis-consume a concurrent push's ACK).
3. **The dispatch matrix defers `RunTransient` inside an exchange wait** (§5) — no re-entrant `do_exchange`.
4. **A brief `TickTrigger::Quiesce`** around the broadcast (§4/§6) gives every worker one identical SAL
   prefix; pushes keep flowing throughout.
5. **A recovery-walker skip arm** for `RunTransient`/`DropTransient` — transients are never replayed.

Multi-source joins stay correct: the master drives each `ScanDelta` source **to completion in dependency
order** (awaiting its all-worker ACKs before the next), so source A's `IntegrateTrace` is fully seeded before
source B's delta probes it — the live CREATE-VIEW "first fills the trace, second produces the full join"
sequence, realized with an **async** barrier (awaited reply futures) instead of the broken synchronous futex
loop. Single-source-per-epoch holds (self-joins are multi-segment, rejected here), keeping the symmetric
2-term DBSP form valid.

**Relay meta resolution.** `prepare_relay` (`master/dispatch.rs:554`) resolves re-scatter columns via
`get_shard_cols` / `get_join_shard_cols` / `view_range_join_n_eq`, all funnelling through `view_meta`
(`meta.rs:268`), which without intervention falls back to `load_meta_circuit` → sys tables (empty for a
transient) → `LoadedCircuit::default()` → empty `shard_cols` (a serial-parallelism cliff, only accidentally
correct for GROUP BY/equi-join) and `range_join_n_eq = None` instead of `Some(0)` (→ skips the
`op_relay_broadcast` a pure-range join needs → **missing rows**). Fix — **pre-inject `ViewMeta[tid]`; touch
no getter.** `view_meta` is **memo-first** (`meta.rs:269` checks `self.meta` before the sys read), so
inserting the transient's `ViewMeta` into `self.meta[tid]` makes all four getters return it with **zero
changes** (rewriting them would break the durable path, which populates `meta[tid]` lazily via
`load_meta_circuit`). Add `ViewMeta::from_loaded(&LoadedCircuit)` factored from `view_meta`'s memo-miss body
(`meta.rs:272`): find the `ExchangeShard` node (`shard_cols`), `compute_join_shard_map(&loaded)`
(`optimize.rs:13`), `circuit_range_join_n_eq(&loaded)` (`load.rs:343`), any `Join` node. The `LoadedCircuit`
**must be `topo_sort`ed first** (`compute_join_shard_map` walks `loaded.outgoing`). The master builds it from
the decoded frame — no compile, no `CompileOutput` (a *worker* artifact the master never holds — do **not**
thread `shard_cols` into it). `get_schema_and_names` needs **no** change: `prepare_relay` uses only
`name_bytes`, which comes back empty for a transient and is handled gracefully (nameless relay group; internal
relays reconstruct by schema; the client reply carries names from `ReplySchema::OneOff`).

**All-replicated shuffle shapes must not N×-overcount.** `execute_multi_worker_step` calls
`view_all_sources_replicated(view_id)` (`exec.rs:323`), which reads the **DepTab** (`meta.rs:226`) — empty for
a transient → returns `false` even when all sources are replicated. For an all-replicated GROUP BY / DISTINCT /
set-op / semi-join (an `ExchangeShard` shape, `skips_exchange` false), Arm 1 is the **only** guard that skips
the output shuffle; missing it, every worker's full local result is exchanged → **N× weight overcount**
(`SELECT dept, COUNT(*) FROM replicated_dim GROUP BY dept` at W>1). Fix: make `view_all_sources_replicated`
lifecycle-opaque — when `cache[tid]` exists, derive sources from the circuit's `ScanDelta` set + live
`tables[s].schema.replicated()` (stable for the query under the held read lock). Safe because within this
plan's SQL scope every real source is a `ScanDelta`; a partitioned-`ScanTrace` circuit is multi-segment and
rejected. (Join-over-replicated is unaffected — handled at compile via `co_partitioned`.)

**The `flush_view_or_abort` gate.** `handle_backfill` calls `flush_view_or_abort(view_id)` (`worker/mod.rs:1028`)
when the view produced rows, flushing `tables[tid]` to scratch **shards** — contradicting §5's RAM-only claim.
Gate it off: `checkpointed() == false ⇒ skip flush`; `scan_family(tid)` reads the MemTable directly.

## 10. Teardown & crash-safety

A transient allocates `tables[tid]`/`cache[tid]`/`meta[tid]` + per-rank scratch dirs that must be freed.
`unregister_table` (`query/dag/mod.rs:265`) is a **master-local** `HashMap::remove` (idempotent); its only
driver today is the durable, fsynced catalog DROP hook. A transient needs a **non-durable** teardown.

**Eager `.await`ed emit at the handler tail — no RAII guard.** A `Drop` cannot tear down: every SAL emit is
serialized by the **async** `sal_writer_excl` (`committer.rs:633`), and `Drop` cannot `.await` it. No guard is
needed: the master handler is inline (`connection_loop` awaits `handle_message`, `executor.rs:459`) and the
async drive `.await`s to completion before streaming; the handler **returns normally** on success (`Ok(true)`),
client disconnect (`Ok(false)`), and send-timeout eviction (`guard_eviction` *awaits* the aborted send,
`tls/mod.rs:311`). So emit `FLAG_DROP_TRANSIENT` as a normal `.await`ed SAL broadcast under `sal_writer_excl` at
the **tail** of `handle_run_transient` — reachable on every `return` — then free the master maps.

- Emit `DropTransient` **after** the drive's last source ACK returns, so per-worker SAL append order guarantees
  every worker sees `RunTransient` before `DropTransient` and none is mid-exchange for `tid` when it lands. Each
  worker's arm is **idempotent** (map-remove no-op on absent tid; `ENOENT`-ignoring scratch-dir removal).
- **No transient-drive mutex.** Concurrent transients serialize by construction: SAL groups are totally ordered,
  and `RunTransient` defers inside another tid's exchange wait (§5), so a worker completes drive #1's chunk loop
  before entering drive #2's. The racing parties `fan_out_backfill` fought (the committer, `relay_loop`,
  `tick_loop`) are handled by inheriting the tick discipline (§9), not by a lock.
- **Worker fault is fail-stop.** The watchdog reaps the cluster (`request_shutdown`, `executor.rs:575`) and the
  process exits — no `DropTransient` is delivered. The transient's scratch dirs (never in any manifest/sys table)
  orphan on disk. Reap them with a **dedicated boot-time GC pass** over a **dedicated scratch root** (put transient
  scratch under `<base>/_transient/<tid>/…`, not a schema dir, and add a boot pass in `bootstrap.rs` before
  serving). The existing `gc_orphan_directories` **cannot** reach it — it scans only live schema dirs and refuses
  unrecognized `base_dir` entries (`write_path.rs:637`). Release builds are `panic = "abort"`, so a panic is
  identical to a worker fault → boot GC.

## 11. Testing

- **Rust unit**: `compile_loaded` over an in-memory `LoadedCircuit` == `compile_view` over the sys-table form for
  the same circuit; the builder rejects an out-of-`i32`-range node id; `register_transient_plan` → run →
  `DropTransient` leaves `tables`/`cache`/`meta` empty and removes scratch dirs; `ViewMeta::from_loaded` for a
  pure-range-join transient yields `range_n_eq = Some(0)` and non-empty `shard_cols`; the `segments.len() > 1`
  guard rejects a 3-way join with `Unsupported`.
- **Integration** (`crates/gnitz-sql/tests/`, `feature=integration`): each of a 2-way join, single-table
  GROUP BY + HAVING, DISTINCT, set-op, `EXISTS`/`IN` semi/anti-join, and a non-indexed WHERE on a string and a
  float column returns the **same rows and weights** as the equivalent CREATE VIEW then scan; `WHERE pk=5` still
  serves via the thin path (assert no transient id allocated); the drift cross-check (§3); the thin-path errors
  that remain (ambiguous column, `Bind`/`Exec`) keep their exact messages. Fixtures force the fall-through
  statically (rule 5) so routing is not data-flaky.
- **E2E** (`crates/gnitz-py/tests/test_adhoc_query.py`, `GNITZ_WORKERS=4`) — the correctness gate:
  - every shuffle shape (2-way join / GROUP BY / DISTINCT / set-op / semi-join) is **weight-correct at W=4**
    (the `SELECT k, COUNT(*) FROM t GROUP BY k` split-count is the partitioned regression);
  - a GROUP BY / DISTINCT over an **all-replicated** table is weight-correct at W=4 (guards the
    `view_all_sources_replicated` N×-overcount);
  - a pure-range-join transient returns **all** rows at W=4 (guards the `range_n_eq` relay);
  - **the non-blocking property**: a long-running ad-hoc join runs concurrently with a stream of pushes to an
    unrelated table and a **forced checkpoint mid-drive** — the query returns correct weights, the pushes commit
    without stalling, and no SAL orphan/deadlock occurs (guards the §9 drive discipline — the specific failure the
    old `fan_out_backfill` driver would have caused);
  - a transient leaves **no** manifest/checkpoint artifact even when a checkpoint is forced mid-query (guards §2);
  - client disconnect mid-stream and two concurrent transients leave no leaked `tables[tid]`/scratch dirs (guards §10).
- **`make verify`** then **`make e2e WORKERS=4`** green after each stage.

## 12. Sequencing

Ordered so no stage routes a shape to a path that cannot serve it correctly at W>1, and the thin path is never
removed.

- [ ] **Stage 1 — executor core, non-shuffle shapes.** The `Transient` lifecycle (4th `RelationKind` +
  `checkpointed()` + forced `recovery_source()` arm, with the restructured `collect_ephemeral_flush_tables`);
  `FLAG_RUN_TRANSIENT` + `FLAG_DROP_TRANSIENT` + their `SalMessageKind`s, worker arms, and dispatch-matrix cells;
  `Circuit::into_rows` reuse + `run_query` + transient frame codec; the in-memory `LoadedCircuit` builder (with the
  `i32` reject) + `compile_loaded` split + `register_transient_plan` + the monotonic Transient id allocator; the
  one-shot driver on the **tick discipline** (`RunTransient` emitted like a tick, no `maybe_checkpoint`, async ACK
  await, `get_source_ids`, empty-epoch seed, `TickTrigger::Quiesce`, recovery skip-arm); **stream-per-chunk** for
  `Single`/linear shapes; the eager `.await`ed `DropTransient` teardown + dedicated boot-time scratch GC; the
  `compile_query_to_circuit` split (local ids) + `segments.len() == 1` guard. Route (via the §3 pre-classifier)
  single-source filter/project + non-indexed / non-i64-residual / typed WHERE; keep `SELECT non_pk` on the fixed
  thin path. **Non-blocking, W>1-correct** (single source needs no exchange).
- [ ] **Stage 2 — distributed shuffle.** `relay_loop`'s `BACKFILL_DECISION_STOP` on `all_pad`;
  `ViewMeta::from_loaded` + master-side `meta[tid]` pre-injection; make `view_all_sources_replicated`
  lifecycle-opaque; gate `flush_view_or_abort` off for Transient; reuse `execute_multi_worker_step` + the async
  relay drive for the accumulate-then-scan shapes. Now route single-table GROUP BY / DISTINCT / aggregate / set-op /
  2-way join / `EXISTS`-`IN` to the executor. **Non-blocking, W>1-correct** for every single-segment shape, verified
  by the concurrent-push + forced-checkpoint E2E gate.
- [ ] **`make verify` and `make e2e WORKERS=4`** green after each stage, with the W=4 weight-correctness
  (partitioned **and** replicated), pure-range-join relay, non-blocking-under-checkpoint, checkpoint-exclusion, and
  leak assertions of §11 as the gate.
