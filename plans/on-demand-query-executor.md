# On-demand query executor: ad-hoc SELECT as a run-once DBSP circuit

## 1. Problem

gnitz has **two query engines that share nothing**:

- **Ad-hoc SELECT** (`crates/gnitz-sql/src/dml/select.rs`) — a thin, client-resident
  keyseek engine: an access-path ladder (PK seek → `pk IN` multi-seek → ordered
  range index → equality index → hard error, `select.rs:103-178`), gather the whole
  reply into one client `ZSetBatch`, then project/limit client-side. It has **no
  relational operator**. Joins, GROUP BY, DISTINCT, aggregation, set-ops, and every
  non-indexed WHERE are rejected with "belongs in a CREATE VIEW". Its residual
  filter is **i64-only** (`exec/eval.rs`) — it cannot even filter a `TEXT`/`F64`
  column that *is* indexed. There is no ORDER BY, OFFSET, or deterministic LIMIT.
- **CREATE VIEW** (`crates/gnitz-sql/src/plan/view/`) — the full DBSP compiler:
  every relational shape, compiled to a circuit, registered, materialized,
  maintained incrementally, checkpointed, recovered.

Every relational capability therefore has to be built **twice** (once as a client
keyseek wart, once as an engine circuit) or forced into a **persistent, maintained
view** even when the user wants a one-shot answer. The thin path grows a new wart
every time someone wants something ad-hoc.

This plan collapses the two into **one on-demand executor**. A SELECT is compiled
by the **existing view planner** into the same circuit a CREATE VIEW builds, shipped
to the engine as a **transient** (non-durable, unregistered) circuit, run **once**
over the committed base snapshot, its result **streamed** back, and all state
**discarded**. This is sound in the engine's own theory (CLAUDE.md §3): every
operator is "first a scalar function on Z-Sets, then lifted" to the incremental
form; the engine today only instantiates the lifted form. The scalar form — run the
operator DAG once over the base Z-sets — *is* an ad-hoc query, and it is already
latent in every `ops/` kernel and realized daily by `backfill_view`.

The payoff: ad-hoc SELECT gains the **entire** relational surface (joins, GROUP BY,
aggregation, DISTINCT, set-ops, subqueries), a **server-side, fully-typed** WHERE
(dissolving the i64-only and non-indexed-error limits at once), and — at the serving
edge — ORDER BY / OFFSET / deterministic LIMIT. The thin keyseek engine is **deleted**.

## 2. What is reused vs. genuinely new

The compute core is **already decoupled** from the view lifecycle, so the bulk of
this is reuse:

**Reused verbatim** — the operator kernels (`ops/`), the VM, the free compiler
`compile_view` (`query/compiler/mod.rs:299`), the id-parametric backfill drivers
`backfill_view` (`catalog/ddl.rs:386`), `execute_multi_worker_step<E>` /
`backfill_view_step_multi_worker<E>` (`query/dag/exec.rs:305,375`), the exchange
transport (`ExchangeCallback::do_exchange`, `query/dag/mod.rs:204`; the master
`ExchangeAccumulator`, `reactor/exchange.rs:24`), `worker_for_partition`
(`ops/exchange/router.rs:30`) and the seek/scan fan-outs, the per-node circuit codec
`encode_op_node`/`decode_op_node` (`gnitz-wire/src/circuit.rs:445,524`),
`Circuit::into_rows` (`gnitz-core/src/circuit.rs:59`) and the `encode_ddl_txn` family
framing (`gnitz-core/src/protocol/message.rs:313`), the streamed-reply path
`stream_batch_response` + `ReplySchema::OneOff` (`worker/reply.rs`), the `ScanLease`
RAII, and `DagEngine::unregister_table` (`query/dag/mod.rs:265`, which already frees
the RAM traces + compiled plan + scratch dirs through existing `Drop` impls).

**Genuinely new** (the whole delta of this plan):
1. A **transient circuit transport**: a non-durable `FLAG_RUN_TRANSIENT` frame +
   `SalMessageKind::RunTransient`, carrying the `CircuitRows` triple + output schema,
   routed to an **in-memory** `LoadedCircuit` instead of the sys-table ingest.
2. An **in-memory `LoadedCircuit` builder** + a `compile_loaded` split of
   `compile_view`, so a circuit compiles without a sys-table read.
3. **Transient-id registration/teardown**: an in-memory `tables`/`cache`/`meta` entry
   for the request's duration, and a redirect of the one view-bound relay coupling
   (`load_meta_circuit` → in-memory plan).
4. **Bounded circuit sources**: the access-path ladder emits key bounds that a
   circuit `scan` source opens as a bounded cursor (a new `drain_range_to_batch`),
   so point/index reads do not regress to full scans.
5. The **client-side result sink**: ORDER BY / OFFSET / deterministic LIMIT over the
   streamed result batch.
6. **Deletion** of the thin keyseek path once (4) lands.

## 3. Decided semantics

### 3.1 Visibility

The executor reads the **committed base snapshot on the workers, with view-scan
freshness**: it drains pending DAG ticks before reading (`drain_then_lock`,
`executor.rs:1730`), so the result reflects **every base commit ACKed before the
read began**; the catalog read lock excludes concurrent DDL; there is no MVCC. It
does **not** reflect the calling transaction's own uncommitted buffered writes.

This is **byte-for-byte today's direct-SELECT visibility**: `execute_select` reads
through raw `client.scan`/`seek` with **no overlay** (`select.rs` imports none), and
buffered txn writes are never sent until COMMIT (`client.rs:1399`). The
read-your-own-writes overlay (`dml/overlay.rs`) is DML-only and **base-PK-keyed** —
it is fundamentally incompatible with circuit execution, which dissolves base-PK
identity through joins/aggregates, so it cannot be re-applied to a circuit result.
The existing SELECT-vs-DML asymmetry (UPDATE/DELETE read-your-writes; SELECT does
not) is preserved exactly. Read-your-own-writes for ad-hoc SELECT is a **non-goal**.

### 3.2 Result model (entry-based, weight passthrough)

The streamed result is a consolidated Z-set: one entry per `(PK, payload)`, the
weight surfaced as a per-row attribute (`Row.weight`), never expanded — the
established contract (`gnitz-py/src/lib.rs:1388`; `apply_limit` counts entries). The
circuit's output PK for shapes without a natural key is a hidden synthetic slot
(`_group_pk`/`_join_pk`/`_set_pk`), stripped from presentation by `visible_columns()`
— identical to a view scan. `DISTINCT` and aggregation now collapse multiplicity
**server-side inside the circuit** (the `distinct`/`reduce` operators), so a
`SELECT DISTINCT` returns true distinct rows — the projection PK-requirement that
blocked client-side DISTINCT does not apply (the circuit carries its own output key).

### 3.3 The sink: ORDER BY / OFFSET / LIMIT

A Z-set has no order; by DBSP theory ordering is not a homomorphism and is inherently
a **serving-edge** operation. The sink runs **client-side over the streamed result
batch** (see §7), supporting `ORDER BY <col|position> [ASC|DESC] [NULLS FIRST|LAST]`
(multi-key), `OFFSET`, and `LIMIT` (deterministic top-N when ordered). Cost is
**O(result)**: for a filtered/aggregated query the result is small; an *unfiltered*
`SELECT * FROM t ORDER BY x` streams the whole relation to the client and sorts it in
RAM. A server-side ordered/bounded sink (a master-side top-N heap) is a **non-goal**
of this plan — the client-side sink's typed comparator is exactly the comparator such
a sink would need, so it is forward-compatible.

## 4. Request lifecycle

```
CLIENT (gnitz-sql / gnitz-core)
  execute_select(query):
    circuit = compile_query_to_circuit(query)      // reuse the view planner, no registration
    bounds  = access_path_bounds(query.where)      // PK/index selection → source bounds (Stage B)
    rows    = client.run_query(circuit, bounds)     // FLAG_RUN_TRANSIENT; streams result batch
    result  = sink(rows, order_by, offset, limit)   // client-side ORDER BY/OFFSET/LIMIT
    return Rows{ schema, batch: result }

MASTER (runtime/orchestration)
  decode transient frame -> CircuitRows + out_schema
  tid = fresh transient id
  register in-memory: tables[tid] = { scratch view_dir, out_schema }, plan = compile_loaded(...)
  RAII drop_guard = { on any exit: unregister_table(tid) + worker transient-drop }
  distribute (reuse existing policy, §6):
     point-seek  -> unicast owner
     scan/replicated -> broadcast
     sharded (join/groupby/setop) -> broadcast + fan_out_backfill exchange loop, keyed on tid
  for source_id in circuit.sources:
     drive backfill_view_step_multi_worker(tid, source_id, bounded_delta, exchange)
  scan the accumulated transient output store, stream to client (ScanLease-scoped)

WORKER
  dispatch_inner(RunTransient, tid, block, req_id):   // new arm, Gather/Scan template
     build LoadedCircuit from block; compile_loaded under tid (scratch dir)
     drive its shard's epoch(s) over the bounded local source cursor
     accumulate output delta into the unregistered RAM output store
     stream_batch_response(result, ReplySchema::OneOff(out_schema), req_id, client_id)
```

## 5. Component changes

### 5.1 gnitz-sql — compile the query, ship it, apply the sink

- **Factor `compile_query_to_circuit`** out of the CREATE VIEW path. Today
  `plan::execute_create_view` drives the view planner (`plan/view/dispatch.rs`) on the
  query and then registers a view. Split the "query → `Circuit` + output `Schema`"
  half into a standalone function that both CREATE VIEW and `execute_select` call.
  The planner already handles the full relational surface — **no new SQL planning
  code**; `execute_select` stops rejecting joins/GROUP BY/DISTINCT/non-indexed WHERE.
- **`execute_select` becomes**: honor `ORDER BY`/`OFFSET`/`LIMIT` on the envelope
  (§5.5), compile the query to a circuit, derive source bounds from the WHERE
  access-path ladder (Stage B; the existing `collect_index_range_candidates` /
  `collect_index_seek_candidates` / `try_extract_pk_*` in `dml/plan.rs`), call
  `client.run_query`, apply the client-side sink, return `SqlResult::Rows`.
- **Delete the thin keyseek engine** once bounded sources land (Stage B): the WHERE
  ladder in `dml/select.rs` collapses to "emit source bounds"; `exec/eval.rs`
  (i64-only interpreter), `exec/residual.rs`, and the client-side residual filter are
  removed — WHERE is now a **server-side, fully-typed** `op_filter`
  (`ops/linear.rs:21`, evaluates float/string/int/null via a compiled `ScalarFunc`).
- **The sink** lives in a new `exec/order.rs` (§7).

### 5.2 gnitz-core — `run_query` client API + transient frame

- Reuse `Circuit::into_rows() -> CircuitRows` (`circuit.rs:59`) to get the
  `(nodes, edges, node_columns)` row triple the client already builds for CREATE VIEW.
- New `GnitzClient::run_query(circuit_rows, out_schema, bounds) -> ScanResult`:
  encode a **`FLAG_RUN_TRANSIENT`** frame — structurally the `encode_ddl_txn`
  family-block layout (`protocol/message.rs:313`) carrying **only** the 3 circuit
  families + the output-schema column family (no VIEW_TAB/COL_TAB/DEP rows), tagged
  with a client-chosen transient id + `client_id`/`request_id` — then reassemble the
  streamed reply exactly as `recv_scan` does (`connection.rs:400`).

### 5.3 gnitz-wire — the transient flag + source bounds

- Add the **`FLAG_RUN_TRANSIENT`** flag bit.
- **Source bounds on the circuit node** (Stage B): today `OpNode::ScanDelta(TableId)`
  / `ScanTrace(TableId)` carry only a table id (`circuit.rs:322`). Extend them with an
  optional bound descriptor (a PK point / PK range / index selection — reuse the
  existing `RangeDescriptor` / `pack_pk_cols` the wire already defines), threaded
  through `encode_op_node`/`decode_op_node`.

### 5.4 gnitz-engine — the transient executor

- **`SalMessageKind::RunTransient`** (`runtime/protocol/sal.rs:229`): a `classify`
  arm for `FLAG_RUN_TRANSIENT`, an `ALL_KINDS` entry, a top-level `dispatch` arm →
  `run_via_dispatch_inner`, and a worker `dispatch_inner` arm (`worker/mod.rs:675`)
  modeled on the `Gather` handler (`mod.rs:737`) — a synthetic-schema, client-routed
  streamed reply.
- **In-memory `LoadedCircuit` builder + `compile_loaded` split.** `load_circuit`
  (`compiler/load.rs:40`) is the *only* sys-table-reading step in `compile_view`;
  everything after (`topo_sort`, `annotate`, `build_plan`) works on the in-memory
  `LoadedCircuit` (`compiler/mod.rs:73`). Split `compile_view` into `load_circuit(...)`
  + `compile_loaded(loaded, view_dir, view_schema, ext_tables) -> Result<CompileOutput>`
  (the body from `mod.rs:310`). Add a builder that constructs `LoadedCircuit` from the
  delivered `CircuitRows` directly — same `decode_op_node` per node, same
  `(kind, position)` node-column sort, same edge-validity check `load_circuit`
  performs — mapping the client's `u64` node ids to the engine's `i32` keys. The
  transient path calls `compile_loaded` with a scratch `view_dir` and the transient id
  for scratch-table naming; **no sys-table read, no fsync**.
- **Transient-id registration & teardown.** Register `tid` in the DAG in-memory for
  the request only: a `tables[tid]` entry with a scratch `view_dir` + the output
  schema, and the compiled plan in `cache[tid]`. Its operator traces are `Rederive`
  `Table`s (RAM-first: no manifest/fsync/shard files until a 4 MiB/partition ceiling,
  `table/mod.rs:281,41`). Because the ephemeral checkpoint sweep enumerates only
  `self.cache` + `self.tables(kind==View)` (`query/dag/ingest.rs:253-267`), and a
  transient id is `kind != View`, it is **provably invisible to checkpoints**.
  Teardown = `drop_transient(tid) ≡ unregister_table(tid)` (`dag/mod.rs:265`: drops the
  RAM store + `CompileOutput` + meta) wrapped in a **RAII guard scoped to the in-flight
  request future**, alongside the existing `ScanLease`. Every exit path — success,
  client disconnect (`Ok(false)`), worker fault (`Err`), `GNITZ_CLIENT_SEND_TIMEOUT_MS`
  eviction — drops it **exactly once**; workers drop their per-rank scratch on the same
  signal.
- **Redirect the one view-bound coupling.** The master relay `prepare_relay`
  (`master/dispatch.rs:554`) resolves re-scatter columns via `get_shard_cols(view_id)`
  / `get_join_shard_cols` → `view_meta(view_id)` → `load_meta_circuit(view_id)`, a
  sys-table read. For a transient id, `view_meta`/`get_shard_cols` must resolve from
  the **in-memory transient plan** (already in `cache[tid]`) instead. This is the only
  place the exchange transport is not already id-opaque.

### 5.5 Honoring the envelope clauses

`ORDER BY`/`OFFSET`/`LIMIT` are honored only for direct SELECT. Add `order_by: bool`
to `HonoredQueryClauses` (`plan/validate.rs:276`) and gate the guard's ORDER BY reject
(`validate.rs:323`) on it; set `order_by: true` at the `dml/select.rs` call site and
`false` at the other six construction sites (`validate.rs:426`, `view/scalar.rs:400`,
`view/dispatch.rs:41,418,629`, `view/exists.rs:180`) — CREATE VIEW/CTE/subquery/INSERT
keep rejecting ORDER BY. Remove the OFFSET rejection in `select.rs:72-77`; add
`extract_offset` mirroring `extract_limit` (`dml/plan.rs:66`); `LIMIT ALL` already
parses as `limit: None`.

## 6. Bounded sources & server-side filter (Stage B — the fusion)

The read/operator layer is already built for this. `op_scan_trace` is
`cursor.drain_to_batch(0)` — it drains a cursor it was **handed** (`ops/scan.rs:14`),
with zero "whole table" knowledge; `execute_epoch` accepts any input batch. So a
bounded source is: hand the source a **bounded cursor** instead of a full one.

- **Emit bounds at plan time.** The access-path ladder already exists in
  `gnitz-sql` (`collect_index_range_candidates`/`collect_index_seek_candidates`/
  `try_extract_pk_seek_residual`, `dml/plan.rs`). It currently emits client seek calls;
  it instead emits a **source bound** onto the circuit's `ScanDelta`/`ScanTrace` node
  (§5.3). PK fully bound → point/range bound (unicast, §6 distribution); indexed
  prefix → index selection; otherwise → no bound (full scan) with the predicate
  compiled into the circuit's `op_filter`.
- **Open a bounded cursor.** Add `ReadCursor::drain_range_to_batch(start, end)` — a
  mechanical extraction of the already-open-coded walk in `seek_by_index_range`
  (`catalog/store_io.rs:365-389`: `seek_bytes(start)` … `while valid && cur_pk < end`).
  The source-open site (`open_store_cursor`) uses it when the node carries a bound.
- **Non-indexed WHERE = server-side typed filter.** A predicate with no serving index
  compiles into `op_filter` (`ops/linear.rs:21`) over a full source scan. The compiled
  `ScalarFunc` predicate evaluates **all** types (`EXPR_FCMP_*`, `EXPR_STR_COL_*`, int
  arithmetic/compare, `IS NULL`, `AND/OR/NOT`; `expr/program.rs:13`). This dissolves
  **both** thin-path limits — the i64-only residual and the non-indexed hard error —
  in one move: any WHERE runs server-side over any column type.

Distribution (reuse existing policy, keyed on the transient id): PK point-bound →
`fan_out_seek`-style unicast to `worker_for_partition` (`ops/exchange/router.rs:30`);
scan/replicated → `fan_out_scan` broadcast; sharded (join / GROUP BY / set-op / range
join) → `fan_out_backfill` broadcast + the exchange relay loop
(`master/dispatch.rs:777`), driven by `execute_multi_worker_step` whose 6 arms pick
replicated/range-join/exchanged/join-scatter/single **from the compiled plan**
(`query/dag/exec.rs:305`) — no new distribution logic.

## 7. The sink — ORDER BY / OFFSET / LIMIT (`exec/order.rs`)

Client-side over the streamed result batch. A typed multi-column comparator over the
`ZSetBatch` (`ColData` `Fixed`/`Strings`/`Bytes`/`U128s`, `PkColumn`, null bitmap),
matching the engine's `compare_rows` order so client order equals the order the engine
would impose: signed ints signed-order, unsigned magnitude, `F32`/`F64` `total_cmp`,
`STRING`/`BLOB` byte-wise (== the engine's german-string content order), `U128`/`UUID`
unsigned, `I128` signed. Null placement is absolute (default NULLS LAST for ASC / FIRST
for DESC, `nulls_first = asc == false`; explicit honored), not flipped by DESC. Keys
resolve as a column reference (against the result schema) or a **1-based visible-output
position**. Pipeline: stable-sort a row-index permutation, slice `[offset, offset+limit)`,
materialize via `copy_batch_row`; a plain `SELECT` with no order/offset/effective-limit
is a zero-copy passthrough.

Two correctness requirements (from adversarial review of the sink):
1. **Positional ORDER BY is visible-column-aware.** `ORDER BY n` maps to the *n*-th
   **visible** output column (skip `META_FLAG_HIDDEN`), because a circuit result over a
   hidden-PK shape (`_group_pk`, etc.) carries a hidden column at index 0 and `SELECT *`
   is the queryable form — mirror `apply_hidden_column_aliases`
   (`view/dispatch.rs:360`) / `visible_columns()` (`gnitz-py/src/lib.rs:944`). Naive
   physical `pos-1` sorts by the hidden key silently.
2. **No worker-count determinism claim.** A scan is per-worker sorted runs concatenated
   in rank order, hash-partitioned — not a global merge — so a **stable** sort's
   tie-break differs W=1 vs W=4. Ties are unspecified by SQL; do not claim otherwise,
   and make tests assert a total order over unique keys or per-tie-group set-equality.

## 8. Deleting the thin path

Once §6 lands, `Statement::Query` routes entirely through the executor and these are
**removed** (pre-alpha, no legacy): the `dml/select.rs` access-path ladder and its
non-indexed hard error; `exec/eval.rs` (the i64-only `InterpBackend`); `exec/residual.rs`
and `residual_filtered`; the client-side projection-PK requirement's role in SELECT
(the circuit owns the output key). What remains in `dml/select.rs` is thin: compile,
derive bounds, `run_query`, sink. UPDATE/DELETE keep their own committed+overlay ladder
(`dml/mutate.rs`) — unchanged; the overlay is DML-only.

## 9. Non-goals

- **Read-your-own-writes for buffered txn writes in ad-hoc SELECT** (§3.1) — incompatible
  with circuit execution; would need server-side delta injection, a separate feature.
- **A server-side ordered/bounded sink** (master top-N heap) — the sink is client-side,
  O(result); the comparator is forward-compatible with a future server sink.
- **STRING/BLOB result chunking** — a string/blob-bearing result must fit one
  `MAX_W2M_MSG` frame (`worker/reply.rs:268` — the existing view-scan limit; inherited,
  not new). Fixed-width results chunk freely.
- **Standing/incremental queries** — `CREATE VIEW` remains the way to register a
  maintained view; the executor is one-shot.
- **Caching compiled transient circuits** — each query compiles fresh (a plan cache
  keyed by query shape is a separate optimization, not required for correctness).

## 10. Testing

- **Rust unit** (`exec/order.rs`): the comparator across every type incl. negatives,
  U64 high-bit, `-0.0/NaN` (`total_cmp`), strings, `U128`/`UUID`/`I128`; ASC/DESC; NULLS
  default + explicit not flipped by DESC; multi-key; visible-position resolution over a
  schema with a hidden col; offset/limit slicing incl. past-end and passthrough.
- **Engine unit** (`gnitz-engine`): `compile_loaded` over an in-memory `LoadedCircuit`
  equals `compile_view` over the sys-table form for the same circuit; `drain_range_to_batch`
  equals the `seek_by_index_range` walk; transient-id register→run→`unregister_table`
  leaves `tables`/`cache`/`meta` empty and removes scratch dirs.
- **Integration** (`crates/gnitz-sql/tests/`, `feature=integration`): ad-hoc `SELECT`
  with a join, a GROUP BY + HAVING, a DISTINCT, a set-op, a scalar subquery, a
  non-indexed `WHERE` on a string and a float column — each returns the same rows as the
  equivalent CREATE VIEW then scan; ORDER BY/OFFSET/LIMIT shape the result; a point-seek
  `WHERE pk=5` unicasts (assert via worker logs) and does not full-scan.
- **E2E** (`crates/gnitz-py/tests/test_adhoc_query.py`, `GNITZ_WORKERS=4`): the above
  under multi-worker, proving exchange-backed sharded one-shots; a transient circuit
  leaves no manifest/checkpoint artifact (assert data dir unchanged after the query);
  client disconnect mid-stream frees transient state (assert no leaked scratch dirs).
- **`make verify` then `make e2e WORKERS=4`.**

## 11. Sequencing

- [ ] **Stage A — transient executor core (single-source / replicated; W=1 covers all
  shapes).** `FLAG_RUN_TRANSIENT` + `SalMessageKind::RunTransient` + worker
  `dispatch_inner` arm; `Circuit::into_rows` reuse + `run_query` client API + transient
  frame codec; in-memory `LoadedCircuit` builder + `compile_loaded` split;
  transient-id register/teardown RAII; accumulate into an unregistered RAM store + stream
  via `ReplySchema::OneOff`. Full-scan sources only. Route to the executor the queries the
  thin path cannot serve (non-indexed WHERE, aggregates, single-table GROUP BY/DISTINCT);
  keep the thin path for point/index seeks meanwhile. Server-side typed `op_filter` for
  non-indexed WHERE.
- [ ] **Sink — ORDER BY / OFFSET / deterministic LIMIT** (`exec/order.rs`) with the two
  §7 correctness requirements; `HonoredQueryClauses.order_by` + `extract_offset` (§5.5).
  Lands on top of Stage A over the streamed result.
- [ ] **Stage B — bounded sources + delete the thin path.** Source-bound descriptor on
  `ScanDelta`/`ScanTrace` (wire + compiler); `ReadCursor::drain_range_to_batch`;
  access-path ladder emits bounds; point-bound unicast distribution. Then route **all**
  `Statement::Query` through the executor and **delete** the thin keyseek engine
  (`exec/eval.rs`, `exec/residual.rs`, the `select.rs` ladder + non-indexed error).
- [ ] **Stage C — distributed sharded one-shots.** Reuse `fan_out_backfill` +
  `execute_multi_worker_step` exchange loop for joins / GROUP BY / set-ops / range joins
  at W>1, keyed on the transient id; redirect `view_meta`/`get_shard_cols` to the
  in-memory transient plan (the sole view-bound relay coupling).
- [ ] **`make verify` and `make e2e WORKERS=4`** green after each stage.
