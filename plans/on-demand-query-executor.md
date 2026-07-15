# On-demand query executor: ad-hoc SELECT as a run-once DBSP circuit

## 1. Problem

gnitz has **two query engines that share nothing**:

- **Ad-hoc SELECT** (`crates/gnitz-sql/src/dml/select.rs`) — a thin, client-resident
  keyseek engine: an access-path ladder (PK seek → `pk IN` multi-seek → ordered
  range index → equality index → hard error, `select.rs:103-178`), gather the whole
  reply into one client `ZSetBatch`, project/limit client-side. It has **no
  relational operator**. Joins, GROUP BY, DISTINCT, aggregation, set-ops, and every
  non-indexed WHERE are rejected with "belongs in a CREATE VIEW". Its residual filter
  is **i64-only** (`exec/eval.rs`) — it cannot filter a `TEXT`/`F64` column even when
  that column *is* indexed. There is no ORDER BY, OFFSET, or deterministic LIMIT.
- **CREATE VIEW** (`crates/gnitz-sql/src/plan/view/`) — the full DBSP compiler: every
  relational shape, compiled to a circuit, registered, materialized, maintained
  incrementally, checkpointed, recovered.

Every relational capability is therefore built **twice** (a client keyseek wart and
an engine circuit) or forced into a **persistent, maintained view** even for a
one-shot answer.

The fix is to **unify the compute, not the dispatch**. The relational *compute* —
join, reduce, distinct, set-ops — already exists once, in the operator kernels and
the view compiler. What ad-hoc SELECT lacks is a way to *run that compute once and
discard it*. So: compile a SELECT with the **existing view planner** into the same
circuit a CREATE VIEW builds, run it **once** over the committed base snapshot as a
**transient** (unregistered, non-durable, unmaintained) circuit, stream the result,
discard all state. This is sound in the engine's own theory (CLAUDE.md §3): every
operator is "first a scalar function on Z-Sets, then lifted"; the engine today only
instantiates the lifted (incremental) form. The scalar form — run the DAG once over
the base Z-sets — *is* an ad-hoc query, already realized by `backfill_view`.

**Crucially, the dispatch stays split.** A point/index read is one unicast seek with
zero allocations; a circuit is a compile + trace-table allocation + epoch drive.
Forcing the cheap read through the expensive machine is a pure regression. So the
**thin keyseek path is kept, unchanged**, for exactly the reads it serves cheapest;
the executor picks up only the shapes the thin path *rejects today* — by turning its
two `return Err`s into **fall-through**, not by replacing it.

Payoff: ad-hoc SELECT gains the full relational surface (joins, GROUP BY,
aggregation, DISTINCT, set-ops, subqueries), a **server-side, fully-typed** WHERE
(dissolving the i64-only and non-indexed-error limits), and — at the serving edge —
ORDER BY / OFFSET / deterministic LIMIT. No point-lookup regression; no engine
deleted.

## 2. The lifecycle model (the unifying frame)

A view today is *literally* "compile + register + `backfill_view` once + then keep
ticking." An ad-hoc query is that same sequence **truncated before maintenance**:
compile, register (in-memory only), run once, drop. The two are not parallel engines
— the query is a **prefix of the view lifecycle**.

`RelationKind` (`query/dag/mod.rs:84`) is already a "property-bundle" enum ("Bundles
every per-kind property that used to be set independently") with three variants:
`View` / `BaseTable` / `SystemCatalog`. The plan needs a **fourth**, because a
transient can be none of them: `View` is checkpointed/maintained, `BaseTable` is
`unique_pk`-tagged with base-only DML/index paths, `SystemCatalog` is neither. This
variant is not optional decoration — `TableEntry.kind` is non-nullable and
`register_table` is **uncallable** without it.

Add **`RelationKind::OneShot`**: ephemeral (Rederive traces), **not** maintained (no
dep edges), **not** checkpointed, **not** durably named. Replace the scattered
`kind == View` predicates that today conflate several axes with **positive lifecycle
accessors** — `lifecycle.checkpointed()`, `lifecycle.maintained()`,
`lifecycle.durable()`, `lifecycle.drains_ticks()`. This is required for correctness,
not tidiness:

- **Checkpoint exclusion (a real bug in the naive design).**
  `collect_ephemeral_flush_tables` (`query/dag/ingest.rs:253`) has **two** halves: the
  *trace* half iterates `self.cache.values_mut()` with **no `kind` filter**; only the
  *output* half filters `kind != View`. A transient's compiled plan **must** live in
  `cache[tid]` to run, so a `kind != View` guard alone would still let a checkpoint
  fire mid-query and flush the transient's operator traces. Both halves must gate on
  `lifecycle.checkpointed() == false`. A positive accessor is robust by construction;
  the negative `kind != View` form is one forgotten guard from a checkpoint leak.
- **Freshness / recovery** likewise dispatch off `is_view()` today (`dag/mod.rs:131`,
  `RecoverySource`); a OneShot wants "yes freshness-drain, no checkpoint, no recover",
  a combination only per-axis accessors express cleanly.

A later **`CachedPlan`** preset (keep the compiled plan, drop the state) is the
natural home for plan-caching — see §9. It is a lifecycle bit-flip, not a fifth
special case.

## 3. Routing — thin path unchanged, executor by fall-through

`execute_select` keeps the existing access-path ladder verbatim. The **only** change
is that the two points where it `return Err` today instead **fall through** to the
executor:

- `select.rs:172` "WHERE on non-indexed column not supported" → run as a circuit
  (server-side `op_filter` over a full source scan; §5).
- an indexed seek whose residual is non-i64 (string/float/u128 — `exec/eval.rs`
  rejects it) → run as a circuit.

And the up-front rejections that make a query "not a single-table keyseek" —
`JOINs not supported` (`select.rs:93`), DISTINCT/GROUP BY/HAVING routed to CREATE
VIEW (`select.rs:56`), multi-table FROM — **route to the executor** instead of
erroring. The router is therefore **not a new classifier**: the thin path serves what
its ladder resolves; everything it would have rejected goes to the executor. This
sidesteps the "dynamic serviceability" instability (an index dropped between calls
just changes which side of the existing ladder a query lands on, exactly as today).

Behaviors the thin path keeps that the executor does **not** need to replicate,
because the thin path still serves them: the `pk IN (…)` fetch-cap early-stop
(`seek_pk_multi` `row_cap`, `dml/plan.rs:231`), point-seek unicast, i64 residuals.
One user-visible change: `SELECT non_pk_col FROM t` (dropping all PK columns) is
rejected by the thin path today (`exec/batch.rs:100`, "projection must include a
PRIMARY KEY column"); it now falls through to the executor, which gives the result
its own hidden output key and **succeeds** — a capability gain (tested for parity of
the *other* thin-path errors, §12).

## 4. Request lifecycle

```
CLIENT (gnitz-sql / gnitz-core)
  execute_select(query):
    if thin_ladder_resolves(query): serve via thin path (unchanged) + client sink (§8)
    else:
      circuit, out_schema = compile_query_to_circuit(query)   // reuse the view planner
      rows = client.run_query(circuit, out_schema)            // FLAG_RUN_TRANSIENT; streamed result
      return sink(rows, order_by, offset, limit)              // §8

MASTER
  decode transient frame -> CircuitRows + out_schema
  tid = allocate_oneshot_id()                                 // reserved server range (§5)
  register_transient_plan(tid, compile_loaded(...), out_schema, RelationKind::OneShot)
  synthesize + inject ViewMeta[tid] (shard_cols/join_shard_map/range_n_eq/needs_exchange/has_join)  // §10
  guard = TransientGuard(tid)                                 // drives the ephemeral teardown protocol (§11)
  for source_id in circuit.sources:                           // enumerate the circuit, NOT the (empty) DepTab
     drive one-shot epochs over source_id (broadcast; exchange loop if sharded, §10)
  if shape is Single/linear: stream the terminal epoch batch directly       // no output store
  else:                     scan the accumulated RAM output store, stream    // join/agg/distinct/setop
  // guard drop -> ephemeral drop-transient broadcast + master free (§11)

WORKER
  dispatch_inner(RunTransient, tid, block, req_id):    // new arm, Gather/Scan template
     build LoadedCircuit from block; compile_loaded under tid (scratch dir)
     run this shard's epoch(s) over the local source cursor(s); exchange via do_exchange if sharded
     stream_batch_response(result, ReplySchema::OneOff(out_schema), req_id, client_id)
  dispatch_inner(DropTransient, tid, ...):             // new arm: free tables[tid]/cache[tid]/meta[tid] + scratch
```

## 5. Component changes — the executor

Reused verbatim: the operator kernels (`ops/`), the VM, the compiler internals after
`load_circuit` (`topo_sort`/`annotate`/`build_plan`), the exchange transport
(`ExchangeCallback::do_exchange`, `query/dag/mod.rs:204`; `ExchangeAccumulator`), the
per-node circuit codec (`encode_op_node`/`decode_op_node`, `gnitz-wire/src/circuit.rs`),
`Circuit::into_rows` (`gnitz-core/src/circuit.rs:59`) + the `encode_ddl_txn` family
framing (`gnitz-core/src/protocol/message.rs:313`), the streamed-reply path
(`stream_batch_response` + `ReplySchema::OneOff`), and `unregister_table`
(`query/dag/mod.rs:265`).

- **gnitz-sql** — factor `compile_query_to_circuit` out of the CREATE VIEW path
  (`plan::execute_create_view` already drives the view planner on the query, then
  registers; split the "query → `Circuit` + output `Schema`" half so both callers
  share it). `execute_select` gains the §3 fall-through + the §8 sink. **No new SQL
  planning** — the view planner already handles the full relational surface.
- **gnitz-core** — `GnitzClient::run_query(circuit_rows, out_schema)`: encode a
  **`FLAG_RUN_TRANSIENT`** frame (the `encode_ddl_txn` family-block layout carrying
  the 3 circuit families + the output-schema column family, tagged with a
  server-allocated `tid` + `client_id`/`request_id`; **no** VIEW_TAB/COL_TAB/DEP rows,
  no fsync), reassemble the streamed reply as `recv_scan` does.
- **gnitz-wire** — add the `FLAG_RUN_TRANSIENT` and `FLAG_DROP_TRANSIENT` flag bits.
- **gnitz-engine**:
  - `SalMessageKind::RunTransient` + `DropTransient` (`runtime/protocol/sal.rs:229`):
    `classify` arms, `ALL_KINDS` entries, top-level `dispatch` arms, and worker
    `dispatch_inner` arms (`worker/mod.rs:675`) modeled on the `Gather` handler
    (`mod.rs:737`).
  - **In-memory `LoadedCircuit` builder + `compile_loaded` split.** `load_circuit`
    (`compiler/load.rs:40`) is the only sys-table-reading step in `compile_view`;
    split it out and add `compile_loaded(loaded, view_dir, view_schema, ext_tables)
    -> Result<CompileOutput>` (the body from `mod.rs:310`). A builder constructs
    `LoadedCircuit` from the delivered `CircuitRows` directly (same `decode_op_node`,
    same `(kind, position)` node-column sort, same edge-validity check), mapping the
    client's `u64` node ids to the engine's `i32` keys (client ids are small and
    sequential — no truncation risk).
  - **`register_transient_plan(tid, CompileOutput, out_schema, OneShot)`** — a new
    inserter that seeds `cache[tid]` + `tables[tid]` (scratch `view_dir` + out schema)
    directly, because `ensure_compiled` (the only cache inserter today) reads sys
    tables and returns `None` for a transient.
  - **Server-allocated OneShot ids** from a reserved range distinct from the durable
    table-id space, so concurrent transients get distinct `tid` **and** scratch
    `view_dir` (scratch tables are named `_{hist,int,reduce}_{tid}_{nid}`,
    `emit.rs:590` — a collision corrupts a concurrent query).
  - **A one-shot driver** (not `backfill_view` verbatim — it derives sources from the
    DepTab, which is empty for an unregistered transient): enumerate the compiled
    circuit's `ScanDelta`/`ScanTrace` source ids directly, drain each source
    chunk-wise, drive its epochs to completion (single-source-per-epoch, seeding each
    join input's trace across epochs — §10), then stream.
  - **Stream-direct vs accumulate.** A `Single`/linear shape's terminal epoch output
    is already the complete result in one batch — stream it via `stream_batch_response`
    with no output store. A multi-source/non-linear shape (join/reduce/distinct/set-op)
    accumulates deltas into an unregistered RAM output `Table` (`tables[tid]`, which
    carries the circuit's hidden output key — `_join_pk`/`_group_pk`), scanned once via
    `scan_family(tid)`.

## 6. Visibility & consistency

The executor reads the **committed base snapshot on the workers, with view-scan
freshness** (drain pending ticks before reading, `executor.rs:1730`; catalog read
lock excludes concurrent DDL; no MVCC). It does **not** reflect the calling
transaction's own uncommitted buffered writes. This is **byte-for-byte today's direct
SELECT visibility**: `execute_select` reads through raw `client.scan`/`seek` with **no
overlay**; the read-your-own-writes overlay (`dml/overlay.rs`) is DML-only and
base-PK-keyed — fundamentally incompatible with circuit execution, which dissolves
base-PK identity through joins/aggregates. The existing SELECT-vs-DML asymmetry is
preserved exactly; read-your-own-writes for ad-hoc SELECT is a **non-goal**.

## 7. Result model

One entry per `(PK, payload)`, weight surfaced as a per-row attribute, never expanded
(`gnitz-py/src/lib.rs:1388`; `apply_limit` counts entries). Shapes without a natural
key carry a hidden synthetic slot (`_group_pk`/`_join_pk`/`_set_pk`), stripped by
`visible_columns()` — identical to a view scan. DISTINCT and aggregation collapse
multiplicity **server-side inside the circuit**, so `SELECT DISTINCT` returns true
distinct rows (the projection-PK limitation that blocks client-side DISTINCT does not
apply — the circuit owns the output key).

## 8. The sink — ORDER BY / OFFSET / LIMIT (`exec/order.rs`)

Client-side over the returned batch (thin-path result or streamed executor result). A
typed multi-column comparator over the `ZSetBatch` matching the engine's
`compare_rows` order: signed ints signed-order, unsigned magnitude, `F32`/`F64`
`total_cmp`, `STRING`/`BLOB` byte-wise (== the engine's german-string content order),
`U128`/`UUID` unsigned, `I128` signed. Null placement absolute (default NULLS LAST for
ASC / FIRST for DESC; explicit honored), not flipped by DESC. Keys resolve as a column
reference or a **1-based visible-output position**. Pipeline: stable-sort a row-index
permutation, slice `[offset, offset+limit)`, materialize via `copy_batch_row`; a plain
`SELECT` with no order/offset/effective-limit is a zero-copy passthrough. Envelope
honoring: add `order_by: bool` to `HonoredQueryClauses` (`plan/validate.rs:276`), gate
the ORDER BY reject on it (`true` only at the `dml/select.rs` site; `false` at the
other six), remove the OFFSET reject, add `extract_offset`.

Two correctness requirements (from adversarial review):
1. **Positional ORDER BY is visible-column-aware** — `ORDER BY n` maps to the *n*-th
   **visible** output column (skip `META_FLAG_HIDDEN`), because a circuit result over a
   hidden-PK shape carries a hidden column at index 0 and `SELECT *` is the queryable
   form. Mirror `visible_columns()` / `apply_hidden_column_aliases`
   (`view/dispatch.rs:360`). Naive physical `pos-1` sorts by the hidden key silently.
2. **No worker-count determinism claim** — a scan is per-worker sorted runs
   concatenated in rank order, hash-partitioned, not a global merge; a stable sort's
   tie-break differs W=1 vs W=4. Ties are unspecified by SQL; tests assert a total
   order over unique keys or per-tie-group set-equality.

**Honest cost.** The sink is **O(result)**, client-side. For filtered/aggregated
queries the result is small. For an *unfiltered* `SELECT * FROM t ORDER BY x` the
result is the whole relation, streamed to the client and sorted in RAM; a string/blob
result must additionally fit one `MAX_W2M_MSG` frame (`worker/reply.rs:268` — the
existing view-scan limit, inherited). The real fix for both is a **server-side
ordered/bounded/paged result sink** (a master-side top-N heap-merge across workers) —
the deeper missing primitive that view scans need too. It is a **non-goal of this
plan** (a separate, larger effort); the client-side comparator here is exactly the
comparator that sink would need, so it is forward-compatible.

## 9. Non-goals

- **Deleting the thin keyseek path** — kept permanently; it serves point/index reads
  optimally and the executor has no cheaper equivalent. Unify compute, not dispatch.
- **Bounded circuit sources** — with seeks on the thin path, the executor always
  full-scans its base sources (correct: a non-indexed filter has no index to bound,
  and relational shapes read whole sources anyway) and always broadcasts/replicated,
  never unicasts (which avoids the point-bound-into-exchange deadlock: a unicast into a
  join/GROUP-BY circuit would block one worker in `do_exchange_wait` on a relay the
  other workers never join). A source-bound optimization for mixed
  indexed+non-indexed predicates is possible later, gated on a benchmark — not
  load-bearing.
- **A server-side ordered/bounded/paged result sink** (§8) — the real deeper primitive
  for large ordered reads; separate larger effort.
- **Plan caching** — each query compiles fresh. The natural home is a future
  `CachedPlan` lifecycle preset (§2): keep the compiled plan across identical queries,
  drop the state. A lifecycle bit-flip, out of scope here.
- **Read-your-own-writes for buffered txn writes in ad-hoc SELECT** (§6).
- **STRING/BLOB result chunking** — one-frame limit inherited from the view-scan path.
- **Standing/incremental queries** — `CREATE VIEW` remains the maintained preset.

## 10. Distributed correctness (the substantial piece)

Multi-epoch one-shot joins are correct and are exactly the live CREATE-VIEW
distributed backfill: `fan_out_backfill` is synchronous (`collect_acks_and_relay`
blocks to completion, `master/dispatch.rs:354`), so the master drives source A's
epochs fully — seeding `I(A)` via the join's IntegrateTrace — then source B, whose
`dB ⋈ z⁻¹(I(A))` yields the full join, accumulated into `tables[tid]`. Streaming
happens after the loop, so there is no barrier problem, and single-source-per-epoch
keeps the symmetric 2-term DBSP form valid. The `execute_multi_worker_step` driver
(`query/dag/exec.rs:305`) picks replicated / range-join / exchanged / join-scatter /
single arms **from the compiled plan** — id-parametric, reused for the transient id.

The real work is the **relay's meta resolution**, which the naive design understates.
`prepare_relay` (`master/dispatch.rs:554`) resolves re-scatter columns via
`get_shard_cols(view_id)` / `get_join_shard_cols` / `view_range_join_n_eq` — all
funnelling through `view_meta` → `load_meta_circuit` → `load_circuit` **by view_id
prefix over the sys tables** (`meta.rs:241`, `load.rs:40`). A transient has no sys-table
rows, so this returns `LoadedCircuit::default()` and every getter yields the "nothing
special" answer: empty `shard_cols` (→ every row scatters to one partition: a serial
parallelism cliff, and for GROUP BY/equi-join only *accidentally* correct) and
`range_join_n_eq = None` instead of `Some(0)` (→ the master skips the
`op_relay_broadcast` a pure-range join needs → **missing join rows**). And
`shard_cols` is **not** in `CompileOutput` (`compiler/mod.rs:171` carries `shape`,
`join_shard_map`, `range_join_n_eq`, … but `shard_cols` is only a local in
`finalize_side`). So the fix is:

- Thread `shard_cols` (+ `needs_exchange`, `has_join`) into `CompileOutput`.
- **Synthesize a `ViewMeta` for the transient and inject it into `self.meta[tid]`** at
  `register_transient_plan` time, and make `view_meta`/`get_shard_cols`/`get_join_shard_cols`/
  `view_range_join_n_eq` **lifecycle-opaque** — read `meta[tid]`/`cache[tid]` for any
  lifecycle instead of `load_meta_circuit`'s sys-table read.
- Make `get_schema_and_names` (also called by `prepare_relay`, `dispatch.rs:655`)
  lifecycle-opaque: `get_col_names_bytes` (`metadata.rs:151`) lazily reads COL_TAB,
  which a transient lacks — serve names from the injected out-schema instead.

This is the largest single component. It is *not* "redirect one coupling"; it is a
`ViewMeta` reconstruction spanning four meta getters plus the schema/names path.

## 11. Teardown & crash-safety

`unregister_table` (`query/dag/mod.rs:265`) is a **master-local** in-memory map
removal; its only driver today is the durable, fsynced catalog DROP hook applied
per-worker. A Rust RAII `Drop` on the request future **cannot** perform worker teardown
(`Drop` can't await, so it can't broadcast-and-collect). So teardown is a real
**ephemeral protocol**:

- A `TransientGuard(tid)` owned by the in-flight request future fires on **every** exit
  — success, client disconnect (`Ok(false)`), worker fault (`Err`),
  `GNITZ_CLIENT_SEND_TIMEOUT_MS` eviction — and emits a non-durable **`FLAG_DROP_TRANSIENT`**
  broadcast; each worker's `DropTransient` arm frees `tables[tid]`/`cache[tid]`/`meta[tid]`
  + its per-rank scratch dirs; the master frees its own maps. Exactly-once per exit path.
- **Worker fault** is fail-stop (the watchdog reaps the cluster, `exec.rs:257`), which
  *bypasses* the guard, so the transient's scratch dirs (never in any manifest/sys
  table) would orphan on disk across a restart. Reap them with a **boot-time GC of the
  reserved OneShot scratch-dir prefix** (the ids come from a reserved range, so the
  prefix is recognizable), alongside the existing `gc_orphan_directories`.

## 12. Testing

- **Rust unit** (`exec/order.rs`): comparator across every type incl. negatives, U64
  high-bit, `-0.0`/NaN (`total_cmp`), strings, `U128`/`UUID`/`I128`; ASC/DESC; NULLS
  default + explicit not flipped by DESC; multi-key; visible-position over a
  hidden-column schema; offset/limit slicing incl. past-end and passthrough.
- **Engine unit**: `compile_loaded` over an in-memory `LoadedCircuit` == `compile_view`
  over the sys-table form for the same circuit; `register_transient_plan` →
  run → `DropTransient` leaves `tables`/`cache`/`meta` empty and removes scratch dirs;
  the synthesized `ViewMeta` for a pure-range-join transient yields `range_n_eq =
  Some(0)` and non-empty `shard_cols`.
- **Integration** (`crates/gnitz-sql/tests/`, `feature=integration`): each of a join,
  GROUP BY + HAVING, DISTINCT, set-op, scalar subquery, and a non-indexed WHERE on a
  string and a float column returns the **same rows and weights** as the equivalent
  CREATE VIEW then scan; ORDER BY/OFFSET/LIMIT shape the result; `WHERE pk=5` still
  serves via the thin path (assert no transient id is allocated); the thin-path errors
  that remain (e.g. ambiguous column in a `SELECT *`) keep their exact messages.
- **E2E** (`crates/gnitz-py/tests/test_adhoc_query.py`, `GNITZ_WORKERS=4`) — the
  correctness gate, since single-worker hides the shuffle: every shuffle shape
  (join/GROUP BY/DISTINCT/set-op) is **weight-correct at W=4** (the `SELECT k,
  COUNT(*) FROM t GROUP BY k` split-count failure is the specific regression to guard);
  a transient leaves **no** manifest/checkpoint artifact even when a checkpoint is
  forced mid-query (asserts the §2 positive-accessor exclusion); client disconnect
  mid-stream leaves no leaked `tables[tid]`/scratch dirs (asserts §11).
- **`make verify`** then **`make e2e WORKERS=4`** green after each stage.

## 13. Sequencing

Ordered so **no stage ever routes a shape to a path that cannot serve it correctly at
W>1**, and the thin path is never removed.

- [ ] **Stage 1 — executor core, non-shuffle shapes (+ the sink).** The `OneShot`
  lifecycle (4th `RelationKind` + positive `checkpointed()`/`maintained()`/`durable()`
  accessors, fixing the checkpoint-sweep exclusion in *both* halves); `FLAG_RUN_TRANSIENT`
  + `FLAG_DROP_TRANSIENT` + their `SalMessageKind`s and worker arms; `Circuit::into_rows`
  reuse + `run_query` + transient frame codec; in-memory `LoadedCircuit` builder +
  `compile_loaded` split + `register_transient_plan` + server-allocated OneShot ids; the
  one-shot driver enumerating `circuit.sources`; **stream-direct** for `Single`/linear
  shapes; the `TransientGuard` + `DropTransient` teardown protocol + boot-time orphan GC.
  Route (via §3 fall-through) single-source filter/project + non-indexed / non-i64-residual
  WHERE + `SELECT non_pk`. Server-side typed `op_filter`. Ship the client-side sink
  (§8) — it applies to thin-path and executor results alike. **W>1-correct** (single
  source needs no exchange; broadcast+gather). Keep the thin path.
- [ ] **Stage 2 — distributed shuffle.** Thread `shard_cols`/`needs_exchange`/`has_join`
  into `CompileOutput`; synthesize + inject `ViewMeta[tid]`; make
  `view_meta`/`get_shard_cols`/`get_join_shard_cols`/`view_range_join_n_eq`/
  `get_schema_and_names` lifecycle-opaque (§10); reuse `execute_multi_worker_step` + the
  relay loop for the accumulate-then-scan shapes. Now route joins / GROUP BY / DISTINCT /
  aggregate / set-op / subqueries to the executor. **W>1-correct** for every shape.
- [ ] **`make verify` and `make e2e WORKERS=4`** green after each stage, with the W=4
  weight-correctness and checkpoint/leak assertions of §12 as the gate.
