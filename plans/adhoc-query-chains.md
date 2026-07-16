# Ad-hoc multi-segment query chains (transient ViewChain execution)

## 1. Problem

The single-segment ad-hoc executor runs a SELECT that compiles to **one** circuit as a transient
(`RelationKind::Transient`), driven once over the committed base on the steady-state tick discipline,
streamed, dropped. It rejects `segments.len() > 1` with a clean `Unsupported`.

But the view planner emits a `ViewChain` **bundle** (`plan/view/dispatch.rs:138-150`,
`MAX_CHAIN_SEGMENTS = 64`) for: 3+-way joins and self-joins (`plan_join_chain` pushes a hidden segment per
intermediate step / a pass-through wrapper per repeated base), `DISTINCT`/`GROUP BY` *over a join* (the join
compiles to a hidden view `H`), correlated scalar subqueries / `ANY`/`ALL` (`emit_scalar_subquery_pieces`
pushes a hidden reduce + join), non-pass-through CTEs, and derived tables in FROM. This plan lifts the
`len > 1` rejection: run the whole chain once as a bundle of transients, discard.

The chain is drivable **without any durable registration or DepTab membership** — the mechanics that resolve
segment ordering and upstream-output reads never touch the DepTab, and staying out of it is what keeps the
transient chain invisible to the maintenance fan-out.

## 2. What the chain needs beyond the single-segment executor

The single-segment executor already provides: the `Transient` lifecycle + `checkpointed()` exclusion, the
`FLAG_RUN_TRANSIENT`/`FLAG_DROP_TRANSIENT` frames + `SalMessageKind`s + dispatch-matrix cells, the in-memory
`LoadedCircuit` builder + `compile_loaded` split + `register_transient_plan`, the monotonic Transient id
allocator, the non-blocking tick-discipline drive (emit like a tick under `sal_writer_excl`, no
`maybe_checkpoint`, async ACK await, `TickTrigger::Quiesce`), `ViewMeta::from_loaded` master pre-injection,
the `view_all_sources_replicated`/`flush_view_or_abort` lifecycle-opacity, and the eager `.await`ed
`DropTransient` teardown + dedicated boot scratch GC. The chain reuses all of it **per segment**, adding:

## 3. Ship the whole bundle with local ids

`compile_query_to_circuit(query) -> Vec<PlannedView>` already returns every segment (the single-segment path
just took `segments[0]`). The client ships **all** segments in the `FLAG_RUN_TRANSIENT` frame (each
segment's 3 circuit families + output-schema family), and mints segment ids from a **local** provisional
source (sequential from 1), **not** `client.alloc_table_id()`. Each downstream segment's circuit already
embeds its upstream segments' provisional ids as `ScanDelta`/`ScanTrace` source ids (the view planner embeds
`vid` in the emitter closure before building the circuit, `join.rs:675`).

## 4. Server-side: remap, register, order, drive

**Remap.** The master allocates N monotonic Transient tids and builds a `provisional → tid` map, then
rewrites every segment circuit's `ScanDelta`/`ScanTrace` source ids that point at an **in-bundle** provisional
id to its tid (base-table source ids are left untouched). Applied during the `LoadedCircuit` build.

**Register all N up front.** The master registers `tables[tid_i]` (each segment's output schema, from the
frame) and injects `meta[tid_i] = ViewMeta::from_loaded(&loaded_i)` (each segment can shuffle independently)
for **every** segment before driving any — so a downstream segment's `compile_loaded` finds every upstream
segment's schema in `ext_tables` (the resident `tables` map), and `open_store_cursor(upstream_tid)` resolves.

**Order by a local Kahn sort — never the DepTab.** Derive the intra-bundle order directly from each
segment's in-memory `Circuit::dependencies()` (the `ScanDelta` walk, `gnitz-core/src/circuit.rs:41`),
restricted to in-bundle tids: Kahn's algorithm with the input-order fallback on the (by-construction
impossible) cycle — the same algorithm as `order_by_intra_bundle_deps` (`meta.rs:145`) but sourced from the
bundle, **not** from `get_source_ids`/`self.dep`/DEP_TAB. The transient chain writes **no** DEP_TAB rows, so
`evaluate_dag_multi_worker`'s push fan-out (which reads `self.dep`, populated exclusively from DEP_TAB,
`exec.rs:414`) can never enqueue a transient segment — no maintenance entanglement, and no `maintained()`
accessor needed.

**Drive in dependency order.** For each segment in Kahn order, run the single-segment tick-discipline drive:
enumerate its `get_source_ids` (base tables **and** upstream Transient tids alike), drive each source to
completion (awaiting all-worker ACKs before the next source), ingesting into `tables[tid_i]`. A downstream
segment's upstream source resolves via `dag.tables[upstream_tid].handle.open_cursor()`
(`open_store_cursor`, `store_io.rs:438` — keys purely off `dag.tables`, never DEP_TAB), populated by the
upstream segment's just-completed drive. **Stream only the final segment**; intermediate segments accumulate
into their `tables[tid]` (never flushed to shards — the `flush_view_or_abort` gate applies to every segment).

**Self-join pass-through wrapper.** A self-join's wrapper segment is `ScanDelta(base) → Sink(vid)`, a full
identity copy under a fresh id (`join.rs:568`, `passthrough_rel` + `emit_linear`) — its sole purpose is to
manufacture a second distinct `source_id` so the join's two inputs are never the same id in one epoch
(preserving single-source-per-epoch). For a transient, materialize it like any accumulate segment (drive it
first in Kahn order, ingesting the base copy into its `tables[tid]`); the join then reads the base once
directly and the wrapper once — two distinct source ids, correct cross-term.

## 5. Teardown

The bundle allocates N `tables[tid]`/`cache[tid]`/`meta[tid]` + N scratch dirs. At the handler tail, emit a
`FLAG_DROP_TRANSIENT` covering the **whole tid range** (after the final segment's last source ACK), then free
the master maps — the single-segment eager-teardown protocol, over N ids. Each worker's `DropTransient` arm
frees the range idempotently. Boot GC over the dedicated `<base>/_transient/` scratch root already reclaims
every segment's scratch dir after a fail-stop, regardless of N.

## 6. Testing

- **Integration** (`crates/gnitz-sql/tests/`): a 3-way join, a join + GROUP BY, a self-join, a correlated
  scalar subquery, an `EXISTS` over a join, and a CTE each return the **same rows and weights** as the
  equivalent CREATE VIEW then scan; the Kahn order equals the durable `order_by_intra_bundle_deps` order for
  the same bundle.
- **E2E** (`GNITZ_WORKERS=4`): each multi-segment shape is **weight-correct at W=4** (guards the per-segment
  shuffle + the upstream-output read across segments); a chain runs concurrently with pushes to an unrelated
  table and a forced checkpoint mid-drive without stalling ingestion or leaking (the non-blocking property,
  extended to N segments); teardown after an N-segment chain leaves `tables`/`cache`/`meta` empty and removes
  all N scratch dirs; a fail-stop mid-chain leaves no scratch after the next boot.
- **`make verify`** then **`make e2e WORKERS=4`** green.
