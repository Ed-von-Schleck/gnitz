# Join-trace elision: bind a PK-keyed join side to the base table's own store

## Problem

Every SQL equi-join view maintains a private `IntegrateTrace` child table per
side (`_int_{view_id}_{nid}`, `query/compiler/emit.rs:530-550`), fed by that
side's reindex MAP (`gnitz-sql/src/plan/view/join.rs:296-328`). When the join
key **is the side's primary key** — the canonical fact⋈dim shape,
`ON fact.customer_id = dim.c_id` — that child table is a byte-for-byte
re-keyed copy of the dim's own store: same rows, same OPK key bytes, same
per-worker partition slice. Each view pays:

- a full duplicate of the dim relation per view (O(views × |dim|) memory),
- a second ingest → memtable consolidation → heap-fold → spill → compaction
  pipeline for every dim delta, per view (the flush/compact cluster is
  ~25-30% of worker cycles in the `view_maintenance` bench profile),
- a full re-derivation of that duplicate at every boot and view recompile.

The engine already has the machinery to read a join trace from a registered
table instead: `OpNode::ScanTrace(tid)` feeding a Join's `PORT_TRACE` binds
an external-table trace register (`is_join_trace_side` + the ScanTrace emit
arm, `emit.rs:111-132, 183-217`; cursors per epoch via `build_ext_cursors`,
`query/dag/mod.rs:1417-1448`), and `CircuitBuilder::join(delta,
trace_table_id)` emits exactly that shape (`gnitz-core/src/circuit.rs:392-399`).
Today only the C and Python circuit-builder APIs use it
(`gnitz-capi/src/lib.rs:1148-1151`, `gnitz-py/src/lib.rs:1929`); no
SQL-planned view does.

This plan makes the SQL planner emit the table-backed form for eligible join
sides, deleting the duplicated trace, its maintenance, and its backfill.

## Eligibility (per side; evaluated in `execute_create_join_view`)

Side `S` (schema `s_schema`, table id `s_tid`, join columns `s_join_cols`,
carried targets `s_target_tcs`) is **store-bound** iff all of:

1. **Equi join** (the range/band builder is untouched: its trace must be
   ordered by `[eq…, range]`, never the PK).
2. **Base table**: `client.resolve_table_id(schema_name, name)` succeeds for
   `S`'s relation (TABLE_TAB-only probe — a view name misses it,
   `gnitz-sql/src/bind/resolve.rs:129-151`). Views keep per-view traces: a
   view store's PK is not DML-governed and its content is worker-locally
   partitioned by the *view's* key, not necessarily the join key.
3. **Key = full PK**: `s_join_cols == s_schema.pk_cols` as an ordered
   sequence.
4. **No key promotion on S**: every entry of `s_target_tcs` is `0`
   (self-derive). With 3+4 the reindexed `_join_pk` bytes equal the base
   table's natural PK region byte-for-byte: a self-derived reindex of a PK
   column is a verbatim OPK-window copy (`PromoteKind::Pk`,
   `ops/reindex.rs:102-133`; `reindex_output_type_code` keeps fixed ints at
   their own width and sign, `gnitz-wire/src/types.rs:373-382`), so the other
   side's probe keys compare byte-equal against the store's PK region.
5. **Distribution = full PK, or replicated**: read `TABLE_TAB.flags`
   (`table_flags_dist_prefix`/`table_flags_replicated`,
   `gnitz-wire/src/catalog.rs:404-421`; the client already reads these —
   `GnitzClient::table_replicated`, `client.rs:1050-1066`; add the
   `dist_prefix` sibling accessor). Required: `dist_prefix_len == 0` (the
   full-PK default) or `== s_schema.pk_cols.len()`, or `replicated == true`.
   A `CLUSTER BY` prefix table partitions its store by the prefix hash while
   the join scatter routes by the full-key hash — contents would diverge per
   worker.
6. **S is not a preserved side**: `S = right ⇒ !join_type.preserves_right()`,
   `S = left ⇒ !join_type.preserves_left()`. A preserved side's null-fill
   `ν_S = positive_part(S_all − π_S(inner))` needs `S_all` and `π_S(inner)`
   on the identical `[_join_pk, S-payload]` layout; the store-bound trace's
   payload (non-PK columns only) differs from the reindexed `S_all` payload
   (all columns), and reconciling them is a reindex-payload-shape change out
   of this plan's scope. INNER ⇒ both sides may bind; LEFT ⇒ right side only;
   RIGHT ⇒ left only; FULL ⇒ neither.
7. **No reference to S's PK columns** in the SELECT projection or residual
   ON conjuncts. The store's PK values live only in its PK region; the join
   output carries S's *payload* columns (see layout below), so an `s.pk_col`
   reference has no payload slot to project from — a reference makes the
   side ineligible (fallback to today's circuit). Note `SELECT *` projects
   every combined column including S's PK (the Wildcard arm,
   `join.rs:1146-1151`), so a wildcard join view never elides; the win
   applies to explicit projections that skip the dim PK — which is the
   common fact⋈dim shape, since the dim PK duplicates the fact FK.

Anything ineligible falls back to the current `integrate_trace` emission —
the fallback is the unchanged existing code path, not a variant.

PK columns are non-nullable, so the store-bound side never needs the
NULL-key filter (`input_s_match == input_s_raw` already holds today for a
PK-keyed side).

## Circuit shape (right side bound; left mirrors)

Today (`join.rs:281-331`):

```rust
let reindex_b = cb.map_reindex(input_b_match, &right_join_cols, &right_target_tcs, prog_b);
let trace_a = cb.integrate_trace(reindex_a);
let trace_b = cb.integrate_trace(reindex_b);
let join_ab = cb.join_with_trace_node(reindex_a, trace_b);
let join_ba = cb.join_with_trace_node(reindex_b, trace_a);
```

Store-bound right side:

```rust
let reindex_b = cb.map_reindex(input_b_raw, &right_join_cols, &right_target_tcs, prog_b); // delta path, unchanged
let trace_a = cb.integrate_trace(reindex_a);
let join_ab = cb.join(reindex_a, right_tid);              // ΔA ⋈ tick-aligned store of B (see below)
let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z⁻¹(I(A)), unchanged
```

`cb.join` allocates the `ScanTrace(right_tid)` node and wires it to
`PORT_TRACE` (`circuit.rs:392-399`). `Circuit::dependencies()` excludes
`ScanTrace` and keeps `ScanDelta(right_tid)` (`circuit.rs:44-63`), so the dim
remains a cascade dependency (term BA) and DROP-protection is unchanged,
while the trace read never triggers view recalculation by itself.

**Pending compensation — the store-bound join reads tick-aligned state.**
The base store is *push*-aligned: `handle_push` ingests the effective delta
into the store immediately and stashes it in `pending_deltas`
(`worker/mod.rs:1115-1141`); the epoch (tick) runs later, and ticks are
coalesced (`TICK_COALESCE_ROWS = 10_000` / `TICK_DEADLINE_MS = 20`,
`executor.rs:41-42`) — plus a push **must** ingest inline even while the
worker is blocked in an exchange wait (the dispatch matrix,
`worker/mod.rs:623`, deadlock avoidance), with its tick deferred. A private
`IntegrateTrace` is *tick*-aligned. Reading the raw store would therefore
double-count: for SAL order `PUSH(Δa) PUSH(Δb) TICK(fact) TICK(dim)`, the
fact epoch's store cursor already sees Δb (term AB emits the pair) and the
later dim tick re-emits it through term BA against `trace_a` (which absorbed
Δa in the fact epoch). Exactly-once requires term AB to see B **as of B's
last tick**, which is a worker-local identity:

> `tick_aligned(B) = store(B) − pending(B)`, where `pending(B)` is this
> worker's `pending_deltas[B]` (effective deltas ingested since B's last
> tick — the tick drain removes them, `worker/mod.rs:1143-1158`).

The store-bound join term computes `ΔA ⋈ store(B) − ΔA ⋈ pending(B)`; by
bilinearity over net weights this equals `ΔA ⋈ tick_aligned(B)` exactly.
Mechanics:

- **Op**: `op_join_delta_trace` (`ops/join/delta_trace.rs`) gains an
  optional second pass: after the store-cursor `cogroup_intersection`, run
  the identical co-group against the pending batch (`BatchCursor` — the
  batch-backed `SortedKeyStream` the same skeleton already accepts,
  `ops/cogroup.rs:51-94`) emitting each match at the **negated** product
  weight, then consolidate. The pending batch is a base-table batch: PK =
  B's PK = the probe key bytes, payload = B's non-PK columns = the store's
  trace payload — schemas align by construction.
- **Instruction**: `Instr::JoinDT` gains `pending_reg: Option<u16>`, a
  delta-kind register carrying the snapshot; the compiler allocates it for
  every store-bound `ScanTrace`-fed join and `CompileOutput`/`SubPlan`
  expose `pending_regs: Vec<(u16, i64)>` beside `ext_trace_regs`.
- **Seeding + snapshot timing**: `ExchangeCallback`
  (`query/dag/mod.rs:179-181`) gains
  `fn pending_snapshot(&mut self, table_id: i64) -> Option<Batch>`; the
  worker implements it as a consolidated clone of `pending_deltas[tid]`
  (multiple stashed batches merge-consolidate at snapshot time so the
  `BatchCursor` walk sees (PK, payload)-sorted input); the master/test
  impls return `None` (empty compensation — correct wherever push and tick
  are not decoupled, i.e. the single-process `execute_epoch` and backfill
  paths). `execute_multi_worker_step` takes the snapshot **and builds the
  view's ext-trace cursors at epoch entry, before `do_exchange`**
  (`dag/mod.rs:1280-1291`) and threads both into `execute_sub_plan`
  (today's cursor build inside `execute_sub_plan`, `dag/mod.rs:1496-1497`,
  moves up for plans with store-bound sides). Pre-wait snapshotting makes
  the (cursor, pending) pair atomic on the single-threaded worker: an
  inline push landing during the exchange wait adds memtable runs the
  already-built cursor does not reference and pending entries the snapshot
  does not contain — consistently invisible to this epoch, ticked later
  through term BA.

Compensation cost: one extra co-group of the reindexed delta against a batch
bounded by the tick-coalescing window (≤ ~10k rows), per store-bound side
per epoch — small against the deleted per-epoch trace ingest, memtable
consolidation, and fold/spill pipeline it replaces, and zero when the
pending snapshot is empty (the common case outside write bursts).

**Term-AB output layout.** The engine emits the join output as
`[left_PK ‖ left payload ‖ trace payload]` (`merge_schemas_for_join` on the
trace register's schema; `write_join_row` copies every trace payload column).
For the store-bound side the trace schema is the base table schema, whose
payload is the **non-PK columns only**. Define, side-locally:

- `store_payload` = B's non-PK columns in base-schema order,
  `rn = store_payload.len()`;
- `bx` = the ordered list of B source columns that B's reindex MAP copies to
  payload (whatever `build_reindex_program` emits for B at the time of
  implementation), `wb = bx.len()`. **Requirement pinned by this plan:**
  `bx` must contain every column of `store_payload` — the emission keeps B's
  reindex program as the builder produces it and asserts the containment, so
  term BA can reproduce the canonical span.

Then:

- term AB: `[k ‖ A-payload(la) ‖ store_payload(rn)]` (`la` = the left
  reindex MAP's payload width);
- term BA: `[k ‖ B-reindexed(wb) ‖ A-payload(la)]`.

`normalize_to_ab` (`join.rs:71-94`) canonicalizes both onto
`[k ‖ A-payload(la) ‖ store_payload(rn)]`:

- `proj_ab` = identity over `k..k + la + rn`;
- `proj_ba` = `A` cols from `k + wb ..`, then for each column of
  `store_payload` its position within `bx` (`k + bx.pos_of(col)`).

Everything downstream renumbers onto the `rn`-wide right span: `out_cols`
(`join.rs:438-467`) pushes only B's non-PK defs; the residual filter's
`merged_schema` and the projection's alias map expose B as `store_payload`
with adjusted intra-side indices (a PK-column reference was already rejected
by eligibility rule 7); the `MAX_COLUMNS` guard uses `k + la + rn`.

## Why the delta/trace semantics are exact

With pending compensation, term AB reads `tick_aligned(B)` — B's integral
as of B's last tick on this worker. Epochs (ticks) are serialized on the
worker event loop (concurrent ticks are deferred, never interleaved:
`worker/mod.rs:623` dispatch matrix), so tick order is a total order per
worker and:

- ΔB epoch: only term BA runs (`join_ab`'s delta register is empty); it
  joins ΔB against `trace_a = z⁻¹(I(A))`, i.e. the fact rows ticked before
  this epoch.
- ΔA epoch: term AB joins ΔA against `tick_aligned(B)`, i.e. the dim rows
  whose ticks ran before this epoch.

Pair (a, b) is emitted by AB iff `tick(b) < tick(a)` and by BA iff
`tick(a) < tick(b)` — a perfect XOR, the asymmetric 2-term form
`d(A⋈B) = ΔA ⋈ I(B) + ΔB ⋈ z⁻¹(I(A))` realized on tick order, exactly as
today's private tick-aligned trace realizes it. View read visibility is
also unchanged: a scan between two ticks shows the same contents the
private-trace circuit would show.

**Per-worker content equivalence.** Under rules 3-5 the join key equals the
dim's distribution key, so the join-shard scatter routes the other side's
deltas by `partition_for_pk_bytes` of the dim's own partition bytes — for a
compound or promoted key via `compound_join_packer`
(`ops/exchange/router.rs:276-310`), for the plain single-column key via the
OPK hash invariant (`hash_row_for_partition`, `router.rs:239-255`); both
agree byte-for-byte with the reindexed `_join_pk`, which is already
load-bearing for today's co-partitioned traces. Worker W's store slice is
exactly what W's private trace held. Replicated dims hold a full copy on
every worker, matching the replicated-trace content under the existing
any-replicated co-partition rule (`query/compiler/optimize.rs:36-48`). DML
retractions/updates net out in the store cursor the same way they netted in
the trace (both are ZSet stores read through consolidating cursors);
`unique_pk` enforcement happens at base-table ingest, upstream of both.

**Cursor mechanics.** `build_ext_cursors` opens a cursor on the base table
per epoch (`create_cursor_compacting`, threshold-gated compaction on the
read path — `dag/mod.rs:1426-1431`); the base table is in `dag.tables`, so
resolution needs no new plumbing beyond the epoch-entry timing move above.

## Backfill: store-bound sources must not replay

Join views backfill via `fan_out_backfill(vid, src)` per source
(`view_seeds_exchange_backfill` is true for any Join node,
`dag/mod.rs:828-840`; live driver at
`runtime/orchestration/executor.rs:1710-1728`, boot closure driver at
`runtime/bootstrap.rs:210-217` with `view_id == 0`). Replaying a
store-bound source's existing rows as deltas through its term would
double-count: the other source's replay already joins every one of its rows
against the **full** store (the store predates the view, and backfill runs
with empty `pending` — see serialization below — so compensation subtracts
nothing). The committed replay rule:

> A view's backfill replays every non-store-bound source, plus — when
> **all** of its sources are store-bound (both-sides-bound INNER) — exactly
> one designated source: the **lowest table id**, matching `get_source_ids`
> order. The designated source's replay drives its term against the other
> side's full store (`Δ ⋈ (store − ∅)` = every pair, once); the other
> store-bound source replays nothing and has no trace to fill.

Mechanically:

1. **New DagEngine query** `view_store_bound_sources(view_id) -> Vec<i64>`
   (cached in `ViewPropCache` beside `join_shard`): from the loaded meta
   circuit, every `t` where `OpNode::ScanTrace(t)` has an outgoing
   `PORT_TRACE` edge into a `Join` and `t` is also a `ScanDelta` source.
   A companion `view_backfill_sources(view_id)` applies the replay rule
   above and is the single decision point both drivers consult.
2. **Live CREATE** (`executor.rs:1710-1728`): iterate
   `view_backfill_sources(vid)` instead of `get_source_ids(vid)`. This is
   the only change to the view-scoped driver — the worker-side
   `backfill_view_step` arm needs no guard because the master simply never
   fans out a skipped source (no round exists, so no barrier/pad concern).
3. **Closure-mode backfill** (boot, `view_id == 0`): `drive_dag`
   (`dag/mod.rs:1142-1210`) gains a `backfill: bool`; when set,
   `build_pending` (`dag/mod.rs:1091-1125`) omits seed entries
   `(view, source)` where `view_backfill_sources(view)` excludes `source`.
   Only the seed level can be store-bound (store-bound sources are base
   tables; cascaded pending entries are view outputs), so the filter lives
   only there. Every worker computes the same exclusion from the same
   catalog, so per-round chunk padding stays collectively consistent.
   `handle_backfill`'s `view_id == 0` arm passes `backfill = true`; all
   live paths pass `false`.

**Serialization of backfill vs pushes.** The master executor is
single-threaded and runs the live-CREATE fan-out loop inside `handle_ddl_txn`
before acknowledging the DDL (`executor.rs:1710-1728` precedes the OK
response), so no client push can be dispatched — hence committed on any
worker — while a view-scoped backfill round is in flight; boot backfill runs
quiesced. `pending_deltas` is therefore empty on every worker throughout
backfill, which is what the replay rule's `store − ∅` step relies on.

The inline single-process `backfill_view` (`catalog/ddl.rs:381-445`) never
sees join views (`hooks.rs:500-507` gates it off for any Join) — no change.

## Engine fix: source-node lookup must survive the second node per tid

With elision, source `B` appears as **two** nodes: `ScanDelta(B)` (term BA)
and `ScanTrace(B)` (term AB's trace). `compute_join_shard_cols`
(`dag/mod.rs:573-599`) walks forward from "the ScanTrace/ScanDelta node for
source_id" to find the scatter key; it must collect from **all** nodes
carrying the tid (the ScanTrace node feeds only the Join on `PORT_TRACE` and
contributes no reindex; the ScanDelta node yields `reindex_b`'s key as
today). Adjust the lookup to iterate every matching node and merge the walk
results (dedup identical sequences, exactly as `reindex_cols_through_filters`
already does for sibling reindexes), with a unit test pinning: a circuit with
both node kinds for one tid derives the same join-shard cols as the
ScanDelta-only circuit. (For *eligible* circuits the defect is
consequence-masked — empty cols route to the no-exchange arm, identical to
the co-partitioned dispatch rules 3-5 force anyway — but the fix removes a
HashMap-iteration nondeterminism.) The other per-tid lookups are already
safe: `compute_join_shard_map` walks `ScanDelta` nodes only
(`load.rs:207-217`), `source_reg_map` is populated by the `ScanDelta` emit
arm only (`emit.rs:179`), `dependencies()`/`get_source_ids` are
ScanDelta-derived, `circuit_range_join_n_eq` reads Join nodes only, and
`scan_tid_through_filters` walks backward from an `ExchangeShard`, which an
equi join does not carry.

Everything else already handles the shape: `is_join_trace_side` allocates the
trace register and `ext_trace_regs` binding (`emit.rs:183-217`);
`compute_co_partitioned`/`annotate` read only `ScanDelta` reindex chains;
`execute_multi_worker_step` arm 4 dispatches B's deltas locally
(co-partitioned by rules 3-5) and scatters A's deltas by the join key as
today (`dag/mod.rs:1280-1291`).

## Lifecycle

Nothing new: the base table outlives the view; `unregister_table` on view
drop removes the plan (and its remaining owned child tables) without touching
the base store; recompile re-resolves the `ScanTrace` register against
`dag.tables`. The elided side has no scratch dir to create, erase, or defer-
delete.

## Files touched

| File | Change |
|---|---|
| `crates/gnitz-core/src/client.rs` | `table_dist_prefix(tid)` accessor beside `table_replicated` |
| `crates/gnitz-sql/src/plan/view/join.rs` | eligibility check; store-bound emission (`cb.join`), `normalize_to_ab`/`out_cols`/`equi_nf` `o_col_tcs` arity/alias-map renumbering onto the `store_payload` span; fallback path untouched |
| `crates/gnitz-engine/src/ops/join/delta_trace.rs` | optional negated pending pass (`BatchCursor` co-group) |
| `crates/gnitz-engine/src/query/vm/mod.rs`, `vm/exec.rs`, `vm/builder.rs` | `JoinDT.pending_reg`; seeding in the dispatch arm |
| `crates/gnitz-engine/src/query/compiler/{mod,emit}.rs` | pending-register allocation for store-bound ScanTrace joins; `pending_regs` on `CompileOutput`/`SubPlan` |
| `crates/gnitz-engine/src/query/dag/mod.rs` | `view_store_bound_sources` / `view_backfill_sources` (+ ViewPropCache slots); `ExchangeCallback::pending_snapshot`; epoch-entry cursor+snapshot in `execute_multi_worker_step`; `drive_dag`/`build_pending` backfill filter; `compute_join_shard_cols` multi-node lookup |
| `crates/gnitz-engine/src/runtime/orchestration/executor.rs` | live-create fan-out iterates `view_backfill_sources` |
| `crates/gnitz-engine/src/runtime/orchestration/worker/mod.rs` | `pending_snapshot` impl over `pending_deltas`; closure-mode `backfill = true` |

## Testing

- **Eligibility matrix (planner unit tests)**: each rule flips elision off —
  view-as-dim, key ≠ full PK, permuted key order, a *dim-side* carried
  promotion (dim PK I32 vs fact key I64 → common type I64 ≠ dim's
  self-derived slot, so `carried_reindex_tc ≠ 0` — rule 4), CLUSTER-BY-prefix
  dim, preserved side (LEFT preserving the dim), `SELECT d.c_id`,
  `SELECT *`; and the positive cases: INNER both sides bound when both
  qualify, LEFT with dim on the right bound, replicated dim bound, and —
  eligible by rules 3-5 — a *fact-side* promotion against an unpromoted dim
  PK (fact key I32 vs dim PK I64: the dim self-derives, carried tc 0, and
  the fact side's promoted I64 OPK probe bytes equal the dim store's PK
  bytes). Assert circuit shape (presence/absence of `IntegrateTrace` /
  `ScanTrace` nodes).
- **Engine unit**: `compute_join_shard_cols` with dual nodes per tid;
  `view_store_bound_sources` derivation.
- **E2E parity** (`GNITZ_WORKERS=4` and `WORKERS=1`): a fact⋈dim INNER and a
  fact-LEFT-dim view, store-bound, vs the same queries forced ineligible
  (e.g. selecting `d.c_id`): identical results through a mixed
  insert/update/delete stream on **both** tables — dim churn exercises term
  BA against `trace_a` plus store netting; fact churn exercises term AB
  against the store.
- **Pending compensation (unit, ops/join tests)**: `op_join_delta_trace`
  with a pending batch — matches subtracted at negated product weight;
  pending rows absent from the store snapshot (impossible in production but
  pins the algebra); empty pending = today's behavior byte-for-byte.
- **Coalesced-tick double-count pin (e2e)**: push Δfact then Δdim inside one
  coalescing window (single `execute_sql` batch touching both tables, or two
  pushes without an intervening read), where Δfact rows join Δdim rows; the
  pair must appear exactly once. This is the exact SAL shape
  `PUSH PUSH TICK TICK` that raw-store reading double-counts.
- **Backfill**: create the view over pre-populated fact+dim (both orders of
  table-id: dim < fact and fact < dim, pinning order-independence of the
  replay rule); contents equal the fresh-data run. Both-sides-bound INNER
  over pre-populated tables (designated lowest-tid source replays; view
  complete, exactly-once). Boot recovery: restart the server and assert
  re-derivation equality (closure-mode filter).
- **Interleaving**: pushes to dim and fact alternating in small batches with
  reads between epochs — the exactly-once pair accounting (no double, no
  loss) across the tick-aligned asymmetric form.
- **Drop/recompile**: drop the view (base table intact, further dim pushes
  fine); recreate; drop the dim → rejected/behaves per existing dependency
  rules (unchanged by this plan, but pinned).

Perf validation: dim-churn variant of the view-maintenance bench (updates on
`dim_customer` during the loop) A/B interleaved — the elided design removes
the per-view dim-trace ingest/flush entirely; memory: assert the absence of
`_int_` scratch dirs for the bound side.

## Invariants preserved

- The store-bound term reads `tick_aligned(B) = store(B) − pending(B)` —
  the same tick-aligned integral the private trace held — so view contents
  and read visibility at every tick boundary are unchanged; z⁻¹ semantics
  ride on the worker's serialized tick order exactly as today.
- Delta scatter and trace partitioning co-partition byte-for-byte (rules 3-5
  make the join key the store's own distribution key; promotions are
  excluded).
- `dependencies()` still lists the bound side (via its `ScanDelta`), so
  cascade wiring, DROP protection, and dep-map ordering are unchanged.
- Weight exactness of the remaining null-fill (`ν` of the *other*, preserved
  side) is untouched — its operands never involve the bound side's payload
  layout.
- Views ineligible on any rule compile to byte-identical circuits to today.
