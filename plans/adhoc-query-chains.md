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

## 3. Ship the whole bundle with reserved-band local ids

`compile_query_to_circuit(query) -> Vec<PlannedView>` already returns every segment (the single-segment path
just took `segments[0]`). The client ships **all** segments in one `FLAG_RUN_TRANSIENT` frame and mints
segment ids from a **local** provisional source, **not** `client.alloc_table_id()` — no durable
`SEQ_ID_TABLES` id is burned for an ephemeral query. Each downstream segment's circuit already embeds its
upstream segments' provisional ids as `ScanDelta`/`ScanTrace` source ids (the view planner embeds `vid` in
the emitter closure before building the circuit, `join.rs:675`).

### 3a. Provisional ids live in a reserved sub-band — never `1, 2, 3, …`

A `ScanDelta`'s `source` field is **one namespace shared with durable relation ids**. `ViewChain::new_transient`
mints `next_local_id: Some(1)` today (`plan/view/mod.rs:61-67`, `mint_id` at `:71-80`), which is harmless only
because a single segment has no in-bundle cross-references. **The moment chains ship, it is a silent
wrong-answer bug:** `FIRST_USER_TABLE_ID = 16` (`gnitz-wire/src/catalog.rs:~196`) and
`MAX_CHAIN_SEGMENTS = 64` (`gnitz-core/src/client.rs:192`), so a bundle of ≥ 17 segments mints provisional
id `17`, and §4's remap — which can only recognize "in-bundle" by value — would rewrite a **genuine base
table 17**'s `ScanDelta` onto a transient segment. Ids `1..=13` likewise alias the system catalog families.

Mint from a reserved sub-band instead, so "in-bundle" is a total, unambiguous range check:

```rust
// gnitz-wire/src/catalog.rs — beside FIRST_USER_TABLE_ID, whose bound this is the other end of.
/// First id of the transient band: durable relations live in `[FIRST_USER_TABLE_ID, 1<<31)`, a
/// transient's in-RAM id in `[1<<31, 1<<32)`. See `catalog/sys_tables.rs` for why the band is u32.
pub const TRANSIENT_ID_BASE: u64 = 1 << 31;
/// Bundle-local provisional segment ids: the low `MAX_CHAIN_SEGMENTS` slots of the transient band.
/// Remapped to a real transient tid before it is ever used as a relation id, so it never reaches the
/// storage layer — but it MUST still be u32-safe and disjoint from every durable id, because it rides
/// the wire in a `ScanDelta.source` field alongside real base-table ids.
pub const CHAIN_LOCAL_ID_BASE: u64 = TRANSIENT_ID_BASE;
```
The band base becomes a **wire contract** the moment the client mints into it, so it moves to `gnitz-wire`
and the engine's `pub(crate) const TRANSIENT_ID_BASE: i64` (`catalog/sys_tables.rs:63`) becomes
`gnitz_wire::TRANSIENT_ID_BASE as i64` — one definition, not the same magic number in two crates. Its
doc comment (the u32-contract rationale, `sys_tables.rs:36-62`) stays with the wire constant;
`TRANSIENT_ID_LIMIT` stays engine-private (only the allocator's upper bound needs it).

`ViewChain::new_transient` seeds `next_local_id: Some(CHAIN_LOCAL_ID_BASE)`, so segment `i` is
`CHAIN_LOCAL_ID_BASE + i`. The master's own allocator then starts **above** the sub-band:
`next_transient_id: Rc::new(Cell::new(TRANSIENT_ID_BASE + MAX_CHAIN_SEGMENTS as i64))`
(`runtime/orchestration/executor.rs:338`) — a one-line seed change that splits the transient band into
`[2^31, 2^31+64)` bundle-local and `[2^31+64, 2^32)` master-allocated. A provisional id can then alias
neither a durable relation nor a **concurrent** query's live transient tid.

> **Do not put provisional ids at `1 << 62`.** `TRANSIENT_PROVISIONAL_VIEW_ID = 1 << 62`
> (`gnitz-wire/src/flags.rs:126`) is a **client-side schema-cache key only** (`connection.rs:287`) and is
> documented as *"deliberately UNRELATED to the engine's transient id band"*. A relation id is **`u32` by
> physical contract** — the SAL group header, `Table::table_id`, `PartitionedTable::new`/`ShardIndex::new`,
> `Batch::encode_to_wire`, and the on-disk `shard_{tid}_{lsn}.db` filenames all narrow to u32
> (`catalog/sys_tables.rs:36-73`). That doc names this exact trap: *"An id above 2^32 does not fail loudly —
> it truncates, so `1<<62 | n` would arrive at the worker as `n`, a **live user relation**"*. `1 << 31`
> round-trips every narrowing exactly. The row-key `vid` stamped into the shipped circuit families is a
> different namespace (a column value, never narrowed) — but it costs nothing to reuse the same
> per-segment provisional id for both roles, and one id per segment is what keeps §4's remap total.

`MAX_CHAIN_SEGMENTS` must also be **enforced on this path**. Today only `create_view_chain` checks it
(`gnitz-core/src/client.rs:1073-1078`); `compile_query_to_circuit` (`plan/view/dispatch.rs:186-201`) does
not. The transient encoder rejects `segments.len() > MAX_CHAIN_SEGMENTS` before minting, and the master
re-checks on decode — the sub-band is only `MAX_CHAIN_SEGMENTS` wide, so an over-long bundle would run into
the allocator's range.

### 3b. Frame layout: N positional 4-block groups

The single-segment frame ships 3 circuit families keyed `CIRCUIT_NODES_TAB = 11` / `CIRCUIT_EDGES_TAB = 12` /
`CIRCUIT_NODE_COLUMNS_TAB = 13` plus one output-schema block, and the engine matches the three by tid and
takes the remaining block **"by exclusion: its tid is not a `CIRCUIT_*_TAB` id"**
(`encode_run_transient`, `gnitz-core/src/protocol/message.rs:307-337`; decode at `executor.rs:2117-2127`).
**That scheme does not generalize**: every segment's families carry the *same* three tids, so tid-matching
cannot say which segment a block belongs to, and exclusion cannot separate N schema blocks.

Make **position** the discriminator: the frame body is `4 * N` blocks, segment `i` occupying
`blocks[4i .. 4i+4]` in the fixed order `(nodes, edges, node_columns, out_schema)`. The prologue's `u32`
count becomes `4 * N`. The engine validates `count % 4 == 0` and `count / 4 <= MAX_CHAIN_SEGMENTS`, then
chunks. `N == 1` keeps today's **block count and order** exactly — 3 families in `CIRCUIT_FAMILIES` order,
then the schema block — so the single-segment path is the chain path's degenerate case, not a fork of it.
It is **not byte-identical**: the vid stamped into the circuit rows and the schema block's `u32` tid both
move from `TRANSIENT_PROVISIONAL_VIEW_ID` to `CHAIN_LOCAL_ID_BASE + i` (§3a). That is a pure wire change
with no compatibility concern (pre-alpha), and it retires the exclusion trick — position now identifies the
schema block, so the block's tid carries a real per-segment identity instead of a truncated `0`.

Each segment's circuit rows carry their own provisional `vid`, so the engine builds segment `i` with
`build_loaded_from_batches(nodes_i, edges_i, ncols_i, CHAIN_LOCAL_ID_BASE + i, out_schema_i)` — the call
`transient_loaded` already makes with a fixed vid (`query/dag/mod.rs:568-581`), now parameterized by segment.
`TRANSIENT_PROVISIONAL_VIEW_ID`'s role **narrows to what its doc says it is** — the client's schema-cache key
in `recv_scan` (`connection.rs:287`). Its remaining producers (`client.rs:1231`,
`protocol/message.rs:335`, `query/dag/mod.rs:576`, `query/compiler/mod.rs:1944`) switch to the per-segment
provisional id, and the paragraph of its doc describing the truncated-to-`0` schema-block tid and the
by-exclusion decode (`flags.rs:113-117`) is deleted with the mechanism it describes.

## 4. Server-side: remap, register, order, drive

**Remap.** The master allocates N monotonic Transient tids (`alloc_transient_id`,
`executor.rs:146-160`, now seeded above the bundle-local sub-band per §3a) and builds a
`provisional → tid` map keyed on `CHAIN_LOCAL_ID_BASE + i`, then rewrites every segment circuit's
`ScanDelta`/`ScanTrace` source ids that fall in `[CHAIN_LOCAL_ID_BASE, CHAIN_LOCAL_ID_BASE + N)` to
their tid. Applied during the `LoadedCircuit` build. Base-table source ids are left untouched, and
**that is now decidable rather than guessed**: a durable relation id is `< 2^31` by construction —
`precheck_family` rejects any TABLE_TAB/VIEW_TAB id `>= TRANSIENT_ID_BASE` at the point it enters
`dag.tables` (`catalog/write_path.rs:314-321`), and `allocate_table_id` asserts the same bound
(`catalog/registry.rs:197-206`) — so no base-table id can ever land in the sub-band.

**The existing malformed-frame guard is kept, and runs *after* the remap.** `run_transient` today rejects
any `ScanDelta` source in the transient band:

```rust
// executor.rs:2201-2210 — "an id there could alias a CONCURRENT ad-hoc query's in-flight transient
// (drives overlap on the `drive_rwlock` read side), silently driving its half-built store into this
// result. Same trust-boundary posture as the `node_id_i32` gate and the `precheck_family` band guard:
// reject, never interpret."
if let Some(s) = sources.iter().find(|&&s| s >= TRANSIENT_ID_BASE) { return Err(...); }
```
Its rationale survives verbatim: every **in-bundle** id is rewritten to this bundle's own tid before the
check, so any source still in the band is exactly the malformed/hostile case — a reference to some other
query's transient, or to a sub-band slot this bundle never minted. Ordering matters and is load-bearing:
remap first, then guard the survivors. Guarding first would reject every legal chain.

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
- **Id-band regression** (the §3a bug, and the reason it must be a test rather than an argument): a chain
  whose **base table's tid collides with a provisional slot under the old scheme** returns correct rows —
  concretely, create ≥ 17 user tables so a real tid lands in `[16, 16+MAX_CHAIN_SEGMENTS)`, then run a
  multi-segment chain over that table. Under `next_local_id: Some(1)` the remap rewrites its `ScanDelta`
  onto a segment and the query silently returns the wrong relation's rows; under `CHAIN_LOCAL_ID_BASE` it
  is correct. Assert **weights**, not just row presence.
- **Trust boundary** (`gnitz-engine`): a hand-built frame whose `ScanDelta` names a sub-band slot the bundle
  never minted (`CHAIN_LOCAL_ID_BASE + N`, for a bundle of `N` segments) is rejected as malformed, not
  interpreted; one naming a live *other* transient's tid is likewise rejected (the `executor.rs:2201-2210`
  guard, post-remap). A bundle of `MAX_CHAIN_SEGMENTS + 1` segments is rejected before any id is minted.
- **Frame layout** (`gnitz-wire` / `gnitz-core`): a 1-segment `FLAG_RUN_TRANSIENT` frame round-trips
  encode → decode to one segment with the same 4 blocks in `CIRCUIT_FAMILIES` order (§3b's `N == 1`
  degeneracy); an `N`-segment frame decodes to `N` segments with each group's
  `(nodes, edges, node_columns, out_schema)` matched by position; a frame whose block count is not a
  multiple of 4, or whose `count / 4` exceeds `MAX_CHAIN_SEGMENTS`, is rejected.
- **Allocator** (`gnitz-engine`): `alloc_transient_id`'s first id is `TRANSIENT_ID_BASE + MAX_CHAIN_SEGMENTS`,
  and no allocated tid ever falls in the bundle-local sub-band.
- **E2E** (`GNITZ_WORKERS=4`): each multi-segment shape is **weight-correct at W=4** (guards the per-segment
  shuffle + the upstream-output read across segments); a chain runs concurrently with pushes to an unrelated
  table and a forced checkpoint mid-drive without stalling ingestion or leaking (the non-blocking property,
  extended to N segments); teardown after an N-segment chain leaves `tables`/`cache`/`meta` empty and removes
  all N scratch dirs; a fail-stop mid-chain leaves no scratch after the next boot.
- **`make verify`** then **`make e2e WORKERS=4`** green.
