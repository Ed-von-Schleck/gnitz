# Equi-join reindex payload pruning: stop persisting dead columns in join traces

> Line numbers below are approximate and drift; anchor on symbol names. All
> citations are `crates/gnitz-*` relative. Scope is **equi joins only** — see
> "Deliberately out of scope".

## Problem

Every equi-join side is reindexed onto the join key by a MAP whose expression
program **copies every source column into the payload** (`build_reindex_program`
in `gnitz-sql/src/plan/view/predicates.rs` — "Build a reindex ExprProgram that
copies all columns as payload"), and the engine derives the reindex output
schema structurally as `[key slots ‖ ALL input columns]` (`reindex_output_schema`
in `gnitz-engine/src/query/compiler/optimize.rs`).

Consequences, per join side:

- The join key is stored **twice** in the trace — once as the OPK-packed PK
  slot, once as its verbatim payload copy — even when no downstream node reads
  the payload copy.
- Columns the view never references still flow through the reindex MAP and the
  per-view `IntegrateTrace` child table (every epoch's ingest → memtable
  consolidation → heap-tier fold → spill → compaction), and are copied into every
  join-output row (`write_join_row`).

**The join-shard *input* scatter is NOT shrunk** (a deliberate limitation): the
master routes the raw source delta by join key *before* the worker-side reindex
runs. `prepare_relay` → `get_join_shard_cols` → `compute_join_shard_cols` →
`reindex_cols_through_filters` walks scan→reindex through **`Filter` nodes only**
and reads `reindex_cols` as indices *into the source schema*; the master then
scatters full-width source rows by those key indices. Pruning the reindex
*output* is downstream of that scatter and cannot shrink it.

The `view_maintenance` bench profile puts the ephemeral write pipeline
(`run_merge_body`/`write_to_batch`/`scatter`) at ~29% of worker userspace time
(reproducible via `make bench-profile`) — but that is the **aggregate** flush
cost across all traces and view stores, not join traces specifically, and roughly
half of it (`run_merge_body`'s heap merge, `find_lower_bound_bytes`' PK-only
binary search, bloom hashing) is per-**row** work that byte-pruning cannot touch.
Honest accounting: on the committed bench the win is the key-duplicate only
(~8 of 48 trace-row bytes, both sides all-`NOT NULL`), which nets to well under
1% end-to-end; the material win is the **wide fact/dim table with a narrow
SELECT** — the canonical incremental-view-maintenance shape — where most payload
columns are dead, and which no committed bench exercises yet (see Perf
validation).

Set-ops and DISTINCT already prune (their `map_hash_row` carries an explicit
projection list). GROUP BY has no reindex MAP (its shard-key analysis
`scan_tid_through_filters` walks `Filter` nodes only, so a pre-shard projection
would break the co-partition skip). This plan targets exactly the **equi** join
reindex sites; nothing else changes.

## Design

`MapKind::Expression` gains an explicit kept-payload-column list. The planner
emits the pruned list for equi-join reindexes; the engine derives the reindex
output schema from it. `op_map` needs **no** change: it packs the key from the
*input* batch (`ReindexPacker`) and writes the payload via
`func.evaluate_map_batch(batch, out_schema)`, so payload width is already driven
by the node's output schema — only the schema derivation changes.

**Why a wire field is necessary.** The wire circuit has no per-node output schema
(`loaded.out_schema` is the *circuit's* single output schema); the engine derives
every intermediate node's schema structurally, and for a reindex node
`reindex_output_schema` assumes *all* input columns. The only way to communicate
a narrower payload is to carry the kept-column list on the node.

**Why not a pre-narrowing projection** (the tempting zero-wire-change
alternative): inserting a `MapKind::Projection` before the reindex would narrow
the input structurally with no engine change — but a projection *renumbers*
columns, and `reindex_cols_through_filters` walks scan→reindex through `Filter`
nodes only, treating `reindex_cols` as source-schema indices for the master's
pre-reindex scatter. A projection would stop that walk (it is not a `Filter`) and
break the reindex-key ↔ source-column correspondence. Pruning the payload while
keeping the reindex *input* equal to the source leaves the master-scatter
machinery untouched.

### Encoding (reuse `NODE_COL_KIND_PROJ`)

The kept-payload list is carried as `NODE_COL_KIND_PROJ` (kind 2) rows on the
`OPCODE_MAP_EXPR` node — the same kind already used by `OPCODE_MAP_PROJ` and
`OPCODE_MAP_HASH_ROW` to mean exactly "ordered list of kept source-column
indices, no promotion" (`value1 = source column index`, `position` = kept order,
no `value2`). No new wire constant; decode reuses the existing `collect_cols`
helper. The `OPCODE_MAP_EXPR` arm already collects kind-6 (REINDEX, the key
columns) rows; it additionally collects kind-2 (payload) rows — disjoint kinds on
one node, exactly as `OPCODE_REDUCE` already collects three disjoint kinds.
Decode stays total (every arm filters by explicit kind; unknown kinds dropped).

`MapKind::Expression` gains `payload_cols: Vec<u16>`.

**Convention (single, committed):** `payload_cols` is meaningful only on a
**reindex** node (non-empty `reindex_cols`). Non-empty = the kept payload
columns, source-schema indices in order. **Empty = all input columns** (the
unpruned identity) — used by `SELECT *` equi joins, any equi side with nothing to
drop, the range/band and EXISTS/IN reindex sites (which this plan leaves
unpruned), and the gnitz-core unit tests; this keeps every unpruned circuit
byte-identical to today. On a compute map (empty `reindex_cols`) `payload_cols`
must be empty; the engine **rejects** a non-empty `payload_cols` there, and
rejects duplicate/out-of-range indices. (Known debt: "empty = all" cannot express
a *zero*-payload reindex, foreclosing a future EXISTS/IN inner-side prune; an
always-explicit list would avoid that but costs a `(0..n).collect()` at every
unpruned site today. Accepted for this plan.)

### Wire / core / engine plumbing

- `gnitz-wire/src/circuit.rs`: add the `payload_cols` field; `decode_op_node`'s
  `OPCODE_MAP_EXPR` arm collects the kind-2 rows.
- `gnitz-core/src/circuit.rs`: `encode_op_node` emits the kind-2 rows;
  `CircuitBuilder::map_reindex` gains `payload_cols: &[usize]`; `map_expr` passes
  none. The wire enum, core encode/decode, and the engine's exhaustive `MapKind`
  destructure (`emit.rs`) change in one commit to keep the workspace compiling.
  `gnitz-capi` exposes no reindex builder, so no C-API change.
- `reindex_output_schema(in_schema, reindex_cols, target_tcs, payload_cols)`
  (`optimize.rs`): empty ⇒ unchanged (`cols[pk_n..] = all input columns`);
  non-empty ⇒ `cols[pk_n + i] = in_schema.columns[payload_cols[i]]`, count assert
  `pk_n + payload_cols.len()`.
- `emit_node`'s `MapKind::Expression` arm (`emit.rs`): validate `payload_cols`
  with the existing `emit_failed` pattern (each `< num_columns()`, no duplicates,
  `pk_n + len() <= MAX_COLUMNS`, empty whenever `reindex_cols` is empty), then
  thread into `reindex_output_schema`. Free hardening: a `debug_assert!` that
  `payload_cols.len()` equals the decoded program's `copy_col` count, catching a
  planner desync in tests. The identity-MAP elision (`sequential_copy_base`)
  already requires `reindex_cols.is_empty()`, so it never fires for a pruned
  reindex.
- `is_join_trace_side`, `reindex_cols_through_filters`, `compute_co_partitioned`,
  `propagate_distinct`: read `reindex_cols` only — no change (verified).

### Planner (`gnitz-sql/src/plan/view/join.rs`, equi path)

**Pruning lives inside `emit_join`, keyed on that function's `projection`
PARAMETER — not the top-level `select.projection`.** This is load-bearing: a
chain's intermediate segment is emitted by the same `emit_join` with a
*synthetic* projection from `live_columns_projection` (later ONs + downstream refs
already folded in), and `plan_join_chain`'s provenance/offset bookkeeping depends
only on the segment's synthetic-PK arity and stored-column offsets — neither of
which the internal reindex-payload width touches. So chains compose for free.
(Keying on the user SELECT instead would prune a column a later ON needs;
`build_join_view_projection` would then fail to resolve — a loud plan-time error,
but still a trap.)

**Live-set computation.** For each side, the kept payload columns are the side's
columns referenced by:

1. the function's `projection` argument — `build_join_view_projection` resolves
   every item to a combined index `idx ∈ [0, left_n + right_n)`; `idx < left_n`
   marks left column `idx`, else right column `idx − left_n`. A `Wildcard` marks
   everything (pruning degenerates to today's layout);
2. the residual ON conjuncts (INNER only) **and the top-level WHERE**
   (`where_filter`/`post_filter`) — both bind against the combined `out_cols`, so
   every column they reference must survive. Collect via
   `resolve_qualified_column`/`resolve_unqualified_column`;
3. **preserved side with a nullable join key: the nullable key columns stay in
   that side's live set.** The ν identity is over the *packed* key plus kept payload, and the
   packed key is not injective across `{NULL, real 0}` (a NULL integer key packs
   to synthetic 0, a NULL string to the empty-content hash 0). NULL-keyed
   preserved rows are filtered out of the match but re-keyed *unfiltered* into
   `p_all`, so a NULL-keyed row (true S = 0) and a 0-keyed row (S from real
   matches) would coarsen to one ν identity if the key's payload copy — whose
   null bit is the disambiguator — is dropped: `positive_part(2 − 2) = 0` silently
   loses the null-fill. Keeping the nullable key columns (with null bits) restores
   the split. **Only the nullable components are needed** — a non-nullable
   component packs injectively; a non-nullable string key hashes, but the join
   *matches* on that same hash, so S is packed-key determined and any
   hash-collision coarsening is consistent with the join and invisible in the
   (key-not-selected) output.

Join keys are otherwise **not** auto-included: they live in the PK slots; a
payload copy exists only if a rule above references them. `SELECT *` marks
everything, so the pruned list is empty (= all) and the circuit is byte-identical
to today.

**Reuse, don't reinvent.** Compute the keep-sets with the existing
`collect_column_refs` (`ast_util.rs`) + `resolve_qualified_column` /
`resolve_unqualified_column` (`bind/resolve.rs`) — the same resolvers
`build_residual_filter_prog` and `build_join_view_projection` already use. Rule 2
is ~6 lines over those. Do **not** add a new `collect_referenced_cols` walker, and
do **not** reuse the chain-liveness `collect_live_from_expr`/`LiveNames` (those
emit per-alias *name* sets for provenance, not per-side *indices*).

**Reindex programs.** `build_reindex_program(&schema, &keep)`: `copy_col(tc,
src_ci, out_pos)` for `(out_pos, src_ci)` in `keep` order. The same
`(keep, program, payload_cols)` triple drives the side's match reindex AND its
`a_all`/`b_all` re-key (`join_type.preserves_left/right`), so `p_all` and
`π_P(inner)` carry byte-identical logical rows and the null-fill cancellation
rides `compare_rows` content comparison exactly as today.

**Renumbering onto the pruned layout — read positions, never source indices.**
After pruning, side widths are `pl = keep_l.len()` / `pr = keep_r.len()` and each
kept column sits at its *pruned position*, not its source index. Every site that
used `left_n`/`right_n` or a source column index must use `pl`/`pr` or the
kept-position map: `normalize_to_ab(pl, pr)`, the `equi_nf` closure
(`p0`/`p_n`/`o_col_tcs`/`preserved_is_right` reorder), `out_cols` (k `_join_pk`
defs, then pruned left defs, then pruned right defs), the residual/WHERE filter's
combined `merged_schema` and its pruned `AliasMap` (`col_offset ∈ {0, pl}`),
`build_join_view_projection` against the pruned alias map, and the column-count
guard `k + pl + pr`. **A mis-renumber is silent when column widths coincide**
(no `pk_stride`/count panic — a wrong-but-present column is read) — hence the
"drop a column *before* a kept column" test below.

**Left unpruned in this plan (pass `payload_cols = &[]`):** the range/band
reindex sites in `emit_range_join` (its trace is ordered/re-keyed by
eq-prefix/range/source-PK, and pruning it requires source-PK retention plus
intricate silent-failure-prone pair-PK renumbering — deferred), and the 8
EXISTS/IN `map_reindex` sites in `exists.rs` (mechanical `&[]` to satisfy the new
signature; no behavior change).

**Why the equi outer-join null-fill stays weight-exact under pruning.** The inner
match is computed from the reindex *key* slot (`_join_pk`, driven by
`reindex_cols`, which pruning never touches), so the inner output is byte-correct.
The only surface is `ν = positive_part(P − π_P(inner))`. `op_weight_clamp`
subtracts *raw* weights and clamps the *result*, so over a coarsened identity
`(κ, ρ)` it emits `max(0, Σ w_P·(1 − S))`, which equals the exact `Σ w_P·[S=0]`
iff no coarsened identity mixes an `S=0` row with an `S≥1` row. Equi ν is keyed by
`_join_pk`, and S is a function of that same key, so S is constant over any
coarsened identity — *except* the NULL/real-0 packing collision, which rule 3
splits back apart. Weight-exact (verified against `op_weight_clamp`,
`copy_col` null-bit propagation, and null-first `compare_rows`).

**What is not pruned:** GROUP BY inputs, set-op/DISTINCT sides (already projected),
range/band and EXISTS/IN reindexes (this plan), and the view's own store (its
schema is the SELECT list already — the final projection MAP before `sink` prunes
the store; this plan prunes what flows and persists *before* that projection).

### Deliberately out of scope (deferred, not dropped-as-wrong)

- **Range/band reindex pruning.** Correct in principle (source-PK retention +
  band-key retention make the null-fill weight-exact), but its pair-PK/`nf_tail`
  renumbering onto pruned spans is silent-failure-prone and no committed bench
  exercises range/band joins at all — the risk/complexity is unjustified until a
  representative workload exists.
- **EXISTS/IN pruning.** The inner subquery side's payload is provably 100% dead,
  but exploiting it wants a zero-payload reindex the "empty = all" convention
  cannot express; the marginal win (EXISTS/IN over a wide subquery) does not
  justify the engine work here. The 8 call sites are updated mechanically only.

## Sequencing

1. **Wire + core + engine destructure** (one commit — the `MapKind` field change
   ripples through all three): `MapKind::Expression.payload_cols`, kind-2
   encode/decode on `OPCODE_MAP_EXPR`, `map_reindex` signature (all non-pruning
   callers — gnitz-core tests, the range/band sites, the 8 exists.rs sites — pass
   `&[]`), engine `emit.rs` destructure threading the field unused. Kind-2
   roundtrip tests; every existing circuit decodes with an empty list.
2. **Engine semantics**: `reindex_output_schema` + `emit_node` validation + unit
   tests (empty ⇒ byte-identical schemas; non-empty ⇒ subset layout; out-of-range
   / duplicate / non-reindex-with-payload ⇒ clean compile failure). Update the ~4
   `reindex_output_schema` call sites in `compiler/mod.rs` tests.
3. **Planner, equi join**: keep-sets (rules 1, 2, 3), pruned schemas/alias-map,
   program + node emission, position renumbering; tests. Behavior-neutral for
   `SELECT *`.

Steps 1-2 are behavior-neutral. Each step compiles and passes `make verify` +
`make e2e`.

## Files touched

| File | Change |
|---|---|
| `crates/gnitz-wire/src/circuit.rs` | `payload_cols` field; kind-2 collect in `OPCODE_MAP_EXPR` decode |
| `crates/gnitz-core/src/circuit.rs` | `map_reindex` signature; `encode_op_node` kind-2 rows |
| `crates/gnitz-engine/src/query/compiler/optimize.rs` | `reindex_output_schema` payload param |
| `crates/gnitz-engine/src/query/compiler/emit.rs` | Expression arm validation + threading + debug assert |
| `crates/gnitz-engine/src/query/compiler/mod.rs` | ~4 `reindex_output_schema` test call sites |
| `crates/gnitz-sql/src/plan/view/predicates.rs` | `build_reindex_program(schema, keep)`; rule-2 keep-set (reusing `collect_column_refs` + `resolve_*`) |
| `crates/gnitz-sql/src/plan/view/join.rs` | equi keep-sets, pruned layouts, position renumbering; range/band `map_reindex` sites pass `&[]` |
| `crates/gnitz-sql/src/plan/view/exists.rs` | 8 `map_reindex` sites pass `&[]` |

**Verified clean** (derive from the post-pruning register schema, no full-width
assumption, no change): `merge_schemas_for_join`, `write_join_row`,
`op_null_extend`, the `IntegrateTrace` child table, manifest/checkpoint schema
persistence, backfill, `get_join_shard_cols`/`compute_join_shard_cols`.

## Testing

- **Wire/engine units**: kind-2 payload roundtrip on `OPCODE_MAP_EXPR`;
  `reindex_output_schema` subset layout; emit rejection of out-of-range /
  duplicate / non-reindex `payload_cols`.
- **Planner circuit-shape tests**: a join selecting a strict column subset emits
  reindex MAPs whose `payload_cols` equal the live sets and whose downstream
  projection indices match the pruned layout; `SELECT *` emits empty lists
  (byte-identical to today); a residual-ON column not in the SELECT is retained; a
  top-level WHERE column not in the SELECT is retained; a selected join-key column
  retains its payload copy; an unreferenced non-nullable join-key column does not;
  a preserved-side nullable join-key column is retained even when unselected.
  **Renumber pin**: a join whose SELECT drops a column positioned *before* a kept
  column (so a missed source-index→pruned-position renumber diverges rather than
  leaving kept columns at position 0).
- **E2E behavior** (`gnitz-py`, `GNITZ_WORKERS=4`): full existing join suite
  passes. New:
  - INNER/LEFT/RIGHT/FULL equi joins whose SELECT omits the join keys and several
    payload columns — results identical to a `SELECT *` view projected
    client-side.
  - **ν-coarsening pin**: preserved side with two rows sharing (key, kept payload)
    but differing in a dropped column; other side empty → one null-filled row at
    weight 2; insert a match → both retract; delete the match → weight-2 null-fill
    returns. W=4.
  - **NULL-vs-real-0 key pin (rule 3)**: LEFT JOIN on a *nullable* integer key,
    SELECT omitting the key; preserved rows `(key=NULL, x=5)` and `(key=0, x=5)`,
    other side matching key 0 → the NULL row null-fills at weight 1 while the 0
    row joins, across churn on both sides. Repeat with a nullable STRING key
    (NULL vs `''`).
  - **3-way equi chain**: a middle segment whose step-local residual-ON references
    a column dropped from the final SELECT (must be retained); a middle LEFT JOIN
    on a nullable key; results identical to the unpruned equivalent.
  - Backfill: create the pruned join view over pre-populated tables; contents
    equal the fresh-data run.
- **Perf validation**: this is where the design must earn its keep. Build a
  wide-fact/narrow-SELECT variant of `test_view_maintenance` (~20 dead fact
  columns) and A/B it against the unpruned code — on the committed narrow schema
  the win is only the key-duplicate and is expected below the ±3-4% `bench-full`
  noise floor, so the wide variant (or an isolated microbench on the
  flush/compact path) is the decisive measurement. Confirm via `perf report` that
  the per-byte flush cluster's share drops.

## Invariants preserved

- Exchange routing, the input scatter, and co-partition analysis are functions of
  `reindex_cols`/promotions only — untouched.
- `p_all` / `π_P(inner)` byte-identity per preserved row (same pruned program on
  both), which the null-fill cancellation requires.
- Empty `payload_cols` ⇒ byte-identical schemas, programs, and stored circuits to
  today; only equi joins with non-wildcard projections change.
- Element identity remains (PK, payload) everywhere; pruning changes *which*
  payload a given intermediate carries, uniformly for every producer and consumer.
  Every downstream schema consumer derives from the pruned register schema.
