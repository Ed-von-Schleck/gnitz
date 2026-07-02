# Join reindex payload pruning: stop persisting dead columns in join traces

## Problem

Every join side is reindexed onto the join key by a MAP whose expression
program **copies every source column into the payload**
(`build_reindex_program`, `gnitz-sql/src/plan/view/predicates.rs:86-93`: "Build
a reindex ExprProgram that copies all columns as payload"), and the engine
derives the reindex output schema as `[key slots ‖ ALL input columns]`
(`reindex_output_schema`, `gnitz-engine/src/query/compiler/optimize.rs:319-352`).

Consequences, per join side:

- The join key is stored **twice** in the trace — once as the OPK-packed PK
  slot, once as its verbatim payload copy — even when no downstream node ever
  reads the payload copy.
- Columns the view never references still flow through the reindex MAP, the
  per-view `IntegrateTrace` child table (every epoch's ingest → memtable
  consolidation → heap-tier fold → spill → compaction), the join-output row
  materialization (`write_join_row` copies every right-payload column), and —
  for range/band joins — the pair-PK output exchange. (The join-shard *input*
  scatter is NOT shrunk: the master routes the raw source delta by join key
  before the worker-side reindex MAP runs —
  `runtime/orchestration/master/dispatch.rs:620` → `get_join_shard_cols` →
  the `ReindexPacker` over the source schema — so full source rows ship
  regardless.)

The `view_maintenance` bench profile puts the ephemeral write pipeline
(`Table::flush → compact_in_memory → run_merge_body/write_to_batch/scatter`)
at ~25-30% of worker cycles; join traces are among its main byte feeders. In
that bench's narrow schema the dead join-key duplicate alone is 8 of 48
trace-row bytes; realistic wide tables carry far more dead payload.

Set-ops and DISTINCT already prune (their `map_hash_row` carries an explicit
projection list); GROUP BY has no reindex MAP at all (its `ExchangeShard`
ships source rows directly and its shard-key analysis walks only `Filter`
nodes — `scan_tid_through_filters`, `query/compiler/load.rs:302-319` — so a
pre-shard projection would break the co-partition skip). This plan therefore
targets exactly the join reindex sites; nothing else changes.

## Design

`MapKind::Expression` gains an explicit payload-column list. Empty means
"all columns" — which is byte-identical to today for every existing circuit
shape (plain compute maps and unpruned reindexes). The planner emits the
pruned list for join reindexes; the engine derives the reindex output schema
from it. `op_map` needs **no** change: the key is packed from the *input*
batch (`ReindexPacker::new(in_schema, reindex_cols, ..)` +
`packer.promote_into(&in_mb, &mut output)`, `ops/linear.rs:136-137`), and the
payload is whatever the expression program writes — the planner's program
already defines the copy set.

### Wire (`gnitz-wire`)

- New node-column kind: `pub const NODE_COL_KIND_PAYLOAD: u64 = 9;` — one row
  per kept payload column, `value1 = source column index`, `position` = kept
  order (`crates/gnitz-wire/src/circuit.rs:79-87` block).
- `MapKind::Expression` gains `payload_cols: Vec<u16>`
  (`circuit.rs:196-200`). Semantics documented on the field: source columns
  copied to payload, in order; **empty = all columns of the input schema, in
  schema order** (the identity with today).
- `decode_op_node`'s `OPCODE_MAP_EXPR` arm collects the kind-9 rows
  (`circuit.rs:323-350`); validation: each index is a plain in-range check at
  the engine trust boundary (below), not here — decode stays total for
  in-range u16s.

**Convention (single, committed):** the wire field `payload_cols` is empty
for plain compute maps (`reindex_cols` empty — their payload is defined by
the program, e.g. the group-by post-map) and, as the engine-side default,
means "all input columns" on a reindex node. The **planner always emits an
explicit list for every reindex node** after this change (full list when
unpruned), so the empty-on-reindex default exists only to keep decode total.
The engine **rejects** (compile failure) a non-empty `payload_cols` on a
node with empty `reindex_cols`, and rejects duplicate indices.

### Core (`gnitz-core`)

- `encode_op_node` (`gnitz-core/src/circuit.rs:129-157` region) emits the
  kind-9 rows, mirroring the `NODE_COL_KIND_REINDEX` encoding; the two
  `MapKind::Expression` constructors (`map_expr` at `circuit.rs:277-281`
  passes `Vec::new()`, `map_reindex` at `circuit.rs:303-307` passes the new
  parameter) are updated together — the wire enum, core encode/decode, and
  the engine's exhaustive `MapKind` destructure (`emit.rs:253-257`) must
  change in one commit to keep the workspace compiling.

`CircuitBuilder::map_reindex` (`circuit.rs:295-310`) gains a
`payload_cols: &[usize]` parameter. The only non-join callers are the
gnitz-core unit tests (`circuit.rs:588, 623, 658`), which pass `&[]`; the
join builders always pass an explicit list. No wrapper method — the
signature changes and every call site is updated. (`gnitz-capi` exposes no
reindex builder — `gnitz_circuit_map` is Projection-only — so no C-API
change exists.)

### Engine compiler (`gnitz-engine/src/query/compiler`)

- `reindex_output_schema(in_schema, reindex_cols, target_tcs, payload_cols)`
  (`optimize.rs:319-352`): when `payload_cols` is empty, behavior is
  unchanged (`cols[pk_n..pk_n+n] = all input columns`); when non-empty, the
  payload region is `payload_cols` in order
  (`cols[pk_n + i] = in_schema.columns[payload_cols[i]]`), and the
  column-count assert uses `pk_n + payload_cols.len()`.
- `emit_node`'s `MapKind::Expression` arm (`emit.rs:253-355`): validate
  `payload_cols` (each `< in_schema.num_columns()`, no duplicates, count
  within `MAX_COLUMNS` after adding `pk_n`, and empty whenever
  `reindex_cols` is empty) with the same `emit_failed` pattern as the
  existing reindex-bounds checks (`emit.rs:295-302`); thread the list into
  `reindex_output_schema`. The identity-MAP elision
  (`sequential_copy_base`, `emit.rs:269-278`) already requires
  `reindex_cols.is_empty()` and a layout match, so it never fires for a
  pruned reindex.
- `is_join_trace_side`, `reindex_cols_through_filters`,
  `compute_co_partitioned`, `propagate_distinct`: all read `reindex_cols`
  only — no change (the shard/scatter key and the distinctness reset are
  functions of the key, not the payload).

### Planner (`gnitz-sql/src/plan/view/join.rs`)

**Live-set computation.** For each side, the kept payload columns are the
side's columns referenced by:

1. the SELECT projection — `build_join_view_projection`
  (`join.rs:1106-1151`) resolves every item to a combined index
  `idx ∈ [0, left_n + right_n)`; `idx < left_n` marks left column `idx`,
  else right column `idx - left_n`. A `Wildcard` marks everything (pruning
  degenerates to today's layout);
2. the residual ON conjuncts (INNER only) — collect column references by
  resolving each residual `Expr`'s identifiers through the same
  `resolve_qualified_column`/`resolve_unqualified_column` calls
  `build_residual_filter_prog` uses (a small
  `collect_referenced_cols(&residual, &alias_map) -> Vec<usize>` walker over
  the conjunct expressions);
3. **range/band joins only**: both sides' source PK columns. The pair-PK
  re-key reads them from the payload (`cb.map_reindex(merged, &pair_pk_cols,
  ..)` at `join.rs:771`), as do the null-fill re-keys (`join.rs:984, 1003`,
  band ν at `join.rs:1026-1057` including `b_all` at 1054) and `nf_tail`'s
  pair-PK re-key (`join.rs:853-875`), which packs the *other* side's PK out
  of the null-extended payload — so the pruned `o_col_tcs` handed to
  `null_extend` must retain O's PK columns and the `nf_projection` math
  (`join.rs:880-892`) renumbers onto the pruned spans. The range builder's
  own column-count guard (`join.rs:599-608`) uses the pruned
  `pair_pk + k + pl + pr`.

Join-key columns are **not** auto-included: they live in the PK slots; the
payload copy exists only if rule 1/2 references them — with one exception:

4. **Preserved side with a nullable join key: the nullable key columns stay
   in that side's live set.** The ν identity is over the *packed* key plus
   the kept payload, and the packed key is not injective across
   `{NULL, real 0}`: a NULL integer key packs to the synthetic 0 and a NULL
   string to the empty-content hash 0 (`join.rs:272-275`; `ColPromoter`
   encodes a NULL cell's zero value bytes like a real 0,
   `ops/reindex.rs:268-302`). NULL-keyed preserved rows are filtered out of
   the match (`join.rs:288-295, 310-317`) but re-keyed *unfiltered* into
   `p_all` (`join.rs:383-392, 399-408`), so a NULL-keyed row (true S = 0)
   and a 0-keyed row (S from real matches) coarsen to one ν identity if the
   key's payload copy — whose null bit is the disambiguator — is dropped:
   `positive_part(2 − 2) = 0` silently loses the null-fill. Keeping the
   nullable key columns in payload restores today's identity split. A
   non-nullable key packs injectively (per-column OPK encode; string keys
   hash, but the join *matches* on that same hash, so S is packed-key
   determined and coarsening stays consistent with match semantics).

**Ordering.** Live sets are computed after `extract_join_predicates` (which
resolves keys against the full schemas) and before any circuit node is
allocated. Each side gets `keep_l: Vec<usize>` / `keep_r: Vec<usize>` (source
columns in schema order, filtered to live) and the inverse position maps.
From there the builder works with *pruned side widths*
`pl = keep_l.len()`, `pr = keep_r.len()` everywhere it used
`left_n`/`right_n` today, and pruned per-side column-def accessors.

**Reindex programs.** `build_reindex_program` gains the kept list
(`build_reindex_program(&schema, &keep)`): `copy_col(tc, src_ci, out_pos)`
for `(out_pos, src_ci)` in `keep` order, with a `debug_assert!(!keep.is_empty())`
— every planner call site passes an explicit list (the full column list when
unpruned). The same `(keep, program, payload_cols)` triple is used for the
side's match reindex AND (outer joins) its `a_all`/`b_all` re-key
(`join.rs:383-392, 399-408`), so `p_all` and `π_P(inner)` carry identical
logical rows — the null-fill cancellation rides `compare_rows` content
comparison exactly as today (German-string heap offsets may differ; content
equality is what consolidation groups on).

**Downstream index arithmetic** — all in `join.rs`, all mechanical
renumbering onto the pruned layout:

- `normalize_to_ab` (`join.rs:71-94`): call with `pl`, `pr`.
- `equi_nf` (`join.rs:354-370`): `p0/p_n` become pruned offsets/widths;
  `o_col_tcs = schema_type_codes` over the pruned other-side defs; the
  `preserved_is_right` reorder uses `pl`/`pr`.
- `out_cols` (`join.rs:438-467`): the k `_join_pk` defs, then the *pruned*
  left defs, then the *pruned* right defs (nullability adjustment per
  preserved side unchanged).
- Residual filter (`join.rs:475-484`): `build_residual_filter_prog` binds
  against the pruned `merged_schema`; the binding resolves through a pruned
  `AliasMap` whose `ResolvedRelation`s carry pruned per-side schemas with
  `col_offset ∈ {0, pl}`. (Residual columns are in the live set by rule 2,
  so every reference resolves.)
- `build_join_view_projection` runs against the pruned alias map as well;
  its output `final_projection` indices are pruned-combined indices.
- Column-count guard (`join.rs:252-259`): `k + pl + pr`.
- The range builder (`join.rs:559-1090`) gets the same treatment at its
  sites: side reindexes (`join.rs:699-709`), the `union_schema`/re-key
  (`join.rs:771`), the null-fill branches (`join.rs:830-1050`), with rule 3
  keeping both PK sets live.

**Why the outer-join null-fill stays weight-exact under pruning.** The ν
identity coarsens from (packed key, full payload) to (packed key, kept
payload). For any coarsened identity `x'` aggregating preserved rows that
share the packed key, the other-side match count `S` is a function of the
packed key alone — *given rule 4*, which keeps nullable key columns (and
their null bits) in the kept payload so a NULL-keyed row never coarsens
together with a real-0-keyed row. Then
`π_P(inner)(x') = Σ_{a∈x'} w_a·S = w_P(x')·S` and
`positive_part(w_P(x') − w_P(x')·S) = w_P(x')·[S=0]` — the same closed form
CLAUDE.md derives for the uncoarsened identity. Rows collapsing under
coarsening all null-fill together or all match together; no partial leakage
is possible. (Dedicated tests pin both the collapse case and the
NULL-vs-real-0 case below.)

**What is deliberately not pruned:** GROUP BY inputs (no reindex node; the
shard-key walk is Filter-transparent only), set-op/DISTINCT sides (already
projected via `map_hash_row`), the range-join threshold `m` pipeline's
2-column schemas, and the view's own store (its schema is the SELECT list
already — the final projection MAP before `sink` prunes the store today;
this plan prunes what flows and persists *before* that projection).

## Sequencing

1. **Wire + core + engine destructure** (one commit — the `MapKind` enum
   change ripples through all three): `NODE_COL_KIND_PAYLOAD`,
   `MapKind::Expression.payload_cols`, core `encode_op_node`/`map_reindex`
   signature (tests pass `&[]`), engine `emit.rs` destructure threading the
   field unused. Roundtrip tests; all existing circuits decode with an empty
   list (identical semantics).
2. **Engine semantics**: `reindex_output_schema` + `emit_node` validation +
   unit tests (empty list ⇒ byte-identical schemas to today; non-empty ⇒
   subset layout; out-of-range / duplicate / non-reindex-with-payload ⇒
   clean compile failure).
4. **Planner, equi join**: live sets, pruned schemas/alias-map, program +
   node emission, index renumbering; tests.
5. **Planner, range/band join**: same, with rule-3 PK retention; tests.

Each step compiles and passes `make verify` + `make e2e` on its own (steps
1-3 are behavior-neutral).

## Files touched

| File | Change |
|---|---|
| `crates/gnitz-wire/src/circuit.rs` | kind const, `payload_cols` field, encode/decode |
| `crates/gnitz-core/src/circuit.rs` | `map_reindex` signature; `encode_op_node` kind-9 rows |
| `crates/gnitz-engine/src/query/compiler/optimize.rs` | `reindex_output_schema` payload param |
| `crates/gnitz-engine/src/query/compiler/emit.rs` | Expression arm validation + threading |
| `crates/gnitz-sql/src/plan/view/predicates.rs` | `build_reindex_program(schema, keep)`; `collect_referenced_cols` |
| `crates/gnitz-sql/src/plan/view/join.rs` | live sets, pruned layouts, renumbering (equi + range) |

## Testing

- **Wire/engine units**: kind-9 roundtrip; `reindex_output_schema` subset
  layout; emit rejection of out-of-range `payload_cols`.
- **Planner circuit-shape tests** (existing gnitz-sql test style — build the
  circuit, inspect nodes): a join selecting a strict column subset emits
  reindex MAPs whose `payload_cols` equal the live sets and whose downstream
  projection indices match the pruned layout; `SELECT *` emits empty lists
  (byte-identical circuit to today); a residual-ON column not in the SELECT
  is retained; a selected join-key column retains its payload copy; an
  unreferenced join-key column does not.
- **E2E behavior** (`crates/gnitz-py`, `GNITZ_WORKERS=4`): the full existing
  join suite must pass unchanged. New cases:
  - INNER/LEFT/RIGHT/FULL joins whose SELECT omits the join keys and several
    payload columns — results identical to a `SELECT *` view projected
    client-side.
  - **ν-coarsening pin**: preserved side with two rows sharing (key, kept
    payload) but differing in a dropped column; other side empty → one
    null-filled row at weight 2; then insert a match → both retract (net 0
    null-fill), inner rows appear; then delete the match → weight-2
    null-fill returns. Run at W=4.
  - **NULL-vs-real-0 key pin (rule 4)**: LEFT JOIN on a *nullable* integer
    key, SELECT omitting the key; preserved rows `(key=NULL, x=5)` and
    `(key=0, x=5)`, other side holding matches for key 0. The NULL-keyed row
    must null-fill at weight 1 while the 0-keyed row joins — across
    insert/retract churn on both sides. Repeat with a nullable STRING key
    (NULL vs `''`).
  - Band join and pure-range LEFT join with pruned SELECTs (pair-PK re-key
    correctness under pruning).
  - Backfill: create the pruned join view over pre-populated tables; contents
    equal the fresh-data run.
- **Perf**: interleaved A/B `make bench-full` on `test_view_maintenance`;
  confirm via `perf report` that the flush/compaction cluster's share drops.
  For a wide-schema signal, a one-off variant of the bench with ~20 dead
  columns on the fact table (not committed; measurement only).

## Invariants preserved

- Exchange routing and co-partition analysis are functions of
  `reindex_cols`/promotions only — untouched, so delta scatter and trace
  keying still co-partition byte-for-byte.
- `p_all` / `π_P(inner)` byte-identity per preserved row (same pruned program
  on both), which the null-fill cancellation requires.
- Empty `payload_cols` ⇒ byte-identical schemas, programs, and stored
  circuits to today; only join views with non-wildcard projections change.
- Element identity remains (PK, payload) everywhere; pruning changes *which*
  payload a given intermediate carries, uniformly for every producer and
  consumer of that intermediate.
