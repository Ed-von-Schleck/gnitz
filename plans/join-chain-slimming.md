# Slim hidden join segments: WHERE pushdown, then dead-column pruning

Hidden join segments — the intermediates of an N-way chain and the hidden view
`H` under GROUP BY / DISTINCT — materialize more than any consumer reads, in
both dimensions. Part 1 cuts the **rows** (the WHERE runs inside the join
circuit instead of over `H`'s stored output); Part 2 cuts the **columns**
(segments project only what downstream references). Part 1 lands first: it
fixes where the join WHERE binds, which Part 2's liveness collection then
relies on.

## Problem

- **Rows.** `compile_join_to_hidden`
  (`crates/gnitz-sql/src/plan/view/dispatch.rs:608-626`) strips the WHERE
  from `H` (`join_select.selection = None`, `dispatch.rs:618`); the outer
  operator re-applies it over `H`'s output (`group_by.rs:373-379`;
  `set_op.rs:65-71` via `emit_distinct_pieces`). So `H` materializes and
  forever-maintains the **unfiltered** join. A plain join view
  (`ViewShape::Join`) already pushes the WHERE into the circuit
  (`classify_join_where`, `join.rs:157-180`): INNER folds it into the ON
  residual, OUTER-equi filters post-null-fill, OUTER-range is rejected
  (`join.rs:171-176`). Only the grouped/DISTINCT shapes lose the pushdown.
- **Columns.** Intermediate segments project **all** accumulated real columns
  (`all_real_columns_projection`, `join.rs:202-223`; emitted per non-last
  step at `join.rs:358-373`), and `H` is built with a wildcard projection
  (`dispatch.rs:617`) — regardless of what later ONs, the final projection,
  or the outer operator reference.
- **What that width/cardinality costs, engine-side.** A segment's projected
  rows are stored twice: its own ephemeral per-view store (registered at
  `crates/gnitz-engine/src/catalog/hooks.rs:472-479`, schema = the projected
  columns, `hooks.rs:440`; populated per epoch via `ingest_by_ref`,
  `query/dag/mod.rs:1224` — a segment always has a downstream consumer), and
  the consumer's state over it — for a downstream join step, the
  `IntegrateTrace` child table (`query/compiler/emit.rs:530-550`) whose width
  follows `build_reindex_program`'s copy-all over the segment's schema
  (`plan/view/predicates.rs:86-93`). For a 1%-selective WHERE that is ~100×
  the surviving rows; for a 4-way join of 20-column tables projecting 2, it
  is ~60-column rows through segment 2 — both forever.
- **Secondary asymmetries fixed by Part 1.** (a) OUTER-range + WHERE works
  today under GROUP BY/DISTINCT (outer-op filter over `H`) but is rejected as
  a plain join view — verified live: `SELECT region, COUNT(*) FROM a LEFT
  JOIN b ON a.k=b.k AND a.lo<b.hi WHERE amt>40 GROUP BY region` returns
  `[(500,1),(None,1)]`, while the same WHERE in a plain LEFT-range view
  errors (`test_where_on_outer_range_join_rejected`,
  `crates/gnitz-py/tests/test_join_where.py:236-250`). (b) The outer-op path
  binds the WHERE by bare name over `H` (`bind_single_table` →
  `find_unique_column`, qualifiers informational — `bind/structural.rs:214-225`,
  `bind/resolve.rs:52-65`), so a WHERE naming a column both sides carry
  errors as ambiguous *even when qualified*; the join-circuit path binds
  through the join `AliasMap` with true qualified resolution.
- **Width also burns the schema cap.** `MAX_COLUMNS = 65`
  (`gnitz-wire/src/catalog.rs:151`) is guarded on the pre-projection merged
  width `k + left + right` (`reject_column_overflow`, `join.rs:461-464`,
  `:801`). Pruning narrows the *accumulator* (the left input of every step
  after the first), so ≥3-way chains of wide tables that overflow today plan
  after Part 2. (A 2-way join of two >32-column tables still overflows — its
  guard sees two full source schemas; that is out of reach of projection
  pruning by construction.)

## Part 1 — the WHERE always runs inside the join circuit

One uniform rule: a top-level WHERE over a join is consumed inside the join
circuit — INNER folds it into the residual; every OUTER form applies it as
one linear filter over the full-width post-null-fill output, before the user
projection. The outer operator never sees a WHERE when its input is a join.
The filter is linear (`Q^Δ = Q`), so placement changes what `H` stores, never
the result.

### 1.1 `compile_join_to_hidden` keeps the WHERE

`dispatch.rs:618`: stop clearing `join_select.selection`. The clone still
blanks `group_by`, `having`, `distinct` (they belong to the outer operator
and must never enter the join). `plan_join_chain`'s last step then feeds
`select.selection` to `classify_join_where` (call at `join.rs:327-336`)
exactly as the plain-join shape does.

### 1.2 The outer operator receives a Select without the WHERE

`resolve_operator_input` (`dispatch.rs:512-529`) returns
`(source, Cow<'a, Select>)`: plain-table path `Cow::Borrowed(select)` (WHERE
handling in the operator unchanged — a filter over one base relation is
already minimal); join path `Cow::Owned` clone with `selection = None`. The
three call sites — the `ViewShape::Distinct`/`GroupBy` arms
(`dispatch.rs:73, 77`) and `compile_hidden_body`'s grouped branch
(`dispatch.rs:494`) — pass `&returned_select` to `emit_group_by_pieces` /
`emit_distinct_pieces`; `&Cow<Select>` deref-coerces, so no emitter signature
changes, and the Cow is a local that drops after the emit call (no lifetime
escape, including inside `add_segment`'s closure). An owned rewritten
`Select`, not a flag, because the operator reads projection/group_by/having
from the same value.

### 1.3 `classify_join_where` accepts WHERE on outer range joins

Delete the `has_range` rejection arm (`join.rs:171-176`) and the parameter;
the OUTER path returns the conjuncts as `where_filter` regardless of
range-ness. `emit_join`'s `debug_assert!(where_filter.is_empty())` before the
range dispatch (`join.rs:429-432`) is removed; `where_filter` becomes a real
`&[Expr]` parameter of `emit_range_join`. Stale prose updated in the same
commit: `LoweredJoin::where_filter`'s doc (`join.rs:116-119` "rejected for
OUTER range joins"), the `emit_join` range-branch comment (`join.rs:427-428`),
`test_join_where.py`'s module docstring (`:1-8`), and the three `dispatch.rs`
comment blocks describing the old model — the dispatch-arm comment
("resolving its projection / WHERE / group columns against H by name",
`dispatch.rs:68-71`) and `compile_join_to_hidden`'s doc + body comment
("full-column view H … WHERE … stripped for H", `dispatch.rs:603-607`,
`:614-616`).

Semantics: the WHERE is a linear 3VL filter over the post-null-fill Z-set —
a null-filled column failing the predicate drops its row, `IS NULL` keeps
unmatched rows — identical to what the outer-op filter over `H` computes
today, and a linear filter commutes with the downstream reduce/distinct (SQL
evaluates WHERE before GROUP BY; DISTINCT stays filter-then-distinct).
HAVING is untouched: it stays in the outer operator (`group_by.rs:614-635`).

### 1.4 `emit_range_join`: outer output tail filters at full width

Today the inner pairs and each null-fill branch project to the **user**
columns before the union (`projected = cb.map(rekey, &final_projection)`,
`join.rs:1022`; `nf_tail`'s final map, `join.rs:1072-1085`), so no post-union
node can see a non-projected column. The committed outer tail — INNER keeps
today's single-map shape, its `where_filter` being empty by construction
(INNER folds into `residual`, which the range path already filters at
`join.rs:948-953`):

```
rekey            [pair-PK | _join_pk × k, A, B]           (join.rs:963, unchanged)
inner_full  = map(rekey, [payload_offset .. payload_offset + left_n + right_n])
                 → [pair-PK | A, B]     (drops the per-term _join_pk slots,
                                         which must never survive the union)
nf_full     = per ν branch, nf_tail with its final map generalized to ALL
              combined columns (the nf_projection math, join.rs:1072-1085,
              over every combined index: A col ci → pair_pk + pa + ci when
              P=A; when P=B, A col ci → pair_pk + pb + right_n + ci and
              B col ci → pair_pk + pb + (ci − left_n) — the same P=B
              canonicalizing reorder, now full-width)
unioned     = union(inner_full, nf_full…)   (accumulator-on-PORT_IN_B pattern
                                             as today; inner_full is fresh and
                                             single-consumer)
filtered    = where_filter.is_empty() ? unioned
            : filter(unioned, build_residual_filter_prog(&where_filter,
                  alias_map, &combined_schema, pair_pk)?)
sink_input  = map(filtered, second_stage)    then cb.shard (join.rs:1195)
```

- `combined_schema` = `pair_pk_coldefs` (`join.rs:993-1006`) followed by
  `combined_coldef(0..left_n+right_n)` (`join.rs:974-988` — carries the
  preserved-side nullability the 3VL compile needs); `pk_cols = 0..pair_pk`;
  base offset `pair_pk` (the `JoinResidual` leaf resolves a combined index
  `ci` to slot `base + ci`, mirroring the equi call at `join.rs:680-690`).
- `second_stage` is the affine remap of the existing `final_projection`
  (built against the `rekey` layout, whose payload offset is
  `payload_offset = pair_pk + k`, `join.rs:969`): index `< pair_pk` stays;
  index `i ≥ payload_offset` becomes `pair_pk + (i − payload_offset)`.
  `build_join_view_projection` is unchanged. All index arithmetic here is
  expressed in terms of the layout constants (`pair_pk`, `k`,
  `payload_offset`, side widths), never absolute offsets.
- One committed circuit shape for every OUTER range/band join, `where_filter`
  present or not (the extra full-width map bytes are per-epoch, in-worker,
  pre-exchange — noise). Pure-range (`n_eq == 0`, LEFT only) rides the same
  tail via its `nf_keyed` branch (`join.rs:1088-1133`), NULL-range-key rows
  included; pure-range RIGHT/FULL stays rejected upstream.

The equi path needs no circuit change: `where_filter` already lands as the
single post-null-fill filter over `merged` (`join.rs:662-690`).

### 1.5 Capability changes

- `WHERE` on a plain LEFT/RIGHT/FULL **range/band** join view: rejected →
  supported (post-null-fill filter). `test_where_on_outer_range_join_rejected`
  flips to a positive test.
- `WHERE a.id = k` over a grouped/DISTINCT join where both sides carry `id`:
  "ambiguous column" error → resolves (qualified binding through the alias
  map). Unqualified dup names still error as ambiguous on both paths.
- One deliberate narrowing: a WHERE naming a **synthetic** column
  (`_join_pk*` / `_pair_pk_*`) binds by name over `H` today and errors after
  pushdown (the alias map carries only real relations). Synthetic PK names
  are internal layout, not a supported query surface; no test or documented
  behavior relies on them.
- No other result changes: INNER folding is already the plain-join behavior,
  and the OUTER filter computes inside `H` the same Z-set the outer operator
  computed over `H`.

## Part 2 — segments project only live columns

### 2.1 Liveness

A column of original relation alias `a` is **live at segment `h_i`** iff it
is referenced by (1) any later join step's ON clause
(`extract_join_predicates` resolves each step's ON through the provenance
alias map, `join.rs:302-323`), or (2) the final consumer: the final
projection and the top-level WHERE conjuncts — both of which, after Part 1,
are the chain's own clauses for every entry point (`plan_join_chain` sees
`H`'s projection as its final projection, §2.3, and the WHERE via
`classify_join_where`).

**Reference pre-pass.** Before the emit loop, `plan_join_chain` walks each
step's ON plus the final projection and WHERE once, collecting
`(alias, column)` pairs: `CompoundIdentifier(q, c)` resolves `q` against the
aliases present at that step (aliases enter left-deep, so step `j` sees
aliases `0..=j`); a bare `Identifier(c)` resolves to the unique alias
carrying `c` among them (the `resolve_unqualified_column` rule,
`bind/resolve.rs:86-102`) — an ambiguous bare name is recorded against every
candidate alias (a safe over-approximation; real binding errors it later
regardless). Sub-expressions walk via `expr_operands` (`ast_util.rs:81-103`),
whose node set mirrors the binder's — an expression node it omits is one the
binder rejects, so under-collection cannot produce a wrong result, only the
same error. A wildcard projection item marks every column of every alias
live. The pre-pass resolves names against the unpruned base schemas (the
provenance carries them before any pruning), so it needs no schema surgery.
Per segment `i`, the live set is the union over steps `j > i` plus the final
consumer. Original relations themselves are never pruned — pruning starts at
the first emitted segment's projection.

**Why pruning cannot corrupt the outer null-fill.** The ν subtraction
`positive_part(P − π_P(inner))` is exact iff rows sharing one identity share
one match count `S`. The identity is (reindex PK region, kept payload
including the null word) — `op_weight_clamp` co-groups on PK and sub-merges
on the full payload (`crates/gnitz-engine/src/ops/distinct.rs:59-85`) — and
`S` is a function of the step's join/range key columns **alone**: an outer
step carries no residual (`emit_join` rejects non-INNER + residual,
`join.rs:409-417`), and the band/threshold match tests read only the reindex
key slots. Every ON-referenced column is live by rule (1), and the key's
null bit rides in the kept payload (so a NULL key and a real 0 never
conflate). Two rows that coarsen onto one pruned identity therefore agree on
every S-determining column, share `S`, and the clamp stays weight-exact by
the same bag argument as the unpruned form —
`positive_part(Σw − Σw·S) = Σw·[S=0]`. Both sides of the subtraction see the
identically-pruned rows (`p_all` re-keys the segment's own delta,
`input_delta_tagged(acc_tid)`; `π_P(inner)` projects the join over the same
input). Steps preserving only the right side re-key `input_b_raw` — an
original, never-pruned relation. The final user-visible view's
(PK, payload) coarsening under a narrow projection exists today and is
correct Z-set weight summing; pruning intermediates adds nothing new there.

### 2.2 Pruned provenance

`all_real_columns_projection` (`join.rs:202-223`) takes the per-alias live
sets and emits, per alias in provenance order, only the live columns (schema
order). Its provenance entry carries a **pruned per-alias `Rc<Schema>`** —
the kept `ColumnDef`s verbatim, `pk_cols` filtered to kept positions (and
possibly empty when an alias's PK is dead) purely to keep the struct
well-formed (resolution reads only `.columns`;
`resolve_qualified_column`/`resolve_unqualified_column`,
`bind/resolve.rs:71-102` — the *segment's* PK region always comes from the
emitted segment schema, never from a provenance entry) — and the
payload-relative offset within the pruned accumulator. The same single loop
builds projection and provenance, as today, so order and offsets cannot
drift. Downstream stays untouched: later ONs / final projection / WHERE
resolve through the alias map built from the pruned per-alias schemas
(name → position within the pruned schema → pruned `col_offset`; every
referenced column is live by construction, so resolution cannot miss);
`add_segment` returns the emitted (pruned) accumulator schema
(`join.rs:360-371`); `reject_column_overflow` sees pruned accumulator widths
automatically; nullability flags are never edited (the outer-join adjustment
happens downstream in `out_cols` / `combined_coldef` as today); the
self-join pass-through wrapper (`join.rs:283-298`) stays an identity copy of
its base relation (a source, pruned at the next segment like any alias).
Keeping full per-alias schemas and pruning only the physical projection is
rejected: name → position → offset would disagree with the physical layout.

### 2.3 `H`'s projection under GROUP BY / DISTINCT

`compile_join_to_hidden` sets `join_select.projection` to the outer
operator's referenced **names** instead of the wildcard
(`dispatch.rs:617`): one `SelectItem::UnnamedExpr(Identifier(name))` per
distinct name in the clauses the operator evaluates over `H` — GROUP BY
expressions (`group_by.rs:194-208`), aggregate arguments
(`group_by.rs:242-252`, `extract_func_arg_col` `group_by.rs:1013-1030`),
HAVING references, the DISTINCT projection items — collected with the §2.1
walker in **bare-name mode**: every reference, qualified or not, marks its
bare name live across *every* alias carrying it. Each collection mirrors the
binding discipline of its consumer — the operator binds over `H` by bare
name with informational qualifiers (`bind_single_table` →
`find_unique_column`; `single_relation_col_name` discards the qualifier,
`ast_util.rs:176-183`), so collecting `SUM(a.amt)` as qualified-to-`a` would
prune a same-named `b.amt` and turn today's deterministic "ambiguous
column" error into an acceptance that depends on what else happens to be
live — an optimization changing semantics. Bare-name marking keeps every
candidate in `H`, so ambiguity behavior is byte-identical before and after,
independent of liveness. (The §2.1 chain pre-pass keeps true qualified
resolution — its consumers, the ON/projection/WHERE binders, resolve through
the alias map.) The WHERE is not collected: after Part 1 it lives in
`join_select.selection` and enters the §2.1 pre-pass as the chain's own
final WHERE. A wildcard DISTINCT projection keeps the wildcard; an **empty**
collected set (e.g. `SELECT COUNT(*) FROM a JOIN b ON …`) also keeps the
wildcard — the degenerate no-pruning case, rather than an untested
zero-payload view.
`plan_join_chain` then sees those identifiers as its final projection, so
one mechanism serves both entry points.

### 2.4 Degeneration

A `SELECT *` final (or wildcard DISTINCT) marks everything live: layouts,
circuits, and results are byte-identical to today, and plain 2-way joins
emit no intermediate segment at all (`join.rs:353-354`), so the existing
join suite is untouched. `test_star_projection_drops_intermediate_join_pk`
(`crates/gnitz-py/tests/test_multiway_join.py:245-266`) must keep passing
unchanged.

## Files touched

- `crates/gnitz-sql/src/plan/view/join.rs` — `classify_join_where` two-case
  rule; `emit_range_join` `where_filter` + full-width outer tail (Part 1);
  reference pre-pass, live sets, live-set-driven projection with pruned
  provenance (Part 2).
- `crates/gnitz-sql/src/plan/view/dispatch.rs` — keep `selection` in
  `join_select`; `resolve_operator_input` returns `(source, Cow<Select>)`
  (Part 1); `H` projection from operator names (Part 2).
- `crates/gnitz-sql/src/ast_util.rs` — the `(alias, column)` reference
  walker (Part 2).
- `crates/gnitz-py/tests/` (`test_join_where.py`, `test_groupby_chain.py`,
  `test_set_ops.py`, `test_multiway_join.py`, `test_self_join.py`) and
  `crates/gnitz-sql/tests/planner_join.rs` — below.

## Tests

Part 1 (Python E2E, `GNITZ_WORKERS=4`, unless noted):

1. Filtered grouped INNER join, incremental: insert rows below/above the
   WHERE threshold, then retract a contributing row — per-group aggregates
   track (extends `test_group_by_over_join_with_where`,
   `test_groupby_chain.py:209-231`, today a static check). Same for DISTINCT
   over an INNER join with a WHERE.
2. OUTER-equi grouped join, null-fill rows against the predicate: a
   preserved-side WHERE keeps the null-filled group, a right-side WHERE
   drops it, `WHERE right_col IS NULL` selects exactly the unmatched rows —
   under insert/retract churn on both sides.
3. OUTER band **and** pure-range LEFT join + WHERE + GROUP BY / DISTINCT:
   the `[(500,1),(None,1)]`-style result under updates and retractions.
4. Flip `test_where_on_outer_range_join_rejected` into a positive test: a
   plain `LEFT JOIN … ON a.x < b.y WHERE …` view creates and maintains
   correctly, including a null-fill row passing/failing the WHERE.
5. Backfill parity: the filtered grouped-join view over pre-populated tables
   equals the fresh-insert run.
6. Qualified dup-name WHERE (`WHERE a.id = k`, both sides carry `id`) over a
   grouped join resolves and filters correctly.
7. Rust (`planner_join.rs`, integration): an opcode-shape pin for the
   restructured outer range tail — full-width union, exactly one filter node
   when a WHERE is present (none when absent), a single projection map before
   the shard.

Part 2 (Python E2E unless noted):

8. Multiway INNER join where a later ON references a column absent from the
   final projection — retained through the intermediate; results equal a
   `SELECT *` view projected client-side (extends
   `test_second_on_references_first_table`, `test_multiway_join.py:205-222`).
9. 4-way join of wide tables with a 2-column final SELECT — correctness
   under insert/update/delete; plus a shape whose **final-step** overflow
   check clears only when pruned: with 4 tables × 18 columns and `k = 1`,
   the unpruned checks run 37 → 56 → 74 (`1 + 55 + 18` at the final step,
   the accumulator `h_1` being 55 columns), so the view is rejected today
   (74 > 65) and plans once `h_1` is pruned below 46 columns.
10. Self-join chain (repeated relation via the pass-through wrapper) with a
    narrow final projection.
11. LEFT and FULL steps mid-chain with pruning: null-fill rows correct under
    retraction, including rows that agree on every kept non-key column.
12. GROUP BY / DISTINCT over a join with `H` pruned to the operator's names —
    correctness under churn and backfill parity over pre-populated tables;
    MIN/MAX over a joined value column (agg-argument retention);
    `SELECT COUNT(*) FROM a JOIN b ON …` (empty name set → wildcard `H`);
    a qualified agg argument (`SUM(a.amt)`) where both sides carry `amt`
    still errors as ambiguous (bare-name collection pin).
13. Wildcard degeneration: `SELECT *` multiway join and wildcard DISTINCT
    result-identical to today; `test_star_projection_drops_intermediate_join_pk`
    unchanged.
14. Backfill parity for a pruned plain multiway view over pre-populated
    tables.
15. Rust (`planner_join.rs`-style, integration): create a chained view via
    `SqlPlanner`, resolve the hidden segment by
    `gnitz_core::hidden_view_name(final_vid, 0)` through
    `resolve_table_or_view_id`, and assert its registered columns equal the
    live set — a later-ON-only column present, a dead column absent.

## Sequencing

- [ ] 1. Part 1, range tail: `classify_join_where` relaxation,
  `emit_range_join` `where_filter` + full-width outer tail, stale-comment
  updates, tests 4 and 7; `make e2e` green (W=4).
- [ ] 2. Part 1, dispatch: keep WHERE in `join_select`, `Cow<Select>` to the
  outer operator, tests 1–3, 5–6; `make e2e` green.
- [ ] 3. Part 2, chain: reference walker + pre-pass + pruned
  provenance/projection for `ViewShape::Join` chains, tests 8–11, 14, 15;
  `make e2e` green.
- [ ] 4. Part 2, `H`: operator-name projection in `compile_join_to_hidden`,
  tests 12–13; `make e2e` green.
