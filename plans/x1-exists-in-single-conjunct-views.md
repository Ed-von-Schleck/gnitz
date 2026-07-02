# x1: EXISTS / NOT EXISTS / IN / NOT IN — single-conjunct view support + IN-list surface

## Problem

GnitzDB has no SQL surface for existence checks. `Expr::InList`, `Expr::InSubquery`,
`Expr::Exists`, `Expr::AnyOp/AllOp` all fall through `bind_structural`'s catch-all
(`gnitz-sql/src/bind/structural.rs:98-100`) in every binding context. The only working
form is the `DELETE ... WHERE pk IN (literals)` multi-seek fast path
(`try_extract_pk_in`, `gnitz-sql/src/dml/plan.rs:188-228`, consumed only by
`execute_delete`, `gnitz-sql/src/dml/mutate.rs:329-350`) — `UPDATE` and direct `SELECT`
lack even that.

Meanwhile the engine already contains everything a semi/anti-join needs: the outer-join
null-fill `ν = positive_part(P − π_P(inner))` is a weight-exact anti-join
(`positive_diff`, `gnitz-core/src/circuit.rs:379-383`; equi construction
`gnitz-sql/src/plan/view/join.rs:354-419`, band `join.rs:1012-1064`, pure-range
threshold `join.rs:896-1009`). Dedicated semi/anti opcodes were deliberately retired in
favor of this composition (retired wire opcodes 16–19, `gnitz-wire/src/circuit.rs:23-26`).
This plan adds the SQL surface for the single-subquery-conjunct case as circuit
derivations of the shipped null-fill machinery — no new engine operators, no new wire
opcodes.

**Load-bearing assumptions about the join machinery** (re-verify against the live tree
before starting; if either has changed, re-derive the offset arithmetic and trace
construction from the then-current join builder rather than from this plan's snippets):
(a) `build_reindex_program` copies **all** input columns as payload, so every reindex
output is `[new-PK, <every input col>]` — the band/pure-range projection index math
(§6–§7) hardcodes those offsets; (b) each equi join side integrates its own private
trace (`integrate_trace(reindex_x)`, `join.rs:325-326`) rather than reading an external
store.

## Scope (committed)

Supported after this plan:

1. **IN / NOT IN (literal list)** in every expression context (view WHERE, HAVING, join
   residual, DML residual, computed projection), via bind-time desugar.
2. **`WHERE pk IN (literals)`** multi-seek fast path extended from DELETE to UPDATE and
   direct SELECT (single-column PK, as today).
3. **CREATE VIEW `SELECT … FROM a [AS al] WHERE [local_conjuncts AND] <subq_conjunct>`**
   where `<subq_conjunct>` is exactly one of:
   - `[NOT] EXISTS (SELECT … FROM b [AS bl] [WHERE …])`
   - `x [NOT] IN (SELECT y FROM b [AS bl] [WHERE …])` (`x` a column of `a`, `y` a column of `b`)
   with correlation classified by the existing ON-clause vocabulary: an equality prefix
   (0..=PK_LIST_MAX_COLS pairs) plus at most one range conjunct (`extract_join_predicates`,
   `gnitz-sql/src/plan/view/predicates.rs:236`).

Rejected with targeted errors (each names its workaround):

- Subquery expressions anywhere except as one top-level AND conjunct of a Simple-shaped
  view WHERE (under OR/NOT, in projections, in HAVING, in DML, in direct SELECT, in
  GROUP BY views, more than one per view). Explicit `bind_structural` arms for
  `Exists` / `InSubquery` / `AnyOp` / `AllOp` / `Subquery` replace the generic catch-all
  message for these variants.
- Uncorrelated EXISTS (no conjunct pairing an outer and an inner column).
- Subquery FROM = same relation id as the outer FROM (same rationale + message shape as
  the self-join guard, `join.rs:162-170`; workaround: wrap one side in a view).
- Subquery containing joins, GROUP BY, DISTINCT, nested subqueries, or a non-`SELECT`
  body (compose via views instead).
- Subquery-WHERE conjuncts referencing only the outer relation, or landing in
  `extract_join_predicates`' residual with a both-sides reference (same reasoning as the
  outer-join + residual rejection, `join.rs:196-211`: a residual would have to
  participate in the match-existence decision).
- `NOT IN (subquery)` when the outer column or the subquery output column is nullable
  (SQL's NULL-poisoning semantics diverge from anti-join; the non-nullable restriction
  makes them coincide). `IN` needs no restriction: the join layer already drops NULL keys
  (`multi_null_filter_prog` gates, `join.rs:277-323`), which is exactly IN's
  WHERE-context semantics.
- Tuple IN (`(x1,x2) IN (SELECT …)`), and non-column outer operands of IN.
- Pure-range correlation with a range column that is not a ≤8-byte integer (the MIN/MAX
  threshold accumulator limit, same as pure-range LEFT JOIN, `join.rs:653-666`).

## Design

### 1. IN-list desugar (`bind/structural.rs`)

New arm in `bind_structural` before the catch-all:

```rust
// `e IN (l1, …, ln)` ≡ `e = l1 OR … OR e = ln`; NOT IN negates it. SQL 3VL is
// preserved by the desugar itself: a NULL `e` makes every Eq NULL, the OR-chain
// NULL, and NOT(NULL) NULL — the row is excluded either way (same argument as
// the BETWEEN desugar above).
Expr::InList { expr: e, list, negated } => {
    if list.is_empty() {
        return Err(GnitzSqlError::Unsupported("IN with an empty list".into()));
    }
    let mut acc: Option<BoundExpr> = None;
    for item in list {
        let eq = BoundExpr::BinOp(
            Box::new(bind_structural(e, leaf)?),
            BinOp::Eq,
            Box::new(bind_structural(item, leaf)?),
        );
        acc = Some(match acc {
            None => eq,
            Some(prev) => BoundExpr::BinOp(Box::new(prev), BinOp::Or, Box::new(eq)),
        });
    }
    let chain = acc.unwrap();
    Ok(if *negated {
        BoundExpr::UnaryOp(UnaryOp::Not, Box::new(chain))
    } else {
        chain
    })
}
```

The tested expression `e` is bound **once** before the loop and cloned into each `Eq`
(re-binding per element would materialize N `AggCall` nodes for `agg IN (…)` in
HAVING). This lands simultaneously on all three `LeafBinder` contexts (`SingleTable`
`structural.rs:226`, HAVING `plan/view/group_by.rs:920`, `JoinResidual`
`plan/view/predicates.rs:449`) and on both evaluators: the engine expr VM (3VL AND/OR
kernels, `gnitz-engine/src/expr/batch.rs:205-231`) and the client-side DML row
evaluator (`eval_pred_row`, `exec/eval.rs:224`). String list elements work in the
engine-VM contexts via the existing `EXPR_STR_COL_EQ_CONST` lowering
(`gnitz-sql/src/lower.rs:52-136`); in the client-side DML residual they keep erroring —
`eval_expr`'s `lit_str` arm rejects string literals (`exec/eval.rs:108-114`), the same
boundary plain string `=` has there today. List length is unbounded (O(n) opcodes;
acceptable).

Ordering note: the DML fast-path check (`try_extract_pk_in`) runs before any binding
(`mutate.rs:329`), so the desugar does not shadow it. A `NOT IN` or non-PK IN in
UPDATE/DELETE now falls through to the predicate scan and evaluates correctly instead of
erroring.

### 2. `pk IN` multi-seek for UPDATE and direct SELECT

**UPDATE** (`dml/mutate.rs`, `execute_update`): mirror the DELETE fast path. When
`try_extract_pk_in(where_expr, &schema)` matches: dedup the keys, `client.seek` each,
and for each found row apply the SET assignments via the existing `write_set_rows` into
one accumulated update batch, then a single `push_with_mode(…, WireConflictMode::Update)`.
Count = rows actually found.

**Direct SELECT** (`dml/select.rs`): in the WHERE ladder ahead of
`try_extract_pk_seek_residual` (`select.rs:103`): when `try_extract_pk_in` matches,
seek each deduped key and concatenate the replies into one output batch with the
existing `copy_batch_row` (`exec/batch.rs:8`), then flow into the shared
projection/LIMIT tail. IN is a whole top-level conjunct here (never combined with a
residual — `try_extract_pk_in` only matches a bare `InList` expression), so no residual
filtering applies.

### 3. Subquery conjunct classification (`plan/view/dispatch.rs`)

`ViewShape` gains a variant. In `ViewShape::classify` (`dispatch.rs:99-151`), after the
JOIN check and before the GROUP BY check:

- Flatten the WHERE into top-level conjuncts (copy of `flatten_conjuncts`,
  `dml/plan.rs:86-98`, moved to `ast_util.rs` and shared).
- Partition conjuncts into `subq` and `local`: a conjunct is `subq` when, after peeling
  `Expr::Nested` and one `UnaryOp(Not, …)` layer (folding the Not into `negated` — so
  `NOT (EXISTS …)` behaves like `NOT EXISTS …`), it matches
  `Expr::Exists{..} | Expr::InSubquery{..}`; everything else is `local`.
- `subq.len() == 1` → `ViewShape::Subquery { select, subq_conjunct, local_conjuncts }`,
  after rejecting `group_by_is_present || projection_has_aggregate` (error: "EXISTS/IN
  subqueries are not supported together with GROUP BY/aggregates; put the subquery in an
  inner view"). `subq.len() > 1` → error "one subquery conjunct per view; stack views".
  `subq.is_empty()` → existing Simple/GroupBy routing unchanged. Subqueries nested
  deeper than a top-level conjunct are caught later by the new explicit
  `bind_structural` arms with the targeted message.

New builder module `plan/view/exists.rs`, dispatched like the other shapes.

### 4. Subquery binding and correlation extraction (`plan/view/exists.rs`)

- Envelope: for EXISTS, `subquery: Box<Query>` (sqlparser 0.56, `Expr::Exists`) — run
  `reject_unhonored_query_clauses` (`with: false, limit: false`) on it; for IN,
  `subquery: Box<SetExpr>` (`Expr::InSubquery`) — require `SetExpr::Select`. Run
  `reject_unhonored_select_clauses` (`where_filter: true, grouping: false,
  distinct: false`) on the inner select; require exactly one FROM item with no joins
  (`extract_table_factor_name`).
- Resolve outer and inner relations via `binder.resolve` (views allowed); reject
  `outer_tid == inner_tid`.
- Build the two-relation `AliasMap` exactly as `execute_create_join_view` does
  (`join.rs:172-189`): outer alias at `col_offset 0`, inner alias at
  `col_offset outer_schema.columns.len()`.
- **Correlation predicate**: the inner WHERE (for IN: additionally the synthesized pair)
  is split by side. For each top-level conjunct of the inner WHERE, collect its column
  references (`Identifier` / 2-part `CompoundIdentifier` resolved against the alias map);
  a conjunct whose references are all inner-side becomes an inner pre-filter (bound with
  `bind_single_table` against the inner schema, compiled with
  `compile_bound_expr_to_program`); all remaining conjuncts are re-assembled into one
  AND-tree and fed to `extract_join_predicates(on_expr, outer_schema, inner_schema,
  alias_map)`. Any non-empty residual from that call → reject. Outer-only conjuncts have
  inner-side reference count 0 and outer count > 0 → reject ("hoist it into the view's
  own WHERE"; for NOT EXISTS hoisting would be semantically wrong, so no silent rewrite).
- **IN**: the outer operand must bind to `BoundExpr::ColRef` on the outer schema; the
  subquery projection must be exactly one item binding to `ColRef` on the inner schema.
  The pair `(outer_col, inner_col)` is appended to the equality list (validated through
  the same `validate_join_key_pair` path inside `extract_join_predicates` — float keys
  rejected, cross-width promotion applied). `NOT IN`: additionally require both columns
  `!is_nullable`.
- **EXISTS projection is ignored** (SQL defines EXISTS independent of its select list;
  it is not bound — divergence from Postgres name-checking, acceptable pre-alpha and
  documented in the module header).
- Zero equality pairs and no range conjunct → "uncorrelated EXISTS" rejection.

Outer `local_conjuncts` bind with `bind_single_table` against the outer schema and
compile into one filter applied to the raw outer input before everything else.

### 5. Circuit construction — equi correlation (n_range = 0)

Derivation of the equi outer-join circuit (`join.rs:280-419`) with the O-side columns
never materialized. `A` = outer (filtered by local conjuncts), `B` = inner (filtered by
inner pre-filter). `k` = number of equality pairs.

```rust
let mut cb = CircuitBuilder::new(view_id, 0);
let input_a_raw = cb.input_delta_tagged(outer_tid);
let a_local = /* local_conjuncts filter or input_a_raw */;
let input_b_raw = cb.input_delta_tagged(inner_tid);
let b_local = /* inner pre-filter or input_b_raw */;

// NULL-key gating + reindex, verbatim from the equi join builder (join.rs:277-323),
// with `a_local`/`b_local` in place of the raw inputs.
let a_match = /* multi_null_filter_prog gate over a_local when key nullable */;
let b_match = /* … over b_local … */;
let reindex_a = cb.map_reindex(a_match, &outer_cols, &left_target_tcs, build_reindex_program(&outer_schema));
let reindex_b = cb.map_reindex(b_match, &inner_cols, &right_target_tcs, build_reindex_program(&inner_schema));
let trace_a = cb.integrate_trace(reindex_a);
let trace_b = cb.integrate_trace(reindex_b);
let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // [k pk, A, B]
let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // [k pk, B, A]

// π_A(inner): project each term straight to [_join_pk × k, A] (no normalize_to_ab,
// B's columns are never carried).
let pa_ab = cb.map(join_ab, &(k..k + a_n).collect::<Vec<_>>());
let pa_ba = cb.map(join_ba, &(k + b_n..k + b_n + a_n).collect::<Vec<_>>());
let pi_a = cb.union(pa_ab, pa_ba);

// a_all re-keys the FULL (locally filtered, NULL keys included) outer input with the
// identical encoding, reusing `reindex_a` when the key is non-nullable (join.rs:380-392).
let a_all = /* reindex_a, or a fresh map_reindex(a_local, …) when key nullable */;
let nu = cb.positive_diff(a_all, pi_a); // ν = max(0, A − π_A(inner)), keyed [_join_pk, A]

let sink_input = if negated {
    nu // NOT EXISTS / NOT IN: exactly the unmatched rows, weight w_A each
} else {
    // EXISTS / IN: A − ν. Matched rows keep weight w_A; unmatched cancel to 0 at
    // consolidation. `a_all` may alias `reindex_a` (shared node) so it rides the
    // non-destructive PORT_IN_B operand; negate(ν) is fresh (positive_diff's
    // centralized operand-order rule, gnitz-core/src/circuit.rs:371-383).
    let neg = cb.negate(nu);
    cb.union(neg, a_all)
};
```

Weight-exactness (per preserved identity `x`, weight `w_A ≥ 0`, matched inner weight
sum `S ≥ 0`): `ν(x) = max(0, w_A·(1 − S)) = w_A·[S=0]`; semi `= w_A − ν = w_A·[S>0]`.
Both hold for bag-valued outer inputs (a non-`unique_pk` view) by the same clamp-the-
result argument as the shipped null-fill. NULL outer keys: never in `π_A(inner)`, so
`ν = w_A` (correct for NOT EXISTS — no match exists) and semi `= 0` (correct for
EXISTS/IN). Partition-locality: identical shapes to the LEFT null-fill, so the runtime
join-shard scatter (`execute_multi_worker_step` arm 4) co-locates `a_all`, `π_A(inner)`
and the traces per `_join_pk` worker; the circuit carries no `ExchangeShard`.

**Output schema/keying** (same convention as equi join views — and deliberately
**divergent from the range variants**, which key by the outer source PK: an equi
circuit has no output exchange, so re-keying to the outer PK is physically impossible
there, while the range circuits must exchange anyway. The module header documents this
shape-dependent PK surface — `[_join_pk×k, a…]` for equi vs `[_src_pk×pa, a…]` for
band/pure-range — so downstream consumers are not surprised): view PK = the k synthetic
`_join_pk`/`_join_pk_{i}` columns (types = the pairs' promoted `T_i`, non-nullable);
payload = the user projection over the outer relation only. Projection via
`build_join_view_projection` (`join.rs:1106-1160`) with an alias map containing only the
outer relation, `n_combined = a_n`, `payload_offset = k`,
`coldef = |i| outer_schema.columns[i].clone()` (no nullability changes — there is no
null-fill column). The inner alias is not in the map, so inner columns are unresolvable
in the projection — correct SQL scoping for free. Column-count guard:
`k + a_n + b_n ≤ MAX_COLUMNS` (the widest intermediate is the join term).

### 6. Circuit construction — band correlation (n_eq ≥ 1 + one range conjunct)

Derivation of the band branch of `build_range_join_view` (`join.rs:559-1066`), preserved
side = outer, output keyed by the **outer source PK** riding the mandatory range-join
output exchange:

- Reindex slots `[eq…, range]`, per-side NULL gate, `map_reindex`, `integrate_trace`,
  and the two `join_with_trace_range_node` terms exactly as `join.rs:680-727` (no
  `partition_filter` — band scatters by eq prefix), with `a_local`/`b_local` as inputs.
- `merged = normalize_to_ab(...)` (the band π needs the full-width re-key of `merged`,
  `join.rs:1026-1032`): `rekey_a = map_reindex(merged, &pair_pk_cols[..pa], zero,
  build_reindex_program(&union_schema))`, `proj_a = map(rekey_a, pa+k .. pa+k+a_n)`
  → `π_A(inner)` keyed `[a.pk…, A]`.
- `a_all = map_reindex(a_local, &outer_schema.pk_cols, zero, build_reindex_program(&outer_schema))`
  (`join.rs:1033-1038`), `ν = positive_diff(a_all, proj_a)`.
- `sink_pre = ν` (negated) or `union(negate(ν), a_all)` (non-negated), then the output
  `ExchangeShard` over the leading `pa` PK slots and `sink` — the arm-1 runtime
  (input relay + output exchange) drives it because the circuit contains
  `DeltaTraceRange` nodes.
- Output: view PK = outer PK columns, exposed as leading columns named `_src_pk_{i}`
  (fresh names — the outer PK columns also appear verbatim in the payload, and reusing
  their names would trip `reject_duplicate_column_names` on explicit projections);
  payload = user projection over outer columns at `payload_offset = pa`.

### 7. Circuit construction — pure-range correlation (n_eq == 0)

Derivation of the pure-range LEFT threshold branch (`join.rs:896-1009`), both
polarities. **Shared-helper refactor first**: the threshold pipeline (`join.rs:930-990`)
and the output tail (shard + view_pk assembly) are hardwired inside
`build_range_join_view` to the pair-PK convention (`nf_tail` re-keys onto
`[a.pk…, b.pk…]`, `join.rs:845-893`; the shard/`view_pk` pair is `pair_pk`-wide with a
`debug_assert_eq!`, `join.rs:1071-1080`). The semi variants need a `pa`-wide
shard/view_pk and no null-extend, so before deriving them, extract from
`build_range_join_view` (behavior-neutral, same file): (a) the inline threshold
pipeline (`m` reduce + `trace_m` + the two matched terms + `rekey_a`) and (b) a
parameterized output tail (PK width, null-extend on/off). The range LEFT JOIN keeps
compiling byte-identically through the extracted helpers.

- Threshold pipeline verbatim: `map_hash_row(reindex_b, &[0], 0)` →
  `reduce_multi_local(mbh, &[], &[(AGG_MAX|AGG_MIN, 1)], false)` → `map_reindex` →
  `trace_m`; matched terms `j_am = join_with_trace_range_node(int_a, trace_m, 0, rel_ab)`
  and `j_ma = join_with_trace_range_node(reindex_m, trace_a, 0, rel_ba)`; per-term
  A-projection; `matched_raw = union(m_am, m_ma)`; the shared `rekey_a` re-key onto
  `[a.pk…, A]` (`join.rs:951-988`).
- **EXISTS** (non-negated): `sink_pre = rekey_a(matched_raw)` — `matched = A ⋈ {m}`
  carries each row's weight verbatim (one-row `m` cannot multiply); NULL-range-key rows
  are absent from `int_a` and correctly excluded.
- **NOT EXISTS** (negated): `sink_pre = union(rekey_a(int_a), negate(matched))` plus,
  when the range key is nullable, the NULL-key branch off the unfiltered broadcast input
  (`multi_null_filter_prog(want_null: true)` → re-key by `a.pk` → `partition_filter` →
  union), verbatim `join.rs:997-1008`. `A − ∅ = A` handles an empty/all-NULL inner side
  with no sentinel.
- Output exchange + `_src_pk_{i}` keying as in §6. Range column must be a ≤8-byte
  integer (reject up front, message mirroring `join.rs:661-665`).

The EXISTS mirror-witness problem that forbids pure-range RIGHT/FULL joins does not
arise: the preserved side is always the outer relation.

### 8. Targeted rejection arms (`bind/structural.rs`)

Before the catch-all, arms for `Expr::Exists { .. }`, `Expr::InSubquery { .. }`,
`Expr::Subquery(_)`, `Expr::AnyOp { .. }`, `Expr::AllOp { .. }` returning
`Unsupported` with position-specific guidance ("[NOT] EXISTS/IN (SELECT …) is only
supported as a top-level AND conjunct of a CREATE VIEW WHERE clause"; ANY/ALL and scalar
subqueries: "not supported"). These fire in every context the exists-builder does not
intercept (under OR, HAVING, projections, DML, direct SELECT).

## Sequencing

1. **IN-list desugar + targeted rejection arms** (`bind/structural.rs`), unit tests for
   the desugar tree and 3VL, e2e filter tests through views and DML residuals.
2. **`pk IN` multi-seek for UPDATE and SELECT** (`dml/mutate.rs`, `dml/select.rs`,
   `exec/batch.rs` reuse), e2e parity tests with the DELETE path.
3. **Classification + equi EXISTS/NOT EXISTS/IN/NOT IN** (`dispatch.rs`,
   `plan/view/exists.rs`), unit circuit-shape tests + e2e.
4. **Band + pure-range correlation variants** in `plan/view/exists.rs`, e2e.

Each step compiles and passes `make verify` + `make e2e` (`GNITZ_WORKERS=4`)
independently.

## Files touched

| File | Change |
|------|--------|
| `crates/gnitz-sql/src/bind/structural.rs` | `InList` desugar arm; targeted `Exists`/`InSubquery`/`Subquery`/`AnyOp`/`AllOp` rejection arms |
| `crates/gnitz-sql/src/ast_util.rs` | `flatten_conjuncts` moved here (shared by DML planner and view classifier) |
| `crates/gnitz-sql/src/dml/plan.rs` | drop local `flatten_conjuncts` (use `ast_util`) |
| `crates/gnitz-sql/src/dml/mutate.rs` | UPDATE `pk IN` multi-seek fast path |
| `crates/gnitz-sql/src/dml/select.rs` | SELECT `pk IN` multi-seek + batch concat via `copy_batch_row` |
| `crates/gnitz-sql/src/plan/view/dispatch.rs` | `ViewShape::Subquery` classification |
| `crates/gnitz-sql/src/plan/view/exists.rs` | new: subquery binding, correlation extraction, three circuit variants |
| `crates/gnitz-sql/src/plan/view/mod.rs` | register `exists` module |
| `crates/gnitz-sql/src/plan/view/predicates.rs` | pub(crate) visibility for helpers the exists builder reuses (no behavior change) |
| `crates/gnitz-sql/src/plan/view/join.rs` | behavior-neutral extraction of the pure-range threshold pipeline + parameterized output tail (§7) |
| `crates/gnitz-py/tests/test_exists.py` | new e2e suite |
| `crates/gnitz-py/tests/test_dml.py` | UPDATE/SELECT `pk IN` cases |

## Testing

- **Binder units**: IN-list desugar tree shapes (`IN`, `NOT IN`, strings, floats, single
  element); empty-list rejection; targeted messages for EXISTS under OR / in projection
  / ANY/ALL.
- **Planner circuit-shape units** (mirroring the join builder's tests): equi
  semi/anti node counts and port wiring (π_A projections, `positive_diff` operand order,
  `a_all` aliasing `reindex_a` iff key non-nullable); band and pure-range variants; every
  rejection path (two subquery conjuncts, same-source, outer-only inner conjunct,
  residual correlation, nullable NOT IN, tuple IN, uncorrelated EXISTS, 16-byte
  pure-range column).
- **E2E behavior** (`GNITZ_WORKERS=4` throughout; every scenario also asserts weights,
  not just row presence):
  - EXISTS/NOT EXISTS/IN/NOT IN over populated tables (backfill) and via incremental
    pushes that flip membership back and forth (insert first inner match → outer row
    appears in EXISTS view and leaves NOT EXISTS view; retract last match → reverse).
  - Multiplicity: outer side = a `UNION ALL` view carrying weight-2 rows; assert
    weight-exact semi/anti output.
  - NULL keys: nullable outer/inner correlation columns, EXISTS vs NOT EXISTS asymmetry;
    NOT IN nullable rejection message.
  - Local conjuncts on both sides; compound (2-column) correlation keys; cross-width
    key promotion (i32 outer vs i64 inner).
  - Band (`b.k = a.k AND b.t < a.t`) and pure-range (`b.y < a.x`) variants, including
    empty inner side and all-NULL inner range column.
  - Views as inner/outer sources; DROP/re-CREATE; view-over-EXISTS-view chains.
  - `pk IN` UPDATE/SELECT parity incl. repeated keys, absent keys, UUID string keys.

## Invariants preserved

- All merge/consolidation paths still sort by (PK, payload); the new circuits introduce
  no new orderings.
- Single-source-per-epoch: two distinct source ids per circuit (same-source rejected),
  so the symmetric 2-term join stays valid; ν absorbs within-epoch simultaneity via
  `positive_part` exactly as the shipped null-fill.
- Non-linear ops (`positive_part`) sit on partition-local, consolidated scopes identical
  to the LEFT-join shapes the engine already validates.
- The equi circuits carry no `ExchangeShard` (join-shard scatter arm); the range
  circuits carry exactly the one output `ExchangeShard` over their view PK (compound-key
  alignment invariant, `join.rs:1068-1081`).
- No engine, wire, or catalog changes; existing views compile byte-identically.
