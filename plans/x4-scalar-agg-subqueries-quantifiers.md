# x4: correlated scalar aggregate subqueries + ANY/ALL quantifiers

## Problem

With semi/anti/mark lowering shipped, the remaining high-value subquery forms are
value-producing: `WHERE x > (SELECT MAX(y) FROM b WHERE b.k = a.k)`,
`SELECT a.*, (SELECT COUNT(*) FROM b WHERE b.k = a.k)`, and quantified comparisons
(`x < ANY(SELECT …)`, `x >= ALL(SELECT …)`). All are rejected today: scalar
`Expr::Subquery(_)` at `bind/structural.rs:209` and `Expr::AnyOp`/`Expr::AllOp` at
`bind/structural.rs:210`.

The standard decorrelation aggregates the inner side grouped by its correlation columns
into a derived relation `G`, LEFT JOINs the outer side against `G`, and rewrites the
subquery expression to a reference to `G`'s aggregate column — plus the "COUNT bug"
repair: a grouped reduce emits **nothing** for an empty group (confirmed: the reduce's
COUNT(*) companion gates group existence; `group_by.rs:329-352` appends the companion,
and an emptied group is "retracted, not null-filled", `group_by.rs:118-124`), so a
`COUNT` subquery must read `0` through the join's null-fill, not lose the row.

**This plan is pure SQL-planner lowering — no engine, wire, or opcode changes.** Every
circuit shape it emits (grouped reduce, global-ground reduce, equi/pure-range join, LEFT
null-fill, linear filter/map) already exists and is reused verbatim: the lowering
synthesizes ordinary `Select` ASTs and feeds them to the existing `emit_group_by_pieces`,
`plan_join_chain`, and `emit_linear` builders.

**Cardinality note (the IVM-specific decision).** SQL requires a runtime error when a
scalar subquery yields more than one row. A materialized view cannot raise mid-tick, so
GnitzDB supports scalar subqueries only where single-row-ness is *provable by
construction*: a single aggregate over the correlation group (grouped `G` emits exactly
one row per key; a `global_ground` reduce emits exactly one row total). Non-aggregate
scalar subqueries, and aggregate subqueries carrying their own `GROUP BY`/`HAVING` (which
break the one-row-per-key guarantee), stay rejected with an error saying exactly that.

## Prerequisite state (verified against the live tree)

Front door and shape classification: `plan/view/dispatch.rs::execute_create_view` inlines
CTEs and derived tables onto a `ViewChain`, classifies the body via
`ViewShape::classify`, dispatches to a per-shape builder, and bundles the hidden segments
plus the user view into one atomic `client.create_view_chain(...)`.

The reusable builders (all `-> EmitPieces`, i.e. `(Circuit, Vec<ColumnDef>, Vec<u32>)`,
`plan/view/mod.rs:22`):

- **`group_by::emit_group_by_pieces(client, view_id, &select, source) -> EmitPieces`**
  (`group_by.rs:184-189`). `source` is a pre-resolved `(tid, Rc<Schema>)`. For
  `SELECT k AS k0, AGG(e) AS a0 FROM t GROUP BY k` (group column *not* the source PK) it
  emits out_cols `[_group_pk (U128, hidden, non-null), k0, a0]` with PK `[0]` — **the
  correlation key `k0` rides as an ordinary payload column, not the segment PK**
  (`group_by.rs:382-402`, 522-566). For `SELECT AGG(e) AS a0 FROM t` (no GROUP BY) it sets
  `global_ground = true` (`group_by.rs:250`) and emits a single "ground" row over an empty
  source: `COUNT = 0` (non-nullable), `SUM/MIN/MAX/AVG = NULL` (nullable); out_cols
  `[_group_pk, a0]`, PK `[0]`.
- **`join::plan_join_chain(client, binder, emit_vid, &select, chain) -> EmitPieces`**
  (`join.rs:343-349`). Honors FROM/joins + WHERE + projection only; **rejects GROUP BY /
  DISTINCT** on the outer join `Select` (`join.rs:350-358`). Reads join *kind* from
  `join.join_operator`: `LeftOuter/Left(On(e)) -> LEFT`, `Inner/Join(On(e)) -> INNER`
  (`join.rs:159-175`). A LEFT equi join is fully value-returning — an unmatched left row is
  emitted with **all right columns NULL** (`equi_nf` → `null_extend`, `join.rs:830-872`;
  right columns marked nullable when `preserves_left`, `join.rs:63-80`).
- **`crate::plan::lp::lower_linear(client, binder, &select) -> Rel` +
  `simple::emit_linear(vid, rel) -> EmitPieces`** (`dispatch.rs:88-91`,
  `lp.rs:41-91`). Lowers a single-table filter/map body to `Project(Filter?(Source))`,
  binding WHERE via `bind_single_table` and the projection via `build_projection` — **both
  support computed expressions** (CASE/COALESCE/NULLIF/arithmetic through `bind_structural`
  and `build_projection`; the computed-projection path is exercised by
  `test_computed_projections.py`).

Load-bearing constraints that shape the design:

1. **The join-view projection is column-refs-only.** `build_join_view_projection`
   (`join.rs:1654-1716`) accepts only a bare `Identifier`, a 2-part `CompoundIdentifier`,
   an `ExprWithAlias` **of a column ref**, and bare `Wildcard`; any computed projection
   item errors (`"only column references supported in AS clause"` / `"unsupported SELECT
   item"`). `SELECT a.*` (`QualifiedWildcard`) is **not** supported and errors; bare `*`
   expands to *all non-hidden columns of both sides*. So the outer columns must be
   enumerated as explicit qualified refs, and a projected computed expression (a projected
   `COUNT`, which becomes `COALESCE(agg, 0)`) cannot live in the join projection.
2. **The WHERE over a join *does* support computed expressions.** For an OUTER join the
   WHERE is a post-null-fill 3VL `where_filter` bound with `bind_structural`, so OR / CASE /
   COALESCE / BETWEEN / IN and references to the right (joined) relation's columns are all
   supported; only aggregate *function calls* in the predicate are rejected (a reference to
   a pre-materialized aggregate *column* is a plain column ref and is fine)
   (`join.rs:187-204`, `predicates.rs:465-469`, `499-512`).
3. **The join right side may be a pre-compiled hidden segment resolved by alias.**
   `plan_join_chain` resolves every join relation by name through the binder, which probes
   its cache first (`resolve.rs:155-175`); `binder.cache_alias(name, (vid, schema))`
   registers a compiled segment under a name (`resolve.rs:219-223`). `cache_alias` calls
   `validate_relation_name` → `validate_user_identifier`, which **rejects a leading `_`**
   (reserved) but accepts an internal `_` (`plan/validate.rs:762-772`). So a synthetic
   segment alias must be a non-`_`-leading valid identifier.
4. **A LEFT/RIGHT/FULL join ON must be purely equi and/or one range conjunct** — a residual
   conjunct in an outer ON is rejected (`join.rs:659-667`); move extra predicates to the
   WHERE. An INNER ON folds any residual (and the WHERE) into a linear filter over the join
   (`classify_join_where`, `join.rs:187-204`). A *pure* comparison ON like `a.x < g.a0`
   (`n_eq == 0`) is accepted; for INNER there is no width cap, for LEFT it is capped to a
   ≤8-byte integer and RIGHT/FULL pure-range is rejected (`join.rs:1117-1139`).
5. **Aggregate output type is statically decidable** (`agg_result_type`,
   `group_by.rs:93-108`): `COUNT/COUNT(x) -> I64`; `AVG -> F64` always; `SUM/MIN/MAX ->
   F64` iff the source column is float, else a ≤8-byte integer. A float aggregate value
   cannot be a join key (`validate_join_key_pair` → `reject_float_key`,
   `predicates.rs:125-128`). A float **GROUP BY** column is also hard-rejected
   (`group_by.rs:232`).
6. The correlation side-split machinery already exists for EXISTS/IN: `count_side_refs`
   (`exists.rs:994-1025`) classifies a conjunct as inner-only / outer-only / both-sides
   against a flat two-relation alias map; `extract_join_predicates` validates equi/range key
   pairs with cross-width/cross-sign/float rules (`predicates.rs:304-427`).

## Scope (committed)

Supported after this plan, in a CREATE VIEW whose **FROM is a single base table** (the
"outer") — a JOIN-FROM view classifies as `ViewShape::Join` and keeps today's rejection of
subqueries in its WHERE; that combination is out of scope:

1. **Correlated scalar aggregate subqueries** — `(SELECT AGG(e) FROM <inner> WHERE corr
   [AND inner-local…])` with `AGG ∈ {COUNT(*), COUNT(x), SUM, MIN, MAX, AVG}`, in WHERE
   comparisons (including under OR / inside CASE / inside COALESCE), and in projections.
   The **inner FROM is a single base table** (inner-local WHERE filters allowed); a
   join / derived-table / grouped / DISTINCT inner is rejected. Correlation: one or more
   `inner_col = outer_col` equality conjuncts in the subquery's top-level WHERE (**equality
   only** — a range correlation with aggregation is rejected). Every non-correlation,
   non-inner-local conjunct referencing the outer is rejected.
2. **Uncorrelated scalar aggregate subqueries in WHERE comparisons** —
   `outer_col OP (SELECT AGG(e) FROM <inner>)` with `OP ∈ {=, <, <=, >, >=}`, lowered to a
   join against the one-row global-aggregate segment (`=` → equi join, range op →
   pure-range INNER join). Restricted to a **top-level AND conjunct of the WHERE** (§4
   Item 1). Both the **aggregate and the outer operand must be integer-typed** (§4 Item 4);
   AVG (always float) and a float `SUM/MIN/MAX` source, or a float outer column, are
   rejected up front with a scalar-specific message. `<>`, and projection / CASE positions,
   of an *uncorrelated* scalar are rejected (they would need a keyless cross join).
3. **Quantified comparisons over subqueries**:
   - `x = ANY(sub)` ≡ `x IN (sub)` and `x <> ALL(sub)` ≡ `x NOT IN (sub)` — rewritten to
     the existing IN / NOT IN mark path with its restrictions.
   - `x OP ANY(sub)` / `x OP ALL(sub)` for range `OP`, correlated or uncorrelated-ANY, via
     MIN/MAX rewrites (§3). The subquery's output column must be **non-nullable**.
   - Rejected: `= ALL`, `<> ANY`, uncorrelated `ALL` (empty-set-TRUE would need a keyless
     preserved join).
4. **Multiple** scalar subqueries in one view stack multiple `(G, join)` pairs (they are
   not counted by the "at most one EXISTS/IN subquery" guard, which counts only
   `Exists`/`InSubquery`, `ast_util.rs:127-131`). A view mixing an EXISTS/IN subquery with
   a scalar subquery is rejected.

Still rejected: non-aggregate scalar subqueries; an aggregate subquery with its own
GROUP BY / HAVING (§4 Item 2); scalar subqueries in HAVING / DML / a direct top-level
SELECT (the existing `bind_structural` backstop); a JOIN-FROM outer; a
join/derived/grouped/DISTINCT inner; correlation across more than one nesting level;
LATERAL.

## Design

### 0. Decorrelation shape (the committed structure)

Every supported view lowers to a fixed three-layer chain of **existing** circuit pieces:

```
Gᵢ   = reduce over the inner table, one per subquery          (emit_group_by_pieces)
H    = the outer  ⟕/⋈  every Gᵢ, projecting outer cols + Gᵢ agg cols   (plan_join_chain)
final= linear filter/map over H: rewritten WHERE + projection  (lower_linear/emit_linear)
```

Rationale for the three layers, forced by the load-bearing constraints:

- The **aggregate value must be materialized as a physical column** (data-dependent per
  correlation key), so the subquery becomes a `Gᵢ` reduce joined to the outer — not a
  compile-time constant like the mark path (the mark splice folds a subquery to a literal
  `0/1` and is therefore not reusable here; `exists.rs:493-497`, `867-890`).
- The **WHERE** substitution (including the COUNT repair `COALESCE(agg, 0)`) needs computed
  expressions, which the join-view WHERE supports — but is placed instead on the linear
  `final` for a single uniform substitution site.
- The **projection** substitution needs computed expressions for a projected `COUNT`, which
  the join-view projection *cannot* express (constraint 1). Hence `H` carries only column
  refs (outer columns + `Gᵢ` aggregate columns) and the user projection is applied by the
  linear `final` over `H`, mirroring the shipped "hidden join → operator over H" pattern
  (`dispatch.rs::compile_join_to_hidden` / `resolve_operator_input`).

The uniform `H` + linear `final` is one accepted extra segment per scalar-subquery view;
no per-shape special-casing (revisit only with evidence).

The whole lowering is a **semantic AST rewrite** run inside a new dispatch builder
`scalar::emit_scalar_subquery_pieces(client, final_vid, select, binder, chain)`. It needs
the binder/catalog (to resolve inner/outer schemas and aggregate types), then synthesizes
the `Gᵢ`, `H`, and `final` `Select` ASTs and calls the three existing builders. The
existing IN path already demonstrates AST-node synthesis to reuse machinery
(`exists.rs:277-348`), so this is idiomatic.

### 1. Correlated scalar aggregate

For each correlated scalar-aggregate subquery `(SELECT AGG(e) FROM b WHERE b.k = a.k [AND
b.k2 = a.k2] [AND <inner-local>])`:

- **Resolve two scopes** (outer = the view's single FROM table `a`; inner = the subquery's
  single FROM table `b`) into a flat two-relation alias map, and split the inner WHERE
  conjuncts with `count_side_refs` (`exists.rs:994-1025`): inner-only → `G`'s WHERE;
  `inner_col = outer_col` equalities → the correlation key list `K = [(b.kᵢ, a.kᵢ)]`
  (equality only; at least one required, else the uncorrelated path §2); a range or other
  outer-referencing conjunct → reject with a targeted message.
- **Build `Gᵢ`** as a hidden segment via `chain.add_segment(client, |client, chain, vid|
  group_by::emit_group_by_pieces(client, vid, &g_select, inner_source))`, where `g_select`
  is the synthesized

  ```sql
  SELECT b.k AS <k0>, b.k2 AS <k1>, AGG(e) AS <a0>  FROM b  [WHERE <inner-local>]  GROUP BY b.k, b.k2
  ```

  Each correlation column and the aggregate is projected under a **synthetic name**
  (`<k0>`, `<a0>`, …) generated distinct from the outer schema's column names, so the
  downstream join/linear layers reference them unambiguously. A **float correlation
  column** is rejected up front with a scalar-specific message (it would otherwise surface
  as the reduce's `reject_float_key` "GROUP BY" error, `group_by.rs:232`). Register `Gᵢ`
  under a synthetic alias `sqN` (non-`_`-leading, distinct from the outer alias) via
  `binder.cache_alias`.
- **Stack `Gᵢ` onto `H`** as `… <outer> LEFT JOIN sqN ON a.k = sqN.<k0> AND a.k2 =
  sqN.<k1> …`. The correlation columns join as ordinary payload columns of both sides. Type
  promotion / cross-sign / float rules are enforced by the join builder's
  `validate_join_key_pair`. `H` projects every non-hidden outer column (enumerated as
  qualified `a.col` refs — `a.*` is unsupported, constraint 1) plus `sqN.<a0>` as a bare
  ref.
- **Substitute the subquery node** in the view's WHERE / projection with a reference to
  `H`'s aggregate column `<a0>`:
  - `COUNT(*)` / `COUNT(x)`: `COALESCE(<a0>, 0)` — the COUNT-bug repair. `<a0>` is nullable
    after the LEFT JOIN; `COALESCE(<a0>, 0)` binds through the existing `bind_coalesce`
    desugar to `Case { [(IsNotNull(<a0>), <a0>)], else: 0 }` for free (`structural.rs:301-324`).
  - `SUM` / `MIN` / `MAX` / `AVG`: the bare column ref `<a0>` — SQL defines these as NULL
    over an empty group, exactly what the null-fill delivers. A float `AVG`/`SUM`/`MIN`/`MAX`
    is fine here: it is a payload column, never a join key.

Weight-exactness: `Gᵢ` emits one weight-1 row per live group, so the LEFT JOIN preserves
each outer row at weight `w_A` (matched via the bilinear `w_A·1`, unmatched via `ν = w_A`).
The linear `final` over `H` is a filter/map. Incremental transitions are inherited: a group's
aggregate change retracts the old `G` row and inserts the new; the join propagates the paired
retraction/insertion; the last-row retraction of a group retracts the `G` row and the
null-fill re-emits the outer row with a NULL aggregate — flipping `COALESCE(<a0>, 0)` back to
`0` with balanced weights.

### 2. Uncorrelated scalar aggregate in a WHERE comparison

`G_global = SELECT AGG(e) AS <a0> FROM b` (no GROUP BY → `global_ground = true`), which
holds **exactly one row at all times**: the real aggregate when `b` is non-empty, the ground
row (`COUNT = 0`, `SUM/MIN/MAX/AVG = NULL`) when empty.

`outer_col OP (SELECT AGG …)`, restricted to a **top-level AND conjunct of the WHERE**
(§4 Item 1), lowers to an **INNER join between the outer and `G_global`** with the comparison
as the join ON (no keyless cross join is needed — the single-row segment joins on the
comparison itself):

- `=` → equi `Join` on `ON outer_col = sqN.<a0>`. A NULL aggregate (empty source
  SUM/MIN/MAX ground) is dropped by the join's NULL-key gate — matching SQL (`x = NULL` is
  UNKNOWN → filtered). `COUNT`'s ground row is a real `0`, so `x = 0` matches an empty source
  — also SQL-correct.
- `<, <=, >, >=` → pure-range **INNER** `Join` (`ON outer_col OP sqN.<a0>`, `n_eq = 0`).
  INNER pure-range applies no width cap (constraint 4); only
  `validate_range_join_key_pair` applies, so an integer aggregate is required.

`H` projects the outer columns only (the aggregate is consumed by the join-as-filter). Both
operands are join keys, so **both must be non-float integers** (§4 Item 4). Because the join
is INNER and `G_global` has exactly one weight-1 row, every surviving outer row keeps weight
`w_A`. The comparison being an unconditional join filter is exactly why it is restricted to a
top-level AND conjunct: an `OR`/`NOT`/projection/CASE placement is rejected.

### 3. Quantifier rewrites (`Expr::AnyOp` / `Expr::AllOp`)

`right` must be `Expr::Subquery(q)`; `left` a column of the outer scope. `= ANY` / `<> ALL`
are AST-rewritten to `IN` / `NOT IN` and deferred to the existing mark path. For range `OP`,
let the subquery's value set per correlation key be `S_K` (or global when uncorrelated), with
the **non-nullable inner column** enforced at rewrite (`inner_schema.columns[out].is_nullable`
must be false; else reject) — with NULLs in the set, SQL's ANY/ALL go three-valued in ways the
MIN/MAX rewrite cannot reproduce under negation, and the restriction makes them coincide
exactly.

| Form | Lowering |
|------|----------|
| `x = ANY(S)` | rewrite to `x IN (S)` → mark path |
| `x <> ALL(S)` | rewrite to `x NOT IN (S)` → mark path |
| `x < ANY(S_K)` (corr.) | `Gᵢ(K, m = MAX(y))`; LEFT JOIN on K; predicate **`m IS NOT NULL AND x < m`** |
| `x < ALL(S_K)` (corr.) | `Gᵢ(K, m = MIN(y))`; LEFT JOIN on K; predicate **`m IS NULL OR x < m`** |
| `x < ANY(S)` (uncorr.) | §2 pure-range INNER join against `G_global(m = MAX(y))` |
| other range ops | same with MIN/MAX swapped per the op's direction |
| `= ALL`, `<> ANY`, uncorr. `ALL` | rejected |

The correlated ANY/ALL predicate is emitted **on the linear `final` over `H`** (not the
join), so its existence conjunct and the comparison are one 3VL filter over the materialized
`m` column. `m` is nullable after the LEFT JOIN, so `m IS NULL` / `m IS NOT NULL` bind as
ordinary column null-tests (constant-folding does not fire).

Correctness of the three-valued edges under the non-nullable-inner restriction:

- **ANY over an empty group** must be FALSE: the LEFT JOIN null-fills `m = NULL`; the explicit
  `m IS NOT NULL` conjunct makes the predicate a definite FALSE (not UNKNOWN), exact in all
  positions — critically under `NOT`, where SQL's `NOT FALSE = TRUE` but a bare `x < NULL`
  would be UNKNOWN and drop.
- **ALL over an empty group** must be TRUE: covered by the explicit `m IS NULL OR …` disjunct.
- **NULL outer `x`**: UNKNOWN in both SQL and the rewrite (`m IS NOT NULL AND (NULL < m)` and
  `m IS NULL OR (NULL < m)` both leave the comparison UNKNOWN when `m` is non-null) — exact.

For the **uncorrelated ANY** (top-level AND conjunct), the INNER pure-range join is itself the
existence test: an empty source gives `m = NULL`, so `x < NULL` is UNKNOWN → no match → the
row drops = ANY-over-empty = FALSE, exact for a conjunct position. No explicit existence
conjunct is needed there.

### 4. Audit-fix guards (each item, and where it is enforced)

All enforced in `scalar::emit_scalar_subquery_pieces` before segment synthesis:

- **Item 1 — uncorrelated scalar comparison must be a top-level AND conjunct.** The
  uncorrelated lowering (§2) turns the comparison into a join-as-filter, which is only
  sound as a top-level AND conjunct; the detector accepts an uncorrelated scalar only when
  its enclosing comparison is a top-level conjunct of the WHERE (peeled like the EXISTS/IN
  fast path, `dispatch.rs:271-293`), and rejects it under OR / NOT / in a projection / inside
  CASE. (The **correlated** form is unaffected — it substitutes a column ref over a
  row-preserving LEFT JOIN and works in any position, including OR/CASE/COALESCE.)
- **Item 2 — reject an inner GROUP BY / HAVING.** Scalar-subquery detection requires the
  subquery body's projection to be a single aggregate call **and** `q.group_by` empty **and**
  `q.having` is `None`; otherwise `"a scalar aggregate subquery cannot carry its own GROUP BY
  or HAVING"`. (Prevents the multi-row-per-key cardinality violation / weight multiplication.)
- **Item 3 — constant-fold `COUNT` under `IS [NOT] NULL`.** When a `COUNT` scalar subquery is
  the direct operand of `Expr::IsNull` / `Expr::IsNotNull`, replace the whole null-test with
  the boolean constant (`IS NULL → 1=0`, `IS NOT NULL → 1=1`) and **skip decorrelating that
  occurrence** (a COUNT is never NULL). Without this the COUNT substitution `COALESCE(<a0>,0)`
  under `IS NULL` would reach `bind_null_test` on a computed expression → the internal
  `"expected a column reference"` error (`structural.rs:374-378`, `ast_util.rs:266-273`). A
  `SUM/MIN/MAX/AVG` subquery under `IS [NOT] NULL` needs no fold — it substitutes to a bare
  column and `<a0> IS NULL` binds as an ordinary column null-test.
- **Item 4 — reject float in the uncorrelated form (both operands).** §2 keys a join on
  `(outer_col, agg)`; both are keys, and `validate_join_key_pair` rejects a float on either
  side (`predicates.rs:125-128`). Reject up front, with a scalar-specific message, when the
  aggregate is float-typed (AVG always; SUM/MIN/MAX over a float source — decidable via
  `agg_result_type`) **or** the outer operand column is float. (The correlated form is
  exempt: the aggregate is a payload column, never a key.)
- **Item 5 — reject a `COUNT` subquery only in a *non-last* COALESCE operand.** `bind_coalesce`
  binds the **last** operand via `bind_structural` (accepts the computed COUNT repair) but
  every **non-last** operand via `bind_null_test` (rejects it) (`structural.rs:301-324`). So
  `COALESCE(val, (SELECT COUNT…))` is allowed; only a `COUNT` subquery in a non-last COALESCE
  position is rejected, with a targeted message. (`SUM/MIN/MAX/AVG` substitute to a bare
  column and are unrestricted in COALESCE.)
- **Item 6 — ANY/ALL existence conjunct.** The lowering emits `m IS NOT NULL AND x OP m`
  (ANY) / `m IS NULL OR x OP m` (ALL), per §3 — not the bare `x OP m` shorthand.

### 5. Classification / dispatch and synthetic naming

- **Classification.** In `ViewShape::classify` (`dispatch.rs:203-311`), after the EXISTS/IN
  fast-path and mark-subquery checks and before `GroupBy`/`Simple`, add a
  `ViewShape::ScalarSubquery(select)` arm reached when the single-FROM body's WHERE or
  projection contains a scalar `Expr::Subquery(single-aggregate)` or an `Expr::AnyOp` /
  `Expr::AllOp` over a subquery. A view mixing an EXISTS/IN subquery (counted by
  `count_subqueries`) with a scalar subquery is rejected. `= ANY` / `<> ALL` nodes are
  rewritten to `InSubquery` before this check so they route to the existing IN/NOT IN path.
- **Nesting (depth-1).** The inner subquery's own scopes are resolved against the (outer,
  inner) alias pair only; a two-level-out column reference fails resolution with a depth-1
  error. An EXISTS/IN inside a scalar subquery's WHERE is rejected by the existing
  `count_side_refs` nested-subquery guard (`exists.rs:1012-1015`).
- **Synthetic naming.** Segment aliases `sq0, sq1, …` (valid identifiers, non-`_`-leading,
  distinct from the outer alias/name). Synthetic column names `<kM>`/`<aN>` for `Gᵢ`'s group
  and aggregate columns, generated distinct from the outer schema's visible column names
  (append a disambiguating suffix on collision), so the `H` and `final` layers reference them
  without clashing with user columns.

### 6. Binder / backstop

The targeted rejection arms in `bind_structural` (`structural.rs:209-212`) remain the
backstop for unsupported contexts (HAVING, DML, a direct top-level SELECT, a JOIN-FROM
outer): the new builder intercepts scalar `Subquery`/`AnyOp`/`AllOp` only for a single-table
CREATE VIEW body, and produces synthesized ASTs that bind through the untouched
`bind_single_table` / `SingleTable` leaf — **no change to the structural recursion or the
`LeafBinder` trait**. Scalar-subquery detection reuses the aggregate-binding vocabulary of
the grouped builder (`SingleTable::bind_function` + `reject_unsupported_fn_qualifiers`);
anything that is not a single bare aggregate call → `"a scalar subquery must be a single
aggregate over its correlation group"`.

## Sequencing

1. **Correlated scalar aggregates in WHERE** — `scalar.rs` builder + `ViewShape` arm; `Gᵢ`
   grouped-reduce synthesis, LEFT-join `H`, linear `final` with the WHERE substitution and
   COUNT repair (`COALESCE`). Planner units + e2e incl. the COUNT bug.
2. **Scalar aggregates in projections** — projection substitution through the linear `final`
   (bare col for SUM/MIN/MAX/AVG, `COALESCE` for COUNT). e2e transition tests.
3. **Uncorrelated comparisons** — `G_global` + equi / pure-range INNER join-as-filter, with
   the Item 1 top-level-conjunct and Item 4 float guards. e2e incl. empty-source ground
   behavior for `=` vs range vs COUNT.
4. **ANY/ALL rewrites** — `= ANY`/`<> ALL` → IN/NOT IN routing; range ANY/ALL MIN/MAX rewrites
   with the Item 6 existence conjuncts and the non-nullable-inner restriction. e2e truth
   tables.

Each step compiles and passes `make verify` + `make e2e` (W=4) independently.

## Files touched

| File | Change |
|------|--------|
| `crates/gnitz-sql/src/plan/view/scalar.rs` | **new** — `emit_scalar_subquery_pieces`: detection, correlation split, `Gᵢ`/`H`/`final` synthesis, subquery→column substitution, all §4 guards |
| `crates/gnitz-sql/src/plan/view/dispatch.rs` | `ViewShape::ScalarSubquery` arm + its dispatch; `= ANY`/`<> ALL` → IN/NOT IN pre-rewrite; mixed EXISTS+scalar rejection |
| `crates/gnitz-sql/src/plan/view/mod.rs` | register the `scalar` module |
| `crates/gnitz-sql/src/bind/structural.rs` | rejection-arm messages for `Subquery`/`AnyOp`/`AllOp` made context-specific (supported in a single-table view body, not here) |
| `crates/gnitz-py/tests/test_scalar_subquery.py` | **new** e2e suite |
| `crates/gnitz-py/tests/test_exists.py` | ANY/ALL cases (`= ANY` / `<> ALL` parity with IN / NOT IN) |

## Testing

- **Planner units** (`scalar.rs` + `dispatch.rs`): correlation-key extraction (multi-column,
  cross-width promotion); inner-local vs correlation vs outer-only conjunct classification;
  every rejection — non-aggregate scalar; inner GROUP BY / HAVING (Item 2); range correlation
  with an aggregate; float correlation column; float-typed aggregate **and** float outer
  operand in the uncorrelated form (Item 4 — `x = (SELECT AVG(y))` and `x < (SELECT MAX(f))`
  over float `f`, and `f = (SELECT COUNT(*))` over float `f`, must all give the scalar-specific
  message, not a join-key error); uncorrelated scalar under OR / in a projection (Item 1);
  uncorrelated `<>`; `= ALL`; `<> ANY`; uncorrelated `ALL`; nullable inner column for a range
  quantifier; two-level correlation; scalar in HAVING / DML / direct SELECT; a JOIN-FROM outer;
  a join / derived / grouped / DISTINCT inner; `COUNT` in a non-last COALESCE operand (Item 5);
  `COUNT` under `IS [NOT] NULL` folds to a constant (Item 3). Rewrite shapes: COUNT → `COALESCE`,
  SUM → bare col ref, `Gᵢ` synthesizing group-column projections, ANY existence conjunct, ALL
  disjunct.
- **E2E behavior** (W=4, weight-asserted, backfill + incremental for every scenario):
  - **COUNT bug**: outer rows with zero inner matches show `0` (not dropped, not NULL); insert
    the first inner row → transitions to `1` with a balanced retract/insert pair; delete it →
    back to `0`.
  - SUM/MIN/MAX/AVG: NULL over an empty group; MIN/MAX retraction of the current extremum with
    a **non-nullable** correlation key (the AggValueIndex fast path is gated on non-nullable
    group keys — a nullable key falls back to trace replay, correct but slower, so the AVI case
    is exercised deliberately); AVG float path (correlated form).
  - Scalar in WHERE comparisons crossing the threshold incrementally (rows entering / leaving
    the view as the group aggregate moves).
  - Projected scalar aggregate: a projected COUNT (`COALESCE` path over `H`) and a projected
    SUM/MIN/MAX/AVG (bare col), each with incremental transitions.
  - Uncorrelated: `x = (SELECT MAX…)` incl. empty source (no matches) and the `COUNT` ground
    (`x = 0` matches an empty source); range form against a moving global MAX.
  - ANY/ALL: full truth-table sweep per op — empty set, all-smaller, all-larger, mixed, NULL
    outer `x` — for correlated and (ANY-only) uncorrelated forms; `= ANY` / `<> ALL` parity
    with IN / NOT IN; a **negated** correlated ANY over an empty group returns the row (the
    Item 6 existence conjunct makes `NOT (x < ANY(∅))` TRUE).
  - Multiple scalar subqueries in one view (two correlated; correlated + uncorrelated),
    weight-asserted; the DROP-cascade of the hidden `Gᵢ`/`H` segments.
  - Nesting: a scalar aggregate whose correlation is one level deep; an EXISTS inside a scalar
    subquery's WHERE is rejected.
- **Perf**: `make bench` unchanged (no engine code touched); one e2e sanity timing on a
  scalar-subquery view chain, kept generous.

## Invariants preserved

- No engine / wire / opcode changes; every emitted circuit shape already exists (grouped
  reduce, global-ground reduce, equi / pure-range join, LEFT null-fill, linear filter/map),
  reached only through the existing `emit_group_by_pieces` / `plan_join_chain` /
  `emit_linear` builders.
- Provable scalar cardinality: every value-producing subquery materializes through a reduce
  keyed by its correlation columns (one row per key) or a `global_ground` reduce (one row
  total) — no runtime-error path is introduced.
- Grouped reduces keep `global_ground = false`; only the genuine uncorrelated global scalar
  sets it, preserving the pure-range join's empty-trace requirement.
- Weight-exactness of the LEFT-JOIN null-fill and the reduce retract/insert pairs is relied
  on, not modified.
- Single-source-per-epoch and the self-join guard hold: `Gᵢ`'s inner table and the outer are
  distinct source ids; a scalar subquery over the same base table the outer scans is the
  shared-source-branches shape (distinct dependency edges → separate epochs), already proven
  correct.
