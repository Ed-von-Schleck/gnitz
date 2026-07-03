# x2: bound logical IR + atomic hidden-view chains (multi-join substrate)

## Problem

The view planner is shape-based: `ViewShape::classify`
(`gnitz-sql/src/plan/view/dispatch.rs:99-151`) routes a CREATE VIEW body to one of five
builders, each hard-limited to one relational operator — one JOIN
(`join.rs:124-128` rejects `joins.len() != 1`), no WHERE on join views
(`join.rs:107-115`), no GROUP BY over joins, CTEs only as identity pass-throughs
(`dispatch.rs:158-274`), no derived tables. The engine's compiled-plan model
(`CompileOutput { pre, post, side_b }`, `gnitz-engine/src/query/compiler/mod.rs:147-161`)
holds at most one exchange stage per circuit, so a multi-operator query cannot become
one circuit — but chained views already cascade correctly and unboundedly at runtime
(`drive_dag`, `gnitz-engine/src/query/dag/mod.rs:1142-1210`; depth
`catalog/hooks.rs:465-479`; one client ACK per tick, exchange rounds command-only under
the SAL durability contract). Users can hand-build chains today; the planner cannot.

This plan replaces shape classification with a bound logical IR, a splitter that cuts
the IR into single-anchor segments, and an atomic multi-view DDL bundle that creates the
resulting chain — hidden intermediates plus the user-named final view — in one
`DDL_TXN`. It is a front-end + client change; the engine already ingests N-view bundles
(`handle_ddl_txn` collects all VIEW_TAB `+1` PKs, `runtime/orchestration/executor.rs:1428`,
and backfills each in batch order, `executor.rs:1678-1696`) and already permits co-drop
of dependent views in one batch (`write_path.rs:522-539`).

## Prerequisite state

The SemiAnti emitter and Sequencing step 6 reuse the shipped `[NOT] EXISTS`/`[NOT] IN`
support: the planner lowers a single subquery conjunct over a Simple-shaped view into
semi/anti circuits derived from the outer-join null-fill (`plan/view/exists.rs`,
dispatched via `ViewShape::Subquery`), and `bind_structural` desugars IN-lists and
rejects out-of-position subqueries with targeted errors. Re-verify two code-level facts
this plan leans on, both plausible targets of concurrent join-machinery work: the
distributed backfill tail resolves a view's sources via `get_source_ids`
(`executor.rs:1683`, `dag/mod.rs:493-496`) and replays them from their stores — if the
source-list accessor or join-trace sourcing has changed (e.g. base-table trace elision),
re-derive §3 against the then-current lookup; and `write_circuit_rows`' family layouts
(`client.rs:776-955`) are the assembler's template.

## Scope (committed)

SQL surface after this plan (CREATE VIEW bodies):

- **Multi-way JOIN**: `FROM t1 JOIN t2 ON … JOIN t3 ON … [JOIN …]`, left-deep in
  syntactic order (no join reordering), each step INNER/LEFT/RIGHT/FULL with today's
  per-step ON vocabulary (equality prefix + ≤1 range conjunct + INNER-only residual).
- **WHERE on join views** (single-anchor case compiles into the same circuit as a linear
  filter over the join output — no chain needed).
- **GROUP BY / HAVING / DISTINCT / set-ops over arbitrary sub-plans** (anchored inputs
  become hidden views).
- **Derived tables**: `FROM (SELECT …) AS d` — the sub-select lowers recursively.
- **General CTEs**: each `WITH x AS (SELECT …)` is lowered **per reference** — a linear
  CTE inlines its sub-tree, an anchor-bearing CTE lowers to its own hidden view at each
  reference site (referenced *N* times ⇒ *N* independent hidden views, each with a
  distinct source id). No node-level DAG sharing: `Rel` children stay unique-owned
  `Box<Rel>`, the splitter needs no pointer-identity interning, and two references of one
  anchor CTE joined together are two distinct sources (never a self-join). The extra
  store per duplicate reference is the accepted cost; cross-reference common-subexpression
  sharing is a separate, whole-planner increment. To bound the pathological
  self-referential-CTE blow-up (`WITH b AS (SELECT * FROM a JOIN a) …` doubling per
  level), the assembler rejects a bundle whose segment count exceeds `MAX_CHAIN_SEGMENTS =
  64` with a clean planner error. The pass-through-only CTE inliner (`dispatch.rs:158-274`)
  is deleted.
- **Multiple subquery conjuncts** and subquery conjuncts over any sub-plan: each
  `[NOT] EXISTS`/`[NOT] IN` conjunct becomes its own Semi/Anti anchor (per-conjunct
  restrictions unchanged from the current exists builder).
- **Self-joins and same-source set-ops**: when the *same pre-existing relation id* (a
  base table or a committed view) appears twice in one tree, the right occurrence is
  wrapped in an auto-generated hidden pass-through view, giving it a distinct source id —
  the documented two-views-over-one-base pattern (`test_shared_source_incremental.py`),
  now automatic. (Two references of one anchor CTE are already distinct hidden views per
  the per-reference rule above, so they never reach this path.) The extra store copy is
  the accepted cost.

Still rejected: ORDER BY/LIMIT in views, correlated scalar subqueries, ANY/ALL,
subqueries outside top-level WHERE conjuncts, join reordering, non-equi/range join
predicates beyond the existing vocabulary.

## Design

### 1. Name reservation and hidden-view convention

- Hidden views are named `__h{final_vid}_{i}` (`final_vid` = the user view's allocated
  id, `i` = topo index). Uniqueness is structural: `final_vid` is globally unique and
  the intra-bundle qname-uniqueness gap (`precheck_qname_unique` compares only against
  committed caches, `write_path.rs:201-214`) is never exercised.
- The planner rejects user-supplied names with a leading `_` for CREATE TABLE
  (`plan/ddl.rs:204`), CREATE VIEW (`dispatch.rs:27`), and CREATE SCHEMA by calling
  `gnitz_core::validate_user_identifier` (`gnitz-wire/src/catalog.rs:127-142`, which
  already rejects a leading `_`) right after `extract_name` — the same pattern as
  CREATE INDEX's `validate_user_index_name` wrapper (`plan/ddl.rs:609`). Today nothing validates these names in
  production (engine `catalog/ddl.rs` validator call sites are `#[cfg(test)]`-only;
  `precheck_sys_ingest` checks no name characters), so this is the single enforcement
  point. The chain assembler mints hidden names internally, below this check.
- DROP VIEW / DROP TABLE with a leading-`_` name is rejected planner-side the same way.
  Direct `SELECT * FROM __h…` keeps working (harmless, aids debugging; documented as
  unsupported surface). There is no SHOW VIEWS surface to filter.

### 2. The bound logical IR (`gnitz-sql/src/plan/lp.rs`, new)

All name resolution, clause validation, and predicate classification happen during
AST→IR lowering; IR nodes carry only resolved data. One committed node set:

```rust
pub(crate) enum Rel {
    /// `vis` is the user-visible column range. Full range for tables and user views;
    /// for a hidden join-segment view it excludes the leading synthetic PK region
    /// (`_join_pk × k`, `join.rs:438-448`, or `_pair_pk × pair_pk`, `join.rs:801-814`)
    /// — those columns are the hidden view's physical PK and cannot be projected away,
    /// but they must never leak into alias resolution, `SELECT *` expansion, or
    /// downstream logical column offsets. Every logical column index maps to physical
    /// as `vis.start + logical`; wildcard expansion iterates `vis` only.
    Source { tid: u64, schema: Rc<Schema>, vis: std::ops::Range<usize> },
    /// Linear filter; pred is bound against input's schema.
    Filter { input: Box<Rel>, pred: BoundExpr },
    /// Linear projection; items as in codec/project_schema (PassThrough | Computed).
    Project { input: Box<Rel>, items: Vec<ProjItem>, out_cols: Vec<ColumnDef> },
    /// One join step. eq/range/residual classified by extract_join_predicates
    /// against (left.schema, right.schema).
    Join {
        left: Box<Rel>, right: Box<Rel>, kind: JoinKind, // Inner|Left|Right|Full
        eq: EqKeys,                     // (left_cols, right_cols, target_tcs)
        range: Option<RangeConjunct>,
        residual: Option<BoundExpr>,    // INNER only; ANDed, bound at lowering vs the
                                        // logical [left.schema ++ right.schema]; the
                                        // emitter shifts its indices to physical (§2 map).
    },
    /// EXISTS/IN (negated = NOT …): the x1 semi/anti anchor over arbitrary inputs.
    SemiAnti { outer: Box<Rel>, inner: Box<Rel>, negated: bool, eq: EqKeys,
               range: Option<RangeConjunct> },
    Reduce { input: Box<Rel>, group_cols: Vec<usize>, aggs: Vec<AggSpec>,
             having: Option<BoundExpr>, out_cols: Vec<ColumnDef> },
    Distinct { input: Box<Rel>, cols: Vec<usize>, out_cols: Vec<ColumnDef> },
    SetOp { op: SetOperator, all: bool, left: Box<Rel>, right: Box<Rel>,
            out_cols: Vec<ColumnDef> },
}
```

Every node exposes `fn schema(&self) -> &Schema` (columns + `pk_cols`, computed at
construction — the `pk_cols` fixes each node's physical PK arity `k`).
Every node — `Join.residual` included — carries only resolved data: nothing survives as
raw AST past lowering, so no IR consumer ever needs the `AliasMap` again. The residual is
the one predicate that today binds at emit time (`build_residual_filter_prog`,
`predicates.rs:468`, resolves ON columns through the `AliasMap`, then shifts by the
`_join_pk` arity `k`). That `AliasMap` is gone after lowering, so the residual is instead
bound *during* lowering against the join's logical `[left.schema ++ right.schema]`
(indices `0..left_n+right_n`), ANDed into one `BoundExpr`, and re-indexed to physical at
emit by the general §2 mapping below — identical result, because a residual is INNER-only
and the merged `out_cols` clone the source `ColumnDef`s, so `fold_null_test` sees the same
nullability at either binding site.

**Lowering (AST → Rel)** replaces `ViewShape::classify`:

- `Query` envelope / `Select` clause validation reuses `reject_unhonored_query_clauses`
  / `reject_unhonored_select_clauses` unchanged.
- FROM: first item → `Source` (or recursive lowering for a parenthesized derived table);
  each `join` entry folds `Join { left = acc, right = lower(relation) }` left-deep,
  classifying its ON via `extract_join_predicates`. Alias scoping uses the existing
  `AliasMap` over **logical** (visible) column space: per join step the right alias's
  `col_offset` is the accumulated left *visible* width. When the splitter cuts the left
  subtree into a hidden view, every alias that resolved into it keeps its logical
  offset and the segment emitter adds the hidden `Source.vis.start` shift when
  compiling physical column indices (reindex cols, filter programs, projections) — so
  `a.x` two hops down resolves identically before and after the cut, and the synthetic
  PK region is invisible throughout.
- WHERE: split into top-level conjuncts (`flatten_conjuncts`); subquery conjuncts fold
  `SemiAnti` anchors (inner side lowered recursively — a subquery FROM may itself be a
  derived table); remaining conjuncts fold one `Filter` above the FROM tree. Placement:
  a conjunct pushes below a `Join` onto the side that resolves all its columns **only
  when the other side is not preserved** — INNER: both sides pushable; LEFT: left-side
  conjuncts push, right-side conjuncts stay above (they must see the null-fill: pushing
  `t2.x > 5` below `t1 LEFT JOIN t2` would null-fill-and-keep t1 rows whose only
  matches fail the predicate — the same asymmetry the outer+residual rejection encodes,
  `join.rs:203-211`); RIGHT: mirrored; FULL: nothing pushes. Above-the-join conjuncts
  compile as a post-null-fill linear `Filter` over the merged output (the residual
  splice point, `join.rs:475-484`, whose `out_cols` already carry outer-join
  nullability, `join.rs:454-467`), so 3VL over null-filled columns is correct.
- GROUP BY / aggregates → `Reduce` (reusing the grouped builder's binding: group cols,
  agg specs, HAVING); DISTINCT → `Distinct`; set-op bodies → `SetOp` with both sides
  lowered recursively.
- CTEs: keep the parsed `Query` per `WITH` name; each *reference* re-lowers that body
  into a fresh `Rel` sub-tree (linear or anchor-bearing). No node sharing — the splitter
  then cuts each anchor-bearing reference into its own hidden view. Distinct references
  therefore carry distinct source ids by construction.
- Same *pre-existing* source id appearing more than once anywhere in the tree (a base
  table or committed view — not two CTE references, which already differ): wrap **every
  occurrence after the first** in its own `PassThroughView(Source)`, each realized by the
  splitter as a distinct hidden linear view (distinct vid ⇒ distinct source id) whose input
  is that pre-existing relation. For `a JOIN a JOIN a`, both the 2nd and 3rd `a` are wrapped
  in separate hidden views — one shared wrapper would recreate a same-source join and trip
  the guard.

**Logical → physical column mapping (the emit-time contract).** Lowering resolves every
column to a *logical* index (0-based within a node's `schema()`); the emitter maps logical
→ physical once, at three points. The map is **per-input piecewise**, not a single scalar:
each of an anchor's inputs contributes its own leading synthetic-PK arity (its
`Source.vis.start`, 0 for a base table), and the emitter shifts a column by the cumulative
offset of the side it came from.

> **Per-side join mapping (load-bearing — a single scalar base is unsound).** A binary
> join's merged output is physically `[_pk × k_f, left.FULL…, right.FULL…]`, where each
> side's FULL width *includes that side's own synthetic-PK region* (`build_reindex_program`
> copies every input column, `_join_pk` included, into payload). With `left` carrying a
> synthetic PK of arity `k_left` (= its `vis.start`) and `right` of arity `k_right`, a
> **visible** column at logical index `L` maps as:
> - `L < left_visible_n` (left col): physical `= k_f + k_left + L`
> - `L ≥ left_visible_n` (right col): physical `= k_f + k_left + k_right + L`
>
> The split point is `left_visible_n`. For base-table inputs `k_left = k_right = 0`, so both
> collapse to today's single `k_f` shift (behavior-neutral golden tests hold). For a
> **left-hidden** input the shift is still uniform (`k_right = 0`). The break the single
> scalar would cause is **right-hidden** (or both-hidden): a `SELECT j.x FROM t JOIN (…join…)
> j` would emit `k_f + L` for `j.x` and read `j`'s `_join_pk` instead — wrong value + a
> synthetic-PK leak. So `build_join_view_projection`, the residual/tail shift, and wildcard
> expansion take this 2-piece shift keyed on `left_visible_n`, not one `base`. Non-join
> anchors (reduce/distinct/set-op/linear) have one input, so their map is the single
> `k` scalar as written below.

The single-input scalars are still the leading synthetic-PK arity `k` and, for a
hidden-view input, its `Source.vis.start`:

- **Anchor output PK arity `k`.** Each anchor emitter produces
  `[_pk × k, payload…]`; `k` is fixed by the anchor kind: equi join / equi semi-anti =
  eq-slot count (`_join_pk` at 0..k, `join.rs:439-448`, `exists.rs:485-493`); range join =
  `pair_pk` (`join.rs:764-815`); range semi-anti = outer PK arity `pa` (`exists.rs:689`);
  reduce = its reduce-PK arity; distinct/set-op = their PK arity; linear = source PK
  count. The emitter returns `k` alongside its circuit so downstream index math is exact.
- **Pre-anchor linear inputs.** WHERE-pushdown and derived tables put `Filter`/`Project`
  nodes *below* an anchor, above its `Source` leaves (`FROM (SELECT * FROM t WHERE x>5) d
  JOIN …`). An anchor input is therefore a linear chain terminating in one `Source`, not a
  bare id. A shared recursion compiles it, threading the leaf's tid so the join's
  self-join guard still sees two distinct sources:

  ```rust
  // Returns (input node feeding the anchor, the leaf Source's tid). `emit_projection`
  // is the shared map / map_expr projection emitter lifted from simple.rs:86-132.
  fn compile_linear_input(cb: &mut CircuitBuilder, rel: &Rel) -> (NodeId, u64) {
      match rel {
          Rel::Source { tid, .. } => (cb.input_delta_tagged(*tid), *tid),
          Rel::Filter { input, pred } => {
              let (n, tid) = compile_linear_input(cb, input);
              let prog = compile_bound_expr_to_program(pred, input.schema())?;
              (cb.filter(n, Some(prog)), tid)
          }
          Rel::Project { input, items, .. } => {
              let (n, tid) = compile_linear_input(cb, input);
              (emit_projection(cb, n, items, input.schema()), tid)
          }
          _ => unreachable!("splitter guarantees a linear chain over one Source here"),
      }
  }
  ```

- **Post-anchor linear tail.** The splitter's connected-subtree rule keeps the linear
  `Filter`/`Project` nodes *above* an anchor in the anchor's own segment (`WHERE` over a
  join output, `SELECT * FROM (…GROUP BY…) d WHERE c>5` ⇒ `Filter(Reduce)`,
  `count(*)+1`). Tails appear above **every** anchor kind, so the tail compiler is shared
  by all emitters, not just the join — it is exactly the residual-splice generalized
  (`join.rs:469-485`). Tail predicates were bound at lowering against the anchor's
  *logical* output; the emitter shifts them by `k` to reach the physical `[_pk × k,
  payload…]` layout:

  A column's shift is supplied by a `remap: &dyn Fn(usize) -> usize` closure so the
  single-input anchors pass `|c| c + k` while the join passes the 2-piece per-side map
  (above). `shift_bound_expr` must be **total** over `BoundExpr` — `AggCall.arg` can carry a
  `ColRef`, so the catch-all cloning it would silently leave that index unshifted (it is
  only latently safe today because ON/tail predicates reject aggregates; recurse anyway):

  ```rust
  fn shift_bound_expr(e: &BoundExpr, remap: &dyn Fn(usize) -> usize) -> BoundExpr {
      match e {
          BoundExpr::ColRef(c)    => BoundExpr::ColRef(remap(*c)),
          BoundExpr::IsNull(c)    => BoundExpr::IsNull(remap(*c)),
          BoundExpr::IsNotNull(c) => BoundExpr::IsNotNull(remap(*c)),
          BoundExpr::BinOp(l, op, r) =>
              BoundExpr::BinOp(shift_bound_expr(l, remap).into(), *op, shift_bound_expr(r, remap).into()),
          BoundExpr::UnaryOp(op, x) => BoundExpr::UnaryOp(*op, shift_bound_expr(x, remap).into()),
          BoundExpr::AggCall { func, arg } =>
              BoundExpr::AggCall { func: *func, arg: arg.as_ref().map(|a| shift_bound_expr(a, remap).into()) },
          lit => lit.clone(), // Lit* carry no column index
      }
  }

  // `out_schema` is the anchor's PHYSICAL output ([_pk × k, payload…]); tail predicates
  // and projection items were bound against the anchor's logical output, so shift by k.
  fn compile_linear_tail(cb: &mut CircuitBuilder, mut cur: NodeId, tail: &[LinearNode],
                         k: usize, out_schema: &Schema) -> NodeId {
      for node in tail {
          match node {
              LinearNode::Filter { pred } => {
                  let prog = compile_bound_expr_to_program(&shift_bound_expr(pred, k), out_schema)?;
                  cur = cb.filter(cur, Some(prog));
              }
              LinearNode::Project { items } => cur = emit_projection_shifted(cb, cur, items, k, out_schema),
          }
      }
      cur
  }
  ```

  (`compile_linear_tail` takes the same `remap` closure; the code above elides it for
  brevity.) The `Join.residual` is the first `compile_linear_tail` call on the join's merged
  output, with the **per-side** `remap` (left cols `+ (k_f + k_left)`, right cols
  `+ (k_f + k_left + k_right)`, split at `left_visible_n`); each side's synthetic-PK region
  is copied into the payload by `build_reindex_program` but is never a logical column.
  Because the shift is a pure function of `(k_f, k_left, k_right, left_visible_n)`, a segment
  whose input was cut into a hidden view compiles byte-identically to one over a base table
  (where `k_left = k_right = 0`) — the synthetic PK stays invisible end to end.

### 3. The splitter (Rel → Vec<Segment>)

An **anchor** is a `Join`, `SemiAnti`, `Reduce`, `Distinct`, or `SetOp` node. Committed
rule, applied bottom-up:

- A **segment** is a maximal connected subtree containing **exactly one anchor**, plus
  the linear (`Filter`/`Project`) nodes directly above it up to (a) the next anchor's
  input boundary or (b) the root.
- When an anchor's input subtree itself contains an anchor, that input becomes its own
  segment emitted as a **hidden view**; the consuming node's input is replaced by
  `Source { tid: hidden_vid }`. Linear nodes *between* two anchors belong to the
  **upstream** segment (they compile after the upstream anchor, before its sink), so
  hidden stores are filtered/projected as small as possible before materialization.
- A tree with no anchor is one linear segment (the existing Simple view compilation).
- The root segment gets the user's name; all others get `__h{final_vid}_{i}` in
  emission (topological) order.

Consequences: a single-anchor query (join + WHERE + projection; GROUP BY + HAVING;
one EXISTS + locals) compiles to **one** view exactly as today, plus the new
linear-tail capability (WHERE over join output = a linear `Filter` compiled onto the
join circuit before projection, precisely how INNER residuals already compile,
`join.rs:469-484`). Chains appear only for genuinely multi-anchor queries.

**Backfill-order invariant (load-bearing):** an anchor-bearing hidden view carries a
`Join` node or an `ExchangeShard`, so `view_seeds_exchange_backfill`
(`dag/mod.rs:828-840`) is true and it takes the **distributed** backfill tail
(`executor.rs:1674-1696`); a linear pass-through hidden view takes the **inline** path
(`hook_view_register`, gated on `!view_seeds_exchange_backfill`). A chain requires the
distributed tail to fill an upstream hidden view before a downstream one scans it. The
assembler **emits VIEW_TAB rows in topological order** — a standing contract, since the
register hook computes each view's `depth` from its already-registered sources
(`hooks.rs:465-470`, `unwrap_or(0)` for a not-yet-seen source) and that depth seeds boot
rebuild ordering. But the *live backfill tail* need not also couple to row order: today it
iterates raw VIEW_TAB batch order (`family_pks_by_sign`, `executor.rs:1428`), and the plan
originally guarded that coupling with a pre-commit topo assertion — which cannot run
cleanly (the dep-map is unpopulated before the ingest loop; asserting after it is
post-fsync, unabortable, and boot-loop-prone). Instead, **the tail topologically sorts
`new_view_ids` by their intra-bundle dependencies** (`get_source_ids`, which the tail
already calls at `executor.rs:1683`; the dep-map is populated during the ingest loop,
DEP_TAB applying before VIEW_TAB in topo priority). Backfill materialization order then
holds independently of batch row order, no new pre-commit check is added, and boot
recovery already re-derives order from the dep-map. Anchor-view compilation is likewise
order-safe: the register hook skips the inline branch for every seeding view
(`!view_seeds_exchange_backfill`, `hooks.rs:504`) and compiles it at the tail when all
bundle views are registered. A deliberately row-misordered bundle is the regression test:
its live backfill must still materialize correctly.

The inline pass-through is safe because its source **pre-exists**: the inline
`backfill_view` is skipped on the master (its `active_part_start == active_part_end` gate,
`hooks.rs:502`) and runs on **each worker** during `ddl_sync` hook firing
(`store_io.rs:443`), filling the worker's own partition from a base table or committed
view — never a same-bundle hidden view, so it never races the distributed tail. A
`debug_assert` in the splitter pins that a linear hidden segment's input is a pre-existing
relation id; under the per-reference CTE rule (§2) this holds universally, because the
only linear hidden view is the same-pre-existing-source pass-through — an anchor CTE
referenced twice becomes two distinct hidden views, never a pass-through over a
same-bundle exchange view.

### 4. Segment emitters (refactor of `plan/view/*`)

The five shape builders are dismantled into emitters keyed by anchor kind, each producing
`(Circuit, Vec<ColumnDef>, Vec<u32> /* pk */)` from a segment. A segment's inputs are
**linear `&Rel` sub-trees** (a `Source`, or `Filter`/`Project` chains over one `Source`),
not bare relation ids: each emitter obtains its anchor input node(s) via
`compile_linear_input` (§2) — so a WHERE pushed to one join side or a derived table's
inner filter is compiled in place — and each emitter finishes by threading its anchor
output through the shared `compile_linear_tail(cb, anchor_out, tail, k, out_schema)` (§2)
before `cb.sink`, passing its own physical PK arity `k` and output schema:

- `emit_linear` — from `simple.rs` (filter + projection/expr-map); `k` = source PK count.
- `emit_join` — from `join.rs` (equi + range/band, all join kinds, residuals, null-fill);
  `k` = eq-slot count (equi) or `pair_pk` (range). The residual is the first tail entry;
  the generalized tail also carries any post-join `Filter`/`Project` (the residual-splice
  point, `join.rs:469-485`, generalized to any bound filter/projection over the merged
  schema).
- `emit_semi_anti` — from `plan/view/exists.rs`; `k` = eq-slot count (equi) or outer PK
  arity `pa` (range) — `_join_pk`/`_src_pk` sits at slot 0, so a tail over the outer
  columns shifts by that `k`.
- `emit_reduce` — from `group_by.rs`; `k` = reduce PK arity. HAVING stays the
  reduce's own post-reduce filter; a tail is any *further* `Filter`/`Project` above it.
- `emit_distinct` / `emit_set_op` — from `set_op.rs`; `k` = their PK arity.

Every emitter therefore compiles pre-anchor linear inputs and post-anchor linear tails
uniformly — the linear-tail capability is not join-specific.

`execute_create_view` becomes: lower → split → for each segment allocate a vid
(`alloc_table_id`, one round-trip each — accepted; N is small) → emit circuits (hidden
vids are known before emission so downstream segments reference them) → assemble → one
`create_view_chain` call. `ViewShape` and the per-shape `execute_create_*_view` entry
points are **deleted** (their bodies live on as emitters); `dispatch.rs` keeps only the
envelope validation and the new pipeline. No legacy path remains.

Hidden-view `sql_text`: the printable sub-select does not exist for synthesized
segments; store the user's full statement text in the final view's VIEW_TAB row and
`"-- internal segment {i} of __h-owner {final_vid}"` for hidden rows (`sql_definition`
is informational; nothing re-parses it — verified: only stored/returned by
`find_view_record`, `client.rs:1147-1166`).

### 5. Atomic chain creation (`gnitz-core/src/client.rs`)

New public API, generalizing `write_circuit_rows` (`client.rs:776-955`):

```rust
pub struct PlannedView<'a> {
    pub name: &'a str, pub sql_text: &'a str, pub circuit: Circuit,
    pub output_columns: &'a [ColumnDef], pub pk_cols: &'a [u32],
}
/// Creates all views in one atomic DDL_TXN. `views` must be in dependency
/// (topological) order — VIEW_TAB row order sets each view's registration
/// `depth`; the distributed backfill tail re-derives materialization order
/// from the dep-map (§3). Returns the vids in input order.
pub fn create_view_chain(&mut self, schema_name: &str, views: &[PlannedView<'_>])
    -> Result<Vec<u64>, ClientError>
```

Implementation: per-view pre-flight validation identical to `write_circuit_rows`
(`789-822`), then **one `BatchAppender` per family across all views** — COL_TAB rows for
every view, DEP_TAB rows for every view (compound `(vid, dep_tid)` PK packing unchanged,
`client.rs:851-866`), CIRCUIT_NODES/EDGES/NODE_COLUMNS for every circuit, and one
VIEW_TAB batch with all `+1` rows **in input order** — then a single
`push_ddl_txn(&families)`. One families entry per tid is mandatory: the engine's
derived lists read only the first block per family
(`family_pks_by_sign`/`.find`, `executor.rs:1355,1428,1488,1545`).
`write_circuit_rows` becomes `create_view_chain` with one element (single definition;
the old body is deleted).

`create_view_chain` rejects a `views` slice longer than `MAX_CHAIN_SEGMENTS` up front (the
per-reference CTE blow-up guard, §Scope), before any allocation.

Atomicity: the existing per-family `precheck → mark → apply` loop
(`executor.rs:1576-1591`) plus `compensate_stage_a` (`executor.rs:1592-1607`,
`write_path.rs:748-819`; its weight-homogeneity `debug_assert` holds for an all-`+1`
chain) already gives all-or-nothing; a mid-chain failure (e.g. user-name collision
caught by `precheck_qname_unique`) rolls back every applied family. The only engine
change on this path is the distributed backfill tail's dependency-order sort (§3), which
replaces the tail's reliance on VIEW_TAB row order.

### 6. Cascading DROP

`Client::drop_view` (`client.rs:957-981`) is extended: after resolving the named view's
record, scan VIEW_TAB for records whose name starts with the **separator-terminated**
prefix `__h{vid}_` in the same schema (the trailing `_` is load-bearing: `__h5_` must
not match `__h51_0`; enforced by construction and pinned by a unit test) — ownership is
name-encoded; hidden views are never shared across user views — and build **one**
VIEW_TAB batch with `-1` rows for the user view and every hidden member, full payload
reproduced per record as today, in one `push_ddl_txn`. The engine's co-drop carve-out admits the
batch because every dependent is present in the same batch's `drop_ids`
(`write_path.rs:522-539`); `hook_view_register`'s `-1` branch cascades each view's
circuit/deps/columns internally (`hooks.rs:508-522`).

`drop_schema`'s member drain (`client.rs:580-602`) now sees hidden views in its VIEW_TAB
scan; dropping a user view removes its chain, and a subsequent direct drop attempt on an
already-removed hidden member must be tolerated: `drop_view` returns `Ok` when
`find_view_record` finds nothing during a drain (new `drop_view_if_exists` used by
`drain_drops`; the strict `drop_view` keeps erroring for users). Hidden members reached
directly by the drain before their owner are protected by the engine dependency guard
and retried later — the existing retry-until-stable loop already handles order.

## Sequencing

1. **Name reservation**: `validate_user_identifier` at CREATE TABLE/VIEW/SCHEMA + DROP,
   planner unit tests, e2e error-message tests.
2. **`create_view_chain`** (client, replacing `write_circuit_rows`) **+ the backfill
   tail's dependency-order sort** (engine, `handle_ddl_txn`). Rust integration test via
   `gnitz-test-harness`: two hand-built chained circuits (linear-over-base + join-over-it)
   created in one bundle over pre-populated bases; assert atomic rollback on a name
   collision in the second view; assert backfill correctness at W=4, including a
   deliberately **row-misordered** bundle (hidden view after its consumer in VIEW_TAB
   order) that must still backfill correctly via the tail sort.
3. **Cascading DROP** (`drop_view` + `drop_view_if_exists` + drain tolerance), e2e:
   drop user view → hidden members gone; DROP TABLE under a chain still RESTRICTs.
4. **IR + lowering + emitters, behavior-neutral**: all currently-supported view shapes
   route through lower→split→emit and produce circuits the engine compiles identically.
   Lands the shared `compile_linear_input` / `compile_linear_tail` / `shift_bound_expr`
   helpers (§2) and the pre-bound `Join.residual`; `ViewShape` and the CTE inliner are
   deleted. Gate: full existing e2e suite green; circuit golden tests (node/edge multiset
   comparison) for one view of each legacy shape — the residual golden pins the pre-bind +
   shift equals the old emit-time bind byte-for-byte.
5. **Multi-anchor surface**: multi-way joins, WHERE-over-join, GROUP-BY-over-join,
   DISTINCT/set-ops over sub-plans, derived tables, general CTEs (per reference: linear
   inline, anchor CTE → a hidden view at each reference).
6. **Multiple subquery conjuncts + subqueries over sub-plans** (SemiAnti anchors
   composed by the splitter).
7. **Self-join pass-through** wrapping + tests.

Each step compiles and passes `make verify` + `make e2e` independently.

## Files touched

| File | Change |
|------|--------|
| `crates/gnitz-sql/src/plan/lp.rs` | new: `Rel` IR, lowering, splitter |
| `crates/gnitz-sql/src/plan/view/dispatch.rs` | classification deleted; lower→split→emit pipeline; name validation |
| `crates/gnitz-sql/src/plan/view/{simple,join,exists,group_by,set_op}.rs` | entry points become segment emitters over linear `&Rel` inputs; shared `compile_linear_input` / `compile_linear_tail` / `shift_bound_expr` helpers |
| `crates/gnitz-sql/src/plan/ddl.rs` | CREATE TABLE/SCHEMA name validation; DROP leading-`_` rejection |
| `crates/gnitz-core/src/client.rs` | `create_view_chain` (replaces `write_circuit_rows`), cascading `drop_view`, `drop_view_if_exists` |
| `crates/gnitz-engine/src/runtime/orchestration/executor.rs` | distributed backfill tail sorts `new_view_ids` in intra-bundle dependency order (`get_source_ids`) instead of trusting VIEW_TAB row order |
| `crates/gnitz-engine/tests/` (or `test_support`) | chain-bundle integration test via `gnitz-test-harness` |
| `crates/gnitz-py/tests/test_multi_join.py` | new e2e suite (multi-join, group-by-over-join, derived tables, CTEs, chains) |
| `crates/gnitz-py/tests/test_catalog.py` | hidden-view drop/cascade/visibility cases |

## Testing

- **Planner units**: lowering of each SQL form to the expected `Rel` tree; splitter
  segment boundaries (linear folds upstream; one anchor per segment; pass-through
  wrapping; an anchor CTE referenced twice = two independent hidden segments;
  `MAX_CHAIN_SEGMENTS` rejection); name-reservation rejections.
- **Golden circuit tests**: every legacy single-anchor shape emits a circuit with the
  same node/edge multiset as before the refactor.
- **Client/engine integration**: multi-view bundle atomicity (success, mid-bundle
  precheck failure → zero residue, crash-recovery replay of a committed chain zone);
  co-drop bundle; multi-block-per-family regression guard (assert assembler emits one
  entry per tid).
- **E2E behavior** (W=4, weight-asserted): 3- and 4-way joins incl. outer steps;
  `WHERE` over join views; `GROUP BY` over a join with incremental retractions crossing
  group boundaries; EXISTS + EXISTS + local WHERE in one view; derived table with
  aggregate inside; an anchor CTE referenced twice (two independent hidden views, two
  stores, joined as distinct sources); self-join of a base table; chain creation over
  populated bases (backfill order) and boot-restart rebuild of a chain; `DROP VIEW`
  cascade; `DROP TABLE` blocked under a chain.
- **Perf**: `make bench` before/after step 4 (behavior-neutral refactor must not move
  view-maintenance throughput beyond the documented ±3-4% e2e noise); a chain micro-lag
  check (per-hop cascade cost) via a 3-hop chain e2e timing assertion kept generous.

## Invariants preserved

- One `CompileOutput` per segment: the splitter guarantees ≤1 anchor per segment (a
  binary set-op anchor legitimately carries its parallel `side_b` second exchange —
  that is one `CompileOutput`, not two stages); `CompileOutput` and
  `execute_multi_worker_step` are untouched.
- Single-source-per-epoch and the self-join guard: every segment's two sources remain
  distinct relation ids; the cascade delivers one `(view, source)` epoch per edge.
- SAL durability contract: chain creation is one durable DDL zone; backfill exchange
  rounds stay command-only.
- Engine wire format, opcodes, and `precheck_sys_ingest` semantics unchanged; the
  engine-side co-drop and N-view ingest paths are exercised, not modified. The sole
  engine change is the distributed backfill tail's dependency-order sort of `new_view_ids`
  (§3) — same `fan_out_backfill` calls, reordered; the `precheck → mark → apply`
  atomicity, zone durability, and hooks are untouched.
- Base-table positivity, (PK, payload) identity, and OPK conventions untouched.
