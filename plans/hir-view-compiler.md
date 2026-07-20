# A unified HIR pipeline for the view-body compiler

## 1. Problem

`gnitz-sql`'s view-body compiler (`plan/view/` + `plan/lp.rs`, ~7,400 non-test LOC) has no
intermediate representation between the sqlparser AST and the `CircuitBuilder` call sequence.
`ViewShape::classify` (`plan/view/dispatch.rs:363-476`) sorts every CREATE VIEW body into one of
eight shapes (`SetOp`, `Distinct`, `Join`, `Subquery`, `MarkSubquery`, `ScalarSubquery`,
`GroupBy`, `Simple`) by a fixed-precedence ladder, and each shape is compiled by a monolithic
emitter that works directly on the raw AST:

- `plan/lp.rs` defines a three-node `Rel` IR (`Source`/`Filter`/`Project`) consumed **only** by the
  linear path (`simple.rs:35-58`). Every other emitter — `join.rs` (1911 L), `group_by.rs`
  (1136 L), `scalar.rs` (1254 L), `exists.rs` (1008 L), `set_op.rs` (436 L) — pattern-matches
  `Select`/`SetExpr`/`Expr` directly.
- `scalar.rs` emits **zero** circuit ops: it decorrelates scalar-aggregate subqueries by
  *synthesizing new sqlparser AST* (`blank_select`, `fn_call`, `table_factor` constructors,
  `scalar.rs:1124-1254`) and re-entering the classifier through `plan_join_chain` /
  `emit_group_by_pieces` / `emit_linear`. The AST is the de-facto IR, so decorrelation costs a
  round trip through name re-resolution against schemas the compiler itself just built.
- Composition exists only along hand-wired edges (`compile_hidden_body` `dispatch.rs:638-685`,
  `resolve_operator_input` `:694-722`, `compile_derived_tables` `:761-824`,
  `compile_join_to_hidden` `:832-857`). Every missing edge is
  an explicit `Unsupported`: EXISTS/IN/scalar subqueries over a compiled CTE or derived table
  (`dispatch.rs:225-252`), set-op sides whose FROM is an inline join or derived table
  (`set_op.rs:22-35` — a *CTE alias* side already resolves through the binder), any set operation
  whose side is itself a set operation, at most one subquery per view (`dispatch.rs:424-428`), no
  subquery inside a subquery (`exists.rs:996-1000`), no derived table inside a derived table
  (`dispatch.rs:657-663`). Each missing edge is a bespoke multi-commit design when it is wanted —
  the mechanism (hidden chain segments) exists, but it has no general driver, so every edge
  re-derives segment cutting, schema plumbing, and re-binding by hand.
- The measured LOC split of the cluster: ~35% AST pattern-matching/classification, ~20%
  schema/column bookkeeping, ~17% validation, and only ~28% actual circuit emission. The
  bookkeeping is duplicated per shape — the synthetic hidden-PK `ColumnDef` construction, the
  WHERE-elided-when-const idiom, and the duplicate-name check each appear at four to seven
  sites, with no single home for any of them.

Patching the idioms into shared helpers and hand-wiring the next two or three composition edges
would shrink the duplication but leave both root causes standing: the sqlparser AST as the
compiler's working representation (with its re-resolution round trips and `Select`-cloning
surgery), and per-edge composition as the growth mode (8 shapes × N edges, each a bespoke
design). The fix class is the representation, not the call sites.

No off-the-shelf front end fits: the one candidate over the sqlparser AST
(`datafusion-sql`/`datafusion-expr`) mandates an Arrow type system irreconcilable with
fixed-width ints, German strings, OPK keys, and Z-set weights, drags ~1M transitive SLoC into a
crate shipped as a C ABI and a pyo3 wheel, and would still leave the hard part — the
LogicalPlan→Circuit lowering with weight/key/linearity reasoning — to be written from scratch.
Every comparable in-house system (Materialize, RisingWave, Databend, Feldera-via-Calcite)
converged instead on: parse → bind into a typed, compositional HIR with subqueries as
first-class nodes → decorrelating rewrites → one generic lowering. This plan builds that
pipeline for gnitz-sql, sized to what the engine can actually execute.

What is **not** the problem and is **not** touched: the statement dispatcher and the
`reject_unhonored_*` envelope validators (`plan/validate.rs:186,319,376` — the exhaustive
destructures are deliberate upgrade armor), the catalog `Binder` and per-statement snapshot
(`bind/resolve.rs`), the single structural expression walk (`bind/structural.rs` — its logic is
kept; `bind_structural` and the `LeafBinder<R>` trait are already generic over the leaf
reference type, shipped alongside `BExpr<R>`, so the view leaves can produce `HirExpr` while
every existing leaf keeps returning the `usize` instantiation),
the opcode backend (`lower.rs`) and the client interpreter (`exec/eval.rs`) — both keep consuming
the runtime `BoundExpr` unchanged — DDL/ALTER (`plan/ddl.rs`, `plan/alter.rs`), the DML verbs,
`codec/`, the wire protocol, and the engine. The engine executes every circuit this plan emits;
no engine change is part of this plan.

## 2. The committed design

One new module tree, `src/hir/`, replaces the classifier and all per-shape AST handling:

```
src/hir/mod.rs         RelExpr, ColId, smart constructors, on-demand logical-schema
                       derivation (cols stored only on Get), shared col_by_id/slot_of
src/hir/bind.rs        Query/Select → RelExpr (all name resolution, scoping, typing)
src/hir/rewrite.rs     HIR→HIR passes: ANY/ALL, decorrelation, predicate classification,
                       column pruning
src/hir/physical.rs    the one positional pass: ColId → column index (resolve_refs via the
                       generic BExpr::try_map_refs), the fold_preds AND-fold, projection
                       physicalization
src/hir/lower/mod.rs   RelExpr → ViewChain segments: cut rules + per-node emission driver;
                       the linear path resolves to a plan/lp.rs::Rel and delegates to
                       simple::emit_linear (one home for strategy + seeding rule)
src/hir/lower/         join.rs (shipped), reduce.rs, setop.rs, exists.rs, predicates.rs —
                       per-node orchestration shells; they call the retained plan/view/
                       emission primitives cross-module during migration, which relocate
                       here at the §5 sweep (§2.5 "what moved means")
src/access.rs          the PK/index access-path recognizers over bound conjuncts (one home,
                       shared by the HIR lowering's scan-bound step and dml's ReadSpec/mutate
                       planning) — shipped
```

`plan/view/` is deleted at the end of the migration (§5). `execute_create_view` and
`execute_alter_view` keep their signatures and callers; their
bodies become `bind → rewrite → physicalize-on-demand → lower`. `ViewChain`
(`plan/view/mod.rs:38-130`) survives unchanged as the segment sink — it is already the correct
dependency-ordered, atomic-commit DAG builder; what it lacked was a general driver.

### 2.1 Column identity: `ColId`, and the split between logical and physical

Every column produced anywhere in one compilation gets a **stable opaque id**:

```rust
// hir/mod.rs
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColId(u32); // unique within one bind_query invocation
```

All references in the HIR — filter predicates, join keys, group columns, aggregate arguments,
projection inputs, correlation pairs — are `ColId`s, never positional indices. Ids are minted in
bind and are **never renumbered**: rewrites splice and prune freely without invalidating any
reference. Positional indices, key-leading column order, synthetic key slots, and hidden-column
flags are **physical** facts that exist only in `hir/physical.rs` (§2.4), which runs after all
rewrites. This is the discipline that makes "resolved once, never re-resolved" actually true
under structural rewrites; the code this plan deletes needed `Provenance`'s manual positional
bookkeeping (`plan_join_chain`, `join.rs:491`; `Provenance` at `join.rs:360`, offsets tracked via
`items.len()` / `acc_schema.columns.len()` growth) for the same reason, per shape and by hand.

A node's **logical schema** is a list of columns with identity and type but no layout:

```rust
pub(crate) struct HirCol {
    pub id: ColId,
    pub def: gnitz_core::ColumnDef, // name, type_code, nullable, is_hidden
}
```

`RelExpr` — the relational IR:

```rust
pub(crate) enum RelExpr {
    /// A catalog relation, or (during lowering) an already-cut hidden segment.
    /// `cols` mirrors the registered schema 1:1 (same order); `from_catalog`
    /// gates index-bound extraction, as `Binder::is_catalog_relation` today.
    Get {
        tid: u64,
        schema: Rc<Schema>,          // the registered physical schema (leaf anchor)
        cols: Vec<HirCol>,
        from_catalog: bool,
        // The scan bound is NOT a Get field. A linear view has exactly one Get
        // (its primary source), and the bound is always a primary-position
        // decision made at that Get's emit site — where the client, tid, schema,
        // and resolved WHERE are all in hand — threaded straight into
        // `input_delta_bounded`. It is a lowering-local (in this box and every
        // later one), not stored back into the Rc-shared immutable node.
    },
    Filter { input: Rc<RelExpr>, preds: Vec<HirExpr> },
    Project { input: Rc<RelExpr>, items: Vec<ProjEntry> },
    Join {
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
        /// The shipped code reuses plan/view/join.rs::JoinType (Inner | Left |
        /// Right | Full) — one enum, no parallel HIR kind. Semi | Anti |
        /// Mark { mark: HirCol } extend it in the subqueries commit.
        kind: JoinType,
        /// Raw ON conjuncts over left ∪ right ColIds. The predicate-
        /// classification rewrite (§2.3 pass 3) replaces this with `classified`.
        on: Vec<HirExpr>,
        /// Filled by pass 3; `None` before it.
        classified: Option<JoinClass>,
    },
    Reduce {
        input: Rc<RelExpr>,
        group_cols: Vec<ColId>,
        /// Raw reduce outputs, exactly what the engine reduce emits: per aggregate
        /// a raw value column plus, for AVG and nullable SUM, the COUNT_NON_NULL
        /// companion. HirAgg = { func, arg: Option<ColId>, out: HirCol,
        /// companion: Option<HirCol> }. Named HirAgg because group_by.rs's private
        /// physical `AggSpec { op, col, out_type }` is a different type and keeps
        /// its name in the retained file.
        aggs: Vec<HirAgg>,
        /// The hidden cardinality-COUNT companion, when the strategy requires it.
        ground: Option<HirCol>,
    },
    Distinct { input: Rc<RelExpr> },
    SetOp {
        op: SetOpKind, // Union | Intersect | Except
        all: bool,
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
        /// Per-side column pairing + promoted common types
        /// (`set_op_common_type`, set_op.rs:45-51), and the output HirCols.
        out: Vec<SetOpCol>, // { left: ColId, right: ColId, out: HirCol /* promoted tc */ }
    },
}

pub(crate) struct ProjEntry {
    pub expr: HirExpr,
    pub out: HirCol, // name, inferred type, hidden flag — one unit, no parallel vectors
}

pub(crate) struct JoinClass {
    pub eq: Vec<EqPair>,          // { left: ColId, right: ColId, tc: TypeCode }
    pub range: Option<HirRange>,  // { left: ColId, right: ColId, op: RangeRel, tc: TypeCode }
    pub residual: Vec<HirExpr>,   // INNER/Semi-Anti-Mark only; outer+residual rejected as today
}
```

Every node's logical schema (`fn cols(&self) -> Vec<HirCol>`) is **derived on demand** —
stored only on `Get`, whose ids are minted once and must stay stable; every other node
computes it, so no rewrite ever clones or re-derives stored cols. The derivation:
`Filter` passes through; `Project` is its `ProjEntry.out`s; `Join` is left ++ right
(++ mark for Mark; left-only for Semi/Anti) with the **null-providing side's** nullability
widened — the right side for Left, the left side for Right, both for Full
(`combined_payload_coldefs`, `join.rs:65-82`); `Reduce` is group cols ++ the **raw** agg
columns (each `HirAgg`'s `out` and `companion`, plus `ground` — the raw reduce output, not the
finalized SELECT shape; §2.2); `SetOp` is the promoted `out` cols with the per-operator output nullability of `set_op.rs:391-395`
(Union: `l || r`; Intersect: `l && r`; Except: `l`); `Distinct` passes through. Constructors
also run the semantic validations that guard the node (aggregate-argument typing, MIN/MAX
orderability, set-op arity and type compatibility) — one home each. The strict
grouped-projection validator is the one guard that cannot live in a constructor (it needs the
SELECT list, which the `Reduce` node never sees); it fires in bind (§2.2).

**Expressions.** `ir.rs`'s expression enum is already generic over its leaf reference type
(shipped): `IsNull`/`IsNotNull` carry a bare column ref, `AggCall`/`Case`/`InList` recurse
through `Box<BExpr<R>>` bodies, and `LitWide` carries an out-of-i64-range integer literal's
raw magnitude string (consumed by the `src/access.rs` recognizers, rejected by the one
`lower_bound_expr` arm everywhere else):

```rust
// ir.rs — shipped
pub(crate) enum BExpr<R> {
    ColRef(R), LitInt(i64), LitFloat(f64), LitStr(String), LitWide(String), LitNull,
    BinOp(Box<BExpr<R>>, BinOp, Box<BExpr<R>>), UnaryOp(UnaryOp, Box<BExpr<R>>),
    IsNull(R), IsNotNull(R),
    AggCall { func: AggFunc, arg: Option<Box<BExpr<R>>> },
    Case { branches: Vec<(BExpr<R>, BExpr<R>)>, else_: Option<Box<BExpr<R>>> },
    InList { inner: Box<BExpr<R>>, items: Vec<BExpr<R>> },
}
pub(crate) type BoundExpr = BExpr<usize>;   // the runtime IR — unchanged for
                                            // exec/eval.rs, lower.rs, dml/
// hir/mod.rs — HirExpr and HirRef::Col shipped; the Subquery variant arrives
// with the subqueries commit. BExpr's shipped leaf walkers — try_map_refs
// (leaf substitution; resolve_refs is its ColId→usize instantiation) and
// for_each_ref (reference collection) — are the two generic walks every HIR
// pass uses; the Mark substitution reuses try_map_refs.
pub(crate) type HirExpr = BExpr<HirRef>;
pub(crate) enum HirRef {
    Col(ColId),
    /// Exists only between bind and decorrelation; `resolve_refs` (§2.4)
    /// rejects any survivor, so the runtime BoundExpr can never contain one.
    Subquery(Box<SubqueryRef>),
}
pub(crate) struct SubqueryRef {
    pub kind: SubqueryKind, // Exists { negated: bool } | Scalar { coalesce_zero: bool }
    pub rel: Rc<RelExpr>,
    /// Correlation conjuncts: predicates over outer ∪ inner ColIds (an outer
    /// reference is just a ColId owned by the outer tree — no OuterRef variant).
    /// Empty for an uncorrelated scalar (§2.3 pass 2 handles that arm).
    pub correlation: Vec<HirExpr>,
    /// IN form only: the injected `outer_operand = inner_col` pair and whether
    /// either side is nullable — carried for the §2.3 nullability guards.
    pub in_pair: Option<(ColId, ColId)>,
    pub in_nullable: bool,
}
```

`infer_type` is already split (shipped) into the generic `infer_type_with` core (leaf-typing
closure) and the `usize` `BoundExpr::infer_type(&self, schema: &Schema) -> TypeCode` façade.
No `Subquery`/`OuterRef` variant ever enters the runtime `BoundExpr`: the illegal state is
unrepresentable downstream of the single `resolve_refs` conversion point, and `exec/eval.rs` /
`lower.rs` / `dml/` compile unchanged.

### 2.2 Bind (`hir/bind.rs`)

`bind_query(client, binder, query) -> Result<Rc<RelExpr>, GnitzSqlError>` — the one entry,
replacing `ViewShape::classify` + `build_query_segments`'s routing + `inline_ctes` +
`compile_derived_tables`. It reuses, unchanged: the catalog `Binder` (resolution + snapshot +
alias cache), `bind_structural` with per-context `LeafBinder`s (the leaves now produce
`HirExpr`; the trait already carries a default-rejecting `bind_subquery`,
`bind/structural.rs:81-87`), the projection expansion machinery (`WildcardRewrite`,
`ast_util.rs:574`; `codec/project_schema::build_projection`'s item analysis,
`project_schema.rs:145`), and every envelope validator (`reject_unhonored_query_clauses`,
`reject_unhonored_select_clauses`, `plain_select_body`, duplicate-name checks) at the same
points they fire today.

Structure — plain recursive descent, no classification ladder:

- **CTEs**: bind each body to `Rc<RelExpr>`, apply positional column aliases to its `HirCol`
  names (absorbing `apply_hidden_column_aliases`, `dispatch.rs:502-521`), and register
  alias → `Rc<RelExpr>` in a bind-scope map. Scoping precedence as today: a CTE shadows a
  catalog name; a sibling derived-table alias shadows a same-named CTE in the final body
  (`dispatch.rs:780,818`); a later CTE sees earlier ones. The pass-through fast path (identity
  projection of one table → alias to the source's `Get`, keeping catalog provenance and
  index-bound eligibility) is kept.
- **FROM**: a table name resolves via `binder.resolve` → `Get` (fresh `ColId`s minted per
  reference site — two references to the same *table* are distinct `Get`s with distinct ids;
  two references to the same *CTE alias* share one `Rc`); a derived table binds its subquery
  recursively; a join list folds left-deep into `Join` nodes with the ON conjuncts bound raw
  into `Join.on`. Name resolution binds through the existing `AliasMap` discipline against the
  in-scope alias → `HirCol` environments; qualified and bare references, case-insensitivity,
  and the deterministic "ambiguous column" error behave exactly as `find_unique_column` /
  `resolve_qualified_column` define today.
- **WHERE**: conjunct-flattened (`flatten_conjuncts`, `ast_util.rs:318`) into `Filter::preds`
  above the FROM tree. For a join FROM this is a `Filter` over the `Join`; §2.3 pass 3 decides
  its placement (INNER: folded into the join residual; outer: post-null-fill filter) — WHERE
  conjuncts are never promoted into join keys.
- **GROUP BY / aggregates / HAVING**: build `Reduce` from the group cols and the union of the
  projection's **and HAVING's** aggregate calls. The `Reduce`'s logical output is the **raw**
  reduce columns (per-`HirAgg` value + companion, plus `ground`), not the finalized SELECT
  shape. Any AVG / nullable-SUM reference in SELECT or HAVING binds to the composite
  expression over those raw `ColId`s — `sum * 1.0 / cnt` for AVG, `sum` gated on `cnt != 0`
  for nullable SUM — exactly `bind_having_expr`'s `bind_function` rule (`group_by.rs:958-999`;
  the post-map applies the identical rule). HAVING binds into an ordinary `Filter` **between**
  the `Reduce` and the finalize `Project`, referencing raw `ColId`s; a HAVING-only aggregate
  (`HAVING SUM(b) > 5` with `SUM(b)` unprojected) gets raw outputs referenced only by that
  `Filter` and simply not emitted by the finalize `Project`, preserving
  `collect_having_aggs`'s behavior (called at `group_by.rs:295-307`, defined at `:845-872` —
  un-projected, not `META_FLAG_HIDDEN`-flagged). This reproduces today's circuit order:
  reduce → filter over raw output → finalize map (`group_by.rs:637-645`, `:480-645`). Group
  keys are bare/qualified column references only (`group_by.rs:210-221`; anything else keeps
  today's rejection), so `Reduce.group_cols: Vec<ColId>` is sufficient. The `Reduce`'s
  group-col `HirCol`s keep their **source** names — SELECT aliasing happens only in the
  finalize `Project` above — so HAVING still resolves by source name and
  `HAVING <select-alias>` stays rejected, exactly `resolve_group_col`'s behavior
  (`group_by.rs:933-945`). The strict grouped-projection validator fires here, where the
  finalize `Project` is built: every projected item must be a bare group-col reference or an
  aggregate call — `SELECT a+1 … GROUP BY a` stays rejected (`group_by.rs:249-292`), and bind
  is the gate because a general `Project` node could otherwise express it. `SELECT MIN(x) FROM
  t` is a `Reduce` with empty `group_cols` — no special routing.
- **DISTINCT**: wraps the bound body in `Distinct`. DISTINCT combined with GROUP BY stays
  rejected in bind (today's behavior via the classify ladder + side resolver,
  `dispatch.rs:392-394` + `set_op.rs:69-77`; without this guard, `Distinct`-over-`Reduce`
  would silently start compiling).
- **Set operations**: `SetExpr::SetOperation` binds both sides recursively —
  `SetExpr::Select`, a parenthesized `SetExpr::Query`, or a nested `SetOperation` all bind the
  same way — then the `SetOp` constructor pairs columns positionally and applies cross-width
  promotion (`set_op_common_type`: identical type, or both concrete ≤8-byte ints promoted via
  `join_key_common_type`, else rejected; promotion is metadata-only — the physical widening is
  folded into the content-hash reindex's `target_tcs`, no cast op exists). Nesting and
  non-plain sides fall out of recursion.
- **Subqueries in expressions**: the view path's `LeafBinder::bind_subquery` binds the inner
  query recursively in a two-scope environment (inner names first, then outer — an outer
  reference resolves to the outer tree's `ColId`), splits the inner WHERE into pre-filter
  conjuncts (inner-only ids, stay inside `rel`) versus correlation conjuncts (mixed-scope ids →
  `SubqueryRef.correlation`; the acceptance set — equality pairs plus the one supported range
  form — is `resolve_correlation`'s, `exists.rs:162-376`, with the same rejection messages),
  injects the IN operand as `in_pair`, computes `in_nullable`, and returns
  `HirRef::Subquery`. A scalar subquery's projection must be a single aggregate call —
  `classify_aggregate`'s check (`scalar.rs:1027-1045`) moves here; non-aggregate scalar
  subqueries keep today's rejection message. An uncorrelated scalar (empty correlation set)
  binds like any other; §2.3 pass 2 gives it its own lowering arm and position guard.
  **Bind rejects `NOT IN` when `in_nullable`** — the current
  `exists.rs:300-307` guard, position-independent, kept verbatim: SQL's three-valued NOT IN
  diverges from the anti-join when NULLs are possible. Arbitrary boolean position and
  projection position bind identically; the rewrite decides Semi/Anti vs Mark.

Bind performs no circuit work and no id allocation. It is exercised through the same
server-backed planner-test harness as every other compiler test (there is no mock catalog;
`Binder::resolve` takes a live `GnitzClient`).

### 2.3 Rewrites (`hir/rewrite.rs`)

Ordered passes; each is a pure `Rc<RelExpr> → Rc<RelExpr>` spine rebuild, and **every** pass
memoizes by `Rc::as_ptr` so a shared subtree (multiply-referenced CTE) remains one shared node —
otherwise a naive rebuild would silently split it into two segments later.

1. **ANY/ALL normalization** — `x = ANY(sub)` → IN-shaped `Subquery`; `x <> ALL(sub)` → NOT IN
   (subject to the same `in_nullable` rejection, applied here for the rewritten form);
   `x < ANY(SELECT y …)` → `x < (SELECT MAX(y) …)` and the other range/quantifier pairings, as
   the scalar builder's tables define today. Runs first so later passes see only
   `Exists`/`Scalar` kinds.
2. **Decorrelation** — replaces `scalar.rs`'s AST synthesis and `exists.rs`'s front half. For
   each node expression containing `HirRef::Subquery`:
   - `Exists` as a top-level `Filter` conjunct → `Join { kind: Semi | Anti }` with `rel` as
     right child and the correlation conjuncts as `Join.on`. Uncorrelated EXISTS keeps today's
     rejection (no incremental cross-join).
   - `Exists` anywhere else (under OR/NOT, inside CASE, projected) → `Join { kind: Mark }`;
     the leaf becomes `ColRef(mark_id)`. **This arm rejects a nullable IN** (`in_nullable` on
     the `SubqueryRef`) — the current `exists.rs:284-296` guard: a two-valued mark bit cannot
     represent UNKNOWN. Position is first known here, which is why this guard lives in the
     rewrite while the NOT IN guard lives in bind.
   - `Scalar`, correlated → `Reduce` over `rel` grouped by its correlation columns, left-joined
     to the outer input on those columns; the leaf becomes `ColRef(agg_id)`, wrapped in
     `COALESCE(…, 0)` only when `coalesce_zero` (COUNT / COUNT(col) over an empty group; SUM /
     AVG / MIN / MAX stay NULL, per SQL). Multiple subqueries chain joins left-deep in
     expression order.
   - `Scalar`, uncorrelated (`correlation` empty): only the position "one side of a top-level
     WHERE comparison conjunct `outer_expr OP (SELECT agg …)`" is supported, as today. The
     subquery becomes a global `Reduce` (empty group cols → one-row output) and the conjunct
     becomes `Join { kind: Inner }` of the outer input against it with the comparison as the
     sole join predicate — `=` classifies as the equi pair, `<,≤,>,≥` as the pure-range
     conjunct, i.e. the aggregate *value* is the join key (today's `build_uncorrelated_join`,
     `scalar.rs:642-721`). Any other position of an uncorrelated scalar keeps today's rejection
     (`substitute_scalar`, `scalar.rs:773-778`). This arm exists because the correlated form
     degenerates to a keyless join on empty correlation, which the engine cannot execute.
   Post-condition: no `HirRef::Subquery` remains (enforced structurally by `resolve_refs`,
   §2.4 — a survivor is a compile error of the pipeline, not a runtime surprise).
3. **Predicate classification** — one pass, after the tree's structure is final: for every
   `Join`, partition **`Join.on` only** into `JoinClass { eq, range, residual }` using the
   `extract_join_predicates` + `PredicateCollector` logic (`predicates.rs:270-326`, `:333-436`)
   re-hosted on `HirExpr`: per-pair promoted types, at most one range conjunct, residual-alone
   rejected (`predicates.rs:295-301`), outer-kind + residual rejected (`join.rs:811-818`).
   WHERE conjuncts — the `Filter` above the join — are **never** promoted into join keys
   (today's `classify_join_where`, `join.rs:335-352`, does no such migration, and for outer
   joins promotion would change which rows null-fill): for an INNER join the `Filter`'s
   conjuncts fold into `JoinClass.residual` (today's post-join residual program); for an outer
   join the `Filter` stays a filter node above the null-fill, exactly `where_filter`'s
   placement. Scan-bound extraction is deliberately **not** part of this pass — it is a
   catalog-probing physical access decision and lives in lowering (§2.5), next to the other
   catalog-dependent emission decisions. The access-path recognizers already live in
   `src/access.rs` and take bound conjuncts as input (shipped; the AST-input twins are
   deleted, and the ad-hoc ReadSpec planner, the UPDATE/DELETE planning, and
   `plan/index_bound` all call the shared bound-conjunct entry points). Each caller keeps
   its own gating: the view path stays gated on `is_catalog_relation` (`lp.rs:71-76`), the
   ad-hoc path probes unconditionally as today.
4. **Column pruning** — one generic top-down demand pass over `ColId`s. A column is live iff
   referenced by a parent's expressions **or structure**: `ProjEntry.expr`s, `Filter.preds`,
   `JoinClass.eq`/`range`/`residual`, `Reduce.group_cols`, `HirAgg.arg`, `SetOpCol` pairs,
   correlation conjuncts, and the Mark column. Two node classes are **demand barriers** whose
   full output column set is forced live regardless of parent demand:
   - **Set-identity nodes** — `Distinct`, and every `SetOp` except `Union { all: true }`.
     Their columns *are* the dedup/match identity: the content hash spans all projected
     columns (`set_op.rs:119`, `:208-211`), and INTERSECT/EXCEPT arithmetic matches on
     full-row content even in the ALL variants. Projection does not commute through them —
     pruning `b` out of `SELECT a FROM (SELECT a,b FROM t1 UNION SELECT a,b FROM t2)` would
     change the hash's equivalence classes and corrupt weights
     (`π_a(distinct(X)) ≠ distinct(π_a(X))`). Only UNION ALL — pure Z-set addition — commutes
     and may be narrowed.
   - **Shared nodes** — any `Rc` with more than one parent (a multiply-referenced CTE). Its
     full schema is demanded, matching today's behavior (a hidden CTE segment materializes
     its declared schema). This removes all cross-parent demand-union bookkeeping: every node
     the pass actually narrows has exactly one demanding parent.
   Elsewhere: drop dead `ProjEntry`s, narrow `HirAgg` lists and UNION ALL `SetOpCol` lists,
   prune dead join payload columns, and insert pruning `Project`s below segment-expensive
   nodes. Pure id-set arithmetic — no reference is renumbered, so nothing invalidates.
   Physical keep-rules (a key column is never dead; a nullable preserved-side join-key's
   payload copy survives for ν identity — today's `equi_keep_combined` rule 3,
   `join.rs:697-703`) are applied in `physical.rs`, which is the layer that knows them — the
   logical pass prunes only columns nothing references. (The old code's counterpart is
   `plan_join_chain`'s per-chain liveness pre-pass, `join.rs:539-563`, gated on multi-join
   chains, plus `emit_join`'s keep-rules; this pass replaces them generically — it is
   required for join-chain parity, and the barriers keep it at exactly parity everywhere
   else.)

### 2.4 Physicalization (`hir/physical.rs`)

The single positional pass, run per emitted circuit during lowering. For each node it derives:

- `layout: Vec<ColId>` — the physical column order: key slots first, then payload. Key
  derivation per node kind (the one home of every convention that today lives in five files):
  `Get` → the registered schema's `pk_cols`; `Filter`/`Distinct` → input's; `Project` → the
  input key carried and pinned in front (`build_projection`'s `place_pk_front`,
  `project_schema.rs:113-140`: a PK column projected later moves to the front; one absent from
  the projection is auto-prepended `.hidden()`; a PK explicitly projected into a payload slot
  is additionally re-emitted there via the expr-map strategy, `simple.rs:73-76`); `Join` equi →
  the join-key slots with promoted types (`join_pk_coldefs`, `join.rs:1681-1695`),
  band/pure-range → the source-PK pair (`_pair_pk_{slot}`, `join.rs:1324`), Semi/Anti →
  left key, Mark → left key + mark appended; `Reduce` → the `ReduceOutKey` decision
  (`PkPermutation` / `SingleNaturalCol` / `SyntheticFold`, the last minting the hidden
  `_group_pk` column — `group_by.rs:347-366`) from the group-col/input-key relation, exactly
  the current rule; `SetOp`/`Distinct` → the synthetic content-hash key
  (`_set_pk`/`_distinct_pk`, U128, hidden).
- `schema: Rc<Schema>` — the `Schema` a hidden view materializing this node registers
  (`ColumnDef`s in layout order + `pk_cols`). A segment cut at any node registers exactly this.
- `resolve_refs(expr: &HirExpr) -> Result<BoundExpr, _>` — leaf substitution
  `ColId → usize` through the node's layout map; a surviving `HirRef::Subquery` is rejected
  here, making "no subquery reaches the runtime IR" a property of the one conversion point.
- The physical keep-rules for pruning (key columns; ν-identity payload copies on nullable
  preserved-side join keys), applied when materializing `layout`.

Everything downstream of `resolve_refs` — `compile_filter_program`, the projection map, the
opcode backend — is today's code, unchanged.

### 2.5 Lowering (`hir/lower.rs`)

`lower(client, chain, rel, final_vid, …) -> EmitPieces` walks the tree once, emits circuits
through the existing `CircuitBuilder`, and pushes hidden segments through the existing
`ViewChain::add_segment`. Three rules govern what the old code hand-wired per shape:

**The segment-cut rule.** The engine compiles a view to `PlanShape::Single` or
`Exchanged { sides (≤2), post }` — one parallel exchange level, never a sequential one
(`crates/gnitz-engine/src/query/compiler/mod.rs:172-181`); the runtime dispatch arms
(range-relay ▸ exchanged-sides ▸ join-scatter ▸ single, `query/dag/exec.rs:309-366`) are
mutually exclusive, so a circuit mixing a join with an exchange-carrying combine would take the
wrong arm. Classify HIR nodes: **combine-class** — `Join` (any kind), `Reduce`, `Distinct`,
`SetOp` — and **linear-class** — `Get`, `Filter`, `Project`.

> A circuit contains exactly one combine-class **HIR node** plus any linear nodes above and
> below it. When lowering reaches a combine-class node one of whose (transitive, linear-path)
> inputs contains another combine-class node, that input subtree is cut: emitted as a hidden
> segment via `chain.add_segment` and replaced by `Get { tid: segment vid, from_catalog:
> false }` carrying the segment's physicalized schema.

Lowering memoizes cuts by `Rc::as_ptr` → segment vid: a multiply-referenced subtree (shared
CTE) is cut **once** and every reference resolves to the same hidden segment — today's
`inline_ctes` compile-once sharing (`dispatch.rs:539-564`). The rewrites' Rc-preserving
memoization (§2.3) exists precisely to keep this identity intact through the passes.

"One combine-class HIR node" is a property of the HIR, not of the emitted `OpNode` multiset: a
single `Reduce` node lowers to *two* engine Reduce ops around the exchange in the two-phase
global-aggregate strategy (`group_by.rs:442-471`), a pure-range LEFT join embeds its threshold
reduce, semi/anti/outer joins embed `WeightClamp` ops, and an `all: false` SetOp embeds its
per-leaf distinct clamps (`set_op.rs:202-212`) — none of those are separate HIR nodes, so the
rule cuts exactly where today's compiler cuts. The engine-level invariant the cut rule
guarantees — and the one `plan/view/mod.rs::debug_assert_exchange_topology` checks on every
emitted circuit, from the two paths every circuit reaches (`ViewChain::add_segment` for hidden
segments, the final push for the user-named view), since the engine does **not** fail cleanly
on a violation (a sequential exchange inside a carved side hits an `unreachable!` in
`build_plan`'s emit, `query/compiler/emit.rs:632-634`) — is:

> ≤ 2 `ExchangeShard` nodes; two only as parallel set-op sides; no `ExchangeShard` downstream
> of another `ExchangeShard`; no `ExchangeShard` in a circuit that also contains a `Join` node,
> except the join-strategy-internal ones the join emitters themselves place (the range-join
> output shard).

This one rule reproduces every existing composition (join chains: each step cut, matching
`plan_join_chain`'s per-step `add_segment`, `join.rs:656-685`; GROUP BY/DISTINCT over a join:
join cut, reduce over `H`; scalar `G_i` reduce segments) and generalizes to every §2.7 addition
with no new per-edge code.

**The source-collision rule.** A circuit's delta inputs must be distinct source ids, and the
engine has **no** backstop — the failure mode differs by shape, which is why the rule is the
frontend's alone. In a single-pipeline circuit (a join: both inputs compile into one sub-plan),
a duplicate-source `ScanDelta` silently overwrites the emit-side `source_reg_map`
(`query/compiler/emit.rs:284-288`), corrupting weights. In a two-side `Exchanged` circuit
(INTERSECT/EXCEPT: each side is its own sub-pipeline with its own register map), the hazard is
instead single-source-per-epoch: one push would drive both sides' deltas in the same epoch,
which the bilinear/clamp algebra does not admit — the reason today's guard exists
(`set_op.rs:270`). After cuts, if two delta inputs of one circuit would carry the same `tid`,
one side is wrapped in an identity pass-through segment (identity `Project` over the `Get`, cut
unconditionally) — today's self-join wrapper (`join.rs:591-606`), applied uniformly to joins,
INTERSECT, and EXCEPT. **UNION / UNION ALL are exempt**: they are linear merges; the dag
explicitly clones one epoch's delta to both sides (`query/dag/exec.rs:60-92`), `a UNION a`
compiles unwrapped today (`set_op.rs:266-267`), and wrapping it would regress a working
physical plan. The same-relation INTERSECT/EXCEPT guard is deleted because the wrapper
*manufactures* the property the guard was protecting: the wrapper segment turns one user push
into two separate cascade epochs (`evaluate_dag_multi_worker` dedupes work by
`(view_id, source_id)`, `query/dag/exec.rs:407-458`), and the ν-difference pipeline — linear
ops plus a single weight-clamp integral (`op_weight_clamp`) — converges to the correct net
weights under separate-epoch arrival, the same property self-joins already rely on. The
between-epoch transient (e.g. `t EXCEPT t` momentarily at `+t` after the first epoch) is
cascade-internal: both epochs run inside one `evaluate_dag_multi_worker` call and the view
trace flushes once after the DAG settles (`query/dag/exec.rs:460-463`), so it is never
queryable — the same accepted transient self-joins produce today
(`test_self_join.py::test_employee_manager`).

**The backfill-seeding rule.** The engine inline-backfills a new view at DDL-hook time unless
its circuit contains a `Join` or `ExchangeShard` node (`view_seeds_exchange_backfill`,
`query/dag/meta.rs:375-378`; gate at `catalog/hooks.rs:598-605`); only seeding views get the
dependency-ordered distributed backfill (`runtime/orchestration/executor.rs:2607-2631`, ordered
by `order_by_intra_bundle_deps`). A linear circuit whose delta source is an in-bundle segment
that *itself* seeds would therefore inline-backfill from a still-empty segment and lose the
base data. **Shipped, one home:** `simple::emit_linear(chain, view_id, rel)` decides the shard
itself — `chain.segment_seeds_backfill(rel.source_tid())` appends an identity `shard` on the
view's own PK when the source segment's circuit contains a `Join` or `ExchangeShard` node —
and every linear emit site (the HIR linear lowering, hidden bodies, the scalar final, the
self-join collision wrappers) routes through it. The direct-source predicate is sufficient: the
shard is added at emit time, so a linear segment over a seeding source itself becomes seeding,
and the next segment reading it sees the shard — sharding self-propagates down chains; a
non-seeding middle over a seeding source cannot exist. A linear circuit over a base table or a
linear segment correctly stays unsharded (inline-backfilled in chain/dependency order at hook
time).

**Per-node emission — what "moved" means, precisely.** Each old emit body splits into two
layers with different fates:

- **Index/Schema-shaped primitives** — already AST-free `pub(crate)` functions taking
  `&[usize]` / `Schema` / `NodeId`: `normalize_to_ab`, `emit_equi_join_terms`, `range_slots`,
  `range_gate_reindex_prologue`, `side_target_tcs`, `build_pure_range_threshold`,
  `join_pk_coldefs`, `band_union_schema`, `union_null_key_rows`, `is_identity_projection`,
  `build_join_view_projection` (join.rs); `semi_and_anti` and the `emit_equi_exists` /
  `emit_range_exists` branch bodies (exists.rs); `hash_shard_side` and the set-op arithmetic
  (set_op.rs); the reduce strategy emit (group_by.rs); `predicates.rs`'s program builders
  (its collector re-hosted on `BExpr<R>` by the §5 genericization). These are reused
  **verbatim, in place**: during migration the `hir/lower/*` modules are orchestration shells
  that call them cross-module (`crate::plan::view::join::emit_equi_join_terms(…)`). The
  physical relocation into `hir/lower/` happens only in the §5 sweep, once `scalar.rs` and
  `compile_hidden_body` — which keep the old orchestration alive as callers — are deleted.
- **AST-fused orchestration shells** — `emit_join`'s head and tail (`equi_keep_combined` over
  `SelectItem`s, the `LoweredJoin` destructuring, `prune_schema`/`prune_alias_map`),
  `emit_group_by_pieces`'s `Select`-driven shell, the mark branches' per-branch re-binding.
  These are **written fresh** against the physicalized node (layout + keep-set from
  `physical.rs`, predicates from `JoinClass`) — they are the AST-coupled layer this plan
  exists to delete, and they are never shared with the old path.

The table below names the primitives each node's new shell drives.

**Scan bounds are a primary-position lowering decision.** Only a circuit's primary delta input
can carry a backfill bound (`input_delta_bounded` uses the builder's primary source;
`input_delta_tagged` has no bound slot, `gnitz-core/src/circuit.rs:165-185`). Lowering
therefore runs the access-path recognizers (over the `Filter` conjuncts directly above a
`from_catalog` `Get` — today `scan_bound_for_input`, `plan/index_bound.rs:29-39`) **only** for
the `Get` it places in the primary position of the linear and reduce circuits — exactly the two
sites bounded today (`simple.rs:81`, `group_by.rs:386`). Secondary/tagged positions, set-op
sides, and `Distinct` inputs stay probe-free, preserving the deliberate
no-`GET_INDICES`-probe behavior documented at `dispatch.rs:200-206`; no bound is computed that
lowering cannot honor.

| HIR node | Emits (existing helpers) |
|---|---|
| `Get` | `input_delta_bounded(bound)` (primary position, bound per the rule above) / `input_delta_tagged(tid)` (all other positions) |
| `Filter` | `and_fold_compile` (`predicates.rs:489-503`) → `cb.filter`, elided when constant-true (the one home of the four copies of that idiom) |
| `Project` | the `map_expr` / pure-`map` / identity strategy triple (`simple.rs:87-118`) + the seeding-rule shard |
| `Join` Inner/Left/Right/Full equi | `emit_join`'s body (`join.rs:803-1099`) calling `normalize_to_ab` (`join.rs:105-163`), `emit_equi_join_terms` (`:164-211`), and the null-fill `positive_diff`/`null_extend`; correlation/join-key NULL gating rides inside the shared term builders (`emit_equi_join_terms`, `range_gate_reindex_prologue`, `join.rs:251+`) and the `predicates.rs` null-filter programs, moved unchanged |
| `Join` band/pure-range | `emit_range_join` (`join.rs:1134-1566`) incl. its call to `build_pure_range_threshold` (`join.rs:1614+`), eq-prefix scatter vs broadcast |
| `Join` Semi/Anti | `semi_and_anti`'s ν-difference (`exists.rs:465-469`) over the equi terms built by `emit_equi_exists` (`exists.rs:486-555`); band via `emit_range_exists`'s inner-terms branch (`exists.rs:652-683`), pure-range via its threshold branch (`exists.rs:602-651`) |
| `Join` Mark | the matched/unmatched two-branch structure of `finish_mark_pieces` (`exists.rs:758-793`). **Re-hosted, not moved verbatim**: today each branch re-binds the WHERE against the raw AST with the subquery as a `0/1` constant (`emit_mark_branch`, `exists.rs:810-811`) and the projection via `build_mark_projection`'s `resolve_proj_col_with` (`exists.rs:829-873`, substitution at `:866-868`); in HIR the two branches receive the pre-bound expressions with `ColRef(mark_id)` → `LitInt(0|1)` substituted per branch — the per-branch AST re-binding is exactly the round trip this plan deletes. No constant-folding step is needed: `compile_filter_program` elides only a literal-true WHERE, a mark=0 branch emits an always-false filter, and both branches are always emitted and unioned (`exists.rs:779-781`) — keep that shape, never collapse to one filter over a runtime mark column |
| `Reduce` | the reduce strategy emit with **raw** outputs (`AggShape` companions included), two-phase / replicated / sharded selection (`group_by.rs:438-478`, driven by `client.table_replicated` read at `:192` — reduce emission is catalog-dependent, as today), `global_ground`. The post-reduce finalize map (`group_by.rs:480-645`) is **not** part of Reduce emission: it is the finalize `Project` node's emission, and the HAVING `Filter` compiles between the two against the raw reduce columns — reproducing today's circuit order reduce → filter(raw) → finalize map |
| `Distinct` / `SetOp` | `hash_shard_side` (`set_op.rs:108-127`) + the set-op arithmetic (`set_op.rs:319-368`) |

Per-shape *semantic* validations stay with the node they guard, one place each: outer join +
residual ON (`join.rs:811-818`), residual-only ON, pure-range RIGHT/FULL (`join.rs:1204`),
NOT-IN/mark nullability (§2.2/§2.3). Shape-*routing* validations — "not supported over a
compiled sub-plan", "must be a single plain table", "at most one subquery" — are deleted with
the routing they patched.

### 2.6 Entry points

- `execute_create_view` / `execute_alter_view` (`dispatch.rs`): unchanged envelopes, SQL-text
  capture, chain commit, ALTER's compile-first + self-reference check + two-zone drop/create.
  The body between "envelope validated" and "chain committed" becomes the HIR pipeline.
- Ad-hoc SELECT never reaches this pipeline: it is served by the ReadSpec rows/fold sinks
  (`dml/select.rs`), and every derivation shape is rejected there with the CREATE VIEW
  redirect — the HIR pipeline's only entry points are the two CREATE/ALTER VIEW handlers.
- Hidden naming (`__h{owner}_{idx}` via `gnitz_core::hidden_view_name`), DROP-cascade, id
  allocation order, and `create_view_chain` atomicity: unchanged (`ViewChain` is untouched).

### 2.7 Newly compiling shapes, and what stays rejected

Enabled by generic composition — the compiler routes them with no per-edge code; each ships
with the e2e coverage of §4 in the commit that enables it:

1. **Shipped.** Nested set-operation chains — `A UNION B UNION C`, parenthesized sides, mixed
   operators (`(A UNION ALL B) INTERSECT C`): inner set-op cut to a segment, outer re-hashes it.
2. Set-op sides whose FROM is an inline join, a grouped query, or a derived table. **The join
   and grouped halves are shipped**; the derived-table half waits on the CTE/derived commit.
3. GROUP BY / DISTINCT over a set operation or over any supported derived-table body. Waits on
   the CTE/derived commit: a derived or CTE body must currently be a plain SELECT
   (`plain_select_body`), so a set-operation body cannot yet be a derived table.
4. EXISTS / IN / scalar subqueries over compiled CTEs and derived tables.
5. Multiple EXISTS/IN/scalar subqueries per view (chained Semi/Anti/Mark joins).
6. Subqueries nested inside subqueries.
7. Derived tables nested inside derived tables.
8. **Shipped.** Same-relation INTERSECT/EXCEPT and same-relation set-op-vs-join mixes, via the
   uniform pass-through wrapper (UNION never needed it and stays unwrapped).

Still rejected, unchanged, because engine semantics (not frontend routing) exclude them:
recursive CTEs; LATERAL; multi-item FROM (comma cross join); residual-only ON; outer join with
residual ON; pure-range RIGHT/FULL; uncorrelated EXISTS; non-equi/range correlation; `NOT IN`
over nullable operands; nullable IN in mark position; non-aggregate scalar subqueries;
uncorrelated scalar subqueries outside a top-level WHERE comparison; DISTINCT combined with
GROUP BY; ORDER BY / LIMIT inside a view body; window functions; GROUP BY over arbitrary
expressions; projected expressions over group columns (`SELECT a+1 … GROUP BY a`); HAVING by
SELECT alias. Their error messages and tests carry over.

## 3. What is deleted, moved, kept

**Deleted** (by the §5 sweep; nothing is deleted while a consumer remains): `plan/lp.rs`;
`ViewShape` + `classify` + `as_subquery_conjunct` + the subquery-count gate +
`compile_hidden_body` + `compile_join_to_hidden` + `resolve_operator_input` +
`collect_operator_names` + `is_compilable_hidden_body` + `inline_ctes` +
`compile_derived_tables` (routing halves; scoping behavior reappears in bind); `scalar.rs` in
full (its single-aggregate and position guards re-home per §2.2/§2.3); `exists.rs`'s
`resolve_correlation` and both `emit_*_pieces` front halves (its nullability guards move to
§2.2/§2.3, its emit bodies to `hir/lower/`); `plan_join_chain`'s resolve/classify/liveness half
and `Provenance`; `group_by.rs`'s AST-side analysis and the HAVING leaf cluster; `set_op.rs`'s
side resolution and the same-relation guard; every composition-edge `Unsupported` string; the
`ast_util.rs` helpers whose only callers were the above. (The AST-input twins of the
access-path recognizers are already gone — deleted by the shipped `src/access.rs` move.)

**Moved** (physically, only at the §5 sweep — during the per-shape commits nothing relocates;
`hir/lower/` calls the primitives cross-module per §2.5): the index/Schema-shaped primitives
and `predicates.rs` (collector re-hosted on `HirExpr`; program builders unchanged) →
`hir/lower/`. (The access-path recognizers already moved to `src/access.rs` — bound-conjunct
input, shared with dml — in the shipped checkbox 3.)

**Kept verbatim**: everything listed at the end of §1, plus `ViewChain`, `EmitPieces`, and all
`CircuitBuilder` usage (the full kept API surface is quoted in §6).

Honest effort accounting: the reusable layer is the `CircuitBuilder` call sequences — the
index/Schema-shaped primitives, reused verbatim. The AST-fused orchestration shells around
them (projection/residual/keep resolution over `SelectItem`s and alias maps) are **rewritten**
against the physicalized HIR, and the classification, AST-surgery, re-binding, and per-shape
bookkeeping layers are deleted outright. The structural gains this plan is committed to,
independent of net line count: one IR instead of two (`Rel` + AST-as-IR), one composition
mechanism instead of per-edge wiring, one home for every schema/key convention, decorrelation
without AST synthesis, and a growth mode where a new operator is one node + one bind arm + one
lowering arm and a new composition is zero compiler code.

## 4. Verification

All compiler tests are server-backed (the crate has no mock catalog; `Binder::resolve` takes a
live `GnitzClient`, and circuit inspection goes through the circuit-nodes system-table scan the
existing planner tests use).

- **Behavioral gate**: `make verify` and `make e2e` (`GNITZ_WORKERS=4`) green at every §5
  checkbox. Migrated shapes must not change any passing test's observable result, error
  messages included; where a formerly-rejected shape now compiles, the test asserting the
  rejection is updated to assert the new behavior in the same commit that enables it.
- **Circuit-shape pinning** (shipped — `common/mod.rs::canonical_circuit_dump` +
  `planner_circuit_snapshot.rs`, goldens authored against the old compiler, which the HIR
  pipeline must reproduce). Sentinels are retained through the sweep; a legitimate shape
  change is a reviewed golden update, always cross-checked against the shape's weight pin.
  Two pre-identified exception classes assert the *new* multiset with the delta reviewed:
  (a) shapes where the generic pruning pass narrows a reindex/projection the old code did
  not prune (checkpoint-8); (b) shapes where the uniform collision wrapper arranges wrappers
  differently — e.g. a 3-way self-join `t⋈t⋈t`: today one wrapper per repeated base id reused
  across steps (`join.rs:591-601`), under the HIR cut only the actual per-circuit collision is
  wrapped, so the multiset differs while both plans are correct and source-distinct.
- **Semantics + weight pinning** (shipped — rejection variant + message pins in
  `planner_scalar_subquery.rs` / `planner_group_by.rs` / `planner_cte.rs`; the pre-existing
  `NOT IN`-over-nullable, ambiguous-column, and correlated/uncorrelated scalar-result pins
  are relied on in place). **Weight pins** (`planner_weight_pins.rs`, `GNITZ_WORKERS=4`) are
  the primary correctness net — insert/delete multiplicity for the predicate-bearing shapes
  (filter, join residual), the self-join collision wrapper, and the two-phase / replicated
  reduce arms — the net that survives a structural golden legitimately becoming an exception.
- **New-composition coverage**: one e2e test per §2.7 item, multi-worker. Items 1–5 and 8 get
  the full matrix — maintained results under post-create inserts *and* deletes (weight
  correctness, not row presence) plus the data-before-view (backfill) order — with item 8
  explicitly covering `t EXCEPT t` / `t INTERSECT t` backfill (the same-relation guard
  deletion is the riskiest behavior change), and a linear final over each new segment kind
  (the seeding rule's coverage). Items 6 and 7 (nested subqueries / nested derived tables —
  pure compositions of edges the other items already exercise) get one maintained-result test
  each, not the full matrix.
- **Frontend structural assertion**: the §2.5 engine-level invariant (exchange count/topology)
  debug-asserted on every emitted circuit — the only guard there is, since the engine panics
  or corrupts rather than erroring on a violation. Asserted inside `ViewChain::add_segment` and
  at the final emit, so it covers every circuit by construction and no emitter can escape it.

## 5. Sequencing

Each checkbox is one commit; the suite is green at every one. Old code is deleted only when its
last consumer is gone — the AST emitters are cross-consumed (`scalar.rs` re-enters
`plan_join_chain` and `emit_group_by_pieces`; `compile_hidden_body` routes every CTE/derived
body), so the old pipeline remains present until the final sweep. Per-shape commits are
**additive plus a routing switch**: they add `hir/lower/` shells and flip the `ViewShape` arm.
Where a shell would otherwise have duplicated an emission body, that body is **extracted to one
home** and both emitters call it (`agg::emit_reduce`, `join::emit_equi_null_fill`, …) rather
than carrying two copies to the sweep — the sweep then deletes callers, not duplicates.

- [x] **Pinning suite.** Done — `planner_circuit_snapshot.rs` (DFS-from-sink canonical dump;
  `#N` labels; base refs by name, segments by structural index; expr excluded — predicate
  content is weight-pinned) pins ~17 primitive-family/arm/composition sentinels plus an
  order-independence unit test; rejection gaps in `planner_scalar_subquery.rs` +
  `planner_group_by.rs`/`planner_cte.rs`; weight pins in `planner_weight_pins.rs` (W=4).
  Sentinels stay green through the sweep; legitimate shape changes are reviewed golden updates
  cross-checked against the weight pins. No production change.
- [x] **`BExpr<R>` genericization.** Done — `ir.rs` carries `enum BExpr<R>` with
  `type BoundExpr = BExpr<usize>`; `infer_type` split into the generic `infer_type_with` core
  (leaf-typing closure) and a `usize` `infer_type` façade; `bind/structural.rs`'s
  `LeafBinder<R = usize>` trait and `bind_structural`/`bind_literal`/`bind_coalesce`/`bind_nullif`
  genericized over `R` (`R: Clone` on all but `bind_literal`). The four leaf impls (`MarkLeaf`,
  single-table, join-residual, HAVING) and every runtime consumer (`exec/eval.rs`, `lower.rs`,
  `dml/`) compile unchanged via the alias. No behavior change.
- [x] **`src/access.rs`.** Done — the PK/index recognizers (`try_extract_pk_seek_residual`,
  `try_extract_pk_in`, `try_extract_pk_range`, the seek/range candidate collectors,
  `best_index_bound`) live in the AST-free `src/access.rs` leaf over bound conjuncts; the
  ad-hoc ReadSpec planner (`dml/select.rs::where_bound_and_predicate`), UPDATE/DELETE
  planning (`dml/mutate.rs` / `dml/plan.rs::classify_access`), and `plan/index_bound.rs`
  (now a thin bind-then-delegate bridge, the last AST island) all call the shared entry
  points; the AST-input forms are deleted. The WHERE binds once per statement; candidate
  residuals are borrows of the bound WHERE (only the winner is cloned/compiled); native
  literals pack by value via `pk_codec::pack_pk_value`. Out-of-i64-range integer literals
  bind as `BExpr::LitWide` (raw magnitude string): servable wide seeks pack byte-exactly
  into PK/index bounds, un-servable positions reject via the one `lower_bound_expr` arm
  plus eager `find_wide_literal` guards on the lazily-interpreted mutate positions
  (`TestWideLiteralMutateGuards` e2e).
- [x] **HIR core + linear routing.** Done — `hir/mod.rs` (`ColId`, `RelExpr`,
  constructors, `HirExpr`, the shared `col_by_id`/`slot_of` lookups; node cols are
  *derived* by `RelExpr::cols()`, stored only on `Get` where ids are minted),
  `hir/bind.rs` for single-table bodies, `hir/physical.rs` (`resolve_refs` via the
  generic `BExpr::try_map_refs`, the shared `fold_preds` AND-fold,
  `physicalize_projection` over the `pub(crate)` `place_pk_front`),
  `hir/lower/mod.rs` with linear lowering. The linear path resolves to a
  `plan/lp.rs::Rel` and delegates to `simple::emit_linear`, which owns the
  backfill-seeding rule in one home (`ViewChain::segment_seeds_backfill` +
  `Rel::source_tid`); every linear emit site — the HIR path, hidden bodies,
  the scalar final (its hand-rolled `over_h` conditional deleted), and the
  self-join collision wrappers — routes through it, fixing the two formerly
  shard-less emit sites. Verified by `planner_projection.rs` metadata pins.
- [x] **Joins.** Done — FROM join lists bind raw ON into `Join` nodes
  (`JoinScope`: one combined `HirCol` list + per-relation spans; the shared
  `expand_wildcard` serves linear and join projections); `hir/rewrite.rs::classify`
  is `PredicateCollector` re-hosted on `HirExpr` (`ColId`-membership sides, WHERE
  folded into the INNER residual in place); `hir/lower/join.rs` is the emission
  shell over the retained `join.rs`/`predicates.rs` primitives
  (`emit_equi_join_terms`, `normalize_to_ab`, `range_gate_reindex_prologue`,
  `build_pure_range_threshold`, `prune_schema`, …), with chains and self-joins
  through the cut + collision rules (cuts memoized by `Rc::as_ptr`, chain-liveness
  pruning of cut segments). The HIR reuses `plan/view/join.rs::JoinType` directly
  (no parallel kind enum; Semi/Anti/Mark extend it in the subqueries commit).
  `ViewShape::{Simple,Join}` merged into one `ViewShape::Relational` arm.
  `join.rs`'s old orchestration stays resident for `scalar.rs` and
  `compile_hidden_body`. Verified by the circuit-shape goldens and the 3-way
  chain / self-join / outer-chain weight pins (`planner_weight_pins.rs`, W=4).
- [x] **Reduce, Distinct, SetOp.** Done — bind builds the raw-output `Reduce` (HAVING-agg
  union, source-name binding, grouped-projection strictness) and set-op trees;
  `hir/lower/{reduce,setop}.rs` are the emission shells. `ViewShape::{GroupBy,Distinct,SetOp}`
  collapse into `Relational`, so every relational body now routes through HIR and only the
  three subquery shapes keep an AST emitter. `set_op.rs` is down to four AST-free primitives;
  `group_by.rs` stays resident for `compile_hidden_body`.
  The three governing rules got one home each in `hir/lower/mod.rs`: the segment cut
  (`resolve_input`/`cut_segment`, over one compilation-wide `CutMemo`), the source collision
  (`resolve_collisions`, comparing *resolved* tids so a side that already became a segment is
  never wrapped redundantly), and the exchange-topology assert (moved into
  `ViewChain::add_segment` + the final emit, so every circuit is covered by construction).
  `RelExpr::map_children` collapses the rewrite's per-kind rebuild arms and `classify`
  memoizes by `Rc::as_ptr`, keeping a shared subtree one node. The reduce strategy/layout
  (`agg::{ReduceShape, emit_reduce, reduce_output_schema}`) and the outer-join null-fills
  (`join::{emit_equi_null_fill, emit_range_null_fill_tail, pair_pk_coldefs}`) were extracted
  to one home each rather than duplicated across the AST and HIR emitters — the §5 sweep now
  deletes callers, not copies. Enables §2.7 items 1, 8, and the join/grouped halves of item 2;
  item 3 and item 2's derived-table half wait on the CTE/derived commit, since a derived or
  CTE body must still be a plain SELECT (`plain_select_body`). Verified by the circuit-shape
  goldens (unchanged — both emitters drive the same extracted primitives), the weight pins,
  and `test_hir_new_compositions.py` (nested/mixed chains, computed DISTINCT / set-op sides,
  grouped and join set-op sides, same-relation INTERSECT/EXCEPT, each under inserts, deletes,
  and the data-before-view backfill order, asserted on weights).
- [ ] **Subqueries.** `HirRef::Subquery`, the two-scope leaf, bind-time NOT-IN and
  single-aggregate guards, ANY/ALL + decorrelation rewrites (mark-position guard, correlated
  and uncorrelated scalar arms), Semi/Anti/Mark lowering shells over the retained `exists.rs`
  primitives. Enables §2.7 items 4–7, tests in the same commit. `ViewShape::{Subquery,
  MarkSubquery, ScalarSubquery}` route through HIR.
- [ ] **CTE/derived scoping + column pruning.** CTE and derived-table binding moves into
  `hir/bind.rs` scoping (shadowing precedence pinned by the §4 tests); the pruning pass goes
  live; the old `inline_ctes`/`compile_derived_tables` routing is bypassed. At this point no
  query reaches the old emitters.
- [ ] **Deletion sweep.** Everything in §3's deleted list goes in one commit, and the
  index/Schema-shaped primitives are physically relocated into `hir/lower/` (their old
  orchestration callers are gone); the pinning suite now runs purely against the HIR
  pipeline; stale `ast_util` helpers and dead `Unsupported` strings swept.

## 6. Kept API surface (verbatim)

The signatures the HIR pipeline builds against, quoted from source so implementation needs no
lookup. All are kept unchanged.

`crates/gnitz-sql/src/plan/view/mod.rs`:

```rust
pub(crate) struct ViewChain {
    owner_vid: Option<u64>,
    pub segments: Vec<PlannedView>,
}
pub(crate) type EmitPieces = (Circuit, Vec<ColumnDef>, Vec<u32>);
impl ViewChain {
    pub fn new() -> Self
    pub fn owner_vid(&mut self, client: &mut GnitzClient) -> Result<u64, GnitzSqlError>
    pub fn add_segment(
        &mut self,
        client: &mut GnitzClient,
        emit: impl FnOnce(&mut GnitzClient, &mut ViewChain, u64) -> Result<EmitPieces, GnitzSqlError>,
    ) -> Result<(u64, Rc<Schema>), GnitzSqlError>
}
```

`crates/gnitz-core/src/circuit.rs` — every `CircuitBuilder` method the view emitters call:

```rust
pub fn new(view_id: u64, primary_source_id: u64) -> Self
pub fn input_delta(&mut self) -> NodeId
pub fn input_delta_bounded(&mut self, bound: Option<gnitz_wire::ScanBound>) -> NodeId
pub fn input_delta_tagged(&mut self, source_table_id: u64) -> NodeId
pub fn trace_scan(&mut self, table_id: u64) -> NodeId
pub fn filter(&mut self, input: NodeId, expr: Option<ExprProgram>) -> NodeId
pub fn map_expr(&mut self, input: NodeId, program: ExprProgram) -> NodeId
pub fn map_reindex(&mut self, input: NodeId, reindex_cols: &[usize], target_tcs: &[u8], program: ExprProgram) -> NodeId
pub fn map_hash_row(&mut self, input: NodeId, projection: &[usize], target_tcs: &[u8], branch_id: u8) -> NodeId
pub fn map(&mut self, input: NodeId, projection: &[usize]) -> NodeId
pub fn negate(&mut self, input: NodeId) -> NodeId
pub fn union(&mut self, a: NodeId, b: NodeId) -> NodeId
pub fn distinct(&mut self, input: NodeId) -> NodeId
pub fn positive_part(&mut self, input: NodeId) -> NodeId
pub fn positive_diff(&mut self, minuend: NodeId, subtrahend: NodeId) -> NodeId
pub fn join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId
pub fn join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId
pub fn join_with_trace_range_node(&mut self, delta: NodeId, trace_node: NodeId, n_eq: u8, rel: RangeRel) -> NodeId
pub fn partition_filter(&mut self, input: NodeId) -> NodeId
pub fn reduce(&mut self, input: NodeId, group_cols: &[usize], agg_func_id: u64, agg_col_idx: usize) -> NodeId
pub fn reduce_multi(&mut self, input: NodeId, group_cols: &[usize], agg_specs: &[(u64, usize)], global_ground: bool, out_key: ReduceOutKey) -> NodeId
pub fn reduce_multi_local(&mut self, input: NodeId, group_cols: &[usize], agg_specs: &[(u64, usize)], global_ground: bool, out_key: ReduceOutKey) -> NodeId
pub fn shard(&mut self, input: NodeId, shard_cols: &[usize]) -> NodeId
pub fn integrate_trace(&mut self, input: NodeId) -> NodeId
pub fn null_extend(&mut self, input: NodeId, right_col_type_codes: &[u64]) -> NodeId
pub fn sink(&mut self, input: NodeId) -> NodeId
pub fn build(self) -> Circuit
```

`crates/gnitz-core/src/protocol/types.rs`:

```rust
pub struct ColumnDef {
    pub name: String,
    pub type_code: TypeCode,
    pub is_nullable: bool,
    pub fk_table_id: u64,
    pub fk_col_idx: u64,
    pub is_serial: bool,
    pub is_hidden: bool,
}
// builders: ColumnDef::new(name, type_code, is_nullable), .serial(), .hidden()

pub struct Schema {
    pub columns: Vec<ColumnDef>,
    pub pk_cols: Vec<usize>,
}
// Schema::from_parts(columns, pk_cols) -> Result<Schema, &'static str>   (types.rs:267)
```

`crates/gnitz-wire/src/circuit.rs:319-323`:

```rust
pub struct ScanBound {
    pub idx_cols: crate::PkColList,
    pub desc: crate::RangeDescriptor,
}
```

`crates/gnitz-sql/src/bind/structural.rs` (shipped generic shapes; the four runtime leaf
impls resolve to the `R = usize` default, the view leaves will instantiate `R = HirRef`):

```rust
pub(crate) trait LeafBinder<R = usize> {
    fn bind_column(&self, e: &Expr) -> Result<BExpr<R>, GnitzSqlError>;
    fn bind_function(&self, f: &Function) -> Result<BExpr<R>, GnitzSqlError>;
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BExpr<R>, GnitzSqlError>;
    fn bind_subquery(&self, _e: &Expr) -> Result<BExpr<R>, GnitzSqlError> { /* default: Err(Unsupported) */ }
}
pub(crate) fn bind_structural<R: Clone, L: LeafBinder<R>>(expr: &Expr, leaf: &L) -> Result<BExpr<R>, GnitzSqlError>
```

`crates/gnitz-sql/src/bind/resolve.rs`:

```rust
impl Binder {
    pub(crate) fn resolve(&mut self, client: &mut GnitzClient, name: &str) -> Result<(u64, Rc<Schema>), GnitzSqlError>  // :180
    pub(crate) fn is_catalog_relation(&self, name: &str) -> bool                                                        // :158
}
pub(crate) type AliasMap = HashMap<String, ResolvedRelation>;                                                           // :17
pub(crate) fn find_unique_column<'a>(columns: impl IntoIterator<Item = &'a ColumnDef>, col_name: &str)
    -> Result<Option<usize>, GnitzSqlError>   // iterator-generic so &[HirCol] callers pass .iter().map(|c| &c.def)
pub(crate) fn resolve_qualified_column(table_alias: &str, col_name: &str, tables: &AliasMap) -> Result<usize, GnitzSqlError> // :80
```

`crates/gnitz-sql/src/lower.rs:428-436`:

```rust
pub(crate) fn compile_filter_program(pred: &BoundExpr, schema: &Schema) -> Result<Option<gnitz_core::ExprProgram>, GnitzSqlError>
```

`crates/gnitz-sql/src/codec/project_schema.rs:145`:

```rust
pub(crate) fn build_projection(projection: &[SelectItem], source_schema: &Schema) -> Result<(Vec<ProjItem>, Vec<ColumnDef>), GnitzSqlError>
```
