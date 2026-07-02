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

The planner already lowers a single `[NOT] EXISTS`/`[NOT] IN` conjunct over a
Simple-shaped view into semi/anti circuits derived from the outer-join null-fill
(`plan/view/exists.rs`, dispatched via a `ViewShape::Subquery` variant), and
`bind_structural` desugars IN-lists and rejects out-of-position subqueries with targeted
errors. **These symbols do not exist at authoring time** — they are the deliverable of
the preceding increment; the SemiAnti scope items and Sequencing step 6 are hard-blocked
on them, while every other step stands on the current tree. Additionally re-verify two
code-level facts this plan leans on, both plausible targets of concurrent join-machinery
work: the distributed backfill tail resolves a view's sources via `get_source_ids`
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
- **General CTEs**: each `WITH x AS (SELECT …)` lowers once; an anchor-bearing CTE
  becomes one hidden view shared by all its references (DAG sharing); a linear CTE
  inlines per reference. The pass-through-only CTE inliner (`dispatch.rs:158-274`) is
  deleted.
- **Multiple subquery conjuncts** and subquery conjuncts over any sub-plan: each
  `[NOT] EXISTS`/`[NOT] IN` conjunct becomes its own Semi/Anti anchor (per-conjunct
  restrictions unchanged from the current exists builder).
- **Self-joins and same-source set-ops**: the right occurrence is wrapped in an
  auto-generated hidden pass-through view, giving it a distinct source id — the
  documented two-views-over-one-base pattern (`test_shared_source_incremental.py`),
  now automatic. The extra store copy is the accepted cost.

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
        residual: Vec<sqlparser::ast::Expr>, // INNER only, bound at emit vs merged schema
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

Every node exposes `fn schema(&self) -> &[ColumnDef]` (computed at construction).
`Join.residual` stays as raw AST fragments because `build_residual_filter_prog`
(`predicates.rs:468`) binds against the merged `[_join_pk…, A, B]` schema that only
exists at emit time; everything else is `BoundExpr`/indices.

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
- CTEs: lower each CTE body once into a `Rel`; references clone linear trees or share
  the anchor-bearing tree by reference (an `Rc<Rel>` interned per CTE name).
- Same source id appearing twice anywhere in the tree: wrap the second occurrence in
  `PassThroughView(Source)` — realized by the splitter as a hidden linear view.

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

**Backfill-order invariant (load-bearing):** every hidden view contains an anchor, and
every anchor circuit carries a `Join` node or an `ExchangeShard`, so
`view_seeds_exchange_backfill` (`dag/mod.rs:828-840`) is true for every chain member —
all of them take the distributed backfill tail, which runs strictly in VIEW_TAB row
order (`executor.rs:1678-1696`, `family_pks_by_sign` insertion order; each
`fan_out_backfill` is fully synchronous and the worker flushes the view after its last
chunk, `worker/mod.rs:1233`, so upstream is materialized before downstream scans). The
assembler emits VIEW_TAB rows in topo order. Note the final user view carries the
*smallest* vid but the *last* row (hidden names embed its id), so PK order ≠ row order
by construction — any future PK-sort/consolidation of the VIEW_TAB batch on this path
would silently break the tail; therefore `handle_ddl_txn` gains a topological-order
assertion over `new_view_ids` (dep-map lookup per pair; abort the bundle, don't
backfill misordered), pinned by a deliberately misordered regression test. The one
hidden-view exception is the self-join pass-through, which is linear and takes the
**inline** path — safe for a different reason than "source pre-exists": the inline
`backfill_view` is skipped on the master (its `active_part_start == active_part_end`
gate, `hooks.rs:502`) and runs on **each worker** during `ddl_sync` hook firing
(`store_io.rs:443`), filling the worker's own partition from the pass-through's source
(a base table or pre-existing view — a `debug_assert` in the splitter pins that a
linear hidden segment's input is a pre-existing relation id, never a same-bundle
exchange view).

### 4. Segment emitters (refactor of `plan/view/*`)

The five shape builders are dismantled into emitters keyed by anchor kind, each
producing `(Circuit, Vec<ColumnDef>, Vec<u32> /* pk */)` from a segment whose inputs are
plain relation ids:

- `emit_linear` — from `simple.rs` (filter + projection/expr-map).
- `emit_join` — from `join.rs` (equi + range/band, all join kinds, residuals, null-fill),
  now also compiling the segment's post-anchor linear `Filter`/`Project` tail before the
  sink (the residual-filter splice point, `join.rs:469-484`, generalizes to any bound
  filter over the merged schema).
- `emit_semi_anti` — from `plan/view/exists.rs`.
- `emit_reduce` — from `group_by.rs`.
- `emit_distinct` / `emit_set_op` — from `set_op.rs`.

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
/// (topological) order — VIEW_TAB row order drives hook firing and the
/// distributed backfill tail. Returns the vids in input order.
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

Atomicity: the existing per-family `precheck → mark → apply` loop
(`executor.rs:1576-1591`) plus `compensate_stage_a` (`executor.rs:1592-1607`,
`write_path.rs:748-819`; its weight-homogeneity `debug_assert` holds for an all-`+1`
chain) already gives all-or-nothing; a mid-chain failure (e.g. user-name collision
caught by `precheck_qname_unique`) rolls back every applied family. The only engine
change on this path is the `new_view_ids` topo-order assertion (§3).

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
2. **`create_view_chain`** (client) replacing `write_circuit_rows`; Rust integration
   test via `gnitz-test-harness`: two hand-built chained circuits (linear-over-base +
   join-over-it) created in one bundle over pre-populated bases; assert atomic rollback
   on a name collision in the second view; assert backfill correctness at W=4.
3. **Cascading DROP** (`drop_view` + `drop_view_if_exists` + drain tolerance), e2e:
   drop user view → hidden members gone; DROP TABLE under a chain still RESTRICTs.
4. **IR + lowering + emitters, behavior-neutral**: all currently-supported view shapes
   route through lower→split→emit and produce circuits the engine compiles identically;
   `ViewShape` and CTE inliner deleted. Gate: full existing e2e suite green; circuit
   golden tests (node/edge multiset comparison) for one view of each legacy shape.
5. **Multi-anchor surface**: multi-way joins, WHERE-over-join, GROUP-BY-over-join,
   DISTINCT/set-ops over sub-plans, derived tables, general CTEs (shared and inlined).
6. **Multiple subquery conjuncts + subqueries over sub-plans** (SemiAnti anchors
   composed by the splitter).
7. **Self-join pass-through** wrapping + tests.

Each step compiles and passes `make verify` + `make e2e` independently.

## Files touched

| File | Change |
|------|--------|
| `crates/gnitz-sql/src/plan/lp.rs` | new: `Rel` IR, lowering, splitter |
| `crates/gnitz-sql/src/plan/view/dispatch.rs` | classification deleted; lower→split→emit pipeline; name validation |
| `crates/gnitz-sql/src/plan/view/{simple,join,exists,group_by,set_op}.rs` | entry points become segment emitters over relation-id inputs |
| `crates/gnitz-sql/src/plan/ddl.rs` | CREATE TABLE/SCHEMA name validation; DROP leading-`_` rejection |
| `crates/gnitz-core/src/client.rs` | `create_view_chain` (replaces `write_circuit_rows`), cascading `drop_view`, `drop_view_if_exists` |
| `crates/gnitz-engine/src/runtime/orchestration/executor.rs` | topological-order assertion over `new_view_ids` before the backfill tail |
| `crates/gnitz-engine/tests/` (or `test_support`) | chain-bundle integration test via `gnitz-test-harness` |
| `crates/gnitz-py/tests/test_multi_join.py` | new e2e suite (multi-join, group-by-over-join, derived tables, CTEs, chains) |
| `crates/gnitz-py/tests/test_catalog.py` | hidden-view drop/cascade/visibility cases |

## Testing

- **Planner units**: lowering of each SQL form to the expected `Rel` tree; splitter
  segment boundaries (linear folds upstream; one anchor per segment; pass-through
  wrapping; shared CTE = one hidden segment); name-reservation rejections.
- **Golden circuit tests**: every legacy single-anchor shape emits a circuit with the
  same node/edge multiset as before the refactor.
- **Client/engine integration**: multi-view bundle atomicity (success, mid-bundle
  precheck failure → zero residue, crash-recovery replay of a committed chain zone);
  co-drop bundle; multi-block-per-family regression guard (assert assembler emits one
  entry per tid).
- **E2E behavior** (W=4, weight-asserted): 3- and 4-way joins incl. outer steps;
  `WHERE` over join views; `GROUP BY` over a join with incremental retractions crossing
  group boundaries; EXISTS + EXISTS + local WHERE in one view; derived table with
  aggregate inside; CTE referenced twice (shared hidden view — one store, two readers);
  self-join; chain creation over populated bases (backfill order) and boot-restart
  rebuild of a chain; `DROP VIEW` cascade; `DROP TABLE` blocked under a chain.
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
  engine-side co-drop and N-view ingest paths are exercised, not modified.
- Base-table positivity, (PK, payload) identity, and OPK conventions untouched.
