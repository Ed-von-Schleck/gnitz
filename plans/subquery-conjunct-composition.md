# Multiple EXISTS/IN conjuncts + operator-over-sub-plan composition

## Problem

The hidden-view chain substrate (atomic `create_view_chain`, dependency-ordered
backfill, cascading DROP, the `emit_*_pieces` / `*_over` source-override pattern,
`compile_join_to_hidden`) is shipped and proven. It already composes: multi-way joins
(inner + LEFT/RIGHT/FULL), self-joins, GROUP BY / DISTINCT directly over a join, and
DISTINCT/set-op **finals** over a compiled CTE sub-plan.

Two composition gaps remain, both "an operator whose input is itself a non-linear
sub-plan." The first has **no workaround** and is the real deliverable:

1. **Multiple `[NOT] EXISTS` / `[NOT] IN` conjuncts** in one WHERE
   (`… WHERE EXISTS(A) AND EXISTS(B)`). The semi/anti-join builder handles exactly one
   subquery conjunct; the classifier peels one and drops the rest into `local`
   (linear filters), where a second `EXISTS` fails to bind. There is no CTE workaround:
   an `EXISTS`-bodied CTE is not a compilable hidden body (`lower_linear` cannot bind an
   `EXISTS` in the WHERE), so it cannot be pre-materialized either.

2. **Set-op directly over a raw join, per side**
   (`(SELECT … FROM a JOIN b) UNION (SELECT … FROM c)`). `compile_set_op_side` rejects a
   JOIN'd FROM. The CTE form (`WITH j AS (a JOIN b) SELECT … FROM j UNION …`) works today,
   so this is low priority.

## Committed design

### 1. Multiple subquery conjuncts → a chain of semi/anti-join segments

Each `[NOT] EXISTS`/`[NOT] IN` conjunct becomes one hidden semi/anti-join segment over
the previous:

```
h0 = outer  WHERE subq0  [AND <linear locals>]
h1 = h0     WHERE subq1
...
final = h_{n-2} WHERE subq_{n-1}
```

A semi/anti-join emits the **outer's** surviving rows, so every segment carries the
outer's schema and natural PK — like a linear filter. A later conjunct's correlation
(`… WHERE inner.x = outer.y`) therefore resolves **by name** against the prior segment
(same column names as the base outer). No synthetic-PK / provenance machinery is needed;
this is the same shape as the linear-CTE chain, with a semi-join emit per link.

**Mechanics** (all in `gnitz-sql/src/plan/view/`):

- **Stage-A split of the semi-join emit** (`exists.rs`). Today `build_equi_exists` and
  `build_range_exists` each `alloc_table_id()` internally and call
  `create_view_with_circuit`. Refactor both to return `(circuit, cols, pk)` for a
  pre-allocated `view_id`; add `emit_exists_pieces(client, view_id, select, subq,
  outer_not, locals, binder, outer_override)` that dispatches to them; make
  `execute_create_exists_view` the thin single-conjunct wrapper (alloc + emit + create).
  This is behaviour-neutral — the existing EXISTS/IN e2e tests are the gate (as with the
  join / group-by / distinct splits already shipped).

- **Outer source-override.** The outer is resolved at `exists.rs` ~L89–90
  (`extract_table_name_and_alias` + `binder.resolve`). Thread an
  `Option<(u64, Rc<Schema>)>` override, mirroring `group_by::emit_group_by_pieces_over`
  and `set_op::compile_set_op_side`'s `source_override`: `Some` uses the prior hidden
  segment as the outer, keeping the **base outer's alias** so correlations resolve by
  name. `None` is the existing base-table path.

- **Peel all conjuncts.** Today `ViewShape::Subquery` (`dispatch.rs`) peels one subquery
  conjunct and returns the rest as `local`. Add a detector that collects **every**
  top-level `[NOT] EXISTS`/`[NOT] IN` conjunct (via `flatten_conjuncts`) plus the
  remaining linear conjuncts. Route to a new `create_subquery_chain_view` when the count
  is > 1 (the single-conjunct case stays on the existing `ViewShape::Subquery` path).

- **Chain + assemble.** For conjuncts `subq0..subq_{n-1}`: emit `h0` with
  `outer_override = None` and the linear locals folded in; emit `h1..final` each with
  `outer_override = Some(prior segment)`. Bundle via `create_view_chain` — reuse the
  `assemble_join_chain` helper (`dispatch.rs`) or a `HiddenSegment`-based equivalent;
  the hidden segments name `__h{final_vid}_{i}`.

- **NOT EXISTS / NOT IN ordering.** Each conjunct is independent (a filter over the
  running relation), so anti-join segments chain identically to semi-join segments; the
  `negated` flag rides each segment's `subq`. Order follows syntactic conjunct order.

**Gate.** `make verify` + `make e2e` (W=4). New e2e tests must check **weights**, not
just row presence: two EXISTS conjuncts (semi∘semi), EXISTS AND NOT EXISTS (semi∘anti),
IN AND EXISTS, each with incremental insert + retraction that flips membership at each
link, plus a linear local alongside; backfill (view created after data) and DROP-cascade
of the hidden segments.

### 2. Set-op directly over a raw join, per side (low priority)

Compile each set-op side whose FROM has joins to a hidden `H` via
`compile_join_to_hidden` (`dispatch.rs`), then run `compile_set_op_side` with a
`source_override = Some(H)` (the override parameter already exists). Detect at dispatch a
set-op whose left/right SELECT has `from[0].joins` non-empty and route to a chain that
emits H segment(s) per side + the set-op final via `set_op::emit_set_op_pieces`.

## Out of scope

- Deeper nonlinear-over-nonlinear composition (GROUP BY / DISTINCT over a DISTINCT or
  set-op or EXISTS sub-plan). The CTE body for those is itself rejected, so each would
  need the same "compile sub-plan to a hidden view + operator-over-H" treatment; value is
  low and there is no user demand yet.
- Correlated scalar subqueries, ANY/ALL, subqueries outside top-level WHERE conjuncts
  (unchanged from the current EXISTS/IN builder's restrictions).

## Testing

- Rust: the semi-join emit split is behaviour-neutral — existing `exists`/`in` unit and
  e2e tests are the byte-for-byte gate.
- Python e2e (W=4), all weight-verified: `test_subquery_chain.py` — two-EXISTS,
  EXISTS+NOT-EXISTS, IN+EXISTS, EXISTS+linear-local, three-conjunct, backfill,
  DROP-cascade, incremental membership flips at each link.

## Invariants preserved

- Single-source-per-epoch and the self-join guard: each semi-join segment's two sources
  (its running outer and its subquery's inner) are distinct relations; a subquery over
  the same base as the outer is the shared-source-branches case (separate epochs), the
  same as today's single-EXISTS builder.
- `MAX_CHAIN_SEGMENTS = 64` bounds the conjunct count.
