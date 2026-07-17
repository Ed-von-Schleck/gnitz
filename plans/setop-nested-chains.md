# Nested set-operation chains: `A UNION B UNION C` and parenthesized sides

## 1. Problem

Any set operation whose side is itself a set operation is unconditionally rejected — in CREATE
VIEW and ad-hoc alike. sqlparser (0.62) parses `A UNION B UNION C` left-associatively as
`SetOperation { left: SetOperation{A, Union, B}, op: Union, right: C }` (the Pratt loop in
`parse_remaining_set_exprs` accumulates on the left; INTERSECT binds tighter at precedence 20,
so unparenthesized `A UNION B INTERSECT C` nests on the right), and an explicitly parenthesized
side arrives as `SetExpr::Query(Box<Query>)`, not `SetExpr::Select`. `emit_set_op_pieces`
(`crates/gnitz-sql/src/plan/view/set_op.rs:226-241`) requires both sides to be
`SetExpr::Select` and errors "set operation: left side must be a SELECT" otherwise. Nothing
upstream inspects the sides (`ViewShape::classify` borrows them untouched,
`plan/view/dispatch.rs:274-297`), and no test in the tree exercises a 3-way chain.

Inline compilation of a nested chain into one circuit is **not** available: the engine's
compiled plan shape admits at most two exchange boundaries per circuit
(`CompileError::TooManyExchanges`, `crates/gnitz-engine/src/query/compiler/mod.rs:45-46,
469-478`) arranged as parallel pre-exchange sides with a single exchange phase per epoch
(`query/dag/exec.rs:59-102`) — and every set-op side is hashed + sharded
(`hash_shard_side`, `set_op.rs:108-127`), so three leaves already exceed the shape. The
composition mechanism the architecture provides for exactly this situation is the **chain
segment**: N-way joins compile one hidden view per intermediate step
(`plan_join_chain`, `join.rs:539-548`), and a set-op side can already read a chain-compiled
hidden view today via binder aliasing (the CTE path: the alias resolves through
`binder.resolve` inside `resolve_side_source`). This plan applies that same pattern to nested
set-op sides.

## 2. Design

### 2a. Recursive side normalization into hidden segments

`emit_set_op_pieces` gains a normalization step for each side before its existing body runs:

- `SetExpr::Select(s)` — unchanged, today's leaf pipeline.
- `SetExpr::SetOperation { .. }` — the subtree is compiled **recursively as a hidden segment
  view** via `chain.add_segment(client, |client, chain, vid| emit_set_op_pieces(...))`
  (`plan/view/mod.rs:99-128` — mints the vid before the circuit builds, auto-names per the
  `__h{owner}_{idx}` convention through `push_hidden`, so `DROP VIEW`'s cascade collects it,
  `crates/gnitz-core/src/client.rs:1283-1296`). The parent then consumes the hidden view as a
  **pre-resolved source**: the side becomes a synthetic bare-wildcard `Select` whose
  `(vid, schema)` is passed directly into `resolve_set_op_side` — which never inspects FROM
  (only `resolve_side_source` does, `set_op.rs:22-35`) — the exact `emit_distinct_pieces`
  pattern (`set_op.rs:407-418`). No binder alias: hidden names start with `_`, which
  `validate_user_identifier` rejects as an alias (`crates/gnitz-wire/src/catalog.rs:256-259`),
  and the join chain likewise references its intermediates by tid, never through the binder
  (`join.rs:675,683`). `emit_set_op_pieces`' signature grows `&mut ViewChain` (dispatch call
  site `plan/view/dispatch.rs:99-104` updated).
- `SetExpr::Query(q)` (parenthesized side) — recurse on `q.body` after rejecting a wrapper
  carrying **any** non-default field besides `body` (`with`, `order_by`, `limit_clause`,
  `fetch`, `locks`, `for_clause`, `settings`, `format_clause`, `pipe_operators` — sqlparser
  0.62 `Query`, `ast/query.rs:40-69`): `"only a plain set-operation or SELECT is supported
  inside a parenthesized set operation side"`. Parens-in-parens is handled by the recursion.
- Any other `SetExpr` variant (VALUES, …) keeps the existing rejection.

A left-deep chain of n leaves therefore compiles to n−1 segments (n−2 hidden + the final), each
an ordinary 2-side set-op circuit within the engine's supported shape. Chain length is bounded
by the existing `MAX_CHAIN_SEGMENTS = 64` enforcement (`create_view_chain`,
`crates/gnitz-core/src/client.rs:1073-1078`) — no new cap. With no nesting present, the emitted
circuit is byte-identical to today's.

### 2b. Semantics compose pairwise — no new rules

Because every node is a stock 2-side set-op over (table | view | hidden view) relations,
today's rules apply verbatim at each level, reading the nested side's composed schema off its
hidden view:

- **Types**: per-column `set_op_common_type` pairwise per node (`set_op.rs:277-315`); a nested
  side contributes its already-promoted output types. String/float/wide pairs keep today's
  rejection at whichever node pairs them.
- **Names** from the left side at every node (`set_op.rs:374-381`) — hence the leftmost SELECT
  overall. Duplicate-name checking stays where it is (the final schema per node,
  `reject_duplicate_column_names`, `set_op.rs:398`).
- **Nullability** composes per node with today's op rules (`Intersect: l && r`, `Except: l`,
  `Union[All]: l || r`, `set_op.rs:383-396`).
- **Weights**: each pairwise pipeline is exact bag arithmetic at any weight
  (`positive_diff = max(0, a−b)` clamps to `[0, i64::MAX]`; the `{0,1}` clamp exists only in
  the `Distinct` quantifier's explicit `set_op_leaves` distinct). One deliberate,
  Z-set-preserving consequence: an inner `UNION ALL`'s cross-branch duplicates keep their
  distinct branch-salted `_set_pk`s in the hidden store (two weight-1 rows) and coalesce into
  one summed-weight entry only when the parent re-hashes the visible content — logical
  multiplicity is exact at every level, and the folding matches the existing convention that
  within-branch duplicates already fold to weight ≥ 2.
- **Quantifiers**: `ByName`/`AllByName`/`DistinctByName` and `Minus` stay rejected at every
  depth; `None` ≡ `Distinct` as today (`set_op.rs:321-367`).

### 2c. The same-relation guard is already right

The per-circuit guard (`INTERSECT`/`EXCEPT` reject `left_tid == right_tid`,
`set_op.rs:270-274`) is unchanged and remains sufficient: a nested side always enters its
parent as a **distinct hidden-view source id**, so `(A UNION B) INTERSECT A` becomes legal —
and correct. A push to `A` reaches the parent through two *separate* epochs (the direct `A`
edge, and the cascade edge after the hidden view's own tick), which is precisely the
shared-source-branches discipline the engine already defines for `t ⋈ view-over-t` (CLAUDE.md
§3: work is queued per `(view, source_id)` edge; the guard's discriminator is source-id
equality, not base-table overlap — pinned today by the accepted two-views-over-one-base test,
`planner_set_ops.rs:372-414`). The weight-clamp is a *unary* operator whose transitions
telescope across epochs (`op_weight_clamp` emits `clamp(w_old+Δw) − clamp(w_old)` against its
own history, `ops/distinct.rs:17-30`), so the direct and cascaded epochs compose exactly in
either order — the bilinear cross-term concern is join-only. Direct `X INTERSECT X` /
`X EXCEPT X` stays rejected exactly as today; the textually identical
`(A UNION B) INTERSECT (A UNION B)` is accepted (two distinct hidden segments, weight-correct,
double-materialized — the cost of not proving subtree equivalence).

### 2d. Cost posture

Each hidden segment is a real maintained view: its intermediate result is materialized and
incrementally maintained, identical in kind to an N-way join's intermediate segments. That is
the architecture's price for composition beyond one circuit, already accepted for joins; a
chain's storage/maintenance cost scales with its intermediates. Ad-hoc SELECT is unaffected:
a nested chain is multi-segment, and multi-segment ad-hoc queries are rejected (the fix targets
CREATE VIEW); the executor's multi-segment error string gains "nested set operation" in its
shape list so the rejection stays truthful.

## 3. Testing

Rust (`crates/gnitz-sql/tests/planner_set_ops.rs` additions):
- 3-way `UNION ALL` / `UNION` / `EXCEPT` / `INTERSECT` chains create views whose rows and
  **weights** match hand-computed Z-sets at W=4, including after retractions driven through
  every leaf source (cascade maintenance through the hidden segments).
- Parenthesized nesting: `(A UNION ALL B) INTERSECT C`, `A EXCEPT (B UNION C)`; precedence
  pinning: `A UNION B INTERSECT C` ≡ `A UNION (B INTERSECT C)`.
- Weight composition: an inner `UNION ALL` producing weight-2 content consumed by an outer
  `EXCEPT ALL` / `INTERSECT ALL` — bag arithmetic exact.
- Shared source across levels: `(A UNION B) INTERSECT A` accepted and weight-correct under
  pushes and retractions to `A`; `X INTERSECT X` still rejected;
  `(A UNION B) INTERSECT (A UNION B)` accepted and weight-correct (two hidden segments).
- Rejections at depth: BY NAME inside a nested side; ORDER BY / LIMIT / FOR UPDATE inside a
  parenthesized side; float/string pairs at an inner node; column-count mismatch between an
  inner chain and its outer sibling; a 66-leaf chain trips `MAX_CHAIN_SEGMENTS`.
- Composition: names from the leftmost SELECT; type promotion across three widths
  (`U8 ∪ I8 ∪ U32`, pairwise ladder); nullability across mixed ops.
- Structure: a 3-way chain compiles to exactly 2 segments, each set-op-shaped with no join
  opcodes (extends `test_set_op_distinct_join_free_circuit_shape`,
  `planner_set_ops.rs:738-769`); `DROP VIEW` of the user view cascades all hidden set-op
  segments.
E2E (`crates/gnitz-py/tests/test_set_ops.py` additions, `GNITZ_WORKERS=4`):
- Differential 3-way tests mirroring the existing 2-way differential classes (random ops +
  retractions vs a Python bag oracle) for chains and parenthesized mixes.
- `make verify` and `make e2e WORKERS=4` green.

## 4. Sequencing

- [ ] `set_op.rs`: side normalization (recursive hidden-segment emission via
      `chain.add_segment` for `SetOperation` sides, `Query` unwrap with the full
      non-default-field rejection, pre-resolved-source consumption of hidden sides) wrapped
      around the unchanged 2-side emitter; `emit_set_op_pieces` signature grows
      `&mut ViewChain`; the executor's multi-segment message gains "nested set operation".
- [ ] Rust integration tests (`planner_set_ops.rs` additions incl. structure + cascade-drop).
- [ ] E2E differential additions; full suite; `make verify`.
