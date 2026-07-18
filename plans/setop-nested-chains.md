# Nested set-operation chains: `A UNION B UNION C` and parenthesized sides

## 1. Problem

Any set operation whose side is itself a set operation is unconditionally rejected — in CREATE
VIEW and ad-hoc alike. sqlparser (0.62) parses `A UNION B UNION C` left-associatively as
`SetOperation { left: SetOperation{A, Union, B}, op: Union, right: C }` (the Pratt loop in
`parse_remaining_set_exprs`, `src/parser/mod.rs:14602-14634`, accumulates on the left; UNION/
EXCEPT/MINUS bind at precedence 10 and INTERSECT at 20, so unparenthesized `A UNION B INTERSECT C`
nests on the right as `A UNION (B INTERSECT C)`), and an explicitly parenthesized side arrives as
`SetExpr::Query(Box<Query>)` (`parse_query_body`, `src/parser/mod.rs:14576-14580`), not
`SetExpr::Select`. `emit_set_op_pieces` (`crates/gnitz-sql/src/plan/view/set_op.rs:226-241`)
requires both sides to be `SetExpr::Select` and errors "set operation: left/right side must be a
SELECT" otherwise. Nothing upstream inspects the sides (`ViewShape::classify` borrows them
untouched, `plan/view/dispatch.rs:277-297`), and no test in the tree exercises a 3-way chain or a
parenthesized side.

Inline compilation of a **mixed-operator** nested chain into one circuit is **not** available. A
compiled plan's `PlanShape::Exchanged` is N **parallel** pre-exchange sides feeding **one**
post-combine phase (`crates/gnitz-engine/src/query/compiler/mod.rs:172-181, 402-468`; the evaluator,
`query/dag/exec.rs:59-102`, runs each active side to its `ExchangeShard`, relays it once, then runs a
single post phase) — it cannot express a **sequential** exchange (post-combine → re-hash → exchange
again). A mixed chain needs exactly that: set membership is an in-circuit content hash re-salted per
level (UNION ALL salts its branches with distinct `branch_id`s; INTERSECT/EXCEPT need `branch_id 0`
on both sides to co-hash), so an inner `UNION ALL` feeding an outer `INTERSECT` must re-hash the
union's output to co-hash with the outer sibling — a second exchange downstream of the first combine,
which the parallel shape cannot hold. (The `sides.len() ∈ {1,2}` match arm — more is
`CompileError::TooManyExchanges`, `compiler/mod.rs:45-46, 469-478` — is structurally generic over N,
so a **pure** single-operator chain could in principle flatten to N parallel sides; this plan folds
pure and mixed chains alike into one uniform mechanism rather than maintaining a second flattening
path.) Every set-op side is hashed + sharded (`hash_shard_side`, `set_op.rs:108-127`). The
composition mechanism the architecture provides for exactly this situation is the **chain segment**:
an N-way join compiles
one hidden view per intermediate step (`plan_join_chain` → `chain.add_segment`, `join.rs:675`; the
self-join pass-through wrapper at `join.rs:595`) and reads each intermediate by its view id via
`input_delta_tagged(tid)` (`join.rs:683, 899-900`), never through the binder. This plan applies
that same pattern to nested set-op sides.

## 2. Design

### 2a. Recursive side normalization into hidden segments

Extract per-side resolution from `emit_set_op_pieces` into one helper, `normalize_set_op_side`,
that handles the three side shapes and recurses. `emit_set_op_pieces`' signature grows
`chain: &mut ViewChain` (the sole caller, `dispatch.rs:104`, already holds `chain` and passes it).

The current per-side prologue in `emit_set_op_pieces` — the two `match … { SetExpr::Select(s) => s,
_ => Err(…) }` blocks (`set_op.rs:226-241`) and the four `resolve_side_source` /
`resolve_set_op_side` lines (`set_op.rs:251-256`) — is replaced by two calls:

```rust
pub(crate) fn emit_set_op_pieces(
    client: &mut GnitzClient,
    view_id: u64,
    op: SetOperator,
    set_quantifier: SetQuantifier,
    left: &SetExpr,
    right: &SetExpr,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
) -> Result<EmitPieces, GnitzSqlError> {
    let mut cb = CircuitBuilder::new(view_id, 0);
    let right_branch_id = matches!((op, set_quantifier), (SetOperator::Union, SetQuantifier::All)) as u8;

    let (left_filtered, left_proj, left_cols, left_tid) =
        normalize_set_op_side(client, &mut cb, chain, binder, left, "set operation")?;
    let (right_filtered, right_proj, right_cols, right_tid) =
        normalize_set_op_side(client, &mut cb, chain, binder, right, "set operation")?;

    // …everything from the same-relation guard (set_op.rs:270) through the return
    //   value (set_op.rs:401) is UNCHANGED…
}
```

The helper:

```rust
/// Normalize one set-op side into the 4-tuple `emit_set_op_pieces` consumes —
/// `(filtered_node, proj_indices, out_cols, source_tid)` — recursing on nested set
/// operations. A `Select` leaf runs today's resolve pipeline on the parent `cb`; a
/// `SetOperation` side compiles to a hidden segment on `chain` and is consumed as a
/// bare pass-through of that hidden view's visible columns; a parenthesized `Query`
/// side has its envelope rejected and recurses on the inner body.
fn normalize_set_op_side(
    client: &mut GnitzClient,
    cb: &mut CircuitBuilder,
    chain: &mut ViewChain,
    binder: &mut Binder<'_>,
    side: &SetExpr,
    context: &str,
) -> Result<(gnitz_core::NodeId, Vec<usize>, Vec<ColumnDef>, u64), GnitzSqlError> {
    match side {
        SetExpr::Select(select) => {
            let src = resolve_side_source(client, binder, select, context)?;
            resolve_set_op_side(select, cb, context, false, src)
        }
        SetExpr::SetOperation { op, set_quantifier, left, right } => {
            // Compile the subtree as a hidden segment view (add_segment mints the vid
            // before the circuit builds and names it `__h{owner}_{idx}` via push_hidden,
            // so DROP VIEW's prefix cascade collects it). `binder` is captured by the
            // closure; the reborrow ends when add_segment returns, freeing it for the
            // sibling side. `left`/`right` are `&Box<SetExpr>`; deref-coerce to `&SetExpr`.
            let (vid, schema) = chain.add_segment(client, |client, chain, vid| {
                emit_set_op_pieces(client, vid, *op, *set_quantifier, left, right, binder, chain)
            })?;
            // Consume the hidden view as a bare pass-through: read its delta by vid and
            // project its VISIBLE columns. The hidden `_set_pk` at slot 0 is dropped by
            // `visible_columns`, so the parent re-hashes fresh visible content into its
            // own `_set_pk`. No WHERE, no filter, no float re-check (a set-op output is
            // never float — every leaf already ran `reject_float_keys`).
            let node = cb.input_delta_tagged(vid);
            let (proj_indices, out_cols): (Vec<usize>, Vec<ColumnDef>) =
                schema.visible_columns().map(|(i, c)| (i, c.clone())).unzip();
            Ok((node, proj_indices, out_cols, vid))
        }
        SetExpr::Query(q) => {
            // A parenthesized side. Reject the whole `Query` envelope (WITH, ORDER BY,
            // LIMIT/OFFSET, FETCH, FOR UPDATE/SHARE, FOR XML/JSON, SETTINGS, FORMAT, pipe
            // operators) via the crate's single exhaustive `Query` destructure, then
            // recurse on the inner body. A parenthesized single SELECT thus collapses
            // back to a leaf and adds no segment; parens-in-parens unwind by recursion.
            reject_unhonored_query_clauses(q, HonoredQueryClauses::NONE, context)?;
            normalize_set_op_side(client, cb, chain, binder, &q.body, context)
        }
        _ => Err(GnitzSqlError::Unsupported(format!(
            "{context}: only a SELECT or a nested set operation is supported as a side"
        ))),
    }
}
```

Imports to add in `set_op.rs`: `ViewChain` (`use crate::plan::view::{EmitPieces, ViewChain};`)
and `reject_unhonored_query_clauses, HonoredQueryClauses` (extend the existing
`crate::plan::validate::{…}` use). `SetExpr`, `CircuitBuilder`, `input_delta_tagged`, and
`Schema::visible_columns` are already in scope / used (`set_op.rs:16-17, 84, 147`).

Mechanics that make this sound, all verified against the join-chain precedent:

- **Recursion terminates and is well-typed.** The cycle is
  `normalize_set_op_side → chain.add_segment::<C> → C → emit_set_op_pieces → normalize_set_op_side`.
  The closure `C` calls the *named* `emit_set_op_pieces`, so `C`'s type does not depend on itself —
  one monomorphization, runtime recursion only (identical to `compile_hidden_body`'s closure calling
  `plan_join_chain`, which itself calls `add_segment`).
- **Segment order and naming.** `build_query_segments` allocates `owner_vid` (the final view's id)
  *before* the shape match (`dispatch.rs:97`), so `push_hidden`'s `owner_vid(client)` returns it
  without re-allocating; each `add_segment` mints a fresh vid and pushes the deepest subtree first.
  A left-deep chain of n leaves compiles to n−1 segments (n−2 hidden + the final), each an ordinary
  2-side set-op circuit (`sides.len() == 2`), within the supported shape. With no `SetOperation`
  appearing as a *side* (the non-nested case, including a parenthesized single SELECT), zero hidden
  segments are added and the emitted circuit is byte-identical to today's.
- **Chain length** is bounded by the existing `MAX_CHAIN_SEGMENTS = 64` check in
  `create_view_chain` (`crates/gnitz-core/src/client.rs:191, 1055-1060`) — no new cap.
- **Dependency cascade is automatic.** The parent's `input_delta_tagged(vid)` emits a
  `ScanDelta { source: vid }` node; `Circuit::dependencies()` collects it and `create_view_chain`
  writes a `DEP_TAB` row `(parent_vid, vid)` (`client.rs:1139-1143`). A push to a base table thus
  cascades base → hidden segment → parent through ordinary per-`(view, source_id)` edge evaluation.
- **DROP cascade is automatic.** Hidden segments are named `__h{owner}_{idx}`
  (`gnitz_core::hidden_view_name`, `client.rs:197-205`); `drop_view` collects every catalog view
  whose name `starts_with("__h{owner}_")` and retracts them with the user view in one atomic
  `VIEW_TAB` batch (`client.rs:1250-1281`, matcher `1699-1715`). No binder alias is created — hidden
  names start with `_`, which `validate_user_identifier` rejects as a user alias
  (`crates/gnitz-wire/src/catalog.rs:252-260`) — and the parent references the segment by vid, not
  by name, exactly as the join chain does.
- **Late-rejection is atomic.** If a sibling side (or the outer node's BY NAME / type / column-count
  check) rejects *after* an earlier side already pushed a hidden segment, the whole build fails and
  the local `chain` is discarded before `create_view_chain` — nothing commits. Durable
  `alloc_table_id`s consumed by the discarded segments simply advance the id sequence, identical to
  the join chain's existing behavior and benign.

**Top-level parentheses.** A fully-parenthesized body — `CREATE VIEW v AS (A UNION B)`, or the same
as an ad-hoc SELECT — arrives as `query.body = SetExpr::Query(q)`, which `ViewShape::classify`
(`dispatch.rs:277-297`) today routes to its `_ =>` arm, the misleading "CREATE VIEW only supports
SELECT". Enabling parenthesized *sides* without this would leave `(A UNION B)` as a whole body failing
while the sides of `A UNION (B UNION C)` work. Add a `SetExpr::Query(q)` arm to `classify` that peels
the envelope and re-classifies the body — `reject_unhonored_query_clauses(q,
HonoredQueryClauses::NONE, "CREATE VIEW")?; return ViewShape::classify(q);` (both symbols already
imported at `dispatch.rs:13`) — so top-level and side handling are symmetric, `((A UNION B))` unwinds
by the same recursion, and the shared ad-hoc path stops emitting the wrong-surface message.

### 2b. Semantics compose pairwise — no new rules

Every node is a stock 2-side set-op over (table | view | hidden view) relations, so today's rules
apply verbatim at each level, reading the nested side's composed schema off its hidden view:

- **Types**: per-column `set_op_common_type` pairwise per node (`set_op.rs:45-51, 291-303`); a nested
  side contributes its already-promoted output types (its `out_cols` are the hidden view's visible
  `ColumnDef`s). Each accepted pairwise step is a value-preserving widen to a concrete ≤8-byte
  integer (else `None` → reject), so the left-associative ladder is value-preserving end-to-end and
  equal logical values co-hash after the parent's `map_hash_row` widening. `U64` vs `I64`, any
  16-byte pair, and string/float pairs stay rejected at whichever node pairs them.
- **Names** from the left side at every node (`set_op.rs:376-381`) — hence the leftmost SELECT
  overall. Duplicate-name checking stays per node on that node's final schema
  (`reject_duplicate_column_names`, `set_op.rs:398`).
- **Nullability** composes per node with today's op rules (`Intersect: l && r`, `Except: l`,
  `Union[All]: l || r`, `set_op.rs:391-395`). Pairwise composition is a conservative
  over-approximation that only ever tightens when the op forbids NULL, so it never mislabels.
- **Weights**: each pairwise pipeline is exact bag arithmetic at any weight (`positive_part =
  max(0, a−b)` clamps to `[0, i64::MAX]`; the `{0,1}` clamp exists only in `set_op_leaves`' explicit
  DISTINCT). The one Z-set-preserving consequence to note: an inner `UNION ALL`'s cross-branch
  duplicate keeps two branch-salted `_set_pk`s in the hidden store (two weight-1 rows, same visible
  content). The parent reads only the visible content and re-hashes it with its **own** per-side
  `branch_id` (`reindex_hash_row` hashes `branch_id + payload`, `ops/reindex.rs`), which **erases the
  inner salt** — the physical split is invisible upward — so the two rows collapse to one (PK,
  payload) whose weights are summed: by `op_weight_clamp`'s `into_consolidated` before the clamp
  (`ops/distinct.rs:39`) for an INTERSECT/EXCEPT/DISTINCT parent, or by the output-store integrate
  for a UNION ALL parent. This is the *same* mechanism the existing 2-way ALL path relies on when two
  base rows project to identical content within one branch; nesting adds no new path. The cross-width
  widen is value-preserving and runs **before** the hash (`widen_native_le`, `expr/plan.rs`), so equal
  logical values co-hash across a promotion ladder. Logical multiplicity is exact at every level
  regardless of physical row split.
- **Quantifiers**: `ByName`/`AllByName`/`DistinctByName` and `Minus` stay rejected at every depth;
  `None` ≡ `Distinct` as today (`set_op.rs:319-367`).

### 2c. The same-relation guard is already right

The per-circuit guard (`INTERSECT`/`EXCEPT` reject `left_tid == right_tid`, `set_op.rs:270-274`) is
unchanged and remains sufficient: `add_segment` mints a fresh vid per subtree, so any side that is a
nested set-op enters its parent as a **distinct source id** and the guard can only fire when *both*
sides are the same leaf relation. Thus `(A UNION B) INTERSECT A` becomes legal and correct, while
direct `X INTERSECT X` / `X EXCEPT X` stays rejected, and the textually identical
`(A UNION B) INTERSECT (A UNION B)` is accepted (two distinct hidden segments — weight-correct,
double-materialized; the cost of not proving subtree equivalence).

A push to a shared base `A` reaches the parent through two *separate* epochs (the direct `A` edge
and the cascade edge after the hidden segment's own tick), which is exactly the shared-source-branch
discipline the engine already defines for `t ⋈ view-over-t` (CLAUDE.md §3: work is queued per
`(view, source_id)` edge; the guard's discriminator is source-id equality, not base-table overlap —
pinned by the accepted two-views-over-one-base tests, `planner_set_ops.rs:372-416`). The weight-clamp
is a **unary** operator whose transitions telescope against its own integral trace — `op_weight_clamp`
emits `clamp(w_old + Δw) − clamp(w_old)` per consolidated (PK, payload) (`ops/distinct.rs:21-30,
78-79`) — so the direct and cascaded epochs compose to the correct final state in either order. The
bilinear cross-term concern is join-only, and the INTERSECT/EXCEPT algebra carries no join/anti-join
node. A dependent read observes the settled post-cascade state, never the consistent-but-stale
mid-cascade transient — a pre-existing engine visibility property (the same one the 2-way set-op and
`t ⋈ view-over-t` paths already rely on), not something nesting changes.

### 2d. Cost posture

Each hidden segment is a real maintained view — materialized and incrementally maintained. For a
**mixed** chain this is load-bearing, exactly as an N-way join's intermediates are. For a **pure
`UNION ALL`** chain it is avoidable overhead: the n−2 left-deep segments are cumulative
(seg₀ = A∪B, seg₁ = seg₀∪C, …), so storage is ~triangular (≈O(n·|result|)) and a push to a leaf
cascades through O(n) sequential epoch rounds. This plan accepts that cost to keep **one** uniform
mechanism instead of a second flattening path (which would need the sequential-exchange engine change
of §1); the chain length — and thus the worst case — is bounded by `MAX_CHAIN_SEGMENTS = 64`.
Set-operation *bodies* of CTEs and derived tables remain rejected after this plan (`plain_select_body`
unwraps only a `SELECT`, `validate.rs:379-387`) — a separate FROM-side surface, out of scope here.
Ad-hoc SELECT is unaffected: a nested chain is multi-segment, and multi-segment ad-hoc queries are
rejected (the fix targets CREATE VIEW). Two sites carry the multi-segment shape list and both gain
"nested set operation" so the rejection stays truthful:

- the runtime rejection string, `crates/gnitz-sql/src/dml/select.rs:361-366` (`segments.len() != 1`):
  `"…(3+-way join, self-join, DISTINCT / GROUP BY over a join, correlated subquery, non-pass-through
  CTE, a derived table in FROM, or a nested set operation); use CREATE VIEW"`.
- the mirroring doc comment on `compile_query_to_circuit`, `plan/view/dispatch.rs:184-186`.

## 3. Testing

Rust (`crates/gnitz-sql/tests/planner_set_ops.rs` additions; helpers `read_view`, `col_idx`,
`i64_at`, `batch.weights[r]`, `scan_circuit_nodes`, `opcode_node_count`, `resolve_table_or_view_id`,
`gnitz_core::hidden_view_name` already exist — see `test_set_ops_compound_pk` and
`test_set_op_distinct_join_free_circuit_shape`):

- **3-way chains** — `UNION ALL` / `UNION` / `EXCEPT` / `INTERSECT` chains create views whose rows
  and **summed weights per visible tuple** match hand-computed Z-sets at W=4, including after
  retractions driven through every leaf source (cascade maintenance through the hidden segments).
- **Parenthesized nesting** — `(A UNION ALL B) INTERSECT C`, `A EXCEPT (B UNION C)`; precedence
  pinning `A UNION B INTERSECT C` ≡ `A UNION (B INTERSECT C)`; the collapse property:
  `(SELECT c FROM a) UNION SELECT c FROM b` and `((SELECT c FROM a)) UNION …` are accepted,
  weight-correct, and add **no** hidden segment (`hidden_view_name(final_vid, 0)` does not resolve —
  single-segment, byte-identical to the unparenthesized form); and the top-level fully-parenthesized
  body `CREATE VIEW v AS (A UNION B)` / `(A UNION B UNION C)` (the `classify` peel) is accepted and
  weight-matches its unparenthesized form.
- **Weight composition** — an inner `UNION ALL` producing weight-2 content consumed by an outer
  `EXCEPT ALL` / `INTERSECT ALL`; assert the summed multiplicity, not physical row count.
- **Shared source across levels** — `(A UNION B) INTERSECT A` accepted and weight-correct under
  pushes and retractions to `A`; `X INTERSECT X` still rejected; `(A UNION B) INTERSECT (A UNION B)`
  accepted and weight-correct (two hidden segments).
- **Rejections at depth** — BY NAME inside a nested side; ORDER BY / LIMIT / FOR UPDATE / a nested
  WITH inside a parenthesized side (each yields the specific `reject_unhonored_query_clauses`
  message); float/string pairs at an inner node; column-count mismatch between an inner chain and its
  outer sibling; a 66-leaf chain trips `MAX_CHAIN_SEGMENTS`.
- **Composition** — names from the leftmost SELECT; type promotion across three widths
  (`U8 ∪ I8 ∪ U32` → `I64`, pairwise ladder); nullability across mixed ops.
- **Structure** — a 3-way chain compiles to exactly 2 segments: the final view and `__h{final}_0`
  each carry set-op opcodes (a `positive_part`/`union` as appropriate) and zero
  `OPCODE_JOIN_DELTA_TRACE`; `__h{final}_1` does not resolve (extends
  `test_set_op_distinct_join_free_circuit_shape`). `DROP VIEW` of the user view cascades: after it,
  neither the user view nor `__h{final}_0` resolves.

E2E (`crates/gnitz-py/tests/test_set_ops.py` additions, `GNITZ_WORKERS=4`):

- Differential 3-way tests mirroring the existing 2-way differential classes (random ops +
  retractions vs a Python bag oracle) for chains and parenthesized mixes.
- `make verify` and `make e2e WORKERS=4` green.

## 4. Sequencing

- [ ] `set_op.rs`: add `normalize_set_op_side` (Select-leaf / `SetOperation`-segment /
      `Query`-envelope-reject-then-recurse / other-reject); replace the two side-match blocks and the
      four resolve lines in `emit_set_op_pieces` with two `normalize_set_op_side` calls; grow
      `emit_set_op_pieces`' signature with `&mut ViewChain` (now 8 params → add
      `#[allow(clippy::too_many_arguments)]` above it, the crate convention for the existing 8-arg
      fns) and thread `chain` from the `dispatch.rs:104` call site; add the `ViewChain` and
      `reject_unhonored_query_clauses, HonoredQueryClauses` imports.
- [ ] `dispatch.rs`: add a `SetExpr::Query(q)` arm to `ViewShape::classify` that peels the envelope
      and re-classifies `q.body` (top-level parenthesized body). Add "nested set operation" to the
      `dml/select.rs:361-366` rejection string and the `dispatch.rs:184-186` doc comment.
- [ ] Rust integration tests (`planner_set_ops.rs` additions incl. structure, collapse-to-leaf,
      top-level-parenthesized, and cascade-drop).
- [ ] E2E differential additions; full suite; `make verify`.
