# EXCEPT ALL / INTERSECT ALL: stop silently returning DISTINCT results

## Goal

`EXCEPT ALL` and `INTERSECT ALL` **silently compile to set (DISTINCT) semantics**,
dropping bag multiplicity instead of preserving it. This is a live, SQL-reachable
correctness bug: any user writing `a EXCEPT ALL b` gets `a EXCEPT b` results with no
error. Two phases:

1. **Reject `ALL` for INTERSECT/EXCEPT (one-line, ships immediately).** Converts silent
   wrong answers into an honest `Unsupported`.
2. **Implement true bag semantics** via one new non-linear operator (`positive_part`),
   after which `EXCEPT ALL = positive_part(A − B)` and `INTERSECT ALL = A − positive_part(A − B)`.

## 1. The bug

`execute_create_set_op_view` (`planner.rs:3090`) dispatches on `(op, set_quantifier)`
(`planner.rs:3158-3211`):

```rust
let out_node = match (op, set_quantifier) {
    (SetOperator::Union, SetQuantifier::All) => cb.union(left_node, right_node),
    (SetOperator::Union, _) => { let m = cb.union(left_node, right_node); cb.distinct(m) }
    (SetOperator::Intersect, _) => { /* distinct_l/r + semi-join — SET semantics */ }   // :3167
    (SetOperator::Except, _)    => { /* distinct_l/r + anti-join  — SET semantics */ }   // :3185
    _ => return Err(GnitzSqlError::Unsupported(format!("set operation {op:?} not supported"))), // :3208
};
```

The `(SetOperator::Intersect, _)` and `(SetOperator::Except, _)` arms match **every**
quantifier, including `SetQuantifier::All`. So `INTERSECT ALL` / `EXCEPT ALL` fall into
the DISTINCT builders, and the `_ => Unsupported` reject at `:3208` is **unreachable** for
them. Only `UNION ALL` honors `ALL` (caught by the explicit `(Union, All)` arm at `:3159`,
which also sets `right_branch_id = 1` at `:3118-3121` so identical rows keep distinct PKs).

Standard SQL bag semantics, per row `x` with left count `cL` and right count `cR`:
`EXCEPT ALL` ⇒ `max(0, cL − cR)`; `INTERSECT ALL` ⇒ `min(cL, cR)`. GnitzDB returns the
deduplicated set (`cL>0 && cR==0` for EXCEPT, `cL>0 && cR>0` for INTERSECT) instead. No
test pins the `ALL` behavior today.

## 2. Phase 1 — reject (ships immediately)

Insert explicit `ALL` reject arms **before** the `(op, _)` DISTINCT arms (match arms are
ordered top-to-bottom), at `planner.rs:3167`:

```rust
(SetOperator::Intersect, SetQuantifier::All) | (SetOperator::Except, SetQuantifier::All) =>
    return Err(GnitzSqlError::Unsupported(
        "INTERSECT ALL / EXCEPT ALL (bag semantics) is not yet supported; \
         use INTERSECT / EXCEPT (set semantics)".into())),
(SetOperator::Intersect, _) => { /* unchanged */ }
(SetOperator::Except, _)    => { /* unchanged */ }
```

`SetQuantifier::All` is the only variant to reject; the no-keyword form (`EXCEPT` ≡
`EXCEPT DISTINCT`) and `DISTINCT` keep flowing to the `(op, _)` arms. No other code
changes. This stops the silent wrong results with zero risk to the working DISTINCT paths.

**Test (Phase 1):** in `test_sql_rejections` (`test_workers.py:1816`) add `intersect_all`
and `except_all` entries asserting `CREATE VIEW … INTERSECT ALL / EXCEPT ALL …` raises and
registers no view (the suite's existing rejection-assert shape). Confirm `EXCEPT` /
`INTERSECT` (no `ALL`) still create successfully.

## 3. Phase 2 — bag semantics via `positive_part`

Bag `EXCEPT ALL` / `INTERSECT ALL` need element-wise `max(0, ·)` on the net weight, which
no current operator provides (`distinct` clamps magnitude to 1; there is no positive-part).
Add one non-linear operator and both fall out:

```
EXCEPT ALL    = positive_part( A − B )          // max(0, cL − cR)
INTERSECT ALL = A − positive_part( A − B )       // cL − max(0, cL−cR) = min(cL, cR)
```

Proof of `INTERSECT ALL`: if `cL ≤ cR`, `positive_part(cL−cR)=0`, result `cL = min`; if
`cL > cR`, `positive_part = cL−cR`, result `cL−(cL−cR)=cR = min`. ✓

### 3.1 The `positive_part` operator

A non-linear operator clamping each consolidated (PK, payload) weight's negative part to
zero while preserving positive magnitude. Incremental form (mirrors `distinct`'s
boundary-crossing emit, `ops/distinct.rs:61-64`, but keeps magnitude instead of `signum`):

```
w_new = w_old + Δw
out_w = max(0, w_new) − max(0, w_old)
```

- `w_old` is the pre-tick integral (point lookup, DBSP Prop 4.7 — O(|delta|), same as
  `distinct`); `Δw` the input delta weight. Retraction-correct: a count drop (`Δw<0`)
  yields `out_w ≤ 0`. Consolidation mandatory before it (non-linear).
- New surface, mirroring `distinct` end-to-end: a wire opcode (`OPCODE_POSITIVE_PART`,
  alongside `OPCODE_DISTINCT` at `gnitz-wire/src/circuit.rs`), a `CircuitBuilder::positive_part`
  (mirror `distinct` at `gnitz-core/src/circuit.rs`), a compiler dispatch arm (mirror the
  `Distinct` arm), and an engine op `ops/positive_part.rs` (a one-line variant of
  `op_distinct`: replace `signum(w) → max(0, w)` in the boundary emit).

### 3.2 Planner construction

Both sides content-hashed with `branch_id = 0` (identical payloads share a hash PK, so
consolidation yields the per-row counts `cL`, `cR`) — exactly the existing
`compile_set_op_side(.., 0)` path (`planner.rs:3123-3124`), **not** the UNION-ALL
branch-tagging. Then:

```rust
// EXCEPT ALL
let diff = cb.union(left_node, cb.negate(right_node));   // x ↦ cL − cR
cb.positive_part(diff)                                    // x ↦ max(0, cL − cR)

// INTERSECT ALL
let diff = cb.union(left_node, cb.negate(right_node));
let pos  = cb.positive_part(diff);
cb.union(left_node, cb.negate(pos))                       // x ↦ cL − max(0, cL−cR) = min
```

`positive_part`'s consolidation sees the true net `cL − cR` per (hash-PK, payload). The
same-relation guard (`planner.rs:3134`) and the schema/type-compat checks (`:3141-3156`)
apply unchanged. The `ALL` reject arms from Phase 1 are replaced by these constructions.

### 3.3 Tests (Phase 2)

- **Bag arithmetic.** Left `{x:3, y:1}`, right `{x:1, z:2}`: `EXCEPT ALL ⇒ {x:2, y:1}`;
  `INTERSECT ALL ⇒ {x:1}`. (Multiplicity via a `UNION ALL` source feeding each side, or a
  raw weighted push in a Rust unit test.)
- **Retraction.** Delete one `x` from the right after the above; `EXCEPT ALL` x-count rises
  `2→3`, `INTERSECT ALL` x-count falls `1→0`. Pins `positive_part`'s boundary emit under
  negative deltas.
- **Set/bag divergence.** A case where `EXCEPT` and `EXCEPT ALL` differ (`cL=2, cR=1`:
  set→absent, bag→1), asserting they are now distinct code paths.
- **`positive_part` unit test** (`ops/` Rust): integral `5`, deltas `+3, −10, +4` ⇒ emits
  `max(0,8)−max(0,5)=+3`, then `max(0,−2)−max(0,8)=−8`, then `max(0,2)−max(0,−2)=+2`.

## 4. Scope

- Phase 1 is self-contained and should land first (correctness; converts silent wrong
  answers to an explicit error). Phase 2 is a feature add gated on the one new operator.
- `UNION ALL` is unaffected (already correct, `:3159`). `UNION`/`INTERSECT`/`EXCEPT`
  DISTINCT are unaffected.
- `positive_part` is a general bag operator; once it exists, other bag/multiset operations
  (e.g. a future `ALL`-quantified construct) reuse it. It is the multiplicity-preserving
  counterpart to `distinct` — see foundations §3 on weight-exactness (`distinct` clamps
  multiplicity; bag ops must not).
- This bug is the inverse of the weight-exactness principle: multiplicity **clamped where
  the `ALL` quantifier demands it be preserved**. Distinct from the band-LEFT bag over-fill
  (`plans/band-left-bag-multiplicity-overfill.md`), which clamps in a *subtraction*; here
  the clamp is the wrong *quantifier dispatch*.
