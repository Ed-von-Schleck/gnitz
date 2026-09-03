# 3. The keyless cross join

**Status:** research handover, not a plan. Self-contained — no other file is needed.

This is what is left of the DDL-desugar survey once the rest of it shipped.
`CROSS JOIN`, `USING`, `NATURAL`, comma-joins, `MINUS`, view output column
aliases and the `OR REPLACE` / `IF [NOT] EXISTS` clause family were one note; all
of them are implemented except the one item below, which is not a front-end
desugar at all — it needs a circuit shape that does not exist.

## Provenance of the evidence below

- **Tree:** `main` at `c5be8c94` plus the desugar change set (comma-join binding,
  `USING` / `NATURAL` column merging, the INNER `WHERE` → key promotion, `MINUS`,
  view output aliases, `CREATE OR REPLACE VIEW` and `IF [NOT] EXISTS` /
  `IF EXISTS`). Code is cited by **file and symbol only, never by line number**.
- **How the results were obtained:** a throwaway pytest file in a `git worktree`
  (since removed), against a debug server and a Python extension built from that
  same tree. **Workers: 4.** The one row-level result is a **weight-multiset**
  comparison via `_oracle.scan_multiset`, not a row-set one.
- **"OK" means the planner and the circuit compiler accepted the statement**, and
  for the one measured view that its rows and weights matched an oracle. Every
  cost claim below is analytic reasoning from the code and is labelled as such —
  nothing here was profiled.

## Fixtures the quoted statements ran against

```sql
CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL);
CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL);
```

## The gap is narrower than "no equality"

```
CREATE VIEW x1 AS SELECT t.id, u.id FROM t CROSS JOIN u
  -> bind error: a join needs at least one equijoin or range predicate between its
     two sides. Write it in the step's ON / USING clause, or — for an INNER step,
     which a CROSS JOIN and a comma-separated FROM both are — in the WHERE.
CREATE VIEW x2 AS SELECT t.id, u.id FROM t, u
  -> the same bind error, from the same rule
CREATE VIEW x3 AS SELECT t.id, u.id FROM t JOIN u ON 1 = 1
  -> the same bind error (a constant is not a cross-table comparison)
CREATE VIEW x4 AS SELECT t.id, u.id FROM t, u WHERE t.v < u.w              OK
CREATE VIEW x5 AS SELECT t.id, u.id FROM t CROSS JOIN u WHERE t.k = u.k    OK
```

`x5` is the first to notice: `CROSS JOIN` is a keyless INNER step, which is
exactly what the comma is, so a WHERE keys either one. The spelling is not what
is missing.

`x4` is the second. A comma-join whose WHERE carries only a **range**
comparison already works: `reject_join_key_arity` refuses `n_eq == 0 && !has_range`,
so a range alone satisfies it and the body takes the pure-range broadcast path.
Verified by weight-multiset at W=4, not merely accepted
(`test_range_only_comma_join`). What is missing is therefore not "a join without
an equality" but **a join with no cross-table predicate at all**.

## Where the refusal lives

`reject_join_key_arity` in `gnitz-sql/src/hir/guards.rs`, and nowhere else.
`join_keys_and_type` maps `JoinOperator::CrossJoin` to `JoinKeys::None` — the
same keyless step a comma and a bare `JOIN b` produce — so every spelling reaches
that one rule and none of them is refused earlier by name.

`test_join_cross_rejects` in `crates/gnitz-py/tests/test_joins.py` pins the bare
refusal, so implementing this is a deliberate test change, not a gap.

## Two candidate lowerings, and why the obvious one is the wrong one

**Constant key.** A cross join is expressible today by giving both sides a
constant column and equi-joining on it:

```sql
CREATE VIEW ca AS SELECT id AS a, 1 AS one FROM t;
CREATE VIEW cb AS SELECT id AS b, 1 AS one FROM u;
CREATE VIEW cx AS SELECT p.a, q.b FROM ca p JOIN cb q ON p.one = q.one;
```

Measured: over 12 × 12 rows at W=4 this produced the exact 144-entry product with
the correct weight on every pair. So the shape **computes the right answer** — it
is not a workaround with a caveat.

It is still the wrong lowering. Routing hashes the encoded key bytes, and a
constant column is one key, so every row of both sides lands on one partition:
**O(|A| · |B|) on a single worker**, with the other W−1 idle (analytic — no
partition distribution was measured).

**Broadcast one side, worker-filter the other.** This is what the existing
`n_eq == 0` path already does. In `emit_range` (`gnitz-sql/src/hir/lower/join.rs`),
`n_eq == 0` wraps each side's reindex in `cb.worker_filter(...)` before the trace,
so one side is replicated and the other stays partitioned: **O(|A|/W · |B|) per
worker**. That is the distribution shape a cross join wants.

Only the *distribution* transfers, not the pipeline. `build_pure_range_threshold`
in the same file derives a one-row `MIN`/`MAX` threshold from the other side and
joins against that — which is why pure range supports LEFT only, and why its range
column must be a ≤ 8-byte integer. A cross join has no threshold to derive.

## The missing piece

There is **no keyless join opcode**. `CircuitBuilder::join_with_trace_range_node`
takes an `n_eq` and a `RangeRel`; `n_eq == 0` is reachable only with a range
comparison to carry. A cross join needs either a relation constant-true at that
call, or a new opcode beside it in `gnitz-store`'s join operator.

## Open questions, not resolved

- **What the output PK is.** An equi join's output PK is the synthetic `_join_pk`
  over the key columns, and with no key there are none. The pair PK the range join
  synthesizes (`a.pk` ++ `b.pk`) is the obvious candidate, and
  `reject_pair_pk_overflow` in `hir/guards.rs` already caps its arity — but
  nothing was checked about whether the equi emit path can carry that PK shape.
- **The delta cost.** Each epoch emits `|dA| × |B|`. Whether that is acceptable,
  and whether it should be gated on a size the planner cannot know, is untouched.
- **Whether the outer orientations are wanted.** Pure range supports LEFT only and
  rejects RIGHT/FULL at plan time; nothing says what a `LEFT CROSS JOIN` would
  even mean.
- **Whether it is worth building.** Every case with a predicate is already served,
  including the range-only one; the remaining case is a true cartesian product,
  which in an incremental engine is the one shape whose cost the user most likely
  did not intend.

## Explicitly NOT verified

- No cost measurement of either lowering — the O() claims are read off the code.
- The single-partition claim for the constant key is analytic; no partition
  distribution or per-worker timing was collected.
- Nothing about what a keyless join does to the exchange topology assertions
  (`debug_assert_exchange_topology`).
- No probe of a keyless join against the capacity-bounded or delta-fed view paths.
