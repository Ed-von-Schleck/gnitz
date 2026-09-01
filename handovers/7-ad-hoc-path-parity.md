# 7. Ad-hoc SELECT parity, inside its intentional scope

**Status:** research handover, not a plan. Self-contained — no other file is needed.

## Provenance of the evidence below

- **Tree:** `main` at `fb013479`, with an uncommitted in-flight refactor across
  `crates/gnitz-engine/src/query/compiler/*` and `crates/gnitz-engine/src/catalog/write_path.rs`.
  Code is cited by **file and symbol only, never by line number** — symbols were verified to
  exist at the time of writing; line numbers would already be stale (they moved under that
  refactor during this session).
- **How the SQL results were obtained:** temporary integration tests in throwaway
  `git worktree`s (since removed; the working tree was never modified), calling
  `SqlPlanner::execute` on one statement at a time and printing accept/reject plus the error
  text. Server: `GNITZ_SERVER_BIN=<repo-root>/gnitz-server`, the prebuilt binary at the repo
  root dated 2026-08-31 13:35 — **not verified to have been built from `fb013479`**. The Rust
  test harness has no staleness check (that check lives in the Python E2E suite).
- **Workers: 1.** `ServerHandle::start()` is `start_n(1)`. Nothing here was exercised at W>1,
  so no claim below covers exchange, fanout or partitioning behaviour.
- **"OK" below means the planner and the engine's circuit compiler accepted the statement. It
  does NOT mean the resulting view produces correct rows.** No probe inserted a row, read a
  result, or compared weights against an oracle. Before building on any statement shown as
  compiling, validate its semantics — in a Z-set engine that means checking weights, not row
  presence. Any cost or complexity claim not backed by a quoted measurement is analytic
  reasoning from the code, and is labelled as such where it appears.

## Fixtures the quoted statements ran against

```sql
CREATE TABLE t  (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL,
                 b BIGINT NOT NULL, f DOUBLE NOT NULL, s TEXT NOT NULL, sn2 TEXT);
```

Not every column appears in every quoted statement. `sn2` is the only nullable TEXT column.


The ad-hoc path is deliberately limited to reading one relation. Everything below is a gap
*within* that scope — none of it asks the ad-hoc path to derive a relation. The derivation
rejections (JOIN, set ops, subqueries, derived tables) are the design and are out of scope
here; `reject_derivation` (`gnitz-sql/src/dml/select.rs`) is the one template.

## 7a. Computed projection rejected under DISTINCT and GROUP BY

```
SELECT DISTINCT a + b AS ab FROM t
  -> unsupported: SELECT DISTINCT: computed expressions are not supported
```

but the same shape as a view is fine:

```
CREATE VIEW d2 AS SELECT DISTINCT a + b AS ab FROM t    OK
```

This is a straight inconsistency between the two paths. The plain rows sink already supports
computed projections (`SELECT a + b AS ab FROM t` → OK; `SELECT UPPER(s) AS us FROM t` → OK)
via `ProjItem::Computed` (`gnitz-sql/src/codec/project_schema.rs`). Only the fold sink
refuses: `resolve_set_projection` (`gnitz-sql/src/dml/group_by.rs`) requires a
`BoundExpr::ColRef` and rejects anything else.

The grouped case (`SELECT k, SUM(a)+1 …`) is a different gap and out of scope here: the
grouped SELECT list admits only bare group columns and aggregate calls on *both* paths, and
lifting that is separate planner-side work (a post-reduce map).

## 7b. `ORDER BY <expression>`

```
SELECT id FROM t ORDER BY a + b   -> unsupported: ORDER BY supports only column references and 1-based positions
SELECT a + b AS ab FROM t ORDER BY ab   OK
SELECT id FROM t ORDER BY 1              OK
```

Since aliasing already works, this is plausibly a planner-side rewrite (project the
expression as a hidden column, order by it) rather than new sink capability — the read path
already appends hidden columns for non-projected sort keys, described at
`resolve_read_spec_order` (`gnitz-sql/src/exec/order.rs`): "reusing or appending a hidden
payload column for a non-projected source key … appends are hidden and never shift a visible
position". That mechanism looks like the one to reuse. Not confirmed.

## 7c. Pass-through CTE requires a full positional identity projection

```
WITH x AS (SELECT id, a FROM t) SELECT id FROM x
  -> unsupported: ad-hoc SELECT reads a single relation; this query derives a new one
     (non-pass-through CTE). ...
WITH x AS (SELECT id FROM t WHERE a > 1) SELECT id FROM x   -> same
```

The first is only rejected because `t` has more than two columns. Rule, in `cte_passthrough` (`gnitz-sql/src/bind/resolve.rs`): identity means a bare `*`, or exactly one identifier per
source column, in order, name-matched. A *narrowing* projection is not identity.

The `WHERE`'d CTE (second case) genuinely derives and the CREATE VIEW advice is correct
there; only the narrowing case looks like an unnecessary refusal.

## 7d. FROM-less SELECT

```
SELECT 1      -> unsupported: direct SELECT without FROM is not supported
SELECT NOW()  -> same
```

Code: the `FromShape::Empty` arm of `route_select` (`gnitz-sql/src/dml/select.rs`). This breaks drivers, health checks and tooling that
probe a connection.

**This is the largest of the four despite looking smallest.** Both existing sinks read a
relation: the rows sink builds a `ReadSpec` against a target schema, the fold sink folds
rows. A constant row belongs to neither. Whether it should be evaluated entirely client-side
(the client already links `gnitz-expr` and evaluates batches — see
`gnitz-sql/src/exec/`), or given a degenerate sink, was not investigated.

## Explicitly NOT verified

- Whether `resolve_read_spec_order`'s hidden-append mechanism can carry a computed
  expression rather than a source column. This is the crux of 7b and was not checked.
- What `SELECT 1` should return for a schema (column name, type) — no convention was
  established.
- Whether any of these have tests pinning the current rejection text.
  `gnitz-sql/tests/adhoc_surface.rs` exists and its `assert_feature_rejection` helper
  suggests some are pinned; not enumerated.
