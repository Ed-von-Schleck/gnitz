# 10. `DECIMAL(p, s)` fixed-point

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

Thin note — the least investigated item in the SQL-surface survey that produced it. Recorded
so the gap is not lost, not because research was done.

## Current state

```
CREATE TABLE ty6 (id BIGINT NOT NULL PRIMARY KEY, v DECIMAL(10,2))
  -> unsupported: unsupported SQL type: Decimal(PrecisionAndScale(10, 2))
CREATE TABLE ty7 (id BIGINT NOT NULL PRIMARY KEY, v NUMERIC(18,4))
  -> unsupported: unsupported SQL type: Numeric(PrecisionAndScale(18, 4))
```

Only **zero-scale** decimal is accepted, and only at two precisions:

```
CREATE TABLE ty17 (id BIGINT NOT NULL PRIMARY KEY, v DECIMAL(38,0))   OK
```

`sql_type_to_typecode` (`gnitz-sql/src/types.rs`) maps `DECIMAL(p,0)` / `NUMERIC(p,0)` with `p ∈ {38, 39}` to
`TypeCode::U128`, with the in-file rationale that `DECIMAL(38,0)` is the common idiom for
128-bit integers and `(39,0)` covers the full u128 range.

Casting to it is refused separately:

```
-- against CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, ...)
CREATE VIEW fn73 AS SELECT id, CAST(a AS DECIMAL(38,0)) AS r FROM t
  -> unsupported: CAST to U128 is not supported
```

Gate: `is_cast_target` (`gnitz-sql/src/types.rs`) admits `has_scalar_register(tc) || tc ==
String`; U128 has no scalar register.

## Why this is not in the same class as adding `DATE` / `TIMESTAMP`

The cheap path available to temporal types exists because `DATE`/`TIMESTAMP` can route to an existing
`FixedInt`/`ScalarKind` and inherit the integer machinery unchanged. A scaled decimal cannot:
scale has to propagate through `+`, `−`, `×`, `÷` (the result scale of a multiply is the sum
of operand scales; division needs a rounding policy), which is new arithmetic in the
expression VM rather than a relabelling of existing arithmetic.

That contrast is the only real finding in this note.

## Practical consequence today

Exact money must be integer-encoded by hand (minor units in a `BIGINT`), with scale tracked
outside the database. `DOUBLE` is not a substitute where exactness matters: IEEE-754 addition
is non-associative, which the code notes in `gnitz-sql/src/agg.rs` and
`gnitz-sql/src/exec/agg_finish.rs`, and which CLAUDE.md states the consequence of: float
SUM/AVG is order-dependent, and a view's backfill is not worker-count stable.

## Explicitly NOT verified — essentially everything

- No design work. No decision on representation (i64 with a scale in the schema? i128?
  per-column scale vs. per-value?).
- Not investigated: whether the existing U128 path could carry a schema-level scale
  annotation, which might be much cheaper than a new numeric type.
- Rounding policy, overflow behaviour, comparison and PK/OPK encoding for a scaled type:
  not considered.
- Demand for this feature was not assessed at all; it was deprioritized on effort grounds only.
