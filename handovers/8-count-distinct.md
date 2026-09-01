# 8. `COUNT(DISTINCT x)`

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
CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, ts BIGINT NOT NULL, user_id BIGINT NOT NULL,
                 amt BIGINT NOT NULL, s TEXT NOT NULL);
```

Not every column appears in every quoted statement. `sn2` is the only nullable TEXT column.


## Current state

```
CREATE VIEW g4 AS SELECT k, COUNT(DISTINCT a) AS c FROM t GROUP BY k
  -> unsupported: DISTINCT: not supported on aggregates
```

Gate: `reject_unsupported_fn_qualifiers` (`gnitz-sql/src/ast_util.rs`), which rejects
`DISTINCT` along with `FILTER (WHERE …)`, `OVER`, `WITHIN GROUP`, `IGNORE/RESPECT NULLS`,
parametric calls, and in-argument `ORDER BY`/`LIMIT`/`SEPARATOR`. Aggregate names come from
`AGG_NAMES` (`gnitz-sql/src/ast_util.rs`): count, sum, min, max, avg.

## Verified: the two-view form compiles

```
CREATE VIEW t2_d AS SELECT DISTINCT user_id, ts / 3600000000 AS hour FROM ev   OK
CREATE VIEW t2_c AS SELECT hour, COUNT(*) AS uniq_users FROM t2_d GROUP BY hour OK
```

So the shape is expressible: a DISTINCT over `(group cols, distinct col)` feeding a
`COUNT(*)` grouped by the group cols. What is missing is the planner synthesizing that
inner segment.

## Why this is more than a pre/post-reduce map

There is separate, cheaper planner-side work to add a map on either side of one reduce — that
is what would unlock `SUM(a*b)`, `SUM(CASE …)`, `GROUP BY <expr>` and `SELECT k, SUM(a)/SUM(b)`.
This item needs more than that: a whole extra relational node
(a `Distinct`) between the source and the reduce, keyed differently from the reduce's own
group key. The machinery exists — `gnitz-sql` already builds hidden segments for CTEs, for
the self-join collision wrapper (`wrap_passthrough_segment`), and for combine
inputs cut by `lower_reduce` — but this is a new use of it.

`FILTER (WHERE …)` sits behind the same gate and is worth considering in the same plan:
`COUNT(*) FILTER (WHERE p)` is `SUM(CASE WHEN p THEN 1 ELSE 0 END)`, which the pre-reduce map
described above would make expressible. That may make `FILTER` nearly free once that lands,
and it is a different mechanism from `COUNT(DISTINCT)` — do not assume one plan covers both.

## Explicitly NOT verified

- **Multiple `COUNT(DISTINCT)` on different columns in one query.** Each needs its own
  distinct segment with its own key; whether that composes was not tested or reasoned about.
- **Mixing `COUNT(DISTINCT x)` with plain aggregates** in one SELECT — e.g.
  `SELECT k, SUM(a), COUNT(DISTINCT b) FROM t GROUP BY k`. The plain aggregates reduce over
  the raw rows while the distinct one reduces over de-duplicated rows, so they cannot share
  one reduce. Not investigated; this is likely the hard part.
- Whether `COUNT(DISTINCT)` over a nullable column matches SQL semantics (SQL excludes
  NULLs) under the DISTINCT-then-COUNT(*) rewrite.
- No result-correctness verification of `t2_d`/`t2_c`.
