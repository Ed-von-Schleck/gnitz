# 4. Error-message quality

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


Small, self-contained, and worth attaching to whichever larger SQL-surface change lands
first rather than planning alone.

## 4a. An unknown aggregate reports a grouping error

```
CREATE VIEW g13 AS SELECT k, STDDEV(a)      AS sd FROM t GROUP BY k
  -> plan error: GROUP BY SELECT: only column refs and aggregates supported
CREATE VIEW g14 AS SELECT k, STRING_AGG(s, ',') AS sg FROM t GROUP BY k   -> same
CREATE VIEW g15 AS SELECT k, ARRAY_AGG(a)   AS ag FROM t GROUP BY k       -> same
```

A user who typed `STDDEV` is told about column references. Cause (traced, not guessed):
`is_agg_call` consults `AGG_NAMES` (`gnitz-sql/src/ast_util.rs` — count/sum/min/max/avg
only), so `STDDEV` is not classified as an aggregate; `collect_aggs` therefore never sees
it, and `bind_finalize_projection` falls through to `reject_computed_grouped_item`
(`gnitz-sql/src/ast_util.rs`). The desired message already exists in that same file:
`unknown_function` renders `function '<name>' not supported`.

## 4b. One defect, two messages

```
view:    SELECT k, SUM(a * b) FROM t GROUP BY k  -> unsupported: expected a column reference
ad-hoc:  SELECT k, SUM(a * b) FROM t GROUP BY k  -> unsupported: aggregate on computed expression not supported
```

The ad-hoc wording (in `agg_arg_col`, `gnitz-sql/src/agg.rs`) names the actual problem; the view wording
does not. There is an established pattern of giving a message one home that both paths call:
`reject_min_max_unorderable` (`gnitz-sql/src/agg.rs`) and `reject_ungrouped_column`
(`gnitz-sql/src/ast_util.rs`) are each written once and used by both. Which of the two
files a shared message belongs in is not obvious from those two examples.

Note: there is separate, higher-priority work to let a grouped query compute expressions on
the way into and out of the reduce (a pre/post-reduce map; planner-side only, no engine
change). If that lands, both of these messages become unreachable for this input. Sequence
accordingly — do not spend effort on a message that is about to be deleted.

## 4c. Raw parser AST leaks into user-facing errors

```
POSITION('a' IN s)
  -> unsupported: expression type not supported: Position { expr: Value(ValueWithSpan {
     value: SingleQuotedString("a"), span: Span(Location(1,41)..Location(1,44)) }),
     in: Identifier(Ident { value: "s", quote_style: None, span: ... }) }

a IS DISTINCT FROM b
  -> unsupported: expression type not supported: IsDistinctFrom(Identifier(Ident { value: "a",
     quote_style: None, span: ... }), Identifier(Ident { value: "b", ... }))

EXTRACT(YEAR FROM a)
  -> unsupported: expression type not supported: Extract { field: Year, syntax: From, expr: ... }
```

Source: the catch-all arm of `bind_structural` in `gnitz-sql/src/bind/structural.rs`, which
formats the `sqlparser::ast::Expr` with `{:?}`.

## Explicitly NOT verified

- Whether any test asserts these exact strings. `gnitz-sql/tests/common/mod.rs` has
  `assert_rejects_variant(client, sn, sql, want_variant, want_msg)` which pins variant and a
  message substring, and the doc comment on `reject_derivation`
  (`gnitz-sql/src/dml/select.rs`) says of the ad-hoc derivation template "One template, one code path, asserted verbatim by tests" — so at
  least that message is pinned and changing it will require test edits. Which other messages
  are pinned was not enumerated.
- No survey was done of error messages outside the cases above.
