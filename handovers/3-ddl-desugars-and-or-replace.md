# 3. DDL desugars, and the one wrong-behaviour item

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
CREATE TABLE u  (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL);
CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, ts BIGINT NOT NULL, user_id BIGINT NOT NULL,
                 amt BIGINT NOT NULL, s TEXT NOT NULL);
```

Not every column appears in every quoted statement. `sn2` is the only nullable TEXT column.


Groups a genuine correctness bug with several pure syntax desugars, because they all land in
`gnitz-sql` statement handling.

## 3a. `OR REPLACE` / `IF NOT EXISTS` are parsed, dropped, then fail

The only item found in the survey behind this note where a clause is silently dropped and the
statement then fails with an error that does not mention it. Everything else in the surveyed
surface either works or refuses cleanly and by name. This is a wrong *outcome* (an error
where the user asked for a replace or a no-op), not a silent wrong *result*.

```
CREATE VIEW e8 AS SELECT id, a FROM t                      OK
CREATE OR REPLACE VIEW e8 AS SELECT id, b FROM t
  -> exec error: server error: Table or view already exists: s0.e8
CREATE VIEW IF NOT EXISTS e8 AS SELECT id, b FROM t
  -> exec error: server error: Table or view already exists: s0.e8
```

(`s0` is the auto-generated schema name the test harness created; it carries no meaning.)

`IF NOT EXISTS` should be a no-op; instead it errors. `OR REPLACE` should replace; instead
it errors.

Code: `reject_unhonored_create_view_clauses` in `gnitz-sql/src/validate.rs` destructures
`CreateView` exhaustively and marks `or_alter: _`, `or_replace: _`, `if_not_exists: _` as
ignored, with the comment "loud on drop; implement later". Its doc comment says the same. So the drop is deliberate and recorded — this is unfinished work, not an oversight.

**The likely reuse:** `plan_alter_view` in `gnitz-sql/src/hir/create.rs` already implements
"drop-then-create under the same name with a FRESH vid (ids are never reused), as ONE DDL
zone", with the old vid's and its hidden segments' `-1` rows in the same bundle as the new
chain's `+1`s. That is what `OR REPLACE` needs.

**Complication found, not resolved:** `ALTER VIEW` refuses when the view has dependents.

```
CREATE VIEW e11 AS SELECT id, a FROM t   OK
CREATE VIEW v1  AS SELECT id, a FROM e11 OK      (v1 now depends on e11)
ALTER VIEW e11  AS SELECT id, b FROM t
  -> exec error: server error: View dependency: entity 's0.e11'
```

So if `OR REPLACE` is built on the `ALTER VIEW` path it inherits that refusal. Whether that
is the right semantics for `OR REPLACE` is an open question for the plan.

## 3b. Join-form desugars

Each was probed as a full `CREATE VIEW jN AS SELECT ... FROM <the shape below>`; only the
FROM clause is shown.

```
FROM t CROSS JOIN u          -> unsupported: CREATE VIEW JOIN: only INNER / LEFT / RIGHT / FULL JOIN ... ON supported
FROM t JOIN u USING (k)      -> same message
FROM t NATURAL JOIN u        -> same message
```

Code: `join_on_and_type` in `gnitz-sql/src/hir/guards.rs` matches only
`JoinOperator::{Inner,Join,LeftOuter,Left,RightOuter,Right,FullOuter}(JoinConstraint::On(e))`;
everything else falls to its catch-all arm.

`USING (k)` is `ON a.k = b.k` plus output-column coalescing. `CROSS JOIN` was verified
reachable today via a constant key, which shows the equi-join machinery can express it:

```
CREATE VIEW t3_a AS SELECT id, 1 AS one, amt FROM ev                          OK
CREATE VIEW t3_b AS SELECT id, 1 AS one, w   FROM u                           OK
CREATE VIEW t3_x AS SELECT a.id, b.w FROM t3_a a JOIN t3_b b ON a.one = b.one OK
```

That is evidence the shape compiles, **not** a recommendation to implement `CROSS JOIN` that
way — a constant key sends every row to one partition, which is a cost question nobody has
looked at here.

## 3c. `MINUS`

```
SELECT k FROM t MINUS SELECT k FROM u   -> unsupported: set operation Minus not supported
```

`SetOperator` in the `sqlparser` 0.62 crate (its own `src/ast/query.rs`, not a path in
this repo) is `{ Union, Except, Intersect, Minus }`, with `Minus` documented there as
"non-standard". `gnitz-sql/src/hir/bind.rs` `bind_set_op` maps the first three to
`SetOpKind` and rejects the rest.

`MINUS` is Oracle's spelling of `EXCEPT` (distinct by default), so this *looks* like one
match arm mapping it to `SetOpKind::Except` — an inference from the surrounding code, not
something that was implemented or tested. Worth a moment's thought about what
`MINUS ALL` should mean, since Oracle has no such form.

## 3d. View output column aliases

```
CREATE VIEW e5 (x, y) AS SELECT id, a FROM t
  -> unsupported: CREATE VIEW: output column aliases is not supported
```

Code: the `output column aliases` rejection in `reject_unhonored_create_view_clauses`
(`gnitz-sql/src/validate.rs`). Note that CTE column aliases *are* supported:
`apply_positional_aliases` in `gnitz-sql/src/bind/resolve.rs` applies them. Whether it is reusable for view output aliases is unknown — not investigated.

## Explicitly NOT verified

- Whether `TEMPORARY` views (rejected by the same function) belong in this group. They
  imply a lifetime concept that may not exist; not investigated.
- Whether `IF NOT EXISTS` should compare the existing view's definition or just its name.
- Nothing about `CROSS JOIN` partitioning cost.
- No runtime/result verification anywhere; see the provenance section at the top.
