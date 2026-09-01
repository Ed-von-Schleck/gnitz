# 6. Temporal types and calendar functions

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


Contains the load-bearing assumption that makes this look cheap, flagged as unverified.
**Verify that assumption before planning anything else here.**

## Current state

Each type was probed as `CREATE TABLE tyN (id BIGINT NOT NULL PRIMARY KEY, v <type>)`; only
the column is shown after the first. Each expression was probed as
`CREATE VIEW fnN AS SELECT id, <expr> AS r FROM t`. The `Extract` AST dump is elided.

```
CREATE TABLE ty0 (id BIGINT NOT NULL PRIMARY KEY, v DATE)
  -> unsupported: unsupported SQL type: Date
v TIMESTAMP  -> unsupported SQL type: Timestamp(None, None)
v TIME       -> unsupported SQL type: Time(None, None)
v DATETIME   -> unsupported SQL type: Datetime(None)
v INTERVAL   -> unsupported SQL type: Interval { fields: None, precision: None }

EXTRACT(YEAR FROM a)  -> unsupported: expression type not supported: Extract { ... }
NOW()                 -> unsupported: function 'now' not supported
CURRENT_DATE          -> unsupported: function 'current_date' not supported
CURRENT_TIMESTAMP     -> unsupported: function 'current_timestamp' not supported
```

Verified accepted by probe: `VARCHAR(n)`, `CHAR(n)`, `TEXT`, `REAL`, `DECIMAL(38,0)`,
`UUID`, `SMALLINT`, `TINYINT`, `BIGINT UNSIGNED`. `DECIMAL(39,0)` is accepted per
the `DECIMAL(p,0)` arm of `sql_type_to_typecode` but was **not** probed. Source of the
mapping: `sql_type_to_typecode` in `gnitz-sql/src/types.rs`.

## Verified workaround: bucketing on BIGINT epochs already works

```
CREATE VIEW t1_h AS SELECT id, ts / 3600000000 AS hour, user_id, amt FROM ev            OK
CREATE VIEW t1_r AS SELECT hour, SUM(amt) AS total, COUNT(*) AS n FROM t1_h GROUP BY hour OK
SELECT hour, total FROM t1_r ORDER BY total DESC LIMIT 10                                OK
```

So hour/day bucketing is reachable with no new work if timestamps are stored as integers.
Month/quarter/year bucketing is not (it needs calendar arithmetic, not division).

## The assumption that makes native types look cheap — NOT VERIFIED

`TypeCode` (`gnitz-wire/src/types.rs`) has 15 variants. Two functions are exhaustive over
it with no `_` arm, each carrying a comment that a new variant is a compile error until
someone classifies it:

- `FixedInt::from_type_code` (`gnitz-wire/src/types.rs`)
- `ScalarKind::from_type_code` (same file)

**The assumption:** if `DATE` maps to `FixedInt::I32` and `TIMESTAMP` to `FixedInt::I64`,
they inherit OPK encoding, PK eligibility, `compare_pk_bytes`, join-key promotion, index key
type, register image, the reduce/AVI extreme path and the whole expression VM for free —
because storage and the DBSP VM dispatch on width and `ScalarKind`, never on the specific
type code.

**This was NOT established by audit.** It is inferred from reading `FixedInt`/`ScalarKind`,
`ColumnLocator`, and `ops/reduce/agg.rs` (whose `Accumulator::new` resolves everything
through `ScalarKind::from_type_code`). An attempted survey of `TypeCode::` uses across
`gnitz-store` and `gnitz-engine` was interrupted and never completed. **A fresh session's
first task is to complete that survey** — enumerate every exhaustive match over `TypeCode`
and every site that branches on a specific variant, and confirm each either routes to the
integer path or needs a deliberate decision. If the assumption fails, this item's cost
estimate is wrong.

## The per-type question set (centralized, which is the encouraging part)

`gnitz-wire/src/types.rs` holds the per-type predicates in one file: `wire_name`,
`is_float`, `register_image`, `is_wide_int`, `admits_text_literal`, `is_german_string`,
`is_signed_int`, `is_pk_eligible`, `wire_stride`, `reindex_output_type`,
`join_key_common_type`, `carried_reindex_tc`, plus `index_key_type`,
`validate_pk_column_types`, `is_widening_promotion`, `resolve_reindex_type`.

## Where the real cost sits (analytic, and conditional on the assumption above)

**If** the inheritance assumption holds — and it is unverified — the cost is not in storage or
the VM, but in:
- SQL literal forms (`DATE '2024-01-01'`, `TIMESTAMP '...'`) → an integer constant.
- Client rendering: `gnitz-py` read/write paths, or a `DATE` comes back as an integer.
- Calendar opcodes: `EXTRACT`, `DATE_TRUNC`, date arithmetic. Civil-date conversion is
  self-contained pure integer math, but it is new opcodes in `gnitz-expr`.
- Promotion policy: whether `DATE` joins with `I32`, what `join_key_common_type` returns.

## The design constraint to settle FIRST

`NOW()`, `CURRENT_DATE`, `CURRENT_TIMESTAMP` and `RANDOM()` are non-deterministic. The
reasoning — analytic, not something the code states anywhere — is that a maintained view whose
predicate or projection reads wall-clock time cannot be incrementally maintained: rows would
need to enter and leave the view with no input delta to drive them, and an epoch only ever
carries an input delta. If that holds, such functions need to be rejected in a view body;
whether they should still be allowed in ad-hoc reads is a design question, not a conclusion.

What was actually checked: grepping `gnitz-sql`, `gnitz-engine` and `gnitz-expr` for
`deterministic` / `non-determin` / `volatile` turned up **no general determinism or volatility
classification of functions**. The one determinism-related rejection found is `DISTINCT ON`
(the doc comment on `reject_unhonored_select_clauses` in `gnitz-sql/src/validate.rs`:
"non-deterministic without an ORDER BY views forbid"), which is about ordering, not function
volatility. (`volatile` also appears in that file's `CREATE TABLE` destructure, but as a
vendor keyword accepted as a no-op.) Note also that today's rejection of `NOW()` is incidental — it is an unknown function
name, not a determinism check. So the guard would be new work; that grep is the whole basis
for saying so, and it is not an exhaustive audit.

## Explicitly NOT verified

- The `ScalarKind` assumption above — the single most important open item.
- `TIME`, `INTERVAL`, and timezone handling were not considered at all. `INTERVAL` in
  particular is not obviously an integer.
- Whether `DATE`/`TIMESTAMP` should be distinct type codes or a display annotation on an
  integer column. Both were considered only informally.
- No result-correctness verification anywhere.
