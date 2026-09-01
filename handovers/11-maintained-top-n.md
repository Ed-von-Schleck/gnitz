# 11. Maintained top-N in a view body

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

Two separate fixtures, because the rejections and the `EXPLAIN` evidence came from different
probe runs.

```sql
-- run A, for the CREATE VIEW rejections below
CREATE TABLE t  (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL,
                 b BIGINT NOT NULL, f DOUBLE NOT NULL, s TEXT NOT NULL, sn2 TEXT);
CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, ts BIGINT NOT NULL, user_id BIGINT NOT NULL,
                 amt BIGINT NOT NULL, s TEXT NOT NULL);
CREATE VIEW t1_h AS SELECT id, ts / 3600000000 AS hour, user_id, amt FROM ev;
CREATE VIEW t1_r AS SELECT hour, SUM(amt) AS total, COUNT(*) AS n FROM t1_h GROUP BY hour;
```

Run B, the `EXPLAIN` section further down, defines its own `ev` and `g` inline — same table
name, different columns (`uid` rather than `user_id`, no `s`). The two runs are unrelated.

Assessed as the lowest value-per-effort item in a survey of the SQL surface: the read path
already serves this shape, so it is a performance item rather than a capability gap. Recorded
with the evidence behind that judgement so a fresh session can re-weigh it rather than
re-derive it.

## Current state

```
CREATE VIEW t4 AS SELECT hour, total FROM t1_r ORDER BY total DESC LIMIT 10
  -> unsupported: CREATE VIEW: LIMIT/OFFSET is not supported
CREATE VIEW e1 AS SELECT id, a FROM t ORDER BY a          -> unsupported: CREATE VIEW: ORDER BY is not supported
CREATE VIEW e3 AS SELECT id, a FROM t OFFSET 5            -> unsupported: CREATE VIEW: LIMIT/OFFSET is not supported
```

Gate: `plan_create_view` (`gnitz-sql/src/hir/create.rs`) calls `reject_unhonored_query_clauses`
with only `with: true` honored. The in-file comment states the reason: every other tail clause "has no
incremental-view semantics and would otherwise be silently dropped".

## Why it was deprioritized: the read path already does it

Verified `EXPLAIN` over a maintained grouped view:

```
CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, ts BIGINT NOT NULL, uid BIGINT NOT NULL, amt BIGINT NOT NULL)
CREATE VIEW g AS SELECT uid, SUM(amt) AS total FROM ev GROUP BY uid

EXPLAIN SELECT uid, total FROM g ORDER BY total DESC LIMIT 10
      read view g (drains pending ticks when stale)
      access: full scan
      predicate: none
      projection: 2 columns
      order/limit: server top-10, client sort, client window
```

So a leaderboard read against a maintained view works today, ships only 10 rows per worker,
and the top-k is applied server-side. What it costs is `access: full scan` of the view on
every read. Also verified: `... LIMIT 10 OFFSET 5` → OK.

For contrast, a predicate read of the same view:

```
EXPLAIN SELECT uid, total FROM g WHERE total > 5
      access: full scan
      predicate: server-side
```

and a PK point lookup on a base table:

```
EXPLAIN SELECT * FROM ev WHERE id = 3
      access: pk point lookup
```

## What a maintained top-N would need (analytic, not investigated)

Ordered per-partition state with bounded output. This is the only gap in the survey behind
this note that plausibly needs a new DBSP operator. Window functions, the other obvious
candidate, appear not to: their desugar *shapes* — a partition aggregate as
`source JOIN (grouped view over the same source) ON key`, and running total / rank as a band
self-join plus `GROUP BY` — were all verified to compile on the existing operator set. (Those
shapes are not finished rewrites: an inner band self-join drops the top-ranked row, and tie
and direction semantics were never worked out. They establish that no new operator is needed,
not that the rewrite is done.) If someone does want to build a new operator, the mechanics
observed while reading:

- Operator state is a child table created via `EmitCtx::create_child_table` under
  `gnitz_store::storage::ChildAddr::Scratch { child, rank }`, tracked by a scratch guard so a
  failed compile removes it (`gnitz-engine/src/query/compiler/emit.rs` — cited by symbol, not
  line: that file is under active refactor and line numbers there move).
- Such state is checkpointed and resumed **generically**: `gnitz-engine/src/catalog/view_state.rs`
  documents a view as resumable iff its recorded topology matches the launched
  `(worker_count, STATE_FORMAT)`, **every child that carries its state — output-store children
  and owned operator scratch — is stamped with the committed checkpoint generation**, every
  view it scans is itself valid, and none of its sources is a STREAM.
- The VM instruction set is small and exhaustively matched: `Instr` in
  `gnitz-engine/src/query/vm/mod.rs` has 10 variants, and `reads()` plus
  `writes_state_during_replay()` are both written out with no `..`, so a new variant is a
  compile error at each until classified.
- Constraint to respect: "a different worker count re-shapes every keyed store's row
  placement", so any new state must be partitionable by the existing key rule or the
  worker-count relayout breaks.

## Explicitly NOT verified

- **No measurement.** How large a view the full-scan read stays acceptable for was not
  measured, and that number is the entire argument for or against this item.
- Interaction with capacity-bounded views and skeleton rows was **not** investigated. A
  bounded view's read hydrates missing keys from the source; how an ordered operator would
  interact with that is unknown. Note that `capacity` already refuses everything but
  filter/projection over one relation and an inner equi-join:
  `CREATE VIEW v3 WITH (capacity='4 MB') AS SELECT k, SUM(a) … GROUP BY k` →
  `unsupported: CREATE VIEW WITH (capacity …): GROUP BY / an aggregate is not supported`.
- Whether a bounded ordered operator is even expressible in DBSP without unbounded
  retraction on delete (removing the current #1 promotes an arbitrary row from outside the
  retained set, which the operator no longer holds) — this is the obvious hard question and
  it was not investigated.
