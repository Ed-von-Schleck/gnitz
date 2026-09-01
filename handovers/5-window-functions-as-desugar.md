# 5. Window functions as a planner desugar

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
-- the rejections in "Current state"
CREATE TABLE t  (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL,
                 b BIGINT NOT NULL, f DOUBLE NOT NULL, s TEXT NOT NULL, sn2 TEXT);
-- the desugar probes
CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL);
```


The headline finding: **the engine appears to need no new operator for this.** Every
desugar target was accepted by the planner and circuit compiler. Read the caveats before
building on that.

## Current state

Each was probed as `CREATE VIEW wN AS <the statement below>`; the `QUALIFY` line elides its
leading `SELECT id, a FROM t`.

```
SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) AS rn FROM t
  -> unsupported: window functions (OVER): not supported on aggregates
SELECT id, SUM(a) OVER (PARTITION BY k) AS sa FROM t          -> same
SELECT id, RANK() OVER (ORDER BY a) AS r FROM t               -> same
SELECT id, LAG(a) OVER (ORDER BY id) AS p FROM t              -> same
... QUALIFY ROW_NUMBER() OVER (ORDER BY a) = 1
  -> unsupported: CREATE VIEW: QUALIFY is not supported
```

Gates: `reject_unsupported_fn_qualifiers` (`gnitz-sql/src/ast_util.rs`) rejects `over` for any
function call; `QUALIFY` and the `WINDOW` clause are rejected in
`reject_unhonored_select_clauses` (`gnitz-sql/src/validate.rs`).

## Verified: the desugar targets compile today

All of the following were accepted:

```
-- partition aggregate, via a named grouped view
CREATE VIEW g AS SELECT k, SUM(a) AS total FROM ev GROUP BY k                      OK
CREATE VIEW w AS SELECT ev.id, ev.a, g.total FROM ev JOIN g ON ev.k = g.k          OK

-- partition aggregate, grouped side inline as a derived table (what a rewrite would emit)
CREATE VIEW w2 AS SELECT e.id, e.a, s.total
  FROM ev e JOIN (SELECT k, SUM(a) AS total FROM ev GROUP BY k) s ON e.k = s.k     OK

-- running total: band self-join + GROUP BY
CREATE VIEW rt AS SELECT a1.id, SUM(a2.a) AS running
  FROM ev a1 JOIN ev a2 ON a1.k = a2.k AND a2.id <= a1.id GROUP BY a1.id           OK

-- RANK: band self-join + COUNT
CREATE VIEW rk AS SELECT a1.id, COUNT(*) AS rank
  FROM ev a1 JOIN ev a2 ON a1.k = a2.k AND a2.a > a1.a GROUP BY a1.id              OK

-- global rank, no partition: pure-range self-join
CREATE VIEW rk2 AS SELECT a1.id, COUNT(*) AS rnk
  FROM ev a1 JOIN ev a2 ON a2.a > a1.a GROUP BY a1.id                              OK

-- percent-of-partition: outer view over the join result
CREATE VIEW w3 AS SELECT id, a * 100 / total AS pct FROM w                         OK
```

### These are shape probes, not finished desugars

They establish that the *operator shapes* compile. They are **not** correct rewrites of the
window functions named in the comments, and two gaps are already visible by inspection:

- **`rk` and `rk2` silently drop the top-ranked row.** With an inner join on `a2.a > a1.a`,
  the row holding the partition maximum matches nothing, contributes no join output, and so
  has no group under `GROUP BY a1.id` — it vanishes from the result instead of ranking 1. A
  real rewrite needs a LEFT JOIN (or a union with the boundary case) and a `+1`. `rt` does
  not have this problem, because `a2.id <= a1.id` always matches the row itself.
- **Direction, ties and off-by-one were never worked out.** `COUNT(*)` of strictly-greater
  values is closer to `RANK() OVER (... ORDER BY a DESC) - 1` than to `RANK()`. Ties, NULL
  ordering, and `ROWS`-vs-`RANGE` frame semantics on a non-unique ORDER BY column are all
  untouched here — the fixture's `id` is a PK, which hides the tie question entirely.

## Why the same-source-twice shapes work

- `resolve_collisions` (`gnitz-sql/src/hir/lower/mod.rs`) — "a circuit's delta inputs must
  carry distinct source ids, so a repeated `tid` wraps the later side in an identity
  pass-through segment". `wrap_passthrough_segment`, in the same file, builds that wrapper as a HIR
  identity `Project` lowered through `lower_linear`.
- The join lowering (`gnitz-sql/src/hir/lower/join.rs`) calls it for the two join inputs.
- **Wording caution about CLAUDE.md.** It says the same-source-feeding-both-inputs shape "is
  rejected by the planner (self-join and same-relation INTERSECT/EXCEPT guards; the
  discriminator is source-id equality)". The *invariant* it states holds — after
  `resolve_collisions`, the two delta inputs do carry distinct source ids — but the mechanism
  is **resolution by wrapping, not rejection of the query**: a self-join compiles — the
  `rt`, `rk` and `rk2` probes above are all `ev` joined to itself. Read "rejected" as
  "prevented from arising".
  The same-relation INTERSECT/EXCEPT case is named by the same `wrap_passthrough_segment`
  doc comment but was **not** probed here, so nothing is claimed about it.
- Group keys of any type work: `ReduceOutKey::SyntheticFold` (`gnitz-wire/src/circuit.rs`)
  is the leading synthetic U128 `_group_pk` fold with the group columns as payload, and for a
  string column that fold hashes the *content* — `hash_group_col` →
  `hash_german_string_content` in `gnitz-store/src/schema/key.rs`, a 4-byte LE length
  prefix then the content, following the heap pointer for long strings.
  `GROUP BY <text column>` was verified compiling.

## The cost question — analytic, NOT measured

Reasoning from the semantics, not from any benchmark:

- A window function assigns a value to **every row of a partition**. When one row is
  inserted, a partition-level value changes, so the output delta must retract and re-emit
  every row of that partition. That is O(partition), not O(delta), and it is inherent to
  the semantics under incremental maintenance — not a gnitz limitation.
- The band-join forms (`rt`, `rk`) additionally hold O(partition²) join state.
- Crucially: **the engine already exhibits exactly this cost** for the hand-written
  equivalents above, which compile today. So a desugar adds no new cost class; it makes an
  existing cost reachable through nicer syntax. That is an argument for honesty about which
  forms to accept, not an argument against the feature.

None of this was measured. A plan should measure before deciding the accept/reject line.

## Explicitly NOT verified

- **Correctness.** No probe inserted a row or read a result. That the seven statements above
  compile says nothing about whether they compute the right weights. In a Z-set engine the
  band-self-join-plus-COUNT shape is exactly where a weight bug would hide, and it must be
  validated against an oracle before any rewrite is built on it.
- Multi-worker behaviour: everything above ran at W=1. The self-join wrapper interacts with
  exchange/partitioning; that was not exercised.
- `LAG`/`LEAD`: **no desugar was tested.** The sketch "previous row = a bounded-range MAX"
  is untested speculation. Treat these as an open problem, possibly out of scope.
- Frame clauses (`ROWS BETWEEN …`, `RANGE BETWEEN …`) were not tested or considered at all.
- `DENSE_RANK`, `NTILE`, `FIRST_VALUE`, `LAST_VALUE`, `PERCENT_RANK`: not considered.
- Whether the planner can synthesize the derived-table form (`w2`) as *hidden* segments the
  user never names, and whether hidden-segment naming/limits allow it.
- Whether `QUALIFY` should be in scope or deferred.

## Where to start

Validate correctness of `w2`, `rt` and `rk` against an oracle first. If those hold, the rest
is binder work: window-spec parsing, partition/order validation, and the accept/reject line.
