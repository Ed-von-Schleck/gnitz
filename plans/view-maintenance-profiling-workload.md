# Realistic incremental-view profiling workload

A self-contained benchmark whose purpose is to **keep gnitz's incremental
view-maintenance hot paths executing on-CPU for a long enough wall-clock window that
`perf` accumulates a statistically meaningful number of samples on the code worth
optimizing**. It is a profiling instrument, not a measurement scoreboard: absolute
throughput/latency numbers are secondary. What the instrument must guarantee is that the
DBSP operators (filter, join, SUM/COUNT and MIN/MAX reduce, distinct), the
consolidation/merge kernels, and the exchange path run hot against a large standing state,
under a continuous retraction-heavy insert/update/delete stream, for tens of seconds of
on-CPU worker time — so the flamegraph resolves each operator family rather than handing
`perf` a few dozen samples to spread across six kernels.

It requires **no engine changes**. Correctness (`verify` config) is checked by reusing the
existing differential weight-multiset oracle (`crates/gnitz-py/tests/_oracle.py`); server
lifecycle reuses the existing `_Server` (`crates/gnitz-py/tests/_serverproc.py`); perf
recording and reporting reuse `benchmarks/helpers/perf.PerfRecorder` and
`benchmarks/report.py`. The only genuinely new code is the retraction-heavy
above-threshold stream generator and the perf-window orchestration.

This builds directly on `benchmarks/combined/test_view_pipeline.py`, which already compiles
a `fact → filter → join → grouped-SUM/COUNT → distinct` chain — but drives it with 100-row
SQL `INSERT`s (far below the auto-tick threshold) under `bench_timer`, so the circuit never
runs inside a profiled window and never sees an UPDATE/DELETE. The four things this
instrument adds over that benchmark: (1) above-threshold batches so the circuit ticks
continuously, (2) an UPDATE/DELETE retraction stream via the `push`/`delete` API, (3) a
wall-clock-bounded `perf` window with master/worker attribution, and (4) MIN/MAX
(non-linear reduce) coverage.

## 1. Objective and non-goals

**Objective.** Produce a runnable instrument that (a) seeds a large base state, (b) drives a
sustained, hot-key-skewed insert/update/delete mutation stream into a base table feeding a
chained view DAG, (c) runs that stream for a fixed wall-clock budget under `perf record`,
and (d) is proven correct at small scale by the existing differential oracle. Two
configurations of the *same* code: `verify` (small scale, exact-oracle assertions at
periodic checkpoints, debug binary, no perf) and `profile` (large scale, wall-clock window,
DWARF perf, sanity + quantitative-coverage asserts, release binary).

**Non-goals (decided exclusions).** No latency CDFs, percentile reporting, or cross-system
comparison — `benchmarks/helpers/timing.py` is not used. No regression gate. No
modification of `benchmarks/micro/`, `benchmarks/combined/`, `benchmarks/conftest.py`, or
`benchmarks/run.py`. No new SQL features: every view is a shape the planner compiles today.
No durability-bypass or tick-threshold tuning: the workload drives the engine through its
normal client API. No global (ungrouped) aggregate views (the planner rejects them; §2).

## 2. Verified engine behavior this instrument relies on

Every design choice rests on behavior confirmed in the current source. References are by
file + symbol (line numbers drift); the behavior, not the line, is load-bearing.

- **The view tick is decoupled from the push ACK and fires on a per-table row threshold.**
  A push's client ACK is gated on the durable `fdatasync`, not on view maintenance: the
  committer fires the auto-tick fire-and-forget through an unbounded channel *before*
  awaiting fsync, then sends the response after fsync
  (`runtime/orchestration/committer.rs`, `fire_auto_tick` / Phase C / Phase D). The auto-tick
  fires when some base table's accumulated row count satisfies
  `tick_rows[tid] >= TICK_COALESCE_ROWS` where `TICK_COALESCE_ROWS = 10_000`
  (`committer.rs`, `executor.rs`; the operator is `>=`, so a batch of exactly 10 000 fires).
  The counter is cleared on every tick (`executor.rs::drain_tick_rows_into`), so a sustained
  stream of pushes each ≥ 10 000 rows trips the auto-tick essentially every push, running the
  circuit continuously in the background.

- **Push and delete are separate, independently-ACKed commits.** The synchronous client
  (`gnitz.connect` → `Connection.push` / `.delete`) blocks on each request's post-fsync ACK
  before issuing the next. A round's upsert push and its delete are therefore two separate
  commit groups, each evaluated against the threshold on its own. With the §6 mix the upsert
  push (≈ `BATCH_OPS` rows, deletes are ~2%) crosses ≥ 10 000 **on its own**; the small
  delete commit typically leaves a sub-threshold residual that rolls into the next push.
  The auto-tick is async and coalescing (a `TICK_DEADLINE_MS` window), so the per-round tick
  count is "≈ one," race-dependent, not exactly one — adequate for a profiling instrument,
  and the §11 periodic drain makes circuit execution observable regardless.

- **One base-table tick maintains the entire downstream DAG, in dependency order.**
  `handle_push` buffers the delta into `pending_deltas`; `handle_tick` calls `evaluate_dag`
  → `drive_dag`, which seeds the pushed source's immediate dependents and walks the **full
  transitive forward closure**, executing each view's epoch in ascending `depth` order where
  `depth = max(source.depth)+1` (`runtime/orchestration/worker/mod.rs`,
  `query/dag/mod.rs::drive_dag`, `catalog/hooks.rs`). A view at depth 3 is reached from a
  depth-0 base tick in one `drive_dag` call; a source view is fully ingested before any
  greater-depth dependent runs. A view producing output is fanned to each dependent with a
  per-edge clone (`queue_dependents`), so a shared view feeding two consumers never has its
  delta stolen by one branch.

- **A unique-PK conflict-update push auto-retracts the stored old payload.** On a
  `unique_pk` base table, pushing a payload at an existing PK makes `enforce_unique_pk` read
  the currently stored row and emit `retract(-1, old_payload) + insert(+1, new_payload)`;
  weights with `|w| > 1` are clamped to ±1 first (`query/dag/mod.rs`,
  `enforce_unique_pk` / `normalize_unique_pk_weights`). The default conflict mode is
  `Update` (set in `gnitz-core` `Connection::push` → `WireConflictMode::default()`, not in
  the Python wrapper). **Consequence:** INSERT and UPDATE are the same call — push a full row
  at a new PK (insert) or an existing PK (update/row-replace); the engine produces the
  retraction. The generator tracks only the **set of live PKs**, never payloads.
  `Connection.delete(tid, schema, pks)` ships PK-only; the engine resolves the stored payload
  for the `-1` (`gnitz-core/src/client.rs::delete`). Intra-batch duplicate PKs resolve
  last-wins under `Update`, and deleting an absent PK is a safe no-op.

- **SQL-created tables are `unique_pk`.** Every `CREATE TABLE` registers the table with
  `unique_pk = true` unconditionally (`gnitz-sql` `plan/ddl.rs`), which arms
  `enforce_unique_pk`. Creating the tables with `execute_sql` is sufficient.

- **Grouped aggregates are correct at every worker count; ungrouped aggregates do not
  compile.** A `GROUP BY` aggregate partitions rows by group key (`ExchangeShard` over the
  group columns), so each group lives wholly on one worker and its retraction is recomputed
  by that worker's local `op_reduce` + `AggValueIndex`, yielding the next-best extremum —
  covered by passing W≥2 tests (`crates/gnitz-py/tests/test_multiworker_ops.py`
  grouped-min/grouped-max retraction). A **global** (no-`GROUP BY`) aggregate view is
  **rejected at plan time** with `aggregate function not allowed in expression context`
  (`gnitz-sql` view dispatch → `Simple` shape → aggregate sink rejection), so it cannot be
  created via SQL at all. (The `op_gather_reduce` operator that would mishandle a distributed
  ungrouped non-linear retraction is unwired in SQL planning — dead code for a future
  milestone, not a reachable bug.) SUM/COUNT and MIN/MAX are all exact for grouped views.
  **Consequence:** every aggregate view here is grouped — not to dodge a runtime bug, but
  because ungrouped aggregate views are unsupported.

- **The planner compiles one shape per view; pipelines are built by chaining views.** A view
  is exactly one of filter/projection, single JOIN, GROUP BY, DISTINCT, or set-op
  (`gnitz-sql` `plan/view/dispatch.rs::ViewShape::classify`); no `ORDER BY`/`LIMIT`,
  subquery, `COUNT(DISTINCT)`, or date/time type/function in a view. A view may source
  another view (name resolution falls back to the view table; `gnitz-core` `client.rs`
  resolve), with no chain-depth limit, and a view may be a JOIN input on either side (the
  self-join guard keys on source-id equality, which the diamond never triggers). All five
  views below compile today; a structurally equivalent chain is exercised by
  `test_multiworker_ops.py::test_deep_pipeline_after_prior_schemas` (filter → view⋈base join
  → grouped SUM/COUNT → DISTINCT, asserted at W≥2) and `benchmarks/combined/test_view_pipeline.py`.

- **`scan` forces a synchronous drain; `seek` does not.** A scan sends a `Drain` of every
  pushed base table (`tick_tids`) and awaits it before reading the consolidated net state;
  the tick loop expands each base table to its full forward closure, so **any** scan flushes
  **all** pending maintenance — including sub-threshold residual deltas that never
  auto-ticked — and leaves every view current, order-independent across views
  (`runtime/orchestration/executor.rs::handle_scan`). Scanned rows expose `.weight` and named
  output columns via `_asdict()`. A seek issues no drain.

## 3. Hard invariants the implementation must respect

1. **No ungrouped aggregates.** Every aggregate view has a `GROUP BY` (ungrouped aggregate
   views do not compile).
2. **All columns integer-encoded.** Only integer column types (the planner has no date type).
3. **Base tables created via `CREATE TABLE`** so they are `unique_pk` (arms conflict-update
   retraction).
4. **Per-batch PK uniqueness.** Within one pushed batch each target PK appears at most once;
   a PK deleted in a round is excluded from that round's upsert set.
5. **Round-start liveness snapshot for retraction targets.** UPDATE and DELETE targets are
   sampled from the set of PKs that were live at the **start of the round**; a PK inserted in
   the current round becomes eligible for update/delete only in later rounds. Delete targets
   are deduplicated within the round, and `live_pks` swap-removes are applied by key identity
   after the round resolves, not by stale sampled index. (Makes intra-round
   INSERT-then-DELETE of a fresh PK structurally impossible and keeps the oracle's
   "exclude-deleted-from-upsert" rule exact.)
6. **Inserts use fresh, monotonically increasing PKs; deletes never resurrect.**
7. **`dim_customer` is static after seeding.** Only `fact_orders` receives the stream, so the
   firehose table is the sole tick driver and the join's dimension side is a static trace.
8. **Views created before any data is loaded** (DBSP discipline: a view processes only deltas
   after its creation; the seed is the first delta stream through the DAG).
9. **The `profile` window uses periodic in-window drains by design** (§11). This deliberately
   reverses the intuition that scans must stay out of a profiled window: a drain *is* the
   circuit running on-CPU plus the consolidation read path — both profiling targets — and it
   makes circuit execution observable and bounds `pending_deltas`. The scanned leaf view
   (`v_ext`) is `≤ REGION_CARD` rows, so the read-back cost is negligible.

## 4. Schema

Two base tables, all integer columns, single `BIGINT` primary keys.

```sql
CREATE TABLE dim_customer (
    c_id     BIGINT PRIMARY KEY,
    c_region INT,
    c_nation INT
);

CREATE TABLE fact_orders (
    o_id       BIGINT PRIMARY KEY,
    o_customer BIGINT,
    o_amount   BIGINT,
    o_status   INT
);
```

`dim_customer` is seeded with `c_id` in the inclusive range `[1, DIM_ROWS]`; the generator
draws `o_customer` from the same `[1, DIM_ROWS]` range (§6), so every order joins exactly one
customer and there are **no orphan orders**. `o_amount`/`o_status` drive the membership-flip,
extremum, and payload-churn paths; no dead carrier column is added.

## 5. View DAG

Five views spanning every operator family to profile. Each is a single circuit shape; all
compile today (§2).

```sql
CREATE VIEW v_open AS
    SELECT o_id, o_customer, o_amount
    FROM fact_orders
    WHERE o_status = 1;

CREATE VIEW v_join AS
    SELECT v_open.o_id          AS o_id,
           v_open.o_amount      AS o_amount,
           dim_customer.c_region AS c_region,
           dim_customer.c_nation AS c_nation
    FROM v_open
    JOIN dim_customer ON v_open.o_customer = dim_customer.c_id;

CREATE VIEW v_rev AS
    SELECT c_region, SUM(o_amount) AS revenue, COUNT(*) AS cnt
    FROM v_join
    GROUP BY c_region;

CREATE VIEW v_ext AS
    SELECT c_region, MIN(o_amount) AS lo, MAX(o_amount) AS hi
    FROM v_join
    GROUP BY c_region;

CREATE VIEW v_active AS
    SELECT DISTINCT o_customer
    FROM v_open;
```

| View | Shape | Operator profiled | Depth | Source(s) | Retraction stress |
|------|-------|-------------------|-------|-----------|-------------------|
| `v_open` | filter+projection | linear filter | 1 | `fact_orders` | status flips move rows in/out |
| `v_join` | single equi-join (view⋈base) | bilinear join Δ⋈trace | 2 | `v_open`, `dim_customer` | cascaded membership retraction |
| `v_rev` | GROUP BY | **linear** reduce (SUM/COUNT) | 3 | `v_join` | exact weight arithmetic on retract |
| `v_ext` | GROUP BY | **non-linear** reduce (MIN/MAX) | 3 | `v_join` | history-replay via `AggValueIndex` |
| `v_active` | DISTINCT | distinct (0-boundary crossing) | 2 | `v_open` | `+a`/`−a` at the 0-boundary |

A single `fact_orders` tick drives this entire closure each tick (§2). `v_open` fans to
`v_join` and `v_active`; `v_join` fans to `v_rev` and `v_ext` — the meaningful fan-out
(one view feeding two consumers) is preserved. `v_rev` (linear SUM/COUNT fast path) and
`v_ext` (non-linear MIN/MAX history-replay) are **both** kept because they are distinct
operator code paths, both named profiling targets. `o_customer` is skewed (§6), so the join
fan-out and the `c_region` grouping are hot-key-skewed, stressing `ExchangeShard` routing at
W>1. (A second base-level filter branch and a dead carrier column were dropped: they
exercise no operator the five views above do not already keep hot.)

## 6. Workload generator

State is one growable array `live_pks` (the live fact PKs) and an integer `next_id`. No
payloads are tracked (§2).

**Skew sampler (decided: O(1) power-law over the current live set).** Exact Zipf over a
growing, churning key set is O(N); instead use the power transform

```
idx = floor(N * U**SKEW)        # U ~ Uniform[0,1), N = len(live_pks)
hot_pk = live_pks[idx]
```

`U ∈ [0,1)` gives `idx ∈ [0, N-1]` (no out-of-bounds), and `P(idx < N·x) = x**(1/SKEW)`.
With `SKEW = 3.0`, ~50% of accesses fall on the first 12.5% of slots and ~80% on the first
51% — concentrated hot-key access, O(1), adapting to N automatically. Deletes use
swap-remove (`live_pks[idx] = live_pks[-1]; pop()`), so hot *slots* stay hot while their
*key identity* churns. Customer selection uses the same transform mapped to the inclusive
range with `o_customer = 1 + floor(DIM_ROWS * U**SKEW_CUST)` and `SKEW_CUST = 2.0`, so some
regions are hot and every customer id is a seeded `dim_customer` PK (no orphans). Skew is for
realistic data shape (uneven group/trace sizes), not correctness; the sampler is seeded
(`SEED = 42`) so `verify` is reproducible.

**Seeding.** Create schema, tables, then all views (invariant 8). Load `dim_customer`
(`DIM_ROWS` rows, `c_id = 1..DIM_ROWS`, `c_region = c_id % REGION_CARD`,
`c_nation = c_id % NATION_CARD`) in one `push`. Load `fact_orders` (`FACT_SEED_ROWS` rows,
PKs `1..FACT_SEED_ROWS`) in `SEED_CHUNK`-row pushes (`SEED_CHUNK > 10_000`, so seed pushes
auto-tick); per row `o_customer` skewed, `o_amount` uniform in `[0, AMOUNT_MAX)`,
`o_status = 1` with probability 0.5 else uniform in `{0,2,3,4}`. Seed pushes flow through the
DAG as deltas. After seeding, `scan` one view once to flush the tail (outside any perf
window). Initialize `live_pks = [1..FACT_SEED_ROWS]`, `next_id = FACT_SEED_ROWS + 1`. The
push/seed loop reuses the `resolve_table → ZSetBatch(schema) → append(**row) → push(tid)`
pattern (cf. `benchmarks/helpers/datagen.bulk_load`).

**Round resolution.** A round draws `BATCH_OPS` operations from `MIX`
(`INSERT 0.52, UPDATE 0.46, DELETE 0.02`) against a **round-start snapshot** of `live_pks`
(invariant 5):

- **INSERT** — `pk = next_id++`; emit a fresh row (skewed `o_customer`, random
  `o_amount`/`o_status`); queue `pk` for append after the round.
- **UPDATE** — `pk = hot_pk` from the snapshot; emit a fresh full row at `pk` (row-replace;
  engine auto-retracts the old payload). Fresh `o_status`/`o_amount` exercise membership-flip
  and extremum paths.
- **DELETE** — `pk = hot_pk` from the snapshot; add to the round's delete set.

The round resolves to **one upsert batch** (insert + update rows, per-batch PK-deduped
last-wins, deleted PKs excluded — invariant 4) pushed via `Connection.push`, then **one**
`Connection.delete(fact_tid, fact_schema, sorted(delete_set))`. After both ACK, `live_pks`
is updated: append the inserted PKs, swap-remove the deleted PKs by identity. The generator
yields `(upsert_rows, delete_pks)` per round.

**Generator self-checks (cheap, always on).** To prevent a generator bug from passing the
oracle silently (the oracle is fed the same ops it sends the engine), assert per round:
every `delete_pk` was in the round-start snapshot; every `o_customer ∈ [1, DIM_ROWS]`;
`len(live_pks) == (inserts so far) − (deletes so far) + FACT_SEED_ROWS`. A violation aborts
non-zero before any oracle comparison.

## 7. Run configurations

One code path, two decided parameter sets (`benchmarks/profiling/config.py`):

| Parameter | `verify` | `profile` |
|-----------|----------|-----------|
| `DIM_ROWS` | 1 000 | 50 000 |
| `REGION_CARD` / `NATION_CARD` | 50 / 25 | 500 / 100 |
| `FACT_SEED_ROWS` | 20 000 | 1 000 000 |
| `BATCH_OPS` | 4 000 | 50 000 |
| `SEED_CHUNK` | 20 000 | 100 000 |
| duration control | `STREAM_ROUNDS = 60` (fixed, deterministic) | `PERF_SECONDS = 90` (wall-clock loop) |
| checkpoint / drain | oracle compare every `CHECKPOINT_EVERY = 10` rounds | synchronous drain every `DRAIN_EVERY = 40` rounds |
| binary | debug | release (`gnitz-server-release`) |
| perf | off | on (DWARF call graphs) |
| end check | exact oracle assert | sanity + quantitative-coverage asserts |

Shared constants: `MIX = (0.52, 0.46, 0.02)`, `SKEW = 3.0`, `SKEW_CUST = 2.0`,
`AMOUNT_MAX = 1_000_000`, `OPEN_STATUS = 1`, `SEED = 42`, `SCHEMA = "profiling"`. `verify`
runs at W=1 and W=4; `profile` runs at W=4.

**Why a wall-clock window for `profile`, not a fixed op count.** `perf -F 99` needs the
circuit on-CPU for tens of seconds to resolve six operator families across four workers. A
fixed op count makes the window's wall-clock a hostage to per-round timing; a short window
hands `perf` a few dozen worker samples — noise. `profile` therefore **loops the round
generator until `PERF_SECONDS` of wall-clock elapses** inside the window, so the sample count
is bounded by time, not by a round budget. `BATCH_OPS = 50_000` keeps each per-worker delta
large (≈ 12.5k rows/worker at W=4 against a ≥ 250k-row trace partition), so each tick is
substantial operator work that dominates the per-round fsync cost — workers stay on-CPU most
of the window. `verify` keeps a fixed `STREAM_ROUNDS` for reproducibility (the oracle needs a
deterministic op sequence).

## 8. Correctness (`verify` config) — reuse the existing oracle

The `verify` config reuses `crates/gnitz-py/tests/_oracle.py` (the differential
weight-multiset harness already used by the engine test suite): the runner adds
`crates/gnitz-py/tests` to `sys.path` and `import _oracle`. The runner maintains the two
mirror dicts directly with the harness:

```
base: dict[o_id]  -> {o_customer, o_amount, o_status}   # apply_insert/apply_update/apply_delete
dim:  dict[c_id]  -> {c_region, c_nation}               # filled at seed, static
```

Expected per-view multisets are composed **through the DAG topology** (each downstream view
from the previously-computed upstream multiset, not re-derived from `base`), using the
harness plus one small new helper `oracle_group_aggregate` (group a multiset by a key,
emit `(key, sum, count)` for `v_rev` and `(key, min, max)` for `v_ext`):

- `v_open`   = `oracle_filter_project(base, where=status==OPEN_STATUS, project=[o_id,o_customer,o_amount])`
- `v_join`   = `oracle_equijoin(v_open_rows ⋈ dim on o_customer==c_id, out=[o_id,o_amount,c_region,c_nation])`
- `v_rev`    = `oracle_group_aggregate(v_join, key=c_region, agg=sum/count over o_amount)`
- `v_ext`    = `oracle_group_aggregate(v_join, key=c_region, agg=min/max over o_amount)`
- `v_active` = distinct `o_customer` over `v_open` (filter-project then collapse to weight 1)

Comparison reuses `_oracle.scan_multiset` + `assert_view_matches` (sum `row.weight` per
projected tuple, assert no negative net weight, structured per-tuple diff on mismatch). Scan
projections are the named output columns read from each scanned row's `_asdict()` — never a
separately-maintained column list that could drift from the SQL.

**Checkpoint discipline (stronger than a single final compare).** A single end-of-run
comparison checks only the final integral and consolidates away ghost/cancelling-weight bugs.
Instead, `verify` compares every view against the oracle **every `CHECKPOINT_EVERY` rounds**
(each checkpoint `scan` forces a full drain). This observes the integral at multiple points
along the delta stream, so incremental retraction — grouped MIN/MAX history-replay and
distinct boundary crossings — is actually exercised and observed, not just the terminal
state. A mismatch prints the per-tuple diff and exits non-zero. Runs on the debug binary so
engine asserts fire.

## 9. Sanity + coverage checks (`profile` config)

No full oracle at large scale. After the stream, `scan` each view (final drain) and assert:
every scanned weight is `+1` (no negative/ghost leak); `Σ v_rev.cnt == |v_join|` (the join
output count, **not** `|v_open|`); `v_rev` and `v_ext` have identical `c_region` key sets of
size `≤ REGION_CARD`; every `v_active` customer appears in some `v_open` row; every view is
non-empty and larger than at post-seed (proving the stream propagated and the circuit
ticked). Because there are no orphan orders (§6), `|v_join| == |v_open|`, but the assertion is
written against `|v_join|` so it stays correct even if the mapping is later changed.

**Quantitative profiling-coverage gate.** Parse the worker-PID-filtered `perf report` (§10)
and assert (a) the total worker on-CPU sample count is `≥ MIN_WORKER_SAMPLES` (catches the
sample-starvation failure — a short or idle window), and (b) the summed self-time of the
operator/consolidation/exchange symbols is `≥ MIN_OPERATOR_FRACTION` of worker on-CPU
samples. This turns acceptance criterion 3 from a subjective read into a falsifiable gate.
A failure exits non-zero.

## 10. perf integration

Reuse `benchmarks/helpers/perf.PerfRecorder` with **DWARF call graphs** (`PerfRecorder(...,
dwarf=True)`): the `profile` config uses the release binary, whose heavily-inlined operator
kernels collapse into a few mega-frames under frame-pointer unwinding; DWARF recovers the
inlined callers so join/reduce/distinct/consolidation self-time separates. `PerfRecorder`
records the master PID plus its discovered worker children, writes `pids.json`, patches the
header on SIGINT stop, and emits a flamegraph. The runner:

```
rec = PerfRecorder(master_pid, results_dir, dwarf=True)   # workers are master's children
rec.start()                                                # AFTER seed + tail-flush
run_stream_until(PERF_SECONDS, drain_every=DRAIN_EVERY)    # the only thing in the window
rec.stop(); rec.flamegraph()
```

Generate `report.txt` by reusing `benchmarks/report.py::report_perf`, which reads
`pids.json` and emits **separate master and worker** hot-function tables via
`perf report --stdio --no-children -n --pid <pids>`. The master-vs-worker split is essential:
the single-threaded master owns SAL/IPC/relay/fsync, and only the worker table answers
"is operator code material." Write a small `summary.json` (`rounds`, `wall_s`, `workers`,
`config`, per-view final row counts, `commit`/`dirty` via the same `git rev-parse --short`
+ `git status --porcelain` stamping as `benchmarks/conftest.py::write_results`, plus the
worker operator-sample fraction from §9). Outputs land in
`benchmarks/results/<UTC-timestamp>_profile_w4/` (the runner creates this dir and never
prunes; the existing `run.py` rotation is independent — note it may itself reap old profile
dirs once `results/` exceeds its keep count, which is acceptable).

**Prerequisite (documented, not enforced).** `perf record -p <pid>` against another process
requires `kernel.perf_event_paranoid ≤ 1` (or `CAP_PERFMON`/root). `PerfRecorder` degrades
gracefully (prints a hint, runs without profiling) if denied, so a missing privilege does
not crash the run — but the coverage gate (§9) will then fail on zero samples, surfacing it.

## 11. Runner control flow (`run_profile.py`)

CLI: `--config {verify,profile}`, `--workers N`. The runner inserts `benchmarks/` and
`crates/gnitz-py/tests` onto `sys.path` (it is invoked as a plain script from
`crates/gnitz-py`, so neither is on the path by default) to import `helpers.perf`
(`benchmarks/helpers/perf.py`), `report` (`benchmarks/report.py`, top-level — call
`report_perf(results_dir/"perf.data", file=...)`), and `_oracle` / `_serverproc`
(`crates/gnitz-py/tests/`). Flow:

1. Resolve binary (`GNITZ_SERVER_BIN` env, else release for `profile` / debug for `verify`)
   and start the server via the reused `_Server` (`_serverproc._Server(binary).start()`;
   waits ≤ 10 s for the socket; `master_pid = server.proc.pid`; PDEATHSIG ties the master to
   the runner so an interrupted run cannot orphan the ~1 GB SAL).
2. `gnitz.connect(server.sock_path)`; create schema, tables, then views (`execute_sql`);
   `resolve_table` each base table → `(tid, schema)` and each view → `(view_id, _)`.
3. Seed `dim_customer` and `fact_orders`; `scan` one view once (tail-flush).
4. `verify`: run `STREAM_ROUNDS` rounds, mirroring each round into the oracle and comparing
   every `CHECKPOINT_EVERY` rounds. `profile`: `rec.start()`; loop rounds until `PERF_SECONDS`
   elapse, forcing a synchronous drain (`scan(v_ext)`, ≤ `REGION_CARD` rows) every
   `DRAIN_EVERY` rounds to keep the circuit on-CPU and exercise the consolidation read path;
   `rec.stop(); rec.flamegraph()`.
5. End check: `verify` runs a final oracle compare of every view; `profile` runs the sanity
   asserts (§9), generates `report.txt` via `report_perf`, applies the quantitative coverage
   gate, and writes `summary.json`.
6. `_Server.teardown()` (kills the master, copies worker logs, removes the data dir + SAL).

## 12. File-change summary

| File | Change |
|------|--------|
| `benchmarks/profiling/__init__.py` | new — empty package marker |
| `benchmarks/profiling/config.py` | new — all constants; `VERIFY`/`PROFILE` parameter dicts |
| `benchmarks/profiling/schema.py` | new — DDL + `CREATE VIEW` SQL strings only (no column lists) |
| `benchmarks/profiling/workload.py` | new — power-law sampler, `seed()`, `stream()` with round-start snapshot discipline (invariant 5), generator self-checks |
| `benchmarks/profiling/oracle_ext.py` | new — thin: `import _oracle`; add `oracle_group_aggregate` + the per-view DAG-topology expected-multiset composition |
| `benchmarks/profiling/run_profile.py` | new — CLI runner orchestrating both configs (§11) |
| `crates/gnitz-py/tests/_serverproc.py` | edit — move the `_Server` class here from `tests/conftest.py` (no behavior change) so both the test fixtures and this runner import one implementation |
| `crates/gnitz-py/tests/conftest.py` | edit — import `_Server` from `_serverproc` instead of defining it inline |
| `Makefile` | add `bench-profile` and `bench-profile-verify` targets |

Dropped versus a from-scratch design: no `verify.py` (use `_oracle.scan_multiset` +
`assert_view_matches`), no standalone `oracle.py` (folded into the thin `oracle_ext.py`), no
`benchmarks/helpers/server.py` (reuse `_Server`), no bespoke `report.txt` generator (reuse
`report.py::report_perf`), no hand-maintained per-view column lists (read from scanned-row
`_asdict()`).

Makefile targets (matching the existing `cd crates/gnitz-py && uv run` pattern):

```make
bench-profile: release-server pyext-release ## Realistic profiling workload + perf (W=4)
	cd crates/gnitz-py && GNITZ_RELEASE=1 GNITZ_WORKERS=4 \
	  uv run python ../../benchmarks/profiling/run_profile.py --config profile --workers 4

bench-profile-verify: server pyext ## Correctness gate for the profiling workload (debug)
	cd crates/gnitz-py && GNITZ_SERVER_BIN=$(abspath gnitz-server) GNITZ_WORKERS=1 \
	  uv run python ../../benchmarks/profiling/run_profile.py --config verify --workers 1
	cd crates/gnitz-py && GNITZ_SERVER_BIN=$(abspath gnitz-server) GNITZ_WORKERS=4 \
	  uv run python ../../benchmarks/profiling/run_profile.py --config verify --workers 4
```

## 13. Acceptance criteria

1. **Correctness, W=1 and W=4 (debug).** `make bench-profile-verify` runs seed + stream;
   every view's scanned weighted multiset equals the `_oracle` reference exactly (values and
   weights) at every checkpoint and at the end; generator self-checks pass; no server crash;
   exit 0. Validates that the insert/update/delete stream — including grouped MIN/MAX and
   distinct retraction — maintains every view correctly, observed incrementally.
2. **Sustained maintenance under perf, W=4 (release).** `make bench-profile` seeds 1 M fact
   rows and streams for `PERF_SECONDS` without crash; every view ends larger than post-seed;
   sanity asserts pass; `perf.data`, `flamegraph.svg`, `report.txt`, `summary.json` are
   produced under `benchmarks/results/<ts>_profile_w4/`.
3. **The intended hot paths are present and material — falsifiably.** The worker-PID table in
   `report.txt` attributes `≥ MIN_OPERATOR_FRACTION` of worker on-CPU self-time to DBSP-operator
   and consolidation code (join Δ⋈trace, SUM/COUNT and MIN/MAX reduce, distinct, sort-merge
   consolidation, exchange/scatter), with total worker samples `≥ MIN_WORKER_SAMPLES`; the
   §9 coverage gate enforces both and fails the run otherwise. The durable-write path
   (WAL/SAL/fsync) is expected to appear chiefly in the master table and in kernel/kworker
   contexts excluded by the `--pid` filter.
4. **No ungrouped-aggregate exposure.** No view computes an ungrouped aggregate; a grep of
   `schema.py` confirms every aggregate has `GROUP BY`.
