Never check if failures are pre-existing; as a rule, all tests are green at the start of a session.

Never check or confirm that compilation warnings or Clippy errors/warnings are caused by the current session, always directly fix them.

Use `semble search` (the semble MCP server is installed) to find code by describing what it does or naming a symbol/identifier instead of grep. There are other useful semble commands as well. Prefer it over grep + read every time. Use `semble clear all` to clear a stale index.

gnitz is **pre-alpha** and not used in production anywhere because it has not been released, so there are **never** any compatibility concerns and there should **never** be legacy code remaining.

**Plans (`plans/`):**
- Never cite a plan path/filename in code, tests, or comments.
- Not ground truth: validate against source/tests/git; only edit the plan named for the task.
- Validate the problem, not just the design: a claimed cost must state the input size that
  can actually occur (trace the producer — a `MAX_` cap is a trust boundary, not a reachable
  range) and the largest instance present in the tree.
- Authoring: state one committed design — no history, no optional/either-or, no "follow-on/future-work" (fold in, spin out a new plan, or drop); include validated snippets; never cross-link plans (state needed facts inline); out-of-scope bugs get their own plan. When a plan is written, all decisions need to be made - no "gates" in plans are allowed.
- Don't write memories about a specific plan (they're transient).

**Comments:**
- Say whether a comment describes current behaviour or a cost the code avoids; a
  counterfactual read as behaviour sends the next reader down the wrong path.
- Scope a contract to what it governs — a claim true only of its own function must not read
  as a system invariant.

**This file states contracts and invariants, not mechanisms.** A private type or
function name, a numeric default, or a step-by-step of how one module currently
works belongs in that module's doc comment, where it is next to the code that
would falsify it. Don't add such detail here; it rots silently.

GnitzDB-specific development guidelines are in the **GnitzDB Developer Guide**
section at the end of this file.

# Over-arching Design Rules

The fundamental mottos are "ZSets all the way down" and "ZSets over the wire". Never go against these ideas.

---

# GnitzDB Theoretical Foundations

Read this before reasoning about storage, operators, or merge paths. Most of it
exists to overwrite a standard-database assumption that is **wrong here**:

| Standard DB assumption | GnitzDB | |
|---|---|---|
| A row is present or absent | A row carries an integer **weight**, which may be negative | §1 |
| A row is identified by its PK | Identity is **(PK, all payload columns)** | §1 |
| A PK is unique | Unique for base tables and reduce output only; intermediate batches routinely repeat a PK | §1 |
| A PK is a typed tuple, compared by dispatching on type | A PK is an **opaque ordered byte string**; every comparison is one `memcmp` | §1, §4 |
| Sorting by key is enough to group | Sorting by PK alone **silently corrupts weights** | §2 |
| A query recomputes over current state | Operators consume and emit **deltas**; state lives in a separate integral | §3 |
| Aggregation is a fold over rows | An aggregate emits a **retraction of the old value plus the new value** | §3 |
| DISTINCT / EXCEPT / outer joins need an anti-join | There is **no anti-join operator** — all of them are weight arithmetic | §3 |

## 1. Z-Sets

A **Z-Set** over a domain *D* maps each element to an integer **weight**
(multiplicity), with finite support. Z-Sets form an **abelian group** under
pointwise addition — every DBSP theorem below depends on that group structure.

Here *D* is the set of rows of a table schema, and an element is identified by
**(PK, all payload columns)**. The PK alone does *not* identify an element.

**Weights.** Accumulated base-table weights are always ≥ 0, enforced by the DML
layer (INSERT = +1, DELETE = retraction, UPDATE = retract + insert).
Intermediate circuit nodes may produce negative weights; base-table traces never
accumulate one. Load-bearing: `distinct` assumes it, and DBSP Propositions
4.4/4.5 (the distinct-elimination rules) require positive inputs.

**Indexed Z-Sets** map keys to Z-Sets, `K → (V → ℤ)`. GROUP BY produces one via a
grouping function that is **linear**, so GROUP BY itself needs no state and is
incrementally free; only the aggregation within each group needs the integral.
`map_reindex` (repartitioning by group/join key) is its physical form.

**PK uniqueness is not a general invariant.** The PK region is a sort/routing
key. It is unique for base-table batches (`enforce_unique_pk` on ingest) and for
reduce output (one row per group). It is **not** unique for intermediate batches:
`map_reindex` overwrites the PK region with a join/group column value, and join
output inherits the left input's PK. Every operator uses full (PK, payload)
identity; none may assume PK uniqueness.

**PK columns** are an ordered list of fixed-width integer scalars, signed or
unsigned, non-nullable. STRING, BLOB and float columns cannot be PK columns —
IEEE-754 breaks the byte-equal key contract, since ±0.0 differ byte-wise but
compare equal numerically, and NaN has no canonical bit pattern.

**A PK is an opaque, ordered byte string wherever it flows.** It is stored as an
order-preserving key (OPK, §4) whose plain unsigned byte comparison *is* the
typed lexicographic PK order, at any width — so every ordered operation (merge,
sort, consolidation, range scan) is one `compare_pk_bytes`, and the encode/decode
boundary is the only type-aware code. The encoding is also a bijection, so
byte-equal ⟺ key-equal and consolidation and dedup group on the raw bytes.
Routing and XOR8 probes hash those same bytes, making the hash a **pure function
of the logical key**: equal keys co-partition and co-probe, and physical width or
padding at the wire boundary never moves a key.

**Hidden key slots.** Synthetic view keys (`_join_pk`, `_set_pk`, `_group_pk`, …)
and unprojected passthrough PK columns are real schema columns flagged hidden
(`META_FLAG_HIDDEN`). They are excluded from wildcard expansion, name resolution,
duplicate-name checks and client rows; PK region, routing, sort and consolidation
are unaffected. Base-table columns are never hidden.

## 2. Z-Set Operations

**Addition** `(A + B)(x) = A(x) + B(x)` is batch concatenation followed by
consolidation. **Negation** is `(-A)(x) = -A(x)`.

**Consolidation** groups duplicate (PK, payload) entries, sums their weights, and
drops elements whose net weight is zero ("ghost elimination").

> **Consolidation is the only operation that merges duplicate rows and sums
> weights.** (In Z-Set algebra filter/projection/join also change element count —
> but those operate on the mathematical Z-Set, not on unconsolidated batches.)

Consolidation uses a **total order**, not hash grouping — sort-merge is what
enables N-way merging, range scans and compaction. Sort key: PK first, by
unsigned byte comparison over the OPK region (`compare_pk_bytes`), then payload
columns in schema order (`compare_rows`). Payload comparison skips PK columns:
null < non-null and null == null; STRING by German-string comparison; F64/F32 by
`total_cmp`, so NaN has a defined position and transitivity holds for every
input; everything else as a sign-extended signed integer.

> **Every merge and consolidation path must sort by (PK, payload), never by PK
> alone.** PK-only ordering interleaves rows that share a PK but differ in
> payload, so weights accumulate against the wrong element. It bites the
> non-linear aggregates hardest: MIN/MAX secondary-index entries routinely share
> a PK while varying in payload.

> **Every batch entering an N-way merge must already be sorted by (PK, payload).**
> A merge reads each input linearly; an unsorted input makes the heap deliver
> duplicates non-adjacently and the accumulator produces *wrong weights
> silently* — no error, no assertion.

## 3. DBSP: Incremental Computation on Z-Sets

GnitzDB implements the DBSP model: a query consumes the input **delta** and
produces the **output delta**, never recomputing from current state. A **stream**
is a sequence of Z-Sets indexed by tick.

> Numbered results cite Budiu et al., *DBSP: Automatic Incremental View
> Maintenance for Rich Query Languages*, **PVLDB 16(7):1601–1614, 2023**. The
> arXiv preprint (2203.16684v1) numbers several of them differently — Inversion
> is 2.22 there, and the distinct rules are 4.5/4.6. Check the edition before
> trusting a number.

Incrementalization is two steps, not one. A scalar operator is first **lifted** to
a stream operator, `(↑f)(s)[t] = f(s[t])` (Def 2.3); the incremental version is
then `Q^Δ = D ∘ Q ∘ I` for a *stream* operator Q (Def 3.1) — so a lifted query
becomes `(↑Q)^Δ = D ∘ ↑Q ∘ I`. Because **D and I are inverses** (Thm 2.20,
*Inversion*), the **chain rule** `(Q₁ ∘ Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ` holds (Prop 3.2) —
each sub-operator incrementalizes independently. **GnitzDB therefore omits D
entirely:** circuits are compiled in incremental form, linear operators apply
straight to deltas (Thm 3.3 *Linear*, `Q^Δ = Q` for LTI Q), and only non-linear
and bilinear operators consult the integral. An epoch's output is already a
delta.

**Single-source-per-epoch.** An epoch carries one input delta from one source —
a circuit never sees simultaneous deltas from two sources. This makes the
bilinear cross-delta term `dA ⋈ dB` always zero and keeps trace cursors
(snapshotted at tick start) valid throughout.

### Linear operators

`L(A + B) = L(A) + L(B)`: filter, map, negate, UNION ALL, delay (z⁻¹).
Incrementalization adds no state (Theorem 3.3) — delay holds one tick by
definition, but nothing *extra*. Consolidation before a linear operator is
optional.

### Bilinear operators

Join is bilinear: output weight = product of input weights. Both operands need
their integral, and consolidation is required.

Thm 3.4 (*Bilinear*) takes the two **change** streams `dA`, `dB` as its inputs;
`I(dA)` and `I(dB)` are the relations they accumulate. Keep the `d` on both
operands of a term or on neither — `I(A)` would be a second integral, not a
relation.

```
⋈Δ(dA, dB) = dA ⋈ dB + z⁻¹(I(dA)) ⋈ dB + dA ⋈ z⁻¹(I(dB))
           = I(dA) ⋈ dB + dA ⋈ z⁻¹(I(dB))
```

The theorem states both forms as one equality: the collapse folds `dA ⋈ dB` into
new-state `I(dA)` on the left, leaving old-state on the right. **GnitzDB uses the
symmetric 2-term form** `dA ⋈ z⁻¹(I(dB)) + dB ⋈ z⁻¹(I(dA))` — both sides read
old-state cursors, correct because single-source-per-epoch forces `dA ⋈ dB = 0`.

**Shared-source branches** (`t ⋈ view-over-t`) do not break this: one push
reaches the two join inputs in two *separate* epochs and trace cursors are
rebuilt each epoch, so the later epoch joins against a trace that already
absorbed the earlier delta — the asymmetric form realized across epochs, with the
cross-term emitted exactly once. The one shape that would put two deltas in one
epoch, the same source feeding both inputs, is rejected by the planner (self-join
and same-relation INTERSECT/EXCEPT guards; the discriminator is source-id
equality, not base-table overlap).

**Join output schema** is `[left_PK, left_payload..., right_payload...]`. Output
PK = left input PK = the join key after exchange repartition; the right batch's
PK is not duplicated. Original table PKs survive as payload columns, moved there
by `map_reindex`.

### Outer joins (LEFT / RIGHT / FULL)

Described for LEFT; RIGHT is the mirror, FULL does both sides. For each delta
row: if inner matches exist, emit them at `w_delta × w_trace`; if none, emit one
null-filled row at `w_delta`. This is **not bilinear** — output weight depends on
match *existence*, not on weight arithmetic alone.

The paper does not incrementalize outer joins — it lowers them to plans with
extra joins, as Feldera does by injecting ghost `(k, NULL)` tuples into an inner
join. GnitzDB's construction is join-free and its own.

There is **no fused outer opcode**. `LEFT JOIN = inner ∪ null_extend(ν)`, where
`ν` is the unmatched preserved rows at their true multiplicity: per preserved
identity `x` with weight `w_A ≥ 0`, and `S ≥ 0` the summed other-side weight it
matches, `ν(x) = w_A · [S = 0]`. Two contracts bind every realization:

- **Weight-exact for a bag-valued preserved side** (e.g. a `UNION ALL` view).
  Clamping a *witness* to 1, rather than clamping the *result* of a weight-exact
  subtraction, leaks a spurious weight-`w−1` null-fill on a matched weight-`w` row.
- **Computable partition-locally**, cancelling per worker before any output
  exchange. A realization needing a second sequential exchange to co-locate both
  operands is inadmissible — the compiler forbids it.

Equi and band joins implement all three orientations. Pure range (`n_eq == 0`)
supports LEFT only — RIGHT/FULL is rejected at plan time — and derives from a
MIN/MAX threshold row, so its range column must be a ≤8-byte integer.

### Non-linear operators

These consult the accumulated integral, and **consolidation is mandatory** — they
must see true net weights. This is enforced, not assumed.

*Distinct* is O(|delta|) via Prop 4.7: point lookups into the integral detect
transitions across the positive boundary. Zero counts as non-positive — Def 4.3
is `weight > 0 → 1, else 0`, and the transition test is `> 0` against `≤ 0`, so
"changes sign" (the paper's prose) is looser than the actual boundary.

*Reduce* emits `δ_out = Agg(history + δ_in) − Agg(history)` — the old aggregate
at weight −1, the new at +1.

- **SUM and COUNT are linear**, so `new = old + delta_contribution`: no history
  replay, and consolidating the input delta is skippable.
- **MIN and MAX are not.** Retracting the current extremum needs the next value
  from history, so they carry a secondary value index instead of scanning the trace.
- **Float SUM/AVG is order-dependent** — addition is non-associative, so the value
  is a function of (query, data, worker count, access path, chunk size) and
  reproduces only while all of those hold. A *maintained* view is worker-count
  stable (the two-phase combine excludes float SUM, so a global one keeps the
  single-worker funnel and a grouped one lands each group on one worker), but its
  **backfill** chunks per worker over whatever cursor the cost gate picks, so
  `CREATE INDEX`, more data, or a different worker count each move the low bits;
  the **ad-hoc fold** reassociates once more, summing per-worker partials in reply
  order. Use an integer type where exactness matters.

*Set operations* (UNION/INTERSECT/EXCEPT, DISTINCT and ALL) are **join-free**:
each is a linear combination of `{union, negate}` plus the weight-clamp primitive
(`distinct = clamp[-1,1]`, `positive_part = clamp[0,i64::MAX]`) over
content-hashed leaves — EXCEPT DISTINCT = `positive_part(distinct(A) −
distinct(B))`, INTERSECT DISTINCT = `distinct(A) − positive_part(distinct(A) −
distinct(B))`. There is **no anti-join operator**: these and the outer-join
null-fills are the only `positive_part` users.

### The integral (trace)

`I(A)_t = Σ(dA_0..dA_t)`. Each tick's delta is added to a persistent or ephemeral
store; an operator reads it back through a cursor that merges the in-memory and
on-disk tiers on the fly, summing weights of matching (PK, payload) entries and
dropping ghosts — so every tier depends on §2's sort invariant. Trace cursors are
snapshotted at tick start, so an operator sees `z⁻¹(I(X))`, the integral *before*
the current delta.

## 4. The Region Convention

Column buffers are flat (pointer, size) pairs in canonical order — the layout of
both WAL blocks and shard files. `pk_stride` is the encoded key width: the sum of
the PK columns' encoded widths, tightly packed, fixed for a given schema.

```
region[0] = pk         (count × pk_stride bytes, OPK key)
region[1] = weight     (count × 8 bytes, i64 LE)
region[2] = null       (count × 8 bytes, u64 LE, 1 bit per payload col)
region[3..3+P-1] = payload columns (non-PK, schema order)
region[3+P] = blob     (variable-length string heap)
```

The PK region holds the **order-preserving key (OPK)** — the same encoding at rest
and in-engine: the PK columns concatenated in PK-list order, each big-endian with
signed columns sign-flipped. PK columns are not repeated in the payload regions.

Null bitmap uses **payload column indexing**: bit N is the N-th non-PK column in
schema order. For a **single-PK** schema this is `payload_idx = ci if ci <
pk_index else ci - 1`; with a **compound PK** the columns are renumbered around
*every* PK position, so that closed form does not hold — read the schema's
`payload_mapping`, never `ci - 1`.

---

# GnitzDB Developer Guide

## Prerequisites

Linux with an io_uring kernel (≥ 5.x); stable Rust; `uv` (drives Python + `maturin`). Nothing is version-pinned.

## Build targets

`make help` lists every target with its own description — that is the current
list, this is not. The ones whose semantics are not obvious from the name:

- **`make verify`** = `fmt-check` + `clippy` (warnings are errors) + `test`.
  The pre-commit gate; there is no CI.
- **`make test`** builds all workspace crates except `gnitz-py` (a pyo3
  extension can't link a test harness). `make test T=name` runs one test;
  `make rust-engine-test` is the faster engine-only loop.
- **`make e2e`** rebuilds *both* the server binary and the Python extension
  (which carries the SQL planner) before running pytest. Maturin no-ops when
  nothing changed, so the rebuild costs nothing — never skip it, and never
  trust a stale binary.

## Project structure

Two sides that meet at the wire protocol: the **SQL/client side** (a library,
also exposed as C and Python bindings) plans and drives queries, and ships them
over `gnitz-wire`; the **engine** executes them as a multi-process server and
never links the planner. `gnitz-mirror` is the sole crate on both sides, and
links no planner either.

### Workspace crates

| Crate | Role | Depends on |
|-------|------|------------|
| `gnitz-wire` | Wire-protocol constants + codecs — the one definition client and engine must agree on | — |
| `gnitz-expr` | The one expression evaluator, and the resolved column addressing it reads through | `wire` |
| `gnitz-core` | Client core: connection, protocol, and the logical type / expression / circuit model | `wire`, `expr` |
| `gnitz-sql` | SQL front end: parser, binder, query planner | `core`, `expr`, `wire` |
| `gnitz-capi` | C ABI bindings over the client core + planner | `core`, `sql` |
| `gnitz-py` | Python extension (pyo3) — the driver + planner the test/benchmark suites run against | `core`, `expr`, `sql`, `wire` |
| `gnitz-engine` | The single-node database as a library: Z-set store, DBSP operators, circuit compiler, catalog | `wire`, `expr` |
| `gnitz-server` | The multi-process server binary — the `runtime` rung and nothing else | `engine`, `wire`, `expr` |
| `gnitz-mirror` | The client mirror: a local copy of a view answering reads in the host's process — the one crate on both sides, and a leaf | `core`, `engine`, `wire` |
| `gnitz-engine-testkit` | Dev-only: `gnitz-engine`'s test helpers, compiled as a library so `gnitz-server`'s tests reach them | `engine`, `wire` |
| `gnitz-test-harness` | Spawns a `gnitz-server` subprocess in a private tmpdir for integration tests | — |

### The engine (`gnitz-engine`)

Strictly layered — every module depends only on those beneath it (and all of them
on `foundation`):

```
runtime (L7)   → catalog, query, ops, storage, schema    orchestration · protocol · reactor
                 — lives in `gnitz-server`, and is the only rung that does
catalog        → query, ops, storage, schema
query (L5)     → ops, expr, storage, schema               compiler · vm · dag
ops            → expr, storage, schema                    join · reduce · exchange · …
expr           → storage, schema
storage        → schema                                   repr (L2) · lsm (L3)
schema         → foundation
foundation (L0)  — independent leaves; depends on nothing
```

Each subsystem's `mod.rs` header states its own surface, its internal split, and
what is deliberately closed off. Read that rather than a summary here.

`gnitz-wire` and `gnitz-expr` sit **below this whole table**, like `foundation`:
they are separate crates, so a `storage`-layer `use gnitz_expr::RowSource` is not
an up-edge into the engine's own `expr` module. Read `expr` in the ladder above as
the engine-local expression layer only.

A relation is stored as one `Table` per worker. Test scaffolding lives in
`test_support` / `test_rng` and per-module `tests/`. `test_support` is split by
reach: `shared` is compiled a second time as `gnitz-engine-testkit` and so sees
only the engine's public API, `internal` is this crate's own. A helper goes in
`internal` unless `gnitz-server` needs it — putting an engine-only helper in
`shared` forces whatever it touches to become published API.

## Running E2E tests

**Always run E2E with multiple workers.** Single-worker mode skips
exchange/fanout paths and will miss distributed bugs.

```bash
make e2e                       # rebuilds server, GNITZ_WORKERS=4
make e2e K='joins' WORKERS=1   # pytest -k filter; override worker count
make test T=name               # one Rust test (make rust-engine-test = engine only)
```

Note that when piping `make e2e` or any other command through `| tail ...`, do not trust the exit code 0, since that only says that `tail` exited successfully, not necessarily the tests.

## Environment variables

The knobs a session usually reaches for. This is not the full set — every
`GNITZ_*` override is declared next to the value it overrides, with its default.

| Var | Effect |
|-----|--------|
| `GNITZ_WORKERS` | Worker count (Makefile/tests → server `--workers`) |
| `GNITZ_LOG_LEVEL` | `quiet` / `normal` / `verbose` (`debug` = alias) |
| `GNITZ_CPU_AFFINITY` | Pin master and workers to CPUs (default on); `0` when servers share a host |
| `GNITZ_SERVER_BIN` | Override server binary (e.g. aim E2E at the release build) |
| `GNITZ_CHECKPOINT_BYTES` | SAL checkpoint threshold |
| `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES` | CREATE UNIQUE INDEX pre-flight in-RAM sort budget before spilling to disk |
| `GNITZ_CLIENT_SEND_TIMEOUT_MS` | Per-frame deadline on ring-slot client egress before a stalled client is evicted |
| `GNITZ_RAM_TIER_BYTES` | Per-store RAM-tier ceiling before it spills to a shard; shrink it to reach the disk regime on small data |

## Debug logging

```bash
cd crates/gnitz-py && GNITZ_WORKERS=4 GNITZ_LOG_LEVEL=debug uv run pytest -x <test-file>
```

Log format: `<epoch>.<ms> <tag> <level> <message>` — tags: `M` (master), `W0`–`WN`.

For temporary logging: `gnitz_debug!` / `gnitz_info!` macros. Remove before committing.

### Where test logs go

Python E2E scratch lives in the gitignored `<repo>/tmp`, under pytest's
`pytest-of-<user>/pytest-<N>/`. A green run deletes the whole thing at the end;
a run with **any** failure keeps every failed test's data dir for post-mortem,
and one such run-generation survives into the next (a SIGKILLed run's for three
days). `make clean` reclaims them. The logs below sit in `<repo>/tmp` at a
stable path instead, so they outlive the run.

Server stderr of the shared session server: always written to
`<repo>/tmp/server_debug.log`. Pytest's `-s` flag does NOT capture this — it
lives on disk regardless of pytest's stdout/stderr capture mode.

A test that starts its own server (the `own_server` fixture) gets a private log
next to its data dir, `<data_dir>.log`, holding both stdout and stderr of every
boot that test ran.

Worker logs: workers write to `<data_dir>/worker_<N>.log`. The conftest copies
the session server's to `<repo>/tmp/last_worker_N.log` on session teardown.

Live tail (during a long-running test):
`tail -f tmp/pytest-of-*/pytest-*/*/data/worker_*.log`

Pre-existing logs from the previous session are overwritten on the next test
run, so save copies before re-running if you need them.

### Using tests for debugging

1. **Reproduce the failure on a single test** with `pytest <test-file>::<test> -v`.
   Always pass `GNITZ_WORKERS=4` — multi-worker bugs hide at W=1.
2. **Re-run a few times** to check determinism. Flaky failures often point to
   a race that the deterministic single-test run will mask.
3. **Read both logs side-by-side** — the master log shows what was
   dispatched; the worker log shows what was processed. Discrepancies in
   ordering between them are usually the smoking gun.
4. **Add temporary `gnitz_info!` lines** at the suspected boundary
   (handler entry, SAL emit, ACK reply). `make server pyext` then re-run.
   Strip them before committing.
5. **Use the debug binary** (the default `make server` output). Release
   builds clamp corrupt values silently and hide the real failure mode.
6. **Test logs survive the session**, code state does NOT — if you want
   to attach a log to a bug report, copy it out of `<repo>/tmp/`
   before the next test run overwrites it.

## Capacity-bounded views

```sql
CREATE VIEW recent WITH (capacity = '4 MB') AS SELECT id, body FROM messages WHERE kind = 3;
```

The view is maintained exactly as any other — **correctness is never a function
of what is resident**. Past the capacity a sweep rewrites parts of the view's
output store as **skeleton rows**: the PK and one coarse summed weight, no
payload. A read touching such a key recomputes it from the view's operator traces
(join) or source store (linear), which stay full-fidelity.

Capacity bounds the **registered on-disk shard bytes of that one store on one
worker**, victim-ordered by *write* recency — nothing records that a row was
read. Not the traces, not per-row residency, not cluster-wide, and not a bound on
read peak: a read over hydrated keys materializes them, so peak is higher than
the unbounded twin's, not lower. Eligible bodies are exactly two: filter/projection
over one relation, and plain inner equi-join. Bounded views are **leaf** views:
nothing may be created over one, and `ALTER VIEW … AS` cannot retarget one.

Read paths branch on whether a store *holds* a skeleton row, never on whether it
has a capacity — so a bounded view under its cap reads exactly like any other
relation, and the branch cannot disagree with what the sweep did.

This is the one deliberate local exception to the (PK, payload) element identity
of §1/§2: when a cursor holds a skeleton run, its merge comparators fold a whole
PK group to the skeleton row regardless of payload, because that row already
carries the key's summed weight. Every other cursor, and every compaction merge,
keeps (PK, payload).

**Fold totality** — every merge that can see a skeleton row folds a per-key
*time-prefix* of the store's history, cut at an ingest boundary — becomes a
precondition of storage correctness here: it plus base-table positivity is what
makes a skeleton row's single summed weight per key exact. A future *partial*
compaction that broke it would corrupt bounded views.

## Streams

```sql
CREATE TABLE events (id BIGINT PRIMARY KEY, kind BIGINT, amount BIGINT)
    WITH (stream = true);
CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM events GROUP BY kind;
-- push rows into `events`, or INSERT into it; read `hot`.
-- SELECT * FROM events  → error: a stream holds no rows.
```

A **stream** is a relation with a schema and a primary key that holds no rows.
Views over it are maintained exactly as views over a table are, but nothing it
ingests is ever *recovered*: pushed rows exist only as the deltas they produce, and
every view reaching a stream is reset and rebuilt at boot. The stream's
*definition* is ordinary durable catalog state. Target: log-structured ingestion,
where the raw log lives upstream and only the derived view state is worth keeping.

A stream push skips the pre-ACK `fdatasync` and the durable write. Removing the
sync is a **latency** win, so it concentrates at small batch sizes and amortizes
away as the batch grows. It accrues to *stream-only* commit batches — a stream
push the committer coalesces with a base-table push rides that batch's sync, so
those rows do reach the disk, and are discarded on replay.

**A stream is append-only.** Pushed weights must be `>= 1`; the engine rejects a
batch containing any weight `<= 0`, which is what keeps §1's positivity invariant
engine-enforced (nothing clamps a stream's weights the way `enforce_unique_pk`
clamps a base table's). CDC input carrying a before-image retraction is therefore
not expressible against a stream.

Its PK is a routing, sort and identity key and is **not unique**: duplicate PKs
with different payloads are legal, exactly as for intermediate batches inside a
circuit. So `INSERT INTO events VALUES (1, …)` twice yields one element at weight
2, where the same statement against a table raises a duplicate-key error.

`WITH (replicated = true)` and `CLUSTER BY` mean what they mean for a table.
Rejected: reading a stream anywhere outside a view body (SQL or wire), `UPDATE`,
`DELETE`, `CREATE INDEX`, `ALTER TABLE` column ops, `INSERT … ON CONFLICT`,
`SERIAL`, a `FOREIGN KEY` to or from one, a write inside a transaction, and
`WITH (capacity = …)` on a view over one. `DROP TABLE` and `ALTER TABLE … RENAME
TO` work.

At boot, a view reaching a stream **returns to the value it would have if the
stream had never received a row** — zero rows for `stream ⋈ table`, fully
null-filled for `table LEFT JOIN stream`, and *grown* for
`SELECT id FROM t EXCEPT SELECT id FROM s`. Non-monotone, and correct.

Backpressure: pushed batches sit in RAM until a tick drains them, bounded by the
tick trigger; the client is bounded by waiting on the push ACK. A stream's rows
occupy the SAL exactly as a table's do, so it is reclaimed by the same byte-based
checkpoint test and at the same rate — except inside a DDL window, where no
checkpoint runs, and where a stream is the most likely thing to reach the ceiling.

Every read of a stream-fed view drains pending ticks: freshness is measured
against the published tick, which a stream push never advances. A cost, not a
correctness defect — when nothing is pending the drain takes the empty fast path.

## Delta feeds

`CREATE VIEW … WITH (delta = '32 MB')` retains a view's recent deltas, read back
through `ReadBound::Delta { after_tick }` as ordinary batches — **weights and
all**, so a row-set comparison of a feed tests nothing. No subscription verb and
no server-side cursor registry: the engine stays request/response, the cursor
lives on the client, nothing retained survives a restart, `capacity` is refused
with it, and `ALTER VIEW … AS` cannot retarget a fed view.

**A delta read is the one read verb that does not drain pending ticks** — the
exception to the paragraph above. It answers "what has happened", not "what is
current", so a push the tick loop has not run yet is a round the next poll
carries.

**A cursor can expire, and every subscriber must handle it.** The budget bounds
**registered on-disk shard bytes**, exactly as `capacity` does, and the sweep
drops the oldest rounds whether or not anyone is still reading them; the RAM tier
beneath it is bounded separately. A cursor at or below what a worker has dropped
is refused, and so is one whose tag names a different boot or a different
relation. The recovery is always the same and is not optional: discard the copy
and bootstrap. A `CREATE VIEW` backfill never enters the delta store, which is
what makes a foreign cursor unsafe rather than merely stale.

The feed is per-worker; a replicated view's feed lives on worker 0 alone.

A **mirror** (`gnitz-mirror`) is a local copy of a view fed by that feed, read in
the host's process at its last polled round: not read-your-own-writes, and two
mirrors are no consistent cut; an unheld relation is delegated upstream. One live
handle per process, one process per data directory; it poisons on an ingest error.

A view whose STRING/BLOB rows on one worker exceed the reply frame cap **cannot
be mirrored**: such a reply goes out as one frame, and a bootstrap reads the view
whole, so it has no projection, predicate or `LIMIT` to narrow and its one
recovery — bootstrap again — fails identically. The ceiling is the server's, not
the mirror's: an unprojected read of that view fails for any client.

## SAL durability contract

**Rule: an ACK to a client implies fdatasync iff the operation wrote something a
restart must recover.**

The SAL (Shared Append-Only Log) carries both data writes and ephemeral
commands on the same mmap'd fd. Workers see all SAL entries immediately
via Acquire/Release atomics on the size prefix — fdatasync is irrelevant
for cross-process visibility. It exists solely for crash recovery.

Durable operations are atomic: crash recovery applies an operation in full
or not at all, so a crash never leaves a half-written DDL or push behind.
They fdatasync before the ACK, publish their zone LSN, and reply with it.
Everything else wakes the workers without a sync and replies LSN `0`: the
command-only operations — view ticks, scans, seeks, backfills, validation
queries — plus a push to a **stream**, which upserts nothing that survives a
restart. The dichotomy is what must be recovered, not what carries rows.

The **checkpoint** is the sole shard-durability point: between checkpoints no
table publishes a manifest on the ingest path, so the fsynced SAL alone carries
durability and every table's overflow lives in the RAM tier. A checkpoint
persists the base and system tables, then, after draining pending view ticks,
every view's operator traces and output stores under a monotonic checkpoint
generation. It publishes **unconditionally**, even for an empty or unchanged
store: the resume verdict and the boot relayout each decide by "every child
carries a manifest", which a gated publish would make undecidable.

At open a view is **resumed from its checkpoint when generation-valid, rebuilt
otherwise; secondary indexes are always rebuilt.** Resume is *incremental*: the
checkpointed output store and operator traces load from their shards, and only
the un-checkpointed SAL tail is fed through the circuit — the view is never
re-derived from the base. A view resumes iff the recorded topology matches the
launched `(worker_count, STATE_FORMAT)`, every one of its output children is at
the committed generation, and every view it scans is itself valid; else it is
reset and rebuilt from base. Recovery is **non-windowed**: the un-checkpointed
tail is replayed once on a freshly-reset SAL, so peak recovery RAM is
~(tail)/W per worker. The checkpoint generation is durably advanced before the
fork, which closes the reset→boot-checkpoint crash window: a crash there leaves
the durable generation ahead of every un-checkpointed view, forcing a rebuild
rather than a silently-stale resume.

A restart at a **different worker count** relays each base relation's children
onto the launched count before any store opens — writing the new set beside the
old and removing the source only once every target is durable, so a crash at any
point leaves the older set intact and the next boot redoes the work.

## Benchmarking

```bash
make bench                          # quick mode, 1 worker
make bench-full                     # full mode, 4 workers
make bench WORKERS=4 PERF=1         # knobs: WORKERS, CLIENTS, FULL=1, PERF=1
```

`make help` lists the rest (feature/transaction tiers, worker×client sweeps,
`perf` and DWARF profiling variants, native-CPU builds).

Results: `benchmarks/results/` (gitignored). The runner rotates old results (keeps 10).
Generate report tables with `benchmarks/report.py`.

Workflow: `make bench` → change → commit → `make bench` → compare `summary.json`.

### Rust micro-benchmarks

`make bench` is end-to-end (full server + IPC), so it can't isolate a tight
in-process loop. For that, the engine carries `#[ignore]`d timing tests that
print throughput and must be run in `--release`:

```bash
cd crates && cargo test -p gnitz-engine --release <name>_bench \
    -- --ignored --nocapture --test-threads=1
# reactor/IPC benchmarks live in the server crate: -p gnitz-server
```

Add one alongside the others: name the test `*_bench`, mark it `#[ignore]`,
time only the hot region with `std::time::Instant`, and `std::hint::black_box`
anything the optimizer could elide.

## Debugging failures

1. **Confirm the suspected path actually runs before debugging why it
   misbehaves.** A plausible plan or prior model can frame the whole hunt around
   code that never executes; when hypotheses keep diverging or contradicting a
   confirmed symptom, suspect the framing, not the bug's subtlety.
2. **A passing check only rules out the failure it was built to detect.** When
   concluding "correct" / "no bug," verify the property the domain *defines* as
   correctness — in a Z-set engine that is weights, not row presence — and ask what
   a *different kind* of failure would look like; a clean result on the wrong
   observable closes the case silently. A flagged-but-unverified "probably fine" is
   unverified — run the cheap disconfirming test instead of narrating the doubt.
3. **Confirm the size before optimizing the cost.** A path that is slow only at an input
   size nothing can produce is not slow. Bound the reachable input from its producer first.
4. **Log first, never guess.** Rebuilds are expensive. One well-instrumented
   run reveals more than ten speculative attempts.
   - `gnitz_debug!` / `gnitz_info!` for structured logging
   - `GNITZ_LOG_LEVEL=debug` to enable debug-level messages
   - `RUST_BACKTRACE=1` for panic backtraces
5. **Use the debug binary for crashes.** Release builds silently clamp
   corrupt values.
6. **Isolate with 1 worker first.** Pass with W=1 but fail with W=4 →
   bug is in exchange/fanout, not computation.
7. **Bisect by sub-path.** Disable the new fast-path to confirm the bug
   is in the new code.
8. **Verify your fix is in the binary.** `make e2e` rebuilds both the
   server and the Python extension before running tests.

## GIT Branches

All development happens on main for now; never branch off.

## Compat / Legacy code

Gnitz is pre-alpha, there are no production uses of this software. Keeping legacy code in
the codebase is strictly not allowed.
