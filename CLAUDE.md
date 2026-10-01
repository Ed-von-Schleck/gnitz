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
| A PK is unique | Unique only in the accumulated state of a base table or a reduce output; batches routinely repeat a PK | §1 |
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

**Weights.** Accumulated base-table weights are always ≥ 0, enforced by the
engine on ingest: whatever a client pushes, the effective delta of an INSERT is
+1, of a DELETE a retraction, of an UPDATE a retraction plus an insert.
Intermediate circuit nodes may produce negative weights; base-table traces never
accumulate one. Load-bearing: DBSP Propositions 4.4/4.5 (the
distinct-elimination rules) require positive inputs.

**Indexed Z-Sets** map keys to Z-Sets, `K → (V → ℤ)`. GROUP BY produces one via a
grouping function that is **linear**, so GROUP BY itself needs no state and is
incrementally free; only the aggregation within each group needs the integral.

**PK uniqueness is not a general invariant.** The PK region is a sort/routing
key. It is unique in the accumulated state of a base table (enforced on ingest)
and of a reduce output (one row per group) — a *delta* of either can still carry
a retraction and an insert under one PK. It is **not** unique for intermediate
batches: a join re-keys its inputs on the join key, which many rows share. Every
operator uses full (PK, payload) identity; none may assume PK uniqueness.

**PK columns** are an ordered list of fixed-width integer-stored scalars — the
integer types, signed or unsigned, and the types stored as one, such as UUID or
DECIMAL — non-nullable. STRING, BLOB and float columns cannot be PK columns —
IEEE-754 breaks the byte-equal key contract, since ±0.0 differ byte-wise but
compare equal numerically, and NaN has no canonical bit pattern.

**A PK is an opaque, ordered byte string wherever it flows.** It is stored as an
order-preserving key (OPK, §4) whose unsigned byte comparison *is* the typed
lexicographic PK order — so every ordered operation is one byte comparison, and
the encode/decode boundary is the only type-aware code. The encoding is a
bijection, so byte-equal ⟺ key-equal, and routing hashes those same bytes: the
hash is a **pure function of the logical key**.

**Hidden key slots.** Synthetic view keys and unprojected passthrough PK columns
are real schema columns flagged hidden: invisible to name resolution, wildcard
expansion and client rows, and ordinary columns to the PK region, routing, sort
and consolidation.

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
unsigned byte comparison over the OPK region, then payload columns in schema
order, each by its own type's order. The order is total for every input: null
sorts before non-null, and floats compare by a total order, so NaN has a defined
position.

> **Every merge and consolidation path must sort by (PK, payload), never by PK
> alone.** PK-only ordering interleaves rows that share a PK but differ in
> payload, so weights accumulate against the wrong element.

> **Every batch entering an N-way merge must already be sorted by (PK, payload).**
> A merge reads each input linearly; an unsorted input makes the heap deliver
> duplicates non-adjacently and the accumulator produces *wrong weights
> silently* — a release build checks nothing; only debug and checked builds
> verify a batch's layout.

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
bilinear cross-delta term `dA ⋈ dB` always zero.

### Linear operators

`L(A + B) = L(A) + L(B)`: filter, map, negate, UNION ALL, delay (z⁻¹).
Incrementalization adds no state (Theorem 3.3) — delay holds one tick by
definition, but nothing *extra*. Consolidation before a linear operator is
optional. Delay is the paper's operator; no GnitzDB operator implements it.

### Bilinear operators

Join is bilinear: output weight = product of input weights. Both operands need
their integral, and consolidation is required.

Thm 3.4 (*Bilinear*) takes the two **change** streams `dA`, `dB` as its inputs;
`I(dA)` and `I(dB)` are the relations they accumulate.

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
epoch, the same source feeding both inputs, never reaches a circuit: the planner
turns a self-join into the shared-source shape above (the discriminator is
source-id equality, not base-table overlap).

**Join output schema** is `[key, left_payload..., right_payload...]` over the
**SQL sides**, not the delta/trace ports — so both terms of the symmetric form
share one schema. For a keyed join the key is the shared join key; the other
input's PK region is not duplicated.

**Keyless (cross) joins** are INNER only. The product is computed
partition-locally under a broadcast of the delta — no exchange co-locates the
two operands — so every epoch emits `|Δ| × |other side|` rows.

### Outer joins (LEFT / RIGHT / FULL)

Described for LEFT; RIGHT is the mirror, FULL does both sides — for the join
shapes that support them; the planner rejects the rest. For each delta
row: if inner matches exist, emit them at `w_delta × w_trace`; if none, emit one
null-filled row at `w_delta`. This is **not bilinear** — output weight depends on
match *existence*, not on weight arithmetic alone.

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
  replay, and consolidating the input delta is skippable for a reduce of only
  these, where each is exact — which a float SUM is not.
- **MIN and MAX are not.** Retracting the current extremum needs the next value
  from history, so they carry a secondary value index instead of scanning the trace.
- **Float SUM/AVG is order-dependent** — addition is non-associative, so the value
  is a function of (query, data, worker count, access path, chunk size) and
  reproduces only while all of those hold. Use an integer type where exactness
  matters.

*Set operations* (UNION/INTERSECT/EXCEPT, DISTINCT and ALL) are **join-free**:
each is a linear combination of `{union, negate}` plus the weight-clamp primitive
(`distinct = clamp[0,1]`, `positive_part = clamp[0,i64::MAX]`) over
content-hashed leaves — EXCEPT DISTINCT = `positive_part(distinct(A) −
B)`, INTERSECT DISTINCT = `distinct(A) − positive_part(distinct(A) − B)`. There is **no anti-join operator**: these, the band outer-join
null-fills, and band EXISTS/IN are the only `positive_part` users.

### The integral (trace)

`I(A)_t = Σ(dA_0..dA_t)`. Each delta is added to a store; an operator reads it
back through a cursor that merges the in-memory and on-disk tiers on the fly,
summing weights of matching (PK, payload) entries and dropping ghosts — so every
tier depends on §2's sort invariant. An operator reading a trace sees
`z⁻¹(I(X))`, the integral *before* the current delta.

## 4. The Region Convention

Column buffers are flat (pointer, size) pairs in canonical order — the layout of
a batch in memory and in a WAL block. A shard file keeps the same regions in the
same order but may encode each one compactly, so the sizes below are a batch's.
`pk_stride` is the encoded key width: the sum of
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
*every* PK position, so that closed form does not hold — read the mapping from
the schema, never `ci - 1`.

---

# GnitzDB Developer Guide

## Prerequisites

Linux with an io_uring kernel (≥ 6.7: `IORING_OP_FUTEX_WAITV`); stable Rust; `uv` (drives Python + `maturin`). Nothing is version-pinned.

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
- **`make e2e-checked`** runs the suite against release codegen with the Z-set
  layout verifiers left in. Run it when the unsafe batch kernels changed or a
  bug is release-only — release compiles the layout verifiers out.

The Python extension that ships is the one maturin installs at
`crates/gnitz-py/python/gnitz/_native*.so`. `crates/target/*/libgnitz.so` is
cargo's own output, linked by nothing and arbitrarily stale — never measure it.

## Project structure

Two sides that meet at the wire protocol: the **SQL/client side** (a library,
also exposed as Python bindings) plans and drives queries, and ships them
over `gnitz-wire`; the **engine** executes them as a multi-process server and
never links the planner. `gnitz-mirror` is the sole crate on both sides, and
links no planner either; `gnitz-py` links it, so the Python extension carries the
Z-set kernel and store too — but not the DBSP layer.

Two contracts decide what goes where. **The row kernels are `gnitz-zset`'s** —
every operator, merge, sort, encoder and cursor — and the crates above call them
once per batch, per run or per probed key. A loop over rows above the kernel
decides policy and reads rows through the kernel's accessors; it never computes a
Z-set. And **a client links no compiler, no VM and no catalog** — a fact of the
crate graph rather than a convention: a host holding a mirrored view links
`gnitz-zset` and `gnitz-store`, and `gnitz-server` is a binary nothing can link.

### Workspace crates

| Crate | Role |
|-------|------|
| `gnitz-foundation` | The process and the OS under it: logging, `GNITZ_*` env overrides, fault-injection seams, host RAM and POSIX file-I/O — independent leaves every other crate may name |
| `gnitz-wire` | Wire-protocol constants + codecs and the circuit graph — the one definition client and engine must agree on. Also the one owner of XXH3, since a client computes some of the same digests |
| `gnitz-expr` | The one expression evaluator, and the resolved column addressing it reads through |
| `gnitz-core` | Client core: connection, protocol, the client schema and batch, and the mirror state machine |
| `gnitz-sql` | SQL front end: parser, binder, query planner |
| `gnitz-tokio` | The Rust async client: a `Connection` future over tokio's reactor, and the `AsyncClient` handle |
| `gnitz-py` | Python extension (pyo3) — the driver + planner the test/benchmark suites run against |
| `gnitz-zset` | The Z-set kernel: the schema, columnar batches and the shard image, the cursor over runs, and the operators |
| `gnitz-store` | The Z-set store: the LSM, the relation registry, the `ReadSpec` executor |
| `gnitz-server` | The multi-process server binary: the DBSP layer — circuit compiler, bytecode VM, epoch execution, system-table catalog — under the `runtime` rung that drives it |
| `gnitz-mirror` | The mirror store: the local copy a client reads through, and the one implementor of `gnitz-core`'s `MirrorStore` — the one crate on both sides. Drives `gnitz-store` directly and links no DBSP layer |
| `gnitz-zset-testkit` | Dev-only: `gnitz-zset`'s test helpers, compiled as a library so other crates' tests reach them |
| `gnitz-test-harness` | Spawns a `gnitz-server` subprocess in a private tmpdir for integration tests |

### The engine (`gnitz-zset` + `gnitz-store` + `gnitz-server`)

Strictly layered — every module depends only on those beneath it, and the two
crate seams are rungs of the same ladder:

```
runtime        → catalog, query, and both crates below
catalog        → query, and both crates below
query          → both crates below
  ── the crate seam: everything above is `gnitz-server` ──
read           → relation, storage, and the kernel
relation       → storage, and the kernel
storage        → the kernel
  ── the crate seam: everything above is `gnitz-store`, below is `gnitz-zset` ──
stream         → algebra, repr, schema
algebra        → repr, schema
repr           → schema
schema           — the bottom rung; names none of the others
```

The two operator rungs split on one question: **does the operator read a trace?**
`algebra` holds the functions of one Z-set, which read no trace: a read and a
circuit run the same kernels there. `stream` holds the operators that take this tick's delta together with a cursor
over its history; only a circuit dispatches to them, and nothing in `gnitz-store`
names one.

Each crate is one compilation unit, so the ladder among its own rungs is pinned
by a source-text test rather than by the crate graph. `catalog` and `query`
cannot fail-stop the process — the abort is private to `runtime` — so every
fallible path in them returns its error.

A subsystem's surface and internal split are documented in its `mod.rs` header,
not here.

`gnitz-foundation`, `gnitz-wire` and `gnitz-expr` sit **below this whole
table**: they are separate crates.

A relation is stored once per worker. A relation's id exceeds the id of every
relation it scans, so ascending id order is dependency order.

Test helpers live in each engine crate's `test_support`. A helper stays
crate-internal unless another crate's tests need it; only then does it go in the
`shared` part of `gnitz-zset`'s, which is compiled again as `gnitz-zset-testkit`
and so turns whatever it touches into published API.

A module's unit tests live in `<dir>/tests/<module>.rs`, attached back with
`#[cfg(test)] #[path]` so they keep private access; `<dir>/mod.rs`'s own go to
`<dir>/tests/<dir-name>.rs`. Tests no single module owns live in the subsystem's
`suites/`, which reaches only that subsystem's surface.

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
| `GNITZ_SERVER_BIN` | Override server binary (e.g. aim E2E at the release build) |
| `GNITZ_RAM_TIER_BYTES` | Per-store RAM-tier ceiling before it spills to a shard; shrink it to reach the disk regime on small data |

## Debug logging

```bash
make e2e-debug K='<expr>'      # rebuilds first, then runs with GNITZ_LOG_LEVEL=debug at W=4
```

The suite refuses to start on a server binary or extension older than the newest
Rust source, so a bare `uv run pytest` cannot silently test the previous build.
`GNITZ_ALLOW_STALE_BIN=1` overrides it.

Log format: `<epoch>.<ms> <tag> <level> <message>` — tags: `M` (master), `W0`–`WN`.

For temporary logging: `gnitz_debug!` / `gnitz_info!` macros. Remove before committing.

### Where test logs go

Everything is under the gitignored `<repo>/tmp` — copy out what you need before
re-running.

- **Session server stderr:** `tmp/server_debug.log`, on disk regardless of
  pytest's capture mode (`-s` does not show it).
- **A test's own server** (the `own_server` fixture): `<data_dir>.log`, stdout
  and stderr of every boot that test ran.
- **Workers:** `<data_dir>/worker_<N>.log`; the session server's are copied to
  `tmp/last_worker_N.log` at teardown.
- **Data dirs:** `tmp/pytest-of-<user>/pytest-<N>/`. A green run deletes them; a
  failed test's is kept for post-mortem. `make clean` reclaims them.

Live tail: `tail -f tmp/pytest-of-*/pytest-*/*/data/worker_*.log`

## Capacity-bounded views

```sql
CREATE VIEW recent WITH (capacity = '4 MB') AS SELECT id, body FROM messages WHERE kind = 3;
```

The view is maintained exactly as any other — **correctness is never a function
of what is resident**. Past the capacity a sweep rewrites parts of the view's
output store as **skeleton rows**: the PK and one coarse summed weight, no
payload. A read touching such a key recomputes it from the view's operator traces
(join) or source store (linear), which stay full-fidelity.

Capacity bounds one store's shard bytes on one worker — not the traces, not
the cluster, and not read peak. Bounded views are **leaf** views: nothing may be
created over one.

Read paths branch on whether a store *holds* a skeleton row, never on whether it
has a capacity.

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

A stream push needs no pre-ACK `fdatasync` and makes no durable write.

**A stream is append-only.** Pushed weights must be `>= 1`; the engine rejects a
batch containing any weight `<= 0`, which is what keeps §1's positivity invariant
engine-enforced. CDC input carrying a before-image retraction is therefore not
expressible against a stream.

Its PK is a routing, sort and identity key and is **not unique**: duplicate PKs
with different payloads are legal, exactly as for intermediate batches inside a
circuit. So `INSERT INTO events VALUES (1, …)` twice yields one element at weight
2, where the same statement against a table raises a duplicate-key error.

A stream is readable only inside a view body, and takes no `UPDATE`, `DELETE`,
index, foreign key or transactional write.

At boot, a view reaching a stream **returns to the value it would have if the
stream had never received a row** — zero rows for `stream ⋈ table`, fully
null-filled for `table LEFT JOIN stream`, and *grown* for
`SELECT id FROM t EXCEPT SELECT id FROM s`. Non-monotone, and correct.

A read of a view drains pending ticks when a push it reaches has not been
ticked yet — a stream push exactly as a table's.

## Window functions

A window call (`f(…) OVER (…)`, `QUALIFY`, `WINDOW`) desugars in the planner into
operators that exist without it, and is legal only in a view body. Partition and
order
keys are join keys and carry the join-key rules; `ROW_NUMBER` is `RANK` over the
ORDER BY extended by the input's row key, so it needs one.

## Delta feeds

`CREATE VIEW … WITH (delta = '32 MB')` retains a view's recent deltas, read back
by polling from a client-held cursor as ordinary batches — **weights and all**,
so a row-set comparison of a feed tests nothing. No subscription verb and no
server-side cursor registry: the engine stays request/response, the cursor lives
on the client, and nothing retained survives a restart.

**A delta read is the one read verb that does not drain pending ticks.** It
answers "what has happened", not "what is current", so a push the tick loop has
not run yet is a round the next poll carries.

**A cursor can expire, and every subscriber must handle it.** The budget is a
byte bound, and the oldest rounds are dropped whether or not anyone is still
reading them. An expired cursor is refused, and so is one from a different boot
or a different relation. The recovery is always the same and is not optional:
discard the copy and bootstrap.

A **mirror** is a local copy of a view fed by that feed, held by a client and
read in the host's process at its last polled round: not read-your-own-writes,
and two mirrors are no consistent cut; a relation the copy does not hold is
delegated upstream. The copy lives in a store (`gnitz-mirror`) the host opens and
attaches to a client. An ingest error costs the one copy it hit, which bootstraps
again; a store whose copy may be torn refuses the reads it would have answered
until it is closed.

**A poll reports, per view, whether it reseeded** — discarded the copy and read
the view whole. That is a discontinuity every subscriber has to react to, and no
cursor carries it: an expiry-driven reseed inside one boot keeps the tag and moves
the tick forward, which is exactly what an ordinary advance looks like.

## SAL durability contract

**Rule: an operation that wrote something a restart must recover is ACKed only
after its fdatasync; nothing else needs one.**

The SAL (Shared Append-Only Log) carries both data writes and ephemeral
commands. Workers see a SAL entry as soon as it is published — fdatasync is
irrelevant for cross-process visibility. It exists solely for crash recovery.

Durable operations are atomic: crash recovery applies an operation in full
or not at all, so a crash never leaves a half-written DDL or push behind.
They fdatasync before the ACK. Everything else needs no sync: the command-only
operations — view ticks, scans, seeks, backfills, validation queries — plus a
push to a **stream**, which upserts nothing that survives a restart. The
dichotomy is what must be recovered, not what carries rows.

The **checkpoint** is the sole shard-durability point: between checkpoints the
fsynced SAL alone carries durability. A checkpoint persists the base and system
tables, then, after draining pending view ticks, every view's operator traces
and output stores.

At open a view is **resumed from its checkpoint when that checkpoint is valid,
rebuilt from base otherwise**. Resume is *incremental*: only the un-checkpointed
SAL tail is fed through the circuit — the view is never re-derived from the
base. A view is valid only if every view it scans is, and a crash at any point
must force a rebuild rather than a silently-stale resume.

A restart at a **different worker count** relays each base relation's rows onto
the launched count before any worker opens a store. The old set is removed only
once the new one is durable, so a crash at any point leaves one complete set.

## Benchmarking

```bash
make bench                          # quick mode, 1 worker
make bench-full                     # full mode, 4 workers
make bench WORKERS=4 PERF=1         # knobs: WORKERS, CLIENTS, FULL=1, PERF=1
```

`make help` lists the rest. Results land in the gitignored `benchmarks/results/`;
`benchmarks/report.py` turns them into report tables.

Workflow: `make bench` → change → commit → `make bench` → compare `summary.json`.

### Rust micro-benchmarks

`make bench` is end-to-end (full server + IPC), so it can't isolate a tight
in-process loop. For that, the engine carries `#[ignore]`d benchmark tests that
print what they measured and must be run in `--release`.

`make bench-rust` runs the set, `make bench-rust T=<name>` one of them. It builds
the whole workspace in release, so iterate on a single crate with cargo directly:

```bash
cd crates && cargo test -p gnitz-zset --release <name>_bench \
    -- --ignored --nocapture --test-threads=1
# LSM, registry and read-executor benchmarks: -p gnitz-store; catalog/compiler and reactor/IPC: -p gnitz-server
```

Add one alongside the others: name the test `*_bench`, mark it `#[ignore]`,
measure only the hot region — instructions retired where the question is cost,
wall clock where it is latency — and `std::hint::black_box` anything the
optimizer could elide.

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
   run reveals more than ten speculative attempts. Add temporary `gnitz_debug!` /
   `gnitz_info!` lines at the suspected boundary, run with
   `GNITZ_LOG_LEVEL=debug` and `RUST_BACKTRACE=1`, and strip them before
   committing.
5. **Reproduce on a single test** with `pytest <test-file>::<test> -v` at
   `GNITZ_WORKERS=4`, and re-run it a few times: a flaky failure often points to
   a race that one deterministic run masks.
6. **Read the master and worker logs side by side** — the master log shows what
   was dispatched, the worker log what was processed. Discrepancies in ordering
   between them are usually the smoking gun.
7. **Use the debug binary.** Release compiles out the debug assertions and the
   layout verifiers, so a corrupt batch flows on silently and the real failure
   mode is hidden.
8. **Isolate with 1 worker.** Pass with W=1 but fail with W=4 → bug is in
   exchange/fanout, not computation.
9. **Bisect by sub-path.** Disable the new fast-path to confirm the bug is in
   the new code.
10. **Verify your fix is in the binary.** `make e2e` rebuilds both the server
    and the Python extension before running tests.

## GIT Branches

All development happens on main for now; never branch off.
