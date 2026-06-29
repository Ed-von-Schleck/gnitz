Never check if failures are pre-existing; as a rule, all tests are green at the start of a session.

Never check or confirm that compilation warnings or Clippy errors/warnings are caused by the current session, always directly fix them.

Use `semble search` (the semble MCP server is installed) to find code by describing what it does or naming a symbol/identifier instead of grep. There are other useful semble commands as well. Prefer it over grep + read every time. Use `semble clear all` to clear a stale index.

gnitz is **pre-alpha** and not used in production anywhere because it has not been released, so there are **never** any compatibility concerns and there should **never** be legacy code remaining.

**Plans (`plans/`):**
- Never cite a plan path/filename in code, tests, or comments.
- Not ground truth: validate against source/tests/git; only edit the plan named for the task.
- Authoring: state one committed design — no history, no optional/either-or, no "follow-on/future-work" (fold in, spin out a new plan, or drop); include validated snippets; never cross-link plans (state needed facts inline); out-of-scope bugs get their own plan. When a plan is written, all decisions need to be made - no "gates" in plans are allowed.
- Don't write memories about a specific plan (they're transient).

GnitzDB-specific development guidelines are in the **GnitzDB Developer Guide**
section at the end of this file.

# Over-arching Design Rules

The fundamental mottos are "ZSets all the way down" and "ZSets over the wire". Never go against these ideas.

---

# GnitzDB Theoretical Foundations

Every storage operation, DBSP operator, and merge path derives from these
definitions. Bugs often trace to violating an invariant stated here.

## 1. Z-Sets

A **Z-Set** over domain *D* is a function *Z: D → ℤ* mapping each element
to an integer **weight** (multiplicity), with finite support. Z-Sets form
an **abelian group** `(ℤ[D], +, 0, -)` under pointwise addition — this
group structure is the prerequisite for the entire DBSP theory (I, D, and
all incrementalization theorems depend on it).

An **indexed Z-Set** `ℤ[V][K]` maps keys to Z-Sets: `K → (V → ℤ)`.
GROUP BY produces indexed Z-Sets via the grouping function `G_p`, which
is **linear** — GROUP BY itself needs no state and is incrementally free.
Only the aggregation within each group requires the integral. In GnitzDB,
`map_reindex` (repartitioning by group/join key) is the physical
implementation of `G_p`.

In GnitzDB, *D* is the set of rows for a table schema. A row is identified
by **(PK, payload)** — the primary key plus all non-PK column values.

> **Element identity = (PK, all payload columns).**
> The PK alone does NOT identify an element. Only (PK, payload) does.

**Positivity of base tables.** Accumulated base table weights are always
≥ 0, enforced by the DML layer (INSERT = +1, DELETE = retraction,
UPDATE = retract + insert). Intermediate circuit nodes may produce
negative weights, but base table traces never accumulate negative net
weights. Load-bearing: `distinct` assumes this, and DBSP Propositions
4.5/4.6 (distinct-elimination) require positive inputs.

**Compound PKs.** A table's primary key is an ordered list of one or more
columns. Each PK column is a fixed-width integer scalar — unsigned or
signed; STRING, BLOB, and float columns cannot be PK columns, and PK
columns are non-nullable. Floats are excluded because IEEE-754 breaks the
byte-equal key contract: -0.0 and +0.0 compare unequal byte-wise but equal
numerically, and NaN has no single canonical bit pattern. The PK columns
are packed in PK-list order and stored order-preserving big-endian (OPK, §6);
the concatenation is the key.

**PK order is typed; comparison is not.** A primary key's order is the typed
lexicographic order — unsigned columns by magnitude, signed columns in signed
order, compound keys column-by-column in PK-list order. That order is carried by
the bytes themselves: a PK is stored as an order-preserving key (OPK, §6) whose
plain unsigned byte comparison *is* the typed order, at any width. So a PK is an
**opaque, ordered byte string wherever it flows** — every ordered operation
(merge, sort, consolidation, range scan) is one byte comparison
(`compare_pk_bytes`), and the encode/decode boundary is the only type-aware
code. The encoding is also a bijection (§6), so equal keys are byte-equal and
consolidation and dedup group on the raw bytes.

**Hash invariant.** Partition routing and XOR8 filter probes hash a PK by its
OPK bytes, making the hash a **pure function of the logical key**: every
producer and consumer agrees, equal keys co-partition and co-probe, and physical
width or padding at the wire boundary never changes where a key lands.

**Physical representation.** The in-engine batch is region-based (§6): a flat
buffer of contiguous columnar regions — PK (OPK bytes, `pk_stride`/row),
weight, null word, then payload columns — not separate per-row fields. Multiple
rows may share the same PK if their payloads differ.

**PK uniqueness is not a general invariant.** The PK region is a
sort/routing key. It is unique for base table batches (DML-enforced — every
SQL-created table is registered `unique_pk`) and reduce output
(one row per group). It is NOT unique for
intermediate batches: `map_reindex` overwrites the PK region with a
join/group column value, and join output inherits the left input's PK.
Multiple output rows may share the same PK with different payloads.
All operators use full (PK, payload) identity — none assume PK
uniqueness for intermediate results.

## 2. Z-Set Operations

**Addition:** `(A + B)(x) = A(x) + B(x)`. Implemented by batch
concatenation followed by consolidation.

**Negation:** `(-A)(x) = -A(x)`. Implemented by `op_negate`.

**Consolidation:** Groups duplicate (PK, payload) entries, sums weights,
drops elements with net weight zero ("ghost elimination").

> **In GnitzDB's physical representation, consolidation is the only
> operation that merges duplicate rows and sums weights.** (In Z-Set
> algebra, filter/projection/join also change element count — but those
> operate on the mathematical Z-Set, not on unconsolidated batches.)

**GnitzDB's consolidation uses a total order** (implementation choice —
hash grouping is theoretically valid, but sort-merge enables N-way
merging, range scans, and compaction). Sort key: PK first by unsigned
byte comparison over the OPK region (`compare_pk_bytes`; §1, §6), then
payload columns in schema order (`compare_rows`).

> **Invariant: all merge/consolidation paths must sort by (PK, payload),
> not just PK.** PK-only ordering interleaves rows with matching PKs but
> different payloads, causing silent weight-accumulation errors.

## 3. DBSP: Incremental Computation on Z-Sets

GnitzDB implements the DBSP model (Budiu et al., VLDB 2023). Instead of
recomputing queries from scratch, it processes only the **delta** and
produces the **output delta**.

**Lifting.** Every Z-Set operator (filter, join, distinct, ...) is first
a scalar function on Z-Sets, then **lifted** to a stream operator:
`(↑f)(s)[t] = f(s[t])` (DBSP Definition 2.3). Lifting distributes over
composition: `↑(f ∘ g) = ↑f ∘ ↑g`. The incrementalized version is then
`Q^Δ = D ∘ ↑Q ∘ I`.

**D and I are inverses** (DBSP Theorem 2.20): `D(I(s)) = s` and
`I(D(s)) = s`. This identity enables the **chain rule**:
`(Q₁ ∘ Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ` — each sub-operator can be
incrementalized independently. This is why GnitzDB can compile circuits
in incremental form without wrapping the entire query in D/I.

### Streams and Deltas

A **stream** is a sequence of Z-Sets indexed by time (tick). Each tick:
input delta *dA* arrives, circuit produces output delta *dO*.

### Execution Model

**Single-source-per-epoch.** `execute_epoch` takes one `input_delta` and
one `source_id` per call — a circuit never sees simultaneous deltas from
multiple sources. This makes the bilinear cross-delta term `dA ⋈ dB`
always zero and keeps trace cursors (snapshotted at tick start) valid
throughout.

**No explicit D operator.** The DBSP paper defines `Q^Δ = D ∘ Q ∘ I`.
GnitzDB omits D: circuits are compiled in incremental form. Theorem 3.3
(`Q^Δ = Q` for LTI Q) means linear operators apply directly to deltas.
Only non-linear and bilinear operators need the integral. The output of
`execute_epoch` is already a delta.

### Operators

**Linear operators** satisfy `L(A + B) = L(A) + L(B)`:
- Filter, Map, Negate, Union All (Z-Set addition), Delay (z⁻¹).
- No additional state for incrementalization (Theorem 3.3: `Q^Δ = Q`
  for LTI Q). Delay holds one tick of state by definition, but
  incrementalization adds no *extra* state.
- Consolidation before a linear operator is optional.

**Bilinear operators** — linear separately in each argument:
- Join is bilinear: output weight = product of input weights.
- DBSP Theorem 3.4 (three terms):
  `d(A ⋈ B) = dA ⋈ dB + z⁻¹(I(A)) ⋈ dB + dA ⋈ z⁻¹(I(B))`
  Collapsed 2-term form: `I(A) ⋈ dB + dA ⋈ z⁻¹(I(B))` — asymmetric
  (A = new state incl. dA, B = old state excl. dB) absorbs `dA ⋈ dB`.
- **GnitzDB's symmetric 2-term form:**
  `d(A ⋈ B) = dA ⋈ z⁻¹(I(B)) + dB ⋈ z⁻¹(I(A))`
  Both sides use old-state cursors. Correct under single-source-per-epoch
  (dA ⋈ dB = 0).
- **Shared-source branches** (`t ⋈ view-over-t`) do not break this: one
  user push reaches the two join inputs in two *separate* epochs
  (`evaluate_dag` queues work per `(view, source_id)` edge), and trace
  cursors are rebuilt at each epoch start. The later epoch joins its
  delta against a trace that already absorbed the earlier epoch's delta,
  so the cross-term `dA ⋈ dB` is emitted exactly once — the asymmetric
  2-term form, realized across epochs. The only shape that would place
  two simultaneous deltas in one epoch — the same `source_id` feeding
  both join inputs — is rejected by the planner (self-join and
  same-relation INTERSECT/EXCEPT guards; the discriminator is source-id
  equality, not base-table overlap).
- Require integral of both operands. Consolidation required.
- **Join output schema:** `[left_PK, left_payload..., right_payload...]`.
  Output PK = left input PK (= join key after exchange repartition).
  The right batch's PK (also = join key) is not duplicated in the
  output. Original table PKs are preserved in payload columns (moved
  there by `map_reindex`). No information is lost.

**Left outer join** — extends bilinear join with null-fill:
- For each delta row, if inner-join matches exist: emit them (weight =
  `w_delta × w_trace`, same as inner join). If no match: emit one
  null-filled row (weight = `w_delta`, right columns = NULL via
  `NullAccessor`).
- **Not bilinear**: the null-fill path breaks the weight-product property
  (output weight depends on match existence, not just weight arithmetic).
- Implemented as `OPCODE_JOIN_DELTA_TRACE_OUTER` (`join.py:212-270`).
  Feldera uses a different approach: a "saturate" operator injects ghost
  `(k, NULL)` tuples, then runs a standard inner join.

*Set-difference form (the general null-fill).* A LEFT outer join is equivalently
`inner ∪ null_extend(A − distinct(π_A(inner)))`, where `π_A(inner)` projects the
**preserved side's full payload**. The null-fill is thus a Z-set **difference**
`A − D` — linear (adding `A` and `−D`), not an anti-join that must recover
payload: the matched preserved rows are read straight off the inner output and
deduplicated to one row per preserved identity by `distinct`. Keying `D` by the
**preserved row's identity** (its source PK) is what makes this the general form —
it holds even when one preserved row matches a whole interval of the other side
(range / band joins). An equi join may instead key `D` by the join key (the
`distinct(B_keys)` form), valid only because equi-match existence is a function of
`B` alone. **No delta-delta cross term is needed** despite `D` depending on the
preserved side: the lone non-linear operator, `distinct`, natively absorbs the
within-epoch `ΔA` / `Δmatched` simultaneity (DBSP Prop 4.7 — it emits `+a` / `−a`
exactly on the per-identity 0-boundary crossing). GnitzDB's band/range LEFT join
uses this form.

*Empty-other-side robustness.* `D = ∅` gives `A − ∅ = A`: an empty or absent other
side null-fills every preserved row, free, with no sentinel. A null-fill written
instead as a **direct** match against a witness relation — e.g. `A ⋈_{a.x ≥ m} {m}`
for a single-inequality threshold `m = MAX/MIN(other.col)` — **vanishes** when that
relation is empty and must be seeded with a sentinel row; the difference form
`A − (A ⋈_{a.x < m} {m})` keeps the bare `A` passthrough, so empty `{m}` correctly
null-fills all of `A` and needs no seed. Prefer the difference form for any
"`A` that does / does not satisfy `P`" split whose witness can be absent.

*Weight-exactness — `distinct` clamps multiplicity.* `D = distinct(π_A(inner))` is
exact only when the preserved side's identities are unique (weight ≤ 1 — the
`unique_pk` base-table case, the common one). A **bag-valued** preserved side (a
non-`unique_pk` table, a `UNION ALL` view) is **over-filled**: `distinct` clamps
matched multiplicity to 1 while `A` keeps weight `w`, so a *matched* weight-`w`
preserved row leaks a spurious weight-`w−1` null-fill. The exact form subtracts
match **existence** while preserving `A`'s multiplicity, with an existence witness
that is a function of the **other** side (so it is available in-epoch for a row
matched in its own insertion epoch): an anti-join by the join key over
`distinct(B_keys)` (equi — match existence is a function of `B` alone), or a
threshold join against the one-row-per-group `MAX/MIN(other.range)` (band/range —
the one row per group cannot multiply, so no `distinct` is needed and `A`'s weight
is carried verbatim). A witness taken from the **inner output** (`distinct(π_A(inner))`)
is *not* a valid weight-exact substitute through a `z⁻¹` trace: it is entangled with
`ΔA`, so a trace anti-join would spuriously null-fill a row matched in its own epoch.
The current band set-difference dodges that only by netting `+a`/`−a` in-epoch through
`distinct` — which is exactly what clamps the weight; the threshold witness fixes both.

**Non-linear operators** — require access to the accumulated integral:

*Distinct:* O(|delta|) via DBSP Proposition 4.7 — point lookups into
the integral detect transitions across the positive boundary
(non-positive → positive or positive → non-positive; zero counts as
non-positive per `distinct`: weight > 0 → 1, else 0).

*Reduce (aggregation):*
`δ_out = Agg(history + δ_in) - Agg(history)` — emit old aggregate at
weight -1, new at +1.
- **SUM, COUNT are linear:** `Agg(A+B) = Agg(A) + Agg(B)`, so
  `new = old + delta_contribution`. No history replay; consolidation of
  input delta skippable (`reduce.py` fast path, lines 460–465).
- **MIN, MAX are non-linear:** retraction of current min/max requires
  the next value from history. Uses optional AggValueIndex for
  O(log N + 1) lookup instead of full trace scan.

*Anti-join and Semi-join:*
Non-linear (depend on `distinct(B)` internally).
`antijoin(A, B) = A - semijoin(A, distinct(B))`. Incremental:
`Δ(antijoin(A, B)) = anti_dt(ΔA, I(D)) - semi_dt(ΔD, I(A)) - semi_dd(ΔA, ΔD)`
where `D = distinct(B)`. Output uses left schema only; weights preserved
(not multiplied). SQL: EXCEPT = anti-join, INTERSECT = bidirectional
semi-join.

*All non-linear operators:* Consolidation mandatory — must see true net
weights. Enforced by `ConsolidatedScope`.

### The Integral (Trace)

`I(A)_t = Σ(dA_0..dA_t)`. Stored as `ZSetStore` (PersistentTable or
EphemeralTable). `integrate_op` adds each tick's delta.

Cursors see the net state through two-tier consolidation:

1. **MemTableCursor**: reads from `get_consolidated_snapshot()` — N-way
   merge of sorted runs via `merge_runs_to_consolidated()`.
2. **UnifiedCursor**: merges MemTable snapshot + shard cursors on-the-fly
   via tournament tree, summing weights of matching (PK, payload) entries,
   dropping ghosts.

Both tiers depend on the (PK, payload) sort invariant (§4). Trace
cursors are snapshotted in `prepare_for_tick`, reflecting `z⁻¹(I(X))`
— the integral *before* the current delta.

## 4. Consolidation Paths

Three paths. All MUST sort by (PK, payload):

### 4a. Single-batch: `ArenaZSetBatch.to_consolidated()`

Sorts by (PK, payload) via `compare_indices`, walks sorted index
accumulating weights, drops ghosts. Returns self if already consolidated.

### 4b. N-way merge: `merge_runs_to_consolidated()`

Merges N sorted batches via min-heap (tournament tree) ordered by
**(PK, payload)**. Pending-group algorithm accumulates weight when
(PK, payload) matches, flushes on differ.

> **Precondition: each input batch MUST be sorted by (PK, payload).**
> `merge_batches` iterates each batch linearly via `MemBatchCursor`.
> If a batch is unsorted, the cursor produces entries out of order,
> the heap delivers non-adjacent duplicates, and the pending-group
> algorithm silently produces wrong weights. The Rust
> `Table::upsert_and_maybe_flush` defensively re-sorts batches
> as a safety net.

> **The heap MUST use payload-aware comparison.** PK-only ordering
> interleaves rows with same PK but different payloads across cursors,
> preventing weight accumulation. This specifically affects non-linear
> aggregates (MIN/MAX) whose secondary index tables have entries with
> matching PKs but varying payloads.

### 4c. Shard compaction: `compact_shards()` / `open_and_merge()` (Rust)

Merges N sorted shard files via min-heap ordered by **(PK, payload)**.
Pending-group drain: pops one entry at a time, accumulates weight while
(PK, payload) matches, flushes on differ — same algorithm as 4b.
Per-shard data is already consolidated; cross-shard duplicates are not.

## 5. Row Comparison: `compare_rows`

Compares **payload columns only** (PK compared separately). Iterates
columns in schema order, skipping pk_index:

1. Null: null < non-null; null == null.
2. Type dispatch: STRING (German string comparison), U128, F64/F32,
   default (signed integer via sign-extension).

**F64/F32 NaN:** all row-comparison helpers use `f64::total_cmp` /
`f32::total_cmp`, which imposes a strict total order on IEEE-754 values
(including NaN bit-pattern ordering). This applies to `compare_rows` on
the canonical merge path as well as the non-merge sort helpers
(`compare_cursor_payload_to_batch_row` in `ops/util.rs` and
`compare_by_group_cols` in `ops/reduce.rs`). Transitivity holds for all
inputs, including NaN.

## 6. The Region Convention

Column buffers are stored as flat (pointer, size) pairs in canonical order.
This layout applies to WAL blocks and shard files. `pk_stride` is the encoded
key width — the sum of the PK columns' encoded widths, tightly packed (no
inter-column padding) — and is fixed for a given schema.

```
region[0] = pk         (count × pk_stride bytes, OPK key)
region[1] = weight     (count × 8 bytes, i64 LE)
region[2] = null       (count × 8 bytes, u64 LE, 1 bit per payload col)
region[3..3+P-1] = payload columns (non-PK, schema order)
region[3+P] = blob     (variable-length string heap)
```

The PK region holds the **order-preserving key (OPK)** — the same encoding at
rest and in-engine — formed by concatenating the PK columns in PK-list order,
each encoded big-endian with signed columns sign-flipped. Two properties make it
load-bearing: an unsigned byte comparison of two keys *is* their typed PK order
(so every ordered path is one `memcmp`, §1), and the encoding is a bijection —
byte-equality is PK-equality (consolidation and dedup group on the raw bytes)
and the column values decode back out.

PK columns are not included in the payload region pointers.

Null bitmap uses **payload column indexing**: bit N is the N-th non-PK
column in schema order. The dense payload index is assigned by scanning the
columns left to right and numbering only the non-PK columns 0, 1, 2, …
(`SchemaDescriptor::compute_mappings`): `payload_mapping[ci]` holds that slot
for a payload column and `PAYLOAD_MAPPING_PK_SENTINEL` for a PK column, with
the inverse in `payload_to_ci[pi]`. For a **single-PK** schema this reduces to
the closed form `payload_idx = ci if ci < pk_index else ci - 1`; with a
**compound PK** the columns are renumbered around *every* PK position, so the
closed form does not hold — read `payload_mapping`, never `ci - 1`.

---

# GnitzDB Developer Guide

## Prerequisites

Linux with an io_uring kernel (≥ 5.x); stable Rust; `uv` (drives Python + `maturin`). Nothing is version-pinned.

## Build targets

| What | Command | Rebuilds |
|------|---------|----------|
| Unit tests | `make test` | all workspace crates except `gnitz-py` |
| Server binary | `make server` | gnitz-engine |
| Python extension | `make pyext` | gnitz-sql, gnitz-core, gnitz-py |
| E2E tests | `make e2e` | server + pyext, then runs pytest |
| Release binary | `make release-server` | gnitz-engine (release) |

`make e2e` rebuilds both the server binary and the Python extension
(which contains the SQL planner). Maturin is a no-op when nothing
changed, so there's no cost.

**Pre-commit gate (no CI):** `make verify` = `fmt-check` + `clippy` (warnings are errors) + `test`. Also `make fmt`, `make check`.

## Project structure

Two sides that meet only at the wire protocol: the **SQL/client side** (a library,
also exposed as C and Python bindings) plans and drives queries; the **engine**
executes them as a multi-process server.

### Workspace crates

| Crate | Role | Depends on |
|-------|------|------------|
| `gnitz-wire` | Wire-protocol constants + codecs — the one definition client and engine must agree on | — |
| `gnitz-core` | Client core: connection, protocol, and the logical type / expression / circuit model | `wire` |
| `gnitz-sql` | SQL front end: parser, binder, query planner | `core` |
| `gnitz-capi` | C ABI bindings over the client core + planner | `core`, `sql` |
| `gnitz-py` | Python extension (pyo3) — the driver + planner the test/benchmark suites run against | `core`, `sql` |
| `gnitz-engine` | The DBSP execution engine and `gnitz-server` binary | `wire` |
| `gnitz-test-harness` | Spawns a `gnitz-server` subprocess in a private tmpdir for integration tests | — |

The SQL side compiles a query and ships it to the engine over `gnitz-wire`; the
engine never links the planner.

### The engine (`gnitz-engine`)

Strictly layered — every module depends only on those beneath it (and all of them
on `foundation`):

```
runtime (L7)   → catalog, query, ops, storage, schema    orchestration · protocol · reactor
catalog        → query, ops, storage, schema
query (L5)     → ops, expr, storage, schema               compiler · vm · dag
ops            → expr, storage, schema                    join · reduce · exchange · …
expr           → storage, schema
storage        → schema                                   repr (L2) · lsm (L3)
schema         → foundation
foundation (L0)  — independent leaves; depends on nothing
```

- **`foundation`** (L0) — unrelated leaves grouped only for layering: `log` (the `gnitz_*` macros), `codec` (LE pack/unpack), `xxh` (XXH3), `posix_io` (fd I/O, fsync, sockets), `syscall` (eventfd/futex/memfd/mmap), `worker_ctx` (worker rank/count).
- **`schema`** — SQL type constants, schema descriptors, row-format helpers, and the order-preserving-key cluster (`key`). Shared by the storage, IPC, and query layers.
- **`storage`** — the WAL/shard/MemTable stack behind one curated facade, in two sub-layers:
  - `repr` (L2) — the in-memory batch and the kernels over it: `batch` (region layout), `batch_wire` (wire/shard serde), `batch_pool` (buffer recycling), `columnar` (comparators), `merge` (sort-merge consolidation), `scatter` (exchange repartition), `heap` (k-way merge), `bloom`/`xor8` (PK-probe filters).
  - `lsm` (L3) — the on-disk half: `wal`, `shard_file`/`shard_reader`/`shard_index`, `compact` (N-way compaction), `memtable`, `read_cursor`, and the `Table`/`PartitionedTable` facades.
- **`expr`** — compiled expression programs evaluated over batches (`program`, `batch`, `plan`).
- **`ops`** — the DBSP operators: `join` (inner/outer/anti/semi, split by Δ⋈trace, Δ⋈Δ, and range), `reduce` (aggregation), `exchange` (repartition: `router` + `relay`), `distinct`, `linear` (filter/map/negate/union), `scan`, `reindex` (re-key for join/group), `cogroup`, `index` (secondary indexes).
- **`query`** (L5) — the circuit layer behind the `dag` facade: `compiler` (view → DBSP circuit → VM program), `vm` (executes the program), `dag` (`DagEngine`: plan cache, epoch evaluator, ingestion). `catalog` and `runtime` reach this layer only through `dag`.
- **`catalog`** — the DDL/metadata engine wrapping `DagEngine`: `ddl`, `sys_tables`, `hooks`, `registry`, `metadata`, `validation`, `write_path`, persistence (`store_io`, `partition_lsn`), `cache`, `bootstrap`.
- **`runtime`** (L7) — the multi-process server (`main.rs` builds `gnitz-server`):
  - `orchestration` — `master` (SAL dispatcher: fans push/scan out to workers, collects via W2M), `worker` (event loop owning a partition subset), `executor` (single-threaded server executor), `committer` (durable-commit batcher).
  - `protocol` — `wire` (IPC message format), `sal` (shared append-only log), `w2m`/`w2m_ring` (lock-free worker→master ring).
  - `reactor` — the single-threaded io_uring reactor (`block_on`/`spawn`/`timer`/reply routing).

Test scaffolding lives in `test_support` / `test_rng` and per-module `tests/`.

## Running E2E tests

**Always run E2E with multiple workers.** Single-worker mode skips
exchange/fanout paths and will miss distributed bugs.

```bash
make e2e                       # rebuilds server, GNITZ_WORKERS=4
make e2e K='joins' WORKERS=1   # pytest -k filter; override worker count
make test T=name               # one Rust test (make rust-engine-test = engine only)
```

## Environment variables

| Var | Effect |
|-----|--------|
| `GNITZ_WORKERS` | Worker count (Makefile/tests → server `--workers`) |
| `GNITZ_LOG_LEVEL` | `quiet` / `normal` / `verbose` (`debug` = alias) |
| `GNITZ_SERVER_BIN` | Override server binary (e.g. aim E2E at the release build) |
| `GNITZ_CHECKPOINT_BYTES` | SAL checkpoint threshold (default 75% of SAL) |

## Debug logging

```bash
cd crates/gnitz-py && GNITZ_WORKERS=4 GNITZ_LOG_LEVEL=debug uv run pytest -x <test-file>
```

Log format: `<epoch>.<ms> <tag> <level> <message>` — tags: `M` (master), `W0`–`WN`.

For temporary logging: `gnitz_debug!` / `gnitz_info!` macros. Remove before committing.

### Where test logs go

Server stderr (master process): always written to `~/git/gnitz/tmp/server_debug.log`.
Pytest's `-s` flag does NOT capture this — it lives on disk regardless of
pytest's stdout/stderr capture mode.

Worker logs: workers write to `<data_dir>/worker_<N>.log` inside the per-test
tmpdir. The conftest copies them to `~/git/gnitz/tmp/last_worker_N.log` on
session teardown — those are the canonical post-mortem files.

Live tail (during a long-running test): `tail -f ~/git/gnitz/tmp/gnitz_data_*/data/worker_*.log`

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
   to attach a log to a bug report, copy it out of `~/git/gnitz/tmp/`
   before the next test run overwrites it.

## SAL durability contract

**Rule: an ACK to a client implies fdatasync iff the operation upserted data.**

The SAL (Shared Append-Only Log) carries both data writes and ephemeral
commands on the same mmap'd fd. Workers see all SAL entries immediately
via Acquire/Release atomics on the size prefix — fdatasync is irrelevant
for cross-process visibility. It exists solely for crash recovery.

Durable operations are atomic: crash recovery applies an operation in full
or not at all, so a crash never leaves a half-written DDL or push behind.
Operations that upsert table data fdatasync before the ACK; command-only
operations — view ticks, scans, seeks, backfills, validation queries —
wake the workers without it, since a lost command needs no recovery
(base-table data is intact and views are re-derived).

## Benchmarking

```bash
make bench                          # quick mode, 1 worker
make bench-full                     # full mode, 4 workers
make bench-sweep                    # sweep workers=1,2,4 × clients=1,2,4
make bench-perf                     # full + perf record + perf stat

# Knobs: WORKERS, CLIENTS, FULL=1, PERF=1
make bench WORKERS=4 PERF=1        # quick, 4 workers, perf
```

Results: `benchmarks/results/` (gitignored). The runner rotates old results (keeps 10).

Workflow: `make bench` → change → commit → `make bench` → compare `summary.json`.

### Rust micro-benchmarks

`make bench` is end-to-end (full server + IPC), so it can't isolate a tight
in-process loop. For that, the engine carries `#[ignore]`d timing tests that
print throughput and must be run in `--release`:

```bash
cd crates && cargo test -p gnitz-engine --release <name>_bench \
    -- --ignored --nocapture --test-threads=1
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
3. **Log first, never guess.** Rebuilds are expensive. One well-instrumented
   run reveals more than ten speculative attempts.
   - `gnitz_debug!` / `gnitz_info!` for structured logging
   - `GNITZ_LOG_LEVEL=debug` to enable debug-level messages
   - `RUST_BACKTRACE=1` for panic backtraces
4. **Use the debug binary for crashes.** Release builds silently clamp
   corrupt values.
5. **Isolate with 1 worker first.** Pass with W=1 but fail with W=4 →
   bug is in exchange/fanout, not computation.
6. **Bisect by sub-path.** Disable the new fast-path to confirm the bug
   is in the new code.
7. **Verify your fix is in the binary.** `make e2e` rebuilds both the
   server and the Python extension before running tests.

## See also

- `async-invariants.md` — runtime & reactor: kernel coordination, scheduling, known latent issues.

## GIT Branches

All development happens on main for now; never branch off.

## Compat / Legacy code

Gnitz is pre-alpha, there are no production uses of this software. Keeping legacy code in
the codebase is strictly not allowed.
