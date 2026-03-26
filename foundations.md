# GnitzDB Theoretical Foundations

This document defines the algebraic model underlying GnitzDB. Every storage
operation, every DBSP operator, and every merge path is a direct consequence
of these definitions. Bugs in this system almost always trace back to
violating one of the invariants stated here.

## 1. Z-Sets

A **Z-Set** over a domain *D* is a function *Z: D -> Z* (integers), mapping
each element to an integer **weight**. Only finitely many elements have
non-zero weight. The weight represents multiplicity: +1 means "present",
-1 means "retracted", +3 means "three copies".

In GnitzDB, the domain *D* is the set of all possible **rows** for a given
table schema. A row is fully identified by **(PK, payload)** — the primary
key together with all non-PK column values. Two rows with the same PK but
different payload values are **different elements** with independent weights.

This is the single most important definition in the system:

> **Element identity = (PK, all payload columns).**
> The PK alone does NOT identify an element.
> Only (PK, payload) does.

GnitzDB stores Z-Sets physically as `ArenaZSetBatch`: columnar buffers
where each row is a (pk_lo, pk_hi, weight, null_word, col_0, col_1, ...)
tuple. Multiple rows may share the same PK if their payloads differ.

## 2. Z-Set Operations

**Addition (Union):** (A + B)(x) = A(x) + B(x) for all x.
Implemented by concatenating batches and later consolidating.

**Negation:** (-A)(x) = -A(x).
Implemented by `op_negate` (flips all weights).

**Consolidation:** Given a batch with duplicate elements (same PK and same
payload appearing multiple times), consolidation groups them and sums their
weights. Elements whose net weight is zero are dropped ("ghost elimination").

> **Consolidation is the only operation that changes the number of rows.**
> It must be correct for the entire system to function.

**Consolidation requires a total order on elements.** GnitzDB sorts by
PK first (u128 comparison: high 64 bits, then low 64 bits), then by
payload columns in schema order (the `compare_rows` function). This
ensures that all rows with identical (PK, payload) are adjacent in the
sorted output, allowing a single-pass accumulation.

> **Invariant: any merge or consolidation path must produce output sorted
> by (PK, payload), not just by PK.** If rows with the same PK but
> different payloads are interleaved, the sequential weight accumulation
> algorithm silently produces incorrect results (duplicate entries that
> should have been summed, or ghost rows that should have been eliminated).

## 3. DBSP: Incremental Computation on Z-Sets

GnitzDB implements the DBSP (Database Stream Processing) model from
Budiu et al. The core idea: instead of recomputing a query from scratch
on every update, the system processes only the **delta** (the change) and
produces the **output delta**.

### Streams and Deltas

A **stream** is a sequence of Z-Sets indexed by time (tick). At each tick,
the input delta *dA* arrives and the circuit produces output delta *dO*.

### Operators

Operators are classified by linearity:

**Linear operators** satisfy `L(A + B) = L(A) + L(B)`:
- Filter, Map, Negate, Union, Delay (z^-1)
- These are stateless — they transform the current delta without history.
- Consolidation is optional before a linear operator (an optimization).

**Bilinear operators** satisfy `L(A, B+C) = L(A, B) + L(A, C)`:
- Join: `d(A join B) = dA join I(B) + I(A) join dB` (incremental expansion)
- These require the **integral** (accumulated history) of one operand.
- Consolidation of the delta input is required for correctness.

**Non-linear operators** have no algebraic shortcut:
- Distinct, Reduce with MIN/MAX
- These must replay the full accumulated state to compute the new value.
- Consolidation of the delta input is mandatory.
- The `ConsolidatedScope` context manager enforces this.

### The Integral (Trace)

The **integral** I(A) is the running sum of all deltas: I(A)_t = sum(dA_0..dA_t).
Stored physically as a `ZSetStore` (PersistentTable or EphemeralTable).
The `integrate_op` instruction adds each tick's delta into the trace.

Trace tables accumulate rows over time. When a cursor reads the trace,
it sees the net state: all insertions minus all retractions, consolidated.

## 4. Consolidation Paths in GnitzDB

Consolidation happens in three places. All three MUST sort by (PK, payload):

### 4a. Single-batch: `ArenaZSetBatch.to_consolidated()`

Sorts the batch's rows by (PK, payload) using `compare_indices`, then
walks the sorted index array accumulating weights. Ghost rows (net weight
zero) are dropped. Returns a new batch or self if already consolidated.

### 4b. N-way merge: `_merge_runs_to_consolidated()` (Rust FFI)

Merges N sorted batches (MemTable runs + accumulator) into one consolidated
batch. Uses a min-heap (tournament tree) ordered by **(PK, payload)** — not
just PK. The sequential pending-group algorithm reads one row at a time from
the heap, accumulating weight when (PK, payload) matches the pending row and
flushing when it differs.

> **The heap MUST use payload-aware comparison for its ordering.**
> PK-only ordering causes rows with the same PK but different payloads to
> interleave across cursors. When this happens, two rows with the same
> (PK, payload) from different batches may arrive non-adjacently, preventing
> weight accumulation. This bug specifically affects non-linear aggregates
> (MIN/MAX) whose secondary index tables have entries with matching PKs but
> varying payloads.

### 4c. Shard compaction: `compact_shards()` (Rust)

Merges N sorted shard files. Uses the same tournament tree pattern with
payload-aware comparison (`compare_to_root` calls `compare_rows`). Shard
data is already consolidated per-shard, so within-cursor duplicates don't
arise — but cross-shard duplicates do.

## 5. Row Comparison: `compare_rows`

Compares two rows by their **payload columns only** (PK is compared
separately). Iterates columns in schema order, skipping pk_index:

1. Null check: null < non-null; null == null.
2. Type dispatch: STRING (German string comparison), U128, F64/F32 (IEEE),
   default (signed integer via sign-extension).

This function must be identical across RPython (`core/comparator.py`) and
Rust (`merge.rs`, `compact.rs`). Any divergence causes sort-order
inconsistencies that corrupt consolidation.

## 6. The Region Convention

Column buffers cross the RPython-Rust FFI boundary as flat arrays of
(pointer, size) pairs in a canonical order:

```
region[0] = pk_lo      (count * 8 bytes, u64 LE per row)
region[1] = pk_hi      (count * 8 bytes, u64 LE per row)
region[2] = weight     (count * 8 bytes, i64 LE per row)
region[3] = null       (count * 8 bytes, u64 LE per row, 1 bit per payload col)
region[4..4+P-1] = payload columns (non-PK only, in schema order)
region[4+P] = blob     (variable-length string heap)
```

`col_bufs[pk_index]` in RPython has stride=0 and is skipped when packing
regions. The Rust side receives only payload column pointers.

The null bitmap word uses **payload column indexing**: bit N corresponds to
the N-th non-PK column. For column ci in the schema:
`payload_idx = ci if ci < pk_index else ci - 1`.
