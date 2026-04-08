# GnitzDB Theoretical Foundations

Every storage operation, DBSP operator, and merge path derives from these
definitions. Bugs almost always trace to violating an invariant stated here.

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

**Single-column PKs only.** Each table has exactly one PK column
(enforced by schema validation). The 128-bit slot holds one column
value: `(lo, hi)` for U128, `(value, 0)` for narrower types.

**Physical representation.** `ArenaZSetBatch`: columnar buffers where each
row is (pk_lo, pk_hi, weight, null_word, col_0, col_1, ...). Multiple
rows may share the same PK if their payloads differ.

**PK uniqueness is not a general invariant.** The 128-bit PK slot is a
sort/routing key. It is unique for base table batches (DML-enforced)
and reduce output (one row per group). It is NOT unique for
intermediate batches: `map_reindex` overwrites the PK slot with a
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
merging, range scans, and compaction). Sort key: PK first (u128: high
64 bits, then low 64 bits), then payload columns in schema order
(`compare_rows`).

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
  (dA ⋈ dB = 0). Must be revised if batched multi-source epochs are added.
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
   merge of sorted runs via `_merge_runs_to_consolidated()` (Rust FFI).
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

### 4b. N-way merge: `_merge_runs_to_consolidated()` (Rust FFI)

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

Historical bug (fixed 2026-03): the heap used PK-only ordering.
`collect_min_indices` used full comparison but its tree walk stopped at
non-equal nodes, missing matching entries deeper in the heap. This
corrupted any table flushed between ticks where retractions and
insertions for the same (PK, payload) ended up in different shards.

## 5. Row Comparison: `compare_rows`

Compares **payload columns only** (PK compared separately). Iterates
columns in schema order, skipping pk_index:

1. Null: null < non-null; null == null.
2. Type dispatch: STRING (German string comparison), U128, F64/F32,
   default (signed integer via sign-extension).

**F64/F32 NaN:** Rust treats NaN as equal to every value
(`partial_cmp().unwrap_or(Equal)`). This violates transitivity
(NaN == 1.0, NaN == 2.0, but 1.0 ≠ 2.0) and does not impose a total
order. Safe only because NaN does not arise in normal DML paths. If
NaN is ever stored, must switch to `f64::total_cmp`.

## 6. The Region Convention

Column buffers are stored as flat (pointer, size) pairs in canonical order:

```
region[0] = pk_lo      (count × 8 bytes, u64 LE)
region[1] = pk_hi      (count × 8 bytes, u64 LE)
region[2] = weight     (count × 8 bytes, i64 LE)
region[3] = null       (count × 8 bytes, u64 LE, 1 bit per payload col)
region[4..4+P-1] = payload columns (non-PK, schema order)
region[4+P] = blob     (variable-length string heap)
```

PK columns are not included in the payload region pointers.

Null bitmap uses **payload column indexing**: bit N = N-th non-PK column.
For schema column ci: `payload_idx = ci if ci < pk_index else ci - 1`.
