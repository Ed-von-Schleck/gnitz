Never check if failures are pre-existing; as a rule, all tests are green at the start of a session.

Never check or confirm that compilation warnings or Clippy errors/warnings are caused by the current session, always directly fix them.

Use `semble search` (the semble MCP server is installed) to find code by describing what it does or naming a symbol/identifier instead of grep. There are other useful semble commands as well. Prefer it over grep + read every time.

gnitz is **pre-alpha** and not used in production anywhere because it has not been released, so there are **never** any compatibility concerns and there should **never** be legacy code remaining.

Read dev-guide.md for GnitzDB-specific development guidelines.

---

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
