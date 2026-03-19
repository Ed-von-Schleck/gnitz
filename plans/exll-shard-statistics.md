# ExaLogLog Per-Column Cardinality Sketches

Per-shard, per-column `n_distinct` estimates stored as `.exll_cN` sidecar files.
No prerequisites. Purely additive — no existing behaviour changes.

---

## Background

### What gnitz already knows per shard

`ShardHandle` currently records three things at flush time:

- `(pk_min, pk_max)` — PK range bounds for range pruning
- `shard.view.count` — exact row count, derivable from the shard file header
- `Xor8Filter` — PK membership testing (built from the PK buffers via XXH)

Row count and PK range are useful for PK-keyed queries. They say nothing about the
value distribution of non-PK columns.

### The gap: non-PK column cardinality

`n_distinct(col)` for non-PK columns is not derivable from any existing metadata.
This matters because gnitz joins are not limited to PK columns.

The SQL planner (`rust_client/gnitz-sql/src/planner.rs`) builds joins via MAP+reindex:
both sides of a join are passed through `map_reindex(input, join_col, ...)`, which
promotes `join_col` to become the batch PK via `PARAM_REINDEX_COL` before the DBSP
join operators see them. The join column can be any column — in practice it is
frequently an FK column (e.g. `orders.customer_id → customers.id`). Secondary index
circuits (`index_circuit.py`) are maintained for FK and other indexed columns under
the same assumption.

For a PK-keyed join, `n_distinct(join_col) == row_count` and the XOR8 filter's
implicit row count is sufficient. For an FK-column join, `n_distinct(orders.customer_id)`
is the number of distinct customers referenced by orders — potentially far less than
`row_count(orders)` — and no existing metadata can estimate it. ExaLogLog closes
this gap.

### Why ExaLogLog specifically

ExaLogLog (Ertl, EDBT 2025) achieves 43% better space efficiency than HyperLogLog
for the same relative error by storing both a maximum update value *and* a sliding
window of history flags per register, enabling a maximum-likelihood estimator instead
of the biased harmonic mean.

Reference implementation: `github.com/dynatrace-research/exaloglog-paper`

### Use cases, ordered by immediacy

**1. `APPROX_COUNT_DISTINCT(col)` aggregate** (`optimizations.md` §25)

The most immediately actionable use. The shard-level sketches built at flush time are
exactly the per-group accumulators needed for an approximate COUNT DISTINCT aggregate.
`step(val)` calls `exll_add`; `get_value` calls `exll_estimate`. No query planner or
optimizer infrastructure required — it is a standalone aggregate function. Restricted
to append-only sources initially (ExaLogLog has no deletion operation).

**2. Exchange shard key quality**

When choosing a shard column for `EXCHANGE_SHARD` in multi-worker circuits,
`n_distinct(col) / row_count` reveals how many distinct values are available for
routing. A column with low n_distinct (e.g. a status enum) produces skewed partitions;
a column with n_distinct ≈ row_count produces balanced ones. This signal is usable
today, without a planner, to guide shard column selection.

**3. Join ordering** (`optimizations.md` §15)

For FK-column and indexed-column joins, `n_distinct(join_col)` is the correct
cardinality signal for choosing join order and estimating trace state size. The
adaptive swap inside `op_join_delta_trace` already uses runtime sizes; compile-time
circuit ordering (for 3+ way joins) would benefit from this data. This use case
requires the logical plan layer (`optimizations.md` §15 notes it as a prerequisite)
which is currently declared but unused.

For completeness: join cardinality (output row count) requires set *intersection*
cardinality `|distinct(L.join_key) ∩ distinct(R.join_key)|`, not just n_distinct of
each side independently. That requires Theta sketch sidecars (see `research/sketches.md`
§Opportunity 7). ExaLogLog provides the per-table n_distinct inputs to that estimate.

**4. Filter selectivity**

`n_distinct(col) / row_count` bounds selectivity under a uniform assumption; combined
with existing min/max bounds this improves range selectivity estimates. Actionable only
once predicate pushdown to the storage layer exists — currently `op_filter` operates
on rows returned by cursors and the predicate is invisible to shard selection.

**5. Zone map investment signal** (`optimizations.md` §24)

`shard_n_distinct / global_n_distinct` reveals whether a column's values are
correlated with PK order: a low ratio means the shard covers a tight slice of the
column's value domain and zone map range pruning will be effective; a ratio near 1.0
means zone maps won't prune. This signal is useful only once non-PK zone maps (min/max
per column per shard) exist — zone maps are a prerequisite, and ExaLogLog would then
guide whether to build them for a given column at all.

---

## Algorithm

### Parameters

| Symbol | Meaning | Value |
|--------|---------|-------|
| `t` | sub-bucket bits | 2 |
| `d` | history bits per register | 20 |
| `p` | log₂ of register count | 12 |
| `m` | register count (`2^p`) | 4096 |
| register width | `6 + t + d` bits | 28 bits |
| state size | `ceil(m × 28 / 8)` bytes | 14 336 bytes |
| relative std error | `≈ 1.04 / sqrt(m)` | ≈ 1.6% |

Constraint: `t + p ≤ 26` (`V = 26`, the token bit width). With `t=2, p=12`: 14 ≤ 26. ✓

### Register encoding

Each 28-bit register holds two fields:

```
[ upper (8 bits = 6+t) | lower (20 bits = d) ]
      u = r >> d              history flags
```

`u` is the maximum update value seen for this bucket. The lower `d` bits are a
sliding window: bit at position `delta` (from the MSB of the d-bit field) is set
if an update with value `u - delta` was ever observed, for `delta ∈ [1, d]`.

Initial register state: `r = 0` (u = 0 is the sentinel for "no update seen").

### Add

```
mask = ((1 << t) << p) - 1              # lower (p+t) bits
idx  = (h & mask) >> t                  # bits [p+t-1 : t] → register index ∈ [0, m)
nlz  = clz64(h | mask)                  # leading zeros; ORing mask prevents clz(0)
k    = (nlz << t) + (h & ((1<<t)-1)) + 1   # ∈ [1, (65-p-t)×2^t]

r     = packed_get(registers, idx)
u     = r >> d
delta = k - u

if delta > 0:
    r_new = k << d
    if delta <= d:
        r_new |= ((1 << d) | (r & ((1<<d)-1))) >> delta
    packed_set(registers, idx, r_new)
elif delta < 0 and d + delta >= 0:
    r_new = r | (1 << (d + delta))
    if r_new != r:
        packed_set(registers, idx, r_new)
# delta == 0 or outside history window: no-op
```

`clz64(x)`: no RPython builtin; implement with a 6-step binary search (~12 lines).
The `h | mask` trick ensures the argument is never zero (clz(0) is undefined in C).

### Merge (per-register)

Not a simple max. History windows must be shifted to align before ORing:

```
mergeRegister(r1, r2):
    u1, u2 = r1 >> d, r2 >> d
    x = 1 << d
    if u1 > u2 and u2 > 0:
        return r1 | shift_right(x | (r2 & (x-1)), u1 - u2)
    elif u2 > u1 and u1 > 0:
        return r2 | shift_right(x | (r1 & (x-1)), u2 - u1)
    else:
        return r1 | r2   # u1 == u2, or one side is 0 (empty)
```

`shift_right(s, delta)`: returns `s >> delta`; returns 0 if `delta >= 64`.

Sketch-level merge: for each register index `i`, set `registers[i] =
mergeRegister(self.registers[i], other.registers[i])`.

### Estimator

The estimator uses a maximum-likelihood approach. For each register, a `contribute`
function decomposes the register value into ML coefficients, populating a 64-entry
count array `b_counts` and accumulating into a fixed-point sum `agg`. After all `m`
registers, Newton iteration solves the ML equation; a precomputed bias correction
constant is applied.

```
# Per-register contribution (see contribute() below)
agg = r_uint64(0)       # unsigned 64-bit fixed-point accumulator (×2^-64 units)
b_counts = [0] * 64     # b_counts[j] = count of registers contributing at level j

for i in range(m):
    r = packed_get(registers, i)
    agg += contribute(r, b_counts)  # modifies b_counts in place, returns u64 delta

factor = float(m << (t + 1))       # = m × 2^(t+1)
a = u64_to_float(agg) * POW2_NEG64 * factor
x = solve_mle(a, b_counts, J=63-p-t)
n_hat = factor * x / (1.0 + BIAS_CORRECTION / float(m))
```

`BIAS_CORRECTION` for `(t=2, d=20)`: `0.48147289716` (precomputed constant from the
paper's `MLBiasCorrectionConstants` table).

**`contribute(r, b_counts) → r_uint64`:**

The contribute function extracts how this register's stored state (max `u` and history
flags) informs the MLE. It iterates over the observation levels implied by the register
and increments `b_counts[j]` for each level that has definitive information (either a
flag confirming the value was seen, or the absence of a flag confirming it was not seen
at that level). Returns the fixed-point fractional contribution to `agg`.

This function is the most complex part. Translate directly from `ExaLogLog.java`'s
`contribute` method in the reference implementation.

**`solve_mle(a, b_counts, J) → float`:**

Newton iteration over the equation derived in the paper (Algorithm 8). Converges in
2–5 iterations in practice. Translate from `DistinctCountUtil.solveMaximumLikelihoodEquation`.

### Packed array access

Registers are 28 bits each, not byte-aligned. Two consecutive registers straddle 7
bytes. Access helpers:

```python
def packed_get(buf, idx):
    bit_off = idx * 28
    byte_off = bit_off >> 3
    bit_shift = bit_off & 7
    # Read 5 bytes (40 bits covers a 28-bit window at any alignment)
    raw = r_uint64(0)
    i = 0
    while i < 5:
        raw |= r_uint64(ord(buf[byte_off + i])) << (i * 8)
        i += 1
    return intmask((raw >> bit_shift) & r_uint64(0xFFFFFFF))  # 28-bit mask

def packed_set(buf, idx, val):
    bit_off = idx * 28
    byte_off = bit_off >> 3
    bit_shift = bit_off & 7
    mask = r_uint64(0xFFFFFFF) << bit_shift
    raw = r_uint64(0)
    i = 0
    while i < 5:
        raw |= r_uint64(ord(buf[byte_off + i])) << (i * 8)
        i += 1
    raw = (raw & ~mask) | ((r_uint64(val) & r_uint64(0xFFFFFFF)) << bit_shift)
    i = 0
    while i < 5:
        buf[byte_off + i] = chr(intmask((raw >> (i * 8)) & r_uint64(0xFF)))
        i += 1
```

State size in bytes: `STATE_BYTES = (m * 28 + 7) // 8 = 14336` for `p=12`. Allocate
with `lltype.malloc(rffi.CCHARP.TO, STATE_BYTES, flavor="raw")`.

---

## Z-Set Semantics

### Weight > 1: not a problem

ExaLogLog measures **set cardinality** — how many distinct values of a column exist.
`COUNT(DISTINCT col)` counts each value once regardless of row multiplicity. A row with
weight +5 contributes one distinct value to `n_distinct(col)`, identical to weight +1.
ExaLogLog's `add` is idempotent under repeated identical inputs (`register[b] =
max(register[b], k)`), which is the correct behaviour: adding the same column value
five times and once produce the same sketch state.

Calling `add(col_value)` exactly once per physical row (not `weight` times) is correct.

### Weight < 0: skip these rows

A consolidated shard may contain negative-weight rows. These are tombstones: retractions
of rows whose positive counterpart lives in a different, older shard. Including a
tombstone row in the sketch inflates `n_distinct` — we count values that are in the
process of being deleted.

**Fix:** in the build loop, read the weight buffer and skip rows with `weight <= 0`.

```python
weight_p = rffi.cast(rffi.LONGLONGP, weight_ptr)
for i in range(count):
    if rffi.cast(lltype.Signed, weight_p[i]) > 0:
        h = compute_checksum(rffi.ptradd(col_ptr, i * stride), stride)
        exll_add(registers, h)
```

`weight_ptr` is `region_list[2][0]` (weight is always region index 2).

### Resulting semantics

The per-shard sketch counts distinct column values **among positively-weighted rows in
that shard**. Merging sketches across all shards via `mergeRegister` gives the union of
all positive-weight observations. This is an upper bound on the true live `n_distinct`
because:

- A value may be positive in shard A (+1) and retracted in shard B (-1). With
  positive-weight filtering, it appears in A's sketch and not B's. After merge: counted
  once. Correct if the net weight across all shards is still > 0 (i.e., the value
  survives). Slightly overcounts if the net is 0 (the value has been fully deleted but
  the two shards haven't been compacted together yet). This window closes at the next
  L0→L1 compaction that spans both shards.

The overcount is bounded by the fraction of live rows that are pending cross-shard
cancellations. For typical workloads (low delete rate), this is negligible. For
query planning the direction is safe: an over-estimate of `n_distinct` leads to
under-estimating selectivity, which is the conservative choice.

---

## File Changes

### 1. `gnitz/storage/exll.py` (new, ~350 lines)

```
STATE_BYTES    = 14336   # (4096 × 28 + 7) // 8
REGISTERS      = 4096    # m = 2^p, p = 12
T              = 2
D              = 20
P              = 12
BIAS_CORRECTION = 0.48147289716   # for (t=2, d=20)
POW2_NEG64     = 5.421010862427522e-20   # 2^-64

_clz64(x: r_uint64) → int
    # 6-step binary search for leading zeros; returns 64 if x == 0.
    # Do NOT call with x == 0; use (h | mask) pattern at call sites.

packed_get(buf, idx) → int          # read 28-bit register at index idx
packed_set(buf, idx, val)           # write 28-bit register at index idx

shift_right(s, delta) → int         # s >> delta; 0 if delta >= 64

exll_add(registers, h)              # update registers with 64-bit hash h
exll_merge_into(dst_registers, src_registers)  # merge src into dst in-place
contribute(r, b_counts) → r_uint64  # per-register ML coefficient extraction
solve_mle(a, b_counts, J) → float   # Newton iteration
exll_estimate(registers) → float    # full estimator

class ExllSketch(object):
    _immutable_fields_ = ["m"]
    # m == 0  →  null/absent sentinel (returned by load_exll on miss)
    # m > 0   →  live sketch backed by raw registers array

    def __init__(self, registers, m):
        self.registers = registers
        self.m = m

    def is_present(self): return self.m > 0
    def n_distinct(self): return exll_estimate(self.registers) if self.m > 0 else -1.0
    def free(self): ...   # lltype.free if m > 0 and registers not null

NULL_EXLL_SKETCH = ExllSketch(lltype.nullptr(rffi.CCHARP.TO), 0)

def build_exll(col_ptr, stride, count, weight_ptr) → ExllSketch or None
    # Allocates STATE_BYTES, calls exll_add for each row with weight > 0.
    # Returns None if count == 0 or all rows have weight <= 0.

def save_exll(sketch, filepath)
    # Header (8 bytes): u32 m + u32 p (for forward-compat version check)
    # Data: STATE_BYTES of packed register array
    # fsync after write. Atomic: write to filepath+".tmp", rename.

def load_exll(filepath) → ExllSketch
    # Returns NULL_EXLL_SKETCH on missing file or corrupt header.
    # Allocates STATE_BYTES, reads register array, returns ExllSketch(registers, m).
```

RPython memory notes:
- Registers: `lltype.malloc(rffi.CCHARP.TO, STATE_BYTES, flavor="raw")`
- Zero-init with a `while` loop (no `memset` helper needed; or use `buffer.c_memmove`
  from `gnitz/core/buffer.py` with a zero-source if available)
- Hashing: `xxh.compute_checksum(rffi.cast(rffi.VOIDP, col_ptr), stride)` — already
  used by `writer_table.py` for blob dedup checksums
- All float arithmetic (`solve_mle`, `exll_estimate`) uses standard RPython floats
- `u64_to_float(x)`: `float(intmask(x))` does not work for values > `sys.maxint`. Use
  the two-halves trick: `float(intmask(x >> 32)) * 4294967296.0 + float(intmask(x & 0xFFFFFFFF))`
  if the full 64-bit range is needed for `agg`. In practice `agg` accumulates at most
  `m = 4096` contributions each ≤ `2^64 / m`, so overflow is impossible; direct
  `float(intmask(agg))` suffices after verifying the range.
- `NULL_EXLL_SKETCH` is a module-level prebuilt constant; the RPython annotator treats
  it as a `prebuilt ExllSketch`. Its `registers` field is `lltype.nullptr(...)` which
  is valid as long as no code path dereferences it when `m == 0`.

### 2. `gnitz/storage/writer_table.py` (3 locations, ~20 lines total)

**a) `_write_shard_file` signature** — add `schema` parameter:

```python
def _write_shard_file(filename, table_id, count, region_list,
                      pk_lo_ptr, pk_hi_ptr, schema):
```

**b) `_write_shard_file` body** — after the existing `build_xor8` / `save_xor8`
block (after `os.rename`), add:

```python
from gnitz.storage.exll import build_exll, save_exll
weight_ptr = region_list[2][0]   # region 2 is always the weight buffer
pk_idx = schema.pk_index
ci = 0
num_cols = len(schema.columns)
while ci < num_cols:
    col = schema.columns[ci]
    if ci != pk_idx and col.field_type.code != types.TYPE_STRING.code:
        non_pk_off = ci if ci < pk_idx else ci - 1
        region_idx = 4 + non_pk_off
        col_ptr, col_size = region_list[region_idx]
        stride = col.field_type.size
        sketch = build_exll(col_ptr, stride, count, weight_ptr)
        if sketch is not None:
            save_exll(sketch, filename + ".exll_c%d" % ci)
            sketch.free()
    ci += 1
```

**c) Two callers** — both already have `schema` in scope, pass it through:

```python
# write_batch_to_shard:
_write_shard_file(filename, table_id, count, regions,
                  batch.pk_lo_buf.base_ptr, batch.pk_hi_buf.base_ptr,
                  schema)          # add schema here

# TableShardWriter.finalize:
_write_shard_file(filename, self.table_id, self.count, regions,
                  self.pk_lo_buf.base_ptr, self.pk_hi_buf.base_ptr,
                  self.schema)     # add self.schema here
```

### 3. `gnitz/storage/index.py` (ShardHandle, ~25 lines)

**a) `__init__`** — after loading `xor8_filter`, load per-column sketches:

```python
from gnitz.storage.exll import load_exll, NULL_EXLL_SKETCH
from gnitz.core import types as _types
num_cols = len(schema.columns)
exll_sketches = newlist_hint(num_cols)
ci = 0
while ci < num_cols:
    col = schema.columns[ci]
    if ci != schema.pk_index and col.field_type.code != _types.TYPE_STRING.code:
        exll_sketches.append(load_exll(filename + ".exll_c%d" % ci))
    else:
        exll_sketches.append(NULL_EXLL_SKETCH)
    ci += 1
self.exll_sketches = exll_sketches
```

The list is always `num_cols` entries of type `ExllSketch` (never `None` — null object
pattern avoids the RPython annotator's None-in-list restriction from Appendix A §3).

**b) `n_distinct` method** (new public method):

```python
def n_distinct(self, col_idx):
    """Returns the ExaLogLog n_distinct estimate for col_idx, or -1.0 if
    no sketch was built for that column (PK, string, or pre-statistics shard)."""
    if col_idx < 0 or col_idx >= len(self.exll_sketches):
        return -1.0
    return self.exll_sketches[col_idx].n_distinct()
```

**c) `close`** — free exll sketches alongside xor8:

```python
def close(self):
    self.view.close()
    if self.xor8_filter is not None:
        self.xor8_filter.free()
    i = 0
    while i < len(self.exll_sketches):
        self.exll_sketches[i].free()
        i += 1
```

---

## String Columns

`TYPE_STRING` is skipped. The 16-byte German String inline struct is
`(u32 length, u32 prefix, u64 blob_arena_offset)`. The `blob_arena_offset` field is
a byte offset into the **per-shard** blob arena: it is shard-relative and differs for
identical strings in different shards. Hashing the full 16 bytes would treat the same
string value as a different hash input in each shard, making cross-shard sketch merge
meaningless. Correct string support requires hashing the actual string content (follow
the offset into the blob arena), which adds complexity. Defer to a follow-up.

`TYPE_U128` (also 16 bytes) is **not** skipped — it is a raw integer value, not a
pointer, and can be hashed directly as 16 bytes.

---

## Implementation Order

Each item is one commit. Run the relevant test target after each.

```
[1] exll.py — packed array helpers + add + merge
      test target: storage_comprehensive_test (test_exll_add_merge)

[2] exll.py — contribute + solve_mle + estimate
      test target: storage_comprehensive_test (test_exll_estimate_accuracy)

[3] exll.py — build_exll + save_exll + load_exll + NULL_EXLL_SKETCH
      test target: storage_comprehensive_test (test_exll_save_load)

[4] writer_table.py — schema param + build/save loop
    index.py — load loop + n_distinct + close
      test target: storage_comprehensive_test (test_exll_per_column_shard)
```

Splitting item [1] and [2] avoids debugging the complex MLE estimator while the
packed array and add/merge logic is still in flux.

---

## Tests

All new tests go in `rpython_tests/storage_comprehensive_test.py`.

**`test_exll_add_merge`**

Build two `ExllSketch` objects with disjoint key sets (hash distinct integers). Merge
one into the other. Verify: every key that was added to either sketch still produces
the expected register state changes (i.e. adding the same key again is a no-op after
merge). Verify `mergeRegister` with manually constructed register pairs where u1 > u2
and u1 < u2 to confirm history shifting.

**`test_exll_estimate_accuracy`**

1. Build a sketch by calling `exll_add` for 10 000 distinct 64-bit hash values.
2. Call `exll_estimate` and assert the result is within 5% of 10 000. (At p=12, ~1.6%
   relative std error; 5% allows 3σ.)
3. Repeat with 1 000 distinct values (small-cardinality case) and with 1 000 000
   (large-cardinality case). Assert within 5% in both.
4. Verify cross-validation against the Java reference implementation for a fixed seed
   and 10 000 values (golden value, computed once from reference impl).

**`test_exll_save_load`**

Build a sketch over 500 distinct values. Call `save_exll(sketch, path)`. Call
`load_exll(path)`. Verify the loaded sketch produces the same `n_distinct()` estimate
as the original (within float epsilon). Verify `load_exll(non_existent_path)` returns
`NULL_EXLL_SKETCH` without error.

**`test_exll_per_column_shard`**

1. Build an `ArenaZSetBatch` with schema `(pk: U128, col_i64: I64, col_u64: U64,
   col_str: STRING)`. Insert 200 rows:
   - `col_i64`: 100 distinct values (values 0–99, each appearing in 2 rows).
   - `col_u64`: 200 distinct values.
   - `col_str`: 50 distinct values.
   - Mix: 10 rows with weight -1 (tombstones); their `col_i64` values do not appear
     in any positive-weight row.
2. `write_batch_to_shard(batch, path, table_id=1)`.
3. Assert sidecar files: `.exll_c1` exists (I64), `.exll_c2` exists (U64),
   `.exll_c3` absent (STRING).
4. Construct `ShardHandle(path, schema, ...)`.
5. `h.n_distinct(0)` (PK) → -1.0.
6. `h.n_distinct(1)` (I64) → within 10% of 100 (tombstone rows excluded).
7. `h.n_distinct(2)` (U64) → within 10% of 200.
8. `h.n_distinct(3)` (STRING) → -1.0.
9. Call `h.close()` — no memory leak (verified by running under valgrind or ASAN in
   the translation test mode if available; at minimum verify no assertion failure).

**`test_exll_weight_filtering`**

Focused test for the Z-Set weight fix. Build two sketches over the same column data:
one that includes all rows, one built via `build_exll` (which filters weight ≤ 0).
The batch contains 50 rows with weight +1 (values 0–49) and 20 rows with weight -1
(values 50–69, not present anywhere with positive weight). Assert that `build_exll`
produces an estimate close to 50, and that the naive (all-rows) estimate is higher
(closer to 70). This confirms the weight-filter is actually excluding tombstones.

---

## What This Enables (Next Steps)

With per-column `n_distinct` available via `ShardHandle.n_distinct`, the following
become possible in priority order:

- **`APPROX_COUNT_DISTINCT`** (`optimizations.md` §25): reuse the same `ExllSketch`
  as an aggregate function accumulator — `step(val)` calls `exll_add`; `get_value`
  calls `exll_estimate`. Restricted to append-only sources initially. This is the
  first consumer and has no further prerequisites.

- **Exchange shard key selection**: compare `n_distinct(col) / row_count` across
  candidate shard columns to select one with high cardinality. Immediately usable in
  the multi-worker exchange path without a query planner.

- **Join cardinality estimation** (`optimizations.md` §15): for FK-column and
  indexed-column joins, `n_distinct(join_col)` in the source table is the per-table
  input to join cardinality estimates. For output size estimation, combine with Theta
  sketch intersection cardinality (measures `|distinct(L.join_col) ∩ distinct(R.join_col)|`
  directly). Requires the logical plan layer as a prerequisite before it drives any
  decisions.

- **Filter selectivity**: `1.0 / n_distinct` under the uniform assumption; combine
  with min/max for range selectivity. Requires predicate pushdown to the storage layer
  before it is actionable at the shard level.

- **Zone map investment guide** (`optimizations.md` §24): `shard_n_distinct /
  global_n_distinct` measures column-PK correlation; only build zone maps for columns
  where this ratio is low. Requires zone map infrastructure to exist first.
