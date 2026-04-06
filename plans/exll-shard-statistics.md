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

The SQL planner (`crates/gnitz-sql/src/planner.rs`) builds joins via MAP+reindex:
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

`clz64(x)`: use `x.leading_zeros()` in Rust.
The `h | mask` trick ensures the argument is never zero.

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

```rust
fn packed_get(buf: &[u8], idx: usize) -> u32 {
    let bit_off = idx * 28;
    let byte_off = bit_off / 8;
    let bit_shift = bit_off & 7;
    // Read 5 bytes (40 bits covers a 28-bit window at any alignment)
    let mut raw: u64 = 0;
    for i in 0..5 {
        raw |= (buf[byte_off + i] as u64) << (i * 8);
    }
    ((raw >> bit_shift) & 0xFFF_FFFF) as u32
}

fn packed_set(buf: &mut [u8], idx: usize, val: u32) {
    let bit_off = idx * 28;
    let byte_off = bit_off / 8;
    let bit_shift = bit_off & 7;
    let mask: u64 = 0xFFF_FFFF << bit_shift;
    let mut raw: u64 = 0;
    for i in 0..5 {
        raw |= (buf[byte_off + i] as u64) << (i * 8);
    }
    raw = (raw & !mask) | (((val as u64) & 0xFFF_FFFF) << bit_shift);
    for i in 0..5 {
        buf[byte_off + i] = ((raw >> (i * 8)) & 0xFF) as u8;
    }
}
```

State size in bytes: `STATE_BYTES = (m * 28 + 7) / 8 = 14336` for `p=12`.
Backed by `Vec<u8>` with `vec![0u8; STATE_BYTES]`.

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

```rust
for i in 0..count {
    let w = read_i64_le(weight_data, i * 8);
    if w > 0 {
        let h = xxh3_hash(&col_data[i * stride..(i + 1) * stride]);
        exll_add(&mut registers, h);
    }
}
```

`weight_data` is region 2 (weight is always region index 2).

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

### 1. `crates/gnitz-engine/src/exll.rs` (new, ~350 lines)

New module implementing the ExaLogLog sketch.

```rust
const STATE_BYTES: usize = 14336;  // (4096 × 28 + 7) / 8
const M: usize = 4096;             // 2^P registers
const T: u32 = 2;
const D: u32 = 20;
const P: u32 = 12;
const BIAS_CORRECTION: f64 = 0.48147289716;

pub struct ExllSketch {
    registers: Vec<u8>,  // STATE_BYTES packed 28-bit registers
}

impl ExllSketch {
    pub fn new() -> Self { ... }
    pub fn add(&mut self, h: u64) { ... }
    pub fn merge(&mut self, other: &ExllSketch) { ... }
    pub fn estimate(&self) -> f64 { ... }
}

// Packed 28-bit array access
fn packed_get(buf: &[u8], idx: usize) -> u32 { ... }
fn packed_set(buf: &mut [u8], idx: usize, val: u32) { ... }

// MLE estimator internals
fn contribute(r: u32, b_counts: &mut [u32; 64]) -> u64 { ... }
fn solve_mle(a: f64, b_counts: &[u32; 64], j: u32) -> f64 { ... }

// Build from column data, skipping weight <= 0 rows
pub fn build_exll(col_data: &[u8], stride: usize, count: usize,
                  weight_data: &[u8]) -> Option<ExllSketch> { ... }

// File I/O: 8-byte header (u32 m + u32 p) + STATE_BYTES
pub fn save_exll(sketch: &ExllSketch, path: &str) -> std::io::Result<()> { ... }
pub fn load_exll(path: &str) -> Option<ExllSketch> { ... }
```

Hashing: use `xxhash_rust::xxh3::xxh3_64(&col_data[..stride])`.

### 2. `crates/gnitz-engine/src/shard_file.rs` — build/save in flush path

After `build_xor8` / `save_xor8`, iterate non-PK non-STRING columns and
call `build_exll` + `save_exll` for each. The schema and weight region
are already available in the flush context.

### 3. `crates/gnitz-engine/src/shard_reader.rs` — load + n_distinct

On `MappedShard::open()`, load per-column `.exll_cN` sidecar files.
Store `Vec<Option<ExllSketch>>` indexed by column.
Add `pub fn n_distinct(&self, col_idx: usize) -> f64` returning
the estimate or `-1.0` for absent sketches.

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

Each item is one commit. Run `cargo test -p gnitz-engine` after each.

```
[1] exll.rs — packed array helpers + add + merge
[2] exll.rs — contribute + solve_mle + estimate
[3] exll.rs — build_exll + save_exll + load_exll
[4] shard_file.rs + shard_reader.rs — build/save in flush, load + n_distinct
```

Splitting [1] and [2] avoids debugging the complex MLE estimator while the
packed array and add/merge logic is still in flux.

---

## Tests

All new tests go in `crates/gnitz-engine/src/exll.rs` as `#[cfg(test)] mod tests`.

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
