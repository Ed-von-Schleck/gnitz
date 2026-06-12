# Eliminate the column-kind partial-function class via a sum type

## Problem

`SchemaDescriptor` exposes two **partial functions** keyed on a logical column
index, each valid only for one kind of column:

- `payload_idx(col_idx) -> usize` (`schema.rs:293`) — valid **only** for payload
  columns. For a PK column it `debug_assert!`s and, in release, returns
  `payload_mapping[col_idx]` which is `PAYLOAD_MAPPING_PK_SENTINEL` (`u8::MAX`)
  widened to `usize`. That value is then used as a null-bitmap shift
  (`1u64 << 255` → defined-but-garbage) or a payload-region column index
  (out-of-range read). Silent corruption, not a crash.
- `pk_byte_offset(col_idx) -> u8` (`schema.rs:334`) — valid **only** for PK
  columns. For a payload column it `debug_assert!`s, then walks `pk_columns()`,
  finds no match, and hits `unreachable!()` in debug; in release the walk
  returns a stale `off` (the full PK stride) with no error.

Both are guarded by `debug_assert!`, so the failure mode **differs between
build profiles**: a panic with a useful message in debug, silent wrong-offset
arithmetic in release. The correctness of every call site rests on an unwritten
precondition ("caller must ensure `col_idx` is/ is not a PK column," stated only
in the doc comments) that the compiler does not check.

The recurring shape at the call sites makes the hazard concrete. Multiple sites
have independently reinvented the same product-of-(bool, overloaded-int) to
paper over the partiality:

- `ops/util.rs:150-165` — `IndexColExtractor { is_pk: bool, off_or_pi: usize }`
  with the comment *"PK byte offset when `is_pk`, else the dense payload slot
  index."*
- `ops/reduce/agg.rs:95-101` — `is_pk_col: bool` + `pi_or_pk_off: u8`, same
  overloading.
- `dag.rs:1453-1466` — `is_pk_col` + `src_payload_idx` (with a `usize::MAX`
  sentinel for the PK case) + a separately-computed `src_pk_field_off`.
- The FK enforcement sites (`runtime/master.rs:1242,1249,1383`,
  `catalog/validation.rs:160,161`) compute **both** offsets eagerly across an
  `if is_pk_col { … } else { … }`, which is precisely how the
  `payload_idx(pk_col)` panic was introduced and shipped to a debug e2e run.

Each such site is a hand-rolled, unchecked sum type. The compiler permits
constructing the payload index for a PK column and vice versa; nothing ties the
offset to the column kind it belongs to.

This plan replaces the partial functions with a **total** classification that
returns a sum type carrying the correct offset for whichever kind the column
is, so that obtaining a wrong-kind offset becomes unrepresentable.

## Goal and non-goals

**Goal.** Make it a compile-time impossibility to (a) compute a payload index
for a PK column or a PK byte offset for a payload column, and (b) use a PK byte
offset where a payload index is expected (or vice versa). Remove the silent
release-mode corruption path entirely.

**Non-goals.** This plan does not change the on-row byte layout, the
`payload_mapping`/`payload_to_ci` precomputation, the wire format, or any
storage path. It does not touch `pk_index_single` (a separate boundary canary
with its own documented hard-assert rationale, `schema.rs:258-263`). It does not
introduce the index-PK-width admission gate (a distinct invariant).

## Design

### Layer 1 — `ColumnLocation` sum type + total `locate()`

Add to `schema.rs`:

```rust
/// Where a logical column's value lives within a row. Total over every
/// `col_idx < num_columns`: exactly one variant applies, and each carries the
/// offset that is meaningful for that kind only. Replaces the partial pair
/// (`payload_idx`, `pk_byte_offset`) + an `is_pk_col` branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnLocation {
    /// Column is part of the packed PK region. `byte_offset` is its start
    /// within `get_pk_bytes(row)`; pair with `columns[col_idx].size()`.
    Pk { byte_offset: u8 },
    /// Column is in the payload region. `idx` is the dense payload slot used
    /// for `col_data(idx)` and as the null-bitmap bit position.
    Payload { idx: u8 },
}

impl SchemaDescriptor {
    /// Classify `col_idx` and return its kind-specific offset. Total: every
    /// in-range column resolves to exactly one variant. O(pk_count) for the
    /// `Pk` arm (walks `pk_columns()` to sum widths), O(1) for `Payload`.
    #[inline]
    pub fn locate(&self, col_idx: usize) -> ColumnLocation {
        debug_assert!(col_idx < self.num_columns(), "locate: col_idx out of range");
        let m = self.payload_mapping[col_idx];
        if m == PAYLOAD_MAPPING_PK_SENTINEL {
            ColumnLocation::Pk { byte_offset: self.pk_byte_offset_internal(col_idx) }
        } else {
            ColumnLocation::Payload { idx: m }
        }
    }
}
```

`pk_byte_offset_internal` is the existing `pk_byte_offset` body, made private.
The single public, total entry point is `locate`. The only remaining
domain assumption (`col_idx < num_columns`) is a real total-function bound, not
a column-kind precondition, and is identical for both arms.

`Payload.idx` is `u8` to match `payload_mapping`'s element type and the existing
`SortDesc::pi` / `payload_mapping_byte` encoding; the null-bitmap shift and
`col_data` index both take it via `as usize` at the use site, exactly as today.

### Layer 2 — newtype offsets (prevents *misuse* of a correct offset)

Layer 1 stops you from computing the wrong-kind offset. It does not stop you
from taking a correctly-computed `Payload.idx` and feeding it to a function
expecting a PK byte offset. To close that, wrap the two integers so they are not
interchangeable and not raw `usize`/`u8`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadIdx(u8);   // dense payload slot / null-bit position
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PkByteOffset(u8); // start within the packed PK region

impl PayloadIdx {
    #[inline] pub fn get(self) -> usize { self.0 as usize }
    #[inline] pub fn null_mask(self) -> u64 { 1u64 << self.0 }
}
impl PkByteOffset {
    #[inline] pub fn get(self) -> usize { self.0 as usize }
}
```

`ColumnLocation::Payload` then carries `PayloadIdx`, `Pk` carries
`PkByteOffset`. `null_mask()` centralizes the `1u64 << pi` shift so the
`<< u8::MAX` footgun cannot recur even by hand. Downstream APIs that consume an
offset (e.g. `MemBatch::get_col_ptr`, null-word tests) take the newtype, so a
mix-up is a type error.

Layer 2 is higher-churn (every consumer signature that currently takes a bare
`usize` payload index changes). It is sequenced last and can be deferred without
losing Layer 1's primary guarantee. Recommendation: land Layer 1 fully; adopt
Layer 2 opportunistically as each consumer is touched, with the newtypes
introduced up front so new code uses them.

### Layer 3 — seal the partial functions

Once `locate()` exists and call sites migrate:

1. Make `pk_byte_offset` private (`pk_byte_offset_internal`), reachable only
   through `locate`.
2. Either (a) delete `payload_idx`, or (b) keep it as a thin wrapper that
   `match`es `locate()` and **hard-`assert!`s** the `Pk` arm — mirroring the
   release-active canary philosophy already documented on `pk_index_single`
   (`schema.rs:253-257`): a `debug_assert!` that compiles out and ships silent
   corruption is the exact hazard being removed, so any residual direct caller
   must fail loudly in release too. Prefer (a); use (b) only if a genuinely
   payload-only boundary caller cannot cleanly take a `PayloadIdx`.

`is_pk_col` stays — it is a legitimate total predicate and several sites need
the boolean without the offset (e.g. `pk_is_fast`-style decisions).
`payload_mapping_byte` stays for the `SortDesc::pi` fast-path that already reads
the precomputed sentinel-or-index byte directly.

## Migration

Two distinct call-site classes:

### Class A — known-payload, by construction (no change needed)

Sites that obtain `col_idx` from `payload_columns()` (or otherwise already know
the column is payload) call `payload_idx` safely. These are the iteration
bodies in `ops/util.rs`, `ops/reduce/*`, `dag.rs` projection loops, `dml.rs`
payload assembly, `gnitz-py`/`wal_block` decoders, `compiler.rs`,
`storage/read_cursor.rs`, `storage/compact.rs`, `ops/linear.rs`,
`ops/exchange.rs` payload arms, and `runtime/master.rs:986,1146,1454`. If Layer
2 lands, `payload_columns()` yields a `PayloadIdx` and these are mechanical;
otherwise they are untouched. They are **not** the hazard and do not gate this
plan.

### Class B — arbitrary-kind extraction (the hazard; migrate all)

Sites where `col_idx` arrives from outside (a FK referenced column, an index
source column, an aggregate/sort key) and may be either kind. Every one of these
currently branches on `is_pk_col` and computes one or both offsets. Convert each
to a single `match schema.locate(col_idx)`.

Inventory (the `if is_pk_col { pk_byte_offset } else { payload_idx }` shape):

| Site | Current construct |
|---|---|
| `ops/util.rs:156-165` | `IndexColExtractor { is_pk, off_or_pi }` |
| `ops/util.rs:284-336` | repeated `is_pk_col` + dual offset (3 occurrences) |
| `ops/reduce/agg.rs:95-101` | `Accumulator { is_pk_col, pi_or_pk_off }` |
| `ops/reduce/sort.rs:103,135` | paired `pk_byte_offset` / `payload_idx` |
| `ops/exchange.rs:42,47` | paired |
| `ops/index.rs:82,127,129` | `gi_is_pk`/`avi` PK-vs-payload offset selection |
| `dag.rs:1453-1485` | `build_index_batch` dual offset + null bit |
| `runtime/master.rs:1242,1249` | FK parent-DELETE RESTRICT key derivation |
| `runtime/master.rs:1383` | FK UPDATE-RESTRICT retired-value derivation |
| `catalog/validation.rs:160,161` | single-worker FK RESTRICT |
| `sql/dml.rs:958,970,1057,1065,1194,1244` | UPDATE/DELETE key + payload paths |
| `gnitz-py/src/lib.rs:344,680,767,1090` | binding-layer cell extraction |

Canonical transform — the manual sum type collapses to the real one. Before:

```rust
struct IndexColExtractor { is_pk: bool, off_or_pi: usize, size: usize }
let is_pk = schema.is_pk_col(col_idx);
let off_or_pi = if is_pk { schema.pk_byte_offset(col_idx) as usize }
                else     { schema.payload_idx(col_idx) };
// per row:
let bytes = if self.is_pk { &mb.get_pk_bytes(row)[self.off_or_pi..self.off_or_pi+self.size] }
            else          { mb.get_col_ptr(row, self.off_or_pi, self.size) };
```

After:

```rust
struct IndexColExtractor { loc: ColumnLocation, size: usize }
let loc = schema.locate(col_idx);
// per row:
let bytes = match self.loc {
    ColumnLocation::Pk { byte_offset } => {
        let o = byte_offset as usize;
        &mb.get_pk_bytes(row)[o..o + self.size]
    }
    ColumnLocation::Payload { idx } => mb.get_col_ptr(row, idx as usize, self.size),
};
```

The classification happens once (cache the `ColumnLocation` exactly as the
`bool`+`usize` were cached); the per-row `match` lowers to the same branch as
the per-row `if self.is_pk`, so there is no hot-loop regression. The payload
null-mask, where present, becomes `idx.null_mask()` (Layer 2) or
`1u64 << (idx as u64)` (Layer 1).

For the FK extraction sites the two arms already do *materially different* work
(PK value is on the wire / in the packed PK; non-PK value needs storage
resolution because a DELETE carries no payload). The `match` is therefore not a
mechanical offset swap but the natural control structure, and it makes the
"forgot one arm" / "computed the payload index for a PK column" mistakes
unrepresentable.

## Hot-path and performance

- `locate()` is `#[inline]`; the `Pk` arm is O(pk_count) (≤5) and runs once per
  extractor construction, never per row — identical to today's
  `pk_byte_offset`.
- A stored `ColumnLocation` is a 2-byte value (tag + `u8`); matching it per row
  is one branch, the same code today's `bool` produces. No added per-row cost.
- `null_mask()` and `PayloadIdx::get()` are `#[inline]` and compile to the
  current shift/cast.

## Wide-PK interaction

`ColumnLocation::Pk.byte_offset` indexes into `get_pk_bytes(row)`, which is
valid for both narrow and wide PK regions (the raw-bytes accessor, not the
`u128` fast path). This is the same slice the current `pk_byte_offset` callers
use, so wide compound PKs are handled identically. `locate` makes no
narrow-vs-wide assumption.

## Testing

1. **Totality / agreement property test** (`schema.rs` tests): for a generated
   space of schemas (vary `num_columns` 1..=MAX, `pk_count` 1..=MAX_PK,
   PK-column positions, column widths), assert for every `col_idx` that
   `locate` agrees with the legacy mapping:
   - `Payload { idx }` ⟺ `!is_pk_col` and `idx == payload_mapping[col_idx]`;
   - `Pk { byte_offset }` ⟺ `is_pk_col` and `byte_offset` equals the prefix-sum
     of PK-column widths in pk-list order.
   This pins the new primitive against the existing one before the old one is
   removed.
2. **Exhaustive small-schema check**: for every schema with ≤4 columns and every
   PK subset, `locate` returns exactly one variant for each column and the
   `Payload.idx` set equals `0..num_payload_cols`.
3. **No-`unwrap`/no-sentinel grep gate**: after migration, a test (or CI grep)
   asserting zero remaining `payload_idx(` calls outside Class-A loops and zero
   `PAYLOAD_MAPPING_PK_SENTINEL` comparisons outside `schema.rs`/`locate`. This
   prevents regression to the hand-rolled pattern.
4. Existing engine + e2e suites must stay green at each phase (the migration is
   behavior-preserving by construction).

## Sequencing

1. **Phase 1 — primitive.** Add `ColumnLocation` + `locate()` + the totality
   property tests. No call-site changes. Lands independently; nothing depends on
   the old functions being gone.
2. **Phase 2 — Class B migration.** Convert every arbitrary-kind site (table
   above) to `match locate()`, including the FK sites and the three hand-rolled
   sum types (`IndexColExtractor`, `Accumulator`, `build_index_batch`). Each
   site is behavior-preserving and independently testable.
3. **Phase 3 — seal.** Make `pk_byte_offset` private; delete `payload_idx` or
   reduce it to a hard-`assert!` wrapper. Add the grep gate test. After this,
   the silent release-mode wrong-offset path no longer exists.
4. **Phase 4 — newtypes (optional, opportunistic).** Introduce `PayloadIdx` /
   `PkByteOffset`, thread them through `ColumnLocation` and the consumer
   signatures as those consumers are touched. Convert Class-A `payload_columns()`
   to yield `PayloadIdx`. Highest churn, lowest marginal risk; safe to spread
   over time.

Phases 1–3 deliver the core guarantee and remove the corruption path. Phase 4
is the "most sound" completion that also prevents cross-using a correct offset.

## Risks

- **Churn in Phase 2** touches correctness-critical extraction loops
  (`build_index_batch`, agg/sort accumulators, FK validation). Mitigated by the
  Phase-1 agreement property test (proves `locate` ≡ old mapping) and by keeping
  each conversion behavior-preserving; review per-site that the `Pk`/`Payload`
  arms reproduce the exact prior byte/index arithmetic.
- **`u8` payload index ceiling.** `Payload.idx: u8` matches the existing
  `payload_mapping` element type and `MAX_COLUMNS`; no new ceiling is
  introduced, but the newtype makes the width explicit and should assert
  `payload count ≤ u8::MAX` where the mapping is built (already implied by
  `MAX_COLUMNS`).
- **Phase 4 signature churn** can ripple through batch accessors. Bounded by
  doing it consumer-by-consumer; the enum in Phases 1–3 already carries the
  newtypes' values as plain integers, so Phase 4 is purely a wrapping pass.
