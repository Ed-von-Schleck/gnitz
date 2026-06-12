# Assorted optimizations

Six independent items. Each section stands on its own — pick them off in
any order. Nothing here depends on anything else here.

---

## 1. Point seek with residual filter

`dml::execute_select` falls back to a full table scan when `WHERE`
contains both a complete PK equality chain and an extra non-PK
predicate, e.g. `WHERE pk_a = 1 AND pk_b = 2 AND status = 'x'`. The PK
prefix would resolve to a single row, but `try_extract_pk_seek`
(`gnitz-sql/src/dml.rs:724`) returns `None` as soon as `walk_and_tree`
sees a non-PK term.

Change the seek extractor to peel off the PK terms and return whatever
remains as a residual filter expression. The seek hits one row (or
none); the residual is applied client-side to that single row. Cost: a
boolean evaluation on at most one row; reward: one row read instead of
a full delta scan.

### Sketch

```rust
struct PkSeekMatch {
    pk:       PkTuple,
    residual: Option<Expr>,
}

fn try_extract_pk_seek_with_residual(
    expr: &Expr, schema: &Schema,
) -> Option<PkSeekMatch> {
    let stride = schema.pk_stride() as u8;
    let mut tuple = PkTuple::new(stride);
    let mut slot_set = [false; gnitz_core::MAX_PK_COLUMNS];
    let mut bound = 0usize;
    let mut residual_terms: Vec<Expr> = Vec::new();

    let ok = walk_and_tree_split(expr, schema, &mut |term| -> TermClass {
        match try_col_eq_literal(term, schema) {
            Some((col_idx, val)) => {
                // Locate pk_pos / byte offset in one pass.
                let mut pk_pos: Option<usize> = None;
                let mut off = 0usize;
                for (i, &pi) in schema.pk_indices().iter().enumerate() {
                    if pi == col_idx { pk_pos = Some(i); break; }
                    off += schema.columns[pi].type_code.wire_stride();
                }
                let Some(pk_pos) = pk_pos else { return TermClass::Residual };
                if slot_set[pk_pos] { return TermClass::Reject; }
                slot_set[pk_pos] = true;
                bound += 1;
                let s = schema.columns[col_idx].type_code.wire_stride();
                tuple.buf[off..off + s].copy_from_slice(&val.to_le_bytes()[..s]);
                TermClass::PkBind
            }
            None => TermClass::Residual,
        }
    }, &mut residual_terms);

    if !ok || bound != schema.pk_count() { return None; }
    let residual = match residual_terms.len() {
        0 => None,
        1 => Some(residual_terms.pop().unwrap()),
        _ => Some(fold_and(residual_terms)),
    };
    Some(PkSeekMatch { pk: tuple, residual })
}
```

`walk_and_tree_split` mirrors today's `walk_and_tree` but classifies
each leaf instead of failing on the first non-PK term. `Reject` aborts
(duplicate PK binding); `Residual` accumulates the term for
post-filtering; `PkBind` updates the tuple. `fold_and` rebuilds an
`AND` tree from the leftover predicates.

In `execute_select`, after the seek returns its single-row batch,
compile the residual into an `ExprProgram` and run `apply_residual_filter`
(the helper already exists for the index-seek path at `dml.rs:677`).

### Tests

- `WHERE pk = 1 AND status = 'x'` → seek + residual; matches exactly the
  rows the full-scan path matches.
- `WHERE pk_a = 1 AND pk_b = 2 AND non_pk = 0.5` → same.
- `WHERE pk = 1 AND pk = 2` → still `None` (duplicate PK binding).
- `WHERE non_pk_a = 1 AND non_pk_b = 2` → falls through to index/scan
  path unchanged (no PK term).
- `WHERE pk = 1 OR status = 'x'` → `None`; OR cannot factor through.

---

## 2. Collapse `PkColumn` variants

`PkColumn` in `gnitz-core/src/protocol/types.rs:156` has three variants
(`U64s`, `U128s`, `Bytes`) for what is physically the same thing: a
contiguous LE byte buffer with a per-row stride. Every accessor
(`len`, `get`, `push_u128`, `push_bytes`, `swap`, `clear`, `truncate`,
`push_from`, `to_vec_u128`, `extend_from`) carries a three-arm match;
`push_from` and `extend_from` carry a 3x3 cross-product that panics
on variant mismatch.

Replace with a single struct that stores stride explicitly:

```rust
pub struct PkColumn {
    stride: u8,
    buf:    Vec<u8>,
}

impl PkColumn {
    pub fn empty_for_schema(schema: &Schema) -> Self {
        Self { stride: schema.pk_stride() as u8, buf: Vec::new() }
    }
    pub fn len(&self) -> usize { self.buf.len() / self.stride as usize }
    pub fn stride(&self) -> u8 { self.stride }

    pub fn get_bytes(&self, i: usize) -> &[u8] {
        let s = self.stride as usize;
        &self.buf[i * s..i * s + s]
    }
    /// Project to u128. Only valid when stride <= 16.
    pub fn get(&self, i: usize) -> u128 {
        debug_assert!(self.stride as usize <= 16);
        let mut b = [0u8; 16];
        b[..self.stride as usize].copy_from_slice(self.get_bytes(i));
        u128::from_le_bytes(b)
    }
    pub fn push_bytes(&mut self, b: &[u8]) {
        debug_assert_eq!(b.len(), self.stride as usize);
        self.buf.extend_from_slice(b);
    }
    pub fn push_u128(&mut self, v: u128) {
        debug_assert!(self.stride as usize <= 16);
        self.buf.extend_from_slice(&v.to_le_bytes()[..self.stride as usize]);
    }
    pub fn push_from(&mut self, src: &PkColumn, i: usize) {
        debug_assert_eq!(self.stride, src.stride);
        self.buf.extend_from_slice(src.get_bytes(i));
    }
    pub fn swap(&mut self, i: usize, j: usize) {
        if i == j { return; }
        let s = self.stride as usize;
        let (lo, hi) = if i < j { (i, j) } else { (j, i) };
        let (left, right) = self.buf.split_at_mut(hi * s);
        left[lo * s..lo * s + s].swap_with_slice(&mut right[..s]);
    }
    pub fn clear(&mut self) { self.buf.clear(); }
    pub fn truncate(&mut self, n: usize) { self.buf.truncate(n * self.stride as usize); }
    pub fn extend_from(&mut self, other: &PkColumn) {
        debug_assert_eq!(self.stride, other.stride);
        self.buf.extend_from_slice(&other.buf);
    }
}
```

Notes / risks:

- The variant-mismatch panic paths (`push_from`, `extend_from`) become
  a single `debug_assert_eq!(stride)`. Existing call sites already
  uphold this invariant.
- `PkColumn::for_type(tc)` (`types.rs:169`) becomes `PkColumn { stride:
  tc.wire_stride() as u8, buf: vec![] }` — single source of truth via
  `empty_for_schema`.
- `PkColumn::to_vec_u128` keeps its `panic!` for wide PKs; the
  `debug_assert!(stride <= 16)` in `get` documents the contract.
- `PartialEq<Vec<u128>>` impl at `types.rs:416` is a tests-only
  convenience; can keep with `debug_assert!(self.stride <= 16)`.
- Wire codec call sites that branched on the variant (`wal_block.rs`,
  `codec.rs`) collapse to a single `extend_from_slice` of the stride
  bytes per row.

### Tests

- The full existing `ZSetBatch` / `BatchAppender` test suite
  (`protocol/types.rs` `tests` module) passes unchanged.
- Roundtrip: build a batch with `U64`, `U128`, and `(U64,U64)` PKs and
  check `get`, `get_bytes`, `push_from`, `swap`, `extend_from`.

---

## 3. Hoist enum dispatch out of catalog row loops

`gnitz-core/src/client.rs::extract_col_entries` (line 639) does seven
`col_u64`/`col_str` calls per row. Each one does
`match &col.columns[k] { ColData::Fixed(buf) => ... }` plus a bounds
check. For a schema with thousands of columns, the per-row dispatch
dominates over the actual bytes touched. Same pattern in
`find_table_record`, `find_view_record`, `find_schema_id`.

Resolve the column variant once outside the loop, take a typed slice,
and index it directly.

### Sketch

```rust
fn extract_col_entries(
    col_batch:  &ZSetBatch,
    owner_id:   u64,
    owner_kind: u64,
) -> Result<Vec<ColumnDef>, ClientError> {
    let owner_ids   = fixed_slice_u64(&col_batch.columns[1])?;
    let owner_kinds = fixed_slice_u64(&col_batch.columns[2])?;
    let col_idxs    = fixed_slice_u64(&col_batch.columns[3])?;
    let names       = strings_slice(&col_batch.columns[4])?;
    let tc_vals     = fixed_slice_u64(&col_batch.columns[5])?;
    let nullables   = fixed_slice_u64(&col_batch.columns[6])?;
    let fk_tids     = fixed_slice_u64(&col_batch.columns[7])?;
    let fk_cidxs    = fixed_slice_u64(&col_batch.columns[8])?;

    let mut entries: Vec<(u64, ColumnDef)> = Vec::new();
    for i in 0..col_batch.len() {
        if col_batch.weights[i] <= 0 { continue; }
        if owner_ids[i] != owner_id || owner_kinds[i] != owner_kind {
            continue;
        }
        let type_code = type_code_from_u64(tc_vals[i])
            .map_err(ClientError::Protocol)?;
        entries.push((col_idxs[i], ColumnDef {
            name:        names[i].clone().unwrap_or_default(),
            type_code,
            is_nullable: nullables[i] != 0,
            fk_table_id: fk_tids[i],
            fk_col_idx:  fk_cidxs[i],
        }));
    }
    entries.sort_by_key(|(idx, _)| *idx);
    Ok(entries.into_iter().map(|(_, c)| c).collect())
}

/// Borrow a `ColData::Fixed` payload column as a `&[u64]` view (LE).
fn fixed_slice_u64(col: &ColData) -> Result<U64Slice<'_>, ClientError> {
    match col {
        ColData::Fixed(buf) => Ok(U64Slice { buf }),
        _ => Err(ClientError::ServerError("expected Fixed column".into())),
    }
}
/// Index helper that reads bytes 8·i..8·(i+1) as `u64::from_le_bytes`.
struct U64Slice<'a> { buf: &'a [u8] }
impl<'a> std::ops::Index<usize> for U64Slice<'a> { /* ... */ }
```

`strings_slice` returns `&[Option<String>]` from `ColData::Strings`.
The same accessor pair is reused by `find_schema_id`,
`find_table_record`, `find_view_record`, `find_index_record`.

### Tests

- Existing client/integration tests cover the metadata scans; just
  rerun them.
- Microbench: `extract_col_entries` on a 1000-column COL_TAB batch
  before/after. Expect a meaningful drop in per-row time.

---

## 4. Coalesced key buffer for reduce sort

`ops/reduce.rs::argsort_delta` (line 576) has a fast path for a
single-column non-nullable integer group key that pre-extracts keys
into a dense `Vec<T>`. The general path falls through to
`compare_by_group_cols` (line 624), which on every comparison calls
`MemBatch::get_col_ptr` and `get_null_word` for each group column —
pointer-chasing through up to `MAX_COLUMNS` non-contiguous column
buffers per comparison, with `O(N log N)` comparisons.

Generalise the fast path: build one contiguous scratch buffer of
`stride`-byte keys (per row, in group-column order), then sort indices
by a single `[u8]::cmp` of the per-row slices.

### Sketch

```rust
fn argsort_delta_coalesced(
    batch: &Batch,
    schema: &SchemaDescriptor,
    descs: &[SortDesc],
) -> Vec<u32> {
    let mb = batch.as_mem_batch();
    let n  = batch.count;

    // Per-column: 1 tag byte (null=0x00, non-null=0x01) + stride bytes.
    let row_w: usize = descs.iter().map(|d| 1 + d.cs as usize).sum();
    let mut keys = vec![0u8; n * row_w];

    for row in 0..n {
        let null_word = mb.get_null_word(row);
        let row_off = row * row_w;
        let mut off = row_off;
        for d in descs {
            let is_null = if d.pi != PAYLOAD_MAPPING_PK_SENTINEL
                && (null_word >> d.pi) & 1 == 1
            { true } else { false };
            keys[off] = if is_null { 0 } else { 1 };          // NULLS FIRST
            off += 1;
            if !is_null {
                let cs = d.cs as usize;
                let src = if d.pi == PAYLOAD_MAPPING_PK_SENTINEL {
                    // PK region byte slice at this column's offset
                    &mb.pk_bytes(row)[d.pk_off..d.pk_off + cs]
                } else {
                    mb.get_col_ptr(row, d.pi as usize, cs)
                };
                // Encode the column bytes so a lexicographic `[u8]::cmp`
                // reproduces the typed ordering: see "Encoding" below.
                encode_key(&mut keys[off..off + cs], src, d.tc);
            }
            off += d.cs as usize;
        }
    }

    let mut indices: Vec<u32> = (0..n as u32).collect();
    indices.sort_unstable_by(|&a, &b| {
        let sa = &keys[a as usize * row_w..(a as usize + 1) * row_w];
        let sb = &keys[b as usize * row_w..(b as usize + 1) * row_w];
        sa.cmp(sb)
    });
    indices
}
```

### Encoding (load-bearing)

A bytewise `[u8]::cmp` is lexicographic big-endian. Column reads from
`MemBatch` are little-endian. So `encode_key` must transform the bytes
so that lex-order on the encoded bytes matches typed order:

- Unsigned ints: byte-reverse (LE → BE).
- Signed ints: byte-reverse, then flip the sign bit of the now-MSB byte.
- `F32`/`F64`: byte-reverse, then if the sign bit is set flip all
  bytes, else flip just the sign bit (standard IEEE-754 → unsigned
  monotone mapping).
- `U128`/`UUID`: byte-reverse the 16-byte LE struct.
- `String`/`Blob`: not supported by this fast path (variable width).
  Fall back to `compare_by_group_cols`. Today's path is already the
  fallback, so this is no regression.

Without this encoding the fast path silently produces wrong orderings
for signed types and floats — these would be the bugs hardest to
notice in production.

### Tests

- Argsort each TypeCode (U8/I8/.../U128/F32/F64) against the
  `compare_by_group_cols` reference. Same result.
- NULLs: nullable column with mixed NULL / non-NULL → NULLs sort first.
- Two-column key (`U64`, `I32`) with crossing orderings — fast path
  matches reference.
- Single-column String / Blob falls through to the existing path.

---

## 5. Inline German-string compare fast path

`schema::compare_german_strings` (line 535) always re-reads the tail
via `german_string_tail`. For short strings (length ≤
`SHORT_STRING_THRESHOLD`, i.e. 12 bytes) the entire string lives
inside the 16-byte struct itself; no blob deref is needed and the
length-then-prefix-then-suffix logic can fall out of a fixed-shape
inline compare.

### Layout reminder

Bytes 0..4: length (u32 LE). Bytes 4..8: first 4 chars. Bytes 8..16:
either chars 4..12 (when length ≤ 12) or the blob heap offset (when
length > 12). Trailing inline bytes past `length` are guaranteed zero
by `encode_german_string` (`gnitz-wire/src/lib.rs:903`).

### Sketch

```rust
const SHORT_STRING_THRESHOLD: usize = 12;

#[inline]
pub(crate) fn compare_german_strings(
    a: &[u8], blob_a: &[u8],
    b: &[u8], blob_b: &[u8],
) -> Ordering {
    let len_a = read_u32_le(a, 0) as usize;
    let len_b = read_u32_le(b, 0) as usize;

    // Inline fast path: both strings are entirely in their 16-byte
    // struct. The chars live at bytes [4..4+len_a] and [4..4+len_b];
    // bytes past len are zero (encoder invariant), so a single memcmp
    // of bytes [4..16] correctly orders the prefixes, and a length
    // tiebreak finishes the job. No blob reads, no branches per-byte.
    if len_a <= SHORT_STRING_THRESHOLD && len_b <= SHORT_STRING_THRESHOLD {
        // bytewise lex compare of [4..16] is the same as comparing
        // the actual chars [0..min(len)] because beyond-len bytes are
        // zero on both sides — so they agree until one side's chars
        // are exhausted.
        return match a[4..16].cmp(&b[4..16]) {
            Ordering::Equal => len_a.cmp(&len_b),
            ord             => ord,
        };
    }
    // ...existing path: prefix compare, then tail compare via
    //    german_string_tail (which dereferences blob for long strings).
    let min_len = len_a.min(b_len_or(len_b));
    let prefix_cmp = min_len.min(4);
    let ord = a[4..4 + prefix_cmp].cmp(&b[4..4 + prefix_cmp]);
    if ord != Ordering::Equal { return ord; }
    if min_len <= 4 { return len_a.cmp(&len_b); }
    let tail_a = german_string_tail(a, blob_a, len_a, min_len);
    let tail_b = german_string_tail(b, blob_b, len_b, min_len);
    match tail_a.cmp(tail_b) {
        Ordering::Equal => len_a.cmp(&len_b),
        ord             => ord,
    }
}
```

### What the audit suggested vs what works

Comparing the structs as `u128` (`u128::from_le_bytes(a[..16]).cmp(&b)`)
does **not** produce the right ordering — the LE conversion makes the
length word the *low* bits of the `u128` and the string bytes the
*high* bits, so a u128 compare orders by string-bytes-then-length but
treats those bytes as a little-endian integer, which is the reverse of
lex order. Compare the 12 char bytes directly via `[u8]::cmp` as
above; that's the correct fast path.

The `[4..16]`-as-key trick relies on the trailing-zero invariant.
That invariant is currently held by `encode_german_string`. Add a
debug-assert in the comparator to lock it in:

```rust
debug_assert!(
    a[4 + len_a..16].iter().all(|&b| b == 0),
    "german string inline tail not zero-padded"
);
```

### Tests

- All pairs of inline strings up to length 12 against
  `Vec<u8>::cmp(&Vec<u8>)`.
- Mixed inline vs long string — falls through to the slow path; same
  answer as today.
- Both long — unchanged path.
- Trailing-zero invariant: encode, mutate a trailing byte to non-zero,
  expect the debug-assert to fire.

---

## 6. Unify logical `Schema` and engine `SchemaDescriptor`

Two structures describe the same thing:

- `gnitz-core/src/protocol/types.rs::Schema` — heap-allocated
  (`Vec<ColumnDef>`, `Vec<usize>`); recomputes `pk_stride`, payload
  offsets, and PK byte offsets on every call by iterating
  `pk_indices()`.
- `gnitz-engine/src/schema.rs::SchemaDescriptor` — fixed-size
  arrays, `const fn` constructible, precomputes strides, payload
  mapping, and PK offsets at construction.

The two must agree on `pk_stride`, `pk_byte_offset`, payload index
mapping, and `group_cols_eq_pk` semantics or batches written by one
side will not decode by the other. The `Schema` helpers in
`protocol/types.rs:32-138` exist explicitly to mirror the engine's
precomputed equivalents.

### Goal

One precomputed structure shared by both crates; logical-name lookups
hang off it. Concretely:

```rust
// in gnitz-wire (the only crate both core and engine depend on)
pub struct SchemaDescriptor {
    columns:        [SchemaColumn; MAX_COLUMNS],
    num_columns:    u32,
    pk_count:       u32,
    pk_indices:     [u32; MAX_PK_COLUMNS],
    pk_stride:      u32,
    payload_mapping:[u32; MAX_COLUMNS],   // schema → payload, sentinel for PK
    pk_byte_offsets:[u32; MAX_COLUMNS],   // schema → offset within PK region
}

// gnitz-core: name table sitting next to the precomputed schema
pub struct Schema {
    pub descriptor:   SchemaDescriptor,
    pub column_names: Vec<String>,
    pub fk_table_ids: Vec<u64>,
    pub fk_col_idxs:  Vec<u64>,
    pub is_nullable:  Vec<bool>,
}
```

The engine continues to access only `descriptor`; the planner / wire
codec continue to access names and FK metadata via `Schema`. There's
no duplicated `pk_stride()` computation, no risk of drift.

### Why this is non-trivial

- `gnitz-wire` is the natural home for `SchemaDescriptor`, but the
  engine's `SchemaDescriptor::new` is `const fn` and depends on
  internal layout helpers. Moving it is plumbing.
- `Schema::columns` is currently `Vec<ColumnDef>`. Splitting metadata
  out of `ColumnDef` (because `type_code` and `is_nullable` now live
  in `descriptor.columns`) means touching every `Schema { columns:
  vec![...], pk_cols: vec![...] }` construction site — there are
  ~30 of them in tests and the catalog.
- The protocol message types (`gnitz-core/src/protocol/message.rs`)
  serialise `Schema`; the wire format must not change. Decoding into
  the new `Schema` shape rebuilds the descriptor from the columns
  array in one pass.
- `pk_indices`-based helpers on `Schema` (`pk_count`, `pk_stride`,
  `pk_byte_offset`, `payload_idx`, `group_cols_eq_pk`,
  `is_single_col_natural_pk`) become forwarders to the descriptor.

### Suggested ordering

1. Land the engine `SchemaDescriptor` move into `gnitz-wire` first,
   without changing `gnitz-core::Schema`. Both crates depend on the
   wire crate so this is a one-way refactor.
2. Add `Schema::descriptor` as a derived field, populated by a new
   `Schema::from_columns(cols, pk_indices)` constructor. Keep
   `Schema::columns: Vec<ColumnDef>` and `Schema::pk_cols:
   Vec<usize>` for now — both views point at the same data; helpers
   read from `descriptor`.
3. Eventually strip the redundant fields once all call sites read
   names and FKs through dedicated accessors.

Step 1 alone is worth landing. Steps 2–3 should be tackled only if
there is concrete evidence the redundant computation matters
(profile, don't speculate).

### Tests

- The existing `Schema` test suite in `protocol/types.rs` (the
  `tests` module, `test_pk_byte_offset`, `test_payload_idx`,
  `test_num_payload_cols`, etc.) passes against the descriptor-backed
  implementation.
- An assertion in `from_columns`: every `pk_byte_offset(pk_col)`
  matches the precomputed `descriptor.pk_byte_offsets[pk_col]`.
- Roundtrip: encode/decode a `Schema` via the protocol codec; the
  descriptor it reconstructs must equal the descriptor on the sender
  side.

---

## 7. Cap thread-local scatter-buffer capacity after large batches

`exchange.rs` pools two thread-local vectors to avoid per-call heap
allocation during scatter/merge operations:

```rust
thread_local! {
    static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
    static WORKER_ROWS:     RefCell<Vec<Vec<(u8, u32)>>> = const { RefCell::new(Vec::new()) };
}
```

`fill_worker_indices` clears the inner `Vec<u32>` buckets between
calls but does not release their capacity. A single large batch —
a bulk import or replay of 1M rows with 4 workers — causes each
inner `Vec<u32>` to grow to ~250 K entries (≈1 MB each, ≈4 MB total
per TLS pool). That capacity is permanently retained on the thread.
A server that alternates between bulk ingestion and steady-state
transactional traffic wastes memory for the lifetime of every worker
thread that has ever seen a large batch.

### Fix

In the clear loop inside `fill_worker_indices` and the corresponding
clear inside `relay_scatter_merge_walk`, shrink any inner vec whose
capacity exceeds a threshold:

```rust
const MAX_RETAIN_ROWS: usize = 65_536;   // 256 KB per bucket at u32

// fill_worker_indices:
out[..num_workers].iter_mut().for_each(|v| {
    v.clear();
    if v.capacity() > MAX_RETAIN_ROWS {
        v.shrink_to(MAX_RETAIN_ROWS);
    }
});

// relay_scatter_merge_walk:
worker_rows[..num_workers].iter_mut().for_each(|v| {
    v.clear();
    if v.capacity() > MAX_RETAIN_ROWS {
        v.shrink_to(MAX_RETAIN_ROWS);
    }
});
```

`shrink_to` is a no-op when the allocator cannot shrink in-place
(capacity stays at the current value until the vector is dropped);
it is never an allocation in the steady state. The threshold
(65 536 u32s = 256 KB per bucket) accommodates any realistic batch
without releasing capacity that will be needed again immediately.

### Tests

- No new tests required: existing scatter and relay-walk tests
  continue to pass.
- Optional: a test that processes a 100 000-row batch, clears, and
  asserts `SCATTER_INDICES` inner-vec capacities are ≤ MAX_RETAIN_ROWS.
