# Convert `found_col_ptr` / `found_blob_ptr` to lifetime-bound slices

`MemTable`, `Table`, and `PartitionedTable` expose two pointer-returning
methods:

```rust
pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8;
pub fn found_blob_ptr(&self) -> *const u8;
```

The single production caller, `Batch::append_row_from_ptable_found`
(`batch.rs:1117`), immediately wraps both pointers in
`unsafe { std::slice::from_raw_parts(...) }` after asserting non-null.
The raw pointer return type provides no lifetime tracking: a caller that
stored the pointer and later mutated or dropped the table would produce a
use-after-free. Rust cannot catch that. Converting to `&[u8]` ties the
slice lifetime to the immutable borrow of `self`, so the compiler
enforces the contract.

`table.rs::read_found_u128` is a second caller that checks for null to
distinguish "row not found" from "row found". With the `&[u8]` API, an
empty slice serves as the "not found" sentinel — valid because every
column has stride ≥ 1 (`U8` is the minimum), so a live column slice
is never empty.

## Touchpoints

`crates/gnitz-engine/src/storage/memtable.rs`:

```rust
// Before:
pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8 {
    match self.found_entry() {
        Some((run, row)) => run.get_col_ptr(row, payload_col, col_size).as_ptr(),
        None => std::ptr::null(),
    }
}
pub fn found_blob_ptr(&self) -> *const u8 {
    match self.found_entry() {
        Some((run, _)) => run.blob.as_ptr(),
        None => std::ptr::null(),
    }
}

// After:
pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> &[u8] {
    match self.found_entry() {
        Some((run, row)) => run.get_col_ptr(row, payload_col, col_size),
        None => &[],
    }
}
pub fn found_blob_ptr(&self) -> &[u8] {
    match self.found_entry() {
        Some((run, _)) => &run.blob,
        None => &[],
    }
}
```

`crates/gnitz-engine/src/storage/table.rs`:

```rust
// Before:
pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8 {
    match &self.found_source {
        FoundSource::MemTable        => self.memtable.found_col_ptr(payload_col, col_size),
        FoundSource::Shard(arc, idx) => arc.get_col_ptr(*idx, payload_col, col_size).as_ptr(),
        FoundSource::None            => std::ptr::null(),
    }
}
pub fn found_blob_ptr(&self) -> *const u8 {
    match &self.found_source {
        FoundSource::MemTable        => self.memtable.found_blob_ptr(),
        FoundSource::Shard(arc, _)   => arc.blob_slice().as_ptr(),
        FoundSource::None            => std::ptr::null(),
    }
}

// After:
pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> &[u8] {
    match &self.found_source {
        FoundSource::MemTable        => self.memtable.found_col_ptr(payload_col, col_size),
        FoundSource::Shard(arc, idx) => arc.get_col_ptr(*idx, payload_col, col_size),
        FoundSource::None            => &[],
    }
}
pub fn found_blob_ptr(&self) -> &[u8] {
    match &self.found_source {
        FoundSource::MemTable        => self.memtable.found_blob_ptr(),
        FoundSource::Shard(arc, _)   => arc.blob_slice(),
        FoundSource::None            => &[],
    }
}
```

`table.rs::read_found_u128` uses the null check as a "not found"
sentinel; replace with `is_empty()`:

```rust
pub fn read_found_u128(&self, payload_col: usize, col_size: usize) -> Option<u128> {
    let slice = self.found_col_ptr(payload_col, col_size);
    if slice.is_empty() { return None; }
    // ... read u128 from slice directly, no unsafe needed ...
}
```

`crates/gnitz-engine/src/storage/partitioned_table.rs`:

```rust
// After (delegates to Table, no logic change):
pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> &[u8] {
    match self.last_found_partition {
        Some(local) => self.tables[local].found_col_ptr(payload_col, col_size),
        None        => &[],
    }
}
pub fn found_blob_ptr(&self) -> &[u8] {
    match self.last_found_partition {
        Some(local) => self.tables[local].found_blob_ptr(),
        None        => &[],
    }
}
```

`crates/gnitz-engine/src/storage/batch.rs` —
`append_row_from_ptable_found` loses its three `unsafe` blocks:

```rust
// String / Blob column:
let src_slice = ptable.found_col_ptr(pi, cs); // &[u8], no unsafe
assert!(!src_slice.is_empty());
let (mut dest, is_long) = crate::schema::prep_german_string_copy(src_slice);
if is_long {
    let length = u32::from_le_bytes(src_slice[0..4].try_into().unwrap()) as usize;
    let blob = ptable.found_blob_ptr();       // &[u8], no unsafe
    assert!(!blob.is_empty());
    let old_offset = u64::from_le_bytes(src_slice[8..16].try_into().unwrap()) as usize;
    let src_data = &blob[old_offset..old_offset + length];
    let new_offset = self.blob.len();
    self.blob.extend_from_slice(src_data);
    dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
}
self.extend_col(pi, &dest);

// Fixed-width column:
let src_slice = ptable.found_col_ptr(pi, cs); // &[u8], no unsafe
assert!(!src_slice.is_empty());
self.extend_col(pi, src_slice);
```

## Testing

- `make test` — existing `append_row_from_ptable_found_narrow_byte_roundtrip`
  and all `Table` / `PartitionedTable` found-row tests pass unchanged.
- Compile-time: no `unsafe` blocks remain in `append_row_from_ptable_found`.
  `grep -n "unsafe" batch.rs` should not match that function.
