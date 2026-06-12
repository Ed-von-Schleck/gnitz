# Bounds-check raw German-string relocation reads

## What

Three sites relocate a long German string by reading its heap bytes through a
**raw blob base pointer plus a stored offset/length, with no bounds check**:

- `schema::write_string_from_raw` (`crates/gnitz-engine/src/schema.rs:569`):

  ```rust
  let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
  let old_offset = u64::from_le_bytes(src[8..16].try_into().unwrap()) as usize;
  let src_data = unsafe { std::slice::from_raw_parts(src_blob_ptr.add(old_offset), length) };
  let new_offset = blob.len();
  blob.extend_from_slice(src_data);
  ```

- `ops/util::write_string_from_raw` (`ops/util.rs:60`) — a thin `unsafe`
  forwarder to the above; sole caller is the join right-payload copy
  (`ops/join.rs:470`), which passes `right_cursor.blob_ptr()` as the base.
- `Batch::append_row_from_ptable_found` (`storage/batch.rs:1128`) — an inlined
  copy of the same body, reading through `ptable.found_blob_ptr()`.

`length` and `old_offset` come from the 16-byte German-string struct stored in
a column. When that struct is correct the read is in-bounds. When it is corrupt
— a damaged shard file, a truncated mmap, an encoder bug — `old_offset` /
`length` are arbitrary, and `from_raw_parts(base.add(old_offset), length)`
fabricates a slice over unrelated process memory. `extend_from_slice` then
copies that memory into the output batch, which is returned to the client and
may be written to disk: a segfault at best, an out-of-bounds read / information
leak at worst.

This is **not** contained the way the read-accessor path is. A checked slice
(`&blob[a..b]`) panics on overrun and the worker's `catch_unwind` turns it into
a clean `Err`. `from_raw_parts` performs no check at all — there is no panic to
catch; the process reads whatever is at `base + old_offset`.

The sibling that takes the blob as a slice rather than a raw pointer —
`relocate_german_string_vec` (`schema.rs:470`) — already bounds-checks: on a
malformed offset/length it emits a clean length-0 cell and does not touch
`dst_blob` (regression test `schema.rs` `test_malformed_long_string_fallback_clean_header`).
The raw-pointer twins simply never got the same guard.

## Severity

Reachable on **today's** schemas (narrow PKs, any `STRING`/`BLOB` payload column
flowing through a join or a ptable-found row copy) — it does not depend on any
wide-PK or unreleased feature. The trigger is corrupt on-disk/in-memory blob
data, i.e. storage corruption or an engine bug, not attacker-controlled wire
input, so it is a hardening / memory-safety-robustness fix rather than a
remote-exploitable hole. But because the failure mode is an unchecked raw read
(uncatchable, unlike the slice-index paths), it ranks above the checked-slice
hardening items.

## What NOT to do

Do not skip the relocation and leave the destination cell pointing at the old
(now-invalid) offset — that produces a struct whose heap pointer is dangling in
the output batch's blob arena. Match `relocate_german_string_vec`'s contract
exactly: on a failed bounds check emit a clean **length-0** cell (header fully
zeroed, no `dst_blob` write). That keeps the two relocation paths consistent
and never fabricates or dangles data.

## Change

Thread the source blob length to every raw relocation site and check the read
range before constructing the slice. The length is already available at each
caller (`ReadCursor::blob_len()`, the ptable's found-blob length).

`schema::write_string_from_raw` gains a `src_blob_len: usize` parameter:

```rust
pub unsafe fn write_string_from_raw(
    blob: &mut Vec<u8>,
    src: &[u8],
    src_blob_ptr: *const u8,
    src_blob_len: usize,
) -> [u8; 16] {
    let (mut dest, is_long) = prep_german_string_copy(src);
    if is_long {
        let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
        let old_offset = u64::from_le_bytes(src[8..16].try_into().unwrap()) as usize;
        if src_blob_ptr.is_null() || old_offset.saturating_add(length) > src_blob_len {
            // Malformed: emit a clean empty cell, matching
            // relocate_german_string_vec's fallback. dest is already a
            // zeroed-header short cell from prep_german_string_copy on the
            // non-long branch; force length 0 here.
            dest[0..8].copy_from_slice(&0u64.to_le_bytes());
            return dest;
        }
        let src_data = unsafe { std::slice::from_raw_parts(src_blob_ptr.add(old_offset), length) };
        let new_offset = blob.len();
        blob.extend_from_slice(src_data);
        dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
    }
    dest
}
```

(Confirm the exact bytes `prep_german_string_copy` leaves in `dest` for the
fallback — the goal is a header identical to what `relocate_german_string_vec`
emits on its malformed branch: length 0, prefix zeroed, offset zeroed. Pin it
with the test below so the two paths can't drift.)

Callers:

- `ops/util::write_string_from_raw` forwards the new `src_blob_len`.
- `ops/join.rs:470` passes `right_cursor.blob_len()` (the cursor already exposes
  it; `compare_cursor_payload_to_batch_row` at `ops/util.rs:91` uses exactly
  this length to bound a slice over the same pointer).
- `Batch::append_row_from_ptable_found` (`batch.rs:1128`) applies the same
  in-range check against the ptable's found-blob length before its
  `from_raw_parts`; emit the length-0 fallback on overrun.

The in-bounds path copies the identical bytes it does today — valid data is
unaffected.

## Testing

- **Unit (`schema.rs`)**: drive `write_string_from_raw` with a long-string
  struct whose `old_offset`/`length` overrun a deliberately tiny `src_blob_len`;
  assert it returns a length-0 cell and does not extend `blob`. Add the mirror
  of the existing `relocate_german_string_vec` malformed test so both paths
  assert the same fallback header byte-for-byte.
- **Unit (`batch.rs`)**: construct a ptable-found row whose STRING cell carries
  an out-of-range offset and assert `append_row_from_ptable_found` emits an
  empty string rather than reading past the blob.
- **Regression**: a valid long-string join row and a valid ptable-found
  long-string row still relocate byte-for-byte (existing join / scan tests
  cover this; confirm they stay green).
- `make test` green throughout.
