# WAL/IPC decode silently zero-fills out-of-bounds regions

`Batch::decode_from_wal_block` (`crates/gnitz-engine/src/storage/batch.rs:1581`)
builds region pointers with a bounds check that, on failure, leaves the pointer
null instead of erroring:

```rust
for i in 0..nr_mem {
    let off = offsets[i] as usize;
    let sz = sizes[i] as usize;
    region_sizes[i] = sizes[i];
    if sz > 0 && off + sz <= data.len() {
        ptrs[i] = unsafe { data.as_ptr().add(off) };
    }
}
```

A region whose declared `[off, off + sz)` extends past `data.len()` keeps
`ptrs[i] == null` while `region_sizes[i]` stays at the declared `sz`.
`Batch::from_regions` (`batch.rs:345`) then **skips** the copy for a null pointer
(`if sz > 0 && !ptrs[i].is_null()`), and because its destination buffer is
zero-initialized (`data.resize(total_size, 0)`, `batch.rs:376`), the region is
left all-zero. No error is raised — decode returns `Ok((batch, bytes_consumed))`
with a silently zeroed column (or an empty BLOB heap, dropping every string in
the block).

## Why the surrounding validation does not catch it

Two validation layers run before this loop; neither covers region *data extents*:

1. `wal::validate_and_parse` (`storage/wal.rs:157`) rejects a block whose
   `total_size > block.len()` (`wal.rs:179`) and whose region **directory** runs
   past `total_size` (`dir_off + 8 > total_size`, `wal.rs:201`). It reads each
   region's `(offset, size)` but **never checks `offset + size <= total_size`**.
   A directory entry can name a region that points past the block.
2. `decode_from_wal_block` validates each region's **size** against the
   schema-expected stride (`batch.rs:1625-1641`: PK = `n*pk_stride`, weight =
   `n*8`, null = `n*8`, payload = `n*col.size()`). It validates `sizes[i]`, not
   `offsets[i]`, and the variable-length **BLOB region has no size check at all**.

So a block can pass both layers with a region size that matches the schema but an
offset that places `[off, off + sz)` beyond the buffer — and that region is
zero-filled rather than rejected.

## Reachability and severity

The auditor's framing ("truncated WAL file" / "corrupted network packet") is
imprecise and should not be carried into the fix rationale:

- **Block truncation is already caught.** A short buffer trips
  `total_size > block.len()` in `validate_and_parse` → `Err(Truncated)`. The slice
  handed to `decode_from_wal_block` is exactly `data_size` long (`wire.rs:1114-1117`),
  so `data.len() == total_size`; the silent path is not reachable by truncation.
- **The real trigger is a corrupt directory `(offset, size)`** that keeps the
  block length self-consistent but names a region extending past it.
- **Checksums mask it on the verified path, not the IPC path.** `decode_wire`
  (`wire.rs:980`) verifies the xxh checksum over `[HEADER_SIZE, total_size]`
  (which includes the directory), so directory corruption is caught there first.
  But `decode_wire_ipc` (`wire.rs:986`) decodes with `verify_checksum = false`
  for the *trusted intra-process W2M ring* — there, a corrupt directory entry
  (producer bug, or memory corruption of the shared ring) reaches the silent
  zero-fill and returns `Ok` with wrong data. This is **intra-process**, not
  network; the practical exposure is a producer/encoder bug, not adversarial
  input.

The bug is therefore a latent correctness footgun, not a routine failure mode.
It is worth fixing because the failure is *silent wrong data* and the fix is
strictly safe: a well-formed block never names an out-of-bounds region, so the
new check can only ever fire on genuinely malformed input.

## Fix

Return an error in the pointer loop when a non-empty region runs past the block.
`off` and `sz` both derive from `u32` directory entries (`wal.rs:204-205`), so
`off + sz` cannot overflow `usize` on a 64-bit target; the addition matches the
existing `off + sz <= data.len()` style.

```rust
for i in 0..nr_mem {
    let off = offsets[i] as usize;
    let sz = sizes[i] as usize;
    region_sizes[i] = sizes[i];
    if sz > 0 {
        if off + sz > data.len() {
            return Err("WAL block region extends past block bounds");
        }
        ptrs[i] = unsafe { data.as_ptr().add(off) };
    }
}
```

This covers the BLOB region as well (it is included in `0..nr_mem`,
`nr_mem == 3 + npc + 1`), giving the otherwise-unvalidated heap its only bounds
check. The happy path is unchanged: one extra comparison per region, and every
valid block sets `ptrs[i]` exactly as before.

## Testing

Add a reject-test alongside the existing size-mismatch tests
(`decode_from_wal_block_rejects_mismatched_pk_stride` at `batch.rs:2394`,
`…_mismatched_weight_region` at `batch.rs:2414`). Those corrupt a region **size**
and trip the schema-size validation *before* this loop, so they are unaffected by
the new check. The new test corrupts a region **offset** so the size still matches
schema but the data extent overruns the block:

```rust
#[test]
fn decode_from_wal_block_rejects_region_offset_past_block() {
    let schema = single_col_pk_schema(type_code::U64);
    let mut b = Batch::with_schema(schema, 1);
    b.extend_pk(42u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &7i64.to_le_bytes());
    b.count += 1;

    let sz = b.wire_byte_size(1);
    let mut buf = vec![0u8; sz];
    b.encode_to_wire(1, &mut buf, 0, false);
    // Corrupt the REG_PK (region 0) OFFSET directory entry: point it far past
    // the block while leaving its size (= n*pk_stride) schema-valid.
    let off_off = gnitz_wire::WAL_HEADER_SIZE + REG_PK * 8;
    buf[off_off..off_off + 4].copy_from_slice(&(buf.len() as u32).to_le_bytes());
    // verify_checksum = false: the unverified IPC path is the one this guards.
    let r = Batch::decode_from_wal_block(&buf, &schema, false);
    assert_eq!(r.err(), Some("WAL block region extends past block bounds"));
}
```

Run: `cargo test -p gnitz-engine decode_from_wal_block`.
