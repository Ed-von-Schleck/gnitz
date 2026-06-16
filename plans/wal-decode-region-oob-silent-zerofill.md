# WAL/IPC region codec silently zero-fills / drops out-of-bounds data

The WAL/IPC region codec has three sites where a region's declared
`[offset, offset + size)` extent is never checked against the actual buffer, so
a malformed directory entry silently substitutes empty/zero data instead of
raising an error. All three return `Ok` / succeed with silently wrong data:

1. `Batch::decode_from_wal_block` (`batch.rs:1571`) — **any** region
   (PK/weight/null/payload/blob) whose extent runs past the block keeps a null
   pointer and is zero-filled by `from_regions`.
2. `decode_mem_batch_from_wal_block` (`batch.rs:1716`) — the variable-length
   **BLOB** region (the fixed regions are already checked) silently falls back to
   an empty slice on overrun, dropping every long string in the block.
3. `Batch::encode_range_to_wire` (`batch.rs:1516`) — drops the blob heap
   unconditionally on the assumption that the schema is wire-safe (no long
   strings), with nothing asserting that precondition.

The fixes turn each decode overrun into an error and the encode precondition
into a debug assertion. Every fix is strictly safe: a well-formed block never
names an out-of-bounds region, and a wire-safe batch never carries a blob, so
the new checks only ever fire on genuinely malformed input or a producer bug.

## Site 1 — `decode_from_wal_block` zero-fills any OOB region

The pointer loop builds region pointers with a bounds check that, on failure,
leaves the pointer null instead of erroring:

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

## Site 2 — `decode_mem_batch_from_wal_block` silently empties an OOB blob

The zero-copy sibling decoder validates every **fixed** region's extent —
`off + sz > data.len()` → `Err("data WAL region OOB")` via the `validate`
closure (`batch.rs:1743-1761`) — but then computes the variable-length BLOB
region with the same silent fallback the fixed regions are protected from:

```rust
let blob_r = REG_PAYLOAD_START + npc;
let blob = {
    let off = wal_offsets[blob_r] as usize;
    let sz  = sizes[blob_r] as usize;
    if sz > 0 && off + sz <= data.len() { &data[off..off + sz] } else { &[] }
};
```

A BLOB extent past the block yields an empty heap, so every long (> 12 byte)
string in the batch resolves its 16-byte German-string struct against an empty
blob — silent string loss with an `Ok` return. This decoder is the **IPC
zero-copy path** (it calls `validate_and_parse(.., false)` — checksum
verification is always off), i.e. exactly the unchecked path where decode-side
corruption is reachable (see Reachability).

## Site 3 — `encode_range_to_wire` drops the blob with no wire-safe assertion

The range encoder writes a null/zero blob region on the premise that the schema
is wire-safe, but nothing enforces that premise:

```rust
// blob: null ptr with 0 bytes — no long strings in wire-safe schemas
ptrs[blob_idx]  = std::ptr::null();
sizes[blob_idx] = 0;
```

The premise currently holds: the callers (`stream_batch_response`,
`send_scan_response`, and the `pending_streams` drain in
`emit_pending_scan_chunk`, all in `runtime/worker.rs`) gate this encoder on
`schema_wire_safe`; a STRING/BLOB ("non-wire-safe") reply is routed to the
full-blob `encode_wire_into` path instead (`worker.rs:1339-1359`). So the bug is
latent, not live. But the function asserts none of this locally: a future caller
that hands a string-bearing batch here would ship 16-byte structs whose heap was
silently dropped, and the receiver would decode dangling references. A debug
assertion makes that a loud failure at the encode site instead.

## Why the existing validation misses the decode overruns

Two validation layers run before Site 1's loop; neither covers region *data
extents*:

1. `wal::validate_and_parse` (`storage/wal.rs:157`) rejects a block whose
   `total_size > block.len()` (`wal.rs:179`) and whose region **directory** runs
   past `total_size` (`dir_off + 8 > total_size`, `wal.rs:201`). It reads each
   region's `(offset, size)` but **never checks `offset + size <= total_size`**.
   A directory entry can name a region that points past the block.
2. `decode_from_wal_block` validates each region's **size** against the
   schema-expected stride (`batch.rs:1615-1631`: PK = `n*pk_stride`, weight =
   `n*8`, null = `n*8`, payload = `n*col.size()`). It validates `sizes[i]`, not
   `offsets[i]`, and the variable-length **BLOB region has no size check at all**.

So a block can pass both layers with a region size that matches the schema but an
offset that places `[off, off + sz)` beyond the buffer — and that region is
zero-filled rather than rejected. Site 2 already closes the offset gap for its
fixed regions but left the same gap open for the blob.

## Reachability

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
  But the unverified intra-process W2M ring decodes with `verify_checksum = false`
  (`decode_wire_ipc`, and `decode_mem_batch_from_wal_block` unconditionally) —
  there, a corrupt directory entry (producer bug, or memory corruption of the
  shared ring) reaches the silent zero-fill/empty-blob and returns `Ok` with
  wrong data. This is **intra-process**, not network; the practical exposure is a
  producer/encoder bug, not adversarial input.

The bugs are therefore latent correctness footguns, not routine failure modes.
They are worth fixing because the failure is *silent wrong data* and the fixes
are strictly safe: a well-formed block never names an out-of-bounds region and a
wire-safe batch never carries a blob, so the new checks can only ever fire on
genuinely malformed input.

## Fixes

### Site 1 — error on any OOB region (`decode_from_wal_block`)

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

### Site 2 — error on an OOB blob (`decode_mem_batch_from_wal_block`)

Match the fixed-region `validate` closure's error idiom rather than silently
emptying the heap:

```rust
let blob_r = REG_PAYLOAD_START + npc;
let blob = {
    let off = wal_offsets[blob_r] as usize;
    let sz  = sizes[blob_r] as usize;
    if sz == 0 {
        &[]
    } else if off + sz <= data.len() {
        &data[off..off + sz]
    } else {
        return Err("data WAL blob region OOB");
    }
};
```

### Site 3 — assert the wire-safe precondition (`encode_range_to_wire`)

Add the assertion alongside the existing range-bounds `debug_assert!` at the top
of the function, before the region loop:

```rust
debug_assert!(
    start_row + count <= self.count,
    "encode_range_to_wire: range [{start_row}, {}) out of bounds (batch count = {})",
    start_row + count, self.count,
);
// Wire-safe precondition: a wire-safe schema carries no long strings, so the
// blob heap is empty and is intentionally dropped below. Callers gate this
// encoder on `schema_wire_safe`; a STRING/BLOB batch routes to the full-blob
// `encode_wire_into` path. Assert it so a future caller that mis-routes string
// data fails loudly here rather than shipping structs whose heap vanished.
debug_assert!(
    self.blob.is_empty(),
    "encode_range_to_wire on a batch with a {}-byte blob: wire-safe schemas \
     carry no long strings; the heap would be silently dropped",
    self.blob.len(),
);
```

## Testing

The existing size-mismatch tests (`decode_from_wal_block_rejects_mismatched_pk_stride`
at `batch.rs:~2394`, `…_mismatched_weight_region` at `batch.rs:~2414`) corrupt a
region **size** and trip the schema-size validation *before* Site 1's loop, so
they are unaffected by the new checks. The new tests corrupt a region **offset**
(or the blob **size**) so the size still matches schema but the data extent
overruns the block.

### Site 1

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

### Site 2

```rust
#[test]
fn decode_mem_batch_rejects_blob_region_past_block() {
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
    // Fixed regions stay schema-valid; corrupt only the variable-length BLOB
    // region (index REG_PAYLOAD_START + npc) so [off, off + sz) overruns the
    // block: offset = block end, size = 8. The fixed-region `validate` closure
    // is unaffected — this exercises the blob path specifically.
    let blob_r = REG_PAYLOAD_START + schema.num_payload_cols();
    let entry = gnitz_wire::WAL_HEADER_SIZE + blob_r * 8;
    buf[entry..entry + 4].copy_from_slice(&(buf.len() as u32).to_le_bytes());  // offset
    buf[entry + 4..entry + 8].copy_from_slice(&8u32.to_le_bytes());           // size
    let r = decode_mem_batch_from_wal_block(&buf, &schema);
    assert_eq!(r.err(), Some("data WAL blob region OOB"));
}
```

### Site 3

The encode guard is a `debug_assert!`, so this test panics under
`debug_assertions` (the default `cargo test` build). `b.blob` is a private field,
reachable from the in-file `mod tests`.

```rust
#[test]
#[should_panic(expected = "wire-safe schemas")]
fn encode_range_to_wire_panics_on_nonempty_blob() {
    let schema = single_col_pk_schema(type_code::U64);
    let mut b = Batch::with_schema(schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &7i64.to_le_bytes());
    b.count += 1;
    // A non-empty heap on a wire-safe encode must fail loudly, not vanish.
    b.blob.push(0xAB);
    let mut out = vec![0u8; b.wire_byte_size(1) + 16];
    b.encode_range_to_wire(0, 1, 1, &mut out, 0, false);
}
```

Run: `cargo test -p gnitz-engine decode_from_wal_block decode_mem_batch encode_range_to_wire`.
