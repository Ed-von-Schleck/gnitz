//! WAL-block encode/decode for the client wire codec.

use super::error::ProtocolError;
use super::types::{check_not_null, Schema, ZSetBatch};
use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};

// ── Internal helpers ─────────────────────────────────────────────────────────

// ── Region read helpers ───────────────────────────────────────────────────────

/// Append a region of 64-bit values (u64 or i64) to `dst` via bulk memcpy.
/// Correct on little-endian.
fn read_64bit_region_into<T: Copy>(
    dst: &mut Vec<T>,
    src: &[u8],
    count: usize,
    label: &str,
) -> Result<(), ProtocolError> {
    debug_assert_eq!(std::mem::size_of::<T>(), 8);
    let expected = count * 8;
    if src.len() != expected {
        return Err(ProtocolError::DecodeError(format!(
            "{label} region size mismatch: expected {expected}, got {}",
            src.len()
        )));
    }
    let base = dst.len();
    dst.reserve(count);
    // SAFETY: src is `expected` bytes (checked above); `reserve` leaves room for
    // `count` more Ts = `expected` bytes past `base`. Both are valid,
    // non-overlapping regions, and the copy initializes every element `set_len`
    // then publishes.
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr().add(base) as *mut u8, expected);
        dst.set_len(base + count);
    }
    Ok(())
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Frame the batch's §6 region list ([`super::regions::regions`]) into a WAL
/// block.
///
/// For a lone block. A transaction frame instead hands `gnitz-wire` the region
/// lists and lets it frame each block into the frame itself, which is one copy
/// rather than two.
pub(crate) fn encode_wal_block(table_id: u32, batch: &ZSetBatch) -> Vec<u8> {
    let regions = super::regions::regions(batch);
    // Size the output to exactly one block, then frame in place, so encode never
    // returns BufferTooSmall. checksum = true: client frames always carry a body
    // checksum.
    let mut out = vec![0u8; gnitz_wire::wal::block_size_of(&regions)];
    gnitz_wire::wal::encode(&mut out, 0, table_id, batch.len() as u32, &regions, true)
        .expect("WAL encode: pre-sized buffer");
    out
}

/// Decode a WAL block from `data`, expecting columns described by `schema`.
/// Returns `(ZSetBatch, table_id)`.
///
/// Skips checksum verification — the production form: every client decode
/// runs over a trusted stream (OS-delivered Unix socket bytes or a TLS
/// record layer) where transport integrity is already guaranteed. Use
/// [`decode_wal_block_verified`] where the checksum itself is under test.
pub fn decode_wal_block(data: &[u8], schema: &Schema) -> Result<(ZSetBatch, u32), ProtocolError> {
    let mut sink = ZSetBatch::new(schema);
    let table_id = decode_wal_block_impl(data, schema, false, &mut sink)?;
    Ok((sink, table_id))
}

/// [`decode_wal_block`] appending into `sink` instead of building a fresh batch
/// — what a multi-frame reply train decodes through, so no frame is copied
/// twice. `sink` must have been built from `schema`; a mismatch is refused
/// before anything is appended.
///
/// A decode error partway through leaves `sink` half-appended. Every driver
/// closes the session on a `step` error, and `Session::close` resets the
/// accumulator, so no torn batch is read back.
pub(crate) fn decode_wal_block_into(sink: &mut ZSetBatch, data: &[u8], schema: &Schema) -> Result<(), ProtocolError> {
    decode_wal_block_impl(data, schema, false, sink).map(|_| ())
}

/// Like [`decode_wal_block`] but verifies the block's XXH3 body checksum.
/// The crate's round-trip tests are its only callers — the only coverage that
/// client-encoded checksums are correct; production decodes run over a trusted
/// stream and go through [`decode_wal_block`].
#[cfg(test)]
pub(crate) fn decode_wal_block_verified(data: &[u8], schema: &Schema) -> Result<(ZSetBatch, u32), ProtocolError> {
    let mut sink = ZSetBatch::new(schema);
    let table_id = decode_wal_block_impl(data, schema, true, &mut sink)?;
    Ok((sink, table_id))
}

/// `sink` is a well-formed batch of `schema`, and so can be appended to.
fn sink_matches(sink: &ZSetBatch, schema: &Schema) -> Result<(), ProtocolError> {
    let sink_err = |e: String| ProtocolError::DecodeError(format!("decode sink: {e}"));
    sink.layout_matches(schema).map_err(sink_err)?;
    sink.check_columns().map_err(sink_err)
}

fn decode_wal_block_impl(
    data: &[u8],
    schema: &Schema,
    verify_checksum: bool,
    sink: &mut ZSetBatch,
) -> Result<u32, ProtocolError> {
    // Shared framer validates format: version, `total_size` in-bounds, body
    // checksum, region count ≤ cap, and every region's [off, off+sz) extent
    // within the block. On success each region slices unchecked.
    let mut region_offsets = [0u64; gnitz_wire::MAX_WIRE_REGIONS];
    let mut region_sizes = [0u32; gnitz_wire::MAX_WIRE_REGIONS];
    let header = gnitz_wire::wal::validate_and_parse(data, &mut region_offsets, &mut region_sizes, verify_checksum)?;
    let num_regions = header.num_regions as usize;
    let mut regions: [&[u8]; gnitz_wire::MAX_WIRE_REGIONS] = [&[]; gnitz_wire::MAX_WIRE_REGIONS];
    for (r, region) in regions[..num_regions].iter_mut().enumerate() {
        let off = region_offsets[r] as usize;
        *region = &data[off..off + region_sizes[r] as usize];
    }
    decode_regions_into(sink, &regions[..num_regions], header.entry_count as usize, schema)?;
    Ok(header.table_id)
}

/// Decode `count` rows laid out as `regions` (canonical order, blob last) under `schema`,
/// appending to `sink`: the one rule for a WAL block's body and an engine batch's regions.
pub fn decode_regions_into(
    sink: &mut ZSetBatch,
    regions: &[&[u8]],
    count: usize,
    schema: &Schema,
) -> Result<(), ProtocolError> {
    // Client's half of the split: schema conformance.
    let num_regions = regions.len();
    let expected_num_regions = gnitz_wire::wal::num_regions(schema.num_payload_cols());
    if num_regions != expected_num_regions {
        return Err(ProtocolError::DecodeError(format!(
            "WAL block num_regions mismatch: expected {expected_num_regions}, got {num_regions}"
        )));
    }
    let pk_stride = schema.pk_stride();

    if count == 0 {
        return Ok(());
    }

    sink_matches(sink, schema)?;

    // Read system regions, by the shared §6 index — the same rule the encoder
    // builds the list with, rather than a counter that happens to agree.
    let pk = regions[REG_PK];
    let expected_pk_sz = count * pk_stride;
    if pk.len() != expected_pk_sz {
        return Err(ProtocolError::DecodeError(format!(
            "pk region size mismatch: expected {expected_pk_sz}, got {}",
            pk.len()
        )));
    }
    // Bound the stride so the `as u8` a `PkColumn` holds stays lossless, and so
    // a nonsense schema cannot ask for an unbounded per-row extent.
    if pk_stride > gnitz_wire::MAX_PK_BYTES {
        return Err(ProtocolError::DecodeError(format!(
            "pk_stride {pk_stride} exceeds MAX_PK_BYTES {}",
            gnitz_wire::MAX_PK_BYTES
        )));
    }

    // Size every stream before any of them grows: the regions are exact, and a
    // frame carries as many rows as a 64 MiB reply budget holds.
    sink.reserve(count);

    // A `PkColumn` holds the same OPK bytes the region carries, so the whole
    // region moves in one copy — no per-row, per-column transcode.
    sink.pks.push_region_bytes(pk);
    read_64bit_region_into(&mut sink.weights, regions[REG_WEIGHT], count, "weights")?;
    let nulls_at = sink.nulls.len();
    read_64bit_region_into(&mut sink.nulls, regions[REG_NULL_BMP], count, "nulls")?;
    // Over exactly the words this block appended, so a train stays linear.
    check_not_null(&sink.nulls[nulls_at..], schema).map_err(ProtocolError::DecodeError)?;

    // Blob region (always last). A German cell's heap offset is relative to the
    // arena it was encoded against, so an appending decode shifts every cell of
    // this block by where that arena lands in the sink's.
    let block_blob = regions[num_regions - 1];
    let blob_base = sink.blob.len();
    sink.blob.extend_from_slice(block_blob);

    // Read column regions. The payload iterator supplies the slot, so payload
    // slot `pi` ↔ region `REG_PAYLOAD_START + pi` is stated, not produced as a
    // side effect of a counter.
    for (pi, ci, col) in schema.payload_columns() {
        let region = regions[REG_PAYLOAD_START + pi];
        // One width rule for every column kind: `wire_stride` is 16 for STRING,
        // BLOB and the three 16-byte int types alike.
        let expected_sz = count * col.type_code.wire_stride();
        if region.len() != expected_sz {
            return Err(ProtocolError::DecodeError(format!(
                "column {ci} region size mismatch: expected {expected_sz}, got {}",
                region.len()
            )));
        }
        let dst = &mut sink.payload[pi].bytes;
        let at = dst.len();
        dst.extend_from_slice(region);
        if gnitz_wire::is_german_string(col.type_code as u8) {
            for cell in dst[at..].as_chunks_mut::<16>().0 {
                // The cells arrive verbatim, so this is where a heap extent that
                // overruns the arena — or a padding/prefix skew that would order
                // two equal values unequal — is stopped, exactly as the engine's
                // own passthrough decode stops it.
                if !gnitz_wire::german_string_cell_ok(cell, block_blob) {
                    return Err(ProtocolError::DecodeError(format!(
                        "column {ci}: German string cell is not in canonical form"
                    )));
                }
                gnitz_wire::shift_german_string_heap(cell, blob_base);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "tests/wal_block.rs"]
mod tests;
