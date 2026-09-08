//! WAL-block encode/decode for the client wire codec.

use super::error::ProtocolError;
use super::types::{Schema, ZSetBatch};
use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};

// ── Internal helpers ─────────────────────────────────────────────────────────

// ── Region read helpers ───────────────────────────────────────────────────────

/// Append a region of 64-bit values (u64 or i64) to `dst` via bulk memcpy.
/// Correct on little-endian.
fn read_64bit_region_into<T: Copy>(
    dst: &mut Vec<T>,
    data: &[u8],
    off: usize,
    sz: usize,
    count: usize,
    label: &str,
) -> Result<(), ProtocolError> {
    debug_assert_eq!(std::mem::size_of::<T>(), 8);
    let expected = count * 8;
    if sz != expected {
        return Err(ProtocolError::DecodeError(format!(
            "{label} region size mismatch: expected {expected}, got {sz}"
        )));
    }
    let src = &data[off..off + expected];
    let base = dst.len();
    dst.reserve(count);
    // SAFETY: src is `expected` bytes (bounds-checked above); `reserve` leaves
    // room for `count` more Ts = `expected` bytes past `base`. Both are valid,
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
pub(crate) fn encode_wal_block(schema: &Schema, table_id: u32, batch: &ZSetBatch) -> Vec<u8> {
    let regions = super::regions::regions(batch, schema);
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
///
/// Public because a locally-served read has to end in a `ZSetBatch` too, and it
/// must be *this* function: a local reply and a remote one decoded by two
/// different rules could disagree.
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

/// `sink` is a well-formed batch of `schema` and so can be appended to: same PK
/// stride, same column count, and every payload region at the length its
/// declared type and row count imply.
///
/// [`ZSetBatch::check_columns`] is the shared column rule, not a second spelling
/// of it. Not the whole of [`ZSetBatch::validate`]: its NOT NULL sweep walks
/// every row, which across a train's frames would be quadratic — and the server
/// runs that check anyway.
fn sink_matches(sink: &ZSetBatch, schema: &Schema) -> Result<(), ProtocolError> {
    let sink_err = |e: String| ProtocolError::DecodeError(format!("decode sink: {e}"));
    if sink.pks.stride() as usize != schema.pk_stride() {
        return Err(sink_err(format!(
            "mismatched PK stride: expected {}, got {}",
            schema.pk_stride(),
            sink.pks.stride()
        )));
    }
    if sink.columns.len() != schema.num_columns() {
        return Err(sink_err(format!(
            "column count {} != schema column count {}",
            sink.columns.len(),
            schema.num_columns()
        )));
    }
    sink.check_columns(schema).map_err(sink_err)
}

fn decode_wal_block_impl(
    data: &[u8],
    schema: &Schema,
    verify_checksum: bool,
    sink: &mut ZSetBatch,
) -> Result<u32, ProtocolError> {
    // Shared framer validates format: version, `total_size` in-bounds, body
    // checksum, region count ≤ cap, and every region's [off, off+sz) extent
    // within the block. On success `dir` can index each region unchecked.
    let mut region_offsets = [0u64; gnitz_wire::MAX_WIRE_REGIONS];
    let mut region_sizes = [0u32; gnitz_wire::MAX_WIRE_REGIONS];
    let header = gnitz_wire::wal::validate_and_parse(data, &mut region_offsets, &mut region_sizes, verify_checksum)?;
    let table_id = header.table_id;
    let num_regions = header.num_regions as usize;

    // Client's half of the split: schema conformance.
    let expected_num_regions = gnitz_wire::wal::num_regions(schema.num_payload_cols());
    if num_regions != expected_num_regions {
        return Err(ProtocolError::DecodeError(format!(
            "WAL block num_regions mismatch: expected {expected_num_regions}, got {num_regions}"
        )));
    }
    let pk_stride = schema.pk_stride();

    // Index the framer-filled offset/size arrays directly — no per-decode heap
    // Vec (the engine's `decode_mem_batch_inner`/`decode_schema_block` do the same).
    let dir = |r: usize| (region_offsets[r] as usize, region_sizes[r] as usize);

    let count = header.entry_count as usize;

    if count == 0 {
        return Ok(table_id);
    }

    sink_matches(sink, schema)?;

    // Read system regions, by the shared §6 index — the same rule the encoder
    // builds the list with, rather than a counter that happens to agree.
    let (pk_off, pk_sz) = dir(REG_PK);
    let (wt_off, wt_sz) = dir(REG_WEIGHT);
    let (null_off, null_sz) = dir(REG_NULL_BMP);

    let expected_pk_sz = count * pk_stride;
    if pk_sz != expected_pk_sz {
        return Err(ProtocolError::DecodeError(format!(
            "pk region size mismatch: expected {expected_pk_sz}, got {pk_sz}"
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
    sink.reserve(schema, count);

    // A `PkColumn` holds the same OPK bytes the region carries, so the whole
    // region moves in one copy — no per-row, per-column transcode.
    sink.pks.push_region_bytes(&data[pk_off..pk_off + pk_sz]);
    read_64bit_region_into(&mut sink.weights, data, wt_off, wt_sz, count, "weights")?;
    read_64bit_region_into(&mut sink.nulls, data, null_off, null_sz, count, "nulls")?;

    // Blob region (always last). A German cell's heap offset is relative to the
    // arena it was encoded against, so an appending decode shifts every cell of
    // this block by where that arena lands in the sink's.
    let (blob_off, blob_sz) = dir(num_regions - 1);
    let block_blob = &data[blob_off..blob_off + blob_sz];
    let blob_base = sink.blob.len();
    sink.blob.extend_from_slice(block_blob);

    // Read column regions. The payload iterator supplies the slot, so payload
    // slot `pi` ↔ region `REG_PAYLOAD_START + pi` is stated, not produced as a
    // side effect of a counter; PK slots keep their empty placeholder.
    for (pi, ci, col) in schema.payload_columns() {
        let (reg_off, reg_sz) = dir(REG_PAYLOAD_START + pi);
        // One width rule for every column kind: `wire_stride` is 16 for STRING,
        // BLOB and the three 16-byte int types alike.
        let expected_sz = count * col.type_code.wire_stride();
        if reg_sz != expected_sz {
            return Err(ProtocolError::DecodeError(format!(
                "column {ci} region size mismatch: expected {expected_sz}, got {reg_sz}"
            )));
        }
        let dst = &mut sink.columns[ci];
        let at = dst.len();
        dst.extend_from_slice(&data[reg_off..reg_off + reg_sz]);
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

    Ok(table_id)
}

#[cfg(test)]
#[path = "tests/wal_block.rs"]
mod tests;
