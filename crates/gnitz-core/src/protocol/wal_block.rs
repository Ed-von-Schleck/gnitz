//! WAL-block encode/decode for the client wire codec.

use super::error::ProtocolError;
use super::regions::ViewBuffers;
use super::types::{null_word_get, ColData, Schema, TypeCode, ZSetBatch};
use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Decode a STRING/BLOB column region into per-row raw-byte cells: `None` for
/// a null row, else the German string resolved against `blob`. Shared by the
/// STRING and BLOB decode arms (STRING then UTF-8-validates each cell).
///
/// `nulls` is *this block's* null words and nothing else — it is both the row
/// count and the region index, so an over-long slice is a wrong count rather
/// than a read past the block. `reg_off` is the region's directory offset,
/// which the caller has checked covers `nulls.len() * 16` bytes.
fn decode_german_col(
    data: &[u8],
    reg_off: usize,
    blob: &[u8],
    nulls: &[u64],
    payload_idx: usize,
) -> Result<Vec<Option<Vec<u8>>>, ProtocolError> {
    let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(nulls.len());
    for (row, &null_word) in nulls.iter().enumerate() {
        if null_word_get(null_word, payload_idx) {
            vals.push(None);
            continue;
        }
        let struct_start = reg_off + row * 16;
        let cell = &data[struct_start..struct_start + 16];
        vals.push(Some(gnitz_wire::try_decode_german_string(cell, blob).ok_or_else(
            || ProtocolError::DecodeError("German String blob arena out of bounds".into()),
        )?));
    }
    Ok(vals)
}

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

/// Frame the batch's §6 region list ([`ViewBuffers::regions`]) into a WAL block.
///
/// For a lone block. A transaction frame instead hands `gnitz-wire` the region
/// lists and lets it frame each block into the frame itself, which is one copy
/// rather than two.
pub(crate) fn encode_wal_block(schema: &Schema, table_id: u32, batch: &ZSetBatch) -> Vec<u8> {
    let mut bufs = ViewBuffers::default();
    let regions = bufs.regions(batch, schema);
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
/// stride, same column count, and every payload slot carrying the variant its
/// declared type calls for at the length its row count implies.
///
/// [`ZSetBatch::check_columns`] is the shared column rule, not a second spelling
/// of it. Not the whole of [`ZSetBatch::validate`]: its NOT NULL sweep walks
/// every row, which across a train's frames would be quadratic — and the server
/// runs that check anyway.
fn sink_matches(sink: &ZSetBatch, schema: &Schema) -> Result<(), ProtocolError> {
    let sink_err = |e: String| ProtocolError::DecodeError(format!("decode sink: {e}"));
    if sink.pks.stride as usize != schema.pk_stride() {
        return Err(sink_err(format!(
            "mismatched PK stride: expected {}, got {}",
            schema.pk_stride(),
            sink.pks.stride
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

    // The PK region at rest is OPK (order-preserving big-endian). Walk it back
    // to the native LE bytes the in-memory `PkColumn` holds, so re-encoding the
    // batch (which OPK-encodes assuming LE input) does not double-encode.
    //
    // Decoded in place: a per-row scratch plus an `extend_from_slice` of a
    // runtime-length row compiles to a `call memcpy` per row. `decode_pk_column`
    // writes through a `&mut [u8]`, so the rows are zeroed in one bulk memset
    // first — cheaper than the memcpy it replaces, and the row loop overwrites
    // every one of those bytes.
    {
        let col_info: Vec<(usize, u8)> = schema.pk_col_codes().collect();
        let rows = super::regions::extend_zeroed(&mut sink.pks.buf, count * pk_stride);
        // Per-row extent is dominated by the `pk_sz == count * pk_stride` check
        // above plus the directory's `pk_off + pk_sz <= total_size`, so no
        // per-row bounds check is needed on the source.
        for row in 0..count {
            let src = &data[pk_off + row * pk_stride..pk_off + (row + 1) * pk_stride];
            let dst = &mut rows[row * pk_stride..(row + 1) * pk_stride];
            let mut off = 0;
            for &(cs, tc) in &col_info {
                gnitz_wire::decode_pk_column(&src[off..off + cs], tc, &mut dst[off..off + cs]);
                off += cs;
            }
        }
    }
    read_64bit_region_into(&mut sink.weights, data, wt_off, wt_sz, count, "weights")?;
    let nulls_base = sink.nulls.len();
    read_64bit_region_into(&mut sink.nulls, data, null_off, null_sz, count, "nulls")?;

    // Blob region (always last)
    let (blob_off, blob_sz) = dir(num_regions - 1);
    let blob = if blob_sz > 0 {
        &data[blob_off..blob_off + blob_sz]
    } else {
        &[]
    };

    // Read column regions. The payload iterator supplies the slot, so payload
    // slot `pi` ↔ region `REG_PAYLOAD_START + pi` is stated, not produced as a
    // side effect of a counter; PK slots keep the empty `Fixed` placeholder
    // `ZSetBatch::filler_columns` builds.
    let ZSetBatch { nulls, columns, .. } = sink;
    // This block's null words alone — `decode_german_col` derives its row count
    // and its per-row region offset from them.
    let nulls = &nulls[nulls_base..];
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
        match (&mut columns[ci], col.type_code) {
            (ColData::Strings(dst), TypeCode::String) => {
                // Raw German-string cells, then UTF-8-validate each into a String.
                dst.reserve(count);
                for cell in decode_german_col(data, reg_off, blob, nulls, pi)? {
                    dst.push(match cell {
                        None => None,
                        Some(bytes) => Some(
                            String::from_utf8(bytes)
                                .map_err(|e| ProtocolError::DecodeError(format!("utf8 in German String: {e}")))?,
                        ),
                    });
                }
            }
            (ColData::Bytes(dst), TypeCode::Blob) => {
                dst.extend(decode_german_col(data, reg_off, blob, nulls, pi)?);
            }
            (ColData::Fixed(dst), _) => dst.extend_from_slice(&data[reg_off..reg_off + reg_sz]),
            // `sink_matches` already paired every payload slot with its declared
            // type; an error rather than a panic because the sink is reached
            // from the wire.
            _ => {
                return Err(ProtocolError::DecodeError(format!(
                    "decode sink: column {ci}: ColData variant contradicts schema type {:?}",
                    col.type_code
                )))
            }
        }
    }

    Ok(table_id)
}

#[cfg(test)]
#[path = "tests/wal_block.rs"]
mod tests;
