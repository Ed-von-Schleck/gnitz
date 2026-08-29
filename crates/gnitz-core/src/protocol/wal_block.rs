//! WAL-block encode/decode for the client wire codec.

use super::error::ProtocolError;
use super::regions::ViewBuffers;
use super::types::{null_word_get, ColData, PkColumn, Schema, TypeCode, ZSetBatch};
use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Decode a STRING/BLOB column region into per-row raw-byte cells: `None` for
/// a null row, else the German string resolved against `blob`. Shared by the
/// STRING and BLOB decode arms (STRING then UTF-8-validates each cell).
/// `reg_off` is the region's directory offset; the caller has validated the
/// region is `count * 16` bytes and in-bounds, so the per-row struct extent
/// needs no re-check.
fn decode_german_col(
    data: &[u8],
    reg_off: usize,
    blob: &[u8],
    nulls: &[u64],
    payload_idx: usize,
    count: usize,
) -> Result<Vec<Option<Vec<u8>>>, ProtocolError> {
    let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(count);
    for (row, &null_word) in nulls.iter().enumerate().take(count) {
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

/// Read a region of 64-bit values (u64 or i64) via bulk memcpy. Correct on little-endian.
fn read_64bit_region<T: Copy>(
    data: &[u8],
    off: usize,
    sz: usize,
    count: usize,
    label: &str,
) -> Result<Vec<T>, ProtocolError> {
    debug_assert_eq!(std::mem::size_of::<T>(), 8);
    let expected = count * 8;
    if sz != expected {
        return Err(ProtocolError::DecodeError(format!(
            "{label} region size mismatch: expected {expected}, got {sz}"
        )));
    }
    let src = &data[off..off + expected];
    let mut v: Vec<T> = Vec::with_capacity(count);
    // SAFETY: src is `expected` bytes (bounds-checked above); v has room for
    // `count` Ts = `expected` bytes. Both are valid, non-overlapping regions,
    // and the copy initializes every element `set_len` then publishes.
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), v.as_mut_ptr() as *mut u8, expected);
        v.set_len(count);
    }
    Ok(v)
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Frame the batch's §6 region list ([`ViewBuffers::regions`]) into a WAL block.
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
    decode_wal_block_impl(data, schema, false)
}

/// Like [`decode_wal_block`] but verifies the block's XXH3 body checksum.
/// The crate's round-trip tests are its only callers — the only coverage that
/// client-encoded checksums are correct; production decodes run over a trusted
/// stream and go through [`decode_wal_block`].
#[cfg(test)]
pub(crate) fn decode_wal_block_verified(data: &[u8], schema: &Schema) -> Result<(ZSetBatch, u32), ProtocolError> {
    decode_wal_block_impl(data, schema, true)
}

fn decode_wal_block_impl(
    data: &[u8],
    schema: &Schema,
    verify_checksum: bool,
) -> Result<(ZSetBatch, u32), ProtocolError> {
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
        return Ok((ZSetBatch::new(schema), table_id));
    }

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
    // The PK region at rest is OPK (order-preserving big-endian). Walk it back
    // to the native LE bytes the in-memory `PkColumn` holds, so re-encoding the
    // batch (which OPK-encodes assuming LE input) does not double-encode. A
    // scalar key is the one-column case of the same walk.
    //
    // `le_row` is the per-row scratch, so a stride past it would index out of
    // bounds — reject at the boundary rather than panic. This also keeps the
    // `as u8` below lossless, since MAX_PK_BYTES is well under 255.
    if pk_stride > gnitz_wire::MAX_PK_BYTES {
        return Err(ProtocolError::DecodeError(format!(
            "pk_stride {pk_stride} exceeds MAX_PK_BYTES {}",
            gnitz_wire::MAX_PK_BYTES
        )));
    }
    let pks: PkColumn = {
        let col_info: Vec<(usize, u8)> = schema.pk_col_codes().collect();
        let mut decoded = Vec::with_capacity(pk_sz);
        let mut le_row = [0u8; gnitz_wire::MAX_PK_BYTES];
        // Per-row extent is dominated by the `pk_sz == count * pk_stride` check
        // above plus the directory's `pk_off + pk_sz <= total_size`, so no
        // per-row bounds check is needed.
        for row in 0..count {
            let base = pk_off + row * pk_stride;
            let src = &data[base..base + pk_stride];
            let mut off = 0;
            for &(cs, tc) in &col_info {
                gnitz_wire::decode_pk_column(&src[off..off + cs], tc, &mut le_row[off..off + cs]);
                off += cs;
            }
            decoded.extend_from_slice(&le_row[..pk_stride]);
        }
        PkColumn {
            stride: pk_stride as u8,
            buf: decoded,
        }
    };
    let weights: Vec<i64> = read_64bit_region(data, wt_off, wt_sz, count, "weights")?;
    let nulls: Vec<u64> = read_64bit_region(data, null_off, null_sz, count, "nulls")?;

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
    let mut columns = ZSetBatch::filler_columns(schema, 0);
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
        columns[ci] = match col.type_code {
            TypeCode::String => {
                // Raw German-string cells, then UTF-8-validate each into a String.
                let raw = decode_german_col(data, reg_off, blob, &nulls, pi, count)?;
                let mut vals: Vec<Option<String>> = Vec::with_capacity(count);
                for cell in raw {
                    vals.push(match cell {
                        None => None,
                        Some(bytes) => Some(
                            String::from_utf8(bytes)
                                .map_err(|e| ProtocolError::DecodeError(format!("utf8 in German String: {e}")))?,
                        ),
                    });
                }
                ColData::Strings(vals)
            }
            TypeCode::Blob => ColData::Bytes(decode_german_col(data, reg_off, blob, &nulls, pi, count)?),
            _ => ColData::Fixed(data[reg_off..reg_off + reg_sz].to_vec()),
        };
    }

    Ok((
        ZSetBatch {
            pks,
            weights,
            nulls,
            columns,
        },
        table_id,
    ))
}

#[cfg(test)]
#[path = "tests/wal_block.rs"]
mod tests;
