//! The **meta-schema block** codec: the one WAL block that carries a relation's
//! column shape across the wire — engine → client on every schema-bearing reply,
//! client → engine on every schema-bearing push and per-family `PUSH_TXN` block.
//!
//! Both directions live here because the block is a contract, not a
//! representation: each side owns a different schema type (`Schema` in the
//! client, `SchemaDescriptor` in the engine), and neither needs to be visible to
//! the other for the *bytes* to be defined once. The codec therefore speaks in
//! neutral per-column facts — `(type_code, flags, name)` plus the ordered PK
//! list — and each side builds its own type from them.
//!
//! The block is a normal WAL block over the [`META_SCHEMA_COLS`] shape (§6):
//! one row per column, keyed by `col_idx`.
//!
//! ```text
//! region[0] pk        col_idx, u64 OPK (big-endian), 8 B/row
//! region[1] weight    i64 LE, always 1
//! region[2] null      u64 LE, always 0 (no column is nullable)
//! region[3] type_code u64 LE
//! region[4] flags     u64 LE (`pack_col_meta_flags`)
//! region[5] name      16-byte German-string cells
//! region[6] blob      the German-string heap
//! ```

use crate::wal;
use crate::{
    blob_extent, col_meta_nullable, col_meta_pk_pos, encode_german_string, german_string_content, is_pk_eligible,
    is_valid_type_code, payload_region_in, read_u64_le, write_u64_le, LEADING_COL_PK, MAX_COLUMNS, MAX_PK_COLUMNS,
    META_SCHEMA_COLS, REG_NULL_BMP, REG_PK, REG_WEIGHT, SHORT_STRING_THRESHOLD,
};

const REG_TYPE_CODE: usize = payload_region_in(META_SCHEMA_COLS, "type_code");
const REG_FLAGS: usize = payload_region_in(META_SCHEMA_COLS, "flags");
const REG_NAME: usize = payload_region_in(META_SCHEMA_COLS, "name");

/// Exactly how many regions a meta-schema block has: the fixed three, one per
/// META_SCHEMA payload column, and the blob heap. A block naming any other count
/// is not this block, so the decoder rejects it rather than reading whichever
/// regions happen to be present.
pub(crate) const SCHEMA_BLOCK_REGIONS: usize = wal::num_regions(META_SCHEMA_COLS.len() - 1);
const REG_BLOB: usize = SCHEMA_BLOCK_REGIONS - 1;

/// `col_idx` is a `U64` primary key, so its OPK image is 8 big-endian bytes.
const PK_STRIDE: usize = 8;

// The codec writes one 8-byte big-endian `col_idx` per row into region 0, which
// is only the block's key if `col_idx` is the whole key and is that column.
const _: () = {
    assert!(LEADING_COL_PK.len() == 1 && LEADING_COL_PK[0] == 0);
    assert!(META_SCHEMA_COLS[0].type_code as u8 == crate::type_code::U64);
};

/// One column as the block carries it. `name` borrows the block's own bytes on
/// decode; on encode it is whatever the caller has (an empty slice for the
/// anonymous blocks that describe physical shape only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaBlockCol<'a> {
    pub type_code: u8,
    /// The packed per-column meta word (`pack_col_meta_flags`): nullable,
    /// hidden, serial, and the column's position within the PK tuple.
    pub flags: u64,
    pub name: &'a [u8],
}

/// Blob-heap bytes `name` spills: nothing while it fits inline in its cell.
fn blob_bytes(name: &[u8]) -> usize {
    if name.len() <= SHORT_STRING_THRESHOLD {
        0
    } else {
        name.len()
    }
}

/// Per-row bytes of each region, indexed by region: the `col_idx` key, weight,
/// null word, type_code, flags, the 16-byte German-string name cell, and the
/// blob heap (which is not per-row). Encode sizing, the scratch-buffer split and
/// the decode-side shape check all read this one table, so a stride change
/// cannot land in two of the three.
const ROW_BYTES: [usize; SCHEMA_BLOCK_REGIONS] = {
    let mut t = [0usize; SCHEMA_BLOCK_REGIONS];
    t[REG_PK] = PK_STRIDE;
    t[REG_WEIGHT] = 8;
    t[REG_NULL_BMP] = 8;
    t[REG_TYPE_CODE] = 8;
    t[REG_FLAGS] = 8;
    t[REG_NAME] = 16;
    t[REG_BLOB] = 0; // sized by the spill, not by the row count
    t
};

/// Per-row bytes of the six fixed-stride regions.
const FIXED_ROW_BYTES: usize = {
    let (mut sum, mut r) = (0usize, 0);
    while r < REG_BLOB {
        sum += ROW_BYTES[r];
        r += 1;
    }
    sum
};

/// Block size for `n` columns spilling `blob` bytes of long names.
fn block_len(n: usize, blob: usize) -> usize {
    let mut sizes = [0u32; SCHEMA_BLOCK_REGIONS];
    for (r, sz) in sizes.iter_mut().enumerate() {
        *sz = (n * ROW_BYTES[r]) as u32;
    }
    sizes[REG_BLOB] = blob as u32;
    wal::block_size(&sizes)
}

/// Encoded size of the block for `cols` — what [`encode_into`] writes.
pub(crate) fn encoded_len(cols: &[SchemaBlockCol]) -> usize {
    block_len(cols.len(), cols.iter().map(|c| blob_bytes(c.name)).sum())
}

/// Encoded size of an **anonymous** block of `num_columns` columns — every name
/// empty, so nothing spills and the size is a pure function of the count. Lets
/// a caller that only needs the size (the engine's frame sizing, once per reply)
/// skip building the column list at all.
pub fn anonymous_encoded_len(num_columns: usize) -> usize {
    block_len(num_columns, 0)
}

/// Write the block for `cols` into `out[offset..]`, returning the bytes written
/// (always [`encoded_len`]). The offset form exists because the engine writes
/// this block in place, into a buffer it pre-sized for the whole frame; the
/// [`encode`] wrapper serves callers that just want the bytes.
///
/// Panics if `out[offset..]` is shorter than [`encoded_len`] — a caller that
/// sized its buffer from anything but `encoded_len` has a bug here, not a
/// runtime condition to report.
pub fn encode_into(out: &mut [u8], offset: usize, tid: u32, cols: &[SchemaBlockCol], checksum: bool) -> usize {
    let n = cols.len();
    // One scratch buffer carved into the six fixed-stride regions, so a block
    // whose names all fit inline (the anonymous case, and most named ones)
    // costs a single allocation. The blob heap grows only if a name spills.
    let mut fixed = vec![0u8; n * FIXED_ROW_BYTES];
    let (pk, rest) = fixed.split_at_mut(n * PK_STRIDE);
    let (weight, rest) = rest.split_at_mut(n * 8);
    let (null, rest) = rest.split_at_mut(n * 8);
    let (type_code, rest) = rest.split_at_mut(n * 8);
    let (flags, names) = rest.split_at_mut(n * 8);
    let mut blob: Vec<u8> = Vec::new();

    for (i, c) in cols.iter().enumerate() {
        // The PK region is OPK: an unsigned column encodes big-endian.
        pk[i * PK_STRIDE..(i + 1) * PK_STRIDE].copy_from_slice(&(i as u64).to_be_bytes());
        write_u64_le(weight, i * 8, 1);
        write_u64_le(type_code, i * 8, c.type_code as u64);
        write_u64_le(flags, i * 8, c.flags);
        names[i * 16..(i + 1) * 16].copy_from_slice(&encode_german_string(c.name, &mut blob));
    }

    // `null` stays all-zero: no meta-schema column is nullable.
    let regions: [&[u8]; SCHEMA_BLOCK_REGIONS] = [pk, weight, null, type_code, flags, names, &blob];
    let end = wal::encode(out, offset, tid, n as u32, &regions, checksum).expect("schema block buffer too small");
    end - offset
}

/// [`encode_into`] into a fresh `Vec`. Always checksummed — the in-place form
/// is the one whose caller varies that.
pub fn encode(tid: u32, cols: &[SchemaBlockCol]) -> Vec<u8> {
    let mut out = vec![0u8; encoded_len(cols)];
    encode_into(&mut out, 0, tid, cols, true);
    out
}

/// A validated meta-schema block, borrowing the bytes it was decoded from.
///
/// Everything a consumer needs is checked here — region shape, `col_idx`
/// ordering, type-code validity, PK eligibility and arity — so building a
/// `Schema` / `SchemaDescriptor` from it cannot fail on wire grounds.
pub struct SchemaBlock<'a> {
    block: &'a [u8],
    count: usize,
    tc_off: usize,
    fl_off: usize,
    name_off: usize,
    blob: &'a [u8],
    pk_indices: [u32; MAX_PK_COLUMNS],
    pk_count: usize,
}

impl<'a> SchemaBlock<'a> {
    /// Decode and validate `block`.
    ///
    /// `max_pk` bounds the PK arity this consumer can represent. The two sides
    /// pass different values on purpose and they are **not** merged: the client
    /// passes `PK_LIST_MAX_COLS`, the capacity of the persisted PK-list codec it
    /// must round-trip a key through; the engine passes `MAX_PK_COLUMNS`, the
    /// width of its internal secondary-index schema. Folding one into the other
    /// would silently widen the client-facing limit.
    pub fn decode(block: &'a [u8], verify_checksum: bool, max_pk: usize) -> Result<Self, &'static str> {
        debug_assert!(max_pk <= MAX_PK_COLUMNS, "max_pk exceeds the PK-array capacity");
        // Format validity — header size, version, `total_size` in bounds, body
        // checksum, region count ≤ cap, and every region's extent within the
        // block — is the shared framer's job. What is added below is schema
        // conformance only; the parsed directory lands in `offs`/`sizes` with
        // every extent already bounded.
        let mut offs = [0u64; wal::MAX_WIRE_REGIONS];
        let mut sizes = [0u32; wal::MAX_WIRE_REGIONS];
        let header = wal::validate_and_parse(block, &mut offs, &mut sizes, verify_checksum).map_err(|e| match e {
            crate::WalError::InvalidVersion => "schema block wrong version",
            crate::WalError::ChecksumMismatch => "schema block checksum mismatch",
            // Region count exceeds the directory cap — a forged over-long header.
            crate::WalError::InvalidShard => "schema block directory overflows buffer",
            // Truncated / BufferTooSmall: shorter than its header, directory, or
            // a declared region requires.
            _ => "schema block truncated",
        })?;

        let count = header.entry_count as usize;
        if count == 0 {
            return Err("empty schema block");
        }
        if count > MAX_COLUMNS {
            return Err("schema exceeds column limit");
        }
        // Exact, not a lower bound: a block with fewer regions still has every
        // region this decoder reads, so a lower bound would accept a block the
        // producer never emits and the peer rejects.
        if header.num_regions as usize != SCHEMA_BLOCK_REGIONS {
            return Err("schema block region count mismatch");
        }

        let reg = |r: usize| (offs[r] as usize, sizes[r] as usize);
        let (pk_off, pk_sz) = reg(REG_PK);
        let (null_off, _) = reg(REG_NULL_BMP);
        let (tc_off, _) = reg(REG_TYPE_CODE);
        let (fl_off, _) = reg(REG_FLAGS);
        let (name_off, _) = reg(REG_NAME);
        let (blob_off, blob_sz) = reg(REG_BLOB);

        // Every fixed-stride region must be exactly `count` rows wide, against
        // the same table `block_len` sizes them with.
        for (r, &per_row) in ROW_BYTES.iter().enumerate().take(REG_BLOB) {
            if sizes[r] as usize != count * per_row {
                return Err("schema block region size mismatch");
            }
        }

        // No meta-schema column is nullable, so the encoder writes an all-zero
        // null word per row. Checked rather than assumed: a set bit would mean
        // the type, flags or name of that column is absent, and every read below
        // takes the cell at face value.
        for i in 0..count {
            if read_u64_le(block, null_off + i * 8) != 0 {
                return Err("schema block declares a null column field");
            }
        }

        // `col_idx` must be exactly `[0, 1, …, count-1]`: consumers address
        // columns by physical row position, so a gap, a duplicate, or a reorder
        // would silently re-route a column's type. It is an unsigned U64 PK
        // stored OPK, hence the big-endian read.
        let pk_data = &block[pk_off..pk_off + pk_sz];
        for i in 0..count {
            if u64::from_be_bytes(pk_data[i * 8..(i + 1) * 8].try_into().unwrap()) != i as u64 {
                return Err("schema col_idx not in monotonic order");
            }
        }

        let blob = &block[blob_off..blob_off + blob_sz];
        let mut pk_pairs = [(0u8, 0u32); MAX_PK_COLUMNS];
        let mut pk_count = 0usize;

        for i in 0..count {
            let raw_tc = read_u64_le(block, tc_off + i * 8);
            // The whole word, not its low byte: truncating here would admit a
            // block the peer's `u8`-typed decode rejects, so the two ends would
            // disagree on which blocks are admissible.
            if raw_tc > u8::MAX as u64 || !is_valid_type_code(raw_tc as u8) {
                return Err("schema: invalid type code");
            }
            let tc = raw_tc as u8;
            let fl = read_u64_le(block, fl_off + i * 8);

            // A name cell whose heap extent overruns the blob is a decode error,
            // not an empty name: the name is the column's identity downstream.
            let cell = &block[name_off + i * 16..name_off + (i + 1) * 16];
            let len = crate::read_u32_le(cell, 0) as usize;
            if len > SHORT_STRING_THRESHOLD && blob_extent(blob.len(), read_u64_le(cell, 8), len).is_none() {
                return Err("schema name blob arena out of bounds");
            }

            if let Some(pos) = col_meta_pk_pos(fl) {
                // Rejected here rather than at each side's schema constructor,
                // whose asserts would abort the process on a nullable/STRING/BLOB
                // key and which accept float keys the byte-equal key contract
                // rules out.
                if col_meta_nullable(fl) {
                    return Err("PK column must be non-nullable");
                }
                if !is_pk_eligible(tc) {
                    return Err("PK column type not PK-eligible");
                }
                if pk_count >= max_pk {
                    return Err("too many PK columns");
                }
                pk_pairs[pk_count] = (pos, i as u32);
                pk_count += 1;
            }
        }
        if pk_count == 0 {
            return Err("no PK column");
        }

        // Sort by position in the PK tuple, so `PRIMARY KEY (b, a)` decodes back
        // to the order the user declared. Single-PK schemas all carry position
        // 0, making this a no-op on the common path.
        pk_pairs[..pk_count].sort_by_key(|(p, _)| *p);
        let mut pk_indices = [0u32; MAX_PK_COLUMNS];
        for (k, (_, ci)) in pk_pairs[..pk_count].iter().enumerate() {
            pk_indices[k] = *ci;
        }

        Ok(SchemaBlock {
            block,
            count,
            tc_off,
            fl_off,
            name_off,
            blob,
            pk_indices,
            pk_count,
        })
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.count
    }

    /// Column `i`. Panics past `num_columns()`.
    pub fn column(&self, i: usize) -> SchemaBlockCol<'a> {
        assert!(i < self.count, "schema block column {i} out of range");
        let cell = &self.block[self.name_off + i * 16..self.name_off + (i + 1) * 16];
        SchemaBlockCol {
            type_code: read_u64_le(self.block, self.tc_off + i * 8) as u8,
            flags: read_u64_le(self.block, self.fl_off + i * 8),
            // `decode` bounded every long cell's heap extent, so this resolves
            // to the real content rather than the degraded-empty fallback.
            name: german_string_content(cell, self.blob),
        }
    }

    /// Every column in physical order.
    pub fn columns(&self) -> impl ExactSizeIterator<Item = SchemaBlockCol<'a>> + '_ {
        (0..self.count).map(|i| self.column(i))
    }

    /// The PK column indices, in declared PK-tuple order. Never empty.
    #[inline]
    pub fn pk_indices(&self) -> &[u32] {
        &self.pk_indices[..self.pk_count]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pack_col_meta_flags, TypeCode, PK_LIST_MAX_COLS};

    fn col(tc: TypeCode, name: &str, pk_pos: Option<u8>, nullable: bool) -> SchemaBlockCol<'_> {
        SchemaBlockCol {
            type_code: tc as u8,
            flags: pack_col_meta_flags(nullable, false, false, pk_pos),
            name: name.as_bytes(),
        }
    }

    /// The decode error `block` yields, without needing `SchemaBlock: Debug`.
    fn decode_err(block: &[u8], max_pk: usize) -> &'static str {
        SchemaBlock::decode(block, true, max_pk)
            .err()
            .expect("expected a rejection")
    }

    fn simple() -> Vec<u8> {
        let cols = [
            col(TypeCode::U64, "id", Some(0), false),
            col(TypeCode::String, "a_rather_long_column_name", None, true),
        ];
        encode(7, &cols)
    }

    #[test]
    fn roundtrips_shape_names_and_pk_order() {
        let cols = [
            col(TypeCode::U64, "b", Some(1), false),
            col(TypeCode::I32, "a", Some(0), false),
            col(TypeCode::String, "payload_name_over_twelve", None, true),
        ];
        let block = encode(42, &cols);
        let sb = SchemaBlock::decode(&block, true, MAX_PK_COLUMNS).unwrap();
        assert_eq!(sb.num_columns(), 3);
        assert_eq!(sb.columns().collect::<Vec<_>>(), cols.to_vec());
        // Declared order `(a, b)`, not column order `(b, a)`.
        assert_eq!(sb.pk_indices(), &[1, 0]);
    }

    #[test]
    fn encoded_len_matches_what_encode_writes() {
        let cols = [
            col(TypeCode::U64, "id", Some(0), false),
            col(TypeCode::String, "spills_into_the_blob_heap", None, false),
        ];
        assert_eq!(encode(1, &cols).len(), encoded_len(&cols));
    }

    /// The count-only sizing path must agree with the general one, or a caller
    /// that sizes a buffer through it writes past the end.
    #[test]
    fn anonymous_encoded_len_agrees_with_the_general_size() {
        for n in [1usize, 2, 7, MAX_COLUMNS] {
            let cols: Vec<SchemaBlockCol> = (0..n)
                .map(|i| col(TypeCode::U64, "", (i == 0).then_some(0), false))
                .collect();
            assert_eq!(anonymous_encoded_len(n), encoded_len(&cols), "n = {n}");
            assert_eq!(anonymous_encoded_len(n), encode(1, &cols).len(), "n = {n}");
        }
    }

    /// A type_code word whose low byte names a valid type but whose high bits
    /// are set. Truncating to `u8` accepts it; the peer's `u8`-typed decode
    /// rejects it — so the two ends would disagree on which blocks are legal.
    #[test]
    fn rejects_a_type_code_that_only_fits_after_truncation() {
        let mut block = simple();
        let mut offs = [0u64; wal::MAX_WIRE_REGIONS];
        let mut sizes = [0u32; wal::MAX_WIRE_REGIONS];
        wal::validate_and_parse(&block, &mut offs, &mut sizes, false).unwrap();
        let tc_off = offs[REG_TYPE_CODE] as usize;
        // Low byte 2 is a valid type code; the top 32 bits are not part of it.
        write_u64_le(&mut block, tc_off, 0x0000_0001_0000_0002);
        let n = block.len();
        wal::stamp_checksum(&mut block, n);
        assert_eq!(decode_err(&block, MAX_PK_COLUMNS), "schema: invalid type code");
    }

    /// A block with fewer regions than the meta-schema shape. Every region this
    /// decoder reads still fits, so a lower-bound check would accept it.
    #[test]
    fn rejects_a_block_with_the_wrong_region_count() {
        let block = simple();
        let n = 5u32; // pk, weight, null, type_code, flags — no name, no blob
        let mut short = block.clone();
        crate::write_u32_le(&mut short, crate::WAL_OFF_NUM_REGIONS, n);
        let n = short.len();
        wal::stamp_checksum(&mut short, n);
        assert_eq!(decode_err(&short, MAX_PK_COLUMNS), "schema block region count mismatch");
    }

    /// `max_pk` is the consumer's own limit: the same block is admissible to the
    /// engine (5) and not to the client (4).
    #[test]
    fn pk_arity_is_bounded_by_the_caller_not_a_shared_cap() {
        let cols: Vec<SchemaBlockCol> = (0..5)
            .map(|i| SchemaBlockCol {
                type_code: TypeCode::U64 as u8,
                flags: pack_col_meta_flags(false, false, false, Some(i as u8)),
                name: b"k",
            })
            .collect();
        let block = encode(1, &cols);
        assert_eq!(
            SchemaBlock::decode(&block, true, MAX_PK_COLUMNS)
                .unwrap()
                .pk_indices()
                .len(),
            5
        );
        assert_eq!(decode_err(&block, PK_LIST_MAX_COLS), "too many PK columns");
    }

    /// The block asserts no column field is null; a set bit would mean the
    /// type, flags or name of that column is absent from the cells the decoder
    /// nonetheless reads.
    #[test]
    fn rejects_a_row_claiming_a_null_column_field() {
        let mut block = simple();
        let mut offs = [0u64; wal::MAX_WIRE_REGIONS];
        let mut sizes = [0u32; wal::MAX_WIRE_REGIONS];
        wal::validate_and_parse(&block, &mut offs, &mut sizes, false).unwrap();
        write_u64_le(&mut block, offs[REG_NULL_BMP] as usize, 1);
        let n = block.len();
        wal::stamp_checksum(&mut block, n);
        assert_eq!(
            decode_err(&block, MAX_PK_COLUMNS),
            "schema block declares a null column field"
        );
    }

    #[test]
    fn rejects_out_of_order_col_idx() {
        let mut block = simple();
        let mut offs = [0u64; wal::MAX_WIRE_REGIONS];
        let mut sizes = [0u32; wal::MAX_WIRE_REGIONS];
        wal::validate_and_parse(&block, &mut offs, &mut sizes, false).unwrap();
        let pk_off = offs[REG_PK] as usize;
        block[pk_off..pk_off + 8].copy_from_slice(&5u64.to_be_bytes());
        let n = block.len();
        wal::stamp_checksum(&mut block, n);
        assert_eq!(
            decode_err(&block, MAX_PK_COLUMNS),
            "schema col_idx not in monotonic order"
        );
    }

    #[test]
    fn rejects_a_nullable_or_ineligible_pk_column() {
        let nullable = [col(TypeCode::U64, "id", Some(0), true)];
        assert_eq!(
            decode_err(&encode(1, &nullable), MAX_PK_COLUMNS),
            "PK column must be non-nullable"
        );
        let float = [col(TypeCode::F64, "id", Some(0), false)];
        assert_eq!(
            decode_err(&encode(1, &float), MAX_PK_COLUMNS),
            "PK column type not PK-eligible"
        );
    }

    #[test]
    fn rejects_a_block_with_no_pk_column() {
        let cols = [col(TypeCode::U64, "a", None, false)];
        assert_eq!(decode_err(&encode(1, &cols), MAX_PK_COLUMNS), "no PK column");
    }

    #[test]
    fn rejects_a_name_cell_pointing_past_the_blob_heap() {
        let mut block = simple();
        let mut offs = [0u64; wal::MAX_WIRE_REGIONS];
        let mut sizes = [0u32; wal::MAX_WIRE_REGIONS];
        wal::validate_and_parse(&block, &mut offs, &mut sizes, false).unwrap();
        // Row 1's name is the long one; push its heap offset past the heap.
        let cell = offs[REG_NAME] as usize + 16;
        write_u64_le(&mut block, cell + 8, 1 << 20);
        let n = block.len();
        wal::stamp_checksum(&mut block, n);
        assert_eq!(
            decode_err(&block, MAX_PK_COLUMNS),
            "schema name blob arena out of bounds"
        );
    }
}
