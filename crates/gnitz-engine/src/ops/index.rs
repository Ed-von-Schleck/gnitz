//! Secondary index integration: GiDesc, AviDesc, op_integrate_with_indexes.

use crate::schema::{SchemaDescriptor, SchemaColumn, type_code};
use crate::storage::Batch;

// ---------------------------------------------------------------------------
// Public descriptor types
// ---------------------------------------------------------------------------

/// GI/AVI descriptor for integrate_with_indexes.
pub struct GiDesc {
    pub table: *mut crate::storage::Table,
    pub col_idx: u32,
}

pub struct AviDesc {
    pub table: *mut crate::storage::Table,
    pub for_max: bool,
    pub agg_col_type_code: u8,
    pub group_by_cols: Vec<u32>,
    pub agg_col_idx: u32,
}

/// GI schema: U128 PK + I64 payload (spk_hi).
pub(crate) fn make_gi_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// AVI index schema: the group-by columns (native fixed-width types, in GROUP
/// BY order) followed by the order-encoded aggregate value (U64). All columns
/// are PK; there is no payload. Built only for byte-form-eligible group keys
/// (see `compiler::avi_group_key_eligible`).
pub(crate) fn make_avi_schema(src: &SchemaDescriptor, group_by_cols: &[u32]) -> SchemaDescriptor {
    let mut cols = Vec::with_capacity(group_by_cols.len() + 1);
    let mut pk = Vec::with_capacity(group_by_cols.len() + 1);
    for (i, &c) in group_by_cols.iter().enumerate() {
        // Force non-nullable: eligibility guarantees the value is always present.
        cols.push(SchemaColumn::new(src.columns[c as usize].type_code, 0));
        pk.push(i as u32);
    }
    cols.push(SchemaColumn::new(type_code::U64, 0)); // av_encoded
    pk.push(group_by_cols.len() as u32);
    let schema = SchemaDescriptor::new(&cols, &pk);
    // Guard against a gate relaxation overshooting the engine PK limit. The
    // byte-form cursor orders wide keys via compare_pk_bytes, so the bound is
    // MAX_PK_BYTES, not the narrow cap.
    debug_assert!(
        schema.pk_stride() as usize <= crate::schema::MAX_PK_BYTES,
        "make_avi_schema: composite key {} exceeds MAX_PK_BYTES",
        schema.pk_stride(),
    );
    schema
}

// ---------------------------------------------------------------------------
// op_integrate_with_indexes
// ---------------------------------------------------------------------------

/// Integrate a delta batch into a target table, optionally populating
/// GroupIndex and AggValueIndex secondary indexes.
///
/// The Rust Table handles memtable capacity internally (flush-on-overflow).
pub fn op_integrate_with_indexes(
    batch: &Batch,
    target_table: Option<&mut crate::storage::Table>,
    input_schema: &SchemaDescriptor,
    gi: Option<&GiDesc>,
    avi: Option<&AviDesc>,
) -> Result<(), crate::storage::StorageError> {
    if batch.count == 0 {
        return Ok(());
    }

    // `reduce` for non-group-by-PK aggregates may emit retract+insert pairs
    // that are physically unsorted while claiming `consolidated = true`.
    if let Some(table) = target_table {
        let mut cloned = batch.clone_batch();
        cloned.sorted = false;
        cloned.consolidated = false;
        table.ingest_owned_batch_memonly(cloned)?;
    }

    let mb = batch.as_mem_batch();

    // GroupIndex population
    if let Some(gi_desc) = gi {
        let gi_schema = make_gi_schema();
        let mut gi_batch = Batch::with_schema(gi_schema, batch.count);
        gi_batch.sorted = false;
        gi_batch.consolidated = false;

        let gi_col = gi_desc.col_idx as usize;
        let gi_is_pk = input_schema.is_pk_col(gi_col);
        // PK has no payload-index slot; the null-bit probe is skipped below,
        // so a dummy 0 is safe.
        let gi_pi = if gi_is_pk { 0 } else { input_schema.payload_idx(gi_col) };
        let gc_extractor = super::util::IndexColExtractor::new(input_schema, gi_col);

        for row in 0..batch.count {
            // PK is never null (catalog rule: pk-not-null.md), so its dense
            // bit position is undefined and the null probe is skipped.
            if !gi_is_pk && (mb.get_null_word(row) >> gi_pi) & 1 != 0 {
                continue;
            }

            let source_pk = mb.get_pk(row);
            let gc_u64 = gc_extractor.extract(&mb, row);
            let source_pk_lo = source_pk as u64;
            let source_pk_hi = (source_pk >> 64) as u64;
            let weight = mb.get_weight(row);

            // Composite key: ck_lo = source_pk_lo, ck_hi = gc_u64
            gi_batch.extend_pk(((gc_u64 as u128) << 64) | (source_pk_lo as u128));
            gi_batch.extend_weight(&weight.to_le_bytes());
            gi_batch.extend_null_bmp(&0u64.to_le_bytes());
            // Payload: spk_hi (source pk high 64 bits) as I64
            gi_batch.extend_col(0, &(source_pk_hi as i64).to_le_bytes());
            gi_batch.count += 1;
        }

        if gi_batch.count > 0 {
            let gi_table = unsafe { &mut *gi_desc.table };
            let _ = gi_table.ingest_owned_batch_memonly(gi_batch);
        }
    }

    // AggValueIndex population. Key layout: group_key_bytes ++ av_encoded(8),
    // ordered group-major then aggregate-minor, so the lookup's prefix walk
    // (`agg::apply_agg_from_value_index`) matches the full group identity — no
    // hash, no collision. Gather against `input_schema`: the delta `mb` is
    // physically laid out in it.
    if let Some(avi_desc) = avi {
        let avi_schema = make_avi_schema(input_schema, &avi_desc.group_by_cols);
        let mut avi_batch = Batch::with_schema(avi_schema, batch.count);
        avi_batch.sorted = false;
        avi_batch.consolidated = false;

        let avi_col = avi_desc.agg_col_idx as usize;
        let avi_is_pk = input_schema.is_pk_col(avi_col);
        // Column width is loop-invariant; resolve it (and the PK byte offset or
        // payload slot) once. A PK aggregate is sliced from the packed PK
        // region, a payload aggregate read from its dense slot.
        let avi_w = input_schema.columns[avi_col].size() as usize;
        let (avi_pk_off, avi_pi) = if avi_is_pk {
            (input_schema.pk_byte_offset(avi_col) as usize, 0)
        } else {
            (0, input_schema.payload_idx(avi_col))
        };

        let extractor = super::util::GroupKeyExtractor::new(input_schema, &avi_desc.group_by_cols);
        let n = extractor.stride;
        let mut key = [0u8; crate::schema::MAX_PK_BYTES];
        for row in 0..batch.count {
            // PK is never null, so its dense bit position is undefined.
            if !avi_is_pk && (mb.get_null_word(row) >> avi_pi) & 1 != 0 {
                continue;
            }

            extractor.gather(&mb, row, &mut key);
            let av_bytes = if avi_is_pk {
                &mb.get_pk_bytes(row)[avi_pk_off..avi_pk_off + avi_w]
            } else {
                mb.get_col_ptr(row, avi_pi, avi_w)
            };
            let av_u64 = super::util::encode_ordered(
                av_bytes, avi_desc.agg_col_type_code, avi_desc.for_max,
            );
            key[n..n + super::util::AVI_AV_BYTES].copy_from_slice(&av_u64.to_le_bytes());

            avi_batch.extend_pk_bytes(&key[..n + super::util::AVI_AV_BYTES]);
            avi_batch.extend_weight(&mb.get_weight(row).to_le_bytes());
            avi_batch.extend_null_bmp(&0u64.to_le_bytes());
            avi_batch.count += 1;
        }

        if avi_batch.count > 0 {
            let avi_table = unsafe { &mut *avi_desc.table };
            let _ = avi_table.ingest_owned_batch_memonly(avi_batch);
        }
    }

    Ok(())
}

#[cfg(test)]
mod avi_encode_tests {
    use super::super::util::{encode_ordered, decode_ordered};
    use crate::schema::{type_code, TypeCode};

    fn decode_i32(enc: u64, for_max: bool) -> i32 {
        decode_ordered(enc, TypeCode::I32, for_max) as i32
    }

    // A signed aggregate (PK or payload) must encode order-preservingly so the
    // ascending AVI cursor walk yields MIN first, and decode back to the value.
    #[test]
    fn i32_encoding_is_order_preserving_and_round_trips() {
        let vals: [i32; 5] = [i32::MIN, -100, -1, 0, 1_000_000];
        let enc: Vec<u64> = vals
            .iter()
            .map(|v| encode_ordered(&v.to_le_bytes(), type_code::I32, false))
            .collect();
        for w in enc.windows(2) {
            assert!(w[0] < w[1], "ascending i32 must encode to ascending u64");
        }
        for (v, &e) in vals.iter().zip(enc.iter()) {
            assert_eq!(decode_i32(e, false), *v, "round-trip i32");
        }
    }

    // for_max inverts the order so the ascending walk yields MAX first.
    #[test]
    fn for_max_inverts_order() {
        let lo = encode_ordered(&(-5i32).to_le_bytes(), type_code::I32, true);
        let hi = encode_ordered(&100i32.to_le_bytes(), type_code::I32, true);
        assert!(lo > hi, "for_max: larger value must encode smaller");
        assert_eq!(decode_i32(hi, true), 100);
    }

    #[test]
    fn f32_encoding_is_order_preserving() {
        let vals: [f32; 4] = [-2.5, -0.5, 0.5, 2.5];
        let enc: Vec<u64> = vals
            .iter()
            .map(|v| encode_ordered(&v.to_bits().to_le_bytes(), type_code::F32, false))
            .collect();
        for w in enc.windows(2) {
            assert!(w[0] < w[1], "ascending f32 must encode to ascending u64");
        }
    }

    // The shared bytes core means a PK aggregate column (sliced out of the
    // packed slot) encodes identically to the same value in a payload column.
    #[test]
    fn pk_slice_and_standalone_encode_identically() {
        let v: i32 = -42;
        // Standalone payload-style 4-byte buffer.
        let payload = encode_ordered(&v.to_le_bytes(), type_code::I32, false);
        // Same value sliced from offset 4 of an 8-byte (U32,U32)-style PK slot.
        let mut slot = [0u8; 8];
        slot[4..8].copy_from_slice(&v.to_le_bytes());
        let pk_slice = encode_ordered(&slot[4..8], type_code::I32, false);
        assert_eq!(payload, pk_slice);
    }

    #[test]
    fn unsigned_encoding_is_raw_value() {
        let v: u32 = 0xABCD_1234;
        let enc = encode_ordered(&v.to_le_bytes(), type_code::U32, false);
        assert_eq!(enc, v as u64);
    }
}
