//! Secondary index integration: GiDesc, AviDesc, op_integrate_with_indexes.

use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::Batch;

// ---------------------------------------------------------------------------
// Public descriptor types
// ---------------------------------------------------------------------------

/// GI/AVI descriptor for integrate_with_indexes.
///
/// The `table` raw pointer is `pub(crate)` rather than `pub`: it is a bare
/// `*mut Table` whose access is constrained to crate-internal call sites
/// (the compiler emits the desc in `vm`, `op_integrate_with_indexes` consumes
/// it) — never a cross-boundary handle.
pub struct GiDesc {
    pub(crate) table: *mut crate::storage::Table,
    pub(crate) col_idx: u32,
}

pub struct AviDesc {
    pub(crate) table: *mut crate::storage::Table,
    pub(crate) for_max: bool,
    pub(crate) agg_col_type_code: u8,
    pub(crate) group_by_cols: Vec<u32>,
    pub(crate) agg_col_idx: u32,
}

/// Width of the GI key's leading `gc` (group-code) prefix: a single U64. The
/// source PK columns follow at byte offset `GI_GC_BYTES`.
pub(crate) const GI_GC_BYTES: usize = 8;

/// GI index PK = [U64 gc] ++ source PK columns, all in the PK region; no
/// payload. Signed source PK columns are mapped to their unsigned counterpart
/// of the same width: GI ordering only groups entries by the `gc` prefix, so
/// within-prefix order is irrelevant, but a signed column would sort negative
/// source PKs *before* the zero-padded prefix-seek key and the scan would skip
/// them. The remap keeps the byte width (pk_stride unchanged) and equality
/// (consolidation holds), and makes 0 the true minimum so a zero-padded prefix
/// seek lands on the first entry of the group.
pub(crate) fn make_gi_schema(src: &SchemaDescriptor) -> SchemaDescriptor {
    // gc + src PK columns must fit MAX_PK_COLUMNS.
    assert!(
        src.pk_indices().len() < crate::schema::MAX_PK_COLUMNS,
        "make_gi_schema: source PK column count {} leaves no room for the gc \
         prefix within MAX_PK_COLUMNS={}",
        src.pk_indices().len(),
        crate::schema::MAX_PK_COLUMNS,
    );
    // gc + source pk_stride must fit MAX_PK_BYTES.
    assert!(
        GI_GC_BYTES + src.pk_stride() as usize <= crate::schema::MAX_PK_BYTES,
        "make_gi_schema: gc({GI_GC_BYTES}) + source pk_stride {} exceeds MAX_PK_BYTES={}",
        src.pk_stride(),
        crate::schema::MAX_PK_BYTES,
    );
    let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
    let mut pk = vec![0u32];
    for (ord, _ci, col) in src.pk_columns() {
        let mut c = *col;
        c.type_code = match col.type_code {
            type_code::I8 => type_code::U8,
            type_code::I16 => type_code::U16,
            type_code::I32 => type_code::U32,
            type_code::I64 => type_code::U64,
            other => other,
        };
        cols.push(c);
        pk.push((ord + 1) as u32);
    }
    SchemaDescriptor::new(&cols, &pk)
}

/// AVI index schema: the group-by columns (native fixed-width types, in GROUP
/// BY order) followed by the order-encoded aggregate value (U64). All columns
/// are PK; there is no payload. Built only for byte-form-eligible group keys
/// (see `query::compiler::avi_group_key_eligible`).
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

    // `op_reduce` now ships its retract+insert output as an honest unconsolidated
    // delta (sorted = consolidated = false), so the table ingest re-sorts/folds it;
    // no defensive flag-scrub is needed here. `clone_batch` stays: `batch` is
    // borrowed and read again by the GI/AVI population below.
    if let Some(table) = target_table {
        table.ingest_owned_batch_memonly(batch.clone_batch())?;
    }

    let mb = batch.as_mem_batch();

    // GroupIndex population
    if let Some(gi_desc) = gi {
        let gi_schema = make_gi_schema(input_schema);
        // `Raw` from `with_schema`; the `extend_*` population below never raises
        // the layout, so the table ingest re-sorts/folds it.
        let mut gi_batch = Batch::with_schema(gi_schema, batch.count);

        let gi_col = gi_desc.col_idx as usize;
        let gc_extractor = super::util::IndexColExtractor::new(input_schema, gi_col);

        // Each row overwrites a constant-length `key[..GI_GC_BYTES + src_pk.len()]`
        // window, so no stale trailing bytes can leak across iterations.
        let mut key = [0u8; crate::schema::MAX_PK_BYTES];
        for row in 0..batch.count {
            // PK is never null (catalog rule: pk-not-null.md); the extractor's
            // locator returns false for a PK column and checks the null-bitmap
            // bit for a payload column.
            if gc_extractor.is_null(&mb, row) {
                continue;
            }

            let gc_u64 = gc_extractor.extract(&mb, row);
            let weight = mb.get_weight(row);

            key[..GI_GC_BYTES].copy_from_slice(&gc_u64.to_le_bytes());
            let src_pk = mb.get_pk_bytes(row);
            key[GI_GC_BYTES..GI_GC_BYTES + src_pk.len()].copy_from_slice(src_pk);

            gi_batch.extend_pk_bytes(&key[..GI_GC_BYTES + src_pk.len()]);
            gi_batch.extend_weight(&weight.to_le_bytes());
            gi_batch.extend_null_bmp(&0u64.to_le_bytes());
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
        // `Raw` from `with_schema`; the `extend_*` population below never raises it.
        let mut avi_batch = Batch::with_schema(avi_schema, batch.count);

        let avi_col = avi_desc.agg_col_idx as usize;
        // Resolve the aggregate column's location once (loop-invariant): a PK
        // aggregate is sliced from the packed PK region, a payload aggregate read
        // from its dense slot.
        let avi_loc = input_schema.locate(avi_col);

        let extractor = super::util::GroupKeyExtractor::new(input_schema, &avi_desc.group_by_cols);
        let n = extractor.stride;
        let mut key = [0u8; crate::schema::MAX_PK_BYTES];
        for row in 0..batch.count {
            // PK is never null; a NULL payload aggregate is skipped.
            if avi_loc.is_null(&mb, row) {
                continue;
            }

            extractor.gather(&mb, row, &mut key);
            let av_bytes = avi_loc.bytes(&mb, row);
            let av_u64 = super::util::encode_ordered(av_bytes, avi_desc.agg_col_type_code, avi_desc.for_max);
            // Serialise the order-encoded value big-endian: the index orders
            // entries by raw lexicographic byte comparison, so big-endian bytes
            // make lexicographic order match the encoded value's numeric order.
            key[n..n + super::util::AVI_AV_BYTES].copy_from_slice(&av_u64.to_be_bytes());

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
    use super::super::util::{decode_ordered, encode_ordered};
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
