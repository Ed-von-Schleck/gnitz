use super::error::ProtocolError;
use super::header::{
    META_FLAG_NULLABLE, META_FLAG_IS_PK,
    META_FLAG_PK_POS_SHIFT, META_FLAG_PK_POS_MASK,
};
use super::types::{
    BatchAppender, ColData, ColumnDef, Schema, ZSetBatch, meta_schema,
    type_code_from_u64, MAX_COLUMNS,
};

/// Convert a Schema to a META_SCHEMA-shaped ZSetBatch (one row per column).
/// Mirrors Python's `schema_to_batch`.
pub fn schema_to_batch(schema: &Schema) -> ZSetBatch {
    let ms = meta_schema();
    let mut batch = ZSetBatch::new(ms);

    // Route through the validated, schema-aware appender so the META_SCHEMA
    // layout lives in exactly one place (`meta_schema()`). The cursor skips the
    // `col_idx` PK column, so the chained values land in cols 1/2/3.
    {
        let mut appender = BatchAppender::new(&mut batch, ms);
        for (ci, col) in schema.columns.iter().enumerate() {
            let mut flags: u64 = 0;
            if col.is_nullable { flags |= META_FLAG_NULLABLE; }
            // Encode position-in-PK-tuple so compound `PRIMARY KEY (b, a)`
            // decodes back to the user-declared order.
            if let Some(pos) = schema.pk_cols.iter().position(|&p| p == ci) {
                flags |= META_FLAG_IS_PK;
                flags |= (pos as u64) << META_FLAG_PK_POS_SHIFT;
            }
            appender.add_row(ci as u128, 1)
                .u64_val(col.type_code as u64)
                .u64_val(flags)
                .str_val(&col.name);
        }
    }

    batch
}

/// Reconstruct a Schema from a META_SCHEMA-shaped ZSetBatch.
/// Mirrors Python's `batch_to_schema`.
pub fn batch_to_schema(batch: &ZSetBatch) -> Result<Schema, ProtocolError> {
    let count = batch.len();
    if count > MAX_COLUMNS {
        return Err(ProtocolError::DecodeError("schema exceeds column limit".into()));
    }
    let mut columns: Vec<ColumnDef> = Vec::with_capacity(count);
    // Collect (position, column_idx) so we can sort by position before
    // building `pk_cols`. Single-PK schemas all carry position 0 and the
    // sort is a no-op.
    let mut pk_pairs: Vec<(u8, usize)> = Vec::new();

    let type_code_fixed = match &batch.columns[1] {
        ColData::Fixed(v) => v,
        _ => return Err(ProtocolError::DecodeError("col 1 (type_code) must be Fixed".into())),
    };
    let flags_fixed = match &batch.columns[2] {
        ColData::Fixed(v) => v,
        _ => return Err(ProtocolError::DecodeError("col 2 (flags) must be Fixed".into())),
    };
    let names_col = match &batch.columns[3] {
        ColData::Strings(v) => v,
        _ => return Err(ProtocolError::DecodeError("col 3 (name) must be Strings".into())),
    };

    for i in 0..count {
        let col_idx = batch.pks.get(i) as u64;
        if col_idx != i as u64 {
            return Err(ProtocolError::DecodeError(format!(
                "schema batch col_idx out of order: expected {}, got {}", i, col_idx
            )));
        }

        let type_code_raw = u64::from_le_bytes(
            type_code_fixed[i * 8..(i + 1) * 8].try_into().unwrap()
        );
        let flags = u64::from_le_bytes(
            flags_fixed[i * 8..(i + 1) * 8].try_into().unwrap()
        );
        let name = match &names_col[i] {
            Some(s) => s.clone(),
            None => return Err(ProtocolError::DecodeError(format!("null name at col {}", i))),
        };

        let tc = type_code_from_u64(type_code_raw)?;
        let is_nullable = (flags & META_FLAG_NULLABLE) != 0;
        let is_pk       = (flags & META_FLAG_IS_PK)    != 0;

        if is_pk {
            let pos = ((flags & META_FLAG_PK_POS_MASK) >> META_FLAG_PK_POS_SHIFT) as u8;
            pk_pairs.push((pos, i));
        }

        columns.push(ColumnDef { name, type_code: tc, is_nullable, fk_table_id: 0, fk_col_idx: 0 });
    }

    pk_pairs.sort_by_key(|(p, _)| *p);
    let pk_cols: Vec<usize> = pk_pairs.into_iter().map(|(_, i)| i).collect();

    Schema::validate_pk_cols(&pk_cols, columns.len())
        .map_err(|e| ProtocolError::DecodeError(e.into()))?;
    // PK columns must additionally be non-nullable and PK-eligible — the same
    // invariants the engine's SchemaDescriptor::new hard-asserts.
    for &pk in &pk_cols {
        if columns[pk].is_nullable {
            return Err(ProtocolError::DecodeError("PK column must be non-nullable".into()));
        }
        if !columns[pk].type_code.is_pk_eligible() {
            return Err(ProtocolError::DecodeError("PK column type not PK-eligible".into()));
        }
    }

    Ok(Schema { columns, pk_cols })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{ColData, Schema, ColumnDef, TypeCode, ZSetBatch, meta_schema};
    use crate::protocol::wal_block::{encode_wal_block, decode_wal_block, VerifyChecksum};

    // ── type_code_from_u64 error paths ──────────────────────────────────────

    #[test]
    fn test_unknown_type_code_zero() {
        use crate::protocol::error::ProtocolError;
        use crate::protocol::types::type_code_from_u64;
        assert!(matches!(type_code_from_u64(0), Err(ProtocolError::UnknownTypeCode(0))));
    }

    #[test]
    fn test_unknown_type_code_16() {
        // 16 is the first unassigned type-code value after I128 (15).
        use crate::protocol::error::ProtocolError;
        use crate::protocol::types::type_code_from_u64;
        assert!(matches!(type_code_from_u64(16), Err(ProtocolError::UnknownTypeCode(16))));
    }

    #[test]
    fn test_unknown_type_code_max() {
        use crate::protocol::error::ProtocolError;
        use crate::protocol::types::type_code_from_u64;
        assert!(matches!(type_code_from_u64(u64::MAX), Err(ProtocolError::UnknownTypeCode(_))));
    }

    // ── batch_to_schema error paths ──────────────────────────────────────────

    /// Build a valid META_SCHEMA batch for `ncols` columns (all U64, col 0 is PK).
    /// Routes through the real `schema_to_batch` encoder so the META_SCHEMA layout
    /// is defined in exactly one place; the error-path tests then corrupt the
    /// returned batch to exercise `batch_to_schema`'s validation.
    fn make_meta_batch(ncols: usize) -> ZSetBatch {
        let schema = Schema {
            columns: (0..ncols).map(|i| ColumnDef {
                name:        format!("col{}", i),
                type_code:   TypeCode::U64,
                is_nullable: false,
                fk_table_id: 0,
                fk_col_idx:  0,
            }).collect(),
            pk_cols: vec![0],
        };
        schema_to_batch(&schema)
    }

    #[test]
    fn test_batch_to_schema_no_pk_flag() {
        use crate::protocol::error::ProtocolError;
        let mut batch = make_meta_batch(2);
        // Clear all IS_PK flags
        if let ColData::Fixed(ref mut v) = batch.columns[2] {
            for b in v.iter_mut() { *b = 0; }
        }
        let res = batch_to_schema(&batch);
        assert!(matches!(res, Err(ProtocolError::DecodeError(_))));
    }

    #[test]
    fn test_batch_to_schema_col_idx_out_of_order() {
        use crate::protocol::error::ProtocolError;
        let mut batch = make_meta_batch(2);
        batch.pks.swap(0, 1); // [1, 0] instead of [0, 1]
        let res = batch_to_schema(&batch);
        assert!(matches!(res, Err(ProtocolError::DecodeError(_))));
    }

    #[test]
    fn test_batch_to_schema_col_idx_gap() {
        use crate::protocol::error::ProtocolError;
        let mut batch = make_meta_batch(2);
        batch.pks.set_u128(1, 5); // gap: [0, 5]
        let res = batch_to_schema(&batch);
        assert!(matches!(res, Err(ProtocolError::DecodeError(_))));
    }

    #[test]
    fn test_batch_to_schema_col_idx_duplicate() {
        use crate::protocol::error::ProtocolError;
        let mut batch = make_meta_batch(2);
        batch.pks.set_u128(1, 0); // duplicate: [0, 0]
        let res = batch_to_schema(&batch);
        assert!(matches!(res, Err(ProtocolError::DecodeError(_))));
    }

    #[test]
    fn test_batch_to_schema_nullable_pk_rejected() {
        use crate::protocol::error::ProtocolError;
        use crate::protocol::header::META_FLAG_NULLABLE;
        let mut batch = make_meta_batch(2);
        // Set the NULLABLE flag on the PK column (col 0).
        if let ColData::Fixed(ref mut v) = batch.columns[2] {
            let f = u64::from_le_bytes(v[0..8].try_into().unwrap()) | META_FLAG_NULLABLE;
            v[0..8].copy_from_slice(&f.to_le_bytes());
        }
        let err = batch_to_schema(&batch).unwrap_err();
        assert!(matches!(err, ProtocolError::DecodeError(ref m) if m.contains("non-nullable")));
    }

    #[test]
    fn test_batch_to_schema_ineligible_pk_rejected() {
        use crate::protocol::error::ProtocolError;
        let mut batch = make_meta_batch(2);
        // Retype the PK column (col 0) to F64 (10), which is not PK-eligible.
        if let ColData::Fixed(ref mut v) = batch.columns[1] {
            v[0..8].copy_from_slice(&10u64.to_le_bytes());
        }
        let err = batch_to_schema(&batch).unwrap_err();
        assert!(matches!(err, ProtocolError::DecodeError(ref m) if m.contains("PK-eligible")));
    }

    #[test]
    fn test_batch_to_schema_too_many_pk_rejected() {
        use crate::protocol::error::ProtocolError;
        use crate::protocol::header::META_FLAG_IS_PK;
        // PK_LIST_MAX_COLS is 4; flag 6 columns as PK.
        let mut batch = make_meta_batch(6);
        if let ColData::Fixed(ref mut v) = batch.columns[2] {
            for i in 0..6 {
                v[i * 8..i * 8 + 8].copy_from_slice(&META_FLAG_IS_PK.to_le_bytes());
            }
        }
        let err = batch_to_schema(&batch).unwrap_err();
        assert!(matches!(err, ProtocolError::DecodeError(_)));
    }

    #[test]
    fn test_batch_to_schema_exceeds_column_limit_rejected() {
        use crate::protocol::error::ProtocolError;
        let batch = make_meta_batch(MAX_COLUMNS + 1);
        let err = batch_to_schema(&batch).unwrap_err();
        assert!(matches!(err, ProtocolError::DecodeError(ref m) if m.contains("column limit")));
    }

    // ── schema/meta roundtrip ────────────────────────────────────────────────

    #[test]
    fn test_schema_meta_roundtrip() {
        let original = Schema {
            columns: vec![
                ColumnDef { name: "id".into(),    type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "name".into(),  type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "score".into(), type_code: TypeCode::F64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "tag".into(),   type_code: TypeCode::I32,    is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "uuid".into(),  type_code: TypeCode::U128,   is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
        };

        let ms = meta_schema();
        let meta_batch = schema_to_batch(&original);
        let encoded = encode_wal_block(ms, 0, &meta_batch);
        let (decoded_batch, _, _) = decode_wal_block(&encoded, ms, VerifyChecksum::Yes).unwrap();
        let reconstructed = batch_to_schema(&decoded_batch).unwrap();

        assert_eq!(original, reconstructed, "schema mismatch after meta roundtrip");
    }
}
