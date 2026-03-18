use crate::error::ProtocolError;
use crate::header::{META_FLAG_NULLABLE, META_FLAG_IS_PK};
use crate::types::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};

/// Convert a Schema to a META_SCHEMA-shaped ZSetBatch (one row per column).
/// Mirrors Python's `schema_to_batch`.
pub fn schema_to_batch(schema: &Schema) -> ZSetBatch {
    let ncols = schema.columns.len();
    let mut pk_lo   = Vec::with_capacity(ncols);
    let mut pk_hi   = Vec::with_capacity(ncols);
    let mut weights = Vec::with_capacity(ncols);
    let mut nulls   = Vec::with_capacity(ncols);

    // META_SCHEMA: col_idx(pk=0), type_code(U64), flags(U64), name(String)
    let mut type_code_bytes = Vec::with_capacity(ncols * 8);
    let mut flags_bytes     = Vec::with_capacity(ncols * 8);
    let mut names: Vec<Option<String>> = Vec::with_capacity(ncols);

    for (ci, col) in schema.columns.iter().enumerate() {
        pk_lo.push(ci as u64);
        pk_hi.push(0u64);
        weights.push(1i64);
        nulls.push(0u64);

        type_code_bytes.extend_from_slice(&(col.type_code as u64).to_le_bytes());

        let mut flags: u64 = 0;
        if col.is_nullable        { flags |= META_FLAG_NULLABLE; }
        if ci == schema.pk_index  { flags |= META_FLAG_IS_PK; }
        flags_bytes.extend_from_slice(&flags.to_le_bytes());

        names.push(Some(col.name.clone()));
    }

    ZSetBatch {
        pk_lo,
        pk_hi,
        weights,
        nulls,
        columns: vec![
            ColData::Fixed(vec![]),          // col 0 = col_idx (pk placeholder)
            ColData::Fixed(type_code_bytes), // col 1 = type_code
            ColData::Fixed(flags_bytes),     // col 2 = flags
            ColData::Strings(names),         // col 3 = name
        ],
    }
}

/// Reconstruct a Schema from a META_SCHEMA-shaped ZSetBatch.
/// Mirrors Python's `batch_to_schema`.
pub fn batch_to_schema(batch: &ZSetBatch) -> Result<Schema, ProtocolError> {
    let count = batch.len();
    let mut columns: Vec<ColumnDef> = Vec::with_capacity(count);
    let mut pk_index: Option<usize> = None;

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
        let col_idx = batch.pk_lo[i];
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

        let tc = TypeCode::try_from_u64(type_code_raw)?;
        let is_nullable = (flags & META_FLAG_NULLABLE) != 0;
        let is_pk       = (flags & META_FLAG_IS_PK)    != 0;

        if is_pk {
            if pk_index.is_some() {
                return Err(ProtocolError::DecodeError("multiple pk flags in schema batch".into()));
            }
            pk_index = Some(i);
        }

        columns.push(ColumnDef { name, type_code: tc, is_nullable });
    }

    let pk_index = pk_index.ok_or_else(|| {
        ProtocolError::DecodeError("no PK column found in schema batch".into())
    })?;

    Ok(Schema { columns, pk_index })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColData, Schema, ColumnDef, TypeCode, ZSetBatch, meta_schema};
    use crate::wal_block::{encode_wal_block, decode_wal_block};

    // ── TypeCode::try_from_u64 error paths ──────────────────────────────────

    #[test]
    fn test_unknown_type_code_zero() {
        use crate::error::ProtocolError;
        assert!(matches!(TypeCode::try_from_u64(0), Err(ProtocolError::UnknownTypeCode(0))));
    }

    #[test]
    fn test_unknown_type_code_13() {
        use crate::error::ProtocolError;
        assert!(matches!(TypeCode::try_from_u64(13), Err(ProtocolError::UnknownTypeCode(13))));
    }

    #[test]
    fn test_unknown_type_code_max() {
        use crate::error::ProtocolError;
        assert!(matches!(TypeCode::try_from_u64(u64::MAX), Err(ProtocolError::UnknownTypeCode(_))));
    }

    // ── batch_to_schema error paths ──────────────────────────────────────────

    /// Build a valid META_SCHEMA batch for `ncols` columns (all U64, col 0 is PK).
    fn make_meta_batch(ncols: usize) -> ZSetBatch {
        use crate::header::META_FLAG_IS_PK;
        let mut type_code_bytes = vec![];
        let mut flags_bytes     = vec![];
        let mut names           = vec![];
        for i in 0..ncols {
            type_code_bytes.extend_from_slice(&8u64.to_le_bytes()); // U64 = 8
            let flags: u64 = if i == 0 { META_FLAG_IS_PK } else { 0 };
            flags_bytes.extend_from_slice(&flags.to_le_bytes());
            names.push(Some(format!("col{}", i)));
        }
        ZSetBatch {
            pk_lo:   (0..ncols as u64).collect(),
            pk_hi:   vec![0; ncols],
            weights: vec![1; ncols],
            nulls:   vec![0; ncols],
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(type_code_bytes),
                ColData::Fixed(flags_bytes),
                ColData::Strings(names),
            ],
        }
    }

    #[test]
    fn test_batch_to_schema_no_pk_flag() {
        use crate::error::ProtocolError;
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
        use crate::error::ProtocolError;
        let mut batch = make_meta_batch(2);
        batch.pk_lo.swap(0, 1); // [1, 0] instead of [0, 1]
        let res = batch_to_schema(&batch);
        assert!(matches!(res, Err(ProtocolError::DecodeError(_))));
    }

    #[test]
    fn test_batch_to_schema_col_idx_gap() {
        use crate::error::ProtocolError;
        let mut batch = make_meta_batch(2);
        batch.pk_lo[1] = 5; // gap: [0, 5]
        let res = batch_to_schema(&batch);
        assert!(matches!(res, Err(ProtocolError::DecodeError(_))));
    }

    #[test]
    fn test_batch_to_schema_col_idx_duplicate() {
        use crate::error::ProtocolError;
        let mut batch = make_meta_batch(2);
        batch.pk_lo[1] = 0; // duplicate: [0, 0]
        let res = batch_to_schema(&batch);
        assert!(matches!(res, Err(ProtocolError::DecodeError(_))));
    }

    // ── schema/meta roundtrip ────────────────────────────────────────────────

    fn schemas_equal(a: &Schema, b: &Schema) -> bool {
        if a.pk_index != b.pk_index || a.columns.len() != b.columns.len() {
            return false;
        }
        for (ca, cb) in a.columns.iter().zip(b.columns.iter()) {
            if ca.name != cb.name || ca.type_code != cb.type_code || ca.is_nullable != cb.is_nullable {
                return false;
            }
        }
        true
    }

    #[test]
    fn test_schema_meta_roundtrip() {
        let original = Schema {
            columns: vec![
                ColumnDef { name: "id".into(),    type_code: TypeCode::U64,    is_nullable: false },
                ColumnDef { name: "name".into(),  type_code: TypeCode::String, is_nullable: true  },
                ColumnDef { name: "score".into(), type_code: TypeCode::F64,    is_nullable: false },
                ColumnDef { name: "tag".into(),   type_code: TypeCode::I32,    is_nullable: true  },
                ColumnDef { name: "uuid".into(),  type_code: TypeCode::U128,   is_nullable: false },
            ],
            pk_index: 0,
        };

        let ms = meta_schema();
        let meta_batch = schema_to_batch(&original);
        let encoded = encode_wal_block(ms, 0, &meta_batch);
        let (decoded_batch, _, _) = decode_wal_block(&encoded, ms).unwrap();
        let reconstructed = batch_to_schema(&decoded_batch).unwrap();

        assert!(schemas_equal(&original, &reconstructed),
            "schema mismatch after meta roundtrip");
    }
}
