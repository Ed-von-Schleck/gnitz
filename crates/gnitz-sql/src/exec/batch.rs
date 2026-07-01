use crate::ast_util::is_wildcard_projection;
use crate::bind::find_unique_column;
use crate::codec::nullmap::{null_word_get, null_word_set};
use crate::error::GnitzSqlError;
use gnitz_core::{ColData, PkColumn, Schema, ZSetBatch};
use sqlparser::ast::{Expr, SelectItem};

pub(crate) fn copy_batch_row(src: &ZSetBatch, i: usize, dst: &mut ZSetBatch, schema: &Schema) {
    dst.pks.push_from(&src.pks, i);
    dst.weights.push(src.weights[i]);
    dst.nulls.push(src.nulls[i]);
    for (_pi, ci, col_def) in schema.payload_columns() {
        let stride = col_def.type_code.wire_stride();
        src.columns[ci].push_row_from(i, stride, &mut dst.columns[ci]);
    }
}

pub(crate) fn apply_projection(
    projection: &[SelectItem],
    schema: &Schema,
    batch: Option<ZSetBatch>,
) -> Result<(Schema, ZSetBatch), GnitzSqlError> {
    let is_wildcard = is_wildcard_projection(projection);

    if is_wildcard {
        let b = batch.unwrap_or_else(|| ZSetBatch::new(schema));
        return Ok((schema.clone(), b));
    }

    // Extract named columns; dedup preserving first-occurrence order
    let mut col_indices: Vec<usize> = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = find_unique_column(&schema.columns, &ident.value)?
                    .ok_or_else(|| GnitzSqlError::Bind(format!("column '{}' not found in projection", ident.value)))?;
                if !col_indices.contains(&idx) {
                    col_indices.push(idx);
                }
            }
            SelectItem::Wildcard(_) => {
                for i in 0..schema.columns.len() {
                    if !col_indices.contains(&i) {
                        col_indices.push(i);
                    }
                }
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "only simple column references supported in SELECT projection".to_string(),
                ))
            }
        }
    }

    // Identity fast-path: every source column projected in source order.
    // Checked before allocating new_cols/new_schema — on the common case
    // (named projection that names every column) those allocations would
    // be dead. The identity case projects every column, so PK columns are
    // included and the no-PK-projected guard below is satisfied implicitly.
    let is_identity =
        col_indices.len() == schema.columns.len() && col_indices.iter().enumerate().all(|(i, &ci)| ci == i);
    if is_identity {
        let b = batch.unwrap_or_else(|| ZSetBatch::new(schema));
        return Ok((schema.clone(), b));
    }

    // 1. Build new PK column set; every projected source-PK becomes a new PK.
    let new_pk_cols: Vec<usize> = col_indices
        .iter()
        .enumerate()
        .filter(|(_, &old_ci)| schema.is_pk_col(old_ci))
        .map(|(new_ci, _)| new_ci)
        .collect();

    if new_pk_cols.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "projection must include at least one PRIMARY KEY column".to_string(),
        ));
    }

    let new_cols: Vec<gnitz_core::ColumnDef> = col_indices.iter().map(|&i| schema.columns[i].clone()).collect();
    let new_schema = Schema {
        columns: new_cols,
        pk_cols: new_pk_cols,
    };

    let src_batch = batch.unwrap_or_else(|| ZSetBatch::new(schema));
    let row_count = src_batch.len();
    let ZSetBatch {
        pks: src_pks,
        weights,
        nulls: src_nulls,
        columns: mut src_columns,
    } = src_batch;
    let mut new_batch = ZSetBatch::new(&new_schema);
    new_batch.weights = weights;

    // 3. PK region: move when layout is byte-identical; else rebuild from
    //    the packed source bytes. `pk_preserved` covers compound→compound
    //    when columns and order match; the single-PK source can only land
    //    here (see invariants above).
    let pk_preserved = new_schema.pk_cols.len() == schema.pk_cols.len()
        && new_schema
            .pk_cols
            .iter()
            .enumerate()
            .all(|(i, &new_pk_ci)| col_indices[new_pk_ci] == schema.pk_cols[i]);

    if pk_preserved {
        new_batch.pks = src_pks;
    } else {
        // Single-PK source can't reach here: if the lone PK is projected
        // then pk_preserved is true; if not, new_pk_cols is empty and we
        // returned the no-PK-projected error above. So the source PK
        // column is always compound (Bytes).
        let src_bytes = match &src_pks {
            PkColumn::Bytes { buf, .. } => buf.as_slice(),
            _ => unreachable!("non-preserved PK rebuild requires compound source"),
        };
        let src_stride = schema.pk_stride();

        // Per-PK (col_off, stride) is invariant across rows — hoist.
        let pk_mappings: Vec<(usize, usize)> = new_schema
            .pk_cols
            .iter()
            .map(|&new_pk_ci| {
                let old_ci = col_indices[new_pk_ci];
                (
                    schema.pk_byte_offset(old_ci),
                    schema.columns[old_ci].type_code.wire_stride(),
                )
            })
            .collect();

        match &mut new_batch.pks {
            PkColumn::Bytes { buf, .. } => {
                buf.reserve(row_count * new_schema.pk_stride());
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    for &(col_off, stride) in &pk_mappings {
                        buf.extend_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    }
                }
            }
            PkColumn::U64s(v) => {
                // Destination is single-PK → exactly one mapping.
                let (col_off, stride) = pk_mappings[0];
                v.reserve(row_count);
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    let mut b = [0u8; 8];
                    b[..stride].copy_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    v.push(u64::from_le_bytes(b));
                }
            }
            PkColumn::U128s(v) => {
                let (col_off, stride) = pk_mappings[0];
                v.reserve(row_count);
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    let mut b = [0u8; 16];
                    b[..stride].copy_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    v.push(u128::from_le_bytes(b));
                }
            }
        }
    }

    // 4. Null bitmap: move when payload layout matches the source's;
    //    else rebuild bit-by-bit using a hoisted (new_pi, old_pi) mapping.
    let payload_preserved = new_schema.num_payload_cols() == schema.num_payload_cols()
        && new_schema
            .payload_columns()
            .zip(schema.payload_columns())
            .all(|((_, new_ci, _), (_, old_ci, _))| col_indices[new_ci] == old_ci);

    if payload_preserved {
        new_batch.nulls = src_nulls;
    } else {
        // Projected PKs become new PKs (not payload), so old_ci is a
        // source payload column by construction — payload_idx is safe.
        let pi_mappings: Vec<(usize, usize)> = new_schema
            .payload_columns()
            .map(|(new_pi, new_ci, _)| (new_pi, schema.payload_idx(col_indices[new_ci])))
            .collect();

        new_batch.nulls.reserve(row_count);
        for &old_word in &src_nulls {
            let mut new_word = 0u64;
            for &(new_pi, old_pi) in &pi_mappings {
                if null_word_get(old_word, old_pi) {
                    null_word_set(&mut new_word, new_pi, true);
                }
            }
            new_batch.nulls.push(new_word);
        }
    }

    // 5. Payload columns: strictly payload→payload. `src_columns` is owned,
    //    so swap whole vectors instead of cloning per element — avoids
    //    per-row Option<String>/Option<Vec<u8>> allocations on string/blob
    //    result sets.
    for (_, new_ci, _) in new_schema.payload_columns() {
        let old_ci = col_indices[new_ci];
        let src_col = std::mem::replace(&mut src_columns[old_ci], ColData::Fixed(Vec::new()));
        match (src_col, &mut new_batch.columns[new_ci]) {
            (ColData::Fixed(s), ColData::Fixed(d)) => *d = s,
            (ColData::Strings(s), ColData::Strings(d)) => *d = s,
            (ColData::Bytes(s), ColData::Bytes(d)) => *d = s,
            (ColData::U128s(s), ColData::U128s(d)) => *d = s,
            _ => unreachable!("mismatched ColData variants for column {new_ci}"),
        }
    }

    Ok((new_schema, new_batch))
}

pub(crate) fn apply_limit(mut batch: ZSetBatch, schema: &Schema, limit: usize) -> ZSetBatch {
    let n = batch.pks.len();
    if n <= limit {
        return batch;
    }

    batch.pks.truncate(limit);
    batch.weights.truncate(limit);
    batch.nulls.truncate(limit);

    for (_pi, ci, col_def) in schema.payload_columns() {
        match &mut batch.columns[ci] {
            ColData::Fixed(buf) => {
                let stride = col_def.type_code.wire_stride();
                buf.truncate(limit * stride);
            }
            ColData::Strings(v) => {
                v.truncate(limit);
            }
            ColData::Bytes(v) => {
                v.truncate(limit);
            }
            ColData::U128s(v) => {
                v.truncate(limit);
            }
        }
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::compound_schema_u64_u64;

    #[test]
    fn compound_pk_copy_batch_row_preserves_compound_pk() {
        let schema = compound_schema_u64_u64();
        let mut src = ZSetBatch::new(&schema);
        let mut pk_bytes = [0u8; 16];
        pk_bytes[..8].copy_from_slice(&11u64.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&22u64.to_le_bytes());
        src.pks.push_bytes(&pk_bytes);
        src.weights.push(1);
        src.nulls.push(0);
        if let ColData::Fixed(buf) = &mut src.columns[2] {
            buf.extend_from_slice(&5i64.to_le_bytes());
        }

        let mut dst = ZSetBatch::new(&schema);
        copy_batch_row(&src, 0, &mut dst, &schema);
        assert_eq!(dst.pks.len(), 1);
        match &dst.pks {
            PkColumn::Bytes { stride: 16, buf } => {
                assert_eq!(buf, &pk_bytes.to_vec());
            }
            _ => panic!("expected Bytes variant after copy"),
        }
    }
}
