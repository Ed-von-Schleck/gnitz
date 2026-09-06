use crate::ast_util::{aliased_def, expand_wildcard_item, is_bare_wildcard_projection, scalar_projection_item};
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::validate::reject_duplicate_projection_names;
use gnitz_core::{Schema, TypeCode, ZSetBatch};
use sqlparser::ast::SelectItem;

/// One schema's payload copy plan: each payload column's index and type,
/// resolved once. A row-at-a-time gather builds this before its loop —
/// `Schema::payload_columns` re-scans `pk_cols` per column and re-matches each
/// type code, which over a million-row ordered result dominates the copy itself.
pub(crate) struct RowGather {
    payload: Vec<(usize, TypeCode)>,
}

impl RowGather {
    pub(crate) fn new(schema: &Schema) -> Self {
        RowGather {
            payload: schema
                .payload_columns()
                .map(|(_pi, ci, def)| (ci, def.type_code))
                .collect(),
        }
    }

    /// Append `src`'s row `i` to `dst`.
    pub(crate) fn copy(&self, src: &ZSetBatch, i: usize, dst: &mut ZSetBatch) {
        dst.pks.push_from(&src.pks, i);
        dst.weights.push(src.weights[i]);
        dst.nulls.push(src.nulls[i]);
        for &(ci, tc) in &self.payload {
            dst.push_cell_from(ci, src, ci, tc, i);
        }
    }
}

/// A resolved projection: the output schema and, per output column, its source
/// column index. `None` is the passthrough — a wildcard, or a named projection
/// that reproduces the source schema exactly — where the source batch IS the
/// result.
pub(crate) type Projection = Option<(Schema, Vec<usize>)>;

/// Resolve `projection` against `schema` — the whole fallible half of projecting,
/// so `INSERT … RETURNING` resolves before it writes and [`project`]s after,
/// never committing a row for a projection that then fails.
pub(crate) fn resolve_projection(
    projection: &[SelectItem],
    schema: &Schema,
    rel_alias: &str,
) -> Result<Projection, GnitzSqlError> {
    // Only a *bare* `*` on a schema with no hidden payload column is the no-op
    // passthrough; a `* EXCEPT/EXCLUDE/RENAME` (or a rejected `* REPLACE/ILIKE`),
    // or a DROP COLUMN'd base table, falls through to the expansion arm so the
    // dropped slot is filtered out (§6).
    if is_bare_wildcard_projection(projection) && !schema.has_hidden_payload() {
        return Ok(None);
    }

    // One output column per projection item (`SELECT a, a` yields two columns —
    // the convention every other surface follows): the source column index and
    // its output `ColumnDef` (alias applied).
    let mut col_indices: Vec<usize> = Vec::new();
    let mut out_defs: Vec<gnitz_core::ColumnDef> = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(o) => {
                for (i, def) in expand_wildcard_item(o, &schema.columns, "SELECT")? {
                    col_indices.push(i);
                    out_defs.push(def);
                }
            }
            _ => {
                let (e, alias) = scalar_projection_item(item, "SELECT projection")?;
                let BoundExpr::ColRef(idx) = bind_single_table(e, schema, rel_alias)? else {
                    return Err(GnitzSqlError::Unsupported(
                        "only simple column references supported in SELECT projection".to_string(),
                    ));
                };
                col_indices.push(idx);
                out_defs.push(aliased_def(&schema.columns[idx], alias));
            }
        }
    }

    reject_duplicate_projection_names(projection, out_defs.iter(), "RETURNING")?;

    // Identity fast-path: every source column projected once, in source order,
    // under its own name. Checked before allocating new_schema — on the common
    // case (named projection that names every column) those allocations would
    // be dead. The identity case projects every column, so PK columns are
    // included and the no-PK-projected guard below is satisfied implicitly.
    let is_identity = col_indices.len() == schema.columns.len()
        && col_indices.iter().enumerate().all(|(i, &ci)| ci == i)
        && out_defs.iter().zip(&schema.columns).all(|(d, c)| d.name == c.name);
    if is_identity {
        return Ok(None);
    }

    // Build the new PK column set; every projected source-PK becomes a new PK.
    let new_pk_cols: Vec<u32> = col_indices
        .iter()
        .enumerate()
        .filter(|(_, &old_ci)| schema.is_pk_col(old_ci))
        .map(|(new_ci, _)| new_ci as u32)
        .collect();

    if new_pk_cols.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "projection must include at least one PRIMARY KEY column".to_string(),
        ));
    }

    Ok(Some((Schema { columns: out_defs, pk_cols: new_pk_cols }, col_indices)))
}

/// Apply a resolved projection to `batch` (absent = an empty batch). Infallible:
/// every rejection happened in [`resolve_projection`]. The passthrough hands the
/// source batch straight back — no copy.
pub(crate) fn project(resolved: Projection, schema: &Schema, batch: Option<ZSetBatch>) -> (Schema, ZSetBatch) {
    let Some((new_schema, col_indices)) = resolved else {
        return (schema.clone(), batch.unwrap_or_else(|| ZSetBatch::new(schema)));
    };

    let src_batch = batch.unwrap_or_else(|| ZSetBatch::new(schema));
    let row_count = src_batch.len();
    let ZSetBatch {
        pks: src_pks,
        weights,
        nulls: src_nulls,
        columns: mut src_columns,
        blob,
    } = src_batch;
    let mut new_batch = ZSetBatch::new(&new_schema);
    new_batch.weights = weights;
    // The arena moves whole: a projection keeps every surviving cell's offset
    // valid, and a dropped column's spill is dead weight in a per-statement
    // buffer, not a leak.
    new_batch.blob = blob;

    // PK region: move when the layout is byte-identical; else rebuild from the
    // packed source bytes. `pk_preserved` covers compound→compound when columns
    // and order match; a single-PK source reaches it the same way.
    let pk_preserved = new_schema.pk_cols.len() == schema.pk_cols.len()
        && new_schema
            .pk_cols
            .iter()
            .enumerate()
            .all(|(i, &new_pk_ci)| col_indices[new_pk_ci as usize] as u32 == schema.pk_cols[i]);

    if pk_preserved {
        new_batch.pks = src_pks;
    } else {
        // A subset / reordered / duplicated PK projection re-packs the region
        // row by row. `get_bytes` borrows the row's key in place — a duplicated
        // lone PK (`SELECT pk, pk`) has a scalar source but a two-slot Bytes
        // destination, and both read the same way. This path is cold: the common
        // full-projection case took `pk_preserved` above.

        // Per-PK (col_off, stride) is invariant across rows — hoist.
        let pk_mappings: Vec<(usize, usize)> = new_schema
            .pk_cols
            .iter()
            .map(|&new_pk_ci| {
                let old_ci = col_indices[new_pk_ci as usize];
                (
                    schema.pk_byte_offset(old_ci),
                    schema.columns[old_ci].type_code.wire_stride(),
                )
            })
            .collect();

        // Every arity is the same walk: for each destination PK column, copy its
        // OPK bytes out of the source tuple at that column's offset. A column's
        // OPK image is independent of the columns around it, so no re-encode.
        new_batch.pks.reserve(row_count);
        let mut key = [0u8; gnitz_core::MAX_PK_BYTES];
        for i in 0..row_count {
            let row = src_pks.get_bytes(i);
            let mut at = 0;
            for &(col_off, stride) in &pk_mappings {
                key[at..at + stride].copy_from_slice(&row[col_off..col_off + stride]);
                at += stride;
            }
            new_batch.pks.push_region_bytes(&key[..at]);
        }
    }

    // Null bitmap: move when the payload layout matches the source's; else
    // rebuild bit-by-bit using a hoisted (new_pi, old_pi) mapping.
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
                if gnitz_wire::null_word_get(old_word, old_pi) {
                    gnitz_wire::null_word_set(&mut new_word, new_pi, true);
                }
            }
            new_batch.nulls.push(new_word);
        }
    }

    // Payload columns: strictly payload→payload. `src_columns` is owned, so whole
    // regions move rather than being copied cell by cell. A source column
    // projected more than once is cloned for every occurrence but its last, which
    // still takes the move.
    let payload_maps: Vec<(usize, usize)> = new_schema
        .payload_columns()
        .map(|(_, new_ci, _)| (new_ci, col_indices[new_ci]))
        .collect();
    for (pos, &(new_ci, old_ci)) in payload_maps.iter().enumerate() {
        let used_later = payload_maps[pos + 1..].iter().any(|&(_, o)| o == old_ci);
        new_batch.columns[new_ci] = if used_later {
            src_columns[old_ci].clone()
        } else {
            std::mem::take(&mut src_columns[old_ci])
        };
    }

    (new_schema, new_batch)
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;
