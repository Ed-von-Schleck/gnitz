//! The AST-free set-operation / DISTINCT primitives the HIR lowering drives:
//! the cross-width type promotion (`set_op_common_type`), the content-hash
//! reindex + shard (`hash_shard_side`), the weight-clamp leaves
//! (`set_op_leaves`), and the bare-column projection resolver the ad-hoc
//! `SELECT DISTINCT` path shares (`resolve_set_projection`).

use crate::ast_util::{is_bare_wildcard_projection, wildcard_name_is_visible, WildcardRewrite};
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::plan::validate::reject_float_keys;
use gnitz_core::{CircuitBuilder, ColumnDef, FixedInt, Schema, TypeCode};
use sqlparser::ast::SelectItem;

/// Per-column common type for a set-op pair, or `None` to keep the exact-match
/// type-mismatch error. Same types pass through; a cross-width integer pair
/// promotes to the join-key ladder's common type (same-sign → the wider type;
/// cross-sign with the unsigned operand ≤ U32 → the narrowest strictly-wider
/// signed type, e.g. `U32` vs `I32` → `I64`) but only when the result is a
/// concrete ≤8-byte integer. `None`, the `I128` collapse (`U64` vs `I64`), and
/// every 16-byte / non-integer / string pair are rejected: the widening path
/// loads a value into one i64 register and cannot represent a 16-byte extremum.
pub(crate) fn set_op_common_type(l: TypeCode, r: TypeCode) -> Option<TypeCode> {
    if l == r {
        return Some(l);
    }
    let t = l.join_key_common_type(r)?;
    FixedInt::from_type_code(t).is_some().then_some(t)
}

/// Hash the projected columns to a synthetic content PK — widening
/// each column whose `target_tcs` entry is non-zero into the promoted layout so
/// both set-op sides share one physical representation — then shard by that PK.
pub(crate) fn hash_shard_side(
    cb: &mut CircuitBuilder,
    filtered: gnitz_core::NodeId,
    proj_indices: &[usize],
    target_tcs: &[u8],
    branch_id: u8,
) -> gnitz_core::NodeId {
    // Reindex by a hash of the projected columns, so set membership
    // (EXCEPT/INTERSECT/UNION-distinct) is decided by the projected row content,
    // not by the source table's PK: two rows from different tables sharing a PK
    // but differing in payload must not match.
    let reindexed = cb.map_hash_row(filtered, proj_indices, target_tcs, branch_id);
    // Repartition by the synthetic hash PK (column 0) so that under
    // multiple workers each row lands on the worker that owns its new PK's
    // shard, co-locating matching rows for the downstream set arithmetic and
    // placing each output row on its owning worker for the sink/scan. The hash
    // is computed in-circuit, so the master cannot pre-shard the source by it;
    // this in-circuit exchange is mandatory. Single-worker mode elides the IPC.
    cb.shard(reindexed, &[0])
}

/// Resolve a set-operation side's projection to source column indices plus
/// output column definitions. Supports `SELECT *`, bare column references, and
/// aliased column references; rejects computed expressions (which have no
/// meaningful set identity here) with a clean error rather than silently
/// dropping them.
pub(crate) fn resolve_set_projection(
    projection: &[SelectItem],
    source_schema: &Schema,
    context: &str,
) -> Result<(Vec<usize>, Vec<ColumnDef>), GnitzSqlError> {
    // Wildcard expands to *visible* columns only: an upstream synthetic key
    // (`_join_pk`, `_set_pk`, …) must not participate in UNION/INTERSECT/
    // EXCEPT/DISTINCT row identity — hashing it into the dedup key would keep
    // otherwise-identical rows distinct. Only a *bare* `*` takes this fast path;
    // a `* EXCEPT/EXCLUDE/RENAME` (or a rejected `* REPLACE/ILIKE`) falls into
    // the single Wildcard arm below.
    if is_bare_wildcard_projection(projection) {
        let (indices, cols): (Vec<usize>, Vec<ColumnDef>) =
            source_schema.visible_columns().map(|(i, c)| (i, c.clone())).unzip();
        reject_float_keys(source_schema, &indices)?;
        return Ok((indices, cols));
    }
    let mut indices: Vec<usize> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                // Visible columns only (as the bare-`*` fast path); `EXCEPT`/
                // `EXCLUDE`/`RENAME` rewrite by name, `REPLACE`/`ILIKE` reject.
                let rw =
                    WildcardRewrite::for_item(item, |n| wildcard_name_is_visible(&source_schema.columns, n), context)?;
                for (i, col) in source_schema.visible_columns() {
                    let Some(out) = rw.rewrite_column(col) else { continue };
                    indices.push(i);
                    out_cols.push(out);
                }
            }
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                // Resolve the column once; an alias only renames the output column.
                let ci = match bind_single_table(expr, source_schema)? {
                    BoundExpr::ColRef(ci) => ci,
                    _ => {
                        return Err(GnitzSqlError::Unsupported(format!(
                            "{context}: computed expressions are not supported"
                        )))
                    }
                };
                indices.push(ci);
                let mut col = source_schema.columns[ci].clone();
                if let SelectItem::ExprWithAlias { alias, .. } = item {
                    col.name = alias.value.clone();
                }
                out_cols.push(col);
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "{context}: unsupported SELECT item"
                )))
            }
        }
    }
    // Single chokepoint: every projected column lands in `indices`, so one pass
    // here rejects a float row-identity key regardless of which SELECT-item arm
    // produced it (a new arm is covered automatically).
    reject_float_keys(source_schema, &indices)?;
    Ok((indices, out_cols))
}

/// The two leaf nodes feeding an INTERSECT/EXCEPT weight-clamp arm. The distinct
/// form clamps each side to {0,1} via `distinct` so the arithmetic is set-valued;
/// `all` keeps the raw per-row bag counts.
pub(crate) fn set_op_leaves(
    cb: &mut CircuitBuilder,
    all: bool,
    left: gnitz_core::NodeId,
    right: gnitz_core::NodeId,
) -> (gnitz_core::NodeId, gnitz_core::NodeId) {
    if all {
        (left, right)
    } else {
        (cb.distinct(left), cb.distinct(right))
    }
}
