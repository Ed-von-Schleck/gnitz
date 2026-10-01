//! Read I/O on relation families. [`RelationRegistry::open_bound`] opens every
//! source a bound can name as a [`SourceCursor`], choosing between the store and
//! an index over it;
//! [`RelationRegistry::gather_bytes`] is the batched FK parent probe.

use crate::relation::{Relation, RelationKind, RelationRegistry};
use gnitz_expr::ColumnTable;
use gnitz_wire::{KeyRange, PkKeys, ReadBound};
use gnitz_zset::repr::{empty_cursor, Batch, BoundedIndexCursor, SourceCursor};

const INDEX_SCAN_RATIO: usize = 16;

impl RelationRegistry {
    /// The FK parent probe: every live row of `keys` at weight 1, projected to the payload
    /// column `ref_col`.
    pub fn gather_bytes(&self, id: u64, keys: PkKeys, ref_col: u8) -> Result<Batch, String> {
        let entry = self.relation_or_err(id)?;
        entry.gather(keys, None).project_live(&[ref_col as u32])
    }

    /// Open `bound`'s source over `id`, without walking it, and the part of `bound`
    /// the source does not apply.
    pub fn open_bound(&self, id: u64, bound: ReadBound) -> Result<(SourceCursor, ReadBound), String> {
        let entry = self.relation_or_err(id)?;
        // A stream holds no rows, but a backfill over one still feeds an empty
        // epoch: that is what mints a global aggregate's ground row.
        if entry.kind() == RelationKind::Stream {
            let empty = empty_cursor(entry.schema());
            return Ok((SourceCursor::Full(Box::new(empty)), ReadBound::None));
        }
        let cursor = match bound {
            ReadBound::None => SourceCursor::Full(Box::new(entry.cursor())),
            ReadBound::PkSet(keys) => {
                let schema = entry.schema();
                if keys.stride() != schema.pk_stride() {
                    return Err(format!(
                        "open_bound: PkSet key stride {} != pk_stride {} (table {id})",
                        keys.stride(),
                        schema.pk_stride()
                    ));
                }
                // A key this worker holds no row for copies nothing.
                SourceCursor::PkSet(Box::new(entry.gather(keys, None)))
            }
            ReadBound::Range(r) => return open_range(entry, r),
        };
        Ok((cursor, ReadBound::None))
    }
}

/// The walk `r` names over `entry`, and the part of it the cursor leaves unapplied.
fn open_range(entry: &Relation, r: KeyRange) -> Result<(SourceCursor, ReadBound), String> {
    let cols = entry.bound_cols(r.cols(), "open_bound")?;
    let schema = entry.schema();
    if r.walks_pk(schema.pk_cols()) {
        let cursor = entry.store().held().range_cursor(schema.pk_range_keys(&r));
        return Ok((SourceCursor::Full(Box::new(cursor)), ReadBound::None));
    }
    if let Some(ic) = entry.index_on(cols.as_slice()) {
        let idx = ic.cursor_over(&r);
        if idx.estimated_length() <= entry.store().held().estimated_rows() / INDEX_SCAN_RATIO {
            let walk = BoundedIndexCursor::new(idx, entry.cursor(), ic.key_spec());
            return Ok((SourceCursor::Bounded(Box::new(walk)), ReadBound::None));
        }
    }
    Ok((SourceCursor::Full(Box::new(entry.cursor())), ReadBound::Range(r)))
}

#[cfg(test)]
#[path = "tests/store_io.rs"]
mod tests;
