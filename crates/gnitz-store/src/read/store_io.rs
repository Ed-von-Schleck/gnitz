//! Read I/O on relation families. [`RelationRegistry::open_bound`] opens every
//! source a bound can name as a [`SourceCursor`], choosing between the store and
//! an index over it;
//! [`RelationRegistry::probe`] answers a HasPk probe.

use crate::relation::{Relation, RelationKind, RelationRegistry};
use gnitz_expr::ColumnTable;
use gnitz_wire::{Holders, KeyRange, PkKeys, Probe, ReadBound};
use gnitz_zset::repr::{empty_cursor, Batch, BoundedIndexCursor, SourceCursor};
use gnitz_zset::schema::SchemaDescriptor;

const INDEX_SCAN_RATIO: usize = 16;

impl RelationRegistry {
    /// Answer `probe` at `keys` over the rows this process holds of `id`.
    pub fn probe(&self, id: u64, probe: Probe, keys: &Batch) -> Result<Batch, String> {
        let stride = keys.schema().pk_stride();
        let keyed_as = |store: SchemaDescriptor| match store.pk_stride() == stride {
            true => Ok(()),
            false => Err(format!(
                "probe: key stride {stride} != the probed store's {} (table {id})",
                store.pk_stride()
            )),
        };
        let (cols, holders) = match probe {
            Probe::Pk => {
                let mut held = Batch::empty_with_schema(keys.schema());
                // An id this process has not registered holds no row.
                if let Some(relation) = self.relation(id) {
                    keyed_as(relation.schema())?;
                    let table = relation.table();
                    for key in (0..keys.len()).map(|i| keys.get_pk_bytes(i)) {
                        if table.has_pk_bytes(key) {
                            held.push_key_row(key, 1);
                        }
                    }
                }
                return Ok(held);
            }
            Probe::PkColumn(col) => {
                let relation = self.relation_or_err(id)?;
                keyed_as(relation.schema())?;
                let keys = PkKeys::from_sorted(stride, keys.pk_data().to_vec());
                return relation.gather(keys, None).project_live(&[col]);
            }
            Probe::Index(cols, holders) => (cols, holders),
        };
        let index = self
            .relation(id)
            .and_then(|r| r.index_on(cols.as_slice()))
            .ok_or_else(|| format!("No index on columns {:?} for table {}", cols.as_slice(), id))?;
        keyed_as(index.schema())?;
        let span = index.key_spec().key_size();
        let mut cursor = index.cursor();
        let mut entries = Batch::empty_with_schema(keys.schema());
        for key in (0..keys.len()).map(|i| keys.get_pk_bytes(i)) {
            match holders {
                Holders::UpTo(cap) => {
                    cursor.for_each_positive_with_prefix_capped(&key[..span], cap.get() as usize, |c| {
                        entries.push_key_row(c.current_pk_bytes(), 1)
                    })
                }
                _ if !cursor.seek_first_positive_with_prefix(&key[..span]) => {}
                Holders::First => entries.push_key_row(cursor.current_pk_bytes(), 1),
                Holders::Echo => entries.push_key_row(key, 1),
            }
        }
        Ok(entries)
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
    let schema = entry.schema();
    if r.walks_pk(schema.pk_cols()) {
        let cursor = entry.table().range_cursor(schema.pk_range_keys(&r));
        return Ok((SourceCursor::Full(Box::new(cursor)), ReadBound::None));
    }
    if let Some(ic) = entry.index_on(r.cols().as_slice()) {
        let idx = ic.cursor_over(&r);
        if idx.estimated_length() <= entry.table().estimated_rows() / INDEX_SCAN_RATIO {
            let walk = BoundedIndexCursor::new(idx, entry.cursor(), ic.key_spec());
            return Ok((SourceCursor::Bounded(Box::new(walk)), ReadBound::None));
        }
    }
    Ok((SourceCursor::Full(Box::new(entry.cursor())), ReadBound::Range(r)))
}

#[cfg(test)]
#[path = "tests/store_io.rs"]
mod tests;
