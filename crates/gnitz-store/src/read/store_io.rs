//! Read I/O on relation families. [`Relation::open_bound`] opens every
//! source a bound can name as a [`SourceCursor`], choosing between the store and
//! an index over it;
//! [`RelationRegistry::probe`] answers a HasPk probe, in the layout
//! [`Relation::probe_reply_schema`] names.

use super::check_stride;
use crate::relation::{Cut, Relation, RelationKind, RelationRegistry};
use crate::storage::Table;
use gnitz_wire::{KeyRange, PkColList, PkKeys, ReadBound};
use gnitz_zset::repr::{
    empty_cursor, project_unique, Batch, BoundedIndexCursor, KeyGather, SourceCursor, StoredRow, UniqueRows,
};
use gnitz_zset::schema::{project_schema, SchemaDescriptor};
use std::num::NonZeroU64;

/// One `HasPk` probe: the store it reads, and what it answers each matched key
/// with.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Probe {
    /// The relation's own PK store: each key a live row carries, echoed.
    Pk,
    /// A base table's own PK store: each key a live row carries, with that
    /// row's column.
    PkColumn(u32),
    /// The secondary index on this exact column list: the stored entries
    /// under the keys' spans, `[span ‖ holder PK]`, in key order, up to `cap`
    /// in all.
    Index(PkColList, NonZeroU64),
}

/// A range walks an index only while the walk's estimated entries are at most
/// one in this many of the store's rows; past that it scans the store under
/// the range.
const INDEX_SCAN_RATIO: usize = 16;

impl RelationRegistry {
    /// Answer `probe` at `keys` over the rows this process holds of `id`.
    pub fn probe(&self, id: u64, probe: Probe, keys: PkKeys) -> Result<Batch, String> {
        let relation = self.relation_or_err(id)?;
        let keyed_as = |stride| check_stride("probe", id, &keys, stride);
        match probe {
            Probe::Pk => {
                keyed_as(relation.schema().pk_stride())?;
                let table = relation.table_or_err()?;
                let mut echo = Batch::empty_with_schema(&relation.probe_reply_schema(probe)?);
                keys.iter()
                    .filter(|k| table.has_pk_bytes(k))
                    .for_each(|key| echo.push_key_row(key, 1));
                Ok(echo)
            }
            Probe::PkColumn(col) => {
                keyed_as(relation.schema().pk_stride())?;
                relation.table_or_err()?;
                let rows = relation
                    .unique_rows()
                    .ok_or_else(|| format!("probe: column probe of relation {id}, which is not a base table"))?;
                project_unique(rows, &relation.schema(), &keys, &[col])
            }
            Probe::Index(cols, cap) => {
                let index = relation.index_or_err(cols.as_slice())?;
                // The keys are the probed spans themselves.
                keyed_as(index.key_spec().key_size())?;
                let mut entries = Batch::empty_with_schema(&index.key_spec().index_schema(&relation.schema()));
                let cap = usize::try_from(cap.get()).unwrap_or(usize::MAX);
                index.for_each_entry(relation, keys, cap, |entry| entries.push_key_row(entry, 1));
                Ok(entries)
            }
        }
    }
}

impl Relation {
    /// The layout a `probe` of this relation answers in.
    pub fn probe_reply_schema(&self, probe: Probe) -> Result<SchemaDescriptor, String> {
        let schema = self.schema();
        match probe {
            Probe::Pk => Ok(schema.pk_only()),
            Probe::PkColumn(col) => project_schema(&schema, &[col]),
            Probe::Index(cols, _) => Ok(self.index_or_err(cols.as_slice())?.key_spec().index_schema(&schema)),
        }
    }

    /// Open `bound`'s source over this relation, without walking it, and the part
    /// of `bound` the source does not apply. The source borrows nothing.
    pub fn open_bound(&self, bound: ReadBound, cut: Cut) -> Result<(SourceCursor<'static>, Option<KeyRange>), String> {
        self.open_source(bound, cut, None)
    }

    /// [`Self::open_bound`] at `Cut::Now`, where a base table answers each
    /// whole key it is asked for by its unique-PK probe instead of a cursor
    /// seek.
    pub fn open_read(&self, bound: ReadBound) -> Result<(SourceCursor<'_>, Option<KeyRange>), String> {
        self.open_source(bound, Cut::Now, self.unique_rows())
    }

    /// This process's store of the relation as one live row per key, which only
    /// a base table's is; `None` for any other kind.
    fn unique_rows(&self) -> Option<&dyn UniqueRows> {
        (self.kind() == RelationKind::BaseTable).then(|| self.table() as &dyn UniqueRows)
    }

    fn open_source<'a>(
        &self,
        bound: ReadBound,
        cut: Cut,
        unique: Option<&'a dyn UniqueRows>,
    ) -> Result<(SourceCursor<'a>, Option<KeyRange>), String> {
        // A stream holds no rows, but a backfill over one still feeds an empty
        // epoch: that keeps every worker's exchange rounds in step.
        if self.kind() == RelationKind::Stream {
            let empty = empty_cursor(self.schema());
            return Ok((SourceCursor::Full(empty), None));
        }
        let cursor = match bound {
            ReadBound::None => SourceCursor::Full(self.table().open_cursor(cut)),
            ReadBound::PkSet(keys) => {
                check_stride("open_bound", self.id(), &keys, self.schema().pk_stride())?;
                // A key this worker holds no row for copies nothing.
                match unique.filter(|_| keys.stride() == self.schema().pk_stride()) {
                    Some(store) => SourceCursor::PkSet(KeyGather::unique(store, self.schema(), keys)),
                    None => SourceCursor::PkSet(KeyGather::Merge(self.gather(keys, cut))),
                }
            }
            ReadBound::Range(r) => return open_range(self, r, cut, unique),
        };
        Ok((cursor, None))
    }
}

/// The walk `r` names over `entry`, and the part of it the cursor leaves unapplied.
/// `unique`, when given, is `entry`'s store, probed for an index entry's row.
fn open_range<'a>(
    entry: &Relation,
    r: KeyRange,
    cut: Cut,
    unique: Option<&'a dyn UniqueRows>,
) -> Result<(SourceCursor<'a>, Option<KeyRange>), String> {
    let schema = entry.schema();
    let table = entry.table();
    if r.walks_pk(schema.pk_cols()) {
        let cursor = table.range_cursor(&r, cut);
        return Ok((SourceCursor::Full(cursor), None));
    }
    // An index holds a pending row's entry from its ingest on, so it is walked
    // only where the store's two cuts read the same rows.
    let index = entry
        .index_on(r.cols().as_slice())
        .filter(|_| cut == Cut::Now || !table.has_pending());
    if let Some(ic) = index {
        let idx = ic.cursor_over(&r);
        // No entry in the range: no store cursor to open.
        if !idx.is_valid() {
            return Ok((SourceCursor::Full(empty_cursor(schema)), None));
        }
        if idx.estimated_length() <= table.estimated_rows() / INDEX_SCAN_RATIO {
            let walk = match unique {
                Some(store) => BoundedIndexCursor::new_unique(idx, store, schema),
                None => BoundedIndexCursor::new(idx, table.open_cursor(cut)),
            };
            return Ok((SourceCursor::Bounded(Box::new(walk)), None));
        }
    }
    Ok((SourceCursor::Full(table.open_cursor(cut)), Some(r)))
}

/// The probe reads the rows `Cut::Now` reads.
impl UniqueRows for Table {
    fn unique_live_row_at(&self, key: &[u8]) -> Option<StoredRow> {
        Table::unique_live_row_at(self, key)
    }
}

#[cfg(test)]
#[path = "tests/store_io.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/store_io.rs"]
mod bench;
