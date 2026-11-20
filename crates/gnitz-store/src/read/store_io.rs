//! Read I/O on relation families. [`Relation::open_read`] and
//! [`Relation::open_sealed`] open every source a bound can name as a
//! [`SourceCursor`], choosing between the store and an index over it;
//! [`RelationRegistry::probe`] answers a HasPk probe, in the layout
//! [`Relation::probe_reply_schema`] names.

use super::check_stride;
use super::source::{KeyGather, SourceCursor};
use crate::relation::{Cut, Relation, RelationRegistry};
use crate::storage::Table;
use gnitz_expr::ColumnLocator;
use gnitz_trace::runs::{PkSetGather, empty_cursor};
use gnitz_wire::{KeyRange, PkColList, PkKeys, ReadBound};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{SchemaDescriptor, project_schema};
use std::num::NonZeroU64;

/// One `HasPk` probe: the store it reads, and what it answers each matched key
/// with.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Probe {
    /// The relation's own PK store, where every key nets 0 or 1: each key a
    /// live row carries, echoed.
    Pk,
    /// The relation's own PK store, where every key nets 0 or 1: each key a
    /// live row carries, with that row's column.
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
        let keyed_as = |stride| check_stride("probe", id, keys.stride(), stride);
        // A PK store probe reads a store whose keys net 0 or 1.
        let unique_rows = || {
            keyed_as(relation.schema().pk_stride())?;
            relation.table_or_err()?;
            (relation.unique_rows())
                .ok_or_else(|| format!("probe: PK probe of relation {id}, whose keys may net above 1"))
        };
        let out_schema = relation.probe_reply_schema(probe)?;
        match probe {
            Probe::Pk => Ok(project_unique(
                unique_rows()?,
                &relation.schema(),
                &keys,
                &[],
                &out_schema,
            )),
            Probe::PkColumn(col) => Ok(project_unique(
                unique_rows()?,
                &relation.schema(),
                &keys,
                &[col],
                &out_schema,
            )),
            Probe::Index(cols, cap) => {
                let index = relation.index_or_err(cols.as_slice())?;
                // The keys are the probed spans themselves.
                keyed_as(index.key_spec().key_size())?;
                let mut entries = Batch::empty_with_schema(&out_schema);
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
            Probe::Pk => project_schema(&schema, &[]),
            Probe::PkColumn(col) => project_schema(&schema, &[col]),
            Probe::Index(cols, _) => Ok(self.index_or_err(cols.as_slice())?.key_spec().index_schema(&schema)),
        }
    }

    /// Open `bound`'s source over the rows `Cut::Sealed` reads, without walking
    /// it, for a reader of the payload columns `read` names, as payload bits:
    /// the source may write every other column as zeroes. The source borrows
    /// nothing, so a backfill holds it across its own registry writes; its
    /// circuit filters the rows whole, so the part of `bound` the source does
    /// not apply is dropped.
    pub fn open_sealed(&self, bound: ReadBound, read: u64) -> Result<SourceCursor<'static>, String> {
        Ok(self.open_source(bound, Cut::Sealed, None, read)?.0)
    }

    /// Open `bound`'s source over the rows `Cut::Now` reads, without walking it,
    /// and the part of `bound` the source does not apply, for a reader of the
    /// payload columns `read` names, as in [`Self::open_sealed`]. A store whose
    /// keys net 0 or 1 answers each whole key it is asked for by its probe
    /// instead of a cursor seek.
    pub(crate) fn open_read(
        &self,
        bound: ReadBound,
        read: u64,
    ) -> Result<(SourceCursor<'_>, Option<KeyRange>), String> {
        self.open_source(bound, Cut::Now, self.unique_rows(), read)
    }

    /// This process's store of the relation, where its every key nets 0 or 1;
    /// `None` for any other kind, or where this process holds no store.
    fn unique_rows(&self) -> Option<&Table> {
        let table = self.table_or_err().ok()?;
        self.kind().nets_unit_per_key().then_some(table)
    }

    fn open_source<'a>(
        &self,
        bound: ReadBound,
        cut: Cut,
        unique: Option<&'a Table>,
        read: u64,
    ) -> Result<(SourceCursor<'a>, Option<KeyRange>), String> {
        let schema = self.schema();
        self.table_or_err()?;
        Ok(match bound {
            ReadBound::None => (SourceCursor::full(self.open_cursor(cut, read)), None),
            ReadBound::PkSet(keys) => {
                check_stride("open", self.id(), keys.stride(), schema.pk_stride())?;
                // A key this worker holds no row for copies nothing.
                let gather = match unique {
                    Some(store) => KeyGather::unique(store, schema, keys, self.unread(read)),
                    None => KeyGather::Merge(self.gather(keys, cut, read)),
                };
                (SourceCursor::listed(gather), None)
            }
            ReadBound::Range(r) => self.open_range(r, cut, unique, read),
        })
    }

    /// The walk `r` names over this relation's store, which holds one, for a
    /// reader of the payload columns `read` names, as payload bits, and the
    /// part of it the cursor leaves unapplied. `unique`, when given, is that
    /// store, probed for an index entry's row.
    fn open_range<'a>(
        &self,
        r: KeyRange,
        cut: Cut,
        unique: Option<&'a Table>,
        read: u64,
    ) -> (SourceCursor<'a>, Option<KeyRange>) {
        let (schema, table) = (self.schema(), self.table());
        if r.walks_pk(schema.pk_cols()) {
            let mut cursor = table.range_cursor(&r, cut);
            cursor.skip_payload(self.unread(read));
            return (SourceCursor::full(cursor), None);
        }
        // An index holds a pending row's entry from its ingest on, so it is
        // walked only where the store's two cuts read the same rows.
        let index = self
            .index_on(r.cols().as_slice())
            .filter(|_| cut == Cut::Now || !table.has_pending());
        if let Some(ic) = index {
            let idx = ic.cursor_over(&r);
            // No entry in the range: no store cursor to open.
            if !idx.is_valid() {
                return (SourceCursor::full(empty_cursor(schema)), None);
            }
            if idx.estimated_length() <= table.estimated_rows() / INDEX_SCAN_RATIO {
                // The walk feeds the gather its keys.
                let none = PkKeys::from_sorted(schema.pk_stride(), Vec::new());
                let gather = match unique {
                    Some(store) => KeyGather::unique(store, schema, none, self.unread(read)),
                    None => KeyGather::Merge(PkSetGather::over(self.open_cursor(cut, read), none)),
                };
                return (SourceCursor::index_walk(idx, gather), None);
            }
        }
        // The reader applies the range, so it reads the range's columns.
        let walked = schema.payload_bits(r.cols().as_slice().iter().copied());
        (SourceCursor::full(self.open_cursor(cut, read | walked)), Some(r))
    }
}

/// Every live row of `keys`, a strictly ascending list of whole PKs of
/// `schema`, probed from `table`, whose every key nets 0 or 1, at weight 1 and
/// in `out_schema`, the [`project_schema`] layout of `cols`: the PK, then the
/// payload columns `cols` names. The probe reads the rows `Cut::Now` reads.
fn project_unique(
    table: &Table,
    schema: &SchemaDescriptor,
    keys: &PkKeys,
    cols: &[u32],
    out_schema: &SchemaDescriptor,
) -> Batch {
    debug_assert_eq!(keys.stride(), schema.pk_stride());
    let locs: Vec<ColumnLocator> = cols.iter().map(|&c| schema.locate(c as usize)).collect();
    let mut out = Batch::with_capacity(out_schema, keys.len());
    for stored in keys.iter().filter_map(|k| table.unique_live_row_at(k)) {
        let (src, row) = stored.source();
        out.append_projected_row(&locs, src, row, 1);
    }
    // One row per key, in ascending key order.
    out.certify_consolidated();
    out
}

#[cfg(test)]
#[path = "tests/store_io.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/store_io.rs"]
mod bench;
