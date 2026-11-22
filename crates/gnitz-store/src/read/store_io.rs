//! Read I/O on relation families. [`Relation::open_read`] and
//! [`Relation::open_sealed`] open every source a bound can name as a
//! [`SourceCursor`] — a whole cursor, the rows of a key list, or the walk of a
//! secondary-index range feeding that list each entry's key — choosing between
//! the store and an index over it; [`RelationRegistry::probe`] answers a HasPk
//! probe, in the layout [`Relation::probe_reply_schema`] names.

use super::check_stride;
use crate::relation::{Cut, Relation, RelationRegistry};
use crate::storage::Table;
use gnitz_expr::{ColumnLocator, RowFilter};
use gnitz_trace::runs::{PkSetGather, ReadCursor, StoredRow, empty_cursor};
use gnitz_wire::{KeyRange, PkColList, PkKeys, ReadBound};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{SchemaDescriptor, pk_keys, project_schema};
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
        let out_schema = relation.probe_reply_schema(probe)?;
        let cols: &[u32] = match &probe {
            Probe::Pk => &[],
            Probe::PkColumn(col) => std::slice::from_ref(col),
            Probe::Index(cols, cap) => {
                let index = relation.index_or_err(cols.as_slice())?;
                // The keys are the probed spans themselves.
                keyed_as(index.key_spec().key_size())?;
                let mut entries = Batch::empty_with_schema(&out_schema);
                let cap = usize::try_from(cap.get()).unwrap_or(usize::MAX);
                index.for_each_entry(relation, keys, cap, |entry| entries.push_key_row(entry, 1));
                return Ok(entries);
            }
        };
        keyed_as(relation.schema().pk_stride())?;
        let table = relation.table_or_err()?;
        // A PK store probe reads a store whose keys net 0 or 1.
        if !relation.kind().nets_unit_per_key() {
            return Err(format!("probe: PK probe of relation {id}, whose keys may net above 1"));
        }
        Ok(project_unique(table, &keys, cols, &out_schema))
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
        let table = self.table_or_err()?;
        self.open_source(bound, Cut::Now, self.kind().nets_unit_per_key().then_some(table), read)
    }

    /// `unique`, when given, is this relation's store, probed for each key a
    /// key list or an index entry names; a probe reads the rows `Cut::Now`
    /// reads.
    fn open_source<'a>(
        &self,
        bound: ReadBound,
        cut: Cut,
        unique: Option<&'a Table>,
        read: u64,
    ) -> Result<(SourceCursor<'a>, Option<KeyRange>), String> {
        debug_assert!(
            unique.is_none() || cut == Cut::Now,
            "a key probe reads the rows Cut::Now reads"
        );
        let (schema, table) = (self.schema(), self.table_or_err()?);
        let full = |cursor| SourceCursor(Source::Full(cursor));
        let (keys, walk) = match bound {
            ReadBound::None => return Ok((full(self.open_cursor(cut, read)), None)),
            ReadBound::PkSet(keys) => {
                check_stride("open", self.id(), keys.stride(), schema.pk_stride())?;
                (keys, None)
            }
            ReadBound::Range(r) if r.walks_pk(schema.pk_cols()) => {
                let mut cursor = table.range_cursor(&r, cut);
                cursor.skip_payload(self.unread(read));
                return Ok((full(cursor), None));
            }
            ReadBound::Range(r) => {
                // An index holds a pending row's entry from its ingest on, so it is
                // walked only where the store's two cuts read the same rows.
                let index = self.index_on(r.cols().as_slice());
                match index
                    .filter(|_| cut == Cut::Now || !table.has_pending())
                    .map(|ic| ic.cursor_over(&r))
                {
                    // No entry in the range: no store cursor to open.
                    Some(idx) if !idx.is_valid() => return Ok((full(empty_cursor(schema)), None)),
                    Some(idx) if idx.estimated_length() <= table.estimated_rows() / INDEX_SCAN_RATIO => {
                        let stride = schema.pk_stride();
                        let walk = IndexWalk { idx, stride, pks: Vec::new() };
                        (PkKeys::from_sorted(stride, Vec::new()), Some(Box::new(walk)))
                    }
                    _ => {
                        // The reader applies the range, so it reads the range's columns.
                        let walked = schema.payload_bits(r.cols().as_slice().iter().copied());
                        return Ok((full(self.open_cursor(cut, read | walked)), Some(r)));
                    }
                }
            }
        };
        // A key this worker holds no row for copies nothing.
        let gather = match unique {
            Some(table) => KeyGather::Unique(UniqueGather {
                table,
                keys,
                next: 0,
                skip: self.unread(read),
            }),
            // The walk feeds the gather its keys a refill at a time, so its
            // cursor spans the whole store.
            None if walk.is_some() => KeyGather::Merge(PkSetGather::over(self.open_cursor(cut, read), keys)),
            None => KeyGather::Merge(self.gather(keys, cut, read)),
        };
        Ok((SourceCursor(Source::Keyed { gather, walk }), None))
    }
}

/// A chunked source of `Batch`es: a whole cursor, or the rows of a key list.
pub struct SourceCursor<'a>(Source<'a>);

/// What backs a [`SourceCursor`].
// An index walk holds a cursor beside the gather's, so it is boxed.
enum Source<'a> {
    Full(ReadCursor),
    /// The rows of a key list, resolved by `gather`: listed whole (`pk IN (…)`)
    /// with no `walk`, or fed a refill at a time by an index range's `walk`.
    Keyed {
        gather: KeyGather<'a>,
        walk: Option<Box<IndexWalk>>,
    },
}

impl SourceCursor<'_> {
    /// Whether the source walks a secondary index.
    #[cfg(test)]
    pub(super) fn walks_index(&self) -> bool {
        matches!(self.0, Source::Keyed { walk: Some(_), .. })
    }

    /// The next non-empty source chunk, or `None` once the source is exhausted.
    /// A key list's merge gather exceeds `max_rows` rather than split a PK group.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut raw = |_: PkKeys| Err(String::new());
        (self.drain_hydrated(max_rows, &mut RowFilter::default(), &mut raw))
            .expect("a raw drain met a skeleton row: hydrate its keys instead")
            .map(|(b, _)| b)
    }

    /// [`Self::drain_chunk`] over a store that may hold skeleton rows: each one the
    /// chunk meets is replaced by its key's rows, which `hydrate` recomputes from
    /// the keys, ascending. A skeleton row counts once against `max_rows` however
    /// many rows it hydrates to. A whole cursor leaves out the rows of its
    /// in-memory runs that `filter` drops; every other row stays, hydrated ones
    /// included, for the caller to filter. The `bool` is whether `filter` already
    /// decided every row of the chunk, so the caller need not filter it again:
    /// only a whole cursor's chunk drawn from in-memory runs alone, with no
    /// skeleton row hydrated. An index range or a key list never reads `filter`.
    pub(super) fn drain_hydrated(
        &mut self,
        max_rows: usize,
        filter: &mut RowFilter,
        hydrate: &mut impl FnMut(PkKeys) -> Result<Batch, String>,
    ) -> Result<Option<(Batch, bool)>, String> {
        let (gather, mut walk) = match &mut self.0 {
            Source::Full(c) => return c.drain_hydrated(max_rows, filter, hydrate),
            Source::Keyed { gather, walk } => (gather, walk.as_deref_mut()),
        };
        loop {
            let chunk = match gather {
                KeyGather::Merge(g) => g.drain_hydrated(max_rows, hydrate)?,
                // A store of one row per key holds no skeleton rows.
                KeyGather::Unique(g) => g.drain_chunk(max_rows),
            };
            if let Some(chunk) = chunk {
                // The gather returns every row under an entry's source PK, so a
                // second one is an owner the walk cannot read exactly.
                debug_assert!(
                    walk.is_none() || (1..chunk.len()).all(|i| chunk.get_pk_bytes(i - 1) != chunk.get_pk_bytes(i)),
                    "index owner holds two rows under one PK"
                );
                return Ok(Some((chunk, false)));
            }
            let Some(w) = walk.as_mut() else { return Ok(None) };
            if !w.refill(gather, max_rows) {
                return Ok(None);
            }
        }
    }
}

/// The walk of one secondary-index key range, feeding each entry's source PK
/// to a [`KeyGather`] over the relation it indexes, over one snapshot of each.
/// The gather returns every row under an entry's source PK, so the walk is
/// exact only over rows unique on their PK: a source PK then has one live row
/// and at most one live index entry.
struct IndexWalk {
    /// Positioned on the index range and clamped at its end; an entry is a
    /// leading span followed by the source PK.
    idx: ReadCursor,
    /// The source PK's width: the gather's `pk_stride`.
    stride: usize,
    /// A refill's source PKs in index order, flat at `stride`.
    pks: Vec<u8>,
}

impl IndexWalk {
    /// Reload `gather` with the source PKs of the next `max_rows` index entries;
    /// `false` once the index range is spent.
    fn refill(&mut self, gather: &mut KeyGather<'_>, max_rows: usize) -> bool {
        let span = self.idx.schema().pk_stride() - self.stride;
        self.pks.clear();
        let mut taken = 0;
        while taken < max_rows
            && let Some(pk) = self.idx.current_pk()
        {
            debug_assert!(self.idx.current_weight() > 0, "an index entry at a non-positive weight");
            self.pks.extend_from_slice(&pk[span..]);
            self.idx.advance();
            taken += 1;
        }
        if taken == 0 {
            return false;
        }
        let keys = pk_keys(self.stride, self.pks.chunks_exact(self.stride));
        match gather {
            KeyGather::Merge(g) => g.reload(keys),
            KeyGather::Unique(g) => (g.keys, g.next) = (keys, 0),
        }
        true
    }
}

/// The rows of a strictly ascending list of keys: gathered off a cursor, or
/// probed key by key from a store whose every key nets 0 or 1.
// One per keyed read; boxing the merge gather would cost each one an allocation.
#[allow(clippy::large_enum_variant)]
enum KeyGather<'a> {
    Merge(PkSetGather),
    Unique(UniqueGather<'a>),
}

/// Every live row of a strictly ascending list of whole keys of `table`, whose
/// every key nets 0 or 1, one [`Table::unique_live_row_at`] probe per key.
struct UniqueGather<'a> {
    table: &'a Table,
    keys: PkKeys,
    /// Index of the next key, in keys — not bytes.
    next: usize,
    /// The payload columns written as zeroes; see [`ReadCursor::skip_payload`].
    skip: u64,
}

impl UniqueGather<'_> {
    /// The live rows of the next keys, at most `max_rows` of them, at weight 1;
    /// `None` once the list is exhausted.
    fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        assert!(max_rows > 0, "UniqueGather::drain_chunk: max_rows must be positive");
        let (table, mut keys, next) = (self.table, self.keys.iter().skip(self.next), &mut self.next);
        let mut found = 0;
        let live = std::iter::from_fn(|| {
            while found < max_rows {
                let key = keys.next()?;
                *next += 1;
                if let Some(row) = table.unique_live_row_at(key) {
                    found += 1;
                    return Some(row);
                }
            }
            None
        });
        let mut out = StoredRow::gather(table.schema(), live, 1, self.skip);
        if out.is_empty() {
            return None;
        }
        out.certify_consolidated();
        Some(out)
    }
}

/// Every live row of `keys`, a strictly ascending list of whole PKs of
/// `table`, probed from it, whose every key nets 0 or 1, at weight 1 and
/// in `out_schema`, the [`project_schema`] layout of `cols`: the PK, then the
/// payload columns `cols` names. The probe reads the rows `Cut::Now` reads.
fn project_unique(table: &Table, keys: &PkKeys, cols: &[u32], out_schema: &SchemaDescriptor) -> Batch {
    let schema = table.schema();
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
