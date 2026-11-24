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
    /// live row carries, with that row's payload columns `read` names, as
    /// payload bits, in slot order.
    Pk { read: u64 },
    /// The secondary index on this exact column list: its entries under the
    /// keys' spans, in [`KeySpec::index_schema`](gnitz_zset::algebra::KeySpec::index_schema),
    /// in key order, up to `cap` in all.
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
        let schema = relation.schema();
        match probe {
            Probe::Index(cols, cap) => {
                let index = relation.index_or_err(cols.as_slice())?;
                // The keys are the probed spans themselves.
                keyed_as(index.key_spec().key_size())?;
                let mut gather = index.entry_store(relation).gather(keys, Cut::Now);
                let mut entries = Batch::empty_with_schema(&index.key_spec().index_schema(&schema));
                let cap = usize::try_from(cap.get()).unwrap_or(usize::MAX);
                gather.for_each_positive_capped(cap, |c| entries.push_key_row(c.current_pk_bytes(), 1));
                Ok(entries)
            }
            Probe::Pk { read } => {
                let out_schema = relation.probe_reply_schema(probe)?;
                keyed_as(schema.pk_stride())?;
                let table = relation.table_or_err()?;
                // A PK store probe reads a store whose keys net 0 or 1.
                if !relation.kind().nets_unit_per_key() {
                    return Err(format!("probe: PK probe of relation {id}, whose keys may net above 1"));
                }
                Ok(project_unique(table, &keys, &read_cols(&schema, read), &out_schema))
            }
        }
    }
}

impl Relation {
    /// The layout a `probe` of this relation answers in.
    pub fn probe_reply_schema(&self, probe: Probe) -> Result<SchemaDescriptor, String> {
        let schema = self.schema();
        match probe {
            Probe::Pk { read } if read & !schema.payload_slots() != 0 => Err(format!(
                "probe: payload bits {read:#x} past a {}-column schema",
                schema.num_columns()
            )),
            Probe::Pk { read } => project_schema(&schema, &read_cols(&schema, read)),
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
        let whole = |cursor| SourceCursor {
            source: Source::Cursor(cursor),
            walk: None,
        };
        let (keys, walk) = match bound {
            ReadBound::None => return Ok((whole(self.open_cursor(cut, read)), None)),
            ReadBound::PkSet(keys) => {
                check_stride("open", self.id(), keys.stride(), schema.pk_stride())?;
                (keys, None)
            }
            ReadBound::Range(r) if r.walks_pk(schema.pk_cols()) => {
                let mut cursor = table.range_cursor(&r, cut);
                cursor.skip_payload(self.unread(read));
                return Ok((whole(cursor), None));
            }
            ReadBound::Range(r) => {
                // An index holds a pending row's entry from its ingest on, so it is
                // walked only where the store's two cuts read the same rows.
                let index = self.index_on(r.cols().as_slice());
                match index
                    .filter(|_| cut == Cut::Now || !table.has_pending())
                    .map(|ix| ix.entry_store(self).range_cursor(&r, Cut::Now))
                {
                    // No entry in the range: no store cursor to open.
                    Some(idx) if !idx.is_valid() => return Ok((whole(empty_cursor(schema)), None)),
                    Some(idx) if idx.estimated_length() <= table.estimated_rows() / INDEX_SCAN_RATIO => {
                        let stride = schema.pk_stride();
                        let walk = IndexWalk { idx, stride, pks: Vec::new() };
                        (PkKeys::from_sorted(stride, Vec::new()), Some(Box::new(walk)))
                    }
                    _ => {
                        // The reader applies the range, so it reads the range's columns.
                        let walked = schema.payload_bits(r.cols().as_slice().iter().copied());
                        return Ok((whole(self.open_cursor(cut, read | walked)), Some(r)));
                    }
                }
            }
        };
        // A key this worker holds no row for copies nothing.
        let source = match unique {
            Some(table) => Source::Unique(UniqueGather {
                table,
                keys,
                next: 0,
                skip: self.unread(read),
            }),
            // The walk feeds the gather its keys a refill at a time, so its
            // cursor spans the whole store.
            None if walk.is_some() => Source::Merge(PkSetGather::over(self.open_cursor(cut, read), keys)),
            None => Source::Merge(self.gather(keys, cut, read)),
        };
        Ok((SourceCursor { source, walk }, None))
    }
}

/// A chunked source of `Batch`es: a whole cursor, or the rows of a key list.
pub struct SourceCursor<'a> {
    source: Source<'a>,
    /// An index range's walk, feeding a key list's source its keys a refill at
    /// a time. Boxed: it holds a cursor beside the source's.
    walk: Option<Box<IndexWalk>>,
}

/// What a [`SourceCursor`] drains.
// One per read; boxing the merge gather would cost each one an allocation.
#[allow(clippy::large_enum_variant)]
enum Source<'a> {
    Cursor(ReadCursor),
    /// The rows of a strictly ascending key list, gathered off a cursor.
    Merge(PkSetGather),
    Unique(UniqueGather<'a>),
}

impl SourceCursor<'_> {
    /// Whether the source walks a secondary index.
    #[cfg(test)]
    pub(super) fn walks_index(&self) -> bool {
        self.walk.is_some()
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
        loop {
            let chunk = match &mut self.source {
                Source::Cursor(c) => return c.drain_hydrated(max_rows, filter, hydrate),
                Source::Merge(g) => g.drain_hydrated(max_rows, hydrate)?,
                // A store of one row per key holds no skeleton rows.
                Source::Unique(g) => g.drain_chunk(max_rows),
            };
            if let Some(chunk) = chunk {
                // The gather returns every row under an entry's source PK, so a
                // second one is an owner the walk cannot read exactly.
                debug_assert!(
                    self.walk.is_none() || (1..chunk.len()).all(|i| chunk.get_pk_bytes(i - 1) != chunk.get_pk_bytes(i)),
                    "index owner holds two rows under one PK"
                );
                return Ok(Some((chunk, false)));
            }
            let Some(refill) = self.walk.as_mut().and_then(|w| w.refill(max_rows)) else {
                return Ok(None);
            };
            match &mut self.source {
                Source::Merge(g) => g.reload(refill),
                Source::Unique(g) => (g.keys, g.next) = (refill, 0),
                Source::Cursor(_) => unreachable!("an index walk feeds a key list"),
            }
        }
    }
}

/// The walk of one secondary-index key range, feeding each entry's source PK
/// to a key list over the relation it indexes, over one snapshot of each.
/// The key list's source returns every row under an entry's source PK, so the
/// walk is exact only over rows unique on their PK: a source PK then has one
/// live row and at most one live index entry.
struct IndexWalk {
    /// Positioned on the index range and clamped at its end; an entry is a
    /// leading span followed by the source PK.
    idx: ReadCursor,
    /// The source PK's width: the relation's `pk_stride`.
    stride: usize,
    /// A refill's source PKs in index order, flat at `stride`.
    pks: Vec<u8>,
}

impl IndexWalk {
    /// The source PKs of the next `max_rows` index entries, ascending; `None`
    /// once the index range is spent.
    fn refill(&mut self, max_rows: usize) -> Option<PkKeys> {
        let span = self.idx.schema().pk_stride() - self.stride;
        self.pks.clear();
        for _ in 0..max_rows {
            let Some(pk) = self.idx.current_pk() else { break };
            debug_assert!(self.idx.current_weight() > 0, "an index entry at a non-positive weight");
            self.pks.extend_from_slice(&pk[span..]);
            self.idx.advance();
        }
        (!self.pks.is_empty()).then(|| pk_keys(self.stride, self.pks.chunks_exact(self.stride)))
    }
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

/// The columns of the payload slots `read` names, in slot order.
fn read_cols(schema: &SchemaDescriptor, read: u64) -> Vec<u32> {
    (0..schema.num_payload_cols())
        .filter(|&pi| read >> pi & 1 != 0)
        .map(|pi| schema.payload_col_idx(pi) as u32)
        .collect()
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
