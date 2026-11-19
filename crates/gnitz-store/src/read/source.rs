//! [`SourceCursor`] — a chunked source of batches, whichever cursor backs it —
//! [`KeyGather`], the rows of a key list, and [`IndexWalk`], the walk of a
//! secondary-index range that feeds a gather each entry's key.

use gnitz_expr::RowFilter;
use gnitz_trace::runs::{PkSetGather, ReadCursor, StoredRow};
use gnitz_wire::PkKeys;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{SchemaDescriptor, pk_keys};

use crate::storage::Table;

/// A chunked source of `Batch`es: a whole cursor, or the rows of a key list.
// An index walk holds a cursor beside the gather's, so it is boxed.
pub enum SourceCursor<'a> {
    Full(ReadCursor),
    /// The rows of a key list, resolved by `gather`: listed whole (`pk IN (…)`)
    /// with no `walk`, or fed a refill at a time by an index range's `walk`.
    Keyed {
        gather: KeyGather<'a>,
        walk: Option<Box<IndexWalk>>,
    },
}

impl<'a> SourceCursor<'a> {
    /// The walk of `idx` — a secondary-index range, positioned on it and
    /// clamped at its end — gathering each entry's row through `gather`, which
    /// holds no keys yet.
    pub(super) fn index_walk(idx: ReadCursor, gather: KeyGather<'a>) -> Self {
        let walk = IndexWalk { idx, pks: Vec::new() };
        SourceCursor::Keyed { gather, walk: Some(Box::new(walk)) }
    }
}

impl SourceCursor<'_> {
    /// [`ReadCursor::skip_payload`] on whichever cursor or probe the rows are
    /// drained from.
    pub(super) fn skip_payload(&mut self, skip: u64) {
        match self {
            SourceCursor::Full(c) => c.skip_payload(skip),
            SourceCursor::Keyed { gather, .. } => gather.skip_payload(skip),
        }
    }

    /// The next non-empty source chunk, or `None` once the source is exhausted.
    /// A key list's merge gather exceeds `max_rows` rather than split a PK group.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut raw =
            |_: PkKeys| -> Result<Batch, String> { panic!("a raw drain met a skeleton row: hydrate its keys instead") };
        match self.drain_hydrated(max_rows, &mut RowFilter::default(), &mut raw) {
            Ok(chunk) => chunk.map(|(b, _)| b),
            Err(e) => unreachable!("a raw drain hydrates nothing: {e}"),
        }
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
        let (gather, mut walk) = match self {
            SourceCursor::Full(c) => return c.drain_hydrated(max_rows, filter, hydrate),
            SourceCursor::Keyed { gather, walk } => (gather, walk.as_deref_mut()),
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
pub struct IndexWalk {
    /// Positioned on the index range and clamped at its end; an entry is a
    /// leading span followed by the source PK.
    idx: ReadCursor,
    /// A refill's source PKs in index order, flat at the gather's `pk_stride`.
    pks: Vec<u8>,
}

impl IndexWalk {
    /// Reload `gather` with the source PKs of the next `max_rows` index entries;
    /// `false` once the index range is spent.
    fn refill(&mut self, gather: &mut KeyGather<'_>, max_rows: usize) -> bool {
        let stride = gather.schema().pk_stride();
        let span = self.idx.schema().pk_stride() - stride;
        self.pks.clear();
        let mut taken = 0;
        while taken < max_rows && self.idx.is_valid() {
            debug_assert!(self.idx.current_weight() > 0, "an index entry at a non-positive weight");
            self.pks.extend_from_slice(&self.idx.current_pk_bytes()[span..]);
            self.idx.advance();
            taken += 1;
        }
        if taken == 0 {
            return false;
        }
        gather.reload(pk_keys(stride, self.pks.chunks_exact(stride)));
        true
    }
}

/// The rows of a strictly ascending list of keys: gathered off a cursor, or
/// probed key by key from a store whose every key nets 0 or 1.
// One per keyed read; boxing the merge gather would cost each one an allocation.
#[allow(clippy::large_enum_variant)]
pub enum KeyGather<'a> {
    Merge(PkSetGather),
    Unique(UniqueGather<'a>),
}

impl<'a> KeyGather<'a> {
    /// A gather of `keys`, whole PKs of `schema`, probed from `table`, whose
    /// every key nets 0 or 1.
    pub(super) fn unique(table: &'a Table, schema: SchemaDescriptor, keys: PkKeys) -> Self {
        debug_assert_eq!(keys.stride(), schema.pk_stride());
        KeyGather::Unique(UniqueGather { table, schema, keys, next: 0, skip: 0 })
    }

    fn skip_payload(&mut self, skip: u64) {
        match self {
            KeyGather::Merge(g) => g.skip_payload(skip),
            KeyGather::Unique(g) => g.skip = skip,
        }
    }

    fn reload(&mut self, keys: PkKeys) {
        match self {
            KeyGather::Merge(g) => g.reload(keys),
            KeyGather::Unique(g) => (g.keys, g.next) = (keys, 0),
        }
    }

    fn schema(&self) -> &SchemaDescriptor {
        match self {
            KeyGather::Merge(g) => g.schema(),
            KeyGather::Unique(g) => &g.schema,
        }
    }
}

/// Every live row of a strictly ascending list of whole keys, one
/// [`Table::unique_live_row_at`] probe per key.
pub struct UniqueGather<'a> {
    table: &'a Table,
    schema: SchemaDescriptor,
    keys: PkKeys,
    /// Index of the next key, in keys — not bytes.
    next: usize,
    /// See [`ReadCursor::skip_payload`].
    skip: u64,
}

impl UniqueGather<'_> {
    /// The live rows of the next keys, at most `max_rows` of them, at weight 1;
    /// `None` once the list is exhausted.
    fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        assert!(max_rows > 0, "UniqueGather::drain_chunk: max_rows must be positive");
        let mut rows = Vec::with_capacity(max_rows.min(self.keys.len() - self.next));
        for key in self.keys.iter().skip(self.next) {
            if rows.len() >= max_rows {
                break;
            }
            self.next += 1;
            rows.extend(self.table.unique_live_row_at(key));
        }
        if rows.is_empty() {
            return None;
        }
        let mut out = Batch::empty_with_schema(&self.schema);
        out.append_source_rows(rows.iter().map(StoredRow::source), 1, self.skip);
        out.certify_consolidated();
        Some(out)
    }
}
