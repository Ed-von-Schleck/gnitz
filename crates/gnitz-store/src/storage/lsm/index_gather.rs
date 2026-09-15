//! Index-indirected row gather: walk one cursor's key range, resolve each
//! collected source PK against a second cursor. The storage-level mechanism
//! under both the wire range-seek and the bounded backfill scan — all policy
//! (which index, whether to bound at all, selectivity gating) stays with the
//! caller. [`SourceCursor`] sits here too: it is the one dispatch over this
//! walk and its unbounded siblings, and names no catalog concept.
//!
//! **An index owner is always a base table**, so `enforce_unique_pk` gives each
//! PK exactly one live payload and a PK group cannot straddle a chunk boundary.
//! That is what makes a chunked walk exact without a per-row window filter. If
//! the invariant ever broke, a skewed entry would yield a visibly wrong row
//! rather than being silently dropped.

use super::batch::Batch;
use super::read_cursor::{PkSetGather, ReadCursor, SkeletonKeys};
use crate::schema::IndexKeySpec;
use crate::storage::spill::sort_indices;

/// A chunked walk of one secondary-index key range, gathering each in-range live
/// entry's source row from the base table. One unchunked drain
/// (`drain_chunk(usize::MAX)`) is the wire range-seek; the chunked drive is the
/// bounded backfill scan — one mechanism under both.
///
/// Holds the index cursor and the base cursor **at once**, and nothing is
/// re-opened between chunks: `open_cursor` returns an `Rc` snapshot of the
/// store's runs, held for the whole chunked backfill, so re-opening per chunk
/// would give successive chunks different snapshots — a torn read no full scan
/// can produce. Holding both is safe: a `ReadCursor` owns `Rc<MappedShard>` /
/// `Rc<Batch>`, which pins the mmap rather than the file, and memtable runs are
/// never mutated in place.
// `pub`, but `storage/mod.rs` re-exports it `pub(crate)`: reachable as
// `SourceCursor::Bounded`'s field type, nameable only in this crate.
pub struct BoundedIndexCursor {
    idx: ReadCursor,
    /// The base cursor, walked once per chunk over that chunk's sorted PKs.
    src: PkSetGather,
    /// The key buffer passed back and forth with `src`, so a chunk allocates none.
    spare: Vec<u8>,
    /// The chunk's collected source-PK OPK images, flat at `src`'s `pk_stride` —
    /// what `spec.split_entry` leaves. Flat bytes, not `Vec<PkBuf>`: a chunk runs
    /// to the DDL scan chunk size and is unbounded on the ungated drain, the
    /// accumulator shape `spill`'s header argues for.
    pks: Vec<u8>,
    /// Reused index permutation for the indirect sort of `pks`.
    order: Vec<u32>,
    spec: IndexKeySpec,
}

impl BoundedIndexCursor {
    /// Wrap an index cursor its opener already range-seeked — that seek is the
    /// only thing bounding the walk — plus the base cursor its entries resolve
    /// against. `pk_capacity` pre-sizes the per-chunk PK scratch, in keys.
    pub(crate) fn new(idx: ReadCursor, src: ReadCursor, spec: IndexKeySpec, pk_capacity: usize) -> Self {
        let stride = src.schema.pk_stride();
        BoundedIndexCursor {
            idx,
            src: PkSetGather::over(src),
            spare: Vec::new(),
            pks: Vec::with_capacity(pk_capacity * stride),
            order: Vec::with_capacity(pk_capacity),
            spec,
        }
    }

    /// The next up-to-`n` in-range rows, or `None` when the range is exhausted.
    /// A returned batch may be EMPTY (in-range index entries whose base rows are
    /// absent/retracted) — `None` strictly means "no further chunk exists".
    ///
    /// No re-open between chunks: chunk N+1's first PK may sort below chunk N's
    /// last, and reloading the gather ends its sweep, so that key repositions by a
    /// binary search per run — O(log N) on the first probe, not a rescan.
    pub(crate) fn drain_chunk(&mut self, n: usize) -> Option<Batch> {
        let stride = self.src.schema().pk_stride();
        self.pks.clear();
        let mut collected = 0;
        // `new` clamped the cursor at `end`, so exhaustion IS the range bound.
        while self.idx.valid && collected < n {
            // The gate is `> 0` on the CONSOLIDATED merge group, not a per-entry
            // presence test: an UPDATE of an indexed column retracts the old index
            // entry and inserts the new one, so a range spanning both values sees
            // the old key at net weight 0 and collects the source PK exactly once.
            if self.idx.current_weight > 0 {
                self.pks
                    .extend_from_slice(self.spec.split_entry(self.idx.current_pk_bytes()).1);
                collected += 1;
            }
            self.idx.advance();
        }
        // The walk's verdict, never the gather's: the loop exits with nothing
        // collected only once the index cursor is exhausted, so `None` is final.
        if collected == 0 {
            return None;
        }
        // A range spans many duplicate groups, so collected PKs interleave across
        // the base and the gather below needs them ascending. These are index
        // entries' source-PK OPK suffixes, whose memcmp order IS base storage
        // order, so a byte sort is the seek order.
        sort_indices(&self.pks, stride, &mut self.order);
        // Skipping a record equal to its predecessor IS the dedup, so the gather's
        // list is strictly ascending and each PK group is copied once.
        let mut keys = std::mem::take(&mut self.spare);
        keys.clear();
        let mut prev: &[u8] = &[];
        for &i in &self.order {
            let pk = &self.pks[i as usize * stride..(i as usize + 1) * stride];
            if pk != prev {
                keys.extend_from_slice(pk);
                prev = pk;
            }
        }
        self.spare = self.src.reload(keys);
        // A window whose base rows all resolved away is an empty chunk, not the end.
        Some(
            self.src
                .next_chunk(usize::MAX)
                .unwrap_or_else(|| Batch::empty_with_schema(self.src.schema())),
        )
    }
}

/// A chunked source of `Batch`es over one relation, in every shape a bound can
/// take.
pub enum SourceCursor {
    Full(Box<ReadCursor>),
    Bounded(Box<BoundedIndexCursor>),
    /// `pk IN (…)` gather over a listed key set.
    PkSet(Box<PkSetGather>),
}

impl SourceCursor {
    /// The next source rows, or `None` once the source is exhausted. A returned
    /// batch may be empty — `Bounded` yields one for a window of index entries
    /// whose base rows all resolved away — so only `None` means exhausted.
    ///
    /// `max_rows` bounds every variant exactly except `PkSet`, which tests it
    /// before each key and then drains that key's whole group, so it can
    /// overshoot to `max_rows - 1 + |largest group|`. Callers read `chunk.len()`.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut skeletons = SkeletonKeys::default();
        let chunk = self.drain_live_chunk(max_rows, &mut skeletons);
        skeletons.assert_none();
        chunk
    }

    /// [`Self::drain_chunk`] with skeleton rows split out; see [`ReadCursor::drain_live_chunk`].
    pub(crate) fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_live_chunk(max_rows, skeletons),
            // An index owner is a base table, which holds no skeleton row.
            SourceCursor::Bounded(c) => c.drain_chunk(max_rows),
            SourceCursor::PkSet(g) => g.next_live_chunk(max_rows, skeletons),
        }
    }
}
