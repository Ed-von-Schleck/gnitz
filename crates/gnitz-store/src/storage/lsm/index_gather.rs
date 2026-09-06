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

use super::batch::{Batch, Layout};
use super::read_cursor::{PkSetGather, ReadCursor};
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
///
/// That the base snapshot is taken *after* the index snapshot is the safe order
/// for the non-atomic base-then-index write path: an entry the index cursor
/// yields had its base row written earlier still, so no row can go missing.
// `pub`, but `storage/mod.rs` re-exports it `pub(crate)`: reachable as
// `SourceCursor::Bounded`'s field type, nameable only in this crate.
pub struct BoundedIndexCursor {
    idx: ReadCursor,
    src: ReadCursor,
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
            src,
            pks: Vec::with_capacity(pk_capacity * stride),
            order: Vec::with_capacity(pk_capacity),
            spec,
        }
    }

    /// The next up-to-`n` in-range rows, or `None` when the range is exhausted.
    /// A returned batch may be EMPTY (in-range index entries whose base rows are
    /// absent/retracted) — `None` strictly means "no further chunk exists".
    ///
    /// No re-seek between chunks: chunk N+1's first PK may sort below chunk N's
    /// last, and `advance_to` is backward-capable via a binary search, so a chunk
    /// boundary costs O(log N) on the first probe — not a rescan.
    pub(crate) fn drain_chunk(&mut self, n: usize) -> Option<Batch> {
        let stride = self.src.schema.pk_stride();
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
        // One live payload per PK (an index owner is always a base table), so the
        // collected count sizes the result exactly.
        let mut batch = Batch::with_capacity(&self.src.schema, collected);
        // Skipping a record equal to its predecessor IS the dedup, so each PK
        // group is copied once, at its net `current_weight` (never a hardcoded 1,
        // so Z-Set multiplicity survives).
        let mut prev: &[u8] = &[];
        for &i in &self.order {
            let at = i as usize * stride;
            let pk = &self.pks[at..at + stride];
            if pk == prev {
                continue;
            }
            prev = pk;
            self.src.copy_live_pk_group_into(pk, &mut batch);
        }
        // Consolidated by construction: the group walk emits (PK, payload) order
        // at sub-group granularity over strictly-ascending PKs, at nonzero net
        // weights. Certifying it spares the ingest tail an O(chunk log chunk)
        // re-sort, as the full-scan drain path does.
        batch.certify_layout(Layout::Consolidated, &self.src.schema);
        Some(batch)
    }
}

/// A chunked source of `Batch`es over one relation, in every shape a bound can
/// take. Interchangeable by construction for the circuit backfill: the circuit's
/// `Filter` decides what the view contains, so which variant is chosen only
/// decides how many rows the scan reads. The ad-hoc `ReadSpec` scan adds the
/// `PkSet` shape and drives the same enum.
///
/// The cursor variants are boxed (a `ReadCursor` is several hundred bytes;
/// clippy's `large_enum_variant`) — one allocation per scan, never per chunk.
pub enum SourceCursor {
    Full(Box<ReadCursor>),
    Bounded(Box<BoundedIndexCursor>),
    /// `pk IN (…)` gather over a listed key set.
    PkSet(Box<PkSetGather>),
    /// A provably-empty index range. Distinct from `Full` so nothing is scanned,
    /// and a variant rather than an error so the source still feeds one empty
    /// epoch (which is what mints a global aggregate's ground row).
    Empty,
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
        match self {
            SourceCursor::Full(c) => c.drain_chunk(max_rows),
            SourceCursor::Bounded(c) => c.drain_chunk(max_rows),
            SourceCursor::PkSet(g) => g.next_chunk(max_rows),
            SourceCursor::Empty => None,
        }
    }
}
