//! Index-indirected row gather: walk one cursor's key range, resolve each
//! collected source PK against a second cursor. The storage-level mechanism
//! under both the wire range-seek and the bounded backfill scan — all policy
//! (which index, whether to bound at all, selectivity gating) stays with the
//! caller.
//!
//! **An index owner is always a base table**, so `enforce_unique_pk` gives each
//! PK exactly one live payload and a PK group cannot straddle a chunk boundary.
//! That is what makes a chunked walk exact without a per-row window filter. If
//! the invariant ever broke, a skewed entry would yield a visibly wrong row
//! rather than being silently dropped.

use super::batch::{Batch, Layout};
use super::read_cursor::ReadCursor;
use crate::schema::key::PkBuf;
use crate::schema::IndexKeySpec;

/// Resolve already-collected source PKs against the base table into a result
/// batch — every live row of each collected PK group, at its net
/// `current_weight` (never a hardcoded 1, so Z-Set multiplicity is preserved) —
/// or `None` when nothing resolves.
///
/// The base cursor is caller-held so a chunked walk resolves every chunk against
/// ONE base snapshot (a per-chunk re-open would give successive chunks different
/// snapshots: a torn read no full scan can produce).
///
/// `pks` must be **ascending** and **deduplicated** (each PK group is walked
/// once). Ascending order makes each routed partition's probes one monotone
/// forward sweep that keeps shard pages and merge state hot, and makes the
/// output (PK, payload)-ascending; the PKs are index entries' source-PK OPK
/// suffixes, whose memcmp order equals base storage order, so a byte sort *is*
/// the seek order. Ascending **within** a chunk only — chunk N+1's first PK may
/// sort below chunk N's last, which is why the sweep must stay backward-capable.
fn gather_source_rows(src: &mut ReadCursor, pks: &[PkBuf]) -> Option<Batch> {
    debug_assert!(
        pks.windows(2).all(|w| w[0] < w[1]),
        "gather_source_rows requires strictly ascending (sorted + deduped) PKs"
    );
    // An index owner is always a base table (one live payload per PK), so
    // `pks.len()` sizes the result exactly.
    let src_schema = *src.schema();
    let mut cand = Batch::with_capacity(src_schema, pks.len());
    for pk in pks {
        src.copy_live_pk_group_into(pk.pk_bytes(), &mut cand);
    }
    (cand.count > 0).then_some(cand)
}

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
pub struct BoundedIndexCursor {
    idx: ReadCursor,
    src: ReadCursor,
    pks: Vec<PkBuf>,
    spec: IndexKeySpec,
}

impl BoundedIndexCursor {
    /// Position `idx` at `start` and wrap the index cursor and the base cursor
    /// for the walk over the half-open range `[start, end)` (`end = None` ⇒ to
    /// the end of the index). The range is the cursor's from then on — no walk
    /// re-checks it, per `ReadCursor::seek_range_bytes`. `spec` supplies
    /// `key_size()`, the leading-key byte length where each entry's source-PK OPK
    /// suffix starts. `pk_capacity` pre-sizes the per-chunk PK scratch (pass the
    /// measured range size capped at the chunk size, or 0 to grow).
    pub(crate) fn new(
        mut idx: ReadCursor,
        src: ReadCursor,
        start: PkBuf,
        end: Option<PkBuf>,
        spec: IndexKeySpec,
        pk_capacity: usize,
    ) -> Self {
        idx.seek_range_bytes(start.pk_bytes(), end.as_ref().map(PkBuf::pk_bytes));
        BoundedIndexCursor {
            idx,
            src,
            pks: Vec::with_capacity(pk_capacity),
            spec,
        }
    }

    /// The next up-to-`n` in-range rows, or `None` when the range is exhausted.
    /// A returned batch may be EMPTY (in-range index entries whose base rows are
    /// absent/retracted) — `None` strictly means "no further chunk exists".
    pub(crate) fn drain_chunk(&mut self, n: usize) -> Option<Batch> {
        self.pks.clear();
        // `new` clamped the cursor at `end`, so exhaustion IS the range bound.
        while self.idx.valid && self.pks.len() < n {
            // The gate is `> 0` on the CONSOLIDATED merge group, not a per-entry
            // presence test: an UPDATE of an indexed column retracts the old index
            // entry and inserts the new one, so a range spanning both values sees
            // the old key at net weight 0 and collects the source PK exactly once.
            if self.idx.current_weight > 0 {
                let cur = self.idx.current_pk_bytes();
                self.pks.push(PkBuf::from_bytes(self.spec.split_entry(cur).1));
            }
            self.idx.advance();
        }
        // The walk's verdict, never the gather's: the loop exits with `pks` empty
        // only once the index cursor is exhausted, so a `None` here is final.
        if self.pks.is_empty() {
            return None;
        }
        // A range spans many duplicate groups, so collected PKs interleave across
        // the base; the gather requires ascending order for its monotone sweep.
        // No re-seek between chunks: chunk N+1's first PK may sort below chunk N's
        // last, and `advance_to` is backward-capable via a binary search, so a
        // chunk boundary costs O(log N) on the first probe — not a rescan.
        // The dedup gives the gather the strictly-ascending PK list its monotone
        // sweep requires, so each PK group is walked exactly once.
        self.pks.sort_unstable();
        self.pks.dedup();
        // `Some(empty)`, not `None`: the gather returns `None` when nothing
        // resolves. Both backfill drivers read `None` as exhaustion, so letting
        // that escape would silently truncate the view mid-range.
        let mut batch =
            gather_source_rows(&mut self.src, &self.pks).unwrap_or_else(|| Batch::empty_with_schema(self.src.schema()));
        // Consolidated by construction: the group walk emits (PK, payload) order
        // at sub-group granularity over strictly-ascending PKs, at nonzero net
        // weights. Certifying it spares the ingest tail an O(chunk log chunk)
        // re-sort, as the full-scan drain path does.
        batch.certify_layout(Layout::Consolidated, self.src.schema());
        Some(batch)
    }
}
