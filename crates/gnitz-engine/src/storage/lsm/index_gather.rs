//! Index-indirected row gather: walk one cursor's key range, resolve each
//! collected source PK against a second cursor. The storage-level mechanism
//! under both the wire range-seek and the bounded backfill scan — all policy
//! (which index, whether to bound at all, selectivity gating) stays with the
//! caller.

use super::batch::{Batch, Layout};
use super::read_cursor::ReadCursor;
use crate::schema::key::PkBuf;
use crate::schema::SchemaDescriptor;

/// Resolve already-collected source PKs against a base-table cursor into a
/// result batch — every present, live, exact-PK row at its net `current_weight`
/// (never a hardcoded 1, so Z-Set multiplicity is preserved) — or `None` when
/// nothing resolves.
///
/// The cursor is caller-held so a chunked walk resolves every chunk against ONE
/// base snapshot (a per-chunk re-open would give successive chunks different
/// snapshots: a torn read no full scan can produce).
///
/// `pks` must be **ascending** in `compare_pk_bytes` (storage) order: each probe
/// then lower-bounds at or past the previous key, turning K scattered
/// point-seeks into one monotone forward sweep that keeps shard pages and merge
/// state hot. The PKs are index entries' source-PK OPK suffixes, whose memcmp
/// order equals base storage order, so a byte sort *is* the seek order; `PkBuf`
/// carries the exact `src_pk_stride` bytes inline (up to `MAX_PK_BYTES`), so
/// wide sources resolve with no widen. `acc` is sized to `pks.len()` — a tight
/// upper bound, since base PKs are unique and each carries one indexed value —
/// so it never grows row by row.
pub(crate) fn gather_source_rows(
    src_cursor: &mut ReadCursor,
    src_schema: SchemaDescriptor,
    pks: &[PkBuf],
) -> Option<Batch> {
    debug_assert!(
        pks.windows(2).all(|w| w[0] <= w[1]),
        "gather_source_rows requires ascending PKs for the monotone sweep"
    );
    let mut acc = Batch::with_schema(src_schema, pks.len());
    for pk in pks {
        // PKs are asserted ascending above, so the galloping `advance_to`
        // seeds each probe at the prior position — one monotone forward sweep.
        if src_cursor.advance_to_exact_live(pk.pk_bytes()) {
            let w = src_cursor.current_weight;
            src_cursor.copy_current_row_into(&mut acc, w);
        }
    }
    (acc.count > 0).then_some(acc)
}

/// A chunked walk of one secondary-index key range, gathering each in-range live
/// entry's source row from the base table. One unchunked drain
/// (`drain_chunk(usize::MAX)`) is the wire range-seek; the chunked drive is the
/// bounded backfill scan — one mechanism under both.
///
/// Holds the index cursor and the base cursor **at once**, and neither is
/// re-opened between chunks: `open_cursor` returns an `Rc` snapshot of the
/// store's runs, and the ad-hoc transient drive runs concurrently with live
/// traffic, so re-opening per chunk would give successive chunks different
/// snapshots — a torn read no full scan can produce. Holding both is safe: a
/// `ReadCursor` owns `Rc<MappedShard>` / `Rc<Batch>`, which pins the mmap rather
/// than the file, and memtable runs are never mutated in place.
pub(crate) struct BoundedIndexCursor {
    idx: ReadCursor,
    src: ReadCursor,
    end: Option<PkBuf>,
    done: bool,
    pks: Vec<PkBuf>,
    idx_key_size: usize,
    src_schema: SchemaDescriptor,
}

impl BoundedIndexCursor {
    /// Position `idx` at `start` and wrap both cursors for the walk over the
    /// half-open range `[start, end)` (`end = None` ⇒ to the end of the index).
    /// `idx_key_size` is the index's leading-key byte length — the source-PK OPK
    /// suffix starts there. `pk_capacity` pre-sizes the per-chunk PK scratch
    /// (pass the measured range size capped at the chunk size, or 0 to grow).
    pub(crate) fn new(
        mut idx: ReadCursor,
        src: ReadCursor,
        start: &PkBuf,
        end: Option<PkBuf>,
        idx_key_size: usize,
        src_schema: SchemaDescriptor,
        pk_capacity: usize,
    ) -> Self {
        idx.seek_bytes(start.pk_bytes());
        BoundedIndexCursor {
            idx,
            src,
            end,
            done: false,
            pks: Vec::with_capacity(pk_capacity),
            idx_key_size,
            src_schema,
        }
    }

    /// The next up-to-`n` in-range rows, or `None` when the range is exhausted.
    /// A returned batch may be EMPTY (in-range index entries whose base rows are
    /// absent/retracted) — `None` strictly means "no further chunk exists".
    pub(crate) fn drain_chunk(&mut self, n: usize) -> Option<Batch> {
        self.pks.clear();
        while !self.done && self.idx.valid && self.pks.len() < n {
            let cur = self.idx.current_pk_bytes();
            if self.end.as_ref().is_some_and(|e| cur >= e.pk_bytes()) {
                self.done = true;
                break;
            }
            // The gate is `> 0` on the CONSOLIDATED merge group, not a per-entry
            // presence test: an UPDATE of an indexed column retracts the old index
            // entry and inserts the new one, so a range spanning both values sees
            // the old key at net weight 0 and collects the source PK exactly once.
            if self.idx.current_weight > 0 {
                let src_pk_stride = self.src_schema.pk_stride() as usize;
                self.pks.push(PkBuf::from_bytes(
                    &cur[self.idx_key_size..self.idx_key_size + src_pk_stride],
                ));
            }
            self.idx.advance();
        }
        // The walk's verdict, never the gather's: the loop exits with `pks` empty
        // only when the index cursor is past `end` or invalid (every other
        // iteration either pushes or steps over a non-positive entry). Monotone —
        // `done` never clears — so a `None` is final.
        if self.pks.is_empty() {
            return None;
        }
        // A range spans many duplicate groups, so collected PKs interleave across
        // the base; the gather requires ascending order for its monotone sweep.
        // No re-seek between chunks: chunk N+1's first PK may sort below chunk N's
        // last, and `advance_to` is backward-capable via a binary search, so a
        // chunk boundary costs O(log N) on the first probe — not a rescan.
        // The dedup is hardening only: distinct live index keys map to distinct
        // source PKs (a base row carries one indexed value), so duplicates exist
        // only under index corruption — where they would silently double the
        // row's weight downstream.
        self.pks.sort_unstable();
        self.pks.dedup();
        // `Some(empty)`, not `None`: the gather returns `None` when nothing
        // resolves. Both backfill drivers read `None` as exhaustion, so letting
        // that escape would silently truncate the view mid-range.
        let mut batch = gather_source_rows(&mut self.src, self.src_schema, &self.pks)
            .unwrap_or_else(|| Batch::empty_with_schema(&self.src_schema));
        // Consolidated by construction — ascending unique PKs, one live row per
        // PK, nonzero net weights — so certify it (debug-verified) and spare the
        // ingest tail its O(chunk log chunk) re-sort, matching what the
        // full-scan drain path certifies.
        batch.certify_layout(Layout::Consolidated, &self.src_schema);
        Some(batch)
    }
}
