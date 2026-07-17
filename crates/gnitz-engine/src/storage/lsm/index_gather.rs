//! Index-indirected row gather: walk one cursor's key range, resolve each
//! collected source PK against a second cursor. The storage-level mechanism
//! under both the wire range-seek and the bounded backfill scan — all policy
//! (which index, whether to bound at all, selectivity gating) stays with the
//! caller.

use super::batch::{Batch, Layout};
use super::read_cursor::ReadCursor;
use crate::schema::key::PkBuf;
use crate::schema::{IndexKeySpec, RowView, SchemaDescriptor, MAX_PK_BYTES};

/// Resolve already-collected source PKs against a base-table cursor into a
/// result batch — every live row whose index entry key `[span ‖ src_pk]` lies in
/// the half-open `[lo, hi)`, at its net `current_weight` (never a hardcoded 1, so
/// Z-Set multiplicity is preserved) — or `None` when nothing resolves.
///
/// `lo`/`hi` are the byte bounds of exactly the entries whose PKs were collected
/// (the caller's per-chunk window), over the identical key the write path built
/// (`IndexKeySpec::write_entry`). Resolving by PK alone would be wrong on a
/// non-`unique_pk` table, where one PK can carry several live payloads with
/// different indexed values: an entry denotes (span, PK), not PK. The window
/// filter also makes a *chunked* walk exact — a PK group whose entries straddle
/// a chunk boundary is gathered by both chunks, but each row passes the filter
/// only in the chunk whose window holds its entry. `hi == None` is `+∞`.
/// `lo.len()` IS the index PK stride — every cut key is built at exactly that
/// width (`index_range_keys`).
///
/// `spec` must be **full-arity** over the index's columns. A prefix-arity spec
/// would only null-check its leading columns, admitting rows the index omits: the
/// write path skips a row with a NULL in ANY indexed column, so the gather must
/// apply that same all-column gate and let `lo`/`hi` do the prefix bounding.
///
/// The cursor is caller-held so a chunked walk resolves every chunk against ONE
/// base snapshot (a per-chunk re-open would give successive chunks different
/// snapshots: a torn read no full scan can produce).
///
/// `pks` must be **ascending** and **deduplicated** (each PK group is walked
/// once). Ascending order makes the probes one monotone forward sweep that keeps
/// shard pages and merge state hot; the PKs are index entries' source-PK OPK
/// suffixes, whose memcmp order equals base storage order, so a byte sort *is*
/// the seek order. Ascending **within** a chunk only — chunk N+1's first PK may
/// sort below chunk N's last, which is why the sweep must stay backward-capable.
fn gather_source_rows(
    src_cursor: &mut ReadCursor,
    src_schema: SchemaDescriptor,
    pks: &[PkBuf],
    spec: &IndexKeySpec,
    lo: &[u8],
    hi: Option<&[u8]>,
) -> Option<Batch> {
    debug_assert!(
        pks.windows(2).all(|w| w[0] < w[1]),
        "gather_source_rows requires strictly ascending (sorted + deduped) PKs"
    );
    // Phase 1 — candidates: every live row of each PK group. `pks.len()` sizes the
    // common (`unique_pk`) case exactly; a multi-payload PK grows the batch.
    let mut cand = Batch::with_schema(src_schema, pks.len());
    for pk in pks {
        // A `false` return means no base row at or past `pk` exists. `pks`
        // ascends within the chunk, so every remaining probe is larger and
        // equally absent — this break is per-chunk and correct; the next chunk's
        // (possibly backward) first probe re-enters through the seek inside
        // `gather_pk_group`.
        if !src_cursor.gather_pk_group(pk.pk_bytes(), |c| {
            if c.current_weight > 0 {
                c.copy_current_row_into(&mut cand, c.current_weight);
            }
        }) {
            break;
        }
    }
    if cand.count == 0 {
        return None;
    }
    // Phase 2 — drop every candidate outside `[lo, hi)`, or that the index omits
    // (NULL in an indexed column).
    retain_in_index_range(cand, src_schema, spec, lo, hi)
}

/// Whether `row` of `mb` is denoted by an index entry in `[lo, hi)`. `key` is
/// scratch for the row's entry key, rebuilt through the write path's own
/// `IndexKeySpec::write_entry` (`false` = not indexed: a NULL in any indexed
/// column).
#[inline]
fn row_in_index_range<'b>(
    mb: &impl RowView<'b>,
    row: usize,
    spec: &IndexKeySpec,
    idx_stride: usize,
    key: &mut [u8; MAX_PK_BYTES],
    lo: &[u8],
    hi: Option<&[u8]>,
) -> bool {
    if !spec.write_entry(mb, row, key) {
        return false;
    }
    let k = &key[..idx_stride];
    k >= lo && hi.is_none_or(|h| k < h)
}

/// Keep the rows of `cand` whose index entry key lies in `[lo, hi)`. One
/// contiguous pass over the flat batch; on a miss, one `scatter_copy`
/// (`Batch::from_indexed_rows`).
///
/// On a `unique_pk` table every candidate matches (the row's own span produced
/// the entry the walk yielded), so the scan finds no failing row and `cand` is
/// returned verbatim — no copy, and no allocation at all. Only a real miss (a
/// multi-payload PK group, or a NULL-gated row on a prefix seek) pays for one.
fn retain_in_index_range(
    cand: Batch,
    src_schema: SchemaDescriptor,
    spec: &IndexKeySpec,
    lo: &[u8],
    hi: Option<&[u8]>,
) -> Option<Batch> {
    let idx_stride = lo.len();
    // Guards the full-arity precondition: with a prefix spec the suffix copy in
    // `write_entry` would land inside the uncovered columns' bytes.
    debug_assert_eq!(spec.key_size() + src_schema.pk_stride() as usize, idx_stride);
    debug_assert!(hi.is_none_or(|h| h.len() == idx_stride));
    // MAX_PK_BYTES (80) bounds every index schema's pk_stride (asserted in
    // `SchemaDescriptor::new`), so the key scratch is a stack array.
    let mut key = [0u8; MAX_PK_BYTES];

    let mb = cand.as_mem_batch();
    let first_fail = (0..cand.count).position(|r| !row_in_index_range(&mb, r, spec, idx_stride, &mut key, lo, hi));
    let Some(first_fail) = first_fail else {
        return Some(cand);
    };
    // Rows below `first_fail` already passed — range-extend, never re-test.
    let mut keep: Vec<u32> = (0..first_fail as u32).collect();
    keep.extend(
        (first_fail + 1..cand.count)
            .filter(|&r| row_in_index_range(&mb, r, spec, idx_stride, &mut key, lo, hi))
            .map(|r| r as u32),
    );
    if keep.is_empty() {
        return None;
    }
    Some(Batch::from_indexed_rows(&mb, &keep, &src_schema))
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
    /// The current chunk window's inclusive lower bound — the walk's `start` at
    /// first, then each chunk's exclusive upper bound in turn.
    start: PkBuf,
    end: Option<PkBuf>,
    done: bool,
    pks: Vec<PkBuf>,
    spec: IndexKeySpec,
    src_schema: SchemaDescriptor,
}

impl BoundedIndexCursor {
    /// Position `idx` at `start` and wrap both cursors for the walk over the
    /// half-open range `[start, end)` (`end = None` ⇒ to the end of the index).
    /// `spec` is the index's full-arity key spec — its `key_size()` is the
    /// leading-key byte length (where the source-PK OPK suffix starts), and it
    /// re-projects each candidate row's entry key for the per-chunk window
    /// filter. `pk_capacity` pre-sizes the per-chunk PK scratch (pass the
    /// measured range size capped at the chunk size, or 0 to grow).
    pub(crate) fn new(
        mut idx: ReadCursor,
        src: ReadCursor,
        start: PkBuf,
        end: Option<PkBuf>,
        spec: IndexKeySpec,
        src_schema: SchemaDescriptor,
        pk_capacity: usize,
    ) -> Self {
        idx.seek_bytes(start.pk_bytes());
        BoundedIndexCursor {
            idx,
            src,
            start,
            end,
            done: false,
            pks: Vec::with_capacity(pk_capacity),
            spec,
            src_schema,
        }
    }

    /// The next up-to-`n` in-range rows, or `None` when the range is exhausted.
    /// A returned batch may be EMPTY (in-range index entries whose base rows are
    /// absent/retracted) — `None` strictly means "no further chunk exists".
    pub(crate) fn drain_chunk(&mut self, n: usize) -> Option<Batch> {
        self.pks.clear();
        let idx_key_size = self.spec.key_size();
        let src_pk_stride = self.src_schema.pk_stride() as usize;
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
                self.pks
                    .push(PkBuf::from_bytes(&cur[idx_key_size..idx_key_size + src_pk_stride]));
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
        // This chunk's exclusive upper bound: the first UNCOLLECTED entry (the
        // index cursor's resting key), clamped to `end`. The gather emits every
        // live row of each collected PK group, so on a source where one PK owns
        // several live entries a group can straddle a chunk boundary; windowing
        // each chunk to exactly the entries it collected makes each row pass the
        // filter in the one chunk that collected its entry — chunked and
        // unchunked drains yield identical multisets by construction.
        let hi: Option<PkBuf> = if self.idx.valid
            && self
                .end
                .as_ref()
                .is_none_or(|e| self.idx.current_pk_bytes() < e.pk_bytes())
        {
            Some(PkBuf::from_bytes(self.idx.current_pk_bytes()))
        } else {
            self.end
        };
        // A range spans many duplicate groups, so collected PKs interleave across
        // the base; the gather requires ascending order for its monotone sweep.
        // No re-seek between chunks: chunk N+1's first PK may sort below chunk N's
        // last, and `advance_to` is backward-capable via a binary search, so a
        // chunk boundary costs O(log N) on the first probe — not a rescan.
        // The dedup is load-bearing: on a non-`unique_pk` table two live rows at
        // one PK with different indexed values produce two entries sharing that
        // PK suffix, so the collected PKs repeat — and the gather walks each PK
        // group exactly once.
        self.pks.sort_unstable();
        self.pks.dedup();
        // `Some(empty)`, not `None`: the gather returns `None` when nothing
        // resolves. Both backfill drivers read `None` as exhaustion, so letting
        // that escape would silently truncate the view mid-range.
        let mut batch = gather_source_rows(
            &mut self.src,
            self.src_schema,
            &self.pks,
            &self.spec,
            self.start.pk_bytes(),
            hi.as_ref().map(|e| e.pk_bytes()),
        )
        .unwrap_or_else(|| Batch::empty_with_schema(&self.src_schema));
        // The next chunk's window begins where this one ended. (`None` = `+∞`:
        // the walk is over and the next `drain_chunk` returns `None` above.)
        if let Some(h) = hi {
            self.start = h;
        }
        // Consolidated by construction — rows are emitted in (PK, payload) order
        // (the group walk is at (PK, payload)-sub-group granularity, over
        // strictly-ascending PKs) with nonzero net weights, and the window filter
        // preserves order — so certify it (debug-verified) and spare the ingest
        // tail its O(chunk log chunk) re-sort, matching what the full-scan drain
        // path certifies. `debug_verify_consolidated` checks strictly-increasing
        // (PK, payload) and nonzero weight, never PK uniqueness, so two rows
        // sharing a PK with ascending payloads certify cleanly.
        batch.certify_layout(Layout::Consolidated, &self.src_schema);
        Some(batch)
    }
}
