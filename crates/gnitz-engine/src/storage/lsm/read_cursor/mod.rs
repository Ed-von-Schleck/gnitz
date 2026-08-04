//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped).

use std::cell::OnceCell;
use std::cmp::Ordering;
use std::ptr;
#[cfg(test)]
use std::rc::Rc;

use super::batch::Batch;
use super::columnar::with_payload_cmp;
use super::columnar::ColumnarSource;
use super::heap::{drive_merge, HeapNode, LoserTree};
use super::merge::{self, UnifiedSource};
#[cfg(test)]
use super::shard_reader::MappedShard;
use crate::schema::key::{compare_pk_ordering, pk_bytes_eq};
use crate::schema::SchemaDescriptor;

mod output;

use super::run::Run;
use gnitz_expr::RowSource;
pub(crate) use output::DrainGuard;

#[cfg(test)]
thread_local! {
    pub(crate) static REWIND_CALLS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Default `drain_chunk` size for DDL scans (index/view backfill, unique
/// pre-flight). Bounds peak backfill memory at O(chunk × row_width); also the
/// default for `CatalogEngine::ddl_scan_chunk_rows`, which tests shrink to
/// exercise chunk boundaries.
pub(crate) const DDL_SCAN_CHUNK_ROWS: usize = 65_536;

// ---------------------------------------------------------------------------
// CursorState — per-source position tracker (struct-of-arrays pair to
// `sources`).  Splitting source from state lets the borrow checker see
// `&self.sources` (immutable) and `&mut self.states` (mutable) as disjoint
// fields, so the merge driver can hold both at once without dance.
// ---------------------------------------------------------------------------

struct CursorState {
    position: usize,
    /// The source's row count, or the upper bound a range seek clamped it to
    /// (`seek_range_bytes`). Cached here so validity and the range clamp read
    /// `&[CursorState]` alone, without a parallel borrow of `&[Run]`.
    count: usize,
}

impl CursorState {
    #[inline]
    fn is_valid(&self) -> bool {
        self.position < self.count
    }

    fn advance(&mut self) {
        if self.is_valid() {
            self.position += 1;
        }
    }

    /// Seek to the first row whose OPK bytes are `>= key`. `key` must be exactly
    /// `pk_stride` OPK bytes.
    fn seek_bytes(&mut self, src: &Run, key: &[u8]) {
        self.position = src.find_lower_bound_bytes(key);
    }

    /// Galloping forward seek to the first row whose OPK bytes are `>= key`,
    /// seeded at the live `position`. Forward-only and position-owned, so a
    /// stale or non-monotone hint is unrepresentable — equals `seek_bytes`'s
    /// landing index for any key, only cheaper when the boundary moves forward.
    fn advance_to(&mut self, src: &Run, key: &[u8]) {
        self.position = src.advance_to(key, self.position);
    }
}

// ---------------------------------------------------------------------------
// ReadCursor
// ---------------------------------------------------------------------------

/// Dispatch on the source set that was live at the last absolute reposition
/// (`mode_for`), replacing the previous `tree: Option<LoserTree>` + parallel
/// `entries.len() == 1` checks. The variants are exhaustive so each call site
/// dispatches once, and the ≤2-live variants carry the indices they drive — so a
/// bypass applies whether or not the live sources are the leading ones. `Multi`
/// carries no indices: dead sources still enter the tree, as sentinel leaves.
///
/// Forward progress does not re-derive the mode, so it can be pessimistic (a
/// `Multi` whose sources have since exhausted down to one). That direction is
/// always safe — `drive_pair_inner`'s validity checks and the tree's sentinels
/// handle an exhausted member — so mode and liveness agreeing is not an invariant.
///
/// `Pair` (exactly 2 live sources) bypasses the loser tree with a
/// one-compare-per-row 2-head merge read straight from their two states — the
/// frequent DBSP shape (a delta against a well-compacted trace). It drops the
/// tree's per-emit indirection and bounds checks, not comparisons: at k=2 the tree
/// already does one compare per `replace_top` (`shard_merge_scan_bench` sweeps k).
/// `Pair` keeps no separate head state, so the seek/rewind paths re-drive it with
/// no special handling.
enum SourceMode {
    Empty,
    Single(usize),
    Pair(usize, usize),
    Multi(LoserTree),
}

pub struct ReadCursor {
    sources: Vec<Run>,
    states: Vec<CursorState>,
    /// Many cursor consumers (point lookups, seeks) never call
    /// `scatter_drained_into`; build on first use.
    unified_sources: OnceCell<Vec<UnifiedSource>>,
    mode: SourceMode,
    schema: SchemaDescriptor,
    // Current row state
    pub valid: bool,
    pub current_weight: i64,
    pub current_null_word: u64,
    current_entry_idx: usize,
    current_row: usize,
}

/// The comparator the `_with` variants are monomorphized over — the same
/// [`merge::RowComparator`] the flush/compaction kernel uses, at this cursor's
/// source type.
trait RowComparator: merge::RowComparator<Run> {}
impl<F: merge::RowComparator<Run>> RowComparator for F {}

impl ReadCursor {
    /// Build a ReadCursor from owned in-memory batches (no shards) — a test-only
    /// helper for reading a batch directly without a backing `Table` (no scratch
    /// dir / `mkdir`).
    #[cfg(test)]
    pub(crate) fn from_owned(snapshots: &[Rc<Batch>], schema: SchemaDescriptor) -> ReadCursor {
        create_read_cursor(snapshots, &[], schema)
    }

    /// Builds the loser tree under the selected `row_cmp` (the `with_payload_cmp!`
    /// layer hands it a monomorphized comparator). Keyless leaf: it carries only
    /// the row index; the comparator reads each player's OPK bytes through
    /// `(source_idx, row)` — `compare_pk_ordering`, then the payload tiebreak. That
    /// is [`merge::merge_less`], the same order the flush/compaction kernel merges
    /// under, and the one the drive and forward-seek paths key the heap through, so
    /// a tree maintained in place can never order rows differently from how it was
    /// built.
    #[inline]
    fn build_tree_with<RowCmp: RowComparator>(
        sources: &[Run],
        states: &[CursorState],
        schema: &SchemaDescriptor,
        row_cmp: RowCmp,
    ) -> LoserTree {
        let init = |i: usize| states[i].is_valid().then(|| states[i].position as u32);
        LoserTree::build(sources.len(), init, merge::merge_less(schema, sources, row_cmp))
    }

    fn build_tree(sources: &[Run], states: &[CursorState], schema: &SchemaDescriptor) -> LoserTree {
        // `with_payload_cmp!` selects the payload comparator; the PK axis is
        // `compare_pk_ordering` (no stride dispatch).
        with_payload_cmp!(schema, Self::build_tree_with, sources, states, schema)
    }

    /// The drive mode for the currently live sources: a source whose
    /// `[position, count)` window is empty cannot contribute a row, so with one or
    /// two live it takes a bypass instead of a tree. Counts at most three — that is
    /// all the dispatch distinguishes.
    ///
    /// Emptiness is not permanent, which is why the mode is *derived* at every
    /// absolute reposition rather than the source set being narrowed:
    /// `CursorState::advance_to` searches its source's full row count and is
    /// backward-capable, so a later seek at a lower key can move a source that a
    /// range seek emptied back inside its clamped window. Dropping the source
    /// would discard rows it can still contribute.
    fn mode_for(sources: &[Run], states: &[CursorState], schema: &SchemaDescriptor) -> SourceMode {
        let mut live = states
            .iter()
            .enumerate()
            .filter_map(|(i, st)| st.is_valid().then_some(i));
        match (live.next(), live.next(), live.next()) {
            (None, _, _) => SourceMode::Empty,
            (Some(a), None, _) => SourceMode::Single(a),
            (Some(a), Some(b), None) => SourceMode::Pair(a, b),
            _ => SourceMode::Multi(Self::build_tree(sources, states, schema)),
        }
    }

    fn new(sources: Vec<Run>, states: Vec<CursorState>, schema: SchemaDescriptor) -> Self {
        debug_assert_eq!(sources.len(), states.len());
        let mode = Self::mode_for(&sources, &states, &schema);
        let mut cursor = ReadCursor {
            sources,
            states,
            unified_sources: OnceCell::new(),
            mode,
            schema,
            valid: false,
            current_weight: 0,
            current_null_word: 0,
            current_entry_idx: 0,
            current_row: 0,
        };
        cursor.drive();
        cursor
    }

    /// Re-derive the drive mode after the per-source positions have been moved,
    /// then drive to the first live row. Shared tail of [`seek_bytes`],
    /// [`seek_range_bytes`], [`rewind`], and [`advance_to`]'s reposition arm:
    /// repositioning changes which sources are live (and invalidates a tree's
    /// cached head comparisons), so the mode must be recomputed before the next
    /// `drive`. Nothing is destroyed — `sources` stays intact, which is what
    /// keeps the positional `unified_sources` cache valid by construction.
    fn rebuild_and_drive(&mut self) {
        self.mode = Self::mode_for(&self.sources, &self.states, &self.schema);
        self.drive();
    }

    /// Position the cursor on the half-open OPK range `[start, end)`. Each
    /// source seeks to its lower bound of `start`, and — when `end` is given —
    /// has its row count clamped to its lower bound of `end`, so every
    /// drain/advance path simply exhausts at the cut: no per-row boundary
    /// check, no over-read past the range. One reposition + one rebuild (a
    /// separate truncate-after-seek would re-drive and lose the seeked group).
    /// The upper bound is part of the cursor's view from then on (`rewind`
    /// keeps it). Both keys must be exactly `pk_stride` OPK bytes.
    pub fn seek_range_bytes(&mut self, start: &[u8], end: Option<&[u8]>) {
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.seek_bytes(src, start);
            if let Some(end) = end {
                state.count = state.count.min(src.find_lower_bound_bytes(end));
            }
        }
        self.rebuild_and_drive();
    }

    /// Reset every source to its first row and re-drive, positioning the cursor
    /// at the first row in storage order. Use this instead of `seek(u128::MIN)`
    /// to rewind a cursor: a signed-PK trace's minimum is a negative value, not
    /// the all-zero bytes `seek(0)` lands on, so `seek(u128::MIN)` would skip
    /// every negative-keyed row. Rewinding by row index is correct at every PK
    /// shape.
    pub fn rewind(&mut self) {
        #[cfg(test)]
        REWIND_CALLS.with(|c| c.set(c.get() + 1));
        for state in self.states.iter_mut() {
            state.position = 0;
        }
        self.rebuild_and_drive();
    }

    /// Byte-addressed sibling of [`seek`]. `key` must be exactly `pk_stride`
    /// OPK bytes (the bytes from `get_pk_bytes`/`current_pk_bytes`). Correct at
    /// every PK width and signedness: the inner `drive`/`build_tree` key the
    /// heap via `compare_pk_ordering` over the full OPK bytes, so this
    /// survives `pk_stride > 16` where the u128 `seek` cannot.
    pub fn seek_bytes(&mut self, key: &[u8]) {
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.seek_bytes(src, key);
        }
        self.rebuild_and_drive();
    }

    /// Galloping forward lower-bound seek, seeding each source's search at that
    /// source's live `position` (the stream owns its hint — there is no hint
    /// parameter). Lands on the identical row `seek_bytes(key)` would
    /// (`gallop_opk` == `lower_bound_opk` for every hint),
    /// only cheaper when the boundary moves forward. Backward-capable: a
    /// non-monotone hint forfeits the speedup (the search falls back to a bounded
    /// `[0, position)` scan) but never correctness — so monotone consumers (the
    /// co-group merges, the natural-PK reduce reads) get the skip while
    /// non-monotone ones (the unordered `gather_family_bytes` resolve, the
    /// `Lt`/`Le` range reset) stay correct. `key` must be exactly `pk_stride` OPK
    /// bytes.
    ///
    /// On a live multi-source cursor a *strictly-forward* seek additionally
    /// maintains the loser tree in place (`seek_forward_multi`) instead of
    /// rebuilding it: the dominant access pattern (the co-group / range / reduce
    /// drivers all repositioning in ascending key order) thus pays one
    /// `Θ(log num_sources)` walk-up per galloped laggard and **zero** allocation,
    /// not a from-scratch `Θ(num_sources)` rebuild + its two Vec allocations on
    /// every step.
    pub fn advance_to(&mut self, key: &[u8]) {
        // Strictly-forward seek on a live multi-source cursor: maintain the loser
        // tree in place instead of rebuilding it. The boundary is strict — `key`
        // must be > the current emitted PK. At `key == current_pk` the lower bound
        // of `key` can be a row this cursor already consumed (an earlier payload at
        // the same PK), which a forward gallop cannot reach; that case — and every
        // backward / exhausted / Single / Empty case — takes the absolute
        // reposition + rebuild below. `self.valid` is checked first so
        // `current_pk_cmp_bytes` never reads an unpositioned cursor.
        if self.valid && matches!(self.mode, SourceMode::Multi(_)) && self.current_pk_cmp_bytes(key) == Ordering::Less {
            self.seek_forward_multi(key);
            return;
        }
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.advance_to(src, key);
        }
        self.rebuild_and_drive();
    }

    /// Gallop the `Multi` loser tree forward to the first head `>= key`, then
    /// drive the first live group. The gallop keys the heap through the shared
    /// [`merge::merge_less`] order, with the comparator selected by `with_payload_cmp!`.
    /// Precondition (enforced by [`advance_to`]'s dispatch):
    /// `matches!(self.mode, SourceMode::Multi)` and `key` > the current emitted PK.
    fn seek_forward_multi(&mut self, key: &[u8]) {
        with_payload_cmp!(self.schema, Self::seek_forward_multi_with, self, key);
    }

    /// Gallop the heads forward with the selected `row_cmp`, then drive. The PK
    /// axis is `compare_pk_ordering` (no stride dispatch).
    #[inline]
    fn seek_forward_multi_with<RowCmp: RowComparator>(&mut self, key: &[u8], row_cmp: RowCmp) {
        self.gallop_heap_forward(key, row_cmp);
        self.drive_with_inner(row_cmp);
    }

    /// Maintain the `Multi` loser tree in place while galloping its heads forward
    /// to `key`, keyed by the shared [`merge::merge_less`] order. Scoping
    /// the heap/sources/states borrows to this call frees `self` for the caller's
    /// following `drive`. Precondition: `matches!(self.mode, SourceMode::Multi)`.
    fn gallop_heap_forward<RowCmp: RowComparator>(&mut self, key: &[u8], row_cmp: RowCmp) {
        let heap = match &mut self.mode {
            SourceMode::Multi(h) => h,
            _ => unreachable!("gallop_heap_forward requires SourceMode::Multi"),
        };
        let less = merge::merge_less(&self.schema, &self.sources, row_cmp);
        Self::seek_phase(heap, &self.sources, &mut self.states, key, &less);
    }

    /// Advance every laggard head (OPK `< key`) to its own `lower_bound(key)`,
    /// restoring the loser tree with one `replace_top`/`pop_top` per gallop. Only
    /// the current root is ever a laggard — it is the global min, so once it is
    /// `>= key` every head is — and each gallop moves that source to `>= key`, so
    /// the loop touches at most `num_sources` leaves and always terminates. On
    /// return every head is `>= key`, leaving the tree positioned exactly as a
    /// from-scratch rebuild at `key` would; the caller's `drive` then folds the
    /// first live group. `key` is exactly `pk_stride` OPK bytes; the PK axis is
    /// `compare_pk_ordering`.
    fn seek_phase(
        heap: &mut LoserTree,
        sources: &[Run],
        states: &mut [CursorState],
        key: &[u8],
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        while !heap.is_empty() {
            let HeapNode { source_idx: src, row } = *heap.peek();
            let (src, row) = (src as usize, row as usize);
            // The root is the global min, so once its OPK bytes reach `key` every
            // head has. `compare_pk_ordering` compares the full stride bytes —
            // exact at every width. Otherwise the root lags; gallop it forward.
            if compare_pk_ordering(sources[src].get_pk_bytes(row), key) != Ordering::Less {
                break;
            }
            states[src].advance_to(&sources[src], key); // gallop this laggard
            if states[src].is_valid() {
                heap.replace_top(states[src].position as u32, less);
            } else {
                heap.pop_top(less);
            }
        }
    }

    /// Seek to `key` (exact `pk_stride` OPK bytes) and report whether it landed
    /// on a *present, live* row: positioned, PK byte-equal, and net weight > 0.
    /// The weight gate is load-bearing, not redundant — a single-source cursor
    /// (`drive_single`) does not consolidate, so it can surface a tombstone an
    /// uncompacted source still holds; every point lookup must filter it. Folds
    /// the `seek_bytes` + valid/exact-PK/positive-weight predicate the system-table
    /// point lookups (liveness probe, single-row retract, the unique-upsert probe)
    /// would otherwise open-code.
    pub fn seek_exact_live(&mut self, key: &[u8]) -> bool {
        self.seek_bytes(key);
        self.valid && self.current_pk_eq(key) && self.current_weight > 0
    }

    /// Galloping forward sibling of [`seek_exact_live`] for a probe **sequence**
    /// sharing one cursor: `advance_to(key)` then the same
    /// present/exact-PK/positive-weight gate. Each source's search is seeded at
    /// its live position, so an ascending run of keys is one monotone forward
    /// sweep that keeps shard pages and merge state hot; an unordered run still
    /// beats re-seeking from scratch via the bounded `[0, position)` backward
    /// branch. Correct for any probe order — `advance_to` is backward-capable —
    /// so an out-of-order key forfeits only the speedup. `key` must be exactly
    /// `pk_stride` OPK bytes. A lone random point lookup can keep
    /// `seek_exact_live`.
    pub fn advance_to_exact_live(&mut self, key: &[u8]) -> bool {
        self.advance_to(key);
        self.valid && self.current_pk_eq(key) && self.current_weight > 0
    }

    /// PK region of the current row as raw bytes, without copying. The single PK
    /// accessor for any width — correct for compound/wide PKs.
    pub fn current_pk_bytes(&self) -> &[u8] {
        self.sources[self.current_entry_idx].get_pk_bytes(self.current_row)
    }

    /// The current row as a `(source, row)` pair for the shared [`RowSource`]
    /// kernels (`compare_rows`, group-key extraction, row copies). Resolves the
    /// (entry, row) pair once per row; the returned source is the current row's
    /// **own** entry, so its blob arena backs the row's German strings. The
    /// opaque `impl RowSource` keeps `Run` private to lsm; every use is
    /// monomorphic. The bound is `RowSource`, not `ColumnarSource`: a cursor's
    /// weight comes off the cursor (`current_weight`), never off the positioned
    /// source. Callers must gate on `valid` first.
    #[inline]
    pub(crate) fn current_row_source(&self) -> (&impl RowSource, usize) {
        debug_assert!(self.valid, "current_row_source on an invalid cursor");
        (&self.sources[self.current_entry_idx], self.current_row)
    }

    /// The current row's PK as its native scalar value (right-aligned BE via
    /// `widen_pk_be`). Only narrow (`pk_stride ≤ 16`) relations have a scalar PK;
    /// the catalog sys-table readers (all narrow) call this. Decoded on demand
    /// from the current row's OPK bytes — the cursor caches no scalar key. Gate on
    /// `valid` first.
    #[inline]
    pub fn current_key_narrow(&self) -> u128 {
        debug_assert!(self.valid, "current_key_narrow on an invalid cursor");
        let bytes = self.current_pk_bytes();
        assert!(bytes.len() <= 16, "narrow PK cursor");
        gnitz_wire::widen_pk_be(bytes, bytes.len())
    }

    /// Whether the current row's PK equals the group's OPK `key_bytes`. Callers
    /// must gate on `valid` first — this reads the current row's bytes, which is
    /// undefined on an unpositioned cursor.
    #[inline]
    pub fn current_pk_eq(&self, key_bytes: &[u8]) -> bool {
        pk_bytes_eq(self.current_pk_bytes(), key_bytes)
    }

    /// Walk the equal-`key` PK group, invoking `f(&*self)` at each emitted row so
    /// the callback can read the current trace row's columns / weight (e.g.
    /// `write_join_row`, `push_current_row`). Commits `current_*` per row (the
    /// callback reads through it), but hoists the `with_payload_cmp!` dispatch out
    /// of the loop. `f` must not re-enter the cursor. On return the cursor sits at the
    /// first row past the group (or `valid == false` at end of source) with every
    /// `current_*` field committed, at `(PK, payload)`-sub-group granularity — the
    /// same group-walk contract as the `while current_pk_eq { advance }` loops it
    /// replaces.
    pub(crate) fn for_each_pk_group_row<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], f: F) {
        with_payload_cmp!(self.schema, Self::for_each_pk_group_row_with, self, key, f);
    }

    /// Walks the equal-PK group, invoking `f` at each emitted row. The monomorphized
    /// `row_cmp` is threaded through each `advance_with`, so the per-row advance never
    /// re-selects the comparator.
    #[inline]
    fn for_each_pk_group_row_with<RowCmp: RowComparator, F: FnMut(&ReadCursor)>(
        &mut self,
        key: &[u8],
        mut f: F,
        row_cmp: RowCmp,
    ) {
        while self.valid && self.current_pk_eq(key) {
            f(&*self);
            self.advance_with(row_cmp);
        }
    }

    /// Group-walking sibling of [`Self::advance_to_exact_live`]: position on
    /// `key`'s PK group — seeking only when not already sitting on it — and
    /// append every **live** row of it to `out`. An absent key appends nothing.
    /// The single keyed-read primitive for a store whose PK need not be unique:
    /// a base table's PK names one row, but a view output store runs no
    /// `enforce_unique_pk` and its synthetic key (`_join_pk`) names one row per
    /// row the join produced for it.
    ///
    /// The `current_weight > 0` gate is per row, not a presence test for the key:
    /// a retracted member an uncompacted source still holds sorts within its PK
    /// by payload, so it can head the group, and rejecting the key on it would
    /// drop the live rows behind it.
    ///
    /// Skipping the seek at `Equal` is sound because a cursor driven through
    /// seeks and this method always rests on a group's FIRST unconsumed row:
    /// `new`/`advance_to` drive to a group head, and the group walk consumes
    /// whole groups, stopping on the next group's first row. `Equal` is also
    /// exactly the case `advance_to` handles worst — it declines the in-place
    /// forward gallop (strictly `Less` only) and takes the absolute reposition
    /// plus tree rebuild — so the skip removes the most expensive seek shape
    /// entirely. `Greater` (a backward probe) does NOT mean the group is
    /// absent, only that an earlier consumer left the cursor ahead: it seeks.
    pub(crate) fn copy_live_pk_group_into(&mut self, key: &[u8], out: &mut Batch) {
        if !self.valid || self.current_pk_cmp_bytes(key) != Ordering::Equal {
            self.advance_to(key);
        }
        self.for_each_pk_group_row(key, |c| {
            if c.current_weight > 0 {
                c.copy_current_row_into(out, c.current_weight);
            }
        });
    }

    /// Storage-order comparison of the current row's PK against an OPK `key`
    /// (already-encoded bytes). Universal byte path; correct at every PK width.
    /// Callers must gate on `valid` first.
    #[inline]
    pub fn current_pk_cmp_bytes(&self, key: &[u8]) -> Ordering {
        self.current_pk_bytes().cmp(key)
    }

    /// Position the cursor at the first row whose PK begins with `prefix` and
    /// whose weight is `> 0`. Returns `true` on a hit (cursor stays positioned;
    /// `current_pk_bytes`, `current_weight` etc. are valid). Returns `false`
    /// on miss (cursor may be past the prefix range or fully invalid).
    ///
    /// Centralises the seek-pad-walk dance used by the compound-PK secondary
    /// index: zero-pads `prefix` to `pk_stride` for `seek_bytes`, then walks
    /// forward until the prefix terminates or a positive-weight match appears.
    /// `prefix` is the OPK image of the leading PK column(s).
    pub fn seek_first_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
        let stride = self.schema.pk_stride() as usize;
        let mut key = [0u8; crate::schema::MAX_PK_BYTES];
        let copy_len = prefix.len().min(stride);
        key[..copy_len].copy_from_slice(&prefix[..copy_len]);
        // Suffix stays zero: 0x00 is the OPK minimum for every PK type (signed
        // MIN maps to all-zeros after the sign flip), so the zero-padded key is
        // the correct lower bound for the suffix columns at every type.
        self.seek_bytes(&key[..stride]);
        self.walk_to_positive_with_prefix(prefix)
    }

    /// Visit every positive-weight row whose PK begins with `prefix`, invoking
    /// `f(&*self)` at each (the callback reads the committed `current_*` row
    /// state; it must not re-enter the cursor). The seek/advance/walk loop the
    /// system-table readers (circuit load, view-row retraction) share.
    pub fn for_each_positive_with_prefix<F: FnMut(&ReadCursor)>(&mut self, prefix: &[u8], mut f: F) {
        let mut hit = self.seek_first_positive_with_prefix(prefix);
        while hit {
            f(&*self);
            self.advance();
            hit = self.walk_to_positive_with_prefix(prefix);
        }
    }

    /// Walk forward from the current position (no seek) to the next row whose
    /// PK begins with `prefix` and whose weight is `> 0`. Returns `false` once
    /// the prefix range ends or the cursor exhausts. The seek-less companion to
    /// [`seek_first_positive_with_prefix`]: callers that already consumed one
    /// match `advance()` past it then call this to find the next, instead of
    /// re-seeking (which would re-find the same entry and spin forever).
    pub fn walk_to_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
        while self.valid {
            if !self.current_pk_bytes().starts_with(prefix) {
                return false;
            }
            if self.current_weight > 0 {
                return true;
            }
            self.advance();
        }
        false
    }

    pub fn advance(&mut self) {
        if !self.valid {
            return;
        }
        self.pre_step_single();
        self.drive();
    }

    /// Step past the previously-emitted row before a re-drive. Only `Single`
    /// pre-steps: its `drive` just commits the row at the current position, so we
    /// must advance off it first. `Empty`/`Pair`/`Multi` do not — their drives
    /// already consumed the emitted group (the `Pair` fold and the heap fold both
    /// advance every head past it), so the next drive opens a fresh group.
    #[inline]
    fn pre_step_single(&mut self) {
        if let SourceMode::Single(i) = self.mode {
            self.states[i].advance();
        }
    }

    #[inline]
    fn advance_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        self.pre_step_single();
        self.drive_with(row_cmp);
    }

    #[inline]
    fn drive(&mut self) {
        with_payload_cmp!(self.schema, Self::drive_with, self);
    }

    /// Commit (or invalidate from) the `(net_weight, source_idx, row)` a drive
    /// emitted (the `Single` bypass and every `Multi`/`Pair` driver route here).
    /// The PK is tracked as `(current_entry_idx, current_row)`; `current_pk_bytes`
    /// / `current_key_narrow` decode it on demand.
    #[inline]
    fn commit_emitted(&mut self, emitted: Option<(i64, usize, usize)>) {
        if let Some((weight, idx, row)) = emitted {
            self.valid = true;
            self.current_weight = weight;
            self.current_entry_idx = idx;
            self.current_row = row;
            self.current_null_word = self.sources[idx].get_null_word(row);
        } else {
            self.valid = false;
        }
    }

    /// Single-live-source bypass: no heap, no ghost filter. Every production
    /// `Batch` source is a run set's run (`RunSet::push` debug-asserts each one
    /// consolidated) and shards are ghost-free by construction, so the state's
    /// current position is either valid emit-ready or past-end. The other
    /// sources — if any — have an empty `[position, count)` window and cannot
    /// contribute a row to fold against. Routes through `commit_emitted` so
    /// every mode materializes `current_*` in one place.
    #[inline]
    fn drive_single(&mut self, i: usize) {
        let s = &self.states[i];
        let emitted = s
            .is_valid()
            .then(|| (self.sources[i].get_weight(s.position), i, s.position));
        self.commit_emitted(emitted);
    }

    /// Shared N-way merge driver body for the `Multi` drives. Mirrors storage's
    /// `run_merge`: the caller selects `less` / `same_pk` / `eq_payload`
    /// (per stride and payload), and monomorphization compiles each combination to
    /// its own branch-free copy of the (selector-independent) advance/weight/emit
    /// hot loop. Returns the emitted group as `(weight, source_idx, row)`, or
    /// `None` if drained.
    #[inline]
    fn drive_inner<L, SP, EQ>(
        heap: &mut LoserTree,
        sources: &[Run],
        states: &mut [CursorState],
        less: L,
        same_pk: SP,
        eq_payload: EQ,
    ) -> Option<(i64, usize, usize)>
    where
        L: Fn(&HeapNode, &HeapNode) -> bool + Copy,
        SP: Fn(usize, usize, usize, usize) -> bool,
        EQ: Fn(usize, usize, usize, usize) -> bool,
    {
        let mut emitted: Option<(i64, usize, usize)> = None;
        drive_merge(
            heap,
            less,
            |src| {
                states[src].advance();
                states[src].is_valid().then(|| states[src].position as u32)
            },
            same_pk,
            eq_payload,
            |src, row| sources[src].get_weight(row),
            |gs, gr, nw| {
                emitted = Some((nw, gs, gr));
                std::ops::ControlFlow::Break(())
            },
        );
        emitted
    }

    /// Drive to the next group with the selected `row_cmp`. The single mode
    /// dispatch: Empty invalidates, Single/Pair take their bypass, and `Multi`
    /// drives the loser tree via `drive_with_inner`. The PK axis is
    /// `compare_pk_ordering` (no stride dispatch).
    ///
    /// On the heap path `drive_merge` folds tied rows for us; we hand it `Break`
    /// from `emit` the first time we see a non-ghost group to return immediately.
    /// Ghost groups (net weight = 0) cause `drive_merge` to skip emit and open the
    /// next group, so the loop naturally walks past them.
    #[inline]
    fn drive_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        match self.mode {
            SourceMode::Empty => self.valid = false,
            SourceMode::Single(i) => self.drive_single(i),
            SourceMode::Pair(a, b) => self.drive_pair_inner(a, b, row_cmp),
            SourceMode::Multi(_) => self.drive_with_inner(row_cmp),
        }
    }

    /// `Multi` drive, monomorphized on payload (`row_cmp`).
    /// Precondition: `matches!(self.mode, SourceMode::Multi)`.
    #[inline]
    fn drive_with_inner<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        let heap = match &mut self.mode {
            SourceMode::Multi(h) => h,
            _ => unreachable!("drive_with_inner requires SourceMode::Multi"),
        };
        let schema = &self.schema;
        let sources = &self.sources;
        let states = &mut self.states;
        // `drive_inner` returns the emitted group as a tuple rather than writing
        // `self.current_*` directly: its closures already reborrow `&sources` +
        // `&mut states`, so also capturing `&mut self.current_*` would force a
        // whole-`self` borrow. `commit_emitted` writes the tuple after it returns.
        //
        // The shared merge comparator trio (`repr::merge`): `less` settles the
        // PK axis via `compare_pk_ordering` then the payload `row_cmp`;
        // `same_pk` is the width-agnostic OPK equality (so two distinct wide
        // PKs sharing a prefix never fold); `eq_payload` is payload-only (the
        // PK term is `same_pk`).
        let less = merge::merge_less(schema, sources, row_cmp);
        let same_pk = merge::merge_same_pk(sources);
        let eq_payload = merge::merge_eq_payload(schema, sources, row_cmp);
        let emitted = Self::drive_inner(heap, sources, states, less, same_pk, eq_payload);
        self.commit_emitted(emitted);
    }

    /// Two-head merge bypass for `SourceMode::Pair(a, b)` — the heap-free analogue
    /// of `drive_single`. Reads the two heads straight from `states[a]`/`states[b]`
    /// (no cached head positions, so the seek/rewind paths re-drive a Pair with no
    /// special handling), merges by one PK compare (`compare_pk_ordering`) plus the
    /// payload `row_cmp` on a PK tie, folds equal `(PK, payload)` across both heads,
    /// and skips ghost groups — committing the first live group's exemplar.
    /// Consumes the emitted group (advances both heads past it), so the next call
    /// opens the next group — the same postcondition `drive_with_inner`'s heap fold
    /// leaves, which is why `advance` must NOT pre-step for a Pair. `a < b`
    /// (`mode_for` scans in index order), so the exemplar of a fully-tied
    /// `(PK, payload)` group is `a`'s row. Monomorphized on payload (`row_cmp`).
    #[inline]
    fn drive_pair_inner<RowCmp: RowComparator>(&mut self, a: usize, b: usize, row_cmp: RowCmp) {
        let schema = &self.schema;
        let sources = &self.sources;
        let states = &mut self.states;

        let emitted: Option<(i64, usize, usize)> = loop {
            let v0 = states[a].is_valid();
            let v1 = states[b].is_valid();
            if !v0 && !v1 {
                break None;
            }
            // Pick the exemplar (the smaller head by (PK, payload)), capturing its
            // PK bytes from the same fetch, and note whether the OTHER head's
            // current row is also in this group. The latter is true ONLY when the
            // two heads tie on both PK and payload: a PK difference (Less/Greater)
            // or a same-PK payload difference makes the loser a strictly-later
            // group, so it cannot fold now — and the selection compare already
            // told us which, letting the common (distinct-PK) path skip the other
            // head's fold probe entirely.
            let (ex_src, ex_pk, other_in_group): (usize, &[u8], bool) = if !v1 {
                (a, sources[a].get_pk_bytes(states[a].position), false)
            } else if !v0 {
                (b, sources[b].get_pk_bytes(states[b].position), false)
            } else {
                let p0 = sources[a].get_pk_bytes(states[a].position);
                let p1 = sources[b].get_pk_bytes(states[b].position);
                match compare_pk_ordering(p0, p1) {
                    Ordering::Less => (a, p0, false),
                    Ordering::Greater => (b, p1, false),
                    Ordering::Equal => {
                        match row_cmp(schema, &sources[a], states[a].position, &sources[b], states[b].position) {
                            Ordering::Greater => (b, p1, false),
                            Ordering::Less => (a, p0, false),
                            Ordering::Equal => (a, p0, true), // identical (PK,payload): fold both
                        }
                    }
                }
            };
            let ex_row = states[ex_src].position;

            // Account the exemplar's weight and consume it WITHOUT a same-group
            // test — by construction it is the group's first row (mirrors
            // `drive_merge`'s group-open, which skips the first-row test).
            let mut net = sources[ex_src].get_weight(ex_row);
            states[ex_src].advance();

            // Fold one head's run into `net`: accumulate every remaining row equal
            // to the exemplar `(ex_pk, payload)` and advance past it. Applied to
            // the exemplar's own source — matching what `drive_merge` does for the
            // `Multi` path, so the two modes cannot disagree on a source carrying
            // intra-source duplicates — and, when it joined this group, to the
            // other head.
            let mut fold_run = |s: usize, net: &mut i64| {
                while states[s].is_valid() {
                    let pos = states[s].position;
                    if !pk_bytes_eq(sources[s].get_pk_bytes(pos), ex_pk)
                        || row_cmp(schema, &sources[s], pos, &sources[ex_src], ex_row) != Ordering::Equal
                    {
                        break;
                    }
                    *net += sources[s].get_weight(pos);
                    states[s].advance();
                }
            };
            fold_run(ex_src, &mut net);
            if other_in_group {
                // Only the fully-tied arm sets `other_in_group`, and it picks `a`
                // as the exemplar — so the head still to fold is always `b`.
                fold_run(b, &mut net);
            }
            if net != 0 {
                break Some((net, ex_src, ex_row));
            }
            // Ghost group (net weight 0 across both heads): walk to the next.
        };
        self.commit_emitted(emitted);
    }

    /// Approximate the number of rows remaining in this cursor (upper bound).
    #[cfg(test)]
    pub fn estimated_length(&self) -> usize {
        self.states.iter().map(|s| s.count.saturating_sub(s.position)).sum()
    }

    /// The number of raw entries this cursor's runs hold in `[start, end)`
    /// (`end = None` ⇒ to the end of every run). Both keys must be exactly
    /// `pk_stride` OPK bytes.
    ///
    /// Exact over **raw** entries: cross-run duplicates and ghosts are counted, so
    /// this is an upper bound on the live group count a walk would emit. `&self` —
    /// nothing is repositioned and no merge tree is built, so a later `seek_bytes`
    /// lands identically whether or not this ran. `O(sources × log N)`: two
    /// per-source binary lower bounds, the same searches `seek_bytes` would run to
    /// reach either edge.
    ///
    /// Every source of one cursor is a run of the same `Table` and shares that
    /// schema's stride, which is what makes one `start`/`end` pair valid across all
    /// of them.
    pub fn count_range_raw(&self, start: &[u8], end: Option<&[u8]>) -> usize {
        debug_assert_eq!(start.len(), self.schema.pk_stride() as usize);
        debug_assert!(end.is_none_or(|e| e.len() == self.schema.pk_stride() as usize));
        self.sources
            .iter()
            .zip(self.states.iter())
            .map(|(src, st)| {
                let lo = src.find_lower_bound_bytes(start);
                // `st.count` is the run's row count, so it is `lb(+∞)` for the
                // unbounded arm. Callers short-circuit the inverted and saturated
                // cases and each run is sorted, so `hi >= lo`; `saturating_sub`
                // costs nothing and keeps a future caller from underflowing.
                let hi = end.map_or(st.count, |e| src.find_lower_bound_bytes(e));
                hi.saturating_sub(lo)
            })
            .sum()
    }

    /// Raw column pointer for the current row, indexed by LOGICAL column index.
    ///
    /// Returns null for the PK column — use `current_key_narrow()` /
    /// `current_pk_bytes()` instead.
    /// Returning only `pk_lo` for a 16-byte PK would silently truncate the
    /// high half regardless of backing source.
    pub fn col_ptr(&self, col_idx: usize, col_size: usize) -> *const u8 {
        self.col_bytes(col_idx, col_size).map_or(ptr::null(), <[u8]>::as_ptr)
    }

    /// Raw bytes of logical column `col` (length `size`) for the current row, or
    /// `None` for an invalid cursor, a PK column, or an out-of-range/absent
    /// column. The primitive the pointer form above is derived from — the source
    /// accessor already returns a bounds-carrying slice, so nothing here has to
    /// rebuild one from a raw pointer.
    ///
    /// Does NOT consult the null bitmap: a NULL *value* still yields `Some` bytes,
    /// so callers needing NULL semantics check `col_is_null` / the null word first.
    pub(crate) fn col_bytes(&self, col: usize, size: usize) -> Option<&[u8]> {
        if !self.valid {
            return None;
        }
        // A PK column has no payload slot, which is the `None` result.
        let payload_idx = self.schema.try_payload_idx(col)?;
        let src = &self.sources[self.current_entry_idx];
        Some(gnitz_expr::RowSource::get_col_ptr(
            src,
            self.current_row,
            payload_idx,
            size,
        ))
    }

    /// Blob arena slice (bounds-carrying) for the current row's source.
    pub fn blob_slice(&self) -> &[u8] {
        if !self.valid {
            return &[];
        }
        self.sources[self.current_entry_idx].blob()
    }

    /// Decode the German string at logical column `col` of the current row into raw
    /// bytes (STRING and BLOB share the 16-byte layout). Returns empty when the column
    /// pointer is null or a long-string offset overruns the blob — the latter is
    /// `german_string_content`'s own degrade-to-empty contract, shared with the
    /// ordering and hashing paths so every reader sees a corrupt cell the same way.
    pub(crate) fn read_german_bytes(&self, col: usize) -> Vec<u8> {
        match self.col_bytes(col, 16) {
            Some(cell) => gnitz_wire::german_string_content(cell, self.blob_slice()).to_vec(),
            None => Vec::new(),
        }
    }

    /// Read a fixed 8-byte little-endian integer at logical column `col` of the
    /// current row. Every system/circuit column read this way is 8-byte; the
    /// `debug_assert` catches schema drift in dev. An absent column (invalid
    /// cursor, PK, or out of range — the null bitmap is not consulted, so a NULL
    /// *value* still reads its bytes) degrades to 0, the same
    /// degrade-don't-abort contract as `read_german_bytes`.
    pub(crate) fn read_i64(&self, col: usize) -> i64 {
        debug_assert_eq!(
            self.schema.columns[col].size() as usize,
            8,
            "read_i64: column not 8-byte"
        );
        self.col_bytes(col, 8)
            .map_or(0, |b| i64::from_le_bytes(b.try_into().unwrap()))
    }

    /// True iff logical column `col` is NULL in the current row. PK columns are never
    /// null (`try_payload_idx` returns `None` for them).
    pub(crate) fn col_is_null(&self, col: usize) -> bool {
        match self.schema.try_payload_idx(col) {
            Some(pi) => gnitz_wire::null_word_get(self.current_null_word, pi),
            None => false,
        }
    }
}

/// Build a ReadCursor over `runs`, skipping empty ones. Each `Run` owns its
/// backing via `Rc`, so the cursor has no borrow lifetime and callers hand it a
/// lazy iterator rather than materializing a slice per tier.
pub(crate) fn from_runs(runs: impl IntoIterator<Item = Run>, schema: SchemaDescriptor) -> ReadCursor {
    let mut sources = Vec::new();
    let mut states = Vec::new();
    for run in runs {
        let count = run.count();
        if count > 0 {
            sources.push(run);
            states.push(CursorState { position: 0, count });
        }
    }
    ReadCursor::new(sources, states, schema)
}

/// Test-only shorthand for [`from_runs`] over a batch slice and a shard slice.
#[cfg(test)]
pub(crate) fn create_read_cursor(
    batches: &[Rc<Batch>],
    shard_arcs: &[Rc<MappedShard>],
    schema: SchemaDescriptor,
) -> ReadCursor {
    from_runs(
        batches
            .iter()
            .cloned()
            .map(Run::Mem)
            .chain(shard_arcs.iter().cloned().map(Run::Shard)),
        schema,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
