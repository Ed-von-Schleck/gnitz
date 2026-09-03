//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped).

use std::cell::OnceCell;
use std::cmp::Ordering;
use std::ops::Range;
#[cfg(test)]
use std::rc::Rc;

use super::batch::Batch;
use super::columnar::with_payload_cmp;
use super::columnar::ColumnarSource;
use super::heap::{drive_merge, HeapNode, LoserTree};
use super::merge::MemBatch;
use super::merge::{self, ColPtr, PosCursor, UnifiedSource};
#[cfg(test)]
use super::shard_reader::MappedShard;
use crate::schema::key::{compare_pk_ordering, pk_bytes_eq, PkBuf};
use crate::schema::SchemaDescriptor;

mod gather;
mod output;

use super::run::Run;
pub use gather::PkSetGather;
use gnitz_expr::RowSource;

// ---------------------------------------------------------------------------
// ReadCursor
// ---------------------------------------------------------------------------

/// Dispatch on the source set that was live at the last absolute reposition
/// (`mode_for`). `Single` carries the index it drives, so the bypass applies
/// whether or not the live source is the leading one; `Multi` drives
/// `ReadCursor::tree`, into which dead sources enter as sentinel leaves.
///
/// Forward progress does not re-derive the mode, so it can be pessimistic (a
/// `Multi` whose sources have since exhausted down to one). That direction is
/// safe — the tree's sentinels handle an exhausted member — so mode and liveness
/// agreeing is not an invariant.
#[derive(Clone, Copy)]
enum SourceMode {
    Empty,
    Single(usize),
    Multi,
}

pub struct ReadCursor {
    sources: Vec<Run>,
    states: Vec<PosCursor>,
    /// The scatter's source views and the flat payload-`ColPtr` table they index
    /// into (see `mem_batch_to_unified`). Many cursor consumers (point lookups,
    /// seeks) never call `scatter_drained_into`; built on first use.
    unified_sources: OnceCell<(Vec<UnifiedSource>, Vec<ColPtr>)>,
    /// The N-way merge tournament, sized once from `sources.len()` — which never
    /// changes — and re-played in place by each reposition. Idle under
    /// `Empty`/`Single`.
    tree: LoserTree,
    mode: SourceMode,
    pub(crate) schema: SchemaDescriptor,
    /// At least one source is a capacity-bounded view's skeleton shard, so the
    /// merge runs with payload coarsening on (see [`merge::merge_less`]). Derived
    /// once at open — the source set never changes — and it is the whole cost
    /// every other relation pays for this feature: one enum-discriminant test per
    /// run beside the empty-run filter `from_runs` already runs.
    any_skeleton: bool,
    // Current row state
    pub valid: bool,
    pub current_weight: i64,
    pub(crate) current_null_word: u64,
    current_entry_idx: usize,
    current_row: usize,
}

/// The comparator the `_with` variants are monomorphized over — the same
/// [`merge::RowComparator`] the flush/compaction kernel uses, at this cursor's
/// source type.
trait RowComparator: merge::RowComparator<Run> {}
impl<F: merge::RowComparator<Run>> RowComparator for F {}

impl ReadCursor {
    /// The schema every row this cursor yields is shaped by — fixed at open, so
    /// a consumer reads it from here rather than carrying a second copy that
    /// could go stale against the store the cursor came from.
    pub(crate) fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// True when a run this walk merges is a skeleton shard, so a consumer that
    /// cannot read a skeleton row must hydrate first. Per-run over the runs the
    /// open selected — a strict subset of the store-level
    /// [`Table::has_skeleton_rows`](super::table::Table::has_skeleton_rows).
    pub(crate) fn any_skeleton(&self) -> bool {
        self.any_skeleton
    }

    /// A cursor over already-materialized batches, skipping empty ones — what a
    /// caller with rows in hand uses when it needs the cursor interface, with no
    /// backing `Table` and no scratch dir. Keeps [`Run`] inside the LSM layer.
    pub(crate) fn over_batches(batches: &[std::rc::Rc<Batch>], schema: SchemaDescriptor) -> ReadCursor {
        from_runs(batches.iter().map(|b| Run::Mem(std::rc::Rc::clone(b))), schema)
    }

    /// Re-play the tournament at the sources' current positions. Keyless leaf:
    /// it carries only the row index, and the comparator reads each player's OPK
    /// bytes through `(source_idx, row)`. That comparator is
    /// [`merge::merge_less`] — the same order the flush/compaction kernel merges
    /// under, and the one the drive and forward-seek paths key the tree through,
    /// so a tree maintained in place can never order rows differently from how
    /// it was played.
    fn rebuild_tree(&mut self) {
        with_payload_cmp!(self.schema, Self::rebuild_tree_with, self);
    }

    #[inline]
    fn rebuild_tree_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        // Destructured so the tournament's `&mut tree` and the comparator's
        // `&sources` / `&states` are disjoint field borrows.
        let ReadCursor {
            tree,
            sources,
            states,
            schema,
            any_skeleton,
            ..
        } = self;
        tree.rebuild(
            |i| states[i].is_valid().then(|| states[i].position as u32),
            merge::merge_less(schema, sources, row_cmp, *any_skeleton),
        );
    }

    /// The drive mode for the currently live sources: a source whose
    /// `[position, count)` window is empty cannot contribute a row, so with a
    /// single live source the drive skips the tree entirely.
    ///
    /// Emptiness is not permanent, which is why the mode is *derived* at every
    /// absolute reposition rather than the source set being narrowed:
    /// a galloping seek searches its source's full row count and is
    /// backward-capable, so a later seek at a lower key can move a source that a
    /// range seek emptied back inside its clamped window. Dropping the source
    /// would discard rows it can still contribute.
    ///
    /// Two live sources take the tree, not a bypass: a 2-head merge measures at
    /// parity on a full scan and ~5% behind on a monotone `advance_to` sweep,
    /// where only `Multi` reaches `seek_forward_multi`'s in-place gallop.
    fn mode_for(states: &[PosCursor]) -> SourceMode {
        let mut live = states
            .iter()
            .enumerate()
            .filter_map(|(i, st)| st.is_valid().then_some(i));
        match (live.next(), live.next()) {
            (None, _) => SourceMode::Empty,
            (Some(a), None) => SourceMode::Single(a),
            _ => SourceMode::Multi,
        }
    }

    pub(crate) fn new(sources: Vec<Run>, states: Vec<PosCursor>, schema: SchemaDescriptor) -> Self {
        debug_assert_eq!(sources.len(), states.len());
        let any_skeleton = sources.iter().any(ColumnarSource::is_skeleton);
        let mut cursor = ReadCursor {
            // Sized for the source count; `rebuild_and_drive` plays it.
            tree: LoserTree::empty(sources.len()),
            sources,
            any_skeleton,
            states,
            unified_sources: OnceCell::new(),
            mode: SourceMode::Empty,
            schema,
            valid: false,
            current_weight: 0,
            current_null_word: 0,
            current_entry_idx: 0,
            current_row: 0,
        };
        cursor.rebuild_and_drive();
        cursor
    }

    /// Re-derive the drive mode after the per-source positions have been moved,
    /// then drive to the first live row. Repositioning changes which sources are
    /// live and invalidates a tree's cached head comparisons, so every absolute
    /// reposition must recompute the mode before the next drive. `sources` is
    /// left intact, which keeps the positional `unified_sources` cache valid.
    fn rebuild_and_drive(&mut self) {
        self.mode = Self::mode_for(&self.states);
        if matches!(self.mode, SourceMode::Multi) {
            self.rebuild_tree();
        }
        self.drive();
    }

    /// Position the cursor on the half-open OPK range `[start, end)`. Clamping
    /// each source's row count at `end` makes every drain/advance path exhaust at
    /// the cut, so no walk needs a per-row boundary check. The upper bound is
    /// part of the cursor's view from then on, and `rewind` keeps it; it only
    /// ever narrows, so a later call cannot widen the window. Both keys must be
    /// exactly `pk_stride` OPK bytes.
    pub fn seek_range_bytes(&mut self, start: &[u8], end: Option<&[u8]>) {
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.position = src.find_lower_bound_bytes(start);
            if let Some(end) = end {
                state.count = state.count.min(src.find_lower_bound_bytes(end));
            }
        }
        self.rebuild_and_drive();
    }

    /// Reset every source to its first row, positioning the cursor at the first
    /// row in storage order — by row index, so no key has to be spelled. The
    /// keyless join probe opens with it, having no key to seek by.
    pub(crate) fn rewind(&mut self) {
        for state in self.states.iter_mut() {
            state.position = 0;
        }
        self.rebuild_and_drive();
    }

    /// Absolute lower-bound seek: each source binary-searches its full row range
    /// for the first row `>= key`. `key` must be exactly `pk_stride` OPK bytes
    /// (the bytes `get_pk_bytes`/`current_pk_bytes` yield). Correct at every PK
    /// width and signedness — the search and the tree both order by the raw OPK
    /// bytes.
    ///
    /// [`advance_to`] lands on the same row for any key and is never slower, so
    /// prefer it. What this adds is a guaranteed mode re-derivation: `advance_to`
    /// may leave a pessimistic `Multi` behind, which costs `drain_single_source`
    /// its bulk-copy path. Seek with this before a bulk drain.
    pub fn seek_bytes(&mut self, key: &[u8]) {
        self.seek_range_bytes(key, None);
    }

    /// Galloping lower-bound seek, seeding each source's search at its own live
    /// position, and on a strictly-forward multi-source step maintaining the
    /// loser tree in place rather than rebuilding it. Lands on the same row
    /// [`seek_bytes`] would for any key, so an out-of-order probe forfeits only
    /// the speedup. `key` must be exactly `pk_stride` OPK bytes.
    ///
    /// This is the seek to reach for by default; `seek_bytes` is never cheaper.
    pub(crate) fn advance_to(&mut self, key: &[u8]) {
        // The in-place gallop needs a strictly greater key: at `key == current_pk`
        // the lower bound can be a row already consumed (an earlier payload at the
        // same PK), which a forward gallop cannot reach. `valid` is tested first so
        // `current_pk_cmp_bytes` never reads an unpositioned cursor.
        if self.valid && matches!(self.mode, SourceMode::Multi) && self.current_pk_cmp_bytes(key) == Ordering::Less {
            self.seek_forward_multi(key);
            return;
        }
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.position = src.advance_to(key, state.position);
        }
        self.rebuild_and_drive();
    }

    /// Gallop the `Multi` loser tree forward to the first head `>= key`, then
    /// drive the first live group. Precondition (enforced by [`advance_to`]'s
    /// dispatch): the mode is `Multi` and `key` > the current emitted PK.
    fn seek_forward_multi(&mut self, key: &[u8]) {
        with_payload_cmp!(self.schema, Self::seek_forward_multi_with, self, key);
    }

    #[inline]
    fn seek_forward_multi_with<RowCmp: RowComparator>(&mut self, key: &[u8], row_cmp: RowCmp) {
        // Scoping the field borrows to `seek_phase` frees `self` for the drive.
        let ReadCursor {
            tree,
            sources,
            states,
            schema,
            any_skeleton,
            ..
        } = &mut *self;
        let less = merge::merge_less(schema, sources, row_cmp, *any_skeleton);
        Self::seek_phase(tree, sources, states, key, &less);
        self.drive_multi_with(row_cmp);
    }

    /// Advance every laggard head (OPK `< key`) to its own `lower_bound(key)`,
    /// restoring the loser tree with one `replace_top`/`pop_top` per gallop. Only
    /// the root is ever a laggard — it is the global min, so once it reaches `key`
    /// every head has — and each gallop moves that source to `>= key`, so the loop
    /// touches at most `num_sources` leaves and terminates. It leaves the tree
    /// positioned exactly as a from-scratch rebuild at `key` would.
    fn seek_phase(
        heap: &mut LoserTree,
        sources: &[Run],
        states: &mut [PosCursor],
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
            states[src].position = sources[src].advance_to(key, states[src].position);
            heap.step_top(states[src].is_valid().then_some(states[src].position as u32), less);
        }
    }

    /// Seek to `key` (exact `pk_stride` OPK bytes) and report whether it landed
    /// on a present, live row: positioned, PK byte-equal, and net weight > 0.
    /// Correct for any probe order, so a sequence of probes can share one cursor
    /// and an ascending run becomes a single monotone sweep.
    ///
    /// The weight gate is not redundant — a single-source cursor does not
    /// consolidate, so it can surface a tombstone an uncompacted source still
    /// holds, and every point lookup must reject it.
    ///
    /// This tests only the group at `key`'s lower bound, so it answers "is this
    /// key live" only where the PK is unique. Use [`copy_live_pk_group_into`]
    /// against a store whose PK can repeat.
    pub fn advance_to_exact_live(&mut self, key: &[u8]) -> bool {
        self.advance_to(key);
        self.valid && self.current_pk_eq(key) && self.current_weight > 0
    }

    /// PK region of the current row as raw bytes, without copying. The single PK
    /// accessor for any width — correct for compound/wide PKs.
    pub fn current_pk_bytes(&self) -> &[u8] {
        self.sources[self.current_entry_idx].get_pk_bytes(self.current_row)
    }

    /// Whether the current row came out of a capacity-bounded view's skeleton
    /// shard — a (PK, coarse weight) pair whose payload columns do not exist on
    /// disk. Its caller must hydrate that key from the view's own traces or
    /// source store instead of copying the row. Gate on `valid` first.
    ///
    /// One indexed load: `commit_emitted` records the emitted row's source in
    /// every drive mode, single-source bypass included.
    #[inline]
    pub(crate) fn current_is_skeleton(&self) -> bool {
        debug_assert!(self.valid, "current_is_skeleton on an invalid cursor");
        self.sources[self.current_entry_idx].is_skeleton()
    }

    /// The current row as a `(source, row)` pair for the shared [`RowSource`]
    /// kernels. The source is the row's own entry, so its blob arena backs the
    /// row's German strings. The bound is `RowSource`, not `ColumnarSource`,
    /// because a cursor's weight comes off the cursor — a merged group's net
    /// weight is not the positioned source's stored weight. Gate on `valid`.
    #[inline]
    pub fn current_row_source(&self) -> (&impl RowSource, usize) {
        debug_assert!(self.valid, "current_row_source on an invalid cursor");
        (&self.sources[self.current_entry_idx], self.current_row)
    }

    /// The current row's PK as its native scalar value. Only narrow
    /// (`pk_stride ≤ 16`) relations have one; panics above that width. Gate on
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
    pub(crate) fn current_pk_eq(&self, key_bytes: &[u8]) -> bool {
        pk_bytes_eq(self.current_pk_bytes(), key_bytes)
    }

    /// Seek to the start of `key`'s PK group, unless the cursor already sits in
    /// it. The reposition every keyed reader needs before [`for_each_pk_group_row`]
    /// when it does not already know where the cursor stands.
    ///
    /// The seek is skipped at `Equal` because every walk and every seek leaves the
    /// cursor on a group's first unconsumed row, and `advance_to` is *not*
    /// idempotent there: at `key == current_pk` the lower bound can be a row
    /// already consumed (an earlier payload at the same PK), which a forward
    /// gallop cannot reach. `Greater` does not mean the group is absent, only
    /// that an earlier consumer left the cursor ahead.
    pub(crate) fn seek_pk_group(&mut self, key: &[u8]) {
        if !self.valid || self.current_pk_cmp_bytes(key) != Ordering::Equal {
            self.advance_to(key);
        }
    }

    /// Walk the equal-`key` PK group from wherever the cursor stands, invoking
    /// `f(&*self)` at each emitted row; the callback reads the row through the
    /// committed `current_*` state and must not re-enter the cursor. On return the
    /// cursor sits at the first row past the group, or is invalid at end of
    /// source.
    ///
    /// Seek-free: a caller that has just located `key` through the merge (a
    /// cogroup's `Equal` arm) would pay a discarded comparison for a reposition it
    /// knows is unnecessary. Everyone else calls [`Self::seek_pk_group`] first.
    ///
    /// Has its own body rather than deferring to [`Self::for_each_row_while`]
    /// with an equality `cont`: that closure does not fold into the walk, and the
    /// call it leaves costs 0.8% of `join_equi_dt_bench`.
    pub(crate) fn for_each_pk_group_row<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], f: F) {
        with_payload_cmp!(self.schema, Self::for_each_pk_group_row_with::<_, _>, self, key, f);
    }

    #[inline]
    fn for_each_pk_group_row_with<RowCmp: RowComparator, F: FnMut(&ReadCursor)>(
        &mut self,
        key: &[u8],
        mut f: F,
        row_cmp: RowCmp,
    ) {
        while self.valid && self.current_pk_eq(key) {
            f(&*self);
            self.drive_with(row_cmp);
        }
    }

    /// `f(i, w)` for each row `i` of `mb[range]`, one PK group, with the weight of
    /// the byte-equal (PK, payload) trace row, or `0`: one lockstep pass over the
    /// two sorted sides from the cursor's position, which the caller has put at
    /// or past the group, in place of a seek per row.
    pub(crate) fn for_each_mem_row_weight<F: FnMut(usize, i64)>(&mut self, mb: &MemBatch, range: Range<usize>, f: F) {
        with_payload_cmp!(
            self.schema,
            Self::for_each_mem_row_weight_with::<_, _>,
            self,
            mb,
            range,
            f
        );
    }

    #[inline]
    fn for_each_mem_row_weight_with<F, RowCmp>(&mut self, mb: &MemBatch, range: Range<usize>, mut f: F, row_cmp: RowCmp)
    where
        F: FnMut(usize, i64),
        RowCmp: for<'x> merge::RowComparator<Run, MemBatch<'x>>,
    {
        if range.is_empty() {
            return;
        }
        let key = mb.get_pk_bytes(range.start);
        debug_assert!(
            pk_bytes_eq(key, mb.get_pk_bytes(range.end - 1)),
            "for_each_mem_row_weight: range spans more than one PK group"
        );
        debug_assert!(
            !self.valid || self.current_pk_cmp_bytes(key) != Ordering::Less,
            "for_each_mem_row_weight: cursor behind the group"
        );
        for i in range {
            let w = loop {
                if !self.valid || !self.current_pk_eq(key) {
                    break 0;
                }
                match row_cmp(
                    &self.schema,
                    &self.sources[self.current_entry_idx],
                    self.current_row,
                    mb,
                    i,
                ) {
                    Ordering::Less => self.advance(),
                    Ordering::Equal => {
                        let w = self.current_weight;
                        self.advance();
                        break w;
                    }
                    Ordering::Greater => break 0,
                }
            };
            f(i, w);
        }
    }

    /// Walk forward from the current row while `cont` holds, calling `f` on each;
    /// `f` reads the row through the committed `current_*` state and must not
    /// re-enter the cursor. Seek-free — the caller positions it. On return the
    /// cursor sits ON the first row for which `cont` was false, or is exhausted.
    ///
    /// Selects the payload comparator once for the whole walk, where
    /// [`Self::advance`] re-runs that dispatch per call in `Multi` mode.
    pub(crate) fn for_each_row_while<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor)>(&mut self, cont: C, f: F) {
        with_payload_cmp!(self.schema, Self::for_each_row_while_with::<_, _, _>, self, cont, f);
    }

    /// Threads the monomorphized `row_cmp` through each drive, so the per-row
    /// advance never re-selects the comparator.
    #[inline]
    fn for_each_row_while_with<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor), RowCmp: RowComparator>(
        &mut self,
        cont: C,
        mut f: F,
        row_cmp: RowCmp,
    ) {
        while self.valid && cont(self.current_pk_bytes()) {
            f(&*self);
            self.drive_with(row_cmp);
        }
    }

    /// Seek to `key`'s PK group and append every live row of it to `out`; an
    /// absent key appends nothing. The keyed-read primitive for a store whose PK
    /// can repeat — a view output store runs no `enforce_unique_pk`, so its
    /// synthetic key names one row per row the join produced.
    ///
    /// The weight gate is per row, not a presence test for the key: a retracted
    /// member an uncompacted source still holds sorts within its PK by payload,
    /// so it can head the group, and rejecting the key on it would drop the live
    /// rows behind it.
    pub(crate) fn copy_live_pk_group_into(&mut self, key: &[u8], out: &mut Batch) {
        self.seek_pk_group(key);
        self.for_each_pk_group_row(key, |c| {
            if c.current_weight > 0 {
                c.copy_current_row_into(out, c.current_weight);
            }
        });
    }

    /// Storage-order comparison of the current row's PK against an OPK `key`.
    /// Correct at every PK width. Callers must gate on `valid` first.
    #[inline]
    pub(crate) fn current_pk_cmp_bytes(&self, key: &[u8]) -> Ordering {
        compare_pk_ordering(self.current_pk_bytes(), key)
    }

    /// Position the cursor at the first row whose PK begins with `prefix` and
    /// whose weight is `> 0`. Returns `true` on a hit (cursor stays positioned;
    /// `current_pk_bytes`, `current_weight` etc. are valid). Returns `false`
    /// on miss (cursor may be past the prefix range or fully invalid).
    ///
    /// `prefix` is the OPK image of the leading PK column(s); it is zero-padded
    /// to `pk_stride` to form the seek key. 0x00 is the OPK minimum for every PK
    /// type (signed MIN maps to all-zeros after the sign flip), so the padded key
    /// is the correct lower bound for the suffix columns at every type.
    #[inline]
    pub fn seek_first_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
        let stride = self.schema.pk_stride() as usize;
        self.advance_to(PkBuf::from_bytes(prefix).padded(stride));
        self.walk_to_positive_with_prefix(prefix)
    }

    /// Visit every positive-weight row whose PK begins with `prefix`, invoking
    /// `f(&*self)` at each (the callback reads the committed `current_*` row
    /// state; it must not re-enter the cursor). The seek/advance/walk loop the
    /// system-table readers (circuit load, view-row retraction) share.
    pub fn for_each_positive_with_prefix<F: FnMut(&ReadCursor)>(&mut self, prefix: &[u8], f: F) {
        if !self.seek_first_positive_with_prefix(prefix) {
            return;
        }
        self.for_each_positive_while(|pk| pk.starts_with(prefix), f);
    }

    /// [`Self::for_each_row_while`] with the weight gate applied — the one place
    /// the two positive-row walks below spell `current_weight > 0`.
    fn for_each_positive_while<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor)>(&mut self, cont: C, mut f: F) {
        self.for_each_row_while(cont, |c| {
            if c.current_weight > 0 {
                f(c);
            }
        });
    }

    /// Every positive-weight row from HERE to the end of the cursor's window.
    /// The prefix form narrows within the window; this takes the whole of it,
    /// which for a cursor positioned over a range is that range.
    pub fn for_each_positive<F: FnMut(&ReadCursor)>(&mut self, f: F) {
        self.for_each_positive_while(|_| true, f);
    }

    /// Walk forward from the current position (no seek) to the next row whose PK
    /// begins with `prefix` and whose weight is `> 0`. Returns `false` once the
    /// prefix range ends or the cursor exhausts. Re-seeking here instead would
    /// re-find the row already consumed and spin forever.
    fn walk_to_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
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

    /// Step to the next live group. Every drive consumes the group it emitted, so
    /// this is just a re-drive.
    pub fn advance(&mut self) {
        self.drive();
    }

    /// Step the drive one group. The mode is settled *before* the payload
    /// comparator is selected, so the `Single` bypass and the exhausted case run
    /// without the `with_payload_cmp!` dispatch they never use.
    #[inline]
    fn drive(&mut self) {
        match self.mode {
            SourceMode::Empty => self.valid = false,
            SourceMode::Single(i) => self.drive_single(i),
            SourceMode::Multi => with_payload_cmp!(self.schema, Self::drive_multi_with, self),
        }
    }

    /// Commit (or invalidate from) the `(net_weight, source_idx, row)` a drive
    /// emitted — the one writer of the `current_*` state, for every mode. The PK
    /// stays as `(current_entry_idx, current_row)` and is decoded on demand.
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
    /// consolidated) and shards are ghost-free by construction, so the current
    /// position is either emit-ready or past-end. Any other source has an empty
    /// `[position, count)` window and cannot contribute a row to fold against.
    #[inline]
    fn drive_single(&mut self, i: usize) {
        let state = &mut self.states[i];
        let emitted = state.is_valid().then(|| {
            let row = state.position;
            state.position += 1;
            (self.sources[i].get_weight(row), i, row)
        });
        self.commit_emitted(emitted);
    }

    /// Drive to the next live group with the payload comparator already selected —
    /// the form a walk that selected it once reuses per row.
    #[inline]
    fn drive_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        match self.mode {
            SourceMode::Empty => self.valid = false,
            SourceMode::Single(i) => self.drive_single(i),
            SourceMode::Multi => self.drive_multi_with(row_cmp),
        }
    }

    /// `Multi` drive, monomorphized on payload (`row_cmp`).
    /// Precondition: `matches!(self.mode, SourceMode::Multi)`.
    ///
    /// `drive_merge` folds tied rows for us; `emit` returns `Break` on the first
    /// non-ghost group to return immediately. Ghost groups (net weight = 0) skip
    /// emit and open the next group, so the walk passes over them.
    #[inline]
    fn drive_multi_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        let ReadCursor {
            tree,
            sources,
            states,
            schema,
            any_skeleton,
            ..
        } = &mut *self;
        let coarsen = *any_skeleton;
        // The emitted group comes back as a tuple rather than being written to
        // `self.current_*` in place: the closures below already reborrow
        // `&sources` + `&mut states`, so capturing `&mut self` too would conflict.
        let mut emitted: Option<(i64, usize, usize)> = None;
        drive_merge(
            tree,
            merge::merge_less(schema, sources, row_cmp, coarsen),
            |src| {
                states[src].advance();
                states[src].is_valid().then(|| states[src].position as u32)
            },
            merge::merge_same_pk(sources),
            merge::merge_eq_payload(schema, sources, row_cmp, coarsen),
            |src, row| sources[src].get_weight(row),
            |gs, gr, nw| {
                emitted = Some((nw, gs, gr));
                std::ops::ControlFlow::Break(())
            },
        );
        self.commit_emitted(emitted);
    }

    /// Upper bound on the rows a walk from HERE emits — the pre-size a batch that
    /// walk fills wants. The per-source windows (so a range seek's clamp is in
    /// it) PLUS the row the cursor sits on: a drive CONSUMES the group it emits,
    /// so `position` is already past a row still to be visited.
    pub fn estimated_length(&self) -> usize {
        let ahead: usize = self.states.iter().map(|s| s.count.saturating_sub(s.position)).sum();
        ahead + usize::from(self.valid)
    }

    /// The number of raw entries this cursor's runs hold in `[start, end)`
    /// (`end = None` ⇒ to the end of every run). Both keys must be exactly
    /// `pk_stride` OPK bytes.
    ///
    /// Counts **raw** entries: cross-run duplicates and ghosts are included, so
    /// this is an upper bound on the live group count a walk would emit. Takes
    /// `&self` — nothing is repositioned and no merge tree is built, so a later
    /// seek lands identically whether or not this ran. `O(sources × log N)`.
    ///
    /// Every source of one cursor is a run of the same `Table` and shares that
    /// schema's stride, which is what makes one `start`/`end` pair valid across all
    /// of them.
    pub(crate) fn count_range_raw(&self, start: &[u8], end: Option<&[u8]>) -> usize {
        debug_assert_eq!(start.len(), self.schema.pk_stride() as usize);
        debug_assert!(end.is_none_or(|e| e.len() == self.schema.pk_stride() as usize));
        self.sources
            .iter()
            .zip(self.states.iter())
            .map(|(src, st)| {
                let lo = src.find_lower_bound_bytes(start);
                // The unbounded arm stops at `st.count`, which a range seek may
                // have clamped below the run's row count — so an unbounded count on
                // a range-seeked cursor answers within that cursor's window.
                // Each run is sorted, so `hi >= lo`.
                let hi = end.map_or(st.count, |e| src.find_lower_bound_bytes(e));
                hi.saturating_sub(lo)
            })
            .sum()
    }

    /// Raw bytes of logical column `col` (length `size`) for the current row, or
    /// `None` for an invalid cursor, a PK column, or an out-of-range/absent
    /// column. Read a PK through `current_pk_bytes` / `current_key_narrow`
    /// instead — a PK column has no payload slot to read here.
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
    fn blob_slice(&self) -> &[u8] {
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
    pub fn read_german_bytes(&self, col: usize) -> Vec<u8> {
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
    pub fn read_i64(&self, col: usize) -> i64 {
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
    pub fn col_is_null(&self, col: usize) -> bool {
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
    let runs = runs.into_iter();
    let cap = runs.size_hint().0;
    let mut sources = Vec::with_capacity(cap);
    let mut states = Vec::with_capacity(cap);
    for run in runs {
        let count = run.count();
        if count > 0 {
            sources.push(run);
            states.push(PosCursor { position: 0, count });
        }
    }
    ReadCursor::new(sources, states, schema)
}

/// A cursor over nothing, in `schema`'s shape — what a relation this process
/// holds no store for reads as, so a detached handle answers every read the way
/// a store holding none of the requested rows does.
pub(crate) fn empty(schema: SchemaDescriptor) -> ReadCursor {
    from_runs(std::iter::empty(), schema)
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

#[cfg(test)]
mod bench;
#[cfg(test)]
#[path = "../tests/read_cursor.rs"]
mod tests;
