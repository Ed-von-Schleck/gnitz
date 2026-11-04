//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped). Over a skeleton run, a PK group holding a
//! skeleton row comes out as that one coarse row: a split drain sets its key
//! aside for hydration, and a raw drain refuses it.

use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::{ControlFlow, Range};

use crate::repr::batch::{Batch, MemBatch};
use crate::repr::loser_tree::{LoserTree, Player};
use crate::repr::merge::{self, MergeOrder};
use crate::repr::pool::{recycle, take, vec_bytes};
use crate::repr::scatter::{ColumnarSource, Pick};
use crate::repr::seek::gallop_by;
use crate::repr::shard::MappedShard;
use crate::schema::{compare_pk_ordering, group_floor, with_payload_cmp, PayloadOrder, SchemaDescriptor};

mod gather;
mod output;
mod probe;
mod source;

use super::run::Run;
pub use gather::PkSetGather;
use gnitz_wire::RowSource;
pub(crate) use probe::GroupProbe;
pub use source::{BoundedIndexCursor, KeyGather, SourceCursor, UniqueRows};

/// The skeleton rows a split drain set aside instead of copying: their OPK keys, flat
/// and ascending, and — in debug builds — each one's coarse weight.
#[derive(Default)]
struct SkeletonKeys {
    keys: Vec<u8>,
    coarse: Vec<i64>,
}

impl SkeletonKeys {
    fn push(&mut self, pk: &[u8], weight: i64) {
        self.keys.extend_from_slice(pk);
        if cfg!(debug_assertions) {
            self.coarse.push(weight);
        }
    }

    /// Every row of `rows` of `shard`, a skeleton one.
    fn extend(&mut self, shard: &MappedShard, rows: Range<usize>) {
        shard.append_pks(rows.clone(), &mut self.keys);
        if cfg!(debug_assertions) {
            self.coarse.extend(rows.map(|r| shard.get_weight(r)));
        }
    }

    /// Run a raw drain, which has no hydrator to recompute a skeleton row's
    /// payload, and assert it met none.
    fn raw(drain: impl FnOnce(&mut SkeletonKeys) -> Option<Batch>) -> Option<Batch> {
        let mut keys = SkeletonKeys::default();
        let chunk = drain(&mut keys);
        assert!(
            keys.keys.is_empty(),
            "a raw drain met a skeleton row: hydrate its keys instead"
        );
        chunk
    }
}

/// A row the cursor stood on, by source and row: readable through
/// [`ReadCursor::row_at`] after the cursor has moved on.
#[derive(Clone, Copy)]
pub(crate) struct CursorRow {
    src: u32,
    row: u32,
}

impl CursorRow {
    /// This row as a [`Pick`] at `weight`.
    #[inline]
    fn pick(self, weight: i64) -> Pick {
        (self.src, self.row, weight)
    }
}

// ---------------------------------------------------------------------------
// ReadCursor
// ---------------------------------------------------------------------------

/// An N-way merge over one relation's runs, positioned on one group at a time.
pub struct ReadCursor {
    /// The runs the merge folds *across*; each is already folded within. One key
    /// clamps them all, so an identity two runs share cannot reach single-source
    /// mode.
    sources: Vec<Run>,
    /// Per source, the rows still to walk: `start` is the next row, `end` the row
    /// count or a range seek's clamp for the cursor's life. A reposition may
    /// leave `start` past `end`, which leaves the walk empty.
    states: Vec<Range<usize>>,
    /// The N-way merge tournament over `sources.len()` leaves — which never
    /// changes — re-played in place by each reposition. Idle while `mode` is
    /// `Some`.
    tree: LoserTree,
    /// The one source live at the last absolute reposition, walked without `tree`.
    /// `None` drives `tree`, and may be pessimistic: only a reposition re-derives it.
    mode: Option<usize>,
    schema: SchemaDescriptor,
    /// The merge's order, coarsened iff a source is a capacity-bounded view's
    /// skeleton shard. Keyed by the PK's leading bytes: finding a shared prefix
    /// would read every run's last row at each open.
    order: MergeOrder,
    /// The drain's per-source row windows, reused across chunks.
    drain_windows: Vec<Range<usize>>,
    /// Whether the cursor stands on a row. Written only by `commit_emitted`.
    valid: bool,
    /// The current row's net weight, while `valid`.
    current_weight: i64,
    /// The row the cursor stands on, while `valid`.
    current: CursorRow,
    /// How many leading sources are one store's heap runs newest first, each
    /// newer than every source after them; set by
    /// [`Self::mark_heap_newest_first`], 0 otherwise.
    heap_newest_first: u32,
}

impl ReadCursor {
    /// The one live source, or `None` for none or several. Re-derived rather than
    /// the source set being narrowed: a seek is backward-capable, so a later one
    /// at a lower key can re-liven a source an earlier range seek emptied.
    fn live_single(states: &[Range<usize>]) -> Option<usize> {
        let mut live = states
            .iter()
            .enumerate()
            .filter_map(|(i, st)| (!st.is_empty()).then_some(i));
        match (live.next(), live.next()) {
            (Some(a), None) => Some(a),
            _ => None,
        }
    }

    /// Re-derive the drive mode after the per-source positions moved, then advance
    /// to the first live row.
    fn rebuild_and_advance(&mut self) {
        self.mode = Self::live_single(&self.states);
        match self.mode {
            Some(i) => self.advance_single(i),
            None => with_payload_cmp!(self.schema, |p| self.rebuild_and_advance_merge_with(p)),
        }
    }

    /// One payload dispatch for the tournament re-play and the advance after it.
    /// Ties go to [`merge::merge_less`], as in the advance and forward seek that
    /// step the tree afterwards.
    #[inline]
    fn rebuild_and_advance_merge_with<P: PayloadOrder>(&mut self, payload: P) {
        {
            let ReadCursor { tree, sources, states, schema, order, .. } = &mut *self;
            tree.rebuild(
                #[inline(always)]
                |i| order.head(sources, i, &states[i]),
                merge::merge_less(schema, sources, *order, payload),
            );
        }
        self.advance_merge_with(payload);
    }

    /// Position the cursor on the half-open OPK range `[start, end)`, clamping every
    /// source at `end` for the cursor's life, `rewind` included.
    fn seek_range_bytes(&mut self, start: &[u8], end: Option<&[u8]>) {
        debug_assert_eq!(start.len(), self.schema.pk_stride());
        debug_assert!(end.is_none_or(|e| e.len() == self.schema.pk_stride()));
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.start = src.seek(start, None);
            if let Some(end) = end {
                // `end` lands at or after `start`'s landing.
                state.end = state.end.min(src.seek(end, Some(state.start)));
            }
        }
        self.rebuild_and_advance();
    }

    /// Reset every source to its first row, positioning the cursor at the first
    /// row in storage order — by row index, so no key has to be spelled.
    #[cfg(test)]
    pub(crate) fn rewind(&mut self) {
        for state in self.states.iter_mut() {
            state.start = 0;
        }
        self.rebuild_and_advance();
    }

    /// Galloping lower-bound seek: each source's search is seeded at its live
    /// position, and a strictly-forward multi-source step keeps the loser tree in
    /// place. Lands where an absolute lower-bound seek would for any key, so an
    /// out-of-order probe forfeits only the speedup. `key` must be exactly
    /// `pk_stride` OPK bytes.
    pub(crate) fn advance_to(&mut self, key: &[u8]) {
        // Mode before the comparison, so a single-source cursor — with no tree to
        // gallop — does not pay for one that cannot change what it does. `valid`
        // before both: `current_pk_cmp_bytes` reads the positioned row.
        if self.valid && self.mode.is_none() && self.current_pk_cmp_bytes(key) == Ordering::Less {
            self.skip_below(key, |_, _| {});
        } else {
            self.reposition_to(key);
        }
    }

    /// [`Self::advance_to`] with its forward precondition already established:
    /// positioned, and below `key`. Skipping that comparison is the whole reason
    /// the two are separate.
    fn advance_to_forward(&mut self, key: &[u8]) {
        // Only the merge mode has a tournament an in-place gallop can maintain.
        if self.mode.is_none() {
            self.skip_below(key, |_, _| {});
        } else {
            self.reposition_to(key);
        }
    }

    /// Seed every source's gallop at its live position, then re-derive the mode —
    /// what every path that cannot maintain the tree in place takes.
    /// Backward-capable: each source searches its full row range.
    fn reposition_to(&mut self, key: &[u8]) {
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.start = src.seek(key, Some(state.start));
        }
        self.rebuild_and_advance();
    }

    /// Stand on the first live group whose PK is `>= key`, handing `skipped` each
    /// source with the rows of its walk it passes over. Those rows are unmerged:
    /// a row and a row of another source cancelling it are both handed over. The
    /// row the cursor stands on is consumed already, so it is not. Only the
    /// sources whose head lies below `key` are galloped, and the loser tree is
    /// kept in place. `key` must be exactly `pk_stride` OPK bytes and above the
    /// current row's PK.
    pub(crate) fn skip_below(&mut self, key: &[u8], skipped: impl FnMut(&Run, Range<usize>)) {
        with_payload_cmp!(self.schema, |p| self.skip_below_with(key, p, skipped));
    }

    #[inline]
    fn skip_below_with<P: PayloadOrder>(
        &mut self,
        key: &[u8],
        payload: P,
        mut skipped: impl FnMut(&Run, Range<usize>),
    ) {
        let ReadCursor {
            tree,
            sources,
            states,
            schema,
            order,
            mode,
            ..
        } = &mut *self;
        let sources = &*sources;
        let lags = |s: &Run, r: usize| compare_pk_ordering(s.get_pk_bytes(r), key) == Ordering::Less;
        let mut gallop = |s: &Run, at: &Range<usize>| {
            let to = s.seek(key, Some(at.start));
            skipped(s, at.start..to.min(at.end));
            to
        };
        match *mode {
            // The other sources' windows are empty, and a forward seek keeps them so.
            Some(src) => states[src].start = gallop(&sources[src], &states[src]),
            None => {
                let less = merge::merge_less(schema, sources, *order, payload);
                Self::seek_phase(tree, sources, *order, states, lags, gallop, &less);
            }
        }
        self.advance_with(payload);
    }

    /// Advance every laggard head (`lags` its row) to its own lower bound via
    /// `gallop`, restoring the loser tree with one `step_top` per gallop. Only the
    /// root can lag — it is the global min — so the loop gallops it alone, and
    /// leaves the tree positioned exactly as a from-scratch rebuild at the bound
    /// would.
    fn seek_phase(
        tree: &mut LoserTree,
        sources: &[Run],
        order: MergeOrder,
        states: &mut [Range<usize>],
        lags: impl Fn(&Run, usize) -> bool,
        mut gallop: impl FnMut(&Run, &Range<usize>) -> usize,
        less: &impl Fn(&Player, &Player) -> bool,
    ) {
        while let Some(Player { source_idx: src, row, .. }) = tree.peek() {
            let (src, row) = (src as usize, row as usize);
            // The root is the global min, so once it no longer lags neither does any head.
            if !lags(&sources[src], row) {
                break;
            }
            states[src].start = gallop(&sources[src], &states[src]);
            tree.step_top(order.head(sources, src, &states[src]), less);
        }
    }

    /// Position on `key`'s PK group; `true` when the cursor stands on it. `key` is
    /// a whole PK, or leading columns of one, whose group is every row it
    /// prefixes. Across calls, keys must strictly ascend, the first at or above
    /// where the cursor was positioned.
    pub(crate) fn seek_pk_group_ascending(&mut self, key: &[u8]) -> bool {
        if !self.valid {
            return false;
        }
        match compare_pk_ordering(&self.current_pk_bytes()[..key.len()], key) {
            Ordering::Greater => false,
            Ordering::Equal => true,
            Ordering::Less => {
                let stride = self.schema.pk_stride();
                if key.len() == stride {
                    self.advance_to_forward(key);
                } else {
                    self.advance_to_forward(group_floor(key, stride).pk_bytes());
                }
                self.valid && self.current_pk_starts_with(key)
            }
        }
    }

    /// Whether the cursor stands on a row. The `current_*` accessors are
    /// meaningful only while this returns `true`.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// The current row's net weight: its group's weights summed across sources.
    #[inline]
    pub fn current_weight(&self) -> i64 {
        debug_assert!(self.valid, "current_weight on an invalid cursor");
        self.current_weight
    }

    /// PK region of the current row as raw bytes, without copying. The single PK
    /// accessor for any width — correct for compound/wide PKs.
    pub fn current_pk_bytes(&self) -> &[u8] {
        debug_assert!(self.valid, "current_pk_bytes on an invalid cursor");
        let (src, row) = self.row_at(self.current);
        src.get_pk_bytes(row)
    }

    /// Whether the rows under this cursor's position outnumber its sources: a
    /// row walked costs more than one source's seek.
    #[inline]
    pub fn passed_more_than_a_seek(&self) -> bool {
        self.states.iter().map(|s| s.start).sum::<usize>() > self.sources.len()
    }

    /// Whether the current row came out of a capacity-bounded view's skeleton
    /// shard — a (PK, coarse weight) pair whose payload columns do not exist on
    /// disk. Its caller must hydrate that key from the view's own traces or source
    /// store instead of copying the row.
    ///
    /// One indexed load: `commit_emitted` records the emitted row's source in
    /// both modes, single-source bypass included.
    #[inline]
    fn current_is_skeleton(&self) -> bool {
        debug_assert!(self.valid, "current_is_skeleton on an invalid cursor");
        self.row_at(self.current).0.is_skeleton()
    }

    /// The current row as a `(source, row)` pair for the shared [`RowSource`]
    /// kernels. The source is the row's own entry, so its blob arena backs the
    /// row's German strings. The bound is `RowSource`, not `ColumnarSource`,
    /// because a cursor's weight comes off the cursor — a merged group's net
    /// weight is not the positioned source's stored weight.
    #[inline]
    pub fn current_row_source(&self) -> (&impl RowSource, usize) {
        debug_assert!(self.valid, "current_row_source on an invalid cursor");
        self.row_at(self.current)
    }

    /// The current row as a handle that stays readable through [`Self::row_at`]
    /// after the cursor moves on.
    #[inline]
    pub(crate) fn current_position(&self) -> CursorRow {
        debug_assert!(self.valid, "current_position on an invalid cursor");
        self.current
    }

    /// The current row as a [`Pick`] at its net weight.
    #[inline]
    fn current_pick(&self) -> Pick {
        self.current.pick(self.current_weight)
    }

    /// The source and row of a handle [`Self::current_position`] issued.
    #[inline(always)]
    pub(crate) fn row_at(&self, at: CursorRow) -> (&Run, usize) {
        (&self.sources[at.src as usize], at.row as usize)
    }

    /// Whether the current row's PK begins with `key`, a whole PK included.
    #[inline]
    fn current_pk_starts_with(&self, key: &[u8]) -> bool {
        self.current_pk_bytes().starts_with(key)
    }

    /// Walk `key`'s PK group — as [`Self::seek_pk_group_ascending`] reads a key —
    /// from wherever the cursor stands, invoking `f` at each emitted row; on
    /// return the cursor sits past the group.
    ///
    /// Not [`Self::for_each_row_while`] with an equality `cont`: this crate
    /// builds at `opt-level = 0` in dev, where that closure is a real call per row.
    pub(crate) fn for_each_pk_group_row<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], mut f: F) {
        with_payload_cmp!(
            self.schema,
            |payload| while self.valid && self.current_pk_starts_with(key) {
                f(&*self);
                self.advance_with(payload);
            }
        )
    }

    /// `f(i, w)` for every row `i` of the (PK, payload)-sorted `mb`, `w` the net
    /// weight of the trace element equal to row `i` in (PK, payload), or 0. Rows
    /// are probed in order, each moving the cursor forward from where the last left it.
    pub(crate) fn for_each_mem_row_weight<F: FnMut(usize, i64)>(&mut self, mb: &MemBatch, mut f: F) {
        // A skeleton run folds a PK group regardless of payload; no history holds one.
        debug_assert!(!self.order.coarsen, "element walk over a skeleton store");
        with_payload_cmp!(self.schema, |payload| for i in 0..mb.count {
            let w = self.weight_of_with(mb, i, payload);
            f(i, w);
        })
    }

    /// `mb[i]`'s weight at or after the cursor, consuming the element on a match.
    #[inline]
    fn weight_of_with<P: PayloadOrder>(&mut self, mb: &MemBatch, i: usize, payload: P) -> i64 {
        let key = mb.get_pk_bytes(i);
        // `advance_to_forward`, with the payload order already selected.
        if self.valid && self.current_pk_cmp_bytes(key) == Ordering::Less {
            match self.mode {
                None => self.skip_below_with(key, payload, |_, _| {}),
                Some(_) => self.reposition_to(key),
            }
        }
        if !self.valid || self.current_pk_cmp_bytes(key).is_ne() {
            return 0;
        }
        let mut ord = self.cmp_current_payload(mb, i, payload);
        if ord == Ordering::Less {
            self.seek_element_forward_with(mb, i, payload);
            if !self.valid || self.current_pk_cmp_bytes(key).is_ne() {
                return 0;
            }
            ord = self.cmp_current_payload(mb, i, payload);
        }
        match ord {
            Ordering::Equal => {
                let w = self.current_weight;
                self.advance_with(payload);
                w
            }
            _ => 0,
        }
    }

    #[inline]
    fn cmp_current_payload<P: PayloadOrder>(&self, mb: &MemBatch, i: usize, payload: P) -> Ordering {
        let (src, row) = self.row_at(self.current);
        payload.compare(&self.schema, src, row, mb, i)
    }

    /// Inside `mb[i]`'s PK group, forward-seek to the first live element `>= mb[i]`.
    /// Every head already sits at or above the cursor's element, which shares the PK.
    fn seek_element_forward_with<P: PayloadOrder>(&mut self, mb: &MemBatch, i: usize, payload: P) {
        let ReadCursor {
            tree,
            sources,
            states,
            schema,
            order,
            mode,
            ..
        } = &mut *self;
        let (sources, schema) = (&*sources, &*schema);
        let lags = |s: &Run, r: usize| payload.compare_full(schema, s, r, mb, i).is_lt();
        let gallop = |s: &Run, at: &Range<usize>| gallop_by(s.row_count(), Some(at.start), |r| lags(s, r));
        match *mode {
            // The other sources' windows are empty, and a forward seek keeps them so.
            Some(src) => states[src].start = gallop(&sources[src], &states[src]),
            None => {
                let less = merge::merge_less(schema, sources, *order, payload);
                Self::seek_phase(tree, sources, *order, states, lags, gallop, &less);
            }
        }
        self.advance_with(payload);
    }

    /// Walk forward while `cont` holds, calling `f` on each row; on return the
    /// cursor sits ON the first row `cont` rejected, or is exhausted. Seek-free.
    /// Selects the payload comparator once, where [`Self::advance`] re-runs that
    /// dispatch per call in merge mode.
    pub(crate) fn for_each_row_while<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor)>(&mut self, cont: C, mut f: F) {
        with_payload_cmp!(
            self.schema,
            |payload| while self.valid && cont(self.current_pk_bytes()) {
                f(&*self);
                self.advance_with(payload);
            }
        )
    }

    /// Storage-order comparison of the current row's PK against an OPK `key`.
    /// Correct at every PK width.
    #[inline]
    fn current_pk_cmp_bytes(&self, key: &[u8]) -> Ordering {
        compare_pk_ordering(self.current_pk_bytes(), key)
    }

    /// Land on the first positive-weight row whose PK begins with `prefix`, no
    /// longer than the PK; `true` iff there is one.
    pub(crate) fn seek_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
        self.advance_to(group_floor(prefix, self.schema.pk_stride()).pk_bytes());
        self.walk_positive_with_prefix_until(prefix, |_| ControlFlow::Break(()))
    }

    /// Visit every positive-weight row whose PK begins with `prefix`, from where
    /// the cursor stands, until `f` breaks, which leaves the cursor on that row;
    /// `true` iff `f` broke. Selects the payload comparator once, as
    /// [`Self::for_each_row_while`] does.
    fn walk_positive_with_prefix_until<F: FnMut(&ReadCursor) -> ControlFlow<()>>(
        &mut self,
        prefix: &[u8],
        mut f: F,
    ) -> bool {
        with_payload_cmp!(self.schema, |payload| {
            while self.valid && self.current_pk_starts_with(prefix) {
                if self.current_weight > 0 && f(&*self).is_break() {
                    return true;
                }
                self.advance_with(payload);
            }
            false
        })
    }

    /// Call `f` on every remaining row at positive weight.
    pub fn for_each_positive<F: FnMut(&ReadCursor)>(&mut self, mut f: F) {
        self.for_each_row_while(
            |_| true,
            |c| {
                if c.current_weight > 0 {
                    f(c);
                }
            },
        );
    }

    /// Step to the next live group; an advance consumes the group it emitted, so
    /// stepping is just re-driving. Mode before order, so the single-source
    /// bypass skips a `with_payload_cmp!` dispatch it never uses.
    #[inline]
    pub fn advance(&mut self) {
        match self.mode {
            Some(i) => self.advance_single(i),
            None => self.advance_merge(),
        }
    }

    /// [`Self::advance`]'s merge arm, out of line: every walk that selected its
    /// order once inlines [`Self::advance_merge_with`] instead.
    #[inline(never)]
    fn advance_merge(&mut self) {
        with_payload_cmp!(self.schema, |p| self.advance_merge_with(p))
    }

    /// Commit (or invalidate from) the `(source_idx, row, net_weight)` an advance
    /// emitted — the one writer of `valid` and the `current_*` state, for both
    /// modes. The PK stays as `current` and is decoded on demand.
    #[inline]
    fn commit_emitted(&mut self, emitted: Option<(usize, usize, i64)>) {
        if let Some((idx, row, weight)) = emitted {
            self.valid = true;
            self.current_weight = weight;
            self.current = CursorRow { src: idx as u32, row: row as u32 };
        } else {
            self.valid = false;
        }
    }

    /// Single-live-source bypass: no tournament, no ghost filter. Every other source's
    /// window is empty, and a run arrives already folded, so the position is either
    /// emit-ready or past-end.
    #[inline]
    fn advance_single(&mut self, i: usize) {
        let state = &mut self.states[i];
        let emitted = (state.start < state.end).then(|| {
            let row = state.start;
            state.start += 1;
            (i, row, self.sources[i].get_weight(row))
        });
        self.commit_emitted(emitted);
    }

    /// Advance to the next live group with the payload order already selected —
    /// the form a walk that selected it once reuses per row.
    #[inline(always)]
    fn advance_with<P: PayloadOrder>(&mut self, payload: P) {
        match self.mode {
            Some(i) => self.advance_single(i),
            None => self.advance_merge_with(payload),
        }
    }

    /// [`merge::drive`] over the cursor's tournament: `advance_merge_with` takes
    /// one group, `drain_sorted_into` a whole chunk.
    #[inline(always)]
    fn drive<P: PayloadOrder>(&mut self, payload: P, emit: impl FnMut(usize, usize, i64) -> ControlFlow<()>) {
        let ReadCursor { tree, sources, states, schema, order, .. } = &mut *self;
        merge::drive(tree, schema, sources, *order, states, payload, emit);
    }

    /// Merge-mode advance (`self.mode.is_none()`), monomorphized on payload.
    /// [`merge::drive`] folds tied rows; `emit` `Break`s on the first non-ghost
    /// group, so ghosts are passed over rather than surfaced.
    #[inline(always)]
    fn advance_merge_with<P: PayloadOrder>(&mut self, payload: P) {
        let mut emitted: Option<(usize, usize, i64)> = None;
        self.drive(payload, |gs, gr, nw| {
            emitted = Some((gs, gr, nw));
            ControlFlow::Break(())
        });
        self.commit_emitted(emitted);
    }

    /// Declare the leading [`Run::Mem`] sources one store's heap runs in age
    /// order, newest first, and every source after them older than each: a
    /// [`GroupProbe`] over a store of one row per PK reads a PK off the first
    /// of them holding it.
    pub fn mark_heap_newest_first(&mut self) {
        let heap = self.sources.iter().take_while(|run| matches!(run, Run::Mem(_))).count();
        self.heap_newest_first = heap as u32;
    }

    /// Upper bound on the rows a walk from HERE emits — the pre-size a batch that
    /// walk fills wants. The per-source windows (so a range seek's clamp is in
    /// it) PLUS the row the cursor sits on: a drive CONSUMES the group it emits,
    /// so `start` is already past a row still to be visited.
    pub fn estimated_length(&self) -> usize {
        let ahead: usize = self.states.iter().map(|s| s.len()).sum();
        ahead + usize::from(self.valid)
    }
}

/// Build a ReadCursor over `runs`, skipping empty ones, and position it on the
/// first live PK group. Every run must be folded. `cap` is an allocation hint
/// for the source vectors.
pub fn from_runs(runs: impl IntoIterator<Item = Run>, schema: SchemaDescriptor, cap: usize) -> ReadCursor {
    build(runs, schema, cap, ReadCursor::rebuild_and_advance)
}

/// [`from_runs`] positioned on the first live row of the OPK band `[start, end)`,
/// every source clamped at `end` for the cursor's life.
pub fn from_runs_in_band(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    cap: usize,
    start: &[u8],
    end: Option<&[u8]>,
) -> ReadCursor {
    build(runs, schema, cap, |c| c.seek_range_bytes(start, end))
}

/// [`from_runs`] positioned on the first live row at or above `key`, each
/// source galloping from its first row: cheaper than [`from_runs_in_band`]'s
/// binary search where `key` lies near the bottom of every run.
pub fn from_runs_at(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    cap: usize,
    key: &[u8],
) -> ReadCursor {
    build(runs, schema, cap, |c| c.reposition_to(key))
}

/// Positioned by `position` before it is returned, so no caller moves a cursor it
/// has to mutate first.
#[inline]
fn build(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    cap: usize,
    position: impl FnOnce(&mut ReadCursor),
) -> ReadCursor {
    let CursorBufs { mut sources, mut states, losers, windows } = take(&CURSOR_BUFS, |_| true).unwrap_or_default();
    sources.reserve(cap);
    states.reserve(cap);
    // `for_each`, not `for`: the stores hand in nested adaptors, which fold
    // far cheaper than they step.
    runs.into_iter().for_each(|run| {
        debug_assert!(
            !matches!(&run, Run::Mem(b) if !b.is_consolidated()),
            "a cursor run must be folded"
        );
        let count = run.row_count();
        if count > 0 {
            debug_assert!(
                count < u32::MAX as usize,
                "merge source exceeds the tournament's u32 row"
            );
            sources.push(run);
            states.push(0..count);
        }
    });
    let order = MergeOrder::leading(schema.pk_stride(), sources.iter().any(ColumnarSource::is_skeleton));
    let mut cursor = ReadCursor {
        // Sized for the source count; `rebuild_and_advance` plays it.
        tree: LoserTree::with_buffer(sources.len(), losers),
        sources,
        order,
        states,
        drain_windows: windows,
        mode: None,
        schema,
        valid: false,
        current_weight: 0,
        current: CursorRow { src: 0, row: 0 },
        heap_newest_first: 0,
    };
    position(&mut cursor);
    cursor
}

/// A dropped cursor's vectors, emptied, for the thread's next [`build`].
#[derive(Default)]
struct CursorBufs {
    sources: Vec<Run>,
    states: Vec<Range<usize>>,
    losers: Vec<Player>,
    windows: Vec<Range<usize>>,
}

/// Elements a pooled vector keeps room for; a larger one is freed, so a cursor
/// over many runs does not park its buffer for a point read to take.
const MAX_POOLED_ELEMS: usize = 4096;

/// `v` emptied, or a fresh vector in place of one above [`MAX_POOLED_ELEMS`];
/// and the heap bytes it keeps.
fn emptied<T>(mut v: Vec<T>) -> (Vec<T>, usize) {
    if v.capacity() > MAX_POOLED_ELEMS {
        return (Vec::new(), 0);
    }
    v.clear();
    let bytes = vec_bytes(&v);
    (v, bytes)
}

thread_local! {
    /// Each thread's own: a `Run` holds an `Rc`, so the buffers never cross threads.
    static CURSOR_BUFS: Cell<VecDeque<CursorBufs>> = const { Cell::new(VecDeque::new()) };
}

impl Drop for ReadCursor {
    /// Return the cursor's vectors to the thread's pool. Emptying `sources`
    /// drops its runs, so a pooled buffer holds no batch or shard alive.
    fn drop(&mut self) {
        let (sources, a) = emptied(std::mem::take(&mut self.sources));
        let (states, b) = emptied(std::mem::take(&mut self.states));
        let (losers, c) = emptied(self.tree.take_buffer());
        let (windows, d) = emptied(std::mem::take(&mut self.drain_windows));
        let bufs = CursorBufs { sources, states, losers, windows };
        recycle(&CURSOR_BUFS, bufs, a + b + c + d);
    }
}

/// A cursor over nothing, in `schema`'s shape.
pub fn empty_cursor(schema: SchemaDescriptor) -> ReadCursor {
    from_runs(std::iter::empty(), schema, 0)
}

#[cfg(test)]
#[path = "benches/read_cursor.rs"]
mod bench;
#[cfg(test)]
#[path = "tests/read_cursor.rs"]
mod tests;
