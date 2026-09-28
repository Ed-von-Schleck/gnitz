//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped).

use std::cmp::Ordering;
use std::ops::Range;
#[cfg(test)]
use std::rc::Rc;

use crate::schema::key::{compare_pk_ordering, pk_bytes_eq, PkBuf};
use crate::schema::payload_order::{with_payload_cmp, PayloadOrder};
use crate::schema::SchemaDescriptor;
#[cfg(test)]
use crate::storage::repr::batch::Batch;
use crate::storage::repr::heap::{HeapNode, LoserTree};
use crate::storage::repr::merge::MemBatch;
use crate::storage::repr::merge::{self, ColumnarSource, PosCursor};
#[cfg(test)]
use crate::storage::repr::shard_reader::MappedShard;

mod gather;
mod output;

use super::run::Run;
pub use gather::PkSetGather;
use gnitz_expr::RowSource;

/// The skeleton rows a split drain set aside instead of copying: their OPK keys, flat
/// and ascending, and — in debug builds — each one's coarse weight.
#[derive(Default)]
pub(crate) struct SkeletonKeys {
    pub(crate) keys: Vec<u8>,
    pub(crate) coarse: Vec<i64>,
}

impl SkeletonKeys {
    fn push(&mut self, pk: &[u8], weight: i64) {
        self.keys.extend_from_slice(pk);
        if cfg!(debug_assertions) {
            self.coarse.push(weight);
        }
    }

    /// The raw drains' guard: a raw drain has no hydrator to recompute a skeleton
    /// row's payload.
    pub(crate) fn assert_none(&self) {
        assert!(
            self.keys.is_empty(),
            "a raw drain met a skeleton row: hydrate its keys instead"
        );
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
    states: Vec<PosCursor>,
    /// The N-way merge tournament, sized once from `sources.len()` — which never
    /// changes — and re-played in place by each reposition. Idle while `mode` is
    /// `Some`.
    tree: LoserTree,
    /// The one source live at the last absolute reposition, walked without `tree`.
    /// `None` drives `tree`, and may be pessimistic: only a reposition re-derives it.
    mode: Option<usize>,
    pub(crate) schema: SchemaDescriptor,
    /// At least one source is a capacity-bounded view's skeleton shard, so the
    /// merge runs with payload coarsening on (see [`merge::merge_less`]).
    any_skeleton: bool,
    /// The drain's merge-order scratch, reused across chunks.
    merge_order: Vec<(u32, u32, i64)>,
    /// The drain's per-source row windows, reused across chunks.
    drain_windows: Vec<Range<usize>>,
    // Current row state
    pub valid: bool,
    pub current_weight: i64,
    current_entry_idx: usize,
    current_row: usize,
}

impl ReadCursor {
    /// The one live source, or `None` for none or several. Re-derived rather than
    /// the source set being narrowed: a seek is backward-capable, so a later one
    /// at a lower key can re-liven a source an earlier range seek emptied.
    fn live_single(states: &[PosCursor]) -> Option<usize> {
        let mut live = states
            .iter()
            .enumerate()
            .filter_map(|(i, st)| st.is_valid().then_some(i));
        match (live.next(), live.next()) {
            (Some(a), None) => Some(a),
            _ => None,
        }
    }

    /// Positioned by `position` before it is returned, so no caller moves a cursor it
    /// has to mutate first.
    #[inline]
    fn new(
        sources: Vec<Run>,
        states: Vec<PosCursor>,
        schema: SchemaDescriptor,
        position: impl FnOnce(&mut ReadCursor),
    ) -> Self {
        debug_assert_eq!(sources.len(), states.len());
        let any_skeleton = sources.iter().any(ColumnarSource::is_skeleton);
        let mut cursor = ReadCursor {
            // Sized for the source count; `rebuild_and_advance` plays it.
            tree: LoserTree::empty(sources.len()),
            sources,
            any_skeleton,
            states,
            merge_order: Vec::new(),
            drain_windows: Vec::new(),
            mode: None,
            schema,
            valid: false,
            current_weight: 0,
            current_entry_idx: 0,
            current_row: 0,
        };
        position(&mut cursor);
        cursor
    }

    /// Re-derive the drive mode after the per-source positions moved, then advance
    /// to the first live row.
    fn rebuild_and_advance(&mut self) {
        self.mode = Self::live_single(&self.states);
        match self.mode {
            Some(i) => self.advance_single(i),
            None => with_payload_cmp!(self.schema, Self::rebuild_and_advance_merge_with, self),
        }
    }

    /// One payload dispatch for the tournament re-play and the advance after it.
    /// The leaf is keyless — [`merge::merge_less`] reads each player's OPK bytes
    /// through `(source_idx, row)` — and it is the comparator the advance and
    /// forward-seek paths key the tree through, so a tree maintained in place
    /// cannot order rows differently from how it was played.
    #[inline]
    fn rebuild_and_advance_merge_with<P: PayloadOrder>(&mut self, payload: P) {
        {
            // Destructured so the tournament's `&mut tree` and the comparator's
            // `&sources` / `&states` are disjoint field borrows.
            let ReadCursor {
                tree,
                sources,
                states,
                schema,
                any_skeleton,
                ..
            } = &mut *self;
            tree.rebuild(
                |i| states[i].is_valid().then(|| states[i].position as u32),
                merge::merge_less(schema, sources, payload, *any_skeleton),
            );
        }
        self.advance_merge_with(payload);
    }

    /// Position the cursor on the half-open OPK range `[start, end)` and report the
    /// **raw** entry count in that window — cross-run duplicates and ghosts
    /// included, so an upper bound on the live groups a walk emits, and free,
    /// being the byproduct of the searches the seek runs anyway. Clamping each
    /// source at `end` is what makes every walk exhaust at the cut with no per-row
    /// boundary check; the bound only narrows, and `rewind` keeps it.
    pub(crate) fn seek_range_bytes(&mut self, start: &[u8], end: Option<&[u8]>) -> usize {
        debug_assert_eq!(start.len(), self.schema.pk_stride());
        debug_assert!(end.is_none_or(|e| e.len() == self.schema.pk_stride()));
        let mut raw = 0usize;
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.position = src.find_lower_bound_bytes(start);
            if let Some(end) = end {
                state.count = state.count.min(src.find_lower_bound_bytes(end));
            }
            // Each run is sorted, so the clamped count never sorts below the
            // lower bound.
            raw += state.count.saturating_sub(state.position);
        }
        self.rebuild_and_advance();
        raw
    }

    /// Reset every source to its first row, positioning the cursor at the first
    /// row in storage order — by row index, so no key has to be spelled. The
    /// keyless join probe opens with it, having no key to seek by.
    pub(crate) fn rewind(&mut self) {
        for state in self.states.iter_mut() {
            state.position = 0;
        }
        self.rebuild_and_advance();
    }

    /// Absolute lower-bound seek: each source binary-searches its full row range
    /// for the first row `>= key`. `key` must be exactly `pk_stride` OPK bytes
    /// (the bytes `get_pk_bytes`/`current_pk_bytes` yield). Correct at every PK
    /// width and signedness — the search and the tree both order by the raw OPK
    /// bytes.
    ///
    /// `Self::advance_to` lands identically, galloping from where the cursor
    /// stands; this one's landing owes nothing to where the cursor stood, which
    /// is what makes it the tests' and benches' independent oracle.
    pub fn seek_bytes(&mut self, key: &[u8]) {
        self.seek_range_bytes(key, None);
    }

    /// Galloping lower-bound seek: each source's search is seeded at its live
    /// position, and a strictly-forward multi-source step keeps the loser tree in
    /// place. Lands where [`Self::seek_bytes`] would for any key, so an
    /// out-of-order probe forfeits only the speedup. `key` must be exactly
    /// `pk_stride` OPK bytes.
    pub(crate) fn advance_to(&mut self, key: &[u8]) {
        // Mode before the comparison, so a single-source cursor — with no tree to
        // gallop — does not pay for one that cannot change what it does. `valid`
        // before both: `current_pk_cmp_bytes` reads the positioned row.
        if self.valid && self.mode.is_none() && self.current_pk_cmp_bytes(key) == Ordering::Less {
            self.seek_forward_merge(key);
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
            self.seek_forward_merge(key);
        } else {
            self.reposition_to(key);
        }
    }

    /// Seed every source's gallop at its live position, then re-derive the mode —
    /// what every path that cannot maintain the tree in place takes.
    /// Backward-capable: each source searches its full row range.
    fn reposition_to(&mut self, key: &[u8]) {
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.position = src.advance_to(key, state.position);
        }
        self.rebuild_and_advance();
    }

    /// Gallop the loser tree forward to the first head `>= key`, then advance to
    /// the first live group. [`Self::advance_to_forward`]'s dispatch is what
    /// establishes the mode and the strictly-forward key this needs.
    fn seek_forward_merge(&mut self, key: &[u8]) {
        with_payload_cmp!(self.schema, Self::seek_forward_merge_with, self, key);
    }

    #[inline]
    fn seek_forward_merge_with<P: PayloadOrder>(&mut self, key: &[u8], payload: P) {
        // Scoping the field borrows to `seek_phase` frees `self` for the advance.
        let ReadCursor {
            tree,
            sources,
            states,
            schema,
            any_skeleton,
            ..
        } = &mut *self;
        let less = merge::merge_less(schema, sources, payload, *any_skeleton);
        Self::seek_phase(tree, sources, states, key, &less);
        self.advance_merge_with(payload);
    }

    /// Advance every laggard head (OPK `< key`) to its own `lower_bound(key)`,
    /// restoring the loser tree with one `step_top` per gallop. Only the root can
    /// lag — it is the global min — so the loop gallops it alone, and leaves the
    /// tree positioned exactly as a from-scratch rebuild at `key` would.
    fn seek_phase(
        heap: &mut LoserTree,
        sources: &[Run],
        states: &mut [PosCursor],
        key: &[u8],
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        while let Some(HeapNode { source_idx: src, row }) = heap.peek() {
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

    /// Position on `key`'s PK group; `true` when the cursor stands on it. Across calls,
    /// keys must strictly ascend, the first at or above where the cursor was positioned.
    pub(crate) fn seek_pk_group_ascending(&mut self, key: &[u8]) -> bool {
        if !self.valid {
            return false;
        }
        match self.current_pk_cmp_bytes(key) {
            Ordering::Greater => false,
            Ordering::Equal => true,
            Ordering::Less => {
                self.advance_to_forward(key);
                self.valid && self.current_pk_eq(key)
            }
        }
    }

    /// PK region of the current row as raw bytes, without copying. The single PK
    /// accessor for any width — correct for compound/wide PKs.
    pub fn current_pk_bytes(&self) -> &[u8] {
        self.sources[self.current_entry_idx].get_pk_bytes(self.current_row)
    }

    /// Whether the current row came out of a capacity-bounded view's skeleton
    /// shard — a (PK, coarse weight) pair whose payload columns do not exist on
    /// disk. Its caller must hydrate that key from the view's own traces or source
    /// store instead of copying the row.
    ///
    /// One indexed load: `commit_emitted` records the emitted row's source in
    /// both modes, single-source bypass included.
    #[inline]
    pub(crate) fn current_is_skeleton(&self) -> bool {
        debug_assert!(self.valid, "current_is_skeleton on an invalid cursor");
        self.sources[self.current_entry_idx].is_skeleton()
    }

    /// The current row as a `(source, row)` pair for the shared [`RowSource`]
    /// kernels. The source is the row's own entry, so its blob arena backs the
    /// row's German strings. The bound is `RowSource`, not `ColumnarSource`,
    /// because a cursor's weight comes off the cursor — a merged group's net
    /// weight is not the positioned source's stored weight.
    #[inline]
    pub fn current_row_source(&self) -> (&impl RowSource, usize) {
        debug_assert!(self.valid, "current_row_source on an invalid cursor");
        (&self.sources[self.current_entry_idx], self.current_row)
    }

    /// The current row's PK as its native scalar value. Only narrow
    /// (`pk_stride ≤ 16`) relations have one; panics above that width.
    #[inline]
    pub fn current_key_narrow(&self) -> u128 {
        debug_assert!(self.valid, "current_key_narrow on an invalid cursor");
        let bytes = self.current_pk_bytes();
        assert!(bytes.len() <= gnitz_wire::NARROW_PK_MAX_BYTES, "narrow PK cursor");
        gnitz_wire::widen_pk_be(bytes)
    }

    /// Whether the current row's PK equals the group's OPK `key_bytes`.
    #[inline]
    pub fn current_pk_eq(&self, key_bytes: &[u8]) -> bool {
        pk_bytes_eq(self.current_pk_bytes(), key_bytes)
    }

    /// Walk the equal-`key` PK group from wherever the cursor stands, invoking
    /// `f` at each emitted row; on return the cursor sits past the group.
    ///
    /// Not [`Self::for_each_row_while`] with an equality `cont`: `gnitz-store`
    /// builds at `opt-level = 0` in dev, where that closure is a real call per row.
    pub(crate) fn for_each_pk_group_row<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], f: F) {
        with_payload_cmp!(self.schema, Self::for_each_pk_group_row_with::<_, _>, self, key, f);
    }

    #[inline]
    fn for_each_pk_group_row_with<P: PayloadOrder, F: FnMut(&ReadCursor)>(&mut self, key: &[u8], mut f: F, payload: P) {
        while self.valid && self.current_pk_eq(key) {
            f(&*self);
            self.advance_with(payload);
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
    fn for_each_mem_row_weight_with<F: FnMut(usize, i64), P: PayloadOrder>(
        &mut self,
        mb: &MemBatch,
        range: Range<usize>,
        mut f: F,
        payload: P,
    ) {
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
                match payload.compare(
                    &self.schema,
                    &self.sources[self.current_entry_idx],
                    self.current_row,
                    mb,
                    i,
                ) {
                    Ordering::Less => self.advance_with(payload),
                    Ordering::Equal => {
                        let w = self.current_weight;
                        self.advance_with(payload);
                        break w;
                    }
                    Ordering::Greater => break 0,
                }
            };
            f(i, w);
        }
    }

    /// Walk forward while `cont` holds, calling `f` on each row; on return the
    /// cursor sits ON the first row `cont` rejected, or is exhausted. Seek-free.
    /// Selects the payload comparator once, where [`Self::advance`] re-runs that
    /// dispatch per call in merge mode.
    pub(crate) fn for_each_row_while<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor)>(&mut self, cont: C, f: F) {
        with_payload_cmp!(self.schema, Self::for_each_row_while_with::<_, _, _>, self, cont, f);
    }

    /// Threads the selected `payload` order through each advance, so the per-row
    /// step never re-selects it.
    #[inline]
    fn for_each_row_while_with<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor), P: PayloadOrder>(
        &mut self,
        cont: C,
        mut f: F,
        payload: P,
    ) {
        while self.valid && cont(self.current_pk_bytes()) {
            f(&*self);
            self.advance_with(payload);
        }
    }

    /// Storage-order comparison of the current row's PK against an OPK `key`.
    /// Correct at every PK width.
    #[inline]
    fn current_pk_cmp_bytes(&self, key: &[u8]) -> Ordering {
        compare_pk_ordering(self.current_pk_bytes(), key)
    }

    /// Position the cursor at the first row whose PK begins with `prefix` and
    /// whose weight is `> 0`. Returns `true` on a hit (cursor stays positioned;
    /// `current_pk_bytes`, `current_weight` etc. are valid). Returns `false` on
    /// miss (cursor may be past the prefix range or fully invalid).
    ///
    /// `prefix` is the OPK image of the leading PK column(s), zero-padded to
    /// `pk_stride`: 0x00 is the OPK minimum for every PK type (signed MIN maps to
    /// all-zeros after the sign flip), so the padded key is the correct lower bound
    /// for the suffix columns. The post-seek walk steps rather than re-seeks — a
    /// re-seek would re-find the row already consumed and spin forever.
    pub fn seek_first_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
        let stride = self.schema.pk_stride();
        self.advance_to(PkBuf::from_bytes(prefix).padded(stride));
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

    /// Visit every positive-weight row whose PK begins with `prefix`, invoking
    /// `f(&*self)` at each.
    pub(crate) fn for_each_positive_with_prefix<F: FnMut(&ReadCursor)>(&mut self, prefix: &[u8], f: F) {
        if !self.seek_first_positive_with_prefix(prefix) {
            return;
        }
        self.for_each_positive_while(|pk| pk.starts_with(prefix), f);
    }

    /// [`Self::for_each_positive_with_prefix`] stopping once `f` has been
    /// invoked `max` times. A non-unique index's span names every row holding
    /// the value, so a caller with a row budget pays for what it returns rather
    /// than for the whole group.
    pub fn for_each_positive_with_prefix_capped<F: FnMut(&ReadCursor)>(&mut self, prefix: &[u8], max: usize, mut f: F) {
        if max == 0 || !self.seek_first_positive_with_prefix(prefix) {
            return;
        }
        // `cont` is `Fn`, and runs before each row including the ones the weight
        // gate drops, so the count lives in a `Cell` and is bumped by `f`.
        let taken = std::cell::Cell::new(0usize);
        self.for_each_positive_while(
            |pk| pk.starts_with(prefix) && taken.get() < max,
            |c| {
                f(c);
                taken.set(taken.get() + 1);
            },
        );
    }

    /// [`Self::for_each_row_while`] with the weight gate applied — the one place
    /// the two positive-row walks below spell `current_weight > 0`.
    pub(crate) fn for_each_positive_while<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor)>(&mut self, cont: C, mut f: F) {
        self.for_each_row_while(cont, |c| {
            if c.current_weight > 0 {
                f(c);
            }
        });
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
        with_payload_cmp!(self.schema, Self::advance_merge_with, self)
    }

    /// Commit (or invalidate from) the `(source_idx, row, net_weight)` an advance
    /// emitted — the one writer of the `current_*` state, for both modes. The PK
    /// stays as `(current_entry_idx, current_row)` and is decoded on demand.
    #[inline]
    fn commit_emitted(&mut self, emitted: Option<(usize, usize, i64)>) {
        if let Some((idx, row, weight)) = emitted {
            self.valid = true;
            self.current_weight = weight;
            self.current_entry_idx = idx;
            self.current_row = row;
        } else {
            self.valid = false;
        }
    }

    /// Single-live-source bypass: no heap, no ghost filter. Every other source's
    /// window is empty, and a run arrives already folded, so the position is either
    /// emit-ready or past-end.
    #[inline]
    fn advance_single(&mut self, i: usize) {
        let state = &mut self.states[i];
        let emitted = state.is_valid().then(|| {
            let row = state.position;
            state.position += 1;
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

    /// The five-field destructure both merge-mode walks need before handing the
    /// tournament to [`merge::drive`]: `advance_merge_with` takes one group,
    /// `output::drain_sorted_into_with` a whole chunk.
    #[inline(always)]
    pub(super) fn drive<P: PayloadOrder>(
        &mut self,
        payload: P,
        emit: impl FnMut(usize, usize, i64) -> std::ops::ControlFlow<()>,
    ) {
        let ReadCursor {
            tree,
            sources,
            states,
            schema,
            any_skeleton,
            ..
        } = &mut *self;
        let coarsen = *any_skeleton;
        merge::drive(tree, schema, sources, states, payload, coarsen, emit);
    }

    /// Merge-mode advance (`self.mode.is_none()`), monomorphized on payload.
    /// [`merge::drive`] folds tied rows; `emit` `Break`s on the first non-ghost
    /// group, so ghosts are passed over rather than surfaced.
    #[inline(always)]
    fn advance_merge_with<P: PayloadOrder>(&mut self, payload: P) {
        // The emitted group comes back as a tuple rather than being written to
        // `self.current_*` in place: the drive already holds `&mut self`, so the
        // closure cannot.
        let mut emitted: Option<(usize, usize, i64)> = None;
        self.drive(payload, |gs, gr, nw| {
            emitted = Some((gs, gr, nw));
            std::ops::ControlFlow::Break(())
        });
        self.commit_emitted(emitted);
    }

    /// Upper bound on the rows a walk from HERE emits — the pre-size a batch that
    /// walk fills wants. The per-source windows (so a range seek's clamp is in
    /// it) PLUS the row the cursor sits on: a drive CONSUMES the group it emits,
    /// so `position` is already past a row still to be visited.
    pub(crate) fn estimated_length(&self) -> usize {
        let ahead: usize = self.states.iter().map(|s| s.count.saturating_sub(s.position)).sum();
        ahead + usize::from(self.valid)
    }
}

/// Build a ReadCursor over `runs`, skipping empty ones, and position it on the
/// first live PK group. Every run must be folded. `cap` is an allocation hint
/// for the source vectors.
pub(crate) fn from_runs(runs: impl IntoIterator<Item = Run>, schema: SchemaDescriptor, cap: usize) -> ReadCursor {
    build(runs, schema, cap, ReadCursor::rebuild_and_advance)
}

/// [`from_runs`] positioned on the first live row `>= key`.
pub(crate) fn from_runs_at(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    cap: usize,
    key: &[u8],
) -> ReadCursor {
    build(runs, schema, cap, |c| c.reposition_to(key))
}

/// [`from_runs`] positioned on the OPK band `[start, end)`, and its raw entry count
/// as [`ReadCursor::seek_range_bytes`] reports it.
pub(crate) fn from_runs_in_band(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    cap: usize,
    start: &[u8],
    end: Option<&[u8]>,
) -> (ReadCursor, usize) {
    let mut raw = 0;
    let cursor = build(runs, schema, cap, |c| raw = c.seek_range_bytes(start, end));
    (cursor, raw)
}

/// [`from_runs`] without the initial positioning: the cursor stays invalid until
/// something seeks it.
pub(crate) fn from_runs_unpositioned(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    cap: usize,
) -> ReadCursor {
    build(runs, schema, cap, |_| {})
}

#[inline]
fn build(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    cap: usize,
    position: impl FnOnce(&mut ReadCursor),
) -> ReadCursor {
    let mut sources = Vec::with_capacity(cap);
    let mut states = Vec::with_capacity(cap);
    for run in runs {
        debug_assert!(
            !matches!(&run, Run::Mem(b) if !b.is_consolidated()),
            "a cursor run must be folded"
        );
        let count = run.row_count();
        if count > 0 {
            sources.push(run);
            states.push(PosCursor::new(count));
        }
    }
    ReadCursor::new(sources, states, schema, position)
}

/// A cursor over nothing, in `schema`'s shape.
pub(crate) fn empty_cursor(schema: SchemaDescriptor) -> ReadCursor {
    from_runs(std::iter::empty(), schema, 0)
}

/// Test-only shorthand for [`from_runs`] over a batch slice and a shard slice,
/// folding any batch that is not yet folded.
#[cfg(test)]
pub(crate) fn create_read_cursor(
    batches: &[Rc<Batch>],
    shard_arcs: &[Rc<MappedShard>],
    schema: SchemaDescriptor,
) -> ReadCursor {
    from_runs(
        batches
            .iter()
            .map(|b| Run::Mem(Batch::consolidate_if_needed(b, &schema).map_or_else(|| Rc::clone(b), Rc::new)))
            .chain(shard_arcs.iter().cloned().map(Run::Shard)),
        schema,
        batches.len() + shard_arcs.len(),
    )
}

#[cfg(test)]
mod bench;
#[cfg(test)]
#[path = "../tests/read_cursor.rs"]
mod tests;
