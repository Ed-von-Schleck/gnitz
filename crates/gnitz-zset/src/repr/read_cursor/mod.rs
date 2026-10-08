//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped).

use std::cmp::Ordering;
use std::ops::{ControlFlow, Range};

use crate::repr::loser_tree::{HeapNode, LoserTree};
use crate::repr::merge::MemBatch;
use crate::repr::merge::{self, ColumnarSource, MergeOrder, PosCursor};
use crate::repr::seek::gallop_by;
use crate::schema::key::{compare_pk_ordering, pk_bytes_eq};
use crate::schema::payload_order::{with_payload_cmp, PayloadOrder};
use crate::schema::SchemaDescriptor;
use gnitz_wire::PkBuf;

mod gather;
mod output;
mod source;

use super::run::Run;
pub use gather::PkSetGather;
use gnitz_wire::RowSource;
pub use source::{BoundedIndexCursor, SourceCursor};

/// The skeleton rows a split drain set aside instead of copying: their OPK keys, flat
/// and ascending, and — in debug builds — each one's coarse weight.
#[derive(Default)]
pub struct SkeletonKeys {
    pub keys: Vec<u8>,
    pub coarse: Vec<i64>,
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
    fn assert_none(&self) {
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
    /// merge runs with payload coarsening on (see [`MergeOrder`]).
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

    /// The merge order of a cursor over `schema`, coarsened iff a source is a
    /// skeleton run. Keyed by the PK's leading bytes: finding a shared prefix
    /// would read every run's last row at each open.
    fn order(schema: &SchemaDescriptor, any_skeleton: bool) -> MergeOrder {
        MergeOrder::leading(schema.pk_stride(), any_skeleton)
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
    /// A leaf is keyed under [`Self::order`] and ties go to
    /// [`merge::merge_less`], the pair the advance and forward-seek paths step
    /// the tree through, so a tree maintained in place cannot order rows
    /// differently from how it was played.
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
                |i| {
                    states[i].is_valid().then(|| {
                        (
                            states[i].position as u32,
                            Self::order(schema, *any_skeleton).key(sources, i, states[i].position),
                        )
                    })
                },
                merge::merge_less(schema, sources, Self::order(schema, *any_skeleton), payload),
            );
        }
        self.advance_merge_with(payload);
    }

    /// Position the cursor on the half-open OPK range `[start, end)`, clamping every
    /// source at `end` for the cursor's life, `rewind` included.
    pub(crate) fn seek_range_bytes(&mut self, start: &[u8], end: Option<&[u8]>) {
        debug_assert_eq!(start.len(), self.schema.pk_stride());
        debug_assert!(end.is_none_or(|e| e.len() == self.schema.pk_stride()));
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.position = src.find_lower_bound_bytes(start);
            if let Some(end) = end {
                state.count = state.count.min(src.find_lower_bound_bytes(end));
            }
        }
        self.rebuild_and_advance();
    }

    /// Reset every source to its first row, positioning the cursor at the first
    /// row in storage order — by row index, so no key has to be spelled.
    #[cfg(test)]
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
    /// is what makes it the tests' independent oracle.
    #[cfg(test)]
    pub(crate) fn seek_bytes(&mut self, key: &[u8]) {
        self.seek_range_bytes(key, None);
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
        let less = merge::merge_less(schema, sources, Self::order(schema, *any_skeleton), payload);
        Self::seek_phase(
            tree,
            sources,
            Self::order(schema, *any_skeleton),
            states,
            |s: &Run, r: usize| compare_pk_ordering(s.get_pk_bytes(r), key) == Ordering::Less,
            |s: &Run, pos: usize| s.advance_to(key, pos),
            &less,
        );
        self.advance_merge_with(payload);
    }

    /// Advance every laggard head (`lags` its row) to its own lower bound via
    /// `gallop`, restoring the loser tree with one `step_top` per gallop. Only the
    /// root can lag — it is the global min — so the loop gallops it alone, and
    /// leaves the tree positioned exactly as a from-scratch rebuild at the bound
    /// would.
    fn seek_phase(
        heap: &mut LoserTree,
        sources: &[Run],
        order: MergeOrder,
        states: &mut [PosCursor],
        lags: impl Fn(&Run, usize) -> bool,
        gallop: impl Fn(&Run, usize) -> usize,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        while let Some(HeapNode { source_idx: src, row, .. }) = heap.peek() {
            let (src, row) = (src as usize, row as usize);
            // The root is the global min, so once it no longer lags neither does any head.
            if !lags(&sources[src], row) {
                break;
            }
            states[src].position = gallop(&sources[src], states[src].position);
            let next = states[src].is_valid().then(|| {
                (
                    states[src].position as u32,
                    order.key(sources, src, states[src].position),
                )
            });
            heap.step_top(next, less);
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
                match key.len() == stride {
                    true => self.advance_to_forward(key),
                    // The prefix over an all-zero rest is its group's lower bound.
                    false => self.advance_to_forward(PkBuf::from_bytes(key).widened(stride).pk_bytes()),
                }
                self.valid && self.current_pk_starts_with(key)
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

    /// The current row as `(source index, row)`: with [`Self::source_at`], a
    /// handle on the row that stays readable after the cursor moves on.
    #[inline]
    pub(crate) fn current_position(&self) -> (usize, usize) {
        debug_assert!(self.valid, "current_position on an invalid cursor");
        (self.current_entry_idx, self.current_row)
    }

    /// The source [`Self::current_position`] indexes.
    #[inline(always)]
    pub(crate) fn source_at(&self, idx: usize) -> &Run {
        &self.sources[idx]
    }

    /// The current row's PK as its image: its OPK bytes read as a big-endian
    /// integer. Only narrow (`pk_stride ≤ 16`) relations have one; panics above
    /// that width.
    #[inline]
    pub fn current_key_narrow(&self) -> u128 {
        debug_assert!(self.valid, "current_key_narrow on an invalid cursor");
        let bytes = self.current_pk_bytes();
        assert!(bytes.len() <= gnitz_wire::NARROW_PK_MAX_BYTES, "narrow PK cursor");
        gnitz_wire::widen_pk_be(bytes)
    }

    /// Whether the current row's PK equals the group's OPK `key_bytes`.
    #[inline]
    pub(crate) fn current_pk_eq(&self, key_bytes: &[u8]) -> bool {
        pk_bytes_eq(self.current_pk_bytes(), key_bytes)
    }

    /// Whether the current row's PK begins with `key`, a whole PK included.
    #[inline]
    fn current_pk_starts_with(&self, key: &[u8]) -> bool {
        pk_bytes_eq(&self.current_pk_bytes()[..key.len()], key)
    }

    /// Walk `key`'s PK group — as [`Self::seek_pk_group_ascending`] reads a key —
    /// from wherever the cursor stands, invoking `f` at each emitted row; on
    /// return the cursor sits past the group.
    ///
    /// Not [`Self::for_each_row_while`] with an equality `cont`: this crate
    /// builds at `opt-level = 0` in dev, where that closure is a real call per row.
    pub(crate) fn for_each_pk_group_row<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], f: F) {
        with_payload_cmp!(self.schema, Self::for_each_pk_group_row_with::<_, _>, self, key, f);
    }

    #[inline]
    fn for_each_pk_group_row_with<P: PayloadOrder, F: FnMut(&ReadCursor)>(&mut self, key: &[u8], mut f: F, payload: P) {
        while self.valid && self.current_pk_starts_with(key) {
            f(&*self);
            self.advance_with(payload);
        }
    }

    /// `f(i, w)` for every row `i` of the (PK, payload)-sorted `mb`, `w` the net
    /// weight of the trace element equal to row `i` in (PK, payload), or 0. Rows
    /// are probed in order, each moving the cursor forward from where the last left it.
    pub(crate) fn for_each_mem_row_weight<F: FnMut(usize, i64)>(&mut self, mb: &MemBatch, f: F) {
        with_payload_cmp!(self.schema, Self::for_each_mem_row_weight_with::<_, _>, self, mb, f);
    }

    #[inline]
    fn for_each_mem_row_weight_with<F: FnMut(usize, i64), P: PayloadOrder>(
        &mut self,
        mb: &MemBatch,
        mut f: F,
        payload: P,
    ) {
        // A skeleton run folds a PK group regardless of payload; no history holds one.
        debug_assert!(!self.any_skeleton, "element walk over a skeleton store");
        for i in 0..mb.count {
            let w = self.weight_of_with(mb, i, payload);
            f(i, w);
        }
    }

    /// `mb[i]`'s weight at or after the cursor, consuming the element on a match.
    #[inline]
    fn weight_of_with<P: PayloadOrder>(&mut self, mb: &MemBatch, i: usize, payload: P) -> i64 {
        let key = mb.get_pk_bytes(i);
        // `advance_to_forward`, with the payload order already selected.
        if self.valid && self.current_pk_cmp_bytes(key) == Ordering::Less {
            match self.mode {
                None => self.seek_forward_merge_with(key, payload),
                Some(_) => self.reposition_to(key),
            }
        }
        if !self.valid || !self.current_pk_eq(key) {
            return 0;
        }
        let mut ord = self.cmp_current_payload(mb, i, payload);
        if ord == Ordering::Less {
            self.seek_element_forward_with(mb, i, payload);
            if !self.valid || !self.current_pk_eq(key) {
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
        payload.compare(
            &self.schema,
            &self.sources[self.current_entry_idx],
            self.current_row,
            mb,
            i,
        )
    }

    /// Inside `mb[i]`'s PK group, forward-seek to the first live element `>= mb[i]`.
    /// Every head already sits at or above the cursor's element, which shares the PK.
    fn seek_element_forward_with<P: PayloadOrder>(&mut self, mb: &MemBatch, i: usize, payload: P) {
        let key = mb.get_pk_bytes(i);
        let ReadCursor {
            tree,
            sources,
            states,
            schema,
            any_skeleton,
            mode,
            ..
        } = &mut *self;
        let (sources, schema) = (&*sources, &*schema);
        let lags = |s: &Run, r: usize| {
            compare_pk_ordering(s.get_pk_bytes(r), key).then_with(|| payload.compare(schema, s, r, mb, i))
                == Ordering::Less
        };
        let gallop = |s: &Run, pos: usize| gallop_by(s.row_count(), pos, |r| lags(s, r));
        match *mode {
            // The other sources' windows are empty, and a forward seek keeps them so.
            Some(src) => states[src].position = gallop(&sources[src], states[src].position),
            None => {
                let less = merge::merge_less(schema, sources, Self::order(schema, *any_skeleton), payload);
                Self::seek_phase(
                    tree,
                    sources,
                    Self::order(schema, *any_skeleton),
                    states,
                    lags,
                    gallop,
                    &less,
                );
            }
        }
        self.advance_with(payload);
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

    /// Visit every positive-weight row whose PK begins with `prefix`, invoking
    /// `f(&*self)` at each.
    pub(crate) fn for_each_positive_with_prefix<F: FnMut(&ReadCursor)>(&mut self, prefix: &[u8], mut f: F) {
        self.for_each_positive_with_prefix_until(prefix, |c| {
            f(c);
            ControlFlow::Continue(())
        });
    }

    /// [`Self::for_each_positive_with_prefix`] until `f` breaks, which leaves the
    /// cursor on that row. `true` iff `f` broke.
    ///
    /// `prefix` is the OPK image of the leading PK column(s). Zero-padded to
    /// `pk_stride` it is the least key beginning with it: 0x00 is the OPK minimum
    /// for every PK type (signed MIN maps to all-zeros after the sign flip).
    pub(crate) fn for_each_positive_with_prefix_until<F: FnMut(&ReadCursor) -> ControlFlow<()>>(
        &mut self,
        prefix: &[u8],
        f: F,
    ) -> bool {
        let stride = self.schema.pk_stride();
        self.advance_to(PkBuf::from_bytes(prefix).widened(stride).pk_bytes());
        self.walk_positive_with_prefix_until(prefix, f)
    }

    /// [`Self::for_each_positive_with_prefix_until`] from where the cursor
    /// stands. Selects the payload comparator once, as [`Self::for_each_row_while`]
    /// does.
    pub(crate) fn walk_positive_with_prefix_until<F: FnMut(&ReadCursor) -> ControlFlow<()>>(
        &mut self,
        prefix: &[u8],
        f: F,
    ) -> bool {
        with_payload_cmp!(
            self.schema,
            Self::walk_positive_with_prefix_with::<_, _>,
            self,
            prefix,
            f
        )
    }

    #[inline]
    fn walk_positive_with_prefix_with<F: FnMut(&ReadCursor) -> ControlFlow<()>, P: PayloadOrder>(
        &mut self,
        prefix: &[u8],
        mut f: F,
        payload: P,
    ) -> bool {
        while self.valid && self.current_pk_bytes().starts_with(prefix) {
            if self.current_weight > 0 && f(&*self).is_break() {
                return true;
            }
            self.advance_with(payload);
        }
        false
    }

    /// [`Self::for_each_row_while`] with the weight gate applied — the one place
    /// the two positive-row walks below spell `current_weight > 0`.
    pub fn for_each_positive_while<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor)>(&mut self, cont: C, mut f: F) {
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
        let order = Self::order(schema, *any_skeleton);
        merge::drive(tree, schema, sources, order, states, payload, emit);
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
    pub fn estimated_length(&self) -> usize {
        let ahead: usize = self.states.iter().map(|s| s.count.saturating_sub(s.position)).sum();
        ahead + usize::from(self.valid)
    }
}

/// Build a ReadCursor over `runs`, skipping empty ones, and position it on the
/// first live PK group. Every run must be folded. `cap` is an allocation hint
/// for the source vectors.
pub fn from_runs(runs: impl IntoIterator<Item = Run>, schema: SchemaDescriptor, cap: usize) -> ReadCursor {
    build(runs, schema, cap, ReadCursor::rebuild_and_advance)
}

/// [`from_runs`] positioned on the OPK band `[start, end)`, as
/// [`ReadCursor::seek_range_bytes`].
pub fn from_runs_in_band(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    cap: usize,
    start: &[u8],
    end: Option<&[u8]>,
) -> ReadCursor {
    build(runs, schema, cap, |c| c.seek_range_bytes(start, end))
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
    // `for_each`, not `for`: the stores hand in nested adaptors, which fold
    // far cheaper than they step.
    runs.into_iter().for_each(|run| {
        debug_assert!(
            !matches!(&run, Run::Mem(b) if !b.is_consolidated()),
            "a cursor run must be folded"
        );
        let count = run.row_count();
        if count > 0 {
            sources.push(run);
            states.push(PosCursor::new(count));
        }
    });
    ReadCursor::new(sources, states, schema, position)
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
