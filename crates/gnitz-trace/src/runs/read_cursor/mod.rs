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

use crate::runs::MappedShard;
use gnitz_zset::repr::gallop_by;
use gnitz_zset::repr::{Batch, MemBatch};
use gnitz_zset::repr::{ColumnarSource, Pick, span_of};
use gnitz_zset::repr::{RunMerge, merge_consolidated};
use gnitz_zset::repr::{recycle, take, vec_bytes};
use gnitz_zset::schema::{PayloadOrder, SchemaDescriptor, compare_pk_ordering, group_floor, with_payload_cmp};

mod gather;
mod output;
mod probe;

use super::run::Run;
pub use gather::PkSetGather;
use gnitz_wire::{PkKeys, RowSource};
pub(crate) use probe::GroupProbe;

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

    /// Move every skeleton row of `picks`, spans included, to its key, keeping
    /// the rest in order; `sources` are the runs the picks name.
    fn split_from(&mut self, sources: &[Run], picks: &mut Vec<Pick>) {
        picks.retain(|&pick| match span_of(pick) {
            Some((s, rows)) => match &sources[s] {
                Run::Shard(shard) if shard.is_skeleton() => {
                    self.extend(shard, rows);
                    false
                }
                _ => true,
            },
            None => {
                let src = &sources[pick.0 as usize];
                if src.is_skeleton() {
                    self.push(src.get_pk_bytes(pick.1 as usize), pick.2);
                }
                !src.is_skeleton()
            }
        });
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

/// `live`, a split drain's chunk, with the skeleton rows it set aside in
/// `skeletons` replaced by their keys' rows, which `hydrate` recomputes from
/// the keys, ascending; `live` itself where it set none aside.
fn hydrate_skeletons(
    live: Batch,
    skeletons: SkeletonKeys,
    hydrate: &mut impl FnMut(PkKeys) -> Result<Batch, String>,
) -> Result<Batch, String> {
    if skeletons.keys.is_empty() {
        return Ok(live);
    }
    let keys = PkKeys::from_sorted(live.schema().pk_stride(), skeletons.keys);
    #[cfg(debug_assertions)]
    let asked = keys.clone();
    let hydrated = hydrate(keys)?;
    #[cfg(debug_assertions)]
    assert_hydration_matches(&hydrated, &asked, &skeletons.coarse);
    if live.is_empty() {
        return Ok(hydrated);
    }
    // Both consolidated and PK-disjoint.
    Ok(merge_consolidated(
        &[hydrated.as_mem_batch(), live.as_mem_batch()],
        live.schema(),
    ))
}

/// Tripwire: a hydration's rows sum, per PK, to the coarse weight the skeleton row
/// at that key carried — by linearity of the PK projection — and name exactly the
/// keys asked.
#[cfg(debug_assertions)]
fn assert_hydration_matches(out: &Batch, keys: &PkKeys, coarse: &[i64]) {
    assert_eq!(keys.len(), coarse.len());
    let mut got = Vec::new();
    let mut i = 0;
    while i < out.len() {
        let stride = out.schema().pk_stride();
        let j = gnitz_zset::repr::pk_group_end(out.pk(), stride, i, stride);
        got.push((out.get_pk_bytes(i), out.as_mem_batch().sum_weights(i, j)));
        i = j;
    }
    let want: Vec<_> = keys.iter().zip(coarse.iter().copied()).collect();
    assert_eq!(
        got, want,
        "hydration off its skeleton rows: (pk, summed weight) per key"
    );
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
    /// The N-way merge over `sources`, re-played in place by each reposition.
    /// Its walks end at the row count or a range seek's clamp for the cursor's
    /// life. Its order is keyed by the PK's leading bytes: finding a shared
    /// prefix would read every run's last row at each open; and it coarsens iff
    /// a source is a capacity-bounded view's skeleton shard. Its tournament is
    /// idle while `mode` is `Some`.
    merge: RunMerge,
    /// The one source live at the last absolute reposition, walked without the
    /// tournament. `None` drives it, and may be pessimistic: only a reposition
    /// re-derives it. A forward seek keeps a single source single.
    mode: Option<usize>,
    schema: SchemaDescriptor,
    /// The drain's per-source row windows, reused across chunks.
    drain_windows: Vec<Range<usize>>,
    /// The row the cursor stands on, at its group's net weight; `None` once
    /// exhausted. Its source's walk is already past it.
    current: Option<Pick>,
    /// See [`Self::skip_payload`].
    skip_payload: u64,
}

impl ReadCursor {
    /// Let every drain from here on write the payload columns `skip` names, as
    /// payload bits, as zeroes that are not NULL instead of decoding them: for a
    /// reader that never reads them. A drain of one in-memory run alone copies
    /// them as they are. Only over a store of one live row per PK, whose chunks
    /// stay consolidated with columns zeroed.
    pub fn skip_payload(&mut self, skip: u64) {
        self.skip_payload = skip;
    }

    /// Re-derive the drive mode after the per-source positions moved, then advance
    /// to the first live row. Re-derived rather than the source set being
    /// narrowed: a seek is backward-capable, so a later one at a lower key can
    /// re-liven a source an earlier range seek emptied.
    fn rebuild_and_advance(&mut self) {
        let mut live = (self.merge.cursors().iter().enumerate()).filter(|(_, st)| !st.is_empty());
        self.mode = match (live.next(), live.next()) {
            (Some((i, _)), None) => Some(i),
            _ => None,
        };
        match self.mode {
            Some(i) => self.advance_single(i),
            None => with_payload_cmp!(self.schema, |p| self.rebuild_and_advance_merge_with(p)),
        }
    }

    /// One payload dispatch for the tournament re-play and the advance after it.
    /// Out of line: a replay costs far more than the call, and inlined it
    /// would grow every reposition, the single-source ones included.
    #[inline(never)]
    fn rebuild_and_advance_merge_with<P: PayloadOrder>(&mut self, payload: P) {
        self.merge.rebuild(&self.schema, &self.sources, payload);
        self.advance_merge_with(payload);
    }

    /// Position the cursor on the half-open OPK range `[start, end)`, clamping every
    /// source at `end` for the cursor's life, `rewind` included.
    fn seek_range_bytes(&mut self, start: &[u8], end: Option<&[u8]>) {
        debug_assert_eq!(start.len(), self.schema.pk_stride());
        debug_assert!(end.is_none_or(|e| e.len() == self.schema.pk_stride()));
        for (src, state) in self.sources.iter().zip(self.merge.cursors_mut()) {
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
        for state in self.merge.cursors_mut() {
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
        // Mode before the comparison, so a single-source cursor — whose
        // reposition costs less than the payload dispatch of a gallop — pays
        // for neither.
        if self.mode.is_none() && self.current_pk().is_some_and(|pk| compare_pk_ordering(pk, key).is_lt()) {
            self.skip_below(key, |_, _| {});
        } else {
            self.reposition_to(key);
        }
    }

    /// Seed every source's gallop at its live position, then re-derive the mode —
    /// what every path that cannot maintain the tree in place takes.
    /// Backward-capable: each source searches its full row range.
    fn reposition_to(&mut self, key: &[u8]) {
        for (src, state) in self.sources.iter().zip(self.merge.cursors_mut()) {
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
        let lags = |_: &SchemaDescriptor, s: &Run, r: usize| compare_pk_ordering(s.get_pk_bytes(r), key).is_lt();
        self.gallop_forward_with(payload, lags, |_, s, at| {
            let to = s.seek(key, Some(at.start));
            skipped(s, at.start..to.min(at.end));
            to
        });
    }

    /// Move every walk whose head `lags` to where `gallop` lands it, both
    /// handed the cursor's schema, then stand on the first live group from
    /// there: the one walk where a single source is live, whose others are
    /// empty and stay so, else the laggards of the tournament, kept in place.
    #[inline]
    fn gallop_forward_with<P: PayloadOrder>(
        &mut self,
        payload: P,
        lags: impl Fn(&SchemaDescriptor, &Run, usize) -> bool,
        mut gallop: impl FnMut(&SchemaDescriptor, &Run, &Range<usize>) -> usize,
    ) {
        let ReadCursor { sources, merge, schema, mode, .. } = &mut *self;
        match *mode {
            Some(src) => {
                let walk = &mut merge.cursors_mut()[src];
                walk.start = gallop(schema, &sources[src], walk);
            }
            None => merge.seek_laggards(schema, sources, payload, lags, gallop),
        }
        self.advance_with(payload);
    }

    /// Position on `key`'s PK group; `true` when the cursor stands on it. `key` is
    /// a whole PK, or leading columns of one, whose group is every row it
    /// prefixes. Across calls, keys must strictly ascend, the first at or above
    /// where the cursor was positioned.
    pub(crate) fn seek_pk_group_ascending(&mut self, key: &[u8]) -> bool {
        let Some(pk) = self.current_pk() else {
            return false;
        };
        match compare_pk_ordering(&pk[..key.len()], key) {
            Ordering::Greater => false,
            Ordering::Equal => true,
            Ordering::Less => {
                let stride = self.schema.pk_stride();
                let floor;
                let target = match key.len() == stride {
                    true => key,
                    false => {
                        floor = group_floor(key, stride);
                        floor.pk_bytes()
                    }
                };
                self.skip_below(target, |_, _| {});
                self.current_pk().is_some_and(|pk| pk.starts_with(key))
            }
        }
    }

    /// The schema of the rows the cursor reads.
    #[inline]
    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// Whether the cursor stands on a row. The `current_*` accessors may be
    /// called only while this returns `true`.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    /// The current row's net weight: its group's weights summed across sources.
    #[inline]
    pub fn current_weight(&self) -> i64 {
        self.current_position().2
    }

    /// PK region of the current row as raw bytes, without copying, or `None`
    /// on an exhausted cursor. The single PK accessor for any width — correct
    /// for compound/wide PKs.
    #[inline(always)]
    pub fn current_pk(&self) -> Option<&[u8]> {
        let (src, row, _) = self.current?;
        Some(self.sources[src as usize].get_pk_bytes(row as usize))
    }

    /// [`Self::current_pk`] of a cursor standing on a row.
    pub fn current_pk_bytes(&self) -> &[u8] {
        self.current_pk().expect("current_pk_bytes on an exhausted cursor")
    }

    /// Whether the rows under this cursor's position outnumber its sources: a
    /// row walked costs more than one source's seek.
    #[inline]
    pub fn passed_more_than_a_seek(&self) -> bool {
        self.merge.cursors().iter().map(|s| s.start).sum::<usize>() > self.sources.len()
    }

    /// The current row as a `(source, row)` pair for the shared [`RowSource`]
    /// kernels. The source is the row's own entry, so its blob arena backs the
    /// row's German strings. The bound is `RowSource`, not `ColumnarSource`,
    /// because a cursor's weight comes off the cursor — a merged group's net
    /// weight is not the positioned source's stored weight.
    #[inline]
    pub fn current_row_source(&self) -> (&impl RowSource, usize) {
        self.row_at(self.current_position())
    }

    /// The current row at its net weight, as a handle that stays readable
    /// through [`Self::row_at`] after the cursor moves on.
    #[inline(always)]
    pub(crate) fn current_position(&self) -> Pick {
        self.current.expect("a current_* accessor on an exhausted cursor")
    }

    /// The source and row of a [`Pick`] of this cursor's sources.
    #[inline(always)]
    pub(crate) fn row_at(&self, (src, row, _): Pick) -> (&Run, usize) {
        (&self.sources[src as usize], row as usize)
    }

    /// Walk `key`'s PK group — as [`Self::seek_pk_group_ascending`] reads a key —
    /// from wherever the cursor stands, invoking `f` at each emitted row; on
    /// return the cursor sits past the group.
    #[inline]
    pub(crate) fn for_each_pk_group_row<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], f: F) {
        self.for_each_row_while(|pk| pk.starts_with(key), f);
    }

    /// Walk forward while `cont` holds of the PK, calling `f` on each row; on
    /// return the cursor sits ON the first row `cont` rejected, or is exhausted.
    /// Seek-free.
    #[inline]
    pub(crate) fn for_each_row_while<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor)>(&mut self, cont: C, mut f: F) {
        self.walk_until(cont, |c| {
            f(c);
            ControlFlow::Continue(())
        });
    }

    /// Walk forward while `cont` holds of the PK, calling `f` on each row until
    /// it breaks, which leaves the cursor on that row; `true` iff `f` broke.
    /// Selects the payload comparator once, where [`Self::advance`] re-runs
    /// that dispatch per call in merge mode.
    #[inline]
    fn walk_until<C: Fn(&[u8]) -> bool, F: FnMut(&ReadCursor) -> ControlFlow<()>>(
        &mut self,
        cont: C,
        mut f: F,
    ) -> bool {
        with_payload_cmp!(self.schema, |payload| {
            while self.current_pk().is_some_and(&cont) {
                if f(&*self).is_break() {
                    return true;
                }
                self.advance_with(payload);
            }
            false
        })
    }

    /// `f(i, w)` for every row `i` of the (PK, payload)-sorted `mb`, `w` the net
    /// weight of the trace element equal to row `i` in (PK, payload), or 0. Rows
    /// are probed in order, each moving the cursor forward from where the last left it.
    pub(crate) fn for_each_mem_row_weight<F: FnMut(usize, i64)>(&mut self, mb: &MemBatch, mut f: F) {
        // A skeleton run folds a PK group regardless of payload; no history holds one.
        debug_assert!(!self.merge.coarsens(), "element walk over a skeleton store");
        with_payload_cmp!(self.schema, |payload| for i in 0..mb.len() {
            let w = self.weight_of_with(mb, i, payload);
            f(i, w);
        })
    }

    /// `mb[i]`'s weight at or after the cursor, consuming the element on a match.
    #[inline]
    fn weight_of_with<P: PayloadOrder>(&mut self, mb: &MemBatch, i: usize, payload: P) -> i64 {
        let key = mb.get_pk_bytes(i);
        if self.current_pk().is_some_and(|pk| compare_pk_ordering(pk, key).is_lt()) {
            self.skip_below_with(key, payload, |_, _| {});
        }
        let Some(pk) = self.current_pk() else { return 0 };
        if compare_pk_ordering(pk, key).is_ne() {
            return 0;
        }
        let mut ord = self.cmp_current_payload(mb, i, payload);
        if ord == Ordering::Less {
            // Inside `mb[i]`'s PK group, forward-seek to the first live element
            // `>= mb[i]`: every head already sits at or above the cursor's
            // element, which shares the PK.
            let lags = |schema: &SchemaDescriptor, s: &Run, r: usize| payload.compare_full(schema, s, r, mb, i).is_lt();
            self.gallop_forward_with(payload, lags, |schema, s, at| {
                gallop_by(s.row_count(), Some(at.start), |r| lags(schema, s, r))
            });
            let Some(pk) = self.current_pk() else { return 0 };
            if compare_pk_ordering(pk, key).is_ne() {
                return 0;
            }
            ord = self.cmp_current_payload(mb, i, payload);
        }
        if ord.is_ne() {
            return 0;
        }
        let w = self.current_weight();
        self.advance_with(payload);
        w
    }

    /// The current row's payload against `mb[i]`'s.
    #[inline]
    fn cmp_current_payload<P: PayloadOrder>(&self, mb: &MemBatch, i: usize, payload: P) -> Ordering {
        let (src, row) = self.row_at(self.current_position());
        payload.compare(&self.schema, src, row, mb, i)
    }

    /// Land on the first positive-weight row whose PK begins with `prefix`, no
    /// longer than the PK; `true` iff there is one.
    pub(crate) fn seek_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
        self.advance_to(group_floor(prefix, self.schema.pk_stride()).pk_bytes());
        self.walk_until(
            |pk| pk.starts_with(prefix),
            |c| match c.current_weight() > 0 {
                true => ControlFlow::Break(()),
                false => ControlFlow::Continue(()),
            },
        )
    }

    /// Call `f` on every remaining row at positive weight.
    pub fn for_each_positive<F: FnMut(&ReadCursor)>(&mut self, mut f: F) {
        self.for_each_row_while(
            |_| true,
            |c| {
                if c.current_weight() > 0 {
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

    /// Single-live-source bypass: no tournament, no ghost filter. Every other source's
    /// window is empty, and a run arrives already folded, so the position is either
    /// emit-ready or past-end.
    #[inline]
    fn advance_single(&mut self, i: usize) {
        let state = &mut self.merge.cursors_mut()[i];
        self.current = (state.start < state.end).then(|| {
            let row = state.start;
            state.start += 1;
            (i as u32, row as u32, self.sources[i].get_weight(row))
        });
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

    /// Merge-mode advance (`self.mode.is_none()`), monomorphized on payload.
    /// [`RunMerge::drive`] folds tied rows; `emit` `Break`s on the first non-ghost
    /// group, so ghosts are passed over rather than surfaced.
    #[inline(always)]
    fn advance_merge_with<P: PayloadOrder>(&mut self, payload: P) {
        let mut emitted = None;
        self.merge
            .drive::<_, _, false>(&self.schema, &self.sources, payload, |src, row, w| {
                emitted = Some((src as u32, row as u32, w));
                ControlFlow::Break(0)
            });
        self.current = emitted;
    }

    /// Upper bound on the rows a walk from HERE emits — the pre-size a batch that
    /// walk fills wants. The per-source windows (so a range seek's clamp is in
    /// it) PLUS the row the cursor sits on: a drive CONSUMES the group it emits,
    /// so `start` is already past a row still to be visited.
    pub fn estimated_length(&self) -> usize {
        let ahead: usize = self.merge.cursors().iter().map(|s| s.len()).sum();
        ahead + usize::from(self.is_valid())
    }
}

/// Build a ReadCursor over `runs`, skipping empty ones, and position it on the
/// first live PK group. Every run must be folded.
pub fn from_runs(runs: impl IntoIterator<Item = Run>, schema: SchemaDescriptor) -> ReadCursor {
    build(runs, schema, ReadCursor::rebuild_and_advance)
}

/// [`from_runs`] positioned on the first live row of the OPK band `[start, end)`,
/// every source clamped at `end` for the cursor's life.
pub fn from_runs_in_band(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    start: &[u8],
    end: Option<&[u8]>,
) -> ReadCursor {
    build(runs, schema, |c| c.seek_range_bytes(start, end))
}

/// [`from_runs`] positioned on the first live row at or above `key`, each
/// source galloping from its first row: cheaper than [`from_runs_in_band`]'s
/// binary search where `key` lies near the bottom of every run.
pub fn from_runs_at(runs: impl IntoIterator<Item = Run>, schema: SchemaDescriptor, key: &[u8]) -> ReadCursor {
    build(runs, schema, |c| c.reposition_to(key))
}

/// Positioned by `position` before it is returned, so no caller moves a cursor it
/// has to mutate first. The source vectors reserve `runs`' lower size bound; past
/// it they grow, and the pool keeps what they grew to.
#[inline]
fn build(
    runs: impl IntoIterator<Item = Run>,
    schema: SchemaDescriptor,
    position: impl FnOnce(&mut ReadCursor),
) -> ReadCursor {
    let runs = runs.into_iter();
    let CursorBufs { mut sources, mut merge, windows } = take(&CURSOR_BUFS, |_| true).unwrap_or_default();
    let (hint, _) = runs.size_hint();
    sources.reserve(hint);
    // `for_each`, not `for`: the stores hand in nested adaptors, which fold
    // far cheaper than they step.
    runs.for_each(|run| {
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
        }
    });
    // `rebuild_and_advance` plays the tournament.
    merge.open(&sources, schema.pk_stride());
    let mut cursor = ReadCursor {
        sources,
        merge,
        drain_windows: windows,
        mode: None,
        schema,
        current: None,
        skip_payload: 0,
    };
    position(&mut cursor);
    cursor
}

/// A dropped cursor's vectors, emptied, for the thread's next [`build`].
#[derive(Default)]
struct CursorBufs {
    sources: Vec<Run>,
    merge: RunMerge,
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
        let mut merge = std::mem::take(&mut self.merge);
        let b = merge.clear(MAX_POOLED_ELEMS);
        let (windows, c) = emptied(std::mem::take(&mut self.drain_windows));
        let bufs = CursorBufs { sources, merge, windows };
        recycle(&CURSOR_BUFS, bufs, a + b + c);
    }
}

/// A cursor over nothing, in `schema`'s shape.
pub fn empty_cursor(schema: SchemaDescriptor) -> ReadCursor {
    from_runs(std::iter::empty(), schema)
}

#[cfg(test)]
#[path = "benches/read_cursor.rs"]
mod bench;
#[cfg(test)]
#[path = "tests/read_cursor.rs"]
mod tests;
