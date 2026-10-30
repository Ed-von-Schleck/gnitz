//! The Z-set merges: the N-way run merge over any [`ColumnarSource`], the
//! two-way galloping batch merge, and in-batch consolidation.
//!
//! Operates on flat columnar buffers: pk[OPK big-endian, `pk_stride` B/row],
//! weight[i64 LE], null_bitmap[u64 LE], payload columns, blob arena.
//!
//! Every merge fuses its walk with consolidation: rows with the same
//! (PK, payload) have their weights summed; rows whose net weight is zero are
//! dropped. [`merge_consolidated`] is Z-Set `+` over consolidated batches, and
//! picks the kernel that computes it.

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{ControlFlow, Range};

use super::batch::{Batch, MemBatch};
use super::loser_tree::{LoserTree, Player};
use super::scatter::{ColumnarSource, Pick};
use super::string_heap::{rebase_string_cells, row_long_bytes, BlobCache};
use super::writer::DirectWriter;
use crate::schema::{pack_pk_be, pk_width_dispatch, with_payload_cmp, PayloadOrder, PkSortKey, SchemaDescriptor};
use gnitz_wire::{native_key_image, Heap, NARROW_PK_MAX_BYTES, REG_PAYLOAD_START, REG_WEIGHT};

// ---------------------------------------------------------------------------
// Z-Set `+` over consolidated batches
// ---------------------------------------------------------------------------

/// Z-set `+` of `sources`, each consolidated, as one consolidated batch.
///
/// Sources that each end below the PK the next begins at share no row and hold
/// none out of order across them: in order, they are the sum, and are
/// concatenated. Otherwise two sources gallop against each other, and so does
/// a dominant one — usually a previous fold's output — against the sum of the
/// rest, so its stretches are bulk-copied; any other mix takes the N-way fold.
pub fn merge_consolidated(sources: &[MemBatch<'_>], schema: &SchemaDescriptor) -> Batch {
    let held = sources.iter().filter(|s| s.count > 0);
    let ascending = held
        .clone()
        .zip(held.skip(1))
        .all(|(a, b)| a.get_pk_bytes(a.count - 1) < b.get_pk_bytes(0));
    let mut out = match ascending {
        true => Batch::concat(schema, sources.iter().copied()),
        false => merge_interleaved(sources, schema),
    };
    out.certify_consolidated();
    out
}

/// [`merge_consolidated`] of sources that interleave, so at least two hold rows.
fn merge_interleaved(sources: &[MemBatch<'_>], schema: &SchemaDescriptor) -> Batch {
    let rows: usize = sources.iter().map(|s| s.count).sum();
    let (big, dominant) = sources
        .iter()
        .enumerate()
        .max_by_key(|(_, s)| s.count)
        .expect("interleaved sources");
    if dominant.count * 2 < rows {
        return merge_rows(sources, schema);
    }
    // The rest keep their order, which is what lets an ascending load's runs
    // sum by concatenation.
    let rest: Vec<MemBatch> = sources
        .iter()
        .enumerate()
        .filter(|&(i, s)| i != big && s.count > 0)
        .map(|(_, s)| *s)
        .collect();
    let summed;
    let other = match &rest[..] {
        [one] => *one,
        rest => {
            summed = merge_consolidated(rest, schema);
            summed.as_mem_batch()
        }
    };
    let srcs = [*dominant, other];
    pk_width_dispatch!(schema.pk_stride(), |K| {
        with_payload_cmp!(schema, |p| two_way::<K, _>(&srcs, schema, p))
    })
}

/// The N-way fold of `sources`, each sorted by (PK, payload). Out of line, so
/// the loop compiles as it does without [`merge_consolidated`]'s tests ahead of
/// it.
#[inline(never)]
fn merge_rows(sources: &[MemBatch<'_>], schema: &SchemaDescriptor) -> Batch {
    let rows = run_merge(sources, schema, sources.iter().map(|s| 0..s.count));
    super::scatter::materialize_carrying(sources, schema, &rows)
}

// ---------------------------------------------------------------------------
// run_merge: the generic N-way (PK, payload) merge + consolidation
// ---------------------------------------------------------------------------

/// The consolidated merge of sorted `sources`, source `i` walked over the `i`-th
/// of `windows`: one [`Pick`] per (PK, payload) group whose weights do not sum to
/// zero, at that sum, in (PK, payload) order.
pub(crate) fn run_merge<S: ColumnarSource>(
    sources: &[S],
    schema: &SchemaDescriptor,
    windows: impl IntoIterator<Item = Range<usize>>,
) -> Vec<Pick> {
    if sources.is_empty() {
        return Vec::new();
    }
    let mut cursors: Vec<Range<usize>> = windows.into_iter().collect();
    debug_assert_eq!(cursors.len(), sources.len(), "run_merge: one window per source");
    debug_assert!(
        cursors.iter().all(|c| c.end < u32::MAX as usize),
        "merge source exceeds the tournament's u32 row"
    );
    debug_assert!(
        schema.num_payload_cols() == 0 || !sources.iter().any(S::is_skeleton),
        "a skeleton row merges under the PK-only schema: this order is never coarsened"
    );
    let mut rows = Vec::with_capacity(cursors.iter().map(|c| c.len()).sum());

    // Dispatch the payload order, monomorphizing one branch-free copy of the
    // merge loop.
    with_payload_cmp!(schema, |payload| {
        let order = MergeOrder::past_shared_prefix(sources, &cursors, schema.pk_stride());
        let mut tree = LoserTree::empty(cursors.len());
        tree.rebuild(
            |i| order.head(sources, i, &cursors[i]),
            merge_less(schema, sources, order, payload),
        );
        drive(
            &mut tree,
            schema,
            sources,
            order,
            &mut cursors,
            payload,
            |src, row, w| {
                rows.push((src as u32, row as u32, w));
                ControlFlow::Continue(())
            },
        );
    });
    rows
}

/// The order of two players whose keys under `order` tie, which [`drive`] and
/// the read cursor share. Generic over the source type, so each caller
/// monomorphizes its own branch-free copy.
#[inline]
pub(crate) fn merge_less<'a, S, P>(
    schema: &'a SchemaDescriptor,
    sources: &'a [S],
    order: MergeOrder,
    payload: P,
) -> impl Fn(&Player, &Player) -> bool + Copy + 'a
where
    S: ColumnarSource,
    P: PayloadOrder + 'a,
{
    let wide = order.has_tail(schema.pk_stride());
    move |a, b| tie_less::<false, S, P>(schema, sources, order, wide, payload, a, b)
}

/// [`merge_less`]'s order, and under `FOLD` its group boundary: of an exemplar
/// and a later root, the root starts a new group exactly when the exemplar
/// orders before it — except that a skeleton row orders before no row of its
/// PK, so under `FOLD` the whole PK group folds into it.
///
/// The PK bytes behind the key span, which only a wide PK has, then the
/// `payload` order — unless `order` coarsens and one row is a skeleton row,
/// which is tested ahead of `payload`, so a skeleton row's absent columns are
/// never read.
#[inline(always)]
fn tie_less<const FOLD: bool, S: ColumnarSource, P: PayloadOrder>(
    schema: &SchemaDescriptor,
    sources: &[S],
    order: MergeOrder,
    wide: bool,
    payload: P,
    a: &Player,
    b: &Player,
) -> bool {
    let (a_src, a_row) = (a.source_idx as usize, a.row as usize);
    let (b_src, b_row) = (b.source_idx as usize, b.row as usize);
    let pk = match wide {
        true => order
            .tail(&sources[a_src], a_row)
            .cmp(order.tail(&sources[b_src], b_row)),
        false => Ordering::Equal,
    };
    match pk {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            if order.coarsen {
                let (sa, sb) = (sources[a_src].is_skeleton(), sources[b_src].is_skeleton());
                if sa || sb {
                    return !FOLD && sa && !sb;
                }
            }
            payload.compare(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Less
        }
    }
}

/// How a merge orders its rows ahead of their payloads.
///
/// The PK bytes it keys its tournament by are all of a PK up to
/// [`NARROW_PK_MAX_BYTES`] wide and that many bytes of a wider one: the bytes
/// ahead of that span are the same in every row the merge reads, and the ones
/// behind it break a key tie.
#[derive(Clone, Copy)]
pub(crate) struct MergeOrder {
    /// Where the key span starts in a wide PK; `None` for a narrow one.
    wide_at: Option<usize>,
    /// Collapse the payload axis at a skeleton row: it sorts before every
    /// hydrated row of its PK, making it the exemplar [`drive`] folds the whole
    /// PK group into. A runtime flag, not a second monomorphisation axis: it is
    /// constant for a merge's life and reached only on a PK tie.
    pub(crate) coarsen: bool,
}

impl MergeOrder {
    /// Keyed by the leading bytes of a `stride`-byte PK.
    pub(crate) fn leading(stride: usize, coarsen: bool) -> Self {
        MergeOrder {
            wide_at: (stride > NARROW_PK_MAX_BYTES).then_some(0),
            coarsen,
        }
    }

    /// Keyed past the prefix every row `cursors` walk of `sources` shares —
    /// what each walk's first and last rows share, a source being sorted — so
    /// that keys of small values in wide columns do not all tie.
    fn past_shared_prefix<S: ColumnarSource>(sources: &[S], cursors: &[Range<usize>], stride: usize) -> Self {
        if stride <= NARROW_PK_MAX_BYTES {
            return Self::leading(stride, false);
        }
        let mut bounds = sources
            .iter()
            .zip(cursors)
            .filter(|(_, c)| !c.is_empty())
            .flat_map(|(s, c)| [s.get_pk_bytes(c.start), s.get_pk_bytes(c.end - 1)]);
        let first = bounds.next().unwrap_or(&[]);
        let shared = |pk: &[u8]| first.iter().zip(pk).take_while(|(a, b)| a == b).count();
        let at = bounds.map(shared).min().unwrap_or(0);
        MergeOrder {
            wide_at: Some(at.min(stride - NARROW_PK_MAX_BYTES)),
            coarsen: false,
        }
    }

    /// Whether a PK has bytes behind the key span. Derived per merge rather than
    /// stored: a wider `MergeOrder`, copied into every comparator, costs the
    /// merge loop more than this check does once.
    #[inline(always)]
    fn has_tail(self, stride: usize) -> bool {
        self.wide_at.is_some_and(|at| at + NARROW_PK_MAX_BYTES < stride)
    }

    /// The tournament leaf of source `src`'s walk `c`: its current row and that
    /// row's key, or `None` once the walk is exhausted.
    #[inline(always)]
    pub(crate) fn head<S: ColumnarSource>(self, sources: &[S], src: usize, c: &Range<usize>) -> Option<(u32, u128)> {
        match c.start < c.end {
            true => Some((c.start as u32, self.key(&sources[src], c.start))),
            false => None,
        }
    }

    /// A row's tournament key.
    #[inline(always)]
    fn key<S: ColumnarSource>(self, source: &S, row: usize) -> u128 {
        let pk = source.get_pk_bytes(row);
        match self.wide_at {
            None => pack_pk_be(pk),
            Some(at) => u128::from_be_bytes(*pk[at..].first_chunk().unwrap()),
        }
    }

    /// The PK bytes behind a row's key, where there [are any](Self::has_tail).
    #[inline(always)]
    fn tail<S: ColumnarSource>(self, source: &S, row: usize) -> &[u8] {
        &source.get_pk_bytes(row)[self.wide_at.unwrap_or(0) + NARROW_PK_MAX_BYTES..]
    }
}

/// Drive an N-way (PK, payload) merge to completion over `sources`, folding each
/// group's weights and calling `emit(group_src, group_row, net_weight)` once per
/// surviving (net ≠ 0) group; a `Break` returns immediately. The output PK is
/// re-derived from `(group_src, group_row)` by the caller.
///
/// Every N-way merge runs through this — [`run_merge`] and the read cursor's
/// advance and drain — and it builds its order and group boundary itself, from
/// the one [`tie_less`].
///
/// `#[inline(always)]`: each caller's `emit` returns a constant `ControlFlow`, so
/// forced inlining folds the branch and drops the unused arm per monomorphisation.
#[inline(always)]
pub(crate) fn drive<S, P>(
    tree: &mut LoserTree,
    schema: &SchemaDescriptor,
    sources: &[S],
    order: MergeOrder,
    cursors: &mut [Range<usize>],
    payload: P,
    mut emit: impl FnMut(usize, usize, i64) -> ControlFlow<()>,
) where
    S: ColumnarSource,
    P: PayloadOrder,
{
    let less = merge_less(schema, sources, order, payload);
    let wide = order.has_tail(schema.pk_stride());
    macro_rules! step {
        ($src:expr) => {{
            let c = &mut cursors[$src];
            c.start += 1;
            tree.step_top(order.head(sources, $src, c), &less);
        }};
    }

    while let Some(group) = tree.peek() {
        let (group_src, group_row, group_key) = (group.source_idx as usize, group.row as usize, group.key());

        // Open the group: take the root's weight and step past it. The first row
        // is the exemplar, so the boundary test would be tautologically false —
        // and its payload term walks every column.
        let mut net_weight: i64 = sources[group_src].get_weight(group_row);
        step!(group_src);

        // Fold tied rows: each iteration peeks the new root, breaks at the group
        // boundary, otherwise accumulates weight and steps again.
        while let Some(top) = tree.peek() {
            let (cur_src, cur_row) = (top.source_idx as usize, top.row as usize);
            if top.key() != group_key || tie_less::<true, S, P>(schema, sources, order, wide, payload, &group, &top) {
                break;
            }
            net_weight += sources[cur_src].get_weight(cur_row);
            step!(cur_src);
        }

        if net_weight != 0 && emit(group_src, group_row, net_weight).is_break() {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// The two-way merge
// ---------------------------------------------------------------------------

/// One batch's PK region, read as keys of the type `K` matching its stride.
struct Keys<'a, K> {
    pk: &'a [u8],
    stride: usize,
    count: usize,
    key: PhantomData<K>,
}

impl<'a, K: PkSortKey<'a>> Keys<'a, K> {
    fn of(batch: &MemBatch<'a>) -> Self {
        Keys {
            pk: batch.pk(),
            stride: batch.pk_stride(),
            count: batch.count,
            key: PhantomData,
        }
    }

    /// # Safety
    /// `row < self.count`.
    #[inline(always)]
    unsafe fn at(&self, row: usize) -> K {
        debug_assert!(row < self.count);
        K::from_opk(unsafe { self.pk.get_unchecked(row * self.stride..(row + 1) * self.stride) })
    }

    /// The first row past `row` whose key is not `key`, which `row`'s is. A
    /// linear step: an equal-PK group is short.
    #[inline(always)]
    fn group_end(&self, row: usize, key: K) -> usize {
        let mut end = row + 1;
        // SAFETY: `end < count`.
        while end < self.count && unsafe { self.at(end) } == key {
            end += 1;
        }
        end
    }

    /// The first row past `row` whose key is not below `key`, which `row`'s
    /// is: a gallop, so the next row costs one probe and a far one `O(log gap)`.
    #[inline(always)]
    fn skip_below(&self, row: usize, key: K) -> usize {
        // SAFETY: the gallop probes rows below `count` only.
        super::seek::gallop_from(self.count, row, |i| unsafe { self.at(i) } < key)
    }
}

/// One region of a two-way merge: each side's first cell and the output's cells.
struct MergeRegion<'w> {
    src: [*const u8; 2],
    dst: &'w mut [u8],
    stride: usize,
}

/// A two-way merge's output, written a stretch of one side's rows at a time.
///
/// It holds every region's bounds for the merge's life: where the sides
/// interleave a stretch is a row or two, and looking its regions up would cost
/// more than copying it.
struct MergeSink<'w> {
    srcs: &'w [MemBatch<'w>; 2],
    /// Where each side's heap lies in the output's, if it was carried.
    heap_at: [Option<usize>; 2],
    regions: Vec<MergeRegion<'w>>,
    blob: &'w mut Vec<u8>,
    cache: Option<&'w mut BlobCache>,
    string_slots: u64,
    /// Rows written, of the `room` the output has.
    rows: usize,
    room: usize,
    /// Carried heap bytes that only rows left out name.
    dead: usize,
}

impl<'w> MergeSink<'w> {
    fn new(srcs: &'w [MemBatch<'w>; 2], heap_at: [Option<usize>; 2], writer: &'w mut DirectWriter<'_>) -> Self {
        let (schema, room) = (writer.schema, writer.rows());
        let last = schema.num_fixed_regions() - 1;
        for src in srcs {
            debug_assert!(src.schema.same_region_types(schema), "a merge side of another layout");
            assert!(
                src.count <= src.cap
                    && src.region_start(last) + src.count * schema.region_stride(last) <= src.data.len()
            );
        }
        let (regions, blob, cache) = writer.split_mut();
        let regions = regions.enumerate().map(|(r, dst)| MergeRegion {
            // SAFETY: region `r` starts inside each side's arena: the regions
            // lie back to back, and the assert above bounds the last.
            src: srcs
                .each_ref()
                .map(|s| unsafe { s.data.as_ptr().add(s.region_start(r)) }),
            dst,
            stride: schema.region_stride(r),
        });
        MergeSink {
            srcs,
            heap_at,
            regions: regions.collect(),
            blob,
            cache,
            string_slots: schema.string_payload_slots(),
            rows: 0,
            room,
            dead: 0,
        }
    }

    /// Rows `[start, end)` of `side` follow; possibly none.
    ///
    /// # Safety
    /// `start <= end <= ` that side's row count, and the output has room for
    /// `end - start` more rows.
    #[inline(always)]
    unsafe fn push(&mut self, side: usize, start: usize, end: usize) {
        if start == end {
            return;
        }
        let at = self.rows;
        debug_assert!(start < end && end <= self.srcs[side].count && at + (end - start) <= self.room);
        let n = end - start;
        for region in &mut self.regions {
            let w = region.stride;
            // SAFETY: the stretch lies inside its side's rows and the rows the
            // output has room for, and every region holds that many cells.
            unsafe {
                copy_cell(
                    region.src[side].add(start * w),
                    region.dst.as_mut_ptr().add(at * w),
                    n * w,
                )
            };
        }
        for pi in gnitz_wire::BitIter(self.string_slots) {
            let cells = &mut self.regions[REG_PAYLOAD_START + pi].dst[at * 16..(at + n) * 16];
            let (blob, heap_at) = (self.srcs[side].blob, self.heap_at[side]);
            rebase_string_cells(cells, Heap::Plain(blob), self.blob, heap_at, self.cache.as_deref_mut());
        }
        self.rows += n;
    }

    /// Row `ia` of side 0 and row `jb` of side 1 are one element of weight
    /// `weight`: side 0's row follows at that weight, unless it is zero.
    ///
    /// # Safety
    /// As [`Self::push`]'s, for row `ia` of side 0.
    #[inline(always)]
    unsafe fn push_folded(&mut self, ia: usize, jb: usize, weight: i64) {
        self.leave_out(1, jb);
        if weight == 0 {
            return self.leave_out(0, ia);
        }
        unsafe { self.push(0, ia, ia + 1) };
        let weights = self.regions[REG_WEIGHT].dst.as_chunks_mut::<8>().0;
        weights[self.rows - 1] = weight.to_le_bytes();
    }

    /// `row` of `side` is not copied: a carried heap holds its long bytes dead.
    #[inline(always)]
    fn leave_out(&mut self, side: usize, row: usize) {
        if self.heap_at[side].is_some() {
            self.dead += row_long_bytes(&self.srcs[side], self.string_slots, row);
        }
    }
}

/// Copy `width` bytes. At a width a cell commonly has it is a load and a store,
/// where a copy of a runtime length is a `memcpy` call.
///
/// # Safety
/// `src` and `dst` each address `width` bytes, and do not overlap.
#[inline(always)]
unsafe fn copy_cell(src: *const u8, dst: *mut u8, width: usize) {
    use std::ptr::copy_nonoverlapping as copy;
    unsafe {
        match width {
            8 => copy(src, dst, 8),
            16 => copy(src, dst, 16),
            4 => copy(src, dst, 4),
            _ => copy(src, dst, width),
        }
    }
}

/// The two-way merge of `srcs`, each consolidated: both sides' rows in
/// (PK, payload) order, equal elements' weights summed into one row, net-zero
/// elements dropped. Out of line, so each (key, payload order) pair compiles as
/// its own function.
///
/// Only the shared-PK arm folds: a galloped stretch stops at the other side's
/// head PK, so nothing in it can share a (PK, payload) across sides, and a
/// consolidated input repeats none internally.
#[inline(never)]
fn two_way<'a, K, P>(srcs: &[MemBatch<'a>; 2], schema: &SchemaDescriptor, payload: P) -> Batch
where
    K: PkSortKey<'a>,
    P: PayloadOrder,
{
    let [a, b] = srcs;
    let (n_a, n_b) = (a.count, b.count);
    let (keys_a, keys_b) = (Keys::<K>::of(a), Keys::<K>::of(b));
    let mut out = Batch::with_capacity(schema, n_a + n_b);
    let slots = schema.string_payload_slots();
    let heap_at = [
        out.carry_heap(a, slots, &[(0, n_a)]),
        out.carry_heap(b, slots, &[(0, n_b)]),
    ];
    let mut cache = out.blob_cache(n_a + n_b);
    let dead = out.write_at_most(n_a + n_b, &mut cache, |writer| {
        let mut sink = MergeSink::new(srcs, heap_at, writer);
        let (mut ia, mut jb) = (0usize, 0usize);
        // SAFETY: `ia` and `jb` only grow, each stays within its side's rows
        // (a skip and a group end stop at the side's count), and every row is
        // pushed at most once, so at most `n_a + n_b` rows — the output's
        // room — are written; the keys are read at `ia < n_a`, `jb < n_b`.
        unsafe {
            while ia < n_a && jb < n_b {
                let (ka, kb) = (keys_a.at(ia), keys_b.at(jb));
                match ka.cmp(&kb) {
                    // A single-source stretch: one galloping skip, one copy.
                    Ordering::Less => {
                        let start = ia;
                        ia = keys_a.skip_below(ia, kb);
                        sink.push(0, start, ia);
                    }
                    Ordering::Greater => {
                        let start = jb;
                        jb = keys_b.skip_below(jb, ka);
                        sink.push(1, start, jb);
                    }
                    // A shared PK: both equal-PK groups, interleaved by payload.
                    // The only arm that folds, and the only one that reads row by
                    // row.
                    Ordering::Equal => {
                        let (ga, gb) = (keys_a.group_end(ia, ka), keys_b.group_end(jb, ka));
                        while ia < ga && jb < gb {
                            match payload.compare(schema, a, ia, b, jb) {
                                Ordering::Less => {
                                    sink.push(0, ia, ia + 1);
                                    ia += 1;
                                }
                                Ordering::Greater => {
                                    sink.push(1, jb, jb + 1);
                                    jb += 1;
                                }
                                // One element on both sides: its weights sum into
                                // one row, dropped at zero.
                                Ordering::Equal => {
                                    sink.push_folded(ia, jb, a.get_weight(ia) + b.get_weight(jb));
                                    ia += 1;
                                    jb += 1;
                                }
                            }
                        }
                        // One group ended, and nothing left folds against the
                        // other's remainder.
                        sink.push(0, ia, ga);
                        sink.push(1, jb, gb);
                        (ia, jb) = (ga, gb);
                    }
                }
            }
            sink.push(0, ia, n_a);
            sink.push(1, jb, n_b);
        }
        (sink.rows, sink.dead)
    });
    out.charge_dead(dead);
    out
}

// ---------------------------------------------------------------------------
// Single-batch sort + consolidation
// ---------------------------------------------------------------------------

/// Sort a single batch by (PK, payload) and consolidate: sum the weights of
/// identical rows, drop ghosts. Appends one [`Pick`] `(0, row, net weight)` per
/// group to `out` — [`run_merge`]'s single-source counterpart, in the shape
/// [`super::scatter::UnifiedSet::materialize`] copies out and the caller sizes
/// its arena from.
pub(crate) fn consolidate_groups(batch: &MemBatch, out: &mut Vec<Pick>) {
    if batch.count == 0 {
        return;
    }
    with_payload_cmp!(batch.schema, |p| consolidate_groups_inner(batch, out, p))
}

/// Whether `batch`'s rows already stand as [`consolidate_groups`] would leave
/// them: strictly (PK, payload)-ascending, with no ghost. One forward pass, which
/// the first pair out of order ends.
pub(crate) fn in_consolidated_order(batch: &Batch) -> bool {
    if batch.is_empty() {
        return true;
    }
    with_payload_cmp!(batch.schema(), |p| ascending_with(batch, p)) && !batch.has_ghost()
}

/// Whether `batch`, of at least one row, is strictly (PK, payload)-ascending.
fn ascending_with<P: PayloadOrder>(batch: &Batch, payload: P) -> bool {
    let schema = batch.schema();
    let stride = schema.pk_stride();
    pk_width_dispatch!(stride, |K| {
        let mut keys = batch.pk().chunks_exact(stride).map(K::from_opk);
        let mut prev = keys.next().expect("a batch of at least one row");
        keys.zip(1..).all(|(key, i)| {
            // The payload order is read only where two rows share a PK.
            let below = Ord::cmp(&prev, &key).then_with(|| payload.compare(schema, batch, i - 1, batch, i));
            prev = key;
            below.is_lt()
        })
    })
}

/// One argsort element: a row's PK sort key and its index.
trait ArgEntry: Copy {
    fn idx(self) -> usize;
    fn same_pk(self, other: Self) -> bool;
}

/// A `(sort key, row-index)` pair. Keeps the key co-located with its index so the
/// comparator reads from the element being positioned rather than chasing a
/// separate key array.
#[derive(Copy, Clone)]
struct SortEntry<K> {
    key: K,
    idx: u32,
}

impl<K: Copy + Eq> ArgEntry for SortEntry<K> {
    #[inline(always)]
    fn idx(self) -> usize {
        self.idx as usize
    }
    #[inline(always)]
    fn same_pk(self, other: Self) -> bool {
        self.key == other.key
    }
}

/// A 17..=[`PACKED_MAX_STRIDE`]-byte PK's `[u128; 2]` key with the row index in
/// its low four bytes, which the left-aligned key leaves zero: ordered by one
/// plain compare.
#[derive(Copy, Clone, PartialEq, Eq)]
struct PackedEntry([u128; 2]);

impl Ord for PackedEntry {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0[0], self.0[1]).cmp(&(other.0[0], other.0[1]))
    }
}

impl PartialOrd for PackedEntry {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const PACKED_MAX_STRIDE: usize = size_of::<PackedEntry>() - size_of::<u32>();

impl PackedEntry {
    #[inline(always)]
    fn new(opk: &[u8], idx: u32) -> Self {
        debug_assert!((17..=PACKED_MAX_STRIDE).contains(&opk.len()));
        let [hi, lo] = <[u128; 2]>::from_opk(opk);
        PackedEntry([hi, lo | idx as u128])
    }
}

impl ArgEntry for PackedEntry {
    #[inline(always)]
    fn idx(self) -> usize {
        self.0[1] as u32 as usize
    }
    #[inline(always)]
    fn same_pk(self, other: Self) -> bool {
        self.0[0] == other.0[0] && (self.0[1] ^ other.0[1]) >> 32 == 0
    }
}

/// The argsort half of [`consolidate_groups`]: the entries sorted by PK alone —
/// a batch whose PKs already stand in order sorts in one pass — and each run of
/// a PK sorted by its payload as [`fold_runs`] folds it.
#[inline]
fn consolidate_groups_inner<P: PayloadOrder>(batch: &MemBatch, out: &mut Vec<Pick>, payload: P) {
    let (n, schema, stride) = (batch.count, batch.schema, batch.pk_stride());
    if (17..=PACKED_MAX_STRIDE).contains(&stride) {
        let mut entries: Vec<PackedEntry> = (0..n as u32)
            .map(|i| PackedEntry::new(batch.get_pk_bytes(i as usize), i))
            .collect();
        entries.sort_unstable();
        return fold_runs(&mut entries, batch, schema, payload, out);
    }
    pk_width_dispatch!(stride, |K| {
        let mut entries: Vec<SortEntry<K>> = (0..n as u32)
            .map(|i| SortEntry {
                key: K::from_opk(batch.get_pk_bytes(i as usize)),
                idx: i,
            })
            .collect();
        entries.sort_unstable_by(|a, b| Ord::cmp(&a.key, &b.key));
        fold_runs(&mut entries, batch, schema, payload, out);
    })
}

/// The fold half of [`consolidate_groups`]: over entries sorted by PK, sort each
/// run of a PK by its payload and push one survivor per (PK, payload) group whose
/// weights do not cancel.
#[inline]
fn fold_runs<E: ArgEntry, P: PayloadOrder>(
    entries: &mut [E],
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    payload: P,
    out: &mut Vec<Pick>,
) {
    let weights = batch.weight().as_chunks::<8>().0;
    // The run in hand's payload keys and rows, in a buffer every run reuses.
    let mut keyed = Vec::new();
    for run in entries.chunk_by_mut(|a, b| a.same_pk(*b)) {
        match run {
            [one] => {
                let weight = i64::from_le_bytes(weights[one.idx()]);
                if weight != 0 {
                    out.push((0, one.idx() as u32, weight));
                }
            }
            run => fold_run(run, batch, schema, payload, weights, &mut keyed, out),
        }
    }
}

/// Sort `run`, the rows of one PK, by its payload and fold it. Out of line, so
/// the loop over a batch of distinct PKs stays tight.
#[inline(never)]
fn fold_run<E: ArgEntry, P: PayloadOrder>(
    run: &mut [E],
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    payload: P,
    weights: &[[u8; 8]],
    keyed: &mut Vec<(u128, u32)>,
    out: &mut Vec<Pick>,
) {
    let cmp = |a: usize, b: usize| payload.compare(schema, batch, a, batch, b);
    if let [a, b] = run {
        let (a, b) = (a.idx(), b.idx());
        let weight = |i: usize| i64::from_le_bytes(weights[i]);
        let mut push = |i: usize, w: i64| {
            if w != 0 {
                out.push((0, i as u32, w));
            }
        };
        match cmp(a, b) {
            Ordering::Equal => push(a, weight(a) + weight(b)),
            Ordering::Less => (push(a, weight(a)), push(b, weight(b))).0,
            Ordering::Greater => (push(b, weight(b)), push(a, weight(a))).0,
        }
        return;
    }
    if P::FIXED_INT {
        keyed.clear();
        keyed.extend(run.iter().map(|e| (0, e.idx() as u32)));
        fold_keyed(batch, schema, 0, keyed, weights, out);
    } else {
        run.sort_unstable_by(|a, b| cmp(a.idx(), b.idx()));
        fold_sorted(run, |e| e.idx(), |a, b| cmp(a.idx(), b.idx()).is_eq(), weights, out);
    }
}

/// Sort `keyed`, rows that tie on every fixed-int, non-null payload column
/// before `from`, by the rest of the payload, and fold them. The columns from
/// `from` that fit 128 bits are packed into each row's key from the high bits
/// down, each order-preserving, and the rows sorted by it; rows still tied on it
/// are sorted the same way by the columns after.
fn fold_keyed(
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    from: usize,
    keyed: &mut [(u128, u32)],
    weights: &[[u8; 8]],
    out: &mut Vec<Pick>,
) {
    /// Shift each key left past the column and put the column's bits in.
    #[inline(always)]
    fn put<const W: usize>(keyed: &mut [(u128, u32)], cells: &[u8], signed: bool) {
        let cells = cells.as_chunks::<W>().0;
        for (key, row) in keyed.iter_mut() {
            *key = (*key << (W * 8)) | native_key_image(&cells[*row as usize], signed);
        }
    }
    // The rows tie on the key the columns before `from` made, so whatever of it
    // these columns do not shift out is a prefix every row shares.
    let (mut room, mut next) = (u128::BITS as usize, from);
    for (pc, col) in schema.payload_columns().skip(from) {
        let cs = col.size();
        let Some(left) = room.checked_sub(cs * 8) else { break };
        (room, next) = (left, next + 1);
        let (cells, signed) = (batch.col_data(pc), col.is_signed());
        match cs {
            1 => put::<1>(keyed, cells, signed),
            2 => put::<2>(keyed, cells, signed),
            4 => put::<4>(keyed, cells, signed),
            8 => put::<8>(keyed, cells, signed),
            w => unreachable!("a fixed-int column of {w} bytes"),
        }
    }
    keyed.sort_unstable_by_key(|e| e.0);
    if next == schema.num_payload_cols() {
        return fold_sorted(keyed, |e| e.1 as usize, |a, b| a.0 == b.0, weights, out);
    }
    for tie in keyed.chunk_by_mut(|a, b| a.0 == b.0) {
        match tie {
            [_] => fold_sorted(tie, |e| e.1 as usize, |_, _| true, weights, out),
            tie => fold_keyed(batch, schema, next, tie, weights, out),
        }
    }
}

/// Push one survivor per group of adjacent `same` rows of `sorted` whose weights
/// do not cancel: the group's first row, at the group's summed weight.
#[inline(always)]
fn fold_sorted<T>(
    sorted: &[T],
    idx: impl Fn(&T) -> usize,
    same: impl Fn(&T, &T) -> bool,
    weights: &[[u8; 8]],
    out: &mut Vec<Pick>,
) {
    for group in sorted.chunk_by(same) {
        let weight: i64 = group.iter().map(|e| i64::from_le_bytes(weights[idx(e)])).sum();
        if weight != 0 {
            out.push((0, idx(&group[0]) as u32, weight));
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/merge.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/merge.rs"]
mod bench;
