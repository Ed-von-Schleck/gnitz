//! The borrowed batch view [`MemBatch`], the columnar source abstraction, and
//! the merges built on them: the N-way run merge, in-batch consolidation, and
//! the two-way batch merge.
//!
//! Operates on flat columnar buffers: pk[OPK big-endian, `pk_stride` B/row],
//! weight[i64 LE], null_bitmap[u64 LE], payload columns, blob arena.
//!
//! The N-way merge is a fused k-way merge + inline consolidation: rows with the
//! same (PK, payload) have their weights summed; rows whose net weight is zero
//! are dropped. [`Batch::merged_consolidated`] is the two-input counterpart over
//! the same comparator family — Z-Set `+`, fold included.

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{ControlFlow, Range};

use super::batch::{Batch, FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_WEIGHT};
use super::loser_tree::{HeapNode, LoserTree};
use super::scatter::DecodedColumns;
use super::string_heap::{rebase_string_cells, row_long_bytes, BlobCache};
use super::writer::DirectWriter;
use crate::schema::key::{pack_pk_be, pk_width_dispatch, PkSortKey};
use crate::schema::payload_order::{compare_full_rows, with_payload_cmp, PayloadOrder};
use crate::schema::SchemaDescriptor;
use gnitz_expr::BatchView;
use gnitz_wire::read_u64_le;
use gnitz_wire::RowSource;
use gnitz_wire::NARROW_PK_MAX_BYTES;

// ---------------------------------------------------------------------------
// ColPtr / UnifiedSource: type-erased column accessors, one `(base, stride)`
// `ColPtr` per region, for in-memory `MemBatch` and shard sources alike.
// ---------------------------------------------------------------------------

/// A column as `base + row * stride`. Stride 0 reads one shared element for
/// every row. `base` itself need not lie inside the column: a decoded window is
/// rebased so that its first row lands on its first cell.
#[derive(Clone, Copy)]
pub(crate) struct ColPtr {
    pub base: *const u8,
    pub stride: usize,
}

impl ColPtr {
    /// Row `i`'s address.
    #[inline(always)]
    pub(crate) fn row_ptr(self, i: usize) -> *const u8 {
        self.base.wrapping_add(i * self.stride)
    }

    /// Row `i`'s first `len` bytes.
    ///
    /// # Safety
    /// Row `i` is a row of a live column this addresses, and `len` does not
    /// exceed its element width.
    #[inline(always)]
    pub(crate) unsafe fn row<'a>(self, i: usize, len: usize) -> &'a [u8] {
        std::slice::from_raw_parts(self.row_ptr(i), len)
    }

    /// Copy rows `start..` of this column of `width`-byte elements onto `dst`,
    /// `dst.len() / width` rows.
    ///
    /// # Safety
    /// Those rows are rows of a live column this addresses.
    pub(crate) unsafe fn copy_rows(self, start: usize, width: usize, dst: &mut [u8]) {
        if self.stride != 0 {
            return dst.copy_from_slice(std::slice::from_raw_parts(self.row_ptr(start), dst.len()));
        }
        if dst.is_empty() {
            return;
        }
        // One element repeated: write it once, then double the written prefix,
        // so any element width costs O(log rows) copies.
        dst[..width].copy_from_slice(self.row(0, width));
        let mut filled = width;
        while filled < dst.len() {
            let n = filled.min(dst.len() - filled);
            dst.copy_within(..n, filled);
            filled += n;
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct UnifiedSource<'a> {
    pub pk: ColPtr,
    pub null_bmp: ColPtr,
    /// Null bits OR'd into every null word the scatter copies out.
    pub null_pad_mask: u64,
    /// Index of this source's first payload `ColPtr` in the caller-owned table
    /// the scatter reads through: column `pi` is `cols[cols_off + pi]`. Out of
    /// line because a by-value `[ColPtr; MAX_COLUMNS]` would be zeroed per source
    /// per call, whatever the schema's real column count.
    pub cols_off: usize,
    pub blob: &'a [u8],
    /// Where `blob` already lies in the scatter's destination heap, when carried
    /// whole; `None` relocates each cell.
    pub heap_at: Option<usize>,
}

/// Derive a `UnifiedSource` view over an in-memory `MemBatch`: every region
/// becomes a `(base, stride)` `ColPtr` into the batch's `data`, and the blob
/// arena is borrowed. Pure pointer arithmetic — no allocation,
/// no scan. The payload `ColPtr`s are appended to `cols`, the flat table the
/// scatter indexes through `UnifiedSource::cols_off`.
pub(crate) fn mem_batch_to_unified<'a>(
    mb: &MemBatch<'a>,
    schema: &SchemaDescriptor,
    cols: &mut Vec<ColPtr>,
) -> UnifiedSource<'a> {
    let data_ptr = mb.data.as_ptr();
    let cols_off = cols.len();
    for (pi, col) in schema.payload_columns() {
        cols.push(ColPtr {
            base: unsafe { data_ptr.add(mb.region_start(REG_PAYLOAD_START + pi)) },
            stride: col.size() as usize,
        });
    }
    UnifiedSource {
        pk: ColPtr { base: data_ptr, stride: mb.pk_stride() },
        null_bmp: ColPtr {
            base: unsafe { data_ptr.add(mb.region_start(REG_NULL_BMP)) },
            stride: FIXED_REGION_BYTES,
        },
        null_pad_mask: 0,
        cols_off,
        blob: mb.blob,
        heap_at: None,
    }
}

// ---------------------------------------------------------------------------
// MemBatch: a view over flat columnar buffers (one batch / sorted run)
// ---------------------------------------------------------------------------

/// A borrowed view of batch rows: an arena of `cap` rows laid out under
/// `schema`, of which the first `count` are rows. A [`Batch`]'s own, or a WAL
/// block's fixed bytes, which are an arena exactly as large as its rows.
#[derive(Clone)]
pub struct MemBatch<'a> {
    pub(crate) data: &'a [u8],
    /// The layout: region `r` starts at `schema.region_start(r, cap)`.
    pub(crate) schema: &'a SchemaDescriptor,
    pub(crate) cap: usize,
    pub(crate) blob: &'a [u8],
    /// Row count of the view. Read from outside through [`MemBatch::len`].
    pub(crate) count: usize,
    /// An upper bound on the bytes of `blob` no cell references.
    pub(in crate::repr) dead_heap: usize,
}

impl<'a> MemBatch<'a> {
    /// Row count of the view.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Byte width of the PK region per row.
    #[inline(always)]
    pub fn pk_stride(&self) -> usize {
        self.schema.pk_stride()
    }

    /// Where region `r` starts in the arena.
    #[inline(always)]
    pub(crate) fn region_start(&self, r: usize) -> usize {
        self.schema.region_start(r, self.cap)
    }

    /// The `count` rows of region `r`.
    #[inline(always)]
    pub(crate) fn region(&self, r: usize) -> &'a [u8] {
        let off = self.region_start(r);
        &self.data[off..off + self.count * self.schema.region_stride(r)]
    }

    /// PK region as a contiguous slice (`count * pk_stride` bytes).
    #[inline]
    pub fn pk(&self) -> &'a [u8] {
        &self.data[..self.count * self.pk_stride()]
    }

    /// Weight region as a contiguous slice (`count * 8` bytes).
    #[inline]
    pub(crate) fn weight(&self) -> &'a [u8] {
        let off = self.region_start(REG_WEIGHT);
        &self.data[off..off + self.count * 8]
    }

    /// Wrapping sum of the weights of rows `[start, end)`, over one
    /// bounds-checked slice of the weight region.
    #[inline]
    pub fn sum_weights(&self, start: usize, end: usize) -> i64 {
        self.weight()[start * 8..end * 8]
            .as_chunks::<8>()
            .0
            .iter()
            .fold(0i64, |a, w| a.wrapping_add(i64::from_le_bytes(*w)))
    }

    /// Null bitmap region as a contiguous slice (`count * 8` bytes).
    #[inline(always)]
    pub(crate) fn null_bmp(&self) -> &'a [u8] {
        let off = self.region_start(REG_NULL_BMP);
        &self.data[off..off + self.count * 8]
    }

    /// Payload column `pi` as a contiguous slice (`count * stride` bytes).
    /// Caller supplies the stride from the schema (see `payload_columns`).
    #[inline(always)]
    pub fn col_data(&self, pi: usize, stride: usize) -> &'a [u8] {
        let off = self.region_start(REG_PAYLOAD_START + pi);
        &self.data[off..off + self.count * stride]
    }

    #[inline(always)]
    pub fn get_pk_bytes(&self, row: usize) -> &'a [u8] {
        let stride = self.pk_stride();
        let off = row * stride;
        &self.data[off..off + stride]
    }
    /// The `n` bytes at `at` of row `row`'s PK.
    #[inline(always)]
    pub(crate) fn get_pk_range(&self, row: usize, at: usize, n: usize) -> &'a [u8] {
        debug_assert!(at + n <= self.pk_stride());
        let off = row * self.pk_stride() + at;
        &self.data[off..off + n]
    }
    #[inline(always)]
    pub fn get_weight(&self, row: usize) -> i64 {
        gnitz_wire::read_i64_le(self.data, self.region_start(REG_WEIGHT) + row * 8)
    }
    #[inline(always)]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.region_start(REG_NULL_BMP) + row * 8)
    }
    #[inline(always)]
    pub(crate) fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &'a [u8] {
        let off = self.region_start(REG_PAYLOAD_START + payload_col) + row * col_size;
        &self.data[off..off + col_size]
    }
}

/// Each method forwards via UFCS to the inherent accessor of the same name, so
/// the call binds to the concrete read rather than recursing into the trait.
///
/// Every forwarder — **and every inherent accessor it UFCS-calls** — is
/// `#[inline(always)]`; see [`BatchView`] for why the plain hint is not enough.
/// Promoting only the forwarder would leave the terminus a real call and do half
/// the job.
impl<'a> RowSource for MemBatch<'a> {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        MemBatch::get_pk_bytes(self, row)
    }
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        MemBatch::get_null_word(self, row)
    }
    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        MemBatch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline(always)]
    fn blob(&self) -> &[u8] {
        self.blob
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        MemBatch::len(self)
    }
}

/// The region half. The per-row accessors above are **not** derived from these
/// (see [`BatchView`]): the derived form costs a row-count load, a second
/// multiply and a second range check per read, which `-O0` cannot hoist.
impl<'a> BatchView for MemBatch<'a> {
    #[inline(always)]
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8] {
        MemBatch::col_data(self, payload_col, col_size)
    }
    #[inline(always)]
    fn null_bmp(&self) -> &[u8] {
        MemBatch::null_bmp(self)
    }
    #[inline(always)]
    fn pk_region(&self) -> (&[u8], usize) {
        (MemBatch::pk(self), self.pk_stride())
    }
}

impl<'a> ColumnarSource for MemBatch<'a> {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        MemBatch::get_weight(self, row)
    }

    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut Vec<ColPtr>,
        _window: Range<usize>,
        _decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        mem_batch_to_unified(self, schema, cols)
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// PosCursor: one source's position within the N-way merge
// ---------------------------------------------------------------------------

/// One source's merge position: `position` walks `[0, count)`. Sources are
/// ghost-free by construction (a shard is *written* consolidated, a RAM-tier run
/// folded on the way in) and [`drive`]'s fold drops a stray ghost anyway, so
/// advancing is a bare `position + 1`.
///
/// `count` is the walk's upper bound, which need not be the source's row count:
/// a read cursor's range seek clamps it to the range's exclusive end so the walk
/// simply exhausts at the cut.
pub(crate) struct PosCursor {
    pub(crate) position: usize,
    pub(crate) count: usize,
}

impl PosCursor {
    /// A cursor over `[0, count)`, from row 0.
    #[inline]
    pub(crate) fn new(count: usize) -> Self {
        debug_assert!(
            count < u32::MAX as usize,
            "merge source exceeds the heap node's u32 row"
        );
        PosCursor { position: 0, count }
    }

    #[inline]
    pub(crate) fn is_valid(&self) -> bool {
        self.position < self.count
    }

    /// Step past the current row. Callers advance only a position they have
    /// established is valid (the merge drivers advance the heap root).
    #[inline]
    pub(crate) fn advance(&mut self) {
        self.position += 1;
    }
}

// ---------------------------------------------------------------------------
// run_merge: the generic N-way (PK, payload) merge + consolidation
// ---------------------------------------------------------------------------

/// N-way (PK, payload) merge + consolidation over any sorted columnar sources —
/// the single owner of the merge that a store's flush and shard compaction
/// (`compact::merge_and_route`) share.
///
/// Rows with the same (PK, payload) have their weights summed; zero-weight
/// (PK, payload) groups are dropped. The payload-aware heap ordering puts equal
/// (PK, payload) entries consecutively at the root, so the single pending-group
/// drain in [`drive`] folds both intra-source duplicates (consecutive
/// matching rows inside one sorted source) and cross-source duplicates in one
/// pass.
///
/// Selects the payload order once via `with_payload_cmp!`, seats one
/// [`PosCursor`] per source, and drives [`drive`]; the PK axis is settled by
/// each row's key under a [`MergeOrder`], and past it by [`merge_less`]. Each source's walk
/// bound is its own [`RowSource::row_count`].
/// `emit(group_src, group_row, net_weight)` fires once per surviving (net ≠ 0)
/// group; the caller turns `(src, row)` into its output (a `DirectWriter` row for
/// flush, a guard-routed shard append for compaction).
pub(crate) fn run_merge<S: ColumnarSource>(
    sources: &[S],
    schema: &SchemaDescriptor,
    emit: impl FnMut(usize, usize, i64),
) {
    if sources.is_empty() {
        return;
    }
    let mut cursors: Vec<PosCursor> = sources.iter().map(|s| PosCursor::new(s.row_count())).collect();

    // Dispatch the payload order, monomorphizing one branch-free copy of the
    // merge loop.
    with_payload_cmp!(schema, run_merge_body, sources, &mut cursors, schema, emit)
}

/// Z-set `+` of `sources`, each consolidated, as one consolidated batch.
pub fn merge_consolidated(sources: &[MemBatch<'_>], schema: &SchemaDescriptor) -> Batch {
    let held = sources.iter().filter(|s| s.count > 0);
    let ascending = held
        .clone()
        .zip(held.skip(1))
        .all(|(a, b)| a.get_pk_bytes(a.count - 1) < b.get_pk_bytes(0));
    let mut out = match ascending {
        // Sources that each end below the PK the next begins at share no row
        // and hold none out of order across them: in order, they are the merge.
        true => Batch::concat(schema, sources.iter().cloned()),
        false => merge_rows(sources, schema),
    };
    out.certify_consolidated();
    out
}

/// The N-way fold of `sources`, each sorted by (PK, payload). Out of line, so
/// the loop compiles as it does without [`merge_consolidated`]'s test ahead of it.
#[inline(never)]
fn merge_rows(sources: &[MemBatch<'_>], schema: &SchemaDescriptor) -> Batch {
    let mut rows: Vec<(u32, u32, i64)> = Vec::with_capacity(sources.iter().map(|s| s.count).sum());
    run_merge(sources, schema, |src, row, w| rows.push((src as u32, row as u32, w)));
    super::scatter::materialize_carrying(sources, schema, &rows)
}

/// A `RowSource` that also carries the Z-set weight: a storage row the N-way
/// merge folds.
pub(crate) trait ColumnarSource: RowSource {
    /// The row's signed Z-set weight / multiplicity (region[1]).
    fn get_weight(&self, row: usize) -> i64;

    /// A [`UnifiedSource`] over this source's regions, readable at rows in
    /// `window`, with one payload `ColPtr` per payload column of `schema`
    /// appended to `cols`.
    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut Vec<ColPtr>,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_>;

    /// Whether this source's rows are (PK, coarse weight) pairs with no payload —
    /// a capacity-bounded view's skeleton shard.
    fn is_skeleton(&self) -> bool;
}

/// A borrowed source reads as the source it borrows.
impl<T: ColumnarSource + ?Sized> ColumnarSource for &T {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        (**self).get_weight(row)
    }

    #[inline(always)]
    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut Vec<ColPtr>,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        (**self).to_unified(schema, cols, window, decoded)
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        (**self).is_skeleton()
    }
}

/// The order of two players whose keys under `order` tie, which [`drive`] and
/// the read cursor share. Generic over the source type, so each caller
/// monomorphizes its own branch-free copy.
///
/// The PK bytes behind the key span, which only a wide PK has, then the
/// `payload` order — unless `order` coarsens and one row is a skeleton row,
/// which is tested ahead of `payload`, so a skeleton row's absent columns are
/// never read.
#[inline]
pub(crate) fn merge_less<'a, S, P>(
    schema: &'a SchemaDescriptor,
    sources: &'a [S],
    order: MergeOrder,
    payload: P,
) -> impl Fn(&HeapNode, &HeapNode) -> bool + Copy + 'a
where
    S: ColumnarSource,
    P: PayloadOrder + 'a,
{
    let wide = order.has_tail(schema.pk_stride());
    move |a, b| {
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
                        return sa && !sb;
                    }
                }
                payload.compare(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Less
            }
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
    coarsen: bool,
}

impl MergeOrder {
    /// Keyed by the leading bytes of a `stride`-byte PK.
    pub(crate) fn leading(stride: usize, coarsen: bool) -> Self {
        MergeOrder {
            wide_at: (stride > NARROW_PK_MAX_BYTES).then_some(0),
            coarsen,
        }
    }

    /// Keyed past the prefix every row of `sources` shares, so that keys of
    /// small values in wide columns do not all tie. A source is sorted, so what
    /// its first and last rows share, all of its rows share. Never coarsened:
    /// a compaction that writes skeleton rows merges under the PK-only schema
    /// instead (`compact::merge_and_route`).
    fn past_shared_prefix<S: ColumnarSource>(sources: &[S], stride: usize) -> Self {
        if stride <= NARROW_PK_MAX_BYTES {
            return Self::leading(stride, false);
        }
        let mut bounds = sources
            .iter()
            .filter(|s| s.row_count() > 0)
            .flat_map(|s| [s.get_pk_bytes(0), s.get_pk_bytes(s.row_count() - 1)]);
        let first = bounds.next().unwrap_or(&[]);
        let shared = |pk: &[u8]| first.iter().zip(pk).take_while(|(a, b)| a == b).count();
        let at = bounds.map(shared).min().unwrap_or(0);
        MergeOrder {
            wide_at: Some(at.min(stride - NARROW_PK_MAX_BYTES)),
            coarsen: false,
        }
    }

    /// Whether a PK has bytes behind the key span.
    #[inline(always)]
    fn has_tail(self, stride: usize) -> bool {
        self.wide_at.is_some_and(|at| at + NARROW_PK_MAX_BYTES < stride)
    }

    /// A row's tournament key.
    #[inline(always)]
    pub(crate) fn key<S: ColumnarSource>(self, sources: &[S], src: usize, row: usize) -> u128 {
        let pk = sources[src].get_pk_bytes(row);
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
/// Every merge in the tree runs through this — the flush/compaction kernel
/// ([`run_merge`]) and the read cursor's advance and drain — and it builds all
/// four closures itself, so no caller can pair a heap order with a mismatched
/// group boundary. `order` is [`merge_less`]'s and means the same here.
///
/// `#[inline(always)]`: each caller's `emit` returns a constant `ControlFlow`, so
/// forced inlining folds the branch and drops the unused arm per monomorphisation.
#[inline(always)]
pub(crate) fn drive<S, P>(
    tree: &mut LoserTree,
    schema: &SchemaDescriptor,
    sources: &[S],
    order: MergeOrder,
    cursors: &mut [PosCursor],
    payload: P,
    mut emit: impl FnMut(usize, usize, i64) -> ControlFlow<()>,
) where
    S: ColumnarSource,
    P: PayloadOrder,
{
    // `less` and `same_group` read `(source_idx, row)` out of the heap node and
    // never touch `cursors`, so they coexist with the `&mut cursors` `step!` holds.
    let less = merge_less(schema, sources, order, payload);
    let wide = order.has_tail(schema.pk_stride());
    // Of two rows whose keys tie, as `less` is.
    let same_group = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        if wide && order.tail(&sources[a_src], a_row) != order.tail(&sources[b_src], b_row) {
            return false;
        }
        if order.coarsen && (sources[a_src].is_skeleton() || sources[b_src].is_skeleton()) {
            return true;
        }
        payload.compare(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Equal
    };
    // Sources are ghost-free, so the advance is a bare `position + 1`.
    macro_rules! step {
        ($src:expr) => {{
            let c = &mut cursors[$src];
            c.advance();
            let next = c
                .is_valid()
                .then(|| (c.position as u32, order.key(sources, $src, c.position)));
            tree.step_top(next, &less);
        }};
    }

    while let Some(top) = tree.peek() {
        let (group_src, group_row, group_key) = (top.source_idx as usize, top.row as usize, top.key());

        // Open the group: take the root's weight and step past it. The first row
        // is the exemplar, so `same_group` would be tautologically true — and its
        // payload term walks every column.
        let mut net_weight: i64 = sources[group_src].get_weight(group_row);
        step!(group_src);

        // Fold tied rows: each iteration peeks the new root, breaks at the group
        // boundary, otherwise accumulates weight and steps again.
        while let Some(top) = tree.peek() {
            let (cur_src, cur_row) = (top.source_idx as usize, top.row as usize);
            if top.key() != group_key || !same_group(group_src, group_row, cur_src, cur_row) {
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

/// The tournament build for [`run_merge`]; the walk itself is [`drive`].
/// Monomorphised per (source, payload) so the hot loop stays branch-free.
#[inline]
fn run_merge_body<S, P>(
    sources: &[S],
    cursors: &mut [PosCursor],
    schema: &SchemaDescriptor,
    mut emit: impl FnMut(usize, usize, i64),
    payload: P,
) where
    S: ColumnarSource,
    P: PayloadOrder,
{
    let order = MergeOrder::past_shared_prefix(sources, schema.pk_stride());
    let mut tree = LoserTree::build(
        cursors.len(),
        |i| {
            cursors[i]
                .is_valid()
                .then(|| (cursors[i].position as u32, order.key(sources, i, cursors[i].position)))
        },
        merge_less(schema, sources, order, payload),
    );
    drive(&mut tree, schema, sources, order, cursors, payload, |src, row, w| {
        emit(src, row, w);
        ControlFlow::Continue(())
    });
}

// ---------------------------------------------------------------------------
// The two-way merge: Z-Set `+` over two consolidated batches
// ---------------------------------------------------------------------------

impl Batch {
    /// Z-Set `+` of two consolidated batches: both sides' rows in (PK, payload)
    /// order, equal elements' weights summed into one row, net-zero elements
    /// dropped.
    ///
    /// Only the shared-PK arm folds: a galloped stretch stops at the other
    /// side's head PK, so nothing in it can share a (PK, payload) across sides,
    /// and a consolidated input repeats none internally.
    pub fn merged_consolidated(&self, other: &Batch, schema: &SchemaDescriptor) -> Batch {
        debug_assert!(
            self.stands_consolidated() && other.stands_consolidated(),
            "merged_consolidated: both inputs must be consolidated",
        );
        let mut out = pk_width_dispatch!(schema.pk_stride(), |K| {
            with_payload_cmp!(schema, merged_consolidated_body::<K, _>, self, other, schema)
        });
        out.certify_consolidated();
        out
    }
}

/// One batch's PK region, read as keys of the type `K` matching its stride.
struct Keys<'a, K> {
    pk: &'a [u8],
    stride: usize,
    count: usize,
    key: PhantomData<K>,
}

impl<'a, K: PkSortKey<'a>> Keys<'a, K> {
    fn of(batch: &'a Batch) -> Self {
        Keys {
            pk: batch.pk_data(),
            stride: batch.schema().pk_stride(),
            count: batch.count,
            key: PhantomData,
        }
    }

    #[inline(always)]
    fn at(&self, row: usize) -> K {
        assert!(row < self.count);
        // SAFETY: `row` is one of the `count` rows `pk` holds.
        unsafe { self.at_unchecked(row) }
    }

    /// # Safety
    /// `row < self.count`.
    #[inline(always)]
    unsafe fn at_unchecked(&self, row: usize) -> K {
        debug_assert!(row < self.count);
        K::from_opk(unsafe { self.pk.get_unchecked(row * self.stride..(row + 1) * self.stride) })
    }

    /// The first row past `row` whose key is not `key`, which `row`'s is. A
    /// linear step: an equal-PK group is short.
    #[inline(always)]
    fn group_end(&self, row: usize, key: K) -> usize {
        let mut end = row + 1;
        // SAFETY: `end < count`.
        while end < self.count && unsafe { self.at_unchecked(end) } == key {
            end += 1;
        }
        end
    }

    /// The first row past `row` whose key is not below `key`, which `row`'s
    /// is: a gallop, so the next row costs one probe and a far one `O(log gap)`.
    #[inline(always)]
    fn skip_below(&self, row: usize, key: K) -> usize {
        let (mut lo, mut step) = (row, 1);
        // SAFETY (both loops): the probed row is below `count`.
        while lo + step < self.count && unsafe { self.at_unchecked(lo + step) } < key {
            lo += step;
            step *= 2;
        }
        let (mut lo, mut hi) = (lo + 1, (lo + step).min(self.count));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match unsafe { self.at_unchecked(mid) } < key {
                true => lo = mid + 1,
                false => hi = mid,
            }
        }
        lo
    }
}

/// One region of a two-way merge: each side's cells and the output's.
struct MergeRegion<'w> {
    src: [&'w [u8]; 2],
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
        let (regions, blob, cache) = writer.split_mut();
        let regions = regions.enumerate().map(|(r, dst)| MergeRegion {
            src: [srcs[0].region(r), srcs[1].region(r)],
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
    #[inline(always)]
    fn push(&mut self, side: usize, start: usize, end: usize) {
        if start == end {
            return;
        }
        let at = self.rows;
        assert!(start < end && end <= self.srcs[side].count && at + (end - start) <= self.room);
        let n = end - start;
        // SAFETY (both arms): the stretch lies inside its side's rows and the
        // rows the output has room for, and every region holds that many cells.
        if n == 1 {
            for region in &mut self.regions {
                let w = region.stride;
                unsafe {
                    copy_cell(
                        region.src[side].as_ptr().add(start * w),
                        region.dst.as_mut_ptr().add(at * w),
                        w,
                    )
                };
            }
        } else {
            for region in &mut self.regions {
                let w = region.stride;
                let src = unsafe { region.src[side].as_ptr().add(start * w) };
                unsafe { std::ptr::copy_nonoverlapping(src, region.dst.as_mut_ptr().add(at * w), n * w) };
            }
        }
        for pi in gnitz_wire::BitIter(self.string_slots) {
            let cells = &mut self.regions[REG_PAYLOAD_START + pi].dst[at * 16..(at + n) * 16];
            let (blob, heap_at) = (self.srcs[side].blob, self.heap_at[side]);
            rebase_string_cells(cells, blob, self.blob, heap_at, self.cache.as_deref_mut());
        }
        self.rows += n;
    }

    /// Row `ia` of side 0 and row `jb` of side 1 are one element of weight
    /// `weight`: side 0's row follows at that weight, unless it is zero.
    #[inline(always)]
    fn push_folded(&mut self, ia: usize, jb: usize, weight: i64) {
        self.leave_out(1, jb);
        if weight == 0 {
            return self.leave_out(0, ia);
        }
        self.push(0, ia, ia + 1);
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

/// Copy one `width`-byte cell. At a width columns commonly have it is a load
/// and a store, where a copy of a runtime length is a `memcpy` call.
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

/// Out of line, so each (key, payload order) pair compiles as its own function.
#[inline(never)]
fn merged_consolidated_body<'a, K, P>(a: &'a Batch, b: &'a Batch, schema: &SchemaDescriptor, payload: P) -> Batch
where
    K: PkSortKey<'a>,
    P: PayloadOrder,
{
    let (n_a, n_b) = (a.count, b.count);
    let srcs = [a.as_mem_batch(), b.as_mem_batch()];
    let (keys_a, keys_b) = (Keys::<K>::of(a), Keys::<K>::of(b));
    let mut out = Batch::with_capacity(schema, n_a + n_b);
    let mut session = out.append_session(n_a + n_b);
    let heap_at = [
        session.carry(&srcs[0], &[(0, n_a)]),
        session.carry(&srcs[1], &[(0, n_b)]),
    ];
    let dead = session.write_at_most(n_a + n_b, |writer| {
        let mut sink = MergeSink::new(&srcs, heap_at, writer);
        let (mut ia, mut jb) = (0usize, 0usize);
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
                        match payload.compare(schema, &srcs[0], ia, &srcs[1], jb) {
                            Ordering::Less => {
                                sink.push(0, ia, ia + 1);
                                ia += 1;
                            }
                            Ordering::Greater => {
                                sink.push(1, jb, jb + 1);
                                jb += 1;
                            }
                            // One element carried by both sides: `+` sums its
                            // two weights into one row, and a zero sum drops it
                            // (§2), which is why this merge can emit fewer rows
                            // than it read.
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
        (sink.rows, sink.dead)
    });
    out.charge_dead(dead);
    out
}

// ---------------------------------------------------------------------------
// Single-batch sort + consolidation
// ---------------------------------------------------------------------------

/// Sort a single batch by (PK, payload) and consolidate: sum the weights of
/// identical rows, drop ghosts. Appends one `(0, row, net weight)` survivor per
/// group to `out` — [`run_merge`]'s single-source counterpart, in the shape
/// [`super::scatter::scatter_unified_sources`] materializes and the caller sizes
/// its arena from.
pub(crate) fn consolidate_groups(batch: &MemBatch, schema: &SchemaDescriptor, out: &mut Vec<(u32, u32, i64)>) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    with_payload_cmp!(schema, consolidate_groups_inner, n, batch, schema, out)
}

/// Whether `batch`'s rows already stand as [`consolidate_groups`] would leave
/// them: strictly (PK, payload)-ascending, with no ghost. One forward pass, which
/// the first pair out of order ends.
pub(crate) fn in_consolidated_order(batch: &Batch) -> bool {
    if batch.count == 0 {
        return true;
    }
    let schema = batch.schema();
    let stride = schema.pk_stride();
    let ascending = pk_width_dispatch!(stride, |K| {
        let mut keys = batch.pk_data().chunks_exact(stride).map(K::from_opk);
        let mut prev = keys.next().expect("a batch of at least one row");
        keys.zip(1..).all(|(key, i)| {
            // The payload order is read only where two rows share a PK.
            let below = Ord::cmp(&prev, &key).then_with(|| compare_full_rows(schema, batch, i - 1, batch, i));
            prev = key;
            below.is_lt()
        })
    });
    ascending && !batch.has_ghost()
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
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PackedEntry([u128; 2]);

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

/// The argsort half of [`consolidate_groups`]: the sort key carries the whole
/// OPK image, so a key tie goes straight to the payload order.
#[inline]
fn consolidate_groups_inner<P: PayloadOrder>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    out: &mut Vec<(u32, u32, i64)>,
    payload: P,
) {
    let stride = batch.pk_stride();
    if (17..=PACKED_MAX_STRIDE).contains(&stride) {
        let mut entries: Vec<PackedEntry> = (0..n as u32)
            .map(|i| PackedEntry::new(batch.get_pk_bytes(i as usize), i))
            .collect();
        // By PK, then row index; the payload order then applies within each PK.
        entries.sort_unstable();
        if schema.num_payload_cols() > 0 {
            for run in entries.chunk_by_mut(|a, b| a.same_pk(*b)).filter(|r| r.len() > 1) {
                run.sort_unstable_by(|a, b| payload.compare(schema, batch, a.idx(), batch, b.idx()));
            }
        }
        return drain_groups(&entries, batch, schema, payload, out);
    }
    pk_width_dispatch!(stride, |K| {
        let mut entries: Vec<SortEntry<K>> = (0..n as u32)
            .map(|i| SortEntry {
                key: K::from_opk(batch.get_pk_bytes(i as usize)),
                idx: i,
            })
            .collect();
        entries.sort_unstable_by(|a, b| {
            let (x, y) = (a.idx as usize, b.idx as usize);
            Ord::cmp(&a.key, &b.key).then_with(|| payload.compare(schema, batch, x, batch, y))
        });
        drain_groups(&entries, batch, schema, payload, out);
    })
}

/// The fold half of [`consolidate_groups`]: walk the sorted entries and push one
/// survivor per (PK, payload) group whose weights do not cancel.
#[inline]
fn drain_groups<E: ArgEntry, P: PayloadOrder>(
    entries: &[E],
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    payload: P,
    out: &mut Vec<(u32, u32, i64)>,
) {
    let mut pending = entries[0];
    let mut pending_weight = batch.get_weight(pending.idx());

    for &cur in &entries[1..] {
        let (pi, ci) = (pending.idx(), cur.idx());
        if pending.same_pk(cur) && payload.compare(schema, batch, pi, batch, ci).is_eq() {
            pending_weight += batch.get_weight(ci);
        } else {
            if pending_weight != 0 {
                out.push((0, pi as u32, pending_weight));
            }
            pending = cur;
            pending_weight = batch.get_weight(ci);
        }
    }
    if pending_weight != 0 {
        out.push((0, pending.idx() as u32, pending_weight));
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
