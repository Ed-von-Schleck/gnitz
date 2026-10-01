//! The borrowed batch view [`MemBatch`], the columnar source abstraction and
//! the direct row writer over it, and the merges built on them: the N-way run
//! merge, in-batch consolidation, and the two-way batch merge.
//!
//! Operates on flat columnar buffers: pk[OPK big-endian, `pk_stride` B/row],
//! weight[i64 LE], null_bitmap[u64 LE], payload columns, blob arena.
//!
//! The N-way merge is a fused k-way merge + inline consolidation: rows with the
//! same (PK, payload) have their weights summed; rows whose net weight is zero
//! are dropped. [`Batch::merged_consolidated`] is the two-input counterpart over
//! the same comparator family — Z-Set `+`, fold included.

use std::cmp::Ordering;
use std::ops::{ControlFlow, Range};

use super::batch::{Batch, Layout};
use super::loser_tree::{HeapNode, LoserTree};
use super::scatter::DecodedColumns;
use super::seek::pk_group_end;
use super::string_heap::{rebase_string_cell, BlobCache};
use crate::schema::key::{compare_pk_ordering, pk_bytes_eq, pk_width_dispatch, PkSortKey};
use crate::schema::payload_order::{with_payload_cmp, PayloadOrder};
use crate::schema::SchemaDescriptor;
use gnitz_expr::{BatchView, RowSource};
use gnitz_wire::read_u64_le;

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
}

#[derive(Clone, Copy)]
pub(crate) struct UnifiedSource<'a> {
    pub pk: ColPtr,
    pub null_bmp: ColPtr,
    /// Null bits OR'd into every null word the scatter copies out.
    pub null_pad_mask: u64,
    /// Index of this source's first payload `ColPtr` in the caller-owned table
    /// the scatter reads through: column `pi` is `cols[cols_off + pi]`. Out of
    /// line because a by-value `[ColPtr; MAX_COLUMNS]` is 1 KiB zeroed per source
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
        let off = mb.offsets[super::batch::REG_PAYLOAD_START + pi];
        cols.push(ColPtr {
            base: unsafe { data_ptr.add(off) },
            stride: col.size() as usize,
        });
    }
    UnifiedSource {
        pk: ColPtr {
            base: unsafe { data_ptr.add(mb.offsets[super::batch::REG_PK]) },
            stride: mb.pk_stride as usize,
        },
        null_bmp: ColPtr {
            base: unsafe { data_ptr.add(mb.offsets[super::batch::REG_NULL_BMP]) },
            stride: super::batch::FIXED_REGION_BYTES,
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

/// Borrowed slice-view of a `Batch`.
///
/// The full data buffer is referenced as `data: &[u8]`, with `offsets` recording
/// the byte offset of each region (PK, weight, null_bmp, payload_0..N). This
/// lets `Batch::as_mem_batch` return a `MemBatch` without allocating a
/// `Vec<&[u8]>` of column slices.
///
/// Per-column strides are not stored here — callers iterate
/// `schema.payload_columns()` and pass the column size explicitly.
#[derive(Clone)]
pub struct MemBatch<'a> {
    pub(crate) data: &'a [u8],
    /// Borrowed, not owned: `usize` × `MAX_BATCH_REGIONS` is ~½ KiB, and this
    /// view exists to be derived per range, per chunk and per operator call.
    /// `Batch` already holds the array inline, so `as_mem_batch` lends it; the
    /// one view with no owning `Batch`, a borrowed wire frame, borrows it from
    /// its [`super::batch_wire::WalBlock`].
    pub(crate) offsets: &'a [usize; super::batch::MAX_BATCH_REGIONS],
    pub pk_stride: u8, // byte width of the PK region per row
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

    /// PK region as a contiguous slice (`count * pk_stride` bytes).
    #[inline]
    pub fn pk(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PK];
        &self.data[off..off + self.count * self.pk_stride as usize]
    }

    /// Weight region as a contiguous slice (`count * 8` bytes).
    #[inline]
    pub(crate) fn weight(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_WEIGHT];
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
        let off = self.offsets[super::batch::REG_NULL_BMP];
        &self.data[off..off + self.count * 8]
    }

    /// Payload column `pi` as a contiguous slice (`count * stride` bytes).
    /// Caller supplies the stride from the schema (see `payload_columns`).
    #[inline(always)]
    pub fn col_data(&self, pi: usize, stride: usize) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PAYLOAD_START + pi];
        &self.data[off..off + self.count * stride]
    }

    #[inline(always)]
    pub fn get_pk_bytes(&self, row: usize) -> &'a [u8] {
        let stride = self.pk_stride as usize;
        let off = self.offsets[super::batch::REG_PK] + row * stride;
        &self.data[off..off + stride]
    }
    /// The leading `n` bytes of row `row`'s PK.
    #[inline(always)]
    pub(crate) fn get_pk_prefix(&self, row: usize, n: usize) -> &'a [u8] {
        debug_assert!(n <= self.pk_stride as usize);
        let off = self.offsets[super::batch::REG_PK] + row * self.pk_stride as usize;
        &self.data[off..off + n]
    }
    #[inline(always)]
    pub fn get_weight(&self, row: usize) -> i64 {
        gnitz_wire::read_i64_le(self.data, self.offsets[super::batch::REG_WEIGHT] + row * 8)
    }
    #[inline(always)]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.offsets[super::batch::REG_NULL_BMP] + row * 8)
    }
    #[inline(always)]
    pub(crate) fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PAYLOAD_START + payload_col] + row * col_size;
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
        (MemBatch::pk(self), self.pk_stride as usize)
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
// DirectWriter: writes into pre-allocated output buffers
// ---------------------------------------------------------------------------

/// Writes rows into the pre-allocated region buffers of a `write_to_batch`
/// arena.
///
/// **Every entry point must write each live byte of each row it counts** — the
/// batch invariant (see `Batch::with_capacity`), and here the whole reason the
/// arena can skip its memset. A skipped cell within a counted row leaks the
/// recycled buffer's previous contents rather than reading back a zero. Hence
/// the `repr::scatter` kernels write PK/weight/null in one fused pass and every
/// payload cell unconditionally, a null cell included. Rows the writer declines
/// to count (a ghost weight) need nothing: every reader bounds the batch to
/// `count`.
pub(crate) struct DirectWriter<'a> {
    // `repr`'s row-copy kernels write these fixed regions directly, so they are
    // `pub(super)`; `blob`/`blob_cache` stay private — the heap is reached only
    // through its methods.
    pub(super) pk: &'a mut [u8],
    pub(super) weight: &'a mut [u8],
    pub(super) null_bmp: &'a mut [u8],
    pub(super) col_bufs: Vec<&'a mut [u8]>,
    /// Growable blob arena; capacity is reserved up-front by `write_to_batch`,
    /// and `blob.len()` doubles as the next-write offset.
    blob: &'a mut Vec<u8>,
    blob_cache: BlobCache,
    pub(super) count: usize,
    /// Borrowed, not owned: the scatter reads it per column, and a
    /// `SchemaDescriptor` is 360 bytes (pinned in `schema`) — copying it in would
    /// put a `memcpy` of that size on every writer open.
    pub schema: &'a SchemaDescriptor,
}

impl<'a> DirectWriter<'a> {
    /// One writable slice per fixed region, carved at `offsets[r]` from
    /// `data`'s own start — so a wire block's header comes out as
    /// the first region's leading pad.
    pub(crate) fn over_regions(
        data: &'a mut [u8],
        offsets: &[usize],
        strides: &[u8],
        rows: usize,
        schema: &'a SchemaDescriptor,
        blob: &'a mut Vec<u8>,
    ) -> Self {
        use super::batch::REG_PAYLOAD_START;
        let nr = strides.len();
        debug_assert!(nr >= REG_PAYLOAD_START, "a carve must cover the three fixed regions");

        let mut fixed: [&mut [u8]; REG_PAYLOAD_START] = [&mut [], &mut [], &mut []];
        let mut col_bufs: Vec<&mut [u8]> = Vec::with_capacity(nr - REG_PAYLOAD_START);
        let mut rest: &mut [u8] = data;
        let mut base = 0usize;
        for r in 0..nr {
            let after_pad = std::mem::take(&mut rest).split_at_mut(offsets[r] - base).1;
            let sz = rows * strides[r] as usize;
            let (region, remainder) = after_pad.split_at_mut(sz);
            match fixed.get_mut(r) {
                Some(slot) => *slot = region,
                None => col_bufs.push(region),
            }
            base = offsets[r] + sz;
            rest = remainder;
        }
        let [pk, weight, null_bmp] = fixed;

        DirectWriter {
            pk,
            weight,
            null_bmp,
            col_bufs,
            blob,
            blob_cache: BlobCache::new(rows),
            count: 0,
            schema,
        }
    }

    /// Write one German-string cell at `out_row`, rebased per
    /// [`rebase_string_cell`].
    #[inline]
    pub(super) fn write_string_cell(
        &mut self,
        payload_col: usize,
        src_struct: &[u8],
        src_blob: &[u8],
        heap_at: Option<usize>,
        out_row: usize,
    ) {
        let off = out_row * 16;
        let dst = &mut self.col_bufs[payload_col][off..off + 16];
        rebase_string_cell(dst, src_struct, src_blob, self.blob, heap_at, &mut self.blob_cache);
    }

    /// Carry a source heap whole onto the end of this writer's heap. Returns the
    /// base the caller shifts its copied cells' offsets by.
    pub(super) fn adopt_heap(&mut self, src_blob: &[u8]) -> usize {
        let base = self.blob.len();
        self.blob.extend_from_slice(src_blob);
        base
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
/// `compare_pk_ordering` (one byte comparator at every width). Each source's
/// walk bound is its own [`RowSource::row_count`].
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
    // merge loop; the PK axis is `compare_pk_ordering` (no stride dispatch).
    with_payload_cmp!(schema, run_merge_body, sources, &mut cursors, schema, emit)
}

/// Z-set `+` of `sources`, each consolidated, as one consolidated batch.
pub fn merge_consolidated(sources: &[MemBatch<'_>], schema: &SchemaDescriptor) -> Batch {
    let mut rows: Vec<(u32, u32, i64)> = Vec::with_capacity(sources.iter().map(|s| s.count).sum());
    run_merge(sources, schema, |src, row, w| rows.push((src as u32, row as u32, w)));
    let mut out = super::scatter::materialize_carrying(sources, schema, &rows);
    out.certify_layout(Layout::Consolidated);
    out
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

/// The heap order [`drive`] and the read cursor share. Generic over the source
/// type, so each caller monomorphizes its own branch-free copy.
///
/// `compare_pk_ordering` on each player's full OPK bytes (exact at every width —
/// no cached key, no stride dispatch), then the `payload` order.
///
/// `coarsen` collapses the payload axis for a cursor holding a skeleton run: a
/// skeleton row sorts before every hydrated row of its PK, making it the exemplar
/// [`drive`] folds the whole PK group into. Tested ahead of `payload`, so a
/// skeleton row's absent columns are never read. A runtime flag, not a second
/// monomorphisation axis: it is constant for a cursor's life and reached only on
/// a PK tie.
#[inline]
pub(crate) fn merge_less<'a, S, P>(
    schema: &'a SchemaDescriptor,
    sources: &'a [S],
    payload: P,
    coarsen: bool,
) -> impl Fn(&HeapNode, &HeapNode) -> bool + Copy + 'a
where
    S: ColumnarSource,
    P: PayloadOrder + 'a,
{
    move |a, b| {
        let (a_src, a_row) = (a.source_idx as usize, a.row as usize);
        let (b_src, b_row) = (b.source_idx as usize, b.row as usize);
        match compare_pk_ordering(sources[a_src].get_pk_bytes(a_row), sources[b_src].get_pk_bytes(b_row)) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => {
                if coarsen {
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

/// Drive an N-way (PK, payload) merge to completion over `sources`, folding each
/// group's weights and calling `emit(group_src, group_row, net_weight)` once per
/// surviving (net ≠ 0) group; a `Break` returns immediately. The output PK is
/// re-derived from `(group_src, group_row)` by the caller — there is no cached
/// key to hand it.
///
/// Every merge in the tree runs through this — the flush/compaction kernel
/// ([`run_merge`]) and the read cursor's advance and drain — and it builds all
/// four closures itself, so no caller can pair a heap order with a mismatched
/// group boundary. `coarsen` is [`merge_less`]'s flag and means the same here.
///
/// `#[inline(always)]`: each caller's `emit` returns a constant `ControlFlow`, so
/// forced inlining folds the branch and drops the unused arm per monomorphisation.
#[inline(always)]
pub(crate) fn drive<S, P>(
    tree: &mut LoserTree,
    schema: &SchemaDescriptor,
    sources: &[S],
    cursors: &mut [PosCursor],
    payload: P,
    coarsen: bool,
    mut emit: impl FnMut(usize, usize, i64) -> ControlFlow<()>,
) where
    S: ColumnarSource,
    P: PayloadOrder,
{
    // `less` and `same_group` read `(source_idx, row)` out of the heap node and
    // never touch `cursors`, so they coexist with the `&mut cursors` `step!` holds.
    let less = merge_less(schema, sources, payload, coarsen);
    let same_group = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        if !pk_bytes_eq(sources[a_src].get_pk_bytes(a_row), sources[b_src].get_pk_bytes(b_row)) {
            return false;
        }
        if coarsen && (sources[a_src].is_skeleton() || sources[b_src].is_skeleton()) {
            return true;
        }
        payload.compare(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Equal
    };
    // Sources are ghost-free, so the advance is a bare `position + 1`.
    macro_rules! step {
        ($src:expr) => {{
            let c = &mut cursors[$src];
            c.advance();
            let next = c.is_valid().then(|| c.position as u32);
            tree.step_top(next, &less);
        }};
    }

    while let Some(top) = tree.peek() {
        let (group_src, group_row) = (top.source_idx as usize, top.row as usize);

        // Open the group: take the root's weight and step past it. The first row
        // is the exemplar, so `same_group` would be tautologically true — and its
        // payload term walks every column.
        let mut net_weight: i64 = sources[group_src].get_weight(group_row);
        step!(group_src);

        // Fold tied rows: each iteration peeks the new root, breaks at the group
        // boundary, otherwise accumulates weight and steps again.
        while let Some(top) = tree.peek() {
            let (cur_src, cur_row) = (top.source_idx as usize, top.row as usize);
            if !same_group(group_src, group_row, cur_src, cur_row) {
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
    let mut tree = LoserTree::build(
        cursors.len(),
        |i| cursors[i].is_valid().then(|| cursors[i].position as u32),
        merge_less(schema, sources, payload, false),
    );
    // `coarsen: false` — skeleton folding is a read-path concern, and compaction
    // re-materializes per PK anyway (`compact::merge_and_route`). The literal also
    // const-folds the skeleton test out of this monomorphisation.
    drive(&mut tree, schema, sources, cursors, payload, false, |src, row, w| {
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
    /// Only the shared-PK arm folds: a galloped run stops at the other side's
    /// head PK, so nothing in it can share a (PK, payload) across sides, and a
    /// consolidated input repeats none internally.
    pub fn merged_consolidated(&self, other: &Batch, schema: &SchemaDescriptor) -> Batch {
        debug_assert!(
            self.is_consolidated() && other.is_consolidated(),
            "merged_consolidated: both inputs must be consolidated",
        );
        let mut out = with_payload_cmp!(schema, merged_consolidated_body, self, other, schema);
        out.certify_layout(Layout::Consolidated);
        out
    }
}

#[inline]
fn merged_consolidated_body<P: PayloadOrder>(a: &Batch, b: &Batch, schema: &SchemaDescriptor, payload: P) -> Batch {
    let (n_a, n_b) = (a.count, b.count);
    let (mb_a, mb_b) = (a.as_mem_batch(), b.as_mem_batch());

    let mut out = Batch::with_capacity(schema, n_a + n_b);
    {
        // One session for the whole merge: expected run length is 2 for a set
        // operation's uniform 128-bit PKs, so per-run setup would dominate.
        let mut sink = out.append_session(n_a + n_b);
        let carry_a = sink.carry(&mb_a, &[(0, n_a)]);
        let carry_b = sink.carry(&mb_b, &[(0, n_b)]);
        let (mut ia, mut jb) = (0usize, 0usize);
        while ia < n_a && jb < n_b {
            match compare_pk_ordering(a.get_pk_bytes(ia), b.get_pk_bytes(jb)) {
                // A single-source run: one galloping skip, one bulk append.
                Ordering::Less => {
                    let s = ia;
                    ia = a.advance_to(b.get_pk_bytes(jb), ia);
                    sink.push_ranges(&mb_a, carry_a, &[(s, ia)]);
                }
                Ordering::Greater => {
                    let s = jb;
                    jb = b.advance_to(a.get_pk_bytes(ia), jb);
                    sink.push_ranges(&mb_b, carry_b, &[(s, jb)]);
                }
                // A shared PK: bracket both equal-PK groups and interleave them
                // by payload. The only arm that folds, and the only one that
                // reads row by row.
                Ordering::Equal => {
                    let (ga, gb) = (pk_group_end(a, ia), pk_group_end(b, jb));
                    // The single-source stretch still open, ending at its own
                    // side's live cursor. `flush!` closes it into one push.
                    enum Open {
                        None,
                        A(usize),
                        B(usize),
                    }
                    let mut open = Open::None;
                    macro_rules! flush {
                        () => {
                            match std::mem::replace(&mut open, Open::None) {
                                Open::A(s) => sink.push_ranges(&mb_a, carry_a, &[(s, ia)]),
                                Open::B(s) => sink.push_ranges(&mb_b, carry_b, &[(s, jb)]),
                                Open::None => {}
                            }
                        };
                    }
                    while ia < ga && jb < gb {
                        match payload.compare(schema, &mb_a, ia, &mb_b, jb) {
                            Ordering::Less => {
                                if !matches!(open, Open::A(_)) {
                                    flush!();
                                    open = Open::A(ia);
                                }
                                ia += 1;
                            }
                            Ordering::Greater => {
                                if !matches!(open, Open::B(_)) {
                                    flush!();
                                    open = Open::B(jb);
                                }
                                jb += 1;
                            }
                            // One element carried by both sides: `+` sums its two
                            // weights into one row, and a zero sum drops it (§2),
                            // which is why this merge can emit fewer rows than it read.
                            Ordering::Equal => {
                                flush!();
                                let w = a.get_weight(ia) + b.get_weight(jb);
                                sink.push_row_at(&mb_a, carry_a, ia, w);
                                sink.leave_out(&mb_b, carry_b, jb);
                                ia += 1;
                                jb += 1;
                            }
                        }
                    }
                    // One group ended, so the open stretch runs on into its own
                    // side's unpicked tail, and the other side's remainder — which
                    // nothing left can fold against — follows.
                    match open {
                        Open::A(s) => {
                            sink.push_ranges(&mb_a, carry_a, &[(s, ga)]);
                            sink.push_ranges(&mb_b, carry_b, &[(jb, gb)]);
                        }
                        Open::B(s) => {
                            sink.push_ranges(&mb_b, carry_b, &[(s, gb)]);
                            sink.push_ranges(&mb_a, carry_a, &[(ia, ga)]);
                        }
                        Open::None => {
                            sink.push_ranges(&mb_a, carry_a, &[(ia, ga)]);
                            sink.push_ranges(&mb_b, carry_b, &[(jb, gb)]);
                        }
                    }
                    // The only advance that is not an `advance_to`.
                    ia = ga;
                    jb = gb;
                }
            }
        }
        sink.push_ranges(&mb_a, carry_a, &[(ia, n_a)]);
        sink.push_ranges(&mb_b, carry_b, &[(jb, n_b)]);
    }
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

/// A `(sort key, row-index)` pair. Keeps the key co-located with its index so the
/// comparator reads from the element being positioned rather than chasing a
/// separate key array.
#[derive(Copy, Clone)]
struct SortEntry<K> {
    key: K,
    idx: u32,
}

/// The argsort half of [`consolidate_groups`]: [`PkSortKey`] carries the whole
/// OPK image, so a key tie goes straight to the payload order.
#[inline]
fn consolidate_groups_inner<P: PayloadOrder>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    out: &mut Vec<(u32, u32, i64)>,
    payload: P,
) {
    pk_width_dispatch!(batch.pk_stride as usize, |K| {
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
fn drain_groups<K: Copy + Eq, P: PayloadOrder>(
    entries: &[SortEntry<K>],
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    payload: P,
    out: &mut Vec<(u32, u32, i64)>,
) {
    let mut pending = entries[0];
    let mut pending_weight = batch.get_weight(pending.idx as usize);

    for cur in &entries[1..] {
        let (pi, ci) = (pending.idx as usize, cur.idx as usize);
        if pending.key == cur.key && payload.compare(schema, batch, pi, batch, ci).is_eq() {
            pending_weight += batch.get_weight(ci);
        } else {
            if pending_weight != 0 {
                out.push((0, pending.idx, pending_weight));
            }
            pending = *cur;
            pending_weight = batch.get_weight(ci);
        }
    }
    if pending_weight != 0 {
        out.push((0, pending.idx, pending_weight));
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/merge.rs"]
mod tests;
