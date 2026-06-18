//! In-memory N-way merge for MemTable consolidation.
//!
//! Operates on flat columnar buffers: pk[u128 LE], weight[i64],
//! null_bitmap[u64], payload columns, blob arena.
//!
//! The merge is a fused k-way merge + inline consolidation: rows with the same
//! (PK, payload) have their weights summed; rows whose net weight is zero are dropped.

use std::cell::Cell;
use std::cmp::Ordering;

use super::columnar::{with_payload_cmp, with_pk_ord, ColumnarSource, PkOrd, SortEntry};
// `columnar` as a module path is needed only by the test module's
// `compare_pk_bytes`/`compare_rows` calls; dispatch uses `with_payload_cmp!`.
#[cfg(test)]
use super::columnar;
use super::heap::{drive_merge, HeapNode, LoserTree};
use crate::foundation::codec::read_u64_le;
use crate::schema::{BlobCache, RowView, SchemaDescriptor, MAX_COLUMNS};
use gnitz_wire::is_german_string;

// ---------------------------------------------------------------------------
// ColPtr / UnifiedSource: type-erased column accessors that work uniformly
// for in-memory `MemBatch` regions (always Raw, base = data + offset) and
// shard `RegionView::{Raw, Constant}` regions (Raw via mmap offset, Constant
// via inline `value` buffer with stride 0). Stride 0 makes
// `base.add(ri * stride) == base` for every row, so a Constant region reads
// the same bytes for every output row without any branch in the hot loop.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub(crate) struct ColPtr {
    pub base: *const u8,
    pub stride: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct UnifiedSource {
    pub pk: ColPtr,
    pub null_bmp: ColPtr,
    pub cols: [ColPtr; MAX_COLUMNS - 1],
    pub blob_ptr: *const u8,
    pub blob_len: usize,
}

// ---------------------------------------------------------------------------
// Blob cache: TLS-pooled HashMap<(blob_id, offset), new_offset> used by
// `relocate_german_string_vec` to dedupe long-string copies. Allocating the
// HashMap on every scan was hot in the profile; pool it across calls and
// only acquire one when the schema actually contains a STRING column.
// ---------------------------------------------------------------------------

/// Don't recycle caches that grew beyond this many buckets — keeps idle pool
/// memory bounded. Sized for typical merge fan-in of a few thousand unique
/// long-string spans; oversized caches are dropped instead of pooled.
const BLOB_CACHE_RECYCLE_CAP: usize = 65_536;

thread_local! {
    static BLOB_CACHE_POOL: Cell<Vec<BlobCache>> =
        const { Cell::new(Vec::new()) };
}

fn acquire_blob_cache() -> BlobCache {
    BLOB_CACHE_POOL
        .try_with(|p| {
            let mut pool = p.take();
            let cache = pool.pop().unwrap_or_default();
            p.set(pool);
            cache
        })
        .unwrap_or_default()
}

fn recycle_blob_cache(mut cache: BlobCache) {
    if cache.capacity() > BLOB_CACHE_RECYCLE_CAP {
        return;
    }
    cache.clear();
    let _ = BLOB_CACHE_POOL.try_with(|p| {
        let mut pool = p.take();
        pool.push(cache);
        p.set(pool);
    });
}

/// RAII wrapper that returns a pooled blob cache only when the schema has at
/// least one STRING column, and recycles it on drop.
pub(crate) struct BlobCacheGuard(Option<BlobCache>);

impl BlobCacheGuard {
    pub(crate) fn acquire(schema: &SchemaDescriptor, max_rows: usize) -> Self {
        let has_strings = schema
            .payload_columns()
            .any(|(_, _, col)| is_german_string(col.type_code));
        if has_strings {
            let mut cache = acquire_blob_cache();
            cache.reserve(max_rows);
            Self(Some(cache))
        } else {
            Self(None)
        }
    }

    pub(crate) fn get_mut(&mut self) -> Option<&mut BlobCache> {
        self.0.as_mut()
    }
}

impl Drop for BlobCacheGuard {
    fn drop(&mut self) {
        if let Some(cache) = self.0.take() {
            recycle_blob_cache(cache);
        }
    }
}

// ---------------------------------------------------------------------------
// MemBatch: a view over flat columnar buffers (one batch / sorted run)
// ---------------------------------------------------------------------------

/// A `MemBatch` that has been certified sorted by (PK, payload).
///
/// The only ways to obtain one are:
/// - `Batch::as_sorted_mem_batch()` — runtime check on the `sorted` flag
/// - `SortedMemBatch::new_unchecked()` — caller asserts the invariant
///
/// `merge_batches` requires `&[SortedMemBatch]` so the compiler enforces that
/// only certified-sorted inputs reach the N-way merge.
#[repr(transparent)]
pub(crate) struct SortedMemBatch<'a>(MemBatch<'a>);

impl<'a> SortedMemBatch<'a> {
    /// Wrap `mb` asserting it is already sorted by (PK, payload).
    /// Use `Batch::as_sorted_mem_batch()` for the checked variant.
    pub(crate) fn new_unchecked(mb: MemBatch<'a>) -> Self {
        SortedMemBatch(mb)
    }
}

impl<'a> std::ops::Deref for SortedMemBatch<'a> {
    type Target = MemBatch<'a>;
    fn deref(&self) -> &MemBatch<'a> {
        &self.0
    }
}

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
    pub data: &'a [u8],
    // `usize` to match `Batch::offsets`: a large batch's cumulative offset can
    // exceed 4 GB. `as_mem_batch` copies the array verbatim.
    pub offsets: [usize; super::batch::MAX_BATCH_REGIONS],
    pub pk_stride: u8, // byte width of the PK region per row
    pub blob: &'a [u8],
    pub count: usize,
}

impl<'a> MemBatch<'a> {
    /// PK region as a contiguous slice (`count * pk_stride` bytes).
    #[inline]
    pub fn pk(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PK];
        &self.data[off..off + self.count * self.pk_stride as usize]
    }

    /// Weight region as a contiguous slice (`count * 8` bytes).
    #[inline]
    pub fn weight(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_WEIGHT];
        &self.data[off..off + self.count * 8]
    }

    /// Null bitmap region as a contiguous slice (`count * 8` bytes).
    #[inline]
    pub fn null_bmp(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_NULL_BMP];
        &self.data[off..off + self.count * 8]
    }

    /// Payload column `pi` as a contiguous slice (`count * stride` bytes).
    /// Caller supplies the stride from the schema (see `payload_columns`).
    #[inline]
    pub fn col_data(&self, pi: usize, stride: usize) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PAYLOAD_START + pi];
        &self.data[off..off + self.count * stride]
    }

    #[inline(always)]
    pub fn get_pk(&self, row: usize) -> u128 {
        let stride = self.pk_stride as usize;
        let off = self.offsets[super::batch::REG_PK] + row * stride;
        gnitz_wire::widen_pk_be(&self.data[off..off + stride], stride)
    }

    #[inline]
    pub fn get_pk_bytes(&self, row: usize) -> &'a [u8] {
        let stride = self.pk_stride as usize;
        let off = self.offsets[super::batch::REG_PK] + row * stride;
        &self.data[off..off + stride]
    }
    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        let off = self.offsets[super::batch::REG_WEIGHT] + row * 8;
        i64::from_le_bytes(self.data[off..off + 8].try_into().unwrap())
    }
    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.offsets[super::batch::REG_NULL_BMP] + row * 8)
    }
    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PAYLOAD_START + payload_col] + row * col_size;
        &self.data[off..off + col_size]
    }
}

/// `MemBatch` is the sole physical batch the schema-layer locators read through
/// [`RowView`] — the trait lives up in `schema` so the L1 locator types never
/// name this L2 type (breaking the last `schema → storage` up-edge). Each method
/// forwards via UFCS to the inherent accessor of the same name, so the call binds
/// to the concrete read rather than recursing into the trait; `#[inline]` plus
/// monomorphization erase the trait entirely (no vtable — §3 / W2 guardrail).
impl<'b> RowView<'b> for MemBatch<'b> {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        MemBatch::get_null_word(self, row)
    }
    #[inline]
    fn get_pk_bytes(&self, row: usize) -> &'b [u8] {
        MemBatch::get_pk_bytes(self, row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, col: usize, size: usize) -> &'b [u8] {
        MemBatch::get_col_ptr(self, row, col, size)
    }
}

impl<'a> ColumnarSource for MemBatch<'a> {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        self.get_null_word(row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        MemBatch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] {
        self.blob
    }
}

// ---------------------------------------------------------------------------
// MemBatchCursor: position within a MemBatch
// ---------------------------------------------------------------------------

pub(crate) struct MemBatchCursor {
    pub position: usize,
    pub count: usize,
}

impl MemBatchCursor {
    pub fn new(count: usize) -> Self {
        MemBatchCursor { position: 0, count }
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.position < self.count
    }

    #[inline]
    pub fn advance(&mut self) {
        if self.position < self.count {
            self.position += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// DirectWriter: writes into pre-allocated output buffers
// ---------------------------------------------------------------------------

pub struct DirectWriter<'a> {
    // The repartition-scatter cluster (sibling `repr::scatter`) writes these
    // fixed-region buffers directly in its fused per-row loops, so they are
    // `pub(super)` (visible within `repr`); `blob`/`blob_cache` stay private —
    // scatter reaches the heap only through `write_string_cell`.
    pub(super) pk: &'a mut [u8],
    pub(super) pk_stride: u8,
    pub(super) weight: &'a mut [u8],
    pub(super) null_bmp: &'a mut [u8],
    pub(super) col_bufs: Vec<&'a mut [u8]>,
    /// Growable blob arena; capacity is reserved up-front by `write_to_batch`,
    /// and `blob.len()` doubles as the next-write offset.
    blob: &'a mut Vec<u8>,
    blob_cache: BlobCacheGuard,
    pub(super) count: usize,
    pub(super) schema: SchemaDescriptor,
}

impl<'a> DirectWriter<'a> {
    pub fn new(
        pk: &'a mut [u8],
        weight: &'a mut [u8],
        null_bmp: &'a mut [u8],
        col_bufs: Vec<&'a mut [u8]>,
        blob: &'a mut Vec<u8>,
        schema: SchemaDescriptor,
        blob_cache_capacity: usize,
    ) -> Self {
        let pk_stride = super::batch::pk_stride(&schema);
        DirectWriter {
            pk,
            pk_stride,
            weight,
            null_bmp,
            col_bufs,
            blob,
            blob_cache: BlobCacheGuard::acquire(&schema, blob_cache_capacity),
            count: 0,
            schema,
        }
    }

    // `#[inline]`: the only hot caller is `scatter_copy`'s explicit-weight loop,
    // now in the sibling `repr::scatter` module. Same-module placement used to
    // inline this for free; across the module boundary (release builds have no
    // LTO and default codegen-units) the hint restores it.
    #[inline]
    pub fn write_row(&mut self, batch: &MemBatch, row: usize, weight: i64) {
        if weight == 0 {
            return;
        }
        let out_row = self.count;
        self.count += 1;

        let pk_bytes = batch.get_pk_bytes(row);
        let null_word = batch.get_null_word(row);

        let stride = self.pk_stride as usize;
        // extend_pk_bytes already asserts bytes.len() == pk_stride at ingest
        // time, so a stride mismatch is caught there.
        self.pk[out_row * stride..][..stride].copy_from_slice(pk_bytes);
        self.weight[out_row * 8..out_row * 8 + 8].copy_from_slice(&weight.to_le_bytes());
        self.null_bmp[out_row * 8..out_row * 8 + 8].copy_from_slice(&null_word.to_le_bytes());

        let schema = self.schema;
        for (payload_idx, _ci, col) in schema.payload_columns() {
            let col_size = col.size() as usize;
            let is_null = (null_word >> payload_idx) & 1 != 0;

            if is_null {
                let off = out_row * col_size;
                self.col_bufs[payload_idx][off..off + col_size].fill(0);
            } else if is_german_string(col.type_code) {
                let src_struct = batch.get_col_ptr(row, payload_idx, 16);
                self.write_string_cell(payload_idx, src_struct, batch.blob, out_row);
            } else {
                let src = batch.get_col_ptr(row, payload_idx, col_size);
                let off = out_row * col_size;
                self.col_bufs[payload_idx][off..off + col_size].copy_from_slice(src);
            }
        }
    }

    /// Write one 16-byte German string struct from raw source slices.
    ///
    /// `#[inline]`: called per row in the German-string column pass of all three
    /// `repr::scatter` entry points (a sibling module since the carve). Restores
    /// the cross-module inlining that same-module placement gave for free.
    #[inline]
    pub(super) fn write_string_cell(&mut self, payload_col: usize, src_struct: &[u8], src_blob: &[u8], out_row: usize) {
        let dest =
            crate::schema::relocate_german_string_vec(src_struct, src_blob, self.blob, self.blob_cache.get_mut());
        let off = out_row * 16;
        self.col_bufs[payload_col][off..off + 16].copy_from_slice(&dest);
    }

    pub fn row_count(&self) -> usize {
        self.count
    }
}

// ---------------------------------------------------------------------------
// Narrow-region PK key packing
// ---------------------------------------------------------------------------

// `pack_pk_be` / `pk_sort_key` moved to `schema::key`; re-exported so
// `merge::*` / `super::merge::*` call sites and this module's own `pk_sort_key`
// uses are unchanged.
pub(crate) use crate::schema::key::{pack_pk_be, pk_sort_key};

// ---------------------------------------------------------------------------
// merge_batches: the main entry point
// ---------------------------------------------------------------------------

/// Perform N-way merge + consolidation of **sorted** `MemBatch` slices.
///
/// Rows with the same (PK, payload) have their weights summed; zero-weight
/// (PK, payload) groups are dropped.  The payload-aware heap ordering ensures
/// equal (PK, payload) entries appear consecutively at the root, so the
/// single-level pending-group drain below handles both intra-cursor
/// duplicates (consecutive matching rows inside one sorted batch) and
/// cross-cursor duplicates (matching rows in different batches) in one pass.
///
/// The `SortedMemBatch` parameter enforces at the call site that every input
/// is certified sorted by (PK, payload).
pub(crate) fn merge_batches(batches: &[SortedMemBatch], schema: &SchemaDescriptor, writer: &mut DirectWriter) {
    let n = batches.len();
    if n == 0 {
        return;
    }

    let mut cursors: Vec<MemBatchCursor> = (0..n).map(|i| MemBatchCursor::new(batches[i].count)).collect();

    // Dispatch the payload comparator (outer) then the stride comparator (inner),
    // each monomorphizing its own branch-free copy of the merge loop.
    with_payload_cmp!(schema, merge_run, &mut cursors, batches, schema, writer)
}

/// Payload-dispatch layer; defers to the stride dispatch in `merge_run_pk`.
#[inline]
fn merge_run<RowCmp>(
    cursors: &mut [MemBatchCursor],
    batches: &[SortedMemBatch],
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
    row_cmp: RowCmp,
) where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
{
    with_pk_ord!(schema, merge_run_pk, cursors, batches, schema, writer, row_cmp)
}

/// N-way merge closure builder. The keyless heap reads each player's OPK bytes
/// through `(source_idx, row)`: the stride-dispatched `pk_ord` settles the PK
/// axis, then the payload `row_cmp`. `same_pk` is the width-agnostic OPK byte
/// equality (so two distinct wide PKs sharing a 16-byte prefix never fold);
/// `eq_payload` is the payload term.
#[inline]
fn merge_run_pk<RowCmp, PK>(
    cursors: &mut [MemBatchCursor],
    batches: &[SortedMemBatch],
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
    row_cmp: RowCmp,
    pk_ord: PK,
) where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
    PK: PkOrd,
{
    // `less` reads `a.row` / `b.row` from the heap node directly — never
    // touches `cursors` — so it coexists with the `&mut cursors` borrow held
    // by `advance`.  `source_idx` doubles as the batch index here.
    let less = |a: &HeapNode, b: &HeapNode| -> bool {
        let ba = &batches[a.source_idx as usize];
        let bb = &batches[b.source_idx as usize];
        match pk_ord.cmp(ba.get_pk_bytes(a.row as usize), bb.get_pk_bytes(b.row as usize)) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => row_cmp(schema, &ba.0, a.row as usize, &bb.0, b.row as usize) == Ordering::Less,
        }
    };
    let same_pk = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        batches[a_src].get_pk_bytes(a_row) == batches[b_src].get_pk_bytes(b_row)
    };
    let eq_payload = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        row_cmp(schema, &batches[a_src].0, a_row, &batches[b_src].0, b_row) == Ordering::Equal
    };
    merge_batches_inner(cursors, batches, writer, less, same_pk, eq_payload);
}

/// Single generic N-way merge driver body. Monomorphised on the `less` /
/// `same_pk` / `eq_payload` closures its caller selects per stride and payload —
/// each combination compiles to its own branch-free copy of the hot loop. The
/// keyless node carries only the row; `less`/`same_pk` read the OPK bytes through
/// `(source_idx, row)`, and the output PK is re-derived from `(src, row)` in the
/// emit (`write_row` reads `get_pk_bytes`).
#[inline]
fn merge_batches_inner<L, SP, EQ>(
    cursors: &mut [MemBatchCursor],
    batches: &[SortedMemBatch],
    writer: &mut DirectWriter,
    less: L,
    same_pk: SP,
    eq_payload: EQ,
) where
    L: Fn(&HeapNode, &HeapNode) -> bool + Copy,
    SP: Fn(usize, usize, usize, usize) -> bool,
    EQ: Fn(usize, usize, usize, usize) -> bool,
{
    let mut tree = LoserTree::build(
        cursors.len(),
        |i| cursors[i].is_valid().then(|| cursors[i].position as u32),
        less,
    );
    drive_merge(
        &mut tree,
        less,
        |src| {
            cursors[src].advance();
            cursors[src].is_valid().then(|| cursors[src].position as u32)
        },
        same_pk,
        eq_payload,
        |src, row| batches[src].get_weight(row),
        |group_src, group_row, w| {
            writer.write_row(&batches[group_src], group_row, w);
            std::ops::ControlFlow::Continue(())
        },
    );
}

// ---------------------------------------------------------------------------
// Single-batch sort + consolidation
// ---------------------------------------------------------------------------

/// Sort a single batch by (PK, payload) and consolidate: sum weights for
/// identical (PK, payload) rows, drop ghosts (net weight == 0).
///
pub(crate) fn sort_and_consolidate(batch: &MemBatch, schema: &SchemaDescriptor, writer: &mut DirectWriter) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    with_payload_cmp!(schema, sort_consolidate_inner, n, batch, schema, writer)
}

/// Sort-plus-consolidate for all PK widths. Primary key is the order-preserving
/// `pk_sort_key`; ties break on raw OPK bytes (implied-equal/free for narrow,
/// separating wide low-16 collisions) then payload.
#[inline]
fn sort_consolidate_inner<RowCmp>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
    row_cmp: RowCmp,
) where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
{
    let mut entries: Vec<SortEntry> = (0..n as u32)
        .map(|i| SortEntry {
            pk: pk_sort_key(batch.get_pk_bytes(i as usize)),
            idx: i,
        })
        .collect();

    entries.sort_unstable_by(|a, b| match a.pk.cmp(&b.pk) {
        Ordering::Equal => match batch
            .get_pk_bytes(a.idx as usize)
            .cmp(batch.get_pk_bytes(b.idx as usize))
        {
            Ordering::Equal => row_cmp(schema, batch, a.idx as usize, batch, b.idx as usize),
            ord => ord,
        },
        ord => ord,
    });
    drain_groups_into(
        n,
        batch,
        schema,
        writer,
        row_cmp,
        |p, c| batch.get_pk_bytes(p) == batch.get_pk_bytes(c),
        |pos| (entries[pos].idx as usize, entries[pos].pk),
    );
}

/// Weight-fold an already-sorted batch: sum weights for identical (PK, payload)
/// rows and drop ghosts (net weight == 0). Caller must guarantee sorted input.
pub(crate) fn fold_sorted(batch: &MemBatch, schema: &SchemaDescriptor, writer: &mut DirectWriter) {
    let n = batch.count;
    if n == 0 {
        return;
    }
    // No ordered PK comparison here: input is already sorted. Group
    // detection in `drain_groups_into` is the `cur_pk == pending_pk` packed
    // prefix reject plus the exact OPK-byte `pk_eq` (redundant-but-correct for
    // narrow, the real separator for wide low-16 collisions).
    with_payload_cmp!(schema, fold_with, n, batch, schema, writer)
}

/// `fold_sorted` closure dispatcher: forwards to the single generic drain with
/// the exact OPK-byte `pk_eq` for every width.
#[inline]
fn fold_with<RowCmp>(n: usize, batch: &MemBatch, schema: &SchemaDescriptor, writer: &mut DirectWriter, row_cmp: RowCmp)
where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
{
    drain_groups_into(
        n,
        batch,
        schema,
        writer,
        row_cmp,
        |p, c| batch.get_pk_bytes(p) == batch.get_pk_bytes(c),
        |pos| (pos, pack_pk_be(batch.get_pk_bytes(pos))),
    );
}

/// Shared pending-group drain loop used by `sort_and_consolidate` and `fold_sorted`.
///
/// `resolve(pos)` maps an iteration position to `(batch_row_idx, pk)`.
/// For `sort_and_consolidate` this is an indirection through a sorted index array;
/// for `fold_sorted` the input is already sorted so `pos == batch_row_idx`.
///
/// `cur_pk == pending_pk` (the packed `pk_sort_key` prefix) is the O(1)
/// reject — exact for narrow, an inequality filter for wide. `pk_eq` is the
/// exact OPK-byte equality, redundant-but-correct for narrow (the prefix is
/// the whole PK) and the real separator for wide (two distinct PKs sharing a
/// 16-byte prefix). It only runs once the O(1) prefix reject passes.
#[inline]
fn drain_groups_into<RowCmp, PkEq>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
    row_cmp: RowCmp,
    pk_eq: PkEq,
    resolve: impl Fn(usize) -> (usize, u128),
) where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
    PkEq: Fn(usize, usize) -> bool + Copy,
{
    let (mut pending_idx, mut pending_pk) = resolve(0);
    let mut pending_weight = batch.get_weight(pending_idx);

    for pos in 1..n {
        let (cur_idx, cur_pk) = resolve(pos);
        let same_group = cur_pk == pending_pk
            && pk_eq(pending_idx, cur_idx)
            && row_cmp(schema, batch, pending_idx, batch, cur_idx) == Ordering::Equal;

        if same_group {
            pending_weight += batch.get_weight(cur_idx);
        } else {
            if pending_weight != 0 {
                writer.write_row(batch, pending_idx, pending_weight);
            }
            pending_idx = cur_idx;
            pending_pk = cur_pk;
            pending_weight = batch.get_weight(cur_idx);
        }
    }
    if pending_weight != 0 {
        writer.write_row(batch, pending_idx, pending_weight);
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::super::batch::Batch;
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

    fn make_schema_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    /// Build an owned `Batch` from a row tuple list. Tests obtain a `MemBatch`
    /// view via `batch.as_mem_batch()`. Avoids the prior pattern of building
    /// disjoint pk/weight/null/col Vecs, which doesn't fit the new MemBatch
    /// layout (single `data` slice + offsets).
    fn make_batch_i64(rows: &[(u128, i64, i64)]) -> Batch {
        let schema = make_schema_i64();
        let mut b = Batch::with_schema(schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b
    }

    fn empty_batch_i64() -> Batch {
        let schema = make_schema_i64();
        Batch::empty_with_schema(&schema)
    }

    fn read_pk_packed(out_pk: &[u8], i: usize, stride: usize) -> u128 {
        // PK region is OPK (order-preserving big-endian); widen_pk_be recovers
        // the native unsigned value from the right-aligned BE bytes.
        gnitz_wire::widen_pk_be(&out_pk[i * stride..(i + 1) * stride], stride)
    }

    // Takes &[Batch] so test call sites don't need to construct SortedMemBatch.
    fn run_merge(batches: &[Batch], schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
        let mem_batches: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let sorted: Vec<SortedMemBatch> = mem_batches
            .iter()
            .map(|mb| SortedMemBatch::new_unchecked(mb.clone()))
            .collect();
        let total_rows: usize = sorted.iter().map(|b| b.count).sum();
        let total_blob: usize = sorted.iter().map(|b| b.blob.len()).sum();
        let pk_stride = schema.pk_stride() as usize;

        let mut out_pk = vec![0u8; total_rows * pk_stride];
        let mut out_weight = vec![0u8; total_rows * 8];
        let mut out_null = vec![0u8; total_rows * 8];
        let mut out_col0 = vec![0u8; total_rows * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob: Vec<u8> = Vec::with_capacity(blob_cap);

        {
            let mut writer = DirectWriter {
                pk: &mut out_pk,
                pk_stride: pk_stride as u8,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_cache: BlobCacheGuard::acquire(schema, 0),
                count: 0,
                schema: *schema,
            };

            merge_batches(&sorted, schema, &mut writer);

            let count = writer.row_count();
            let mut result = Vec::with_capacity(count);
            for i in 0..count {
                let pk = read_pk_packed(&out_pk, i, pk_stride);
                let lo = pk as u64;
                let hi = (pk >> 64) as u64;
                let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
                let v = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
                result.push((lo, hi, w, v));
            }
            result
        }
    }

    #[test]
    fn test_single_batch_passthrough() {
        let schema = make_schema_i64();
        let batch = make_batch_i64(&[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);

        let result = run_merge(&[batch], &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (20, 0, 1, 200));
        assert_eq!(result[2], (30, 0, 1, 300));
    }

    #[test]
    fn test_two_batch_interleave() {
        let schema = make_schema_i64();
        let b1 = make_batch_i64(&[(10, 1, 100), (30, 1, 300)]);
        let b2 = make_batch_i64(&[(20, 1, 200), (40, 1, 400)]);

        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (20, 0, 1, 200));
        assert_eq!(result[2], (30, 0, 1, 300));
        assert_eq!(result[3], (40, 0, 1, 400));
    }

    #[test]
    fn test_consolidation_same_pk_same_payload() {
        let schema = make_schema_i64();
        let b1 = make_batch_i64(&[(10, 1, 100)]);
        let b2 = make_batch_i64(&[(10, 2, 100)]);

        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (10, 0, 3, 100));
    }

    #[test]
    fn test_consolidation_cancellation() {
        let schema = make_schema_i64();
        let b1 = make_batch_i64(&[(10, 1, 100)]);
        let b2 = make_batch_i64(&[(10, -1, 100)]);

        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_same_pk_different_payload() {
        let schema = make_schema_i64();
        let b1 = make_batch_i64(&[(10, 1, 100)]);
        let b2 = make_batch_i64(&[(10, 1, 200)]);

        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 10);
        assert_eq!(result[1].0, 10);
    }

    #[test]
    fn test_three_way_merge() {
        let schema = make_schema_i64();
        let b1 = make_batch_i64(&[(10, 1, 100), (40, 1, 400)]);
        let b2 = make_batch_i64(&[(20, 1, 200), (50, 1, 500)]);
        let b3 = make_batch_i64(&[(30, 1, 300), (60, 1, 600)]);

        let result = run_merge(&[b1, b2, b3], &schema);
        assert_eq!(result.len(), 6);
        let pks: Vec<u64> = result.iter().map(|r| r.0).collect();
        assert_eq!(pks, vec![10, 20, 30, 40, 50, 60]);
    }

    #[test]
    fn test_empty_batches() {
        let schema = make_schema_i64();
        let result = run_merge(&[], &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_one_empty_one_nonempty() {
        let schema = make_schema_i64();
        let empty = empty_batch_i64();
        let b = make_batch_i64(&[(10, 1, 100)]);

        let result = run_merge(&[empty, b], &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (10, 0, 1, 100));
    }

    #[test]
    fn test_partial_cancellation_three_batches() {
        let schema = make_schema_i64();
        // Insert PK=10 w=+1, PK=20 w=+1
        let b1 = make_batch_i64(&[(10, 1, 100), (20, 1, 200)]);
        // Delete PK=10 w=-1
        let b2 = make_batch_i64(&[(10, -1, 100)]);
        // Insert PK=30 w=+1
        let b3 = make_batch_i64(&[(30, 1, 300)]);
        let result = run_merge(&[b1, b2, b3], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (20, 0, 1, 200));
        assert_eq!(result[1], (30, 0, 1, 300));
    }

    #[test]
    fn test_pk_hi_differentiation() {
        let schema = make_schema_i64();
        let b1 = make_batch_i64(&[(10, 1, 100)]);
        let b2 = make_batch_i64(&[((1u128 << 64) | 10, 1, 200)]);

        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (10, 1, 1, 200));
    }

    #[test]
    fn test_many_duplicates_accumulate() {
        let schema = make_schema_i64();
        // 5 separate single-row batches, same (PK, payload) → merged weight = 5
        let batches: Vec<Batch> = (0..5).map(|_| make_batch_i64(&[(42, 1, 999)])).collect();
        let result = run_merge(&batches, &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (42, 0, 5, 999));
    }

    #[test]
    fn test_weight_zero_skip() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[(10, 0, 100), (20, 1, 200)]);

        let result = run_merge(&[b], &schema);
        // Zero-weight rows just pass through the merge (they don't get consolidated out
        // unless they cancel with another row). A single zero-weight row is still zero.
        // Actually: our merge always outputs pending_weight != 0 check, so zero-weight
        // rows from a single batch get dropped.
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (20, 0, 1, 200));
    }

    #[test]
    fn test_sorted_output_large() {
        let schema = make_schema_i64();
        // 100 rows in reverse order, split into batches of 10
        let mut batches = Vec::new();
        for chunk in 0..10 {
            let base = (9 - chunk) * 10;
            let mut rows = Vec::new();
            for i in 0..10 {
                let pk = (base + i) as u128;
                rows.push((pk, 1i64, (pk * 100) as i64));
            }
            // Sort within each batch (required: inputs are sorted runs)
            rows.sort_by_key(|r| r.0);
            batches.push(make_batch_i64(&rows));
        }
        let result = run_merge(&batches, &schema);
        assert_eq!(result.len(), 100);
        for (i, row) in result.iter().enumerate() {
            assert_eq!(row.0, i as u64);
        }
    }

    #[test]
    fn test_within_cursor_duplicates() {
        // Two rows with the same PK within a single sorted batch
        let schema = make_schema_i64();
        let b = make_batch_i64(&[(10, 1, 100), (10, 1, 100), (20, 1, 200)]);

        let result = run_merge(&[b], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (10, 0, 2, 100));
        assert_eq!(result[1], (20, 0, 1, 200));
    }

    #[test]
    fn test_within_cursor_dup_different_payload() {
        // Two rows with same PK but different payload within one batch
        let schema = make_schema_i64();
        let b = make_batch_i64(&[(10, 1, 100), (10, 1, 200), (20, 1, 300)]);

        let result = run_merge(&[b], &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (10, 0, 1, 200));
        assert_eq!(result[2], (20, 0, 1, 300));
    }

    // -----------------------------------------------------------------------
    // sort_and_consolidate tests
    // -----------------------------------------------------------------------

    fn run_consolidate(b: &Batch, schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
        let batch = b.as_mem_batch();
        let n = batch.count;
        let total_blob = batch.blob.len();
        let pk_stride = schema.pk_stride() as usize;

        let mut out_pk = vec![0u8; n * pk_stride];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob: Vec<u8> = Vec::with_capacity(blob_cap);

        let count;
        {
            let mut writer = DirectWriter {
                pk: &mut out_pk,
                pk_stride: pk_stride as u8,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_cache: BlobCacheGuard::acquire(schema, 0),
                count: 0,
                schema: *schema,
            };
            sort_and_consolidate(&batch, schema, &mut writer);
            count = writer.row_count();
        }

        let mut result = Vec::new();
        for i in 0..count {
            let pk = read_pk_packed(&out_pk, i, pk_stride);
            let lo = pk as u64;
            let hi = (pk >> 64) as u64;
            let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
            let val = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
            result.push((lo, hi, w, val));
        }
        result
    }

    #[test]
    fn test_consolidate_empty() {
        let schema = make_schema_i64();
        let b = empty_batch_i64();
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_consolidate_single_row() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[(5, 1, 42)]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (5, 0, 1, 42));
    }

    #[test]
    fn test_consolidate_already_sorted_no_dups() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (1, 0, 1, 10));
        assert_eq!(result[1], (2, 0, 1, 20));
        assert_eq!(result[2], (3, 0, 1, 30));
    }

    #[test]
    fn test_consolidate_unsorted_no_dups() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[(3, 1, 30), (1, 1, 10), (2, 1, 20)]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 3);
        // Should be sorted by PK
        assert_eq!(result[0], (1, 0, 1, 10));
        assert_eq!(result[1], (2, 0, 1, 20));
        assert_eq!(result[2], (3, 0, 1, 30));
    }

    #[test]
    fn test_consolidate_dup_weight_accumulation() {
        let schema = make_schema_i64();
        // Same (PK, payload) with +1 and +1 → merged to +2
        let b = make_batch_i64(&[(5, 1, 42), (5, 1, 42)]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (5, 0, 2, 42));
    }

    #[test]
    fn test_consolidate_ghost_elimination() {
        let schema = make_schema_i64();
        // Same (PK, payload) with +1 and -1 → ghost, eliminated
        let b = make_batch_i64(&[(5, 1, 42), (5, -1, 42)]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_consolidate_same_pk_different_payload() {
        let schema = make_schema_i64();
        // Same PK but different payloads → both survive, sorted by payload
        let b = make_batch_i64(&[(5, 1, 200), (5, 1, 100)]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 2);
        // Sorted by payload (I64 signed comparison: 100 < 200)
        assert_eq!(result[0], (5, 0, 1, 100));
        assert_eq!(result[1], (5, 0, 1, 200));
    }

    #[test]
    fn test_consolidate_unsorted_mixed() {
        let schema = make_schema_i64();
        // Unsorted: insert + retract + different PKs
        let b = make_batch_i64(&[
            (10, 1, 100),  // insert pk=10 val=100
            (5, 1, 50),    // insert pk=5 val=50
            (10, -1, 100), // retract pk=10 val=100
            (5, 1, 50),    // duplicate insert pk=5 val=50
        ]);

        let result = run_consolidate(&b, &schema);
        // pk=10 val=100: +1-1=0 → ghost
        // pk=5 val=50: +1+1=+2
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (5, 0, 2, 50));
    }

    #[test]
    fn test_consolidate_all_cancel() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[(1, 1, 10), (1, -1, 10), (2, 3, 20), (2, -3, 20)]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 0);
    }

    // -----------------------------------------------------------------------
    // MemBatch PK-accessor tests (OPK byte view vs widened value)
    // -----------------------------------------------------------------------

    fn make_schema_u64_pk() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    #[test]
    fn mem_batch_get_pk_bytes_matches_get_pk_u128() {
        let schema = make_schema_i64();
        let pks: &[u128] = &[0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX];
        let mut b = Batch::with_schema(schema, pks.len());
        for &pk in pks {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        let mb = b.as_mem_batch();
        assert_eq!(mb.pk_stride, 16);
        for (i, &pk) in pks.iter().enumerate() {
            let bytes = mb.get_pk_bytes(i);
            assert_eq!(bytes.len(), 16, "row {i} stride");
            // PK region is OPK (order-preserving big-endian) at rest.
            assert_eq!(bytes, &pk.to_be_bytes(), "row {i} opk bytes");
            assert_eq!(mb.get_pk(i), pk, "row {i} u128");
        }
    }

    #[test]
    fn mem_batch_get_pk_bytes_matches_get_pk_u64() {
        let schema = make_schema_u64_pk();
        let pks: &[u64] = &[0, 1, 1 << 32, u64::MAX];
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(pks.len());
        for &pk in pks {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        let mb = b.as_mem_batch();
        assert_eq!(mb.pk_stride, 8);
        for (i, &pk) in pks.iter().enumerate() {
            let bytes = mb.get_pk_bytes(i);
            assert_eq!(bytes.len(), 8, "row {i} stride");
            // PK region is OPK (order-preserving big-endian) at rest.
            assert_eq!(bytes, &pk.to_be_bytes(), "row {i} opk bytes");
            assert_eq!(mb.get_pk(i), pk as u128, "row {i} u128");
        }
    }

    // -----------------------------------------------------------------------
    // Narrow-region merge dispatch: signed / narrow-unsigned / compound
    // -----------------------------------------------------------------------

    fn make_schema_single(pk_tc: u8) -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[SchemaColumn::new(pk_tc, 0), SchemaColumn::new(type_code::I64, 0)],
            &[0],
        )
    }

    /// OPK-encode a single PK column's native little-endian bytes. The PK region
    /// is OPK at rest, so test fixtures must store OPK (not native LE) bytes for
    /// the merge/sort path to order them correctly.
    fn opk_pk(le: &[u8], tc: u8) -> Vec<u8> {
        let mut out = vec![0u8; le.len()];
        gnitz_wire::encode_pk_column(le, tc, &mut out);
        out
    }

    /// Build a Batch from `(pk_bytes, weight, payload_i64)` rows. `pk_bytes`
    /// length must equal the schema's pk_stride (asserted by extend_pk_bytes).
    fn make_batch_bytes(schema: &SchemaDescriptor, rows: &[(Vec<u8>, i64, i64)]) -> Batch {
        let mut b = Batch::empty_with_schema(schema);
        b.reserve_rows(rows.len().max(1));
        for (pk, w, val) in rows {
            b.extend_pk_bytes(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b
    }

    fn run_fold(b: &Batch, schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
        let batch = b.as_mem_batch();
        let n = batch.count;
        let pk_stride = schema.pk_stride() as usize;
        let mut out_pk = vec![0u8; n * pk_stride];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let mut out_blob: Vec<u8> = Vec::with_capacity(1);
        let count;
        {
            let mut writer = DirectWriter {
                pk: &mut out_pk,
                pk_stride: pk_stride as u8,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_cache: BlobCacheGuard::acquire(schema, 0),
                count: 0,
                schema: *schema,
            };
            fold_sorted(&batch, schema, &mut writer);
            count = writer.row_count();
        }
        let mut result = Vec::new();
        for i in 0..count {
            let pk = read_pk_packed(&out_pk, i, pk_stride);
            let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
            let v = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
            result.push((pk as u64, (pk >> 64) as u64, w, v));
        }
        result
    }

    fn packed(r: &(u64, u64, i64, i64)) -> u128 {
        ((r.1 as u128) << 64) | r.0 as u128
    }

    fn signed_values(stride: usize) -> Vec<i128> {
        match stride {
            1 => vec![i8::MIN as i128, -1, 0, 1, i8::MAX as i128],
            2 => vec![i16::MIN as i128, -1, 0, 1, i16::MAX as i128],
            4 => vec![i32::MIN as i128, -1, 0, 1, i32::MAX as i128],
            8 => vec![i64::MIN as i128, -1, 0, 1, i64::MAX as i128],
            _ => unreachable!(),
        }
    }

    fn le_bytes_i(v: i128, stride: usize) -> Vec<u8> {
        v.to_le_bytes()[..stride].to_vec()
    }

    fn check_signed_pk(pk_tc: u8) {
        let schema = make_schema_single(pk_tc);
        let stride = schema.pk_stride() as usize;
        let vals = signed_values(stride); // ascending signed order
        let n = vals.len();
        // Permutation of 0..5 so the sort/heap actually has to reorder.
        let order = [2usize, 0, 4, 1, 3];
        let expect: Vec<i64> = (0..n as i64).collect();
        // OPK bytes for a native signed value (BE with the sign bit flipped).
        let opk = |v: i128| opk_pk(&le_bytes_i(v, stride), pk_tc);
        // read_pk_packed is widen_pk_be, which yields the *unsigned* reading of
        // the OPK bytes; the oracle is the same widening of the expected OPK.
        let expect_pk = |v: i128| gnitz_wire::widen_pk_be(&opk(v), stride);

        // merge_batches: one single-row sorted batch per row.
        let batches: Vec<Batch> = order
            .iter()
            .map(|&idx| make_batch_bytes(&schema, &[(opk(vals[idx]), 1, idx as i64)]))
            .collect();
        let m = run_merge(&batches, &schema);
        assert_eq!(m.len(), n, "tc={pk_tc} merge len");
        assert_eq!(
            m.iter().map(|r| r.3).collect::<Vec<_>>(),
            expect,
            "tc={pk_tc} merge signed order"
        );
        for (i, r) in m.iter().enumerate() {
            assert_eq!(packed(r), expect_pk(vals[i]), "tc={pk_tc} merge pk decode row {i}");
        }

        // sort_and_consolidate: one unsorted batch.
        let unsorted: Vec<(Vec<u8>, i64, i64)> = order.iter().map(|&idx| (opk(vals[idx]), 1, idx as i64)).collect();
        let c = run_consolidate(&make_batch_bytes(&schema, &unsorted), &schema);
        assert_eq!(
            c.iter().map(|r| r.3).collect::<Vec<_>>(),
            expect,
            "tc={pk_tc} consolidate"
        );

        // fold_sorted: pre-sorted (ascending signed) batch.
        let sorted_rows: Vec<(Vec<u8>, i64, i64)> = (0..n).map(|idx| (opk(vals[idx]), 1, idx as i64)).collect();
        let f = run_fold(&make_batch_bytes(&schema, &sorted_rows), &schema);
        assert_eq!(f.iter().map(|r| r.3).collect::<Vec<_>>(), expect, "tc={pk_tc} fold");
    }

    #[test]
    fn narrow_signed_single_pk_ordering() {
        check_signed_pk(type_code::I8);
        check_signed_pk(type_code::I16);
        check_signed_pk(type_code::I32);
        check_signed_pk(type_code::I64);
    }

    #[test]
    fn narrow_unsigned_single_pk_ordering() {
        let cases: Vec<(u8, usize, Vec<u128>)> = vec![
            (type_code::U8, 1, vec![0, 1, 127, 128, 255]),
            (type_code::U16, 2, vec![0, 1, 256, 32768, 65535]),
            (type_code::U32, 4, vec![0, 1, 1 << 16, 1 << 31, u32::MAX as u128]),
        ];
        for (tc, stride, vals) in cases {
            let schema = make_schema_single(tc);
            assert_eq!(schema.pk_stride() as usize, stride, "tc={tc} stride");
            let n = vals.len();
            let order = [4usize, 2, 0, 3, 1];
            let expect: Vec<i64> = (0..n as i64).collect();
            // Store OPK bytes; widen_pk_be recovers the native unsigned value.
            let le = |idx: usize| opk_pk(&vals[idx].to_le_bytes()[..stride], tc);

            let batches: Vec<Batch> = order
                .iter()
                .map(|&idx| make_batch_bytes(&schema, &[(le(idx), 1, idx as i64)]))
                .collect();
            let m = run_merge(&batches, &schema);
            assert_eq!(m.iter().map(|r| r.3).collect::<Vec<_>>(), expect, "tc={tc} merge");
            for (i, r) in m.iter().enumerate() {
                assert_eq!(packed(r), vals[i], "tc={tc} pk decode row {i}");
            }

            let uns: Vec<(Vec<u8>, i64, i64)> = order.iter().map(|&idx| (le(idx), 1, idx as i64)).collect();
            let c = run_consolidate(&make_batch_bytes(&schema, &uns), &schema);
            assert_eq!(c.iter().map(|r| r.3).collect::<Vec<_>>(), expect, "tc={tc} consolidate");

            let srt: Vec<(Vec<u8>, i64, i64)> = (0..n).map(|idx| (le(idx), 1, idx as i64)).collect();
            let f = run_fold(&make_batch_bytes(&schema, &srt), &schema);
            assert_eq!(f.iter().map(|r| r.3).collect::<Vec<_>>(), expect, "tc={tc} fold");
        }
    }

    #[test]
    fn narrow_compound_u64_u64_lexicographic_trap() {
        // (U64, U64) stride 16. The second PK column lands in the MORE
        // significant u128 bytes after packing, so a plain u128 numeric
        // compare orders col1-major and disagrees with the correct
        // per-column (col0, col1) order. A regression to plain u128 fails.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        assert_eq!(schema.pk_stride(), 16);
        // OPK: each unsigned column big-endian, concatenated in pk-list order.
        let mkpk = |a: u64, b: u64| {
            let mut v = Vec::with_capacity(16);
            v.extend_from_slice(&a.to_be_bytes());
            v.extend_from_slice(&b.to_be_bytes());
            v
        };
        // Correct order: (1,256) < (1,257) < (256,1) < (256,2).
        let rows = vec![
            (mkpk(256, 1), 1i64, 30i64),
            (mkpk(1, 257), 1, 20),
            (mkpk(256, 2), 1, 40),
            (mkpk(1, 256), 1, 10),
        ];
        let c = run_consolidate(&make_batch_bytes(&schema, &rows), &schema);
        assert_eq!(c.iter().map(|r| r.3).collect::<Vec<_>>(), vec![10, 20, 30, 40]);

        // merge_batches across 3 sorted inputs (interleaved tuples).
        let b1 = make_batch_bytes(&schema, &[(mkpk(1, 256), 1, 10), (mkpk(256, 1), 1, 30)]);
        let b2 = make_batch_bytes(&schema, &[(mkpk(1, 257), 1, 20)]);
        let b3 = make_batch_bytes(&schema, &[(mkpk(256, 2), 1, 40)]);
        let m = run_merge(&[b1, b2, b3], &schema);
        assert_eq!(m.iter().map(|r| r.3).collect::<Vec<_>>(), vec![10, 20, 30, 40]);

        // Same (PK, payload) across cursors consolidate; net-zero dropped.
        let d1 = make_batch_bytes(&schema, &[(mkpk(5, 7), 1, 99)]);
        let d2 = make_batch_bytes(&schema, &[(mkpk(5, 7), 2, 99)]);
        let d3 = make_batch_bytes(&schema, &[(mkpk(5, 7), -3, 99)]);
        assert_eq!(run_merge(&[d1, d2, d3], &schema).len(), 0);

        // Same PK, different payload → ordered by payload on PK tie.
        let e = run_consolidate(
            &make_batch_bytes(&schema, &[(mkpk(9, 9), 1, 200), (mkpk(9, 9), 1, 100)]),
            &schema,
        );
        assert_eq!(e.iter().map(|r| r.3).collect::<Vec<_>>(), vec![100, 200]);

        // fold_sorted on pre-sorted compound input.
        let f = run_fold(
            &make_batch_bytes(
                &schema,
                &[(mkpk(1, 256), 1, 10), (mkpk(1, 257), 1, 20), (mkpk(256, 1), 1, 30)],
            ),
            &schema,
        );
        assert_eq!(f.iter().map(|r| r.3).collect::<Vec<_>>(), vec![10, 20, 30]);
    }

    #[test]
    fn narrow_compound_shapes() {
        // (U32, U32) stride 8
        {
            let s = SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U32, 0),
                    SchemaColumn::new(type_code::U32, 0),
                    SchemaColumn::new(type_code::I64, 0),
                ],
                &[0, 1],
            );
            assert_eq!(s.pk_stride(), 8);
            // OPK: each unsigned column big-endian.
            let pk = |a: u32, b: u32| {
                let mut v = Vec::new();
                v.extend_from_slice(&a.to_be_bytes());
                v.extend_from_slice(&b.to_be_bytes());
                v
            };
            // ascending (a,b): (1,9) < (1,10) < (2,0)
            let c = run_consolidate(
                &make_batch_bytes(&s, &[(pk(2, 0), 1, 2), (pk(1, 10), 1, 1), (pk(1, 9), 1, 0)]),
                &s,
            );
            assert_eq!(c.iter().map(|r| r.3).collect::<Vec<_>>(), vec![0, 1, 2]);
        }
        // (U64, U16, U8) stride 11
        {
            let s = SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::U16, 0),
                    SchemaColumn::new(type_code::U8, 0),
                    SchemaColumn::new(type_code::I64, 0),
                ],
                &[0, 1, 2],
            );
            assert_eq!(s.pk_stride(), 11);
            // OPK: each unsigned column big-endian (U8 is a single byte).
            let pk = |a: u64, b: u16, c: u8| {
                let mut v = Vec::new();
                v.extend_from_slice(&a.to_be_bytes());
                v.extend_from_slice(&b.to_be_bytes());
                v.push(c);
                v
            };
            let c = run_consolidate(
                &make_batch_bytes(
                    &s,
                    &[
                        (pk(1, 2, 3), 1, 1),
                        (pk(1, 2, 2), 1, 0),
                        (pk(1, 3, 0), 1, 2),
                        (pk(2, 0, 0), 1, 3),
                    ],
                ),
                &s,
            );
            assert_eq!(c.iter().map(|r| r.3).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
            let f = run_fold(
                &make_batch_bytes(
                    &s,
                    &[
                        (pk(1, 2, 2), 1, 0),
                        (pk(1, 2, 3), 1, 1),
                        (pk(1, 3, 0), 1, 2),
                        (pk(2, 0, 0), 1, 3),
                    ],
                ),
                &s,
            );
            assert_eq!(f.iter().map(|r| r.3).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        }
        // (U64, U32, U32) stride 16
        {
            let s = SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::U32, 0),
                    SchemaColumn::new(type_code::U32, 0),
                    SchemaColumn::new(type_code::I64, 0),
                ],
                &[0, 1, 2],
            );
            assert_eq!(s.pk_stride(), 16);
            // OPK: each unsigned column big-endian.
            let pk = |a: u64, b: u32, c: u32| {
                let mut v = Vec::new();
                v.extend_from_slice(&a.to_be_bytes());
                v.extend_from_slice(&b.to_be_bytes());
                v.extend_from_slice(&c.to_be_bytes());
                v
            };
            let m1 = make_batch_bytes(&s, &[(pk(1, 0, 5), 1, 0), (pk(1, 0, 6), 1, 1)]);
            let m2 = make_batch_bytes(&s, &[(pk(1, 1, 0), 1, 2)]);
            let m3 = make_batch_bytes(&s, &[(pk(2, 0, 0), 1, 3)]);
            let m = run_merge(&[m1, m2, m3], &s);
            assert_eq!(m.iter().map(|r| r.3).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        }
    }

    // -----------------------------------------------------------------------
    // Wide-region merge dispatch (pk_stride > 16): N u64 PK columns. The
    // packed `u128` key is only the order-preserving low-16 OPK prefix here, so
    // ordered comparison and group detection rest on `compare_pk_bytes`.
    // PKs are deliberately constructed so the first two columns (= the low
    // 16 bytes) collide while a later column differs: the prefix fast-reject
    // does NOT fire, exercising the `compare_pk_bytes` term directly.
    // -----------------------------------------------------------------------

    // `MAX_PK_COLUMNS == 5`, so the 64/80-byte strides use U128 PK columns
    // rather than 8/10 U64 columns.
    const WIDE_24: &[u8] = &[type_code::U64, type_code::U64, type_code::U64];
    const WIDE_64: &[u8] = &[type_code::U128, type_code::U128, type_code::U128, type_code::U128];
    const WIDE_80: &[u8] = &[
        type_code::U128,
        type_code::U128,
        type_code::U128,
        type_code::U128,
        type_code::U128,
    ];

    /// Schema with the given PK column type codes + one I64 payload.
    fn pk_payload_schema(tcs: &[u8]) -> SchemaDescriptor {
        let mut cols: Vec<SchemaColumn> = tcs.iter().map(|&t| SchemaColumn::new(t, 0)).collect();
        cols.push(SchemaColumn::new(type_code::I64, 0));
        let pk: Vec<u32> = (0..tcs.len() as u32).collect();
        SchemaDescriptor::new(&cols, &pk)
    }

    fn col_w(tc: u8) -> usize {
        match tc {
            type_code::U64 => 8,
            type_code::U128 => 16,
            _ => unreachable!(),
        }
    }

    /// OPK PK bytes: col0 = `lead`, last col = `tail`, all middle columns 0.
    /// Each unsigned column is stored big-endian (its OPK form), concatenated
    /// in pk-list order. The packed low-16 prefix is fully determined by the
    /// leading columns (col0, plus zeros), so two PKs sharing `lead` but
    /// differing in `tail` collide in the prefix yet are distinct under
    /// `compare_pk_bytes` (which orders col0 → … → last). Holds for every
    /// stride here.
    fn wpk(tcs: &[u8], lead: u128, tail: u128) -> Vec<u8> {
        let last = tcs.len() - 1;
        let mut v = Vec::new();
        for (i, &tc) in tcs.iter().enumerate() {
            let val: u128 = if i == 0 {
                lead
            } else if i == last {
                tail
            } else {
                0
            };
            let cw = col_w(tc);
            // Big-endian = OPK for an unsigned column: take the low `cw` bytes
            // of the BE u128 representation (the value fits within `cw` here).
            v.extend_from_slice(&val.to_be_bytes()[16 - cw..]);
        }
        v
    }

    /// Run `run` against a fresh single-payload-column `DirectWriter` and return
    /// the `(pk_bytes, weight, i64_payload)` of each output row. Width-agnostic:
    /// drives both narrow and wide strides (callers build the schema explicitly).
    fn writer_run(
        schema: &SchemaDescriptor,
        total_rows: usize,
        run: impl FnOnce(&mut DirectWriter),
    ) -> Vec<(Vec<u8>, i64, i64)> {
        let stride = schema.pk_stride() as usize;
        let rows = total_rows.max(1);
        let mut out_pk = vec![0u8; rows * stride];
        let mut out_w = vec![0u8; rows * 8];
        let mut out_n = vec![0u8; rows * 8];
        let mut out_c = vec![0u8; rows * 8];
        let mut out_b: Vec<u8> = Vec::with_capacity(1);
        let count;
        {
            let mut writer = DirectWriter {
                pk: &mut out_pk,
                pk_stride: stride as u8,
                weight: &mut out_w,
                null_bmp: &mut out_n,
                col_bufs: vec![&mut out_c],
                blob: &mut out_b,
                blob_cache: BlobCacheGuard::acquire(schema, 0),
                count: 0,
                schema: *schema,
            };
            run(&mut writer);
            count = writer.row_count();
        }
        (0..count)
            .map(|i| {
                let pk = out_pk[i * stride..(i + 1) * stride].to_vec();
                let w = i64::from_le_bytes(out_w[i * 8..i * 8 + 8].try_into().unwrap());
                let v = i64::from_le_bytes(out_c[i * 8..i * 8 + 8].try_into().unwrap());
                (pk, w, v)
            })
            .collect()
    }

    fn run_merge_wide(batches: &[Batch], schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
        let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let sorted: Vec<SortedMemBatch> = mem.iter().map(|mb| SortedMemBatch::new_unchecked(mb.clone())).collect();
        let total: usize = sorted.iter().map(|b| b.count).sum();
        writer_run(schema, total, |w| {
            merge_batches(&sorted, schema, w);
        })
    }

    /// Byte-form consolidation harness: returns `(pk_bytes, weight, i64_payload)`
    /// per output row. Width-agnostic (sibling of the packed-`u64` `run_consolidate`
    /// above, for PK shapes whose bytes don't fit a single `u64`).
    fn run_consolidate_bytes(b: &Batch, schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
        let mb = b.as_mem_batch();
        let total = mb.count;
        writer_run(schema, total, |w| {
            sort_and_consolidate(&mb, schema, w);
        })
    }

    fn run_fold_wide(b: &Batch, schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
        let mb = b.as_mem_batch();
        let total = mb.count;
        writer_run(schema, total, |w| {
            fold_sorted(&mb, schema, w);
        })
    }

    #[test]
    fn wide_nway_merge_ordering_low16_collision() {
        for (tcs, want_stride) in [(WIDE_24, 24usize), (WIDE_64, 64), (WIDE_80, 80)] {
            let s = pk_payload_schema(tcs);
            assert_eq!(s.pk_stride() as usize, want_stride);
            // A < B < C < D < E by compare_pk_bytes. B,C,D share col0
            // (= the low-16 prefix); only the trailing column distinguishes
            // them, so the packed-prefix reject cannot separate them.
            let a = wpk(tcs, 0, 0);
            let b = wpk(tcs, 1, 0);
            let c = wpk(tcs, 1, 1);
            let d = wpk(tcs, 1, 2);
            let e = wpk(tcs, 2, 0);
            // Three sorted batches, each internally ordered.
            let b1 = make_batch_bytes(&s, &[(a.clone(), 1, 0), (c.clone(), 1, 2)]);
            let b2 = make_batch_bytes(&s, &[(b.clone(), 1, 1), (e.clone(), 1, 4)]);
            let b3 = make_batch_bytes(&s, &[(d.clone(), 1, 3)]);
            let out = run_merge_wide(&[b1, b2, b3], &s);
            assert_eq!(
                out.len(),
                5,
                "stride={want_stride}: distinct low16-colliding PKs must not fold"
            );
            assert_eq!(
                out.iter().map(|r| r.2).collect::<Vec<_>>(),
                vec![0, 1, 2, 3, 4],
                "stride={want_stride}: payload order tracks compare_pk_bytes order",
            );
            let expect_pks = [a, b, c, d, e];
            for (i, r) in out.iter().enumerate() {
                assert_eq!(r.0, expect_pks[i], "stride={want_stride}: pk row {i}");
                assert_eq!(r.1, 1, "stride={want_stride}: weight row {i}");
            }
            // Output is fully ordered under the column-aware comparator.
            for w in out.windows(2) {
                assert_eq!(
                    columnar::compare_pk_bytes(&w[0].0, &w[1].0),
                    Ordering::Less,
                    "stride={want_stride}: output not strictly ordered by compare_pk_bytes",
                );
            }
        }
    }

    #[test]
    fn wide_distinct_pk_identical_payload_not_folded() {
        // Two rows: different wide PKs sharing the 16-byte prefix, identical
        // (weight-1, val=42) payload. Without the compare_pk_bytes term in
        // the wide `eq_payload` these would fold into one summed group.
        let tcs = WIDE_24;
        let s = pk_payload_schema(tcs);
        let p1 = wpk(tcs, 7, 0);
        let p2 = wpk(tcs, 7, 1);
        let b1 = make_batch_bytes(&s, &[(p1.clone(), 1, 42)]);
        let b2 = make_batch_bytes(&s, &[(p2.clone(), 1, 42)]);
        let out = run_merge_wide(&[b1, b2], &s);
        assert_eq!(out.len(), 2, "prefix-colliding distinct PKs must stay separate");
        assert_eq!(out[0], (p1, 1, 42));
        assert_eq!(out[1], (p2, 1, 42));
    }

    #[test]
    fn wide_consolidation_sum_ghost_and_payload_order() {
        let tcs = WIDE_64;
        let s = pk_payload_schema(tcs);
        let p = wpk(tcs, 5, 5);

        // Same PK + same payload across two batches → weights sum.
        let sum = run_merge_wide(
            &[
                make_batch_bytes(&s, &[(p.clone(), 1, 9)]),
                make_batch_bytes(&s, &[(p.clone(), 2, 9)]),
            ],
            &s,
        );
        assert_eq!(sum, vec![(p.clone(), 3, 9)]);

        // Net-zero (ghost) dropped.
        let ghost = run_merge_wide(
            &[
                make_batch_bytes(&s, &[(p.clone(), 1, 9)]),
                make_batch_bytes(&s, &[(p.clone(), -1, 9)]),
            ],
            &s,
        );
        assert_eq!(ghost.len(), 0);

        // Same PK, different payload → two rows in payload order.
        let two = run_merge_wide(
            &[
                make_batch_bytes(&s, &[(p.clone(), 1, 200)]),
                make_batch_bytes(&s, &[(p.clone(), 1, 100)]),
            ],
            &s,
        );
        assert_eq!(two.iter().map(|r| r.2).collect::<Vec<_>>(), vec![100, 200]);
        assert!(two.iter().all(|r| r.0 == p && r.1 == 1));
    }

    /// PIN — root adjacency of equal-(PK, payload) rows across batches.
    /// Three single-row batches, all PK=5: b1/b3 carry the *same* payload
    /// (val=100) with opposite weights, b2 carries a different payload
    /// (val=200) and sits between them in batch order. Each batch is trivially
    /// (PK, payload)-sorted, but the matching val=100 rows are NOT adjacent in
    /// batch order.
    ///
    /// The merge heap MUST order by (PK, payload) so the two val=100 rows reach
    /// `drive_merge`'s fold root consecutively and their +1/-1 weights cancel;
    /// val=200 survives at weight 1. A PK-only heap `less` (dropping the
    /// payload tiebreak) leaves the three same-PK rows unordered among
    /// themselves, the fold breaks on the first payload mismatch, and the
    /// +1/-1 pair never folds — leaking a spurious row.
    #[test]
    fn test_merge_same_pk_nonadjacent_payload_interleave() {
        let schema = make_schema_i64();
        let b1 = make_batch_i64(&[(5, 1, 100)]);
        let b2 = make_batch_i64(&[(5, 1, 200)]);
        let b3 = make_batch_i64(&[(5, -1, 100)]);

        let result = run_merge(&[b1, b2, b3], &schema);
        assert_eq!(
            result.len(),
            1,
            "val=100 +1/-1 pair must cancel; only val=200 survives, got {result:?}"
        );
        assert_eq!(result[0], (5, 0, 1, 200));
    }

    #[test]
    fn wide_fold_sorted_prefix_collision_and_identical_payload() {
        let tcs = WIDE_24;
        let s = pk_payload_schema(tcs);
        // Pre-sorted: (1,1,0) and (1,1,1) collide in low 16 AND carry an
        // identical payload (val=0); they must remain two distinct rows.
        // (1,1,2) is a third distinct PK.
        let p0 = wpk(tcs, 1, 0);
        let p1 = wpk(tcs, 1, 1);
        let p2 = wpk(tcs, 1, 2);
        let b = make_batch_bytes(&s, &[(p0.clone(), 1, 0), (p1.clone(), 1, 0), (p2.clone(), 1, 5)]);
        let out = run_fold_wide(&b, &s);
        assert_eq!(out.len(), 3, "distinct prefix-colliding PKs must not fold");
        assert_eq!(out[0], (p0, 1, 0));
        assert_eq!(out[1], (p1, 1, 0));
        assert_eq!(out[2], (p2, 1, 5));

        // Adjacent same PK + same payload DOES fold; net-zero drops.
        let q = wpk(tcs, 2, 2);
        let r = wpk(tcs, 3, 3);
        let b2 = make_batch_bytes(
            &s,
            &[
                (q.clone(), 1, 7),
                (q.clone(), 1, 7),
                (r.clone(), 1, -1),
                (r.clone(), -1, -1),
            ],
        );
        let out2 = run_fold_wide(&b2, &s);
        assert_eq!(out2, vec![(q, 2, 7)]);
    }

    #[test]
    fn wide_sort_and_consolidate_matches_merge() {
        let tcs = WIDE_24;
        let s = pk_payload_schema(tcs);
        // Unsorted rows incl. prefix collisions, a consolidating pair, and a
        // ghost pair. compare_pk_bytes order: (1,1,0) < (1,1,2) < (4,4,4).
        let k0 = wpk(tcs, 1, 0);
        let k1 = wpk(tcs, 1, 2);
        let k2 = wpk(tcs, 4, 4);
        let rows: Vec<(Vec<u8>, i64, i64)> = vec![
            (k2.clone(), 1, 30),
            (k0.clone(), 1, 10),
            (k1.clone(), 1, 20),
            (k0.clone(), 2, 10),  // consolidates with row 1
            (k2.clone(), -1, 30), // ghost-cancels row 0
        ];
        let out = run_consolidate_bytes(&make_batch_bytes(&s, &rows), &s);
        // (4,4,4) cancels out; (1,1,0) sums to weight 3.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], (k0.clone(), 3, 10));
        assert_eq!(out[1], (k1.clone(), 1, 20));

        // Feeding the same rows through merge_batches as one sorted cursor
        // (pre-sorted by compare_pk_bytes) yields the identical result.
        let sorted_rows: Vec<(Vec<u8>, i64, i64)> = vec![
            (k0.clone(), 1, 10),
            (k0.clone(), 2, 10),
            (k1.clone(), 1, 20),
            (k2.clone(), 1, 30),
            (k2.clone(), -1, 30),
        ];
        let via_merge = run_merge_wide(&[make_batch_bytes(&s, &sorted_rows)], &s);
        assert_eq!(via_merge, out);
    }

    // -----------------------------------------------------------------------
    // OPK consolidation-output equivalence (signed / compound / wide)
    //
    // `sort_and_consolidate` now sorts by an order-preserving key instead of a
    // per-comparison typed column decode. These tests pin that the consolidated
    // output is identical to an independent reference built directly from
    // `compare_pk_bytes` + `compare_rows` — the canonical total order — for the
    // signed, compound, and wide PK shapes the new sort key covers.
    // -----------------------------------------------------------------------

    /// Independent reference: argsort by `compare_pk_bytes` then `compare_rows`,
    /// fold consecutive equal `(PK, payload)` groups, drop net-zero ghosts.
    fn consolidate_reference(b: &Batch, schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
        let mb = b.as_mem_batch();
        let n = mb.count;
        let mut idx: Vec<usize> = (0..n).collect();
        idx.sort_by(
            |&x, &y| match columnar::compare_pk_bytes(mb.get_pk_bytes(x), mb.get_pk_bytes(y)) {
                Ordering::Equal => columnar::compare_rows(schema, &mb, x, &mb, y),
                ord => ord,
            },
        );
        let mut out = Vec::new();
        let mut i = 0;
        while i < n {
            let head = idx[i];
            let mut w = mb.get_weight(head);
            i += 1;
            while i < n
                && columnar::compare_pk_bytes(mb.get_pk_bytes(head), mb.get_pk_bytes(idx[i])) == Ordering::Equal
                && columnar::compare_rows(schema, &mb, head, &mb, idx[i]) == Ordering::Equal
            {
                w += mb.get_weight(idx[i]);
                i += 1;
            }
            if w != 0 {
                let pk = mb.get_pk_bytes(head).to_vec();
                let v = i64::from_le_bytes(mb.get_col_ptr(head, 0, 8).try_into().unwrap());
                out.push((pk, w, v));
            }
        }
        out
    }

    fn assert_consolidate_matches_reference(schema: &SchemaDescriptor, rows: &[(Vec<u8>, i64, i64)]) {
        let b = make_batch_bytes(schema, rows);
        let got = run_consolidate_bytes(&b, schema);
        let want = consolidate_reference(&b, schema);
        assert_eq!(got, want, "OPK consolidated output diverged from reference");
        // Independent of the reference's grouping: the OPK sort must leave the
        // output non-descending under the canonical column-aware comparator.
        for w in got.windows(2) {
            assert_ne!(
                columnar::compare_pk_bytes(&w[0].0, &w[1].0),
                Ordering::Greater,
                "OPK output not ordered by compare_pk_bytes",
            );
        }
    }

    /// OPK bytes for a single I64 PK column (BE with the sign bit flipped).
    fn i64_pk(v: i64) -> Vec<u8> {
        opk_pk(&v.to_le_bytes(), type_code::I64)
    }

    /// Decode a single-column OPK PK back to its native I64 value.
    fn i64_from_opk(opk: &[u8]) -> i64 {
        let mut le = [0u8; 8];
        gnitz_wire::decode_pk_column(&opk[..8], type_code::I64, &mut le);
        i64::from_le_bytes(le)
    }

    #[test]
    fn opk_single_signed_i64_negatives() {
        let s = pk_payload_schema(&[type_code::I64]);
        // Unsorted, spanning negatives and extremes, with a fold and a ghost.
        let rows = vec![
            (i64_pk(5), 1, 50),
            (i64_pk(-1), 1, 10),
            (i64_pk(i64::MIN), 1, 99),
            (i64_pk(-1), 2, 10), // folds with row 1 → weight 3
            (i64_pk(i64::MAX), 1, 7),
            (i64_pk(0), 1, 0),
            (i64_pk(5), -1, 50), // ghost-cancels row 0
            (i64_pk(-100), 1, 3),
        ];
        assert_consolidate_matches_reference(&s, &rows);
        // Concrete order check: negatives precede non-negatives.
        let out = run_consolidate_bytes(&make_batch_bytes(&s, &rows), &s);
        let pks: Vec<i64> = out.iter().map(|r| i64_from_opk(&r.0)).collect();
        assert_eq!(pks, vec![i64::MIN, -100, -1, 0, i64::MAX]);
    }

    #[test]
    fn opk_compound_unsigned_first_column_dominates() {
        let s = pk_payload_schema(&[type_code::U32, type_code::U64]);
        // OPK: each unsigned column big-endian, concatenated in pk-list order.
        let mk = |a: u32, b: u64| {
            let mut v = Vec::with_capacity(12);
            v.extend_from_slice(&a.to_be_bytes());
            v.extend_from_slice(&b.to_be_bytes());
            v
        };
        // Second column large enough to invert order under a raw LE u128 compare;
        // first column must still dominate.
        let rows = vec![
            (mk(2, 1), 1, 20),
            (mk(1, u64::MAX), 1, 10),
            (mk(1, u64::MAX), 1, 10), // fold → weight 2
            (mk(1, 5), 1, 11),
        ];
        assert_consolidate_matches_reference(&s, &rows);
    }

    #[test]
    fn opk_compound_mixed_signed_negative_second_column() {
        let s = pk_payload_schema(&[type_code::U64, type_code::I32]);
        // OPK per column: U64 big-endian, I32 big-endian with sign bit flipped.
        let mk = |a: u64, b: i32| {
            let mut v = Vec::with_capacity(12);
            v.extend_from_slice(&a.to_be_bytes());
            v.extend_from_slice(&opk_pk(&b.to_le_bytes(), type_code::I32));
            v
        };
        let rows = vec![
            (mk(1, 0), 1, 1),
            (mk(1, -5), 1, 2), // negative second column sorts before 0
            (mk(1, i32::MIN), 1, 3),
            (mk(0, i32::MAX), 1, 4),
            (mk(1, -5), -1, 2), // ghost
        ];
        assert_consolidate_matches_reference(&s, &rows);
    }

    #[test]
    fn opk_wide_prefix_straddle_and_collision() {
        // WIDE_24: col straddling byte 16 is the third U64. wpk shares the
        // 16-byte OPK prefix when `lead` matches, so the encoder's BE prefix
        // must still order by the trailing column.
        let tcs = WIDE_24;
        let s = pk_payload_schema(tcs);
        let rows = vec![
            (wpk(tcs, 4, 4), 1, 30),
            (wpk(tcs, 1, 2), 1, 20),
            (wpk(tcs, 1, 0), 1, 10),
            (wpk(tcs, 1, 0), 2, 10),  // fold
            (wpk(tcs, 4, 4), -1, 30), // ghost
            (wpk(tcs, 1, 9), 1, 99),
        ];
        assert_consolidate_matches_reference(&s, &rows);
    }

    mod opk_consolidate_proptest {
        use super::*;
        use proptest::prelude::*;

        fn schemas() -> Vec<SchemaDescriptor> {
            vec![
                pk_payload_schema(&[type_code::I64]),
                pk_payload_schema(&[type_code::I32]),
                pk_payload_schema(&[type_code::U32, type_code::U64]),
                pk_payload_schema(&[type_code::U64, type_code::I32]),
                pk_payload_schema(&[type_code::U64, type_code::U64, type_code::U64]),
            ]
        }

        fn arb_rows(stride: usize) -> impl Strategy<Value = Vec<(Vec<u8>, i64, i64)>> {
            // Small weight and payload domains make folds and ghost-cancels
            // likely; random PK bytes exercise the full ordering.
            let row = (prop::collection::vec(any::<u8>(), stride), -3i64..=3i64, 0i64..4i64);
            prop::collection::vec(row, 0..40)
        }

        proptest! {
            /// New `sort_and_consolidate` output equals the `compare_pk_bytes`
            /// reference for every covered PK shape, over random batches.
            #[test]
            fn consolidate_matches_reference(
                (si, rows) in (0usize..5).prop_flat_map(|si| {
                    let stride = schemas()[si].pk_stride() as usize;
                    (Just(si), arb_rows(stride))
                })
            ) {
                let s = schemas()[si];
                assert_consolidate_matches_reference(&s, &rows);
            }
        }
    }
}
