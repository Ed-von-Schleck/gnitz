//! In-memory N-way merge for MemTable consolidation.
//!
//! Operates on flat columnar buffers: pk[u128 LE], weight[i64],
//! null_bitmap[u64], payload columns, blob arena.
//!
//! The merge is a fused k-way merge + inline consolidation: rows with the same
//! (PK, payload) have their weights summed; rows whose net weight is zero are dropped.

use std::cell::Cell;
use std::cmp::Ordering;

use super::columnar::{self, ColumnarSource};
use super::batch::FIXED_REGION_BYTES;
use crate::schema::{BlobCache, SchemaDescriptor, type_code, MAX_COLUMNS};
use super::heap::MergeHeap;
use crate::util::read_u64_le;

use type_code::STRING as TYPE_STRING;

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
pub(crate) struct UnifiedSource<'a> {
    pub pk: ColPtr,
    pub null_bmp: ColPtr,
    pub cols: [ColPtr; MAX_COLUMNS - 1],
    pub blob: &'a [u8],
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
    BLOB_CACHE_POOL.try_with(|p| {
        let mut pool = p.take();
        let cache = pool.pop().unwrap_or_default();
        p.set(pool);
        cache
    }).unwrap_or_default()
}

fn recycle_blob_cache(mut cache: BlobCache) {
    if cache.capacity() > BLOB_CACHE_RECYCLE_CAP { return; }
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
        let has_strings = schema.payload_columns()
            .any(|(_, _, col)| col.type_code == TYPE_STRING);
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
pub struct SortedMemBatch<'a>(MemBatch<'a>);

impl<'a> SortedMemBatch<'a> {
    /// Wrap `mb` asserting it is already sorted by (PK, payload).
    /// Use `Batch::as_sorted_mem_batch()` for the checked variant.
    pub(crate) fn new_unchecked(mb: MemBatch<'a>) -> Self {
        SortedMemBatch(mb)
    }
}

impl<'a> std::ops::Deref for SortedMemBatch<'a> {
    type Target = MemBatch<'a>;
    fn deref(&self) -> &MemBatch<'a> { &self.0 }
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
    pub offsets: [u32; super::batch::MAX_BATCH_REGIONS],
    pub pk_stride: u8,       // 8 for U64 PK, 16 for U128 PK
    pub blob: &'a [u8],
    pub count: usize,
}

impl<'a> MemBatch<'a> {
    /// PK region as a contiguous slice (`count * pk_stride` bytes).
    #[inline]
    pub fn pk(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PK] as usize;
        &self.data[off..off + self.count * self.pk_stride as usize]
    }

    /// Weight region as a contiguous slice (`count * 8` bytes).
    #[inline]
    pub fn weight(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_WEIGHT] as usize;
        &self.data[off..off + self.count * 8]
    }

    /// Null bitmap region as a contiguous slice (`count * 8` bytes).
    #[inline]
    pub fn null_bmp(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_NULL_BMP] as usize;
        &self.data[off..off + self.count * 8]
    }

    /// Payload column `pi` as a contiguous slice (`count * stride` bytes).
    /// Caller supplies the stride from the schema (see `payload_columns`).
    #[inline]
    pub fn col_data(&self, pi: usize, stride: usize) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PAYLOAD_START + pi] as usize;
        &self.data[off..off + self.count * stride]
    }

    #[inline(always)]
    pub fn get_pk(&self, row: usize) -> u128 {
        let stride = self.pk_stride as usize;
        let off = self.offsets[super::batch::REG_PK] as usize + row * stride;
        if stride == 16 {
            u128::from_le_bytes(self.data[off..off + 16].try_into().unwrap())
        } else {
            u64::from_le_bytes(self.data[off..off + 8].try_into().unwrap()) as u128
        }
    }
    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        let off = self.offsets[super::batch::REG_WEIGHT] as usize + row * 8;
        i64::from_le_bytes(self.data[off..off + 8].try_into().unwrap())
    }
    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.offsets[super::batch::REG_NULL_BMP] as usize + row * 8)
    }
    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PAYLOAD_START + payload_col] as usize + row * col_size;
        &self.data[off..off + col_size]
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

pub struct MemBatchCursor {
    pub batch_idx: usize,
    pub position: usize,
    pub count: usize,
}

impl MemBatchCursor {
    pub fn new(batch_idx: usize, count: usize) -> Self {
        MemBatchCursor {
            batch_idx,
            position: 0,
            count,
        }
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

    #[inline]
    pub fn peek_key(&self, batches: &[SortedMemBatch]) -> u128 {
        if self.is_valid() {
            batches[self.batch_idx].get_pk(self.position)
        } else {
            u128::MAX
        }
    }
}

// ---------------------------------------------------------------------------
// DirectWriter: writes into pre-allocated output buffers
// ---------------------------------------------------------------------------

pub struct DirectWriter<'a> {
    pk: &'a mut [u8],
    pk_stride: u8,
    weight: &'a mut [u8],
    null_bmp: &'a mut [u8],
    col_bufs: Vec<&'a mut [u8]>,
    /// Growable blob arena; capacity is reserved up-front by `write_to_batch`,
    /// and `blob.len()` doubles as the next-write offset.
    blob: &'a mut Vec<u8>,
    blob_cache: BlobCacheGuard,
    count: usize,
    schema: SchemaDescriptor,
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

    pub fn write_row(
        &mut self,
        batch: &MemBatch,
        row: usize,
        weight: i64,
    ) {
        if weight == 0 {
            return;
        }
        let out_row = self.count;
        self.count += 1;

        let pk = batch.get_pk(row);
        let null_word = batch.get_null_word(row);

        let stride = self.pk_stride as usize;
        debug_assert!(stride == 16 || pk >> 64 == 0, "write_row: U64 batch requires high bits == 0");
        self.pk[out_row * stride..out_row * stride + stride]
            .copy_from_slice(&pk.to_le_bytes()[..stride]);
        self.weight[out_row * 8..out_row * 8 + 8].copy_from_slice(&weight.to_le_bytes());
        self.null_bmp[out_row * 8..out_row * 8 + 8].copy_from_slice(&null_word.to_le_bytes());

        let schema = self.schema;
        for (payload_idx, _ci, col) in schema.payload_columns() {
            let col_size = col.size as usize;
            let is_null = (null_word >> payload_idx) & 1 != 0;

            if is_null {
                let off = out_row * col_size;
                self.col_bufs[payload_idx][off..off + col_size].fill(0);
            } else if col.type_code == TYPE_STRING {
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
    pub(super) fn write_string_cell(
        &mut self,
        payload_col: usize,
        src_struct: &[u8],
        src_blob: &[u8],
        out_row: usize,
    ) {
        let dest = crate::schema::relocate_german_string_vec(
            src_struct, src_blob, self.blob, self.blob_cache.get_mut(),
        );
        let off = out_row * 16;
        self.col_bufs[payload_col][off..off + 16].copy_from_slice(&dest);
    }

    /// Write one row by reading from a `ColumnarSource`. Used by the cursor
    /// scatter path (`ReadCursor::scatter_drained_into`) so the writer's
    /// internal field layout stays private to this module.
    pub(super) fn scatter_row<S: ColumnarSource>(
        &mut self,
        source: &S,
        pk: u128,
        weight: i64,
        row: usize,
    ) {
        let dst_row = self.count;
        let stride = self.pk_stride as usize;
        debug_assert!(stride == 16 || pk >> 64 == 0,
            "scatter_row: U64 batch requires high bits == 0");
        self.pk[dst_row * stride..][..stride]
            .copy_from_slice(&pk.to_le_bytes()[..stride]);
        self.weight[dst_row * 8..][..8]
            .copy_from_slice(&weight.to_le_bytes());
        let null_word = source.get_null_word(row);
        self.null_bmp[dst_row * 8..][..8]
            .copy_from_slice(&null_word.to_le_bytes());

        let src_blob = source.blob_slice();
        let schema = self.schema;
        for (pi, _ci, col) in schema.payload_columns() {
            let cs = col.size as usize;
            if col.type_code == TYPE_STRING {
                let src_struct = source.get_col_ptr(row, pi, 16);
                self.write_string_cell(pi, src_struct, src_blob, dst_row);
            } else {
                let src = source.get_col_ptr(row, pi, cs);
                self.col_bufs[pi][dst_row * cs..][..cs].copy_from_slice(src);
            }
        }
        self.count += 1;
    }

    pub fn row_count(&self) -> usize {
        self.count
    }
}

// ---------------------------------------------------------------------------
// merge_batches: the main entry point
// ---------------------------------------------------------------------------

/// Less-than predicate for the N-way merge heap: orders by PK first, then
/// by full payload via `columnar::compare_rows`.  Equal (PK, payload) entries
/// therefore surface at the heap root in adjacent iterations, which the
/// pending-group drain in `merge_batches` relies on for O(1) consolidation.
///
/// Recreated per-call by callers so the captured borrow of `cursors` stays
/// scoped to a single heap operation, leaving `cursors` free to be mutated
/// between operations.
fn merge_entry_less<'c>(
    cursors: &'c [MemBatchCursor],
    batches: &'c [SortedMemBatch],
    schema: &'c SchemaDescriptor,
) -> impl Fn(&super::heap::HeapNode, &super::heap::HeapNode) -> bool + 'c {
    move |a, b| {
        if a.key != b.key {
            return a.key < b.key;
        }
        let bi = cursors[a.idx].batch_idx;
        let bj = cursors[b.idx].batch_idx;
        let ri = cursors[a.idx].position;
        let rj = cursors[b.idx].position;
        columnar::compare_rows(schema, &batches[bi].0, ri, &batches[bj].0, rj) == Ordering::Less
    }
}

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
pub fn merge_batches(
    batches: &[SortedMemBatch],
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batches.len();
    if n == 0 {
        return;
    }

    let mut cursors: Vec<MemBatchCursor> = (0..n)
        .map(|i| MemBatchCursor::new(i, batches[i].count))
        .collect();

    let mut tree = MergeHeap::build(
        cursors.len(),
        |i| {
            if cursors[i].is_valid() {
                Some(cursors[i].peek_key(batches))
            } else {
                None
            }
        },
        merge_entry_less(&cursors, batches, schema),
    );

    let mut has_pending = false;
    let mut pending_batch: usize = 0;
    let mut pending_row: usize = 0;
    let mut pending_pk: u128 = 0;
    let mut pending_weight: i64 = 0;

    while !tree.is_empty() {
        let ci = tree.min_idx();
        let bi = cursors[ci].batch_idx;
        let ri = cursors[ci].position;
        let cur_pk = batches[bi].get_pk(ri);
        let cur_weight = batches[bi].get_weight(ri);

        let same_group = has_pending
            && cur_pk == pending_pk
            && columnar::compare_rows(
                schema,
                &batches[pending_batch].0, pending_row,
                &batches[bi].0, ri,
            ) == Ordering::Equal;

        if same_group {
            pending_weight += cur_weight;
        } else {
            if has_pending && pending_weight != 0 {
                writer.write_row(&batches[pending_batch], pending_row, pending_weight);
            }
            pending_batch = bi;
            pending_row = ri;
            pending_pk = cur_pk;
            pending_weight = cur_weight;
            has_pending = true;
        }

        cursors[ci].advance();
        let new_key = if cursors[ci].is_valid() {
            Some(cursors[ci].peek_key(batches))
        } else {
            None
        };
        tree.advance(ci, new_key, &merge_entry_less(&cursors, batches, schema));
    }

    if has_pending && pending_weight != 0 {
        writer.write_row(&batches[pending_batch], pending_row, pending_weight);
    }
}

// ---------------------------------------------------------------------------
// Single-batch sort + consolidation
// ---------------------------------------------------------------------------

/// Sort a single batch by (PK, payload) and consolidate: sum weights for
/// identical (PK, payload) rows, drop ghosts (net weight == 0).
///
/// Uses Rust's stable sort on an index array — no tournament tree needed.
/// Key-pointer entry: the 16-byte PK travels with the row index so the sort
/// comparator reads the key from the element being positioned, not from a
/// separate array at a random offset.
#[derive(Copy, Clone)]
struct SortEntry {
    pk: u128,
    idx: u32,
}

pub fn sort_and_consolidate(
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    let mut entries: Vec<SortEntry> = (0..n as u32)
        .map(|i| SortEntry { pk: batch.get_pk(i as usize), idx: i })
        .collect();

    entries.sort_unstable_by(|a, b| match a.pk.cmp(&b.pk) {
        Ordering::Equal => columnar::compare_rows(schema, batch, a.idx as usize, batch, b.idx as usize),
        ord => ord,
    });

    drain_groups_into(n, batch, schema, writer, |pos| {
        (entries[pos].idx as usize, entries[pos].pk)
    });
}

/// Weight-fold an already-sorted batch: sum weights for identical (PK, payload)
/// rows and drop ghosts (net weight == 0). Caller must guarantee sorted input.
pub fn fold_sorted(
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batch.count;
    if n == 0 {
        return;
    }
    drain_groups_into(n, batch, schema, writer, |pos| (pos, batch.get_pk(pos)));
}

/// Shared pending-group drain loop used by `sort_and_consolidate` and `fold_sorted`.
///
/// `resolve(pos)` maps an iteration position to `(batch_row_idx, pk)`.
/// For `sort_and_consolidate` this is an indirection through a sorted index array;
/// for `fold_sorted` the input is already sorted so `pos == batch_row_idx`.
fn drain_groups_into(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
    resolve: impl Fn(usize) -> (usize, u128),
) {
    let (mut pending_idx, mut pending_pk) = resolve(0);
    let mut pending_weight = batch.get_weight(pending_idx);

    for pos in 1..n {
        let (cur_idx, cur_pk) = resolve(pos);
        let same_group = cur_pk == pending_pk
            && columnar::compare_rows(schema, batch, pending_idx, batch, cur_idx) == Ordering::Equal;

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

/// Scatter-copy rows from a batch at the given indices.
/// Indices are NOT sorted — rows are written in the order given.
/// If `weights` is non-empty, uses weights[i] for row i; otherwise reads
/// the weight from the source batch at indices[i].
pub fn scatter_copy(
    batch: &MemBatch,
    indices: &[u32],
    weights: &[i64],
    writer: &mut DirectWriter,
) {
    if indices.is_empty() { return; }

    if !weights.is_empty() {
        // Explicit-weight path (consolidation merge): row-by-row with zero-weight skip.
        for (i, &idx) in indices.iter().enumerate() {
            let w = weights[i];
            if w != 0 {
                writer.write_row(batch, idx as usize, w);
            }
        }
        return;
    }

    // Column-first scatter (repartition / join hot path).
    // Input must not contain zero-weight rows — callers guarantee this.
    #[cfg(debug_assertions)]
    for &idx in indices {
        debug_assert_ne!(
            batch.get_weight(idx as usize), 0,
            "scatter_copy: zero-weight row at index {idx} (filter before scatter)",
        );
    }
    scatter_col_first(batch, indices, writer);
}

fn scatter_col_first(batch: &MemBatch<'_>, indices: &[u32], writer: &mut DirectWriter<'_>) {
    let n = indices.len();
    let base = writer.count; // first output row for this scatter

    // Fused PK + weight + null_bmp gather: one pass over `indices` instead of three.
    // PK stride dispatches to a const-N helper so the inner loop sees a literal width.
    if writer.pk_stride == 8 {
        scatter_col_first_fixed::<8>(batch, indices, base, writer);
    } else {
        scatter_col_first_fixed::<16>(batch, indices, base, writer);
    }

    let schema = writer.schema;
    for (pi, _ci, col) in schema.payload_columns() {
        let cs = col.size as usize;
        if col.type_code == TYPE_STRING {
            // Blob relocation is sequential per-row; no way to batch.
            for (out, &idx) in indices.iter().enumerate() {
                let row = idx as usize;
                let src_struct = batch.get_col_ptr(row, pi, 16);
                writer.write_string_cell(pi, src_struct, batch.blob, base + out);
            }
        } else {
            // Source null cells are zero by Batch invariant, so we copy
            // unconditionally and let `gather_col` vectorize.
            let src_col = batch.col_data(pi, cs);
            let dst_col = &mut writer.col_bufs[pi][base * cs..];
            match cs {
                1  => gather_col::<1>(src_col, dst_col, indices),
                2  => gather_col::<2>(src_col, dst_col, indices),
                4  => gather_col::<4>(src_col, dst_col, indices),
                8  => gather_col::<8>(src_col, dst_col, indices),
                16 => gather_col::<16>(src_col, dst_col, indices),
                _  => {
                    for (out, &idx) in indices.iter().enumerate() {
                        let i = idx as usize;
                        dst_col[out * cs..][..cs].copy_from_slice(&src_col[i * cs..][..cs]);
                    }
                }
            }
        }
    }

    writer.count += n;
}

// Fused PK + weight + null_bmp gather in one pass over `indices`, replacing three
// separate gather_col calls. Caller invariant: every index < batch.count; writer
// slices sized for at least (base + indices.len()) rows.
#[inline(always)]
fn scatter_col_first_fixed<const PKS: usize>(
    batch: &MemBatch<'_>,
    indices: &[u32],
    base: usize,
    writer: &mut DirectWriter<'_>,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pk_src = batch.pk();
    let wt_src = batch.weight();
    let nb_src = batch.null_bmp();
    let pk_dst = &mut writer.pk[base * PKS..];
    let wt_dst = &mut writer.weight[base * FB..];
    let nb_dst = &mut writer.null_bmp[base * FB..];
    debug_assert!(pk_dst.len() >= indices.len() * PKS);
    debug_assert!(wt_dst.len() >= indices.len() * FB);
    debug_assert!(nb_dst.len() >= indices.len() * FB);
    for (out, &idx) in indices.iter().enumerate() {
        let i = idx as usize;
        debug_assert!((i + 1) * PKS <= pk_src.len());
        debug_assert!((i + 1) * FB <= wt_src.len());
        debug_assert!((i + 1) * FB <= nb_src.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                pk_src.as_ptr().add(i * PKS),
                pk_dst.as_mut_ptr().add(out * PKS),
                PKS,
            );
            std::ptr::copy_nonoverlapping(
                wt_src.as_ptr().add(i * FB),
                wt_dst.as_mut_ptr().add(out * FB),
                FB,
            );
            std::ptr::copy_nonoverlapping(
                nb_src.as_ptr().add(i * FB),
                nb_dst.as_mut_ptr().add(out * FB),
                FB,
            );
        }
    }
}

// `N` is a const so LLVM sees a fixed-width copy and emits optimal load/store code.
// Caller invariant: every `idx` in `indices` is `< src.len() / N`; `dst.len() >= indices.len() * N`.
#[inline(always)]
unsafe fn copy_row<const N: usize>(src: &[u8], dst: &mut [u8], idx: usize, out: usize) {
    std::ptr::copy_nonoverlapping(src.as_ptr().add(idx * N), dst.as_mut_ptr().add(out * N), N);
}

#[inline(always)]
fn gather_col<const N: usize>(src: &[u8], dst: &mut [u8], indices: &[u32]) {
    debug_assert!(dst.len() >= indices.len() * N);
    // On x86_64, prefetch source rows ahead when src exceeds half of L1d and would stall on DRAM.
    #[cfg(target_arch = "x86_64")]
    {
        const AHEAD: usize = 64;
        if src.len() > 16 * 1024 && indices.len() > AHEAD * 2 {
            let end = indices.len() - AHEAD;
            for out in 0..end {
                unsafe {
                    let idx = *indices.get_unchecked(out) as usize;
                    let pi = *indices.get_unchecked(out + AHEAD) as usize;
                    debug_assert!((idx + 1) * N <= src.len());
                    std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                        src.as_ptr().add(pi * N) as *const i8,
                    );
                    copy_row::<N>(src, dst, idx, out);
                }
            }
            for out in end..indices.len() {
                unsafe {
                    let idx = *indices.get_unchecked(out) as usize;
                    debug_assert!((idx + 1) * N <= src.len());
                    copy_row::<N>(src, dst, idx, out);
                }
            }
            return;
        }
    }
    for (out, &idx) in indices.iter().enumerate() {
        let i = idx as usize;
        debug_assert!((i + 1) * N <= src.len());
        unsafe { copy_row::<N>(src, dst, i, out); }
    }
}

/// Column-first scatter from multiple sources in a pre-determined (src_idx, row_idx) order.
///
/// `sources[i]` holds the MemBatch for source `i`; entries in `rows` are `(src_idx, row_idx)`
/// in emission order. Destination writes are sequential per column; source reads are scattered.
/// No zero-weight check — callers must filter before calling.
pub fn scatter_multi_source(
    sources: &[Option<MemBatch<'_>>],
    rows: &[(u8, u32)],
    writer: &mut DirectWriter<'_>,
) {
    if rows.is_empty() { return; }
    #[cfg(debug_assertions)]
    for &(si, ri) in rows {
        let src = sources[si as usize].as_ref().unwrap();
        debug_assert_ne!(
            src.get_weight(ri as usize), 0,
            "scatter_multi_source: zero-weight row at source={si} index={ri}",
        );
    }
    let n = rows.len();
    let base = writer.count;

    if writer.pk_stride == 8 {
        scatter_mb_pk_wt_nbm::<8>(sources, rows, base, writer);
    } else {
        scatter_mb_pk_wt_nbm::<16>(sources, rows, base, writer);
    }

    // One pass per column keeps destination writes sequential.
    let schema = writer.schema;
    for (pi, _ci, col) in schema.payload_columns() {
        let cs = col.size as usize;
        if col.type_code == TYPE_STRING {
            for (out, &(si, ri)) in rows.iter().enumerate() {
                let src = sources[si as usize].as_ref().unwrap();
                let row = ri as usize;
                let src_struct = src.get_col_ptr(row, pi, 16);
                writer.write_string_cell(pi, src_struct, src.blob, base + out);
            }
        } else {
            let dst = &mut writer.col_bufs[pi][base * cs..];
            gather_mb_col_dispatch(sources, rows, pi, cs, dst);
        }
    }

    writer.count += n;
}

// PK stride is `PKS` (literal 8 or 16); weight and null_bmp are always 8.
// Hoisting the PK stride out of the row loop is what unlocks fixed-width
// loads/stores instead of memcpy in the inner loop.
#[inline(always)]
fn scatter_mb_pk_wt_nbm<const PKS: usize>(
    sources: &[Option<MemBatch<'_>>],
    rows: &[(u8, u32)],
    base: usize,
    writer: &mut DirectWriter<'_>,
) {
    const FB: usize = FIXED_REGION_BYTES;
    for (out, &(si, ri)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
        let row = ri as usize;
        let dst_row = base + out;
        let pk_off = src.offsets[super::batch::REG_PK] as usize + row * PKS;
        writer.pk[dst_row * PKS..][..PKS]
            .copy_from_slice(&src.data[pk_off..pk_off + PKS]);
        let w_off = src.offsets[super::batch::REG_WEIGHT] as usize + row * FB;
        writer.weight[dst_row * FB..][..FB]
            .copy_from_slice(&src.data[w_off..w_off + FB]);
        let n_off = src.offsets[super::batch::REG_NULL_BMP] as usize + row * FB;
        writer.null_bmp[dst_row * FB..][..FB]
            .copy_from_slice(&src.data[n_off..n_off + FB]);
    }
}

// Dispatches column-size to a const-N gather; falls back to a runtime-sized
// copy for unusual column widths.
#[inline(always)]
fn gather_mb_col_dispatch(
    sources: &[Option<MemBatch<'_>>],
    rows: &[(u8, u32)],
    pi: usize,
    cs: usize,
    dst: &mut [u8],
) {
    match cs {
        1  => gather_mb_col::<1>(sources, rows, pi, dst),
        2  => gather_mb_col::<2>(sources, rows, pi, dst),
        4  => gather_mb_col::<4>(sources, rows, pi, dst),
        8  => gather_mb_col::<8>(sources, rows, pi, dst),
        16 => gather_mb_col::<16>(sources, rows, pi, dst),
        _  => {
            for (out, &(si, ri)) in rows.iter().enumerate() {
                let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
                let row = ri as usize;
                let src_off = src.offsets[super::batch::REG_PAYLOAD_START + pi] as usize + row * cs;
                dst[out * cs..][..cs].copy_from_slice(&src.data[src_off..src_off + cs]);
            }
        }
    }
}

#[inline(always)]
fn gather_mb_col<const N: usize>(
    sources: &[Option<MemBatch<'_>>],
    rows: &[(u8, u32)],
    pi: usize,
    dst: &mut [u8],
) {
    for (out, &(si, ri)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
        let off = src.offsets[super::batch::REG_PAYLOAD_START + pi] as usize + ri as usize * N;
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.data.as_ptr().add(off),
                dst.as_mut_ptr().add(out * N),
                N,
            );
        }
    }
}

/// Column-first scatter from multiple `UnifiedSource`s with explicit per-row
/// weights from the merge walk.
///
/// Used by `ReadCursor::scatter_drained_into`: the merge produces a *net*
/// consolidated weight per (PK, payload) group, which may differ from any
/// single source's stored weight when multiple inputs contribute to the same
/// group. Callers must filter zero-weight rows before calling — `drain_sorted_into`
/// does so.
///
/// `UnifiedSource` covers both `MemBatch` and `MappedShard` regions through a
/// flat `(base, stride)` representation; `as_unified` (in `read_cursor.rs`)
/// validates encodings and returns `None` for unsupported ones (e.g. Constant
/// pk, TwoValue non-weight regions), in which case the caller uses the
/// row-major loop.
///
/// **Precondition:** every `sources[si]` referenced by `rows[*].0` must be
/// `Some`. The inner loop uses `unwrap_unchecked` for codegen — a `None` slot
/// is UB. `scatter_drained_into` upholds this by aborting the whole fast path
/// if any `as_unified` returns `None`.
pub(crate) fn scatter_unified_sources_with_weights(
    sources: &[Option<UnifiedSource<'_>>],
    rows: &[(u32, u32, i64)],
    writer: &mut DirectWriter<'_>,
) {
    if rows.is_empty() { return; }
    #[cfg(debug_assertions)]
    for &(_si, _ri, w) in rows {
        debug_assert_ne!(
            w, 0,
            "scatter_unified_sources_with_weights: zero-weight row in drain buffer",
        );
    }
    let n = rows.len();
    let base = writer.count;

    // Fused PK + weight + null_bmp pass. PK stride dispatches into a const-N
    // helper so the inner loop sees a literal width (8 or 16) — without that,
    // `copy_from_slice` lowers to memcpy.
    if writer.pk_stride == 8 {
        scatter_unified_pk_wt_nbm::<8>(sources, rows, base, writer);
    } else {
        scatter_unified_pk_wt_nbm::<16>(sources, rows, base, writer);
    }

    let schema = writer.schema;
    for (pi, _ci, col) in schema.payload_columns() {
        let cs = col.size as usize;
        if col.type_code == TYPE_STRING {
            // Blob relocation is per-row regardless; no way to batch.
            for (out, &(si, ri, _)) in rows.iter().enumerate() {
                let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
                let cp = src.cols[pi];
                let p = unsafe { cp.base.add(ri as usize * cp.stride) };
                let src_struct = unsafe { std::slice::from_raw_parts(p, 16) };
                writer.write_string_cell(pi, src_struct, src.blob, base + out);
            }
        } else {
            let dst = &mut writer.col_bufs[pi][base * cs..];
            gather_unified_col_dispatch(sources, rows, pi, cs, dst);
        }
    }

    writer.count += n;
}

// PK stride is `PKS` (literal 8 or 16); weight and null_bmp are always 8.
// `null_bmp.stride == 0` for shard Constant regions, in which case
// `base.add(ri * 0) == base` reads the same bytes for every output row.
#[inline(always)]
fn scatter_unified_pk_wt_nbm<const PKS: usize>(
    sources: &[Option<UnifiedSource<'_>>],
    rows: &[(u32, u32, i64)],
    base: usize,
    writer: &mut DirectWriter<'_>,
) {
    const FB: usize = FIXED_REGION_BYTES;
    for (out, &(si, ri, w)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
        let dst_row = base + out;
        let pk_ptr = unsafe { src.pk.base.add(ri as usize * PKS) };
        let nbm_ptr = unsafe { src.null_bmp.base.add(ri as usize * src.null_bmp.stride) };
        writer.pk[dst_row * PKS..][..PKS]
            .copy_from_slice(unsafe { std::slice::from_raw_parts(pk_ptr, PKS) });
        writer.weight[dst_row * FB..][..FB].copy_from_slice(&w.to_le_bytes());
        writer.null_bmp[dst_row * FB..][..FB]
            .copy_from_slice(unsafe { std::slice::from_raw_parts(nbm_ptr, FB) });
    }
}

#[inline(always)]
fn gather_unified_col_dispatch(
    sources: &[Option<UnifiedSource<'_>>],
    rows: &[(u32, u32, i64)],
    pi: usize,
    cs: usize,
    dst: &mut [u8],
) {
    match cs {
        1  => gather_unified_col::<1>(sources, rows, pi, dst),
        2  => gather_unified_col::<2>(sources, rows, pi, dst),
        4  => gather_unified_col::<4>(sources, rows, pi, dst),
        8  => gather_unified_col::<8>(sources, rows, pi, dst),
        16 => gather_unified_col::<16>(sources, rows, pi, dst),
        _  => {
            for (out, &(si, ri, _)) in rows.iter().enumerate() {
                let src = unsafe {
                    sources.get_unchecked(si as usize).as_ref().unwrap_unchecked()
                };
                let cp = src.cols[pi];
                let p = unsafe { cp.base.add(ri as usize * cp.stride) };
                dst[out * cs..][..cs]
                    .copy_from_slice(unsafe { std::slice::from_raw_parts(p, cs) });
            }
        }
    }
}

// `N` is a const so LLVM emits fixed-width load/store and not a memcpy call.
// `dst` is taken as a raw slice (rather than indexing through `writer.col_bufs`)
// so the bounds check stays out of the hot inner loop.
#[inline(always)]
fn gather_unified_col<const N: usize>(
    sources: &[Option<UnifiedSource<'_>>],
    rows: &[(u32, u32, i64)],
    pi: usize,
    dst: &mut [u8],
) {
    for (out, &(si, ri, _)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
        let cp = src.cols[pi];
        let ptr = unsafe { cp.base.add(ri as usize * cp.stride) };
        unsafe { std::ptr::copy_nonoverlapping(ptr, dst.as_mut_ptr().add(out * N), N) };
    }
}

/// Sort a single batch by (PK, payload) WITHOUT consolidation.
/// All N input rows produce N output rows — no weight merging, no ghost elimination.
/// Duplicate (PK, payload) entries are preserved as separate rows.
#[allow(dead_code)]
pub fn sort_only(
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    let mut entries: Vec<SortEntry> = (0..n as u32)
        .map(|i| SortEntry { pk: batch.get_pk(i as usize), idx: i })
        .collect();

    entries.sort_unstable_by(|a, b| match a.pk.cmp(&b.pk) {
        Ordering::Equal => columnar::compare_rows(schema, batch, a.idx as usize, batch, b.idx as usize),
        ord => ord,
    });

    for e in &entries {
        let idx = e.idx as usize;
        writer.write_row(batch, idx, batch.get_weight(idx));
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::batch::Batch;
    use crate::schema::{SchemaColumn, SchemaDescriptor, MAX_COLUMNS};

    fn make_schema_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; MAX_COLUMNS];
        // col 0 = PK (U128, size 16)
        columns[0] = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
        // col 1 = I64 value column
        columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };

        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
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

    fn read_pk_u128(out_pk: &[u8], i: usize) -> u128 {
        u128::from_le_bytes(out_pk[i * 16..(i + 1) * 16].try_into().unwrap())
    }

    // Takes &[Batch] so test call sites don't need to construct SortedMemBatch.
    fn run_merge(batches: &[Batch], schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
        let mem_batches: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let sorted: Vec<SortedMemBatch> = mem_batches.iter()
            .map(|mb| SortedMemBatch::new_unchecked(mb.clone()))
            .collect();
        let total_rows: usize = sorted.iter().map(|b| b.count).sum();
        let total_blob: usize = sorted.iter().map(|b| b.blob.len()).sum();

        let mut out_pk = vec![0u8; total_rows * 16];
        let mut out_weight = vec![0u8; total_rows * 8];
        let mut out_null = vec![0u8; total_rows * 8];
        let mut out_col0 = vec![0u8; total_rows * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob: Vec<u8> = Vec::with_capacity(blob_cap);

        {
            let mut writer = DirectWriter {
                pk: &mut out_pk,
                pk_stride: 16,
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
                let pk = read_pk_u128(&out_pk, i);
                let lo = pk as u64;
                let hi = (pk >> 64) as u64;
                let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
                let v = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
                result.push((lo, hi, w, v));
            }
            return result;
        }
    }

    #[test]
    fn test_single_batch_passthrough() {
        let schema = make_schema_i64();
        let batch = make_batch_i64(&[
            (10, 1, 100),
            (20, 1, 200),
            (30, 1, 300),
        ]);

        let result = run_merge(&[batch], &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (20, 0, 1, 200));
        assert_eq!(result[2], (30, 0, 1, 300));
    }

    #[test]
    fn test_two_batch_interleave() {
        let schema = make_schema_i64();
        let b1 = make_batch_i64(&[
            (10, 1, 100),
            (30, 1, 300),
        ]);
        let b2 = make_batch_i64(&[
            (20, 1, 200),
            (40, 1, 400),
        ]);

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
        let batches: Vec<Batch> = (0..5)
            .map(|_| make_batch_i64(&[(42, 1, 999)]))
            .collect();
        let result = run_merge(&batches, &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (42, 0, 5, 999));
    }

    #[test]
    fn test_weight_zero_skip() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[
            (10, 0, 100),
            (20, 1, 200),
        ]);

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
        for i in 0..100 {
            assert_eq!(result[i].0, i as u64);
        }
    }

    #[test]
    fn test_within_cursor_duplicates() {
        // Two rows with the same PK within a single sorted batch
        let schema = make_schema_i64();
        let b = make_batch_i64(&[
            (10, 1, 100),
            (10, 1, 100),
            (20, 1, 200),
        ]);

        let result = run_merge(&[b], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (10, 0, 2, 100));
        assert_eq!(result[1], (20, 0, 1, 200));
    }

    #[test]
    fn test_within_cursor_dup_different_payload() {
        // Two rows with same PK but different payload within one batch
        let schema = make_schema_i64();
        let b = make_batch_i64(&[
            (10, 1, 100),
            (10, 1, 200),
            (20, 1, 300),
        ]);

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

        let mut out_pk = vec![0u8; n * 16];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob: Vec<u8> = Vec::with_capacity(blob_cap);

        let count;
        {
            let mut writer = DirectWriter {
                pk: &mut out_pk,
                pk_stride: 16,
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
            let pk = read_pk_u128(&out_pk, i);
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
        let b = make_batch_i64(&[
            (1, 1, 10),
            (2, 1, 20),
            (3, 1, 30),
        ]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (1, 0, 1, 10));
        assert_eq!(result[1], (2, 0, 1, 20));
        assert_eq!(result[2], (3, 0, 1, 30));
    }

    #[test]
    fn test_consolidate_unsorted_no_dups() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[
            (3, 1, 30),
            (1, 1, 10),
            (2, 1, 20),
        ]);

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
        let b = make_batch_i64(&[
            (5, 1, 42),
            (5, 1, 42),
        ]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (5, 0, 2, 42));
    }

    #[test]
    fn test_consolidate_ghost_elimination() {
        let schema = make_schema_i64();
        // Same (PK, payload) with +1 and -1 → ghost, eliminated
        let b = make_batch_i64(&[
            (5, 1, 42),
            (5, -1, 42),
        ]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_consolidate_same_pk_different_payload() {
        let schema = make_schema_i64();
        // Same PK but different payloads → both survive, sorted by payload
        let b = make_batch_i64(&[
            (5, 1, 200),
            (5, 1, 100),
        ]);

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
            (10, 1, 100),   // insert pk=10 val=100
            (5, 1, 50),     // insert pk=5 val=50
            (10, -1, 100),  // retract pk=10 val=100
            (5, 1, 50),     // duplicate insert pk=5 val=50
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
        let b = make_batch_i64(&[
            (1, 1, 10),
            (1, -1, 10),
            (2, 3, 20),
            (2, -3, 20),
        ]);

        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 0);
    }

    // -----------------------------------------------------------------------
    // sort_only tests
    // -----------------------------------------------------------------------

    fn run_sort(b: &Batch, schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
        let batch = b.as_mem_batch();
        let n = batch.count;
        let total_blob = batch.blob.len();

        let mut out_pk = vec![0u8; n * 16];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob: Vec<u8> = Vec::with_capacity(blob_cap);

        let count;
        {
            let mut writer = DirectWriter {
                pk: &mut out_pk,
                pk_stride: 16,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_cache: BlobCacheGuard::acquire(schema, 0),
                count: 0,
                schema: *schema,
            };
            sort_only(&batch, schema, &mut writer);
            count = writer.row_count();
        }

        let mut result = Vec::new();
        for i in 0..count {
            let pk = read_pk_u128(&out_pk, i);
            let lo = pk as u64;
            let hi = (pk >> 64) as u64;
            let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
            let val = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
            result.push((lo, hi, w, val));
        }
        result
    }

    #[test]
    fn test_sort_unsorted() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[
            (3, 1, 30),
            (1, 1, 10),
            (2, 1, 20),
        ]);

        let result = run_sort(&b, &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (1, 0, 1, 10));
        assert_eq!(result[1], (2, 0, 1, 20));
        assert_eq!(result[2], (3, 0, 1, 30));
    }

    #[test]
    fn test_sort_preserves_duplicates() {
        let schema = make_schema_i64();
        // Same (PK, payload) with +1 and -1 — both MUST survive (no consolidation)
        let b = make_batch_i64(&[
            (5, -1, 42),
            (5, 1, 42),
            (10, 1, 100),
        ]);

        let result = run_sort(&b, &schema);
        assert_eq!(result.len(), 3); // ALL rows preserved
        assert_eq!(result[0].0, 5);
        assert_eq!(result[1].0, 5);
        assert_eq!(result[2].0, 10);
    }

    #[test]
    fn test_sort_already_sorted() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[
            (1, 1, 10),
            (2, 1, 20),
        ]);

        let result = run_sort(&b, &schema);
        assert_eq!(result, vec![(1, 0, 1, 10), (2, 0, 1, 20)]);
    }

    // -----------------------------------------------------------------------
    // scatter_copy tests
    // -----------------------------------------------------------------------

    fn run_scatter(
        b: &Batch,
        indices: &[u32],
        weights: &[i64],
        schema: &SchemaDescriptor,
    ) -> Vec<(u64, u64, i64, i64)> {
        let batch = b.as_mem_batch();
        let n = indices.len();
        let total_blob = batch.blob.len();
        let mut out_pk = vec![0u8; n * 16];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob: Vec<u8> = Vec::with_capacity(blob_cap);

        let count;
        {
            let mut writer = DirectWriter {
                pk: &mut out_pk,
                pk_stride: 16,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_cache: BlobCacheGuard::acquire(schema, 0),
                count: 0,
                schema: *schema,
            };
            scatter_copy(&batch, indices, weights, &mut writer);
            count = writer.row_count();
        }

        let mut result = Vec::new();
        for i in 0..count {
            let pk = read_pk_u128(&out_pk, i);
            let lo = pk as u64;
            let hi = (pk >> 64) as u64;
            let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
            let val = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
            result.push((lo, hi, w, val));
        }
        result
    }

    #[test]
    fn test_scatter_basic() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[
            (1, 1, 10),
            (2, 1, 20),
            (3, 1, 30),
        ]);

        // Pick rows 2 and 0 (out of order)
        let result = run_scatter(&b, &[2, 0], &[], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (3, 0, 1, 30));
        assert_eq!(result[1], (1, 0, 1, 10));
    }

    #[test]
    fn test_scatter_empty_indices() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[(1, 1, 10)]);

        let result = run_scatter(&b, &[], &[], &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_scatter_with_explicit_weights() {
        let schema = make_schema_i64();
        let b = make_batch_i64(&[
            (1, 1, 10),
            (2, 1, 20),
        ]);

        // Override weights: row 1 gets w=5, row 0 gets w=-1
        let result = run_scatter(&b, &[1, 0], &[5, -1], &schema);
        assert_eq!(result[0], (2, 0, 5, 20));
        assert_eq!(result[1], (1, 0, -1, 10));
    }
}
