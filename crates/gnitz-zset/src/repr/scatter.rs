//! Column-first row copy into a `DirectWriter`: choose the rows, then copy them.
//!
//! Two kernels, split on where the output weight comes from. [`scatter_copy`]
//! carries each source row's own. [`scatter_unified_sources`] takes it from the
//! caller's `(src, row, weight)` triple: a `UnifiedSource` has no weight region,
//! so the triple is the only channel for a merge fold's net weight — which is
//! not any one source row's weight. [`gather_rows`] takes the same triples and
//! reads each cell through its source, for picks sparse in their sources.

use std::ops::Range;

use super::batch::{range_rows, write_to_batch, Batch, FIXED_REGION_BYTES};
use super::batch_pool::PooledBuf;
use super::merge::{ColPtr, ColumnarSource, MemBatch, UnifiedSource};
use super::string_heap::{
    carried_dead, prorated_blob_cap, rebase_string_cells, relocate_german_string_vec, row_long_bytes,
};
use super::writer::DirectWriter;
use crate::schema::SchemaDescriptor;

/// Instantiate `$f` at the const width matching `$w`, which is also passed on.
/// The literal width keeps the per-row copy a load/store instead of a `memcpy`
/// call. `N = 0` is the runtime arm: a compound PK stride (U64+U32 = 12) reaches
/// it, a payload column width never does — those targets reject it.
macro_rules! width_dispatch {
    ($w:expr, $f:ident, $($arg:expr),* $(,)?) => {{
        let w = $w;
        match w {
            1 => $f::<1>($($arg,)* w),
            2 => $f::<2>($($arg,)* w),
            4 => $f::<4>($($arg,)* w),
            8 => $f::<8>($($arg,)* w),
            16 => $f::<16>($($arg,)* w),
            _ => $f::<0>($($arg,)* w),
        }
    }};
}
pub(crate) use width_dispatch;

/// Each `[start, end)` run of `width`-byte cells of `src`, copied onto `dst`
/// back to back.
#[inline(always)]
pub(crate) fn copy_runs<const N: usize>(
    src: &[u8],
    dst: &mut [u8],
    runs: impl Iterator<Item = (usize, usize)>,
    width: usize,
) {
    // `N = 0` is the runtime width a compound PK stride takes.
    let w = if N == 0 { width } else { N };
    let mut at = 0usize;
    for (start, end) in runs {
        let n = end - start;
        if n == 1 && N != 0 {
            // A constant width keeps the one-row run a load and a store.
            let cell: [u8; N] = src[start * N..start * N + N].try_into().unwrap();
            dst[at * N..at * N + N].copy_from_slice(&cell);
        } else {
            dst[at * w..(at + n) * w].copy_from_slice(&src[start * w..end * w]);
        }
        at += n;
    }
}

/// Copy every `[start, end)` range of `src`, a batch of the writer's layout, in
/// list order: exactly the writer's rows. String cells are shifted onto `src`'s
/// heap carried at `heap_at`, or relocated.
pub(crate) fn copy_ranges(
    src: &MemBatch<'_>,
    heap_at: Option<usize>,
    ranges: &[(usize, usize)],
    writer: &mut DirectWriter<'_>,
) {
    let schema = writer.schema;
    debug_assert!(
        schema.same_regions(src.schema),
        "copy_ranges: a source of another layout"
    );
    match *ranges {
        // One run is one bulk copy per region.
        [(start, end)] => {
            for r in 0..schema.num_regions() {
                let stride = schema.region_stride(r);
                writer
                    .region_mut(r)
                    .copy_from_slice(&src.region(r)[start * stride..end * stride]);
            }
        }
        // Many runs land back to back, a region at a time: a run of one row is
        // then a load and a store, where a bulk copy is a `memcpy` call.
        _ => {
            debug_assert_eq!(
                range_rows(ranges),
                writer.rows(),
                "copy_ranges: a writer of another row count"
            );
            for r in 0..schema.num_regions() {
                width_dispatch!(
                    schema.region_stride(r),
                    copy_runs,
                    src.region(r),
                    writer.region_mut(r),
                    ranges.iter().copied()
                );
            }
        }
    }
    for pi in gnitz_wire::BitIter(schema.string_payload_slots()) {
        writer.rebase_string_col(pi, src.blob, heap_at);
    }
}

/// Copy the rows `indices` names, in the order given, each carrying its own
/// weight: exactly the writer's rows. A caller supplying its own weights wants
/// [`scatter_unified_sources`].
pub(crate) fn scatter_copy(batch: &MemBatch, indices: &[u32], writer: &mut DirectWriter) {
    assert_eq!(
        indices.len(),
        writer.rows(),
        "scatter_copy: a writer of another row count"
    );
    // Checked once, so the per-row copies below read inside the source.
    let last = indices.iter().fold(0u32, |max, &i| max.max(i));
    assert!(
        indices.is_empty() || (last as usize) < batch.count,
        "scatter_copy: an index past the source's rows"
    );

    #[cfg(debug_assertions)]
    for &idx in indices {
        debug_assert_ne!(
            batch.get_weight(idx as usize),
            0,
            "scatter_copy: zero-weight row at index {idx} (filter before scatter)",
        );
    }

    width_dispatch!(writer.schema.pk_stride(), scatter_pk_wt_nbm, batch, indices, writer);

    let schema = writer.schema;
    for (pi, col) in schema.payload_columns() {
        let cs = col.size() as usize;
        if col.type_code.is_german_string() {
            let src = batch.col_data(pi, 16).as_chunks::<16>().0;
            let (cells, blob, mut cache) = writer.string_col_mut(pi);
            for (cell, &idx) in cells.as_chunks_mut::<16>().0.iter_mut().zip(indices) {
                *cell = relocate_german_string_vec(&src[idx as usize], batch.blob, blob, cache.as_deref_mut());
            }
        } else {
            let src_col = batch.col_data(pi, cs);
            width_dispatch!(cs, gather_col, src_col, writer.col_mut(pi), indices);
        }
    }
}

#[inline(always)]
fn scatter_pk_wt_nbm<const PKS: usize>(
    batch: &MemBatch<'_>,
    indices: &[u32],
    writer: &mut DirectWriter<'_>,
    width: usize,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pks = if PKS == 0 { width } else { PKS };
    let pk_src = batch.pk();
    let wt_src = batch.weight();
    let nb_src = batch.null_bmp();
    let (pk_dst, wt_dst, nb_dst) = writer.fixed_mut();
    assert!(pk_dst.len() == indices.len() * pks && wt_dst.len() == indices.len() * FB);
    for (out, &idx) in indices.iter().enumerate() {
        let i = idx as usize;
        debug_assert!((i + 1) * pks <= pk_src.len());
        debug_assert!((i + 1) * FB <= wt_src.len());
        debug_assert!((i + 1) * FB <= nb_src.len());
        unsafe {
            std::ptr::copy_nonoverlapping(pk_src.as_ptr().add(i * pks), pk_dst.as_mut_ptr().add(out * pks), pks);
            std::ptr::copy_nonoverlapping(wt_src.as_ptr().add(i * FB), wt_dst.as_mut_ptr().add(out * FB), FB);
            std::ptr::copy_nonoverlapping(nb_src.as_ptr().add(i * FB), nb_dst.as_mut_ptr().add(out * FB), FB);
        }
    }
}

#[inline(always)]
fn gather_col<const N: usize>(src: &[u8], dst: &mut [u8], indices: &[u32], width: usize) {
    assert!(N != 0, "a payload column is 1, 2, 4, 8 or 16 bytes, not {width}");
    assert_eq!(dst.len(), indices.len() * N);
    for (out, &idx) in indices.iter().enumerate() {
        let i = idx as usize;
        debug_assert!((i + 1) * N <= src.len());
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr().add(i * N), dst.as_mut_ptr().add(out * N), N);
        }
    }
}

/// Copy the rows `rows` names, in the order given, each at the weight its triple
/// carries: exactly the writer's rows. The sources may be any mix of `MemBatch`
/// and shard backings.
///
/// `cols` is the flat payload-`ColPtr` table the sources were built against;
/// source `si`'s column `pi` is `cols[sources[si].cols_off + pi]`.
pub(crate) fn scatter_unified_sources(
    sources: &[UnifiedSource<'_>],
    cols: &[ColPtr],
    rows: &[(u32, u32, i64)],
    writer: &mut DirectWriter<'_>,
) {
    assert_eq!(
        rows.len(),
        writer.rows(),
        "scatter_unified_sources: a writer of another row count"
    );
    #[cfg(debug_assertions)]
    for &(_si, _ri, w) in rows {
        debug_assert_ne!(w, 0, "scatter_unified_sources: zero-weight row (filter before scatter)");
    }

    width_dispatch!(
        writer.schema.pk_stride(),
        scatter_unified_pk_wt_nbm,
        sources,
        rows,
        writer
    );

    let schema = writer.schema;
    for (pi, col) in schema.payload_columns() {
        let cs = col.size() as usize;
        if col.type_code.is_german_string() {
            let (cells, blob, mut cache) = writer.string_col_mut(pi);
            for (cell, &(si, ri, _)) in cells.as_chunks_mut::<16>().0.iter_mut().zip(rows) {
                let src = unsafe { sources.get_unchecked(si as usize) };
                let src_struct = unsafe { cols.get_unchecked(src.cols_off + pi).row(ri as usize, 16) };
                cell.copy_from_slice(&src_struct[..16]);
                rebase_string_cells(cell, src.blob, blob, src.heap_at, cache.as_deref_mut());
            }
        } else {
            width_dispatch!(cs, gather_unified_col, sources, cols, rows, pi, writer.col_mut(pi));
        }
    }
}

// The destination stride is the writer's; source reads keep `src.pk.stride`, so a
// Constant region repeats its one row for every output row. A null word keeps
// only the writer's payload columns' bits.
#[inline(always)]
fn scatter_unified_pk_wt_nbm<const PKS: usize>(
    sources: &[UnifiedSource<'_>],
    rows: &[(u32, u32, i64)],
    writer: &mut DirectWriter<'_>,
    width: usize,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pks = if PKS == 0 { width } else { PKS };
    let keep = gnitz_wire::low_bits_mask(writer.schema.num_payload_cols());
    let (pk_dst, wt_dst, nbm_dst) = writer.fixed_mut();
    assert!(pk_dst.len() == rows.len() * pks && wt_dst.len() == rows.len() * FB);
    let (pk_dst, wt_dst, nbm_dst) = (pk_dst.as_mut_ptr(), wt_dst.as_mut_ptr(), nbm_dst.as_mut_ptr());
    for (dst_row, &(si, ri, w)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize) };
        let pk_ptr = src.pk.row_ptr(ri as usize);
        let nbm_ptr = src.null_bmp.row_ptr(ri as usize);
        let wb = w.to_le_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(pk_ptr, pk_dst.add(dst_row * pks), pks);
            std::ptr::copy_nonoverlapping(wb.as_ptr(), wt_dst.add(dst_row * FB), FB);
            let nbm = ((nbm_ptr as *const u64).read_unaligned() | src.null_pad_mask) & keep;
            (nbm_dst.add(dst_row * FB) as *mut u64).write_unaligned(nbm);
        }
    }
}

#[inline(always)]
fn gather_unified_col<const N: usize>(
    sources: &[UnifiedSource<'_>],
    cols: &[ColPtr],
    rows: &[(u32, u32, i64)],
    pi: usize,
    dst: &mut [u8],
    width: usize,
) {
    assert!(N != 0, "a payload column is 1, 2, 4, 8 or 16 bytes, not {width}");
    assert_eq!(dst.len(), rows.len() * N);
    for (out, &(si, ri, _)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize) };
        let ptr = unsafe { cols.get_unchecked(src.cols_off + pi).row_ptr(ri as usize) };
        unsafe { std::ptr::copy_nonoverlapping(ptr, dst.as_mut_ptr().add(out * N), N) };
    }
}

/// Copy the rows `picks` names out of `sources`, in the order given, each at
/// the weight its triple carries, into a fresh unconsolidated batch.
///
/// Every cell is read through its source's row accessor, so a packed shard
/// column decodes only the blocks the picks land in: the kernel for picks
/// sparse in their sources, where [`UnifiedSet`] would decode whole windows.
pub(crate) fn gather_rows(
    sources: &[impl ColumnarSource],
    schema: &SchemaDescriptor,
    picks: &[(u32, u32, i64)],
) -> Batch {
    write_to_batch(schema, picks.len(), 0, |w| {
        width_dispatch!(schema.pk_stride(), gather_pk_wt_nbm, sources, picks, w);
        for (pi, col) in schema.payload_columns() {
            if col.type_code.is_german_string() {
                let (cells, blob, _) = w.string_col_mut(pi);
                for (cell, &(si, ri, _)) in cells.as_chunks_mut::<16>().0.iter_mut().zip(picks) {
                    let s = &sources[si as usize];
                    *cell = relocate_german_string_vec(s.get_col_ptr(ri as usize, pi, 16), s.blob(), blob, None);
                }
            } else {
                let dst = w.col_mut(pi);
                width_dispatch!(col.size() as usize, gather_cells, sources, picks, pi, dst);
            }
        }
    })
}

#[inline(always)]
fn gather_pk_wt_nbm<const PKS: usize>(
    sources: &[impl ColumnarSource],
    picks: &[(u32, u32, i64)],
    w: &mut DirectWriter<'_>,
    width: usize,
) {
    let pks = if PKS == 0 { width } else { PKS };
    let keep = gnitz_wire::low_bits_mask(w.schema.num_payload_cols());
    let (pk, weight, null_bmp) = w.fixed_mut();
    let weights = weight.as_chunks_mut::<8>().0.iter_mut();
    let nulls = null_bmp.as_chunks_mut::<8>().0.iter_mut();
    let pk = pk.chunks_exact_mut(pks);
    for (((&(si, ri, weight), pk), wt), nb) in picks.iter().zip(pk).zip(weights).zip(nulls) {
        debug_assert_ne!(weight, 0, "gather_rows: zero-weight row (filter before gather)");
        let s = &sources[si as usize];
        pk.copy_from_slice(&s.get_pk_bytes(ri as usize)[..pks]);
        *wt = weight.to_le_bytes();
        *nb = (s.get_null_word(ri as usize) & keep).to_le_bytes();
    }
}

#[inline(always)]
fn gather_cells<const N: usize>(
    sources: &[impl ColumnarSource],
    picks: &[(u32, u32, i64)],
    pi: usize,
    dst: &mut [u8],
    width: usize,
) {
    assert!(N != 0, "a payload column is 1, 2, 4, 8 or 16 bytes, not {width}");
    for (&(si, ri, _), cell) in picks.iter().zip(dst.as_chunks_mut::<N>().0) {
        *cell = sources[si as usize].get_col_ptr(ri as usize, pi, N).try_into().unwrap();
    }
}

/// The columns a [`UnifiedSet`]'s sources decoded to be viewed, freed with the set.
pub(crate) struct DecodedColumns(Vec<PooledBuf>);

impl DecodedColumns {
    /// Hold `column` for the set's lifetime; its first byte's address.
    pub(crate) fn hold(&mut self, column: PooledBuf) -> *const u8 {
        let base = column.as_ptr();
        self.0.push(column);
        base
    }
}

/// [`scatter_unified_sources`]'s source side: the per-source [`UnifiedSource`]
/// views, the flat payload-`ColPtr` table they index into, and the columns they
/// decoded.
pub(crate) struct UnifiedSet<'a> {
    sources: Vec<UnifiedSource<'a>>,
    cols: Vec<ColPtr>,
    schema: SchemaDescriptor,
    /// Heap bytes and rows of the whole sources, windows notwithstanding.
    src_blob: usize,
    src_rows: usize,
    _decoded: DecodedColumns,
    #[cfg(debug_assertions)]
    windows: Vec<Range<usize>>,
}

impl<'a> UnifiedSet<'a> {
    /// Views over `sources` under `schema`, source `si` readable at rows in the
    /// `si`-th window.
    pub(crate) fn of<S: ColumnarSource>(
        sources: &'a [S],
        schema: &SchemaDescriptor,
        windows: impl IntoIterator<Item = Range<usize>>,
    ) -> Self {
        let mut cols = Vec::with_capacity(sources.len() * schema.num_payload_cols());
        let mut decoded = DecodedColumns(Vec::new());
        #[cfg(debug_assertions)]
        let (windows, each) = {
            let windows: Vec<Range<usize>> = windows.into_iter().collect();
            assert_eq!(windows.len(), sources.len(), "UnifiedSet::of: one window per source");
            (windows.clone(), windows)
        };
        #[cfg(not(debug_assertions))]
        let each = windows;
        let views = sources
            .iter()
            .zip(each)
            .map(|(s, w)| s.to_unified(schema, &mut cols, w, &mut decoded))
            .collect();
        UnifiedSet {
            sources: views,
            cols,
            schema: *schema,
            src_blob: sources.iter().map(|s| s.blob().len()).sum(),
            src_rows: sources.iter().map(|s| s.row_count()).sum(),
            _decoded: decoded,
            #[cfg(debug_assertions)]
            windows,
        }
    }

    /// [`of`](Self::of) with every source readable at every row.
    pub(crate) fn whole<S: ColumnarSource>(sources: &'a [S], schema: &SchemaDescriptor) -> Self {
        Self::of(sources, schema, sources.iter().map(|s| 0..s.row_count()))
    }

    /// Rows across the whole sources.
    pub(crate) fn src_rows(&self) -> usize {
        self.src_rows
    }

    /// Copy the rows `rows` names, in the order given, each at the weight its
    /// triple carries, into a fresh unconsolidated batch under the set's schema.
    /// `of_rows` is the row count of the whole output `rows` is a share of; it
    /// sizes the heap reserved.
    pub(crate) fn materialize(&self, rows: &[(u32, u32, i64)], of_rows: usize) -> Batch {
        #[cfg(debug_assertions)]
        for &(si, ri, _) in rows {
            debug_assert!(
                self.windows[si as usize].contains(&(ri as usize)),
                "UnifiedSet::materialize: row {ri} of source {si} outside its window {:?}",
                self.windows[si as usize],
            );
        }
        let blob_cap = if self.schema.has_german_string() {
            prorated_blob_cap(self.src_blob, of_rows, rows.len())
        } else {
            0
        };
        write_to_batch(&self.schema, rows.len(), blob_cap, |writer| {
            scatter_unified_sources(&self.sources, &self.cols, rows, writer);
        })
    }
}

/// [`UnifiedSet::materialize`] over whole `sources`, each heap carried whole
/// when [`carried_dead`] prefers it for the rows `rows` keeps of that source.
pub(crate) fn materialize_carrying(
    sources: &[MemBatch<'_>],
    schema: &SchemaDescriptor,
    rows: &[(u32, u32, i64)],
) -> Batch {
    let mut set = UnifiedSet::whole(sources, schema);
    let mask = schema.string_payload_slots();
    if mask == 0 {
        return set.materialize(rows, rows.len());
    }
    let mut kept = vec![0usize; sources.len()];
    for &(si, _, _) in rows {
        kept[si as usize] += 1;
    }
    // A carried heap lands after the ones before it in a fresh, empty heap.
    let (mut heap_end, mut dead) = (0, 0);
    let (mut relocated_blob, mut relocated_rows, mut relocated_out) = (0, 0, 0);
    for (si, s) in sources.iter().enumerate() {
        let excluded = || {
            let all: usize = (0..s.count).map(|row| row_long_bytes(s, mask, row)).sum();
            let rows = rows.iter().filter(|&&(src, _, _)| src as usize == si);
            all - rows
                .map(|&(_, row, _)| row_long_bytes(s, mask, row as usize))
                .sum::<usize>()
        };
        match carried_dead(s.blob.len(), s.dead_heap, s.count, kept[si], excluded) {
            Some(d) => {
                set.sources[si].heap_at = Some(heap_end);
                heap_end += s.blob.len();
                dead += d;
            }
            None => {
                relocated_blob += s.blob.len();
                relocated_rows += s.count;
                relocated_out += kept[si];
            }
        }
    }
    let blob_cap = heap_end + prorated_blob_cap(relocated_blob, relocated_rows, relocated_out);
    let mut out = write_to_batch(schema, rows.len(), blob_cap, |writer| {
        for (src, view) in sources.iter().zip(&set.sources) {
            if let Some(at) = view.heap_at {
                let base = writer.adopt_heap(src.blob);
                debug_assert_eq!(base, at, "carried heaps land in source order");
            }
        }
        scatter_unified_sources(&set.sources, &set.cols, rows, writer);
    });
    out.charge_dead(dead);
    out
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/scatter.rs"]
mod tests;
