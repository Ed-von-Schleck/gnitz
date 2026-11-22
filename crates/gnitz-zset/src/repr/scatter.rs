//! Column-first row copy into a `DirectWriter`: choose the rows, then copy them.
//!
//! Four kernels, split first on where the output weight comes from.
//! [`copy_ranges`] copies `[start, end)` ranges of one batch of the writer's
//! layout and [`scatter_copy`] an index list; each row keeps its own weight,
//! and its strings are shifted onto the source heap where the caller carried
//! it, else relocated.
//! [`scatter_unified_sources`] (through [`UnifiedSet`]) takes the weight from
//! the caller's [`Pick`] instead: the set's region table holds no weight
//! region, so the pick is the only channel for a merge fold's net weight —
//! which is not any one source row's weight. A set built for spans also reads
//! each source's weights, and copies a [span](SPAN) — a merge's stretch of
//! groups of one — region by region, at the weights its rows hold.
//! [`gather_rows`] takes the same
//! picks and reads each cell through its source's row accessor, or in place
//! where every source holds the region so, for picks sparse in their sources.
//! A region read in place or decoded is read through one [`ColPtr`] per
//! source.
//!
//! A merge source is a [`ColumnarSource`]: a row source that also carries the
//! Z-set weight and answers each region as a `ColPtr`, which every N-way merge
//! and these kernels read sources through.

use std::borrow::Borrow;
use std::cell::Cell;
use std::ops::Range;

use super::batch::{Batch, MemBatch, range_rows};
use super::columnar::{ColPtr, ColumnarSource, DecodedColumns};
use super::pool::with_scratch;
use super::string_heap::{
    BlobCache, HeapCoder, StringDest, carry_dead_rows, prorated_blob_cap, rebase_string_cell, relocated_cell,
};
use super::writer::DirectWriter;
use crate::schema::SchemaDescriptor;
use gnitz_wire::{FIXED_REGION_BYTES, Heap, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use smallvec::{SmallVec, smallvec};

/// One output row of a pick-driven copy: `(source, row, weight)` — the index of
/// the source the row is read from, its row there, and the weight it is copied
/// out at, which overrides that row's own weight. A merge emits one per surviving
/// (PK, payload) group, at the group's net weight.
///
/// A pick [`span_pick`] builds is a span instead: it stands for rows
/// `start..start + len` of source `src`, each copied at the weight it holds
/// there — a merge's stretch of groups of one. [`span_of`] reads one back.
/// Only a set built to copy spans (see [`UnifiedSet::with`]) takes them.
pub type Pick = (u32, u32, i64);

/// The source-index bit that makes a [`Pick`] a span.
pub(crate) const SPAN: u32 = 1 << 31;

/// The span of rows `rows` of source `src`.
#[inline(always)]
pub fn span_pick(src: usize, rows: Range<usize>) -> Pick {
    debug_assert!(!rows.is_empty() && (src as u32) < SPAN);
    (src as u32 | SPAN, rows.start as u32, rows.len() as i64)
}

/// The source and rows of `pick` where it is a span.
#[inline(always)]
pub fn span_of((src, start, len): Pick) -> Option<(usize, Range<usize>)> {
    (src & SPAN != 0).then(|| ((src & !SPAN) as usize, start as usize..start as usize + len as usize))
}

/// The source and rows `pick` stands for: a [span](SPAN)'s, else its one row.
#[inline(always)]
fn pick_rows(pick: Pick) -> (usize, Range<usize>) {
    span_of(pick).unwrap_or((pick.0 as usize, pick.1 as usize..pick.1 as usize + 1))
}

/// The `(source, row)` of every row `picks` stands for, in order.
fn each_row(picks: &[Pick]) -> impl Iterator<Item = (usize, usize)> + '_ {
    picks.iter().flat_map(|&pick| {
        let (si, rows) = pick_rows(pick);
        rows.map(move |ri| (si, ri))
    })
}

/// The rows `picks` stand for, each span's counted, where `SPANS` says some
/// may be spans.
fn picked_rows<const SPANS: bool>(picks: &[Pick]) -> usize {
    match SPANS {
        true => picks.iter().map(|&p| pick_rows(p).1.len()).sum(),
        false => picks.len(),
    }
}

thread_local! {
    /// Pick lists of materializes whose caller keeps none, reused across calls.
    static PICKS: Cell<Vec<Pick>> = const { Cell::new(Vec::new()) };
}

/// Run `f` over the thread's pick scratch, emptied, as [`with_scratch`] runs it.
pub fn with_picks<R>(f: impl FnOnce(&mut Vec<Pick>) -> R) -> R {
    with_scratch(&PICKS, f)
}

/// Instantiate `$f` at the const width matching the region stride `$w`, which
/// is also passed on. The literal width keeps the per-row copy a load/store
/// instead of a `memcpy` call; 12 and 24 are the common compound PK strides
/// (U64+U32, three U64). `N = 0` is the runtime arm, which any other compound
/// PK stride reaches; a target reads its width through [`stride`].
#[macro_export]
#[doc(hidden)]
macro_rules! stride_dispatch {
    ($w:expr, $f:ident, $($arg:expr),* $(,)?) => {{
        let w = $w;
        match w {
            1 => $f::<1>($($arg,)* w),
            2 => $f::<2>($($arg,)* w),
            4 => $f::<4>($($arg,)* w),
            8 => $f::<8>($($arg,)* w),
            12 => $f::<12>($($arg,)* w),
            16 => $f::<16>($($arg,)* w),
            24 => $f::<24>($($arg,)* w),
            _ => $f::<0>($($arg,)* w),
        }
    }};
}
pub use crate::stride_dispatch;

/// The width a const-width copier runs at: `N`, or `width` in the runtime arm
/// `N = 0`.
#[inline(always)]
pub const fn stride<const N: usize>(width: usize) -> usize {
    if N == 0 { width } else { N }
}

/// Each `[start, end)` run of `width`-byte cells of `src`, copied onto `dst`
/// back to back. A constant width indexes whole cells, so a one-row run is a
/// load and a store with no byte-offset arithmetic. `runs` takes a run list as
/// it stands: an iterator built over the list at the call compiles to more
/// instructions per run in [`copy_ranges`].
#[inline(always)]
pub fn copy_runs<const N: usize>(
    src: &[u8],
    dst: &mut [u8],
    runs: impl IntoIterator<Item: Borrow<(usize, usize)>>,
    width: usize,
) {
    if N == 0 {
        let mut at = 0usize;
        for run in runs {
            let &(start, end) = run.borrow();
            dst[at * width..(at + end - start) * width].copy_from_slice(&src[start * width..end * width]);
            at += end - start;
        }
        return;
    }
    let (src, dst) = (src.as_chunks::<N>().0, dst.as_chunks_mut::<N>().0);
    let mut at = 0usize;
    for run in runs {
        let &(start, end) = run.borrow();
        let n = end - start;
        if n == 1 {
            dst[at] = src[start];
        } else {
            dst[at..at + n].copy_from_slice(&src[start..end]);
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
        schema.same_region_types(src.schema),
        "copy_ranges: a source of another layout"
    );
    match *ranges {
        // One run is one bulk copy per region.
        [(start, end)] => {
            for r in 0..schema.num_fixed_regions() {
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
            for r in 0..schema.num_fixed_regions() {
                stride_dispatch!(
                    schema.region_stride(r),
                    copy_runs,
                    src.region(r),
                    writer.region_mut(r),
                    ranges
                );
            }
        }
    }
    for pi in gnitz_wire::BitIter(schema.string_payload_slots()) {
        writer.rebase_string_col(pi, Heap::Plain(src.blob), heap_at.into());
    }
}

/// Copy the rows `indices` names, in the order given, each carrying its own
/// weight: exactly the writer's rows. String cells are shifted onto `batch`'s
/// heap carried at `heap_at`, or relocated. A caller supplying its own weights
/// wants [`scatter_unified_sources`].
pub(crate) fn scatter_copy(batch: &MemBatch, indices: &[u32], heap_at: Option<usize>, writer: &mut DirectWriter) {
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

    let schema = writer.schema;
    for r in 0..schema.num_fixed_regions() {
        stride_dispatch!(
            schema.region_stride(r),
            gather_col,
            batch.region(r),
            writer.region_mut(r),
            indices
        );
    }
    for pi in gnitz_wire::BitIter(schema.string_payload_slots()) {
        writer.rebase_string_col(pi, Heap::Plain(batch.blob), heap_at.into());
    }
}

/// The `width`-byte cell of every row `indices` names, copied onto `dst` back
/// to back; `indices` lie inside `src`, as [`scatter_copy`] checks.
#[inline(always)]
fn gather_col<const N: usize>(src: &[u8], dst: &mut [u8], indices: &[u32], width: usize) {
    let w = stride::<N>(width);
    assert_eq!(dst.len(), indices.len() * w);
    for (out, &idx) in indices.iter().enumerate() {
        let i = idx as usize;
        debug_assert!((i + 1) * w <= src.len());
        // SAFETY: `i` is a row of `src`, `out` a row of `dst`, by the asserts.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr().add(i * w), dst.as_mut_ptr().add(out * w), w);
        }
    }
}

/// Copy the rows `rows` names, in the order given, each at the weight its pick
/// carries, or a [span](SPAN)'s rows at their own: exactly the writer's rows.
/// The sources may be any mix of `MemBatch` and shard backings.
///
/// `table` holds `n` [`ColPtr`]s per slot of the writer's layout, one per
/// source in source order: slot 0 the PK, slot 1 the null words, slot `2 + pi`
/// payload column `pi`. `strings` and `weights` are per source: its heap and
/// where that heap is carried, and under `SPANS` its weights. `rows` holds
/// spans only under `SPANS`.
///
/// Out of line: inlined into [`UnifiedSet::write`], its per-row loops lose
/// registers to the write's own state.
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn scatter_unified_sources<const SPANS: bool>(
    table: &[ColPtr],
    weights: &[ColPtr],
    strings: &[(Heap<'_>, Carry)],
    rows: &[Pick],
    codebook: Option<&dyn HeapCoder>,
    skip: u64,
    writer: &mut DirectWriter<'_>,
) {
    let n = strings.len();
    // Every kernel below writes the rows `rows` stands for unchecked.
    assert_eq!(
        picked_rows::<SPANS>(rows),
        writer.rows(),
        "scatter_unified_sources: a writer of another row count"
    );
    let schema = writer.schema;
    assert_eq!(table.len(), (2 + schema.num_payload_cols()) * n);
    // SAFETY: `k` is a slot of the writer's schema, inside `table` by the assert.
    let slot = |k: usize| unsafe { table.get_unchecked(k * n..(k + 1) * n) };
    let keep = schema.payload_slots() & !skip;
    stride_dispatch!(
        schema.pk_stride(),
        copy_fixed_picks,
        slot(0),
        weights,
        slot(1),
        rows,
        keep,
        SPANS,
        writer
    );

    for (pi, col) in schema.payload_columns() {
        let cs = col.size();
        let tbl = slot(2 + pi);
        if skip >> pi & 1 == 1 {
            // A column the reader never reads: zero cells, an empty string's included.
            writer.col_mut(pi).fill(0);
        } else if col.type_code.is_german_string() {
            let (cells, blob, cache) = writer.string_col_mut(pi);
            let cells = cells.as_chunks_mut::<16>().0;
            // A plain pick list is zipped as a slice, which a span list is not.
            match SPANS {
                true => copy_string_cells(tbl, strings, each_row(rows), codebook, cells, blob, cache),
                false => {
                    let rows = rows.iter().map(|&(si, ri, _)| (si as usize, ri as usize));
                    copy_string_cells(tbl, strings, rows, codebook, cells, blob, cache)
                }
            }
        } else {
            gnitz_wire::by_cell_width!(cs, |T| copy_col_picks::<{ size_of::<T>() }, SPANS>(
                tbl,
                rows,
                writer.col_mut(pi)
            ));
        }
    }
}

/// The German-string cell of every `(source, row)` of `rows`, read through
/// source `si`'s `tbl[si]` and rebased onto `blob` from that source's heap.
/// Out of line, as [`copy_col_picks`] is.
#[inline(never)]
fn copy_string_cells(
    tbl: &[ColPtr],
    strings: &[(Heap<'_>, Carry)],
    rows: impl Iterator<Item = (usize, usize)>,
    codebook: Option<&dyn HeapCoder>,
    cells: &mut [[u8; 16]],
    blob: &mut Vec<u8>,
    mut cache: Option<&mut BlobCache>,
) {
    let picked = cells.iter_mut().zip(rows).map(|(cell, (si, ri))| {
        // SAFETY: `tbl` holds one column per source, and `ri` is a row of
        // source `si` inside the window it was resolved at.
        cell.copy_from_slice(unsafe { tbl.get_unchecked(si).row(ri, 16) });
        (cell, si)
    });
    // Matched once, so the per-cell loop tests for no codebook.
    // SAFETY (both arms): `strings` holds one entry per source.
    match codebook {
        None => picked.for_each(|(cell, si)| {
            let (heap, carry) = unsafe { *strings.get_unchecked(si) };
            rebase_string_cell(cell, heap, blob, carry.map(|(at, _)| at).into(), cache.as_deref_mut())
        }),
        Some(book) => picked.for_each(|(cell, si)| {
            let heap = unsafe { strings.get_unchecked(si).0 };
            rebase_string_cell(cell, heap, blob, StringDest::Placed(Some(book)), cache.as_deref_mut())
        }),
    }
}

/// The PK, weight and null word of every row `picks` stands for, read through
/// source `si`'s `pk[si]` and `nulls[si]`: a pick's weight is its own, and a
/// [span](SPAN)'s, under `spans`, its rows' as `weights[si]` reads them. A null
/// word keeps only the bits of `keep`. The destination stride is the writer's;
/// a source read keeps its `ColPtr`'s stride, so a Constant region repeats its
/// one row for every output row.
///
/// `picks` must stand for exactly the writer's rows, as each caller asserts.
/// Out of line, as [`copy_col_picks`] is.
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn copy_fixed_picks<const PKS: usize>(
    pk: &[ColPtr],
    weights: &[ColPtr],
    nulls: &[ColPtr],
    picks: &[Pick],
    keep: u64,
    spans: bool,
    writer: &mut DirectWriter<'_>,
    width: usize,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pks = stride::<PKS>(width);
    assert!(pk.len() == nulls.len() && (!spans || weights.len() == nulls.len()));
    let (pk_dst, wt_dst, nbm_dst) = writer.fixed_mut();
    let (pk_dst, wt_dst, nbm_dst) = (pk_dst.as_mut_ptr(), wt_dst.as_mut_ptr(), nbm_dst.as_mut_ptr());
    // SAFETY (every block below): the tables hold one entry per source, the
    // rows read are rows of the source their `ColPtr`s address, and the rows
    // written are rows of the writer.
    let copy_pick = |at: usize, (si, ri, w): Pick| unsafe {
        debug_assert_ne!(w, 0, "copy_fixed_picks: zero-weight row (filter before copy)");
        let (si, ri) = (si as usize, ri as usize);
        let pk_ptr = pk.get_unchecked(si).row_ptr(ri);
        let nbm_ptr = nulls.get_unchecked(si).row_ptr(ri);
        let d = pk_dst.add(at * pks);
        if PKS == 0 && (16..=32).contains(&pks) {
            // Two overlapping 16-byte moves cover any width in 16..=32.
            let head = (pk_ptr as *const [u8; 16]).read_unaligned();
            let tail = (pk_ptr.add(pks - 16) as *const [u8; 16]).read_unaligned();
            (d as *mut [u8; 16]).write_unaligned(head);
            (d.add(pks - 16) as *mut [u8; 16]).write_unaligned(tail);
        } else {
            std::ptr::copy_nonoverlapping(pk_ptr, d, pks);
        }
        std::ptr::copy_nonoverlapping(w.to_le_bytes().as_ptr(), wt_dst.add(at * FB), FB);
        let nbm = (nbm_ptr as *const u64).read_unaligned() & keep;
        (nbm_dst.add(at * FB) as *mut u64).write_unaligned(nbm);
    };
    if !spans {
        return picks.iter().enumerate().for_each(|(at, &pick)| copy_pick(at, pick));
    }
    let mut at = 0usize;
    for &pick in picks {
        let Some((si, rows)) = span_of(pick) else {
            copy_pick(at, pick);
            at += 1;
            continue;
        };
        let n = rows.len();
        // SAFETY: as above, for the span's rows.
        unsafe {
            pk.get_unchecked(si).copy_rows(
                rows.start,
                pks,
                std::slice::from_raw_parts_mut(pk_dst.add(at * pks), n * pks),
            );
            let wt = std::slice::from_raw_parts_mut(wt_dst.add(at * FB), n * FB);
            weights.get_unchecked(si).copy_rows(rows.start, FB, wt);
            let nb = nulls.get_unchecked(si);
            for (i, r) in rows.enumerate() {
                let nbm = (nb.row_ptr(r) as *const u64).read_unaligned() & keep;
                (nbm_dst.add((at + i) * FB) as *mut u64).write_unaligned(nbm);
            }
        }
        at += n;
    }
}

/// The `N`-byte cell of every row `picks` stands for, read through source
/// `si`'s `tbl[si]`, a [span](SPAN)'s, under `SPANS`, copied whole. `picks`
/// must stand for exactly `dst`'s cells, as each caller asserts.
///
/// Out of line: inlined, it changes how [`scatter_unified_sources`]'s other
/// loops compile.
#[inline(never)]
fn copy_col_picks<const N: usize, const SPANS: bool>(tbl: &[ColPtr], picks: &[Pick], dst: &mut [u8]) {
    let dst = dst.as_mut_ptr();
    let mut at = 0usize;
    for &pick in picks {
        // SAFETY (both arms): `tbl` holds one column per source, the rows read
        // are rows of the source its `ColPtr` addresses, and the cells written
        // are cells of `dst`.
        unsafe {
            if SPANS && let Some((si, rows)) = span_of(pick) {
                let cells = std::slice::from_raw_parts_mut(dst.add(at * N), rows.len() * N);
                tbl.get_unchecked(si).copy_rows(rows.start, N, cells);
                at += rows.len();
                continue;
            }
            let src = tbl.get_unchecked(pick.0 as usize).row_ptr(pick.1 as usize);
            std::ptr::copy_nonoverlapping(src, dst.add(at * N), N);
        }
        at += 1;
    }
}

/// Copy the rows `picks` names out of `sources`, in the order given, each at
/// the weight its pick carries, into a fresh unconsolidated batch.
///
/// A region every source holds in place is read through one [`ColPtr`] per
/// source; any other is read through each source's row accessor, so a packed
/// shard column decodes only the blocks the picks land in: the kernel for picks
/// sparse in their sources, where [`UnifiedSet`] would decode whole windows.
/// The payload columns `skip` names are not read: each cell is written as a
/// zero that is not NULL.
pub fn gather_rows(sources: &[impl ColumnarSource], schema: &SchemaDescriptor, picks: &[Pick], skip: u64) -> Batch {
    Batch::write_new(schema, picks.len(), 0, |w| gather_into(sources, schema, picks, skip, w))
}

/// [`gather_rows`] into the rows `w` writes, one per pick.
pub fn gather_into(
    sources: &[impl ColumnarSource],
    schema: &SchemaDescriptor,
    picks: &[Pick],
    skip: u64,
    w: &mut DirectWriter<'_>,
) {
    // The in-place kernels write a row per pick unchecked.
    assert_eq!(picks.len(), w.rows(), "gather_into: a writer of another row count");
    #[cfg(debug_assertions)]
    for &(si, ri, _) in picks {
        debug_assert!((ri as usize) < sources[si as usize].row_count());
    }
    let mut direct = DirectCols::new();
    let hoist = picks.len() >= HOIST_PICKS_PER_SOURCE * (sources.len() + 1);
    let keep = schema.payload_slots() & !skip;
    // The PK and null words are copied in one pass, so both are read in place
    // or neither is.
    if hoist && all_in_place(&mut direct, sources, &[REG_PK, REG_NULL_BMP]) {
        let (pk, nulls) = direct.split_at(sources.len());
        stride_dispatch!(
            schema.pk_stride(),
            copy_fixed_picks,
            pk,
            &[],
            nulls,
            picks,
            keep,
            false,
            w
        );
    } else {
        stride_dispatch!(schema.pk_stride(), gather_pk_wt_nbm, sources, picks, keep, w);
    }
    for (pi, col) in schema.payload_columns() {
        if skip >> pi & 1 == 1 {
            w.col_mut(pi).fill(0);
        } else if col.type_code.is_german_string() {
            let (cells, blob, _) = w.string_col_mut(pi);
            for (cell, &(si, ri, _)) in cells.as_chunks_mut::<16>().0.iter_mut().zip(picks) {
                let s = &sources[si as usize];
                *cell = relocated_cell(s.german_cell(ri as usize, pi), s.heap(), blob, None);
            }
        } else if hoist && all_in_place(&mut direct, sources, &[REG_PAYLOAD_START + pi]) {
            let dst = w.col_mut(pi);
            gnitz_wire::by_cell_width!(col.size(), |T| copy_col_picks::<{ size_of::<T>() }, false>(
                &direct, picks, dst
            ));
        } else {
            let dst = w.col_mut(pi);
            gnitz_wire::by_cell_width!(col.size(), |T| gather_cells::<{ size_of::<T>() }>(
                sources, picks, pi, dst
            ));
        }
    }
}

/// Picks per source, counting one source more for the table's allocation, from
/// which [`gather_rows`] reads a region every source holds in place through one
/// [`ColPtr`] per source: resolving a source's region costs about what reading a
/// few of its cells through the row accessor does.
const HOIST_PICKS_PER_SOURCE: usize = 4;

/// Whether every one of `sources` holds each of `regions` in place; if so,
/// `direct` holds them region by region, each region's in source order.
fn all_in_place<S: ColumnarSource>(direct: &mut DirectCols, sources: &[S], regions: &[usize]) -> bool {
    direct.clear();
    direct.reserve(regions.len() * sources.len());
    for &r in regions {
        for s in sources {
            match s.in_place(r) {
                Some(col) => direct.push(col),
                None => return false,
            }
        }
    }
    true
}

#[inline(always)]
fn gather_pk_wt_nbm<const PKS: usize>(
    sources: &[impl ColumnarSource],
    picks: &[Pick],
    keep: u64,
    w: &mut DirectWriter<'_>,
    width: usize,
) {
    let pks = stride::<PKS>(width);
    let (pk, weight, null_bmp) = w.fixed_mut();
    let weights = weight.as_chunks_mut::<8>().0.iter_mut();
    let nulls = null_bmp.as_chunks_mut::<8>().0.iter_mut();
    let rows = picks.iter().zip(pk.chunks_exact_mut(pks)).zip(weights).zip(nulls);
    for (((&(si, ri, weight), pk), wt), nb) in rows {
        debug_assert_ne!(weight, 0, "gather_rows: zero-weight row (filter before gather)");
        let s = &sources[si as usize];
        pk.copy_from_slice(&s.get_pk_bytes(ri as usize)[..pks]);
        *wt = weight.to_le_bytes();
        *nb = (s.get_null_word(ri as usize) & keep).to_le_bytes();
    }
}

#[inline(always)]
fn gather_cells<const N: usize>(sources: &[impl ColumnarSource], picks: &[Pick], pi: usize, dst: &mut [u8]) {
    let cells = dst.as_chunks_mut::<N>().0;
    for (&(si, ri, _), cell) in picks.iter().zip(cells) {
        *cell = sources[si as usize].get_col_ptr(ri as usize, pi, N).try_into().unwrap();
    }
}

/// A [`UnifiedSet`]'s slot-major region table, inline up to 40 entries across
/// all slots and sources: four sources' PK and null words, and 32 payload
/// columns among them. Nothing keeps a pointer into it: the scatter slices it
/// at write time.
type ColTable = SmallVec<[ColPtr; 40]>;

/// Where a source's heap is carried whole into the scatter's destination
/// heap, and the bytes of it charged dead; `None` relocates each cell.
type Carry = Option<(usize, usize)>;

/// A [`UnifiedSet`]'s per-source values, inline up to 4 sources.
type PerSource<T> = SmallVec<[T; 4]>;

/// [`gather_rows`]' hoisted per-source regions, inline up to 8.
type DirectCols = SmallVec<[ColPtr; 8]>;

/// Room for `n` entries in the empty `v`, where its inline storage holds fewer.
#[inline(always)]
fn reserve<A: smallvec::Array>(v: &mut SmallVec<A>, n: usize) {
    if v.capacity() < n {
        v.reserve_exact(n);
    }
}

/// [`scatter_unified_sources`]'s source side: every source's regions under the
/// set's schema, the heaps they read, and the columns they decoded.
pub struct UnifiedSet<'a> {
    /// One [`ColPtr`] per source for each slot of the set's schema, slot `k`'s
    /// at `table[k * n..(k + 1) * n]` for `n` sources: slot 0 the PK, slot 1 the
    /// null words, slot `2 + pi` payload column `pi`.
    table: ColTable,
    /// Each source's heap, and its [`Carry`].
    strings: PerSource<(Heap<'a>, Carry)>,
    /// Each source's weights, where the set copies [spans](SPAN); empty
    /// otherwise.
    weights: PerSource<ColPtr>,
    schema: &'a SchemaDescriptor,
    /// Heap bytes and rows of the whole sources, windows notwithstanding.
    src_blob: usize,
    src_rows: usize,
    _decoded: DecodedColumns,
    /// Payload columns written as zeroes instead of copied.
    skip: u64,
    #[cfg(debug_assertions)]
    windows: Vec<Range<usize>>,
}

impl<'a> UnifiedSet<'a> {
    /// `f` applied to views over `sources` under `schema`, source `si` readable
    /// at rows in the `si`-th window, the payload columns `skip` names left
    /// undecoded and written as zeroes. Where `spans` is set, the rows the set
    /// copies may hold [spans](span_pick), and it reads each source's weights for
    /// them. The set is built in this frame and lent to `f`, so its inline
    /// tables are never copied out by a return.
    pub fn with<S: ColumnarSource, R>(
        sources: &'a [S],
        schema: &'a SchemaDescriptor,
        windows: impl IntoIterator<Item = Range<usize>>,
        skip: u64,
        spans: bool,
        f: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let mut set = UnifiedSet {
            table: ColTable::new(),
            strings: PerSource::new(),
            weights: PerSource::new(),
            schema,
            src_blob: 0,
            src_rows: 0,
            _decoded: DecodedColumns::default(),
            skip,
            #[cfg(debug_assertions)]
            windows: Vec::new(),
        };
        let (n, slots) = (sources.len(), 2 + schema.num_payload_cols());
        let keep = schema.payload_slots() & !skip;
        reserve(&mut set.table, slots * n);
        reserve(&mut set.strings, n);
        // Written through raw pointers and lengthened once each: a push per
        // entry costs a capacity check, which a set of a few rows pays per call.
        let (table, strings) = (set.table.as_mut_ptr(), set.strings.as_mut_ptr());
        let mut each = windows.into_iter();
        for (si, s) in sources.iter().enumerate() {
            let w = each.next().expect("UnifiedSet::with: one window per source");
            set.src_blob += s.heap().plain_len();
            set.src_rows += s.row_count();
            // SAFETY: `strings` was reserved past `si`.
            unsafe { strings.add(si).write((s.heap(), None)) };
            for k in 0..slots {
                let r = match k {
                    0 => Some(REG_PK),
                    // Under a schema that keeps no null bit, every null word written is zero.
                    1 => (keep != 0).then_some(REG_NULL_BMP),
                    _ => (skip >> (k - 2) & 1 == 0).then_some(REG_PAYLOAD_START + k - 2),
                };
                let col = match r {
                    Some(r) if !w.is_empty() => s.region(r, &w, &mut set._decoded),
                    // A slot the scatter reads only as zeroes, or never: no
                    // pick names a row outside its source's window.
                    _ => ColPtr::zeroes(),
                };
                // SAFETY: the table was reserved for `slots` entries per source.
                unsafe { table.add(k * n + si).write(col) };
            }
            if spans {
                let weights = match w.is_empty() {
                    true => ColPtr::zeroes(),
                    false => s.region(REG_WEIGHT, &w, &mut set._decoded),
                };
                set.weights.push(weights);
            }
            #[cfg(debug_assertions)]
            set.windows.push(w);
        }
        debug_assert!(each.next().is_none(), "UnifiedSet::with: one window per source");
        // SAFETY: the loop wrote every entry below each new length.
        unsafe {
            set.table.set_len(slots * n);
            set.strings.set_len(n);
        }
        f(&mut set)
    }

    /// [`with`](Self::with) with every source readable at every row.
    pub fn with_whole<S: ColumnarSource, R>(
        sources: &'a [S],
        schema: &'a SchemaDescriptor,
        f: impl FnOnce(&mut Self) -> R,
    ) -> R {
        Self::with(sources, schema, sources.iter().map(|s| 0..s.row_count()), 0, false, f)
    }

    /// Copy the rows `rows` names, in the order given, each at the weight its
    /// pick carries or a [span](span_pick)'s rows at their own, into a fresh
    /// unconsolidated batch under the set's schema, its heap coded under
    /// `codebook` where one is given (see [`append_value`](super::append_value)).
    /// The heap reserved is prorated from the whole sources, and none where the
    /// set writes no string column: under a schema without one, such as a
    /// skeleton merge's PK-only one over sources that hold strings, or where
    /// every one is skipped.
    pub fn materialize(&self, rows: &[Pick], codebook: Option<&dyn HeapCoder>) -> Batch {
        self.write(rows, None, codebook)
    }

    /// [`materialize`](Self::materialize) reserving `blob_cap` heap bytes where
    /// given, with the heap of each source whose `strings` entry carries it
    /// adopted where that entry places it before any cell lands.
    fn write(&self, rows: &[Pick], blob_cap: Option<usize>, codebook: Option<&dyn HeapCoder>) -> Batch {
        let carried: SmallVec<[(&[u8], usize, usize); 4]> = (self.strings.iter())
            .filter_map(|&(heap, carry)| {
                carry.map(|(at, dead)| (heap.plain().expect("a carried heap is plain"), at, dead))
            })
            .collect();
        crate::with_spans!(!self.weights.is_empty(), |SPANS| {
            debug_assert!(
                SPANS || rows.iter().all(|&p| span_of(p).is_none()),
                "UnifiedSet::materialize: a span, in a set built for none"
            );
            #[cfg(debug_assertions)]
            for (si, ri) in each_row(rows) {
                debug_assert!(
                    self.windows[si].contains(&ri),
                    "UnifiedSet::materialize: row {ri} of source {si} outside its window {:?}",
                    self.windows[si],
                );
            }
            let n = picked_rows::<SPANS>(rows);
            let blob_cap = blob_cap.unwrap_or_else(|| match self.schema.string_payload_slots() & !self.skip {
                0 => 0,
                _ => prorated_blob_cap(self.src_blob, self.src_rows, n),
            });
            Batch::write_new_carrying(self.schema, n, blob_cap, &carried, |writer| {
                scatter_unified_sources::<SPANS>(
                    &self.table,
                    &self.weights,
                    &self.strings,
                    rows,
                    codebook,
                    self.skip,
                    writer,
                );
            })
        })
    }
}

/// [`UnifiedSet::materialize`] over whole `sources`, each heap carried whole
/// where that beats relocating the cells of the rows `rows` keeps of it.
/// No row of a source may be picked twice.
pub fn materialize_carrying(sources: &[MemBatch<'_>], schema: &SchemaDescriptor, rows: &[Pick]) -> Batch {
    UnifiedSet::with_whole(sources, schema, |set| carry_and_write(set, sources, schema, rows))
}

/// [`materialize_carrying`]'s body over the set built for it.
fn carry_and_write(
    set: &mut UnifiedSet<'_>,
    sources: &[MemBatch<'_>],
    schema: &SchemaDescriptor,
    rows: &[Pick],
) -> Batch {
    if schema.string_payload_slots() == 0 {
        return set.materialize(rows, None);
    }
    let mut kept: SmallVec<[usize; 4]> = smallvec![0; sources.len()];
    // Counted through one slice, so the loop does not re-resolve inline storage per row.
    let counts = &mut kept[..];
    for &(si, _, _) in rows {
        counts[si as usize] += 1;
    }
    // A carried heap lands after the ones before it in a fresh, empty heap.
    let mut heap_end = 0;
    let (mut relocated_blob, mut relocated_rows, mut relocated_out) = (0, 0, 0);
    // Each source's picked rows as a bitset, set in one pass over the picks
    // on the first source that needs them.
    let mut named: Option<SmallVec<[Vec<u64>; 4]>> = None;
    for (si, s) in sources.iter().enumerate() {
        let named_here = || {
            let named = named.get_or_insert_with(|| {
                let mut sets: SmallVec<[Vec<u64>; 4]> = sources.iter().map(|s| vec![0; s.count.div_ceil(64)]).collect();
                for &(src, row, _) in rows {
                    let row = row as usize;
                    sets[src as usize][row / 64] |= 1 << (row % 64);
                }
                sets
            });
            std::mem::take(&mut named[si])
        };
        match carry_dead_rows(s, kept[si], true, named_here) {
            Some(d) => {
                set.strings[si].1 = Some((heap_end, d));
                heap_end += s.blob.len();
            }
            None => {
                relocated_blob += s.blob.len();
                relocated_rows += s.count;
                relocated_out += kept[si];
            }
        }
    }
    let blob_cap = heap_end + prorated_blob_cap(relocated_blob, relocated_rows, relocated_out);
    set.write(rows, Some(blob_cap), None)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/scatter.rs"]
mod tests;
