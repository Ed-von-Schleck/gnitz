//! Exchange repartition: column-first scatter of selected (possibly reordered,
//! multi-source) rows into a `DirectWriter`, plus the two per-row passes that
//! share its shape — PK routing and secondary-index projection.
//!
//! The merge half of `merge.rs` consolidates sorted runs in place; this half
//! *scatters* arbitrary row selections during exchange repartition, joins,
//! distinct, and reduce. Two kernels, split on the source axis alone:
//! [`scatter_copy`] for one hoisted source addressed by `&[u32]`, and
//! [`scatter_unified_sources`] for reordered `(src, row, weight)`
//! triples over any mix of `MemBatch` and shard backings. Both
//! share one shape: a fused PK + weight + null_bmp pass dispatched on `pk_stride`
//! to a const-width (`PKS`) helper, then one sequential pass per payload column
//! (column widths dispatched to a const-`N` gather). Those const-generic arms and
//! every `#[inline(always)]` are load-bearing: the literal width is what keeps the
//! per-row copy a fixed-width load/store rather than a `memcpy` call. The writer's
//! fixed-region buffers are written directly, so `DirectWriter` keeps them
//! `pub(super)`.

use super::batch::{Batch, FIXED_REGION_BYTES};
use super::merge::{ColPtr, DirectWriter, MemBatch, UnifiedSource};
use gnitz_wire::is_german_string;

/// Instantiate `$f` at the const PK width matching `$stride`. The literal width
/// is what lets the fused per-row pass emit fixed-width loads/stores instead of a
/// `memcpy`; `PKS = 0` is the runtime-stride sentinel for compound widths outside
/// the ladder (e.g. U64+U32 = 12), whose instantiation reads `writer.pk_stride`.
///
/// One spelling for all three scatter entry points, so a width added here reaches
/// every one of them.
macro_rules! pk_stride_dispatch {
    ($stride:expr, $f:ident, $($arg:expr),* $(,)?) => {
        match $stride {
            1 => $f::<1>($($arg),*),
            2 => $f::<2>($($arg),*),
            4 => $f::<4>($($arg),*),
            8 => $f::<8>($($arg),*),
            16 => $f::<16>($($arg),*),
            _ => $f::<0>($($arg),*),
        }
    };
}

/// Instantiate `$f` at the const column width matching `$cs`, or evaluate `$fallback`
/// for a width outside the ladder. Same role as [`pk_stride_dispatch`] for the
/// per-payload-column gathers.
macro_rules! col_width_dispatch {
    ($cs:expr, $f:ident, ($($arg:expr),* $(,)?), $fallback:expr) => {
        match $cs {
            1 => $f::<1>($($arg),*),
            2 => $f::<2>($($arg),*),
            4 => $f::<4>($($arg),*),
            8 => $f::<8>($($arg),*),
            16 => $f::<16>($($arg),*),
            _ => $fallback,
        }
    };
}

/// Route each live row of `mb` into its owning worker's slot of `slots`, by the
/// relation's distribution prefix (`slots.len()` is the worker count).
///
/// The one placement rule for a table key. The master's push fan-out and the
/// boot relayout both drive it, and they have to agree row-for-row: a relayout
/// that placed a row where the push path would not is a row no read can reach.
///
/// Weight-0 rows are dropped — they are not Z-set elements, and a client is free
/// to send one. Dropping them here means every consumer of the indices reads the
/// same row set for free.
pub fn route_rows_by_pk(mb: &MemBatch, schema: &crate::schema::SchemaDescriptor, slots: &mut [Vec<u32>]) {
    let num_workers = slots.len();
    for i in 0..mb.count {
        if mb.get_weight(i) == 0 {
            continue;
        }
        slots[schema.worker_for_pk(mb.get_pk_bytes(i), num_workers)].push(i as u32);
    }
}

/// Project every live row of `src` into one secondary-index entry.
///
/// An index schema is all PK and no payload, so the entry *is* its key —
/// `(indexed col(s) [promoted] ‖ source PK)`, composed by `spec.write_entry`,
/// which also decides the SQL NULL-distinctness skip.
///
/// Retractions (weight < 0) project, so an index entry retracts with its source
/// row; weight-0 rows are dropped for the reason [`route_rows_by_pk`] drops them.
pub fn batch_project_index(
    src: &Batch,
    spec: &crate::schema::IndexKeySpec,
    idx_schema: &crate::schema::SchemaDescriptor,
) -> Batch {
    let idx_stride = idx_schema.pk_stride();

    let mut out = Batch::with_capacity(idx_schema, src.count.max(1));
    // MAX_PK_BYTES bounds every index schema's pk_stride (asserted in
    // SchemaDescriptor::new), so the scratch PK buffer lives on the stack with no
    // per-batch heap allocation. The used [..idx_stride] prefix is fully
    // overwritten each row; the single zero-init covers the (currently empty) tail.
    let mut idx_pk_buf = [0u8; crate::schema::MAX_PK_BYTES];

    let mb = src.as_mem_batch();

    for row in 0..src.count {
        let weight = src.get_weight(row);
        if weight == 0 {
            continue;
        }
        if !spec.write_entry(&mb, row, &mut idx_pk_buf) {
            continue;
        }
        out.push_key_row(&idx_pk_buf[..idx_stride], weight);
    }

    // `out` is `Raw` from `with_capacity`; the `extend_*` loop above never raises
    // it, and the index-table ingest re-sorts/folds it.
    out
}

/// Scatter-copy rows from a batch at the given indices, carrying each row's own
/// weight. Indices are NOT sorted — rows are written in the order given.
///
/// The single-source kernel: `&[u32]` indices, source hoisted out of every loop.
/// A caller that reorders rows across *several* sources uses
/// [`scatter_unified_sources`] — whose `(src, row, weight)` triples are 4× the
/// index memory this one needs, on the per-worker SAL ingest scatter and the
/// boot relayout.
pub(crate) fn scatter_copy(batch: &MemBatch, indices: &[u32], writer: &mut DirectWriter) {
    if indices.is_empty() {
        return;
    }

    // Input must not contain zero-weight rows — callers guarantee this.
    #[cfg(debug_assertions)]
    for &idx in indices {
        debug_assert_ne!(
            batch.get_weight(idx as usize),
            0,
            "scatter_copy: zero-weight row at index {idx} (filter before scatter)",
        );
    }
    scatter_col_first(batch, indices, writer);
}

fn scatter_col_first(batch: &MemBatch<'_>, indices: &[u32], writer: &mut DirectWriter<'_>) {
    let n = indices.len();
    let base = writer.count; // first output row for this scatter

    // Fused PK + weight + null_bmp gather: one pass over `indices` instead of three.
    pk_stride_dispatch!(writer.pk_stride, scatter_col_first_fixed, batch, indices, base, writer);

    let schema = writer.schema;
    for (pi, col) in schema.payload_columns() {
        let cs = col.size() as usize;
        if is_german_string(col.type_code) {
            // Blob relocation is sequential per-row; no way to batch.
            for (out, &idx) in indices.iter().enumerate() {
                let row = idx as usize;
                let src_struct = batch.get_col_ptr(row, pi, 16);
                writer.write_string_cell(pi, src_struct, batch.blob, base + out);
            }
        } else {
            // The null *bit* governs what a cell means, so the value bytes are
            // copied unconditionally — no per-row null test, letting `gather_col`
            // vectorize. A null cell's bytes are never read back as a value.
            let src_col = batch.col_data(pi, cs);
            let dst_col = &mut writer.col_bufs[pi][base * cs..];
            col_width_dispatch!(cs, gather_col, (src_col, dst_col, indices), {
                for (out, &idx) in indices.iter().enumerate() {
                    let i = idx as usize;
                    dst_col[out * cs..][..cs].copy_from_slice(&src_col[i * cs..][..cs]);
                }
            });
        }
    }

    writer.count += n;
}

// Fused PK + weight + null_bmp gather in one pass over `indices`, replacing three
// separate gather_col calls. `PKS = 0` is the runtime-stride sentinel for
// compound widths outside the const dispatch (e.g. U64+U32 = 12); its
// instantiation reads `writer.pk_stride` and compiles to the same body the
// hand-written dynamic twin had. Caller invariant: every index < batch.count;
// writer slices sized for at least (base + indices.len()) rows.
#[inline(always)]
fn scatter_col_first_fixed<const PKS: usize>(
    batch: &MemBatch<'_>,
    indices: &[u32],
    base: usize,
    writer: &mut DirectWriter<'_>,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pks = if PKS == 0 { writer.pk_stride as usize } else { PKS };
    let pk_src = batch.pk();
    let wt_src = batch.weight();
    let nb_src = batch.null_bmp();
    let pk_dst = &mut writer.pk[base * pks..];
    let wt_dst = &mut writer.weight[base * FB..];
    let nb_dst = &mut writer.null_bmp[base * FB..];
    debug_assert!(pk_dst.len() >= indices.len() * pks);
    debug_assert!(wt_dst.len() >= indices.len() * FB);
    debug_assert!(nb_dst.len() >= indices.len() * FB);
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
                    // Use wrapping_add: `pi * N` may exceed the allocation
                    // boundary on speculative indices; wrapping_add imposes no
                    // provenance constraint so this is well-defined UB-free.
                    std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                        src.as_ptr().wrapping_add(pi * N) as *const i8,
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
        unsafe {
            copy_row::<N>(src, dst, i, out);
        }
    }
}

/// Column-first scatter from multiple `UnifiedSource`s with explicit per-row
/// weights from the merge walk.
///
/// `cols` is the flat payload-`ColPtr` table the sources were built against;
/// source `si`'s column `pi` is `cols[sources[si].cols_off + pi]`.
///
/// Used by the read-cursor drain, shard compaction and the flush-path merge.
/// Callers must pass only net-nonzero weights; both the drain walk and
/// `merge::drive`'s group fold emit only net-nonzero groups.
pub(crate) fn scatter_unified_sources(
    sources: &[UnifiedSource],
    cols: &[ColPtr],
    rows: &[(u32, u32, i64)],
    writer: &mut DirectWriter<'_>,
) {
    if rows.is_empty() {
        return;
    }
    #[cfg(debug_assertions)]
    for &(_si, _ri, w) in rows {
        debug_assert_ne!(w, 0, "scatter_unified_sources: zero-weight row in drain buffer",);
    }
    let n = rows.len();
    let base = writer.count;

    // Fused PK + weight + null_bmp pass.
    pk_stride_dispatch!(writer.pk_stride, scatter_unified_pk_wt_nbm, sources, rows, base, writer);

    let schema = writer.schema;
    for (pi, col) in schema.payload_columns() {
        let cs = col.size() as usize;
        if is_german_string(col.type_code) {
            // Blob relocation is per-row regardless; no way to batch.
            for (out, &(si, ri, _)) in rows.iter().enumerate() {
                let src = unsafe { sources.get_unchecked(si as usize) };
                let src_struct = unsafe { cols.get_unchecked(src.cols_off + pi).row(ri as usize, 16) };
                // Guard against null blob_ptr (source with no string data):
                // from_raw_parts on a null pointer is UB even when len==0.
                let src_blob: &[u8] = if src.blob_ptr.is_null() {
                    &[]
                } else {
                    unsafe { std::slice::from_raw_parts(src.blob_ptr, src.blob_len) }
                };
                writer.write_string_cell(pi, src_struct, src_blob, base + out);
            }
        } else {
            let dst = &mut writer.col_bufs[pi][base * cs..];
            gather_unified_col_dispatch(sources, cols, rows, pi, cs, dst);
        }
    }

    writer.count += n;
}

// PK stride is the literal `PKS` (1/2/4/8/16) — or `writer.pk_stride` read at
// runtime for the compound-width `PKS = 0` sentinel — for the destination
// only; source reads use `src.pk.stride` so Constant PK regions (stride=0)
// read the same bytes for every output row — identical to the existing
// null_bmp Constant behaviour. No source/dest stride conflation.
// Raw pointer writes eliminate the redundant bounds checks that slice indexing
// emits — `DirectWriter` pre-allocates exactly `count` rows per buffer.
#[inline(always)]
fn scatter_unified_pk_wt_nbm<const PKS: usize>(
    sources: &[UnifiedSource],
    rows: &[(u32, u32, i64)],
    base: usize,
    writer: &mut DirectWriter<'_>,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pks = if PKS == 0 { writer.pk_stride as usize } else { PKS };
    let pk_dst = writer.pk.as_mut_ptr();
    let wt_dst = writer.weight.as_mut_ptr();
    let nbm_dst = writer.null_bmp.as_mut_ptr();
    for (out, &(si, ri, w)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize) };
        let dst_row = base + out;
        let pk_ptr = unsafe { src.pk.row_ptr(ri as usize) };
        let nbm_ptr = unsafe { src.null_bmp.row_ptr(ri as usize) };
        let wb = w.to_le_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(pk_ptr, pk_dst.add(dst_row * pks), pks);
            std::ptr::copy_nonoverlapping(wb.as_ptr(), wt_dst.add(dst_row * FB), FB);
            // Unaligned read/OR/write rather than a straight copy: a shard
            // written before an `ALTER … ADD COLUMN` carries no bits for the
            // appended columns, and `null_pad_mask` forces them to NULL. The
            // mask is 0 for every in-memory source and every full-width shard.
            let nbm = (nbm_ptr as *const u64).read_unaligned() | src.null_pad_mask;
            (nbm_dst.add(dst_row * FB) as *mut u64).write_unaligned(nbm);
        }
    }
}

#[inline(always)]
fn gather_unified_col_dispatch(
    sources: &[UnifiedSource],
    cols: &[ColPtr],
    rows: &[(u32, u32, i64)],
    pi: usize,
    cs: usize,
    dst: &mut [u8],
) {
    col_width_dispatch!(cs, gather_unified_col, (sources, cols, rows, pi, dst), {
        for (out, &(si, ri, _)) in rows.iter().enumerate() {
            let src = unsafe { sources.get_unchecked(si as usize) };
            let cp = unsafe { cols.get_unchecked(src.cols_off + pi) };
            dst[out * cs..][..cs].copy_from_slice(unsafe { cp.row(ri as usize, cs) });
        }
    });
}

// `N` is a const so LLVM emits fixed-width load/store and not a memcpy call.
// `dst` is taken as a raw slice (rather than indexing through `writer.col_bufs`)
// so the bounds check stays out of the hot inner loop.
#[inline(always)]
fn gather_unified_col<const N: usize>(
    sources: &[UnifiedSource],
    cols: &[ColPtr],
    rows: &[(u32, u32, i64)],
    pi: usize,
    dst: &mut [u8],
) {
    for (out, &(si, ri, _)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize) };
        let ptr = unsafe { cols.get_unchecked(src.cols_off + pi).row_ptr(ri as usize) };
        unsafe { std::ptr::copy_nonoverlapping(ptr, dst.as_mut_ptr().add(out * N), N) };
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/scatter.rs"]
mod tests;
