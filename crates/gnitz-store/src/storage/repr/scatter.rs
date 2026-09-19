//! Column-first row copy into a `DirectWriter`: choose the rows, then copy them.
//!
//! Two kernels, split on where the output weight comes from. [`scatter_copy`]
//! carries each source row's own. [`scatter_unified_sources`] takes it from the
//! caller's `(src, row, weight)` triple: a `UnifiedSource` has no weight region,
//! so the triple is the only channel for a merge fold's net weight — which is
//! not any one source row's weight.

use super::batch::FIXED_REGION_BYTES;
use super::merge::{ColPtr, DirectWriter, MemBatch, UnifiedSource};
use gnitz_wire::is_german_string;

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

/// Reset `out` to `num_workers` slots and fill each with the live rows of `mb`
/// that worker owns: the one placement rule for a table key. Weight-0 rows are
/// not Z-set elements and are dropped.
pub fn route_rows_by_pk<'a>(
    mb: &MemBatch,
    schema: &crate::schema::SchemaDescriptor,
    out: &'a mut Vec<Vec<u32>>,
    num_workers: usize,
) -> &'a mut [Vec<u32>] {
    let slots = reset_slots(out, num_workers);
    for i in 0..mb.count {
        if mb.get_weight(i) == 0 {
            continue;
        }
        slots[schema.worker_for_pk(mb.get_pk_bytes(i), num_workers)].push(i as u32);
    }
    slots
}

/// Reset `out` to `num_workers` empty slots, keeping their allocations.
pub fn reset_slots<T>(out: &mut Vec<Vec<T>>, num_workers: usize) -> &mut [Vec<T>] {
    if out.len() < num_workers {
        out.resize_with(num_workers, Vec::new);
    }
    let slots = &mut out[..num_workers];
    slots.iter_mut().for_each(Vec::clear);
    slots
}

/// Copy the rows `indices` names, in the order given, each carrying its own
/// weight. A caller supplying its own weights wants [`scatter_unified_sources`].
pub(crate) fn scatter_copy(batch: &MemBatch, indices: &[u32], writer: &mut DirectWriter) {
    if indices.is_empty() {
        return;
    }

    #[cfg(debug_assertions)]
    for &idx in indices {
        debug_assert_ne!(
            batch.get_weight(idx as usize),
            0,
            "scatter_copy: zero-weight row at index {idx} (filter before scatter)",
        );
    }

    let n = indices.len();
    let base = writer.count; // first output row for this scatter

    width_dispatch!(
        writer.pk_stride as usize,
        scatter_pk_wt_nbm,
        batch,
        indices,
        base,
        writer
    );

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
            width_dispatch!(cs, gather_col, src_col, dst_col, indices);
        }
    }

    writer.count += n;
}

#[inline(always)]
fn scatter_pk_wt_nbm<const PKS: usize>(
    batch: &MemBatch<'_>,
    indices: &[u32],
    base: usize,
    writer: &mut DirectWriter<'_>,
    width: usize,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pks = if PKS == 0 { width } else { PKS };
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

#[inline(always)]
fn gather_col<const N: usize>(src: &[u8], dst: &mut [u8], indices: &[u32], width: usize) {
    assert!(N != 0, "a payload column is 1, 2, 4, 8 or 16 bytes, not {width}");
    debug_assert!(dst.len() >= indices.len() * N);
    for (out, &idx) in indices.iter().enumerate() {
        let i = idx as usize;
        debug_assert!((i + 1) * N <= src.len());
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr().add(i * N), dst.as_mut_ptr().add(out * N), N);
        }
    }
}

/// Copy the rows `rows` names, in the order given, each at the weight its triple
/// carries. The sources may be any mix of `MemBatch` and shard backings.
///
/// `cols` is the flat payload-`ColPtr` table the sources were built against;
/// source `si`'s column `pi` is `cols[sources[si].cols_off + pi]`.
pub(crate) fn scatter_unified_sources(
    sources: &[UnifiedSource<'_>],
    cols: &[ColPtr],
    rows: &[(u32, u32, i64)],
    writer: &mut DirectWriter<'_>,
) {
    if rows.is_empty() {
        return;
    }
    #[cfg(debug_assertions)]
    for &(_si, _ri, w) in rows {
        debug_assert_ne!(w, 0, "scatter_unified_sources: zero-weight row (filter before scatter)");
    }
    let n = rows.len();
    let base = writer.count;

    width_dispatch!(
        writer.pk_stride as usize,
        scatter_unified_pk_wt_nbm,
        sources,
        rows,
        base,
        writer
    );

    let schema = writer.schema;
    for (pi, col) in schema.payload_columns() {
        let cs = col.size() as usize;
        if is_german_string(col.type_code) {
            // Blob relocation is per-row regardless; no way to batch.
            for (out, &(si, ri, _)) in rows.iter().enumerate() {
                let src = unsafe { sources.get_unchecked(si as usize) };
                let src_struct = unsafe { cols.get_unchecked(src.cols_off + pi).row(ri as usize, 16) };
                writer.write_string_cell(pi, src_struct, src.blob, base + out);
            }
        } else {
            let dst = &mut writer.col_bufs[pi][base * cs..];
            width_dispatch!(cs, gather_unified_col, sources, cols, rows, pi, dst);
        }
    }

    writer.count += n;
}

// The destination stride is the writer's; source reads keep `src.pk.stride`, so a
// Constant region repeats its one row for every output row.
#[inline(always)]
fn scatter_unified_pk_wt_nbm<const PKS: usize>(
    sources: &[UnifiedSource<'_>],
    rows: &[(u32, u32, i64)],
    base: usize,
    writer: &mut DirectWriter<'_>,
    width: usize,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pks = if PKS == 0 { width } else { PKS };
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
            // A shard predating an `ALTER … ADD COLUMN` carries no bits for the
            // appended columns; `null_pad_mask` forces them NULL.
            let nbm = (nbm_ptr as *const u64).read_unaligned() | src.null_pad_mask;
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
