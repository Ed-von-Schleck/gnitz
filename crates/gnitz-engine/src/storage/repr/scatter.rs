//! Exchange repartition: column-first scatter of selected (possibly reordered,
//! multi-source) rows into a `DirectWriter`.
//!
//! The merge half of `merge.rs` consolidates sorted runs in place; this half
//! *scatters* arbitrary row selections during exchange repartition, joins,
//! distinct, and reduce. The three public entry points
//! (`scatter_copy`, `scatter_multi_source`, `scatter_unified_sources_with_weights`)
//! share one shape: a fused PK + weight + null_bmp pass dispatched on `pk_stride`
//! to a const-width (`PKS`) helper, then one sequential pass per payload column
//! (column widths dispatched to a const-`N` gather). Those const-generic arms and
//! every `#[inline(always)]` are load-bearing: the literal width is what keeps the
//! per-row copy a fixed-width load/store rather than a `memcpy` call. The writer's
//! fixed-region buffers are written directly, so `DirectWriter` keeps them
//! `pub(super)`.

use super::batch::FIXED_REGION_BYTES;
use super::merge::{DirectWriter, MemBatch, UnifiedSource};
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

/// Scatter-copy rows from a batch at the given indices.
/// Indices are NOT sorted — rows are written in the order given.
/// If `weights` is non-empty, uses weights[i] for row i; otherwise reads
/// the weight from the source batch at indices[i].
pub fn scatter_copy(batch: &MemBatch, indices: &[u32], weights: &[i64], writer: &mut DirectWriter) {
    if indices.is_empty() {
        return;
    }

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

/// Column-first scatter from multiple sources in a pre-determined (src_idx, row_idx) order.
///
/// `sources[i]` holds the MemBatch for source `i`; entries in `rows` are `(src_idx, row_idx)`
/// in emission order. Destination writes are sequential per column; source reads are scattered.
/// No zero-weight check — callers must filter before calling.
///
/// **Precondition: no entry of `rows` may name a `None` source, and every
/// `row_idx` must be `< sources[src_idx].count`.** The per-column gathers reach
/// the source through `get_unchecked(..).unwrap_unchecked()`, so naming an absent
/// source is undefined behaviour, not a panic. Callers build `rows` by walking the
/// `Some` sources (`ops::exchange::relay`), which upholds it by construction.
pub fn scatter_multi_source(sources: &[Option<MemBatch<'_>>], rows: &[(u8, u32)], writer: &mut DirectWriter<'_>) {
    if rows.is_empty() {
        return;
    }
    #[cfg(debug_assertions)]
    for &(si, ri) in rows {
        let src = sources[si as usize].as_ref().unwrap();
        debug_assert_ne!(
            src.get_weight(ri as usize),
            0,
            "scatter_multi_source: zero-weight row at source={si} index={ri}",
        );
    }
    let n = rows.len();
    let base = writer.count;

    pk_stride_dispatch!(writer.pk_stride, scatter_mb_pk_wt_nbm, sources, rows, base, writer);

    // One pass per column keeps destination writes sequential.
    let schema = writer.schema;
    for (pi, col) in schema.payload_columns() {
        let cs = col.size() as usize;
        if is_german_string(col.type_code) {
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

// PK stride is the literal `PKS` (1/2/4/8/16) or, for the `PKS = 0` sentinel,
// `writer.pk_stride` read at runtime (compound widths outside the const
// dispatch); weight and null_bmp are always 8. Hoisting the PK stride out of
// the row loop is what unlocks fixed-width loads/stores instead of memcpy in
// the inner loop. All sources share the writer schema, so
// `src.pk_stride == writer.pk_stride` per `debug_assert_eq!`.
#[inline(always)]
fn scatter_mb_pk_wt_nbm<const PKS: usize>(
    sources: &[Option<MemBatch<'_>>],
    rows: &[(u8, u32)],
    base: usize,
    writer: &mut DirectWriter<'_>,
) {
    const FB: usize = FIXED_REGION_BYTES;
    let pks = if PKS == 0 { writer.pk_stride as usize } else { PKS };
    for (out, &(si, ri)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
        debug_assert_eq!(src.pk_stride as usize, pks);
        let row = ri as usize;
        let dst_row = base + out;
        let pk_off = src.offsets[super::batch::REG_PK] + row * pks;
        writer.pk[dst_row * pks..][..pks].copy_from_slice(&src.data[pk_off..pk_off + pks]);
        let w_off = src.offsets[super::batch::REG_WEIGHT] + row * FB;
        writer.weight[dst_row * FB..][..FB].copy_from_slice(&src.data[w_off..w_off + FB]);
        let n_off = src.offsets[super::batch::REG_NULL_BMP] + row * FB;
        writer.null_bmp[dst_row * FB..][..FB].copy_from_slice(&src.data[n_off..n_off + FB]);
    }
}

// Dispatches column-size to a const-N gather; falls back to a runtime-sized
// copy for unusual column widths.
#[inline(always)]
fn gather_mb_col_dispatch(sources: &[Option<MemBatch<'_>>], rows: &[(u8, u32)], pi: usize, cs: usize, dst: &mut [u8]) {
    col_width_dispatch!(cs, gather_mb_col, (sources, rows, pi, dst), {
        for (out, &(si, ri)) in rows.iter().enumerate() {
            let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
            let row = ri as usize;
            let src_off = src.offsets[super::batch::REG_PAYLOAD_START + pi] + row * cs;
            dst[out * cs..][..cs].copy_from_slice(&src.data[src_off..src_off + cs]);
        }
    });
}

#[inline(always)]
fn gather_mb_col<const N: usize>(sources: &[Option<MemBatch<'_>>], rows: &[(u8, u32)], pi: usize, dst: &mut [u8]) {
    for (out, &(si, ri)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize).as_ref().unwrap_unchecked() };
        let off = src.offsets[super::batch::REG_PAYLOAD_START + pi] + ri as usize * N;
        unsafe {
            std::ptr::copy_nonoverlapping(src.data.as_ptr().add(off), dst.as_mut_ptr().add(out * N), N);
        }
    }
}

/// Column-first scatter from multiple `UnifiedSource`s with explicit per-row
/// weights from the merge walk.
///
/// Used by the read-cursor drain (`ReadCursor::scatter_drained_into`) and the
/// flush-path `merge_survivors` + `scatter_survivors`. Callers must pass only net-nonzero weights; both
/// the drain walk and `drive_merge`'s group fold emit only net-nonzero groups.
pub(crate) fn scatter_unified_sources_with_weights(
    sources: &[UnifiedSource],
    rows: &[(u32, u32, i64)],
    writer: &mut DirectWriter<'_>,
) {
    if rows.is_empty() {
        return;
    }
    #[cfg(debug_assertions)]
    for &(_si, _ri, w) in rows {
        debug_assert_ne!(
            w, 0,
            "scatter_unified_sources_with_weights: zero-weight row in drain buffer",
        );
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
                let src_struct = unsafe { src.cols[pi].row(ri as usize, 16) };
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
            gather_unified_col_dispatch(sources, rows, pi, cs, dst);
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
    rows: &[(u32, u32, i64)],
    pi: usize,
    cs: usize,
    dst: &mut [u8],
) {
    col_width_dispatch!(cs, gather_unified_col, (sources, rows, pi, dst), {
        for (out, &(si, ri, _)) in rows.iter().enumerate() {
            let src = unsafe { sources.get_unchecked(si as usize) };
            dst[out * cs..][..cs].copy_from_slice(unsafe { src.cols[pi].row(ri as usize, cs) });
        }
    });
}

// `N` is a const so LLVM emits fixed-width load/store and not a memcpy call.
// `dst` is taken as a raw slice (rather than indexing through `writer.col_bufs`)
// so the bounds check stays out of the hot inner loop.
#[inline(always)]
fn gather_unified_col<const N: usize>(sources: &[UnifiedSource], rows: &[(u32, u32, i64)], pi: usize, dst: &mut [u8]) {
    for (out, &(si, ri, _)) in rows.iter().enumerate() {
        let src = unsafe { sources.get_unchecked(si as usize) };
        let ptr = unsafe { src.cols[pi].row_ptr(ri as usize) };
        unsafe { std::ptr::copy_nonoverlapping(ptr, dst.as_mut_ptr().add(out * N), N) };
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::super::batch::Batch;
    use super::super::merge::ColPtr;
    use super::*;
    use crate::schema::{SchemaDescriptor, MAX_COLUMNS};
    use crate::test_support::{make_schema_u128_i64, wide_pk_3xu64_schema};

    fn make_batch_i64(rows: &[(u128, i64, i64)]) -> Batch {
        crate::test_support::make_batch_u128_raw(&make_schema_u128_i64(), rows)
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
        let pk_stride = schema.pk_stride() as usize;
        let mut out_pk = vec![0u8; n * pk_stride];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob: Vec<u8> = Vec::with_capacity(blob_cap);

        let count;
        {
            let mut writer = DirectWriter::new(
                &mut out_pk,
                &mut out_weight,
                &mut out_null,
                vec![&mut out_col0],
                &mut out_blob,
                schema,
                0,
            );
            scatter_copy(&batch, indices, weights, &mut writer);
            count = writer.row_count();
        }

        let mut result = Vec::new();
        for i in 0..count {
            let pk = crate::test_support::read_pk_opk(&out_pk, i, pk_stride);
            let lo = pk as u64;
            let hi = (pk >> 64) as u64;
            let w = gnitz_wire::read_i64_le(&out_weight, i * 8);
            let val = gnitz_wire::read_i64_le(&out_col0, i * 8);
            result.push((lo, hi, w, val));
        }
        result
    }

    #[test]
    fn test_scatter_basic() {
        let schema = make_schema_u128_i64();
        let b = make_batch_i64(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        // Pick rows 2 and 0 (out of order)
        let result = run_scatter(&b, &[2, 0], &[], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (3, 0, 1, 30));
        assert_eq!(result[1], (1, 0, 1, 10));
    }

    #[test]
    fn test_scatter_empty_indices() {
        let schema = make_schema_u128_i64();
        let b = make_batch_i64(&[(1, 1, 10)]);

        let result = run_scatter(&b, &[], &[], &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_scatter_with_explicit_weights() {
        let schema = make_schema_u128_i64();
        let b = make_batch_i64(&[(1, 1, 10), (2, 1, 20)]);

        // Override weights: row 1 gets w=5, row 0 gets w=-1
        let result = run_scatter(&b, &[1, 0], &[5, -1], &schema);
        assert_eq!(result[0], (2, 0, 5, 20));
        assert_eq!(result[1], (1, 0, -1, 10));
    }
    // -----------------------------------------------------------------------
    // Wide-stride scatter tests (compound PK, pk_stride = 24)
    //
    // Exercises the runtime-stride `PKS = 0` sentinel arm of each scatter
    // helper. Builds source
    // batches via `extend_pk_bytes` (the u128 path panics for stride 24) and a
    // bespoke `DirectWriter` sized for 24-byte PKs (the shared `run_scatter`
    // helper hardcodes 16).
    // -----------------------------------------------------------------------

    fn make_batch_compound_pk_24(rows: &[([u8; 24], i64, i64)]) -> Batch {
        // `empty_with_schema` installs the schema; no follow-up `set_schema`
        // call (it bakes in the single-PK invariant `num_payload + 1 ==
        // num_columns`, which doesn't hold for compound PKs).
        let schema = wide_pk_3xu64_schema();
        let mut b = Batch::empty_with_schema(&schema);
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

    #[test]
    fn test_scatter_col_first_sentinel_wide_pk() {
        let schema = wide_pk_3xu64_schema();
        // Three source rows with distinguishable PKs and payloads.
        let pk_a = [1u8; 24];
        let pk_b = [2u8; 24];
        let pk_c = [3u8; 24];
        let b = make_batch_compound_pk_24(&[(pk_a, 1, 100), (pk_b, 1, 200), (pk_c, 1, 300)]);
        let mb = b.as_mem_batch();
        let indices: &[u32] = &[2, 0]; // reordered

        let n = indices.len();
        let mut pk = vec![0u8; n * 24];
        let mut wt = vec![0u8; n * 8];
        let mut nb = vec![0u8; n * 8];
        let mut col0 = vec![0u8; n * 8];
        let mut blob: Vec<u8> = Vec::with_capacity(1);
        {
            let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
            assert!(
                writer.pk_stride != 8 && writer.pk_stride != 16,
                "test must exercise the PKS = 0 sentinel arm",
            );
            scatter_copy(&mb, indices, &[], &mut writer);
            assert_eq!(writer.row_count(), 2);
        }
        assert_eq!(&pk[0..24], &pk_c);
        assert_eq!(&pk[24..48], &pk_a);
        assert_eq!(gnitz_wire::read_i64_le(&wt, 0), 1);
        assert_eq!(gnitz_wire::read_i64_le(&wt, 8), 1);
        assert_eq!(gnitz_wire::read_i64_le(&col0, 0), 300);
        assert_eq!(gnitz_wire::read_i64_le(&col0, 8), 100);
    }

    #[test]
    fn test_scatter_multi_source_sentinel_wide_pk() {
        let schema = wide_pk_3xu64_schema();
        let pk_a = [0xaau8; 24];
        let pk_b = [0xbbu8; 24];
        let pk_c = [0xccu8; 24];
        let pk_d = [0xddu8; 24];
        let s0 = make_batch_compound_pk_24(&[(pk_a, 1, 10), (pk_b, 1, 20)]);
        let s1 = make_batch_compound_pk_24(&[(pk_c, 1, 30), (pk_d, 1, 40)]);
        let mb0 = s0.as_mem_batch();
        let mb1 = s1.as_mem_batch();
        let sources: Vec<Option<MemBatch<'_>>> = vec![Some(mb0.clone()), Some(mb1.clone())];
        // Drive an interleaved emission order.
        let rows: &[(u8, u32)] = &[(1, 0), (0, 1), (0, 0), (1, 1)];

        let n = rows.len();
        let mut pk = vec![0u8; n * 24];
        let mut wt = vec![0u8; n * 8];
        let mut nb = vec![0u8; n * 8];
        let mut col0 = vec![0u8; n * 8];
        let mut blob: Vec<u8> = Vec::with_capacity(1);
        {
            let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
            assert!(
                writer.pk_stride != 8 && writer.pk_stride != 16,
                "test must exercise the PKS = 0 sentinel arm",
            );
            scatter_multi_source(&sources, rows, &mut writer);
            assert_eq!(writer.row_count(), 4);
        }
        assert_eq!(&pk[0..24], &pk_c);
        assert_eq!(&pk[24..48], &pk_b);
        assert_eq!(&pk[48..72], &pk_a);
        assert_eq!(&pk[72..96], &pk_d);
        let vals: Vec<i64> = (0..4).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
        assert_eq!(vals, vec![30, 20, 10, 40]);
    }

    #[test]
    fn test_scatter_unified_sources_sentinel_wide_pk() {
        let schema = wide_pk_3xu64_schema();
        let pk_a = [0x11u8; 24];
        let pk_b = [0x22u8; 24];
        let pk_c = [0x33u8; 24];
        let s = make_batch_compound_pk_24(&[(pk_a, 1, 7), (pk_b, 1, 8), (pk_c, 1, 9)]);
        let mb = s.as_mem_batch();

        // Build a UnifiedSource that points into `mb`. Only the PK,
        // null_bmp, and one payload column are exercised; remaining `cols`
        // entries are zero-stride placeholders so any accidental read
        // returns deterministic bytes.
        let pk_base = mb.data.as_ptr().wrapping_add(mb.offsets[super::super::batch::REG_PK]);
        let nbm_base = mb
            .data
            .as_ptr()
            .wrapping_add(mb.offsets[super::super::batch::REG_NULL_BMP]);
        let col0_base = mb
            .data
            .as_ptr()
            .wrapping_add(mb.offsets[super::super::batch::REG_PAYLOAD_START]);
        let zero: u8 = 0;
        let mut cols = [ColPtr {
            base: &zero as *const u8,
            stride: 0,
        }; MAX_COLUMNS - 1];
        cols[0] = ColPtr {
            base: col0_base,
            stride: 8,
        };
        let src = UnifiedSource {
            pk: ColPtr {
                base: pk_base,
                stride: 24,
            },
            null_bmp: ColPtr {
                base: nbm_base,
                stride: 8,
            },
            null_pad_mask: 0,
            cols,
            blob_ptr: mb.blob.as_ptr(),
            blob_len: mb.blob.len(),
        };
        let sources = vec![src];
        // (src_idx, row_idx, weight) — explicit weights override the
        // batch-resident ones.
        let rows: &[(u32, u32, i64)] = &[(0, 2, 5), (0, 0, -1), (0, 1, 3)];

        let n = rows.len();
        let mut pk = vec![0u8; n * 24];
        let mut wt = vec![0u8; n * 8];
        let mut nb = vec![0u8; n * 8];
        let mut col0 = vec![0u8; n * 8];
        let mut blob: Vec<u8> = Vec::with_capacity(1);
        {
            let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
            assert!(
                writer.pk_stride != 8 && writer.pk_stride != 16,
                "test must exercise the PKS = 0 sentinel arm",
            );
            scatter_unified_sources_with_weights(&sources, rows, &mut writer);
            assert_eq!(writer.row_count(), 3);
        }
        assert_eq!(&pk[0..24], &pk_c);
        assert_eq!(&pk[24..48], &pk_a);
        assert_eq!(&pk[48..72], &pk_b);
        let weights: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&wt, i * 8)).collect();
        assert_eq!(weights, vec![5, -1, 3]);
        let vals: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
        assert_eq!(vals, vec![9, 7, 8]);
    }

    // -----------------------------------------------------------------------
    // Const-stride scatter pins (PKS=8 and PKS=16 arms)
    //
    // The wide-PK tests above force the *dynamic* arm (stride 24). All other
    // set-op/union/scatter tests use a U128 PK (stride 16) only transitively;
    // none exercise the literal `PKS=8` arm, and none pin `scatter_multi_source`
    // / `scatter_unified_sources_with_weights` to a *specific* const width. These
    // build PKs as distinguishable byte patterns (via `extend_pk_bytes`, which is
    // byte-transparent — scatter copies PK bytes verbatim) and assert the exact
    // PK bytes, weights, and payloads land per output row. If a const arm
    // dispatched the wrong width (e.g. PKS=8 routed to `::<16>`), the per-row PK
    // copy would read/write 16 bytes against an 8-byte-strided region and either
    // panic on the out-of-bounds slice or scramble the PK bytes — caught here.
    // -----------------------------------------------------------------------

    /// Build a stride-8 `(U64 pk, I64 payload)` batch from raw 8-byte PK
    /// patterns. PK bytes are stored verbatim (scatter is byte-transparent).
    fn make_batch_pk8(rows: &[([u8; 8], i64, i64)]) -> Batch {
        let schema = crate::test_support::make_schema_u64_i64();
        let mut b = Batch::empty_with_schema(&schema);
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

    /// Build a stride-16 `(U128 pk, I64 payload)` batch from raw 16-byte PK
    /// patterns.
    fn make_batch_pk16(rows: &[([u8; 16], i64, i64)]) -> Batch {
        let schema = make_schema_u128_i64(); // U128 pk + I64 payload → stride 16
        let mut b = Batch::empty_with_schema(&schema);
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

    #[test]
    fn test_scatter_multi_source_const_pk8() {
        let schema = crate::test_support::make_schema_u64_i64(); // stride 8
        let pk_a = [0x11u8; 8];
        let pk_b = [0x22u8; 8];
        let pk_c = [0x33u8; 8];
        let pk_d = [0x44u8; 8];
        let s0 = make_batch_pk8(&[(pk_a, 1, 10), (pk_b, 1, 20)]);
        let s1 = make_batch_pk8(&[(pk_c, 1, 30), (pk_d, 1, 40)]);
        let mb0 = s0.as_mem_batch();
        let mb1 = s1.as_mem_batch();
        let sources: Vec<Option<MemBatch<'_>>> = vec![Some(mb0.clone()), Some(mb1.clone())];
        let rows: &[(u8, u32)] = &[(1, 0), (0, 1), (0, 0), (1, 1)];

        let n = rows.len();
        let mut pk = vec![0u8; n * 8];
        let mut wt = vec![0u8; n * 8];
        let mut nb = vec![0u8; n * 8];
        let mut col0 = vec![0u8; n * 8];
        let mut blob: Vec<u8> = Vec::with_capacity(1);
        {
            let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
            assert_eq!(writer.pk_stride, 8, "test must exercise the const PKS=8 arm");
            scatter_multi_source(&sources, rows, &mut writer);
            assert_eq!(writer.row_count(), 4);
        }
        // Emission order (1,0),(0,1),(0,0),(1,1) ⇒ pk_c, pk_b, pk_a, pk_d.
        assert_eq!(&pk[0..8], &pk_c);
        assert_eq!(&pk[8..16], &pk_b);
        assert_eq!(&pk[16..24], &pk_a);
        assert_eq!(&pk[24..32], &pk_d);
        let vals: Vec<i64> = (0..4).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
        assert_eq!(vals, vec![30, 20, 10, 40]);
        let weights: Vec<i64> = (0..4).map(|i| gnitz_wire::read_i64_le(&wt, i * 8)).collect();
        assert_eq!(weights, vec![1, 1, 1, 1]);
    }

    #[test]
    fn test_scatter_multi_source_const_pk16() {
        let schema = make_schema_u128_i64(); // U128 pk → stride 16
        let pk_a = [0xa1u8; 16];
        let pk_b = [0xb2u8; 16];
        let pk_c = [0xc3u8; 16];
        let pk_d = [0xd4u8; 16];
        let s0 = make_batch_pk16(&[(pk_a, 1, 10), (pk_b, 1, 20)]);
        let s1 = make_batch_pk16(&[(pk_c, 1, 30), (pk_d, 1, 40)]);
        let mb0 = s0.as_mem_batch();
        let mb1 = s1.as_mem_batch();
        let sources: Vec<Option<MemBatch<'_>>> = vec![Some(mb0.clone()), Some(mb1.clone())];
        let rows: &[(u8, u32)] = &[(1, 0), (0, 1), (0, 0), (1, 1)];

        let n = rows.len();
        let mut pk = vec![0u8; n * 16];
        let mut wt = vec![0u8; n * 8];
        let mut nb = vec![0u8; n * 8];
        let mut col0 = vec![0u8; n * 8];
        let mut blob: Vec<u8> = Vec::with_capacity(1);
        {
            let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
            assert_eq!(writer.pk_stride, 16, "test must exercise the const PKS=16 arm");
            scatter_multi_source(&sources, rows, &mut writer);
            assert_eq!(writer.row_count(), 4);
        }
        assert_eq!(&pk[0..16], &pk_c);
        assert_eq!(&pk[16..32], &pk_b);
        assert_eq!(&pk[32..48], &pk_a);
        assert_eq!(&pk[48..64], &pk_d);
        let vals: Vec<i64> = (0..4).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
        assert_eq!(vals, vec![30, 20, 10, 40]);
    }

    #[test]
    fn test_scatter_unified_sources_const_pk8() {
        let schema = crate::test_support::make_schema_u64_i64(); // stride 8
        let pk_a = [0x11u8; 8];
        let pk_b = [0x22u8; 8];
        let pk_c = [0x33u8; 8];
        let s = make_batch_pk8(&[(pk_a, 1, 7), (pk_b, 1, 8), (pk_c, 1, 9)]);
        let mb = s.as_mem_batch();

        // Build a UnifiedSource pointing into `mb`. Only PK, null_bmp, and one
        // payload column are read; the rest are zero-stride placeholders.
        let pk_base = mb.data.as_ptr().wrapping_add(mb.offsets[super::super::batch::REG_PK]);
        let nbm_base = mb
            .data
            .as_ptr()
            .wrapping_add(mb.offsets[super::super::batch::REG_NULL_BMP]);
        let col0_base = mb
            .data
            .as_ptr()
            .wrapping_add(mb.offsets[super::super::batch::REG_PAYLOAD_START]);
        let zero: u8 = 0;
        let mut cols = [ColPtr {
            base: &zero as *const u8,
            stride: 0,
        }; MAX_COLUMNS - 1];
        cols[0] = ColPtr {
            base: col0_base,
            stride: 8,
        };
        let src = UnifiedSource {
            pk: ColPtr {
                base: pk_base,
                stride: 8,
            },
            null_bmp: ColPtr {
                base: nbm_base,
                stride: 8,
            },
            null_pad_mask: 0,
            cols,
            blob_ptr: mb.blob.as_ptr(),
            blob_len: mb.blob.len(),
        };
        let sources = vec![src];
        let rows: &[(u32, u32, i64)] = &[(0, 2, 5), (0, 0, -1), (0, 1, 3)];

        let n = rows.len();
        let mut pk = vec![0u8; n * 8];
        let mut wt = vec![0u8; n * 8];
        let mut nb = vec![0u8; n * 8];
        let mut col0 = vec![0u8; n * 8];
        let mut blob: Vec<u8> = Vec::with_capacity(1);
        {
            let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
            assert_eq!(writer.pk_stride, 8, "test must exercise the const PKS=8 arm");
            scatter_unified_sources_with_weights(&sources, rows, &mut writer);
            assert_eq!(writer.row_count(), 3);
        }
        // Emission order rows[2],rows[0],rows[1] ⇒ pk_c, pk_a, pk_b.
        assert_eq!(&pk[0..8], &pk_c);
        assert_eq!(&pk[8..16], &pk_a);
        assert_eq!(&pk[16..24], &pk_b);
        let weights: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&wt, i * 8)).collect();
        assert_eq!(weights, vec![5, -1, 3]);
        let vals: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
        assert_eq!(vals, vec![9, 7, 8]);
    }

    #[test]
    fn test_scatter_unified_sources_const_pk16() {
        let schema = make_schema_u128_i64(); // U128 pk → stride 16
        let pk_a = [0xa1u8; 16];
        let pk_b = [0xb2u8; 16];
        let pk_c = [0xc3u8; 16];
        let s = make_batch_pk16(&[(pk_a, 1, 7), (pk_b, 1, 8), (pk_c, 1, 9)]);
        let mb = s.as_mem_batch();

        let pk_base = mb.data.as_ptr().wrapping_add(mb.offsets[super::super::batch::REG_PK]);
        let nbm_base = mb
            .data
            .as_ptr()
            .wrapping_add(mb.offsets[super::super::batch::REG_NULL_BMP]);
        let col0_base = mb
            .data
            .as_ptr()
            .wrapping_add(mb.offsets[super::super::batch::REG_PAYLOAD_START]);
        let zero: u8 = 0;
        let mut cols = [ColPtr {
            base: &zero as *const u8,
            stride: 0,
        }; MAX_COLUMNS - 1];
        cols[0] = ColPtr {
            base: col0_base,
            stride: 8,
        };
        let src = UnifiedSource {
            pk: ColPtr {
                base: pk_base,
                stride: 16,
            },
            null_bmp: ColPtr {
                base: nbm_base,
                stride: 8,
            },
            null_pad_mask: 0,
            cols,
            blob_ptr: mb.blob.as_ptr(),
            blob_len: mb.blob.len(),
        };
        let sources = vec![src];
        let rows: &[(u32, u32, i64)] = &[(0, 2, 5), (0, 0, -1), (0, 1, 3)];

        let n = rows.len();
        let mut pk = vec![0u8; n * 16];
        let mut wt = vec![0u8; n * 8];
        let mut nb = vec![0u8; n * 8];
        let mut col0 = vec![0u8; n * 8];
        let mut blob: Vec<u8> = Vec::with_capacity(1);
        {
            let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
            assert_eq!(writer.pk_stride, 16, "test must exercise the const PKS=16 arm");
            scatter_unified_sources_with_weights(&sources, rows, &mut writer);
            assert_eq!(writer.row_count(), 3);
        }
        assert_eq!(&pk[0..16], &pk_c);
        assert_eq!(&pk[16..32], &pk_a);
        assert_eq!(&pk[32..48], &pk_b);
        let weights: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&wt, i * 8)).collect();
        assert_eq!(weights, vec![5, -1, 3]);
        let vals: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
        assert_eq!(vals, vec![9, 7, 8]);
    }
}
