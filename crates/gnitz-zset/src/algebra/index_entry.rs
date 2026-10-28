//! A secondary index's projection of its owner's rows: [`index_entries`], and
//! [`spans`] their spans alone.

use super::key_pack::KeyPacker;
use crate::repr::{copy_runs, range_rows, runs_where, write_to_batch, Batch, MemBatch};
use crate::schema::key::KeySpec;
use crate::schema::SchemaDescriptor;

/// The row runs of `mb` that `spec` indexes — no NULL in an indexed column — and whose
/// weight `keep` admits.
fn indexed_runs(mb: &MemBatch<'_>, spec: &KeySpec, keep: impl Fn(i64) -> bool) -> Vec<(usize, usize)> {
    let indexed_slots = spec.locators().iter().fold(0, |slots, loc| slots | loc.null_bit());
    let weights = mb.weight().as_chunks::<8>().0;
    let nulls = mb.null_bmp().as_chunks::<8>().0;
    runs_where(weights.len(), |row| {
        keep(i64::from_le_bytes(weights[row])) && u64::from_le_bytes(nulls[row]) & indexed_slots == 0
    })
}

/// The `spec` span of each positive-weight row of `mb` that `spec` indexes, in row order.
pub fn spans(mb: &MemBatch<'_>, spec: &KeySpec) -> Vec<u8> {
    let runs = indexed_runs(mb, spec, |w| w > 0);
    let key_size = spec.key_size();
    let mut out = vec![0; range_rows(&runs) * key_size];
    KeyPacker::of_span(spec).pack_rows(&mut out, key_size, mb, &runs);
    out
}

/// Each nonzero-weight row of `source` that `spec` indexes, as its index entry
/// `[span ‖ source PK]` at that row's weight, in source order. A row NULL in an
/// indexed column has no entry.
pub fn index_entries(source: &Batch, spec: &KeySpec, idx_schema: &SchemaDescriptor) -> Batch {
    /// Copy each `runs` row's PK behind its entry's span: `W` bytes, or
    /// `src_stride` where `W = 0`.
    #[inline(always)]
    fn suffix<const W: usize>(
        entries: &mut [u8],
        idx_stride: usize,
        key_size: usize,
        pks: &[u8],
        src_stride: usize,
        runs: &[(usize, usize)],
    ) {
        let w = if W == 0 { src_stride } else { W };
        let mut entries = entries.chunks_exact_mut(idx_stride);
        for &(s, e) in runs {
            // The PKs lead the zip: it stops on them without taking an entry it leaves unwritten.
            for (pk, entry) in pks[s * w..e * w].chunks_exact(w).zip(entries.by_ref()) {
                entry[key_size..key_size + w].copy_from_slice(pk);
            }
        }
    }

    let (idx_stride, key_size) = (idx_schema.pk_stride(), spec.key_size());
    let src_stride = source.schema().pk_stride();
    assert_eq!(idx_stride, key_size + src_stride, "index schema of another spec");
    let mb = source.as_mem_batch();
    let runs = indexed_runs(&mb, spec, |w| w != 0);
    let rows = range_rows(&runs);
    write_to_batch(idx_schema, rows, 0, |w| {
        let (entries, weights, nulls) = w.fixed_mut();
        KeyPacker::of_span(spec).pack_rows(entries, idx_stride, &mb, &runs);
        let suffix = match src_stride {
            4 => suffix::<4>,
            8 => suffix::<8>,
            16 => suffix::<16>,
            24 => suffix::<24>,
            32 => suffix::<32>,
            _ => suffix::<0>,
        };
        suffix(entries, idx_stride, key_size, source.pk(), src_stride, &runs);
        copy_runs::<8>(source.weight(), weights, runs.iter().copied(), 8);
        // An index entry is all key: it has no payload column to be NULL.
        nulls.fill(0);
    })
}

#[cfg(test)]
#[path = "tests/index_entry.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/index_entry.rs"]
mod bench;
