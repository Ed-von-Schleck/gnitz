//! A secondary index's key: its span ([`KeySpec`]), each owner row's entry
//! under it ([`index_entries`]), and the spans of a batch's positive-weight
//! indexed rows ([`spans`]).

use gnitz_wire::{PkBuf, PkListRole, RowSource};

use super::key_pack::KeyPacker;
use crate::repr::{copy_runs, range_rows, runs_where, write_to_batch, Batch, MemBatch};
use crate::schema::{DerivedSchema, SchemaDescriptor};

/// A key span: columns at their own types, packed tightly in order.
#[derive(Clone)]
pub struct KeySpec {
    packer: KeyPacker,
    /// The null bits of the span's payload columns: a row with any of them set
    /// is unindexed.
    null_mask: u64,
}

impl KeySpec {
    /// The span of a secondary index on `cols` of `owner`.
    ///
    /// `Err` where `cols` breaks `gnitz_wire::validate_index_key`.
    pub fn new(cols: &[u32], owner: &SchemaDescriptor) -> Result<Self, String> {
        gnitz_wire::validate_index_key(cols, owner.num_columns(), owner.pk_cols().len(), |c| {
            owner.columns()[c as usize].type_code
        })
        .map_err(|rule| format!("Index: {}", rule.for_role(PkListRole::ColumnList)))?;
        let locs = cols.iter().map(|&c| owner.locate(c as usize));
        Ok(KeySpec {
            null_mask: locs.clone().fold(0, |mask, loc| mask | loc.null_bit()),
            packer: KeyPacker::of_span(locs),
        })
    }

    /// The index schema this spec's entries land in: the key columns, then
    /// `source`'s PK columns, all in the PK.
    pub fn index_schema(&self, source: &SchemaDescriptor) -> SchemaDescriptor {
        let mut b = self.key_columns();
        b.push_pk_of(source);
        b.finish().expect("KeySpec::new bounds the index schema")
    }

    /// The key columns alone, all in the PK: one row's PK region is one span.
    pub fn span_schema(&self) -> SchemaDescriptor {
        self.key_columns()
            .finish()
            .expect("KeySpec::new admits its key columns")
    }

    fn key_columns(&self) -> DerivedSchema {
        let mut b = DerivedSchema::new();
        self.packer.key_columns().for_each(|c| b.push_pk(c));
        b
    }

    /// Span width in bytes — the sum of the column widths.
    #[inline]
    pub fn key_size(&self) -> usize {
        self.packer.out_stride
    }

    /// One row's leading-key span, `key_size()` bytes; `None` when any indexed
    /// column is NULL: the row is unindexed.
    pub fn span(&self, mb: &impl RowSource, row: usize) -> Option<PkBuf> {
        if mb.get_null_word(row) & self.null_mask != 0 {
            return None;
        }
        let mut key = PkBuf::zeroed(self.key_size());
        self.packer.pack_prefix(key.pk_bytes_mut(), mb, row);
        Some(key)
    }

    /// Split an index entry `[span ‖ source PK]` at `key_size()`; nothing is
    /// decoded.
    pub fn split_entry<'a>(&self, entry: &'a [u8]) -> (&'a [u8], &'a [u8]) {
        debug_assert!(entry.len() > self.key_size(), "index entry shorter than its span");
        entry.split_at(self.key_size())
    }
}

/// The row runs of `mb` that `spec` indexes — no NULL in an indexed column — and whose
/// weight `keep` admits.
fn indexed_runs(mb: &MemBatch<'_>, spec: &KeySpec, keep: impl Fn(i64) -> bool) -> Vec<(usize, usize)> {
    let weights = mb.weight().as_chunks::<8>().0;
    let nulls = mb.null_bmp().as_chunks::<8>().0;
    runs_where(weights.len(), |row| {
        keep(i64::from_le_bytes(weights[row])) && u64::from_le_bytes(nulls[row]) & spec.null_mask == 0
    })
}

/// The `spec` span of each positive-weight row of `mb` that `spec` indexes, in row order.
pub fn spans(mb: &MemBatch<'_>, spec: &KeySpec) -> Vec<u8> {
    let runs = indexed_runs(mb, spec, |w| w > 0);
    let key_size = spec.key_size();
    let mut out = vec![0; range_rows(&runs) * key_size];
    spec.packer.pack_rows(&mut out, key_size, mb, &runs);
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
        spec.packer.pack_rows(entries, idx_stride, &mb, &runs);
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
