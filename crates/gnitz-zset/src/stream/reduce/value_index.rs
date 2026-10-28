//! The aggregate-value index, the secondary index a reduce's MIN/MAX
//! aggregates read their history out of: one entry per input row and MIN/MAX
//! whose column is not NULL in it.
//!
//! ```text
//! PK      = group key ‖ ordinal ‖ value image (8 bytes; 16 when any ordinal is wide)
//! payload = the whole image (BLOB), only when an ordinal is a string
//! ```
//!
//! The ordinal names the aggregate, so `MIN(a)` and `MAX(a)` never collide, and
//! a MAX image is complemented, so each ordinal's first entry is its extreme.

use crate::algebra::KeyPacker;
use crate::algebra::{image_slot_col, ImageCol, IMAGE_COL};
use crate::algebra::{Agg, AggValues};
use crate::repr::{copy_runs, range_rows, Batch, ReadCursor};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;
use gnitz_wire::{payload_bytes, TypeCode, MAX_PK_BYTES};
use std::borrow::Cow;

// ---------------------------------------------------------------------------
// Key layout
// ---------------------------------------------------------------------------

const ORDINAL_COL: SchemaColumn = SchemaColumn::new(TypeCode::U8, false);
/// [`IMAGE_COL`] is the only payload column.
const IMAGE_SLOT: usize = 0;
const ORDINAL_BYTES: usize = ORDINAL_COL.size();
// The ordinal is stored as one bare byte at the group key's end.
const _: () = assert!(ORDINAL_BYTES == 1);

// ---------------------------------------------------------------------------
// The compile-time index
// ---------------------------------------------------------------------------

/// One value-indexed aggregate; its position in [`ValueIndex::aggs`] is its ordinal.
struct IndexedAgg {
    /// The aggregate this ordinal serves.
    agg_idx: u8,
    image: ImageCol,
}

/// The value index a reduce's plan carries, baked once at compile time.
pub(super) struct ValueIndex {
    /// Packs a row's group columns into the key's leading bytes.
    key_packer: KeyPacker,
    pub(super) schema: SchemaDescriptor,
    aggs: Vec<IndexedAgg>,
}

impl ValueIndex {
    /// The index over `aggs`' MIN/MAX aggregates, or `None` when there are none.
    pub(super) fn new(src: &SchemaDescriptor, group_cols: &[u32], aggs: &[Agg]) -> Result<Option<Self>, String> {
        let aggs: Vec<IndexedAgg> = aggs
            .iter()
            .enumerate()
            .filter_map(|(k, agg)| {
                let image = agg.index_image()?;
                Some(IndexedAgg { agg_idx: k as u8, image })
            })
            .collect();
        if aggs.is_empty() {
            return Ok(None);
        }
        let has_wide = aggs.iter().any(|a| a.image.is_wide());
        let has_payload = aggs.iter().any(|a| !a.image.fits_slot());
        let suffix = [ORDINAL_COL, image_slot_col(has_wide)];
        let (key_packer, mut b) = KeyPacker::new_group_key(src, group_cols, &suffix)?;
        if has_payload {
            b.push(IMAGE_COL);
        }
        let schema = b.finish().map_err(|e| format!("min/max index: {e}"))?;
        debug_assert!(!has_payload || schema.payload_slot(schema.num_columns() - 1) == Some(IMAGE_SLOT));
        Ok(Some(ValueIndex { schema, key_packer, aggs }))
    }

    /// The least and the greatest group key among `rows`; `None` for no row.
    pub(super) fn group_span<R: RowSource>(
        &self,
        src: &R,
        rows: impl Iterator<Item = usize>,
    ) -> Option<(PkBuf, PkBuf)> {
        let mut key = [0u8; MAX_PK_BYTES];
        rows.map(|row| PkBuf::from_bytes(self.key_packer.pack_prefix(&mut key, src, row)))
            .fold(None, |span, group| match span {
                None => Some((group, group)),
                Some((lo, hi)) => Some((lo.min(group), hi.max(group))),
            })
    }

    /// Seed group `g` of every value-indexed aggregate — the group of `row` —
    /// with its extreme out of the index, emptying one whose ordinal has no
    /// positive entry.
    pub(super) fn seed_extremes<R: RowSource>(
        &self,
        cur: &mut ReadCursor,
        src: &R,
        row: usize,
        vals: &mut AggValues,
        g: usize,
    ) {
        let mut key = [0u8; MAX_PK_BYTES];
        let stride = self.key_packer.out_stride;
        let prefix_len = stride + ORDINAL_BYTES;
        self.key_packer.pack_prefix(&mut key, src, row);
        for (j, a) in self.aggs.iter().enumerate() {
            let k = a.agg_idx as usize;
            key[stride] = j as u8;
            if !cur.seek_positive_with_prefix(&key[..prefix_len]) {
                vals.clear_extreme(k, g);
                continue;
            }
            let image = if a.image.fits_slot() {
                Cow::Borrowed(&cur.current_pk_bytes()[prefix_len..])
            } else {
                let (src, row) = cur.current_row_source();
                payload_bytes(src, row, IMAGE_SLOT)
            };
            vals.seed_from_index(k, g, &image);
        }
    }
}

// ---------------------------------------------------------------------------
// Population
// ---------------------------------------------------------------------------

impl ValueIndex {
    /// The index entries `delta` contributes, each at its row's weight, unsorted:
    /// one ordinal at a time, each of its regions in one pass.
    pub(super) fn entries(&self, delta: &Batch) -> Batch {
        // Consolidated, so no row is a ghost.
        debug_assert!(delta.is_consolidated());
        let mb = delta.as_mem_batch();
        let n = delta.len();
        let stride = self.key_packer.out_stride;
        let width = self.schema.pk_stride();
        let value = stride + ORDINAL_BYTES..width;
        let has_payload = self.schema.num_payload_cols() != 0;
        let mut out = Batch::with_capacity(&self.schema, n * self.aggs.len());
        // A string ordinal's whole image of one row; empty between rows.
        let mut image = Vec::new();

        for (j, a) in self.aggs.iter().enumerate() {
            // A NULL has no image: MIN/MAX skips it.
            let runs = delta.runs_without_nulls(a.image.loc.null_bit());
            let rows = range_rows(&runs);
            out.append_session(rows).write(rows, |w| {
                self.key_packer.pack_rows(w.pk_mut(), width, &mb, &runs);
                copy_runs::<8>(delta.weight(), w.weight_mut(), runs.iter().copied(), 8);
                w.null_bmp_mut().fill(0);
                let (mut regions, blob, _) = w.split_mut();
                let keys = regions.next().expect("the PK region");
                // Past the weights and the null words: the image cells, where
                // the index has them. They hold the whole image of a string
                // ordinal, and nothing for any other.
                let mut cells = regions.nth(2).filter(|_| has_payload);
                if a.image.fits_slot() {
                    cells.take().into_iter().for_each(|cells| cells.fill(0));
                }
                let live = runs.iter().flat_map(|&(s, e)| s..e);
                for (i, (row, key)) in live.zip(keys.chunks_exact_mut(width)).enumerate() {
                    key[stride] = j as u8;
                    a.image.write_slot(&mb, row, &mut key[value.clone()], &mut image);
                    if let Some(cells) = cells.as_deref_mut() {
                        cells[i * 16..][..16].copy_from_slice(&gnitz_wire::encode_german_string(&image, blob));
                        image.clear();
                    }
                }
            });
        }
        out
    }
}
