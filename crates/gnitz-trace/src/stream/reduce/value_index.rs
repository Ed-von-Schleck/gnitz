//! The aggregate-value index, the secondary index a reduce's MIN/MAX
//! aggregates read their history out of: one entry per input row and MIN/MAX
//! whose column is not NULL in it.
//!
//! ```text
//! PK      = group prefix ‖ ordinal ‖ value image (8 bytes; 16 when any ordinal is wide)
//! payload = the whole image (BLOB), only when an ordinal is a string
//! ```
//!
//! The group prefix is the reduce's [`GroupPrefix`].
//!
//! The ordinal names the aggregate, so `MIN(a)` and `MAX(a)` never collide, and
//! a MAX image is complemented, so each ordinal's first entry is its extreme.

use crate::runs::ReadCursor;
use gnitz_wire::{MAX_PK_BYTES, TypeCode, payload_bytes};
use gnitz_zset::algebra::{Agg, AggValues, GroupOutKey, GroupPrefix};
use gnitz_zset::algebra::{IMAGE_COL, ImageCol, image_slot_col};
use gnitz_zset::repr::{Batch, copy_runs, range_rows};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};
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
    /// The group prefix leading an entry's key.
    pub(super) prefix: GroupPrefix,
    pub(super) schema: SchemaDescriptor,
    aggs: Vec<IndexedAgg>,
}

impl ValueIndex {
    /// The index over `aggs`' MIN/MAX aggregates of a reduce grouped by `key`;
    /// `None` when there are none.
    pub(super) fn new(key: &GroupOutKey, aggs: &[Agg]) -> Result<Option<Self>, String> {
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
        let (prefix, mut b) = key.index_prefix(&suffix);
        if has_payload {
            b.push(IMAGE_COL);
        }
        let schema = b.finish().map_err(|e| format!("min/max index: {e}"))?;
        debug_assert!(!has_payload || schema.payload_slot(schema.num_columns() - 1) == Some(IMAGE_SLOT));
        Ok(Some(ValueIndex { schema, prefix, aggs }))
    }

    /// Seed group `g` of each value-indexed aggregate `k` — the group whose
    /// prefix is `prefix` — with its extreme out of the index, emptying one whose
    /// ordinal has no positive entry; with `retracted`, the fold of the group's
    /// retractions, only each one that [`AggValues::recedes`]. `cur` is opened by
    /// `open` at its first seek.
    pub(super) fn seed_extremes(
        &self,
        (cur, mut open): (&mut Option<ReadCursor>, impl FnMut() -> ReadCursor),
        prefix: &[u8],
        vals: &mut AggValues,
        g: usize,
        retracted: Option<&AggValues>,
    ) {
        let stride = prefix.len();
        let mut key = [0; MAX_PK_BYTES];
        key[..stride].copy_from_slice(prefix);
        let prefix_len = stride + ORDINAL_BYTES;
        for (j, a) in self.aggs.iter().enumerate() {
            let k = a.agg_idx as usize;
            if retracted.is_some_and(|r| !r.recedes(k, g, vals)) {
                continue;
            }
            let cur = cur.get_or_insert_with(&mut open);
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
    /// one ordinal at a time, each of its regions in one pass. Out of line, so
    /// its row loops compile apart from the caller's.
    #[inline(never)]
    pub(super) fn entries(&self, delta: &Batch) -> Batch {
        // Consolidated, so no row is a ghost.
        debug_assert!(delta.is_consolidated());
        let mb = delta.as_mem_batch();
        let n = delta.len();
        let stride = self.prefix.width();
        let width = self.schema.pk_stride();
        let value = stride + ORDINAL_BYTES..width;
        let has_payload = self.schema.num_payload_cols() != 0;
        let mut out = Batch::with_capacity(&self.schema, n * self.aggs.len());
        // A string ordinal's whole image of one row; empty between rows.
        let mut image = Vec::new();
        // Every row's group prefix, computed once for all ordinals when
        // computing one hashes; each ordinal otherwise computes its own rows'.
        let shared = (self.aggs.len() > 1 && self.prefix.hashes()).then(|| {
            let mut keys = vec![0u8; n * stride];
            self.prefix.write(&mb, &[(0, n)], &mut keys, stride);
            keys
        });

        for (j, a) in self.aggs.iter().enumerate() {
            // A NULL has no image: MIN/MAX skips it.
            let runs = delta.runs_without_nulls(a.image.loc.null_bit());
            let rows = range_rows(&runs);
            out.write_rows(rows, |w| {
                match &shared {
                    Some(keys) => {
                        let live = runs.iter().flat_map(|&(s, e)| s..e);
                        for (row, key) in live.zip(w.pk_mut().chunks_exact_mut(width)) {
                            key[..stride].copy_from_slice(&keys[row * stride..][..stride]);
                        }
                    }
                    None => self.prefix.write(&mb, &runs, w.pk_mut(), width),
                }
                copy_runs::<8>(delta.weight(), w.weight_mut(), &runs, 8);
                w.null_bmp_mut().fill(0);
                let (keys, mut cells, blob) = w.pk_and_payload_mut(IMAGE_SLOT..IMAGE_SLOT + has_payload as usize);
                // The image cells, where the index has them, hold the whole
                // image of a string ordinal, and nothing for any other.
                let mut cells = cells.next();
                if a.image.fits_slot() {
                    cells.take().into_iter().for_each(|cells| cells.fill(0));
                }
                keys.chunks_exact_mut(width).for_each(|k| k[stride] = j as u8);
                if a.image.write_slots(&mb, &runs, keys, width, value.start, value.len()) {
                    return;
                }
                let live = runs.iter().flat_map(|&(s, e)| s..e);
                for (i, (row, key)) in live.zip(keys.chunks_exact_mut(width)).enumerate() {
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
