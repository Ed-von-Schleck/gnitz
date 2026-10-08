//! The aggregate-value index (AVI): the secondary index a reduce's MIN/MAX
//! aggregates read their history out of. One entry per (input row × MIN/MAX).
//!
//! ```text
//! PK      = group key ‖ ordinal ‖ value image (8 bytes; 16 when any ordinal is wide)
//! payload = the whole image (BLOB), only when an ordinal is a string
//! ```
//!
//! The ordinal names the aggregate, so `MIN(a)` and `MAX(a)` never collide, and
//! a MAX image is complemented, so each ordinal's first entry is its extreme.

use crate::algebra::ReindexPacker;
use crate::algebra::{
    append_image, has_fixed_image, image_slot_col, int16_image, scalar_image, write_image_slot, ImageKind, WideKind,
    IMAGE_COL,
};
use crate::algebra::{Accumulator, ExtremeSpec};
use crate::repr::{copy_runs, range_rows, Batch, ReadCursor};
use crate::schema::{ColumnLocator, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode, MAX_PK_BYTES};
use gnitz_wire::payload_bytes;
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;
use std::ops::ControlFlow;

// ---------------------------------------------------------------------------
// Key layout
// ---------------------------------------------------------------------------

const ORDINAL_COL: SchemaColumn = SchemaColumn::new(TypeCode::U8, false);
/// [`IMAGE_COL`] is the only payload column.
const IMAGE_SLOT: usize = 0;
const ORDINAL_BYTES: usize = ORDINAL_COL.size() as usize;
// [`AviBake::prefix`] stores the ordinal as one bare byte.
const _: () = assert!(ORDINAL_BYTES == 1);

// ---------------------------------------------------------------------------
// The compile-time bake
// ---------------------------------------------------------------------------

/// One value-indexed aggregate; its position in [`AviBake::aggs`] is its ordinal.
struct AviAgg {
    /// The accumulator this ordinal serves.
    acc_idx: u8,
    spec: ExtremeSpec,
}

/// The AVI resources a reduce's `ReducePlan` carries, baked once at compile time.
pub(super) struct AviBake {
    /// Packs a row's group columns into the key's leading bytes.
    key_packer: ReindexPacker,
    pub(super) schema: SchemaDescriptor,
    aggs: Vec<AviAgg>,
    /// Width of the value slot, shared by every ordinal.
    value_bytes: usize,
}

impl AviBake {
    /// The index over `accs`' MIN/MAX aggregates, or `None` when there are none.
    pub(super) fn new(
        src: &SchemaDescriptor,
        group_by_cols: &[u32],
        accs: &[Accumulator],
    ) -> Result<Option<Self>, String> {
        let aggs: Vec<AviAgg> = accs
            .iter()
            .enumerate()
            .filter_map(|(k, acc)| {
                let spec = acc.extreme_index_spec()?;
                Some(AviAgg { acc_idx: k as u8, spec })
            })
            .collect();
        if aggs.is_empty() {
            return Ok(None);
        }
        let has_wide = aggs.iter().any(|a| matches!(a.spec.kind, ImageKind::Wide(_)));
        let has_payload = aggs.iter().any(|a| !has_fixed_image(a.spec.kind));
        let suffix = [ORDINAL_COL, image_slot_col(has_wide)];
        let (key_packer, mut b) = ReindexPacker::new_group_key(src, group_by_cols, &suffix)?;
        if has_payload {
            b.push(IMAGE_COL);
        }
        let schema = b.finish().map_err(|e| format!("min/max index: {e}"))?;
        debug_assert!(!has_payload || schema.payload_slot(schema.num_columns() - 1) == Some(IMAGE_SLOT));
        Ok(Some(AviBake {
            schema,
            key_packer,
            aggs,
            value_bytes: suffix[1].size() as usize,
        }))
    }

    /// `group ‖ ordinal` over a buffer the group key is already packed into.
    #[inline]
    fn prefix<'a>(&self, buf: &'a mut [u8], ord: u8) -> &'a [u8] {
        buf[self.key_packer.out_stride] = ord;
        &buf[..self.key_packer.out_stride + ORDINAL_BYTES]
    }

    /// The least and the greatest `group` prefix among `rows`; `None` for no row.
    pub(super) fn group_span<R: RowSource>(
        &self,
        src: &R,
        rows: impl Iterator<Item = usize>,
    ) -> Option<(PkBuf, PkBuf)> {
        self.key_packer.prefix_span(src, rows)
    }

    /// Seed every value-indexed accumulator of `row`'s group with its extreme
    /// out of the index, resetting one whose ordinal has no positive entry.
    pub(super) fn seed_extremes<R: RowSource>(
        &self,
        cur: &mut ReadCursor,
        src: &R,
        row: usize,
        accs: &mut [Accumulator],
    ) {
        let mut key = [0u8; MAX_PK_BYTES];
        self.key_packer.pack_prefix(&mut key, src, row);
        for (j, a) in self.aggs.iter().enumerate() {
            let acc = &mut accs[a.acc_idx as usize];
            let prefix_len = self.prefix(&mut key, j as u8).len();
            if !cur.for_each_positive_with_prefix_until(&key[..prefix_len], |_| ControlFlow::Break(())) {
                acc.reset();
                continue;
            }
            let image = if has_fixed_image(a.spec.kind) {
                &cur.current_pk_bytes()[prefix_len..]
            } else {
                let (src, row) = cur.current_row_source();
                payload_bytes(src, row, IMAGE_SLOT)
            };
            acc.seed_from_index(image);
        }
    }
}

// ---------------------------------------------------------------------------
// Population
// ---------------------------------------------------------------------------

/// The index entries `delta` contributes, each at its row's weight, unsorted:
/// one ordinal at a time, each of its regions in one pass.
pub(super) fn avi_batch(delta: &Batch, bake: &AviBake) -> Batch {
    // Consolidated, so no row is a ghost.
    debug_assert!(delta.is_consolidated());
    let mb = delta.as_mem_batch();
    let n = delta.count;
    let stride = bake.key_packer.out_stride;
    let value = stride + ORDINAL_BYTES..stride + ORDINAL_BYTES + bake.value_bytes;
    let width = value.end;
    let has_payload = bake.schema.num_payload_cols() != 0;
    let mut out = Batch::with_capacity(&bake.schema, (n * bake.aggs.len()).max(1));
    // A string ordinal's images, back to back, and each one's end.
    let (mut images, mut ends) = (Vec::new(), Vec::new());

    for (j, a) in bake.aggs.iter().enumerate() {
        let ExtremeSpec { loc, kind, max } = a.spec;
        // A NULL has no image: MIN/MAX skips it.
        let runs = delta.runs_without_nulls(match loc {
            ColumnLocator::Payload { slot, .. } => 1 << slot,
            ColumnLocator::Pk { .. } => 0,
        });
        let live = || runs.iter().flat_map(|&(s, e)| s..e);

        if let ImageKind::Wide(WideKind::Bytes) = kind {
            images.clear();
            ends.clear();
            for row in live() {
                append_image(&loc, kind, max, &mb, row, &mut images);
                ends.push(images.len());
            }
        }

        let rows = range_rows(&runs);
        out.append_session(rows).write(rows, |w| {
            bake.key_packer.pack_rows(w.pk_mut(), width, &mb, &runs);
            copy_runs::<8>(delta.weight_data(), w.weight_mut(), runs.iter().copied(), 8);
            w.null_bmp_mut().fill(0);
            let keys = w.pk_mut().chunks_exact_mut(width);
            match kind {
                ImageKind::Scalar(kind) => {
                    for (key, row) in keys.zip(live()) {
                        key[stride] = j as u8;
                        write_image_slot(
                            &mut key[value.clone()],
                            &scalar_image(&loc, kind, max, &mb, row).to_be_bytes(),
                        );
                    }
                }
                ImageKind::Wide(WideKind::Fixed(_)) => {
                    for (key, row) in keys.zip(live()) {
                        key[stride] = j as u8;
                        write_image_slot(&mut key[value.clone()], &int16_image(&loc, max, &mb, row));
                    }
                }
                ImageKind::Wide(WideKind::Bytes) => {
                    let mut start = 0;
                    for (key, &end) in keys.zip(&ends) {
                        key[stride] = j as u8;
                        write_image_slot(&mut key[value.clone()], &images[start..end]);
                        start = end;
                    }
                }
            }

            if has_payload {
                // The whole image of a string ordinal; nothing for any other.
                let (cells, blob, _) = w.string_col_mut(IMAGE_SLOT);
                match kind {
                    ImageKind::Wide(WideKind::Bytes) => {
                        let mut start = 0;
                        for (cell, &end) in cells.as_chunks_mut::<16>().0.iter_mut().zip(&ends) {
                            *cell = gnitz_wire::encode_german_string(&images[start..end], blob);
                            start = end;
                        }
                    }
                    _ => cells.fill(0),
                }
            }
        });
    }
    out
}
