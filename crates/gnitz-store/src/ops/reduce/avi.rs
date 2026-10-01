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

use super::super::order_image::{
    append_image, has_fixed_image, image_slot_col, int16_image, scalar_image, write_image_slot, ImageKind, WideKind,
    IMAGE_COL,
};
use super::agg::{Accumulator, ExtremeSpec};
use crate::ops::reindex::ReindexPacker;
use crate::schema::{ColumnLocator, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode, MAX_PK_BYTES};
use crate::storage::{Batch, MemBatch, ReadCursor};
use gnitz_expr::payload_bytes;
use gnitz_expr::RowSource;

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
pub struct AviBake {
    /// Packs a row's group columns into the key's leading bytes.
    key_packer: ReindexPacker,
    pub schema: SchemaDescriptor,
    aggs: Vec<AviAgg>,
    /// Width of the value slot, shared by every ordinal.
    value_bytes: usize,
    has_wide: bool,
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
            b.push(IMAGE_COL)
                .expect("one payload column fits behind a PK-only schema");
        }
        let schema = b.finish();
        debug_assert!(!has_payload || schema.payload_slot(schema.num_columns() - 1) == Some(IMAGE_SLOT));
        Ok(Some(AviBake {
            schema,
            key_packer,
            aggs,
            value_bytes: suffix[1].size() as usize,
            has_wide,
        }))
    }

    /// `group ‖ ordinal` over a buffer the group key is already packed into.
    #[inline]
    fn prefix<'a>(&self, buf: &'a mut [u8], ord: u8) -> &'a [u8] {
        buf[self.key_packer.out_stride] = ord;
        &buf[..self.key_packer.out_stride + ORDINAL_BYTES]
    }

    /// `group ‖ ordinal ‖ image` over the same buffer.
    #[inline]
    fn entry<'a>(&self, buf: &'a mut [u8], ord: u8, image: &[u8]) -> &'a [u8] {
        let n = self.prefix(buf, ord).len();
        write_image_slot(&mut buf[n..n + self.value_bytes], image);
        &buf[..n + self.value_bytes]
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
            if !cur.seek_first_positive_with_prefix(&key[..prefix_len]) {
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

/// The index entries `delta` contributes, each at its row's weight, unsorted.
pub fn avi_batch(delta: &Batch, bake: &AviBake) -> Batch {
    // The VM folds this register before any reader, so no row is a ghost.
    debug_assert!(delta.is_consolidated());
    if bake.has_wide {
        avi_entries::<true>(delta, bake)
    } else {
        avi_entries::<false>(delta, bake)
    }
}

/// Without wide ordinals, the (never-taken) wide arm stays out of the loop.
fn avi_entries<const HAS_WIDE: bool>(delta: &Batch, bake: &AviBake) -> Batch {
    let mb = delta.as_mem_batch();
    let mut out = Batch::with_capacity(&bake.schema, (delta.count * bake.aggs.len()).max(1));

    let width = bake.key_packer.out_stride + ORDINAL_BYTES + bake.value_bytes;
    let mut image = Vec::new();
    bake.key_packer.for_each_key(&mb, width, |row, key| {
        let weight = mb.get_weight(row);
        for (j, a) in bake.aggs.iter().enumerate() {
            let ExtremeSpec { loc, kind, max } = a.spec;
            // A NULL has no image: MIN/MAX skips it.
            if loc.is_null(&mb, row) {
                continue;
            }
            match kind {
                ImageKind::Scalar(kind) => {
                    let image = scalar_image(&loc, kind, max, &mb, row).to_be_bytes();
                    out.push_zero_filled_row(bake.entry(key, j as u8, &image), weight);
                }
                ImageKind::Wide(kind) => {
                    let entry = WideEntry {
                        bake,
                        ord: j as u8,
                        loc: &loc,
                        kind,
                        max,
                        mb: &mb,
                        row,
                        weight,
                    };
                    if HAS_WIDE {
                        entry.push(&mut out, key, &mut image);
                    } else {
                        entry.push_outlined(&mut out, key, &mut image);
                    }
                }
            }
        }
    });
    out
}

/// One wide ordinal's index entry for one row.
struct WideEntry<'a> {
    bake: &'a AviBake,
    ord: u8,
    loc: &'a ColumnLocator,
    kind: WideKind,
    max: bool,
    mb: &'a MemBatch<'a>,
    row: usize,
    weight: i64,
}

impl WideEntry<'_> {
    #[inline(always)]
    fn push(&self, out: &mut Batch, key: &mut [u8], image: &mut Vec<u8>) {
        let Self {
            bake,
            ord,
            loc,
            kind,
            max,
            mb,
            row,
            weight,
        } = *self;
        match kind {
            WideKind::Fixed(_) => {
                let image = int16_image(loc, max, mb, row);
                out.push_zero_filled_row(bake.entry(key, ord, &image), weight);
            }
            WideKind::Bytes => {
                image.clear();
                append_image(loc, ImageKind::Wide(kind), max, mb, row, image);
                out.begin_row(bake.entry(key, ord, image), weight);
                out.extend_col_blob(IMAGE_SLOT, image);
                out.commit_row(0);
            }
        }
    }

    #[inline(never)]
    fn push_outlined(&self, out: &mut Batch, key: &mut [u8], image: &mut Vec<u8>) {
        self.push(out, key, image)
    }
}
