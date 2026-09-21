//! The aggregate-value index (AVI): the secondary index a reduce's MIN/MAX
//! aggregates read their history out of. One entry per (input row × MIN/MAX).
//!
//! ```text
//! PK      = group key ‖ ordinal ‖ value image (8 bytes; 16 when any ordinal is wide)
//! payload = the whole wide image (BLOB), only when an ordinal is wide
//! ```
//!
//! The ordinal names the aggregate, so `MIN(a)` and `MAX(a)` never collide, and
//! a MAX image is complemented, so each ordinal's first entry is its extreme.

use crate::schema::key::ReindexPacker;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_PK_BYTES};
use crate::storage::{Batch, ReadCursor};
use gnitz_expr::payload_bytes;
use gnitz_expr::RowSource;
use gnitz_wire::ImageKind;

use super::super::order_image::{
    append_wide_image, image_slot_col, scalar_image, wide_native, write_image_slot, IMAGE_COL,
};
use super::agg::{Accumulator, ExtremeSpec};

// ---------------------------------------------------------------------------
// Key layout
// ---------------------------------------------------------------------------

const ORDINAL_COL: SchemaColumn = SchemaColumn::new(type_code::U8, 0);
/// [`IMAGE_COL`] is the only payload column.
const WIDE_SLOT: usize = 0;
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
}

impl AviBake {
    /// The index over `accs`' MIN/MAX aggregates, or `None` when there are none.
    pub(super) fn new(
        src: &SchemaDescriptor,
        group_by_cols: &[u32],
        accs: &[Accumulator],
    ) -> Result<Option<Self>, crate::schema::OpBuildErr> {
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
        let suffix = [ORDINAL_COL, image_slot_col(has_wide)];
        let key_packer = ReindexPacker::new_group_key(src, group_by_cols, &suffix)?;
        let mut b = crate::schema::DerivedSchema::new();
        super::super::group_key::push_group_index_key(&mut b, &key_packer, &suffix);
        if has_wide {
            b.push(IMAGE_COL)
                .expect("one payload column fits behind a PK-only schema");
        }
        let schema = b.finish();
        debug_assert!(!has_wide || schema.try_payload_idx(schema.num_columns() - 1) == Some(WIDE_SLOT));
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
            match a.spec.kind {
                ImageKind::Scalar(_) => acc.seed_from_index(&cur.current_pk_bytes()[prefix_len..]),
                ImageKind::Wide(_) => {
                    let (src, row) = cur.current_row_source();
                    acc.seed_from_index(payload_bytes(src, row, WIDE_SLOT));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Population
// ---------------------------------------------------------------------------

/// The index entries `delta` contributes, each at its row's weight, unsorted.
pub fn avi_batch(delta: &Batch, bake: &AviBake) -> Batch {
    let mb = delta.as_mem_batch();
    let mut out = Batch::with_capacity(&bake.schema, (delta.count * bake.aggs.len()).max(1));

    let mut key = [0u8; MAX_PK_BYTES];
    let (mut image, mut scratch) = (Vec::new(), [0u8; 16]);
    for row in 0..delta.count {
        let weight = mb.get_weight(row);
        if weight == 0 {
            continue;
        }
        bake.key_packer.pack_prefix(&mut key, &mb, row);
        for (j, a) in bake.aggs.iter().enumerate() {
            // A NULL has no image: MIN/MAX skips it.
            if a.spec.loc.is_null(&mb, row) {
                continue;
            }
            let ExtremeSpec { loc, kind, max } = a.spec;
            match kind {
                ImageKind::Scalar(kind) => {
                    let av = scalar_image(&loc, kind, max, &mb, row).to_be_bytes();
                    out.push_zero_filled_row(bake.entry(&mut key, j as u8, &av), weight, 0);
                }
                ImageKind::Wide(kind) => {
                    image.clear();
                    append_wide_image(kind, max, wide_native(&loc, kind, &mb, row, &mut scratch), &mut image);
                    out.begin_row(bake.entry(&mut key, j as u8, &image), weight);
                    out.extend_col_blob(WIDE_SLOT, &image);
                    out.commit_row(0);
                }
            }
        }
    }
    out
}

#[cfg(test)]
#[path = "tests/avi.rs"]
mod tests;
