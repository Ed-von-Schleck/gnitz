//! The top-N index: every input row, keyed so that a prefix walk of one group
//! visits its rows in ORDER BY order. The reduce's aggregate-value index with
//! the row attached — the same group-key packer, the same order images, the
//! same "populate before the operator reads" contract.
//!
//! ```text
//! PK      = group key (the AVI packer) ‖ rank_0 ‖ image_0, a string's cut to its slot
//! payload = image_0 when a string's ‖ rank_i ‖ image_i for i ≥ 1   (BLOB each)
//!         ‖ the output row's payload columns, in output order
//! ```
//!
//! An image is the column's order image, byte-complemented for DESC. A rank
//! byte, over a nullable key only, places NULLs; a NULL has no image. Images are
//! prefix-free, and a BLOB payload compares by content, so the trace's own `(PK,
//! payload)` order *is* the ORDER BY order. A fixed-width `image_0` sits whole in
//! the PK, so only rows equal on the first key share a PK and fall to the
//! merge's row-by-row equal-PK arm; a string's does past its slot.

use crate::algebra::ReindexPacker;
use crate::repr::Batch;
use crate::schema::{oob_col, ColumnLocator, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode};
use gnitz_expr::{OrderLocator, RowSource};
use gnitz_wire::OrderKey;
use gnitz_wire::PkBuf;

use crate::algebra::{
    append_image, has_fixed_image, image_slot_col, int16_image, scalar_image, write_image_slot, ImageKind, WideKind,
    IMAGE_COL,
};

/// The rank byte leading a nullable key's image.
const RANK_COL: SchemaColumn = SchemaColumn::new(TypeCode::U8, false);

/// One ORDER BY key, resolved against the input: the same `(loc, desc,
/// nulls_first)` [`OrderLocator`] the read path's comparator reads, plus the
/// image encoding that puts the same order into bytes. Sharing the spec is what
/// ties the two — a maintained window and an ad-hoc `ORDER BY … LIMIT` must
/// select the same rows.
struct OrderSpec {
    key: OrderLocator,
    kind: ImageKind,
    nullable: bool,
}

impl OrderSpec {
    /// Append the key's image for `row`: over a nullable column the rank byte,
    /// then — non-NULL only — the order image, complemented for DESC. The rank
    /// places NULLs and `desc` does not flip it, exactly as
    /// [`gnitz_expr::cmp_order_keys`] orders them.
    #[inline]
    fn append_image(&self, src: &impl RowSource, row: usize, out: &mut Vec<u8>) {
        if self.nullable {
            let is_null = self.key.loc.is_null(src, row);
            out.push((is_null != self.key.nulls_first) as u8);
            if is_null {
                return;
            }
        }
        append_image(&self.key.loc, self.kind, self.key.desc, src, row, out);
    }

    /// Write the key's lead for `row` into `lead`, [`Self::lead_cols`] wide: the
    /// rank byte of a nullable key, then the image, zero-padded or cut to its
    /// slot. A string's whole image is appended to `image`.
    #[inline]
    fn write_lead(&self, src: &impl RowSource, row: usize, lead: &mut [u8], image: &mut Vec<u8>) {
        let slot = match self.nullable {
            false => lead,
            true => {
                let is_null = self.key.loc.is_null(src, row);
                let (rank, slot) = lead.split_at_mut(1);
                rank[0] = (is_null != self.key.nulls_first) as u8;
                if is_null {
                    slot.fill(0);
                    return;
                }
                slot
            }
        };
        let (loc, desc) = (&self.key.loc, self.key.desc);
        match self.kind {
            ImageKind::Scalar(kind) => slot.copy_from_slice(&scalar_image(loc, kind, desc, src, row).to_be_bytes()),
            ImageKind::Wide(WideKind::Fixed(_)) => slot.copy_from_slice(&int16_image(loc, desc, src, row)),
            ImageKind::Wide(WideKind::Bytes) => {
                append_image(loc, self.kind, desc, src, row, image);
                write_image_slot(slot, image);
            }
        }
    }

    /// The PK columns holding the image's lead: the rank byte of a nullable
    /// key, then the image slot.
    fn lead_cols(&self) -> impl Iterator<Item = SchemaColumn> {
        let slot = image_slot_col(matches!(self.kind, ImageKind::Wide(_)));
        self.nullable.then_some(RANK_COL).into_iter().chain([slot])
    }

    /// Whether the lead holds the image whole, so no payload column repeats it.
    fn fits_lead(&self) -> bool {
        has_fixed_image(self.kind)
    }
}

/// The index resources a `TopNPlan` carries, baked once at compile time.
pub struct TopNIndex {
    key_packer: ReindexPacker,
    pub schema: SchemaDescriptor,
    /// The first ORDER BY key, whose image leads the PK.
    lead: OrderSpec,
    rest: Vec<OrderSpec>,
    /// The carried payload columns as `op_topn` reads them back.
    pub(super) carried_in_index: Vec<ColumnLocator>,
    /// Width of [`OrderSpec::lead_cols`].
    lead_bytes: usize,
}

impl TopNIndex {
    pub(super) fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        output: &SchemaDescriptor,
    ) -> Result<Self, String> {
        let mut order = order.iter().map(|key| {
            let loc = input
                .try_locate(key.col as usize)
                .ok_or_else(|| oob_col("top-n: order column", key.col as u32, input))?;
            Ok(OrderSpec {
                key: OrderLocator::of(loc, key),
                kind: ImageKind::of(loc.type_code()),
                nullable: input.columns[key.col as usize].nullable,
            })
        });
        // Without a key the index orders a group's rows arbitrarily, so which rows
        // fill the window would not be a function of the Z-set.
        let lead = order.next().ok_or_else(|| "top-n: no order keys".to_string())??;
        let rest = order.collect::<Result<Vec<_>, String>>()?;
        let suffix: Vec<SchemaColumn> = lead.lead_cols().collect();
        let (key_packer, mut b) = ReindexPacker::new_group_key(input, group_cols, &suffix)?;
        for _ in 0..rest.len() + usize::from(!lead.fits_lead()) {
            b.push(IMAGE_COL);
        }
        b.push_payload_of(output);
        let schema = b.finish().map_err(|e| format!("top-n: index {e}"))?;
        let tail = schema.num_columns() - output.num_payload_cols();
        Ok(TopNIndex {
            key_packer,
            lead,
            rest,
            carried_in_index: (tail..schema.num_columns()).map(|c| schema.locate(c)).collect(),
            schema,
            lead_bytes: suffix.iter().map(|c| c.size() as usize).sum(),
        })
    }

    /// Pack `row`'s group columns into the leading bytes of `buf`, returning the
    /// prefix `seek_first_positive_with_prefix` matches.
    #[inline]
    pub(super) fn group_prefix<'a, R: RowSource>(&self, buf: &'a mut [u8], src: &R, row: usize) -> &'a [u8] {
        self.key_packer.pack_prefix(buf, src, row)
    }

    /// The least and the greatest [`Self::group_prefix`] among `rows`; `None` for
    /// no row.
    pub(super) fn group_span<R: RowSource>(
        &self,
        src: &R,
        rows: impl Iterator<Item = usize>,
    ) -> Option<(PkBuf, PkBuf)> {
        self.key_packer.prefix_span(src, rows)
    }

    /// The index entries `delta` contributes, one per row at its weight,
    /// unsorted. `carried` locates the output's payload columns in `delta`.
    pub(super) fn batch(&self, delta: &Batch, carried: &[ColumnLocator]) -> Batch {
        let mb = delta.as_mem_batch();
        // One entry per row, and every carried string plus every wide image lands
        // in this heap — so the source's own heap is the presize the crate's
        // string-emitting operators all take.
        let mut out = Batch::with_capacity_blob(&self.schema, delta.count.max(1), delta.blob().len());
        let stride = self.key_packer.out_stride;
        let mut image = Vec::new();
        self.key_packer.for_each_key(&mb, stride + self.lead_bytes, |row, key| {
            let weight = mb.get_weight(row);
            // A weight-0 row contributes nothing: consolidation would drop it.
            if weight == 0 {
                return;
            }
            image.clear();
            self.lead
                .write_lead(&mb, row, &mut key[stride..stride + self.lead_bytes], &mut image);
            out.begin_row(key, weight);
            let mut col = 0;
            if !self.lead.fits_lead() {
                out.extend_col_blob(col, &image);
                col += 1;
            }
            for spec in &self.rest {
                image.clear();
                spec.append_image(&mb, row, &mut image);
                out.extend_col_blob(col, &image);
                col += 1;
            }
            out.append_cells_from(col, carried, &mb, row);
            out.commit_row();
        });
        out
    }
}
