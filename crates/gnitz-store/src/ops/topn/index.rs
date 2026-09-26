//! The top-N index: every input row, keyed so that a prefix walk of one group
//! visits its rows in ORDER BY order. The reduce's aggregate-value index with
//! the row attached — the same group-key packer, the same order images, the
//! same "populate before the operator reads" contract.
//!
//! ```text
//! PK      = group key (the AVI packer) ‖ leading bytes of image_0
//! payload = image_0 … image_{k-1}   (BLOB, one per ORDER BY key)
//!         ‖ the output row's payload columns, in output order
//! ```
//!
//! An image is one rank byte placing NULLs, then the column's order image, byte-
//! complemented for DESC. Images are prefix-free, and a BLOB payload compares by
//! content, so the store's own `(PK, payload)` order *is* the ORDER BY order; the
//! leading image bytes in the PK keep one group's entries off the merge's
//! row-by-row equal-PK arm, exactly as the AVI's value column does.

use crate::schema::key::ReindexPacker;
use crate::schema::{ColumnLocator, OpBuildErr, SchemaDescriptor, SchemaFacts, MAX_PK_BYTES};
use crate::storage::Batch;
use gnitz_expr::{OrderLocator, RowSource};
use gnitz_wire::OrderKey;

use super::super::order_image::{append_image, image_slot_col, write_image_slot, ImageKind, IMAGE_COL};

/// One ORDER BY key, resolved against the input: the same `(loc, desc,
/// nulls_first)` [`OrderLocator`] the read path's comparator reads, plus the
/// image encoding that puts the same order into bytes. Sharing the spec is what
/// ties the two — a maintained window and an ad-hoc `ORDER BY … LIMIT` must
/// select the same rows.
struct OrderSpec {
    key: OrderLocator,
    kind: ImageKind,
}

impl OrderSpec {
    /// Append the key's image for `row`: the rank byte, then — non-NULL only —
    /// the order image, complemented for DESC. The rank places NULLs and `desc`
    /// does not flip it, exactly as [`gnitz_expr::cmp_order_keys`] orders them.
    #[inline]
    fn append_image(&self, src: &impl RowSource, row: usize, out: &mut Vec<u8>) {
        let is_null = self.key.loc.is_null(src, row);
        out.push((is_null != self.key.nulls_first) as u8);
        if !is_null {
            append_image(&self.key.loc, self.kind, self.key.desc, src, row, out);
        }
    }
}

/// The index resources a `TopNPlan` carries, baked once at compile time.
pub struct TopNIndex {
    key_packer: ReindexPacker,
    pub schema: SchemaDescriptor,
    /// Never empty, so `order[0]` — whose image leads the PK — always exists.
    order: Vec<OrderSpec>,
    /// The carried payload columns as `op_topn` reads them back.
    pub(super) carried_in_index: Vec<ColumnLocator>,
    /// Width of the lead slot this bake's suffix reserved, by `order[0]`'s kind.
    lead_bytes: usize,
}

impl TopNIndex {
    pub(super) fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        output: &SchemaDescriptor,
    ) -> Result<Self, OpBuildErr> {
        // Without a key the index orders a group's rows arbitrarily, so which rows
        // fill the window would not be a function of the Z-set.
        if order.is_empty() {
            return Err(OpBuildErr::shape("top-n: no order keys"));
        }
        let order: Vec<OrderSpec> = order
            .iter()
            .map(|key| {
                let loc = input
                    .try_locate(key.col as usize)
                    .ok_or_else(|| OpBuildErr::oob_col("top-n: order column", key.col as u32, input))?;
                Ok(OrderSpec {
                    key: OrderLocator::of(loc, key),
                    kind: ImageKind::of(loc.type_code()),
                })
            })
            .collect::<Result<_, OpBuildErr>>()?;
        let suffix = [image_slot_col(matches!(order[0].kind, ImageKind::Wide(_)))];
        let (key_packer, mut b) = ReindexPacker::new_group_key(input, group_cols, &suffix)?;
        let over = |e| OpBuildErr::shape(format!("top-n: index {e}"));
        for _ in &order {
            b.push(IMAGE_COL).map_err(over)?;
        }
        for (_, &c) in output.payload_columns() {
            b.push(c).map_err(over)?;
        }
        let schema = b.finish();
        let tail = schema.num_columns() - output.num_payload_cols();
        Ok(TopNIndex {
            key_packer,
            order,
            carried_in_index: (tail..schema.num_columns()).map(|c| schema.locate(c)).collect(),
            schema,
            lead_bytes: suffix[0].size() as usize,
        })
    }

    /// Pack `row`'s group columns into the leading bytes of `buf`, returning the
    /// prefix `seek_first_positive_with_prefix` matches.
    #[inline]
    pub(super) fn group_prefix<'a, R: RowSource>(&self, buf: &'a mut [u8], src: &R, row: usize) -> &'a [u8] {
        self.key_packer.pack_prefix(buf, src, row)
    }

    /// The index entries `delta` contributes, one per row at its weight,
    /// unsorted. `carried` locates the output's payload columns in `delta`.
    pub(super) fn batch(&self, delta: &Batch, carried: &[ColumnLocator]) -> Batch {
        let mb = delta.as_mem_batch();
        // One entry per row, and every carried string plus every wide image lands
        // in this heap — so the source's own heap is the presize the crate's
        // string-emitting operators all take.
        let mut out = Batch::with_capacity_blob(&self.schema, delta.count.max(1), delta.blob.len());
        let k = self.order.len();
        let stride = self.key_packer.out_stride;
        let mut key = [0u8; MAX_PK_BYTES];
        let mut image = Vec::new();
        for row in 0..delta.count {
            let weight = mb.get_weight(row);
            // A weight-0 row contributes nothing: consolidation would drop it.
            if weight == 0 {
                continue;
            }
            self.group_prefix(&mut key, &mb, row);
            // `image_0` doubles as the PK's lead; every image is written once.
            image.clear();
            self.order[0].append_image(&mb, row, &mut image);
            write_image_slot(&mut key[stride..stride + self.lead_bytes], &image);
            out.begin_row(&key[..stride + self.lead_bytes], weight);
            out.extend_col_blob(0, &image);
            for (i, spec) in self.order.iter().enumerate().skip(1) {
                image.clear();
                spec.append_image(&mb, row, &mut image);
                out.extend_col_blob(i, &image);
            }
            let mut null_word = 0u64;
            for (pi, loc) in carried.iter().enumerate() {
                out.append_cell_from(k + pi, loc, &mb, row, &mut null_word);
            }
            out.commit_row(null_word);
        }
        out
    }
}
