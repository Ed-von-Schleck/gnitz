//! The top-N index: every input row, keyed so that a prefix walk of one group
//! visits its rows in ORDER BY order. The reduce's aggregate-value index with
//! the row attached — the same group-key packer, the same order images, the
//! same "populate before the operator reads" contract.
//!
//! ```text
//! PK      = group key (the AVI packer) ‖ leading 8 bytes of image_0
//! payload = image_0 … image_{k-1}   (BLOB, one per ORDER BY key)
//!         ‖ the output row's payload columns, in output order
//! ```
//!
//! An image is one rank byte placing NULLs, then the column's order image, byte-
//! complemented for DESC. Images are prefix-free, and a BLOB payload compares by
//! content, so the store's own `(PK, payload)` order *is* the ORDER BY order; the
//! leading image bytes in the PK keep one group's entries off the merge's
//! row-by-row equal-PK arm, exactly as the AVI's value column does.

use crate::schema::key::{leading_u64, ReindexPacker};
use crate::schema::{type_code, ColumnLocator, OpBuildErr, SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_BYTES};
use crate::storage::{Batch, Table};
use gnitz_expr::{OrderLocator, RowSource};
use gnitz_wire::OrderKey;

use super::super::group_key::push_group_index_key;
use super::super::order_image::{append_image, ImageKind};

/// The PK column carrying `image_0`'s leading bytes, and the reservation the
/// group key is packed inside.
const LEAD_COL: SchemaColumn = SchemaColumn::new(type_code::U64, 0);
const SUFFIX: [SchemaColumn; 1] = [LEAD_COL];
const LEAD_BYTES: usize = LEAD_COL.size() as usize;
const _: () = assert!(LEAD_BYTES == 8);
/// One image column: content-ordered, never NULL (a NULL value has an image).
const IMAGE_COL: SchemaColumn = SchemaColumn::new(type_code::BLOB, 0);

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
    /// The carried payload columns as the populate reads them, and as `op_topn`
    /// reads them back — one bake each, so the image-column offset between the
    /// two schemas is never re-spelled at a call site.
    carried_in_input: Vec<ColumnLocator>,
    pub(super) carried_in_index: Vec<ColumnLocator>,
}

impl TopNIndex {
    pub(super) fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        carried_cols: &[u32],
    ) -> Result<Self, OpBuildErr> {
        // Without a key the index orders a group's rows arbitrarily, so which rows
        // fill the window would not be a function of the Z-set.
        if order.is_empty() {
            return Err(OpBuildErr::shape("top-n: no order keys"));
        }
        let key_packer = ReindexPacker::new_group_key(input, group_cols, &SUFFIX)?;
        let order = order
            .iter()
            .map(|key| {
                let loc = input.locate(key.col as usize);
                let kind = ImageKind::of(TypeCode::from_validated_u8(loc.type_code()))
                    .ok_or_else(|| OpBuildErr::shape(format!("top-n: order column {} has no order image", key.col)))?;
                Ok(OrderSpec { key: OrderLocator::of(loc, key), kind })
            })
            .collect::<Result<Vec<_>, OpBuildErr>>()?;
        let mut b = crate::schema::DerivedSchema::new();
        push_group_index_key(&mut b, &key_packer, &SUFFIX);
        const OVERFLOW: &str = "top-n: index exceeds MAX_COLUMNS";
        for _ in &order {
            b.push(IMAGE_COL).ok_or_else(|| OpBuildErr::shape(OVERFLOW))?;
        }
        for &c in carried_cols {
            b.push(input.columns[c as usize])
                .ok_or_else(|| OpBuildErr::shape(OVERFLOW))?;
        }
        let schema = b.finish();
        let tail = schema.num_columns() - carried_cols.len();
        Ok(TopNIndex {
            key_packer,
            order,
            carried_in_input: carried_cols.iter().map(|&c| input.locate(c as usize)).collect(),
            carried_in_index: (tail..schema.num_columns()).map(|c| schema.locate(c)).collect(),
            schema,
        })
    }

    /// Pack `row`'s group columns into the leading bytes of `buf`, returning the
    /// prefix `seek_first_positive_with_prefix` matches.
    #[inline]
    pub(super) fn group_prefix<'a, R: RowSource>(&self, buf: &'a mut [u8], src: &R, row: usize) -> &'a [u8] {
        self.key_packer.pack_prefix(buf, src, row)
    }

    /// The index entries `delta` contributes, one per row, at the row's weight.
    /// Left `Raw`: the ingest's consolidation is what sorts it.
    fn batch(&self, delta: &Batch) -> Batch {
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
            // `image_0` doubles as the PK's lead (the whole window is overwritten,
            // so no stale bytes leak across rows); every image is written once.
            image.clear();
            self.order[0].append_image(&mb, row, &mut image);
            key[stride..stride + LEAD_BYTES].copy_from_slice(&leading_u64(&image).to_be_bytes());
            out.begin_row(&key[..stride + LEAD_BYTES], weight);
            out.extend_col_blob(0, &image);
            for (i, spec) in self.order.iter().enumerate().skip(1) {
                image.clear();
                spec.append_image(&mb, row, &mut image);
                out.extend_col_blob(i, &image);
            }
            let mut null_word = 0u64;
            for (pi, loc) in self.carried_in_input.iter().enumerate() {
                out.append_cell_from(k + pi, loc, &mb, row, &mut null_word);
            }
            out.commit_row(null_word);
        }
        out
    }
}

/// Accumulate `delta`'s index entries into the operator's index table. Runs
/// before the operator reads the table, so a prefix walk visits the post-delta
/// group.
pub fn op_populate_topn(
    delta: &Batch,
    table: &mut Table,
    index: &TopNIndex,
) -> Result<(), crate::storage::StorageError> {
    table.ingest_owned_batch(index.batch(delta))
}
