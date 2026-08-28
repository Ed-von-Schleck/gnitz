//! Secondary index integration: AviBake, IntegrateTarget, op_integrate_with_indexes.

use crate::ops::AggDescriptor;
use crate::schema::{type_code, ColumnLocator, SchemaColumn, SchemaDescriptor};
use crate::storage::Batch;
use gnitz_wire::AggFunc;

use crate::schema::key::ReindexPacker;

// ---------------------------------------------------------------------------
// Public descriptor types
// ---------------------------------------------------------------------------

/// The compile-time-baked write-side AVI resources (`Program::avi_bakes`):
/// the composite index schema, the group-key gatherer, the value-indexed
/// (`AggFunc::uses_value_index`) subset of the reduce's descriptors in
/// descriptor order (entry `j` is the aggregate written under ordinal `j` in
/// the combined key `group_cols ‖ ordinal ‖ av_encoded`), and each aggregate
/// column's resolved locator. Baked once at compile so the per-tick
/// population loop re-derives nothing. Carrying the descriptors (rather than
/// a stripped `(col, for_max, type)` tuple) lets the population derive
/// `for_max`/type the same way the reduce read side does, so the two cannot
/// drift.
///
/// [`AviBake::new`] takes the reduce's *whole* descriptor list and applies the
/// `uses_value_index` selection itself, so the ordinal order this documents has
/// one spelling rather than one per caller.
pub(crate) struct AviBake {
    /// Packs a row's group columns into the AVI key's leading OPK prefix. The
    /// same packer that stamps a reindexed `_join_pk`, so the prefix is the OPK
    /// image `AviBake::schema` declares — one byte comparison orders it.
    pub(crate) key_packer: ReindexPacker,
    pub(crate) schema: SchemaDescriptor,
    /// Each value-indexed aggregate paired with its column's location in the
    /// reduce's input schema. Entry `j` is written under ordinal `j`.
    pub(crate) aggs: Vec<(AggDescriptor, ColumnLocator)>,
}

impl AviBake {
    /// The index schema is the key packer's own key columns, then the ordinal
    /// selecting which non-linear aggregate the entry belongs to, then
    /// [`super::util::AVI_AV_COL`]. All columns are PK; there is no payload.
    ///
    /// The ordinal sits **between** the group key and the value so the
    /// byte-ordered key sorts by `(group, ordinal, av)`: `MIN(a)` (ordinal 0) and
    /// `MAX(a)` (ordinal 1) coexist with no collision, and within an ordinal the
    /// per-aggregate `for_max` encoding sorts the extreme first. The empty global
    /// key reduces the prefix to just `ordinal`.
    ///
    /// `None` is unreachable for a well-formed input schema: the group-key layout
    /// reserves these two slots out of the PK budget, so **every** group set has
    /// an index and no reduce is left on a per-epoch trace rescan.
    pub(crate) fn new(src: &SchemaDescriptor, group_by_cols: &[u32], aggs: &[AggDescriptor]) -> Option<Self> {
        let key_packer = ReindexPacker::new_group_key(src, group_by_cols);
        let mut b = crate::schema::DerivedSchema::new();
        for c in key_packer.key_columns() {
            b.push_pk(c)?;
        }
        b.push_pk(SchemaColumn::new(type_code::U8, 0))?; // ordinal
        b.push_pk(SchemaColumn::new(super::util::AVI_AV_COL, 0))?; // av_encoded
        Some(AviBake {
            schema: b.finish(),
            key_packer,
            aggs: aggs
                .iter()
                .filter(|d| d.agg_op.uses_value_index())
                .map(|d| (*d, src.locate(d.col_idx as usize)))
                .collect(),
        })
    }
}

/// What [`op_integrate_with_indexes`] accumulates a delta into. The two are
/// exclusive by construction — the emitter produces one or the other, never both
/// and never neither — so a pair of `Option`s would admit two states nothing
/// produces. The VM states the same rule one layer up in its own
/// `Instr::Integrate` operand; this is that rule at the operator's own boundary.
///
/// The `unsafe` deref does not disappear, it *moves*: `Program::tables` is a
/// `Vec<*mut Table>`, so the VM resolves each pointer at its own seat where the
/// pool lives, and `ops` stops carrying one.
pub(crate) enum IntegrateTarget<'a> {
    /// A trace table: the delta accumulates as rows.
    Trace(&'a mut crate::storage::Table),
    /// The combined aggregate value index: one table serves *every* MIN/MAX
    /// aggregate of a reduce, keyed by the baked schema and key packer.
    Avi(&'a mut crate::storage::Table, &'a AviBake),
}

// ---------------------------------------------------------------------------
// op_integrate_with_indexes
// ---------------------------------------------------------------------------

/// Integrate a delta batch into a target table, optionally populating the
/// combined AggValueIndex secondary index.
///
/// The Rust Table handles memtable capacity internally (flush-on-overflow).
pub(crate) fn op_integrate_with_indexes(
    batch: &Batch,
    target: IntegrateTarget<'_>,
) -> Result<(), crate::storage::StorageError> {
    if batch.count == 0 {
        return Ok(());
    }

    // `op_reduce` now ships its retract+insert output as an honest unconsolidated
    // delta (sorted = consolidated = false); the borrowed ingest consolidates it
    // straight into the memtable's owned copy — one copy per tick, and `batch`
    // stays readable for the AVI population below.
    let (avi_table, bake) = match target {
        IntegrateTarget::Trace(table) => return table.ingest_borrowed_batch(batch),
        IntegrateTarget::Avi(table, bake) => (table, bake),
    };

    let mb = batch.as_mem_batch();

    // Combined AggValueIndex population. Key layout: group_key_bytes ++
    // ordinal(1) ++ av_encoded(8), ordered group-major, then by ordinal, then by
    // value — so the lookup's prefix walk (`agg::apply_agg_from_value_index`) on
    // `group ‖ ordinal` matches the full group+aggregate identity with no hash
    // and no collision. One entry per (row, value-indexed aggregate) is written
    // into the one table with one ingest. `for_max`/type are derived from each
    // baked aggregate descriptor exactly as the reduce read side does.
    {
        let num_aggs = bake.aggs.len();
        // `Raw` from `with_capacity`; the `extend_*` population below never
        // raises it. Capacity is one entry per (row × value-indexed aggregate).
        let mut avi_batch = Batch::with_capacity(bake.schema, batch.count * num_aggs);

        let packer = &bake.key_packer;
        let n = packer.out_stride;
        let mut key = [0u8; crate::schema::MAX_PK_BYTES];
        let mut pk_scratch = [0u8; 16];
        for row in 0..batch.count {
            let weight = mb.get_weight(row);
            // A weight-0 row contributes nothing: consolidation drops the entry
            // it would write and `seek_first_positive_with_prefix` skips it, so
            // writing one is waste, not corruption. The sibling projections
            // (`batch_project_index`, `route_rows_by_pk`) drop it too.
            if weight == 0 {
                continue;
            }
            packer.pack_into(&mut key[..n], &mb, row);
            for (j, (d, loc)) in bake.aggs.iter().enumerate() {
                // PK is never null; a NULL payload aggregate is skipped for this
                // ordinal (the seek then misses → MIN/MAX renders NULL).
                if loc.is_null(&mb, row) {
                    continue;
                }
                // Ordinal is a native u8 — its OPK encoding is its byte. Each
                // ordinal overwrites the constant-length `key[..n + 1 + AV]`
                // window, so no stale bytes leak across the ordinal loop.
                key[n] = j as u8;
                // `native_le_bytes` OPK-decodes a PK-source aggregate, so it
                // encodes identically to the same value in a payload column (and
                // to the batch-walk accumulator's `step_from_batch`, which reads
                // through the same accessor).
                let av_u64 = super::util::encode_ordered(
                    loc.native_le_bytes(&mb, row, &mut pk_scratch),
                    crate::schema::TypeCode::from_validated_u8(loc.type_code()),
                    d.agg_op == AggFunc::Max,
                );
                // Serialise the order-encoded value big-endian: the index orders
                // entries by raw lexicographic byte comparison, so big-endian
                // bytes make lexicographic order match the encoded value's order.
                key[n + 1..n + 1 + super::util::AVI_AV_BYTES].copy_from_slice(&av_u64.to_be_bytes());

                avi_batch.extend_pk_bytes(&key[..n + 1 + super::util::AVI_AV_BYTES]);
                avi_batch.extend_weight(&weight.to_le_bytes());
                avi_batch.extend_null_bmp(&0u64.to_le_bytes());
                avi_batch.count += 1;
            }
        }

        if avi_batch.count > 0 {
            // Propagate like the trace ingest above: `ops` is a Result-returning
            // library layer (its test/bench callers `.unwrap()`); the server
            // consumer (the VM Integrate instruction) is what fail-stops.
            avi_table.ingest_owned_batch(avi_batch)?;
        }
    }

    Ok(())
}
