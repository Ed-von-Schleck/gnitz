//! Secondary index integration: AviBake/AviDesc, op_integrate_with_indexes.

use crate::ops::AggDescriptor;
use crate::schema::{type_code, ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor};
use crate::storage::Batch;
use gnitz_wire::AggFunc;

use super::reindex::ReindexPacker;

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
pub struct AviBake {
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
    pub(crate) fn new(src: &SchemaDescriptor, group_by_cols: &[u32], aggs: &[AggDescriptor]) -> Self {
        AviBake {
            key_packer: avi_key_packer(src, group_by_cols),
            schema: make_avi_schema(src, group_by_cols),
            aggs: aggs.iter().map(|d| (*d, src.locate(d.col_idx as usize))).collect(),
        }
    }
}

/// Combined AggValueIndex descriptor for `op_integrate_with_indexes`. One
/// descriptor (and one index table) serves *every* MIN/MAX aggregate of a
/// reduce; everything but the table lives in the baked [`AviBake`].
///
/// The `table` raw pointer is `pub(crate)` rather than `pub`: it is a bare
/// `*mut Table` whose access is constrained to crate-internal call sites
/// (the compiler emits the desc in `vm`, `op_integrate_with_indexes` consumes
/// it) — never a cross-boundary handle. `bake` borrows from the compiled
/// program, so building a desc per tick allocates nothing.
pub struct AviDesc<'a> {
    pub(crate) table: *mut crate::storage::Table,
    pub(crate) bake: &'a AviBake,
}

/// AVI index schema: the group-by columns (native fixed-width types, in GROUP
/// BY order), then a `u8` ordinal selecting which non-linear aggregate the
/// entry belongs to, then the order-encoded aggregate value (U64). All columns
/// are PK; there is no payload. Built only for byte-form-eligible group keys
/// (see `query::compiler::avi_group_key_eligible`).
///
/// The ordinal sits **between** the group key and the value so the byte-ordered
/// key sorts by `(group, ordinal, av)`: a group's ordinal-0 entries precede its
/// ordinal-1 entries, and within an ordinal the per-aggregate `for_max`
/// encoding sorts the extreme first. So `MIN(a)` (ordinal 0) and `MAX(a)`
/// (ordinal 1) coexist with no collision and no `for_max` clash, and one
/// schema shape serves single- and multi-aggregate reduces alike. The empty
/// global key reduces the prefix to just `ordinal`.
pub(crate) fn make_avi_schema(src: &SchemaDescriptor, group_by_cols: &[u32]) -> SchemaDescriptor {
    // All-PK, no payload, PK dense in declared order — `DerivedSchema`'s shape.
    // Its bounds (and `SchemaDescriptor::new`'s MAX_PK_BYTES assert) are the
    // backstop should the `avi_group_key_eligible` gate ever be relaxed past the
    // engine's PK limit.
    let mut b = DerivedSchema::new();
    let over = || panic!("make_avi_schema: composite key exceeds the PK column limit");
    for &c in group_by_cols {
        // Force non-nullable: eligibility guarantees the value is always present.
        b.push_pk(SchemaColumn::new(src.columns[c as usize].type_code, 0))
            .unwrap_or_else(over);
    }
    b.push_pk(SchemaColumn::new(type_code::U8, 0)).unwrap_or_else(over); // ordinal
    b.push_pk(SchemaColumn::new(type_code::U64, 0)).unwrap_or_else(over); // av_encoded
    b.finish()
}

/// The packer that fills an AVI key's group prefix. Each column carries its own
/// source type as the promotion target, so a slot is the plain OPK image of the
/// column at the width [`make_avi_schema`] declares for it. `avi_group_key_eligible`
/// has already excluded nullable and non-PK-eligible group columns, so every slot
/// reads a present, fixed-width value.
pub(crate) fn avi_key_packer(src: &SchemaDescriptor, group_by_cols: &[u32]) -> ReindexPacker {
    let tcs: Vec<u8> = group_by_cols
        .iter()
        .map(|&c| src.columns[c as usize].type_code)
        .collect();
    ReindexPacker::new(src, group_by_cols, &tcs)
}

// ---------------------------------------------------------------------------
// op_integrate_with_indexes
// ---------------------------------------------------------------------------

/// Integrate a delta batch into a target table, optionally populating the
/// combined AggValueIndex secondary index.
///
/// The Rust Table handles memtable capacity internally (flush-on-overflow).
pub fn op_integrate_with_indexes(
    batch: &Batch,
    target_table: Option<&mut crate::storage::Table>,
    avi: Option<&AviDesc<'_>>,
) -> Result<(), crate::storage::StorageError> {
    if batch.count == 0 {
        return Ok(());
    }

    // `op_reduce` now ships its retract+insert output as an honest unconsolidated
    // delta (sorted = consolidated = false); the borrowed ingest consolidates it
    // straight into the memtable's owned copy — one copy per tick, and `batch`
    // stays readable for the AVI population below.
    if let Some(table) = target_table {
        table.ingest_borrowed_batch(batch)?;
    }

    let mb = batch.as_mem_batch();

    // Combined AggValueIndex population. Key layout: group_key_bytes ++
    // ordinal(1) ++ av_encoded(8), ordered group-major, then by ordinal, then by
    // value — so the lookup's prefix walk (`agg::apply_agg_from_value_index`) on
    // `group ‖ ordinal` matches the full group+aggregate identity with no hash
    // and no collision. One entry per (row, value-indexed aggregate) is written
    // into the one table with one ingest. `for_max`/type are derived from each
    // baked aggregate descriptor exactly as the reduce read side does.
    if let Some(avi_desc) = avi {
        let bake = avi_desc.bake;
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
                    d.col_type_code,
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
            let avi_table = unsafe { &mut *avi_desc.table };
            // Propagate like the base-table ingest at the top of this fn: `ops`
            // is a Result-returning library layer (its test/bench callers
            // `.unwrap()`); the runtime consumer (the VM Integrate instruction)
            // is what fail-stops.
            avi_table.ingest_owned_batch(avi_batch)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod avi_encode_tests {
    use super::super::util::{decode_ordered, encode_ordered};
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, TypeCode};
    use crate::storage::Batch;

    fn decode_i32(enc: u64, for_max: bool) -> i32 {
        decode_ordered(if for_max { !enc } else { enc }, TypeCode::I32) as i32
    }

    // A signed aggregate (PK or payload) must encode order-preservingly so the
    // ascending AVI cursor walk yields MIN first, and decode back to the value.
    #[test]
    fn i32_encoding_is_order_preserving_and_round_trips() {
        let vals: [i32; 5] = [i32::MIN, -100, -1, 0, 1_000_000];
        let enc: Vec<u64> = vals
            .iter()
            .map(|v| encode_ordered(&v.to_le_bytes(), TypeCode::I32, false))
            .collect();
        for w in enc.windows(2) {
            assert!(w[0] < w[1], "ascending i32 must encode to ascending u64");
        }
        for (v, &e) in vals.iter().zip(enc.iter()) {
            assert_eq!(decode_i32(e, false), *v, "round-trip i32");
        }
    }

    // for_max inverts the order so the ascending walk yields MAX first.
    #[test]
    fn for_max_inverts_order() {
        let lo = encode_ordered(&(-5i32).to_le_bytes(), TypeCode::I32, true);
        let hi = encode_ordered(&100i32.to_le_bytes(), TypeCode::I32, true);
        assert!(lo > hi, "for_max: larger value must encode smaller");
        assert_eq!(decode_i32(hi, true), 100);
    }

    #[test]
    fn f32_encoding_is_order_preserving() {
        let vals: [f32; 4] = [-2.5, -0.5, 0.5, 2.5];
        let enc: Vec<u64> = vals
            .iter()
            .map(|v| encode_ordered(&v.to_bits().to_le_bytes(), TypeCode::F32, false))
            .collect();
        for w in enc.windows(2) {
            assert!(w[0] < w[1], "ascending f32 must encode to ascending u64");
        }
    }

    #[test]
    fn unsigned_encoding_is_raw_value() {
        let v: u32 = 0xABCD_1234;
        let enc = encode_ordered(&v.to_le_bytes(), TypeCode::U32, false);
        assert_eq!(enc, v as u64);
    }

    // PK-source aggregate columns: the AVI population reads a row's aggregate
    // value through `ColumnLocator::native_le_bytes`, which must OPK-decode a PK
    // column's at-rest bytes to native LE before order-encoding. Without the
    // decode the image is byte-swapped: order breaks (a byte-swap-sensitive pair
    // like 1/256 inverts) and the value no longer round-trips.
    //
    // Schema `[U64 a (pk), <tc> b (pk), <tc> c (payload)]`. Each row stores the
    // same native value in the PK-source column `b` and the payload column `c`,
    // so the read+encode must produce an identical image from either — a PK
    // aggregate encodes exactly like the same value in a payload column. `a` is
    // the row index, keeping every PK distinct. `vals` are native values (signed
    // ones as their `i128` value) in ascending order.
    fn check_pk_source(tc: TypeCode, vals: &[i128]) {
        let signed = gnitz_wire::is_signed_int(tc as u8);
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(tc as u8, 0),
                SchemaColumn::new(tc as u8, 0),
            ],
            &[0, 1],
        );
        let cs = schema.columns[2].size() as usize;
        let payload_idx = schema.try_payload_idx(2).unwrap();

        let mut batch = Batch::with_capacity(schema, vals.len().max(1));
        for (i, &v) in vals.iter().enumerate() {
            // OPK-encode the PK region (sign-flip for a signed `b`); `opk_pk`
            // truncates each u128 to the column width, so a negative `v as u128`
            // still packs the correct two's-complement LE bytes.
            batch.extend_pk_opk(&schema, &[i as u128, v as u128]);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(payload_idx, &(v as u128).to_le_bytes()[..cs]);
            batch.count += 1;
        }
        let mb = batch.as_mem_batch();

        // The AVI value image exactly as `op_integrate_with_indexes` builds it.
        let encode_at = |loc: &crate::schema::ColumnLocator, row: usize, for_max: bool| {
            let mut scratch = [0u8; 16];
            encode_ordered(loc.native_le_bytes(&mb, row, &mut scratch), tc, for_max)
        };
        let loc_pk = schema.locate(1);
        let loc_pl = schema.locate(2);

        for for_max in [false, true] {
            let mut prev: Option<u64> = None;
            for (i, &v) in vals.iter().enumerate() {
                let enc_pk = encode_at(&loc_pk, i, for_max);
                let enc_pl = encode_at(&loc_pl, i, for_max);
                assert_eq!(
                    enc_pk, enc_pl,
                    "PK-source and payload-source must encode identically (v={v}, for_max={for_max})",
                );
                // Order-preserving: for MIN a larger value encodes larger; for
                // MAX the order inverts so the ascending cursor walk yields the
                // max first. With the byte-swap bug the 1/256 pair would break
                // this monotonicity.
                if let Some(p) = prev {
                    if for_max {
                        assert!(enc_pk < p, "MAX: larger value must encode smaller (v={v})");
                    } else {
                        assert!(enc_pk > p, "MIN: larger value must encode larger (v={v})");
                    }
                }
                prev = Some(enc_pk);
                // Round-trips through `decode_ordered` to the value's native bits
                // (sign-extended for signed, zero-extended for unsigned).
                let expected = if signed { (v as i64) as u64 } else { v as u64 };
                assert_eq!(
                    decode_ordered(if for_max { !enc_pk } else { enc_pk }, tc),
                    expected,
                    "round-trip through decode_ordered (v={v}, for_max={for_max})",
                );
            }
        }
    }

    // A signed PK aggregate column with negative and positive values.
    #[test]
    fn signed_pk_source_orders_and_round_trips() {
        check_pk_source(TypeCode::I32, &[i32::MIN as i128, -100, -1, 0, 1_000_000]);
        check_pk_source(
            TypeCode::I64,
            &[i64::MIN as i128, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX as i128],
        );
    }

    // An unsigned PK aggregate column crossing the high-byte boundary: 1
    // (0x0001) and 256 (0x0100) byte-swap into each other, so the buggy
    // encode-without-decode would report 256 < 1.
    #[test]
    fn unsigned_pk_source_high_byte_boundary_orders_and_round_trips() {
        check_pk_source(TypeCode::U16, &[1, 256, 0x0102, 0xFFFF]);
        check_pk_source(
            TypeCode::U64,
            &[1, 256, 0x0100_0000, (u32::MAX as i128) + 1, i64::MAX as i128],
        );
    }
}
