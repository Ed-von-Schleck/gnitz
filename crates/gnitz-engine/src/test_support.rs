//! Shared `#[cfg(test)]` builders for the crate's wide-PK tests: a single
//! `(U64, U64, U64)`-PK + `I64`-payload schema and one OPK-encoding batch
//! builder, so no test re-derives the order-preserving key layout (§6) by hand.

use std::cmp::Ordering;

use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{compare_pk_bytes, encode_order_preserving_pk, Batch, ConsolidatedBatch};

/// The canonical wide-PK test schema: a 3×U64 compound primary key
/// (`pk_stride = 24`, so `pk_is_wide()`) with a single I64 payload column.
pub(crate) fn wide_pk_3xu64_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2],
    )
}

/// OPK-encode native PK column values (one per PK column, in `pk_columns()`
/// order) into the canonical order-preserving key — the exact bytes the ingest
/// path produces, and byte-identical to [`Batch::extend_pk_opk`]. Signed columns
/// are passed as `v as u128` (the low `size()` little-endian bytes are the
/// two's-complement image the encoder sign-flips).
pub(crate) fn opk_pk(schema: &SchemaDescriptor, vals: &[u128]) -> Vec<u8> {
    let stride = schema.pk_stride() as usize;
    let mut le = vec![0u8; stride];
    let mut off = 0;
    for ((_ord, _ci, col), &v) in schema.pk_columns().zip(vals) {
        let cs = col.size() as usize;
        le[off..off + cs].copy_from_slice(&v.to_le_bytes()[..cs]);
        off += cs;
    }
    let mut opk = vec![0u8; stride];
    encode_order_preserving_pk(schema, &le, &mut opk);
    opk
}

/// Build a consolidated wide-PK batch from native `(c0, c1, c2, weight, payload)`
/// tuples. The PK is OPK-encoded via [`Batch::extend_pk_opk`] (big-endian per
/// column — the at-rest §6 layout), so the bytes are byte-identical to an
/// ingested row and to what `partition_for_pk_bytes` routes.
///
/// `rows` must be OPK-sorted (non-decreasing PK). This is asserted here, at the
/// `sorted = true` / `new_unchecked` claim it guards: `new_unchecked` trusts the
/// flag without inspecting bytes, so a dropped big-endian flip in the encoder
/// (which sorts e.g. 256 before 1) trips this assert instead of silently
/// scattering scrambled bytes past a test's self-referential checks. Equal PKs
/// with differing payloads are allowed (multiset deltas), so only a strictly
/// *decreasing* PK is rejected.
pub(crate) fn make_wide_batch(
    schema: &SchemaDescriptor,
    rows: &[(u64, u64, u64, i64, i64)],
) -> ConsolidatedBatch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(c0, c1, c2, w, val) in rows {
        b.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    for r in 1..b.count {
        assert_ne!(
            compare_pk_bytes(b.get_pk_bytes(r - 1), b.get_pk_bytes(r)),
            Ordering::Greater,
            "make_wide_batch row {r}: non-OPK-sorted PK (encoder regression?)",
        );
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

/// Build a one-row `Batch` from OPK-encoded `pk` bytes (see [`opk_pk`]), DBSP
/// weight `w`, and a single non-null I64 payload `val` at payload slot 0 — the
/// row shape wide-PK storage/dag tests ingest one at a time.
pub(crate) fn wide_row(schema: &SchemaDescriptor, pk: &[u8], w: i64, val: i64) -> Batch {
    let mut b = Batch::with_schema(*schema, 1);
    b.extend_pk_bytes(pk);
    b.extend_weight(&w.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &val.to_le_bytes());
    b.count += 1;
    b
}
