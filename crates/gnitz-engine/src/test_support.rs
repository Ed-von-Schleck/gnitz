//! Shared `#[cfg(test)]` test helpers: OPK-encoding batch builders (a canonical
//! `(U64, U64, U64)`-PK + `I64`-payload schema, so no test re-derives the §6
//! order-preserving key layout by hand) and the crate's arbitrary type-code
//! proptest strategies (a single source of truth, so PK-eligibility can't drift
//! from `is_pk_eligible`).

use std::cmp::Ordering;

use proptest::prelude::*;

use crate::schema::key::{compare_pk_bytes, encode_order_preserving_pk};
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout};

/// The canonical wide-PK test schema: a 3×U64 compound primary key
/// (`pk_stride = 24`, wide) with a single I64 payload column.
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

/// An all-PK schema: one column per type code in `types`, every column a PK
/// column (`pk_indices = 0..n`) and no payload — the generic PK-shape builder
/// for the OPK encode/compare/route tests.
pub(crate) fn pk_only_schema(types: &[u8]) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
    let pk: Vec<u32> = (0..types.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
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
/// `rows` must be OPK-sorted (non-decreasing PK). The explicit assert below runs
/// in every build (unlike `certify_layout`'s debug-only verify) and gives a
/// targeted message: a dropped big-endian flip in the encoder (which sorts e.g.
/// 256 before 1) trips it here rather than silently scattering scrambled bytes
/// past a test's self-referential checks. Equal PKs with differing payloads are
/// allowed (multiset deltas), so only a strictly *decreasing* PK is rejected;
/// `certify_layout(Consolidated)` then debug-verifies the full (PK, payload) order.
pub(crate) fn make_wide_batch(schema: &SchemaDescriptor, rows: &[(u64, u64, u64, i64, i64)]) -> Batch {
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
    b.certify_layout(Layout::Consolidated, schema);
    b
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

/// I64 pk + I64 payload schema — the signed-PK exercise of the order-preserving
/// key (negatives sort before positives only because the encoder sign-flips).
pub(crate) fn make_schema_i64pk_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build a sorted, consolidated batch with an I64 PK and a single I64 payload
/// from native `(pk, weight, payload)` tuples. The PK is OPK-encoded via
/// [`Batch::extend_pk_opk`] (sign-flipped big-endian), so the bytes match an
/// ingested row; callers must pass OPK-sorted rows.
pub(crate) fn make_batch_i64pk(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> Batch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        b.extend_pk_opk(schema, &[(pk as u64) as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
}

/// U64 pk + a single STRING payload column.
pub(crate) fn make_schema_pk_u64_payload_string() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    )
}

/// U64 pk + a single BLOB payload column.
pub(crate) fn make_schema_pk_u64_payload_blob() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::BLOB, 0),
        ],
        &[0],
    )
}

// ---------------------------------------------------------------------------
// Shared proptest strategies
// ---------------------------------------------------------------------------

/// Every wire `TypeCode`. The single source of truth for the schema-generating
/// proptests across the crate; since any column type is a valid payload, this
/// doubles as the arbitrary-payload-type strategy.
pub(crate) fn arb_type_code() -> impl Strategy<Value = u8> {
    prop_oneof![
        Just(type_code::U8),
        Just(type_code::I8),
        Just(type_code::U16),
        Just(type_code::I16),
        Just(type_code::U32),
        Just(type_code::I32),
        Just(type_code::U64),
        Just(type_code::I64),
        Just(type_code::U128),
        Just(type_code::I128),
        Just(type_code::UUID),
        Just(type_code::F32),
        Just(type_code::F64),
        Just(type_code::STRING),
        Just(type_code::BLOB),
    ]
}

/// The PK-eligible type codes, derived from [`gnitz_wire::is_pk_eligible`] so the
/// set can never drift from the predicate the schema layer enforces (fixed-width
/// integer scalars: U8..U64, I8..I64, U128, I128, UUID — STRING / BLOB / float
/// are rejected by `SchemaDescriptor::new`).
pub(crate) fn arb_pk_type() -> impl Strategy<Value = u8> {
    arb_type_code().prop_filter("type code must be PK-eligible", |&tc| gnitz_wire::is_pk_eligible(tc))
}

/// Re-run the current test binary filtered to `internal_test` (with `envs`
/// set) and assert the child `gnitz_fatal_abort!`s — `_exit(134)` is a normal
/// (non-signal) exit on Linux, so `code() == Some(134)`. A direct in-process
/// call would terminate the test runner; a clean subprocess also avoids any
/// multi-threaded fork hazard. The internal test must guard on one of the
/// `envs` vars so a normal test sweep skips it.
pub(crate) fn assert_test_aborts_134(internal_test: &str, envs: &[(&str, &str)]) {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.arg(internal_test);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let status = cmd.status().unwrap();
    assert_eq!(
        status.code(),
        Some(134),
        "{internal_test} must gnitz_fatal_abort! (exit 134)",
    );
}
