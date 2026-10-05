use super::*;
use crate::project::reply_program;
use crate::test_support::{col, ncol, schema};
use gnitz_core::BatchAppender;
use gnitz_wire::{payload_is_null, payload_str, payload_u64};
use gnitz_wire::{ColumnDef, TypeCode};

/// A spilled body, and one the map computes.
const LONG: &str = "a string past the inline prefix";
const FALLBACK: &str = "a computed fallback past the inline prefix";

/// `(k I64 PK, v I64 NULL, s STRING NULL)`.
fn source() -> Schema {
    schema(
        vec![
            col("k", TypeCode::I64),
            ncol("v", TypeCode::I64),
            ncol("s", TypeCode::String),
        ],
        &[0],
    )
}

/// The reply map of the SELECT list `sql` over [`source`]: the hidden key, then the items.
pub(super) fn map_of(src: &Schema, sql: &[&str]) -> ClientMap {
    let payload: Vec<_> = sql
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let bound = crate::test_support::bind_sql(e, src).unwrap();
            let def = ColumnDef::new(format!("c{i}"), bound.infer_ty(&src.columns).tc, true);
            (bound, def)
        })
        .collect();
    let (out, program) = reply_program(payload, src).unwrap();
    ClientMap::new(program, src, Arc::new(out)).unwrap()
}

/// A PK copy is the key's native value — the OPK sign flip undone — a payload copy
/// moves with its NULLs, and every row keeps its key and weight.
#[test]
fn a_key_copy_decodes_and_a_payload_copy_moves() {
    let src = source();
    let mut b = ZSetBatch::new(&src);
    let mut app = BatchAppender::new(&mut b);
    app.add_row((-5i64) as u128, 1).i64_val(7).null();
    app.add_row(3, 2).null().null();
    let pks = b.pks.clone();

    let out = map_of(&src, &["v", "k"]).apply(b);

    assert_eq!(out.pks, pks);
    assert_eq!(out.weights, [1, 2]);
    assert_eq!(payload_u64(&out, 0, 0) as i64, 7);
    assert!(payload_is_null(&out, 1, 0));
    assert_eq!(
        [payload_u64(&out, 0, 1) as i64, payload_u64(&out, 1, 1) as i64],
        [-5, 3]
    );
    assert!(!payload_is_null(&out, 1, 1), "a key copy is never NULL");
}

/// A computed string spills into the output arena while a copied one keeps its
/// offset into the source's, so both must read back in one batch.
#[test]
fn a_computed_string_spills_beside_a_copied_one() {
    let src = source();
    let mut b = ZSetBatch::new(&src);
    let mut app = BatchAppender::new(&mut b);
    app.add_row(1, 1).i64_val(0).str_val(LONG);
    app.add_row(2, 1).null().null();

    let out = map_of(
        &src,
        &["s", &format!("COALESCE(CASE WHEN v IS NULL THEN s END, '{FALLBACK}')")],
    )
    .apply(b);

    assert_eq!(payload_str(&out, 0, 0).unwrap().to_owned(), LONG);
    assert!(payload_is_null(&out, 1, 0));
    assert_eq!(
        [
            payload_str(&out, 0, 1).unwrap().to_owned(),
            payload_str(&out, 1, 1).unwrap().to_owned()
        ],
        [FALLBACK, FALLBACK]
    );
}
