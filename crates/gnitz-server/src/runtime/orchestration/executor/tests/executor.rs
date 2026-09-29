use super::*;
use crate::test_support::{col_def, make_batch_raw, make_schema_u64_i64};

/// A batch whose rows carry `weights`, one distinct PK each.
fn weighted(weights: &[i64]) -> Batch {
    let rows: Vec<(u64, i64, i64)> = weights.iter().enumerate().map(|(i, &w)| (i as u64, w, 0)).collect();
    make_batch_raw(&make_schema_u64_i64(), &rows)
}

/// A stream is append-only, and nothing clamps its weights the way
/// `enforce_unique_pk` clamps a base table's. Bag multiplicity above 1 is the
/// legal case, so a `> 1` check would be wrong in the other direction.
#[test]
fn stream_push_rejects_only_non_positive_weights() {
    let ok = gnitz_wire::WireConflictMode::Update;
    assert_eq!(stream_push_error(7, &weighted(&[1, 5, 1]), ok), None);
    assert_eq!(stream_push_error(7, &weighted(&[]), ok), None);
    for bad in [vec![0], vec![-1], vec![1, 1, -3], vec![2, 0]] {
        let e = stream_push_error(7, &weighted(&bad), ok).expect("must be rejected");
        assert!(e.contains("append-only"), "got: {e}");
        assert!(e.contains("table 7"), "must name the relation: {e}");
    }
}

/// `Error` mode asserts a primary-key uniqueness a stream does not have, and
/// rejecting it is what keeps `push_reads_committed_state` false. It is refused
/// even when every weight is legal.
#[test]
fn stream_push_rejects_error_conflict_mode() {
    let e = stream_push_error(7, &weighted(&[1]), gnitz_wire::WireConflictMode::Error).expect("must be rejected");
    assert!(e.contains("conflict mode 'error'"), "got: {e}");
}

use gnitz_core::{encode_push_txn, PushFamily};
use gnitz_core::{BatchAppender, Schema, ZSetBatch};
use gnitz_wire::{ColumnDef, TypeCode};

/// A catalog holding `public.t1` and `public.t2`, both `(id U64 PK, v I64)`.
fn two_table_catalog() -> (tempfile::TempDir, CatalogEngine, [u64; 2]) {
    let tmp = tempfile::tempdir().unwrap();
    let mut cat = CatalogEngine::open(tmp.path().to_str().unwrap(), 1).unwrap();
    let cols = [col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let tids = ["public.t1", "public.t2"].map(|name| cat.create_table(name, &cols, &[0]).unwrap() as u64);
    (tmp, cat, tids)
}

/// The client schema `(id U64 PK, v <v>)`.
fn client_schema(v: TypeCode) -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new("v", v, false),
        ],
        pk_cols: vec![0],
    }
}

/// A `PUSH_TXN` body holding, per `(tid, schema, rows)`, a family of `rows` rows.
fn push_txn_body(families: &[(u64, &Schema, usize)]) -> Vec<u8> {
    let batches: Vec<ZSetBatch> = families
        .iter()
        .map(|&(_, schema, rows)| {
            let mut b = ZSetBatch::new(schema);
            for pk in 0..rows as u128 {
                BatchAppender::new(&mut b, schema).add_row(pk, 1).i64_val(10);
            }
            b
        })
        .collect();
    let families: Vec<PushFamily> = families
        .iter()
        .zip(&batches)
        .map(|(&(tid, schema, _), batch)| PushFamily {
            tid,
            schema,
            batch,
            mode: gnitz_wire::WireConflictMode::Update,
            basis: gnitz_wire::txn_frame::BLIND,
        })
        .collect();
    let frame = encode_push_txn(&families);
    frame[gnitz_wire::control::peek_control_block(&frame).unwrap().body].to_vec()
}

/// A zero-row family block is a well-formed item, and still refused: an empty
/// family would open a zone and bump its table's commit LSN.
#[test]
fn a_push_txn_family_with_no_rows_is_refused() {
    let (_tmp, cat, [t1, t2]) = two_table_catalog();
    let schema = client_schema(TypeCode::I64);
    let ok = push_txn_body(&[(t1, &schema, 1)]);
    assert_eq!(decode_push_txn_frame(&cat, &ok).map(|t| t.families.len()).ok(), Some(1));
    let bad = push_txn_body(&[(t1, &schema, 1), (t2, &schema, 0)]);
    let Err(e) = decode_push_txn_frame(&cat, &bad) else {
        panic!("an empty family must be refused");
    };
    assert_eq!(e.text, format!("TXN: empty batch for table {t2}"));
}

/// A family is decoded against its target's catalog record: one laying out other
/// columns is refused, and one naming no relation is `NotFound`.
#[test]
fn a_push_txn_family_is_decoded_against_its_target() {
    let (_tmp, cat, [t1, _]) = two_table_catalog();
    let floats = client_schema(TypeCode::F64);
    let Err(e) = decode_push_txn_frame(&cat, &push_txn_body(&[(t1, &floats, 1)])) else {
        panic!("a family under other column types must be refused");
    };
    assert!(e.text.contains("Schema mismatch"), "{}", e.text);

    let ints = client_schema(TypeCode::I64);
    let Err(e) = decode_push_txn_frame(&cat, &push_txn_body(&[(99_999, &ints, 1)])) else {
        panic!("a family naming no relation must be refused");
    };
    assert_eq!(e.status, WireStatus::NotFound);
}
