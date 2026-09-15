use super::*;
use crate::protocol::types::{ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch};
use crate::protocol::wal_block::{decode_wal_block, encode_wal_block};
use crate::test_support::payload_of;
use gnitz_expr::{
    BatchView, CmpOp, Evaluator, ExprResults, IntArithOp, LogicalInstr, LogicalProgram, Output, Reg, SchemaFacts,
};

/// One row's scalar result. `Evaluator` drives whole batches, and these tests
/// assert a row at a time.
fn row_value(ev: &Evaluator, mb: &dyn BatchView, row: usize) -> Option<i64> {
    match ev.eval_all(mb) {
        ExprResults::Scalar(vals) => vals[row],
        ExprResults::Str { .. } => panic!("row_value over a string-valued program"),
    }
}

// ── Fixture A: permuted, non-contiguous compound PK ──────────────────────
//
// ci0 U32   PK, SECOND in the PK list -> OPK offset 8
// ci1 I32   payload slot 0, non-nullable
// ci2 I64   payload slot 1, nullable
// ci3 I64   PK, FIRST in the PK list  -> OPK offset 0
// ci4 STRING payload slot 2, nullable
// ci5 BLOB   payload slot 3, nullable
// ci6 U128   payload slot 4, non-nullable
//
// A naive `payload_col_idx` (the identity) and a column-order OPK
// derivation both pass on a PK-at-column-0 schema and fail here.

const PK3: [i64; 3] = [-5, 100, 1i64 << 40];
const PK0: [u32; 3] = [7, 0, u32::MAX];
const C1: [i32; 3] = [10, -20, 30];
const C2: [i64; 3] = [1000, 0 /* NULL */, -3000];
const C6: [u128; 3] = [1, u128::MAX, 1u128 << 100];
/// Row 0 spills (> 12 bytes), row 1 is inline, row 2 is NULL.
const C4: [&str; 3] = ["aaa long value that spills", "short", ""];
const C5: [&[u8]; 3] = [b"zzz long blob value spilling", b"bb", b""];

fn fixture_a_schema() -> Schema {
    Schema::from_parts(
        vec![
            ColumnDef::new("k1", TypeCode::U32, false),
            ColumnDef::new("a", TypeCode::I32, false),
            ColumnDef::new("b", TypeCode::I64, true),
            ColumnDef::new("k0", TypeCode::I64, false),
            ColumnDef::new("s", TypeCode::String, true),
            ColumnDef::new("z", TypeCode::Blob, true),
            ColumnDef::new("w", TypeCode::U128, false),
        ],
        vec![3, 0],
    )
    .expect("fixture A is a client-valid schema")
}

/// One PK row's **native little-endian** columns, laid out in PK-list order
/// (I64 at 0..8, U32 at 8..12) — the order `push_bytes`'s encode walk reads.
/// Built as bytes, never with `push_u128`: that one truncates a single `u128`
/// to the stride, so a negative I64 would sign-extend over the U32 column.
fn pk12(k0: i64, k1: u32) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[0..8].copy_from_slice(&k0.to_le_bytes());
    out[8..12].copy_from_slice(&k1.to_le_bytes());
    out
}

fn fixture_a_batch() -> ZSetBatch {
    let schema = fixture_a_schema();
    let mut pks = PkColumn::empty_for_schema(&schema);
    for row in 0..3 {
        pks.push_bytes(&schema, &pk12(PK3[row], PK0[row]));
    }
    let mut blob = Vec::new();
    let mut c1 = Vec::new();
    let mut c2 = Vec::new();
    for row in 0..3 {
        c1.extend_from_slice(&C1[row].to_le_bytes());
        c2.extend_from_slice(&C2[row].to_le_bytes());
    }
    ZSetBatch {
        pks,
        weights: vec![1, -1, 3],
        // Row 1 nulls payload slot 1 (ci2); row 2 nulls slots 2 and 3
        // (ci4/ci5) — so the bitmap is not uniformly zero.
        nulls: vec![0, 0b10, 0b1100],
        payload: payload_of(
            &schema,
            vec![
                c1,
                c2,
                german_col(&[Some(C4[0].as_bytes()), Some(C4[1].as_bytes()), None], &mut blob),
                german_col(&[Some(C5[0]), Some(C5[1]), None], &mut blob),
                C6.iter().flat_map(|v| v.to_le_bytes()).collect(),
            ],
        ),
        blob,
    }
}

/// A STRING/BLOB column region: one 16-byte cell per value, a zeroed cell for a
/// null one, spilling into `blob`.
fn german_col(vals: &[Option<&[u8]>], blob: &mut Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(vals.len() * 16);
    for v in vals {
        out.extend_from_slice(&gnitz_wire::encode_german_string(v.unwrap_or(&[]), blob));
    }
    out
}

/// Every payload slot of fixture A as `(slot, width)`.
const A_SLOTS: [(usize, usize); 5] = [(0, 4), (1, 8), (2, 16), (3, 16), (4, 16)];

// ── Fixture B: a single narrow PK — the shape real traffic has ───────────

fn fixture_b_schema() -> Schema {
    Schema::from_parts(
        vec![
            ColumnDef::new("k", TypeCode::U32, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        vec![0],
    )
    .expect("fixture B is a client-valid schema")
}

const B_PK: [u64; 3] = [1, 2, u32::MAX as u64];

fn fixture_b_batch() -> ZSetBatch {
    let mut v = Vec::new();
    for x in [7i64, -8, 9] {
        v.extend_from_slice(&x.to_le_bytes());
    }
    ZSetBatch {
        pks: PkColumn::from_natives(&fixture_b_schema(), B_PK.iter().map(|&x| x as u128)),
        weights: vec![1; 3],
        nulls: vec![0; 3],
        payload: payload_of(&fixture_b_schema(), vec![v]),
        blob: vec![],
    }
}

// ── The region/per-row contract ──────────────────────────────────────────

/// Fixture A's PK columns as the harness wants them, at their PK-list
/// offsets: ci3 (I64) first, ci0 (U32) second.
fn a_pk_expect() -> [(Vec<u128>, u8, usize); 2] {
    [
        (PK3.iter().map(|&v| v as u128).collect(), TypeCode::I64 as u8, 0),
        (PK0.iter().map(|&v| v as u128).collect(), TypeCode::U32 as u8, 8),
    ]
}

#[test]
fn zsetbatch_satisfies_the_region_per_row_contract() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();
    {
        let pk = a_pk_expect();
        gnitz_expr::assert_batchview_consistent(
            &batch,
            3,
            &A_SLOTS,
            &[(pk[0].1, pk[0].2, &pk[0].0), (pk[1].1, pk[1].2, &pk[1].0)],
        );
    }
    // A residual can legitimately get an empty batch: every region length
    // must degrade to 0.
    let empty = ZSetBatch::new(&schema);
    gnitz_expr::assert_batchview_consistent(&empty, 0, &A_SLOTS, &[]);
}

#[test]
fn locate_addresses_the_pk_region_the_builder_hands_out() {
    // Take the address from `locate`, never from the test's own arithmetic:
    // the harness pins an offset absolutely, this pins it to what the
    // *schema* answers, so a `locate` that drifted from the builder's own
    // PK-list walk fails here rather than reading a neighbouring column.
    let check =
        |schema: &Schema, batch: &ZSetBatch, rows: usize, cols: &[(usize, usize)], want: &[(usize, &[u128])]| {
            let pk: Vec<gnitz_expr::PkColExpect<'_>> = want
                .iter()
                .map(|&(ci, vals)| match SchemaFacts::locate(schema, ci) {
                    gnitz_expr::ColumnLocator::Pk { byte_off, type_code, .. } => (type_code, byte_off as usize, vals),
                    other => panic!("column {ci} must locate to the PK region, got {other:?}"),
                })
                .collect();
            gnitz_expr::assert_batchview_consistent(batch, rows, cols, &pk);
        };

    let a_k0: Vec<u128> = PK3.iter().map(|&v| v as u128).collect();
    let a_k1: Vec<u128> = PK0.iter().map(|&v| v as u128).collect();
    check(
        &fixture_a_schema(),
        &fixture_a_batch(),
        3,
        &A_SLOTS,
        &[(3, &a_k0), (0, &a_k1)],
    );

    let b_k: Vec<u128> = B_PK.iter().map(|&v| v as u128).collect();
    check(&fixture_b_schema(), &fixture_b_batch(), 3, &[(0, 8)], &[(0, &b_k)]);
}

// ── The shared evaluator over a client batch ─────────────────────────────

#[test]
fn the_shared_evaluator_reads_a_client_batch() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // ci3 is a PK column: `LoadColInt` accepts one (`ColKind::FixedIntCol` is
    // not payload-only) and lowers to `Instr::LoadPk`, so this exercises
    // `locate`'s PK arm, its payload arm and the region addressing together.
    let ev = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColInt { col: 3 },
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        Output::Result(Reg(2)),
        vec![],
    )
    .resolve_scalar(&schema)
    .expect("program resolves against the client schema");

    for row in 0..3 {
        let v = row_value(&ev, &batch, row).expect("row {row} must not be null");
        assert_eq!(v, PK3[row] + C1[row] as i64, "row {row}");
        // The program names only non-nullable slots, so `no_nulls` is on.
    }
}

#[test]
fn nullable_payload_null_bits_reach_the_evaluator() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // A different program from the one above: `analyze` tests only the
    // slots the instructions name, so naming ci2 (payload
    // slot 1, nullable) is what forces `no_nulls` off.
    let ev = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColInt { col: 3 },
            LogicalInstr::LoadColInt { col: 2 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        Output::Result(Reg(2)),
        vec![],
    )
    .resolve_scalar(&schema)
    .expect("program resolves against the client schema");

    assert_eq!(row_value(&ev, &batch, 0), Some(PK3[0] + C2[0]));
    assert!(row_value(&ev, &batch, 1).is_none(), "row 1 nulls the nullable column");
    assert_eq!(row_value(&ev, &batch, 2), Some(PK3[2] + C2[2]));
}

#[test]
fn filter_over_the_region_path() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // ci2 > 0: row 0 passes (1000), row 1 is NULL (dropped by
    // `bool_bits & !null_bits`), row 2 fails (-3000).
    let ev = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColInt { col: 2 },
            LogicalInstr::LoadConst { val: 0 },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ],
        Output::Result(Reg(2)),
        vec![],
    )
    .resolve_filter(&schema)
    .expect("predicate resolves against the client schema");

    let mut ranges: Vec<(usize, usize)> = Vec::new();
    ev.filter_ranges(&batch, &mut ranges);
    assert_eq!(ranges, vec![(0, 1)]);
}

#[test]
fn string_columns_compare_through_the_shared_blob_heap() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // STRING (ci4) vs BLOB (ci5): both pass `check_col(GermanString)`.
    let ev = LogicalProgram::new(
        vec![LogicalInstr::StrColCol { op: CmpOp::Lt, col_a: 4, col_b: 5 }],
        Output::Result(Reg(0)),
        vec![],
    )
    .resolve_scalar(&schema)
    .expect("string compare resolves against the client schema");

    // Row 0 spills in both columns: with a per-column arena the two cells
    // would share a heap offset and the comparison would read the wrong
    // bytes (and come out equal, not less-than).
    for row in 0..2 {
        let want = (C4[row].as_bytes() < C5[row]) as i64;
        assert_eq!(row_value(&ev, &batch, row), Some(want), "row {row}");
    }
    assert!(row_value(&ev, &batch, 2).is_none(), "row 2 nulls both string columns");
}

#[test]
#[should_panic(expected = "payload slot 2: length")]
fn the_region_list_rejects_a_region_whose_length_contradicts_its_type() {
    let mut batch = fixture_a_batch();
    // The region list's own guard, for a batch that never went through
    // `ZSetBatch::validate` — which states the same rule for the push path.
    batch.payload[2].bytes.truncate(16);
    let _ = regions(&batch);
}

// ── The encode path shares the builder ───────────────────────────────────

#[test]
fn fixture_round_trips_through_encode_and_decode() {
    for (schema, batch, tid) in [
        (fixture_a_schema(), fixture_a_batch(), 77u32),
        (fixture_b_schema(), fixture_b_batch(), 5u32),
    ] {
        let encoded = encode_wal_block(tid, &batch);
        let (decoded, got_tid) = decode_wal_block(&encoded, &schema).expect("block decodes");
        assert_eq!(got_tid, tid);
        assert_eq!(decoded, batch, "batch must survive encode -> decode");
    }
}
