use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

/// The column list `(type_code, nullable)` pairs describe, with `pk_index` the
/// single PK column — the shape every builder case below is written against.
fn make_schema_cols(cols: &[(u8, u8)], pk_index: u32) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
    for (i, &(tc, nullable)) in cols.iter().enumerate() {
        columns[i] = SchemaColumn::new(tc, nullable);
    }
    SchemaDescriptor::new(&columns[..cols.len()], &[pk_index])
}

// With a non-leading compound PK the payload slots are renumbered around every
// PK position, so `payload_idx = ci - 1` does not hold. For pk_indices=[1, 2]
// over four columns, payload slot 0 maps to logical column 0 and slot 1 to
// logical column 3 — never to 0 and 1.
#[test]
fn batch_builder_physical_col_idx_compound_pk() {
    let cols = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
    ];
    let schema = SchemaDescriptor::new(&cols, &[1, 2]);
    let mut bb = BatchBuilder::new(schema);
    assert_eq!(bb.curr_col, 0);
    assert_eq!(bb.physical_col_idx(), 0);
    bb.curr_col = 1;
    assert_eq!(bb.physical_col_idx(), 3);
}

/// `BatchBuilder` over two nullable STRING columns: inline cells, cells that
/// spill to the blob heap, the empty string, and a NULL whose cell must be
/// zeroed rather than left holding the previous row's bytes.
#[test]
fn batch_builder_writes_string_cells_and_nulls() {
    let schema = make_schema_cols(
        &[(type_code::U64, 0), (type_code::STRING, 1), (type_code::STRING, 1)],
        0,
    );

    // (col 0, col 1). Long values (> 12 bytes) land in the blob heap, short
    // ones stay inline in the 16-byte struct. `None` is a NULL cell.
    type Row<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);
    let cases: &[Row] = &[
        (Some(b"Alice"), Some(b"short")),
        (Some(b"Bob has a long name!"), Some(b"Also quite a long description")),
        (Some(b""), Some(b"nonempty")),
        (None, Some(b"another long one for blob storage")),
    ];

    let mut bb = BatchBuilder::new(schema);
    for (pk, &(a, b)) in cases.iter().enumerate() {
        bb.begin_row(pk as u128, 1);
        for cell in [a, b] {
            match cell {
                Some(v) => bb.put_blob(v),
                None => bb.put_null(),
            }
        }
        bb.end_row();
    }
    let batch = bb.finish();

    assert_eq!(batch.count, cases.len());
    for (i, &(a, b)) in cases.iter().enumerate() {
        for (col, want) in [(0usize, a), (1, b)] {
            assert_eq!(
                batch.get_null_word(i) >> col & 1,
                u64::from(want.is_none()),
                "row {i} col {col} null bit"
            );
            match want {
                Some(v) => assert_eq!(
                    crate::test_support::read_german_string(&batch, col, i),
                    v,
                    "row {i} col {col}"
                ),
                None => assert!(
                    batch.get_col_ptr(i, col, 16).iter().all(|&b| b == 0),
                    "row {i} col {col}: a null cell must be zeroed"
                ),
            }
        }
    }
}

// 3.14 / 2.718 are test-fixture values exercising f32/f64 round-trip, not
// approximations of PI/E.
#[test]
#[allow(clippy::approx_constant)]
fn batch_builder_writes_every_payload_type_at_its_own_width() {
    // U64 pk, then one of each remaining type at payload index 0..=11.
    let schema = make_schema_cols(
        &[
            (type_code::U64, 0),    // pk
            (type_code::U8, 0),     // pi 0
            (type_code::I8, 0),     // pi 1
            (type_code::U16, 0),    // pi 2
            (type_code::I16, 0),    // pi 3
            (type_code::U32, 0),    // pi 4
            (type_code::I32, 0),    // pi 5
            (type_code::F32, 0),    // pi 6
            (type_code::U64, 0),    // pi 7
            (type_code::I64, 0),    // pi 8
            (type_code::F64, 0),    // pi 9
            (type_code::STRING, 0), // pi 10
            (type_code::I128, 0),   // pi 11
        ],
        0,
    );

    // A negative 16-byte payload (a cross-sign `_join_pk` surfaced into a
    // payload slot), with bits in both halves.
    let i128_val: i128 = -0x0123_4567_89AB_CDEF_1122_3344_5566_7788;

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(100, 1);
    for v in [42i128, -7, 1000, -500, 70000, -12345] {
        bb.put_int(v as u128);
    }
    bb.put_float(3.14);
    bb.put_int(0x1234_5678_9ABC_DEF0u128);
    bb.put_int(-99999i128 as u128);
    bb.put_float(2.718281828);
    bb.put_blob(b"hello world!");
    bb.put_int(i128_val as u128);
    bb.end_row();
    let batch = bb.finish();

    assert_eq!(batch.count, 1);
    assert_eq!(batch.get_col_ptr(0, 0, 1), &[42]);
    assert_eq!(batch.get_col_ptr(0, 1, 1), &[(-7i8) as u8]);
    assert_eq!(batch.get_col_ptr(0, 2, 2), &1000u16.to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 3, 2), &(-500i16).to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 4, 4), &70000u32.to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 5, 4), &(-12345i32).to_le_bytes());
    let f32_val = f32::from_le_bytes(batch.get_col_ptr(0, 6, 4).try_into().unwrap());
    assert!((f32_val - 3.14f32).abs() < 1e-5, "f32: {f32_val}");
    assert_eq!(batch.get_col_ptr(0, 7, 8), &0x1234_5678_9ABC_DEF0u64.to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 8, 8), &(-99999i64).to_le_bytes());
    let f64_val = f64::from_le_bytes(batch.get_col_ptr(0, 9, 8).try_into().unwrap());
    assert!((f64_val - 2.718281828).abs() < 1e-9, "f64: {f64_val}");
    assert_eq!(crate::test_support::read_german_string(&batch, 10, 0), b"hello world!");
    let got = i128::from_le_bytes(batch.get_col_ptr(0, 11, 16).try_into().unwrap());
    assert_eq!(got, i128_val, "a 16-byte payload round-trips at its full width");
}
