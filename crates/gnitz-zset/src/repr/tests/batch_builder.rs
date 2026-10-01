use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};

/// The schema `(type_code, nullable)` pairs describe, keyed on `pk`.
fn schema_of(cols: &[(TypeCode, bool)], pk: &[u32]) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = cols
        .iter()
        .map(|&(tc, nullable)| SchemaColumn::new(tc, nullable))
        .collect();
    SchemaDescriptor::new(&cols, pk)
}

/// Payload slots skip every PK column, so slot 1 is column 3. Every column has
/// its own width, so a slot mapped to the wrong column misplaces its cells.
#[test]
fn batch_builder_writes_payload_around_a_non_leading_compound_pk() {
    use TypeCode::*;
    let schema = schema_of(&[(U8, false), (U64, false), (U32, false), (I16, false)], &[1, 2]);
    let mut bb = BatchBuilder::new(&schema);
    for (k, a, b) in [(1u128, 0xAB_u128, -5i16), (2, 0xCD, 300)] {
        bb.begin_row_opk(&[k, k * 10], 1);
        bb.put_int(a);
        bb.put_int(b as u128);
        bb.end_row();
    }
    let batch = bb.finish();
    for (row, (a, b)) in [(0xABu8, -5i16), (0xCD, 300)].into_iter().enumerate() {
        assert_eq!(batch.get_col_ptr(row, 0, 1), &[a], "row {row} slot 0");
        assert_eq!(batch.get_col_ptr(row, 1, 2), &b.to_le_bytes(), "row {row} slot 1");
    }
}

/// `BatchBuilder` over two nullable STRING columns: inline cells, cells that
/// spill to the blob heap, the empty string, and a NULL whose cell must be
/// zeroed rather than left holding the previous row's bytes.
#[test]
fn batch_builder_writes_string_cells_and_nulls() {
    let schema = schema_of(
        &[
            (TypeCode::U64, false),
            (TypeCode::String, true),
            (TypeCode::String, true),
        ],
        &[0],
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

    let mut bb = BatchBuilder::new(&schema);
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

#[test]
fn batch_builder_writes_every_payload_type_at_its_own_width() {
    // U64 pk, then one of each remaining type at payload index 0..=11.
    let schema = schema_of(
        &[
            (TypeCode::U64, false),    // pk
            (TypeCode::U8, false),     // pi 0
            (TypeCode::I8, false),     // pi 1
            (TypeCode::U16, false),    // pi 2
            (TypeCode::I16, false),    // pi 3
            (TypeCode::U32, false),    // pi 4
            (TypeCode::I32, false),    // pi 5
            (TypeCode::F32, false),    // pi 6
            (TypeCode::U64, false),    // pi 7
            (TypeCode::I64, false),    // pi 8
            (TypeCode::F64, false),    // pi 9
            (TypeCode::String, false), // pi 10
            (TypeCode::I128, false),   // pi 11
        ],
        &[0],
    );

    // A negative 16-byte payload (a cross-sign `_join_pk` surfaced into a
    // payload slot), with bits in both halves.
    let i128_val: i128 = -0x0123_4567_89AB_CDEF_1122_3344_5566_7788;

    let mut bb = BatchBuilder::new(&schema);
    bb.begin_row(100, 1);
    for v in [42i128, -7, 1000, -500, 70000, -12345] {
        bb.put_int(v as u128);
    }
    bb.put_float(1.5);
    bb.put_int(0x1234_5678_9ABC_DEF0u128);
    bb.put_int(-99999i128 as u128);
    bb.put_float(-2.25);
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
    assert_eq!(batch.get_col_ptr(0, 6, 4), &1.5f32.to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 7, 8), &0x1234_5678_9ABC_DEF0u64.to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 8, 8), &(-99999i64).to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 9, 8), &(-2.25f64).to_le_bytes());
    assert_eq!(crate::test_support::read_german_string(&batch, 10, 0), b"hello world!");
    let got = i128::from_le_bytes(batch.get_col_ptr(0, 11, 16).try_into().unwrap());
    assert_eq!(got, i128_val, "a 16-byte payload round-trips at its full width");
}
