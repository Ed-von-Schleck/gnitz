use super::*;
use crate::schema::SchemaColumn;
use crate::test_support::u64_pk_schema;
use gnitz_wire::TypeCode;

/// A builder of a `(pk U64 | tcs…)` batch, every payload column NOT NULL.
fn builder(tcs: &[TypeCode]) -> BatchBuilder {
    let cols: Vec<SchemaColumn> = tcs.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
    BatchBuilder::new(&u64_pk_schema(&cols))
}

/// Each value is written at its own column's width — two's complement for a
/// signed integer, DATE as its I32 day count, an F32 as the `as f32` narrowing.
#[test]
fn a_value_is_written_at_the_column_width() {
    let mut b = builder(&[TypeCode::I16, TypeCode::Date, TypeCode::U8, TypeCode::F32]);
    b.begin_row(1, 1)
        .put_int((-2i128) as u128)
        .put_int((-1i128) as u128)
        .put_int(255)
        .put_float(0.1);
    let batch = b.finish();
    assert_eq!(batch.col_data(0), (-2i16).to_le_bytes());
    assert_eq!(batch.col_data(1), (-1i32).to_le_bytes());
    assert_eq!(batch.col_data(2), [255]);
    assert_eq!(batch.col_data(3), 0.1f32.to_le_bytes());
}

/// A `put_cell` whose fill fails leaves its column unwritten and the row open
/// for the value that replaces it.
#[test]
fn a_failed_put_cell_leaves_the_row_open() {
    let mut b = builder(&[TypeCode::I64]);
    b.begin_row(1, 1);
    assert!(b.put_cell(|_, _| Err(())).is_err());
    b.put_int(5).end_row();
    assert_eq!(b.finish().col_data(0), 5i64.to_le_bytes());
}

/// A value wider than its column is refused at the write, not truncated into a
/// plausible cell.
#[test]
#[should_panic(expected = "0x100 is wider than the U8 column")]
fn put_int_refuses_a_value_wider_than_its_column() {
    builder(&[TypeCode::U8]).begin_row(1, 1).put_int(256);
}

/// An integer written into a German-string column panics before the region can go out
/// of shape.
#[test]
#[should_panic(expected = "an integer cannot be written to the String column")]
fn a_fixed_value_in_a_string_column_panics() {
    builder(&[TypeCode::String]).begin_row(1, 1).put_int(42);
}

#[test]
#[should_panic(expected = "a String value cannot be written to the U64 column")]
fn a_string_in_a_fixed_column_panics() {
    builder(&[TypeCode::U64]).begin_row(1, 1).put_string("oops");
}

#[test]
#[should_panic(expected = "a float cannot be written to the I64 column")]
fn a_float_in_an_integer_column_panics() {
    builder(&[TypeCode::I64]).begin_row(1, 1).put_float(1.5);
}

#[test]
#[should_panic(expected = "BatchBuilder: row got 1 of 2")]
fn closing_an_under_pushed_row_panics() {
    let mut b = builder(&[TypeCode::String, TypeCode::Blob]);
    b.begin_row(1, 1).put_string("only one");
    b.end_row();
}

#[test]
#[should_panic(expected = "BatchBuilder: row got 1 of 2")]
fn starting_a_row_after_an_under_pushed_one_panics() {
    let mut b = builder(&[TypeCode::I64, TypeCode::I64]);
    b.begin_row(1, 1).put_int(1);
    b.begin_row(2, 1);
}
