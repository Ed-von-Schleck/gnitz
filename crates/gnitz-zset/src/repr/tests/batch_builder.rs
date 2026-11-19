use super::*;
use crate::schema::SchemaColumn;
use crate::test_support::u64_pk_schema;
use gnitz_wire::TypeCode;

/// A builder of a `(pk U64 | tcs…)` batch, every payload column NOT NULL.
fn builder(tcs: &[TypeCode]) -> BatchBuilder {
    let cols: Vec<SchemaColumn> = tcs.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
    BatchBuilder::new(&u64_pk_schema(&cols))
}

/// `put_int` writes each integer at its own column's width — two's complement
/// for a signed one, DATE as its I32 day count.
#[test]
fn put_int_writes_at_the_column_width() {
    let mut b = builder(&[TypeCode::I16, TypeCode::Date, TypeCode::U8]);
    b.begin_row(1, 1)
        .put_int((-2i128) as u128)
        .put_int((-1i128) as u128)
        .put_int(255);
    let batch = b.finish();
    assert_eq!(batch.col_data(0), (-2i16).to_le_bytes());
    assert_eq!(batch.col_data(1), (-1i32).to_le_bytes());
    assert_eq!(batch.col_data(2), [255]);
}

/// A value wider than its column is refused at the write, not truncated into a
/// plausible cell.
#[test]
#[should_panic(expected = "0x100 is wider than the U8 column")]
fn put_int_refuses_a_value_wider_than_its_column() {
    builder(&[TypeCode::U8]).begin_row(1, 1).put_int(256);
}

#[test]
#[should_panic(expected = "is wider than the U64 column")]
fn a_wide_value_in_a_narrow_column_panics() {
    builder(&[TypeCode::U64]).begin_row(1, 1).put_int(1 << 64);
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
