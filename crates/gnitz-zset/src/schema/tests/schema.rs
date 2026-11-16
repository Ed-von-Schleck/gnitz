use super::*;

/// The PK columns in PK-list order, then each named payload column in the order
/// named, a repeat kept.
#[test]
fn project_schema_places_the_pk_then_the_named_payload() {
    use gnitz_wire::TypeCode::{I32, I64, U32, U64};
    let col = |tc| SchemaColumn::new(tc, false);
    let opt = SchemaColumn::new(I64, true);
    let input = SchemaDescriptor::new(&[col(I32), col(U64), opt, col(U32)], &[3, 1]);
    assert_eq!(
        project_schema(&input, &[2, 0, 2]).unwrap(),
        SchemaDescriptor::new(&[col(U32), col(U64), opt, col(I32), opt], &[0, 1]),
    );
}

/// The builder admits `MAX_COLUMNS` columns; `finish` refuses one more, never
/// truncating it.
#[test]
fn derived_schema_refuses_past_max_columns() {
    let k = SchemaColumn::new(gnitz_wire::TypeCode::U64, false);
    let mut b = DerivedSchema::new();
    b.push_pk(k);
    for _ in 1..MAX_COLUMNS {
        b.push(k);
    }
    assert_eq!(b.finish().unwrap(), SchemaDescriptor::new(&[k; MAX_COLUMNS], &[0]));
    b.push(k);
    assert_eq!(b.finish(), Err(SchemaRefusal::TooManyColumns(MAX_COLUMNS + 1)));
    assert_eq!(b.payload().len(), MAX_COLUMNS);
    b.push(k);
    assert_eq!(b.finish(), Err(SchemaRefusal::TooManyColumns(MAX_COLUMNS + 2)));
}

/// A schema record's arity prefix carries no redundancy of its own, so every
/// flip in it must either be refused or change the descriptor.
#[test]
fn no_flip_in_a_schema_records_arity_prefix_is_silently_inert() {
    use gnitz_wire::TypeCode::{F64, I64, U64};
    let cols = [
        SchemaColumn::new(U64, false),
        SchemaColumn::new(I64, false),
        SchemaColumn::new(I64, true),
        SchemaColumn::new(F64, true),
    ];
    let reference = SchemaDescriptor::new(&cols, &[0]);
    let mut buf = encode_schema_record(&reference);
    for byte in 0..4 + 1 + reference.pk_cols().len() {
        for bit in 0..8 {
            buf[byte] ^= 1 << bit;
            if let Ok(decoded) = decode_schema_record(&buf) {
                assert_ne!(decoded, reference, "prefix byte {byte} bit {bit} changed nothing");
            }
            buf[byte] ^= 1 << bit;
        }
    }
}

/// The decode of a schema record refuses what the descriptor refuses: a
/// nullable PK column, and a column count past `MAX_COLUMNS`.
#[test]
fn decode_schema_record_refuses_what_the_descriptor_refuses() {
    use gnitz_wire::TypeCode::U64;
    let cases = [
        ("nullable PK column", vec![SchemaColumn::new(U64, true)]),
        (
            "column count past MAX_COLUMNS",
            vec![SchemaColumn::new(U64, false); MAX_COLUMNS + 2],
        ),
    ];
    for (what, cols) in &cases {
        let record = gnitz_wire::schema_record::encode(
            cols.iter().map(|c| RecordCol {
                ty: ColType::of(c.type_code),
                nullable: c.nullable,
                hidden: false,
                name: b"k",
            }),
            &[0],
        );
        assert!(decode_schema_record(&record).is_err(), "{what}");
    }
}
