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
