use super::*;
use gnitz_expr::ColumnTable;

fn view() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[3, 1],
    )
}

#[test]
fn the_delta_schema_stamps_the_view_key_and_keeps_its_payload_space() {
    let view = view();
    let delta = make_delta_schema(&view).expect("the stamped shape fits");
    let types = |s: &SchemaDescriptor| -> Vec<(TypeCode, bool)> {
        s.columns[..s.num_columns()]
            .iter()
            .map(|c| (c.type_code, c.nullable))
            .collect()
    };
    assert_eq!(
        types(&delta),
        [
            (TypeCode::I64, true),
            (TypeCode::U64, false),
            (TypeCode::String, false),
            (TypeCode::I32, false),
            (TypeCode::U64, false),
        ],
        "the view's columns under their own numbers, then the stamp",
    );
    assert_eq!(delta.pk_cols(), [4, 3, 1]);
    assert_eq!(delta.pk_stride(), 8 + view.pk_stride());
    let payload = |s: &SchemaDescriptor| -> Vec<TypeCode> { s.payload_columns().map(|(_, c)| c.type_code).collect() };
    assert_eq!(payload(&delta), payload(&view));
}

#[test]
fn a_view_with_no_key_column_to_spare_has_no_delta_schema() {
    let cols = vec![SchemaColumn::new(TypeCode::U64, false); gnitz_wire::MAX_PK_COLUMNS];
    let pk: Vec<u32> = (0..cols.len() as u32).collect();
    assert!(make_delta_schema(&SchemaDescriptor::new(&cols, &pk)).is_none());
}

#[test]
fn a_delta_key_round_trips_its_round() {
    let key = [&delta_round_prefix(7)[..], &[0xAB_u8; 12]].concat();
    assert_eq!(delta_round(&key), 7);
    // Rounds order as their prefixes do.
    assert!(delta_round_prefix(255) < delta_round_prefix(256));
}
