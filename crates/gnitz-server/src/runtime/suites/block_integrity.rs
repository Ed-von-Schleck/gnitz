//! The descriptive bytes a frame carries outside any checksum — a schema
//! record's arity prefix. A data block's WAL header is `gnitz_wire::wal`'s,
//! swept there.

use crate::test_support::sweep_bit_flips;
use gnitz_expr::ColumnTable;
use gnitz_store::schema::decode_schema_block;
use gnitz_store::schema::SchemaDescriptor;

/// A schema record for a 4-column schema, keyed by its first column.
fn schema_record_4col() -> Vec<u8> {
    use gnitz_store::schema::SchemaColumn;
    use gnitz_wire::TypeCode;
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::F64, true),
    ];
    let schema = SchemaDescriptor::new(&cols, &[0]);
    gnitz_store::schema::encode_schema_block(&schema)
}

/// The arity prefix carries no redundancy of its own, so every flip in it must
/// either be refused or change the descriptor.
#[test]
fn no_flip_in_a_schema_records_arity_prefix_is_silently_inert() {
    let mut buf = schema_record_4col();
    let reference = decode_schema_block(&buf).expect("clean");
    let prefix = 4 + 1 + reference.pk_cols().len();
    sweep_bit_flips(&mut buf, 0..prefix, |byte, bit, buf| {
        let Ok(decoded) = decode_schema_block(buf) else {
            return;
        };
        let same = decoded.num_columns() == reference.num_columns()
            && decoded.pk_cols() == reference.pk_cols()
            && (0..decoded.num_columns()).all(|c| {
                decoded.columns[c].type_code == reference.columns[c].type_code
                    && decoded.columns[c].nullable == reference.columns[c].nullable
            });
        assert!(!same, "schema prefix byte {byte} bit {bit} changed nothing observable");
    });
}
