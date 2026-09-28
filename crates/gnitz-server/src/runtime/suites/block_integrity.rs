//! The descriptive bytes a frame carries outside any checksum — a data block's
//! WAL header and a schema record's arity prefix.

use crate::test_support::{encode_to_wire_vec, make_batch, make_schema_u64_i64, sweep_bit_flips};
use gnitz_store::schema::decode_schema_block;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;
use gnitz_wire::wal::WAL_HEADER_SIZE;

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

// ---------------------------------------------------------------------------
// The header's forgeable fields, through the real consumers
// ---------------------------------------------------------------------------

/// The arity prefix carries no redundancy of its own, so every flip in it must
/// either be refused or change the descriptor.
#[test]
fn no_flip_in_a_schema_records_arity_prefix_is_silently_inert() {
    let mut buf = schema_record_4col();
    let reference = decode_schema_block(&buf).expect("clean");
    let prefix = 4 + 1 + reference.pk_indices().len();
    sweep_bit_flips(&mut buf, 0..prefix, |byte, bit, buf| {
        let Ok(decoded) = decode_schema_block(buf) else {
            return;
        };
        let same = decoded.num_columns() == reference.num_columns()
            && decoded.pk_indices() == reference.pk_indices()
            && (0..decoded.num_columns()).all(|c| {
                decoded.columns[c].type_code == reference.columns[c].type_code
                    && decoded.columns[c].nullable == reference.columns[c].nullable
            });
        assert!(!same, "schema prefix byte {byte} bit {bit} changed nothing observable");
    });
}

/// Every single-bit flip in a data block's header is rejected by the parser
/// the SAL replay path uses.
#[test]
fn single_bit_header_sweep_rejects_every_flip() {
    let schema = make_schema_u64_i64();
    let clean_data = encode_to_wire_vec(&make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]));
    Batch::decode_from_wal_block(&clean_data, &schema).expect("clean");

    let mut buf = clean_data.clone();
    sweep_bit_flips(&mut buf, 0..WAL_HEADER_SIZE, |byte, bit, buf| {
        assert!(
            Batch::decode_from_wal_block(buf, &schema).is_err(),
            "byte {byte} bit {bit} was accepted"
        );
    });
}
