use super::*;
use crate::test_support::{encode_schema_record, sweep_bit_flips};
use gnitz_wire::KeyListRefusal;
use gnitz_wire::TypeCode::{F64, I32, I64, U32, U64};
use gnitz_wire::schema_record::{self, RecordCol};

fn col(tc: gnitz_wire::TypeCode) -> SchemaColumn {
    SchemaColumn::new(tc, false)
}

/// The PK columns in PK-list order, then the payload columns of the slots
/// `read` names, in slot order; a bit past the payload slots is refused.
#[test]
fn project_schema_places_the_pk_then_the_read_payload() {
    let opt = SchemaColumn::new(I64, true);
    // Payload slots: 0 = column 0 (I32), 1 = column 2 (I64 NULL).
    let input = SchemaDescriptor::new(&[col(I32), col(U64), opt, col(U32)], &[3, 1]);
    let project = |read| project_schema(&input, read);
    assert_eq!(
        project(0b11).unwrap(),
        SchemaDescriptor::new(&[col(U32), col(U64), col(I32), opt], &[0, 1])
    );
    assert_eq!(
        project(0b10).unwrap(),
        SchemaDescriptor::new(&[col(U32), col(U64), opt], &[0, 1])
    );
    assert_eq!(
        project(0).unwrap(),
        SchemaDescriptor::new(&[col(U32), col(U64)], &[0, 1])
    );
    assert!(project(0b100).is_err());
}

/// The builder admits `MAX_COLUMNS` columns; `finish` refuses one more, never
/// truncating it.
#[test]
fn derived_schema_refuses_past_max_columns() {
    let mut b = DerivedSchema::new();
    b.push_pk(col(U64));
    for _ in 1..MAX_COLUMNS {
        b.push(col(U64));
    }
    assert_eq!(
        b.finish().unwrap(),
        SchemaDescriptor::new(&[col(U64); MAX_COLUMNS], &[0])
    );
    b.push(col(U64));
    assert_eq!(b.finish(), Err(SchemaRefusal::TooManyColumns(MAX_COLUMNS + 1)));
}

#[test]
#[should_panic(expected = "behind a payload column")]
fn derived_schema_refuses_a_key_column_behind_the_payload() {
    let mut b = DerivedSchema::new();
    b.push(col(U64));
    b.push_pk(col(U64));
}

/// A schema record's arity prefix — the PK count, the PK indices and the column
/// count — carries no redundancy of its own, so every flip in it must either be
/// refused or change the descriptor.
#[test]
fn no_flip_in_a_schema_records_arity_prefix_is_silently_inert() {
    let cols = [
        col(U64),
        col(I64),
        SchemaColumn::new(I64, true),
        SchemaColumn::new(F64, true),
    ];
    let reference = SchemaDescriptor::new(&cols, &[0]);
    let mut buf = encode_schema_record(&reference);
    assert_eq!(decode_schema_record(&buf), Ok(reference));
    let prefix = 2 + reference.pk_cols().len() + 2;
    sweep_bit_flips(&mut buf, 0..prefix, |byte, bit, b| {
        if let Ok(decoded) = decode_schema_record(b) {
            assert_ne!(decoded, reference, "prefix byte {byte} bit {bit} changed nothing");
        }
    });
}

/// A record the wire decode admits but the descriptor refuses comes back as the
/// descriptor's own refusal.
#[test]
fn decode_schema_record_passes_on_the_descriptors_refusal() {
    let nullable_pk = RecordCol {
        ty: gnitz_wire::ColType::of(U64),
        nullable: true,
        hidden: false,
        name: "k",
    };
    let record = schema_record::encode([nullable_pk].into_iter(), &[0]);
    assert_eq!(
        decode_schema_record(&record),
        Err(SchemaRefusal::Pk(KeyListRefusal::Nullable { col: 0 }).to_string())
    );
}
