use super::super::fixtures::compound_pk_bytes;
use super::*;
use gnitz_engine::schema::SchemaColumn;
use gnitz_wire::type_code;

#[test]
fn check_batch_64_payload_cols_full_null_word() {
    // 65 columns: 1 U64 PK + 64 nullable payload cols → npc == 64, the
    // widest null word a probe row can carry.
    let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
    for _ in 0..64 {
        cols.push(SchemaColumn::new(type_code::I64, 1));
    }
    let schema = SchemaDescriptor::new(&cols, &[0]);
    assert_eq!(schema.num_payload_cols(), 64);

    let keys = vec![42u128];
    let batch = build_check_batch(&schema, &keys, type_code::U64, None);
    assert_eq!(batch.count, 1);

    let null_word = u64::from_le_bytes(batch.null_bmp_data()[0..8].try_into().unwrap());
    assert_eq!(null_word, u64::MAX, "all 64 payload columns must be marked null");
}

/// The probe row's null word is the schema's *nullable* slots, not "every
/// payload column": a bit under a NOT NULL column is exactly what the
/// null-blind `FixedIntNonnull` row comparator and `is_null` disagree about,
/// and the probe batch crosses the wire into the worker's generic decode
/// path, where nothing knows it is special.
#[test]
fn check_batch_leaves_not_null_columns_unmarked() {
    let cols = vec![
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
        SchemaColumn::new(type_code::I64, 0),
    ];
    let schema = SchemaDescriptor::new(&cols, &[0]);

    let batch = build_check_batch(&schema, &[42u128], type_code::U64, None);
    assert_eq!(batch.count, 1);

    let null_word = u64::from_le_bytes(batch.null_bmp_data()[0..8].try_into().unwrap());
    assert_eq!(null_word, 0b010, "only the nullable payload column may be marked null");
}

#[test]
fn check_batch_nonleading_pk_probe_matches_stored_opk() {
    // Base table `(label STRING, id U64 PRIMARY KEY)` — PK at column 1.
    // The FK parent fast-path passes a base-table schema whose lone PK may
    // be declared at any column position; resolving the leading key column
    // from `columns[0]` (the STRING) would encode the probe at the wrong
    // type/width and mangle the existence check.
    let cols = vec![
        SchemaColumn::new(type_code::STRING, 0),
        SchemaColumn::new(type_code::U64, 0),
    ];
    let schema = SchemaDescriptor::new(&cols, &[1]);

    let keys = vec![42u128];
    let batch = build_check_batch(&schema, &keys, type_code::U64, None);

    // The parent stores id=42 as `encode_pk_column(42, U64)` = 42u64.to_be_bytes().
    let mut expected = [0u8; 8];
    gnitz_wire::encode_pk_column(&42u64.to_le_bytes(), type_code::U64, &mut expected);
    assert_eq!(
        batch.get_pk_bytes(0),
        &expected[..],
        "probe key must equal the stored OPK PK for a non-leading PK column"
    );
}

#[test]
fn format_pk_value_uuid_full_128_bits() {
    // A UUID with non-zero high 64 bits. The lower 64 bits alone would be
    // misread as a different (truncated) value.
    let uuid: u128 = 0x550e8400_e29b_41d4_a716_446655440000u128;
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::UUID, 0)], &[0]);

    let s = format_pk_value_bytes(&uuid.to_be_bytes(), &schema);
    // Must not be the lower-64 truncation (11975073520896 or similar).
    let truncated = format!("{}", uuid as u64);
    assert_ne!(s, truncated, "UUID must not be formatted as truncated u64");
    // Must contain the high-word hex digits.
    assert!(s.contains("550e8400"), "UUID formatting must include high bits");
}

#[test]
fn format_pk_value_uuid_two_distinct_uuids_differ() {
    // Two UUIDs that differ only in the high 64 bits must produce different strings.
    let uuid_a: u128 = 0x11111111_0000_0000_0000_000000000001u128;
    let uuid_b: u128 = 0x22222222_0000_0000_0000_000000000001u128;
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::UUID, 0)], &[0]);

    let sa = format_pk_value_bytes(&uuid_a.to_be_bytes(), &schema);
    let sb = format_pk_value_bytes(&uuid_b.to_be_bytes(), &schema);
    assert_ne!(sa, sb, "UUIDs differing in high bits must format differently");
}

#[test]
fn format_pk_value_bytes_wide_compound_u64x3() {
    // Three U64 columns = 24-byte PK: too wide for a u128, exercising the
    // byte-form renderer where a u128 key would truncate.
    // OPK for unsigned U64 is big-endian.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0, 1, 2],
    );
    assert!(schema.pk_stride() > 16);
    let pk = compound_pk_bytes(&[&7u64.to_be_bytes(), &8u64.to_be_bytes(), &9u64.to_be_bytes()]);
    assert_eq!(format_pk_value_bytes(pk.pk_bytes(), &schema), "7, 8, 9");
}
