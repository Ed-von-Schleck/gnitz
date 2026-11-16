use super::*;
use gnitz_wire::{ColumnDef, TypeCode};

#[test]
fn from_parts_enforces_full_rule_set() {
    let cols = vec![
        ColumnDef::new("a", TypeCode::U64, false),    // 0: eligible, non-null
        ColumnDef::new("b", TypeCode::I32, false),    // 1: eligible, non-null
        ColumnDef::new("s", TypeCode::String, false), // 2: ineligible type
        ColumnDef::new("n", TypeCode::U64, true),     // 3: nullable
    ];

    let schema = |pk: &[u32], columns: &[ColumnDef]| Schema::from_parts(columns.to_vec(), pk);

    assert!(schema(&[0], &cols).is_ok());
    assert!(schema(&[0, 1], &cols).is_ok());

    // The rule set is `gnitz_wire::validate_pk_tuple`'s, tested there; these
    // pin what this caller hands it — its arity cap and each column's type and
    // nullability.
    for (pk, want) in [
        (&[0, 1, 0, 1, 0][..], "out of range 1..=4"),
        (&[3][..], "primary key column 'n' must not be nullable"),
        (
            &[2][..],
            "primary key column 's' has type_code STRING; only fixed-width integer",
        ),
    ] {
        let got = schema(pk, &cols).unwrap_err();
        assert!(got.contains(want), "pk {pk:?}: {got:?} does not mention {want:?}");
    }

    // Column-count cap: the null bitmap is one u64, so > MAX_COLUMNS rejects.
    let wide: Vec<ColumnDef> = (0..=gnitz_wire::MAX_COLUMNS)
        .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, i > 0))
        .collect();
    let got = schema(&[0], &wide).unwrap_err();
    assert!(got.contains("MAX_COLUMNS"), "{got}");
    assert!(schema(&[0], &wide[..gnitz_wire::MAX_COLUMNS]).is_ok());
}

/// Every fact a `Schema` carries must survive the block round-trip: column
/// types, nullability, names, the `hidden` marker, and the declared PK order
/// (which is not column order here).
#[test]
fn schema_survives_the_block_roundtrip() {
    let original = Schema::from_parts(
        vec![
            ColumnDef::new("id", TypeCode::U64, false).hidden(),
            ColumnDef::new("name", TypeCode::String, true),
            ColumnDef::new("score", TypeCode::F64, false),
            ColumnDef::new("tag", TypeCode::I32, true),
            ColumnDef::new("uuid", TypeCode::U128, false),
        ],
        &[4, 0],
    )
    .unwrap();
    assert_eq!(Schema::from_record(original.to_record()).unwrap(), original);
}

/// The record is cached: a rename re-encodes it under the new name, and
/// whether a schema's record is cached does not enter its equality.
#[test]
fn a_rename_re_encodes_the_cached_record() {
    let mut schema = Schema::from_parts(
        vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, true),
        ],
        &[0],
    )
    .unwrap();
    let fresh = schema.clone();
    let before = schema.to_record().to_vec();
    assert_eq!(schema, fresh, "a cached record does not enter equality");
    schema.rename_column(1, Arc::from("w"));
    let renamed = Schema::from_record(schema.to_record()).unwrap();
    assert_eq!(&*renamed.columns()[1].name, "w");
    assert_ne!(schema.to_record(), &before[..]);
    assert_eq!(Schema::from_record(fresh.to_record()).unwrap(), fresh);
}

/// The client's PK arity cap is `gnitz_wire::PK_LIST_MAX_COLS` — a key it accepts must
/// round-trip through the persisted PK-list word — so it must refuse a record
/// the shared codec, capped wider, admits.
#[test]
fn a_pk_wider_than_the_client_codec_is_rejected() {
    let n = gnitz_wire::PK_LIST_MAX_COLS + 1;
    let key = ColumnDef::new("k", TypeCode::U64, false);
    let pk: Vec<u32> = (0..n as u32).collect();
    let block = gnitz_wire::schema_record::encode((0..n).map(|_| key.record_col()), &pk);
    assert!(
        gnitz_wire::schema_record::decode(&block, |_| Ok(())).is_ok(),
        "the codec admits it"
    );
    assert!(Schema::from_record(&block).is_err());
}
