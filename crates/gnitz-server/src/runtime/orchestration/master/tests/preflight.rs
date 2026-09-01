use super::*;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::SchemaColumn;
use gnitz_wire::type_code;

/// The probe row's null word is the schema's *nullable* slots, not "every
/// payload column": a bit under a NOT NULL column is exactly what the
/// null-blind `FixedIntNonnull` row comparator and `is_null` disagree about,
/// and the probe batch crosses the wire into the worker's generic decode
/// path, where nothing knows it is special.
#[test]
fn check_batch_marks_only_the_nullable_payload_columns() {
    let cols = vec![
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
        SchemaColumn::new(type_code::I64, 0),
    ];
    let schema = SchemaDescriptor::new(&cols, &[0]);

    let batch = build_check_batch(&schema, &[42u128], type_code::U64, None);
    assert_eq!(batch.len(), 1);

    let null_word = u64::from_le_bytes(batch.null_bmp_data()[0..8].try_into().unwrap());
    assert_eq!(null_word, 0b010, "only the nullable payload column may be marked null");
}

/// Base table `(label STRING, id U64 PRIMARY KEY)` — PK at column 1. The FK
/// parent fast-path passes a base-table schema whose lone PK may be declared at
/// any column position; resolving the leading key column from `columns[0]` (the
/// STRING) would encode the probe at the wrong type and width, and mangle the
/// existence check.
#[test]
fn check_batch_nonleading_pk_probe_matches_stored_opk() {
    let cols = vec![
        SchemaColumn::new(type_code::STRING, 0),
        SchemaColumn::new(type_code::U64, 0),
    ];
    let schema = SchemaDescriptor::new(&cols, &[1]);

    let batch = build_check_batch(&schema, &[42u128], type_code::U64, None);

    let mut expected = [0u8; 8];
    gnitz_wire::encode_pk_column(&42u64.to_le_bytes(), type_code::U64, &mut expected);
    assert_eq!(
        batch.get_pk_bytes(0),
        &expected[..],
        "probe key must equal the stored OPK PK for a non-leading PK column"
    );
}

// -- the per-span merge accounting -----------------------------------------

/// OPK leading-key span of a single U128 value (U128 OPK == big-endian).
fn span_u128(v: u128) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes())
}

fn offer_all(acc: &mut PreflightAccumulator, keys: &[PkBuf]) -> bool {
    for &k in keys {
        if !acc.offer(k) {
            return false;
        }
    }
    true
}

/// Adjacent equal spans — within one worker's run or as two workers' equal
/// heads, indistinguishable at this layer — flip the verdict; the verdict is
/// monotonic thereafter.
#[test]
fn accumulator_adjacent_equal_is_duplicate() {
    let mut acc = PreflightAccumulator::new(1000);
    assert!(offer_all(&mut acc, &[span_u128(1), span_u128(2), span_u128(3)]));
    assert!(!acc.offer(span_u128(3)), "equal to prev ⇒ duplicate");
    assert!(!acc.offer(span_u128(4)), "verdict is monotonic");
    assert!(acc.duplicate);
}

#[test]
fn accumulator_distinct_keys_no_duplicate() {
    let mut acc = PreflightAccumulator::new(1000);
    let keys: Vec<PkBuf> = [1u128, 2, 3, 100, u128::MAX].into_iter().map(span_u128).collect();
    assert!(offer_all(&mut acc, &keys));
    assert!(!acc.duplicate);
    let seed = acc.into_seed();
    assert!(!seed.capped(), "under-cap seed must not report capped");
    for k in &keys {
        assert!(seed.may_contain(k.pk_bytes()), "seed under cap holds every span");
    }
    assert!(!seed.may_contain(span_u128(999).pk_bytes()), "and nothing else");
}

/// A duplicate found after the cap has been crossed is still detected — the
/// verdict never depends on the seed.
#[test]
fn accumulator_duplicate_after_cap_crossing() {
    let mut acc = PreflightAccumulator::new(2);
    assert!(offer_all(
        &mut acc,
        &[span_u128(1), span_u128(2), span_u128(3), span_u128(4)]
    ));
    assert!(!acc.offer(span_u128(4)));
    assert!(acc.duplicate);
    assert!(acc.into_seed().capped());
}
