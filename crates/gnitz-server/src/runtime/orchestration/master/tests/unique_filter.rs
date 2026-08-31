use super::super::fixtures::{compound_pk_bytes, make_row_batch, two_col_schema, u64_schema};
use super::*;
use gnitz_store::schema::SchemaColumn;
use gnitz_wire::type_code;

/// OPK leading-key span of a single U64 value — the form `key_bytes`
/// produces for a U64-promoted index column (U64 OPK == big-endian). Used to
/// build expected filter/accumulator keys in these unit tests.
fn span_u64(v: u64) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes())
}

/// Span-extraction spec for a unique index on `cols` of `schema`, promoted
/// via `make_index_schema` exactly as production circuit registration does.
fn test_spec(cols: &[u32], schema: &SchemaDescriptor) -> IndexKeySpec {
    let idx_schema = gnitz_store::schema::make_index_schema(cols, schema).unwrap();
    IndexKeySpec::new(cols, schema, &idx_schema)
}

#[test]
fn filter_insert_basic() {
    let mut f = UniqueFilter::new();
    f.insert(span_u64(1).pk_bytes());
    f.insert(span_u64(2).pk_bytes());
    assert!(f.may_contain(span_u64(1).pk_bytes()));
    assert!(f.may_contain(span_u64(2).pk_bytes()));
    assert!(!f.capped());
    assert!(!f.may_contain(span_u64(3).pk_bytes()));
}

#[test]
fn filter_cap_drops_values() {
    // Exceed a small parameterized cap and verify the filter disables
    // itself: the set is dropped whole, and it then proves nothing absent.
    let mut f = UniqueFilter::with_cap(8);
    for k in 0..10u64 {
        f.insert(span_u64(k).pk_bytes());
        if f.capped() {
            break;
        }
    }
    assert!(f.capped(), "filter should cap after exceeding the limit");
    assert_eq!(f.len(), 0, "the set is dropped whole, never truncated");
    // Further inserts are no-ops, and every span reads as possibly present.
    f.insert(span_u64(99999999).pk_bytes());
    assert_eq!(f.len(), 0);
    assert!(f.may_contain(span_u64(12345).pk_bytes()));
}

#[test]
fn extract_into_filter_pk_col() {
    // Schema: PK-only U64. Test that the PK-column locator extracts PKs.
    let schema = u64_schema();
    let batch = make_row_batch(
        schema,
        &[
            (10, 1, 0, 0),
            (20, 1, 0, 0),
            (30, -1, 0, 0), // delete row — should be skipped
        ],
    );
    let mut filter = UniqueFilter::new();
    extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
    assert!(filter.may_contain(span_u64(10).pk_bytes()));
    assert!(filter.may_contain(span_u64(20).pk_bytes()));
    assert!(!filter.may_contain(span_u64(30).pk_bytes()), "negative weight skipped");
}

#[test]
fn extract_into_filter_signed_pk_col_uses_native_key() {
    // Single signed I64 PK indexed by itself. Its leading index key now
    // promotes to a *signed* I64 (order-preserving), so the extracted span is
    // the I64-OPK (sign-bit-flipped) of the NATIVE value — for a self-indexed
    // I64 PK that equals the source's at-rest OPK bytes, since the source type
    // already matches the index type. The extraction must build the span from
    // the *native* key (`pk_native_key`) re-encoded at the promoted index type
    // — feeding the OPK-widened `get_pk` value would double-flip and seek a
    // wrong key, hiding genuine duplicates.
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::I64, 0)], &[0]);
    let mut opk = [0u8; 8];
    gnitz_wire::encode_pk_column(&(-5i64).to_le_bytes(), type_code::I64, &mut opk);
    let keys = [PkBuf::from_bytes(&opk)];
    let batch = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| k.pk_bytes()), None);

    // The promoted leading key is I64, so the filter span is the I64-OPK of
    // the native value (= the at-rest OPK bytes here).
    let promoted_span = PkBuf::from_bytes(&opk);
    // The old U64-promotion image (BE of the two's-complement u64 bits, no
    // sign flip) is a DIFFERENT, non-order-preserving span the extractor must
    // NOT hold.
    let unsigned_span = PkBuf::from_bytes(&((-5i64) as u64).to_be_bytes());
    assert_ne!(
        promoted_span, unsigned_span,
        "signed I64 OPK (sign-flipped) differs from the unsigned image"
    );

    let mut filter = UniqueFilter::new();
    extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
    assert!(
        filter.may_contain(promoted_span.pk_bytes()),
        "filter holds the I64-promoted native span"
    );
    assert!(
        !filter.may_contain(unsigned_span.pk_bytes()),
        "must not hold the non-order-preserving unsigned image"
    );
}

#[test]
fn extract_into_filter_payload_col_skips_nulls() {
    // Schema: PK U64, payload U64 (nullable). Test extraction by col 1.
    let schema = two_col_schema();
    let batch = make_row_batch(
        schema,
        &[
            (1, 1, 0, 100), // payload=100, not null
            (2, 1, 1, 200), // null bit set → should be skipped
            (3, 1, 0, 300),
        ],
    );
    let mut filter = UniqueFilter::new();
    // Single payload column promoted to a U64 index column (8-byte span).
    extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[1], &schema));
    assert!(filter.may_contain(span_u64(100).pk_bytes()));
    assert!(!filter.may_contain(span_u64(200).pk_bytes()), "null values skipped");
    assert!(filter.may_contain(span_u64(300).pk_bytes()));
    assert_eq!(filter.len(), 2);
}

#[test]
fn extract_into_filter_stops_at_the_cap() {
    let schema = u64_schema();
    let batch = make_row_batch(schema, &[(10, 1, 0, 0), (20, 1, 0, 0)]);
    // Cap of zero: the first row overflows, so the walk caps and stops.
    let mut filter = UniqueFilter::with_cap(0);
    extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
    assert!(filter.capped());
    assert_eq!(filter.len(), 0, "a capped filter holds nothing");
}

/// Zero workers, null catalog, dummy SAL: the unique-filter methods
/// touch only the `unique_filters` map.
fn filter_dispatcher() -> MasterDispatcher {
    MasterDispatcher::new(
        0,
        Vec::new(),
        std::ptr::null_mut(),
        0,
        SalWriter::new(std::ptr::null_mut(), -1, 0, 0),
        Rc::new(W2mReceiver::new(Vec::new())),
        Vec::new(),
    )
}

/// The full CREATE-time seed chain on a pre-flight that overflowed the
/// cap: the accumulator's seed is empty-because-capped, not
/// empty-because-the-table-is-empty, so the published filter must never
/// prove a key absent (it must fall through to the broadcast).
#[test]
fn capped_preflight_seed_never_proves_absence() {
    let mut acc = PreflightAccumulator::new(3);
    for k in [10u64, 20, 30, 40] {
        assert!(acc.offer(span_u64(k)), "distinct keys never flip the verdict");
    }
    let seed = acc.into_seed();
    assert!(seed.capped(), "cap + 1 distinct keys must cap the seed");
    let disp = filter_dispatcher();
    disp.unique_filter_seed(7, 0, seed);
    assert!(
        !disp.unique_filter_all_absent(7, 0, [span_u64(10).pk_bytes()].into_iter()),
        "a capped pre-flight seed must fall through to the broadcast",
    );
}

/// Exactly-at-cap pre-flight: the seed is the complete distinct set, so
/// the published filter proves absence for fresh keys and reports seeded
/// keys as possibly present.
#[test]
fn at_cap_preflight_seed_proves_absence() {
    let mut acc = PreflightAccumulator::new(3);
    for k in [10u64, 20, 30] {
        assert!(acc.offer(span_u64(k)));
    }
    let seed = acc.into_seed();
    assert!(!seed.capped(), "exactly cap distinct keys must keep the full seed");
    let disp = filter_dispatcher();
    disp.unique_filter_seed(7, 0, seed);
    assert!(
        disp.unique_filter_all_absent(7, 0, [span_u64(40).pk_bytes()].into_iter()),
        "fresh key is provably absent"
    );
    assert!(
        !disp.unique_filter_all_absent(7, 0, [span_u64(20).pk_bytes()].into_iter()),
        "seeded key falls through"
    );
}

/// A capped seed publishes a warm+capped filter whose entry exists in
/// `unique_filters` — so `ensure_unique_filters_warm`'s
/// `contains_key` skip applies and no key is ever proven absent.
#[test]
fn unique_filter_seed_capped_publishes_warm_capped_entry() {
    let mut seed = UniqueFilter::with_cap(0);
    seed.insert(span_u64(1).pk_bytes());
    assert!(seed.capped());
    let disp = filter_dispatcher();
    disp.unique_filter_seed(7, 0, seed);
    {
        let filters = disp.unique_filters.borrow();
        let filter = filters.get(&(7, 0)).expect("entry must exist");
        assert!(filter.warm);
        assert!(filter.capped());
    }
    assert!(!disp.unique_filter_all_absent(7, 0, [span_u64(12345).pk_bytes()].into_iter()));
}

#[test]
fn extract_into_filter_compound_pk_extracts_single_column() {
    // (A U32, B U32) both PK, unique index on A. Two rows share A=5 but
    // differ in B, so the extraction must slice out A alone — the packed
    // (A,B) key would make them two distinct filter entries.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
        ],
        &[0, 1],
    );
    assert_eq!(schema.pk_stride(), 8);
    // PK region is OPK; for unsigned U32 that is big-endian. extract_into_filter
    // decodes OPK→native via pk_native_key, so the fixture must be OPK.
    let keys = [
        compound_pk_bytes(&[&5u32.to_be_bytes(), &1u32.to_be_bytes()]),
        compound_pk_bytes(&[&5u32.to_be_bytes(), &2u32.to_be_bytes()]),
    ];
    let batch = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| k.pk_bytes()), None);
    let mut filter = UniqueFilter::new();
    // Index on a U32 column promotes to a U64 (8-byte) index column.
    extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
    assert!(
        filter.may_contain(span_u64(5).pk_bytes()),
        "filter holds column A's value"
    );
    assert_eq!(filter.len(), 1, "the shared A=5 collapses to one entry");
}

#[test]
fn extract_into_filter_compound_pk_second_column_offset() {
    // Unique index on B (the second PK column at byte offset 4). Confirms
    // the locator slices the right column out of the packed key.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
        ],
        &[0, 1],
    );
    // OPK PK region: unsigned U32 columns are stored big-endian.
    let keys = [
        compound_pk_bytes(&[&5u32.to_be_bytes(), &11u32.to_be_bytes()]),
        compound_pk_bytes(&[&6u32.to_be_bytes(), &22u32.to_be_bytes()]),
    ];
    let batch = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| k.pk_bytes()), None);
    let mut filter = UniqueFilter::new();
    extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[1], &schema));
    assert!(filter.may_contain(span_u64(11).pk_bytes()));
    assert!(filter.may_contain(span_u64(22).pk_bytes()));
    assert_eq!(filter.len(), 2);
}

/// A composite index whose span exceeds 16 bytes takes `probe_key`'s xxh3
/// branch — the width no `u128` image could hold. Two spans sharing a
/// 16-byte prefix must stay distinct entries.
#[test]
fn extract_into_filter_wide_composite_span() {
    // (U128, U64) both PK, indexed together: a 24-byte span.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0, 1],
    );
    assert_eq!(schema.pk_stride(), 24);
    // Only the trailing column differs, so the two spans share 16 bytes.
    let keys = [
        compound_pk_bytes(&[&7u128.to_be_bytes(), &3u64.to_be_bytes()]),
        compound_pk_bytes(&[&7u128.to_be_bytes(), &4u64.to_be_bytes()]),
    ];
    assert_eq!(keys[0].pk_bytes()[..16], keys[1].pk_bytes()[..16]);
    let batch = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| k.pk_bytes()), None);
    let mut filter = UniqueFilter::new();
    extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0, 1], &schema));
    assert!(filter.may_contain(keys[0].pk_bytes()));
    assert!(filter.may_contain(keys[1].pk_bytes()));
    assert_eq!(filter.len(), 2, "a shared prefix must not merge two spans");
}
