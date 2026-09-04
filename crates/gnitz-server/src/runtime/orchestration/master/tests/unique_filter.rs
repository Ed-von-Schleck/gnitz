use super::super::fixtures::{make_row_batch, test_dispatcher, two_col_schema};
use super::super::preflight::PreflightAccumulator;
use super::*;

/// The OPK leading-key span of a single U64 value — what `key_bytes` produces
/// for a U64-promoted index column.
fn span_u64(v: u64) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes())
}

/// Span-extraction spec for a unique index on `cols`, promoted exactly as
/// production circuit registration does.
fn test_spec(cols: &[u32], schema: &SchemaDescriptor) -> IndexKeySpec {
    IndexKeySpec::new(cols, schema).unwrap()
}

/// The filter's whole contract: it never proves a present span absent, and past
/// its cap it disables itself rather than truncating — a partial set would prove
/// "absent" for a present key.
#[test]
fn filter_proves_absence_only_until_it_caps() {
    let mut f = UniqueFilter::with_cap(8);
    for k in 1..=2u64 {
        f.insert(span_u64(k).pk_bytes());
    }
    assert!(!f.capped());
    assert!(f.may_contain(span_u64(1).pk_bytes()));
    assert!(
        !f.may_contain(span_u64(3).pk_bytes()),
        "a fresh span is provably absent"
    );

    for k in 3..=10u64 {
        f.insert(span_u64(k).pk_bytes());
    }
    assert!(f.capped(), "past the cap the filter disables itself");
    // The set is gone whole: nothing is provably absent any more, and further
    // inserts are no-ops.
    f.insert(span_u64(99).pk_bytes());
    assert!(f.may_contain(span_u64(12345).pk_bytes()));
}

/// `extract_into_filter`'s own three rules, over one batch: a retraction is not
/// an occupancy, a NULL in an indexed column is not a key (`key_bytes` → false),
/// and the walk stops at the cap.
#[test]
fn extract_into_filter_skips_retractions_and_nulls() {
    let schema = two_col_schema();
    // (pk, weight, payload): a live row, a NULL payload, a retraction.
    let batch = make_row_batch(schema, &[(1, 1, Some(100)), (2, 1, None), (3, -1, Some(300))]);

    // Indexed by the PK column: only the retraction is skipped.
    let mut by_pk = UniqueFilter::new();
    extract_into_filter(&mut by_pk, &batch.as_mem_batch(), &test_spec(&[0], &schema));
    assert!(by_pk.may_contain(span_u64(1).pk_bytes()));
    assert!(by_pk.may_contain(span_u64(2).pk_bytes()));
    assert!(
        !by_pk.may_contain(span_u64(3).pk_bytes()),
        "a retracted row is not an occupancy"
    );

    // Indexed by the nullable payload column: the NULL is skipped too.
    let mut by_payload = UniqueFilter::new();
    extract_into_filter(&mut by_payload, &batch.as_mem_batch(), &test_spec(&[1], &schema));
    assert!(by_payload.may_contain(span_u64(100).pk_bytes()));
    assert!(!by_payload.may_contain(span_u64(200).pk_bytes()));
    assert_eq!(by_payload.len(), 1, "the NULL and the retraction both dropped out");

    // Cap of zero: the first row overflows, so the walk caps and stops.
    let mut capped = UniqueFilter::with_cap(0);
    extract_into_filter(&mut capped, &batch.as_mem_batch(), &test_spec(&[0], &schema));
    assert!(capped.capped());
}

/// The CREATE-time seed chain. A seed that fits the cap is the complete distinct
/// set, so the published filter proves fresh keys absent; one that overflowed is
/// empty-because-capped, not empty-because-the-table-is-empty, and must never
/// prove anything absent — every query falls through to the broadcast.
#[test]
fn preflight_seed_proves_absence_only_when_it_fits_the_cap() {
    for (keys, want_capped) in [(&[10u64, 20, 30][..], false), (&[10, 20, 30, 40][..], true)] {
        let mut acc = PreflightAccumulator::new(3);
        for &k in keys {
            assert!(acc.offer(span_u64(k)), "distinct keys never flip the duplicate verdict");
        }
        let seed = acc.into_seed();
        assert_eq!(seed.capped(), want_capped);

        // Null catalog: the unique-filter methods touch only the filter map.
        let disp = test_dispatcher(Vec::new(), std::ptr::null_mut());
        disp.unique_filter_seed(7, 0, seed);
        assert!(
            disp.unique_filters.borrow()[&(7, 0)].warm,
            "a published seed is warm, so no warmup scan rebuilds over it"
        );
        assert_eq!(
            disp.unique_filter_all_absent(7, 0, [span_u64(40).pk_bytes()].into_iter()),
            !want_capped,
            "a capped seed must fall through to the broadcast",
        );
        assert!(
            !disp.unique_filter_all_absent(7, 0, [span_u64(20).pk_bytes()].into_iter()),
            "a seeded key falls through either way"
        );
    }
}
