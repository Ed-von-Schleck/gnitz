use super::super::fixtures::test_dispatcher;
use super::*;
use crate::test_support::u64_pk_schema;
use gnitz_store::schema::SchemaColumn;
use gnitz_store::storage::BatchBuilder;
use gnitz_wire::TypeCode;

/// The filter never proves a present span absent. The cap is tested before the
/// insert, so a repeat span still fits at exactly `cap`; the first distinct span
/// past it drops the whole set rather than truncating it, since a partial set
/// would prove a present span absent.
#[test]
fn filter_proves_absence_only_until_it_caps() {
    let mut f = UniqueFilter { cap: 3, ..UniqueFilter::new() };
    let span = |k: u64| k.to_be_bytes();
    for k in 1..=3 {
        assert!(f.insert(&span(k)));
    }
    assert!(f.insert(&span(2)), "a repeat span costs no capacity");
    assert!(
        !f.proves_all_absent([&span(9)[..]].into_iter()),
        "a cold filter proves nothing"
    );
    f.mark_warm();
    assert!(!f.proves_all_absent([&span(9)[..], &span(1)[..]].into_iter()));
    assert!(f.proves_all_absent([&span(4)[..], &span(5)[..]].into_iter()));

    assert!(!f.insert(&span(4)), "the first distinct span past the cap");
    assert!(
        !f.proves_all_absent([&span(5)[..]].into_iter()),
        "a capped filter proves nothing"
    );
    assert!(!f.insert(&span(1)), "and records nothing more");
}

/// A retraction is not an occupancy, and a NULL in an indexed column is not a
/// key: only the live, non-NULL rows' spans enter the filter.
#[test]
fn extract_into_filter_takes_only_live_non_null_spans() {
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::U64, true));
    let mut bb = BatchBuilder::new(schema);
    for (pk, weight, payload) in [(1, 1, Some(100)), (2, 1, None), (3, -1, Some(300))] {
        bb.begin_row(pk, weight);
        match payload {
            Some(v) => bb.put_int(v),
            None => bb.put_null(),
        }
        bb.end_row();
    }
    let batch = bb.finish();

    for (col, present, absent) in [(0, &[1, 2][..], &[3][..]), (1, &[100], &[0, 300])] {
        let spec = KeySpec::new(&[col], &schema).unwrap();
        let mut f = UniqueFilter::new();
        extract_into_filter(&mut f, &batch.as_mem_batch(), &spec);
        f.mark_warm();
        let proves_absent = |v: u128| f.proves_all_absent([spec.seek_prefix(&[v]).pk_bytes()].into_iter());
        for &v in present {
            assert!(!proves_absent(v), "column {col}: {v} is present");
        }
        for &v in absent {
            assert!(proves_absent(v), "column {col}: {v} is absent");
        }
    }
}

/// A published seed proves absence at once, with no warm-up scan, and only for
/// its own `(table, columns)`, until its index or its table drops it.
#[test]
fn a_seed_is_published_warm_under_its_own_index() {
    let (disp, _) = test_dispatcher(Vec::new(), std::ptr::null_mut());
    let (a, ab) = (PkColList::from_slice(&[0]), PkColList::from_slice(&[0, 1]));
    let mut seed = UniqueFilter::new();
    seed.insert(&1u64.to_be_bytes());
    disp.unique_filter_seed(7, a, seed);
    disp.unique_filter_seed(7, ab, UniqueFilter::new());
    disp.unique_filter_seed(8, a, UniqueFilter::new());
    let absent = |tid, cols, k: u64| disp.unique_filter_all_absent(tid, cols, [&k.to_be_bytes()[..]].into_iter());

    assert!(absent(7, a, 5));
    assert!(!absent(7, a, 1), "a seeded span");
    disp.unique_filter_remove(7, a);
    assert!(!absent(7, a, 5), "a removed filter proves nothing");
    assert!(absent(7, ab, 5), "the composite index keeps its own");
    disp.unique_filter_invalidate_table(7);
    assert!(!absent(7, ab, 5));
    assert!(absent(8, a, 5), "another table's filter survives");
}
