use super::*;
use gnitz_zset::schema::pk_keys;

/// A filter proves absence until it caps: a repeat span costs no capacity, and
/// the first distinct span past the cap drops the whole set.
#[test]
fn filter_proves_absence_only_until_it_caps() {
    let mut f = UniqueFilter::new();
    let span = |k: u64| k.to_be_bytes();
    for k in 1..=3 {
        assert!(f.insert(&span(k), 3));
    }
    assert!(f.insert(&span(2), 3), "a repeat span costs no capacity");
    assert!(f.may_contain(&span(1)));
    assert!(!f.may_contain(&span(4)));

    assert!(!f.insert(&span(4), 3), "the first distinct span past the cap");
    assert!(f.may_contain(&span(5)), "a capped filter proves nothing");
    assert!(!f.insert(&span(1), 3), "and records nothing more");
}

/// A published seed drops the spans it proves absent, only for its own
/// `(table, columns)`, until it caps or its index or its table drops it.
#[test]
fn a_seed_drops_absent_spans_under_its_own_index() {
    let checks = Checks::new();
    let (a, ab) = (PkColList::from_slice(&[0]), PkColList::from_slice(&[0, 1]));
    let mut seed = UniqueFilter::new();
    seed.insert(&1u64.to_be_bytes(), UNIQUE_FILTER_CAP);
    checks.unique_filter_seed(7, a, seed);
    checks.unique_filter_seed(7, ab, UniqueFilter::new());
    checks.unique_filter_seed(8, a, UniqueFilter::new());
    // Of spans 1 and 5, the ones the filter keeps.
    let kept = |tid, cols| {
        let spans = [1u64.to_be_bytes(), 5u64.to_be_bytes()];
        let mut keys = pk_keys(8, spans.iter().map(|s| &s[..]));
        checks.unique_filter_retain_possible(tid, cols, &mut keys);
        keys.iter()
            .map(|k| u64::from_be_bytes(k.try_into().unwrap()))
            .collect::<Vec<_>>()
    };

    assert_eq!(kept(7, a), [1], "the seeded span alone");
    checks.unique_filter_remove(7, a);
    assert_eq!(kept(7, a), [1, 5], "a removed filter proves nothing");
    assert_eq!(kept(7, ab), [], "the composite index keeps its own");
    checks.forget(7);
    assert_eq!(kept(7, ab), [1, 5]);
    assert_eq!(kept(8, a), [], "another table's filter survives");

    let mut capped = UniqueFilter::new();
    assert!(!capped.insert(&9u64.to_be_bytes(), 0));
    checks.unique_filter_seed(8, a, capped);
    assert_eq!(kept(8, a), [1, 5], "a capped filter proves nothing");
}

/// A write claims its spans where its filter proves one of them absent, and an
/// admitted claim enters that filter; a write whose index has no filter, whose
/// filter holds its every span, or whose filter capped claims nothing.
#[test]
fn a_claim_is_taken_where_a_span_is_absent_and_enters_its_filter() {
    let checks = Checks::new();
    let (a, b) = (PkColList::from_slice(&[0]), PkColList::from_slice(&[1]));
    let mut seed = UniqueFilter::new();
    seed.insert(&1u64.to_be_bytes(), UNIQUE_FILTER_CAP);
    checks.unique_filter_seed(7, a, seed);
    let mut capped = UniqueFilter::new();
    assert!(!capped.insert(&9u64.to_be_bytes(), 0));
    checks.unique_filter_seed(8, a, capped);
    let keys_of = |vals: &[u64]| {
        pk_keys(
            8,
            vals.iter()
                .map(|v| v.to_be_bytes())
                .collect::<Vec<_>>()
                .iter()
                .map(|s| &s[..]),
        )
    };

    assert!(
        !checks.unique_filter_retain_possible(7, b, &mut keys_of(&[1, 2])),
        "no filter"
    );
    assert!(
        !checks.unique_filter_retain_possible(7, a, &mut keys_of(&[1])),
        "every span held"
    );
    assert!(
        !checks.unique_filter_retain_possible(8, a, &mut keys_of(&[1, 2])),
        "a capped filter"
    );
    let mut possible = keys_of(&[1, 2]);
    assert!(checks.unique_filter_retain_possible(7, a, &mut possible), "2 is absent");
    assert_eq!(possible.as_bytes(), 1u64.to_be_bytes());
    checks.unique_filters_admit(&[SpanClaim {
        tid: 7,
        cols: a,
        key_size: 8,
        spans: [1u64.to_be_bytes(), 2u64.to_be_bytes()].concat(),
    }]);

    let filters = checks.unique_filters.borrow();
    let held = |v: u64| filters[&(7, a)].may_contain(&v.to_be_bytes());
    assert_eq!([held(1), held(2), held(3)], [true, true, false]);
}

/// An admitted claim that caps its filter marks it churned, so the next write
/// to the index rebuilds it from the live index.
#[test]
fn a_claim_that_caps_marks_its_filter_churned() {
    let checks = Checks { unique_filter_cap: 1, ..Checks::new() };
    let a = PkColList::from_slice(&[0]);
    let mut seed = UniqueFilter::new();
    assert!(seed.insert(&1u64.to_be_bytes(), 1));
    checks.unique_filter_seed(7, a, seed);
    assert!(matches!(checks.unique_filters.borrow()[&(7, a)], UniqueFilter::Live(_)));

    checks.unique_filters_admit(&[SpanClaim {
        tid: 7,
        cols: a,
        key_size: 8,
        spans: [2u64.to_be_bytes(), 3u64.to_be_bytes()].concat(),
    }]);

    let filters = checks.unique_filters.borrow();
    assert!(matches!(filters[&(7, a)], UniqueFilter::Churned));
    assert!(
        filters[&(7, a)].may_contain(&9u64.to_be_bytes()),
        "a capped filter proves nothing"
    );
}
