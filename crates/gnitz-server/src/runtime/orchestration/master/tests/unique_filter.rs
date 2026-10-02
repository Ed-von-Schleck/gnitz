use super::super::fixtures::test_dispatcher;
use super::*;
use crate::test_support::u64_pk_schema;
use gnitz_wire::TypeCode;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::SchemaColumn;

/// A filter proves absence until it caps: a repeat span costs no capacity, and
/// the first distinct span past the cap drops the whole set.
#[test]
fn filter_proves_absence_only_until_it_caps() {
    let mut f = UniqueFilter { cap: 3, ..UniqueFilter::new() };
    let span = |k: u64| k.to_be_bytes();
    for k in 1..=3 {
        assert!(f.insert(&span(k)));
    }
    assert!(f.insert(&span(2)), "a repeat span costs no capacity");
    assert!(f.may_contain(&span(1)));
    assert!(!f.may_contain(&span(4)));

    assert!(!f.insert(&span(4)), "the first distinct span past the cap");
    assert!(f.may_contain(&span(5)), "a capped filter proves nothing");
    assert!(!f.insert(&span(1)), "and records nothing more");
}

/// A retraction is not an occupancy, and a NULL in an indexed column is not a
/// key: only the live, non-NULL rows' spans enter the filter.
#[test]
fn extract_into_filter_takes_only_live_non_null_spans() {
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::U64, true));
    let mut bb = BatchBuilder::new(&schema);
    for (pk, weight, payload) in [(1, 1, Some(100)), (2, 1, None), (3, -1, Some(300))] {
        bb.begin_row(pk, weight);
        bb.put_opt_int(payload);
        bb.end_row();
    }
    let batch = bb.finish();

    for (col, present, absent) in [(0, &[1, 2][..], &[3][..]), (1, &[100], &[0, 300])] {
        let spec = KeySpec::new(&[col], &schema).unwrap();
        let mut f = UniqueFilter::new();
        extract_into_filter(&mut f, &batch.as_mem_batch(), &spec);
        let held = |v: u128| f.may_contain(spec.seek_prefix(&[v]).pk_bytes());
        for &v in present {
            assert!(held(v), "column {col}: {v} is present");
        }
        for &v in absent {
            assert!(!held(v), "column {col}: {v} is absent");
        }
    }
}

/// A published seed drops the spans it proves absent, only for its own
/// `(table, columns)`, until it caps or its index or its table drops it.
#[test]
fn a_seed_drops_absent_spans_under_its_own_index() {
    let (disp, _) = test_dispatcher(Vec::new(), std::ptr::null_mut());
    let (a, ab) = (PkColList::from_slice(&[0]), PkColList::from_slice(&[0, 1]));
    let mut seed = UniqueFilter::new();
    seed.insert(&1u64.to_be_bytes());
    disp.unique_filter_seed(7, a, seed);
    disp.unique_filter_seed(7, ab, UniqueFilter::new());
    disp.unique_filter_seed(8, a, UniqueFilter::new());
    // Of spans 1 and 5, the ones the filter keeps.
    let kept = |tid, cols| {
        let spans = [1u64.to_be_bytes(), 5u64.to_be_bytes()];
        let mut order = vec![0, 1];
        disp.unique_filter_retain_possible(tid, cols, &mut order, |i| &spans[i as usize]);
        order
    };

    assert_eq!(kept(7, a), [0], "the seeded span alone");
    disp.unique_filter_remove(7, a);
    assert_eq!(kept(7, a), [0, 1], "a removed filter proves nothing");
    assert_eq!(kept(7, ab), [], "the composite index keeps its own");
    disp.unique_filter_invalidate_table(7);
    assert_eq!(kept(7, ab), [0, 1]);
    assert_eq!(kept(8, a), [], "another table's filter survives");

    let mut capped = UniqueFilter { cap: 0, ..UniqueFilter::new() };
    assert!(!capped.insert(&9u64.to_be_bytes()));
    disp.unique_filter_seed(8, a, capped);
    assert_eq!(kept(8, a), [0, 1], "a capped filter proves nothing");
}
