use super::*;
use crate::test_support::{make_batch, u64_pk_schema};
use gnitz_wire::TypeCode;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaColumn;

fn batch(rows: impl IntoIterator<Item = (u64, i64)>) -> Batch {
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::U64, false)]);
    let rows: Vec<_> = rows.into_iter().map(|(pk, weight)| (pk, weight, 0)).collect();
    make_batch(&schema, &rows)
}

/// `checks` with an FK presence cache for table 9 holding `keys`.
fn with_presence(checks: Checks, keys: impl IntoIterator<Item = u128>) -> Checks {
    checks.fk_presence.borrow_mut().insert(9, keys.into_iter().collect());
    checks
}

/// Of `keys`, the ones `tid`'s presence cache holds; `None` where it has none.
fn held(checks: &Checks, tid: u64, keys: impl IntoIterator<Item = u64>) -> Option<Vec<u64>> {
    let caches = checks.fk_presence.borrow();
    let cache = caches.get(&tid)?;
    Some(keys.into_iter().filter(|&k| cache.contains(&(k as u128))).collect())
}

/// A repeat key costs no capacity, and the first distinct key past the cap
/// drops every key but itself.
#[test]
fn a_cache_past_its_cap_is_dropped_whole() {
    let mut cache = FxHashSet::default();
    for k in [1, 2, 3, 2] {
        record_present(&mut cache, 3, k);
    }
    let held = |cache: &FxHashSet<u128>| (1..=4).filter(|k| cache.contains(k)).collect::<Vec<_>>();
    assert_eq!(held(&cache), [1, 2, 3]);
    record_present(&mut cache, 3, 4);
    assert_eq!(held(&cache), [4]);
}

/// Per key the batch's last row decides: a delete after an insert evicts, an
/// insert after a delete records, and a zero weight is no row.
#[test]
fn a_small_write_leaves_each_key_as_its_last_row_does() {
    let checks = with_presence(Checks::new(), [1, 2, 3]);
    checks.committed(
        9,
        batch([(1, -1), (2, -1), (2, 1), (4, 1), (4, -1), (5, 1), (3, 0)]).as_mem_batch(),
    );
    assert_eq!(held(&checks, 9, 1..=5), Some(vec![2, 3, 5]));
}

/// A write past the fill bound records none of its inserts and still evicts
/// every key it retracts.
#[test]
fn a_bulk_write_evicts_and_records_nothing() {
    let checks = with_presence(
        Checks {
            fk_presence_cap: 1 << 20,
            ..Checks::new()
        },
        [1, 2],
    );
    let fresh = 100..100 + FK_PRESENCE_FILL_MAX_ROWS as u64 + 1;
    checks.committed(9, batch(fresh.clone().map(|k| (k, 1))).as_mem_batch());
    assert_eq!(held(&checks, 9, fresh.clone()), Some(vec![]));
    assert_eq!(held(&checks, 9, 1..=2), Some(vec![1, 2]));

    checks.committed(9, batch(fresh.map(|k| (k, 1)).chain([(1, -1)])).as_mem_batch());
    assert_eq!(held(&checks, 9, 1..=2), Some(vec![2]));
}

/// A committed batch reaches its own table's cache alone, and gives a table
/// with none no cache; forgetting a table drops its cache.
#[test]
fn a_committed_batch_reaches_its_own_tables_cache_alone() {
    let checks = with_presence(Checks::new(), [5, 7]);
    checks.committed(8, batch([(7, -1), (6, 1)]).as_mem_batch());
    assert_eq!(held(&checks, 8, [6]), None);
    checks.committed(9, batch([(5, -1)]).as_mem_batch());
    checks.forget(8);
    assert_eq!(held(&checks, 9, [5, 6, 7]), Some(vec![7]));
    checks.forget(9);
    assert_eq!(held(&checks, 9, [5, 6, 7]), None);
}

/// A filter proves absence until it caps: a repeat span costs no capacity, and
/// the first distinct span past the cap drops the whole set.
#[test]
fn filter_proves_absence_only_until_it_caps() {
    let mut f = UniqueFilter::default();
    assert!(f.insert_all([1, 2, 3, 2], 3));
    assert!(f.may_contain(1));
    assert!(!f.may_contain(4));

    assert!(!f.insert_all([4], 3), "the first distinct span past the cap");
    assert!(f.may_contain(5), "a capped filter proves nothing");
    assert!(!f.insert_all([1], 3), "and records nothing more");
}

/// A filter is kept per `(table, columns)`, until its index or its table
/// drops it.
#[test]
fn a_filter_is_dropped_with_its_index_or_its_table() {
    let checks = Checks::new();
    let (a, ab) = (PkColList::from_slice(&[0]), PkColList::from_slice(&[0, 1]));
    let seed = |tid, cols, fp| {
        let mut f = UniqueFilter::default();
        f.insert_all([fp], usize::MAX);
        checks.unique_filters.borrow_mut().insert((tid, cols), f);
    };
    seed(7, a, 1);
    seed(7, ab, 2);
    seed(8, a, 3);
    // Which of fingerprints 1, 2 and 3 the filter for `(tid, cols)` holds.
    let held = |tid, cols| {
        let filters = checks.unique_filters.borrow();
        let f = filters.get(&(tid, cols))?;
        Some((1..=3).filter(|&k| f.may_contain(k)).collect::<Vec<_>>())
    };

    assert_eq!(held(7, a), Some(vec![1]));
    checks.unique_filter_remove(7, a);
    assert_eq!(held(7, a), None);
    assert_eq!(held(7, ab), Some(vec![2]), "the composite index keeps its own");
    checks.forget(7);
    assert_eq!(held(7, ab), None);
    assert_eq!(held(8, a), Some(vec![3]), "another table's filter survives");
}
