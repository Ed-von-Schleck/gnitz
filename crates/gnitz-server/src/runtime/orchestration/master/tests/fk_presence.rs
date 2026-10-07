use super::*;
use crate::test_support::{make_batch_raw, u64_pk_schema};
use gnitz_wire::TypeCode;
use gnitz_zset::schema::SchemaColumn;

fn batch(rows: impl IntoIterator<Item = (u64, i64)>) -> Batch {
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::U64, false));
    let rows: Vec<_> = rows.into_iter().map(|(pk, weight)| (pk, weight, 0)).collect();
    make_batch_raw(&schema, &rows)
}

fn held(cache: &FkPresence, keys: impl IntoIterator<Item = u64>) -> Vec<u64> {
    keys.into_iter().filter(|&k| cache.holds(k as u128)).collect()
}

/// A repeat key costs no capacity, and the first distinct key past the cap
/// drops every key but itself.
#[test]
fn a_cache_past_its_cap_is_dropped_whole() {
    let mut cache = FkPresence::new(3);
    for k in [1, 2, 3, 2] {
        cache.record(k);
    }
    assert_eq!(held(&cache, 1..=4), [1, 2, 3]);
    cache.record(4);
    assert_eq!(held(&cache, 1..=4), [4]);
}

/// Per key the batch's last row decides: a delete after an insert evicts, an
/// insert after a delete records, and a zero weight is no row.
#[test]
fn a_small_write_leaves_each_key_as_its_last_row_does() {
    let mut cache = FkPresence::new(64);
    for k in [1, 2, 3] {
        cache.record(k);
    }
    cache.apply(&batch([(1, -1), (2, -1), (2, 1), (4, 1), (4, -1), (5, 1), (3, 0)]));
    assert_eq!(held(&cache, 1..=5), [2, 3, 5]);
}

/// A write past the fill bound records none of its inserts and still evicts
/// every key it retracts.
#[test]
fn a_bulk_write_evicts_and_records_nothing() {
    let mut cache = FkPresence::new(1 << 20);
    for k in [1, 2] {
        cache.record(k);
    }
    let fresh = 100..100 + FILL_MAX_ROWS as u64 + 1;
    cache.apply(&batch(fresh.clone().map(|k| (k, 1))));
    assert_eq!(held(&cache, fresh.clone()), [] as [u64; 0]);
    assert_eq!(held(&cache, 1..=2), [1, 2]);

    cache.apply(&batch(fresh.map(|k| (k, 1)).chain([(1, -1)])));
    assert_eq!(held(&cache, 1..=2), [2]);
}

/// A table has a cache from the first key a probe finds in it until it is
/// invalidated, and a durable batch reaches that table's cache alone.
#[test]
fn a_table_has_a_cache_from_its_first_found_key() {
    let (disp, _writers) = super::super::fixtures::test_dispatcher(vec![0], std::ptr::null_mut());
    let held = |keys: [u64; 3]| disp.fk_presence_of(9).map(|cache| held(&cache, keys));
    assert_eq!(held([5, 6, 7]), None);

    disp.fk_presence_found(9, 5);
    disp.fk_presence_found(9, 7);
    assert_eq!(held([5, 6, 7]), Some(vec![5, 7]));

    disp.fk_presence_ingest_batch(8, &batch([(7, -1)]));
    disp.fk_presence_ingest_batch(9, &batch([(5, -1)]));
    disp.fk_presence_invalidate_table(8);
    assert_eq!(held([5, 6, 7]), Some(vec![7]));
    disp.fk_presence_invalidate_table(9);
    assert_eq!(held([5, 6, 7]), None);
}
