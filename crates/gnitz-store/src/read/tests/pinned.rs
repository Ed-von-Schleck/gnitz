use super::*;
use crate::relation::RelationKind;
use crate::test_support::{
    make_batch_raw, make_schema_u64_i64, opk_pk, payload0_i64, relation_fixture_with, spilling, TID,
};
use gnitz_wire::PkBuf;

/// A page of `limit` rows after `after`, in `make_schema_u64_i64`'s layout.
fn page(limit: u64, after: Option<&[u8]>) -> PageRequest {
    PageRequest {
        layout: make_schema_u64_i64().layout_digest(),
        limit,
        after: vec![after.map(PkBuf::from_bytes)],
    }
}

/// A page's keys, in page order, each row at weight 1 and `val = 0`.
fn keys(b: &Batch) -> Vec<u64> {
    (0..b.len())
        .map(|i| {
            assert_eq!((b.get_weight(i), payload0_i64(b, i)), (1, 0));
            gnitz_wire::widen_pk_be(b.get_pk_bytes(i)) as u64
        })
        .collect()
}

/// `(0..100)`, cancelled rows among them, pinned as snapshot 1.
fn pinned() -> crate::test_support::RelationFixture {
    let schema = make_schema_u64_i64();
    let all: Vec<_> = (0..100).map(|k| (k, 1, 0)).collect();
    // Every third key retracted in a later round, so a drain meets cancelled groups.
    let cancelled: Vec<_> = (0..100).step_by(3).map(|k| (k, -1, 0)).collect();
    let rounds = [make_batch_raw(&schema, &all), make_batch_raw(&schema, &cancelled)];
    let mut r = relation_fixture_with(spilling(), RelationKind::BaseTable, schema, &[], rounds);
    r.pin(1, |k| k == RelationKind::BaseTable).unwrap();
    r
}

#[test]
fn pages_walk_the_pinned_rows_in_key_order_each_full_but_the_last() {
    let r = pinned();
    let schema = make_schema_u64_i64();
    let live: Vec<u64> = (0..100).filter(|k| k % 3 != 0).collect();
    let mut walked = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let rows = r.pinned_page(1, TID, &page(7, after.as_deref()), Slot::SOLO).unwrap();
        walked.extend(keys(&rows));
        if rows.len() < 7 {
            break;
        }
        after = Some(rows.get_pk_bytes(rows.len() - 1).to_vec());
    }
    assert_eq!(walked, live);
    let last = opk_pk(&schema, &[99]);
    assert!(r
        .pinned_page(1, TID, &page(7, Some(&last)), Slot::SOLO)
        .unwrap()
        .is_empty());
}

#[test]
fn a_page_after_the_greatest_key_is_empty_and_a_short_key_or_another_layout_is_refused() {
    let r = pinned();
    assert!(r
        .pinned_page(1, TID, &page(7, Some(&[0xff; 8])), Slot::SOLO)
        .unwrap()
        .is_empty());
    assert!(r.pinned_page(1, TID, &page(7, Some(&[0; 3])), Slot::SOLO).is_err());
    let other_layout = PageRequest { layout: 0, ..page(7, None) };
    assert!(r.pinned_page(1, TID, &other_layout, Slot::SOLO).is_err());
}

#[test]
fn a_store_reads_its_share_after_its_own_cursor() {
    let r = pinned();
    let schema = make_schema_u64_i64();
    let at_10 = opk_pk(&schema, &[10]);
    let req = PageRequest {
        after: vec![None, Some(PkBuf::from_bytes(&at_10))],
        ..page(14, None)
    };
    assert_eq!(
        keys(&r.pinned_page(1, TID, &req, Slot::new(1, 2)).unwrap()),
        [11, 13, 14, 16, 17, 19, 20]
    );
    assert_eq!(
        keys(&r.pinned_page(1, TID, &req, Slot::new(0, 2)).unwrap()),
        [1, 2, 4, 5, 7, 8, 10]
    );
    assert!(r.pinned_page(1, TID, &req, Slot::new(0, 3)).is_err());
}
