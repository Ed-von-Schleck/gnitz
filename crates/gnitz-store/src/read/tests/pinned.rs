use super::*;
use crate::relation::{RelationKind, Snapshot};
use crate::test_support::{
    TID, make_batch, make_schema_u64_i64, opk_pk, payload0_i64, registered_fixture, spec, spilling,
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

/// `(0..100)`, cancelled rows among them, pinned by an origin at `slot` that
/// is gone by the time the snapshot is read.
fn pinned(slot: Slot) -> Snapshot {
    let schema = make_schema_u64_i64();
    let all: Vec<_> = (0..100).map(|k| (k, 1, 0)).collect();
    // Every third key retracted in a later round, so a drain meets cancelled groups.
    let cancelled: Vec<_> = (0..100).step_by(3).map(|k| (k, -1, 0)).collect();
    let rounds = [make_batch(&schema, &all), make_batch(&schema, &cancelled)];
    registered_fixture(
        spilling(),
        slot,
        spec(TID, RelationKind::BaseTable, schema),
        schema,
        &[],
        rounds,
    )
    .pin()
}

#[test]
fn pages_walk_the_pinned_rows_in_key_order_each_full_but_the_last() {
    let s = pinned(Slot::SOLO);
    let schema = make_schema_u64_i64();
    let live: Vec<u64> = (0..100).filter(|k| k % 3 != 0).collect();
    let mut walked = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let rows = s.page(TID, &page(7, after.as_deref())).unwrap();
        walked.extend(keys(&rows));
        if rows.len() < 7 {
            break;
        }
        after = Some(rows.get_pk_bytes(rows.len() - 1).to_vec());
    }
    assert_eq!(walked, live);
    let last = opk_pk(&schema, &[99]);
    assert!(s.page(TID, &page(7, Some(&last))).unwrap().is_empty());
}

#[test]
fn a_page_after_the_greatest_key_is_empty_and_one_the_pin_cannot_answer_is_refused() {
    let s = pinned(Slot::SOLO);
    assert!(s.page(TID, &page(7, Some(&[0xff; 8]))).unwrap().is_empty());
    let other_layout = PageRequest { layout: 0, ..page(7, None) };
    for (id, req, why) in [
        (TID, page(7, Some(&[0; 3])), "stride"),
        (TID, other_layout, "reply schema"),
        (TID + 1, page(7, None), "holds no relation"),
    ] {
        let err = s.page(id, &req).map(drop).unwrap_err();
        assert!(err.contains(why), "{err}");
    }
}

#[test]
fn a_store_reads_its_share_after_its_own_cursor() {
    let schema = make_schema_u64_i64();
    let at_10 = opk_pk(&schema, &[10]);
    let req = PageRequest {
        after: vec![None, Some(PkBuf::from_bytes(&at_10))],
        ..page(14, None)
    };
    let read = |slot| pinned(slot).page(TID, &req);
    assert_eq!(keys(&read(Slot::new(1, 2)).unwrap()), [11, 13, 14, 16, 17, 19, 20]);
    assert_eq!(keys(&read(Slot::new(0, 2)).unwrap()), [1, 2, 4, 5, 7, 8, 10]);
    assert!(read(Slot::new(0, 3)).is_err());
}
