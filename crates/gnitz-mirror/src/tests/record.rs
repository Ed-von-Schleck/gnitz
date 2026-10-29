//! The record codec: what a round trip preserves, and what bytes it refuses.

use gnitz_zset::schema::encode_schema_block;
use gnitz_zset_testkit::make_schema_u64_i64;

use super::*;

fn record(feed: Feed) -> MirrorRecord {
    MirrorRecord {
        name: RelName::new("public", "recent").unwrap(),
        block: encode_schema_block(&make_schema_u64_i64()),
        pk_repeats: false,
        indexes: vec![PkColList::from_slice(&[1]), PkColList::from_slice(&[1, 0])],
        feed,
    }
}

fn one_cursor() -> Feed {
    Feed::At(DeltaCursor::from_pair(0xFEED, 41).unwrap())
}

/// A registration round-trips with its cursor and decodes to the layout its
/// block describes; one with no feed position comes back with none, as does
/// one being refilled.
#[test]
fn a_record_round_trips() {
    for feed in [one_cursor(), Feed::Empty] {
        let rec = record(feed);
        assert_eq!(MirrorRecord::decode(&rec.encode()), Some((rec, make_schema_u64_i64())));
    }
    let filling = MirrorRecord::decode(&record(Feed::Filling).encode()).map(|(rec, _)| rec.feed);
    assert_eq!(filling, Some(Feed::Empty), "a refill in progress persists as no value");
}

#[test]
fn bytes_that_are_no_record_are_refused() {
    let good = record(one_cursor()).encode();
    let mut refused: Vec<(String, Vec<u8>)> = (0..good.len())
        .map(|cut| (format!("cut at {cut}"), good[..cut].to_vec()))
        .collect();
    let mut overlong = good.clone();
    overlong.push(0);
    refused.push(("a trailing byte".into(), overlong));
    let mut name = good.clone();
    let at = name.windows(6).position(|w| w == b"recent").unwrap();
    name[at] = 0xFF;
    refused.push(("a name that is not UTF-8".into(), name));
    let no_layout = MirrorRecord {
        block: vec![0xAB; 20],
        ..record(one_cursor())
    };
    refused.push(("a block describing no layout".into(), no_layout.encode()));
    let descending = MirrorRecord {
        indexes: vec![PkColList::from_slice(&[1, 0]), PkColList::from_slice(&[1])],
        ..record(one_cursor())
    };
    refused.push(("index lists out of order".into(), descending.encode()));
    let twice = MirrorRecord {
        indexes: vec![PkColList::from_slice(&[1]); 2],
        ..record(one_cursor())
    };
    refused.push(("an index listed twice".into(), twice.encode()));

    for (what, bytes) in refused {
        assert!(MirrorRecord::decode(&bytes).is_none(), "{what}");
    }
}
