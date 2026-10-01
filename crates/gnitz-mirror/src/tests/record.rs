//! The record codec: what a round trip preserves, and what bytes it refuses.

use gnitz_zset::schema::encode_schema_block;
use gnitz_zset_testkit::make_schema_u64_i64;

use super::*;

fn record(cursor: Option<DeltaCursor>) -> MirrorRecord {
    MirrorRecord {
        schema_name: "public".to_string(),
        name: "recent".to_string(),
        block: encode_schema_block(&make_schema_u64_i64()),
        cursor,
    }
}

fn one_cursor() -> Option<DeltaCursor> {
    Some(DeltaCursor {
        tag: 0xFEED,
        tick: NonZeroU64::new(41).unwrap(),
    })
}

/// A registration round-trips with its cursor and decodes to the layout its
/// block describes; one with no feed position comes back with none.
#[test]
fn a_record_round_trips() {
    for cursor in [one_cursor(), None] {
        let rec = record(cursor);
        assert_eq!(MirrorRecord::decode(&rec.encode()), Some((rec, make_schema_u64_i64())));
    }
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

    for (what, bytes) in refused {
        assert!(MirrorRecord::decode(&bytes).is_none(), "{what}");
    }
}
