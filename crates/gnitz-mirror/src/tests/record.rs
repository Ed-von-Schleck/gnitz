//! The record codec: what a round trip preserves, and what bytes it refuses.

use gnitz_core::Schema;
use gnitz_wire::{ColumnDef, TypeCode};

use super::*;

fn record(cursor: Option<DeltaCursor>) -> MirrorRecord {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    };
    MirrorRecord {
        schema_name: "public".to_string(),
        name: "recent".to_string(),
        block: schema.to_block(),
        cursor,
    }
}

fn one_cursor() -> Option<DeltaCursor> {
    Some(DeltaCursor {
        tag: 0xFEED,
        tick: NonZeroU64::new(41).unwrap(),
    })
}

/// A registration round-trips with its cursor, and one with no feed position
/// comes back with none rather than as a cursor at round 0 — which the next poll
/// would answer off a copy the gate is there to refuse.
#[test]
fn a_record_round_trips() {
    for cursor in [one_cursor(), None] {
        let rec = record(cursor);
        assert_eq!(MirrorRecord::decode(&rec.encode()).map(|(r, _)| r), Some(rec));
    }
}

#[test]
fn bytes_encode_did_not_produce_are_refused() {
    let good = record(one_cursor()).encode();
    let mut refused: Vec<(String, Vec<u8>)> = (0..good.len())
        .map(|cut| (format!("cut at {cut}"), good[..cut].to_vec()))
        .collect();
    let mut overlong = good.clone();
    overlong.push(0);
    refused.push(("a trailing byte".into(), overlong));
    let mut flag = good.clone();
    flag[0] = 2;
    refused.push(("an unknown cursor flag".into(), flag));
    // The flag byte, then the tag, then the tick.
    let mut round_0 = good.clone();
    round_0[9..17].fill(0);
    refused.push(("a cursor at round 0".into(), round_0));
    let no_layout = MirrorRecord {
        block: vec![0xAB; 20],
        ..record(one_cursor())
    };
    refused.push(("a block describing no layout".into(), no_layout.encode()));

    for (what, bytes) in refused {
        assert!(MirrorRecord::decode(&bytes).is_none(), "{what}");
    }
}
