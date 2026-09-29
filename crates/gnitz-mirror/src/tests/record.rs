//! The record codec: what a round trip preserves, and what bytes it refuses.

use gnitz_core::Schema;
use gnitz_wire::{ColumnDef, TypeCode};

use super::*;

fn schema_block() -> Vec<u8> {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    };
    schema.to_block()
}

fn record(cursor: Option<DeltaCursor>) -> MirrorRecord {
    MirrorRecord {
        schema_name: "public".to_string(),
        name: "recent".to_string(),
        block: schema_block(),
        cursor,
    }
}

fn one_cursor() -> Option<DeltaCursor> {
    Some(DeltaCursor {
        tag: 0xFEED,
        tick: NonZeroU64::new(41).unwrap(),
    })
}

#[test]
fn a_record_with_a_cursor_round_trips() {
    let rec = record(one_cursor());
    assert_eq!(MirrorRecord::decode(&rec.encode()).map(|(r, _)| r), Some(rec));
}

/// A registration with no feed position comes back with none, rather than as a
/// cursor at round 0 — which the next poll would answer off a copy the gate is
/// there to refuse.
#[test]
fn a_record_without_a_cursor_reads_back_without_one() {
    let rec = record(None);
    assert_eq!(MirrorRecord::decode(&rec.encode()).map(|(r, _)| r), Some(rec));
}

#[test]
fn truncated_or_overlong_bytes_are_refused() {
    let bytes = record(one_cursor()).encode();
    for cut in 0..bytes.len() {
        assert_eq!(
            MirrorRecord::decode(&bytes[..cut]).map(|(r, _)| r),
            None,
            "bytes cut at {cut}"
        );
    }
    let mut longer = bytes;
    longer.push(0);
    assert_eq!(MirrorRecord::decode(&longer).map(|(r, _)| r), None);
}

#[test]
fn an_unknown_cursor_flag_is_refused() {
    let mut bytes = record(one_cursor()).encode();
    bytes[0] = 2;
    assert_eq!(MirrorRecord::decode(&bytes).map(|(r, _)| r), None);
}

#[test]
fn a_block_describing_no_layout_is_refused() {
    let rec = MirrorRecord {
        block: vec![0xAB; 20],
        ..record(one_cursor())
    };
    assert_eq!(MirrorRecord::decode(&rec.encode()).map(|(r, _)| r), None);
}

#[test]
fn a_cursor_at_round_0_is_refused() {
    let mut bytes = record(one_cursor()).encode();
    // The flag byte, then the tag, then the tick.
    bytes[9..17].fill(0);
    assert_eq!(MirrorRecord::decode(&bytes).map(|(r, _)| r), None);
}
