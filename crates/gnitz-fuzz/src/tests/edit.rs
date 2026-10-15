use super::*;
use crate::anatomy::Anatomy;
use crate::test_support::{ddl_frame, scan_frame};

fn traffic() -> Vec<Frame> {
    vec![
        Frame { conn: 0, bytes: ddl_frame() },
        Frame { conn: 1, bytes: scan_frame() },
    ]
}

/// Every edit encodes to bytes that decode to it.
#[test]
fn an_edit_round_trips_its_encoding() {
    let edits = [
        Edit::Int { frame: 1, pick: 2, how: 3 },
        Edit::DropItem { frame: 4, pick: 5, how: 6 },
        Edit::RepeatItem { frame: 7, pick: 8, how: 9 },
        Edit::Text { frame: 1, pick: 1, how: u64::MAX },
        Edit::Bytes { frame: 0, pick: 65535, how: 1 },
        Edit::DropFrame { frame: 3 },
        Edit::RepeatFrame { frame: 3 },
        Edit::SwapFrame { frame: 3 },
        Edit::MoveFrame { frame: 3, conn: 2 },
    ];
    for e in edits {
        assert_eq!(Edit::decode(&e.encode()), e);
    }
}

/// Every edit of every kind, at any selector, leaves traffic it can act on
/// edited and other traffic as it was — none panics.
#[test]
fn every_edit_is_total() {
    for kind in 0..9u8 {
        for sel in [0u8, 1, 7, 0x80, 0xFF] {
            let bytes = [kind, sel, sel, sel, sel ^ 0x55, sel, 1, 2, 3, 4, 5, 6, sel];
            let e = Edit::decode(&bytes);
            let mut sent = traffic();
            e.apply(&mut sent);
            let mut empty = Vec::new();
            e.apply(&mut empty);
            assert!(empty.is_empty());
        }
    }
}

/// An integer edit rewrites the one field it picks, and only it.
#[test]
fn an_int_edit_rewrites_one_field() {
    let frame = scan_frame();
    let fields = Anatomy::of(&frame).fields;
    let mut sent = vec![Frame { conn: 0, bytes: frame.clone() }];
    // `how & 7 == 3` stores `how >> 3`; pick 0 is the first decoder field.
    Edit::Int { frame: 0, pick: 0, how: (0xAB << 3) | 3 }.apply(&mut sent);
    let f = fields[0];
    assert_eq!(sent[0].bytes[f.range()], 0xABu32.to_le_bytes()[..f.width]);
    let outside = |b: &[u8]| [b[..f.at].to_vec(), b[f.at + f.width..].to_vec()].concat();
    assert_eq!(outside(&sent[0].bytes), outside(&frame));
}

/// A text edit that empties the circuit leaves a frame whose block still
/// parses, with the string emptied.
#[test]
fn a_text_edit_keeps_the_frame_decodable() {
    let mut sent = vec![Frame { conn: 0, bytes: ddl_frame() }];
    Edit::Text { frame: 0, pick: 0, how: 2 }.apply(&mut sent);
    let after = Anatomy::of(&sent[0].bytes);
    assert_eq!(after.blocks.len(), 1);
    assert!(after.strings.iter().any(|s| s.content.is_empty()));
}

/// Frame edits move, repeat, drop and swap whole frames.
#[test]
fn frame_edits_rearrange_the_traffic() {
    let mut sent = traffic();
    Edit::SwapFrame { frame: 0 }.apply(&mut sent);
    assert_eq!(sent.iter().map(|f| f.conn).collect::<Vec<_>>(), [1, 0]);
    Edit::RepeatFrame { frame: 1 }.apply(&mut sent);
    assert_eq!(sent.len(), 3);
    Edit::MoveFrame { frame: 2, conn: 3 }.apply(&mut sent);
    assert_eq!(sent[2].conn, 3);
    Edit::DropFrame { frame: 5 }.apply(&mut sent);
    assert_eq!(sent.len(), 2);
}
