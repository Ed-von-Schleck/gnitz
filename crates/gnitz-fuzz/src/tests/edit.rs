use super::*;
use crate::anatomy::Anatomy;
use crate::test_support::{ddl_frame, scan_frame};

fn traffic() -> Vec<Frame> {
    vec![
        Frame { conn: 0, bytes: ddl_frame() },
        Frame {
            conn: 1,
            bytes: scan_frame(gnitz_wire::ReadBound::None),
        },
    ]
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
    let frame = scan_frame(gnitz_wire::ReadBound::None);
    let fields = Anatomy::of(&frame).fields;
    let mut sent = vec![Frame { conn: 0, bytes: frame.clone() }];
    // `how & 7 == 3` stores `how >> 3`; pick 0 is the first decoder field.
    Edit {
        kind: Kind::Int,
        frame: 0,
        pick: 0,
        how: (0xAB << 3) | 3,
    }
    .apply(&mut sent);
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
    Edit {
        kind: Kind::Text,
        frame: 0,
        pick: 0,
        how: 2,
    }
    .apply(&mut sent);
    let after = Anatomy::of(&sent[0].bytes);
    assert_eq!(after.blocks.len(), 1);
    assert!(after.strings.iter().any(|s| s.content.is_empty()));
}

/// Frame edits move, repeat, drop and swap whole frames.
#[test]
fn frame_edits_rearrange_the_traffic() {
    let mut sent = traffic();
    let edit = |kind, frame, pick| Edit { kind, frame, pick, how: 0 };
    edit(Kind::SwapFrame, 0, 0).apply(&mut sent);
    assert_eq!(sent.iter().map(|f| f.conn).collect::<Vec<_>>(), [1, 0]);
    edit(Kind::RepeatFrame, 1, 0).apply(&mut sent);
    assert_eq!(sent.len(), 3);
    edit(Kind::MoveFrame, 2, MAX_CONNS as u16 + 3).apply(&mut sent);
    assert_eq!(sent[2].conn, 3);
    edit(Kind::DropFrame, 5, 0).apply(&mut sent);
    assert_eq!(sent.len(), 2);
}
