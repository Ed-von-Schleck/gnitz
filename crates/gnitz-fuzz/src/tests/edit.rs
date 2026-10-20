use super::*;
use crate::test_support::{cut, frames, scan_frame};

fn traffic() -> Vec<Frame> {
    let conns = 0..;
    conns.zip(frames()).map(|(conn, bytes)| Frame { conn, bytes }).collect()
}

/// Every edit of a frame's insides, at every pick and in every mode, leaves
/// the other frames as they were. An integer edit changes the bytes of one
/// field or cell alone; an edit that keeps lengths true leaves a frame that
/// decodes as deep as it did.
#[test]
fn every_edit_changes_only_what_it_picks() {
    let traffic = traffic();
    let depth = |a: &Anatomy| (a.blocks.len(), a.lists.len(), a.strings.len());
    for kind in KINDS {
        for (i, before) in traffic.iter().enumerate() {
            let a = Anatomy::of(&before.bytes);
            for (pick, how) in (0..32).flat_map(|p| (0..32).map(move |h| (p, h))) {
                let mut sent = traffic.clone();
                Edit { kind, frame: i as u16, pick, how }.apply(&mut sent);
                if matches!(
                    kind,
                    Kind::DropFrame | Kind::RepeatFrame | Kind::SwapFrame | Kind::MoveFrame
                ) {
                    continue;
                }
                let others = |s: &[Frame]| [&s[..i], &s[i + 1..]].concat();
                assert_eq!(others(&sent), others(&traffic), "{kind:?} {pick} {how}");
                let after = &sent[i].bytes;
                match kind {
                    Kind::Int => {
                        let changed: Vec<usize> = (0..after.len()).filter(|&b| after[b] != before.bytes[b]).collect();
                        assert!(
                            a.fields
                                .iter()
                                .chain(&a.cells)
                                .any(|f| changed.iter().all(|b| f.range().contains(b))),
                            "{pick} {how}"
                        );
                    }
                    Kind::Bytes => {}
                    _ => assert_eq!(depth(&Anatomy::of(after)), depth(&a), "{kind:?} {pick} {how}"),
                }
            }
        }
    }
    for kind in KINDS {
        let mut empty = Vec::new();
        Edit { kind, frame: 0, pick: 0, how: 0 }.apply(&mut empty);
        assert!(empty.is_empty());
    }
}

/// Dropping or repeating a list's item is the frame the client encodes for
/// the list without it, or with it twice.
#[test]
fn an_item_edit_is_the_frame_of_the_edited_list() {
    for (repeat, how, want) in [(false, 0, cut(&[2])), (true, 1, cut(&[1, 2, 2]))] {
        let mut frame = scan_frame(&cut(&[1, 2]));
        item(&mut frame, 0, how, repeat);
        assert_eq!(frame, scan_frame(&want));
    }
}

/// Frame edits move, repeat, drop and swap whole frames.
#[test]
fn frame_edits_rearrange_the_traffic() {
    let mut sent = traffic();
    let conns = |s: &[Frame]| s.iter().map(|f| f.conn).collect::<Vec<_>>();
    for (kind, frame, pick, want) in [
        (Kind::SwapFrame, 0, 0, vec![1, 0, 2]),
        (Kind::RepeatFrame, 1, 0, vec![1, 0, 0, 2]),
        (Kind::MoveFrame, 2, MAX_CONNS as u16 + 3, vec![1, 0, 3, 2]),
        (Kind::DropFrame, 5, 0, vec![1, 3, 2]),
    ] {
        Edit { kind, frame, pick, how: 0 }.apply(&mut sent);
        assert_eq!(conns(&sent), want, "{kind:?}");
    }
}
