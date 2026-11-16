use super::*;
use crate::edit::{KINDS, Kind};

/// Any bytes decode: missing head bytes read as zero, an edit's kind wraps
/// and a trailing part of an edit is ignored; a seed's unedited input is that
/// seed on the first boot.
#[test]
fn any_bytes_are_an_input() {
    let unedited = |seed| Input {
        boot: Boot::decode(0),
        seed,
        edits: vec![],
    };
    assert_eq!(Input::decode(&[]), unedited(0));
    assert_eq!(Input::decode(&Input::unedited(513)), unedited(513));
    let mut bytes = vec![0x1F, 1, 2, KINDS.len() as u8 + 1, 3, 0, 4, 0];
    bytes.extend(5u64.to_le_bytes());
    bytes.push(0xFF);
    let edit = Edit {
        kind: Kind::DropItem,
        frame: 3,
        pick: 4,
        how: 5,
    };
    assert_eq!(
        Input::decode(&bytes),
        Input {
            boot: Boot::decode(0x1F),
            seed: 0x0201,
            edits: vec![edit]
        }
    );
}

/// Each of the boot byte's five low bits matters: every value is a boot of
/// its own.
#[test]
fn every_boot_is_reachable() {
    for b in 0..32u8 {
        for c in 0..b {
            assert_ne!(Boot::decode(b), Boot::decode(c));
        }
    }
}
