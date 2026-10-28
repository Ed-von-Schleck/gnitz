use super::*;
use crate::test_support::{circuit, ddl_frame, frames, predicate};
use gnitz_wire::CTRL_HEADER_SIZE;

/// A `DDL_TXN` is taken apart all the way down — its item, the item's block,
/// the `circuits` row's string, the circuit in it and the program inside the
/// circuit — and a splice at any of those depths is the frame the client
/// encodes for the spliced value.
#[test]
fn a_splice_at_any_depth_is_the_frame_of_the_spliced_value() {
    let frame = ddl_frame(&circuit(&predicate()));
    let a = Anatomy::of(&frame);
    let ([string], [program]) = (&a.strings[..], &a.programs[..]) else {
        panic!("one string and one program: {a:?}");
    };
    assert_eq!(frame[program.clone()], predicate()[..]);
    assert!(holds(&string.content, program));
    assert!(
        a.fields.iter().any(|f| holds(program, &f.range())),
        "the program's own fields are mapped"
    );

    let grown = [&predicate()[..], b"tail"].concat();
    for (range, with, want) in [
        (program.clone(), &grown[..], ddl_frame(&circuit(&grown))),
        (string.content.clone(), &[][..], ddl_frame(&[])),
        // A heap string made inline.
        (string.content.clone(), &b"abc"[..], ddl_frame(b"abc")),
    ] {
        assert_eq!(a.splice(&frame, range, with), want);
    }
}

/// Every frame's fields are disjoint, every cell lies in a block, its blob is
/// a section, and a scan's fields tile it around the program its read spec
/// carries: a read spec and a key image are integers all the way through.
#[test]
fn the_fields_of_a_scan_tile_it() {
    for (i, frame) in frames().into_iter().enumerate() {
        let mut a = Anatomy::of(&frame);
        a.fields.sort_by_key(|f| f.at);
        assert!(
            a.fields.windows(2).all(|w| w[0].at + w[0].width <= w[1].at),
            "{:?}",
            a.fields
        );
        assert!(a
            .cells
            .iter()
            .all(|c| a.blocks.iter().any(|b| holds(&b.range, &c.range()))));
        let blob = peek_control_block(&frame).unwrap().blob;
        assert!(a.sections.iter().any(|s| s.content == blob), "{:?}", a.sections);
        if i > 0 {
            assert_eq!(a.programs.len(), usize::from(i == 1));
            let outside = a
                .fields
                .iter()
                .filter(|f| !a.programs.iter().any(|p| holds(p, &f.range())));
            let programs = a.programs.iter().map(|p| p.len()).sum::<usize>();
            assert_eq!(outside.map(|f| f.width).sum::<usize>() + programs, frame.len());
        }
    }
    assert!(Anatomy::of(&frames()[2]).fields.iter().any(|f| f.width == 16));
}

/// Every prefix of a frame — what a cut edit leaves — has an anatomy, and
/// it lies inside the prefix.
#[test]
fn every_prefix_of_a_frame_has_an_anatomy_inside_it() {
    for frame in frames() {
        for n in 0..=frame.len() {
            let a = Anatomy::of(&frame[..n]);
            assert!(a.fields.iter().chain(&a.cells).all(|f| f.at + f.width <= n));
            if n < CTRL_HEADER_SIZE {
                assert!(a.fields.is_empty());
            }
        }
    }
}
