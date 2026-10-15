use super::*;
use crate::test_support::{circuit, ddl_frame, predicate, scan_frame};

/// A `DDL_TXN` is taken apart all the way down: its item, the item's block,
/// the `circuits` row's string, the circuit in it, and the expression program
/// inside the circuit.
#[test]
fn the_anatomy_reaches_an_expression_inside_a_circuit_inside_a_row() {
    let frame = ddl_frame();
    let a = Anatomy::of(&frame);
    assert_eq!(a.blocks.len(), 1);
    let circuit = circuit();
    let cell = a
        .strings
        .iter()
        .find(|s| frame[s.content.clone()] == circuit[..])
        .expect("the circuit's string");
    let predicate = predicate();
    let program = a
        .sections
        .iter()
        .find(|s| holds(&cell.content, &s.content) && frame[s.content.clone()] == predicate[..])
        .expect("the predicate's section inside the circuit");
    assert!(
        a.fields.iter().any(|f| holds(&program.content, &f.range())),
        "the program's own fields are mapped"
    );
    assert!(!a.cells.is_empty(), "the row's cells are mapped");
    let mut distinct = a.fields.clone();
    distinct.sort_by_key(|f| (f.at, f.width));
    distinct.dedup();
    assert_eq!(distinct.len(), a.fields.len(), "each field is mapped once");
}

/// A scan's `ReadSpec` blob is mapped past the header's six words.
#[test]
fn a_scan_frame_maps_its_read_spec() {
    let frame = scan_frame(gnitz_wire::ReadBound::None);
    let a = Anatomy::of(&frame);
    assert!(a.fields.len() > 6, "{:?}", a.fields);
    assert!(a.fields.iter().any(|f| f.at >= gnitz_wire::control::CTRL_HEADER_SIZE));
}

/// Growing the predicate inside the circuit inside the row keeps every length
/// around it true: the section's prefix, the string's cell and the block are
/// all written anew, and the frame decodes to the grown predicate.
#[test]
fn a_splice_deep_in_a_frame_restates_every_enclosing_length() {
    let frame = ddl_frame();
    let a = Anatomy::of(&frame);
    let predicate = predicate();
    let program = a
        .sections
        .iter()
        .find(|s| frame[s.content.clone()] == predicate[..])
        .unwrap();
    let grown = [predicate.as_slice(), b"tail"].concat();
    let out = a.splice(&frame, program.content.clone(), &grown);

    let after = Anatomy::of(&out);
    assert_eq!(after.blocks.len(), 1, "the block still parses");
    let cell = &after.strings[0];
    let circuit = &out[cell.content.clone()];
    assert_eq!(circuit.len(), frame[a.strings[0].content.clone()].len() + 4);
    assert!(
        after.sections.iter().any(|s| out[s.content.clone()] == grown[..]),
        "the grown section is a section"
    );
}

/// Nothing to take apart is an empty anatomy, not a failure.
#[test]
fn an_undecodable_frame_has_an_empty_anatomy() {
    let a = Anatomy::of(b"not a frame");
    assert!(a.fields.is_empty() && a.cells.is_empty() && a.blocks.is_empty());
}

/// A scan bounded by a key range reads 16-byte key images, each one field,
/// and an integer edit of any field of it leaves a frame.
#[test]
fn a_wide_key_is_one_field() {
    use gnitz_wire::{KeyRange, PkColList, ReadBound};
    let range = KeyRange::point(PkColList::from_slice(&[0]), &[], u128::MAX);
    let frame = scan_frame(ReadBound::Range(range));
    let a = Anatomy::of(&frame);
    assert!(a.fields.iter().any(|f| f.width == 16), "{:?}", a.fields);
    let mut sent = vec![crate::seed::Frame { conn: 0, bytes: frame }];
    for pick in (0..2 * a.fields.len() as u16).step_by(2) {
        for how in 0..8 {
            let kind = crate::edit::Kind::Int;
            crate::edit::Edit { kind, frame: 0, pick, how }.apply(&mut sent);
        }
    }
}
