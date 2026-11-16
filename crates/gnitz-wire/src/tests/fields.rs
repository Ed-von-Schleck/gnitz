use super::*;
use crate::codec::{Writer, decode_all};
use crate::{CTRL_HEADER_SIZE, ControlHeader, append_frame, peek_control_block};

/// A `u8`, a section, then a counted list of `u32`s: every integer is a field,
/// the section and the list are located, and the list's items are its own.
#[test]
fn a_map_locates_every_field_section_and_list_a_decode_read() {
    let mut w = Writer::new();
    w.u8(7).bytes32(b"abc").list(&[10u32, 20, 30]);
    let bytes = w.into_vec();
    let (decoded, map) = map(&bytes, || {
        decode_all(&bytes, "test", |r| {
            let a = r.u8()?;
            let s = r.bytes32()?.to_vec();
            let l: Vec<u32> = r.list("items", 8)?;
            Ok((a, s, l))
        })
    });
    assert_eq!(decoded.unwrap(), (7, b"abc".to_vec(), vec![10, 20, 30]));
    let widths: Vec<(usize, usize)> = map.fields.iter().map(|f| (f.at, f.width)).collect();
    assert_eq!(widths, [(0, 1), (1, 4), (8, 2), (10, 4), (14, 4), (18, 4)]);
    assert_eq!(map.sections, [Section { prefix: 1, content: 5..8 }]);
    assert_eq!(
        map.lists,
        [List {
            count: 8,
            items: vec![10..14, 14..18, 18..22]
        }]
    );
}

/// A circuit's node list is a counted list: its count leads the cell, and
/// each node, opcode through input ids, is one of its items.
#[test]
fn a_circuit_cell_maps_its_nodes_as_a_list() {
    let mut c = crate::Circuit::default();
    let scan = c.input_delta(7, crate::ReadBound::None);
    c.negate(scan);
    let bytes = c.encode();
    let (decoded, map) = map(&bytes, || crate::Circuit::decode(&bytes));
    assert!(decoded.is_ok());
    let nodes = map.lists.iter().find(|l| l.count == 0).expect("the node list");
    assert_eq!(nodes.items.len(), 2);
    assert_eq!(nodes.items[0].start, 2);
    assert_eq!(nodes.items[0].end, nodes.items[1].start);
    assert_eq!(nodes.items[1].end, bytes.len());
}

/// The control header's six words are fields, in layout order, and its blob
/// is a section behind its length word, wherever the frame lies inside the
/// mapped string.
#[test]
fn a_peeked_control_header_maps_its_six_words() {
    let mut bytes = vec![0xEE; 3];
    let hdr = ControlHeader {
        target_id: 9,
        arg0: 1,
        arg1: 2,
        ..Default::default()
    };
    append_frame(&mut bytes, &hdr, b"blob", None);
    let (peeked, map) = map(&bytes, || peek_control_block(&bytes[3..]));
    assert_eq!(peeked.unwrap().hdr.target_id, 9);
    let fields: Vec<(usize, usize)> = map.fields.iter().map(|f| (f.at, f.width)).collect();
    assert_eq!(fields, [(3, 4), (7, 4), (11, 8), (19, 8), (27, 8), (35, 8)]);
    assert_eq!(fields.last().map(|&(at, w)| at + w), Some(3 + CTRL_HEADER_SIZE));
    let blob = 3 + CTRL_HEADER_SIZE;
    assert_eq!(map.sections, [Section { prefix: 3 + 4, content: blob..blob + 4 }]);
}

/// A reader over a copy of the bytes notes nothing: a map holds offsets into
/// the mapped string alone.
#[test]
fn a_reader_outside_the_mapped_string_notes_nothing() {
    let bytes = 5u32.to_le_bytes();
    let copy = bytes.to_vec();
    let (_, map) = map(&bytes, || decode_all(&copy, "test", |r| r.u32()));
    assert!(map.fields.is_empty());
}

/// A decode that unwinds leaves no mapping open behind it.
#[test]
fn an_unwinding_decode_closes_its_map() {
    let bytes = [1u8];
    let unwound = std::panic::catch_unwind(|| map(&bytes, || panic!("decoder bug")));
    assert!(unwound.is_err());
    let (_, map) = map(&bytes, || decode_all(&bytes, "test", |r| r.u8()));
    assert_eq!(map.fields, [Field { at: 0, width: 1 }]);
}

/// A failed attempt leaves nothing behind; a successful one reports the
/// sections it read.
#[test]
fn an_attempt_keeps_only_a_decode_that_succeeded() {
    let mut w = Writer::new();
    w.u16(1).bytes32(b"xy");
    let bytes = w.into_vec();
    let (_, map) = map(&bytes, || {
        let failed = attempt(|| decode_all(&bytes, "test", |r| r.u64().and(r.u8())));
        assert!(failed.is_err());
        let (_, sections) = attempt(|| decode_all(&bytes, "test", |r| r.u16().and(r.bytes32()))).unwrap();
        assert_eq!(sections, [Section { prefix: 2, content: 6..8 }]);
    });
    let fields: Vec<(usize, usize)> = map.fields.iter().map(|f| (f.at, f.width)).collect();
    assert_eq!(fields, [(0, 2), (2, 4)]);
}
