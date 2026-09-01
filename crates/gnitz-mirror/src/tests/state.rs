//! `mirror_state`'s record format: what a round trip preserves, and what a
//! damaged file must be refused for.
//!
//! Every rejection here collapses to the same recovery — resume nothing, reclaim
//! the copies, bootstrap every view — so the only failure that matters is a
//! damaged file read back as a *plausible* one. A record whose lengths were
//! misread names copies at a feed position they never reached, and the next poll
//! re-applies an interval the copies already hold: the row set stays identical
//! and every weight in it doubles.

use gnitz_store_testkit::scratch_dir;

use super::*;

const TOPOLOGY: u64 = 0x1234_5678_9ABC_DEF0;

/// A directory holding one written state file, and the bytes it holds.
fn written(name: &str, cursors: &HashMap<u64, DeltaCursor>) -> (String, Vec<u8>) {
    let dir = scratch_dir("mirror_state", name);
    std::fs::create_dir_all(&dir).unwrap();
    let records = HashMap::from([(
        7u64,
        MirrorRecord {
            schema_name: "public".to_string(),
            name: "recent".to_string(),
            block: vec![0xAB; 20],
        },
    )]);
    write_state(&dir, 9, TOPOLOGY, &records, cursors).expect("the file is written");
    let bytes = std::fs::read(path(&dir)).unwrap();
    (dir, bytes)
}

fn one_cursor() -> HashMap<u64, DeltaCursor> {
    HashMap::from([(7u64, DeltaCursor { tag: 0xFEED, tick: 41 })])
}

/// Put `bytes` back and read them under the launched topology word.
fn reread(dir: &str, bytes: &[u8]) -> Option<MirrorState> {
    std::fs::write(path(dir), bytes).unwrap();
    read_state(dir, TOPOLOGY)
}

#[test]
fn a_round_trip_keeps_the_generation_the_record_and_its_cursor() {
    let (dir, _) = written("round_trip", &one_cursor());
    let state = read_state(&dir, TOPOLOGY).expect("the file just written reads back");
    assert_eq!(state.generation, 9);
    let rec = state.records.get(&7).expect("the record came back under its id");
    assert_eq!((rec.schema_name.as_str(), rec.name.as_str()), ("public", "recent"));
    assert_eq!(rec.block, vec![0xAB; 20], "the schema block is kept verbatim");
    assert_eq!(state.cursors.get(&7), Some(&DeltaCursor { tag: 0xFEED, tick: 41 }));
}

/// A registration with no feed position comes back with none, rather than as a
/// cursor at round 0 — which the next poll would answer off a copy the gate is
/// there to refuse.
#[test]
fn a_registration_written_without_a_cursor_reads_back_without_one() {
    let (dir, _) = written("no_cursor", &HashMap::new());
    let state = read_state(&dir, TOPOLOGY).expect("a record with no cursor is still a record");
    assert!(state.records.contains_key(&7));
    assert!(state.cursors.is_empty(), "no position was written, so none is read");
}

/// A file this binary did not launch under is refused whatever else it holds.
#[test]
fn a_foreign_topology_word_is_refused() {
    let (dir, _) = written("foreign_topology", &one_cursor());
    assert!(read_state(&dir, TOPOLOGY ^ 1).is_none());
}

/// Every bit of the fence words and of the three lengths that drive the walk.
///
/// These are the fields a misread turns into a *plausible* state; the generation,
/// the ids and the cursors carry no redundancy and a flip in one is indetectable
/// by construction, so they are outside the sweep rather than silently passing it.
#[test]
fn a_flip_in_a_fence_word_or_a_length_is_refused() {
    let (dir, bytes) = written("bit_flips", &one_cursor());
    let mut buf = bytes.clone();
    // Magic, topology and count in the header; then the record's block, schema
    // and name lengths.
    for span in [0..4, 16..32, HEADER_LEN + 28..HEADER_LEN + RECORD_PREFIX_LEN] {
        gnitz_store_testkit::sweep_bit_flips(&mut buf, span, |byte, bit, damaged| {
            assert!(
                reread(&dir, damaged).is_none(),
                "a flip of bit {bit} in byte {byte} was read back as a usable state",
            );
        });
    }
    assert!(reread(&dir, &bytes).is_some(), "the sweep restored every byte");
}

/// A file cut short anywhere, and one with a byte after the last record.
#[test]
fn a_truncated_or_overlong_file_is_refused() {
    let (dir, bytes) = written("length", &one_cursor());
    for cut in 0..bytes.len() {
        assert!(
            reread(&dir, &bytes[..cut]).is_none(),
            "a file cut at {cut} was accepted"
        );
    }
    let mut longer = bytes.clone();
    longer.push(0);
    assert!(
        reread(&dir, &longer).is_none(),
        "a byte past the last record means the walk and the file disagree",
    );
}
