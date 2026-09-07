//! `mirror_state`'s record format: what a round trip preserves, and what a
//! damaged file must be refused for.
//!
//! Every rejection here collapses to the same recovery — resume nothing, reclaim
//! the copies, bootstrap every view — so the only failure that matters is a
//! damaged file read back as a *plausible* one: a record naming copies at a feed
//! position they never reached, which the next poll re-applies an interval onto,
//! doubling every weight while the row set stays identical. The generation word
//! sits outside the sweep: a flipped generation can never equal the manifests'.

use gnitz_store_testkit::scratch_dir;

use super::*;

/// A directory holding one written state file, and the bytes it holds.
fn written(name: &str, cursor: Option<DeltaCursor>) -> (String, Vec<u8>) {
    let dir = scratch_dir("mirror_state", name);
    std::fs::create_dir_all(&dir).unwrap();
    write_state(&dir, 9, &encode_records(&records(cursor))).expect("the file is written");
    let bytes = std::fs::read(path(&dir)).unwrap();
    (dir, bytes)
}

fn records(cursor: Option<DeltaCursor>) -> HashMap<u64, MirrorRecord> {
    HashMap::from([(
        7u64,
        MirrorRecord {
            schema_name: "public".to_string(),
            name: "recent".to_string(),
            block: vec![0xAB; 20],
            cursor,
        },
    )])
}

fn one_cursor() -> Option<DeltaCursor> {
    Some(DeltaCursor { tag: 0xFEED, tick: 41 })
}

/// Put `bytes` back and read them.
fn reread(dir: &str, bytes: &[u8]) -> Option<PersistedState> {
    std::fs::write(path(dir), bytes).unwrap();
    read_state(dir)
}

#[test]
fn a_round_trip_keeps_the_generation_the_record_and_its_cursor() {
    let (dir, _) = written("round_trip", one_cursor());
    let state = read_state(&dir).expect("the file just written reads back");
    assert_eq!(state.generation, 9);
    assert_eq!(
        state.records,
        records(one_cursor()),
        "the record and its cursor come back verbatim"
    );
}

/// A registration with no feed position comes back with none, rather than as a
/// cursor at round 0 — which the next poll would answer off a copy the gate is
/// there to refuse.
#[test]
fn a_registration_written_without_a_cursor_reads_back_without_one() {
    let (dir, _) = written("no_cursor", None);
    let state = read_state(&dir).expect("a record with no cursor is still a record");
    assert_eq!(
        state.records.get(&7).map(|r| r.cursor),
        Some(None),
        "no position was written, so none is read",
    );
}

/// Every bit of the block: a flip either refuses the file or reads back the
/// undamaged state (a flipped table id is immaterial), never a
/// plausible-and-wrong one.
#[test]
fn a_flip_anywhere_in_the_block_is_refused_or_immaterial() {
    let (dir, bytes) = written("bit_flips", one_cursor());
    let want = PersistedState {
        generation: 9,
        records: records(one_cursor()),
    };
    let mut buf = bytes.clone();
    gnitz_store_testkit::sweep_bit_flips(&mut buf, HEADER_LEN..bytes.len(), |byte, bit, damaged| {
        if let Some(got) = reread(&dir, damaged) {
            assert_eq!(
                got, want,
                "a flip of bit {bit} in byte {byte} was read back as a different state",
            );
        }
    });
    assert!(reread(&dir, &bytes).is_some(), "the sweep restored every byte");
}

/// A file cut short anywhere, and one with a byte after the block.
#[test]
fn a_truncated_or_overlong_file_is_refused() {
    let (dir, bytes) = written("length", one_cursor());
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
        "a byte past the block means the codec and the file disagree",
    );
}
