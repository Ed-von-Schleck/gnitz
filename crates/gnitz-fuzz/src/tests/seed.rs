use super::*;
use crate::test_support::{circuit, ddl_frame, predicate};
use Direction::{Received, Sent};

/// A recording of `frames`, each its direction, connection and bytes.
fn recording(frames: &[(Direction, u32, &[u8])]) -> Vec<u8> {
    let records: Vec<recording::Recorded> = frames
        .iter()
        .map(|&(dir, conn, frame)| recording::Recorded { conn, dir, frame })
        .collect();
    recording::encode(&records)
}

/// Import keeps one seed per distinct conversation sent, drops recordings
/// that sent nothing or too much, counts the unreadable, and importing again
/// changes nothing; a seed reads back as what was sent and received, and its
/// starts are cut out of it.
#[test]
fn import_keeps_one_seed_per_distinct_conversation() {
    let [rec, seeds, starts] = [(); 3].map(|()| tempfile::tempdir().unwrap());
    let ddl = ddl_frame(&circuit(&predicate()));
    let a = recording(&[(Sent, 0, &ddl), (Received, 0, b"reply"), (Sent, 1, b"two")]);
    let bulk = vec![0u8; MAX_SEED_SENT + 1];
    for (name, bytes) in [
        ("a", a.clone()),
        ("b", a),
        ("c", recording(&[(Received, 0, b"only received")])),
        ("d", b"torn".to_vec()),
        ("e", recording(&[(Sent, 0, &bulk)])),
    ] {
        std::fs::write(rec.path().join(name).with_extension(recording::EXT), bytes).unwrap();
    }
    let want = Imported {
        seeds: 1,
        duplicates: 2,
        oversized: 1,
        unreadable: 1,
    };
    assert_eq!(import(rec.path(), seeds.path()).unwrap(), want);
    assert_eq!(import(rec.path(), seeds.path()).unwrap(), want);

    let all = Seeds::open(seeds.path()).unwrap();
    assert_eq!(all.len(), 1, "importing again changes nothing");
    let (seed, _) = all.pick(7).unwrap();
    let frame = |conn, bytes: &[u8]| Frame { conn, bytes: bytes.to_vec() };
    assert_eq!(
        seed,
        Seed {
            sent: vec![frame(0, &ddl), frame(1, b"two")],
            received: vec![frame(0, b"reply")]
        }
    );

    write_starts(&all, starts.path()).unwrap();
    let read = |path: &str| std::fs::read(starts.path().join(path)).unwrap();
    let programs: Vec<_> = std::fs::read_dir(starts.path().join("program")).unwrap().collect();
    assert_eq!(programs.len(), 1);
    assert_eq!(
        std::fs::read(programs[0].as_ref().unwrap().path()).unwrap(),
        predicate()
    );
    assert_eq!(read("traffic/seed-00000"), crate::input::Input::unedited(0));
    assert_eq!(read("reply/seed-00000"), gnitz_wire::framed(b"reply"));
}
