use super::*;

/// A recording in the format `gnitz_core::recording` writes: a header, then
/// one record per frame.
fn recording(frames: &[(u8, u32, &[u8])]) -> Vec<u8> {
    let mut out = b"GNZR".to_vec();
    out.extend_from_slice(&gnitz_wire::wal::WAL_FORMAT_VERSION.to_le_bytes());
    for &(dir, conn, frame) in frames {
        out.push(dir);
        out.extend_from_slice(&conn.to_le_bytes());
        out.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        out.extend_from_slice(frame);
    }
    out
}

/// Import keeps one seed per distinct conversation sent, drops recordings
/// that sent nothing or too much, and counts the unreadable; a seed reads back
/// as the frames that were sent.
#[test]
fn import_keeps_one_seed_per_distinct_conversation() {
    let rec = tempfile::tempdir().unwrap();
    let seeds = tempfile::tempdir().unwrap();
    let a = recording(&[(0, 0, b"one"), (1, 0, b"reply"), (0, 1, b"two")]);
    std::fs::write(rec.path().join("a.frames"), &a).unwrap();
    std::fs::write(rec.path().join("b.frames"), &a).unwrap();
    std::fs::write(rec.path().join("c.frames"), recording(&[(1, 0, b"only received")])).unwrap();
    std::fs::write(rec.path().join("d.frames"), b"torn").unwrap();
    let bulk = vec![0u8; MAX_SEED_SENT + 1];
    std::fs::write(rec.path().join("e.frames"), recording(&[(0, 0, &bulk)])).unwrap();
    let imported = import(rec.path(), seeds.path()).unwrap();
    assert_eq!(
        imported,
        Imported {
            seeds: 1,
            duplicates: 2,
            oversized: 1,
            unreadable: 1
        }
    );

    let all = Seeds::open(seeds.path()).unwrap();
    assert_eq!(all.len(), 1);
    let (seed, _) = all.pick(7).unwrap();
    let sent: Vec<(u32, &[u8])> = seed.sent.iter().map(|f| (f.conn, f.bytes.as_slice())).collect();
    assert_eq!(sent, [(0, &b"one"[..]), (1, &b"two"[..])]);
    assert_eq!(seed.received.len(), 1);
    assert_eq!(
        import(rec.path(), seeds.path()).unwrap().seeds,
        1,
        "importing again changes nothing"
    );
}
