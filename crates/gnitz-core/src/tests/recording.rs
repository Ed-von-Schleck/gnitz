use super::*;

/// Two connections to one target share a recording, numbered in the order
/// they opened, and every frame reads back in the order it was noted.
#[test]
fn connections_to_one_target_share_a_recording_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let a = Tap::open_in(dir.path(), "target-a");
    let b = Tap::open_in(dir.path(), "target-a");
    let other = Tap::open_in(dir.path(), "target-b");
    a.note(Direction::Sent, b"one");
    b.note(Direction::Received, b"two");
    a.note(Direction::Sent, b"");
    other.note(Direction::Sent, b"elsewhere");

    let mut files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    assert_eq!(files.len(), 2, "one recording per target");
    let shared = files
        .iter()
        .map(|f| std::fs::read(f).unwrap())
        .find(|bytes| read(bytes).unwrap().len() == 3)
        .expect("the shared recording");
    let got = read(&shared).unwrap();
    let want = [
        Recorded {
            conn: 0,
            dir: Direction::Sent,
            frame: b"one",
        },
        Recorded {
            conn: 1,
            dir: Direction::Received,
            frame: b"two",
        },
        Recorded {
            conn: 0,
            dir: Direction::Sent,
            frame: b"",
        },
    ];
    assert_eq!(got, want);
}

/// A recording of another wire version, a torn record and a foreign file are
/// each refused rather than read as frames.
#[test]
fn read_refuses_what_a_replay_could_not_send() {
    assert!(read(b"nonsense").is_err());
    let mut header = MAGIC.to_vec();
    header.extend_from_slice(&(WAL_FORMAT_VERSION ^ 1).to_le_bytes());
    assert!(read(&header).unwrap_err().contains("wire format"));

    let mut torn = MAGIC.to_vec();
    torn.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    assert_eq!(read(&torn).unwrap(), []);
    torn.extend_from_slice(&[0, 0, 0, 0, 0, 9, 0, 0, 0, b'x']);
    assert!(read(&torn).unwrap_err().contains("runs past"));
}
