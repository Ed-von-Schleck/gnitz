use super::*;
use crate::test_support::sweep_bit_flips;
use gnitz_wire::MAX_PK_BYTES;

/// A manifest with `count` entries alternating an empty and a widest guard key
/// over two levels, and a `count`-byte caller record, so every field shape
/// round-trips.
fn sample(count: usize) -> Manifest {
    Manifest {
        checkpoint_mark: 11,
        caller_record: (0..count as u8).map(|b| b ^ 0xA5).collect(),
        shards: ShardSet {
            run_bytes: 9 << 20,
            entries: (0..count)
                .map(|i| {
                    let level = (i % 2) as u64;
                    ManifestEntry {
                        seq: 200 + i as u64,
                        newest: 100 + i as u64,
                        level,
                        guard_key: if level == 0 {
                            PkBuf::zeroed(0)
                        } else {
                            PkBuf::from_bytes(&[42 + i as u8; MAX_PK_BYTES])
                        },
                    }
                })
                .collect(),
        },
    }
}

/// The magic and version words every manifest opens with.
const IDENTITY: usize = 16;

/// `body` behind `magic` and `version`, sealed by a valid digest.
fn framed(magic: u64, version: u64, body: impl FnOnce(&mut Writer)) -> Vec<u8> {
    let mut w = Writer::new();
    w.u64(magic).u64(version);
    body(&mut w);
    let mut buf = w.into_vec();
    let digest = gnitz_wire::checksum(&buf);
    buf.extend_from_slice(&digest.to_le_bytes());
    buf
}

#[test]
fn decode_rejects_a_malformed_manifest() {
    let mut trailing = encode(&sample(2));
    trailing.extend_from_slice(&[0u8; 16]);
    let cases: &[(&str, Vec<u8>, StorageError)] = &[
        (
            "bad magic",
            framed(0xDEADBEEF, VERSION, |_| {}),
            StorageError::Corrupt("manifest magic"),
        ),
        // Any non-current version is rejected outright — there is no legacy reader.
        (
            "old version",
            framed(MAGIC, VERSION - 1, |_| {}),
            StorageError::Corrupt("manifest version"),
        ),
        (
            "short buffer",
            framed(MAGIC, VERSION, |_| {})[..IDENTITY + 7].to_vec(),
            StorageError::Corrupt("manifest truncated"),
        ),
        ("trailing bytes", trailing, StorageError::Corrupt("manifest checksum")),
        (
            "guard key too wide",
            framed(MAGIC, VERSION, |w| {
                w.u64(0)
                    .u64(0)
                    .bytes32(&[])
                    .u64(1)
                    .u64(1)
                    .u64(1)
                    .bytes32(&[0; MAX_PK_BYTES + 1]);
            }),
            StorageError::Corrupt("manifest body"),
        ),
        (
            "torn entry",
            framed(MAGIC, VERSION, |w| {
                w.u64(0).u64(0).bytes32(&[]).u64(1).u64(1);
            }),
            StorageError::Corrupt("manifest body"),
        ),
    ];
    for (name, buf, want) in cases {
        assert_eq!(decode(buf).unwrap_err(), *want, "{name}");
    }
}

#[test]
fn every_byte_past_the_identity_is_inside_the_digest() {
    // Seqs, levels, guard keys, stamps and the header words have no other
    // check, so the sweep is over every byte rather than a chosen few.
    let mut buf = encode(&sample(3));
    let span = IDENTITY..buf.len();
    sweep_bit_flips(&mut buf, span, |byte, bit, buf| {
        assert_eq!(
            decode(buf).unwrap_err(),
            StorageError::Corrupt("manifest checksum"),
            "byte {byte} bit {bit}"
        );
    });
}

#[test]
fn read_roundtrips_a_prepared_manifest_at_zero_one_and_many_entries() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().to_str().unwrap();
    assert_eq!(read(d).unwrap(), None, "no manifest reads as absent");

    for count in [0usize, 1, 2, 7] {
        let m = published(d, count);
        assert_eq!(read(d).unwrap(), Some(m), "count={count}");
    }
}

/// `dir` holding a published manifest of `count` entries, each entry's shard
/// file beside it: the barrier's publish step, minus the fsyncs a round-trip
/// does not observe.
fn published(dir: &str, count: usize) -> Manifest {
    fs::create_dir_all(dir).unwrap();
    let m = sample(count);
    for e in &m.shards.entries {
        fs::write(shard_path(dir, e.seq), e.seq.to_le_bytes()).unwrap();
    }
    stage(dir, &m).unwrap();
    commit(dir).unwrap();
    m
}

/// A name `u64::from_str` would read as a seq but `shard_name` never writes
/// denotes no shard.
#[test]
fn only_the_canonical_shard_name_denotes_a_seq() {
    assert_eq!(shard_seq(&shard_name(7)), Some(7));
    for name in [
        "shard_07.db",
        "shard_+7.db",
        "shard_7.db.tmp",
        "shard_.db",
        "shard_18446744073709551616.db",
    ] {
        assert_eq!(shard_seq(name), None, "{name}");
    }
}

/// A batch retires every store it names, a store with no manifest or no
/// directory among them.
#[test]
fn retire_stores_removes_every_store_it_names() {
    let root = tempfile::tempdir().unwrap();
    let dir = |name: &str| format!("{}/{name}", root.path().to_str().unwrap());
    published(&dir("a"), 2);
    published(&dir("b"), 0);
    fs::create_dir(dir("unpublished")).unwrap();
    published(&dir("kept"), 1);

    retire_stores(&[dir("a"), dir("b"), dir("unpublished"), dir("absent")]).unwrap();
    for name in ["a", "b", "unpublished", "absent"] {
        assert!(!Path::new(&dir(name)).exists(), "{name}");
    }
    assert!(read(&dir("kept")).unwrap().is_some(), "a store the batch does not name");
}

/// A batch puts every store whose source published a manifest, each copy
/// reading back as its source, and makes nothing for one that published none.
#[test]
fn link_stores_puts_every_published_store_and_nothing_else() {
    let root = tempfile::tempdir().unwrap();
    let dir = |name: &str| format!("{}/{name}", root.path().to_str().unwrap());
    let a = published(&dir("src/a"), 3);
    let b = published(&dir("src/b"), 0);
    fs::create_dir_all(dir("src/unpublished")).unwrap();

    let pairs = [("a", "dst/x/a"), ("b", "dst/y/b"), ("unpublished", "dst/z/c")]
        .map(|(src, dst)| (dir(&format!("src/{src}")), dir(dst)));
    link_stores(&pairs).unwrap();
    assert_eq!(read(&dir("dst/x/a")).unwrap(), Some(a.clone()));
    assert_eq!(read(&dir("dst/y/b")).unwrap(), Some(b));
    for e in &a.shards.entries {
        assert_eq!(
            fs::read(shard_path(&dir("dst/x/a"), e.seq)).unwrap(),
            e.seq.to_le_bytes()
        );
    }
    assert!(
        !Path::new(&dir("dst/z")).exists(),
        "nothing is made for an unpublished store"
    );
    link_stores(&[]).unwrap();
}
