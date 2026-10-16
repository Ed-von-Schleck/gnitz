use super::*;
use crate::test_support::sweep_bit_flips;

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
        let m = sample(count);
        // The barrier's publish step, minus the fsyncs a round-trip does
        // not observe.
        prepare(d, &encode(&m)).unwrap();
        commit(d).unwrap();
        assert_eq!(read(d).unwrap(), Some(m), "count={count}");
    }
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
