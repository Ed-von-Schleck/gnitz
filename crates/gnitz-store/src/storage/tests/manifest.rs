use super::*;
use crate::test_support::sweep_bit_flips;
use gnitz_wire::write_u64_le;

/// A manifest with `count` entries alternating an empty and an 8-byte guard
/// key over two levels, and a `count`-byte caller record, so every field shape
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
                            PkBuf::from_bytes(&(42 + i as u64).to_be_bytes())
                        },
                    }
                })
                .collect(),
        },
    }
}

#[test]
fn roundtrips_at_zero_one_and_many_entries() {
    for count in [0usize, 1, 2, 7] {
        let m = sample(count);
        assert_eq!(decode(&encode(&m)).unwrap(), m, "count={count}");
    }
}

/// The magic and version words every manifest opens with.
const IDENTITY: usize = 16;

/// An identity-plus-digest buffer with `magic` and `version` written raw.
fn forged_identity(magic: u64, version: u64) -> Vec<u8> {
    let mut buf = vec![0u8; IDENTITY + 8];
    write_u64_le(&mut buf, 0, magic);
    write_u64_le(&mut buf, 8, version);
    buf
}

/// `body` behind a valid identity and digest.
fn sealed(body: &[u8]) -> Vec<u8> {
    let mut buf = forged_identity(MAGIC, VERSION)[..IDENTITY].to_vec();
    buf.extend_from_slice(body);
    let digest = gnitz_wire::checksum(&buf);
    buf.extend_from_slice(&digest.to_le_bytes());
    buf
}

#[test]
fn decode_rejects_a_malformed_manifest() {
    let header = |w: &mut Writer| {
        w.u64(0).u64(0).bytes32(&[]);
    };
    let mut too_wide = Writer::new();
    header(&mut too_wide);
    too_wide.u64(1).u64(1).u64(1).bytes32(&[0; MAX_PK_BYTES + 1]);
    let mut torn = Writer::new();
    header(&mut torn);
    torn.u64(1).u64(1);
    let mut trailing = encode(&sample(2));
    trailing.extend_from_slice(&[0u8; 16]);
    let cases: &[(&str, Vec<u8>, StorageError)] = &[
        (
            "bad magic",
            forged_identity(0xDEADBEEF, VERSION),
            StorageError::Corrupt("manifest magic"),
        ),
        // Any non-current version is rejected outright — there is no legacy reader.
        (
            "old version",
            forged_identity(MAGIC, VERSION - 1),
            StorageError::Corrupt("manifest version"),
        ),
        (
            "short buffer",
            forged_identity(MAGIC, VERSION)[..IDENTITY + 7].to_vec(),
            StorageError::Corrupt("manifest truncated"),
        ),
        ("trailing bytes", trailing, StorageError::Corrupt("manifest checksum")),
        (
            "guard key too wide",
            sealed(&too_wide.into_vec()),
            StorageError::Corrupt("manifest body"),
        ),
        (
            "torn entry",
            sealed(&torn.into_vec()),
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
fn read_roundtrips_a_prepared_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().to_str().unwrap();
    assert_eq!(read(d).unwrap(), None, "no manifest reads as absent");

    for count in [0usize, 3] {
        let m = sample(count);
        // The barrier's publish step, minus the fsyncs a round-trip does
        // not observe.
        prepare(d, &encode(&m)).unwrap();
        commit(d).unwrap();
        assert_eq!(read(d).unwrap(), Some(m), "count={count}");
    }
}
