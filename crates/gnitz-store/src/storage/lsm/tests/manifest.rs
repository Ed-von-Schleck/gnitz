use super::*;
use crate::test_support::sweep_bit_flips;
use gnitz_wire::write_u64_le;

/// A manifest with `count` entries alternating L0 (empty guard key) and L1 (an
/// 8-byte guard key), so every field shape round-trips.
fn sample(count: usize) -> Manifest {
    Manifest {
        compact_seq: 11,
        checkpoint_gen: 5,
        run_bytes: 9 << 20,
        entries: (0..count)
            .map(|i| {
                let level = (i % 2) as u64;
                ManifestEntry {
                    name: format!("shard_{i}.db"),
                    max_lsn: 100 + i as u64,
                    level,
                    guard_key: if level == 0 {
                        PkBuf::zeroed(0)
                    } else {
                        PkBuf::from_bytes(&(42 + i as u64).to_be_bytes())
                    },
                }
            })
            .collect(),
    }
}

#[test]
fn roundtrips_at_zero_one_and_many_entries() {
    for count in [0usize, 1, 2, 7] {
        let m = sample(count);
        assert_eq!(decode(&encode(&m)).unwrap(), m, "count={count}");
    }
}

#[test]
fn encode_is_deterministic() {
    assert_eq!(encode(&sample(5)), encode(&sample(5)));
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

#[test]
fn decode_rejects_a_malformed_identity() {
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
    ];
    for (name, buf, want) in cases {
        assert_eq!(decode(buf).unwrap_err(), *want, "{name}");
    }
}

#[test]
fn trailing_bytes_report_checksum_mismatch() {
    let mut buf = encode(&sample(2));
    buf.extend_from_slice(&[0u8; 16]);
    assert_eq!(decode(&buf).unwrap_err(), StorageError::Corrupt("manifest checksum"));
}

#[test]
fn every_byte_past_the_identity_is_inside_the_digest() {
    // Names, levels, guard keys, LSNs and the header counters have no other
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
        // The barrier's `flush_commit` step, minus the fsyncs a round-trip does
        // not observe.
        prepare(d, &encode(&m)).unwrap().commit().unwrap();
        assert_eq!(read(d).unwrap(), Some(m), "count={count}");
    }
}
