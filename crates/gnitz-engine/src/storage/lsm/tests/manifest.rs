use super::*;
use crate::test_support::sweep_bit_flips;

fn hdr(compact_seq: u64, checkpoint_gen: u64) -> ManifestHeader {
    ManifestHeader {
        compact_seq,
        checkpoint_gen,
        layout_seq: 0,
    }
}

fn make_entry(max_lsn: u64, name: &str) -> ManifestEntryRaw {
    ManifestEntryRaw::new(name, max_lsn, 1, 42)
}

/// A header-only buffer with `magic`, `version` and `count` written raw, so a
/// case can forge any one of them.
fn forged_header(magic: u64, version: u64, count: u64) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER_SIZE];
    write_u64_le(&mut buf, 0, magic);
    write_u64_le(&mut buf, 8, version);
    write_u64_le(&mut buf, 16, count);
    buf
}

#[test]
fn parse_rejects_a_malformed_header() {
    let cases: &[(&str, Vec<u8>, StorageError)] = &[
        (
            "bad magic",
            forged_header(0xDEADBEEF, VERSION, 0),
            StorageError::InvalidMagic,
        ),
        // Any non-current version is rejected outright — there is no legacy reader.
        (
            "old version",
            forged_header(MAGIC, VERSION - 1, 0),
            StorageError::InvalidVersion,
        ),
        // `count * ENTRY_SIZE` must not wrap past the length check.
        (
            "count overflow",
            forged_header(MAGIC, VERSION, u64::MAX),
            StorageError::Truncated,
        ),
        ("short buffer", vec![0u8; 10], StorageError::Truncated),
    ];
    for (name, buf, want) in cases {
        assert_eq!(parse(buf).unwrap_err(), *want, "{name}");
    }
}

#[test]
fn serialize_rejects_a_buffer_that_cannot_hold_the_entries() {
    let mut buf = vec![0u8; HEADER_SIZE];
    assert_eq!(
        serialize(&mut buf, &[make_entry(1, "test.db")], hdr(0, 0)),
        Err(StorageError::BufferTooSmall)
    );
}

// --- File I/O tests ---

/// Publish `entries` at `path` for round-trip tests: stage the `.tmp` via
/// the production `prepare_file`, then rename it into place (the barrier's
/// `flush_commit` step, minus the fsyncs the round-trip doesn't observe).
fn write_manifest(path: &std::ffi::CStr, entries: &[ManifestEntryRaw], header: ManifestHeader) {
    let m = prepare_file(path, entries, header).unwrap();
    m.commit().unwrap();
}

#[test]
fn write_read_file_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    for count in [0usize, 3] {
        let path = dir.path().join(format!("MANIFEST_{count}"));
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        let entries: Vec<ManifestEntryRaw> = (0..count)
            .map(|i| make_entry(i as u64 + 1, &format!("shard_{i}.db")))
            .collect();

        write_manifest(&cpath, &entries, hdr(5, 2));
        assert!(path.exists());

        let (out, header) = read_file(&cpath).unwrap().unwrap();
        assert_eq!(header, hdr(5, 2), "count={count}");
        assert_eq!(out.len(), count);
        for (i, e) in out.iter().enumerate() {
            assert_eq!((e.max_lsn, e.level, e.guard_key), (i as u64 + 1, 1, 42));
        }
    }
}

#[test]
fn read_file_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("DOES_NOT_EXIST");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    assert!(
        read_file(&cpath).unwrap().is_none(),
        "missing manifest file reads as the empty manifest"
    );
}

/// A serialized manifest for `count` entries, ready to forge against.
fn serialized(count: usize) -> Vec<u8> {
    let entries: Vec<ManifestEntryRaw> = (0..count)
        .map(|i| make_entry(100 + i as u64, &format!("shard_7_{i}.db")))
        .collect();
    let mut buf = vec![0u8; serialized_size(count)];
    serialize(&mut buf, &entries, hdr(11, 5)).unwrap();
    buf
}

#[test]
fn roundtrips_at_zero_one_and_many_entries() {
    for count in [0usize, 1, 5] {
        let buf = serialized(count);
        assert_eq!(buf.len(), serialized_size(count));
        let (out, header) = parse(&buf).unwrap();
        assert_eq!(header, hdr(11, 5), "count={count}");
        assert_eq!(out.len(), count);
        for (i, e) in out.iter().enumerate() {
            assert_eq!(e.max_lsn, 100 + i as u64);
            assert_eq!(e.filename_str(), format!("shard_7_{i}.db"));
            assert_eq!((e.level, e.guard_key), (1, 42));
        }
    }
}

#[test]
fn forged_entry_count_up_reports_truncated() {
    // The length check runs ahead of the digest, which is what pins `parse`'s
    // order: a genuinely short file must not report a hash mismatch.
    let mut buf = serialized(5);
    write_u64_le(&mut buf, OFF_ENTRY_COUNT, 6);
    assert_eq!(parse(&buf).unwrap_err(), StorageError::Truncated);
}

#[test]
fn forged_entry_count_down_reports_checksum_mismatch() {
    // The count field is inside the digest. Were it not, a shrunk count
    // would drop shards from the live set, and `gc_orphans` unlinks any
    // shard the loaded manifest does not name.
    let mut buf = serialized(5);
    write_u64_le(&mut buf, OFF_ENTRY_COUNT, 4);
    assert_eq!(parse(&buf).unwrap_err(), StorageError::ChecksumMismatch);
}

#[test]
fn trailing_bytes_report_checksum_mismatch() {
    // The neighbouring case the count field cannot close: bytes appended to
    // an otherwise honest manifest. The digest spans the whole buffer.
    let mut buf = serialized(2);
    buf.extend_from_slice(&[0u8; 16]);
    assert_eq!(parse(&buf).unwrap_err(), StorageError::ChecksumMismatch);
}

#[test]
fn every_byte_past_the_count_field_is_inside_the_digest() {
    // Filenames, levels, guard keys, LSNs and the header counters have no
    // other check, so the sweep is over every byte rather than a chosen few.
    // It starts past the count field, whose forgeries split between
    // `Truncated` and `ChecksumMismatch` and have their own tests above.
    let mut buf = serialized(3);
    let span = OFF_COMPACT_SEQ..buf.len();
    sweep_bit_flips(&mut buf, span, |byte, bit, buf| {
        assert_eq!(
            parse(buf).unwrap_err(),
            StorageError::ChecksumMismatch,
            "byte {byte} bit {bit}"
        );
    });
}

#[test]
fn peek_header_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("MANIFEST_GEN");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    // Absent file ⇒ Ok(None).
    assert_eq!(peek_header(&cpath).unwrap(), None);

    // The two sequence fields are independent and both round-trip.
    let full = ManifestHeader {
        compact_seq: 3,
        checkpoint_gen: 42,
        layout_seq: 7,
    };
    write_manifest(&cpath, &[make_entry(1, "shard_1.db")], full);
    assert_eq!(peek_header(&cpath).unwrap(), Some(full));

    // Republish at generation 0 (the base-round stamp).
    write_manifest(&cpath, &[make_entry(1, "shard_1.db")], hdr(3, 0));
    assert_eq!(peek_header(&cpath).unwrap().unwrap().checkpoint_gen, 0);
}
