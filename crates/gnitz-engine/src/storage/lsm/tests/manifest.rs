use super::*;

fn hdr(compact_seq: u64, checkpoint_gen: u64) -> ManifestHeader {
    ManifestHeader {
        compact_seq,
        checkpoint_gen,
        layout_seq: 0,
    }
}

#[test]
fn parse_rejects_count_overflow() {
    // A corrupt header whose count * ENTRY_SIZE overflows usize must be
    // rejected, not wrap past the length check.
    let mut buf = vec![0u8; HEADER_SIZE];
    write_u64_le(&mut buf, 0, MAGIC);
    write_u64_le(&mut buf, 8, VERSION);
    write_u64_le(&mut buf, 16, u64::MAX); // count
    let r = parse(&buf);
    assert!(matches!(r, Err(StorageError::Truncated)));
}

fn make_entry(max_lsn: u64, name: &str) -> ManifestEntryRaw {
    ManifestEntryRaw::new(name, max_lsn, 1, 42)
}

#[test]
fn old_version_rejected() {
    // Any non-current version must be rejected outright — no legacy reader.
    let mut buf = vec![0u8; HEADER_SIZE];
    write_u64_le(&mut buf, 0, MAGIC);
    write_u64_le(&mut buf, 8, VERSION - 1);
    write_u64_le(&mut buf, 16, 0);

    assert_eq!(parse(&buf).unwrap_err(), StorageError::InvalidVersion);
}

#[test]
fn bad_magic() {
    let mut buf = vec![0u8; HEADER_SIZE];
    write_u64_le(&mut buf, 0, 0xDEADBEEF);

    assert_eq!(parse(&buf).unwrap_err(), StorageError::InvalidMagic);
}

#[test]
fn truncated() {
    assert_eq!(parse(&[0u8; 10]).unwrap_err(), StorageError::Truncated);
}

#[test]
fn buffer_too_small() {
    let entries = vec![make_entry(1, "test.db")];
    let mut buf = vec![0u8; 32]; // too small for header + entry
    assert_eq!(
        serialize(&mut buf, &entries, hdr(0, 0)),
        Err(StorageError::BufferTooSmall)
    );
}

#[test]
fn empty_manifest() {
    let mut buf = vec![0u8; HEADER_SIZE];
    let written = serialize(&mut buf, &[], hdr(42, 0)).unwrap();
    assert_eq!(written, HEADER_SIZE);

    let (out, header) = parse(&buf).unwrap();
    assert!(out.is_empty());
    assert_eq!(header.compact_seq, 42);
}

#[test]
fn filename_null_terminated() {
    let e = make_entry(1, "hello.db");
    let mut buf = vec![0u8; serialized_size(1)];
    serialize(&mut buf, &[e], hdr(0, 0)).unwrap();

    let (out, _) = parse(&buf).unwrap();

    // Extract filename
    let end = out[0].filename.iter().position(|&b| b == 0).unwrap_or(128);
    let name = std::str::from_utf8(&out[0].filename[..end]).unwrap();
    assert_eq!(name, "hello.db");
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
    let path = dir.path().join("MANIFEST");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    let entries = vec![
        make_entry(1, "shard_1.db"),
        make_entry(2, "shard_2.db"),
        make_entry(3, "shard_3.db"),
    ];

    write_manifest(&cpath, &entries, hdr(5, 2));
    assert!(path.exists());

    let (out, header) = read_file(&cpath).unwrap().unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(
        header,
        hdr(5, 2),
        "header must round-trip through prepare_file/read_file"
    );
    assert_eq!(out[0].max_lsn, 1);
    assert_eq!(out[1].max_lsn, 2);
    assert_eq!(out[2].max_lsn, 3);
    assert_eq!(out[0].level, 1);
    assert_eq!(out[0].guard_key, 42);
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

#[test]
fn write_file_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("MANIFEST_EMPTY");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    write_manifest(&cpath, &[], hdr(42, 0));

    let (out, header) = read_file(&cpath).unwrap().unwrap();
    assert!(out.is_empty());
    assert_eq!(header.compact_seq, 42);
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
    let base = serialized(3);
    for off in OFF_COMPACT_SEQ..base.len() {
        let mut buf = base.clone();
        buf[off] ^= 0x01;
        assert_eq!(parse(&buf).unwrap_err(), StorageError::ChecksumMismatch, "byte {off}");
    }
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
