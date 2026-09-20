use super::*;

/// Nothing but this pin notices a system family's column shape changing. On a
/// failure, bump `WAL_FORMAT_VERSION` and paste the reported digest. The shard
/// word needs no pin — it carries the digest itself.
#[test]
fn stored_shape_is_pinned_to_the_format_words() {
    assert_eq!(
        (crate::SYS_SCHEMA_DIGEST, WAL_FORMAT_VERSION),
        (12444786633684072045, 25),
        "system-family column shapes changed"
    );
}

fn make_test_regions() -> Vec<Vec<u8>> {
    // 3 regions: pk_lo (16B), pk_hi (16B), weight (16B) — simulating 2 rows
    let r0 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
    let r1 = vec![0u8; 16];
    let r2 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
    vec![r0, r1, r2]
}

fn wal_block<'a>(table_id: u32, entry_count: u32, regions: &[&'a [u8]]) -> WalBlock<'a> {
    let mut list = Regions::new();
    for r in regions {
        list.push(r);
    }
    WalBlock { table_id, entry_count, regions: list }
}

/// Frame `regions` into a fresh buffer, as a SAL slot would.
fn encode(table_id: u32, entry_count: u32, regions: &[&[u8]], checksum_body: bool) -> Vec<u8> {
    let b = wal_block(table_id, entry_count, regions);
    let mut buf = vec![0u8; b.size()];
    b.write(&mut buf, checksum_body);
    buf
}

/// The header fields and every region's bytes come back, a second block appends
/// behind the first, and an odd-length region leaves no gap behind it.
#[test]
fn encode_and_parse_roundtrip_append_and_alignment() {
    let regions = make_test_regions();
    let slices: Vec<&[u8]> = regions.iter().map(Vec::as_slice).collect();

    let mut buf = encode(7, 2, &slices, true);
    let end1 = buf.len();
    let (h, parsed) = validate_and_parse(&buf, true).unwrap();
    assert_eq!(h.table_id, 7);
    assert_eq!(h.entry_count, 2);
    assert_eq!(h.num_regions, 3);
    assert_eq!(h.total_size, end1);
    assert_eq!(&*parsed, &slices[..]);

    // A second block appends behind the first, as the IPC paths write them, and
    // is recovered by its own SIZE field rather than by a remembered offset.
    buf.extend_from_slice(&encode(20, 2, &slices, true));
    for (off, want_tid) in [(0, 7u32), (end1, 20)] {
        let block = block_slice_at(&buf, off).expect("a framed block");
        let (h, _) = validate_and_parse(block, true).unwrap();
        assert_eq!(h.table_id, want_tid);
    }

    // Regions pack end to end: a 5-byte region leaves no gap behind it, so the
    // block is exactly its directory plus its region bytes.
    let (r0, r1) = ([1u8, 2, 3, 4, 5], [6u8, 7, 8, 9]);
    let buf = encode(0, 2, &[&r0, &r1], true);
    validate_and_parse(&buf, true).unwrap();
    let (off0, off1) = (dir_entry(&buf, 0).0, dir_entry(&buf, 1).0);
    assert_eq!(off0, body_start(2));
    assert_eq!(off1, off0 + r0.len(), "regions pack end to end");
    assert_eq!(buf.len(), body_start(2) + r0.len() + r1.len());
}

/// The appending and the in-place framer produce the same bytes.
#[test]
fn append_to_matches_write() {
    let regions = make_test_regions();
    let slices: Vec<&[u8]> = regions.iter().map(Vec::as_slice).collect();
    let (odd, empty) = ([1u8, 2, 3], []);
    for list in [&slices[..], &[&odd, &empty, &odd], &[]] {
        let b = wal_block(9, 2, list);
        let mut appended = vec![0xAB];
        b.append_to(&mut appended);
        assert_eq!(appended[1..], encode(9, 2, list, false), "{} regions", list.len());
    }
}

/// Every framer guard, against the block that trips it. The error kind is what
/// separates them: a merged table asserting only "an error" would pass on the
/// wrong guard, and each of these reaches a different consumer.
#[test]
fn each_framer_guard_mints_its_own_error() {
    let regions = make_test_regions();
    let slices: Vec<&[u8]> = regions.iter().map(Vec::as_slice).collect();

    let bad_version = {
        let mut buf = vec![0u8; WAL_HEADER_SIZE];
        write_u32_le(&mut buf, WAL_OFF_VERSION, 99);
        write_u32_le(&mut buf, WAL_OFF_SIZE, WAL_HEADER_SIZE as u32);
        buf
    };
    let bad_checksum = {
        let mut buf = encode(1, 2, &slices, true);
        buf[WAL_HEADER_SIZE + 1] ^= 0xFF; // one corrupt body byte
        buf
    };
    // A region count past `MAX_WIRE_REGIONS` must be rejected, never clamped.
    let overlong_region_count = {
        let mut buf = encode(1, 1, &[], false);
        write_u32_le(&mut buf, WAL_OFF_NUM_REGIONS, (MAX_WIRE_REGIONS + 1) as u32);
        buf
    };
    // A directory entry whose [offset, offset + size) runs past `total_size`:
    // decoders index each region without a further bounds check, so this cannot
    // be accepted. Region 0's offset is pushed to the block end, where its size
    // (16) overruns.
    let extent_past_block = {
        let mut buf = encode(1, 2, &slices, false);
        let end = buf.len() as u32;
        write_u32_le(&mut buf, dir_entry_offset(REG_PK), end);
        buf
    };

    // The two forged blocks above are checksum=false, which isolates the guard
    // under test from the (directory-covering) checksum.
    let cases: &[(&str, Vec<u8>, bool, WalError)] = &[
        ("short of a header", vec![0u8; 10], true, WalError::Truncated),
        ("unknown version", bad_version, true, WalError::InvalidVersion),
        ("corrupt body", bad_checksum, true, WalError::ChecksumMismatch),
        (
            "overlong region count",
            overlong_region_count,
            false,
            WalError::InvalidShard,
        ),
        (
            "region extent past block",
            extent_past_block,
            false,
            WalError::Truncated,
        ),
    ];
    for (what, block, verify, want) in cases {
        assert_eq!(validate_and_parse(block, *verify).err(), Some(*want), "{what}");
    }
}

#[test]
fn empty_regions() {
    let buf = encode(1, 0, &[], true);
    assert_eq!(buf.len(), WAL_HEADER_SIZE); // just a header, no directory, no data

    let (h, regions) = validate_and_parse(&buf, true).unwrap();
    assert_eq!(h.num_regions, 0);
    assert!(regions.is_empty());
}
