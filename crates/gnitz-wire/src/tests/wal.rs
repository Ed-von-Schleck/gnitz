use super::*;

/// Nothing but this pin notices a system family's column shape changing. On a
/// failure, bump `WAL_FORMAT_VERSION` and paste the reported digest. The shard
/// word needs no pin — it carries the digest itself.
#[test]
fn stored_shape_is_pinned_to_the_format_words() {
    assert_eq!(
        (crate::SYS_SCHEMA_DIGEST, WAL_FORMAT_VERSION),
        (1141556639288764826, 18),
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

fn as_slices(regions: &[Vec<u8>]) -> Vec<&[u8]> {
    regions.iter().map(|r| r.as_slice()).collect()
}

/// One encode → `validate_and_parse` walk, carrying every structural claim the
/// framer makes: the header fields come back, each region's bytes are verbatim,
/// `block_size_of` predicts the length `encode` returns, a second block appends
/// behind the first and `block_slice_at` finds it by its own SIZE field, and a
/// region of odd length still leaves the next one 8-byte aligned.
#[test]
fn encode_and_parse_roundtrip_append_and_alignment() {
    let regions = make_test_regions();
    let slices = as_slices(&regions);
    let mut buf = vec![0u8; 8192];

    let end1 = encode(&mut buf, 0, 7, 2, &slices, true).unwrap();
    assert_eq!(
        end1,
        block_size_of(&slices),
        "block_size_of must predict what encode writes"
    );

    let mut offsets = [0u64; MAX_WIRE_REGIONS];
    let mut rsizes = [0u32; MAX_WIRE_REGIONS];
    let h = validate_and_parse(&buf[..end1], &mut offsets, &mut rsizes, true).unwrap();
    assert_eq!(h.table_id, 7);
    assert_eq!(h.entry_count, 2);
    assert_eq!(h.num_regions, 3);
    assert_eq!(h.total_size, end1);
    for (i, region) in regions.iter().enumerate() {
        let (off, sz) = (offsets[i] as usize, rsizes[i] as usize);
        assert_eq!(sz, region.len());
        assert_eq!(&buf[off..off + sz], &region[..]);
    }

    // A second block appends behind the first, as the IPC paths write them, and
    // is recovered by its own SIZE field rather than by a remembered offset.
    let end2 = encode(&mut buf, end1, 20, 2, &slices, true).unwrap();
    assert!(end2 > end1);
    for (off, want_tid) in [(0, 7u32), (end1, 20)] {
        let block = block_slice_at(&buf, off).expect("a framed block");
        let h = validate_and_parse(block, &mut offsets, &mut rsizes, true).unwrap();
        assert_eq!(h.table_id, want_tid);
    }

    // A 5-byte region must still leave the next one 8-byte aligned.
    let (r0, r1) = ([1u8, 2, 3, 4, 5], [6u8, 7, 8, 9]);
    let odd: [&[u8]; 2] = [&r0, &r1];
    let end = encode(&mut buf, 0, 0, 2, &odd, true).unwrap();
    validate_and_parse(&buf[..end], &mut offsets, &mut rsizes, true).unwrap();
    assert_eq!(offsets[1] % 8, 0, "region 1 not 8-byte aligned: {}", offsets[1]);
}

/// Every framer guard, against the block that trips it. The error kind is what
/// separates them: a merged table asserting only "an error" would pass on the
/// wrong guard, and each of these reaches a different consumer.
#[test]
fn each_framer_guard_mints_its_own_error() {
    let regions = make_test_regions();
    let slices = as_slices(&regions);

    let bad_version = {
        let mut buf = vec![0u8; WAL_HEADER_SIZE];
        write_u32_le(&mut buf, WAL_OFF_VERSION, 99);
        write_u32_le(&mut buf, WAL_OFF_SIZE, WAL_HEADER_SIZE as u32);
        buf
    };
    let bad_checksum = {
        let mut buf = vec![0u8; 4096];
        let end = encode(&mut buf, 0, 1, 2, &slices, true).unwrap();
        buf[WAL_HEADER_SIZE + 1] ^= 0xFF; // one corrupt body byte
        buf.truncate(end);
        buf
    };
    // A region count past `MAX_WIRE_REGIONS` must be rejected, never clamped —
    // the out slices below are deliberately long enough to hold it.
    let overlong_region_count = {
        let mut buf = vec![0u8; 4096];
        let end = encode(&mut buf, 0, 1, 1, &[], false).unwrap();
        write_u32_le(&mut buf, WAL_OFF_NUM_REGIONS, (MAX_WIRE_REGIONS + 1) as u32);
        buf.truncate(end);
        buf
    };
    // A directory entry whose [offset, offset + size) runs past `total_size`:
    // decoders index each region without a further bounds check, so this cannot
    // be accepted. Region 0's offset is pushed to the block end, where its size
    // (16) overruns.
    let extent_past_block = {
        let mut buf = vec![0u8; 4096];
        let end = encode(&mut buf, 0, 1, 2, &slices, false).unwrap();
        write_u32_le(&mut buf, dir_entry_offset(REG_PK), end as u32);
        buf.truncate(end);
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
    let mut offsets = [0u64; MAX_WIRE_REGIONS + 1];
    let mut rsizes = [0u32; MAX_WIRE_REGIONS + 1];
    for (what, block, verify, want) in cases {
        assert_eq!(
            validate_and_parse(block, &mut offsets, &mut rsizes, *verify),
            Err(*want),
            "{what}"
        );
    }

    // An out buffer that cannot hold the block is refused at encode time.
    let r0 = [0u8; 100];
    let mut small = vec![0u8; 64];
    assert_eq!(encode(&mut small, 0, 0, 0, &[&r0], true), Err(WalError::BufferTooSmall));
}

#[test]
fn empty_regions() {
    let mut buf = vec![0u8; 4096];
    let new_offset = encode(&mut buf, 0, 1, 0, &[], true).unwrap();
    assert_eq!(new_offset, WAL_HEADER_SIZE); // just a header, no directory, no data

    let h = validate_and_parse(&buf[..new_offset], &mut [], &mut [], true).unwrap();
    assert_eq!(h.num_regions, 0);
}
