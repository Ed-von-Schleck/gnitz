use super::*;

/// A system family's column shape is part of what this word guards, and nothing
/// else notices it changing — so pin it. If this fails, bump
/// `WAL_FORMAT_VERSION`, then paste the reported digest here. The engine's
/// `storage::repr::layout` pins the same digest to `SHARD_VERSION`; bump that
/// too.
#[test]
fn stored_shape_is_pinned_to_the_format_words() {
    assert_eq!(
        (crate::SYS_SCHEMA_DIGEST, WAL_FORMAT_VERSION),
        (14684290124813841089, 10),
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

#[test]
fn encode_decode_roundtrip() {
    let regions = make_test_regions();
    let mut buf = vec![0u8; 4096];

    let block_len = encode(&mut buf, 0, 7, 2, &as_slices(&regions), true).unwrap();

    let mut offsets = [0u64; 16];
    let mut rsizes = [0u32; 16];
    let h = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, true).unwrap();
    assert_eq!(h.table_id, 7);
    assert_eq!(h.entry_count, 2);
    assert_eq!(h.num_regions, 3);
    assert_eq!(h.total_size, block_len);

    // Verify region data
    for (i, region) in regions.iter().enumerate() {
        let off = offsets[i] as usize;
        let sz = rsizes[i] as usize;
        assert_eq!(sz, 16);
        assert_eq!(&buf[off..off + sz], &region[..]);
    }
}

#[test]
fn encode_append_semantics() {
    let regions = make_test_regions();
    let slices = as_slices(&regions);
    let mut buf = vec![0u8; 8192];

    // Encode two blocks sequentially (like IPC does)
    let off1 = encode(&mut buf, 0, 10, 2, &slices, true).unwrap();
    let off2 = encode(&mut buf, off1, 20, 2, &slices, true).unwrap();
    assert!(off2 > off1);

    // Decode both blocks
    let mut offsets = [0u64; 16];
    let mut rsizes = [0u32; 16];

    let h1 = validate_and_parse(&buf[..off1], &mut offsets, &mut rsizes, true).unwrap();
    assert_eq!(h1.table_id, 10);

    let h2 = validate_and_parse(&buf[off1..off2], &mut offsets, &mut rsizes, true).unwrap();
    assert_eq!(h2.table_id, 20);
}

#[test]
fn alignment() {
    // Region of 5 bytes should still result in 8-byte aligned next region
    let r0 = [1u8, 2, 3, 4, 5];
    let r1 = [6u8, 7, 8, 9];
    let regions: [&[u8]; 2] = [&r0, &r1];
    let mut buf = vec![0u8; 4096];

    let new_offset = encode(&mut buf, 0, 0, 2, &regions, true).unwrap();

    let mut offsets = [0u64; 16];
    let mut rsizes = [0u32; 16];
    validate_and_parse(&buf[..new_offset], &mut offsets, &mut rsizes, true).unwrap();

    // Second region offset must be 8-byte aligned
    assert_eq!(offsets[1] % 8, 0, "region 1 not 8-byte aligned: {}", offsets[1]);
}

#[test]
fn bad_version() {
    let mut buf = vec![0u8; WAL_HEADER_SIZE];
    write_u32_le(&mut buf, WAL_OFF_VERSION, 99);
    write_u32_le(&mut buf, WAL_OFF_SIZE, WAL_HEADER_SIZE as u32);

    let rc = validate_and_parse(&buf, &mut [], &mut [], true);
    assert_eq!(rc, Err(WalError::InvalidVersion));
}

#[test]
fn bad_checksum() {
    let regions = make_test_regions();
    let mut buf = vec![0u8; 4096];
    let new_offset = encode(&mut buf, 0, 1, 2, &as_slices(&regions), true).unwrap();

    // Corrupt one byte in the body
    buf[WAL_HEADER_SIZE + 1] ^= 0xFF;

    let rc = validate_and_parse(&buf[..new_offset], &mut [], &mut [], true);
    assert_eq!(rc, Err(WalError::ChecksumMismatch));
}

#[test]
fn truncated_block() {
    // Too short for header
    let rc = validate_and_parse(&[0u8; 10], &mut [], &mut [], true);
    assert_eq!(rc, Err(WalError::Truncated));
}

#[test]
fn empty_regions() {
    let mut buf = vec![0u8; 4096];
    let new_offset = encode(&mut buf, 0, 1, 0, &[], true).unwrap();
    assert_eq!(new_offset, WAL_HEADER_SIZE); // just a header, no directory, no data

    let h = validate_and_parse(&buf[..new_offset], &mut [], &mut [], true).unwrap();
    assert_eq!(h.num_regions, 0);
}

#[test]
fn rejects_overlong_region_count() {
    // A forged block whose region count exceeds MAX_WIRE_REGIONS (or the
    // caller's out slices) must be rejected, never silently clamped.
    let mut buf = vec![0u8; 4096];
    let block_len = encode(&mut buf, 0, 1, 1, &[], false).unwrap();
    write_u32_le(&mut buf, WAL_OFF_NUM_REGIONS, (MAX_WIRE_REGIONS + 1) as u32);

    let mut offsets = [0u64; MAX_WIRE_REGIONS + 1];
    let mut rsizes = [0u32; MAX_WIRE_REGIONS + 1];
    let rc = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, false);
    assert_eq!(rc, Err(WalError::InvalidShard));
}

#[test]
fn rejects_region_extent_past_block() {
    // A directory entry whose [offset, offset + size) runs past total_size
    // must be rejected, not silently accepted — decoders rely on this to
    // avoid zero-filling an out-of-bounds region. checksum=false isolates
    // the extent check from the (directory-covering) checksum.
    let regions = make_test_regions();
    let mut buf = vec![0u8; 4096];
    let block_len = encode(&mut buf, 0, 1, 2, &as_slices(&regions), false).unwrap();

    // Point region 0's offset at the block end; its size (16) now overruns.
    write_u32_le(&mut buf, WAL_HEADER_SIZE, block_len as u32);

    let mut offsets = [0u64; 16];
    let mut rsizes = [0u32; 16];
    let rc = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, false);
    assert_eq!(rc, Err(WalError::Truncated));
}

#[test]
fn buffer_too_small() {
    let r0 = [0u8; 100];
    let regions: [&[u8]; 1] = [&r0];
    let mut buf = vec![0u8; 64]; // too small

    let rc = encode(&mut buf, 0, 0, 0, &regions, true);
    assert_eq!(rc, Err(WalError::BufferTooSmall));
}
