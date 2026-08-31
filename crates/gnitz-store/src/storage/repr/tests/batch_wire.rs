use super::super::batch::{REG_PAYLOAD_START, REG_WEIGHT};
use super::*;
use crate::schema::{type_code, SchemaDescriptor};
use crate::test_support::{make_batch_raw, pk_payload_schema};

/// A one-row batch encoded to a WAL block, ready to have its directory forged.
fn encoded_block(schema: &SchemaDescriptor) -> Vec<u8> {
    make_batch_raw(schema, &[(42, 1, 7)]).encode_to_wire_vec(1, false)
}

/// A fixed region whose directory size disagrees with what the schema implies
/// for the row count is refused, rather than decoded against a mis-sized region.
#[test]
fn decode_from_wal_block_rejects_mismatched_region_sizes() {
    let schema = pk_payload_schema(&[type_code::U64]); // pk_stride = 8
    for (region, forged) in [(REG_PK, 24u32), (REG_WEIGHT, 4)] {
        let mut buf = encoded_block(&schema);
        let size_off = gnitz_wire::wal::dir_entry_offset(region) + 4;
        buf[size_off..size_off + 4].copy_from_slice(&forged.to_le_bytes());
        let r = Batch::decode_from_wal_block(&buf, &schema, false);
        assert_eq!(r.err(), Some("data WAL region size mismatch"), "region {region}");
    }
}

/// `COUNT` and `NUM_REGIONS` live in the 32-byte header, outside the block's
/// own checksum, so `n -> 0` is a single-bit flip whenever `n` is a power of
/// two. Only the exact region-size relation rejects them. A genuinely empty
/// block is the negative control: `count == 0` with regions to match decodes.
#[test]
fn decode_from_wal_block_rejects_header_count_forgeries() {
    let schema = pk_payload_schema(&[type_code::U64]);
    let clean = make_batch_raw(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]).encode_to_wire_vec(7, true);
    for forged in [0u32, 2, 1000] {
        let mut buf = clean.clone();
        gnitz_wire::write_u32_le(&mut buf, gnitz_wire::WAL_OFF_COUNT, forged);
        assert_eq!(
            Batch::decode_from_wal_block(&buf, &schema, true).err(),
            Some("data WAL region size mismatch"),
            "COUNT 3 -> {forged} must be rejected by the exact region-size relation"
        );
    }

    let mut buf = clean.clone();
    gnitz_wire::write_u32_le(&mut buf, gnitz_wire::WAL_OFF_NUM_REGIONS, 4);
    assert!(
        Batch::decode_from_wal_block(&buf, &schema, true).is_err(),
        "a forged region count must be rejected"
    );

    let empty = Batch::empty_with_schema(&schema).encode_to_wire_vec(7, true);
    let (decoded, _) = Batch::decode_from_wal_block(&empty, &schema, true).expect("an empty block decodes");
    assert_eq!(decoded.count, 0);
}

/// The variable-length blob region is the one whose extent the schema cannot
/// predict. When `[off, off + size)` overruns the block, the decoder must never
/// reach the point of resolving strings against a heap that is not there.
/// `verify_checksum = false`: the unverified IPC path is the one this guards.
#[test]
fn decode_mem_batch_rejects_blob_region_past_block() {
    let schema = pk_payload_schema(&[type_code::U64]);
    let mut buf = encoded_block(&schema);
    let entry = gnitz_wire::wal::dir_entry_offset(REG_PAYLOAD_START + schema.num_payload_cols());
    let block_end = buf.len() as u32;
    buf[entry..entry + 4].copy_from_slice(&block_end.to_le_bytes());
    buf[entry + 4..entry + 8].copy_from_slice(&8u32.to_le_bytes());

    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let r = decode_mem_batch_from_wal_block(&buf, &schema, &mut offsets);
    assert_eq!(r.err(), Some("data WAL block invalid"));
}

#[test]
#[should_panic(expected = "no German string")]
fn encode_range_to_wire_panics_on_nonempty_blob() {
    let schema = pk_payload_schema(&[type_code::U64]);
    let mut b = make_batch_raw(&schema, &[(1, 1, 7)]);
    // A non-empty heap on a range encode must fail loudly, not vanish.
    b.blob.push(0xAB);
    let mut out = vec![0u8; b.wire_byte_size() + 16];
    b.encode_range_to_wire(0, 1, 1, &mut out, 0, false);
}
