use super::super::batch::{REG_PAYLOAD_START, REG_WEIGHT};
use super::*;
use crate::schema::type_code;

#[test]
fn decode_from_wal_block_rejects_mismatched_pk_stride() {
    let schema = crate::test_support::pk_i64_schema(type_code::U64); // pk_stride = 8
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk(42u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &7i64.to_le_bytes());
    b.count += 1;

    let sz = b.wire_byte_size();
    let mut buf = vec![0u8; sz];
    b.encode_to_wire(1, &mut buf, 0, false);
    // Corrupt the REG_PK (region 0) size directory entry: claim 24 bytes.
    let size_off = gnitz_wire::wal::dir_entry_offset(REG_PK) + 4;
    buf[size_off..size_off + 4].copy_from_slice(&24u32.to_le_bytes());
    let r = Batch::decode_from_wal_block(&buf, &schema, false);
    assert_eq!(r.err(), Some("data WAL region size mismatch"));
}

#[test]
fn decode_from_wal_block_rejects_mismatched_weight_region() {
    let schema = crate::test_support::pk_i64_schema(type_code::U64);
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk(42u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &7i64.to_le_bytes());
    b.count += 1;

    let sz = b.wire_byte_size();
    let mut buf = vec![0u8; sz];
    b.encode_to_wire(1, &mut buf, 0, false);
    // Corrupt the REG_WEIGHT (region 1) size: claim 4 bytes instead of 8.
    let size_off = gnitz_wire::wal::dir_entry_offset(REG_WEIGHT) + 4;
    buf[size_off..size_off + 4].copy_from_slice(&4u32.to_le_bytes());
    let r = Batch::decode_from_wal_block(&buf, &schema, false);
    assert_eq!(r.err(), Some("data WAL region size mismatch"));
}

#[test]
fn decode_from_wal_block_rejects_region_offset_past_block() {
    let schema = crate::test_support::pk_i64_schema(type_code::U64);
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk(42u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &7i64.to_le_bytes());
    b.count += 1;

    let sz = b.wire_byte_size();
    let mut buf = vec![0u8; sz];
    b.encode_to_wire(1, &mut buf, 0, false);
    // Corrupt the REG_PK (region 0) OFFSET directory entry: point it past the
    // block while leaving its size (= n*pk_stride) schema-valid. The region's
    // [off, off + sz) now overruns the block, so `validate_and_parse` rejects
    // it instead of the decoder silently zero-filling the PK column.
    let block_end = buf.len() as u32;
    let off_off = gnitz_wire::wal::dir_entry_offset(REG_PK);
    buf[off_off..off_off + 4].copy_from_slice(&block_end.to_le_bytes());
    // verify_checksum = false: the unverified IPC path is the one this guards.
    let r = Batch::decode_from_wal_block(&buf, &schema, false);
    assert_eq!(r.err(), Some("data WAL block invalid"));
}

#[test]
fn decode_mem_batch_rejects_blob_region_past_block() {
    let schema = crate::test_support::pk_i64_schema(type_code::U64);
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk(42u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &7i64.to_le_bytes());
    b.count += 1;

    let sz = b.wire_byte_size();
    let mut buf = vec![0u8; sz];
    b.encode_to_wire(1, &mut buf, 0, false);
    // Fixed regions stay schema-valid; corrupt only the variable-length BLOB
    // region (index REG_PAYLOAD_START + npc) so [off, off + sz) overruns the
    // block: offset = block end, size = 8. `validate_and_parse` rejects the
    // OOB extent, so the decoder never resolves strings against an empty heap.
    let blob_r = REG_PAYLOAD_START + schema.num_payload_cols();
    let entry = gnitz_wire::wal::dir_entry_offset(blob_r);
    let block_end = buf.len() as u32;
    buf[entry..entry + 4].copy_from_slice(&block_end.to_le_bytes()); // offset
    buf[entry + 4..entry + 8].copy_from_slice(&8u32.to_le_bytes()); // size
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let r = decode_mem_batch_from_wal_block(&buf, &schema, &mut offsets);
    assert_eq!(r.err(), Some("data WAL block invalid"));
}

#[test]
#[should_panic(expected = "wire-safe schemas")]
fn encode_range_to_wire_panics_on_nonempty_blob() {
    let schema = crate::test_support::pk_i64_schema(type_code::U64);
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &7i64.to_le_bytes());
    b.count += 1;
    // A non-empty heap on a wire-safe encode must fail loudly, not vanish.
    b.blob.push(0xAB);
    let mut out = vec![0u8; b.wire_byte_size() + 16];
    b.encode_range_to_wire(0, 1, 1, &mut out, 0, false);
}
