use super::codec::encode_schema_block;
use super::types::{Schema, ZSetBatch};
use super::WireConflictMode;
use gnitz_wire::control::{encode_frame_head, frame_head_size, ControlHeader};
use gnitz_wire::wal::WalBlock;

/// One frame payload, without the 4-byte length prefix. An empty batch ships no
/// data block, and still ships its schema block.
pub fn encode_frame(hdr: ControlHeader, blob: &[u8], schema: Option<&Schema>, data: Option<&ZSetBatch>) -> Vec<u8> {
    let tid = hdr.target_id;
    let schema = schema.map(|s| encode_schema_block(s, tid as u32));
    let data = data.filter(|b| !b.is_empty()).map(|b| b.wal_block(tid));
    let head = frame_head_size(blob.len(), schema.as_ref().map_or(0, Vec::len));
    let mut out = Vec::with_capacity(head + data.as_ref().map_or(0, WalBlock::size));
    out.resize(head, 0);
    let written = encode_frame_head(&mut out, &hdr, blob, schema.as_deref(), data.is_some());
    debug_assert_eq!(written, head, "frame_head_size must size its own write");
    if let Some(d) = &data {
        d.append_to(&mut out);
    }
    out
}

/// Encode an atomic user-table push transaction frame (`ClientVerb::PushTxn`) into
/// wire bytes (without the 4-byte frame header).
pub fn encode_push_txn(
    families: &[(u64, &Schema, &ZSetBatch, WireConflictMode)],
    preconditions: &[(u64, u64)],
) -> Vec<u8> {
    let families: Vec<(WireConflictMode, Vec<u8>, WalBlock<'_>)> = families
        .iter()
        .map(|(tid, schema, batch, mode)| (*mode, encode_schema_block(schema, *tid as u32), batch.wal_block(*tid)))
        .collect();
    gnitz_wire::txn_frame::encode_push_txn(&families, preconditions)
}

/// Encode an atomic DDL transaction frame (`ClientVerb::DdlTxn`) into wire bytes
/// (without the 4-byte frame header). Each family is named by its system table id
/// alone; its batch carries its own layout, which `Session::submit` validates
/// against that id's system schema.
pub fn encode_ddl_txn(families: &[(u64, ZSetBatch)]) -> Vec<u8> {
    let blocks: Vec<WalBlock<'_>> = families.iter().map(|(tid, b)| b.wal_block(*tid)).collect();
    gnitz_wire::txn_frame::encode_ddl_txn(&blocks)
}

#[cfg(test)]
#[path = "tests/message.rs"]
mod tests;
