use super::codec::encode_schema_block;
use super::types::{Schema, ZSetBatch};
use super::WireConflictMode;
use gnitz_wire::control::{encode_frame_head, frame_head_size, ControlHeader};
use gnitz_wire::txn_frame::PushTxnItem;
use gnitz_wire::Regions;

/// One frame payload, without the 4-byte length prefix. An empty batch ships no
/// data block, and still ships its schema block.
pub fn encode_frame(hdr: ControlHeader, blob: &[u8], schema: Option<&Schema>, data: Option<&ZSetBatch>) -> Vec<u8> {
    let schema = schema.map(encode_schema_block);
    let data = data.filter(|b| !b.is_empty()).map(|b| (b.len(), b.wire_regions()));
    let head = frame_head_size(blob.len(), schema.as_ref().map(Vec::len));
    let mut out = Vec::with_capacity(head + data.as_ref().map_or(0, |(_, r)| gnitz_wire::wal::block_size(r)));
    out.resize(head, 0);
    let written = encode_frame_head(&mut out, &hdr, blob, schema.as_deref(), data.is_some());
    debug_assert_eq!(written, head, "frame_head_size must size its own write");
    if let Some((rows, regions)) = &data {
        gnitz_wire::wal::append_block(*rows, regions, &mut out);
    }
    out
}

/// One family of a user-table push transaction: a batch into `tid` under
/// `mode`.
pub struct PushFamily<'a> {
    pub tid: u64,
    pub schema: &'a Schema,
    pub batch: &'a ZSetBatch,
    pub mode: WireConflictMode,
    /// The commit fails if `tid` was written after `basis`, the watermark of the
    /// read this family was built from; `BLIND` for a family built from no read.
    pub basis: u64,
}

/// Encode an atomic user-table push transaction frame (`ClientVerb::PushTxn`) into
/// wire bytes (without the 4-byte frame header).
pub fn encode_push_txn(families: &[PushFamily<'_>]) -> Vec<u8> {
    let schemas: Vec<Vec<u8>> = families.iter().map(|f| encode_schema_block(f.schema)).collect();
    let items: Vec<PushTxnItem<'_, (usize, Regions<'_>)>> = families
        .iter()
        .zip(&schemas)
        .map(|(f, schema_block)| PushTxnItem {
            tid: f.tid,
            mode: f.mode,
            basis: f.basis,
            schema_block,
            data: (f.batch.len(), f.batch.wire_regions()),
        })
        .collect();
    gnitz_wire::txn_frame::encode_push_txn(&items)
}

/// Encode an atomic DDL transaction frame (`ClientVerb::DdlTxn`) into wire bytes
/// (without the 4-byte frame header). Each family is named by its system table id
/// alone; its batch carries its own layout, which `Session::submit` validates
/// against that id's system schema.
pub fn encode_ddl_txn(families: &[(u64, ZSetBatch)]) -> Vec<u8> {
    let blocks: Vec<(u64, usize, Regions<'_>)> = families
        .iter()
        .map(|(tid, b)| (*tid, b.len(), b.wire_regions()))
        .collect();
    gnitz_wire::txn_frame::encode_ddl_txn(&blocks)
}

#[cfg(test)]
#[path = "tests/message.rs"]
mod tests;
