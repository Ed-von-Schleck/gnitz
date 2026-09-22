use super::codec::encode_schema_block;
use super::types::{Schema, ZSetBatch};
use super::WireConflictMode;
use gnitz_wire::control::{encode_frame_head, frame_head_size, ControlHeader};
use gnitz_wire::txn_frame::PushTxnItem;
use gnitz_wire::wal::WalBlock;

/// One frame payload, without the 4-byte length prefix. An empty batch ships no
/// data block, and still ships its schema block.
pub fn encode_frame(hdr: ControlHeader, blob: &[u8], schema: Option<&Schema>, data: Option<&ZSetBatch>) -> Vec<u8> {
    let tid = hdr.target_id;
    let schema = schema.map(encode_schema_block);
    let data = data.filter(|b| !b.is_empty()).map(|b| b.wal_block(tid));
    let head = frame_head_size(blob.len(), schema.as_ref().map(Vec::len));
    let mut out = Vec::with_capacity(head + data.as_ref().map_or(0, WalBlock::size));
    out.resize(head, 0);
    let written = encode_frame_head(&mut out, &hdr, blob, schema.as_deref(), data.is_some());
    debug_assert_eq!(written, head, "frame_head_size must size its own write");
    if let Some(d) = &data {
        d.append_to(&mut out);
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
    /// The transaction read `tid`: the commit fails if it was written after the
    /// frame's basis.
    pub reads: bool,
}

/// Encode an atomic user-table push transaction frame (`ClientVerb::PushTxn`) into
/// wire bytes (without the 4-byte frame header), under the OCC `basis`.
pub fn encode_push_txn(families: &[PushFamily<'_>], basis: u64) -> Vec<u8> {
    let schemas: Vec<Vec<u8>> = families.iter().map(|f| encode_schema_block(f.schema)).collect();
    let items: Vec<PushTxnItem<'_, WalBlock<'_>>> = families
        .iter()
        .zip(&schemas)
        .map(|(f, schema_block)| PushTxnItem {
            mode: f.mode,
            reads: f.reads,
            schema_block,
            data: f.batch.wal_block(f.tid),
        })
        .collect();
    gnitz_wire::txn_frame::encode_push_txn(basis, &items)
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
