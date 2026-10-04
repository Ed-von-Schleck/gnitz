use super::types::{Schema, ZSetBatch};
use gnitz_wire::control::{append_frame, ControlHeader};
use gnitz_wire::txn_frame::{encode_items, FrameItem};
use gnitz_wire::WireConflictMode;
use gnitz_wire::{ClientVerb, WireFlags};

/// One frame payload, without the 4-byte length prefix. An empty batch ships no
/// data block, and still ships the schema record.
pub fn encode_frame(hdr: ControlHeader, blob: &[u8], schema: Option<&[u8]>, data: Option<&ZSetBatch>) -> Vec<u8> {
    let mut out = Vec::new();
    append_frame(
        &mut out,
        &hdr,
        blob,
        schema,
        data.filter(|b| !b.is_empty()).map(|b| b.wire_regions()).as_deref(),
    );
    out
}

/// One family of a user-table push transaction: a batch into `target` under
/// `mode`.
pub struct PushFamily<'a> {
    pub target: crate::Target,
    pub schema: &'a Schema,
    pub batch: &'a ZSetBatch,
    pub mode: WireConflictMode,
    /// The commit fails if `target` was written after `basis`, the watermark of the
    /// read this family was built from; `BLIND` for a family built from no read.
    pub basis: u64,
}

/// Encode an atomic user-table push transaction frame (`ClientVerb::PushTxn`) into
/// wire bytes (without the 4-byte frame header). Every family carries its schema
/// record, which the master validates it against.
pub fn encode_push_txn(families: &[PushFamily<'_>]) -> Vec<u8> {
    let schemas: Vec<Vec<u8>> = families.iter().map(|f| f.schema.to_block()).collect();
    let items: Vec<FrameItem> = families
        .iter()
        .zip(&schemas)
        .map(|(f, schema)| FrameItem {
            hdr: ControlHeader {
                flags: WireFlags {
                    conflict_mode: f.mode,
                    ..Default::default()
                },
                ..ControlHeader::naming(ClientVerb::PushTxn, f.target, f.basis)
            },
            schema: Some(schema),
            data: Some(f.batch.wire_regions()),
        })
        .collect();
    encode_items(ClientVerb::PushTxn, &items)
}

/// Encode an atomic DDL transaction frame (`ClientVerb::DdlTxn`) into wire bytes
/// (without the 4-byte frame header). Each family is named by its system table id
/// alone; its batch carries its own layout, which `Session::submit` validates
/// against that id's system schema.
pub fn encode_ddl_txn(families: &[(u64, ZSetBatch)]) -> Vec<u8> {
    let items: Vec<FrameItem> = families
        .iter()
        .map(|(tid, b)| FrameItem {
            hdr: ControlHeader { target_id: *tid, ..Default::default() },
            schema: None,
            data: Some(b.wire_regions()),
        })
        .collect();
    encode_items(ClientVerb::DdlTxn, &items)
}

#[cfg(test)]
#[path = "tests/message.rs"]
mod tests;
