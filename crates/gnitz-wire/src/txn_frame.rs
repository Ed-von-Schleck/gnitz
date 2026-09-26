//! The four **multi-item request frames** — `PUSH_TXN`, `DDL_TXN`, `SCAN_MULTI`
//! and `DELTA_POLL` — in both directions.
//!
//! All four open with a control header naming the frame's verb (`target_id =
//! 0`), and run their items to the end of the frame. They differ only in what
//! one item is:
//!
//! ```text
//! DDL_TXN     ctrl                | ([u64 tid][data block])*
//! PUSH_TXN    ctrl                | ([u64 tid][u8 mode][u64 basis][u32 len][schema record][data block])*
//! SCAN_MULTI  ctrl                | ([u64 tid][u16 schema_version])*
//! DELTA_POLL  ctrl                | ([u64 view_id][u64 after_tick][u32 len][reply block])*
//! ```
//!
//! **A reply fault's `target_id`:** a `DELTA_POLL` fault naming one of its views
//! ends that position alone; every other fault ends the request.

use crate::codec::{Reader, Writer};
use crate::control::{encode_frame_head, ControlHeader, CTRL_HEADER_SIZE};
use crate::region::Regions;
use crate::wal::block_size;
use crate::{ClientVerb, WireConflictMode, WireFlags};

/// Maximum relations in one `SCAN_MULTI`. The master holds one scan lease and
/// one reply train of bookkeeping per relation; a handful of related tables
/// covers a realistic consistent snapshot.
pub(crate) const SCAN_MULTI_MAX_RELATIONS: usize = 16;

/// Maximum views in one `DELTA_POLL`: the ceiling on the leases and reply
/// trains one poll puts on the master. A mirroring host holds tens of views, and
/// a host past this chunks into a second request.
pub const DELTA_POLL_MAX_VIEWS: usize = 64;

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// The shared prologue: a control header naming `verb`, every other field zero.
/// Each encoder below appends its own body.
fn prologue(verb: ClientVerb, body_hint: usize) -> Writer {
    let hdr = ControlHeader {
        flags: WireFlags { verb, ..Default::default() },
        ..Default::default()
    };
    let mut head = [0u8; CTRL_HEADER_SIZE];
    encode_frame_head(&mut head, &hdr, &[], None, false);
    let mut w = Writer::with_capacity(CTRL_HEADER_SIZE + body_hint);
    w.raw(&head);
    w
}

/// Encode a `DDL_TXN` frame, without the 4-byte frame header: every
/// system-table write of one DDL statement, ingested under one durable SAL zone.
/// Each block is `(tid, rows, regions)`.
pub fn encode_ddl_txn(blocks: &[(u64, usize, Regions<'_>)]) -> Vec<u8> {
    let body = blocks.iter().map(|(_, _, r)| 8 + block_size(r)).sum();
    let mut w = prologue(ClientVerb::DdlTxn, body);
    for (tid, rows, regions) in blocks {
        w.u64(*tid).block(*rows, regions);
    }
    w.into_vec()
}

/// The basis of a `PUSH_TXN` family built from no read. No read reports it, and
/// no commit exceeds it.
pub const BLIND: u64 = u64::MAX;

/// One `PUSH_TXN` family, in both directions.
pub struct PushTxnItem<'a, D> {
    /// The target relation.
    pub tid: u64,
    pub mode: WireConflictMode,
    /// The commit fails if `tid` was written after `basis`, the watermark of the
    /// read this family was built from; [`BLIND`] for a family built from no read.
    pub basis: u64,
    pub schema_block: &'a [u8],
    /// The rows and region list to encode; the framed block slice once decoded.
    pub data: D,
}

/// Encode a `PUSH_TXN` frame, without the 4-byte frame header: user-table
/// writes committed as one zone. Every family carries its schema block, so the
/// master validates it with no warm-cache version.
pub fn encode_push_txn(items: &[PushTxnItem<'_, (usize, Regions<'_>)>]) -> Vec<u8> {
    let body: usize = items
        .iter()
        .map(|f| 8 + 1 + 8 + 4 + f.schema_block.len() + block_size(&f.data.1))
        .sum();
    let mut w = prologue(ClientVerb::PushTxn, body);
    for f in items {
        w.u64(f.tid)
            .u8(f.mode.as_wire())
            .u64(f.basis)
            .bytes32(f.schema_block)
            .block(f.data.0, &f.data.1);
    }
    w.into_vec()
}

/// Encode a `SCAN_MULTI` frame, without the 4-byte frame header: N relations
/// read at one SAL cut, each `(tid, cached schema version)`, answered in order.
pub fn encode_scan_multi(relations: &[(u64, u16)]) -> Vec<u8> {
    let mut w = prologue(ClientVerb::ScanMulti, relations.len() * (8 + 2));
    for (tid, version) in relations {
        w.u64(*tid).u16(*version);
    }
    w.into_vec()
}

/// One DELTA_POLL item: every delta `view_id` recorded after round `after_tick`
/// (`0` = the whole view), in `reply_block`'s layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaPollItem<'a> {
    pub view_id: u64,
    pub after_tick: u64,
    pub reply_block: &'a [u8],
}

/// Encode a `DELTA_POLL` frame, without the 4-byte frame header.
pub fn encode_delta_poll(views: &[DeltaPollItem<'_>]) -> Vec<u8> {
    let body: usize = views.iter().map(|v| 8 + 8 + 4 + v.reply_block.len()).sum();
    let mut w = prologue(ClientVerb::DeltaPoll, body);
    for v in views {
        w.u64(v.view_id).u64(v.after_tick).bytes32(v.reply_block);
    }
    w.into_vec()
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Walk a frame body item by item. A frame names at least one item and at most
/// `cap`; `item` reads one.
fn items<'a, T>(
    body: &'a [u8],
    ctx: &'static str,
    cap: usize,
    mut item: impl FnMut(&mut Reader<'a>) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    if body.is_empty() {
        return Err(format!("{ctx}: empty item list"));
    }
    let mut r = Reader::new(body, ctx);
    let mut out = Vec::new();
    while r.remaining() > 0 {
        if out.len() == cap {
            return Err(format!("{ctx}: too many items (max {cap})"));
        }
        out.push(item(&mut r)?);
    }
    Ok(out)
}

/// Decode a `DDL_TXN` frame body into `(table_id, block)` pairs, in send order;
/// each block's schema is the caller's catalog's.
pub fn decode_ddl_txn(body: &[u8]) -> Result<Vec<(u64, &[u8])>, String> {
    items(body, "DDL_TXN", usize::MAX, |r| Ok((r.u64()?, r.block()?)))
}

/// Decode a `PUSH_TXN` frame body into its families, in send order, each block
/// borrowed from `body`.
pub fn decode_push_txn(body: &[u8]) -> Result<Vec<PushTxnItem<'_, &[u8]>>, String> {
    items(body, "PUSH_TXN", usize::MAX, |r| {
        let tid = r.u64()?;
        let mode =
            WireConflictMode::from_wire(r.u8()?).ok_or_else(|| "PUSH_TXN: unknown family conflict mode".to_string())?;
        let basis = r.u64()?;
        Ok(PushTxnItem {
            tid,
            mode,
            basis,
            schema_block: r.bytes32()?,
            data: r.block()?,
        })
    })
}

/// Decode a `SCAN_MULTI` frame body into its per-relation `(tid,
/// client_schema_version)` list, in request order.
pub fn decode_scan_multi(body: &[u8]) -> Result<Vec<(u64, u16)>, String> {
    items(body, "SCAN_MULTI", SCAN_MULTI_MAX_RELATIONS, |r| {
        Ok((r.u64()?, r.u16()?))
    })
}

/// Decode a `DELTA_POLL` frame body into its items, each block borrowed from
/// `body`. View id `0` is refused: it is the id of a fault ending the request.
pub fn decode_delta_poll(body: &[u8]) -> Result<Vec<DeltaPollItem<'_>>, String> {
    items(body, "DELTA_POLL", DELTA_POLL_MAX_VIEWS, |r| {
        let view_id = r.u64()?;
        if view_id == 0 {
            return Err("DELTA_POLL: view id 0 names no view".to_string());
        }
        Ok(DeltaPollItem {
            view_id,
            after_tick: r.u64()?,
            reply_block: r.bytes32()?,
        })
    })
}

#[cfg(test)]
#[path = "tests/txn_frame.rs"]
mod tests;
