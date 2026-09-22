//! The four **multi-item request frames** — `PUSH_TXN`, `DDL_TXN`, `SCAN_MULTI`
//! and `DELTA_POLL` — in both directions.
//!
//! All four open with a control header naming the frame's verb (`target_id =
//! 0`), and run their items to the end of the frame. They differ only in what
//! one item is:
//!
//! ```text
//! DDL_TXN     ctrl                | [data block]*
//! PUSH_TXN    ctrl (arg0 = basis) | ([u8 mode][u8 reads][u32 len][schema record][data block])*
//! SCAN_MULTI  ctrl                | ([u64 tid][u16 schema_version])*
//! DELTA_POLL  ctrl                | ([u64 view_id][u64 after_tick][u32 len][reply block])*
//! ```
//!
//! **A reply fault's `target_id`:** a `DELTA_POLL` fault naming one of its views
//! ends that position alone; every other fault ends the request.

use crate::codec::{Reader, Writer};
use crate::control::{encode_frame_head, ControlHeader, CTRL_HEADER_SIZE};
use crate::wal::WalBlock;
use crate::{read_u32_le, ClientVerb, WireConflictMode, WireFlags, WAL_OFF_TID};

/// Maximum relations in one `SCAN_MULTI`. The master holds one scan lease and
/// one reply train of bookkeeping per relation; a handful of related tables
/// covers a realistic consistent snapshot.
pub const SCAN_MULTI_MAX_RELATIONS: usize = 16;

/// Maximum views in one `DELTA_POLL`: the ceiling on the leases and reply
/// trains one poll puts on the master. A mirroring host holds tens of views, and
/// a host past this chunks into a second request.
pub const DELTA_POLL_MAX_VIEWS: usize = 64;

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// The shared prologue: a control header naming `verb` and `arg0`, every other
/// field zero. Each encoder below appends its own body.
fn prologue(verb: ClientVerb, arg0: u64, body_hint: usize) -> Writer {
    let hdr = ControlHeader {
        flags: WireFlags { verb, ..Default::default() },
        arg0,
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
pub fn encode_ddl_txn(blocks: &[WalBlock<'_>]) -> Vec<u8> {
    let mut w = prologue(ClientVerb::DdlTxn, 0, blocks.iter().map(|b| b.size()).sum());
    for b in blocks {
        w.block(b);
    }
    w.into_vec()
}

/// One `PUSH_TXN` family, in both directions.
pub struct PushTxnItem<'a, D> {
    pub mode: WireConflictMode,
    /// The transaction read this family's relation: the commit fails if the
    /// relation was written after the frame's basis.
    pub reads: bool,
    pub schema_block: &'a [u8],
    /// A `WalBlock` to encode; the framed block slice once decoded.
    pub data: D,
}

impl PushTxnItem<'_, &[u8]> {
    /// The target relation, read from the data block's `WAL_OFF_TID`.
    pub fn tid(&self) -> u32 {
        read_u32_le(self.data, WAL_OFF_TID)
    }
}

/// Encode a `PUSH_TXN` frame, without the 4-byte frame header: user-table
/// writes committed as one zone. Every family carries its schema block, so the
/// master validates it with no warm-cache version.
pub fn encode_push_txn(basis: u64, items: &[PushTxnItem<'_, WalBlock<'_>>]) -> Vec<u8> {
    let body: usize = items
        .iter()
        .map(|f| 1 + 1 + 4 + f.schema_block.len() + f.data.size())
        .sum();
    let mut w = prologue(ClientVerb::PushTxn, basis, body);
    for f in items {
        w.u8(f.mode.as_wire())
            .u8(f.reads as u8)
            .bytes32(f.schema_block)
            .block(&f.data);
    }
    w.into_vec()
}

/// Encode a `SCAN_MULTI` frame, without the 4-byte frame header: N relations
/// read at one SAL cut, each `(tid, cached schema version)`, answered in order.
pub fn encode_scan_multi(relations: &[(u64, u16)]) -> Vec<u8> {
    let mut w = prologue(ClientVerb::ScanMulti, 0, relations.len() * (8 + 2));
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
    let mut w = prologue(ClientVerb::DeltaPoll, 0, body);
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
pub fn decode_ddl_txn(body: &[u8]) -> Result<Vec<(u32, &[u8])>, String> {
    items(body, "DDL_TXN", usize::MAX, |r| {
        let b = r.block()?;
        Ok((read_u32_le(b, WAL_OFF_TID), b))
    })
}

/// Decode a `PUSH_TXN` frame body into its families, in send order, each block
/// borrowed from `body`. The basis is the frame's `arg0`.
pub fn decode_push_txn(body: &[u8]) -> Result<Vec<PushTxnItem<'_, &[u8]>>, String> {
    items(body, "PUSH_TXN", usize::MAX, |r| {
        let mode =
            WireConflictMode::from_wire(r.u8()?).ok_or_else(|| "PUSH_TXN: unknown family conflict mode".to_string())?;
        let reads = match r.u8()? {
            0 => false,
            1 => true,
            b => return Err(format!("PUSH_TXN: reads flag {b} is neither 0 nor 1")),
        };
        Ok(PushTxnItem {
            mode,
            reads,
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
