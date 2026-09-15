//! The four **multi-item request frames** — `PUSH_TXN`, `DDL_TXN`, `SCAN_MULTI`
//! and `DELTA_POLL` — in both directions.
//!
//! All four open with a control header naming only the frame's verb
//! (`target_id = 0`), and run their items to the end of the frame. They differ
//! only in what one item is:
//!
//! ```text
//! DDL_TXN     ctrl            | [data block]*
//! PUSH_TXN    ctrl (arg1 = p) | p × [u64 tid][u64 basis_lsn] | ([u8 mode][schema block][data block])*
//! SCAN_MULTI  ctrl            | ([u64 tid][u16 schema_version])*
//! DELTA_POLL  ctrl            | ([u64 view_id][u64 after_tick][u32 len][reply block])*
//! ```
//!
//! Defining them here rather than once per crate is what keeps the item widths —
//! the mode byte, the 10-byte relation record, the 16-byte precondition — from
//! being restated on each side of the wire.
//!
//! The per-item WAL blocks are **pre-encoded bytes**: what a schema or data block
//! contains belongs to `schema_block` and to each side's batch codec, and every
//! block is self-sizing (its `WAL_OFF_SIZE`), so the frame walk needs nothing
//! beyond the bytes.
//!
//! **In the reply direction `target_id` says what a fault covers.** A fault at
//! `0` — which [`validate_item_ids`] keeps out of every item list — rejects the
//! whole frame; one naming an item ends that item's position and no other. Only
//! `DELTA_POLL` answers per item; the other three fault at `0` alone.

use crate::codec::{Reader, Writer};
use crate::control::{encode_ctrl_block, ControlHeader, DecodedControl, CTRL_HEADER_SIZE};
use crate::wal;
use crate::{read_u32_le, ClientVerb, WireConflictMode, WireFlags, WAL_OFF_TID};

/// Bytes one `SCAN_MULTI` relation record occupies: a `u64` tid and the client's
/// `u16` cached schema version (`0` = none, so the server sends that relation's
/// schema block).
pub(crate) const RELATION_BYTES: usize = 8 + 2;

/// Bytes one OCC precondition occupies: `[u64 tid][u64 basis_lsn]`.
pub(crate) const PRECONDITION_BYTES: usize = 8 + 8;

/// Maximum relations in one `SCAN_MULTI`. The master holds one scan lease and
/// one reply train of bookkeeping per relation; a handful of related tables
/// covers a realistic consistent snapshot.
pub const SCAN_MULTI_MAX_RELATIONS: usize = 16;

/// Maximum views in one `DELTA_POLL`: the ceiling on the leases and reply
/// trains one poll puts on the master. A mirroring host holds tens of views, and
/// a host past this chunks into a second request.
pub const DELTA_POLL_MAX_VIEWS: usize = 64;

/// The rules a multi-item frame's id list obeys that need the items themselves:
/// non-empty, no duplicate — which would answer one position twice — and no id
/// `0`, which names no relation and is the id a rejection of the frame carries.
/// Run on the client encoder and again on the server, so both reject an
/// identical list with identical text. The count cap is the decoders'.
///
/// A client that skipped the empty check would desync its connection: an empty
/// frame's lone error frame is one the N=0 reply loop never consumes.
pub fn validate_item_ids<T>(ctx: &str, items: &[T], id: impl Fn(&T) -> u64) -> Result<(), String> {
    if items.is_empty() {
        return Err(format!("{ctx}: empty item list"));
    }
    for (i, item) in items.iter().enumerate() {
        let this = id(item);
        if this == 0 {
            return Err(format!("{ctx}: id 0 names no relation"));
        }
        if items[..i].iter().any(|other| id(other) == this) {
            return Err(format!("{ctx}: duplicate id {this}"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// The shared prologue: a control header naming `verb` and `arg1`, every other
/// field zero. Each encoder below appends its own body.
fn prologue(verb: ClientVerb, arg1: u64, body_hint: usize) -> Writer {
    let hdr = ControlHeader {
        flags: WireFlags { verb, ..Default::default() },
        arg1,
        ..Default::default()
    };
    let mut w = Writer::with_capacity(CTRL_HEADER_SIZE + body_hint);
    // Written into the frame, not into a scratch `Vec` and copied in.
    encode_ctrl_block(w.reserve(CTRL_HEADER_SIZE), &hdr, &[]);
    w
}

/// One WAL block a transaction frame carries, as the region list it is framed
/// from rather than as bytes.
///
/// The frame encoders below size the whole frame from these and then write each
/// block **into** it, so a batch is copied once — into the frame — instead of
/// once into a per-block `Vec` and again into the frame. At the 64 MB frame cap
/// that second copy is milliseconds, on a path every autocommit `UPDATE` /
/// `DELETE` runs and an OCC retry re-runs.
pub struct WalBlock<'a> {
    pub table_id: u32,
    pub entry_count: u32,
    pub regions: &'a [&'a [u8]],
}

impl WalBlock<'_> {
    /// The framed size — what the frame reserves for this block.
    pub fn size(&self) -> usize {
        crate::wal::block_size_of(self.regions)
    }

    /// Frame this block into `dst`, which must be exactly [`Self::size`] bytes.
    fn write(&self, dst: &mut [u8]) {
        crate::wal::encode(dst, 0, self.table_id, self.entry_count, self.regions, false)
            .expect("WAL encode: the frame reserved block_size_of bytes");
    }
}

/// Encode an atomic **DDL transaction** frame (`ClientVerb::DdlTxn`), without the
/// 4-byte frame header. Every system-table write — a `CREATE`'s N family
/// batches, a `DROP`/`CREATE INDEX`/`CREATE SCHEMA`'s single batch — rides one
/// such frame, so the server ingests the whole bundle under one durable SAL
/// zone.
///
/// Each block embeds its own `table_id` and total size, so the server walks the
/// list by header alone with no schema in hand and defers schema resolution to
/// its catalog.
pub fn encode_ddl_txn(blocks: &[WalBlock<'_>]) -> Vec<u8> {
    let mut w = prologue(ClientVerb::DdlTxn, 0, blocks.iter().map(|b| b.size()).sum());
    for b in blocks {
        let n = b.size();
        b.write(w.reserve(n));
    }
    w.into_vec()
}

/// Encode an atomic **user-table push transaction** frame (`ClientVerb::PushTxn`),
/// without the 4-byte frame header — the client-facing analogue of
/// [`encode_ddl_txn`], which is exclusive to system families.
///
/// Each family is `(conflict mode, meta-schema block, data block)`. The schema
/// block is **always present**, which is what lets the master validate the
/// family against its catalog with no warm-cache version, exactly as the DDL
/// frame does.
///
/// A `preconditions` entry `(tid, basis)` asserts "no commit has written `tid`
/// with a zone LSN greater than `basis`". Their count rides `arg1`.
pub fn encode_push_txn(families: &[(WireConflictMode, &[u8], WalBlock<'_>)], preconditions: &[(u64, u64)]) -> Vec<u8> {
    let body: usize = families.iter().map(|(_, s, d)| 1 + s.len() + d.size()).sum();
    let mut w = prologue(
        ClientVerb::PushTxn,
        preconditions.len() as u64,
        preconditions.len() * PRECONDITION_BYTES + body,
    );
    for (tid, basis) in preconditions {
        w.u64(*tid).u64(*basis);
    }
    for (mode, schema_block, wal_block) in families {
        w.u8(mode.as_wire()).raw(schema_block);
        let n = wal_block.size();
        wal_block.write(w.reserve(n));
    }
    w.into_vec()
}

/// Encode a **multi-relation scan** frame (`ClientVerb::ScanMulti`), without the
/// 4-byte frame header: a consistent snapshot of N relations at one server-side
/// SAL cut. The server answers with N reply trains in this exact order, which
/// the caller reads positionally.
///
/// The control header's schema version stays zero; the per-relation versions
/// ride the body.
pub fn encode_scan_multi(relations: &[(u64, u16)]) -> Vec<u8> {
    let mut w = prologue(ClientVerb::ScanMulti, 0, relations.len() * RELATION_BYTES);
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

/// Encode a **delta poll** frame (`ClientVerb::DeltaPoll`), without the 4-byte frame
/// header.
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

/// Decode a `DDL_TXN` frame into its per-family `(table_id, wal-block
/// slice)` list, in send order. Walks the concatenated blocks by header alone —
/// `table_id` at `WAL_OFF_TID`, total size at `WAL_OFF_SIZE` — so the caller
/// resolves each family's schema from its own catalog and decodes the slice
/// itself.
pub fn decode_ddl_txn<'a>(frame: &'a [u8], ctrl: &DecodedControl) -> Result<Vec<(u32, &'a [u8])>, String> {
    const CTX: &str = "DDL_TXN";
    let mut off = ctrl.block_size;
    let mut families = Vec::new();
    while off < frame.len() {
        let block = wal::block_slice_at(frame, off).map_err(|e| format!("{CTX}: family block: {e}"))?;
        families.push((read_u32_le(block, WAL_OFF_TID), block));
        off += block.len();
    }
    Ok(families)
}

/// One decoded `PUSH_TXN` family: the target `tid` (read from the data
/// block's `WAL_OFF_TID`), the conflict `mode`, and the borrowed schema and
/// data WAL-block slices — same lifetime discipline as [`decode_ddl_txn`]'s
/// `(u32, &[u8])`.
pub struct TxnFamily<'a> {
    pub tid: u32,
    pub mode: WireConflictMode,
    pub schema_block: &'a [u8],
    pub wal_block: &'a [u8],
}

/// The two lists a `PUSH_TXN` frame decodes to: its per-family write bundle
/// and its OCC preconditions (each `(tid, basis_lsn)`).
pub type DecodedPushTxn<'a> = (Vec<TxnFamily<'a>>, Vec<(u64, u64)>);

/// Decode a `PUSH_TXN` frame into its per-family list plus its OCC
/// precondition list, in send order — the user-table analogue of
/// [`decode_ddl_txn`]. Truncation at any field rejects the whole frame; the
/// count/duplicate/tid-legality shape rules belong to the handler, so a
/// well-formed but empty list decodes cleanly and is rejected there with a
/// specific message.
pub fn decode_push_txn<'a>(frame: &'a [u8], ctrl: &DecodedControl) -> Result<DecodedPushTxn<'a>, String> {
    const CTX: &str = "PUSH_TXN";
    let body = &frame[ctrl.block_size..];
    // Bound by the bytes physically present, so a hostile count cannot force a
    // giant pre-allocation before the reads reject it.
    if ctrl.hdr.arg1 > (body.len() / PRECONDITION_BYTES) as u64 {
        return Err(format!("{CTX}: precondition section truncated"));
    }
    let pre_count = ctrl.hdr.arg1 as usize;
    let mut r = Reader::new(body, CTX);
    let mut preconditions = Vec::with_capacity(pre_count);
    for _ in 0..pre_count {
        preconditions.push((r.u64()?, r.u64()?));
    }

    let mut off = ctrl.block_size + pre_count * PRECONDITION_BYTES;
    let mut families = Vec::new();
    while off < frame.len() {
        let mode =
            WireConflictMode::from_wire(frame[off]).ok_or_else(|| format!("{CTX}: unknown family conflict mode"))?;
        off += 1;
        let schema_block = wal::block_slice_at(frame, off).map_err(|e| format!("{CTX}: schema block: {e}"))?;
        off += schema_block.len();
        let wal_block = wal::block_slice_at(frame, off).map_err(|e| format!("{CTX}: data block: {e}"))?;
        off += wal_block.len();
        families.push(TxnFamily {
            tid: read_u32_le(wal_block, WAL_OFF_TID),
            mode,
            schema_block,
            wal_block,
        });
    }
    Ok((families, preconditions))
}

/// Decode a `SCAN_MULTI` frame into its per-relation `(tid,
/// client_schema_version)` list, in request order. A body that is not a whole
/// number of records rejects the whole frame; the duplicate/tid-legality shape
/// rules are the handler's.
pub fn decode_scan_multi(frame: &[u8], ctrl: &DecodedControl) -> Result<Vec<(u64, u16)>, String> {
    const CTX: &str = "SCAN_MULTI";
    let body = &frame[ctrl.block_size..];
    if !body.len().is_multiple_of(RELATION_BYTES) {
        return Err(format!("{CTX}: relation record truncated"));
    }
    let count = body.len() / RELATION_BYTES;
    if count > SCAN_MULTI_MAX_RELATIONS {
        return Err(format!(
            "{CTX}: too many items ({count}, max {SCAN_MULTI_MAX_RELATIONS})"
        ));
    }
    let mut r = Reader::new(body, CTX);
    let mut relations = Vec::with_capacity(count);
    for _ in 0..count {
        relations.push((r.u64()?, r.u16()?));
    }
    Ok(relations)
}

/// Decode a `DELTA_POLL` frame into its items, each block borrowed from `frame`.
pub fn decode_delta_poll<'a>(frame: &'a [u8], ctrl: &DecodedControl) -> Result<Vec<DeltaPollItem<'a>>, String> {
    const CTX: &str = "DELTA_POLL";
    let mut r = Reader::new(&frame[ctrl.block_size..], CTX);
    let mut views = Vec::new();
    while r.remaining() > 0 {
        if views.len() == DELTA_POLL_MAX_VIEWS {
            return Err(format!("{CTX}: too many items (max {DELTA_POLL_MAX_VIEWS})"));
        }
        views.push(DeltaPollItem {
            view_id: r.u64()?,
            after_tick: r.u64()?,
            reply_block: r.bytes32()?,
        });
    }
    Ok(views)
}

#[cfg(test)]
#[path = "tests/txn_frame.rs"]
mod tests;
