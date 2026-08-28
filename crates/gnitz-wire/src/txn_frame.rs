//! The three **transaction-shaped request frames** — `PUSH_TXN`, `DDL_TXN` and
//! `SCAN_MULTI` — in both directions.
//!
//! All three open the same way: the reused control block (`target_id = 0`, every
//! seek field zero, the frame's flag bit), then a `u32` item count. They differ
//! only in what one item is:
//!
//! ```text
//! DDL_TXN     ctrl | u32 n | n × [data block]
//! PUSH_TXN    ctrl | u32 n | n × [u8 mode][schema block][data block]
//!                          | u32 p | p × [u64 tid][u64 basis_lsn]
//! SCAN_MULTI  ctrl | u32 n | n × [u64 tid][u16 schema_version]
//! ```
//!
//! Defining them here rather than once per crate is what keeps the item widths —
//! the mode byte, the 10-byte relation record, the `4 + 16n` precondition
//! section — from being restated on each side of the wire.
//!
//! The per-item WAL blocks are **pre-encoded bytes**: what a schema or data block
//! contains belongs to `schema_block` and to each side's batch codec, and every
//! block is self-sizing (its `WAL_OFF_SIZE`), so the frame walk needs nothing
//! beyond the bytes.

use crate::codec::{Reader, Writer};
use crate::control::{ctrl_block_size, encode_ctrl_block, peek_control_block, ControlHeader};
use crate::wal;
use crate::{read_u32_le, FLAG_DDL_TXN, FLAG_PUSH_TXN, FLAG_SCAN_MULTI, STATUS_OK, WAL_HEADER_SIZE, WAL_OFF_TID};

/// Bytes one `SCAN_MULTI` relation record occupies: a `u64` tid and the client's
/// `u16` cached schema version (`0` = none, so the server sends that relation's
/// schema block).
pub(crate) const RELATION_BYTES: usize = 8 + 2;

/// Bytes one OCC precondition occupies: `[u64 tid][u64 basis_lsn]`.
pub(crate) const PRECONDITION_BYTES: usize = 8 + 8;

/// The least a `PUSH_TXN` family can encode to: its mode byte plus the two WAL
/// blocks' headers. Bounds a hostile family count against the bytes that
/// physically remain.
const MIN_PUSH_FAMILY_BYTES: usize = 1 + 2 * WAL_HEADER_SIZE;

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// The shared prologue: the reused control block (`target_id = 0`, every seek
/// field zero, the given `flags`) followed by the `u32` item count. Each
/// encoder below appends its own per-item body.
fn prologue(client_id: u64, flags: u64, count: usize, body_hint: usize) -> Writer {
    let hdr = ControlHeader {
        status: STATUS_OK,
        target_id: 0,
        client_id,
        flags,
        seek_pk: 0,
        seek_col_idx: 0,
        request_id: 0,
    };
    let ctrl_len = ctrl_block_size(0, 0);
    let mut ctrl = vec![0u8; ctrl_len];
    // A client request frame carries a body checksum, as its WAL blocks do.
    encode_ctrl_block(&mut ctrl, 0, &hdr, &[], &[], true);

    let mut w = Writer::with_capacity(ctrl_len + 4 + body_hint);
    w.raw(&ctrl).u32(count as u32);
    w
}

/// Encode an atomic **DDL transaction** frame (`FLAG_DDL_TXN`), without the
/// 4-byte frame header. Every system-table write — a `CREATE`'s N family
/// batches, a `DROP`/`CREATE INDEX`/`CREATE SCHEMA`'s single batch — rides one
/// such frame, so the server ingests the whole bundle under one durable SAL
/// zone.
///
/// Each block embeds its own `table_id` and total size, so the server walks the
/// list by header alone with no schema in hand and defers schema resolution to
/// its catalog.
pub fn encode_ddl_txn(client_id: u64, blocks: &[&[u8]]) -> Vec<u8> {
    let mut w = prologue(
        client_id,
        FLAG_DDL_TXN,
        blocks.len(),
        blocks.iter().map(|b| b.len()).sum(),
    );
    for b in blocks {
        w.raw(b);
    }
    w.into_vec()
}

/// Encode an atomic **user-table push transaction** frame (`FLAG_PUSH_TXN`),
/// without the 4-byte frame header — the client-facing analogue of
/// [`encode_ddl_txn`], which is exclusive to system families.
///
/// Each family is `(conflict mode, meta-schema block, data block)`. The schema
/// block is **always present**, which is what lets the master validate the
/// family against its catalog with no warm-cache version, exactly as the DDL
/// frame does.
///
/// A `preconditions` entry `(tid, basis)` asserts "no commit has written `tid`
/// with a zone LSN greater than `basis`". The section is always emitted: an
/// empty slice still writes its zero count.
pub fn encode_push_txn(client_id: u64, families: &[(u8, &[u8], &[u8])], preconditions: &[(u64, u64)]) -> Vec<u8> {
    let body: usize = families.iter().map(|(_, s, d)| 1 + s.len() + d.len()).sum();
    let mut w = prologue(
        client_id,
        FLAG_PUSH_TXN,
        families.len(),
        body + 4 + preconditions.len() * PRECONDITION_BYTES,
    );
    for (mode, schema_block, wal_block) in families {
        w.u8(*mode).raw(schema_block).raw(wal_block);
    }
    w.u32(preconditions.len() as u32);
    for (tid, basis) in preconditions {
        w.u64(*tid).u64(*basis);
    }
    w.into_vec()
}

/// Encode a **multi-relation scan** frame (`FLAG_SCAN_MULTI`), without the
/// 4-byte frame header: a consistent snapshot of N relations at one server-side
/// SAL cut. The server answers with N reply trains in this exact order, which
/// the caller reads positionally.
///
/// The control block's schema-version bits stay zero; the per-relation versions
/// ride the body.
pub fn encode_scan_multi(client_id: u64, relations: &[(u64, u16)]) -> Vec<u8> {
    let mut w = prologue(
        client_id,
        FLAG_SCAN_MULTI,
        relations.len(),
        relations.len() * RELATION_BYTES,
    );
    for (tid, version) in relations {
        w.u64(*tid).u16(*version);
    }
    w.into_vec()
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Walk the shared prologue, returning `(count, offset of the first item)`.
///
/// A count past what the remaining bytes can physically hold is rejected here,
/// so every caller may `Vec::with_capacity(count)` directly and a hostile count
/// cannot force a giant pre-allocation on an ingress-capped frame.
///
/// The control block is validated (version, region count, checksum) but not
/// returned — a caller that needs the routing header already peeked it.
fn decode_prologue(data: &[u8], ctx: &'static str, min_item_bytes: usize) -> Result<(usize, usize), String> {
    let ctrl = wal::block_slice_at(data, 0).map_err(|e| format!("{ctx}: control block: {e}"))?;
    peek_control_block(ctrl).map_err(|e| format!("{ctx}: {e}"))?;
    let off = ctrl.len();
    if off + 4 > data.len() {
        return Err(format!("{ctx}: item count truncated"));
    }
    let count = read_u32_le(data, off) as usize;
    let off = off + 4;
    let max_items = data.len().saturating_sub(off) / min_item_bytes + 1;
    if count > max_items {
        return Err(format!("{ctx}: item count {count} exceeds what the frame holds"));
    }
    Ok((count, off))
}

/// Decode a `FLAG_DDL_TXN` frame into its per-family `(table_id, wal-block
/// slice)` list, in send order. Walks the concatenated blocks by header alone —
/// `table_id` at `WAL_OFF_TID`, total size at `WAL_OFF_SIZE` — so the caller
/// resolves each family's schema from its own catalog and decodes the slice
/// itself.
pub fn decode_ddl_txn(data: &[u8]) -> Result<Vec<(u32, &[u8])>, String> {
    const CTX: &str = "DDL_TXN";
    let (count, mut off) = decode_prologue(data, CTX, WAL_HEADER_SIZE)?;
    let mut families = Vec::with_capacity(count);
    for _ in 0..count {
        let block = wal::block_slice_at(data, off).map_err(|e| format!("{CTX}: family block: {e}"))?;
        families.push((read_u32_le(block, WAL_OFF_TID), block));
        off += block.len();
    }
    Ok(families)
}

/// One decoded `FLAG_PUSH_TXN` family: the target `tid` (read from the data
/// block's `WAL_OFF_TID`), the conflict `mode` byte, and the borrowed schema and
/// data WAL-block slices — same lifetime discipline as [`decode_ddl_txn`]'s
/// `(u32, &[u8])`.
pub struct TxnFamily<'a> {
    pub tid: u32,
    pub mode: u8,
    pub schema_block: &'a [u8],
    pub wal_block: &'a [u8],
}

/// The two lists a `FLAG_PUSH_TXN` frame decodes to: its per-family write bundle
/// and its OCC preconditions (each `(tid, basis_lsn)`).
pub type DecodedPushTxn<'a> = (Vec<TxnFamily<'a>>, Vec<(u64, u64)>);

/// Decode a `FLAG_PUSH_TXN` frame into its per-family list plus its OCC
/// precondition list, in send order — the user-table analogue of
/// [`decode_ddl_txn`]. Truncation at any field rejects the whole frame; the
/// count/duplicate/tid-legality shape rules belong to the handler, so a
/// well-formed but empty list decodes cleanly and is rejected there with a
/// specific message.
///
/// The precondition section sits *after* the families rather than before the
/// count, which is what lets the prologue and [`decode_ddl_txn`] stay identical.
pub fn decode_push_txn(data: &[u8]) -> Result<DecodedPushTxn<'_>, String> {
    const CTX: &str = "PUSH_TXN";
    let (count, mut off) = decode_prologue(data, CTX, MIN_PUSH_FAMILY_BYTES)?;
    let mut families = Vec::with_capacity(count);
    for _ in 0..count {
        if off + 1 > data.len() {
            return Err(format!("{CTX}: family mode truncated"));
        }
        let mode = data[off];
        off += 1;
        let schema_block = wal::block_slice_at(data, off).map_err(|e| format!("{CTX}: schema block: {e}"))?;
        off += schema_block.len();
        let wal_block = wal::block_slice_at(data, off).map_err(|e| format!("{CTX}: data block: {e}"))?;
        off += wal_block.len();
        families.push(TxnFamily {
            tid: read_u32_le(wal_block, WAL_OFF_TID),
            mode,
            schema_block,
            wal_block,
        });
    }

    // The trailing section is fixed-width from here, so the bounds-checked
    // cursor covers both its count and its body.
    let mut r = Reader::new(&data[off..], CTX);
    let pre_count = r.u32()? as usize;
    // Bound by the bytes physically remaining so a hostile count cannot force a
    // giant pre-allocation before the reads reject it.
    if pre_count > r.remaining() / PRECONDITION_BYTES {
        return Err(format!("{CTX}: precondition section truncated"));
    }
    let mut preconditions = Vec::with_capacity(pre_count);
    for _ in 0..pre_count {
        preconditions.push((r.u64()?, r.u64()?));
    }
    Ok((families, preconditions))
}

/// Decode a `FLAG_SCAN_MULTI` frame into its per-relation `(tid,
/// client_schema_version)` list, in request order. Truncation at any field
/// rejects the whole frame; the count/duplicate/tid-legality shape rules are the
/// handler's.
pub fn decode_scan_multi(data: &[u8]) -> Result<Vec<(u64, u16)>, String> {
    const CTX: &str = "SCAN_MULTI";
    let (count, off) = decode_prologue(data, CTX, RELATION_BYTES)?;
    let mut r = Reader::new(&data[off..], CTX);
    let mut relations = Vec::with_capacity(count);
    for _ in 0..count {
        relations.push((r.u64()?, r.u16()?));
    }
    Ok(relations)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal self-sizing WAL block carrying `tid`, standing in for a schema
    /// or data block: the frame walk only reads its header.
    fn block(tid: u32) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let n = wal::encode(&mut buf, 0, tid, 1, &[&[7u8; 24]], true).unwrap();
        buf.truncate(n);
        buf
    }

    /// Every frame's control block routes the same way: `target_id = 0`, the
    /// caller's client id, and **exactly** the one routing flag — no seek fields,
    /// and none of the schema-version bits a per-target request would carry
    /// (SCAN_MULTI's versions ride its body instead).
    #[test]
    fn the_shared_prologue_carries_only_the_routing_flag() {
        let d = block(16);
        let frames = [
            (encode_ddl_txn(0xABCD, &[&d]), FLAG_DDL_TXN),
            (encode_push_txn(0xABCD, &[(0, &d, &d)], &[]), FLAG_PUSH_TXN),
            (encode_scan_multi(0xABCD, &[(7, 0)]), FLAG_SCAN_MULTI),
        ];
        for (frame, flag) in frames {
            let ctrl = wal::block_slice_at(&frame, 0).unwrap();
            let c = peek_control_block(ctrl).unwrap();
            assert_eq!(c.flags, flag, "only the routing flag is set");
            assert_eq!(c.target_id, 0);
            assert_eq!(c.client_id, 0xABCD);
            assert_eq!(c.seek_pk, 0);
            assert_eq!(c.seek_col_idx, 0);
            assert_eq!(c.status, STATUS_OK);
            // The item count starts immediately after the control block.
            assert_eq!(ctrl.len(), ctrl_block_size(0, 0));
        }
    }

    #[test]
    fn ddl_txn_roundtrips_every_family_in_order() {
        let (a, b) = (block(4), block(2));
        let frame = encode_ddl_txn(0xABCD, &[&a, &b]);
        let got = decode_ddl_txn(&frame).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], (4, &a[..]));
        assert_eq!(got[1], (2, &b[..]));
    }

    #[test]
    fn push_txn_roundtrips_families_modes_and_preconditions() {
        let (s0, d0, s1, d1) = (block(16), block(16), block(17), block(17));
        let pre = [(16u64, 42u64), (17, 43)];
        let frame = encode_push_txn(1, &[(3, &s0, &d0), (1, &s1, &d1)], &pre);
        let (fams, got_pre) = decode_push_txn(&frame).unwrap();
        assert_eq!(fams.len(), 2);
        assert_eq!((fams[0].tid, fams[0].mode), (16, 3));
        assert_eq!((fams[1].tid, fams[1].mode), (17, 1));
        assert_eq!(fams[0].schema_block, &s0[..]);
        assert_eq!(fams[0].wal_block, &d0[..]);
        assert_eq!(got_pre, pre);
    }

    #[test]
    fn push_txn_with_no_preconditions_still_encodes_the_section() {
        let (s, d) = (block(16), block(16));
        let frame = encode_push_txn(1, &[(0, &s, &d)], &[]);
        let (fams, pre) = decode_push_txn(&frame).unwrap();
        assert_eq!(fams.len(), 1);
        assert!(pre.is_empty());
        // The zero count is four bytes past the last family, not an omission.
        assert_eq!(frame.len(), ctrl_block_size(0, 0) + 4 + 1 + s.len() + d.len() + 4);
    }

    #[test]
    fn scan_multi_roundtrips_order_and_versions() {
        let rels = [(7u64, 0u16), (8, 3), (9, u16::MAX)];
        assert_eq!(decode_scan_multi(&encode_scan_multi(1, &rels)).unwrap(), rels);
    }

    /// Every frame must reject a truncation at any offset rather than panic or
    /// silently return a short list.
    #[test]
    fn a_truncation_anywhere_is_a_decode_error() {
        let (s, d) = (block(16), block(16));
        let push = encode_push_txn(1, &[(0, &s, &d)], &[(16, 1)]);
        let ddl = encode_ddl_txn(1, &[&d]);
        let scan = encode_scan_multi(1, &[(7, 0), (8, 1)]);
        for cut in 1..push.len() {
            assert!(decode_push_txn(&push[..cut]).is_err(), "PUSH_TXN cut at {cut}");
        }
        for cut in 1..ddl.len() {
            assert!(decode_ddl_txn(&ddl[..cut]).is_err(), "DDL_TXN cut at {cut}");
        }
        for cut in 1..scan.len() {
            assert!(decode_scan_multi(&scan[..cut]).is_err(), "SCAN_MULTI cut at {cut}");
        }
    }

    /// A count far past what the frame can hold must be rejected, never used to
    /// size an allocation.
    #[test]
    fn a_hostile_item_count_does_not_drive_the_allocation() {
        let mut scan = encode_scan_multi(1, &[(7, 0)]);
        let count_off = ctrl_block_size(0, 0);
        scan[count_off..count_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_scan_multi(&scan).is_err());

        let (s, d) = (block(16), block(16));
        let mut push = encode_push_txn(1, &[(0, &s, &d)], &[]);
        let pre_off = push.len() - 4;
        push[pre_off..].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_push_txn(&push).is_err());
    }
}
