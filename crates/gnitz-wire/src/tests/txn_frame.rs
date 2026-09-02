use super::*;

/// The one region every stand-in block below carries.
const REGION: &[u8] = &[7u8; 24];

/// A minimal WAL block carrying `tid`, as the frame encoders take it.
fn wal_block(tid: u32) -> WalBlock<'static> {
    WalBlock {
        table_id: tid,
        entry_count: 1,
        regions: &[REGION],
    }
}

/// The same block already framed — what a decode must hand back, and what a
/// *schema* block (which the frame carries pre-encoded) is built from.
fn block(tid: u32) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let n = wal::encode(&mut buf, 0, tid, 1, &[REGION], true).unwrap();
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
        (encode_ddl_txn(0xABCD, &[wal_block(16)]), FLAG_DDL_TXN),
        (encode_push_txn(0xABCD, &[(0, &d, wal_block(16))], &[]), FLAG_PUSH_TXN),
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
    let frame = encode_ddl_txn(0xABCD, &[wal_block(4), wal_block(2)]);
    let got = decode_ddl_txn(&frame).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], (4, &a[..]));
    assert_eq!(got[1], (2, &b[..]));
}

#[test]
fn push_txn_roundtrips_families_modes_and_preconditions() {
    let (s0, d0, s1) = (block(16), block(16), block(17));
    let pre = [(16u64, 42u64), (17, 43)];
    let frame = encode_push_txn(1, &[(3, &s0, wal_block(16)), (1, &s1, wal_block(17))], &pre);
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
    let s = block(16);
    let frame = encode_push_txn(1, &[(0, &s, wal_block(16))], &[]);
    let (fams, pre) = decode_push_txn(&frame).unwrap();
    assert_eq!(fams.len(), 1);
    assert!(pre.is_empty());
    // The zero count is four bytes past the last family, not an omission.
    assert_eq!(
        frame.len(),
        ctrl_block_size(0, 0) + 4 + 1 + s.len() + wal_block(16).size() + 4
    );
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
    let s = block(16);
    let push = encode_push_txn(1, &[(0, &s, wal_block(16))], &[(16, 1)]);
    let ddl = encode_ddl_txn(1, &[wal_block(16)]);
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

    let s = block(16);
    let mut push = encode_push_txn(1, &[(0, &s, wal_block(16))], &[]);
    let pre_off = push.len() - 4;
    push[pre_off..].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_push_txn(&push).is_err());
}
