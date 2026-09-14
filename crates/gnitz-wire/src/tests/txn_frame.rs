use super::*;

fn item(view_id: u64, after_tick: u64, reply_block: &[u8]) -> DeltaPollItem<'_> {
    DeltaPollItem { view_id, after_tick, reply_block }
}

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
        (encode_delta_poll(0xABCD, &[item(7, 3, &[2])]), FLAG_DELTA_POLL),
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

/// A view's cursor and reply block round-trip at any length, in order.
#[test]
fn delta_poll_roundtrips_every_view_in_order() {
    let blocks: Vec<Vec<u8>> = [1usize, 128, 7].iter().map(|n| vec![0x5A; *n]).collect();
    let views: Vec<DeltaPollItem> = (0..3)
        .map(|i| item(i as u64 + 1, [0, 42, u64::MAX][i], &blocks[i]))
        .collect();

    let frame = encode_delta_poll(1, &views);
    assert_eq!(decode_delta_poll(&frame).unwrap(), views);
}

/// The item rules both multi-item frames share, and the accepted lists that
/// bracket them. The wording is the contract: client and server reject an
/// identical list with identical text, so a caller reading one message is
/// reading both.
#[test]
fn a_multi_item_id_list_is_validated() {
    for ctx in ["SCAN_MULTI", "DELTA_POLL"] {
        assert!(validate_item_ids(ctx, &[4u64, 5, 6], |&id| id).is_ok());
        let cases: &[(Vec<u64>, &str)] = &[
            (vec![], "empty item list"),
            (vec![4, 5, 4], "duplicate id 4"),
            (vec![1, 0], "id 0 names no relation"),
        ];
        for (bad, want) in cases {
            let err = validate_item_ids(ctx, bad, |&id| id).expect_err(want);
            assert!(err.contains(want), "{err:?} does not name {want:?}");
        }
    }
}

/// A frame at its format's cap round-trips, and one past it is refused by the
/// decoder — the one place that sees a count before it is trusted with an
/// allocation, so the cap is enforced there and nowhere else.
#[test]
fn a_frame_past_its_cap_is_refused_by_the_decoder() {
    let views: Vec<DeltaPollItem> = (1..=DELTA_POLL_MAX_VIEWS as u64).map(|id| item(id, 5, b"b")).collect();
    assert_eq!(
        decode_delta_poll(&encode_delta_poll(1, &views)).unwrap().len(),
        views.len()
    );
    let mut over = views.clone();
    over.push(item(u64::MAX, 5, b"b"));
    let err = decode_delta_poll(&encode_delta_poll(1, &over)).expect_err("past the cap");
    assert!(err.contains("too many items"), "{err:?}");

    let rels: Vec<(u64, u16)> = (1..=SCAN_MULTI_MAX_RELATIONS as u64).map(|id| (id, 0)).collect();
    assert_eq!(
        decode_scan_multi(&encode_scan_multi(1, &rels)).unwrap().len(),
        rels.len()
    );
    let mut over = rels.clone();
    over.push((u64::MAX, 0));
    let err = decode_scan_multi(&encode_scan_multi(1, &over)).expect_err("past the cap");
    assert!(err.contains("too many items"), "{err:?}");
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
    let poll = encode_delta_poll(1, &[item(7, 3, &[4]), item(8, 5, &[6, 7])]);
    for cut in 1..poll.len() {
        assert!(decode_delta_poll(&poll[..cut]).is_err(), "DELTA_POLL cut at {cut}");
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

    let mut poll = encode_delta_poll(1, &[item(7, 1, &[2])]);
    poll[count_off..count_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_delta_poll(&poll).is_err());

    let s = block(16);
    let mut push = encode_push_txn(1, &[(0, &s, wal_block(16))], &[]);
    let pre_off = push.len() - 4;
    push[pre_off..].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_push_txn(&push).is_err());
}
