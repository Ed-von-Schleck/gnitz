use super::*;
use crate::control::peek_control_block;
use crate::WireStatus;

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
    let n = wal::encode(&mut buf, 0, tid, 1, &[REGION], false).unwrap();
    buf.truncate(n);
    buf
}

fn peeked<'a, T>(
    frame: &'a [u8],
    decode: impl Fn(&'a [u8], &DecodedControl) -> Result<T, String>,
) -> Result<T, String> {
    let ctrl = peek_control_block(frame).map_err(str::to_string)?;
    decode(frame, &ctrl)
}

/// Every frame's control header carries only its verb, and a push's
/// precondition count in `arg1`.
#[test]
fn the_shared_prologue_carries_only_the_routing_flag() {
    let d = block(16);
    let frames = [
        (encode_ddl_txn(&[wal_block(16)]), ClientVerb::DdlTxn, 0),
        (
            encode_push_txn(&[(WireConflictMode::Update, &d, wal_block(16))], &[(16, 1), (17, 2)]),
            ClientVerb::PushTxn,
            2,
        ),
        (encode_scan_multi(&[(7, 0)]), ClientVerb::ScanMulti, 0),
        (encode_delta_poll(&[item(7, 3, &[2])]), ClientVerb::DeltaPoll, 0),
    ];
    for (frame, verb, arg1) in frames {
        let c = peek_control_block(&frame).unwrap();
        assert_eq!(
            c.hdr.flags,
            WireFlags { verb, ..Default::default() },
            "only the verb is set"
        );
        assert_eq!(c.hdr.target_id, 0);
        assert_eq!(c.hdr.arg0, 0);
        assert_eq!(c.hdr.arg1, arg1);
        assert_eq!(c.hdr.status, WireStatus::Ok);
        assert!(c.blob.is_empty());
        assert_eq!(c.block_size, CTRL_HEADER_SIZE);
    }
}

#[test]
fn ddl_txn_roundtrips_every_family_in_order() {
    let (a, b) = (block(4), block(2));
    let frame = encode_ddl_txn(&[wal_block(4), wal_block(2)]);
    let got = peeked(&frame, decode_ddl_txn).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], (4, &a[..]));
    assert_eq!(got[1], (2, &b[..]));
}

#[test]
fn push_txn_roundtrips_families_modes_and_preconditions() {
    let (s0, d0, s1) = (block(16), block(16), block(17));
    let pre = [(16u64, 42u64), (17, 43)];
    let frame = encode_push_txn(
        &[
            (WireConflictMode::Error, &s0, wal_block(16)),
            (WireConflictMode::Update, &s1, wal_block(17)),
        ],
        &pre,
    );
    let (fams, got_pre) = peeked(&frame, decode_push_txn).unwrap();
    assert_eq!(fams.len(), 2);
    assert_eq!((fams[0].tid, fams[0].mode), (16, WireConflictMode::Error));
    assert_eq!((fams[1].tid, fams[1].mode), (17, WireConflictMode::Update));
    assert_eq!(fams[0].schema_block, &s0[..]);
    assert_eq!(fams[0].wal_block, &d0[..]);
    assert_eq!(got_pre, pre);
}

#[test]
fn push_txn_with_no_preconditions_carries_no_section() {
    let s = block(16);
    let frame = encode_push_txn(&[(WireConflictMode::Update, &s, wal_block(16))], &[]);
    let (fams, pre) = peeked(&frame, decode_push_txn).unwrap();
    assert_eq!(fams.len(), 1);
    assert!(pre.is_empty());
    assert_eq!(frame.len(), CTRL_HEADER_SIZE + 1 + s.len() + wal_block(16).size());
}

#[test]
fn scan_multi_roundtrips_order_and_versions() {
    let rels = [(7u64, 0u16), (8, 3), (9, u16::MAX)];
    assert_eq!(peeked(&encode_scan_multi(&rels), decode_scan_multi).unwrap(), rels);
}

/// A view's cursor and reply block round-trip at any length, in order.
#[test]
fn delta_poll_roundtrips_every_view_in_order() {
    let blocks: Vec<Vec<u8>> = [1usize, 128, 7].iter().map(|n| vec![0x5A; *n]).collect();
    let views: Vec<DeltaPollItem> = (0..3)
        .map(|i| item(i as u64 + 1, [0, 42, u64::MAX][i], &blocks[i]))
        .collect();

    let frame = encode_delta_poll(&views);
    assert_eq!(peeked(&frame, decode_delta_poll).unwrap(), views);
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
/// decoder.
#[test]
fn a_frame_past_its_cap_is_refused_by_the_decoder() {
    let views: Vec<DeltaPollItem> = (1..=DELTA_POLL_MAX_VIEWS as u64).map(|id| item(id, 5, b"b")).collect();
    assert_eq!(
        peeked(&encode_delta_poll(&views), decode_delta_poll).unwrap().len(),
        views.len()
    );
    let mut over = views.clone();
    over.push(item(u64::MAX, 5, b"b"));
    let err = peeked(&encode_delta_poll(&over), decode_delta_poll).expect_err("past the cap");
    assert!(err.contains("too many items"), "{err:?}");

    let rels: Vec<(u64, u16)> = (1..=SCAN_MULTI_MAX_RELATIONS as u64).map(|id| (id, 0)).collect();
    assert_eq!(
        peeked(&encode_scan_multi(&rels), decode_scan_multi).unwrap().len(),
        rels.len()
    );
    let mut over = rels.clone();
    over.push((u64::MAX, 0));
    let err = peeked(&encode_scan_multi(&over), decode_scan_multi).expect_err("past the cap");
    assert!(err.contains("too many items"), "{err:?}");
}

/// Every frame must reject a truncation inside an item rather than panic or
/// silently return a short list. A cut exactly on an item boundary is a shorter
/// well-formed frame, since items run to the frame's end.
#[test]
fn a_truncation_inside_an_item_is_a_decode_error() {
    fn check<'a, T>(
        name: &str,
        frame: &'a [u8],
        boundaries: &[usize],
        decode: impl Fn(&'a [u8], &DecodedControl) -> Result<T, String> + Copy,
    ) {
        for cut in 1..frame.len() {
            if !boundaries.contains(&cut) {
                assert!(peeked(&frame[..cut], decode).is_err(), "{name} cut at {cut}");
            }
        }
    }
    let h = CTRL_HEADER_SIZE;
    let s = block(16);
    let push = encode_push_txn(&[(WireConflictMode::Update, &s, wal_block(16))], &[(16, 1)]);
    check("PUSH_TXN", &push, &[h + PRECONDITION_BYTES], decode_push_txn);
    let ddl = encode_ddl_txn(&[wal_block(16)]);
    check("DDL_TXN", &ddl, &[h], decode_ddl_txn);
    let scan = encode_scan_multi(&[(7, 0), (8, 1)]);
    check("SCAN_MULTI", &scan, &[h, h + RELATION_BYTES], decode_scan_multi);
    let poll = encode_delta_poll(&[item(7, 3, &[4]), item(8, 5, &[6, 7])]);
    check("DELTA_POLL", &poll, &[h, h + 21], decode_delta_poll);
}

/// A precondition count past what the frame can hold is rejected, never used to
/// size an allocation.
#[test]
fn a_hostile_precondition_count_does_not_drive_the_allocation() {
    let s = block(16);
    let frame = encode_push_txn(&[(WireConflictMode::Update, &s, wal_block(16))], &[]);
    let mut ctrl = peek_control_block(&frame).unwrap();
    ctrl.hdr.arg1 = u64::MAX;
    let err = decode_push_txn(&frame, &ctrl).err().expect("a hostile count");
    assert!(err.contains("precondition section truncated"), "{err:?}");
}
