use super::*;
use crate::control::peek_control_block;
use crate::WireStatus;

fn item(view_id: u64, after_tick: u64, reply_block: &[u8]) -> DeltaPollItem<'_> {
    DeltaPollItem { view_id, after_tick, reply_block }
}

/// The one region every stand-in block below carries.
const REGION: &[u8] = &[7u8; 24];

/// A tid past `u32`: the item framing carries all 64 bits.
const WIDE_TID: u64 = (1 << 40) | 16;

/// A minimal one-row block, as the frame encoders take it.
fn wal_block() -> (usize, Regions<'static>) {
    let mut r = Regions::new();
    r.push(REGION);
    r.push(&[]);
    (1, r)
}

/// The same block already framed — what a decode must hand back, and what a
/// *schema* block (which the frame carries pre-encoded) is built from.
fn block() -> Vec<u8> {
    let mut buf = Vec::new();
    crate::wal::append_block(1, &[REGION, &[]], &mut buf);
    buf
}

fn ddl_item(tid: u64) -> (u64, usize, Regions<'static>) {
    let (rows, r) = wal_block();
    (tid, rows, r)
}

fn peeked<'a, T>(frame: &'a [u8], decode: impl Fn(&'a [u8]) -> Result<T, String>) -> Result<T, String> {
    let ctrl = peek_control_block(frame).map_err(str::to_string)?;
    ctrl.client_verb().map_err(str::to_string)?;
    decode(&frame[ctrl.body.clone()])
}

fn family(
    tid: u64,
    mode: WireConflictMode,
    basis: u64,
    schema_block: &[u8],
) -> PushTxnItem<'_, (usize, Regions<'static>)> {
    PushTxnItem {
        tid,
        mode,
        basis,
        schema_block,
        data: wal_block(),
    }
}

/// Every frame's control header carries only its verb; its body starts right
/// after the header.
#[test]
fn the_shared_prologue_carries_only_the_routing_flag() {
    let d = block();
    let frames = [
        (encode_ddl_txn(&[ddl_item(16)]), ClientVerb::DdlTxn),
        (
            encode_push_txn(&[family(16, WireConflictMode::Update, 42, &d)]),
            ClientVerb::PushTxn,
        ),
        (encode_scan_multi(&[(7, 0)]), ClientVerb::ScanMulti),
        (encode_delta_poll(&[item(7, 3, &[2])]), ClientVerb::DeltaPoll),
    ];
    for (frame, verb) in frames {
        let c = peek_control_block(&frame).unwrap();
        assert_eq!(
            c.hdr.flags,
            WireFlags { verb, ..Default::default() },
            "only the verb is set"
        );
        assert_eq!(c.hdr.target_id, 0);
        assert_eq!(c.hdr.arg0, 0);
        assert_eq!(c.hdr.arg1, 0);
        assert_eq!(c.hdr.status, WireStatus::Ok);
        assert!(c.blob.is_empty());
        assert_eq!(c.body, CTRL_HEADER_SIZE..frame.len());
        assert_eq!(c.client_verb(), Ok(verb));
    }
}

#[test]
fn ddl_txn_roundtrips_every_family_in_order() {
    let b = block();
    let frame = encode_ddl_txn(&[ddl_item(WIDE_TID), ddl_item(2)]);
    let got = peeked(&frame, decode_ddl_txn).unwrap();
    assert_eq!(got, [(WIDE_TID, &b[..]), (2, &b[..])]);
}

#[test]
fn push_txn_roundtrips_families_modes_and_bases() {
    let (s0, s1, d) = (b"schema 0".to_vec(), b"schema one".to_vec(), block());
    let w = 0x0102_0304_0506_0708;
    let frame = encode_push_txn(&[
        family(16, WireConflictMode::Error, BLIND, &s0),
        family(WIDE_TID, WireConflictMode::Update, w, &s1),
    ]);
    let fams = peeked(&frame, decode_push_txn).unwrap();
    assert_eq!(fams.len(), 2);
    assert_eq!(
        (fams[0].tid, fams[0].mode, fams[0].basis),
        (16, WireConflictMode::Error, BLIND)
    );
    assert_eq!(
        (fams[1].tid, fams[1].mode, fams[1].basis),
        (WIDE_TID, WireConflictMode::Update, w)
    );
    assert_eq!((fams[0].schema_block, fams[0].data), (&s0[..], &d[..]));
    assert_eq!((fams[1].schema_block, fams[1].data), (&s1[..], &d[..]));
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

/// Every frame names at least one item; a delta poll names no view `0`; a
/// repeated id is two positions, answered separately.
#[test]
fn the_item_rules_are_the_decoders() {
    let empty = |verb| {
        let hdr = ControlHeader {
            flags: WireFlags { verb, ..Default::default() },
            ..Default::default()
        };
        let mut head = [0u8; CTRL_HEADER_SIZE];
        encode_frame_head(&mut head, &hdr, &[], None, false);
        head
    };
    let want = "empty item list";
    let errs = [
        peeked(&empty(ClientVerb::DdlTxn), decode_ddl_txn).err(),
        peeked(&empty(ClientVerb::PushTxn), decode_push_txn).err(),
        peeked(&empty(ClientVerb::ScanMulti), decode_scan_multi).err(),
        peeked(&empty(ClientVerb::DeltaPoll), decode_delta_poll).err(),
    ];
    for err in errs {
        let err = err.expect(want);
        assert!(err.contains(want), "{err:?} does not name {want:?}");
    }

    let err = peeked(
        &encode_delta_poll(&[item(1, 0, b"b"), item(0, 0, b"b")]),
        decode_delta_poll,
    )
    .expect_err("view id 0");
    assert!(err.contains("view id 0"), "{err:?}");

    let views = [item(4, 0, b"b"), item(4, 0, b"b")];
    assert_eq!(peeked(&encode_delta_poll(&views), decode_delta_poll).unwrap(), views);
    let rels = [(4u64, 0u16), (4, 0)];
    assert_eq!(peeked(&encode_scan_multi(&rels), decode_scan_multi).unwrap(), rels);
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
    /// `two` holds `one`'s item twice; no prefix of it decodes but `one`.
    fn check<'a, T>(name: &str, one: &'a [u8], two: &'a [u8], decode: impl Fn(&'a [u8]) -> Result<T, String> + Copy) {
        assert!(peeked(one, decode).is_ok(), "{name} single item");
        for cut in 1..two.len() {
            if cut != one.len() {
                assert!(peeked(&two[..cut], decode).is_err(), "{name} cut at {cut}");
            }
        }
    }
    let s = block();
    let fam = || family(16, WireConflictMode::Update, 1, &s);
    check(
        "PUSH_TXN",
        &encode_push_txn(&[fam()]),
        &encode_push_txn(&[fam(), fam()]),
        decode_push_txn,
    );
    check(
        "DDL_TXN",
        &encode_ddl_txn(&[ddl_item(16)]),
        &encode_ddl_txn(&[ddl_item(16), ddl_item(16)]),
        decode_ddl_txn,
    );
    check(
        "SCAN_MULTI",
        &encode_scan_multi(&[(7, 0)]),
        &encode_scan_multi(&[(7, 0), (7, 0)]),
        decode_scan_multi,
    );
    let v = item(7, 3, &[4, 5]);
    check(
        "DELTA_POLL",
        &encode_delta_poll(&[v]),
        &encode_delta_poll(&[v, v]),
        decode_delta_poll,
    );
}
