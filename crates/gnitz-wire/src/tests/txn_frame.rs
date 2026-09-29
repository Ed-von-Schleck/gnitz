use super::*;
use crate::control::CTRL_HEADER_SIZE;
use crate::WireConflictMode;

fn rel(tid: u64, reply_layout: u64) -> ScanMultiItem {
    ScanMultiItem { tid, reply_layout }
}

fn item(view_id: u64, after_tick: u64, reply_layout: u64) -> DeltaPollItem {
    DeltaPollItem { view_id, after_tick, reply_layout }
}

/// The PK region every stand-in block below carries: one 24-byte key.
const PK: &[u8] = &[7u8; 24];
const WEIGHT: &[u8] = &1i64.to_le_bytes();
const NULLS: &[u8] = &[0u8; 8];

/// A tid past `u32`: the item header carries all 64 bits.
const WIDE_TID: u64 = (1 << 40) | 16;

/// A minimal one-row canonical region list.
fn regions() -> Regions<'static> {
    let mut r = Regions::new();
    for region in [PK, WEIGHT, NULLS, &[]] {
        r.push(region);
    }
    r
}

/// The same block framed — what a decoded item's data section must hold.
fn block() -> Vec<u8> {
    let mut buf = Vec::new();
    crate::wal::append_block(&regions(), 0, &mut buf);
    buf
}

fn ddl_item(tid: u64) -> FrameItem<'static> {
    FrameItem {
        hdr: ControlHeader { target_id: tid, ..Default::default() },
        schema: None,
        data: Some(regions()),
    }
}

fn family(tid: u64, conflict_mode: WireConflictMode, basis: u64, schema: &[u8]) -> FrameItem<'_> {
    FrameItem {
        hdr: ControlHeader {
            target_id: tid,
            flags: WireFlags { conflict_mode, ..Default::default() },
            arg0: basis,
            ..Default::default()
        },
        schema: Some(schema),
        data: Some(regions()),
    }
}

/// Peek `frame`'s prologue, check it as a client frame, and decode its body.
fn peeked<'a, T>(frame: &'a [u8], decode: impl Fn(&'a [u8]) -> Result<T, String>) -> Result<T, String> {
    let ctrl = peek_control_block(frame)?;
    ctrl.client_verb().map_err(str::to_string)?;
    decode(&frame[ctrl.body.clone()])
}

fn ddl(body: &[u8]) -> Result<Vec<(&[u8], DecodedControl)>, String> {
    decode_items(body, ClientVerb::DdlTxn)
}

fn push(body: &[u8]) -> Result<Vec<(&[u8], DecodedControl)>, String> {
    decode_items(body, ClientVerb::PushTxn)
}

/// Every frame's prologue carries only its verb; its body starts right after it.
#[test]
fn the_prologue_carries_only_the_verb() {
    let d = b"schema".to_vec();
    let frames = [
        (encode_items(ClientVerb::DdlTxn, &[ddl_item(16)]), ClientVerb::DdlTxn),
        (
            encode_items(ClientVerb::PushTxn, &[family(16, WireConflictMode::Update, 42, &d)]),
            ClientVerb::PushTxn,
        ),
        (encode_scan_multi(&[rel(7, 0)]), ClientVerb::ScanMulti),
        (encode_delta_poll(&[item(7, 3, 2)]), ClientVerb::DeltaPoll),
    ];
    for (frame, verb) in frames {
        let c = peek_control_block(&frame).unwrap();
        assert_eq!(
            c.hdr,
            ControlHeader {
                flags: WireFlags { verb, ..Default::default() },
                ..Default::default()
            },
            "only the verb is set"
        );
        assert!(c.blob.is_empty());
        assert_eq!(c.body, CTRL_HEADER_SIZE..frame.len());
        assert_eq!(c.client_verb(), Ok(verb));
    }
}

/// Each item is its own frame: its bytes end where its last section does, its
/// body is empty, and its data section is the block it was built from.
#[test]
fn ddl_txn_roundtrips_every_family_in_order() {
    let b = block();
    let frame = encode_items(ClientVerb::DdlTxn, &[ddl_item(WIDE_TID), ddl_item(2)]);
    let got = peeked(&frame, ddl).unwrap();
    assert_eq!(got.len(), 2);
    for ((bytes, c), tid) in got.iter().zip([WIDE_TID, 2]) {
        assert_eq!(c.hdr.target_id, tid);
        assert_eq!(c.hdr.flags.verb, ClientVerb::DdlTxn);
        assert_eq!(&bytes[c.data.clone().unwrap()], &b[..]);
        assert_eq!(c.body, bytes.len()..bytes.len());
    }
}

#[test]
fn push_txn_roundtrips_families_modes_and_bases() {
    let (s0, s1, d) = (b"schema 0".to_vec(), b"schema one".to_vec(), block());
    let w = 0x0102_0304_0506_0708;
    let frame = encode_items(
        ClientVerb::PushTxn,
        &[
            family(16, WireConflictMode::Error, BLIND, &s0),
            family(WIDE_TID, WireConflictMode::Update, w, &s1),
        ],
    );
    let fams = peeked(&frame, push).unwrap();
    assert_eq!(fams.len(), 2);
    let want = [
        (16, WireConflictMode::Error, BLIND, &s0),
        (WIDE_TID, WireConflictMode::Update, w, &s1),
    ];
    for ((bytes, c), (tid, mode, basis, schema)) in fams.iter().zip(want) {
        assert_eq!(
            (c.hdr.target_id, c.hdr.flags.conflict_mode, c.hdr.arg0),
            (tid, mode, basis)
        );
        assert_eq!(&bytes[c.schema.clone().unwrap()], &schema[..]);
        assert_eq!(&bytes[c.data.clone().unwrap()], &d[..]);
    }
}

#[test]
fn scan_multi_roundtrips_order_and_layouts() {
    let rels = [rel(7, 0), rel(8, 3), rel(9, u64::MAX)];
    assert_eq!(peeked(&encode_scan_multi(&rels), decode_scan_multi).unwrap(), rels);
}

#[test]
fn delta_poll_roundtrips_every_view_in_order() {
    let views: Vec<DeltaPollItem> = (0..3)
        .map(|i| item(i as u64 + 1, [0, 42, u64::MAX][i], [1, u64::MAX, 7][i]))
        .collect();
    let frame = encode_delta_poll(&views);
    assert_eq!(peeked(&frame, decode_delta_poll).unwrap(), views);
}

/// Every frame names at least one item; a delta poll names no view `0`; a
/// repeated id is two positions, answered separately.
#[test]
fn the_item_rules_are_the_decoders() {
    let empty = |verb| encode_items(verb, &[]);
    let want = "empty item list";
    let errs = [
        peeked(&empty(ClientVerb::DdlTxn), ddl).err(),
        peeked(&empty(ClientVerb::PushTxn), push).err(),
        peeked(&empty(ClientVerb::ScanMulti), decode_scan_multi).err(),
        peeked(&empty(ClientVerb::DeltaPoll), decode_delta_poll).err(),
    ];
    for err in errs {
        let err = err.expect(want);
        assert!(err.contains(want), "{err:?} does not name {want:?}");
    }

    let err = peeked(&encode_delta_poll(&[item(1, 0, 9), item(0, 0, 9)]), decode_delta_poll).expect_err("view id 0");
    assert!(err.contains("view id 0"), "{err:?}");

    let views = [item(4, 0, 9), item(4, 0, 9)];
    assert_eq!(peeked(&encode_delta_poll(&views), decode_delta_poll).unwrap(), views);
    let rels = [rel(4, 0), rel(4, 0)];
    assert_eq!(peeked(&encode_scan_multi(&rels), decode_scan_multi).unwrap(), rels);
}

/// A frame at its format's cap round-trips, and one past it is refused by the
/// decoder.
#[test]
fn a_frame_past_its_cap_is_refused_by_the_decoder() {
    let views: Vec<DeltaPollItem> = (1..=DELTA_POLL_MAX_VIEWS as u64).map(|id| item(id, 5, 9)).collect();
    assert_eq!(
        peeked(&encode_delta_poll(&views), decode_delta_poll).unwrap().len(),
        views.len()
    );
    let mut over = views.clone();
    over.push(item(u64::MAX, 5, 9));
    let err = peeked(&encode_delta_poll(&over), decode_delta_poll).expect_err("past the cap");
    assert!(err.contains("too many items"), "{err:?}");

    let rels: Vec<ScanMultiItem> = (1..=SCAN_MULTI_MAX_RELATIONS as u64).map(|id| rel(id, 0)).collect();
    assert_eq!(
        peeked(&encode_scan_multi(&rels), decode_scan_multi).unwrap().len(),
        rels.len()
    );
    let mut over = rels.clone();
    over.push(rel(u64::MAX, 0));
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
        for cut in CTRL_HEADER_SIZE + 1..two.len() {
            if cut != one.len() {
                assert!(peeked(&two[..cut], decode).is_err(), "{name} cut at {cut}");
            }
        }
    }
    let s = block();
    let fam = || family(16, WireConflictMode::Update, 1, &s);
    check(
        "PUSH_TXN",
        &encode_items(ClientVerb::PushTxn, &[fam()]),
        &encode_items(ClientVerb::PushTxn, &[fam(), fam()]),
        push,
    );
    check(
        "DDL_TXN",
        &encode_items(ClientVerb::DdlTxn, &[ddl_item(16)]),
        &encode_items(ClientVerb::DdlTxn, &[ddl_item(16), ddl_item(16)]),
        ddl,
    );
    check(
        "SCAN_MULTI",
        &encode_scan_multi(&[rel(7, 0)]),
        &encode_scan_multi(&[rel(7, 0), rel(7, 0)]),
        decode_scan_multi,
    );
    let v = item(7, 3, 4);
    check(
        "DELTA_POLL",
        &encode_delta_poll(&[v]),
        &encode_delta_poll(&[v, v]),
        decode_delta_poll,
    );
}

/// A body of one hand-built item: `hdr` (status and verb as given), `blob`,
/// and the sections named.
fn one_item(hdr: ControlHeader, blob: &[u8], schema: bool, data: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let r = regions();
    crate::control::append_frame(
        &mut out,
        &hdr,
        blob,
        schema.then_some(&b"s"[..]),
        data.then_some(&r[..]),
    );
    out
}

/// Each verb refuses an item whose sections differ from its shape, one that
/// names another verb, one under a non-`Ok` status and one carrying a blob.
#[test]
fn an_item_off_its_verbs_shape_is_refused() {
    let shapes = [
        (ClientVerb::DdlTxn, false, true),
        (ClientVerb::PushTxn, true, true),
        (ClientVerb::ScanMulti, false, false),
        (ClientVerb::DeltaPoll, false, false),
    ];
    for (verb, schema, data) in shapes {
        let hdr = ControlHeader {
            target_id: 7,
            flags: WireFlags { verb, ..Default::default() },
            ..Default::default()
        };
        assert!(
            decode_items(&one_item(hdr, b"", schema, data), verb).is_ok(),
            "{verb:?}"
        );
        for (s, d) in [(false, false), (true, false), (false, true), (true, true)] {
            if (s, d) != (schema, data) {
                let err = decode_items(&one_item(hdr, b"", s, d), verb).expect_err("wrong sections");
                assert!(
                    err.contains("schema record") || err.contains("data block"),
                    "{verb:?}: {err}"
                );
            }
        }
        let other = ControlHeader {
            flags: WireFlags {
                verb: if verb == ClientVerb::ScanSpec {
                    ClientVerb::Push
                } else {
                    ClientVerb::ScanSpec
                },
                ..Default::default()
            },
            ..hdr
        };
        let err = decode_items(&one_item(other, b"", schema, data), verb).expect_err("other verb");
        assert!(err.contains("names verb"), "{verb:?}: {err}");
        let fault = ControlHeader { status: WireStatus::Error, ..hdr };
        let err = decode_items(&one_item(fault, b"boom", schema, data), verb).expect_err("fault");
        assert!(err.contains("status"), "{verb:?}: {err}");
        let err = decode_items(&one_item(hdr, b"blob", schema, data), verb).expect_err("blob");
        assert!(err.contains("blob"), "{verb:?}: {err}");
    }
    let err = decode_items(
        &one_item(ControlHeader::default(), b"", false, false),
        ClientVerb::ScanSpec,
    )
    .expect_err("a single-item verb");
    assert!(err.contains("not a multi-item verb"), "{err}");
}
