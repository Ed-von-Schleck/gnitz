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

/// Peek `frame`'s prologue — it carries only its verb, and its body starts right
/// after it — check it as a client frame, and decode its body.
fn peeked<'a, T>(frame: &'a [u8], decode: impl Fn(&'a [u8]) -> Result<T, String>) -> Result<T, String> {
    let ctrl = peek_control_block(frame)?;
    let verb = ctrl.client_verb().map_err(str::to_string)?;
    let prologue = ControlHeader {
        flags: WireFlags { verb, ..Default::default() },
        ..Default::default()
    };
    assert_eq!(ctrl.hdr, prologue, "only the verb is set");
    assert!(ctrl.blob.is_empty());
    assert_eq!(ctrl.body, CTRL_HEADER_SIZE..frame.len());
    decode(&frame[ctrl.body.clone()])
}

fn ddl(body: &[u8]) -> Result<Vec<(&[u8], DecodedControl)>, String> {
    decode_items(body, ClientVerb::DdlTxn)
}

fn push(body: &[u8]) -> Result<Vec<(&[u8], DecodedControl)>, String> {
    decode_items(body, ClientVerb::PushTxn)
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
    // A repeated id is two positions, answered separately.
    let rels = [rel(7, 0), rel(8, 3), rel(9, u64::MAX), rel(7, 0)];
    assert_eq!(peeked(&encode_scan_multi(&rels), decode_scan_multi).unwrap(), rels);
}

#[test]
fn delta_poll_roundtrips_every_view_in_order() {
    let views = [
        item(1, 0, 1),
        item(2, 42, u64::MAX),
        item(3, u64::MAX, 7),
        item(1, 0, 1),
    ];
    let frame = encode_delta_poll(&views);
    assert_eq!(peeked(&frame, decode_delta_poll).unwrap(), views);
}

/// A delta poll names no view `0`: it is the id of a fault ending the request.
#[test]
fn a_delta_poll_refuses_view_id_zero() {
    let err = peeked(&encode_delta_poll(&[item(1, 0, 9), item(0, 0, 9)]), decode_delta_poll).expect_err("view id 0");
    assert!(err.contains("view id 0"), "{err:?}");
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

/// Each verb's items: a list at its cap decodes and one past it is refused; an
/// empty list is refused; a truncation inside an item is an error rather than a
/// short list (a cut on an item boundary is a shorter well-formed body, since
/// items run to the end); and an item is refused whose sections differ from the
/// verb's shape, that names another verb, that carries a non-`Ok` status, or
/// that carries a blob.
#[test]
fn decode_items_enforces_each_verbs_item_rules() {
    let multi: Vec<ClientVerb> = ClientVerb::ALL
        .iter()
        .copied()
        .filter(|&v| item_shape(v).is_some())
        .collect();
    assert_eq!(multi.len(), 4);
    for verb in multi {
        let (schema, data, cap) = item_shape(verb).unwrap();
        let hdr = ControlHeader {
            target_id: 7,
            flags: WireFlags { verb, ..Default::default() },
            ..Default::default()
        };
        let refused = |body: &[u8], want: &str| {
            let err = decode_items(body, verb).expect_err(want);
            assert!(err.contains(want), "{verb:?}: {err:?} does not name {want:?}");
        };
        let one = one_item(hdr, b"", schema, data);

        let n = cap.min(3);
        assert_eq!(decode_items(&one.repeat(n), verb).unwrap().len(), n, "{verb:?}");
        if cap != usize::MAX {
            refused(&one.repeat(cap + 1), "too many items");
        }
        refused(&[], "empty item list");

        let two = one.repeat(2);
        for cut in (1..two.len()).filter(|&c| c != one.len()) {
            assert!(decode_items(&two[..cut], verb).is_err(), "{verb:?} cut at {cut}");
        }

        for (s, d) in [(false, false), (true, false), (false, true), (true, true)] {
            if (s, d) != (schema, data) {
                let err = decode_items(&one_item(hdr, b"", s, d), verb).unwrap_err();
                assert!(
                    err.contains("schema record") || err.contains("data block"),
                    "{verb:?}: {err}"
                );
            }
        }
        let other = ControlHeader {
            flags: WireFlags {
                verb: ClientVerb::Push,
                ..Default::default()
            },
            ..hdr
        };
        refused(&one_item(other, b"", schema, data), "names verb");
        refused(
            &one_item(
                ControlHeader { status: WireStatus::Error, ..hdr },
                b"boom",
                schema,
                data,
            ),
            "status",
        );
        refused(&one_item(hdr, b"blob", schema, data), "blob");
    }
    let err = decode_items(
        &one_item(ControlHeader::default(), b"", false, false),
        ClientVerb::ScanSpec,
    )
    .unwrap_err();
    assert!(err.contains("not a multi-item verb"), "{err}");
}
