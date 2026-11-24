use super::{
    FLAG_SHARED, FLAG_ZONE, FLAG_ZONE_END, GroupHead, Header, KNOWN_FLAGS, MAX_WORKERS, OFF_ARG0, OFF_BASE, OFF_FLAGS,
    OFF_KIND, OFF_TARGETS, OFF_WIDTH, PAYLOAD_HEAD, SalMessageKind, group_header_size, payload_encode, payload_parse,
    payload_rows, payload_size, payload_sizes,
};
use crate::runtime::park::WorkerSet;
use crate::runtime::sal::fixtures::{group_at, test_log};
use crate::runtime::sal::format::RequestHead;
use crate::runtime::sal::{Apply, Inbound};
use crate::runtime::test_support::assert_fail_stops;
use crate::runtime::w2m::fixtures::id;
use crate::test_support::encode_schema_record;
use crate::test_support::{
    certified, make_batch, make_batch_bytes, make_schema_pk_u64_payload_string, make_schema_u64_i64, weighted_rows,
};
use gnitz_wire::TypeCode;
use gnitz_wire::wal::WAL_HEADER_SIZE;
use gnitz_zset::repr::{Batch, BatchBuilder, WireRows};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

// ---------------------------------------------------------------------------
// The header codec: every field round-trips, the bytes are pinned, and a value
// this build does not know is corruption rather than a guess.
// ---------------------------------------------------------------------------

/// The header `header` and the directory of `sizes`, encoded over a buffer of
/// other bytes and not yet sealed.
fn encoded(header: &Header, sizes: &[usize]) -> Vec<u8> {
    let mut hdr = vec![0xA5; group_header_size(sizes.len())];
    header.encode(&mut hdr);
    for (i, &size) in sizes.iter().enumerate() {
        Header::set_payload_size(&mut hdr, i, size);
    }
    hdr
}

/// [`encoded`], sealed.
fn sealed(header: &Header, sizes: &[usize]) -> Vec<u8> {
    let mut hdr = encoded(header, sizes);
    Header::seal(&mut hdr);
    hdr
}

/// Edit the sealed header `hdr` and seal it again, so the decode behind the
/// digest is what a verify exercises.
fn reseal(hdr: &mut [u8], edit: impl FnOnce(&mut [u8])) {
    edit(hdr);
    Header::seal(hdr);
}

/// Every scalar the group header carries survives the encode and the verify,
/// the top epoch and the top of the base's range included. A header is
/// authenticated by a digest over its whole span, so an encode/decode offset
/// that drifted would verify and hand back the wrong field value with no error
/// anywhere.
#[test]
fn every_header_field_round_trips() {
    let targets = WorkerSet::one(0).with(2).with(4);
    let request = RequestHead {
        kind: SalMessageKind::Backfill,
        target_id: 0xABCD_1234,
        arg0: 0x1111_2222_3333_4444,
        arg1: 0x5555_6666_7777_8888,
    };
    let base = 0xFFFF_FFF8u32;
    let shapes: [(bool, bool, _, &[usize]); 2] = [
        (true, false, Some(id(0x0A0B_0C0D)), &[8, 0, 24]),
        (false, true, None, &[40]),
    ];
    for (zoned, shared, reply, sizes) in shapes {
        let header = Header {
            head: GroupHead::new(request, zoned, shared, reply, targets),
            base,
            epoch: u32::MAX,
            width: 5,
            payload_sum: 0x0123_4567_89AB_CDEF,
        };
        let mut hdr = encoded(&header, sizes);
        if zoned {
            Header::set_zone_end(&mut hdr);
        }
        Header::seal(&mut hdr);

        let got = Header::verify(&hdr, u64::from(base)).expect("the header verifies where it was sealed");
        assert_eq!(got.head.request, request);
        assert_eq!(
            (got.head.zoned(), got.head.zone_end(), got.head.shared()),
            (zoned, zoned, shared)
        );
        assert_eq!((got.head.reply, got.head.targets), (reply, targets));
        assert_eq!(
            (got.base, got.epoch, got.width, got.payload_sum),
            (base, u32::MAX, 5, 0x0123_4567_89AB_CDEF)
        );
        assert_eq!(
            payload_sizes(&hdr).collect::<Vec<_>>(),
            sizes,
            "every directory entry, the empty payload included"
        );
        assert!(
            Header::verify(&hdr, u64::from(base) - 8).is_none(),
            "a header verifies at its own offset alone"
        );
    }
}

/// The header's bytes, field by field: what a log written by this build holds.
#[test]
fn a_header_is_these_bytes() {
    let request = RequestHead {
        kind: SalMessageKind::Push,
        target_id: 0x1122_3344_5566_7788,
        arg0: 0x0102_0304_0506_0708,
        arg1: 0x1112_1314_1516_1718,
    };
    let header = Header {
        head: GroupHead::new(request, true, false, Some(id(0x0A0B_0C0D)), WorkerSet::one(1).with(3)),
        base: 0x00C0_FFE8,
        epoch: 7,
        width: 4,
        payload_sum: 0x2122_2324_2526_2728,
    };
    let hdr = sealed(&header, &[9, 300]);
    #[rustfmt::skip]
    let behind_the_digest: [u8; 72] = [
        9, 8, 4, 0,                                     // kind, flags (zone), width, zero
        0x0D, 0x0C, 0x0B, 0x0A,                         // request id
        0xE8, 0xFF, 0xC0, 0x00,                         // base
        7, 0, 0, 0,                                     // epoch
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // target id
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // arg0
        0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // arg1
        0b1010, 0, 0, 0, 0, 0, 0, 0,                    // targets
        0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, // payload checksum
        9, 0, 0, 0, 0, 0, 0, 0,                         // directory: 9 bytes,
        0x2C, 0x01, 0, 0, 0, 0, 0, 0,                   // then 300
    ];
    assert_eq!(
        hdr[8..],
        behind_the_digest,
        "the header layout moved: bump `SAL_LAYOUT`"
    );
    assert_eq!(
        hdr[..8],
        gnitz_wire::checksum(&behind_the_digest).to_le_bytes(),
        "the digest of everything behind it"
    );
}

/// A digest-valid header whose scalars name nothing a writer produces does not
/// verify: it was written by another layout, and guessing is not an option.
/// Each refusal follows an edit that verifies, so none can be the digest's.
#[test]
fn an_unknown_ordinal_in_a_verified_header_does_not_verify() {
    let request = RequestHead {
        kind: SalMessageKind::ScanSpec,
        target_id: 0,
        arg0: 0,
        arg1: 0,
    };
    let header = Header {
        head: GroupHead::new(request, true, false, None, WorkerSet::one(0)),
        base: 0,
        epoch: 1,
        width: 1,
        payload_sum: 0,
    };
    let mut hdr = sealed(&header, &[8]);
    let verify = |hdr: &[u8]| Header::verify(hdr, 0);

    reseal(&mut hdr, |hdr| hdr[OFF_KIND] = SalMessageKind::Tick.as_wire());
    assert_eq!(
        verify(&hdr).expect("the re-seal verifies").head.request.kind,
        SalMessageKind::Tick
    );

    let unknown = (0..=u8::MAX)
        .find(|&b| SalMessageKind::from_wire(b).is_none())
        .expect("fewer than 256 kinds");
    reseal(&mut hdr, |hdr| hdr[OFF_KIND] = unknown);
    assert!(verify(&hdr).is_none(), "an unknown kind ordinal must not verify");

    reseal(&mut hdr, |hdr| {
        hdr[OFF_KIND] = SalMessageKind::ScanSpec.as_wire();
        hdr[OFF_FLAGS] = FLAG_ZONE | FLAG_ZONE_END | FLAG_SHARED;
    });
    assert!(
        verify(&hdr).expect("the known flags verify").head.zone_end(),
        "the known flags verify"
    );

    reseal(&mut hdr, |hdr| hdr[OFF_FLAGS] = FLAG_ZONE_END | FLAG_SHARED);
    assert!(verify(&hdr).is_none(), "a zone end outside every zone must not verify");

    reseal(&mut hdr, |hdr| hdr[OFF_FLAGS] = KNOWN_FLAGS + 1);
    assert!(verify(&hdr).is_none(), "an unknown flag bit must not verify");

    reseal(&mut hdr, |hdr| hdr[OFF_FLAGS] = 0);
    assert_eq!(verify(&hdr).expect("the re-seal verifies").width, 1);
    reseal(&mut hdr, |hdr| hdr[OFF_WIDTH] = MAX_WORKERS as u8 + 1);
    assert!(
        verify(&hdr).is_none(),
        "a width past the most workers a cluster runs must not verify"
    );

    // Shared, so the addressed set does not size the header.
    reseal(&mut hdr, |hdr| {
        hdr[OFF_WIDTH] = 1;
        hdr[OFF_FLAGS] = FLAG_SHARED;
    });
    assert!(verify(&hdr).expect("the re-seal verifies").head.shared());
    reseal(&mut hdr, |hdr| hdr[OFF_KIND] = SalMessageKind::DdlSync.as_wire());
    assert!(verify(&hdr).is_some(), "a shared DdlSync verifies");
    reseal(&mut hdr, |hdr| {
        gnitz_wire::write_u64_le(hdr, OFF_TARGETS, 0b11);
    });
    assert!(
        verify(&hdr).is_none(),
        "a worker addressed past the group's width must not verify"
    );

    // One payload for the one addressed worker, so the header is as long shared
    // or not.
    reseal(&mut hdr, |hdr| {
        gnitz_wire::write_u64_le(hdr, OFF_TARGETS, 0b1);
        hdr[OFF_FLAGS] = 0;
    });
    assert!(
        verify(&hdr).is_none(),
        "a DdlSync with a payload per worker must not verify"
    );
    reseal(&mut hdr, |hdr| hdr[OFF_KIND] = SalMessageKind::ScanSpec.as_wire());
    assert!(verify(&hdr).is_some(), "which any other kind may hold");

    reseal(&mut hdr, |hdr| gnitz_wire::write_u32_le(hdr, OFF_BASE, 8));
    assert!(verify(&hdr).is_none(), "a header sealed for another offset");
    assert!(Header::verify(&hdr, 8).is_some(), "verifies there");
}

/// A request's arguments lie in the digested header, so a damaged one is a
/// damaged header to the live drain, which fail-stops on it whether or not the
/// group is a zone member.
#[test]
fn the_live_path_aborts_on_a_damaged_request_argument() {
    assert_fail_stops(
        module_path!(),
        "the_live_path_aborts_on_a_damaged_request_argument_internal",
        "is corrupt — the log is damaged",
    );
}

/// Runs only in the re-exec'd abort child.
#[test]
fn the_live_path_aborts_on_a_damaged_request_argument_internal() {
    if !gnitz_test_harness::in_child_test() {
        return;
    }
    let mut log = test_log(1 << 20, 1, 1);
    let tick = || Apply::Tick { first_round: 5, tids: vec![9].into() };
    log.excl().apply(tick()).expect("group fits");
    // A sanity read before the damage, so the abort below is the damage.
    let mut reader = log.reader(0);
    assert!(
        matches!(reader.next_request(|_, _| None), Some(Inbound::Apply { apply, .. }) if apply == tick()),
        "the tick decodes"
    );
    assert!(!group_at(log.log(), 0).head.zoned(), "a tick is no zone member");
    log.header_mut(0)[OFF_ARG0] ^= 1;

    let mut reader = log.reader(0);
    let _ = reader.next_request(|_, _| None);
    unreachable!("a damaged first round must fail-stop, not tick another round");
}

// ---------------------------------------------------------------------------
// The payload codec
// ---------------------------------------------------------------------------

/// The payload of `blob` and `rows`, encoded over a buffer of other bytes.
fn payload(blob: &[u8], rows: Option<WireRows<'_>>) -> Vec<u8> {
    let mut out = vec![0xA5; payload_size(blob, rows)];
    payload_encode(&mut out, blob, rows);
    out
}

/// A payload's rows, its record decoded.
fn sal_rows(wire: &[u8]) -> Result<Option<Batch>, String> {
    payload_rows(wire, |_| None).map(|(_, rows)| rows)
}

/// Eight rows of unequal region strides: a 4-byte PK, then stride-4 and
/// stride-2 payloads.
fn padded_batch() -> Batch {
    let sd = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I16, false),
        ],
        &[0],
    );
    let mut bb = BatchBuilder::new(&sd);
    for i in 0..8u32 {
        bb.begin_row(i as u128, i as i64 + 1);
        bb.put_int(i as u128 * 10);
        bb.put_int(i as u128 * 3);
        bb.end_row();
    }
    bb.finish()
}

/// Every payload shape decodes to what was sent, and no proper prefix of it
/// decodes.
#[test]
fn every_payload_shape_round_trips_and_no_prefix_decodes() {
    let fixed = make_schema_u64_i64();
    let consolidated = certified(make_batch(&fixed, &[(1, 2, -5), (2, -1, 7), (9, 3, 0)]));
    let empty = Batch::empty_with_schema(&fixed);
    let raw = make_batch(
        &fixed,
        &(0..8).map(|i| (7 - i, i as i64 + 1, i as i64 * 10)).collect::<Vec<_>>(),
    );
    let long = b"a string long enough to leave the inline prefix".as_slice();
    let strings = certified(make_batch_bytes(
        &make_schema_pk_u64_payload_string(),
        &[(1, 1, b"inline".as_slice()), (2, -2, long), (3, 1, long)],
    ));
    let padded = padded_batch();
    let row_width = (raw.wire_whole().unwrap().byte_size() - WAL_HEADER_SIZE) / raw.len();
    let range = raw.wire_rows_within(2, WAL_HEADER_SIZE + 3 * row_width).unwrap();
    assert_eq!(range.rows(), 3, "a budget of three rows frames three");
    let whole = |b: &Batch| (0..b.len()).collect::<Vec<_>>();

    let fixed_rel = encode_schema_record(&fixed);
    let string_rel = encode_schema_record(strings.schema());
    let padded_rel = encode_schema_record(padded.schema());
    /// A payload's blob and rows, beside the source batch and the rows of it the
    /// payload sends, in the order it sends them. A payload with rows has their
    /// schema record as its blob.
    type Shape<'a> = (&'a [u8], Option<WireRows<'a>>, Option<(&'a Batch, Vec<usize>)>);
    let shapes: [Shape<'_>; 7] = [
        (b"", None, None),
        (b"a blob and no rows", None, None),
        (
            &fixed_rel,
            consolidated.wire_whole(),
            Some((&consolidated, whole(&consolidated))),
        ),
        // An empty batch has no rows to send.
        (&fixed_rel, empty.wire_whole(), None),
        (&string_rel, strings.wire_whole(), Some((&strings, whole(&strings)))),
        (&fixed_rel, Some(range), Some((&raw, vec![2, 3, 4]))),
        (
            &padded_rel,
            padded.as_mem_batch().wire_listed(&[6, 1, 3]),
            Some((&padded, vec![6, 1, 3])),
        ),
    ];

    for (shape, (blob, data, sent)) in shapes.into_iter().enumerate() {
        let wire = payload(blob, data);
        let (got_blob, got_rows) = payload_parse(&wire).unwrap_or_else(|e| panic!("shape {shape}: {e}"));
        assert_eq!(got_blob, blob, "shape {shape}");
        assert_eq!(got_rows.is_some(), data.is_some(), "shape {shape}");
        let rows = sal_rows(&wire).unwrap_or_else(|e| panic!("shape {shape}: {e}"));
        match (sent, rows) {
            (None, None) => {}
            (Some((src, rows)), Some(got)) => {
                let all = weighted_rows(src);
                let want: Vec<_> = rows.iter().map(|&i| all[i].clone()).collect();
                assert_eq!(weighted_rows(&got), want, "shape {shape}");
            }
            (want, got) => panic!(
                "shape {shape}: sent rows {}, decoded rows {}",
                want.is_some(),
                got.is_some()
            ),
        }
        // A rows payload cut at the end of its blob is a payload without rows:
        // a payload's extent comes from the digested directory, not from itself.
        let blob_end = PAYLOAD_HEAD + blob.len();
        for cut in 0..wire.len() {
            if data.is_some() && cut == blob_end {
                assert_eq!(payload_parse(&wire[..cut]), Ok((blob, None)), "shape {shape}");
                continue;
            }
            assert!(
                sal_rows(&wire[..cut]).is_err(),
                "shape {shape}: a {cut}/{}-byte prefix decodes",
                wire.len()
            );
        }
    }
}

/// A payload holds nothing behind what its head and its data block declare.
#[test]
fn a_payload_with_bytes_it_does_not_declare_is_refused() {
    let sd = make_schema_u64_i64();
    let batch = certified(make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]));
    let record = encode_schema_record(&sd);

    let mut no_rows = payload(b"blob", None);
    assert_eq!(sal_rows(&no_rows).map(|rows| rows.is_some()), Ok(false));
    no_rows.push(0);
    assert!(
        sal_rows(&no_rows).is_err(),
        "a byte past the blob reads as a data block, which does not decode"
    );

    let mut rows = payload(&record, Some(batch.wire_whole().unwrap()));
    assert!(sal_rows(&rows).expect("the payload decodes").is_some());
    rows.extend_from_slice(&[0; 8]);
    assert!(sal_rows(&rows).is_err(), "bytes past the data block");
}

/// A payload's rows are laid out under the schema `known` answers for its
/// record, else under the record itself.
#[test]
fn a_payload_lays_its_rows_out_under_the_known_schema_or_its_record() {
    let sd = make_schema_u64_i64();
    let batch = certified(make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]));
    let junk = [0xFFu8; 12];
    let wire = payload(&junk, Some(batch.wire_whole().unwrap()));
    let got = payload_rows(&wire, |record| {
        assert_eq!(record, junk.as_slice());
        Some(sd)
    })
    .expect("the known schema lays the rows out")
    .1
    .expect("rows");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));
    assert!(sal_rows(&wire).is_err(), "the junk record itself does not decode");

    let record = encode_schema_record(&sd);
    let wire = payload(&record, Some(batch.wire_whole().unwrap()));
    let got = sal_rows(&wire).expect("the record decodes").expect("rows");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));
}

/// A payload is its blob's length as a little-endian `u64`, the blob, then the
/// data block. The bytes are pinned: a change to them moves `SAL_LAYOUT`.
#[test]
fn a_payload_is_its_blob_length_the_blob_and_the_block() {
    let sd = make_schema_u64_i64();
    let batch = certified(make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]));
    let record = encode_schema_record(&sd);
    let rows = batch.wire_whole().unwrap();
    let mut block = vec![0; rows.byte_size()];
    rows.encode(&mut block);

    let wire = payload(&record, Some(rows));
    let mut want = (record.len() as u64).to_le_bytes().to_vec();
    want.extend_from_slice(&record);
    want.extend_from_slice(&block);
    assert_eq!(wire, want, "the payload layout moved: bump `SAL_LAYOUT`");
    assert_eq!(payload(b"abc", None), [3, 0, 0, 0, 0, 0, 0, 0, b'a', b'b', b'c']);
}
