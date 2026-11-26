use super::*;

#[test]
fn a_hello_is_checked_at_its_prefix_and_classified() {
    assert_eq!(read_client_hello(&HELLO), Ok(&[][..]));

    let mut forged = client_hello(b"t");
    forged[0] ^= 1;
    assert_eq!(read_client_hello(&forged), Err(HelloError::Malformed));
    assert_eq!(read_client_hello(&HELLO[..7]), Err(HelloError::Malformed));
}

#[test]
fn a_client_hello_round_trips_its_token_and_refuses_a_longer_one() {
    for token in [&b""[..], b"gnitz_00", &[7u8; MAX_TOKEN_BYTES]] {
        let hello = client_hello(token);
        assert!(hello.len() <= HANDSHAKE_FRAME_MAX);
        assert_eq!(read_client_hello(&hello), Ok(token));
    }
    let long = [&HELLO[..], &[0u8; MAX_TOKEN_BYTES + 1]].concat();
    assert_eq!(read_client_hello(&long), Err(HelloError::Malformed));
}

#[test]
fn a_server_hello_carries_each_verdict() {
    for &status in AuthStatus::ALL {
        assert_eq!(read_server_hello(&server_hello(status)), Ok(status));
    }
    let mut unknown = server_hello(AuthStatus::Accepted);
    unknown[HELLO.len()] = crate::outside(AuthStatus::from_wire);
    assert_eq!(read_server_hello(&unknown), Err(HelloError::Malformed));
    assert_eq!(read_server_hello(&HELLO), Err(HelloError::Malformed));
    let long = [&server_hello(AuthStatus::Accepted)[..], b"x"].concat();
    assert_eq!(read_server_hello(&long), Err(HelloError::Malformed));
}

#[test]
fn a_refusing_hello_names_its_release_to_a_client_of_another_version() {
    let refusal = refusal_hello();
    // From this build's own version the refusal carries no verdict.
    assert_eq!(read_server_hello(&refusal), Err(HelloError::Malformed));

    // The version is the little-endian word at offset 4, read before what follows it.
    let mut other = refusal.clone();
    other[4] ^= 1;
    let peer = WIRE_VERSION ^ 1;
    let want = HelloError::Version { peer, release: Some(RELEASE.to_string()) };
    assert_eq!(read_server_hello(&other), Err(want));

    // What no release name is — a verdict byte from another version among
    // them — reads as a server naming none.
    for rest in [&b""[..], b"\x01", b"0.1 8", &[b'9'; MAX_RELEASE_BYTES + 1], b"\xff\xfe"] {
        let hello = [&other[..HELLO.len()], rest].concat();
        assert_eq!(
            read_server_hello(&hello),
            Err(HelloError::Version { peer, release: None }),
            "{rest:?}"
        );
    }
    // A client's HELLO carries its token where a refusal carries a release.
    let mut client = client_hello(b"gnitz_0.1.8");
    client[4] ^= 1;
    assert_eq!(
        read_client_hello(&client),
        Err(HelloError::Version { peer, release: None })
    );
}

/// One sample of every layout client and engine both decode, past the bytes
/// that carry [`WIRE_VERSION`] itself.
fn wire_layout_samples() -> Vec<u8> {
    use crate::multi_frame::{self, DeltaCursor, DeltaPollItem, ScanItem, SubId};
    use crate::*;

    let mut out = Vec::new();
    // A header with every field set, and a blob.
    let hdr = ControlHeader {
        status: WireStatus::StaleCatalog,
        target_id: 0x1111_2222_3333_4444,
        flags: WireFlags {
            verb: Some(ClientVerb::SnapshotPage),
            conflict_mode: WireConflictMode::Error,
            continuation: true,
            scan_last: true,
            lane: WireLane::SyncAnswer,
        },
        arg0: 0x5555_6666_7777_8888,
        arg1: 0x9999_AAAA_BBBB_CCCC,
    };
    append_frame(&mut out, &hdr, b"blob", None);
    // One frame per reply header the server builds.
    let sub = SubId::new(4).unwrap();
    for reply in [
        ControlHeader::ack(7, 0x5555_6666_7777_8888),
        ControlHeader::train(7, false),
        ControlHeader::train(7, true),
        ControlHeader::delta_terminal(7, DeltaCursor::from_pair(5, 6).unwrap()),
        ControlHeader::pushed_train(7, sub),
        ControlHeader::sync_answer(WireStatus::Ok),
    ] {
        append_frame(&mut out, &reply, &[], None);
    }

    // A frame of a write spanning several.
    let mut more = ControlHeader::naming(ClientVerb::PushTxn, Target::from(0), 0);
    more.flags.continuation = true;
    append_frame(&mut out, &more, &[], None);

    let target = Target { tid: 7, token: 9 };
    multi_frame::encode_scan_multi(&mut out, &[ScanItem { target, reply_layout: 3, spec: b"spec" }]);
    let poll = DeltaPollItem {
        view: target,
        from: DeltaCursor::from_pair(5, 6),
        reply_layout: 3,
        spec: b"spec",
    };
    multi_frame::encode_delta_poll(&mut out, &[poll, DeltaPollItem { from: None, ..poll }], SubId::new(4));
    out.extend(multi_frame::encode_held(
        [SubId::new(4), SubId::new(u64::MAX)].map(Option::unwrap),
    ));

    let cols = PkColList::from_slice(&[2, 0]);
    let rows = ReadSpec {
        bound: ReadBound::Range(KeyRange::new(cols, &[1], 2, 3)),
        predicate: vec![1, 2],
        sink: ReadSink {
            map: None,
            kind: SinkKind::Rows {
                cut: Some(RowsCut {
                    k: std::num::NonZeroU64::new(5).unwrap(),
                    order: vec![OrderKey { col: 3, desc: true, nulls_first: true }],
                }),
            },
        },
    };
    let fold = ReadSpec {
        bound: ReadBound::PkSet(PkKeys::from_sorted(1, vec![4, 9])),
        predicate: vec![],
        sink: ReadSink {
            map: Some(ComputeMap {
                program: vec![7],
                out_cols: vec![(TypeCode::I64, true)],
            }),
            kind: SinkKind::Fold(GroupAggs {
                group_cols: vec![2],
                aggs: vec![AggDescriptor { agg_op: AggFunc::Min, col_idx: 4 }],
            }),
        },
    };
    out.extend(rows.encode());
    out.extend(fold.encode());
    out.extend(ReadSpec::all_rows(ReadBound::None).encode());
    let set = KeyRange::with_spans(cols, &[1], [(2, 2), (3, 3)]).unwrap();
    out.extend(ReadSpec::all_rows(ReadBound::Range(set)).encode());

    // A data block: a compound (I16, U8) key, a weight, a null word, a STRING
    // column's heap-backed cell and the heap, part of it dead.
    let mut heap = Vec::new();
    let cell = encode_german_string(b"a value past the inline cell", &mut heap);
    let mut pk = Vec::new();
    push_opk(&mut pk, 2, key_image(TypeCode::I16, -2i16 as u128));
    push_opk(&mut pk, 1, 3);
    let regions: [&[u8]; 5] = [&pk, &(-1i64).to_le_bytes(), &1u64.to_le_bytes(), &cell, &heap];
    wal::append_block(&regions, 1, &mut out);

    let schema = [
        schema_record::RecordCol {
            ty: TypeCode::U64.into(),
            nullable: false,
            hidden: false,
            name: "k",
        },
        schema_record::RecordCol {
            ty: ColType::decimal(9),
            nullable: true,
            hidden: true,
            name: "v",
        },
    ];
    let record = schema_record::encode(schema.iter().copied(), &[0]);
    let desc = RelDescriptorBlob {
        class: RelClass::FedView,
        pk_repeats: true,
        serial: true,
        indexes: vec![RelIndex { cols, is_unique: true }],
        schema: &record,
    };
    out.extend(desc.encode());
    let page = snapshot::PageRequest {
        layout: 3,
        limit: 10,
        after: vec![None, Some(PkBuf::from_bytes(&[1, 2]))],
    };
    out.extend(page.encode());

    out.extend(&client_hello(b"token")[HELLO.len()..]);
    for &status in AuthStatus::ALL {
        out.extend(&server_hello(status)[HELLO.len()..]);
    }
    out
}

/// Every layout of [`wire_layout_samples`] moves with [`WIRE_EPOCH`].
#[test]
fn the_wire_layouts_are_pinned_to_the_epoch() {
    assert_eq!(
        (WIRE_EPOCH, crate::checksum(&wire_layout_samples())),
        (42, 0xc283_a799_cc30_cb09),
        "a client↔engine layout moved: bump WIRE_EPOCH and this digest together"
    );
}
