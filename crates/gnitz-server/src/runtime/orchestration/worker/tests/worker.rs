use super::*;
use crate::runtime::test_support::decode_continuation;
use crate::runtime::w2m::{self, W2mReceiver};
use gnitz_engine::catalog::PUBLIC_SCHEMA_ID;
use gnitz_engine_testkit::{col_def, make_batch_raw, make_schema_u64_i64, u64_pk_schema, CatalogTestExt};
use gnitz_store::schema::SchemaColumn;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_wire::type_code;

/// `buffer_pending_delta` appends into an existing entry rather than
/// replacing it — the shape the live push path and boot SAL replay share.
#[test]
fn pending_deltas_accumulate_per_relation() {
    let schema = make_schema_u64_i64();
    let mut pending: HashMap<i64, Batch> = HashMap::new();

    buffer_pending_delta(&mut pending, 100, make_batch_raw(&schema, &[(1, 1, 10)]));
    assert_eq!(pending[&100].count, 1);
    buffer_pending_delta(&mut pending, 100, make_batch_raw(&schema, &[(2, 1, 20)]));
    assert_eq!(pending[&100].count, 2, "a second delta appends to the same table");
    buffer_pending_delta(&mut pending, 200, make_batch_raw(&schema, &[(3, 1, 30)]));
    assert_eq!(pending[&200].count, 1, "a different table gets its own entry");
}

/// Stage 0 wire-protocol contract: every reply helper (`send_ack`,
/// `send_response`, `send_error`) must echo the inbound request_id back
/// on the W2M region so the master reactor can route it. We fake out the
/// W2M writer with a real anonymous mmap, fire each helper with a
/// distinct id, then read the messages back through `decode_wire` and
/// assert the ids round-trip.
#[test]
fn send_helpers_echo_the_request_id() {
    use crate::runtime::wire as ipc;
    let (region, w2m_writer) = make_ring();
    let region_ptr = region.ptr();

    let mut wp = make_test_worker(std::ptr::null_mut(), w2m_writer);

    let req_ack: u64 = 42;
    let req_resp: u64 = 0xCAFE_BABE_DEAD_BEEF;
    let req_err: u64 = u64::MAX;
    wp.send_ack(7, req_ack);
    // Pass ReplySchema::ClientAuthored: it emits no block and so consults
    // no catalog, which this test does not have (null catalog pointer).
    // The id round-trip is the assertion of interest.
    let schema = make_schema_u64_i64();
    wp.send_response(8, None, ReplySchema::ClientAuthored(&schema), req_resp, 0, 0u128)
        .unwrap();
    wp.send_error("boom", req_err);

    let decoded_ids: Vec<u64> = walk_frames(region_ptr)
        .iter()
        .map(|(_, frame)| ipc::decode_wire_ipc(frame).expect("decode_wire_ipc").control.request_id)
        .collect();
    assert_eq!(decoded_ids, vec![req_ack, req_resp, req_err]);
}

/// `seek_col_idx` carries `pack_pk_cols(cols)` — the packed flag (bit 63) is
/// always set, so it is never 0 and never collides with the PK sentinel —
/// optionally OR'd with the holder directive on bit 62, which must survive
/// the round trip without disturbing the column list.
#[test]
fn from_wire_decodes_the_column_list_with_and_without_the_holder_directive() {
    assert!(matches!(HasPkLookup::from_wire(0), HasPkLookup::PrimaryKey));
    for cols in [&[0u32][..], &[3][..], &[63][..], &[1, 4][..], &[0, 2, 5, 7][..]] {
        let packed = gnitz_wire::pack_pk_cols(cols);
        assert_ne!(packed, 0);
        for want in [false, true] {
            let word = packed | if want { gnitz_wire::HAS_PK_WANT_HOLDER } else { 0 };
            match HasPkLookup::from_wire(word) {
                HasPkLookup::SecondaryIndex {
                    cols: decoded,
                    want_holder,
                } => {
                    assert_eq!(decoded.as_slice(), cols);
                    assert_eq!(want_holder, want);
                }
                HasPkLookup::PrimaryKey => panic!("packed list must decode to SecondaryIndex"),
            }
        }
    }
}

// -- Walk-the-matrix dispatch tests ---------------------------------------

/// The one test constructor for `WorkerProcess`, through the production one so
/// a new field cannot be missed here. A zeroed `SalReader` is a null log: only
/// the SAL-drain tests read one, and they assign a real reader afterwards. The
/// budget is pinned rather than read from the environment, so a shell that
/// exports `GNITZ_REPLY_FRAME_BUDGET` does not reshape these frames.
fn make_test_worker(catalog: *mut CatalogEngine, writer: W2mWriter) -> WorkerProcess {
    let mut wp = WorkerProcess::new(0, catalog, unsafe { std::mem::zeroed() }, writer, HashMap::new());
    wp.reply_frame_budget = ipc::FRAME_CAP;
    wp
}

/// One SAL group as the matrix tests hand it to `dispatch`: a kind, a
/// target and a round. `slots` stays 0, so the "not for us" guard never
/// fires and every arm is reached.
fn sal_msg(kind: SalMessageKind, target_id: u32, lsn: u64) -> SalMessage {
    SalMessage {
        kind,
        target_id,
        lsn,
        ..Default::default()
    }
}

/// Build a worker that's safe for `dispatch` calls whose behavior
/// does not enter the catalog (Tick/DdlSync/ExchangeRelay inside an
/// exchange wait, plus ExchangeRelay at top-level which warns
/// without touching the catalog).
///
/// The W2M ring is unused by these arms; sal_reader is also unused
/// because we drive `dispatch` directly.
fn make_worker_for_matrix() -> WorkerProcess {
    make_test_worker(std::ptr::null_mut(), unsafe { std::mem::zeroed() })
}

/// Tick inside an exchange wait MUST defer to `deferred_replay`,
/// not run inline. Cited bug: an inline tick eval re-enters `view_id`
/// with a different source and produces schema-mismatched relays.
#[test]
fn tick_defers_inside_exchange() {
    let mut wp = make_worker_for_matrix();
    let ctx = DispatchContext::InEval { relay_wait: (100, 5) };
    assert!(wp.exchange.deferred_replay.is_empty());
    assert!(wp.dispatch(ctx, &sal_msg(SalMessageKind::Tick, 999, 7), None).is_none());
    assert_eq!(wp.exchange.deferred_replay.len(), 1);
    assert!(
        matches!(
            wp.exchange.deferred_replay[0],
            Deferred::Tick {
                target_id: 999,
                round: 7,
                ..
            }
        ),
        "the Tick's target and its round must both be carried into the replay queue"
    );
}

/// A **delta** read inside an exchange wait must defer, carrying the whole
/// request — `seek_pk` above all, which is where the master put the
/// interval's upper cut. A replayed read that lost it would cut at `T = 0`,
/// return nothing, and still be answered with a terminal frame reporting the
/// real `T`: the client would advance its cursor over rounds it never
/// received. It shares the tick's FIFO, so the replay order is SAL order.
#[test]
fn delta_read_defers_inside_exchange_with_its_whole_request() {
    let mut wp = make_worker_for_matrix();
    let ctx = DispatchContext::InEval { relay_wait: (100, 5) };
    let frame = Box::leak(
        ipc::WireMsg {
            target_id: 77,
            client_id: 0xC1,
            seek_pk: 4242,
            seek_pk_extra: &[9, 8, 7],
            ..Default::default()
        }
        .encode_to_vec()
        .into_boxed_slice(),
    );
    // A tick first, so the shared FIFO's insertion order is observable.
    assert!(wp.dispatch(ctx, &sal_msg(SalMessageKind::Tick, 999, 3), None).is_none());
    assert!(wp
        .dispatch(
            ctx,
            &sal_msg(SalMessageKind::ScanSpec { delta: true }, 77, 0),
            Some(frame)
        )
        .is_none());
    assert_eq!(wp.exchange.deferred_replay.len(), 2, "one queue, in SAL order");
    assert!(matches!(wp.exchange.deferred_replay[0], Deferred::Tick { .. }));
    match &wp.exchange.deferred_replay[1] {
        Deferred::DeltaRead {
            target_id,
            client_id,
            seek_pk,
            seek_pk_extra,
            ..
        } => {
            assert_eq!((*target_id, *client_id, *seek_pk), (77, 0xC1, 4242));
            assert_eq!(seek_pk_extra.as_slice(), &[9, 8, 7]);
        }
        other => panic!("a delta read must defer as a DeltaRead, got {other:?}"),
    }
}

/// Encode a header-only ExchangeRelay wire frame (schema, no data batch)
/// whose control block echoes `source_id` via `seek_pk`, as the master's
/// `emit_relay_with_decision` does. Leaked to `'static` for `dispatch`.
/// A DECODABLE frame is now required — a corrupt/undecodable relay fail-stops
/// the worker (mirrors the DdlSync decode arm) rather than defaulting to an
/// empty batch, so tests can no longer feed `&[]`.
fn encode_relay_frame(target_id: u64, source_id: u128, schema: &SchemaDescriptor) -> &'static [u8] {
    // No data batch — a header-only relay. `seek_pk` echoes the source_id the
    // waiter matches on; `seek_col_idx` 0 is BACKFILL_DECISION_CONTINUE.
    let block = gnitz_engine::catalog::encode_schema_block(schema, target_id as u32);
    let msg = ipc::WireMsg {
        target_id,
        seek_pk: source_id,
        schema_block: Some(&block),
        ..Default::default()
    };
    Box::leak(msg.encode_to_vec().into_boxed_slice())
}

/// Encode a wire frame carrying `batch` under `schema`. Leaked to `'static`
/// for `dispatch`, which takes its payload from the SAL mapping.
fn encode_data_frame(target_id: u64, schema: &SchemaDescriptor, batch: &Batch) -> &'static [u8] {
    let block = gnitz_engine::catalog::encode_schema_block(schema, target_id as u32);
    let msg = ipc::WireMsg {
        target_id,
        schema_block: Some(&block),
        data: ipc::WireData::Whole(Some(batch)),
        ..Default::default()
    };
    Box::leak(msg.encode_to_vec().into_boxed_slice())
}

/// ExchangeRelay inside an exchange wait whose `(view_id, source_id)`
/// matches `want_key` returns `RelayMatched(batch)`; a non-matching
/// pair is parked in `pending_relays`.
#[test]
fn exchange_relay_inside_exchange() {
    let mut wp = make_worker_for_matrix();
    let schema = make_schema_u64_i64();
    let want_key = (100, 0);
    let ctx = DispatchContext::InEval { relay_wait: want_key };

    // Mismatched view (target_id=200 ≠ want 100): parked under (200, 0).
    let frame = encode_relay_frame(200, 0, &schema);
    assert!(wp
        .dispatch(ctx, &sal_msg(SalMessageKind::ExchangeRelay, 200, 0), Some(frame))
        .is_none());
    assert!(
        wp.exchange.pending_relays.contains_key(&(200, 0)),
        "non-matching relay must be parked in pending_relays"
    );

    // Matching key (target_id=100, source_id=0 == want_key): returns the batch.
    let frame = encode_relay_frame(100, 0, &schema);
    assert!(
        wp.dispatch(ctx, &sal_msg(SalMessageKind::ExchangeRelay, 100, 0), Some(frame))
            .is_some(),
        "a key-matching relay must short-circuit out of dispatch with its batch"
    );
}

/// ExchangeRelay at TopLevel is a protocol bug — it can only arrive
/// while the worker is blocked in `do_exchange_wait`. The dispatcher
/// warns and continues; no observable state change.
#[test]
fn exchange_relay_top_level_warns_and_continues() {
    let mut wp = make_worker_for_matrix();
    let empty: &'static [u8] = &[];
    assert!(wp
        .dispatch(
            DispatchContext::TopLevel,
            &sal_msg(SalMessageKind::ExchangeRelay, 100, 0),
            Some(empty)
        )
        .is_none());
    assert!(
        wp.exchange.pending_relays.is_empty(),
        "TopLevel must NOT park relays — they belong to do_exchange_wait"
    );
}

/// DdlSync inside an exchange wait MUST stage its batch in
/// `exchange.deferred` rather than applying it: an inline catalog mutation
/// races the in-flight DAG evaluation. (Adding a `SalMessageKind` without
/// deciding both of its cells is already a compile error — `dispatch`
/// matches exhaustively on `(ctx, kind)` — so what needs a test is the
/// defer decision itself.)
#[test]
fn ddl_sync_defers_inside_exchange() {
    let mut wp = make_worker_for_matrix();
    let schema = make_schema_u64_i64();
    let ctx = DispatchContext::InEval { relay_wait: (0, 0) };

    let frame = encode_data_frame(42, &schema, &make_batch_raw(&schema, &[(1, 1, 10)]));
    assert!(wp
        .dispatch(ctx, &sal_msg(SalMessageKind::DdlSync, 42, 0), Some(frame))
        .is_none());
    assert_eq!(
        wp.exchange.deferred.len(),
        1,
        "DdlSync must stage its batch, not apply it"
    );
    assert_eq!(wp.exchange.deferred[0].target_id, 42);
    assert!(
        wp.exchange.deferred_replay.is_empty(),
        "DdlSync must not touch the deferred-tick queue"
    );
}

// -- next_sal_message invariant tests -------------------------------------

/// The SAL read gate, driven through `next_sal_message`:
///
/// 1. Groups in the expected epoch are consumed in order.
/// 2. A group from a *later* epoch is rejected and stays parked — repeatedly,
///    so the cursor did not advance past it.
/// 3. `SalReader::rewind` moves to cursor 0 in the next epoch, which is
///    exactly where the master writes after its own reset.
#[test]
fn next_sal_message_gates_on_the_epoch() {
    use crate::runtime::sal::fixtures::TestLog;
    use crate::runtime::sal::SalReader;

    const SAL_SIZE: usize = 1 << 20;
    // Single worker; each group puts a one-byte payload at slot 0.
    let sal = TestLog::new(SAL_SIZE, 1, 1);
    let write = |target, lsn, kind, epoch| {
        sal.seek(sal.cursor(), epoch);
        sal.write(target, lsn, kind, &[&[0u8; 1]]);
    };

    write(42, 100, SalMessageKind::Push, 1);
    write(43, 101, SalMessageKind::DdlSync, 1);
    // A group from the next epoch, ahead of the reader.
    write(44, 102, SalMessageKind::Push, 2);

    let mut wp = make_test_worker(std::ptr::null_mut(), unsafe { std::mem::zeroed() });
    wp.sal_reader = unsafe { SalReader::new(sal.ptr() as *const u8, 0, SAL_SIZE, -1, 1) };

    let next = |wp: &mut WorkerProcess| wp.next_sal_message().map(|m| (m.kind, m.target_id));
    assert_eq!(next(&mut wp), Some((SalMessageKind::Push, 42)));
    assert_eq!(next(&mut wp), Some((SalMessageKind::DdlSync, 43)));

    // The epoch-2 group parks: rejected now, and still rejected on a retry —
    // the cursor did not slip past it.
    assert!(wp.next_sal_message().is_none(), "an epoch-ahead group must park");
    assert!(wp.next_sal_message().is_none(), "and stay parked");

    // After the rewind the reader is at cursor 0 in epoch 2, where the master
    // writes its first post-checkpoint group.
    sal.seek(0, 2);
    sal.write(77, 103, SalMessageKind::Push, &[&[0u8; 1]]);
    wp.sal_reader.rewind();
    assert_eq!(next(&mut wp), Some((SalMessageKind::Push, 77)));
}

// -- pending stream chunking tests -----------------------------------------------

/// A production-sized ring and its writer. The mmap reservation is
/// lazily populated, so the 1 GiB backing costs address space, not RAM.
fn make_ring() -> (crate::runtime::test_support::SharedRegion, W2mWriter) {
    let region = unsafe { w2m::fixtures::test_ring(w2m::W2M_REGION_SIZE) };
    let writer = W2mWriter::new(region.ptr());
    (region, writer)
}

/// A U64 PK with a stride-4 payload column: the shape whose wire block pads
/// between regions, so its size is monotone in the row count but not affine.
fn padded_schema() -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(type_code::U32, 0))
}

/// `n` rows of `schema`, PK and payload both counting from 0.
fn make_n_row_batch(schema: SchemaDescriptor, n: usize) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n as u64).map(|i| (i, 1, i as i64)).collect();
    make_batch_raw(&schema, &rows)
}

/// Row `row`'s PK widened from its OPK bytes — `Batch::get_pk` for a
/// borrowed `MemBatch`.
fn mem_pk(b: &gnitz_store::storage::MemBatch<'_>, row: usize) -> u128 {
    gnitz_wire::widen_pk_be(b.get_pk_bytes(row), b.pk_stride as usize)
}

fn consume_one(ptr: *mut u8) -> Vec<u8> {
    let (_, frame) = walk_frames(ptr).into_iter().next().expect("expected one ring message");
    frame
}

/// Wire size of a `count`-row range of `batch`, with an optional schema block.
fn range_size(batch: &Batch, count: usize, prebuilt: Option<&[u8]>) -> usize {
    ipc::WireMsg {
        data: ipc::WireData::Range {
            batch,
            start_row: 0,
            count,
        },
        schema_block: prebuilt,
        ..Default::default()
    }
    .size()
}

/// Every non-terminal frame of a train fills its budget to within one row:
/// one more row would exceed it. Both an 8-aligned and a padded schema, since
/// a chunk sized by inverting a linear model overshoots on the padded one.
#[test]
fn train_frames_fill_the_budget_to_within_one_row() {
    for (label, batch) in [
        ("8-aligned", make_n_row_batch(make_schema_u64_i64(), 40)),
        ("padded", make_n_row_batch(padded_schema(), 40)),
    ] {
        let block = Rc::new(gnitz_engine::catalog::encode_schema_block(&batch.schema, 1));
        let per_row = batch.wire_byte_size_range(2) - batch.wire_byte_size_range(1);
        // Room for four rows beside the schema block on the first frame.
        let budget = range_size(&batch, 4, Some(block.as_slice()));

        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        wp.reply_frame_budget = budget;
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch),
            request_id: 5,
            client_id: 0,
            target_id: 1,
            prebuilt_schema: Some(block),
            server_version: 0,
            kind: PendingScanKind::Chunked { next_row: 0 },
        });
        let mut passes = 0;
        while !wp.pending_streams.is_empty() {
            wp.emit_pending_scan_chunk();
            passes += 1;
            assert!(passes < 50, "{label}: the train must drain within a bounded pass count");
        }

        let frames = walk_frames(ptr);
        assert!(
            frames.len() >= 2,
            "{label}: 40 rows at a 4-row budget must span several frames"
        );
        for (i, (_, bytes)) in frames.iter().enumerate() {
            assert!(bytes.len() <= budget, "{label}: frame {i} is {} bytes", bytes.len());
            if i + 1 < frames.len() {
                assert!(
                    bytes.len() + per_row > budget,
                    "{label}: frame {i} is {} bytes of a {budget}-byte budget: another row fits",
                    bytes.len()
                );
            }
        }
    }
}

/// First (and only) PendingScan chunk — next_row == 0, so the prebuilt schema
/// block must appear in the frame and decode_wire_ipc must succeed without a hint.
/// `force_fifo` is the whole difference between a splittable reply emitting
/// inline and the same reply queueing through `pending_streams` — which is what
/// puts a multi-scan's relations on the ring in request order. Either way its
/// lone chunk is byte-shaped the same.
#[test]
fn force_fifo_decides_whether_a_fitting_reply_emits_inline_or_queues() {
    let schema = make_schema_u64_i64();
    for force_fifo in [false, true] {
        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);

        wp.send_scan_response(
            1,
            Rc::new(make_n_row_batch(schema, 5)),
            ReplySchema::ClientAuthored(&schema),
            3,
            0,
            0,
            force_fifo,
        )
        .expect("a small batch must not error");

        if force_fifo {
            assert_eq!(wp.pending_streams.len(), 1, "force_fifo must enqueue, not emit");
            assert!(
                matches!(
                    wp.pending_streams.front().map(|p| &p.kind),
                    Some(PendingScanKind::Chunked { .. })
                ),
                "a splittable reply queues as the Chunked variant"
            );
            wp.emit_pending_scan_chunk();
        }
        assert!(wp.pending_streams.is_empty(), "the queue ends empty either way");

        let data = consume_one(ptr);
        let ctrl = gnitz_wire::control::peek_control_block_ipc(&data).expect("peek_control_block");
        assert_eq!(ctrl.status, STATUS_OK);
        assert_ne!(ctrl.flags & FLAG_SCAN_LAST, 0);
        assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);

        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        let decoded = decode_continuation(&data, &schema, 0, &mut offsets).expect("decode with schema hint");
        let b = decoded.data_batch.as_ref().expect("data block");
        assert_eq!(b.count, 5);
        for i in 0..5usize {
            assert_eq!(mem_pk(b, i), i as u128);
        }
    }
}

/// A blob-bearing (STRING/TEXT) reply must FIFO too: under force_fifo it
/// queues as `PendingScanKind::WholeBlob` (not immediately emitted), and
/// `emit_pending_scan_chunk` emits its one frame with
/// FLAG_CONTINUATION | FLAG_SCAN_LAST, then pops. This is the mainline case a
/// TEXT dimension table hits.
#[test]
fn force_fifo_queues_a_whole_blob_reply() {
    let dir = gnitz_engine_testkit::scratch_dir("worker", "force_fifo_text");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("s", type_code::STRING)];
    let tid = gnitz_engine::catalog::FIRST_USER_TABLE_ID;
    engine
        .register_table(tid, PUBLIC_SCHEMA_ID, "tfifo", &cols, &[0])
        .unwrap();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    assert!(schema.has_german_string(), "a STRING schema carries a German string");

    let (region, writer) = make_ring();
    let ptr = region.ptr();
    let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);

    // One all-zero TEXT row (empty inline string), well under MAX_W2M_MSG.
    let batch = Batch::zeroed(schema, 1);
    wp.send_scan_response(tid as u64, Rc::new(batch), ReplySchema::Table(&schema), 5, 0, 0, true)
        .unwrap();
    assert_eq!(wp.pending_streams.len(), 1);
    assert!(
        matches!(
            wp.pending_streams.front().map(|p| &p.kind),
            Some(PendingScanKind::WholeBlob)
        ),
        "a TEXT reply must queue as the WholeBlob variant under force_fifo"
    );
    assert!(walk_frames(ptr).is_empty(), "nothing is emitted at enqueue time");

    wp.emit_pending_scan_chunk();
    assert!(wp.pending_streams.is_empty(), "the single blob frame pops the train");
    let frames = walk_frames(ptr);
    assert_eq!(frames.len(), 1);
    let ctrl = gnitz_wire::control::peek_control_block_ipc(&frames[0].1).unwrap();
    assert_eq!(ctrl.status, STATUS_OK);
    assert_ne!(ctrl.flags & FLAG_SCAN_LAST, 0);
    assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);
    // Decodes via the blob-capable path; the first frame carries the schema block.
    let decoded = ipc::decode_wire_ipc(&frames[0].1).expect("the blob frame decodes standalone");
    assert!(decoded.schema.is_some(), "the frame carries the schema block");
    assert_eq!(decoded.data_batch.map(|b| b.count).unwrap_or(0), 1);

    engine.close();
}

// -- stream_batch_response / pending_streams FIFO tests --------------------

/// Read every published message off a test ring in publish order,
/// returning `(ring_prefix_req_id, frame_bytes)`.
fn walk_frames(ptr: *mut u8) -> Vec<(u32, Vec<u8>)> {
    let receiver = W2mReceiver::new(vec![ptr]);
    let mut out = Vec::new();
    while let Some(slot) = receiver.try_read_slot(0) {
        out.push((slot.internal_req_id, slot.bytes().to_vec()));
    }
    out
}

/// A batch of `count` decodable all-zero rows — used to cross a wire-size
/// limit without writing that many real bytes.
/// Two queued trains drain strictly FIFO: every frame of train A
/// (multi-chunk, terminal FLAG_SCAN_LAST) precedes train B's, and B's
/// first chunk carries B's own schema block.
#[test]
fn pending_streams_drain_two_trains_fifo() {
    let schema_a = make_schema_u64_i64();
    // B's schema has 3 columns so its frames are distinguishable from A's.
    let schema_b = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    );
    let batch_a = make_n_row_batch(schema_a, 10);
    let batch_b = make_n_row_batch(schema_b, 5);
    let block_a = Rc::new(gnitz_engine::catalog::encode_schema_block(&schema_a, 1));
    let block_b = Rc::new(gnitz_engine::catalog::encode_schema_block(&schema_b, 2));

    // Budget: exactly the first chunk's size at 4 rows (A's schema block
    // included), so train A's 10 rows span at least two frames.
    let budget = range_size(&batch_a, 4, Some(block_a.as_slice()));

    let (region, writer) = make_ring();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.pending_streams.push_back(PendingScan {
        batch: Rc::new(batch_a),
        request_id: 11,
        client_id: 0,
        target_id: 1,
        prebuilt_schema: Some(block_a),
        server_version: 0,
        kind: PendingScanKind::Chunked { next_row: 0 },
    });
    wp.pending_streams.push_back(PendingScan {
        batch: Rc::new(batch_b),
        request_id: 22,
        client_id: 0,
        target_id: 2,
        prebuilt_schema: Some(block_b),
        server_version: 0,
        kind: PendingScanKind::Chunked { next_row: 0 },
    });

    // One chunk per pass, as drain_sal drives it.
    wp.reply_frame_budget = budget;
    let mut passes = 0;
    while !wp.pending_streams.is_empty() {
        wp.emit_pending_scan_chunk();
        passes += 1;
        assert!(passes < 50, "trains must drain within a bounded pass count");
    }

    let frames = walk_frames(ptr);
    let a_frames: Vec<_> = frames.iter().filter(|(req, _)| *req == 11).collect();
    let b_frames: Vec<_> = frames.iter().filter(|(req, _)| *req == 22).collect();
    assert!(a_frames.len() >= 2, "budget must split train A into multiple chunks");
    assert!(!b_frames.is_empty());
    let first_b = frames.iter().position(|(req, _)| *req == 22).unwrap();
    let last_a = frames.iter().rposition(|(req, _)| *req == 11).unwrap();
    assert!(last_a < first_b, "train A's frames must FULLY precede train B's");

    // Per train: every frame is a continuation, only the last is terminal,
    // the chunks cover every row exactly once, and the FIRST chunk carries
    // that train's own schema block (decodes standalone with its column
    // count; continuations decode only against the train's schema hint).
    for (req, schema, ncols, total_rows) in [(11u32, &schema_a, 2usize, 10usize), (22u32, &schema_b, 3usize, 5usize)] {
        let train: Vec<_> = frames.iter().filter(|(r, _)| *r == req).collect();
        let mut rows = 0usize;
        for (i, (_, bytes)) in train.iter().enumerate() {
            let ctrl = gnitz_wire::control::peek_control_block_ipc(bytes).expect("ctrl");
            assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);
            let is_last = i == train.len() - 1;
            assert_eq!(
                ctrl.flags & FLAG_SCAN_LAST != 0,
                is_last,
                "FLAG_SCAN_LAST only on the train's terminal chunk"
            );
            if i == 0 {
                let decoded = ipc::decode_wire_ipc(bytes).expect("first chunk must decode standalone");
                let s = decoded.schema.expect("first chunk carries a schema block");
                assert_eq!(s.num_columns(), ncols, "the block is this train's schema");
                rows += decoded.data_batch.map(|b| b.count).unwrap_or(0);
            } else {
                assert!(
                    ipc::decode_wire_ipc(bytes).is_err(),
                    "a continuation carries no schema block, so it cannot decode standalone"
                );
                let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
                let decoded = decode_continuation(bytes, schema, 0, &mut offsets)
                    .expect("continuation decodes against the schema hint");
                rows += decoded.data_batch.map(|b| b.count).unwrap_or(0);
            }
        }
        assert_eq!(rows, total_rows, "the train's chunks cover all rows exactly once");
    }
}

/// A fitting result through `stream_batch_response` must be byte-identical
/// to `send_response` (same flags, `seek_pk` + `request_id` echo): unicast
/// consumers forward these slots verbatim, so the single-frame wire shape
/// must not change. Covers both the non-empty and the empty-result paths.
#[test]
fn a_fitting_stream_batch_is_byte_identical_to_send_response() {
    let schema = make_schema_u64_i64();
    let batch = make_n_row_batch(schema, 5);

    let (region_ref, writer_ref) = make_ring();
    let (region_new, writer_new) = make_ring();
    let ptr_ref = region_ref.ptr();
    let ptr_new = region_new.ptr();
    let mut wp_ref = make_test_worker(std::ptr::null_mut(), writer_ref);
    let mut wp_new = make_test_worker(std::ptr::null_mut(), writer_new);

    let req = 0xCAFE_u64;
    let client = 7u64;
    let pk = 0xDEAD_BEEF_u128;
    wp_ref
        .send_response(8, Some(&batch), ReplySchema::ClientAuthored(&schema), req, client, pk)
        .unwrap();
    wp_ref
        .send_response(8, None, ReplySchema::ClientAuthored(&schema), req + 1, client, 0)
        .unwrap();

    assert!(wp_new
        .stream_batch_response(
            8,
            Some(batch.clone()),
            ReplySchema::ClientAuthored(&schema),
            req,
            client,
            pk
        )
        .is_ok());
    assert!(wp_new
        .stream_batch_response(8, None, ReplySchema::ClientAuthored(&schema), req + 1, client, 0)
        .is_ok());
    assert!(wp_new.pending_streams.is_empty(), "fitting results never enqueue");

    let ref_frames = walk_frames(ptr_ref);
    let new_frames = walk_frames(ptr_new);
    assert_eq!(ref_frames.len(), 2);
    assert_eq!(
        ref_frames, new_frames,
        "single-frame stream_batch_response must be byte-identical to send_response"
    );
}

/// An oversized splittable result enqueues a train instead of emitting a
/// frame past `ipc::FRAME_CAP`; nothing is emitted until drain_sal.
#[test]
fn an_oversized_stream_batch_enqueues_a_train() {
    let schema = make_schema_u64_i64(); // 32 B/row on the wire
    let rows = (ipc::FRAME_CAP / 32) + 4096;
    let batch = Batch::zeroed(schema, rows);

    let (region, writer) = make_ring();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    let err = wp.stream_batch_response(3, Some(batch), ReplySchema::ClientAuthored(&schema), 5, 9, 0);
    assert!(err.is_ok(), "an oversized splittable result must chunk, not error");
    assert_eq!(wp.pending_streams.len(), 1);
    let ps = wp.pending_streams.front().unwrap();
    let PendingScanKind::Chunked { next_row, .. } = &ps.kind else {
        panic!("an oversized splittable result must enqueue a Chunked train");
    };
    assert_eq!(*next_row, 0);
    assert_eq!(ps.request_id, 5);
    assert_eq!(ps.client_id, 9);
    assert!(
        walk_frames(ptr).is_empty(),
        "the train's first chunk is emitted by drain_sal, not at enqueue time"
    );
}

/// An oversized blob-bearing (STRING) result returns the clean error — the
/// variable-width streaming chunker is an explicit non-goal.
#[test]
fn an_oversized_string_batch_cannot_be_chunked() {
    let dir = gnitz_engine_testkit::scratch_dir("worker", "string_oversized");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("s", type_code::STRING)];
    let tid = gnitz_engine::catalog::FIRST_USER_TABLE_ID;
    engine
        .register_table(tid, PUBLIC_SCHEMA_ID, "tstr", &cols, &[0])
        .unwrap();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    assert!(schema.has_german_string());

    // 40 B/row (8 pk + 8 weight + 8 null + 16 string struct), empty blob.
    let rows = (ipc::FRAME_CAP / 40) + 4096;
    let batch = Batch::zeroed(schema, rows);

    let (_region, writer) = make_ring();
    let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);
    let err = wp
        .stream_batch_response(tid as u64, Some(batch), ReplySchema::Table(&schema), 5, 0, 0)
        .expect_err("oversized STRING result must surface the clean error");
    assert!(
        err.text.contains("cannot be chunked"),
        "error names the limitation: {err}"
    );
    assert!(wp.pending_streams.is_empty(), "a blob-bearing result never enqueues");

    engine.close();
}

/// A projected (gather) reply schema must ride a ONE-OFF wire block: the
/// table-keyed cache must neither serve it (the master would decode
/// projected rows with the base table's stride) nor store it (a later
/// table reply would be decoded with the projected stride).
#[test]
fn a_projected_stream_batch_carries_a_one_off_block() {
    let dir = gnitz_engine_testkit::scratch_dir("worker", "projected_one_off");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![
        col_def("id", type_code::U64),
        col_def("a", type_code::U64),
        col_def("b", type_code::U64),
    ];
    let tid = gnitz_engine::catalog::FIRST_USER_TABLE_ID;
    engine
        .register_table(tid, PUBLIC_SCHEMA_ID, "tproj", &cols, &[0])
        .unwrap();
    let table_schema = engine.registry().get_schema_desc(tid).unwrap();
    let projected = gnitz_store::schema::project_schema(&table_schema, &[1]).unwrap();
    assert_ne!(projected.num_columns(), table_schema.num_columns());

    let (region, writer) = make_ring();
    let ptr = region.ptr();
    let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);

    // Fitting projected reply: one frame carrying the projected schema.
    let small = Batch::zeroed(projected, 2);
    assert!(wp
        .stream_batch_response(tid as u64, Some(small), ReplySchema::OneOff(&projected), 5, 0, 0)
        .is_ok());
    let frames = walk_frames(ptr);
    assert_eq!(frames.len(), 1);
    let decoded = ipc::decode_wire_ipc(&frames[0].1).expect("decode projected reply");
    assert_eq!(
        decoded.schema.expect("schema block present").num_columns(),
        projected.num_columns()
    );

    // Oversized projected reply: the queued train holds the one-off block.
    let rows = (ipc::FRAME_CAP / 32) + 4096;
    let big = Batch::zeroed(projected, rows);
    assert!(wp
        .stream_batch_response(tid as u64, Some(big), ReplySchema::OneOff(&projected), 6, 0, 0)
        .is_ok());
    assert_eq!(wp.pending_streams.len(), 1);
    let expected_block = gnitz_engine::catalog::encode_schema_block(&projected, tid as u32);
    let ps = wp.pending_streams.front().unwrap();
    assert!(
        matches!(ps.kind, PendingScanKind::Chunked { .. }),
        "an oversized projected reply must enqueue a Chunked train"
    );
    assert_eq!(
        ps.prebuilt_schema.as_deref().map(Vec::as_slice),
        Some(expected_block.as_slice()),
        "the train's schema block is the one-off projected block",
    );

    // Both paths left the table's cached wire block untouched.
    assert!(
        engine.get_cached_schema_wire_block(tid).is_none(),
        "a projected reply must never populate the table's schema-block cache"
    );

    engine.close();
}
