use super::*;
use crate::catalog::PUBLIC_SCHEMA_ID;
use crate::runtime::w2m::{self, W2mReceiver};
use crate::test_support::{col_def, make_batch_raw, make_schema_u64_i64, u64_pk_schema};
use gnitz_store::relation::Relation;
use gnitz_store::schema::SchemaColumn;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::BatchBuilder;
use gnitz_wire::type_code;

/// `buffer_pending_delta` appends into an existing entry rather than
/// replacing it — the shape the live push path and boot SAL replay share.
#[test]
fn pending_deltas_accumulate_per_relation() {
    let schema = make_schema_u64_i64();
    let mut pending: HashMap<i64, Batch> = HashMap::new();

    buffer_pending_delta(&mut pending, 100, make_batch_raw(&schema, &[(1, 1, 10)]));
    assert_eq!(pending[&100].len(), 1);
    buffer_pending_delta(&mut pending, 100, make_batch_raw(&schema, &[(2, 1, 20)]));
    assert_eq!(pending[&100].len(), 2, "a second delta appends to the same table");
    buffer_pending_delta(&mut pending, 200, make_batch_raw(&schema, &[(3, 1, 30)]));
    assert_eq!(pending[&200].len(), 1, "a different table gets its own entry");
}

/// Stage 0 wire-protocol contract: every reply helper (`send_ack`,
/// `send_response`, `send_fault`) must echo the inbound request_id back
/// on the W2M region so the master reactor can route it. We fake out the
/// W2M writer with a real anonymous mmap, fire each helper with a
/// distinct id, then read the messages back through `decode_wire` and
/// assert the ids round-trip.
#[test]
fn send_helpers_echo_the_request_id() {
    use crate::runtime::wire as ipc;
    let (region, w2m_writer) = ring_and_writer();
    let region_ptr = region.ptr();

    let mut wp = make_test_worker(std::ptr::null_mut(), w2m_writer);

    let req_ack: u64 = 42;
    let req_resp: u64 = 0xCAFE_BABE_DEAD_BEEF;
    let req_err: u64 = u64::MAX;
    wp.send_ack(7, req_ack);
    // Pass ReplySchema::ClientAuthored: it emits no block and so consults
    // no catalog, which this test does not have (null catalog pointer).
    // The id round-trip is the assertion of interest.
    wp.send_response(route(8, req_resp, 0), None, ReplySchema::ClientAuthored, 0u128, 0)
        .unwrap();
    wp.send_fault(&gnitz_wire::WireFault::from("boom"), req_err);

    let decoded_ids: Vec<u64> = walk_frames(region_ptr)
        .iter()
        .map(|(_, frame)| ipc::decode_wire_ipc(frame).expect("decode_wire_ipc").control.request_id)
        .collect();
    assert_eq!(decoded_ids, vec![req_ack, req_resp, req_err]);
}

// -- Walk-the-matrix dispatch tests ---------------------------------------

/// The one test constructor for `WorkerProcess`, through the production one so
/// a new field cannot be missed here. A zeroed `SalReader` is a null log: only
/// the SAL-drain tests read one, and they assign a real reader afterwards. The
/// budget is pinned rather than read from the environment, so a shell that
/// exports `GNITZ_REPLY_FRAME_BUDGET` does not reshape these frames.
fn make_test_worker(catalog: *mut CatalogEngine, writer: W2mWriter) -> WorkerProcess {
    let mut wp = WorkerProcess::new(0, catalog, unsafe { std::mem::zeroed() }, writer, -1, HashMap::new());
    wp.reply_frame_budget = ipc::FRAME_CAP;
    wp
}

/// The reply route every helper below takes, in the order `dispatch_inner`
/// resolves it. Inline-emitting; [`fifo_route`] is the queued twin.
fn route(target_id: u64, request_id: u64, client_id: u64) -> ReplyRoute {
    ReplyRoute {
        target_id,
        request_id,
        client_id,
        fifo: false,
    }
}

/// [`route`] for a group the master wrote as one of several.
fn fifo_route(target_id: u64, request_id: u64, client_id: u64) -> ReplyRoute {
    ReplyRoute {
        fifo: true,
        ..route(target_id, request_id, client_id)
    }
}

/// One SAL group as the matrix tests hand it to `dispatch`: a kind, a
/// target and a round. The slot bytes travel beside it, so the message itself
/// carries no payload.
fn sal_msg(kind: SalMessageKind, target_id: u32, lsn: u64) -> SalMessage {
    SalMessage {
        lsn,
        kind,
        zone_start: false,
        target_id,
        base: 0,
        payload: &[],
        dir: &[],
    }
}

/// A bare control block, as every command verb's slot carries, stamped with the
/// `target_id` a dispatcher reads it from. The write path builds header and slot
/// off one template, so a fixture must not let them disagree.
fn control_frame(target_id: u64) -> &'static [u8] {
    Box::leak(
        ipc::WireMsg { target_id, ..Default::default() }
            .encode_to_vec()
            .into_boxed_slice(),
    )
}

/// Build a worker that's safe for dispatch calls whose behavior
/// does not enter the catalog (Tick/DdlSync/ExchangeRelay inside an
/// exchange wait, plus ExchangeRelay at top-level which warns
/// without touching the catalog).
///
/// The W2M ring is unused by these arms; sal_reader is also unused
/// because we drive the dispatchers directly.
fn make_worker_for_matrix() -> WorkerProcess {
    make_test_worker(std::ptr::null_mut(), unsafe { std::mem::zeroed() })
}

/// Tick inside an exchange wait MUST defer to `deferred_replay`,
/// not run inline. Cited bug: an inline tick eval re-enters `view_id`
/// with a different source and produces schema-mismatched relays.
#[test]
fn tick_defers_inside_exchange() {
    let mut wp = make_worker_for_matrix();
    assert!(wp.exchange.deferred_replay.is_empty());
    assert!(wp
        .dispatch_in_eval((100, 5), &sal_msg(SalMessageKind::Tick, 999, 7), control_frame(999))
        .is_none());
    assert_eq!(wp.exchange.deferred_replay.len(), 1);
    let parked = &wp.exchange.deferred_replay[0];
    assert_eq!(
        (parked.kind, parked.wire.control.target_id, parked.lsn),
        (SalMessageKind::Tick, 999, 7),
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
    assert!(wp
        .dispatch_in_eval((100, 5), &sal_msg(SalMessageKind::Tick, 999, 3), control_frame(999))
        .is_none());
    assert!(wp
        .dispatch_in_eval((100, 5), &sal_msg(SalMessageKind::DeltaScanSpec, 77, 0), frame)
        .is_none());
    assert_eq!(wp.exchange.deferred_replay.len(), 2, "one queue, in SAL order");
    assert_eq!(wp.exchange.deferred_replay[0].kind, SalMessageKind::Tick);
    let read = &wp.exchange.deferred_replay[1];
    assert_eq!(read.kind, SalMessageKind::DeltaScanSpec);
    let ctrl = &read.wire.control;
    assert_eq!((ctrl.target_id, ctrl.client_id, ctrl.seek_pk), (77, 0xC1, 4242));
    assert_eq!(ctrl.seek_pk_extra.as_slice(), &[9, 8, 7]);
}

/// Encode a header-only ExchangeRelay wire frame (schema, no data batch)
/// whose control block echoes `source_id` via `seek_pk`, as the master's
/// `emit_relay_with_decision` does. Leaked to `'static` for `dispatch`, which
/// fail-stops on a frame that does not decode.
fn encode_relay_frame(target_id: u64, source_id: u128, schema: &SchemaDescriptor) -> &'static [u8] {
    // No data batch — a header-only relay. `seek_pk` echoes the source_id the
    // waiter matches on; `seek_col_idx` 0 is BACKFILL_DECISION_CONTINUE.
    let block = crate::catalog::encode_schema_block(schema, target_id as u32);
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
    let block = crate::catalog::encode_schema_block(schema, target_id as u32);
    let msg = ipc::WireMsg {
        target_id,
        schema_block: Some(&block),
        data: ipc::WireData::Whole(Some(batch)),
        ..Default::default()
    };
    Box::leak(msg.encode_to_vec().into_boxed_slice())
}

/// ExchangeRelay inside an exchange wait whose `(view_id, source_id)`
/// matches `want_key` comes back as a [`RelayHit`]; a non-matching pair is
/// parked in `pending_relays`.
#[test]
fn exchange_relay_inside_exchange() {
    let mut wp = make_worker_for_matrix();
    let schema = make_schema_u64_i64();
    let want_key = (100, 0);

    // Mismatched view (target_id=200 ≠ want 100): parked under (200, 0).
    let frame = encode_relay_frame(200, 0, &schema);
    assert!(wp
        .dispatch_in_eval(want_key, &sal_msg(SalMessageKind::ExchangeRelay, 200, 0), frame)
        .is_none());
    assert!(
        wp.exchange.pending_relays.contains_key(&(200, 0)),
        "non-matching relay must be parked in pending_relays"
    );

    // Matching key (target_id=100, source_id=0 == want_key): returns the batch.
    let frame = encode_relay_frame(100, 0, &schema);
    assert!(
        wp.dispatch_in_eval(want_key, &sal_msg(SalMessageKind::ExchangeRelay, 100, 0), frame)
            .is_some(),
        "a key-matching relay must short-circuit out of the dispatcher with its batch"
    );
}

/// ExchangeRelay at TopLevel is a protocol bug — it can only arrive
/// while the worker is blocked in `do_exchange_wait`. The dispatcher
/// warns and continues; no observable state change.
#[test]
fn exchange_relay_top_level_warns_and_continues() {
    let mut wp = make_worker_for_matrix();
    let frame = encode_relay_frame(100, 0, &make_schema_u64_i64());
    wp.dispatch_top_level(&sal_msg(SalMessageKind::ExchangeRelay, 100, 0), frame);
    assert!(
        wp.exchange.pending_relays.is_empty(),
        "TopLevel must NOT park relays — they belong to do_exchange_wait"
    );
}

/// DdlSync inside an exchange wait MUST stage its batch in
/// `exchange.deferred` rather than applying it: an inline catalog mutation
/// races the in-flight DAG evaluation. (Adding a `SalMessageKind` without
/// deciding its cell is already a compile error — `in_eval` is total — so
/// what needs a test is the defer decision itself.)
#[test]
fn ddl_sync_defers_inside_exchange() {
    let mut wp = make_worker_for_matrix();
    let schema = make_schema_u64_i64();

    let frame = encode_data_frame(42, &schema, &make_batch_raw(&schema, &[(1, 1, 10)]));
    assert!(wp
        .dispatch_in_eval((0, 0), &sal_msg(SalMessageKind::DdlSync, 42, 0), frame)
        .is_none());
    assert_eq!(
        wp.exchange.deferred.len(),
        1,
        "DdlSync must stage its batch, not apply it"
    );
    assert_eq!(wp.exchange.deferred[0].wire.control.target_id, 42);
    assert!(
        wp.exchange.deferred_replay.is_empty(),
        "DdlSync must not touch the post-ACK replay queue"
    );
}

/// `Flush` and `Push` run INLINE inside an exchange wait — neither queue may
/// grow, and both must ACK from inside the wait. Flush is the deadlock cell: it
/// would never ACK, so `flush_round` would hold `sal_writer_excl` forever and
/// `relay_loop` could never write the relay this worker is parked on.
#[test]
fn flush_and_push_run_inline_inside_exchange() {
    const SAL_SIZE: usize = 1 << 20;
    use crate::runtime::sal::fixtures::TestLog;
    use crate::runtime::sal::SalReader;

    let dir = crate::test_support::scratch_dir("worker", "flush_push_inline");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    // `Flush` rewinds the reader before flushing, so it needs a real log.
    let sal = TestLog::new(SAL_SIZE, 1, 1);
    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(&mut engine, writer);
    wp.sal_reader = SalReader::new(sal.log(), 0, 0);

    // Target 7 is a system id, which a Push may not name — but a control-only
    // slot carries no rows, so the arm ACKs without reaching the store. What is
    // under test is the disposition, not the ingest.
    for kind in [SalMessageKind::Flush, SalMessageKind::Push] {
        assert!(wp
            .dispatch_in_eval((100, 5), &sal_msg(kind, 7, 0), control_frame(7))
            .is_none());
        assert!(
            wp.exchange.deferred.is_empty() && wp.exchange.deferred_replay.is_empty(),
            "{kind:?} must run inline inside an exchange wait, not park"
        );
    }
    assert_eq!(
        walk_frames(ptr).len(),
        2,
        "each inline arm answers from inside the wait"
    );
    let _ = std::fs::remove_dir_all(&dir);
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
    wp.sal_reader = SalReader::new(sal.log(), 0, 0);

    let next = |wp: &mut WorkerProcess| wp.next_sal_message().map(|(m, _)| (m.kind, m.target_id));
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

/// A ring and its writer, sized well past anything these tests publish — the
/// largest is a few KiB.
fn ring_and_writer() -> (crate::runtime::test_support::SharedRegion, W2mWriter) {
    let region = unsafe { w2m::fixtures::test_ring(1 << 22) };
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
    let mut b = BatchBuilder::new(schema);
    for i in 0..n as u128 {
        b.begin_row(i, 1);
        // Every payload column carries the row index: `make_batch_raw` fills
        // only slot 0, which leaves a wider schema's later columns unwritten.
        for _ in 0..schema.num_payload_cols() {
            b.put_int(i);
        }
        b.end_row();
    }
    b.finish()
}

/// A U64 PK with one STRING payload column — the shape whose batches carry a
/// live blob heap, so every frame of a train over one must compact it.
fn string_schema() -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(type_code::STRING, 0))
}

/// `(pk, string)` rows over [`string_schema`] at weight 1. Values past
/// `SHORT_STRING_THRESHOLD` (12 bytes) land in the heap; shorter ones stay
/// inline and leave the heap empty.
fn long_string_batch(schema: &SchemaDescriptor, rows: &[(u64, &str)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, v) in rows {
        b.begin_row(pk as u128, 1);
        b.put_string(v);
        b.end_row();
    }
    b.finish()
}

/// Row `row`'s PK widened from its OPK bytes — `Batch::get_pk` for a
/// borrowed `MemBatch`.
fn mem_pk(b: &gnitz_store::storage::MemBatch<'_>, row: usize) -> u128 {
    gnitz_wire::widen_pk_be(b.get_pk_bytes(row))
}

fn consume_one(ptr: *mut u8) -> Vec<u8> {
    let (_, frame) = walk_frames(ptr).into_iter().next().expect("expected one ring message");
    frame
}

/// Wire size of a frame carrying `count` rows of `schema`, with an optional
/// schema block — the budget that fits exactly that many rows.
fn frame_size(schema: SchemaDescriptor, count: usize, prebuilt: Option<&[u8]>) -> usize {
    ipc::WireMsg {
        data: ipc::WireData::Whole(Some(&make_n_row_batch(schema, count))),
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
        let schema = *batch.schema();
        let block = Rc::new(crate::catalog::encode_schema_block(&schema, 1));
        let per_row = frame_size(schema, 2, None) - frame_size(schema, 1, None);
        // Room for four rows beside the schema block on the first frame.
        let budget = frame_size(schema, 4, Some(block.as_slice()));

        let (region, writer) = ring_and_writer();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        wp.reply_frame_budget = budget;
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch),
            route: route(1, 5, 0),
            prebuilt_schema: Some(block),
            server_version: 0,
            next_row: 0,
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
        let (region, writer) = ring_and_writer();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);

        let r = if force_fifo {
            fifo_route(1, 3, 0)
        } else {
            route(1, 3, 0)
        };
        wp.send_shared_scan_response(r, Rc::new(make_n_row_batch(schema, 5)), ReplySchema::ClientAuthored, 0);

        if force_fifo {
            assert_eq!(wp.pending_streams.len(), 1, "force_fifo must enqueue, not emit");
            assert_eq!(wp.pending_streams.front().unwrap().next_row, 0);
            wp.emit_pending_scan_chunk();
        }
        assert!(wp.pending_streams.is_empty(), "the queue ends empty either way");

        let data = consume_one(ptr);
        let ctrl = gnitz_wire::control::peek_control_block_ipc(&data).expect("peek_control_block");
        assert_eq!(ctrl.status, STATUS_OK);
        assert_ne!(ctrl.flags & FLAG_SCAN_LAST, 0);
        assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);

        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        let decoded = ipc::decode_wire_ipc_zero_copy_with_ctrl(&data, ctrl, Some(&schema), &mut offsets)
            .expect("decode with schema hint");
        let b = decoded.data_batch.as_ref().expect("data block");
        assert_eq!(b.len(), 5);
        for i in 0..5usize {
            assert_eq!(mem_pk(b, i), i as u128);
        }
    }
}

/// A `force_fifo` reply that fits goes out as ONE frame over the SOURCE batch —
/// no sub-batch, so no copy and no heap relocation on the path every `scan_many`
/// relation takes. The source carries dead heap bytes (what a blob-sharing
/// filter produces), which a sub-batch would compact away: the frame's byte size
/// is what tells the two paths apart.
#[test]
fn force_fifo_emits_a_fitting_reply_over_the_source_batch() {
    let schema = string_schema();
    let mut batch = long_string_batch(&schema, &[(1, "a long enough value"), (2, "another long value")]);
    batch.blob.extend_from_slice(&[0u8; 4096]);
    let batch = Rc::new(batch);
    let compacted = batch.wire_chunk_within(0, 0, usize::MAX);
    assert!(
        compacted.wire_byte_size() < batch.wire_byte_size(),
        "the fixture must have a dedupable heap for the size test to discriminate"
    );

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.send_shared_scan_response(fifo_route(1, 5, 0), Rc::clone(&batch), ReplySchema::ClientAuthored, 0);
    assert_eq!(wp.pending_streams.len(), 1, "force_fifo queues even a fitting reply");
    assert!(walk_frames(ptr).is_empty(), "nothing is emitted at enqueue time");

    wp.emit_pending_scan_chunk();
    assert!(wp.pending_streams.is_empty(), "one frame drains the train");
    let frames = walk_frames(ptr);
    assert_eq!(frames.len(), 1);
    let ctrl = gnitz_wire::control::peek_control_block_ipc(&frames[0].1).unwrap();
    assert_eq!(ctrl.status, STATUS_OK);
    assert_ne!(ctrl.flags & FLAG_SCAN_LAST, 0);
    assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);
    let whole = ipc::WireMsg {
        data: ipc::WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .size();
    assert_eq!(
        frames[0].1.len(),
        whole,
        "the frame must be the source batch verbatim, heap and all"
    );
}

// -- reply-train FIFO and chunking tests -----------------------------------

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
    let block_a = Rc::new(crate::catalog::encode_schema_block(&schema_a, 1));
    let block_b = Rc::new(crate::catalog::encode_schema_block(&schema_b, 2));

    // Budget: exactly the first chunk's size at 4 rows (A's schema block
    // included), so train A's 10 rows span at least two frames.
    let budget = frame_size(schema_a, 4, Some(block_a.as_slice()));

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.pending_streams.push_back(PendingScan {
        batch: Rc::new(batch_a),
        route: route(1, 11, 0),
        prebuilt_schema: Some(block_a),
        server_version: 0,
        next_row: 0,
    });
    wp.pending_streams.push_back(PendingScan {
        batch: Rc::new(batch_b),
        route: route(2, 22, 0),
        prebuilt_schema: Some(block_b),
        server_version: 0,
        next_row: 0,
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
                rows += decoded.data_batch.map(|b| b.len()).unwrap_or(0);
            } else {
                assert!(
                    ipc::decode_wire_ipc(bytes).is_err(),
                    "a continuation carries no schema block, so it cannot decode standalone"
                );
                let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
                let decoded = ipc::decode_wire_ipc_zero_copy_with_ctrl(bytes, ctrl, Some(schema), &mut offsets)
                    .expect("continuation decodes against the schema hint");
                rows += decoded.data_batch.map(|b| b.len()).unwrap_or(0);
            }
        }
        assert_eq!(rows, total_rows, "the train's chunks cover all rows exactly once");
    }
}

/// An oversized result enqueues a train instead of emitting a frame past
/// `ipc::FRAME_CAP`; nothing is emitted until drain_sal.
#[test]
fn an_oversized_reply_enqueues_a_train() {
    let schema = make_schema_u64_i64(); // 32 B/row on the wire
    let rows = (ipc::FRAME_CAP / 32) + 4096;
    let batch = Batch::zeroed(&schema, rows);

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.send_scan_response(route(3, 5, 9), batch, ReplySchema::ClientAuthored, 0);
    assert_eq!(wp.pending_streams.len(), 1);
    let ps = wp.pending_streams.front().unwrap();
    assert_eq!(ps.next_row, 0);
    assert_eq!(ps.route.request_id, 5);
    assert_eq!(ps.route.client_id, 9);
    assert!(
        walk_frames(ptr).is_empty(),
        "the train's first chunk is emitted by drain_sal, not at enqueue time"
    );
}

/// A single row wider than a shrunken `reply_frame_budget` still ships: the
/// budget is a split point, not a limit, so the frame goes out over budget and
/// no fault is raised. Only [`ipc::FRAME_CAP`] — what the client can read — is
/// a refusal.
#[test]
fn a_row_wider_than_the_budget_ships_one_over_budget_frame() {
    let schema = string_schema();
    let batch = long_string_batch(&schema, &[(1, &"x".repeat(4096)), (2, &"y".repeat(4096))]);

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.reply_frame_budget = 512;
    wp.send_scan_response(route(1, 5, 0), batch, ReplySchema::ClientAuthored, 0);

    let mut passes = 0;
    while !wp.pending_streams.is_empty() {
        wp.emit_pending_scan_chunk();
        passes += 1;
        assert!(passes < 10, "the train must drain within a bounded pass count");
    }
    let frames = walk_frames(ptr);
    assert_eq!(
        frames.len(),
        2,
        "one row per frame, since one row alone busts the budget"
    );
    for (_, bytes) in &frames {
        let ctrl = gnitz_wire::control::peek_control_block_ipc(bytes).unwrap();
        assert_eq!(ctrl.status, STATUS_OK, "an over-budget frame is not a fault");
        assert!(bytes.len() > 512, "each frame is one row wider than the budget");
    }
}

/// A single row wider than [`ipc::FRAME_CAP`] has nothing left to narrow: the
/// train pops and the request is answered with the oversize fault.
#[test]
fn a_row_wider_than_the_frame_cap_faults() {
    let schema = string_schema();
    // One row whose string alone exceeds what a client can read in one frame.
    let batch = long_string_batch(&schema, &[(1, &"z".repeat(ipc::FRAME_CAP + 4096))]);

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.send_scan_response(route(1, 5, 0), batch, ReplySchema::ClientAuthored, 0);
    assert_eq!(
        wp.pending_streams.len(),
        1,
        "an oversized reply queues before it faults"
    );

    wp.emit_pending_scan_chunk();
    assert!(wp.pending_streams.is_empty(), "the faulting train pops");
    let frames = walk_frames(ptr);
    assert_eq!(frames.len(), 1, "the fault is the whole reply");
    let ctrl = gnitz_wire::control::peek_control_block_ipc(&frames[0].1).unwrap();
    assert_eq!(ctrl.status, gnitz_wire::STATUS_ERROR);
    assert_eq!(ctrl.request_id, 5);
    let text = String::from_utf8_lossy(&ctrl.error_msg);
    assert!(
        text.contains("exceeds the maximum frame payload"),
        "the fault names the cap: {text}"
    );
}

/// A STRING-column reply splits across frames like any other, and every frame
/// carries a heap compacted to its own rows. Drive one to exhaustion at a small
/// budget and reassemble: PKs, **weights** and string contents must all match
/// the source, and no frame may exceed the budget — in a Z-set engine a row-set
/// comparison would test nothing.
#[test]
fn a_long_string_train_reassembles_with_its_weights() {
    let schema = string_schema();
    let rows: Vec<(u64, String)> = (0..25u64)
        .map(|i| (i, format!("value-{i}-{}", "p".repeat(40))))
        .collect();
    let source = long_string_batch(&schema, &rows.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>());
    assert!(!source.blob.is_empty(), "40+ byte values must live in the heap");

    // ~230 B per row, so a 2 KiB budget puts a handful of rows in each frame.
    let budget = 2048;

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.reply_frame_budget = budget;
    wp.send_scan_response(route(1, 5, 0), source, ReplySchema::ClientAuthored, 0);
    let mut passes = 0;
    while !wp.pending_streams.is_empty() {
        wp.emit_pending_scan_chunk();
        passes += 1;
        assert!(passes < 50, "the train must drain within a bounded pass count");
    }

    let frames = walk_frames(ptr);
    assert!(
        frames.len() >= 2,
        "a {budget}-byte budget must split 25 long-string rows"
    );
    let mut got: Vec<(u128, i64, String)> = Vec::new();
    for (i, (_, bytes)) in frames.iter().enumerate() {
        assert!(bytes.len() <= budget, "frame {i} is {} bytes of {budget}", bytes.len());
        let ctrl = gnitz_wire::control::peek_control_block_ipc(bytes).unwrap();
        assert_eq!(ctrl.status, STATUS_OK);
        assert_eq!(
            ctrl.flags & FLAG_SCAN_LAST != 0,
            i == frames.len() - 1,
            "FLAG_SCAN_LAST only on the terminal frame"
        );
        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        let decoded = ipc::decode_wire_ipc_zero_copy_with_ctrl(bytes, ctrl, Some(&schema), &mut offsets)
            .expect("every frame decodes against the client's own schema");
        let b = decoded.data_batch.as_ref().expect("data block");
        for r in 0..b.len() {
            got.push((
                mem_pk(b, r),
                b.get_weight(r),
                gnitz_store::storage::payload_string(b, r, 0),
            ));
        }
    }
    let want: Vec<(u128, i64, String)> = rows.iter().map(|(k, v)| (*k as u128, 1i64, v.clone())).collect();
    assert_eq!(got, want, "the train reassembles to the source, weights included");
}

/// A projected (gather) reply schema must ride a ONE-OFF wire block: the
/// table-keyed cache must neither serve it (the master would decode
/// projected rows with the base table's stride) nor store it (a later
/// table reply would be decoded with the projected stride). The block is
/// unchecksummed — every consumer of a one-off reply decodes through the ring,
/// which verifies none.
#[test]
fn a_projected_reply_carries_a_one_off_block() {
    let dir = crate::test_support::scratch_dir("worker", "projected_one_off");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![
        col_def("id", type_code::U64),
        col_def("a", type_code::U64),
        col_def("b", type_code::U64),
    ];
    let tid = crate::catalog::FIRST_USER_TABLE_ID;
    engine
        .register_table(tid, PUBLIC_SCHEMA_ID, "tproj", &cols, &[0])
        .unwrap();
    let table_schema = engine.registry().relation(tid).map(Relation::schema).unwrap();
    let projected = gnitz_store::schema::project_schema(&table_schema, &[1]).unwrap();
    assert_ne!(projected.num_columns(), table_schema.num_columns());

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);

    // Fitting projected reply: one frame carrying the projected schema.
    let small = Batch::zeroed(&projected, 2);
    wp.send_scan_response(route(tid as u64, 5, 0), small, ReplySchema::OneOff(&projected), 0);
    let frames = walk_frames(ptr);
    assert_eq!(frames.len(), 1);
    let decoded = ipc::decode_wire_ipc(&frames[0].1).expect("decode projected reply");
    assert_eq!(
        decoded.schema.expect("schema block present").num_columns(),
        projected.num_columns()
    );

    // Oversized projected reply: the queued train holds the one-off block.
    let rows = (ipc::FRAME_CAP / 32) + 4096;
    let big = Batch::zeroed(&projected, rows);
    wp.send_scan_response(route(tid as u64, 6, 0), big, ReplySchema::OneOff(&projected), 0);
    assert_eq!(wp.pending_streams.len(), 1);
    let expected_block = crate::catalog::encode_schema_block_ipc(&projected, tid as u32);
    let ps = wp.pending_streams.front().unwrap();
    assert_eq!(ps.next_row, 0);
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
