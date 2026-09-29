use super::*;
use crate::runtime::sal::fixtures::bare_message;
use crate::runtime::w2m::{self, W2mReceiver};
use crate::test_support::{make_batch_raw, make_schema_u64_i64, u64_pk_schema};
use gnitz_store::schema::SchemaColumn;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::BatchBuilder;
use gnitz_wire::TypeCode;

/// Every reply helper (`send_ack`, `send_reply`, `send_fault`) publishes
/// on the ring prefix of the request id it was handed.
#[test]
fn send_helpers_publish_on_the_request_id() {
    let (region, w2m_writer) = ring_and_writer();
    let region_ptr = region.ptr();

    let mut wp = make_test_worker(std::ptr::null_mut(), w2m_writer);

    let req_ack: u32 = 42;
    let req_resp: u32 = 0xCAFE_BABE;
    let req_err: u32 = 0x7FFF_FFFE;
    wp.send_ack(7, req_ack);
    let schema = make_schema_u64_i64();
    wp.send_reply(route(8, req_resp), Batch::empty_with_schema(&schema));
    wp.send_fault(&gnitz_wire::WireFault::from("boom"), req_err);

    let decoded_ids: Vec<u32> = walk_frames(region_ptr).iter().map(|(id, _)| *id).collect();
    assert_eq!(decoded_ids, vec![req_ack, req_resp, req_err]);
}

// -- Walk-the-matrix dispatch tests ---------------------------------------

/// The one test constructor for `WorkerProcess`, through the production one so
/// a new field cannot be missed here. A zeroed `SalReader` is a null log: only
/// the SAL-drain tests read one, and they assign a real reader afterwards. The
/// budget is pinned rather than read from the environment, so a shell that
/// exports `GNITZ_REPLY_FRAME_BUDGET` does not reshape these frames.
fn make_test_worker(catalog: *mut CatalogEngine, writer: W2mWriter) -> WorkerProcess {
    let mesh = crate::runtime::mesh::fixtures::meshes(1, crate::runtime::mesh::OUTBOX_BYTES)
        .pop()
        .unwrap();
    let mut wp = WorkerProcess::new(catalog, unsafe { std::mem::zeroed() }, writer, mesh);
    wp.reply_frame_budget = gnitz_wire::MAX_FRAME_PAYLOAD;
    wp
}

/// The reply route every helper below takes, in the order `dispatch_inner`
/// resolves it. Inline-emitting; [`fifo_route`] is the queued twin.
fn route(target_id: u64, request_id: u32) -> ReplyRoute {
    ReplyRoute {
        target_id,
        request_id,
        fifo: false,
        schema_version: 0,
    }
}

/// [`route`] for a group the master wrote as one of several.
fn fifo_route(target_id: u64, request_id: u32) -> ReplyRoute {
    ReplyRoute {
        fifo: true,
        ..route(target_id, request_id)
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

/// A Tick group's slot: the first round in `arg0`, the tids in the blob, as
/// `write_tick_group` lays them out.
fn tick_frame(tids: &[u64], round: u64) -> &'static [u8] {
    let blob: Vec<u8> = tids.iter().flat_map(|t| t.to_le_bytes()).collect();
    Box::leak(
        ipc::WireMsg {
            arg0: round,
            blob: &blob,
            ..Default::default()
        }
        .encode_to_vec()
        .into_boxed_slice(),
    )
}

/// Build a worker that's safe for dispatch calls whose behavior
/// does not enter the catalog. A DdlSync slot's decode consults the catalog.
///
/// The W2M ring is unused by these arms; sal_reader is also unused
/// because we drive the dispatchers directly.
fn make_worker_for_matrix() -> WorkerProcess {
    make_test_worker(std::ptr::null_mut(), unsafe { std::mem::zeroed() })
}

/// Decode `frame` as a `kind` group aimed at `target` and dispatch it as a
/// group drained inside an exchange wait.
fn dispatch_in_eval(wp: &mut WorkerProcess, kind: SalMessageKind, target: u64, frame: &'static [u8]) {
    let req = wp.decode_request(&bare_message(kind, target), frame);
    wp.dispatch_in_eval(req);
}

/// A read inside an exchange wait is parked with its whole request, in request
/// order.
#[test]
fn reads_defer_inside_exchange_in_request_order() {
    let mut wp = make_worker_for_matrix();
    let frame = |target_id: u64, arg0: u64| -> &'static [u8] {
        Box::leak(
            ipc::WireMsg {
                target_id,
                arg0,
                arg1: 31,
                blob: &[9, 8, 7],
                ..Default::default()
            }
            .encode_to_vec()
            .into_boxed_slice(),
        )
    };
    let reads = [
        (SalMessageKind::Scan, 75, 4240),
        (SalMessageKind::ScanSpec, 76, 4241),
        (SalMessageKind::DeltaRead, 77, 4242),
    ];
    for (kind, target, arg0) in reads {
        dispatch_in_eval(&mut wp, kind, target, frame(target, arg0));
    }
    assert_eq!(wp.deferred.len(), reads.len(), "one queue, in SAL order");
    for (parked, (kind, target, arg0)) in wp.deferred.iter().zip(reads) {
        assert_eq!(parked.kind, kind);
        let ctrl = &parked.wire.control;
        let blob = &parked.wire.blob;
        assert_eq!((ctrl.hdr.target_id, ctrl.hdr.arg0, ctrl.hdr.arg1), (target, arg0, 31));
        assert_eq!(blob.as_slice(), &[9, 8, 7]);
    }
}

/// A request dispatched inside an exchange wait leaves a queued train unsent: a
/// slow client can fill the ring, and a worker blocked on it would stall the
/// round every peer waits on.
#[test]
fn a_queued_train_is_not_emitted_inside_an_exchange_wait() {
    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.send_reply(fifo_route(1, 5), make_n_row_batch(make_schema_u64_i64(), 3));

    dispatch_in_eval(&mut wp, SalMessageKind::Scan, 999, control_frame(999));

    assert_eq!(wp.pending_streams.front().map(|t| t.next_row), Some(0));
    assert!(walk_frames(ptr).is_empty());
}

/// A `_sequences` DdlSync runs inline inside an exchange wait: dropped, queued
/// nowhere, answered never.
#[test]
fn a_sequences_ddl_sync_runs_inline_inside_an_exchange_wait() {
    let dir = crate::test_support::scratch_dir("worker", "sequences_ddl_sync_inline");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (region, writer) = ring_and_writer();
    let mut wp = make_test_worker(&mut engine, writer);
    let tid = SysFamily::Sequence.id();

    dispatch_in_eval(&mut wp, SalMessageKind::DdlSync, tid, control_frame(tid));
    assert!(wp.deferred.is_empty(), "a `_sequences` DdlSync must not park");
    assert!(walk_frames(region.ptr()).is_empty(), "a DdlSync is never answered");
    let _ = std::fs::remove_dir_all(&dir);
}

/// `Flush` and `Push` run inline inside an exchange wait: neither parks, and
/// both ACK from inside the wait.
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
    wp.sal_reader = SalReader::new(sal.log(), 0, 1);

    // Target 7 is a system id, which a Push may not name — but a control-only
    // slot carries no rows, so the arm ACKs without reaching the store. What is
    // under test is the disposition, not the ingest.
    for kind in [SalMessageKind::Flush, SalMessageKind::Push] {
        dispatch_in_eval(&mut wp, kind, 7, control_frame(7));
        assert!(
            wp.deferred.is_empty(),
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

/// One Tick group carrying two tids ticks both — each drains its buffered
/// delta — and ACKs once.
#[test]
fn a_two_tid_tick_group_ticks_both_and_acks_once() {
    let dir = crate::test_support::scratch_dir("worker", "two_tid_tick");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let schema = make_schema_u64_i64();
    for tid in [500, 501] {
        engine
            .dag
            .buffer_unticked(tid, make_batch_raw(&schema, &[(tid, 1, 10)]));
    }
    let (region, writer) = ring_and_writer();
    let mut wp = make_test_worker(&mut engine, writer);

    let req = wp.decode_request(&bare_message(SalMessageKind::Tick, 0), tick_frame(&[500, 501], 7));
    wp.handle_request(req);

    for tid in [500, 501] {
        assert!(wp.cat().dag.take_unticked(tid).is_none(), "tid {tid} was ticked");
    }
    let frames = walk_frames(region.ptr());
    assert_eq!(frames.len(), 1, "the group ACKs once");
    let ctrl = gnitz_wire::control::peek_control_block(&frames[0].1).unwrap();
    assert_eq!(ctrl.hdr.status, WireStatus::Ok);
    let _ = std::fs::remove_dir_all(&dir);
}

// -- SAL read gate tests ---------------------------------------------------

/// The SAL read gate, driven through the worker's own reader:
///
/// 1. Groups in the expected epoch are consumed in order.
/// 2. A group from a *later* epoch is rejected and stays parked — repeatedly,
///    so the cursor did not advance past it.
/// 3. `SalReader::rewind` moves to cursor 0 in the next epoch, which is
///    exactly where the master writes after its own reset.
#[test]
fn the_sal_reader_gates_on_the_epoch() {
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
    wp.sal_reader = SalReader::new(sal.log(), 0, 1);

    let next = |wp: &mut WorkerProcess| wp.sal_reader.next().map(|(m, _)| (m.kind, m.target_id));
    assert_eq!(next(&mut wp), Some((SalMessageKind::Push, 42)));
    assert_eq!(next(&mut wp), Some((SalMessageKind::DdlSync, 43)));

    // The epoch-2 group parks: rejected now, and still rejected on a retry —
    // the cursor did not slip past it.
    assert!(wp.sal_reader.next().is_none(), "an epoch-ahead group must park");
    assert!(wp.sal_reader.next().is_none(), "and stay parked");

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
    u64_pk_schema(SchemaColumn::new(TypeCode::U32, false))
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
    u64_pk_schema(SchemaColumn::new(TypeCode::String, false))
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

/// Wire size of a frame carrying `count` rows of `schema` — the budget that fits
/// exactly that many rows.
fn frame_size(schema: SchemaDescriptor, count: usize) -> usize {
    ipc::WireMsg {
        data: ipc::WireData::Whole(&make_n_row_batch(schema, count)),
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
        let per_row = frame_size(schema, 2) - frame_size(schema, 1);
        let budget = frame_size(schema, 4);

        let (region, writer) = ring_and_writer();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        wp.reply_frame_budget = budget;
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch),
            route: route(1, 5),
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

/// `fifo` is the whole difference between a splittable reply emitting inline
/// and the same reply queueing through `pending_streams` — which is what puts a
/// multi-scan's relations on the ring in request order. Either way its lone
/// chunk is byte-shaped the same.
#[test]
fn fifo_decides_whether_a_fitting_reply_emits_inline_or_queues() {
    let schema = make_schema_u64_i64();
    for fifo in [false, true] {
        let (region, writer) = ring_and_writer();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);

        let r = if fifo { fifo_route(1, 3) } else { route(1, 3) };
        wp.send_reply(r, Rc::new(make_n_row_batch(schema, 5)));

        if fifo {
            assert_eq!(wp.pending_streams.len(), 1, "fifo must enqueue, not emit");
            assert_eq!(wp.pending_streams.front().unwrap().next_row, 0);
            wp.emit_pending_scan_chunk();
        }
        assert!(wp.pending_streams.is_empty(), "the queue ends empty either way");

        let data = consume_one(ptr);
        let ctrl = gnitz_wire::control::peek_control_block(&data).expect("peek_control_block");
        assert_eq!(ctrl.hdr.status, WireStatus::Ok);
        assert!(ctrl.hdr.flags.scan_last);
        assert!(ctrl.hdr.flags.continuation);

        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        let b = gnitz_store::storage::decode_mem_batch_from_wal_block(
            &data[ctrl.data.clone().expect("data block")],
            &schema,
            &mut offsets,
        )
        .expect("decode with schema hint");
        assert_eq!(b.len(), 5);
        for i in 0..5usize {
            assert_eq!(mem_pk(&b, i), i as u128);
        }
    }
}

/// A `fifo` reply that fits goes out as ONE frame over the SOURCE batch —
/// no sub-batch, so no copy and no heap relocation on the path every `scan_many`
/// relation takes. The source carries dead heap bytes (what a blob-sharing
/// filter produces), which a sub-batch would compact away: the frame's byte size
/// is what tells the two paths apart.
#[test]
fn fifo_emits_a_fitting_reply_over_the_source_batch() {
    let schema = string_schema();
    let mut batch = long_string_batch(&schema, &[(1, "a long enough value"), (2, "another long value")]);
    batch.blob.extend_from_slice(&[0u8; 4096]);
    let batch = Rc::new(batch);
    let (chunk, _) = batch.wire_chunk_within(0, 0, usize::MAX);
    let gnitz_store::storage::WireChunk::Owned(compacted) = chunk else {
        panic!("a heap-bearing batch must relocate into a chunk of its own");
    };
    assert!(
        compacted.wire_byte_size() < batch.wire_byte_size(),
        "the fixture must have a dedupable heap for the size test to discriminate"
    );

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.send_reply(fifo_route(1, 5), Rc::clone(&batch));
    assert_eq!(wp.pending_streams.len(), 1, "fifo queues even a fitting reply");
    assert!(walk_frames(ptr).is_empty(), "nothing is emitted at enqueue time");

    wp.emit_pending_scan_chunk();
    assert!(wp.pending_streams.is_empty(), "one frame drains the train");
    let frames = walk_frames(ptr);
    assert_eq!(frames.len(), 1);
    let ctrl = gnitz_wire::control::peek_control_block(&frames[0].1).unwrap();
    assert_eq!(ctrl.hdr.status, WireStatus::Ok);
    assert!(ctrl.hdr.flags.scan_last);
    assert!(ctrl.hdr.flags.continuation);
    let whole = ipc::WireMsg {
        data: ipc::WireData::Whole(&batch),
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

/// Two queued trains drain strictly FIFO: every frame of train A
/// (multi-chunk, terminal `scan_last`) precedes train B's, and each queued
/// train's frames echo its own route's `schema_version`.
#[test]
fn pending_streams_drain_two_trains_fifo() {
    let schema_a = make_schema_u64_i64();
    // B's schema has 3 columns so its frames are distinguishable from A's.
    let schema_b = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0],
    );
    let batch_a = make_n_row_batch(schema_a, 10);
    let batch_b = make_n_row_batch(schema_b, 5);

    // Budget: exactly 4 of A's rows, so train A's 10 rows span several frames.
    let budget = frame_size(schema_a, 4);

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.pending_streams.push_back(PendingScan {
        batch: Rc::new(batch_a),
        route: ReplyRoute { schema_version: 7, ..route(1, 11) },
        next_row: 0,
    });
    wp.pending_streams.push_back(PendingScan {
        batch: Rc::new(batch_b),
        route: ReplyRoute { schema_version: 9, ..route(2, 22) },
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

    for (req, schema, version, total_rows) in [(11u32, &schema_a, 7u16, 10usize), (22u32, &schema_b, 9u16, 5usize)] {
        let train: Vec<_> = frames.iter().filter(|(r, _)| *r == req).collect();
        let mut rows = 0usize;
        for (i, (_, bytes)) in train.iter().enumerate() {
            let ctrl = gnitz_wire::control::peek_control_block(bytes).expect("ctrl");
            assert!(ctrl.hdr.flags.continuation);
            assert!(ctrl.schema.is_none(), "a reply frame carries no schema block");
            assert_eq!(
                ctrl.hdr.flags.schema_version, version,
                "the frame echoes its route's version"
            );
            let is_last = i == train.len() - 1;
            assert_eq!(
                ctrl.hdr.flags.scan_last, is_last,
                "scan_last only on the train's terminal chunk"
            );
            let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
            let b = gnitz_store::storage::decode_mem_batch_from_wal_block(
                &bytes[ctrl.data.clone().expect("data block")],
                schema,
                &mut offsets,
            )
            .expect("every frame decodes against the train's schema");
            rows += b.len();
        }
        assert_eq!(rows, total_rows, "the train's chunks cover all rows exactly once");
    }
}

/// An oversized result enqueues a train instead of emitting a frame past
/// `gnitz_wire::MAX_FRAME_PAYLOAD`; nothing is emitted until drain_sal.
#[test]
fn an_oversized_reply_enqueues_a_train() {
    let schema = make_schema_u64_i64(); // 32 B/row on the wire
    let rows = (gnitz_wire::MAX_FRAME_PAYLOAD / 32) + 4096;
    let batch = Batch::zeroed(&schema, rows);

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.send_reply(route(3, 5), batch);
    assert_eq!(wp.pending_streams.len(), 1);
    let ps = wp.pending_streams.front().unwrap();
    assert_eq!(ps.next_row, 0);
    assert_eq!(ps.route.request_id, 5);
    assert!(
        walk_frames(ptr).is_empty(),
        "the train's first chunk is emitted by drain_sal, not at enqueue time"
    );
}

/// A row wider than `reply_frame_budget` still ships, alone in an over-budget
/// frame, with no fault.
#[test]
fn a_row_wider_than_the_budget_ships_one_over_budget_frame() {
    let schema = string_schema();
    let batch = long_string_batch(&schema, &[(1, &"x".repeat(4096)), (2, &"y".repeat(4096))]);

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.reply_frame_budget = 512;
    wp.send_reply(route(1, 5), batch);

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
        let ctrl = gnitz_wire::control::peek_control_block(bytes).unwrap();
        assert_eq!(ctrl.hdr.status, WireStatus::Ok, "an over-budget frame is not a fault");
        assert!(bytes.len() > 512, "each frame is one row wider than the budget");
    }
}

/// A row wider than [`gnitz_wire::MAX_FRAME_PAYLOAD`] answers the request with the
/// oversize fault.
#[test]
fn a_row_wider_than_the_frame_cap_faults() {
    let schema = string_schema();
    // One row whose string alone exceeds what a client can read in one frame.
    let batch = long_string_batch(&schema, &[(1, &"z".repeat(gnitz_wire::MAX_FRAME_PAYLOAD + 4096))]);

    let (region, writer) = ring_and_writer();
    let ptr = region.ptr();
    let mut wp = make_test_worker(std::ptr::null_mut(), writer);
    wp.send_reply(route(1, 5), batch);
    assert_eq!(
        wp.pending_streams.len(),
        1,
        "an oversized reply queues before it faults"
    );

    wp.emit_pending_scan_chunk();
    assert!(wp.pending_streams.is_empty(), "the faulting train pops");
    let frames = walk_frames(ptr);
    assert_eq!(frames.len(), 1, "the fault is the whole reply");
    let ctrl = gnitz_wire::control::peek_control_block(&frames[0].1).unwrap();
    let fault = ctrl.fault(&frames[0].1).expect("a fault frame");
    assert_eq!(fault.status, gnitz_wire::WireStatus::Error);
    assert_eq!(frames[0].0, 5);
    assert!(
        fault.text.contains("exceeds the maximum frame payload"),
        "the fault names the cap: {fault}"
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
    wp.send_reply(route(1, 5), source);
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
        let ctrl = gnitz_wire::control::peek_control_block(bytes).unwrap();
        assert_eq!(ctrl.hdr.status, WireStatus::Ok);
        assert_eq!(
            ctrl.hdr.flags.scan_last,
            i == frames.len() - 1,
            "scan_last only on the terminal frame"
        );
        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        let b = gnitz_store::storage::decode_mem_batch_from_wal_block(
            &bytes[ctrl.data.clone().expect("data block")],
            &schema,
            &mut offsets,
        )
        .expect("every frame decodes against the client's own schema");
        for r in 0..b.len() {
            got.push((mem_pk(&b, r), b.get_weight(r), gnitz_expr::payload_string(&b, r, 0)));
        }
    }
    let want: Vec<(u128, i64, String)> = rows.iter().map(|(k, v)| (*k as u128, 1i64, v.clone())).collect();
    assert_eq!(got, want, "the train reassembles to the source, weights included");
}
