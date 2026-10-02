use super::*;
use crate::catalog::SysFamily;
use crate::runtime::sal::fixtures::TestLog;
use crate::runtime::sal::{DirectGroup, GroupTargets, WorkerSet};
use crate::runtime::w2m::{self, W2mReceiver};
use crate::test_support::{
    make_batch_bytes_raw, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64, weighted_rows,
};
use gnitz_wire::control::{peek_control_block, DecodedControl};

// -- fixtures ---------------------------------------------------------------

/// A worker over a real SAL and a real W2M ring — the one ring its replies and
/// its SAL wakes share, as in production — through the production constructor
/// so a new field cannot be missed here, with the receiver that reads its ring. The budget is pinned rather than read from the environment,
/// so a shell that exports `GNITZ_REPLY_FRAME_BUDGET` does not reshape these
/// frames.
fn test_worker(catalog: *mut CatalogEngine) -> (WorkerProcess, TestLog, W2mReceiver) {
    let ring = w2m::fixtures::test_ring(1 << 22);
    let sal = TestLog::with_rings(1 << 20, vec![ring], 1);
    let mesh = crate::runtime::mesh::fixtures::meshes(1, crate::runtime::mesh::OUTBOX_BYTES)
        .pop()
        .unwrap();
    let mut wp = WorkerProcess::new(catalog, SalReader::new(sal.log(), 0, 1), W2mWriter::new(ring), mesh);
    wp.reply_frame_budget = gnitz_wire::MAX_FRAME_PAYLOAD;
    (wp, sal, W2mReceiver::new(vec![ring]))
}

fn route(target_id: u64, request_id: u32) -> ReplyRoute {
    ReplyRoute { target_id, request_id, fifo: false }
}

/// [`route`] for a group the master wrote as one of several.
fn fifo_route(target_id: u64, request_id: u32) -> ReplyRoute {
    ReplyRoute {
        fifo: true,
        ..route(target_id, request_id)
    }
}

/// One published ring message: its ring-prefix request id, control block and
/// bytes.
struct Frame {
    req: u32,
    ctrl: DecodedControl,
    bytes: Vec<u8>,
}

impl Frame {
    /// The rows it carries; a frame without a data block carries none.
    fn rows(&self, schema: &SchemaDescriptor) -> Batch {
        match self.ctrl.data.clone() {
            Some(data) => {
                Batch::decode_from_wal_block(&self.bytes[data], schema).expect("the frame decodes against its schema")
            }
            None => Batch::empty_with_schema(schema),
        }
    }
}

/// Every message published since the last call, in publish order.
fn frames(rx: &W2mReceiver) -> Vec<Frame> {
    let mut out = Vec::new();
    while let Some(slot) = rx.try_read_slot(0) {
        let bytes = slot.bytes().to_vec();
        out.push(Frame {
            req: slot.internal_req_id,
            ctrl: peek_control_block(&bytes).expect("a control block"),
            bytes,
        });
    }
    out
}

/// Emit queued trains until none remain, as successive `drain_sal` passes do.
fn drain_trains(wp: &mut WorkerProcess) {
    for _ in 0..1000 {
        if wp.pending_streams.is_empty() {
            return;
        }
        wp.emit_pending_scan_chunk();
    }
    panic!("the trains must drain within a bounded pass count");
}

/// Wire size of `batch` sent whole as one frame.
fn frame_size(batch: &Batch) -> usize {
    ipc::WireMsg {
        data: batch.wire_whole(),
        ..Default::default()
    }
    .size()
}

/// A nonzero weight per row, of both signs, so a scrambled or dropped weight
/// region cannot reassemble to the source.
fn weight(i: u64) -> i64 {
    if i.is_multiple_of(2) {
        i as i64 + 1
    } else {
        -(i as i64)
    }
}

// -- dispatch ---------------------------------------------------------------

/// The in-wait matrix, driven through the exchange wait's own drain loop: reads
/// park in SAL order; `HasPk`, `Push` and `Flush` answer from inside the wait; a
/// DdlSync that addresses no worker is stepped over; and a queued train stays unsent — a
/// slow client can fill the ring, and a worker blocked on it would stall the
/// round every peer waits on.
#[test]
fn in_wait_dispositions() {
    let dir = crate::test_support::scratch_dir("worker", "in_wait_dispositions");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, sal, rx) = test_worker(&mut engine);
    wp.send_reply(fifo_route(1, 5), make_batch_raw(&make_schema_u64_i64(), &[(1, 1, 10)]));

    let seq = SysFamily::Sequence.id();
    // Target 7 is a system id, which a Push may not name — but a control-only
    // slot carries no rows, so the arm ACKs without reaching the store.
    // `Flush` goes last: the reader leaves the epoch on stepping past it.
    let groups = [
        (SalMessageKind::ScanSpec, 76),
        (SalMessageKind::DeltaRead, 77),
        (SalMessageKind::HasPk, 7),
        (SalMessageKind::Push, 7),
        (SalMessageKind::DdlSync, seq),
        (SalMessageKind::Flush, 7),
    ];
    for (kind, target_id) in groups {
        // Addresses no worker.
        let set = match kind {
            SalMessageKind::DdlSync => WorkerSet::EMPTY,
            _ => WorkerSet::ALL,
        };
        sal.excl()
            .write(&DirectGroup {
                template: ipc::WireMsg { target_id, ..Default::default() },
                targets: GroupTargets { set, ..GroupTargets::UNADDRESSED },
                ..DirectGroup::new(kind)
            })
            .expect("group fits");
    }
    while let Some(req) = wp.next_request() {
        wp.dispatch_in_eval(req);
    }

    let parked: Vec<_> = wp.deferred.iter().map(|r| (r.kind, r.route.target_id)).collect();
    assert_eq!(
        parked,
        [(SalMessageKind::ScanSpec, 76), (SalMessageKind::DeltaRead, 77)]
    );
    let out = frames(&rx);
    let statuses: Vec<_> = out.iter().map(|f| f.ctrl.hdr.status).collect();
    assert_eq!(
        statuses,
        [WireStatus::Error, WireStatus::Ok, WireStatus::Ok],
        "HasPk (a keyless probe faults), Push and Flush answer; the train stays queued"
    );
    let fault = out[0].ctrl.fault(&out[0].bytes).expect("a fault frame");
    assert!(fault.text.contains("carries its keys"), "{fault}");
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
    let (mut wp, sal, rx) = test_worker(&mut engine);

    // The first round in `arg0`, the tids in the blob, as `write_tick_group`
    // lays them out.
    let tids: Vec<u8> = [500u64, 501].iter().flat_map(|t| t.to_le_bytes()).collect();
    sal.excl()
        .write(&DirectGroup {
            template: ipc::WireMsg {
                arg0: 7,
                blob: &tids,
                ..Default::default()
            },
            ..DirectGroup::new(SalMessageKind::Tick)
        })
        .expect("group fits");
    wp.drain_sal();

    for tid in [500, 501] {
        assert!(wp.cat().dag.take_unticked(tid).is_none(), "tid {tid} was ticked");
    }
    let out = frames(&rx);
    assert_eq!(out.len(), 1, "the group ACKs once");
    assert_eq!(out[0].ctrl.hdr.status, WireStatus::Ok);
    let _ = std::fs::remove_dir_all(&dir);
}

// -- reply trains -----------------------------------------------------------

/// Every reply shape through one budget. A reply that fits and is not `fifo`
/// goes out at once; every other reply queues, and the queue drains strictly
/// FIFO, train by train. Each train's frames carry `continuation`, no schema
/// block, and `scan_last` on the terminal frame alone, and reassemble to the
/// source — PKs, payloads and **weights**, since in a Z-set engine a row-set
/// comparison would test nothing.
#[test]
fn reply_trains_drain_fifo_and_reassemble_exactly() {
    let fixed = make_schema_u64_i64();
    let strings = make_schema_pk_u64_payload_string();
    let budget = frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 4]));

    let a = make_batch_raw(&fixed, &(0..10).map(|i| (i, weight(i), i as i64)).collect::<Vec<_>>());
    let b_vals: Vec<String> = (0..25).map(|i| format!("value-{i}-{}", "p".repeat(40))).collect();
    let b = make_batch_bytes_raw(
        &strings,
        &(0..25u64)
            .map(|i| (i, weight(i), b_vals[i as usize].as_bytes()))
            .collect::<Vec<_>>(),
    );
    let wide = ["x".repeat(4096), "y".repeat(4096)];
    let c = make_batch_bytes_raw(&strings, &[(1, 1, wide[0].as_bytes()), (2, -2, wide[1].as_bytes())]);
    let one = make_batch_raw(&fixed, &[(9, -3, 90)]);
    let empty = Batch::empty_with_schema(&fixed);
    let trains = [
        (11, &a, fixed),
        (22, &b, strings),
        (33, &c, strings),
        (55, &one, fixed),
        (66, &empty, fixed),
    ];

    let (mut wp, _sal, rx) = test_worker(std::ptr::null_mut());
    wp.reply_frame_budget = budget;
    wp.send_reply(route(1, 11), a.clone());
    wp.send_reply(route(2, 22), b.clone());
    wp.send_reply(route(3, 33), c.clone());
    wp.send_reply(route(4, 44), one.clone());
    wp.send_reply(fifo_route(5, 55), one.clone());
    wp.send_reply(fifo_route(6, 66), empty.clone());

    let inline = frames(&rx);
    assert_eq!(inline.len(), 1, "only the fitting non-fifo reply emits at enqueue time");
    assert_eq!(inline[0].req, 44);
    assert!(inline[0].ctrl.hdr.flags.scan_last);
    assert_eq!(weighted_rows(&inline[0].rows(&fixed)), weighted_rows(&one));

    drain_trains(&mut wp);
    let out = frames(&rx);
    let mut order: Vec<u32> = out.iter().map(|f| f.req).collect();
    order.dedup();
    assert_eq!(
        order,
        [11, 22, 33, 55, 66],
        "each train's frames fully precede the next's"
    );

    for (req, source, schema) in trains {
        let train: Vec<&Frame> = out.iter().filter(|f| f.req == req).collect();
        let mut got = Vec::new();
        for (i, f) in train.iter().enumerate() {
            let hdr = &f.ctrl.hdr;
            assert_eq!(hdr.status, WireStatus::Ok);
            assert!(hdr.flags.continuation);
            assert!(f.ctrl.schema.is_none(), "a reply frame carries no schema block");
            assert_eq!(
                hdr.flags.scan_last,
                i + 1 == train.len(),
                "train {req}: scan_last on frame {i}"
            );
            got.extend(weighted_rows(&f.rows(&schema)));
        }
        assert_eq!(got, weighted_rows(source), "train {req} reassembles to its source");
    }

    let sizes = |req| -> Vec<(usize, usize)> {
        out.iter()
            .filter(|f| f.req == req)
            .map(|f| (f.rows(if req == 11 { &fixed } else { &strings }).len(), f.bytes.len()))
            .collect()
    };
    // Fixed width: every non-terminal frame fills the budget exactly.
    assert_eq!(
        sizes(11),
        [
            (4, budget),
            (4, budget),
            (2, frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 2])))
        ]
    );
    // Heap strings: several rows a frame, each frame compacted to its own heap.
    let b_sizes = sizes(22);
    assert!(b_sizes.len() >= 2, "{b_sizes:?}");
    assert!(b_sizes.iter().all(|&(_, bytes)| bytes <= budget), "{b_sizes:?}");
    // A row wider than the budget ships alone, over budget, with no fault.
    let c_sizes = sizes(33);
    assert_eq!(c_sizes.len(), 2);
    assert!(
        c_sizes.iter().all(|&(rows, bytes)| rows == 1 && bytes > budget),
        "{c_sizes:?}"
    );
}

/// A `fifo` reply that fits goes out as ONE frame over the SOURCE batch — no
/// sub-batch, so no copy and no heap relocation on the path every `scan_many`
/// relation takes. The source carries dead heap bytes, which a sub-batch would
/// compact away: the frame's byte size is what tells the two paths apart.
#[test]
fn fifo_emits_a_fitting_reply_over_the_source_batch() {
    let schema = make_schema_pk_u64_payload_string();
    let live = make_batch_bytes_raw(
        &schema,
        &[(1, 1, b"a long enough value"), (2, 1, b"another long value")],
    );
    // The same rows over a heap padded with 4096 unreferenced bytes, framed as
    // an engine block that states them.
    let padded = [live.blob(), &[0u8; 4096]].concat();
    let mut regions: Vec<&[u8]> = live.wire_regions().to_vec();
    *regions.last_mut().unwrap() = &padded;
    let mut block = Vec::new();
    gnitz_wire::wal::append_block(&regions, 4096, &mut block);
    let batch = Rc::new(Batch::decode_from_wal_block(&block, &schema).unwrap());
    assert!(
        batch.dead_heap() > 0,
        "the fixture must have dead heap bytes for the size test to discriminate"
    );

    let (mut wp, _sal, rx) = test_worker(std::ptr::null_mut());
    wp.send_reply(fifo_route(1, 5), Rc::clone(&batch));
    drain_trains(&mut wp);
    let out = frames(&rx);
    assert_eq!(out.len(), 1);
    assert!(out[0].ctrl.hdr.flags.scan_last);
    assert_eq!(
        out[0].bytes.len(),
        frame_size(&batch),
        "the frame must be the source batch verbatim, heap and all"
    );
}

/// A row wider than [`gnitz_wire::MAX_FRAME_PAYLOAD`] answers its request with
/// the oversize fault, and the train behind it still ships.
#[test]
fn a_row_wider_than_the_frame_cap_faults_its_train_alone() {
    let schema = make_schema_pk_u64_payload_string();
    // One row whose string alone exceeds what a client can read in one frame.
    let batch = make_batch_bytes_raw(
        &schema,
        &[(1, 1, "z".repeat(gnitz_wire::MAX_FRAME_PAYLOAD + 4096).as_bytes())],
    );
    let next = make_batch_raw(&make_schema_u64_i64(), &[(1, 1, 10)]);

    let (mut wp, _sal, rx) = test_worker(std::ptr::null_mut());
    wp.send_reply(route(1, 5), batch);
    wp.send_reply(fifo_route(2, 6), next);
    drain_trains(&mut wp);

    let out = frames(&rx);
    assert_eq!(out.iter().map(|f| f.req).collect::<Vec<_>>(), [5, 6]);
    let fault = out[0].ctrl.fault(&out[0].bytes).expect("a fault frame");
    assert_eq!(fault.status, WireStatus::Error);
    assert!(
        fault.text.contains("exceeds the maximum frame payload"),
        "the fault names the cap: {fault}"
    );
    assert_eq!(out[1].ctrl.hdr.status, WireStatus::Ok);
    assert!(out[1].ctrl.hdr.flags.scan_last);
}
