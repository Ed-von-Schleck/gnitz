use super::fixtures::{control_push as to, group_at, scan, test_log, unanswered};
use super::{
    epoch_word, group_header_size, group_limit, Apply, Routed, SalExcl, SalFull, SalMessageKind, SalScope, SalWriter,
    WorkerSet, CHECKPOINT_RESERVE, PREFIX_BYTES,
};
use crate::catalog::SysFamily;
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::sal::format::payload_rows;
use crate::runtime::test_support::try_poll_once;
use crate::runtime::w2m::fixtures::id;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_store::relation::PkRule;
use gnitz_zset::schema::encode_schema_block;
use std::task::Poll;

/// A group that does not fit is refused `SalFull` and leaves the log as it was.
/// Only one within its kind's limit, refused past the checkpoint's floor, calls
/// for a checkpoint, and a base round's rewind clears the call.
#[test]
fn a_group_that_does_not_fit_is_refused_sal_full() {
    let cap = 1024;
    let log = test_log(PREFIX_BYTES + CHECKPOINT_RESERVE + cap, 1, 1);
    let refuse = |payload: usize| {
        let before = log.cursor();
        log.try_forge(0, false, SalMessageKind::ScanSpec, &[&vec![0; payload]])
            .expect_err("the group must be refused");
        assert_eq!(log.cursor(), before, "the log is untouched");
    };

    refuse(cap);
    assert!(!log.needs_checkpoint(), "no checkpoint admits it");

    log.seek((cap - PREFIX_BYTES) as u64);
    refuse(8);
    assert!(log.needs_checkpoint(), "a checkpoint admits it");

    log.excl().checkpoint_reset();
    assert!(!log.needs_checkpoint(), "the rewind clears the refusal");

    log.excl().apply(Apply::FlushEph { generation: 1 }).expect("group fits");
    let over = cap - log.cursor() as usize - PREFIX_BYTES - group_header_size(1) + 8;
    refuse(over);
    assert!(
        !log.needs_checkpoint(),
        "directly behind a checkpoint, another one removes nothing ahead of it"
    );

    log.forge(0, false, SalMessageKind::ScanSpec, &[&[0; 8]]);
    refuse(over);
    assert!(log.needs_checkpoint(), "a checkpoint removes the group ahead of it");
}

/// With the cursor inside the checkpoint band, where a group of any other kind
/// is refused, the band admits a `FlushEph` and a `Flush` one behind the other.
#[test]
fn the_checkpoint_band_admits_both_rounds() {
    let cap = 1024;
    let log = test_log(PREFIX_BYTES + CHECKPOINT_RESERVE + cap, 1, 1);
    log.seek(cap as u64);
    let mut excl = log.excl();

    for (name, refused) in [
        ("a read", excl.read(scan(0, &[]), WorkerSet::ALL, None, id(1))),
        (
            "a tick",
            excl.apply(Apply::Tick { first_round: 2, tids: [1][..].into() })
                .map(drop),
        ),
        ("a push", excl.write_group(&to(1))),
    ] {
        refused.expect_err(name);
    }
    assert_eq!(log.cursor(), cap as u64, "a refused group leaves the log untouched");

    excl.apply(Apply::FlushEph { generation: 1 })
        .expect("an ephemeral round fits the band");
    excl.apply(Apply::Flush).expect("a base round fits behind it");

    let mut at = cap as u64;
    let kinds = [(); 2].map(|()| {
        let msg = group_at(log.log(), at);
        at = msg.end;
        msg.head.request.kind
    });
    assert_eq!(kinds, [SalMessageKind::FlushEph, SalMessageKind::Flush]);
    excl.write_group(&to(1)).expect_err("the band stays closed to a push");
}

/// The checkpoint threshold counts the bytes published behind the epoch's
/// `FlushEph`, at a threshold of one byte: a log that ends with its checkpoint
/// calls for none, one more group calls for the next, and a base round's rewind
/// starts the count again.
#[test]
fn a_checkpoint_is_due_by_the_bytes_behind_the_last_one() {
    let log = test_log(1 << 20, 1, 1).checkpoint_at(1);
    let mut excl = log.excl();
    assert!(!log.needs_checkpoint(), "an empty log");

    excl.write_group(&to(1)).expect("group fits");
    assert!(log.needs_checkpoint(), "one group crosses the threshold");

    // The checkpoint: the base round and its rewind, a tick of the drain, the
    // ephemeral round.
    excl.apply(Apply::Flush).expect("group fits");
    excl.checkpoint_reset();
    assert!(!log.needs_checkpoint(), "the rewind empties the log");
    excl.apply(Apply::Tick { first_round: 2, tids: [1][..].into() })
        .expect("group fits");
    assert!(
        log.needs_checkpoint(),
        "the drain's tick is ahead of the ephemeral round"
    );
    excl.apply(Apply::FlushEph { generation: 1 }).expect("group fits");
    assert!(log.cursor() > 1, "the checkpoint left its groups in the log");
    assert!(!log.needs_checkpoint(), "nothing is behind the completed checkpoint");

    excl.read(scan(0, &[]), WorkerSet::ALL, None, id(1))
        .expect("group fits");
    assert!(log.needs_checkpoint(), "one group behind it");

    excl.checkpoint_reset();
    assert!(!log.needs_checkpoint(), "the next base round's rewind");
}

/// Nothing a scope lays out is visible until it commits; then all of it is, in
/// layout order: every zoned group a zone member, exactly the last of them
/// marked `ZONE_END`, and the unzoned ones — on either side of it — outside the
/// zone. The zone's LSN is the scope's end.
#[test]
fn a_scope_publishes_its_zone_at_commit() {
    let log = test_log(1 << 20, 1, 1);
    let mut excl = log.excl();
    let mut scope = excl.begin();
    let mut bases = Vec::new();
    for (target, zoned) in [(200, true), (201, false), (202, true), (203, false)] {
        bases.push(scope.end());
        scope.write(&to(target), zoned).expect("group fits");
    }
    for &base in &bases {
        assert_eq!(log.log().prefix_word(base), 0);
    }
    assert_eq!(log.cursor(), 0, "the published end stands still inside a scope");

    let lsn = scope.commit().expect("the zone was open");
    assert_eq!(lsn, epoch_word(1, log.cursor()), "the position just past the scope");
    let read: Vec<_> = bases
        .iter()
        .map(|&base| {
            let m = group_at(log.log(), base);
            (m.head.request.target_id, m.head.zoned(), m.head.zone_end())
        })
        .collect();
    assert_eq!(
        read,
        [
            (200, true, false),
            (201, false, false),
            (202, true, true),
            (203, false, false)
        ]
    );
}

/// A unit that fails part-way is taken back whole — the zone's last member
/// included — so the scope still commits the zone before it; a scope dropped
/// uncommitted publishes nothing, and the next group is laid out over its span.
#[test]
fn a_failed_unit_is_taken_back_and_the_rest_commits() {
    let log = test_log(1 << 20, 1, 1);
    let mut excl = log.excl();
    let mut scope = excl.begin();
    scope.write(&to(1), true).expect("group fits");
    let taken_back = scope.end();
    let oversized = vec![0u8; 1 << 20];
    scope
        .unit(|s| {
            s.write(&to(2), true)?;
            s.write(&to(3), false)?;
            s.write(&unanswered(&scan(4, &oversized)), true)
        })
        .expect_err("the last group of the unit does not fit");
    assert_eq!(scope.end(), taken_back, "the staged end is restored");
    assert!(scope.commit().is_some(), "the zone before the unit closes");
    assert_eq!(log.cursor(), taken_back);

    assert!(
        group_at(log.log(), 0).head.zone_end(),
        "the member left is the closing one"
    );
    assert_eq!(log.log().prefix_word(taken_back), 0);

    excl.begin().write(&to(4), true).expect("group fits");
    assert_eq!(log.cursor(), taken_back, "an uncommitted scope moves nothing");
    assert_eq!(log.log().prefix_word(taken_back), 0);

    excl.write_group(&to(5)).expect("group fits");
    assert_eq!(
        group_at(log.log(), taken_back).head.request.target_id,
        5,
        "the dropped span is reused"
    );
}

/// A unit taken back because its second group did not fit a nearly full ring
/// is refused whole: the group behind the refused one is measured, not laid
/// out. Within its kind's limit, the unit calls for a checkpoint: the refusal
/// is the writer's to remember, whatever the scope does with the unit.
#[test]
fn a_taken_back_unit_a_checkpoint_may_admit_calls_for_one() {
    let cap = 1024;
    let log = test_log(PREFIX_BYTES + CHECKPOINT_RESERVE + cap, 1, 1);
    let one = log.push_footprint(&[], Routed::Same(None));
    // Room for one control push, not for two.
    let at = (cap - one - PREFIX_BYTES) as u64;
    log.seek(at);
    let mut excl = log.excl();
    let mut scope = excl.begin();
    let err = scope
        .unit(|s| {
            s.write(&to(1), true)?;
            s.write(&to(2), true)?;
            assert!(!log.needs_checkpoint(), "the unit is judged whole");
            s.write(&to(3), true)
        })
        .expect_err("the second group does not fit");
    assert_eq!(scope.end(), at, "the unit is taken back");
    assert_eq!(err.end - at, 3 * one as u64, "the refusal spans the whole unit");
    assert!(err.checkpoint_may_admit(), "within the limit, past the floor");
    assert!(log.needs_checkpoint(), "a checkpoint may admit the unit");
    assert_eq!(scope.commit(), None, "nothing was left to publish");
    assert_eq!(log.cursor(), at);

    excl.checkpoint_reset();
    assert!(!log.needs_checkpoint(), "the rewind clears the refusal");
}

/// A checkpoint may make room for a span of pushes exactly when it is within
/// the limit of a push and starts past the checkpoint's floor, which is offset 0 in an epoch without an ephemeral
/// round. Directly behind the `FlushEph`, the push of exactly what the
/// checkpoint's groups leave of the limit is laid out, and one a byte larger is
/// refused without calling for a checkpoint.
#[test]
fn a_checkpoint_may_admit_a_unit_within_the_limit_refused_past_its_floor() {
    let ring = PREFIX_BYTES + CHECKPOINT_RESERVE + 4096;
    let log = test_log(ring, 1, 1);
    let limit = group_limit(SalMessageKind::Push, ring);
    let span = |from: u64, bytes: usize| SalFull {
        kind: SalMessageKind::Push,
        end: from + bytes as u64,
        may_admit: false,
    };
    // Asked of a span at the published end, then of one behind a staged group.
    let may_admit = |excl: &mut SalExcl, bytes: usize| {
        let w = excl.writer;
        let mut scope = excl.begin();
        let at_published_end = w.checkpoint_may_admit(scope.end(), &span(scope.end(), bytes));
        scope.write(&to(1), false).expect("group fits");
        (
            at_published_end,
            w.checkpoint_may_admit(scope.end(), &span(scope.end(), bytes)),
        )
    };
    let mut excl = log.excl();
    assert_eq!(may_admit(&mut excl, 8), (false, true), "an empty log");
    assert_eq!(may_admit(&mut excl, limit), (false, true));
    assert_eq!(may_admit(&mut excl, limit + 1), (false, false), "over the limit");

    excl.apply(Apply::Tick { first_round: 2, tids: [1][..].into() })
        .expect("group fits");
    assert_eq!(
        may_admit(&mut excl, 8),
        (true, true),
        "a tick in an epoch without an ephemeral round is ahead of the floor"
    );
    excl.apply(Apply::FlushEph { generation: 1 }).expect("group fits");
    assert_eq!(may_admit(&mut excl, 8), (false, true), "directly behind the checkpoint");
    assert_eq!(may_admit(&mut excl, limit), (false, true));
    assert_eq!(may_admit(&mut excl, limit + 1), (false, false), "over the limit");
    drop(excl);
    let left = limit - log.cursor() as usize;
    assert!(left < limit, "the checkpoint's groups are in the log");

    let payload = vec![0u8; left - PREFIX_BYTES - group_header_size(1)];
    let refuse = [&payload[..], &[0u8; 1]].concat();
    log.try_forge(1, false, SalMessageKind::Push, &[&refuse])
        .expect_err("one byte past what the checkpoint leaves");
    assert!(!log.needs_checkpoint(), "no checkpoint is called for at the floor");
    log.try_forge(1, false, SalMessageKind::Push, &[&payload])
        .expect("exactly what the checkpoint leaves");
    assert_eq!(log.cursor() as usize, limit);

    let at = log.cursor();
    assert!(
        log.checkpoint_may_admit(at, &span(at, 8)),
        "a push is ahead of the floor"
    );
    let mut excl = log.excl();
    excl.checkpoint_reset();
    assert_eq!(may_admit(&mut excl, 8), (false, true), "the rewind empties the log");
}

/// The writer holds pushes once a scope commits a zone with a push in it, and
/// only then: an unzoned push, a push taken back with its unit, a dropped scope
/// and a DDL-only zone leave it clear, and a checkpoint clears it.
#[test]
fn a_committed_zoned_push_marks_the_writer_as_holding_pushes() {
    let log = test_log(1 << 20, 1, 1);
    let batch = make_batch(&make_schema_u64_i64(), &[(1, 1, 10)]);
    let record = encode_schema_block(batch.schema());
    let push = |scope: &mut SalScope, recovered| {
        scope.push(
            16,
            PkRule::Enforce,
            &record,
            Routed::Same(batch.wire_whole()),
            recovered,
        )
    };
    let oversized = vec![0u8; 1 << 20];

    log.ddl_zone(&[(SysFamily::Table, &batch)]);
    assert!(!log.holds_pushes(), "a DDL-only zone holds no push");

    let mut excl = log.excl();
    let mut scope = excl.begin();
    push(&mut scope, false).expect("group fits");
    assert_eq!(scope.commit(), None, "no zone was open");
    assert!(!log.holds_pushes(), "an unzoned push is outside every zone");

    let mut scope = excl.begin();
    scope.ddl_sync(SysFamily::Table, &batch).expect("group fits");
    scope
        .unit(|s| {
            push(s, true)?;
            s.write(&unanswered(&scan(4, &oversized)), true)
        })
        .expect_err("the last group of the unit does not fit");
    assert!(scope.commit().is_some(), "the zone before the unit closes");
    assert!(!log.holds_pushes(), "the push was taken back with its unit");

    push(&mut excl.begin(), true).expect("group fits");
    assert!(!log.holds_pushes(), "a dropped scope publishes nothing");

    let mut scope = excl.begin();
    push(&mut scope, true).expect("group fits");
    assert!(scope.commit().is_some(), "the zone was open");
    assert!(log.holds_pushes(), "a committed zone pushed rows");

    excl.checkpoint_reset();
    assert!(!log.holds_pushes(), "a checkpoint clears it");
}

/// A zone's LSN is the log position just past its scope, so it rises with the
/// log, across a checkpoint too; the watermark follows the last scope that
/// published anything, and nothing else moves it.
#[test]
fn zone_lsns_rise_with_the_log_and_the_watermark_follows_the_commits() {
    let log = test_log(1 << 20, 1, 1);
    let mut excl = log.excl();
    let boot = log.watermark();
    assert_eq!(boot, epoch_word(1, 0), "below every zone LSN of the boot's epoch");

    assert_eq!(excl.begin().commit(), None);
    assert_eq!(log.watermark(), boot, "an empty scope publishes nothing");

    let mut stream = excl.begin();
    stream.write(&to(1), false).expect("group fits");
    assert_eq!(stream.commit(), None, "an unzoned group closes no zone");
    let first = log.watermark();
    assert_eq!(first, epoch_word(1, log.cursor()), "but it is published");
    assert_eq!(group_at(log.log(), 0).head.request.target_id, 1);

    excl.write_group(&to(9)).expect("group fits");
    assert_eq!(excl.begin().commit(), None);
    assert_eq!(log.watermark(), first, "a group outside every scope moves no watermark");

    let mut zoned = excl.begin();
    zoned.write(&to(2), true).expect("group fits");
    let second = zoned.commit().expect("the zone was open");
    assert!(second > first);
    assert_eq!(second, epoch_word(1, log.cursor()));
    assert_eq!(log.watermark(), second);

    excl.checkpoint_reset();
    let mut zoned = excl.begin();
    zoned.write(&to(3), true).expect("group fits");
    assert!(
        zoned.commit().expect("the zone was open") > second,
        "a checkpoint keeps LSNs rising"
    );
}

/// A waiter resolves once the fdatasync the driver runs covers its LSN, and at
/// once for `0`, which no zone carries; a checkpoint's rewind makes every
/// published group durable without one.
#[test]
fn durable_through_waits_for_an_fdatasync_or_a_rewind() {
    let log: &'static SalWriter = Box::leak(Box::new(test_log(1 << 20, 1, 1)));
    let mut excl = log.excl();
    assert!(try_poll_once(log.durable_through(0)).is_some(), "LSN 0 needs no sync");

    let mut zone = |g| {
        let mut scope = excl.begin();
        scope.write(&to(g), true).expect("group fits");
        scope.commit().expect("the zone was open")
    };
    let first = zone(1);
    let mut waiting = Box::pin(log.durable_through(first));
    assert!(try_poll_once(&mut waiting).is_none(), "published, not yet durable");
    let reactor = make_reactor();
    let driver = reactor.clone();
    reactor.spawn(async move { log.run_syncs(&driver).await });
    reactor.block_on(waiting);
    assert!(
        !log.syncing_all(),
        "no fdatasync is in flight once its waiter is answered"
    );

    let second = zone(2);
    let mut waiting = Box::pin(log.durable_through(second));
    assert!(
        try_poll_once(&mut waiting).is_none(),
        "the next zone waits for its own sync"
    );
    excl.checkpoint_reset();
    assert!(try_poll_once(&mut waiting).is_some(), "the rewind's sync covers it");
}

/// The writer reports the fdatasync in flight as covering everything exactly
/// while nothing was published since it was submitted: a zone committed under
/// it ends that, and its waiter is answered as the first zone's is.
#[test]
fn an_fdatasync_in_flight_covers_everything_until_a_zone_commits_under_it() {
    let log: &'static SalWriter = Box::leak(Box::new(test_log(1 << 20, 1, 1)));
    let zone = |g| {
        let mut excl = log.excl();
        let mut scope = excl.begin();
        scope.write(&to(g), true).expect("group fits");
        scope.commit().expect("the zone was open")
    };
    let reactor = make_reactor();
    let driver = reactor.clone();
    reactor.spawn(async move { log.run_syncs(&driver).await });

    reactor.block_on(async {
        let first = zone(1);
        let mut first_durable = Box::pin(log.durable_through(first));
        assert!(
            try_poll_once(&mut first_durable).is_none(),
            "published, not yet durable"
        );
        assert!(!log.syncing_all(), "no fdatasync is in flight yet");
        // Let the driver run until its fdatasync is submitted.
        std::future::poll_fn(|cx| {
            if log.syncing_all() {
                return Poll::Ready(());
            }
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await;

        let second = zone(2);
        assert!(!log.syncing_all(), "the sync in flight was submitted before the zone");
        first_durable.await;
        log.durable_through(second).await;
    });
}

/// Dropping a `SalExcl` wakes each worker a group written under it reached —
/// once, however many groups reached it — and no other; an apply wakes at
/// its write.
#[test]
fn dropping_a_sal_excl_wakes_exactly_the_workers_it_reached() {
    let log = test_log(1 << 20, 4, 1);
    let seqs = || (0..4).map(|w| log.parks().park(w).value()).collect::<Vec<_>>();

    drop(log.excl());
    assert_eq!(seqs(), [0, 0, 0, 0], "nothing written, nothing woken");

    {
        let mut excl = log.excl();
        for set in [WorkerSet::one(2), WorkerSet::one(2).with(0)] {
            excl.read(scan(0, &[]), set, None, id(1)).expect("group fits");
        }
        assert_eq!(seqs(), [0, 0, 0, 0], "no wake before the drop");
    }
    assert_eq!(seqs(), [1, 0, 1, 0], "a worker reached twice is woken once");

    let mut excl = log.excl();
    excl.apply(Apply::Flush).expect("group fits");
    assert_eq!(
        seqs(),
        [2, 1, 2, 1],
        "the broadcast wakes every launched worker at the write"
    );
    drop(excl);
    assert_eq!(seqs(), [2, 1, 2, 1], "and the drop wakes none again");
}

/// A group written through the reused `pwrite` scratch buffer zeroes its
/// payloads' padding, whatever an earlier, larger group left in the buffer.
#[test]
fn a_pwritten_group_zeroes_its_padding_over_an_earlier_group() {
    let log = test_log(1 << 20, 3, 1);
    let dirty = vec![vec![0xFF; 8 << 10]; 3];
    log.excl()
        .read(scan(16, &[]), WorkerSet::ALL, Some(&dirty), id(1))
        .expect("group fits");
    let base = log.cursor();
    let blobs: Vec<Vec<u8>> = vec![vec![1; 5001], vec![2; 3], vec![3; 4003]];
    log.excl()
        .read(scan(16, &[]), WorkerSet::ALL, Some(&blobs), id(1))
        .expect("group fits");

    let msg = group_at(log.log(), base);
    let mut padded = 0;
    for (w, blob) in blobs.iter().enumerate() {
        let slot = msg.slot(w as u32).expect("every worker is written");
        let (sent, rows) = payload_rows(slot, |_| None).expect("a payload decodes");
        assert_eq!((sent, rows.is_none()), (&blob[..], true), "worker {w}");
        let pad = slot.len().next_multiple_of(8) - slot.len();
        // SAFETY: a slot's padding lies inside its group, inside the mapping.
        let padding = unsafe { std::slice::from_raw_parts(slot.as_ptr().add(slot.len()), pad) };
        assert!(padding.iter().all(|&b| b == 0), "worker {w}: padding {padding:?}");
        padded += pad;
    }
    assert!(padded > 0, "some payload is padded");
}
