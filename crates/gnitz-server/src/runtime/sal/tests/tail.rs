use super::*;
use crate::catalog::SysFamily;
use crate::runtime::sal::fixtures::{scan, test_log, unanswered, zone_rows};
use crate::runtime::sal::format::{PREFIX_BYTES, RequestHead, SAL_FORMAT, anchor_word};
use crate::runtime::sal::group::Group;
use crate::runtime::sal::{Routed, SalWriter};
use crate::test_support::encode_schema_record;
use crate::test_support::sweep_bit_flips;
use gnitz_store::relation::PkRule;

const SIZE: usize = 1 << 20;
const NW: usize = 4;
const TID: u64 = 16;

/// The zone LSNs a quiescent log commits, in log order. Every log here ends
/// each zone with its last zoned group, so a committed tail ends at a closing
/// member.
fn committed(log: SalLog) -> Vec<u64> {
    let tail = CommittedTail::read(log).expect("the anchor verifies");
    let members: Vec<_> = tail.groups().collect();
    assert!(
        members.last().is_none_or(|m| m.head.zone_end()),
        "the replay set ends at a closing member"
    );
    (members.iter())
        .filter(|m| m.head.zone_end())
        .map(|m| tail.lsn(m))
        .collect()
}

/// The target of every group a boot replays, in log order.
fn replayed(log: SalLog) -> Vec<u64> {
    let tail = CommittedTail::read(log).expect("the anchor verifies");
    tail.groups().map(|m| m.head.request.target_id).collect()
}

/// Whether each group the walk reads from 0 is a zone member, and its target.
fn walk(log: SalLog) -> Vec<(bool, u64)> {
    let epoch = log.anchor().expect("the anchor verifies");
    log.walk(epoch)
        .map(|m| (m.head.zoned(), m.head.request.target_id))
        .collect()
}

// -----------------------------------------------------------------------
// The publication prefix
// -----------------------------------------------------------------------

/// No bit of a closing member's prefix word changes the walk or loses the
/// zone, even at epoch 1: the stride comes from the digested directory, the
/// generation from the header's own epoch, and no single flip zeroes the word.
#[test]
fn a_closing_members_prefix_is_not_single_bit_zeroable() {
    let mut log = test_log(SIZE, 1, 1);
    let (lsn, bases) = log.zone(&[11]);
    log.command();

    let view = *log.log();
    let clean = walk(view);
    assert_eq!(clean.len(), 2, "the zone's one member, a tick");
    assert_eq!(committed(view), vec![lsn]);

    sweep_bit_flips(log.prefix_mut(bases[0]), 0..PREFIX_BYTES, |byte, bit, _| {
        assert_eq!(walk(view), clean, "prefix byte {byte} bit {bit} changed the walk");
        assert_eq!(committed(view), vec![lsn], "prefix byte {byte} bit {bit} lost the zone");
    });
}

// -----------------------------------------------------------------------
// The anchor
// -----------------------------------------------------------------------

/// All-zero anchor bytes are a fresh file.
#[test]
fn a_fresh_file_anchors_epoch_0() {
    let mut log = test_log(SIZE, 1, 1);
    log.anchor_mut().fill(0);
    let view = *log.log();
    assert_eq!(view.anchor(), Ok(0));
    let tail = CommittedTail::read(view).unwrap();
    assert_eq!((tail.live_epoch(), tail.groups().count()), (1, 0));
}

/// Any flipped bit of the anchor fails the boot.
#[test]
fn a_flipped_anchor_bit_fails_the_boot() {
    let mut log = test_log(SIZE, 1, 3);
    let view = *log.log();
    assert_eq!(view.anchor(), Ok(3));

    sweep_bit_flips(log.anchor_mut(), 0..PREFIX_BYTES, |byte, bit, _| {
        let err = CommittedTail::read(view).err();
        assert!(
            err.is_some_and(|e| e.contains("SAL anchor")),
            "anchor byte {byte} bit {bit} must fail the read"
        );
    });
}

/// A log anchored under another format word is another build's, and fails the
/// boot whatever its groups carry: a zero word (a log written before the word
/// existed), any flipped bit, and another build's whole word alike.
#[test]
fn a_log_of_another_format_fails_the_boot() {
    let mut log = test_log(SIZE, 1, 3);
    log.zone(&[11]);
    let view = *log.log();
    assert_eq!(view.anchor(), Ok(3));

    let refused = |view: SalLog, what: &str| {
        let err = CommittedTail::read(view).err();
        assert!(
            err.is_some_and(|e| e.contains("SAL format word")),
            "{what} must fail the read"
        );
    };
    sweep_bit_flips(log.format_mut(), 0..PREFIX_BYTES, |byte, bit, _| {
        refused(view, &format!("format byte {byte} bit {bit}"));
    });
    for word in [0, SAL_FORMAT ^ 1 << 32] {
        log.format_mut().copy_from_slice(&word.to_ne_bytes());
        refused(view, &format!("format word {word:#x}"));
    }

    // A fresh file has written no group, so its format word is not read.
    log.anchor_mut().fill(0);
    assert_eq!(view.anchor(), Ok(0));
}

/// A rewind stores this build's format word beside the anchor it moves.
#[test]
fn a_rewind_stamps_the_format_word() {
    let mut log = test_log(SIZE, 1, 1);
    log.format_mut().fill(0);
    log.excl().checkpoint_reset();
    assert_eq!(log.log().anchor(), Ok(2));
    assert_eq!(log.format_mut(), SAL_FORMAT.to_ne_bytes());
}

/// A rewind anchors the next epoch before it empties the ring, so a kill
/// between the two leaves that epoch's anchor over the old epoch's groups —
/// which the walk rejects by their header epoch.
#[test]
fn the_next_epochs_anchor_over_the_old_ring_is_an_empty_tail() {
    let mut log = test_log(SIZE, 1, 1);
    log.zone(&[11]);
    log.zone(&[21]);
    assert_eq!(committed(*log.log()).len(), 2);

    log.anchor_mut().copy_from_slice(&anchor_word(2).to_ne_bytes());
    let tail = CommittedTail::read(*log.log()).expect("an empty tail");
    assert_eq!((tail.live_epoch(), tail.groups().count()), (3, 0));
}

/// A checkpoint reset durably anchors the next epoch at cursor 0: the old
/// epoch's groups are leftovers, and the next group lands at 0.
#[test]
fn a_checkpoint_reset_durably_moves_the_anchor_epoch() {
    let log = test_log(SIZE, 1, 1);
    log.zone(&[11]);
    log.zone(&[21]);
    log.excl().checkpoint_reset();

    let view = *log.log();
    assert_eq!(view.anchor(), Ok(2), "the new epoch");
    assert_eq!(log.cursor(), 0);
    assert_eq!(walk(view), []);
    assert!(committed(view).is_empty());

    let (lsn, bases) = log.zone(&[31]);
    assert_eq!(bases, [0]);
    assert_eq!(committed(view), [lsn]);
}

/// A previous epoch's leftover past the new frontier ends the log.
#[test]
fn a_previous_epochs_leftover_ends_the_walk() {
    let log = test_log(SIZE, 1, 1);
    log.zone(&[11]);
    log.zone(&[22]);
    log.zone(&[33]);
    // A shorter epoch-2 log over the same bytes: one group, so epoch 1's
    // second and third groups survive past the new frontier.
    log.excl().checkpoint_reset();
    log.zone(&[44]);

    assert_eq!(walk(*log.log()), [(true, 44)], "the walk ends at the epoch-1 leftover");
}

// -----------------------------------------------------------------------
// The prefix rule: the walk stops at the first defect
// -----------------------------------------------------------------------

/// Every defect ends the committed set at the zone before it. Each row builds
/// a log, damages it, and returns the zones before the stop.
#[test]
fn a_defect_ends_the_committed_set_at_the_zone_before_it() {
    type Row = fn(&mut SalWriter) -> Vec<u64>;
    // Damage to member `i` of the middle one of three three-member zones.
    fn middle_zone_member(log: &mut SalWriter, i: usize) -> Vec<u64> {
        let (first, _) = log.zone(&[11, 12, 13]);
        let (_, second) = log.zone(&[21, 22, 23]);
        log.zone(&[31, 32, 33]);
        log.damage_header(second[i]);
        vec![first]
    }
    let rows: [(&str, Row); 7] = [
        ("the last zone's closing member", |log| {
            let (first, _) = log.zone(&[11, 12]);
            let (_, last) = log.zone(&[21, 22]);
            log.damage_header(last[1]);
            vec![first]
        }),
        ("a zone's first member", |log| middle_zone_member(log, 0)),
        ("a zone's middle member", |log| middle_zone_member(log, 1)),
        ("a zone's closing member", |log| middle_zone_member(log, 2)),
        ("a command group between zones", |log| {
            let (first, _) = log.zone(&[11]);
            let tick = log.command();
            log.zone(&[21]);
            log.zone(&[31]);
            log.damage_header(tick);
            vec![first]
        }),
        ("the stream groups a stream-only commit batch writes", |log| {
            let stream = log.forge(41, false, SalMessageKind::Push, &[&[0u8; 64]]);
            log.zone(&[11]);
            log.damage_header(stream);
            vec![]
        }),
        ("a torn slot in the first member of the last zone", |log| {
            let (first, _) = log.push_zone(&[TID]);
            let (_, last) = log.push_zone(&[TID, TID + 1]);
            log.damage_slot(last[0], 0);
            vec![first]
        }),
    ];
    for (case, build) in rows {
        let mut log = test_log(SIZE, NW, 1);
        let before = build(&mut log);
        assert_eq!(committed(*log.log()), before, "{case}: the zones before the stop");
    }
}

/// What a boot reads of the tail and what the writer that wrote it knew agree
/// on whether a push committed, over every shape a log takes.
#[test]
fn the_tail_and_its_writer_agree_on_holding_pushes() {
    type Shape = fn(&SalWriter);
    let shapes: [(&str, Shape, bool); 6] = [
        ("an empty log", |_| {}, false),
        (
            "control-only zones around a command",
            |log| {
                log.zone(&[11, 12]);
                log.command();
                log.zone(&[21]);
            },
            true,
        ),
        (
            "a push zone",
            |log| {
                log.push_zone(&[TID, TID + 1]);
            },
            true,
        ),
        (
            "a DDL-only zone",
            |log| log.ddl_zone(&[(SysFamily::Table, &zone_rows())]),
            false,
        ),
        (
            "a stream push outside every zone",
            |log| {
                let batch = zone_rows();
                let record = encode_schema_record(batch.schema());
                let mut excl = log.excl();
                let mut scope = excl.begin();
                scope
                    .push(TID, PkRule::Enforce, &record, Routed::Same(batch.wire_whole()), false)
                    .expect("group fits");
                assert_eq!(scope.commit(), None, "no zone was open");
            },
            false,
        ),
        (
            "a push zone left open",
            |log| {
                log.open_member();
            },
            false,
        ),
    ];
    for (case, build, holds) in shapes {
        let log = test_log(SIZE, NW, 1);
        build(&log);
        let tail = CommittedTail::read(*log.log()).expect("the anchor verifies");
        assert_eq!(tail.holds_pushes(), holds, "{case}: the tail");
        assert_eq!(log.holds_pushes(), holds, "{case}: the writer");
    }
}

/// An unclosed tail zone costs nothing, torn or not.
#[test]
fn an_unclosed_tail_zone_costs_nothing() {
    let mut log = test_log(SIZE, 1, 1);
    let (first, _) = log.zone(&[11]);
    let (second, _) = log.zone(&[22]);
    let torn = log.open_member();
    assert_eq!(
        walk(*log.log()),
        [(true, 11), (true, 22), (true, 21)],
        "the open member is published and read as a zone member"
    );
    assert_eq!(committed(*log.log()), [first, second]);
    assert_eq!(replayed(*log.log()), [11, 22], "the open member is not replayed");

    log.damage_header(torn);
    assert_eq!(replayed(*log.log()), [11, 22]);
    assert_eq!(
        committed(*log.log()),
        [first, second],
        "every committed zone before the torn header must survive"
    );
}

/// Rot in any worker's payload demotes the zone.
#[test]
fn the_demotion_is_global_across_slots() {
    for victim in 0..NW as u32 {
        let mut log = test_log(SIZE, NW, 1);
        let (_, bases) = log.push_zone(&[TID]);
        log.damage_slot(bases[0], victim);
        assert_eq!(committed(*log.log()), [], "slot {victim}");
    }
}

/// Slot rot in an unzoned group inside a zone's span — a stream push coalesced
/// into a base table's batch — costs the zone nothing.
#[test]
fn slot_damage_in_an_unzoned_group_does_not_stop_the_walk() {
    let mut log = test_log(SIZE, NW, 1);
    let batch = zone_rows();
    let record = encode_schema_record(batch.schema());
    let rows = Routed::Same(batch.wire_whole());
    let mut excl = log.excl();
    let mut scope = excl.begin();
    let mut push = |tid, recovered| {
        let base = scope.end();
        scope
            .push(tid, PkRule::Enforce, &record, rows, recovered)
            .expect("group fits");
        base
    };
    push(TID, true);
    let unzoned = push(TID + 1, false);
    push(TID, true);
    let lsn = scope.commit().expect("the zone was open");
    drop(excl);
    log.damage_slot(unzoned, 0);

    let tail = CommittedTail::read(*log.log()).unwrap();
    let members: Vec<(bool, u64)> = tail
        .groups()
        .map(|m| (m.head.zoned(), m.head.request.target_id))
        .collect();
    assert_eq!(
        members,
        [(true, TID), (true, TID)],
        "both members replay; the stream group is not one"
    );
    assert_eq!(committed(*log.log()), [lsn], "the zone closes at the scope's end");
}

// -----------------------------------------------------------------------
// The typed read
// -----------------------------------------------------------------------

/// The members of the closed zones, each as its ingest: a command between
/// zones and the member of an open zone are not among them.
#[test]
fn the_tail_yields_each_members_ingest_in_log_order() {
    let log = test_log(SIZE, NW, 1);
    log.ddl_zone(&[(SysFamily::Table, &zone_rows())]);
    log.push_zone(&[TID, TID + 1]);
    log.command();
    log.open_member();

    let tail = CommittedTail::read(*log.log()).expect("the anchor verifies");
    let ingests: Vec<_> = tail.ingests().map(|g| g.expect("an ingest").0).collect();
    assert_eq!(
        ingests,
        [
            Ingest::DdlSync { family: SysFamily::Table },
            Ingest::Push { tid: TID, rule: PkRule::Enforce },
            Ingest::Push { tid: TID + 1, rule: PkRule::Enforce },
        ]
    );
}

/// A committed zone member whose header decodes to no ingest is an error of
/// the replay.
#[test]
fn a_zone_member_that_is_no_ingest_fails_the_replay() {
    let rows = zone_rows();
    let read = scan(4, &[]);
    let cases = [
        (
            "a DdlSync naming no system family",
            Group {
                request: RequestHead::new(SalMessageKind::DdlSync, u64::MAX, 0, 0),
                ..Group::ddl_sync(SysFamily::Table, &rows)
            },
        ),
        ("a zoned read", unanswered(&read)),
    ];
    for (case, group) in cases {
        let log = test_log(SIZE, NW, 1);
        log.commit_zone(&[group]);
        let tail = CommittedTail::read(*log.log()).expect("the anchor verifies");
        assert!(tail.ingests().next().is_some_and(|g| g.is_err()), "{case}");
    }
}
