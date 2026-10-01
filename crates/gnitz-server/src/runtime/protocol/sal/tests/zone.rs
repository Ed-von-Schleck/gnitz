use super::*;
use crate::runtime::sal::fixtures::{group_at, TestLog};
use crate::runtime::sal::{DirectGroup, GroupData, SalMessageKind, ANCHOR_BYTES, ANCHOR_RECORD, PREFIX_BYTES};
use crate::runtime::wire::{WireData, WireMsg, WireSchema};
use crate::test_support::{make_batch, make_schema_u64_i64, sweep_bit_flips};
use gnitz_zset::repr::Batch;

const SIZE: usize = 1 << 20;
const NW: usize = 4;
const TID: u64 = 16;

/// The zone LSNs a quiescent log commits, in log order.
fn committed(log: SalLog) -> Result<Vec<u64>, String> {
    let mut lsns: Vec<u64> = CommittedTail::read(log)?.groups().map(|m| m.lsn).collect();
    lsns.dedup();
    Ok(lsns)
}

/// The `(lsn, target_id)` of each group the walk reads from 0.
fn walk(log: SalLog) -> Vec<(u64, u64)> {
    let epoch = log.anchor().expect("the anchor verifies").0;
    log.walk(epoch).map(|m| (m.lsn, m.target_id)).collect()
}

/// Unsynced, damage at `stop` commits exactly `before`; once the log is
/// synced, it fails the boot naming `stop`.
fn truncates_then_fails(case: &str, log: &TestLog, before: &[u64], stop: u64) {
    assert_eq!(
        committed(log.log()).unwrap(),
        before,
        "{case}: unsynced, the zones before the stop"
    );
    log.synced_through(log.cursor());
    let err = committed(log.log()).expect_err("damage below the anchored offset is a hole");
    assert!(
        err.contains(&format!("offset={stop},")),
        "{case}: the error names the stop: {err}"
    );
}

// -----------------------------------------------------------------------
// The publication prefix
// -----------------------------------------------------------------------

/// No bit of a closing member's prefix word changes the walk or loses the
/// zone, even at epoch 1: the stride comes from the digested directory, the
/// generation from the header's own epoch, and no single flip zeroes the word.
#[test]
fn a_closing_members_prefix_is_not_single_bit_zeroable() {
    let log = TestLog::new(SIZE, 1, 1);
    let (lsn, bases) = log.zone(&[11]);
    log.command();

    let view = log.log();
    let clean = walk(view);
    assert_eq!(clean.len(), 2, "the zone's one member, a tick");
    assert_eq!(committed(view).unwrap(), vec![lsn]);

    let prefix = unsafe { std::slice::from_raw_parts_mut(log.ptr().add(bases[0] as usize), PREFIX_BYTES) };
    sweep_bit_flips(prefix, 0..PREFIX_BYTES, |byte, bit, _| {
        assert_eq!(walk(view), clean, "prefix byte {byte} bit {bit} changed the walk");
        assert_eq!(
            committed(view).unwrap(),
            vec![lsn],
            "prefix byte {byte} bit {bit} lost the zone"
        );
    });
}

// -----------------------------------------------------------------------
// The anchor
// -----------------------------------------------------------------------

/// All-zero anchor bytes are a fresh file.
#[test]
fn a_fresh_file_anchors_epoch_0() {
    let log = TestLog::new(SIZE, 1, 1);
    unsafe { std::ptr::write_bytes(log.anchor_ptr(), 0, ANCHOR_RECORD) };
    let view = log.log();
    assert_eq!(view.anchor(), Ok((0, 0)));
    let tail = CommittedTail::read(view).unwrap();
    assert_eq!((tail.live_epoch(), tail.groups().count()), (1, 0));
}

/// A damaged anchor fails the boot.
#[test]
fn a_flipped_anchor_bit_fails_the_boot() {
    let log = TestLog::new(SIZE, 1, 3);
    log.zone(&[11]);
    log.synced_through(log.cursor());
    let view = log.log();
    assert_eq!(view.anchor(), Ok((3, log.cursor())));

    let anchor = unsafe { &mut *view.anchor_record() };
    sweep_bit_flips(anchor, 0..ANCHOR_RECORD, |byte, bit, _| {
        let err = CommittedTail::read(view).err();
        assert!(
            err.is_some_and(|e| e.contains("SAL anchor")),
            "anchor byte {byte} bit {bit} must fail the read"
        );
    });
}

/// A checkpoint reset durably anchors the next epoch at cursor 0: the old
/// epoch's groups are leftovers, an fdatasync completing after it covered the
/// abandoned epoch only, and the next group lands at 0.
#[test]
fn a_checkpoint_reset_durably_moves_the_anchor_epoch() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(&[11]);
    log.zone(&[21]);
    let (epoch, through) = (log.writer.epoch(), log.cursor());
    log.excl().checkpoint_reset();
    log.writer.mark_synced(epoch, through);

    let view = log.log();
    assert_eq!(view.anchor(), Ok((2, 0)), "the new epoch, nothing synced");
    assert_eq!(log.cursor(), 0);
    assert_eq!(walk(view), []);
    assert!(committed(view).unwrap().is_empty());

    let (lsn, bases) = log.zone(&[31]);
    assert_eq!(bases, [0]);
    assert_eq!(committed(view).unwrap(), [lsn]);
}

/// A previous epoch's leftover past the new frontier ends the log.
#[test]
fn a_previous_epochs_leftover_ends_the_walk() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(&[11]);
    log.zone(&[22]);
    log.zone(&[33]);
    // A shorter epoch-2 log over the same bytes: one group, so epoch 1's
    // second and third groups survive past the new frontier.
    log.excl().checkpoint_reset();
    let (lsn, _) = log.zone(&[44]);

    assert_eq!(walk(log.log()), [(lsn, 44)], "the walk ends at the epoch-1 leftover");
}

// -----------------------------------------------------------------------
// The prefix rule: the walk stops at the first defect
// -----------------------------------------------------------------------

/// Every defect ends the committed set at the zone before it, and fails the
/// boot once it lies below the synced offset. Each row builds a log, damages
/// it, and returns the zones before the stop and the stop.
#[test]
fn a_defect_truncates_unsynced_and_fails_synced() {
    type Row = fn(&mut TestLog) -> (Vec<u64>, u64);
    // Damage to member `i` of the middle one of three three-member zones.
    fn middle_zone_member(log: &mut TestLog, i: usize) -> (Vec<u64>, u64) {
        let (first, _) = log.zone(&[11, 12, 13]);
        let (_, second) = log.zone(&[21, 22, 23]);
        log.zone(&[31, 32, 33]);
        log.damage_header(second[i]);
        (vec![first], second[i])
    }
    let rows: [(&str, Row); 8] = [
        ("the last zone's closing member", |log| {
            let (first, _) = log.zone(&[11, 12]);
            let (_, last) = log.zone(&[21, 22]);
            log.damage_header(last[1]);
            (vec![first], last[1])
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
            (vec![first], tick)
        }),
        ("the stream groups a stream-only commit batch writes", |log| {
            let stream = log.write(41, 0, SalMessageKind::Push, &[&[0u8; 64]]);
            log.zone(&[11]);
            log.damage_header(stream);
            (vec![], stream)
        }),
        ("a member of another zone while one is open", |log| {
            let (first, _) = log.zone(&[11]);
            log.open_member();
            let (_, third) = log.zone(&[31]);
            (vec![first], third[0])
        }),
        ("a torn slot in the first member of the last zone", |log| {
            let (first, _) = log.push_zone(&[TID]);
            let (_, last) = log.push_zone(&[TID, TID + 1]);
            log.damage_slot(last[0], 0);
            (vec![first], last[0])
        }),
    ];
    for (case, build) in rows {
        let mut log = TestLog::new(SIZE, NW, 1);
        let (before, stop) = build(&mut log);
        truncates_then_fails(case, &log, &before, stop);
    }
}

/// An unclosed tail zone past the synced offset costs nothing, torn or not.
#[test]
fn an_unclosed_tail_zone_costs_nothing() {
    let mut log = TestLog::new(SIZE, 1, 1);
    let (first, _) = log.zone(&[11]);
    let (second, _) = log.zone(&[21]);
    log.synced_through(log.cursor());
    let torn = log.open_member();
    assert_eq!(committed(log.log()).unwrap(), [first, second]);

    log.damage_header(torn);
    assert_eq!(
        committed(log.log()).unwrap(),
        [first, second],
        "every committed zone before the torn header must survive"
    );
}

/// A log read under a smaller `GNITZ_SAL_BYTES` than it was written under.
#[test]
fn a_stop_past_the_mapping_below_synced_fails() {
    let log = TestLog::new(SIZE, 1, 1);
    let (first, _) = log.zone(&[11]);
    let (_, second) = log.zone(&[21]);
    // A mapping that ends inside the second zone's group.
    let shrunk = || unsafe { SalLog::new(log.anchor_ptr(), ANCHOR_BYTES + second[0] as usize + 64) };
    assert_eq!(committed(shrunk()).unwrap(), [first]);

    log.synced_through(log.cursor());
    let err = committed(shrunk()).expect_err("fsynced groups past the new bound");
    assert!(err.contains(&format!("offset={},", second[0])), "{err}");
}

/// Rot in any worker's slot demotes the zone.
#[test]
fn the_demotion_is_global_across_slots() {
    for victim in 0..NW as u32 {
        let log = TestLog::new(SIZE, NW, 1);
        let (_, bases) = log.push_zone(&[TID]);
        log.damage_slot(bases[0], victim);
        truncates_then_fails(&format!("slot {victim}"), &log, &[], bases[0]);
    }
}

/// Slot rot in an unzoned group inside a zone's span — a stream push coalesced
/// into a base table's batch — costs the zone nothing.
#[test]
fn slot_damage_in_an_unzoned_group_does_not_stop_the_walk() {
    let log = TestLog::new(SIZE, NW, 1);
    let batch = rows();
    let (member, stream) = (
        WireSchema::encoded(TID, batch.schema()),
        WireSchema::encoded(TID + 1, batch.schema()),
    );
    let data = GroupData::Same(WireData::Whole(&batch));
    let (member, stream) = (DirectGroup::push(&member, data, 0), DirectGroup::push(&stream, data, 0));
    let mut excl = log.excl();
    let scope = excl.begin("test");
    scope.write(&member, true).expect("group fits");
    let unzoned = log.cursor();
    scope.write(&stream, false).expect("group fits");
    scope.write(&member, true).expect("group fits");
    let lsn = scope.lsn();
    assert!(scope.commit(), "the zone was open");
    log.damage_slot(unzoned, 0);

    let members: Vec<(u64, u64)> = CommittedTail::read(log.log())
        .unwrap()
        .groups()
        .map(|m| (m.lsn, m.target_id))
        .collect();
    assert_eq!(
        members,
        [(lsn, TID), (lsn, TID)],
        "both members replay; the stream group is not one"
    );
}

/// Two rows every push below carries.
fn rows() -> Batch {
    make_batch(&make_schema_u64_i64(), &[(1, 1, 10), (2, 1, 20)])
}

/// Log shapes and damage for the recovery walk.
impl TestLog {
    /// A committed zone of one control-only `DdlSync` group per target: its
    /// LSN and every member's base.
    fn zone(&self, targets: &[u64]) -> (u64, Vec<u64>) {
        let groups: Vec<DirectGroup> = targets
            .iter()
            .map(|&target_id| DirectGroup {
                template: WireMsg { target_id, ..Default::default() },
                ..DirectGroup::new(SalMessageKind::DdlSync)
            })
            .collect();
        self.commit_zone(&groups)
    }

    /// A committed zone of one `Push` group per target, every worker sent
    /// [`rows`].
    fn push_zone(&self, targets: &[u64]) -> (u64, Vec<u64>) {
        let batch = rows();
        let relations: Vec<WireSchema> = targets
            .iter()
            .map(|&t| WireSchema::encoded(t, batch.schema()))
            .collect();
        let groups: Vec<DirectGroup> = relations
            .iter()
            .map(|r| DirectGroup::push(r, GroupData::Same(WireData::Whole(&batch)), 0))
            .collect();
        self.commit_zone(&groups)
    }

    /// The command group the committer fires between zones: `lsn = 0`.
    fn command(&self) -> u64 {
        self.write(9, 0, SalMessageKind::Tick, &[&[0u8; 64]])
    }

    /// The first member of a zone that never closes, at the LSN the next scope
    /// would take — what a crash between a zone's groups leaves. Returns its base.
    fn open_member(&self) -> u64 {
        self.write(21, self.writer.next_zone_lsn(), SalMessageKind::DdlSync, &[&[0u8; 64]])
    }

    /// Corrupt one byte of slot `w` of the group at `base`.
    fn damage_slot(&self, base: u64, w: u32) {
        let slot = group_at(self.log(), base).slot(w).expect("slot carries bytes");
        let off = (slot.as_ptr() as usize) - (self.ptr() as usize);
        unsafe { *self.ptr().add(off + slot.len() / 2) ^= 0xFF };
    }
}
