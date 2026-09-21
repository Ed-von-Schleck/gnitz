use super::*;
use crate::runtime::sal::fixtures::{group_at, TestLog};
use crate::runtime::sal::{group_header_size, DirectGroup, SalMessageKind, ANCHOR_BYTES, FLAG_ZONE_END, PREFIX_BYTES};
use crate::runtime::wire::WireMsg;
use crate::test_support::{make_batch, make_schema_u64_i64, sweep_bit_flips};

const SIZE: usize = 1 << 20;
const NW: usize = 4;
const TID: u32 = 16;

/// The zone LSNs a quiescent log commits, in log order.
fn committed(log: SalLog) -> Result<Vec<u64>, String> {
    let mut lsns: Vec<u64> = CommittedTail::read(log)?.groups().map(|m| m.lsn).collect();
    lsns.dedup();
    Ok(lsns)
}

/// The `(lsn, target_id)` of each group read from 0, and the corrupt offset
/// the walk stops on, if any.
fn walk(log: SalLog) -> (Vec<(u64, u32)>, Option<u64>) {
    let epoch = log.anchor().expect("the anchor verifies").0;
    let (mut groups, mut at) = (Vec::new(), 0);
    loop {
        match log.read_at(at, EpochGate::Walk(epoch)) {
            SalStep::Group(m, next) => {
                groups.push((m.lsn, m.target_id));
                at = next;
            }
            SalStep::Corrupt(off) => return (groups, Some(off)),
            SalStep::Absent => return (groups, None),
        }
    }
}

/// Unsynced, damage at `stop` commits exactly `before`; once the log is
/// synced, it fails the boot naming `stop`.
fn truncates_then_fails(log: &TestLog, before: &[u64], stop: u64) {
    assert_eq!(
        committed(log.log()).unwrap(),
        before,
        "unsynced: the zones before the stop"
    );
    log.synced_through(log.cursor());
    let err = committed(log.log()).expect_err("damage below the anchored offset is a hole");
    assert!(
        err.contains(&format!("offset={stop},")),
        "the error names the stop: {err}"
    );
}

// -----------------------------------------------------------------------
// The publication prefix
// -----------------------------------------------------------------------

/// No bit of the prefix word changes a walk: the stride comes from the
/// digested directory and the generation from the header's own epoch.
#[test]
fn the_whole_prefix_word_is_neutralised() {
    let mut log = TestLog::new(SIZE, 1, 3);
    log.group(11, 101, SalMessageKind::DdlSync, 0);
    let middle = log.group(22, 102, SalMessageKind::DdlSync, 0);
    log.group(33, 103, SalMessageKind::DdlSync, 0);

    let view = log.log();
    let clean = walk(view);
    assert_eq!(clean, (vec![(101, 11), (102, 22), (103, 33)], None));

    sweep_bit_flips(log.prefix_bytes(middle), 0..PREFIX_BYTES, |byte, bit, _| {
        assert_eq!(walk(view), clean, "prefix byte {byte} bit {bit} changed the walk");
    });
}

/// Presence is the whole prefix word: a small `payload_size` alone is zeroable
/// by one bit flip.
#[test]
fn a_closing_members_prefix_is_not_single_bit_zeroable() {
    let mut log = TestLog::new(SIZE, 1, 1);
    let closing = log.zone(7, &[11])[0];
    log.group(33, 0, SalMessageKind::Tick, 0);
    // A 2-slot empty group, whose payload is 64, a single set bit.
    let empty2 = log
        .try_write(44, 0, SalMessageKind::Tick, 0, &[&[], &[]])
        .expect("group fits");

    let view = log.log();
    let clean = walk(view);
    assert_eq!(clean.0.len(), 3, "the zone's one member, a tick, a 2-slot empty group");
    assert_eq!(committed(view).unwrap(), vec![7]);

    for &base in &[closing, empty2] {
        sweep_bit_flips(log.prefix_bytes(base), 0..PREFIX_BYTES, |byte, bit, _| {
            assert_eq!(walk(view), clean, "prefix {base}+{byte} bit {bit} changed the walk");
            assert_eq!(
                committed(view).unwrap(),
                vec![7],
                "prefix {base}+{byte} bit {bit} lost the zone"
            );
        });
    }
}

// -----------------------------------------------------------------------
// The anchor
// -----------------------------------------------------------------------

/// All-zero anchor bytes are a fresh file.
#[test]
fn a_fresh_file_anchors_epoch_0() {
    let log = TestLog::new(SIZE, 1, 1);
    unsafe { std::ptr::write_bytes(log.anchor_ptr(), 0, ANCHOR_BYTES) };
    let view = log.log();
    assert_eq!(view.anchor(), Ok((0, 0)));
    let tail = CommittedTail::read(view).unwrap();
    assert_eq!((tail.epoch(), tail.groups().count()), (0, 0));
}

/// A damaged anchor fails the boot.
#[test]
fn a_flipped_anchor_bit_fails_the_boot() {
    let log = TestLog::new(SIZE, 1, 3);
    log.zone(1, &[11]);
    log.synced_through(log.cursor());
    let view = log.log();
    assert_eq!(view.anchor(), Ok((3, log.cursor())));

    let anchor = unsafe { std::slice::from_raw_parts_mut(log.anchor_ptr(), 16) };
    sweep_bit_flips(anchor, 0..16, |byte, bit, _| {
        let err = CommittedTail::read(view).err();
        assert!(
            err.is_some_and(|e| e.contains("SAL anchor")),
            "anchor byte {byte} bit {bit} must fail the read"
        );
    });
}

/// After a rewind, the old epoch's groups are leftovers.
#[test]
fn a_rewind_durably_moves_the_anchor_epoch() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    log.zone(2, &[21]);
    log.synced_through(log.cursor());
    log.writer.rewind(2);

    let view = log.log();
    assert_eq!(view.anchor(), Ok((2, 0)), "the new epoch, nothing synced");
    assert_eq!(walk(view), (vec![], None));
    assert!(committed(view).unwrap().is_empty());
}

/// An fdatasync completing after a rewind covered the abandoned epoch only.
#[test]
fn a_mark_synced_from_before_a_rewind_changes_nothing() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    let (epoch, through) = (log.writer.epoch(), log.cursor());
    log.writer.rewind(2);
    log.writer.mark_synced(epoch, through);
    assert_eq!(log.log().anchor(), Ok((2, 0)));
}

/// A previous epoch's leftover past the new frontier ends the log.
#[test]
fn a_previous_epochs_leftover_ends_the_walk() {
    let log = TestLog::new(SIZE, 1, 1);
    log.group(11, 1, SalMessageKind::DdlSync, 0);
    log.group(22, 2, SalMessageKind::DdlSync, 0);
    log.group(33, 3, SalMessageKind::DdlSync, 0);
    // A shorter epoch-2 log over the same bytes: one group, so epoch 1's
    // second and third groups survive past the new frontier.
    log.seek(0, 2);
    log.group(44, 9, SalMessageKind::DdlSync, 0);

    let view = log.log();
    assert_eq!(view.anchor(), Ok((2, 0)), "the anchor names the walk's epoch");
    assert_eq!(
        walk(view),
        (vec![(9, 44)], None),
        "the walk ends at the epoch-1 leftover, which is absent, not corrupt"
    );
}

// -----------------------------------------------------------------------
// The prefix rule: the walk stops at the first defect
// -----------------------------------------------------------------------

/// Damage inside the last zone demotes it alone.
#[test]
fn damage_in_the_last_zone_demotes_it() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11, 12]);
    let last = log.zone(2, &[21, 22]);
    log.damage_header(last[1]);

    assert_eq!(committed(log.log()).unwrap(), vec![1]);
}

/// Damage in an unclosed tail zone costs nothing.
#[test]
fn damage_in_an_unclosed_tail_zone_costs_nothing() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    log.group(21, 2, SalMessageKind::DdlSync, 0);
    let torn = log.group(22, 2, SalMessageKind::DdlSync, 0);
    log.damage_header(torn);

    assert_eq!(committed(log.log()).unwrap(), vec![1]);
}

/// A persisted prefix whose header page was lost, past the synced offset.
#[test]
fn a_torn_header_page_does_not_cost_the_committed_prefix() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    log.zone(2, &[21]);
    log.synced_through(log.cursor());
    // An unclosed tail zone whose first member's header was lost.
    let torn = log.group(31, 3, SalMessageKind::DdlSync, 0);
    log.zero_header(torn, 1);

    assert_eq!(
        committed(log.log()).unwrap(),
        vec![1, 2],
        "every committed zone before the torn page must survive"
    );
}

/// Damage in a middle member of a middle zone.
#[test]
fn damage_in_a_middle_zone() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11, 12, 13]);
    let second = log.zone(2, &[21, 22, 23]);
    log.zone(3, &[31, 32, 33]);
    log.damage_header(second[1]);
    truncates_then_fails(&log, &[1], second[1]);
}

/// Damage in a zone's first member.
#[test]
fn damage_in_a_zones_first_group() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11, 12, 13]);
    let second = log.zone(2, &[21, 22, 23]);
    log.zone(3, &[31, 32, 33]);
    log.damage_header(second[0]);
    truncates_then_fails(&log, &[1], second[0]);
}

/// Damage in a zone's closing member.
#[test]
fn damage_in_a_zones_closing_member() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11, 12, 13]);
    let second = log.zone(2, &[21, 22, 23]);
    log.zone(3, &[31, 32, 33]);
    log.damage_header(second[2]);
    truncates_then_fails(&log, &[1], second[2]);
}

/// Damage in an `lsn = 0` command group between zones.
#[test]
fn damage_in_a_command_group_between_zones() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    let tick = log.command();
    log.zone(2, &[21]);
    log.zone(3, &[31]);
    log.damage_header(tick);
    truncates_then_fails(&log, &[1], tick);
}

/// Damage in the `lsn = 0` groups a stream-only commit batch writes.
#[test]
fn damage_in_stream_groups() {
    let log = TestLog::new(SIZE, 1, 1);
    let first = log.group(41, 0, SalMessageKind::Push, 0);
    let second = log.group(42, 0, SalMessageKind::Push, 0);
    log.zone(9, &[11]);
    log.damage_header(first);
    log.damage_header(second);
    truncates_then_fails(&log, &[], first);
}

/// A zone that lost its first and closing members: the surviving middle one
/// is never replayed.
#[test]
fn a_headless_zone_ends_the_committed_set() {
    let log = TestLog::new(SIZE, 1, 1);
    let first = log.group(21, 2, SalMessageKind::DdlSync, 0);
    let middle = log.group(22, 2, SalMessageKind::DdlSync, 0);
    let closing = log.group(23, 2, SalMessageKind::DdlSync, FLAG_ZONE_END);
    log.zone(3, &[31]);
    log.damage_header(first);
    log.damage_header(closing);

    assert_eq!(walk(log.log()), (vec![], Some(first)));
    assert!(
        CommittedTail::read(log.log()).unwrap().groups().next().is_none(),
        "the survivor at offset {middle} must not replay"
    );
    truncates_then_fails(&log, &[], first);
}

/// A torn one-group zone (the SERIAL shape) ends the committed set, so a later
/// zone on the same sequence never applies without it.
#[test]
fn a_torn_one_group_zone_ends_the_committed_set() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    let second = log.zone(2, &[21]);
    log.zone(3, &[21]);
    log.damage_header(second[0]);
    truncates_then_fails(&log, &[1], second[0]);
}

/// A log read under a smaller `GNITZ_SAL_BYTES` than it was written under.
#[test]
fn a_stop_past_the_mapping_below_synced_fails() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    let second = log.zone(2, &[21]);
    // A mapping that ends inside the second zone's group.
    let shrunk = || unsafe { SalLog::new(log.anchor_ptr(), ANCHOR_BYTES + second[0] as usize + 64) };
    assert_eq!(committed(shrunk()).unwrap(), vec![1]);

    log.synced_through(log.cursor());
    let err = committed(shrunk()).expect_err("fsynced groups past the new bound");
    assert!(err.contains(&format!("offset={},", second[0])), "{err}");
}

/// A member of another zone while one is open.
#[test]
fn an_interleaved_zone_member() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    log.group(21, 2, SalMessageKind::DdlSync, 0);
    let third = log.zone(3, &[31]);
    truncates_then_fails(&log, &[1], third[0]);
}

/// A zone member's torn slot.
#[test]
fn a_torn_slot() {
    let log = TestLog::new(SIZE, NW, 1);
    let bases = log.push_zone(5, &[TID]);
    log.damage_slot(bases[0], log.a_slot_with_rows(bases[0]));
    truncates_then_fails(&log, &[], bases[0]);
}

/// Rot in any worker's slot demotes the zone.
#[test]
fn the_demotion_is_global_across_slots() {
    let probe = TestLog::new(SIZE, NW, 1);
    let base = probe.push_zone(5, &[TID])[0];
    let written: Vec<u32> = group_at(probe.log(), base).slots_written().map(|(w, _)| w).collect();
    assert_eq!(written.len(), NW, "every slot is written");

    for victim in written {
        let log = TestLog::new(SIZE, NW, 1);
        let base = log.push_zone(5, &[TID])[0];
        log.damage_slot(base, victim);
        assert!(
            committed(log.log()).unwrap().is_empty(),
            "rot in slot {victim} must demote the zone"
        );
    }
}

/// A torn last zone is skipped whole.
#[test]
fn a_torn_last_zone_is_skipped_whole() {
    let log = TestLog::new(SIZE, NW, 1);
    log.push_zone(4, &[TID]);
    let last = log.push_zone(5, &[TID, TID + 1]);
    // Rot the FIRST family of the last zone.
    log.damage_slot(last[0], log.a_slot_with_rows(last[0]));

    assert_eq!(
        committed(log.log()).unwrap(),
        vec![4],
        "the torn zone must be dropped whole, and the durable one before it kept"
    );
}

/// Slot rot in an unzoned group inside a zone's span — a stream push coalesced
/// into a base table's batch — costs the zone nothing.
#[test]
fn slot_damage_in_an_unzoned_group_does_not_stop_the_walk() {
    let log = TestLog::new(SIZE, NW, 1);
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let scope = log.writer.begin(5, "test");
    log.push_group(TID, schema, &batch, |g| scope.write(g, true));
    let stream = log.push_group(TID + 1, schema, &batch, |g| scope.write(g, false));
    log.push_group(TID, schema, &batch, |g| scope.write(g, true));
    assert!(scope.commit(), "the zone was open");
    log.damage_slot(stream, log.a_slot_with_rows(stream));

    let members: Vec<(u64, u32)> = CommittedTail::read(log.log())
        .unwrap()
        .groups()
        .map(|m| (m.lsn, m.target_id))
        .collect();
    assert_eq!(
        members,
        vec![(5, TID), (5, TID)],
        "both members replay; the stream group is not one"
    );
}

/// Log shapes and damage for the recovery walk.
impl TestLog {
    /// One group with a 64-byte slot and the given header, outside any scope.
    /// Returns its base.
    fn group(&self, target: u32, lsn: u64, kind: SalMessageKind, flags: u8) -> u64 {
        self.try_write(target, lsn, kind, flags, &[&[0u8; 64]])
            .expect("group fits")
    }

    /// A committed zone of one control-only `DdlSync` group per target.
    /// Returns every member's base.
    fn zone(&self, lsn: u64, targets: &[u32]) -> Vec<u64> {
        let scope = self.writer.begin(lsn, "test");
        let bases: Vec<u64> = targets
            .iter()
            .map(|&t| {
                let base = self.cursor();
                scope
                    .write(
                        &DirectGroup {
                            template: WireMsg {
                                target_id: t as u64,
                                ..Default::default()
                            },
                            ..DirectGroup::new(SalMessageKind::DdlSync)
                        },
                        true,
                    )
                    .expect("group fits");
                base
            })
            .collect();
        assert!(scope.commit(), "the zone was open");
        bases
    }

    /// The command group the committer fires between zones: `lsn = 0`.
    fn command(&self) -> u64 {
        self.group(9, 0, SalMessageKind::Tick, 0)
    }

    /// A committed zone of one PK-partitioned `Push` group per target over `NW`
    /// workers. Returns every member's base.
    fn push_zone(&self, lsn: u64, targets: &[u32]) -> Vec<u64> {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let scope = self.writer.begin(lsn, "test");
        let bases: Vec<u64> = targets
            .iter()
            .map(|&t| self.push_group(t, schema, &batch, |g| scope.write(g, true)))
            .collect();
        assert!(scope.commit(), "the zone was open");
        bases
    }

    /// Flip one bit of the header at `base`.
    fn damage_header(&self, base: u64) {
        unsafe { *self.ptr().add(base as usize + PREFIX_BYTES) ^= 1 };
    }

    /// Zero the header at `base`, leaving its prefix.
    fn zero_header(&self, base: u64, slots: usize) {
        unsafe {
            std::ptr::write_bytes(
                self.ptr().add(base as usize + PREFIX_BYTES),
                0,
                group_header_size(slots),
            )
        };
    }

    /// The publication prefix word at `base`, as mutable bytes.
    fn prefix_bytes(&mut self, base: u64) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr().add(base as usize), PREFIX_BYTES) }
    }

    /// Corrupt one byte of slot `w` of the group at `base`.
    fn damage_slot(&self, base: u64, w: u32) {
        let slot = group_at(self.log(), base).slot(w).expect("slot carries bytes");
        let off = (slot.as_ptr() as usize) - (self.ptr() as usize);
        unsafe { *self.ptr().add(off + slot.len() / 2) ^= 0xFF };
    }

    /// The highest slot of the group at `base` that carries rows.
    fn a_slot_with_rows(&self, base: u64) -> u32 {
        let msg = group_at(self.log(), base);
        (0..NW as u32)
            .rev()
            .find(|&w| {
                msg.slot(w)
                    .is_some_and(|b| crate::runtime::wire::decode_sal_slot(b).unwrap().data_batch.is_some())
            })
            .expect("some slot carries rows")
    }
}
