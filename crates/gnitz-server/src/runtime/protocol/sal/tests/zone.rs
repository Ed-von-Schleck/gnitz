use super::*;
use crate::runtime::sal::fixtures::{group_at, TestLog};
use crate::runtime::sal::{group_header_size, DirectGroup};
use crate::runtime::wire::WireMsg;
use crate::test_support::{make_batch, make_schema_u64_i64, sweep_bit_flips};

const SIZE: usize = 1 << 20;
const NW: usize = 4;
const TID: u32 = 16;

/// The committed set of a quiescent log, sorted — the shape the tests assert on.
fn committed_lsns(log: SalLog, kind: SalMessageKind, families: &HashMap<i64, u64>) -> Result<Vec<u64>, String> {
    CommittedTail::open(log, log.walk_epoch(), kind, families).map(|tail| {
        let mut v: Vec<u64> = tail.committed.into_iter().collect();
        v.sort_unstable();
        v
    })
}

/// The `(lsn, target_id)` pairs a walk reads, and the offsets it reports
/// corrupt.
fn walk(log: SalLog) -> (Vec<(u64, u32)>, Vec<u64>) {
    let mut groups = Vec::new();
    let mut corrupt = Vec::new();
    for step in log.walk_from(0, log.walk_epoch()) {
        match step {
            SalStep::Group(m, _) => groups.push((m.lsn, m.target_id)),
            SalStep::Corrupt(off) => corrupt.push(off),
            SalStep::Absent => unreachable!("the walk stops at the end of the log"),
        }
    }
    (groups, corrupt)
}

fn committed(log: SalLog) -> Result<Vec<u64>, String> {
    committed_lsns(log, SalMessageKind::DdlSync, &HashMap::new())
}

// -----------------------------------------------------------------------
// The publication prefix
// -----------------------------------------------------------------------

/// No bit of the prefix word changes a walk: the stride comes from the
/// digested directory and the generation from the header's own epoch.
#[test]
fn the_whole_prefix_word_is_neutralised() {
    let mut log = TestLog::new(SIZE, 1, 3);
    log.group(11, 101, SalMessageKind::DdlSync, false);
    let middle = log.group(22, 102, SalMessageKind::DdlSync, false);
    log.group(33, 103, SalMessageKind::DdlSync, false);

    let view = log.log();
    let clean = walk(view);
    assert_eq!(clean.0, vec![(101, 11), (102, 22), (103, 33)]);
    assert!(clean.1.is_empty());

    sweep_bit_flips(log.prefix_bytes(middle), 0..PREFIX_BYTES, |byte, bit, _| {
        assert_eq!(walk(view), clean, "prefix byte {byte} bit {bit} changed the walk");
    });
}

/// Presence is the whole prefix word, not its `payload_size` half: a small
/// `payload_size` is zeroable by a bit flip, so a low-half-only test would let
/// one flip stop the walk before that transaction's sentinel.
#[test]
fn a_sentinels_prefix_is_not_single_bit_zeroable() {
    let mut log = TestLog::new(SIZE, 1, 1);
    let bases = log.zone(7, &[11]);
    let sentinel = *bases.last().unwrap();
    log.group(33, 0, SalMessageKind::Tick, false);
    // A 2-slot empty group, whose payload is 64, a single set bit.
    let empty2 = log
        .try_write(44, 0, SalMessageKind::Tick, false, &[&[], &[]])
        .expect("group fits");

    let view = log.log();
    let clean = walk(view);
    assert_eq!(clean.0.len(), 4, "zone group, sentinel, tick, 2-slot empty group");
    assert_eq!(committed(view).unwrap(), vec![7]);

    for &base in &[sentinel, empty2] {
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
// The walk epoch
// -----------------------------------------------------------------------

/// A previous epoch's leftover past the new frontier is absent, not damage —
/// the ring wrap is an ordinary end-of-log.
#[test]
fn a_previous_epochs_leftover_ends_the_walk() {
    let log = TestLog::new(SIZE, 1, 1);
    log.group(11, 1, SalMessageKind::DdlSync, false);
    log.group(22, 2, SalMessageKind::DdlSync, false);
    log.group(33, 3, SalMessageKind::DdlSync, false);
    // A shorter epoch-2 log over the same bytes: one group, so epoch 1's
    // second and third groups survive past the new frontier.
    log.seek(0, 2);
    log.group(44, 9, SalMessageKind::DdlSync, false);

    let view = log.log();
    assert_eq!(view.walk_epoch(), 2, "offset 0's header anchors the walk");
    let (groups, corrupt) = walk(view);
    assert_eq!(groups, vec![(9, 44)], "the walk ends at the epoch-1 leftover");
    assert!(corrupt.is_empty(), "a leftover is absent, not corrupt");
}

/// The walk epoch comes from group 0's digested header, not its prefix copy,
/// so no single flip there can make every later comparison fail.
#[test]
fn group_zeros_prefix_epoch_is_not_a_single_point_of_failure() {
    let mut log = TestLog::new(SIZE, 1, 5);
    log.group(11, 101, SalMessageKind::DdlSync, false);
    log.group(22, 102, SalMessageKind::DdlSync, false);
    let view = log.log();
    let clean = walk(view);
    assert_eq!(clean.0.len(), 2);

    // The high half of the prefix word is its epoch copy.
    sweep_bit_flips(log.prefix_bytes(0), 4..PREFIX_BYTES, |byte, bit, _| {
        assert_eq!(view.walk_epoch(), 5, "epoch byte {byte} bit {bit} moved the walk epoch");
        assert_eq!(walk(view), clean, "epoch byte {byte} bit {bit} changed the walk");
    });
}

/// With offset 0's header damaged, the anchor is the *maximum* epoch in the
/// ring — not the first valid header found, which under a page revert can be
/// an older leftover at a low offset.
#[test]
fn the_walk_epoch_survives_a_damaged_offset_zero() {
    // A previous epoch's leftover further in, left behind by a shorter later
    // pass, plus the live epoch-4 log over the front.
    let log = TestLog::new(SIZE, 1, 2);
    for i in 0..8 {
        log.group(90 + i, 1, SalMessageKind::DdlSync, false);
    }
    log.seek(0, 4);
    log.group(11, 101, SalMessageKind::DdlSync, false);
    log.group(22, 102, SalMessageKind::DdlSync, false);
    log.group(33, 103, SalMessageKind::DdlSync, false);
    log.damage_header(0);

    let view = log.log();
    assert_eq!(view.walk_epoch(), 4, "the maximum epoch in the ring, not a leftover's");
    let (groups, corrupt) = walk(view);
    assert_eq!(corrupt, vec![0], "offset 0 is the damage");
    assert_eq!(
        groups,
        vec![(102, 22), (103, 33)],
        "the resync must recover every committed group behind the damage"
    );
}

/// Only a damaged offset 0 selects the ring-wide sweep. A fresh ring and a
/// reset ring must both answer from offset 0 alone — otherwise every boot of
/// every fresh database pays a full-mapping hash sweep.
#[test]
fn only_a_damaged_offset_zero_takes_a_sweep() {
    // A fresh all-zero mapping: no header, zero prefix, epoch floor 0.
    assert_eq!(TestLog::new(SIZE, 1, 0).log().walk_epoch(), 0);

    // A reset ring: `rewind` zeroes offset 0's prefix but leaves its header,
    // so the probe answers and the floor carries forward.
    let log = TestLog::new(SIZE, 1, 7);
    log.group(11, 1, SalMessageKind::DdlSync, false);
    log.writer.rewind(8);
    assert_eq!(
        log.log().walk_epoch(),
        7,
        "a reset ring's floor comes from the surviving header, not the zeroed prefix"
    );
    assert_eq!(log.writer.epoch(), 8, "the next writer epoch is one above the floor");

    // A damaged offset 0 with a non-zero prefix is the one shape that sweeps.
    let log = TestLog::new(SIZE, 1, 6);
    log.group(11, 1, SalMessageKind::DdlSync, false);
    log.group(22, 2, SalMessageKind::DdlSync, false);
    log.damage_header(0);
    assert_eq!(log.log().walk_epoch(), 6);
}

// -----------------------------------------------------------------------
// The zone-span rule
// -----------------------------------------------------------------------

/// (a) Damage inside a middle committed zone is a hole: a later committed
/// zone is durable behind it, so the boot must fail naming the offset.
#[test]
fn damage_in_a_middle_zone_fails_the_boot() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11, 12]);
    let second = log.zone(2, &[21, 22]);
    log.zone(3, &[31, 32]);
    log.damage_header(second[1]);

    let err = committed(log.log()).expect_err("a hole must fail the boot");
    assert!(
        err.contains(&format!("offset={}", second[1])),
        "the error names the offset: {err}"
    );
    assert!(err.contains("lsn=2"), "and the zone: {err}");
}

/// (b) Damage inside the last zone demotes it; every earlier zone still
/// applies.
#[test]
fn damage_in_the_last_zone_demotes_it() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11, 12]);
    let last = log.zone(2, &[21, 22]);
    log.damage_header(last[1]);

    assert_eq!(committed(log.log()).unwrap(), vec![1]);
}

/// (c) Damage in an `lsn = 0` command group between two zones costs nothing,
/// and the committer fires one of these ticks after every push.
#[test]
fn damage_in_a_command_group_between_zones_costs_nothing() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    let tick = log.command();
    log.zone(2, &[21]);
    log.zone(3, &[31]);
    log.damage_header(tick);

    assert_eq!(
        committed(log.log()).unwrap(),
        vec![1, 2, 3],
        "every committed zone must survive rot in a tick group"
    );
}

/// (d) Damage in an unclosed tail zone costs nothing — the zone was never
/// promised to anyone.
#[test]
fn damage_in_an_unclosed_tail_zone_costs_nothing() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    log.group(21, 2, SalMessageKind::DdlSync, true);
    let torn = log.group(22, 2, SalMessageKind::DdlSync, false);
    log.damage_header(torn);

    assert_eq!(committed(log.log()).unwrap(), vec![1]);
}

/// A stream-only commit batch writes its `Push` groups with no zone start
/// and no sentinel, so damage in one costs nothing however many precede a
/// committed zone. This is why a stream batch opens no zone at all: pass 1 is
/// built to a budget of at most one un-fsynced zone, and a run of them could
/// refuse a boot over damage to a zone no client was ever promised.
#[test]
fn damage_in_zone_less_stream_batches_costs_nothing() {
    let log = TestLog::new(SIZE, 1, 1);
    let first = log.group(41, 7, SalMessageKind::Push, false);
    let second = log.group(42, 8, SalMessageKind::Push, false);
    log.zone(9, &[11]);
    log.damage_header(first);
    log.damage_header(second);

    assert_eq!(
        committed(log.log()).unwrap(),
        vec![9],
        "only the closed zone commits, and the damaged stream groups do not fail the boot"
    );
}

/// (e) A sentinel arriving with no zone open lost that zone's first group;
/// applying its siblings would half-apply an atomic unit.
#[test]
fn a_zone_that_lost_its_first_group_fails_the_boot() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    let second = log.zone(2, &[21, 22]);
    log.zone(3, &[31]);
    log.damage_header(second[0]);

    let err = committed(log.log()).expect_err("a lost head group must fail the boot");
    assert!(err.contains("lsn=2"), "{err}");
}

/// A zone that lost **both** its start group and its sentinel, with a middle
/// group surviving. Pass 1 sees no damage and the boot proceeds, so the only
/// thing keeping the survivor out of the replay is exact membership in the
/// committed set — which is why that set must never become a high-water mark.
#[test]
fn a_surviving_group_of_a_headless_zone_is_not_replayed() {
    let log = TestLog::new(SIZE, 1, 1);
    let first = log.group(21, 2, SalMessageKind::DdlSync, true);
    let middle = log.group(22, 2, SalMessageKind::DdlSync, false);
    let sentinel = log.sentinel(2);
    log.zone(3, &[31]);
    log.damage_header(first);
    log.damage_header(sentinel);

    let view = log.log();
    let (groups, corrupt) = walk(view);
    assert_eq!(corrupt, vec![first, sentinel], "only the zone's ends are damaged");
    assert!(
        groups.contains(&(2, 22)),
        "the middle group survives the walk: {groups:?}"
    );
    assert_eq!(
        committed(view).unwrap(),
        vec![3],
        "zone 2 never commits, so its survivor must not replay"
    );
    let fams = families(&[21, 22, 31]);
    let tail = CommittedTail::open(view, view.walk_epoch(), SalMessageKind::DdlSync, &fams).unwrap();
    assert_eq!(
        tail.groups().map(|m| (m.lsn, m.target_id)).collect::<Vec<_>>(),
        vec![(3, 31)],
        "pass 2 must skip the surviving group at offset {middle}"
    );
}

/// A zone with a readable start, no sentinel, and a group after it did close:
/// its sentinel was destroyed, not omitted.
#[test]
fn a_lost_sentinel_is_not_a_zone_that_never_closed() {
    let log = TestLog::new(SIZE, 1, 1);
    let first = log.zone(1, &[11]);
    log.zone(2, &[21]);
    // Destroy the first zone's sentinel.
    log.damage_header(*first.last().unwrap());

    let err = committed(log.log()).expect_err("a destroyed sentinel must fail the boot");
    assert!(err.contains("lost its commit sentinel"), "{err}");
}

// -----------------------------------------------------------------------
// The resync scan
// -----------------------------------------------------------------------

/// The resync scan skips what the walk stops on: zero words, and an intact
/// previous-epoch header below the frontier (the page-revert shape). Both must
/// still reach the zone's sentinel and report the hole.
#[test]
fn the_resync_scan_sweeps_past_zero_runs_and_leftovers() {
    for leftover in [false, true] {
        let log = TestLog::new(SIZE, 1, 1);
        if leftover {
            // A previous epoch's group parked where the live log will straddle
            // it, so the scan meets an intact epoch-1 header below the frontier.
            for _ in 0..40 {
                log.group(90, 1, SalMessageKind::DdlSync, false);
            }
        }
        log.seek(0, 4);
        log.zone(1, &[11]);
        let second_start = log.group(21, 2, SalMessageKind::DdlSync, true);
        log.group(22, 2, SalMessageKind::DdlSync, false);
        log.sentinel(2);
        log.zone(3, &[31]);
        // Destroy the zone's first group's header entirely, leaving its prefix:
        // the walk must resync across whatever follows.
        log.zero_header(second_start, 1);

        let err = committed(log.log()).expect_err("the hole must be reported (leftover={leftover})");
        assert!(err.contains("lsn=2"), "leftover={leftover}: {err}");
    }
}

/// A group's prefix and header can fall on different pages, and unordered
/// writeback can persist the prefix and lose the header. Without the resync
/// scan that would be terminal and throw the committed prefix away.
#[test]
fn a_torn_header_page_does_not_cost_the_committed_prefix() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    log.zone(2, &[21]);
    // An unclosed tail zone whose head group's header was lost.
    let torn = log.group(31, 3, SalMessageKind::DdlSync, true);
    log.zero_header(torn, 1);

    assert_eq!(
        committed(log.log()).unwrap(),
        vec![1, 2],
        "every committed zone before the torn page must survive"
    );
}

/// Both passes run the same walk, so a zone pass 1 commits is a zone pass 2
/// reaches. A pass 2 that stopped at the damage instead would drop zone 2's
/// groups on the floor while reporting it committed — an ACKed transaction
/// lost in silence.
#[test]
fn both_passes_see_the_same_groups_past_damage() {
    let log = TestLog::new(SIZE, 1, 1);
    log.zone(1, &[11]);
    let tick = log.command();
    log.zone(2, &[21]);
    log.damage_header(tick);

    let view = log.log();
    let (groups, corrupt) = walk(view);
    assert_eq!(corrupt, vec![tick]);
    assert_eq!(
        groups,
        vec![(1, 11), (1, 0), (2, 21), (2, 0)],
        "zone 1 + its sentinel, then zone 2 + its sentinel; the tick is the damage"
    );
    assert_eq!(committed(view).unwrap(), vec![1, 2]);
}

fn families(targets: &[u32]) -> HashMap<i64, u64> {
    targets.iter().map(|&t| (t as i64, 0u64)).collect()
}

/// The zone-protocol shapes over the shared log fixture: a closed zone, the
/// command groups between them, and the damage a recovery walk has to survive.
impl TestLog {
    /// One group with a 64-byte slot, laid down below the writer's zone state
    /// with the zone-start byte spelled here. Returns its base.
    fn group(&self, target: u32, lsn: u64, kind: SalMessageKind, zone_start: bool) -> u64 {
        self.try_write(target, lsn, kind, zone_start, &[&[0u8; 64]])
            .expect("group fits")
    }

    /// A closed zone through the production writer: one control-only `DdlSync`
    /// group per entry of `targets` inside one [`SalScope`], then the commit
    /// sentinel its `commit` writes. Returns every base, the sentinel's last.
    fn zone(&self, lsn: u64, targets: &[u32]) -> Vec<u64> {
        let scope = self.writer.begin(lsn, "test");
        let mut bases: Vec<u64> = targets
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
        bases.push(self.cursor());
        assert!(scope.commit().expect("sentinel fits"), "the zone was open");
        bases
    }

    /// The ephemeral command group the committer fires between zones: `lsn = 0`,
    /// in no zone's span.
    fn command(&self) -> u64 {
        self.group(9, 0, SalMessageKind::Tick, false)
    }

    /// A push zone over `NW` workers: one scatter group per entry of `targets`
    /// (the first opening the zone), then the commit sentinel. Rows are
    /// PK-partitioned, so most slots are `ctrl + schema` with no data block —
    /// exactly the shape a partitioned push leaves. Returns each group's base.
    fn push_zone(&self, lsn: u64, targets: &[u32]) -> Vec<u64> {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let scope = self.writer.begin(lsn, "test");
        let bases: Vec<u64> = targets
            .iter()
            .map(|&t| self.push_group(lsn, t, schema, &batch, |g| scope.write(g, true)))
            .collect();
        assert!(scope.commit().expect("sentinel fits"), "the zone was open");
        bases
    }

    /// Flip one bit of the header at `base`.
    fn damage_header(&self, base: u64) {
        unsafe { *self.ptr().add(base as usize + PREFIX_BYTES) ^= 1 };
    }

    /// The state unordered mmap writeback leaves when a group's prefix and
    /// header fall on different pages: the prefix persists, the header does not.
    fn zero_header(&self, base: u64, slots: usize) {
        unsafe {
            std::ptr::write_bytes(
                self.ptr().add(base as usize + PREFIX_BYTES),
                0,
                group_header_size(slots),
            )
        };
    }

    /// The publication prefix word at `base`, as a mutable byte slice. The
    /// mapping is shared, so a reader built from the same region sees the edit.
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
            .find(|&w| msg.slot(w).is_some())
            .expect("some slot carries rows")
    }
}

/// A push whose rows do not reach every worker leaves the remaining slots a
/// control block and a schema block with no data block. Pass 1 must read that
/// as "decodes", not as "carries rows" — otherwise every partitioned push
/// demotes its own zone.
#[test]
fn a_row_less_slot_is_not_a_damaged_one() {
    let log = TestLog::new(SIZE, NW, 1);
    let bases = log.push_zone(5, &[TID]);

    // The fixture must actually leave a row-less slot, or it proves nothing.
    let msg = group_at(log.log(), bases[0]);
    let row_less = (0..NW as u32)
        .filter(|&w| {
            let slot = msg.slot(w).expect("every slot is written");
            assert!(msg.slot_intact(w, slot), "slot {w} verifies");
            ipc::decode_sal_slot(slot).expect("slot decodes").data_batch.is_none()
        })
        .count();
    assert!(row_less > 0, "the fixture must leave at least one slot row-less");

    assert_eq!(
        committed_lsns(log.log(), SalMessageKind::Push, &families(&[TID])).unwrap(),
        vec![5]
    );
}

/// The demotion verdict is global: rot in worker 3's slot must demote the
/// zone on every walk, worker 0's included. A per-slot verdict would apply the
/// zone on three workers and skip it on one.
#[test]
fn the_demotion_is_global_across_slots() {
    for damage_offset_zero in [false, true] {
        let log = TestLog::new(SIZE, NW, 1);
        // A leading command group, so offset 0 is not the zone's own head and
        // can be damaged independently: with it gone there is no tail-wide slot
        // count to read, and only each group's own count is available.
        log.command();
        let bases = log.push_zone(5, &[TID]);

        // Rot the highest slot carrying rows.
        let victim = log.a_slot_with_rows(bases[0]);
        log.damage_slot(bases[0], victim);
        if damage_offset_zero {
            log.damage_header(0);
        }

        assert!(
            committed_lsns(log.log(), SalMessageKind::Push, &families(&[TID]))
                .unwrap()
                .is_empty(),
            "rot in slot {victim} must demote the zone whichever slot a walk looks at \
             (offset0_damaged={damage_offset_zero})"
        );
    }
}

/// A group whose family is absent from the map is not validated, so damage in
/// one cannot demote the zone it sits in.
///
/// This is the shape a stream takes: `user_flushed_lsns` omits storeless
/// relations, and the committer coalesces a stream push into a base table's
/// commit batch — base group first (it opens the zone; a stream group never
/// does), stream group second, inside the span. With the stream present at LSN
/// 0 instead, a torn stream slot would discard the fdatasync'd base push beside
/// it. The rule is general: pass 1 validates exactly what pass 2 can apply.
#[test]
fn damage_in_an_unmapped_family_does_not_demote_the_zone() {
    const STREAM_TID: u32 = TID + 1;
    let log = TestLog::new(SIZE, NW, 1);
    let bases = log.push_zone(5, &[TID, STREAM_TID]);
    log.damage_slot(bases[1], log.a_slot_with_rows(bases[1]));

    assert_eq!(
        committed_lsns(log.log(), SalMessageKind::Push, &families(&[TID])).unwrap(),
        vec![5],
        "the base push must still replay"
    );
    // The same damage in a mapped family does demote it, so the assertion above
    // is about the map and not about the fixture failing to damage anything.
    assert!(
        committed_lsns(log.log(), SalMessageKind::Push, &families(&[TID, STREAM_TID]))
            .unwrap()
            .is_empty()
    );
}

/// A torn last zone is skipped whole, not partially: pass 1 drops the LSN, so
/// pass 2 never applies one family's group while skipping its sibling's.
#[test]
fn a_torn_last_zone_is_skipped_whole() {
    let log = TestLog::new(SIZE, NW, 1);
    log.push_zone(4, &[TID]);
    let last = log.push_zone(5, &[TID, TID + 1]);
    // Rot the FIRST family of the last zone.
    log.damage_slot(last[0], log.a_slot_with_rows(last[0]));

    assert_eq!(
        committed_lsns(log.log(), SalMessageKind::Push, &families(&[TID, TID + 1])).unwrap(),
        vec![4],
        "the torn zone must be dropped whole, and the durable one before it kept"
    );
}
