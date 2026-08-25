//! The SAL **zone protocol**: the atomic unit recovery commits, both directions.
//!
//! A zone's byte span runs from a `FLAG_ZONE_START` group to the
//! `FLAG_TXN_COMMIT` sentinel [`SalWriter::write_commit_sentinel`] closes it
//! with, and its groups apply all-or-nothing. A whole CREATE is one zone — its N
//! families ride one `FLAG_DDL_TXN` bundle under a single zone LSN — so all of
//! COL_TAB and TABLE_TAB replay, or none of it.
//!
//! Recovery reads it in two passes over the same walk, both behind
//! [`CommittedTail`]. Neither knows what a group *means*: the master (pre-fork,
//! system families) and each worker (post-fork, user families) vary only which
//! groups are theirs, and what they do with the bytes.
//!
//! A child of `sal` rather than a peer, so the walk reads the mmap through
//! `SalReader::read_at` / `valid_headers_from` without either becoming part of
//! the format module's surface.

use std::collections::{HashMap, HashSet};

use super::{EpochGate, SalMessage, SalReader, SalStep, SalWriter, FLAG_DDL_SYNC, FLAG_TXN_COMMIT, FLAG_ZONE_START};
use crate::runtime::wire as ipc;

impl SalWriter {
    /// Write an empty commit sentinel for an atomic zone.
    ///
    /// A slotless group carrying `FLAG_DDL_SYNC | FLAG_TXN_COMMIT`. Recovery uses
    /// the sentinel as the "this LSN is closed" mark — without it, all groups at
    /// this LSN are skipped. Every worker still sees it (the flags live in the
    /// group header, which is not per-slot) and it is inert under the worker's hot
    /// path: the FLAG_DDL_SYNC branch no-ops on a group with no batch.
    pub fn write_commit_sentinel(&self, lsn: u64) -> Result<(), String> {
        let group = self.begin("write_commit_sentinel", 0, lsn, FLAG_DDL_SYNC | FLAG_TXN_COMMIT, &[])?;
        self.finish(group);
        Ok(())
    }
}

/// One step of the recovery walk: a group that was published at an offset, or an
/// offset whose bytes were published but do not verify.
enum WalkStep {
    Group(SalMessage<'static>),
    Corrupt(u64),
}

impl SalReader {
    /// Walk the SAL from offset 0 against the walk epoch `E`, resyncing past damage
    /// to the next candidate valid at `E` so that one bad offset does not discard the
    /// committed groups behind it. A zero prefix or a header at another epoch ends
    /// the walk — the ring's leftovers begin there.
    ///
    /// Both passes walk identically, so they cannot disagree about where the log
    /// ends. Successive resyncs examine disjoint increasing ranges, so a walk sweeps
    /// the mapping at most once in total.
    ///
    /// The reader's own `worker_id` never reaches the walk's control flow: group
    /// headers are slot-independent, so any reader yields the same steps.
    fn walk(&self, epoch: u32) -> impl Iterator<Item = WalkStep> + '_ {
        let mut offset: u64 = 0;
        std::iter::from_fn(move || match self.read_at(offset, EpochGate::Walk(epoch)) {
            SalStep::Group(msg, next) => {
                offset = next;
                Some(WalkStep::Group(msg))
            }
            SalStep::Corrupt => {
                let corrupt_at = offset;
                offset = self
                    .valid_headers_from(offset + 8)
                    .find(|&(_, e)| e == epoch)
                    .map_or(self.mmap_size(), |(base, _)| base);
                Some(WalkStep::Corrupt(corrupt_at))
            }
            SalStep::Absent | SalStep::OtherEpoch => None,
        })
    }
}

/// The groups of one zone, as pass 1 accumulates them.
#[derive(Default)]
struct Zone {
    lsn: u64,
    /// The `FLAG_ZONE_START` group's offset — the start of the zone's byte span.
    start: u64,
    /// The first corrupt offset inside the span, if any.
    damage: Option<u64>,
    /// Every non-sentinel group in the span.
    groups: Vec<ZoneGroup>,
}

/// As much of a group's header as pass 1 keeps: enough to re-find its slots and
/// to ask whether this walk applies it.
struct ZoneGroup {
    base: u64,
    flags: u32,
    target_id: u32,
    slots: u32,
}

/// The committed part of the un-checkpointed tail. Constructing it is pass 1;
/// iterating it is pass 2.
///
/// Both ask [`applies`](Self::applies), so pass 1 cannot demote a zone over a
/// group pass 2 would have skipped.
pub(crate) struct CommittedTail<'a> {
    reader: &'a SalReader,
    epoch: u32,
    /// A group is this walk's iff it carries one of these flags…
    flags: u32,
    /// …and its zone LSN is past what its family already has on disk. A family
    /// absent from the map has no store to recover into.
    family_lsns: &'a HashMap<i64, u64>,
    committed: HashSet<u64>,
}

impl<'a> CommittedTail<'a> {
    /// Pass 1 over the walk at `epoch`, scoped to `flags` and `family_lsns`.
    pub(crate) fn open(
        reader: &'a SalReader,
        epoch: u32,
        flags: u32,
        family_lsns: &'a HashMap<i64, u64>,
    ) -> Result<Self, String> {
        let mut tail = CommittedTail {
            reader,
            epoch,
            flags,
            family_lsns,
            committed: HashSet::new(),
        };
        tail.committed = tail.collect_committed_lsns()?;
        Ok(tail)
    }

    fn applies(&self, msg_flags: u32, target_id: u32, lsn: u64) -> bool {
        msg_flags & self.flags != 0 && self.family_lsns.get(&(target_id as i64)).is_some_and(|&f| lsn > f)
    }

    /// Pass 2: this walk's groups from every committed zone, in log order.
    ///
    /// Yielded whole rather than as bytes — a worker re-reads one group across
    /// several slots through [`SalReader::slot_at`], which needs `msg.base` — and
    /// undecoded, so the caller's own per-slot skips run before the copy.
    pub(crate) fn groups(&self) -> impl Iterator<Item = SalMessage<'static>> + '_ {
        self.reader.walk(self.epoch).filter_map(move |step| {
            // Pass 1 already reached its verdict on every corrupt offset: it either
            // failed the boot or established that nothing committed lies there.
            let WalkStep::Group(msg) = step else { return None };
            let keep = self.committed.contains(&msg.lsn) && self.applies(msg.flags, msg.target_id, msg.lsn);
            keep.then_some(msg)
        })
    }

    /// Pass 1: the LSNs whose zone is both closed and intact.
    ///
    /// A zone's byte span runs from its `FLAG_ZONE_START` group to its
    /// `FLAG_TXN_COMMIT` sentinel, and nothing else lives in it — a zone is written
    /// with no suspension point inside it, so no other writer can place a group
    /// there. Damage costs that zone a group, and the verdict follows from where it
    /// sits:
    ///
    /// * inside any zone but the last — `Err`, naming the offset. A later committed
    ///   zone is durable behind it, so this is a hole in the log rather than a torn
    ///   tail, and the boot must not proceed past it.
    /// * inside the **last** zone — demote it. A sentinel on disk is not an ACK, so
    ///   failing the boot there would brick it on a transaction nobody was promised.
    /// * outside every zone span — an `lsn = 0` command group, or a zone that never
    ///   closed. Nothing was promised; the walk resyncs and the boot proceeds.
    ///
    /// Two more shapes are fatal for the same reason: a sentinel that arrives with no
    /// zone open lost its zone's *first* group, and a zone with a group after it but
    /// no sentinel lost its *sentinel* (an unclosed zone can only be the last thing
    /// in the log).
    fn collect_committed_lsns(&self) -> Result<HashSet<u64>, String> {
        let mut committed: HashSet<u64> = HashSet::new();
        let mut open: Option<Zone> = None;
        // A damaged zone that did close. Fatal once a later sentinel proves a
        // committed zone is durable behind it; discarded if it was the log's last.
        let mut torn: Option<String> = None;
        let mut last_closed: Option<Zone> = None;

        for step in self.reader.walk(self.epoch) {
            let msg = match step {
                WalkStep::Corrupt(off) => {
                    if let Some(z) = open.as_mut() {
                        z.damage.get_or_insert(off);
                    }
                    continue;
                }
                WalkStep::Group(msg) => msg,
            };
            let mine = open.as_ref().is_some_and(|z| z.lsn == msg.lsn);
            if msg.flags & FLAG_TXN_COMMIT != 0 {
                if let Some(e) = torn.take() {
                    return Err(e);
                }
                // A sentinel with no zone open lost the zone's first group; one that
                // closes a *different* zone lost that zone's sentinel as well.
                let Some(zone) = open.take().filter(|_| mine) else {
                    return Err(format!(
                        "SAL replay: committed zone lsn={} lost a group before its commit sentinel at \
                         offset={}; its FLAG_ZONE_START group is not in the log",
                        msg.lsn, msg.base
                    ));
                };
                match zone.damage {
                    None => {
                        committed.insert(zone.lsn);
                    }
                    Some(at) => {
                        torn = Some(format!(
                            "SAL replay: committed zone lsn={} lost a group at offset={at} (zone ends \
                             at offset={}); a later committed zone is durable behind it, so this is a \
                             hole in the log rather than a torn tail",
                            zone.lsn, msg.base
                        ))
                    }
                }
                last_closed = Some(zone);
                continue;
            }
            // A group arriving while another zone is open means that zone did close
            // and its sentinel was destroyed.
            if !mine {
                if let Some(z) = open.take() {
                    return Err(format!(
                        "SAL replay: committed zone lsn={} at offset={} lost its commit sentinel (a \
                         group at offset={} follows it, so the zone did close)",
                        z.lsn, z.start, msg.base
                    ));
                }
            }
            if msg.flags & FLAG_ZONE_START != 0 {
                open = Some(Zone {
                    lsn: msg.lsn,
                    start: msg.base,
                    ..Zone::default()
                });
            }
            // Only groups behind an open zone start are the zone's. A non-zero `lsn`
            // alone does not make one: the ephemeral flush round carries the
            // checkpoint generation there and belongs to no zone.
            if let Some(z) = open.as_mut() {
                z.groups.push(ZoneGroup {
                    base: msg.base,
                    flags: msg.flags,
                    target_id: msg.target_id,
                    slots: msg.slots,
                });
            }
        }

        if let Some(zone) = last_closed {
            if committed.contains(&zone.lsn) && !self.zone_blocks_decode(&zone) {
                committed.remove(&zone.lsn);
            }
        }
        Ok(committed)
    }

    /// Whether every block of `zone` that [`groups`](Self::groups) would yield
    /// decodes — the last surviving zone's, which pass 1 demotes if any does not.
    ///
    /// Across every slot the group declares, not only this reader's, so worker 3
    /// cannot demote a zone worker 0 applies. (Under a worker-count change pass 2
    /// reads only slot 0 of a replicated group, so validating the rest can still
    /// demote a zone over bytes nobody reads — reachable only on a crash boot with a
    /// torn non-zero replicated slot, and harmless at the launched count, where every
    /// worker does need its own slot.)
    ///
    /// "Decodes" is not "carries rows": a push whose rows all land on one worker
    /// leaves the other slots a control block and a schema block with no data block,
    /// which is correctly a no-op on replay.
    fn zone_blocks_decode(&self, zone: &Zone) -> bool {
        let lsn = zone.lsn;
        for g in &zone.groups {
            if !self.applies(g.flags, g.target_id, lsn) {
                continue;
            }
            for w in 0..g.slots {
                if let Some(bytes) = self.reader.slot_at(g.base, w) {
                    if let Err(e) = ipc::decode_wire(bytes) {
                        gnitz_warn!(
                            "SAL replay: last committed zone lsn={lsn} is torn (offset={} slot={w} \
                             target={}: {e}); skipping it whole",
                            g.base,
                            g.target_id
                        );
                        return false;
                    }
                }
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Walk, resync, epoch and zone-span rules over hand-built logs: the group-header
// shapes are what recovery classifies, and the payloads are opaque (no family
// map), so the zone rules are isolated from block decoding. Below them, the same
// predicate over real wire slots, where "validate" means the block decodes and
// not that it carries rows. The end-to-end crash path is covered in
// `crates/gnitz-py/tests/test_crash_recovery.py`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::master::scatter::with_commit_indices;
    use crate::runtime::sal::{group_header_size, sal_write_group, FLAG_PUSH, FLAG_TICK};
    use crate::runtime::wire::build_schema_wire_block;
    use gnitz_engine::storage::compute_wire_props;
    use gnitz_engine_testkit::{make_batch, make_schema_u64_i64, sweep_bit_flips, SharedRegion};

    const SIZE: usize = 1 << 20;
    const NW: usize = 4;
    const TID: u32 = 16;

    /// The committed set of a quiescent log, sorted — the shape the tests assert on.
    fn committed_lsns(reader: &SalReader, flags: u32, families: &HashMap<i64, u64>) -> Result<Vec<u64>, String> {
        CommittedTail::open(reader, reader.walk_epoch(), flags, families).map(|tail| {
            let mut v: Vec<u64> = tail.committed.into_iter().collect();
            v.sort_unstable();
            v
        })
    }
    /// A hand-built SAL: groups appended at a cursor the embedded `SalWriter`
    /// tracks, so the commit sentinels come from the production writer.
    struct Log {
        ptr: *mut u8,
        writer: SalWriter,
        epoch: u32,
    }

    impl Log {
        fn new(region: &SharedRegion, epoch: u32) -> Log {
            let ptr = region.ptr();
            let writer = SalWriter::new(ptr, -1, SIZE as u64, 1);
            writer.reset(0, epoch);
            Log { ptr, writer, epoch }
        }

        /// One ordinary group with a 64-byte slot per worker. Returns its base.
        fn group(&self, target: u32, lsn: u64, flags: u32) -> u64 {
            let base = self.writer.cursor();
            let payload = [0u8; 64];
            let next =
                unsafe { sal_write_group(self.ptr, base, target, lsn, flags, self.epoch, SIZE as u64, &[&payload]) }
                    .expect("group fits");
            self.writer.reset(next, self.epoch);
            base
        }

        /// A closed zone: one group per entry of `targets`, the first carrying
        /// `FLAG_ZONE_START`, then the commit sentinel. Returns every base, the
        /// sentinel's last.
        fn zone(&self, lsn: u64, targets: &[u32]) -> Vec<u64> {
            let mut bases: Vec<u64> = targets
                .iter()
                .enumerate()
                .map(|(i, &t)| self.group(t, lsn, FLAG_DDL_SYNC | if i == 0 { FLAG_ZONE_START } else { 0 }))
                .collect();
            bases.push(self.writer.cursor());
            self.writer.write_commit_sentinel(lsn).expect("sentinel fits");
            bases
        }

        /// The ephemeral command group the committer fires between zones: `lsn = 0`,
        /// in no zone's span.
        fn command(&self) -> u64 {
            self.group(9, 0, FLAG_TICK)
        }

        fn reader(&self) -> SalReader {
            SalReader::for_walk(self.ptr as *const u8, 0, SIZE)
        }

        /// Flip one bit of the header at `base`.
        fn damage_header(&self, base: u64) {
            unsafe { *self.ptr.add(base as usize + 8) ^= 1 };
        }

        /// The state unordered mmap writeback leaves when a group's prefix and
        /// header fall on different pages: the prefix persists, the header does not.
        fn zero_header(&self, base: u64, slots: usize) {
            unsafe { std::ptr::write_bytes(self.ptr.add(base as usize + 8), 0, group_header_size(slots)) };
        }

        /// The publication prefix word at `base`, as a mutable byte slice. The
        /// mapping is shared, so a reader built from the same region sees the edit.
        fn prefix_bytes(&mut self, base: u64) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr.add(base as usize), 8) }
        }
    }

    /// The `(lsn, target_id)` pairs a walk reads, and the offsets it reports
    /// corrupt.
    fn walk(reader: &SalReader) -> (Vec<(u64, u32)>, Vec<u64>) {
        let mut groups = Vec::new();
        let mut corrupt = Vec::new();
        for step in reader.walk(reader.walk_epoch()) {
            match step {
                WalkStep::Group(m) => groups.push((m.lsn, m.target_id)),
                WalkStep::Corrupt(off) => corrupt.push(off),
            }
        }
        (groups, corrupt)
    }

    fn committed(reader: &SalReader) -> Result<Vec<u64>, String> {
        committed_lsns(reader, FLAG_DDL_SYNC, &HashMap::new())
    }

    // -----------------------------------------------------------------------
    // The publication prefix
    // -----------------------------------------------------------------------

    /// No bit of the prefix word changes a walk: the stride comes from the
    /// digested directory and the generation from the header's own epoch.
    #[test]
    fn the_whole_prefix_word_is_neutralised() {
        let region = SharedRegion::new(SIZE);
        let mut log = Log::new(&region, 3);
        log.group(11, 101, FLAG_DDL_SYNC);
        let middle = log.group(22, 102, FLAG_DDL_SYNC);
        log.group(33, 103, FLAG_DDL_SYNC);

        let reader = log.reader();
        let clean = walk(&reader);
        assert_eq!(clean.0, vec![(101, 11), (102, 22), (103, 33)]);
        assert!(clean.1.is_empty());

        sweep_bit_flips(log.prefix_bytes(middle), 0..8, |byte, bit, _| {
            assert_eq!(walk(&reader), clean, "prefix byte {byte} bit {bit} changed the walk");
        });
    }

    /// Presence is the whole prefix word, not its `payload_size` half. A commit
    /// sentinel's payload is one set bit, so a low-half-only test would let a
    /// single flip stop the walk before that transaction's sentinel.
    #[test]
    fn a_sentinels_prefix_is_not_single_bit_zeroable() {
        let region = SharedRegion::new(SIZE);
        let mut log = Log::new(&region, 1);
        let bases = log.zone(7, &[11]);
        let sentinel = *bases.last().unwrap();
        log.group(33, 0, FLAG_TICK);
        // A 4-slot empty group, whose payload is 64 — also one bit.
        let empty4 = log.writer.cursor();
        unsafe {
            sal_write_group(log.ptr, empty4, 44, 0, FLAG_TICK, 1, SIZE as u64, &[&[], &[], &[], &[]])
                .expect("group fits")
        };

        let reader = log.reader();
        let clean = walk(&reader);
        assert_eq!(clean.0.len(), 4, "zone group, sentinel, tick, 4-slot empty group");
        assert_eq!(committed(&reader).unwrap(), vec![7]);

        for &base in &[sentinel, empty4] {
            sweep_bit_flips(log.prefix_bytes(base), 0..8, |byte, bit, _| {
                assert_eq!(walk(&reader), clean, "prefix {base}+{byte} bit {bit} changed the walk");
                assert_eq!(
                    committed(&reader).unwrap(),
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
        let region = SharedRegion::new(SIZE);
        {
            let old = Log::new(&region, 1);
            old.group(11, 1, FLAG_DDL_SYNC);
            old.group(22, 2, FLAG_DDL_SYNC);
            old.group(33, 3, FLAG_DDL_SYNC);
        }
        // A shorter epoch-2 log over the same bytes: one group, so epoch 1's
        // second and third groups survive past the new frontier.
        let new = Log::new(&region, 2);
        new.group(44, 9, FLAG_DDL_SYNC);

        let reader = new.reader();
        assert_eq!(reader.walk_epoch(), 2, "offset 0's header anchors the walk");
        let (groups, corrupt) = walk(&reader);
        assert_eq!(groups, vec![(9, 44)], "the walk ends at the epoch-1 leftover");
        assert!(corrupt.is_empty(), "a leftover is absent, not corrupt");
    }

    /// The walk epoch comes from group 0's digested header, not its prefix copy,
    /// so no single flip there can make every later comparison fail.
    #[test]
    fn group_zeros_prefix_epoch_is_not_a_single_point_of_failure() {
        let region = SharedRegion::new(SIZE);
        let mut log = Log::new(&region, 5);
        log.group(11, 101, FLAG_DDL_SYNC);
        log.group(22, 102, FLAG_DDL_SYNC);
        let reader = log.reader();
        let clean = walk(&reader);
        assert_eq!(clean.0.len(), 2);

        // Bytes 4..8 of the prefix word are its epoch copy.
        sweep_bit_flips(log.prefix_bytes(0), 4..8, |byte, bit, _| {
            assert_eq!(
                reader.walk_epoch(),
                5,
                "epoch byte {byte} bit {bit} moved the walk epoch"
            );
            assert_eq!(walk(&reader), clean, "epoch byte {byte} bit {bit} changed the walk");
        });
    }

    /// With offset 0's header damaged, the anchor is the *maximum* epoch in the
    /// ring — not the first valid header found, which under a page revert can be
    /// an older leftover at a low offset.
    #[test]
    fn the_walk_epoch_survives_a_damaged_offset_zero() {
        let region = SharedRegion::new(SIZE);
        // A previous epoch's leftover further in, left behind by a shorter later
        // pass, plus the live epoch-4 log over the front.
        {
            let old = Log::new(&region, 2);
            for i in 0..8 {
                old.group(90 + i, 1, FLAG_DDL_SYNC);
            }
        }
        let log = Log::new(&region, 4);
        log.group(11, 101, FLAG_DDL_SYNC);
        log.group(22, 102, FLAG_DDL_SYNC);
        log.group(33, 103, FLAG_DDL_SYNC);
        log.damage_header(0);

        let reader = log.reader();
        assert_eq!(
            reader.walk_epoch(),
            4,
            "the maximum epoch in the ring, not a leftover's"
        );
        let (groups, corrupt) = walk(&reader);
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
        let region = SharedRegion::new(SIZE);
        assert_eq!(Log::new(&region, 0).reader().walk_epoch(), 0);

        // A reset ring: `boot_reset`/`checkpoint_reset` zero offset 0's prefix but
        // leave its header, so the probe answers and the floor carries forward.
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 7);
        log.group(11, 1, FLAG_DDL_SYNC);
        log.writer.boot_reset(8);
        assert_eq!(
            log.reader().walk_epoch(),
            7,
            "a reset ring's floor comes from the surviving header, not the zeroed prefix"
        );
        assert_eq!(log.writer.epoch(), 8, "the next writer epoch is one above the floor");

        // A damaged offset 0 with a non-zero prefix is the one shape that sweeps.
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 6);
        log.group(11, 1, FLAG_DDL_SYNC);
        log.group(22, 2, FLAG_DDL_SYNC);
        log.damage_header(0);
        assert_eq!(log.reader().walk_epoch(), 6);
    }

    // -----------------------------------------------------------------------
    // The zone-span rule
    // -----------------------------------------------------------------------

    /// (a) Damage inside a middle committed zone is a hole: a later committed
    /// zone is durable behind it, so the boot must fail naming the offset.
    #[test]
    fn damage_in_a_middle_zone_fails_the_boot() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11, 12]);
        let second = log.zone(2, &[21, 22]);
        log.zone(3, &[31, 32]);
        log.damage_header(second[1]);

        let reader = log.reader();
        let err = committed(&reader).expect_err("a hole must fail the boot");
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
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11, 12]);
        let last = log.zone(2, &[21, 22]);
        log.damage_header(last[1]);

        assert_eq!(committed(&log.reader()).unwrap(), vec![1]);
    }

    /// (c) Damage in an `lsn = 0` command group between two zones costs nothing,
    /// and the committer fires one of these ticks after every push.
    #[test]
    fn damage_in_a_command_group_between_zones_costs_nothing() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        let tick = log.command();
        log.zone(2, &[21]);
        log.zone(3, &[31]);
        log.damage_header(tick);

        assert_eq!(
            committed(&log.reader()).unwrap(),
            vec![1, 2, 3],
            "every committed zone must survive rot in a tick group"
        );
    }

    /// (d) Damage in an unclosed tail zone costs nothing — the zone was never
    /// promised to anyone.
    #[test]
    fn damage_in_an_unclosed_tail_zone_costs_nothing() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        log.group(21, 2, FLAG_DDL_SYNC | FLAG_ZONE_START);
        let torn = log.group(22, 2, FLAG_DDL_SYNC);
        log.damage_header(torn);

        assert_eq!(committed(&log.reader()).unwrap(), vec![1]);
    }

    /// A stream-only commit batch writes its `FLAG_PUSH` groups with no zone start
    /// and no sentinel, so damage in one costs nothing however many precede a
    /// committed zone. This is why a stream batch opens no zone at all: pass 1 is
    /// built to a budget of at most one un-fsynced zone, and a run of them could
    /// refuse a boot over damage to a zone no client was ever promised.
    #[test]
    fn damage_in_zone_less_stream_batches_costs_nothing() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        let first = log.group(41, 7, FLAG_PUSH);
        let second = log.group(42, 8, FLAG_PUSH);
        log.zone(9, &[11]);
        log.damage_header(first);
        log.damage_header(second);

        assert_eq!(
            committed(&log.reader()).unwrap(),
            vec![9],
            "only the closed zone commits, and the damaged stream groups do not fail the boot"
        );
    }

    /// (e) A sentinel arriving with no zone open lost that zone's first group;
    /// applying its siblings would half-apply an atomic unit.
    #[test]
    fn a_zone_that_lost_its_first_group_fails_the_boot() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        let second = log.zone(2, &[21, 22]);
        log.zone(3, &[31]);
        log.damage_header(second[0]);

        let err = committed(&log.reader()).expect_err("a lost head group must fail the boot");
        assert!(err.contains("lsn=2"), "{err}");
    }

    /// A zone with a readable start, no sentinel, and a group after it did close:
    /// its sentinel was destroyed, not omitted.
    #[test]
    fn a_lost_sentinel_is_not_a_zone_that_never_closed() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        let first = log.zone(1, &[11]);
        log.zone(2, &[21]);
        // Destroy the first zone's sentinel.
        log.damage_header(*first.last().unwrap());

        let err = committed(&log.reader()).expect_err("a destroyed sentinel must fail the boot");
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
            let region = SharedRegion::new(SIZE);
            if leftover {
                // A previous epoch's group parked where the live log will straddle
                // it, so the scan meets an intact epoch-1 header below the frontier.
                let old = Log::new(&region, 1);
                for _ in 0..40 {
                    old.group(90, 1, FLAG_DDL_SYNC);
                }
            }
            let log = Log::new(&region, 4);
            log.zone(1, &[11]);
            let second_start = log.group(21, 2, FLAG_DDL_SYNC | FLAG_ZONE_START);
            log.group(22, 2, FLAG_DDL_SYNC);
            log.writer.write_commit_sentinel(2).expect("sentinel fits");
            log.zone(3, &[31]);
            // Destroy the zone's first group's header entirely, leaving its prefix:
            // the walk must resync across whatever follows.
            log.zero_header(second_start, 1);

            let err = committed(&log.reader()).expect_err("the hole must be reported (leftover={leftover})");
            assert!(err.contains("lsn=2"), "leftover={leftover}: {err}");
        }
    }

    /// A group's prefix and header can fall on different pages, and unordered
    /// writeback can persist the prefix and lose the header. Without the resync
    /// scan that would be terminal and throw the committed prefix away.
    #[test]
    fn a_torn_header_page_does_not_cost_the_committed_prefix() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        log.zone(2, &[21]);
        // An unclosed tail zone whose head group's header was lost.
        let torn = log.group(31, 3, FLAG_DDL_SYNC | FLAG_ZONE_START);
        log.zero_header(torn, 1);

        assert_eq!(
            committed(&log.reader()).unwrap(),
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
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        let tick = log.command();
        log.zone(2, &[21]);
        log.damage_header(tick);

        let reader = log.reader();
        let (groups, corrupt) = walk(&reader);
        assert_eq!(corrupt, vec![tick]);
        assert_eq!(
            groups,
            vec![(1, 11), (1, 0), (2, 21), (2, 0)],
            "zone 1 + its sentinel, then zone 2 + its sentinel; the tick is the damage"
        );
        assert_eq!(committed(&reader).unwrap(), vec![1, 2]);
    }

    /// A push zone over `NW` workers: one `scatter_wire_group` per entry of
    /// `targets` (the first opening the zone), then the commit sentinel. Rows are
    /// PK-partitioned, so most slots are `ctrl + schema` with no data block —
    /// exactly the shape a partitioned push leaves. Returns each group's base.
    fn push_zone(writer: &SalWriter, lsn: u64, targets: &[u32]) -> Vec<u64> {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let block = build_schema_wire_block(&schema, TID);
        let props = compute_wire_props(&schema);
        let req_ids: Vec<u64> = (0..NW as u64).collect();
        let mut bases = Vec::new();
        for (i, &t) in targets.iter().enumerate() {
            bases.push(writer.cursor());
            let flags = FLAG_PUSH | if i == 0 { FLAG_ZONE_START } else { 0 };
            with_commit_indices(&batch, &schema, NW, |wi| {
                writer
                    .scatter_wire_group(
                        &batch,
                        wi,
                        &schema,
                        t,
                        lsn,
                        flags,
                        0,
                        0,
                        &req_ids,
                        Some(block.as_slice()),
                        Some(props),
                    )
                    .expect("group fits")
            });
        }
        writer.write_commit_sentinel(lsn).expect("sentinel fits");
        bases
    }

    fn families(targets: &[u32]) -> HashMap<i64, u64> {
        targets.iter().map(|&t| (t as i64, 0u64)).collect()
    }

    fn reader_at(region: &SharedRegion, w: u32) -> SalReader {
        SalReader::for_walk(region.ptr() as *const u8, w, SIZE)
    }

    /// Corrupt one byte of slot `w` of the group at `base`, past the control
    /// block's own header so the block checksum is what catches it.
    fn damage_slot(region: &SharedRegion, base: u64, w: u32) {
        let slot = reader_at(region, w).slot_at(base, w).expect("slot carries bytes");
        let off = (slot.as_ptr() as usize) - (region.ptr() as usize);
        unsafe { *region.ptr().add(off + gnitz_wire::WAL_HEADER_SIZE) ^= 0xFF };
    }

    /// The highest slot of the group at `base` that carries rows.
    fn a_slot_with_rows(region: &SharedRegion, base: u64) -> u32 {
        (0..NW as u32)
            .rev()
            .find(|&w| reader_at(region, w).slot_at(base, w).is_some())
            .expect("some slot carries rows")
    }

    /// A push whose rows do not reach every worker leaves the remaining slots a
    /// control block and a schema block with no data block. Pass 1 must read that
    /// as "decodes", not as "carries rows" — otherwise every partitioned push
    /// demotes its own zone.
    #[test]
    fn a_row_less_slot_is_not_a_damaged_one() {
        let region = SharedRegion::new(SIZE);
        let writer = SalWriter::new(region.ptr(), -1, SIZE as u64, NW);
        writer.reset(0, 1);
        let bases = push_zone(&writer, 5, &[TID]);

        // The fixture must actually leave a row-less slot, or it proves nothing.
        let row_less = (0..NW as u32)
            .filter(|&w| {
                let slot = reader_at(&region, w)
                    .slot_at(bases[0], w)
                    .expect("every slot is written");
                ipc::decode_wire(slot).expect("slot decodes").data_batch.is_none()
            })
            .count();
        assert!(row_less > 0, "the fixture must leave at least one slot row-less");

        assert_eq!(
            committed_lsns(&reader_at(&region, 0), FLAG_PUSH, &families(&[TID])).unwrap(),
            vec![5]
        );
    }

    /// The demotion verdict is global: rot in worker 3's slot must demote the
    /// zone on every walk, worker 0's included. A per-slot verdict would apply the
    /// zone on three workers and skip it on one.
    #[test]
    fn the_demotion_is_global_across_slots() {
        for damage_offset_zero in [false, true] {
            let region = SharedRegion::new(SIZE);
            let writer = SalWriter::new(region.ptr(), -1, SIZE as u64, NW);
            writer.reset(0, 1);
            // A leading command group, so offset 0 is not the zone's own head and
            // can be damaged independently: with it gone there is no tail-wide slot
            // count to read, and only each group's own count is available.
            let next = unsafe {
                sal_write_group(region.ptr(), 0, 9, 0, FLAG_TICK, 1, SIZE as u64, &[&[0u8; 8]]).expect("fits")
            };
            writer.reset(next, 1);
            let bases = push_zone(&writer, 5, &[TID]);

            // Rot the highest slot carrying rows, so the reader below (worker 0)
            // is not the damaged one.
            let victim = a_slot_with_rows(&region, bases[0]);
            damage_slot(&region, bases[0], victim);
            if damage_offset_zero {
                unsafe { *region.ptr().add(8) ^= 1 };
            }

            for w in 0..NW as u32 {
                assert!(
                    committed_lsns(&reader_at(&region, w), FLAG_PUSH, &families(&[TID]))
                        .unwrap()
                        .is_empty(),
                    "worker {w} must demote the zone rot lives in slot {victim} of \
                     (offset0_damaged={damage_offset_zero})"
                );
            }
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
        let region = SharedRegion::new(SIZE);
        let writer = SalWriter::new(region.ptr(), -1, SIZE as u64, NW);
        writer.reset(0, 1);
        let bases = push_zone(&writer, 5, &[TID, STREAM_TID]);
        damage_slot(&region, bases[1], a_slot_with_rows(&region, bases[1]));

        assert_eq!(
            committed_lsns(&reader_at(&region, 0), FLAG_PUSH, &families(&[TID])).unwrap(),
            vec![5],
            "the base push must still replay"
        );
        // The same damage in a mapped family does demote it, so the assertion above
        // is about the map and not about the fixture failing to damage anything.
        assert!(
            committed_lsns(&reader_at(&region, 0), FLAG_PUSH, &families(&[TID, STREAM_TID]))
                .unwrap()
                .is_empty()
        );
    }

    /// A torn last zone is skipped whole, not partially: pass 1 drops the LSN, so
    /// pass 2 never applies one family's group while skipping its sibling's.
    #[test]
    fn a_torn_last_zone_is_skipped_whole() {
        let region = SharedRegion::new(SIZE);
        let writer = SalWriter::new(region.ptr(), -1, SIZE as u64, NW);
        writer.reset(0, 1);
        push_zone(&writer, 4, &[TID]);
        let last = push_zone(&writer, 5, &[TID, TID + 1]);
        // Rot the FIRST family of the last zone.
        damage_slot(&region, last[0], a_slot_with_rows(&region, last[0]));

        assert_eq!(
            committed_lsns(&reader_at(&region, 0), FLAG_PUSH, &families(&[TID, TID + 1])).unwrap(),
            vec![4],
            "the torn zone must be dropped whole, and the durable one before it kept"
        );
    }
}
