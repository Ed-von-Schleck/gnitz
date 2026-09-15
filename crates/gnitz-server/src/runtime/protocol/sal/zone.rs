//! The SAL **zone protocol**: the atomic unit recovery commits, both directions.
//!
//! A zone's byte span runs from its zone-start group to the
//! [`SalMessageKind::ZoneCommit`] sentinel `SalScope::commit` closes it with,
//! and its groups apply all-or-nothing. A whole CREATE is one zone — its N
//! families ride one `DDL_TXN` bundle under a single zone LSN — so all of
//! COL_TAB and TABLE_TAB replay, or none of it.
//!
//! Recovery reads it in two passes over the same walk, both behind
//! [`CommittedTail`]. Neither knows what a group *means*: the master (pre-fork,
//! system families) and each worker (post-fork, user families) vary only which
//! groups are theirs, and what they do with the bytes.

use std::collections::{HashMap, HashSet};

use super::{EpochGate, SalLog, SalMessage, SalMessageKind, SalStep, PREFIX_BYTES};
use crate::runtime::wire as ipc;

impl SalLog {
    /// Walk the SAL from `start` against the walk epoch `E`, resyncing past damage
    /// to the next candidate valid at `E` so that one bad offset does not discard the
    /// committed groups behind it. A zero prefix or a header at another epoch ends
    /// the walk — the ring's leftovers begin there.
    ///
    /// Both passes walk from 0, and identically, so they cannot disagree about
    /// where the log ends; a `start` past 0 re-walks a span pass 1 already
    /// judged. Successive resyncs examine disjoint increasing ranges, so a walk
    /// sweeps the mapping at most once.
    fn walk_from(&self, start: u64, epoch: u32) -> impl Iterator<Item = SalStep> + '_ {
        let mut offset: u64 = start;
        std::iter::from_fn(move || {
            let step = self.read_at(offset, EpochGate::Walk(epoch));
            match step {
                SalStep::Group(_, next) => offset = next,
                SalStep::Corrupt(at) => {
                    offset = self
                        .valid_headers_from(at + PREFIX_BYTES as u64)
                        .find(|&(_, e)| e == epoch)
                        .map_or(self.mmap_size as u64, |(base, _)| base);
                }
                SalStep::Absent => return None,
            }
            Some(step)
        })
    }
}

/// One zone's byte span and integrity, as pass 1 accumulates it. Four scalars:
/// the groups inside the span are re-walked from it when a verdict needs them,
/// rather than kept for the one zone in a tail that is ever asked.
#[derive(Clone, Copy, Default)]
struct Zone {
    lsn: u64,
    /// The zone-start group's offset — the start of the zone's span.
    start: u64,
    /// The commit sentinel's offset — the span's exclusive end. Zero until the
    /// zone closes.
    end: u64,
    /// The first corrupt offset inside the span, if any.
    damage: Option<u64>,
}

/// The committed part of the un-checkpointed tail. Constructing it is pass 1;
/// iterating it is pass 2.
///
/// Both ask [`applies`](Self::applies), so pass 1 cannot demote a zone over a
/// group pass 2 would have skipped.
pub(crate) struct CommittedTail<'a> {
    log: SalLog,
    epoch: u32,
    /// A group is this walk's iff it is of this kind…
    kind: SalMessageKind,
    /// …and its zone LSN is past what its family already has on disk. A family
    /// absent from the map has no store to recover into.
    family_lsns: &'a HashMap<i64, u64>,
    committed: HashSet<u64>,
}

impl<'a> CommittedTail<'a> {
    /// Pass 1 over the walk at `epoch`, scoped to `kind` and `family_lsns`.
    pub(crate) fn open(
        log: SalLog,
        epoch: u32,
        kind: SalMessageKind,
        family_lsns: &'a HashMap<i64, u64>,
    ) -> Result<Self, String> {
        let mut tail = CommittedTail {
            log,
            epoch,
            kind,
            family_lsns,
            committed: HashSet::new(),
        };
        tail.committed = tail.collect_committed_lsns()?;
        Ok(tail)
    }

    /// Whether this walk applies `msg`. The kind test comes first and is not a
    /// filter for convenience: `OFF_LSN` is shared by three unsynchronised
    /// counters, so without it a tick round could be compared against — and
    /// replayed as — a committed zone LSN of the same value.
    fn applies(&self, msg: &SalMessage) -> bool {
        msg.kind == self.kind
            && self
                .family_lsns
                .get(&(msg.target_id as i64))
                .is_some_and(|&f| msg.lsn > f)
    }

    /// The highest committed zone LSN in the tail, of any kind; `0` for none.
    pub(crate) fn max_committed_lsn(&self) -> u64 {
        self.committed.iter().copied().max().unwrap_or(0)
    }

    /// Pass 2: this walk's groups from every committed zone, in log order.
    ///
    /// Yielded whole rather than as bytes — a worker re-reads one group across
    /// several slots — and undecoded, so the caller's own per-slot skips run
    /// before the copy.
    pub(crate) fn groups(&self) -> impl Iterator<Item = SalMessage> + '_ {
        self.log.walk_from(0, self.epoch).filter_map(move |step| {
            // Pass 1 already reached its verdict on every corrupt offset: it either
            // failed the boot or established that nothing committed lies there.
            let SalStep::Group(msg, _) = step else { return None };
            let keep = self.committed.contains(&msg.lsn) && self.applies(&msg);
            keep.then_some(msg)
        })
    }

    /// Pass 1: the LSNs whose zone is both closed and intact.
    ///
    /// A zone's byte span runs from its zone-start group to its commit
    /// sentinel, and nothing else lives in it — a zone is written with no
    /// suspension point inside it, so no other writer can place a group there.
    /// Damage costs that zone a group, and the verdict follows from where it sits:
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
        let mut last_closed: Option<Zone> = None;

        for step in self.log.walk_from(0, self.epoch) {
            let msg = match step {
                SalStep::Corrupt(off) => {
                    if let Some(z) = open.as_mut() {
                        z.damage.get_or_insert(off);
                    }
                    continue;
                }
                SalStep::Group(msg, _) => msg,
                SalStep::Absent => unreachable!("the walk stops at the end of the log"),
            };
            let mine = open.as_ref().is_some_and(|z| z.lsn == msg.lsn);
            if msg.kind == SalMessageKind::ZoneCommit {
                // A damaged zone that did close is fatal once a *later* sentinel
                // proves a committed zone is durable behind it; the log's last one
                // is only demoted. Derived here rather than carried, so the message
                // is formatted for the zone that fires and not for every zone.
                if let Some(z) = last_closed.filter(|z| z.damage.is_some()) {
                    return Err(format!(
                        "SAL replay: committed zone lsn={} lost a group at offset={} (zone ends \
                         at offset={}); a later committed zone is durable behind it, so this is a \
                         hole in the log rather than a torn tail",
                        z.lsn,
                        z.damage.expect("filtered to damaged"),
                        z.end
                    ));
                }
                // A sentinel with no zone open lost the zone's first group; one that
                // closes a *different* zone lost that zone's sentinel as well.
                let Some(mut zone) = open.take().filter(|_| mine) else {
                    return Err(format!(
                        "SAL replay: committed zone lsn={} lost a group before its commit sentinel at \
                         offset={}; its zone-start group is not in the log",
                        msg.lsn, msg.base
                    ));
                };
                zone.end = msg.base;
                if zone.damage.is_none() {
                    committed.insert(zone.lsn);
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
            if msg.zone_start {
                open = Some(Zone {
                    lsn: msg.lsn,
                    start: msg.base,
                    ..Zone::default()
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
    /// Across every slot the group declares, not only one reader's, so worker 3
    /// cannot demote a zone worker 0 applies.
    ///
    /// The groups are re-walked from the zone's own span rather than kept from
    /// pass 1. Sound because this runs only for a committed zone, whose span is
    /// by definition undamaged and so walks with no resync — and because zone
    /// LSNs are unique within a tail (`lsn_alloc.reserve` is monotone under
    /// `sal_writer_excl`), so the span this finds is the zone's own.
    ///
    /// "Decodes" is not "carries rows": a push whose rows all land on one worker
    /// leaves the other slots a control block alone — no schema and no data
    /// block — which is correctly a no-op on replay.
    fn zone_blocks_decode(&self, zone: &Zone) -> bool {
        for step in self.log.walk_from(zone.start, self.epoch) {
            let SalStep::Group(msg, _) = step else {
                debug_assert!(false, "an intact zone span walks with no resync");
                return false;
            };
            if msg.base >= zone.end {
                break;
            }
            if !self.applies(&msg) {
                continue;
            }
            for (w, bytes) in msg.slots_written() {
                if let Err(e) = ipc::decode_wire(bytes) {
                    gnitz_warn!(
                        "SAL replay: last committed zone lsn={} is torn (offset={} slot={w} \
                         target={}: {e}); skipping it whole",
                        zone.lsn,
                        msg.base,
                        msg.target_id
                    );
                    return false;
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
// not that it carries rows.
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/zone.rs"]
mod tests;
