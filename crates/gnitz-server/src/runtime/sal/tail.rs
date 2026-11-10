//! Which zones a boot replays: every zone closed before the recovery walk's
//! first defect.

use std::collections::BTreeSet;

use super::format::{epoch_word, next_epoch, SalMessageKind};
use super::log::{SalLog, SalMessage};
use super::reader::SalReader;

/// The committed part of the un-checkpointed tail.
pub(crate) struct CommittedTail {
    log: SalLog,
    epoch: u32,
    /// The end of the last zone closed before the walk's first defect.
    end: u64,
    /// The relations the tail's Push groups target.
    pushed: BTreeSet<u64>,
}

impl CommittedTail {
    /// Open `data_dir`'s SAL and read its tail; from here on, a fault on the
    /// mapping reads no pages ahead ([`SalLog::advise_random`]).
    pub(crate) fn open(data_dir: &str) -> Result<Self, String> {
        let tail = Self::read(SalLog::open(data_dir)?)?;
        tail.log.advise_random();
        Ok(tail)
    }

    /// Walk `log` from 0 at its anchored epoch to the first defect.
    pub(crate) fn read(log: SalLog) -> Result<Self, String> {
        let epoch = log.anchor()?;
        let mut end = 0u64;
        let (mut pushed, mut zone) = (BTreeSet::new(), Vec::new());
        for msg in log.walk(epoch).filter(|m| m.head.zoned()) {
            if !msg.intact() {
                break;
            }
            if msg.head.request.kind == SalMessageKind::Push {
                zone.push(msg.head.request.target_id);
            }
            if msg.head.zone_end() {
                end = msg.end;
                pushed.extend(zone.drain(..));
            }
        }
        Ok(CommittedTail { log, epoch, end, pushed })
    }

    /// Whether a push committed since the last base round: this boot's replay
    /// moves a base store.
    pub(crate) fn holds_pushes(&self) -> bool {
        !self.pushed.is_empty()
    }

    /// The tail's Push groups, in log order. A stream's push is written outside
    /// every zone, and every tail group is a zone member, so each targets a base
    /// table.
    pub(crate) fn pushes(&self) -> impl Iterator<Item = SalMessage<'_>> {
        self.groups().filter(|m| m.head.request.kind == SalMessageKind::Push)
    }

    /// The tail's DdlSync groups, in log order.
    pub(crate) fn ddl_syncs(&self) -> impl Iterator<Item = SalMessage<'_>> {
        self.groups().filter(|m| m.head.request.kind == SalMessageKind::DdlSync)
    }

    /// The relations [`Self::pushes`] target, each once, ascending.
    pub(crate) fn pushed(&self) -> &BTreeSet<u64> {
        &self.pushed
    }

    /// Worker `rank`'s live drain of the log the tail was read from.
    pub(crate) fn reader(&self, rank: u32) -> SalReader {
        SalReader::new(self.log, rank, self.live_epoch())
    }

    /// The log the tail was read from.
    pub(super) fn log(&self) -> SalLog {
        self.log
    }

    /// The epoch this boot writes and drains at: one above the tail's.
    pub(super) fn live_epoch(&self) -> u32 {
        next_epoch(self.epoch)
    }

    /// Every zone member below the committed end, in log order, undecoded.
    fn groups(&self) -> impl Iterator<Item = SalMessage<'_>> {
        self.log
            .walk(self.epoch)
            .take_while(move |m| m.end <= self.end)
            .filter(|m| m.head.zoned())
    }

    /// The LSN recovery orders the tail member `msg` by: its own end in the
    /// tail's epoch, at or below its zone's LSN and above every earlier zone's.
    pub(crate) fn lsn(&self, msg: &SalMessage) -> u64 {
        epoch_word(self.epoch, msg.end)
    }
}

#[cfg(test)]
#[path = "tests/tail.rs"]
mod tests;
