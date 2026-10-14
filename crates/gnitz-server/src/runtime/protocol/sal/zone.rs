//! Which zones a boot replays: every zone closed before the recovery walk's
//! first defect.

use super::{next_epoch, SalLog, SalMessage, SalMessageKind};

/// The committed part of the un-checkpointed tail.
#[derive(Clone, Copy)]
pub(crate) struct CommittedTail {
    log: SalLog,
    epoch: u32,
    /// The end of the last zone closed before the walk's first defect.
    end: u64,
}

impl CommittedTail {
    /// Walk `log` from 0 at its anchored epoch to the first defect.
    pub(crate) fn read(log: SalLog) -> Result<Self, String> {
        let epoch = log.anchor()?;
        let (mut end, mut open) = (0u64, None);
        for msg in log.walk(epoch) {
            if msg.head.lsn != 0 {
                if !msg.intact() || open.is_some_and(|zone| zone != msg.head.lsn) {
                    break;
                }
                open = (!msg.head.zone_end()).then_some(msg.head.lsn);
                if msg.head.zone_end() {
                    end = msg.end;
                }
            }
        }
        Ok(CommittedTail { log, epoch, end })
    }

    /// Whether a push committed since the last base round: this boot's replay
    /// moves a base store.
    pub(crate) fn holds_pushes(self) -> bool {
        self.groups().any(|m| m.head.kind == SalMessageKind::Push)
    }

    /// The log the tail was read from.
    pub(crate) fn log(self) -> SalLog {
        self.log
    }

    /// The epoch this boot writes and drains at: one above the tail's.
    pub(crate) fn live_epoch(&self) -> u32 {
        next_epoch(self.epoch)
    }

    /// Every zone member below the committed end, in log order, undecoded.
    pub(crate) fn groups(self) -> impl Iterator<Item = SalMessage> {
        self.log
            .walk(self.epoch)
            .take_while(move |m| m.end <= self.end)
            .filter(|m| m.head.lsn != 0)
    }
}

#[cfg(test)]
#[path = "tests/zone.rs"]
mod tests;
