//! Which zones a boot replays: every zone closed before the recovery walk's
//! first defect, unless that defect lies below the anchored `synced` offset —
//! a hole in the durable log, which fails the boot.

use super::{next_epoch, SalLog, SalMessage};

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
        let (epoch, synced) = log.anchor()?;
        let (mut at, mut end, mut open) = (0u64, 0u64, None);
        for msg in log.walk(epoch) {
            if msg.lsn != 0 {
                if !msg.intact() || open.is_some_and(|zone| zone != msg.lsn) {
                    break;
                }
                open = (!msg.zone_end).then_some(msg.lsn);
                if msg.zone_end {
                    end = msg.end;
                }
            }
            at = msg.end;
        }
        if at < synced {
            return Err(format!(
                "SAL replay: the log breaks at offset={at}, below offset={synced} that a \
                 completed fdatasync covered — a hole in the durable log, not a torn tail"
            ));
        }
        Ok(CommittedTail { log, epoch, end })
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
            .filter(|m| m.lsn != 0)
    }
}

#[cfg(test)]
#[path = "tests/zone.rs"]
mod tests;
