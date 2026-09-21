//! Which zones a boot replays: every zone closed before the recovery walk's
//! first defect, unless that defect lies below the anchored `synced` offset —
//! a hole in the durable log, which fails the boot.

use super::{EpochGate, SalLog, SalMessage, SalStep};

/// The committed part of the un-checkpointed tail. `Copy`: built once by the
/// master before the fork.
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
        for (msg, next) in log.walk(epoch) {
            if !msg.intact() || (msg.lsn != 0 && open.is_some_and(|zone| zone != msg.lsn)) {
                break;
            }
            if msg.lsn != 0 {
                open = (!msg.zone_end).then_some(msg.lsn);
            }
            if msg.zone_end {
                end = next;
            }
            at = next;
        }
        if at < synced {
            return Err(format!(
                "SAL replay: the log breaks at offset={at}, below offset={synced} that a \
                 completed fdatasync covered — a hole in the durable log, not a torn tail"
            ));
        }
        Ok(CommittedTail { log, epoch, end })
    }

    /// The epoch the tail was written at; the next boot writes above it.
    pub(crate) fn epoch(&self) -> u32 {
        self.epoch
    }

    /// Every zone member below the committed end, in log order, undecoded.
    pub(crate) fn groups(self) -> impl Iterator<Item = SalMessage> {
        self.log
            .walk(self.epoch)
            .take_while(move |&(_, next)| next <= self.end)
            .map(|(msg, _)| msg)
            .filter(|msg| msg.lsn != 0)
    }
}

impl SalLog {
    /// Every group readable from offset 0 at `epoch`, with the offset past it,
    /// up to the first that is not.
    fn walk(self, epoch: u32) -> impl Iterator<Item = (SalMessage, u64)> {
        let mut at = 0;
        std::iter::from_fn(move || match self.read_at(at, EpochGate::Walk(epoch)) {
            SalStep::Group(msg, next) => {
                at = next;
                Some((msg, next))
            }
            SalStep::Absent | SalStep::Corrupt(_) => None,
        })
    }
}

#[cfg(test)]
#[path = "tests/zone.rs"]
mod tests;
