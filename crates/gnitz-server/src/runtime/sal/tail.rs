//! Which zones a boot replays: every zone closed before the recovery walk's
//! first defect.

use std::collections::BTreeSet;

use super::format::{SalMessageKind, epoch_word, next_epoch, word_epoch};
use super::log::{SalLog, SalMessage};
use super::reader::SalReader;
use super::request::Ingest;
use crate::catalog::{SysFamily, SysFamilyExt};
use gnitz_zset::repr::Batch;

/// The committed part of the un-checkpointed tail.
pub(crate) struct CommittedTail {
    pub(super) log: SalLog,
    epoch: u32,
    /// The end of the last zone closed before the walk's first defect.
    end: u64,
    /// The epoch this boot writes and drains at: above the tail's.
    pub(super) live_epoch: u32,
}

impl CommittedTail {
    /// Open `data_dir`'s SAL and read its tail, made durable before a boot acts
    /// on it: a zone a crash left intact but unsynced is committed by the stores
    /// the boot flushes it into. A tail without a zone has nothing to sync. From
    /// here on, a fault on the mapping reads no pages ahead
    /// ([`SalLog::advise_random`]).
    pub(crate) fn open(data_dir: &str) -> Result<Self, String> {
        let tail = Self::read(SalLog::open(data_dir)?)?;
        if tail.end > 0 {
            tail.log
                .file
                .sync_data()
                .map_err(|e| format!("failed to sync the SAL tail: {e}"))?;
        }
        tail.log.advise_random();
        Ok(tail)
    }

    /// Walk `log` from 0 at its anchored epoch to the first defect.
    pub(crate) fn read(log: SalLog) -> Result<Self, String> {
        let epoch = log.anchor()?;
        let mut end = 0u64;
        for msg in log.walk(epoch).filter(|m| m.head.zoned()) {
            if !msg.intact() {
                break;
            }
            if msg.head.zone_end() {
                end = msg.end;
            }
        }
        Ok(CommittedTail {
            log,
            epoch,
            end,
            live_epoch: next_epoch(epoch),
        })
    }

    /// Start this boot's epoch above `floor`'s too: the newest zone the system
    /// stores hold, an LSN of the log they were flushed beside. That is this log
    /// unless the data directory came without one, as a backup does, and a zone
    /// written at or below a floor is one a later boot takes for flushed.
    pub(crate) fn write_above(&mut self, floor: u64) {
        self.live_epoch = self.live_epoch.max(next_epoch(word_epoch(floor)));
    }

    /// Whether a push committed since the last base round: this boot's replay
    /// moves a base store.
    pub(crate) fn holds_pushes(&self) -> bool {
        self.push_targets().next().is_some()
    }

    /// Every group a boot replays, in log order, as the ingest it carries: a zone
    /// holds nothing else. A stream's push is written outside every zone, so each
    /// push here targets a base table.
    pub(crate) fn ingests(&self) -> impl Iterator<Item = Result<(Ingest, SalMessage<'_>), String>> {
        self.groups().map(|m| match Ingest::decode(&m.head.request) {
            Ok(ingest) => Ok((ingest, m)),
            Err(e) => Err(format!("SAL replay: the zone member at offset={}: {e}", m.base)),
        })
    }

    /// The base tables the tail's pushes target, each once, ascending.
    pub(crate) fn pushed(&self) -> BTreeSet<u64> {
        self.push_targets().collect()
    }

    /// The target of each Push group the tail holds, read off its header.
    fn push_targets(&self) -> impl Iterator<Item = u64> {
        self.groups()
            .filter(|m| m.head.request.kind == SalMessageKind::Push)
            .map(|m| m.head.request.target_id)
    }

    /// The rows of each of the tail's DdlSync groups, with the family and LSN
    /// it was logged at, in log order. A DdlSync group is shared: its one
    /// payload holds the rows, which the master validated and which already
    /// carry a drop's children, under the family's own shape record.
    pub(crate) fn catalog_rows(&self) -> impl Iterator<Item = Result<(SysFamily, u64, Batch), String>> {
        self.ingests().filter_map(|group| match group {
            Ok((Ingest::DdlSync { family }, msg)) => msg
                .rows(msg.payloads().next().unwrap_or_default(), |_, record| {
                    (record == family.shape_record()).then(|| *family.schema())
                })
                .transpose()
                .map(|rows| rows.map(|rows| (family, self.lsn(&msg), rows))),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
    }

    /// Worker `rank`'s live drain of the log the tail was read from.
    pub(crate) fn reader(&self, rank: u32) -> SalReader {
        SalReader::new(self.log, rank, self.live_epoch)
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
