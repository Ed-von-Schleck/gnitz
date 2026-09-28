//! Exchange rounds. A worker publishes its partition as a train of frames; a round
//! completes once every worker's terminal frame — flagged `scan_last` — is in.
//!
//! Every worker runs the same sequence of rounds and publishes the next only after
//! this one's relay comes back, so at most one round is open: a frame for another
//! `(view_id, source_id)` means the workers diverged.

use crate::runtime::sal::WorkerSet;
use crate::runtime::wire::DecodedWire;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;

/// The open exchange round, keyed by `(view_id, source_id)`: a two-source view opens
/// one round per source, and a relay carries its own source's shard columns.
pub(crate) struct ExchangeRound {
    pub(crate) view_id: i64,
    pub(crate) source_id: i64,
    pub(crate) schema: SchemaDescriptor,
    /// One frame list per worker, in frame order, so the relay's row order does not
    /// depend on arrival order.
    pub(crate) frames: Vec<Vec<Batch>>,
    reported: WorkerSet,
    /// AND of the workers' `flags.drained`: true on a backfill's final round.
    pub(crate) drained: bool,
}

impl ExchangeRound {
    /// The round `frame` opens, among `nw` workers.
    pub(crate) fn open(frame: &DecodedWire, nw: usize) -> Self {
        let hdr = &frame.control.hdr;
        let (view_id, source_id) = (hdr.target_id as i64, hdr.arg0 as i64);
        let schema = frame.schema.unwrap_or_else(|| {
            gnitz_fatal_abort!(
                "exchange: (view_id={view_id}, source_id={source_id}) frame has no schema block — ring corrupt"
            )
        });
        ExchangeRound {
            view_id,
            source_id,
            schema,
            frames: (0..nw).map(|_| Vec::new()).collect(),
            reported: WorkerSet::EMPTY,
            drained: true,
        }
    }

    /// Take worker `w`'s `frame`; true once every worker's terminal frame is in.
    pub(crate) fn accept(&mut self, w: usize, frame: DecodedWire) -> bool {
        let hdr = &frame.control.hdr;
        let key = (hdr.target_id as i64, hdr.arg0 as i64);
        if key != (self.view_id, self.source_id) {
            gnitz_fatal_abort!(
                "exchange: worker {w} sent (view_id, source_id)={key:?} while ({}, {}) is open — workers diverged",
                self.view_id,
                self.source_id
            );
        }
        let flags = hdr.flags;
        if let Some(b) = frame.data_batch {
            self.frames[w].push(b);
        }
        if !flags.scan_last {
            return false;
        }
        debug_assert!(!self.reported.contains(w), "worker {w} ended two trains in one round");
        self.drained &= flags.drained;
        self.reported = self.reported.with(w);
        self.reported.covers(self.frames.len())
    }
}

#[cfg(test)]
#[path = "tests/exchange.rs"]
mod tests;
