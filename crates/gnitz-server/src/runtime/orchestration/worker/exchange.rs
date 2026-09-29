//! The worker's half of an exchange round: publishing its partition on the mesh,
//! waiting for every peer's, part by part, and the matrix that decides what a
//! SAL group arriving during that wait does.

use super::*;
use gnitz_store::ops::ScatterSpec;
use std::borrow::Cow;

/// The worker as a drive's [`DriveHost`].
pub(super) struct DagExchangeCtx<'a> {
    pub(super) worker: &'a mut WorkerProcess,
    /// This worker's source partition is already drained, so its rounds are empty
    /// pads.
    pub(super) own_drained: bool,
    /// `own_drained` until a round completes, then every worker's, ANDed.
    pub(super) all_drained: bool,
}

impl DriveHost for DagExchangeCtx<'_> {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        let cat = self.worker.cat();
        (&mut cat.dag, &mut cat.registry)
    }

    fn exchange(&mut self, view_id: u64, batch: Cow<'_, Batch>, spec: Option<ScatterSpec<'_>>) -> Batch {
        let (batch, all_drained) = self.worker.exchange(view_id, batch, spec, self.own_drained);
        self.all_drained = all_drained;
        batch
    }
}

impl WorkerProcess {
    /// Publish `batch` as this worker's round of `view_id` and block until every
    /// part of it is complete, returning the rows this worker owns and whether
    /// every worker's partition was drained. SAL groups arriving mid-wait are
    /// dispatched per [`Self::dispatch_in_eval`].
    fn exchange(
        &mut self,
        view_id: u64,
        batch: Cow<'_, Batch>,
        spec: Option<ScatterSpec<'_>>,
        drained: bool,
    ) -> (Batch, bool) {
        self.mesh.publish(view_id, drained, &batch, spec);
        // Dropped once every row is written, so this worker holds the partition
        // it sends beside the one it gathers only while its rows span parts.
        let mut batch = self.mesh.sending().then_some(batch);
        loop {
            self.w2m_writer
                .sal_park()
                .park(|| self.sal_reader.is_empty() && !self.mesh.complete());
            while let Some((msg, wire)) = self.sal_reader.next() {
                let req = self.decode_request(&msg, wire);
                self.dispatch_in_eval(req);
            }
            if self.mesh.complete() {
                if let Some(out) = self.mesh.advance(batch.as_deref()) {
                    return out;
                }
                if !self.mesh.sending() {
                    batch = None;
                }
            }
        }
    }

    /// Dispatch a group drained while blocked in an exchange wait: run it where
    /// it arrives, defer it to top level right after the request whose drive
    /// deferred it, or abort on a kind the master never sends mid-drive. At top
    /// level every kind runs inline, so this is the whole matrix; it is total, so
    /// a new `SalMessageKind` cannot compile without a decision.
    pub(super) fn dispatch_in_eval(&mut self, req: Request) {
        use SalMessageKind::*;
        match req.kind {
            // Answered mid-drive, a read would see a half-run tick.
            Scan | ScanSpec | DeltaRead => self.deferred.push(req),
            // The master holds the SAL writer until every worker ACKs a flush.
            Flush => self.handle_request(req),
            // Deferred, the ingest ACK would wait for the drive.
            Push => self.handle_request(req),
            // A drive writes no base-table key.
            HasPk => self.handle_request(req),
            Shutdown => self.handle_request(req),
            // A serial-range reservation, which takes no tick gate: dropped unread.
            DdlSync if SysFamily::from_id(req.wire.control.hdr.target_id) == Some(SysFamily::Sequence) => {
                self.handle_request(req)
            }
            // Never sent beside a drive.
            DdlSync | Tick | FlushEph | Backfill | UniquePreflight => {
                gnitz_fatal_abort!("{:?} inside an exchange wait — diverged from the master", req.kind)
            }
        }
    }
}
