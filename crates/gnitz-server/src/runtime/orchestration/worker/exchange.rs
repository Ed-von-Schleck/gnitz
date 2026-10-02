//! The worker's half of an exchange round: publishing its partition on the mesh,
//! waiting for every peer's, part by part, and the matrix that decides what a
//! SAL group arriving during that wait does.

use super::*;
use gnitz_zset::algebra::ScatterPlan;
use std::borrow::Cow;

impl DriveHost for WorkerProcess<'_> {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        let cat = &mut *self.catalog;
        (&mut cat.dag, &mut cat.registry)
    }

    /// Publish `batch` as this worker's round of `view_id` and block until every
    /// part of it is complete. SAL groups arriving meanwhile go through
    /// [`Self::dispatch_in_wait`].
    fn exchange(&mut self, view_id: u64, batch: Cow<'_, Batch>, plan: &ScatterPlan, fold: bool) -> Batch {
        self.mesh.publish(view_id, &batch, plan, fold);
        // Dropped once every row is written, so this worker holds the partition
        // it sends beside the one it gathers only while its rows span parts.
        let mut batch = self.mesh.sending().then_some(batch);
        loop {
            self.w2m_writer
                .sal_park()
                .park(|| self.sal_reader.is_empty() && !self.mesh.complete());
            while let Some(req) = self.next_request() {
                // Once the round is complete the drive goes on; the next wait, or
                // the top level, serves what the SAL still holds, in order.
                if self.dispatch_in_wait(req) && self.mesh.complete() {
                    break;
                }
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
}

impl WorkerProcess<'_> {
    /// Run one drive; whether every worker's source was drained.
    pub(super) fn drive(&mut self, what: Drive, delta: Option<Batch>) -> Result<bool, String> {
        self.mesh.set_drained(delta.is_none());
        crate::query::drive(self, what, delta)?;
        Ok(self.mesh.drained())
    }

    /// Dispatch a group drained inside an exchange wait; whether it was a read.
    /// Total, so a new request cannot compile without a decision.
    pub(super) fn dispatch_in_wait(&mut self, req: Request) -> bool {
        let Request { route, what, rows } = req;
        match what {
            // A drive writes views, so a read of one would see the tick half-run:
            // it parks. A drive writes no other store, so every other read is
            // answered here, at its place in the SAL — a push behind it must not
            // reach it.
            SalRequest::Read(read) => {
                let registry = &self.catalog.registry;
                match registry.relation(read.target()).is_some_and(|r| r.kind().is_view()) {
                    true => self.replies.push_back(Owed::Parked(route, read, rows)),
                    false => {
                        let answer = answer(self.catalog, &read, rows.as_deref());
                        self.reply(route, answer);
                    }
                }
                true
            }
            // The master holds the SAL writer until every worker ACKs a flush;
            // deferred, a push's ACK would wait for the drive.
            SalRequest::Apply(apply @ (Apply::Flush | Apply::Push { .. })) => {
                self.apply_request(route, apply, rows);
                false
            }
            SalRequest::Shutdown => unsafe { libc::_exit(0) },
            // Never sent beside a drive.
            SalRequest::Apply(
                apply @ (Apply::DdlSync { .. } | Apply::Tick { .. } | Apply::FlushEph { .. } | Apply::Backfill { .. }),
            ) => gnitz_fatal_abort!("{apply:?} inside an exchange wait — diverged from the master"),
        }
    }
}
