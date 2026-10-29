//! The worker's half of an exchange round: publishing its partition on the mesh
//! and waiting for every peer's, part by part.

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
    /// [`WorkerProcess::dispatch`] with `in_drive`.
    fn exchange(
        &mut self,
        view_id: u64,
        batch: Cow<'_, Batch>,
        plan: &ScatterPlan,
        fold: bool,
        drained: bool,
    ) -> (Batch, bool) {
        let mut round = self.mesh.open(view_id, batch, plan, fold, drained);
        loop {
            self.mesh
                .park()
                .park(|| self.sal_reader.is_empty() && !self.mesh.complete());
            while let Some(req) = self.next_request() {
                // Once the round is complete the drive goes on; the next wait, or
                // the top level, serves what the SAL still holds, in order.
                let read = matches!(req, Inbound::Read { .. });
                self.dispatch(req, true);
                if read && self.mesh.complete() {
                    break;
                }
            }
            if let Some(gathered) = self.mesh.step(&mut round) {
                return gathered;
            }
        }
    }
}
