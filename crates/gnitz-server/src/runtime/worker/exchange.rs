//! The worker's half of an exchange round: publishing its partition on the mesh
//! and waiting for every peer's, part by part.

use super::WorkerProcess;
use crate::query::{DagEngine, DriveHost, Lane};
use crate::runtime::mesh::Round;
use gnitz_store::relation::RelationRegistry;
use gnitz_zset::repr::Batch;

impl DriveHost for WorkerProcess<'_> {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        let cat = &mut *self.catalog;
        (&mut cat.dag, &mut cat.registry)
    }

    /// Publish `lanes` as this worker's rounds — one, or as few as hold them —
    /// and block until every part of each is complete. SAL groups served
    /// meanwhile go through [`WorkerProcess::drain_sal`] with `in_drive`.
    fn exchange<'b>(
        &mut self,
        mut lanes: impl ExactSizeIterator<Item = Lane<'b>>,
        drained: bool,
        gathered: &mut Vec<Batch>,
    ) -> bool {
        let (max, mut all) = (self.mesh.max_lanes(), true);
        while lanes.len() > 0 {
            let mut round = self.mesh.open(lanes.by_ref().take(max), drained);
            all &= self.finish_round(&mut round, gathered);
        }
        all
    }
}

impl WorkerProcess<'_> {
    /// Wait out every part of `round` and push each lane's rows to `gathered`:
    /// whether every worker was drained. SAL groups arriving meanwhile are
    /// served, unless a seal of the running `Tick` group is still ahead: then
    /// the SAL waits, and every worker seals the log as far as the group.
    fn finish_round(&mut self, round: &mut Round<'_>, gathered: &mut Vec<Batch>) -> bool {
        let serves = !self.seals_ahead;
        loop {
            let sal = &self.sal_reader;
            let woken = || serves && !sal.is_empty();
            self.mesh.spin_for_part(woken);
            self.mesh.park().park(|| !woken() && !self.mesh.complete());
            // Once the round is complete the drive goes on.
            if serves {
                self.drain_sal(true);
            }
            if let Some(drained) = self.mesh.step(round, gathered) {
                return drained;
            }
        }
    }
}
