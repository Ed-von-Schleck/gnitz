//! The worker's half of an exchange round: publishing its partition on the mesh
//! and waiting for every peer's, part by part.

use super::*;
use crate::query::Lane;
use crate::runtime::mesh::Round;

impl DriveHost for WorkerProcess<'_> {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        let cat = &mut *self.catalog;
        (&mut cat.dag, &mut cat.registry)
    }

    /// Publish `lanes` as this worker's rounds — one, or as few as hold them —
    /// and block until every part of each is complete. SAL groups arriving
    /// meanwhile go through [`WorkerProcess::serve_next`] with `in_drive`.
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
    /// Wait out every part of `round`, serving SAL groups meanwhile, and push
    /// each lane's rows to `gathered`: whether every worker was drained.
    fn finish_round(&mut self, round: &mut Round<'_>, gathered: &mut Vec<Batch>) -> bool {
        loop {
            self.mesh.spin_for_part(|| !self.sal_reader.is_empty());
            self.mesh
                .park()
                .park(|| self.sal_reader.is_empty() && !self.mesh.complete());
            // Once the round is complete the drive goes on; the next wait, or
            // the top level, serves what the SAL still holds, in order.
            while let Some(read) = self.serve_next(true) {
                if read && self.mesh.complete() {
                    break;
                }
            }
            if let Some(drained) = self.mesh.step(round, gathered) {
                return drained;
            }
        }
    }
}
