//! The mesh fixture, shared by `mesh`'s own suites, its benchmarks and the
//! worker's tests that need a live mesh.

use super::*;
use crate::query::Lane;
use crate::test_support::{certified, make_batch, make_schema_u64_i64};
use gnitz_zset::algebra::Placement;

impl Mesh {
    /// Open a round of one lane, of view `view`.
    pub(crate) fn open_one<'b>(
        &mut self,
        view: u64,
        batch: Cow<'b, Batch>,
        plan: &'b ScatterPlan,
        fold: bool,
        drained: bool,
    ) -> Round<'b> {
        self.open(std::iter::once(Lane { view, batch, plan, fold }), drained)
    }

    /// [`Self::step`] of a round of one lane: its rows, and the round's verdict.
    pub(crate) fn step_one(&mut self, round: &mut Round<'_>) -> Option<(Batch, bool)> {
        let mut rows = Vec::with_capacity(1);
        let drained = self.step(round, &mut rows)?;
        assert_eq!(rows.len(), 1, "a round of one lane");
        Some((rows.pop().expect("one lane"), drained))
    }
}

/// `nw` workers' views of one mesh of `outbox_bytes` outboxes, over parks of
/// their own.
pub(crate) fn meshes(nw: usize, outbox_bytes: usize) -> Vec<Mesh> {
    create(outbox_bytes, WorkerParks::create(nw).expect("map the parks")).expect("map the mesh")
}

/// Routing by the PK of `schema`.
pub(crate) fn by_pk(schema: &SchemaDescriptor) -> ScatterPlan {
    ScatterPlan::native(Placement::full_pk(schema))
}

/// Worker `w`'s `n` rows of round `round`, consolidated: every worker sends the
/// same elements at weight `1 + w`, so a gather must sum across senders, and a
/// block read from the wrong worker or round shows.
pub(crate) fn partition(w: usize, round: u64, n: u64) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n).map(|i| (round * n + i, 1 + w as i64, (i % 7) as i64)).collect();
    certified(make_batch(&make_schema_u64_i64(), &rows))
}
