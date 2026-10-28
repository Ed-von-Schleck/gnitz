use super::MasterDispatcher;
use crate::catalog::CatalogEngine;
use crate::runtime::reactor::reactor_with_rings;
use crate::runtime::sal::fixtures::test_log;
use crate::runtime::w2m::W2mWriter;

/// A dispatcher over one fresh W2M ring per worker, its SAL rewound as a boot
/// leaves it, with each ring's writer for a test that answers in a worker's
/// place. `catalog` may be null where the path under test never calls `cat()`.
pub(super) fn test_dispatcher(
    worker_pids: Vec<i32>,
    catalog: *mut CatalogEngine,
) -> (MasterDispatcher, Vec<W2mWriter>) {
    let (reactor, writers) = reactor_with_rings(worker_pids.len());
    let sal = test_log(1 << 20, worker_pids.len(), 1);
    let disp = MasterDispatcher::new(worker_pids, catalog, sal, reactor);
    (disp, writers)
}
