use super::MasterDispatcher;
use crate::catalog::CatalogEngine;
use crate::runtime::park::WorkerParks;
use crate::runtime::reactor::make_reactor_over;
use crate::runtime::sal::fixtures::test_writer;
use crate::runtime::test_support::try_poll_once;
use crate::runtime::w2m::fixtures::test_rings;
use crate::runtime::w2m::W2mWriter;

/// A dispatcher over one fresh W2M ring per worker, its SAL rewound as a boot
/// leaves it, with each ring's writer for a test that answers in a worker's
/// place. `catalog` may be null where the path under test never calls `cat()`.
pub(super) fn test_dispatcher(
    worker_pids: Vec<i32>,
    catalog: *mut CatalogEngine,
) -> (MasterDispatcher, Vec<W2mWriter>) {
    let (writers, receiver) = test_rings(&vec![64 * 1024; worker_pids.len()]);
    let parks = WorkerParks::create(worker_pids.len()).expect("map the parks");
    let sal = test_writer(1 << 20, parks);
    try_poll_once(sal.lock()).expect("uncontended").boot_rewind(1);
    let reactor = make_reactor_over(receiver);
    let disp = MasterDispatcher::new(worker_pids, catalog, sal, reactor);
    (disp, writers)
}
