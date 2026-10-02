use super::MasterDispatcher;
use crate::catalog::CatalogEngine;
use crate::runtime::reactor::make_reactor_over;
use crate::runtime::sal::fixtures::test_writer;
use crate::runtime::test_support::try_poll_once;
use crate::runtime::w2m::{W2mReceiver, W2mWriter};

/// A dispatcher over one fresh W2M ring per worker, its SAL rewound as a boot
/// leaves it, with each ring's writer for a test that answers in a worker's
/// place. `catalog` may be null where the path under test never calls `cat()`.
pub(super) fn test_dispatcher(
    worker_pids: Vec<i32>,
    catalog: *mut CatalogEngine,
) -> (MasterDispatcher, Vec<W2mWriter>) {
    let rings: Vec<*mut u8> = worker_pids
        .iter()
        .map(|_| crate::runtime::w2m::fixtures::test_ring(64 * 1024))
        .collect();
    let writers = rings.iter().map(|&p| W2mWriter::new(p)).collect();
    let sal = test_writer(1 << 20, &rings);
    try_poll_once(sal.lock()).expect("uncontended").boot_rewind(1);
    let reactor = make_reactor_over(W2mReceiver::new(rings));
    let disp = MasterDispatcher::new(worker_pids, catalog, sal, reactor);
    (disp, writers)
}
