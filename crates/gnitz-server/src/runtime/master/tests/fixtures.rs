use super::{ApplyLock, MasterDispatcher};
use crate::catalog::CatalogEngine;
use crate::runtime::reactor::test_support::reactor_with_rings;
use crate::runtime::sal::fixtures::test_log;
use crate::runtime::sync::WriteGuard;
use crate::runtime::test_support::try_poll_once;
use crate::runtime::w2m::W2mWriter;

/// A dispatcher over `catalog` and one fresh W2M ring per worker, with no
/// processes behind it, its SAL rewound as a boot leaves it, with each ring's
/// writer for a test that answers in a worker's place.
pub(crate) fn test_dispatcher(workers: usize, catalog: CatalogEngine) -> (MasterDispatcher, Vec<W2mWriter>) {
    let (reactor, writers) = reactor_with_rings(workers);
    let sal = test_log(1 << 20, workers, 1);
    let disp = MasterDispatcher::new(catalog, sal, reactor);
    (disp, writers)
}

impl MasterDispatcher {
    /// The catalog, back from the dispatcher.
    pub(crate) fn into_catalog(self) -> CatalogEngine {
        self.catalog.into_inner()
    }

    /// The apply lock, which no task of a test contends.
    pub(crate) fn alone(&self) -> WriteGuard<ApplyLock> {
        try_poll_once(self.apply_lock()).expect("uncontended")
    }
}
