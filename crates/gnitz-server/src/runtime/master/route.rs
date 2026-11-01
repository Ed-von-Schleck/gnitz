//! Fan-out by placement: a push's rows ([`with_routed`]) and a read's keys
//! ([`route_read`]), each to the workers that own them.

use std::cell::RefCell;

use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::repr::Batch;

use crate::runtime::park::WorkerSet;
use crate::runtime::sal::{GroupData, Read};
use gnitz_store::read::Probe;
use gnitz_store::relation::RelationRegistry;
use gnitz_wire::{BoundPeek, MAX_WORKERS};
use gnitz_zset::algebra::Placement;

// Reuse the row lists across calls.
thread_local! {
    static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
}

/// Route `batch`'s nonzero-weight rows to the workers `placement` names, and hand `f` the
/// group data that carries them, framed straight from `batch`.
pub(crate) fn with_routed<R>(
    batch: &Batch,
    placement: Placement,
    num_workers: usize,
    f: impl FnOnce(GroupData<'_>) -> R,
) -> R {
    // One payload for every worker is the batch itself unless it holds a
    // ghost, and then no row needs routing.
    let shared = num_workers == 1 || matches!(placement, Placement::Replicated);
    if shared && !batch.has_ghost() {
        return f(GroupData::Same(batch.wire_whole()));
    }
    // One row has one owner, named by the placement's point query: no list is built.
    if batch.len() == 1 && !batch.has_ghost() {
        if let Some(owner) = placement.owner(batch.get_pk_bytes(0), num_workers) {
            let mut each = [None; MAX_WORKERS];
            each[owner] = batch.wire_whole();
            return f(GroupData::Each(&each[..num_workers]));
        }
    }
    SCATTER_INDICES.with(|pool| {
        let mut pool = pool.borrow_mut();
        match ScatterPlan::native(placement).route(batch, &mut pool, num_workers) {
            [live] => f(GroupData::Same(batch.wire_listed(live))),
            lists => {
                let mut each = [None; MAX_WORKERS];
                for (rows, list) in each.iter_mut().zip(lists) {
                    *rows = batch.wire_listed(list);
                }
                f(GroupData::Each(&each[..lists.len()]))
            }
        }
    })
}

/// Which workers answer a read, and what each is sent: `per_worker[w]` replaces
/// the request's blob for worker `w` when present.
pub(super) struct ReadRoute {
    pub(super) set: WorkerSet,
    pub(super) per_worker: Option<Vec<Vec<u8>>>,
}

/// Which workers answer `read`, and what each is sent: a read of no live
/// relation's rows goes to every worker, any other by its relation's placement.
pub(super) fn route_read(read: &Read<'_>, registry: &RelationRegistry, nw: usize) -> ReadRoute {
    let whole = |set| ReadRoute { set, per_worker: None };
    // `own_pk`: a probe of the relation's own PK store, which every worker holds
    // whole for a replicated relation, so it spreads over all of them by the full key.
    let (tid, bound, own_pk) = match read {
        // Index entries are partitioned by their source row, not by the probed span,
        // and a span may happen to be as wide as the PK.
        Read::HasPk { tid, probe: Probe::Index(..), .. } | Read::KeySpans { tid, .. } => (*tid, None, false),
        Read::HasPk { tid, keys, .. } => (*tid, gnitz_wire::peek_bound(keys), true),
        Read::ScanSpec { tid, spec, .. } | Read::Delta { view: tid, spec, .. } => {
            (*tid, gnitz_wire::peek_bound(spec), false)
        }
        Read::Pin { .. }
        | Read::Unpin { .. }
        | Read::SnapshotPage { .. }
        | Read::Backup { .. }
        | Read::ReleasePreflight => return whole(WorkerSet::ALL),
    };
    let rel = registry
        .relation(tid)
        .unwrap_or_else(|| panic!("master: no relation for target_id={tid}"));
    let schema = rel.schema();
    let placement = match rel.placement() {
        Placement::Replicated if own_pk => Placement::full_pk(&schema),
        p => p,
    };
    let owner = |k: &[u8]| placement.owner(k, nw).expect("a key-routed placement owns every key");
    match (placement, bound) {
        (Placement::Replicated, _) => whole(WorkerSet::one(Placement::REPLICA_OWNER as usize)),
        (_, Some(BoundPeek::Range(r))) => whole(
            placement
                .confined_worker(&schema, &r, nw)
                .map_or(WorkerSet::ALL, WorkerSet::one),
        ),
        (_, Some(BoundPeek::PkSet(s))) if placement.is_key_routed() && s.stride == schema.pk_stride() => {
            let mut counts = [0u32; gnitz_wire::MAX_WORKERS];
            for k in s.keys.chunks_exact(s.stride) {
                counts[owner(k)] += 1;
            }
            let set: WorkerSet = (0..nw).filter(|&w| counts[w] > 0).collect();
            match set.len() {
                0 => whole(WorkerSet::one(0)),
                1 => whole(set),
                _ => ReadRoute {
                    set,
                    per_worker: Some(s.split(&counts[..nw], owner)),
                },
            }
        }
        _ => whole(WorkerSet::ALL),
    }
}

#[cfg(test)]
#[path = "tests/route.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/route.rs"]
mod bench;
