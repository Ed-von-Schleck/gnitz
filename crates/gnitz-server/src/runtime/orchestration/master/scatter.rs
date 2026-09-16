//! Write-path fan-out: which rows of a pushed batch go to which worker, and the
//! SAL group that emission builds.
//!
//! Routing itself is `SchemaDescriptor::worker_for_pk` — the one table-key
//! router — so there is no second placement rule here to disagree with it; what
//! this module adds is the TLS index pool and the group shape.
//!
//! Distinct from the exchange operator's routing (`ops::exchange`), which routes
//! a *join/group* key over a derived schema.

use std::cell::RefCell;

use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;

use crate::runtime::sal::{DirectGroup, GroupData};
use crate::runtime::wire::{WireData, WireSchema};

// Reuse the index scratch across calls so a steady-state push allocates nothing
// for its routing table.
thread_local! {
    static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
}

/// Route each row to its owning worker.
fn fill_scatter(batch: &Batch, schema: &SchemaDescriptor, num_workers: usize, out: &mut Vec<Vec<u32>>) {
    gnitz_store::storage::route_rows_by_pk(&batch.as_mem_batch(), schema, out, num_workers);
}

/// One list of every live row. Weight-0 rows are dropped, as in `fill_scatter`:
/// they are not Z-set elements, and a client is free to send one.
fn fill_live(batch: &Batch, out: &mut Vec<Vec<u32>>) {
    let mb = batch.as_mem_batch();
    let live = &mut gnitz_store::storage::reset_slots(out, 1)[0];
    live.extend((0..batch.len()).filter(|&i| mb.get_weight(i) != 0).map(|i| i as u32));
}

/// Per-worker row indices by the table's distribution prefix, in the TLS pool.
///
/// `f` must not call back into this module — the `SCATTER_INDICES` `RefCell` is
/// already mutably borrowed.
pub(crate) fn with_worker_indices<F, R>(batch: &Batch, schema: &SchemaDescriptor, num_workers: usize, f: F) -> R
where
    F: FnOnce(&[Vec<u32>]) -> R,
{
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        fill_scatter(batch, schema, num_workers, &mut worker_indices);
        f(&worker_indices[..num_workers])
    })
}

/// The fan-out a commit uses for `schema`: one list of the live rows, which
/// every worker is sent, for a replicated relation or a single worker; the
/// partitioned scatter otherwise.
///
/// A replicated table is sent whole because the same [`with_group`] machinery
/// then lands the whole batch in every worker's ingest + SAL slot (inheriting
/// the atomic zone, LSN, ACK accounting, and the committer's single
/// `fdatasync`), so every worker holds an identical full copy. Same TLS-pool
/// borrow contract as [`with_worker_indices`].
pub(crate) fn with_commit_indices<F, R>(batch: &Batch, schema: &SchemaDescriptor, num_workers: usize, f: F) -> R
where
    F: FnOnce(&[Vec<u32>]) -> R,
{
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        if schema.placement().is_replicated() || num_workers == 1 {
            fill_live(batch, &mut worker_indices);
            f(&worker_indices[..1])
        } else {
            fill_scatter(batch, schema, num_workers, &mut worker_indices);
            f(&worker_indices[..num_workers])
        }
    })
}

/// Build the [`DirectGroup`] an emission of `batch` under `worker_indices`
/// produces, and hand it to `f`: `base` with its template framed by `relation`,
/// and `worker_indices` either one list per worker or one list every worker is
/// sent. Here rather than on the log writer, beside the index fills it must be
/// paired with.
///
/// A schema with no German-string column scatters straight into the destination
/// slot ([`WireData::Scattered`]), skipping the intermediate `Batch`. One with a
/// German string cannot: `encode_scattered_to_wire` writes the block header and
/// directory before the scatter, so the blob region's size must be known up
/// front, and it is data-dependent — two rows sharing a source span dedup to one
/// copy. Those slots materialize a sub-`Batch` first.
pub(crate) fn with_group<R>(
    batch: &Batch,
    worker_indices: &[Vec<u32>],
    relation: &WireSchema,
    base: DirectGroup<'_>,
    f: impl FnOnce(&DirectGroup) -> R,
) -> R {
    debug_assert!(
        matches!(base.data, GroupData::Same(WireData::None)),
        "with_group replaces `data` with the per-worker slices; setting it on `base` is dead"
    );
    let schema = relation.descriptor();
    // The sub-batches must outlive the group that borrows them, so they are
    // built here rather than inside the slot fill.
    let mb = batch.as_mem_batch();
    let sub_batches: Vec<Batch> = if schema.has_german_string() {
        worker_indices
            .iter()
            .map(|indices| Batch::from_indexed_rows(&mb, indices, schema))
            .collect()
    } else {
        Vec::new()
    };
    let worker_data: Vec<WireData> = if sub_batches.is_empty() {
        worker_indices
            .iter()
            .map(|indices| WireData::Scattered { batch, indices })
            .collect()
    } else {
        sub_batches.iter().map(WireData::Whole).collect()
    };

    f(&DirectGroup {
        template: relation.frame(base.template),
        data: GroupData::of(&worker_data),
        ..base
    })
}
