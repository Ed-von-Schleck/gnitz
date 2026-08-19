//! Write-path fan-out: which rows of a pushed batch go to which worker.
//!
//! Distinct from the exchange operator's routing (`ops::exchange`), which routes
//! a *join/group* key over a derived schema. This one routes by the table's
//! distribution prefix (`SchemaDescriptor::worker_for_pk`), exactly as the
//! worker-side probe does, so a pushed row lands on the worker that owns it.

use std::cell::RefCell;

use crate::schema::SchemaDescriptor;
use crate::storage::Batch;

// Reuse the index scratch across calls so a steady-state push allocates nothing
// for its routing table.
thread_local! {
    static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
}

/// Route each row to its owning worker, through the shared placement rule the
/// boot relayout also drives.
fn fill_scatter(batch: &Batch, schema: &SchemaDescriptor, num_workers: usize, out: &mut Vec<Vec<u32>>) {
    let slots = crate::ops::reset_slots(out, num_workers);
    crate::storage::route_rows_by_pk(&batch.as_mem_batch(), schema, slots);
}

/// Give every worker every row. Weight-0 rows are dropped, as in `fill_scatter`:
/// they are not Z-set elements, and a client is free to send one.
fn fill_broadcast(batch: &Batch, num_workers: usize, out: &mut Vec<Vec<u32>>) {
    let mb = batch.as_mem_batch();
    let slots = crate::ops::reset_slots(out, num_workers);
    for i in 0..batch.count {
        if mb.get_weight(i) == 0 {
            continue;
        }
        for wi in slots.iter_mut() {
            wi.push(i as u32);
        }
    }
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

/// The fan-out a commit uses for `schema`: a full broadcast for a replicated
/// relation, the partitioned scatter otherwise. One spelling of the write path's
/// shape, so the SAL fit check and the emit that follows it cannot size the same
/// batch differently.
///
/// A replicated table broadcasts because the same `scatter_wire_group(…
/// FLAG_PUSH …)` machinery then lands the whole batch in every worker's ingest +
/// SAL slot (inheriting the atomic zone, LSN, ACK accounting, and the
/// committer's single `fdatasync`), so every worker holds an identical full copy.
/// Same TLS-pool borrow contract as [`with_worker_indices`].
pub(crate) fn with_commit_indices<F, R>(batch: &Batch, schema: &SchemaDescriptor, num_workers: usize, f: F) -> R
where
    F: FnOnce(&[Vec<u32>]) -> R,
{
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        if schema.placement().is_replicated() {
            fill_broadcast(batch, num_workers, &mut worker_indices);
        } else {
            fill_scatter(batch, schema, num_workers, &mut worker_indices);
        }
        f(&worker_indices[..num_workers])
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use gnitz_wire::worker_for_pk_bytes;

    // The master-side scatter (keyed by the table's distribution prefix) and the
    // worker-side route (`worker_for_pk_bytes`) must agree on every row, or a
    // pushed row lands on a worker that never owns it. (These schemas'
    // distribution prefix is the whole PK, so the two hash domains coincide here
    // by construction.)

    fn u64_pk_col() -> SchemaColumn {
        SchemaColumn::new(type_code::U64, 0)
    }
    fn u32_pk_col() -> SchemaColumn {
        SchemaColumn::new(type_code::U32, 0)
    }
    fn i64_payload_col() -> SchemaColumn {
        SchemaColumn::new(type_code::I64, 0)
    }

    fn make_batch_with_raw_pks(schema: &SchemaDescriptor, raw_pks: &[[u8; 16]]) -> Batch {
        let mut b = Batch::with_capacity(*schema, raw_pks.len().max(1));
        for pk in raw_pks {
            b.extend_pk_bytes(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        b
    }

    /// Every row's master-assigned worker equals its worker-side route.
    fn assert_routing_symmetry(schema: &SchemaDescriptor, raw_pks: &[[u8; 16]], label: &str) {
        let batch = make_batch_with_raw_pks(schema, raw_pks);
        let num_workers = 4;

        let mut master_workers = vec![0usize; batch.count];
        with_worker_indices(&batch, schema, num_workers, |worker_indices| {
            for (w, row_indices) in worker_indices.iter().enumerate() {
                for &row_idx in row_indices {
                    master_workers[row_idx as usize] = w;
                }
            }
        });

        let worker_workers: Vec<usize> = (0..batch.count)
            .map(|i| worker_for_pk_bytes(batch.as_mem_batch().get_pk_bytes(i), num_workers))
            .collect();

        assert_eq!(
            master_workers, worker_workers,
            "master and worker routing diverged for {label}"
        );
    }

    #[test]
    fn routing_symmetry_master_worker() {
        let schema = SchemaDescriptor::new(&[u64_pk_col(), u64_pk_col(), i64_payload_col()], &[0, 1]);
        let raw_pks: Vec<[u8; 16]> = (0u64..100)
            .map(|i| {
                let mut pk = [0u8; 16];
                pk[..8].copy_from_slice(&i.wrapping_mul(13).wrapping_add(7).to_le_bytes());
                pk[8..16].copy_from_slice(&i.wrapping_mul(97).wrapping_add(11).to_le_bytes());
                pk
            })
            .collect();
        assert_routing_symmetry(&schema, &raw_pks, "compound U64 PK");
    }

    #[test]
    fn routing_symmetry_four_u32() {
        let schema = SchemaDescriptor::new(
            &[
                u32_pk_col(),
                u32_pk_col(),
                u32_pk_col(),
                u32_pk_col(),
                i64_payload_col(),
            ],
            &[0, 1, 2, 3],
        );
        let raw_pks: Vec<[u8; 16]> = (0u32..100)
            .map(|i| {
                let mut pk = [0u8; 16];
                pk[0..4].copy_from_slice(&i.to_le_bytes());
                pk[4..8].copy_from_slice(&i.wrapping_mul(13).wrapping_add(7).to_le_bytes());
                pk[8..12].copy_from_slice(&i.wrapping_mul(97).wrapping_add(11).to_le_bytes());
                pk[12..16].copy_from_slice(&i.wrapping_mul(31).wrapping_add(3).to_le_bytes());
                pk
            })
            .collect();
        assert_routing_symmetry(&schema, &raw_pks, "compound 4xU32 PK");
    }
}
