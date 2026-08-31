use super::*;
use gnitz_engine::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_wire::type_code;
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
