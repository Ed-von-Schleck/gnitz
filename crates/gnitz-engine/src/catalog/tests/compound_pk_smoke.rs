use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
use crate::storage::Batch;

fn u64_col() -> SchemaColumn { SchemaColumn::new(type_code::U64, 0) }
fn u32_col() -> SchemaColumn { SchemaColumn::new(type_code::U32, 0) }
fn i64_col() -> SchemaColumn { SchemaColumn::new(type_code::I64, 0) }

fn compound_pk_schema_u64_u64() -> SchemaDescriptor {
    SchemaDescriptor::new(&[u64_col(), u64_col(), i64_col()], &[0, 1])
}

fn compound_pk_schema_u32_x4() -> SchemaDescriptor {
    SchemaDescriptor::new(&[u32_col(), u32_col(), u32_col(), u32_col(), i64_col()], &[0, 1, 2, 3])
}

fn make_batch_with_raw_pks(schema: &SchemaDescriptor, raw_pks: &[[u8; 16]]) -> Batch {
    let mut b = Batch::with_schema(*schema, raw_pks.len().max(1));
    for pk in raw_pks {
        b.extend_pk_bytes(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    b
}

fn make_batch_with_pks(schema: &SchemaDescriptor, keys: &[(u64, u64)]) -> Batch {
    let raw: Vec<[u8; 16]> = keys.iter().map(|&(k0, k1)| {
        let mut pk = [0u8; 16];
        pk[..8].copy_from_slice(&k0.to_le_bytes());
        pk[8..16].copy_from_slice(&k1.to_le_bytes());
        pk
    }).collect();
    make_batch_with_raw_pks(schema, &raw)
}

#[test]
fn routing_symmetry_master_worker() {
    use crate::ops::{compute_worker_indices, worker_for_partition};
    use crate::storage::partition_for_pk_bytes;

    let schema = compound_pk_schema_u64_u64();
    let pk_pairs: Vec<(u64, u64)> = (0u64..100)
        .map(|i| (i.wrapping_mul(13).wrapping_add(7), i.wrapping_mul(97).wrapping_add(11)))
        .collect();
    let batch = make_batch_with_pks(&schema, &pk_pairs);
    let num_workers = 4;

    let mut master_workers = vec![0usize; batch.count];
    for (w, row_indices) in compute_worker_indices(
        &batch, schema.pk_indices(), &schema, num_workers,
    ).into_iter().enumerate() {
        for row_idx in row_indices {
            master_workers[row_idx as usize] = w;
        }
    }

    let worker_workers: Vec<usize> = (0..batch.count)
        .map(|i| {
            let p = partition_for_pk_bytes(batch.as_mem_batch().get_pk_bytes(i));
            worker_for_partition(p, num_workers)
        })
        .collect();

    assert_eq!(
        master_workers, worker_workers,
        "master and worker routing diverged for compound U64 PK",
    );
}

#[test]
fn routing_symmetry_four_u32() {
    use crate::ops::{compute_worker_indices, worker_for_partition};
    use crate::storage::partition_for_pk_bytes;

    let schema = compound_pk_schema_u32_x4();
    let raw_pks: Vec<[u8; 16]> = (0u32..100).map(|i| {
        let mut pk = [0u8; 16];
        pk[0..4].copy_from_slice(&i.to_le_bytes());
        pk[4..8].copy_from_slice(&i.wrapping_mul(13).wrapping_add(7).to_le_bytes());
        pk[8..12].copy_from_slice(&i.wrapping_mul(97).wrapping_add(11).to_le_bytes());
        pk[12..16].copy_from_slice(&i.wrapping_mul(31).wrapping_add(3).to_le_bytes());
        pk
    }).collect();
    let b = make_batch_with_raw_pks(&schema, &raw_pks);
    let num_workers = 4;

    let mut master_workers = vec![0usize; b.count];
    for (w, row_indices) in compute_worker_indices(
        &b, schema.pk_indices(), &schema, num_workers,
    ).into_iter().enumerate() {
        for row_idx in row_indices {
            master_workers[row_idx as usize] = w;
        }
    }

    let worker_workers: Vec<usize> = (0..b.count)
        .map(|i| {
            let p = partition_for_pk_bytes(b.as_mem_batch().get_pk_bytes(i));
            worker_for_partition(p, num_workers)
        })
        .collect();

    assert_eq!(
        master_workers, worker_workers,
        "master and worker routing diverged for compound 4xU32 PK",
    );
}

#[test]
fn schema_roundtrip_catalog_preserves_pk_order() {
    let cols = vec![
        u64_col_def("payload"),
        u64_col_def("a"),
        u64_col_def("b"),
    ];
    let dir = temp_dir("cpk_pk_order_roundtrip");

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        engine.create_table("public.cpk_order", &cols, &[2, 1], true).unwrap();
        engine.close();
    }
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid = engine.get_by_name("public", "cpk_order").unwrap();
        let schema = engine.get_schema(tid).unwrap();
        assert_eq!(
            schema.pk_indices(), &[2, 1],
            "PK order (b, a) was not preserved across catalog restart",
        );
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}
