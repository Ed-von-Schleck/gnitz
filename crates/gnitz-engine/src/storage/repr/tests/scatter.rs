use super::super::batch::Batch;
use super::super::merge::mem_batch_to_unified;
use super::*;
use crate::schema::SchemaDescriptor;
use crate::test_support::{make_schema_u128_i64, wide_pk_3xu64_schema};

fn make_batch_i64(rows: &[(u128, i64, i64)]) -> Batch {
    crate::test_support::make_batch_u128_raw(&make_schema_u128_i64(), rows)
}

// -----------------------------------------------------------------------
// scatter_copy tests
// -----------------------------------------------------------------------

fn run_scatter(b: &Batch, indices: &[u32], schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
    let batch = b.as_mem_batch();
    let n = indices.len();
    let total_blob = batch.blob.len();
    let pk_stride = schema.pk_stride() as usize;
    let mut out_pk = vec![0u8; n * pk_stride];
    let mut out_weight = vec![0u8; n * 8];
    let mut out_null = vec![0u8; n * 8];
    let mut out_col0 = vec![0u8; n * 8];
    let blob_cap = if total_blob > 0 { total_blob } else { 1 };
    let mut out_blob: Vec<u8> = Vec::with_capacity(blob_cap);

    let count;
    {
        let mut writer = DirectWriter::new(
            &mut out_pk,
            &mut out_weight,
            &mut out_null,
            vec![&mut out_col0],
            &mut out_blob,
            schema,
            0,
        );
        scatter_copy(&batch, indices, &mut writer);
        count = writer.row_count();
    }

    let mut result = Vec::new();
    for i in 0..count {
        let pk = crate::test_support::read_pk_opk(&out_pk, i, pk_stride);
        let lo = pk as u64;
        let hi = (pk >> 64) as u64;
        let w = gnitz_wire::read_i64_le(&out_weight, i * 8);
        let val = gnitz_wire::read_i64_le(&out_col0, i * 8);
        result.push((lo, hi, w, val));
    }
    result
}

#[test]
fn test_scatter_basic() {
    let schema = make_schema_u128_i64();
    let b = make_batch_i64(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

    // Pick rows 2 and 0 (out of order)
    let result = run_scatter(&b, &[2, 0], &schema);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], (3, 0, 1, 30));
    assert_eq!(result[1], (1, 0, 1, 10));
}

#[test]
fn test_scatter_empty_indices() {
    let schema = make_schema_u128_i64();
    let b = make_batch_i64(&[(1, 1, 10)]);

    let result = run_scatter(&b, &[], &schema);
    assert_eq!(result.len(), 0);
}

// -----------------------------------------------------------------------
// Wide-stride scatter tests (compound PK, pk_stride = 24)
//
// Exercises the runtime-stride `PKS = 0` sentinel arm of each scatter
// helper. Builds source
// batches via `extend_pk_bytes` (the u128 path panics for stride 24) and a
// bespoke `DirectWriter` sized for 24-byte PKs (the shared `run_scatter`
// helper hardcodes 16).
// -----------------------------------------------------------------------

fn make_batch_compound_pk_24(rows: &[([u8; 24], i64, i64)]) -> Batch {
    // `empty_with_schema` installs the schema; no follow-up `set_schema`
    // call (it bakes in the single-PK invariant `num_payload + 1 ==
    // num_columns`, which doesn't hold for compound PKs).
    let schema = wide_pk_3xu64_schema();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(rows.len().max(1));
    for (pk, w, val) in rows {
        b.extend_pk_bytes(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b
}

#[test]
fn test_scatter_col_first_sentinel_wide_pk() {
    let schema = wide_pk_3xu64_schema();
    // Three source rows with distinguishable PKs and payloads.
    let pk_a = [1u8; 24];
    let pk_b = [2u8; 24];
    let pk_c = [3u8; 24];
    let b = make_batch_compound_pk_24(&[(pk_a, 1, 100), (pk_b, 1, 200), (pk_c, 1, 300)]);
    let mb = b.as_mem_batch();
    let indices: &[u32] = &[2, 0]; // reordered

    let n = indices.len();
    let mut pk = vec![0u8; n * 24];
    let mut wt = vec![0u8; n * 8];
    let mut nb = vec![0u8; n * 8];
    let mut col0 = vec![0u8; n * 8];
    let mut blob: Vec<u8> = Vec::with_capacity(1);
    {
        let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
        assert!(
            writer.pk_stride != 8 && writer.pk_stride != 16,
            "test must exercise the PKS = 0 sentinel arm",
        );
        scatter_copy(&mb, indices, &mut writer);
        assert_eq!(writer.row_count(), 2);
    }
    assert_eq!(&pk[0..24], &pk_c);
    assert_eq!(&pk[24..48], &pk_a);
    assert_eq!(gnitz_wire::read_i64_le(&wt, 0), 1);
    assert_eq!(gnitz_wire::read_i64_le(&wt, 8), 1);
    assert_eq!(gnitz_wire::read_i64_le(&col0, 0), 300);
    assert_eq!(gnitz_wire::read_i64_le(&col0, 8), 100);
}

#[test]
fn test_scatter_unified_sources_sentinel_wide_pk() {
    let schema = wide_pk_3xu64_schema();
    let pk_a = [0x11u8; 24];
    let pk_b = [0x22u8; 24];
    let pk_c = [0x33u8; 24];
    let s = make_batch_compound_pk_24(&[(pk_a, 1, 7), (pk_b, 1, 8), (pk_c, 1, 9)]);
    let mb = s.as_mem_batch();

    let mut cols = Vec::new();
    let sources = vec![mem_batch_to_unified(&mb, &schema, &mut cols)];
    // (src_idx, row_idx, weight) — explicit weights override the
    // batch-resident ones.
    let rows: &[(u32, u32, i64)] = &[(0, 2, 5), (0, 0, -1), (0, 1, 3)];

    let n = rows.len();
    let mut pk = vec![0u8; n * 24];
    let mut wt = vec![0u8; n * 8];
    let mut nb = vec![0u8; n * 8];
    let mut col0 = vec![0u8; n * 8];
    let mut blob: Vec<u8> = Vec::with_capacity(1);
    {
        let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
        assert!(
            writer.pk_stride != 8 && writer.pk_stride != 16,
            "test must exercise the PKS = 0 sentinel arm",
        );
        scatter_unified_sources(&sources, &cols, rows, &mut writer);
        assert_eq!(writer.row_count(), 3);
    }
    assert_eq!(&pk[0..24], &pk_c);
    assert_eq!(&pk[24..48], &pk_a);
    assert_eq!(&pk[48..72], &pk_b);
    let weights: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&wt, i * 8)).collect();
    assert_eq!(weights, vec![5, -1, 3]);
    let vals: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
    assert_eq!(vals, vec![9, 7, 8]);
}

// -----------------------------------------------------------------------
// Const-stride scatter pins (PKS=8 and PKS=16 arms)
//
// The wide-PK tests above force the *dynamic* arm (stride 24). All other
// set-op/union/scatter tests use a U128 PK (stride 16) only transitively;
// none exercise the literal `PKS=8` arm, and none pin the scatter kernels to
// a *specific* const width. These
// build PKs as distinguishable byte patterns (via `extend_pk_bytes`, which is
// byte-transparent — scatter copies PK bytes verbatim) and assert the exact
// PK bytes, weights, and payloads land per output row. If a const arm
// dispatched the wrong width (e.g. PKS=8 routed to `::<16>`), the per-row PK
// copy would read/write 16 bytes against an 8-byte-strided region and either
// panic on the out-of-bounds slice or scramble the PK bytes — caught here.
// -----------------------------------------------------------------------

/// Build a stride-8 `(U64 pk, I64 payload)` batch from raw 8-byte PK
/// patterns. PK bytes are stored verbatim (scatter is byte-transparent).
fn make_batch_pk8(rows: &[([u8; 8], i64, i64)]) -> Batch {
    let schema = crate::test_support::make_schema_u64_i64();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(rows.len().max(1));
    for (pk, w, val) in rows {
        b.extend_pk_bytes(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b
}

/// Build a stride-16 `(U128 pk, I64 payload)` batch from raw 16-byte PK
/// patterns.
fn make_batch_pk16(rows: &[([u8; 16], i64, i64)]) -> Batch {
    let schema = make_schema_u128_i64(); // U128 pk + I64 payload → stride 16
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(rows.len().max(1));
    for (pk, w, val) in rows {
        b.extend_pk_bytes(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b
}

/// Two `MemBatch` sources interleaved through the shared kernel: the
/// per-source `cols_off` must address each source's own payload column.
#[test]
fn test_scatter_two_mem_sources_const_pk8() {
    let schema = crate::test_support::make_schema_u64_i64(); // stride 8
    let pk_a = [0x11u8; 8];
    let pk_b = [0x22u8; 8];
    let pk_c = [0x33u8; 8];
    let pk_d = [0x44u8; 8];
    let s0 = make_batch_pk8(&[(pk_a, 1, 10), (pk_b, 1, 20)]);
    let s1 = make_batch_pk8(&[(pk_c, 1, 30), (pk_d, 1, 40)]);
    let (mb0, mb1) = (s0.as_mem_batch(), s1.as_mem_batch());
    let mut cols = Vec::new();
    let sources = vec![
        mem_batch_to_unified(&mb0, &schema, &mut cols),
        mem_batch_to_unified(&mb1, &schema, &mut cols),
    ];
    let rows: &[(u32, u32, i64)] = &[(1, 0, 1), (0, 1, 1), (0, 0, 1), (1, 1, 1)];

    let n = rows.len();
    let mut pk = vec![0u8; n * 8];
    let mut wt = vec![0u8; n * 8];
    let mut nb = vec![0u8; n * 8];
    let mut col0 = vec![0u8; n * 8];
    let mut blob: Vec<u8> = Vec::with_capacity(1);
    {
        let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
        assert_eq!(writer.pk_stride, 8, "test must exercise the const PKS=8 arm");
        scatter_unified_sources(&sources, &cols, rows, &mut writer);
        assert_eq!(writer.row_count(), 4);
    }
    // Emission order (1,0),(0,1),(0,0),(1,1) ⇒ pk_c, pk_b, pk_a, pk_d.
    assert_eq!(&pk[0..8], &pk_c);
    assert_eq!(&pk[8..16], &pk_b);
    assert_eq!(&pk[16..24], &pk_a);
    assert_eq!(&pk[24..32], &pk_d);
    let vals: Vec<i64> = (0..4).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
    assert_eq!(vals, vec![30, 20, 10, 40]);
}

#[test]
fn test_scatter_unified_sources_const_pk8() {
    let schema = crate::test_support::make_schema_u64_i64(); // stride 8
    let pk_a = [0x11u8; 8];
    let pk_b = [0x22u8; 8];
    let pk_c = [0x33u8; 8];
    let s = make_batch_pk8(&[(pk_a, 1, 7), (pk_b, 1, 8), (pk_c, 1, 9)]);
    let mb = s.as_mem_batch();

    let mut cols = Vec::new();
    let sources = vec![mem_batch_to_unified(&mb, &schema, &mut cols)];
    let rows: &[(u32, u32, i64)] = &[(0, 2, 5), (0, 0, -1), (0, 1, 3)];

    let n = rows.len();
    let mut pk = vec![0u8; n * 8];
    let mut wt = vec![0u8; n * 8];
    let mut nb = vec![0u8; n * 8];
    let mut col0 = vec![0u8; n * 8];
    let mut blob: Vec<u8> = Vec::with_capacity(1);
    {
        let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
        assert_eq!(writer.pk_stride, 8, "test must exercise the const PKS=8 arm");
        scatter_unified_sources(&sources, &cols, rows, &mut writer);
        assert_eq!(writer.row_count(), 3);
    }
    // Emission order rows[2],rows[0],rows[1] ⇒ pk_c, pk_a, pk_b.
    assert_eq!(&pk[0..8], &pk_c);
    assert_eq!(&pk[8..16], &pk_a);
    assert_eq!(&pk[16..24], &pk_b);
    let weights: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&wt, i * 8)).collect();
    assert_eq!(weights, vec![5, -1, 3]);
    let vals: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
    assert_eq!(vals, vec![9, 7, 8]);
}

#[test]
fn test_scatter_unified_sources_const_pk16() {
    let schema = make_schema_u128_i64(); // U128 pk → stride 16
    let pk_a = [0xa1u8; 16];
    let pk_b = [0xb2u8; 16];
    let pk_c = [0xc3u8; 16];
    let s = make_batch_pk16(&[(pk_a, 1, 7), (pk_b, 1, 8), (pk_c, 1, 9)]);
    let mb = s.as_mem_batch();

    let mut cols = Vec::new();
    let sources = vec![mem_batch_to_unified(&mb, &schema, &mut cols)];
    let rows: &[(u32, u32, i64)] = &[(0, 2, 5), (0, 0, -1), (0, 1, 3)];

    let n = rows.len();
    let mut pk = vec![0u8; n * 16];
    let mut wt = vec![0u8; n * 8];
    let mut nb = vec![0u8; n * 8];
    let mut col0 = vec![0u8; n * 8];
    let mut blob: Vec<u8> = Vec::with_capacity(1);
    {
        let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, &schema, 0);
        assert_eq!(writer.pk_stride, 16, "test must exercise the const PKS=16 arm");
        scatter_unified_sources(&sources, &cols, rows, &mut writer);
        assert_eq!(writer.row_count(), 3);
    }
    assert_eq!(&pk[0..16], &pk_c);
    assert_eq!(&pk[16..32], &pk_a);
    assert_eq!(&pk[32..48], &pk_b);
    let weights: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&wt, i * 8)).collect();
    assert_eq!(weights, vec![5, -1, 3]);
    let vals: Vec<i64> = (0..3).map(|i| gnitz_wire::read_i64_le(&col0, i * 8)).collect();
    assert_eq!(vals, vec![9, 7, 8]);
}
