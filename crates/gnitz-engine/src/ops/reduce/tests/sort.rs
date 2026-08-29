use super::super::super::group_key::GroupKeyCols;
use super::{argsort_by_key, argsort_delta, argsort_pk_canonical};
use crate::schema::key::compare_pk_bytes;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::Batch;

/// One batch row per supplied OPK byte vector (each must be `pk_stride` bytes),
/// weight 1, null word 0, one zeroed I64 payload column — all the PK-sort paths
/// need (they read only `pk_stride` + `get_pk_bytes`). Shared by the tests and
/// the bench.
fn build_pk_batch(schema: &SchemaDescriptor, pk_rows: &[Vec<u8>]) -> Batch {
    let mut b = Batch::with_capacity(*schema, pk_rows.len().max(1));
    for pk in pk_rows {
        b.extend_pk_bytes(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    b
}

fn schema_single_pk(pk_tc: u8) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[SchemaColumn::new(pk_tc, 0), SchemaColumn::new(type_code::I64, 0)],
        &[0],
    )
}

fn schema_3xu64_pk() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2],
    )
}

/// `argsort_pk_canonical` must reproduce the authoritative `compare_pk_bytes`
/// order. PKs are distinct, so the (unstable) sort yields a unique order and the
/// ordered key bytes compare exactly.
fn assert_canonical_order(schema: &SchemaDescriptor, pk_rows: &[Vec<u8>]) {
    let mut want: Vec<u32> = (0..pk_rows.len() as u32).collect();
    want.sort_by(|&a, &b| compare_pk_bytes(&pk_rows[a as usize], &pk_rows[b as usize]));
    let want_keys: Vec<&[u8]> = want.iter().map(|&i| pk_rows[i as usize].as_slice()).collect();

    let batch = build_pk_batch(schema, pk_rows);
    let mb = batch.as_mem_batch();
    let got = argsort_pk_canonical(&mb);
    let got_keys: Vec<&[u8]> = got.iter().map(|&i| mb.get_pk_bytes(i as usize)).collect();
    assert_eq!(got_keys, want_keys, "argsort_pk_canonical order mismatch");
}

#[test]
fn argsort_u64_arm_full_and_subwidth() {
    // stride 8 (u64 arm): high-bit / signed-OPK byte shapes must order by raw
    // unsigned byte compare (the OPK sign-flip lives in the bytes).
    assert_canonical_order(
        &schema_single_pk(type_code::U64),
        &[
            vec![0x80, 0, 0, 0, 0, 0, 0, 1],
            vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfd],
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            vec![0, 0, 0, 0, 0, 0, 0, 5],
            vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ],
    );
    // stride 4 (u64 arm, sub-width): left-align into the high bytes preserves order.
    assert_canonical_order(
        &schema_single_pk(type_code::U32),
        &[
            vec![0xff, 0, 0, 1],
            vec![0, 0, 0, 9],
            vec![0x80, 0, 0, 0],
            vec![0, 0, 0, 1],
        ],
    );
}

#[test]
fn argsort_u128_arm() {
    let mk = |hi: u8, lo: u8| {
        let mut v = vec![0u8; 16];
        v[0] = hi;
        v[15] = lo;
        v
    };
    assert_canonical_order(
        &schema_single_pk(type_code::U128),
        &[mk(0xff, 2), mk(0, 9), mk(0x80, 1), mk(0, 1), mk(0xff, 1)],
    );
}

#[test]
fn argsort_u128x2_arm_with_leading16_collision() {
    // stride 24 ([u128;2] arm). Rows sharing their leading 16 bytes and differing
    // only in the tail MUST be ordered by the second limb — a bare u128 prefix
    // would tie them and risk mis-merging distinct compound PKs (weight corruption).
    let shared = [0xab_u8; 16];
    let mk_tail = |tail: u64| {
        let mut v = Vec::with_capacity(24);
        v.extend_from_slice(&shared);
        v.extend_from_slice(&tail.to_be_bytes());
        v
    };
    let mk = |a: u64, b: u64, c: u64| {
        let mut v = Vec::with_capacity(24);
        v.extend_from_slice(&a.to_be_bytes());
        v.extend_from_slice(&b.to_be_bytes());
        v.extend_from_slice(&c.to_be_bytes());
        v
    };
    assert_canonical_order(
        &schema_3xu64_pk(),
        &[
            mk_tail(7),
            mk(0, 0, 0),
            mk_tail(2),
            mk(0xffff_ffff_ffff_ffff, 0, 0),
            mk_tail(5),
        ],
    );
}

/// Rows sharing a key come back in ascending source-index order: the whole
/// `(key, index)` pair is the sort key, so the index breaks every tie. That
/// is what makes a float SUM over a group reproducible for a fixed access path.
#[test]
fn argsort_by_key_breaks_ties_on_source_index() {
    // Three keys, four rows each, interleaved.
    let got = argsort_by_key(12, |i| i % 3);
    assert_eq!(got, vec![0, 3, 6, 9, 1, 4, 7, 10, 2, 5, 8, 11]);
    // Every row one key: the identity permutation.
    assert_eq!(argsort_by_key(5, |_| 0u64), vec![0, 1, 2, 3, 4]);
}

/// Regression guard — time `argsort_pk_canonical` over a shuffled ~1M-row batch
/// at each keyed arm. `#[ignore]`; run release:
///   cargo test -p gnitz-engine --release reduce_sort -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn reduce_sort_argsort_bench() {
    let n = 1_000_000usize;
    for &stride in &[8usize, 16, 24] {
        let schema = match stride {
            8 => schema_single_pk(type_code::U64),
            16 => schema_single_pk(type_code::U128),
            _ => schema_3xu64_pk(),
        };
        let rows: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let mut v = vec![0u8; stride];
                let mut chunk = 0usize;
                while chunk * 8 < stride {
                    let seed = (i as u64)
                        .wrapping_add((chunk as u64).wrapping_mul(0x1000))
                        .wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    let start = chunk * 8;
                    let end = (start + 8).min(stride);
                    v[start..end].copy_from_slice(&seed.to_be_bytes()[..end - start]);
                    chunk += 1;
                }
                v
            })
            .collect();
        let batch = build_pk_batch(&schema, &rows);
        let mb = batch.as_mem_batch();

        let t = std::time::Instant::now();
        let idx = argsort_pk_canonical(&mb);
        let dt = t.elapsed();
        std::hint::black_box(&idx);

        let mrps = n as f64 / dt.as_secs_f64() / 1e6;
        println!("argsort stride {stride}: {n} rows in {dt:?} = {mrps:.1} M rows/s");
    }
}

/// The group-sort sibling: `argsort_delta` over a shuffled ~1M-row batch at
/// each key arm — narrow canonical (`u64`), wide canonical (`u128`), and the
/// multi-column digest. `#[ignore]`; run release, as above.
#[test]
#[ignore]
fn reduce_sort_argsort_delta_bench() {
    let n = 1_000_000usize;
    // (label, schema, group cols) — the three arms.
    let narrow = schema_single_pk(type_code::U64);
    let wide = schema_single_pk(type_code::U128);
    let multi = schema_3xu64_pk();
    for (label, schema, group_cols) in [
        ("canonical u64", &narrow, &[0u32][..]),
        ("canonical u128", &wide, &[0u32][..]),
        ("digest 2-col", &multi, &[0u32, 1u32][..]),
    ] {
        let stride = schema.pk_stride() as usize;
        let rows: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let mut v = vec![0u8; stride];
                for chunk in 0..stride.div_ceil(8) {
                    let seed = (i as u64)
                        .wrapping_add((chunk as u64).wrapping_mul(0x1000))
                        .wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    let start = chunk * 8;
                    let end = (start + 8).min(stride);
                    v[start..end].copy_from_slice(&seed.to_be_bytes()[..end - start]);
                }
                v
            })
            .collect();
        let batch = build_pk_batch(schema, &rows);
        let mb = batch.as_mem_batch();
        let keyer = GroupKeyCols::new(schema, group_cols);

        let t = std::time::Instant::now();
        let idx = argsort_delta(&mb, &keyer);
        let dt = t.elapsed();
        std::hint::black_box(&idx);

        let mrps = n as f64 / dt.as_secs_f64() / 1e6;
        println!("argsort_delta {label}: {n} rows in {dt:?} = {mrps:.1} M rows/s");
    }
}
