use super::super::merge::mem_batch_to_unified;
use super::*;
use crate::schema::SchemaDescriptor;
use crate::storage::BatchBuilder;
use crate::test_support::{make_batch_opk, make_schema_u128_i64, make_schema_u64_i64, wide_pk_3xu64_schema};

/// One row read back out of a scatter destination: the PK bytes verbatim, the
/// weight, and the single I64 payload.
type Row = (Vec<u8>, i64, i64);

/// One schema per arm the scatter kernels dispatch on: the two const-width arms
/// (`PKS = 8`, `PKS = 16`) and the `PKS = 0` runtime-stride sentinel.
fn stride_cases() -> [(SchemaDescriptor, usize); 3] {
    [
        (make_schema_u64_i64(), 8),
        (make_schema_u128_i64(), 16),
        (wide_pk_3xu64_schema(), 24),
    ]
}

/// `n` distinct keys of `stride` bytes, each a single repeated byte: a kernel
/// that copied the wrong width would scramble these rather than produce a
/// plausible key. Scatter is byte-transparent, so the bytes need not be valid
/// OPK images of anything.
fn keys(stride: usize, n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| vec![0x11 * (i as u8 + 1); stride]).collect()
}

/// Run `f` against a `DirectWriter` sized for `n` rows of `schema` and read the
/// destination back. Reading the PK as bytes rather than as a widened `u128` is
/// what lets one runner serve every stride.
fn scatter_into(schema: &SchemaDescriptor, n: usize, f: impl FnOnce(&mut DirectWriter)) -> Vec<Row> {
    let stride = schema.pk_stride() as usize;
    let mut pk = vec![0u8; n * stride];
    let (mut wt, mut nb, mut col0) = (vec![0u8; n * 8], vec![0u8; n * 8], vec![0u8; n * 8]);
    let mut blob: Vec<u8> = Vec::with_capacity(1);

    let count;
    {
        let mut w = DirectWriter::new(&mut pk, &mut wt, &mut nb, vec![&mut col0], &mut blob, schema, 0);
        assert_eq!(
            usize::from(w.pk_stride),
            stride,
            "the writer must take its stride from the schema"
        );
        f(&mut w);
        count = w.row_count();
    }
    (0..count)
        .map(|i| {
            (
                pk[i * stride..(i + 1) * stride].to_vec(),
                gnitz_wire::read_i64_le(&wt, i * 8),
                gnitz_wire::read_i64_le(&col0, i * 8),
            )
        })
        .collect()
}

/// `scatter_copy` emits the selected source rows in index order, carrying each
/// row's PK bytes, weight and payload verbatim.
#[test]
fn scatter_copy_gathers_selected_rows_at_every_stride() {
    for (schema, stride) in stride_cases() {
        let k = keys(stride, 3);
        let src = make_batch_opk(&schema, &[(&k[0], 1, 100), (&k[1], 1, 200), (&k[2], 1, 300)]);
        let mb = src.as_mem_batch();

        let got = scatter_into(&schema, 2, |w| scatter_copy(&mb, &[2, 0], w));
        let want = vec![(k[2].clone(), 1, 300), (k[0].clone(), 1, 100)];
        assert_eq!(got, want, "stride {stride}");

        assert!(scatter_into(&schema, 1, |w| scatter_copy(&mb, &[], w)).is_empty());
    }
}

/// `scatter_unified_sources` emits its `(source, row, weight)` triples in list
/// order, taking each output weight from the triple rather than from the source
/// row. The stride-24 case is the only coverage of the `PKS = 0` arm here; the
/// German-string and NULL depth lives in `merge`'s materialization differential.
#[test]
fn scatter_unified_sources_emits_in_list_order_at_every_stride() {
    for (schema, stride) in stride_cases() {
        let k = keys(stride, 3);
        let src = make_batch_opk(&schema, &[(&k[0], 1, 7), (&k[1], 1, 8), (&k[2], 1, 9)]);
        let mb = src.as_mem_batch();
        let mut cols = Vec::new();
        let sources = vec![mem_batch_to_unified(&mb, &schema, &mut cols)];
        let rows: &[(u32, u32, i64)] = &[(0, 2, 5), (0, 0, -1), (0, 1, 3)];

        let got = scatter_into(&schema, 3, |w| scatter_unified_sources(&sources, &cols, rows, w));
        let want = vec![(k[2].clone(), 5, 9), (k[0].clone(), -1, 7), (k[1].clone(), 3, 8)];
        assert_eq!(got, want, "stride {stride}");
    }
}

/// Two sources through the one kernel: each source's `cols_off` must address its
/// own payload column rather than the first source's.
#[test]
fn scatter_unified_sources_addresses_each_sources_own_columns() {
    let schema = make_schema_u64_i64();
    let k = keys(8, 4);
    let s0 = make_batch_opk(&schema, &[(&k[0], 1, 10), (&k[1], 1, 20)]);
    let s1 = make_batch_opk(&schema, &[(&k[2], 1, 30), (&k[3], 1, 40)]);
    let (mb0, mb1) = (s0.as_mem_batch(), s1.as_mem_batch());
    let mut cols = Vec::new();
    let sources = vec![
        mem_batch_to_unified(&mb0, &schema, &mut cols),
        mem_batch_to_unified(&mb1, &schema, &mut cols),
    ];
    let rows: &[(u32, u32, i64)] = &[(1, 0, 1), (0, 1, 1), (0, 0, 1), (1, 1, 1)];

    let got = scatter_into(&schema, 4, |w| scatter_unified_sources(&sources, &cols, rows, w));
    let want: Vec<Row> = [(2usize, 30i64), (1, 20), (0, 10), (3, 40)]
        .iter()
        .map(|&(i, v)| (k[i].clone(), 1, v))
        .collect();
    assert_eq!(got, want);
}

/// `route_rows_by_pk` hashes only the leading distribution prefix, so rows
/// sharing it co-partition however the trailing columns differ — the property
/// `Placement::Keyed { prefix_len }` exists for. Weight-0 rows are not Z-set
/// elements and are dropped.
#[test]
fn route_rows_by_pk_follows_the_distribution_prefix() {
    use crate::schema::{type_code, Placement, SchemaColumn};
    use crate::test_support::opk_pk;
    const NW: usize = 4;

    let cols = [SchemaColumn::new(type_code::U64, 0); 2];
    let by_prefix = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 1 });
    let by_full = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 2 });

    // One `a` group spread over many `b`s, plus a weight-0 row.
    let mut bb = BatchBuilder::new(by_prefix);
    for b in 0..8u128 {
        bb.begin_row_opk(&[7, b], if b == 3 { 0 } else { 1 });
        bb.end_row();
    }
    let batch = bb.finish();

    let mut slots: Vec<Vec<u32>> = vec![Vec::new(); NW];
    route_rows_by_pk(&batch.as_mem_batch(), &by_prefix, &mut slots);
    let want = by_prefix.worker_for_pk(&opk_pk(&by_prefix, &[7, 0]), NW);
    assert_eq!(
        slots[want],
        vec![0, 1, 2, 4, 5, 6, 7],
        "one `a` group lands whole on one worker"
    );
    assert_eq!(
        slots.iter().map(Vec::len).sum::<usize>(),
        7,
        "the weight-0 row is dropped"
    );

    // Hashing the whole PK spreads that same group instead.
    let mut full_slots: Vec<Vec<u32>> = vec![Vec::new(); NW];
    route_rows_by_pk(&batch.as_mem_batch(), &by_full, &mut full_slots);
    assert!(
        full_slots.iter().filter(|s| !s.is_empty()).count() > 1,
        "the full-PK placement must not co-locate the group"
    );
}
