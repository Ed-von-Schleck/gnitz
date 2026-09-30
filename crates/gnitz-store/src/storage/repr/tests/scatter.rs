use super::super::merge::mem_batch_to_unified;
use super::*;
use crate::schema::{SchemaDescriptor, TypeCode};
use crate::storage::{Batch, BatchBuilder};
use crate::test_support::{make_batch_opk, make_schema_u64_i64, payload0_i64, pk_payload_schema};

/// One row read back out of a scatter destination: the PK bytes verbatim, the
/// weight, and the single I64 payload.
type Row = (Vec<u8>, i64, i64);

fn rows_of(out: &Batch) -> Vec<Row> {
    (0..out.len())
        .map(|i| (out.get_pk_bytes(i).to_vec(), out.get_weight(i), payload0_i64(out, i)))
        .collect()
}

/// Both kernels at every PK width they dispatch on, with an odd row count, so
/// a narrow stride leaves the next region unaligned.
#[test]
fn both_kernels_gather_rows_at_every_pk_width() {
    use TypeCode::*;
    for pk in [&[U8][..], &[U16], &[U32], &[U64], &[U128], &[U64; 3]] {
        let schema = pk_payload_schema(pk);
        let stride = schema.pk_stride();
        // A wrong copy width scrambles a single-byte-repeated key.
        let k: Vec<Vec<u8>> = (1..=6u8).map(|i| vec![0x11 * i; stride]).collect();
        let s0 = make_batch_opk(&schema, &[(&k[0], 1, 10), (&k[1], 1, 20), (&k[2], 1, 30)]);
        let s1 = make_batch_opk(&schema, &[(&k[3], 1, 40), (&k[4], 1, 50), (&k[5], 1, 60)]);
        let (mb0, mb1) = (s0.as_mem_batch(), s1.as_mem_batch());

        let copied = write_to_batch(&schema, 3, 0, |w| scatter_copy(&mb0, &[2, 0, 1], w));
        let want = [(&k[2], 1, 30), (&k[0], 1, 10), (&k[1], 1, 20)];
        assert_eq!(
            rows_of(&copied),
            want.map(|(k, w, v)| (k.clone(), w, v)),
            "stride {stride}: copy"
        );
        assert_eq!(write_to_batch(&schema, 1, 0, |w| scatter_copy(&mb0, &[], w)).len(), 0);

        let mut cols = Vec::new();
        let sources = [
            mem_batch_to_unified(&mb0, &schema, &mut cols),
            mem_batch_to_unified(&mb1, &schema, &mut cols),
        ];
        let rows: &[(u32, u32, i64)] = &[(1, 0, 5), (0, 1, -1), (0, 0, 3)];
        let unified = write_to_batch(&schema, 3, 0, |w| scatter_unified_sources(&sources, &cols, rows, w));
        let want = [(&k[3], 5, 40), (&k[1], -1, 20), (&k[0], 3, 10)];
        assert_eq!(
            rows_of(&unified),
            want.map(|(k, w, v)| (k.clone(), w, v)),
            "stride {stride}: unified"
        );
    }
}

/// `route_rows_by_pk` hashes only the distribution prefix, so rows sharing it
/// co-partition; weight-0 rows are dropped.
#[test]
fn route_rows_by_pk_follows_the_distribution_prefix() {
    use crate::schema::{Placement, SchemaColumn};
    use crate::test_support::opk_pk;
    const NW: usize = 4;

    let cols = [SchemaColumn::new(TypeCode::U64, false); 2];
    let by_prefix = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 1 });
    let by_full = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 2 });

    // One `a` group spread over many `b`s, plus a weight-0 row.
    let mut bb = BatchBuilder::new(by_prefix);
    for b in 0..8u128 {
        bb.begin_row_opk(&[7, b], if b == 3 { 0 } else { 1 });
        bb.end_row();
    }
    let batch = bb.finish();

    let mut rows = Vec::new();
    let slots = route_rows_by_pk(&batch.as_mem_batch(), &by_prefix, &mut rows, NW);
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
    let mut full_rows = Vec::new();
    let full_slots = route_rows_by_pk(&batch.as_mem_batch(), &by_full, &mut full_rows, NW);
    assert!(
        full_slots.iter().filter(|s| !s.is_empty()).count() > 1,
        "the full-PK placement must not co-locate the group"
    );
}

/// A replicated relation is held whole by every worker, so the router sends it
/// as one slot of every live row.
#[test]
fn route_rows_by_pk_sends_a_replicated_batch_whole() {
    use crate::schema::Placement;
    const NW: usize = 4;

    let schema = make_schema_u64_i64().with_placement(Placement::Replicated);
    let mut bb = BatchBuilder::new(schema);
    for (pk, weight) in [(1, 1), (2, 0), (3, 2)] {
        bb.begin_row(pk, weight);
        bb.put_int(pk);
        bb.end_row();
    }
    let batch = bb.finish();

    let mut rows = Vec::new();
    let slots = route_rows_by_pk(&batch.as_mem_batch(), &schema, &mut rows, NW);
    assert_eq!(slots, [vec![0, 2]], "one slot of the live rows");
}

/// Carrying two whole heaps charges each source's rows the output leaves out —
/// survivors interleaved across both — and every kept string reads back.
#[test]
fn materialize_carrying_charges_each_sources_dropped_rows() {
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string, read_german_string};
    let schema = make_schema_pk_u64_payload_string();
    // Eight rows a side, one dropped from each: too little to make either
    // carried heap wasteful.
    let side = |first: u64, fill: u8, dropped: &'static [u8]| {
        let kept = [fill; 40];
        let mut rows: Vec<(u64, i64, &[u8])> = (0..8).map(|i| (first + 2 * i, 1, &kept[..])).collect();
        rows[3].2 = dropped;
        make_batch_bytes(&schema, &rows)
    };
    let (a, b) = (side(1, b'a', &[b'x'; 30]), side(2, b'b', &[b'y'; 50]));
    let sources = [a.as_mem_batch(), b.as_mem_batch()];
    let rows: Vec<(u32, u32, i64)> = (0..8)
        .filter(|&r| r != 3)
        .flat_map(|r| [(0, r, 1), (1, r, 1)])
        .collect();
    let out = materialize_carrying(&sources, &schema, &rows);
    assert_eq!(out.dead_heap, 30 + 50);
    assert_eq!(out.blob().len(), a.blob().len() + b.blob().len());
    let got: Vec<Vec<u8>> = (0..out.count).map(|row| read_german_string(&out, 0, row)).collect();
    let want: Vec<Vec<u8>> = (0..14).map(|i| vec![[b'a', b'b'][i % 2]; 40]).collect();
    assert_eq!(got, want);
}
