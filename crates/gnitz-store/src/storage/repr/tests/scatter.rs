use super::super::merge::mem_batch_to_unified;
use super::*;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, BatchBuilder};
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

/// Run `f` against a `write_to_batch` writer sized for `n` rows of `schema` and
/// read the batch back. Reading the PK as bytes rather than as a widened `u128`
/// is what lets one runner serve every stride.
fn scatter_into(schema: &SchemaDescriptor, n: usize, f: impl FnOnce(&mut DirectWriter)) -> Vec<Row> {
    let out = write_to_batch(schema, n, 0, f);
    (0..out.len())
        .map(|i| {
            (
                out.get_pk_bytes(i).to_vec(),
                out.get_weight(i),
                gnitz_wire::read_i64_le(out.col_data(0), i * 8),
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
/// row. The stride-24 case is the only coverage of the `PKS = 0` arm here.
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
    use crate::schema::{Placement, SchemaColumn, TypeCode};
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

// ---------------------------------------------------------------------------
// The shard-backed source shapes. `mem_batch_to_unified` is always full-stride
// and unpadded, so these build their `UnifiedSource` by hand.
// ---------------------------------------------------------------------------

/// U64 pk + two nullable I64 payload columns; slot 1 stands for an appended one.
fn two_nullable_payloads() -> SchemaDescriptor {
    use crate::schema::{SchemaColumn, TypeCode};
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    )
}

/// One row per `(pk, weight, payload_0, payload_1, null_word)`.
fn batch_with_null_words(schema: &SchemaDescriptor, rows: &[(u128, i64, i64, i64, u64)]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len());
    for &(pk, w, v0, v1, nw) in rows {
        b.extend_pk(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_col(0, &v0.to_le_bytes());
        b.extend_col(1, &v1.to_le_bytes());
        b.commit_row(nw);
    }
    b
}

/// Scatter `rows` into a `Batch`, which reads back through the ordinary accessors.
fn unified_to_batch(
    schema: &SchemaDescriptor,
    sources: &[UnifiedSource<'_>],
    cols: &[ColPtr],
    rows: &[(u32, u32, i64)],
    blob_cap: usize,
) -> Batch {
    super::super::batch::write_to_batch(schema, rows.len(), blob_cap, |w| {
        scatter_unified_sources(sources, cols, rows, w);
    })
}

/// `null_pad_mask` forces the columns a pre-`ALTER` shard has no bits for to
/// NULL: each output null word is the source row's own bits ORed with the mask.
#[test]
fn scatter_unified_sources_ors_the_null_pad_mask_into_every_row() {
    let schema = two_nullable_payloads();
    // Slot 1 is the appended column; the source's own bits name slot 0 only.
    let src = batch_with_null_words(&schema, &[(1, 1, 10, 0, 0b00), (2, 1, 20, 0, 0b01)]);
    let mb = src.as_mem_batch();
    let mut cols = Vec::new();
    let mut source = mem_batch_to_unified(&mb, &schema, &mut cols);
    source.null_pad_mask = 0b10;
    let rows: &[(u32, u32, i64)] = &[(0, 1, 1), (0, 0, 1)];

    let out = unified_to_batch(&schema, &[source], &cols, rows, 1);
    assert_eq!(out.get_null_word(0), 0b11, "source row 1's bit, plus the pad bit");
    assert_eq!(
        out.get_null_word(1),
        0b10,
        "source row 0 is NULL in the appended column only"
    );
}

/// A Constant (stride-0) PK or null_bmp region repeats its one row for every
/// output row, while the payload `ColPtr`s keep their own stride.
#[test]
fn scatter_unified_sources_repeats_a_constant_region() {
    let schema = two_nullable_payloads();
    let src = batch_with_null_words(&schema, &[(7, 1, 10, 11, 0b01), (9, 1, 20, 21, 0b10)]);
    let mb = src.as_mem_batch();
    let mut cols = Vec::new();
    let mut source = mem_batch_to_unified(&mb, &schema, &mut cols);
    let const_pk = src.get_pk_bytes(0).to_vec();
    source.pk.stride = 0;
    source.null_bmp.stride = 0;
    let rows: &[(u32, u32, i64)] = &[(0, 1, 1), (0, 0, 1)];

    let out = unified_to_batch(&schema, &[source], &cols, rows, 1);
    for row in 0..2 {
        assert_eq!(out.get_pk_bytes(row), &const_pk[..], "row {row} takes the constant key");
        assert_eq!(out.get_null_word(row), 0b01, "row {row} takes the constant null word");
    }
    assert_eq!(gnitz_wire::read_i64_le(out.col_data(0), 0), 20);
    assert_eq!(gnitz_wire::read_i64_le(out.col_data(0), 8), 10);
}

/// Both kernels relocate a German-string cell into the destination's blob heap,
/// so a value too long to inline still reads back equal.
#[test]
fn both_kernels_relocate_german_string_cells() {
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string, read_german_string};

    let schema = make_schema_pk_u64_payload_string();
    let short: &[u8] = b"tiny";
    let long: &[u8] = b"a string far past the twelve-byte inline prefix";
    let src = make_batch_bytes(&schema, &[(1, 1, short), (2, 1, long)]);
    let mb = src.as_mem_batch();

    let one = src.indexed_rows(&[1, 0]);
    assert_eq!(read_german_string(&one, 0, 0), long);
    assert_eq!(read_german_string(&one, 0, 1), short);

    let mut cols = Vec::new();
    let sources = [mem_batch_to_unified(&mb, &schema, &mut cols)];
    let rows: &[(u32, u32, i64)] = &[(0, 1, 1), (0, 0, 1)];
    let many = unified_to_batch(&schema, &sources, &cols, rows, src.blob.len());
    assert_eq!(read_german_string(&many, 0, 0), long);
    assert_eq!(read_german_string(&many, 0, 1), short);
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
