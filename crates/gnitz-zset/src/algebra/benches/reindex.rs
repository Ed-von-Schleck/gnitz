use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{index_spec_and_schema, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::pk_payload_schema;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

const N: usize = 500_000;

/// Instructions per row of [`ReindexPacker::for_each_key`], over join- and
/// group-key shapes.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reindex_pack_bench() {
    let counter = Counter::instructions();
    let join_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    let mut jb = BatchBuilder::new(&join_schema);
    for i in 0..N as u64 {
        jb.begin_row(i as u128, 1i64);
        jb.put_int((i.wrapping_mul(2_654_435_761)) as u128);
        jb.put_int((i.wrapping_mul(0x9E37_79B9_7F4A_7C15)) as u128);
        jb.put_int((!i) as u128);
        jb.put_int((i as i32).wrapping_mul(-3) as u32 as u128);
        jb.put_blob(format!("key-{}", i % 50_000).as_bytes());
        jb.end_row();
    }
    let jb = jb.finish();
    let join3 = ReindexPacker::new(
        &join_schema,
        &[(1, TypeCode::U64), (2, TypeCode::U64), (3, TypeCode::U64)],
    )
    .unwrap();
    let promoted = ReindexPacker::new(&join_schema, &[(4, TypeCode::I64)]).unwrap();
    let string = ReindexPacker::new(&join_schema, &[(5, TypeCode::U128)]).unwrap();

    // Group keys over [U64 PK, I64, U32 NULL, I64, I64, I64]: (1, 2) packs under a
    // bitmap, and all five overflow the PK column budget into a fold slot.
    let grp_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U32, true),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let mut gb = BatchBuilder::new(&grp_schema);
    for i in 0..N as u64 {
        gb.begin_row(i as u128, 1);
        gb.put_int((i as i64).wrapping_mul(-7) as u128);
        gb.put_opt_int((i % 8 != 0).then_some(i as u32 as u128));
        gb.put_int(i.wrapping_mul(2_654_435_761) as u128);
        gb.put_int((!i) as u128);
        gb.put_int(i as u128);
        gb.end_row();
    }
    let gb = gb.finish();
    let group = |cols: &[u32]| ReindexPacker::new_group_key(&grp_schema, cols, &[]).unwrap().0;
    let (group2, group5) = (group(&[1, 2]), group(&[1, 2, 3, 4, 5]));
    assert!(group2.packs_whole() && !group5.packs_whole());

    for (name, packer, batch) in [
        ("join3", &join3, &jb),
        ("promoted-i32", &promoted, &jb),
        ("string", &string, &jb),
        ("group2-nullable", &group2, &gb),
        ("group5-fold", &group5, &gb),
    ] {
        let mb = &batch.as_mem_batch();
        let pack = || {
            packer.for_each_key(mb, packer.out_stride, |_, key| {
                black_box(key);
            })
        };
        pack();
        let ((), instructions) = counter.measure(pack);
        println!(
            "reindex_pack_bench {name:<16} {:6.1} instr/row (stride {})",
            instructions as f64 / N as f64,
            packer.out_stride
        );
    }
}

/// Instructions per input row of a secondary index's projection — the span
/// alone ([`append_spans`]) and the whole entry ([`index_entries`]) — by span
/// shape, NULL density of the indexed columns, and width of the source PK the
/// entry ends in.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn index_entries_bench() {
    let counter = Counter::instructions();
    let measure = |label: &str, batch: &Batch, cols: &[u32]| {
        let (spec, idx_schema) = index_spec_and_schema(cols, batch.schema()).unwrap();
        let mb = batch.as_mem_batch();
        let slot = spec.key_size().next_multiple_of(8);
        let mut spans = Vec::with_capacity(N * slot);
        append_spans(&mut spans, slot, &mb, &spec, |_| true);
        spans.clear();
        let ((), span) = counter.measure(|| append_spans(&mut spans, slot, &mb, &spec, |_| true));
        black_box(&spans);
        black_box(index_entries(batch, &spec, &idx_schema));
        let (entries, entry) = counter.measure(|| index_entries(batch, &spec, &idx_schema));
        println!(
            "index_entries_bench {label:<40} spans {:5.1}, entries {:5.1} instr/row ({} entries)",
            span as f64 / N as f64,
            entry as f64 / N as f64,
            entries.len()
        );
    };
    let spread = |row: usize| (row as u64).wrapping_mul(2_654_435_761);

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U32, true),
        ],
        &[0],
    );
    for null_every in [0usize, 8, 2] {
        let mut b = BatchBuilder::new(&schema);
        for row in 0..N {
            let live = null_every == 0 || row % null_every != 0;
            b.begin_row(row as u128, 1);
            b.put_opt_int(live.then(|| spread(row) as i64 as u128));
            b.put_opt_int(live.then(|| spread(row) as u32 as u128));
            b.end_row();
        }
        let batch = b.finish();
        for (shape, cols) in [
            ("I64 payload", &[1u32][..]),
            ("U32 payload, 8-byte slot", &[2]),
            ("compound (PK, I64)", &[0, 1]),
            // No indexed column is nullable, so one density is every density.
            ("U64 PK", &[0]),
        ] {
            if cols == [0] && null_every != 0 {
                continue;
            }
            measure(&format!("{shape}, NULL every {null_every}"), &batch, cols);
        }
    }

    let wide = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let mut b = BatchBuilder::new(&wide);
    for row in 0..N {
        b.begin_row_natives(&[(row >> 8) as u128, row as u128], 1);
        b.put_int(spread(row) as i64 as u128);
        b.end_row();
    }
    measure("I64 payload, 16-byte source PK", &b.finish(), &[2]);
}
