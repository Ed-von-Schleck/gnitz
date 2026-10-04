use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};

/// Release-only microbench for `pack_rows` in `KEY_CHUNK`-row chunks, as
/// `for_each_key` runs it, over join- and group-key shapes.
/// `REINDEX_PACK=<name>` times one shape alone, for `perf stat`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reindex_pack_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 1_000_000;
    const ITERS: usize = 20;

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
    let jmb = jb.as_mem_batch();
    let join3 = ReindexPacker::new(
        &join_schema,
        &[(1, TypeCode::U64), (2, TypeCode::U64), (3, TypeCode::U64)],
    )
    .unwrap();
    assert_eq!(join3.out_stride, 24);
    let promoted = ReindexPacker::new(&join_schema, &[(4, TypeCode::I64)]).unwrap();
    let string = ReindexPacker::new(&join_schema, &[(5, TypeCode::U128)]).unwrap();

    // --- Nullable 2-column group key: [U64 PK, I64, U32 NULL], group on (1, 2).
    let grp_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U32, true),
        ],
        &[0],
    );
    let mut gb = BatchBuilder::new(&grp_schema);
    for i in 0..N as u64 {
        gb.begin_row(i as u128, 1);
        gb.put_int((i as i64).wrapping_mul(-7) as u128);
        // Every 8th row is NULL in the nullable group column.
        match i % 8 {
            0 => gb.put_null(),
            _ => gb.put_int(i as u32 as u128),
        }
        gb.end_row();
    }
    let gb = gb.finish();
    let gmb = gb.as_mem_batch();
    let group2 = ReindexPacker::new_group_key(&grp_schema, &[1, 2], &[])
        .expect("integer group columns")
        .0;

    for (name, packer, mb) in [
        ("join3", &join3, &jmb),
        ("promoted-i32", &promoted, &jmb),
        ("string", &string, &jmb),
        ("group2-nullable", &group2, &gmb),
    ] {
        if std::env::var("REINDEX_PACK").is_ok_and(|o| o != name) {
            continue;
        }
        let stride = packer.out_stride;
        let mut buf = vec![0u8; KEY_CHUNK * stride];
        // Warm up.
        packer.pack_rows(&mut buf, stride, mb, &[(0, KEY_CHUNK)]);

        let t = Instant::now();
        let mut acc = 0u64;
        for _ in 0..ITERS {
            for start in (0..N).step_by(KEY_CHUNK) {
                let n = (N - start).min(KEY_CHUNK);
                packer.pack_rows(&mut buf, stride, mb, &[(start, start + n)]);
                acc = acc.wrapping_add(black_box(buf[0]) as u64);
            }
        }
        let secs = t.elapsed().as_secs_f64();
        println!(
            "reindex_pack_bench[{name}]: {:.1} Mrows/s ({N} rows x {ITERS} iters in {secs:.3}s, stride {stride}, checksum {acc})",
            (N * ITERS) as f64 / secs / 1e6,
        );
    }
}

/// Instructions per input row of `append_spans`, by span shape and NULL density.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn append_spans_bench() {
    const N: usize = 500_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
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
            b.begin_row(row as u128, 1);
            let v = (row as u64).wrapping_mul(2_654_435_761);
            match null_every != 0 && row % null_every == 0 {
                true => (b.put_null(), b.put_null()),
                false => (b.put_int(v as i64 as u128), b.put_int(v as u32 as u128)),
            };
            b.end_row();
        }
        let batch = b.finish();
        let mb = batch.as_mem_batch();
        for (label, cols) in [
            ("U64 PK", &[0u32][..]),
            ("I64 payload", &[1]),
            ("compound (PK, I64)", &[0, 1]),
            ("U32 payload, 8-byte slot", &[2]),
        ] {
            let spec = KeySpec::new(cols, &schema).unwrap();
            let slot = spec.key_size().next_multiple_of(8);
            let mut spans = Vec::with_capacity(N * slot);
            append_spans(&mut spans, slot, &mb, &spec, |_| true);
            spans.clear();
            let ((), instructions) = counter.measure(|| append_spans(&mut spans, slot, &mb, &spec, |_| true));
            std::hint::black_box(&spans);
            println!(
                "append_spans_bench null_every={null_every} {label:<26} {:6.1} instr/row ({} spans)",
                instructions as f64 / N as f64,
                spans.len() / slot
            );
        }
    }
}
