use super::*;
use crate::test_support::{col, ncol, schema};
use gnitz_core::{Batch, BatchBuilder};
use gnitz_foundation::rng::Rng;
use gnitz_wire::{OrderKey, TypeCode, WeightSlots};

/// `rows` rows of `(id U64 pk, v)`, ids ascending, each `v` written by `write` from a
/// pseudo-random word.
fn keyed(rows: usize, tc: TypeCode, write: impl Fn(&mut BatchBuilder<&mut Batch>, u64)) -> (Schema, ZSetBatch) {
    let sch = schema(vec![col("id", TypeCode::U64), ncol("v", tc)], &[0]);
    let mut b = ZSetBatch::new(&sch);
    let mut app = b.builder();
    let mut rng = Rng::new(0x9E3779B97F4A7C15);
    for i in 0..rows {
        app.begin_row(i as u128, 1);
        write(&mut app, rng.next_u64());
    }
    (sch, b)
}

/// `rows` rows of `(id U64 pk, v I64)`, the ids `w` concatenated ascending runs of
/// pseudo-random values: a multi-worker PK walk's reply, one worker after another.
fn runs(rows: usize, w: usize) -> (Schema, ZSetBatch) {
    let mut rng = Rng::new(0x9E3779B97F4A7C15);
    let mut ids: Vec<u64> = (0..rows).map(|_| rng.next_u64()).collect();
    for run in ids.chunks_mut(rows.div_ceil(w)) {
        run.sort_unstable();
    }
    let sch = schema(vec![col("id", TypeCode::U64), ncol("v", TypeCode::I64)], &[0]);
    let mut b = ZSetBatch::new(&sch);
    let mut app = b.builder();
    for (i, &id) in ids.iter().enumerate() {
        app.begin_row(id as u128, 1);
        app.put_int((i as i64) as u128);
    }
    (sch, b)
}

/// Instructions per input row of the client's sort and cut, over the leading-key shapes that
/// rank differently: an integer the image decides, few values (long ties), a float, a string
/// the first 8 bytes do and do not decide, a 16-byte integer whose high half always ties, a
/// second key, the key in and against input order, and the key as concatenated ascending
/// runs, the multi-worker PK walk's reply.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn order_slice_bench() {
    const ROWS: usize = 262_144;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let key = |col, desc| OrderKey { col, desc, nulls_first: false };
    let (by_v, by_id, by_id_desc) = ([key(1, false)], [key(0, false)], [key(0, true)]);
    let by_v_id_desc = [key(1, false), key(0, true)];
    let uncut = WeightSlots::ALL;
    let top10 = WeightSlots { skip: 0, keep: 10 };
    let ints = |card: u64| {
        keyed(ROWS, TypeCode::I64, move |app, r| {
            app.put_int((if card == 0 { r as i64 } else { (r % card) as i64 }) as u128);
        })
    };
    let float = || {
        keyed(ROWS, TypeCode::F64, |app, r| {
            app.put_float(r as i64 as f64 / 3.0);
        })
    };
    let string = |prefix: &'static str| {
        keyed(ROWS, TypeCode::String, move |app, r| {
            app.put_string(&format!("{prefix}{r:016x}"));
        })
    };
    let wide = || {
        keyed(ROWS, TypeCode::U128, |app, r| {
            app.put_int((r >> 1) as u128);
        })
    };
    for (name, (sch, b), order, slice) in [
        ("distinct I64, uncut", ints(0), &by_v[..], uncut),
        ("16 values, uncut", ints(16), &by_v[..], uncut),
        ("1024 values, uncut", ints(1024), &by_v[..], uncut),
        ("F64, uncut", float(), &by_v[..], uncut),
        ("STRING, uncut", string(""), &by_v[..], uncut),
        ("STRING, shared prefix, uncut", string("user_"), &by_v[..], uncut),
        ("U128 below 2^64, uncut", wide(), &by_v[..], uncut),
        ("16 values then id DESC, uncut", ints(16), &by_v_id_desc[..], uncut),
        ("pk ascending, uncut", ints(0), &by_id[..], uncut),
        ("pk descending, uncut", ints(0), &by_id_desc[..], uncut),
        ("pk in 4 sorted runs, uncut", runs(ROWS, 4), &by_id[..], uncut),
        ("pk in 16 sorted runs, uncut", runs(ROWS, 16), &by_id[..], uncut),
        ("distinct I64, LIMIT 10", ints(0), &by_v[..], top10),
        ("16 values, LIMIT 10", ints(16), &by_v[..], top10),
        ("pk, LIMIT 10", ints(0), &by_id[..], top10),
    ] {
        let cut = ResultCut::new(std::sync::Arc::new(sch), order, slice).unwrap();
        let (out, sort) = counter.measure(|| cut.apply(b));
        assert_eq!(out.len(), slice.keep.min(ROWS as u64) as usize, "{name}");
        std::hint::black_box(out);
        println!(
            "order_slice_bench {name:<30} {:7.1} instr/row",
            sort as f64 / ROWS as f64
        );
    }
}
