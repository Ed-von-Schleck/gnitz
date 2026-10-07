use super::*;
use crate::test_support::{col, ncol, schema, xorshift};
use gnitz_core::BatchAppender;
use gnitz_wire::{OrderKey, TypeCode};

/// `rows` rows of `(id U64 pk, v)`, ids ascending, each `v` written by `write` from a
/// pseudo-random word.
fn keyed(rows: usize, tc: TypeCode, write: impl Fn(&mut BatchAppender<'_>, u64)) -> (Schema, ZSetBatch) {
    let sch = schema(vec![col("id", TypeCode::U64), ncol("v", tc)], &[0]);
    let mut b = ZSetBatch::new(&sch);
    let mut app = BatchAppender::new(&mut b);
    let mut st = 0x9E3779B97F4A7C15u64;
    for i in 0..rows {
        app.add_row(i as u128, 1);
        write(&mut app, xorshift(&mut st));
    }
    (sch, b)
}

/// Instructions per input row of the client's sort and cut, over the leading-key shapes that
/// rank differently: an integer the image decides, few values (long ties), a float, a string
/// the first 8 bytes do and do not decide, a 16-byte integer whose high half always ties, a
/// second key, and the key in and against input order.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn order_window_bench() {
    const ROWS: usize = 262_144;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let key = |col, desc| OrderKey { col, desc, nulls_first: false };
    let (by_v, by_id, by_id_desc) = ([key(1, false)], [key(0, false)], [key(0, true)]);
    let by_v_id_desc = [key(1, false), key(0, true)];
    let uncut = Window { offset: 0, limit: None };
    let top10 = Window { offset: 0, limit: Some(10) };
    let ints = |card: u64| {
        keyed(ROWS, TypeCode::I64, move |app, r| {
            app.i64_val(if card == 0 { r as i64 } else { (r % card) as i64 });
        })
    };
    let float = || {
        keyed(ROWS, TypeCode::F64, |app, r| {
            app.f64_val(r as i64 as f64 / 3.0);
        })
    };
    let string = |prefix: &'static str| {
        keyed(ROWS, TypeCode::String, move |app, r| {
            app.str_val(&format!("{prefix}{r:016x}"));
        })
    };
    let wide = || {
        keyed(ROWS, TypeCode::U128, |app, r| {
            app.u128_val((r >> 1) as u128);
        })
    };
    for (name, (sch, b), order, window) in [
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
        ("distinct I64, LIMIT 10", ints(0), &by_v[..], top10),
        ("16 values, LIMIT 10", ints(16), &by_v[..], top10),
        ("pk, LIMIT 10", ints(0), &by_id[..], top10),
    ] {
        std::hint::black_box(order_and_window(&sch, b.clone(), order, window));
        // The call consumes its batch, so the clone it is handed is measured and subtracted.
        let (copy, clone) = counter.measure(|| b.clone());
        let (out, sort) = counter.measure(|| order_and_window(&sch, b.clone(), order, window));
        std::hint::black_box((copy, out));
        println!(
            "order_window_bench {name:<30} {:7.1} instr/row",
            (sort - clone) as f64 / ROWS as f64
        );
    }
}
