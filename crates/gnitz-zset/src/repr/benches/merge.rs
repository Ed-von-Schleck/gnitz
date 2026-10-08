use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{make_batch, make_schema_u64_i64, pk_u64_two_i64_schema};
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// `n` sorted rows in PK groups of `dup`, distinct only in the last payload
/// column, so every compare within a group walks the whole payload.
fn bench_sorted_batch(schema: &SchemaDescriptor, n: usize, dup: usize) -> Batch {
    let last = schema.num_payload_cols() - 1;
    let mut b = BatchBuilder::new(schema);
    for i in 0..n {
        let group = (i / dup) as u128;
        b.begin_row(group, 1);
        for (pi, col) in schema.payload_columns() {
            match (pi == last, col.type_code) {
                (true, _) => b.put_int(i as u128),
                (false, TypeCode::F64) => b.put_float(group as f64),
                (false, _) => b.put_int(group),
            }
        }
        b.end_row();
    }
    b.finish()
}

/// `n` rows over [`pk_u64_two_i64_schema`], PK `key_fn(i)`, weight 1.
fn bench_flush_batch(schema: &SchemaDescriptor, n: usize, key_fn: impl Fn(usize) -> u64) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for i in 0..n {
        b.begin_row(key_fn(i) as u128, 1i64);
        b.put_int(i as i64 as u128);
        b.put_int(((i as i64) * 2) as u128);
        b.end_row();
    }
    b.finish()
}

/// A U64 PK over a float, a U128 and a nullable column, each of which forces
/// the `Generic` payload comparator.
fn generic_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    )
}

/// `run_merge`'s compare loop alone — the emit materializes nothing — under a
/// high duplicate-PK rate, over each payload comparator arm.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_merge_dup_pk_bench() {
    const K: usize = 4;
    const N: usize = 200_000;
    const DUP: usize = 8;
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    for (label, schema) in [("fixed_int", make_schema_u64_i64()), ("generic", generic_schema())] {
        let batches: Vec<Batch> = (0..K).map(|_| bench_sorted_batch(&schema, N, DUP)).collect();
        let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let merge = || {
            let mut sink = 0i64;
            run_merge(black_box(&mem), &schema, |_s, _r, w| sink = sink.wrapping_add(w));
            black_box(sink)
        };
        merge();
        let (instr, cyc) = (instructions.measure(merge).1, cycles.measure(merge).1);
        println!(
            "run_merge_dup_pk_bench {label:<9} {:5.1} instr/row, {:5.1} cycles/row",
            instr as f64 / (K * N) as f64,
            cyc as f64 / (K * N) as f64
        );
    }
}

/// `in_consolidated_order` over a batch already in order, so the pass reads
/// every row: PK groups of 1 never reach the payload order, wider groups read it
/// once per row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn in_consolidated_order_bench() {
    const N: usize = 200_000;
    let instructions = Counter::instructions();
    for (label, schema) in [
        ("fixed_int_1col", make_schema_u64_i64()),
        ("fixed_int_2col", pk_u64_two_i64_schema()),
        ("generic", generic_schema()),
    ] {
        for dup in [1usize, 8, 64] {
            let batch = bench_sorted_batch(&schema, N, dup);
            assert!(in_consolidated_order(&batch));
            let (ordered, instr) = instructions.measure(|| in_consolidated_order(black_box(&batch)));
            assert!(ordered);
            println!(
                "in_consolidated_order_bench {label:<14} dup={dup:<3} {:6.2} instr/row",
                instr as f64 / N as f64
            );
        }
    }
}

/// Instructions per call of the two Z-set sums an exchange round ends in —
/// `merge_consolidated` and `Batch::concat` — over N rows in K stripes of one
/// key space. Interleaved stripes are merged; stripes laid end to end take
/// `merge_consolidated`'s concat arm.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn striped_sum_bench() {
    let instructions = Counter::instructions();
    let schema = pk_u64_two_i64_schema();
    for (n, iters) in [(64usize, 10_000usize), (4096, 1000), (1_000_000, 10)] {
        for (k, layout) in [(4usize, "interleaved"), (16, "interleaved"), (16, "end to end")] {
            let per = n / k;
            let stripes: Vec<Batch> = (0..k)
                .map(|j| {
                    bench_flush_batch(&schema, per, |i| match layout {
                        "interleaved" => (i * k + j) as u64,
                        _ => (j * per + i) as u64,
                    })
                })
                .collect();
            let mem: Vec<MemBatch<'_>> = stripes.iter().map(|b| b.as_mem_batch()).collect();
            let rows = per * k;
            assert_eq!(merge_consolidated(&mem, &schema).count, rows, "the merge dropped rows");
            assert_eq!(Batch::concat(&schema, mem.iter().cloned()).count, rows);
            let ((), merged) = instructions.measure(|| {
                for _ in 0..iters {
                    black_box(merge_consolidated(&mem, &schema));
                }
            });
            let ((), concatenated) = instructions.measure(|| {
                for _ in 0..iters {
                    black_box(Batch::concat(&schema, mem.iter().cloned()));
                }
            });
            println!(
                "striped_sum_bench {rows} rows, {k} stripes {layout}: merge {} instr/call, concat {}",
                merged / iters as u64,
                concatenated / iters as u64
            );
        }
    }
}

/// Instructions and cycles per input row of [`Batch::merged_consolidated`], over
/// the shapes its three arms see. `shared_pk_interleave` and `shared_pk_fold`
/// put every row in an equal-PK group, the arm that folds: the first switches
/// side at every row, the second folds every row into its twin. The rest are
/// PK-disjoint and stay in the galloping arms: `alt1` at runs of one row (what a
/// set operation's hashed `_set_pk` produces), `runs4096` at runs of 4096, and
/// `dominant` with one row of the small side between each 4096 of the large.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn merged_consolidated_bench() {
    const N: usize = 500_000;
    const RUN: usize = 4096;
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    let schema = make_schema_u64_i64();
    /// Row `i` of side 0 or 1 as `(pk, payload)`, each side strictly ascending:
    /// the merge takes consolidated operands.
    type Row = fn(usize, usize) -> (usize, usize);
    // Each with the row count of side 1; side 0 holds `N`.
    let cases: [(&str, Row, usize); 5] = [
        ("shared_pk_interleave", |i, side| (i / 8, 2 * (i % 8) + side), N),
        ("shared_pk_fold", |i, _| (i / 8, i % 8), N),
        ("alt1", |i, side| (2 * i + side, i), N),
        ("runs4096", |i, side| ((2 * (i / RUN) + side) * RUN + i % RUN, i), N),
        (
            "dominant",
            |i, side| (if side == 0 { i + i / RUN } else { i * (RUN + 1) + RUN }, i),
            N / RUN,
        ),
    ];
    for (label, row, side1) in cases {
        let [a, b] = [(0, N), (1, side1)].map(|(side, n)| {
            let rows: Vec<(u64, i64, i64)> = (0..n)
                .map(|i| row(i, side))
                .map(|(pk, v)| (pk as u64, 1, v as i64))
                .collect();
            make_batch(&schema, &rows)
        });
        let rows = (N + side1) as f64;
        black_box(a.merged_consolidated(&b, &schema));
        let ((out, instr), cyc) = cycles.measure(|| instructions.measure(|| a.merged_consolidated(&b, &schema)));
        println!(
            "merged_consolidated_bench {label:<20} {:5.1} instr/row, {:5.1} cycles/row ({} rows out)",
            instr as f64 / rows,
            cyc as f64 / rows,
            out.count
        );
    }
}

/// `rows` as `(pk, weight, payload seed)`, already in (PK, payload) order,
/// certified consolidated. A nullable column is NULL in a fifth of the rows;
/// the first string column is long enough to live on the heap.
fn seeded_batch(schema: &SchemaDescriptor, rows: &[(u128, i64, u64)]) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for &(pk, weight, seed) in rows {
        b.begin_row(pk, weight);
        let mut strings = 0;
        for (_, col) in schema.payload_columns() {
            match col.type_code {
                _ if col.nullable && seed % 5 == 0 => b.put_null(),
                TypeCode::String => {
                    strings += 1;
                    match strings {
                        1 => b.put_string(&format!("{seed:028}")),
                        _ => b.put_string(&format!("s{}", seed % 100)),
                    }
                }
                TypeCode::F64 => b.put_float(seed as f64),
                TypeCode::I32 => b.put_int((seed as u32 >> 1) as u128),
                TypeCode::I16 => b.put_int((seed as u16 >> 1) as u128),
                _ => b.put_int((seed >> 1) as u128),
            }
        }
        b.end_row();
    }
    let mut b = b.finish();
    b.certify_consolidated();
    b
}

/// The two-input kernels — [`Batch::merged_consolidated`] and
/// [`merge_consolidated`] over the same pair — on the shapes their callers feed
/// them, per input row: instructions, cycles and mispredicted branches.
///
/// The keys are hashed, so stretch lengths are geometric rather than regular
/// and the merge order is unpredictable, as a scattered load's or a set
/// operation's is; `merged_consolidated_bench` is the regular counterpart. `d`
/// and `s` are a RAM tier's dominant run against the rest at three ratios, and
/// two same-sized deltas. The schemas are a narrow one, a wide mixed one whose
/// payload order is the generic arm, and a 128-bit-PK one. `disjoint` shares no
/// key, `replace` retracts and re-inserts `s / 2` of the large side's keys, and
/// `append` puts the small side above the large.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn two_way_shapes_bench() {
    let (instructions, cycles, misses) = (Counter::instructions(), Counter::cycles(), Counter::branch_misses());
    let col = SchemaColumn::new;
    let wide = SchemaDescriptor::new(
        &[
            col(TypeCode::U64, false),
            col(TypeCode::I64, false),
            col(TypeCode::I32, false),
            col(TypeCode::F64, true),
            col(TypeCode::String, false),
            col(TypeCode::I64, true),
            col(TypeCode::U128, false),
            col(TypeCode::I16, false),
            col(TypeCode::String, true),
        ],
        &[0],
    );
    let wide_pk = SchemaDescriptor::new(
        &[
            col(TypeCode::U128, false),
            col(TypeCode::I64, false),
            col(TypeCode::I64, false),
        ],
        &[0],
    );
    let sizes = [
        (200_000usize, 200_000usize),
        (200_000, 25_000),
        (1_000_000, 6_000),
        (6_000, 6_000),
        (6_000, 16),
    ];
    println!("two_way_shapes_bench            instr/row, cycles/row, branch misses/row");
    for (ty, schema) in [("narrow", make_schema_u64_i64()), ("wide", wide), ("u128 pk", wide_pk)] {
        for shape in ["disjoint", "replace", "append"] {
            for (d, s) in sizes {
                let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ (d as u64) << 20 ^ s as u64;
                let mut xorshift = move || {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state
                };
                // One bit short of the key's width, which `append` sets.
                let bits = schema.pk_stride() as u32 * 8 - 1;
                let mut key = || (((xorshift() as u128) << 64) | xorshift() as u128) >> (128 - bits);
                let mut a_keys: Vec<u128> = (0..d).map(|_| key()).collect();
                a_keys.sort_unstable();
                a_keys.dedup();
                let a_rows: Vec<(u128, i64, u64)> = a_keys.iter().map(|&k| (k, 1, 2 * k as u64)).collect();
                let b_rows: Vec<(u128, i64, u64)> = match shape {
                    "disjoint" => {
                        let mut keys: Vec<u128> = (0..s).map(|_| key()).collect();
                        keys.retain(|k| a_keys.binary_search(k).is_err());
                        keys.sort_unstable();
                        keys.dedup();
                        keys.iter().map(|&k| (k, 1, 2 * k as u64)).collect()
                    }
                    "replace" => {
                        let mut picks: Vec<usize> =
                            (0..s / 2).map(|_| (key() % a_keys.len() as u128) as usize).collect();
                        picks.sort_unstable();
                        picks.dedup();
                        let twin = |i: usize| {
                            let k = a_keys[i];
                            [(k, -1, 2 * k as u64), (k, 1, 2 * k as u64 + 2)]
                        };
                        picks.into_iter().flat_map(twin).collect()
                    }
                    _ => (0..s as u128).map(|i| ((1 << bits) + i, 1, 2 * i as u64)).collect(),
                };
                let (a, b) = (seeded_batch(&schema, &a_rows), seeded_batch(&schema, &b_rows));
                let rows = a.count + b.count;
                let calls = (400_000 / rows).max(1);
                let mem = [a.as_mem_batch(), b.as_mem_batch()];
                type Kernel<'k> = (&'k str, &'k dyn Fn() -> Batch);
                let kernels: [Kernel; 2] = [
                    ("two-way", &|| a.merged_consolidated(&b, &schema)),
                    ("n-way", &|| merge_consolidated(&mem, &schema)),
                ];
                let want = kernels[1].1();
                let mut line = format!("{ty:<7} {shape:<8} {d:>7}+{s:<6}");
                for (name, kernel) in kernels {
                    let got = kernel();
                    assert_eq!(got.pk_data(), want.pk_data(), "{name}: keys");
                    assert_eq!(got.weight_data(), want.weight_data(), "{name}: weights");
                    drop(got);
                    let run = || (0..calls).for_each(|_| drop(black_box(kernel())));
                    // The least of several runs: nothing but the kernel can lower a count.
                    let mut least = [u64::MAX; 3];
                    for _ in 0..9 {
                        let ((((), i), c), m) = misses.measure(|| cycles.measure(|| instructions.measure(run)));
                        least = [least[0].min(i), least[1].min(c), least[2].min(m)];
                    }
                    let [i, c, m] = least.map(|x| x as f64 / (rows * calls) as f64);
                    line += &format!(" | {name} {i:5.1} {c:5.1} {m:4.2}");
                }
                println!("{line}");
            }
        }
    }
}

/// Instructions per call of the two-input kernels on merges too small for
/// anything but a call's fixed cost to show: what a tick of a row or two pays.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn two_way_tiny_bench() {
    use crate::test_support::make_schema_pk_u64_payload_string;
    const CALLS: u64 = 2000;
    let instructions = Counter::instructions();
    for (ty, schema) in [
        ("ints", make_schema_u64_i64()),
        ("strings", make_schema_pk_u64_payload_string()),
    ] {
        for (d, s, shape) in [
            (1u64, 1u64, "append"),
            (15, 1, "append"),
            (6000, 15, "append"),
            (6000, 15, "spread"),
        ] {
            let rows = |keys: &mut dyn Iterator<Item = u64>| -> Vec<(u128, i64, u64)> {
                keys.map(|k| (k as u128, 1, k)).collect()
            };
            let a = seeded_batch(&schema, &rows(&mut (0..d).map(|i| 2 * i)));
            let b = match shape {
                "append" => seeded_batch(&schema, &rows(&mut (0..s).map(|i| 2 * (d + i)))),
                _ => seeded_batch(&schema, &rows(&mut (0..s).map(|i| 2 * i * (d / s) + 1))),
            };
            let mem = [a.as_mem_batch(), b.as_mem_batch()];
            let per_call = |kernel: &dyn Fn() -> Batch| {
                assert_eq!(kernel().count, (d + s) as usize);
                let ((), instr) = instructions.measure(|| (0..CALLS).for_each(|_| drop(black_box(kernel()))));
                instr / CALLS
            };
            println!(
                "two_way_tiny_bench {ty:<7} {d:>4}+{s:<2} {shape:<6} two-way {:6} instr/call, n-way {:6}",
                per_call(&|| a.merged_consolidated(&b, &schema)),
                per_call(&|| merge_consolidated(&mem, &schema)),
            );
        }
    }
}
