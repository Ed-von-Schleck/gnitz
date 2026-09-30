use std::rc::Rc;

use super::*;
use crate::schema::payload_order::PayloadCmpKind;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::create_read_cursor;
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_opk, make_schema_pk_u64_payload_blob, make_schema_u128_i64,
    make_schema_u64_i64, opk_pk, pk_payload_schema, trace_cursor, u64_pk_schema,
};
use gnitz_wire::ClampKind::{Distinct, PositivePart};

/// `op_weight_clamp` of `kind`, folding its input first as the VM does.
fn op_clamp(kind: ClampKind, delta: Batch, cursor: &mut ReadCursor) -> Batch {
    op_weight_clamp(&delta.into_consolidated(), cursor, kind)
}

/// The clamp's per-element arithmetic, `clamp(w_old + Δw, 0, cap) − clamp(w_old,
/// 0, cap)`, at both kinds. Only `positive_part` ever sees a net-negative
/// pre-image.
#[test]
fn weight_clamp_emits_the_clamped_transition_at_both_kinds() {
    let schema = make_schema_u64_i64();
    // (kind, integral weight, delta weight, emitted weight; 0 = no row).
    let cases = [
        (Distinct, 0i64, 3i64, 1i64), // 0 → positive: the element enters
        (Distinct, 3, -2, 0),         // positive → positive: no transition
        (Distinct, 1, -1, -1),        // positive → 0: the element leaves
        (Distinct, 1, 1, 0),          // already a member
        (Distinct, 0, -2, 0),         // 0 → negative: nothing enters
        (PositivePart, 5, 3, 3),      // max(0,8) − max(0,5)
        (PositivePart, 8, -10, -8),   // max(0,−2) − max(0,8)
        (PositivePart, -2, 4, 2),     // a negative pre-image clamps to 0
    ];
    for (kind, w_old, w_delta, want) in cases {
        let held: &[ClampRow] = if w_old == 0 { &[] } else { &[(1, w_old, 10)] };
        let mut ch = trace_cursor(make_batch(&schema, held), schema);
        let out = op_clamp(kind, make_batch(&schema, &[(1, w_delta, 10)]), &mut ch);

        let got: Vec<i64> = (0..out.count).map(|r| out.get_weight(r)).collect();
        let want: Vec<i64> = if want == 0 { vec![] } else { vec![want] };
        assert_eq!(got, want, "{kind:?} w_old={w_old} Δ={w_delta}");
        assert!(out.is_consolidated());
    }
}

/// `(pk, weight, payload)` fixture rows, pre-sorted by `(pk, payload)`.
type ClampRow = (u64, i64, i64);

/// Naive reference: probe each delta element against the trace on its own,
/// instead of walking the two sides together.
fn naive_clamp(kind: ClampKind, delta: &[ClampRow], trace: &[ClampRow]) -> Vec<ClampRow> {
    delta
        .iter()
        .filter_map(|&(pk, dw, val)| {
            let w_old: i64 = trace
                .iter()
                .filter(|&&(p, _, v)| (p, v) == (pk, val))
                .map(|&(_, w, _)| w)
                .sum();
            let out = (w_old + dw).clamp(0, cap(kind)) - w_old.clamp(0, cap(kind));
            (out != 0).then_some((pk, val, out))
        })
        .collect()
}

/// `out` as `(pk, payload, weight)` rows.
fn rows_of(out: &Batch) -> Vec<ClampRow> {
    (0..out.count)
        .map(|r| {
            (
                out.get_pk(r) as u64,
                gnitz_wire::read_i64_le(out.col_data(0), r * 8),
                out.get_weight(r),
            )
        })
        .collect()
}

/// The trace as one cursor per drive mode: one run (single-source), and its rows
/// dealt round-robin over three runs (merge). A run of 0–1 rows leaves the
/// dealt cursor single-source too, since empty runs are dropped.
fn cursors(schema: SchemaDescriptor, trace: &[ClampRow]) -> [(&'static str, ReadCursor); 2] {
    let runs: Vec<Rc<Batch>> = deal(trace.iter().copied(), 3)
        .iter()
        .map(|rows| Rc::new(make_batch(&schema, rows)))
        .collect();
    [
        ("one run", trace_cursor(make_batch(&schema, trace), schema)),
        ("three runs", create_read_cursor(&runs, &[], schema)),
    ]
}

/// `pk` holding payloads `1..=g`, each at weight 1.
fn hot_group(pk: u64, g: i64) -> Vec<ClampRow> {
    (1..=g).map(|v| (pk, 1, v)).collect()
}

/// The walk over the shapes it has to survive, at both kinds and both drive
/// modes: empty on each side, disjoint keys, shared keys, several payloads at
/// one key, a large size skew in each direction, a hot PK group probed once and
/// several times, and a probe past every payload of its group.
#[test]
fn weight_clamp_matches_the_naive_clamp_over_every_shape() {
    let schema = make_schema_u64_i64();
    let hot = |extra: &[ClampRow]| {
        let mut rows = vec![(0, 1, 5)];
        rows.extend(hot_group(1, 64));
        rows.extend_from_slice(extra);
        rows
    };
    let cases: Vec<(Vec<ClampRow>, Vec<ClampRow>)> = vec![
        (vec![], vec![(1, 1, 10)]),
        (vec![(1, 1, 10)], vec![]),
        (vec![(1, 1, 10), (3, 1, 30)], vec![(2, 1, 20), (4, 1, 40)]),
        (vec![(1, -1, 10), (2, 1, 20)], vec![(1, 1, 10), (2, 1, 22)]),
        // several payloads at one PK against a multi-payload trace group
        (
            vec![(1, 1, 10), (1, -1, 11), (5, 1, 50)],
            vec![(1, 1, 11), (1, 1, 12), (1, 1, 13), (5, 1, 55)],
        ),
        // a retraction to zero, a bump of a member, and a new payload in one group
        (
            vec![(1, -1, 10), (1, 1, 20), (1, 1, 40)],
            vec![(1, 1, 10), (1, 1, 20), (1, 1, 30)],
        ),
        // huge delta, tiny trace
        (
            vec![(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, -1, 4), (5, 1, 5), (6, 1, 6)],
            vec![(4, 1, 4)],
        ),
        // tiny delta, huge trace
        (
            vec![(4, -1, 4)],
            vec![(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6)],
        ),
        // a negative history weight
        (vec![(1, 4, 10), (2, 1, 20)], vec![(1, -2, 10), (2, -1, 20)]),
        // hot group: retract 3, bump 40, re-add 64
        (vec![(1, -1, 3), (1, 1, 40), (1, 1, 64)], hot(&[(2, 1, 1)])),
        // hot group: several in-group probes
        (vec![(1, -1, 3), (1, 1, 17), (1, -1, 40), (1, 1, 64)], hot(&[])),
        // a probe past every payload of its group, then a higher PK
        (
            vec![(1, 1, 30), (2, -1, 5)],
            vec![(1, 1, 10), (1, 1, 20), (2, 1, 5), (3, 1, 1)],
        ),
        (vec![(1, 1, 99), (2, -1, 1)], hot(&[(2, 1, 1)])),
    ];
    for (d, t) in &cases {
        for kind in [Distinct, PositivePart] {
            for (mode, mut ch) in cursors(schema, t) {
                let out = op_clamp(kind, make_batch(&schema, d), &mut ch);
                assert!(out.is_consolidated());
                assert_eq!(
                    rows_of(&out),
                    naive_clamp(kind, d, t),
                    "{kind:?} {mode}: delta={d:?} trace={t:?}"
                );
            }
        }
    }
}

/// An element at `+1` in one run and `−1` in another is a ghost the merge folds
/// away: a delta re-adding it sees `w_old = 0`, and the element beside it is
/// still found.
#[test]
fn weight_clamp_sees_a_cross_run_ghost_as_absent() {
    let schema = make_schema_u64_i64();
    let runs = [
        Rc::new(make_batch(&schema, &[(1, 1, 10), (1, 1, 20)])),
        Rc::new(make_batch(&schema, &[(1, -1, 10), (2, 1, 5)])),
    ];
    let delta = [(1, 1, 10), (1, -1, 20), (2, -1, 5)];
    for kind in [Distinct, PositivePart] {
        let mut ch = create_read_cursor(&runs, &[], schema);
        let out = op_clamp(kind, make_batch(&schema, &delta), &mut ch);
        assert_eq!(rows_of(&out), vec![(1, 10, 1), (1, 20, -1), (2, 5, -1)], "{kind:?}");
    }
}

/// One distinct case: the delta rows, and the `(PK bytes, weight)` output.
type ProbeCase<'a> = (&'a [(&'a [u8], i64, i64)], &'a [(&'a [u8], i64)]);

/// The trace probe compares whole OPK keys, at every PK shape. `absent` always
/// sorts *below* `held`, so the group's `advance_to` parks the cursor on the
/// trace row and the key comparison — not cursor exhaustion — is what has to
/// reject it. Each pair is picked to break a probe that read the key as
/// anything but whole bytes: the 3×U64 pair shares its leading 16 bytes and
/// carries the same payload, the I64 pair straddles zero, and the U128 pair
/// sits at the extremes.
#[test]
fn distinct_probes_the_trace_by_whole_opk_keys_at_every_pk_shape() {
    let shapes: [(&str, SchemaDescriptor, &[u128], &[u128]); 5] = [
        ("u64", pk_payload_schema(&[TypeCode::U64]), &[2], &[1]),
        ("2xu64", pk_payload_schema(&[TypeCode::U64; 2]), &[2, 3], &[1, 5]),
        (
            "3xu64",
            pk_payload_schema(&[TypeCode::U64; 3]),
            &[1, 1, 1 << 56],
            &[1, 1, 2],
        ),
        ("i64", pk_payload_schema(&[TypeCode::I64]), &[2], &[-1i64 as u128]),
        ("u128", make_schema_u128_i64(), &[u128::MAX], &[0]),
    ];
    for (name, schema, held, absent) in shapes {
        let (held, absent) = (opk_pk(&schema, held), opk_pk(&schema, absent));
        // (delta rows, expected (PK bytes, weight) output).
        let cases: [ProbeCase; 4] = [
            (&[(&held, 1, 10)], &[]),               // re-add an existing element
            (&[(&held, 1, 99)], &[(&held, 1)]),     // a new payload at a held PK
            (&[(&held, -1, 10)], &[(&held, -1)]),   // full retraction
            (&[(&absent, 1, 10)], &[(&absent, 1)]), // a key the trace does not hold
        ];
        for (delta_rows, want) in cases {
            let trace = make_batch_opk(&schema, &[(&held, 1, 10)]);
            let mut ch = trace_cursor(trace, schema);
            let out = op_clamp(Distinct, make_batch_opk(&schema, delta_rows), &mut ch);

            let got: Vec<(&[u8], i64)> = (0..out.count)
                .map(|r| (out.get_pk_bytes(r), out.get_weight(r)))
                .collect();
            assert_eq!(got, want, "{name}: delta={delta_rows:?}");
        }
    }
}

/// Two elements at one PK — `mk(false)` and `mk(true)` — must compare as
/// distinct: re-adding the first transitions nothing, adding the second emits
/// `+1`.
fn assert_payload_dispatch(schema: &SchemaDescriptor, mk: impl Fn(bool) -> Batch, what: &str) {
    let mut ch = trace_cursor(mk(false), *schema);
    let out = op_clamp(Distinct, mk(false), &mut ch);
    assert_eq!(out.count, 0, "{what}: an existing element must not transition");

    let mut ch = trace_cursor(mk(false), *schema);
    let out = op_clamp(Distinct, mk(true), &mut ch);
    assert_eq!((out.count, out.get_weight(0)), (1, 1), "{what}: a new element emits +1");
}

/// The payload comparator the schema selects. The walk selects it once per
/// scan, so the two arms it can pick are the whole dispatch: a fixed-int
/// payload narrower than the `i64` the comparator reads through, and a BLOB,
/// which shares the German-string layout and so must compare as STRING does
/// rather than through the fixed-width path.
#[test]
fn distinct_compares_payloads_through_the_schema_selected_comparator() {
    let narrow = u64_pk_schema(SchemaColumn::new(TypeCode::I32, false));
    let blob = make_schema_pk_u64_payload_blob();
    assert_eq!(narrow.payload_cmp, PayloadCmpKind::FixedIntNonnull);
    assert_eq!(blob.payload_cmp, PayloadCmpKind::Generic);

    assert_payload_dispatch(
        &narrow,
        |other| make_batch(&narrow, &[(1, 1, if other { 99 } else { 42 })]),
        "i32 payload",
    );
    assert_payload_dispatch(
        &blob,
        |other| make_batch_bytes(&blob, &[(1, 1, if other { &b"bye"[..] } else { &b"hi"[..] })]),
        "blob payload",
    );
}

/// One bench shape: the trace's runs and the delta, each as `(pk, weight,
/// payload)` rows, every payload column holding the row's payload — as a
/// 40-byte heap-backed string of the row under `strings`.
struct BenchShape {
    name: String,
    payload_cols: usize,
    strings: bool,
    runs: Vec<Vec<ClampRow>>,
    delta: Vec<ClampRow>,
}

/// `rows` dealt round-robin over `n` runs.
fn deal(rows: impl IntoIterator<Item = ClampRow>, n: usize) -> Vec<Vec<ClampRow>> {
    let mut runs = vec![Vec::new(); n];
    for (k, row) in rows.into_iter().enumerate() {
        runs[k % n].push(row);
    }
    runs
}

/// A U64 PK over `n` I64 payload columns, or `n` STRING ones under `strings`.
fn bench_schema(n: usize, strings: bool) -> SchemaDescriptor {
    let tc = if strings { TypeCode::String } else { TypeCode::I64 };
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..n).map(|_| SchemaColumn::new(tc, false)));
    SchemaDescriptor::new(&cols, &[0])
}

fn bench_batch(schema: &SchemaDescriptor, rows: &[ClampRow]) -> Batch {
    let mut b = crate::storage::BatchBuilder::new(*schema);
    for &(pk, w, val) in rows {
        b.begin_row(pk as u128, w);
        for (_, col) in schema.payload_columns() {
            match col.type_code {
                TypeCode::String => b.put_string(&format!("{pk:020}-{val:019}")),
                _ => b.put_int(val as u128),
            }
        }
        b.end_row();
    }
    b.finish().into_consolidated()
}

/// `op_weight_clamp` per shape, opening its ranged cursor inside the timed region
/// as an epoch does. `#[ignore]`; run release:
///   cargo test -p gnitz-store --release weight_clamp_bench -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn weight_clamp_bench() {
    const ITERS: usize = 200;
    const N: u64 = 4096;
    let dense = |w: i64| (0..N).map(move |k| (k, w, 0));
    let hot_probes = |g: i64| (0..256).map(move |p| (1, -1, 1 + p * g / 256)).collect::<Vec<_>>();
    let mut shapes = vec![
        BenchShape {
            name: "sparse, 4 runs".into(),
            payload_cols: 1,
            strings: false,
            runs: (0..4u64)
                .map(|r| (0..16_384u64).map(|k| (k * 8 + 2 * r, 1, 0)).collect())
                .collect(),
            delta: (0..N).map(|k| (16_384 + 3 * k, 1, 0)).collect(),
        },
        BenchShape {
            name: "dense retract, 1 run".into(),
            payload_cols: 1,
            strings: false,
            runs: deal(dense(1), 1),
            delta: dense(-1).collect(),
        },
        BenchShape {
            name: "dense re-add, 1 run".into(),
            payload_cols: 1,
            strings: false,
            runs: deal(dense(1), 1),
            delta: dense(1).collect(),
        },
        BenchShape {
            name: "dense retract, 4 runs".into(),
            payload_cols: 1,
            strings: false,
            runs: deal(dense(1), 4),
            delta: dense(-1).collect(),
        },
        BenchShape {
            name: "dense retract, 4 payload cols".into(),
            payload_cols: 4,
            strings: false,
            runs: deal(dense(1), 1),
            delta: dense(-1).collect(),
        },
        BenchShape {
            name: "spread, half hits".into(),
            payload_cols: 1,
            strings: false,
            runs: deal((0..N).map(|k| (2 * k, 1, 0)), 1),
            delta: dense(-1).collect(),
        },
        BenchShape {
            name: "insert-only, all emit".into(),
            payload_cols: 1,
            strings: false,
            runs: deal((0..N).map(|k| (2 * k + 1, 1, 0)), 1),
            delta: (0..N).map(|k| (2 * k, 1, 0)).collect(),
        },
    ];
    // Every row emits a weight other than its own, so the output is copied rather
    // than handed back, with its strings.
    shapes.push(BenchShape {
        name: "insert-only at w=2, strings".into(),
        payload_cols: 1,
        strings: true,
        runs: deal((0..N).map(|k| (2 * k + 1, 1, 0)), 1),
        delta: (0..N).map(|k| (2 * k, 2, 0)).collect(),
    });
    for g in [1_000, 100_000] {
        shapes.push(BenchShape {
            name: format!("hot, 1 probe, G={g}, 4 runs"),
            payload_cols: 1,
            strings: false,
            runs: deal(hot_group(1, g), 4),
            delta: vec![(1, 1, g + 10)],
        });
    }
    for (g, n_runs) in [(1_000, 1), (1_000, 4), (100_000, 4)] {
        shapes.push(BenchShape {
            name: format!("hot, 256 probes, G={g}, {n_runs} runs"),
            payload_cols: 1,
            strings: false,
            runs: deal(hot_group(1, g), n_runs),
            delta: hot_probes(g),
        });
    }

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    for shape in shapes {
        let schema = bench_schema(shape.payload_cols, shape.strings);
        let tmp = tempfile::tempdir().unwrap();
        let mut trace = crate::test_support::scratch_table(tmp.path(), schema);
        for run in &shape.runs {
            trace.ingest_owned_batch(bench_batch(&schema, run)).unwrap();
        }
        let delta = bench_batch(&schema, &shape.delta);
        let (first, last) = (
            delta.get_pk_bytes(0).to_vec(),
            delta.get_pk_bytes(delta.count - 1).to_vec(),
        );
        let mut instructions = 0;
        for _ in 0..ITERS {
            let (out, n) = counter.measure(|| {
                let mut cursor = trace.open_cursor_in_range(&first, &last);
                op_weight_clamp(&delta, &mut cursor, Distinct)
            });
            std::hint::black_box(out);
            instructions += n;
        }
        println!(
            "op_weight_clamp {}: {} instr/iter",
            shape.name,
            instructions / ITERS as u64
        );
    }
}
