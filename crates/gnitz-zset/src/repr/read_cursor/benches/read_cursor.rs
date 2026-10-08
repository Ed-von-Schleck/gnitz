use super::*;
use crate::repr::{Batch, BatchBuilder, MappedShard, Run};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{create_read_cursor, map_shard, pk_payload_schema, pk_u64_two_i64_schema};
use gnitz_foundation::perf::Counter;
use gnitz_wire::PkKeys;
use std::hint::black_box;
use std::path::Path;
use std::rc::Rc;

/// U64 PK, an I64, a nullable I64 and a column that is a STRING or an I64.
fn mixed_schema(last: TypeCode) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(last, false),
        ],
        &[0],
    )
}

/// A cursor over several shards, drained whole and read as a band: instructions
/// and cycles per input row of `materialize`, and instructions to open on a key
/// in the middle and drain 100 rows. Four shards of long strings and NULLs
/// whose key ranges overlap, so rows fold across them; then two packed
/// fixed-width shards whose keys interleave and never meet.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn cursor_drain_bench() {
    const ROWS: usize = 1 << 18;
    let dir = tempfile::tempdir().unwrap();
    let (strings, packed) = (mixed_schema(TypeCode::String), pk_u64_two_i64_schema());
    let overlapping: Vec<Rc<MappedShard>> = (0..4)
        .map(|s| {
            let mut b = BatchBuilder::new(&strings);
            for key in (s * 100_000..).take(ROWS) {
                b.begin_row(key as u128, 1);
                b.put_int(key as u128);
                b.put_opt_int((key % 4 != 0).then_some(7 * key as u128));
                b.put_string(&format!("payload-string-value-{key}"));
                b.end_row();
            }
            map_shard(&dir.path().join(format!("strings_{s}.db")), &b.finish())
        })
        .collect();
    let interleaved: Vec<Rc<MappedShard>> = (0..2)
        .map(|s| {
            let mut b = BatchBuilder::new(&packed);
            for i in 0..2 * ROWS {
                b.begin_row((2 * i + s) as u128, 1);
                b.put_int((i % 1000) as u128);
                b.put_int(3 * i as u128);
                b.end_row();
            }
            map_shard(&dir.path().join(format!("packed_{s}.db")), &b.finish())
        })
        .collect();

    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    for (label, schema, shards) in [
        ("overlapping strings", strings, &overlapping),
        ("interleaved packed", packed, &interleaved),
    ] {
        let rows = (shards.len() * shards[0].row_count()) as f64;
        let drain = || black_box(create_read_cursor(&[], shards, schema).materialize()).count;
        // The first drain faults the shards in.
        drain();
        let (instr, cyc) = (instructions.measure(drain).1, cycles.measure(drain).1);
        let key = shards[0].get_pk_bytes(shards[0].row_count() / 2);
        let (_, band) = instructions.measure(|| {
            let runs = shards.iter().cloned().map(Run::Shard);
            black_box(from_runs_in_band(runs, schema, shards.len(), key, None).drain_chunk(100))
        });
        println!(
            "cursor_drain_bench {label}: {:.1} instr/row, {:.1} cycles/row; a band of 100 rows {band} instr",
            instr as f64 / rows,
            cyc as f64 / rows
        );
    }
}

/// Key `n` at `stride`: its big-endian image right-aligned in zero bytes, so
/// ascending in `n` at every stride.
fn adv_key(n: usize, stride: usize) -> [u8; 40] {
    let mut k = [0u8; 40];
    k[stride - 8..stride].copy_from_slice(&(n as u64).to_be_bytes());
    k
}

/// `(key, weight, payload)` rows, already in (PK, payload) order, as a
/// consolidated batch.
fn adv_batch(schema: &SchemaDescriptor, rows: impl IntoIterator<Item = (usize, i64, i64)>) -> Batch {
    let stride = schema.pk_stride();
    let mut b = BatchBuilder::new(schema);
    for (key, w, v) in rows {
        b.begin_row_bytes(&adv_key(key, stride)[..stride], w);
        b.put_int(v as u128);
        b.end_row();
    }
    let mut b = b.finish();
    b.certify_consolidated();
    b
}

/// One in-memory batch and `k` shards holding the keys `[0, total)` dealt
/// round-robin, so every source overlaps the others. A `retract_step`-th key
/// also stands in the next source at the same payload and weight -1, which
/// cancels it across sources; `retract_step = 0` cancels none. Of the others
/// every 7th stands there at another payload, a PK tie the merge breaks on the
/// payload.
fn adv_merge_cursor(dir: &Path, schema: SchemaDescriptor, total: usize, k: usize, retract_step: usize) -> ReadCursor {
    let mut rows: Vec<Vec<(usize, i64, i64)>> = vec![Vec::new(); k + 1];
    for key in 0..total {
        let (home, next) = (key % (k + 1), (key + 1) % (k + 1));
        rows[home].push((key, 1, key as i64));
        if retract_step != 0 && key % retract_step == 0 {
            rows[next].push((key, -1, key as i64));
        } else if key % 7 == 0 {
            rows[next].push((key, 1, key as i64 ^ 0x5555));
        }
    }
    // A source holds a key once, so key order is (PK, payload) order.
    rows.iter_mut().for_each(|r| r.sort_unstable_by_key(|&(key, ..)| key));
    let mut batches = rows.into_iter().map(|r| adv_batch(&schema, r));
    let delta = Rc::new(batches.next().expect("k + 1 sources"));
    let shards: Vec<Rc<MappedShard>> = batches
        .enumerate()
        .map(|(i, b)| map_shard(&dir.join(format!("{k}_{retract_step}_{i}.db")), &b))
        .collect();
    create_read_cursor(&[delta], &shards, schema)
}

/// Instructions and cycles per probe of a rewound `c` advanced through `keys`.
fn adv_cost(c: &mut ReadCursor, [instructions, cycles]: &[Counter; 2], keys: impl Iterator<Item = usize>) -> String {
    let stride = c.schema.pk_stride();
    let keys: Vec<u8> = keys.flat_map(|n| adv_key(n, stride).into_iter().take(stride)).collect();
    let [instr, cyc] = [instructions, cycles].map(|counter| {
        c.rewind();
        let sweep = || {
            for key in keys.chunks_exact(stride) {
                c.advance_to(black_box(key));
                black_box(c.current_weight);
            }
        };
        counter.measure(sweep).1 as f64 / (keys.len() / stride) as f64
    });
    format!("{instr:7.1} instr/probe, {cyc:7.1} cycles/probe")
}

/// `ReadCursor::advance_to` on each path it takes, at a stride in each PK width
/// arm and key-packing band. `merge`: ascending probes over several sources,
/// the gallop that keeps the loser tree in place — the trace probe of a join,
/// reduce or distinct — by source count, probe gap and whether half the keys
/// cancel across sources. `merge, key under the cursor`: a probe that is not
/// strictly forward, which rebuilds the tree. `single`: one source, which has no
/// tree and repositions absolutely, ascending and descending.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn advance_to_bench() {
    const TOTAL: usize = 1 << 20;
    const GAPS: [usize; 3] = [1, 128, 4096];
    let counters = [Counter::instructions(), Counter::cycles()];
    for pk in [
        &[TypeCode::U64][..],
        &[TypeCode::U64, TypeCode::U32],
        &[TypeCode::U128],
        &[TypeCode::U64; 3],
        &[TypeCode::U64; 5],
    ] {
        let schema = pk_payload_schema(pk);
        let stride = schema.pk_stride();
        let dir = tempfile::tempdir().unwrap();
        for (k, retract_step) in [(2, 0), (2, 2), (16, 0), (16, 2)] {
            let mut c = adv_merge_cursor(dir.path(), schema, TOTAL, k, retract_step);
            assert!(c.mode.is_none(), "several sources are live");
            for gap in GAPS {
                let cost = adv_cost(&mut c, &counters, (gap..TOTAL).step_by(gap));
                println!("advance_to_bench stride={stride:>2} merge K={k:>2} retract_step={retract_step} gap={gap:>4}: {cost}");
            }
            if (k, retract_step) == (16, 0) {
                // A rewound cursor stands on key 0.
                let cost = adv_cost(&mut c, &counters, std::iter::repeat_n(0, 10_000));
                println!("advance_to_bench stride={stride:>2} merge K={k:>2}, key under the cursor: {cost}");
            }
        }
        let dense = adv_batch(&schema, (0..TOTAL).map(|key| (key, 1, 0)));
        let mut c = create_read_cursor(&[], &[map_shard(&dir.path().join("single.db"), &dense)], schema);
        assert!(c.mode.is_some(), "one source is live");
        for gap in GAPS {
            let probes = (gap..TOTAL).step_by(gap);
            let (up, down) = (probes.clone(), probes.rev());
            let (up, down) = (adv_cost(&mut c, &counters, up), adv_cost(&mut c, &counters, down));
            println!("advance_to_bench stride={stride:>2} single gap={gap:>4} ascending:  {up}");
            println!("advance_to_bench stride={stride:>2} single gap={gap:>4} descending: {down}");
        }
    }
}

/// Instructions per gathered row of a `pk IN (…)` drain, over one and four
/// in-memory runs, fixed-width and string payloads, dense and sparse key lists.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_set_gather_drain_bench() {
    const N: u64 = 1 << 20;
    let counter = Counter::instructions();
    for (label, with_str) in [("3xI64", false), ("2xI64+string", true)] {
        let schema = mixed_schema(if with_str { TypeCode::String } else { TypeCode::I64 });
        for srcs in [1u64, 4] {
            let runs: Vec<Run> = (0..srcs)
                .map(|s| {
                    let mut b = BatchBuilder::new(&schema);
                    for i in (s..N).step_by(srcs as usize) {
                        b.begin_row(i as u128, 1);
                        b.put_int(i as u128);
                        match i % 7 {
                            0 => b.put_null(),
                            _ => b.put_int((i * 3) as u128),
                        }
                        match with_str {
                            true => b.put_string(&format!("gather-bench-payload-{:05}", i % 64)),
                            false => b.put_int((i * 5) as u128),
                        }
                        b.end_row();
                    }
                    let mut b = b.finish();
                    b.certify_consolidated();
                    Run::Mem(Rc::new(b))
                })
                .collect();
            for every in [1u64, 16] {
                let keys: Vec<u8> = (0..N).step_by(every as usize).flat_map(u64::to_be_bytes).collect();
                let n_keys = (keys.len() / 8) as f64;
                // The first pass takes the pool's first allocations.
                let [_, instructions] = [(); 2].map(|()| {
                    let keys = PkKeys::from_sorted(8, keys.clone());
                    let cursor = from_runs_in_band(
                        runs.iter().cloned(),
                        schema,
                        runs.len(),
                        keys.iter().next().unwrap(),
                        None,
                    );
                    let mut g = PkSetGather::over(cursor, keys);
                    let (rows, instructions) = counter.measure(|| {
                        let mut rows = 0;
                        while let Some(c) = g.drain_chunk(65_536) {
                            rows += c.len();
                        }
                        rows
                    });
                    assert_eq!(rows as f64, n_keys);
                    instructions
                });
                println!(
                    "pk-set gather {label:<13} src={srcs} every={every:<2} {:>7.1} instr/row",
                    instructions as f64 / n_keys
                );
            }
        }
    }
}
