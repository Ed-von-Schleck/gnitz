use super::tests::{compute, make_schema, project, reindex_on};
use super::MapPlan;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{ColumnTable, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{pk_payload_schema, u64_pk_schema};
use gnitz_expr::{IntArithOp, LogicalInstr, LogicalProgram, Reg, Sink};
use gnitz_wire::{MapKind, NullKeys};
use std::hint::black_box;

/// Instructions per row of the map driver: `evaluate_map_batch` over whole
/// batches, per kind of map, and `append_map_ranges` over `R`-row survivor runs
/// with 16-row gaps (`*_r{R}`), on both sides of the run length below which a
/// map gathers its survivors into one range first.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn map_ranges_bench() {
    use TypeCode::{String as STR, I32, I64, U16, U64};
    const N: usize = 262_144;
    const GAP: usize = 16;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let whole = |name: &str, mut plan: MapPlan, src: &Batch| {
        black_box(plan.evaluate_map_batch(src));
        let (out, instructions) = counter.measure(|| plan.evaluate_map_batch(black_box(src)));
        black_box(out);
        println!(
            "map_ranges_bench {name:<24} {:6.1} instr/row",
            instructions as f64 / N as f64
        );
    };

    // `[U64 PK, payload...]`, every payload column nullable, and `N` rows of it:
    // integers below 1000, strings past the inline threshold so each is
    // heap-backed, and the first payload column NULL on every `null_every`-th row.
    let source = |payload: &[TypeCode], null_every: u64| {
        let schema = make_schema(&[&[U64], payload].concat());
        let mut b = BatchBuilder::new(&schema);
        for i in 0..N as u64 {
            b.begin_row(i as u128, 1);
            for (pi, &tc) in (0..).zip(payload) {
                match tc {
                    _ if pi == 0 && null_every != 0 && i % null_every == 0 => b.put_null(),
                    STR => b.put_string(&format!("row-{i:012}-payload")),
                    _ => b.put_int((i.wrapping_mul(2_654_435_761 + pi) % 1000) as u128),
                }
            }
            b.end_row();
        }
        (schema, b.finish())
    };
    let hash_row = |schema: &SchemaDescriptor, payload: &[TypeCode]| {
        let cols = (1..).zip(payload.iter().copied()).collect();
        MapPlan::from_wire(schema, &MapKind::HashRow { cols }).unwrap()
    };

    // --- Reindex on column 1, the source PK and both payload columns kept — the
    // equijoin / GROUP BY repartition shape. Column 0 is what puts a
    // `ColumnLocator::Pk` copy in the loop. Then the same map dropping NULL keys,
    // whose survivor runs shorten as the NULLs thicken.
    let reindex = |schema: &SchemaDescriptor, nulls| {
        MapPlan::from_wire(schema, &reindex_on(schema, &[1], vec![0, 1, 2], nulls)).unwrap()
    };
    let (two, two_batch) = source(&[I64, I64], 0);
    whole("reindex", reindex(&two, NullKeys::Keep), &two_batch);
    for null_every in [0, 100, 32, 16, 10] {
        let (schema, batch) = source(&[I64, I64], null_every);
        let name = format!("reindex_drop_null{null_every}");
        whole(&name, reindex(&schema, NullKeys::Drop), &batch);
    }

    // --- Hash-row maps, `[U128 hash PK, payload...]`: the set-op / DISTINCT leaf.
    // Fixed-width shapes on both sides of the fold's stack arm, and a
    // heap-backed string beside an integer.
    whole("hash_row[i64x2]", hash_row(&two, &[I64, I64]), &two_batch);
    for (name, payload) in [("hash_row[i64x10]", &[I64; 10][..]), ("hash_row[i64,str]", &[I64, STR])] {
        let (schema, batch) = source(payload, 0);
        whole(name, hash_row(&schema, payload), &batch);
    }

    // --- Projections: every payload column moved to another slot, and two
    // string columns of which both are kept, or only the first.
    let (ints, int_batch) = source(&[I64; 3], 0);
    let permute = || project(&ints, &[3, 1, 2]);
    whole("permute", permute(), &int_batch);
    let (strs, strs_batch) = source(&[STR, STR], 0);
    whole("proj_keep_str", project(&strs, &[1, 2]), &strs_batch);
    whole("proj_drop_str", project(&strs, &[1]), &strs_batch);

    // --- Computed maps: three integer columns, and a string emit that grows the
    // output blob.
    let arith = |op, a, b| LogicalInstr::IntArith { op, a: Reg(a), b: Reg(b) };
    let int3 = || {
        let prog = LogicalProgram::new(
            vec![
                LogicalInstr::LoadCol { col: 1 },
                LogicalInstr::LoadCol { col: 2 },
                LogicalInstr::LoadCol { col: 3 },
                arith(IntArithOp::Add, 0, 1),
                arith(IntArithOp::Mul, 1, 2),
                arith(IntArithOp::Sub, 2, 0),
            ],
            vec![Sink::Reg(Reg(3)), Sink::Reg(Reg(4)), Sink::Reg(Reg(5))],
            vec![],
        );
        compute(&ints, prog, &[(I64, true); 3])
    };
    let (str1, str_batch) = source(&[STR], 0);
    let upper = || {
        let prog = LogicalProgram::new(
            vec![
                LogicalInstr::LoadColStr { col: 1 },
                LogicalInstr::StrCase { a: Reg(0), upper: true },
            ],
            vec![Sink::Reg(Reg(1))],
            vec![],
        );
        compute(&str1, prog, &[(STR, true)])
    };
    whole("int3", int3(), &int_batch);
    whole("upper", upper(), &str_batch);

    // --- Column copies: one column of `[pk..., payload]` copied into an I64 slot,
    // widened from a PK or a payload cell, or decoded at its own width.
    for (name, schema, col) in [
        ("widen_pk_i32", pk_payload_schema(&[I32]), 0),
        ("widen_pk_i32_u64", pk_payload_schema(&[I32, U64]), 0),
        ("widen_payload_i32", u64_pk_schema(SchemaColumn::new(I32, false)), 1),
        ("widen_payload_u16", u64_pk_schema(SchemaColumn::new(U16, false)), 1),
        ("copy_pk_i64", pk_payload_schema(&[I64]), 0),
    ] {
        let mut batch = BatchBuilder::new(&schema);
        for i in 0..N as u64 {
            let natives: Vec<u128> = (0..schema.pk_cols().len() as u64)
                .map(|c| (i + c) as u32 as u128)
                .collect();
            batch.begin_row_natives(&natives, 1);
            batch.put_int(i.wrapping_mul(2_654_435_761) as u16 as u128);
            batch.end_row();
        }
        let plan = compute(&schema, LogicalProgram::copy_cols(&[col]), &[(I64, false)]);
        whole(name, plan, &batch.finish());
    }

    // --- Survivor runs. A map that computes, or that copies fixed-width columns
    // alone, gathers runs under 128 rows and maps longer ones in place; a
    // projection over a string column never gathers.
    for (name, mut plan, src, run) in [
        ("int3_r1", int3(), &int_batch, 1),
        ("int3_r64", int3(), &int_batch, 64),
        ("int3_r256", int3(), &int_batch, 256),
        ("upper_r1", upper(), &str_batch, 1),
        ("upper_r64", upper(), &str_batch, 64),
        ("upper_r256", upper(), &str_batch, 256),
        ("permute_r1", permute(), &int_batch, 1),
        ("permute_r16", permute(), &int_batch, 16),
        ("permute_r64", permute(), &int_batch, 64),
        ("permute_r96", permute(), &int_batch, 96),
        ("permute_r128", permute(), &int_batch, 128),
        ("permute_r256", permute(), &int_batch, 256),
        ("keep_str_r1", project(&strs, &[1, 2]), &strs_batch, 1),
        ("keep_str_r16", project(&strs, &[1, 2]), &strs_batch, 16),
        ("keep_str_r64", project(&strs, &[1, 2]), &strs_batch, 64),
        ("drop_str_r16", project(&strs, &[1]), &strs_batch, 16),
    ] {
        let ranges: Vec<(usize, usize)> = (0..N).step_by(run + GAP).map(|s| (s, (s + run).min(N))).collect();
        let mut keeper = Batch::empty_with_schema(plan.out_schema());
        plan.append_map_ranges(src, &mut keeper, &ranges);
        keeper.clear();
        let ((), instructions) = counter.measure(|| plan.append_map_ranges(black_box(src), &mut keeper, &ranges));
        println!(
            "map_ranges_bench {name:<24} {:6.1} instr/row ({} ranges)",
            instructions as f64 / keeper.count as f64,
            ranges.len()
        );
    }
}
