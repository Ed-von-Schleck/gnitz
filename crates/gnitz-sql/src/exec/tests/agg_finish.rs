use super::*;
use crate::agg::{finalize_agg_bexpr, fold_partial_schema, push_agg_specs};
use crate::expr_lower::compile_scalar_evaluator;
use crate::ir::AggFunc;
use crate::test_support::col_def;
use gnitz_core::PkColumn;
use std::sync::Arc;

/// `(pk U64 | g I64 nullable | sm I16 nullable)` — one nullable group column
/// and a narrow aggregate source, so the fill exercises a NULL group value
/// and a sub-8-byte width.
fn source_schema() -> Schema {
    Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("g", TypeCode::I64, true),
            col_def("sm", TypeCode::I16, true),
        ],
        pk_cols: vec![0],
    }
}

/// Two specs so `accs` is genuinely group-major with a stride of 2:
/// `MIN(sm)` at the source's own 2-byte width, then `COUNT(*)`.
fn agg_specs() -> Vec<AggSpec> {
    vec![
        AggSpec {
            op: WireAggFunc::Min,
            col: 2,
            out_type: TypeCode::I16,
        },
        AggSpec {
            op: WireAggFunc::Count,
            col: 0,
            out_type: TypeCode::I64,
        },
    ]
}

fn partial_schema(src: &Schema, group_cols: &[usize], specs: &[AggSpec]) -> Schema {
    fold_partial_schema(src, group_cols, specs)
        .expect("the SyntheticFold layout is a valid client schema")
        .schema
}

/// The concatenated LE images of `vs` — what a `Fixed` i64 column holds.
fn le(vs: &[i64]) -> Vec<u8> {
    vs.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// A combined COUNT accumulator, in the shape `ColAcc::new` builds for a
/// `SumZero` merge.
fn count_acc(n: i64) -> ColAcc {
    ColAcc::IntSum { bits: n, seen: true }
}

/// A shape with no finalize items — enough for the two `fill_group_batch`
/// tests, which never project.
fn fill_only_shape(src: &Schema, group_positions: Vec<usize>, agg_specs: Vec<AggSpec>) -> FoldShape {
    let partial_schema = partial_schema(src, &group_positions, &agg_specs);
    FoldShape {
        reduce_schema: Arc::new(src.clone()),
        group_positions,
        agg_specs,
        pre: None,
        partial_schema: Arc::new(partial_schema),
        // `fill_group_batch` never reads the output schema; it only has to exist.
        out_schema: Schema::from_parts(vec![col_def("_agg_pk", TypeCode::U128, false).hidden()], vec![0]).unwrap(),
        having: None,
        finalize: Vec::new(),
    }
}

/// The group batch the HAVING filter and the finalize map both run over: group
/// columns copied from each representative partial row, aggregate partials taken
/// from `accs` at the declared width, and one null bit per payload slot. A wrong
/// bit here is a silently wrong HAVING verdict, not a crash.
#[test]
fn fill_group_batch_lays_out_values_and_null_bits() {
    let src = source_schema();
    let spec = fill_only_shape(&src, vec![1], agg_specs());

    // Two representative partial rows: group 0 has g = 10, group 1 has g NULL
    // (payload slot 0). Row 0 also carries the MIN winner an `Extreme` names by
    // row index. The keys are deliberately not `0, 1`, so a staging batch that
    // stamped the group ordinal instead of copying the key would not coincide.
    let mut partial = ZSetBatch::new(&spec.partial_schema);
    for (row, (key, g, sm)) in [(0x77u128, 10i64, -5i16), (0x33, 0, 0)].into_iter().enumerate() {
        partial.pks.push_u128(&spec.partial_schema, key);
        partial.weights.push(1);
        partial.nulls.push(if row == 1 { 0b1 } else { 0 });
        push_fixed_bits(&mut partial.columns[1], g as u64, 8);
        push_fixed_bits(&mut partial.columns[2], sm as u16 as u64, 2);
        push_fixed_bits(&mut partial.columns[3], 0, 8);
    }

    let reps = [Some(0usize), Some(1usize)];
    let accs = vec![
        // Group 0: MIN(sm) = -5 (partial row 0), COUNT = 2.
        ColAcc::Extreme { best: Some(0), is_max: false },
        count_acc(2),
        // Group 1: an all-NULL MIN group, COUNT = 1.
        ColAcc::Extreme { best: None, is_max: false },
        count_acc(1),
    ];

    let got = fill_group_batch(&spec, &partial, &reps, &accs);

    assert_eq!(got.len(), 2);
    assert_eq!(got.weights, vec![1, 1]);
    // Slot 0 = g, slot 1 = MIN(sm), slot 2 = COUNT. Group 1 nulls g and MIN.
    assert_eq!(got.nulls, vec![0, 0b011]);
    // `_group_pk` is the representative row's key, copied.
    assert_eq!(got.pks, PkColumn::from_natives(&spec.partial_schema, [0x77, 0x33]));
    // g: the copied value, then `push_null`'s zero filler.
    assert_eq!(
        got.columns[1],
        10i64.to_le_bytes().iter().chain(&[0; 8]).copied().collect::<Vec<_>>()
    );
    // MIN(sm) truncated to its declared 2 bytes, then a zeroed NULL cell.
    assert_eq!(got.columns[2], [0xfb, 0xff, 0, 0]);
    // COUNT is never NULL.
    assert_eq!(
        got.columns[3],
        2i64.to_le_bytes()
            .iter()
            .chain(&1i64.to_le_bytes())
            .copied()
            .collect::<Vec<_>>()
    );
}

/// The global ground row: no group columns, every aggregate uncontributed.
/// `reps[0] == None` must not be dereferenced.
#[test]
fn fill_group_batch_handles_the_global_ground_row() {
    let src = source_schema();
    let spec = fill_only_shape(&src, vec![], agg_specs());
    let empty = ZSetBatch::new(&spec.partial_schema);
    let accs = vec![ColAcc::Extreme { best: None, is_max: false }, count_acc(0)];
    let got = fill_group_batch(&spec, &empty, &[None], &accs);

    assert_eq!(got.len(), 1);
    // Slot 0 = MIN (NULL), slot 1 = COUNT (0, never NULL).
    assert_eq!(got.nulls, vec![0b01]);
    assert_eq!(got.columns[2], 0i64.to_le_bytes());
}

/// The shape the fold path builds for one direct COUNT — `SELECT g, COUNT(*) …
/// GROUP BY g` at `group_positions = [1]`, `SELECT COUNT(*)` at `[]`. Both
/// finalize items are pass-throughs: a group column, and a `Direct` aggregate
/// whose finalize composite is its own raw column.
fn count_shape(src: &Schema, group_positions: Vec<usize>) -> FoldShape {
    let grouped = !group_positions.is_empty();
    let agg_specs = vec![AggSpec {
        op: WireAggFunc::Count,
        col: 0,
        out_type: TypeCode::I64,
    }];
    let partial_schema = partial_schema(src, &group_positions, &agg_specs);
    // `[_group_pk | g? | COUNT]` — the aggregate trails the group columns.
    let count_ci = 1 + group_positions.len();
    let finalize: Vec<FinalizeItem> = grouped
        .then_some(FinalizeItem::PassThrough { partial_ci: 1 })
        .into_iter()
        .chain([FinalizeItem::PassThrough { partial_ci: count_ci }])
        .collect();
    let out_cols: Vec<_> = grouped
        .then(|| col_def("g", TypeCode::I64, true))
        .into_iter()
        .chain([col_def("c", TypeCode::I64, false)])
        .collect();
    FoldShape {
        reduce_schema: Arc::new(src.clone()),
        group_positions,
        agg_specs,
        pre: None,
        out_schema: build_agg_out_schema(out_cols.clone()).unwrap().0,
        partial_schema: Arc::new(partial_schema),
        having: None,
        finalize,
    }
}

/// The partial mirrors what `fetch_bound` concatenates: replies in worker
/// order, so a group's rows split across two workers and the keys arrive
/// descending — the emission ordinal and the group key then disagree on both
/// order and value, and only the key is a function of the data.
#[test]
fn the_output_row_carries_the_engine_group_key() {
    let src = source_schema();
    let spec = count_shape(&src, vec![1]);

    // (group key, g, this worker's COUNT partial). Worker 0 emits groups
    // 0x2222 then 0x1111; worker 1 emits the rest of 0x1111.
    let mut partial = ZSetBatch::new(&spec.partial_schema);
    for (key, g, n) in [(0x2222u128, 20i64, 1i64), (0x1111, 10, 2), (0x1111, 10, 3)] {
        partial.pks.push_u128(&spec.partial_schema, key);
        partial.weights.push(1);
        partial.nulls.push(0);
        push_fixed_bits(&mut partial.columns[1], g as u64, 8);
        push_fixed_bits(&mut partial.columns[2], n as u64, 8);
    }

    let got = agg_finish(&spec, &partial);

    assert_eq!(got.len(), 2);
    // First-encounter ordinals would be `[0, 1]`; the group keys are not.
    assert_eq!(got.pks, PkColumn::from_natives(&spec.partial_schema, [0x2222, 0x1111]));
    // The values stay paired with their keys: 0x2222 is g = 20 / COUNT 1,
    // 0x1111 is g = 10 / COUNT 2 + 3.
    assert_eq!(got.columns[1], le(&[20, 10]));
    assert_eq!(got.columns[2], le(&[1, 5]));
}

/// The synthesized global ground row has no partial to copy a key from, so
/// it takes V₀ — the same key the engine stamps on the ground row it emits
/// when a worker did contribute one, so one logical row has one key either
/// way.
#[test]
fn the_global_ground_row_is_keyed_at_v0() {
    let src = source_schema();
    let spec = count_shape(&src, vec![]);
    let empty = ZSetBatch::new(&spec.partial_schema);
    let got = agg_finish(&spec, &empty);

    assert_eq!(
        got.pks,
        PkColumn::from_natives(&spec.partial_schema, [gnitz_wire::global_group_key()])
    );
    assert_eq!(got.columns[1], le(&[0]));
}

/// A global `AVG(u)` over a `BIGINT UNSIGNED` column, built the way the planner
/// builds one: `push_agg_specs` splits it into `[Sum, CountNonNull]`, and the
/// finalize item is the shared composite over the two partial columns. That
/// composite is the *only* place the division happens on this path, so these
/// tests exercise it rather than a helper of their own.
fn avg_shape() -> FoldShape {
    let src = Schema {
        columns: vec![col_def("pk", TypeCode::U64, false), col_def("u", TypeCode::U64, true)],
        pk_cols: vec![0],
    };
    let mut agg_specs = Vec::new();
    push_agg_specs(AggFunc::Avg, Some(1), &src.columns, &mut agg_specs).unwrap();
    let partial_schema = partial_schema(&src, &[], &agg_specs);
    // A global aggregate has no group columns, so the SUM lands at partial
    // column 1 and its COUNT_NON_NULL companion at 2.
    let ev = compile_scalar_evaluator(&finalize_agg_bexpr(1, Some(2), AggFunc::Avg), &partial_schema).unwrap();
    assert!(!ev.result_is_str(), "AVG finalizes to a scalar");
    FoldShape {
        reduce_schema: Arc::new(src),
        group_positions: Vec::new(),
        agg_specs,
        pre: None,
        out_schema: build_agg_out_schema(vec![col_def("a", TypeCode::F64, true)]).unwrap().0,
        partial_schema: Arc::new(partial_schema),
        having: None,
        finalize: vec![FinalizeItem::Computed { ev: Box::new(ev) }],
    }
}

/// Drive one worker partial `(sum bits, count)` through the whole finish and
/// read the AVG cell back.
fn finish_avg(sum_bits: i64, cnt: i64) -> Option<f64> {
    let spec = avg_shape();
    let mut partial = ZSetBatch::new(&spec.partial_schema);
    partial
        .pks
        .push_u128(&spec.partial_schema, gnitz_wire::global_group_key());
    partial.weights.push(1);
    partial.nulls.push(0);
    push_fixed_bits(&mut partial.columns[1], sum_bits as u64, 8);
    push_fixed_bits(&mut partial.columns[2], cnt as u64, 8);

    let got = agg_finish(&spec, &partial);
    assert_eq!(got.len(), 1);
    (!gnitz_wire::null_word_get(got.nulls[0], 0))
        .then(|| f64::from_bits(u64::from_le_bytes(got.columns[1][..8].try_into().unwrap())))
}

/// AVG divides its SUM accumulator at the accumulator's declared type. A SUM
/// over a `BIGINT UNSIGNED` source is typed U64 on the partial schema, so past
/// 2^63 its i64 bit pattern only reads as the true sum unsigned — and it is that
/// declared type, not a switch in the finisher, that makes the divide unsigned.
#[test]
fn avg_divides_an_unsigned_sum_unsigned() {
    // One cell of 2^64 - 1: the accumulator holds -1, which is that sum only
    // when read unsigned. (2^64 - 1 has no exact f64 image; it rounds to 2^64.)
    assert_eq!(finish_avg(-1, 1), Some(1.8446744073709552e19));
}

/// A zero CountNonNull companion is AVG's NULL — an empty or all-NULL group.
/// The composite renders it by dividing by zero, so nothing has to special-case
/// it.
#[test]
fn avg_nulls_on_a_zero_count_companion() {
    assert_eq!(finish_avg(0, 0), None);
}
