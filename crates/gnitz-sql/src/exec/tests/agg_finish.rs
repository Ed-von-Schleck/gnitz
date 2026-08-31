use super::*;
use crate::agg::{synthetic_fold_cols, AggMapping};
use crate::ir::AggFunc;
use crate::test_support::col_def;
use gnitz_core::PkColumn;

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
    Schema::from_parts(synthetic_fold_cols(src, group_cols, specs, None), vec![0])
        .expect("the SyntheticFold layout is a valid client schema")
}

/// The concatenated LE images of `vs` — what a `Fixed` i64 column holds.
fn le(vs: &[i64]) -> Vec<u8> {
    vs.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// A combined COUNT accumulator, in the shape `ColAcc::new` builds for a
/// `SumZero` merge.
fn count_acc(n: i64) -> ColAcc {
    ColAcc::IntSum {
        bits: n,
        seen: true,
        tc: TypeCode::I64,
    }
}

fn fixed(col: &ColData) -> &[u8] {
    match col {
        ColData::Fixed(b) => b,
        _ => panic!("expected a Fixed column"),
    }
}

/// The group batch the HAVING filter runs over: group columns copied from
/// each representative partial row, aggregate partials taken from `accs` at
/// the declared width, and one null bit per payload slot. A wrong bit here
/// is a silently wrong HAVING verdict, not a crash.
#[test]
fn fill_group_batch_lays_out_values_and_null_bits() {
    let src = source_schema();
    let specs = agg_specs();
    let partial_s = partial_schema(&src, &[1], &specs);
    // `fill_group_batch` never reads the output schema; it only has to exist.
    let out_s = Schema::from_parts(vec![col_def("_agg_pk", TypeCode::U128, false).hidden()], vec![0]).unwrap();
    let layout = GroupByLayout {
        group_col_indices: vec![1],
        agg_specs: specs,
        agg_mappings: vec![],
        select_items: vec![],
    };

    // Two representative partial rows: group 0 has g = 10, group 1 has g NULL
    // (payload slot 0). The agg columns are never read from `partial`. The
    // keys are deliberately not `0, 1`, so a staging batch that stamped the
    // group ordinal instead of copying the key would not coincide.
    let mut partial = ZSetBatch::new(&partial_s);
    for (row, (key, g)) in [(0x77u128, 10i64), (0x33, 0)].into_iter().enumerate() {
        partial.pks.push_u128(key);
        partial.weights.push(1);
        partial.nulls.push(if row == 1 { 0b1 } else { 0 });
        push_fixed_bits(&mut partial.columns[1], g as u64, 8);
        push_fixed_bits(&mut partial.columns[2], 0, 2);
        push_fixed_bits(&mut partial.columns[3], 0, 8);
    }

    let reps = [Some(0usize), Some(1usize)];
    let accs = vec![
        // Group 0: MIN(sm) = -5, COUNT = 2.
        ColAcc::Extreme {
            best: Some([0xfb, 0xff, 0, 0, 0, 0, 0, 0]),
            is_max: false,
            tc: TypeCode::I16,
        },
        count_acc(2),
        // Group 1: an all-NULL MIN group, COUNT = 1.
        ColAcc::Extreme {
            best: None,
            is_max: false,
            tc: TypeCode::I16,
        },
        count_acc(1),
    ];

    let spec = FoldShape {
        layout,
        partial_schema: partial_s,
        out_schema: out_s,
        having: None,
    };
    let got = fill_group_batch(&spec, &partial, &reps, &accs);

    assert_eq!(got.len(), 2);
    assert_eq!(got.weights, vec![1, 1]);
    // Slot 0 = g, slot 1 = MIN(sm), slot 2 = COUNT. Group 1 nulls g and MIN.
    assert_eq!(got.nulls, vec![0, 0b011]);
    // `_group_pk` is the representative row's key, copied.
    assert_eq!(got.pks, PkColumn::from_u128s(16, [0x77, 0x33]));
    // g: the copied value, then `push_null`'s zero filler.
    assert_eq!(
        fixed(&got.columns[1]),
        10i64.to_le_bytes().iter().chain(&[0; 8]).copied().collect::<Vec<_>>()
    );
    // MIN(sm) truncated to its declared 2 bytes, then a zeroed NULL cell.
    assert_eq!(fixed(&got.columns[2]), &[0xfb, 0xff, 0, 0]);
    // COUNT is never NULL.
    assert_eq!(
        fixed(&got.columns[3]),
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
    let specs = agg_specs();
    let partial_s = partial_schema(&src, &[], &specs);
    let out_s = Schema::from_parts(vec![col_def("_agg_pk", TypeCode::U128, false).hidden()], vec![0]).unwrap();
    let layout = GroupByLayout {
        group_col_indices: vec![],
        agg_specs: specs,
        agg_mappings: vec![],
        select_items: vec![],
    };
    let empty = ZSetBatch::new(&partial_s);
    let spec = FoldShape {
        layout,
        partial_schema: partial_s,
        out_schema: out_s,
        having: None,
    };
    let accs = vec![
        ColAcc::Extreme {
            best: None,
            is_max: false,
            tc: TypeCode::I16,
        },
        count_acc(0),
    ];
    let got = fill_group_batch(&spec, &empty, &[None], &accs);

    assert_eq!(got.len(), 1);
    // Slot 0 = MIN (NULL), slot 1 = COUNT (0, never NULL).
    assert_eq!(got.nulls, vec![0b01]);
    assert_eq!(fixed(&got.columns[2]), &0i64.to_le_bytes());
}

/// The layout the fold path builds for one direct COUNT — `SELECT g,
/// COUNT(*) … GROUP BY g` at `group_cols = [1]`, `SELECT COUNT(*)` at `[]`.
fn count_layout(group_cols: Vec<usize>) -> GroupByLayout {
    let grouped = !group_cols.is_empty();
    GroupByLayout {
        group_col_indices: group_cols,
        agg_specs: vec![AggSpec {
            op: WireAggFunc::Count,
            col: 0,
            out_type: TypeCode::I64,
        }],
        agg_mappings: vec![AggMapping {
            specs_start: 0,
            shape: AggShape::Direct,
            output_name: "c".to_string(),
            output_type: TypeCode::I64,
            output_nullable: false,
            agg_func: AggFunc::Count,
            arg_col: None,
        }],
        select_items: grouped
            .then(|| GroupBySelectItem::GroupCol {
                src_col: 1,
                name: "g".to_string(),
            })
            .into_iter()
            .chain([GroupBySelectItem::Aggregate { agg_idx: 0 }])
            .collect(),
    }
}

/// The partial mirrors what `fetch_bound` concatenates: replies in worker
/// order, so a group's rows split across two workers and the keys arrive
/// descending — the emission ordinal and the group key then disagree on both
/// order and value, and only the key is a function of the data.
#[test]
fn emit_row_copies_the_engine_group_key() {
    let src = source_schema();
    let layout = count_layout(vec![1]);
    let partial_s = partial_schema(&src, &[1], &layout.agg_specs);
    let out_s = build_agg_out_schema(&layout, &src).unwrap();

    // (group key, g, this worker's COUNT partial). Worker 0 emits groups
    // 0x2222 then 0x1111; worker 1 emits the rest of 0x1111.
    let mut partial = ZSetBatch::new(&partial_s);
    for (key, g, n) in [(0x2222u128, 20i64, 1i64), (0x1111, 10, 2), (0x1111, 10, 3)] {
        partial.pks.push_u128(key);
        partial.weights.push(1);
        partial.nulls.push(0);
        push_fixed_bits(&mut partial.columns[1], g as u64, 8);
        push_fixed_bits(&mut partial.columns[2], n as u64, 8);
    }

    let spec = FoldShape {
        layout,
        partial_schema: partial_s,
        out_schema: out_s,
        having: None,
    };
    let got = agg_finish(&spec, &partial);

    assert_eq!(got.len(), 2);
    // First-encounter ordinals would be `[0, 1]`; the group keys are not.
    assert_eq!(got.pks, PkColumn::from_u128s(16, [0x2222, 0x1111]));
    // The values stay paired with their keys: 0x2222 is g = 20 / COUNT 1,
    // 0x1111 is g = 10 / COUNT 2 + 3.
    assert_eq!(fixed(&got.columns[1]), le(&[20, 10]));
    assert_eq!(fixed(&got.columns[2]), le(&[1, 5]));
}

/// The synthesized global ground row has no partial to copy a key from, so
/// it takes V₀ — the same key the engine stamps on the ground row it emits
/// when a worker did contribute one, so one logical row has one key either
/// way.
#[test]
fn emit_row_grounds_the_global_row_at_v0() {
    let src = source_schema();
    let layout = count_layout(vec![]);
    let partial_s = partial_schema(&src, &[], &layout.agg_specs);
    let out_s = build_agg_out_schema(&layout, &src).unwrap();
    let empty = ZSetBatch::new(&partial_s);
    let spec = FoldShape {
        layout,
        partial_schema: partial_s,
        out_schema: out_s,
        having: None,
    };
    let got = agg_finish(&spec, &empty);

    assert_eq!(got.pks, PkColumn::from_u128s(16, [gnitz_wire::global_group_key()]));
    assert_eq!(fixed(&got.columns[1]), le(&[0]));
}

/// One AVG mapping over `[Sum, CountNonNull]` — the only layout `finish_agg`
/// reads, so the rest of the query can stay empty.
fn avg_layout() -> GroupByLayout {
    GroupByLayout {
        group_col_indices: vec![],
        agg_specs: vec![],
        agg_mappings: vec![AggMapping {
            specs_start: 0,
            shape: AggShape::Avg,
            output_name: "a".to_string(),
            output_type: TypeCode::F64,
            output_nullable: true,
            agg_func: AggFunc::Avg,
            arg_col: None,
        }],
        select_items: vec![],
    }
}

fn finish_avg(layout: GroupByLayout, accs: &[ColAcc]) -> Option<f64> {
    let spec = FoldShape {
        layout,
        partial_schema: source_schema(),
        out_schema: Schema::from_parts(vec![col_def("_agg_pk", TypeCode::U128, false).hidden()], vec![0]).unwrap(),
        having: None,
    };
    finish_agg(&spec, accs, 0).map(f64::from_bits)
}

/// AVG divides its SUM accumulator at the accumulator's declared type. A SUM
/// over a `BIGINT UNSIGNED` source is typed U64, so past 2^63 its i64 bit
/// pattern only reads as the true sum unsigned.
#[test]
fn avg_divides_an_unsigned_sum_unsigned() {
    // One cell of 2^64 - 1: the accumulator holds -1, which is that sum only
    // when read unsigned. (2^64 - 1 has no exact f64 image; it rounds to 2^64.)
    let unsigned = ColAcc::IntSum {
        bits: -1,
        seen: true,
        tc: TypeCode::U64,
    };
    assert_eq!(
        finish_avg(avg_layout(), &[unsigned, count_acc(1)]),
        Some(1.8446744073709552e19)
    );
    // The same bit pattern over a signed source is genuinely -1.
    let signed = ColAcc::IntSum {
        bits: -1,
        seen: true,
        tc: TypeCode::I64,
    };
    assert_eq!(finish_avg(avg_layout(), &[signed, count_acc(1)]), Some(-1.0));
}

/// A zero CountNonNull companion is AVG's NULL — an empty or all-NULL group.
#[test]
fn avg_nulls_on_a_zero_count_companion() {
    let sum = ColAcc::IntSum {
        bits: 0,
        seen: false,
        tc: TypeCode::I64,
    };
    assert_eq!(finish_avg(avg_layout(), &[sum, count_acc(0)]), None);
}
