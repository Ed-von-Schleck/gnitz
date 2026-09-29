//! Reduce operator tests. Imports submodule items by name; helpers live in this
//! file rather than a shared `common.rs` so the tests file stays self-contained.

use crate::ops::map::PkSource;
use crate::schema::ColumnTable;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::{Batch, BatchBuilder, Layout, ReadCursor};
use crate::test_support::{
    make_batch_raw, make_schema_i64pk_i64, make_schema_u64_i64, opk_pk, opk_pk_i64, payload0_i64, pk_payload_schema,
    scratch_table, trace_cursor, u64_pk_schema,
};
use gnitz_wire::{read_i64_le, read_u64_le};

use super::super::group_key::GroupOutKey;
use super::agg::Accumulator;
use super::avi::AviBake;
use super::emit::emit_reduce_row;
use super::plan::{ReducePlan, ReduceShape};
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

/// The AVI index schema for `(schema, group cols)` — the same one the plan
/// bakes onto a compiled reduce, reached without naming the aggregate list.
fn avi_schema(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> SchemaDescriptor {
    make_bake(
        schema,
        group_by_cols,
        &[
            AggDescriptor { col_idx: 0, agg_op: AggFunc::Min },
            AggDescriptor::COUNT_STAR,
        ],
    )
    .schema
}

/// A `trace_out` cursor over an empty trace — a view's first epoch.
fn empty_trace(schema: SchemaDescriptor) -> ReadCursor {
    trace_cursor(Batch::empty_with_schema(&schema), schema)
}

/// Shim over [`super::op_reduce::op_reduce`] baking a [`ReducePlan`] per call and
/// the history from the passed cursor, so the many call sites below need not repeat it.
fn op_reduce(
    delta: &Batch,
    trace_out_cursor: &mut ReadCursor,
    input_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    avi_cursor: Option<&mut ReadCursor>,
    seeds_ground: bool,
) -> Batch {
    let plan = make_plan(input_schema, group_by_cols, agg_descs, seeds_ground);
    // The VM folds the register before both the kernel and the value index.
    let cs = (!plan.is_exact_linear())
        .then(|| Batch::consolidate_if_needed(delta, input_schema))
        .flatten();
    let delta = cs.as_ref().unwrap_or(delta);
    // A non-linear reduce always carries a value index. `None` from a caller
    // means "the index holds exactly this delta" — the single-tick shape — so
    // build it here; a caller with prior history passes its own cursor.
    let mut owned = (avi_cursor.is_none() && plan.avi.is_some())
        .then(|| Avi::new(input_schema, group_by_cols, agg_descs, &[delta]));
    let mut owned_cursor = owned.as_mut().map(|a| a.cursor());
    let history = avi_cursor.or(owned_cursor.as_mut());
    super::op_reduce::op_reduce(delta, trace_out_cursor, history, &plan)
}

/// The value index a non-linear reduce reads its history from, populated
/// through the production integrate path.
///
/// `history` must include the delta the reduce is about to consume: the
/// compiler emits the AVI `Integrate` ahead of the `Reduce`, so a prefix seek
/// returns the *post*-delta extreme. Each call is a separate ingest, and the
/// cursor's two-tier consolidation sums weights across them — so a retracted
/// extreme nets to zero and is skipped by the seek.
struct Avi {
    _dir: tempfile::TempDir,
    table: crate::storage::Table,
}

impl Avi {
    pub(crate) fn new(
        in_schema: &SchemaDescriptor,
        group_cols: &[u32],
        agg_descs: &[AggDescriptor],
        history: &[&Batch],
    ) -> Self {
        // The bake applies the value-index selection itself, so ordinal `j` is
        // position `j` in that subset exactly as production has it.
        let bake = make_bake(in_schema, group_cols, agg_descs);
        let dir = tempfile::tempdir().unwrap();
        let mut table = scratch_table(dir.path().to_str().unwrap(), bake.schema);
        for b in history {
            use super::avi::avi_batch;
            table.ingest_owned_batch(avi_batch(b, &bake)).unwrap();
        }
        Avi { _dir: dir, table }
    }

    fn cursor(&mut self) -> ReadCursor {
        self.table.open_cursor()
    }
}

/// Bake a [`ReducePlan`] the way the compiler's `emit_reduce` does.
fn make_plan(
    input_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    seeds_ground: bool,
) -> ReducePlan {
    ReducePlan::from_wire(input_schema, group_by_cols, agg_descs, seeds_ground).unwrap()
}

/// The baked AVI of a value-indexed reduce, reached through the plan that owns
/// it — the only way production builds one.
fn make_bake(in_schema: &SchemaDescriptor, group_cols: &[u32], agg_descs: &[AggDescriptor]) -> AviBake {
    make_plan(in_schema, group_cols, agg_descs, false)
        .avi
        .expect("a value-indexed reduce has an AVI")
}

/// The single accumulator the plan bakes for `desc` — carrying the output
/// column locator its emission and trace read-back go through.
fn make_acc(in_schema: &SchemaDescriptor, group_cols: &[u32], desc: AggDescriptor) -> Accumulator {
    let mut accs = make_plan(in_schema, group_cols, &[desc, AggDescriptor::COUNT_STAR], false)
        .shape
        .acc_template;
    accs.swap_remove(0)
}

/// The AVI value image of an I64 aggregate value, spelled out rather than taken
/// from the code under test: `order_bits`' signed-integer half is
/// the sign-bit flip that puts two's-complement negatives below non-negatives.
fn i64_av(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// The output schema the plan derives for `(schema, group cols, aggs)`.
fn out_schema_for(schema: &SchemaDescriptor, group_cols: &[u32], aggs: &[AggDescriptor]) -> SchemaDescriptor {
    let (key, prefix) = GroupOutKey::new(schema, group_cols, group_cols.iter().copied()).unwrap();
    ReduceShape::new(schema, key, prefix, aggs).unwrap().output_schema
}

/// The group key `identity` of `row` under `cols`.
fn group_identity(schema: &SchemaDescriptor, cols: &[u32], mb: &crate::storage::MemBatch, row: usize) -> u128 {
    GroupOutKey::new(schema, cols, []).unwrap().0.identity(mb, row)
}

/// Asserts that each run under a keyed out-key is exactly one `group_of` group,
/// and returns the visit order.
fn assert_runs_are_groups(
    schema: &SchemaDescriptor,
    cols: &[u32],
    batch: &Batch,
    group_of: impl Fn(usize) -> u128,
) -> Vec<usize> {
    let key = GroupOutKey::new(schema, cols, []).unwrap().0;
    let runs = key.runs(batch);
    let mut seen: Vec<u128> = Vec::new();
    for run in runs.iter() {
        let g = group_of(runs.row(run.start));
        assert!(run.clone().all(|p| group_of(runs.row(p)) == g), "a run mixes groups");
        assert!(!seen.contains(&g), "group {g} is split across runs");
        seen.push(g);
    }
    (0..batch.count).map(|p| runs.row(p)).collect()
}

/// The three arms of the group sort, end to end through `op_reduce`: a
/// sign-flipped narrow route key, a 16-byte route key, and the multi-column
/// digest. Each asserts that every group is emitted exactly once with the right
/// aggregate — group *order* is the key's, which for the digest arm is not the
/// group columns'.
#[test]
fn grouped_reduce_narrow_signed_route_key() {
    // A non-nullable I64 group column keys by its sign-flipped image, so −1
    // sorts below 0.
    let in_schema = u64pk_i64grp_i64val(false);
    let aggs = sum_count_aggs(2);
    let out_schema = out_schema_for(&in_schema, &[1u32], &aggs);
    let mut to_ch = empty_trace(out_schema);

    // Groups −5, 0, 7 interleaved, so a sort is required to make them contiguous.
    let delta = make_batch_u64pk_i64grp_i64val(
        &in_schema,
        &[
            (1, 1, 7, 10),
            (2, 1, -5, 100),
            (3, 1, 0, 1),
            (4, 1, 7, 20),
            (5, 1, -5, 200),
            (6, 1, 0, 2),
        ],
    );

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &aggs, None, false);
    let by_grp: std::collections::HashMap<i64, (i64, i64)> = (0..out.count)
        .map(|r| {
            (
                opk_pk_i64(out.get_pk_bytes(r)),
                (read_i64_le(out.col_data(0), r * 8), read_i64_le(out.col_data(1), r * 8)),
            )
        })
        .collect();
    assert_eq!(out.count, 3, "one row per group, each closed exactly once");
    assert_eq!(by_grp[&-5], (300, 2));
    assert_eq!(by_grp[&0], (3, 2));
    assert_eq!(by_grp[&7], (30, 2));
}

#[test]
fn grouped_reduce_uuid_route_key() {
    // A non-nullable UUID group column keys by its 16-byte image.
    let in_schema = make_schema_u64_uuid_i64();
    let aggs = sum_count_aggs(2);
    let out_schema = out_schema_for(&in_schema, &[1u32], &aggs);
    let mut to_ch = empty_trace(out_schema);

    let uuid_a: u128 = 0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_0001;
    let uuid_b: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0002;
    let delta = build_batch_u64_uuid_i64(
        &in_schema,
        &[(1, uuid_a, 10), (2, uuid_b, 100), (3, uuid_a, 20), (4, uuid_b, 200)],
    );

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &aggs, None, false);
    assert_eq!(out.count, 2, "one row per group, each closed exactly once");
    // A UUID group set is a natural output key, so the row's PK *is* the UUID.
    let by_uuid: std::collections::HashMap<u128, (i64, i64)> = (0..out.count)
        .map(|r| {
            (
                out.get_pk(r),
                (read_i64_le(out.col_data(0), r * 8), read_i64_le(out.col_data(1), r * 8)),
            )
        })
        .collect();
    assert_eq!(by_uuid[&uuid_a], (30, 2));
    assert_eq!(by_uuid[&uuid_b], (300, 2));
}

#[test]
fn grouped_reduce_two_column_digest_key() {
    // A two-column group set keys by the XXH3 fold. Group
    // *order* is the digest's, so the assertion is per group, never positional.
    let in_schema = make_schema_u64_uuid_i64();
    let aggs = [AggDescriptor::COUNT_STAR];
    let out_schema = out_schema_for(&in_schema, &[1u32, 2u32], &aggs);
    let mut to_ch = empty_trace(out_schema);

    let uuid_a: u128 = 0x1111_2222_3333_4444_5555_6666_7777_8888;
    let uuid_b: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0009;
    let delta = build_batch_u64_uuid_i64(
        &in_schema,
        &[
            (1, uuid_a, 42),
            (2, uuid_b, 42),
            (3, uuid_a, 43),
            (4, uuid_a, 42),
            (5, uuid_b, 42),
        ],
    );

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32, 2u32], &aggs, None, false);
    assert_eq!(out.count, 3, "three (uuid, val) groups, each closed exactly once");
    // Exemplar columns are the group columns in order: uuid at payload 0, val at 1.
    let by_group: std::collections::HashMap<(u128, i64), i64> = (0..out.count)
        .map(|r| {
            let uuid = u128::from_le_bytes(out.col_data(0)[r * 16..r * 16 + 16].try_into().unwrap());
            (
                (uuid, read_i64_le(out.col_data(1), r * 8)),
                read_i64_le(out.col_data(2), r * 8),
            )
        })
        .collect();
    assert_eq!(by_group[&(uuid_a, 42)], 2);
    assert_eq!(by_group[&(uuid_a, 43)], 1);
    assert_eq!(by_group[&(uuid_b, 42)], 2);
}

/// A reduce output row's PK as a `u128`, widened from the region's own stride —
/// which is the *input's* stride whenever the group set is the PK.
fn out_pk(b: &Batch, row: usize) -> u128 {
    let bytes = b.get_pk_bytes(row);
    gnitz_wire::widen_pk_be(bytes)
}

/// Local variant of `test_support::make_batch`: stamps the layout with
/// `set_layout_unchecked` (no debug verify) so reduce tests can hand the
/// operator deliberately unordered-but-claimed-consolidated inputs.
fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let mut b = make_batch_raw(schema, rows);
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_reduce_sum_retraction() {
    use crate::schema::{SchemaColumn, TypeCode};

    // Input: pk(U64), grp(I64), val(I64)
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );

    // Output: grp(I64), sum(I64), count(I64). The trailing count is the
    // cardinality companion every circuit reduce carries; op_reduce gates
    // emission on it.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );

    // Empty trace_out
    let mut to_ch = empty_trace(out_schema);

    // Tick 1: insert 3 rows in group 10: val=100, val=200, val=300
    let delta1 = {
        let mut b = BatchBuilder::new(in_schema);
        for (pk, val) in [(1u64, 100i64), (2, 200), (3, 300)] {
            b.begin_row(pk as u128, 1i64);
            b.put_int(10);
            b.put_int(val as u128);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let aggs = sum_count_aggs(2);

    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &[1u32], &aggs, None, false);
    // SUM of (100+200+300) = 600
    assert_eq!(out1.count, 1);
    let sum1 = read_i64_le(out1.col_data(0), 0);
    assert_eq!(sum1, 600);

    // Tick 2: retract pk=2 (val=200) → SUM should go from 600 to 400
    // Need trace_out with previous aggregate
    let mut to_ch2 = trace_cursor(out1, out_schema);

    let delta2 = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(2u128, -1i64);
        b.put_int(10);
        b.put_int(200);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &[1u32], &aggs, None, false);
    // Output: retract old sum (600, w=-1) + insert new sum (400, w=+1) = 2 rows.
    // The group survives (cardinality 3 → 2), so the +1 is emitted.
    assert_eq!(out2.count, 2);
}

/// All-linear gate: a non-nullable SUM-only group driven to empty must emit only
/// the −1 retraction of its stored row — no +1 zombie carrying SUM=0. The reduce
/// carries the appended Count cardinality companion; once the group's net
/// cardinality reaches 0 the gate suppresses the new row, so the group vanishes
/// exactly as SQL (and the Z-set model) require.
#[test]
fn linear_sum_only_emptied_group_eliminated() {
    use crate::schema::{SchemaColumn, TypeCode};

    // Input: pk(U64), grp(I64), val(I64). Output (natural GROUP BY grp):
    // grp(I64), sum(I64 nullable), count(I64 companion).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = sum_count_aggs(2);

    let reduce = |delta: &Batch, to: &mut crate::storage::ReadCursor| {
        op_reduce(delta, to, &in_schema, &[1u32], &aggs, None, false)
    };

    // Tick 1: insert (pk1, grp=10, val=5) → group exists (sum=5, count=1).
    let mut to_ch = empty_trace(out_schema);
    let row = |pk: u128, w: i64| {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(pk, w);
        b.put_int(10);
        b.put_int(5);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = reduce(&row(1, 1), &mut to_ch);
    assert_eq!(out1.count, 1, "group present after first insert");

    // Tick 2: retract the only row → cardinality 1 → 0. The gate suppresses the
    // +1, leaving just the −1 retraction, so the group disappears (no zombie).
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let out2 = reduce(&row(1, -1), &mut to_ch2);
    assert_eq!(out2.count, 1, "emptied group emits only the retraction, no +1 zombie");
    assert_eq!(out2.get_weight(0), -1, "the sole output row is the −1 retraction");
    assert_eq!(
        read_i64_le(out2.col_data(0), 0),
        5,
        "retraction re-emits the stored SUM=5"
    );
}

/// All-linear gate: inserting the *first* row of a new group whose aggregated
/// column is NULL must surface the group (SQL: `SUM = NULL`), not drop it. With
/// the appended Count companion the null-blind row count is 1, so the gate emits
/// the group; the SUM/CountNonNull accumulators stay untouched, so the raw SUM
/// is `0` and the finalize, gated on the count companion, renders SUM = NULL.
#[test]
fn linear_sum_only_new_all_null_group_present() {
    use crate::schema::{SchemaColumn, TypeCode};
    use gnitz_expr::{CmpOp, IntArithOp, LogicalInstr, LogicalProgram, Reg, Sink};

    // Input: pk(U64), grp(I64), val(I64 nullable).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    // Raw reduce output: grp | sum | cnn | count.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false), // grp
            SchemaColumn::new(TypeCode::I64, false), // sum
            SchemaColumn::new(TypeCode::I64, false), // cnn
            SchemaColumn::new(TypeCode::I64, false), // count (companion)
        ],
        &[0],
    );
    // Finalize projects [grp, sum]; cnn and the count companion are stripped.
    let fin_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true), // sum (nullable)
        ],
        &[0],
    );
    // sum / (cnn != 0) → fin payload 0, NULL when cnn is 0.
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 2 }, // r0 = cnn
        LogicalInstr::LoadConst { val: 0, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Ne, a: Reg(0), b: Reg(1) }, // r2 = (cnn != 0)
        LogicalInstr::LoadColInt { col: 1 },                       // r3 = sum
        LogicalInstr::IntArith {
            op: IntArithOp::Div,
            a: Reg(3),
            b: Reg(2),
        }, // r4 = sum / gate
    ];
    // fin payload 0 = the gated sum.
    let sinks = vec![Sink::Reg(Reg(4))];
    let mut fin_func = crate::ops::MapPlan::from_map(
        LogicalProgram::new(instrs, sinks, vec![]),
        &out_schema,
        &fin_schema,
        PkSource::Inherit,
    )
    .unwrap();

    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggFunc::CountNonNull,
        },
        AggDescriptor::COUNT_STAR,
    ];

    // New group g=7 whose only row has val=NULL.
    let mut to_ch = empty_trace(out_schema);
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, 1i64);
        b.put_int(7);
        b.put_null();
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let raw = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &aggs, None, false);
    let fin = fin_func.evaluate_map_batch(&raw);
    assert_eq!(
        raw.count, 1,
        "new all-NULL group is present (cardinality 1), not dropped"
    );
    assert_eq!(
        raw.get_null_word(0) & 1,
        0,
        "the raw SUM of an all-NULL group is present"
    );
    assert_eq!(
        read_i64_le(raw.col_data(0), 0),
        0,
        "the raw SUM of an all-NULL group is 0"
    );
    assert_eq!(fin.count, 1);
    assert_eq!(fin.get_weight(0), 1, "one +1 row");
    assert_eq!(opk_pk_i64(fin.get_pk_bytes(0)), 7, "grp=7");
    assert_eq!(fin.get_null_word(0) & 1, 1, "SUM of an all-NULL group renders as NULL",);
}

/// All-linear gate, reuse path: a COUNT(*)-only group driven to empty is
/// eliminated. The user COUNT(*) *is* the cardinality signal (no companion is
/// appended), so the gate suppresses the +1 once the count nets to 0.
#[test]
fn count_star_only_emptied_group_eliminated() {
    use crate::schema::{SchemaColumn, TypeCode};

    // Input: pk(U64), grp(I64). Output (natural GROUP BY grp): grp(I64), count(I64).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = [AggDescriptor::COUNT_STAR];

    let reduce = |delta: &Batch, to: &mut crate::storage::ReadCursor| {
        op_reduce(delta, to, &in_schema, &[1u32], &aggs, None, false)
    };

    let row = |pk: u128, w: i64| {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(pk, w);
        b.put_int(10);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // Tick 1: insert one row → count=1.
    let mut to_ch = empty_trace(out_schema);
    let out1 = reduce(&row(1, 1), &mut to_ch);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 1, "count=1");

    // Tick 2: retract it → count 1 → 0 → group eliminated (only the −1 retraction).
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let out2 = reduce(&row(1, -1), &mut to_ch2);
    assert_eq!(out2.count, 1, "COUNT(*)=0 group is eliminated, not kept as a zombie");
    assert_eq!(out2.get_weight(0), -1, "the sole output row is the −1 retraction");
}

/// Nullable SUM must transition to NULL when a retraction removes the last
/// non-null contributor from a still-surviving group. Drives `op_reduce` on the
/// linear fold (Count + Sum + CountNonNull) with the finalize program the SQL
/// planner builds for a nullable SUM: the SUM is null-gated by the hidden
/// COUNT_NON_NULL companion (`sum / (cnt != 0)` → NULL when the count is zero,
/// `sum` when it is positive). Pins the mechanism without the planner — a bare
/// `[Count, Sum]` reduce has no extra column to carry the non-null count, which
/// is exactly the defect the companion fixes.
#[test]
fn test_reduce_nullable_sum_retraction_becomes_null() {
    use crate::schema::{SchemaColumn, TypeCode};
    use gnitz_expr::{CmpOp, IntArithOp, LogicalInstr, LogicalProgram, Reg, Sink};

    // Input: pk(U64), grp(I64), val(I64, NULLABLE).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true), // nullable source
        ],
        &[0],
    );

    // Raw reduce output: natural grp | count | sum | cnn.
    // The aggregate column order mirrors agg_descs = [Count, Sum, CountNonNull].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false), // grp
            SchemaColumn::new(TypeCode::I64, false), // count
            SchemaColumn::new(TypeCode::I64, false), // sum
            SchemaColumn::new(TypeCode::I64, false), // cnn (companion)
        ],
        &[0],
    );

    // Finalized output projects [grp, count, sum] — companion stripped.
    let fin_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false), // grp
            SchemaColumn::new(TypeCode::I64, false), // count
            SchemaColumn::new(TypeCode::I64, true),  // sum (nullable)
        ],
        &[0],
    );

    // Finalize program (full reduce-output column indices; resolved below):
    //   copy count(col 1) → fin payload 0,
    //   emit sum-gate = sum(col 2) / (cnn(col 3) != 0) → fin payload 1.
    // div-by-zero (cnn == 0) marks the SUM NULL; div-by-1 (cnn > 0) is exact.
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 3 },                       // r0 = cnn (col 3)
        LogicalInstr::LoadConst { val: 0, unsigned: false },       // r1 = 0
        LogicalInstr::Cmp { op: CmpOp::Ne, a: Reg(0), b: Reg(1) }, // r2 = (cnn != 0) → 1/0
        LogicalInstr::LoadColInt { col: 2 },                       // r3 = sum (col 2)
        LogicalInstr::IntArith {
            op: IntArithOp::Div,
            a: Reg(3),
            b: Reg(2),
        }, // r4 = sum / gate (NULL when gate == 0)
    ];
    // fin payload 0 = count, 1 = the gated sum.
    let sinks = vec![Sink::Col(1), Sink::Reg(Reg(4))];
    let mut fin_func = crate::ops::MapPlan::from_map(
        LogicalProgram::new(instrs, sinks, vec![]),
        &out_schema,
        &fin_schema,
        PkSource::Inherit,
    )
    .unwrap();

    let aggs = [
        AggDescriptor::COUNT_STAR,
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggFunc::CountNonNull,
        },
    ];

    // Tick 1: insert (pk1, grp=10, val=5) and (pk2, grp=10, val=NULL).
    let mut to_ch = empty_trace(out_schema);
    let delta1 = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, 1i64);
        b.put_int(10);
        b.put_int(5);
        b.end_row();
        // val is payload col 1 → its null bit is bit 1.
        b.begin_row(2u128, 1i64);
        b.put_int(10);
        b.put_null();
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let raw1 = op_reduce(&delta1, &mut to_ch, &in_schema, &[1u32], &aggs, None, false);
    let fin1 = fin_func.evaluate_map_batch(&raw1);
    // One group: count=2, sum=5 (non-null while a contributor remains), cnn=1.
    assert_eq!(raw1.count, 1);
    assert_eq!(fin1.count, 1);
    assert_eq!(fin1.get_weight(0), 1);
    assert_eq!(read_i64_le(fin1.col_data(0), 0), 2, "count=2");
    assert_eq!(read_i64_le(fin1.col_data(1), 0), 5, "sum=5");
    assert_eq!(
        (fin1.get_null_word(0) >> 1) & 1,
        0,
        "sum non-null while a contributor remains"
    );

    // Tick 2: retract (pk1, val=5). The group survives via (pk2, NULL): count
    // drops to 1, the last non-null contributor is gone → SUM must become NULL.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let delta2 = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, -1i64);
        b.put_int(10);
        b.put_int(5);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let _raw2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &[1u32], &aggs, None, false);
    let fin2 = fin_func.evaluate_map_batch(&_raw2);
    // Retract the old aggregate (w=-1) and insert the new one (w=+1).
    assert_eq!(fin2.count, 2);

    // The insert row (weight +1) carries the surviving group's new state.
    let insert_row = (0..fin2.count)
        .find(|&r| fin2.get_weight(r) == 1)
        .expect("an insert row");
    assert_eq!(
        read_i64_le(fin2.col_data(0), insert_row * 8),
        1,
        "COUNT(*) survives at 1",
    );
    assert_eq!(
        (fin2.get_null_word(insert_row) >> 1) & 1,
        1,
        "SUM becomes NULL once its last non-null contributor is retracted",
    );

    // The retraction row re-emits the prior (non-null) SUM, cancelling the old
    // output byte-for-byte — the fix introduces no ghost.
    let retract_row = (0..fin2.count)
        .find(|&r| fin2.get_weight(r) == -1)
        .expect("a retract row");
    assert_eq!(
        (fin2.get_null_word(retract_row) >> 1) & 1,
        0,
        "retracted SUM was non-null"
    );
    assert_eq!(read_i64_le(fin2.col_data(1), retract_row * 8), 5, "retracted SUM = 5",);
}

/// A group whose MIN is NULL (all contributors NULL) is kept alive by COUNT(*).
/// When a non-NULL row later joins it, the group's old (NULL-MIN) output row must
/// be retracted *as NULL* to cancel the tick-1 row byte-for-byte. MIN is a Direct
/// passthrough, so the raw null bit is user-visible — the primary user-facing bug.
#[test]
fn null_min_retraction_re_emits_null() {
    // in: pk(U64), grp(I64), val(I64 nullable); out (natural GROUP BY grp):
    // grp(I64), count(I64), min(I64 nullable). min is payload index 1.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Count },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
    ];
    let min_null_bit = 1u64 << 1;

    // Tick 1: (pk=1, grp=10, val=NULL) → COUNT keeps the group alive, MIN=NULL.
    let mut to_ch = empty_trace(out_schema);
    let delta1 = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, 1i64);
        b.put_int(10);
        b.put_null();
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &[1u32], &aggs, None, false);
    assert_eq!(out1.count, 1);
    assert!(
        out1.as_mem_batch().get_null_word(0) & min_null_bit != 0,
        "tick1 MIN must be NULL"
    );

    // Tick 2: (pk=2, grp=10, val=7) → MIN=7, retracts the NULL row.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let delta2 = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(2u128, 1i64);
        b.put_int(10);
        b.put_int(7);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &[1u32], &aggs, None, false);
    let mb2 = out2.as_mem_batch();
    let retr = (0..out2.count)
        .find(|&i| out2.get_weight(i) < 0)
        .expect("retraction row");
    assert!(
        mb2.get_null_word(retr) & min_null_bit != 0,
        "retraction of an all-NULL MIN group must re-emit MIN as NULL to cancel \
         the tick-1 row (null_word={:#x})",
        mb2.get_null_word(retr)
    );
}

#[test]
fn all_null_sum_is_zero_whatever_the_history() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Count },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
    ];
    let out_schema = out_schema_for(&in_schema, &[1], &aggs);
    assert!(!out_schema.columns[2].nullable, "a raw SUM is NOT NULL");
    // `(pk, val, weight)` rows of group 10.
    let delta = |rows: &[(u128, Option<i64>, i64)]| {
        let mut b = BatchBuilder::new(in_schema);
        for &(pk, val, w) in rows {
            b.begin_row(pk, w);
            b.put_int(10);
            b.put_opt_int(val.map(|v| v as u128));
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let reduce = |d: &Batch, trace: &Batch| {
        let mut cur = trace_cursor(trace.clone(), out_schema);
        op_reduce(d, &mut cur, &in_schema, &[1u32], &aggs, None, false)
    };
    // The inserted row's `(null word, SUM)`.
    let new_sum = |out: &Batch| {
        let row = (0..out.count).find(|&i| out.get_weight(i) > 0).expect("an insert row");
        (out.get_null_word(row), read_i64_le(out.col_data(1), row * 8))
    };
    let empty = Batch::empty_with_schema(&out_schema);

    let fresh = reduce(&delta(&[(1, None, 1)]), &empty);
    assert_eq!(new_sum(&fresh), (0, 0), "an all-NULL SUM is 0, null bit clear");

    let folded = reduce(&delta(&[(2, None, 1)]), &fresh);
    assert_eq!(new_sum(&folded), (0, 0), "a fold of an all-NULL SUM stays 0");

    let both = reduce(&delta(&[(1, None, 1), (2, Some(5), 1)]), &empty);
    assert_eq!(new_sum(&both), (0, 5));
    let retracted = reduce(&delta(&[(2, Some(5), -1)]), &both);
    assert_eq!(
        new_sum(&retracted),
        new_sum(&fresh),
        "retracting the last non-null row renders the fresh reduce of the final state"
    );
}

/// Reduce-of-trace over a wide PK (3×U64, stride 24, GROUP BY the full PK).
/// The retraction read seeks `trace_out` by the group's PK bytes; the u128
/// `seek` cannot carry a stride-24 key.
#[test]
fn reduce_trace_seek_wide_pk() {
    use crate::schema::{SchemaColumn, TypeCode};
    use crate::test_support::wide_pk_3xu64_schema;

    // Wide PK: 3×U64 (stride 24) + I64 val. GROUP BY the full PK.
    let in_schema = wide_pk_3xu64_schema();
    // Output: natural wide PK (3×U64) + SUM(I64) + count companion.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1, 2],
    );
    assert!(in_schema.pk_stride() > 16, "test invariant: stride 24 is wide");

    let pk = |a: u64, b: u64, c: u64| opk_pk(&in_schema, &[a as u128, b as u128, c as u128]);

    let aggs = sum_count_aggs(3);
    let group_by = [0u32, 1, 2];

    // Tick 1: two rows in one group (7,7,7) — val 100 and 200 → SUM = 300.
    let mut to_ch = empty_trace(out_schema);
    let delta1 = {
        let mut b = BatchBuilder::new(in_schema);
        for val in [100i64, 200] {
            b.begin_row_bytes(&pk(7, 7, 7), 1i64);
            b.put_int(val as u128);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &group_by, &aggs, None, false);
    assert_eq!(out1.count, 1, "one group");
    assert_eq!(out1.get_pk_bytes(0), &pk(7, 7, 7)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 300);

    // Tick 2: retract the val=200 row. SUM 300 → 100; reads the prior aggregate
    // out of trace_out by PK bytes.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let delta2 = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row_bytes(&pk(7, 7, 7), -1i64);
        b.put_int(200);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &group_by, &aggs, None, false);
    // Insert of new SUM (100, w=+1) and retraction of old SUM (300, w=-1), in
    // payload order.
    assert_eq!(
        out2.count, 2,
        "wide-PK retraction must read trace_out and emit retract+insert"
    );
    assert_eq!(out2.get_weight(0), 1);
    assert_eq!(out2.get_pk_bytes(0), &pk(7, 7, 7)[..]);
    assert_eq!(read_i64_le(out2.col_data(0), 0), 100, "new SUM");
    assert_eq!(out2.get_weight(1), -1);
    assert_eq!(read_i64_le(out2.col_data(0), 8), 300, "retracted old SUM");
}

/// Incremental REDUCE over a narrow COMPOUND PK (2×U64, stride 16), GROUP BY
/// the full PK, SUM. The tick-2 retraction seeks `trace_out` by the group's
/// PK. A compound key's raw-u128 order is last-column-major, so the seek must
/// go by bytes (storage order) to land on the group and retract the old SUM.
#[test]
fn reduce_trace_seek_compound_pk() {
    use crate::schema::{SchemaColumn, TypeCode};

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1],
    );
    assert!(in_schema.pk_cols().len() > 1, "test invariant: compound PK");
    assert!(in_schema.pk_stride() <= 16, "test invariant: stride 16 is narrow");

    let pk = |a: u64, b: u64| opk_pk(&in_schema, &[a as u128, b as u128]);
    let aggs = sum_count_aggs(2);
    let group_by = [0u32, 1];

    // Tick 1: insert (1,5)->100 and (2,3)->200 (two distinct groups).
    let mut to_ch = empty_trace(out_schema);
    let delta1 = {
        let mut b = BatchBuilder::new(in_schema);
        for &(a, c, val) in &[(1u64, 5u64, 100i64), (2, 3, 200)] {
            b.begin_row_bytes(&pk(a, c), 1i64);
            b.put_int(val as u128);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &group_by, &aggs, None, false);
    assert_eq!(out1.count, 2, "two groups");
    assert_eq!(out1.get_pk_bytes(0), &pk(1, 5)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 100);
    assert_eq!(out1.get_pk_bytes(1), &pk(2, 3)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 8), 200);

    // Tick 2: insert (2,3)->50. SUM for (2,3) goes 200 → 250: retract 200,
    // insert 250. Group (1,5) is untouched.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let delta2 = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row_bytes(&pk(2, 3), 1i64);
        b.put_int(50);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &group_by, &aggs, None, false);
    assert_eq!(
        out2.count, 2,
        "compound-PK retraction must read trace_out and emit retract+insert"
    );
    assert_eq!(out2.get_pk_bytes(0), &pk(2, 3)[..]);
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(read_i64_le(out2.col_data(0), 0), 200, "retracted old SUM");
    assert_eq!(out2.get_weight(1), 1);
    assert_eq!(read_i64_le(out2.col_data(0), 8), 250, "new SUM");
}

/// Incremental REDUCE over a narrow SIGNED single-column PK (I64), GROUP BY the
/// full PK, SUM. The tick-2 retraction seek to a negative key used to
/// mislocate (signed zero-extends → negatives sort after positives in raw
/// u128), dropping the retraction.
#[test]
fn reduce_trace_seek_signed_pk() {
    use crate::schema::{SchemaColumn, TypeCode};

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    assert!(
        in_schema.pk_columns().any(|(_, c)| c.is_signed()),
        "test invariant: single signed PK"
    );

    let aggs = sum_count_aggs(1);
    let group_by = [0u32];

    // Tick 1: insert key=-1 -> 200, key=2 -> 100.
    let mut to_ch = empty_trace(out_schema);
    let delta1 = {
        let mut b = BatchBuilder::new(in_schema);
        for &(k, val) in &[(-1i64, 200i64), (2, 100)] {
            b.begin_row_opk(&[(k as u64) as u128], 1i64);
            b.put_int(val as u128);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &group_by, &aggs, None, false);
    assert_eq!(out1.count, 2, "two groups (-1 sorts before 2)");
    assert_eq!(opk_pk_i64(out1.get_pk_bytes(0)), -1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 200);

    // Tick 2: insert key=-1 -> 50. SUM goes 200 → 250: retract + insert.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let delta2 = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row_opk(&[((-1i64) as u64) as u128], 1i64);
        b.put_int(50);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &group_by, &aggs, None, false);
    assert_eq!(
        out2.count, 2,
        "signed-PK retraction must read trace_out and emit retract+insert"
    );
    assert_eq!(opk_pk_i64(out2.get_pk_bytes(0)), -1);
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(read_i64_le(out2.col_data(0), 0), 200, "retracted old SUM");
    assert_eq!(out2.get_weight(1), 1);
    assert_eq!(read_i64_le(out2.col_data(0), 8), 250, "new SUM");
}

#[test]
fn test_reduce_count() {
    use crate::schema::TypeCode;

    // Input: pk(U64), val(I64)
    let in_schema = make_schema_u64_i64();

    // Output: the input's PK region verbatim, then count(I64).
    let out_schema = SchemaDescriptor::new(
        &[
            crate::schema::SchemaColumn::new(TypeCode::U64, false),
            crate::schema::SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // 3 rows: pk=1,2,3 all GROUP BY pk (single group using pk as group)
    let delta = make_batch(&in_schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

    let agg = AggDescriptor::COUNT_STAR;

    // GROUP BY pk → each row is its own group
    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false);
    // Each pk forms its own group, COUNT=1 for each
    assert_eq!(out.count, 3);
    for i in 0..3 {
        let count = read_i64_le(out.col_data(0), i * 8);
        assert_eq!(count, 1, "each single-row group has count=1");
    }
}

// -----------------------------------------------------------------------
// Fix 1: Schema-agnostic reads for sub-8-byte columns
// -----------------------------------------------------------------------

/// `(pk, weight, value)` rows into a `(U64 pk, one narrow-int payload)` schema.
/// The payload is written at the column's own width by `put_int`, so one builder
/// covers every integer type the reduce tests exercise.
fn make_batch_typed(schema: &SchemaDescriptor, rows: &[(u64, i64, i128)]) -> Batch {
    let mut bb = BatchBuilder::new(*schema);
    for &(pk, w, val) in rows {
        bb.begin_row(pk as u128, w);
        bb.put_int(val as u128);
        bb.end_row();
    }
    let mut b = bb.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_reduce_sum_i32() {
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::I32, false));

    // Output: the input's PK region, sum(I64), count(I64) — trailing companion.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // 3 rows with I32 values, group by PK
    let delta = make_batch_typed(&in_schema, &[(1, 1, 100), (2, 1, 200), (3, 1, -50)]);

    let aggs = sum_count_aggs(1);

    // GROUP BY pk → each row is its own group
    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false);
    assert_eq!(out.count, 3);
    // Check values: row offsets depend on PK order (the PK-keyed group path)
    let sum0 = read_i64_le(out.col_data(0), 0);
    let sum1 = read_i64_le(out.col_data(0), 8);
    let sum2 = read_i64_le(out.col_data(0), 16);
    assert_eq!(sum0, 100, "SUM of I32 100");
    assert_eq!(sum1, 200, "SUM of I32 200");
    assert_eq!(sum2, -50, "SUM of I32 -50 (sign extension)");
}

#[test]
fn test_reduce_min_f32() {
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::F32, false));
    let agg = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };
    // MIN selects an existing row, so the output column keeps the F32 source type.
    let out_schema = out_schema_for(&in_schema, &[0u32], &[agg, AggDescriptor::COUNT_STAR]);
    assert_eq!(out_schema.columns[1].type_code, TypeCode::F32);
    let mut to_ch = empty_trace(out_schema);

    // pk(U64), val(F32), GROUP BY pk. Rows in (PK, payload) order so the
    // consolidated flag stamped below is honest.
    let mut bb = BatchBuilder::new(in_schema);
    for v in [-1.0, 3.5, 7.0] {
        bb.begin_row(1, 1);
        bb.put_float(v);
        bb.end_row();
    }
    let mut delta = bb.finish();
    delta.set_layout_unchecked(Layout::Consolidated);

    // GROUP BY pk → all 3 rows in same group
    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[0u32],
        &[agg, AggDescriptor::COUNT_STAR],
        None,
        false,
    );
    assert_eq!(out.count, 1);
    let min_val = f32::from_le_bytes(out.col_data(0)[0..4].try_into().unwrap());
    assert_eq!(min_val, -1.0f32, "MIN of F32 {{3.5, -1.0, 7.0}} should be -1.0");
}

#[test]
fn test_reduce_max_i16() {
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::I16, false));

    let agg = AggDescriptor { col_idx: 1, agg_op: AggFunc::Max };
    // MIN/MAX select an existing row, so the output column keeps the I16 source
    // width — the read below is 2 bytes, not 8.
    let out_schema = out_schema_for(&in_schema, &[0u32], &[agg, AggDescriptor::COUNT_STAR]);
    let mut to_ch = empty_trace(out_schema);

    // 3 rows with I16 values, all same PK, in (PK, payload) order so the
    // consolidated flag the helper stamps is honest.
    let delta = make_batch_typed(&in_schema, &[(1, 1, -100), (1, 1, 50), (1, 1, 200)]);

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[0u32],
        &[agg, AggDescriptor::COUNT_STAR],
        None,
        false,
    );
    assert_eq!(out.count, 1);
    let max_val = i16::from_le_bytes(out.col_data(0)[0..2].try_into().unwrap());
    assert_eq!(max_val, 200, "MAX of I16 {{-100, 200, 50}} should be 200");
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// UUID non-PK GROUP BY correctness
// -----------------------------------------------------------------------

/// Schema: pk(U64) + uuid_col(UUID) + i64_col(I64).
fn make_schema_u64_uuid_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::UUID, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

fn build_batch_u64_uuid_i64(schema: &SchemaDescriptor, rows: &[(u64, u128, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(*schema);
    for &(pk, uuid, val) in rows {
        bb.begin_row(pk as u128, 1);
        bb.put_int(uuid);
        bb.put_int(val as u128);
        bb.end_row();
    }
    let mut b = bb.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

/// MIN/MAX over a UUID payload column: the first row of a group seeds the
/// accumulator without a compare against its empty slot, later rows order on
/// all 16 bytes, and a retraction recedes through the value index.
#[test]
fn uuid_min_max_recede_through_the_value_index() {
    let in_schema = make_schema_u64_uuid_i64();
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Count },
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Max },
    ];
    let out_schema = out_schema_for(&in_schema, &[2u32], &aggs);
    let (lo, mid, hi) = (1u128, 1u128 << 64, u128::MAX - 1);
    let uuid_at = |b: &Batch, row: usize, pi: usize| {
        u128::from_le_bytes(b.col_data(pi)[row * 16..row * 16 + 16].try_into().unwrap())
    };

    let t1 = build_batch_u64_uuid_i64(&in_schema, &[(1, mid, 7), (2, hi, 7), (3, lo, 7)]);
    let mut trace = empty_trace(out_schema);
    let out1 = op_reduce(&t1, &mut trace, &in_schema, &[2u32], &aggs, None, false);
    assert_eq!(out1.count, 1);
    assert_eq!((uuid_at(&out1, 0, 1), uuid_at(&out1, 0, 2)), (lo, hi));

    let t2 = build_batch_u64_uuid_i64(&in_schema, &[(2, hi, 7), (3, lo, 7)]).negated();
    let mut avi = Avi::new(&in_schema, &[2u32], &aggs, &[&t1, &t2]);
    let mut trace2 = trace_cursor(out1, out_schema);
    let out2 = op_reduce(
        &t2,
        &mut trace2,
        &in_schema,
        &[2u32],
        &aggs,
        Some(&mut avi.cursor()),
        false,
    );
    let new_row = (0..out2.count).find(|&i| out2.get_weight(i) > 0).expect("new row");
    assert_eq!((uuid_at(&out2, new_row, 1), uuid_at(&out2, new_row, 2)), (mid, mid));
}

#[test]
fn test_group_runs_uuid_group() {
    // A non-nullable UUID group column keys by its image: the sort is by UUID value.
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::UUID, false));
    let uuid_a: u128 = 0x1000_0000_0000_0000_0000_0000_0000_0001u128;
    let uuid_b: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0002u128;
    // uuid_b < uuid_a (lower high byte)
    let mut bb = BatchBuilder::new(schema);
    for (pk, uuid) in [(1, uuid_a), (2, uuid_b)] {
        bb.begin_row(pk, 1);
        bb.put_int(uuid);
        bb.end_row();
    }
    let batch = bb.finish();
    let order = assert_runs_are_groups(&schema, &[1], &batch, |i| i as u128);
    // Row with uuid_b (row 1) should sort before row with uuid_a (row 0)
    assert_eq!(order, [1, 0], "uuid_b (smaller) must sort first");
}

#[test]
fn test_group_key_uuid_multi_col() {
    // Multi-column GROUP BY that includes a UUID column. Before fix, the group-key fold's
    // hash loop uses a [u8; 8] buffer for UUID (cs=16), panicking on buf[..16].
    let schema = make_schema_u64_uuid_i64();
    let uuid_a: u128 = 0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_0001u128;
    let uuid_b: u128 = 0x1111_2222_3333_4444_5555_6666_7777_8888u128;
    let batch = build_batch_u64_uuid_i64(&schema, &[(1, uuid_a, 42i64), (2, uuid_b, 42i64), (3, uuid_a, 43i64)]);
    let mb = batch.as_mem_batch();

    // GROUP BY (uuid_col=1, i64_col=2)
    let key0 = group_identity(&schema, &[1, 2], &mb, 0); // uuid_a, 42
    let key1 = group_identity(&schema, &[1, 2], &mb, 1); // uuid_b, 42
    let key2 = group_identity(&schema, &[1, 2], &mb, 2); // uuid_a, 43
    let key0b = group_identity(&schema, &[1, 2], &mb, 0); // same as key0

    assert_ne!(key0, key1, "different UUIDs same int must yield different group keys");
    assert_ne!(key0, key2, "same UUID different int must yield different group keys");
    assert_ne!(
        key1, key2,
        "different UUID different int must yield different group keys"
    );
    assert_eq!(key0, key0b, "same inputs must yield the same group key");
}

// -----------------------------------------------------------------------
// GROUP BY containing the PK column (mixed pk/non-pk group_by_cols).
// -----------------------------------------------------------------------

/// Schema: U64 pk (col 0) | I64 other (col 1). pk_index = 0.
fn make_schema_pk0_u64_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

/// Schema: I64 other (col 0) | U64 pk (col 1). pk_index = 1.
fn make_schema_pk1_i64_u64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[1],
    )
}

/// Build a 2-col batch (pk, other) with explicit pk values and `other` payload.
/// Works for either pk_index=0 or pk_index=1 since extend_col(pi, ..) addresses
/// the dense payload region — the non-PK column always lives at payload index 0.
fn build_pk_other(schema: &SchemaDescriptor, rows: &[(u64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, other) in rows {
        b.begin_row(pk as u128, 1i64);
        b.put_int(other as u128);
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_group_key_includes_pk_pki0() {
    // GROUP BY [pk, other] with pk_index=0: the hash loop must read the PK
    // column out of the PK region, not a payload slot.
    let schema = make_schema_pk0_u64_i64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();

    let k_pk10_v100 = group_identity(&schema, &[0, 1], &mb, 0);
    let k_pk20_v100 = group_identity(&schema, &[0, 1], &mb, 1);
    let k_pk10_v200 = group_identity(&schema, &[0, 1], &mb, 2);
    let k_pk10_v100_again = group_identity(&schema, &[0, 1], &mb, 0);

    assert_ne!(k_pk10_v100, k_pk20_v100, "different PKs, same other → distinct keys");
    assert_ne!(k_pk10_v100, k_pk10_v200, "same PK, different other → distinct keys");
    assert_eq!(k_pk10_v100, k_pk10_v100_again, "same row → same key");
}

#[test]
fn test_group_key_includes_pk_pki1() {
    // GROUP BY [other, pk] with pk_index=1: the PK column is read from the PK
    // region, not a payload slot.
    let schema = make_schema_pk1_i64_u64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();

    // group_by [col 0 = other, col 1 = pk]
    let k_pk10_v100 = group_identity(&schema, &[0, 1], &mb, 0);
    let k_pk20_v100 = group_identity(&schema, &[0, 1], &mb, 1);
    let k_pk10_v200 = group_identity(&schema, &[0, 1], &mb, 2);

    assert_ne!(k_pk10_v100, k_pk20_v100);
    assert_ne!(k_pk10_v100, k_pk10_v200);
}

#[test]
fn test_group_runs_pk_in_group() {
    // A multi-column group set containing the PK takes the hashed arm, whose
    // per-column fold must dispatch on the PK sentinel rather than a fake
    // payload index. Each (pk, other) pair is a group; the pairs must not split.
    let schema = make_schema_pk0_u64_i64();
    let batch = build_pk_other(&schema, &[(20, 100), (10, 200), (10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();
    assert_runs_are_groups(&schema, &[0, 1], &batch, |i| {
        let pk = gnitz_wire::widen_pk_be(mb.get_pk_bytes(i));
        let other = payload0_i64(&mb, i);
        (pk << 64) | (other as u64 as u128)
    });
}

// -----------------------------------------------------------------------
// NULL group columns must form a distinct group (not merged with 0).
// -----------------------------------------------------------------------

/// Schema: U64 pk | nullable I64.
/// Build a 2-col batch (pk, nullable_i64). For null rows, payload bytes
/// are zero (DirectWriter convention) and the null bit at payload pi=0 is set.
fn build_pk_null_i64(schema: &SchemaDescriptor, rows: &[(u64, Option<i64>)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, val) in rows {
        b.begin_row(pk as u128, 1);
        b.put_opt_int(val.map(|v| v as u128));
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_group_key_null_distinct_from_zero() {
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let batch = build_pk_null_i64(&schema, &[(1, None), (2, Some(0)), (3, Some(7)), (4, None)]);
    let mb = batch.as_mem_batch();

    let k_null = group_identity(&schema, &[1], &mb, 0);
    let k_zero = group_identity(&schema, &[1], &mb, 1);
    let k_seven = group_identity(&schema, &[1], &mb, 2);
    let k_null2 = group_identity(&schema, &[1], &mb, 3);

    assert_ne!(k_null, k_zero, "NULL must form a distinct group from 0");
    assert_ne!(k_null, k_seven);
    assert_ne!(k_zero, k_seven);
    assert_eq!(k_null, k_null2, "two NULL rows must collapse into the same group");
}

#[test]
fn test_group_runs_nullable_group_col() {
    // A nullable group column keys by the fold: NULL is one group, distinct from
    // the integer 0 it shares its stored bytes with, and its rows are adjacent.
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let batch = build_pk_null_i64(&schema, &[(1, Some(0)), (2, None), (3, Some(5)), (4, None)]);
    let mb = batch.as_mem_batch();
    let is_null = |i: usize| mb.get_null_word(i) & 1 != 0;
    // Group id: NULL gets its own id, distinct from every integer value.
    let order = assert_runs_are_groups(&schema, &[1], &batch, |i| {
        if is_null(i) {
            u128::MAX
        } else {
            payload0_i64(&mb, i) as u128
        }
    });
    let null_positions: Vec<usize> = order
        .iter()
        .enumerate()
        .filter(|&(_, &i)| is_null(i))
        .map(|(p, _)| p)
        .collect();
    assert_eq!(null_positions.len(), 2, "expected 2 NULL rows");
    assert_eq!(
        null_positions[1] - null_positions[0],
        1,
        "the two NULL rows form one contiguous group"
    );
}

// -----------------------------------------------------------------------
// Compound-PK reduce: byte-form emit + Accumulator PK-column read + order
// -----------------------------------------------------------------------

/// 2×U64 compound-PK input schema. pk_indices = [0, 1]; payload col is I64.
/// Build a 2×U64 compound-PK batch. Rows: (pk0, pk1, weight, val).
fn make_batch_compound_2xu64(schema: &SchemaDescriptor, rows: &[(u64, u64, i64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk0, pk1, w, val) in rows {
        b.begin_row_opk(&[pk0 as u128, pk1 as u128], w);
        b.put_int(val as u128);
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

/// emit_reduce_row natural-PK byte path on a 2×U64 compound PK: PK bytes
/// must be copied verbatim from the source row, not packed from group_key.
#[test]
fn test_emit_reduce_row_compound_pk_bytes() {
    let in_schema = pk_payload_schema(&[TypeCode::U64; 2]);

    let pk0: u64 = 0xAAAA_BBBB_CCCC_DDDDu64;
    let pk1: u64 = 0x1111_2222_3333_4444u64;
    let input = make_batch_compound_2xu64(&in_schema, &[(pk0, pk1, 1, 99)]);
    let mb = input.as_mem_batch();

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Count };
    // Natural-PK grouping passes the source row's PK bytes; they're copied verbatim.
    let plan = make_plan(&in_schema, &[0u32, 1u32], std::slice::from_ref(&agg), false);
    let mut output = Batch::with_capacity(&plan.shape.output_schema, 1);
    let accs = plan.shape.acc_template.clone();
    emit_reduce_row(
        &mut output,
        Some((&mb, 0, plan.shape.key.carried())),
        mb.get_pk_bytes(0),
        &accs,
    );

    assert_eq!(output.count, 1);
    // Source PK region is OPK (big-endian); the verbatim copy preserves it.
    let mut expected = [0u8; 16];
    expected[0..8].copy_from_slice(&pk0.to_be_bytes());
    expected[8..16].copy_from_slice(&pk1.to_be_bytes());
    assert_eq!(
        output.get_pk_bytes(0),
        &expected[..],
        "compound natural-PK output must copy source row's PK bytes verbatim"
    );
}

/// Accumulator MIN on the SECOND PK column of a 2×U64 compound PK.
/// Regression: the second PK column must be decoded by walking its
/// byte offset within the PK region, not by widening the whole
/// region to u128 (which would yield column 0).
#[test]
fn test_reduce_min_pk_col_compound_pk() {
    let in_schema = pk_payload_schema(&[TypeCode::U64; 2]);

    // The compound PK, then the I64 aggregate.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0, 1],
    );

    let mut to_ch = empty_trace(out_schema);

    // Two distinct compound PKs whose pk_col_0 and pk_col_1 disagree
    // about ordering: pk_col_1 values are 7 and 3 → MIN must be 3.
    let delta = make_batch_compound_2xu64(&in_schema, &[(10, 7, 1, 100), (20, 3, 1, 200)]);

    // MIN over the SECOND PK column (col_idx=1).
    let agg = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[0u32, 1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        None,
        false,
    );
    // The PK is the group key: each (pk0, pk1) is its own group, so we get one row
    // per input row; MIN within each group equals the row's pk_col_1.
    assert_eq!(out.count, 2);
    let mins: Vec<i64> = (0..out.count).map(|i| read_i64_le(out.col_data(0), i * 8)).collect();
    // Output is in pk_indices order = [0, 1] ascending, so (10,7) precedes (20,3).
    assert_eq!(
        mins,
        vec![7, 3],
        "MIN(pk_col_1) per single-row group must equal that row's pk_col_1"
    );
}

/// Single-PK U64 MIN(pk_col) — sanity check that the byte-offset
/// PK-read path produces the same result as the prior u128 path.
#[test]
fn test_reduce_min_pk_col_single_pk_u64() {
    let in_schema = make_schema_u64_i64();
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // Input is PK-sorted from consolidation; the PK-keyed group walk
    // passes that order straight through.
    let delta = make_batch(&in_schema, &[(7, 1, 0), (42, 1, 0), (99, 1, 0)]);

    let agg = AggDescriptor { col_idx: 0, agg_op: AggFunc::Min };
    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[0u32],
        &[agg, AggDescriptor::COUNT_STAR],
        None,
        false,
    );
    // GROUP BY pk → each row is its own group; MIN(pk) per group equals the row's pk.
    assert_eq!(out.count, 3);
    let mins: Vec<i64> = (0..out.count).map(|i| read_i64_le(out.col_data(0), i * 8)).collect();
    assert_eq!(mins, vec![7, 42, 99]);
}

// -----------------------------------------------------------------------
// Compound-PK subset grouping: PK-region access must be per-PK-column
// (pre-fix the slow path widened the entire region and split groups
// that share the addressed PK column but differ in other PK columns).
// -----------------------------------------------------------------------

/// The group key of `GROUP BY pk_col_0` (single PK column of a
/// compound PK) must return the same u128 for two rows that share
/// pk_col_0 — distinct pk_col_1 values must not collide them into
/// different groups.
#[test]
fn test_group_key_single_pk_col_compound_subset() {
    let schema = pk_payload_schema(&[TypeCode::U64; 2]);
    let batch = make_batch_compound_2xu64(&schema, &[(10, 50, 1, 0), (10, 99, 1, 0), (20, 50, 1, 0)]);
    let mb = batch.as_mem_batch();

    let k0 = group_identity(&schema, &[0u32], &mb, 0);
    let k1 = group_identity(&schema, &[0u32], &mb, 1);
    let k2 = group_identity(&schema, &[0u32], &mb, 2);

    assert_eq!(k0, 10u128, "key must equal pk_col_0 value (10), not whole PK region");
    assert_eq!(k0, k1, "rows sharing pk_col_0 must hash to the same group key");
    assert_eq!(k2, 20u128);
    assert_ne!(k0, k2);
}

/// Pair-test on single-PK U64: the group key must still return
/// the full PK region (bit-identical to the prior whole-region widen).
#[test]
fn test_group_key_single_pk_col_single_pk_bit_identical() {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(42, 1, 100), (99, 1, 200)]);
    let mb = batch.as_mem_batch();

    let k0 = group_identity(&schema, &[0u32], &mb, 0);
    let k1 = group_identity(&schema, &[0u32], &mb, 1);
    assert_eq!(k0, 42u128, "single PK widens to the same value as before");
    assert_eq!(k1, 99u128);
}

/// End-to-end op_reduce: GROUP BY pk_col_0 (a strict subset of a
/// compound PK) with COUNT(*). Pre-fix the slow path widened the
/// whole PK region and split every (pk_col_0, pk_col_1) pair into
/// its own group; the fix collapses rows sharing pk_col_0.
#[test]
fn test_op_reduce_compound_pk_group_by_subset_count() {
    let in_schema = pk_payload_schema(&[TypeCode::U64; 2]);
    // Output: U64 pk (the group column) + I64 count.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // PK-sorted (pk0, pk1): (1,10), (1,20), (2,10).
    let delta = make_batch_compound_2xu64(&in_schema, &[(1, 10, 1, 0), (1, 20, 1, 0), (2, 10, 1, 0)]);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Count };

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false);

    // Two groups: pk_col_0=1 (count=2), pk_col_0=2 (count=1).
    // Pre-fix the count would be 3 (one row per (pk0, pk1) pair).
    assert_eq!(
        out.count, 2,
        "GROUP BY pk_col_0 collapses (1,10) and (1,20) into one group"
    );

    // Output rows in pk_col_0 ascending order.
    let mut entries: Vec<(u64, i64)> = (0..out.count)
        .map(|i| {
            let pk_bytes = out.get_pk_bytes(i);
            // Output group-key PK is unsigned U64, OPK == BE at rest.
            let pk = u64::from_be_bytes(pk_bytes.try_into().unwrap());
            let cnt = read_i64_le(out.col_data(0), i * 8);
            (pk, cnt)
        })
        .collect();
    entries.sort_by_key(|&(pk, _)| pk);
    assert_eq!(entries, vec![(1, 2), (2, 1)]);
}

// -----------------------------------------------------------------------
// U64 MIN/MAX: unsigned ordering for values with the high bit set.
//
// The U64 widening returns the bit pattern reinterpreted as `i64`;
// signed `<`/`>` flips for values >= 2^63. The fix dispatches on
// TypeCode::U64 in the MIN/MAX comparison sites.
// -----------------------------------------------------------------------

/// 3-col schema: U64 pk, I64 grp, U64 val. All values aggregated by grp
/// fall into a single group when `grp` is held constant.
fn make_schema_u64pk_i64grp_u64val() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0],
    )
}

fn make_batch_u64pk_i64grp_u64val(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, i64, u64)], // (pk, weight, grp, u64_val)
) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, w, grp, val) in rows {
        b.begin_row(pk as u128, w);
        b.put_int(grp as u128);
        b.put_int(val as u128);
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

/// `[U64 pk, I64 grp, I64 val]` — group by the I64 payload `grp`. `val_nullable`
/// picks the two shapes the reduce tests need: NOT NULL keeps the reduce output on
/// the null-blind fixed-int comparator, nullable is required wherever a test writes
/// a null bit into `val` (and is what the non-linear delta is then consolidated
/// under). `grp` stays NOT NULL, so it is the reduce's output PK.
fn u64pk_i64grp_i64val(val_nullable: bool) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, val_nullable),
        ],
        &[0],
    )
}

fn make_batch_u64pk_i64grp_i64val(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, i64, i64)], // (pk, weight, grp, i64_val)
) -> Batch {
    // `val as u64` writes the identical LE bytes; the twin builders differ only
    // in the tuple's val type.
    let rows: Vec<(u64, i64, i64, u64)> = rows.iter().map(|&(pk, w, grp, val)| (pk, w, grp, val as u64)).collect();
    make_batch_u64pk_i64grp_u64val(schema, &rows)
}

#[test]
fn test_reduce_min_u64_high_bit_set() {
    let in_schema = make_schema_u64pk_i64grp_u64val();
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };
    let out_schema = out_schema_for(&in_schema, &[1u32], &[agg, AggDescriptor::COUNT_STAR]);

    let mut to_ch = empty_trace(out_schema);

    // One group (grp=7). val=1, u64::MAX, and 2^63 — the unsigned MIN is 1.
    // Pre-fix signed comparison treats u64::MAX as -1 (smallest signed),
    // so the bug reports u64::MAX as the MIN.
    let delta = make_batch_u64pk_i64grp_u64val(
        &in_schema,
        &[(1, 1, 7, u64::MAX), (2, 1, 7, 10), (3, 1, 7, 1u64 << 63), (4, 1, 7, 1)],
    );

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        None,
        false,
    );
    assert_eq!(out.count, 1);
    let min_bits = u64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(min_bits, 1u64, "MIN(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_max_u64_high_bit_set() {
    let in_schema = make_schema_u64pk_i64grp_u64val();
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Max };
    let out_schema = out_schema_for(&in_schema, &[1u32], &[agg, AggDescriptor::COUNT_STAR]);

    let mut to_ch = empty_trace(out_schema);

    // Same input as MIN test. Unsigned MAX is u64::MAX. Pre-fix signed
    // comparison treats 10 (positive i64) as larger than u64::MAX (=-1).
    let delta = make_batch_u64pk_i64grp_u64val(
        &in_schema,
        &[(1, 1, 7, u64::MAX), (2, 1, 7, 10), (3, 1, 7, 1u64 << 63), (4, 1, 7, 1)],
    );

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        None,
        false,
    );
    assert_eq!(out.count, 1);
    let max_bits = u64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(max_bits, u64::MAX, "MAX(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_min_u64_incremental() {
    let in_schema = make_schema_u64pk_i64grp_u64val();
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };
    let out_schema = out_schema_for(&in_schema, &[1u32], &[agg, AggDescriptor::COUNT_STAR]);

    // Tick 1: one row with val=1u64<<60 → MIN = 1u64<<60.
    let mut to_ch = empty_trace(out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 1u64 << 60)]);

    let mut avi1 = Avi::new(&in_schema, &[1u32], &[agg, AggDescriptor::COUNT_STAR], &[&delta1]);
    let out1 = op_reduce(
        &delta1,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi1.cursor()),
        false,
    );
    assert_eq!(out1.count, 1);
    let min1 = u64::from_le_bytes(out1.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(min1, 1u64 << 60);

    // Tick 2: delta adds a row with val=1u64<<63, so the index holds both.
    //
    // MIN(1u64<<60, 1u64<<63) is unchanged at 1u64<<60 under unsigned;
    // under buggy signed compare it would flip to 1u64<<63 = i64::MIN.
    // op_reduce emits retract+new even when the value didn't change, so
    // we get 2 rows; we assert the new emitted value is the unsigned MIN.
    let mut to_ch2 = trace_cursor(out1, out_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, 1u64 << 63)]);
    let mut avi2 = Avi::new(
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        &[&delta1, &delta2],
    );

    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi2.cursor()),
        false,
    );
    assert_eq!(out2.count, 2, "retract old MIN + emit new MIN");
    let retracted = u64::from_le_bytes(out2.col_data(0)[0..8].try_into().unwrap());
    let new_min = u64::from_le_bytes(out2.col_data(0)[8..16].try_into().unwrap());
    assert_eq!(retracted, 1u64 << 60);
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(
        new_min,
        1u64 << 60,
        "MIN unchanged under unsigned ordering; bug would flip it to 1u64<<63"
    );
    assert_eq!(out2.get_weight(1), 1);
}

#[test]
fn test_reduce_max_u64_incremental() {
    let in_schema = make_schema_u64pk_i64grp_u64val();
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Max };
    let out_schema = out_schema_for(&in_schema, &[1u32], &[agg, AggDescriptor::COUNT_STAR]);

    // Tick 1: MAX over a single low value → MAX = 10.
    let mut to_ch = empty_trace(out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 10)]);

    let mut avi1 = Avi::new(&in_schema, &[1u32], &[agg, AggDescriptor::COUNT_STAR], &[&delta1]);
    let out1 = op_reduce(
        &delta1,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi1.cursor()),
        false,
    );
    assert_eq!(out1.count, 1);
    let max1 = u64::from_le_bytes(out1.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(max1, 10);

    // Tick 2: delta adds val=u64::MAX, so the index holds both.
    // Pre-fix signed MAX would treat u64::MAX as -1, keeping MAX=10.
    let mut to_ch2 = trace_cursor(out1, out_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, u64::MAX)]);
    let mut avi2 = Avi::new(
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        &[&delta1, &delta2],
    );

    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi2.cursor()),
        false,
    );
    // Expect: retract old MAX (10) + emit new MAX (u64::MAX).
    assert_eq!(out2.count, 2);
    let retracted = u64::from_le_bytes(out2.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(retracted, 10);
    assert_eq!(out2.get_weight(0), -1);
    let new_max = u64::from_le_bytes(out2.col_data(0)[8..16].try_into().unwrap());
    assert_eq!(new_max, u64::MAX, "new MAX must be unsigned-max u64::MAX");
    assert_eq!(out2.get_weight(1), 1);
}

#[test]
fn test_avi_seed_u64_high_bit() {
    // The AVI fast path seeds an Accumulator with a U64 order image via
    // `seed_from_index`, then folds in delta rows via `step_from_batch`.
    // Validates that the U64 bit pattern preserved by the AVI seed
    // compares correctly under unsigned semantics against incoming
    // delta rows.
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::U64, false));

    let desc = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };
    let mut acc = make_acc(&in_schema, &[0], desc);

    // AVI seeds the accumulator with 1u64<<63 (high bit set); a U64's order
    // image is the value itself.
    acc.seed_from_index(&(1u64 << 63).to_be_bytes());
    assert_eq!(acc.value_bits(), 1u64 << 63);

    // Build a batch with a single row val=10u64, pk=1.
    let batch = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, 1i64);
        b.put_int(10);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let mb = batch.as_mem_batch();
    acc.step_from_batch(&mb, 0, 1);

    // 10u64 < (1u64<<63) under unsigned: MIN updates to 10.
    // Under buggy signed comparison: 10i64 > i64::MIN, MIN stays at i64::MIN.
    assert_eq!(
        acc.value_bits(),
        10u64,
        "unsigned MIN against AVI-seeded U64 high-bit value"
    );
}

#[test]
fn test_reduce_min_max_i64_boundary() {
    // Guard that the TypeCode::U64 branch does not leak into I64 paths:
    // MIN of {i64::MIN, -1, 0, i64::MAX} = i64::MIN,
    // MAX = i64::MAX.
    let in_schema = u64pk_i64grp_i64val(false);
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };
    let out_schema = out_schema_for(&in_schema, &[1u32], &[agg, AggDescriptor::COUNT_STAR]);

    // MIN test.
    {
        let mut to_ch = empty_trace(out_schema);

        let delta = make_batch_u64pk_i64grp_i64val(
            &in_schema,
            &[(1, 1, 7, i64::MIN), (2, 1, 7, -1), (3, 1, 7, 0), (4, 1, 7, i64::MAX)],
        );

        let out = op_reduce(
            &delta,
            &mut to_ch,
            &in_schema,
            &[1u32],
            &[agg, AggDescriptor::COUNT_STAR],
            None,
            false,
        );
        assert_eq!(out.count, 1);
        let min = read_i64_le(out.col_data(0), 0);
        assert_eq!(min, i64::MIN, "MIN(I64) signed ordering preserved");
    }

    // MAX test.
    {
        let mut to_ch = empty_trace(out_schema);

        let delta = make_batch_u64pk_i64grp_i64val(
            &in_schema,
            &[(1, 1, 7, i64::MIN), (2, 1, 7, -1), (3, 1, 7, 0), (4, 1, 7, i64::MAX)],
        );

        let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Max };

        let out = op_reduce(
            &delta,
            &mut to_ch,
            &in_schema,
            &[1u32],
            &[agg, AggDescriptor::COUNT_STAR],
            None,
            false,
        );
        assert_eq!(out.count, 1);
        let max = read_i64_le(out.col_data(0), 0);
        assert_eq!(max, i64::MAX, "MAX(I64) signed ordering preserved");
    }
}

// -----------------------------------------------------------------------
// PK-keyed group walk on unsorted input
//
// op_reduce's PK-keyed group walk visits rows in physical order and
// treats consecutive same-PK rows as one group. That assumption only
// holds when `working` is sorted; an unsorted delta (e.g. from
// `map_reindex` upstream) splits one PK into multiple groups and
// produces duplicate PK rows / double-retractions.
// -----------------------------------------------------------------------

/// Build a raw `Batch` (`sorted = false`, `consolidated = false`) with
/// one I64 payload column. `pk_encode` maps the row's PK type to the
/// u128 that `extend_pk` expects (e.g. `|pk: i64| (pk as u64) as u128`
/// for signed, `|pk: u64| pk as u128` for unsigned).
fn make_batch_raw_pk<T: Copy>(
    schema: &SchemaDescriptor,
    rows: &[(T, i64, i64)],
    pk_encode: impl Fn(T) -> u128,
) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, w, val) in rows {
        // pk_encode yields the native value; OPK-encode (sign-flip for signed).
        b.begin_row_opk(&[pk_encode(pk)], w);
        b.put_int(val as u128);
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Raw);
    b
}

/// `[Sum(sum_col, sum_tc), Count(col 0)]` — a linear SUM plus the appended
/// cardinality companion every circuit reduce carries, the agg_descs the
/// planner produces for `SELECT …, SUM(v) … GROUP BY …`.
fn sum_count_aggs(sum_col: u32) -> [AggDescriptor; 2] {
    [
        AggDescriptor { col_idx: sum_col, agg_op: AggFunc::Sum },
        AggDescriptor::COUNT_STAR,
    ]
}

#[test]
fn test_reduce_group_by_pk_unsorted_input_linear_sum() {
    let in_schema = make_schema_u64_i64();
    let aggs = sum_count_aggs(1);
    let out_schema = out_schema_for(&in_schema, &[0u32], &aggs);
    let mut to_ch = empty_trace(out_schema);

    // Unsorted: pk=5 appears twice, separated by pk=3. The fast path
    // pre-fix walked physical order and split into 3 groups → emitting
    // two distinct pk=5 rows.
    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false);

    assert_eq!(out.count, 2, "one row per distinct PK");
    let pk0 = out.get_pk_bytes(0);
    let pk1 = out.get_pk_bytes(1);
    let pk0_val = gnitz_wire::widen_pk_be(pk0);
    let pk1_val = gnitz_wire::widen_pk_be(pk1);
    assert_eq!(pk0_val, 3, "PK-sorted: 3 precedes 5");
    assert_eq!(pk1_val, 5);
    let sum0 = read_i64_le(out.col_data(0), 0);
    let sum1 = read_i64_le(out.col_data(0), 8);
    assert_eq!(sum0, 20, "SUM for pk=3");
    assert_eq!(sum1, 40, "SUM for pk=5 (10+30) — pre-fix produced two split rows");
    assert!(out.is_consolidated(), "reduce output is certified consolidated");
}

#[test]
fn test_reduce_group_by_pk_unsorted_input_count() {
    let in_schema = make_schema_u64_i64();
    let agg = AggDescriptor { col_idx: 1, agg_op: AggFunc::Count };
    let out_schema = out_schema_for(&in_schema, &[0u32], std::slice::from_ref(&agg));
    let mut to_ch = empty_trace(out_schema);

    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false);

    assert_eq!(out.count, 2);
    let pk0 = out_pk(&out, 0);
    let pk1 = out_pk(&out, 1);
    assert_eq!((pk0, pk1), (3, 5));
    let c0 = read_i64_le(out.col_data(0), 0);
    let c1 = read_i64_le(out.col_data(0), 8);
    assert_eq!((c0, c1), (1, 2), "pk=3 → 1, pk=5 → 2");
}

#[test]
fn test_reduce_group_by_pk_unsorted_sorted_input_equivalence() {
    let in_schema = make_schema_u64_i64();
    let aggs = sum_count_aggs(1);
    let out_schema = out_schema_for(&in_schema, &[0u32], &aggs);
    let mut to_ch = empty_trace(out_schema);

    // Same data as the unsorted-sum test but already in (PK, payload) order. The
    // consolidated input, whose group runs need no sort, must produce
    // identical output.
    let mut delta = make_batch_raw_pk(&in_schema, &[(3, 1, 20), (5, 1, 10), (5, 1, 30)], |pk: u64| pk as u128);
    delta.certify_layout(Layout::Consolidated);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false);

    assert_eq!(out.count, 2);
    let pk0 = out_pk(&out, 0);
    let pk1 = out_pk(&out, 1);
    assert_eq!((pk0, pk1), (3, 5));
    let sum0 = read_i64_le(out.col_data(0), 0);
    let sum1 = read_i64_le(out.col_data(0), 8);
    assert_eq!((sum0, sum1), (20, 40));
}

#[test]
fn test_reduce_group_by_pk_unsorted_compound_pk() {
    let in_schema = pk_payload_schema(&[TypeCode::U64; 2]);
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0, 1],
    );
    let mut to_ch = empty_trace(out_schema);

    // Unsorted compound-PK delta: physical order is (2,1) then (1,2).
    // Canonical pk_indices order should emit (1,2) first.
    let mut delta = make_batch_compound_2xu64(&in_schema, &[(2, 1, 1, 20), (1, 2, 1, 10)]);
    delta.set_layout_unchecked(Layout::Raw);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Count };

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32, 1u32], &[agg], None, false);

    assert_eq!(out.count, 2);
    let pk0 = out.get_pk_bytes(0);
    let pk1 = out.get_pk_bytes(1);
    let p0_col0 = u64::from_be_bytes(pk0[0..8].try_into().unwrap());
    let p0_col1 = u64::from_be_bytes(pk0[8..16].try_into().unwrap());
    let p1_col0 = u64::from_be_bytes(pk1[0..8].try_into().unwrap());
    let p1_col1 = u64::from_be_bytes(pk1[8..16].try_into().unwrap());
    // Canonical pk_indices order is [col0, col1] ascending — (1,2) first.
    // A u128.cmp on the widened PK region would put (2,1) first because
    // col1 dominates the high bytes.
    assert_eq!(
        (p0_col0, p0_col1),
        (1, 2),
        "compound-PK canonical sort: pk_indices priority, not u128 priority"
    );
    assert_eq!((p1_col0, p1_col1), (2, 1));
}

#[test]
fn test_reduce_group_by_pk_unsorted_signed_pk() {
    let in_schema = make_schema_i64pk_i64();
    // The output PK region is the input's, so it carries the signed source PK
    // verbatim (OPK, sign-flipped); we only check ordering of the payload. The
    // trailing I64 is the cardinality companion every circuit reduce carries.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let mut to_ch = empty_trace(out_schema);

    // Unsorted signed-PK delta. A u128.cmp on the widened (zero-extended)
    // i64-as-u64 bit pattern would put negatives at the TOP (they widen
    // to large u64 values), so emit order would start with pk=2.
    let delta = make_batch_raw_pk(&in_schema, &[(-1, 1, 10), (2, 1, 20), (-3, 1, 30)], |pk: i64| {
        (pk as u64) as u128
    });

    let aggs = sum_count_aggs(1);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false);

    assert_eq!(out.count, 3, "one row per distinct signed PK");
    let pks: Vec<i64> = (0..out.count)
        .map(|i| {
            // The output PK region is the input's, so it holds the I64's own
            // OPK bytes verbatim; decode them back to native.
            opk_pk_i64(out.get_pk_bytes(i))
        })
        .collect();
    let sums: Vec<i64> = (0..out.count).map(|i| read_i64_le(out.col_data(0), i * 8)).collect();
    // Canonical signed order: -3, -1, 2.
    assert_eq!(
        pks,
        vec![-3, -1, 2],
        "signed PK must sort via i64 order, not u128-of-bits order"
    );
    assert_eq!(sums, vec![30, 10, 20]);
}

#[test]
fn test_reduce_group_by_pk_unsorted_with_retraction() {
    let in_schema = make_schema_u64_i64();
    let aggs = sum_count_aggs(1);
    let out_schema = out_schema_for(&in_schema, &[0u32], &aggs);

    // Build a pre-populated trace_out carrying (pk=5, SUM=100, count=1). The
    // trailing count is the cardinality companion; the old group's one prior row
    // gives count=1, so the group survives the +1 insert delta.
    let mut prev = BatchBuilder::new(out_schema);
    prev.begin_row(5u128, 1i64);
    prev.put_int(100);
    prev.put_int(1);
    prev.end_row();
    let mut prev = prev.finish();
    prev.set_layout_unchecked(Layout::Consolidated);
    let mut to_ch = trace_cursor(prev, out_schema);

    // Unsorted delta with pk=5 split across the batch. Pre-fix: emits
    // TWO `(pk=5, w=-1, SUM=100)` retractions plus split partials.
    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false);

    // Expected: one retract (pk=5, w=-1, SUM=100), one emit (pk=5,
    // w=+1, SUM=140), one emit (pk=3, w=+1, SUM=20). Order is canonical:
    // pk=3 first, then pk=5 (retract+emit).
    assert_eq!(
        out.count, 3,
        "exactly one retract + one new emit for pk=5 plus pk=3 emit"
    );

    let mut by_pk_w: Vec<(u128, i64, i64)> = (0..out.count)
        .map(|i| {
            let pk = out_pk(&out, i);
            let w = out.get_weight(i);
            let sum = read_i64_le(out.col_data(0), i * 8);
            (pk, w, sum)
        })
        .collect();
    by_pk_w.sort_by_key(|&(pk, w, _)| (pk, w));

    assert_eq!(
        by_pk_w,
        vec![(3, 1, 20), (5, -1, 100), (5, 1, 140),],
        "single retract+emit for pk=5 (sum 10+30+100=140), single emit for pk=3"
    );
}

#[test]
fn test_reduce_min_group_by_pk_retracts_extreme() {
    // PK-keyed MIN over a non-unique-PK input: one PK carrying two payloads,
    // GROUP BY the full PK. Retracting the payload that holds the current MIN
    // must recompute MIN from the survivor — which is what the value index's
    // weight consolidation does: the retracted entry nets to zero and the seek
    // walks past it to 20.
    let in_schema = make_schema_u64_i64();
    let agg = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };
    let out_schema = out_schema_for(&in_schema, &[0u32], &[agg, AggDescriptor::COUNT_STAR]);

    // History: pk=1 with two payloads (val=10, val=20). The group IS the PK, so
    // both belong to group pk=1; MIN(10, 20) = 10.
    let history = make_batch(&in_schema, &[(1, 1, 10), (1, 1, 20)]);

    // trace_out: old MIN(pk=1) = 10.
    let mut prev = BatchBuilder::new(out_schema);
    prev.begin_row(1u128, 1i64);
    prev.put_int(10);
    prev.put_int(2);
    prev.end_row();
    let mut prev = prev.finish();
    prev.set_layout_unchecked(Layout::Consolidated);
    let mut to_ch = trace_cursor(prev, out_schema);

    // Delta: retract (pk=1, val=10) — the payload holding the current MIN.
    let delta = make_batch(&in_schema, &[(1, -1, 10)]);

    // The index carries the history plus this delta's retraction, as the
    // compiler's Integrate-before-Reduce order produces.
    let mut avi = Avi::new(
        &in_schema,
        &[0u32],
        &[agg, AggDescriptor::COUNT_STAR],
        &[&history, &delta],
    );
    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[0u32], // GROUP BY PK col 0 → the PK region is the group key
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi.cursor()),
        false,
    );

    // Retract old MIN=10 (w=-1) + emit new MIN=20 (w=+1). If the retracted
    // index entry did not net to zero, MIN would stay 10 and the emitted +1 row
    // would carry 10 instead of 20.
    assert_eq!(out.count, 2, "one retract + one re-emit for pk=1");
    let mut by_w: Vec<(i64, i64)> = (0..out.count)
        .map(|i| (out.get_weight(i), read_i64_le(out.col_data(0), i * 8)))
        .collect();
    by_w.sort_by_key(|&(w, _)| w);
    assert_eq!(
        by_w,
        vec![(-1, 10), (1, 20)],
        "retract old MIN=10, insert new MIN=20 after the val=10 payload is retracted",
    );
}

// -----------------------------------------------------------------------
// Byte-form AVI: the index lookup walks the full group-key prefix, so two
// distinct groups never share a bucket. This is the only path that drives
// `seek_first_positive_with_prefix` end to end; the other reduce tests take
// the trace-scan fallback (avi = None).
// -----------------------------------------------------------------------

/// Write one AVI group-key column into `dst`, exactly as the production
/// the AVI key packer does: the column's **OPK** image at its declared width. The
/// AVI schema declares each group column a PK column, so its region holds
/// order-preserving big-endian bytes (sign-flipped when signed), not native LE.
fn avi_gcol(dst: &mut [u8], native_le: &[u8], tc: TypeCode) {
    gnitz_wire::encode_pk_column(native_le, tc, dst);
}

#[test]
fn avi_two_groups_distinct_byte_form_keys() {
    // Input: pk(U64), a(U32), b(U32), val(I64); GROUP BY (a, b), MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );

    // delta: one row per group. The delta values are deliberately NOT each
    // group's minimum, so a correct result can only come from the index.
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        for (pk, a, bb, val) in [(1u64, 1u32, 1u32, 50i64), (2, 2, 2, 60)] {
            b.begin_row(pk as u128, 1i64);
            b.put_int(a as u128);
            b.put_int(bb as u128);
            b.put_int(val as u128);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // AVI: key = a(4) ++ b(4) ++ av_encoded(8). Group (1,1) min=10, (2,2) min=20.
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 17, "4 + 4 + 1 + 8");
    let avi_batch = {
        let mut b = BatchBuilder::new(avi_schema);
        for (a, bb, min) in [(1u32, 1u32, 10i64), (2, 2, 20)] {
            let mut key = [0u8; 17];
            avi_gcol(&mut key[0..4], &a.to_le_bytes(), TypeCode::U32);
            avi_gcol(&mut key[4..8], &bb.to_le_bytes(), TypeCode::U32);
            let av = i64_av(min);
            key[8] = 0; // ordinal 0 (single MIN aggregate)
            key[9..17].copy_from_slice(&av.to_be_bytes());
            b.begin_row_bytes(&key, 1i64);
            b.end_row();
        }
        b.finish()
    };

    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    // Delta row 0 is group (1,1), row 1 group (2,2).
    for (row, expected) in [(0, 10i64), (1, 20)] {
        let min = probe_indexed(&in_schema, &[1u32, 2u32], agg, &delta, row, &mut avi_ch).expect("an indexed MIN");
        assert_eq!(
            min.value_bits() as i64,
            expected,
            "row {row}'s group must resolve its own indexed MIN, not the other group's"
        );
    }
}

// -----------------------------------------------------------------------
// AVI lookup on a retraction: when the current MIN is retracted, the new value
// is taken from the index (its post-state), and the old value is retracted
// from trace_out. The lookup must return the smallest surviving entry — the one
// history a non-linear reduce has. (The +1/-1 consolidation of the retracted
// extremum is the AVI table cursor's job, exercised in the storage consolidation
// tests; here the AVI holds the post-state directly.)
// -----------------------------------------------------------------------

#[test]
fn avi_retraction_returns_next_extremum() {
    // Input: pk(U64), a(U32), val(I64); GROUP BY a, MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    // delta: retract the row holding the current MIN (val=5) of group a=1.
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, -1i64);
        b.put_int(1);
        b.put_int(5);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // AVI (post-state for group a=1): the retracted 5 is gone; the surviving
    // values are {10, 20}, so the prefix walk must return the smaller, 10.
    let avi_schema = avi_schema(&in_schema, &[1u32]);
    let avi_batch = {
        let mut b = BatchBuilder::new(avi_schema);
        for min in [10i64, 20] {
            let mut key = [0u8; 13];
            avi_gcol(&mut key[0..4], &1u32.to_le_bytes(), TypeCode::U32);
            let av = i64_av(min);
            key[4] = 0; // ordinal 0 (single MIN aggregate)
            key[5..13].copy_from_slice(&av.to_be_bytes());
            b.begin_row_bytes(&key, 1i64);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // trace_out: previous output for a=1 was MIN=5.
    let to_batch = {
        let mut b = BatchBuilder::new(out_schema);
        b.begin_row_bytes(&1u32.to_be_bytes(), 1i64);
        b.put_int(5);
        b.put_int(3);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = trace_cursor(to_batch, out_schema);
    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi_ch),
        false,
    );

    // Expect a retraction of the old MIN (5, weight -1) and the recomputed MIN
    // (10, weight +1) read from the index — never the retracted 5.
    let mut retracted = None;
    let mut inserted = None;
    for i in 0..out.count {
        let w = out.get_weight(i);
        let v = read_i64_le(out.col_data(0), i * 8);
        if w < 0 {
            retracted = Some(v);
        } else {
            inserted = Some(v);
        }
    }
    assert_eq!(retracted, Some(5), "must retract the stale MIN");
    assert_eq!(
        inserted,
        Some(10),
        "AVI must skip the net-zero retracted extremum and return the next MIN"
    );
}

// -----------------------------------------------------------------------
// A single narrow group column gives the AVI composite a non-power-of-two
// stride (U16 → 10, U32 → 12). The AVI cursor's `drive` widens the PK region
// through `widen_pk_le`, which must zero-extend these strides rather than
// panic.
// -----------------------------------------------------------------------

#[test]
fn avi_non_power_of_two_stride_drives_cursor() {
    for (gtc, gsize, stride) in [(TypeCode::U16, 2usize, 11usize), (TypeCode::U32, 4, 13)] {
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(gtc, false),
                SchemaColumn::new(TypeCode::I64, false),
            ],
            &[0],
        );
        let gval: u64 = 7;
        let delta = {
            let mut b = BatchBuilder::new(in_schema);
            b.begin_row(1u128, 1i64);
            b.put_int(gval as u128);
            b.put_int(100);
            b.end_row();
            let mut b = b.finish();
            b.set_layout_unchecked(Layout::Consolidated);
            b
        };

        let avi_schema = avi_schema(&in_schema, &[1u32]);
        assert_eq!(avi_schema.pk_stride(), stride);
        let avi_batch = {
            let mut b = BatchBuilder::new(avi_schema);
            let mut key = vec![0u8; stride];
            avi_gcol(&mut key[..gsize], &gval.to_le_bytes()[..gsize], gtc);
            let av = i64_av(42i64);
            key[gsize] = 0; // ordinal
            key[gsize + 1..gsize + 9].copy_from_slice(&av.to_be_bytes());
            b.begin_row_bytes(&key, 1i64);
            b.end_row();
            b.finish()
        };

        let mut avi_ch = trace_cursor(avi_batch, avi_schema);

        let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

        let min = probe_indexed(&in_schema, &[1u32], agg, &delta, 0, &mut avi_ch).expect("an indexed MIN");
        assert_eq!(min.value_bits() as i64, 42, "stride {stride}: indexed MIN");
    }
}

// -----------------------------------------------------------------------
// Trace-scan fallback (avi = None): retracting a group's current MIN must
// recompute the next-best from the replayed history, not re-emit the stale
// value. Guards the `fill_cleared_batch` consolidation-flag reset.
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Tie: a group holds two rows at the current MIN. Retracting one copy must
// leave the MIN unchanged — the surviving copy still pins it. The two copies
// share one index entry, so its net weight must stay positive.
// -----------------------------------------------------------------------

#[test]
fn min_tie_retract_one_copy_keeps_min() {
    // Input: pk(U64), g(I64), val(I64); GROUP BY g, MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    let mk_row = |b: &mut BatchBuilder, pk: u64, w: i64, g: i64, val: i64| {
        b.begin_row(pk as u128, w);
        b.put_int(g as u128);
        b.put_int(val as u128);
        b.end_row();
    };

    // History: group g=1 holds val=5 twice (pk=1, pk=2) and val=10 (pk=3).
    let ti_batch = {
        let mut b = BatchBuilder::new(in_schema);
        mk_row(&mut b, 1, 1, 1, 5);
        mk_row(&mut b, 2, 1, 1, 5);
        mk_row(&mut b, 3, 1, 1, 10);
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let to_batch = {
        let mut b = BatchBuilder::new(out_schema);
        b.begin_row_bytes(&opk_pk(&out_schema, &[1]), 1i64);
        b.put_int(5);
        b.put_int(3);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // delta: retract one of the two val=5 rows (pk=1).
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        mk_row(&mut b, 1, -1, 1, 5);
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = trace_cursor(to_batch, out_schema);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    let mut avi = Avi::new(
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        &[&ti_batch, &delta],
    );
    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi.cursor()),
        false,
    );

    let mut new_min = None;
    for i in 0..out.count {
        if out.get_weight(i) > 0 {
            new_min = Some(read_i64_le(out.col_data(0), i * 8));
        }
    }
    assert_eq!(
        new_min,
        Some(5),
        "the surviving duplicate at val=5 must keep MIN=5 after retracting one copy"
    );
}

// -----------------------------------------------------------------------
// MIN ignores NULL aggregate values: a group mixing NULL and non-NULL vals
// must aggregate only the non-NULL rows.
// -----------------------------------------------------------------------

#[test]
fn min_ignores_null_values() {
    // val is nullable.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    let mk_row = |b: &mut BatchBuilder, pk: u64, g: i64, val: i64, is_null: bool| {
        b.begin_row(pk as u128, 1);
        b.put_int(g as u128);
        match is_null {
            true => b.put_null(),
            false => b.put_int(val as u128),
        }
        b.end_row();
    };

    // group g=1: NULL, 7, 3 → MIN ignores NULL → 3.
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        mk_row(&mut b, 1, 1, 0, true);
        mk_row(&mut b, 2, 1, 7, false);
        mk_row(&mut b, 3, 1, 3, false);
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = empty_trace(out_schema);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        None,
        false,
    );

    assert_eq!(out.count, 1);
    let min = read_i64_le(out.col_data(1), 0);
    assert_eq!(min, 3, "MIN must ignore the NULL row and pick 3, not 0");
}

// -----------------------------------------------------------------------
// Multi-column GROUP BY MIN retraction through the AVI: retracting the current
// MIN of a two-column group must return the next-best from the index, keyed by
// the full (a, b) prefix.
// -----------------------------------------------------------------------

#[test]
fn avi_multi_col_retraction_returns_next_extremum() {
    // Input: pk(U64), a(U32), b(U32), val(I64); GROUP BY (a, b), MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    // delta: retract group (3, 4)'s current MIN (val=5).
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, -1i64);
        b.put_int(3);
        b.put_int(4);
        b.put_int(5);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = group_identity(&in_schema, &[1u32, 2u32], &delta.as_mem_batch(), 0);

    // AVI post-state for (3, 4): surviving min is 9. A decoy entry for a
    // different group (3, 5) sharing the a-byte prefix must NOT be matched.
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    let avi_batch = {
        let mut b = BatchBuilder::new(avi_schema);
        let put = |b: &mut BatchBuilder, a: u32, bb: u32, min: i64| {
            let mut key = [0u8; 17];
            avi_gcol(&mut key[0..4], &a.to_le_bytes(), TypeCode::U32);
            avi_gcol(&mut key[4..8], &bb.to_le_bytes(), TypeCode::U32);
            let av = i64_av(min);
            key[8] = 0; // ordinal 0 (single MIN aggregate)
            key[9..17].copy_from_slice(&av.to_be_bytes());
            b.begin_row_bytes(&key, 1i64);
            b.end_row();
        };
        put(&mut b, 3, 4, 9);
        put(&mut b, 3, 5, 1); // decoy: same a, different b, smaller value
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let to_batch = {
        let mut b = BatchBuilder::new(out_schema);
        b.begin_row(group_key, 1i64);
        b.put_int(3);
        b.put_int(4);
        b.put_int(5);
        b.put_int(2);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = trace_cursor(to_batch, out_schema);
    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32, 2u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi_ch),
        false,
    );

    let mut inserted = None;
    for i in 0..out.count {
        if out.get_weight(i) > 0 {
            inserted = Some(read_i64_le(out.col_data(2), i * 8));
        }
    }
    assert_eq!(
        inserted,
        Some(9),
        "must return group (3,4)'s own next MIN (9), not the decoy group (3,5)'s value"
    );
}

// =======================================================================
// Wide byte-form AVI (composite key > 16 bytes). Multi-column and U128/UUID
// group keys produce a composite AVI key past the narrow 16-byte budget, up
// to MAX_PK_BYTES. These tests drive `seek_first_positive_with_prefix`
// through the wide cursor path (pk_stride > 16, ordered by compare_pk_bytes),
// the surface the narrow tests above never reach.
// =======================================================================

// Randomized equivalence: over many wide `(a, b)` groups the AVI lookup must
// return each group's own MIN — the same per-group extremum a trace scan
// would compute. The expected MIN is computed directly in-test (the
// reference); the AVI carries that post-state and the indexed result must
// match it group-for-group. Composite key = a(8) ++ b(8) ++ av(8) = 24 bytes.
#[test]
fn avi_wide_two_u64_groups_match_reference() {
    use std::collections::BTreeMap;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false), // pk
            SchemaColumn::new(TypeCode::U64, false), // a (group)
            SchemaColumn::new(TypeCode::U64, false), // b (group)
            SchemaColumn::new(TypeCode::I64, false), // val
        ],
        &[0],
    );
    assert_eq!(
        avi_schema(&in_schema, &[1u32, 2u32]).pk_stride(),
        25,
        "wide composite: 8 + 8 + 1 + 8",
    );

    // Deterministic LCG. Group coordinates span the full u64 range (high bit
    // set) to exercise the wide compare_pk_bytes ordering, not just low bytes.
    let mut state: u64 = 0x9E3779B97F4A7C15;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };

    // 40 distinct groups, several rows each; reference MIN computed in-test.
    let mut reference: BTreeMap<(u64, u64), i64> = BTreeMap::new();
    let mut group_coords: Vec<(u64, u64)> = Vec::new();
    for _ in 0..40 {
        let a = next() | (1u64 << 63);
        let b = next();
        group_coords.push((a, b));
        let rows = 1 + (next() % 5) as usize;
        let mut group_min = i64::MAX;
        for _ in 0..rows {
            let v = next() as i64;
            group_min = group_min.min(v);
        }
        reference.insert((a, b), group_min);
    }

    // AVI post-state: one entry per group holding its MIN, sorted by
    // compare_pk_bytes order (ascending a, then b — both unsigned).
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    let avi_batch = {
        let mut keys: Vec<[u8; 25]> = reference
            .iter()
            .map(|(&(a, b), &m)| {
                let mut key = [0u8; 25];
                avi_gcol(&mut key[0..8], &a.to_le_bytes(), TypeCode::U64);
                avi_gcol(&mut key[8..16], &b.to_le_bytes(), TypeCode::U64);
                let av = i64_av(m);
                key[16] = 0; // ordinal 0 (single MIN aggregate)
                key[17..25].copy_from_slice(&av.to_be_bytes());
                key
            })
            .collect();
        // Production's ingest sorts the AVI by memcmp of the composite key, so
        // the fixture must be in byte order. The group prefix is OPK, so for
        // these unsigned columns that coincides with numeric (a, b) order.
        keys.sort();
        let mut bt = BatchBuilder::new(avi_schema);
        for key in &keys {
            bt.begin_row_bytes(key, 1i64);
            bt.end_row();
        }
        let mut bt = bt.finish();
        bt.set_layout_unchecked(Layout::Consolidated);
        bt
    };

    // Delta: one representative insert per group. Its val is deliberately the
    // group's MIN + 1000 so a correct result can only come from the index.
    let delta = {
        let mut bt = BatchBuilder::new(in_schema);
        for (i, &(a, b)) in group_coords.iter().enumerate() {
            let decoy = reference[&(a, b)].wrapping_add(1000);
            bt.begin_row(i as u128 + 1, 1);
            bt.put_int(a as u128);
            bt.put_int(b as u128);
            bt.put_int(decoy as u128);
            bt.end_row();
        }
        bt.finish()
    };

    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    let seen: BTreeMap<(u64, u64), i64> = group_coords
        .iter()
        .enumerate()
        .map(|(row, &g)| {
            let min = probe_indexed(&in_schema, &[1u32, 2u32], agg, &delta, row, &mut avi_ch).expect("an indexed MIN");
            (g, min.value_bits() as i64)
        })
        .collect();
    assert_eq!(
        seen, reference,
        "wide AVI per-group MIN must match the in-test scan reference"
    );
}

// Single U128 group column: composite = g(16) ++ av(8) = 24 bytes. Confirms a
// 16-byte group column drives the wide seek and that two groups never share a
// bucket.
#[test]
fn avi_wide_single_u128_group_distinct() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),  // pk
            SchemaColumn::new(TypeCode::U128, false), // g (group)
            SchemaColumn::new(TypeCode::I64, false),  // val
        ],
        &[0],
    );
    let avi_schema = avi_schema(&in_schema, &[1u32]);
    assert_eq!(avi_schema.pk_stride(), 25, "16 + 1 + 8");

    // Two groups, large U128 values (high 64 bits set) → exercises the full
    // 16-byte compare. Group g1 MIN=10, g2 MIN=20.
    let g1: u128 = (1u128 << 120) | 7;
    let g2: u128 = (1u128 << 120) | 9; // shares top bytes with g1, differs low
    let groups = [(g1, 10i64), (g2, 20i64)];

    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        for (i, &(g, _)) in groups.iter().enumerate() {
            b.begin_row(i as u128 + 1, 1i64);
            b.put_int(g);
            b.put_int(999);
            b.end_row();
        }
        b.finish()
    };

    let avi_batch = {
        let mut b = BatchBuilder::new(avi_schema);
        // sorted ascending by g (both share high bytes; g1 < g2 by low byte).
        for &(g, m) in &groups {
            let mut key = [0u8; 25];
            avi_gcol(&mut key[0..16], &g.to_le_bytes(), TypeCode::U128);
            let av = i64_av(m);
            key[16] = 0; // ordinal 0 (single MIN aggregate)
            key[17..25].copy_from_slice(&av.to_be_bytes());
            b.begin_row_bytes(&key, 1i64);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    for (row, &(g, expected)) in groups.iter().enumerate() {
        let min = probe_indexed(&in_schema, &[1u32], agg, &delta, row, &mut avi_ch).expect("an indexed MIN");
        assert_eq!(
            min.value_bits() as i64,
            expected,
            "U128 group {g} must resolve its own MIN"
        );
    }
}

// Mixed signed/unsigned wide key: GROUP BY (a I64, b U64), composite 24. The
// negative `a` group must sort before the positive one under the per-column
// signed comparison in compare_pk_bytes; a seek for each must land on its own
// group. Guards that the AVI schema preserves each group column's type_code.
#[test]
fn avi_wide_mixed_signed_unsigned_key() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false), // pk
            SchemaColumn::new(TypeCode::I64, false), // a (signed group)
            SchemaColumn::new(TypeCode::U64, false), // b (group)
            SchemaColumn::new(TypeCode::I64, false), // val
        ],
        &[0],
    );
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 25);

    // Groups: (-5, 10) MIN=100, (-5, 11) MIN=50, (3, 10) MIN=200.
    // Signed order on column a: -5 < 3, so the (-5,*) groups precede (3,*).
    let groups: [(i64, u64, i64); 3] = [(-5, 10, 100), (-5, 11, 50), (3, 10, 200)];

    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        for (i, &(a, bb, _)) in groups.iter().enumerate() {
            b.begin_row(i as u128 + 1, 1i64);
            b.put_int(a as u128);
            b.put_int(bb as u128);
            b.put_int(777);
            b.end_row();
        }
        b.finish()
    };

    // AVI rows in memcmp (compare_pk_bytes) order of the composite key — what
    // production's ingest produces. The group prefix is OPK, so the signed
    // column's byte order is its signed order.
    let avi_batch = {
        let mut b = BatchBuilder::new(avi_schema);
        let mut keys: Vec<[u8; 25]> = groups
            .iter()
            .map(|&(a, bb, m)| {
                let mut key = [0u8; 25];
                avi_gcol(&mut key[0..8], &a.to_le_bytes(), TypeCode::I64);
                avi_gcol(&mut key[8..16], &bb.to_le_bytes(), TypeCode::U64);
                let av = i64_av(m);
                key[16] = 0; // ordinal 0 (single MIN aggregate)
                key[17..25].copy_from_slice(&av.to_be_bytes());
                key
            })
            .collect();
        keys.sort();
        for key in &keys {
            b.begin_row_bytes(key, 1i64);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    for (row, &(a, bb, expected)) in groups.iter().enumerate() {
        let min = probe_indexed(&in_schema, &[1u32, 2u32], agg, &delta, row, &mut avi_ch).expect("an indexed MIN");
        assert_eq!(
            min.value_bits() as i64,
            expected,
            "signed-key group ({a},{bb}) must resolve its own MIN"
        );
    }
}

// Wide prefix collision: two groups whose composite keys share their first 16
// bytes (a, b) but differ in bytes 17–24 (c). The seek must keep them distinct
// and never return the colliding group's value — even though the decoy holds a
// smaller value that would win a 16-byte-prefix-only match. GROUP BY
// (a, b, c) → composite a(8)++b(8)++c(8)++av(8) = 32 bytes.
#[test]
fn avi_wide_prefix_collision_distinct_groups() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false), // pk
            SchemaColumn::new(TypeCode::U64, false), // a
            SchemaColumn::new(TypeCode::U64, false), // b
            SchemaColumn::new(TypeCode::U64, false), // c
            SchemaColumn::new(TypeCode::I64, false), // val
        ],
        &[0],
    );
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32, 3u32]);
    assert_eq!(avi_schema.pk_stride(), 33, "8 + 8 + 8 + 1 + 8");

    // Both groups share (a, b) = (1, 2); they differ only in c.
    // Group (1,2,3) MIN=100; decoy (1,2,4) MIN=5 (smaller — must NOT leak).
    let groups: [(u64, u64, u64, i64); 2] = [(1, 2, 3, 100), (1, 2, 4, 5)];

    // delta inserts only group (1,2,3); its val is a decoy 999.
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, 1i64);
        b.put_int(1);
        b.put_int(2);
        b.put_int(3);
        b.put_int(999);
        b.end_row();
        b.finish()
    };

    let avi_batch = {
        let mut b = BatchBuilder::new(avi_schema);
        // sorted by (a,b,c): (1,2,3) before (1,2,4).
        for &(a, bb, c, m) in &groups {
            let mut key = [0u8; 33];
            avi_gcol(&mut key[0..8], &a.to_le_bytes(), TypeCode::U64);
            avi_gcol(&mut key[8..16], &bb.to_le_bytes(), TypeCode::U64);
            avi_gcol(&mut key[16..24], &c.to_le_bytes(), TypeCode::U64);
            let av = i64_av(m);
            key[24] = 0; // ordinal 0 (single MIN aggregate)
            key[25..33].copy_from_slice(&av.to_be_bytes());
            b.begin_row_bytes(&key, 1i64);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 4, agg_op: AggFunc::Min };

    let min = probe_indexed(&in_schema, &[1u32, 2u32, 3u32], agg, &delta, 0, &mut avi_ch).expect("an indexed MIN");
    assert_eq!(
        min.value_bits() as i64,
        100,
        "the 16-byte-prefix-sharing decoy (1,2,4)'s smaller value must not leak"
    );
}

// Wide retraction: retracting a wide-key group's current MIN must return the
// next-best from the index — the wide analog of
// `avi_multi_col_retraction_returns_next_extremum`. A prefix-colliding decoy
// group (sharing the first 16 bytes) must not be matched.
#[test]
fn avi_wide_retraction_returns_next_extremum() {
    // GROUP BY (a U64, b U64); composite a(8)++b(8)++av(8) = 24 bytes.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 25);

    // Retract group (1<<40, 2)'s current MIN (val=5).
    let ga: u64 = 1 << 40;
    let gb: u64 = 2;
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        b.begin_row(1u128, -1i64);
        b.put_int(ga as u128);
        b.put_int(gb as u128);
        b.put_int(5);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = group_identity(&in_schema, &[1u32, 2u32], &delta.as_mem_batch(), 0);

    // AVI post-state: group (ga, gb) surviving values {9, 15}; a decoy group
    // (ga, gb+1) sharing the first 8 bytes holds a smaller 1 that must not win.
    let avi_batch = {
        let mut b = BatchBuilder::new(avi_schema);
        let put = |b: &mut BatchBuilder, a: u64, bb: u64, m: i64| {
            let mut key = [0u8; 25];
            avi_gcol(&mut key[0..8], &a.to_le_bytes(), TypeCode::U64);
            avi_gcol(&mut key[8..16], &bb.to_le_bytes(), TypeCode::U64);
            let av = i64_av(m);
            key[16] = 0; // ordinal 0 (single MIN aggregate)
            key[17..25].copy_from_slice(&av.to_be_bytes());
            b.begin_row_bytes(&key, 1i64);
            b.end_row();
        };
        // sorted by (a, b, av): (ga,gb,9),(ga,gb,15),(ga,gb+1,1).
        put(&mut b, ga, gb, 9);
        put(&mut b, ga, gb, 15);
        put(&mut b, ga, gb + 1, 1);
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let to_batch = {
        let mut b = BatchBuilder::new(out_schema);
        b.begin_row(group_key, 1i64);
        b.put_int(ga as u128);
        b.put_int(gb as u128);
        b.put_int(5);
        b.put_int(3);
        b.end_row();
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = trace_cursor(to_batch, out_schema);
    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32, 2u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi_ch),
        false,
    );

    let mut retracted = None;
    let mut inserted = None;
    for i in 0..out.count {
        let w = out.get_weight(i);
        let v = read_i64_le(out.col_data(2), i * 8);
        if w < 0 {
            retracted = Some(v);
        } else {
            inserted = Some(v);
        }
    }
    assert_eq!(retracted, Some(5), "must retract the stale wide-key MIN");
    assert_eq!(
        inserted,
        Some(9),
        "wide AVI must return group (ga,gb)'s next MIN (9), not the decoy's 1"
    );
}

// =======================================================================
// A value-independent aggregate (COUNT) must not read a wide PK source column;
// the AVI order-encoded value must be byte-ordered so incremental MIN/MAX reads
// the true extremal; an F32 AVI seed must render the source's F32 bits; and the
// group-by-PK path must handle a compound PK of any width.
// =======================================================================

// COUNT(*) is compiled with a placeholder arg column index 0, which may be a
// 16-byte UUID PK. COUNT is value-independent and must count without reading
// the column.
#[test]
fn count_accumulator_over_uuid_pk_does_not_panic() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::UUID, false), // 16-byte PK
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let mut b = BatchBuilder::new(schema);
    for pk in [1u128, 2] {
        b.begin_row(pk, 1i64);
        b.put_int(0);
        b.end_row();
    }
    let b = b.finish();
    let desc = AggDescriptor::COUNT_STAR;
    let mut acc = make_acc(&schema, &[0], desc);
    let mb = b.as_mem_batch();
    acc.step_from_batch(&mb, 0, 1);
    acc.step_from_batch(&mb, 1, 1);
    assert_eq!(
        acc.value_bits() as i64,
        2,
        "COUNT over a UUID PK column must count rows"
    );
}

/// The extreme of `deltas[0]`-row-0's group, read back out of an AVI populated
/// from `deltas` through the production path. `col_idx` is the aggregate source
/// column (PK or payload).
fn avi_read_extreme(
    in_schema: &SchemaDescriptor,
    group_by: &[u32],
    col_idx: u32,
    deltas: &[&Batch],
    for_max: bool,
) -> i64 {
    let agg = AggDescriptor {
        col_idx,
        agg_op: if for_max { AggFunc::Max } else { AggFunc::Min },
    };
    let mut avi = Avi::new(in_schema, group_by, &[agg, AggDescriptor::COUNT_STAR], deltas);
    probe_indexed(in_schema, group_by, agg, deltas[0], 0, &mut avi.cursor())
        .expect("AVI seek must find the probed group")
        .value_bits() as i64
}

/// The extreme `op_reduce`'s AVI probe reads for `row`'s group, or `None` when
/// the index holds no positive entry for it.
fn probe_indexed(
    in_schema: &SchemaDescriptor,
    group_by: &[u32],
    agg: AggDescriptor,
    delta: &Batch,
    row: usize,
    avi: &mut ReadCursor,
) -> Option<Accumulator> {
    let bake = make_bake(in_schema, group_by, &[agg, AggDescriptor::COUNT_STAR]);
    let mut acc = make_acc(in_schema, group_by, agg);
    bake.seed_extremes(avi, &delta.as_mem_batch(), row, std::slice::from_mut(&mut acc));
    acc.value().is_some().then_some(acc)
}

// The order-encoded aggregate value must be serialized big-endian so the
// index's lexicographic byte ordering matches numeric order. With three values
// in one group whose extremes differ above the low byte ({101, 111, -5}), a
// little-endian serialization sorts 101 first and reports it as the MIN; -5 is
// never seen.
#[test]
fn avi_full_path_min_max_across_high_byte() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false), // pk
            SchemaColumn::new(TypeCode::U64, false), // g (group)
            SchemaColumn::new(TypeCode::I64, false), // val (agg)
        ],
        &[0],
    );
    // One group g=5 with values {101, 111, -5}.
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        for (pk, v) in [(1u128, 101i64), (2, 111), (3, -5)] {
            b.begin_row(pk, 1i64);
            b.put_int(5);
            b.put_int(v as u128);
            b.end_row();
        }
        b.finish()
    };

    assert_eq!(
        avi_read_extreme(&in_schema, &[1], 2, &[&delta], false),
        -5,
        "MIN across the high-byte boundary must be -5, not 101"
    );
    assert_eq!(
        avi_read_extreme(&in_schema, &[1], 2, &[&delta], true),
        111,
        "MAX must be 111"
    );
}

/// Delta over a two-column PK `[U64 a (pk), <int> b (pk)]`, no payload: one row
/// per `(b, weight)` with a=5. `opk_pk` truncates each u128 to the column
/// width, so a negative `b as u128` still packs the correct two's-complement
/// LE bytes.
fn pk_ab_delta(schema: &SchemaDescriptor, rows: &[(i128, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(bv, w) in rows {
        b.begin_row_opk(&[5u128, bv as u128], w);
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

// A signed PK-source aggregate end-to-end: PK = (a:U64, b:I64), GROUP BY a,
// MIN/MAX(b). The population OPK-decodes b before order-encoding; without the
// decode the byte-swapped image reports the wrong extreme (101 instead of -5 for
// MIN) and the retract-at-extreme walk advances to the wrong next value.
#[test]
fn avi_full_path_pk_source_signed_min_max() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1],
    );
    // Group a=5, b ∈ {-5, 1, 256, 100}: extremes straddle the sign (-5) and the
    // high byte (256).
    let delta = pk_ab_delta(&in_schema, &[(-5, 1), (1, 1), (256, 1), (100, 1)]);
    let read = |deltas: &[&Batch], for_max: bool| avi_read_extreme(&in_schema, &[0], 1, deltas, for_max);

    // Insert epoch.
    assert_eq!(
        read(&[&delta], false),
        -5,
        "MIN across sign/high-byte boundary must be -5"
    );
    assert_eq!(read(&[&delta], true), 256, "MAX across the high byte must be 256");

    // Retract-at-extreme: retracting the current MIN advances it to the next
    // value; retracting the current MAX falls it back.
    let retract_min = pk_ab_delta(&in_schema, &[(-5, -1)]);
    assert_eq!(
        read(&[&delta, &retract_min], false),
        1,
        "after retracting -5, MIN advances to 1"
    );
    let retract_max = pk_ab_delta(&in_schema, &[(256, -1)]);
    assert_eq!(
        read(&[&delta, &retract_max], true),
        100,
        "after retracting 256, MAX falls to 100"
    );
}

// The unsigned high-byte-boundary case at the reduce full-path level: PK =
// (a:U64, b:U64), b ∈ {1, 256, 100}. 1 (0x0001) and 256 (0x0100) byte-swap into
// each other, so a population that skips the OPK decode reports 256 as the MIN.
#[test]
fn avi_full_path_pk_source_unsigned_high_byte() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0, 1],
    );
    let delta = pk_ab_delta(&in_schema, &[(1, 1), (256, 1), (100, 1)]);
    let read = |deltas: &[&Batch], for_max: bool| avi_read_extreme(&in_schema, &[0], 1, deltas, for_max);

    assert_eq!(read(&[&delta], false), 1, "MIN must be 1, not the byte-swapped 256");
    assert_eq!(read(&[&delta], true), 256, "MAX must be 256");

    // Retract the MAX (256); MAX falls back to 100.
    let retract_max = pk_ab_delta(&in_schema, &[(256, -1)]);
    assert_eq!(
        read(&[&delta, &retract_max], true),
        100,
        "after retracting 256, MAX falls to 100"
    );
    // Retract the MIN (1); MIN advances to 100.
    let retract_min = pk_ab_delta(&in_schema, &[(1, -1)]);
    assert_eq!(
        read(&[&delta, &retract_min], false),
        100,
        "after retracting 1, MIN advances to 100"
    );
}

// An F32 MIN/MAX's output column is F32, so the AVI seed — an order image —
// must invert back to the source's own 32-bit IEEE bits.
#[test]
fn avi_f32_seed_renders_f32_bits() {
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::F32, false));
    let desc = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };
    for v in [1.5f32, -2.25, 0.0, -0.0, 1.0e30] {
        let mut acc = make_acc(&in_schema, &[0], desc);
        acc.seed_from_index(&crate::ops::order_image::ieee_order_bits_f32(v.to_bits()).to_be_bytes());
        assert_eq!(
            acc.value_bits(),
            v.to_bits() as u64,
            "F32 AVI seed must render as the F32 bits of v={v}",
        );
    }
}

// A compound PK with pk_stride > 16 (two U128 columns = stride 32) whose GROUP
// BY is exactly the PK groups on the full PK byte window, emitting one
// weight-folded row per distinct PK.
#[test]
fn reduce_wide_compound_pk_group_by_pk_counts_per_pk() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false), // pk col 0
            SchemaColumn::new(TypeCode::U128, false), // pk col 1
        ],
        &[0, 1],
    );
    assert!(in_schema.pk_stride() > 16, "compound U128+U128 PK must exceed 16 bytes");

    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true), // COUNT
        ],
        &[0, 1],
    );

    // Two distinct compound PKs, folded and (PK,payload)-sorted: (1,1) at weight
    // 2 → cnt 2, (1,2) at weight 1 → cnt 1. Flagged consolidated (genuinely
    // folded, ghost-free, sorted), so the group runs end on the wide PK bytes in
    // place — the wide compound-PK path under test.
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        for (a, c, w) in [(1u128, 1u128, 2i64), (1, 2, 1)] {
            b.begin_row_opk(&[a, c], w);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = empty_trace(out_schema);

    let agg = AggDescriptor::COUNT_STAR;

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32, 1u32], &[agg], None, false);

    assert!(out.is_consolidated(), "reduce output is certified consolidated");
    // (1,1) (cnt 2) precedes (1,2) (cnt 1): read counts in physical row order to
    // pin the canonical ordering.
    let counts: Vec<i64> = (0..out.count)
        .filter(|&i| out.get_weight(i) > 0)
        .map(|i| read_i64_le(out.col_data(0), i * 8))
        .collect();
    assert_eq!(
        counts,
        vec![2, 1],
        "two distinct compound PKs in canonical order: (1,1)→2 then (1,2)→1"
    );
}

// -----------------------------------------------------------------------
// Non-linear REDUCE fallback: single trace scan (O(trace + delta)).
//
// The fallback path is reached when all hold: non-linear aggregate,
// no combined value index (a non-AVI-eligible MIN/MAX, e.g. over a
// German string), group_by is not the PK. The tests here pin
// three properties: (1) hash correctness (the cursor-side group key must
// produce byte-identical results to the batch-side one for the same row),
// (2) aggregate correctness across INSERT/DELETE ticks, and (3) the
// trace is scanned at most once per tick (REWIND_CALLS ≤ 1).
// -----------------------------------------------------------------------

/// Read (grp_i64, min_i64) pairs from a fallback-style output (U128 pk | I64 grp | I64 min).
fn read_grp_min_pairs(out: &Batch) -> Vec<(i64, i64)> {
    let grp_data = out.col_data(0);
    let min_data = out.col_data(1);
    let mut pairs: Vec<(i64, i64)> = (0..out.count)
        .filter(|&i| out.get_weight(i) > 0)
        .map(|i| {
            let g = read_i64_le(grp_data, i * 8);
            let m = read_i64_le(min_data, i * 8);
            (g, m)
        })
        .collect();
    pairs.sort_unstable();
    pairs
}

/// Run the grouped non-linear REDUCE correctness test over a **nullable**
/// group column — the shape whose packed group key carries a presence bitmap,
/// so a NULL group and a `0` group must not collide on one output PK.
///
/// Schema: U64 pk | I64 grp (nullable) | I64 val. MIN on val, grouped by grp.
///
/// Two ticks:
///   Tick 1: insert `tick1_rows` → verify MIN per group.
///   Tick 2: apply `delta_rows` against the tick-1 history → verify updated MIN.
fn run_nullable_grp_min_i64(
    tick1_rows: &[GrpValRow],
    delta_rows: &[GrpValRow],
    expected_tick1: &mut Vec<(i64, i64)>, // sorted (grp, min) pairs after tick 1
    expected_tick2: &mut Vec<(i64, i64)>, // sorted (grp, min) pairs after tick 2
) {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true), // nullable grp
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true), // nullable grp (carried through)
            SchemaColumn::new(TypeCode::I64, true), // nullable min
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    // --- Tick 1: empty history, empty trace_out ---
    let delta1 = build_grp_val_delta(&in_schema, tick1_rows);
    let mut to_ch1 = empty_trace(out_schema);
    let mut avi1 = Avi::new(&in_schema, &[1u32], &[agg, AggDescriptor::COUNT_STAR], &[&delta1]);

    let out1 = op_reduce(
        &delta1,
        &mut to_ch1,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi1.cursor()),
        false,
    );

    let got1 = read_grp_min_pairs(&out1);
    expected_tick1.sort_unstable();
    assert_eq!(got1, *expected_tick1, "tick 1 MIN mismatch");

    // --- Tick 2: history = tick1 input + this delta, trace_out = tick1 output ---
    let to_batch = out1;
    let delta2 = build_grp_val_delta(&in_schema, delta_rows);

    let mut to_ch2 = trace_cursor(to_batch, out_schema);
    let mut avi2 = Avi::new(
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        &[&delta1, &delta2],
    );

    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi2.cursor()),
        false,
    );

    let grp_data = out2.col_data(0);
    let min_data = out2.col_data(1);

    // After tick 2, accumulate tick1 + delta. The net MIN per group is what
    // tick1 produced (already verified) plus the new delta's contribution.
    // We build expected from tick1 final values + delta applied.
    let mut final_rows: std::collections::BTreeMap<i64, Vec<i64>> = std::collections::BTreeMap::new();
    for &(_, grp, val, w) in tick1_rows.iter().chain(delta_rows.iter()) {
        let Some(grp) = grp else {
            continue; // NULL group: skip for simplicity (tested separately)
        };
        if w > 0 {
            final_rows.entry(grp).or_default().push(val);
        } else {
            let v = final_rows.entry(grp).or_default();
            v.retain(|&x| x != val); // remove one occurrence
        }
    }
    let mut expected2: Vec<(i64, i64)> = final_rows
        .iter()
        .filter(|(_, vals)| !vals.is_empty())
        .map(|(&g, vals)| (g, *vals.iter().min().unwrap()))
        .collect();
    expected2.sort_unstable();
    expected_tick2.sort_unstable();
    assert_eq!(expected2, *expected_tick2, "tick 2 expected mismatch in test setup");

    // The live MIN values in out2: start from tick1 result and apply net changes,
    // retractions first — a group's new row may precede its retraction.
    let mut live: std::collections::BTreeMap<i64, i64> = got1.iter().cloned().collect();
    for retract in [true, false] {
        for i in (0..out2.count).filter(|&i| (out2.get_weight(i) < 0) == retract) {
            let g = read_i64_le(grp_data, i * 8);
            if retract {
                live.remove(&g);
            } else {
                live.insert(g, read_i64_le(min_data, i * 8));
            }
        }
    }
    let mut live_pairs: Vec<(i64, i64)> = live.into_iter().collect();
    live_pairs.sort_unstable();
    assert_eq!(live_pairs, *expected_tick2, "tick 2 MIN mismatch (fallback path)");
}

/// MIN grouped by nullable I64 — linear probe branch (< HASH_THRESHOLD groups).
#[test]
fn fallback_min_nullable_i64_group_linear_probe() {
    // 3 distinct non-null groups.
    let tick1 = vec![
        (1u64, Some(10i64), 100i64, 1i64),
        (2, Some(10), 50, 1),
        (3, Some(20), 30, 1),
        (4, Some(30), 200, 1),
    ];
    // Delete the row with val=100 from group 10 → new MIN(10) = 50.
    let delta2 = vec![(1u64, Some(10i64), 100i64, -1i64)];
    let mut exp1 = vec![(10i64, 50i64), (20, 30), (30, 200)];
    let mut exp2 = vec![(10i64, 50i64), (20, 30), (30, 200)];
    run_nullable_grp_min_i64(&tick1, &delta2, &mut exp1, &mut exp2);
}

/// MIN grouped by nullable I64 — hash branch (>= HASH_THRESHOLD groups).
#[test]
fn fallback_min_nullable_i64_group_hash_path() {
    // 20 distinct groups (exceeds HASH_THRESHOLD=16).
    let tick1: Vec<GrpValRow> = (0..20)
        .flat_map(|g| {
            vec![
                ((g * 2) as u64, Some(g as i64), (g * 10 + 5) as i64, 1),
                ((g * 2 + 1) as u64, Some(g as i64), (g * 10 + 1) as i64, 1),
            ]
        })
        .collect();
    // MIN per group g = g*10+1. Delete the row with val=51 from group 5 → MIN(5) becomes 55.
    // pk=11 = g*2+1 for g=5 carries val=g*10+1=51.
    let delta2 = vec![(11u64, Some(5i64), 51i64, -1i64)]; // remove pk=11 (val=51)
    let mut exp1: Vec<(i64, i64)> = (0..20).map(|g| (g as i64, (g * 10 + 1) as i64)).collect();
    let mut exp2: Vec<(i64, i64)> = (0..20)
        .map(|g| (g as i64, if g == 5 { 55 } else { (g * 10 + 1) as i64 }))
        .collect();
    run_nullable_grp_min_i64(&tick1, &delta2, &mut exp1, &mut exp2);
}

/// MIN grouped by multi-column key (I64, I64) — no GI → fallback path.
#[test]
fn min_multi_col_group_resolves_per_group() {
    // Schema: U64 pk | I64 c1 | I64 c2 | I64 val. Group by [c1, c2].
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    // Output: U128 pk | I64 c1 | I64 c2 | I64 min (nullable).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    let make_batch = |rows: &[(u64, i64, i64, i64, i64)]| -> Batch {
        let mut b = BatchBuilder::new(in_schema);
        for &(pk, w, c1, c2, val) in rows {
            b.begin_row(pk as u128, w);
            b.put_int(c1 as u128);
            b.put_int(c2 as u128);
            b.put_int(val as u128);
            b.end_row();
        }
        let mut b = b.finish();
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // Tick 1: groups (1,1)→min=10, (1,2)→min=30, (2,1)→min=50.
    let tick1_rows = [
        (1u64, 1, 1i64, 1i64, 10i64),
        (2, 1, 1, 1, 20),
        (3, 1, 1, 2, 30),
        (4, 1, 2, 1, 50),
    ];
    let delta1 = make_batch(&tick1_rows);
    let mut to_ch = empty_trace(out_schema);

    let mut avi1 = Avi::new(&in_schema, &[1u32, 2u32], &[agg, AggDescriptor::COUNT_STAR], &[&delta1]);
    let out1 = op_reduce(
        &delta1,
        &mut to_ch,
        &in_schema,
        &[1u32, 2u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi1.cursor()),
        false,
    );
    // Verify 3 distinct groups came out.
    assert_eq!(out1.count, 3, "tick 1 must emit 3 groups");

    // Tick 2: add val=5 to group (1,1). trace_out is empty (no prior output to
    // retract); the index must still resolve group (1,1)'s extreme over
    // {10, 20, 5} → MIN = 5, keyed by the two-column packed prefix.
    let delta2_rows = [(5u64, 1, 1i64, 1i64, 5i64)];
    let history = make_batch(&tick1_rows);
    let empty_out2 = Batch::empty_with_schema(&out_schema);
    let delta2 = make_batch(&delta2_rows);

    let mut to_ch2 = trace_cursor(empty_out2, out_schema);
    let mut avi2 = Avi::new(
        &in_schema,
        &[1u32, 2u32],
        &[agg, AggDescriptor::COUNT_STAR],
        &[&history, &delta2],
    );

    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32, 2u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi2.cursor()),
        false,
    );

    // No retraction (trace_out was empty), just one insert for group (1,1).
    assert_eq!(out2.count, 1, "tick 2 must emit one row for group (1,1)");
    let out2_min = out2.col_data(2);
    let new_min = read_i64_le(out2_min, 0);
    assert_eq!(
        new_min, 5,
        "MIN for group (1,1) must be 5 — indexed history (10,20) + delta (5) → min=5"
    );
}

// -----------------------------------------------------------------------
// Fix B: true 128-bit group identity (collision resistance + full width).
//
// The fold path (multi-column GROUP BY / single STRING key) now streams the
// canonical per-column material into an Xxh3 digest128, so it carries a full
// 128 bits of entropy. The previous mix64 fold compressed to a 64-bit `h` and
// lifted to u128 bijectively, so it had only 2^64 cardinality — a ~2^32
// birthday bound past which a collision merges two distinct group keys into one
// group (silently wrong aggregation). The single-column int/PK fast paths are
// exact and stay byte-identical (pinned by the *_bit_identical /
// *_canonical_widen tests, unchanged above).
// -----------------------------------------------------------------------

/// 3-col schema: U64 pk | I64 c1 | STRING c2 (both non-PK group columns).
fn make_schema_u64_i64_str() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    )
}

#[test]
fn test_group_key_128bit_collision_resistance() {
    use std::collections::HashSet;
    let schema = make_schema_u64_i64_str();

    // Sweep many distinct (c1, c2) multi-column keys. Each (i, j) is a distinct
    // group; the 128-bit fold must map them to distinct u128 with no collision.
    let mut b = BatchBuilder::new(schema);
    let mut expected = 0usize;
    for i in 0..64i64 {
        for j in 0..64u32 {
            b.begin_row((expected as u128) + 1, 1);
            b.put_int(i as u128);
            b.put_string(&format!("k{j}"));
            b.end_row();
            expected += 1;
        }
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    let mut keys: HashSet<u128> = HashSet::new();
    let mut his: HashSet<u64> = HashSet::new();
    let mut los: HashSet<u64> = HashSet::new();
    for row in 0..b.count {
        let k = group_identity(&schema, &[1u32, 2u32], &mb, row);
        keys.insert(k);
        his.insert((k >> 64) as u64);
        los.insert(k as u64);
    }
    assert_eq!(
        keys.len(),
        expected,
        "all distinct multi-column keys must hash to distinct 128-bit values (no collision)",
    );
    // The full 128-bit width is used: both halves carry real hash output. A
    // 64-bit-then-zero-extended key would pin the high half constant; the old
    // bijective lift would make the high half a deterministic image of the low
    // half. For 4096 keys well under the 2^32 birthday bound, an independent
    // 128-bit digest leaves each half collision-free.
    assert_eq!(
        his.len(),
        expected,
        "high 64 bits must be full-entropy (no truncation collision)"
    );
    assert_eq!(
        los.len(),
        expected,
        "low 64 bits must be full-entropy (no truncation collision)"
    );

    // Determinism: the same row hashes identically across calls (reused hasher).
    assert_eq!(
        group_identity(&schema, &[1u32, 2u32], &mb, 0),
        group_identity(&schema, &[1u32, 2u32], &mb, 0),
        "same row must hash identically",
    );
}

// -----------------------------------------------------------------------
// Fix C: BLOB as a grouping key.
//
// BLOB shares the 16-byte German-string layout with STRING. Long (>12-byte)
// blobs that share a 4-byte prefix force the full-content heap tail.
// -----------------------------------------------------------------------

/// Schema: U64 pk | BLOB grp | I64 val. BLOB is the (non-PK) grouping key.
fn make_schema_u64_blob_grp_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::Blob, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

/// Build a (pk, weight, blob, val) batch. Blobs > 12 bytes go to the heap (long
/// form); shorter blobs stay inline — both exercise german_string_content. Rows
/// are passed in PK order so the batch is validly sorted+consolidated for use as
/// a trace cursor.
fn make_batch_blob_grp_i64(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8], i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, w, blob, val) in rows {
        b.begin_row(pk as u128, w);
        b.put_blob(blob);
        b.put_int(val as u128);
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_group_key_blob_long_shared_prefix() {
    // Two long blobs sharing the 4-byte prefix "PREF" and the same length, so the
    // key must read the heap tail, not just the inline prefix.
    let schema = make_schema_u64_blob_grp_i64();
    let blob_a: &[u8] = b"PREF_aaaaaaaaaa"; // 15 bytes
    let blob_b: &[u8] = b"PREF_bbbbbbbbbb"; // 15 bytes, same prefix+length
    let batch = make_batch_blob_grp_i64(&schema, &[(1, 1, blob_a, 10), (2, 1, blob_b, 20), (3, 1, blob_a, 30)]);
    let mb = batch.as_mem_batch();
    let (keys, _) = GroupOutKey::new(&schema, &[1u32], []).unwrap();
    assert_ne!(
        keys.identity(&mb, 0),
        keys.identity(&mb, 1),
        "distinct blobs key distinct groups"
    );
    assert_eq!(
        keys.identity(&mb, 0),
        keys.identity(&mb, 2),
        "equal blobs key one group"
    );
}

#[test]
fn test_reduce_max_blob_group_retraction() {
    // GROUP BY a long BLOB column, MAX(I64 val). MAX is non-linear and there is
    // no AVI/GI, so the trace replay routes through cursor_matches_group — the
    // compare that `unreachable!`'d on BLOB before Fix C. Two distinct long
    // blobs sharing a 4-byte prefix must group separately, never panic.
    let in_schema = make_schema_u64_blob_grp_i64();
    // Output: synthetic U128 _group_pk | BLOB grp | I64 max (nullable).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::Blob, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Max };

    let blob_a: &[u8] = b"PREFIX_AAAAAAAAAA"; // 17 bytes (long)
    let blob_b: &[u8] = b"PREFIX_BBBBBBBBBB"; // 17 bytes, same prefix+length
    assert_eq!(&blob_a[..6], &blob_b[..6], "shared prefix forces heap tail compare");

    // Tick 1: blob_a → {10, 30}, blob_b → {20}. Empty trace.
    let tick1: &[(u64, i64, &[u8], i64)] = &[(1, 1, blob_a, 10), (2, 1, blob_a, 30), (3, 1, blob_b, 20)];
    let delta1 = make_batch_blob_grp_i64(&in_schema, tick1);
    let mut to_ch1 = empty_trace(out_schema);
    let out1 = op_reduce(
        &delta1,
        &mut to_ch1,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        None,
        false,
    );
    assert_eq!(
        out1.count, 2,
        "two distinct blobs → two groups (must not merge on shared prefix)"
    );
    let maxes1: std::collections::BTreeSet<i64> =
        (0..out1.count).map(|i| read_i64_le(out1.col_data(1), i * 8)).collect();
    assert_eq!(
        maxes1,
        [20i64, 30].into_iter().collect(),
        "MAX(blob_a)=30, MAX(blob_b)=20"
    );

    // Tick 2: retract the val=30 row from blob_a → MAX(blob_a) 30 → 10. The
    // index keys the group by the BLOB's content hash, so the retraction must
    // land on blob_a's own entry.
    let history = make_batch_blob_grp_i64(&in_schema, tick1);
    let to_batch = out1;
    // Retraction: weight -1 on the val=30 blob_a row.
    let delta2 = make_batch_blob_grp_i64(&in_schema, &[(2, -1, blob_a, 30)]);
    let mut to_ch2 = trace_cursor(to_batch, out_schema);
    let mut avi2 = Avi::new(
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        &[&history, &delta2],
    );
    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &[agg, AggDescriptor::COUNT_STAR],
        Some(&mut avi2.cursor()),
        false,
    );
    // blob_a's MAX updates 30 → 10: retract old (30, w=-1) + insert new (10, w=+1).
    // blob_b is untouched.
    let retract = (0..out2.count)
        .find(|&i| out2.get_weight(i) < 0)
        .map(|i| read_i64_le(out2.col_data(1), i * 8));
    let insert = (0..out2.count)
        .find(|&i| out2.get_weight(i) > 0)
        .map(|i| read_i64_le(out2.col_data(1), i * 8));
    assert_eq!(retract, Some(30), "retract old MAX(blob_a)=30");
    assert_eq!(insert, Some(10), "insert new MAX(blob_a)=10 after the 30 row is gone");
}

/// MIN/MAX over a German-string column. Every value shares the index key's
/// 8-byte value slot and spills past the inline cell, so only content order can
/// pick the extreme; one carries a NUL, which the image escapes. Retracting both
/// extremes recedes them through the value index, MAX's complement included.
#[test]
fn german_string_min_max_recede_through_the_value_index() {
    let in_schema = make_schema_u64_blob_grp_i64();
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Count },
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Max },
    ];
    let out_schema = out_schema_for(&in_schema, &[2u32], &aggs);
    // Payload: count, min, max — the natural I64 group key is the PK.
    let (pi_min, pi_max) = (1usize, 2usize);
    let content = |b: &Batch, row: usize, pi: usize| -> Vec<u8> {
        let mb = b.as_mem_batch();
        gnitz_expr::payload_bytes(&mb, row, pi).to_vec()
    };

    // All four share the 8 bytes the key's value slot holds, so the ordering the
    // seek lands on is the BLOB payload's, not the key's.
    let (lo, mid, hi, top): (&[u8], &[u8], &[u8], &[u8]) = (
        b"prefix-shared/aa",
        b"prefix-shared/aaa",
        b"prefix-shared/aab",
        b"prefix-shared/aab\0",
    );
    let t1 = make_batch_blob_grp_i64(
        &in_schema,
        &[(1, 1, mid, 10), (2, 1, hi, 10), (3, 1, lo, 10), (4, 1, top, 10)],
    );
    let mut trace = empty_trace(out_schema);
    let out1 = op_reduce(&t1, &mut trace, &in_schema, &[2u32], &aggs, None, false);
    assert_eq!(out1.count, 1);
    assert_eq!(content(&out1, 0, pi_min), lo);
    assert_eq!(content(&out1, 0, pi_max), top);

    // Retract both extremes; the index holds t1 and t2 and the seek lands on the
    // next value each side.
    let t2 = make_batch_blob_grp_i64(&in_schema, &[(3, 1, lo, 10), (4, 1, top, 10)]).negated();
    let mut avi = Avi::new(&in_schema, &[2u32], &aggs, &[&t1, &t2]);
    let mut trace2 = trace_cursor(out1, out_schema);
    let out2 = op_reduce(
        &t2,
        &mut trace2,
        &in_schema,
        &[2u32],
        &aggs,
        Some(&mut avi.cursor()),
        false,
    );
    let new_row = (0..out2.count).find(|&i| out2.get_weight(i) > 0).expect("new row");
    assert_eq!(content(&out2, new_row, pi_min), mid);
    assert_eq!(content(&out2, new_row, pi_max), hi);
}

// ---------------------------------------------------------------------------
// Global (ungrouped) aggregate ground-row tests
//
// A no-`GROUP BY` aggregate is one logical group with an empty group-column set;
// the output PK is the synthetic constant V₀. `op_reduce` must emit exactly one
// row at V₀ over an empty/fully-retracted source (COUNT=SUM=0, MIN/MAX=NULL).
// ---------------------------------------------------------------------------

/// Source for the global-aggregate tests: `[pk:U64, val:I64(nullable)]`.
/// Build a delta over `u64_pk_schema(SchemaColumn::new(TypeCode::I64, true))` from `(pk, weight, val)` rows.
fn g_delta(rows: &[(u64, i64, i64)]) -> Batch {
    make_batch(&u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)), rows)
}

const G_SUM: AggDescriptor = AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum };
const G_MIN: AggDescriptor = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };

/// Output `[_group_pk:U128, count:I64, min:I64(nullable)]` — the mixed
/// non-linear global-aggregate shape (`SELECT COUNT(*), MIN(x)`).
fn g_out_count_min() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    )
}

/// `op_reduce` over `u64_pk_schema(SchemaColumn::new(TypeCode::I64, true))` with empty group cols (the global-aggregate path).
///
/// `history` is the input the value index has absorbed, this delta included —
/// empty for an all-linear aggregate set, which carries no index.
fn g_reduce(
    delta: &Batch,
    history: &[&Batch],
    trace_out: &mut crate::storage::ReadCursor,
    aggs: &[AggDescriptor],
    seeds_ground: bool,
) -> Batch {
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let mut avi = aggs
        .iter()
        .any(|d| !d.agg_op.is_linear())
        .then(|| Avi::new(&in_schema, &[], aggs, history));
    let mut cursor = avi.as_mut().map(|a| a.cursor());
    op_reduce(delta, trace_out, &in_schema, &[], aggs, cursor.as_mut(), seeds_ground)
}

/// Seed over an empty source emits exactly one ground row at V₀: COUNT=0, SUM=0.
#[test]
fn global_seed_over_empty_emits_one_ground_row() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)),
        &[],
        &[G_SUM, AggDescriptor::COUNT_STAR],
    );
    let mut to_ch = empty_trace(out_schema);

    let raw = g_reduce(
        &g_delta(&[]),
        &[],
        &mut to_ch,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );

    assert_eq!(raw.count, 1, "empty-source global aggregate must emit one ground row");
    assert_eq!(raw.get_weight(0), 1, "ground row weight +1");
    assert_eq!(raw.get_pk(0), gnitz_wire::global_group_key(), "ground PK must be V₀");
    assert_eq!(
        raw.get_null_word(0) & 1,
        0,
        "the raw SUM is present over an empty source"
    );
    assert_eq!(
        read_i64_le(raw.col_data(0), 0),
        0,
        "the raw SUM is 0 over an empty source"
    );
    assert_eq!(
        read_i64_le(raw.col_data(1), 0),
        0,
        "COUNT(*) must be 0 over empty source"
    );
    assert_eq!((raw.get_null_word(0) >> 1) & 1, 0, "COUNT must be present (not NULL)");
}

/// The seed is idempotent: a second empty pad whose trace_out already holds the
/// V₀ ground re-seeds nothing (no weight-2 ground is constructible).
#[test]
fn global_seed_idempotent_across_two_empty_pads() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)),
        &[],
        &[G_SUM, AggDescriptor::COUNT_STAR],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw1 = g_reduce(
        &g_delta(&[]),
        &[],
        &mut to_ch,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );
    assert_eq!(raw1.count, 1, "first pad seeds the ground");

    // Second pad: trace_out now holds the V₀ ground from the first pad.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(
        &g_delta(&[]),
        &[],
        &mut to_ch2,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );
    assert_eq!(raw2.count, 0, "second pad must NOT re-seed (V₀ already in trace_out)");
}

/// Create over a non-empty source emits one computed row and NO ground.
#[test]
fn global_create_over_nonempty_emits_computed_no_ground() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)),
        &[],
        &[G_SUM, AggDescriptor::COUNT_STAR],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw = g_reduce(
        &g_delta(&[(1, 1, 5), (2, 1, 10)]),
        &[],
        &mut to_ch,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );
    assert_eq!(raw.count, 1, "one computed row, no ground");
    assert_eq!(read_i64_le(raw.col_data(0), 0), 15, "SUM=15");
    assert_eq!(read_i64_le(raw.col_data(1), 0), 2, "COUNT=2");
    assert_eq!(raw.get_null_word(0) & 1, 0, "SUM present (not NULL)");
}

/// Fully retracting an all-linear global aggregate sheds the computed row and the
/// ground branch supplies one zero row in its place (net = one ground row).
#[test]
fn global_emptied_by_delete_emits_ground() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)),
        &[],
        &[G_SUM, AggDescriptor::COUNT_STAR],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw1 = g_reduce(
        &g_delta(&[(1, 1, 5)]),
        &[],
        &mut to_ch,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 5, "SUM=5");

    // Retract the only row → cardinality 0.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(
        &g_delta(&[(1, -1, 5)]),
        &[],
        &mut to_ch2,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );

    // Retract old computed (-1) + emit ground (+1) = 2 rows; net view = ground.
    assert_eq!(raw2.count, 2, "retraction of old + ground insert");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(raw2.get_null_word(pos) & 1, 0, "ground SUM present");
    assert_eq!(read_i64_le(raw2.col_data(0), pos * 8), 0, "ground SUM=0");
    assert_eq!(read_i64_le(raw2.col_data(1), pos * 8), 0, "ground COUNT=0");
}

/// A seeded ground transitions to a computed row when the source is populated:
/// the ground at V₀ is retracted and the computed row replaces it.
#[test]
fn global_ground_to_computed_on_first_insert() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)),
        &[],
        &[G_SUM, AggDescriptor::COUNT_STAR],
    );
    // Tick 1: seed the ground over an empty source.
    let mut to_ch = empty_trace(out_schema);
    let ground = g_reduce(
        &g_delta(&[]),
        &[],
        &mut to_ch,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );
    assert_eq!(ground.count, 1);

    // Tick 2: insert a row; trace_out holds the ground.
    let mut to_ch2 = trace_cursor(ground, out_schema);
    let raw2 = g_reduce(
        &g_delta(&[(1, 1, 7)]),
        &[],
        &mut to_ch2,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );

    // Retract ground (-1) + emit computed (+1) = 2 rows; net view = computed.
    assert_eq!(raw2.count, 2, "retract ground + emit computed");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(read_i64_le(raw2.col_data(0), pos * 8), 7, "computed SUM=7");
    assert_eq!(read_i64_le(raw2.col_data(1), pos * 8), 1, "computed COUNT=1");
    assert_eq!(raw2.get_null_word(pos) & 1, 0, "computed SUM present");
}

/// A value-change tick on a surviving global aggregate emits the old/new computed
/// rows and NO ground delta (the group never crosses the cardinality boundary).
#[test]
fn global_value_change_emits_no_ground() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)),
        &[],
        &[G_SUM, AggDescriptor::COUNT_STAR],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw1 = g_reduce(
        &g_delta(&[(1, 1, 5)]),
        &[],
        &mut to_ch,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 5);

    // Change pk1's value 5 → 8 (retract old, insert new) — cardinality stays 1.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(
        &g_delta(&[(1, -1, 5), (1, 1, 8)]),
        &[],
        &mut to_ch2,
        &[G_SUM, AggDescriptor::COUNT_STAR],
        true,
    );
    assert_eq!(raw2.count, 2, "retract old computed + emit new computed");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    // The +1 row is the new computed value, never the ground.
    assert_eq!(read_i64_le(raw2.col_data(0), pos * 8), 8, "new SUM=8");
    assert_eq!(read_i64_le(raw2.col_data(1), pos * 8), 1, "COUNT stays 1");
}

/// Mixed non-linear `[COUNT(*), MIN(x)]` fully emptied → one ground row
/// `COUNT=0, MIN=NULL`, never a `COUNT=−N` zombie.
#[test]
fn global_mixed_count_min_emptied_emits_ground() {
    let out_schema = g_out_count_min();
    let aggs = [AggDescriptor::COUNT_STAR, G_MIN];

    // Tick 1: insert one row into an empty index.
    let mut to_ch = empty_trace(out_schema);
    let d1 = g_delta(&[(1, 1, 5)]);
    let raw1 = g_reduce(&d1, &[&d1], &mut to_ch, &aggs, true);
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 1, "COUNT=1");
    assert_eq!(read_i64_le(raw1.col_data(1), 0), 5, "MIN=5");

    // Tick 2: retract the only row. The index has absorbed both ticks, so the
    // group's one entry nets to zero and the seek finds nothing.
    let d2 = g_delta(&[(1, -1, 5)]);
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(&d2, &[&d1, &d2], &mut to_ch2, &aggs, true);

    assert_eq!(raw2.count, 2, "retract old + ground insert");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(read_i64_le(raw2.col_data(0), pos * 8), 0, "ground COUNT=0, never -N");
    assert_eq!((raw2.get_null_word(pos) >> 1) & 1, 1, "ground MIN=NULL");
}

/// A lone global `MIN` over ≥1 row, then retract the current min → the view
/// advances to the next-best value (the value index over an empty group set,
/// whose key is just the ordinal).
#[test]
fn global_lone_min_retract_to_next_best() {
    // Output `[_group_pk:U128, min:I64(nullable)]`, agg `[MIN]` (no companion).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    // Tick 1: insert val=5 and val=3 → MIN=3.
    let mut to_ch = empty_trace(out_schema);
    let d1 = g_delta(&[(1, 1, 5), (2, 1, 3)]);
    let raw1 = g_reduce(&d1, &[&d1], &mut to_ch, &[G_MIN, AggDescriptor::COUNT_STAR], true);
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 3, "MIN=3");

    // Tick 2: retract the current min (val=3) → MIN advances to 5.
    let d2 = g_delta(&[(2, -1, 3)]);
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(&d2, &[&d1, &d2], &mut to_ch2, &[G_MIN, AggDescriptor::COUNT_STAR], true);
    let mb = raw2.as_mem_batch();
    let pos = (0..raw2.count).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(raw2.col_data(0), pos * 8),
        5,
        "MIN advances 3 → 5 (next-best)"
    );
}

/// The AVI empty-prefix path: a lone global MIN over ≥1 row resolves via the
/// 0-byte-prefix AVI seek — the empty group set packs to a zero-width key, so the
/// prefix walk covers every entry and returns the GLOBAL extremum in agg-value
/// order — and a retraction advances to the next-best AVI post-state.
#[test]
fn global_lone_min_avi_empty_prefix() {
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );

    // AVI for empty group cols: key = av_encoded(8) only (0-byte group prefix).
    let avi_schema = avi_schema(&in_schema, &[]);
    assert_eq!(avi_schema.pk_stride(), 9, "0 group + 1 ordinal + 8 av");
    // The index post-state: one entry per live value, in ascending key order.
    let avi_with = |vals: &[i64]| {
        let mut b = BatchBuilder::new(avi_schema);
        for &v in vals {
            let mut key = [0u8; 9];
            key[0] = 0; // ordinal 0 (single MIN, empty group prefix)
            key[1..9].copy_from_slice(&i64_av(v).to_be_bytes());
            b.begin_row_bytes(&key, 1i64);
            b.end_row();
        }
        b.finish()
    };

    let g_min_avi = |delta: &Batch, to: &mut crate::storage::ReadCursor, avi: &[i64]| {
        let mut avi_ch = trace_cursor(avi_with(avi), avi_schema);
        op_reduce(
            delta,
            to,
            &in_schema,
            &[], // empty group cols
            &[G_MIN, AggDescriptor::COUNT_STAR],
            Some(&mut avi_ch),
            true,
        )
    };

    // The empty-prefix seek walks every entry and returns the GLOBAL min, whatever
    // row's (empty) group it packs.
    let tick1 = g_delta(&[(1, 1, 50), (2, 1, 10), (4, 1, 20)]);
    let mut avi_ch = trace_cursor(avi_with(&[10, 20, 50]), avi_schema);
    let seek = probe_indexed(&in_schema, &[], G_MIN, &tick1, 0, &mut avi_ch).expect("an indexed MIN");
    assert_eq!(
        seek.value_bits() as i64,
        10,
        "the empty-prefix seek returns the GLOBAL min"
    );

    // Tick 1 inserts only, so the group holds its own extreme without a seek.
    let mut to_ch = empty_trace(out_schema);
    let raw1 = g_min_avi(&tick1, &mut to_ch, &[10, 20, 50]);
    let mb1 = raw1.as_mem_batch();
    let p1 = (0..raw1.count).find(|&i| mb1.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(read_i64_le(raw1.col_data(0), p1 * 8), 10);

    // Tick 2: a retraction re-evaluates the group through the seek, which an
    // insert keeps alive; the AVI post-state's min is 20.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_min_avi(&g_delta(&[(2, -1, 10), (3, 1, 30)]), &mut to_ch2, &[20, 30, 50]);
    let mb2 = raw2.as_mem_batch();
    let p2 = (0..raw2.count).find(|&i| mb2.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(raw2.col_data(0), p2 * 8),
        20,
        "AVI advances to the next-best post-state on retraction"
    );
}

// ---------------------------------------------------------------------------
// Two-phase global-aggregate combine
// ---------------------------------------------------------------------------

/// `COUNT(col)` over an all-NULL group renders a concrete `0` with the null bit
/// **clear**, never NULL. Every row hits `step_from_batch`'s null gate, so the
/// CountNonNull accumulator stays untouched; the emitted column's null bit is the
/// only observable distinguishing `0` from NULL (the value bytes are zero either
/// way). Regression guard for the COUNT-family-renders-NULL emitter bug.
#[test]
fn count_non_null_all_null_group_renders_zero_null_clear() {
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)); // [U64 pk, I64 payload(nullable)]
    let desc = AggDescriptor {
        col_idx: 1,
        agg_op: AggFunc::CountNonNull,
    };
    let plan = make_plan(&in_schema, &[0u32], &[desc, AggDescriptor::COUNT_STAR], false);
    let mut accs = plan.shape.acc_template.clone();

    // Two rows whose payload column (payload slot 0) is NULL.
    let mut batch = BatchBuilder::new(u64_pk_schema(SchemaColumn::new(TypeCode::I64, true)));
    for pk in [1u128, 2u128] {
        batch.begin_row(pk, 1);
        batch.put_null();
        batch.end_row();
    }
    let mut batch = batch.finish();
    batch.set_layout_unchecked(Layout::Consolidated);
    let mb = batch.as_mem_batch();
    for row in 0..batch.count {
        accs[0].step_from_batch(&mb, row, mb.get_weight(row));
    }

    // Emit the group row. Natural-PK grouping on the U64 PK col: output is
    // [U64 pk, I64 count_non_null, I64 count], no group-exemplar column.
    let mut output = Batch::with_capacity(&plan.shape.output_schema, 1);
    emit_reduce_row(
        &mut output,
        Some((&mb, 0, plan.shape.key.carried())),
        mb.get_pk_bytes(0),
        &accs,
    );

    assert_eq!(output.count, 1);
    let out_mb = output.as_mem_batch();
    assert_eq!(
        out_mb.get_null_word(0) & 1,
        0,
        "COUNT(col) of all-NULL group must have the null bit CLEAR (renders 0, not NULL)",
    );
    assert_eq!(
        read_i64_le(out_mb.get_col_ptr(0, 0, 8), 0),
        0,
        "COUNT(col) of all-NULL group decodes to 0",
    );
}

/// The ground row renders the COUNT family as a concrete `0` with the null bit
/// clear: an untouched linear accumulator renders `0`. Both COUNT(*)
/// and COUNT(col) ground columns.
#[test]
fn ground_row_renders_count_family_zero_null_clear() {
    let in_schema = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let descs = [
        AggDescriptor::COUNT_STAR,
        AggDescriptor {
            col_idx: 1,
            agg_op: AggFunc::CountNonNull,
        },
    ];
    let v0 = [0u8; 16]; // U128 ground PK (V₀)
    let plan = make_plan(&in_schema, &[], &descs, true);
    // Global-aggregate output: [_group_pk:U128, count_star:I64, count_col:I64].
    let mut raw_output = Batch::with_capacity(&plan.shape.output_schema, 1);
    emit_reduce_row(&mut raw_output, None, &v0, &plan.shape.acc_template);

    assert_eq!(raw_output.count, 1, "ground row emitted");
    let out_mb = raw_output.as_mem_batch();
    // Both count columns (payload slots 0 and 1) render 0 with the null bit clear.
    assert_eq!(
        out_mb.get_null_word(0) & 0b11,
        0,
        "COUNT(*) and COUNT(col) ground columns must be null-clear",
    );
    assert_eq!(
        read_i64_le(out_mb.get_col_ptr(0, 0, 8), 0),
        0,
        "COUNT(*) ground value is 0"
    );
    assert_eq!(
        read_i64_le(out_mb.get_col_ptr(0, 1, 8), 0),
        0,
        "COUNT(col) ground value is 0"
    );
}

// ── ReducePlan::combine — the global reduce split into partials ─────────

/// A combine builds over every partial layout an exact linear partial emits.
#[test]
fn a_combine_builds_over_every_partial_layout() {
    for tc in [TypeCode::I32, TypeCode::U64, TypeCode::I64, TypeCode::Decimal] {
        for nullable in [false, true] {
            let input = u64_pk_schema(SchemaColumn::new(tc, nullable));
            for op in [AggFunc::Count, AggFunc::CountNonNull, AggFunc::Sum] {
                let aggs = [AggDescriptor { agg_op: op, col_idx: 1 }, AggDescriptor::COUNT_STAR];
                let partial = ReducePlan::partial(&input, &aggs).unwrap().unwrap();
                ReducePlan::combine(&partial.shape.output_schema, &aggs, true)
                    .unwrap_or_else(|e| panic!("{op:?} over {tc} nullable={nullable}: {e}"));
            }
        }
    }
}

/// Counts and an integer SUM combine; a float SUM and an extreme do not.
#[test]
fn only_exact_linear_aggregates_combine() {
    let with = |tc: TypeCode, op: AggFunc| {
        let input = u64_pk_schema(SchemaColumn::new(tc, false));
        ReducePlan::partial(
            &input,
            &[AggDescriptor { agg_op: op, col_idx: 1 }, AggDescriptor::COUNT_STAR],
        )
        .unwrap()
        .is_some()
    };
    assert!(with(TypeCode::I64, AggFunc::Sum));
    assert!(with(TypeCode::F64, AggFunc::Count));
    assert!(!with(TypeCode::F64, AggFunc::Sum));
    assert!(!with(TypeCode::F32, AggFunc::Sum));
    assert!(!with(TypeCode::I64, AggFunc::Min));
    assert!(!with(TypeCode::I64, AggFunc::Max));
}

/// A float SUM folds its input to fix the summation order; an integer one does not.
#[test]
fn a_float_sum_consolidates_its_input() {
    let aggs = sum_count_aggs(1);
    let float = u64_pk_schema(SchemaColumn::new(TypeCode::F64, false));
    assert!(!make_plan(&float, &[], &aggs, false).is_exact_linear());
    assert!(make_plan(&make_schema_u64_i64(), &[], &aggs, false).is_exact_linear());
}

/// One reduce instance: its plan and the output it has emitted so far.
struct Instance {
    plan: ReducePlan,
    emitted: Vec<Batch>,
}

impl Instance {
    fn new(plan: ReducePlan) -> Self {
        Instance { plan, emitted: Vec::new() }
    }

    /// One epoch over `delta`, the trace being everything emitted before it.
    fn tick(&mut self, delta: &Batch) -> Batch {
        let schema = self.plan.shape.output_schema;
        let trace = Batch::concat(&schema, self.emitted.iter().map(Batch::as_mem_batch)).into_consolidated(&schema);
        let out = super::op_reduce::op_reduce(delta, &mut trace_cursor(trace, schema), None, &self.plan)
            .into_consolidated(&schema);
        self.emitted.push(Batch::clone(&out));
        out
    }
}

/// `(pk, weight, payload…)` of every row, the payload read as `i64`s.
fn reduce_rows(b: &Batch) -> Vec<(u128, i64, Vec<i64>)> {
    let mb = b.as_mem_batch();
    (0..b.count)
        .map(|r| {
            let payload = (0..b.schema().num_columns() - 1)
                .map(|c| read_i64_le(b.col_data(c), r * 8))
                .collect();
            (b.get_pk(r), mb.get_weight(r), payload)
        })
        .collect()
}

/// Two workers' partials, relayed to one combine, emit exactly the funnel's rows
/// and weights every epoch — through a group emptied back to its ground row.
#[test]
fn two_workers_partials_combine_to_the_funnels_output() {
    let input = make_schema_u64_i64();
    let aggs = [
        AggDescriptor { agg_op: AggFunc::Sum, col_idx: 1 },
        AggDescriptor {
            agg_op: AggFunc::CountNonNull,
            col_idx: 1,
        },
        AggDescriptor::COUNT_STAR,
    ];
    let partial = || Instance::new(ReducePlan::partial(&input, &aggs).unwrap().unwrap());
    let (mut a, mut b) = (partial(), partial());
    let partials = a.plan.shape.output_schema;
    let mut combine = Instance::new(ReducePlan::combine(&partials, &aggs, true).unwrap());
    let mut funnel = Instance::new(make_plan(&input, &[], &aggs, true));

    // Per epoch, each worker's `(pk, weight, val)` rows.
    type Rows<'a> = &'a [(u64, i64, i64)];
    let ticks: &[(Rows<'_>, Rows<'_>)] = &[
        (&[], &[]),
        (&[(1, 1, 10), (2, 1, 20)], &[(3, 1, 5)]),
        (&[(1, -1, 10)], &[]),
        (&[], &[(4, 2, 7)]),
        (&[(2, -1, 20)], &[(3, -1, 5), (4, -2, 7)]),
    ];
    for (i, &(da, db)) in ticks.iter().enumerate() {
        let (da, db) = (make_batch_raw(&input, da), make_batch_raw(&input, db));
        let relayed = Batch::concat(&partials, [a.tick(&da), b.tick(&db)].iter().map(Batch::as_mem_batch));
        let combined = combine.tick(&relayed);
        let whole = Batch::concat(&input, [da, db].iter().map(Batch::as_mem_batch));
        let want = funnel.tick(&whole);
        assert_eq!(reduce_rows(&combined), reduce_rows(&want), "tick {i}");
    }
    // The last tick emptied the group: the ground row stands, at V₀.
    let net = Batch::concat(&partials, combine.emitted.iter().map(Batch::as_mem_batch)).into_consolidated(&partials);
    assert_eq!(
        reduce_rows(&net),
        vec![(gnitz_wire::global_group_key(), 1, vec![0, 0, 0])]
    );
}

// ===========================================================================
// Combined AggValueIndex: one table per reduce, keyed `group ‖ ordinal ‖ av`,
// serving every MIN/MAX aggregate (grouped or global). These drive op_reduce
// through a real combined index populated by the production
// `avi_batch` + ingest flow, so the per-ordinal write and read sides
// agree with no hand-built keys.
// ===========================================================================

/// Create an ephemeral combined-AVI table and populate it by integrating each
/// delta in `deltas` (the accumulated integral the index must reflect at read
/// time) through the real `avi_batch` + ingest. The caller opens a cursor
/// on the returned table.
fn build_combined_avi(
    dir: &std::path::Path,
    in_schema: &SchemaDescriptor,
    group_cols: &[u32],
    agg_descs: &[AggDescriptor],
    deltas: &[&Batch],
) -> crate::storage::Table {
    let avi_schema = avi_schema(in_schema, group_cols);
    let mut t = scratch_table(dir.to_str().unwrap(), avi_schema);
    let bake = make_bake(in_schema, group_cols, agg_descs);
    for d in deltas {
        use super::avi::avi_batch;
        t.ingest_owned_batch(avi_batch(d, &bake)).unwrap();
    }
    t
}

/// Schema `[pk:U64, g:I32, a:I64, b:I64]` (group by the I32 payload `g`).
fn cg_src() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

/// Raw delta over `cg_src()` from `(pk, w, g, a, b)` rows.
fn cg_delta(rows: &[(u64, i64, i32, i64, i64)]) -> Batch {
    let s = cg_src();
    let mut b = BatchBuilder::new(s);
    for &(pk, w, g, a, bb) in rows {
        b.begin_row(pk as u128, w);
        b.put_int(g as u128);
        b.put_int(a as u128);
        b.put_int(bb as u128);
        b.end_row();
    }
    b.finish()
}

/// Foreign-group regression: a source PK appearing in two groups must not leak
/// one group's rows into the other's extremes. The GI over-read this fix removes
/// would corrupt MIN/MAX here; the combined index isolates each group by its key
/// prefix. The delta values are deliberately interleaved so a correct result can
/// only come from per-group isolation.
#[test]
fn reduce_multi_avi_foreign_group() {
    let in_schema = cg_src();
    // Output: [g:I32, min_a:I64(nullable), max_b:I64(nullable), count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];

    // pk=1 and pk=2 each span groups g=10 and g=20 (the foreign-group trigger).
    let delta = cg_delta(&[
        (1, 1, 10, 5, 100),
        (1, 1, 20, 7, 200),
        (2, 1, 10, 3, 50),
        (2, 1, 20, 9, 300),
    ]);

    let tmp = tempfile::tempdir().unwrap();
    let avi_t = build_combined_avi(tmp.path(), &in_schema, &[1u32], &aggs, &[&delta]);
    let mut avi_ch = avi_t.open_cursor();

    let mut to_ch = empty_trace(out_schema);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &aggs, Some(&mut avi_ch), false);

    assert_eq!(out.count, 2, "two groups → two rows");
    for i in 0..out.count {
        let mut le = [0u8; 4];
        gnitz_wire::decode_pk_column(out.get_pk_bytes(i), TypeCode::I32, &mut le);
        let g = i32::from_le_bytes(le);
        let min_a = read_i64_le(out.col_data(0), i * 8);
        let max_b = read_i64_le(out.col_data(1), i * 8);
        let count = read_i64_le(out.col_data(2), i * 8);
        match g {
            10 => {
                assert_eq!(min_a, 3, "group 10 MIN(a) excludes group 20");
                assert_eq!(max_b, 100, "group 10 MAX(b) excludes group 20");
                assert_eq!(count, 2);
            }
            20 => {
                assert_eq!(min_a, 7, "group 20 MIN(a) excludes group 10");
                assert_eq!(max_b, 300, "group 20 MAX(b) excludes group 10");
                assert_eq!(count, 2);
            }
            other => panic!("unexpected group {other}"),
        }
    }
}

/// Schema `[pk:U64, g:I32, a:I64]`; output `[g:I32, min:I64?, count:I64]`.
fn cg3_src() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    )
}
fn cg3_out() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}
/// `(pk, w, g, a, a_null)` delta over `cg3_src()`.
fn cg3_delta(rows: &[(u64, i64, i32, i64, bool)]) -> Batch {
    let s = cg3_src();
    let mut b = BatchBuilder::new(s);
    for &(pk, w, g, a, a_null) in rows {
        b.begin_row(pk as u128, w);
        b.put_int(g as u128);
        match a_null {
            true => b.put_null(),
            false => b.put_int(a as u128),
        }
        b.end_row();
    }
    b.finish()
}
const CG3_MIN: AggDescriptor = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

/// Helper: run one combined-index reduce tick over `cg3_src()` (MIN + COUNT).
/// `avi_deltas` is the accumulated integral; `delta` is this tick's input.
fn cg3_tick(
    dir: &std::path::Path,
    avi_deltas: &[&Batch],
    delta: &Batch,
    aggs: &[AggDescriptor],
    trace_out: &mut crate::storage::ReadCursor,
) -> Batch {
    let in_schema = cg3_src();
    let avi_t = build_combined_avi(dir, &in_schema, &[1u32], aggs, avi_deltas);
    let mut avi_ch = avi_t.open_cursor();
    op_reduce(delta, trace_out, &in_schema, &[1u32], aggs, Some(&mut avi_ch), false)
}

/// Linear companion folded alongside the indexed MIN: COUNT = old + Σdelta, MIN
/// from the index, across an insert then a partial retraction.
#[test]
fn reduce_multi_avi_linear_companion() {
    let aggs = [CG3_MIN, AggDescriptor::COUNT_STAR];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Tick 1: insert g=7 {a=5, a=8, a=3}.
    let d1 = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 8, false), (3, 1, 7, 3, false)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 3, "MIN=3 from the index");
    assert_eq!(read_i64_le(out1.col_data(1), 0), 3, "COUNT=3");

    // Tick 2: retract a=3 (the current min). MIN→5 (index post-state), COUNT 3→2.
    let d2 = cg3_delta(&[(3, -1, 7, 3, false)]);
    let mut to2 = trace_cursor(out1, out_schema);
    let out2 = cg3_tick(tmp.path(), &[&d1, &d2], &d2, &aggs, &mut to2);
    // retract old (MIN=3,count=3 @ -1) + insert new (MIN=5,count=2 @ +1).
    let mb = out2.as_mem_batch();
    let ins = (0..out2.count).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(out2.col_data(0), ins * 8),
        5,
        "MIN recomputed to 5 from the index"
    );
    assert_eq!(
        read_i64_le(out2.col_data(1), ins * 8),
        2,
        "COUNT = old(3) + Σdelta(-1) = 2"
    );
}

/// Phantom-bug regression: a group fully retracted in a tick must emit exactly
/// one −1 retraction and NO +1 (the folded companion COUNT nets to 0, which the
/// cardinality gate reads to suppress the row).
#[test]
fn reduce_multi_avi_emptied_with_companion() {
    let aggs = [CG3_MIN, AggDescriptor::COUNT_STAR];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    let d1 = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 8, false)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1);

    // Tick 2: retract both rows → group emptied.
    let d2 = cg3_delta(&[(1, -1, 7, 5, false), (2, -1, 7, 8, false)]);
    let mut to2 = trace_cursor(out1, out_schema);
    let out2 = cg3_tick(tmp.path(), &[&d1, &d2], &d2, &aggs, &mut to2);
    assert_eq!(
        out2.count, 1,
        "emptied group: only the −1 retraction, no phantom insert"
    );
    assert_eq!(out2.get_weight(0), -1, "the sole row is the retraction");
}

/// Cardinality semantics: a group whose rows are all-NULL in the aggregate
/// column still exists (count > 0) and must emit `(g, NULL, count)`.
#[test]
fn reduce_multi_avi_all_null_emit() {
    let aggs = [CG3_MIN, AggDescriptor::COUNT_STAR];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Group 7 with two rows, both a = NULL.
    let d1 = cg3_delta(&[(1, 1, 7, 0, true), (2, 1, 7, 0, true)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1, "all-NULL group with rows must still emit");
    assert_eq!(out1.get_weight(0), 1);
    assert_eq!(read_i64_le(out1.col_data(1), 0), 2, "COUNT=2");
    // MIN null bit: payload col 0 (min) → null-word bit 0.
    assert_ne!(out1.get_null_word(0) & 1, 0, "MIN renders NULL for all-NULL group");
}

/// Nullable MIN over the combined index: a mixed group `{a=5, a=NULL}` whose
/// non-null row is retracted must SURVIVE as `(g, NULL, 1)` — not be dropped —
/// then disappear when the last (NULL) row is retracted. Regression for the
/// all-NULL-group drop the cardinality gate fixes, while the nullable MIN stays
/// on the combined index.
#[test]
fn reduce_multi_avi_retract_to_all_null() {
    let aggs = [CG3_MIN, AggDescriptor::COUNT_STAR];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Tick 1: g=7 = {a=5, a=NULL}. MIN=5, COUNT=2.
    let d1 = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 0, true)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 5, "MIN=5");

    // Tick 2: retract a=5. Group survives via the NULL row: (g, NULL, 1).
    let d2 = cg3_delta(&[(1, -1, 7, 5, false)]);
    let mut to2 = trace_cursor(out1, out_schema);
    let out2 = cg3_tick(tmp.path(), &[&d1, &d2], &d2, &aggs, &mut to2);
    let mb2 = out2.as_mem_batch();
    let ins = (0..out2.count)
        .find(|&i| mb2.get_weight(i) == 1)
        .expect("a +1 row — group survives");
    assert_eq!(read_i64_le(out2.col_data(1), ins * 8), 1, "COUNT=1 (the NULL row)");
    assert_ne!(out2.get_null_word(ins) & 1, 0, "MIN now NULL but group present");

    // Rebuild tick-2 output row to seed tick-3 trace_out.
    let row2 = {
        // the +1 row of out2
        let mut b = out2.ascending_subset(&[ins as u32]);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // Tick 3: retract the remaining NULL row → group gone (only the −1).
    let d3 = cg3_delta(&[(2, -1, 7, 0, true)]);
    let mut to3 = trace_cursor(row2, out_schema);
    let out3 = cg3_tick(tmp.path(), &[&d1, &d2, &d3], &d3, &aggs, &mut to3);
    assert_eq!(out3.count, 1, "group now absent: only the −1 retraction");
    assert_eq!(out3.get_weight(0), -1);
}

/// `MIN(a), MAX(a)` over the SAME column: two ordinals (opposite directions) in
/// one combined index. The ordinal byte keeps them from colliding.
#[test]
fn reduce_multi_avi_same_col_min_max() {
    let in_schema = cg3_src();
    // Output: [g:I32, min:I64?, max:I64?, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];
    let delta = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 8, false), (3, 1, 7, 1, false)]);

    let tmp = tempfile::tempdir().unwrap();
    let avi_t = build_combined_avi(tmp.path(), &in_schema, &[1u32], &aggs, &[&delta]);
    let mut avi_ch = avi_t.open_cursor();
    let mut to = empty_trace(out_schema);
    let out = op_reduce(&delta, &mut to, &in_schema, &[1u32], &aggs, Some(&mut avi_ch), false);
    assert_eq!(out.count, 1);
    assert_eq!(read_i64_le(out.col_data(0), 0), 1, "MIN(a)=1 (ordinal 0)");
    assert_eq!(
        read_i64_le(out.col_data(1), 0),
        8,
        "MAX(a)=8 (ordinal 1), no ordinal collision"
    );
}

/// Compound group key `(g1, g2)` (both fixed-int, non-nullable) resolves via the
/// combined index. Two group columns + the ordinal + the value stay within the
/// PK budget.
#[test]
fn reduce_multi_avi_compound_group_key() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    // Synthetic group (g1, g2): [_group_pk:U128, g1:I32, g2:I32, min:I64?, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Min },
        AggDescriptor::COUNT_STAR,
    ];
    let delta = {
        let mut b = BatchBuilder::new(in_schema);
        for (pk, g1, g2, a) in [(1u64, 1i32, 1i32, 9i64), (2, 1, 1, 4), (3, 1, 2, 7), (4, 1, 2, 2)] {
            b.begin_row(pk as u128, 1i64);
            b.put_int(g1 as u128);
            b.put_int(g2 as u128);
            b.put_int(a as u128);
            b.end_row();
        }
        b.finish()
    };

    let tmp = tempfile::tempdir().unwrap();
    let avi_t = build_combined_avi(tmp.path(), &in_schema, &[1u32, 2u32], &aggs, &[&delta]);
    let mut avi_ch = avi_t.open_cursor();
    let mut to = empty_trace(out_schema);
    let out = op_reduce(
        &delta,
        &mut to,
        &in_schema,
        &[1u32, 2u32],
        &aggs,
        Some(&mut avi_ch),
        false,
    );
    assert_eq!(out.count, 2, "two compound groups");
    for i in 0..out.count {
        let g1 = gnitz_wire::read_u32_le(out.col_data(0), i * 4) as i32;
        let g2 = gnitz_wire::read_u32_le(out.col_data(1), i * 4) as i32;
        let min = read_i64_le(out.col_data(2), i * 8);
        match (g1, g2) {
            (1, 1) => assert_eq!(min, 4, "group (1,1) MIN"),
            (1, 2) => assert_eq!(min, 2, "group (1,2) MIN"),
            other => panic!("unexpected group {other:?}"),
        }
    }
}

/// Global `MIN(a), SUM(b)` over the combined index, fully retracted, emits the
/// ground row.
#[test]
fn reduce_multi_avi_global_emptied() {
    // [pk:U64, a:I64, b:I64]; global aggregate.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    // Output: [_group_pk:U128, min:I64?, sum:I64, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
        AggDescriptor::COUNT_STAR,
    ];
    let mk = |rows: &[(u64, i64, i64, i64)]| {
        let mut b = BatchBuilder::new(in_schema);
        for &(pk, w, a, bb) in rows {
            b.begin_row(pk as u128, w);
            b.put_int(a as u128);
            b.put_int(bb as u128);
            b.end_row();
        }
        b.finish()
    };
    let tmp = tempfile::tempdir().unwrap();

    let run = |avi_deltas: &[&Batch], delta: &Batch, to: &mut crate::storage::ReadCursor| -> Batch {
        let avi_t = build_combined_avi(tmp.path(), &in_schema, &[], &aggs, avi_deltas);
        let mut avi_ch = avi_t.open_cursor();
        op_reduce(delta, to, &in_schema, &[], &aggs, Some(&mut avi_ch), true)
    };

    // Tick 1: insert {a=5,b=10},{a=3,b=20}. MIN=3, SUM=30, COUNT=2.
    let d1 = mk(&[(1, 1, 5, 10), (2, 1, 3, 20)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = run(&[&d1], &d1, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 3, "MIN=3");
    assert_eq!(read_i64_le(out1.col_data(1), 0), 30, "SUM=30");

    // Tick 2: retract everything → ground row (MIN=NULL, SUM=0, COUNT=0).
    let d2 = mk(&[(1, -1, 5, 10), (2, -1, 3, 20)]);
    let mut to2 = trace_cursor(out1, out_schema);
    let out2 = run(&[&d1, &d2], &d2, &mut to2);
    let mb = out2.as_mem_batch();
    let ins = (0..out2.count)
        .find(|&i| mb.get_weight(i) == 1)
        .expect("a +1 ground row");
    assert_ne!(out2.get_null_word(ins) & (1 << 0), 0, "ground MIN = NULL");
    assert_eq!(out2.get_null_word(ins) & (1 << 1), 0, "ground SUM present");
    assert_eq!(read_i64_le(out2.col_data(1), ins * 8), 0, "ground SUM = 0");
    assert_eq!(read_i64_le(out2.col_data(2), ins * 8), 0, "ground COUNT = 0");
}

// ===========================================================================
// The ascending trace_out probe, over every group-key arm.
//
// `op_reduce` visits groups in ascending output-PK order on every arm, which is
// what its one retraction probe, `seek_pk_group_ascending`, is sound under. These
// tests drive it across multi-epoch retraction (incl. sign-flip boundaries) and
// over the nullable fold arm, and force a multi-source trace cursor so the
// merge-mode gallop runs at scale.
// ===========================================================================

/// One input row over a `[U64 pk, <grp>, I64 val]` schema: `(pk, grp, val,
/// weight)`. `grp == None` marks a NULL group (nullable schemas only). Shared
/// by the monotone-probe tests below and the non-linear fallback tests above.
type GrpValRow = (u64, Option<i64>, i64, i64);

/// Output schema for a synthetic-U128-PK `SUM/COUNT` reduce:
/// `[U128 pk, <grp>, I64 sum, I64 count]`. The trailing count is the
/// cardinality companion every circuit reduce carries.
fn sum_count_out_synthetic(grp_col: SchemaColumn) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            grp_col,
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

/// Output schema for a natural-PK `SUM/COUNT` reduce over a non-nullable
/// group column: `[<grp> pk, I64 sum, I64 count]`.
fn sum_count_out_natural(grp_col: SchemaColumn) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            grp_col,
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

/// Build an input delta over `[U64 pk, <grp>, I64 val]` from `(pk, grp, val, w)`
/// rows; `grp == None` marks the group column NULL (nullable schemas only). The
/// grp column is written from an i64 image, which is byte-identical to the u64
/// image for the non-negative values the U64-group test uses.
fn build_grp_val_delta(schema: &SchemaDescriptor, rows: &[GrpValRow]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, grp, val, w) in rows {
        b.begin_row(pk as u128, w);
        b.put_opt_int(grp.map(|g| g as u128));
        b.put_int(val as u128);
        b.end_row();
    }
    b.finish()
}

/// DBSP linear reference for `SUM(val)/COUNT` over the raw input rows: both
/// aggregates are linear, so `sum[g] += val·w` and `count[g] += w` over every
/// input row across every epoch; a group is live iff `count ≠ 0`. A NULL group
/// (`None`) is its own group.
fn sum_count_reference<'a>(
    rows: impl IntoIterator<Item = &'a GrpValRow>,
) -> std::collections::BTreeMap<Option<i64>, (i128, i64)> {
    let mut m: std::collections::BTreeMap<Option<i64>, (i128, i64)> = std::collections::BTreeMap::new();
    for &(_pk, g, v, w) in rows {
        let e = m.entry(g).or_insert((0, 0));
        e.0 += v as i128 * w as i128;
        e.1 += w;
    }
    m.retain(|_, &mut (_, c)| c != 0);
    m
}

/// Drive `op_reduce` over `epochs` against a real ephemeral `trace_out` Table,
/// integrating each epoch's output delta back into the trace (flushed once
/// after epoch 0 when `flush_after_first`). Returns the final consolidated
/// trace_out batch and the maximum source count any epoch's probe cursor was
/// built from, so a caller can assert the cursor's merge mode (≥ 3 sources, all live at open) was
/// exercised.
fn run_reduce_trace_epochs(
    dir: &std::path::Path,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    group_by: &[u32],
    aggs: &[AggDescriptor],
    epochs: &[Batch],
    flush_after_first: bool,
) -> (std::rc::Rc<Batch>, usize) {
    // A fresh tempdir per call isolates shard files, so a constant table_id is
    // collision-free.
    let mut trace = scratch_table(dir.to_str().unwrap(), *out_schema);
    let mut max_sources = 0usize;
    for (i, d) in epochs.iter().enumerate() {
        // Sources the cursor for THIS epoch's probe sees: memtable runs + folded
        // in-memory runs + shard files (each becomes one CursorSource).
        let sources = trace.runs().count();
        max_sources = max_sources.max(sources);
        let out = {
            let mut ch = trace.open_cursor();
            op_reduce(d, &mut ch, in_schema, group_by, aggs, None, false)
        };
        trace.ingest_owned_batch(out).unwrap();
        if flush_after_first && i == 0 {
            trace.fold_to_ram().unwrap();
        }
    }
    (trace.full_scan(), max_sources)
}

/// Read a synthetic-U128-PK reduce output `[U128 pk, <grp>, I64 sum, I64 count]`
/// into a `grp → (sum, count)` map (a NULL group key reads as `None`), asserting
/// one net-weight-1 row per group.
fn readback_synthetic_i64(batch: &Batch) -> std::collections::BTreeMap<Option<i64>, (i128, i64)> {
    let mut m: std::collections::BTreeMap<Option<i64>, (i128, i64)> = std::collections::BTreeMap::new();
    for r in 0..batch.count {
        assert_eq!(batch.get_weight(r), 1, "each live group row nets weight 1");
        let grp = (batch.get_null_word(r) & 1 == 0).then(|| read_i64_le(batch.col_data(0), r * 8));
        let sum = read_i64_le(batch.col_data(1), r * 8) as i128;
        let count = read_i64_le(batch.col_data(2), r * 8);
        assert!(
            m.insert(grp, (sum, count)).is_none(),
            "one output row per group (grp={grp:?})"
        );
    }
    m
}

/// Read a natural-I64-PK reduce output `[I64 pk(=grp), I64 sum, I64 count]`
/// into a `grp → (sum, count)` map, asserting one net-weight-1 row per group.
fn readback_natural_i64(batch: &Batch) -> std::collections::BTreeMap<Option<i64>, (i128, i64)> {
    let mut m: std::collections::BTreeMap<Option<i64>, (i128, i64)> = std::collections::BTreeMap::new();
    for r in 0..batch.count {
        assert_eq!(batch.get_weight(r), 1, "each live group row nets weight 1");
        let grp = opk_pk_i64(batch.get_pk_bytes(r));
        let sum = read_i64_le(batch.col_data(0), r * 8) as i128;
        let count = read_i64_le(batch.col_data(1), r * 8);
        assert!(
            m.insert(Some(grp), (sum, count)).is_none(),
            "one output row per group (grp={grp})"
        );
    }
    m
}

// Payload I64 group key, multi-epoch retraction across the sign flip. GROUP BY
// a non-nullable I64 payload column ⇒ a natural output PK, the column's
// sign-flipped OPK image. Epoch 1 inserts every
// group; epoch 2 updates and deletes rows in several groups (including the
// negative ones), so the retraction probe galloping over sign-flipped output
// PKs must land on the exact old aggregate row.
#[test]
fn reduce_monotone_probe_payload_i64_group() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false), // grp (non-nullable payload) — natural PK
            SchemaColumn::new(TypeCode::I64, false), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_natural(SchemaColumn::new(TypeCode::I64, false));
    let aggs = sum_count_aggs(2);

    let epoch_rows: [&[GrpValRow]; 2] = [
        &[
            (1, Some(-5), 10, 1),
            (2, Some(-5), 20, 1),
            (3, Some(-1), 5, 1),
            (8, Some(-1), -3, 1),
            (4, Some(0), 7, 1),
            (5, Some(0), 8, 1),
            (6, Some(3), 100, 1),
            (7, Some(3), 200, 1),
        ],
        &[
            (2, Some(-5), 20, -1), // delete a group-(-5) row
            (6, Some(3), 100, -1),
            (6, Some(3), 150, 1), // update a group-3 row 100 → 150
            (4, Some(0), 7, -1),  // delete a group-0 row
            (9, Some(-1), 50, 1), // insert a new group-(-1) row
        ],
    ];
    let batches: Vec<Batch> = epoch_rows.iter().map(|r| build_grp_val_delta(&in_schema, r)).collect();
    let reference = sum_count_reference(epoch_rows.iter().flat_map(|r| r.iter()));

    let tmp = tempfile::tempdir().unwrap();
    let (final_batch, _) =
        run_reduce_trace_epochs(tmp.path(), &in_schema, &out_schema, &[1u32], &aggs, &batches, false);

    assert_eq!(
        final_batch.count,
        reference.len(),
        "one live row per group, no un-cancelled ghosts"
    );
    assert_eq!(
        readback_natural_i64(&final_batch),
        reference,
        "incremental SUM/COUNT across sign-flip retraction must match the from-scratch reference",
    );
}

// Payload U64 group key — the unsigned arm: the output PK is the group value.
// Groups straddle the high byte (1 vs 256) so a byte-order slip in the gallop
// would misorder them.
#[test]
fn reduce_monotone_probe_payload_u64_group() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false), // grp (non-nullable payload) — natural PK
            SchemaColumn::new(TypeCode::I64, false), // val
        ],
        &[0],
    );
    // Natural U64 output PK: [U64 pk(=grp), I64 sum, I64 count].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = sum_count_aggs(2);

    let epoch_rows: [&[GrpValRow]; 2] = [
        &[
            (1, Some(0), 5, 1),
            (2, Some(1), 10, 1),
            (3, Some(1), 20, 1),
            (4, Some(256), 7, 1),
            (5, Some(1000), 100, 1),
            (6, Some(1000), 200, 1),
        ],
        &[
            (3, Some(1), 20, -1), // delete a group-1 row
            (4, Some(256), 7, -1),
            (4, Some(256), 70, 1), // update group 256: 7 → 70
            (7, Some(0), 15, 1),   // insert into group 0
        ],
    ];
    let batches: Vec<Batch> = epoch_rows.iter().map(|r| build_grp_val_delta(&in_schema, r)).collect();
    let reference = sum_count_reference(epoch_rows.iter().flat_map(|r| r.iter()));

    let tmp = tempfile::tempdir().unwrap();
    let (final_batch, _) =
        run_reduce_trace_epochs(tmp.path(), &in_schema, &out_schema, &[1u32], &aggs, &batches, false);

    assert_eq!(final_batch.count, reference.len(), "one live row per group");
    // Natural-PK readback: group value is the (big-endian, unsigned) output PK;
    // payload is [sum, count].
    let mut got: std::collections::BTreeMap<Option<i64>, (i128, i64)> = std::collections::BTreeMap::new();
    for r in 0..final_batch.count {
        assert_eq!(final_batch.get_weight(r), 1);
        let grp = u64::from_be_bytes(final_batch.get_pk_bytes(r)[..8].try_into().unwrap()) as i64;
        let sum = read_i64_le(final_batch.col_data(0), r * 8) as i128;
        let count = read_i64_le(final_batch.col_data(1), r * 8);
        assert!(
            got.insert(Some(grp), (sum, count)).is_none(),
            "one output row per group"
        );
    }
    assert_eq!(
        got, reference,
        "unsigned-arm incremental SUM/COUNT must match the reference"
    );
}

// A nullable group column keys by the fold. Same retraction workload plus a NULL
// group (its own group); results must still match the from-scratch reference.
#[test]
fn reduce_nullable_group_takes_the_hash_arm() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),  // grp NULLABLE ⇒ fold arm
            SchemaColumn::new(TypeCode::I64, false), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_synthetic(SchemaColumn::new(TypeCode::I64, true)); // grp nullable
    let aggs = sum_count_aggs(2);

    let epoch_rows: [&[GrpValRow]; 2] = [
        &[
            (1, None, 10, 1),
            (2, None, 20, 1), // NULL group
            (3, Some(-1), 5, 1),
            (4, Some(7), 100, 1),
            (5, Some(7), 1, 1),
        ],
        &[
            (2, None, 20, -1),     // retract a NULL-group row
            (4, Some(7), 100, -1), // delete a group-7 row
            (6, Some(-1), 50, 1),  // insert into group -1
        ],
    ];
    let batches: Vec<Batch> = epoch_rows.iter().map(|r| build_grp_val_delta(&in_schema, r)).collect();
    let reference = sum_count_reference(epoch_rows.iter().flat_map(|r| r.iter()));
    assert!(reference.contains_key(&None), "the NULL group must stay live");

    let tmp = tempfile::tempdir().unwrap();
    let (final_batch, _) =
        run_reduce_trace_epochs(tmp.path(), &in_schema, &out_schema, &[1u32], &aggs, &batches, false);

    assert_eq!(final_batch.count, reference.len(), "one live row per group");
    assert_eq!(
        readback_synthetic_i64(&final_batch),
        reference,
        "hash-arm incremental SUM/COUNT (incl. the NULL group) must match the reference",
    );
}

// Many groups per epoch over ≥ 3 trace_out sources. 200 groups spanning the sign
// flip (-100..99), six epochs each touching every group (insert / update /
// delete). One flush after epoch 0 plus accumulating memtable runs pushes the
// trace_out cursor into merge mode (≥ 3 sources, all live at open) from epoch 3 on, so the
// merge-mode gallop executes across hundreds of monotone probes. Construction makes every
// group's final aggregate identical (SUM=10, COUNT=3), so any mis-landed
// retraction shows up as a wrong sum or an un-cancelled duplicate.
#[test]
fn reduce_monotone_probe_many_groups_multi_source() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false), // grp — natural PK
            SchemaColumn::new(TypeCode::I64, false), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_natural(SchemaColumn::new(TypeCode::I64, false));
    let aggs = sum_count_aggs(2);

    let groups: Vec<i64> = (-100..100).collect();
    let mut next_pk = 0u64;
    let mut epochs_data: Vec<Vec<GrpValRow>> = Vec::new();
    for ep in 0..6 {
        let mut rows = Vec::new();
        for &g in &groups {
            let vws: Vec<(i64, i64)> = match ep {
                0 => vec![(g, 1), (g + 1000, 1)],
                1 => vec![(5, 1)],
                2 => vec![(g, -1)], // retract epoch-0 insert #1
                3 => vec![(7, 1)],
                4 => vec![(g + 1000, -1)], // retract epoch-0 insert #2
                5 => vec![(-2, 1)],
                _ => unreachable!(),
            };
            for (val, w) in vws {
                rows.push((next_pk, Some(g), val, w));
                next_pk += 1;
            }
        }
        epochs_data.push(rows);
    }
    let batches: Vec<Batch> = epochs_data.iter().map(|r| build_grp_val_delta(&in_schema, r)).collect();
    let reference = sum_count_reference(epochs_data.iter().flatten());
    assert_eq!(reference.len(), 200, "all 200 groups stay live");

    let tmp = tempfile::tempdir().unwrap();
    let (final_batch, max_sources) =
        run_reduce_trace_epochs(tmp.path(), &in_schema, &out_schema, &[1u32], &aggs, &batches, true);

    assert!(
        max_sources >= 3,
        "trace_out probe must reach merge mode (≥ 3 sources, all live at open); saw {max_sources}",
    );
    assert_eq!(
        final_batch.count,
        reference.len(),
        "one live row per group, no duplicates"
    );
    assert_eq!(
        readback_natural_i64(&final_batch),
        reference,
        "many-group multi-source incremental SUM/COUNT must match the from-scratch reference",
    );
}

// ===========================================================================
// MIN/MAX AVI probe-skip: an all-insert integer group folds `combine(old, pos)`
// from the reduce's own accumulator and the stored trace_out extreme instead of
// probing the value index; a group with any retraction (or a float source, or a
// group past the pre-step cap) still probes. These tests drive the *real* AVI
// path — the value index is populated per epoch by `avi_batch` + ingest
// (post-delta, as the compiler wires it: AVI Integrate precedes Reduce) and the
// reduce reads it — and check the skip path against both the unchanged trace-scan
// (`avi = None`) path and a from-scratch oracle.
// ===========================================================================

/// Drive a non-linear (MIN/MAX + COUNT companion) reduce over `epochs` against
/// live ephemeral tables, returning the consolidated `trace_out` after each
/// epoch.
///
/// Each input delta is integrated into a live AVI table *before* the reduce (the
/// index must reflect post-delta `I(input)`), and the reduce reads that index
/// plus the pre-delta `trace_out`, exactly as the compiler orders the two
/// instructions.
fn run_minmax_epochs(
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    group_by: &[u32],
    aggs: &[AggDescriptor],
    epochs: &[Batch],
    global_ground: bool,
) -> Vec<std::rc::Rc<Batch>> {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();

    let mut trace_out = scratch_table(dir, *out_schema);
    let avi_schema = avi_schema(in_schema, group_by);
    let mut avi_t = scratch_table(dir, avi_schema);

    let avi_bake = make_bake(in_schema, group_by, aggs);
    let mut states = Vec::with_capacity(epochs.len());
    for d in epochs {
        // AVI Integrate precedes Reduce: post-delta `I(input)` before the read.
        use super::avi::avi_batch;
        avi_t.ingest_owned_batch(avi_batch(d, &avi_bake)).unwrap();
        let out = {
            let mut to_ch = trace_out.open_cursor();
            let mut avi_ch = avi_t.open_cursor();
            op_reduce(
                d,
                &mut to_ch,
                in_schema,
                group_by,
                aggs,
                Some(&mut avi_ch),
                global_ground,
            )
        };
        trace_out.ingest_owned_batch(out).unwrap();
        states.push(trace_out.full_scan());
    }
    states
}

/// The MIN/MAX epoch fixture: `val` nullable, because the all-NULL-group cases
/// write a null bit into it.
fn mm_in_schema() -> SchemaDescriptor {
    u64pk_i64grp_i64val(true)
}

/// `MIN(val), MAX(val), COUNT(pk)` — MIN ordinal 0, MAX ordinal 1 in the AVI.
fn mm_aggs() -> [AggDescriptor; 3] {
    [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ]
}

/// Raw (unconsolidated) input delta over `mm_in_schema()` from `(pk, grp, val, w)`
/// rows; `op_reduce` consolidates internally. `grp`/`val` are non-NULL.
fn build_mm_delta(rows: &[(u64, i64, i64, i64)]) -> Batch {
    let s = mm_in_schema();
    let mut b = BatchBuilder::new(s);
    for &(pk, grp, val, w) in rows {
        b.begin_row(pk as u128, w);
        b.put_int(grp as u128);
        b.put_int(val as u128);
        b.end_row();
    }
    b.finish()
}

/// `grp → (min, max, count)` for a MIN/MAX/COUNT reduce, NULL extremes as `None`.
type MmState = std::collections::BTreeMap<i64, (Option<i64>, Option<i64>, i64)>;

/// Read a consolidated natural-I64-keyed MIN/MAX/COUNT batch into `grp → (min, max, count)`,
/// a NULL min/max reading as `None`. Asserts one net-weight-1 row per group.
fn readback_mm(b: &Batch) -> MmState {
    let mut m = std::collections::BTreeMap::new();
    for r in 0..b.count {
        assert_eq!(b.get_weight(r), 1, "each live group is one net-weight-1 row");
        let nw = b.get_null_word(r);
        let grp = opk_pk_i64(b.get_pk_bytes(r));
        let min = (!gnitz_wire::null_word_get(nw, 0)).then(|| read_i64_le(b.col_data(0), r * 8));
        let max = (!gnitz_wire::null_word_get(nw, 1)).then(|| read_i64_le(b.col_data(1), r * 8));
        let count = read_i64_le(b.col_data(2), r * 8);
        assert!(m.insert(grp, (min, max, count)).is_none(), "one output row per group");
    }
    m
}

// Primary oracle: a multi-epoch stream of inserts, updates (retract+insert), and
// deletes over many groups. The AVI (probe-skip) path must equal a from-scratch
// group-by MIN/MAX/COUNT oracle after every epoch — weight-exact, not just row
// presence. An all-insert epoch takes the skip path, into an existing group or a
// new one; any retraction/update forces a probe.
#[test]
fn avi_skip_randomized_equivalence_mixed_churn() {
    use crate::test_rng::Rng;
    use std::collections::BTreeMap;

    let in_schema = mm_in_schema();
    let aggs = mm_aggs();
    let out_schema = out_schema_for(&in_schema, &[1u32], &aggs);

    let mut rng = Rng::new(0x00C0_FFEE_1234_5678);
    let mut live: BTreeMap<u64, (i64, i64)> = BTreeMap::new(); // pk -> (grp, val)
    let mut next_pk = 1u64;
    const N_GROUPS: u64 = 10;

    let mut epochs: Vec<Batch> = Vec::new();
    let mut oracles: Vec<MmState> = Vec::new();

    for _ep in 0..30 {
        let mut ops: Vec<(u64, i64, i64, i64)> = Vec::new();
        let n_ops = 1 + rng.gen_range(10) as usize;
        for _ in 0..n_ops {
            let choice = if live.is_empty() { 0 } else { rng.gen_range(3) };
            match choice {
                // insert a fresh pk
                0 => {
                    let grp = rng.gen_range(N_GROUPS) as i64 - 3; // spans negative groups
                    let val = rng.gen_range(120) as i64 - 60;
                    let pk = next_pk;
                    next_pk += 1;
                    live.insert(pk, (grp, val));
                    ops.push((pk, grp, val, 1));
                }
                // update an existing pk's value (retract + insert)
                1 => {
                    let keys: Vec<u64> = live.keys().copied().collect();
                    let pk = keys[rng.gen_range(keys.len() as u64) as usize];
                    let (grp, old_val) = live[&pk];
                    let new_val = rng.gen_range(120) as i64 - 60;
                    ops.push((pk, grp, old_val, -1));
                    ops.push((pk, grp, new_val, 1));
                    live.insert(pk, (grp, new_val));
                }
                // delete an existing pk
                _ => {
                    let keys: Vec<u64> = live.keys().copied().collect();
                    let pk = keys[rng.gen_range(keys.len() as u64) as usize];
                    let (grp, val) = live[&pk];
                    ops.push((pk, grp, val, -1));
                    live.remove(&pk);
                }
            }
        }
        // Consolidate to a net Z-set delta: within-epoch retract+insert of the same
        // (pk, grp, val) cancels (so it never falsely suppresses `saw_negative`).
        let mut netted: BTreeMap<(u64, i64, i64), i64> = BTreeMap::new();
        for &(pk, grp, val, w) in &ops {
            *netted.entry((pk, grp, val)).or_insert(0) += w;
        }
        let rows: Vec<(u64, i64, i64, i64)> = netted
            .into_iter()
            .filter(|&(_, w)| w != 0)
            .map(|((pk, grp, val), w)| (pk, grp, val, w))
            .collect();
        epochs.push(build_mm_delta(&rows));

        // From-scratch oracle from the live base-table state.
        let mut oracle: MmState = BTreeMap::new();
        for &(grp, val) in live.values() {
            let e = oracle.entry(grp).or_insert((None, None, 0));
            e.0 = Some(e.0.map_or(val, |m: i64| m.min(val)));
            e.1 = Some(e.1.map_or(val, |m: i64| m.max(val)));
            e.2 += 1;
        }
        oracles.push(oracle);
    }

    let avi_states = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, false);

    for ep in 0..epochs.len() {
        assert_eq!(
            readback_mm(&avi_states[ep]),
            oracles[ep],
            "epoch {ep}: AVI probe-skip path must equal the from-scratch MIN/MAX oracle",
        );
    }
}

// Explicit skip/probe cases over a single group, one assertion per epoch.
#[test]
fn avi_skip_probe_unit_cases() {
    let in_schema = mm_in_schema();
    let aggs = mm_aggs();
    let out_schema = out_schema_for(&in_schema, &[1u32], &aggs);
    let run = |epochs: &[Batch]| run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, epochs, false);

    // (a)/(b) insert-only: MAX rises above old, MIN unchanged; a later insert
    // below old leaves both extremes folded from `combine(old, pos)`.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 5, 1)]), // min=max=5
            build_mm_delta(&[(2, 0, 8, 1)]), // all-insert: max 5→8 (skip), min stays 5
            build_mm_delta(&[(3, 0, 2, 1)]), // all-insert: min 5→2 (skip), max stays 8
        ];
        let s = run(&epochs);
        assert_eq!(readback_mm(&s[0])[&0], (Some(5), Some(5), 1));
        assert_eq!(
            readback_mm(&s[1])[&0],
            (Some(5), Some(8), 2),
            "insert above raises MAX via skip"
        );
        assert_eq!(
            readback_mm(&s[2])[&0],
            (Some(2), Some(8), 3),
            "insert below lowers MIN via skip"
        );
    }

    // (c) retraction strictly inside the range: probe, extremes unchanged.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 1, 1), (2, 0, 5, 1), (3, 0, 10, 1)]), // min=1, max=10
            build_mm_delta(&[(2, 0, 5, -1)]),                             // retract the interior 5
        ];
        let s = run(&epochs);
        assert_eq!(
            readback_mm(&s[1])[&0],
            (Some(1), Some(10), 2),
            "interior retraction probes; extremes hold"
        );
    }

    // (d) retraction of an extreme with multiplicity 2 at that value: probe, the
    // second copy keeps MAX at 10.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 10, 1), (2, 0, 10, 1), (3, 0, 1, 1)]), // two rows at 10
            build_mm_delta(&[(1, 0, 10, -1)]),                             // retract one 10
        ];
        let s = run(&epochs);
        assert_eq!(
            readback_mm(&s[1])[&0],
            (Some(1), Some(10), 2),
            "duplicated extreme survives one retraction"
        );
    }

    // (e) retraction of the unique extreme: probe, MAX recedes to the next value.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 10, 1), (2, 0, 5, 1), (3, 0, 1, 1)]),
            build_mm_delta(&[(1, 0, 10, -1)]), // retract the unique max
        ];
        let s = run(&epochs);
        assert_eq!(
            readback_mm(&s[1])[&0],
            (Some(1), Some(5), 2),
            "unique extreme recedes to next via probe"
        );
    }

    // (g) group emptied to zero cardinality: the cardinality gate sheds it.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 5, 1)]),
            build_mm_delta(&[(1, 0, 5, -1)]), // retract the sole row
        ];
        let s = run(&epochs);
        assert!(
            !readback_mm(&s[1]).contains_key(&0),
            "emptied group is shed, not a zombie"
        );
    }
}

// (f) A new all-insert group whose aggregate values are all NULL: the skip path
// leaves the MIN/MAX accumulators untouched and renders NULL, while COUNT counts
// the rows.
#[test]
fn avi_skip_new_all_null_group() {
    let in_schema = mm_in_schema();
    let aggs = mm_aggs();
    let out_schema = out_schema_for(&in_schema, &[1u32], &aggs);

    // Nullable-value delta: both rows have a NULL `val`.
    let delta = {
        let s = mm_in_schema();
        let mut b = BatchBuilder::new(s);
        for pk in [1u64, 2] {
            b.begin_row(pk as u128, 1i64);
            b.put_int(0);
            b.put_null();
            b.end_row();
        }
        b.finish()
    };
    let s = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &[delta], false);
    assert_eq!(
        readback_mm(&s[0])[&0],
        (None, None, 2),
        "all-NULL insert-only group: MIN/MAX render NULL, COUNT counts rows",
    );
}

// Cap: an *existing* group receives more than `PRESTEP_CAP` all-insert rows in
// one epoch, tripping the pre-step cap so it force-probes rather than folding:
// past the cap no extreme is pre-stepped; the probe seeds them. The cap trigger —
// not `!has_old` — is what's exercised; the result must match the from-scratch
// extreme, confirming the cap is correctness-neutral.
#[test]
fn avi_skip_cap_force_probes() {
    let in_schema = mm_in_schema();
    let aggs = mm_aggs();
    let out_schema = out_schema_for(&in_schema, &[1u32], &aggs);

    // Epoch 0 creates the group (extreme 500); epoch 1 inserts 200 rows, past the cap, into
    // the now-existing group — an all-insert, has_old group past the cap.
    let seed = build_mm_delta(&[(1, 0, 500, 1)]);
    let bulk: Vec<(u64, i64, i64, i64)> = (0..200).map(|i| (i as u64 + 10, 0, i as i64 - 50, 1)).collect();
    let epochs = [seed, build_mm_delta(&bulk)];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, false);
    assert_eq!(readback_mm(&s[0])[&0], (Some(500), Some(500), 1));
    assert_eq!(
        readback_mm(&s[1])[&0],
        (Some(-50), Some(500), 201),
        "a capped all-insert existing group force-probes and still matches the from-scratch extreme",
    );
}

// (h) An integer MIN and a float MAX on distinct columns of one group in one
// insert-only epoch: both skip the probe, folding `combine(old, pos)`.
#[test]
fn avi_skip_mixed_int_min_float_max() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false), // grp
            SchemaColumn::new(TypeCode::I64, false), // ival (integer MIN, skips)
            SchemaColumn::new(TypeCode::F64, false), // fval (float MAX)
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true), // min(ival)
            SchemaColumn::new(TypeCode::F64, true), // max(fval)
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];
    let build = |rows: &[(u64, i64, i64, f64, i64)]| -> Batch {
        let mut b = BatchBuilder::new(in_schema);
        for &(pk, grp, ival, fval, w) in rows {
            b.begin_row(pk as u128, w);
            b.put_int(grp as u128);
            b.put_int(ival as u128);
            b.put_float(fval);
            b.end_row();
        }
        b.finish()
    };
    let epochs = [
        build(&[(1, 0, 5, 1.0, 1)]),
        build(&[(2, 0, 3, 2.0, 1)]), // all-insert: int MIN 5→3, float MAX 1.0→2.0
    ];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, false);
    let read = |b: &Batch| -> (Option<i64>, Option<f64>, i64) {
        assert_eq!(b.count, 1);
        let nw = b.get_null_word(0);
        let min = (!gnitz_wire::null_word_get(nw, 0)).then(|| read_i64_le(b.col_data(0), 0));
        let max = (!gnitz_wire::null_word_get(nw, 1)).then(|| f64::from_bits(read_u64_le(b.col_data(1), 0)));
        let count = read_i64_le(b.col_data(2), 0);
        (min, max, count)
    };
    assert_eq!(
        read(&s[1]),
        (Some(3), Some(2.0), 2),
        "integer MIN and float MAX both fold in one insert-only pass",
    );
}

/// Drive a float MIN/MAX reduce and check every epoch against a from-scratch
/// oracle over the accumulated rows, ordered by `total_cmp` — the order the
/// group comparator and the index's `ieee_order_bits` image both use, so ±0.0
/// stay distinct and NaN has a defined position.
fn check_float_minmax_against_oracle(val_tc: TypeCode, epochs_rows: &[Vec<(u64, i64, f64, i64)>]) {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(val_tc, false),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];
    // Float MIN/MAX keep the source type in the output.
    let out_schema = out_schema_for(&in_schema, &[1u32], &aggs);
    let build = |rows: &[(u64, i64, f64, i64)]| -> Batch {
        let mut b = BatchBuilder::new(in_schema);
        for &(pk, grp, val, w) in rows {
            b.begin_row(pk as u128, w);
            b.put_int(grp as u128);
            // Narrowed to an F32 column's width by the builder.
            b.put_float(val);
            b.end_row();
        }
        b.finish()
    };
    let epochs: Vec<Batch> = epochs_rows.iter().map(|r| build(r)).collect();
    let states = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, false);

    // The oracle folds the same value the reduce sees: an F32 source rounds
    // through f32. Both sides compare as F64 bits.
    let widen = |v: f64| if val_tc == TypeCode::F32 { v as f32 as f64 } else { v };
    let read_bits = |b: &Batch, pi: usize, i: usize| match val_tc {
        TypeCode::F32 => (f32::from_bits(gnitz_wire::read_u32_le(b.col_data(pi), i * 4)) as f64).to_bits(),
        _ => read_u64_le(b.col_data(pi), i * 8),
    };
    let mut live: std::collections::BTreeMap<u64, (i64, f64)> = std::collections::BTreeMap::new();
    for (ep, rows) in epochs_rows.iter().enumerate() {
        for &(pk, grp, val, w) in rows {
            if w > 0 {
                live.insert(pk, (grp, widen(val)));
            } else {
                live.remove(&pk);
            }
        }
        let mut oracle: std::collections::BTreeMap<i64, (u64, u64)> = std::collections::BTreeMap::new();
        for &(grp, val) in live.values() {
            let e = oracle.entry(grp).or_insert((val.to_bits(), val.to_bits()));
            if val.total_cmp(&f64::from_bits(e.0)) == std::cmp::Ordering::Less {
                e.0 = val.to_bits();
            }
            if val.total_cmp(&f64::from_bits(e.1)) == std::cmp::Ordering::Greater {
                e.1 = val.to_bits();
            }
        }
        let got: std::collections::BTreeMap<i64, (u64, u64)> = (0..states[ep].count)
            .map(|i| {
                (
                    opk_pk_i64(states[ep].get_pk_bytes(i)),
                    (read_bits(&states[ep], 0, i), read_bits(&states[ep], 1, i)),
                )
            })
            .collect();
        assert_eq!(got, oracle, "float MIN/MAX epoch {ep}");
    }
}

#[test]
fn avi_float_f64_minmax_matches_reference() {
    check_float_minmax_against_oracle(
        TypeCode::F64,
        &[
            vec![(1, 0, -0.0, 1), (2, 0, 0.0, 1), (3, 0, 5.0, 1), (4, 0, -5.0, 1)], // ±0.0 both present
            vec![(5, 0, f64::NAN, 1)],                                              // NaN is the max under total_cmp
            vec![(3, 0, 5.0, -1)],                                                  // retract a finite (always probes)
            vec![(5, 0, f64::NAN, -1)],                                             // retract the NaN
        ],
    );
}

#[test]
fn avi_float_f32_minmax_matches_reference() {
    check_float_minmax_against_oracle(
        TypeCode::F32,
        &[
            vec![(1, 0, -0.0, 1), (2, 0, 0.0, 1), (3, 0, 2.5, 1), (4, 0, -2.5, 1)],
            vec![(5, 0, f64::NAN, 1)],
            vec![(3, 0, 2.5, -1)],
        ],
    );
}

/// PK-source MAX: the aggregated column is a PK column, read through
/// `native_le_bytes` both when pre-stepping the accumulator (skip path) and when
/// the AVI is populated. `b_signed` picks a signed (sign-flipped OPK) or unsigned
/// second PK column.
fn check_pk_source_max(b_signed: bool) {
    use std::collections::BTreeMap;
    let b_tc = if b_signed { TypeCode::I64 } else { TypeCode::U64 };
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false), // a (pk, group)
            SchemaColumn::new(b_tc, false),          // b (pk, aggregated by MAX)
            SchemaColumn::new(TypeCode::I64, false), // pad (payload)
        ],
        &[0, 1],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false), // a (natural PK)
            SchemaColumn::new(b_tc, true),           // max(b) (nullable)
            SchemaColumn::new(TypeCode::I64, false), // count
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];
    let build = |rows: &[(u64, i64, i64)]| -> Batch {
        let mut bt = BatchBuilder::new(in_schema);
        for &(a, b, w) in rows {
            bt.begin_row_opk(&[a as u128, b as u128], w);
            bt.put_int(0);
            bt.end_row();
        }
        bt.finish()
    };
    let low = if b_signed { -5 } else { 5 };
    let epochs = [
        build(&[(1, 10, 1), (1, low, 1), (2, 3, 1)]), // a=1: max=10 (count 2); a=2: max=3
        build(&[(1, 20, 1)]),                         // all-insert: a=1 max 10→20 (skip)
        build(&[(1, 20, -1)]),                        // retract extreme: a=1 max 20→10 (probe)
    ];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[0u32], &aggs, &epochs, false);
    let read = |b: &Batch| -> BTreeMap<u64, (i64, i64)> {
        let mut m = BTreeMap::new();
        for r in 0..b.count {
            let a = u64::from_be_bytes(b.get_pk_bytes(r)[..8].try_into().unwrap());
            let max_b = read_i64_le(b.col_data(0), r * 8);
            let count = read_i64_le(b.col_data(1), r * 8);
            m.insert(a, (max_b, count));
        }
        m
    };
    assert_eq!(read(&s[0]), BTreeMap::from([(1, (10, 2)), (2, (3, 1))]));
    assert_eq!(
        read(&s[1])[&1],
        (20, 3),
        "insert-only PK-source MAX rises via the skip path"
    );
    assert_eq!(
        read(&s[2])[&1],
        (10, 2),
        "retract-at-extreme PK-source MAX recedes via the probe"
    );
}

#[test]
fn avi_skip_pk_source_max_signed() {
    check_pk_source_max(true);
}

#[test]
fn avi_skip_pk_source_max_unsigned() {
    check_pk_source_max(false);
}

// Global (ungrouped) MIN/MAX over the V₀ group (empty prefix): once the V₀ group
// exists, an insert-only epoch skips; a retract-at-extreme epoch probes and
// recedes.
#[test]
fn avi_skip_global_aggregate() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];
    let build = |rows: &[(u64, i64, i64)]| -> Batch {
        let mut b = BatchBuilder::new(in_schema);
        for &(pk, val, w) in rows {
            b.begin_row(pk as u128, w);
            b.put_int(val as u128);
            b.end_row();
        }
        b.finish()
    };
    let epochs = [
        build(&[(1, 5, 1)]),            // seed the V₀ group (new → probe): min=max=5
        build(&[(2, 8, 1), (3, 1, 1)]), // insert-only into existing V₀ (skip): min=1, max=8, count=3
        build(&[(2, 8, -1)]),           // retract the max (probe): max 8→5
    ];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[], &aggs, &epochs, true);
    let read = |b: &Batch| -> (Option<i64>, Option<i64>, i64) {
        assert_eq!(b.count, 1, "global aggregate is exactly one row");
        let nw = b.get_null_word(0);
        let min = (nw & 1 == 0).then(|| read_i64_le(b.col_data(0), 0));
        let max = (!gnitz_wire::null_word_get(nw, 1)).then(|| read_i64_le(b.col_data(1), 0));
        let count = read_i64_le(b.col_data(2), 0);
        (min, max, count)
    };
    assert_eq!(read(&s[0]), (Some(5), Some(5), 1), "seed global MIN/MAX");
    assert_eq!(
        read(&s[1]),
        (Some(1), Some(8), 3),
        "insert-only global MIN/MAX into existing V₀ via skip"
    );
    assert_eq!(
        read(&s[2]),
        (Some(1), Some(5), 2),
        "retract-the-max global recedes via probe"
    );
}

#[test]
fn test_reduce_output_schema_natural_pk() {
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::U64, false), // group col
            SchemaColumn::new(TypeCode::I64, false), // agg col
        ],
        &[0],
    );
    let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum }];
    let out = out_schema_for(&input, &[1], &aggs);
    // Natural PK (single U64 group col) → [U64_PK, I64_agg]
    assert_eq!(out.num_columns(), 2);
    assert_eq!(out.columns[0].type_code, TypeCode::U64);
    assert_eq!(out.columns[1].type_code, TypeCode::I64);
}

#[test]
fn test_reduce_output_schema_compound_natural_pk() {
    // Input: pk_indices = [0, 1] (compound 2×U64), payload I64.
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1],
    );
    let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AggFunc::Count }];
    let out = out_schema_for(&input, &[0, 1], &aggs);
    // 2 PK cols + 1 agg col.
    assert_eq!(out.num_columns(), 3);
    assert_eq!(out.pk_cols(), &[0, 1]);
    assert_eq!(out.columns[0].type_code, TypeCode::U64);
    assert_eq!(out.columns[1].type_code, TypeCode::U64);
    assert_eq!(out.columns[2].type_code, TypeCode::I64);
}

#[test]
fn test_reduce_output_schema_single_pk_group_by_pk() {
    // Single-PK input grouped by its PK must collapse to the single-column
    // natural-PK shape (one PK col + agg).
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let aggs = vec![AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum }];
    let out = out_schema_for(&input, &[0], &aggs);
    assert_eq!(out.num_columns(), 2);
    assert_eq!(out.pk_cols(), &[0]);
    assert_eq!(out.columns[0].type_code, TypeCode::U64);
    assert_eq!(out.columns[1].type_code, TypeCode::I64);
}

#[test]
fn test_reduce_output_schema_synthetic_pk() {
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::String, false), // group col
            SchemaColumn::new(TypeCode::I64, false),    // agg col
        ],
        &[0],
    );
    let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AggFunc::Count }];
    let out = out_schema_for(&input, &[1], &aggs);
    // Synthetic PK (STRING group col) → [U128_hash, STRING_group, I64_count]
    assert_eq!(out.num_columns(), 3);
    assert_eq!(out.columns[0].type_code, TypeCode::U128);
    assert_eq!(out.columns[1].type_code, TypeCode::String);
    assert_eq!(out.columns[2].type_code, TypeCode::I64);
}

/// Only an extreme's raw output column is nullable: over a nullable source, or
/// with no group columns.
#[test]
fn reduce_output_schema_agg_nullability_matrix() {
    for src_nullable in [false, true] {
        // Group by the payload `grp` (I64 is not a natural reduce key, so this is
        // the SyntheticFold shape).
        let input = u64pk_i64grp_i64val(src_nullable);
        for agg_op in [
            AggFunc::Count,
            AggFunc::CountNonNull,
            AggFunc::Sum,
            AggFunc::Min,
            AggFunc::Max,
        ] {
            let aggs = vec![AggDescriptor { col_idx: 2, agg_op }];
            for group_cols in [&[1u32][..], &[][..]] {
                let out = out_schema_for(&input, group_cols, &aggs);
                // Aggregates are the trailing output columns.
                let got = out.columns[out.num_columns() - 1].nullable;
                let want = match agg_op {
                    AggFunc::Count | AggFunc::CountNonNull | AggFunc::Sum => false,
                    AggFunc::Min | AggFunc::Max => src_nullable || group_cols.is_empty(),
                };
                assert_eq!(
                    got, want,
                    "{agg_op:?}: src_nullable={src_nullable}, group_cols={group_cols:?} \
                     → expected nullable={want}",
                );
            }
        }
    }
}

// ── ReducePlan::from_wire — the circuit-node trust boundary ─────────────

/// The guard that refused a REDUCE node's parameters.
fn plan_rejection(schema: &SchemaDescriptor, group: &[u32], aggs: &[AggDescriptor]) -> String {
    ReducePlan::from_wire(schema, group, aggs, false)
        .map(|_| "a plan")
        .expect_err("expected a rejection")
        .to_string()
}

/// Every derivation a reduce runs — the output key, the output schema, the
/// accumulators — indexes the fixed `[_; 65]` schema array raw, so an
/// out-of-range column has to be refused ahead of all three.
#[test]
fn reduce_column_indices_out_of_range_are_rejected() {
    let schema = make_schema_u64_i64();
    let count = |col| vec![AggDescriptor { agg_op: AggFunc::Count, col_idx: col }];
    assert_eq!(
        plan_rejection(&schema, &[200], &count(0)),
        "group key: column 200 out of range (2 cols)"
    );
    assert_eq!(
        plan_rejection(&schema, &[0], &count(200)),
        "reduce: aggregate column 200 out of range (2 cols)"
    );
}

#[test]
fn a_reduce_without_a_count_is_rejected() {
    let schema = make_schema_u64_i64();
    for agg_op in [AggFunc::Min, AggFunc::Sum] {
        assert_eq!(
            plan_rejection(&schema, &[0], &[AggDescriptor { agg_op, col_idx: 1 }]),
            "reduce: a circuit reduce needs a COUNT(*)"
        );
    }
}

/// col 0 = U64 PK and the whole group key; col 1 = the
/// aggregate column, whose type is the only thing the two tests below vary.
fn agg_over(tc: TypeCode) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[SchemaColumn::new(TypeCode::U64, false), SchemaColumn::new(tc, false)],
        &[0],
    )
}

/// A hand-built circuit bypasses the SQL binder, and the engine still refuses
/// a sum over a wide or a calendar column.
#[test]
fn a_summing_aggregate_over_a_non_scalar_column_is_rejected() {
    let aggs = [
        AggDescriptor { agg_op: AggFunc::Sum, col_idx: 1 },
        AggDescriptor::COUNT_STAR,
    ];
    for tc in [TypeCode::U128, TypeCode::String, TypeCode::Date, TypeCode::Timestamp] {
        assert_eq!(
            plan_rejection(&agg_over(tc), &[0], &aggs),
            format!("reduce: Sum is not defined over type code {tc}"),
        );
    }
}

/// The aggregates that read no value (COUNT) or select a whole row (MIN/MAX)
/// take every column type, wide ones included — the other half of the
/// eligibility rule.
#[test]
fn a_row_selecting_aggregate_takes_every_column_type() {
    for agg_op in [AggFunc::Count, AggFunc::Min, AggFunc::Max] {
        let aggs = [AggDescriptor { agg_op, col_idx: 1 }, AggDescriptor::COUNT_STAR];
        for tc in [TypeCode::I64, TypeCode::U128, TypeCode::UUID, TypeCode::String] {
            assert!(
                ReducePlan::from_wire(&agg_over(tc), &[0], &aggs, false).is_ok(),
                "{agg_op:?} over type code {tc}"
            );
        }
    }
}

/// Times `op_reduce` over a 1M-row delta per shape, against an empty and a
/// populated trace. `#[ignore]`; run release:
///   cargo test -p gnitz-store --release op_reduce_bench -- --ignored --nocapture --test-threads=1
/// `OP_REDUCE_BENCH_SHAPE=<label>` runs one shape alone, for `perf stat`.
#[test]
#[ignore]
fn op_reduce_bench() {
    const N: u64 = 1 << 20;
    let only = std::env::var("OP_REDUCE_BENCH_SHAPE").ok();
    let mix = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let grp_val = u64pk_i64grp_i64val(false);
    let single_pk = pk_payload_schema(&[TypeCode::U64]);
    let compound_pk = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let agg = |col_idx: u32, agg_op: AggFunc| [AggDescriptor { col_idx, agg_op }, AggDescriptor::COUNT_STAR];

    // `[U64 pk, I64 grp, I64 val]` rows, raw.
    let grp_rows = |salt: u64, grp: &dyn Fn(u64) -> u64| {
        let mut bb = BatchBuilder::new(grp_val);
        for i in 0..N {
            bb.begin_row(mix(i + salt * N) as u128, 1);
            bb.put_int(grp(i) as u128);
            bb.put_int(mix(i ^ salt) as i64 as u128);
            bb.end_row();
        }
        bb.finish()
    };
    // PK-sorted rows over `schema` (one or two U64 PK columns, one I64 value),
    // certified consolidated.
    let sorted_rows = |schema: SchemaDescriptor, salt: u64| {
        let mut bb = BatchBuilder::new(schema);
        for i in 0..N {
            match schema.pk_cols().len() {
                1 => bb.begin_row(i as u128, 1),
                _ => bb.begin_row_opk(&[(i / 16) as u128, (i % 16) as u128], 1),
            }
            bb.put_int((i + salt) as u128);
            bb.end_row();
        }
        let mut b = bb.finish();
        b.certify_layout(Layout::Consolidated);
        b
    };

    type Shape<'a> = (
        &'a str,
        SchemaDescriptor,
        Vec<u32>,
        [AggDescriptor; 2],
        Box<dyn Fn(u64) -> Batch + 'a>,
    );
    let shapes: Vec<Shape> = vec![
        (
            "keyed_u64_sum",
            grp_val,
            vec![1],
            agg(2, AggFunc::Sum),
            Box::new(|s| grp_rows(s, &|i| i % 65_536)),
        ),
        (
            "source_pk",
            single_pk,
            vec![0],
            agg(1, AggFunc::Sum),
            Box::new(|s| sorted_rows(single_pk, s)),
        ),
        (
            "leading_pk_col",
            compound_pk,
            vec![0],
            agg(2, AggFunc::Sum),
            Box::new(|s| sorted_rows(compound_pk, s)),
        ),
        (
            "ungrouped_sum",
            grp_val,
            vec![],
            agg(2, AggFunc::Sum),
            Box::new(|s| grp_rows(s, &|_| 0)),
        ),
        (
            "ungrouped_min",
            grp_val,
            vec![],
            agg(2, AggFunc::Min),
            Box::new(|s| grp_rows(s, &|_| 0)),
        ),
        (
            "keyed_min_8",
            grp_val,
            vec![1],
            agg(2, AggFunc::Min),
            Box::new(|s| grp_rows(s, &|i| i / 8)),
        ),
    ];
    for (label, schema, group, aggs, make) in &shapes {
        if only.as_deref().is_some_and(|o| o != *label) {
            continue;
        }
        let plan = ReducePlan::from_wire(schema, group, aggs, false).unwrap();
        let out_schema = out_schema_for(schema, group, aggs);
        let (d1, d2) = (make(1), make(2));
        let tmp = tempfile::tempdir().unwrap();

        let mut avi = plan.avi.is_some().then(|| Avi::new(schema, group, aggs, &[&d2]));
        let mut history = avi.as_mut().map(|a| a.cursor());
        let mut empty = empty_trace(out_schema);
        let t = std::time::Instant::now();
        let out = super::op_reduce::op_reduce(&d2, &mut empty, history.as_mut(), &plan);
        let cold = t.elapsed();
        std::hint::black_box(&out);

        let mut trace = scratch_table(tmp.path().to_str().unwrap(), out_schema);
        let mut avi = plan.avi.is_some().then(|| Avi::new(schema, group, aggs, &[&d1, &d2]));
        {
            let mut avi1 = plan.avi.is_some().then(|| Avi::new(schema, group, aggs, &[&d1]));
            let mut h1 = avi1.as_mut().map(|a| a.cursor());
            let out1 = super::op_reduce::op_reduce(&d1, &mut trace.open_cursor(), h1.as_mut(), &plan);
            trace.ingest_owned_batch(out1).unwrap();
        }
        let mut history = avi.as_mut().map(|a| a.cursor());
        let mut populated = trace.open_cursor();
        let t = std::time::Instant::now();
        let out = super::op_reduce::op_reduce(&d2, &mut populated, history.as_mut(), &plan);
        let warm = t.elapsed();
        std::hint::black_box(&out);
        trace.ingest_owned_batch(out).unwrap();

        println!("op_reduce {label}: empty trace {cold:?}, populated trace {warm:?}");
    }
}

/// Times `op_reduce` over a 1M-row delta against a populated trace, for SUM and
/// MIN across packed group-key shapes. `#[ignore]`; run release:
///   cargo test -p gnitz-store --release op_reduce_group_sweep_bench -- --ignored --nocapture --test-threads=1
/// `REDUCE_SWEEP=<label>` runs one alone, for `perf stat`.
#[test]
#[ignore]
fn op_reduce_group_sweep_bench() {
    const N: u64 = 1 << 20;
    let only = std::env::var("REDUCE_SWEEP").ok();
    let mix = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let shapes: [(&str, &[(TypeCode, bool)]); 6] = [
        ("i32_notnull", &[(TypeCode::I32, false)]),
        ("i32_nullable", &[(TypeCode::I32, true)]),
        ("i64_nullable", &[(TypeCode::I64, true)]),
        ("2xi32_notnull", &[(TypeCode::I32, false); 2]),
        ("2xi64_nullable", &[(TypeCode::I64, true); 2]),
        ("3xi64_nullable", &[(TypeCode::I64, true); 3]),
    ];
    for (shape, group) in shapes {
        let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
        cols.extend(group.iter().map(|&(tc, nullable)| SchemaColumn::new(tc, nullable)));
        cols.push(SchemaColumn::new(TypeCode::I64, false));
        let schema = SchemaDescriptor::new(&cols, &[0]);
        let group_cols: Vec<u32> = (1..=group.len() as u32).collect();
        let val = group.len() as u32 + 1;
        let rows = |salt: u64| {
            let mut bb = BatchBuilder::new(schema);
            for i in 0..N {
                bb.begin_row(mix(i + salt * N) as u128, 1);
                for (c, &(tc, nullable)) in group.iter().enumerate() {
                    let g = (i >> (4 * c)) % 256;
                    match nullable && i % 16 == c as u64 {
                        true => bb.put_null(),
                        false if tc == TypeCode::I32 => bb.put_int((g as i32 - 128) as u32 as u128),
                        false => bb.put_int((g as i64 - 128) as u64 as u128),
                    }
                }
                bb.put_int(mix(i ^ salt) as i64 as u128);
                bb.end_row();
            }
            bb.finish()
        };
        let (d1, d2) = (rows(1), rows(2));
        for agg_op in [AggFunc::Sum, AggFunc::Min] {
            let label = format!("{shape}_{agg_op:?}").to_lowercase();
            if only.as_deref().is_some_and(|o| o != label) {
                continue;
            }
            let aggs = [AggDescriptor { col_idx: val, agg_op }, AggDescriptor::COUNT_STAR];
            let plan = ReducePlan::from_wire(&schema, &group_cols, &aggs, false).unwrap();
            let out_schema = out_schema_for(&schema, &group_cols, &aggs);
            let tmp = tempfile::tempdir().unwrap();
            let mut trace = scratch_table(tmp.path().to_str().unwrap(), out_schema);
            {
                let mut avi1 = plan
                    .avi
                    .is_some()
                    .then(|| Avi::new(&schema, &group_cols, &aggs, &[&d1]));
                let mut h1 = avi1.as_mut().map(|a| a.cursor());
                let out1 = super::op_reduce::op_reduce(&d1, &mut trace.open_cursor(), h1.as_mut(), &plan);
                trace.ingest_owned_batch(out1).unwrap();
            }
            let mut avi = plan
                .avi
                .is_some()
                .then(|| Avi::new(&schema, &group_cols, &aggs, &[&d1, &d2]));
            let mut history = avi.as_mut().map(|a| a.cursor());
            let mut populated = trace.open_cursor();
            let t = std::time::Instant::now();
            let out = super::op_reduce::op_reduce(&d2, &mut populated, history.as_mut(), &plan);
            let warm = t.elapsed();
            std::hint::black_box(&out);
            println!("op_reduce_group_sweep {label}: populated trace {warm:?}");
        }
    }
}

/// `op_reduce` over a three-run trace_out whose two older runs lie wholly below the
/// delta's first output key: the delta touches every group of the newest run and
/// adds a new group between each pair. `#[ignore]`; run release:
///   cargo test -p gnitz-store --release op_reduce_multi_run_bench -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn op_reduce_multi_run_bench() {
    const G: u64 = 1 << 14;
    const ITERS: usize = 200;
    let schema = pk_payload_schema(&[TypeCode::U64]);
    let group = [0u32];
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum },
        AggDescriptor::COUNT_STAR,
    ];
    let plan = ReducePlan::from_wire(&schema, &group, &aggs, false).unwrap();
    let out_schema = out_schema_for(&schema, &group, &aggs);
    let rows = |keys: &[u64]| {
        let mut bb = BatchBuilder::new(schema);
        for &k in keys {
            bb.begin_row(k as u128, 1);
            bb.put_int(k as u128);
            bb.end_row();
        }
        let mut b = bb.finish();
        b.certify_layout(Layout::Consolidated);
        b
    };
    let tmp = tempfile::tempdir().unwrap();
    let mut trace = crate::test_support::scratch_table(tmp.path().to_str().unwrap(), out_schema);
    let runs: [Vec<u64>; 3] = [
        (0..2 * G).step_by(2).collect(),
        (1..2 * G).step_by(2).collect(),
        (2 * G..4 * G).step_by(2).collect(),
    ];
    for keys in &runs {
        let out = super::op_reduce::op_reduce(&rows(keys), &mut trace.open_cursor(), None, &plan);
        trace.ingest_owned_batch(out).unwrap();
    }
    let delta = rows(&(2 * G..4 * G).collect::<Vec<_>>());
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let mut instructions = 0;
    for _ in 0..ITERS {
        let mut cursor = trace.open_cursor();
        let (out, n) = counter.measure(|| super::op_reduce::op_reduce(&delta, &mut cursor, None, &plan));
        std::hint::black_box(out);
        instructions += n;
    }
    println!("op_reduce multi-run: {} instr/iter", instructions / ITERS as u64);
}
