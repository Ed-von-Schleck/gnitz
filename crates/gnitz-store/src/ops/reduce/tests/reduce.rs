//! Reduce operator tests. Imports submodule items by name; helpers live in this
//! file rather than a shared `common.rs` so the tests file stays self-contained.

use crate::expr::PkSource;
use crate::ops::op_negate;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::{Batch, BatchBuilder, Layout, ReadCursor};
use crate::test_support::{
    make_batch_raw, make_schema_i64pk_i64, make_schema_u64_i64, opk_pk_i64, pk_payload_schema, scratch_table,
    trace_cursor, u64_pk_schema,
};
use gnitz_wire::{read_i64_le, read_u64_le};

use super::super::group_key::GroupKeyCols;
use super::agg::Accumulator;
use super::avi::AviBake;
use super::emit::{emit_global_ground, emit_reduce_row};
use super::plan::{build_reduce_output_schema, ReducePlan};
use super::sort::{argsort_delta, compare_by_group_cols};
use crate::schema::ColumnLocator;
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

/// Resolve `cols` to the baked group-column locators — what `ReducePlan::build`
/// stores in `group_key.cols`.
fn locate_cols(schema: &SchemaDescriptor, cols: &[u32]) -> Vec<ColumnLocator> {
    cols.iter().map(|&c| schema.locate(c as usize)).collect()
}

/// The AVI index schema for `(schema, group cols)` — the same one the plan
/// bakes onto a compiled reduce, reached without naming the aggregate list.
fn avi_schema(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> SchemaDescriptor {
    make_bake(
        schema,
        group_by_cols,
        &[AggDescriptor { col_idx: 0, agg_op: AggFunc::Min }],
    )
    .schema
}

/// A `trace_out` cursor over an empty trace — a view's first epoch.
fn empty_trace(schema: SchemaDescriptor) -> ReadCursor {
    trace_cursor(Batch::empty_with_schema(&schema), schema)
}

/// Shim over [`super::op_reduce::op_reduce`] baking a [`ReducePlan`] per call —
/// deriving `out_key` from the input schema (exactly the one kind compile-time
/// validation admits for a given (schema, group cols)) and the history from the
/// passed cursor, so the many call sites below need not repeat it.
#[allow(clippy::too_many_arguments)]
fn op_reduce(
    delta: &Batch,
    trace_out_cursor: &mut ReadCursor,
    input_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    avi_cursor: Option<&mut ReadCursor>,
    global_ground: bool,
    i_am_owner: bool,
) -> Batch {
    let plan = make_plan(input_schema, group_by_cols, agg_descs, global_ground, i_am_owner);
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
        let mut table = scratch_table(dir.path().to_str().unwrap(), bake.schema, 0);
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

/// Bake a [`ReducePlan`] the way the compiler's `emit_reduce` does, with
/// `out_key` derived from the input schema.
fn make_plan(
    input_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    global_ground: bool,
    i_am_owner: bool,
) -> ReducePlan {
    ReducePlan::from_wire(input_schema, group_by_cols, agg_descs, global_ground, i_am_owner).unwrap()
}

/// The baked AVI of a value-indexed reduce, reached through the plan that owns
/// it — the only way production builds one.
fn make_bake(in_schema: &SchemaDescriptor, group_cols: &[u32], agg_descs: &[AggDescriptor]) -> AviBake {
    make_plan(in_schema, group_cols, agg_descs, false, false)
        .avi
        .expect("a value-indexed reduce has an AVI")
}

/// The single accumulator the plan bakes for `desc` — carrying the output
/// column locator its emission and trace read-back go through.
fn make_acc(in_schema: &SchemaDescriptor, group_cols: &[u32], desc: AggDescriptor) -> Accumulator {
    let mut accs = make_plan(in_schema, group_cols, &[desc], false, false).acc_template;
    accs.pop().unwrap()
}

/// The AVI value image of an I64 aggregate value, spelled out rather than taken
/// from the code under test: `ColumnLocator::order_bits`' signed-integer half is
/// the sign-bit flip that puts two's-complement negatives below non-negatives.
fn i64_av(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// The output schema the plan derives for `(schema, group cols, aggs)` — the
/// layout every trace `ReadCursor` below reads back through. The four
/// `test_build_reduce_output_schema_*` tests and the nullability matrix are what
/// pin that layout; these call sites consume it.
fn out_schema_for(schema: &SchemaDescriptor, group_cols: &[u32], aggs: &[AggDescriptor]) -> SchemaDescriptor {
    build_reduce_output_schema(schema, group_cols, aggs, schema.reduce_out_key(group_cols)).unwrap()
}

/// `argsort_delta`'s one contract: every group's rows land contiguously, so the
/// group walk's boundary test closes each group exactly once. `group_of` names
/// the group of a source row index. Visit *order* is deliberately unpinned — it
/// is the group key's order, which for a hashed key is the digest's.
fn assert_groups_contiguous(order: &[u32], group_of: impl Fn(u32) -> u128) {
    let mut closed: Vec<u128> = Vec::new();
    let mut open: Option<u128> = None;
    for &i in order {
        let g = group_of(i);
        if open != Some(g) {
            if let Some(prev) = open {
                closed.push(prev);
            }
            assert!(!closed.contains(&g), "group {g} is split: its rows are not contiguous");
            open = Some(g);
        }
    }
}

/// The three arms of the group sort, end to end through `op_reduce`: a
/// sign-flipped narrow route key, a 16-byte route key, and the multi-column
/// digest. Each asserts that every group is emitted exactly once with the right
/// aggregate — group *order* is the key's, which for the digest arm is not the
/// group columns'.
#[test]
fn grouped_reduce_narrow_signed_route_key() {
    // Single non-nullable I64 group column: canonical, ≤8 bytes ⇒ the `u64` arm,
    // whose route key sign-flips so −1 sorts below 0.
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

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &aggs, None, false, false);
    let by_grp: std::collections::HashMap<i64, (i64, i64)> = (0..out.count)
        .map(|r| {
            (
                read_i64_le(out.col_data(0), r * 8),
                (read_i64_le(out.col_data(1), r * 8), read_i64_le(out.col_data(2), r * 8)),
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
    // Single non-nullable UUID group column: canonical, 16 bytes ⇒ the `u128` arm.
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

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &aggs, None, false, false);
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
    // A two-column group set is not canonical ⇒ the XXH3 digest arm. Group
    // *order* is the digest's, so the assertion is per group, never positional.
    let in_schema = make_schema_u64_uuid_i64();
    let aggs = [AggDescriptor { col_idx: 0, agg_op: AggFunc::Count }];
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

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32, 2u32], &aggs, None, false, false);
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

fn make_schema_u64_f32() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F32, 0),
        ],
        &[0],
    )
}

fn make_batch_f32(schema: &SchemaDescriptor, rows: &[(u64, i64, f32)]) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_capacity(schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_bits().to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_reduce_sum_retraction() {
    use crate::schema::{type_code, SchemaColumn};

    // Input: pk(U64), grp(I64), val(I64)
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

    // Output: pk(U128), grp(I64), sum(I64), count(I64). The trailing count is the
    // cardinality companion every all-linear reduce now carries; op_reduce gates
    // emission on it (and asserts its presence).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

    // Empty trace_out
    let mut to_ch = empty_trace(out_schema);

    // Tick 1: insert 3 rows in group 10: val=100, val=200, val=300
    let delta1 = {
        let mut b = Batch::with_capacity(&in_schema, 3);
        for (pk, val) in [(1u64, 100i64), (2, 200), (3, 300)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &10i64.to_le_bytes()); // grp=10
            b.extend_col(1, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let aggs = sum_count_aggs(2);

    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &[1u32], &aggs, None, false, false);
    // SUM of (100+200+300) = 600
    assert_eq!(out1.count, 1);
    let sum1 = read_i64_le(out1.col_data(1), 0);
    assert_eq!(sum1, 600);

    // Tick 2: retract pk=2 (val=200) → SUM should go from 600 to 400
    // Need trace_out with previous aggregate
    let mut to_ch2 = trace_cursor(out1, out_schema);

    let delta2 = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(2u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &200i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &[1u32], &aggs, None, false, false);
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
    use crate::schema::{type_code, SchemaColumn};

    // Input: pk(U64), grp(I64), val(I64). Output (synthetic GROUP BY grp):
    // _group_pk(U128), grp(I64), sum(I64 nullable), count(I64 companion).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = sum_count_aggs(2);

    let reduce = |delta: &Batch, to: &mut crate::storage::ReadCursor| {
        op_reduce(delta, to, &in_schema, &[1u32], &aggs, None, false, false)
    };

    // Tick 1: insert (pk1, grp=10, val=5) → group exists (sum=5, count=1).
    let mut to_ch = empty_trace(out_schema);
    let row = |pk: u128, w: i64| {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp=10
        b.extend_col(1, &5i64.to_le_bytes()); // val=5
        b.count += 1;
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
        read_i64_le(out2.col_data(1), 0),
        5,
        "retraction re-emits the stored SUM=5"
    );
}

/// All-linear gate: inserting the *first* row of a new group whose aggregated
/// column is NULL must surface the group (SQL: `SUM = NULL`), not drop it. With
/// the appended Count companion the null-blind row count is 1, so the gate emits
/// the group; the SUM/CountNonNull accumulators stay untouched, so the raw SUM
/// bit is NULL and the NullfillSum finalize renders SUM = NULL.
#[test]
fn linear_sum_only_new_all_null_group_present() {
    use crate::schema::{type_code, SchemaColumn};
    use gnitz_expr::{CmpOp, IntArithOp, LogicalInstr, LogicalProgram, Output, Reg, Sink};

    // Input: pk(U64), grp(I64), val(I64 nullable).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    // Raw reduce output: _group_pk | grp | sum (nullable) | cnn | count.
    // agg order mirrors agg_descs = [Sum, CountNonNull, Count] — the nullable-SUM
    // NullfillSum pair plus the trailing appended cardinality companion.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // _group_pk
            SchemaColumn::new(type_code::I64, 0),  // grp
            SchemaColumn::new(type_code::I64, 1),  // sum (nullable)
            SchemaColumn::new(type_code::I64, 0),  // cnn
            SchemaColumn::new(type_code::I64, 0),  // count (companion)
        ],
        &[0],
    );
    // Finalize projects [pk, grp, sum]; cnn and the count companion are stripped.
    let fin_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1), // sum (nullable)
        ],
        &[0],
    );
    // NullfillSum: grp → fin payload 0; sum(col 2) / (cnn(col 3) != 0) → fin
    // payload 1 (div-by-zero marks SUM NULL when the non-null count is 0).
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 3 }, // r0 = cnn
        LogicalInstr::LoadConst { val: 0 },
        LogicalInstr::Cmp { op: CmpOp::Ne, a: Reg(0), b: Reg(1) }, // r2 = (cnn != 0)
        LogicalInstr::LoadColInt { col: 2 },                       // r3 = sum
        LogicalInstr::IntArith {
            op: IntArithOp::Div,
            a: Reg(3),
            b: Reg(2),
        }, // r4 = sum / gate
    ];
    // fin payload 0 = grp; fin payload 1 = the gated sum.
    let sinks = vec![Sink::Col(1), Sink::Reg(Reg(4))];
    let fin_func = crate::expr::MapPlan::from_map(
        LogicalProgram::new(instrs, Output::Slots(sinks), vec![]),
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
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ];

    // New group g=7 whose only row has val=NULL.
    let mut to_ch = empty_trace(out_schema);
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val (payload idx 1) NULL
        b.extend_col(0, &7i64.to_le_bytes()); // grp=7
        b.extend_col(1, &0i64.to_le_bytes()); // val NULL (placeholder)
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let raw = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &aggs, None, false, false);
    let fin = fin_func.evaluate_map_batch(&raw);
    assert_eq!(
        raw.count, 1,
        "new all-NULL group is present (cardinality 1), not dropped"
    );
    assert_eq!(fin.count, 1);
    assert_eq!(fin.get_weight(0), 1, "one +1 row");
    assert_eq!(read_i64_le(fin.col_data(0), 0), 7, "grp=7");
    assert_eq!(
        (fin.get_null_word(0) >> 1) & 1,
        1,
        "SUM of an all-NULL group renders as NULL",
    );
}

/// All-linear gate, reuse path: a COUNT(*)-only group driven to empty is
/// eliminated. The user COUNT(*) *is* the cardinality signal (no companion is
/// appended), so the gate suppresses the +1 once the count nets to 0.
#[test]
fn count_star_only_emptied_group_eliminated() {
    use crate::schema::{type_code, SchemaColumn};

    // Input: pk(U64), grp(I64). Output (synthetic GROUP BY grp):
    // _group_pk(U128), grp(I64), count(I64).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [AggDescriptor { col_idx: 0, agg_op: AggFunc::Count }];

    let reduce = |delta: &Batch, to: &mut crate::storage::ReadCursor| {
        op_reduce(delta, to, &in_schema, &[1u32], &aggs, None, false, false)
    };

    let row = |pk: u128, w: i64| {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp=10
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // Tick 1: insert one row → count=1.
    let mut to_ch = empty_trace(out_schema);
    let out1 = reduce(&row(1, 1), &mut to_ch);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(1), 0), 1, "count=1");

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
    use crate::schema::{type_code, SchemaColumn};
    use gnitz_expr::{CmpOp, IntArithOp, LogicalInstr, LogicalProgram, Output, Reg, Sink};

    // Input: pk(U64), grp(I64), val(I64, NULLABLE).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1), // nullable source
        ],
        &[0],
    );

    // Raw reduce output: synthetic U128 _group_pk | grp | count | sum | cnn.
    // The aggregate column order mirrors agg_descs = [Count, Sum, CountNonNull].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // _group_pk
            SchemaColumn::new(type_code::I64, 0),  // grp
            SchemaColumn::new(type_code::I64, 0),  // count
            SchemaColumn::new(type_code::I64, 1),  // sum (nullable)
            SchemaColumn::new(type_code::I64, 0),  // cnn (companion)
        ],
        &[0],
    );

    // Finalized output projects [pk, grp, count, sum] — companion stripped.
    let fin_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // _group_pk
            SchemaColumn::new(type_code::I64, 0),  // grp
            SchemaColumn::new(type_code::I64, 0),  // count
            SchemaColumn::new(type_code::I64, 1),  // sum (nullable)
        ],
        &[0],
    );

    // Finalize program (full reduce-output column indices; resolved below):
    //   copy grp(col 1) → fin payload 0, count(col 2) → fin payload 1,
    //   emit sum-gate = sum(col 3) / (cnn(col 4) != 0) → fin payload 2.
    // div-by-zero (cnn == 0) marks the SUM NULL; div-by-1 (cnn > 0) is exact.
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 4 },                       // r0 = cnn (col 4)
        LogicalInstr::LoadConst { val: 0 },                        // r1 = 0
        LogicalInstr::Cmp { op: CmpOp::Ne, a: Reg(0), b: Reg(1) }, // r2 = (cnn != 0) → 1/0
        LogicalInstr::LoadColInt { col: 3 },                       // r3 = sum (col 3)
        LogicalInstr::IntArith {
            op: IntArithOp::Div,
            a: Reg(3),
            b: Reg(2),
        }, // r4 = sum / gate (NULL when gate == 0)
    ];
    // fin payload 0 = grp, 1 = count, 2 = the gated sum.
    let sinks = vec![Sink::Col(1), Sink::Col(2), Sink::Reg(Reg(4))];
    let fin_func = crate::expr::MapPlan::from_map(
        LogicalProgram::new(instrs, Output::Slots(sinks), vec![]),
        &out_schema,
        &fin_schema,
        PkSource::Inherit,
    )
    .unwrap();

    let aggs = [
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggFunc::CountNonNull,
        },
    ];

    // Tick 1: insert (pk1, grp=10, val=5) and (pk2, grp=10, val=NULL).
    let mut to_ch = empty_trace(out_schema);
    let delta1 = {
        let mut b = Batch::with_capacity(&in_schema, 2);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp
        b.extend_col(1, &5i64.to_le_bytes()); // val = 5
        b.count += 1;
        // val is payload col 1 → its null bit is bit 1.
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp
        b.extend_col(1, &0i64.to_le_bytes()); // val = NULL (placeholder bytes)
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let raw1 = op_reduce(&delta1, &mut to_ch, &in_schema, &[1u32], &aggs, None, false, false);
    let fin1 = fin_func.evaluate_map_batch(&raw1);
    // One group: count=2, sum=5 (non-null while a contributor remains), cnn=1.
    assert_eq!(raw1.count, 1);
    assert_eq!(fin1.count, 1);
    assert_eq!(fin1.get_weight(0), 1);
    assert_eq!(read_i64_le(fin1.col_data(1), 0), 2, "count=2");
    assert_eq!(read_i64_le(fin1.col_data(2), 0), 5, "sum=5");
    assert_eq!(
        (fin1.get_null_word(0) >> 2) & 1,
        0,
        "sum non-null while a contributor remains"
    );

    // Tick 2: retract (pk1, val=5). The group survives via (pk2, NULL): count
    // drops to 1, the last non-null contributor is gone → SUM must become NULL.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let delta2 = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let _raw2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &[1u32], &aggs, None, false, false);
    let fin2 = fin_func.evaluate_map_batch(&_raw2);
    // Retract the old aggregate (w=-1) and insert the new one (w=+1).
    assert_eq!(fin2.count, 2);

    // The insert row (weight +1) carries the surviving group's new state.
    let insert_row = (0..fin2.count)
        .find(|&r| fin2.get_weight(r) == 1)
        .expect("an insert row");
    assert_eq!(
        read_i64_le(fin2.col_data(1), insert_row * 8),
        1,
        "COUNT(*) survives at 1",
    );
    assert_eq!(
        (fin2.get_null_word(insert_row) >> 2) & 1,
        1,
        "SUM becomes NULL once its last non-null contributor is retracted",
    );

    // The retraction row re-emits the prior (non-null) SUM, cancelling the old
    // output byte-for-byte — the fix introduces no ghost.
    let retract_row = (0..fin2.count)
        .find(|&r| fin2.get_weight(r) == -1)
        .expect("a retract row");
    assert_eq!(
        (fin2.get_null_word(retract_row) >> 2) & 1,
        0,
        "retracted SUM was non-null"
    );
    assert_eq!(read_i64_le(fin2.col_data(2), retract_row * 8), 5, "retracted SUM = 5",);
}

/// A group whose MIN is NULL (all contributors NULL) is kept alive by COUNT(*).
/// When a non-NULL row later joins it, the group's old (NULL-MIN) output row must
/// be retracted *as NULL* to cancel the tick-1 row byte-for-byte. MIN is a Direct
/// passthrough, so the raw null bit is user-visible — the primary user-facing bug.
#[test]
fn null_min_retraction_re_emits_null() {
    // in: pk(U64), grp(I64), val(I64 nullable); out (payload GROUP BY grp):
    // pk(U128), grp(I64), count(I64), min(I64 nullable). min is payload index 2.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Count },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
    ];
    let min_null_bit = 1u64 << 2;

    // Tick 1: (pk=1, grp=10, val=NULL) → COUNT keeps the group alive, MIN=NULL.
    let mut to_ch = empty_trace(out_schema);
    let delta1 = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val (payload idx 1) NULL
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &[1u32], &aggs, None, false, false);
    assert_eq!(out1.count, 1);
    assert!(
        out1.as_mem_batch().get_null_word(0) & min_null_bit != 0,
        "tick1 MIN must be NULL"
    );

    // Tick 2: (pk=2, grp=10, val=7) → MIN=7, retracts the NULL row.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let delta2 = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &7i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &[1u32], &aggs, None, false, false);
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

/// A linear SUM whose group stays all-NULL across a fold must keep the new
/// output row's raw SUM bit NULL: folding a previously-NULL SUM's zero bytes
/// would saturate `has_value` and decode NULL as 0. Pins the linear-fold null
/// gate that skips a NULL old aggregate. This is the raw SUM bit only — a
/// nullable SUM's user-visible null-ness rides the NullfillSum companion, which
/// `test_reduce_nullable_sum_retraction_becomes_null` covers.
#[test]
fn null_sum_fold_stays_null() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Count },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
    ];
    let sum_null_bit = 1u64 << 2;
    let null_row = |pk: u128| {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val NULL
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = empty_trace(out_schema);
    let out1 = op_reduce(&null_row(1), &mut to_ch, &in_schema, &[1u32], &aggs, None, false, false);
    assert!(
        out1.as_mem_batch().get_null_word(0) & sum_null_bit != 0,
        "tick1 SUM NULL"
    );

    let mut to_ch2 = trace_cursor(out1, out_schema);
    let out2 = op_reduce(
        &null_row(2),
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &aggs,
        None,
        false,
        false,
    );
    let mb2 = out2.as_mem_batch();
    let new_row = (0..out2.count).find(|&i| out2.get_weight(i) > 0).expect("insert row");
    assert!(
        mb2.get_null_word(new_row) & sum_null_bit != 0,
        "SUM of a still-all-NULL group must stay NULL (null_word={:#x})",
        mb2.get_null_word(new_row)
    );
}

/// Reduce-of-trace over a wide PK (3×U64, stride 24, GROUP BY the full PK).
/// The retraction read seeks `trace_out` by the group's PK bytes; the u128
/// `seek` cannot carry a stride-24 key.
#[test]
fn reduce_trace_seek_wide_pk() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::test_support::{opk_pk, wide_pk_3xu64_schema};

    // Wide PK: 3×U64 (stride 24) + I64 val. GROUP BY the full PK.
    let in_schema = wide_pk_3xu64_schema();
    // Output: natural wide PK (3×U64) + SUM(I64, nullable) + count companion.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
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
        let mut b = Batch::with_capacity(&in_schema, 2);
        for val in [100i64, 200] {
            b.extend_pk_bytes(&pk(7, 7, 7));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &group_by, &aggs, None, false, false);
    assert_eq!(out1.count, 1, "one group");
    assert_eq!(out1.get_pk_bytes(0), &pk(7, 7, 7)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 300);

    // Tick 2: retract the val=200 row. SUM 300 → 100; reads the prior aggregate
    // out of trace_out by PK bytes.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let delta2 = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk_bytes(&pk(7, 7, 7));
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &200i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &group_by, &aggs, None, false, false);
    // Retraction of old SUM (300, w=-1) then insert of new SUM (100, w=+1).
    assert_eq!(
        out2.count, 2,
        "wide-PK retraction must read trace_out and emit retract+insert"
    );
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(out2.get_pk_bytes(0), &pk(7, 7, 7)[..]);
    assert_eq!(read_i64_le(out2.col_data(0), 0), 300, "retracted old SUM");
    assert_eq!(out2.get_weight(1), 1);
    assert_eq!(read_i64_le(out2.col_data(0), 8), 100, "new SUM");
}

/// Incremental REDUCE over a narrow COMPOUND PK (2×U64, stride 16), GROUP BY
/// the full PK, SUM. The tick-2 retraction seeks `trace_out` by the group's
/// PK. A compound key's raw-u128 order is last-column-major, so the seek must
/// go by bytes (storage order) to land on the group and retract the old SUM.
#[test]
fn reduce_trace_seek_compound_pk() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::test_support::opk_pk;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    assert!(in_schema.pk_indices().len() > 1, "test invariant: compound PK");
    assert!(in_schema.pk_stride() <= 16, "test invariant: stride 16 is narrow");

    let pk = |a: u64, b: u64| opk_pk(&in_schema, &[a as u128, b as u128]);
    let aggs = sum_count_aggs(2);
    let group_by = [0u32, 1];

    // Tick 1: insert (1,5)->100 and (2,3)->200 (two distinct groups).
    let mut to_ch = empty_trace(out_schema);
    let delta1 = {
        let mut b = Batch::with_capacity(&in_schema, 2);
        for &(a, c, val) in &[(1u64, 5u64, 100i64), (2, 3, 200)] {
            b.extend_pk_bytes(&pk(a, c));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &group_by, &aggs, None, false, false);
    assert_eq!(out1.count, 2, "two groups");
    assert_eq!(out1.get_pk_bytes(0), &pk(1, 5)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 100);
    assert_eq!(out1.get_pk_bytes(1), &pk(2, 3)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 8), 200);

    // Tick 2: insert (2,3)->50. SUM for (2,3) goes 200 → 250: retract 200,
    // insert 250. Group (1,5) is untouched.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let delta2 = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk_bytes(&pk(2, 3));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &50i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &group_by, &aggs, None, false, false);
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
    use crate::schema::{type_code, SchemaColumn};

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
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
        let mut b = Batch::with_capacity(&in_schema, 2);
        for &(k, val) in &[(-1i64, 200i64), (2, 100)] {
            b.extend_pk_opk(&[(k as u64) as u128]);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = op_reduce(&delta1, &mut to_ch, &in_schema, &group_by, &aggs, None, false, false);
    assert_eq!(out1.count, 2, "two groups (-1 sorts before 2)");
    assert_eq!(opk_pk_i64(out1.get_pk_bytes(0)), -1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 200);

    // Tick 2: insert key=-1 -> 50. SUM goes 200 → 250: retract + insert.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let delta2 = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk_opk(&[((-1i64) as u64) as u128]);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &50i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out2 = op_reduce(&delta2, &mut to_ch2, &in_schema, &group_by, &aggs, None, false, false);
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
    use crate::schema::type_code;

    // Input: pk(U64), val(I64)
    let in_schema = make_schema_u64_i64();

    // Output: the input's PK region verbatim, then count(I64).
    let out_schema = SchemaDescriptor::new(
        &[
            crate::schema::SchemaColumn::new(type_code::U64, 0),
            crate::schema::SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // 3 rows: pk=1,2,3 all GROUP BY pk (single group using pk as group)
    let delta = make_batch(&in_schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

    let agg = AggDescriptor { col_idx: 0, agg_op: AggFunc::Count };

    // GROUP BY pk → each row is its own group
    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false, false);
    // Each pk forms its own group, COUNT=1 for each
    assert_eq!(out.count, 3);
    for i in 0..3 {
        let count = read_i64_le(out.col_data(0), i * 8);
        assert_eq!(count, 1, "each single-row group has count=1");
    }
}

#[test]
fn test_argsort_delta_f32_group() {
    // F32 is not `is_pk_eligible`, so the group key is the XXH3 fold: rows of one
    // value must still land contiguously, at whatever position the digest puts them.
    let schema = make_schema_u64_f32();
    let batch = make_batch_f32(
        &schema,
        &[
            (1, 1, 2.0f32),
            (2, 1, -1.0f32),
            (3, 1, 0.5f32),
            (4, 1, -1.0f32),
            (5, 1, 2.0f32),
        ],
    );
    let mb = batch.as_mem_batch();
    let indices = argsort_delta(&mb, &GroupKeyCols::new(&schema, &[1]));
    assert_eq!(indices.len(), 5);
    assert_groups_contiguous(&indices, |i| {
        let ptr = mb.get_col_ptr(i as usize, 0, 4);
        u32::from_le_bytes(ptr.try_into().unwrap()) as u128
    });
}

#[test]
fn test_compare_by_group_cols_f32_negative() {
    let schema = make_schema_u64_f32();
    let batch = make_batch_f32(&schema, &[(1, 1, -5.0f32), (2, 1, 3.0f32)]);
    let mb = batch.as_mem_batch();
    let descs_v = locate_cols(&schema, &[1]);
    let descs = &descs_v[..];
    let ord = compare_by_group_cols(&mb, 0, &mb, 1, descs);
    assert_eq!(ord, std::cmp::Ordering::Less);
    let ord2 = compare_by_group_cols(&mb, 1, &mb, 0, descs);
    assert_eq!(ord2, std::cmp::Ordering::Greater);
}

#[test]
fn test_group_key_f32() {
    let schema = make_schema_u64_f32();
    let batch = make_batch_f32(&schema, &[(1, 1, 1.5f32), (2, 1, 2.5f32)]);
    let mb = batch.as_mem_batch();
    let key0 = GroupKeyCols::new(&schema, &[1]).key_row(&mb, 0);
    let key1 = GroupKeyCols::new(&schema, &[1]).key_row(&mb, 1);
    assert_ne!(key0, key1, "different F32 values must produce different group keys");
}

// -----------------------------------------------------------------------
// Fix 1: Schema-agnostic reads for sub-8-byte columns
// -----------------------------------------------------------------------

fn make_schema_with_type(tc: u8) -> SchemaDescriptor {
    SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0])
}

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
    let in_schema = make_schema_with_type(type_code::I32);

    // Output: the input's PK region, sum(I64), count(I64) — trailing companion.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // 3 rows with I32 values, group by PK
    let delta = make_batch_typed(&in_schema, &[(1, 1, 100), (2, 1, 200), (3, 1, -50)]);

    let aggs = sum_count_aggs(1);

    // GROUP BY pk → each row is its own group
    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false, false);
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
    let in_schema = make_schema_with_type(type_code::F32);

    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // Use a 2-col input schema: pk(U64), val(F32), GROUP BY pk. Rows in
    // (PK, payload) order so the consolidated flag the helper stamps is honest.
    let delta = make_batch_f32(&in_schema, &[(1, 1, -1.0f32), (1, 1, 3.5f32), (1, 1, 7.0f32)]);

    let agg = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };

    // GROUP BY pk → all 3 rows in same group
    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false, false);
    assert_eq!(out.count, 1);
    // MIN should be -1.0 stored as f64 bits
    let bits = u64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
    let min_val = f64::from_bits(bits);
    assert_eq!(min_val, -1.0f64, "MIN of F32 {{3.5, -1.0, 7.0}} should be -1.0");
}

#[test]
fn test_reduce_max_i16() {
    let in_schema = make_schema_with_type(type_code::I16);

    let agg = AggDescriptor { col_idx: 1, agg_op: AggFunc::Max };
    // MIN/MAX select an existing row, so the output column keeps the I16 source
    // width — the read below is 2 bytes, not 8.
    let out_schema = out_schema_for(&in_schema, &[0u32], std::slice::from_ref(&agg));
    let mut to_ch = empty_trace(out_schema);

    // 3 rows with I16 values, all same PK, in (PK, payload) order so the
    // consolidated flag the helper stamps is honest.
    let delta = make_batch_typed(&in_schema, &[(1, 1, -100), (1, 1, 50), (1, 1, 200)]);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false, false);
    assert_eq!(out.count, 1);
    let max_val = i16::from_le_bytes(out.col_data(0)[0..2].try_into().unwrap());
    assert_eq!(max_val, 200, "MAX of I16 {{-100, 200, 50}} should be 200");
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// UUID non-PK GROUP BY correctness
// -----------------------------------------------------------------------

/// Schema: pk(U64) + uuid_payload(UUID). UUID is at payload index 0.
fn make_schema_u64_pk_uuid_payload() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::UUID, 0),
        ],
        &[0],
    )
}

/// Schema: pk(U64) + uuid_col(UUID) + i64_col(I64).
fn make_schema_u64_uuid_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::UUID, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn build_batch_u64_uuid(schema: &SchemaDescriptor, rows: &[(u64, u128)]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, uuid) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &uuid.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

fn build_batch_u64_uuid_i64(schema: &SchemaDescriptor, rows: &[(u64, u128, i64)]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, uuid, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &uuid.to_le_bytes());
        b.extend_col(1, &val.to_le_bytes());
        b.count += 1;
    }
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
    let out1 = op_reduce(&t1, &mut trace, &in_schema, &[2u32], &aggs, None, false, false);
    assert_eq!(out1.count, 1);
    assert_eq!((uuid_at(&out1, 0, 2), uuid_at(&out1, 0, 3)), (lo, hi));

    let t2 = op_negate(build_batch_u64_uuid_i64(&in_schema, &[(2, hi, 7), (3, lo, 7)]));
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
        false,
    );
    let new_row = (0..out2.count).find(|&i| out2.get_weight(i) > 0).expect("new row");
    assert_eq!((uuid_at(&out2, new_row, 2), uuid_at(&out2, new_row, 3)), (mid, mid));
}

#[test]
fn test_compare_by_group_cols_uuid_non_pk() {
    // UUID non-PK column used as GROUP BY column. Before the fix, compare_by_group_cols
    // falls to the else branch with cs=16, panicking on a_buf[..16] (buf is [u8; 8]).
    let schema = make_schema_u64_pk_uuid_payload();
    let uuid_lo: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0001u128;
    let uuid_hi: u128 = 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFFu128;
    let batch = build_batch_u64_uuid(&schema, &[(1, uuid_lo), (2, uuid_hi)]);
    let mb = batch.as_mem_batch();
    let descs_v = locate_cols(&schema, &[1]);
    let descs = &descs_v[..];

    // uuid_lo < uuid_hi (compare by the 128-bit value)
    let ord = compare_by_group_cols(&mb, 0, &mb, 1, descs);
    assert_eq!(
        ord,
        std::cmp::Ordering::Less,
        "uuid_lo row must compare less than uuid_hi row"
    );

    let ord2 = compare_by_group_cols(&mb, 1, &mb, 0, descs);
    assert_eq!(
        ord2,
        std::cmp::Ordering::Greater,
        "uuid_hi row must compare greater than uuid_lo row"
    );

    let ord3 = compare_by_group_cols(&mb, 0, &mb, 0, descs);
    assert_eq!(ord3, std::cmp::Ordering::Equal, "same row must compare equal to itself");
}

#[test]
fn test_argsort_delta_uuid_group() {
    // A non-nullable UUID group column is a canonical key, so it takes the
    // `u128` route-key arm — order-preserving, so the sort is by UUID value.
    let schema = make_schema_u64_pk_uuid_payload();
    let uuid_a: u128 = 0x1000_0000_0000_0000_0000_0000_0000_0001u128;
    let uuid_b: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0002u128;
    // uuid_b < uuid_a (lower high byte)
    let batch = build_batch_u64_uuid(&schema, &[(1, uuid_a), (2, uuid_b)]);
    let indices = argsort_delta(&batch.as_mem_batch(), &GroupKeyCols::new(&schema, &[1]));
    assert_eq!(indices.len(), 2);
    // Row with uuid_b (row 1) should sort before row with uuid_a (row 0)
    assert_eq!(indices[0], 1, "uuid_b (smaller) must sort first");
    assert_eq!(indices[1], 0, "uuid_a (larger) must sort second");
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
    let key0 = GroupKeyCols::new(&schema, &[1, 2]).key_row(&mb, 0); // uuid_a, 42
    let key1 = GroupKeyCols::new(&schema, &[1, 2]).key_row(&mb, 1); // uuid_b, 42
    let key2 = GroupKeyCols::new(&schema, &[1, 2]).key_row(&mb, 2); // uuid_a, 43
    let key0b = GroupKeyCols::new(&schema, &[1, 2]).key_row(&mb, 0); // same as key0

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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Schema: I64 other (col 0) | U64 pk (col 1). pk_index = 1.
fn make_schema_pk1_i64_u64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[1],
    )
}

/// Build a 2-col batch (pk, other) with explicit pk values and `other` payload.
/// Works for either pk_index=0 or pk_index=1 since extend_col(pi, ..) addresses
/// the dense payload region — the non-PK column always lives at payload index 0.
fn build_pk_other(schema: &SchemaDescriptor, rows: &[(u64, i64)]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, other) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &other.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_group_key_includes_pk_pki0() {
    // GROUP BY [pk, other] with pk_index=0: hash loop must dispatch via
    // is_pk_col, not call payload_idx(0, 0) and underflow.
    let schema = make_schema_pk0_u64_i64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();

    let k_pk10_v100 = GroupKeyCols::new(&schema, &[0, 1]).key_row(&mb, 0);
    let k_pk20_v100 = GroupKeyCols::new(&schema, &[0, 1]).key_row(&mb, 1);
    let k_pk10_v200 = GroupKeyCols::new(&schema, &[0, 1]).key_row(&mb, 2);
    let k_pk10_v100_again = GroupKeyCols::new(&schema, &[0, 1]).key_row(&mb, 0);

    assert_ne!(k_pk10_v100, k_pk20_v100, "different PKs, same other → distinct keys");
    assert_ne!(k_pk10_v100, k_pk10_v200, "same PK, different other → distinct keys");
    assert_eq!(k_pk10_v100, k_pk10_v100_again, "same row → same key");
}

#[test]
fn test_group_key_includes_pk_pki1() {
    // GROUP BY [other, pk] with pk_index=1: previously read the wrong
    // payload column when c_idx == pki for non-zero pk_index.
    let schema = make_schema_pk1_i64_u64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();

    // group_by [col 0 = other, col 1 = pk]
    let k_pk10_v100 = GroupKeyCols::new(&schema, &[0, 1]).key_row(&mb, 0);
    let k_pk20_v100 = GroupKeyCols::new(&schema, &[0, 1]).key_row(&mb, 1);
    let k_pk10_v200 = GroupKeyCols::new(&schema, &[0, 1]).key_row(&mb, 2);

    assert_ne!(k_pk10_v100, k_pk20_v100);
    assert_ne!(k_pk10_v100, k_pk10_v200);
}

#[test]
fn test_compare_by_group_cols_includes_pk() {
    // Sort/compare path must dispatch on the PK sentinel rather than
    // dereferencing a fake pi for the PK column.
    let schema = make_schema_pk0_u64_i64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100)]);
    let mb = batch.as_mem_batch();

    let descs_v = locate_cols(&schema, &[0, 1]);
    let descs = &descs_v[..];
    // First locator covers PK — must resolve to the Pk variant.
    assert!(matches!(descs[0], ColumnLocator::Pk { .. }));

    assert_eq!(compare_by_group_cols(&mb, 0, &mb, 1, descs), std::cmp::Ordering::Less);
    assert_eq!(
        compare_by_group_cols(&mb, 1, &mb, 0, descs),
        std::cmp::Ordering::Greater
    );
    assert_eq!(compare_by_group_cols(&mb, 0, &mb, 0, descs), std::cmp::Ordering::Equal);
}

#[test]
fn test_argsort_delta_pk_in_group() {
    // A multi-column group set containing the PK takes the hashed arm, whose
    // per-column fold must dispatch on the PK sentinel rather than a fake
    // payload index. Each (pk, other) pair is a group; the pairs must not split.
    let schema = make_schema_pk0_u64_i64();
    let batch = build_pk_other(&schema, &[(20, 100), (10, 200), (10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();
    let indices = argsort_delta(&mb, &GroupKeyCols::new(&schema, &[0, 1]));
    assert_eq!(indices.len(), 5);
    assert_groups_contiguous(&indices, |i| {
        let pk = gnitz_wire::widen_pk_be(mb.get_pk_bytes(i as usize));
        let other = i64::from_le_bytes(mb.get_col_ptr(i as usize, 0, 8).try_into().unwrap());
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
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        let null_word: u64 = if val.is_none() { 1 } else { 0 };
        b.extend_null_bmp(&null_word.to_le_bytes());
        // Nulls store as zero bytes — same byte pattern as integer 0.
        b.extend_col(0, &val.unwrap_or(0).to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_group_key_null_distinct_from_zero() {
    let schema = u64_pk_schema(SchemaColumn::new(type_code::I64, 1));
    let batch = build_pk_null_i64(&schema, &[(1, None), (2, Some(0)), (3, Some(7)), (4, None)]);
    let mb = batch.as_mem_batch();

    let k_null = GroupKeyCols::new(&schema, &[1]).key_row(&mb, 0);
    let k_zero = GroupKeyCols::new(&schema, &[1]).key_row(&mb, 1);
    let k_seven = GroupKeyCols::new(&schema, &[1]).key_row(&mb, 2);
    let k_null2 = GroupKeyCols::new(&schema, &[1]).key_row(&mb, 3);

    assert_ne!(k_null, k_zero, "NULL must form a distinct group from 0");
    assert_ne!(k_null, k_seven);
    assert_ne!(k_zero, k_seven);
    assert_eq!(k_null, k_null2, "two NULL rows must collapse into the same group");
}

#[test]
fn test_compare_by_group_cols_nulls_first() {
    let schema = u64_pk_schema(SchemaColumn::new(type_code::I64, 1));
    let batch = build_pk_null_i64(&schema, &[(1, Some(7)), (2, None), (3, None)]);
    let mb = batch.as_mem_batch();
    let descs_v = locate_cols(&schema, &[1]);
    let descs = &descs_v[..];

    // NULL < 7 (NULLS FIRST)
    assert_eq!(compare_by_group_cols(&mb, 1, &mb, 0, descs), std::cmp::Ordering::Less);
    assert_eq!(
        compare_by_group_cols(&mb, 0, &mb, 1, descs),
        std::cmp::Ordering::Greater
    );
    // NULL == NULL → equal (same group)
    assert_eq!(compare_by_group_cols(&mb, 1, &mb, 2, descs), std::cmp::Ordering::Equal);
}

#[test]
fn test_argsort_delta_nullable_group_col() {
    // A nullable group column is not canonical, so the key is the fold, which
    // streams a null marker: NULL is one group, distinct from the integer 0 it
    // shares its stored bytes with, and its rows must be adjacent.
    let schema = u64_pk_schema(SchemaColumn::new(type_code::I64, 1));
    let batch = build_pk_null_i64(&schema, &[(1, Some(0)), (2, None), (3, Some(5)), (4, None)]);
    let mb = batch.as_mem_batch();
    let indices = argsort_delta(&mb, &GroupKeyCols::new(&schema, &[1]));
    let is_null = |i: u32| mb.get_null_word(i as usize) & 1 != 0;
    // Group id: NULL gets its own id, distinct from every integer value.
    assert_groups_contiguous(&indices, |i| {
        if is_null(i) {
            u128::MAX
        } else {
            i64::from_le_bytes(mb.get_col_ptr(i as usize, 0, 8).try_into().unwrap()) as u128
        }
    });
    let null_positions: Vec<usize> = indices
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
    let n = rows.len();
    let mut b = Batch::with_capacity(schema, n.max(1));
    for &(pk0, pk1, w, val) in rows {
        let mut pk_bytes = [0u8; 16];
        // 2×U64 compound PK: both unsigned, OPK == BE per column.
        pk_bytes[0..8].copy_from_slice(&pk0.to_be_bytes());
        pk_bytes[8..16].copy_from_slice(&pk1.to_be_bytes());
        b.extend_pk_bytes(&pk_bytes);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

/// emit_reduce_row natural-PK byte path on a 2×U64 compound PK: PK bytes
/// must be copied verbatim from the source row, not packed from group_key.
#[test]
fn test_emit_reduce_row_compound_pk_bytes() {
    let in_schema = pk_payload_schema(&[type_code::U64; 2]);

    // Output schema matches what build_reduce_output_schema would produce
    // for a PkPermutation grouping on this input with a COUNT aggregate:
    // 2 PK cols (U64,U64) followed by I64 count.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );

    let pk0: u64 = 0xAAAA_BBBB_CCCC_DDDDu64;
    let pk1: u64 = 0x1111_2222_3333_4444u64;
    let input = make_batch_compound_2xu64(&in_schema, &[(pk0, pk1, 1, 99)]);
    let mb = input.as_mem_batch();

    let mut output = Batch::with_capacity(&out_schema, 1);
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Count };
    // Natural-PK grouping passes the source row's PK bytes; they're copied verbatim.
    let plan = make_plan(&in_schema, &[0u32, 1u32], std::slice::from_ref(&agg), false, false);
    let accs = plan.acc_template.clone();
    emit_reduce_row(&mut output, (&mb, 0), mb.get_pk_bytes(0), &accs, &plan);

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
    let in_schema = pk_payload_schema(&[type_code::U64; 2]);

    // Output: full natural compound PK + I64 agg, matching the
    // build_reduce_output_schema layout for PkPermutation.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
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
        &[agg],
        None,
        false,
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // Input is PK-sorted from consolidation; the PK-keyed group walk
    // passes that order straight through.
    let delta = make_batch(&in_schema, &[(7, 1, 0), (42, 1, 0), (99, 1, 0)]);

    let agg = AggDescriptor { col_idx: 0, agg_op: AggFunc::Min };
    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false, false);
    // GROUP BY pk → each row is its own group; MIN(pk) per group equals the row's pk.
    assert_eq!(out.count, 3);
    let mins: Vec<i64> = (0..out.count).map(|i| read_i64_le(out.col_data(0), i * 8)).collect();
    assert_eq!(mins, vec![7, 42, 99]);
}

/// Permuted group_by_cols on a compound PK must still emit rows in
/// pk_indices order (the input is PK-sorted from consolidation; the
/// fast path skips the sort and passes row order through).
#[test]
fn test_reduce_group_by_pk_permuted_preserves_pk_order() {
    let in_schema = pk_payload_schema(&[type_code::U64; 2]);
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0, 1],
    );

    let mut to_ch = empty_trace(out_schema);

    // Two PKs whose [0,1] and [1,0] orderings disagree: (1,2) vs (2,1).
    // PK-sorted (pk_indices=[0,1]) order: (1,2) then (2,1).
    let delta = make_batch_compound_2xu64(&in_schema, &[(1, 2, 1, 10), (2, 1, 1, 20)]);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Count };

    // group_by_cols permuted to [1, 0] — a valid set permutation of pk_indices.
    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32, 0u32],
        &[agg],
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2);
    let row0_pk = out.get_pk_bytes(0);
    let row1_pk = out.get_pk_bytes(1);
    let p0_col0 = u64::from_be_bytes(row0_pk[0..8].try_into().unwrap());
    let p0_col1 = u64::from_be_bytes(row0_pk[8..16].try_into().unwrap());
    let p1_col0 = u64::from_be_bytes(row1_pk[0..8].try_into().unwrap());
    let p1_col1 = u64::from_be_bytes(row1_pk[8..16].try_into().unwrap());
    assert_eq!(
        (p0_col0, p0_col1),
        (1, 2),
        "first emitted row must be (1, 2) in pk_indices order"
    );
    assert_eq!(
        (p1_col0, p1_col1),
        (2, 1),
        "second emitted row must be (2, 1) in pk_indices order"
    );
}

// -----------------------------------------------------------------------
// Compound-PK subset grouping: PK-region access must be per-PK-column
// (pre-fix the slow path widened the entire region and split groups
// that share the addressed PK column but differ in other PK columns).
// -----------------------------------------------------------------------

/// compare_by_group_cols on the PK-sentinel branch must compare only
/// the addressed PK column. Two rows that share `pk_col_0` but differ
/// in `pk_col_1` must compare Equal under `GROUP BY pk_col_0`.
#[test]
fn test_compare_by_group_cols_pk_sentinel_compound_subset() {
    let schema = pk_payload_schema(&[type_code::U64; 2]);
    let batch = make_batch_compound_2xu64(&schema, &[(10, 7, 1, 100), (10, 9, 1, 200), (20, 7, 1, 300)]);
    let mb = batch.as_mem_batch();

    let descs_v = locate_cols(&schema, &[0u32]);
    let descs = &descs_v[..];
    assert!(
        matches!(descs[0], ColumnLocator::Pk { byte_off: 0, .. }),
        "subset group on PK col 0 must resolve to a Pk locator at byte offset 0"
    );

    // Same pk_col_0 (10), different pk_col_1 → Equal under GROUP BY pk_col_0.
    assert_eq!(
        compare_by_group_cols(&mb, 0, &mb, 1, descs),
        std::cmp::Ordering::Equal,
        "rows with same pk_col_0 must form one group regardless of pk_col_1",
    );
    // Different pk_col_0 → ordering follows pk_col_0.
    assert_eq!(compare_by_group_cols(&mb, 0, &mb, 2, descs), std::cmp::Ordering::Less);
    assert_eq!(
        compare_by_group_cols(&mb, 2, &mb, 0, descs),
        std::cmp::Ordering::Greater
    );
}

/// compare_by_group_cols on PK-sentinel with `GROUP BY pk_col_1`
/// (non-zero PK byte offset) must isolate pk_col_1.
#[test]
fn test_compare_by_group_cols_pk_sentinel_compound_pk_col_1() {
    let schema = pk_payload_schema(&[type_code::U64; 2]);
    let batch = make_batch_compound_2xu64(&schema, &[(1, 50, 1, 100), (2, 50, 1, 200), (3, 60, 1, 300)]);
    let mb = batch.as_mem_batch();

    let descs_v = locate_cols(&schema, &[1u32]);
    let descs = &descs_v[..];
    assert!(
        matches!(descs[0], ColumnLocator::Pk { byte_off: 8, .. }),
        "pk_col_1 byte offset within PK region"
    );

    // Same pk_col_1 (50), different pk_col_0 → Equal.
    assert_eq!(compare_by_group_cols(&mb, 0, &mb, 1, descs), std::cmp::Ordering::Equal);
    // Different pk_col_1 → ordering follows pk_col_1.
    assert_eq!(compare_by_group_cols(&mb, 0, &mb, 2, descs), std::cmp::Ordering::Less);
}

/// Single-PK U64 with `GROUP BY pk` must be bit-identical to the prior
/// whole-region widen path — byte offset 0, size = pk_stride = 8.
#[test]
fn test_compare_by_group_cols_pk_sentinel_single_pk_bit_identical() {
    let schema = make_schema_u64_i64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();

    let descs_v = locate_cols(&schema, &[0u32]);
    let descs = &descs_v[..];
    assert!(matches!(descs[0], ColumnLocator::Pk { byte_off: 0, size: 8, .. }));

    assert_eq!(compare_by_group_cols(&mb, 0, &mb, 1, descs), std::cmp::Ordering::Less);
    assert_eq!(
        compare_by_group_cols(&mb, 1, &mb, 0, descs),
        std::cmp::Ordering::Greater
    );
    assert_eq!(compare_by_group_cols(&mb, 0, &mb, 2, descs), std::cmp::Ordering::Equal);
}

/// The group key of `GROUP BY pk_col_0` (single PK column of a
/// compound PK) must return the same u128 for two rows that share
/// pk_col_0 — distinct pk_col_1 values must not collide them into
/// different groups.
#[test]
fn test_group_key_single_pk_col_compound_subset() {
    let schema = pk_payload_schema(&[type_code::U64; 2]);
    let batch = make_batch_compound_2xu64(&schema, &[(10, 50, 1, 0), (10, 99, 1, 0), (20, 50, 1, 0)]);
    let mb = batch.as_mem_batch();

    let k0 = GroupKeyCols::new(&schema, &[0u32]).key_row(&mb, 0);
    let k1 = GroupKeyCols::new(&schema, &[0u32]).key_row(&mb, 1);
    let k2 = GroupKeyCols::new(&schema, &[0u32]).key_row(&mb, 2);

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

    let k0 = GroupKeyCols::new(&schema, &[0u32]).key_row(&mb, 0);
    let k1 = GroupKeyCols::new(&schema, &[0u32]).key_row(&mb, 1);
    assert_eq!(k0, 42u128, "single PK widens to the same value as before");
    assert_eq!(k1, 99u128);
}

/// End-to-end op_reduce: GROUP BY pk_col_0 (a strict subset of a
/// compound PK) with COUNT(*). Pre-fix the slow path widened the
/// whole PK region and split every (pk_col_0, pk_col_1) pair into
/// its own group; the fix collapses rows sharing pk_col_0.
#[test]
fn test_op_reduce_compound_pk_group_by_subset_count() {
    let in_schema = pk_payload_schema(&[type_code::U64; 2]);
    // GROUP BY a single U64 column → use_natural_pk via
    // SingleNaturalCol. Output: U64 pk + I64 count.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let mut to_ch = empty_trace(out_schema);

    // PK-sorted (pk0, pk1): (1,10), (1,20), (2,10).
    let delta = make_batch_compound_2xu64(&in_schema, &[(1, 10, 1, 0), (1, 20, 1, 0), (2, 10, 1, 0)]);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Count };

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false, false);

    // Two groups: pk_col_0=1 (count=2), pk_col_0=2 (count=1).
    // Pre-fix the count would be 3 (one row per (pk0, pk1) pair).
    assert_eq!(
        out.count, 2,
        "GROUP BY pk_col_0 collapses (1,10) and (1,20) into one group"
    );

    // Output rows in pk_col_0 ascending order (slow path argsorts).
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    )
}

fn make_batch_u64pk_i64grp_u64val(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, i64, u64)], // (pk, weight, grp, u64_val)
) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_capacity(schema, n.max(1));
    for &(pk, w, grp, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &grp.to_le_bytes());
        b.extend_col(1, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

/// `[U64 pk, I64 grp, I64 val]` — group by the I64 payload `grp`. `val_nullable`
/// picks the two shapes the reduce tests need: NOT NULL keeps the reduce output on
/// the null-blind fixed-int comparator, nullable is required wherever a test writes
/// a null bit into `val` (and is what the non-linear delta is then consolidated
/// under). `grp` stays NOT NULL — it is the group key, which
/// `GroupKeyExtractor::new` asserts on.
fn u64pk_i64grp_i64val(val_nullable: bool) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, u8::from(val_nullable)),
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
    let out_schema = out_schema_for(&in_schema, &[1u32], std::slice::from_ref(&agg));

    let mut to_ch = empty_trace(out_schema);

    // One group (grp=7). val=1, u64::MAX, and 2^63 — the unsigned MIN is 1.
    // Pre-fix signed comparison treats u64::MAX as -1 (smallest signed),
    // so the bug reports u64::MAX as the MIN.
    let delta = make_batch_u64pk_i64grp_u64val(
        &in_schema,
        &[(1, 1, 7, u64::MAX), (2, 1, 7, 10), (3, 1, 7, 1u64 << 63), (4, 1, 7, 1)],
    );

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &[agg], None, false, false);
    assert_eq!(out.count, 1);
    let min_bits = u64::from_le_bytes(out.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(min_bits, 1u64, "MIN(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_max_u64_high_bit_set() {
    let in_schema = make_schema_u64pk_i64grp_u64val();
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Max };
    let out_schema = out_schema_for(&in_schema, &[1u32], std::slice::from_ref(&agg));

    let mut to_ch = empty_trace(out_schema);

    // Same input as MIN test. Unsigned MAX is u64::MAX. Pre-fix signed
    // comparison treats 10 (positive i64) as larger than u64::MAX (=-1).
    let delta = make_batch_u64pk_i64grp_u64val(
        &in_schema,
        &[(1, 1, 7, u64::MAX), (2, 1, 7, 10), (3, 1, 7, 1u64 << 63), (4, 1, 7, 1)],
    );

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &[agg], None, false, false);
    assert_eq!(out.count, 1);
    let max_bits = u64::from_le_bytes(out.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(max_bits, u64::MAX, "MAX(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_min_u64_incremental() {
    let in_schema = make_schema_u64pk_i64grp_u64val();
    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };
    let out_schema = out_schema_for(&in_schema, &[1u32], std::slice::from_ref(&agg));

    // Tick 1: one row with val=1u64<<60 → MIN = 1u64<<60.
    let mut to_ch = empty_trace(out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 1u64 << 60)]);

    let mut avi1 = Avi::new(&in_schema, &[1u32], &[agg], &[&delta1]);
    let out1 = op_reduce(
        &delta1,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi1.cursor()),
        false,
        false,
    );
    assert_eq!(out1.count, 1);
    let min1 = u64::from_le_bytes(out1.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(min1, 1u64 << 60);

    // Tick 2: delta adds a row with val=1u64<<63, so the index holds both.
    //
    // MIN(1u64<<60, 1u64<<63) is unchanged at 1u64<<60 under unsigned;
    // under buggy signed compare it would flip to 1u64<<63 = i64::MIN.
    // op_reduce emits retract+new even when the value didn't change, so
    // we get 2 rows; we assert the new emitted value is the unsigned MIN.
    let mut to_ch2 = trace_cursor(out1, out_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, 1u64 << 63)]);
    let mut avi2 = Avi::new(&in_schema, &[1u32], &[agg], &[&delta1, &delta2]);

    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi2.cursor()),
        false,
        false,
    );
    assert_eq!(out2.count, 2, "retract old MIN + emit new MIN");
    let retracted = u64::from_le_bytes(out2.col_data(1)[0..8].try_into().unwrap());
    let new_min = u64::from_le_bytes(out2.col_data(1)[8..16].try_into().unwrap());
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
    let out_schema = out_schema_for(&in_schema, &[1u32], std::slice::from_ref(&agg));

    // Tick 1: MAX over a single low value → MAX = 10.
    let mut to_ch = empty_trace(out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 10)]);

    let mut avi1 = Avi::new(&in_schema, &[1u32], &[agg], &[&delta1]);
    let out1 = op_reduce(
        &delta1,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi1.cursor()),
        false,
        false,
    );
    assert_eq!(out1.count, 1);
    let max1 = u64::from_le_bytes(out1.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(max1, 10);

    // Tick 2: delta adds val=u64::MAX, so the index holds both.
    // Pre-fix signed MAX would treat u64::MAX as -1, keeping MAX=10.
    let mut to_ch2 = trace_cursor(out1, out_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, u64::MAX)]);
    let mut avi2 = Avi::new(&in_schema, &[1u32], &[agg], &[&delta1, &delta2]);

    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi2.cursor()),
        false,
        false,
    );
    // Expect: retract old MAX (10) + emit new MAX (u64::MAX).
    assert_eq!(out2.count, 2);
    let retracted = u64::from_le_bytes(out2.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(retracted, 10);
    assert_eq!(out2.get_weight(0), -1);
    let new_max = u64::from_le_bytes(out2.col_data(1)[8..16].try_into().unwrap());
    assert_eq!(new_max, u64::MAX, "new MAX must be unsigned-max u64::MAX");
    assert_eq!(out2.get_weight(1), 1);
}

#[test]
fn test_avi_seed_u64_high_bit() {
    // The AVI fast path seeds an Accumulator with a U64 order image via
    // `seed_encoded_extreme`, then folds in delta rows via `step_from_batch`.
    // Validates that the U64 bit pattern preserved by the AVI seed
    // compares correctly under unsigned semantics against incoming
    // delta rows.
    let in_schema = make_schema_with_type(type_code::U64);

    let desc = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };
    let mut acc = make_acc(&in_schema, &[0], desc);

    // AVI seeds the accumulator with 1u64<<63 (high bit set); a U64's order
    // image is the value itself.
    acc.seed_encoded_extreme(1u64 << 63);
    assert_eq!(acc.value_bits(), 1u64 << 63);

    // Build a batch with a single row val=10u64, pk=1.
    let batch = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10u64.to_le_bytes());
        b.count += 1;
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
    let out_schema = out_schema_for(&in_schema, &[1u32], std::slice::from_ref(&agg));

    // MIN test.
    {
        let mut to_ch = empty_trace(out_schema);

        let delta = make_batch_u64pk_i64grp_i64val(
            &in_schema,
            &[(1, 1, 7, i64::MIN), (2, 1, 7, -1), (3, 1, 7, 0), (4, 1, 7, i64::MAX)],
        );

        let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &[agg], None, false, false);
        assert_eq!(out.count, 1);
        let min = read_i64_le(out.col_data(1), 0);
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

        let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &[agg], None, false, false);
        assert_eq!(out.count, 1);
        let max = read_i64_le(out.col_data(1), 0);
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
    let n = rows.len();
    let mut b = Batch::with_capacity(schema, n.max(1));
    for &(pk, w, val) in rows {
        // pk_encode yields the native value; OPK-encode (sign-flip for signed).
        b.extend_pk_opk(&[pk_encode(pk)]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Raw);
    b
}

/// `[Sum(sum_col, sum_tc), Count(col 0)]` — a linear SUM plus the appended
/// cardinality companion every all-linear reduce carries, the agg_descs the
/// planner produces for `SELECT …, SUM(v) … GROUP BY …`.
fn sum_count_aggs(sum_col: u32) -> [AggDescriptor; 2] {
    [
        AggDescriptor { col_idx: sum_col, agg_op: AggFunc::Sum },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
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

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false, false);

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
    // The rows above are physically in canonical PK order (3 before 5), but
    // op_reduce ships its output as an honest unconsolidated delta: it never
    // certifies the claim (a decreasing aggregate would emit a descending old/new
    // pair at the same PK), so downstream re-sorts/folds.
    assert!(!out.is_consolidated(), "reduce output is an unconsolidated delta");
}

#[test]
fn test_reduce_group_by_pk_unsorted_input_count() {
    let in_schema = make_schema_u64_i64();
    let agg = AggDescriptor { col_idx: 1, agg_op: AggFunc::Count };
    let out_schema = out_schema_for(&in_schema, &[0u32], std::slice::from_ref(&agg));
    let mut to_ch = empty_trace(out_schema);

    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &[agg], None, false, false);

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
    // consolidated-`working` branch, which skips the argsort, must produce
    // identical output.
    let mut delta = make_batch_raw_pk(&in_schema, &[(3, 1, 20), (5, 1, 10), (5, 1, 30)], |pk: u64| pk as u128);
    delta.certify_layout(Layout::Consolidated);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false, false);

    assert_eq!(out.count, 2);
    let pk0 = out_pk(&out, 0);
    let pk1 = out_pk(&out, 1);
    assert_eq!((pk0, pk1), (3, 5));
    let sum0 = read_i64_le(out.col_data(0), 0);
    let sum1 = read_i64_le(out.col_data(0), 8);
    assert_eq!((sum0, sum1), (20, 40));
}

#[test]
fn test_reduce_group_by_pk_unsorted_compound_pk_permuted() {
    let in_schema = pk_payload_schema(&[type_code::U64; 2]);
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0, 1],
    );
    let mut to_ch = empty_trace(out_schema);

    // Unsorted compound-PK delta: physical order is (2,1) then (1,2).
    // Canonical pk_indices order should emit (1,2) first.
    let mut delta = make_batch_compound_2xu64(&in_schema, &[(2, 1, 1, 20), (1, 2, 1, 10)]);
    delta.set_layout_unchecked(Layout::Raw);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Count };

    // Permuted GROUP BY: [1, 0]. PkPermutation still holds.
    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32, 0u32],
        &[agg],
        None,
        false,
        false,
    );

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
    // trailing I64 is the cardinality companion every all-linear reduce carries.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
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

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false, false);

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
    let mut prev = Batch::with_capacity(&out_schema, 1);
    prev.extend_pk(5u128);
    prev.extend_weight(&1i64.to_le_bytes());
    prev.extend_null_bmp(&0u64.to_le_bytes());
    prev.extend_col(0, &100i64.to_le_bytes());
    prev.extend_col(1, &1i64.to_le_bytes());
    prev.count += 1;
    prev.set_layout_unchecked(Layout::Consolidated);
    let mut to_ch = trace_cursor(prev, out_schema);

    // Unsorted delta with pk=5 split across the batch. Pre-fix: emits
    // TWO `(pk=5, w=-1, SUM=100)` retractions plus split partials.
    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[0u32], &aggs, None, false, false);

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
    let out_schema = out_schema_for(&in_schema, &[0u32], std::slice::from_ref(&agg));

    // History: pk=1 with two payloads (val=10, val=20). The group IS the PK, so
    // both belong to group pk=1; MIN(10, 20) = 10.
    let history = make_batch(&in_schema, &[(1, 1, 10), (1, 1, 20)]);

    // trace_out: old MIN(pk=1) = 10.
    let mut prev = Batch::with_capacity(&out_schema, 1);
    prev.extend_pk(1u128);
    prev.extend_weight(&1i64.to_le_bytes());
    prev.extend_null_bmp(&0u64.to_le_bytes());
    prev.extend_col(0, &10i64.to_le_bytes());
    prev.count += 1;
    prev.set_layout_unchecked(Layout::Consolidated);
    let mut to_ch = trace_cursor(prev, out_schema);

    // Delta: retract (pk=1, val=10) — the payload holding the current MIN.
    let delta = make_batch(&in_schema, &[(1, -1, 10)]);

    // The index carries the history plus this delta's retraction, as the
    // compiler's Integrate-before-Reduce order produces.
    let mut avi = Avi::new(&in_schema, &[0u32], &[agg], &[&history, &delta]);
    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[0u32], // GROUP BY PK col 0 → the PK region is the group key
        &[agg],
        Some(&mut avi.cursor()),
        false,
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
fn avi_gcol(dst: &mut [u8], native_le: &[u8], tc: u8) {
    gnitz_wire::encode_pk_column(native_le, tc, dst);
}

#[test]
fn avi_two_groups_distinct_byte_form_keys() {
    // Input: pk(U64), a(U32), b(U32), val(I64); GROUP BY (a, b), MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // Output: synthetic U128 PK, group cols (a, b), MIN.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // delta: one row per group. The delta values are deliberately NOT each
    // group's minimum, so a correct result can only come from the index.
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 2);
        for (pk, a, bb, val) in [(1u64, 1u32, 1u32, 50i64), (2, 2, 2, 60)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &a.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &bb.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(3).unwrap(), &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // AVI: key = a(4) ++ b(4) ++ av_encoded(8). Group (1,1) min=10, (2,2) min=20.
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 17, "4 + 4 + 1 + 8");
    let avi_batch = {
        let mut b = Batch::with_capacity(&avi_schema, 2);
        for (a, bb, min) in [(1u32, 1u32, 10i64), (2, 2, 20)] {
            let mut key = [0u8; 17];
            avi_gcol(&mut key[0..4], &a.to_le_bytes(), type_code::U32);
            avi_gcol(&mut key[4..8], &bb.to_le_bytes(), type_code::U32);
            let av = i64_av(min);
            key[8] = 0; // ordinal 0 (single MIN aggregate)
            key[9..17].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b
    };

    let mut to_ch = empty_trace(out_schema);
    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi_ch),
        false,
        false,
    );

    assert_eq!(out.count, 2, "two groups → two rows");
    // Output payload: a at pi 0, b at pi 1, min at pi 2.
    for i in 0..out.count {
        let a = gnitz_wire::read_u32_le(out.col_data(0), i * 4);
        let bb = gnitz_wire::read_u32_le(out.col_data(1), i * 4);
        let min = read_i64_le(out.col_data(2), i * 8);
        let expected = match (a, bb) {
            (1, 1) => 10,
            (2, 2) => 20,
            _ => panic!("unexpected group ({a}, {bb})"),
        };
        assert_eq!(
            min, expected,
            "group ({a},{bb}) must resolve its own indexed MIN, not the other group's"
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // delta: retract the row holding the current MIN (val=5) of group a=1.
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &1u32.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = GroupKeyCols::new(&in_schema, &[1u32]).key_row(&delta.as_mem_batch(), 0);

    // AVI (post-state for group a=1): the retracted 5 is gone; the surviving
    // values are {10, 20}, so the prefix walk must return the smaller, 10.
    let avi_schema = avi_schema(&in_schema, &[1u32]);
    let avi_batch = {
        let mut b = Batch::with_capacity(&avi_schema, 2);
        for min in [10i64, 20] {
            let mut key = [0u8; 13];
            avi_gcol(&mut key[0..4], &1u32.to_le_bytes(), type_code::U32);
            let av = i64_av(min);
            key[4] = 0; // ordinal 0 (single MIN aggregate)
            key[5..13].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // trace_out: previous output for a=1 was MIN=5.
    let to_batch = {
        let mut b = Batch::with_capacity(&out_schema, 1);
        b.extend_pk(group_key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &1u32.to_le_bytes()); // a
        b.extend_col(1, &5i64.to_le_bytes()); // min
        b.count += 1;
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
        &[agg],
        Some(&mut avi_ch),
        false,
        false,
    );

    // Expect a retraction of the old MIN (5, weight -1) and the recomputed MIN
    // (10, weight +1) read from the index — never the retracted 5.
    let mut retracted = None;
    let mut inserted = None;
    for i in 0..out.count {
        let w = out.get_weight(i);
        let v = read_i64_le(out.col_data(1), i * 8);
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
    for (gtc, gsize, stride) in [(type_code::U16, 2usize, 11usize), (type_code::U32, 4, 13)] {
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(gtc, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(gtc, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let gval: u64 = 7;
        let delta = {
            let mut b = Batch::with_capacity(&in_schema, 1);
            b.extend_pk(1u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &gval.to_le_bytes()[..gsize]);
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &100i64.to_le_bytes());
            b.count += 1;
            b.set_layout_unchecked(Layout::Consolidated);
            b
        };

        let avi_schema = avi_schema(&in_schema, &[1u32]);
        assert_eq!(avi_schema.pk_stride(), stride);
        let avi_batch = {
            let mut b = Batch::with_capacity(&avi_schema, 1);
            let mut key = vec![0u8; stride];
            avi_gcol(&mut key[..gsize], &gval.to_le_bytes()[..gsize], gtc);
            let av = i64_av(42i64);
            key[gsize] = 0; // ordinal
            key[gsize + 1..gsize + 9].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
            b
        };

        let mut to_ch = empty_trace(out_schema);
        let mut avi_ch = trace_cursor(avi_batch, avi_schema);

        let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

        let out = op_reduce(
            &delta,
            &mut to_ch,
            &in_schema,
            &[1u32],
            &[agg],
            Some(&mut avi_ch),
            false,
            false,
        );

        assert_eq!(out.count, 1, "stride {stride}");
        // Output payload: g at pi 0, min at pi 1.
        let min = read_i64_le(out.col_data(1), 0);
        assert_eq!(min, 42, "stride {stride}: indexed MIN");
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let mk_row = |b: &mut Batch, pk: u64, w: i64, g: i64, val: i64| {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &val.to_le_bytes());
        b.count += 1;
    };

    // History: group g=1 holds val=5 twice (pk=1, pk=2) and val=10 (pk=3).
    let ti_batch = {
        let mut b = Batch::with_capacity(&in_schema, 3);
        mk_row(&mut b, 1, 1, 1, 5);
        mk_row(&mut b, 2, 1, 1, 5);
        mk_row(&mut b, 3, 1, 1, 10);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = GroupKeyCols::new(&in_schema, &[1u32]).key_row(&ti_batch.as_mem_batch(), 0);

    let to_batch = {
        let mut b = Batch::with_capacity(&out_schema, 1);
        b.extend_pk(group_key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &1i64.to_le_bytes());
        b.extend_col(1, &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // delta: retract one of the two val=5 rows (pk=1).
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        mk_row(&mut b, 1, -1, 1, 5);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = trace_cursor(to_batch, out_schema);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    let mut avi = Avi::new(&in_schema, &[1u32], &[agg], &[&ti_batch, &delta]);
    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi.cursor()),
        false,
        false,
    );

    let mut new_min = None;
    for i in 0..out.count {
        if out.get_weight(i) > 0 {
            new_min = Some(read_i64_le(out.col_data(1), i * 8));
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let null_bit = 1u64 << in_schema.try_payload_idx(2).unwrap();
    let mk_row = |b: &mut Batch, pk: u64, g: i64, val: i64, is_null: bool| {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(if is_null { null_bit } else { 0 }).to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &val.to_le_bytes());
        b.count += 1;
    };

    // group g=1: NULL, 7, 3 → MIN ignores NULL → 3.
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 3);
        mk_row(&mut b, 1, 1, 0, true);
        mk_row(&mut b, 2, 1, 7, false);
        mk_row(&mut b, 3, 1, 3, false);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = empty_trace(out_schema);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    let out = op_reduce(&delta, &mut to_ch, &in_schema, &[1u32], &[agg], None, false, false);

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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // delta: retract group (3, 4)'s current MIN (val=5).
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &3u32.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &4u32.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(3).unwrap(), &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = GroupKeyCols::new(&in_schema, &[1u32, 2u32]).key_row(&delta.as_mem_batch(), 0);

    // AVI post-state for (3, 4): surviving min is 9. A decoy entry for a
    // different group (3, 5) sharing the a-byte prefix must NOT be matched.
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    let avi_batch = {
        let mut b = Batch::with_capacity(&avi_schema, 2);
        let put = |b: &mut Batch, a: u32, bb: u32, min: i64| {
            let mut key = [0u8; 17];
            avi_gcol(&mut key[0..4], &a.to_le_bytes(), type_code::U32);
            avi_gcol(&mut key[4..8], &bb.to_le_bytes(), type_code::U32);
            let av = i64_av(min);
            key[8] = 0; // ordinal 0 (single MIN aggregate)
            key[9..17].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        };
        put(&mut b, 3, 4, 9);
        put(&mut b, 3, 5, 1); // decoy: same a, different b, smaller value
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let to_batch = {
        let mut b = Batch::with_capacity(&out_schema, 1);
        b.extend_pk(group_key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &3u32.to_le_bytes());
        b.extend_col(1, &4u32.to_le_bytes());
        b.extend_col(2, &5i64.to_le_bytes());
        b.count += 1;
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
        &[agg],
        Some(&mut avi_ch),
        false,
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
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::U64, 0), // a (group)
            SchemaColumn::new(type_code::U64, 0), // b (group)
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
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
                avi_gcol(&mut key[0..8], &a.to_le_bytes(), type_code::U64);
                avi_gcol(&mut key[8..16], &b.to_le_bytes(), type_code::U64);
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
        let mut bt = Batch::with_capacity(&avi_schema, keys.len());
        for key in &keys {
            bt.extend_pk_bytes(key);
            bt.extend_weight(&1i64.to_le_bytes());
            bt.extend_null_bmp(&0u64.to_le_bytes());
            bt.count += 1;
        }
        bt.set_layout_unchecked(Layout::Consolidated);
        bt
    };

    // Delta: one representative insert per group. Its val is deliberately the
    // group's MIN + 1000 so a correct result can only come from the index.
    let delta = {
        let mut bt = Batch::with_capacity(&in_schema, group_coords.len());
        for (i, &(a, b)) in group_coords.iter().enumerate() {
            bt.extend_pk(i as u128 + 1);
            bt.extend_weight(&1i64.to_le_bytes());
            bt.extend_null_bmp(&0u64.to_le_bytes());
            bt.extend_col(in_schema.try_payload_idx(1).unwrap(), &a.to_le_bytes());
            bt.extend_col(in_schema.try_payload_idx(2).unwrap(), &b.to_le_bytes());
            let decoy = reference[&(a, b)].wrapping_add(1000);
            bt.extend_col(in_schema.try_payload_idx(3).unwrap(), &decoy.to_le_bytes());
            bt.count += 1;
        }
        bt
    };

    let mut to_ch = empty_trace(out_schema);
    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi_ch),
        false,
        false,
    );

    assert_eq!(out.count, reference.len(), "one row per group");
    let mut seen: BTreeMap<(u64, u64), i64> = BTreeMap::new();
    for i in 0..out.count {
        let a = read_u64_le(out.col_data(0), i * 8);
        let b = read_u64_le(out.col_data(1), i * 8);
        let m = read_i64_le(out.col_data(2), i * 8);
        seen.insert((a, b), m);
    }
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
            SchemaColumn::new(type_code::U64, 0),  // pk
            SchemaColumn::new(type_code::U128, 0), // g (group)
            SchemaColumn::new(type_code::I64, 0),  // val
        ],
        &[0],
    );
    // A single U128 group column uses a natural PK: the group value IS the
    // output PK; the only payload column is the aggregate.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // PK = group value g
            SchemaColumn::new(type_code::I64, 1),  // MIN
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
        let mut b = Batch::with_capacity(&in_schema, 2);
        for (i, &(g, _)) in groups.iter().enumerate() {
            b.extend_pk(i as u128 + 1);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &999i64.to_le_bytes());
            b.count += 1;
        }
        b
    };

    let avi_batch = {
        let mut b = Batch::with_capacity(&avi_schema, 2);
        // sorted ascending by g (both share high bytes; g1 < g2 by low byte).
        for &(g, m) in &groups {
            let mut key = [0u8; 25];
            avi_gcol(&mut key[0..16], &g.to_le_bytes(), type_code::U128);
            let av = i64_av(m);
            key[16] = 0; // ordinal 0 (single MIN aggregate)
            key[17..25].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = empty_trace(out_schema);
    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi_ch),
        false,
        false,
    );

    assert_eq!(out.count, 2);
    for i in 0..out.count {
        let g = out.get_pk(i);
        let m = read_i64_le(out.col_data(0), i * 8);
        let expected = if g == g1 {
            10
        } else if g == g2 {
            20
        } else {
            panic!("group {g}")
        };
        assert_eq!(m, expected, "U128 group {g} must resolve its own MIN");
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
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::I64, 0), // a (signed group)
            SchemaColumn::new(type_code::U64, 0), // b (group)
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 25);

    // Groups: (-5, 10) MIN=100, (-5, 11) MIN=50, (3, 10) MIN=200.
    // Signed order on column a: -5 < 3, so the (-5,*) groups precede (3,*).
    let groups: [(i64, u64, i64); 3] = [(-5, 10, 100), (-5, 11, 50), (3, 10, 200)];

    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 3);
        for (i, &(a, bb, _)) in groups.iter().enumerate() {
            b.extend_pk(i as u128 + 1);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &a.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &bb.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(3).unwrap(), &777i64.to_le_bytes());
            b.count += 1;
        }
        b
    };

    // AVI rows in memcmp (compare_pk_bytes) order of the composite key — what
    // production's ingest produces. The group prefix is OPK, so the signed
    // column's byte order is its signed order.
    let avi_batch = {
        let mut b = Batch::with_capacity(&avi_schema, 3);
        let mut keys: Vec<[u8; 25]> = groups
            .iter()
            .map(|&(a, bb, m)| {
                let mut key = [0u8; 25];
                avi_gcol(&mut key[0..8], &a.to_le_bytes(), type_code::I64);
                avi_gcol(&mut key[8..16], &bb.to_le_bytes(), type_code::U64);
                let av = i64_av(m);
                key[16] = 0; // ordinal 0 (single MIN aggregate)
                key[17..25].copy_from_slice(&av.to_be_bytes());
                key
            })
            .collect();
        keys.sort();
        for key in &keys {
            b.extend_pk_bytes(key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = empty_trace(out_schema);
    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi_ch),
        false,
        false,
    );

    assert_eq!(out.count, 3);
    for i in 0..out.count {
        let a = read_i64_le(out.col_data(0), i * 8);
        let bb = read_u64_le(out.col_data(1), i * 8);
        let m = read_i64_le(out.col_data(2), i * 8);
        let expected = match (a, bb) {
            (-5, 10) => 100,
            (-5, 11) => 50,
            (3, 10) => 200,
            _ => panic!("unexpected group ({a}, {bb})"),
        };
        assert_eq!(m, expected, "signed-key group ({a},{bb}) must resolve its own MIN");
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
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::U64, 0), // a
            SchemaColumn::new(type_code::U64, 0), // b
            SchemaColumn::new(type_code::U64, 0), // c
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
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
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &1u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &2u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(3).unwrap(), &3u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(4).unwrap(), &999i64.to_le_bytes());
        b.count += 1;
        b
    };

    let avi_batch = {
        let mut b = Batch::with_capacity(&avi_schema, 2);
        // sorted by (a,b,c): (1,2,3) before (1,2,4).
        for &(a, bb, c, m) in &groups {
            let mut key = [0u8; 33];
            avi_gcol(&mut key[0..8], &a.to_le_bytes(), type_code::U64);
            avi_gcol(&mut key[8..16], &bb.to_le_bytes(), type_code::U64);
            avi_gcol(&mut key[16..24], &c.to_le_bytes(), type_code::U64);
            let av = i64_av(m);
            key[24] = 0; // ordinal 0 (single MIN aggregate)
            key[25..33].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = empty_trace(out_schema);
    let mut avi_ch = trace_cursor(avi_batch, avi_schema);

    let agg = AggDescriptor { col_idx: 4, agg_op: AggFunc::Min };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32, 2u32, 3u32],
        &[agg],
        Some(&mut avi_ch),
        false,
        false,
    );

    assert_eq!(out.count, 1, "delta touched only group (1,2,3)");
    let c = read_u64_le(out.col_data(2), 0);
    let m = read_i64_le(out.col_data(3), 0);
    assert_eq!(c, 3, "must resolve group (1,2,3)");
    assert_eq!(
        m, 100,
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let avi_schema = avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 25);

    // Retract group (1<<40, 2)'s current MIN (val=5).
    let ga: u64 = 1 << 40;
    let gb: u64 = 2;
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &ga.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &gb.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(3).unwrap(), &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = GroupKeyCols::new(&in_schema, &[1u32, 2u32]).key_row(&delta.as_mem_batch(), 0);

    // AVI post-state: group (ga, gb) surviving values {9, 15}; a decoy group
    // (ga, gb+1) sharing the first 8 bytes holds a smaller 1 that must not win.
    let avi_batch = {
        let mut b = Batch::with_capacity(&avi_schema, 3);
        let put = |b: &mut Batch, a: u64, bb: u64, m: i64| {
            let mut key = [0u8; 25];
            avi_gcol(&mut key[0..8], &a.to_le_bytes(), type_code::U64);
            avi_gcol(&mut key[8..16], &bb.to_le_bytes(), type_code::U64);
            let av = i64_av(m);
            key[16] = 0; // ordinal 0 (single MIN aggregate)
            key[17..25].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        };
        // sorted by (a, b, av): (ga,gb,9),(ga,gb,15),(ga,gb+1,1).
        put(&mut b, ga, gb, 9);
        put(&mut b, ga, gb, 15);
        put(&mut b, ga, gb + 1, 1);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let to_batch = {
        let mut b = Batch::with_capacity(&out_schema, 1);
        b.extend_pk(group_key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &ga.to_le_bytes());
        b.extend_col(1, &gb.to_le_bytes());
        b.extend_col(2, &5i64.to_le_bytes());
        b.count += 1;
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
        &[agg],
        Some(&mut avi_ch),
        false,
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
// Reduce-path correctness regressions: a value-independent aggregate (COUNT)
// must not read a wide PK source column; the AVI order-encoded value must be
// byte-ordered so incremental MIN/MAX reads the true extremal; an F32 AVI seed
// must promote to F64 bits; and the group-by-PK path must handle a compound PK
// of any width.
// =======================================================================

// Bug 1: COUNT(*) is compiled with a placeholder arg column index 0. When
// column 0 is a 16-byte UUID PK, the old step_from_batch decoded the PK column
// into an 8-byte scratch buffer before the per-op match — `pk_le_buf[..16]` was
// out of range and the worker crashed. COUNT is value-independent and must
// return before touching the column.
#[test]
fn count_accumulator_over_uuid_pk_does_not_panic() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::UUID, 0), // 16-byte PK
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let mut b = Batch::with_capacity(&schema, 2);
    for pk in [1u128, 2] {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(schema.try_payload_idx(1).unwrap(), &0i64.to_le_bytes());
        b.count += 1;
    }
    let desc = AggDescriptor { col_idx: 0, agg_op: AggFunc::Count };
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

// Populate a fresh ephemeral AVI table from `deltas` through the production
// `avi_batch` + ingest, then read the extreme of delta[0]-row-0's group back
// through the production `AviBake::seed_extreme` seek. `col_idx` is the
// aggregate source column (PK or payload). Shared by the AVI full-path tests
// below.
fn avi_read_extreme(
    in_schema: &SchemaDescriptor,
    group_by: &[u32],
    col_idx: u32,
    deltas: &[&Batch],
    for_max: bool,
) -> i64 {
    // The aggregate's type is the source column's type.
    let avi_schema = avi_schema(in_schema, group_by);
    let tmp = tempfile::tempdir().unwrap();
    let mut avi_t = scratch_table(tmp.path().to_str().unwrap(), avi_schema, 0);
    let agg = AggDescriptor {
        col_idx,
        agg_op: if for_max { AggFunc::Max } else { AggFunc::Min },
    };
    let bake = make_bake(in_schema, group_by, &[agg]);

    // Each avi_batch ingest is a separate AVI ingest; the cursor's
    // two-tier consolidation sums weights across ingests, so a retracted extreme
    // (net-zero) is skipped by seek_first_positive_with_prefix.
    for d in deltas {
        use super::avi::avi_batch;
        avi_t.ingest_owned_batch(avi_batch(d, &bake)).unwrap();
    }

    let mut ch = avi_t.open_cursor();
    let mut gk = [0u8; crate::schema::MAX_PK_BYTES];
    bake.pack_group(&mut gk, &deltas[0].as_mem_batch(), 0);
    let mut acc = make_acc(in_schema, group_by, agg);
    // Ordinal 0: the single aggregate.
    bake.seed_extreme(&mut ch, &mut gk, 0, &mut acc);
    assert!(!acc.is_untouched(), "AVI seek must find the probed group");
    acc.value_bits() as i64
}

// Bug 3: the order-encoded aggregate value must be serialized big-endian so the
// index's lexicographic byte ordering matches numeric order. With three values
// in one group whose extremes differ above the low byte ({101, 111, -5}), a
// little-endian serialization sorts 101 first and reports it as the MIN; -5 is
// never seen.
#[test]
fn avi_full_path_min_max_across_high_byte() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::U64, 0), // g (group)
            SchemaColumn::new(type_code::I64, 0), // val (agg)
        ],
        &[0],
    );
    // One group g=5 with values {101, 111, -5}.
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 3);
        for (pk, v) in [(1u128, 101i64), (2, 111), (3, -5)] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &5u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &v.to_le_bytes());
            b.count += 1;
        }
        b
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
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(bv, w) in rows {
        b.extend_pk_opk(&[5u128, bv as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
    }
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
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

// Bug 4: a float aggregate's accumulator and output column are F64, so the AVI
// seed for an F32 source must promote to F64 bits — not zero-extend the raw
// 32-bit IEEE bits, which an F64 reader interprets as a tiny denormal.
#[test]
fn avi_f32_seed_promotes_to_f64_bits() {
    let in_schema = make_schema_with_type(type_code::F32);
    let desc = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };
    for v in [1.5f32, -2.25, 0.0, 1.0e30] {
        let mut acc = make_acc(&in_schema, &[0], desc);
        acc.seed_encoded_extreme(gnitz_wire::ieee_order_bits_f32(v.to_bits()));
        let bits = acc.value_bits();
        assert_eq!(
            bits,
            f64::to_bits(v as f64),
            "F32 AVI seed must render as the F64 bits of (f32 as f64) for v={v}",
        );
        assert_eq!(f64::from_bits(bits), v as f64);
    }
}

// Bug 5: a compound PK with pk_stride > 16 (two U128 columns = stride 32) whose
// GROUP BY is exactly the PK must take the unified group-by-PK path — group
// membership tested on the full PK byte window, argsorted by canonical PK
// order, emitting one weight-folded row per distinct PK. Before the
// unification the {8,16}-stride gate forced this onto the slow per-column path.
#[test]
fn reduce_wide_compound_pk_group_by_pk_counts_per_pk() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // pk col 0
            SchemaColumn::new(type_code::U128, 0), // pk col 1
        ],
        &[0, 1],
    );
    assert!(in_schema.pk_stride() > 16, "compound U128+U128 PK must exceed 16 bytes");

    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1), // COUNT
        ],
        &[0, 1],
    );

    // Two distinct compound PKs, folded and (PK,payload)-sorted: (1,1) at weight
    // 2 → cnt 2, (1,2) at weight 1 → cnt 1. Flagged consolidated (genuinely
    // folded, ghost-free, sorted) but with `sorted` left unset, so op_reduce
    // re-derives canonical order through the argsort_pk_canonical branch — the
    // wide compound-PK path under test.
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 2);
        for (a, c, w) in [(1u128, 1u128, 2i64), (1, 2, 1)] {
            b.extend_pk_opk(&[a, c]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut to_ch = empty_trace(out_schema);

    let agg = AggDescriptor { col_idx: 0, agg_op: AggFunc::Count };

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[0u32, 1u32],
        &[agg],
        None,
        false,
        false,
    );

    // op_reduce ships an honest unconsolidated delta — it does not certify even on
    // the group-by-PK fast path.
    assert!(!out.is_consolidated(), "reduce output is an unconsolidated delta");
    // The argsort_pk_canonical branch still physically orders the wide compound-PK
    // output: (1,1) (cnt 2) precedes (1,2) (cnt 1). Read counts in physical row
    // order to pin that canonical ordering (the branch under test).
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1), // nullable grp
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1), // nullable grp (carried through)
            SchemaColumn::new(type_code::I64, 1), // nullable min
        ],
        &[0],
    );

    let agg = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };

    // --- Tick 1: empty history, empty trace_out ---
    let delta1 = build_grp_val_delta(&in_schema, tick1_rows);
    let mut to_ch1 = empty_trace(out_schema);
    let mut avi1 = Avi::new(&in_schema, &[1u32], &[agg], &[&delta1]);

    let out1 = op_reduce(
        &delta1,
        &mut to_ch1,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi1.cursor()),
        false,
        false,
    );

    let got1 = read_grp_min_pairs(&out1);
    expected_tick1.sort_unstable();
    assert_eq!(got1, *expected_tick1, "tick 1 MIN mismatch");

    // --- Tick 2: history = tick1 input + this delta, trace_out = tick1 output ---
    let to_batch = out1;
    let delta2 = build_grp_val_delta(&in_schema, delta_rows);

    let mut to_ch2 = trace_cursor(to_batch, out_schema);
    let mut avi2 = Avi::new(&in_schema, &[1u32], &[agg], &[&delta1, &delta2]);

    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi2.cursor()),
        false,
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

    // The live MIN values in out2: start from tick1 result and apply net changes.
    let mut live: std::collections::BTreeMap<i64, i64> = got1.iter().cloned().collect();
    for i in 0..out2.count {
        let w = out2.get_weight(i);
        let g = read_i64_le(grp_data, i * 8);
        let m = read_i64_le(min_data, i * 8);
        if w < 0 {
            live.remove(&g);
        } else if w > 0 {
            live.insert(g, m);
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // Output: U128 pk | I64 c1 | I64 c2 | I64 min (nullable).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let agg = AggDescriptor { col_idx: 3, agg_op: AggFunc::Min };

    let pi_c1 = in_schema.try_payload_idx(1).unwrap();
    let pi_c2 = in_schema.try_payload_idx(2).unwrap();
    let pi_val = in_schema.try_payload_idx(3).unwrap();

    let make_batch = |rows: &[(u64, i64, i64, i64, i64)]| -> Batch {
        let mut b = Batch::with_capacity(&in_schema, rows.len().max(1));
        for &(pk, w, c1, c2, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(pi_c1, &c1.to_le_bytes());
            b.extend_col(pi_c2, &c2.to_le_bytes());
            b.extend_col(pi_val, &val.to_le_bytes());
            b.count += 1;
        }
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

    let mut avi1 = Avi::new(&in_schema, &[1u32, 2u32], &[agg], &[&delta1]);
    let out1 = op_reduce(
        &delta1,
        &mut to_ch,
        &in_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi1.cursor()),
        false,
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
    let mut avi2 = Avi::new(&in_schema, &[1u32, 2u32], &[agg], &[&history, &delta2]);

    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi2.cursor()),
        false,
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    )
}

#[test]
fn test_group_key_128bit_collision_resistance() {
    use std::collections::HashSet;
    let schema = make_schema_u64_i64_str();

    let pi_c1 = schema.try_payload_idx(1).unwrap();
    let pi_c2 = schema.try_payload_idx(2).unwrap();

    // Sweep many distinct (c1, c2) multi-column keys. Each (i, j) is a distinct
    // group; the 128-bit fold must map them to distinct u128 with no collision.
    let mut b = Batch::with_capacity(&schema, 1);
    let mut expected = 0usize;
    for i in 0..64i64 {
        for j in 0..64u32 {
            b.extend_pk((expected as u128) + 1);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(pi_c1, &i.to_le_bytes());
            b.extend_col_blob(pi_c2, format!("k{j}").as_bytes());
            b.count += 1;
            expected += 1;
        }
    }
    let mb = b.as_mem_batch();

    let mut keys: HashSet<u128> = HashSet::new();
    let mut his: HashSet<u64> = HashSet::new();
    let mut los: HashSet<u64> = HashSet::new();
    for row in 0..b.count {
        let k = GroupKeyCols::new(&schema, &[1u32, 2u32]).key_row(&mb, row);
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
        GroupKeyCols::new(&schema, &[1u32, 2u32]).key_row(&mb, 0),
        GroupKeyCols::new(&schema, &[1u32, 2u32]).key_row(&mb, 0),
        "same row must hash identically",
    );
}

// -----------------------------------------------------------------------
// Fix C: BLOB as a grouping key.
//
// BLOB shares the 16-byte German-string layout with STRING; the two
// group-membership compare (compare_by_group_cols) now
// dispatch BLOB through compare_german_strings instead of `unreachable!`-ing.
// Mirrors test_distinct_blob_payload_no_panic. Long (>12-byte) blobs that share
// a 4-byte prefix force the full-content heap tail comparison.
// -----------------------------------------------------------------------

/// Schema: U64 pk | BLOB grp | I64 val. BLOB is the (non-PK) grouping key.
fn make_schema_u64_blob_grp_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::BLOB, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build a (pk, weight, blob, val) batch. Blobs > 12 bytes go to the heap (long
/// form); shorter blobs stay inline — both exercise german_string_content. Rows
/// are passed in PK order so the batch is validly sorted+consolidated for use as
/// a trace cursor.
fn make_batch_blob_grp_i64(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8], i64)]) -> Batch {
    let pi_grp = schema.try_payload_idx(1).unwrap();
    let pi_val = schema.try_payload_idx(2).unwrap();
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, w, blob, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col_blob(pi_grp, blob);
        b.extend_col(pi_val, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_compare_by_group_cols_blob_no_panic() {
    // Two long blobs sharing the 4-byte prefix "PREF" and the same length, so
    // compare_german_strings must walk the heap tail (not just the inline
    // prefix). Pre-fix this `unreachable!`s; post-fix it orders by content.
    let schema = make_schema_u64_blob_grp_i64();
    let blob_a: &[u8] = b"PREF_aaaaaaaaaa"; // 15 bytes
    let blob_b: &[u8] = b"PREF_bbbbbbbbbb"; // 15 bytes, same prefix+length
    assert_eq!(blob_a.len(), blob_b.len());
    assert_eq!(&blob_a[..4], &blob_b[..4]);
    let batch = make_batch_blob_grp_i64(&schema, &[(1, 1, blob_a, 10), (2, 1, blob_b, 20)]);
    let mb = batch.as_mem_batch();
    let descs_v = locate_cols(&schema, &[1]);
    let descs = &descs_v[..];

    assert_eq!(
        compare_by_group_cols(&mb, 0, &mb, 1, descs),
        std::cmp::Ordering::Less,
        "blob_a < blob_b by content tail"
    );
    assert_eq!(
        compare_by_group_cols(&mb, 1, &mb, 0, descs),
        std::cmp::Ordering::Greater
    );
    assert_eq!(
        compare_by_group_cols(&mb, 0, &mb, 0, descs),
        std::cmp::Ordering::Equal,
        "same blob compares equal"
    );

    // The hash path (Fix B's German-string arm) and the cursor compare must
    // agree with the batch compare on this BLOB key.
    let k0 = GroupKeyCols::new(&schema, &[1u32]).key_row(&mb, 0);
    let k1 = GroupKeyCols::new(&schema, &[1u32]).key_row(&mb, 1);
    assert_ne!(k0, k1, "distinct blobs must hash to distinct group keys");
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
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::BLOB, 0),
            SchemaColumn::new(type_code::I64, 1),
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
    let out1 = op_reduce(&delta1, &mut to_ch1, &in_schema, &[1u32], &[agg], None, false, false);
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
    let mut avi2 = Avi::new(&in_schema, &[1u32], &[agg], &[&history, &delta2]);
    let out2 = op_reduce(
        &delta2,
        &mut to_ch2,
        &in_schema,
        &[1u32],
        &[agg],
        Some(&mut avi2.cursor()),
        false,
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
    // Payload: grp exemplar, count, min, max.
    let (pi_min, pi_max) = (2usize, 3usize);
    let content = |b: &Batch, row: usize, pi: usize| -> Vec<u8> {
        let mb = b.as_mem_batch();
        crate::storage::payload_bytes(&mb, row, pi).to_vec()
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
    let out1 = op_reduce(&t1, &mut trace, &in_schema, &[2u32], &aggs, None, false, false);
    assert_eq!(out1.count, 1);
    assert_eq!(content(&out1, 0, pi_min), lo);
    assert_eq!(content(&out1, 0, pi_max), top);

    // Retract both extremes; the index holds t1 and t2 and the seek lands on the
    // next value each side.
    let t2 = op_negate(make_batch_blob_grp_i64(&in_schema, &[(3, 1, lo, 10), (4, 1, top, 10)]));
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
// row at V₀ over an empty/fully-retracted source (COUNT=0, SUM/MIN/MAX=NULL).
// ---------------------------------------------------------------------------

/// Source for the global-aggregate tests: `[pk:U64, val:I64(nullable)]`.
/// Build a delta over `u64_pk_schema(SchemaColumn::new(type_code::I64, 1))` from `(pk, weight, val)` rows.
fn g_delta(rows: &[(u64, i64, i64)]) -> Batch {
    make_batch(&u64_pk_schema(SchemaColumn::new(type_code::I64, 1)), rows)
}

const G_COUNT: AggDescriptor = AggDescriptor { col_idx: 0, agg_op: AggFunc::Count };
const G_SUM: AggDescriptor = AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum };
const G_MIN: AggDescriptor = AggDescriptor { col_idx: 1, agg_op: AggFunc::Min };

/// Output `[_group_pk:U128, count:I64, min:I64(nullable)]` — the mixed
/// non-linear global-aggregate shape (`SELECT COUNT(*), MIN(x)`).
fn g_out_count_min() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// `op_reduce` over `u64_pk_schema(SchemaColumn::new(type_code::I64, 1))` with empty group cols (the global-aggregate path).
///
/// `history` is the input the value index has absorbed, this delta included —
/// empty for an all-linear aggregate set, which carries no index.
fn g_reduce(
    delta: &Batch,
    history: &[&Batch],
    trace_out: &mut crate::storage::ReadCursor,
    aggs: &[AggDescriptor],
    i_am_owner: bool,
) -> Batch {
    let in_schema = u64_pk_schema(SchemaColumn::new(type_code::I64, 1));
    let mut avi = aggs
        .iter()
        .any(|d| !d.agg_op.is_linear())
        .then(|| Avi::new(&in_schema, &[], aggs, history));
    let mut cursor = avi.as_mut().map(|a| a.cursor());
    op_reduce(
        delta,
        trace_out,
        &in_schema,
        &[],
        aggs,
        cursor.as_mut(),
        true,
        i_am_owner,
    )
}

/// Seed over an empty source emits exactly one ground row at V₀: COUNT=0, SUM=NULL.
#[test]
fn global_seed_over_empty_emits_one_ground_row() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(type_code::I64, 1)),
        &[],
        &[G_SUM, G_COUNT],
    );
    let mut to_ch = empty_trace(out_schema);

    let raw = g_reduce(&g_delta(&[]), &[], &mut to_ch, &[G_SUM, G_COUNT], true);

    assert_eq!(raw.count, 1, "empty-source global aggregate must emit one ground row");
    assert_eq!(raw.get_weight(0), 1, "ground row weight +1");
    assert_eq!(raw.get_pk(0), gnitz_wire::global_group_key(), "ground PK must be V₀");
    assert_eq!(raw.get_null_word(0) & 1, 1, "SUM must be NULL over empty source");
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
        &u64_pk_schema(SchemaColumn::new(type_code::I64, 1)),
        &[],
        &[G_SUM, G_COUNT],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw1 = g_reduce(&g_delta(&[]), &[], &mut to_ch, &[G_SUM, G_COUNT], true);
    assert_eq!(raw1.count, 1, "first pad seeds the ground");

    // Second pad: trace_out now holds the V₀ ground from the first pad.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(&g_delta(&[]), &[], &mut to_ch2, &[G_SUM, G_COUNT], true);
    assert_eq!(raw2.count, 0, "second pad must NOT re-seed (V₀ already in trace_out)");
}

/// A non-owner worker's empty pad seeds nothing — it emits a literally empty
/// batch (asserted on the batch itself, not a merged result).
#[test]
fn global_non_owner_empty_pad_emits_zero_rows() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(type_code::I64, 1)),
        &[],
        &[G_SUM, G_COUNT],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw = g_reduce(
        &g_delta(&[]),
        &[],
        &mut to_ch,
        &[G_SUM, G_COUNT],
        false, // not the V₀ owner
    );
    assert_eq!(raw.count, 0, "non-owner empty pad must emit a zero-row batch");
}

/// Create over a non-empty source emits one computed row and NO ground.
#[test]
fn global_create_over_nonempty_emits_computed_no_ground() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(type_code::I64, 1)),
        &[],
        &[G_SUM, G_COUNT],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw = g_reduce(
        &g_delta(&[(1, 1, 5), (2, 1, 10)]),
        &[],
        &mut to_ch,
        &[G_SUM, G_COUNT],
        true,
    );
    assert_eq!(raw.count, 1, "one computed row, no ground");
    assert_eq!(read_i64_le(raw.col_data(0), 0), 15, "SUM=15");
    assert_eq!(read_i64_le(raw.col_data(1), 0), 2, "COUNT=2");
    assert_eq!(raw.get_null_word(0) & 1, 0, "SUM present (not NULL)");
}

/// Fully retracting an all-linear global aggregate sheds the computed row and the
/// ground branch supplies one NULL/zero row in its place (net = one ground row).
#[test]
fn global_emptied_by_delete_emits_ground() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(type_code::I64, 1)),
        &[],
        &[G_SUM, G_COUNT],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw1 = g_reduce(&g_delta(&[(1, 1, 5)]), &[], &mut to_ch, &[G_SUM, G_COUNT], true);
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 5, "SUM=5");

    // Retract the only row → cardinality 0.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(&g_delta(&[(1, -1, 5)]), &[], &mut to_ch2, &[G_SUM, G_COUNT], true);

    // Retract old computed (-1) + emit ground (+1) = 2 rows; net view = ground.
    assert_eq!(raw2.count, 2, "retraction of old + ground insert");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(raw2.get_null_word(pos) & 1, 1, "ground SUM=NULL");
    assert_eq!(read_i64_le(raw2.col_data(1), pos * 8), 0, "ground COUNT=0");
}

/// A seeded ground transitions to a computed row when the source is populated:
/// the ground at V₀ is retracted and the computed row replaces it.
#[test]
fn global_ground_to_computed_on_first_insert() {
    let out_schema = out_schema_for(
        &u64_pk_schema(SchemaColumn::new(type_code::I64, 1)),
        &[],
        &[G_SUM, G_COUNT],
    );
    // Tick 1: seed the ground over an empty source.
    let mut to_ch = empty_trace(out_schema);
    let ground = g_reduce(&g_delta(&[]), &[], &mut to_ch, &[G_SUM, G_COUNT], true);
    assert_eq!(ground.count, 1);

    // Tick 2: insert a row; trace_out holds the ground.
    let mut to_ch2 = trace_cursor(ground, out_schema);
    let raw2 = g_reduce(&g_delta(&[(1, 1, 7)]), &[], &mut to_ch2, &[G_SUM, G_COUNT], true);

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
        &u64_pk_schema(SchemaColumn::new(type_code::I64, 1)),
        &[],
        &[G_SUM, G_COUNT],
    );
    let mut to_ch = empty_trace(out_schema);
    let raw1 = g_reduce(&g_delta(&[(1, 1, 5)]), &[], &mut to_ch, &[G_SUM, G_COUNT], true);
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 5);

    // Change pk1's value 5 → 8 (retract old, insert new) — cardinality stays 1.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(
        &g_delta(&[(1, -1, 5), (1, 1, 8)]),
        &[],
        &mut to_ch2,
        &[G_SUM, G_COUNT],
        true,
    );
    assert_eq!(raw2.count, 2, "retract old computed + emit new computed");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    // The +1 row is the new computed value, never a NULL ground.
    assert_eq!(read_i64_le(raw2.col_data(0), pos * 8), 8, "new SUM=8");
    assert_eq!(read_i64_le(raw2.col_data(1), pos * 8), 1, "COUNT stays 1");
    assert_eq!(raw2.get_null_word(pos) & 1, 0, "no NULL ground delta on a value change");
}

/// Mixed non-linear `[COUNT(*), MIN(x)]` fully emptied → one ground row
/// `COUNT=0, MIN=NULL`, never a `COUNT=−N` zombie.
#[test]
fn global_mixed_count_min_emptied_emits_ground() {
    let out_schema = g_out_count_min();
    let aggs = [G_COUNT, G_MIN];

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
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // Tick 1: insert val=5 and val=3 → MIN=3.
    let mut to_ch = empty_trace(out_schema);
    let d1 = g_delta(&[(1, 1, 5), (2, 1, 3)]);
    let raw1 = g_reduce(&d1, &[&d1], &mut to_ch, &[G_MIN], true);
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 3, "MIN=3");

    // Tick 2: retract the current min (val=3) → MIN advances to 5.
    let d2 = g_delta(&[(2, -1, 3)]);
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_reduce(&d2, &[&d1, &d2], &mut to_ch2, &[G_MIN], true);
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
    let in_schema = u64_pk_schema(SchemaColumn::new(type_code::I64, 1));
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // AVI for empty group cols: key = av_encoded(8) only (0-byte group prefix).
    let avi_schema = avi_schema(&in_schema, &[]);
    assert_eq!(avi_schema.pk_stride(), 9, "0 group + 1 ordinal + 8 av");
    let avi_with = |min: i64| {
        let mut b = Batch::with_capacity(&avi_schema, 1);
        let av = i64_av(min);
        let mut key = [0u8; 9];
        key[0] = 0; // ordinal 0 (single MIN, empty group prefix)
        key[1..9].copy_from_slice(&av.to_be_bytes());
        b.extend_pk_bytes(&key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
        b
    };

    let g_min_avi = |delta: &Batch, to: &mut crate::storage::ReadCursor, avi: i64| {
        let mut avi_ch = trace_cursor(avi_with(avi), avi_schema);
        op_reduce(
            delta,
            to,
            &in_schema,
            &[], // empty group cols
            &[G_MIN],
            Some(&mut avi_ch),
            true,
            true,
        )
    };

    // Tick 1: delta val=50 is NOT the min; the AVI holds the true global min 10,
    // so a correct result can only come from the 0-byte-prefix index seek.
    let mut to_ch = empty_trace(out_schema);
    let raw1 = g_min_avi(&g_delta(&[(1, 1, 50)]), &mut to_ch, 10);
    let mb1 = raw1.as_mem_batch();
    let p1 = (0..raw1.count).find(|&i| mb1.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(raw1.col_data(0), p1 * 8),
        10,
        "AVI empty-prefix seek returns the GLOBAL min, not the delta's 50"
    );

    // Tick 2: a retraction re-evaluates the group; the AVI post-state is 20.
    let mut to_ch2 = trace_cursor(raw1, out_schema);
    let raw2 = g_min_avi(&g_delta(&[(2, -1, 10)]), &mut to_ch2, 20);
    let mb2 = raw2.as_mem_batch();
    let p2 = (0..raw2.count).find(|&i| mb2.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(raw2.col_data(0), p2 * 8),
        20,
        "AVI advances to the next-best post-state on retraction"
    );
}

// ---------------------------------------------------------------------------
// SumZero aggregate + two-phase global-aggregate combine
//
// SumZero = Sum's fold with Count's `0` identity: it sums its source column like
// SUM but renders an untouched accumulator as a concrete `0` (null bit clear),
// not NULL. The two-phase global-aggregate combine uses it to sum per-worker
// partial COUNT/COUNT_NON_NULL columns (a COUNT's empty value is `0`, not NULL).
// ---------------------------------------------------------------------------

/// SumZero folds values exactly like Sum, is linear (keeps the combine on the
/// linear fast path), and an untouched accumulator renders `0`, not NULL.
#[test]
fn sumzero_folds_like_sum_and_empty_renders_zero() {
    assert!(AggFunc::SumZero.is_linear(), "SumZero must be linear");

    let schema = u64_pk_schema(SchemaColumn::new(type_code::I64, 1));
    let desc = AggDescriptor { col_idx: 1, agg_op: AggFunc::SumZero };
    let mut acc = make_acc(&schema, &[0], desc);

    // Fresh: untouched, and renders 0 (Count's identity) rather than NULL (Sum's).
    assert!(acc.is_untouched(), "fresh SumZero is untouched");
    assert!(acc.empty_renders_zero(), "an untouched SumZero renders 0, not NULL");

    // Folds values with weight, exactly like Sum: 5·(+1) + 10·(+1) + 7·(−1) = 8.
    let batch = g_delta(&[(1, 1, 5), (2, 1, 10), (3, -1, 7)]);
    let mb = batch.as_mem_batch();
    for row in 0..batch.count {
        acc.step_from_batch(&mb, row, mb.get_weight(row));
    }
    assert!(!acc.is_untouched(), "SumZero with input is touched");
    assert_eq!(
        acc.value_bits() as i64,
        8,
        "SumZero sums its source values (5 + 10 − 7)"
    );
}

/// `COUNT(col)` over an all-NULL group renders a concrete `0` with the null bit
/// **clear**, never NULL. Every row hits `step_from_batch`'s null gate, so the
/// CountNonNull accumulator stays untouched; the emitted column's null bit is the
/// only observable distinguishing `0` from NULL (the value bytes are zero either
/// way). Regression guard for the COUNT-family-renders-NULL emitter bug.
#[test]
fn count_non_null_all_null_group_renders_zero_null_clear() {
    let in_schema = u64_pk_schema(SchemaColumn::new(type_code::I64, 1)); // [U64 pk, I64 payload(nullable)]
    let desc = AggDescriptor {
        col_idx: 1,
        agg_op: AggFunc::CountNonNull,
    };
    let mut acc = make_acc(&in_schema, &[0], desc);

    // Two rows whose payload column (payload slot 0) is NULL.
    let mut batch = Batch::with_capacity(&u64_pk_schema(SchemaColumn::new(type_code::I64, 1)), 2);
    for pk in [1u128, 2u128] {
        batch.extend_pk(pk);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&1u64.to_le_bytes()); // payload slot 0 = NULL
        batch.extend_col(0, &0i64.to_le_bytes());
        batch.count += 1;
    }
    batch.set_layout_unchecked(Layout::Consolidated);
    let mb = batch.as_mem_batch();
    for row in 0..batch.count {
        acc.step_from_batch(&mb, row, mb.get_weight(row));
    }
    assert!(acc.is_untouched(), "all-NULL group leaves CountNonNull untouched");

    // Emit the group row. Natural-PK grouping on the U64 PK col: output is
    // [U64 pk, I64 count], no group-exemplar column.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let mut output = Batch::with_capacity(&out_schema, 1);
    let accs = vec![acc];
    let plan = make_plan(&in_schema, &[0u32], std::slice::from_ref(&desc), false, false);
    emit_reduce_row(&mut output, (&mb, 0), mb.get_pk_bytes(0), &accs, &plan);

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

/// `emit_global_ground` still renders the COUNT family as a concrete `0` with the
/// null bit clear after the explicit COUNT seed loop was deleted — the untouched
/// accumulators now render via `empty_renders_zero`. Guards that deletion for both
/// COUNT(*) and COUNT(col) ground columns.
#[test]
fn emit_global_ground_renders_count_family_zero_null_clear() {
    let in_schema = u64_pk_schema(SchemaColumn::new(type_code::I64, 1));
    // Global-aggregate output: [_group_pk:U128, count_star:I64, count_col:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let descs = [
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
        AggDescriptor {
            col_idx: 1,
            agg_op: AggFunc::CountNonNull,
        },
    ];
    let mut raw_output = Batch::with_capacity(&out_schema, 1);
    let v0 = [0u8; 16]; // U128 ground PK (V₀)
    let plan = make_plan(&in_schema, &[], &descs, true, true);
    emit_global_ground(&mut raw_output, &v0, &plan);

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

/// Combine-input partial schema: `[_group_pk:U128, cnt:I64(nullable)]` — one
/// per-worker partial count column, all rows at PK V₀ (the local reduce output).
fn combine_partial_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// Build a combine-input delta of partials at PK V₀ from `(weight, Option<value>)`
/// (a `None` value is a NULL partial count column).
fn combine_partials(rows: &[(i64, Option<i64>)]) -> Batch {
    let schema = combine_partial_schema();
    let v0 = gnitz_wire::global_group_key();
    let mut b = Batch::with_capacity(&schema, rows.len().max(1));
    for &(w, val) in rows {
        b.extend_pk(v0);
        b.extend_weight(&w.to_le_bytes());
        match val {
            Some(v) => {
                b.extend_null_bmp(&0u64.to_le_bytes());
                b.extend_col(0, &v.to_le_bytes());
            }
            None => {
                b.extend_null_bmp(&1u64.to_le_bytes()); // cnt (payload idx 0) NULL
                b.extend_col(0, &0i64.to_le_bytes());
            }
        }
        b.count += 1;
    }
    b
}

// SumZero merges the partial counts; a trailing Count counts partial rows (the
// existence gate `op_reduce` finds via the lone AggFunc::Count).
const C_SUMZERO: AggDescriptor = AggDescriptor { col_idx: 1, agg_op: AggFunc::SumZero };
const C_COUNT_PARTIALS: AggDescriptor = AggDescriptor { col_idx: 0, agg_op: AggFunc::Count };

/// The phase-3 combine `op_reduce` over hand-built partials (empty group cols →
/// one global group at V₀, `global_ground = true`, `i_am_owner = true`).
fn combine_reduce(delta: &Batch, trace_out: &mut crate::storage::ReadCursor) -> Batch {
    let in_schema = combine_partial_schema();
    op_reduce(
        delta,
        trace_out,
        &in_schema,
        &[], // empty group cols ⇒ one global group at V₀
        &[C_SUMZERO, C_COUNT_PARTIALS],
        None,
        true, // global_ground
        true, // i_am_owner (V₀'s owner)
    )
}

/// The combine sums the partial *count values* (3 + 5 = 8), not the number of
/// partials (2). Consolidation of identical partials nets by weight.
#[test]
fn combine_sums_partial_counts_not_partial_rows() {
    let out_schema = out_schema_for(&combine_partial_schema(), &[], &[C_SUMZERO, C_COUNT_PARTIALS]);
    let mut to_ch = empty_trace(out_schema);

    // Two workers' partials: counts 3 and 5.
    let out = combine_reduce(&combine_partials(&[(1, Some(3)), (1, Some(5))]), &mut to_ch);
    assert_eq!(out.count, 1, "one combined row");
    assert_eq!(out.get_pk(0), gnitz_wire::global_group_key(), "combined row at V₀");
    assert_eq!(
        read_i64_le(out.col_data(0), 0),
        8,
        "SumZero sums partial counts (3 + 5), not the partial count (#partials = 2)"
    );
    assert_eq!(read_i64_le(out.col_data(1), 0), 2, "COUNT-of-partials gate = 2");
}

/// All-NULL partials (every worker's COUNT(col) is NULL — a fresh all-NULL
/// column) combine to a concrete `0` with the null bit clear, never NULL.
#[test]
fn combine_all_null_partials_render_zero_not_null() {
    let out_schema = out_schema_for(&combine_partial_schema(), &[], &[C_SUMZERO, C_COUNT_PARTIALS]);
    let mut to_ch = empty_trace(out_schema);

    // Two non-empty workers, each with a NULL partial count → SumZero untouched.
    let out = combine_reduce(&combine_partials(&[(1, None), (1, None)]), &mut to_ch);
    assert_eq!(out.count, 1, "non-empty global (2 partials) emits one combined row");
    assert_eq!(read_i64_le(out.col_data(0), 0), 0, "all-NULL COUNT(col) combines to 0");
    assert_eq!(
        out.get_null_word(0) & 1,
        0,
        "combined COUNT(col) is 0 with null bit CLEAR, not NULL"
    );
    assert_eq!(
        read_i64_le(out.col_data(1), 0),
        2,
        "still 2 partials (global non-empty)"
    );
}

/// Full retraction nets the COUNT-of-partials to 0; the gate sheds the computed
/// row and the combine emits the ground (combined = 0 via SumZero, null clear).
#[test]
fn combine_full_retraction_sheds_to_ground() {
    let out_schema = out_schema_for(&combine_partial_schema(), &[], &[C_SUMZERO, C_COUNT_PARTIALS]);
    let mut to_ch = empty_trace(out_schema);

    let out1 = combine_reduce(&combine_partials(&[(1, Some(5))]), &mut to_ch);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 5, "combined = 5");

    // Retract the only partial → COUNT-of-partials nets to 0.
    let mut to_ch2 = trace_cursor(out1, out_schema);
    let out2 = combine_reduce(&combine_partials(&[(-1, Some(5))]), &mut to_ch2);
    assert_eq!(out2.count, 2, "retract old computed (−1) + ground insert (+1)");
    let mb = out2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(out2.col_data(0), pos * 8),
        0,
        "ground combined count = 0 (SumZero's `0` identity)"
    );
    assert_eq!(
        out2.get_null_word(pos) & 1,
        0,
        "combined count present (SumZero grounds to 0, not NULL)"
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
    let mut t = scratch_table(dir.to_str().unwrap(), avi_schema, 0);
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Raw delta over `cg_src()` from `(pk, w, g, a, b)` rows.
fn cg_delta(rows: &[(u64, i64, i32, i64, i64)]) -> Batch {
    let s = cg_src();
    let mut b = Batch::with_capacity(&s, rows.len().max(1));
    for &(pk, w, g, a, bb) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(s.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(s.try_payload_idx(2).unwrap(), &a.to_le_bytes());
        b.extend_col(s.try_payload_idx(3).unwrap(), &bb.to_le_bytes());
        b.count += 1;
    }
    b
}

/// Foreign-group regression: a source PK appearing in two groups must not leak
/// one group's rows into the other's extremes. The GI over-read this fix removes
/// would corrupt MIN/MAX here; the combined index isolates each group by its key
/// prefix. The delta values are deliberately interleaved so a correct result can
/// only come from per-group isolation.
#[test]
fn reduce_multi_avi_foreign_group() {
    let in_schema = cg_src();
    // Output: [_group_pk:U128, g:I32, min_a:I64(nullable), max_b:I64(nullable), count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Max },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
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

    let out = op_reduce(
        &delta,
        &mut to_ch,
        &in_schema,
        &[1u32],
        &aggs,
        Some(&mut avi_ch),
        false,
        false,
    );

    assert_eq!(out.count, 2, "two groups → two rows");
    for i in 0..out.count {
        let g = gnitz_wire::read_u32_le(out.col_data(0), i * 4) as i32;
        let min_a = read_i64_le(out.col_data(1), i * 8);
        let max_b = read_i64_le(out.col_data(2), i * 8);
        let count = read_i64_le(out.col_data(3), i * 8);
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

/// Schema `[pk:U64, g:I32, a:I64]`; output `[_group_pk:U128, g:I32, min:I64?, count:I64]`.
fn cg3_src() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}
fn cg3_out() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}
/// `(pk, w, g, a, a_null)` delta over `cg3_src()`.
fn cg3_delta(rows: &[(u64, i64, i32, i64, bool)]) -> Batch {
    let s = cg3_src();
    let mut b = Batch::with_capacity(&s, rows.len().max(1));
    for &(pk, w, g, a, a_null) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        let null_word = if a_null {
            1u64 << s.try_payload_idx(2).unwrap()
        } else {
            0
        };
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(s.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(s.try_payload_idx(2).unwrap(), &a.to_le_bytes());
        b.count += 1;
    }
    b
}
const CG3_MIN: AggDescriptor = AggDescriptor { col_idx: 2, agg_op: AggFunc::Min };
const CG3_COUNT: AggDescriptor = AggDescriptor { col_idx: 0, agg_op: AggFunc::Count };

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
    op_reduce(
        delta,
        trace_out,
        &in_schema,
        &[1u32],
        aggs,
        Some(&mut avi_ch),
        false,
        false,
    )
}

/// Linear companion folded alongside the indexed MIN: COUNT = old + Σdelta, MIN
/// from the index, across an insert then a partial retraction.
#[test]
fn reduce_multi_avi_linear_companion() {
    let aggs = [CG3_MIN, CG3_COUNT];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Tick 1: insert g=7 {a=5, a=8, a=3}.
    let d1 = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 8, false), (3, 1, 7, 3, false)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(1), 0), 3, "MIN=3 from the index");
    assert_eq!(read_i64_le(out1.col_data(2), 0), 3, "COUNT=3");

    // Tick 2: retract a=3 (the current min). MIN→5 (index post-state), COUNT 3→2.
    let d2 = cg3_delta(&[(3, -1, 7, 3, false)]);
    let mut to2 = trace_cursor(out1, out_schema);
    let out2 = cg3_tick(tmp.path(), &[&d1, &d2], &d2, &aggs, &mut to2);
    // retract old (MIN=3,count=3 @ -1) + insert new (MIN=5,count=2 @ +1).
    let mb = out2.as_mem_batch();
    let ins = (0..out2.count).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(out2.col_data(1), ins * 8),
        5,
        "MIN recomputed to 5 from the index"
    );
    assert_eq!(
        read_i64_le(out2.col_data(2), ins * 8),
        2,
        "COUNT = old(3) + Σdelta(-1) = 2"
    );
}

/// Phantom-bug regression: a group fully retracted in a tick must emit exactly
/// one −1 retraction and NO +1 (the folded companion COUNT nets to 0, which the
/// cardinality gate reads to suppress the row).
#[test]
fn reduce_multi_avi_emptied_with_companion() {
    let aggs = [CG3_MIN, CG3_COUNT];
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
    let aggs = [CG3_MIN, CG3_COUNT];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Group 7 with two rows, both a = NULL.
    let d1 = cg3_delta(&[(1, 1, 7, 0, true), (2, 1, 7, 0, true)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1, "all-NULL group with rows must still emit");
    assert_eq!(out1.get_weight(0), 1);
    assert_eq!(read_i64_le(out1.col_data(2), 0), 2, "COUNT=2");
    // MIN null bit: payload col 1 (min) → null-word bit 1.
    assert_ne!(
        out1.get_null_word(0) & (1 << 1),
        0,
        "MIN renders NULL for all-NULL group"
    );
}

/// Nullable MIN over the combined index: a mixed group `{a=5, a=NULL}` whose
/// non-null row is retracted must SURVIVE as `(g, NULL, 1)` — not be dropped —
/// then disappear when the last (NULL) row is retracted. Regression for the
/// all-NULL-group drop the cardinality gate fixes, while the nullable MIN stays
/// on the combined index.
#[test]
fn reduce_multi_avi_retract_to_all_null() {
    let aggs = [CG3_MIN, CG3_COUNT];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Tick 1: g=7 = {a=5, a=NULL}. MIN=5, COUNT=2.
    let d1 = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 0, true)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(1), 0), 5, "MIN=5");

    // Tick 2: retract a=5. Group survives via the NULL row: (g, NULL, 1).
    let d2 = cg3_delta(&[(1, -1, 7, 5, false)]);
    let mut to2 = trace_cursor(out1, out_schema);
    let out2 = cg3_tick(tmp.path(), &[&d1, &d2], &d2, &aggs, &mut to2);
    let mb2 = out2.as_mem_batch();
    let ins = (0..out2.count)
        .find(|&i| mb2.get_weight(i) == 1)
        .expect("a +1 row — group survives");
    assert_eq!(read_i64_le(out2.col_data(2), ins * 8), 1, "COUNT=1 (the NULL row)");
    assert_ne!(out2.get_null_word(ins) & (1 << 1), 0, "MIN now NULL but group present");

    // Rebuild tick-2 output row to seed tick-3 trace_out.
    let row2 = {
        let mut b = Batch::with_capacity(&out_schema, 1);
        // copy the +1 row out of out2
        b.extend_pk_bytes(out2.get_pk_bytes(ins));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&out2.get_null_word(ins).to_le_bytes());
        b.extend_col(0, &out2.col_data(0)[ins * 4..ins * 4 + 4]);
        b.extend_col(1, &out2.col_data(1)[ins * 8..ins * 8 + 8]);
        b.extend_col(2, &out2.col_data(2)[ins * 8..ins * 8 + 8]);
        b.count += 1;
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

/// `MIN(a), MAX(a)` over the SAME column: two ordinals (opposite `for_max`) in
/// one combined index. The ordinal byte keeps them from colliding.
#[test]
fn reduce_multi_avi_same_col_min_max() {
    let in_schema = cg3_src();
    // Output: [_group_pk:U128, g:I32, min:I64?, max:I64?, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Max },
        CG3_COUNT,
    ];
    let delta = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 8, false), (3, 1, 7, 1, false)]);

    let tmp = tempfile::tempdir().unwrap();
    let avi_t = build_combined_avi(tmp.path(), &in_schema, &[1u32], &aggs, &[&delta]);
    let mut avi_ch = avi_t.open_cursor();
    let mut to = empty_trace(out_schema);
    let out = op_reduce(
        &delta,
        &mut to,
        &in_schema,
        &[1u32],
        &aggs,
        Some(&mut avi_ch),
        false,
        false,
    );
    assert_eq!(out.count, 1);
    assert_eq!(read_i64_le(out.col_data(1), 0), 1, "MIN(a)=1 (ordinal 0)");
    assert_eq!(
        read_i64_le(out.col_data(2), 0),
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // Synthetic group (g1, g2): [_group_pk:U128, g1:I32, g2:I32, min:I64?, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ];
    let delta = {
        let mut b = Batch::with_capacity(&in_schema, 4);
        for (pk, g1, g2, a) in [(1u64, 1i32, 1i32, 9i64), (2, 1, 1, 4), (3, 1, 2, 7), (4, 1, 2, 2)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g1.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &g2.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(3).unwrap(), &a.to_le_bytes());
            b.count += 1;
        }
        b
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

/// Global `MIN(a), SUM(b)` (no GROUP BY) served by the combined index, fully
/// retracted: the emptied source emits the ground row (MIN=NULL, SUM=NULL,
/// COUNT=0) — NOT a phantom SUM=0. Guards the `|| global_ground` disjunct of
/// `cardinality_idx`.
#[test]
fn reduce_multi_avi_global_emptied() {
    // [pk:U64, a:I64, b:I64]; global aggregate.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // Output: [_group_pk:U128, min:I64?, sum:I64?, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ];
    let mk = |rows: &[(u64, i64, i64, i64)]| {
        let mut b = Batch::with_capacity(&in_schema, rows.len().max(1));
        for &(pk, w, a, bb) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &a.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &bb.to_le_bytes());
            b.count += 1;
        }
        b
    };
    let tmp = tempfile::tempdir().unwrap();

    let run = |avi_deltas: &[&Batch], delta: &Batch, to: &mut crate::storage::ReadCursor| -> Batch {
        let avi_t = build_combined_avi(tmp.path(), &in_schema, &[], &aggs, avi_deltas);
        let mut avi_ch = avi_t.open_cursor();
        op_reduce(delta, to, &in_schema, &[], &aggs, Some(&mut avi_ch), true, true)
    };

    // Tick 1: insert {a=5,b=10},{a=3,b=20}. MIN=3, SUM=30, COUNT=2.
    let d1 = mk(&[(1, 1, 5, 10), (2, 1, 3, 20)]);
    let mut to1 = empty_trace(out_schema);
    let out1 = run(&[&d1], &d1, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 3, "MIN=3");
    assert_eq!(read_i64_le(out1.col_data(1), 0), 30, "SUM=30");

    // Tick 2: retract everything → ground row (MIN=NULL, SUM=NULL, COUNT=0).
    let d2 = mk(&[(1, -1, 5, 10), (2, -1, 3, 20)]);
    let mut to2 = trace_cursor(out1, out_schema);
    let out2 = run(&[&d1, &d2], &d2, &mut to2);
    let mb = out2.as_mem_batch();
    let ins = (0..out2.count)
        .find(|&i| mb.get_weight(i) == 1)
        .expect("a +1 ground row");
    assert_ne!(out2.get_null_word(ins) & (1 << 0), 0, "ground MIN = NULL");
    assert_ne!(
        out2.get_null_word(ins) & (1 << 1),
        0,
        "ground SUM = NULL (not a phantom 0)"
    );
    assert_eq!(read_i64_le(out2.col_data(2), ins * 8), 0, "ground COUNT = 0");
}

// ===========================================================================
// The ascending trace_out probe, over every group-key arm.
//
// `op_reduce` visits groups in ascending output-PK order on every arm, which is
// what its one retraction probe, `seek_pk_group_ascending`, is sound under. These
// tests drive it across multi-epoch retraction (incl. sign-flip boundaries) and
// over the nullable/hash arm, force a multi-source trace cursor so the merge-mode
// gallop and the ascending tripwire run at scale, and pin the classifier.
// ===========================================================================

/// One input row over a `[U64 pk, <grp>, I64 val]` schema: `(pk, grp, val,
/// weight)`. `grp == None` marks a NULL group (nullable schemas only). Shared
/// by the monotone-probe tests below and the non-linear fallback tests above.
type GrpValRow = (u64, Option<i64>, i64, i64);

/// Output schema for a synthetic-U128-PK `SUM/COUNT` reduce:
/// `[U128 pk, <grp>, I64 sum(nullable), I64 count]`. The trailing count is the
/// cardinality companion every all-linear reduce carries.
fn sum_count_out_synthetic(grp_col: SchemaColumn) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            grp_col,
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build an input delta over `[U64 pk, <grp>, I64 val]` from `(pk, grp, val, w)`
/// rows; `grp == None` marks the group column NULL (nullable schemas only). The
/// grp column is written from an i64 image, which is byte-identical to the u64
/// image for the non-negative values the U64-group test uses.
fn build_grp_val_delta(schema: &SchemaDescriptor, rows: &[GrpValRow]) -> Batch {
    let g_pi = schema.try_payload_idx(1).unwrap();
    let v_pi = schema.try_payload_idx(2).unwrap();
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, grp, val, w) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        let null_word = if grp.is_none() { 1u64 << g_pi } else { 0 };
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(g_pi, &grp.unwrap_or(0).to_le_bytes());
        b.extend_col(v_pi, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
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
    let mut trace = scratch_table(dir.to_str().unwrap(), *out_schema, 0);
    let mut max_sources = 0usize;
    for (i, d) in epochs.iter().enumerate() {
        // Sources the cursor for THIS epoch's probe sees: memtable runs + folded
        // in-memory runs + shard files (each becomes one CursorSource).
        let sources = trace.runs().count();
        max_sources = max_sources.max(sources);
        let out = {
            let mut ch = trace.open_cursor();
            op_reduce(d, &mut ch, in_schema, group_by, aggs, None, false, false)
        };
        trace.ingest_owned_batch(out).unwrap();
        if flush_after_first && i == 0 {
            trace.flush().unwrap();
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

// Payload I64 group key, multi-epoch retraction across the sign flip. GROUP BY
// a non-nullable I64 payload column ⇒ canonical Payload-arm key + synthetic
// U128 output PK. Epoch 1 inserts every
// group; epoch 2 updates and deletes rows in several groups (including the
// negative ones), so the retraction probe galloping over sign-flipped output
// PKs must land on the exact old aggregate row.
#[test]
fn reduce_monotone_probe_payload_i64_group() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0), // grp (non-nullable payload) — Payload arm
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_synthetic(SchemaColumn::new(type_code::I64, 0));
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
        readback_synthetic_i64(&final_batch),
        reference,
        "incremental SUM/COUNT across sign-flip retraction must match the from-scratch reference",
    );
}

// Payload U64 group key — the unsigned arm. A non-nullable U64 payload column is
// natural-PK-eligible, so the output PK IS the group value (stride 8) and the
// key is Payload-arm canonical. Groups straddle the high byte (1 vs 256) so a
// byte-order slip in the gallop would misorder them.
#[test]
fn reduce_monotone_probe_payload_u64_group() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0), // grp (non-nullable payload) — natural PK
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    // Natural U64 output PK: [U64 pk(=grp), I64 sum(nullable), I64 count].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
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

// A nullable group column takes the hash arm, so the classifier returns None —
// but the visit order still ascends, which is what lets the one retraction probe
// serve every arm. Same retraction workload plus a NULL group (its own group);
// results must still match the from-scratch reference.
#[test]
fn reduce_nullable_group_takes_the_hash_arm() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1), // grp NULLABLE ⇒ hash arm
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_synthetic(SchemaColumn::new(type_code::I64, 1)); // grp nullable
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

// Pin the shared classifier: the two canonical single-column arms (a PK
// column, a non-nullable routable-int payload), and the hash fold (None) for
// nullable / STRING / float / multi-column / empty group sets.
// `GroupKeyCols::key_row` dispatches its fast path on the arm and `op_reduce`
// keys its monotone probe on `is_some()`, so a drift here is a correctness
// bug, not a perf one.
#[test]
fn single_col_canonical_group_key_predicate() {
    use super::super::group_key::single_col_canonical_group_key;
    let u64c = SchemaColumn::new(type_code::U64, 0);
    let i64c = SchemaColumn::new(type_code::I64, 0);
    let i64_null = SchemaColumn::new(type_code::I64, 1);
    let strc = SchemaColumn::new(type_code::STRING, 0);
    let f64c = SchemaColumn::new(type_code::F64, 0);
    let u128c = SchemaColumn::new(type_code::U128, 0);

    let check = |cols: &[SchemaColumn], pk: &[u32], gb: &[u32], expected: bool, label: &str| {
        let schema = SchemaDescriptor::new(cols, pk);
        assert_eq!(single_col_canonical_group_key(&schema, gb), expected, "{label}");
    };
    check(&[u64c, i64c], &[0], &[1], true, "non-nullable I64 payload");
    check(&[u64c, u64c], &[0], &[1], true, "non-nullable U64 payload");
    check(&[u64c, u128c], &[0], &[1], true, "U128 payload (routable_int)");
    check(&[u64c, i64c], &[0], &[0], true, "single PK column");
    check(&[u64c, i64_null], &[0], &[1], false, "nullable → hash arm");
    check(&[u64c, strc], &[0], &[1], false, "STRING → hash arm");
    check(&[u64c, f64c], &[0], &[1], false, "float → hash arm");
    check(&[u64c, i64c, i64c], &[0], &[1, 2], false, "multi-column → hash arm");
    check(&[u64c, i64c], &[0], &[], false, "empty (global) group set");
}

// Many groups per epoch over ≥ 3 trace_out sources. 200 groups spanning the sign
// flip (-100..99), six epochs each touching every group (insert / update /
// delete). One flush after epoch 0 plus accumulating memtable runs pushes the
// trace_out cursor into merge mode (≥ 3 sources, all live at open) from epoch 3 on, so the
// multi-source gallop (`seek_forward_multi`) and the debug ascending tripwire
// both execute across hundreds of monotone probes. Construction makes every
// group's final aggregate identical (SUM=10, COUNT=3), so any mis-landed
// retraction shows up as a wrong sum or an un-cancelled duplicate.
#[test]
fn reduce_monotone_probe_many_groups_multi_source() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0), // grp — Payload arm
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_synthetic(SchemaColumn::new(type_code::I64, 0));
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
        readback_synthetic_i64(&final_batch),
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

    let mut trace_out = scratch_table(dir, *out_schema, 0);
    let avi_schema = avi_schema(in_schema, group_by);
    let mut avi_t = scratch_table(dir, avi_schema, 2);

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
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ]
}

/// Raw (unconsolidated) input delta over `mm_in_schema()` from `(pk, grp, val, w)`
/// rows; `op_reduce` consolidates internally. `grp`/`val` are non-NULL.
fn build_mm_delta(rows: &[(u64, i64, i64, i64)]) -> Batch {
    let s = mm_in_schema();
    let g_pi = s.try_payload_idx(1).unwrap();
    let v_pi = s.try_payload_idx(2).unwrap();
    let mut b = Batch::with_capacity(&s, rows.len().max(1));
    for &(pk, grp, val, w) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(g_pi, &grp.to_le_bytes());
        b.extend_col(v_pi, &val.to_le_bytes());
        b.count += 1;
    }
    b
}

/// `grp → (min, max, count)` for a MIN/MAX/COUNT reduce, NULL extremes as `None`.
type MmState = std::collections::BTreeMap<i64, (Option<i64>, Option<i64>, i64)>;

/// Read a consolidated `mm_out_schema()` batch into `grp → (min, max, count)`,
/// a NULL min/max reading as `None`. Asserts one net-weight-1 row per group.
fn readback_mm(b: &Batch) -> MmState {
    let mut m = std::collections::BTreeMap::new();
    for r in 0..b.count {
        assert_eq!(b.get_weight(r), 1, "each live group is one net-weight-1 row");
        let nw = b.get_null_word(r);
        let grp = read_i64_le(b.col_data(0), r * 8);
        let min = (!gnitz_wire::null_word_get(nw, 1)).then(|| read_i64_le(b.col_data(1), r * 8));
        let max = (!gnitz_wire::null_word_get(nw, 2)).then(|| read_i64_le(b.col_data(2), r * 8));
        let count = read_i64_le(b.col_data(3), r * 8);
        assert!(m.insert(grp, (min, max, count)).is_none(), "one output row per group");
    }
    m
}

// Primary oracle: a multi-epoch stream of inserts, updates (retract+insert), and
// deletes over many groups. The AVI (probe-skip) path must equal a from-scratch
// group-by MIN/MAX/COUNT oracle after every epoch — weight-exact, not just row
// presence. An all-insert epoch into an existing group takes the skip path; a new
// group or any retraction/update forces a probe.
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
        let g_pi = s.try_payload_idx(1).unwrap();
        let v_pi = s.try_payload_idx(2).unwrap();
        let mut b = Batch::with_capacity(&s, 2);
        for pk in [1u64, 2] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&(1u64 << v_pi).to_le_bytes()); // val NULL
            b.extend_col(g_pi, &0i64.to_le_bytes());
            b.extend_col(v_pi, &0i64.to_le_bytes());
            b.count += 1;
        }
        b
    };
    let s = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &[delta], false);
    assert_eq!(
        readback_mm(&s[0])[&0],
        (None, None, 2),
        "all-NULL insert-only group: MIN/MAX render NULL, COUNT counts rows",
    );
}

// Cap: an *existing* group receives more than `SKIP_TRACK_CAP` all-insert rows in
// one epoch, tripping the pre-step cap so it force-probes (its partial pre-stepped
// accumulator is discarded by the probe) rather than folding. The cap trigger —
// not `!has_old` — is what's exercised; the result must match the from-scratch
// extreme, confirming the cap is correctness-neutral.
#[test]
fn avi_skip_cap_force_probes() {
    let in_schema = mm_in_schema();
    let aggs = mm_aggs();
    let out_schema = out_schema_for(&in_schema, &[1u32], &aggs);

    // Epoch 0 creates the group (extreme 500); epoch 1 inserts 200 > 128 rows into
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
// insert-only epoch: the integer MIN skips (fold `combine(old, pos)`), the float
// MAX probes (floats always probe) — both correct in the same pass.
#[test]
fn avi_skip_mixed_int_min_float_max() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0), // grp
            SchemaColumn::new(type_code::I64, 0), // ival (integer MIN, skips)
            SchemaColumn::new(type_code::F64, 0), // fval (float MAX, probes)
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1), // min(ival)
            SchemaColumn::new(type_code::F64, 1), // max(fval) widens to F64
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Max },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ];
    let build = |rows: &[(u64, i64, i64, f64, i64)]| -> Batch {
        let g_pi = in_schema.try_payload_idx(1).unwrap();
        let i_pi = in_schema.try_payload_idx(2).unwrap();
        let f_pi = in_schema.try_payload_idx(3).unwrap();
        let mut b = Batch::with_capacity(&in_schema, rows.len().max(1));
        for &(pk, grp, ival, fval, w) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(g_pi, &grp.to_le_bytes());
            b.extend_col(i_pi, &ival.to_le_bytes());
            b.extend_col(f_pi, &fval.to_bits().to_le_bytes());
            b.count += 1;
        }
        b
    };
    let epochs = [
        build(&[(1, 0, 5, 1.0, 1)]),
        build(&[(2, 0, 3, 2.0, 1)]), // all-insert: int MIN 5→3 (skip), float MAX 1.0→2.0 (probe)
    ];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, false);
    let read = |b: &Batch| -> (Option<i64>, Option<f64>, i64) {
        assert_eq!(b.count, 1);
        let nw = b.get_null_word(0);
        let min = (!gnitz_wire::null_word_get(nw, 1)).then(|| read_i64_le(b.col_data(1), 0));
        let max = (!gnitz_wire::null_word_get(nw, 2)).then(|| f64::from_bits(read_u64_le(b.col_data(2), 0)));
        let count = read_i64_le(b.col_data(3), 0);
        (min, max, count)
    };
    assert_eq!(
        read(&s[1]),
        (Some(3), Some(2.0), 2),
        "integer MIN skips and float MAX probes in one insert-only pass",
    );
}

/// Drive a float MIN/MAX reduce and check every epoch against a from-scratch
/// oracle over the accumulated rows, ordered by `total_cmp` — the order the
/// group comparator and the index's `ieee_order_bits` image both use, so ±0.0
/// stay distinct and NaN has a defined position.
fn check_float_minmax_against_oracle(val_tc: TypeCode, epochs_rows: &[Vec<(u64, i64, f64, i64)>]) {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(val_tc as u8, 0),
        ],
        &[0],
    );
    // Float MIN/MAX widen to F64 in the output, regardless of an F32 source.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::F64, 1),
            SchemaColumn::new(type_code::F64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Max },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ];
    let build = |rows: &[(u64, i64, f64, i64)]| -> Batch {
        let g_pi = in_schema.try_payload_idx(1).unwrap();
        let v_pi = in_schema.try_payload_idx(2).unwrap();
        let mut b = Batch::with_capacity(&in_schema, rows.len().max(1));
        for &(pk, grp, val, w) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(g_pi, &grp.to_le_bytes());
            if val_tc == TypeCode::F32 {
                b.extend_col(v_pi, &(val as f32).to_bits().to_le_bytes());
            } else {
                b.extend_col(v_pi, &val.to_bits().to_le_bytes());
            }
            b.count += 1;
        }
        b
    };
    let epochs: Vec<Batch> = epochs_rows.iter().map(|r| build(r)).collect();
    let states = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, false);

    // The oracle folds the same value the reduce sees: an F32 source is read
    // back at F64 width, so round it through f32 first.
    let widen = |v: f64| if val_tc == TypeCode::F32 { v as f32 as f64 } else { v };
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
                    read_i64_le(states[ep].col_data(0), i * 8),
                    (
                        read_u64_le(states[ep].col_data(1), i * 8),
                        read_u64_le(states[ep].col_data(2), i * 8),
                    ),
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
    let b_tc = if b_signed { type_code::I64 } else { type_code::U64 };
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // a (pk, group)
            SchemaColumn::new(b_tc, 0),           // b (pk, aggregated by MAX)
            SchemaColumn::new(type_code::I64, 0), // pad (payload)
        ],
        &[0, 1],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // a (natural PK)
            SchemaColumn::new(b_tc, 1),           // max(b) (nullable)
            SchemaColumn::new(type_code::I64, 0), // count
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Max },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ];
    let build = |rows: &[(u64, i64, i64)]| -> Batch {
        let pad_pi = in_schema.try_payload_idx(2).unwrap();
        let mut bt = Batch::with_capacity(&in_schema, rows.len().max(1));
        for &(a, b, w) in rows {
            bt.extend_pk_opk(&[a as u128, b as u128]);
            bt.extend_weight(&w.to_le_bytes());
            bt.extend_null_bmp(&0u64.to_le_bytes());
            bt.extend_col(pad_pi, &0i64.to_le_bytes());
            bt.count += 1;
        }
        bt
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Max },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ];
    let build = |rows: &[(u64, i64, i64)]| -> Batch {
        let v_pi = in_schema.try_payload_idx(1).unwrap();
        let mut b = Batch::with_capacity(&in_schema, rows.len().max(1));
        for &(pk, val, w) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(v_pi, &val.to_le_bytes());
            b.count += 1;
        }
        b
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
fn test_agg_output_type() {
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Count, type_code::I64),
        type_code::I64
    );
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Sum, type_code::F64),
        type_code::F64
    );
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Sum, type_code::I32),
        type_code::I64
    );
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Max, type_code::F32),
        type_code::F64
    );
    // MIN/MAX select an existing row, so they preserve the source type: every
    // ≤8-byte integer keeps its own type (no widening to I64).
    assert_eq!(gnitz_wire::agg_output_type(AggFunc::Min, type_code::I8), type_code::I8);
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Max, type_code::I16),
        type_code::I16
    );
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Min, type_code::I32),
        type_code::I32
    );
    assert_eq!(gnitz_wire::agg_output_type(AggFunc::Max, type_code::U8), type_code::U8);
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Min, type_code::U16),
        type_code::U16
    );
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Max, type_code::U32),
        type_code::U32
    );
    // U64 folds into the general rule (the source type *is* U64); SUM over a
    // U64 source is also typed U64 (the i64 accumulator bit pattern is the
    // correct unsigned sum), so a downstream unsigned compare re-seeds right.
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Min, type_code::U64),
        type_code::U64
    );
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Max, type_code::U64),
        type_code::U64
    );
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Sum, type_code::U64),
        type_code::U64
    );
    // A wide source keeps its type: MIN/MAX select one of its rows.
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Max, type_code::STRING),
        type_code::STRING
    );
    assert_eq!(
        gnitz_wire::agg_output_type(AggFunc::Min, type_code::U128),
        type_code::U128
    );
}

#[test]
fn test_build_reduce_output_schema_natural_pk() {
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0), // group col
            SchemaColumn::new(type_code::I64, 0), // agg col
        ],
        &[0],
    );
    let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum }];
    let out = build_reduce_output_schema(&input, &[1], &aggs, crate::schema::ReduceOutKey::SingleNaturalCol).unwrap();
    // Natural PK (single U64 group col) → [U64_PK, I64_agg]
    assert_eq!(out.num_columns(), 2);
    assert_eq!(out.columns[0].type_code, type_code::U64);
    assert_eq!(out.columns[1].type_code, type_code::I64);
}

#[test]
fn test_build_reduce_output_schema_compound_natural_pk() {
    // Input: pk_indices = [0, 1] (compound 2×U64), payload I64.
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AggFunc::Count }];
    // group_cols = [1, 0] — permuted; the set still equals pk_indices.
    let out = build_reduce_output_schema(&input, &[1, 0], &aggs, crate::schema::ReduceOutKey::PkPermutation).unwrap();
    // 2 PK cols + 1 agg col; pk_indices in source's pk-list order [0, 1].
    assert_eq!(out.num_columns(), 3);
    assert_eq!(out.pk_indices(), &[0, 1]);
    assert_eq!(out.columns[0].type_code, type_code::U64);
    assert_eq!(out.columns[1].type_code, type_code::U64);
    assert_eq!(out.columns[2].type_code, type_code::I64);
}

#[test]
fn test_build_reduce_output_schema_single_pk_group_by_pk() {
    // Single-PK input grouped by its PK must collapse to the single-column
    // natural-PK shape (one PK col + agg).
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = vec![AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum }];
    let out = build_reduce_output_schema(&input, &[0], &aggs, crate::schema::ReduceOutKey::PkPermutation).unwrap();
    assert_eq!(out.num_columns(), 2);
    assert_eq!(out.pk_indices(), &[0]);
    assert_eq!(out.columns[0].type_code, type_code::U64);
    assert_eq!(out.columns[1].type_code, type_code::I64);
}

#[test]
fn test_build_reduce_output_schema_synthetic_pk() {
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::STRING, 0), // group col
            SchemaColumn::new(type_code::I64, 0),    // agg col
        ],
        &[0],
    );
    let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AggFunc::Count }];
    let out = build_reduce_output_schema(&input, &[1], &aggs, crate::schema::ReduceOutKey::SyntheticFold).unwrap();
    // Synthetic PK (STRING group col) → [U128_hash, STRING_group, I64_count]
    assert_eq!(out.num_columns(), 3);
    assert_eq!(out.columns[0].type_code, type_code::U128);
    assert_eq!(out.columns[1].type_code, type_code::STRING);
    assert_eq!(out.columns[2].type_code, type_code::I64);
}

/// The reduce output schema must declare each aggregate column nullable exactly
/// when `emit_agg_col` can set its null bit: an untouched SUM/MIN/MAX, which a
/// NULL source value or an empty group set produces. The zero-identity family
/// (COUNT / COUNT_NON_NULL / SumZero) renders a concrete `0` and stays NOT NULL,
/// which is what keeps a COUNT-only or NOT-NULL-source grouped reduce on the
/// null-blind fixed-int comparator. This pins the schema builder's *application*
/// of the shared `AggFunc::raw_output_nullable`; the rule itself is pinned in
/// `gnitz-wire`. The `group_cols = &[]` arm is also the range-join threshold
/// reduce's shape (group-less MIN over a NOT NULL column).
#[test]
fn build_reduce_output_schema_agg_nullability_matrix() {
    for src_nullable in [false, true] {
        // Group by the payload `grp` (I64 is not a natural reduce key, so this is
        // the SyntheticFold shape).
        let input = u64pk_i64grp_i64val(src_nullable);
        for agg_op in [
            AggFunc::Count,
            AggFunc::CountNonNull,
            AggFunc::SumZero,
            AggFunc::Sum,
            AggFunc::Min,
            AggFunc::Max,
        ] {
            let aggs = vec![AggDescriptor { col_idx: 2, agg_op }];
            for group_cols in [&[1u32][..], &[][..]] {
                let out_key = input.reduce_out_key(group_cols);
                let out = build_reduce_output_schema(&input, group_cols, &aggs, out_key).unwrap();
                // Aggregates are the trailing output columns.
                let got = out.columns[out.num_columns() - 1].nullable != 0;
                let want = match agg_op {
                    AggFunc::Count | AggFunc::CountNonNull | AggFunc::SumZero => false,
                    AggFunc::Sum | AggFunc::Min | AggFunc::Max => src_nullable || group_cols.is_empty(),
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
    ReducePlan::from_wire(schema, group, aggs, false, false)
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
        "reduce: group column 200 out of range (2 cols)"
    );
    assert_eq!(
        plan_rejection(&schema, &[0], &count(200)),
        "reduce: aggregate column 200 out of range (2 cols)"
    );
}

/// col 0 = U64 PK and the whole group key (⇒ PkPermutation); col 1 = the
/// aggregate column, whose type is the only thing the two tests below vary.
fn agg_over(tc: u8) -> SchemaDescriptor {
    SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0])
}

/// A summing aggregate adds its argument in a scalar register (`ScalarKind`) —
/// the ≤8-byte int/float set — so a wide column has no accumulator for it.
/// Covers the low-level `CircuitBuilder` path that bypasses the SQL binder.
#[test]
fn a_summing_aggregate_over_a_non_scalar_column_is_rejected() {
    for agg_op in [AggFunc::Sum, AggFunc::SumZero] {
        let aggs = [AggDescriptor { agg_op, col_idx: 1 }];
        for tc in [type_code::U128, type_code::STRING] {
            assert_eq!(
                plan_rejection(&agg_over(tc), &[0], &aggs),
                "reduce: summed column type has no scalar register image",
                "{agg_op:?} over type code {tc}",
            );
        }
    }
}

/// The aggregates that read no value (COUNT) or select a whole row (MIN/MAX)
/// take every column type, wide ones included — the other half of the
/// eligibility rule.
#[test]
fn a_row_selecting_aggregate_takes_every_column_type() {
    for agg_op in [AggFunc::Count, AggFunc::Min, AggFunc::Max] {
        let aggs = [AggDescriptor { agg_op, col_idx: 1 }];
        for tc in [type_code::I64, type_code::U128, type_code::UUID, type_code::STRING] {
            assert!(
                ReducePlan::from_wire(&agg_over(tc), &[0], &aggs, false, false).is_ok(),
                "{agg_op:?} over type code {tc}"
            );
        }
    }
}
