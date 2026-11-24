use gnitz_expr::{ConstIdx, IntArithOp, LogicalInstr, LogicalProgram, Reg, Sink};
use gnitz_wire::{MAX_PK_COLUMNS, MapKind, NullKeys, TypeCode};

use super::MapPlan;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    cell, extended, keyed_schema, le_cell, make_batch, make_int_batch, make_schema_u64_i64, opk_pk, pk_only_schema,
    pk_payload_schema, rekey_plan, u64_pk_schema, weighted_rows,
};
use gnitz_wire::MAX_COLUMNS;

/// A U64 PK, then a nullable payload column per `payload` type.
pub(super) fn make_schema(payload: &[TypeCode]) -> SchemaDescriptor {
    u64_pk_schema(&payload.iter().map(|&t| SchemaColumn::new(t, true)).collect::<Vec<_>>())
}

/// A computed map of `program` over `in_schema` into the payload slots `out_cols`.
pub(super) fn compute(in_schema: &SchemaDescriptor, program: LogicalProgram, out_cols: &[(TypeCode, bool)]) -> MapPlan {
    MapPlan::from_compute_map(in_schema, &program.to_map(out_cols.to_vec())).unwrap()
}

/// A copy of `in_schema`'s columns `cols`, each slot at its source column's
/// type and nullability.
pub(super) fn project(in_schema: &SchemaDescriptor, cols: &[u32]) -> MapPlan {
    let slots: Vec<(TypeCode, bool)> = cols
        .iter()
        .map(|&c| {
            let col = in_schema.columns()[c as usize];
            (col.type_code, col.nullable)
        })
        .collect();
    compute(in_schema, LogicalProgram::copy_cols(cols), &slots)
}

/// Every row keeps its PK and weight, NULLs included, while copied and
/// computed columns fill the output slots in sink order.
#[test]
fn a_computed_map_copies_and_computes_every_row_at_its_weight() {
    let in_schema = make_schema(&[TypeCode::I64, TypeCode::I64]);
    let prog = LogicalProgram::new(
        vec![
            LogicalInstr::LoadCol { col: 1 },
            LogicalInstr::LoadCol { col: 2 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        vec![Sink::Col(2), Sink::Col(1), Sink::Reg(Reg(2))],
        vec![],
    )
    .unwrap();
    let mut plan = compute(&in_schema, prog, &[(TypeCode::I64, true); 3]);

    let input = make_int_batch(
        &in_schema,
        &[
            (1, 3, &[Some(10), Some(20)]),
            (2, -2, &[None, Some(40)]),
            (3, 1, &[Some(-5), Some(7)]),
        ],
    );
    let want = make_int_batch(
        plan.output_schema(),
        &[
            (1, 3, &[Some(20), Some(10), Some(30)]),
            (2, -2, &[Some(40), None, None]),
            (3, 1, &[Some(7), Some(-5), Some(2)]),
        ],
    );
    assert_eq!(
        weighted_rows(&plan.evaluate_map_batch(&input.as_mem_batch())),
        weighted_rows(&want)
    );

    let empty = plan.evaluate_map_batch(&Batch::empty_with_schema(&in_schema).as_mem_batch());
    assert_eq!((empty.len(), empty.schema()), (0, plan.output_schema()));
}

/// A projection of string columns, reordered, moves each row's content; one
/// that drops a heap-backed column relocates the strings it keeps and drops the
/// dead heap.
#[test]
fn a_string_projection_moves_content_and_drops_a_dead_heap() {
    let in_schema = make_schema(&[TypeCode::String, TypeCode::String]);
    let rows = [
        (1, ["ab", "long-string-one-xyz"]),
        (2, ["cd", "long-string-two-abcdef"]),
    ];
    let mut batch = BatchBuilder::new(&in_schema);
    for (pk, strs) in rows {
        batch.begin_row(pk, 1);
        strs.iter().for_each(|s| {
            batch.put_string(s);
        });
        batch.end_row();
    }
    let batch = batch.finish();
    for cols in [&[2, 1][..], &[1]] {
        let mut plan = project(&in_schema, cols);
        let out = plan.evaluate_map_batch(&batch.as_mem_batch());
        let mut want = BatchBuilder::new(plan.output_schema());
        for (pk, strs) in rows {
            want.begin_row(pk, 1);
            cols.iter().for_each(|&c| {
                want.put_string(strs[c as usize - 1]);
            });
            want.end_row();
        }
        assert_eq!(weighted_rows(&out), weighted_rows(&want.finish()), "{cols:?}");
        if cols == [1] {
            assert!(out.blob().len() < batch.blob().len(), "the dropped column's heap");
        }
    }
}

/// A copy of 64 payload columns, as many as a null word has bits, carries
/// every slot's value and NULL, the top slot's included.
#[test]
fn a_copy_of_64_payload_columns_carries_every_slot() {
    let s = make_schema(&[TypeCode::U64; 64]);
    let mut b = BatchBuilder::new(&s);
    for i in 0..40u64 {
        b.begin_row(i as u128, 1 + i as i64 % 3);
        for k in 0..64 {
            b.put_opt_int((!(i + k).is_multiple_of(5)).then_some((i * 100 + k) as u128));
        }
        b.end_row();
    }
    let src = b.finish();
    let out = project(&s, &(1..=64).collect::<Vec<_>>()).evaluate_map_batch(&src.as_mem_batch());
    assert_eq!(weighted_rows(&out), weighted_rows(&src));
}

/// A copied column holds each row's value in its slot — a PK column decoded
/// from its OPK image, a narrow column sign- or zero-extended by its own
/// signedness into a wider slot — whether it is the whole PK, one column of a
/// compound PK whose list order is not its column order, or a payload column;
/// the PK region and the weights carry over unchanged.
#[test]
fn a_copied_column_holds_each_rows_value_in_its_slot() {
    use TypeCode::{I8, I16, I32, I64, I128, U8, U16, U32, U64};
    let col = |tc| SchemaColumn::new(tc, false);
    for (src, slot) in [
        (I8, I8),
        (U16, U16),
        (I32, I32),
        (U64, U64),
        (I128, I128),
        (I8, I64),
        (U8, I16),
        (I16, I32),
        (U16, U64),
        (I32, I64),
        (U32, I64),
    ] {
        let (sw, dw) = (src.wire_stride(), slot.wire_stride());
        // Each width's ends and the values around zero, then a spread.
        let top = 1u128 << (8 * sw - 1);
        let vals: Vec<u128> = [0, 1, top, top - 1, u128::MAX]
            .into_iter()
            .chain((0..300u128).map(|i| (i as i128 - 150).wrapping_mul(7_654_321_987_654_321) as u128))
            .map(|v| v & gnitz_wire::image_mask(sw))
            .collect();
        let want: Vec<u8> = vals
            .iter()
            .flat_map(|&v| extended(src, v).to_le_bytes()[..dw].to_vec())
            .collect();
        let permuted = SchemaDescriptor::new(&[col(U16), col(src), col(U64)], &[2, 1, 0]);
        for (schema, c) in [
            (pk_payload_schema(&[src]), 0),
            (permuted, 1),
            (u64_pk_schema(&[col(src)]), 1),
        ] {
            let pk = schema.pk_cols();
            let payload: Vec<u32> = (0..schema.num_columns() as u32).filter(|ci| !pk.contains(ci)).collect();
            // Column `c` holds the value, every other its row number.
            let cell = |ci: u32, r: usize, v: u128| match ci == c {
                true => v,
                false => r as u128 & gnitz_wire::image_mask(schema.columns()[ci as usize].size()),
            };
            let mut batch = BatchBuilder::new(&schema);
            for (r, &v) in vals.iter().enumerate() {
                let natives: Vec<u128> = pk.iter().map(|&ci| cell(ci, r, v)).collect();
                batch.begin_row_natives(&natives, [1, -2, 3][r % 3]);
                payload.iter().for_each(|&ci| {
                    batch.put_int(cell(ci, r, v));
                });
                batch.end_row();
            }
            let batch = batch.finish();
            let out = compute(&schema, LogicalProgram::copy_cols(&[c]), &[(slot, false)])
                .evaluate_map_batch(&batch.as_mem_batch());
            let what = format!("{src} -> {slot}, PK {:?} column {c}", schema.pk_cols());
            assert_eq!(out.col_data(0), &want[..], "{what}");
            assert_eq!((out.pk(), out.weight()), (batch.pk(), batch.weight()), "{what}");
        }
    }
}

/// Survivor runs — short enough to gather into one range first or long enough
/// to map in place — map behind rows the output still holds, and onto an
/// output cleared of earlier rows, exactly as those rows alone map: no PK,
/// null word or string of another row shows through.
#[test]
fn appended_runs_map_as_their_rows_alone() {
    const N: usize = 600;
    let s = make_schema(&[TypeCode::I64, TypeCode::String, TypeCode::I64]);
    let mut b = BatchBuilder::new(&s);
    for i in 0..N as u64 {
        b.begin_row(i as u128, [1, -1, 2][i as usize % 3]);
        b.put_opt_int((i % 5 != 0).then_some(((i * 7 % 13) as i64 - 6) as u128));
        match i % 7 {
            0 => {
                b.put_null();
            }
            k if k % 2 == 0 => {
                b.put_string("short");
            }
            _ => {
                b.put_string(&format!("a heap-backed string {i}"));
            }
        }
        b.put_opt_int((i % 3 != 0).then_some(i as u128));
        b.end_row();
    }
    let src = b.finish();
    let sum = LogicalProgram::new(
        vec![
            LogicalInstr::LoadCol { col: 1 },
            LogicalInstr::LoadCol { col: 3 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        vec![Sink::Col(2), Sink::Reg(Reg(2))],
        vec![],
    )
    .unwrap();
    let plans = [
        (
            "compute",
            compute(&s, sum, &[(TypeCode::String, true), (TypeCode::I64, true)]),
        ),
        ("int projection", project(&s, &[3, 1])),
        ("string projection", project(&s, &[2, 1])),
        ("reindex", rekey_plan(&s, &[1], &[0, 1, 2, 3], NullKeys::Keep)),
        (
            "hash-row",
            MapPlan::from_wire(
                &s,
                &MapKind::HashRow {
                    cols: vec![(1, TypeCode::I64), (2, TypeCode::String)],
                },
            )
            .unwrap(),
        ),
    ];
    for (name, mut plan) in plans {
        for run in [1, 20, 30, 200] {
            let ranges: Vec<(usize, usize)> = (0..N).step_by(run + 3).map(|s| (s, (s + run).min(N))).collect();
            let rows: Vec<u32> = ranges.iter().flat_map(|&(s, e)| s as u32..e as u32).collect();
            let want = weighted_rows(&plan.evaluate_map_batch(&src.ascending_subset(&rows).as_mem_batch()));
            let mut out = Batch::empty_with_schema(plan.output_schema());
            plan.append_map_ranges(&src.as_mem_batch(), &mut out, &[(0, N)]);
            plan.append_map_ranges(&src.as_mem_batch(), &mut out, &ranges);
            let tail: Vec<u32> = (N as u32..out.len() as u32).collect();
            assert_eq!(
                weighted_rows(&out.ascending_subset(&tail)),
                want,
                "{name}, runs of {run}, behind held rows"
            );
            out.clear();
            plan.append_map_ranges(&src.as_mem_batch(), &mut out, &ranges);
            assert_eq!(
                weighted_rows(&out),
                want,
                "{name}, runs of {run}, onto a cleared output"
            );
        }
    }
}

/// A computed string beside a copy of its source column: the copy still
/// resolves against the blob the emit appends to, a heap-backed result lands in
/// the output's own blob, and a NULL result ships a zeroed computed cell, though
/// its lane computed bytes, so an insert and its retraction cancel.
#[test]
fn a_string_emit_composes_with_a_copy_of_its_source() {
    let in_schema = make_schema(&[TypeCode::String]);
    let long = b"a-long-value-past-twelve".as_slice();
    let mut batch = BatchBuilder::new(&in_schema);
    for (pk, s) in [(1u128, Some(long)), (2, Some(b"short".as_slice())), (3, None)] {
        batch.begin_row(pk, 1);
        match s {
            Some(s) => {
                batch.put_blob(s);
            }
            None => {
                batch.put_null();
            }
        }
        batch.end_row();
    }
    let batch = batch.finish();

    // `||` propagates the NULL source, over the suffix its lane still concatenates.
    let prog = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) },
            LogicalInstr::StrConcat { a: Reg(0), b: Reg(1), skip_null: false },
        ],
        vec![Sink::Col(1), Sink::Reg(Reg(2))],
        vec![b"-suffix".to_vec()],
    )
    .unwrap();
    let mut plan = compute(&in_schema, prog, &[(TypeCode::String, true); 2]);
    let out = plan.evaluate_map_batch(&batch.as_mem_batch());

    let mut want = BatchBuilder::new(plan.output_schema());
    for (pk, cells) in [
        (1u128, Some((long, b"a-long-value-past-twelve-suffix".as_slice()))),
        (2, Some((b"short", b"short-suffix"))),
        (3, None),
    ] {
        want.begin_row(pk, 1);
        match cells {
            Some((copy, cat)) => {
                want.put_blob(copy);
                want.put_blob(cat);
            }
            None => {
                want.put_null();
                want.put_null();
            }
        }
        want.end_row();
    }
    assert_eq!(weighted_rows(&out), weighted_rows(&want.finish()));
    assert_eq!(
        &out.col_data(1)[32..48],
        &[0u8; 16],
        "a NULL computed cell is all zeros"
    );
}

/// A reindex stamps each row's PK with the OPK image of its key column — the
/// sign flip included — and keeps the row's weight.
#[test]
fn a_reindex_keys_each_row_by_the_opk_image_of_its_key_column() {
    let schema = make_schema_u64_i64();
    let rows = [(1, 1, 200), (2, -3, 100), (3, 1, -300)];
    let batch = make_batch(&schema, &rows);
    let out = rekey_plan(&schema, &[1], &[1], NullKeys::Keep).evaluate_map_batch(&batch.as_mem_batch());
    assert_eq!(out.len(), rows.len());
    for (r, &(_, w, v)) in rows.iter().enumerate() {
        assert_eq!(
            out.get_pk_bytes(r),
            opk_pk(&pk_payload_schema(&[TypeCode::I64]), &[v as u128])
        );
        assert_eq!(out.get_weight(r), w);
        assert_eq!(gnitz_wire::read_i64_le(out.col_data(0), r * 8), v);
    }
}

/// A hash-row map keys a row by its payload *content*, not by its cell bytes:
/// equal rows must land on one PK, or EXCEPT/INTERSECT cannot cancel them, and
/// unequal rows must not, or two elements coalesce into one.
#[test]
fn hash_row_keys_a_row_by_its_content_across_nulls_strings_and_blobs() {
    const LONG_A: &[u8] = b"the-quick-brown-fox";
    // Equal in the 4 bytes the cell inlines as its prefix, different past them.
    const LONG_B: &[u8] = b"the-quick-brown-cat";

    // (payload value, is NULL, STRING content, BLOB content). Each cell is
    // encoded as its row is built, so equal content lands at unequal offsets.
    let rows: &[(i64, bool, &[u8], &[u8])] = &[
        (5, false, LONG_A, b"blob-past-twelve-bytes"),
        // 1: row 0 again.
        (5, false, LONG_A, b"blob-past-twelve-bytes"),
        // 2..=4: one column changed each.
        (7, false, LONG_A, b"blob-past-twelve-bytes"),
        (5, false, LONG_B, b"blob-past-twelve-bytes"),
        (5, false, LONG_A, b"blob-past-twelve-other"),
        // 5, 6: NULL payload cells whose underlying bytes differ.
        (111, true, LONG_A, b"blob-past-twelve-bytes"),
        (222, true, LONG_A, b"blob-past-twelve-bytes"),
        // 7, 8: inline cells, twice.
        (5, false, b"abc", b"abc"),
        (5, false, b"abc", b"abc"),
        // 9, 10: the two columns' contents swapped.
        (5, false, b"ab", b"cd"),
        (5, false, b"cd", b"ab"),
        // 11, 12: one content split at another byte between the columns.
        (5, false, b"ab", b"c"),
        (5, false, b"a", b"bc"),
    ];

    // The rows repeat past the map's key chunk, so every chunk must key alike.
    const REPEATS: usize = 60;
    let in_schema = make_schema(&[TypeCode::I64, TypeCode::String, TypeCode::Blob]);
    let mut batch = BatchBuilder::new(&in_schema);
    for row in 0..rows.len() * REPEATS {
        let (v, is_null, s, b) = rows[row % rows.len()];
        batch.begin_row(row as u128, 1);
        match is_null {
            true => {
                batch.put_null();
            }
            false => {
                batch.put_int(v as u128);
            }
        }
        batch.put_blob(s);
        batch.put_blob(b);
        batch.end_row();
    }
    let batch = batch.finish();

    let hash_row = MapKind::HashRow {
        cols: vec![(1, TypeCode::I64), (2, TypeCode::String), (3, TypeCode::Blob)],
    };
    let out = MapPlan::from_wire(&in_schema, &hash_row)
        .unwrap()
        .evaluate_map_batch(&batch.as_mem_batch());
    assert_eq!(out.len(), batch.len());
    let pk = |row: usize| out.get_pk_bytes(row);

    for row in rows.len()..out.len() {
        assert_eq!(pk(row), pk(row % rows.len()), "row {row} keys unlike its first copy");
    }
    assert_eq!(pk(0), pk(1), "equal content must key equally, at whatever heap offset");
    assert_eq!(
        pk(5),
        pk(6),
        "a NULL cell hashes as its marker, not as the bytes under it"
    );
    assert_eq!(pk(7), pk(8), "and so must two rows of inline cells");
    for (a, b, what) in [
        (0, 2, "a differing payload value"),
        (0, 3, "a long string differing only past its inline prefix"),
        (0, 4, "a differing BLOB beside an equal STRING"),
        (0, 5, "a NULL cell against a non-NULL one"),
        (0, 7, "inline cells against heap-backed ones"),
        (9, 10, "the two string columns' contents swapped"),
        (11, 12, "one content split at another byte"),
    ] {
        assert_ne!(pk(a), pk(b), "{what} must move the PK");
    }
}

/// Each way a client-supplied column list can overrun a schema array, address
/// an unnumbered copy slot, or promote outside the copy kernel's domain — and a
/// corrupt or over-wide computed map — is refused by name. A projection bound
/// counts the PK column the output carries.
#[test]
fn a_malformed_map_is_refused_by_name() {
    let s = make_schema_u64_i64();
    let reindex =
        |keep: Vec<u32>, key: Vec<gnitz_wire::ReindexSlot>| MapKind::Reindex { keep, key, nulls: NullKeys::Keep };
    let compute = |program: Vec<u8>, n: usize| {
        MapKind::Compute(gnitz_wire::ComputeMap {
            program,
            out_cols: vec![(TypeCode::I64, false); n],
        })
    };
    let n = MAX_PK_COLUMNS + 1;
    let wide = u64_pk_schema(&vec![SchemaColumn::new(TypeCode::U64, false); n - 1]);
    let wide_key = (0..n as u32).map(|c| (c, TypeCode::U64)).collect();
    for (schema, mk, why) in [
        (
            s,
            MapKind::Compute(LogicalProgram::copy_cols(&[200]).to_map(vec![(TypeCode::I64, false)])),
            "map: program/schema mismatch: ColOutOfRange { col: 200".to_string(),
        ),
        (
            s,
            reindex(vec![9], vec![(0, TypeCode::U64)]),
            "reindex map: payload column 9 out of range (2 cols)".to_string(),
        ),
        (
            wide,
            reindex(vec![0], wide_key),
            format!("reindex key: {n} columns exceeds the {MAX_PK_COLUMNS}-column PK limit"),
        ),
        (
            s,
            MapKind::HashRow { cols: vec![(9, TypeCode::I64)] },
            "hash-row map: column 9 out of range (2 cols)".to_string(),
        ),
        (
            s,
            MapKind::HashRow { cols: vec![(1, TypeCode::String)] },
            "map: program/schema mismatch".to_string(),
        ),
        (s, compute(vec![0xff; 8], 1), "map: invalid program".to_string()),
        (s, compute(Vec::new(), 1), "map: invalid program".to_string()),
        (
            s,
            compute(vec![1, 2, 3], MAX_COLUMNS),
            format!(
                "compute map: output column count {} exceeds MAX_COLUMNS ({MAX_COLUMNS})",
                MAX_COLUMNS + 1
            ),
        ),
    ] {
        let got = MapPlan::from_wire(&schema, &mk)
            .map(|_| ())
            .expect_err("a malformed map");
        assert!(got.starts_with(&why), "{got:?} is not {why:?}");
    }
}

/// A map reproduces its input only when it keeps every row under its own PK,
/// copies every payload column into its own slot, computes no output slot, and
/// the two schemas locate every column alike. Every case resolves, so each
/// condition decides one; a hash-row map's slot may widen a column within the
/// copy kernel's domain.
#[test]
fn a_map_is_an_identity_only_as_an_in_order_copy_under_the_inherited_pk() {
    use TypeCode::{I32, I64, U32, U64, U128};
    let copy = LogicalProgram::copy_cols;
    let inherit = |p: LogicalProgram, a: &SchemaDescriptor, b: &SchemaDescriptor| {
        MapPlan::inheriting(p, a, *b).expect("the map resolves")
    };
    let hash_row = |s: &SchemaDescriptor, tc: TypeCode| {
        MapPlan::from_wire(s, &MapKind::HashRow { cols: vec![(1, tc)] }).expect("a well-formed hash-row map")
    };
    let schema = u64_pk_schema(&[SchemaColumn::new(I64, true), SchemaColumn::new(I64, false)]);
    let narrow_in = u64_pk_schema(&[SchemaColumn::new(I64, true), SchemaColumn::new(I32, false)]);
    // Three I64 columns, the PK at column `pk`.
    let ints = |pk: u32| {
        let cols = [0, 1, 2].map(|ci| SchemaColumn::new(I64, ci != pk));
        SchemaDescriptor::new(&cols, &[pk])
    };
    let (mid_pk, pk_only) = (ints(1), pk_only_schema(&[U64]));
    let u128_pk = keyed_schema(&[U128], &[SchemaColumn::new(I64, true)]);
    let u32s = u64_pk_schema(&[SchemaColumn::new(U32, false)]);
    let three = u64_pk_schema(&[SchemaColumn::new(U32, false), SchemaColumn::new(I64, false)]);
    let u64_i64 = make_schema_u64_i64();
    let program = |sinks| LogicalProgram::new(vec![LogicalInstr::LoadCol { col: 2 }], sinks, vec![]).unwrap();
    assert_eq!(hash_row(&u128_pk, I64).output_schema(), &u128_pk);
    for (what, plan, identity) in [
        ("in-order copy", inherit(copy(&[1, 2]), &schema, &schema), true),
        ("reordered copy", inherit(copy(&[2, 1]), &schema, &schema), false),
        (
            "computed sink",
            inherit(program(vec![Sink::Col(1), Sink::Reg(Reg(0))]), &schema, &schema),
            false,
        ),
        (
            "an instruction no sink reads",
            inherit(program(vec![Sink::Col(1), Sink::Col(2)]), &schema, &schema),
            true,
        ),
        (
            "different payload type",
            inherit(copy(&[1, 2]), &narrow_in, &schema),
            false,
        ),
        ("different PK layout", inherit(copy(&[1, 2]), &ints(0), &ints(1)), false),
        (
            "a PK that does not lead",
            inherit(copy(&[0, 2]), &mid_pk, &mid_pk),
            true,
        ),
        (
            "a PK column copied into a payload slot",
            inherit(copy(&[1, 2]), &mid_pk, &mid_pk),
            false,
        ),
        (
            "PK-only, the empty program",
            inherit(copy(&[]), &pk_only, &pk_only),
            true,
        ),
        ("projection, same layout", project(&u128_pk, &[1]), true),
        ("hash-row, same layout", hash_row(&u128_pk, I64), false),
        ("hash-row", hash_row(&u32s, U32), false),
        ("hash-row, U32 widened to I64", hash_row(&u32s, I64), false),
        (
            "compound reindex",
            rekey_plan(&u64_i64, &[0, 1], &[0, 1], NullKeys::Keep),
            false,
        ),
        ("pruned reindex", rekey_plan(&three, &[0], &[2], NullKeys::Keep), false),
    ] {
        assert_eq!(plan.is_identity(), identity, "{what}");
    }
}

/// A `Drop` reindex over a nullable key is the filter-then-reindex it replaces,
/// wherever its NULL runs start and end — across a 64-row block, at the batch's
/// two ends, one row long, or covering every row. A `Keep` one re-keys the NULL
/// rows too.
#[test]
fn a_drop_reindex_is_filter_then_reindex_and_keep_keeps_null_keys() {
    // `(U64 pk, I64 NULL key, I64 NULL)`: the key is payload slot 0.
    let s = make_schema(&[TypeCode::I64, TypeCode::I64]);
    let patterns: [&dyn Fn(usize) -> bool; 6] = [
        &|i| i % 7 == 0,
        &|i| (60..140).contains(&i),
        &|i| !(64..190).contains(&i),
        &|i| i % 2 == 1,
        &|i| i != 127,
        &|_| true,
    ];
    for (p, is_null) in patterns.iter().enumerate() {
        let cells: Vec<[Option<i64>; 2]> = (0..200)
            .map(|i| {
                let key = (!is_null(i)).then_some(i as i64 - 100);
                [key, (i % 3 != 0).then_some(-(i as i64))]
            })
            .collect();
        let rows = |keep_null: bool| -> Vec<(u64, i64, &[Option<i64>])> {
            (0..200)
                .filter(|&i| keep_null || !is_null(i))
                .map(|i| (i as u64, [3, -1, 1, 2, -2][i % 5], &cells[i][..]))
                .collect()
        };
        let (batch, filtered) = (make_int_batch(&s, &rows(true)), make_int_batch(&s, &rows(false)));
        for keep in [vec![2], vec![1, 2]] {
            let run = |nulls| rekey_plan(&s, &[1], &keep, nulls);
            let mut drop = run(NullKeys::Drop);
            let dropped = drop.evaluate_map_batch(&batch.as_mem_batch());
            assert_eq!(dropped.schema(), drop.output_schema());
            assert_eq!(
                weighted_rows(&dropped),
                weighted_rows(&run(NullKeys::Keep).evaluate_map_batch(&filtered.as_mem_batch())),
                "pattern {p}"
            );
            assert_eq!(
                run(NullKeys::Keep).evaluate_map_batch(&batch.as_mem_batch()).len(),
                batch.len()
            );
        }
    }
}

/// Each output column `holders` names holds, on every row, the value input
/// column `c` packs at `ty`: a copy at its own type, the PK column an inherited
/// key keeps in place, or a key slot packing it. `rekeys_onto_pk_prefix`
/// answers exactly for a reindex onto a leading run of the input's PK.
#[test]
fn a_holder_holds_the_value_its_column_packs() {
    use TypeCode::{I16, I32, I64, U16, U64};
    let s = keyed_schema(
        &[U64, I32],
        &[SchemaColumn::new(I16, false), SchemaColumn::new(U16, false)],
    );
    let mut b = BatchBuilder::new(&s);
    for r in 0..50u128 {
        b.begin_row_natives(&[r * 7 % 13, (r as i32 - 25) as u32 as u128], 1);
        b.put_int((r as i16 * -3) as u16 as u128);
        b.put_int(r * 11 % 17);
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();
    let reindex = |key: Vec<gnitz_wire::ReindexSlot>, keep: Vec<u32>| {
        MapPlan::from_wire(&s, &MapKind::Reindex { keep, key, nulls: NullKeys::Keep }).unwrap()
    };
    let hash_row = MapKind::HashRow { cols: vec![(2, I16), (3, U16)] };
    for (what, mut plan, onto_pk_prefix) in [
        ("projection", project(&s, &[3, 2]), false),
        (
            "reindex onto the leading PK column",
            rekey_plan(&s, &[0], &[1, 2, 3], NullKeys::Keep),
            true,
        ),
        (
            "reindex onto the whole PK",
            rekey_plan(&s, &[0, 1], &[2], NullKeys::Keep),
            true,
        ),
        (
            "reindex onto a later PK column",
            rekey_plan(&s, &[1], &[0, 2], NullKeys::Keep),
            false,
        ),
        (
            "reindex onto a payload column",
            rekey_plan(&s, &[2], &[0, 3], NullKeys::Keep),
            false,
        ),
        ("promoted reindex", reindex(vec![(2, I64), (1, I64)], vec![3]), false),
        ("hash-row", MapPlan::from_wire(&s, &hash_row).unwrap(), false),
    ] {
        assert_eq!(plan.rekeys_onto_pk_prefix().is_some(), onto_pk_prefix, "{what}");
        let out = plan.evaluate_map_batch(&mb);
        let (os, omb) = (*plan.output_schema(), out.as_mem_batch());
        let mut held = 0;
        for c in 0..s.num_columns() as u32 {
            let own = s.columns()[c as usize].type_code;
            for &ty in TypeCode::ALL.iter().filter(|&&t| t.is_pk_eligible() && own.packs_at(t)) {
                for h in plan.holders(&s, c, ty) {
                    held += 1;
                    let loc = os.locate(h as usize);
                    for row in 0..b.len() {
                        let v = extended(own, le_cell(&cell(&mb, s.locate(c as usize), row).unwrap()));
                        assert_eq!(
                            cell(&omb, loc, row).unwrap(),
                            v.to_le_bytes()[..loc.size()],
                            "{what}: column {c} at {ty}, holder {h}, row {row}"
                        );
                    }
                }
            }
        }
        assert!(held > 0, "{what}");
    }
}
