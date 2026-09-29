use gnitz_expr::{ExprValidateErr, LogicalProgram, Reg, Sink};

use super::{MapPlan, PkSource};
use crate::ops::reindex::FoldCols;
use crate::schema::{SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode, MAX_COLUMNS};
use crate::storage::Batch;
use crate::storage::BatchBuilder;

/// Build a `Batch` of `(pk, weight, null_word, payload i64 cells)` rows against
/// `schema` — the engine-side physical batch these tests drive [`MapPlan`]
/// with, as opposed to the owned-buffer view `gnitz-expr`'s own tests use.
fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, u64, &[i64])]) -> Batch {
    let mut batch = BatchBuilder::new(*schema);
    for &(pk, weight, null_word, cols) in rows {
        batch.begin_row(pk as u128, weight);
        for (pi, _col) in schema.payload_columns() {
            match null_word >> pi & 1 {
                1 => batch.put_null(),
                _ => batch.put_int(cols[pi] as u128),
            }
        }
        batch.end_row();
    }
    batch.finish()
}

fn make_schema(pk_index: u32, col_types: &[TypeCode]) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::EMPTY; MAX_COLUMNS];
    for (i, &tc) in col_types.iter().enumerate() {
        let nullable = i != pk_index as usize;
        columns[i] = SchemaColumn::new(tc, nullable);
    }
    SchemaDescriptor::new(&columns[..col_types.len()], &[pk_index])
}

#[test]
fn test_projection_batch() {
    let in_schema = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let out_schema = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let batch = make_int_batch(&in_schema, &[(1, 1, 0, &[10, 20]), (2, 1, 0, &[30, 40])]);

    let prog = LogicalProgram::copy_cols(&[2, 1]);
    let mut func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
    let result = func.evaluate_map_batch(&batch);
    assert_eq!(result.count, 2);

    let r0_col0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
    let r0_col1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(r0_col0, 20);
    assert_eq!(r0_col1, 10);
}

#[test]
fn test_map_copy_and_emit() {
    let in_schema = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let out_schema = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64]);

    let batch = make_int_batch(&in_schema, &[(1, 1, 0, &[10, 20])]);

    use gnitz_expr::{IntArithOp, LogicalInstr};
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::IntArith {
            op: IntArithOp::Add,
            a: Reg(0),
            b: Reg(1),
        },
    ];
    let sinks = vec![Sink::Col(1), Sink::Reg(Reg(2))];
    let prog = LogicalProgram::new(instrs, sinks, vec![]);

    let mut func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
    let result = func.evaluate_map_batch(&batch);
    assert_eq!(result.count, 1);

    let v0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(v0, 10);
    let v1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(v1, 30);
}

#[test]
fn test_empty_batch() {
    let schema = make_schema(0, &[TypeCode::U64, TypeCode::I64]);
    let batch = Batch::empty_with_schema(&schema);

    let mut func = MapPlan::from_map(LogicalProgram::copy_cols(&[1]), &schema, &schema, PkSource::Inherit).unwrap();
    let result = func.evaluate_map_batch(&batch);
    assert_eq!(result.count, 0);
}

#[test]
fn test_map_blob_passthrough_and_fallback() {
    // Input: [U64 PK, STRING s1 (short inline), STRING s2 (long, heap-backed)].
    fn build(schema: &SchemaDescriptor) -> Batch {
        let mut b = BatchBuilder::new(*schema);
        for (pk, s1, s2) in [
            (1u128, b"ab".as_slice(), b"long-string-one-xyz".as_slice()),
            (2u128, b"cd".as_slice(), b"long-string-two-abcdef".as_slice()),
        ] {
            b.begin_row(pk, 1);
            b.put_blob(s1);
            b.put_blob(s2);
            b.end_row();
        }
        b.finish()
    }

    let in_schema = make_schema(0, &[TypeCode::U64, TypeCode::String, TypeCode::String]);

    // (A) Keep BOTH string columns (reordered) → passthrough fires; the shared
    // blob keeps every long string's heap offset valid through the verbatim copy.
    {
        let batch = build(&in_schema);
        let out_schema = make_schema(0, &[TypeCode::U64, TypeCode::String, TypeCode::String]);
        let prog = LogicalProgram::copy_cols(&[2, 1]);
        let mut func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
        let out = func.evaluate_map_batch(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(
            crate::test_support::read_german_string(&out, 0, 0),
            b"long-string-one-xyz"
        ); // s2 → out payload 0
        assert_eq!(crate::test_support::read_german_string(&out, 1, 0), b"ab"); // s1 → out payload 1
        assert_eq!(
            crate::test_support::read_german_string(&out, 0, 1),
            b"long-string-two-abcdef"
        );
        assert_eq!(crate::test_support::read_german_string(&out, 1, 1), b"cd");
    }

    // (B) Drop the long string s2 → passthrough gated OFF, so the relocate path runs and the
    // output blob carries only the referenced (here empty, short-inline) spans.
    {
        let batch = build(&in_schema);
        let out_schema = make_schema(0, &[TypeCode::U64, TypeCode::String]);
        let prog = LogicalProgram::copy_cols(&[1]);
        let mut func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
        let out = func.evaluate_map_batch(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(crate::test_support::read_german_string(&out, 0, 0), b"ab");
        assert_eq!(crate::test_support::read_german_string(&out, 0, 1), b"cd");
        assert!(
            out.blob().len() < batch.blob().len(),
            "dropped-string relocate must not copy the dead heap ({} vs {})",
            out.blob().len(),
            batch.blob().len(),
        );
    }
}

/// PK-source `CopyCol` through `from_map`: a compound PK's columns projected into
/// payload slots must decode out of the OPK region back to native LE — verbatim
/// for the U128 column, sign-flip-undone for the signed I64 column. `copy_column`
/// reads the source column's own width at its own `tc`, so a program carrying the
/// real source type codes round-trips both.
#[test]
fn test_map_pk_copy_col_u128_and_signed_i64() {
    // PK = (U128 c0, I64 c1); payload = I64 c2.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1],
    );
    // Out: same compound PK, then both PK columns copied into payload slots plus
    // the payload passthrough.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U128, false), // payload 0 ← PK col 0
            SchemaColumn::new(TypeCode::I64, false),  // payload 1 ← PK col 1
            SchemaColumn::new(TypeCode::I64, false),  // payload 2 ← payload col 2
        ],
        &[0, 1],
    );

    let pk0: u128 = 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210;
    let pk1: i64 = -5; // negative: exercises the OPK sign-bit flip on decode
    let mut batch = BatchBuilder::new(in_schema);
    batch.begin_row_opk(&[pk0, pk1 as u64 as u128], 1i64);
    batch.put_int(42);
    batch.end_row();
    let batch = batch.finish();

    let prog = LogicalProgram::copy_cols(&[
        0, // PK col 0 (U128) → payload 0
        1, // PK col 1 (I64) → payload 1
        2, // payload col 2 (I64) → payload 2
    ]);
    let out = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit)
        .unwrap()
        .evaluate_map_batch(&batch);

    assert_eq!(out.count, 1);
    assert_eq!(out.pk_data(), batch.pk_data(), "PK region copied verbatim");
    assert_eq!(out.get_weight(0), 1);
    assert_eq!(
        u128::from_le_bytes(out.col_data(0)[..16].try_into().unwrap()),
        pk0,
        "U128 PK column must round-trip through copy_column",
    );
    assert_eq!(
        i64::from_le_bytes(out.col_data(1)[..8].try_into().unwrap()),
        pk1,
        "signed I64 PK column must un-flip the OPK sign bit",
    );
    assert_eq!(i64::from_le_bytes(out.col_data(2)[..8].try_into().unwrap()), 42);
    assert_eq!(out.get_null_word(0), 0, "no copied column is null");
}

/// Cross-width widen through `from_map` (the shape a cross-width set-op UNION
/// produces): a narrow source column copied into a wider promoted output slot
/// sign/zero-extends per the SOURCE column's signedness — from the payload region
/// and from the PK region alike.
#[test]
fn test_map_copy_col_widens_into_promoted_slot() {
    // PK = (U16 c0, I16 c1); payload = I8 c2 (negative), U8 c3.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U16, false),
            SchemaColumn::new(TypeCode::I16, false),
            SchemaColumn::new(TypeCode::I8, false),
            SchemaColumn::new(TypeCode::U8, false),
        ],
        &[0, 1],
    );
    // Every copied column lands in an I64 slot — 4 distinct narrow→wide widens.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U16, false),
            SchemaColumn::new(TypeCode::I16, false),
            SchemaColumn::new(TypeCode::I64, false), // ← U16 PK  (zero-extend)
            SchemaColumn::new(TypeCode::I64, false), // ← I16 PK  (sign-extend)
            SchemaColumn::new(TypeCode::I64, false), // ← I8 payload (sign-extend)
            SchemaColumn::new(TypeCode::I64, false), // ← U8 payload (zero-extend)
        ],
        &[0, 1],
    );

    let (c0, c1, c2, c3): (u16, i16, i8, u8) = (0xBEEF, -300, -7, 0xFE);
    let mut batch = BatchBuilder::new(in_schema);
    batch.begin_row_opk(&[c0 as u128, c1 as u16 as u128], 1i64);
    batch.put_int(c2 as u128);
    batch.put_int(c3 as u128);
    batch.end_row();
    let batch = batch.finish();

    let prog = LogicalProgram::copy_cols(&[
        0, // U16 PK  → zero-extend
        1, // I16 PK  → sign-extend
        2, // I8 payload → sign-extend
        3, // U8 payload → zero-extend
    ]);
    let out = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit)
        .unwrap()
        .evaluate_map_batch(&batch);

    assert_eq!(out.count, 1);
    let widened = |pi: usize| i64::from_le_bytes(out.col_data(pi)[..8].try_into().unwrap());
    assert_eq!(widened(0), c0 as i64, "U16 PK zero-extends");
    assert_eq!(widened(1), c1 as i64, "I16 PK sign-extends");
    assert_eq!(widened(2), c2 as i64, "I8 payload sign-extends");
    assert_eq!(widened(3), c3 as i64, "U8 payload zero-extends");
}

/// An instruction-free program writes output slots — the shape every `copy_cols`
/// map has, so `validate` cannot reject it outright. The filter role reads a
/// result register back, so it rejects rather than letting the program pass as a
/// filter matching nothing (which a client would read as an empty table).
#[test]
fn test_register_free_predicate_is_rejected() {
    let schema = make_schema(0, &[TypeCode::U64, TypeCode::I64]);
    let prog = LogicalProgram::copy_cols(&[]);
    assert_eq!(
        prog.resolve_filter(&schema).err(),
        Some(ExprValidateErr::OutputRoleMismatch)
    );
    // The same shape is legitimate as a map: `copy_cols` builds it.
    assert!(MapPlan::from_map(LogicalProgram::copy_cols(&[1]), &schema, &schema, PkSource::Inherit).is_ok());
}

/// The predicate wiring over a real `Batch`, including the range coalescing
/// that turns per-row verdicts into the `(start, end)` list every consumer reads.
#[test]
fn test_from_predicate_filter_ranges_over_a_batch() {
    use gnitz_expr::{CmpOp, LogicalInstr};

    let schema = make_schema(0, &[TypeCode::U64, TypeCode::I64]);
    // Rows 1..=6 with col1 = 5, 20, 30, 0, 40, 50 → `col1 > 15` keeps
    // {1, 2} and {4, 5}: two runs, so a PK-only or per-row answer would differ.
    let rows: Vec<(u64, i64, u64, &[i64])> = vec![
        (1, 1, 0, &[5]),
        (2, 1, 0, &[20]),
        (3, 1, 0, &[30]),
        (4, 1, 0, &[0]),
        (5, 1, 0, &[40]),
        (6, 1, 0, &[50]),
    ];
    let batch = make_int_batch(&schema, &rows);

    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 15, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
    ];
    let mut func = LogicalProgram::new(instrs, vec![Sink::Reg(Reg(2))], vec![])
        .resolve_filter(&schema)
        .unwrap();

    let mut ranges = Vec::new();
    func.ranges(&batch.as_mem_batch(), &mut ranges);
    assert_eq!(ranges, vec![(1, 3), (4, 6)]);

    // `out` is cleared, not appended to, so a reused buffer cannot leak a
    // previous chunk's ranges into this one.
    func.ranges(&batch.as_mem_batch(), &mut ranges);
    assert_eq!(ranges, vec![(1, 3), (4, 6)]);
}

/// A batch of `(pk, string cells)` rows: one STRING payload column per entry
/// of `cells`, encoded through the blob heap the map must relocate or share.
fn make_string_batch(schema: &SchemaDescriptor, rows: &[&[&[u8]]]) -> Batch {
    let mut batch = BatchBuilder::new(*schema);
    for (row, cells) in rows.iter().enumerate() {
        batch.begin_row(row as u128 + 1, 1);
        for (pi, _col) in schema.payload_columns() {
            batch.put_blob(cells[pi]);
        }
        batch.end_row();
    }
    batch.finish()
}

/// A map whose *only* computed column is a string. The compute kernel is
/// gated on the emit lists being non-empty, and a gate that counted only the
/// scalar list would drop the kernel here and ship the STRING region
/// uninitialized — which validation cannot catch, because the `Emit` is in
/// the program and the output-coverage popcount is satisfied.
#[test]
fn map_whose_only_computed_column_is_a_string_still_runs_the_kernel() {
    use gnitz_expr::LogicalInstr;
    // [U64 pk, STRING name] -> [U64 pk, U64 id_copy, STRING upper_name].
    let in_schema = make_schema(0, &[TypeCode::U64, TypeCode::String]);
    let out_schema = make_schema(0, &[TypeCode::U64, TypeCode::U64, TypeCode::String]);
    let mut batch = make_string_batch(&in_schema, &[&[b"abc"], &[b"a-long-value-past-twelve"]]);
    // The copied column is the PK, which `PkSource::Inherit` carries verbatim; give
    // the output's first payload slot something to hold.
    batch.count = 2;

    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrCase { a: Reg(0), upper: true },
    ];
    let sinks = vec![Sink::Col(0), Sink::Reg(Reg(1))];
    let mut func = MapPlan::from_map(
        LogicalProgram::new(instrs, sinks, vec![]),
        &in_schema,
        &out_schema,
        PkSource::Inherit,
    )
    .unwrap();
    let out = func.evaluate_map_batch(&batch);
    assert_eq!(out.count, 2);
    assert_eq!(crate::test_support::read_german_string(&out, 1, 0), b"ABC");
    assert_eq!(
        crate::test_support::read_german_string(&out, 1, 1),
        b"A-LONG-VALUE-PAST-TWELVE",
        "a heap-backed value must land in the output's own blob"
    );
}

/// A string emit alongside a passthrough of every input string column. The
/// copied cells still resolve against the adopted blob after the emit has
/// appended to it, and the output stops sharing once it has.
#[test]
fn string_emit_composes_with_blob_passthrough() {
    use gnitz_expr::LogicalInstr;
    // [U64 pk, STRING s] -> [U64 pk, STRING s_copy, STRING s_upper].
    let in_schema = make_schema(0, &[TypeCode::U64, TypeCode::String]);
    let out_schema = make_schema(0, &[TypeCode::U64, TypeCode::String, TypeCode::String]);
    let batch = make_string_batch(&in_schema, &[&[b"a-long-value-past-twelve"], &[b"short"]]);

    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrCase { a: Reg(0), upper: true },
    ];
    let sinks = vec![Sink::Col(1), Sink::Reg(Reg(1))];
    let mut func = MapPlan::from_map(
        LogicalProgram::new(instrs, sinks, vec![]),
        &in_schema,
        &out_schema,
        PkSource::Inherit,
    )
    .unwrap();
    let out = func.evaluate_map_batch(&batch);

    assert_eq!(
        crate::test_support::read_german_string(&out, 0, 0),
        b"a-long-value-past-twelve"
    );
    assert_eq!(crate::test_support::read_german_string(&out, 0, 1), b"short");
    assert_eq!(
        crate::test_support::read_german_string(&out, 1, 0),
        b"A-LONG-VALUE-PAST-TWELVE"
    );
    assert_eq!(crate::test_support::read_german_string(&out, 1, 1), b"SHORT");
}

/// A NULL string row must ship a zeroed cell *and* its bitmap bit — a
/// non-deterministic value there would leave an insert and its retraction
/// unable to cancel.
#[test]
fn null_string_emit_zeroes_the_cell_and_sets_the_bit() {
    use gnitz_expr::LogicalInstr;
    let in_schema = make_schema(0, &[TypeCode::U64, TypeCode::String]);
    let out_schema = make_schema(0, &[TypeCode::U64, TypeCode::U64, TypeCode::String]);
    let mut batch = make_string_batch(&in_schema, &[&[b"abc"], &[b"def"]]);
    // Row 1's source string is NULL.
    gnitz_wire::write_u64_le(batch.null_bmp_data_mut(), 8, 1);

    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrCase { a: Reg(0), upper: true },
    ];
    let sinks = vec![Sink::Col(0), Sink::Reg(Reg(1))];
    let mut func = MapPlan::from_map(
        LogicalProgram::new(instrs, sinks, vec![]),
        &in_schema,
        &out_schema,
        PkSource::Inherit,
    )
    .unwrap();
    let out = func.evaluate_map_batch(&batch);

    assert_eq!(crate::test_support::read_german_string(&out, 1, 0), b"ABC");
    assert_eq!(&out.col_data(1)[16..32], &[0u8; 16], "a NULL cell is all zeros");
    assert_eq!(
        gnitz_wire::read_u64_le(out.null_bmp_data(), 8) & 2,
        2,
        "the output bitmap bit for the string column must be set"
    );
}

#[test]
fn map_with_pack_pk_source_promotes_payload_to_pk() {
    use crate::ops::reindex::ReindexPacker;
    use crate::test_support::{make_batch, make_schema_u64_i64};
    // `PkSource::Pack` rewrites the output PK by reading the referenced
    // column through the reindex packer. Verifies (1) every row's output PK
    // matches the source column value, (2) the resulting batch is correctly
    // marked unsorted/unconsolidated (the stamp destroys PK order).

    // Input: PK u64, payload i64. Reindex on the payload (col 1) — the
    // new output PK is each row's payload value.
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 200), (2, 1, 100), (3, 1, 300)]);

    // Projection plan: output keeps the same single payload column.
    let packer = ReindexPacker::new(&schema, &[(1, TypeCode::I64)]).unwrap();
    let mut plan = MapPlan::from_map(
        LogicalProgram::copy_cols(&[1]),
        &schema,
        &schema,
        PkSource::Pack(packer),
    )
    .unwrap();
    let out = plan.evaluate_map_batch(&batch);
    assert_eq!(out.count, 3);
    // Each output row's PK is the sign-aware OPK image of its source payload
    // value (col 1 is I64): `widen_pk_be(encode_pk_column(v))`, i.e. the value
    // with its sign bit flipped, matching how `ColumnLocator::opk_image` routes the same
    // value. A raw-native `== 200` assertion would falsely fail signed reindex.
    let opk_i64 = |v: i64| ((v as u64) ^ 0x8000_0000_0000_0000) as u128;
    assert_eq!(out.get_pk(0), opk_i64(200));
    assert_eq!(out.get_pk(1), opk_i64(100));
    assert_eq!(out.get_pk(2), opk_i64(300));
    // Payload itself is unchanged by the projection.
    let payload = |row: usize| gnitz_wire::read_i64_le(out.col_data(0), row * 8);
    assert_eq!(payload(0), 200);
    assert_eq!(payload(1), 100);
    assert_eq!(payload(2), 300);
    // The PK stamp destroys PK order — output must be marked accordingly.
    assert!(!out.is_consolidated(), "a stamped PK must drop the layout claim");
    assert!(!out.is_consolidated(), "a stamped PK must not be marked consolidated");
}

/// `PkSource::HashRow` keys a row by its payload *content*, not by its cell
/// bytes: equal rows must land on one PK, or EXCEPT/INTERSECT cannot cancel
/// them, and unequal rows must not, or two elements coalesce into one.
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
    ];

    let in_schema = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::String, TypeCode::Blob]);
    let mut batch = BatchBuilder::new(in_schema);
    for (row, &(v, is_null, s, b)) in rows.iter().enumerate() {
        batch.begin_row(row as u128, 1);
        match is_null {
            true => batch.put_null(),
            false => batch.put_int(v as u128),
        }
        batch.put_blob(s);
        batch.put_blob(b);
        batch.end_row();
    }
    let batch = batch.finish();

    // The hash-row output schema: one synthetic U128 PK, then the payload.
    let out_schema = make_schema(0, &[TypeCode::U128, TypeCode::I64, TypeCode::String, TypeCode::Blob]);
    let mut plan = MapPlan::from_map(
        LogicalProgram::copy_cols(&[1, 2, 3]),
        &in_schema,
        &out_schema,
        PkSource::HashRow(FoldCols::new(out_schema.payload_locators())),
    )
    .unwrap();
    let out = plan.evaluate_map_batch(&batch);
    assert_eq!(out.count, rows.len());
    let pk = |row: usize| out.get_pk(row);

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
    ] {
        assert_ne!(pk(a), pk(b), "{what} must move the PK");
    }

    // An in-place PK rewrite drops the layout claim.
    assert!(!out.is_consolidated(), "a stamped PK must not be marked consolidated");
}

/// Every PK width is its own monomorphization in `copy_column`'s unswitched PK
/// arm. A compound PK whose list order is not its column order drives all five at
/// once: each column lands in a payload slot of its own type, as the native
/// little-endian value with the OPK sign flip undone.
#[test]
fn a_compound_permuted_pk_decodes_at_every_width() {
    let pk_types = [
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I32,
        TypeCode::U64,
        TypeCode::I128,
    ];
    // PK-list order, deliberately not column order: each column's OPK byte
    // offset comes from this list, so a width mixed up here would be caught as
    // a wrong value rather than a wrong length.
    let pk_order: [u32; 5] = [3, 0, 4, 1, 2];

    let mut cols = [SchemaColumn::EMPTY; MAX_COLUMNS];
    for (i, &t) in pk_types.iter().enumerate() {
        cols[i] = SchemaColumn::new(t, false);
    }
    let in_schema = SchemaDescriptor::new(&cols[..5], &pk_order);
    // The output inherits the PK and adds one payload column per PK column, at
    // that column's own type — so the copy widens nothing and the bytes written
    // are exactly what the decode produced.
    let mut out_cols = cols;
    for (i, &t) in pk_types.iter().enumerate() {
        out_cols[5 + i] = SchemaColumn::new(t, true);
    }
    let out_schema = SchemaDescriptor::new(&out_cols[..10], &pk_order);

    // Both extremes and a wrap-adjacent value per width: the sign flip is what
    // orders these, and dropping it shows up first at the ends.
    let rows: [[i128; 5]; 3] = [
        [i8::MIN as i128, 0, i32::MIN as i128, 0, i128::MIN],
        [-1, 40_000, -1, u64::MAX as i128, -1],
        [i8::MAX as i128, u16::MAX as i128, i32::MAX as i128, 1, i128::MAX],
    ];
    let mut batch = BatchBuilder::new(in_schema);
    for vals in &rows {
        let natives: Vec<u128> = pk_order.iter().map(|&ci| vals[ci as usize] as u128).collect();
        batch.begin_row_opk(&natives, 1i64);
        batch.end_row();
    }
    let batch = batch.finish();

    let mut plan = MapPlan::from_map(
        LogicalProgram::copy_cols(&[0, 1, 2, 3, 4]),
        &in_schema,
        &out_schema,
        PkSource::Inherit,
    )
    .unwrap();
    let out = plan.evaluate_map_batch(&batch);
    assert_eq!(out.count, rows.len());
    for (row, vals) in rows.iter().enumerate() {
        for (pi, (&v, &t)) in vals.iter().zip(&pk_types).enumerate() {
            let w = t.wire_stride();
            assert_eq!(
                &out.col_data(pi)[row * w..row * w + w],
                &(v as u128).to_le_bytes()[..w],
                "row {row}, PK column {pi} (type {t}) decoded wrong",
            );
        }
    }
}

// -----------------------------------------------------------------------
// Benchmark
// -----------------------------------------------------------------------

/// Retired instructions of the map driver: `evaluate_map_batch` over whole
/// batches, and `append_map_ranges` over one range (`*_whole`) and over `R`-row
/// runs with 16-row gaps (`*_r{R}`), where `COMPACT_RUN_LEN` decides.
/// Difference two pass counts, one shape per process:
///
///   cargo build -p gnitz-store --release --tests
///   for s in reindex permute proj_keep_str proj_drop_str int3_whole int3_r16 upper_r16; do for p in 1 21; do \
///     GNITZ_BENCH_SHAPE=$s GNITZ_BENCH_PASSES=$p perf stat -e instructions:u \
///     cargo test -p gnitz-store --release map_ranges_bench -- --ignored --nocapture
///   done; done
#[test]
#[ignore]
fn map_ranges_bench() {
    use crate::ops::reindex::ReindexPacker;
    use gnitz_expr::{IntArithOp, LogicalInstr};
    use std::hint::black_box;

    const N: usize = 262_144;
    const GAP: usize = 16;
    let passes: usize = std::env::var("GNITZ_BENCH_PASSES").map_or(1, |v| v.parse().unwrap());
    let only = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "all".to_string());
    let driven = |name: &str| only == "all" || only == name;
    let mut n_selected = 0usize;
    let mut acc = 0usize;

    // --- Reindex map: [U64 PK, I64, I64] reindexed on col 1, the source PK and
    // both payload columns kept — the equijoin / GROUP BY repartition shape.
    // Column 0 is what puts a `ColumnLocator::Pk` copy in the loop; the keep-set
    // rules retain the source PK on every one of those.
    let rx_in = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let mut rx_batch = BatchBuilder::new(rx_in);
    for i in 0..N as u64 {
        rx_batch.begin_row(i as u128, 1i64);
        rx_batch.put_int(i.wrapping_mul(2_654_435_761) as u128);
        rx_batch.put_int((!i) as u128);
        rx_batch.end_row();
    }
    let rx_batch = rx_batch.finish();
    let rx_packer = ReindexPacker::new(&rx_in, &[(1, TypeCode::I64)]).unwrap();
    let rx_out = rx_packer.output_schema(&rx_in, &[0, 1, 2]).unwrap();
    let mut rx_plan = MapPlan::from_map(
        LogicalProgram::copy_cols(&[0, 1, 2]),
        &rx_in,
        &rx_out,
        PkSource::Pack(rx_packer),
    )
    .unwrap();

    // --- String source: [U64 PK, STRING], every cell past the inline threshold
    // so every one is heap-backed and an emit grows the output blob.
    let str_in = make_schema(0, &[TypeCode::U64, TypeCode::String]);
    let mut str_batch = BatchBuilder::new(str_in);
    for i in 0..N {
        str_batch.begin_row(i as u128, 1i64);
        str_batch.put_blob(format!("row-{i:012}-payload").as_bytes());
        str_batch.end_row();
    }
    let str_batch = str_batch.finish();
    let upper = || {
        MapPlan::from_map(
            LogicalProgram::new(
                vec![
                    LogicalInstr::LoadColStr { col: 1 },
                    LogicalInstr::StrCase { a: Reg(0), upper: true },
                ],
                vec![Sink::Reg(Reg(1))],
                vec![],
            ),
            &str_in,
            &str_in,
            PkSource::Inherit,
        )
        .unwrap()
    };
    let mut se_plan = upper();

    // --- Integer source: [U64 PK, I64, I64, I64], and a map computing three
    // columns out of it.
    let int_in = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64, TypeCode::I64]);
    let mut int_batch = BatchBuilder::new(int_in);
    for i in 0..N as u64 {
        int_batch.begin_row(i as u128, 1);
        for pi in 0..3u64 {
            int_batch.put_int((i.wrapping_mul(2_654_435_761 + pi) % 1000) as u128);
        }
        int_batch.end_row();
    }
    let int_batch = int_batch.finish();
    let arith = |op, a, b| LogicalInstr::IntArith { op, a: Reg(a), b: Reg(b) };
    let int3 = || {
        MapPlan::from_map(
            LogicalProgram::new(
                vec![
                    LogicalInstr::LoadColInt { col: 1 },
                    LogicalInstr::LoadColInt { col: 2 },
                    LogicalInstr::LoadColInt { col: 3 },
                    arith(IntArithOp::Add, 0, 1),
                    arith(IntArithOp::Mul, 1, 2),
                    arith(IntArithOp::Sub, 2, 0),
                ],
                vec![Sink::Reg(Reg(3)), Sink::Reg(Reg(4)), Sink::Reg(Reg(5))],
                vec![],
            ),
            &int_in,
            &int_in,
            PkSource::Inherit,
        )
        .unwrap()
    };
    // A pure projection that moves every payload column to another slot.
    let mut permute = MapPlan::from_map(
        LogicalProgram::copy_cols(&[3, 1, 2]),
        &int_in,
        &int_in,
        PkSource::Inherit,
    )
    .unwrap();

    // --- Hash-row maps: [U64 PK, payload...] -> [U128 hash PK, payload...], the
    // set-op / DISTINCT leaf. Fixed-width shapes on both sides of the fold's
    // stack arm, and heap-backed strings beside integers.
    let hash_row = |name: &'static str, payload: &[TypeCode]| {
        let mut in_tcs = vec![TypeCode::U64];
        in_tcs.extend_from_slice(payload);
        let mut out_tcs = vec![TypeCode::U128];
        out_tcs.extend_from_slice(payload);
        let in_schema = make_schema(0, &in_tcs);
        let mut batch = BatchBuilder::new(in_schema);
        for i in 0..N as u64 {
            batch.begin_row(i as u128, 1);
            for (pi, &tc) in payload.iter().enumerate() {
                if tc == TypeCode::String {
                    batch.put_string(&format!("row-{i:012}-payload"));
                } else {
                    batch.put_int(i.wrapping_mul(2_654_435_761 + pi as u64) as u128);
                }
            }
            batch.end_row();
        }
        let batch = batch.finish();
        let cols: Vec<u32> = (1..=payload.len() as u32).collect();
        let out_schema = make_schema(0, &out_tcs);
        let plan = MapPlan::from_map(
            LogicalProgram::copy_cols(&cols),
            &in_schema,
            &out_schema,
            PkSource::HashRow(FoldCols::new(out_schema.payload_locators())),
        )
        .unwrap();
        (name, plan, batch)
    };
    const I64: TypeCode = TypeCode::I64;
    const STR: TypeCode = TypeCode::String;
    let mut hash_rows = [
        hash_row("hash_row[i64x2]", &[I64, I64]),
        hash_row("hash_row[i64x10]", &[I64; 10]),
        hash_row("hash_row[i64,str]", &[I64, STR]),
        hash_row("hash_row[i64x3,str]", &[I64, I64, I64, STR]),
    ];

    // --- Two heap-backed string columns, [U64 PK, STRING, STRING], and a
    // projection keeping both, or only the first.
    let str2_in = make_schema(0, &[TypeCode::U64, TypeCode::String, TypeCode::String]);
    let mut str2_batch = BatchBuilder::new(str2_in);
    for i in 0..N {
        str2_batch.begin_row(i as u128, 1i64);
        str2_batch.put_string(&format!("first-{i:016}-payload-first"));
        str2_batch.put_string(&format!("second-{i:016}-payload-second"));
        str2_batch.end_row();
    }
    let str2_batch = str2_batch.finish();
    let projection = |cols: &[u32]| {
        let out = crate::schema::project_schema(&str2_in, cols).unwrap();
        MapPlan::from_map(LogicalProgram::copy_cols(cols), &str2_in, &out, PkSource::Inherit).unwrap()
    };
    let (mut keep_str, mut drop_str) = (projection(&[1, 2]), projection(&[1]));

    // Whole-batch shapes, through `evaluate_map_batch`.
    let whole = [
        ("reindex", &mut rx_plan, &rx_batch),
        ("str_emit", &mut se_plan, &str_batch),
        ("permute", &mut permute, &int_batch),
        ("proj_keep_str", &mut keep_str, &str2_batch),
        ("proj_drop_str", &mut drop_str, &str2_batch),
    ]
    .into_iter()
    .chain(hash_rows.iter_mut().map(|(name, plan, batch)| (*name, plan, &*batch)));
    for (name, plan, src) in whole {
        if !driven(name) {
            continue;
        }
        n_selected += 1;
        for _ in 0..passes {
            let out = plan.evaluate_map_batch(black_box(src));
            acc = acc.wrapping_add(out.count).wrapping_add(out.blob().len());
            black_box(&out);
        }
    }

    // Range shapes, through `append_map_ranges`: one range, and `r`-row runs with
    // `GAP`-row gaps.
    let runs = |r: usize| -> Vec<(usize, usize)> { (0..N).step_by(r + GAP).map(|s| (s, (s + r).min(N))).collect() };
    let mut shapes: Vec<(String, Vec<(usize, usize)>)> = vec![("whole".to_string(), vec![(0, N)])];
    shapes.extend([4usize, 16, 64, 128, 256].map(|r| (format!("r{r}"), runs(r))));
    for (family, mut plan, src) in [("int3", int3(), &int_batch), ("upper", upper(), &str_batch)] {
        let mut keeper = Batch::empty_with_schema(plan.out_schema());
        for (suffix, ranges) in &shapes {
            if !driven(&format!("{family}_{suffix}")) {
                continue;
            }
            n_selected += 1;
            for _ in 0..passes {
                keeper.clear();
                plan.append_map_ranges(black_box(src), &mut keeper, ranges);
                acc = acc.wrapping_add(keeper.count).wrapping_add(keeper.blob().len());
            }
            let survivors: usize = ranges.iter().map(|&(s, e)| e - s).sum();
            println!(
                "map_ranges_bench {family}_{suffix}: {} ranges, {survivors} survivors",
                ranges.len()
            );
        }
    }
    println!(
        "map_ranges_bench shape={only} passes={passes} n={N} acc={}",
        black_box(acc)
    );
    assert!(n_selected > 0, "GNITZ_BENCH_SHAPE matched nothing: {only:?}");
}

// ── MapPlan::from_wire — the circuit-node trust boundary ────────────────
//
// Every kind's column lists are client-supplied catalog data: each way they can
// overrun a schema array, address an unnumbered copy slot, or promote outside the
// copy kernel's domain must be a named rejection, not a panic or a truncated key.

/// The guard that refused `mk` over `in_schema`.
fn wire_rejection(in_schema: &SchemaDescriptor, mk: gnitz_wire::MapKind) -> String {
    MapPlan::from_wire(in_schema, &mk)
        .map(|_| "a plan")
        .expect_err("expected a rejection")
        .to_string()
}

/// `(U64 pk, I64)` — the narrow fixture every bounds case indexes past.
fn u64_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

#[test]
fn a_projection_must_name_payload_columns_that_fit_one_schema() {
    let s = u64_i64();
    let proj = |cols: Vec<u32>| wire_rejection(&s, gnitz_wire::MapKind::Projection(cols));
    assert_eq!(
        proj(vec![200]),
        "projection map: column 200 is not a payload column of a 2-column schema"
    );
    // A PK source: `project_schema` drops it while `copy_cols` numbers
    // destinations densely, so the copy would address a slot that does not exist.
    assert_eq!(
        proj(vec![0]),
        "projection map: column 0 is not a payload column of a 2-column schema"
    );
    // Each index is bounded but the list length is not, and duplicates are legal.
    // Exactly MAX_COLUMNS payload sources already overflow — the output also
    // carries the input's PK column, which a length-only bound misses.
    assert_eq!(proj(vec![1; MAX_COLUMNS]), "projection map: output exceeds MAX_COLUMNS");
}

/// A hash-row map over a `(U128 PK, I64)` input carries its payload column into
/// an identical layout, yet rewrites every PK: the program alone reads as an
/// identity, and only the PK source rules it out.
#[test]
fn a_hash_row_map_carrying_every_column_is_not_an_identity() {
    let s = make_schema(0, &[TypeCode::U128, TypeCode::I64]);
    let plan = MapPlan::from_wire(&s, &gnitz_wire::MapKind::HashRow { cols: vec![(1, TypeCode::I64)] })
        .expect("a well-formed hash-row map");
    assert_eq!(plan.out_schema(), &s, "the output layout is the input's");
    assert!(!plan.is_identity(), "a hash-row map is never an identity");
    let inherit = MapPlan::from_map(LogicalProgram::copy_cols(&[1]), &s, &s, PkSource::Inherit).unwrap();
    assert!(inherit.is_identity(), "the same copy under the inherited PK is one");
}

/// An auxiliary reindex on `key`, keeping `keep`.
fn reindex_slots(
    key: Vec<gnitz_wire::ReindexSlot>,
    keep: Vec<u32>,
    nulls: gnitz_wire::NullKeys,
) -> gnitz_wire::MapKind {
    gnitz_wire::MapKind::Reindex {
        keep,
        key,
        role: gnitz_wire::ReindexRole::Auxiliary,
        nulls,
    }
}

/// [`reindex_slots`] on `key` of `s`, each slot self-typed.
fn reindex_on(s: &SchemaDescriptor, key: &[u32], keep: Vec<u32>, nulls: gnitz_wire::NullKeys) -> gnitz_wire::MapKind {
    reindex_slots(crate::test_support::self_typed_slots(s, key), keep, nulls)
}

#[test]
fn reindex_key_and_kept_column_lists_are_bounds_checked() {
    let reindex = |s: &SchemaDescriptor, keep, key: Vec<u32>| reindex_on(s, &key, keep, gnitz_wire::NullKeys::Keep);
    let three = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    // A compound key and a pruned payload both build — and neither is an
    // identity, since a reindex overwrites every row's PK.
    for (s, keep, key) in [(u64_i64(), vec![0, 1], vec![0, 1]), (three, vec![2], vec![0])] {
        let plan = MapPlan::from_wire(&s, &reindex(&s, keep, key)).expect("a well-formed reindex");
        assert!(!plan.is_identity(), "a reindex is never an identity");
    }

    let s = u64_i64();
    assert_eq!(
        wire_rejection(&s, reindex(&s, vec![9], vec![0])),
        "reindex map: payload column 9 out of range (2 cols)"
    );
    assert_eq!(
        wire_rejection(
            &s,
            reindex_slots(vec![(9, TypeCode::I64)], vec![0], gnitz_wire::NullKeys::Keep)
        ),
        "reindex key: column 9 out of range (2 cols)"
    );
    // A key longer than MAX_PK_COLUMNS overflows the output schema's fixed PK array.
    let n = crate::schema::MAX_PK_COLUMNS + 1;
    let wide = SchemaDescriptor::new(&vec![SchemaColumn::new(TypeCode::U64, false); n], &[0]);
    assert_eq!(
        wire_rejection(&wide, reindex(&wide, vec![0], (0..n as u32).collect())),
        format!("reindex key: {n} columns exceeds the {}-column PK limit", n - 1)
    );
}

/// Every region a map writes, for comparing two outputs row for row.
fn regions(b: &Batch) -> Vec<Vec<u8>> {
    let mut out = vec![
        b.pk_data().to_vec(),
        b.weight_data().to_vec(),
        b.null_bmp_data().to_vec(),
    ];
    out.extend((0..b.num_payload_cols()).map(|pi| b.col_data(pi).to_vec()));
    out
}

/// A `Drop` reindex over a nullable key is the filter-then-reindex it replaces:
/// the same rows, weights and PK bytes. A `Keep` one re-keys the NULL rows too.
#[test]
fn a_drop_reindex_is_filter_then_reindex_and_keep_keeps_null_keys() {
    // `(U64 pk, I64 NULL key, I64 NULL)`: the key is payload slot 0.
    let s = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let rows: &[(u64, i64, u64, &[i64])] = &[
        (1, 1, 0b01, &[0, 10]),
        (2, 2, 0, &[-5, 20]),
        (3, 1, 0b10, &[7, 0]),
        (4, 3, 0b11, &[0, 0]),
        (5, 1, 0b01, &[0, 50]),
        (6, -1, 0, &[9, 60]),
    ];
    let defined: Vec<_> = rows.iter().copied().filter(|r| r.2 & 1 == 0).collect();
    let (batch, filtered) = (make_int_batch(&s, rows), make_int_batch(&s, &defined));
    for keep in [vec![2], vec![1, 2]] {
        let run = |nulls, b: &Batch| {
            MapPlan::from_wire(&s, &reindex_on(&s, &[1], keep.clone(), nulls))
                .unwrap()
                .evaluate_map_batch(b)
        };
        let dropped = run(gnitz_wire::NullKeys::Drop, &batch);
        assert_eq!(dropped.count, defined.len());
        assert_eq!(regions(&dropped), regions(&run(gnitz_wire::NullKeys::Keep, &filtered)));
        assert_eq!(run(gnitz_wire::NullKeys::Keep, &batch).count, rows.len());
    }
    // Every row NULL-keyed: an empty output in the reindex's schema.
    let all_null = make_int_batch(&s, &[(1, 1, 0b01, &[0, 1])]);
    let plan = &mut MapPlan::from_wire(&s, &reindex_on(&s, &[1], vec![2], gnitz_wire::NullKeys::Drop)).unwrap();
    let out = plan.evaluate_map_batch(&all_null);
    assert_eq!((out.count, out.schema()), (0, plan.out_schema()));
}

/// A `Drop` reindex's survivor runs are exact wherever its NULL runs start and
/// end: across a 64-row block, at the batch's two ends, and one row long.
#[test]
fn a_drop_reindex_keeps_exactly_the_key_defined_rows_across_blocks() {
    let s = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let patterns: [&dyn Fn(usize) -> bool; 5] = [
        &|i| i % 7 == 0,
        &|i| (60..140).contains(&i),
        &|i| !(64..190).contains(&i),
        &|i| i % 2 == 1,
        &|i| i != 127,
    ];
    for (p, is_null) in patterns.iter().enumerate() {
        let cells: Vec<[i64; 2]> = (0..200)
            .map(|i| [if is_null(i) { 0 } else { i as i64 }, -(i as i64)])
            .collect();
        let rows: Vec<(u64, i64, u64, &[i64])> = cells
            .iter()
            .enumerate()
            .map(|(i, c)| (i as u64, 1, u64::from(is_null(i)), &c[..]))
            .collect();
        let dropped = MapPlan::from_wire(&s, &reindex_on(&s, &[1], vec![2], gnitz_wire::NullKeys::Drop))
            .unwrap()
            .evaluate_map_batch(&make_int_batch(&s, &rows));
        let got: Vec<i64> = dropped
            .col_data(0)
            .as_chunks::<8>()
            .0
            .iter()
            .map(|c| i64::from_le_bytes(*c))
            .collect();
        let want: Vec<i64> = (0..200).filter(|&i| !is_null(i)).map(|i| -(i as i64)).collect();
        assert_eq!(got, want, "pattern {p}");
    }
}

/// A `Drop` reindex whose key is NOT NULL has nothing to drop, so it takes the
/// whole-batch path a `Keep` reindex does.
#[test]
fn a_drop_reindex_over_a_not_null_key_has_no_mask() {
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let mask = |key| {
        MapPlan::from_wire(&s, &reindex_on(&s, &[key], vec![2], gnitz_wire::NullKeys::Drop))
            .unwrap()
            .null_key_mask
    };
    assert_eq!(mask(1), 0);
    assert_eq!(mask(0), 0, "a PK column is never nullable");
    assert_eq!(mask(2), 0b10);
}

/// The set-op full-row identity map: a synthetic U128 PK over the projected
/// payload, each slot optionally promoted to a wider fixed-int type so a
/// cross-width pair (`I32 UNION I64`) hashes one physical layout.
#[test]
fn a_hash_row_map_promotes_within_the_copy_kernel_domain_or_is_rejected() {
    let hash_row = |cols: Vec<u32>, tcs: Vec<TypeCode>| gnitz_wire::MapKind::HashRow {
        cols: cols.into_iter().zip(tcs).collect(),
    };
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
        ],
        &[0],
    );
    assert!(
        MapPlan::from_wire(&s, &hash_row(vec![1], vec![TypeCode::U32])).is_ok(),
        "no promotion"
    );
    assert!(
        MapPlan::from_wire(&s, &hash_row(vec![1], vec![TypeCode::I64])).is_ok(),
        "U32 → I64 is the ≤8-byte widen the copy kernel supports"
    );
    assert_eq!(
        wire_rejection(&s, hash_row(vec![9], vec![TypeCode::U32])),
        "hash-row map: column 9 out of range (2 cols)"
    );
    assert!(
        wire_rejection(&s, hash_row(vec![1], vec![TypeCode::String])).starts_with("map: program/schema mismatch"),
        "a German string is not a fixed-int widen"
    );
}

/// A corrupt computed-map blob is refused, not decoded into a plan that maps
/// garbage — and the rejection names the decode, not the schema.
#[test]
fn a_corrupt_compute_map_program_is_rejected() {
    let s = u64_i64();
    let mk = gnitz_wire::MapKind::Compute(gnitz_wire::ComputeMap {
        program: vec![0xff; 8],
        out_cols: vec![(TypeCode::I64, false)],
    });
    assert!(wire_rejection(&s, mk).starts_with("map: invalid program"));
}

/// A declaration wider than a schema holds is refused by the derivation, never
/// reaching `SchemaDescriptor::new`'s release-active `assert!`.
#[test]
fn compute_map_refuses_an_over_wide_declaration() {
    let schema = make_schema(0, &[TypeCode::U64, TypeCode::I64]);
    let map = gnitz_wire::ComputeMap {
        program: vec![1, 2, 3],
        out_cols: (0..MAX_COLUMNS).map(|_| (TypeCode::I64, false)).collect(),
    };
    let Err(err) = MapPlan::from_compute_map(&schema, &map) else {
        panic!("an over-wide compute map must be refused");
    };
    assert!(
        err.to_string().contains("compute map: output exceeds MAX_COLUMNS"),
        "{err}"
    );
}

/// A corrupt program blob is refused by the program decoder, not decoded into a
/// plan that maps garbage.
#[test]
fn compute_map_refuses_a_corrupt_program() {
    let schema = make_schema(0, &[TypeCode::U64, TypeCode::I64]);
    let map = gnitz_wire::ComputeMap {
        program: vec![0xff; 8],
        out_cols: vec![(TypeCode::I64, false)],
    };
    let Err(err) = MapPlan::from_compute_map(&schema, &map) else {
        panic!("a corrupt compute map program must be refused");
    };
    assert!(err.to_string().contains("map: invalid program"), "{err}");
}

/// A `Drop` reindex against the `IS NOT NULL` filter it replaces, at NULL keys
/// spread evenly so every survivor run is short.
/// `cd crates && cargo test -p gnitz-store --release reindex_drop_null_keys_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn reindex_drop_null_keys_bench() {
    use gnitz_expr::LogicalInstr;
    use gnitz_wire::NullKeys;
    use std::hint::black_box;
    const N: usize = 64 * 1024;
    const ITERS: usize = 50;
    let s = make_schema(0, &[TypeCode::U64, TypeCode::I64, TypeCode::I64, TypeCode::I64]);
    let counters = [
        gnitz_foundation::perf::Counter::instructions(),
        gnitz_foundation::perf::Counter::cycles(),
    ];
    let plan = |nulls| MapPlan::from_wire(&s, &reindex_on(&s, &[1], vec![1, 2, 3], nulls)).unwrap();
    let mut not_null = LogicalProgram::new(
        vec![LogicalInstr::IsNull { col: 1, invert: true }],
        vec![Sink::Reg(Reg(0))],
        vec![],
    )
    .resolve_filter(&s)
    .unwrap();

    // A first pass that prints nothing: the allocator's first large batches pay
    // for mapping fresh memory, which would land on whichever arm runs first.
    for (warm, null_every) in [
        (true, 10),
        (false, 0usize),
        (false, 100),
        (false, 32),
        (false, 16),
        (false, 10),
    ] {
        // A NULL key's cell is zero, as ingest guarantees.
        let is_null = |i: usize| null_every != 0 && i.is_multiple_of(null_every);
        let cells: Vec<[i64; 3]> = (0..N)
            .map(|i| {
                [
                    if is_null(i) { 0 } else { (i as i64 * 7919) % 4096 },
                    i as i64,
                    -(i as i64),
                ]
            })
            .collect();
        let rows: Vec<(u64, i64, u64, &[i64])> = cells
            .iter()
            .enumerate()
            .map(|(i, c)| (i as u64, 1, u64::from(is_null(i)), &c[..]))
            .collect();
        let batch = make_int_batch(&s, &rows);
        let (mut keep, mut drop) = (plan(NullKeys::Keep), plan(NullKeys::Drop));
        let pct = if null_every == 0 {
            0.0
        } else {
            100.0 / null_every as f64
        };
        let arm = |name: &str, f: &mut dyn FnMut()| {
            if warm {
                return f();
            }
            let t = crate::test_support::bench_time(ITERS, &mut *f);
            let per_row = |v: f64| v / N as f64;
            let [instrs, cycles] = counters.each_ref().map(|c| {
                c.as_ref()
                    .map_or("-".into(), |c| format!("{:.1}", per_row(c.measure(&mut *f).1 as f64)))
            });
            println!(
                "{pct:>4.1}% NULL  {name:<28} {:>7.2} ns/row  {instrs:>7} instr/row  {cycles:>7} cycles/row",
                per_row(t.as_nanos() as f64 / ITERS as f64),
            );
        };
        arm("(a) filter + reindex", &mut || {
            let filtered = crate::ops::op_filter(&batch, &mut not_null);
            black_box(keep.evaluate_map_batch(filtered.as_ref().unwrap_or(&batch)));
        });
        arm("(a) drop reindex", &mut || {
            black_box(drop.evaluate_map_batch(&batch));
        });
        arm("(b) reindex + clone", &mut || {
            let all = keep.evaluate_map_batch(&batch);
            black_box(Batch::clone(&all));
            black_box(all);
        });
        arm("(b) keep + drop reindex", &mut || {
            black_box(drop.evaluate_map_batch(&batch));
            black_box(keep.evaluate_map_batch(&batch));
        });
    }
}
