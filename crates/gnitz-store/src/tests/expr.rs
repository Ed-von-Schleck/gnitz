use gnitz_expr::{ExprValidateErr, LogicalProgram, Reg, Sink};

use super::{MapPlan, PkSource};
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_COLUMNS};
use crate::storage::Batch;

/// Build a `Batch` of `(pk, weight, null_word, payload i64 cells)` rows against
/// `schema` — the engine-side physical batch these tests drive [`MapPlan`]
/// with, as opposed to the owned-buffer view `gnitz-expr`'s own tests use.
fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, u64, &[i64])]) -> Batch {
    let mut batch = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, weight, null_word, cols) in rows {
        batch.extend_pk(pk as u128);
        batch.extend_weight(&weight.to_le_bytes());
        batch.extend_null_bmp(&null_word.to_le_bytes());
        for (pi, _col) in schema.payload_columns() {
            if pi < cols.len() {
                batch.extend_col(pi, &cols[pi].to_le_bytes());
            }
        }
        batch.count += 1;
    }
    batch
}

fn make_schema(pk_index: u32, col_types: &[u8]) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::EMPTY; MAX_COLUMNS];
    for (i, &tc) in col_types.iter().enumerate() {
        let nullable = if i == pk_index as usize { 0 } else { 1 };
        columns[i] = SchemaColumn::new(tc, nullable);
    }
    SchemaDescriptor::new(&columns[..col_types.len()], &[pk_index])
}

#[test]
fn test_projection_batch() {
    let in_schema = make_schema(0, &[8, 9, 9]);
    let out_schema = make_schema(0, &[8, 9, 9]);
    let batch = make_int_batch(&in_schema, &[(1, 1, 0, &[10, 20]), (2, 1, 0, &[30, 40])]);

    let prog = LogicalProgram::copy_cols(&[2, 1]);
    let func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
    let result = func.evaluate_map_batch(&batch);
    assert_eq!(result.count, 2);

    let r0_col0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
    let r0_col1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(r0_col0, 20);
    assert_eq!(r0_col1, 10);
}

#[test]
fn test_map_copy_and_emit() {
    let in_schema = make_schema(0, &[8, 9, 9]);
    let out_schema = make_schema(0, &[8, 9, 9]);

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
    let prog = LogicalProgram::new(instrs, sinks, None, vec![]);

    let func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
    let result = func.evaluate_map_batch(&batch);
    assert_eq!(result.count, 1);

    let v0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(v0, 10);
    let v1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(v1, 30);
}

#[test]
fn test_empty_batch() {
    let schema = make_schema(0, &[8, 9]);
    let batch = Batch::empty_with_schema(&schema);

    let func = MapPlan::from_map(LogicalProgram::copy_cols(&[1]), &schema, &schema, PkSource::Inherit).unwrap();
    let result = func.evaluate_map_batch(&batch);
    assert_eq!(result.count, 0);
}

#[test]
fn test_map_blob_passthrough_and_fallback() {
    // German-string struct: short (≤12 bytes) inline, else heap-backed.
    fn push_gs(b: &mut Batch, pi: usize, s: &[u8]) {
        let gs = gnitz_wire::encode_german_string(s, &mut b.blob);
        b.extend_col(pi, &gs);
    }
    // Input: [U64 PK, STRING s1 (short inline), STRING s2 (long, heap-backed)].
    fn build(schema: &SchemaDescriptor) -> Batch {
        let mut b = Batch::with_capacity(schema, 2);
        for (pk, s1, s2) in [
            (1u128, b"ab".as_slice(), b"long-string-one-xyz".as_slice()),
            (2u128, b"cd".as_slice(), b"long-string-two-abcdef".as_slice()),
        ] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            push_gs(&mut b, 0, s1); // payload idx 0 = s1
            push_gs(&mut b, 1, s2); // payload idx 1 = s2
            b.count += 1;
        }
        b
    }

    let in_schema = make_schema(0, &[type_code::U64, type_code::STRING, type_code::STRING]);

    // (A) Keep BOTH string columns (reordered) → passthrough fires; the shared
    // blob keeps every long string's heap offset valid through the verbatim copy.
    {
        let batch = build(&in_schema);
        let out_schema = make_schema(0, &[type_code::U64, type_code::STRING, type_code::STRING]);
        let prog = LogicalProgram::copy_cols(&[2, 1]);
        let func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
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

    // (B) Drop the long string s2 → passthrough gated OFF (a dropped string column
    // would leave dead heap in a shared blob), so the relocate path runs and the
    // output blob carries only the referenced (here empty, short-inline) spans.
    {
        let batch = build(&in_schema);
        let out_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
        let prog = LogicalProgram::copy_cols(&[1]);
        let func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
        let out = func.evaluate_map_batch(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(crate::test_support::read_german_string(&out, 0, 0), b"ab");
        assert_eq!(crate::test_support::read_german_string(&out, 0, 1), b"cd");
        assert!(
            out.blob.len() < batch.blob.len(),
            "dropped-string relocate must not copy the dead heap ({} vs {})",
            out.blob.len(),
            batch.blob.len(),
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
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    // Out: same compound PK, then both PK columns copied into payload slots plus
    // the payload passthrough.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U128, 0), // payload 0 ← PK col 0
            SchemaColumn::new(type_code::I64, 0),  // payload 1 ← PK col 1
            SchemaColumn::new(type_code::I64, 0),  // payload 2 ← payload col 2
        ],
        &[0, 1],
    );

    let pk0: u128 = 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210;
    let pk1: i64 = -5; // negative: exercises the OPK sign-bit flip on decode
    let mut batch = Batch::with_capacity(&in_schema, 1);
    batch.extend_pk_opk(&in_schema, &[pk0, pk1 as u64 as u128]);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &42i64.to_le_bytes());
    batch.count += 1;

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
            SchemaColumn::new(type_code::U16, 0),
            SchemaColumn::new(type_code::I16, 0),
            SchemaColumn::new(type_code::I8, 0),
            SchemaColumn::new(type_code::U8, 0),
        ],
        &[0, 1],
    );
    // Every copied column lands in an I64 slot — 4 distinct narrow→wide widens.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U16, 0),
            SchemaColumn::new(type_code::I16, 0),
            SchemaColumn::new(type_code::I64, 0), // ← U16 PK  (zero-extend)
            SchemaColumn::new(type_code::I64, 0), // ← I16 PK  (sign-extend)
            SchemaColumn::new(type_code::I64, 0), // ← I8 payload (sign-extend)
            SchemaColumn::new(type_code::I64, 0), // ← U8 payload (zero-extend)
        ],
        &[0, 1],
    );

    let (c0, c1, c2, c3): (u16, i16, i8, u8) = (0xBEEF, -300, -7, 0xFE);
    let mut batch = Batch::with_capacity(&in_schema, 1);
    batch.extend_pk_opk(&in_schema, &[c0 as u128, c1 as u16 as u128]);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &c2.to_le_bytes());
    batch.extend_col(1, &c3.to_le_bytes());
    batch.count += 1;

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

/// An instruction-free program has no result register — framing accepts it,
/// and `validate` cannot reject it outright because that is exactly the shape
/// every `copy_cols` map has. The *filter* and *scalar* roles are the
/// ones that read a result register back, so each rejects it rather than letting
/// it masquerade as a filter that passes nothing (which a client would read as
/// an empty table).
#[test]
fn test_register_free_predicate_is_rejected() {
    let schema = make_schema(0, &[8, 9]);
    let prog = LogicalProgram::from_wire(&[], &[], None, vec![]).unwrap();
    assert_eq!(
        prog.resolve_filter(&schema).err(),
        Some(ExprValidateErr::ResultRegRequired)
    );
    // The same shape is legitimate as a map: `copy_cols` builds it.
    assert!(MapPlan::from_map(LogicalProgram::copy_cols(&[1]), &schema, &schema, PkSource::Inherit).is_ok());
}

/// The predicate wiring over a real `Batch`, including the range coalescing
/// that turns per-row verdicts into the `(start, end)` list every consumer reads.
#[test]
fn test_from_predicate_filter_ranges_over_a_batch() {
    use gnitz_expr::{CmpOp, LogicalInstr};

    let schema = make_schema(0, &[type_code::U64, type_code::I64]);
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
        LogicalInstr::LoadConst { val: 15 },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
    ];
    let func = LogicalProgram::new(instrs, Vec::new(), Some(Reg(2)), vec![])
        .resolve_filter(&schema)
        .unwrap();

    let mut ranges = Vec::new();
    func.filter_ranges(&batch.as_mem_batch(), &mut ranges);
    assert_eq!(ranges, vec![(1, 3), (4, 6)]);

    // `out` is cleared, not appended to, so a reused buffer cannot leak a
    // previous chunk's ranges into this one.
    func.filter_ranges(&batch.as_mem_batch(), &mut ranges);
    assert_eq!(ranges, vec![(1, 3), (4, 6)]);
}

/// A batch of `(pk, string cells)` rows: one STRING payload column per entry
/// of `cells`, encoded through the blob heap the map must relocate or share.
fn make_string_batch(schema: &SchemaDescriptor, rows: &[&[&[u8]]]) -> Batch {
    let mut batch = Batch::with_capacity(schema, rows.len().max(1));
    for (row, cells) in rows.iter().enumerate() {
        batch.extend_pk(row as u128 + 1);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        for (pi, _col) in schema.payload_columns() {
            let cell = gnitz_wire::encode_german_string(cells[pi], &mut batch.blob);
            batch.extend_col(pi, &cell);
        }
        batch.count += 1;
    }
    batch
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
    let in_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
    let out_schema = make_schema(0, &[type_code::U64, type_code::U64, type_code::STRING]);
    let mut batch = make_string_batch(&in_schema, &[&[b"abc"], &[b"a-long-value-past-twelve"]]);
    // The copied column is the PK, which `PkSource::Inherit` carries verbatim; give
    // the output's first payload slot something to hold.
    batch.count = 2;

    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrCase { a: Reg(0), upper: true },
    ];
    let sinks = vec![Sink::Col(0), Sink::Reg(Reg(1))];
    let func = MapPlan::from_map(
        LogicalProgram::new(instrs, sinks, None, vec![]),
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
    let in_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
    let out_schema = make_schema(0, &[type_code::U64, type_code::STRING, type_code::STRING]);
    let batch = make_string_batch(&in_schema, &[&[b"a-long-value-past-twelve"], &[b"short"]]);

    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrCase { a: Reg(0), upper: true },
    ];
    let sinks = vec![Sink::Col(1), Sink::Reg(Reg(1))];
    let func = MapPlan::from_map(
        LogicalProgram::new(instrs, sinks, None, vec![]),
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
    assert!(
        !out.shares_blob_with(&batch.as_mem_batch()),
        "appending the emitted bytes must end the sharing, so a later append relocates"
    );
}

/// A NULL string row must ship a zeroed cell *and* its bitmap bit — a
/// non-deterministic value there would leave an insert and its retraction
/// unable to cancel.
#[test]
fn null_string_emit_zeroes_the_cell_and_sets_the_bit() {
    use gnitz_expr::LogicalInstr;
    let in_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
    let out_schema = make_schema(0, &[type_code::U64, type_code::U64, type_code::STRING]);
    let mut batch = make_string_batch(&in_schema, &[&[b"abc"], &[b"def"]]);
    // Row 1's source string is NULL.
    gnitz_wire::write_u64_le(batch.null_bmp_data_mut(), 8, 1);

    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrCase { a: Reg(0), upper: true },
    ];
    let sinks = vec![Sink::Col(0), Sink::Reg(Reg(1))];
    let func = MapPlan::from_map(
        LogicalProgram::new(instrs, sinks, None, vec![]),
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
    use crate::schema::key::ReindexPacker;
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
    let packer = ReindexPacker::new(&schema, &[(1, None)]).unwrap();
    let plan = MapPlan::from_map(
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
    // with its sign bit flipped, matching how `ColumnLocator::route_key` routes the same
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

    let in_schema = make_schema(0, &[type_code::U64, type_code::I64, type_code::STRING, type_code::BLOB]);
    let mut batch = Batch::with_capacity(&in_schema, rows.len());
    for (row, &(v, is_null, s, b)) in rows.iter().enumerate() {
        batch.extend_pk(row as u128);
        batch.extend_weight(&1i64.to_le_bytes());
        // Bit 0 is payload slot 0 — the I64 column.
        batch.extend_null_bmp(&(is_null as u64).to_le_bytes());
        batch.extend_col(0, &v.to_le_bytes());
        for (pi, content) in [(1, s), (2, b)] {
            let cell = gnitz_wire::encode_german_string(content, &mut batch.blob);
            batch.extend_col(pi, &cell);
        }
        batch.count += 1;
    }

    // The hash-row output schema: one synthetic U128 PK, then the payload.
    let out_schema = make_schema(
        0,
        &[type_code::U128, type_code::I64, type_code::STRING, type_code::BLOB],
    );
    let plan = |branch_id| {
        MapPlan::from_map(
            LogicalProgram::copy_cols(&[1, 2, 3]),
            &in_schema,
            &out_schema,
            PkSource::HashRow { branch_id },
        )
        .unwrap()
    };
    let out = plan(0).evaluate_map_batch(&batch);
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

    // The branch discriminator: the same row on the other side of a UNION ALL
    // must not collide, or the two would consolidate to one element.
    let other = plan(1).evaluate_map_batch(&batch);
    assert_ne!(pk(0), other.get_pk(0), "the branch id must reach the digest");
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
        type_code::I8,
        type_code::U16,
        type_code::I32,
        type_code::U64,
        type_code::I128,
    ];
    // PK-list order, deliberately not column order: each column's OPK byte
    // offset comes from this list, so a width mixed up here would be caught as
    // a wrong value rather than a wrong length.
    let pk_order: [u32; 5] = [3, 0, 4, 1, 2];

    let mut cols = [SchemaColumn::EMPTY; MAX_COLUMNS];
    for (i, &t) in pk_types.iter().enumerate() {
        cols[i] = SchemaColumn::new(t, 0);
    }
    let in_schema = SchemaDescriptor::new(&cols[..5], &pk_order);
    // The output inherits the PK and adds one payload column per PK column, at
    // that column's own type — so the copy widens nothing and the bytes written
    // are exactly what the decode produced.
    let mut out_cols = cols;
    for (i, &t) in pk_types.iter().enumerate() {
        out_cols[5 + i] = SchemaColumn::new(t, 1);
    }
    let out_schema = SchemaDescriptor::new(&out_cols[..10], &pk_order);

    // Both extremes and a wrap-adjacent value per width: the sign flip is what
    // orders these, and dropping it shows up first at the ends.
    let rows: [[i128; 5]; 3] = [
        [i8::MIN as i128, 0, i32::MIN as i128, 0, i128::MIN],
        [-1, 40_000, -1, u64::MAX as i128, -1],
        [i8::MAX as i128, u16::MAX as i128, i32::MAX as i128, 1, i128::MAX],
    ];
    let mut batch = Batch::with_capacity(&in_schema, rows.len());
    for vals in &rows {
        let natives: Vec<u128> = pk_order.iter().map(|&ci| vals[ci as usize] as u128).collect();
        batch.extend_pk_opk(&in_schema, &natives);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.count += 1;
    }

    let plan = MapPlan::from_map(
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
            let w = gnitz_wire::wire_stride(t);
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

/// The whole `Instr::Map` body — the copy loop plus the PK stamp — over a
/// reindex map and a string-emitting map. `reindex_pack_bench` covers the
/// packer alone, and neither `make bench` nor the `scan_spec` benches
/// resolve this loop.
///
/// `cd crates && cargo test -p gnitz-store --release map_ranges_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn map_ranges_bench() {
    use crate::schema::key::ReindexPacker;
    use gnitz_expr::LogicalInstr;
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 200_000;
    const ITERS: usize = 20;

    // --- Reindex map: [U64 PK, I64, I64] reindexed on col 1, the source PK and
    // both payload columns kept — the equijoin / GROUP BY repartition shape.
    // Column 0 is what puts a `ColumnLocator::Pk` copy in the loop; the keep-set
    // rules retain the source PK on every one of those.
    let rx_in = make_schema(0, &[type_code::U64, type_code::I64, type_code::I64]);
    let mut rx_batch = Batch::with_capacity(&rx_in, N);
    for i in 0..N as u64 {
        rx_batch.extend_pk(i as u128);
        rx_batch.extend_weight(&1i64.to_le_bytes());
        rx_batch.extend_null_bmp(&0u64.to_le_bytes());
        rx_batch.extend_col(0, &i.wrapping_mul(2_654_435_761).to_le_bytes());
        rx_batch.extend_col(1, &(!i).to_le_bytes());
        rx_batch.count += 1;
    }
    let rx_packer = ReindexPacker::new(&rx_in, &[(1, None)]).unwrap();
    let rx_out = rx_packer.output_schema(&rx_in, &[0, 1, 2]).unwrap();
    let rx_plan = MapPlan::from_map(
        LogicalProgram::copy_cols(&[0, 1, 2]),
        &rx_in,
        &rx_out,
        PkSource::Pack(rx_packer),
    )
    .unwrap();

    // --- String-emitting map: [U64 PK, STRING] -> [U64 PK, STRING upper],
    // the shape whose STRING output comes only from `str_emits`.
    let se_in = make_schema(0, &[type_code::U64, type_code::STRING]);
    let se_out = make_schema(0, &[type_code::U64, type_code::STRING]);
    let mut se_batch = Batch::with_capacity(&se_in, N);
    for i in 0..N {
        se_batch.extend_pk(i as u128);
        se_batch.extend_weight(&1i64.to_le_bytes());
        se_batch.extend_null_bmp(&0u64.to_le_bytes());
        // Past SHORT_STRING_THRESHOLD, so every cell is heap-backed and the
        // emit grows the output blob.
        let v = format!("row-{i:012}-payload");
        let cell = gnitz_wire::encode_german_string(v.as_bytes(), &mut se_batch.blob);
        se_batch.extend_col(0, &cell);
        se_batch.count += 1;
    }
    let se_plan = MapPlan::from_map(
        LogicalProgram::new(
            vec![
                LogicalInstr::LoadColStr { col: 1 },
                LogicalInstr::StrCase { a: Reg(0), upper: true },
            ],
            vec![Sink::Reg(Reg(1))],
            None,
            vec![],
        ),
        &se_in,
        &se_out,
        PkSource::Inherit,
    )
    .unwrap();

    for (name, plan, src) in [("reindex", &rx_plan, &rx_batch), ("str_emit", &se_plan, &se_batch)] {
        let t0 = Instant::now();
        let mut acc = 0usize;
        for _ in 0..ITERS {
            let out = plan.evaluate_map_batch(black_box(src));
            acc = acc.wrapping_add(out.count).wrapping_add(out.blob.len());
            black_box(&out);
        }
        let secs = t0.elapsed().as_secs_f64();
        println!(
            "map_ranges_bench[{name}]: {:.1} Mrows/s ({N} rows x {ITERS} iters in {secs:.3}s, checksum {acc})",
            (N * ITERS) as f64 / secs / 1e6,
        );
    }
}
