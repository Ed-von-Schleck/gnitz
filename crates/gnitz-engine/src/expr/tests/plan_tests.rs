use super::super::plan::ScalarFunc;
use super::super::program::LogicalProgram;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_COLUMNS};
use crate::storage::Batch;

fn make_schema(pk_index: u32, col_types: &[u8]) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::new(0, 0); MAX_COLUMNS];
    for (i, &tc) in col_types.iter().enumerate() {
        let nullable = if i == pk_index as usize { 0 } else { 1 };
        columns[i] = SchemaColumn::new(tc, nullable);
    }
    SchemaDescriptor::new(&columns[..col_types.len()], &[pk_index])
}

fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, u64, &[i64])]) -> Batch {
    let mut batch = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, weight, null_word, cols) in rows {
        batch.extend_pk(pk as u128);
        batch.extend_weight(&weight.to_le_bytes());
        batch.extend_null_bmp(&null_word.to_le_bytes());
        for (pi, _ci, _col) in schema.payload_columns() {
            if pi < cols.len() {
                batch.extend_col(pi, &cols[pi].to_le_bytes());
            }
        }
        batch.count += 1;
    }
    batch
}

#[test]
fn test_projection_batch() {
    let in_schema = make_schema(0, &[8, 9, 9]);
    let out_schema = make_schema(0, &[8, 9, 9]);
    let batch = make_int_batch(&in_schema, &[(1, 1, 0, &[10, 20]), (2, 1, 0, &[30, 40])]);

    let prog = LogicalProgram::copy_cols(&[(2, type_code::I64), (1, type_code::I64)]);
    let func = ScalarFunc::from_map(prog, &in_schema, &out_schema);
    let result = func.evaluate_map_batch(&batch, &out_schema);
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

    use crate::expr::LogicalInstr;
    let instrs = vec![
        LogicalInstr::CopyCol {
            src_col: 1,
            out: 0,
            tc: type_code::I64,
        },
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
        LogicalInstr::Emit { src: 2, out: 1 },
    ];
    let prog = crate::expr::LogicalProgram::new(instrs, 3, 2, vec![]);

    let func = ScalarFunc::from_map(prog, &in_schema, &out_schema);
    let result = func.evaluate_map_batch(&batch, &out_schema);
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

    let func = ScalarFunc::from_map(LogicalProgram::copy_cols(&[(1, type_code::I64)]), &schema, &schema);
    let result = func.evaluate_map_batch(&batch, &schema);
    assert_eq!(result.count, 0);
}

#[test]
fn test_map_blob_passthrough_and_fallback() {
    use crate::schema::german_string_content;

    // German-string struct: short (≤12 bytes) inline, else heap-backed.
    fn push_gs(b: &mut Batch, pi: usize, s: &[u8]) {
        let gs = crate::test_support::german_string(s, &mut b.blob);
        b.extend_col(pi, &gs);
    }
    fn read_gs(batch: &Batch, pi: usize, row: usize) -> Vec<u8> {
        german_string_content(&batch.col_data(pi)[row * 16..row * 16 + 16], &batch.blob).to_vec()
    }
    // Input: [U64 PK, STRING s1 (short inline), STRING s2 (long, heap-backed)].
    fn build(schema: &SchemaDescriptor) -> Batch {
        let mut b = Batch::with_schema(*schema, 2);
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
        let prog = LogicalProgram::copy_cols(&[(2, type_code::STRING), (1, type_code::STRING)]);
        let func = ScalarFunc::from_map(prog, &in_schema, &out_schema);
        let out = func.evaluate_map_batch(&batch, &out_schema);
        assert_eq!(out.count, 2);
        assert_eq!(read_gs(&out, 0, 0), b"long-string-one-xyz"); // s2 → out payload 0
        assert_eq!(read_gs(&out, 1, 0), b"ab"); // s1 → out payload 1
        assert_eq!(read_gs(&out, 0, 1), b"long-string-two-abcdef");
        assert_eq!(read_gs(&out, 1, 1), b"cd");
    }

    // (B) Drop the long string s2 → passthrough gated OFF (a dropped string column
    // would leave dead heap in a shared blob), so the relocate path runs and the
    // output blob carries only the referenced (here empty, short-inline) spans.
    {
        let batch = build(&in_schema);
        let out_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
        let prog = LogicalProgram::copy_cols(&[(1, type_code::STRING)]);
        let func = ScalarFunc::from_map(prog, &in_schema, &out_schema);
        let out = func.evaluate_map_batch(&batch, &out_schema);
        assert_eq!(out.count, 2);
        assert_eq!(read_gs(&out, 0, 0), b"ab");
        assert_eq!(read_gs(&out, 0, 1), b"cd");
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
    let mut batch = Batch::with_schema(in_schema, 1);
    batch.extend_pk_opk(&in_schema, &[pk0, pk1 as u64 as u128]);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &42i64.to_le_bytes());
    batch.count += 1;

    let prog = LogicalProgram::copy_cols(&[
        (0, type_code::U128), // PK col 0 → payload 0
        (1, type_code::I64),  // PK col 1 → payload 1
        (2, type_code::I64),  // payload col 2 → payload 2
    ]);
    let out = ScalarFunc::from_map(prog, &in_schema, &out_schema).evaluate_map_batch(&batch, &out_schema);

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
    let mut batch = Batch::with_schema(in_schema, 1);
    batch.extend_pk_opk(&in_schema, &[c0 as u128, c1 as u16 as u128]);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &c2.to_le_bytes());
    batch.extend_col(1, &c3.to_le_bytes());
    batch.count += 1;

    let prog = LogicalProgram::copy_cols(&[
        (0, type_code::U16),
        (1, type_code::I16),
        (2, type_code::I8),
        (3, type_code::U8),
    ]);
    let out = ScalarFunc::from_map(prog, &in_schema, &out_schema).evaluate_map_batch(&batch, &out_schema);

    assert_eq!(out.count, 1);
    let widened = |pi: usize| i64::from_le_bytes(out.col_data(pi)[..8].try_into().unwrap());
    assert_eq!(widened(0), c0 as i64, "U16 PK zero-extends");
    assert_eq!(widened(1), c1 as i64, "I16 PK sign-extends");
    assert_eq!(widened(2), c2 as i64, "I8 payload sign-extends");
    assert_eq!(widened(3), c3 as i64, "U8 payload zero-extends");
}
