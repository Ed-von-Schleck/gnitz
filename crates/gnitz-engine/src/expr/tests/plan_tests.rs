use super::super::plan::ScalarFunc;
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

    let func = ScalarFunc::from_projection(&[2, 1], &[type_code::I64, type_code::I64], &in_schema, &out_schema);
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
    let batch = Batch::empty(1, 16);

    let func = ScalarFunc::from_projection(&[1], &[type_code::I64], &schema, &schema);
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
        let func = ScalarFunc::from_projection(
            &[2, 1],
            &[type_code::STRING, type_code::STRING],
            &in_schema,
            &out_schema,
        );
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
        let func = ScalarFunc::from_projection(&[1], &[type_code::STRING], &in_schema, &out_schema);
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
