use super::*;
use gnitz_wire::{ColType, ColumnDef};

/// A table keyed on one column `k` of `key`, then `payload`.
fn table(key: TypeCode, payload: &[ColumnDef]) -> Arc<Schema> {
    let mut cols = vec![ColumnDef::new("k", key, false)];
    cols.extend_from_slice(payload);
    Arc::new(Schema::from_parts(cols, &[0]).unwrap())
}

/// One column of every type, every other one nullable and NULL in odd rows,
/// under a compound key of a signed, an unsigned and a 16-byte column.
fn every_type() -> Arc<Schema> {
    let mut cols = vec![
        ColumnDef::new("k1", TypeCode::I16, false),
        ColumnDef::new("k2", TypeCode::U32, false),
        ColumnDef::new("k3", TypeCode::UUID, false),
    ];
    for (i, &tc) in TypeCode::ALL.iter().enumerate() {
        let ty = if tc == TypeCode::Decimal {
            ColType::decimal(2)
        } else {
            ColType::of(tc)
        };
        cols.push(ColumnDef::new(format!("c{i}"), ty, i % 2 == 1));
    }
    Arc::new(Schema::from_parts(cols, &[0, 1, 2]).unwrap())
}

/// Six rows of `schema`, each column's values its type's extremes and odd ones.
fn rows(schema: &Arc<Schema>) -> ZSetBatch {
    const UUID: u128 = 0x0123_4567_89ab_cdef_0123_4567_89ab_cdef;
    let mut batch = ZSetBatch::new(schema);
    let mut app = batch.builder();
    for row in 0..6usize {
        let pick = |lo: i128, hi: i128| [lo, hi, (-1).clamp(lo, hi)][row % 3];
        let k2 = [row as u128, u128::from(u32::MAX) - row as u128, (1 << 31) + row as u128][row % 3];
        let r = app.begin_row_natives(&[pick(-32768, 32767) as u128, k2, UUID ^ row as u128], 1);
        for (_, _, def) in schema.payload_columns() {
            if def.is_nullable && row % 2 == 1 {
                r.put_null();
                continue;
            }
            match def.ty.tc {
                TypeCode::Bool => r.put_int(((row / 2) % 2) as u128),
                TypeCode::F32 => r.put_float(f64::from([f32::NAN, -0.0, f32::MAX][row % 3])),
                TypeCode::F64 => r.put_float([f64::from_bits(0x7ff8_dead_beef_0001), -0.0, f64::MIN_POSITIVE][row % 3]),
                TypeCode::U128 => r.put_int([0, u128::MAX, 1][row % 3]),
                TypeCode::I128 => r.put_int([i128::MIN, i128::MAX, -1][row % 3] as u128),
                TypeCode::UUID => r.put_int(UUID.rotate_left(row as u32)),
                TypeCode::String => r.put_string(["", "héllo", "a string longer than a German inline"][row % 3]),
                TypeCode::Blob => r.put_blob(&[0, 255, row as u8]),
                tc => {
                    let (lo, hi) = FixedInt::from_type_code(tc).expect("an integer type").range();
                    r.put_int(pick(lo, hi) as u128)
                }
            };
        }
    }
    batch
}

/// Every type's cells read back bit for bit, from a file whose Parquet schema
/// is the one a reader outside gnitz reads, and whose `gnitz.columns` names
/// each column's gnitz type.
#[test]
fn every_type_reads_back_bit_for_bit() {
    let schema = every_type();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.parquet");
    let batch = rows(&schema);
    let mut w = create(&path, &schema).unwrap();
    // Two row groups, one empty batch between them.
    write(&mut w, &batch).unwrap();
    write(&mut w, &ZSetBatch::new(&schema)).unwrap();
    write(&mut w, &batch).unwrap();
    assert_eq!(finish(w).unwrap(), 12);

    let groups: Vec<ZSetBatch> = open(&path, &schema).unwrap().map(Result::unwrap).collect();
    assert_eq!(groups.len(), 2);
    for group in groups {
        assert_eq!(group, batch);
    }

    let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    let meta = reader.metadata().file_metadata();
    let mut printed = Vec::new();
    parquet::schema::printer::print_schema(&mut printed, meta.schema());
    assert_eq!(String::from_utf8(printed).unwrap(), PARQUET_SCHEMA);
    let types = meta
        .key_value_metadata()
        .and_then(|kv| kv.iter().find(|k| k.key == COLUMNS_KEY))
        .and_then(|k| k.value.as_deref())
        .unwrap();
    let types: Vec<String> = serde_json::from_str(types).unwrap();
    assert_eq!(
        types,
        [
            "SMALLINT",
            "INT UNSIGNED",
            "UUID",
            "TINYINT UNSIGNED",
            "TINYINT",
            "SMALLINT UNSIGNED",
            "SMALLINT",
            "INT UNSIGNED",
            "INT",
            "FLOAT",
            "BIGINT UNSIGNED",
            "BIGINT",
            "DOUBLE",
            "TEXT",
            "UHUGEINT",
            "UUID",
            "BLOB",
            "HUGEINT",
            "DATE",
            "TIMESTAMP",
            "DECIMAL(18, 2)",
            "BOOLEAN",
        ]
    );
}

/// [`every_type`]'s file as `parquet::schema::printer` prints it.
const PARQUET_SCHEMA: &str = "message gnitz {
  REQUIRED INT32 k1 (INTEGER(16,true));
  REQUIRED INT32 k2 (INTEGER(32,false));
  REQUIRED FIXED_LEN_BYTE_ARRAY (16) k3 (UUID);
  REQUIRED INT32 c0 (INTEGER(8,false));
  OPTIONAL INT32 c1 (INTEGER(8,true));
  REQUIRED INT32 c2 (INTEGER(16,false));
  OPTIONAL INT32 c3 (INTEGER(16,true));
  REQUIRED INT32 c4 (INTEGER(32,false));
  OPTIONAL INT32 c5 (INTEGER(32,true));
  REQUIRED FLOAT c6;
  OPTIONAL INT64 c7 (INTEGER(64,false));
  REQUIRED INT64 c8 (INTEGER(64,true));
  OPTIONAL DOUBLE c9;
  REQUIRED BYTE_ARRAY c10 (STRING);
  OPTIONAL FIXED_LEN_BYTE_ARRAY (16) c11;
  REQUIRED FIXED_LEN_BYTE_ARRAY (16) c12 (UUID);
  OPTIONAL BYTE_ARRAY c13;
  REQUIRED FIXED_LEN_BYTE_ARRAY (16) c14;
  OPTIONAL INT32 c15 (DATE);
  REQUIRED INT64 c16 (TIMESTAMP(MICROS,false));
  OPTIONAL FIXED_LEN_BYTE_ARRAY (9) c17 (DECIMAL(19,2));
  REQUIRED BOOLEAN c18;
}
";

#[test]
fn a_row_not_at_weight_one_is_refused() {
    let schema = table(TypeCode::I64, &[]);
    let dir = tempfile::tempdir().unwrap();
    let mut w = create(&dir.path().join("t.parquet"), &schema).unwrap();
    let mut batch = ZSetBatch::new(&schema);
    batch.builder().begin_row(1, 2);
    assert_eq!(write(&mut w, &batch).unwrap_err().0, "a base-table row at weight 2");
}

/// A file is refused for a table whose columns differ in number, name, type
/// or nullability, a type though its Parquet column is the same physical type,
/// or the same Parquet type altogether; and any file for a table with a hidden
/// column.
#[test]
fn a_file_of_other_columns_is_refused() {
    let v = |tc| ColumnDef::new("v", tc, false);
    let dir = tempfile::tempdir().unwrap();
    for (i, (written, opened)) in [
        (vec![v(TypeCode::Date)], vec![v(TypeCode::I32)]),
        (vec![v(TypeCode::U128)], vec![v(TypeCode::I128)]),
        (vec![v(TypeCode::I64)], vec![ColumnDef::new("w", TypeCode::I64, false)]),
        (vec![v(TypeCode::I64)], vec![ColumnDef::new("v", TypeCode::I64, true)]),
        (
            vec![v(TypeCode::I64)],
            vec![v(TypeCode::I64), ColumnDef::new("w", TypeCode::I64, false)],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let path = dir.path().join(format!("{i}.parquet"));
        let written = table(TypeCode::I64, &written);
        finish(create(&path, &written).unwrap()).unwrap();
        assert_eq!(open(&path, &written).unwrap().count(), 0);
        let err = open(&path, &table(TypeCode::I64, &opened)).err().unwrap();
        assert_eq!(err.0, "its columns are not the table's", "{i}");
    }
    let hidden = table(TypeCode::I64, &[v(TypeCode::I64).hidden()]);
    let err = open(&dir.path().join("0.parquet"), &hidden).err().unwrap();
    assert_eq!(err.0, "restored into a table with hidden columns");
}

/// A file another writer made reads as the values it holds, a DECIMAL as nine
/// and a HUGEINT as sixteen big-endian bytes, and a cell its column cannot
/// hold is refused rather than wrapped.
#[test]
fn a_file_another_writer_made_reads_as_it_holds() {
    let decimal = |sign: u8, x: i64| [&[sign][..], &x.to_be_bytes()].concat();
    let dir = tempfile::tempdir().unwrap();
    for (i, (key, v, k, cell, read)) in [
        (
            TypeCode::I8,
            ColType::decimal(2),
            -128,
            decimal(0xff, -2),
            Ok((-128, -2)),
        ),
        (
            TypeCode::I8,
            ColType::decimal(2),
            300,
            decimal(0, 1),
            Err("column k: 300 is out of range for TINYINT"),
        ),
        // 2^63, one past `i64::MAX`.
        (
            TypeCode::I64,
            ColType::decimal(2),
            1,
            decimal(0, i64::MIN),
            Err("column v: a DECIMAL cell out of range of a 64-bit integer"),
        ),
        (
            TypeCode::I64,
            ColType::of(TypeCode::I128),
            1,
            (-1i128).to_be_bytes().to_vec(),
            Ok((1, -1)),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let schema = table(key, &[ColumnDef::new("v", v, false)]);
        let path = dir.path().join(format!("{i}.parquet"));
        let mut w = create(&path, &schema).unwrap();
        let mut group = w.next_row_group().unwrap();
        while let Some(mut col) = group.next_column().unwrap() {
            match col.untyped() {
                ColumnWriter::Int32ColumnWriter(c) => c.write_batch(&[k as i32], None, None),
                ColumnWriter::Int64ColumnWriter(c) => c.write_batch(&[k], None, None),
                ColumnWriter::FixedLenByteArrayColumnWriter(c) => {
                    c.write_batch(&[FixedLenByteArray::from(cell.clone())], None, None)
                }
                _ => unreachable!("no other column"),
            }
            .unwrap();
            col.close().unwrap();
        }
        group.close().unwrap();
        finish(w).unwrap();
        let got = open(&path, &schema).unwrap().next().unwrap();
        match read {
            Ok((k, v)) => {
                let mut want = ZSetBatch::new(&schema);
                want.builder().begin_row(k as u128, 1).put_int(v as u128);
                assert_eq!(got.unwrap(), want, "{i}");
            }
            Err(refusal) => assert_eq!(got.unwrap_err().0, refusal, "{i}"),
        }
    }
}
