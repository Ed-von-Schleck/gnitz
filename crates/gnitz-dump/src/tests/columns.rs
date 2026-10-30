use super::*;
use gnitz_core::BatchAppender;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::FixedLenByteArray;

/// One column of every type, every other one nullable and NULL in odd rows,
/// under a compound key of a signed and an unsigned column.
fn every_type() -> Arc<Schema> {
    let mut cols = vec![
        ColumnDef::new("k1", TypeCode::I16, false),
        ColumnDef::new("k2", TypeCode::U32, false),
    ];
    for (i, &tc) in TypeCode::ALL.iter().enumerate() {
        let ty = if tc == TypeCode::Decimal {
            ColType::decimal(2)
        } else {
            ColType::of(tc)
        };
        cols.push(ColumnDef::new(format!("c{i}"), ty, i % 2 == 1));
    }
    Arc::new(Schema::from_parts(cols, &[0, 1]).unwrap())
}

/// Six rows of `schema`, each column's values its type's extremes and odd ones.
fn rows(schema: &Schema) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut batch);
    for row in 0..6usize {
        let pick = |lo: i128, hi: i128| [lo, hi, (-1).clamp(lo, hi)][row % 3];
        let k2 = [row as u128, u128::from(u32::MAX) - row as u128, (1 << 31) + row as u128][row % 3];
        let r = app.add_row_natives(&[pick(-32768, 32767) as u128, k2], 1);
        for (_, _, def) in schema.payload_columns() {
            if def.is_nullable && row % 2 == 1 {
                r.null();
                continue;
            }
            match def.ty.tc {
                TypeCode::Bool => r.int_val(((row / 2) % 2) as i128),
                TypeCode::F32 => r.f32_val([f32::NAN, -0.0, f32::MAX][row % 3]),
                TypeCode::F64 => r.f64_val([f64::from_bits(0x7ff8_dead_beef_0001), -0.0, f64::MIN_POSITIVE][row % 3]),
                TypeCode::U128 => r.u128_val([0, u128::MAX, 1][row % 3]),
                TypeCode::I128 => r.u128_val([i128::MIN, i128::MAX, -1][row % 3] as u128),
                TypeCode::UUID => r.u128_val(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef ^ row as u128),
                TypeCode::String => r.str_val(["", "héllo", "a string longer than a German inline"][row % 3]),
                TypeCode::Blob => r.bytes_val(&[0, 255, row as u8]),
                tc => {
                    let (lo, hi) = FixedInt::from_type_code(tc).expect("an integer type").range();
                    r.int_val(pick(lo, hi))
                }
            };
        }
    }
    batch
}

#[test]
fn every_type_reads_back_bit_for_bit() {
    let schema = every_type();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.parquet");
    let batch = rows(&schema);
    let mut w = TableWriter::create(&path, &schema).unwrap();
    // Two row groups, one empty batch between them.
    w.write(&batch).unwrap();
    w.write(&ZSetBatch::new(&schema)).unwrap();
    w.write(&batch).unwrap();
    assert_eq!(w.finish().unwrap(), 12);

    let mut r = TableReader::open(&path, &schema).unwrap();
    let mut groups = 0;
    while let Some(back) = r.next_group().unwrap() {
        assert_eq!(back, batch);
        groups += 1;
    }
    assert_eq!(groups, 2);
}

#[test]
fn a_row_not_at_weight_one_is_refused() {
    let schema = every_type();
    let dir = tempfile::tempdir().unwrap();
    let mut w = TableWriter::create(&dir.path().join("t.parquet"), &schema).unwrap();
    let mut batch = rows(&schema);
    batch.weights[2] = 2;
    let err = w.write(&batch).unwrap_err();
    assert!(err.0.contains("weight 2"), "{err}");
}

/// A file is refused for a table whose columns differ in number, name, type
/// or nullability, a type though its Parquet column is the same physical type,
/// or the same Parquet type altogether.
#[test]
fn a_file_of_other_columns_is_refused() {
    let table = |payload: &[ColumnDef]| {
        let mut cols = vec![ColumnDef::new("k", TypeCode::I64, false)];
        cols.extend_from_slice(payload);
        Arc::new(Schema::from_parts(cols, &[0]).unwrap())
    };
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
        TableWriter::create(&path, &table(&written)).unwrap().finish().unwrap();
        TableReader::open(&path, &table(&written)).unwrap();
        let err = TableReader::open(&path, &table(&opened)).err().unwrap();
        assert!(err.0.contains("its columns are not the table's"), "{i}: {err}");
    }
}

#[test]
fn the_file_names_each_columns_gnitz_type() {
    let schema = every_type();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.parquet");
    TableWriter::create(&path, &schema).unwrap().finish().unwrap();
    let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    let kv = reader.metadata().file_metadata().key_value_metadata().unwrap();
    let types = kv
        .iter()
        .find(|k| k.key == COLUMNS_KEY)
        .and_then(|k| k.value.clone())
        .unwrap();
    let types: Vec<String> = serde_json::from_str(&types).unwrap();
    assert_eq!(
        types,
        [
            "SMALLINT",
            "INT UNSIGNED",
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

/// A file this writer never produces, a cell its column cannot hold, is
/// refused: an I8 key holding 300, a DECIMAL's nine bytes holding 2^63, one
/// past `i64::MAX`, rather than wrapped, and TEXT that is not UTF-8.
#[test]
fn a_cell_its_column_cannot_hold_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    for (i, (key, v, refusal)) in [
        (TypeCode::I8, ColType::of(TypeCode::I64), "out of range"),
        (TypeCode::I64, ColType::decimal(2), "out of range"),
        (TypeCode::I64, ColType::of(TypeCode::String), "not UTF-8"),
    ]
    .into_iter()
    .enumerate()
    {
        let schema = Arc::new(
            Schema::from_parts(
                vec![ColumnDef::new("k", key, false), ColumnDef::new("v", v, false)],
                &[0],
            )
            .unwrap(),
        );
        let path = dir.path().join(format!("{i}.parquet"));
        let (root, types) = file_layout(&schema).unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(COLUMNS_KEY.to_string(), types)]))
            .build();
        let mut w = SerializedFileWriter::new(File::create(&path).unwrap(), Arc::new(root), Arc::new(props)).unwrap();
        let mut group = w.next_row_group().unwrap();
        while let Some(mut col) = group.next_column().unwrap() {
            match col.untyped() {
                ColumnWriter::Int32ColumnWriter(c) => c.write_batch(&[300], None, None),
                ColumnWriter::Int64ColumnWriter(c) => c.write_batch(&[1], None, None),
                ColumnWriter::FixedLenByteArrayColumnWriter(c) => {
                    let cell = FixedLenByteArray::from(vec![0x00, 0x80, 0, 0, 0, 0, 0, 0, 0]);
                    c.write_batch(&[cell], None, None)
                }
                ColumnWriter::ByteArrayColumnWriter(c) => c.write_batch(&[ByteArray::from(vec![0xff])], None, None),
                _ => unreachable!("no other column"),
            }
            .unwrap();
            col.close().unwrap();
        }
        group.close().unwrap();
        w.close().unwrap();
        let err = TableReader::open(&path, &schema).unwrap().next_group().unwrap_err();
        assert!(err.0.contains(refusal), "{i}: {err}");
    }
}
