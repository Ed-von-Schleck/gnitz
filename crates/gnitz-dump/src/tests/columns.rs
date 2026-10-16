use super::*;
use gnitz_core::BatchAppender;
use gnitz_wire::ColType;

/// One column of every type, the nullable ones NULL in odd rows, under a
/// compound key of a signed and an unsigned column.
fn every_type() -> Arc<Schema> {
    let mut cols = vec![
        ColumnDef::new("k1", TypeCode::I16, false),
        ColumnDef::new("k2", TypeCode::U32, false),
    ];
    let types = [
        TypeCode::U8,
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I16,
        TypeCode::U32,
        TypeCode::I32,
        TypeCode::U64,
        TypeCode::I64,
        TypeCode::U128,
        TypeCode::I128,
        TypeCode::UUID,
        TypeCode::F32,
        TypeCode::F64,
        TypeCode::String,
        TypeCode::Blob,
        TypeCode::Date,
        TypeCode::Timestamp,
        TypeCode::Bool,
    ];
    for (i, tc) in types.into_iter().enumerate() {
        cols.push(ColumnDef::new(format!("c{i}"), tc, i % 2 == 1));
    }
    cols.push(ColumnDef::typed("dec".to_string(), ColType::decimal(2), true));
    Arc::new(Schema::from_parts(cols, &[0, 1]).unwrap())
}

fn rows(schema: &Schema) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut batch);
    let extremes: [i128; 3] = [0, 1, -1];
    for row in 0..6i128 {
        let null = row % 2 == 1;
        let pick = |lo: i128, hi: i128| [lo, hi, extremes[(row % 3) as usize].clamp(lo, hi)][(row % 3) as usize];
        let r = app.add_row_natives(
            &[
                (pick(-32768, 32767) as u128),
                pick(0, u32::MAX as i128) as u128 + row as u128,
            ],
            1,
        );
        let opt = |r: &mut BatchAppender<'_>, f: &dyn Fn(&mut BatchAppender<'_>)| match null {
            true => {
                r.null();
            }
            false => f(r),
        };
        r.int_val(pick(0, 255));
        opt(r, &|r| {
            r.int_val(pick(-128, 127));
        });
        r.int_val(pick(0, 65535));
        opt(r, &|r| {
            r.int_val(pick(-32768, 32767));
        });
        r.int_val(pick(0, u32::MAX as i128));
        opt(r, &|r| {
            r.int_val(pick(i32::MIN as i128, i32::MAX as i128));
        });
        r.int_val(pick(0, u64::MAX as i128));
        opt(r, &|r| {
            r.int_val(pick(i64::MIN as i128, i64::MAX as i128));
        });
        r.u128_val([0, u128::MAX, 1][(row % 3) as usize]);
        opt(r, &|r| {
            r.u128_val([i128::MIN, i128::MAX, -1][(row % 3) as usize] as u128);
        });
        r.u128_val(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef ^ row as u128);
        opt(r, &|r| {
            r.f32_val([f32::NAN, -0.0, f32::MAX][(row % 3) as usize]);
        });
        r.f64_val([f64::from_bits(0x7ff8_dead_beef_0001), -0.0, f64::MIN_POSITIVE][(row % 3) as usize]);
        opt(r, &|r| {
            r.str_val(["", "héllo", "a string longer than a German inline"][(row % 3) as usize]);
        });
        r.bytes_val(&[0, 255, row as u8]);
        opt(r, &|r| {
            r.int_val(pick(-719_162, 2_932_896));
        });
        r.int_val(pick(i64::MIN as i128, i64::MAX as i128));
        opt(r, &|r| {
            r.int_val(row % 2);
        });
        opt(r, &|r| {
            r.int_val(pick(i64::MIN as i128, i64::MAX as i128));
        });
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

#[test]
fn a_file_of_other_columns_is_refused() {
    let schema = every_type();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.parquet");
    let mut w = TableWriter::create(&path, &schema).unwrap();
    w.write(&rows(&schema)).unwrap();
    w.finish().unwrap();
    let other = Arc::new(
        Schema::from_parts(
            vec![
                ColumnDef::new("k1", TypeCode::I16, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            &[0],
        )
        .unwrap(),
    );
    assert!(TableReader::open(&path, &other).is_err());
}

/// A column of another gnitz type is refused though its Parquet column is the
/// same physical type, or the same Parquet type altogether.
#[test]
fn a_column_of_another_gnitz_type_is_refused() {
    let table = |v: TypeCode| {
        Arc::new(
            Schema::from_parts(
                vec![ColumnDef::new("k", TypeCode::I64, false), ColumnDef::new("v", v, false)],
                &[0],
            )
            .unwrap(),
        )
    };
    let dir = tempfile::tempdir().unwrap();
    for (written, opened) in [(TypeCode::Date, TypeCode::I32), (TypeCode::U128, TypeCode::I128)] {
        let path = dir.path().join(format!("{written}.parquet"));
        TableWriter::create(&path, &table(written)).unwrap().finish().unwrap();
        TableReader::open(&path, &table(written)).unwrap();
        let err = TableReader::open(&path, &table(opened)).err().unwrap();
        assert!(err.0.contains("its columns are not the table's"), "{err}");
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
    assert_eq!(types[0], "SMALLINT");
    assert!(types.contains(&"HUGEINT".to_string()) && types.contains(&"UHUGEINT".to_string()));
    assert_eq!(
        types[2..].iter().map(String::as_str).collect::<Vec<_>>(),
        [
            "TINYINT UNSIGNED",
            "TINYINT",
            "SMALLINT UNSIGNED",
            "SMALLINT",
            "INT UNSIGNED",
            "INT",
            "BIGINT UNSIGNED",
            "BIGINT",
            "UHUGEINT",
            "HUGEINT",
            "UUID",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "BLOB",
            "DATE",
            "TIMESTAMP",
            "BOOLEAN",
            "DECIMAL(18, 2)",
        ]
    );
}

#[test]
fn a_key_out_of_its_columns_range_is_refused() {
    let table = |key: TypeCode| {
        Arc::new(
            Schema::from_parts(
                vec![
                    ColumnDef::new("k", key, false),
                    ColumnDef::new("v", TypeCode::I64, false),
                ],
                &[0],
            )
            .unwrap(),
        )
    };
    let narrow = table(TypeCode::I8);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.parquet");
    // A file this writer never produces: the I8 key's INT32 holds 300.
    let (root, types) = file_layout(&narrow).unwrap();
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(COLUMNS_KEY.to_string(), types)]))
        .build();
    let mut w = SerializedFileWriter::new(File::create(&path).unwrap(), Arc::new(root), Arc::new(props)).unwrap();
    let mut group = w.next_row_group().unwrap();
    for _ in 0..2 {
        let mut col = group.next_column().unwrap().unwrap();
        match col.untyped() {
            ColumnWriter::Int32ColumnWriter(c) => c.write_batch(&[300], None, None).unwrap(),
            ColumnWriter::Int64ColumnWriter(c) => c.write_batch(&[1], None, None).unwrap(),
            _ => unreachable!("an INT32 key and an INT64 payload"),
        };
        col.close().unwrap();
    }
    group.close().unwrap();
    w.close().unwrap();
    let err = TableReader::open(&path, &narrow).unwrap().next_group().unwrap_err();
    assert!(err.0.contains("out of range"), "{err}");
}
