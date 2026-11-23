use super::*;
use gnitz_wire::{ColType, ColumnDef};
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
fn rows(schema: &Arc<Schema>) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    let mut app = batch.builder();
    for row in 0..6usize {
        let pick = |lo: i128, hi: i128| [lo, hi, (-1).clamp(lo, hi)][row % 3];
        let k2 = [row as u128, u128::from(u32::MAX) - row as u128, (1 << 31) + row as u128][row % 3];
        let r = app.begin_row_natives(&[pick(-32768, 32767) as u128, k2], 1);
        for (_, _, def) in schema.payload_columns() {
            if def.is_nullable && row % 2 == 1 {
                r.put_null();
                continue;
            }
            match def.ty.tc {
                TypeCode::Bool => r.put_int((((row / 2) % 2) as i128) as u128),
                TypeCode::F32 => r.put_float(f64::from([f32::NAN, -0.0, f32::MAX][row % 3])),
                TypeCode::F64 => r.put_float([f64::from_bits(0x7ff8_dead_beef_0001), -0.0, f64::MIN_POSITIVE][row % 3]),
                TypeCode::U128 => r.put_int([0, u128::MAX, 1][row % 3]),
                TypeCode::I128 => r.put_int([i128::MIN, i128::MAX, -1][row % 3] as u128),
                TypeCode::UUID => r.put_int(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef ^ row as u128),
                TypeCode::String => r.put_string(["", "héllo", "a string longer than a German inline"][row % 3]),
                TypeCode::Blob => r.put_blob(&[0, 255, row as u8]),
                tc => {
                    let (lo, hi) = FixedInt::from_type_code(tc).expect("an integer type").range();
                    r.put_int((pick(lo, hi)) as u128)
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
}

#[test]
fn a_row_not_at_weight_one_is_refused() {
    let schema = every_type();
    let dir = tempfile::tempdir().unwrap();
    let mut w = create(&dir.path().join("t.parquet"), &schema).unwrap();
    let mut batch = rows(&schema);
    batch.rewrite(|d| d.weight_mut()[16..24].copy_from_slice(&2i64.to_le_bytes()));
    let err = write(&mut w, &batch).unwrap_err();
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
        finish(create(&path, &table(&written)).unwrap()).unwrap();
        assert_eq!(open(&path, &table(&written)).unwrap().count(), 0);
        let err = open(&path, &table(&opened)).err().unwrap();
        assert!(err.0.contains("its columns are not the table's"), "{i}: {err}");
    }
}

#[test]
fn the_file_names_each_columns_gnitz_type() {
    let schema = every_type();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.parquet");
    finish(create(&path, &schema).unwrap()).unwrap();
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
/// refused rather than wrapped: an I8 key holding 300, and a DECIMAL's nine
/// bytes holding 2^63, one past `i64::MAX`.
#[test]
fn a_cell_its_column_cannot_hold_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    for (i, (key, v, refusal)) in [
        (TypeCode::I8, ColType::of(TypeCode::I64), "out of range"),
        (TypeCode::I64, ColType::decimal(2), "out of range"),
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
        let mut w = create(&path, &schema).unwrap();
        let mut group = w.next_row_group().unwrap();
        while let Some(mut col) = group.next_column().unwrap() {
            match col.untyped() {
                ColumnWriter::Int32ColumnWriter(c) => c.write_batch(&[300], None, None),
                ColumnWriter::Int64ColumnWriter(c) => c.write_batch(&[1], None, None),
                ColumnWriter::FixedLenByteArrayColumnWriter(c) => {
                    let cell = FixedLenByteArray::from(vec![0x00, 0x80, 0, 0, 0, 0, 0, 0, 0]);
                    c.write_batch(&[cell], None, None)
                }
                _ => unreachable!("no other column"),
            }
            .unwrap();
            col.close().unwrap();
        }
        group.close().unwrap();
        finish(w).unwrap();
        let err = open(&path, &schema).unwrap().next().unwrap().unwrap_err();
        assert!(err.0.contains(refusal), "{i}: {err}");
    }
}
