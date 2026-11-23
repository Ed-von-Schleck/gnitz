//! A table's rows as a Parquet file: one Parquet column per visible column, in
//! schema order, each in the Parquet type a reader outside gnitz reads it as.
//!
//! | gnitz | Parquet |
//! |---|---|
//! | TINYINT … INT, signed or not | INT32, `INTEGER(bits, signed)` |
//! | BIGINT | INT64, `INTEGER(64, true)` |
//! | BIGINT UNSIGNED | INT64, `INTEGER(64, false)` |
//! | HUGEINT, UHUGEINT | FIXED_LEN_BYTE_ARRAY(16) |
//! | UUID | FIXED_LEN_BYTE_ARRAY(16), `UUID` |
//! | FLOAT, DOUBLE | FLOAT, DOUBLE, every bit kept |
//! | TEXT | BYTE_ARRAY, `STRING` |
//! | BLOB | BYTE_ARRAY |
//! | DATE | INT32, `DATE` |
//! | TIMESTAMP | INT64, `TIMESTAMP(MICROS, local)` |
//! | DECIMAL(p, s) | FIXED_LEN_BYTE_ARRAY(9), `DECIMAL(19, s)` |
//! | BOOLEAN | BOOLEAN |
//!
//! A 128-bit integer has no Parquet type that holds every value — a
//! `DECIMAL(38, 0)` stops short of `i128`'s range — so each is its 16 bytes,
//! and a DECIMAL, which holds any `i64`, is the 9 bytes that hold 19 digits;
//! both are big-endian two's complement. The file's `gnitz.columns` metadata
//! names each column's gnitz type, which is what tells HUGEINT from UHUGEINT. A
//! nullable column is OPTIONAL, every other one REQUIRED.

use std::borrow::Cow;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::ColumnLocator;
use gnitz_wire::{FixedInt, TypeCode, as_le_bytes, push_zero_cell};
use gnitz_zset::repr::{Batch, MemBatch};
use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit, Type as Physical, ZstdLevel};
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{AsBytes, ByteArray, DataType, FixedLenByteArray, Int32Type};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{ColumnPath, Type};

use crate::DumpError;

/// The file metadata key naming each column's gnitz type, in column order. Its
/// values are `gnitz_wire::ColType`'s `Display` spelling, so a change to that
/// spelling is a change to the format.
const COLUMNS_KEY: &str = "gnitz.columns";

/// The zstd level a dump's pages are compressed at.
const ZSTD_LEVEL: i32 = 3;

/// The digits of every `i64`.
const DECIMAL_PRECISION: i32 = 19;

/// The bytes of a DECIMAL cell in the file: the fewest that hold
/// `DECIMAL_PRECISION` digits.
const DECIMAL_BYTES: usize = 9;

/// The Parquet schema of a file of `schema`'s visible columns, and its
/// `COLUMNS_KEY` value.
fn file_layout(schema: &Schema) -> Result<(Type, String), DumpError> {
    let (mut fields, mut types) = (Vec::new(), Vec::new());
    for (_, def) in schema.visible_columns() {
        let repetition = if def.is_nullable {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        };
        let column = |physical, logical| {
            Type::primitive_type_builder(&def.name, physical)
                .with_repetition(repetition)
                .with_logical_type(logical)
        };
        let scale = i32::from(def.ty.scale);
        let column = match def.ty.tc {
            tc @ (TypeCode::U8
            | TypeCode::I8
            | TypeCode::U16
            | TypeCode::I16
            | TypeCode::U32
            | TypeCode::I32
            | TypeCode::U64
            | TypeCode::I64) => {
                let fi = FixedInt::exact(tc).expect("an integer type");
                let physical = if fi.width() == 8 {
                    Physical::INT64
                } else {
                    Physical::INT32
                };
                column(
                    physical,
                    Some(LogicalType::integer(8 * fi.width() as i8, fi.is_signed())),
                )
            }
            TypeCode::U128 | TypeCode::I128 => column(Physical::FIXED_LEN_BYTE_ARRAY, None).with_length(16),
            TypeCode::UUID => column(Physical::FIXED_LEN_BYTE_ARRAY, Some(LogicalType::Uuid)).with_length(16),
            TypeCode::F32 => column(Physical::FLOAT, None),
            TypeCode::F64 => column(Physical::DOUBLE, None),
            TypeCode::String => column(Physical::BYTE_ARRAY, Some(LogicalType::String)),
            TypeCode::Blob => column(Physical::BYTE_ARRAY, None),
            TypeCode::Date => column(Physical::INT32, Some(LogicalType::Date)),
            TypeCode::Timestamp => column(Physical::INT64, Some(LogicalType::timestamp(false, TimeUnit::MICROS))),
            TypeCode::Decimal => column(
                Physical::FIXED_LEN_BYTE_ARRAY,
                Some(LogicalType::decimal(scale, DECIMAL_PRECISION)),
            )
            .with_length(DECIMAL_BYTES as i32)
            .with_precision(DECIMAL_PRECISION)
            .with_scale(scale),
            TypeCode::Bool => column(Physical::BOOLEAN, None),
        };
        fields.push(Arc::new(column.build()?));
        types.push(def.ty.to_string());
    }
    Ok((
        Type::group_type_builder("gnitz").with_fields(fields).build()?,
        serde_json::Value::from(types).to_string(),
    ))
}

/// A new file at `path` for rows of `schema`; [`write`] adds each row group,
/// and the file is whole once [`finish`] returns.
pub(crate) fn create(path: &Path, schema: &Schema) -> Result<SerializedFileWriter<File>, DumpError> {
    let (root, types) = file_layout(schema)?;
    let mut props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL)?))
        .set_key_value_metadata(Some(vec![KeyValue::new(COLUMNS_KEY.to_string(), types)]));
    // A lone key column's values are distinct within the file, so a
    // dictionary would hold every one and add an index per row.
    if let Some(key) = schema.layout().lone_pk_col() {
        let name = &*schema.columns()[key].name;
        props = props.set_column_dictionary_enabled(ColumnPath::from(name), false);
    }
    let file = File::create(path)?;
    Ok(SerializedFileWriter::new(
        file,
        Arc::new(root),
        Arc::new(props.build()),
    )?)
}

/// `batch`'s rows as one row group of `file`, which [`create`] made for its
/// schema; every row must stand at weight 1.
pub(crate) fn write(file: &mut SerializedFileWriter<File>, batch: &ZSetBatch) -> Result<(), DumpError> {
    let rows = batch.view();
    if let Some(w) = rows.weights().find(|&w| w != 1) {
        return Err(DumpError::from(format!("a base-table row at weight {w}")));
    }
    if batch.is_empty() {
        return Ok(());
    }
    let schema = batch.schema();
    let mut group = file.next_row_group()?;
    for (ci, def) in schema.visible_columns() {
        let mut writer = group.next_column()?.expect("one writer per column");
        write_column(writer.untyped(), &rows, schema.layout().locate(ci), def.is_nullable)?;
        writer.close()?;
    }
    group.close()?;
    Ok(())
}

/// Close `file`, durable once this returns; the rows it holds.
pub(crate) fn finish(mut file: SerializedFileWriter<File>) -> Result<u64, DumpError> {
    let meta = file.finish()?;
    file.inner().sync_all()?;
    Ok(meta.file_metadata().num_rows() as u64)
}

/// Column `loc` of `batch` into `writer`, of the type `file_layout` gives it.
// Out of line: inlined into `write`, it costs every row more.
#[inline(never)]
fn write_column(
    writer: &mut ColumnWriter<'_>,
    batch: &MemBatch<'_>,
    loc: ColumnLocator,
    nullable: bool,
) -> Result<(), DumpError> {
    let bit = loc.null_bit();
    let levels: Option<Vec<i16>> = nullable.then(|| batch.null_words().map(|w| i16::from(w & bit == 0)).collect());
    let levels = levels.as_deref();
    // Native little-endian cells; a key column's are decoded out of the key region.
    let cells: Cow<'_, [u8]> = match loc {
        ColumnLocator::Payload { slot, .. } => Cow::Borrowed(batch.col_data(slot as usize)),
        ColumnLocator::Pk { byte_off, size, type_code } => {
            let mut cells = vec![0; batch.len() * size as usize];
            gnitz_wire::decode_pk_cells(
                batch.pk(),
                batch.schema().pk_stride(),
                byte_off as usize,
                type_code,
                &mut cells,
            );
            Cow::Owned(cells)
        }
    };
    let present = cells
        .chunks_exact(loc.size())
        .zip(batch.null_words())
        .filter(|&(_, w)| w & bit == 0)
        .map(|(c, _)| c);
    let tc = loc.type_code();
    match writer {
        ColumnWriter::Int32ColumnWriter(w) => {
            let fi = FixedInt::from_type_code(tc).expect("an integer");
            let values: Vec<i32> = present.map(|c| fi.decode_le_i64(c) as i32).collect();
            w.write_batch(&values, levels, None)
        }
        ColumnWriter::Int64ColumnWriter(w) => {
            let values: Vec<i64> = present.map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
            w.write_batch(&values, levels, None)
        }
        ColumnWriter::BoolColumnWriter(w) => {
            let values: Vec<bool> = present.map(|c| c[0] != 0).collect();
            w.write_batch(&values, levels, None)
        }
        ColumnWriter::FloatColumnWriter(w) => {
            let values: Vec<f32> = present.map(|c| f32::from_le_bytes(c.try_into().unwrap())).collect();
            w.write_batch(&values, levels, None)
        }
        ColumnWriter::DoubleColumnWriter(w) => {
            let values: Vec<f64> = present.map(|c| f64::from_le_bytes(c.try_into().unwrap())).collect();
            w.write_batch(&values, levels, None)
        }
        // A DECIMAL's i64, big-endian and sign-extended to `DECIMAL_BYTES`.
        ColumnWriter::FixedLenByteArrayColumnWriter(w) if tc == TypeCode::Decimal => {
            let values = byte_values::<FixedLenByteArray>(present, |c, all| {
                let v = i64::from_le_bytes(c.try_into().unwrap());
                all.push((v >> 63) as u8);
                all.extend_from_slice(&v.to_be_bytes());
            });
            w.write_batch(&values, levels, None)
        }
        // A 128-bit cell, big-endian.
        ColumnWriter::FixedLenByteArrayColumnWriter(w) => {
            let values = byte_values::<FixedLenByteArray>(present, |c, all| {
                all.extend_from_slice(&u128::from_le_bytes(c.try_into().unwrap()).to_be_bytes());
            });
            w.write_batch(&values, levels, None)
        }
        ColumnWriter::ByteArrayColumnWriter(w) => {
            let values = byte_values::<ByteArray>(present, |c, all| {
                all.extend_from_slice(gnitz_wire::german_string_content(
                    c.first_chunk().unwrap(),
                    batch.blob(),
                ))
            });
            w.write_batch(&values, levels, None)
        }
        ColumnWriter::Int96ColumnWriter(_) => unreachable!("file_layout makes no INT96 column"),
    }?;
    Ok(())
}

/// Each of `cells`, which `cell` appends to one buffer, as values over that
/// buffer.
fn byte_values<'c, V: From<ByteArray>>(
    cells: impl Iterator<Item = &'c [u8]>,
    mut cell: impl FnMut(&[u8], &mut Vec<u8>),
) -> Vec<V> {
    let mut all = Vec::new();
    let mut ends = Vec::with_capacity(cells.size_hint().1.unwrap_or(0));
    for c in cells {
        cell(c, &mut all);
        ends.push(all.len());
    }
    let all = Bytes::from(all);
    let mut start = 0;
    ends.into_iter()
        .map(|end| {
            let v = V::from(ByteArray::from(all.slice(start..end)));
            start = end;
            v
        })
        .collect()
}

/// The row groups of the file at `path`, each as rows of `schema` at weight 1;
/// refused unless the file's columns are `schema`'s by name, type and
/// nullability.
pub(crate) fn open(
    path: &Path,
    schema: &Arc<Schema>,
) -> Result<impl Iterator<Item = Result<ZSetBatch, DumpError>>, DumpError> {
    // A restored table has no hidden column, so file column i is schema column i.
    if schema.columns().iter().any(|c| c.is_hidden) {
        return Err(DumpError::from("restored into a table with hidden columns"));
    }
    let file = SerializedFileReader::new(File::open(path)?)?;
    let (root, types) = file_layout(schema)?;
    let meta = file.metadata().file_metadata();
    let recorded = meta
        .key_value_metadata()
        .and_then(|kv| kv.iter().find(|kv| kv.key == COLUMNS_KEY))
        .and_then(|kv| kv.value.as_deref());
    if *meta.schema() != root || recorded != Some(types.as_str()) {
        return Err(DumpError::from("its columns are not the table's"));
    }
    let schema = Arc::clone(schema);
    Ok((0..file.num_row_groups()).map(move |g| read_group(&file, g, &schema)))
}

/// Row group `g` of `file`, whose columns are `schema`'s, as rows of `schema`
/// at weight 1.
fn read_group(file: &SerializedFileReader<File>, g: usize, schema: &Arc<Schema>) -> Result<ZSetBatch, DumpError> {
    let group = file.get_row_group(g)?;
    let n = usize::try_from(group.metadata().num_rows()).map_err(|_| DumpError::from("a negative row count"))?;
    let layout = schema.layout();
    let (mut nulls, mut heap) = (vec![0u64; n], Vec::new());
    // Column `ci`'s cells as a batch region holds them, native little-endian.
    let mut read = |ci: usize| -> Result<(ColumnLocator, Vec<u8>), DumpError> {
        let (def, loc) = (&schema.columns()[ci], layout.locate(ci));
        let null_words = def.is_nullable.then_some((&mut nulls[..], loc.null_bit()));
        let mut cells = Vec::new();
        read_column(
            group.get_column_reader(ci)?,
            n,
            loc.type_code(),
            &mut cells,
            null_words,
            &mut heap,
        )
        .map_err(|e| DumpError(format!("column {}: {e}", def.name)))?;
        Ok((loc, cells))
    };
    let keys = schema
        .pk_cols()
        .iter()
        .map(|&ci| read(ci as usize))
        .collect::<Result<Vec<_>, _>>()?;
    let payload = schema
        .payload_columns()
        .map(|(_, ci, _)| read(ci))
        .collect::<Result<Vec<_>, _>>()?;
    // The file's schema is the table's (`open`), and a null bit lands only on a
    // nullable column, so the rows need no check a foreign block would.
    let batch = Batch::write_new_carrying(layout, n, 0, &[(&heap, 0, 0)], |w| {
        let (pk, weight, null_bmp) = w.fixed_mut();
        for (loc, cells) in &keys {
            let ColumnLocator::Pk { byte_off, type_code, .. } = *loc else {
                unreachable!("a key column");
            };
            gnitz_wire::encode_pk_cells(cells, type_code, pk, layout.pk_stride(), byte_off as usize);
        }
        weight.as_chunks_mut::<8>().0.fill(1i64.to_le_bytes());
        null_bmp.copy_from_slice(as_le_bytes(&nulls));
        for (pi, (_, cells)) in payload.iter().enumerate() {
            w.col_mut(pi).copy_from_slice(cells);
        }
    });
    Ok(ZSetBatch::of(schema, batch))
}

/// The `n` cells of a `tc` column appended to `cells` as a batch region holds
/// them — native little-endian, a zero cell under NULL, a string's bytes
/// spilled into `blob` — each NULL row's word in `nulls` given the column's
/// bit. `reader` is of the type `file_layout` gives a `tc` column, which `open`
/// checked the file against.
fn read_column(
    reader: ColumnReader,
    n: usize,
    tc: TypeCode,
    cells: &mut Vec<u8>,
    nulls: Option<(&mut [u64], u64)>,
    blob: &mut Vec<u8>,
) -> Result<(), String> {
    match reader {
        ColumnReader::Int32ColumnReader(r) => {
            let fi = FixedInt::from_type_code(tc).expect("an integer");
            match fi.width() {
                1 => read_int32::<1>(r, n, tc, cells, nulls, fi),
                2 => read_int32::<2>(r, n, tc, cells, nulls, fi),
                _ => read_int32::<4>(r, n, tc, cells, nulls, fi),
            }
        }
        ColumnReader::Int64ColumnReader(r) => read_cells(r, n, tc, cells, nulls, native),
        ColumnReader::BoolColumnReader(r) => read_cells(r, n, tc, cells, nulls, native),
        ColumnReader::FloatColumnReader(r) => read_cells(r, n, tc, cells, nulls, native),
        ColumnReader::DoubleColumnReader(r) => read_cells(r, n, tc, cells, nulls, native),
        ColumnReader::FixedLenByteArrayColumnReader(r) if tc == TypeCode::Decimal => {
            read_cells(r, n, tc, cells, nulls, |v, out| {
                let [sign, be @ ..] = sized::<DECIMAL_BYTES>(v.data(), tc)?;
                let x = i64::from_be_bytes(*be);
                // Nine bytes of two's complement hold an `i64` exactly when the top
                // one is the sign of the rest.
                if *sign != (x >> 63) as u8 {
                    return Err(format!("a {tc} cell out of range of a 64-bit integer"));
                }
                out.extend_from_slice(&x.to_le_bytes());
                Ok(())
            })
        }
        ColumnReader::FixedLenByteArrayColumnReader(r) => read_cells(r, n, tc, cells, nulls, |v, out| {
            let be: &[u8; 16] = sized(v.data(), tc)?;
            out.extend_from_slice(&u128::from_be_bytes(*be).to_le_bytes());
            Ok(())
        }),
        // A TEXT cell's UTF-8 is the push's to check.
        ColumnReader::ByteArrayColumnReader(r) => read_cells(r, n, tc, cells, nulls, |v, out| {
            out.extend_from_slice(&gnitz_wire::encode_german_string(v.data(), blob));
            Ok(())
        }),
        ColumnReader::Int96ColumnReader(_) => unreachable!("file_layout makes no INT96 column"),
    }
}

/// `v`'s native bytes, which on a little-endian host are its cell.
fn native<V: AsBytes>(v: V, out: &mut Vec<u8>) -> Result<(), String> {
    out.extend_from_slice(v.as_bytes());
    Ok(())
}

/// A fixed-length cell of `tc`, refused unless it is `N` bytes long.
fn sized<const N: usize>(d: &[u8], tc: TypeCode) -> Result<&[u8; N], String> {
    d.try_into().map_err(|_| format!("a {tc} cell of {} bytes", d.len()))
}

/// An INT32 column of a type `W` bytes wide, refused where a value is out of its range.
fn read_int32<const W: usize>(
    reader: ColumnReaderImpl<Int32Type>,
    n: usize,
    tc: TypeCode,
    cells: &mut Vec<u8>,
    nulls: Option<(&mut [u64], u64)>,
    fi: FixedInt,
) -> Result<(), String> {
    read_cells(reader, n, tc, cells, nulls, |v: i32, out| {
        // A value the column cannot hold changes when cut to its width.
        if fi.unpack(u128::from(v as u32)) as i32 != v {
            return Err(format!("{v} is out of range for {tc}"));
        }
        out.extend_from_slice(&v.to_le_bytes()[..W]);
        Ok(())
    })
}

/// The `n` values of a column chunk, appended to `cells` as `decode` turns each into a cell; a zero
/// cell of `tc` for each NULL row, whose word in `nulls` gets the column's bit. `nulls` is
/// `Some` exactly when the column may be NULL.
fn read_cells<T: DataType>(
    mut r: ColumnReaderImpl<T>,
    n: usize,
    tc: TypeCode,
    cells: &mut Vec<u8>,
    mut nulls: Option<(&mut [u64], u64)>,
    mut decode: impl FnMut(T::T, &mut Vec<u8>) -> Result<(), String>,
) -> Result<(), String> {
    let mut levels = nulls.is_some().then(|| Vec::with_capacity(n));
    let mut values = Vec::with_capacity(n);
    let mut got = 0;
    while got < n {
        let (records, _, _) = r
            .read_records(n - got, levels.as_mut(), None, &mut values)
            .map_err(|e| e.to_string())?;
        if records == 0 {
            return Err("a column chunk shorter than its row group".to_string());
        }
        got += records;
    }
    cells.reserve(n * tc.wire_stride());
    let mut values = values.into_iter();
    for row in 0..n {
        if let (Some(levels), Some((words, bit))) = (&levels, &mut nulls)
            && levels[row] == 0
        {
            push_zero_cell(cells, tc);
            words[row] |= *bit;
            continue;
        }
        decode(values.next().ok_or("fewer values than rows")?, cells)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/columns.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/columns.rs"]
mod bench;
