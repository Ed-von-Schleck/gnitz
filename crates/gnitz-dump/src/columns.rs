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

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use gnitz_core::{push_zero_cell, Schema, ZSetBatch};
use gnitz_expr::ColumnLocator;
use gnitz_wire::{ColumnDef, FixedInt, TypeCode};
use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit, Type as Physical, ZstdLevel};
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{ByteArray, DataType, FixedLenByteArray};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{ColumnPath, Type};

use crate::DumpError;

/// The file metadata key naming each column's gnitz type, in column order. Its
/// values are `gnitz_sql::sql_type_name`'s spelling, so a change to that
/// spelling is a change to the format.
pub(crate) const COLUMNS_KEY: &str = "gnitz.columns";

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
    let visible = || schema.visible_columns().map(|(_, def)| def);
    let fields = visible()
        .map(|d| parquet_column(d).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    let types: Vec<String> = visible().map(|d| gnitz_sql::sql_type_name(d.ty)).collect();
    Ok((
        Type::group_type_builder("gnitz").with_fields(fields).build()?,
        serde_json::Value::from(types).to_string(),
    ))
}

/// The Parquet column `def` is written as.
fn parquet_column(def: &ColumnDef) -> Result<Type, DumpError> {
    let tc = def.ty.tc;
    let int = |bits: i8, signed: bool| (Physical::INT32, Some(LogicalType::integer(bits, signed)));
    let (physical, logical) = match tc {
        TypeCode::U8 => int(8, false),
        TypeCode::I8 => int(8, true),
        TypeCode::U16 => int(16, false),
        TypeCode::I16 => int(16, true),
        TypeCode::U32 => int(32, false),
        TypeCode::I32 => int(32, true),
        TypeCode::U64 => (Physical::INT64, Some(LogicalType::integer(64, false))),
        TypeCode::I64 => (Physical::INT64, Some(LogicalType::integer(64, true))),
        TypeCode::U128 | TypeCode::I128 => (Physical::FIXED_LEN_BYTE_ARRAY, None),
        TypeCode::UUID => (Physical::FIXED_LEN_BYTE_ARRAY, Some(LogicalType::Uuid)),
        TypeCode::F32 => (Physical::FLOAT, None),
        TypeCode::F64 => (Physical::DOUBLE, None),
        TypeCode::String => (Physical::BYTE_ARRAY, Some(LogicalType::String)),
        TypeCode::Blob => (Physical::BYTE_ARRAY, None),
        TypeCode::Date => (Physical::INT32, Some(LogicalType::Date)),
        TypeCode::Timestamp => (Physical::INT64, Some(LogicalType::timestamp(false, TimeUnit::MICROS))),
        TypeCode::Decimal => (
            Physical::FIXED_LEN_BYTE_ARRAY,
            Some(LogicalType::decimal(i32::from(def.ty.scale), DECIMAL_PRECISION)),
        ),
        TypeCode::Bool => (Physical::BOOLEAN, None),
    };
    let mut b = Type::primitive_type_builder(&def.name, physical)
        .with_repetition(if def.is_nullable {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        })
        .with_logical_type(logical);
    if tc == TypeCode::Decimal {
        b = b
            .with_length(DECIMAL_BYTES as i32)
            .with_precision(DECIMAL_PRECISION)
            .with_scale(i32::from(def.ty.scale));
    } else if physical == Physical::FIXED_LEN_BYTE_ARRAY {
        b = b.with_length(16);
    }
    Ok(b.build()?)
}

/// A table's Parquet file, written a batch — one row group — at a time.
pub(crate) struct TableWriter {
    writer: SerializedFileWriter<File>,
    cols: Vec<(ColumnLocator, ColumnDef)>,
}

impl TableWriter {
    pub(crate) fn create(path: &Path, schema: &Schema) -> Result<TableWriter, DumpError> {
        let (root, types) = file_layout(schema)?;
        let mut props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL)?))
            .set_key_value_metadata(Some(vec![KeyValue::new(COLUMNS_KEY.to_string(), types)]));
        // A lone key column's values are distinct within the file, so a
        // dictionary would hold every one and add an index per row.
        if let &[key] = schema.pk_cols() {
            let name = schema.columns()[key as usize].name.as_str();
            props = props.set_column_dictionary_enabled(ColumnPath::from(name), false);
        }
        let file = File::create(path)?;
        Ok(TableWriter {
            writer: SerializedFileWriter::new(file, Arc::new(root), Arc::new(props.build()))?,
            cols: schema
                .visible_columns()
                .map(|(ci, def)| (schema.layout().locate(ci), def.clone()))
                .collect(),
        })
    }

    /// `batch`'s rows as one row group; every row must stand at weight 1.
    pub(crate) fn write(&mut self, batch: &ZSetBatch) -> Result<(), DumpError> {
        if let Some(&w) = batch.weights.iter().find(|&&w| w != 1) {
            return Err(DumpError::from(format!("a base-table row at weight {w}")));
        }
        if batch.is_empty() {
            return Ok(());
        }
        let mut group = self.writer.next_row_group()?;
        let mut cols = self.cols.iter();
        while let Some(mut writer) = group.next_column()? {
            let (loc, def) = cols.next().expect("one writer per column");
            write_column(writer.untyped(), batch, *loc, def)?;
            writer.close()?;
        }
        group.close()?;
        Ok(())
    }

    /// Close the file; the rows it holds.
    pub(crate) fn finish(self) -> Result<u64, DumpError> {
        let rows = self.writer.close()?.file_metadata().num_rows();
        u64::try_from(rows).map_err(|_| DumpError::from("a negative row count"))
    }
}

/// Column `loc`, defined by `def`, of `batch` into `writer`.
fn write_column(
    writer: &mut ColumnWriter<'_>,
    batch: &ZSetBatch,
    loc: ColumnLocator,
    def: &ColumnDef,
) -> Result<(), DumpError> {
    let present: Vec<usize> = (0..batch.len()).filter(|&i| !loc.is_null(batch, i)).collect();
    let levels: Option<Vec<i16>> = def.is_nullable.then(|| {
        let mut levels = vec![0; batch.len()];
        for &i in &present {
            levels[i] = 1;
        }
        levels
    });
    let levels = levels.as_deref();
    // An integer cell, its bits kept: an unsigned value past the signed range
    // reads back as the same bits.
    let fi = FixedInt::from_type_code(def.ty.tc);
    let int = |i: usize| loc.decode_i64(batch, i, fi.expect("an INT32, INT64 or BOOLEAN column holds integers"));
    // A float cell, which no key column holds: its stored bytes.
    let bytes = |i: usize| loc.bytes(batch, i);
    match writer {
        ColumnWriter::Int32ColumnWriter(w) => {
            let values: Vec<i32> = present.iter().map(|&i| int(i) as i32).collect();
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::Int64ColumnWriter(w) => {
            let values: Vec<i64> = present.iter().map(|&i| int(i)).collect();
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::BoolColumnWriter(w) => {
            let values: Vec<bool> = present.iter().map(|&i| int(i) != 0).collect();
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::FloatColumnWriter(w) => {
            let values: Vec<f32> = present
                .iter()
                .map(|&i| f32::from_le_bytes(bytes(i).try_into().expect("a FLOAT cell is 4 bytes")))
                .collect();
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::DoubleColumnWriter(w) => {
            let values: Vec<f64> = present
                .iter()
                .map(|&i| f64::from_le_bytes(bytes(i).try_into().expect("a DOUBLE cell is 8 bytes")))
                .collect();
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::FixedLenByteArrayColumnWriter(w) => {
            let values = if def.ty.tc == TypeCode::Decimal {
                byte_values(&present, |i, all| {
                    let v = i128::from(loc.decode_i64(batch, i, FixedInt::I64));
                    all.extend_from_slice(&v.to_be_bytes()[16 - DECIMAL_BYTES..]);
                })
            } else {
                byte_values(&present, |i, all| {
                    let mut scratch = [0u8; 16];
                    let le: [u8; 16] = loc
                        .native_le_bytes(batch, i, &mut scratch)
                        .try_into()
                        .expect("a 128-bit cell is 16 bytes");
                    all.extend_from_slice(&u128::from_le_bytes(le).to_be_bytes());
                })
            };
            let values: Vec<FixedLenByteArray> = values.into_iter().map(FixedLenByteArray::from).collect();
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::ByteArrayColumnWriter(w) => {
            let values = byte_values(&present, |i, all| all.extend_from_slice(&loc.content(batch, i)));
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::Int96ColumnWriter(_) => unreachable!("no gnitz type is written as INT96"),
    }
    Ok(())
}

/// The cell of each row of `present`, which `cell` appends to one buffer, as
/// values over that buffer.
fn byte_values(present: &[usize], mut cell: impl FnMut(usize, &mut Vec<u8>)) -> Vec<ByteArray> {
    let mut all = Vec::new();
    let mut ends = Vec::with_capacity(present.len());
    for &i in present {
        cell(i, &mut all);
        ends.push(all.len());
    }
    let all = Bytes::from(all);
    let mut start = 0;
    ends.into_iter()
        .map(|end| {
            let v = ByteArray::from(all.slice(start..end));
            start = end;
            v
        })
        .collect()
}

/// A table's Parquet file read back a row group at a time, each decoded into
/// the schema of the table it was dumped from as restored.
pub(crate) struct TableReader {
    reader: SerializedFileReader<File>,
    schema: Arc<Schema>,
    path: String,
    next: usize,
}

impl TableReader {
    /// Open `path`, whose columns must be `schema`'s by name, type and
    /// nullability.
    pub(crate) fn open(path: &Path, schema: &Arc<Schema>) -> Result<TableReader, DumpError> {
        let reader = SerializedFileReader::new(File::open(path)?)?;
        let shown = path.display().to_string();
        // A file's columns are the dumped table's visible ones, and a restored
        // table has no other, so a file column stands at its schema index.
        if schema.visible_columns().count() != schema.columns().len() {
            return Err(DumpError::from(format!(
                "{shown}: restored into a table with hidden columns"
            )));
        }
        let (root, types) = file_layout(schema)?;
        let meta = reader.metadata().file_metadata();
        let recorded = meta
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|kv| kv.key == COLUMNS_KEY))
            .and_then(|kv| kv.value.as_deref());
        if *meta.schema() != root || recorded != Some(types.as_str()) {
            return Err(DumpError::from(format!("{shown}: its columns are not the table's")));
        }
        Ok(TableReader {
            reader,
            schema: Arc::clone(schema),
            path: shown,
            next: 0,
        })
    }

    /// The next row group's rows, each at weight 1; `None` past the last.
    pub(crate) fn next_group(&mut self) -> Result<Option<ZSetBatch>, DumpError> {
        if self.next == self.reader.num_row_groups() {
            return Ok(None);
        }
        let group = self.reader.get_row_group(self.next)?;
        self.next += 1;
        let schema = &self.schema;
        let layout = schema.layout();
        let n = usize::try_from(group.metadata().num_rows()).map_err(|_| DumpError::from("a negative row count"))?;
        let mut batch = ZSetBatch::new(schema);
        batch.weights = vec![1; n];
        batch.nulls = vec![0; n];
        let pk = schema.pk_cols();
        let mut keys: Vec<Vec<u8>> = vec![Vec::new(); pk.len()];
        for (ci, def) in schema.columns().iter().enumerate() {
            let mut cells = Vec::with_capacity(n * def.ty.tc.wire_stride());
            let nulls = read_column(group.get_column_reader(ci)?, n, def, &mut cells, &mut batch.blob)
                .map_err(|e| DumpError(format!("{}: column {}: {e}", self.path, def.name)))?;
            match layout.payload_slot(ci) {
                Some(pi) => {
                    for row in nulls {
                        gnitz_wire::null_word_set(&mut batch.nulls[row], pi, true);
                    }
                    batch.payload[pi].bytes = cells;
                }
                None => keys[pk.iter().position(|&k| k as usize == ci).expect("a key column")] = cells,
            }
        }
        // Each key cell is its native little-endian bytes; zero-extended,
        // `push_natives` truncates it back to the column's width.
        let widths: Vec<usize> = pk
            .iter()
            .map(|&ci| schema.columns()[ci as usize].ty.tc.wire_stride())
            .collect();
        let mut natives = vec![0u128; pk.len()];
        batch.pks.reserve(n);
        for row in 0..n {
            for ((v, cells), &w) in natives.iter_mut().zip(&keys).zip(&widths) {
                let mut le = [0u8; 16];
                le[..w].copy_from_slice(&cells[row * w..][..w]);
                *v = u128::from_le_bytes(le);
            }
            batch.pks.push_natives(&natives);
        }
        Ok(Some(batch))
    }
}

/// The `n` cells of column `def` appended to `cells` as a batch region holds
/// them — native little-endian, a zero cell under NULL, a string's bytes
/// spilled into `blob` — and the rows that are NULL.
fn read_column(
    reader: ColumnReader,
    n: usize,
    def: &ColumnDef,
    cells: &mut Vec<u8>,
    blob: &mut Vec<u8>,
) -> Result<Vec<usize>, String> {
    let tc = def.ty.tc;
    let mut levels = def.is_nullable.then(|| Vec::with_capacity(n));
    let fixed_int = || FixedInt::from_type_code(tc).ok_or_else(|| format!("an integer for a {tc} column"));
    // An integer as the column holds it, refused outside its range.
    let int = |fi: FixedInt, v: i128, out: &mut Vec<u8>| {
        let (lo, hi) = fi.range();
        if !(lo..=hi).contains(&v) {
            return Err(format!("{v} is out of range for {tc}"));
        }
        out.extend_from_slice(&fi.pack(v).to_le_bytes()[..fi.width()]);
        Ok(())
    };
    match reader {
        ColumnReader::Int32ColumnReader(r) => {
            let fi = fixed_int()?;
            let values = read_all(r, n, levels.as_mut())?;
            fill(values, levels.as_deref(), n, tc, cells, |v, out| {
                int(
                    fi,
                    if fi.is_signed() {
                        i128::from(v)
                    } else {
                        i128::from(v as u32)
                    },
                    out,
                )
            })
        }
        ColumnReader::Int64ColumnReader(r) => {
            let fi = fixed_int()?;
            let values = read_all(r, n, levels.as_mut())?;
            fill(values, levels.as_deref(), n, tc, cells, |v, out| {
                int(
                    fi,
                    if fi.is_signed() {
                        i128::from(v)
                    } else {
                        i128::from(v as u64)
                    },
                    out,
                )
            })
        }
        ColumnReader::BoolColumnReader(r) => {
            let values = read_all(r, n, levels.as_mut())?;
            fill(values, levels.as_deref(), n, tc, cells, |v, out| {
                out.push(u8::from(v));
                Ok(())
            })
        }
        ColumnReader::FloatColumnReader(r) => {
            let values = read_all(r, n, levels.as_mut())?;
            fill(values, levels.as_deref(), n, tc, cells, |v, out| {
                out.extend_from_slice(&v.to_le_bytes());
                Ok(())
            })
        }
        ColumnReader::DoubleColumnReader(r) => {
            let values = read_all(r, n, levels.as_mut())?;
            fill(values, levels.as_deref(), n, tc, cells, |v, out| {
                out.extend_from_slice(&v.to_le_bytes());
                Ok(())
            })
        }
        ColumnReader::FixedLenByteArrayColumnReader(r) => {
            let values = read_all(r, n, levels.as_mut())?;
            let sized = |v: &FixedLenByteArray, len: usize| match v.data().len() == len {
                true => Ok(()),
                false => Err(format!("a {tc} cell of {} bytes", v.data().len())),
            };
            if tc == TypeCode::Decimal {
                fill(values, levels.as_deref(), n, tc, cells, |v, out| {
                    sized(&v, DECIMAL_BYTES)?;
                    let sign = if v.data()[0] & 0x80 != 0 { 0xff } else { 0 };
                    let mut be = [sign; 16];
                    be[16 - DECIMAL_BYTES..].copy_from_slice(v.data());
                    let wide = i128::from_be_bytes(be);
                    let v = i64::try_from(wide).map_err(|_| format!("{wide} is out of range for {tc}"))?;
                    out.extend_from_slice(&v.to_le_bytes());
                    Ok(())
                })
            } else {
                fill(values, levels.as_deref(), n, tc, cells, |v, out| {
                    sized(&v, 16)?;
                    out.extend(v.data().iter().rev());
                    Ok(())
                })
            }
        }
        ColumnReader::ByteArrayColumnReader(r) => {
            let values = read_all(r, n, levels.as_mut())?;
            fill(values, levels.as_deref(), n, tc, cells, |v, out| {
                if tc == TypeCode::String && std::str::from_utf8(v.data()).is_err() {
                    return Err("a TEXT cell that is not UTF-8".to_string());
                }
                out.extend_from_slice(&gnitz_wire::encode_german_string(v.data(), blob));
                Ok(())
            })
        }
        ColumnReader::Int96ColumnReader(_) => Err("an INT96 column".to_string()),
    }
}

/// The values of a column chunk of `n` rows, and its levels into `levels`
/// where it has them; refused when the chunk holds fewer rows.
fn read_all<T: DataType>(
    mut r: ColumnReaderImpl<T>,
    n: usize,
    mut levels: Option<&mut Vec<i16>>,
) -> Result<Vec<T::T>, String> {
    let mut values = Vec::with_capacity(n);
    let mut got = 0;
    while got < n {
        let (records, _, _) = r
            .read_records(n - got, levels.as_deref_mut(), None, &mut values)
            .map_err(|e| e.to_string())?;
        if records == 0 {
            return Err("a column chunk shorter than its row group".to_string());
        }
        got += records;
    }
    Ok(values)
}

/// The `n` rows of a column of type `tc` into `cells`: a zero cell where
/// `levels` says NULL, else the next of `values` as `put` writes it. The rows
/// that are NULL.
fn fill<V>(
    values: Vec<V>,
    levels: Option<&[i16]>,
    n: usize,
    tc: TypeCode,
    cells: &mut Vec<u8>,
    mut put: impl FnMut(V, &mut Vec<u8>) -> Result<(), String>,
) -> Result<Vec<usize>, String> {
    let mut nulls = Vec::new();
    let mut values = values.into_iter();
    for row in 0..n {
        if levels.is_some_and(|l| l[row] == 0) {
            push_zero_cell(cells, tc);
            nulls.push(row);
            continue;
        }
        let v = values.next().ok_or("fewer values than rows")?;
        put(v, cells)?;
    }
    Ok(nulls)
}

#[cfg(test)]
#[path = "tests/columns.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/columns.rs"]
mod bench;
