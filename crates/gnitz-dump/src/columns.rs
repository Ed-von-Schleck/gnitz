//! A table's rows as a Parquet file: one Parquet column per visible column, in
//! schema order, each in the Parquet type a reader outside gnitz reads it as.
//!
//! | gnitz | Parquet |
//! |---|---|
//! | TINYINT … INT, signed or not | INT32, `INTEGER(bits, signed)` |
//! | BIGINT, BIGINT UNSIGNED | INT64, `INTEGER(64, signed)` |
//! | HUGEINT, UHUGEINT | FIXED_LEN_BYTE_ARRAY(16), big-endian two's complement |
//! | UUID | FIXED_LEN_BYTE_ARRAY(16), `UUID` |
//! | FLOAT, DOUBLE | FLOAT, DOUBLE, every bit kept |
//! | TEXT | BYTE_ARRAY, `STRING` |
//! | BLOB | BYTE_ARRAY |
//! | DATE | INT32, `DATE` |
//! | TIMESTAMP | INT64, `TIMESTAMP(MICROS, local)` |
//! | DECIMAL(p, s) | INT64, `DECIMAL(18, s)` |
//! | BOOLEAN | BOOLEAN |
//!
//! A 128-bit integer has no Parquet type that holds every value — a
//! `DECIMAL(38, 0)` stops short of `i128`'s range — so each is its 16 bytes,
//! with the column's gnitz type in the file's `gnitz.columns` metadata. A
//! nullable column is OPTIONAL, every other one REQUIRED.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use gnitz_core::{BatchAppender, Schema, ZSetBatch};
use gnitz_expr::ColumnLocator;
use gnitz_wire::{ColumnDef, FixedInt, TypeCode};
use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit, Type as Physical, ZstdLevel};
use parquet::column::reader::ColumnReader;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;

use crate::DumpError;

/// The file metadata key naming each column's gnitz type, in column order.
pub(crate) const COLUMNS_KEY: &str = "gnitz.columns";

/// The zstd level a dump's pages are compressed at.
const ZSTD_LEVEL: i32 = 3;

/// Each visible column of a table: where its cells are in a batch, and its
/// definition.
fn columns(schema: &Schema) -> Vec<(ColumnLocator, ColumnDef)> {
    schema
        .visible_columns()
        .map(|(ci, def)| (schema.layout().locate(ci), def.clone()))
        .collect()
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
            Physical::INT64,
            Some(LogicalType::decimal(
                i32::from(def.ty.scale),
                i32::from(gnitz_wire::decimal::MAX_DECIMAL_SCALE),
            )),
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
    if physical == Physical::FIXED_LEN_BYTE_ARRAY {
        b = b.with_length(16);
    }
    if tc == TypeCode::Decimal {
        b = b
            .with_precision(i32::from(gnitz_wire::decimal::MAX_DECIMAL_SCALE))
            .with_scale(i32::from(def.ty.scale));
    }
    Ok(b.build()?)
}

/// A table's Parquet file, written a batch — one row group — at a time.
pub(crate) struct TableWriter {
    writer: SerializedFileWriter<File>,
    cols: Vec<(ColumnLocator, ColumnDef)>,
    rows: u64,
}

impl TableWriter {
    pub(crate) fn create(path: &Path, schema: &Schema) -> Result<TableWriter, DumpError> {
        let visible = columns(schema);
        let fields = visible
            .iter()
            .map(|(_, def)| parquet_column(def).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let root = Type::group_type_builder("gnitz").with_fields(fields).build()?;
        let types: Vec<String> = visible
            .iter()
            .map(|(_, def)| gnitz_sql::sql_type_name(def.ty))
            .collect();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL)?))
            .set_key_value_metadata(Some(vec![KeyValue::new(
                COLUMNS_KEY.to_string(),
                serde_json::Value::from(types).to_string(),
            )]))
            .build();
        let file = File::create(path)?;
        Ok(TableWriter {
            writer: SerializedFileWriter::new(file, Arc::new(root), Arc::new(props))?,
            cols: visible,
            rows: 0,
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
        self.rows += batch.len() as u64;
        Ok(())
    }

    /// Close the file; the rows it holds.
    pub(crate) fn finish(self) -> Result<u64, DumpError> {
        self.writer.close()?;
        Ok(self.rows)
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
    let int = |i: usize| {
        let fi = FixedInt::from_type_code(def.ty.tc).expect("an INT32, INT64 or BOOLEAN column holds integers");
        loc.decode_i64(batch, i, fi)
    };
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
            let values: Vec<FixedLenByteArray> = present
                .iter()
                .map(|&i| {
                    let mut scratch = [0u8; 16];
                    let le: [u8; 16] = loc
                        .native_le_bytes(batch, i, &mut scratch)
                        .try_into()
                        .expect("a 128-bit cell is 16 bytes");
                    FixedLenByteArray::from(u128::from_le_bytes(le).to_be_bytes().to_vec())
                })
                .collect();
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::ByteArrayColumnWriter(w) => {
            let values: Vec<ByteArray> = present
                .iter()
                .map(|&i| ByteArray::from(loc.content(batch, i).into_owned()))
                .collect();
            w.write_batch(&values, levels, None)?;
        }
        ColumnWriter::Int96ColumnWriter(_) => unreachable!("no gnitz type is written as INT96"),
    }
    Ok(())
}

/// One Parquet column chunk read whole: its non-null values, and where it has
/// levels, whether each row holds one.
enum Values {
    I32(Vec<i32>),
    I64(Vec<i64>),
    Bool(Vec<bool>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Fixed(Vec<FixedLenByteArray>),
    Bytes(Vec<ByteArray>),
}

/// A column chunk being walked row by row.
struct Cursor {
    values: Values,
    levels: Option<Vec<i16>>,
    /// Whether an INT32 or INT64 holds its column's bits as an unsigned integer.
    unsigned: bool,
    next: usize,
}

/// One cell, as a column chunk holds it.
enum Cell<'a> {
    Int(i128),
    Bool(bool),
    F32(f32),
    F64(f64),
    Wide([u8; 16]),
    Bytes(&'a [u8]),
}

impl Cursor {
    /// Where row `row`'s value stands among the chunk's values; `None` for NULL.
    fn advance(&mut self, row: usize) -> Option<usize> {
        if self.levels.as_ref().is_some_and(|l| l[row] == 0) {
            return None;
        }
        self.next += 1;
        Some(self.next - 1)
    }

    /// The value at `at`; an INT32 or INT64 of an unsigned column read as the
    /// unsigned integer its bits are.
    fn get(&self, at: usize) -> Cell<'_> {
        match &self.values {
            Values::I32(v) if self.unsigned => Cell::Int(i128::from(v[at] as u32)),
            Values::I32(v) => Cell::Int(i128::from(v[at])),
            Values::I64(v) if self.unsigned => Cell::Int(i128::from(v[at] as u64)),
            Values::I64(v) => Cell::Int(i128::from(v[at])),
            Values::Bool(v) => Cell::Bool(v[at]),
            Values::F32(v) => Cell::F32(v[at]),
            Values::F64(v) => Cell::F64(v[at]),
            Values::Fixed(v) => Cell::Wide(v[at].data().try_into().expect("read_chunk checked the width")),
            Values::Bytes(v) => Cell::Bytes(v[at].data()),
        }
    }
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
        let file_schema = reader.metadata().file_metadata().schema_descr();
        let want: Vec<Type> = columns(schema)
            .iter()
            .map(|(_, def)| parquet_column(def))
            .collect::<Result<_, _>>()?;
        if file_schema.num_columns() != want.len() {
            return Err(DumpError::from(format!(
                "{shown}: {} columns, the table has {}",
                file_schema.num_columns(),
                want.len()
            )));
        }
        for (i, want) in want.iter().enumerate() {
            let got = file_schema.column(i);
            let same = got.name().eq_ignore_ascii_case(want.name())
                && got.physical_type() == want.get_physical_type()
                && got.self_type().get_basic_info().repetition() == want.get_basic_info().repetition();
            if !same {
                return Err(DumpError::from(format!(
                    "{shown}: column {i} is {} {:?}, the table's is {} {:?}",
                    got.name(),
                    got.physical_type(),
                    want.name(),
                    want.get_physical_type()
                )));
            }
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
        let columns = schema.columns();
        let n = usize::try_from(group.metadata().num_rows()).map_err(|_| DumpError::from("a negative row count"))?;
        let mut cursors = columns
            .iter()
            .enumerate()
            .map(|(ci, def)| read_chunk(group.get_column_reader(ci)?, n, def))
            .collect::<Result<Vec<_>, DumpError>>()?;
        let pk = schema.pk_cols();
        let mut batch = ZSetBatch::with_capacity(schema, n);
        let mut app = BatchAppender::new(&mut batch);
        let mut key = vec![0u128; pk.len()];
        let mut at: Vec<Option<usize>> = vec![None; columns.len()];
        for row in 0..n {
            for (slot, cursor) in at.iter_mut().zip(cursors.iter_mut()) {
                *slot = cursor.advance(row);
            }
            let cell = |ci: usize| at[ci].map(|i| cursors[ci].get(i));
            for (slot, &ci) in key.iter_mut().zip(pk) {
                let tc = columns[ci as usize].ty.tc;
                *slot = match cell(ci as usize) {
                    Some(Cell::Int(v)) => checked_int(tc, v)
                        .map_err(|e| DumpError::from(format!("{}: key column {ci}: {e}", self.path)))?
                        as u128,
                    Some(Cell::Bool(b)) => u128::from(b),
                    Some(Cell::Wide(b)) => u128::from_be_bytes(b),
                    _ => return Err(DumpError::from(format!("{}: a key cell no key holds", self.path))),
                };
            }
            let row_app = app.add_row_natives(&key, 1);
            for (_, ci, def) in schema.payload_columns() {
                put_cell(row_app, def.ty.tc, cell(ci).as_ref())
                    .map_err(|e| DumpError::from(format!("{}: column {}: {e}", self.path, def.name)))?;
            }
        }
        Ok(Some(batch))
    }
}

/// `cell` appended to the next payload column of type `tc`.
fn put_cell(app: &mut BatchAppender<'_>, tc: TypeCode, cell: Option<&Cell<'_>>) -> Result<(), String> {
    match (tc, cell) {
        (_, None) => {
            app.null();
        }
        (TypeCode::String, Some(Cell::Bytes(b))) => {
            let s = std::str::from_utf8(b).map_err(|_| "a TEXT cell that is not UTF-8".to_string())?;
            app.str_val(s);
        }
        (TypeCode::Blob, Some(Cell::Bytes(b))) => {
            app.bytes_val(b);
        }
        (TypeCode::F32, Some(Cell::F32(v))) => {
            app.f32_val(*v);
        }
        (TypeCode::F64, Some(Cell::F64(v))) => {
            app.f64_val(*v);
        }
        (TypeCode::U128 | TypeCode::I128 | TypeCode::UUID, Some(Cell::Wide(b))) => {
            app.u128_val(u128::from_be_bytes(*b));
        }
        (TypeCode::Bool, Some(Cell::Bool(b))) => {
            app.int_val(i128::from(*b));
        }
        (_, Some(Cell::Int(v))) => {
            app.int_val(checked_int(tc, *v)?);
        }
        _ => return Err(format!("a cell of another type than {tc}")),
    }
    Ok(())
}

/// `v`, refused unless a column of type `tc` holds it.
fn checked_int(tc: TypeCode, v: i128) -> Result<i128, String> {
    let fi = FixedInt::from_type_code(tc).ok_or_else(|| format!("an integer for a {tc} column"))?;
    let (lo, hi) = fi.range();
    if !(lo..=hi).contains(&v) {
        return Err(format!("{v} is out of range for {tc}"));
    }
    Ok(v)
}

/// The `n` rows of the chunk of column `def`.
fn read_chunk(reader: ColumnReader, n: usize, def: &ColumnDef) -> Result<Cursor, DumpError> {
    let mut levels = def.is_nullable.then(|| Vec::with_capacity(n));
    macro_rules! read {
        ($r:expr, $variant:ident) => {{
            let mut values = Vec::with_capacity(n);
            let mut r = $r;
            let mut got = 0;
            while got < n {
                let (records, _, _) = r.read_records(n - got, levels.as_mut(), None, &mut values)?;
                if records == 0 {
                    return Err(DumpError::from("a column chunk shorter than its row group"));
                }
                got += records;
            }
            Values::$variant(values)
        }};
    }
    let values = match reader {
        ColumnReader::Int32ColumnReader(r) => read!(r, I32),
        ColumnReader::Int64ColumnReader(r) => read!(r, I64),
        ColumnReader::BoolColumnReader(r) => read!(r, Bool),
        ColumnReader::FloatColumnReader(r) => read!(r, F32),
        ColumnReader::DoubleColumnReader(r) => read!(r, F64),
        ColumnReader::FixedLenByteArrayColumnReader(r) => read!(r, Fixed),
        ColumnReader::ByteArrayColumnReader(r) => read!(r, Bytes),
        ColumnReader::Int96ColumnReader(_) => return Err(DumpError::from("an INT96 column")),
    };
    if let Values::Fixed(v) = &values {
        if v.iter().any(|v| v.data().len() != 16) {
            return Err(DumpError::from("a 128-bit cell that is not 16 bytes"));
        }
    }
    let unsigned = FixedInt::from_type_code(def.ty.tc).is_some_and(|fi| !fi.is_signed());
    Ok(Cursor { values, levels, unsigned, next: 0 })
}

#[cfg(test)]
#[path = "tests/columns.rs"]
mod tests;
