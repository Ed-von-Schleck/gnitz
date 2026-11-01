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
use gnitz_core::{push_zero_cell, PkColumn, Schema, ZSetBatch};
use gnitz_expr::ColumnLocator;
use gnitz_wire::{ColType, ColumnDef, FixedInt, TypeCode};
use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit, Type as Physical, ZstdLevel};
use parquet::column::reader::{get_typed_column_reader, ColumnReader};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::schema::types::{ColumnPath, Type};

use crate::DumpError;

/// The file metadata key naming each column's gnitz type, in column order. Its
/// values are `gnitz_sql::sql_type_name`'s spelling, so a change to that
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

/// How a gnitz type's cells are held in its Parquet column.
#[derive(Clone, Copy)]
enum Codec {
    /// INT32: an integer of at most 4 bytes; an unsigned one by its bits.
    Int32(FixedInt),
    /// INT64: an 8-byte integer; an unsigned one by its bits.
    Int64,
    Bool,
    Float,
    Double,
    /// FIXED_LEN_BYTE_ARRAY(16): a 128-bit cell, big-endian.
    Wide,
    /// FIXED_LEN_BYTE_ARRAY(DECIMAL_BYTES): a DECIMAL's i64, big-endian and sign-extended.
    Decimal,
    /// BYTE_ARRAY; a TEXT one is UTF-8.
    Bytes {
        text: bool,
    },
}

impl Codec {
    fn physical(self) -> Physical {
        match self {
            Codec::Int32(_) => Physical::INT32,
            Codec::Int64 => Physical::INT64,
            Codec::Bool => Physical::BOOLEAN,
            Codec::Float => Physical::FLOAT,
            Codec::Double => Physical::DOUBLE,
            Codec::Wide | Codec::Decimal => Physical::FIXED_LEN_BYTE_ARRAY,
            Codec::Bytes { .. } => Physical::BYTE_ARRAY,
        }
    }
}

/// `ty`'s cells in the file, and the logical type its Parquet column carries.
fn codec(ty: ColType) -> (Codec, Option<LogicalType>) {
    let int = |fi: FixedInt| {
        let bits = 8 * fi.width() as i8;
        let codec = if bits == 64 { Codec::Int64 } else { Codec::Int32(fi) };
        (codec, Some(LogicalType::integer(bits, fi.is_signed())))
    };
    match ty.tc {
        TypeCode::U8 => int(FixedInt::U8),
        TypeCode::I8 => int(FixedInt::I8),
        TypeCode::U16 => int(FixedInt::U16),
        TypeCode::I16 => int(FixedInt::I16),
        TypeCode::U32 => int(FixedInt::U32),
        TypeCode::I32 => int(FixedInt::I32),
        TypeCode::U64 => int(FixedInt::U64),
        TypeCode::I64 => int(FixedInt::I64),
        TypeCode::U128 | TypeCode::I128 => (Codec::Wide, None),
        TypeCode::UUID => (Codec::Wide, Some(LogicalType::Uuid)),
        TypeCode::F32 => (Codec::Float, None),
        TypeCode::F64 => (Codec::Double, None),
        TypeCode::String => (Codec::Bytes { text: true }, Some(LogicalType::String)),
        TypeCode::Blob => (Codec::Bytes { text: false }, None),
        TypeCode::Date => (Codec::Int32(FixedInt::I32), Some(LogicalType::Date)),
        TypeCode::Timestamp => (Codec::Int64, Some(LogicalType::timestamp(false, TimeUnit::MICROS))),
        TypeCode::Decimal => (
            Codec::Decimal,
            Some(LogicalType::decimal(i32::from(ty.scale), DECIMAL_PRECISION)),
        ),
        TypeCode::Bool => (Codec::Bool, None),
    }
}

/// The Parquet column `def` is written as.
fn parquet_column(def: &ColumnDef) -> Result<Type, DumpError> {
    let (codec, logical) = codec(def.ty);
    let b = Type::primitive_type_builder(&def.name, codec.physical())
        .with_repetition(if def.is_nullable {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        })
        .with_logical_type(logical);
    let b = match codec {
        Codec::Wide => b.with_length(16),
        Codec::Decimal => b
            .with_length(DECIMAL_BYTES as i32)
            .with_precision(DECIMAL_PRECISION)
            .with_scale(i32::from(def.ty.scale)),
        _ => b,
    };
    Ok(b.build()?)
}

/// A table's Parquet file, written a batch — one row group — at a time.
pub(crate) struct TableWriter {
    writer: SerializedFileWriter<File>,
    /// Each visible column's place in a batch, its codec, and whether it may be NULL.
    cols: Vec<(ColumnLocator, Codec, bool)>,
}

impl TableWriter {
    pub(crate) fn create(path: &Path, schema: &Schema) -> Result<TableWriter, DumpError> {
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
        Ok(TableWriter {
            writer: SerializedFileWriter::new(file, Arc::new(root), Arc::new(props.build()))?,
            cols: schema
                .visible_columns()
                .map(|(ci, def)| (schema.layout().locate(ci), codec(def.ty).0, def.is_nullable))
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
            let &(loc, codec, nullable) = cols.next().expect("one writer per column");
            write_column(&mut writer, batch, loc, codec, nullable)?;
            writer.close()?;
        }
        group.close()?;
        Ok(())
    }

    /// Close the file, durable once this returns; the rows it holds.
    pub(crate) fn finish(mut self) -> Result<u64, DumpError> {
        let meta = self.writer.finish()?;
        self.writer.inner().sync_all()?;
        Ok(meta.file_metadata().num_rows() as u64)
    }
}

/// Column `loc` of `batch` into `writer`, its cells as `codec` holds them. The
/// file's schema is built from the same `codec`, so `writer` is of its type.
fn write_column(
    writer: &mut SerializedColumnWriter<'_>,
    batch: &ZSetBatch,
    loc: ColumnLocator,
    codec: Codec,
    nullable: bool,
) -> Result<(), DumpError> {
    let bit = loc.null_bit();
    let levels: Option<Vec<i16>> = nullable.then(|| batch.nulls.iter().map(|&w| i16::from(w & bit == 0)).collect());
    let levels = levels.as_deref();
    // Native little-endian cells; a key column's are decoded out of the key region.
    let cells: Cow<'_, [u8]> = match loc {
        ColumnLocator::Payload { slot, .. } => Cow::Borrowed(&batch.payload[slot as usize].bytes),
        ColumnLocator::Pk { byte_off, size, type_code } => {
            let mut cells = vec![0; batch.len() * size as usize];
            gnitz_wire::decode_pk_cells(
                batch.pks.region(),
                batch.pks.stride(),
                byte_off as usize,
                size as usize,
                type_code.is_signed_int(),
                &mut cells,
            );
            Cow::Owned(cells)
        }
    };
    let present = cells
        .chunks_exact(loc.size())
        .zip(&batch.nulls)
        .filter(|&(_, &w)| w & bit == 0)
        .map(|(c, _)| c);
    let le = |c: &[u8]| -> [u8; 8] { c.try_into().expect("an 8-byte cell") };
    match codec {
        Codec::Int32(fi) => put::<Int32Type>(writer, present.map(|c| fi.decode_le_i64(c) as i32).collect(), levels),
        Codec::Int64 => put::<Int64Type>(writer, present.map(|c| i64::from_le_bytes(le(c))).collect(), levels),
        Codec::Bool => put::<BoolType>(writer, present.map(|c| c[0] != 0).collect(), levels),
        Codec::Float => put::<FloatType>(
            writer,
            present
                .map(|c| f32::from_le_bytes(c.try_into().expect("a 4-byte cell")))
                .collect(),
            levels,
        ),
        Codec::Double => put::<DoubleType>(writer, present.map(|c| f64::from_le_bytes(le(c))).collect(), levels),
        Codec::Wide => put::<FixedLenByteArrayType>(
            writer,
            byte_values(present, |c, all| {
                let cell: [u8; 16] = c.try_into().expect("a 16-byte cell");
                all.extend_from_slice(&u128::from_le_bytes(cell).to_be_bytes());
            }),
            levels,
        ),
        Codec::Decimal => put::<FixedLenByteArrayType>(
            writer,
            byte_values(present, |c, all| {
                let v = i64::from_le_bytes(le(c));
                all.push((v >> 63) as u8);
                all.extend_from_slice(&v.to_be_bytes());
            }),
            levels,
        ),
        Codec::Bytes { .. } => put::<ByteArrayType>(
            writer,
            byte_values(present, |c, all| {
                all.extend_from_slice(gnitz_wire::german_string_content(c.first_chunk().unwrap(), &batch.blob))
            }),
            levels,
        ),
    }
}

/// `values` and their `levels` as one column chunk of `writer`, of type `T`.
fn put<T: DataType>(
    writer: &mut SerializedColumnWriter<'_>,
    values: Vec<T::T>,
    levels: Option<&[i16]>,
) -> Result<(), DumpError> {
    writer.typed::<T>().write_batch(&values, levels, None)?;
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

/// A table's Parquet file read back a row group at a time, each decoded into
/// the schema of the table it was dumped from as restored.
pub(crate) struct TableReader {
    reader: SerializedFileReader<File>,
    schema: Arc<Schema>,
    next: usize,
}

impl TableReader {
    /// Open `path`, whose columns must be `schema`'s by name, type and
    /// nullability.
    pub(crate) fn open(path: &Path, schema: &Arc<Schema>) -> Result<TableReader, DumpError> {
        let reader = SerializedFileReader::new(File::open(path)?)?;
        // A restored table has no hidden column, so file column i is schema column i.
        if schema.columns().iter().any(|c| c.is_hidden) {
            return Err(DumpError::from("restored into a table with hidden columns"));
        }
        let (root, types) = file_layout(schema)?;
        let meta = reader.metadata().file_metadata();
        let recorded = meta
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|kv| kv.key == COLUMNS_KEY))
            .and_then(|kv| kv.value.as_deref());
        if *meta.schema() != root || recorded != Some(types.as_str()) {
            return Err(DumpError::from("its columns are not the table's"));
        }
        Ok(TableReader {
            reader,
            schema: Arc::clone(schema),
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
        let n = usize::try_from(group.metadata().num_rows()).map_err(|_| DumpError::from("a negative row count"))?;
        let mut batch = ZSetBatch::new(schema);
        batch.weights = vec![1; n];
        batch.nulls = vec![0; n];
        let ZSetBatch { payload, nulls, blob, .. } = &mut batch;
        let mut read = |ci: usize, cells: &mut Vec<u8>| -> Result<(), DumpError> {
            let def = &schema.columns()[ci];
            let null_words = def
                .is_nullable
                .then_some((&mut nulls[..], schema.layout().locate(ci).null_bit()));
            read_column(group.get_column_reader(ci)?, n, def.ty, cells, null_words, blob)
                .map_err(|e| DumpError(format!("column {}: {e}", def.name)))
        };
        let mut keys = Vec::with_capacity(schema.pk_cols().len());
        for &ci in schema.pk_cols() {
            let mut cells = Vec::new();
            read(ci as usize, &mut cells)?;
            keys.push(cells);
        }
        for (pi, ci, _) in schema.payload_columns() {
            read(ci, &mut payload[pi].bytes)?;
        }
        batch.pks = PkColumn::from_native_columns(schema, &keys);
        Ok(Some(batch))
    }
}

/// The `n` cells of a column of type `ty` appended to `cells` as a batch region
/// holds them — native little-endian, a zero cell under NULL, a string's bytes
/// spilled into `blob` — each NULL row's word in `nulls` given the column's bit.
fn read_column(
    reader: ColumnReader,
    n: usize,
    ty: ColType,
    cells: &mut Vec<u8>,
    nulls: Option<(&mut [u64], u64)>,
    blob: &mut Vec<u8>,
) -> Result<(), String> {
    let tc = ty.tc;
    match codec(ty).0 {
        Codec::Int32(fi) => match fi.width() {
            1 => read_int32::<1>(reader, n, tc, cells, nulls, fi),
            2 => read_int32::<2>(reader, n, tc, cells, nulls, fi),
            _ => read_int32::<4>(reader, n, tc, cells, nulls, fi),
        },
        Codec::Int64 => read_cells::<Int64Type>(reader, n, tc, cells, nulls, |v, out| {
            out.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }),
        Codec::Bool => read_cells::<BoolType>(reader, n, tc, cells, nulls, |v, out| {
            out.push(u8::from(v));
            Ok(())
        }),
        Codec::Float => read_cells::<FloatType>(reader, n, tc, cells, nulls, |v, out| {
            out.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }),
        Codec::Double => read_cells::<DoubleType>(reader, n, tc, cells, nulls, |v, out| {
            out.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }),
        Codec::Wide => read_cells::<FixedLenByteArrayType>(reader, n, tc, cells, nulls, |v, out| {
            let be: &[u8; 16] = sized(v.data(), tc)?;
            out.extend_from_slice(&u128::from_be_bytes(*be).to_le_bytes());
            Ok(())
        }),
        Codec::Decimal => read_cells::<FixedLenByteArrayType>(reader, n, tc, cells, nulls, |v, out| {
            let [sign, be @ ..] = sized::<DECIMAL_BYTES>(v.data(), tc)?;
            let x = i64::from_be_bytes(*be);
            // Nine bytes of two's complement hold an `i64` exactly when the top
            // one is the sign of the rest.
            if *sign != (x >> 63) as u8 {
                return Err(format!("a {tc} cell out of range of a 64-bit integer"));
            }
            out.extend_from_slice(&x.to_le_bytes());
            Ok(())
        }),
        Codec::Bytes { text } => read_cells::<ByteArrayType>(reader, n, tc, cells, nulls, |v, out| {
            if text && std::str::from_utf8(v.data()).is_err() {
                return Err("a TEXT cell that is not UTF-8".to_string());
            }
            out.extend_from_slice(&gnitz_wire::encode_german_string(v.data(), blob));
            Ok(())
        }),
    }
}

/// A fixed-length cell of `tc`, refused unless it is `N` bytes long.
fn sized<const N: usize>(d: &[u8], tc: TypeCode) -> Result<&[u8; N], String> {
    d.try_into().map_err(|_| format!("a {tc} cell of {} bytes", d.len()))
}

/// An INT32 column of a type `W` bytes wide, refused where a value is out of its range.
fn read_int32<const W: usize>(
    reader: ColumnReader,
    n: usize,
    tc: TypeCode,
    cells: &mut Vec<u8>,
    nulls: Option<(&mut [u64], u64)>,
    fi: FixedInt,
) -> Result<(), String> {
    read_cells::<Int32Type>(reader, n, tc, cells, nulls, |v, out| {
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
/// `Some` exactly when the column may be NULL. `open` checked the file's schema against
/// `file_layout`, so `reader` is of type `T`.
fn read_cells<T: DataType>(
    reader: ColumnReader,
    n: usize,
    tc: TypeCode,
    cells: &mut Vec<u8>,
    mut nulls: Option<(&mut [u64], u64)>,
    mut decode: impl FnMut(T::T, &mut Vec<u8>) -> Result<(), String>,
) -> Result<(), String> {
    let mut r = get_typed_column_reader::<T>(reader);
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
        if let (Some(levels), Some((words, bit))) = (&levels, &mut nulls) {
            if levels[row] == 0 {
                push_zero_cell(cells, tc);
                words[row] |= *bit;
                continue;
            }
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
