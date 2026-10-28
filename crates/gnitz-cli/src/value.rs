//! A reply decoded into display values: one `Value` per visible cell, and each
//! row's weight beside it.

use std::fmt::Write as _;

use gnitz_core::{ScanReply, ZSetBatch};
use gnitz_expr::{ColumnLocator, FloatText};
use gnitz_wire::{
    format_date, format_decimal, format_timestamp, format_uuid, read_signed_exact, read_unsigned_exact, ColType,
    TypeCode,
};

/// One cell. Each variant keeps what its renderings need: the text a terminal
/// shows, and the JSON type a script reads back.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i128),
    UInt(u128),
    F32(f32),
    F64(f64),
    /// A string, and DECIMAL, DATE, TIMESTAMP and UUID in their canonical text,
    /// which JSON carries as a string so no precision is lost.
    Text(String),
    Bytes(Vec<u8>),
}

impl Value {
    /// The cell as a terminal shows it; `None` for NULL, which the caller
    /// spells as it is configured to.
    pub fn text(&self) -> Option<String> {
        Some(match self {
            Value::Null => return None,
            Value::Bool(b) => b.to_string(),
            Value::Int(v) => v.to_string(),
            Value::UInt(v) => v.to_string(),
            Value::F32(v) => FloatText(*v).to_string(),
            Value::F64(v) => FloatText(*v).to_string(),
            Value::Text(s) => s.clone(),
            Value::Bytes(b) => {
                let mut s = String::with_capacity(2 + 2 * b.len());
                s.push_str("\\x");
                for byte in b {
                    let _ = write!(s, "{byte:02x}");
                }
                s
            }
        })
    }

    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        match self {
            Value::Null => J::Null,
            Value::Bool(b) => J::Bool(*b),
            Value::Int(v) => i64::try_from(*v).map_or_else(|_| J::String(v.to_string()), J::from),
            Value::UInt(v) => u64::try_from(*v).map_or_else(|_| J::String(v.to_string()), J::from),
            // A non-finite float has no JSON number; serde would write it as null.
            Value::F32(v) if v.is_finite() => J::from(f64::from(*v)),
            Value::F64(v) if v.is_finite() => J::from(*v),
            Value::F32(_) | Value::F64(_) | Value::Text(_) | Value::Bytes(_) => {
                J::String(self.text().unwrap_or_default())
            }
        }
    }
}

/// Whether a column of `tc` reads right-aligned.
pub fn is_numeric(tc: TypeCode) -> bool {
    !matches!(
        tc,
        TypeCode::String | TypeCode::Blob | TypeCode::UUID | TypeCode::Date | TypeCode::Timestamp | TypeCode::Bool
    )
}

/// The cell at `loc` in `row` of `batch`.
pub fn cell(batch: &ZSetBatch, loc: ColumnLocator, ty: ColType, row: usize) -> Value {
    if loc.is_null(batch, row) {
        return Value::Null;
    }
    let mut scratch = [0u8; 16];
    let b = loc.native_le_bytes(batch, row, &mut scratch);
    let wide = |b: &[u8]| <[u8; 16]>::try_from(b).expect("a 128-bit cell is 16 bytes");
    match ty.tc {
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => Value::UInt(read_unsigned_exact(b).into()),
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => Value::Int(read_signed_exact(b).into()),
        TypeCode::Bool => Value::Bool(b[0] != 0),
        TypeCode::F32 => Value::F32(f32::from_le_bytes(b.try_into().expect("an F32 cell is 4 bytes"))),
        TypeCode::F64 => Value::F64(f64::from_le_bytes(b.try_into().expect("an F64 cell is 8 bytes"))),
        TypeCode::U128 => Value::UInt(u128::from_le_bytes(wide(b))),
        TypeCode::I128 => Value::Int(i128::from_le_bytes(wide(b))),
        TypeCode::UUID => Value::Text(format_uuid(u128::from_le_bytes(wide(b)))),
        TypeCode::Date => Value::Text(format_date(read_signed_exact(b))),
        TypeCode::Timestamp => Value::Text(format_timestamp(read_signed_exact(b))),
        TypeCode::Decimal => Value::Text(format_decimal(read_signed_exact(b).into(), ty.scale)),
        TypeCode::String => Value::Text(String::from_utf8_lossy(&loc.content(batch, row)).into_owned()),
        TypeCode::Blob => Value::Bytes(loc.content(batch, row).into_owned()),
    }
}

/// One column of a decoded reply.
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: ColType,
}

/// A reply's visible columns and its rows, each at its weight.
#[derive(Clone, Debug, Default)]
pub struct Rows {
    pub columns: Vec<Column>,
    pub rows: Vec<(Vec<Value>, i64)>,
}

impl Rows {
    /// Rows of `columns`, each at weight 1.
    pub fn of(columns: &[(&str, TypeCode)], rows: impl IntoIterator<Item = Vec<Value>>) -> Rows {
        Rows {
            columns: columns
                .iter()
                .map(|&(name, tc)| Column {
                    name: name.to_string(),
                    ty: ColType::of(tc),
                })
                .collect(),
            rows: rows.into_iter().map(|r| (r, 1)).collect(),
        }
    }

    pub fn decode(reply: &ScanReply) -> Rows {
        let schema = &reply.schema;
        let shown: Vec<(ColumnLocator, ColType)> = schema
            .visible_columns()
            .map(|(ci, c)| (schema.layout().locate(ci), c.ty))
            .collect();
        let columns = schema
            .visible_columns()
            .map(|(_, c)| Column { name: c.name.clone(), ty: c.ty })
            .collect();
        let batch = &reply.batch;
        let rows = (0..batch.len())
            .map(|row| {
                let cells = shown.iter().map(|&(loc, ty)| cell(batch, loc, ty, row)).collect();
                (cells, batch.weights[row])
            })
            .collect();
        Rows { columns, rows }
    }

    /// Whether every row stands at a positive weight, so the rows read as a
    /// bag: a row at weight `w` is `w` equal rows.
    pub fn is_bag(&self) -> bool {
        self.rows.iter().all(|&(_, w)| w > 0)
    }
}
