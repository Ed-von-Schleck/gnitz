//! A reply decoded into display values: one `Value` per visible cell, and each
//! row's weight beside it.

use std::borrow::Cow;
use std::fmt::Write as _;

use gnitz_core::{ScanReply, ZSetBatch};
use gnitz_expr::{ColumnLocator, FloatText};
use gnitz_wire::{format_date, format_decimal, format_timestamp, format_uuid, ColType, FixedInt, TypeCode};

/// One cell. Each variant keeps what its renderings need: the text a terminal
/// shows, and the JSON type a script reads back.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    /// HUGEINT and UHUGEINT, which JSON carries as a string, as no JSON reader
    /// is bound to hold a number that wide.
    I128(i128),
    U128(u128),
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
    pub fn text(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Owned(match self {
            Value::Null => return None,
            Value::Text(s) => return Some(Cow::Borrowed(s)),
            Value::Bool(b) => b.to_string(),
            Value::Int(v) => v.to_string(),
            Value::UInt(v) => v.to_string(),
            Value::I128(v) => v.to_string(),
            Value::U128(v) => v.to_string(),
            Value::F32(v) => FloatText(*v).to_string(),
            Value::F64(v) => FloatText(*v).to_string(),
            Value::Bytes(b) => {
                let mut s = String::with_capacity(2 + 2 * b.len());
                s.push_str("\\x");
                for byte in b {
                    let _ = write!(s, "{byte:02x}");
                }
                s
            }
        }))
    }

    /// Append the cell as JSON to `out`: integers up to 64 bits and booleans
    /// as JSON's own, a float as a number where it is finite; anything else as
    /// its text in a string, so no precision is lost.
    pub fn write_json(&self, out: &mut String) {
        match self {
            Value::Null => out.push_str("null"),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::Int(v) => {
                let _ = write!(out, "{v}");
            }
            Value::UInt(v) => {
                let _ = write!(out, "{v}");
            }
            // The shortest text that reads back as the float itself, of its
            // own width; a non-finite float has no JSON number.
            Value::F32(v) if v.is_finite() => out.push_str(&serde_json::to_string(v).expect(SERIALIZES)),
            Value::F64(v) if v.is_finite() => out.push_str(&serde_json::to_string(v).expect(SERIALIZES)),
            _ => out.push_str(&json_string(&self.text().unwrap_or_default())),
        }
    }
}

const SERIALIZES: &str = "a finite float or a string serializes";

/// `s` as a JSON string.
pub fn json_string(s: &str) -> String {
    serde_json::to_string(s).expect(SERIALIZES)
}

/// The cell at `loc` in `row` of `batch`.
fn cell(batch: &ZSetBatch, loc: ColumnLocator, ty: ColType, row: usize) -> Value {
    if loc.is_null(batch, row) {
        return Value::Null;
    }
    let mut scratch = [0u8; 16];
    let b = loc.native_le_bytes(batch, row, &mut scratch);
    let wide = |b: &[u8]| <[u8; 16]>::try_from(b).expect("a 128-bit cell is 16 bytes");
    let int = || {
        loc.decode_i64(
            batch,
            row,
            FixedInt::from_type_code(ty.tc).expect("an integer-stored type"),
        )
    };
    match ty.tc {
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => Value::UInt(int() as u64),
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => Value::Int(int()),
        TypeCode::Bool => Value::Bool(b[0] != 0),
        TypeCode::F32 => Value::F32(f32::from_le_bytes(b.try_into().expect("an F32 cell is 4 bytes"))),
        TypeCode::F64 => Value::F64(f64::from_le_bytes(b.try_into().expect("an F64 cell is 8 bytes"))),
        TypeCode::U128 => Value::U128(u128::from_le_bytes(wide(b))),
        TypeCode::I128 => Value::I128(i128::from_le_bytes(wide(b))),
        TypeCode::UUID => Value::Text(format_uuid(u128::from_le_bytes(wide(b)))),
        TypeCode::Date => Value::Text(format_date(int())),
        TypeCode::Timestamp => Value::Text(format_timestamp(int())),
        TypeCode::Decimal => Value::Text(format_decimal(int(), ty.scale)),
        TypeCode::String => Value::Text(String::from_utf8_lossy(&loc.content(batch, row)).into_owned()),
        TypeCode::Blob => Value::Bytes(loc.content(batch, row).into_owned()),
    }
}

/// One column of a decoded reply.
pub struct Column {
    pub name: String,
    pub tc: TypeCode,
}

/// A reply's visible columns and its rows, each at its weight.
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
                .map(|&(name, tc)| Column { name: name.to_string(), tc })
                .collect(),
            rows: rows.into_iter().map(|r| (r, 1)).collect(),
        }
    }

    pub fn decode(reply: &ScanReply) -> Rows {
        let schema = &reply.schema;
        let (shown, columns): (Vec<(ColumnLocator, ColType)>, Vec<Column>) = schema
            .visible_columns()
            .map(|(ci, c)| {
                let column = Column { name: c.name.to_string(), tc: c.ty.tc };
                ((schema.layout().locate(ci), c.ty), column)
            })
            .unzip();
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
