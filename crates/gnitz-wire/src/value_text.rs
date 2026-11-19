//! The one buffer every fixed-width value-text writer fills, and
//! [`ValueText`], which picks the writer by type. Each of
//! [`crate::write_decimal`], [`crate::write_uuid`], [`crate::write_date`] and
//! [`crate::write_timestamp`] is its type's only spelling: the engine's
//! to-text cast writes it, and a client prints the value with it. A float's
//! spelling, [`crate::FloatText`], keeps the same rule.

use core::fmt;

use crate::{ColType, FixedInt, FloatText, TypeCode, write_date, write_decimal, write_timestamp, write_uuid};

/// The longest text any value-text writer produces: a UUID's 36. The others
/// are shorter — a TIMESTAMP at most 29 (`-290308-12-21 19:59:05.224192`, at
/// `i64::MIN`), a DECIMAL 22, a DATE 14 at its `i32`
/// day width.
pub const VALUE_TEXT_MAX: usize = 36;

const _: () = assert!(crate::SCALED_TEXT_MAX <= VALUE_TEXT_MAX);

/// A value of a fixed-width type as a client prints it: `.1` is the native
/// value, zero-extended from the type's width. STRING and BLOB are not
/// fixed-width and have no text here.
pub struct ValueText(pub ColType, pub u128);

impl fmt::Display for ValueText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ValueText(ColType { tc, scale }, v) = *self;
        let buf = &mut [0; VALUE_TEXT_MAX];
        match tc {
            TypeCode::Bool => f.write_str(if v != 0 { "true" } else { "false" }),
            TypeCode::F32 => FloatText(f32::from_bits(v as u32)).fmt(f),
            TypeCode::F64 => FloatText(f64::from_bits(v as u64)).fmt(f),
            TypeCode::U128 => v.fmt(f),
            TypeCode::I128 => (v as i128).fmt(f),
            TypeCode::UUID => f.write_str(write_uuid(v, buf)),
            TypeCode::String | TypeCode::Blob => unreachable!("ValueText of a variable-width {tc:?}"),
            _ => {
                let fi = FixedInt::from_type_code(tc).expect("an integer-stored type");
                let i = fi.unpack(v);
                match tc {
                    TypeCode::Date => f.write_str(write_date(i, buf)),
                    TypeCode::Timestamp => f.write_str(write_timestamp(i, buf)),
                    TypeCode::Decimal => f.write_str(write_decimal(i, scale, buf)),
                    _ if fi.is_signed() => i.fmt(f),
                    _ => (i as u64).fmt(f),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/value_text.rs"]
mod tests;
