//! The one buffer every fixed-width value-text writer fills. Each of
//! [`crate::write_decimal`], [`crate::write_uuid`], [`crate::write_date`] and
//! [`crate::write_timestamp`] is its type's only spelling: the engine's
//! to-text cast writes it, and a client prints the value with it. A float's
//! spelling, [`crate::FloatText`], keeps the same rule.

/// The longest text any value-text writer produces: a UUID's 36. The others
/// are shorter — a TIMESTAMP at most 29 (`-290308-12-21 19:59:05.224192`, at
/// `i64::MIN`), a DECIMAL [`crate::SCALED_TEXT_MAX`], a DATE 14 at its `i32`
/// day width.
pub const VALUE_TEXT_MAX: usize = 36;

const _: () = assert!(crate::SCALED_TEXT_MAX <= VALUE_TEXT_MAX);
