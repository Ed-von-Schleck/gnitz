//! Pure SQL parser for gnitz. Produces `DesiredState` (from gnitz-wire)
//! for DDL and maps SQL data types to engine TypeCodes. No client
//! dependency — linkable from gnitz-engine.

mod error;
mod types;
pub mod migration;
pub mod classify;

pub use error::{GnitzSqlParseError, extract_name, extract_table_factor_name};
pub use types::sql_type_to_typecode;
pub use classify::{classify_sql, Classification};
