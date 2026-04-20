//! Pure SQL parser for gnitz. Produces `DesiredState` (from gnitz-wire)
//! for DDL and maps SQL data types to engine TypeCodes. No client
//! dependency — linkable from gnitz-engine.

mod error;
mod types;
pub mod migration;
pub mod classify;
pub mod expr_program;
pub mod circuit;
pub mod plan_types;
pub mod resolver;
pub mod binder;
pub mod expr_compile;
pub mod view;

pub use error::{GnitzSqlParseError, extract_name, extract_table_factor_name};
pub use types::sql_type_to_typecode;
pub use classify::{classify_sql, Classification};
pub use resolver::CatalogResolver;
pub use circuit::{CircuitBuilder, CircuitGraph, NodeId};
pub use expr_program::{ExprBuilder, ExprProgram};
