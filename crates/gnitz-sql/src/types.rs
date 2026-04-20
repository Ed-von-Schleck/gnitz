//! SQL type mapping re-export. The mapping itself lives in
//! `gnitz-sql-parse` so it's usable from `gnitz-engine`.

use gnitz_protocol::TypeCode;
use sqlparser::ast::DataType;

use crate::error::GnitzSqlError;

pub fn sql_type_to_typecode(dt: &DataType) -> Result<TypeCode, GnitzSqlError> {
    gnitz_sql_parse::sql_type_to_typecode(dt).map_err(GnitzSqlError::from)
}
