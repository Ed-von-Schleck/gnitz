use sqlparser::ast::DataType;
use gnitz_protocol::TypeCode;
use crate::error::GnitzSqlError;

pub fn sql_type_to_typecode(dt: &DataType) -> Result<TypeCode, GnitzSqlError> {
    match dt {
        DataType::BigInt(_)                                 => Ok(TypeCode::I64),
        DataType::Int(_) | DataType::Integer(_)             => Ok(TypeCode::I32),
        DataType::SmallInt(_)                               => Ok(TypeCode::I16),
        DataType::TinyInt(_)                                => Ok(TypeCode::I8),
        DataType::BigIntUnsigned(_)                                                   => Ok(TypeCode::U64),
        DataType::IntUnsigned(_) | DataType::IntegerUnsigned(_) | DataType::UnsignedInteger => Ok(TypeCode::U32),
        DataType::SmallIntUnsigned(_)                                                 => Ok(TypeCode::U16),
        DataType::TinyIntUnsigned(_)                                                  => Ok(TypeCode::U8),
        DataType::Float(_)                                                            => Ok(TypeCode::F32),
        DataType::Double(_) | DataType::DoublePrecision | DataType::Real              => Ok(TypeCode::F64),
        DataType::Varchar(_) | DataType::Text | DataType::Char(_)     => Ok(TypeCode::String),
        DataType::Boolean => Err(GnitzSqlError::Unsupported(
            "BOOLEAN has no gnitz type; use TINYINT(1)".to_string()
        )),
        _ => Err(GnitzSqlError::Unsupported(format!("unsupported SQL type: {:?}", dt))),
    }
}
