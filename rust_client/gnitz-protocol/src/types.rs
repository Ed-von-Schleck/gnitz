use std::sync::OnceLock;
use crate::error::ProtocolError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeCode {
    U8     = 1,
    I8     = 2,
    U16    = 3,
    I16    = 4,
    U32    = 5,
    I32    = 6,
    F32    = 7,
    U64    = 8,
    I64    = 9,
    F64    = 10,
    String = 11,
    U128   = 12,
}

impl TypeCode {
    /// Wire stride in bytes. String returns 8 (u32 offset + u32 length).
    pub fn wire_stride(self) -> usize {
        match self {
            TypeCode::U8  | TypeCode::I8  => 1,
            TypeCode::U16 | TypeCode::I16 => 2,
            TypeCode::U32 | TypeCode::I32 | TypeCode::F32 => 4,
            TypeCode::U64 | TypeCode::I64 | TypeCode::F64 | TypeCode::String => 8,
            TypeCode::U128 => 16,
        }
    }

    pub fn try_from_u64(v: u64) -> Result<Self, ProtocolError> {
        match v {
            1  => Ok(TypeCode::U8),
            2  => Ok(TypeCode::I8),
            3  => Ok(TypeCode::U16),
            4  => Ok(TypeCode::I16),
            5  => Ok(TypeCode::U32),
            6  => Ok(TypeCode::I32),
            7  => Ok(TypeCode::F32),
            8  => Ok(TypeCode::U64),
            9  => Ok(TypeCode::I64),
            10 => Ok(TypeCode::F64),
            11 => Ok(TypeCode::String),
            12 => Ok(TypeCode::U128),
            _  => Err(ProtocolError::UnknownTypeCode(v)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name:        std::string::String,
    pub type_code:   TypeCode,
    pub is_nullable: bool,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub columns:  Vec<ColumnDef>,
    pub pk_index: usize,
}

/// Returns the META_SCHEMA singleton (4 columns: col_idx/U64 pk=0, type_code/U64, flags/U64, name/String).
pub fn meta_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "col_idx".into(),   type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "type_code".into(), type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "flags".into(),     type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "name".into(),      type_code: TypeCode::String, is_nullable: false },
        ],
        pk_index: 0,
    })
}

/// Per-column payload data.
#[derive(Clone, Debug)]
pub enum ColData {
    /// Raw little-endian bytes; length = count * wire_stride (for all non-String, non-U128).
    Fixed(Vec<u8>),
    Strings(Vec<Option<std::string::String>>),
    U128s(Vec<u128>),
}

#[derive(Clone, Debug)]
pub struct ZSetBatch {
    pub pk_lo:   Vec<u64>,
    pub pk_hi:   Vec<u64>,
    pub weights: Vec<i64>,
    pub nulls:   Vec<u64>,
    /// One entry per schema column. Entry at pk_index is a placeholder (Fixed(vec![])).
    pub columns: Vec<ColData>,
}

impl ZSetBatch {
    pub fn new(schema: &Schema) -> Self {
        let columns = schema.columns.iter().enumerate().map(|(ci, col)| {
            if ci == schema.pk_index {
                ColData::Fixed(vec![])
            } else {
                match col.type_code {
                    TypeCode::String => ColData::Strings(vec![]),
                    TypeCode::U128   => ColData::U128s(vec![]),
                    _                => ColData::Fixed(vec![]),
                }
            }
        }).collect();
        ZSetBatch {
            pk_lo: vec![],
            pk_hi: vec![],
            weights: vec![],
            nulls: vec![],
            columns,
        }
    }

    pub fn len(&self) -> usize {
        self.pk_lo.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pk_lo.is_empty()
    }
}
