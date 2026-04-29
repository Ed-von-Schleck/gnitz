use std::sync::OnceLock;
use super::error::ProtocolError;

use gnitz_wire::type_code as tc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeCode {
    U8     = tc::U8,
    I8     = tc::I8,
    U16    = tc::U16,
    I16    = tc::I16,
    U32    = tc::U32,
    I32    = tc::I32,
    F32    = tc::F32,
    U64    = tc::U64,
    I64    = tc::I64,
    F64    = tc::F64,
    String = tc::STRING,
    U128   = tc::U128,
    UUID   = tc::UUID,
}

impl TypeCode {
    /// Wire stride in bytes. String returns 16 (German String struct).
    pub fn wire_stride(self) -> usize {
        gnitz_wire::wire_stride(self as u8)
    }

    pub fn try_from_u64(v: u64) -> Result<Self, ProtocolError> {
        match v {
            _ if v == tc::U8     as u64 => Ok(TypeCode::U8),
            _ if v == tc::I8     as u64 => Ok(TypeCode::I8),
            _ if v == tc::U16    as u64 => Ok(TypeCode::U16),
            _ if v == tc::I16    as u64 => Ok(TypeCode::I16),
            _ if v == tc::U32    as u64 => Ok(TypeCode::U32),
            _ if v == tc::I32    as u64 => Ok(TypeCode::I32),
            _ if v == tc::F32    as u64 => Ok(TypeCode::F32),
            _ if v == tc::U64    as u64 => Ok(TypeCode::U64),
            _ if v == tc::I64    as u64 => Ok(TypeCode::I64),
            _ if v == tc::F64    as u64 => Ok(TypeCode::F64),
            _ if v == tc::STRING as u64 => Ok(TypeCode::String),
            _ if v == tc::U128   as u64 => Ok(TypeCode::U128),
            _ if v == tc::UUID   as u64 => Ok(TypeCode::UUID),
            _ => Err(ProtocolError::UnknownTypeCode(v)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub name:        std::string::String,
    pub type_code:   TypeCode,
    pub is_nullable: bool,
    pub fk_table_id: u64,
    pub fk_col_idx:  u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub columns:  Vec<ColumnDef>,
    pub pk_index: usize,
}

/// Returns the META_SCHEMA singleton (4 columns: col_idx/U64 pk=0, type_code/U64, flags/U64, name/String).
pub fn meta_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "col_idx".into(),   type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "type_code".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "flags".into(),     type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(),      type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PkColumn {
    U64s(Vec<u64>),
    U128s(Vec<u128>),
}

impl PkColumn {
    pub fn for_type(tc: TypeCode) -> Self {
        if tc == TypeCode::U128 || tc == TypeCode::UUID { PkColumn::U128s(vec![]) } else { PkColumn::U64s(vec![]) }
    }
    pub fn len(&self) -> usize {
        match self { PkColumn::U64s(v) => v.len(), PkColumn::U128s(v) => v.len() }
    }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn get(&self, i: usize) -> u128 {
        match self { PkColumn::U64s(v) => v[i] as u128, PkColumn::U128s(v) => v[i] }
    }
    pub fn push_u128(&mut self, pk: u128) {
        match self { PkColumn::U64s(v) => v.push(pk as u64), PkColumn::U128s(v) => v.push(pk) }
    }
    pub fn set_u128(&mut self, i: usize, pk: u128) {
        match self { PkColumn::U64s(v) => v[i] = pk as u64, PkColumn::U128s(v) => v[i] = pk }
    }
    pub fn swap(&mut self, i: usize, j: usize) {
        match self { PkColumn::U64s(v) => v.swap(i, j), PkColumn::U128s(v) => v.swap(i, j) }
    }
    pub fn clear(&mut self) {
        match self { PkColumn::U64s(v) => v.clear(), PkColumn::U128s(v) => v.clear() }
    }
    pub fn truncate(&mut self, len: usize) {
        match self { PkColumn::U64s(v) => v.truncate(len), PkColumn::U128s(v) => v.truncate(len) }
    }
    pub fn to_vec_u128(&self) -> Vec<u128> {
        match self {
            PkColumn::U64s(v) => v.iter().map(|&x| x as u128).collect(),
            PkColumn::U128s(v) => v.clone(),
        }
    }
}

impl PartialEq<Vec<u128>> for PkColumn {
    fn eq(&self, other: &Vec<u128>) -> bool {
        if self.len() != other.len() { return false; }
        match self {
            PkColumn::U64s(v) => v.iter().zip(other).all(|(&a, &b)| a as u128 == b),
            PkColumn::U128s(v) => v.as_slice() == other.as_slice(),
        }
    }
}

/// Per-column payload data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColData {
    /// Raw little-endian bytes; length = count * wire_stride (for all non-String, non-U128).
    Fixed(Vec<u8>),
    Strings(Vec<Option<std::string::String>>),
    U128s(Vec<u128>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZSetBatch {
    pub pks:     PkColumn,
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
                    TypeCode::U128 | TypeCode::UUID => ColData::U128s(vec![]),
                    _                => ColData::Fixed(vec![]),
                }
            }
        }).collect();
        ZSetBatch {
            pks: PkColumn::for_type(schema.columns[schema.pk_index].type_code),
            weights: vec![],
            nulls: vec![],
            columns,
        }
    }

    pub fn len(&self) -> usize {
        self.pks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pks.is_empty()
    }

    /// Append all rows from `other` into `self`. Panics if column layouts differ.
    pub fn extend_from(&mut self, other: &ZSetBatch) {
        match (&mut self.pks, &other.pks) {
            (PkColumn::U64s(a), PkColumn::U64s(b)) => a.extend_from_slice(b),
            (PkColumn::U128s(a), PkColumn::U128s(b)) => a.extend_from_slice(b),
            _ => panic!("extend_from: pk column type mismatch"),
        }
        self.weights.extend_from_slice(&other.weights);
        self.nulls.extend_from_slice(&other.nulls);
        for (a, b) in self.columns.iter_mut().zip(other.columns.iter()) {
            match (a, b) {
                (ColData::Fixed(a), ColData::Fixed(b)) => a.extend_from_slice(b),
                (ColData::Strings(a), ColData::Strings(b)) => a.extend(b.iter().cloned()),
                (ColData::U128s(a), ColData::U128s(b)) => a.extend_from_slice(b),
                _ => panic!("extend_from: column type mismatch"),
            }
        }
    }

    /// Validate that all vectors are consistently sized for the given schema.
    pub fn validate(&self, schema: &Schema) -> Result<(), std::string::String> {
        let n = self.pks.len();
        if self.weights.len() != n {
            return Err(format!("weights length {} != row count {}", self.weights.len(), n));
        }
        if self.nulls.len() != n {
            return Err(format!("nulls length {} != row count {}", self.nulls.len(), n));
        }
        if self.columns.len() != schema.columns.len() {
            return Err(format!(
                "column count {} != schema column count {}",
                self.columns.len(), schema.columns.len()
            ));
        }
        for (ci, col) in self.columns.iter().enumerate() {
            if ci == schema.pk_index { continue; }
            match col {
                ColData::Fixed(bytes) => {
                    let expected = n * schema.columns[ci].type_code.wire_stride();
                    if bytes.len() != expected {
                        return Err(format!(
                            "column {} Fixed byte length {} != expected {}",
                            ci, bytes.len(), expected
                        ));
                    }
                }
                ColData::Strings(v) => {
                    if v.len() != n {
                        return Err(format!(
                            "column {} Strings length {} != row count {}",
                            ci, v.len(), n
                        ));
                    }
                }
                ColData::U128s(v) => {
                    if v.len() != n {
                        return Err(format!(
                            "column {} U128s length {} != row count {}",
                            ci, v.len(), n
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Builder for appending rows to a `ZSetBatch` with schema-aware column mapping.
///
/// Columns are appended in non-PK order: the cursor automatically skips the PK
/// column index, so callers supply only payload values.
pub struct BatchAppender<'a> {
    batch:  &'a mut ZSetBatch,
    schema: &'a Schema,
    cursor: usize,
}

impl<'a> BatchAppender<'a> {
    pub fn new(batch: &'a mut ZSetBatch, schema: &'a Schema) -> Self {
        BatchAppender { batch, schema, cursor: 0 }
    }

    /// Start a new row with the given primary key and weight.
    pub fn add_row(&mut self, pk: u128, weight: i64) -> &mut Self {
        self.batch.pks.push_u128(pk);
        self.batch.weights.push(weight);
        self.batch.nulls.push(0);
        self.cursor = 0;
        self
    }

    /// Override the null mask for the current row (must be called after `add_row`).
    pub fn null_mask(&mut self, mask: u64) -> &mut Self {
        let last = self.batch.nulls.len() - 1;
        self.batch.nulls[last] = mask;
        self
    }

    /// Append a u64 value to the next Fixed column.
    pub fn u64_val(&mut self, v: u64) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => buf.extend_from_slice(&v.to_le_bytes()),
            _ => panic!("BatchAppender: u64_val called on non-Fixed column at schema index {}", ci),
        }
        self.cursor += 1;
        self
    }

    /// Append an i64 value to the next Fixed column.
    pub fn i64_val(&mut self, v: i64) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => buf.extend_from_slice(&v.to_le_bytes()),
            _ => panic!("BatchAppender: i64_val called on non-Fixed column at schema index {}", ci),
        }
        self.cursor += 1;
        self
    }

    /// Append a string value to the next Strings column.
    pub fn str_val(&mut self, s: &str) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Strings(v) => v.push(Some(s.to_string())),
            _ => panic!("BatchAppender: str_val called on non-Strings column at schema index {}", ci),
        }
        self.cursor += 1;
        self
    }

    /// Append a SQL NULL to the next Strings column.
    pub fn str_null(&mut self) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Strings(v) => v.push(None),
            _ => panic!("BatchAppender: str_null called on non-Strings column at schema index {}", ci),
        }
        self.cursor += 1;
        self
    }

    /// Append a u128 value (split as lo/hi) to the next U128s column.
    pub fn u128_val(&mut self, lo: u64, hi: u64) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::U128s(v) => v.push(((hi as u128) << 64) | lo as u128),
            _ => panic!("BatchAppender: u128_val called on non-U128s column at schema index {}", ci),
        }
        self.cursor += 1;
        self
    }

    /// Append a zero/empty value for the current column based on its schema type.
    pub fn zero_val(&mut self) -> &mut Self {
        let ci = self.col_index();
        let tc = self.schema.columns[ci].type_code;
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => {
                let stride = tc.wire_stride();
                buf.extend(std::iter::repeat(0u8).take(stride));
            }
            ColData::Strings(v) => {
                v.push(Some(std::string::String::new()));
            }
            ColData::U128s(v) => {
                v.push(0);
            }
        }
        self.cursor += 1;
        self
    }

    /// Map the payload cursor to the actual schema column index, skipping PK.
    fn col_index(&self) -> usize {
        if self.cursor < self.schema.pk_index {
            self.cursor
        } else {
            self.cursor + 1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_stride_string() {
        assert_eq!(TypeCode::String.wire_stride(), 16,
            "String wire stride must be 16 (German String struct: 4B len + 4B prefix + 8B ptr/inline)");
    }

    #[test]
    fn test_wire_stride_all() {
        assert_eq!(TypeCode::U8.wire_stride(),   1);
        assert_eq!(TypeCode::I8.wire_stride(),   1);
        assert_eq!(TypeCode::U16.wire_stride(),  2);
        assert_eq!(TypeCode::I16.wire_stride(),  2);
        assert_eq!(TypeCode::U32.wire_stride(),  4);
        assert_eq!(TypeCode::I32.wire_stride(),  4);
        assert_eq!(TypeCode::F32.wire_stride(),  4);
        assert_eq!(TypeCode::U64.wire_stride(),  8);
        assert_eq!(TypeCode::I64.wire_stride(),  8);
        assert_eq!(TypeCode::F64.wire_stride(),  8);
        assert_eq!(TypeCode::String.wire_stride(), 16);
        assert_eq!(TypeCode::U128.wire_stride(), 16);
    }

    // --- Step 1: Schema equality tests ---

    #[test]
    fn test_schema_eq() {
        let a = Schema {
            columns: vec![
                ColumnDef { name: "id".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "name".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_schema_ne_col_name() {
        let a = Schema {
            columns: vec![
                ColumnDef { name: "id".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let b = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn test_schema_ne_pk_index() {
        let a = Schema {
            columns: vec![
                ColumnDef { name: "a".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "b".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let b = Schema {
            columns: vec![
                ColumnDef { name: "a".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "b".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 1,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn test_schema_ne_type_code() {
        let a = Schema {
            columns: vec![
                ColumnDef { name: "x".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let b = Schema {
            columns: vec![
                ColumnDef { name: "x".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        assert_ne!(a, b);
    }

    // --- Step 2: validate() tests ---

    #[test]
    fn test_validate_empty_batch() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let batch = ZSetBatch::new(&schema);
        assert!(batch.validate(&schema).is_ok());
    }

    #[test]
    fn test_validate_valid_batch() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "name".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).i64_val(10).str_val("a");
            a.add_row(2u128, 1).i64_val(20).str_val("b");
            a.add_row(3u128, 1).i64_val(30).str_null();
        }
        assert!(batch.validate(&schema).is_ok());
    }

    #[test]
    fn test_validate_mismatched_weights() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        // weights is empty — mismatch
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("weights"));
    }

    #[test]
    fn test_validate_mismatched_strings() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        // Strings column is empty — mismatch
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("Strings"));
    }

    #[test]
    fn test_validate_mismatched_fixed() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        // Fixed column 1 is empty (needs 8 bytes) — mismatch
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("Fixed"));
    }

    #[test]
    fn test_validate_wrong_column_count() {
        let schema2 = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let schema1 = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let batch = ZSetBatch::new(&schema1);
        let err = batch.validate(&schema2).unwrap_err();
        assert!(err.contains("column count"));
    }

    // --- Step 3: BatchAppender tests ---

    #[test]
    fn test_appender_single_row() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "a".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "b".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(42u128, 1)
            .u64_val(100)
            .u64_val(200);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.pks.get(0), 42);
        assert_eq!(batch.weights[0], 1);
        if let ColData::Fixed(buf) = &batch.columns[1] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 100);
        } else { panic!("expected Fixed"); }
        if let ColData::Fixed(buf) = &batch.columns[2] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 200);
        } else { panic!("expected Fixed"); }
    }

    #[test]
    fn test_appender_multi_row() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).u64_val(10);
            a.add_row(2u128, 1).u64_val(20);
            a.add_row(3u128, -1).u64_val(30);
        }
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.pks, vec![1u128, 2u128, 3u128]);
        assert_eq!(batch.weights, vec![1, 1, -1]);
        if let ColData::Fixed(buf) = &batch.columns[1] {
            assert_eq!(buf.len(), 24);
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 10);
            assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 20);
            assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 30);
        } else { panic!("expected Fixed"); }
    }

    #[test]
    fn test_appender_string_col() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .u64_val(42)
            .str_val("hello");
        assert_eq!(batch.len(), 1);
        if let ColData::Strings(v) = &batch.columns[2] {
            assert_eq!(v[0], Some("hello".to_string()));
        } else { panic!("expected Strings"); }
    }

    #[test]
    fn test_appender_null_mask() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::U64, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .null_mask(0x02)
            .u64_val(0);
        assert_eq!(batch.nulls[0], 0x02);
    }

    #[test]
    fn test_appender_u128_col() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "big".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .u128_val(0xDEAD, 0xBEEF);
        if let ColData::U128s(v) = &batch.columns[1] {
            assert_eq!(v[0], ((0xBEEF_u128) << 64) | 0xDEAD);
        } else { panic!("expected U128s"); }
    }

    #[test]
    fn test_appender_mixed_types() {
        // pk(0) + U64 + String + I64 + String
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "a".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "b".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "c".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "d".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .u64_val(100)
            .str_val("hello")
            .i64_val(-5)
            .str_val("world");
        assert_eq!(batch.len(), 1);
        if let ColData::Fixed(buf) = &batch.columns[1] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 100);
        } else { panic!("expected Fixed for col 1"); }
        if let ColData::Strings(v) = &batch.columns[2] {
            assert_eq!(v[0], Some("hello".to_string()));
        } else { panic!("expected Strings for col 2"); }
        if let ColData::Fixed(buf) = &batch.columns[3] {
            assert_eq!(i64::from_le_bytes(buf[0..8].try_into().unwrap()), -5);
        } else { panic!("expected Fixed for col 3"); }
        if let ColData::Strings(v) = &batch.columns[4] {
            assert_eq!(v[0], Some("world".to_string()));
        } else { panic!("expected Strings for col 4"); }
    }

    #[test]
    fn test_appender_pk_not_at_zero() {
        // pk_index=2: columns [A(0), B(1), PK(2), C(3)]
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "a".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "b".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "c".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 2,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(99u128, 1)
            .u64_val(10)   // cursor 0 -> ci 0 (A)
            .u64_val(20)   // cursor 1 -> ci 1 (B)
            .u64_val(30);  // cursor 2 -> ci 3 (C), skips pk_index=2

        assert_eq!(batch.pks.get(0), 99);
        if let ColData::Fixed(buf) = &batch.columns[0] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 10);
        } else { panic!("expected Fixed for col 0"); }
        if let ColData::Fixed(buf) = &batch.columns[1] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 20);
        } else { panic!("expected Fixed for col 1"); }
        if let ColData::Fixed(buf) = &batch.columns[2] {
            assert!(buf.is_empty(), "PK column should be empty placeholder");
        }
        if let ColData::Fixed(buf) = &batch.columns[3] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 30);
        } else { panic!("expected Fixed for col 3"); }
    }

    // --- Step 4: Type-mismatch panics ---

    #[test]
    #[should_panic(expected = "u64_val called on non-Fixed")]
    fn test_appender_type_mismatch_u64_on_string() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).u64_val(42);
    }

    #[test]
    #[should_panic(expected = "i64_val called on non-Fixed")]
    fn test_appender_type_mismatch_i64_on_string() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).i64_val(-7);
    }

    #[test]
    #[should_panic(expected = "str_val called on non-Strings")]
    fn test_appender_type_mismatch_str_on_fixed() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).str_val("oops");
    }

    #[test]
    #[should_panic(expected = "str_null called on non-Strings")]
    fn test_appender_type_mismatch_str_null_on_fixed() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).str_null();
    }

    #[test]
    #[should_panic(expected = "u128_val called on non-U128s")]
    fn test_appender_type_mismatch_u128_on_fixed() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).u128_val(1, 0);
    }

    #[test]
    fn test_appender_then_validate() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).i64_val(10).str_val("hello");
            a.add_row(2u128, -1).i64_val(20).str_null();
        }
        assert!(batch.validate(&schema).is_ok());
    }
}
