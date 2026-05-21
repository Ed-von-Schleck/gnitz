use std::sync::OnceLock;
use super::error::ProtocolError;

pub use gnitz_wire::TypeCode;
pub use gnitz_wire::{MAX_PK_BYTES, MAX_PK_COLUMNS};

/// Convert a u64 wire value to TypeCode, returning an error for unknown codes.
/// Use at wire/network boundaries; internal data should use `TypeCode::from_validated_u8`.
pub fn type_code_from_u64(v: u64) -> Result<TypeCode, ProtocolError> {
    if v > u8::MAX as u64 {
        return Err(ProtocolError::UnknownTypeCode(v));
    }
    TypeCode::try_from_u8(v as u8).ok_or(ProtocolError::UnknownTypeCode(v))
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
    /// PK column indices in compound-key order; length >= 1.
    pub pk_cols:  Vec<usize>,
}

impl Schema {
    /// Number of logical columns in this schema (PK + payload).
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// All PK column indices, in compound-key order.
    #[inline]
    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_cols
    }

    /// Number of PK columns (compound-key arity). The wire/storage layer
    /// represents arity >= 2, but the SQL planner still rejects compound
    /// PRIMARY KEY, so every planner-constructed schema has count 1.
    #[inline]
    pub fn pk_count(&self) -> usize {
        self.pk_cols.len()
    }

    /// On-wire PK region stride: sum of each PK column's `wire_stride()`,
    /// tightly packed (no inter-column padding), mirroring the engine
    /// `SchemaDescriptor` layout. For a single-PK schema this equals the lone
    /// PK column's `wire_stride()`, so the PK region is byte-for-byte
    /// unchanged.
    #[inline]
    pub fn pk_stride(&self) -> usize {
        self.pk_cols.iter().map(|&ci| self.columns[ci].type_code.wire_stride()).sum()
    }

    /// The single PK column index. Use only at boundaries that have not yet
    /// been generalized (format encoders, catalog serialization, SQL parser
    /// path, wire/client BatchAppender). Hard-asserts length-1: a
    /// `debug_assert!` would compile out in release and let the silent
    /// truncation to the first PK column ship to production.
    #[inline]
    #[track_caller]
    pub fn pk_index_single(&self) -> usize {
        assert_eq!(self.pk_cols.len(), 1, "compound PK not yet supported here");
        self.pk_cols[0]
    }

    /// Number of non-PK ("payload") columns: `columns.len() - pk_count`.
    #[inline]
    pub fn num_payload_cols(&self) -> usize {
        self.num_columns() - self.pk_indices().len()
    }

    /// True iff column `ci` is the PK column.
    #[inline]
    pub fn is_pk_col(&self, ci: usize) -> bool {
        self.pk_indices().contains(&ci)
    }

    /// Byte offset of PK column `col_idx` within the packed PK region.
    /// Mirrors the engine `SchemaDescriptor::pk_byte_offset` helper.
    #[inline]
    pub fn pk_byte_offset(&self, col_idx: usize) -> usize {
        debug_assert!(self.is_pk_col(col_idx));
        self.pk_cols
            .iter()
            .take_while(|&&pi| pi != col_idx)
            .map(|&pi| self.columns[pi].type_code.wire_stride())
            .sum()
    }

    /// Map a logical column index to its dense payload index. Caller must
    /// ensure `col_idx` is not the PK column.
    #[inline]
    pub fn payload_idx(&self, col_idx: usize) -> usize {
        debug_assert!(!self.is_pk_col(col_idx), "payload_idx: col_idx must not be a PK column");
        col_idx - self.pk_cols.iter().filter(|&&p| p < col_idx).count()
    }

    /// Iterate over the non-PK ("payload") columns.
    ///
    /// Yields `(payload_idx, col_idx, &ColumnDef)`. The enumerated index is
    /// the dense payload index (null-bitmap bit position).
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, usize, &ColumnDef)> {
        let n = self.num_columns();
        (0..n)
            .filter(move |ci| !self.is_pk_col(*ci))
            .enumerate()
            .map(move |(pi, ci)| (pi, ci, &self.columns[ci]))
    }

    /// True iff `cols` is a permutation of `pk_indices()`. Mirrors
    /// `engine::SchemaDescriptor::group_cols_eq_pk`; planner and engine must
    /// agree on this predicate or the reduce output schema will not match
    /// the circuit's actual output.
    #[inline]
    pub fn group_cols_eq_pk(&self, cols: &[usize]) -> bool {
        cols.len() == self.pk_cols.len() && self.pk_cols.iter().all(|p| cols.contains(p))
    }

    /// True iff `cols` is a single non-nullable column whose type permits
    /// using the source column directly as the reduce output PK (vs. a
    /// synthetic U128). Mirrors `engine::ops::is_single_col_natural_pk`;
    /// narrow signed/unsigned and floats stay on the synthetic path.
    #[inline]
    pub fn is_single_col_natural_pk(&self, cols: &[usize]) -> bool {
        cols.len() == 1
            && matches!(
                self.columns[cols[0]].type_code,
                TypeCode::U64 | TypeCode::U128 | TypeCode::UUID,
            )
            && !self.columns[cols[0]].is_nullable
    }
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
        pk_cols: vec![0],
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PkColumn {
    U64s(Vec<u64>),
    U128s(Vec<u128>),
    /// Wide compound PK (pk_count >= 2): a flat `stride`-byte-per-row buffer
    /// in on-wire LE layout. Has no scalar u128 projection, so the numeric
    /// accessors panic. These panics are a real precondition (not dead code):
    /// the numeric path is reachable from SQL/binding callers, but the SQL
    /// planner still rejects compound PRIMARY KEY, so no such batch reaches
    /// them today.
    Bytes { stride: u8, buf: Vec<u8> },
}

impl PkColumn {
    pub fn for_type(tc: TypeCode) -> Self {
        if tc == TypeCode::U128 || tc == TypeCode::UUID { PkColumn::U128s(vec![]) } else { PkColumn::U64s(vec![]) }
    }
    pub fn len(&self) -> usize {
        match self {
            PkColumn::U64s(v) => v.len(),
            PkColumn::U128s(v) => v.len(),
            PkColumn::Bytes { stride, buf } => buf.len() / *stride as usize,
        }
    }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn get(&self, i: usize) -> u128 {
        match self {
            PkColumn::U64s(v) => v[i] as u128,
            PkColumn::U128s(v) => v[i],
            PkColumn::Bytes { .. } => panic!("wide PK has no u128 projection"),
        }
    }
    /// Append one raw PK tuple (already in on-wire LE layout). Wide-PK only.
    pub fn push_bytes(&mut self, b: &[u8]) {
        match self {
            PkColumn::Bytes { stride, buf } => {
                debug_assert_eq!(b.len(), *stride as usize);
                buf.extend_from_slice(b);
            }
            _ => unreachable!("push_bytes on non-wide PK column"),
        }
    }
    /// Borrow the raw `stride`-byte tuple at row `i`. Wide-PK only.
    pub fn get_bytes(&self, i: usize) -> &[u8] {
        match self {
            PkColumn::Bytes { stride, buf } => {
                let s = *stride as usize;
                &buf[i * s..(i + 1) * s]
            }
            _ => unreachable!("get_bytes on non-wide PK column"),
        }
    }
    pub fn push_u128(&mut self, pk: u128) {
        match self {
            PkColumn::U64s(v) => v.push(pk as u64),
            PkColumn::U128s(v) => v.push(pk),
            PkColumn::Bytes { .. } => panic!("wide PK has no u128 projection"),
        }
    }
    pub fn set_u128(&mut self, i: usize, pk: u128) {
        match self {
            PkColumn::U64s(v) => v[i] = pk as u64,
            PkColumn::U128s(v) => v[i] = pk,
            PkColumn::Bytes { .. } => panic!("wide PK has no u128 projection"),
        }
    }
    pub fn swap(&mut self, i: usize, j: usize) {
        match self {
            PkColumn::U64s(v) => v.swap(i, j),
            PkColumn::U128s(v) => v.swap(i, j),
            PkColumn::Bytes { stride, buf } => {
                if i == j { return; }
                let s = *stride as usize;
                let (lo, hi) = if i < j { (i, j) } else { (j, i) };
                let (left, right) = buf.split_at_mut(hi * s);
                left[lo * s..lo * s + s].swap_with_slice(&mut right[..s]);
            }
        }
    }
    pub fn clear(&mut self) {
        match self {
            PkColumn::U64s(v) => v.clear(),
            PkColumn::U128s(v) => v.clear(),
            PkColumn::Bytes { buf, .. } => buf.clear(),
        }
    }
    pub fn truncate(&mut self, len: usize) {
        match self {
            PkColumn::U64s(v) => v.truncate(len),
            PkColumn::U128s(v) => v.truncate(len),
            PkColumn::Bytes { stride, buf } => buf.truncate(len * *stride as usize),
        }
    }
    pub fn to_vec_u128(&self) -> Vec<u128> {
        match self {
            PkColumn::U64s(v) => v.iter().map(|&x| x as u128).collect(),
            PkColumn::U128s(v) => v.clone(),
            PkColumn::Bytes { .. } => panic!("wide PK has no u128 projection"),
        }
    }

    /// Empty `PkColumn` matching `schema`'s PK layout. Single source of
    /// truth for the variant choice; `ZSetBatch::new`, `GnitzClient::delete`,
    /// and the SQL DML helpers all route through this.
    pub fn empty_for_schema(schema: &Schema) -> Self {
        if schema.pk_count() >= 2 {
            PkColumn::Bytes { stride: schema.pk_stride() as u8, buf: vec![] }
        } else {
            PkColumn::for_type(schema.columns[schema.pk_indices()[0]].type_code)
        }
    }

    /// Read row `i` into a `PkTuple`.
    pub fn get_tuple(&self, i: usize, stride: u8) -> PkTuple {
        let mut t = PkTuple::new(stride);
        match self {
            PkColumn::U64s(v) => t.buf[..8].copy_from_slice(&v[i].to_le_bytes()),
            PkColumn::U128s(v) => t.buf[..16].copy_from_slice(&v[i].to_le_bytes()),
            PkColumn::Bytes { stride: s, buf } => {
                let s = *s as usize;
                debug_assert_eq!(s, stride as usize);
                t.buf[..s].copy_from_slice(&buf[i * s..(i + 1) * s]);
            }
        }
        t
    }

    /// Append the row at `src[i]` to `self`. Variants must match.
    pub fn push_from(&mut self, src: &PkColumn, i: usize) {
        match (self, src) {
            (PkColumn::U64s(d), PkColumn::U64s(s)) => d.push(s[i]),
            (PkColumn::U128s(d), PkColumn::U128s(s)) => d.push(s[i]),
            (
                PkColumn::Bytes { stride: sd, buf: d },
                PkColumn::Bytes { stride: ss, buf: s },
            ) => {
                debug_assert_eq!(*sd, *ss);
                let w = *sd as usize;
                d.extend_from_slice(&s[i * w..(i + 1) * w]);
            }
            _ => unreachable!("push_from: pk variant mismatch"),
        }
    }

    /// Append `pk`'s bytes to `self`, dispatching on variant. Hides the
    /// `Bytes`-vs-scalar choice from DML callers.
    pub fn push_tuple(&mut self, pk: &PkTuple) {
        let s = pk.stride as usize;
        match self {
            PkColumn::U64s(v) => {
                debug_assert!(s <= 8);
                let mut b = [0u8; 8];
                b[..s].copy_from_slice(&pk.buf[..s]);
                v.push(u64::from_le_bytes(b));
            }
            PkColumn::U128s(v) => {
                debug_assert!(s <= 16);
                let mut b = [0u8; 16];
                b[..s].copy_from_slice(&pk.buf[..s]);
                v.push(u128::from_le_bytes(b));
            }
            PkColumn::Bytes { stride, buf } => {
                debug_assert_eq!(*stride as usize, s);
                buf.extend_from_slice(&pk.buf[..s]);
            }
        }
    }

    /// One-row column matching `schema`'s PK layout, initialized with
    /// `pk`'s bytes.
    pub fn one_row(schema: &Schema, pk: &PkTuple) -> Self {
        let mut col = Self::empty_for_schema(schema);
        col.push_tuple(pk);
        col
    }
}

/// SQL→client carrier for one row's PK. Carries `(stride, bytes)` so callers
/// above the wire codec do not need to handle the `(seek_pk: u128 +
/// seek_pk_extra: BLOB)` wire-level split.
#[derive(Clone, Copy)]
pub struct PkTuple {
    pub stride: u8,
    pub buf:    [u8; MAX_PK_BYTES],
}

impl PkTuple {
    pub fn new(stride: u8) -> Self {
        debug_assert!(stride as usize <= MAX_PK_BYTES);
        Self { stride, buf: [0u8; MAX_PK_BYTES] }
    }

    /// Construct from a u128 whose low `stride` bytes carry the column's
    /// native LE bytes (as produced by `parse_pk_literal_packed`). Copies
    /// only `stride` bytes so callers cannot pollute the high padding.
    pub fn from_u128(stride: u8, v: u128) -> Self {
        debug_assert!(stride as usize <= 16);
        let s = stride as usize;
        let mut t = Self::new(stride);
        t.buf[..s].copy_from_slice(&v.to_le_bytes()[..s]);
        t
    }

    /// FFI-boundary constructor: build a tuple from a u128 with the full
    /// 16-byte narrow stride, without a schema lookup. The server reads
    /// only the column's actual stride; the high padding bytes (if any)
    /// are inert. Used by C and Python seek shims.
    pub fn from_u128_narrow(v: u128) -> Self {
        Self::from_u128(16, v)
    }

    /// On-wire PK region bytes 0..stride.
    pub fn as_bytes(&self) -> &[u8] { &self.buf[..self.stride as usize] }

    /// Reconstruct a u128 from the low `stride` bytes. Wide PKs (stride > 16)
    /// have no scalar projection; debug-asserts. Used at the narrow-PK
    /// fast path that still routes through `PkColumn::push_u128`.
    pub fn to_u128_narrow(&self) -> u128 {
        debug_assert!(self.stride as usize <= 16);
        let mut b = [0u8; 16];
        b[..self.stride as usize].copy_from_slice(self.as_bytes());
        u128::from_le_bytes(b)
    }

    /// Split the tuple into the wire form `(seek_pk: u128, seek_pk_extra: &[u8])`.
    /// For narrow PKs (stride ≤ 16) `extra` is empty and the frame is byte-
    /// identical to the pre-compound-PK path.
    pub fn split_wire(&self) -> (u128, &[u8]) {
        let stride = self.stride as usize;
        let n = stride.min(16);
        let mut lo = [0u8; 16];
        lo[..n].copy_from_slice(&self.buf[..n]);
        let low_16 = u128::from_le_bytes(lo);
        let extra: &[u8] = if stride > 16 { &self.buf[16..stride] } else { &[] };
        (low_16, extra)
    }
}

impl PartialEq for PkTuple {
    fn eq(&self, other: &Self) -> bool {
        self.stride == other.stride && self.as_bytes() == other.as_bytes()
    }
}
impl Eq for PkTuple {}

impl std::hash::Hash for PkTuple {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.stride.hash(state);
        self.as_bytes().hash(state);
    }
}

impl std::fmt::Debug for PkTuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PkTuple")
            .field("stride", &self.stride)
            .field("bytes", &self.as_bytes())
            .finish()
    }
}

impl PartialEq<Vec<u128>> for PkColumn {
    fn eq(&self, other: &Vec<u128>) -> bool {
        if self.len() != other.len() { return false; }
        match self {
            PkColumn::U64s(v) => v.iter().zip(other).all(|(&a, &b)| a as u128 == b),
            PkColumn::U128s(v) => v.as_slice() == other.as_slice(),
            PkColumn::Bytes { .. } => panic!("cannot compare wide PK against Vec<u128>"),
        }
    }
}

/// Per-column payload data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColData {
    /// Raw little-endian bytes; length = count * wire_stride (for all non-String, non-U128, non-Blob).
    Fixed(Vec<u8>),
    Strings(Vec<Option<std::string::String>>),
    /// Variable-length raw byte payloads. Same on-wire encoding as `Strings`
    /// (16-byte German-string struct + blob arena spill) but the bytes are
    /// not constrained to be valid UTF-8.
    Bytes(Vec<Option<Vec<u8>>>),
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
            if schema.is_pk_col(ci) {
                ColData::Fixed(vec![])
            } else {
                match col.type_code {
                    TypeCode::String => ColData::Strings(vec![]),
                    TypeCode::Blob => ColData::Bytes(vec![]),
                    TypeCode::U128 | TypeCode::UUID => ColData::U128s(vec![]),
                    _                => ColData::Fixed(vec![]),
                }
            }
        }).collect();
        ZSetBatch {
            pks: PkColumn::empty_for_schema(schema),
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
            (PkColumn::Bytes { stride: sa, buf: a }, PkColumn::Bytes { stride: sb, buf: b }) => {
                assert_eq!(sa, sb, "extend_from: wide PK stride mismatch");
                a.extend_from_slice(b);
            }
            _ => panic!("extend_from: pk column type mismatch"),
        }
        self.weights.extend_from_slice(&other.weights);
        self.nulls.extend_from_slice(&other.nulls);
        for (a, b) in self.columns.iter_mut().zip(other.columns.iter()) {
            match (a, b) {
                (ColData::Fixed(a), ColData::Fixed(b)) => a.extend_from_slice(b),
                (ColData::Strings(a), ColData::Strings(b)) => a.extend(b.iter().cloned()),
                (ColData::Bytes(a), ColData::Bytes(b)) => a.extend(b.iter().cloned()),
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
        if self.columns.len() != schema.num_columns() {
            return Err(format!(
                "column count {} != schema column count {}",
                self.columns.len(), schema.num_columns()
            ));
        }
        for (_pi, ci, col_def) in schema.payload_columns() {
            let col = &self.columns[ci];
            match col {
                ColData::Fixed(bytes) => {
                    let expected = n * col_def.type_code.wire_stride();
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
                ColData::Bytes(v) => {
                    if v.len() != n {
                        return Err(format!(
                            "column {} Bytes length {} != row count {}",
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

    /// Append a raw byte slice to the next Bytes (BLOB) column.
    pub fn bytes_val(&mut self, b: &[u8]) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Bytes(v) => v.push(Some(b.to_vec())),
            _ => panic!("BatchAppender: bytes_val called on non-Bytes column at schema index {}", ci),
        }
        self.cursor += 1;
        self
    }

    /// Append a SQL NULL to the next Bytes (BLOB) column.
    pub fn bytes_null(&mut self) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Bytes(v) => v.push(None),
            _ => panic!("BatchAppender: bytes_null called on non-Bytes column at schema index {}", ci),
        }
        self.cursor += 1;
        self
    }

    /// Append a u128 value to the next U128s column.
    pub fn u128_val(&mut self, v: u128) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::U128s(vec) => vec.push(v),
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
                buf.extend(std::iter::repeat_n(0u8, stride));
            }
            ColData::Strings(v) => {
                v.push(Some(std::string::String::new()));
            }
            ColData::Bytes(v) => {
                v.push(Some(Vec::new()));
            }
            ColData::U128s(v) => {
                v.push(0);
            }
        }
        self.cursor += 1;
        self
    }

    /// Map the payload cursor to the actual schema column index, skipping PK.
    /// Single-PK boundary: BatchAppender's wire/client API is single-PK and
    /// stays so until the client surface is widened.
    fn col_index(&self) -> usize {
        let pk = self.schema.pk_index_single();
        if self.cursor < pk {
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
    fn test_num_payload_cols() {
        // 2-column schema → 1 payload column.
        let s = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(),  type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
        };
        assert_eq!(s.num_payload_cols(), 1);

        // pk_index not at column 0 → same answer (columns.len() - pk_indices().len()).
        let s = Schema {
            columns: vec![
                ColumnDef { name: "a".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "b".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "c".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![2],
        };
        assert_eq!(s.num_payload_cols(), 3);
    }

    #[test]
    fn test_num_columns() {
        let s = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
        };
        assert_eq!(s.num_columns(), 1);

        let s = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(),  type_code: TypeCode::I64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s".into(),  type_code: TypeCode::String, is_nullable: true,  fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
        };
        assert_eq!(s.num_columns(), 3);
    }

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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
        };
        let b = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
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
            pk_cols: vec![0],
        };
        let b = Schema {
            columns: vec![
                ColumnDef { name: "a".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "b".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![1],
        };
        assert_ne!(a, b);
    }

    #[test]
    fn test_schema_ne_type_code() {
        let a = Schema {
            columns: vec![
                ColumnDef { name: "x".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
        };
        let b = Schema {
            columns: vec![
                ColumnDef { name: "x".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
        };
        let schema1 = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .u128_val(((0xBEEF_u128) << 64) | 0xDEAD);
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
            pk_cols: vec![0],
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
            pk_cols: vec![2],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
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
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).u128_val(1);
    }

    #[test]
    fn test_appender_then_validate() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_cols: vec![0],
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
