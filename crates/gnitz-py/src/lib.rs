use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use lru::LruCache;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyDict, PyString, PyTuple};

use gnitz_core::{CircuitBuilder, ExprBuilder, ExprProgram, Circuit, GnitzClient};
use gnitz_core::{ColData, ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch, MAX_COLUMNS, MAX_PK_BYTES};
use gnitz_core::protocol::types::type_code_from_u64;
use gnitz_sql::{SqlPlanner, SqlResult};

// Shared aliases for the recurring compound types threaded through the async IO
// loop and the response decoders, kept in one place so the signatures stay
// readable (and clippy::type_complexity stays quiet).
type SchemaCache = Arc<Mutex<LruCache<u64, (Arc<Schema>, u16)>>>;
type ClientResponse =
    Result<(Option<Arc<Schema>>, Option<ZSetBatch>, u64), gnitz_core::ClientError>;
type ScanReadResult =
    Result<Result<(Option<Arc<Schema>>, u16, Option<ZSetBatch>, u64), String>, String>;

// ---------------------------------------------------------------------------
// GnitzError Python exception
// ---------------------------------------------------------------------------

pyo3::create_exception!(_native, GnitzError, pyo3::exceptions::PyException);

/// Map any `Display` error into a `GnitzError` PyErr. The orphan rule blocks
/// `impl From<ClientError> for PyErr`, so this free helper carries the
/// recurring `.map_err(|e| GnitzError::new_err(e.to_string()))` once. It is
/// GnitzError-specific by design — `PyValueError`/`PyTypeError` sites keep
/// their explicit construction.
fn to_py_err<T, E: std::fmt::Display>(res: Result<T, E>) -> PyResult<T> {
    res.map_err(|e| GnitzError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// TypeCode — Python class whose class attributes mirror gnitz_client.TypeCode
// ---------------------------------------------------------------------------

#[pyclass(name = "TypeCode")]
pub struct PyTypeCode;

#[pymethods]
impl PyTypeCode {
    #[classattr] pub const U8:     u32 = 1;
    #[classattr] pub const I8:     u32 = 2;
    #[classattr] pub const U16:    u32 = 3;
    #[classattr] pub const I16:    u32 = 4;
    #[classattr] pub const U32:    u32 = 5;
    #[classattr] pub const I32:    u32 = 6;
    #[classattr] pub const F32:    u32 = 7;
    #[classattr] pub const U64:    u32 = 8;
    #[classattr] pub const I64:    u32 = 9;
    #[classattr] pub const F64:    u32 = 10;
    #[classattr] pub const STRING: u32 = 11;
    #[classattr] pub const U128:   u32 = 12;
    #[classattr] pub const UUID:   u32 = 13;
    #[classattr] pub const BLOB:   u32 = 14;
}

// ---------------------------------------------------------------------------
// ColumnDef
// ---------------------------------------------------------------------------

#[pyclass(name = "ColumnDef", get_all, set_all)]
pub struct PyColumnDef {
    pub name:        String,
    pub type_code:   u32,
    pub is_nullable: bool,
    pub primary_key: bool,
}

#[pymethods]
impl PyColumnDef {
    #[new]
    #[pyo3(signature = (name, type_code, is_nullable = false, primary_key = false))]
    pub fn new(name: String, type_code: u32, is_nullable: bool, primary_key: bool) -> Self {
        PyColumnDef { name, type_code, is_nullable, primary_key }
    }

    pub fn __repr__(&self) -> String {
        format!("ColumnDef(name={:?}, type_code={}, is_nullable={}, primary_key={})",
                self.name, self.type_code, self.is_nullable, self.primary_key)
    }
}

fn py_col_to_rust(c: &PyColumnDef) -> PyResult<ColumnDef> {
    type_code_from_u64(c.type_code as u64)
        .map(|tc| ColumnDef { name: c.name.clone(), type_code: tc, is_nullable: c.is_nullable, fk_table_id: 0, fk_col_idx: 0 })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn rust_col_to_py(py: Python<'_>, c: &ColumnDef, primary_key: bool) -> PyResult<PyObject> {
    Ok(Py::new(py, PyColumnDef {
        name:        c.name.clone(),
        type_code:   c.type_code as u32,
        is_nullable: c.is_nullable,
        primary_key,
    })?.into_any())
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Stores columns as a Python list of PyColumnDef so Python can access
/// schema.columns[i].type_code etc. without any extra copies.
///
/// `pk_indices` holds the compound-key column indices in **sort order** — e.g.
/// `pk_indices=[2, 1]` sorts by col 2 first, then col 1. Order matters for
/// seek/range semantics.
#[pyclass(name = "Schema")]
pub struct PySchema {
    pub(crate) columns:    Py<PyList>,
    pub(crate) pk_indices: Vec<usize>,
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (columns, pk_index = None, pk_indices = None))]
    pub fn new(
        columns:    Bound<'_, PyList>,
        pk_index:   Option<usize>,
        pk_indices: Option<Vec<usize>>,
    ) -> PyResult<Self> {
        let ncols = columns.len();
        if ncols == 0 || ncols > MAX_COLUMNS {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Schema must have 1 to {MAX_COLUMNS} columns"
            )));
        }
        let list = match (pk_index, pk_indices) {
            (Some(_), Some(_)) => return Err(pyo3::exceptions::PyValueError::new_err(
                "pass either pk_index or pk_indices, not both")),
            (Some(i), None) => vec![i],
            (None, Some(v)) => v,
            (None, None) => {
                let mut pks = Vec::new();
                for (i, item) in columns.iter().enumerate() {
                    let c: PyRef<'_, PyColumnDef> = item.extract()?;
                    if c.primary_key { pks.push(i); }
                }
                if pks.is_empty() { vec![0] } else { pks }
            }
        };
        Schema::validate_pk_cols(&list, ncols)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        // Reject nullable PK columns; reject String/Blob PK types; validate total stride.
        let mut total_stride: usize = 0;
        for &ci in &list {
            let item = columns.get_item(ci)?;
            let col: PyRef<'_, PyColumnDef> = item.extract()?;
            if col.is_nullable {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "column {:?} is nullable, which is not allowed for PK columns", col.name
                )));
            }
            let tc = type_code_from_u64(col.type_code as u64)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            if matches!(tc, TypeCode::String | TypeCode::Blob) {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "column {:?} has type {:?} which cannot be a PK column", col.name, tc)));
            }
            total_stride += tc.wire_stride();
        }
        if total_stride > MAX_PK_BYTES {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "total PK stride {total_stride} exceeds maximum {MAX_PK_BYTES} bytes"
            )));
        }
        // ZSetBatch.nulls stores one u64 per row; null bits map to payload column indices.
        let payload_count = ncols - list.len();
        if payload_count > 64 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "schema has {payload_count} payload columns; null bitmap supports at most 64"
            )));
        }
        Ok(PySchema { columns: columns.unbind(), pk_indices: list })
    }

    #[getter]
    pub fn columns<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> {
        self.columns.bind(py).clone()
    }

    #[getter]
    pub fn pk_indices(&self) -> Vec<usize> { self.pk_indices.clone() }

    /// Single-PK convenience. Raises ValueError on compound schemas.
    #[getter]
    pub fn pk_index(&self) -> PyResult<usize> {
        if self.pk_indices.len() > 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Schema has compound PK; use pk_indices instead of pk_index"
            ));
        }
        Ok(self.pk_indices[0])
    }

    pub fn __repr__(&self, py: Python<'_>) -> String {
        format!("Schema(pk_indices={:?}, ncols={})",
            self.pk_indices, self.columns.bind(py).len())
    }
}

fn py_schema_to_rust(py: Python<'_>, s: &PySchema) -> PyResult<Schema> {
    let list = s.columns.bind(py);
    let mut cols = Vec::with_capacity(list.len());
    for item in list.iter() {
        let c: PyRef<'_, PyColumnDef> = item.extract()?;
        cols.push(py_col_to_rust(&c)?);
    }
    Ok(Schema { columns: cols, pk_cols: s.pk_indices.clone() })
}

fn rust_schema_to_py(py: Python<'_>, s: &Schema) -> PyResult<Py<PySchema>> {
    let py_cols: Vec<PyObject> = s.columns.iter().enumerate()
        .map(|(i, c)| rust_col_to_py(py, c, s.is_pk_col(i)))
        .collect::<PyResult<_>>()?;
    let list = PyList::new(py, py_cols)?;
    Py::new(py, PySchema { columns: list.unbind(), pk_indices: s.pk_cols.clone() })
}

// ---------------------------------------------------------------------------
// Row — Rust-native row object (replaces the former pure-Python Row class)
// ---------------------------------------------------------------------------

#[pyclass(name = "Row")]
pub struct PyRow {
    fields: Py<PyTuple>,
    values: Py<PyTuple>,
    weight: i64,
    field_index: Arc<HashMap<String, usize>>,
}

#[pymethods]
impl PyRow {
    #[new]
    #[pyo3(signature = (fields, values, weight=1))]
    pub fn new(_py: Python<'_>, fields: Bound<'_, PyTuple>, values: Bound<'_, PyTuple>, weight: i64) -> PyResult<Self> {
        let mut map = HashMap::with_capacity(fields.len());
        for i in 0..fields.len() {
            let name: String = fields.get_item(i)?.extract()?;
            map.insert(name, i);
        }
        Ok(PyRow {
            fields: fields.unbind(),
            values: values.unbind(),
            weight,
            field_index: Arc::new(map),
        })
    }

    #[getter]
    pub fn weight(&self) -> i64 { self.weight }

    pub fn __getattr__(&self, py: Python<'_>, name: &str) -> PyResult<PyObject> {
        if let Some(&i) = self.field_index.get(name) {
            return Ok(self.values.bind(py).get_item(i)?.unbind());
        }
        Err(pyo3::exceptions::PyAttributeError::new_err(
            format!("Row has no field {name:?}"),
        ))
    }

    pub fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let values = self.values.bind(py);
        if let Ok(idx) = key.extract::<isize>() {
            let len = values.len() as isize;
            let idx = if idx < 0 { idx + len } else { idx };
            if idx < 0 || idx >= len {
                return Err(pyo3::exceptions::PyIndexError::new_err("index out of range"));
            }
            return Ok(values.get_item(idx as usize)?.unbind());
        }
        if let Ok(name) = key.extract::<&str>() {
            if let Some(&i) = self.field_index.get(name) {
                return Ok(values.get_item(i)?.unbind());
            }
            return Err(pyo3::exceptions::PyKeyError::new_err(name.to_string()));
        }
        Err(pyo3::exceptions::PyTypeError::new_err("key must be int or str"))
    }

    pub fn __iter__(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.values.bind(py).as_any().call_method0("__iter__")?.unbind())
    }

    pub fn __len__(&self, py: Python<'_>) -> usize {
        self.values.bind(py).len()
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let fields = self.fields.bind(py);
        let values = self.values.bind(py);
        let mut parts = Vec::with_capacity(fields.len());
        for i in 0..fields.len() {
            let field_obj = fields.get_item(i)?;
            let f: &str = field_obj.extract()?;
            let v = values.get_item(i)?;
            parts.push(format!("{}={}", f, v.repr()?));
        }
        Ok(format!("Row({}, weight={})", parts.join(", "), self.weight))
    }

    pub fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        if let Ok(other_row) = other.downcast::<PyRow>() {
            let other_ref = other_row.borrow();
            let eq: bool = self.values.bind(py).eq(other_ref.values.bind(py))?;
            Ok(eq.into_pyobject(py)?.to_owned().into_any().unbind())
        } else {
            Ok(py.NotImplemented().into_any())
        }
    }

    pub fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        self.values.bind(py).hash()
    }

    pub fn _asdict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let fields = self.fields.bind(py);
        let values = self.values.bind(py);
        let dict = PyDict::new(py);
        for i in 0..fields.len() {
            dict.set_item(fields.get_item(i)?, values.get_item(i)?)?;
        }
        Ok(dict.unbind())
    }

    pub fn _tuple(&self, py: Python<'_>) -> Py<PyTuple> {
        self.values.clone_ref(py)
    }
}

// ---------------------------------------------------------------------------
// ZSetBatch — Rust-native storage (write path)
// ---------------------------------------------------------------------------

/// Stores batch data in Rust Vecs with a cached Schema.
/// `append_dict()` / `extend_from_dicts()` handle all type extraction,
/// null tracking, and PK handling in Rust. `push()` uses the cached schema
/// and batch without any Python→Rust re-extraction.
#[pyclass(name = "ZSetBatch")]
pub struct PyZSetBatch {
    pub(crate) schema:   Arc<Schema>,
    pub(crate) batch:    ZSetBatch,
    /// Interned PyString for each column name — built once, reused across all appends.
    col_keys:     Vec<Py<PyString>>,
    /// Cached (payload_idx, col_idx) for non-PK columns; built once at construction.
    payload_cols: Vec<(usize, usize)>,
}

/// Private helpers — shared between append_row, append_dict, extend_from_dicts.
impl PyZSetBatch {
    /// Extract one PK column's bytes from a Python value into a PkTuple slice.
    fn write_pk_col_into(
        &self, t: &mut gnitz_core::PkTuple, ci: usize, val: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if val.is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "PK column {:?} cannot be None", self.schema.columns[ci].name
            )));
        }
        let tc  = self.schema.columns[ci].type_code;
        let off = self.schema.pk_byte_offset(ci);
        let s   = tc.wire_stride();
        match tc {
            TypeCode::U128 | TypeCode::UUID => {
                let v = extract_uuid_or_u128(val)?;
                t.buf[off..off + 16].copy_from_slice(&v.to_le_bytes());
            }
            TypeCode::I128 => {
                let v = val.extract::<i128>()? as u128;
                t.buf[off..off + 16].copy_from_slice(&v.to_le_bytes());
            }
            _ => write_fixed_le_into(&mut t.buf[off..off + s], tc, val)?,
        }
        Ok(())
    }

    fn append_pk_from_dict(
        &mut self, py: Python<'_>, dict: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let stride = self.schema.pk_stride() as u8;
        let mut t = gnitz_core::PkTuple::new(stride);
        for &ci in self.schema.pk_indices() {
            let val = dict.get_item(self.col_keys[ci].bind(py))?
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!(
                    "missing PK column {:?}", self.schema.columns[ci].name
                )))?;
            self.write_pk_col_into(&mut t, ci, &val)?;
        }
        self.batch.pks.push_tuple(&t);
        Ok(())
    }

    fn append_pk_from_list(
        &mut self, values: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let stride = self.schema.pk_stride() as u8;
        let mut t = gnitz_core::PkTuple::new(stride);
        for &ci in self.schema.pk_indices() {
            let val = values.get_item(ci)?;
            self.write_pk_col_into(&mut t, ci, &val)?;
        }
        self.batch.pks.push_tuple(&t);
        Ok(())
    }

    /// Truncate all per-row vectors back to `n` rows so a partially written
    /// row (e.g. type error on the third payload column) does not leave the
    /// batch with mismatched column lengths.
    fn rollback_to(&mut self, n: usize) {
        self.batch.truncate(n, self.schema.as_ref());
    }

    fn append_column_value(
        &mut self, ci: usize, payload_idx: usize,
        val: &Bound<'_, PyAny>, nulls: &mut u64,
    ) -> PyResult<()> {
        if val.is_none() {
            return self.append_null_column(ci, payload_idx, nulls);
        }
        match self.schema.columns[ci].type_code {
            TypeCode::String => {
                if let ColData::Strings(v) = &mut self.batch.columns[ci] {
                    v.push(Some(val.extract::<String>()?));
                }
            }
            TypeCode::Blob => {
                if let ColData::Bytes(v) = &mut self.batch.columns[ci] {
                    v.push(Some(val.extract::<Vec<u8>>()?));
                }
            }
            TypeCode::U128 | TypeCode::UUID => {
                if let ColData::U128s(v) = &mut self.batch.columns[ci] {
                    v.push(extract_uuid_or_u128(val)?);
                }
            }
            TypeCode::I128 => {
                if let ColData::U128s(v) = &mut self.batch.columns[ci] {
                    v.push(val.extract::<i128>()? as u128);
                }
            }
            tc => {
                if let ColData::Fixed(buf) = &mut self.batch.columns[ci] {
                    write_fixed_le(buf, tc, val)?;
                }
            }
        }
        Ok(())
    }

    fn append_null_column(
        &mut self, ci: usize, payload_idx: usize, nulls: &mut u64,
    ) -> PyResult<()> {
        if !self.schema.columns[ci].is_nullable {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Non-nullable column {:?} cannot be None",
                self.schema.columns[ci].name,
            )));
        }
        *nulls |= 1u64 << payload_idx;
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => {
                let stride = self.schema.columns[ci].type_code.wire_stride();
                buf.extend(std::iter::repeat_n(0u8, stride));
            }
            ColData::Strings(v) => v.push(None),
            ColData::Bytes(v) => v.push(None),
            ColData::U128s(v) => v.push(0),
        }
        Ok(())
    }

    /// Run `body` against `self`; on error, roll the batch back to its
    /// pre-call row count so a partial write never escapes. Used both per-row
    /// (the single appends) and per-call (`extend_from_dicts` wraps its whole
    /// loop), so every append path shares one all-or-nothing contract. Nesting
    /// is safe: `rollback_to` only truncates, so an inner per-row rollback
    /// followed by the outer batch-level rollback is idempotent.
    fn with_rollback<F>(&mut self, body: F) -> PyResult<()>
    where F: FnOnce(&mut Self) -> PyResult<()> {
        let n = self.batch.len();
        match body(self) {
            Ok(()) => Ok(()),
            Err(e) => { self.rollback_to(n); Err(e) }
        }
    }

    fn append_from_dict_inner(
        &mut self,
        dict: &Bound<'_, PyDict>,
        weight: i64,
    ) -> PyResult<()> {
        self.with_rollback(|s| {
            let py = dict.py();
            s.append_pk_from_dict(py, dict)?;
            s.batch.weights.push(weight);
            let mut nulls: u64 = 0;
            for i in 0..s.payload_cols.len() {
                let (payload_idx, ci) = s.payload_cols[i];
                match dict.get_item(s.col_keys[ci].bind(py))? {
                    Some(val) => s.append_column_value(ci, payload_idx, &val, &mut nulls)?,
                    None      => s.append_null_column(ci, payload_idx, &mut nulls)?,
                }
            }
            s.batch.nulls.push(nulls);
            Ok(())
        })
    }

    fn append_from_list_inner(
        &mut self,
        values: &Bound<'_, PyList>,
        weight: i64,
    ) -> PyResult<()> {
        self.with_rollback(|s| {
            s.append_pk_from_list(values)?;
            s.batch.weights.push(weight);
            let mut nulls: u64 = 0;
            for i in 0..s.payload_cols.len() {
                let (payload_idx, ci) = s.payload_cols[i];
                let val = values.get_item(ci)?;
                s.append_column_value(ci, payload_idx, &val, &mut nulls)?;
            }
            s.batch.nulls.push(nulls);
            Ok(())
        })
    }
}

#[pymethods]
impl PyZSetBatch {
    #[new]
    #[pyo3(signature = (schema))]
    pub fn new(py: Python<'_>, schema: PyRef<'_, PySchema>) -> PyResult<Self> {
        let rust_schema = py_schema_to_rust(py, &schema)?;
        let col_keys = rust_schema.columns.iter()
            .map(|c| PyString::new(py, &c.name).unbind())
            .collect();
        let payload_cols = rust_schema.payload_columns()
            .map(|(pi, ci, _)| (pi, ci))
            .collect();
        let batch = ZSetBatch::new(&rust_schema);
        Ok(PyZSetBatch { schema: Arc::new(rust_schema), batch, col_keys, payload_cols })
    }

    /// Append a row given column-ordered values and a weight (backward compat).
    #[pyo3(signature = (values, weight = 1))]
    pub fn append_row(
        &mut self, _py: Python<'_>,
        values: Bound<'_, PyList>,
        weight: i64,
    ) -> PyResult<()> {
        let n_cols = self.schema.columns.len();
        if values.len() != n_cols {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Expected {} values, got {}", n_cols, values.len(),
            )));
        }
        self.append_from_list_inner(&values, weight)
    }

    /// Append a row from a Python dict of {column_name: value}.
    #[pyo3(signature = (values, weight = 1))]
    pub fn append_dict(
        &mut self, _py: Python<'_>,
        values: Bound<'_, PyDict>,
        weight: i64,
    ) -> PyResult<()> {
        self.append_from_dict_inner(&values, weight)
    }

    /// Append rows from an iterable of dicts. Processes entire batch in one
    /// Rust call, eliminating per-row Python→Rust boundary crossings.
    #[pyo3(signature = (rows, weight = 1))]
    pub fn extend_from_dicts(
        &mut self, py: Python<'_>,
        rows: Bound<'_, PyAny>,
        weight: i64,
    ) -> PyResult<()> {
        // Batch-level atomicity: `append_from_dict_inner` rolls back only the
        // current row, so a failure on row N would otherwise leave rows 0..N in
        // the batch. Wrapping the whole loop in `with_rollback` truncates back
        // to the pre-call length on any error, giving `extend` the same
        // all-or-nothing contract as the single-row appends.
        self.with_rollback(|s| {
            let weight_key = pyo3::intern!(py, "_weight");
            for row_item in rows.try_iter()? {
                let row_item = row_item?;
                let dict: &Bound<'_, PyDict> = row_item.downcast()?;
                let row_weight = match dict.get_item(weight_key)? {
                    Some(w) => w.extract::<i64>()?,
                    None => weight,
                };
                s.append_from_dict_inner(dict, row_weight)?;
            }
            Ok(())
        })
    }

    #[getter]
    pub fn pks(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        pk_column_to_pylist(py, &self.schema, &self.batch.pks)
    }
    #[getter]
    pub fn weights(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        Ok(PyList::new(py, &self.batch.weights)?.unbind())
    }
    #[getter]
    pub fn nulls(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        Ok(PyList::new(py, &self.batch.nulls)?.unbind())
    }
    #[getter]
    pub fn columns(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        rust_batch_columns_to_py(py, self.schema.as_ref(), &self.batch, self.batch.len())
    }

    pub fn __len__(&self) -> usize { self.batch.len() }

    pub fn __repr__(&self) -> String {
        format!("ZSetBatch(len={})", self.batch.len())
    }
}

// ---------------------------------------------------------------------------
// Batch conversion helpers
// ---------------------------------------------------------------------------

fn format_uuid(v: u128) -> String {
    format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (v >> 96) as u32,
        (v >> 80) as u16,
        (v >> 64) as u16,
        (v >> 48) as u16,
        v & 0x0000_ffff_ffff_ffff)
}

/// Accept a Python int, `uuid.UUID` object (via `.int`), or hyphenated /
/// plain UUID hex string, returning the 128-bit value. Int is tried first
/// because it is the common case in bulk inserts and avoids a Python
/// attribute lookup per row.
fn extract_uuid_or_u128(val: &Bound<'_, PyAny>) -> PyResult<u128> {
    if let Ok(n) = val.extract::<u128>() {
        return Ok(n);
    }
    if let Ok(attr) = val.getattr("int") {
        if let Ok(n) = attr.extract::<u128>() {
            return Ok(n);
        }
    }
    if let Ok(s) = val.extract::<String>() {
        let hex = s.replace('-', "");
        return u128::from_str_radix(&hex, 16)
            .map_err(|_| pyo3::exceptions::PyValueError::new_err(
                format!("invalid UUID/U128 hex string: {s:?}")
            ));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected int, uuid.UUID object, or UUID string"
    ))
}

/// Write one fixed-width value as little-endian bytes into `dst` (length must
/// equal `tc.wire_stride()`). Zero-allocation; used for PK regions and as the
/// inner write of `write_fixed_le`.
fn write_fixed_le_into(dst: &mut [u8], tc: TypeCode, item: &Bound<'_, PyAny>) -> PyResult<()> {
    match tc {
        TypeCode::U8   => dst[0] = item.extract::<u8>()?,
        TypeCode::I8   => dst[0] = item.extract::<i8>()? as u8,
        TypeCode::U16  => dst.copy_from_slice(&item.extract::<u16>()?.to_le_bytes()),
        TypeCode::I16  => dst.copy_from_slice(&item.extract::<i16>()?.to_le_bytes()),
        TypeCode::U32  => dst.copy_from_slice(&item.extract::<u32>()?.to_le_bytes()),
        TypeCode::I32  => dst.copy_from_slice(&item.extract::<i32>()?.to_le_bytes()),
        TypeCode::F32  => dst.copy_from_slice(&item.extract::<f32>()?.to_le_bytes()),
        TypeCode::U64  => dst.copy_from_slice(&item.extract::<u64>()?.to_le_bytes()),
        TypeCode::I64  => dst.copy_from_slice(&item.extract::<i64>()?.to_le_bytes()),
        TypeCode::F64  => dst.copy_from_slice(&item.extract::<f64>()?.to_le_bytes()),
        TypeCode::String | TypeCode::U128 | TypeCode::UUID | TypeCode::Blob | TypeCode::I128 =>
            unreachable!("handled before write_fixed_le_into"),
    }
    Ok(())
}

/// Write one fixed-width value as little-endian bytes into the tail of `buf`.
fn write_fixed_le(buf: &mut Vec<u8>, tc: TypeCode, item: &Bound<'_, PyAny>) -> PyResult<()> {
    let stride = tc.wire_stride();
    let start  = buf.len();
    buf.resize(start + stride, 0);
    write_fixed_le_into(&mut buf[start..start + stride], tc, item)
}

/// Materialize a `PkColumn` as a Python list. Single-value-per-row PK
/// variants surface as `list[int]`; compound (`Bytes`) variants surface as
/// `list[bytes]` — one packed PK region per row. Shared between PyZSetBatch
/// and PyRustBatch so both surfaces report the same shape.
fn pk_column_to_pylist(py: Python<'_>, schema: &Schema, pks: &PkColumn) -> PyResult<Py<PyList>> {
    match pks {
        PkColumn::Bytes { stride, buf } => {
            let s = *stride as usize;
            let n = buf.len().checked_div(s).unwrap_or(0);
            let items: Vec<PyObject> = (0..n)
                .map(|i| pyo3::types::PyBytes::new(py, &buf[i * s..(i + 1) * s])
                    .into_any().unbind())
                .collect();
            Ok(PyList::new(py, items)?.unbind())
        }
        _ => {
            // The non-Bytes variants are single-column PKs. A lone I128 PK is
            // stored as u128 bits (U128s) but must surface as a signed int.
            let signed = schema.pk_cols.len() == 1
                && schema.columns[schema.pk_cols[0]].type_code == TypeCode::I128;
            if signed {
                let vals: Vec<i128> = (0..pks.len()).map(|i| pks.get(i) as i128).collect();
                Ok(PyList::new(py, &vals)?.unbind())
            } else {
                let vals: Vec<u128> = (0..pks.len()).map(|i| pks.get(i)).collect();
                Ok(PyList::new(py, &vals)?.unbind())
            }
        }
    }
}

/// Decode the PK column `col_idx` for row `row` into a Python value, given
/// its already-extracted `PkTuple` bytes for that row. Handles UUID
/// stringification, U128 widening, and the narrow integer case uniformly.
fn pk_value_from_tuple(py: Python<'_>, schema: &Schema, col_idx: usize, t: &gnitz_core::PkTuple) -> PyObject {
    let tc     = schema.columns[col_idx].type_code;
    let offset = schema.pk_byte_offset(col_idx);
    let stride = tc.wire_stride();
    let bytes  = &t.buf[offset..offset + stride];
    match tc {
        TypeCode::UUID => {
            let mut b = [0u8; 16];
            b.copy_from_slice(bytes);
            format_uuid(u128::from_le_bytes(b))
                .into_pyobject(py).unwrap().into_any().unbind()
        }
        TypeCode::U128 => {
            let mut b = [0u8; 16];
            b.copy_from_slice(bytes);
            u128::from_le_bytes(b)
                .into_pyobject(py).unwrap().into_any().unbind()
        }
        TypeCode::I128 => {
            let mut b = [0u8; 16];
            b.copy_from_slice(bytes);
            i128::from_le_bytes(b)
                .into_pyobject(py).unwrap().into_any().unbind()
        }
        _ => read_fixed_le(py, tc, bytes),
    }
}

/// Read one fixed-width value as a Python object (integers as int, floats as float).
fn read_fixed_le(py: Python<'_>, tc: TypeCode, slice: &[u8]) -> PyObject {
    match tc {
        TypeCode::U8   => slice[0].into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::I8   => (slice[0] as i8).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::U16  => u16::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::I16  => i16::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::U32  => u32::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::I32  => i32::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::F32  => f32::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::U64  => u64::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::I64  => i64::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::F64  => f64::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::String | TypeCode::U128 | TypeCode::UUID | TypeCode::Blob | TypeCode::I128 => unreachable!(),
    }
}

/// Surface one `ColData::U128s` element as the Python object its column type
/// dictates. The three 16-byte integer types share u128 storage but differ at
/// the surface: UUID → canonical string, I128 → signed int, everything else
/// (U128) → unsigned int. Single source of truth for that decision across every
/// read path (row build, batch columns, scan columns).
fn u128_value_to_py(py: Python<'_>, x: u128, tc: TypeCode) -> PyResult<PyObject> {
    Ok(match tc {
        TypeCode::UUID => format_uuid(x).into_pyobject(py)?.into_any().unbind(),
        TypeCode::I128 => (x as i128).into_pyobject(py)?.into_any().unbind(),
        _              => x.into_pyobject(py)?.into_any().unbind(),
    })
}

/// Convert a Rust ZSetBatch + Schema into a Python ZSetBatch (used by non-lazy scan/seek).
fn rust_batch_to_py(
    py:     Python<'_>,
    schema: Arc<Schema>,
    batch:  ZSetBatch,
) -> PyResult<Py<PyZSetBatch>> {
    let col_keys = schema.columns.iter()
        .map(|c| PyString::new(py, &c.name).unbind())
        .collect();
    let payload_cols = schema.payload_columns()
        .map(|(pi, ci, _)| (pi, ci))
        .collect();
    Py::new(py, PyZSetBatch { schema, batch, col_keys, payload_cols })
}

// ---------------------------------------------------------------------------
// Lazy batch infrastructure (Phase 2)
// ---------------------------------------------------------------------------

/// Per-column decode metadata, every field a pure function of the `Schema`.
/// Computed once via [`ColLayout::for_schema`] and reused across every row of a
/// batch, hoisting the schema lookups out of the per-row build loop. Naming the
/// bundle (rather than scattering parallel `Vec`s) keeps it the single place
/// the derivation lives — and the obvious place to memoize per cached schema if
/// the per-batch recompute ever shows up in a profile.
#[derive(Default)]
struct ColLayout {
    /// `schema.is_pk_col(ci)`, per column.
    is_pk:       Vec<bool>,
    /// `schema.payload_idx(ci)` for non-PK cols; 0 for PK cols (unused).
    payload_idx: Vec<usize>,
    /// `type_code.wire_stride()`, per column.
    wire_stride: Vec<usize>,
}

impl ColLayout {
    fn for_schema(s: &Schema) -> Self {
        let is_pk: Vec<bool> = (0..s.columns.len()).map(|ci| s.is_pk_col(ci)).collect();
        let payload_idx: Vec<usize> = (0..s.columns.len())
            .map(|ci| if is_pk[ci] { 0 } else { s.payload_idx(ci) }).collect();
        ColLayout {
            is_pk,
            payload_idx,
            wire_stride: s.columns.iter().map(|c| c.type_code.wire_stride()).collect(),
        }
    }
}

struct SharedBatchData {
    schema:      Arc<Schema>,
    batch:       ZSetBatch,
    /// Pre-computed field-name tuple, created once and shared across all iterators.
    fields:      Py<PyTuple>,
    /// field name → column index, built once and shared via Arc for O(1) row attr lookup.
    field_index: Arc<HashMap<String, usize>>,
    /// Per-column decode metadata, hoisted out of the per-row loop.
    layout:      ColLayout,
}

fn build_field_index(schema: &Schema) -> Arc<HashMap<String, usize>> {
    Arc::new(schema.columns.iter().enumerate().map(|(i, c)| (c.name.clone(), i)).collect())
}

fn make_shared_batch_data(
    py: Python<'_>,
    s: Arc<Schema>,
    b: ZSetBatch,
) -> PyResult<Arc<SharedBatchData>> {
    if b.is_empty() {
        return Ok(Arc::new(SharedBatchData {
            schema:      s,
            batch:       b,
            fields:      PyTuple::empty(py).unbind(),
            field_index: Arc::new(HashMap::new()),
            layout:      ColLayout::default(),
        }));
    }
    let names: Vec<&str> = s.columns.iter().map(|c| c.name.as_str()).collect();
    let fields = PyTuple::new(py, names)?.unbind();
    let field_index = build_field_index(s.as_ref());
    let layout = ColLayout::for_schema(s.as_ref());
    Ok(Arc::new(SharedBatchData {
        schema: s, batch: b, fields, field_index, layout,
    }))
}

/// Build Python values for a single row from Rust data, appending to `out`. The
/// per-column metadata (PK flag, payload index, wire stride) and the schema
/// + batch all live in `data`, so it is threaded as one borrow rather than seven.
fn build_row_values_into(
    py:   Python<'_>,
    data: &SharedBatchData,
    row:  usize,
    out:  &mut Vec<PyObject>,
) -> PyResult<()> {
    let schema: &Schema = &data.schema;
    let batch = &data.batch;
    let null_word = batch.nulls[row];
    let pk_stride = schema.pk_stride() as u8;
    let pk_tuple  = batch.pks.get_tuple(row, pk_stride);

    let layout = &data.layout;
    for ci in 0..schema.columns.len() {
        if layout.is_pk[ci] {
            out.push(pk_value_from_tuple(py, schema, ci, &pk_tuple));
        } else {
            let p_idx = layout.payload_idx[ci];
            if null_word & (1u64 << p_idx) != 0 {
                out.push(py.None());
            } else {
                match &batch.columns[ci] {
                    ColData::Fixed(buf) => {
                        let stride = layout.wire_stride[ci];
                        out.push(read_fixed_le(
                            py, schema.columns[ci].type_code,
                            &buf[row * stride..(row + 1) * stride],
                        ));
                    }
                    ColData::Strings(v) => match &v[row] {
                        Some(s) => out.push(s.into_pyobject(py)?.into_any().unbind()),
                        None    => out.push(py.None()),
                    },
                    ColData::Bytes(v) => match &v[row] {
                        Some(b) => out.push(pyo3::types::PyBytes::new(py, b).into_any().unbind()),
                        None    => out.push(py.None()),
                    },
                    ColData::U128s(v) => {
                        out.push(u128_value_to_py(py, v[row], schema.columns[ci].type_code)?);
                    }
                }
            }
        }
    }
    Ok(())
}



// ---------------------------------------------------------------------------
// PyRustBatch — lazy batch wrapper (read path only)
// ---------------------------------------------------------------------------

#[pyclass(name = "RustBatch")]
pub struct PyRustBatch {
    data: Arc<SharedBatchData>,
    cached_pks:     Option<Py<PyList>>,
    cached_weights: Option<Py<PyList>>,
    cached_nulls:   Option<Py<PyList>>,
    cached_columns: Option<Py<PyList>>,
}

#[pymethods]
impl PyRustBatch {
    #[getter]
    fn pks(&mut self, py: Python<'_>) -> PyResult<Py<PyList>> {
        if let Some(ref cached) = self.cached_pks {
            return Ok(cached.clone_ref(py));
        }
        let list = pk_column_to_pylist(py, &self.data.schema, &self.data.batch.pks)?;
        self.cached_pks = Some(list.clone_ref(py));
        Ok(list)
    }

    #[getter]
    fn weights(&mut self, py: Python<'_>) -> PyResult<Py<PyList>> {
        if let Some(ref cached) = self.cached_weights {
            return Ok(cached.clone_ref(py));
        }
        let list = PyList::new(py, &self.data.batch.weights)?.unbind();
        self.cached_weights = Some(list.clone_ref(py));
        Ok(list)
    }

    #[getter]
    fn nulls(&mut self, py: Python<'_>) -> PyResult<Py<PyList>> {
        if let Some(ref cached) = self.cached_nulls {
            return Ok(cached.clone_ref(py));
        }
        let list = PyList::new(py, &self.data.batch.nulls)?.unbind();
        self.cached_nulls = Some(list.clone_ref(py));
        Ok(list)
    }

    #[getter]
    fn columns(&mut self, py: Python<'_>) -> PyResult<Py<PyList>> {
        if let Some(ref cached) = self.cached_columns {
            return Ok(cached.clone_ref(py));
        }
        let schema = self.data.schema.as_ref();
        let batch = &self.data.batch;
        let n = batch.len();
        let list = rust_batch_columns_to_py(py, schema, batch, n)?;
        self.cached_columns = Some(list.clone_ref(py));
        Ok(list)
    }

    fn __len__(&self) -> usize { self.data.batch.len() }

    fn __repr__(&self) -> String {
        format!("RustBatch(len={})", self.data.batch.len())
    }
}

/// Materialize column lists for PyRustBatch.columns (same format as PyZSetBatch).
fn rust_batch_columns_to_py(
    py: Python<'_>, schema: &Schema, batch: &ZSetBatch, n: usize,
) -> PyResult<Py<PyList>> {
    let mut col_lists: Vec<PyObject> = Vec::with_capacity(schema.columns.len());
    for (ci, col_def) in schema.columns.iter().enumerate() {
        if schema.is_pk_col(ci) {
            col_lists.push(PyList::empty(py).into_any().unbind());
            continue;
        }
        match &batch.columns[ci] {
            ColData::Fixed(buf) => {
                let stride = col_def.type_code.wire_stride();
                let items: Vec<PyObject> = (0..n)
                    .map(|i| read_fixed_le(py, col_def.type_code, &buf[i*stride..(i+1)*stride]))
                    .collect();
                col_lists.push(PyList::new(py, items)?.into_any().unbind());
            }
            ColData::Strings(v) => {
                let items: Vec<PyObject> = v.iter()
                    .map(|s| match s {
                        Some(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
                        None    => Ok(py.None()),
                    })
                    .collect::<PyResult<_>>()?;
                col_lists.push(PyList::new(py, items)?.into_any().unbind());
            }
            ColData::Bytes(v) => {
                let items: Vec<PyObject> = v.iter()
                    .map(|b| match b {
                        Some(b) => Ok(pyo3::types::PyBytes::new(py, b).into_any().unbind()),
                        None    => Ok(py.None()),
                    })
                    .collect::<PyResult<_>>()?;
                col_lists.push(PyList::new(py, items)?.into_any().unbind());
            }
            ColData::U128s(v) => {
                let tc = col_def.type_code;
                let items: Vec<PyObject> = v.iter()
                    .map(|&x| u128_value_to_py(py, x, tc))
                    .collect::<PyResult<_>>()?;
                col_lists.push(PyList::new(py, items)?.into_any().unbind());
            }
        }
    }
    Ok(PyList::new(py, col_lists)?.unbind())
}

// ---------------------------------------------------------------------------
// PyScanResult — Rust-backed ScanResult
// ---------------------------------------------------------------------------

#[pyclass(name = "ScanResult")]
pub struct PyScanResult {
    data:          Option<Arc<SharedBatchData>>,
    #[pyo3(get)]
    lsn:           u64,
    cached_schema: Option<PyObject>,
    cached_batch:  Option<PyObject>,  // PyRustBatch or py.None()
}

#[pymethods]
impl PyScanResult {
    #[getter]
    fn schema(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        if let Some(ref cached) = self.cached_schema {
            return Ok(cached.clone_ref(py));
        }
        let obj = match &self.data {
            None => py.None(),
            Some(d) => rust_schema_to_py(py, d.schema.as_ref())?.into_any(),
        };
        self.cached_schema = Some(obj.clone_ref(py));
        Ok(obj)
    }

    #[getter]
    fn batch(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        if let Some(ref cached) = self.cached_batch {
            return Ok(cached.clone_ref(py));
        }
        let obj = match &self.data {
            None => py.None(),
            Some(d) => {
                let rb = Py::new(py, PyRustBatch {
                    data: Arc::clone(d),
                    cached_pks:     None,
                    cached_weights: None,
                    cached_nulls:   None,
                    cached_columns: None,
                })?;
                rb.into_bound(py).into_any().unbind()
            }
        };
        self.cached_batch = Some(obj.clone_ref(py));
        Ok(obj)
    }

    fn __iter__(&self, _py: Python<'_>) -> PyResult<PyRowIterator> {
        let (data_clone, len) = match &self.data {
            None => (None, 0),
            Some(d) => (Some(Arc::clone(d)), d.batch.len()),
        };
        Ok(PyRowIterator { data: data_clone, row_buf: Vec::new(), pos: 0, len })
    }

    fn __len__(&self) -> usize {
        self.data.as_ref().map_or(0, |d| d.batch.len())
    }

    fn __bool__(&self) -> bool {
        self.__len__() > 0
    }

    fn all(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let mut iter = self.__iter__(py)?;
        let list = PyList::empty(py);
        while let Some(row) = iter.__next__(py)? {
            list.append(row)?;
        }
        Ok(list.unbind())
    }

    fn first(&self, py: Python<'_>) -> PyResult<PyObject> {
        let mut iter = self.__iter__(py)?;
        match iter.__next__(py)? {
            Some(row) => Ok(row),
            None => Ok(py.None()),
        }
    }

    fn one(&self, py: Python<'_>) -> PyResult<PyObject> {
        let n = self.__len__();
        if n != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Expected exactly 1 row, got {n}"),
            ));
        }
        let mut iter = self.__iter__(py)?;
        Ok(iter.__next__(py)?.unwrap())
    }

    fn one_or_none(&self, py: Python<'_>) -> PyResult<PyObject> {
        let n = self.__len__();
        if n > 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Expected at most 1 row, got {n}"),
            ));
        }
        let mut iter = self.__iter__(py)?;
        match iter.__next__(py)? {
            Some(row) => Ok(row),
            None => Ok(py.None()),
        }
    }

    fn mappings(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let data = match &self.data {
            None => return Ok(PyList::empty(py).unbind()),
            Some(d) => d,
        };
        let list = PyList::empty(py);
        let fields = data.fields.bind(py);
        let mut row_buf = Vec::new();
        for i in 0..data.batch.len() {
            row_buf.clear();
            build_row_values_into(py, data, i, &mut row_buf)?;
            let dict = PyDict::new(py);
            for (fi, val) in row_buf.iter().enumerate() {
                dict.set_item(fields.get_item(fi)?, val)?;
            }
            list.append(dict)?;
        }
        Ok(list.unbind())
    }

    #[pyo3(signature = (col=None))]
    fn scalars(&self, py: Python<'_>, col: Option<PyObject>) -> PyResult<Py<PyList>> {
        let data = match &self.data {
            None => return Ok(PyList::empty(py).unbind()),
            Some(d) => d,
        };
        // Resolve col: None→0, int→index, str→name lookup
        let col_idx = match col {
            None => 0usize,
            Some(ref obj) => {
                if let Ok(idx) = obj.extract::<usize>(py) {
                    idx
                } else if let Ok(name) = obj.extract::<String>(py) {
                    data.schema.columns.iter().position(|c| c.name == name)
                        .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(name))?
                } else {
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "col must be int or str",
                    ));
                }
            }
        };
        let n = data.batch.len();
        if col_idx >= data.schema.columns.len() {
            return Err(pyo3::exceptions::PyIndexError::new_err("column index out of range"));
        }

        // Specialize by column kind. Use get_tuple so compound-PK
        // (PkColumn::Bytes) batches work too.
        if data.schema.is_pk_col(col_idx) {
            let pk_stride = data.schema.pk_stride() as u8;
            let items: Vec<PyObject> = (0..data.batch.pks.len())
                .map(|i| {
                    let t = data.batch.pks.get_tuple(i, pk_stride);
                    pk_value_from_tuple(py, &data.schema, col_idx, &t)
                })
                .collect();
            return Ok(PyList::new(py, items)?.unbind());
        }

        let payload_idx = data.schema.payload_idx(col_idx);
        let nulls = &data.batch.nulls;
        let null_bit = 1u64 << payload_idx;

        match &data.batch.columns[col_idx] {
            ColData::Fixed(buf) => {
                let tc = data.schema.columns[col_idx].type_code;
                let stride = tc.wire_stride();
                let items: Vec<PyObject> = (0..n)
                    .map(|i| {
                        if nulls[i] & null_bit != 0 {
                            py.None()
                        } else {
                            read_fixed_le(py, tc, &buf[i * stride..(i + 1) * stride])
                        }
                    })
                    .collect();
                Ok(PyList::new(py, items)?.unbind())
            }
            ColData::Strings(v) => {
                let items: Vec<PyObject> = v.iter().enumerate()
                    .map(|(i, s)| {
                        if nulls[i] & null_bit != 0 {
                            return Ok(py.None());
                        }
                        match s {
                            Some(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
                            None    => Ok(py.None()),
                        }
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, items)?.unbind())
            }
            ColData::Bytes(v) => {
                let items: Vec<PyObject> = v.iter().enumerate()
                    .map(|(i, b)| {
                        if nulls[i] & null_bit != 0 {
                            return Ok::<PyObject, PyErr>(py.None());
                        }
                        match b {
                            Some(b) => Ok(pyo3::types::PyBytes::new(py, b).into_any().unbind()),
                            None    => Ok(py.None()),
                        }
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, items)?.unbind())
            }
            ColData::U128s(v) => {
                let tc = data.schema.columns[col_idx].type_code;
                let items: Vec<PyObject> = v.iter().enumerate()
                    .map(|(i, &x)| {
                        if nulls[i] & null_bit != 0 {
                            return Ok(py.None());
                        }
                        u128_value_to_py(py, x, tc)
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, items)?.unbind())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PyRowIterator
// ---------------------------------------------------------------------------

#[pyclass]
pub struct PyRowIterator {
    data:        Option<Arc<SharedBatchData>>,
    row_buf:     Vec<PyObject>,
    pos:         usize,
    len:         usize,
}

#[pymethods]
impl PyRowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if self.pos >= self.len {
            return Ok(None);
        }
        let data = self.data.as_ref().unwrap();
        self.row_buf.clear();
        build_row_values_into(py, data, self.pos, &mut self.row_buf)?;
        let weight = data.batch.weights[self.pos];
        self.pos += 1;

        let values_tuple = PyTuple::new(py, &self.row_buf)?;
        let row = Py::new(py, PyRow {
            fields: data.fields.clone_ref(py),
            values: values_tuple.unbind(),
            weight,
            field_index: Arc::clone(&data.field_index),
        })?;
        Ok(Some(row.into_any()))
    }
}

// ---------------------------------------------------------------------------
// GnitzClient
// ---------------------------------------------------------------------------

/// Shared helper: convert a (Option<Arc<Schema>>, Option<ZSetBatch>, u64) response into a Python tuple.
fn response_to_py_tuple(
    py: Python<'_>,
    result: ClientResponse,
) -> PyResult<PyObject> {
    let (opt_schema, opt_batch, view_lsn) = to_py_err(result)?;
    let (py_schema, py_batch) = match (opt_schema, opt_batch) {
        (Some(s), Some(b)) => {
            let ps = rust_schema_to_py(py, s.as_ref())?.into_any();
            let pb = rust_batch_to_py(py, Arc::clone(&s), b)?.into_any();
            (ps, pb)
        }
        (Some(s), None) => {
            let ps = rust_schema_to_py(py, s.as_ref())?.into_any();
            (ps, py.None())
        }
        _ => (py.None(), py.None()),
    };
    let lsn_obj = view_lsn.into_pyobject(py)?.into_any().unbind();
    Ok(PyTuple::new(py, [py_schema, py_batch, lsn_obj])?.into_any().unbind())
}

/// Shared helper: wrap a (Option<Arc<Schema>>, Option<ZSetBatch>, u64) into a lazy PyScanResult.
fn response_to_lazy(
    py: Python<'_>,
    result: ClientResponse,
) -> PyResult<Py<PyScanResult>> {
    let (opt_schema, opt_batch, view_lsn) = to_py_err(result)?;
    let data = match opt_schema {
        Some(s) => {
            let b = opt_batch.unwrap_or_else(|| ZSetBatch::new(s.as_ref()));
            Some(make_shared_batch_data(py, s, b)?)
        }
        None => None,
    };
    Py::new(py, PyScanResult { data, lsn: view_lsn, cached_schema: None, cached_batch: None })
}

/// Macro to mutably borrow the live inner client or raise GnitzError.
macro_rules! client {
    ($self:expr) => {
        $self.inner.as_mut()
            .ok_or_else(|| GnitzError::new_err("client already closed"))?
    };
}

#[pyclass(name = "GnitzClient")]
pub struct PyGnitzClient {
    inner: Option<GnitzClient>,
}

#[pymethods]
impl PyGnitzClient {
    #[new]
    pub fn new(socket_path: &str) -> PyResult<Self> {
        to_py_err(GnitzClient::connect(socket_path)).map(|c| PyGnitzClient { inner: Some(c) })
    }

    pub fn close(&mut self) {
        if let Some(c) = self.inner.take() { c.close(); }
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }
    pub fn __exit__(&mut self,
                    _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject) -> bool {
        self.close();
        false
    }

    // ----- DDL -----

    pub fn create_schema(&mut self, name: &str) -> PyResult<u64> {
        to_py_err(client!(self).create_schema(name))
    }

    pub fn drop_schema(&mut self, name: &str) -> PyResult<()> {
        to_py_err(client!(self).drop_schema(name))
    }

    #[pyo3(signature = (schema_name, table_name, columns, pk_col_idx = 0, unique_pk = true))]
    pub fn create_table(
        &mut self, _py: Python<'_>,
        schema_name: &str, table_name: &str,
        columns: Bound<'_, PyList>,
        pk_col_idx: usize,
        unique_pk: bool,
    ) -> PyResult<u64> {
        let cols: Vec<ColumnDef> = columns.iter()
            .map(|item| { let c: PyRef<'_, PyColumnDef> = item.extract()?; py_col_to_rust(&c) })
            .collect::<PyResult<_>>()?;
        // The Python binding stays single-PK; wrap the supplied index in a
        // one-element slice for the shared compound-PK signature. Compound
        // PKs are reached through SQL DDL (`CREATE TABLE ... PRIMARY KEY
        // (a, b)`), not this surface.
        let pk_slice = [pk_col_idx as u32];
        // Single-PK surface ⇒ partitioned, default distribution (full PK);
        // `REPLICATED` and `CLUSTER BY` are SQL-only features.
        to_py_err(client!(self).create_table(schema_name, table_name, &cols, &pk_slice, unique_pk, false, 0))
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> PyResult<()> {
        to_py_err(client!(self).drop_table(schema_name, table_name))
    }

    // ----- DML -----

    /// push(target_id, batch, conflict_mode="update") -> ingest_lsn: int
    ///
    /// `conflict_mode` controls how PK conflicts are handled:
    ///   - "update" (default): retract-and-insert on PK conflict,
    ///     suitable for DBSP z-set retraction patterns.
    ///   - "error": reject the batch with a SQL-standard
    ///     `duplicate key value violates unique constraint` error.
    #[pyo3(signature = (target_id, batch, conflict_mode=None))]
    pub fn push(
        &mut self, _py: Python<'_>,
        target_id: u64,
        batch: PyRef<'_, PyZSetBatch>,
        conflict_mode: Option<&str>,
    ) -> PyResult<u64> {
        let mode = match conflict_mode {
            None | Some("update") => gnitz_core::WireConflictMode::Update,
            Some("error") => gnitz_core::WireConflictMode::Error,
            Some(other) => return Err(GnitzError::new_err(format!(
                "invalid conflict_mode '{other}': expected 'update' or 'error'"
            ))),
        };
        to_py_err(client!(self).push_with_mode(target_id, batch.schema.as_ref(), &batch.batch, mode))
    }

    /// scan(target_id) -> (Schema | None, ZSetBatch | None, view_lsn: int)
    pub fn scan(&mut self, py: Python<'_>, target_id: u64) -> PyResult<PyObject> {
        response_to_py_tuple(py, client!(self).scan(target_id))
    }

    /// delete(target_id, schema, pks) — `pks` is a list where each element is
    /// either an int (narrow/wide single-PK) or bytes (compound or raw-byte PK).
    pub fn delete(
        &mut self, py: Python<'_>,
        target_id: u64,
        schema: PyRef<'_, PySchema>,
        pks: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let rust_schema = py_schema_to_rust(py, &schema)?;
        let stride = rust_schema.pk_stride();
        let mut pk_col = gnitz_core::PkColumn::empty_for_schema(&rust_schema);
        for pk_val in &pks {
            let t = pk_tuple_from_py_with_stride(pk_val, stride)?;
            pk_col.push_tuple(&t);
        }
        to_py_err(client!(self).delete(target_id, &rust_schema, pk_col))
    }

    // ----- Views -----

    pub fn create_view(
        &mut self, py: Python<'_>,
        schema_name: &str, view_name: &str,
        source_table_id: u64,
        output_schema: PyRef<'_, PySchema>,
    ) -> PyResult<u64> {
        let rust_schema = py_schema_to_rust(py, &output_schema)?;
        to_py_err(client!(self).create_view(schema_name, view_name, source_table_id, &rust_schema.columns))
    }

    /// create_view_with_circuit — CONSUMES circuit.
    pub fn create_view_with_circuit(
        &mut self, py: Python<'_>,
        schema_name: &str, view_name: &str,
        mut circuit: PyRefMut<'_, PyCircuit>,
        output_schema: PyRef<'_, PySchema>,
    ) -> PyResult<u64> {
        let circuit = circuit.inner.take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("Circuit already consumed"))?;
        let rust_schema = py_schema_to_rust(py, &output_schema)?;
        // Hand-built circuits from the Python API emit a single output PK at slot 0.
        to_py_err(client!(self).create_view_with_circuit(schema_name, view_name, "", circuit, &rust_schema.columns, &[0]))
    }

    pub fn drop_view(&mut self, schema_name: &str, view_name: &str) -> PyResult<()> {
        to_py_err(client!(self).drop_view(schema_name, view_name))
    }

    /// resolve_table_id(schema_name, table_name) -> (tid: int, schema: Schema)
    pub fn resolve_table_id(
        &mut self, py: Python<'_>,
        schema_name: &str, table_name: &str,
    ) -> PyResult<PyObject> {
        let (tid, schema) = to_py_err(client!(self).resolve_table_or_view_id(schema_name, table_name))?;
        let py_schema = rust_schema_to_py(py, &schema)?.into_any();
        let tid_obj   = tid.into_pyobject(py)?.into_any().unbind();
        Ok(PyTuple::new(py, [tid_obj, py_schema])?.into_any().unbind())
    }

    /// allocate_table_id() — name matches py_client API
    pub fn allocate_table_id(&mut self) -> PyResult<u64> {
        to_py_err(client!(self).alloc_table_id())
    }

    /// allocate_schema_id()
    pub fn allocate_schema_id(&mut self) -> PyResult<u64> {
        to_py_err(client!(self).alloc_schema_id())
    }

    /// seek_by_index(table_id, col_indices, key_vals) -> (Schema | None, ZSetBatch | None, view_lsn: int)
    ///
    /// `col_indices` is the index's FULL declared column list (the server matches
    /// the circuit by exact list); `key_vals` supplies the leading native key
    /// values (`len(key_vals)` may be `< len(col_indices)` for a leading-prefix
    /// seek). Arity is validated once inside `GnitzClient::seek_by_index` (the
    /// single choke point for every binding), so no validation is duplicated here.
    /// Key values are decoded through `extract_uuid_or_u128`, so a UUID-keyed seek
    /// accepts the same `uuid.UUID` / hex-string forms the insert paths do.
    #[pyo3(signature = (table_id, col_indices, key_vals))]
    pub fn seek_by_index(
        &mut self, py: Python<'_>, table_id: u64,
        col_indices: Vec<u32>, key_vals: Bound<'_, PyList>,
    ) -> PyResult<PyObject> {
        let keys: Vec<u128> = key_vals.iter()
            .map(|item| extract_uuid_or_u128(&item))
            .collect::<PyResult<_>>()?;
        response_to_py_tuple(py, client!(self).seek_by_index(table_id, &col_indices, &keys))
    }

    /// seek(table_id, pk) -> (Schema | None, ZSetBatch | None, view_lsn: int)
    ///
    /// `pk` may be an `int` (back-compat, narrow single-PK tables) or
    /// `bytes` (compound or wide-byte PKs).
    pub fn seek(
        &mut self, py: Python<'_>, table_id: u64, pk: Bound<'_, PyAny>,
    ) -> PyResult<PyObject> {
        let t = pk_tuple_from_py(&pk)?;
        response_to_py_tuple(py, client!(self).seek(table_id, &t))
    }

    // ----- Lazy scan/seek (Phase 2) — skip rust_batch_to_py entirely -----

    /// scan_lazy(target_id) -> ScanResult (native)
    pub fn scan_lazy(&mut self, py: Python<'_>, target_id: u64) -> PyResult<Py<PyScanResult>> {
        response_to_lazy(py, client!(self).scan(target_id))
    }

    /// seek_lazy(table_id, pk) -> ScanResult (native)
    pub fn seek_lazy(
        &mut self, py: Python<'_>, table_id: u64, pk: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyScanResult>> {
        let t = pk_tuple_from_py(&pk)?;
        response_to_lazy(py, client!(self).seek(table_id, &t))
    }

    /// seek_by_index_lazy(table_id, col_indices, key_vals) -> ScanResult (native)
    #[pyo3(signature = (table_id, col_indices, key_vals))]
    pub fn seek_by_index_lazy(
        &mut self, py: Python<'_>, table_id: u64,
        col_indices: Vec<u32>, key_vals: Bound<'_, PyList>,
    ) -> PyResult<Py<PyScanResult>> {
        let keys: Vec<u128> = key_vals.iter()
            .map(|item| extract_uuid_or_u128(&item))
            .collect::<PyResult<_>>()?;
        response_to_lazy(py, client!(self).seek_by_index(table_id, &col_indices, &keys))
    }

    /// execute_sql(schema_name, sql) -> list of result dicts
    pub fn execute_sql(&mut self, py: Python<'_>, schema_name: &str, sql: &str) -> PyResult<PyObject> {
        let client_ref = client!(self);
        let mut planner = SqlPlanner::new(client_ref, schema_name);
        let results = to_py_err(planner.execute(sql))?;

        let py_list = PyList::empty(py);
        for r in results {
            let d = PyDict::new(py);
            match r {
                SqlResult::TableCreated { table_id } => {
                    d.set_item("type", "TableCreated")?;
                    d.set_item("table_id", table_id)?;
                }
                SqlResult::ViewCreated { view_id } => {
                    d.set_item("type", "ViewCreated")?;
                    d.set_item("view_id", view_id)?;
                }
                SqlResult::IndexCreated { index_id } => {
                    d.set_item("type", "IndexCreated")?;
                    d.set_item("index_id", index_id)?;
                }
                SqlResult::Dropped => {
                    d.set_item("type", "Dropped")?;
                }
                SqlResult::RowsAffected { count } => {
                    d.set_item("type", "RowsAffected")?;
                    d.set_item("count", count)?;
                }
                SqlResult::Rows { schema, batch } => {
                    d.set_item("type", "Rows")?;
                    let data = make_shared_batch_data(py, Arc::new(schema), batch)?;
                    let scan_result = Py::new(py, PyScanResult {
                        data: Some(data), lsn: 0,
                        cached_schema: None, cached_batch: None,
                    })?;
                    d.set_item("rows", scan_result)?;
                }
            }
            py_list.append(d)?;
        }
        Ok(py_list.into_any().unbind())
    }
}

// ---------------------------------------------------------------------------
// ExprBuilder + ExprProgram
// ---------------------------------------------------------------------------

macro_rules! expr_builder {
    ($self:expr) => {
        $self.inner.as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("ExprBuilder already consumed"))?
    };
}

#[pyclass(name = "ExprBuilder")]
pub struct PyExprBuilder {
    inner: Option<ExprBuilder>,
}

impl Default for PyExprBuilder {
    fn default() -> Self { Self::new() }
}

#[pymethods]
impl PyExprBuilder {
    #[new]
    pub fn new() -> Self { PyExprBuilder { inner: Some(ExprBuilder::new()) } }

    pub fn load_col_int(&mut self, col_idx: usize) -> PyResult<u32> {
        Ok(expr_builder!(self).load_col_int(col_idx))
    }
    pub fn load_col_float(&mut self, col_idx: usize) -> PyResult<u32> {
        Ok(expr_builder!(self).load_col_float(col_idx))
    }
    pub fn load_const(&mut self, value: i64) -> PyResult<u32> {
        Ok(expr_builder!(self).load_const(value))
    }
    pub fn add(&mut self, a: u32, b: u32) -> PyResult<u32>     { Ok(expr_builder!(self).add(a, b))     }
    pub fn sub(&mut self, a: u32, b: u32) -> PyResult<u32>     { Ok(expr_builder!(self).sub(a, b))     }
    pub fn cmp_eq(&mut self, a: u32, b: u32) -> PyResult<u32>  { Ok(expr_builder!(self).cmp_eq(a, b))  }
    pub fn cmp_ne(&mut self, a: u32, b: u32) -> PyResult<u32>  { Ok(expr_builder!(self).cmp_ne(a, b))  }
    pub fn cmp_gt(&mut self, a: u32, b: u32) -> PyResult<u32>  { Ok(expr_builder!(self).cmp_gt(a, b))  }
    pub fn cmp_ge(&mut self, a: u32, b: u32) -> PyResult<u32>  { Ok(expr_builder!(self).cmp_ge(a, b))  }
    pub fn cmp_lt(&mut self, a: u32, b: u32) -> PyResult<u32>  { Ok(expr_builder!(self).cmp_lt(a, b))  }
    pub fn cmp_le(&mut self, a: u32, b: u32) -> PyResult<u32>  { Ok(expr_builder!(self).cmp_le(a, b))  }
    pub fn bool_and(&mut self, a: u32, b: u32) -> PyResult<u32>{ Ok(expr_builder!(self).bool_and(a, b))}
    pub fn bool_or(&mut self, a: u32, b: u32) -> PyResult<u32> { Ok(expr_builder!(self).bool_or(a, b)) }
    pub fn bool_not(&mut self, a: u32) -> PyResult<u32>        { Ok(expr_builder!(self).bool_not(a))   }
    pub fn is_null(&mut self, col_idx: usize) -> PyResult<u32> { Ok(expr_builder!(self).is_null(col_idx)) }
    pub fn is_not_null(&mut self, col_idx: usize) -> PyResult<u32> { Ok(expr_builder!(self).is_not_null(col_idx)) }

    /// Consume the builder and return a compiled ExprProgram.
    pub fn build(&mut self, result_reg: u32) -> PyResult<PyExprProgram> {
        let b = self.inner.take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("ExprBuilder already consumed"))?;
        Ok(PyExprProgram { inner: b.build(result_reg) })
    }
}

#[pyclass(name = "ExprProgram")]
pub struct PyExprProgram {
    pub(crate) inner: ExprProgram,
}

#[pymethods]
impl PyExprProgram {
    pub fn __repr__(&self) -> String {
        format!("ExprProgram(num_regs={}, result_reg={})", self.inner.num_regs, self.inner.result_reg)
    }
}

// ---------------------------------------------------------------------------
// CircuitBuilder + CircuitGraph
// ---------------------------------------------------------------------------

macro_rules! circuit_builder {
    ($self:expr) => {
        $self.inner.as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("CircuitBuilder already consumed"))?
    };
}

#[pyclass(name = "CircuitBuilder")]
pub struct PyCircuitBuilder {
    inner: Option<CircuitBuilder>,
}

#[pymethods]
impl PyCircuitBuilder {
    #[new]
    pub fn new(view_id: u64, primary_source_id: u64) -> Self {
        PyCircuitBuilder { inner: Some(CircuitBuilder::new(view_id, primary_source_id)) }
    }

    pub fn input_delta(&mut self) -> PyResult<u64>          { Ok(circuit_builder!(self).input_delta()) }
    pub fn trace_scan(&mut self, table_id: u64) -> PyResult<u64> { Ok(circuit_builder!(self).trace_scan(table_id)) }
    pub fn negate(&mut self, input: u64) -> PyResult<u64>   { Ok(circuit_builder!(self).negate(input))   }
    pub fn union(&mut self, a: u64, b: u64) -> PyResult<u64>{ Ok(circuit_builder!(self).union(a, b))     }
    pub fn delay(&mut self, input: u64) -> PyResult<u64>    { Ok(circuit_builder!(self).delay(input))    }
    pub fn distinct(&mut self, input: u64) -> PyResult<u64> { Ok(circuit_builder!(self).distinct(input)) }

    /// filter(input, expr=None) — clones ExprProgram so Python keeps its reference.
    #[pyo3(signature = (input, expr = None))]
    pub fn filter(
        &mut self, input: u64,
        expr: Option<PyRef<'_, PyExprProgram>>,
    ) -> PyResult<u64> {
        let expr_opt = expr.map(|e| e.inner.clone());
        Ok(circuit_builder!(self).filter(input, expr_opt))
    }

    #[pyo3(signature = (input, projection = None))]
    pub fn map(&mut self, input: u64, projection: Option<Vec<usize>>) -> PyResult<u64> {
        Ok(circuit_builder!(self).map(input, projection.as_deref().unwrap_or(&[])))
    }

    pub fn join(&mut self, delta: u64, trace_table_id: u64) -> PyResult<u64> {
        Ok(circuit_builder!(self).join(delta, trace_table_id))
    }
    pub fn anti_join(&mut self, delta: u64, trace_table_id: u64) -> PyResult<u64> {
        Ok(circuit_builder!(self).anti_join(delta, trace_table_id))
    }
    pub fn semi_join(&mut self, delta: u64, trace_table_id: u64) -> PyResult<u64> {
        Ok(circuit_builder!(self).semi_join(delta, trace_table_id))
    }

    #[pyo3(signature = (input, group_by_cols, agg_func_id = 0, agg_col_idx = 0))]
    pub fn reduce(
        &mut self, input: u64,
        group_by_cols: Vec<usize>,
        agg_func_id: u64,
        agg_col_idx: usize,
    ) -> PyResult<u64> {
        Ok(circuit_builder!(self).reduce(input, &group_by_cols, agg_func_id, agg_col_idx))
    }

    pub fn shard(&mut self, input: u64, shard_columns: Vec<usize>) -> PyResult<u64> {
        Ok(circuit_builder!(self).shard(input, &shard_columns))
    }

    pub fn gather(&mut self, input: u64) -> PyResult<u64> {
        Ok(circuit_builder!(self).gather(input))
    }

    pub fn sink(&mut self, input: u64) -> PyResult<u64> {
        Ok(circuit_builder!(self).sink(input))
    }

    pub fn map_expr(&mut self, input: u64, expr: PyRef<'_, PyExprProgram>) -> PyResult<u64> {
        Ok(circuit_builder!(self).map_expr(input, expr.inner.clone()))
    }

    /// Consume builder and produce a Circuit for create_view_with_circuit.
    pub fn build(&mut self) -> PyResult<PyCircuit> {
        let b = self.inner.take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("CircuitBuilder already consumed"))?;
        Ok(PyCircuit { inner: Some(b.build()) })
    }
}

#[pyclass(name = "Circuit")]
pub struct PyCircuit {
    pub(crate) inner: Option<Circuit>,
}

#[pymethods]
impl PyCircuit {
    pub fn __repr__(&self) -> String {
        match &self.inner {
            Some(c) => format!("Circuit(view_id={}, nodes={})", c.view_id, c.nodes.len()),
            None    => "Circuit(consumed)".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared DML encode helpers — single source of truth for argument mapping.
// Both PyGnitzClient (sync, via send_message) and PyAsyncTransport (async)
// use encode_message() with these same patterns.
// ---------------------------------------------------------------------------

fn encode_push_payload(
    client_id: u64, target_id: u64, schema: &Schema, batch: &ZSetBatch,
) -> Result<Vec<u8>, gnitz_core::ProtocolError> {
    gnitz_core::encode_message(target_id, client_id, 0, &gnitz_core::PkTuple::EMPTY, 0, Some(schema), Some(batch))
}

/// Which validation and integer-widening rules `pk_tuple_from_py_impl` applies.
/// A named enum (rather than `Option<usize>`) so the two modes carry their own
/// meaning at every match arm instead of in a comment.
#[derive(Clone, Copy)]
enum PkMode {
    /// Schema PK stride is known (DML paths): byte PKs must equal `stride`
    /// exactly, and ints widen to a `stride`-wide tuple matching the
    /// `PkColumn` variant.
    SchemaStride(usize),
    /// Wire-only narrow seek with no schema at the FFI boundary: byte PKs are
    /// bounded by `MAX_PK_BYTES`, and ints become a narrow 16-byte tuple (the
    /// high padding is inert — the server reads only the column's stride).
    WireNarrow,
}

/// Shared body for the two PK-from-Python builders; `mode` selects the rules.
fn pk_tuple_from_py_impl(
    pk: &Bound<'_, PyAny>, mode: PkMode,
) -> PyResult<gnitz_core::PkTuple> {
    // bytes first: extract::<&[u8]>() rejects non-bytes in O(1), whereas
    // extract_uuid_or_u128 invokes getattr("int") on failure (~100 ns).
    if let Ok(bytes) = pk.extract::<&[u8]>() {
        match mode {
            PkMode::SchemaStride(stride) => {
                if bytes.len() != stride {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "pk bytes length {} != schema pk_stride {}", bytes.len(), stride
                    )));
                }
            }
            PkMode::WireNarrow => {
                if bytes.is_empty() || bytes.len() > MAX_PK_BYTES {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "pk bytes length out of range"));
                }
            }
        }
        return Ok(gnitz_core::PkTuple::from_bytes(bytes));
    }
    if let Ok(val) = extract_uuid_or_u128(pk) {
        return Ok(match mode {
            PkMode::SchemaStride(stride) => gnitz_core::PkTuple::from_u128(stride as u8, val),
            PkMode::WireNarrow           => gnitz_core::PkTuple::from_u128_narrow(val),
        });
    }
    Err(pyo3::exceptions::PyTypeError::new_err("pk must be int, uuid.UUID, UUID string, or bytes"))
}

/// Build a `PkTuple` from a Python value for the wire-only `seek` paths,
/// where no schema is available at the FFI boundary.
fn pk_tuple_from_py(pk: &Bound<'_, PyAny>) -> PyResult<gnitz_core::PkTuple> {
    pk_tuple_from_py_impl(pk, PkMode::WireNarrow)
}

/// Build a `PkTuple` from a Python value when the schema's PK stride is
/// known (DML paths).
fn pk_tuple_from_py_with_stride(
    pk: &Bound<'_, PyAny>, stride: usize,
) -> PyResult<gnitz_core::PkTuple> {
    pk_tuple_from_py_impl(pk, PkMode::SchemaStride(stride))
}

// ---------------------------------------------------------------------------
// AsyncTransport — background I/O thread for async pipelining
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum ResponseKind {
    Push,  // resolve with u64 (seek_pk as u64 = ingest LSN)
    Scan,  // resolve with PyScanResult
}

struct IoRequest {
    payload:   Vec<u8>,
    hint:      Option<(Arc<Schema>, u16)>,
    target_id: u64,
    future:    Py<PyAny>,
    kind:      ResponseKind,
}

/// Bound on the I/O request channel. Limits RAM when Python sends faster than
/// the network flushes. `enqueue` returns GnitzError if the channel is full.
const IO_CHANNEL_DEPTH: usize = 4096;

const ASYNC_SCHEMA_CACHE_CAP: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(64).unwrap();

/// Cap on requests merged into one natural-batching cycle.
const IO_BATCH_MAX: usize = 1024;

static TRANSPORT_ID: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(1);

/// dup() of the connection's socket fd. The I/O thread owns the original
/// sock_fd; `DupFd` is an independent reference whose `shutdown` wakes any
/// in-flight `recv_framed` on the shared open file description, even after
/// the I/O thread has closed sock_fd (the integer may already be recycled).
struct DupFd(std::os::unix::io::RawFd);

impl Drop for DupFd {
    fn drop(&mut self) {
        unsafe { libc::shutdown(self.0, libc::SHUT_RDWR); }
        gnitz_core::close_fd(self.0);
    }
}

/// Sole owner of the connection's `sock_fd` from `connect` until the I/O
/// thread exits, closing it on drop. This covers the setup error paths in
/// `PyAsyncTransport::new` (handshake/dup failure → early return drops the
/// guard) and any unwind through `async_io_loop` (e.g. a poisoned
/// `schema_cache` lock or an `.unwrap()` while resolving futures). Unlike
/// [`DupFd`] it only closes — it never `shutdown`s, because the
/// wake-on-shutdown is the Python-side `DupFd`'s job and this fd's owner never
/// shuts the socket down.
struct SocketGuard(std::os::unix::io::RawFd);

impl Drop for SocketGuard {
    fn drop(&mut self) { gnitz_core::close_fd(self.0); }
}

#[pyclass(name = "AsyncTransport")]
struct PyAsyncTransport {
    tx:           Option<std::sync::mpsc::SyncSender<IoRequest>>,
    dup_fd:       Option<DupFd>,
    event_loop:   Py<PyAny>,
    client_id:    u64,
    thread:       Option<std::thread::JoinHandle<()>>,
    schema_cache: SchemaCache,
}

impl PyAsyncTransport {
    fn enqueue(
        &self, py: Python<'_>,
        payload: Vec<u8>, hint: Option<(Arc<Schema>, u16)>, target_id: u64,
        kind: ResponseKind,
    ) -> PyResult<PyObject> {
        let tx = self.tx.as_ref().ok_or_else(|| {
            GnitzError::new_err("connection closed")
        })?;
        let fut = self.event_loop.call_method0(py, "create_future")?;
        tx.try_send(IoRequest {
            payload,
            hint,
            target_id,
            future: fut.clone_ref(py),
            kind,
        }).map_err(|e| match e {
            std::sync::mpsc::TrySendError::Full(_) =>
                GnitzError::new_err("transport queue full"),
            std::sync::mpsc::TrySendError::Disconnected(_) =>
                GnitzError::new_err("I/O thread exited"),
        })?;
        Ok(fut)
    }
}

#[pymethods]
impl PyAsyncTransport {
    #[new]
    fn new(
        py: Python<'_>,
        socket_path: &str,
        event_loop: PyObject,
        set_result_fn: PyObject,
        set_exception_fn: PyObject,
    ) -> PyResult<Self> {
        // Own the fd from birth so every early return below closes it via RAII
        // rather than a manual `close_fd`; on success the guard is moved into
        // the I/O thread, which closes it when it exits.
        let sock = SocketGuard(to_py_err(gnitz_core::connect(socket_path))?);
        let sock_fd = sock.0;
        // HELLO handshake on the calling thread — captures the negotiated
        // payload limit before the I/O thread starts queueing reads. Drop
        // the GIL across the blocking syscalls so other Python threads
        // can progress if the server is slow to respond.
        let max_payload_len =
            to_py_err(py.allow_threads(|| gnitz_core::hello_handshake(sock_fd)))? as usize;

        let raw_dup = unsafe { libc::dup(sock_fd) };
        if raw_dup < 0 {
            let err = std::io::Error::last_os_error();
            return Err(GnitzError::new_err(format!("dup socket fd: {err}")));
        }
        let dup_fd = DupFd(raw_dup);

        let seq = TRANSPORT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64;
        let client_id = (std::process::id() as u64) << 32 | seq;
        let (tx, rx) = std::sync::mpsc::sync_channel(IO_CHANNEL_DEPTH);
        let loop_ref: Py<PyAny> = event_loop.clone_ref(py);
        let sr_fn: Py<PyAny> = set_result_fn.clone_ref(py);
        let se_fn: Py<PyAny> = set_exception_fn.clone_ref(py);

        let schema_cache: SchemaCache =
            Arc::new(Mutex::new(LruCache::new(ASYNC_SCHEMA_CACHE_CAP)));
        let cache_ref = Arc::clone(&schema_cache);
        let handle = std::thread::spawn(move || {
            async_io_loop(sock, max_payload_len, rx, loop_ref, sr_fn, se_fn, cache_ref);
        });

        Ok(PyAsyncTransport {
            tx:           Some(tx),
            dup_fd:       Some(dup_fd),
            event_loop,
            client_id,
            thread:       Some(handle),
            schema_cache,
        })
    }

    fn push(
        &self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>,
    ) -> PyResult<PyObject> {
        let payload = to_py_err(encode_push_payload(self.client_id, target_id, batch.schema.as_ref(), &batch.batch))?;
        self.enqueue(py, payload, None, target_id, ResponseKind::Push)
    }

    fn scan(&self, py: Python<'_>, target_id: u64) -> PyResult<PyObject> {
        let (flags, hint) = {
            let cache = self.schema_cache.lock().unwrap();
            match cache.peek(&target_id) {
                Some((s, v)) => (
                    gnitz_core::wire_flags_set_schema_version(0u64, *v),
                    Some((Arc::clone(s), *v)),
                ),
                None => (0u64, None),
            }
        };
        let payload = to_py_err(gnitz_core::encode_message(
            target_id, self.client_id, flags, &gnitz_core::PkTuple::EMPTY, 0, None, None,
        ))?;
        self.enqueue(py, payload, hint, target_id, ResponseKind::Scan)
    }

    fn seek(
        &self, py: Python<'_>, target_id: u64, pk: Bound<'_, PyAny>,
    ) -> PyResult<PyObject> {
        let t = pk_tuple_from_py(&pk)?;
        let (flags, hint) = {
            let cache = self.schema_cache.lock().unwrap();
            match cache.peek(&target_id) {
                Some((s, v)) => (
                    gnitz_core::wire_flags_set_schema_version(gnitz_core::FLAG_SEEK, *v),
                    Some((Arc::clone(s), *v)),
                ),
                None => (gnitz_core::FLAG_SEEK, None),
            }
        };
        let payload = to_py_err(gnitz_core::encode_message(
            target_id, self.client_id, flags, &t, 0, None, None,
        ))?;
        self.enqueue(py, payload, hint, target_id, ResponseKind::Scan)
    }

    #[getter]
    fn client_id(&self) -> u64 { self.client_id }

    fn close(&mut self, py: Python<'_>) {
        self.tx.take();
        // DupFd::drop shuts down + closes the dup'd fd; the shutdown wakes
        // any in-flight recv_framed on the I/O thread.
        self.dup_fd.take();
        if let Some(h) = self.thread.take() {
            // Release the GIL so the I/O thread can finish any in-progress
            // with_gil block before the join returns.
            py.allow_threads(|| { let _ = h.join(); });
        }
    }
}

impl Drop for PyAsyncTransport {
    fn drop(&mut self) {
        // Do NOT join: Drop may be called from GC while holding the GIL, and
        // the I/O thread acquires the GIL to resolve futures — joining would
        // deadlock. DupFd::drop still fires the shutdown so the I/O thread
        // can exit promptly on its own.
        self.tx.take();
        self.dup_fd.take();
    }
}

/// Receive a streaming scan response, using `hint` for warm-cache decoding.
///
/// Returns `Ok(Ok(...))` on success, `Ok(Err(text))` for application-level
/// server errors (transport stays up), and `Err(...)` for transport/protocol
/// failures (caller should tear down the connection).
fn recv_scan_response(
    sock_fd:         std::os::unix::io::RawFd,
    max_payload_len: usize,
    hint:            Option<(Arc<Schema>, u16)>,
) -> ScanReadResult {
    let mut schema: Option<Arc<Schema>> = hint.as_ref().map(|(s, _)| Arc::clone(s));
    let mut schema_version: u16 = hint.map_or(0, |(_, v)| v);
    let mut data: Option<ZSetBatch> = None;
    let lsn: u64 = loop {
        let buf = gnitz_core::recv_framed(sock_fd, max_payload_len).map_err(|e| e.to_string())?;
        let hint_ref = schema.as_ref().map(|s| (s.as_ref(), schema_version));
        let msg = gnitz_core::parse_response(&buf, hint_ref).map_err(|e| e.to_string())?;
        if msg.status != 0 {
            let text = msg.error_text
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "server error".into());
            return Ok(Err(text));
        }
        let is_continuation = (msg.flags & gnitz_core::FLAG_CONTINUATION) != 0;
        if let Some(s) = msg.schema {
            schema_version = gnitz_core::wire_flags_get_schema_version(msg.flags);
            schema = Some(Arc::new(s));
        }
        if let Some(batch) = msg.data_batch {
            match data.as_mut() {
                Some(acc) => acc.extend_from_owned(batch),
                None      => data = Some(batch),
            }
        }
        if !is_continuation { break msg.seek_pk as u64; }
    };
    Ok(Ok((schema, schema_version, data, lsn)))
}

/// Decoded scan response carried by `LoopResult::Scan`. Boxed in the enum so the
/// large scan payload doesn't pad the small `Push`/`ScanError` variants.
struct ScanData {
    schema:         Option<Arc<Schema>>,
    schema_version: u16,
    batch:          Option<ZSetBatch>,
    lsn:            u64,
    target_id:      u64,
}

enum LoopResult {
    // Both payloads are boxed so neither pads the small `ScanError` variant
    // (`Message` is ~350 bytes, `ScanData` carries a batch).
    Push(Box<gnitz_core::Message>),
    Scan(Box<ScanData>),
    ScanError(String),
}

fn async_io_loop(
    sock:            SocketGuard,
    max_payload_len: usize,
    rx:              std::sync::mpsc::Receiver<IoRequest>,
    loop_ref:        Py<PyAny>,
    sr_fn:           Py<PyAny>,
    se_fn:           Py<PyAny>,
    schema_cache:    SchemaCache,
) {
    use std::collections::VecDeque;

    // `sock` owns the fd for the whole loop: the two early returns and the
    // normal `break` all fall through to its drop, which closes it — so an
    // unwind through this loop cannot leak it either.
    let sock_fd = sock.0;

    let mut pending_futures: VecDeque<(Py<PyAny>, ResponseKind)> =
        VecDeque::with_capacity(IO_BATCH_MAX);

    // Hoisted scratch — .clear()ed each iteration so the outer slot
    // arrays are reused. (Inner `Vec<u8>` payloads are still produced
    // and dropped per batch by `encode_push_payload`.)
    let mut payloads:    Vec<Vec<u8>>                     = Vec::with_capacity(IO_BATCH_MAX);
    let mut kinds:       Vec<ResponseKind>                = Vec::with_capacity(IO_BATCH_MAX);
    let mut hints:       Vec<Option<(Arc<Schema>, u16)>>  = Vec::with_capacity(IO_BATCH_MAX);
    let mut target_ids:  Vec<u64>                         = Vec::with_capacity(IO_BATCH_MAX);
    let mut results:     Vec<LoopResult>                  = Vec::with_capacity(IO_BATCH_MAX);

    loop {
        // Block until at least one request
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_)  => break, // sender dropped → clean shutdown
        };

        // Drain additional queued requests (natural batching), capped to
        // avoid filling the socket send buffer before reading any responses.
        // Py<PyAny>: Send, so moving futures into pending_futures needs no GIL.
        payloads.clear();
        kinds.clear();
        hints.clear();
        target_ids.clear();
        payloads.push(first.payload);
        kinds.push(first.kind);
        hints.push(first.hint);
        target_ids.push(first.target_id);
        pending_futures.push_back((first.future, first.kind));
        while payloads.len() < IO_BATCH_MAX {
            match rx.try_recv() {
                Ok(req) => {
                    payloads.push(req.payload);
                    kinds.push(req.kind);
                    hints.push(req.hint);
                    target_ids.push(req.target_id);
                    pending_futures.push_back((req.future, req.kind));
                }
                Err(_) => break,
            }
        }

        // Send the whole batch as one writev sequence.
        if let Err(e) = gnitz_core::send_framed_batch(sock_fd, &payloads) {
            Python::with_gil(|py| {
                let exc = GnitzError::new_err(e.to_string())
                    .into_pyobject(py).unwrap().into_any().unbind();
                for (fut, _) in pending_futures.drain(..) {
                    let _ = loop_ref.call_method1(py, "call_soon_threadsafe",
                        (&se_fn, &fut, exc.clone_ref(py)));
                }
            });
            return;
        }

        // Recv all responses for this batch (pure Rust, no GIL).
        // Scan requests use recv_scan_response to collect streaming frames.
        // On transport failure: stop reading and record the error once —
        // remaining futures are failed with the same exception below.
        results.clear();
        let mut recv_err: Option<String> = None;
        for (i, &kind) in kinds.iter().enumerate() {
            let r: Result<LoopResult, String> = match kind {
                ResponseKind::Scan => {
                    let hint = hints[i].take();
                    match recv_scan_response(sock_fd, max_payload_len, hint) {
                        Ok(Ok((schema, schema_version, batch, lsn))) => Ok(LoopResult::Scan(
                            Box::new(ScanData {
                                schema, schema_version, batch, lsn, target_id: target_ids[i],
                            }),
                        )),
                        Ok(Err(err_text)) => Ok(LoopResult::ScanError(err_text)),
                        Err(e) => Err(e),
                    }
                }
                ResponseKind::Push => {
                    gnitz_core::recv_framed(sock_fd, max_payload_len)
                        .map_err(|e| e.to_string())
                        .and_then(|buf| gnitz_core::parse_response(&buf, None)
                            .map_err(|e| e.to_string()))
                        .map(|m| LoopResult::Push(Box::new(m)))
                }
            };
            match r {
                Ok(res) => results.push(res),
                Err(e)  => { recv_err = Some(e); break; }
            }
        }

        // Update the schema cache before acquiring the GIL: the Mutex has no
        // GIL dependency, and holding it inside with_gil would reverse the
        // lock order relative to scan()/seek(), which lock the Mutex then
        // drop it before any GIL-gated code.
        {
            let mut cache = schema_cache.lock().unwrap();
            for result in &results {
                if let LoopResult::Scan(sd) = result {
                    if let Some(s) = &sd.schema {
                        cache.put(sd.target_id, (Arc::clone(s), sd.schema_version));
                    }
                }
            }
        }

        // Single GIL acquisition to resolve all futures.
        let conn_lost = recv_err.is_some();
        Python::with_gil(|py| {
            for result in results.drain(..) {
                let (fut, _kind) = pending_futures.pop_front().unwrap();
                match result {
                    LoopResult::Push(msg) => {
                        let msg = *msg;
                        if msg.status != 0 {
                            let err_text = msg.error_text
                                .filter(|s| !s.is_empty())
                                .unwrap_or_else(|| "server error".into());
                            let exc = GnitzError::new_err(err_text);
                            let _ = loop_ref.call_method1(py, "call_soon_threadsafe",
                                (&se_fn, &fut, exc));
                        } else {
                            let lsn = (msg.seek_pk as u64)
                                .into_pyobject(py).unwrap().into_any().unbind();
                            let _ = loop_ref.call_method1(py, "call_soon_threadsafe",
                                (&sr_fn, &fut, lsn));
                        }
                    }
                    LoopResult::ScanError(err_text) => {
                        let exc = GnitzError::new_err(err_text);
                        let _ = loop_ref.call_method1(py, "call_soon_threadsafe",
                            (&se_fn, &fut, exc));
                    }
                    LoopResult::Scan(sd) => {
                        let ScanData { schema, batch, lsn, .. } = *sd;
                        let data = match schema {
                            Some(s) => {
                                let b = batch.unwrap_or_else(|| ZSetBatch::new(s.as_ref()));
                                Some(make_shared_batch_data(py, s, b).unwrap())
                            }
                            None => None,
                        };
                        let py_val = Py::new(py, PyScanResult {
                            data, lsn, cached_schema: None, cached_batch: None,
                        }).unwrap().into_any();
                        let _ = loop_ref.call_method1(py, "call_soon_threadsafe",
                            (&sr_fn, &fut, py_val));
                    }
                }
            }
            if let Some(e) = recv_err {
                let exc = GnitzError::new_err(e)
                    .into_pyobject(py).unwrap().into_any().unbind();
                for (fut, _) in pending_futures.drain(..) {
                    let _ = loop_ref.call_method1(py, "call_soon_threadsafe",
                        (&se_fn, &fut, exc.clone_ref(py)));
                }
            }
        });

        if conn_lost {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTypeCode>()?;
    m.add_class::<PyColumnDef>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyRow>()?;
    m.add_class::<PyZSetBatch>()?;
    m.add_class::<PyRustBatch>()?;
    m.add_class::<PyScanResult>()?;
    m.add_class::<PyRowIterator>()?;
    m.add_class::<PyGnitzClient>()?;
    m.add_class::<PyExprBuilder>()?;
    m.add_class::<PyExprProgram>()?;
    m.add_class::<PyCircuitBuilder>()?;
    m.add_class::<PyCircuit>()?;
    m.add_class::<PyAsyncTransport>()?;
    m.add("GnitzError", m.py().get_type::<GnitzError>())?;
    Ok(())
}
