use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyDict, PyTuple};

use gnitz_core::{CircuitBuilder, ExprBuilder, ExprProgram, CircuitGraph, GnitzClient};
use gnitz_protocol::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};
use gnitz_sql::{SqlPlanner, SqlResult};

// ---------------------------------------------------------------------------
// GnitzError Python exception
// ---------------------------------------------------------------------------

pyo3::create_exception!(_native, GnitzError, pyo3::exceptions::PyException);

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
    TypeCode::try_from_u64(c.type_code as u64)
        .map(|tc| ColumnDef { name: c.name.clone(), type_code: tc, is_nullable: c.is_nullable })
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
#[pyclass(name = "Schema")]
pub struct PySchema {
    pub(crate) columns:  Py<PyList>,
    pub(crate) pk_index: usize,
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (columns, pk_index = None))]
    pub fn new(columns: Bound<'_, PyList>, pk_index: Option<usize>) -> PyResult<Self> {
        let idx = match pk_index {
            Some(i) => i,
            None => {
                let mut pks = Vec::new();
                for (i, item) in columns.iter().enumerate() {
                    let c: PyRef<'_, PyColumnDef> = item.extract()?;
                    if c.primary_key { pks.push(i); }
                }
                if pks.len() > 1 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Multiple primary_key=True columns; specify pk_index explicitly"));
                }
                pks.first().copied().unwrap_or(0)
            }
        };
        Ok(PySchema { columns: columns.unbind(), pk_index: idx })
    }

    #[getter]
    pub fn columns<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> {
        self.columns.bind(py).clone()
    }

    #[getter]
    pub fn pk_index(&self) -> usize { self.pk_index }

    pub fn __repr__(&self, py: Python<'_>) -> String {
        format!("Schema(pk_index={}, ncols={})", self.pk_index, self.columns.bind(py).len())
    }
}

fn py_schema_to_rust(py: Python<'_>, s: &PySchema) -> PyResult<Schema> {
    let list = s.columns.bind(py);
    let mut cols = Vec::with_capacity(list.len());
    for item in list.iter() {
        let c: PyRef<'_, PyColumnDef> = item.extract()?;
        cols.push(py_col_to_rust(&c)?);
    }
    Ok(Schema { columns: cols, pk_index: s.pk_index })
}

fn rust_schema_to_py(py: Python<'_>, s: &Schema) -> PyResult<Py<PySchema>> {
    let py_cols: Vec<PyObject> = s.columns.iter().enumerate()
        .map(|(i, c)| rust_col_to_py(py, c, i == s.pk_index))
        .collect::<PyResult<_>>()?;
    let list = PyList::new(py, py_cols)?;
    Py::new(py, PySchema { columns: list.unbind(), pk_index: s.pk_index })
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
    pub fn new(py: Python<'_>, fields: Bound<'_, PyTuple>, values: Bound<'_, PyTuple>, weight: i64) -> PyResult<Self> {
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
    pub(crate) schema: Schema,
    pub(crate) batch:  ZSetBatch,
}

fn build_name_to_idx(schema: &Schema) -> HashMap<String, usize> {
    schema.columns.iter().enumerate().map(|(i, c)| (c.name.clone(), i)).collect()
}

/// Private helpers — shared between append_row, append_dict, extend_from_dicts.
impl PyZSetBatch {
    fn append_pk(&mut self, pk_val: &Bound<'_, PyAny>) -> PyResult<()> {
        let pk_idx = self.schema.pk_index;
        if pk_val.is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Missing primary key column {:?}", self.schema.columns[pk_idx].name,
            )));
        }
        if self.schema.columns[pk_idx].type_code == TypeCode::U128 {
            let pk = pk_val.extract::<u128>()?;
            self.batch.pk_lo.push(pk as u64);
            self.batch.pk_hi.push((pk >> 64) as u64);
        } else {
            self.batch.pk_lo.push(pk_val.extract::<u64>()?);
            self.batch.pk_hi.push(0);
        }
        Ok(())
    }

    fn append_column_value(
        &mut self, ci: usize, payload_idx: usize,
        val: &Bound<'_, PyAny>, nulls: &mut u64,
    ) -> PyResult<()> {
        if val.is_none() {
            self.append_null_column(ci, payload_idx, nulls)
        } else {
            match self.schema.columns[ci].type_code {
                TypeCode::String => {
                    if let ColData::Strings(v) = &mut self.batch.columns[ci] {
                        v.push(Some(val.extract::<String>()?));
                    }
                }
                TypeCode::U128 => {
                    if let ColData::U128s(v) = &mut self.batch.columns[ci] {
                        v.push(val.extract::<u128>()?);
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
                buf.extend(std::iter::repeat(0u8).take(stride));
            }
            ColData::Strings(v) => v.push(None),
            ColData::U128s(v) => v.push(0),
        }
        Ok(())
    }

    /// Shared logic for appending one row from a dict.
    fn append_from_dict_inner(
        &mut self,
        dict: &Bound<'_, PyDict>,
        weight: i64,
        name_to_idx: &HashMap<String, usize>,
    ) -> PyResult<()> {
        let pk_idx = self.schema.pk_index;
        let pk_name = &self.schema.columns[pk_idx].name;
        let n_cols = self.schema.columns.len();

        // Extract PK
        let pk_val = dict.get_item(pk_name)?
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!(
                "Missing primary key column {:?}", pk_name,
            )))?;
        self.append_pk(&pk_val)?;
        self.batch.weights.push(weight);

        // Non-PK columns
        let mut nulls: u64 = 0;
        for ci in 0..n_cols {
            if ci == pk_idx { continue; }
            let payload_idx = if ci < pk_idx { ci } else { ci - 1 };
            let col_name = &self.schema.columns[ci].name;
            match dict.get_item(col_name)? {
                Some(val) => self.append_column_value(ci, payload_idx, &val, &mut nulls)?,
                None => self.append_null_column(ci, payload_idx, &mut nulls)?,
            }
        }
        self.batch.nulls.push(nulls);

        // Warn about unknown keys (only if dict has more keys than schema columns)
        let _ = name_to_idx; // used for validation if needed; kept for future use
        Ok(())
    }
}

#[pymethods]
impl PyZSetBatch {
    #[new]
    #[pyo3(signature = (schema))]
    pub fn new(py: Python<'_>, schema: PyRef<'_, PySchema>) -> PyResult<Self> {
        let rust_schema = py_schema_to_rust(py, &schema)?;
        let batch = ZSetBatch::new(&rust_schema);
        Ok(PyZSetBatch { schema: rust_schema, batch })
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
        let pk_idx = self.schema.pk_index;
        let pk_item = values.get_item(pk_idx)?;
        self.append_pk(&pk_item)?;
        self.batch.weights.push(weight);

        let mut nulls: u64 = 0;
        for ci in 0..n_cols {
            if ci == pk_idx { continue; }
            let payload_idx = if ci < pk_idx { ci } else { ci - 1 };
            let val = values.get_item(ci)?;
            self.append_column_value(ci, payload_idx, &val, &mut nulls)?;
        }
        self.batch.nulls.push(nulls);
        Ok(())
    }

    /// Append a row from a Python dict of {column_name: value}.
    #[pyo3(signature = (values, weight = 1))]
    pub fn append_dict(
        &mut self, _py: Python<'_>,
        values: Bound<'_, PyDict>,
        weight: i64,
    ) -> PyResult<()> {
        let name_to_idx = build_name_to_idx(&self.schema);
        self.append_from_dict_inner(&values, weight, &name_to_idx)
    }

    /// Append rows from an iterable of dicts. Processes entire batch in one
    /// Rust call, eliminating per-row Python→Rust boundary crossings.
    #[pyo3(signature = (rows, weight = 1))]
    pub fn extend_from_dicts(
        &mut self, _py: Python<'_>,
        rows: Bound<'_, PyAny>,
        weight: i64,
    ) -> PyResult<()> {
        let name_to_idx = build_name_to_idx(&self.schema);
        for row_item in rows.try_iter()? {
            let row_item = row_item?;
            let dict: &Bound<'_, PyDict> = row_item.downcast()?;
            let row_weight = match dict.get_item("_weight")? {
                Some(w) => w.extract::<i64>()?,
                None => weight,
            };
            self.append_from_dict_inner(dict, row_weight, &name_to_idx)?;
        }
        Ok(())
    }

    // Backward-compat getters — materialize Python lists from Rust Vecs on demand.
    #[getter]
    pub fn pk_lo(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        Ok(PyList::new(py, &self.batch.pk_lo)?.unbind())
    }
    #[getter]
    pub fn pk_hi(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        Ok(PyList::new(py, &self.batch.pk_hi)?.unbind())
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
        rust_batch_columns_to_py(py, &self.schema, &self.batch, self.batch.len())
    }

    pub fn __len__(&self) -> usize { self.batch.len() }

    pub fn __repr__(&self) -> String {
        format!("ZSetBatch(len={})", self.batch.len())
    }
}

// ---------------------------------------------------------------------------
// Batch conversion helpers
// ---------------------------------------------------------------------------

/// Write one fixed-width value as little-endian bytes into buf.
fn write_fixed_le(buf: &mut Vec<u8>, tc: TypeCode, item: &Bound<'_, PyAny>) -> PyResult<()> {
    match tc {
        TypeCode::U8   => buf.push(item.extract::<u8>()?),
        TypeCode::I8   => buf.push(item.extract::<i8>()? as u8),
        TypeCode::U16  => buf.extend_from_slice(&item.extract::<u16>()?.to_le_bytes()),
        TypeCode::I16  => buf.extend_from_slice(&item.extract::<i16>()?.to_le_bytes()),
        TypeCode::U32  => buf.extend_from_slice(&item.extract::<u32>()?.to_le_bytes()),
        TypeCode::I32  => buf.extend_from_slice(&item.extract::<i32>()?.to_le_bytes()),
        TypeCode::F32  => buf.extend_from_slice(&item.extract::<f32>()?.to_le_bytes()),
        TypeCode::U64  => buf.extend_from_slice(&item.extract::<u64>()?.to_le_bytes()),
        TypeCode::I64  => buf.extend_from_slice(&item.extract::<i64>()?.to_le_bytes()),
        TypeCode::F64  => buf.extend_from_slice(&item.extract::<f64>()?.to_le_bytes()),
        TypeCode::String | TypeCode::U128 => unreachable!("handled before write_fixed_le"),
    }
    Ok(())
}

/// Read one fixed-width value as a Python object (integers as int, floats as float).
fn read_fixed_le(py: Python<'_>, tc: TypeCode, slice: &[u8]) -> PyObject {
    match tc {
        TypeCode::U8   => (slice[0] as u64).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::I8   => (slice[0] as i8 as i64).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::U16  => (u16::from_le_bytes(slice.try_into().unwrap()) as u64).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::I16  => (i16::from_le_bytes(slice.try_into().unwrap()) as i64).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::U32  => (u32::from_le_bytes(slice.try_into().unwrap()) as u64).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::I32  => (i32::from_le_bytes(slice.try_into().unwrap()) as i64).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::F32  => (f32::from_le_bytes(slice.try_into().unwrap()) as f64).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::U64  => u64::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::I64  => i64::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::F64  => f64::from_le_bytes(slice.try_into().unwrap()).into_pyobject(py).unwrap().into_any().unbind(),
        TypeCode::String | TypeCode::U128 => unreachable!(),
    }
}

/// Convert a Rust ZSetBatch + Schema into a Python ZSetBatch (used by non-lazy scan/seek).
fn rust_batch_to_py(
    py: Python<'_>,
    schema: &Schema,
    batch:  &ZSetBatch,
) -> PyResult<Py<PyZSetBatch>> {
    Py::new(py, PyZSetBatch {
        schema: schema.clone(),
        batch:  batch.clone(),
    })
}

// ---------------------------------------------------------------------------
// Lazy batch infrastructure (Phase 2)
// ---------------------------------------------------------------------------

struct SharedBatchData {
    schema: Schema,
    batch:  ZSetBatch,
    /// Pre-computed field-name tuple, created once and shared across all iterators.
    fields: Py<PyTuple>,
    /// field name → column index, built once and shared via Arc for O(1) row attr lookup.
    field_index: Arc<HashMap<String, usize>>,
}

fn build_field_index(schema: &Schema) -> Arc<HashMap<String, usize>> {
    Arc::new(schema.columns.iter().enumerate().map(|(i, c)| (c.name.clone(), i)).collect())
}

/// Build Python values for a single row from Rust data, appending to `out`.
fn build_row_values_into(
    py: Python<'_>, schema: &Schema, batch: &ZSetBatch, row: usize,
    out: &mut Vec<PyObject>,
) -> PyResult<()> {
    let pk_idx = schema.pk_index;
    let null_word = batch.nulls[row];
    let mut payload_idx = 0usize;

    for ci in 0..schema.columns.len() {
        if ci == pk_idx {
            let pk_lo = batch.pk_lo[row];
            let pk_hi = batch.pk_hi[row];
            if schema.columns[ci].type_code == TypeCode::U128 {
                let val = (pk_lo as u128) | ((pk_hi as u128) << 64);
                out.push(val.into_pyobject(py)?.into_any().unbind());
            } else {
                out.push(pk_lo.into_pyobject(py)?.into_any().unbind());
            }
        } else {
            if null_word & (1u64 << payload_idx) != 0 {
                out.push(py.None().into());
            } else {
                match &batch.columns[ci] {
                    ColData::Fixed(buf) => {
                        let stride = schema.columns[ci].type_code.wire_stride();
                        out.push(read_fixed_le(
                            py, schema.columns[ci].type_code,
                            &buf[row * stride..(row + 1) * stride],
                        ));
                    }
                    ColData::Strings(v) => match &v[row] {
                        Some(s) => out.push(s.into_pyobject(py)?.into_any().unbind()),
                        None    => out.push(py.None().into()),
                    },
                    ColData::U128s(v) => {
                        out.push(v[row].into_pyobject(py)?.into_any().unbind());
                    }
                }
            }
            payload_idx += 1;
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
    cached_pk_lo:   Option<Py<PyList>>,
    cached_pk_hi:   Option<Py<PyList>>,
    cached_weights: Option<Py<PyList>>,
    cached_nulls:   Option<Py<PyList>>,
    cached_columns: Option<Py<PyList>>,
}

#[pymethods]
impl PyRustBatch {
    #[getter]
    fn pk_lo(&mut self, py: Python<'_>) -> PyResult<Py<PyList>> {
        if let Some(ref cached) = self.cached_pk_lo {
            return Ok(cached.clone_ref(py));
        }
        let list = PyList::new(py, &self.data.batch.pk_lo)?.unbind();
        self.cached_pk_lo = Some(list.clone_ref(py));
        Ok(list)
    }

    #[getter]
    fn pk_hi(&mut self, py: Python<'_>) -> PyResult<Py<PyList>> {
        if let Some(ref cached) = self.cached_pk_hi {
            return Ok(cached.clone_ref(py));
        }
        let list = PyList::new(py, &self.data.batch.pk_hi)?.unbind();
        self.cached_pk_hi = Some(list.clone_ref(py));
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
        let schema = &self.data.schema;
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
        if ci == schema.pk_index {
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
                        None    => Ok(py.None().into()),
                    })
                    .collect::<PyResult<_>>()?;
                col_lists.push(PyList::new(py, items)?.into_any().unbind());
            }
            ColData::U128s(v) => {
                let items: Vec<PyObject> = v.iter()
                    .map(|&x| Ok(x.into_pyobject(py)?.into_any().unbind()))
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
            None => py.None().into(),
            Some(d) => rust_schema_to_py(py, &d.schema)?.into_any(),
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
            None => py.None().into(),
            Some(d) => {
                let rb = Py::new(py, PyRustBatch {
                    data: Arc::clone(d),
                    cached_pk_lo:   None,
                    cached_pk_hi:   None,
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

    fn __iter__(&self, py: Python<'_>) -> PyResult<PyRowIterator> {
        let (data_clone, fields, field_index, len) = match &self.data {
            None => (None, PyTuple::empty(py).unbind(), Arc::new(HashMap::new()), 0),
            Some(d) => (Some(Arc::clone(d)), d.fields.clone_ref(py), Arc::clone(&d.field_index), d.batch.len()),
        };
        Ok(PyRowIterator { data: data_clone, fields, field_index, row_buf: Vec::new(), pos: 0, len })
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
            None => Ok(py.None().into()),
        }
    }

    fn one(&self, py: Python<'_>) -> PyResult<PyObject> {
        let n = self.__len__();
        if n != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Expected exactly 1 row, got {}", n),
            ));
        }
        let mut iter = self.__iter__(py)?;
        Ok(iter.__next__(py)?.unwrap())
    }

    fn one_or_none(&self, py: Python<'_>) -> PyResult<PyObject> {
        let n = self.__len__();
        if n > 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Expected at most 1 row, got {}", n),
            ));
        }
        let mut iter = self.__iter__(py)?;
        match iter.__next__(py)? {
            Some(row) => Ok(row),
            None => Ok(py.None().into()),
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
            build_row_values_into(py, &data.schema, &data.batch, i, &mut row_buf)?;
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

        // Specialize by column kind to avoid per-row dispatch through read_row_value.
        let pk_idx = data.schema.pk_index;
        if col_idx == pk_idx {
            // PK column: read directly from pk_lo (bulk conversion for non-U128)
            if data.schema.columns[col_idx].type_code == TypeCode::U128 {
                let items: Vec<PyObject> = (0..n)
                    .map(|i| {
                        let v = (data.batch.pk_lo[i] as u128) | ((data.batch.pk_hi[i] as u128) << 64);
                        Ok(v.into_pyobject(py)?.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                return Ok(PyList::new(py, items)?.unbind());
            }
            return Ok(PyList::new(py, &data.batch.pk_lo)?.unbind());
        }

        let payload_idx = if col_idx < pk_idx { col_idx } else { col_idx - 1 };
        let nulls = &data.batch.nulls;
        let null_bit = 1u64 << payload_idx;

        match &data.batch.columns[col_idx] {
            ColData::Fixed(buf) => {
                let tc = data.schema.columns[col_idx].type_code;
                let stride = tc.wire_stride();
                let items: Vec<PyObject> = (0..n)
                    .map(|i| {
                        if nulls[i] & null_bit != 0 {
                            py.None().into()
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
                            return Ok(py.None().into());
                        }
                        match s {
                            Some(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
                            None    => Ok(py.None().into()),
                        }
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, items)?.unbind())
            }
            ColData::U128s(v) => {
                let items: Vec<PyObject> = v.iter().enumerate()
                    .map(|(i, &x)| {
                        if nulls[i] & null_bit != 0 {
                            return Ok(py.None().into());
                        }
                        Ok(x.into_pyobject(py)?.into_any().unbind())
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
    fields:      Py<PyTuple>,
    field_index: Arc<HashMap<String, usize>>,
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
        build_row_values_into(py, &data.schema, &data.batch, self.pos, &mut self.row_buf)?;
        let weight = data.batch.weights[self.pos];
        self.pos += 1;

        let values_tuple = PyTuple::new(py, &self.row_buf)?;
        let row = Py::new(py, PyRow {
            fields: self.fields.clone_ref(py),
            values: values_tuple.unbind(),
            weight,
            field_index: Arc::clone(&self.field_index),
        })?;
        Ok(Some(row.into_any()))
    }
}

// ---------------------------------------------------------------------------
// GnitzClient
// ---------------------------------------------------------------------------

/// Shared helper: convert a (Option<Schema>, Option<ZSetBatch>, u64) response into a Python tuple.
fn response_to_py_tuple(
    py: Python<'_>,
    result: Result<(Option<Schema>, Option<ZSetBatch>, u64), gnitz_core::ClientError>,
) -> PyResult<PyObject> {
    let (opt_schema, opt_batch, view_lsn) = result
        .map_err(|e| GnitzError::new_err(e.to_string()))?;
    let (py_schema, py_batch) = match (opt_schema, opt_batch) {
        (Some(s), Some(b)) => {
            let ps = rust_schema_to_py(py, &s)?.into_any();
            let pb = rust_batch_to_py(py, &s, &b)?.into_any();
            (ps, pb)
        }
        (Some(s), None) => {
            let ps = rust_schema_to_py(py, &s)?.into_any();
            (ps, py.None())
        }
        _ => (py.None(), py.None()),
    };
    let lsn_obj = view_lsn.into_pyobject(py)?.into_any().unbind();
    Ok(PyTuple::new(py, [py_schema, py_batch, lsn_obj])?.into_any().unbind())
}

/// Shared helper: wrap a (Option<Schema>, Option<ZSetBatch>, u64) into a lazy PyScanResult.
fn response_to_lazy(
    py: Python<'_>,
    result: Result<(Option<Schema>, Option<ZSetBatch>, u64), gnitz_core::ClientError>,
) -> PyResult<Py<PyScanResult>> {
    let (opt_schema, opt_batch, view_lsn) = result
        .map_err(|e| GnitzError::new_err(e.to_string()))?;
    let data = match (opt_schema, opt_batch) {
        (Some(s), Some(b)) => {
            let names: Vec<&str> = s.columns.iter().map(|c| c.name.as_str()).collect();
            let fields = PyTuple::new(py, names)?.unbind();
            let field_index = build_field_index(&s);
            Some(Arc::new(SharedBatchData { schema: s, batch: b, fields, field_index }))
        }
        _ => None,
    };
    Py::new(py, PyScanResult { data, lsn: view_lsn, cached_schema: None, cached_batch: None })
}

/// Macro to borrow the live inner client or raise GnitzError.
macro_rules! client {
    ($self:expr) => {
        $self.inner.as_ref()
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
        GnitzClient::connect(socket_path)
            .map(|c| PyGnitzClient { inner: Some(c) })
            .map_err(|e| GnitzError::new_err(e.to_string()))
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

    pub fn create_schema(&self, name: &str) -> PyResult<u64> {
        client!(self).create_schema(name).map_err(|e| GnitzError::new_err(e.to_string()))
    }

    pub fn drop_schema(&self, name: &str) -> PyResult<()> {
        client!(self).drop_schema(name).map_err(|e| GnitzError::new_err(e.to_string()))
    }

    #[pyo3(signature = (schema_name, table_name, columns, pk_col_idx = 0, unique_pk = true))]
    pub fn create_table(
        &self, _py: Python<'_>,
        schema_name: &str, table_name: &str,
        columns: Bound<'_, PyList>,
        pk_col_idx: usize,
        unique_pk: bool,
    ) -> PyResult<u64> {
        let cols: Vec<ColumnDef> = columns.iter()
            .map(|item| { let c: PyRef<'_, PyColumnDef> = item.extract()?; py_col_to_rust(&c) })
            .collect::<PyResult<_>>()?;
        client!(self).create_table(schema_name, table_name, &cols, pk_col_idx, unique_pk)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> PyResult<()> {
        client!(self).drop_table(schema_name, table_name)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    // ----- DML -----

    /// push(target_id, batch) -> ingest_lsn: int
    pub fn push(
        &self, _py: Python<'_>,
        target_id: u64,
        batch: PyRef<'_, PyZSetBatch>,
    ) -> PyResult<u64> {
        client!(self).push(target_id, &batch.schema, &batch.batch)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// scan(target_id) -> (Schema | None, ZSetBatch | None, view_lsn: int)
    pub fn scan(&self, py: Python<'_>, target_id: u64) -> PyResult<PyObject> {
        response_to_py_tuple(py, client!(self).scan(target_id))
    }

    /// delete(target_id, schema, pks: list[int]) — pks are full U128 primary keys.
    pub fn delete(
        &self, py: Python<'_>,
        target_id: u64,
        schema: PyRef<'_, PySchema>,
        pks: Vec<u128>,
    ) -> PyResult<()> {
        let rust_schema = py_schema_to_rust(py, &schema)?;
        let pk_pairs: Vec<(u64, u64)> = pks.into_iter()
            .map(|pk| (pk as u64, (pk >> 64) as u64))
            .collect();
        client!(self).delete(target_id, &rust_schema, &pk_pairs)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    // ----- Views -----

    pub fn create_view(
        &self, py: Python<'_>,
        schema_name: &str, view_name: &str,
        source_table_id: u64,
        output_schema: PyRef<'_, PySchema>,
    ) -> PyResult<u64> {
        let rust_schema = py_schema_to_rust(py, &output_schema)?;
        client!(self).create_view(schema_name, view_name, source_table_id, &rust_schema.columns)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// create_view_with_circuit — CONSUMES circuit.
    pub fn create_view_with_circuit(
        &self, py: Python<'_>,
        schema_name: &str, view_name: &str,
        mut circuit: PyRefMut<'_, PyCircuitGraph>,
        output_schema: PyRef<'_, PySchema>,
    ) -> PyResult<u64> {
        let graph = circuit.inner.take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("CircuitGraph already consumed"))?;
        let rust_schema = py_schema_to_rust(py, &output_schema)?;
        client!(self).create_view_with_circuit(schema_name, view_name, "", graph, &rust_schema.columns)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    pub fn drop_view(&self, schema_name: &str, view_name: &str) -> PyResult<()> {
        client!(self).drop_view(schema_name, view_name)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// resolve_table_id(schema_name, table_name) -> (tid: int, schema: Schema)
    pub fn resolve_table_id(
        &self, py: Python<'_>,
        schema_name: &str, table_name: &str,
    ) -> PyResult<PyObject> {
        let (tid, schema) = client!(self).resolve_table_or_view_id(schema_name, table_name)
            .map_err(|e| GnitzError::new_err(e.to_string()))?;
        let py_schema = rust_schema_to_py(py, &schema)?.into_any();
        let tid_obj   = tid.into_pyobject(py)?.into_any().unbind();
        Ok(PyTuple::new(py, [tid_obj, py_schema])?.into_any().unbind())
    }

    /// allocate_table_id() — name matches py_client API
    pub fn allocate_table_id(&self) -> PyResult<u64> {
        client!(self).alloc_table_id().map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// allocate_schema_id()
    pub fn allocate_schema_id(&self) -> PyResult<u64> {
        client!(self).alloc_schema_id().map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// seek_by_index(table_id, col_idx, key_lo, key_hi) -> (Schema | None, ZSetBatch | None, view_lsn: int)
    pub fn seek_by_index(
        &self, py: Python<'_>, table_id: u64, col_idx: u64,
        key_lo: u64, key_hi: u64,
    ) -> PyResult<PyObject> {
        response_to_py_tuple(py, client!(self).seek_by_index(table_id, col_idx, key_lo, key_hi))
    }

    /// seek(table_id, pk_lo, pk_hi) -> (Schema | None, ZSetBatch | None, view_lsn: int)
    pub fn seek(&self, py: Python<'_>, table_id: u64, pk_lo: u64, pk_hi: u64) -> PyResult<PyObject> {
        response_to_py_tuple(py, client!(self).seek(table_id, pk_lo, pk_hi))
    }

    // ----- Lazy scan/seek (Phase 2) — skip rust_batch_to_py entirely -----

    /// scan_lazy(target_id) -> ScanResult (native)
    pub fn scan_lazy(&self, py: Python<'_>, target_id: u64) -> PyResult<Py<PyScanResult>> {
        response_to_lazy(py, client!(self).scan(target_id))
    }

    /// seek_lazy(table_id, pk_lo, pk_hi) -> ScanResult (native)
    pub fn seek_lazy(&self, py: Python<'_>, table_id: u64, pk_lo: u64, pk_hi: u64) -> PyResult<Py<PyScanResult>> {
        response_to_lazy(py, client!(self).seek(table_id, pk_lo, pk_hi))
    }

    /// seek_by_index_lazy(table_id, col_idx, key_lo, key_hi) -> ScanResult (native)
    pub fn seek_by_index_lazy(
        &self, py: Python<'_>, table_id: u64, col_idx: u64,
        key_lo: u64, key_hi: u64,
    ) -> PyResult<Py<PyScanResult>> {
        response_to_lazy(py, client!(self).seek_by_index(table_id, col_idx, key_lo, key_hi))
    }

    /// execute_sql(schema_name, sql) -> list of result dicts
    pub fn execute_sql(&self, py: Python<'_>, schema_name: &str, sql: &str) -> PyResult<PyObject> {
        let client_ref = client!(self);
        let planner = SqlPlanner::new(client_ref, schema_name);
        let results = planner.execute(sql)
            .map_err(|e| GnitzError::new_err(e.to_string()))?;

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
                    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
                    let fields = PyTuple::new(py, names)?.unbind();
                    let field_index = build_field_index(&schema);
                    let data = Arc::new(SharedBatchData { schema, batch, fields, field_index });
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

    #[pyo3(signature = (input, worker_id = 0))]
    pub fn gather(&mut self, input: u64, worker_id: u64) -> PyResult<u64> {
        Ok(circuit_builder!(self).gather(input, worker_id))
    }

    pub fn sink(&mut self, input: u64) -> PyResult<u64> {
        Ok(circuit_builder!(self).sink(input))
    }

    pub fn map_expr(&mut self, input: u64, expr: PyRef<'_, PyExprProgram>) -> PyResult<u64> {
        Ok(circuit_builder!(self).map_expr(input, expr.inner.clone()))
    }

    /// Consume builder and produce a CircuitGraph for create_view_with_circuit.
    pub fn build(&mut self) -> PyResult<PyCircuitGraph> {
        let b = self.inner.take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("CircuitBuilder already consumed"))?;
        Ok(PyCircuitGraph { inner: Some(b.build()) })
    }
}

#[pyclass(name = "CircuitGraph")]
pub struct PyCircuitGraph {
    pub(crate) inner: Option<CircuitGraph>,
}

#[pymethods]
impl PyCircuitGraph {
    pub fn __repr__(&self) -> String {
        match &self.inner {
            Some(g) => format!("CircuitGraph(view_id={}, nodes={})", g.view_id, g.nodes.len()),
            None    => "CircuitGraph(consumed)".to_string(),
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
    m.add_class::<PyCircuitGraph>()?;
    m.add("GnitzError", m.py().get_type::<GnitzError>())?;
    Ok(())
}
