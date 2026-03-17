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
}

#[pymethods]
impl PyColumnDef {
    #[new]
    #[pyo3(signature = (name, type_code, is_nullable = false))]
    pub fn new(name: String, type_code: u32, is_nullable: bool) -> Self {
        PyColumnDef { name, type_code, is_nullable }
    }

    pub fn __repr__(&self) -> String {
        format!("ColumnDef(name={:?}, type_code={}, is_nullable={})",
                self.name, self.type_code, self.is_nullable)
    }
}

fn py_col_to_rust(c: &PyColumnDef) -> PyResult<ColumnDef> {
    TypeCode::try_from_u64(c.type_code as u64)
        .map(|tc| ColumnDef { name: c.name.clone(), type_code: tc, is_nullable: c.is_nullable })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn rust_col_to_py(py: Python<'_>, c: &ColumnDef) -> PyResult<PyObject> {
    Ok(Py::new(py, PyColumnDef {
        name:        c.name.clone(),
        type_code:   c.type_code as u32,
        is_nullable: c.is_nullable,
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
    #[pyo3(signature = (columns, pk_index = 0))]
    pub fn new(columns: Bound<'_, PyList>, pk_index: usize) -> Self {
        PySchema { columns: columns.unbind(), pk_index }
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
    let py_cols: Vec<PyObject> = s.columns.iter()
        .map(|c| rust_col_to_py(py, c))
        .collect::<PyResult<_>>()?;
    let list = PyList::new(py, py_cols)?;
    Py::new(py, PySchema { columns: list.unbind(), pk_index: s.pk_index })
}

// ---------------------------------------------------------------------------
// ZSetBatch
// ---------------------------------------------------------------------------

/// All five fields are Python lists so Python can mutate them in-place,
/// matching py_client's `batch.pk_lo.append(...)` pattern.
#[pyclass(name = "ZSetBatch")]
pub struct PyZSetBatch {
    pub(crate) pk_lo:   Py<PyList>,
    pub(crate) pk_hi:   Py<PyList>,
    pub(crate) weights: Py<PyList>,
    pub(crate) nulls:   Py<PyList>,
    pub(crate) columns: Py<PyList>,
}

#[pymethods]
impl PyZSetBatch {
    /// ZSetBatch(schema)  — pre-populates columns with one empty list per column.
    #[new]
    #[pyo3(signature = (schema))]
    pub fn new(py: Python<'_>, schema: PyRef<'_, PySchema>) -> PyResult<Self> {
        let n_cols = schema.columns.bind(py).len();
        let col_lists: Vec<PyObject> = (0..n_cols)
            .map(|_| Ok(PyList::empty(py).unbind().into_any()))
            .collect::<PyResult<_>>()?;
        let columns = PyList::new(py, col_lists)?;
        Ok(PyZSetBatch {
            pk_lo:   PyList::empty(py).unbind(),
            pk_hi:   PyList::empty(py).unbind(),
            weights: PyList::empty(py).unbind(),
            nulls:   PyList::empty(py).unbind(),
            columns: columns.unbind(),
        })
    }

    #[getter]
    pub fn pk_lo<'py>(&self, py: Python<'py>)   -> Bound<'py, PyList> { self.pk_lo.bind(py).clone()   }
    #[getter]
    pub fn pk_hi<'py>(&self, py: Python<'py>)   -> Bound<'py, PyList> { self.pk_hi.bind(py).clone()   }
    #[getter]
    pub fn weights<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> { self.weights.bind(py).clone() }
    #[getter]
    pub fn nulls<'py>(&self, py: Python<'py>)   -> Bound<'py, PyList> { self.nulls.bind(py).clone()   }
    #[getter]
    pub fn columns<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> { self.columns.bind(py).clone() }

    #[setter]
    pub fn set_columns(&mut self, val: Bound<'_, PyList>) {
        self.columns = val.unbind();
    }

    pub fn __len__(&self, py: Python<'_>) -> usize { self.pk_lo.bind(py).len() }

    pub fn __repr__(&self, py: Python<'_>) -> String {
        format!("ZSetBatch(len={})", self.pk_lo.bind(py).len())
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

/// Convert a Python ZSetBatch + Schema into a Rust ZSetBatch.
/// The schema is taken from the `schema` parameter (same as py_client: push takes schema separately).
fn py_batch_to_rust(
    py: Python<'_>,
    py_schema: &PySchema,
    py_batch:  &PyZSetBatch,
) -> PyResult<(Schema, ZSetBatch)> {
    let schema = py_schema_to_rust(py, py_schema)?;

    let pk_lo: Vec<u64> = py_batch.pk_lo.bind(py).iter()
        .map(|x| x.extract::<u64>()).collect::<PyResult<_>>()?;
    let pk_hi: Vec<u64> = py_batch.pk_hi.bind(py).iter()
        .map(|x| x.extract::<u64>()).collect::<PyResult<_>>()?;
    let weights: Vec<i64> = py_batch.weights.bind(py).iter()
        .map(|x| x.extract::<i64>()).collect::<PyResult<_>>()?;
    let nulls: Vec<u64> = py_batch.nulls.bind(py).iter()
        .map(|x| x.extract::<u64>()).collect::<PyResult<_>>()?;

    let n = pk_lo.len();
    let col_list = py_batch.columns.bind(py);
    let mut columns = Vec::with_capacity(schema.columns.len());

    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            columns.push(ColData::Fixed(vec![]));
            continue;
        }
        let inner: Bound<'_, PyList> = col_list.get_item(ci)?
            .downcast_into()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err(
                format!("columns[{ci}] is not a list")))?;

        match col_def.type_code {
            TypeCode::String => {
                let mut v = Vec::with_capacity(inner.len());
                for item in inner.iter() {
                    if item.is_none() {
                        v.push(None);
                    } else {
                        v.push(Some(item.extract::<String>()?));
                    }
                }
                columns.push(ColData::Strings(v));
            }
            TypeCode::U128 => {
                let v: Vec<u128> = inner.iter()
                    .map(|x| x.extract::<u128>()).collect::<PyResult<_>>()?;
                columns.push(ColData::U128s(v));
            }
            _ => {
                let stride = col_def.type_code.wire_stride();
                let mut buf = Vec::with_capacity(n * stride);
                for item in inner.iter() {
                    write_fixed_le(&mut buf, col_def.type_code, &item)?;
                }
                columns.push(ColData::Fixed(buf));
            }
        }
    }

    Ok((schema, ZSetBatch { pk_lo, pk_hi, weights, nulls, columns }))
}

/// Convert a Rust ZSetBatch + Schema into a Python ZSetBatch.
fn rust_batch_to_py(
    py: Python<'_>,
    schema: &Schema,
    batch:  &ZSetBatch,
) -> PyResult<Py<PyZSetBatch>> {
    let n = batch.pk_lo.len();

    let pk_lo   = PyList::new(py, &batch.pk_lo)?;
    let pk_hi   = PyList::new(py, &batch.pk_hi)?;
    let weights = PyList::new(py, &batch.weights)?;
    let nulls   = PyList::new(py, &batch.nulls)?;

    let mut col_lists: Vec<PyObject> = Vec::with_capacity(schema.columns.len());
    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            col_lists.push(PyList::empty(py).into_any().unbind());
            continue;
        }
        match &batch.columns[ci] {
            ColData::Fixed(buf) => {
                let stride = col_def.type_code.wire_stride();
                let list = PyList::empty(py);
                for i in 0..n {
                    let val = read_fixed_le(py, col_def.type_code, &buf[i*stride..(i+1)*stride]);
                    list.append(val)?;
                }
                col_lists.push(list.into_any().unbind());
            }
            ColData::Strings(v) => {
                let list = PyList::empty(py);
                for s in v {
                    match s {
                        Some(s) => list.append(s.into_pyobject(py)?)?,
                        None    => list.append(py.None())?,
                    }
                }
                col_lists.push(list.into_any().unbind());
            }
            ColData::U128s(v) => {
                let list = PyList::empty(py);
                for &x in v {
                    list.append(x.into_pyobject(py)?)?;
                }
                col_lists.push(list.into_any().unbind());
            }
        }
    }

    let columns = PyList::new(py, col_lists)?;
    Py::new(py, PyZSetBatch {
        pk_lo:   pk_lo.unbind(),
        pk_hi:   pk_hi.unbind(),
        weights: weights.unbind(),
        nulls:   nulls.unbind(),
        columns: columns.unbind(),
    })
}

// ---------------------------------------------------------------------------
// GnitzClient
// ---------------------------------------------------------------------------

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

    /// push(target_id, schema, batch)
    pub fn push(
        &self, py: Python<'_>,
        target_id: u64,
        schema: PyRef<'_, PySchema>,
        batch:  PyRef<'_, PyZSetBatch>,
    ) -> PyResult<()> {
        let (rust_schema, rust_batch) = py_batch_to_rust(py, &schema, &batch)?;
        client!(self).push(target_id, &rust_schema, &rust_batch)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// scan(target_id) -> (Schema | None, ZSetBatch | None)
    pub fn scan(&self, py: Python<'_>, target_id: u64) -> PyResult<PyObject> {
        match client!(self).scan(target_id) {
            Ok((opt_schema, opt_batch)) => {
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
                Ok(PyTuple::new(py, [py_schema, py_batch])?.into_any().unbind())
            }
            Err(e) => Err(GnitzError::new_err(e.to_string())),
        }
    }

    /// delete(target_id, schema, pks: list[int]) — pks are pk_lo values; pk_hi assumed 0.
    pub fn delete(
        &self, py: Python<'_>,
        target_id: u64,
        schema: PyRef<'_, PySchema>,
        pks: Vec<u64>,
    ) -> PyResult<()> {
        let rust_schema = py_schema_to_rust(py, &schema)?;
        let pk_pairs: Vec<(u64, u64)> = pks.into_iter().map(|pk| (pk, 0u64)).collect();
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

    /// seek_by_index(table_id, col_idx, key_lo, key_hi) -> (Schema | None, ZSetBatch | None)
    pub fn seek_by_index(
        &self, py: Python<'_>, table_id: u64, col_idx: u64,
        key_lo: u64, key_hi: u64,
    ) -> PyResult<PyObject> {
        match client!(self).seek_by_index(table_id, col_idx, key_lo, key_hi) {
            Ok((opt_schema, opt_batch)) => {
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
                Ok(PyTuple::new(py, [py_schema, py_batch])?.into_any().unbind())
            }
            Err(e) => Err(GnitzError::new_err(e.to_string())),
        }
    }

    /// seek(table_id, pk_lo, pk_hi) -> (Schema | None, ZSetBatch | None)
    pub fn seek(&self, py: Python<'_>, table_id: u64, pk_lo: u64, pk_hi: u64) -> PyResult<PyObject> {
        match client!(self).seek(table_id, pk_lo, pk_hi) {
            Ok((opt_schema, opt_batch)) => {
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
                Ok(PyTuple::new(py, [py_schema, py_batch])?.into_any().unbind())
            }
            Err(e) => Err(GnitzError::new_err(e.to_string())),
        }
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
                    let py_schema = rust_schema_to_py(py, &schema)?.into_any();
                    let py_batch  = rust_batch_to_py(py, &schema, &batch)?.into_any();
                    d.set_item("schema", py_schema)?;
                    d.set_item("batch", py_batch)?;
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
        Ok(PyExprProgram { inner: Some(b.build(result_reg)) })
    }
}

#[pyclass(name = "ExprProgram")]
pub struct PyExprProgram {
    pub(crate) inner: Option<ExprProgram>,
}

#[pymethods]
impl PyExprProgram {
    pub fn __repr__(&self) -> String {
        match &self.inner {
            Some(p) => format!("ExprProgram(num_regs={}, result_reg={})", p.num_regs, p.result_reg),
            None    => "ExprProgram(consumed)".to_string(),
        }
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
        let expr_opt = match expr {
            Some(e) => Some(
                e.inner.as_ref()
                    .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("ExprProgram consumed"))?
                    .clone()
            ),
            None => None,
        };
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

    /// sink(input, target_table_id) — target_table_id is informational only.
    pub fn sink(&mut self, input: u64, target_table_id: u64) -> PyResult<u64> {
        Ok(circuit_builder!(self).sink(input, target_table_id))
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
    m.add_class::<PyZSetBatch>()?;
    m.add_class::<PyGnitzClient>()?;
    m.add_class::<PyExprBuilder>()?;
    m.add_class::<PyExprProgram>()?;
    m.add_class::<PyCircuitBuilder>()?;
    m.add_class::<PyCircuitGraph>()?;
    m.add("GnitzError", m.py().get_type::<GnitzError>())?;
    Ok(())
}
