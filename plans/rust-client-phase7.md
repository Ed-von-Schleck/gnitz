# Plan: Phase 7 — `gnitz-py`: PyO3 Python Bindings

## Context

Phases 1–6 produced `gnitz-protocol` (wire codec), `gnitz-core` (`GnitzClient`,
`ExprBuilder`, `CircuitBuilder`, 16 integration tests), and `gnitz-capi` (C FFI).
Phase 7 wraps `gnitz-core` in a PyO3 extension module named `gnitz`, providing a
Python API equivalent to `py_client/gnitz_client`.

Reference: `py_client/gnitz_client/` is the authoritative Python implementation.
Its `tests/` suite is the correctness oracle — every test that passes against
`gnitz_client` must pass against `gnitz-py` (modulo minor API differences listed
in §API Differences).

---

## Files

```
gnitz-core/src/types.rs              — modify: #[derive(Clone)] on CircuitGraph
gnitz-core/src/expr.rs               — modify: #[derive(Clone)] on ExprProgram
gnitz-py/Cargo.toml                  — modify: add gnitz-protocol dep + integration feature
gnitz-py/pyproject.toml              — modify: add dev extras for pytest
gnitz-py/src/lib.rs                  — replace stub: full ~700 LOC implementation
gnitz-py/tests/conftest.py           — new: server/client pytest fixtures
gnitz-py/tests/test_connect.py       — new
gnitz-py/tests/test_ddl.py           — new
gnitz-py/tests/test_dml.py           — new
gnitz-py/tests/test_views.py         — new
gnitz-py/tests/test_dbsp_ops.py      — new
gnitz-py/tests/test_workers.py       — new
py_client/gnitz_client_rs/__init__.py — new: compatibility shim
py_client/gnitz_client_rs/batch.py   — new: re-export ZSetBatch
py_client/gnitz_client_rs/types.py   — new: re-export type classes
py_client/gnitz_client_rs/circuit.py — new: re-export CircuitBuilder
py_client/gnitz_client_rs/expr.py    — new: re-export ExprBuilder
```

---

## Prerequisite Changes

### 1. `gnitz-core/src/types.rs` — add Clone to CircuitGraph

`CircuitGraph` is consumed by `create_view_with_circuit`. In PyO3 we wrap it in
`Option<CircuitGraph>` and take it out on use; `Clone` isn't strictly required for
correctness but enables simpler error paths. All fields are Clone (Vec of tuples of u64).

```rust
#[derive(Clone, Debug)]
pub struct CircuitGraph { ... }
```

### 2. `gnitz-core/src/expr.rs` — add Clone to ExprProgram

`ExprProgram` is passed to `CircuitBuilder::filter`. In PyO3, Python holds a reference
to `PyExprProgram`; we must clone the inner `ExprProgram` into `filter()` rather than
move it, because Python's GC doesn't let us move-out of a live Python object.

```rust
#[derive(Clone, Debug)]
pub struct ExprProgram { pub num_regs: u32, pub result_reg: u32, pub code: Vec<u32> }
```

---

## Change 1: `gnitz-py/Cargo.toml`

```toml
[package]
name    = "gnitz-py"
version = "0.1.0"
edition = "2021"

[lib]
name       = "gnitz"
crate-type = ["cdylib"]

[features]
default     = []
integration = ["gnitz-core/integration"]

[dependencies]
pyo3           = { version = "0.23", features = ["extension-module", "abi3-py310"] }
gnitz-core     = { path = "../gnitz-core" }
gnitz-protocol = { path = "../gnitz-protocol" }
```

`gnitz-protocol` must be a **direct** dependency (not transitive through gnitz-core)
because we import `ColData`, `ColumnDef`, `Schema`, `TypeCode`, `ZSetBatch` directly.

---

## Change 2: `gnitz-py/pyproject.toml`

```toml
[build-system]
requires      = ["maturin>=1.0,<2.0"]
build-backend = "maturin"

[project]
name             = "gnitz"
requires-python  = ">= 3.10"
classifiers      = [
    "Programming Language :: Rust",
    "Programming Language :: Python :: Implementation :: CPython",
]

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[tool.maturin]
features    = ["pyo3/extension-module"]
module-name = "gnitz"
```

---

## Change 3: `gnitz-py/src/lib.rs` — Full Implementation (~700 LOC)

Split into four internal modules: `types`, `client`, `builders`. All registered from
the top-level `#[pymodule]`.

```
gnitz-py/src/
├── lib.rs        — module entry-point + GnitzError exception
├── types.rs      — TypeCode, ColumnDef, Schema, ZSetBatch + conversion helpers
├── client.rs     — GnitzClient
└── builders.rs   — ExprBuilder, ExprProgram, CircuitBuilder, CircuitGraph
```

---

### 3a. `src/lib.rs` — Module entry-point

```rust
mod types;
mod client;
mod builders;

use pyo3::prelude::*;

pyo3::create_exception!(gnitz, GnitzError, pyo3::exceptions::PyException);

#[pymodule]
fn gnitz(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<types::PyTypeCode>()?;
    m.add_class::<types::PyColumnDef>()?;
    m.add_class::<types::PySchema>()?;
    m.add_class::<types::PyZSetBatch>()?;
    m.add_class::<client::PyGnitzClient>()?;
    m.add_class::<builders::PyExprBuilder>()?;
    m.add_class::<builders::PyExprProgram>()?;
    m.add_class::<builders::PyCircuitBuilder>()?;
    m.add_class::<builders::PyCircuitGraph>()?;
    m.add("GnitzError", m.py().get_type_bound::<GnitzError>())?;
    Ok(())
}
```

`GnitzError` is the single exception type. Every `ClientError` is converted with
`GnitzError::new_err(e.to_string())`. The exception is accessible as `gnitz.GnitzError`.

---

### 3b. `src/types.rs`

#### `TypeCode` — class-level integer constants

```rust
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
```

Python usage: `TypeCode.U64`, `TypeCode.I64`, etc. — identical to the Python client.

#### `ColumnDef`

```rust
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
```

`get_all, set_all` makes `name`, `type_code`, `is_nullable` Python-readable/writable.
Default `is_nullable=false` matches the Python dataclass default.

#### Rust ↔ Python ColumnDef conversions (module-private helpers)

```rust
pub fn py_col_to_rust(c: &PyColumnDef) -> ProtocolResult<ColumnDef> {
    Ok(ColumnDef {
        name:        c.name.clone(),
        type_code:   TypeCode::try_from_u64(c.type_code as u64)?,
        is_nullable: c.is_nullable,
    })
}

pub fn rust_col_to_py(py: Python, c: &ColumnDef) -> PyResult<Py<PyColumnDef>> {
    Py::new(py, PyColumnDef {
        name:        c.name.clone(),
        type_code:   c.type_code as u32,
        is_nullable: c.is_nullable,
    })
}
```

#### `Schema`

Stores `columns` as a Python list of `PyColumnDef` objects so Python code can access
`schema.columns[i].type_code` etc. without copying.

```rust
#[pyclass(name = "Schema")]
pub struct PySchema {
    pub(crate) columns:  Py<PyList>,   // list[PyColumnDef]
    pub(crate) pk_index: usize,
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (columns, pk_index = 0))]
    pub fn new(py: Python, columns: Bound<PyList>, pk_index: usize) -> PyResult<Self> {
        Ok(PySchema { columns: columns.unbind(), pk_index })
    }

    #[getter] pub fn columns(&self, py: Python) -> Py<PyList> { self.columns.clone_ref(py) }
    #[getter] pub fn pk_index(&self) -> usize                 { self.pk_index }

    pub fn __repr__(&self, py: Python) -> String {
        format!("Schema(pk_index={}, ncols={})",
                self.pk_index, self.columns.bind(py).len())
    }
}
```

#### Rust ↔ Python Schema conversions (module-private helpers)

```rust
/// Extract Rust Schema from a PySchema (borrowing its columns list).
pub fn py_schema_to_rust(py: Python, s: &PySchema) -> PyResult<Schema> {
    let list = s.columns.bind(py);
    let mut cols = Vec::with_capacity(list.len());
    for item in list.iter() {
        let c: PyRef<PyColumnDef> = item.extract()?;
        cols.push(py_col_to_rust(&c).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid type_code: {e}"))
        })?);
    }
    Ok(Schema { columns: cols, pk_index: s.pk_index })
}

/// Build a PySchema from a Rust Schema.
pub fn rust_schema_to_py(py: Python, s: &Schema) -> PyResult<Py<PySchema>> {
    let py_cols: Vec<PyObject> = s.columns.iter()
        .map(|c| rust_col_to_py(py, c).map(|x| x.into_any()))
        .collect::<PyResult<_>>()?;
    let list = PyList::new_bound(py, &py_cols)?;
    Py::new(py, PySchema { columns: list.unbind(), pk_index: s.pk_index })
}
```

#### `ZSetBatch`

The Python client accesses and mutates batch fields directly:

```python
batch = ZSetBatch(schema=tbl_schema)
batch.pk_lo.append(pk)
batch.weights.append(1)
batch.columns = [[], [val1, val2, ...]]
```

We store every field as a `Py<PyList>` so Python can mutate them in-place.
The schema is stored internally (as `Py<PySchema>`) for use at `push` time.

```rust
#[pyclass(name = "ZSetBatch")]
pub struct PyZSetBatch {
    pub(crate) schema:  Py<PySchema>,
    pub(crate) pk_lo:   Py<PyList>,
    pub(crate) pk_hi:   Py<PyList>,
    pub(crate) weights: Py<PyList>,
    pub(crate) nulls:   Py<PyList>,
    pub(crate) columns: Py<PyList>,
}

#[pymethods]
impl PyZSetBatch {
    /// ZSetBatch(schema)  OR  ZSetBatch(schema=schema)
    #[new]
    #[pyo3(signature = (schema))]
    pub fn new(py: Python, schema: Py<PySchema>) -> PyResult<Self> {
        let n_cols = schema.bind(py).borrow().columns.bind(py).len();
        // Pre-populate columns with n_cols empty lists, matching Python convention.
        let col_lists: Vec<PyObject> = (0..n_cols)
            .map(|_| PyList::empty_bound(py).into())
            .collect();
        let columns = PyList::new_bound(py, &col_lists)?;
        Ok(PyZSetBatch {
            schema,
            pk_lo:   PyList::empty_bound(py).unbind(),
            pk_hi:   PyList::empty_bound(py).unbind(),
            weights: PyList::empty_bound(py).unbind(),
            nulls:   PyList::empty_bound(py).unbind(),
            columns: columns.unbind(),
        })
    }

    // Getters: return the Python list directly (Python mutates in-place).
    #[getter] pub fn pk_lo(&self,   py: Python) -> Py<PyList> { self.pk_lo.clone_ref(py)   }
    #[getter] pub fn pk_hi(&self,   py: Python) -> Py<PyList> { self.pk_hi.clone_ref(py)   }
    #[getter] pub fn weights(&self, py: Python) -> Py<PyList> { self.weights.clone_ref(py) }
    #[getter] pub fn nulls(&self,   py: Python) -> Py<PyList> { self.nulls.clone_ref(py)   }
    #[getter] pub fn columns(&self, py: Python) -> Py<PyList> { self.columns.clone_ref(py) }

    // Setter for columns (Python tests do batch.columns = [...]).
    #[setter] pub fn set_columns(&mut self, py: Python, val: Bound<PyList>) {
        self.columns = val.unbind();
    }

    // __len__: delegates to len(pk_lo).
    pub fn __len__(&self, py: Python) -> usize { self.pk_lo.bind(py).len() }
}
```

#### Batch conversion helpers (module-private)

**Python → Rust** (used by `push`):

```rust
pub fn py_batch_to_rust(
    py: Python,
    py_schema: &PySchema,
    py_batch: &PyZSetBatch,
) -> PyResult<(Schema, ZSetBatch)> {
    let schema = py_schema_to_rust(py, py_schema)?;

    // Extract pk arrays and metadata vectors.
    let pk_lo:   Vec<u64> = py_batch.pk_lo.bind(py).iter()
        .map(|x| x.extract::<u64>()).collect::<PyResult<_>>()?;
    let pk_hi:   Vec<u64> = py_batch.pk_hi.bind(py).iter()
        .map(|x| x.extract::<u64>()).collect::<PyResult<_>>()?;
    let weights: Vec<i64> = py_batch.weights.bind(py).iter()
        .map(|x| x.extract::<i64>()).collect::<PyResult<_>>()?;
    let nulls:   Vec<u64> = py_batch.nulls.bind(py).iter()
        .map(|x| x.extract::<u64>()).collect::<PyResult<_>>()?;

    let n = pk_lo.len();
    let col_list = py_batch.columns.bind(py);
    let mut columns = Vec::with_capacity(schema.columns.len());

    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            columns.push(ColData::Fixed(vec![]));
            continue;
        }
        // Each inner list may be shorter than n if Python code left it empty (pk col).
        let inner: Bound<PyList> = col_list.get_item(ci)?.downcast_into()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err(
                format!("columns[{ci}] is not a list")))?;

        match col_def.type_code {
            TypeCode::String => {
                let mut v = Vec::with_capacity(n);
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

/// Write one fixed-width value as little-endian bytes into buf.
fn write_fixed_le(buf: &mut Vec<u8>, tc: TypeCode, item: &Bound<PyAny>) -> PyResult<()> {
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
        _ => unreachable!("String/U128 handled before write_fixed_le"),
    }
    Ok(())
}
```

**Rust → Python** (used by `scan`):

```rust
pub fn rust_batch_to_py(
    py: Python,
    schema: &Schema,
    batch: &ZSetBatch,
) -> PyResult<Py<PyZSetBatch>> {
    let n = batch.pk_lo.len();

    let pk_lo   = PyList::new_bound(py, &batch.pk_lo)?;
    let pk_hi   = PyList::new_bound(py, &batch.pk_hi)?;
    let weights = PyList::new_bound(py, &batch.weights)?;
    let nulls   = PyList::new_bound(py, &batch.nulls)?;

    // Build columns: one list per schema column.
    let mut col_lists: Vec<PyObject> = Vec::with_capacity(schema.columns.len());
    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            // PK column: empty list (convention from py_client)
            col_lists.push(PyList::empty_bound(py).into());
            continue;
        }
        let col_data = &batch.columns[ci];
        match col_data {
            ColData::Fixed(buf) => {
                let stride = col_def.type_code.wire_stride();
                let list = PyList::empty_bound(py);
                for i in 0..n {
                    let val = read_fixed_le(col_def.type_code, &buf[i*stride..(i+1)*stride]);
                    list.append(val.into_py(py))?;
                }
                col_lists.push(list.into());
            }
            ColData::Strings(v) => {
                let list = PyList::empty_bound(py);
                for s in v {
                    match s {
                        Some(s) => list.append(s.into_py(py))?,
                        None    => list.append(py.None())?,
                    }
                }
                col_lists.push(list.into());
            }
            ColData::U128s(v) => {
                let list = PyList::new_bound(py, v.iter().map(|x| *x as u128))?;
                col_lists.push(list.into());
            }
        }
    }

    let columns    = PyList::new_bound(py, &col_lists)?;
    let py_schema  = rust_schema_to_py(py, schema)?;

    Py::new(py, PyZSetBatch {
        schema:  py_schema,
        pk_lo:   pk_lo.unbind(),
        pk_hi:   pk_hi.unbind(),
        weights: weights.unbind(),
        nulls:   nulls.unbind(),
        columns: columns.unbind(),
    })
}

/// Read one fixed-width value from little-endian bytes, returning a Python-compatible type.
/// All integer types return i64 (Python int). F32/F64 return f64 (Python float).
fn read_fixed_le(tc: TypeCode, slice: &[u8]) -> PyFixedVal {
    match tc {
        TypeCode::U8   => PyFixedVal::Int(slice[0] as i64),
        TypeCode::I8   => PyFixedVal::Int(slice[0] as i8 as i64),
        TypeCode::U16  => PyFixedVal::Int(u16::from_le_bytes(slice.try_into().unwrap()) as i64),
        TypeCode::I16  => PyFixedVal::Int(i16::from_le_bytes(slice.try_into().unwrap()) as i64),
        TypeCode::U32  => PyFixedVal::Int(u32::from_le_bytes(slice.try_into().unwrap()) as i64),
        TypeCode::I32  => PyFixedVal::Int(i32::from_le_bytes(slice.try_into().unwrap()) as i64),
        TypeCode::F32  => PyFixedVal::Float(f32::from_le_bytes(slice.try_into().unwrap()) as f64),
        TypeCode::U64  => PyFixedVal::Int(u64::from_le_bytes(slice.try_into().unwrap()) as i64),
        TypeCode::I64  => PyFixedVal::Int(i64::from_le_bytes(slice.try_into().unwrap())),
        TypeCode::F64  => PyFixedVal::Float(f64::from_le_bytes(slice.try_into().unwrap())),
        _ => unreachable!(),
    }
}

enum PyFixedVal { Int(i64), Float(f64) }
impl IntoPy<PyObject> for PyFixedVal {
    fn into_py(self, py: Python) -> PyObject {
        match self { PyFixedVal::Int(i) => i.into_py(py), PyFixedVal::Float(f) => f.into_py(py) }
    }
}
```

**Important**: `U64` columns decoded as `i64` is intentional — Python `int` is arbitrary
precision so no sign/range issue. Python tests compare with plain `int` literals.

---

### 3c. `src/client.rs`

```rust
use pyo3::prelude::*;
use gnitz_core::GnitzClient;
use crate::lib::GnitzError;
use crate::types::{PySchema, PyZSetBatch, py_schema_to_rust, py_batch_to_rust, rust_schema_to_py, rust_batch_to_py};
use crate::builders::{PyCircuitGraph};

/// Macro to extract the inner client or raise GnitzError("client already closed").
macro_rules! client {
    ($self:expr) => {
        $self.inner.as_ref()
            .ok_or_else(|| GnitzError::new_err("client already closed"))?
    };
}
```

#### Struct + constructor

```rust
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
    pub fn __exit__(
        &mut self,
        _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject,
    ) -> bool {
        self.close();
        false   // do not suppress exceptions
    }
```

#### DDL methods

```rust
    pub fn create_schema(&self, name: &str) -> PyResult<u64> {
        client!(self).create_schema(name).map_err(|e| GnitzError::new_err(e.to_string()))
    }

    pub fn drop_schema(&self, name: &str) -> PyResult<()> {
        client!(self).drop_schema(name).map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// create_table(schema_name, table_name, columns, pk_col_idx=0, unique_pk=True)
    #[pyo3(signature = (schema_name, table_name, columns, pk_col_idx=0, unique_pk=true))]
    pub fn create_table(
        &self, py: Python,
        schema_name: &str, table_name: &str,
        columns: Bound<PyList>,
        pk_col_idx: usize,
        unique_pk: bool,
    ) -> PyResult<u64> {
        let cols: Vec<ColumnDef> = columns.iter()
            .map(|item| {
                let c: PyRef<PyColumnDef> = item.extract()?;
                py_col_to_rust(&c).map_err(|e|
                    pyo3::exceptions::PyValueError::new_err(e.to_string()))
            })
            .collect::<PyResult<_>>()?;
        client!(self).create_table(schema_name, table_name, &cols, pk_col_idx, unique_pk)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> PyResult<()> {
        client!(self).drop_table(schema_name, table_name)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }
```

#### DML methods

```rust
    /// push(target_id, schema, batch) — matches py_client signature.
    pub fn push(
        &self, py: Python,
        target_id: u64,
        schema: PyRef<PySchema>,
        batch:  PyRef<PyZSetBatch>,
    ) -> PyResult<()> {
        let (rust_schema, rust_batch) = py_batch_to_rust(py, &schema, &batch)?;
        client!(self).push(target_id, &rust_schema, &rust_batch)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// scan(target_id) -> (Schema | None, ZSetBatch | None)
    pub fn scan(&self, py: Python, target_id: u64) -> PyResult<PyObject> {
        match client!(self).scan(target_id) {
            Ok((opt_schema, opt_batch)) => {
                // We need the schema to interpret column data.
                let py_schema = match &opt_schema {
                    Some(s) => rust_schema_to_py(py, s)?.into_py(py),
                    None    => py.None(),
                };
                let py_batch = match (opt_schema.as_ref(), opt_batch.as_ref()) {
                    (Some(s), Some(b)) => rust_batch_to_py(py, s, b)?.into_py(py),
                    _                  => py.None(),
                };
                Ok(pyo3::types::PyTuple::new_bound(py, &[py_schema, py_batch]).into())
            }
            Err(e) => Err(GnitzError::new_err(e.to_string())),
        }
    }

    /// delete(target_id, schema, pks: list[int]) — pks are pk_lo values, pk_hi=0.
    pub fn delete(
        &self, py: Python,
        target_id: u64,
        schema: PyRef<PySchema>,
        pks: Vec<u64>,
    ) -> PyResult<()> {
        let rust_schema = py_schema_to_rust(py, &schema)?;
        let pk_pairs: Vec<(u64, u64)> = pks.into_iter().map(|pk| (pk, 0u64)).collect();
        client!(self).delete(target_id, &rust_schema, &pk_pairs)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }
```

#### View methods

```rust
    /// create_view(schema_name, view_name, source_table_id, output_schema) -> view_id
    pub fn create_view(
        &self, py: Python,
        schema_name: &str, view_name: &str,
        source_table_id: u64,
        output_schema: PyRef<PySchema>,
    ) -> PyResult<u64> {
        let rust_schema = py_schema_to_rust(py, &output_schema)?;
        client!(self).create_view(schema_name, view_name, source_table_id, &rust_schema.columns)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// create_view_with_circuit(schema_name, view_name, circuit, output_schema) -> view_id
    /// CONSUMES circuit.
    pub fn create_view_with_circuit(
        &self, py: Python,
        schema_name: &str, view_name: &str,
        mut circuit: PyRefMut<PyCircuitGraph>,
        output_schema: PyRef<PySchema>,
    ) -> PyResult<u64> {
        let graph = circuit.inner.take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(
                "CircuitGraph already consumed"))?;
        let rust_schema = py_schema_to_rust(py, &output_schema)?;
        client!(self).create_view_with_circuit(schema_name, view_name, graph, &rust_schema.columns)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    pub fn drop_view(&self, schema_name: &str, view_name: &str) -> PyResult<()> {
        client!(self).drop_view(schema_name, view_name)
            .map_err(|e| GnitzError::new_err(e.to_string()))
    }

    /// resolve_table_id(schema_name, table_name) -> (tid: int, schema: Schema)
    pub fn resolve_table_id(
        &self, py: Python,
        schema_name: &str, table_name: &str,
    ) -> PyResult<PyObject> {
        let (tid, schema) = client!(self).resolve_table_id(schema_name, table_name)
            .map_err(|e| GnitzError::new_err(e.to_string()))?;
        let py_schema = rust_schema_to_py(py, &schema)?;
        Ok(pyo3::types::PyTuple::new_bound(py, &[tid.into_py(py), py_schema.into_py(py)]).into())
    }

    /// alloc_table_id() -> int
    pub fn allocate_table_id(&self) -> PyResult<u64> {
        client!(self).alloc_table_id().map_err(|e| GnitzError::new_err(e.to_string()))
    }
}
```

Note: the Python client uses `allocate_table_id` / `allocate_schema_id` (not `alloc_*`).
Expose both names via `#[pyo3(name = "...")]` if needed for shim compatibility.

---

### 3d. `src/builders.rs`

#### ExprBuilder

```rust
#[pyclass(name = "ExprBuilder")]
pub struct PyExprBuilder {
    inner: Option<ExprBuilder>,
}

#[pymethods]
impl PyExprBuilder {
    #[new]
    pub fn new() -> Self { PyExprBuilder { inner: Some(ExprBuilder::new()) } }

    // All builder methods have the same pattern: check not consumed, delegate.
    pub fn load_col_int(&mut self, col_idx: usize) -> PyResult<u32> {
        b!(self).load_col_int(col_idx)  // see macro below
    }
    pub fn load_col_float(&mut self, col_idx: usize) -> PyResult<u32> {
        b!(self).load_col_float(col_idx)
    }
    pub fn load_const(&mut self, value: i64) -> PyResult<u32> {
        b!(self).load_const(value)
    }
    pub fn add(&mut self, a: u32, b: u32) -> PyResult<u32>    { b_mut!(self).add(a, b)    }
    pub fn sub(&mut self, a: u32, b: u32) -> PyResult<u32>    { b_mut!(self).sub(a, b)    }
    pub fn cmp_eq(&mut self, a: u32, b: u32) -> PyResult<u32> { b_mut!(self).cmp_eq(a, b) }
    pub fn cmp_ne(&mut self, a: u32, b: u32) -> PyResult<u32> { b_mut!(self).cmp_ne(a, b) }
    pub fn cmp_gt(&mut self, a: u32, b: u32) -> PyResult<u32> { b_mut!(self).cmp_gt(a, b) }
    pub fn cmp_ge(&mut self, a: u32, b: u32) -> PyResult<u32> { b_mut!(self).cmp_ge(a, b) }
    pub fn cmp_lt(&mut self, a: u32, b: u32) -> PyResult<u32> { b_mut!(self).cmp_lt(a, b) }
    pub fn cmp_le(&mut self, a: u32, b: u32) -> PyResult<u32> { b_mut!(self).cmp_le(a, b) }
    pub fn bool_and(&mut self, a: u32, b: u32) -> PyResult<u32> { b_mut!(self).bool_and(a, b) }
    pub fn bool_or(&mut self, a: u32, b: u32) -> PyResult<u32>  { b_mut!(self).bool_or(a, b)  }
    pub fn bool_not(&mut self, a: u32) -> PyResult<u32>         { b_mut!(self).bool_not(a)     }
    pub fn is_null(&mut self, col_idx: usize) -> PyResult<u32>  { b_mut!(self).is_null(col_idx) }
    pub fn is_not_null(&mut self, col_idx: usize) -> PyResult<u32> { b_mut!(self).is_not_null(col_idx) }

    /// Consume builder and return an ExprProgram.
    pub fn build(&mut self, result_reg: u32) -> PyResult<PyExprProgram> {
        let b = self.inner.take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("ExprBuilder already consumed"))?;
        Ok(PyExprProgram { inner: Some(b.build(result_reg)) })
    }
}

// Helper macro to borrow the inner ExprBuilder (non-consuming).
macro_rules! b_mut {
    ($self:expr) => {
        $self.inner.as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("ExprBuilder already consumed"))?
    };
}
```

#### ExprProgram

```rust
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
```

`ExprProgram` is opaque from Python's perspective. The only operation is passing it to
`CircuitBuilder.filter()`. It must derive `Clone` (see prerequisite §1) because `filter`
clones it into the circuit builder (rather than moving out of the Python wrapper).

#### CircuitBuilder

```rust
#[pyclass(name = "CircuitBuilder")]
pub struct PyCircuitBuilder {
    inner: Option<CircuitBuilder>,
}

macro_rules! cb {
    ($self:expr) => {
        $self.inner.as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("CircuitBuilder already consumed"))?
    };
}

#[pymethods]
impl PyCircuitBuilder {
    #[new]
    pub fn new(view_id: u64, primary_source_id: u64) -> Self {
        PyCircuitBuilder { inner: Some(CircuitBuilder::new(view_id, primary_source_id)) }
    }

    pub fn input_delta(&mut self) -> PyResult<u64>          { Ok(cb!(self).input_delta())       }
    pub fn trace_scan(&mut self, table_id: u64) -> PyResult<u64> { Ok(cb!(self).trace_scan(table_id)) }
    pub fn negate(&mut self, input: u64) -> PyResult<u64>   { Ok(cb!(self).negate(input))       }
    pub fn union(&mut self, a: u64, b: u64) -> PyResult<u64>{ Ok(cb!(self).union(a, b))         }
    pub fn delay(&mut self, input: u64) -> PyResult<u64>    { Ok(cb!(self).delay(input))        }
    pub fn distinct(&mut self, input: u64) -> PyResult<u64> { Ok(cb!(self).distinct(input))     }

    /// filter(input, expr=None) — expr is an ExprProgram or None.
    /// Clones the ExprProgram rather than consuming it, so Python can hold the reference.
    #[pyo3(signature = (input, expr = None))]
    pub fn filter(
        &mut self, input: u64,
        expr: Option<PyRef<PyExprProgram>>,
    ) -> PyResult<u64> {
        let expr_opt = match expr {
            Some(e) => {
                let prog = e.inner.as_ref()
                    .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("ExprProgram consumed"))?
                    .clone();   // Clone needed — ExprProgram must derive Clone
                Some(prog)
            }
            None => None,
        };
        Ok(cb!(self).filter(input, expr_opt))
    }

    /// map(input, projection=None) — projection is list[int] of output column indices.
    #[pyo3(signature = (input, projection = None))]
    pub fn map(
        &mut self, input: u64,
        projection: Option<Vec<usize>>,
    ) -> PyResult<u64> {
        let cols = projection.as_deref().unwrap_or(&[]);
        Ok(cb!(self).map(input, cols))
    }

    /// join(delta, trace_table_id)
    pub fn join(&mut self, delta: u64, trace_table_id: u64) -> PyResult<u64> {
        Ok(cb!(self).join(delta, trace_table_id))
    }

    pub fn anti_join(&mut self, delta: u64, trace_table_id: u64) -> PyResult<u64> {
        Ok(cb!(self).anti_join(delta, trace_table_id))
    }

    pub fn semi_join(&mut self, delta: u64, trace_table_id: u64) -> PyResult<u64> {
        Ok(cb!(self).semi_join(delta, trace_table_id))
    }

    /// reduce(input, group_by_cols, agg_func_id=0, agg_col_idx=0)
    #[pyo3(signature = (input, group_by_cols, agg_func_id=0, agg_col_idx=0))]
    pub fn reduce(
        &mut self,
        input: u64,
        group_by_cols: Vec<usize>,
        agg_func_id: u64,
        agg_col_idx: usize,
    ) -> PyResult<u64> {
        Ok(cb!(self).reduce(input, &group_by_cols, agg_func_id, agg_col_idx))
    }

    /// shard(input, shard_columns: list[int])
    pub fn shard(&mut self, input: u64, shard_columns: Vec<usize>) -> PyResult<u64> {
        Ok(cb!(self).shard(input, &shard_columns))
    }

    /// gather(input, worker_id=0)
    #[pyo3(signature = (input, worker_id=0))]
    pub fn gather(&mut self, input: u64, worker_id: u64) -> PyResult<u64> {
        Ok(cb!(self).gather(input, worker_id))
    }

    /// sink(input, target_table_id) — target_table_id is informational, not stored.
    pub fn sink(&mut self, input: u64, target_table_id: u64) -> PyResult<u64> {
        Ok(cb!(self).sink(input, target_table_id))
    }

    /// Consume builder and return a CircuitGraph for create_view_with_circuit.
    pub fn build(&mut self) -> PyResult<PyCircuitGraph> {
        let b = self.inner.take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(
                "CircuitBuilder already consumed"))?;
        Ok(PyCircuitGraph { inner: Some(b.build()) })
    }
}
```

#### CircuitGraph

```rust
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
```

---

## Change 4: `gnitz-py/tests/`

All tests:
- Use `GNITZ_WORKERS=4` (env var read by fixtures from `GNITZ_WORKERS`)
- Use the `gnitz` module directly (not the shim)
- Match the exact test patterns from `py_client/tests/` where possible

### `tests/conftest.py`

```python
import os, subprocess, tempfile, time, shutil
import pytest
import gnitz

@pytest.fixture(scope="session")
def server():
    tmpdir = tempfile.mkdtemp(prefix="gnitz_py_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    binary = os.environ.get("GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../../gnitz-server-c")))
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    cmd = [binary, data_dir, sock_path]
    if w := os.environ.get("GNITZ_WORKERS"):
        cmd.append(f"--workers={w}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for _ in range(100):
        if os.path.exists(sock_path): break
        time.sleep(0.1)
    else:
        proc.kill(); proc.communicate()
        raise RuntimeError("Server did not start")
    yield sock_path
    proc.kill(); proc.wait()
    shutil.rmtree(tmpdir, ignore_errors=True)

@pytest.fixture
def client(server):
    c = gnitz.GnitzClient(server)
    yield c
    c.close()
```

### `tests/test_connect.py`

```python
import gnitz

def test_connect_and_close(client):
    assert client is not None

def test_context_manager(server):
    with gnitz.GnitzClient(server) as c:
        sid = c.create_schema("ctx_test")
        assert sid > 0
        c.drop_schema("ctx_test")

def test_close_idempotent(client):
    client.close()
    client.close()  # second call must not raise
```

### `tests/test_ddl.py`

```python
import random, gnitz

def _uid(): return str(random.randint(100000, 999999))

def test_create_schema(client):
    name = "s" + _uid()
    sid = client.create_schema(name)
    assert sid > 0
    client.drop_schema(name)

def test_create_table(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, "nums", cols, pk_col_idx=0)
    assert tid > 0
    client.drop_table(sn, "nums")
    client.drop_schema(sn)

def test_drop_schema_not_found(client):
    import pytest
    with pytest.raises(gnitz.GnitzError):
        client.drop_schema("nonexistent_schema_xyz")

def test_drop_table_not_found(client):
    import pytest
    sn = "s" + _uid()
    client.create_schema(sn)
    with pytest.raises(gnitz.GnitzError):
        client.drop_table(sn, "nonexistent_table_xyz")
    client.drop_schema(sn)

def test_create_table_with_string_col(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("label", gnitz.TypeCode.STRING, is_nullable=True)]
    tid = client.create_table(sn, "labels", cols, pk_col_idx=0)
    assert tid > 0
    client.drop_table(sn, "labels")
    client.drop_schema(sn)
```

### `tests/test_dml.py`

```python
import random, gnitz

def _uid(): return str(random.randint(100000, 999999))

def _setup(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, "t", cols)
    return sn, tid, schema

def _push_rows(client, tid, schema, rows):
    """rows: list of (pk, val) tuples, weight=1"""
    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend(pk for pk, _ in rows)
    batch.pk_hi.extend([0] * len(rows))
    batch.weights.extend([1] * len(rows))
    batch.nulls.extend([0] * len(rows))
    batch.columns[0]  # pk column — stays empty
    batch.columns[1].extend(val for _, val in rows)
    client.push(tid, schema, batch)

def test_push_and_scan(client):
    sn, tid, schema = _setup(client)
    _push_rows(client, tid, schema, [(i, i*10) for i in range(1, 6)])
    _, result = client.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == 5
    client.drop_table(sn, "t")
    client.drop_schema(sn)

def test_scan_empty_table(client):
    sn, tid, schema = _setup(client)
    _, result = client.scan(tid)
    # empty table: scan returns None or empty batch
    assert result is None or len(result.pk_lo) == 0
    client.drop_table(sn, "t")
    client.drop_schema(sn)

def test_delete_rows(client):
    sn, tid, schema = _setup(client)
    _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
    client.delete(tid, schema, [2])  # delete pk=2
    _, result = client.scan(tid)
    assert result is not None
    pks = sorted(result.pk_lo[i] for i in range(len(result.pk_lo))
                 if result.weights[i] > 0)
    assert pks == [1, 3]
    client.drop_table(sn, "t")
    client.drop_schema(sn)

def test_nullable_string_columns(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=True)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, "strs", cols)
    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend([1, 2, 3])
    batch.pk_hi.extend([0, 0, 0])
    batch.weights.extend([1, 1, 1])
    batch.nulls.extend([0, 2, 0])   # row 1: null bit for col 1 (payload idx 0)
    batch.columns[0]  # pk placeholder stays empty
    batch.columns[1].extend(["hello", None, "world"])
    client.push(tid, schema, batch)
    _, result = client.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == 3
    client.drop_table(sn, "strs")
    client.drop_schema(sn)
```

### `tests/test_views.py`

```python
import random, gnitz

def _uid(): return str(random.randint(100000, 999999))

def test_create_drop_view(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, "src", cols)
    vid = client.create_view(sn, "v", tid, gnitz.Schema(cols, pk_index=0))
    assert vid > 0
    client.drop_view(sn, "v")
    client.drop_table(sn, "src")
    client.drop_schema(sn)
```

### `tests/test_dbsp_ops.py`

Tests for filter, join, and reduce views — the three most common circuit patterns.

```python
import random, gnitz

def _uid(): return str(random.randint(100000, 999999))

def _create_table(client, sn, name=None):
    """(pk U64, val I64). Returns (tid, schema)."""
    if name is None: name = "t_" + _uid()
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, name, cols, pk_col_idx=0)
    return tid, schema, name

def _push(client, tid, schema, rows):
    """rows: [(pk, val, weight)]"""
    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend(r[0] for r in rows)
    batch.pk_hi.extend([0] * len(rows))
    batch.weights.extend(r[2] for r in rows)
    batch.nulls.extend([0] * len(rows))
    batch.columns[1].extend(r[1] for r in rows)
    client.push(tid, schema, batch)

def _scan_dict(client, target_id):
    """Return {pk: (val, weight)} for all rows."""
    _, batch = client.scan(target_id)
    if batch is None or len(batch.pk_lo) == 0: return {}
    result = {}
    for i in range(len(batch.pk_lo)):
        if batch.weights[i] == 0: continue
        pk = batch.pk_lo[i]
        val = batch.columns[1][i]
        result[pk] = (val, batch.weights[i])
    return result


class TestFilterView:

    def test_filter_gt_50(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, schema, tname = _create_table(client, sn)

        # Circuit: input_delta → filter(val > 50) → sink
        vid = client.allocate_table_id()
        cb = gnitz.CircuitBuilder(vid, tid)
        inp = cb.input_delta()

        eb = gnitz.ExprBuilder()
        r0 = eb.load_col_int(1)  # col 1 = val
        r1 = eb.load_const(50)
        cond = eb.cmp_gt(r0, r1)
        prog = eb.build(result_reg=cond)

        filt = cb.filter(inp, prog)
        cb.sink(filt, vid)
        circuit = cb.build()

        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        out_schema = gnitz.Schema(out_cols, pk_index=0)
        view_id = client.create_view_with_circuit(sn, "vf", circuit, out_schema)
        assert view_id == vid

        _push(client, tid, schema, [(1, 30, 1), (2, 70, 1), (3, 100, 1)])
        data = _scan_dict(client, vid)
        assert 1 not in data   # 30 <= 50 filtered out
        assert 2 in data and data[2][0] == 70
        assert 3 in data and data[3][0] == 100

        client.drop_view(sn, "vf")
        client.drop_table(sn, tname)
        client.drop_schema(sn)


class TestReduceView:

    def test_sum_reduce(self, client):
        """Verify reduce view accumulates inserted rows."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, schema, tname = _create_table(client, sn)

        vid = client.allocate_table_id()
        cb = gnitz.CircuitBuilder(vid, tid)
        inp = cb.input_delta()
        # reduce: group_by=[col 0 = pk], agg_col=1 (val), agg_func=0
        red = cb.reduce(inp, group_by_cols=[0], agg_func_id=0, agg_col_idx=1)
        cb.sink(red, vid)
        circuit = cb.build()

        # Output schema for reduce: (U128 pk, I64 group_col, I64 agg_val)
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128),
                    gnitz.ColumnDef("gk", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("agg", gnitz.TypeCode.I64)]
        out_schema = gnitz.Schema(out_cols, pk_index=0)
        client.create_view_with_circuit(sn, "vr", circuit, out_schema)

        _push(client, tid, schema, [(1, 10, 1), (2, 20, 1)])
        _, result = client.scan(vid)
        assert result is not None and len(result.pk_lo) > 0

        client.drop_view(sn, "vr")
        client.drop_table(sn, tname)
        client.drop_schema(sn)
```

### `tests/test_workers.py`

```python
import random, gnitz
import pytest

def _uid(): return str(random.randint(100000, 999999))

def test_push_scan_multiworker(client):
    """Push 200 rows and scan back — all must be present."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, "big", cols)

    n = 200
    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend(range(1, n + 1))
    batch.pk_hi.extend([0] * n)
    batch.weights.extend([1] * n)
    batch.nulls.extend([0] * n)
    batch.columns[1].extend(i * 10 for i in range(1, n + 1))
    client.push(tid, schema, batch)

    _, result = client.scan(tid)
    assert result is not None
    pks = sorted(result.pk_lo[i] for i in range(len(result.pk_lo))
                 if result.weights[i] > 0)
    assert pks == list(range(1, n + 1))

    client.drop_table(sn, "big")
    client.drop_schema(sn)
```

---

## Change 5: `py_client/gnitz_client_rs/` — Compatibility Shim

Allows running the existing `py_client/tests/` with `import gnitz_client_rs as gnitz_client`
and minimal code changes.

### `__init__.py`

```python
"""
Thin compatibility shim: exposes gnitz (Rust extension module) under
gnitz_client-compatible names.

Known API differences from gnitz_client:
  - ExprBuilder.build() returns ExprProgram object, not dict
  - CircuitBuilder.build() returns CircuitGraph object, not CircuitGraph dataclass
  - ZSetBatch(schema) positional; no keyword-only form
  - client.scan() returns (Schema|None, ZSetBatch|None) — same as gnitz_client
"""
import gnitz

GnitzClient = gnitz.GnitzClient
GnitzError  = gnitz.GnitzError
TypeCode    = gnitz.TypeCode
ColumnDef   = gnitz.ColumnDef
Schema      = gnitz.Schema

__all__ = ["GnitzClient", "GnitzError", "TypeCode", "ColumnDef", "Schema"]
```

### `batch.py`

```python
from gnitz import ZSetBatch
__all__ = ["ZSetBatch"]
```

### `types.py`

```python
from gnitz import TypeCode, ColumnDef, Schema
__all__ = ["TypeCode", "ColumnDef", "Schema"]
```

### `circuit.py`

```python
from gnitz import CircuitBuilder, CircuitGraph
__all__ = ["CircuitBuilder", "CircuitGraph"]
```

### `expr.py`

```python
from gnitz import ExprBuilder, ExprProgram
__all__ = ["ExprBuilder", "ExprProgram"]
```

---

## API Differences from `gnitz_client`

| Feature | `gnitz_client` | `gnitz-py` |
|---|---|---|
| `ExprBuilder.build()` return type | `dict` | `ExprProgram` object |
| `CircuitBuilder.build()` return type | `CircuitGraph` dataclass | `PyCircuitGraph` object |
| `ZSetBatch` constructor | `ZSetBatch(schema=schema)` | `ZSetBatch(schema)` (positional works too) |
| `ZSetBatch.columns` initialization | caller-managed | pre-populated with empty lists per column |
| `TypeCode` namespace | class with int attrs | PyO3 class with `const` attrs |
| `CircuitBuilder.filter(input, func_id, expr)` | `func_id` param ignored by server | `filter(input, expr=None)` — no `func_id` |

These differences are confined to the builder types. All DDL, DML, and scan APIs are
identical to `gnitz_client`.

---

## Edge Cases

| Case | Handling |
|---|---|
| `client.close()` called twice | `Option::take()` — second call is no-op, no panic |
| `CircuitGraph` passed to `create_view_with_circuit` twice | `Option::take()` → `PyValueError("already consumed")` |
| `ExprProgram` with `None` inner (after `filter`) | Not possible — we `clone()` rather than move |
| `ExprBuilder.build()` called twice | `Option::take()` → `PyValueError("already consumed")` |
| `scan()` on table that was never pushed to | Returns `(schema, None)` or `(schema, empty_batch)` — both handled |
| `batch.columns[pk_idx]` non-empty | `py_batch_to_rust` skips pk column (ci == pk_index) — extra data silently ignored |
| `batch.columns[ci]` empty list for non-pk col | Produces empty `ColData::Fixed(vec![])` — causes server error if pk_lo is non-empty |
| Push batch with 0 rows | `ZSetBatch::new(&schema)` all vectors empty → no-op push |
| `columns` list shorter than `schema.columns` | `get_item(ci)` returns `IndexError` from Python — surfaces as PyO3 error |
| `read_fixed_le` U64 → i64 cast | Intended: Python ints are arbitrary precision; comparison with Python int literals works |
| NULL string in scan result | Returns `None` element in the Python list |
| `gnitz_client_rs.batch.ZSetBatch` | Re-exports `gnitz.ZSetBatch` directly |

---

## Verification

```bash
# 1. Build wheel (installs into current venv)
cd rust_client/gnitz-py
maturin develop

# 2. Verify module loads
python -c "import gnitz; print(gnitz.TypeCode.U64)"   # → 8

# 3. Run gnitz-py test suite
GNITZ_WORKERS=4 uv run pytest rust_client/gnitz-py/tests/ -x --tb=short -v

# 4. Regression: original py_client suite must still pass
GNITZ_WORKERS=4 uv run pytest py_client/tests/ -x --tb=short

# 5. Type-check (no server needed)
cd rust_client && cargo build -p gnitz-py
```

Expected: all gnitz-py tests pass; all py_client tests still pass.
