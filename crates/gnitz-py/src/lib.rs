use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

use gnitz_core::protocol::types::type_code_from_u64;
use gnitz_core::{
    null_word_get, null_word_set, ClientError, ColData, ColumnDef, PkColumn, Schema, TypeCode, WireConflictMode,
    ZSetBatch,
};
use gnitz_core::{Circuit, CircuitBuilder, ExprBuilder, ExprProgram, GnitzClient};
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_sql::{SqlPlanner, SqlResult};

// ---------------------------------------------------------------------------
// GnitzError Python exception
// ---------------------------------------------------------------------------

pyo3::create_exception!(_native, GnitzError, pyo3::exceptions::PyException);
// A user-table transaction failed its OCC precondition (STATUS_TXN_CONFLICT): a
// table it read was written concurrently. A subtype of GnitzError, so existing
// `except GnitzError` handlers still catch it, while applications that want to
// retry can `except GnitzConflictError`.
pyo3::create_exception!(_native, GnitzConflictError, GnitzError);

/// Wrap any `Display` error as a `GnitzError` PyErr. For the handful of
/// failures that carry no retryability verdict (handshake, waker setup).
fn gnitz_err(e: impl std::fmt::Display) -> PyErr {
    GnitzError::new_err(e.to_string())
}

/// Wrap a `Display` failure that has already classified itself: a retryable OCC
/// conflict becomes the dedicated `GnitzConflictError` (a `GnitzError` subclass,
/// so existing `except GnitzError` handlers still catch it while applications
/// that want to retry can name it), everything else the generic `GnitzError`.
/// The verdict comes from the error enum's own `is_conflict`, never from a match
/// re-typed here, so it cannot depend on which call site raised it.
fn err_with_conflict(e: impl std::fmt::Display, conflict: bool) -> PyErr {
    if conflict {
        GnitzConflictError::new_err(e.to_string())
    } else {
        gnitz_err(e)
    }
}

/// Map a client error to a Python exception. Every `ClientError` in this file
/// passes through here.
fn to_py_err<T>(res: Result<T, ClientError>) -> PyResult<T> {
    res.map_err(|e| err_with_conflict(&e, e.is_conflict()))
}

// ---------------------------------------------------------------------------
// ColumnDef
// ---------------------------------------------------------------------------

#[pyclass(name = "ColumnDef", get_all)]
pub struct PyColumnDef {
    pub name: String,
    pub type_code: u32,
    pub is_nullable: bool,
    pub primary_key: bool,
    pub is_hidden: bool,
}

#[pymethods]
impl PyColumnDef {
    #[new]
    #[pyo3(signature = (name, type_code, is_nullable = false, primary_key = false, is_hidden = false))]
    pub fn new(name: String, type_code: u32, is_nullable: bool, primary_key: bool, is_hidden: bool) -> Self {
        PyColumnDef {
            name,
            type_code,
            is_nullable,
            primary_key,
            is_hidden,
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ColumnDef(name={:?}, type_code={}, is_nullable={}, primary_key={}, is_hidden={})",
            self.name, self.type_code, self.is_nullable, self.primary_key, self.is_hidden
        )
    }
}

fn py_col_to_rust(c: &PyColumnDef) -> PyResult<ColumnDef> {
    type_code_from_u64(c.type_code as u64)
        .map(|tc| {
            let mut cd = ColumnDef::new(c.name.clone(), tc, c.is_nullable);
            cd.is_hidden = c.is_hidden;
            cd
        })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn rust_col_to_py(py: Python<'_>, c: &ColumnDef, primary_key: bool) -> PyResult<PyObject> {
    Ok(Py::new(
        py,
        PyColumnDef {
            name: c.name.clone(),
            type_code: c.type_code as u32,
            is_nullable: c.is_nullable,
            primary_key,
            is_hidden: c.is_hidden,
        },
    )?
    .into_any())
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// A validated Rust `Schema`, and nothing else: `columns` is derived from it on
/// access, so there is no second representation that could drift.
///
/// The PK column indices are held in **sort order** — e.g. `pk_indices=[2, 1]`
/// sorts by col 2 first, then col 1. Order matters for seek/range semantics.
#[pyclass(name = "Schema")]
pub struct PySchema {
    pub(crate) rust: Arc<Schema>,
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (columns, pk_index = None, pk_indices = None))]
    pub fn new(columns: Bound<'_, PyList>, pk_index: Option<usize>, pk_indices: Option<Vec<usize>>) -> PyResult<Self> {
        if columns.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Schema must have at least 1 column",
            ));
        }
        if pk_index.is_some() && pk_indices.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "pass either pk_index or pk_indices, not both",
            ));
        }
        // One pass: convert each column and note which ones carry the PK flag,
        // so the implicit-PK fallback needs no second walk of the list.
        let mut cols = Vec::with_capacity(columns.len());
        let mut flagged = Vec::new();
        for (i, item) in columns.iter().enumerate() {
            let c: PyRef<'_, PyColumnDef> = item.extract()?;
            if c.primary_key {
                flagged.push(i);
            }
            cols.push(py_col_to_rust(&c)?);
        }
        let pk_cols = match (pk_index, pk_indices) {
            (Some(i), _) => vec![i],
            (None, Some(v)) => v,
            // No explicit list: every flagged column, in declaration order; if
            // nothing is flagged, column 0 is the key.
            (None, None) if flagged.is_empty() => vec![0],
            (None, None) => flagged,
        };
        // `Schema::from_parts` applies the shared rule set — the MAX_COLUMNS cap,
        // the structural PK rules, and per-PK-column nullability/eligibility.
        let rust = Schema::from_parts(cols, pk_cols).map_err(pyo3::exceptions::PyValueError::new_err)?;
        Ok(PySchema { rust: Arc::new(rust) })
    }

    #[getter]
    pub fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        rust_columns_to_py(py, &self.rust)
    }

    #[getter]
    pub fn pk_indices(&self) -> Vec<usize> {
        self.rust.pk_indices().to_vec()
    }

    /// Single-PK convenience. Raises ValueError on compound schemas.
    #[getter]
    pub fn pk_index(&self) -> PyResult<usize> {
        if self.rust.pk_count() > 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Schema has compound PK; use pk_indices instead of pk_index",
            ));
        }
        Ok(self.rust.pk_indices()[0])
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Schema(pk_indices={:?}, ncols={})",
            self.rust.pk_indices(),
            self.rust.columns.len()
        )
    }
}

/// Encode the Python PK values `pks` (each an int/UUID for a single-column key,
/// or packed `bytes`) into a `PkColumn` for `schema`. Shared by
/// `PyGnitzClient::delete` and `PyTxn::delete`; a single-column key runs through
/// the same typed encoder the append path uses, so a signed or UUID key packs
/// identically whichever surface supplied it.
fn py_pks_to_column(schema: &Schema, pks: &[Bound<'_, PyAny>]) -> PyResult<PkColumn> {
    let stride = schema.pk_stride();
    let mut pk_col = PkColumn::empty_for_schema(schema);
    for pk_val in pks {
        let mut t = gnitz_core::PkTuple::new(stride as u8);
        if let Ok(bytes) = pk_val.downcast::<pyo3::types::PyBytes>() {
            let b = bytes.as_bytes();
            if b.len() != stride {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "pk bytes length {} != schema pk_stride {}",
                    b.len(),
                    stride
                )));
            }
            t.buf[..stride].copy_from_slice(b);
        } else if let [ci] = schema.pk_indices() {
            write_pk_col_into(schema, &mut t, *ci, pk_val)?;
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "a compound pk must be passed as packed bytes",
            ));
        }
        pk_col.push_tuple(&t);
    }
    Ok(pk_col)
}

/// Resolve a Python "schema-ish" argument to a `Bound<PySchema>`: a `Schema`, a
/// `Struct` subclass (via its `_schema`), or a list of `ColumnDef` (wrapped in a
/// fresh `Schema`). The single adapter for every method that accepts any of
/// these forms.
fn resolve_py_schema<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PySchema>> {
    // Bare `Schema` first: it is the common case, and a failed `getattr` would
    // otherwise raise and swallow an AttributeError on every call.
    if let Ok(s) = obj.downcast::<PySchema>() {
        return Ok(s.clone());
    }
    if let Ok(inner) = obj.getattr("_schema") {
        return inner.downcast_into::<PySchema>().map_err(PyErr::from);
    }
    // A sequence of ColumnDef → build a Schema (PK defaults handled by PySchema).
    let list = obj.downcast::<PyList>()?;
    Bound::new(py, PySchema::new(list.clone(), None, None)?)
}

/// The schema's columns as a fresh Python list of `ColumnDef`, each flagged
/// with whether it is a PK column. Derived from the Rust `Schema` on every
/// access — it is the only representation, so `Schema.columns[i].primary_key`
/// always reflects the PK list the schema actually validated.
fn rust_columns_to_py<'py>(py: Python<'py>, s: &Schema) -> PyResult<Bound<'py, PyList>> {
    let py_cols: Vec<PyObject> = s
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| rust_col_to_py(py, c, s.is_pk_col(i)))
        .collect::<PyResult<_>>()?;
    PyList::new(py, py_cols)
}

fn rust_schema_to_py(py: Python<'_>, s: &Arc<Schema>) -> PyResult<Py<PySchema>> {
    Py::new(py, PySchema { rust: Arc::clone(s) })
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
    pub fn weight(&self) -> i64 {
        self.weight
    }

    pub fn __getattr__(&self, py: Python<'_>, name: &str) -> PyResult<PyObject> {
        if let Some(&i) = self.field_index.get(name) {
            return Ok(self.values.bind(py).get_item(i)?.unbind());
        }
        Err(pyo3::exceptions::PyAttributeError::new_err(format!(
            "Row has no field {name:?}"
        )))
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
        Ok(self.values.bind(py).as_any().try_iter()?.into_any().unbind())
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

/// Encode one PK column's Python value into `t` at that column's byte offset.
/// The one typed PK encoder: shared by the batch append path and
/// [`py_pks_to_column`], so a signed, UUID, or wide key packs the same way
/// whichever surface supplied it.
fn write_pk_col_into(schema: &Schema, t: &mut gnitz_core::PkTuple, ci: usize, val: &Bound<'_, PyAny>) -> PyResult<()> {
    if val.is_none() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "PK column {:?} cannot be None",
            schema.columns[ci].name
        )));
    }
    let tc = schema.columns[ci].type_code;
    let off = schema.pk_byte_offset(ci);
    match tc {
        TypeCode::U128 | TypeCode::UUID => {
            t.buf[off..off + 16].copy_from_slice(&extract_uuid_or_u128(val, Some(tc))?.to_le_bytes())
        }
        TypeCode::I128 => t.buf[off..off + 16].copy_from_slice(&(val.extract::<i128>()? as u128).to_le_bytes()),
        _ => write_fixed_le_into(&mut t.buf[off..off + tc.wire_stride()], tc, val)?,
    }
    Ok(())
}

/// Stores batch data in Rust Vecs with a cached Schema. `append` / `extend`
/// handle all type extraction, null tracking, and PK handling in Rust, so
/// `push()` reuses the cached schema and batch without any Python→Rust
/// re-extraction.
#[pyclass(name = "ZSetBatch")]
pub struct PyZSetBatch {
    pub(crate) schema: Arc<Schema>,
    pub(crate) batch: ZSetBatch,
    /// Interned PyString for each column name — built once, reused across all
    /// appends. Interned so a dict lookup keyed by a Python source literal or a
    /// `**kwargs` name hits on pointer identity instead of a string compare.
    col_keys: Vec<Py<PyString>>,
    /// `schema.pk_stride()`, hoisted out of the per-row append.
    pk_stride: u8,
    /// Cached (payload_idx, col_idx) for non-PK columns; built once at construction.
    payload_cols: Vec<(usize, usize)>,
}

/// Private helpers — shared between `append` and `extend`.
impl PyZSetBatch {
    fn append_pk_from_dict(&mut self, py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<()> {
        let mut t = gnitz_core::PkTuple::new(self.pk_stride);
        for &ci in self.schema.pk_indices() {
            let val = dict.get_item(self.col_keys[ci].bind(py))?.ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!("missing PK column {:?}", self.schema.columns[ci].name))
            })?;
            write_pk_col_into(&self.schema, &mut t, ci, &val)?;
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

    /// Append one non-null payload value. The `let … else` arms make the
    /// schema/`ColData` agreement a checked precondition: a mismatch would
    /// otherwise push nothing while the weight and null vectors still advanced,
    /// silently skewing that column's length against the rest of the batch.
    fn append_column_value(&mut self, ci: usize, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let col = &mut self.batch.columns[ci];
        match self.schema.columns[ci].type_code {
            TypeCode::String => {
                let ColData::Strings(v) = col else { variant_mismatch() };
                v.push(Some(val.extract::<String>()?));
            }
            TypeCode::Blob => {
                let ColData::Bytes(v) = col else { variant_mismatch() };
                v.push(Some(val.extract::<Vec<u8>>()?));
            }
            tc @ (TypeCode::U128 | TypeCode::UUID) => {
                let ColData::U128s(v) = col else { variant_mismatch() };
                v.push(extract_uuid_or_u128(val, Some(tc))?);
            }
            TypeCode::I128 => {
                let ColData::U128s(v) = col else { variant_mismatch() };
                v.push(val.extract::<i128>()? as u128);
            }
            tc => {
                let ColData::Fixed(buf) = col else { variant_mismatch() };
                write_fixed_le(buf, tc, val)?;
            }
        }
        Ok(())
    }

    fn append_null_column(&mut self, ci: usize, payload_idx: usize, nulls: &mut u64) -> PyResult<()> {
        if !self.schema.columns[ci].is_nullable {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Non-nullable column {:?} cannot be None",
                self.schema.columns[ci].name,
            )));
        }
        null_word_set(nulls, payload_idx, true);
        self.batch.columns[ci].push_null(self.schema.columns[ci].type_code);
        Ok(())
    }

    /// Run `body` against `self`; on error, roll the batch back to its
    /// pre-call row count so a partial write never escapes. Used both per-row
    /// (the single appends) and per-call (`extend_from_dicts` wraps its whole
    /// loop), so every append path shares one all-or-nothing contract. Nesting
    /// is safe: `rollback_to` only truncates, so an inner per-row rollback
    /// followed by the outer batch-level rollback is idempotent.
    fn with_rollback<F>(&mut self, body: F) -> PyResult<()>
    where
        F: FnOnce(&mut Self) -> PyResult<()>,
    {
        let n = self.batch.len();
        match body(self) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.rollback_to(n);
                Err(e)
            }
        }
    }

    fn append_from_dict_inner(&mut self, dict: &Bound<'_, PyDict>, weight: i64) -> PyResult<()> {
        self.with_rollback(|s| {
            let py = dict.py();
            s.append_pk_from_dict(py, dict)?;
            s.batch.weights.push(weight);
            let mut nulls: u64 = 0;
            for i in 0..s.payload_cols.len() {
                let (payload_idx, ci) = s.payload_cols[i];
                // A missing key and an explicit None are the same thing: NULL.
                match dict.get_item(s.col_keys[ci].bind(py))? {
                    Some(val) if !val.is_none() => s.append_column_value(ci, &val)?,
                    _ => s.append_null_column(ci, payload_idx, &mut nulls)?,
                }
            }
            s.batch.nulls.push(nulls);
            Ok(())
        })
    }
}

/// A payload column's `ColData` variant did not match its declared type code.
/// `ZSetBatch::new` derives one from the other, so reaching this means the batch
/// was assembled outside that constructor.
#[cold]
#[inline(never)]
fn variant_mismatch() -> ! {
    panic!("ColData variant does not match the column's type code")
}

#[pymethods]
impl PyZSetBatch {
    /// Construct a batch for `schema` — a `Schema`, a `Struct` subclass (whose
    /// declared `_schema` is unwrapped), or a bare list of `ColumnDef`, all
    /// resolved through [`resolve_py_schema`].
    #[new]
    #[pyo3(signature = (schema))]
    pub fn new(py: Python<'_>, schema: Bound<'_, PyAny>) -> PyResult<Self> {
        let rust_schema = Arc::clone(&resolve_py_schema(py, &schema)?.borrow().rust);
        let col_keys = rust_schema
            .columns
            .iter()
            .map(|c| PyString::intern(py, &c.name).unbind())
            .collect();
        let payload_cols = rust_schema.payload_columns().map(|(pi, ci, _)| (pi, ci)).collect();
        let batch = ZSetBatch::new(&rust_schema);
        Ok(PyZSetBatch {
            pk_stride: rust_schema.pk_stride() as u8,
            batch,
            col_keys,
            payload_cols,
            schema: rust_schema,
        })
    }

    /// Append one row from `{column_name: value}` keyword arguments; returns
    /// the batch so appends chain. `weight` defaults to 1.
    #[pyo3(signature = (weight = 1, **values))]
    pub fn append<'py>(
        slf: Bound<'py, Self>,
        weight: i64,
        values: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, Self>> {
        let dict = values.unwrap_or_else(|| PyDict::new(slf.py()));
        slf.borrow_mut().append_from_dict_inner(&dict, weight)?;
        Ok(slf)
    }

    /// Append rows from an iterable of dicts (one Rust call, no per-row
    /// Python→Rust crossing); returns the batch so calls chain. A per-row
    /// `_weight` key overrides `weight`.
    #[pyo3(signature = (rows, weight = 1))]
    pub fn extend<'py>(slf: Bound<'py, Self>, rows: Bound<'_, PyAny>, weight: i64) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        // Batch-level atomicity: `append_from_dict_inner` rolls back only the
        // current row, so a failure on row N would otherwise leave rows 0..N in
        // the batch. Wrapping the whole loop in `with_rollback` truncates back
        // to the pre-call length on any error, giving `extend` the same
        // all-or-nothing contract as the single-row appends.
        slf.borrow_mut().with_rollback(|s| {
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
        })?;
        Ok(slf)
    }

    #[getter]
    pub fn pks(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        pk_column_to_pylist(py, &self.schema, &self.batch.pks)
    }
    #[getter]
    pub fn columns(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        rust_batch_columns_to_py(py, self.schema.as_ref(), &self.batch, self.batch.len())
    }

    pub fn __len__(&self) -> usize {
        self.batch.len()
    }

    pub fn __repr__(&self) -> String {
        format!("ZSetBatch(len={})", self.batch.len())
    }
}

// ---------------------------------------------------------------------------
// Batch conversion helpers
// ---------------------------------------------------------------------------

use gnitz_wire::format_uuid;

/// Plain-hex parse for a non-UUID U128 column value: 1..=32 hex digits, no
/// hyphen stripping, no sign.
fn parse_plain_hex(s: &str) -> Option<u128> {
    let b = s.as_bytes();
    if b.is_empty() || b.len() > 32 || !b.iter().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    u128::from_str_radix(s, 16).ok()
}

/// Accept a Python int, `uuid.UUID` object (via `.int`), or string, returning
/// the 128-bit value. Int is tried first because it is the common case in
/// bulk inserts and avoids a Python attribute lookup per row.
///
/// The string arm is type-directed: a UUID column accepts only canonical UUID
/// text (`gnitz_wire::parse_uuid`), a U128 column only plain hex. Callers
/// without a column type in hand (seek keys, raw PKs) pass `None` and get the
/// union of the two forms.
fn extract_uuid_or_u128(val: &Bound<'_, PyAny>, tc: Option<TypeCode>) -> PyResult<u128> {
    if let Ok(n) = val.extract::<u128>() {
        return Ok(n);
    }
    if let Ok(attr) = val.getattr("int") {
        if let Ok(n) = attr.extract::<u128>() {
            return Ok(n);
        }
    }
    if let Ok(s) = val.extract::<String>() {
        return match tc {
            Some(TypeCode::UUID) => gnitz_wire::parse_uuid(&s),
            Some(_) => parse_plain_hex(&s),
            None => gnitz_wire::parse_uuid(&s).or_else(|| parse_plain_hex(&s)),
        }
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("invalid UUID/U128 hex string: {s:?}")));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected int, uuid.UUID object, or UUID string",
    ))
}

/// Write one fixed-width value as little-endian bytes into `dst` (length must
/// equal `tc.wire_stride()`). Zero-allocation; used for PK regions and as the
/// inner write of `write_fixed_le`.
fn write_fixed_le_into(dst: &mut [u8], tc: TypeCode, item: &Bound<'_, PyAny>) -> PyResult<()> {
    match tc {
        TypeCode::U8 => dst[0] = item.extract::<u8>()?,
        TypeCode::I8 => dst[0] = item.extract::<i8>()? as u8,
        TypeCode::U16 => dst.copy_from_slice(&item.extract::<u16>()?.to_le_bytes()),
        TypeCode::I16 => dst.copy_from_slice(&item.extract::<i16>()?.to_le_bytes()),
        TypeCode::U32 => dst.copy_from_slice(&item.extract::<u32>()?.to_le_bytes()),
        TypeCode::I32 => dst.copy_from_slice(&item.extract::<i32>()?.to_le_bytes()),
        TypeCode::F32 => dst.copy_from_slice(&item.extract::<f32>()?.to_le_bytes()),
        TypeCode::U64 => dst.copy_from_slice(&item.extract::<u64>()?.to_le_bytes()),
        TypeCode::I64 => dst.copy_from_slice(&item.extract::<i64>()?.to_le_bytes()),
        TypeCode::F64 => dst.copy_from_slice(&item.extract::<f64>()?.to_le_bytes()),
        TypeCode::String | TypeCode::U128 | TypeCode::UUID | TypeCode::Blob | TypeCode::I128 => {
            unreachable!("handled before write_fixed_le_into")
        }
    }
    Ok(())
}

/// Write one fixed-width value as little-endian bytes onto the tail of `buf`.
/// Encodes into a stack slot first: growing `buf` up front would zero-fill
/// bytes the write immediately overwrites, and the extraction call in between
/// stops the compiler from eliminating the fill.
fn write_fixed_le(buf: &mut Vec<u8>, tc: TypeCode, item: &Bound<'_, PyAny>) -> PyResult<()> {
    let stride = tc.wire_stride();
    let mut tmp = [0u8; 16];
    write_fixed_le_into(&mut tmp[..stride], tc, item)?;
    buf.extend_from_slice(&tmp[..stride]);
    Ok(())
}

/// Materialize a `PkColumn` as a Python list. A single-column key surfaces as
/// that column's own Python type — decoded through [`pk_value_from_tuple`], the
/// same decoder the row path uses, so `batch.pks[i]` and `row[pk_col]` can never
/// disagree about sign or UUID rendering. A compound (`Bytes`) key surfaces as
/// `bytes`: one packed PK region per row.
fn pk_column_to_pylist(py: Python<'_>, schema: &Schema, pks: &PkColumn) -> PyResult<Py<PyList>> {
    if let PkColumn::Bytes { stride, buf } = pks {
        let items: Vec<PyObject> = buf
            .chunks_exact(*stride as usize)
            .map(|c| pyo3::types::PyBytes::new(py, c).into_any().unbind())
            .collect();
        return Ok(PyList::new(py, items)?.unbind());
    }
    let stride = schema.pk_stride();
    let tc = schema.columns[schema.pk_indices()[0]].type_code;
    let items: Vec<PyObject> = (0..pks.len())
        .map(|i| pk_value_to_py(py, tc, pks.col_window(i, 0, stride).as_slice()))
        .collect::<PyResult<_>>()?;
    Ok(PyList::new(py, items)?.unbind())
}

/// Decode one PK column's native-LE bytes into a Python value. The 16-byte
/// integer types route through [`u128_value_to_py`] so a PK renders exactly as
/// the same column would in a payload; everything else is a fixed-width read.
fn pk_value_to_py(py: Python<'_>, tc: TypeCode, bytes: &[u8]) -> PyResult<PyObject> {
    match tc {
        TypeCode::UUID | TypeCode::U128 | TypeCode::I128 => {
            u128_value_to_py(py, u128::from_le_bytes(bytes.try_into().unwrap()), tc)
        }
        _ => Ok(read_fixed_le(py, tc, bytes)),
    }
}

/// Read one fixed-width value as a Python object (integers as int, floats as
/// float). Widths come from `slice.len()` via the shared
/// `read_signed_exact`/`read_unsigned_exact` pair, so the 1/2/4/8-byte table is
/// not restated here; only the sign and float distinctions are type-directed.
fn read_fixed_le(py: Python<'_>, tc: TypeCode, slice: &[u8]) -> PyObject {
    macro_rules! obj {
        ($v:expr) => {
            $v.into_pyobject(py).unwrap().into_any().unbind()
        };
    }
    match tc {
        TypeCode::F32 => obj!(f32::from_le_bytes(slice.try_into().unwrap())),
        TypeCode::F64 => obj!(f64::from_le_bytes(slice.try_into().unwrap())),
        _ if tc.is_signed_int() => obj!(gnitz_wire::read_signed_exact(slice)),
        _ => obj!(gnitz_wire::read_unsigned_exact(slice)),
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
        _ => x.into_pyobject(py)?.into_any().unbind(),
    })
}

/// Decode one payload cell into a Python object, null bit first: a set bit is
/// `None` regardless of the stored value. Per-`ColData` decode — Fixed →
/// [`read_fixed_le`], Strings → str, Bytes → bytes, U128s → [`u128_value_to_py`]
/// — shared by the row build and the `scalars` column loop. `tc` and `stride`
/// are the column's type code and wire stride, precomputed by the caller
/// (`stride` is read only for the Fixed arm).
fn cell_to_py(
    py: Python<'_>,
    col: &ColData,
    row: usize,
    is_null: bool,
    tc: TypeCode,
    stride: usize,
) -> PyResult<PyObject> {
    if is_null {
        return Ok(py.None());
    }
    Ok(match col {
        ColData::Fixed(buf) => read_fixed_le(py, tc, &buf[row * stride..(row + 1) * stride]),
        ColData::Strings(v) => match &v[row] {
            Some(s) => s.into_pyobject(py)?.into_any().unbind(),
            None => py.None(),
        },
        ColData::Bytes(v) => match &v[row] {
            Some(b) => pyo3::types::PyBytes::new(py, b).into_any().unbind(),
            None => py.None(),
        },
        ColData::U128s(v) => u128_value_to_py(py, v[row], tc)?,
    })
}

// ---------------------------------------------------------------------------
// Lazy batch infrastructure
// ---------------------------------------------------------------------------

/// Decode one presented column: its physical column index plus the resolved
/// address (PK byte offset or payload slot, width, and type code) the row build
/// reads through. `ColumnLocator` already carries the type code, so the per-row
/// loop never touches the schema.
type PresentedCol = (usize, ColumnLocator);

struct SharedBatchData {
    schema: Arc<Schema>,
    batch: ZSetBatch,
    /// Pre-computed field-name tuple, created once and shared across all iterators.
    fields: Py<PyTuple>,
    /// field name → presented position, built once and shared via Arc for O(1)
    /// row attr lookup. Positions index into `present`, so they line up with
    /// `fields` and the per-row values tuple.
    field_index: Arc<HashMap<String, usize>>,
    /// The columns to present, in presentation order: all of them when
    /// `include_hidden`, the non-hidden ones otherwise. Rows, `fields`,
    /// `field_index`, and `scalars` all index through this, so every
    /// presentation surface agrees on positions.
    present: Vec<PresentedCol>,
}

fn make_shared_batch_data(
    py: Python<'_>,
    s: Arc<Schema>,
    b: ZSetBatch,
    include_hidden: bool,
) -> PyResult<Arc<SharedBatchData>> {
    let locate = |ci: usize| (ci, SchemaFacts::locate(s.as_ref(), ci));
    let present: Vec<PresentedCol> = if include_hidden {
        (0..s.columns.len()).map(locate).collect()
    } else {
        s.visible_columns().map(|(ci, _)| locate(ci)).collect()
    };
    let names: Vec<&str> = present.iter().map(|&(ci, _)| s.columns[ci].name.as_str()).collect();
    let fields = PyTuple::new(py, names)?.unbind();
    let field_index = Arc::new(
        present
            .iter()
            .enumerate()
            .map(|(pos, &(ci, _))| (s.columns[ci].name.clone(), pos))
            .collect::<HashMap<String, usize>>(),
    );
    Ok(Arc::new(SharedBatchData {
        schema: s,
        batch: b,
        fields,
        field_index,
        present,
    }))
}

/// Decode one cell at `loc` in `row`, for either a PK or a payload column.
/// The single per-cell decode, shared by the row build and `scalars`. A PK
/// column reads its own bytes straight out of the PK region — no whole-tuple
/// copy, so a one-column read costs one column.
fn value_at(py: Python<'_>, data: &SharedBatchData, ci: usize, loc: ColumnLocator, row: usize) -> PyResult<PyObject> {
    let tc = TypeCode::from_validated_u8(loc.type_code());
    match loc {
        ColumnLocator::Pk { byte_off, size, .. } => {
            let w = data.batch.pks.col_window(row, byte_off as usize, size as usize);
            pk_value_to_py(py, tc, w.as_slice())
        }
        ColumnLocator::Payload { slot, size, .. } => {
            let is_null = null_word_get(data.batch.nulls[row], slot as usize);
            cell_to_py(py, &data.batch.columns[ci], row, is_null, tc, size as usize)
        }
    }
}

/// Build Python values for a single row from Rust data, appending to `out`.
fn build_row_values_into(py: Python<'_>, data: &SharedBatchData, row: usize, out: &mut Vec<PyObject>) -> PyResult<()> {
    for &(ci, loc) in &data.present {
        out.push(value_at(py, data, ci, loc, row)?);
    }
    Ok(())
}

/// Build the `Row` object for one row, reusing `buf` as scratch across calls.
fn make_row(py: Python<'_>, data: &Arc<SharedBatchData>, row: usize, buf: &mut Vec<PyObject>) -> PyResult<PyObject> {
    buf.clear();
    build_row_values_into(py, data, row, buf)?;
    // `drain` hands the values over already-owned, so the tuple build costs no
    // refcount traffic — and the Vec keeps its capacity for the next row.
    let values = PyTuple::new(py, buf.drain(..))?.unbind();
    Ok(Py::new(
        py,
        PyRow {
            fields: data.fields.clone_ref(py),
            values,
            weight: data.batch.weights[row],
            field_index: Arc::clone(&data.field_index),
        },
    )?
    .into_any())
}

// ---------------------------------------------------------------------------
// PyRustBatch — lazy batch wrapper (read path only)
// ---------------------------------------------------------------------------

#[pyclass(name = "RustBatch")]
pub struct PyRustBatch {
    data: Arc<SharedBatchData>,
}

#[pymethods]
impl PyRustBatch {
    #[getter]
    fn pks(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        pk_column_to_pylist(py, &self.data.schema, &self.data.batch.pks)
    }

    #[getter]
    fn weights(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        Ok(PyList::new(py, &self.data.batch.weights)?.unbind())
    }

    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        rust_batch_columns_to_py(py, &self.data.schema, &self.data.batch, self.data.batch.len())
    }

    fn __len__(&self) -> usize {
        self.data.batch.len()
    }

    fn __repr__(&self) -> String {
        format!("RustBatch(len={})", self.data.batch.len())
    }
}

/// Materialize per-column value lists, indexed by *physical* column. A PK column
/// holds an empty list — the PK region is surfaced through `.pks`. Decoding runs
/// through [`cell_to_py`] under the column's resolved address, so a NULL reads
/// back as `None` here exactly as it does through a `Row` or `scalars()`.
fn rust_batch_columns_to_py(py: Python<'_>, schema: &Schema, batch: &ZSetBatch, n: usize) -> PyResult<Py<PyList>> {
    let mut col_lists: Vec<PyObject> = Vec::with_capacity(schema.columns.len());
    for ci in 0..schema.columns.len() {
        let items: Vec<PyObject> = match SchemaFacts::locate(schema, ci) {
            ColumnLocator::Pk { .. } => Vec::new(),
            ColumnLocator::Payload { slot, size, .. } => {
                let tc = schema.columns[ci].type_code;
                (0..n)
                    .map(|i| {
                        let is_null = null_word_get(batch.nulls[i], slot as usize);
                        cell_to_py(py, &batch.columns[ci], i, is_null, tc, size as usize)
                    })
                    .collect::<PyResult<_>>()?
            }
        };
        col_lists.push(PyList::new(py, items)?.into_any().unbind());
    }
    Ok(PyList::new(py, col_lists)?.unbind())
}

// ---------------------------------------------------------------------------
// PyScanResult — Rust-backed ScanResult
// ---------------------------------------------------------------------------

#[pyclass(name = "ScanResult")]
pub struct PyScanResult {
    data: Option<Arc<SharedBatchData>>,
    #[pyo3(get)]
    lsn: u64,
}

#[pymethods]
impl PyScanResult {
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.data {
            None => Ok(py.None()),
            Some(d) => Ok(rust_schema_to_py(py, &d.schema)?.into_any()),
        }
    }

    #[getter]
    fn batch(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.data {
            None => Ok(py.None()),
            Some(d) => Ok(Py::new(py, PyRustBatch { data: Arc::clone(d) })?.into_any()),
        }
    }

    fn __iter__(&self) -> PyRowIterator {
        PyRowIterator {
            data: self.data.clone(),
            row_buf: Vec::new(),
            pos: 0,
        }
    }

    /// Truthiness follows from this: CPython derives `__bool__` from `__len__`
    /// when a type defines no `nb_bool`.
    fn __len__(&self) -> usize {
        self.data.as_ref().map_or(0, |d| d.batch.len())
    }

    fn all(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let Some(data) = &self.data else {
            return Ok(PyList::empty(py).unbind());
        };
        let mut buf = Vec::with_capacity(data.present.len());
        let rows: Vec<PyObject> = (0..data.batch.len())
            .map(|i| make_row(py, data, i, &mut buf))
            .collect::<PyResult<_>>()?;
        Ok(PyList::new(py, rows)?.unbind())
    }

    /// The first row, or `None` on an empty result.
    fn first(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.data {
            Some(d) if !d.batch.is_empty() => make_row(py, d, 0, &mut Vec::new()),
            _ => Ok(py.None()),
        }
    }

    fn one(&self, py: Python<'_>) -> PyResult<PyObject> {
        let n = self.__len__();
        if n != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Expected exactly 1 row, got {n}"
            )));
        }
        self.first(py)
    }

    fn one_or_none(&self, py: Python<'_>) -> PyResult<PyObject> {
        let n = self.__len__();
        if n > 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Expected at most 1 row, got {n}"
            )));
        }
        self.first(py)
    }

    fn mappings(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let Some(data) = &self.data else {
            return Ok(PyList::empty(py).unbind());
        };
        let fields: Vec<Bound<'_, PyAny>> = data.fields.bind(py).iter().collect();
        let mut row_buf = Vec::with_capacity(fields.len());
        let dicts: Vec<Py<PyDict>> = (0..data.batch.len())
            .map(|i| {
                row_buf.clear();
                build_row_values_into(py, data, i, &mut row_buf)?;
                let dict = PyDict::new(py);
                for (name, val) in fields.iter().zip(&row_buf) {
                    dict.set_item(name, val)?;
                }
                Ok(dict.unbind())
            })
            .collect::<PyResult<_>>()?;
        Ok(PyList::new(py, dicts)?.unbind())
    }

    #[pyo3(signature = (col=None))]
    fn scalars(&self, py: Python<'_>, col: Option<PyObject>) -> PyResult<Py<PyList>> {
        let Some(data) = &self.data else {
            return Ok(PyList::empty(py).unbind());
        };
        // Resolve col: None→first presented column, int→presented position
        // (consistent with row indexing), str→presented-name lookup.
        let pos = match col {
            None => 0usize,
            Some(ref obj) => {
                if let Ok(idx) = obj.extract::<usize>(py) {
                    idx
                } else if let Ok(name) = obj.extract::<String>(py) {
                    data.field_index
                        .get(&name)
                        .copied()
                        .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(name))?
                } else {
                    return Err(pyo3::exceptions::PyTypeError::new_err("col must be int or str"));
                }
            }
        };
        // The presented-column table already holds this column's resolved
        // address, so the row loop below does no schema lookups.
        let (ci, loc) = *data
            .present
            .get(pos)
            .ok_or_else(|| pyo3::exceptions::PyIndexError::new_err("column index out of range"))?;
        let items: Vec<PyObject> = (0..data.batch.len())
            .map(|i| value_at(py, data, ci, loc, i))
            .collect::<PyResult<_>>()?;
        Ok(PyList::new(py, items)?.unbind())
    }
}

// ---------------------------------------------------------------------------
// PyRowIterator
// ---------------------------------------------------------------------------

#[pyclass(name = "RowIterator")]
pub struct PyRowIterator {
    data: Option<Arc<SharedBatchData>>,
    row_buf: Vec<PyObject>,
    pos: usize,
}

#[pymethods]
impl PyRowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let Some(data) = self.data.as_ref() else {
            return Ok(None);
        };
        if self.pos >= data.batch.len() {
            return Ok(None);
        }
        let row = make_row(py, data, self.pos, &mut self.row_buf)?;
        self.pos += 1;
        Ok(Some(row))
    }
}

// ---------------------------------------------------------------------------
// GnitzClient
// ---------------------------------------------------------------------------

/// Build a lazy `PyScanResult` from one `(schema, batch, lsn)` triple — the
/// shape every read path resolves to, whether it came back from a sync
/// `scan`/`seek`, one relation of a `scan_many`, or the async loop.
fn triple_to_lazy(
    py: Python<'_>,
    triple: (Option<Arc<Schema>>, Option<ZSetBatch>, u64),
    include_hidden: bool,
) -> PyResult<Py<PyScanResult>> {
    let (opt_schema, opt_batch, view_lsn) = triple;
    let data = match opt_schema {
        Some(s) => {
            let b = opt_batch.unwrap_or_else(|| ZSetBatch::new(s.as_ref()));
            Some(make_shared_batch_data(py, s, b, include_hidden)?)
        }
        None => None,
    };
    Py::new(py, PyScanResult { data, lsn: view_lsn })
}

/// `triple_to_lazy` over a `Result`, for the sync client's read methods.
fn response_to_lazy(
    py: Python<'_>,
    result: gnitz_core::ScanResult,
    include_hidden: bool,
) -> PyResult<Py<PyScanResult>> {
    triple_to_lazy(py, to_py_err(result)?, include_hidden)
}

#[pyclass(name = "GnitzClient")]
pub struct PyGnitzClient {
    inner: Option<GnitzClient>,
}

impl PyGnitzClient {
    /// The still-open client, or a `GnitzError` if `close()` already ran.
    fn live(&mut self) -> PyResult<&mut GnitzClient> {
        self.inner
            .as_mut()
            .ok_or_else(|| GnitzError::new_err("client already closed"))
    }
}

#[pymethods]
impl PyGnitzClient {
    #[new]
    pub fn new(py: Python<'_>, socket_path: &str) -> PyResult<Self> {
        // Connect + HELLO are blocking syscalls (up to a 10 s timeout for a
        // `tls://` target); drop the GIL across them as every other blocking
        // method here does.
        to_py_err(py.allow_threads(|| GnitzClient::connect(socket_path))).map(|c| PyGnitzClient { inner: Some(c) })
    }

    /// The client's current OCC basis (the running max of observed server
    /// watermarks, seeded from the HELLO ACK at connect). Read-only, for tests.
    #[getter]
    fn last_seen_lsn(&mut self) -> PyResult<u64> {
        Ok(self.live()?.last_seen_lsn())
    }

    pub fn close(&mut self) {
        if let Some(c) = self.inner.take() {
            c.close();
        }
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject) -> bool {
        self.close();
        false
    }

    // ----- DDL -----

    pub fn create_schema(&mut self, py: Python<'_>, name: &str) -> PyResult<u64> {
        let c = self.live()?;
        to_py_err(py.allow_threads(|| c.create_schema(name)))
    }

    pub fn drop_schema(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        let c = self.live()?;
        to_py_err(py.allow_threads(|| c.drop_schema(name)))
    }

    /// create_table(schema_name, table_name, columns, unique_pk=True).
    /// `columns` may be a `Schema`, a `Struct` subclass, or a list of
    /// `ColumnDef` — all resolved through [`resolve_py_schema`], so the PK
    /// columns come from the same rule every other schema surface applies.
    /// Partitioned, default distribution; no inline UNIQUE surface.
    #[pyo3(signature = (schema_name, table_name, columns, unique_pk = true))]
    pub fn create_table(
        &mut self,
        py: Python<'_>,
        schema_name: &str,
        table_name: &str,
        columns: Bound<'_, PyAny>,
        unique_pk: bool,
    ) -> PyResult<u64> {
        let schema = Arc::clone(&resolve_py_schema(py, &columns)?.borrow().rust);
        let pk: Vec<u32> = schema.pk_indices().iter().map(|&i| i as u32).collect();
        let c = self.live()?;
        to_py_err(
            py.allow_threads(|| {
                c.create_table(schema_name, table_name, &schema.columns, &pk, unique_pk, false, 0, &[])
            }),
        )
    }

    pub fn drop_table(&mut self, py: Python<'_>, schema_name: &str, table_name: &str) -> PyResult<()> {
        let c = self.live()?;
        to_py_err(py.allow_threads(|| c.drop_table(schema_name, table_name)))
    }

    // ----- DML -----

    /// push(target_id, batch) -> ingest_lsn: int. Silent-upsert on PK conflict
    /// (DBSP z-set retraction semantics); SQL-standard rejection is reached via
    /// `INSERT` through `execute_sql`.
    pub fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>) -> PyResult<u64> {
        // Hold the `PyRef` guard here (it is `!Ungil`) and pass only the plain
        // `&Schema`/`&ZSetBatch` into `allow_threads`, so the GIL is free during
        // the blocking push without cloning the batch.
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        let c = self.live()?;
        to_py_err(py.allow_threads(|| c.push(target_id, schema, b)))
    }

    /// delete(target_id, schema, pks) — `pks` is a list where each element is
    /// either an int (single-column PK) or bytes (a packed compound PK).
    pub fn delete(
        &mut self,
        py: Python<'_>,
        target_id: u64,
        schema: PyRef<'_, PySchema>,
        pks: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let rust_schema = Arc::clone(&schema.rust);
        let pk_col = py_pks_to_column(&rust_schema, &pks)?;
        let c = self.live()?;
        to_py_err(py.allow_threads(|| c.delete(target_id, &rust_schema, pk_col)))
    }

    /// Open an atomic write-batch transaction as a context manager. Buffer
    /// writes with `txn.push` / `txn.delete`; a clean `with`-block exit commits
    /// the whole bundle atomically under one durable zone LSN, while an
    /// exception discards it (rollback — nothing was sent).
    ///
    /// ```python
    /// with client.transaction() as txn:
    ///     txn.push(orders_tid, orders_batch)
    ///     txn.delete(carts_tid, cart_schema, [pk])
    /// ```
    pub fn transaction(slf: Bound<'_, PyGnitzClient>) -> PyResult<PyTxn> {
        to_py_err(slf.borrow_mut().live()?.txn_begin())?;
        Ok(PyTxn {
            open: true,
            client: slf.unbind(),
        })
    }

    // ----- Views -----

    pub fn create_view(
        &mut self,
        py: Python<'_>,
        schema_name: &str,
        view_name: &str,
        source_table_id: u64,
        output_schema: PyRef<'_, PySchema>,
    ) -> PyResult<u64> {
        let cols = &output_schema.rust.columns;
        let c = self.live()?;
        to_py_err(py.allow_threads(|| c.create_view(schema_name, view_name, source_table_id, cols)))
    }

    /// Build a `CircuitBuilder` rooted at `source_table_id` (view id defaults
    /// to 0, as hand-built Python circuits use).
    pub fn circuit_builder(&self, source_table_id: u64) -> PyCircuitBuilder {
        PyCircuitBuilder::new(source_table_id, 0)
    }

    /// create_view_with_circuit. `columns` may be a `Schema`, a `Struct`
    /// subclass (`_schema`), or a list of `ColumnDef`.
    pub fn create_view_with_circuit(
        &mut self,
        py: Python<'_>,
        schema_name: &str,
        view_name: &str,
        circuit: PyRef<'_, PyCircuit>,
        columns: Bound<'_, PyAny>,
    ) -> PyResult<u64> {
        let circuit = circuit.inner.clone();
        let schema = Arc::clone(&resolve_py_schema(py, &columns)?.borrow().rust);
        // Hand-built circuits from the Python API emit a single output PK at slot 0.
        let c = self.live()?;
        to_py_err(
            py.allow_threads(|| c.create_view_with_circuit(schema_name, view_name, "", circuit, &schema.columns, &[0])),
        )
    }

    pub fn drop_view(&mut self, py: Python<'_>, schema_name: &str, view_name: &str) -> PyResult<()> {
        let c = self.live()?;
        to_py_err(py.allow_threads(|| c.drop_view(schema_name, view_name)))
    }

    /// resolve_table(schema_name, table_name) -> (tid: int, schema: Schema)
    pub fn resolve_table(&mut self, py: Python<'_>, schema_name: &str, table_name: &str) -> PyResult<PyObject> {
        let c = self.live()?;
        let (tid, schema) = to_py_err(py.allow_threads(|| c.resolve_table_or_view_id(schema_name, table_name)))?;
        let py_schema = rust_schema_to_py(py, &Arc::new(schema))?.into_any();
        let tid_obj = tid.into_pyobject(py)?.into_any().unbind();
        Ok(PyTuple::new(py, [tid_obj, py_schema])?.into_any().unbind())
    }

    /// scan(target_id, include_hidden=False) -> ScanResult
    #[pyo3(signature = (target_id, include_hidden = false))]
    pub fn scan(&mut self, py: Python<'_>, target_id: u64, include_hidden: bool) -> PyResult<Py<PyScanResult>> {
        let c = self.live()?;
        let result = py.allow_threads(|| c.scan(target_id));
        response_to_lazy(py, result, include_hidden)
    }

    /// scan_many(target_ids, include_hidden=False) -> list[ScanResult]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, returned
    /// in request order. An atomic multi-table transaction is never observed
    /// torn across the result list. Same row decoding as `scan`.
    #[pyo3(signature = (target_ids, include_hidden = false))]
    pub fn scan_many(
        &mut self,
        py: Python<'_>,
        target_ids: Vec<u64>,
        include_hidden: bool,
    ) -> PyResult<Vec<Py<PyScanResult>>> {
        let c = self.live()?;
        let results = to_py_err(py.allow_threads(|| c.scan_many(&target_ids)))?;
        results
            .into_iter()
            .map(|triple| triple_to_lazy(py, triple, include_hidden))
            .collect()
    }

    /// seek(table_id, pk=0, include_hidden=False) -> ScanResult.
    /// `pk` may be an `int` (narrow single-PK tables) or `bytes` (compound or
    /// wide-byte PKs).
    #[pyo3(signature = (table_id, pk = None, include_hidden = false))]
    pub fn seek(
        &mut self,
        py: Python<'_>,
        table_id: u64,
        pk: Option<Bound<'_, PyAny>>,
        include_hidden: bool,
    ) -> PyResult<Py<PyScanResult>> {
        let t = match pk {
            Some(ref obj) => pk_tuple_from_py(obj)?,
            None => gnitz_core::PkTuple::from_u128_narrow(0),
        };
        let c = self.live()?;
        let result = py.allow_threads(|| c.seek(table_id, &t));
        response_to_lazy(py, result, include_hidden)
    }

    /// seek_by_index(table_id, col_indices, key_vals, include_hidden=False) -> ScanResult.
    ///
    /// `col_indices` is the index's FULL declared column list (the server matches
    /// the circuit by exact list); `key_vals` supplies the leading native key
    /// values (`len(key_vals)` may be `< len(col_indices)` for a leading-prefix
    /// seek). Arity is validated once inside `GnitzClient::seek_by_index` (the
    /// single choke point for every binding), so no validation is duplicated here.
    /// Key values are decoded through `extract_uuid_or_u128`, so a UUID-keyed seek
    /// accepts the same `uuid.UUID` / hex-string forms the insert paths do.
    #[pyo3(signature = (table_id, col_indices, key_vals, include_hidden = false))]
    pub fn seek_by_index(
        &mut self,
        py: Python<'_>,
        table_id: u64,
        col_indices: Vec<u32>,
        key_vals: Bound<'_, PyList>,
        include_hidden: bool,
    ) -> PyResult<Py<PyScanResult>> {
        let keys: Vec<u128> = key_vals
            .iter()
            .map(|item| extract_uuid_or_u128(&item, None))
            .collect::<PyResult<_>>()?;
        let c = self.live()?;
        let result = py.allow_threads(|| c.seek_by_index(table_id, &col_indices, &keys));
        response_to_lazy(py, result, include_hidden)
    }

    /// execute_sql(sql, schema_name="public") -> list of result dicts
    #[pyo3(signature = (sql, schema_name = "public"))]
    pub fn execute_sql(&mut self, py: Python<'_>, sql: &str, schema_name: &str) -> PyResult<PyObject> {
        // Plan + execute (all wire I/O, no Python) with the GIL released.
        let client_ref = self.live()?;
        let results = py
            .allow_threads(|| SqlPlanner::new(client_ref, schema_name).execute(sql))
            .map_err(|e| err_with_conflict(&e, e.is_conflict()))?;

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
                SqlResult::Altered { object, name } => {
                    d.set_item("type", "Altered")?;
                    d.set_item("object", object)?;
                    d.set_item("name", name)?;
                }
                SqlResult::RowsAffected { count } => {
                    d.set_item("type", "RowsAffected")?;
                    d.set_item("count", count)?;
                }
                SqlResult::Rows { schema, batch } => {
                    d.set_item("type", "Rows")?;
                    d.set_item(
                        "rows",
                        triple_to_lazy(py, (Some(Arc::new(schema)), Some(batch), 0), false)?,
                    )?;
                }
                SqlResult::TransactionStarted => {
                    d.set_item("type", "TransactionStarted")?;
                }
                SqlResult::TransactionCommitted { lsn } => {
                    d.set_item("type", "TransactionCommitted")?;
                    d.set_item("lsn", lsn)?;
                }
                SqlResult::TransactionRolledBack => {
                    d.set_item("type", "TransactionRolledBack")?;
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

/// Atomic write-batch transaction context manager: an RAII handle on the
/// client's open transaction. `push`/`delete` are the client's own write methods
/// — they buffer because a transaction is open, exactly as a SQL `INSERT` between
/// `BEGIN` and `COMMIT` does. Nothing reaches the server until the `with`-block
/// exits cleanly.
#[pyclass(name = "Txn")]
pub struct PyTxn {
    /// False once `__exit__` has committed or discarded the transaction.
    open: bool,
    client: Py<PyGnitzClient>,
}

#[pymethods]
impl PyTxn {
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Buffer a push of `batch` into `target_id` under conflict mode `mode`
    /// (`"update"` — the default — or `"error"`).
    #[pyo3(signature = (target_id, batch, mode = "update"))]
    pub fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>, mode: &str) -> PyResult<()> {
        let m: WireConflictMode = mode.parse().map_err(gnitz_err)?;
        // Hold the `PyRef` guard here and pass only the plain `&Schema`/
        // `&ZSetBatch` into the closure, so `with_client` can drop the GIL.
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        self.with_client(py, move |c| c.push_with_mode(target_id, schema, b, m).map(|_| ()))
    }

    /// Buffer a delete of `pks` from `target_id` (same PK forms as
    /// `GnitzClient.delete`).
    pub fn delete(
        &mut self,
        py: Python<'_>,
        target_id: u64,
        schema: PyRef<'_, PySchema>,
        pks: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let rust_schema = Arc::clone(&schema.rust);
        let pk_col = py_pks_to_column(&rust_schema, &pks)?;
        self.with_client(py, move |c| c.delete(target_id, &rust_schema, pk_col))
    }

    /// Commit the bundle on a clean exit; discard it (rollback) if the block
    /// raised. Returns `False` so an in-block exception is never suppressed.
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: PyObject,
        _exc_val: PyObject,
        _exc_tb: PyObject,
    ) -> PyResult<bool> {
        if !self.open {
            return Ok(false);
        }
        let clean = exc_type.is_none(py);
        let r = self.with_client(py, |c| {
            if clean {
                c.txn_commit().map(|_| ())
            } else {
                c.txn_rollback()
            }
        });
        // Consumed either way — a failed COMMIT has already closed it client-side.
        self.open = false;
        r?;
        Ok(false)
    }
}

impl PyTxn {
    /// Run `f` against the transaction's client. Errors if the transaction is
    /// already closed, or the client is. The call itself runs with the GIL
    /// released — `txn_commit` blocks on a durable server ACK, and freezing every
    /// other Python thread for that round-trip is exactly what the sync client's
    /// write methods already avoid.
    fn with_client(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut GnitzClient) -> Result<(), ClientError> + Send,
    ) -> PyResult<()> {
        if !self.open {
            return Err(GnitzError::new_err("transaction already committed or discarded"));
        }
        let mut cref = self.client.bind(py).borrow_mut();
        let c = cref.live()?;
        to_py_err(py.allow_threads(move || f(c)))
    }
}

#[pyclass(name = "ExprBuilder")]
#[derive(Default)]
pub struct PyExprBuilder {
    inner: ExprBuilder,
}

#[pymethods]
impl PyExprBuilder {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_col_int(&mut self, col_idx: usize) -> u32 {
        self.inner.load_col_int(col_idx)
    }
    pub fn load_const(&mut self, value: i64) -> u32 {
        self.inner.load_const(value)
    }
    pub fn cmp_gt(&mut self, a: u32, b: u32) -> u32 {
        self.inner.cmp_gt(a, b)
    }

    /// Compile the program built so far. The builder stays usable, so one
    /// builder can yield programs for several result registers.
    pub fn build(&self, result_reg: u32) -> PyExprProgram {
        PyExprProgram {
            inner: self.inner.clone().build(result_reg),
        }
    }
}

#[pyclass(name = "ExprProgram")]
pub struct PyExprProgram {
    pub(crate) inner: ExprProgram,
}

#[pymethods]
impl PyExprProgram {
    pub fn __repr__(&self) -> String {
        format!(
            "ExprProgram(num_regs={}, result_reg={})",
            self.inner.num_regs, self.inner.result_reg
        )
    }
}

// ---------------------------------------------------------------------------
// CircuitBuilder + CircuitGraph
// ---------------------------------------------------------------------------

#[pyclass(name = "CircuitBuilder")]
pub struct PyCircuitBuilder {
    inner: CircuitBuilder,
}

#[pymethods]
impl PyCircuitBuilder {
    /// `CircuitBuilder(primary_source_id, view_id=0)`. Hand-built Python
    /// circuits leave `view_id` at its default; `GnitzClient.circuit_builder`
    /// is the usual entry point.
    #[new]
    #[pyo3(signature = (primary_source_id, view_id = 0))]
    pub fn new(primary_source_id: u64, view_id: u64) -> Self {
        PyCircuitBuilder {
            inner: CircuitBuilder::new(view_id, primary_source_id),
        }
    }

    pub fn input_delta(&mut self) -> u64 {
        self.inner.input_delta()
    }
    pub fn negate(&mut self, input: u64) -> u64 {
        self.inner.negate(input)
    }
    pub fn union(&mut self, a: u64, b: u64) -> u64 {
        self.inner.union(a, b)
    }
    pub fn distinct(&mut self, input: u64) -> u64 {
        self.inner.distinct(input)
    }

    /// filter(input, expr=None) — clones ExprProgram so Python keeps its reference.
    #[pyo3(signature = (input, expr = None))]
    pub fn filter(&mut self, input: u64, expr: Option<PyRef<'_, PyExprProgram>>) -> u64 {
        let expr_opt = expr.map(|e| e.inner.clone());
        self.inner.filter(input, expr_opt)
    }

    #[pyo3(signature = (input, projection = None))]
    pub fn map(&mut self, input: u64, projection: Option<Vec<usize>>) -> u64 {
        self.inner.map(input, projection.as_deref().unwrap_or(&[]))
    }

    pub fn join(&mut self, delta: u64, trace_table_id: u64) -> u64 {
        self.inner.join(delta, trace_table_id)
    }

    #[pyo3(signature = (input, group_by_cols, agg_func_id = 0, agg_col_idx = 0))]
    pub fn reduce(&mut self, input: u64, group_by_cols: Vec<usize>, agg_func_id: u64, agg_col_idx: usize) -> u64 {
        self.inner.reduce(input, &group_by_cols, agg_func_id, agg_col_idx)
    }

    pub fn sink(&mut self, input: u64) -> u64 {
        self.inner.sink(input)
    }

    /// Snapshot the graph built so far as a Circuit for
    /// `create_view_with_circuit`. The builder stays usable.
    pub fn build(&self) -> PyCircuit {
        PyCircuit {
            inner: self.inner.clone().build(),
        }
    }
}

#[pyclass(name = "Circuit")]
pub struct PyCircuit {
    pub(crate) inner: Circuit,
}

#[pymethods]
impl PyCircuit {
    pub fn __repr__(&self) -> String {
        format!(
            "Circuit(view_id={}, nodes={})",
            self.inner.view_id,
            self.inner.nodes.len()
        )
    }
}

/// Build a `PkTuple` for the wire-only `seek` paths, which have no schema at the
/// FFI boundary: `bytes` is taken verbatim, and an integer becomes a narrow
/// 16-byte tuple whose high padding is inert (the server reads only the
/// column's own stride). The signed fallback keeps a negative key packing to the
/// same two's-complement bytes the typed append path writes.
fn pk_tuple_from_py(pk: &Bound<'_, PyAny>) -> PyResult<gnitz_core::PkTuple> {
    // bytes first: `downcast` rejects a non-bytes value without materializing a
    // PyErr, whereas `extract_uuid_or_u128` falls through to `getattr("int")`.
    if let Ok(bytes) = pk.downcast::<pyo3::types::PyBytes>() {
        return gnitz_core::PkTuple::try_from_bytes(bytes.as_bytes()).map_err(pyo3::exceptions::PyValueError::new_err);
    }
    if let Ok(val) = extract_uuid_or_u128(pk, None) {
        return Ok(gnitz_core::PkTuple::from_u128_narrow(val));
    }
    if let Ok(val) = pk.extract::<i128>() {
        return Ok(gnitz_core::PkTuple::from_u128_narrow(val as u128));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "pk must be int, uuid.UUID, UUID string, or bytes",
    ))
}

// ---------------------------------------------------------------------------
// AsyncTransport — background I/O thread for async pipelining
// ---------------------------------------------------------------------------

/// A pipelined I/O operation. Push frames are encoded on the submitting
/// thread (schema always included — the async push path is cold); scan/seek
/// are packed on the I/O thread so the cache-aware schema-version stamp reads
/// the session's own cache (never shared cross-thread).
enum IoOp {
    /// Pre-encoded push frame; resolves with u64 (seek_pk = ingest LSN).
    Push(gnitz_core::MessageParts),
    /// Full-table scan; resolves with PyScanResult.
    Scan,
    /// Point seek by PK; resolves with PyScanResult.
    Seek(gnitz_core::PkTuple),
    /// Consistent multi-relation scan; resolves with list[PyScanResult].
    ScanMulti(Vec<u64>),
}

struct IoRequest {
    op: IoOp,
    target_id: u64,
    /// The future to resolve, paired with whether its rows present hidden
    /// columns. `include_hidden` is a property of how the *result* is surfaced,
    /// not of the request, so it travels with the future all the way to the GIL
    /// block instead of being copied into the recv and decode types.
    future: Py<PyAny>,
    include_hidden: bool,
}

/// Bound on the I/O request channel. Limits RAM when Python sends faster than
/// the network flushes. `enqueue` returns GnitzError if the channel is full.
const IO_CHANNEL_DEPTH: usize = 4096;

/// Cap on requests merged into one natural-batching cycle.
const IO_BATCH_MAX: usize = 1024;

#[pyclass(name = "AsyncTransport")]
struct PyAsyncTransport {
    tx: Option<std::sync::mpsc::SyncSender<IoRequest>>,
    /// dup'd fd of the connection's stream socket. The I/O thread owns the
    /// session (and its transport); the waker's `shutdown` (fired on drop)
    /// wakes any in-flight `recv_framed` on the shared open file description,
    /// even after the I/O thread has dropped the session (the integer may
    /// already be recycled).
    waker: Option<gnitz_core::TransportWaker>,
    /// `event_loop.create_future`, bound once — the enqueue path calls it per
    /// operation, and resolving the attribute by name each time would build its
    /// name string every call. Same treatment `call_soon_threadsafe` gets.
    create_future: Py<PyAny>,
    client_id: u64,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl PyAsyncTransport {
    fn enqueue(&self, py: Python<'_>, op: IoOp, target_id: u64, include_hidden: bool) -> PyResult<PyObject> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| GnitzError::new_err("connection closed"))?;
        let fut = self.create_future.call0(py)?;
        tx.try_send(IoRequest {
            op,
            target_id,
            include_hidden,
            future: fut.clone_ref(py),
        })
        .map_err(|e| match e {
            std::sync::mpsc::TrySendError::Full(_) => GnitzError::new_err("transport queue full"),
            std::sync::mpsc::TrySendError::Disconnected(_) => GnitzError::new_err("I/O thread exited"),
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
        // The transport owns the connection from birth, so every early
        // return below closes it via RAII; on success it moves into the
        // I/O thread, which closes it when it exits. Connect + HELLO run
        // synchronously on the calling thread — captures the negotiated
        // payload limit before the I/O thread starts queueing reads. Drop
        // the GIL across the blocking syscalls so other Python threads
        // can progress if the server is slow to respond.
        // The async transport builds a bare `Session`, not a `GnitzClient`, so it
        // tracks no OCC basis — the HELLO ACK's `published_lsn` is discarded here.
        let (transport, max_payload_len) = py
            .allow_threads(|| {
                let mut t = gnitz_core::ClientTransport::connect(socket_path)?;
                let (limit, _published_lsn) = gnitz_core::hello_handshake(&mut t)?;
                Ok::<_, gnitz_core::ProtocolError>((t, limit as usize))
            })
            .map_err(gnitz_err)?;
        let waker = transport.waker().map_err(gnitz_err)?;

        // The same generator the sync client uses, so a process holding both a
        // `GnitzClient` and an `AsyncTransport` cannot mint the same id twice.
        let client_id = gnitz_core::new_client_id();
        let (tx, rx) = std::sync::mpsc::sync_channel(IO_CHANNEL_DEPTH);
        // Bind the two loop methods once instead of resolving the attribute
        // (and building its name string) per resolved future / per enqueue.
        let call_soon = event_loop.getattr(py, "call_soon_threadsafe")?;
        let create_future = event_loop.getattr(py, "create_future")?;
        let sr_fn: Py<PyAny> = set_result_fn.clone_ref(py);
        let se_fn: Py<PyAny> = set_exception_fn.clone_ref(py);

        let handle = std::thread::spawn(move || {
            let session = gnitz_core::Session::from_transport(transport, client_id, max_payload_len);
            async_io_loop(session, rx, call_soon, sr_fn, se_fn);
        });

        Ok(PyAsyncTransport {
            tx: Some(tx),
            waker: Some(waker),
            create_future,
            client_id,
            thread: Some(handle),
        })
    }

    fn push(&self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>) -> PyResult<PyObject> {
        // Encode the push frame with the GIL released — the schema/batch cross
        // as plain `&` refs, no clone (the `PyRef` guard stays out of the closure).
        // FLAG_PUSH marks the frame as a push independent of data presence, so an
        // empty batch (a legitimate empty Z-set delta) is ACKed as a no-op push
        // (LSN 0) instead of being mistaken for a scan — a mis-route whose streamed
        // table dump would desync the one-frame Push reply reader.
        let client_id = self.client_id;
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        let parts = py.allow_threads(|| {
            gnitz_core::encode_message_parts(
                target_id,
                client_id,
                gnitz_core::FLAG_PUSH,
                &gnitz_core::PkTuple::EMPTY,
                0,
                Some(schema),
                Some(b),
            )
        });
        self.enqueue(py, IoOp::Push(parts), target_id, false)
    }

    #[pyo3(signature = (target_id, include_hidden = false))]
    fn scan(&self, py: Python<'_>, target_id: u64, include_hidden: bool) -> PyResult<PyObject> {
        self.enqueue(py, IoOp::Scan, target_id, include_hidden)
    }

    /// scan_many(target_ids, include_hidden=False) -> awaitable[list[ScanResult]]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, resolved
    /// as a list in request order. `target_id` is unused for this op (the tids
    /// ride the frame body).
    #[pyo3(signature = (target_ids, include_hidden = false))]
    fn scan_many(&self, py: Python<'_>, target_ids: Vec<u64>, include_hidden: bool) -> PyResult<PyObject> {
        // A malformed list (empty, over-cap, duplicate tid) is rejected by
        // `Session::pack_scan_multi` when the I/O thread packs it, before any
        // frame is written, and fails this one future.
        self.enqueue(py, IoOp::ScanMulti(target_ids), 0, include_hidden)
    }

    #[pyo3(signature = (target_id, pk, include_hidden = false))]
    fn seek(&self, py: Python<'_>, target_id: u64, pk: Bound<'_, PyAny>, include_hidden: bool) -> PyResult<PyObject> {
        let t = pk_tuple_from_py(&pk)?;
        self.enqueue(py, IoOp::Seek(t), target_id, include_hidden)
    }

    #[getter]
    fn client_id(&self) -> u64 {
        self.client_id
    }

    fn close(&mut self, py: Python<'_>) {
        self.tx.take();
        // TransportWaker::drop shuts down + closes the dup'd fd; the
        // shutdown wakes any in-flight recv_framed on the I/O thread.
        self.waker.take();
        if let Some(h) = self.thread.take() {
            // Release the GIL so the I/O thread can finish any in-progress
            // with_gil block before the join returns.
            py.allow_threads(|| {
                let _ = h.join();
            });
        }
    }
}

impl Drop for PyAsyncTransport {
    fn drop(&mut self) {
        // Do NOT join: Drop may be called from GC while holding the GIL, and
        // the I/O thread acquires the GIL to resolve futures — joining would
        // deadlock. TransportWaker::drop still fires the shutdown so the I/O
        // thread can exit promptly on its own.
        self.tx.take();
        self.waker.take();
    }
}

/// One decoded scan reply: the `(schema, batch, lsn)` triple `Session::recv_scan`
/// returns and `triple_to_lazy` consumes, unchanged.
type ScanTriple = (Option<Arc<Schema>>, Option<ZSetBatch>, u64);

/// What one pipelined request's response resolved to.
enum LoopResult {
    PushOk(u64),
    /// A server-level failure for this one request; fails its future alone.
    Error(String),
    /// Boxed so the scan payload doesn't pad the small `PushOk`/`Error` variants.
    Scan(Box<ScanTriple>),
    /// One `scan_many`'s N per-relation results, in request order.
    ScanMulti(Vec<ScanTriple>),
}

/// How to receive a given request's response, paired positionally with its
/// future. `Scan` covers both a full scan and a point seek — they read
/// identically. `Failed` is a request that never reached the wire (its frame
/// was rejected at pack time); it consumes no reply and fails its future alone.
enum RecvKind {
    Push { target_id: u64 },
    Scan { target_id: u64 },
    ScanMulti { target_ids: Vec<u64> },
    Failed(String),
}

/// Classify a recv error into the loop's result contract: a transport/protocol
/// failure stops the whole batch (`Err`), any other (server-level) error resolves
/// just that one future. Shared by every recv arm.
fn classify_recv_err(e: ClientError) -> Result<LoopResult, String> {
    match e {
        ClientError::Protocol(e) => Err(e.to_string()),
        e => Ok(LoopResult::Error(e.to_string())),
    }
}

fn async_io_loop(
    mut session: gnitz_core::Session,
    rx: std::sync::mpsc::Receiver<IoRequest>,
    call_soon: Py<PyAny>,
    sr_fn: Py<PyAny>,
    se_fn: Py<PyAny>,
) {
    use std::collections::VecDeque;

    // `session` owns the connection for the whole loop: the early return and
    // the normal `break` both fall through to its drop, which closes it — so
    // an unwind through this loop cannot leak it either.

    // Each entry pairs the future to resolve with its result's presentation
    // flag, so neither the recv types nor the decoded results carry it.
    let mut pending_futures: VecDeque<(Py<PyAny>, bool)> = VecDeque::with_capacity(IO_BATCH_MAX);

    // Hoisted scratch — cleared each iteration so the outer buffers are reused.
    let mut parts: Vec<gnitz_core::MessageParts> = Vec::with_capacity(IO_BATCH_MAX);
    let mut recv_kinds: Vec<RecvKind> = Vec::with_capacity(IO_BATCH_MAX);
    let mut results: Vec<LoopResult> = Vec::with_capacity(IO_BATCH_MAX);

    loop {
        // Block until at least one request.
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => break, // sender dropped → clean shutdown
        };

        // Drain queued requests (natural batching), capped to avoid filling
        // the socket send buffer before reading any responses. Each request
        // is packed here on the I/O thread: push frames arrive pre-encoded,
        // scan/seek are stamped with the session-owned cache's schema version.
        // A pack that the session rejects (a malformed `scan_many` tid list)
        // contributes no frame — only a `Failed` recv slot — so the rejection
        // fails that one future without ever touching the wire.
        parts.clear();
        recv_kinds.clear();
        let pack = |req: IoRequest, parts: &mut Vec<_>, kinds: &mut Vec<RecvKind>, futs: &mut VecDeque<_>| {
            let packed = match req.op {
                IoOp::Push(p) => Ok((
                    p,
                    RecvKind::Push {
                        target_id: req.target_id,
                    },
                )),
                IoOp::Scan => Ok((
                    session.pack_scan(req.target_id),
                    RecvKind::Scan {
                        target_id: req.target_id,
                    },
                )),
                IoOp::Seek(pk) => Ok((
                    session.pack_seek(req.target_id, &pk),
                    RecvKind::Scan {
                        target_id: req.target_id,
                    },
                )),
                IoOp::ScanMulti(tids) => session
                    .pack_scan_multi(&tids)
                    .map(|p| (p, RecvKind::ScanMulti { target_ids: tids })),
            };
            match packed {
                Ok((p, rk)) => {
                    parts.push(p);
                    kinds.push(rk);
                }
                Err(e) => kinds.push(RecvKind::Failed(e.to_string())),
            }
            futs.push_back((req.future, req.include_hidden));
        };
        pack(first, &mut parts, &mut recv_kinds, &mut pending_futures);
        while recv_kinds.len() < IO_BATCH_MAX {
            match rx.try_recv() {
                Ok(req) => pack(req, &mut parts, &mut recv_kinds, &mut pending_futures),
                Err(_) => break,
            }
        }

        // Send the whole batch as one writev sequence.
        if let Err(e) = session.send_batch(&parts) {
            fail_all(&rx, &mut pending_futures, &e.to_string(), &call_soon, &se_fn);
            return;
        }

        // Recv all responses for this batch through the session's cache-aware
        // reassembly (pure Rust, no GIL). A server-level error resolves that
        // one future (transport stays up); a transport/protocol failure stops
        // reading and fails the rest below. STATUS_SCHEMA_MISMATCH surfaces as
        // a ServerError here and fails the future — the async driver never
        // inline-retries (positional FIFO correlation forbids it).
        results.clear();
        let mut recv_err: Option<String> = None;
        for rk in &recv_kinds {
            let r: Result<LoopResult, String> = match *rk {
                RecvKind::Push { target_id } => match session.recv_push_ack(target_id) {
                    Ok(lsn) => Ok(LoopResult::PushOk(lsn)),
                    Err(e) => classify_recv_err(e),
                },
                RecvKind::Scan { target_id } => match session.recv_scan(target_id) {
                    Ok(t) => Ok(LoopResult::Scan(Box::new(t))),
                    Err(e) => classify_recv_err(e),
                },
                RecvKind::ScanMulti { ref target_ids } => {
                    // Read the N reply trains positionally, in request order. A
                    // server-side shape/tid rejection arrives as one STATUS_ERROR
                    // train that fails the whole scan_many; a transport/protocol
                    // failure stops the batch (as the single scan does). `collect`
                    // short-circuits on the first error, so `recv_scan` is not
                    // called for tids past a failure — matching a per-tid `break`.
                    match target_ids
                        .iter()
                        .map(|&tid| session.recv_scan(tid))
                        .collect::<Result<Vec<ScanTriple>, _>>()
                    {
                        Ok(triples) => Ok(LoopResult::ScanMulti(triples)),
                        Err(e) => classify_recv_err(e),
                    }
                }
                // Never reached the wire; no reply to consume.
                RecvKind::Failed(ref msg) => Ok(LoopResult::Error(msg.clone())),
            };
            match r {
                Ok(res) => results.push(res),
                Err(e) => {
                    recv_err = Some(e);
                    break;
                }
            }
        }

        // Single GIL acquisition to resolve all futures. The session absorbed
        // every response's schema into its own cache during recv, so there is
        // no separate cache-update step and no cross-thread lock.
        Python::with_gil(|py| {
            for result in results.drain(..) {
                let (fut, include_hidden) = pending_futures.pop_front().unwrap();
                match result {
                    LoopResult::PushOk(lsn) => {
                        let v = lsn.into_pyobject(py).unwrap().into_any().unbind();
                        let _ = call_soon.call1(py, (&sr_fn, &fut, v));
                    }
                    LoopResult::Error(err_text) => {
                        let exc = GnitzError::new_err(err_text);
                        let _ = call_soon.call1(py, (&se_fn, &fut, exc));
                    }
                    LoopResult::Scan(t) => {
                        let py_val = triple_to_lazy(py, *t, include_hidden).unwrap().into_any();
                        let _ = call_soon.call1(py, (&sr_fn, &fut, py_val));
                    }
                    LoopResult::ScanMulti(triples) => {
                        // One PyScanResult per relation, in request order → a
                        // Python list, resolving the single scan_many future.
                        let items: Vec<PyObject> = triples
                            .into_iter()
                            .map(|t| triple_to_lazy(py, t, include_hidden).unwrap().into_any())
                            .collect();
                        let py_list = PyList::new(py, items).unwrap().into_any().unbind();
                        let _ = call_soon.call1(py, (&sr_fn, &fut, py_list));
                    }
                }
            }
        });

        if let Some(e) = recv_err {
            fail_all(&rx, &mut pending_futures, &e, &call_soon, &se_fn);
            return;
        }
    }
}

/// Fail every future this loop still owns with `msg` and stop: the ones already
/// dequeued for the current batch, then every request still sitting in the
/// channel. Draining `rx` to exhaustion (rather than dropping it) is what makes
/// a lost connection surface as a raised exception on *every* submitted
/// operation — a dropped `IoRequest` would leave its coroutine awaiting a future
/// nobody is left to resolve. The drain ends when the last sender is gone, after
/// which `enqueue` reports the closed channel directly.
fn fail_all(
    rx: &std::sync::mpsc::Receiver<IoRequest>,
    pending: &mut std::collections::VecDeque<(Py<PyAny>, bool)>,
    msg: &str,
    call_soon: &Py<PyAny>,
    se_fn: &Py<PyAny>,
) {
    Python::with_gil(|py| {
        let exc = GnitzError::new_err(msg.to_string())
            .into_pyobject(py)
            .unwrap()
            .into_any()
            .unbind();
        for (fut, _) in pending.drain(..) {
            let _ = call_soon.call1(py, (se_fn, &fut, exc.clone_ref(py)));
        }
    });
    // One GIL acquisition per drained request, never one held across the
    // blocking `recv`: the last sender is dropped by `close()`/`Drop` on the
    // Python side, which cannot run while this thread holds the GIL.
    for req in rx.iter() {
        Python::with_gil(|py| {
            let exc = GnitzError::new_err(msg.to_string());
            let _ = call_soon.call1(py, (se_fn, &req.future, exc));
        });
    }
}

/// Decode a persisted catalog column-list `u64` (`TABLE_TAB.pk_col_idx`,
/// `IDX_TAB.source_col_idx`) into a plain list of column indices. Delegates to
/// the shared `gnitz_wire` bit-layout codec so the Python side cannot drift
/// from the Rust encoder; returns a list to match the test callers' `[1]`-style
/// comparisons.
#[pyfunction]
fn unpack_pk_cols(v: u64) -> Vec<u32> {
    gnitz_wire::unpack_pk_cols(v).as_slice().to_vec()
}

/// The `(name, code)` column-type table, straight off `TypeCode::ALL`.
/// `_types.py` builds its `TypeCode` IntEnum from this rather than re-typing the
/// codes, so a variant added in `gnitz_wire` reaches Python with no edit here
/// and none there.
#[pyfunction]
fn type_codes() -> Vec<(&'static str, u8)> {
    TypeCode::ALL.iter().map(|&tc| (tc.wire_name(), tc as u8)).collect()
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyColumnDef>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyRow>()?;
    m.add_class::<PyZSetBatch>()?;
    m.add_class::<PyRustBatch>()?;
    m.add_class::<PyScanResult>()?;
    m.add_class::<PyRowIterator>()?;
    m.add_class::<PyGnitzClient>()?;
    m.add_class::<PyTxn>()?;
    m.add_class::<PyExprBuilder>()?;
    m.add_class::<PyExprProgram>()?;
    m.add_class::<PyCircuitBuilder>()?;
    m.add_class::<PyCircuit>()?;
    m.add_class::<PyAsyncTransport>()?;
    m.add("GnitzError", m.py().get_type::<GnitzError>())?;
    m.add("GnitzConflictError", m.py().get_type::<GnitzConflictError>())?;
    // System-table IDs — single-sourced from gnitz_wire (delegating codec, not
    // a re-typed copy), as is the column-type table behind `type_codes()`.
    m.add("SCHEMA_TAB", gnitz_wire::SCHEMA_TAB)?;
    m.add("TABLE_TAB", gnitz_wire::TABLE_TAB)?;
    m.add("VIEW_TAB", gnitz_wire::VIEW_TAB)?;
    m.add("COL_TAB", gnitz_wire::COL_TAB)?;
    m.add("IDX_TAB", gnitz_wire::IDX_TAB)?;
    m.add("DEP_TAB", gnitz_wire::DEP_TAB)?;
    m.add("SEQ_TAB", gnitz_wire::SEQ_TAB)?;
    m.add("FIRST_USER_TABLE_ID", gnitz_wire::FIRST_USER_TABLE_ID)?;
    m.add("FIRST_USER_SCHEMA_ID", gnitz_wire::FIRST_USER_SCHEMA_ID)?;
    m.add_function(wrap_pyfunction!(unpack_pk_cols, m)?)?;
    m.add_function(wrap_pyfunction!(type_codes, m)?)?;
    Ok(())
}
