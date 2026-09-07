//! The wire→Python direction: every decode from a `ZSetBatch` to a Python
//! object, and the read-side pyclasses built on them — `Row`, `ScanResult`,
//! `RowIterator` and `DeltaReply`.
//!
//! The per-cell decode table is private: what leaves this module is the column-
//! and result-level entry points, so no other module dispatches on a `TypeCode`
//! to read a value.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

use gnitz_core::{Schema, TypeCode, ZSetBatch};
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_wire::format_uuid;

use crate::build_pylist;
use crate::schema::rust_schema_to_py;

/// `subclass` because a result presents its rows as a synthesised subclass
/// carrying one [`ColumnDescriptor`] per column ([`row_type_for`]). `frozen`
/// drops the borrow flag, and with it two atomic RMWs per `&self` method.
#[pyclass(name = "Row", frozen, subclass)]
pub struct PyRow {
    fields: Py<PyTuple>,
    values: Py<PyTuple>,
    weight: i64,
}

/// One presented column, as a descriptor in a row subclass's type dict, so
/// `row.col` is served by CPython's type-attribute cache rather than a field
/// scan behind a raised-and-caught `AttributeError`.
#[pyclass(frozen)]
struct ColumnDescriptor {
    pos: usize,
}

#[pymethods]
impl ColumnDescriptor {
    fn __get__(slf: Bound<'_, Self>, obj: &Bound<'_, PyAny>, _owner: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        // Read off the class rather than an instance (`Row.col`): hand back the
        // descriptor, as a plain getset would.
        if obj.is_none() {
            return Ok(slf.into_any().unbind());
        }
        let row = obj.cast::<PyRow>()?;
        Ok(row.get().values.bind(slf.py()).get_item(slf.get().pos)?.unbind())
    }
}

/// The row subclass for one presented-field tuple, built once per result and
/// cached for every later result with the same column names. Process-global and
/// never evicted; it grows with the program's distinct column-name tuples, not
/// with the data.
///
/// Keyed by the tuple's *contents*, never by the `Arc<Schema>` address: a freed
/// `Arc`'s address can be reused, which would hand a result someone else's
/// descriptors.
fn row_type_for<'py>(py: Python<'py>, fields: &Bound<'py, PyTuple>) -> PyResult<Bound<'py, pyo3::types::PyType>> {
    static CACHE: pyo3::sync::PyOnceLock<Py<PyDict>> = pyo3::sync::PyOnceLock::new();
    let cache = CACHE
        .get_or_try_init(py, || PyResult::Ok(PyDict::new(py).unbind()))?
        .bind(py);
    if let Some(ty) = cache.get_item(fields)? {
        return ty.cast_into::<pyo3::types::PyType>().map_err(Into::into);
    }
    let ns = PyDict::new(py);
    // No `__dict__` / `__weakref__` per row: a row's namespace is its columns.
    ns.set_item(pyo3::intern!(py, "__slots__"), PyTuple::empty(py))?;
    for (pos, name) in fields.as_slice().iter().enumerate() {
        let name = name.cast::<PyString>()?;
        // The underscore namespace is the row object's own (`_fields`, `_asdict`,
        // `_weight`), so such a column gets no descriptor and reaches
        // `PyRow::__getattr__`. Everything else is plain MRO.
        if name.to_cow()?.starts_with('_') || ns.contains(name)? {
            continue;
        }
        ns.set_item(name, Bound::new(py, ColumnDescriptor { pos })?)?;
    }
    let bases = PyTuple::new(py, [py.get_type::<PyRow>()])?;
    let ty = py
        .get_type::<pyo3::types::PyType>()
        .call1((pyo3::intern!(py, "Row"), bases, ns))?
        .cast_into::<pyo3::types::PyType>()?;
    cache.set_item(fields, &ty)?;
    Ok(ty)
}

/// Resolve an `int`-or-`str` key against `fields` into a position, normalizing a
/// negative index the way a Python sequence does. Shared by `row[k]` and
/// `result.scalars(k)`, so both name a column the same way; `what` is the
/// argument's name, for the `TypeError`.
fn key_pos(fields: &Bound<'_, PyTuple>, key: &Bound<'_, PyAny>, what: &str) -> PyResult<usize> {
    if key.cast::<pyo3::types::PyInt>().is_ok() {
        let len = fields.len() as isize;
        let idx = key.extract::<isize>()?;
        let idx = if idx < 0 { idx + len } else { idx };
        if idx < 0 || idx >= len {
            return Err(pyo3::exceptions::PyIndexError::new_err("index out of range"));
        }
        return Ok(idx as usize);
    }
    if let Ok(name) = key.cast::<PyString>() {
        return match field_pos(fields, name)? {
            Some(i) => Ok(i),
            None => Err(pyo3::exceptions::PyKeyError::new_err(name.to_cow()?.into_owned())),
        };
    }
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "{what} must be int or str"
    )))
}

/// Position of `name` among `fields`, or `None`. Field names are interned, so
/// the common case settles on the pointer compare and the string compare covers
/// a computed name. Linear over at most `MAX_COLUMNS`; this serves `row["name"]`
/// and `scalars("name")`, never `row.name`, which goes through the descriptors.
fn field_pos(fields: &Bound<'_, PyTuple>, name: &Bound<'_, PyString>) -> PyResult<Option<usize>> {
    // The identity pass stays separate from the compare pass: fusing them would
    // rich-compare every field before the interned hit.
    let items = fields.as_slice();
    if let Some(i) = items.iter().position(|f| f.is(name)) {
        return Ok(Some(i));
    }
    for (i, f) in items.iter().enumerate() {
        if f.eq(name)? {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

#[pymethods]
impl PyRow {
    #[new]
    #[pyo3(signature = (fields, values, weight=1))]
    pub fn new(fields: Bound<'_, PyTuple>, values: Bound<'_, PyTuple>, weight: i64) -> PyResult<Self> {
        Ok(PyRow {
            fields: fields.unbind(),
            values: values.unbind(),
            weight,
        })
    }

    /// The row's Z-set weight, always: the row object owns its underscore names,
    /// so a *column* named `_weight` is shadowed here and read through
    /// `_asdict()` or by position. A column named `weight` shadows the alias
    /// below, since that name is not the row's own.
    #[getter(_weight)]
    pub fn weight_(&self) -> i64 {
        self.weight
    }

    /// Alias of `_weight`, for a schema with no column of that name.
    #[getter]
    pub fn weight(&self) -> i64 {
        self.weight
    }

    /// The presented column names, positionally aligned with the values —
    /// `namedtuple`'s spelling. Shared by every row of a result, so this borrows
    /// rather than rebuilds.
    #[getter]
    pub fn _fields(&self, py: Python<'_>) -> Py<PyTuple> {
        self.fields.clone_ref(py)
    }

    /// The cold fallback: an underscore-prefixed *column* name, the one kind
    /// [`row_type_for`] installs no descriptor for. Reaching here costs a
    /// raised-and-caught `AttributeError` from the generic-getattr miss first.
    pub fn __getattr__(&self, py: Python<'_>, name: &Bound<'_, PyString>) -> PyResult<Py<PyAny>> {
        match field_pos(self.fields.bind(py), name)? {
            Some(i) => Ok(self.values.bind(py).get_item(i)?.unbind()),
            None => Err(pyo3::exceptions::PyAttributeError::new_err(format!(
                "Row has no field {:?}",
                name.to_cow()?
            ))),
        }
    }

    pub fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let i = key_pos(self.fields.bind(py), key, "key")?;
        Ok(self.values.bind(py).get_item(i)?.unbind())
    }

    pub fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.values.bind(py).as_any().try_iter()?.into_any().unbind())
    }

    pub fn __len__(&self, py: Python<'_>) -> usize {
        self.values.bind(py).len()
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let fields = self.fields.bind(py);
        let mut parts = Vec::with_capacity(fields.len());
        for (name, val) in fields.as_slice().iter().zip(self.values.bind(py).as_slice()) {
            parts.push(format!("{}={}", name.extract::<&str>()?, val.repr()?));
        }
        Ok(format!("Row({}, weight={})", parts.join(", "), self.weight))
    }

    pub fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(other_row) = other.cast::<PyRow>() {
            let eq: bool = self.values.bind(py).eq(other_row.get().values.bind(py))?;
            Ok(eq.into_pyobject(py)?.to_owned().into_any().unbind())
        } else {
            Ok(py.NotImplemented().into_any())
        }
    }

    pub fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        self.values.bind(py).hash()
    }

    pub fn _asdict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (name, val) in self
            .fields
            .bind(py)
            .as_slice()
            .iter()
            .zip(self.values.bind(py).as_slice())
        {
            dict.set_item(name, val)?;
        }
        Ok(dict.unbind())
    }
}

// ---------------------------------------------------------------------------
// Batch conversion helpers
// ---------------------------------------------------------------------------

/// Materialize a `PkColumn` as a Python list: a single-column key as that
/// column's own Python type, a compound key as native-space `bytes` — the form
/// `py_pks_to_column` accepts back.
pub(crate) fn pk_column_to_pylist(py: Python<'_>, schema: &Schema, batch: &ZSetBatch) -> PyResult<Py<PyList>> {
    if schema.pk_count() >= 2 {
        let stride = schema.pk_stride();
        let rows: Vec<_> = (0..batch.pks.len())
            .map(|i| {
                let native = gnitz_core::native_le_key(schema, &batch.pks.get_tuple(i));
                pyo3::types::PyBytes::new(py, &native[..stride])
            })
            .collect();
        return Ok(PyList::new(py, rows)?.unbind());
    }
    // Through the resolved address, like every other decode — the offset and
    // width of the lone PK column are `SchemaFacts::locate`'s answer, not a
    // second derivation from `pk_stride` here.
    let ci = schema.pk_cols[0] as usize;
    let loc = SchemaFacts::locate(schema, ci);
    Ok(build_pylist(py, (0..batch.pks.len()).map(|i| value_at(py, batch, ci, loc, i)))?.unbind())
}

/// Decode one fixed-width column's native-LE bytes into a Python value —
/// serving PK and payload alike, so a column renders the same wherever it is
/// read. The 16-byte integer types route through [`u128_value_to_py`];
/// everything else is a fixed-width read.
fn fixed_value_to_py(py: Python<'_>, tc: TypeCode, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    // `is_wide_int`, not a hand-listed set: the write path keys off the same
    // predicate, so a newly added 16-byte type cannot fall through to the
    // fixed-width arm on one side only.
    if tc.is_wide_int() {
        return u128_value_to_py(py, u128::from_le_bytes(bytes.try_into().unwrap()), tc);
    }
    Ok(read_fixed_le(py, tc, bytes))
}

/// Read one fixed-width value as a Python object (integers as int, floats as
/// float). Widths come from `slice.len()` via the shared
/// `read_signed_exact`/`read_unsigned_exact` pair, so the 1/2/4/8-byte table is
/// not restated here; only the sign and float distinctions are type-directed.
fn read_fixed_le(py: Python<'_>, tc: TypeCode, slice: &[u8]) -> Py<PyAny> {
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

/// Surface one 16-byte integer as its column type dictates: UUID → canonical
/// string, I128 → signed int, U128 → unsigned int. The one place that decides.
fn u128_value_to_py(py: Python<'_>, x: u128, tc: TypeCode) -> PyResult<Py<PyAny>> {
    Ok(match tc {
        TypeCode::UUID => format_uuid(x).into_pyobject(py)?.into_any().unbind(),
        TypeCode::I128 => (x as i128).into_pyobject(py)?.into_any().unbind(),
        _ => x.into_pyobject(py)?.into_any().unbind(),
    })
}

/// Decode one payload cell, null bit first: a set bit is `None` whatever the
/// stored value. STRING surfaces as `str` (UTF-8 is validated here), BLOB as
/// `bytes`, everything else through [`fixed_value_to_py`].
fn cell_to_py(
    py: Python<'_>,
    batch: &ZSetBatch,
    ci: usize,
    row: usize,
    is_null: bool,
    tc: TypeCode,
    stride: usize,
) -> PyResult<Py<PyAny>> {
    if is_null {
        return Ok(py.None());
    }
    let cell = &batch.columns[ci][row * stride..(row + 1) * stride];
    Ok(match tc {
        TypeCode::String => {
            let bytes = gnitz_wire::german_string_content(cell, &batch.blob);
            std::str::from_utf8(bytes)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("invalid UTF-8 in a STRING column: {e}")))?
                .into_pyobject(py)?
                .into_any()
                .unbind()
        }
        TypeCode::Blob => pyo3::types::PyBytes::new(py, gnitz_wire::german_string_content(cell, &batch.blob))
            .into_any()
            .unbind(),
        _ => fixed_value_to_py(py, tc, cell)?,
    })
}

// ---------------------------------------------------------------------------
// Lazy batch infrastructure
// ---------------------------------------------------------------------------

/// One presented column: its physical index plus the resolved address the row
/// build reads through. The locator carries the type code, so the per-row loop
/// never touches the schema.
type PresentedCol = (usize, ColumnLocator);

struct SharedBatchData {
    schema: Arc<Schema>,
    batch: ZSetBatch,
    /// Pre-computed field-name tuple, created once and shared across all iterators.
    fields: Py<PyTuple>,
    /// The `Row` subclass whose type dict holds this result's column
    /// descriptors, resolved once per result by [`row_type_for`].
    row_type: Py<pyo3::types::PyType>,
    /// The columns to present, in presentation order: all of them when
    /// `include_hidden`, the non-hidden ones otherwise. Rows, `fields`, and
    /// `scalars` all index through this, so every presentation surface agrees
    /// on positions.
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
    // Interned: `mappings()` inserts hit on identity, `field_pos` settles on a
    // pointer compare, and the tuple's own hash reads each element's cached one.
    let names = present
        .iter()
        .map(|&(ci, _)| PyString::intern(py, &s.columns[ci].name))
        .collect::<Vec<_>>();
    let fields = PyTuple::new(py, names)?;
    let row_type = row_type_for(py, &fields)?.unbind();
    Ok(Arc::new(SharedBatchData {
        schema: s,
        batch: b,
        fields: fields.unbind(),
        row_type,
        present,
    }))
}

/// Decode one cell at `loc` in `row`, PK or payload — the single per-cell decode,
/// shared by the row build, `scalars`, the PK list and the per-column lists.
fn value_at(py: Python<'_>, batch: &ZSetBatch, ci: usize, loc: ColumnLocator, row: usize) -> PyResult<Py<PyAny>> {
    let tc = TypeCode::from_validated_u8(loc.type_code());
    match loc {
        ColumnLocator::Pk { byte_off, size, .. } => {
            let opk = batch.pks.col_window(row, byte_off as usize, size as usize);
            let native = gnitz_wire::decode_pk_column_owned(opk, loc.type_code());
            fixed_value_to_py(py, tc, &native[..size as usize])
        }
        ColumnLocator::Payload { size, .. } => cell_to_py(
            py,
            batch,
            ci,
            row,
            loc.is_null_word(batch.nulls[row]),
            tc,
            size as usize,
        ),
    }
}

/// Build Python values for a single row from Rust data, appending to `out`.
fn build_row_values_into(py: Python<'_>, data: &SharedBatchData, row: usize, out: &mut Vec<Py<PyAny>>) -> PyResult<()> {
    for &(ci, loc) in &data.present {
        out.push(value_at(py, &data.batch, ci, loc, row)?);
    }
    Ok(())
}

/// Build the `Row` object for one row, reusing `buf` as scratch across calls.
fn make_row(py: Python<'_>, data: &Arc<SharedBatchData>, row: usize, buf: &mut Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    buf.clear();
    build_row_values_into(py, data, row, buf)?;
    // `drain` hands the values over already-owned, so the tuple build costs no
    // refcount traffic, and the Vec keeps its capacity for the next row.
    let values = PyTuple::new(py, buf.drain(..))?;
    // Through the subclass's own `tp_call`: pyo3 gives Rust no way to instantiate
    // a pyclass subtype.
    data.row_type
        .bind(py)
        .call1((data.fields.bind(py), values, data.batch.weights[row]))
        .map(Bound::unbind)
}

/// Materialize per-column value lists, indexed by *physical* column. A PK column
/// holds an empty list — the PK region is surfaced through `.pks`.
pub(crate) fn rust_batch_columns_to_py(py: Python<'_>, schema: &Schema, batch: &ZSetBatch) -> PyResult<Py<PyList>> {
    let n = batch.len();
    let mut col_lists: Vec<Py<PyAny>> = Vec::with_capacity(schema.columns.len());
    for ci in 0..schema.columns.len() {
        let loc = SchemaFacts::locate(schema, ci);
        let rows = if matches!(loc, ColumnLocator::Pk { .. }) { 0 } else { n };
        let col = build_pylist(py, (0..rows).map(|i| value_at(py, batch, ci, loc, i)))?;
        col_lists.push(col.into_any().unbind());
    }
    Ok(PyList::new(py, col_lists)?.unbind())
}

// ---------------------------------------------------------------------------
// PyScanResult — Rust-backed ScanResult
// ---------------------------------------------------------------------------

#[pyclass(name = "ScanResult", frozen)]
pub struct PyScanResult {
    data: Option<Arc<SharedBatchData>>,
    /// The server-side LSN this result was read at, or `None` where there is
    /// none — a SQL `Rows` payload. Reporting 0 would collide with a real LSN 0.
    #[pyo3(get)]
    lsn: Option<u64>,
}

#[pymethods]
impl PyScanResult {
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.data {
            None => Ok(py.None()),
            Some(d) => Ok(rust_schema_to_py(py, &d.schema)?.into_any()),
        }
    }

    /// The PK region as a Python list — one value per row, or packed `bytes`
    /// per row for a compound key. Empty when the result carries no rows.
    #[getter]
    fn pks(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        match &self.data {
            None => Ok(PyList::empty(py).unbind()),
            Some(d) => pk_column_to_pylist(py, &d.schema, &d.batch),
        }
    }

    /// The per-row Z-set weights, positionally aligned with `pks`.
    #[getter]
    fn weights(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        match &self.data {
            None => Ok(PyList::empty(py).unbind()),
            Some(d) => Ok(PyList::new(py, &d.batch.weights)?.unbind()),
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
        Ok(build_pylist(py, (0..data.batch.len()).map(|i| make_row(py, data, i, &mut buf)))?.unbind())
    }

    /// The first row, or `None` on an empty result.
    fn first(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.data {
            Some(d) if !d.batch.is_empty() => make_row(py, d, 0, &mut Vec::with_capacity(d.present.len())),
            _ => Ok(py.None()),
        }
    }

    fn mappings(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let Some(data) = &self.data else {
            return Ok(PyList::empty(py).unbind());
        };
        let fields = data.fields.bind(py);
        let mut row_buf = Vec::with_capacity(fields.len());
        Ok(build_pylist(
            py,
            (0..data.batch.len()).map(|i| {
                row_buf.clear();
                build_row_values_into(py, data, i, &mut row_buf)?;
                let dict = PyDict::new(py);
                for (name, val) in fields.as_slice().iter().zip(&row_buf) {
                    dict.set_item(name, val)?;
                }
                Ok(dict)
            }),
        )?
        .unbind())
    }

    #[pyo3(signature = (col=None))]
    fn scalars(&self, py: Python<'_>, col: Option<Py<PyAny>>) -> PyResult<Py<PyList>> {
        let Some(data) = &self.data else {
            return Ok(PyList::empty(py).unbind());
        };
        // None → the first presented column; otherwise the same int-or-str
        // resolution a row subscript takes, so `res.scalars(k)` and `row[k]`
        // name the same column.
        let pos = match col {
            None => 0usize,
            Some(ref obj) => key_pos(data.fields.bind(py), obj.bind(py), "col")?,
        };
        // The presented-column table already holds this column's resolved
        // address, so the row loop below does no schema lookups.
        let (ci, loc) = data.present[pos];
        Ok(build_pylist(py, (0..data.batch.len()).map(|i| value_at(py, &data.batch, ci, loc, i)))?.unbind())
    }
}

// ---------------------------------------------------------------------------
// PyRowIterator
// ---------------------------------------------------------------------------

#[pyclass(name = "RowIterator")]
pub struct PyRowIterator {
    data: Option<Arc<SharedBatchData>>,
    row_buf: Vec<Py<PyAny>>,
    pos: usize,
}

#[pymethods]
impl PyRowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
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
// The lazy-result builders
// ---------------------------------------------------------------------------

/// Build a lazy `PyScanResult` from one `(schema, batch, lsn)` triple — the
/// shape every read path resolves to, whether it came back from a sync
/// `scan`/`seek`, one relation of a `scan_many`, or the async loop.
pub(crate) fn triple_to_lazy(
    py: Python<'_>,
    triple: (Option<Arc<Schema>>, Option<ZSetBatch>, u64),
    include_hidden: bool,
) -> PyResult<Py<PyScanResult>> {
    let (opt_schema, opt_batch, view_lsn) = triple;
    batch_to_lazy(py, opt_schema, opt_batch, Some(view_lsn), include_hidden)
}

/// One delta read's answer: the rows, and the cursor the next poll takes.
#[pyclass(name = "DeltaReply", frozen)]
pub struct PyDeltaReply {
    /// The delta rows, weights included — a retraction arrives at weight −1.
    #[pyo3(get)]
    rows: Py<PyScanResult>,
    /// `(tag, tick)`: the boot and relation this reply belongs to, and the last
    /// round it covers. Hand it straight back to `delta_poll`.
    #[pyo3(get)]
    cursor: (u64, u64),
}

/// Build a [`PyDeltaReply`] from a delta read's `(rows, cursor)` pair. The reply
/// carries no schema block — the client authored it — so the schema is the one
/// the caller handed in.
pub(crate) fn delta_reply_to_py(
    py: Python<'_>,
    schema: Arc<Schema>,
    out: (Option<ZSetBatch>, gnitz_core::DeltaCursor),
    include_hidden: bool,
) -> PyResult<Py<PyDeltaReply>> {
    let (batch, cursor) = out;
    let rows = batch_to_lazy(py, Some(schema), batch, None, include_hidden)?;
    Py::new(py, PyDeltaReply { rows, cursor: (cursor.tag, cursor.tick) })
}

/// The `PyScanResult` build shared by the read paths and the SQL path, which
/// differ only in whether an LSN exists at all.
pub(crate) fn batch_to_lazy(
    py: Python<'_>,
    opt_schema: Option<Arc<Schema>>,
    opt_batch: Option<ZSetBatch>,
    lsn: Option<u64>,
    include_hidden: bool,
) -> PyResult<Py<PyScanResult>> {
    let data = match opt_schema {
        Some(s) => {
            let b = opt_batch.unwrap_or_else(|| ZSetBatch::new(s.as_ref()));
            Some(make_shared_batch_data(py, s, b, include_hidden)?)
        }
        None => None,
    };
    Py::new(py, PyScanResult { data, lsn })
}
