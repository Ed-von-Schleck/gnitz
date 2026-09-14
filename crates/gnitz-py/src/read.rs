//! The wire→Python direction: every decode from a `ZSetBatch` to a Python
//! object, and the read-side pyclasses built on them — `Row`, `ScanResult`,
//! `RowIterator` and `DeltaReply`.
//!
//! The per-cell decode is private: what leaves this module is [`scan_result`],
//! so no other module dispatches on a `TypeCode` to read a value.

use std::sync::Arc;

use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDate, PyDateTime, PyDict, PyString, PyTuple, PyType};

use gnitz_core::{ColType, ScanReply, Schema, TypeCode, ZSetBatch};
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_wire::decimal::format_decimal;
use gnitz_wire::format_uuid;

use crate::schema::rust_schema_to_py;

/// `subclass` because a result presents its rows as a synthesised subclass
/// carrying its field names and one [`ColumnDescriptor`] per column
/// ([`row_type_for`]). `frozen` drops the borrow flag, and with it two atomic
/// RMWs per `&self` method.
#[pyclass(name = "Row", frozen, subclass)]
pub struct PyRow {
    values: Py<PyTuple>,
    weight: i64,
}

/// One presented column, as a descriptor in a row subclass's type dict, so
/// `row.col` is served by CPython's type-attribute cache.
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
fn row_type_for<'py>(py: Python<'py>, fields: &Bound<'py, PyTuple>) -> PyResult<Bound<'py, PyType>> {
    static CACHE: PyOnceLock<Py<PyDict>> = PyOnceLock::new();
    let cache = CACHE
        .get_or_try_init(py, || PyResult::Ok(PyDict::new(py).unbind()))?
        .bind(py);
    if let Some(ty) = cache.get_item(fields)? {
        return ty.cast_into::<PyType>().map_err(Into::into);
    }
    let ns = PyDict::new(py);
    // No `__dict__` / `__weakref__` per row: a row's namespace is its columns.
    ns.set_item(intern!(py, "__slots__"), PyTuple::empty(py))?;
    ns.set_item(intern!(py, "_fields"), fields)?;
    let base = py.get_type::<PyRow>();
    for (pos, name) in fields.as_slice().iter().enumerate() {
        // A name the row object already answers (`_fields`, `_asdict`,
        // `_weight`, a dunder) keeps that meaning; the column is read by
        // position or through `_asdict()`. A repeated name keeps its first
        // column.
        if base.hasattr(name.cast::<PyString>()?)? || ns.contains(name)? {
            continue;
        }
        ns.set_item(name, Bound::new(py, ColumnDescriptor { pos })?)?;
    }
    let bases = PyTuple::new(py, [base])?;
    let ty = py
        .get_type::<PyType>()
        .call1((intern!(py, "Row"), bases, ns))?
        .cast_into::<PyType>()?;
    cache.set_item(fields, &ty)?;
    Ok(ty)
}

/// Position of `name` among `fields`, or `None`. Field names are interned, so
/// the common case settles on the pointer compare and the string compare covers
/// a computed name. Linear over at most `MAX_COLUMNS`; this serves `row["name"]`,
/// never `row.name`, which goes through the descriptors.
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

/// A row's field names: its subclass's `_fields`, or `Row`'s own empty tuple.
fn fields_of<'py>(slf: &Bound<'py, PyRow>) -> PyResult<Bound<'py, PyTuple>> {
    Ok(slf
        .get_type()
        .getattr(intern!(slf.py(), "_fields"))?
        .cast_into::<PyTuple>()?)
}

#[pymethods]
impl PyRow {
    #[new]
    #[pyo3(signature = (values, _weight=1))]
    pub fn new(values: Bound<'_, PyTuple>, _weight: i64) -> Self {
        PyRow { values: values.unbind(), weight: _weight }
    }

    /// The presented column names, positionally aligned with the values —
    /// `namedtuple`'s spelling. Empty on `Row` itself; a result's row subclass
    /// shadows it with its own tuple.
    #[classattr]
    fn _fields(py: Python<'_>) -> Py<PyTuple> {
        PyTuple::empty(py).unbind()
    }

    /// The row's Z-set weight, always: the row object owns its underscore names,
    /// so a *column* named `_weight` is shadowed here and read through
    /// `_asdict()` or by position.
    #[getter(_weight)]
    pub fn weight_(&self) -> i64 {
        self.weight
    }

    pub fn __getitem__(slf: &Bound<'_, Self>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let values = slf.get().values.bind(py);
        let pos = if key.cast::<pyo3::types::PyInt>().is_ok() {
            let len = values.len() as isize;
            let idx = key.extract::<isize>()?;
            let idx = if idx < 0 { idx + len } else { idx };
            if idx < 0 || idx >= len {
                return Err(pyo3::exceptions::PyIndexError::new_err("index out of range"));
            }
            idx as usize
        } else if let Ok(name) = key.cast::<PyString>() {
            match field_pos(&fields_of(slf)?, name)? {
                Some(i) => i,
                None => return Err(pyo3::exceptions::PyKeyError::new_err(name.to_cow()?.into_owned())),
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err("key must be int or str"));
        };
        Ok(values.get_item(pos)?.unbind())
    }

    pub fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.values.bind(py).as_any().try_iter()?.into_any().unbind())
    }

    pub fn __len__(&self, py: Python<'_>) -> usize {
        self.values.bind(py).len()
    }

    pub fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
        let fields = fields_of(slf)?;
        let values = slf.get().values.bind(slf.py());
        let mut parts = Vec::with_capacity(values.len());
        for (i, val) in values.as_slice().iter().enumerate() {
            match fields.as_slice().get(i) {
                Some(name) => parts.push(format!("{}={}", name.extract::<&str>()?, val.repr()?)),
                None => parts.push(val.repr()?.to_string()),
            }
        }
        Ok(format!("Row({}, _weight={})", parts.join(", "), slf.get().weight))
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

    pub fn _asdict(slf: &Bound<'_, Self>) -> PyResult<Py<PyDict>> {
        let py = slf.py();
        let dict = PyDict::new(py);
        for (name, val) in fields_of(slf)?
            .as_slice()
            .iter()
            .zip(slf.get().values.bind(py).as_slice())
        {
            dict.set_item(name, val)?;
        }
        Ok(dict.unbind())
    }
}

// ---------------------------------------------------------------------------
// Cell decode
// ---------------------------------------------------------------------------

/// `decimal.Decimal`, imported once.
fn py_decimal(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static DECIMAL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    DECIMAL
        .get_or_try_init(py, || {
            Ok::<_, PyErr>(py.import("decimal")?.getattr("Decimal")?.unbind())
        })
        .map(|d| d.bind(py))
}

/// Decode one cell at `loc` in `row`, PK or payload, null bit first: a set bit
/// is `None` whatever the stored value. Integers as int, floats as float, UUID
/// as its canonical string, DATE as `datetime.date`, TIMESTAMP as a naive
/// `datetime.datetime`, DECIMAL as `decimal.Decimal` at the column's scale,
/// STRING as `str`, BLOB as `bytes`.
fn cell_to_py(py: Python<'_>, batch: &ZSetBatch, loc: ColumnLocator, ty: ColType, row: usize) -> PyResult<Py<PyAny>> {
    macro_rules! obj {
        ($v:expr) => {
            $v.into_pyobject(py)?.into_any().unbind()
        };
    }
    if loc.is_null(batch, row) {
        return Ok(py.None());
    }
    let mut scratch = [0u8; 16];
    let b = loc.native_le_bytes(batch, row, &mut scratch);
    Ok(match ty.tc {
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => obj!(gnitz_wire::read_unsigned_exact(b)),
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => obj!(gnitz_wire::read_signed_exact(b)),
        TypeCode::F32 => obj!(f32::from_le_bytes(b.try_into().unwrap())),
        TypeCode::F64 => obj!(f64::from_le_bytes(b.try_into().unwrap())),
        TypeCode::U128 => obj!(u128::from_le_bytes(b.try_into().unwrap())),
        TypeCode::I128 => obj!(i128::from_le_bytes(b.try_into().unwrap())),
        TypeCode::UUID => obj!(format_uuid(u128::from_le_bytes(b.try_into().unwrap()))),
        TypeCode::Date => {
            let (y, m, d) = gnitz_expr::calendar::civil_from_days(gnitz_wire::read_signed_exact(b));
            PyDate::new(py, y as i32, m as u8, d as u8)?.into_any().unbind()
        }
        TypeCode::Timestamp => {
            let (days, h, mi, s, us) = gnitz_expr::calendar::split_micros(gnitz_wire::read_signed_exact(b));
            let (y, m, d) = gnitz_expr::calendar::civil_from_days(days);
            PyDateTime::new(py, y as i32, m as u8, d as u8, h as u8, mi as u8, s as u8, us, None)?
                .into_any()
                .unbind()
        }
        TypeCode::Decimal => py_decimal(py)?
            .call1((format_decimal(gnitz_wire::read_signed_exact(b).into(), ty.scale),))?
            .unbind(),
        // CPython validates the UTF-8 and raises `UnicodeDecodeError`.
        TypeCode::String => PyString::from_bytes(py, gnitz_wire::german_string_content(b, &batch.blob))?
            .into_any()
            .unbind(),
        TypeCode::Blob => PyBytes::new(py, gnitz_wire::german_string_content(b, &batch.blob))
            .into_any()
            .unbind(),
    })
}

// ---------------------------------------------------------------------------
// ScanResult
// ---------------------------------------------------------------------------

/// One result's presentation over a shared batch: the columns shown, in order,
/// and the `Row` subclass that names them.
struct Presented {
    schema: Arc<Schema>,
    batch: Arc<ZSetBatch>,
    cols: Vec<(ColumnLocator, ColType)>,
    row_type: Py<PyType>,
}

/// The visible columns of `schema` over `batch`, or every column when
/// `include_hidden`.
fn present(
    py: Python<'_>,
    schema: Arc<Schema>,
    batch: Arc<ZSetBatch>,
    include_hidden: bool,
) -> PyResult<Arc<Presented>> {
    let shown: Vec<usize> = if include_hidden {
        (0..schema.columns.len()).collect()
    } else {
        schema.visible_columns().map(|(ci, _)| ci).collect()
    };
    let cols = shown
        .iter()
        .map(|&ci| (SchemaFacts::locate(schema.as_ref(), ci), schema.columns[ci].ty()))
        .collect();
    // Interned: `field_pos` settles on a pointer compare, and the tuple's own
    // hash reads each element's cached one.
    let names = shown.iter().map(|&ci| PyString::intern(py, &schema.columns[ci].name));
    let fields = PyTuple::new(py, names)?;
    let row_type = row_type_for(py, &fields)?.unbind();
    Ok(Arc::new(Presented { schema, batch, cols, row_type }))
}

/// The one `ScanResult` build: read verbs, SQL rows, delta rows, `ZSetBatch.rows()`.
pub(crate) fn scan_result(py: Python<'_>, reply: ScanReply) -> PyResult<Py<PyScanResult>> {
    let data = present(py, reply.schema, Arc::new(reply.batch), false)?;
    Py::new(py, PyScanResult { data, lsn: reply.lsn })
}

#[pyclass(name = "ScanResult", frozen)]
pub struct PyScanResult {
    data: Arc<Presented>,
    /// The server-side LSN this result was read at, or `None` where there is
    /// none — a local answer, SQL rows, delta rows. Reporting 0 would collide
    /// with a real LSN 0.
    #[pyo3(get)]
    lsn: Option<u64>,
}

#[pymethods]
impl PyScanResult {
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(rust_schema_to_py(py, &self.data.schema)?.into_any())
    }

    fn __iter__(&self) -> PyRowIterator {
        PyRowIterator {
            data: Arc::clone(&self.data),
            row_buf: Vec::new(),
            pos: 0,
        }
    }

    /// Truthiness follows from this: CPython derives `__bool__` from `__len__`
    /// when a type defines no `nb_bool`.
    fn __len__(&self) -> usize {
        self.data.batch.len()
    }

    /// The same rows presenting every column, hidden key slots included.
    fn including_hidden(&self, py: Python<'_>) -> PyResult<PyScanResult> {
        let data = present(py, Arc::clone(&self.data.schema), Arc::clone(&self.data.batch), true)?;
        Ok(PyScanResult { data, lsn: self.lsn })
    }
}

#[pyclass(name = "RowIterator")]
pub struct PyRowIterator {
    data: Arc<Presented>,
    row_buf: Vec<Py<PyAny>>,
    pos: usize,
}

#[pymethods]
impl PyRowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let data = &*self.data;
        let row = self.pos;
        if row >= data.batch.len() {
            return Ok(None);
        }
        self.row_buf.clear();
        for &(loc, ty) in &data.cols {
            self.row_buf.push(cell_to_py(py, &data.batch, loc, ty, row)?);
        }
        // `drain` hands the values over already-owned, so the tuple build costs no
        // refcount traffic, and the Vec keeps its capacity for the next row.
        let values = PyTuple::new(py, self.row_buf.drain(..))?;
        // Through the subclass's own `tp_call`: pyo3 gives Rust no way to
        // instantiate a pyclass subtype.
        let obj = data.row_type.bind(py).call1((values, data.batch.weights[row]))?;
        self.pos += 1;
        Ok(Some(obj.unbind()))
    }
}

// ---------------------------------------------------------------------------
// DeltaReply
// ---------------------------------------------------------------------------

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

impl PyDeltaReply {
    /// A delta read's rows under `schema` — the reply carries no schema block,
    /// the client authored it — and the cursor it returned.
    pub(crate) fn new(
        py: Python<'_>,
        schema: Arc<Schema>,
        batch: ZSetBatch,
        cursor: gnitz_core::DeltaCursor,
    ) -> PyResult<Py<PyDeltaReply>> {
        let rows = scan_result(py, ScanReply { schema, batch, lsn: None })?;
        Py::new(py, PyDeltaReply { rows, cursor: (cursor.tag, cursor.tick) })
    }
}
