//! The wire→Python direction: every decode from a `ZSetBatch` to a Python
//! object, and the read-side pyclasses built on them.
//!
//! The per-cell decode is private: what leaves this module is [`scan_result`],
//! so no other module dispatches on a `TypeCode` to read a value.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDate, PyDateTime, PyDict, PyString, PyTuple, PyType};
use pyo3::IntoPyObjectExt;

use gnitz_core::{ScanReply, Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_wire::decimal::format_decimal;
use gnitz_wire::format_uuid;
use gnitz_wire::{ColType, TypeCode};

use crate::schema::PySchema;

/// `subclass` because a result presents its rows as a synthesised subclass
/// carrying its field names and one [`ColumnDescriptor`] per column
/// ([`RowType`]). `frozen` drops the borrow flag.
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

/// The `Row` subclass that presents the columns `names`, each with whether it
/// is hidden.
struct RowType(Py<PyType>);

impl RowType {
    /// One per distinct `names`, for the life of the process.
    fn of(py: Python<'_>, names: &[(&str, bool)]) -> PyResult<RowType> {
        static CACHE: LazyLock<Mutex<HashMap<Vec<u8>, Py<PyType>>>> = LazyLock::new(Default::default);
        // Length-prefixed, so no two column lists share a key.
        let mut key = Vec::new();
        for &(name, hidden) in names {
            key.extend_from_slice(&(name.len() as u32).to_le_bytes());
            key.extend_from_slice(name.as_bytes());
            key.push(hidden as u8);
        }
        if let Some(ty) = CACHE.lock().unwrap().get(&key) {
            return Ok(RowType(ty.clone_ref(py)));
        }
        let built = Self::build(py, names)?;
        Ok(RowType(CACHE.lock().unwrap().entry(key).or_insert(built).clone_ref(py)))
    }

    fn build(py: Python<'_>, names: &[(&str, bool)]) -> PyResult<Py<PyType>> {
        // Interned: `field_pos` settles on a pointer compare.
        let fields = PyTuple::new(py, names.iter().map(|(n, _)| PyString::intern(py, n)))?;
        // A repeated name reads its last visible column, else its last hidden one.
        let owner = |name: &str| {
            let last = |hidden: bool| names.iter().rposition(|&c| c == (name, hidden));
            last(false).or_else(|| last(true))
        };
        let keys = PyTuple::new(
            py,
            fields.iter().zip(names).enumerate().map(|(pos, (field, &(name, _)))| {
                if owner(name) == Some(pos) {
                    field.into_any()
                } else {
                    py.None().into_bound(py)
                }
            }),
        )?;
        let ns = PyDict::new(py);
        // No `__dict__` / `__weakref__` per row: a row's namespace is its columns.
        ns.set_item(intern!(py, "__slots__"), PyTuple::empty(py))?;
        ns.set_item(intern!(py, "_fields"), &fields)?;
        ns.set_item(intern!(py, "_keys"), &keys)?;
        let base = py.get_type::<PyRow>();
        for (pos, name) in keys.iter().enumerate() {
            let Ok(name) = name.cast::<PyString>() else {
                continue;
            };
            // A name the row object itself answers keeps that meaning.
            if !(base.hasattr(name)? || ns.contains(name)?) {
                ns.set_item(name, Bound::new(py, ColumnDescriptor { pos })?)?;
            }
        }
        let bases = PyTuple::new(py, [base])?;
        let ty = py.get_type::<PyType>().call1((intern!(py, "Row"), bases, ns))?;
        Ok(ty.cast_into::<PyType>()?.unbind())
    }

    fn row(&self, py: Python<'_>, values: Bound<'_, PyTuple>, weight: i64) -> PyResult<Py<PyAny>> {
        let init = PyClassInitializer::from(PyRow { values: values.unbind(), weight });
        // SAFETY: `build` is the only source of the type: a `PyRow` subclass
        // that adds no instance state.
        unsafe {
            let ptr = pyo3::impl_::pymethods::tp_new_impl::<_, PyRow>(py, init, self.0.bind(py).as_type_ptr())?;
            Ok(Bound::from_owned_ptr(py, ptr).unbind())
        }
    }
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

/// A tuple attribute of the row's subclass.
fn type_tuple<'py>(slf: &Bound<'py, PyRow>, attr: &Bound<'py, PyString>) -> PyResult<Bound<'py, PyTuple>> {
    Ok(slf.get_type().getattr(attr)?.cast_into::<PyTuple>()?)
}

#[pymethods]
impl PyRow {
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
            match field_pos(&type_tuple(slf, intern!(py, "_keys"))?, name)? {
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
        let py = slf.py();
        let parts = type_tuple(slf, intern!(py, "_fields"))?
            .as_slice()
            .iter()
            .zip(slf.get().values.bind(py).as_slice())
            .map(|(name, val)| Ok(format!("{}={}", name.extract::<&str>()?, val.repr()?)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(format!("Row({}, _weight={})", parts.join(", "), slf.get().weight))
    }

    pub fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(other_row) = other.cast::<PyRow>() {
            let eq: bool = self.values.bind(py).eq(other_row.get().values.bind(py))?;
            eq.into_py_any(py)
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
        for (name, val) in type_tuple(slf, intern!(py, "_keys"))?
            .as_slice()
            .iter()
            .zip(slf.get().values.bind(py).as_slice())
        {
            if !name.is_none() {
                dict.set_item(name, val)?;
            }
        }
        Ok(dict.unbind())
    }
}

// ---------------------------------------------------------------------------
// Cell decode
// ---------------------------------------------------------------------------

/// The years a `datetime.date` holds.
const PY_YEARS: std::ops::RangeInclusive<i64> = 1..=9999;

/// `decimal.Decimal`, imported once.
fn py_decimal(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static DECIMAL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    DECIMAL
        .get_or_try_init(py, || {
            Ok::<_, PyErr>(py.import("decimal")?.getattr("Decimal")?.unbind())
        })
        .map(|d| d.bind(py))
}

/// The cell at `loc` in `row`, PK or payload.
fn cell_to_py(py: Python<'_>, batch: &ZSetBatch, loc: ColumnLocator, ty: ColType, row: usize) -> PyResult<Py<PyAny>> {
    if loc.is_null(batch, row) {
        return Ok(py.None());
    }
    let mut scratch = [0u8; 16];
    let b = loc.native_le_bytes(batch, row, &mut scratch);
    Ok(match ty.tc {
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => {
            gnitz_wire::read_unsigned_exact(b).into_py_any(py)?
        }
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
            gnitz_wire::read_signed_exact(b).into_py_any(py)?
        }
        TypeCode::F32 => f32::from_le_bytes(b.try_into().unwrap()).into_py_any(py)?,
        TypeCode::F64 => f64::from_le_bytes(b.try_into().unwrap()).into_py_any(py)?,
        TypeCode::U128 => u128::from_le_bytes(b.try_into().unwrap()).into_py_any(py)?,
        TypeCode::I128 => i128::from_le_bytes(b.try_into().unwrap()).into_py_any(py)?,
        TypeCode::UUID => format_uuid(u128::from_le_bytes(b.try_into().unwrap())).into_py_any(py)?,
        TypeCode::Date => {
            let days = gnitz_wire::read_signed_exact(b);
            let (y, m, d) = gnitz_expr::calendar::civil_from_days(days);
            if PY_YEARS.contains(&y) {
                PyDate::new(py, y as i32, m as u8, d as u8)?.into_any().unbind()
            } else {
                days.into_py_any(py)?
            }
        }
        TypeCode::Timestamp => {
            let micros = gnitz_wire::read_signed_exact(b);
            let (days, h, mi, s, us) = gnitz_expr::calendar::split_micros(micros);
            let (y, m, d) = gnitz_expr::calendar::civil_from_days(days);
            if PY_YEARS.contains(&y) {
                PyDateTime::new(py, y as i32, m as u8, d as u8, h as u8, mi as u8, s as u8, us, None)?
                    .into_any()
                    .unbind()
            } else {
                micros.into_py_any(py)?
            }
        }
        TypeCode::Decimal => py_decimal(py)?
            .call1((format_decimal(gnitz_wire::read_signed_exact(b).into(), ty.scale),))?
            .unbind(),
        // CPython validates the UTF-8 and raises `UnicodeDecodeError`.
        TypeCode::String => PyString::from_bytes(py, loc.content(batch, row))?.into_any().unbind(),
        TypeCode::Blob => PyBytes::new(py, loc.content(batch, row)).into_any().unbind(),
    })
}

// ---------------------------------------------------------------------------
// ScanResult
// ---------------------------------------------------------------------------

/// The visible columns of `schema` over `batch`, or every column when
/// `include_hidden`.
pub(crate) fn present(
    py: Python<'_>,
    schema: Arc<Schema>,
    batch: Arc<ZSetBatch>,
    include_hidden: bool,
    lsn: Option<u64>,
) -> PyResult<PyScanResult> {
    let mut cols = Vec::with_capacity(schema.columns.len());
    let mut names = Vec::with_capacity(schema.columns.len());
    for (ci, c) in schema.columns.iter().enumerate() {
        if c.is_hidden && !include_hidden {
            continue;
        }
        cols.push((SchemaFacts::locate(schema.as_ref(), ci), c.ty));
        names.push((c.name.as_str(), c.is_hidden));
    }
    let row_type = RowType::of(py, &names)?;
    Ok(PyScanResult { schema, batch, cols, row_type, lsn })
}

/// The one `ScanResult` build: read verbs, SQL rows, delta rows, `ZSetBatch.rows()`.
pub(crate) fn scan_result(py: Python<'_>, reply: ScanReply) -> PyResult<Py<PyScanResult>> {
    Py::new(py, present(py, reply.schema, Arc::new(reply.batch), false, reply.lsn)?)
}

/// One result's presentation over a shared batch: the columns shown, in order,
/// and the `Row` subclass that names them.
#[pyclass(name = "ScanResult", frozen)]
pub struct PyScanResult {
    schema: Arc<Schema>,
    batch: Arc<ZSetBatch>,
    cols: Vec<(ColumnLocator, ColType)>,
    row_type: RowType,
    /// The server-side LSN this result was read at, or `None` where there is
    /// none — a local answer, delta rows, SQL rows no server read produced.
    /// Reporting 0 would collide with a real LSN 0.
    #[pyo3(get)]
    lsn: Option<u64>,
}

#[pymethods]
impl PyScanResult {
    #[getter]
    fn schema(&self) -> PySchema {
        PySchema { rust: Arc::clone(&self.schema) }
    }

    fn __iter__(slf: Py<Self>) -> PyRowIterator {
        PyRowIterator { data: slf, row_buf: Vec::new(), pos: 0 }
    }

    /// Truthiness follows from this: CPython derives `__bool__` from `__len__`
    /// when a type defines no `nb_bool`.
    fn __len__(&self) -> usize {
        self.batch.len()
    }

    /// The same rows presenting every column, hidden key slots included.
    fn including_hidden(&self, py: Python<'_>) -> PyResult<PyScanResult> {
        present(py, Arc::clone(&self.schema), Arc::clone(&self.batch), true, self.lsn)
    }
}

#[pyclass(name = "RowIterator")]
pub struct PyRowIterator {
    data: Py<PyScanResult>,
    row_buf: Vec<Py<PyAny>>,
    pos: usize,
}

#[pymethods]
impl PyRowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let data = self.data.get();
        let batch: &ZSetBatch = &data.batch;
        let row = self.pos;
        if row >= batch.len() {
            return Ok(None);
        }
        self.row_buf.clear();
        for &(loc, ty) in &data.cols {
            self.row_buf.push(cell_to_py(py, batch, loc, ty, row)?);
        }
        // `drain` hands the values over already-owned, so the tuple build costs no
        // refcount traffic, and the Vec keeps its capacity for the next row.
        let values = PyTuple::new(py, self.row_buf.drain(..))?;
        let obj = data.row_type.row(py, values, batch.weights[row])?;
        self.pos += 1;
        Ok(Some(obj))
    }
}
