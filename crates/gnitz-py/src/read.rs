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

use gnitz_core::{null_word_get, ColData, Schema, TypeCode, ZSetBatch};
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_wire::format_uuid;

use crate::build_pylist;
use crate::schema::rust_schema_to_py;

/// `subclass` because a result presents its rows as a synthesised subclass
/// carrying one [`ColumnDescriptor`] per column ([`row_type_for`]). `frozen`
/// because no method takes `&mut self`, which drops the borrow flag from every
/// row and inlines away the two atomic RMWs a `&self` method would otherwise run.
#[pyclass(name = "Row", frozen, subclass)]
pub struct PyRow {
    fields: Py<PyTuple>,
    values: Py<PyTuple>,
    weight: i64,
}

/// One presented column, as a descriptor in a row subclass's type dict. `row.col`
/// therefore resolves through `PyObject_GenericGetAttr` → `_PyType_Lookup`, which
/// CPython's type-attribute cache serves — no scan of the field names, and no
/// raised-and-caught `AttributeError` on the way to a `__getattr__` fallback.
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
/// cached for every later result with the same column names.
///
/// Process-global and never evicted: the key set is the distinct column-name
/// tuples a program reads, which its query set bounds — so it grows with the
/// source, not with the data.
///
/// Keyed by the tuple's *contents*, never by the `Arc<Schema>` address: an `Arc`
/// can be freed and a later allocation reuse the address, which would hand a
/// result someone else's descriptors. Interned names make the tuple's hash a
/// read of each element's cached hash, and it is paid once per result rather
/// than once per row.
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
        // The underscore namespace belongs to the row object itself (`_fields`,
        // `_asdict`, `_weight`), so such a column gets no descriptor and reaches
        // `PyRow::__getattr__` instead. Everything else is plain MRO: a column
        // named `weight` shadows the base accessor with no bookkeeping here, and
        // a hidden column sharing a visible one's name resolves to the first
        // position, as `field_pos` does.
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

/// Position of `name` among `fields`, or `None`. Both the field names
/// (`make_shared_batch_data`) and Python's own attribute/literal names are
/// interned, so the common case settles on the pointer compare; the string
/// compare covers a computed name. Linear over at most `MAX_COLUMNS` entries,
/// which is cheaper than keeping a side index in step with the tuple — this is
/// `row["name"]`, `scalars("name")` and the underscore-column fallback, never
/// `row.name`, which resolves through the subclass's own descriptors.
fn field_pos(fields: &Bound<'_, PyTuple>, name: &Bound<'_, PyString>) -> PyResult<Option<usize>> {
    // `as_slice` borrows the tuple's `ob_item` directly: no call and no refcount
    // traffic per element, where an indexed read is a bounds-checked
    // `PyTuple_GetItem` apiece. The identity pass stays separate from the
    // compare pass — fusing them would run a rich-compare on every field
    // *before* the interned hit, which is the work the identity pass skips.
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
    pub fn new(_py: Python<'_>, fields: Bound<'_, PyTuple>, values: Bound<'_, PyTuple>, weight: i64) -> PyResult<Self> {
        Ok(PyRow {
            fields: fields.unbind(),
            values: values.unbind(),
            weight,
        })
    }

    /// The row's Z-set weight, always. The row object owns its underscore
    /// names, so a column spelled `_weight` — which the write surface does
    /// permit, letting the schema take the name back there — is shadowed on
    /// attribute access here exactly as a column named `_fields` or `_asdict`
    /// is, and is read through `_asdict()` or by position. A column *may* be
    /// named `weight`, and its descriptor then shadows the alias below.
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
    /// `namedtuple`'s spelling. The tuple is the row's own field table, shared
    /// by every row of a result, so this is a borrow rather than a rebuild;
    /// deriving the names from `_asdict().keys()` instead materializes a dict
    /// per row.
    #[getter]
    pub fn _fields(&self, py: Python<'_>) -> Py<PyTuple> {
        self.fields.clone_ref(py)
    }

    /// The cold fallback: an underscore-prefixed *column* name, the one kind
    /// [`row_type_for`] installs no descriptor for. Reaching here costs a raised,
    /// fetched and normalized `AttributeError` from the generic-getattr miss that
    /// pyo3's `tp_getattro` runs first — which is why every other column is a
    /// descriptor instead.
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
        let values = self.values.bind(py);
        if key.cast::<pyo3::types::PyInt>().is_ok() {
            let idx = key.extract::<isize>()?;
            let len = values.len() as isize;
            let idx = if idx < 0 { idx + len } else { idx };
            if idx < 0 || idx >= len {
                return Err(pyo3::exceptions::PyIndexError::new_err("index out of range"));
            }
            return Ok(values.get_item(idx as usize)?.unbind());
        }
        if let Ok(name) = key.cast::<PyString>() {
            return match field_pos(self.fields.bind(py), name)? {
                Some(i) => Ok(values.get_item(i)?.unbind()),
                None => Err(pyo3::exceptions::PyKeyError::new_err(name.to_cow()?.into_owned())),
            };
        }
        Err(pyo3::exceptions::PyTypeError::new_err("key must be int or str"))
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

/// Materialize a `PkColumn` as a Python list. A single-column key surfaces as
/// that column's own Python type — decoded through the same address the row
/// path uses, so `batch.pks[i]` and `row[pk_col]` can never disagree about sign
/// or UUID rendering. A compound key surfaces as `bytes`: one packed PK region
/// per row.
pub(crate) fn pk_column_to_pylist(py: Python<'_>, schema: &Schema, batch: &ZSetBatch) -> PyResult<Py<PyList>> {
    if schema.pk_count() >= 2 {
        let stride = batch.pks.stride as usize;
        let chunks = batch.pks.buf.chunks_exact(stride);
        return Ok(PyList::new(py, chunks.map(|c| pyo3::types::PyBytes::new(py, c)))?.unbind());
    }
    // Through the resolved address, like every other decode — the offset and
    // width of the lone PK column are `SchemaFacts::locate`'s answer, not a
    // second derivation from `pk_stride` here.
    let ci = schema.pk_indices()[0];
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

/// Surface one 16-byte integer as the Python object its column type
/// dictates. The three 16-byte integer types share u128 storage but differ at
/// the surface: UUID → canonical string, I128 → signed int, everything else
/// (U128) → unsigned int. Single source of truth for that decision across every
/// read path (row build, batch columns, scan columns).
fn u128_value_to_py(py: Python<'_>, x: u128, tc: TypeCode) -> PyResult<Py<PyAny>> {
    Ok(match tc {
        TypeCode::UUID => format_uuid(x).into_pyobject(py)?.into_any().unbind(),
        TypeCode::I128 => (x as i128).into_pyobject(py)?.into_any().unbind(),
        _ => x.into_pyobject(py)?.into_any().unbind(),
    })
}

/// Decode one payload cell into a Python object, null bit first: a set bit is
/// `None` regardless of the stored value. Per-`ColData` decode — Fixed →
/// [`fixed_value_to_py`], Strings → str, Bytes → bytes — shared by the row
/// build and the `scalars` column loop. `tc` and `stride` are the column's type
/// code and wire stride, precomputed by the caller (`stride` is read only for
/// the Fixed arm).
fn cell_to_py(
    py: Python<'_>,
    col: &ColData,
    row: usize,
    is_null: bool,
    tc: TypeCode,
    stride: usize,
) -> PyResult<Py<PyAny>> {
    if is_null {
        return Ok(py.None());
    }
    Ok(match col {
        ColData::Fixed(buf) => fixed_value_to_py(py, tc, &buf[row * stride..(row + 1) * stride])?,
        ColData::Strings(v) => match &v[row] {
            Some(s) => s.into_pyobject(py)?.into_any().unbind(),
            None => py.None(),
        },
        ColData::Bytes(v) => match &v[row] {
            Some(b) => pyo3::types::PyBytes::new(py, b).into_any().unbind(),
            None => py.None(),
        },
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
    // Interned: each `mappings()` dict insert then hits on identity instead of
    // hashing the name, `row["col"]` settles on a pointer compare inside
    // `field_pos`, and the field tuple's own hash — the key `row_type_for`
    // caches on — is a read of each element's cached hash.
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

/// Decode one cell at `loc` in `row`, for either a PK or a payload column.
/// The single per-cell decode, shared by the row build, `scalars`, the PK list
/// and the per-column lists. A PK column reads its own bytes straight out of
/// the PK region — no whole-tuple copy, so a one-column read costs one column.
fn value_at(py: Python<'_>, batch: &ZSetBatch, ci: usize, loc: ColumnLocator, row: usize) -> PyResult<Py<PyAny>> {
    let tc = TypeCode::from_validated_u8(loc.type_code());
    match loc {
        ColumnLocator::Pk { byte_off, size, .. } => {
            let w = batch.pks.col_window(row, byte_off as usize, size as usize);
            fixed_value_to_py(py, tc, w)
        }
        ColumnLocator::Payload { slot, size, .. } => {
            let is_null = null_word_get(batch.nulls[row], slot as usize);
            cell_to_py(py, &batch.columns[ci], row, is_null, tc, size as usize)
        }
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
    // refcount traffic — and the Vec keeps its capacity for the next row.
    let values = PyTuple::new(py, buf.drain(..))?;
    // Through the subclass's own `tp_call` rather than `Py::new`: pyo3 gives Rust
    // no way to instantiate a pyclass subtype, so the row is built through the
    // subclass's own constructor. One extra call per row built, against
    // a per-row decode that already allocates one Python object per column — and
    // it is what buys every attribute read on the row a `_PyType_Lookup` hit.
    data.row_type
        .bind(py)
        .call1((data.fields.bind(py), values, data.batch.weights[row]))
        .map(Bound::unbind)
}

/// Materialize per-column value lists, indexed by *physical* column. A PK column
/// holds an empty list — the PK region is surfaced through `.pks`. Decoding runs
/// through [`cell_to_py`] under the column's resolved address, so a NULL reads
/// back as `None` here exactly as it does through a `Row` or `scalars()`.
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
    /// The server-side LSN this result was read at, or `None` where the result
    /// carries no LSN at all — a SQL `Rows` payload, which `SqlResult::Rows`
    /// does not carry one for. Reporting 0 there was indistinguishable from a
    /// genuine LSN 0.
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
        // `as_slice`, as `field_pos` and `_asdict` read it: an indexed walk
        // increfs every name and allocates a Vec per call.
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
        // Resolve col: None→first presented column, int→presented position
        // (consistent with row indexing), str→presented-name lookup.
        let pos = match col {
            None => 0usize,
            Some(ref obj) => {
                let obj = obj.bind(py);
                if obj.cast::<pyo3::types::PyInt>().is_ok() {
                    obj.extract::<usize>()?
                } else if let Ok(name) = obj.cast::<PyString>() {
                    match field_pos(data.fields.bind(py), name)? {
                        Some(i) => i,
                        None => return Err(pyo3::exceptions::PyKeyError::new_err(name.to_cow()?.into_owned())),
                    }
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

/// One delta read's answer: the rows, and the cursor to poll from next. The two
/// travel together because a subscriber that kept one without the other would
/// either re-apply rounds it already has or step over rounds it never received.
#[pyclass(name = "DeltaReply", frozen)]
pub struct PyDeltaReply {
    /// The delta rows, weights included — a retraction arrives at weight −1.
    #[pyo3(get)]
    rows: Py<PyScanResult>,
    /// Identifies the boot and the relation this cursor belongs to.
    #[pyo3(get)]
    tag: u64,
    /// The last tick round the reply covers.
    #[pyo3(get)]
    tick: u64,
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
    Py::new(
        py,
        PyDeltaReply {
            rows,
            tag: cursor.tag,
            tick: cursor.tick,
        },
    )
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
