use std::ffi::CStr;
use std::sync::Arc;

use pyo3::ffi;
use pyo3::impl_::extract_argument::argument_extraction_error;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use pyo3::Borrowed;

use gnitz_core::protocol::types::type_code_from_u64;
use gnitz_core::{
    null_word_get, null_word_set, ClientError, ColData, ColumnDef, PkColumn, Schema, TableProps, TypeCode,
    WireConflictMode, ZSetBatch,
};
use gnitz_core::{ConflictClass, DeltaCursor, GnitzClient, MirrorError, PollOutcome, PollResult};
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_mirror::Mirror;
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
// A delta cursor named tick rounds the server's delta store has already dropped
// (STATUS_DELTA_EXPIRED). A subtype of GnitzError, like the conflict error, so
// `except GnitzError` still catches it while a subscriber can name it and
// bootstrap again.
pyo3::create_exception!(_native, GnitzDeltaExpiredError, GnitzError);
// A mirror store refused every further call that touches a copy, because a delta
// did not reach it and the hole a cursor would step over is unrecoverable. A
// subclass of GnitzError, like the conflict and expiry errors, so
// `except GnitzError` still catches it while a host that wants to release the
// store and start over can name it. `close_mirror()` is that recovery.
pyo3::create_exception!(_native, GnitzMirrorPoisonedError, GnitzError);

/// Wrap any `Display` error as a `GnitzError` PyErr. For the handful of
/// failures that carry no retryability verdict (handshake, waker setup).
fn gnitz_err(e: impl std::fmt::Display) -> PyErr {
    GnitzError::new_err(e.to_string())
}

/// Wrap a failure that classifies itself: a retryable OCC conflict becomes the
/// dedicated `GnitzConflictError` (a `GnitzError` subclass, so `except
/// GnitzError` still catches it while a retrying caller can name it), everything
/// else the generic `GnitzError`.
///
/// Generic over the error, never over a detached `(message, verdict)` pair: the
/// verdict is read off the error at the one point that raises, so no call site
/// can pair a message with someone else's classification.
fn classified_err(e: &(impl std::fmt::Display + ConflictClass)) -> PyErr {
    if e.is_conflict() {
        GnitzConflictError::new_err(e.to_string())
    } else {
        gnitz_err(e)
    }
}

/// [`classified_err`] plus the two variants an owned [`ClientError`] can be
/// matched on: `Interrupted` re-raises the `PyErr` it carries, so a
/// `KeyboardInterrupt` stays one, and `DeltaExpired` gets the class a subscriber
/// catches to bootstrap again. Every path producing either raises the same
/// class, because every path maps through here.
fn client_err(e: ClientError) -> PyErr {
    match e {
        ClientError::Interrupted(inner) => match inner.downcast::<PyErr>() {
            Ok(py_err) => *py_err,
            Err(other) => gnitz_err(other),
        },
        ClientError::DeltaExpired => GnitzDeltaExpiredError::new_err(e.to_string()),
        // The arm, and not a pre-call check on the client, is what raises the
        // poison class: refusing *every* method up front would take the
        // connection's own reads down with the copy, where only a read that would
        // have come off the copy is refused.
        ClientError::Mirror(MirrorError::Poisoned(_)) => GnitzMirrorPoisonedError::new_err(e.to_string()),
        other => classified_err(&other),
    }
}

/// [`client_err`] for the SQL layer's error, which wraps a `ClientError`.
fn sql_err(e: gnitz_sql::GnitzSqlError) -> PyErr {
    match e {
        gnitz_sql::GnitzSqlError::Exec(inner) => client_err(inner),
        other => classified_err(&other),
    }
}

/// Map a client error to a Python exception — the `Result`-shaped adapter for
/// a call that is not routed through [`PyGnitzClient::call`].
fn to_py_err<T>(res: Result<T, ClientError>) -> PyResult<T> {
    res.map_err(client_err)
}

// ---------------------------------------------------------------------------
// ColumnDef
// ---------------------------------------------------------------------------

/// `name` is held as an interned `PyString`, not a `String`: a `#[pyclass]`
/// getter over a Rust `String` builds a fresh `PyString` on *every* read, so
/// `c.name` would hand back a different object each time. Interned, every read
/// returns the identical object — the one a dict keyed by that name, or a
/// `**kwargs` splat built from it, hits on by pointer.
#[pyclass(name = "ColumnDef", get_all, frozen)]
pub struct PyColumnDef {
    pub name: Py<PyString>,
    pub type_code: u32,
    pub is_nullable: bool,
    pub primary_key: bool,
    pub is_hidden: bool,
}

#[pymethods]
impl PyColumnDef {
    #[new]
    #[pyo3(signature = (name, type_code, is_nullable = false, primary_key = false, is_hidden = false))]
    pub fn new(
        py: Python<'_>,
        name: &str,
        type_code: u32,
        is_nullable: bool,
        primary_key: bool,
        is_hidden: bool,
    ) -> Self {
        PyColumnDef {
            name: PyString::intern(py, name).unbind(),
            type_code,
            is_nullable,
            primary_key,
            is_hidden,
        }
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "ColumnDef(name={:?}, type_code={}, is_nullable={}, primary_key={}, is_hidden={})",
            self.name.bind(py).to_cow()?,
            self.type_code,
            self.is_nullable,
            self.primary_key,
            self.is_hidden
        ))
    }
}

fn py_col_to_rust(py: Python<'_>, c: &PyColumnDef) -> PyResult<ColumnDef> {
    let name = c.name.bind(py).to_cow()?.into_owned();
    type_code_from_u64(c.type_code as u64)
        .map(|tc| {
            let mut cd = ColumnDef::new(name, tc, c.is_nullable);
            cd.is_hidden = c.is_hidden;
            cd
        })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn rust_col_to_py(py: Python<'_>, c: &ColumnDef, primary_key: bool) -> PyResult<Py<PyAny>> {
    Ok(Py::new(
        py,
        PyColumnDef {
            name: PyString::intern(py, &c.name).unbind(),
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
#[pyclass(name = "Schema", frozen)]
pub struct PySchema {
    pub(crate) rust: Arc<Schema>,
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (columns, pk_indices = None))]
    pub fn new(columns: Bound<'_, PyList>, pk_indices: Option<Vec<usize>>) -> PyResult<Self> {
        if columns.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Schema must have at least 1 column",
            ));
        }
        // One pass: convert each column and note which ones carry the PK flag,
        // so inferring the PK list needs no second walk of the list.
        let mut cols = Vec::with_capacity(columns.len());
        let mut flagged = Vec::new();
        for (i, item) in columns.iter().enumerate() {
            let c: PyRef<'_, PyColumnDef> = item.extract()?;
            if c.primary_key {
                flagged.push(i);
            }
            cols.push(py_col_to_rust(columns.py(), &c)?);
        }
        // No explicit list: every flagged column, in declaration order. Nothing
        // flagged leaves the list empty, which `validate_pk_indices` rejects —
        // as `CREATE TABLE` rejects a PK-less table, so the two surfaces agree
        // on what a keyless relation is.
        let pk_cols = pk_indices.unwrap_or(flagged);
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
    // One scratch tuple for every key: each key rewrites the whole leading
    // `stride` bytes, which is all `push_tuple` reads.
    let mut t = gnitz_core::PkTuple::new(stride as u8);
    for pk_val in pks {
        if let Ok(bytes) = pk_val.cast::<pyo3::types::PyBytes>() {
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

/// Resolve a Python "schema-ish" argument to a `Bound<PySchema>`: a `Schema`,
/// or a list of `ColumnDef` wrapped in a fresh one. The single adapter for
/// every method that accepts either form.
fn resolve_py_schema<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PySchema>> {
    // Bare `Schema` first: it is the common case.
    if let Ok(s) = obj.cast::<PySchema>() {
        return Ok(s.clone());
    }
    // A sequence of ColumnDef → build a Schema (the PK rules are PySchema's).
    let list = obj.cast::<PyList>()?;
    Bound::new(obj.py(), PySchema::new(list.clone(), None)?)
}

/// Collect a known-length fallible iterator into a Python list, reserving that
/// length up front — which `collect::<PyResult<Vec<_>>>()` cannot do, because a
/// fallible collect lower-bounds its `size_hint` to 0 and grows from there.
fn build_pylist<'py, T: IntoPyObject<'py>>(
    py: Python<'py>,
    items: impl ExactSizeIterator<Item = PyResult<T>>,
) -> PyResult<Bound<'py, PyList>> {
    let mut out: Vec<T> = Vec::with_capacity(items.len());
    for item in items {
        out.push(item?);
    }
    PyList::new(py, out)
}

/// The schema's columns as a fresh Python list of `ColumnDef`, each flagged
/// with whether it is a PK column. Derived from the Rust `Schema` on every
/// access — it is the only representation, so `Schema.columns[i].primary_key`
/// always reflects the PK list the schema actually validated.
fn rust_columns_to_py<'py>(py: Python<'py>, s: &Schema) -> PyResult<Bound<'py, PyList>> {
    build_pylist(
        py,
        s.columns
            .iter()
            .enumerate()
            .map(|(i, c)| rust_col_to_py(py, c, s.is_pk_col(i))),
    )
}

fn rust_schema_to_py(py: Python<'_>, s: &Arc<Schema>) -> PyResult<Py<PySchema>> {
    Py::new(py, PySchema { rust: Arc::clone(s) })
}

// ---------------------------------------------------------------------------
// Row — Rust-native row object (replaces the former pure-Python Row class)
// ---------------------------------------------------------------------------

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

    /// The row's Z-set weight, always: no column can be named `_weight`, because
    /// the write surface reserves that spelling for the same quantity. A column
    /// *may* be named `weight`, and its descriptor then shadows the alias below.
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
// ZSetBatch — Rust-native storage (write path)
// ---------------------------------------------------------------------------

/// Extract one 16-byte integer value. Keyed off [`TypeCode::is_wide_int`] by
/// both write paths, so a newly added 16-byte type cannot fall through to the
/// fixed-width arm unnoticed.
///
/// Text reaches only a type [`TypeCode::admits_text_literal`] admits it for —
/// UUID here — the same predicate the SQL `INSERT` writer gates on, so a `U128`
/// column takes an integer and nothing else on either path. `u128`, not the
/// `i128` arm, because a `U128` above `i128::MAX` is legal.
fn extract_wide_int(tc: TypeCode, val: &Bound<'_, PyAny>) -> PyResult<u128> {
    if tc.admits_text_literal() {
        return extract_uuid_or_u128(val);
    }
    match tc {
        TypeCode::U128 => val.extract::<u128>(),
        _ => Ok(val.extract::<i128>()? as u128),
    }
}

/// Encode one non-null PK value into `dst`, whose length is `tc.wire_stride()`.
/// The one typed PK encoder: shared by the keyword append path (which already
/// knows the type code and offset from its resolved plan), the dict path, and
/// [`py_pks_to_column`], so a signed, UUID, or wide key packs the same way
/// whichever surface supplied it.
fn write_pk_bytes(dst: &mut [u8], tc: TypeCode, val: &Bound<'_, PyAny>) -> PyResult<()> {
    if tc.is_wide_int() {
        dst.copy_from_slice(&extract_wide_int(tc, val)?.to_le_bytes());
        return Ok(());
    }
    write_fixed_le_into(dst, tc, val)
}

/// Encode one PK column's Python value into `t` at that column's byte offset,
/// rejecting `None`.
fn write_pk_col_into(schema: &Schema, t: &mut gnitz_core::PkTuple, ci: usize, val: &Bound<'_, PyAny>) -> PyResult<()> {
    if val.is_none() {
        return Err(pk_none_err(schema, ci));
    }
    let tc = schema.columns[ci].type_code;
    let off = schema.pk_byte_offset(ci);
    write_pk_bytes(&mut t.buf[off..off + tc.wire_stride()], tc, val)
}

/// `None` reached a PK column.
fn pk_none_err(schema: &Schema, ci: usize) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("PK column {:?} cannot be None", schema.columns[ci].name))
}

/// Stores batch data in Rust Vecs with a cached Schema. `append` / `extend`
/// handle all type extraction, null tracking, and PK handling in Rust, so
/// `push()` reuses the cached schema and batch without any Python→Rust
/// re-extraction.
#[pyclass(name = "ZSetBatch")]
pub struct PyZSetBatch {
    pub(crate) schema: Arc<Schema>,
    pub(crate) batch: ZSetBatch,
    /// PK scratch for the row under construction. Reused rather than rebuilt
    /// per row, which would re-zero all `MAX_PK_BYTES`: a row missing a PK
    /// column is an error, so every row rewrites the whole `pk_stride` prefix
    /// `push_tuple` reads.
    key_scratch: gnitz_core::PkTuple,
    /// Column index of each payload column, by dense payload index.
    payload_cols: Vec<usize>,
    /// Is [`WEIGHT_KW`] the name of a column? Then it means that column, not the
    /// row weight: the schema is the authority on what a name means, and
    /// `extend`'s own `_weight` parameter still sets the weight for such a batch.
    weight_is_column: bool,
    /// The plan the last row was written through — see [`KwPlan`].
    kw_plan: Option<KwPlan>,
}

/// One row under construction. Payload values land in the batch's columns as
/// they arrive; the PK, weight and null word are pushed together at the end.
/// Both append surfaces write through this, so they agree on column ordering,
/// null handling and error messages by construction.
struct RowWriter<'a> {
    batch: &'a mut ZSetBatch,
    schema: &'a Schema,
    nulls: u64,
}

impl<'a> RowWriter<'a> {
    fn new(batch: &'a mut ZSetBatch, schema: &'a Schema) -> Self {
        RowWriter {
            batch,
            schema,
            nulls: 0,
        }
    }

    /// `None` — no value supplied, or an explicit `None` — writes NULL. A hidden
    /// payload column takes neither: it is a DROP COLUMN tombstone, which stays
    /// NOT NULL, so it takes the same zero filler the SQL `INSERT` writer
    /// pushes. (A hidden *PK* column is a view's key slot, and is written
    /// normally.)
    fn payload(&mut self, payload_idx: usize, ci: usize, val: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        let col = &self.schema.columns[ci];
        if self.schema.is_hidden_payload(ci) {
            self.batch.columns[ci].push_filler(col.type_code);
            return Ok(());
        }
        match val {
            Some(v) if !v.is_none() => push_column_value(self.batch, ci, col.type_code, v),
            _ => {
                if !col.is_nullable {
                    return Err(not_nullable_err(&col.name));
                }
                null_word_set(&mut self.nulls, payload_idx, true);
                self.batch.columns[ci].push_null(col.type_code);
                Ok(())
            }
        }
    }

    /// Close the row: the PK, its weight and the null word are pushed together.
    /// Consuming, so a writer cannot outlive its row and push a second one under
    /// the first row's accumulated null word.
    fn finish(self, key: &gnitz_core::PkTuple, weight: i64) {
        self.batch.pks.push_tuple(key);
        self.batch.weights.push(weight);
        self.batch.nulls.push(self.nulls);
    }
}

/// Private helpers — shared between `append` and `extend`.
impl PyZSetBatch {
    /// Run `body` against `self`; on error, truncate every per-row vector back
    /// to the pre-call row count, so a half-written row never leaves the batch
    /// with mismatched column lengths. Wrap once per surface — `append` its one
    /// row, `extend` its whole loop — and never nested.
    fn with_rollback<F>(&mut self, body: F) -> PyResult<()>
    where
        F: FnOnce(&mut Self) -> PyResult<()>,
    {
        // `weights` rather than `batch.len()`: the row count without the
        // division a compound-PK `PkColumn::len` does.
        let n = self.batch.weights.len();
        match body(self) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.batch.truncate(n, self.schema.as_ref());
                Err(e)
            }
        }
    }
}

/// Append one non-null payload value. The `let … else` arms make the
/// schema/`ColData` agreement a checked precondition: a mismatch would
/// otherwise push nothing while the weight and null vectors still advanced,
/// silently skewing that column's length against the rest of the batch.
fn push_column_value(batch: &mut ZSetBatch, ci: usize, tc: TypeCode, val: &Bound<'_, PyAny>) -> PyResult<()> {
    let col = &mut batch.columns[ci];
    match tc {
        TypeCode::String => {
            let ColData::Strings(v) = col else { variant_mismatch() };
            v.push(Some(val.extract::<String>()?));
        }
        TypeCode::Blob => {
            let ColData::Bytes(v) = col else { variant_mismatch() };
            v.push(Some(val.extract::<Vec<u8>>()?));
        }
        tc if tc.is_wide_int() => {
            let ColData::Fixed(buf) = col else { variant_mismatch() };
            buf.extend_from_slice(&extract_wide_int(tc, val)?.to_le_bytes());
        }
        tc => {
            let ColData::Fixed(buf) = col else { variant_mismatch() };
            write_fixed_le(buf, tc, val)?;
        }
    }
    Ok(())
}

/// A PK column had no value supplied.
#[cold]
fn missing_pk_err(schema: &Schema, ci: usize) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("missing PK column {:?}", schema.columns[ci].name))
}

/// `None` reached a NOT NULL column.
#[cold]
fn not_nullable_err(name: &str) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("Non-nullable column {name:?} cannot be None"))
}

/// The `TypeError` for a supplied name that matches no column. Shared by both
/// append surfaces, so the same mistake reads the same either way.
#[cold]
fn unexpected_name_err(name: &str) -> PyErr {
    pyo3::exceptions::PyTypeError::new_err(format!(
        "ZSetBatch got an unexpected column name '{name}' (the row weight is spelled '{WEIGHT_KW}')"
    ))
}

/// A payload column's `ColData` variant did not match its declared type code.
/// `ZSetBatch::new` derives one from the other, so reaching this means the batch
/// was assembled outside that constructor.
#[cold]
#[inline(never)]
fn variant_mismatch() -> ! {
    panic!("ColData variant does not match the column's type code")
}

// ---------------------------------------------------------------------------
// The row writer — one resolved plan, shared by both append surfaces
// ---------------------------------------------------------------------------

/// The keyword that carries a row's Z-set weight, when no column claims that
/// name.
const WEIGHT_KW: &str = "_weight";

/// One resolved call shape: which argument feeds each schema slot. `append`
/// resolves it from the keyword-name tuple CPython hands the call, `extend` from
/// the key sequence it walks off a row dict.
///
/// A batch caches one. A receiver that does alternate shapes rebuilds per row —
/// correct, and ~1-2 µs against a ~92 ns row.
struct KwPlan {
    kwnames: Py<PyTuple>,
    /// Argument position of [`WEIGHT_KW`], if the call passed it.
    weight: Option<usize>,
    /// `(argument position, column index)` per PK column, in PK order.
    pks: Vec<(usize, usize)>,
    /// Argument position per payload column, by dense payload index. `None`
    /// means no keyword supplied it, so the row takes NULL there.
    payload: Vec<Option<usize>>,
}

/// Same names in the same order? Interned names settle on pointer identity; the
/// text compare is the fallback for names that were not interned.
fn kwnames_eq(a: &[Bound<'_, PyAny>], b: &[Bound<'_, PyAny>]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            if x.is(y) {
                return true;
            }
            let (Ok(xs), Ok(ys)) = (x.cast::<PyString>(), y.cast::<PyString>()) else {
                return false;
            };
            matches!((xs.to_str(), ys.to_str()), (Ok(xc), Ok(yc)) if xc == yc)
        })
}

/// Is `plan` the one resolved from this call's names? A call site with its own
/// keyword tuple is tried by pointer alone: CPython holds a literal call site's
/// name tuple as a code-object constant, so the same object comes back every
/// iteration and one compare settles the hot path.
fn plan_hits(py: Python<'_>, plan: &KwPlan, names: &[Bound<'_, PyAny>], tuple: Option<&Bound<'_, PyTuple>>) -> bool {
    if let Some(t) = tuple {
        if plan.kwnames.as_ptr() == t.as_ptr() {
            return true;
        }
    }
    kwnames_eq(plan.kwnames.bind(py).as_slice(), names)
}

/// First position in `names` holding `col`.
fn kw_position(names: &[Bound<'_, PyString>], col: &str) -> Option<usize> {
    names.iter().position(|n| n.to_str().is_ok_and(|s| s == col))
}

/// Resolve a name tuple into a write plan. Cold: once per call shape.
///
/// Walks the *schema*, not the name list, so a name held by two columns feeds
/// both — `Schema` admits duplicate names, since hidden columns are exempt from
/// the duplicate-name check. A hidden payload column is skipped: it is a DROP
/// COLUMN tombstone [`RowWriter::payload`] fills itself.
fn build_kw_plan(schema: &Schema, weight_is_column: bool, kwnames: &Bound<'_, PyTuple>) -> PyResult<KwPlan> {
    let nkw = kwnames.len();
    let mut names: Vec<Bound<'_, PyString>> = Vec::with_capacity(nkw);
    for item in kwnames.as_slice() {
        names.push(
            item.clone()
                .cast_into::<PyString>()
                .map_err(|_| pyo3::exceptions::PyTypeError::new_err("column names must be strings"))?,
        );
    }

    let mut consumed = vec![false; nkw];
    let weight = if weight_is_column {
        None
    } else {
        kw_position(&names, WEIGHT_KW)
    };
    if let Some(i) = weight {
        consumed[i] = true;
    }
    let mut pks = Vec::with_capacity(schema.pk_indices().len());
    for &ci in schema.pk_indices() {
        let Some(i) = kw_position(&names, &schema.columns[ci].name) else {
            return Err(missing_pk_err(schema, ci));
        };
        consumed[i] = true;
        pks.push((i, ci));
    }
    let mut payload = Vec::with_capacity(nkw);
    for (_, ci, col) in schema.payload_columns() {
        let pos = if schema.is_hidden_payload(ci) {
            None
        } else {
            kw_position(&names, &col.name)
        };
        if let Some(i) = pos {
            consumed[i] = true;
        }
        payload.push(pos);
    }
    if let Some(i) = consumed.iter().position(|c| !c) {
        let name = names[i].to_str()?;
        if names[..i].iter().any(|n| n.to_str().is_ok_and(|s| s == name)) {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "ZSetBatch.append() got multiple values for keyword argument '{name}'"
            )));
        }
        return Err(unexpected_name_err(name));
    }

    Ok(KwPlan {
        kwnames: kwnames.clone().unbind(),
        weight,
        pks,
        payload,
    })
}

impl PyZSetBatch {
    /// Write one row: resolve this call's shape against the cached plan, then
    /// read each column's value through `arg`, which maps a name's position in
    /// `names` to its value. `tuple` is those names as a Python tuple where the
    /// caller holds one, which the plan is matched against by pointer first.
    /// Rolls nothing back: each surface wraps its own call in
    /// [`Self::with_rollback`].
    fn write_row<'a, 'py>(
        &mut self,
        py: Python<'py>,
        names: &[Bound<'py, PyAny>],
        tuple: Option<&Bound<'py, PyTuple>>,
        arg: impl Fn(usize) -> Borrowed<'a, 'py, PyAny>,
        default_weight: i64,
    ) -> PyResult<()> {
        let PyZSetBatch {
            batch,
            schema,
            payload_cols,
            key_scratch,
            weight_is_column,
            kw_plan,
        } = &mut *self;
        let schema: &Schema = schema;
        let plan = match kw_plan {
            Some(p) if plan_hits(py, p, names, tuple) => p,
            slot => {
                let t = match tuple {
                    Some(t) => t.clone(),
                    None => PyTuple::new(py, names)?,
                };
                slot.insert(build_kw_plan(schema, *weight_is_column, &t)?)
            }
        };
        let weight = match plan.weight {
            Some(pos) => arg(pos)
                .extract::<i64>()
                .map_err(|e| argument_extraction_error(py, WEIGHT_KW, e))?,
            None => default_weight,
        };
        let mut row = RowWriter::new(batch, schema);
        for &(pos, ci) in &plan.pks {
            write_pk_col_into(schema, key_scratch, ci, &arg(pos))?;
        }
        for (payload_idx, &pos) in plan.payload.iter().enumerate() {
            let val = pos.map(&arg);
            row.payload(payload_idx, payload_cols[payload_idx], val.as_deref())?;
        }
        row.finish(key_scratch, weight);
        Ok(())
    }

    /// Split one row dict into the column `names` and `args` the row is written
    /// from, and the weight it is written at. [`WEIGHT_KW`] is taken out here
    /// rather than probed for: the walk visits every key anyway, and keeping it
    /// out of `names` lets a batch mixing inserts with retractions run on one
    /// plan. A *column* of that name takes it back — the schema is the authority.
    fn capture_row<'py>(
        &self,
        dict: &Bound<'py, PyDict>,
        names: &mut Vec<Bound<'py, PyAny>>,
        args: &mut Vec<Bound<'py, PyAny>>,
        default_weight: i64,
    ) -> PyResult<i64> {
        let py = dict.py();
        names.clear();
        args.clear();
        let mut supplied = None;
        for (key, val) in dict.iter() {
            if !self.weight_is_column && is_weight_key(py, &key) {
                supplied = Some(val);
                continue;
            }
            names.push(key);
            args.push(val);
        }
        // After the walk, not inside it: an `extract` runs Python, and a dict
        // mutated mid-iteration raises.
        match supplied {
            None => Ok(default_weight),
            Some(w) => w
                .extract::<i64>()
                .map_err(|e| argument_extraction_error(py, WEIGHT_KW, e)),
        }
    }
}

/// Does this dict key spell [`WEIGHT_KW`]? Interned keys — every literal in
/// Python source — settle on the pointer; the text compare is the fallback for a
/// name built at runtime.
fn is_weight_key(py: Python<'_>, key: &Bound<'_, PyAny>) -> bool {
    if key.is(pyo3::intern!(py, WEIGHT_KW)) {
        return true;
    }
    let Ok(text) = key.cast::<PyString>() else {
        return false;
    };
    text.to_str().is_ok_and(|s| s == WEIGHT_KW)
}

/// `ZSetBatch.append(**columns)` — the raw `METH_FASTCALL | METH_KEYWORDS` slot.
///
/// CPython compiles `b.append(pk=k, cust=c)` to `LOAD_CONST (('pk','cust'))` +
/// `CALL_KW`: the keyword-name tuple is a code-object constant, the same object
/// on every loop iteration, and the values arrive on the stack. Taking the
/// fastcall entry directly is what lets a call site be matched by pointer and
/// its resolved plan reused — no kwargs dict, no per-row name lookups, no args
/// tuple. `#[pymethods]` already emits this calling convention, but hands the
/// body a bound argument list rather than the raw `kwnames` tuple, so a plan
/// would have nothing to key on.
///
/// # Safety
/// CPython calling convention: `args` points at `nargs` positional values
/// followed by one value per name in `kwnames`.
unsafe fn append_fastcall(
    py: Python<'_>,
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> PyResult<*mut ffi::PyObject> {
    // The flag CPython ORs into `nargs` to say the callee may borrow the
    // caller's frame is not part of the count, so mask it off.
    if (nargs as usize) & !ffi::PY_VECTORCALL_ARGUMENTS_OFFSET != 0 {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "ZSetBatch.append() takes no positional arguments (columns are keywords)",
        ));
    }
    let slf = Borrowed::from_ptr(py, slf);
    let batch = slf.cast::<PyZSetBatch>()?;
    let batch: &Bound<'_, PyZSetBatch> = &batch;
    // A Python exception, not a panic: a value whose `__index__` re-enters
    // `append` on this batch must not abort the process.
    let mut b = batch.try_borrow_mut()?;
    // No keywords is not an error here — the empty plan reports the missing PK
    // columns.
    let (empty, kw);
    let kwnames = if kwnames.is_null() {
        empty = PyTuple::empty(py);
        &empty
    } else {
        kw = Borrowed::from_ptr(py, kwnames).cast::<PyTuple>()?;
        &kw
    };
    // SAFETY: the fastcall convention above — `args` holds one value per name in
    // `kwnames`, live for the duration of this call.
    let arg = |pos: usize| unsafe { Borrowed::from_ptr(py, *args.add(pos)) };
    b.with_rollback(|s| s.write_row(py, kwnames.as_slice(), Some(kwnames), arg, 1))?;
    drop(b);
    // Chainable, like the pyo3 method it replaces: `b.append(…).append(…)`.
    Ok(batch.clone().into_ptr())
}

/// CPython text signature (`name(…)\n--\n\n`). Without it the descriptor has no
/// `__doc__` and no `__text_signature__`, and `inspect.signature` fails. There
/// are no `.pyi` stubs, so this is where the argument names are documented.
const APPEND_DOC: &CStr = c"append($self, /, *, _weight=1, **columns)\n--\n\n\
Append one row, one keyword per column: batch.append(pk=1, name='x').\n\
An omitted or None column is NULL; _weight is the row's Z-set weight\n\
(default 1, negative to retract). Returns the batch, so appends chain.";

/// pyo3's own slot wrapper around [`append_fastcall`]: it attaches the
/// interpreter and returns null on error.
///
/// It is *not* what protects the re-entrant case — a value whose `__index__`
/// calls back into `append` on this batch is caught by `try_borrow_mut()?`
/// below, which returns a `PyBorrowMutError` (Python `RuntimeError`), never a
/// panic.
const ZSB_APPEND_METHOD: ffi::PyCFunctionFastWithKeywords =
    pyo3::get_trampoline_function!(fastcall_cfunction_with_keywords, append_fastcall);

/// `ffi::PyMethodDef` holds raw pointers so it is not `Sync`; CPython only ever
/// reads this one.
struct MethodDef(ffi::PyMethodDef);
unsafe impl Sync for MethodDef {}

static APPEND_METHOD_DEF: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"append".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunctionFastWithKeywords: ZSB_APPEND_METHOD,
    },
    ml_flags: ffi::METH_FASTCALL | ffi::METH_KEYWORDS,
    ml_doc: APPEND_DOC.as_ptr(),
});

/// Install [`ZSB_APPEND_METHOD`] as `ZSetBatch.append`.
fn install_append_method(py: Python<'_>) -> PyResult<()> {
    let ty = py.get_type::<PyZSetBatch>();
    let def = &APPEND_METHOD_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef;
    let desc = unsafe { ffi::PyDescr_NewMethod(ty.as_type_ptr(), def) };
    let desc = unsafe { Bound::from_owned_ptr_or_err(py, desc)? };
    ty.setattr("append", desc)
}

#[pymethods]
impl PyZSetBatch {
    /// Construct a batch for `schema` — a `Schema` or a bare list of
    /// `ColumnDef`, resolved through [`resolve_py_schema`].
    #[new]
    #[pyo3(signature = (schema))]
    pub fn new(schema: Bound<'_, PyAny>) -> PyResult<Self> {
        let rust_schema = Arc::clone(&resolve_py_schema(&schema)?.borrow().rust);
        let payload_cols: Vec<usize> = rust_schema.payload_columns().map(|(_, ci, _)| ci).collect();
        let weight_is_column = rust_schema.columns.iter().any(|c| c.name == WEIGHT_KW);
        let batch = ZSetBatch::new(&rust_schema);
        Ok(PyZSetBatch {
            key_scratch: gnitz_core::PkTuple::new(rust_schema.pk_stride() as u8),
            batch,
            payload_cols,
            weight_is_column,
            kw_plan: None,
            schema: rust_schema,
        })
    }

    /// Append rows from an iterable of dicts (one Rust call, no per-row
    /// Python→Rust crossing); returns the batch so calls chain. A per-row
    /// `_weight` key overrides the batch-wide `_weight`.
    ///
    /// Rows sharing a key sequence share one resolved write plan, so spell an
    /// absent nullable column as an explicit `None` rather than omitting its
    /// key: a varying key set is one plan resolution per change.
    #[pyo3(signature = (rows, _weight = 1))]
    pub fn extend<'py>(slf: Bound<'py, Self>, rows: Bound<'_, PyAny>, _weight: i64) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        // One scratch pair for the whole call, refilled per row: the names to
        // resolve the plan against, and the values the writer reads through.
        let mut names: Vec<Bound<'_, PyAny>> = Vec::new();
        let mut args: Vec<Bound<'_, PyAny>> = Vec::new();
        // Batch-level atomicity: the row writer rolls nothing back on its own, so
        // wrapping the whole loop truncates to the pre-call length on any error,
        // giving `extend` the same all-or-nothing contract `append` has.
        slf.borrow_mut().with_rollback(|s| {
            for row_item in rows.try_iter()? {
                let row_item = row_item?;
                let dict: &Bound<'_, PyDict> = row_item.cast()?;
                let weight = s.capture_row(dict, &mut names, &mut args, _weight)?;
                s.write_row(py, &names, None, |pos| args[pos].as_borrowed(), weight)?;
            }
            Ok(())
        })?;
        Ok(slf)
    }

    #[getter]
    pub fn pks(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        pk_column_to_pylist(py, &self.schema, &self.batch)
    }
    #[getter]
    pub fn columns(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        rust_batch_columns_to_py(py, self.schema.as_ref(), &self.batch)
    }
    /// Built straight off the weight region, as `ScanResult.weights` is —
    /// returning a `Vec<i64>` would clone the whole region only for pyo3 to
    /// walk the clone and drop it.
    #[getter]
    pub fn weights(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        Ok(PyList::new(py, &self.batch.weights)?.unbind())
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

/// Accept a Python int, `uuid.UUID` object (via `.int`), or string, returning
/// the 128-bit value. Int is tried first because it is the common case in
/// bulk inserts and avoids a Python attribute lookup per row.
///
/// A string is UUID text — canonical or bare 32-hex — and nothing else
/// (`gnitz_wire::parse_uuid`, the crate that owns wire-value text). Which
/// *columns* a string may be written to is not decided here: this function is
/// also reached from the schema-less wire paths (`pk_tuple_from_py`,
/// `seek_by_index`), which have no type code to consult. The type-directed
/// gate lives at [`extract_wide_int`], the one call site that knows the type.
fn extract_uuid_or_u128(val: &Bound<'_, PyAny>) -> PyResult<u128> {
    // `cast` before `extract` on both arms: a failed `extract` builds *and
    // normalizes* a full `PyErr` only to discard it, which a `uuid.UUID` or
    // string argument would otherwise pay on every row.
    if val.cast::<pyo3::types::PyInt>().is_ok() {
        return val.extract::<u128>();
    }
    if let Ok(s) = val.cast::<PyString>() {
        let s = s.to_cow()?;
        return gnitz_wire::parse_uuid(&s)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("invalid UUID string: {s:?}")));
    }
    // Last: `uuid.UUID` and friends, reached only once the cheap type tests fail.
    if let Ok(attr) = val.getattr(pyo3::intern!(val.py(), "int")) {
        if let Ok(n) = attr.extract::<u128>() {
            return Ok(n);
        }
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected int, uuid.UUID object, or UUID string",
    ))
}

/// Write one fixed-width value as little-endian bytes into `dst` (length must
/// equal `tc.wire_stride()`). Zero-allocation; used for PK regions, where the
/// destination already exists.
///
/// This table and [`write_fixed_le`]'s are kept apart deliberately: each writes
/// straight to its own destination shape, and folding either into the other
/// costs a second write per cell (measured there). Because both match every
/// `TypeCode` variant by name with no wildcard, a new fixed-width type is a
/// non-exhaustive-match error in *both* — the compiler, not convention, is what
/// keeps them in step.
///
/// In both, per-arm `extract` is also the range check — `extract::<u8>()` raises
/// Python's `OverflowError` for `append(c=300)` on a `U8` column, where a
/// width-generic pack would silently truncate.
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
            unreachable!("not a fixed-width type; handled before write_fixed_le_into")
        }
    }
    Ok(())
}

/// Append one fixed-width value as little-endian bytes to `buf`. Extends per
/// arm from the extracted value's own `to_le_bytes`, which is one write per
/// cell. Both ways of folding this into [`write_fixed_le_into`] cost a second:
/// a stack slot adds a 16-byte zero-init LLVM cannot prove away (it cannot see
/// the slot as fully initialized across the extraction call), and growing `buf`
/// by the stride first adds the fill that grow does. Measured, the in-place form
/// costs ~89 more instructions per 5-column `append` — ~6% of the call. The
/// duplication is deliberate; see [`write_fixed_le_into`] for what keeps the two
/// tables in step, and for the range check each arm's `extract` doubles as.
fn write_fixed_le(buf: &mut Vec<u8>, tc: TypeCode, item: &Bound<'_, PyAny>) -> PyResult<()> {
    match tc {
        TypeCode::U8 => buf.push(item.extract::<u8>()?),
        TypeCode::I8 => buf.push(item.extract::<i8>()? as u8),
        TypeCode::U16 => buf.extend_from_slice(&item.extract::<u16>()?.to_le_bytes()),
        TypeCode::I16 => buf.extend_from_slice(&item.extract::<i16>()?.to_le_bytes()),
        TypeCode::U32 => buf.extend_from_slice(&item.extract::<u32>()?.to_le_bytes()),
        TypeCode::I32 => buf.extend_from_slice(&item.extract::<i32>()?.to_le_bytes()),
        TypeCode::F32 => buf.extend_from_slice(&item.extract::<f32>()?.to_le_bytes()),
        TypeCode::U64 => buf.extend_from_slice(&item.extract::<u64>()?.to_le_bytes()),
        TypeCode::I64 => buf.extend_from_slice(&item.extract::<i64>()?.to_le_bytes()),
        TypeCode::F64 => buf.extend_from_slice(&item.extract::<f64>()?.to_le_bytes()),
        TypeCode::String | TypeCode::U128 | TypeCode::UUID | TypeCode::Blob | TypeCode::I128 => {
            unreachable!("handled before write_fixed_le")
        }
    }
    Ok(())
}

/// Materialize a `PkColumn` as a Python list. A single-column key surfaces as
/// that column's own Python type — decoded through the same address the row
/// path uses, so `batch.pks[i]` and `row[pk_col]` can never disagree about sign
/// or UUID rendering. A compound key surfaces as `bytes`: one packed PK region
/// per row.
fn pk_column_to_pylist(py: Python<'_>, schema: &Schema, batch: &ZSetBatch) -> PyResult<Py<PyList>> {
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
fn rust_batch_columns_to_py(py: Python<'_>, schema: &Schema, batch: &ZSetBatch) -> PyResult<Py<PyList>> {
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
fn delta_reply_to_py(
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
fn batch_to_lazy(
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

/// What one view's poll did. The round its copy now answers at is `cursor`; the
/// `reseeded` flag is the discontinuity a subscriber has to react to, and no
/// cursor carries it — an expiry-driven reseed inside one boot keeps the tag and
/// moves the tick forward, exactly as an ordinary advance does.
///
/// The flattening is this pyclass's, not the Rust type's: `PollResult` stays an
/// enum where its invariant matters.
#[pyclass(name = "PollResult", frozen, get_all)]
pub struct PyPollResult {
    /// The relation's server id, which a recreated view moves.
    view_id: u64,
    /// The copy was discarded and re-read whole. True for a first registration
    /// and for every recovery; false for a poll that applied deltas, for a reopen
    /// that resumed from its persisted cursor, and for a view whose poll failed —
    /// which is the right answer to "did this view reseed".
    reseeded: bool,
    /// Why this one view's poll failed, or `None`. The other views went on; the
    /// recovery is `forget_view(view_id)`.
    error: Option<String>,
    /// `(tag, tick)`, or `None` when the view has no valid copy.
    cursor: Option<(u64, u64)>,
}

impl From<PollOutcome> for PyPollResult {
    fn from(o: PollOutcome) -> PyPollResult {
        PyPollResult {
            view_id: o.view_id,
            reseeded: o.result.reseeded(),
            error: match &o.result {
                PollResult::Failed(e) => Some(e.to_string()),
                _ => None,
            },
            cursor: o.cursor.map(|c| (c.tag, c.tick)),
        }
    }
}

#[pymethods]
impl PyPollResult {
    /// Rendered the way Python prints the same values, not the way Rust does.
    fn __repr__(&self) -> String {
        let cursor = self
            .cursor
            .map_or("None".to_string(), |(tag, tick)| format!("({tag}, {tick})"));
        let error = self.error.as_ref().map_or("None".to_string(), |m| format!("{m:?}"));
        format!(
            "PollResult(view_id={}, reseeded={}, cursor={cursor}, error={error})",
            self.view_id,
            if self.reseeded { "True" } else { "False" },
        )
    }
}

/// The one way this crate obtains a connection, so that no caller can skip the
/// park hook: it is what makes a blocking call Ctrl-C-interruptible, by aborting
/// it with the signal handler's own exception for [`client_err`] to re-raise.
/// `GnitzClient::connect` cannot install it — `gnitz-core` must not depend on
/// pyo3, which is why `ParkHook` is the host's to supply.
pub(crate) fn connect_client(py: Python<'_>, target: &str) -> PyResult<GnitzClient> {
    let mut client = to_py_err(py.detach(|| GnitzClient::connect(target)))?;
    client.set_park_hook(Some(Box::new(|| {
        Python::attach(|py| py.check_signals()).map_err(|e| ClientError::Interrupted(Box::new(e)))
    })));
    Ok(client)
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

    /// Run one blocking client call: check the client is open, drop the GIL
    /// across it, and map the failure with `map` — [`client_err`] unless the
    /// path raises a class of its own.
    fn call_with<T: Send, E: Send>(
        &mut self,
        py: Python<'_>,
        map: impl FnOnce(E) -> PyErr,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, E> + Send,
    ) -> PyResult<T> {
        let c = self.live()?;
        py.detach(move || f(c)).map_err(map)
    }

    /// [`Self::call_with`] under the default error mapping (a retryable OCC
    /// conflict to `GnitzConflictError`). What a blocking method takes.
    fn call<T: Send>(
        &mut self,
        py: Python<'_>,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, ClientError> + Send,
    ) -> PyResult<T> {
        self.call_with(py, client_err, f)
    }
}

#[pymethods]
impl PyGnitzClient {
    #[new]
    pub fn new(py: Python<'_>, socket_path: &str) -> PyResult<Self> {
        Ok(PyGnitzClient {
            inner: Some(connect_client(py, socket_path)?),
        })
    }

    /// The client's current OCC basis (the running max of observed server
    /// watermarks, seeded from the HELLO ACK at connect). Read-only, for tests.
    #[getter]
    fn last_seen_lsn(&mut self) -> PyResult<u64> {
        Ok(self.live()?.last_seen_lsn())
    }

    /// Request frames this connection has written — what a test asserts a
    /// mirrored read does not move. Answers on a poisoned store.
    #[getter]
    pub fn requests_sent(&mut self) -> PyResult<u64> {
        Ok(self.live()?.requests_sent())
    }

    /// Close the connection and release any mirror store with it. Calling it
    /// twice is fine.
    ///
    /// The GIL goes down for the drop: with a store inside, that drop is a
    /// checkpoint plus an engine close — fsync-bound and unbounded in the copy's
    /// size — and it is also what releases the store's directory lock.
    pub fn close(&mut self, py: Python<'_>) {
        let taken = self.inner.take();
        py.detach(move || drop(taken));
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    pub fn __exit__(&mut self, py: Python<'_>, _exc_type: Py<PyAny>, _exc_val: Py<PyAny>, _exc_tb: Py<PyAny>) -> bool {
        self.close(py);
        false
    }

    // ----- DDL -----

    pub fn create_schema(&mut self, py: Python<'_>, name: &str) -> PyResult<u64> {
        self.call(py, |c| c.create_schema(name))
    }

    pub fn drop_schema(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        self.call(py, |c| c.drop_schema(name))
    }

    /// create_table(schema_name, table_name, columns).
    /// `columns` may be a `Schema` or a list of `ColumnDef` — resolved through
    /// [`resolve_py_schema`], so the PK
    /// columns come from the same rule every other schema surface applies.
    /// Partitioned, default distribution; no inline UNIQUE surface.
    pub fn create_table(
        &mut self,
        py: Python<'_>,
        schema_name: &str,
        table_name: &str,
        columns: Bound<'_, PyAny>,
    ) -> PyResult<u64> {
        let schema = Arc::clone(&resolve_py_schema(&columns)?.borrow().rust);
        let pk: Vec<u32> = schema.pk_indices().iter().map(|&i| i as u32).collect();
        self.call(py, move |c| {
            c.create_table(
                schema_name,
                table_name,
                &schema.columns,
                &pk,
                TableProps::default(),
                &[],
            )
        })
    }

    pub fn drop_table(&mut self, py: Python<'_>, schema_name: &str, table_name: &str) -> PyResult<()> {
        self.call(py, |c| c.drop_table(schema_name, table_name))
    }

    // ----- DML -----

    /// push(target_id, batch) -> ingest_lsn: int. Silent-upsert on PK conflict
    /// (DBSP z-set retraction semantics); SQL-standard rejection is reached via
    /// `INSERT` through `execute_sql`.
    pub fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>) -> PyResult<u64> {
        // Hold the `PyRef` guard here (it is `!Ungil`) and pass only the plain
        // `&Schema`/`&ZSetBatch` into the closure, so the GIL is free during
        // the blocking push without cloning the batch.
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        self.call(py, move |c| c.push(target_id, schema, b))
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
        self.call(py, move |c| c.delete(target_id, &rust_schema, pk_col))
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
        self.call(py, move |c| {
            c.create_view(schema_name, view_name, source_table_id, cols)
        })
    }

    pub fn drop_view(&mut self, py: Python<'_>, schema_name: &str, view_name: &str) -> PyResult<()> {
        self.call(py, |c| c.drop_view(schema_name, view_name))
    }

    /// resolve_table(schema_name, table_name) -> (tid: int, schema: Schema)
    pub fn resolve_table(&mut self, py: Python<'_>, schema_name: &str, table_name: &str) -> PyResult<Py<PyAny>> {
        let (tid, schema) = self.call(py, |c| c.resolve_table_or_view_id(schema_name, table_name))?;
        let py_schema = rust_schema_to_py(py, &schema)?.into_any();
        let tid_obj = tid.into_pyobject(py)?.into_any().unbind();
        Ok(PyTuple::new(py, [tid_obj, py_schema])?.into_any().unbind())
    }

    /// scan(target_id, include_hidden=False) -> ScanResult
    ///
    /// Every row of the relation, off this client's local copy if it mirrors one
    /// and from the server if it does not. `lsn` is `None` for a local answer: a
    /// served LSN is a server-side counter, and a copy's freshness is a feed
    /// round — `cursor(view_id)` is where a host reads it.
    #[pyo3(signature = (target_id, include_hidden = false))]
    pub fn scan(&mut self, py: Python<'_>, target_id: u64, include_hidden: bool) -> PyResult<Py<PyScanResult>> {
        let (schema, batch, lsn) = self.call(py, |c| c.scan_local_first(target_id))?;
        batch_to_lazy(py, schema, batch, lsn, include_hidden)
    }

    /// delta_bootstrap(view_id, view_schema, include_hidden=False) -> DeltaReply
    ///
    /// The view's whole current value, in the view's own schema, plus the cursor
    /// to poll from. `Delta { after_tick: 0 }` is the sum of every delta after
    /// round 0 — the view's entire history — which is precisely what its output
    /// store holds, so this costs exactly what a scan of the view costs. Apply it
    /// to a fresh copy: it replaces state, it does not add to it.
    #[pyo3(signature = (view_id, view_schema, include_hidden = false))]
    pub fn delta_bootstrap(
        &mut self,
        py: Python<'_>,
        view_id: u64,
        view_schema: &Bound<'_, PyAny>,
        include_hidden: bool,
    ) -> PyResult<Py<PyDeltaReply>> {
        let schema = Arc::clone(&resolve_py_schema(view_schema)?.borrow().rust);
        let out = self.call(py, |c| c.delta_bootstrap(view_id, &schema))?;
        delta_reply_to_py(py, schema, out, include_hidden)
    }

    /// delta_poll(view_id, reply_schema, tag, tick, include_hidden=False) -> DeltaReply
    ///
    /// Every delta the view emitted in `(tick, T]`, in `delta_reply_schema`'s
    /// shape. Apply what comes back and store the returned cursor; there is
    /// nothing to filter and nothing to reconcile.
    ///
    /// A `tag` that does not match the one the reply carries names a different
    /// boot or a different relation — a restart, or a `DROP VIEW` /
    /// `CREATE VIEW` of the same name. That is refused with
    /// `GnitzDeltaExpiredError`, not answered with rows: a foreign cursor draws
    /// the other relation's recent deltas, which are unsafe to apply. The
    /// recovery is the one that error always calls for — bootstrap again.
    /// A `tick` of 0 gets the same error for the same reason: it names no copy to
    /// continue, and a bootstrap comes back in the view's schema rather than in
    /// this one.
    #[pyo3(signature = (view_id, reply_schema, tag, tick, include_hidden = false))]
    pub fn delta_poll(
        &mut self,
        py: Python<'_>,
        view_id: u64,
        reply_schema: &Bound<'_, PyAny>,
        tag: u64,
        tick: u64,
        include_hidden: bool,
    ) -> PyResult<Py<PyDeltaReply>> {
        let schema = Arc::clone(&resolve_py_schema(reply_schema)?.borrow().rust);
        let cursor = gnitz_core::DeltaCursor { tag, tick };
        let out = self.call(py, |c| c.delta_poll(view_id, cursor, &schema))?;
        delta_reply_to_py(py, schema, out, include_hidden)
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
        let results = self.call(py, |c| c.scan_many(&target_ids))?;
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
        let triple = self.call(py, move |c| c.seek(table_id, &t))?;
        triple_to_lazy(py, triple, include_hidden)
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
        let mut keys: Vec<u128> = Vec::with_capacity(key_vals.len());
        for item in key_vals.iter() {
            keys.push(extract_uuid_or_u128(&item)?);
        }
        let triple = self.call(py, move |c| c.seek_by_index(table_id, &col_indices, &keys))?;
        triple_to_lazy(py, triple, include_hidden)
    }

    /// execute_sql(sql, schema_name="public") -> list of result dicts
    ///
    /// A `SELECT` (and the `EXPLAIN` of one) over a view this client mirrors is
    /// planned and answered against the local copy; every other statement, and
    /// every read of a relation the copy does not hold, runs on the connection.
    #[pyo3(signature = (sql, schema_name = "public"))]
    pub fn execute_sql(&mut self, py: Python<'_>, sql: &str, schema_name: &str) -> PyResult<Py<PyAny>> {
        // Plan + execute (all wire I/O, no Python) with the GIL released.
        let results = self.call_with(py, sql_err, |c| SqlPlanner::new(c, schema_name).execute(sql))?;
        sql_results_to_py(py, results)
    }

    // ----- Mirroring -----
    //
    // Every method that does work drops the GIL for all of it — the round trips,
    // and the engine and disk work between them. Only the metadata getters, which
    // answer out of memory, hold it.

    /// mirror_at(base_dir)
    ///
    /// Open (or resume) a local copy directory at `base_dir` and read this
    /// client's mirrored views through it. The directory is this client's alone —
    /// **one store per directory**, in this process or any other, and the engine's
    /// own lock on it refuses a second.
    ///
    /// A second call on the same client is refused, naming the path it holds.
    /// `close_mirror()` releases one; so does closing the client.
    pub fn mirror_at(&mut self, py: Python<'_>, base_dir: &str) -> PyResult<()> {
        let base_dir = base_dir.to_string();
        self.call(py, move |c| c.attach_mirror(Mirror::open(&base_dir)?))
    }

    /// mirror_view(schema_name, name) -> PollResult
    ///
    /// Register the view and bring its copy up to date. Idempotent, and the same
    /// call whether this is a first registration or a reopen: the result says
    /// which it was.
    ///
    /// Only a view with a delta feed can be mirrored — create it
    /// `WITH (delta = '<size>')`.
    ///
    /// **A mirrored read answers at the last poll**, not at what the server holds
    /// now: not read-your-own-writes, and two mirrored views are no consistent
    /// cut.
    pub fn mirror_view(&mut self, py: Python<'_>, schema_name: &str, name: &str) -> PyResult<Py<PyPollResult>> {
        let outcome = self.call(py, |c| c.mirror_view(schema_name, name))?;
        Py::new(py, PyPollResult::from(outcome))
    }

    /// forget_view(view_id)
    ///
    /// Stop mirroring the relation: the copy and its directory go, and a later
    /// read of it is delegated upstream.
    pub fn forget_view(&mut self, py: Python<'_>, view_id: u64) -> PyResult<()> {
        self.call(py, |c| c.forget_view(view_id))
    }

    /// poll() -> list[PollResult]
    ///
    /// Advance every registered view by one poll each, and report what each one
    /// did — **one entry per view, whatever happened to it**. A view that failed
    /// carries its message in `error`; the others went on.
    ///
    /// A poll drives no tick server-side, so a drain is "read the view against
    /// the server, then poll once".
    ///
    /// It raises only for a failure of the call rather than of a view: no store
    /// attached, a poisoned one, and a `KeyboardInterrupt`, which stops at the
    /// view it interrupted.
    pub fn poll(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyPollResult>>> {
        let outcomes = self.call(py, GnitzClient::poll_mirror)?;
        outcomes
            .into_iter()
            .map(|o| Py::new(py, PyPollResult::from(o)))
            .collect()
    }

    /// Make every copy and its cursor durable.
    ///
    /// A failure is reported, not fatal: the flush writes shards and publishes
    /// manifests, neither of which mutates what a copy holds, so the store stays
    /// usable and a retry is sound.
    pub fn checkpoint(&mut self, py: Python<'_>) -> PyResult<()> {
        self.call(py, GnitzClient::checkpoint_mirror)
    }

    /// close_mirror()
    ///
    /// Checkpoint (unless poisoned) and release the store, reporting the final
    /// checkpoint rather than letting the destructor swallow it. The connection
    /// stays open and `mirror_at` may be called again.
    ///
    /// It is also the **only** recovery from a poisoned copy, which refuses every
    /// call that touches it and cannot be cleared.
    pub fn close_mirror(&mut self, py: Python<'_>) -> PyResult<()> {
        self.call(py, GnitzClient::close_mirror)
    }

    /// Whether a read of `view_id` is answered locally. Answers on a poisoned
    /// store.
    pub fn mirrors(&mut self, view_id: u64) -> PyResult<bool> {
        Ok(self.live()?.mirrors(view_id))
    }

    /// Every registration this client holds, whether or not the copy behind it is
    /// valid — wider than `mirrors` by the ones a poll has yet to seed.
    pub fn mirrored_ids(&mut self) -> PyResult<Vec<u64>> {
        Ok(self.live()?.mirrored_ids())
    }

    /// cursor(view_id) -> (tag, tick) | None
    ///
    /// The round a local read of `view_id` answers at, or `None` when there is no
    /// valid copy to read one off.
    ///
    /// The tick is the master's global round counter, shared by every relation,
    /// so it advances over rounds that carried this view nothing. Whether a copy
    /// changed is `PollResult.reseeded`, not this.
    pub fn cursor(&mut self, py: Python<'_>, view_id: u64) -> PyResult<Py<PyAny>> {
        match self.live()?.cursor_of(view_id) {
            None => Ok(py.None()),
            Some(DeltaCursor { tag, tick }) => Ok(PyTuple::new(py, [tag, tick])?.into_any().unbind()),
        }
    }

    /// The message that poisoned this client's copy, or `None`. Answers on a
    /// poisoned store — diagnosing one is what it is for.
    #[getter]
    pub fn mirror_poisoned(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.live()?.mirror_poisoned() {
            None => Ok(py.None()),
            Some(m) => Ok(m.into_pyobject(py)?.into_any().unbind()),
        }
    }

    /// reconnect(target)
    ///
    /// Replace the connection, keeping every mirrored copy. What a host does
    /// after a server restart: the restart kills the socket, while the copies —
    /// durable, and resumable — survive it. Refused inside a transaction.
    ///
    /// Every cursor is dropped, so a read between the reconnect and the next poll
    /// goes upstream; the poll that follows reports a reseed for every view. A
    /// client that mirrors nothing gains the same recovery.
    pub fn reconnect(&mut self, py: Python<'_>, target: &str) -> PyResult<()> {
        self.call(py, |c| c.reconnect(target))?;
        // The park hook rides along with the reconnect, so a Ctrl-C still
        // interrupts; nothing is reinstalled here.
        Ok(())
    }
}

/// One `SqlResult` per statement as the list of dicts every SQL entry point
/// hands back — the client's and the mirror's alike, so a statement run through
/// either comes back in the same shape.
pub(crate) fn sql_results_to_py(py: Python<'_>, results: Vec<SqlResult>) -> PyResult<Py<PyAny>> {
    let py_list = PyList::empty(py);
    // Interned keys: `r["type"]` on the Python side hits on pointer identity
    // against its own source literal, where a `PyUnicode` freshly built per
    // key per result makes every lookup a string compare.
    let k_type = pyo3::intern!(py, "type");
    for r in results {
        let d = PyDict::new(py);
        match r {
            SqlResult::TableCreated { table_id } => {
                d.set_item(k_type, "TableCreated")?;
                d.set_item(pyo3::intern!(py, "table_id"), table_id)?;
            }
            SqlResult::ViewCreated { view_id } => {
                d.set_item(k_type, "ViewCreated")?;
                d.set_item(pyo3::intern!(py, "view_id"), view_id)?;
            }
            SqlResult::IndexCreated { index_id } => {
                d.set_item(k_type, "IndexCreated")?;
                d.set_item(pyo3::intern!(py, "index_id"), index_id)?;
            }
            SqlResult::Dropped => {
                d.set_item(k_type, "Dropped")?;
            }
            SqlResult::Altered { object, name } => {
                d.set_item(k_type, "Altered")?;
                d.set_item(pyo3::intern!(py, "object"), object)?;
                d.set_item(pyo3::intern!(py, "name"), name)?;
            }
            SqlResult::RowsAffected { count } => {
                d.set_item(k_type, "RowsAffected")?;
                d.set_item(pyo3::intern!(py, "count"), count)?;
            }
            SqlResult::Rows { schema, batch } => {
                d.set_item(k_type, "Rows")?;
                d.set_item(
                    pyo3::intern!(py, "rows"),
                    batch_to_lazy(py, Some(Arc::new(schema)), Some(batch), None, false)?,
                )?;
            }
            SqlResult::TransactionStarted => {
                d.set_item(k_type, "TransactionStarted")?;
            }
            SqlResult::TransactionCommitted { lsn } => {
                d.set_item(k_type, "TransactionCommitted")?;
                d.set_item(pyo3::intern!(py, "lsn"), lsn)?;
            }
            SqlResult::TransactionRolledBack => {
                d.set_item(k_type, "TransactionRolledBack")?;
            }
        }
        py_list.append(d)?;
    }
    Ok(py_list.into_any().unbind())
}

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
        exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
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
    /// Run `f` against the transaction's client, once the transaction is known
    /// to still be open. The call itself goes through the client's own
    /// [`PyGnitzClient::call`], so a buffered write and a `txn_commit` release
    /// the GIL and classify their errors exactly as the non-transactional
    /// writes do.
    fn with_client(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut GnitzClient) -> Result<(), ClientError> + Send,
    ) -> PyResult<()> {
        if !self.open {
            return Err(GnitzError::new_err("transaction already committed or discarded"));
        }
        self.client.bind(py).borrow_mut().call(py, f)
    }
}

/// Build a `PkTuple` for the wire-only `seek` paths, which have no schema at the
/// FFI boundary: `bytes` is taken verbatim, and an integer becomes a narrow
/// 16-byte tuple whose high padding is inert (the server reads only the
/// column's own stride). The signed fallback keeps a negative key packing to the
/// same two's-complement bytes the typed append path writes.
fn pk_tuple_from_py(pk: &Bound<'_, PyAny>) -> PyResult<gnitz_core::PkTuple> {
    // bytes first: `extract_uuid_or_u128` below falls through to
    // `getattr("int")`, which a bytes key would walk before failing.
    if let Ok(bytes) = pk.cast::<pyo3::types::PyBytes>() {
        return gnitz_core::PkTuple::try_from_bytes(bytes.as_bytes()).map_err(pyo3::exceptions::PyValueError::new_err);
    }
    if let Ok(val) = extract_uuid_or_u128(pk) {
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
// AsyncTransport — the asyncio executor over the connection spine
// ---------------------------------------------------------------------------
//
// Rust owns the session, the slots and what the loop is watching for; Python
// owns the loop itself. The callbacks run *on* the loop thread, so futures
// resolve directly: no thread, no channel, no cross-thread wake.

/// One outstanding operation. `include_hidden` is how the *result* is surfaced,
/// not part of the request, so it travels with the future.
struct Outstanding {
    future: Py<PyAny>,
    include_hidden: bool,
}

#[pyclass(name = "AsyncTransport")]
struct PyAsyncTransport {
    session: gnitz_core::Session,
    /// `event_loop.create_future`, bound once — the submit path calls it per
    /// operation, and resolving the attribute by name each time would build
    /// its name string every call.
    create_future: Py<PyAny>,
    slots: std::collections::HashMap<gnitz_core::SlotId, Outstanding>,
    client_id: u64,
    /// Set by `close` and by a failed step; every later step is a no-op.
    closed: bool,
    /// The loop this transport is registered on, and the fd it is registered
    /// under. Rust owns the registration, so nothing above it can hold a second
    /// copy of what is armed.
    event_loop: Py<PyAny>,
    fd: i32,
    /// Exactly "the loop owes us an `on_writable`".
    writer_armed: bool,
    /// The two bound methods the loop holds, built by [`Self::install`] because
    /// `arm` has only a `&mut self` to build one from. They point back at this
    /// transport, so [`Self::shutdown`] drops them rather than leaving a cycle
    /// for the collector.
    on_readable: Option<Py<PyAny>>,
    on_writable: Option<Py<PyAny>>,
}

/// The Python value one reply resolves its future to. The spine already
/// narrowed it against the request, so nothing here needs the session.
fn narrow(py: Python<'_>, reply: gnitz_core::Reply, include_hidden: bool) -> PyResult<Py<PyAny>> {
    match reply {
        gnitz_core::Reply::Lsn(lsn) => Ok(lsn.into_pyobject(py)?.into_any().unbind()),
        gnitz_core::Reply::Scan(r) => Ok(triple_to_lazy(py, r, include_hidden)?.into_any()),
        // One PyScanResult per relation, in request order → a Python list,
        // resolving the single scan_many future.
        gnitz_core::Reply::Multi(replies) => {
            let per_rel = replies.into_iter().map(|r| triple_to_lazy(py, r, include_hidden));
            Ok(build_pylist(py, per_rel)?.into_any().unbind())
        }
        _ => unreachable!("this transport submits no verb with another reply shape"),
    }
}

/// Resolve one loop future, skipping one already `done()` — a cancelled future
/// refuses a result.
fn settle(py: Python<'_>, future: &Py<PyAny>, value: PyResult<Py<PyAny>>) {
    let _ = (|| -> PyResult<()> {
        let bound = future.bind(py);
        if bound.call_method0(pyo3::intern!(py, "done"))?.is_truthy()? {
            return Ok(());
        }
        match value {
            Ok(v) => bound.call_method1(pyo3::intern!(py, "set_result"), (v,))?,
            Err(e) => bound.call_method1(pyo3::intern!(py, "set_exception"), (e.into_value(py),))?,
        };
        Ok(())
    })();
}

impl PyAsyncTransport {
    /// Register a submitted slot against a fresh loop future, and ask the loop
    /// for a writer — a submitted frame is only queued, and deferring the flush
    /// to that callback keeps a whole turn's submits in one `writev`. Built
    /// after the submit, so a request the spine refuses raises where the caller
    /// made it.
    fn register(&mut self, py: Python<'_>, slot: gnitz_core::SlotId, include_hidden: bool) -> PyResult<Py<PyAny>> {
        let future = self.create_future.call0(py)?;
        self.slots.insert(
            slot,
            Outstanding {
                future: future.clone_ref(py),
                include_hidden,
            },
        );
        self.arm(py, true)?;
        Ok(future)
    }

    /// Add or remove the loop's writable callback, idempotently. Disarming
    /// matters as much as arming: a writer left on an always-writable fd spins
    /// the loop at 100%.
    fn arm(&mut self, py: Python<'_>, on: bool) -> PyResult<()> {
        if self.closed || on == self.writer_armed {
            return Ok(());
        }
        let event_loop = self.event_loop.bind(py);
        match (on, &self.on_writable) {
            (true, Some(cb)) => {
                event_loop.call_method1(pyo3::intern!(py, "add_writer"), (self.fd, cb))?;
            }
            (true, None) => return Ok(()), // never installed: nothing to arm
            (false, _) => {
                event_loop.call_method1(pyo3::intern!(py, "remove_writer"), (self.fd,))?;
            }
        }
        self.writer_armed = on;
        Ok(())
    }

    /// Step the spine for what the loop says the fd will accept, resolve every
    /// slot that completed, and reconcile what the loop is watching for.
    fn drive(&mut self, py: Python<'_>, ready: gnitz_core::Interest) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        let session = &mut self.session;
        let stepped = py.detach(|| session.step(ready));
        let done = match stepped {
            Ok(d) => d,
            Err(e) => {
                self.shutdown(py, e);
                return Ok(());
            }
        };
        for (slot, result) in done {
            self.resolve(py, slot, result);
        }
        // Asked after every step, not only a write one: on TLS a read can
        // queue ciphertext too.
        let want = self.session.interest().write;
        self.arm(py, want)
    }

    fn resolve(&mut self, py: Python<'_>, slot: gnitz_core::SlotId, result: Result<gnitz_core::Reply, ClientError>) {
        let Some(o) = self.slots.remove(&slot) else {
            return;
        };
        let value = match result {
            Ok(reply) => narrow(py, reply, o.include_hidden),
            Err(e) => Err(client_err(e)),
        };
        settle(py, &o.future, value);
    }

    /// Deregister both callbacks and drop them, so the selector never holds a
    /// dead fd and the transport stops pointing at itself.
    fn deregister(&mut self, py: Python<'_>) {
        let event_loop = self.event_loop.bind(py);
        // A loop that is already closed has nothing left to deregister, which is
        // the only way either of these fails.
        if std::mem::take(&mut self.writer_armed) {
            let _ = event_loop.call_method1(pyo3::intern!(py, "remove_writer"), (self.fd,));
        }
        if self.on_readable.take().is_some() {
            let _ = event_loop.call_method1(pyo3::intern!(py, "remove_reader"), (self.fd,));
        }
        self.on_writable = None;
    }

    /// Abandon everything with `cause` and refuse further work. One `PyErr` for
    /// every slot: `ClientError` is not `Clone`, and N slots want N references
    /// to one failure, not N constructions of it.
    fn shutdown(&mut self, py: Python<'_>, cause: ClientError) {
        self.closed = true;
        self.deregister(py);
        self.session.close();
        let err = client_err(cause);
        for (_, o) in std::mem::take(&mut self.slots) {
            settle(py, &o.future, Err(err.clone_ref(py)));
        }
    }
}

#[pymethods]
impl PyAsyncTransport {
    #[new]
    fn new(py: Python<'_>, socket_path: &str, event_loop: Py<PyAny>) -> PyResult<Self> {
        // Connect + HELLO run on the calling (loop) thread, GIL dropped across
        // the blocking syscalls. A bare `Session`, not a `GnitzClient`: no OCC
        // basis to track, so the HELLO ACK's `published_lsn` is discarded.
        let (session, _published_lsn) = to_py_err(py.detach(|| gnitz_core::Session::connect(socket_path)))?;
        let client_id = session.client_id;
        let fd = session.as_raw_fd();
        let create_future = event_loop.getattr(py, "create_future")?;
        Ok(PyAsyncTransport {
            session,
            create_future,
            slots: std::collections::HashMap::new(),
            client_id,
            closed: false,
            event_loop,
            fd,
            writer_armed: false,
            on_readable: None,
            on_writable: None,
        })
    }

    /// Bind the two loop callbacks and register the reader — a second step
    /// because binding them needs a `Bound<Self>`, which `#[new]` cannot
    /// produce. The reader stays armed for the connection's life: on a quiet
    /// socket it costs nothing, where arming per operation costs two
    /// `epoll_ctl`.
    fn install(slf: Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let on_readable = slf.getattr(pyo3::intern!(py, "on_readable"))?.unbind();
        let on_writable = slf.getattr(pyo3::intern!(py, "on_writable"))?.unbind();
        let (event_loop, fd) = {
            let t = slf.borrow();
            (t.event_loop.clone_ref(py), t.fd)
        };
        event_loop
            .bind(py)
            .call_method1(pyo3::intern!(py, "add_reader"), (fd, &on_readable))?;
        let mut t = slf.borrow_mut();
        t.on_readable = Some(on_readable);
        t.on_writable = Some(on_writable);
        Ok(())
    }

    fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>) -> PyResult<Py<PyAny>> {
        // `submit` packs warm against the session's own cache; a stale stamp's
        // mismatch fails this slot and the caller re-issues. No await here to
        // back-pressure on, so at the cap ship what the socket will take;
        // `submit` below raises only if that was not enough.
        if self.session.queued_bytes() >= gnitz_core::MAX_QUEUED_BYTES {
            self.drive(py, gnitz_core::Interest::WRITE)?;
        }
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        let session = &mut self.session;
        let slot = py.detach(|| {
            session.submit(gnitz_core::Request::Push {
                target_id,
                schema,
                batch: b,
                mode: WireConflictMode::Update,
            })
        });
        let slot = to_py_err(slot)?;
        self.register(py, slot, false)
    }

    #[pyo3(signature = (target_id, include_hidden = false))]
    fn scan(&mut self, py: Python<'_>, target_id: u64, include_hidden: bool) -> PyResult<Py<PyAny>> {
        let slot = to_py_err(self.session.submit(gnitz_core::Request::scan(target_id)))?;
        self.register(py, slot, include_hidden)
    }

    /// scan_many(target_ids, include_hidden=False) -> awaitable[list[ScanResult]]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, resolved
    /// as a list in request order. A malformed list (empty, over-cap, duplicate
    /// tid) is rejected before any frame is written, and raises here.
    #[pyo3(signature = (target_ids, include_hidden = false))]
    fn scan_many(&mut self, py: Python<'_>, target_ids: Vec<u64>, include_hidden: bool) -> PyResult<Py<PyAny>> {
        let slot = to_py_err(self.session.submit(gnitz_core::Request::ScanMulti(&target_ids)))?;
        self.register(py, slot, include_hidden)
    }

    #[pyo3(signature = (target_id, pk, include_hidden = false))]
    fn seek(
        &mut self,
        py: Python<'_>,
        target_id: u64,
        pk: Bound<'_, PyAny>,
        include_hidden: bool,
    ) -> PyResult<Py<PyAny>> {
        let t = pk_tuple_from_py(&pk)?;
        let slot = to_py_err(self.session.submit(gnitz_core::Request::seek(target_id, &t)))?;
        self.register(py, slot, include_hidden)
    }

    /// One pyo3 crossing per readable event. It flushes as well as reads, so a
    /// submit a coroutine made earlier in this same loop turn ships here rather
    /// than arming a writer for one turn.
    fn on_readable(&mut self, py: Python<'_>) -> PyResult<()> {
        self.drive(
            py,
            gnitz_core::Interest {
                read: true,
                write: true,
            },
        )
    }

    /// The writer callback: `step(WRITE)`.
    fn on_writable(&mut self, py: Python<'_>) -> PyResult<()> {
        self.drive(py, gnitz_core::Interest::WRITE)
    }

    #[getter]
    fn client_id(&self) -> u64 {
        self.client_id
    }

    fn close(&mut self, py: Python<'_>) {
        self.shutdown(py, ClientError::ServerError("connection closed".into()));
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

/// The reply schema of an incremental delta poll, derived from a view's own
/// schema: a `_tick` U64 key column, then the view's PK columns in PK order,
/// then its payload columns in schema order. Delegates to the shared
/// `gnitz_core` builder, which mirrors what the engine derives for the delta
/// store, so the Python side cannot drift from it.
///
/// A bootstrap read is **not** in this shape — it walks the view's own store and
/// comes back in the view's own schema.
#[pyfunction]
fn delta_reply_schema(py: Python<'_>, view_schema: &Bound<'_, PyAny>) -> PyResult<Py<PySchema>> {
    let rust = Arc::clone(&resolve_py_schema(view_schema)?.borrow().rust);
    rust_schema_to_py(py, &Arc::new(gnitz_core::delta_reply_schema(&rust)))
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

// pyo3 0.29 makes free-threading opt-*out*: a bare `#[pymodule]` emits
// `Py_MOD_GIL_NOT_USED`. This module holds an `unsendable` pyclass and links the
// `Rc`-based engine, so that claim would be false.
#[pymodule(gil_used = true)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyColumnDef>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyRow>()?;
    m.add_class::<PyZSetBatch>()?;
    install_append_method(m.py())?;
    m.add_class::<PyScanResult>()?;
    m.add_class::<PyRowIterator>()?;
    m.add_class::<PyGnitzClient>()?;
    m.add_class::<PyTxn>()?;
    m.add_class::<PyAsyncTransport>()?;
    m.add_class::<PyPollResult>()?;
    m.add("GnitzError", m.py().get_type::<GnitzError>())?;
    m.add("GnitzConflictError", m.py().get_type::<GnitzConflictError>())?;
    m.add("GnitzDeltaExpiredError", m.py().get_type::<GnitzDeltaExpiredError>())?;
    m.add(
        "GnitzMirrorPoisonedError",
        m.py().get_type::<GnitzMirrorPoisonedError>(),
    )?;
    // System-table IDs — single-sourced from gnitz_wire (delegating codec, not
    // a re-typed copy), as is the column-type table behind `type_codes()`.
    // Only the ids something addresses a relation by are exported.
    m.add("TABLE_TAB", gnitz_wire::TABLE_TAB)?;
    m.add("IDX_TAB", gnitz_wire::IDX_TAB)?;
    m.add("FIRST_USER_TABLE_ID", gnitz_wire::FIRST_USER_TABLE_ID)?;
    m.add_class::<PyDeltaReply>()?;
    m.add_function(wrap_pyfunction!(delta_reply_schema, m)?)?;
    m.add_function(wrap_pyfunction!(unpack_pk_cols, m)?)?;
    m.add_function(wrap_pyfunction!(type_codes, m)?)?;
    Ok(())
}
