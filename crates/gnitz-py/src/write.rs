//! The Python→wire direction: the `ZSetBatch` pyclass, its two append
//! surfaces, and every encode from a Python object to wire bytes.
//!
//! The per-cell encode table is private: what leaves this module is the key- and
//! column-level entry points the client's own verbs call. `ZSetBatch.rows()`
//! reads back through `read::scan_result` — a pyclass crosses the directional
//! pair, no codec does.

use std::ffi::CStr;
use std::sync::Arc;

use pyo3::ffi;
use pyo3::impl_::extract_argument::argument_extraction_error;
use pyo3::prelude::*;
use pyo3::types::{PyDate, PyDateAccess, PyDateTime, PyDict, PyString, PyTimeAccess, PyTuple, PyTzInfoAccess};
use pyo3::Borrowed;

use gnitz_core::{ColType, PkColumn, ScanReply, Schema, TypeCode, ZSetBatch};
use gnitz_wire::decimal::{decimal_of_f64, parse_decimal, rescale};

use crate::read::{scan_result, PyScanResult};
use crate::schema::resolve_py_schema;

/// Encode the Python PK values `pks` into a `PkColumn` for `schema`: a
/// single-column key is its value, a compound key a tuple of per-column values
/// in PK order. Shared by `PyGnitzClient::delete` and `PyTxn::delete`; every
/// column runs through the same encoder the append path uses.
pub(crate) fn py_pks_to_column(schema: &Schema, pks: &[Bound<'_, PyAny>]) -> PyResult<PkColumn> {
    let mut pk_col = PkColumn::empty_for_schema(schema);
    pk_col.reserve(pks.len());
    let mut native = Vec::with_capacity(schema.pk_stride());
    for pk_val in pks {
        native.clear();
        match schema.pk_index_single() {
            Some(ci) => push_pk_col(&mut native, schema, ci as usize, pk_val)?,
            None => {
                let tuple = pk_val.cast::<PyTuple>().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err("a compound pk must be a tuple of its column values")
                })?;
                if tuple.len() != schema.pk_count() {
                    return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                        "a compound pk takes {} values, got {}",
                        schema.pk_count(),
                        tuple.len()
                    )));
                }
                for (&ci, v) in schema.pk_cols.iter().zip(tuple.as_slice()) {
                    push_pk_col(&mut native, schema, ci as usize, v)?;
                }
            }
        }
        pk_col.push_bytes(schema, &native);
    }
    Ok(pk_col)
}

/// Append PK column `ci`'s value `v` to `native`, refusing `None`.
fn push_pk_col(native: &mut Vec<u8>, schema: &Schema, ci: usize, v: &Bound<'_, PyAny>) -> PyResult<()> {
    let col = &schema.columns[ci];
    if v.is_none() {
        return Err(not_nullable_err(&col.name));
    }
    push_fixed_le(native, col.ty(), v)
}

/// Stores batch data in Rust Vecs with a cached Schema. `append` / `extend`
/// handle all type extraction, null tracking, and PK handling in Rust, so
/// `push()` reuses the cached schema and batch without any Python→Rust
/// re-extraction.
#[pyclass(name = "ZSetBatch")]
pub struct PyZSetBatch {
    pub(crate) schema: Arc<Schema>,
    pub(crate) batch: ZSetBatch,
    /// Is [`WEIGHT_KW`] the name of a visible column? Then it means that column,
    /// not the row weight: the schema is the authority on what a name means, and
    /// `extend`'s own `_weight` parameter still sets the weight for such a batch.
    weight_is_column: bool,
    /// The plan the last row was written through — see [`KwPlan`].
    kw_plan: Option<KwPlan>,
    /// One row's PK columns packed native-LE, reused so the per-row append
    /// allocates nothing.
    pk_scratch: Vec<u8>,
}

impl PyZSetBatch {
    /// Run `body` against `self`; on error, truncate every per-row vector back
    /// to the pre-call row count, so a half-written row never leaves the batch
    /// with mismatched column lengths. Wrap once per surface — `append` its one
    /// row, `extend` its whole loop — and never nested.
    fn with_rollback<F>(&mut self, body: F) -> PyResult<()>
    where
        F: FnOnce(&mut Self) -> PyResult<()>,
    {
        let mark = self.batch.mark();
        match body(self) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.batch.rollback_to(mark);
                Err(e)
            }
        }
    }
}

/// Append one non-null payload cell of type `tc`, spilling a German string into
/// `blob`. STRING extracts a `str` and BLOB arbitrary bytes: the region carries
/// both alike, so this extraction is where TEXT stays valid UTF-8.
fn push_column_value(col: &mut Vec<u8>, blob: &mut Vec<u8>, ty: ColType, val: &Bound<'_, PyAny>) -> PyResult<()> {
    match ty.tc {
        TypeCode::String => {
            let s = val.extract::<String>()?;
            col.extend_from_slice(&gnitz_wire::encode_german_string(s.as_bytes(), blob));
        }
        TypeCode::Blob => {
            let b = val.extract::<Vec<u8>>()?;
            col.extend_from_slice(&gnitz_wire::encode_german_string(&b, blob));
        }
        _ => push_fixed_le(col, ty, val)?,
    }
    Ok(())
}

/// A PK column had no value supplied.
#[cold]
fn missing_pk_err(schema: &Schema, ci: usize) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("missing PK column {:?}", schema.columns[ci].name))
}

/// `None` reached a NOT NULL column — every PK column is one.
#[cold]
fn not_nullable_err(name: &str) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("Non-nullable column {name:?} cannot be None"))
}

/// The `TypeError` for a supplied name that matches no column.
#[cold]
fn unexpected_name_err(name: &str) -> PyErr {
    pyo3::exceptions::PyTypeError::new_err(format!(
        "ZSetBatch got an unexpected column name '{name}' (the row weight is spelled '{WEIGHT_KW}')"
    ))
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
/// A batch caches one. A receiver that alternates shapes rebuilds per row —
/// correct, at roughly twice the cost of a row that hits the cache.
struct KwPlan {
    kwnames: Py<PyTuple>,
    /// Argument position of [`WEIGHT_KW`], if the call passed it.
    weight: Option<usize>,
    /// One entry per PK column, in PK order — the order their bytes are
    /// appended to the batch's PK buffer.
    pks: Vec<PkPlan>,
    /// One entry per payload column, by dense payload index.
    payload: Vec<PayloadPlan>,
}

/// A PK column of the plan. Carries the type code so the row loop reads the
/// schema only to name a column in an error.
#[derive(Clone, Copy)]
struct PkPlan {
    pos: usize,
    ci: usize,
    ty: ColType,
}

/// A payload column of the plan, with what the row loop needs of its
/// definition.
#[derive(Clone, Copy)]
struct PayloadPlan {
    ci: usize,
    ty: ColType,
    nullable: bool,
    src: PayloadSrc,
}

/// Where one payload slot's value comes from, for a call of this shape.
#[derive(Clone, Copy)]
enum PayloadSrc {
    /// A DROP COLUMN tombstone: the type's zero filler, never a value.
    Filler,
    /// The keyword at this argument position.
    Arg(usize),
    /// No keyword supplied it.
    Absent,
}

/// Is `plan` the one resolved from this call's names? A call site with its own
/// keyword tuple is tried by pointer alone: CPython holds a literal call site's
/// name tuple as a code-object constant, so the same object comes back every
/// iteration and one compare settles the hot path. Otherwise the names are
/// compared in order — interned names settle on pointer identity, the text
/// compare is the fallback for names that were not interned.
fn plan_hits(py: Python<'_>, plan: &KwPlan, names: &[Bound<'_, PyAny>], tuple: Option<&Bound<'_, PyTuple>>) -> bool {
    if let Some(t) = tuple {
        if plan.kwnames.as_ptr() == t.as_ptr() {
            return true;
        }
    }
    let mine = plan.kwnames.bind(py).as_slice();
    mine.len() == names.len()
        && mine.iter().zip(names).all(|(x, y)| {
            x.is(y)
                || match (x.cast::<PyString>(), y.cast::<PyString>()) {
                    (Ok(xs), Ok(ys)) => matches!((xs.to_str(), ys.to_str()), (Ok(a), Ok(b)) if a == b),
                    _ => false,
                }
        })
}

/// First position in `names` holding `col`.
fn kw_position(names: &[&Bound<'_, PyString>], col: &str) -> Option<usize> {
    names.iter().position(|n| n.to_str().is_ok_and(|s| s == col))
}

/// Resolve a name tuple into a write plan. Cold: once per call shape.
///
/// Walks the *schema*, not the name list, so a name held by two columns feeds
/// both — `Schema` admits duplicate names, since hidden columns are exempt from
/// the duplicate-name check. A hidden payload column is a DROP COLUMN
/// tombstone, planned as [`PayloadSrc::Filler`] and never named by a keyword —
/// so one spelling it falls through to [`unexpected_name_err`].
fn build_kw_plan(schema: &Schema, weight_is_column: bool, kwnames: &Bound<'_, PyTuple>) -> PyResult<KwPlan> {
    let mut names: Vec<&Bound<'_, PyString>> = Vec::with_capacity(kwnames.len());
    for item in kwnames.as_slice() {
        names.push(
            item.cast::<PyString>()
                .map_err(|_| pyo3::exceptions::PyTypeError::new_err("column names must be strings"))?,
        );
    }

    let mut consumed = vec![false; names.len()];
    let weight = if weight_is_column {
        None
    } else {
        kw_position(&names, WEIGHT_KW)
    };
    if let Some(i) = weight {
        consumed[i] = true;
    }
    let mut pks = Vec::with_capacity(schema.pk_cols.len());
    for &ci in &schema.pk_cols {
        let ci = ci as usize;
        let Some(i) = kw_position(&names, &schema.columns[ci].name) else {
            return Err(missing_pk_err(schema, ci));
        };
        consumed[i] = true;
        pks.push(PkPlan { pos: i, ci, ty: schema.columns[ci].ty() });
    }
    let mut payload = Vec::with_capacity(names.len());
    for (_, ci, col) in schema.payload_columns() {
        let src = if col.is_hidden {
            PayloadSrc::Filler
        } else if let Some(i) = kw_position(&names, &col.name) {
            consumed[i] = true;
            PayloadSrc::Arg(i)
        } else {
            PayloadSrc::Absent
        };
        payload.push(PayloadPlan {
            ci,
            ty: col.ty(),
            nullable: col.is_nullable,
            src,
        });
    }
    if let Some(i) = consumed.iter().position(|c| !c) {
        return Err(unexpected_name_err(names[i].to_str()?));
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
    ///
    /// PK bytes go straight into the batch's PK buffer, then each payload cell
    /// into its column, and the weight and null word close the row. Rolls
    /// nothing back on error: each surface wraps its own call in
    /// [`Self::with_rollback`], which cuts a half-written row off every stream.
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
            weight_is_column,
            kw_plan,
            pk_scratch,
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
        // Into the reused scratch first: a failed extraction must leave no
        // half-written key behind.
        pk_scratch.clear();
        for &PkPlan { pos, ci, ty } in &plan.pks {
            let v = arg(pos);
            if v.is_none() {
                return Err(not_nullable_err(&schema.columns[ci].name));
            }
            push_fixed_le(pk_scratch, ty, &v)?;
        }
        batch.pks.push_bytes(schema, pk_scratch);
        let mut nulls = 0u64;
        // `enumerate`, because the dense payload index is the null-bitmap bit
        // position.
        for (payload_idx, &PayloadPlan { ci, ty, nullable, src }) in plan.payload.iter().enumerate() {
            let v = match src {
                PayloadSrc::Filler => {
                    batch.payload[payload_idx].push_zero();
                    continue;
                }
                PayloadSrc::Arg(i) => Some(arg(i)),
                PayloadSrc::Absent => None,
            };
            match v {
                Some(v) if !v.is_none() => {
                    let ZSetBatch { payload: cols, blob, .. } = &mut *batch;
                    push_column_value(&mut cols[payload_idx].bytes, blob, ty, &v)?
                }
                _ => {
                    if !nullable {
                        return Err(not_nullable_err(&schema.columns[ci].name));
                    }
                    gnitz_wire::null_word_set(&mut nulls, payload_idx, true);
                    batch.payload[payload_idx].push_zero();
                }
            }
        }
        batch.weights.push(weight);
        batch.nulls.push(nulls);
        debug_assert_eq!(batch.pks.len(), batch.weights.len());
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
        names.clear();
        args.clear();
        let mut supplied = None;
        for (key, val) in dict.iter() {
            if !self.weight_is_column && is_weight_key(&key) {
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
                .map_err(|e| argument_extraction_error(dict.py(), WEIGHT_KW, e)),
        }
    }
}

/// Does this dict key spell [`WEIGHT_KW`]?
fn is_weight_key(key: &Bound<'_, PyAny>) -> bool {
    key.cast::<PyString>()
        .is_ok_and(|s| s.to_str().is_ok_and(|t| t == WEIGHT_KW))
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
    if nargs != 0 {
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
pub(crate) fn install_append_method(py: Python<'_>) -> PyResult<()> {
    let ty = py.get_type::<PyZSetBatch>();
    let def = &APPEND_METHOD_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef;
    let desc = unsafe { ffi::PyDescr_NewMethod(ty.as_type_ptr(), def) };
    let desc = unsafe { Bound::from_owned_ptr_or_err(py, desc)? };
    ty.setattr("append", desc)
}

#[pymethods]
impl PyZSetBatch {
    /// Construct a batch for `schema` — a `Schema` or a bare list of
    /// `ColumnDef`, resolved at the parameter through [`resolve_py_schema`].
    #[new]
    #[pyo3(signature = (schema))]
    pub fn new(#[pyo3(from_py_with = resolve_py_schema)] schema: Arc<Schema>) -> PyResult<Self> {
        let weight_is_column = schema.visible_columns().any(|(_, c)| c.name == WEIGHT_KW);
        let batch = ZSetBatch::new(&schema);
        let pk_scratch = Vec::with_capacity(schema.pk_stride());
        Ok(PyZSetBatch {
            batch,
            weight_is_column,
            kw_plan: None,
            schema,
            pk_scratch,
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
        // `try_borrow_mut`, as `append` does: a row value that re-enters this
        // batch raises instead of aborting the interpreter. Wrapping the whole
        // loop in one rollback gives `extend` the same all-or-nothing contract.
        slf.try_borrow_mut()?.with_rollback(|s| {
            // A sized sequence lets every growth stream skip its climb from
            // zero; a generator has no `__len__`, so the probe is discarded.
            if let Ok(n) = rows.len() {
                s.batch.reserve(n);
            }
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

    /// The rows appended so far, as a `ScanResult` over a copy of the batch.
    pub fn rows(&self, py: Python<'_>) -> PyResult<Py<PyScanResult>> {
        scan_result(
            py,
            ScanReply {
                schema: Arc::clone(&self.schema),
                batch: self.batch.clone(),
                lsn: None,
            },
        )
    }

    pub fn __len__(&self) -> usize {
        self.batch.len()
    }

    pub fn __repr__(&self) -> String {
        format!("ZSetBatch(len={})", self.batch.len())
    }
}

// ---------------------------------------------------------------------------
// Value encoders
// ---------------------------------------------------------------------------

/// Accept a Python int, `uuid.UUID` object (via `.int`), or string, returning
/// the 128-bit value. Int is tried first because it is the common case in
/// bulk inserts and avoids a Python attribute lookup per row.
///
/// A string is UUID text — canonical or bare 32-hex — and nothing else
/// (`gnitz_wire::parse_uuid`, the crate that owns wire-value text). Which
/// *columns* a string may be written to is not decided here: this function is
/// also reached from the schema-less wire path [`py_scalar_key`], which has no
/// type code to consult. The typed encoder
/// [`push_fixed_le`] reaches it for UUID alone.
pub(crate) fn extract_uuid_or_u128(val: &Bound<'_, PyAny>) -> PyResult<u128> {
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

/// Days since the epoch of a `datetime.date` (or the date of a `datetime`).
fn py_days(d: &impl PyDateAccess) -> i64 {
    gnitz_expr::calendar::days_from_civil(d.get_year() as i64, d.get_month() as u32, d.get_day() as u32)
}

/// A DATE value: a `datetime.date` (a `datetime.datetime` is one too, and
/// contributes its date), or the day count as an int.
fn extract_days(item: &Bound<'_, PyAny>) -> PyResult<i32> {
    match item.cast::<PyDate>() {
        Ok(d) => Ok(py_days(d) as i32),
        Err(_) => item.extract::<i32>(),
    }
}

/// A TIMESTAMP value: a naive `datetime.datetime`, a `datetime.date` at
/// midnight, or microseconds since the epoch as an int.
fn extract_micros(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    use gnitz_expr::calendar::{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MIN, MICROS_PER_SEC};
    if let Ok(dt) = item.cast::<PyDateTime>() {
        if dt.get_tzinfo().is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "a TIMESTAMP takes a naive datetime; convert to UTC and drop tzinfo",
            ));
        }
        return Ok(py_days(dt) * MICROS_PER_DAY
            + dt.get_hour() as i64 * MICROS_PER_HOUR
            + dt.get_minute() as i64 * MICROS_PER_MIN
            + dt.get_second() as i64 * MICROS_PER_SEC
            + dt.get_microsecond() as i64);
    }
    match item.cast::<PyDate>() {
        Ok(d) => Ok(py_days(d) * MICROS_PER_DAY),
        Err(_) => item.extract::<i64>(),
    }
}

/// A DECIMAL value at the column's `scale`: a `decimal.Decimal` (or anything
/// that formats as one with `format(x, 'f')`), a `str` of the same spelling, an
/// `int`, or a `float` read at the digits it prints with — a longer fraction
/// rounds half away from zero, as an SQL INSERT does.
fn extract_decimal(item: &Bound<'_, PyAny>, scale: u8) -> PyResult<i64> {
    let overflow =
        || pyo3::exceptions::PyOverflowError::new_err(format!("{item} does not fit a DECIMAL of scale {scale}"));
    if item.cast::<pyo3::types::PyInt>().is_ok() {
        return rescale(item.extract::<i128>()?, 0, scale).ok_or_else(overflow);
    }
    if let Ok(f) = item.cast::<pyo3::types::PyFloat>() {
        let (v, s) = decimal_of_f64(f.value()).ok_or_else(overflow)?;
        return rescale(v as i128, s, scale).ok_or_else(overflow);
    }
    // Borrowed on both branches: `to_cow` hands back CPython's own UTF-8 where
    // it has one, so a row costs no copy of the spelling it already holds.
    let formatted;
    let text = match item.cast::<PyString>() {
        Ok(s) => s.to_cow()?,
        Err(_) => {
            formatted = item.call_method1(pyo3::intern!(item.py(), "__format__"), ("f",))?;
            formatted.cast::<PyString>()?.to_cow()?
        }
    };
    parse_decimal(&text, scale)
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("{text:?} is not a DECIMAL of scale {scale}")))
}

/// Append one non-null fixed-width value to `buf` as its native little-endian
/// bytes — the one typed encoder, serving the PK buffer and every `Fixed`
/// payload column alike, so a key packs the same way whichever surface
/// supplied it.
///
/// Per-arm `extract` is also the range check — `extract::<u8>()` raises
/// Python's `OverflowError` for `append(c=300)` on a `U8` column, where a
/// width-generic pack would silently truncate. Text is accepted for UUID alone,
/// so a `U128` column takes an integer and nothing else; it extracts as `u128`
/// because a value above `i128::MAX` is legal there.
fn push_fixed_le(buf: &mut Vec<u8>, ty: ColType, item: &Bound<'_, PyAny>) -> PyResult<()> {
    match ty.tc {
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
        TypeCode::U128 => buf.extend_from_slice(&item.extract::<u128>()?.to_le_bytes()),
        TypeCode::I128 => buf.extend_from_slice(&item.extract::<i128>()?.to_le_bytes()),
        TypeCode::UUID => buf.extend_from_slice(&extract_uuid_or_u128(item)?.to_le_bytes()),
        TypeCode::Date => buf.extend_from_slice(&extract_days(item)?.to_le_bytes()),
        TypeCode::Timestamp => buf.extend_from_slice(&extract_micros(item)?.to_le_bytes()),
        TypeCode::Decimal => buf.extend_from_slice(&extract_decimal(item, ty.scale)?.to_le_bytes()),
        TypeCode::String | TypeCode::Blob => {
            unreachable!("a German-string column is never a Fixed column or a PK column")
        }
    }
    Ok(())
}

/// Split a Python seek key into the control block's `(seek_pk, seek_pk_extra)`
/// pair. `bytes` is packed native-LE columns; anything else is one scalar
/// ([`py_scalar_key`]).
pub(crate) fn pk_key_from_py(pk: &Bound<'_, PyAny>) -> PyResult<(u128, Vec<u8>)> {
    // bytes first: `py_scalar_key` falls through to `getattr("int")`, which a
    // bytes key would walk before failing.
    if let Ok(bytes) = pk.cast::<pyo3::types::PyBytes>() {
        let b = bytes.as_bytes();
        if b.is_empty() || b.len() > gnitz_core::MAX_PK_BYTES {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "packed pk must be 1..={} bytes, got {}",
                gnitz_core::MAX_PK_BYTES,
                b.len(),
            )));
        }
        let (low, extra) = gnitz_wire::control::split_ctrl_key(b);
        return Ok((low, extra.to_vec()));
    }
    Ok((py_scalar_key(pk)?, Vec::new()))
}

/// One scalar key value as the 16-byte native word the wire carries, of which
/// the server reads only the column's own stride — a truncation, not a range
/// check. The signed arm keeps a negative key packing to the same
/// two's-complement bytes the typed append path writes.
pub(crate) fn py_scalar_key(pk: &Bound<'_, PyAny>) -> PyResult<u128> {
    // `i128` first, so a negative key does not build and discard an
    // `OverflowError` on the unsigned arm; a `U128` key above `i128::MAX`, a
    // `uuid.UUID` and UUID text all fall to the last.
    if let Ok(val) = pk.extract::<i128>() {
        return Ok(val as u128);
    }
    // A `datetime` keys a TIMESTAMP column and a `date` a DATE one; the server
    // reads only the column's own stride, which is what tells them apart from a
    // plain integer key.
    if pk.is_instance_of::<PyDateTime>() {
        return Ok(extract_micros(pk)? as i128 as u128);
    }
    if pk.is_instance_of::<PyDate>() {
        return Ok(extract_days(pk)? as i128 as u128);
    }
    extract_uuid_or_u128(pk)
}
