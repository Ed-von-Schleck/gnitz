//! The Python→wire direction: the `ZSetBatch` pyclass, its two append
//! surfaces, and every encode from a Python object to wire bytes.
//!
//! The per-cell encode table is private: what leaves this module is the key- and
//! column-level entry points the client's own verbs call. `ZSetBatch.rows()`
//! reads back through `read::scan_result`.

use std::borrow::Cow;
use std::ffi::CStr;
use std::sync::Arc;

use pyo3::ffi;
use pyo3::impl_::extract_argument::argument_extraction_error;
use pyo3::prelude::*;
use pyo3::types::{PyDate, PyDateAccess, PyDateTime, PyDict, PyString, PyTimeAccess, PyTuple, PyTzInfoAccess};
use pyo3::Borrowed;

use gnitz_core::{PkColumn, Schema, ZSetBatch};
use gnitz_expr::place::{place_scaled, Placed};
use gnitz_expr::SchemaFacts;
use gnitz_wire::decimal::parse_decimal_text;
use gnitz_wire::{ColType, ColumnDef, FixedInt, ReadBound, ReadSpec, TypeCode, MAX_PK_COLUMNS};

use crate::read::{present, PyScanResult};
use crate::schema::PySchema;

/// The lookup keys `pks` as a `PkColumn` for `schema`: a single-column key is
/// its value, a compound key a tuple of its column values in PK order.
pub(crate) fn py_pks_to_column(schema: &Schema, pks: &[Bound<'_, PyAny>]) -> PyResult<PkColumn> {
    let mut pk_col = PkColumn::empty_for_schema(schema);
    pk_col.reserve(pks.len());
    let mut scratch = Vec::with_capacity(16);
    for pk in pks {
        let tuple;
        let vals = if schema.pk_cols.len() == 1 {
            std::slice::from_ref(pk)
        } else {
            tuple = pk.cast::<PyTuple>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err("a compound pk must be a tuple of its column values")
            })?;
            tuple.as_slice()
        };
        if vals.len() != schema.pk_cols.len() {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "a compound pk takes {} values, got {}",
                schema.pk_cols.len(),
                vals.len()
            )));
        }
        let mut natives = [0u128; MAX_PK_COLUMNS];
        for (k, (native, v)) in natives.iter_mut().zip(vals).enumerate() {
            let col = &schema.columns[schema.pk_cols[k] as usize];
            if v.is_none() {
                return Err(not_nullable_err(&col.name));
            }
            *native = py_native(&mut scratch, col.ty, v, Inexact::Refuse)?;
        }
        pk_col.push_natives(&natives[..vals.len()]);
    }
    Ok(pk_col)
}

/// A read of the rows keyed `pk` — a single-column key's value, or a compound
/// key's tuple.
pub(crate) fn pk_point_spec(schema: &Schema, pk: &Bound<'_, PyAny>) -> PyResult<ReadSpec> {
    let keys = py_pks_to_column(schema, std::slice::from_ref(pk))?.keys();
    Ok(ReadSpec::all_rows(ReadBound::PkSet(keys)))
}

/// Stores batch data in Rust Vecs with a cached Schema. `append` / `extend`
/// handle all type extraction, null tracking, and PK handling in Rust, so
/// `push()` reuses the cached schema and batch without any Python→Rust
/// re-extraction.
#[pyclass(name = "ZSetBatch")]
pub struct PyZSetBatch {
    pub(crate) schema: Arc<Schema>,
    /// Shared with a push still on its way, which sends the rows as they were
    /// at its call: a later append copies them first.
    pub(crate) batch: Arc<ZSetBatch>,
    /// Is [`WEIGHT_KW`] the name of a visible column? Then it means that column,
    /// not the row weight: the schema is the authority on what a name means, and
    /// `extend`'s own `_weight` parameter still sets the weight for such a batch.
    weight_is_column: bool,
    /// The plan the last row was written through — see [`KwPlan`].
    kw_plan: Option<KwPlan>,
    /// [`py_native`]'s buffer, reused by every row written.
    key_scratch: Vec<u8>,
}

/// A [`PyZSetBatch`] open for one surface's rows: its batch, unshared, and
/// what a row is written through.
struct Rows<'b> {
    batch: &'b mut ZSetBatch,
    schema: &'b Schema,
    weight_is_column: bool,
    kw_plan: &'b mut Option<KwPlan>,
    key_scratch: &'b mut Vec<u8>,
}

impl PyZSetBatch {
    /// Run `body` against this batch's rows; on error, truncate every per-row
    /// vector back to the pre-call row count, so a half-written row never
    /// leaves the batch with mismatched column lengths. Wrap once per surface —
    /// `append` its one row, `extend` its whole loop — and never nested.
    fn with_rollback<F>(&mut self, body: F) -> PyResult<()>
    where
        F: FnOnce(&mut Rows<'_>) -> PyResult<()>,
    {
        let mut rows = Rows {
            batch: Arc::make_mut(&mut self.batch),
            schema: &self.schema,
            weight_is_column: self.weight_is_column,
            kw_plan: &mut self.kw_plan,
            key_scratch: &mut self.key_scratch,
        };
        let mark = rows.batch.mark();
        body(&mut rows).inspect_err(|_| rows.batch.rollback_to(mark))
    }
}

/// Append one non-null payload cell of type `tc`, spilling a German string into
/// `blob`. STRING extracts a `str` and BLOB arbitrary bytes: a `str` is UTF-8 by
/// construction; the engine refuses a STRING cell that is not.
fn push_column_value(col: &mut Vec<u8>, blob: &mut Vec<u8>, ty: ColType, val: &Bound<'_, PyAny>) -> PyResult<()> {
    match ty.tc {
        TypeCode::String => {
            let s = val.cast::<PyString>()?.to_cow()?;
            col.extend_from_slice(&gnitz_wire::encode_german_string(s.as_bytes(), blob));
        }
        TypeCode::Blob => {
            let b = val.extract::<Cow<[u8]>>()?;
            col.extend_from_slice(&gnitz_wire::encode_german_string(&b, blob));
        }
        _ => push_fixed_le(col, ty, val, Inexact::Round)?,
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
struct KwPlan {
    kwnames: Py<PyTuple>,
    /// Argument position of [`WEIGHT_KW`], if the call passed it.
    weight: Option<usize>,
    /// One entry per PK column, in PK order.
    pks: Vec<PkPlan>,
    /// One entry per payload column, by dense payload index.
    payload: Vec<PayloadPlan>,
}

/// A PK column of the plan.
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
    /// A hidden column — in a base table's schema, a DROP COLUMN tombstone —
    /// takes the type's zero filler.
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
        pks.push(PkPlan { pos: i, ci, ty: schema.columns[ci].ty });
    }
    let mut payload = Vec::with_capacity(schema.num_payload_cols());
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
            ty: col.ty,
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

impl Rows<'_> {
    /// Write one row: resolve this call's shape against the cached plan, then
    /// read each column's value through `arg`, which maps a name's position in
    /// `names` to its value. `tuple` is those names as a Python tuple where the
    /// caller holds one, which the plan is matched against by pointer first.
    ///
    /// The key is appended once every PK column has extracted, then each payload
    /// cell into its column, and the weight and null word close the row. Rolls
    /// nothing back on error: each surface wraps its own call in
    /// [`PyZSetBatch::with_rollback`], which cuts a half-written row off every
    /// stream.
    fn write_row<'a, 'py>(
        &mut self,
        py: Python<'py>,
        names: &[Bound<'py, PyAny>],
        tuple: Option<&Bound<'py, PyTuple>>,
        arg: impl Fn(usize) -> Borrowed<'a, 'py, PyAny>,
        default_weight: i64,
    ) -> PyResult<()> {
        let Rows {
            batch,
            schema,
            weight_is_column,
            kw_plan,
            key_scratch,
        } = self;
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
        let mut natives = [0u128; MAX_PK_COLUMNS];
        for (native, &PkPlan { pos, ci, ty }) in natives.iter_mut().zip(&plan.pks) {
            let v = arg(pos);
            if v.is_none() {
                return Err(not_nullable_err(&schema.columns[ci].name));
            }
            *native = py_native(key_scratch, ty, &v, Inexact::Round)?;
        }
        batch.pks.push_natives(&natives[..plan.pks.len()]);
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
        // A key the cached plan names at this position needs no text compare.
        let planned = self
            .kw_plan
            .as_ref()
            .map_or(&[][..], |p| p.kwnames.bind(dict.py()).as_slice());
        for (key, val) in dict.iter() {
            let known = planned.get(names.len()).is_some_and(|n| n.is(&key));
            if !known && !self.weight_is_column && is_weight_key(&key) {
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
/// Raw for the `kwnames` tuple itself, which [`plan_hits`] matches a call site
/// by.
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
    // Chainable: `b.append(…).append(…)`.
    Ok(batch.clone().into_ptr())
}

/// CPython text signature (`name(…)\n--\n\n`). Without it the descriptor has no
/// `__doc__` and no `__text_signature__`, and `inspect.signature` fails — and
/// that signature is what the `.pyi` stub's is checked against.
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
    #[new]
    pub fn new(schema: PySchema) -> Self {
        let schema = schema.rust;
        let weight_is_column = schema.visible_columns().any(|(_, c)| c.name == WEIGHT_KW);
        let batch = Arc::new(ZSetBatch::new(&schema));
        PyZSetBatch {
            batch,
            weight_is_column,
            kw_plan: None,
            key_scratch: Vec::with_capacity(16),
            schema,
        }
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

    /// The rows appended so far, as a `ScanResult`: a snapshot, since a later
    /// append copies the batch it shares.
    pub fn rows(&self, py: Python<'_>) -> PyResult<Py<PyScanResult>> {
        Py::new(
            py,
            present(py, Arc::clone(&self.schema), Arc::clone(&self.batch), false, None)?,
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

/// A UUID value: its integer, UUID text, or a `uuid.UUID` (read through `.int`).
fn extract_uuid(val: &Bound<'_, PyAny>) -> PyResult<u128> {
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

/// An aware `datetime` carries an offset no column stores.
fn refuse_aware(dt: &Bound<'_, PyDateTime>) -> PyResult<()> {
    match dt.get_tzinfo() {
        Some(_) => Err(pyo3::exceptions::PyValueError::new_err(
            "a DATE or TIMESTAMP takes a naive datetime; convert to UTC and drop tzinfo",
        )),
        None => Ok(()),
    }
}

/// A DATE value: a `datetime.date` (a naive `datetime.datetime` is one too, and
/// contributes its date), or the day count as an int.
fn extract_days(item: &Bound<'_, PyAny>, inexact: Inexact) -> PyResult<i32> {
    let Ok(d) = item.cast::<PyDate>() else {
        return item.extract::<i32>();
    };
    // A plain `date` is no `datetime`; only a subclass pays the subtype walk.
    if !d.is_exact_instance_of::<PyDate>() {
        if let Ok(t) = item.cast::<PyDateTime>() {
            refuse_aware(t)?;
            if inexact == Inexact::Refuse
                && (t.get_hour(), t.get_minute(), t.get_second(), t.get_microsecond()) != (0, 0, 0, 0)
            {
                return Err(pyo3::exceptions::PyValueError::new_err(format!("{item} is not a DATE")));
            }
        }
    }
    Ok(py_days(d) as i32)
}

/// A TIMESTAMP value: a naive `datetime.datetime`, a `datetime.date` at
/// midnight, or microseconds since the epoch as an int.
fn extract_micros(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    use gnitz_expr::calendar::{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MIN, MICROS_PER_SEC};
    if let Ok(dt) = item.cast::<PyDateTime>() {
        refuse_aware(dt)?;
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

/// A DECIMAL value at the column's `scale`: an `int`, a `float` read at the
/// digits it prints with, or decimal text — a `str`, or what `format(x, 'f')`
/// gives, as for a `decimal.Decimal`.
fn extract_decimal(item: &Bound<'_, PyAny>, scale: u8, inexact: Inexact) -> PyResult<i64> {
    let overflow =
        || pyo3::exceptions::PyOverflowError::new_err(format!("{item} does not fit a DECIMAL of scale {scale}"));
    let not_decimal = || pyo3::exceptions::PyValueError::new_err(format!("{item} is not a DECIMAL of scale {scale}"));
    let (v, s) = if item.cast::<pyo3::types::PyInt>().is_ok() {
        (item.extract::<i128>()?, 0)
    } else if let Ok(f) = item.cast::<pyo3::types::PyFloat>() {
        parse_decimal_text(&f.value().to_string()).ok_or_else(overflow)?
    } else {
        // Borrowed on both branches: `to_cow` hands back CPython's own UTF-8
        // where it has one.
        let formatted;
        let text = match item.cast::<PyString>() {
            Ok(s) => s.to_cow()?,
            Err(_) => {
                formatted = item.call_method1(pyo3::intern!(item.py(), "__format__"), ("f",))?;
                formatted.cast::<PyString>()?.to_cow()?
            }
        };
        parse_decimal_text(&text).ok_or_else(not_decimal)?
    };
    let p = place_scaled(FixedInt::I64, v, s, scale);
    let n = match (p, inexact) {
        (Placed::At(n), _) => Some(n),
        (_, Inexact::Refuse) => None,
        (p, Inexact::Round) => p.stored(),
    };
    n.map(|n| n as i64)
        .ok_or_else(|| if p.is_outside() { overflow() } else { not_decimal() })
}

/// What becomes of a value that falls between two of its column's.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Inexact {
    /// A written cell stores the nearest one.
    Round,
    /// A lookup key names no row, and raises.
    Refuse,
}

/// Append one non-null fixed-width value to `buf` as its native little-endian
/// bytes. Each integer arm's `extract` is its range check.
fn push_fixed_le(buf: &mut Vec<u8>, ty: ColType, item: &Bound<'_, PyAny>, inexact: Inexact) -> PyResult<()> {
    match ty.tc {
        TypeCode::U8 => buf.push(item.extract::<u8>()?),
        // A `bool` only: `1` is an integer, as it is in SQL.
        TypeCode::Bool => buf.push(u8::from(item.extract::<bool>()?)),
        TypeCode::I8 => buf.push(item.extract::<i8>()? as u8),
        TypeCode::U16 => buf.extend_from_slice(&item.extract::<u16>()?.to_le_bytes()),
        TypeCode::I16 => buf.extend_from_slice(&item.extract::<i16>()?.to_le_bytes()),
        TypeCode::U32 => buf.extend_from_slice(&item.extract::<u32>()?.to_le_bytes()),
        TypeCode::I32 => buf.extend_from_slice(&item.extract::<i32>()?.to_le_bytes()),
        TypeCode::F32 => {
            let v = item.extract::<f64>()?;
            let f = gnitz_wire::narrow_f32(v).ok_or_else(|| {
                pyo3::exceptions::PyOverflowError::new_err(format!("{v} is out of range for an F32 column"))
            })?;
            buf.extend_from_slice(&f.to_le_bytes())
        }
        TypeCode::U64 => buf.extend_from_slice(&item.extract::<u64>()?.to_le_bytes()),
        TypeCode::I64 => buf.extend_from_slice(&item.extract::<i64>()?.to_le_bytes()),
        TypeCode::F64 => buf.extend_from_slice(&item.extract::<f64>()?.to_le_bytes()),
        TypeCode::U128 => buf.extend_from_slice(&item.extract::<u128>()?.to_le_bytes()),
        TypeCode::I128 => buf.extend_from_slice(&item.extract::<i128>()?.to_le_bytes()),
        TypeCode::UUID => buf.extend_from_slice(&extract_uuid(item)?.to_le_bytes()),
        TypeCode::Date => buf.extend_from_slice(&extract_days(item, inexact)?.to_le_bytes()),
        TypeCode::Timestamp => buf.extend_from_slice(&extract_micros(item)?.to_le_bytes()),
        TypeCode::Decimal => buf.extend_from_slice(&extract_decimal(item, ty.scale, inexact)?.to_le_bytes()),
        TypeCode::String | TypeCode::Blob => {
            unreachable!("a German-string column is never a fixed-width column or a PK column")
        }
    }
    Ok(())
}

/// `v` as `ty`'s native value, zero-extended. `scratch` is reused across calls.
fn py_native(scratch: &mut Vec<u8>, ty: ColType, v: &Bound<'_, PyAny>, inexact: Inexact) -> PyResult<u128> {
    scratch.clear();
    push_fixed_le(scratch, ty, v, inexact)?;
    let mut le = [0u8; 16];
    le[..scratch.len()].copy_from_slice(scratch);
    Ok(u128::from_le_bytes(le))
}

/// One key value's image in `col`'s key order.
pub(crate) fn py_key_image(col: &ColumnDef, v: &Bound<'_, PyAny>) -> PyResult<u128> {
    if !col.ty.tc.is_pk_eligible() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "column {:?} cannot be a key column",
            col.name
        )));
    }
    let native = py_native(&mut Vec::with_capacity(16), col.ty, v, Inexact::Refuse)?;
    Ok(gnitz_wire::key_image(col.ty.tc, native))
}
