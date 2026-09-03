//! The Python→wire direction: the `ZSetBatch` pyclass, its two append
//! surfaces, and every encode from a Python object to wire bytes.
//!
//! The per-cell encode table is private: what leaves this module is the key- and
//! column-level entry points the client's own verbs call. `ZSetBatch`'s
//! inspection getters read back through `read` — a pyclass crosses the
//! directional pair, no codec does.

use std::ffi::CStr;
use std::sync::Arc;

use pyo3::ffi;
use pyo3::impl_::extract_argument::argument_extraction_error;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use pyo3::Borrowed;

use gnitz_core::{null_word_set, ColData, PkColumn, Schema, TypeCode, ZSetBatch};

use crate::read::{pk_column_to_pylist, rust_batch_columns_to_py};
use crate::schema::resolve_py_schema;

/// Encode the Python PK values `pks` (each an int/UUID for a single-column key,
/// or packed `bytes`) into a `PkColumn` for `schema`. Shared by
/// `PyGnitzClient::delete` and `PyTxn::delete`; a single-column key runs through
/// the same typed encoder the append path uses, so a signed or UUID key packs
/// identically whichever surface supplied it.
pub(crate) fn py_pks_to_column(schema: &Schema, pks: &[Bound<'_, PyAny>]) -> PyResult<PkColumn> {
    let stride = schema.pk_stride();
    let mut pk_col = PkColumn::empty_for_schema(schema);
    // One scratch tuple for every key: each key rewrites the whole leading
    // `stride` bytes, which is all `push_tuple` reads.
    let mut t = gnitz_core::PkTuple::new(stride as u8);
    // A compound key never reaches the typed encoder below, so the lone column's
    // slot is the only one this loop can need.
    let single_pk = match schema.pk_indices() {
        &[ci] => Some(PkSlot::resolve(schema, ci)),
        _ => None,
    };
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
        } else if let Some(slot) = single_pk {
            slot.write(schema, &mut t, pk_val)?;
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "a compound pk must be passed as packed bytes",
            ));
        }
        pk_col.push_tuple(&t);
    }
    Ok(pk_col)
}

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

/// One PK column's address in the packed key. [`Self::resolve`] derives the
/// offset from the column index, so no caller can pair the two wrongly; it walks
/// the PK list summing wire strides, so it is resolved per schema, not per row.
#[derive(Clone, Copy)]
struct PkSlot {
    ci: usize,
    off: usize,
}

impl PkSlot {
    fn resolve(schema: &Schema, ci: usize) -> Self {
        PkSlot {
            ci,
            off: schema.pk_byte_offset(ci),
        }
    }

    /// Encode one PK column's Python value into `t` at this slot, rejecting
    /// `None`.
    fn write(self, schema: &Schema, t: &mut gnitz_core::PkTuple, val: &Bound<'_, PyAny>) -> PyResult<()> {
        if val.is_none() {
            return Err(pk_none_err(schema, self.ci));
        }
        let tc = schema.columns[self.ci].type_code;
        write_pk_bytes(&mut t.buf[self.off..self.off + tc.wire_stride()], tc, val)
    }
}

/// `None` reached a PK column.
#[cold]
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

    /// `None` — no value supplied, or an explicit `None` — writes NULL. A
    /// tombstone column never reaches here: the plan routes it to
    /// [`Self::filler`] instead. (A hidden *PK* column is a view's key slot, and
    /// is written normally.)
    fn payload(&mut self, payload_idx: usize, ci: usize, val: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        let col = &self.schema.columns[ci];
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

    /// A DROP COLUMN tombstone: the type's zero filler and no null bit — it stays
    /// NOT NULL, as the SQL `INSERT` writer also pushes it.
    fn filler(&mut self, ci: usize) {
        let tc = self.schema.columns[ci].type_code;
        self.batch.columns[ci].push_filler(tc);
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
    /// The argument position feeding each PK column, and that column's slot, in
    /// PK order.
    pks: Vec<(usize, PkSlot)>,
    /// `(column index, source)` per payload column, by dense payload index.
    payload: Vec<(usize, PayloadSrc)>,
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
/// COLUMN tombstone, planned as [`PayloadSrc::Filler`] and never named by a
/// keyword — so one spelling it falls through to [`unexpected_name_err`].
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
        pks.push((i, PkSlot::resolve(schema, ci)));
    }
    let mut payload = Vec::with_capacity(nkw);
    for (_, ci, col) in schema.payload_columns() {
        let src = if schema.is_hidden_payload(ci) {
            PayloadSrc::Filler
        } else if let Some(i) = kw_position(&names, &col.name) {
            consumed[i] = true;
            PayloadSrc::Arg(i)
        } else {
            PayloadSrc::Absent
        };
        payload.push((ci, src));
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
        for &(pos, slot) in &plan.pks {
            slot.write(schema, key_scratch, &arg(pos))?;
        }
        // `enumerate`, because the dense payload index is the null-bitmap bit
        // position.
        for (payload_idx, &(ci, src)) in plan.payload.iter().enumerate() {
            match src {
                PayloadSrc::Filler => row.filler(ci),
                PayloadSrc::Arg(i) => row.payload(payload_idx, ci, Some(&*arg(i)))?,
                PayloadSrc::Absent => row.payload(payload_idx, ci, None)?,
            }
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
        let weight_is_column = schema.columns.iter().any(|c| c.name == WEIGHT_KW);
        let batch = ZSetBatch::new(&schema);
        Ok(PyZSetBatch {
            key_scratch: gnitz_core::PkTuple::new(schema.pk_stride() as u8),
            batch,
            weight_is_column,
            kw_plan: None,
            schema,
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
            // A sized sequence lets every growth stream skip its climb from
            // zero; a generator has no `__len__`, so the probe is discarded.
            if let Ok(n) = rows.len() {
                s.batch.reserve(s.schema.as_ref(), n);
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
// Value encoders
// ---------------------------------------------------------------------------

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
/// costs ~89 more instructions per 5-column `append` — ~6% of the call. That
/// figure is a local instructions-retired measurement with no in-tree bench
/// behind it: `gnitz-py` is excluded from `make test` (a pyo3 extension cannot
/// link a test harness), so the repo's `#[ignore]`d `*_bench` convention is
/// unavailable here, and the end-to-end Python harness cannot resolve 89
/// instructions. The duplication is deliberate; see [`write_fixed_le_into`] for what keeps the two
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

/// Build a `PkTuple` for the wire-only `seek` paths, which have no schema at the
/// FFI boundary: `bytes` is taken verbatim, and an integer becomes a narrow
/// 16-byte tuple whose high padding is inert (the server reads only the
/// column's own stride). The signed fallback keeps a negative key packing to the
/// same two's-complement bytes the typed append path writes.
pub(crate) fn pk_tuple_from_py(pk: &Bound<'_, PyAny>) -> PyResult<gnitz_core::PkTuple> {
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
