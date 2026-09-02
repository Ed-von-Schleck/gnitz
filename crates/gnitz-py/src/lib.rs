//! The Python extension module: the exception hierarchy every surface raises
//! through, the connection factory that installs the Ctrl-C park hook, and the
//! `_native` registration.
//!
//! The pyclasses themselves live in the modules below — `schema`, the directional
//! pair `write` / `read`, `client` and `async_transport` — and each one keeps its
//! own helpers private. Registration reaches across only because `_native` must
//! name every class.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use gnitz_core::{ClientError, ConflictClass, GnitzClient, MirrorError, Schema, TypeCode};

mod async_transport;
mod client;
mod read;
mod schema;
mod write;

use async_transport::PyAsyncTransport;
use client::{PyGnitzClient, PyPollResult, PyTxn};
use read::{PyDeltaReply, PyRow, PyRowIterator, PyScanResult};
use schema::{resolve_py_schema, rust_schema_to_py, PyColumnDef, PySchema};
use write::{install_append_method, PyZSetBatch};

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
// The server's shared log was full (STATUS_SAL_FULL). A subtype of GnitzError,
// like the conflict and expiry errors, so `except GnitzError` still catches it
// while a caller that wants to retry — this is the one server error that clears
// itself — can name it.
pyo3::create_exception!(_native, GnitzSalFullError, GnitzError);
// A mirror store refused every further call that touches a copy, because a delta
// did not reach it and the hole a cursor would step over is unrecoverable. A
// subclass of GnitzError, like the conflict and expiry errors, so
// `except GnitzError` still catches it while a host that wants to release the
// store and start over can name it. `close_mirror()` is that recovery.
pyo3::create_exception!(_native, GnitzMirrorPoisonedError, GnitzError);
// A named relation, index or schema the catalog does not hold. A subtype of
// GnitzError like the classes above, so `except GnitzError` still catches it
// while a caller that branches on absence can name it instead of matching the
// message prose.
pyo3::create_exception!(_native, GnitzNotFoundError, GnitzError);

/// Wrap any `Display` error as a `GnitzError` PyErr. For the handful of
/// failures that carry no retryability verdict (handshake, waker setup).
pub(crate) fn gnitz_err(e: impl std::fmt::Display) -> PyErr {
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
pub(crate) fn client_err(e: ClientError) -> PyErr {
    match e {
        ClientError::Interrupted(inner) => match inner.downcast::<PyErr>() {
            Ok(py_err) => *py_err,
            Err(other) => gnitz_err(other),
        },
        ClientError::DeltaExpired => GnitzDeltaExpiredError::new_err(e.to_string()),
        ClientError::SalFull(_) => GnitzSalFullError::new_err(e.to_string()),
        ClientError::NotFound { .. } => GnitzNotFoundError::new_err(e.to_string()),
        // The arm, and not a pre-call check on the client, is what raises the
        // poison class: refusing *every* method up front would take the
        // connection's own reads down with the copy, where only a read that would
        // have come off the copy is refused.
        ClientError::Mirror(MirrorError::Poisoned(_)) => GnitzMirrorPoisonedError::new_err(e.to_string()),
        other => classified_err(&other),
    }
}

/// [`client_err`] for the SQL layer's error, which wraps a `ClientError`.
pub(crate) fn sql_err(e: gnitz_sql::GnitzSqlError) -> PyErr {
    match e {
        gnitz_sql::GnitzSqlError::Exec(inner) => client_err(inner),
        other => classified_err(&other),
    }
}

/// Map a client error to a Python exception — the `Result`-shaped adapter for
/// a call that is not routed through [`PyGnitzClient::call`].
pub(crate) fn to_py_err<T>(res: Result<T, ClientError>) -> PyResult<T> {
    res.map_err(client_err)
}

/// Collect a known-length fallible iterator into a Python list, reserving that
/// length up front — which `collect::<PyResult<Vec<_>>>()` cannot do, because a
/// fallible collect lower-bounds its `size_hint` to 0 and grows from there.
pub(crate) fn build_pylist<'py, T: IntoPyObject<'py>>(
    py: Python<'py>,
    items: impl ExactSizeIterator<Item = PyResult<T>>,
) -> PyResult<Bound<'py, PyList>> {
    let mut out: Vec<T> = Vec::with_capacity(items.len());
    for item in items {
        out.push(item?);
    }
    PyList::new(py, out)
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
fn delta_reply_schema(
    py: Python<'_>,
    #[pyo3(from_py_with = resolve_py_schema)] view_schema: Arc<Schema>,
) -> PyResult<Py<PySchema>> {
    let derived = to_py_err(gnitz_core::delta_reply_schema(&view_schema))?;
    rust_schema_to_py(py, &Arc::new(derived))
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
// `Py_MOD_GIL_NOT_USED`. This module links the `Rc`-based engine, so that claim
// would be false.
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
    m.add("GnitzSalFullError", m.py().get_type::<GnitzSalFullError>())?;
    m.add(
        "GnitzMirrorPoisonedError",
        m.py().get_type::<GnitzMirrorPoisonedError>(),
    )?;
    m.add("GnitzNotFoundError", m.py().get_type::<GnitzNotFoundError>())?;
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
