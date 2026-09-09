//! The Python extension module: the exception hierarchy every surface raises
//! through, the connection factory that installs the Ctrl-C park hook, and the
//! `_native` registration.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use gnitz_core::{ClientError, GnitzClient, MirrorError, Schema, TypeCode};

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

// Every class below subclasses GnitzError, so `except GnitzError` catches them
// all while a caller that branches on one can name it instead of matching prose.

// STATUS_TXN_CONFLICT: a table the transaction read was written concurrently.
// Retryable.
pyo3::create_exception!(_native, GnitzConflictError, GnitzError);
// STATUS_DELTA_EXPIRED: the cursor's rounds are gone, or it names another boot
// or relation. Recovery is to bootstrap again.
pyo3::create_exception!(_native, GnitzDeltaExpiredError, GnitzError);
// STATUS_SAL_FULL: the one server error that clears itself, so retryable.
pyo3::create_exception!(_native, GnitzSalFullError, GnitzError);
// A mirror store refuses every further call that touches a copy. Recovery is
// `close_mirror()`, and nothing else.
pyo3::create_exception!(_native, GnitzMirrorPoisonedError, GnitzError);
// A named relation, index or schema the catalog does not hold.
pyo3::create_exception!(_native, GnitzNotFoundError, GnitzError);

/// Wrap any `Display` error as a `GnitzError` PyErr. For the handful of
/// failures that carry no retryability verdict (handshake, waker setup).
pub(crate) fn gnitz_err(e: impl std::fmt::Display) -> PyErr {
    GnitzError::new_err(e.to_string())
}

/// The class a [`ClientError`] raises as — the one place that decides, so a
/// given failure is always the same Python class. `Interrupted` re-raises the
/// `PyErr` it carries, keeping a `KeyboardInterrupt` one.
pub(crate) fn client_err(e: ClientError) -> PyErr {
    match e {
        ClientError::Interrupted(inner) => match inner.downcast::<PyErr>() {
            Ok(py_err) => *py_err,
            Err(other) => gnitz_err(other),
        },
        ClientError::TxnConflict { .. } => GnitzConflictError::new_err(e.to_string()),
        ClientError::DeltaExpired => GnitzDeltaExpiredError::new_err(e.to_string()),
        ClientError::SalFull(_) => GnitzSalFullError::new_err(e.to_string()),
        ClientError::NotFound { .. } => GnitzNotFoundError::new_err(e.to_string()),
        ClientError::Mirror(MirrorError::Poisoned(_)) => GnitzMirrorPoisonedError::new_err(e.to_string()),
        other => gnitz_err(other),
    }
}

/// [`client_err`] for the SQL layer's error, which wraps a `ClientError`.
pub(crate) fn sql_err(e: gnitz_sql::GnitzSqlError) -> PyErr {
    match e {
        gnitz_sql::GnitzSqlError::Exec(inner) => client_err(inner),
        gnitz_sql::GnitzSqlError::Conflict { .. } => GnitzConflictError::new_err(e.to_string()),
        other => gnitz_err(other),
    }
}

/// Collect a known-length fallible iterator into a Python list, reserving that
/// length up front — which `collect::<PyResult<Vec<_>>>()` cannot do.
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

/// The one way this crate obtains a connection, so no caller can skip the park
/// hook — what makes a blocking call Ctrl-C-interruptible. `gnitz-core` must not
/// depend on pyo3, so the hook is the host's to supply.
pub(crate) fn connect_client(py: Python<'_>, target: &str) -> PyResult<GnitzClient> {
    let mut client = py.detach(|| GnitzClient::connect(target)).map_err(client_err)?;
    client.set_park_hook(Some(Box::new(|| {
        Python::attach(|py| py.check_signals()).map_err(|e| ClientError::Interrupted(Box::new(e)))
    })));
    Ok(client)
}

/// Decode a persisted catalog column-list `u64` (`TABLE_TAB.pk_col_idx`,
/// `IDX_TAB.source_col_idx`) into a list of column indices, through the shared
/// `gnitz_wire` codec. An out-of-range packed count raises.
#[pyfunction]
fn unpack_pk_cols(v: u64) -> PyResult<Vec<u32>> {
    gnitz_wire::unpack_pk_cols(v)
        .map(|cols| cols.as_slice().to_vec())
        .map_err(|rule| pyo3::exceptions::PyValueError::new_err(rule.to_string()))
}

/// The reply schema of an incremental delta poll, derived from a view's own
/// schema: a `_tick` U64 key column, then the view's PK columns in PK order,
/// then its payload columns in schema order. A bootstrap read is *not* in this
/// shape — it comes back in the view's own schema.
#[pyfunction]
fn delta_reply_schema(
    py: Python<'_>,
    #[pyo3(from_py_with = resolve_py_schema)] view_schema: Arc<Schema>,
) -> PyResult<Py<PySchema>> {
    let derived = gnitz_core::delta_reply_schema(&view_schema).map_err(client_err)?;
    rust_schema_to_py(py, &Arc::new(derived))
}

/// The `(name, discriminant)` circuit-opcode table, straight off `Opcode::ALL`.
/// `_types.py` builds its `Opcode` IntEnum from this, so a test names the
/// operator rather than re-typing its durable discriminant.
#[pyfunction]
fn circuit_opcodes() -> Vec<(String, u64)> {
    gnitz_wire::Opcode::ALL
        .iter()
        .map(|&op| (format!("{op:?}"), op.as_wire()))
        .collect()
}

/// The `(name, code)` column-type table, straight off `TypeCode::ALL`, which
/// `_types.py` builds its `TypeCode` IntEnum from.
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
    // a re-typed copy), as are the tables behind `type_codes()` and
    // `circuit_opcodes()`.
    // Only the ids something addresses a relation by are exported.
    m.add("SCHEMA_TAB", gnitz_wire::SCHEMA_TAB)?;
    m.add("TABLE_TAB", gnitz_wire::TABLE_TAB)?;
    m.add("VIEW_TAB", gnitz_wire::VIEW_TAB)?;
    m.add("COL_TAB", gnitz_wire::COL_TAB)?;
    m.add("IDX_TAB", gnitz_wire::IDX_TAB)?;
    m.add("CIRCUIT_NODES_TAB", gnitz_wire::CIRCUIT_NODES_TAB)?;
    m.add("FIRST_USER_TABLE_ID", gnitz_wire::FIRST_USER_TABLE_ID)?;
    // Whether *this extension* keeps its `#[cfg(debug_assertions)]` fault seams.
    // The seams a mirroring test arms live in gnitz-mirror and gnitz-store, which
    // are linked here and not into the server, so the server's build says nothing
    // about them: `e2e-release` pairs a release server with a debug extension.
    m.add("debug_assertions", cfg!(debug_assertions))?;
    m.add_class::<PyDeltaReply>()?;
    m.add_function(wrap_pyfunction!(delta_reply_schema, m)?)?;
    m.add_function(wrap_pyfunction!(unpack_pk_cols, m)?)?;
    m.add_function(wrap_pyfunction!(type_codes, m)?)?;
    m.add_function(wrap_pyfunction!(circuit_opcodes, m)?)?;
    Ok(())
}
