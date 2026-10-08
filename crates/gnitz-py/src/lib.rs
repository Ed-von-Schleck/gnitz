//! The Python extension module: the exception hierarchy every surface raises
//! through, and the `_native` registration.

use pyo3::prelude::*;

use gnitz_core::{ClientError, MirrorError};
use gnitz_wire::{TypeCode, WireStatus};

mod client;
mod drive;
mod read;
mod schema;
mod write;

use client::{PyAsyncClient, PyClient, PyGnitzClient, PyPollResult, PyPushed, PySynced, PyTxn};
use drive::{LoopCore, PyPending, PyPipeline};
use read::{PyRow, PyRowIterator, PyScanResult};
use schema::{PyColumnDef, PySchema};
use write::{install_append_method, PyZSetBatch};

// ---------------------------------------------------------------------------
// GnitzError Python exception
// ---------------------------------------------------------------------------

pyo3::create_exception!(_native, GnitzError, pyo3::exceptions::PyException);

// Every class below subclasses GnitzError, so `except GnitzError` catches them
// all while a caller that branches on one can name it instead of matching prose.

// The request was understood and declined: `ClientError::Refused` at any
// status, and a statement the SQL planner rejects. The connection is intact.
pyo3::create_exception!(_native, GnitzRefusedError, GnitzError);
// The connection is unusable: it failed, was lost, or was closed.
pyo3::create_exception!(_native, GnitzConnectionError, GnitzError);
// MirrorError::Poisoned. Recovery is `close_mirror()`, and nothing else.
pyo3::create_exception!(_native, GnitzMirrorPoisonedError, GnitzError);

// A refusal whose status a caller branches on.

// WireStatus::TxnConflict, and WireStatus::StaleCatalog: nothing was written,
// and the caller's recovery is to run it again.
pyo3::create_exception!(_native, GnitzConflictError, GnitzRefusedError);
// WireStatus::DeltaExpired.
pyo3::create_exception!(_native, GnitzDeltaExpiredError, GnitzRefusedError);
// WireStatus::SalFull.
pyo3::create_exception!(_native, GnitzSalFullError, GnitzRefusedError);
// WireStatus::NotFound.
pyo3::create_exception!(_native, GnitzNotFoundError, GnitzRefusedError);
// WireStatus::IntegrityViolation.
pyo3::create_exception!(_native, GnitzIntegrityError, GnitzRefusedError);

/// Wrap any `Display` error as a plain `GnitzError`.
pub(crate) fn gnitz_err(e: impl std::fmt::Display) -> PyErr {
    GnitzError::new_err(e.to_string())
}

/// The class a [`ClientError`] raises as — the one place that decides, so a
/// given failure is always the same Python class. A refusal raises by its status;
/// `Interrupted` re-raises the `PyErr` it carries, keeping a `KeyboardInterrupt`
/// one.
pub(crate) fn client_err(e: ClientError) -> PyErr {
    match e {
        ClientError::Refused(f) => match f.status {
            WireStatus::TxnConflict | WireStatus::StaleCatalog => GnitzConflictError::new_err(f.text),
            WireStatus::DeltaExpired => GnitzDeltaExpiredError::new_err(f.text),
            WireStatus::SalFull => GnitzSalFullError::new_err(f.text),
            WireStatus::NotFound => GnitzNotFoundError::new_err(f.text),
            WireStatus::IntegrityViolation => GnitzIntegrityError::new_err(f.text),
            WireStatus::Ok | WireStatus::Error => GnitzRefusedError::new_err(f.text),
        },
        ClientError::Protocol(_) | ClientError::ConnectionLost(_) | ClientError::Closed => {
            GnitzConnectionError::new_err(e.to_string())
        }
        ClientError::Interrupted(inner) => match inner.downcast_ref::<PyErr>() {
            Some(py_err) => Python::attach(|py| py_err.clone_ref(py)),
            None => gnitz_err(inner),
        },
        ClientError::Mirror(MirrorError::Poisoned(_)) => GnitzMirrorPoisonedError::new_err(e.to_string()),
        other => gnitz_err(other),
    }
}

/// [`client_err`] for the SQL layer's error, which wraps a `ClientError`.
pub(crate) fn sql_err(e: gnitz_sql::GnitzSqlError) -> PyErr {
    match e {
        gnitz_sql::GnitzSqlError::Client(inner) => client_err(inner),
        gnitz_sql::GnitzSqlError::Rejected(text) => GnitzRefusedError::new_err(text),
        internal @ gnitz_sql::GnitzSqlError::Internal(_) => gnitz_err(internal),
    }
}

/// The `(name, code)` column-type table, straight off `TypeCode::ALL`, which
/// `_types.py` builds its `TypeCode` IntEnum from.
#[pyfunction]
fn type_codes() -> Vec<(&'static str, u8)> {
    TypeCode::ALL.iter().map(|&tc| (tc.wire_name(), tc.as_wire())).collect()
}

/// User-space instructions retired on this thread while `f()` runs.
#[pyfunction]
fn instructions_retired(f: &Bound<'_, PyAny>) -> PyResult<u64> {
    let (out, n) = gnitz_foundation::perf::Counter::instructions().measure(|| f.call0());
    out?;
    Ok(n)
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

// pyo3 makes free-threading opt-*out*: a bare `#[pymodule]` emits
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
    m.add_class::<PyClient>()?;
    m.add_class::<PyGnitzClient>()?;
    m.add_class::<PyAsyncClient>()?;
    m.add_class::<LoopCore>()?;
    m.add_class::<PyTxn>()?;
    m.add_class::<PyPipeline>()?;
    m.add_class::<PyPending>()?;
    m.add_class::<PyPollResult>()?;
    m.add_class::<PyPushed>()?;
    m.add_class::<PySynced>()?;
    m.add("GnitzError", m.py().get_type::<GnitzError>())?;
    m.add("GnitzRefusedError", m.py().get_type::<GnitzRefusedError>())?;
    m.add("GnitzConnectionError", m.py().get_type::<GnitzConnectionError>())?;
    m.add("GnitzConflictError", m.py().get_type::<GnitzConflictError>())?;
    m.add("GnitzDeltaExpiredError", m.py().get_type::<GnitzDeltaExpiredError>())?;
    m.add("GnitzSalFullError", m.py().get_type::<GnitzSalFullError>())?;
    m.add(
        "GnitzMirrorPoisonedError",
        m.py().get_type::<GnitzMirrorPoisonedError>(),
    )?;
    m.add("GnitzNotFoundError", m.py().get_type::<GnitzNotFoundError>())?;
    m.add("GnitzIntegrityError", m.py().get_type::<GnitzIntegrityError>())?;
    // System-table IDs — single-sourced from gnitz_wire (delegating codec, not
    // a re-typed copy), as is the table behind `type_codes()`.
    // Only the ids something addresses a relation by are exported.
    m.add("SCHEMA_TAB", gnitz_wire::SCHEMA_TAB)?;
    m.add("TABLE_TAB", gnitz_wire::TABLE_TAB)?;
    m.add("COL_TAB", gnitz_wire::COL_TAB)?;
    m.add("IDX_TAB", gnitz_wire::IDX_TAB)?;
    m.add("FIRST_USER_TABLE_ID", gnitz_wire::FIRST_USER_TABLE_ID)?;
    // The schema-width cap, so a test locates the boundary instead of naming it.
    m.add("MAX_COLUMNS", gnitz_wire::MAX_COLUMNS)?;
    // Whether *this extension* keeps its `#[cfg(debug_assertions)]` fault seams.
    // The seams a mirroring test arms live in gnitz-mirror and gnitz-store, which
    // are linked here and not into the server, so the server's build says nothing
    // about them: `e2e-release` pairs a release server with a debug extension.
    m.add("debug_assertions", cfg!(debug_assertions))?;
    m.add_function(wrap_pyfunction!(type_codes, m)?)?;
    m.add_function(wrap_pyfunction!(instructions_retired, m)?)?;
    m.add_function(wrap_pyfunction!(schema::sys_schema, m)?)?;
    Ok(())
}
