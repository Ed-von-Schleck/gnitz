//! The Python extension module: the exception hierarchy every surface raises
//! through, and the `_native` registration.
//!
//! `client` holds every verb; `blocking` and `aio` are how each client class
//! waits, and `pool` is where an `aio` client's blocking work runs; `write`
//! and `read` encode and decode values; `reply` turns the other replies into
//! Python objects; `schema` describes columns.

use std::sync::{Mutex, MutexGuard, PoisonError};

use pyo3::prelude::*;

use gnitz_core::ClientError;
use gnitz_wire::{TypeCode, WireStatus};

mod aio;
mod blocking;
mod client;
mod local_server;
mod pool;
mod read;
mod reply;
mod schema;
mod write;

use aio::PyAsyncClient;
use blocking::PyGnitzClient;
use client::{PyClient, PyTxn};
use local_server::PyLocalServer;
use read::{PyRow, PyScanResult};
use reply::{PyPollResult, PyPushed, PySynced};
use schema::{PyColumnDef, PySchema};
use write::{PyZSetBatch, install_append_method};

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

/// Declares each exception class and the `add_exceptions` that registers
/// every one of them on the module, from one table.
macro_rules! exceptions {
    ($($name:ident($base:ty): $doc:literal;)*) => {
        $(pyo3::create_exception!(gnitz, $name, $base, $doc);)*

        fn add_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
            $(m.add(stringify!($name), m.py().get_type::<$name>())?;)*
            Ok(())
        }
    };
}

// Created under `gnitz`, which re-exports every one of them: an exception is
// pickled by its module and name, and `_native` is no module an unpickler can
// import. `client_err` decides which class a failure raises.
exceptions! {
    GnitzError(pyo3::exceptions::PyException): "The base of every exception gnitz raises.";
    GnitzRefusedError(GnitzError): "The request was understood and declined; the connection is intact.";
    GnitzConnectionError(GnitzError): "The connection is unusable: it failed, was lost, or was closed.";
    GnitzConflictError(GnitzRefusedError): "Nothing was written; run it again.";
    GnitzDeltaExpiredError(GnitzRefusedError):
        "The feed does not continue this delta cursor; discard the copy and bootstrap again.";
    GnitzSalFullError(GnitzRefusedError): "The server's log had no room for the request; run it again.";
    GnitzMirrorPoisonedError(GnitzError):
        "The mirror store may be torn; recovery is close_mirror() and nothing else.";
    GnitzNotFoundError(GnitzRefusedError): "The named relation or schema does not exist.";
    GnitzIntegrityError(GnitzRefusedError):
        "The write would break a primary key, unique index or foreign key; nothing was written.";
}

/// Wrap any `Display` error as a plain `GnitzError`.
fn gnitz_err(e: impl std::fmt::Display) -> PyErr {
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
        ClientError::MirrorPoisoned => GnitzMirrorPoisonedError::new_err(e.to_string()),
        e @ ClientError::Mirror(_) => gnitz_err(e),
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

/// A lock that guards plain state: a panic under it leaves that state whole.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
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
    // This extension's own copies of gnitz-mirror and gnitz-store log, and
    // nothing else in the process sets their level.
    if let Some(word) = std::env::var_os("GNITZ_LOG_LEVEL") {
        let word = word.to_string_lossy();
        let level = gnitz_foundation::log::Level::parse(&word)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("GNITZ_LOG_LEVEL: {e}")))?;
        gnitz_foundation::log::set_level(level);
    }
    m.add_class::<PyColumnDef>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyRow>()?;
    m.add_class::<PyZSetBatch>()?;
    install_append_method(m.py())?;
    m.add_class::<PyScanResult>()?;
    m.add_class::<PyClient>()?;
    m.add_class::<PyGnitzClient>()?;
    m.add_class::<PyAsyncClient>()?;
    m.add_class::<PyTxn>()?;
    m.add_class::<PyPollResult>()?;
    m.add_class::<PyLocalServer>()?;
    m.add_class::<PyPushed>()?;
    m.add_class::<PySynced>()?;
    add_exceptions(m)?;
    // The first id a user relation takes, single-sourced from gnitz_wire.
    m.add("FIRST_USER_TABLE_ID", gnitz_wire::FIRST_USER_TABLE_ID)?;
    // The schema-width cap, so a test locates the boundary instead of naming it.
    m.add("MAX_COLUMNS", gnitz_wire::MAX_COLUMNS)?;
    // The requests one connection may hold in flight, for the same reason.
    m.add("MAX_IN_FLIGHT", gnitz_core::MAX_IN_FLIGHT)?;
    // What a server logs once it serves, for the code that starts one.
    m.add("READY_LINE", gnitz_foundation::log::READY_LINE)?;
    // Whether *this extension* keeps its fault seams. A mirroring test arms
    // seams that fire in this process — the mirror's ingest, in this
    // extension's own copies of gnitz-mirror and gnitz-store — so the server's
    // build says nothing about them: `e2e-release` pairs a release server with
    // a debug extension.
    m.add("fault_seams", gnitz_foundation::fault::ARMED)?;
    m.add_function(wrap_pyfunction!(type_codes, m)?)?;
    m.add_function(wrap_pyfunction!(instructions_retired, m)?)?;
    m.add_function(wrap_pyfunction!(local_server::find_server, m)?)?;
    Ok(())
}
