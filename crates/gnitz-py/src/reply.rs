//! The replies that are not rows, as Python objects: the result classes of a
//! poll and a sync, `execute_sql`'s statement dicts, and the converters a verb
//! hands its driver to turn what the client yields into its Python value.

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};

use gnitz_core::{DeltaCursor, PollOutcome, PollResult, Pushed, ScanReply, Synced};
use gnitz_sql::SqlResult;

use crate::client_err;
use crate::read::{PyScanResult, scan_result};

/// What one view's poll did. `PollOutcome` flattened for Python, which has no
/// cheap payload-carrying enum.
#[pyclass(name = "PollResult", frozen, get_all, module = "gnitz")]
pub struct PyPollResult {
    /// The relation's server id, which a recreated view moves.
    view_id: u64,
    /// The copy was discarded and re-read whole — the discontinuity a subscriber
    /// has to react to, which no cursor carries.
    reseeded: bool,
    /// The exception this one view's poll failed with, or `None`.
    error: Option<Py<PyBaseException>>,
    /// `(tag, tick)`, or `None` when the view has no valid copy.
    cursor: Option<(u64, u64)>,
}

impl PyPollResult {
    fn new(py: Python<'_>, o: PollOutcome) -> PyPollResult {
        PyPollResult {
            view_id: o.view_id,
            reseeded: o.result.reseeded(),
            cursor: o.cursor.map(DeltaCursor::pair),
            error: match o.result {
                PollResult::Failed(e) => Some(client_err(e).into_value(py)),
                _ => None,
            },
        }
    }
}

#[pymethods]
impl PyPollResult {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "PollResult(view_id={}, reseeded={}, cursor={}, error={})",
            self.view_id,
            py_repr(py, self.reseeded)?,
            py_repr(py, self.cursor)?,
            py_repr(py, &self.error)?,
        ))
    }
}

/// What one sync brought. `gnitz_core::Synced` for Python.
#[pyclass(name = "Synced", frozen, get_all, module = "gnitz")]
pub struct PySynced {
    /// One `Pushed` per subscription `subscribe` made, in the order made.
    pushed: Py<PyList>,
    /// One `PollResult` per mirrored view.
    mirrored: Py<PyList>,
}

impl PySynced {
    fn new(py: Python<'_>, synced: Synced) -> PyResult<PySynced> {
        let pushed = synced.pushed.into_iter().map(|p| PyPushed::new(py, p));
        let mirrored = synced.mirrored.into_iter().map(|o| PyPollResult::new(py, o));
        Ok(PySynced {
            pushed: PyList::new(py, pushed.collect::<PyResult<Vec<_>>>()?)?.unbind(),
            mirrored: PyList::new(py, mirrored)?.unbind(),
        })
    }
}

/// What a sync handed one subscription. `gnitz_core::Pushed` flattened for
/// Python, as [`PyPollResult`] is.
#[pyclass(name = "Pushed", frozen, get_all, module = "gnitz")]
pub struct PyPushed {
    /// The id `subscribe` returned.
    sub: u64,
    /// The deltas pushed since the last sync, weights and all; `None` once
    /// the subscription ended.
    rows: Option<Py<PyScanResult>>,
    /// `(tag, tick)` past `rows`; `None` once the subscription ended.
    cursor: Option<(u64, u64)>,
    /// The exception the subscription ended with, or `None`.
    error: Option<Py<PyBaseException>>,
}

impl PyPushed {
    fn new(py: Python<'_>, pushed: Pushed) -> PyResult<PyPushed> {
        let sub = pushed.sub.get();
        Ok(match pushed.result {
            Ok((reply, cursor)) => PyPushed {
                sub,
                rows: Some(scan_result(py, reply)?),
                cursor: Some(cursor.pair()),
                error: None,
            },
            Err(e) => PyPushed {
                sub,
                rows: None,
                cursor: None,
                error: Some(client_err(e).into_value(py)),
            },
        })
    }
}

#[pymethods]
impl PyPushed {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Pushed(sub={}, rows={}, cursor={}, error={})",
            self.sub,
            py_repr(py, &self.rows)?,
            py_repr(py, self.cursor)?,
            py_repr(py, &self.error)?,
        ))
    }
}

/// `v` as Python's `repr` prints it.
fn py_repr<'py>(py: Python<'py>, v: impl IntoPyObject<'py>) -> PyResult<String> {
    Ok(v.into_bound_py_any(py)?.repr()?.to_string())
}

/// `execute_sql`'s result: one dict per statement, keyed as `gnitz._types`'
/// TypedDicts are.
pub(crate) fn sql_results(py: Python<'_>, results: Vec<SqlResult>) -> PyResult<Py<PyAny>> {
    let dicts = results.into_iter().map(|r| {
        let (kind, field) = match r {
            SqlResult::Ddl { .. } => ("Ddl", None),
            SqlResult::RowsAffected { count, .. } => ("RowsAffected", Some(("count", count.into_py_any(py)?))),
            SqlResult::Rows(reply) => ("Rows", Some(("rows", scan_result(py, reply)?.into_any()))),
            SqlResult::TransactionStarted => ("TransactionStarted", None),
            SqlResult::TransactionCommitted { lsn } => ("TransactionCommitted", Some(("lsn", lsn.into_py_any(py)?))),
            SqlResult::TransactionRolledBack => ("TransactionRolledBack", None),
        };
        let d = PyDict::new(py);
        d.set_item(pyo3::intern!(py, "type"), PyString::intern(py, kind))?;
        if let Some((key, v)) = field {
            d.set_item(PyString::intern(py, key), v)?;
        }
        Ok(d)
    });
    dicts.collect::<PyResult<Vec<_>>>()?.into_py_any(py)
}

// ---------------------------------------------------------------------------
// Converters: what a verb's driver calls, GIL held, on what the client yields.
// ---------------------------------------------------------------------------

/// The value itself.
pub(crate) fn value<T>(py: Python<'_>, v: T) -> PyResult<Py<PyAny>>
where
    T: for<'py> IntoPyObject<'py>,
{
    v.into_py_any(py)
}

/// `None`, whatever the client yielded.
pub(crate) fn none<T>(py: Python<'_>, _: T) -> PyResult<Py<PyAny>> {
    Ok(py.None())
}

/// A read's rows.
pub(crate) fn scanned(py: Python<'_>, reply: ScanReply) -> PyResult<Py<PyAny>> {
    Ok(scan_result(py, reply)?.into_any())
}

/// What one view's poll did.
pub(crate) fn polled(py: Python<'_>, outcome: PollOutcome) -> PyResult<Py<PyAny>> {
    value(py, PyPollResult::new(py, outcome))
}

/// What one sync brought.
pub(crate) fn synced(py: Python<'_>, synced: Synced) -> PyResult<Py<PyAny>> {
    value(py, PySynced::new(py, synced)?)
}

/// A delta read's rows, and the cursor past them.
pub(crate) fn delta(py: Python<'_>, (reply, cursor): (ScanReply, DeltaCursor)) -> PyResult<Py<PyAny>> {
    (scan_result(py, reply)?, cursor.pair()).into_py_any(py)
}
