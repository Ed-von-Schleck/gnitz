//! The Python binding for a mirrored view: `gnitz.Mirror`.
//!
//! This module links the whole engine into the extension — that is what a mirror
//! is. Nothing of it runs at import: the engine installs no allocator,
//! constructor or signal handler, so `Mirror(...)` is where it first does
//! anything.

use pyo3::prelude::*;
use pyo3::types::PyTuple;

use gnitz_core::ReadTarget;
use gnitz_mirror::{Mirror, MirrorError, PollOutcome};
use gnitz_sql::SqlPlanner;

use crate::{batch_to_lazy, client_err, connect_client, sql_err, sql_results_to_py, to_py_err};
use crate::{GnitzError, PyScanResult};

// A mirror handle refused every further call because a delta did not reach its
// copy, which leaves a hole a cursor would step over. A subclass of GnitzError,
// like the conflict and expiry errors, so `except GnitzError` still catches it
// while a host that wants to discard and reopen the handle can name it.
pyo3::create_exception!(_native, GnitzMirrorPoisonedError, GnitzError);

/// A `!Send` value carried across [`Python::detach`].
///
/// `Ungil` is `Send` only as a stand-in for "holds no Python reference" —
/// `detach` runs the closure on this same thread and starts none. A [`Mirror`]
/// is `!Send` for an unrelated reason, the `Rc`s in the engine under it, and
/// `gnitz-mirror` does not depend on pyo3, so nothing reachable from one can be
/// a Python reference.
struct Confined<T>(T);
// SAFETY: as above — the value never leaves this thread, and holds nothing of
// Python's to smuggle out from under the GIL.
unsafe impl<T> Send for Confined<T> {}

impl<T> Confined<T> {
    /// Unwrap inside the detached closure. A method rather than a field read:
    /// reading `.0` would make the closure capture the field, which is the
    /// `!Send` value itself, and the wrapper would not apply.
    fn into_inner(self) -> T {
        self.0
    }
}

/// Run `f` on the handle with the GIL released, so the rest of the interpreter
/// runs across the whole operation — the round trips, and the engine and disk
/// work between them.
///
/// pyo3's borrow flag and the `unsendable` pyclass keep another thread out of
/// the handle meanwhile: a second thread reaching it raises rather than racing.
fn detached<T: Send>(py: Python<'_>, m: &mut Mirror, f: impl Send + FnOnce(&mut Mirror) -> T) -> T {
    let m = Confined(m);
    py.detach(move || f(m.into_inner()))
}

/// The one place a [`MirrorError`] becomes a Python exception. `Upstream` keeps
/// the client's own classification, so an expired cursor still raises
/// `GnitzDeltaExpiredError`.
fn mirror_err(e: MirrorError) -> PyErr {
    match e {
        MirrorError::Upstream(c) => client_err(c),
        MirrorError::Engine(m) => GnitzError::new_err(m),
        MirrorError::Poisoned(_) => GnitzMirrorPoisonedError::new_err(e.to_string()),
    }
}

/// What one view's poll did. The round its copy now answers at is
/// `Mirror.cursor(view_id)`; this says only whether the copy is a continuation
/// of what the caller last saw.
#[pyclass(name = "PollResult", frozen, get_all)]
pub struct PyPollResult {
    /// The relation's server id, which a recreated view moves.
    view_id: u64,
    /// The copy was discarded and re-read whole, so anything derived from its
    /// previous contents is stale in a way no delta explains. True for a first
    /// registration and for every recovery; false for a poll that applied
    /// deltas, and for a reopen that resumed from its persisted cursor.
    reseeded: bool,
}

impl From<PollOutcome> for PyPollResult {
    fn from(o: PollOutcome) -> PyPollResult {
        PyPollResult {
            view_id: o.view_id,
            reseeded: o.reseeded,
        }
    }
}

#[pymethods]
impl PyPollResult {
    fn __repr__(&self) -> String {
        format!(
            "PollResult(view_id={}, reseeded={})",
            self.view_id,
            if self.reseeded { "True" } else { "False" }
        )
    }
}

/// A local copy of one or more views, read in this process without a round trip.
///
/// Open it on a private data directory and a connect target, register views with
/// `mirror_view`, bring them forward with `poll`, and read them with
/// `execute_sql`. A `SELECT` over a mirrored view is answered off the copy;
/// every other statement, and every unheld relation, goes to the connection the
/// handle owns.
///
/// **A mirrored read answers at the last poll**, not at what the server holds
/// now: not read-your-own-writes, and two mirrored views are no consistent cut.
///
/// **One handle per data directory.** Use `with Mirror(...) as m:` or call
/// `close()` — the exit checkpoint runs from the handle's destructor, which an
/// exiting interpreter may never run.
///
/// **Use it from the thread that opened it, and from no other.** A second thread
/// gets `pyo3_runtime.PanicException` from pyo3's borrow check — a
/// `BaseException`, which `except Exception` does not catch; that thread dies
/// and the process lives on.
///
/// **Every method that does work drops the GIL for all of it** — the round
/// trips, and the engine and disk work between them. Only the metadata getters,
/// which answer out of memory, hold it.
///
/// A mirror cannot be driven from `gnitz.aio`.
#[pyclass(name = "Mirror", unsendable)]
pub struct PyMirror {
    inner: Option<Mirror>,
}

impl PyMirror {
    /// The still-open handle, poisoned or not — what the diagnostic methods
    /// take, so a host can inspect a poisoned handle and then release it.
    fn opened(&mut self) -> PyResult<&mut Mirror> {
        self.inner
            .as_mut()
            .ok_or_else(|| GnitzError::new_err("mirror already closed"))
    }

    /// The still-open, unpoisoned handle — what every method that touches a copy
    /// takes. The check is here and not at the crate's gates because a poison
    /// raised through the SQL layer arrives flattened into a `ClientError` and
    /// would raise the wrong class.
    fn live(&mut self) -> PyResult<&mut Mirror> {
        if let Some(why) = self.opened()?.poisoned().map(str::to_string) {
            return Err(mirror_err(MirrorError::Poisoned(why)));
        }
        self.opened()
    }
}

#[pymethods]
impl PyMirror {
    /// Mirror(base_dir, target)
    ///
    /// Connect to `target` and open (or resume) the copy directory at
    /// `base_dir`. The directory is the handle's alone: it holds the local
    /// catalog, one store per mirrored view, and the cursor file.
    #[new]
    pub fn new(py: Python<'_>, base_dir: &str, target: &str) -> PyResult<Self> {
        let client = connect_client(py, target)?;
        // The engine open is disk work, so the GIL is down for it as it was for
        // the connect.
        let opened = py.detach(move || Confined(Mirror::open(base_dir, client)));
        Ok(PyMirror {
            inner: Some(opened.into_inner().map_err(mirror_err)?),
        })
    }

    /// Checkpoint (unless poisoned) and release the handle. Calling it twice is
    /// fine.
    pub fn close(&mut self, py: Python<'_>) {
        // The drop checkpoints, so the GIL goes down for it like any other write.
        if let Some(m) = self.inner.take() {
            let m = Confined(m);
            py.detach(move || drop(m));
        }
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __exit__(&mut self, py: Python<'_>, _exc_type: Py<PyAny>, _exc_val: Py<PyAny>, _exc_tb: Py<PyAny>) -> bool {
        self.close(py);
        false
    }

    /// mirror_view(schema_name, name) -> PollResult
    ///
    /// Register the view and bring its copy up to date. Idempotent, and the same
    /// call whether this is a first registration or a reopen: the result says
    /// which it was.
    ///
    /// Only a view with a delta feed can be mirrored — create it
    /// `WITH (delta = '<size>')`.
    pub fn mirror_view(&mut self, py: Python<'_>, schema_name: &str, name: &str) -> PyResult<Py<PyPollResult>> {
        let outcome = detached(py, self.live()?, |m| m.mirror_view(schema_name, name)).map_err(mirror_err)?;
        Py::new(py, PyPollResult::from(outcome))
    }

    /// forget_view(view_id)
    ///
    /// Stop mirroring the relation: the copy and its directory go, and a later
    /// read of it is delegated upstream.
    pub fn forget_view(&mut self, py: Python<'_>, view_id: u64) -> PyResult<()> {
        detached(py, self.live()?, |m| m.forget_view(view_id)).map_err(mirror_err)
    }

    /// poll() -> list[PollResult]
    ///
    /// Advance every registered view by one poll each, and report what each one
    /// did.
    ///
    /// A poll drives no tick server-side, so a drain is "read the view against
    /// the server, then poll once".
    ///
    /// Every view is attempted and the first failure is raised after the loop,
    /// naming the view — except a `KeyboardInterrupt`, which stops at the view
    /// it interrupted. An error therefore carries no report, so treat every
    /// mirrored view as possibly reseeded.
    pub fn poll(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyPollResult>>> {
        let outcomes = detached(py, self.live()?, |m| m.poll()).map_err(mirror_err)?;
        outcomes
            .into_iter()
            .map(|o| Py::new(py, PyPollResult::from(o)))
            .collect()
    }

    /// Make every copy and its cursor durable.
    ///
    /// A failure is reported, not fatal: the flush writes shards and publishes
    /// manifests, neither of which mutates what a copy holds, so the handle
    /// stays usable and a retry is sound.
    pub fn checkpoint(&mut self, py: Python<'_>) -> PyResult<()> {
        detached(py, self.live()?, |m| m.checkpoint()).map_err(mirror_err)
    }

    /// Whether a read of `view_id` is answered locally. Answers on a poisoned
    /// handle.
    pub fn mirrors(&mut self, view_id: u64) -> PyResult<bool> {
        Ok(self.opened()?.mirrors(view_id))
    }

    /// Every registration this handle holds, whether or not the copy behind it
    /// is valid — wider than `mirrors` by the ones a poll has yet to seed.
    pub fn mirrored_ids(&mut self) -> PyResult<Vec<u64>> {
        Ok(self.opened()?.mirrored_ids())
    }

    /// cursor(view_id) -> (tag, tick) | None
    ///
    /// The round a local read of `view_id` answers at, or `None` when there is
    /// no valid copy to read one off.
    ///
    /// The tick is the master's global round counter, shared by every relation,
    /// so it advances over rounds that carried this view nothing. Whether a copy
    /// changed is `PollResult.reseeded`, not this.
    pub fn cursor(&mut self, py: Python<'_>, view_id: u64) -> PyResult<Py<PyAny>> {
        match self.opened()?.cursor_of(view_id) {
            None => Ok(py.None()),
            Some(c) => Ok(PyTuple::new(py, [c.tag, c.tick])?.into_any().unbind()),
        }
    }

    /// The message that poisoned this handle, or `None`. Answers on a poisoned
    /// handle — diagnosing one is what it is for.
    #[getter]
    pub fn poisoned(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.opened()?.poisoned() {
            None => Ok(py.None()),
            Some(m) => Ok(m.into_pyobject(py)?.into_any().unbind()),
        }
    }

    /// reconnect(target)
    ///
    /// Replace the upstream connection, keeping every copy and cursor. What a
    /// host does after a server restart: the restart kills the handle's socket,
    /// while the copy — durable, and resumable — survives it.
    pub fn reconnect(&mut self, py: Python<'_>, target: &str) -> PyResult<()> {
        self.live()?; // refuse a closed or poisoned handle before paying for a connect
        let client = connect_client(py, target)?;
        *self.opened()?.client_mut() = client;
        Ok(())
    }

    /// execute_sql(sql, schema_name="public") -> list of result dicts
    ///
    /// The same list `GnitzClient.execute_sql` returns. A `SELECT` (and the
    /// `EXPLAIN` of one) over a mirrored view is planned and answered against
    /// the local copy; every other statement, and every read of a relation this
    /// handle does not hold, runs on the connection the handle owns.
    #[pyo3(signature = (sql, schema_name = "public"))]
    pub fn execute_sql(&mut self, py: Python<'_>, sql: &str, schema_name: &str) -> PyResult<Py<PyAny>> {
        // `sql_err`, not the bare classifier: an unheld relation is delegated to
        // the handle's own client, so a Ctrl-C here arrives as
        // `Exec(ClientError::Interrupted)` and has to reach Python as the
        // `KeyboardInterrupt` it carries.
        let results = detached(py, self.live()?, |m| SqlPlanner::new(m, schema_name).execute(sql)).map_err(sql_err)?;
        sql_results_to_py(py, results)
    }

    /// scan(view_id, include_hidden=False) -> ScanResult
    ///
    /// Every row of the relation, off the copy if it is mirrored and from the
    /// server if it is not. `lsn` is `None`: a served LSN is a server-side
    /// counter, and a copy's freshness is a feed round — `cursor(view_id)` is
    /// where a host reads it.
    #[pyo3(signature = (view_id, include_hidden = false))]
    pub fn scan(&mut self, py: Python<'_>, view_id: u64, include_hidden: bool) -> PyResult<Py<PyScanResult>> {
        let (schema, batch) = to_py_err(detached(py, self.live()?, |m| ReadTarget::scan(m, view_id)))?;
        batch_to_lazy(py, schema, batch, None, include_hidden)
    }

    /// The owned connection's request counter — what a test asserts a mirrored
    /// read does not move. Answers on a poisoned handle.
    #[getter]
    pub fn requests_sent(&mut self) -> PyResult<u64> {
        Ok(self.opened()?.client_mut().requests_sent())
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            None => "<Mirror closed>".to_string(),
            Some(m) if m.poisoned().is_some() => format!("<Mirror {} views, poisoned>", m.mirrored_ids().len()),
            Some(m) => format!("<Mirror {} views>", m.mirrored_ids().len()),
        }
    }
}
