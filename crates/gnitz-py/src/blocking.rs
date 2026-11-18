//! `GnitzClient`: a client whose verbs run on the calling thread with the GIL
//! released.

use std::sync::Mutex;

use pyo3::prelude::*;

use gnitz_core::{BlockingHost, GnitzClient, block_on};
use gnitz_sql::GnitzSqlError;

use crate::client::{Mode, PyClient, resolve_target};
use crate::{client_err, sql_err};

/// A connection whose waits check for Ctrl-C.
fn connect(py: Python<'_>, target: &str) -> PyResult<GnitzClient> {
    let host = BlockingHost::with_hook(|| Python::attach(|py| py.check_signals()).map_err(Into::into));
    py.detach(|| block_on(GnitzClient::connect_with(target, Box::new(host))))
        .map_err(client_err)
}

/// Run `f` on the client with the GIL released.
pub(crate) fn call<T: Send, E: Into<GnitzSqlError> + Send>(
    client: &mut GnitzClient,
    py: Python<'_>,
    f: impl AsyncFnOnce(&mut GnitzClient) -> Result<T, E> + Send,
) -> PyResult<T> {
    py.detach(|| block_on(f(client))).map_err(|e| sql_err(e.into()))
}

// ---------------------------------------------------------------------------
// The class
// ---------------------------------------------------------------------------

/// A connection whose verbs return once they are answered.
#[pyclass(name = "GnitzClient", extends = PyClient, frozen, module = "gnitz")]
pub struct PyGnitzClient;

#[pymethods]
impl PyGnitzClient {
    #[new]
    #[pyo3(signature = (target = None, schema = "public"))]
    fn new(py: Python<'_>, target: Option<String>, schema: &str) -> PyResult<PyClassInitializer<Self>> {
        let client = connect(py, &resolve_target(target)?)?;
        Ok(PyClient::init(Mode::Blocking(Mutex::new(Some(Box::new(client)))), schema).add_subclass(PyGnitzClient))
    }

    /// Close the connection, checkpointing and releasing the mirror store first,
    /// and raise if that final checkpoint failed; the connection closes either
    /// way. Idempotent. The GIL goes down for it: the checkpoint is fsync-bound.
    fn close(slf: &Bound<'_, Self>) -> PyResult<()> {
        // Bound first, so the slot's lock is down before the checkpoint runs.
        let taken = slf.as_super().get().blocking_slot()?.take();
        match taken {
            Some(mut client) => call(&mut client, slf.py(), async |c| c.close_mirror().await),
            None => Ok(()),
        }
    }

    fn __enter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __exit__(
        slf: &Bound<'_, Self>,
        _exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<bool> {
        Self::close(slf)?;
        Ok(false)
    }
}
