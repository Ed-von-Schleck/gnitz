//! `gnitz_core`'s [`LocalServer`] as a Python class: a `gnitz-server` this
//! interpreter runs, which `gnitz.local_server` starts.

use std::path::PathBuf;

use pyo3::exceptions::{PyFileNotFoundError, PyRuntimeError};
use pyo3::prelude::*;

use gnitz_core::local_server::{self, LocalServer};

/// A running gnitz-server. `target` is what `gnitz.connect` takes; `stop`, or
/// leaving a `with` block, stops it.
#[pyclass(name = "LocalServer", module = "gnitz")]
pub struct PyLocalServer {
    server: LocalServer,
}

#[pymethods]
impl PyLocalServer {
    /// Run the server at `binary`, and return once it serves. With no
    /// `data_dir` its data goes with it when it stops.
    #[new]
    #[pyo3(signature = (binary, data_dir = None, workers = 1))]
    fn new(py: Python<'_>, binary: PathBuf, data_dir: Option<PathBuf>, workers: u32) -> PyResult<Self> {
        py.detach(|| LocalServer::start(&binary, data_dir.as_deref(), workers))
            .map(|server| PyLocalServer { server })
            .map_err(PyRuntimeError::new_err)
    }

    #[getter]
    fn target(&self) -> String {
        self.server.socket().to_string_lossy().into_owned()
    }

    #[getter]
    fn data_dir(&self) -> PathBuf {
        self.server.data_dir().to_path_buf()
    }

    #[getter]
    fn running(&mut self) -> bool {
        self.server.running()
    }

    /// Stop the server, which checkpoints first, and remove its socket, its
    /// log and a throwaway server's data. Raises if the server had already
    /// exited by itself, quoting its log. Idempotent.
    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.server.stop()).map_err(PyRuntimeError::new_err)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<bool> {
        self.stop(py)?;
        Ok(false)
    }

    fn __repr__(&mut self) -> String {
        let state = if self.server.running() { "running" } else { "stopped" };
        format!(
            "<LocalServer {} on {} ({state})>",
            self.server.socket().display(),
            self.server.data_dir().display()
        )
    }
}

/// The gnitz-server to run: `GNITZ_SERVER_BIN`'s, else the one in `installed`,
/// else the first on `PATH`.
#[pyfunction]
#[pyo3(signature = (installed = None))]
pub fn find_server(installed: Option<PathBuf>) -> PyResult<String> {
    local_server::find_server(installed.as_deref())
        .map(|bin| bin.to_string_lossy().into_owned())
        .map_err(PyFileNotFoundError::new_err)
}
