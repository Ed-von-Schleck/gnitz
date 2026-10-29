//! `GnitzClient`: a client whose verbs run on the calling thread with the GIL
//! released, and its pipeline, inside which a verb returns a `Pending` with its
//! request on its way.

use std::sync::{Arc, Mutex};
use std::task::Poll;

use pyo3::prelude::*;

use gnitz_core::{block_on, BlockingHost, ClientError, GnitzClient, Sent};
use gnitz_sql::GnitzSqlError;

use crate::client::{resolve_target, Mode, PyClient, Unconverted, Verb};
use crate::{client_err, gnitz_err, lock, sql_err};

/// A client driven on the calling thread.
pub(crate) struct Blocking {
    pub(crate) client: GnitzClient,
    /// The replies an open pipeline has yet to wait for, in call order.
    pipeline: Option<Vec<Slot>>,
}

impl Blocking {
    /// A connection whose waits check for Ctrl-C.
    fn connect(py: Python<'_>, target: &str) -> PyResult<Self> {
        let host = BlockingHost::with_hook(Box::new(|| Python::attach(|py| py.check_signals()).map_err(Into::into)));
        let client = py
            .detach(|| block_on(GnitzClient::connect_with(target, Box::new(host))))
            .map_err(client_err)?;
        Ok(Blocking { client, pipeline: None })
    }

    /// Run `f` on the client with the GIL released.
    pub(crate) fn call<T: Send, E: Into<GnitzSqlError> + Send>(
        &mut self,
        py: Python<'_>,
        f: impl AsyncFnOnce(&mut GnitzClient) -> Result<T, E> + Send,
    ) -> PyResult<T> {
        let client = &mut self.client;
        py.detach(|| block_on(f(client))).map_err(|e| sql_err(e.into()))
    }

    /// Run one verb: wait for its reply and convert it, or inside a pipeline,
    /// send its request and hand back the `Pending` its reply lands in. The
    /// verb's own failure raises here either way.
    pub(crate) fn run(&mut self, slf: &Bound<'_, PyClient>, verb: Verb) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let Some(pipeline) = &mut self.pipeline else {
            let reply = self.call(py, async move |c| {
                let sent = verb(c).await?;
                Ok::<_, GnitzSqlError>(c.wait(sent).await?)
            })?;
            return reply(py);
        };
        let client = &mut self.client;
        let sent = py
            .detach(|| {
                block_on(async move {
                    client.make_room().await?;
                    verb(client).await
                })
            })
            .map_err(sql_err)?;
        let slot = Arc::new(Mutex::new(Reply::Waiting(sent)));
        pipeline.push(Arc::clone(&slot));
        Ok(Py::new(py, PyPending { client: slf.clone().unbind(), slot })?.into_any())
    }
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
        let blocking = Blocking::connect(py, &resolve_target(target)?)?;
        Ok(PyClient::init(Mode::Blocking(Mutex::new(Some(Box::new(blocking)))), schema).add_subclass(PyGnitzClient))
    }

    /// Close the connection, checkpointing and releasing the mirror store first,
    /// and raise if that final checkpoint failed; the connection closes either
    /// way. Idempotent. The GIL goes down for it: the checkpoint is fsync-bound.
    fn close(slf: &Bound<'_, Self>) -> PyResult<()> {
        match slf.as_super().get().take_blocking()? {
            Some(mut blocking) => blocking.call(slf.py(), async |c| c.close_mirror().await),
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

    /// A context manager inside which every verb returns a `Pending`, and one
    /// that is a single request returns it with the request queued — so the
    /// block's requests share round trips. The block's name is the client
    /// itself, typed as what it returns there:
    ///
    /// ```python
    /// with client.pipeline() as piped:
    ///     lsns = [piped.push(tid, b) for b in batches]
    /// print([lsn.result() for lsn in lsns])
    /// ```
    fn pipeline(slf: &Bound<'_, Self>) -> PyPipeline {
        PyPipeline { client: slf.as_super().clone().unbind() }
    }
}

// ---------------------------------------------------------------------------
// Pipelining
// ---------------------------------------------------------------------------

/// A pipelined verb's reply. Shared by its `Pending` and the pipeline that
/// waits for it, so the client holds no reference back to its `Pending`s.
type Slot = Arc<Mutex<Reply>>;

enum Reply {
    /// On its way, or read off the connection and converted by the first wait
    /// for it.
    Waiting(Sent<Unconverted>),
    /// The verb's result, and whether `result()` has handed it to anyone.
    Converted(PyResult<Py<PyAny>>, bool),
}

/// The reply in `slot` if it has arrived, taken out to be converted; `None`
/// once it is.
fn arrived(slot: &Mutex<Reply>) -> Poll<Option<Result<Unconverted, ClientError>>> {
    match &mut *lock(slot) {
        Reply::Waiting(sent) => sent.try_take().map_or(Poll::Pending, |reply| Poll::Ready(Some(reply))),
        Reply::Converted(..) => Poll::Ready(None),
    }
}

/// Wait on `client` for `slot`'s reply, if it is still on its way, and convert
/// it. The GIL goes down once for the whole wait, and an interrupted wait
/// leaves the reply waiting, for the next to take up.
fn land(py: Python<'_>, client: &Py<PyClient>, slot: &Mutex<Reply>) -> PyResult<()> {
    let reply = match arrived(slot) {
        Poll::Ready(reply) => reply,
        Poll::Pending => client.get().blocking(|blocking| {
            blocking.call(py, async |c| loop {
                if let Poll::Ready(reply) = arrived(slot) {
                    break Ok::<_, ClientError>(reply);
                }
                c.turn().await?;
            })
        })?,
    };
    if let Some(reply) = reply {
        let converted = reply.map_err(client_err).and_then(|convert| convert(py));
        *lock(slot) = Reply::Converted(converted, false);
    }
    Ok(())
}

/// The result of a verb called inside `client.pipeline()`: its request is on
/// its way, and `result()` waits for the reply.
#[pyclass(name = "Pending", frozen, generic, module = "gnitz")]
pub(crate) struct PyPending {
    client: Py<PyClient>,
    slot: Slot,
}

#[pymethods]
impl PyPending {
    /// The verb's result, waiting for its reply if it has not arrived. Replies
    /// arrive in call order, so this also completes every call made before it.
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        land(py, &self.client, &self.slot)?;
        match &mut *lock(&self.slot) {
            Reply::Converted(outcome, read) => {
                *read = true;
                match outcome {
                    Ok(value) => Ok(value.clone_ref(py)),
                    Err(e) => Err(e.clone_ref(py)),
                }
            }
            Reply::Waiting(_) => unreachable!("landed above"),
        }
    }
}

/// The context manager `GnitzClient.pipeline()` returns.
#[pyclass(name = "Pipeline", frozen, module = "gnitz._native")]
pub(crate) struct PyPipeline {
    client: Py<PyClient>,
}

#[pymethods]
impl PyPipeline {
    fn __enter__(&self, py: Python<'_>) -> PyResult<Py<PyClient>> {
        self.client.get().blocking(|blocking| {
            if blocking.pipeline.is_some() {
                return Err(gnitz_err("a pipeline is already open on this client"));
            }
            blocking.pipeline = Some(Vec::new());
            Ok(())
        })?;
        Ok(self.client.clone_ref(py))
    }

    /// Wait for every reply still outstanding and raise the first failure
    /// `result()` handed to nobody — unless the block itself raised.
    fn __exit__(&self, py: Python<'_>, exc_type: Py<PyAny>, _exc_val: Py<PyAny>, _exc_tb: Py<PyAny>) -> PyResult<bool> {
        let owed = self
            .client
            .get()
            .blocking(|blocking| Ok(blocking.pipeline.take().unwrap_or_default()));
        // The block's own exception outranks a client it left unusable.
        if !exc_type.is_none(py) {
            return Ok(false);
        }
        let owed = owed?;
        // Replies arrive in call order, so the wait for the last brings every
        // one, and the rest only convert.
        for slot in owed.last().into_iter().chain(&owed) {
            land(py, &self.client, slot)?;
        }
        let unread = owed.iter().find_map(|slot| match &*lock(slot) {
            Reply::Converted(Err(e), false) => Some(e.clone_ref(py)),
            _ => None,
        });
        match unread {
            Some(e) => Err(e),
            None => Ok(false),
        }
    }
}
