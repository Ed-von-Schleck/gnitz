//! The client classes: every verb, written once, and the two classes that run
//! them — `GnitzClient` on the calling thread, `AsyncGnitzClient` on an asyncio
//! event loop.
//!
//! How a verb waits is `drive`'s. Every schema, key and row it hands to the
//! wire is encoded by `write`, and every reply decoded by `read`; nothing here
//! dispatches on a column type.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::IntoPyObjectExt;

use gnitz_core::{
    retraction_batch, BoxFut, ClientError, DeltaCursor, GnitzClient, PollOutcome, PollResult, Pushed, RelName,
    ScanReply, Sent,
};
use gnitz_mirror::{Mirror, MirrorConfig};
use gnitz_sql::{GnitzSqlError, SqlResult};
use gnitz_wire::{KeyRange, PkColList, ReadBound, ReadSpec};
use gnitz_wire::{TableProps, ViewProps, WireConflictMode};

use crate::client_err;
use crate::drive::{Blocking, LoopHandle, PyPipeline};
use crate::read::scan_result;
use crate::schema::PySchema;
use crate::write::{pk_point_spec, py_key_image, py_pks_to_column, PyZSetBatch};

/// What one view's poll did. `PollOutcome` flattened for Python, which has no
/// cheap payload-carrying enum.
#[pyclass(name = "PollResult", frozen, get_all)]
pub struct PyPollResult {
    /// The relation's server id, which a recreated view moves.
    view_id: u64,
    /// The copy was discarded and re-read whole — the discontinuity a subscriber
    /// has to react to, which no cursor carries.
    reseeded: bool,
    /// The exception this one view's poll failed with, or `None`.
    error: Option<Py<PyAny>>,
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
                PollResult::Failed(e) => Some(client_err(e).into_value(py).into_any()),
                _ => None,
            },
        }
    }
}

/// What a sync handed one subscription. `gnitz_core::Pushed` flattened for
/// Python, as [`PyPollResult`] is.
#[pyclass(name = "Pushed", frozen, get_all)]
pub struct PyPushed {
    /// The id `subscribe` returned.
    sub: u64,
    /// The deltas pushed since the last sync, weights and all; `None` once
    /// the subscription ended.
    rows: Option<Py<PyAny>>,
    /// `(tag, tick)` past `rows`; `None` once the subscription ended.
    cursor: Option<(u64, u64)>,
    /// The exception the subscription ended with, or `None`.
    error: Option<Py<PyAny>>,
}

impl PyPushed {
    fn new(py: Python<'_>, pushed: Pushed) -> PyResult<PyPushed> {
        let sub = pushed.sub;
        Ok(match pushed.result {
            Ok((reply, cursor)) => PyPushed {
                sub,
                rows: Some(scan_result(py, reply)?.into_any()),
                cursor: Some(cursor.pair()),
                error: None,
            },
            Err(e) => PyPushed {
                sub,
                rows: None,
                cursor: None,
                error: Some(client_err(e).into_value(py).into_any()),
            },
        })
    }
}

#[pymethods]
impl PyPushed {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let cursor = self
            .cursor
            .map_or("None".to_string(), |(tag, tick)| format!("({tag}, {tick})"));
        let show = |value: &Option<Py<PyAny>>| match value {
            Some(v) => Ok::<_, PyErr>(v.bind(py).repr()?.to_string()),
            None => Ok("None".to_string()),
        };
        Ok(format!(
            "Pushed(sub={}, rows={}, cursor={cursor}, error={})",
            self.sub,
            show(&self.rows)?,
            show(&self.error)?,
        ))
    }
}

#[pymethods]
impl PyPollResult {
    /// Rendered the way Python prints the same values, not the way Rust does.
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let cursor = self
            .cursor
            .map_or("None".to_string(), |(tag, tick)| format!("({tag}, {tick})"));
        let error = match &self.error {
            Some(e) => e.bind(py).repr()?.to_string(),
            None => "None".to_string(),
        };
        Ok(format!(
            "PollResult(view_id={}, reseeded={}, cursor={cursor}, error={error})",
            self.view_id,
            if self.reseeded { "True" } else { "False" },
        ))
    }
}

/// How a client waits, which is the whole difference between its two classes.
pub(crate) enum Mode {
    Closed,
    Blocking(Box<Blocking>),
    Loop(LoopHandle),
}

/// The verbs both client classes inherit. A verb's value is its result on a
/// `GnitzClient`, a future of it on an `AsyncGnitzClient`, and a `Pending`
/// inside `GnitzClient.pipeline()`.
#[pyclass(name = "_Client", subclass)]
pub struct PyClient {
    /// A `Sync` shim: `#[pyclass]` demands `Sync` and a client is `Send` only,
    /// as its host is. Never locked — [`Self::mode`] is the only way in.
    mode: Mutex<Mode>,
    /// The schema of a relation named without one, in SQL and in every verb
    /// that takes a name.
    #[pyo3(get, set)]
    schema: String,
}

/// A verb the client finishes itself: `body` runs with the client `c` to
/// itself, and its value is the verb's.
macro_rules! whole {
    (|$c:ident| $body:expr) => {
        move |$c| Box::pin(async move { Ok(Sent::ready(Ok($body))) })
    };
}

/// A verb that is one request: `pending` is submitted, and its reply awaited
/// with the client already free for the next call.
macro_rules! single {
    (|$c:ident| $pending:expr) => {
        move |$c| Box::pin(async move { Ok($pending.detach()) })
    };
}

impl PyClient {
    fn mode(&mut self) -> &mut Mode {
        self.mode.get_mut().unwrap_or_else(PoisonError::into_inner)
    }

    /// The blocking client: an error on a closed one, and on a client that
    /// lives on an event loop.
    pub(crate) fn blocking(&mut self) -> PyResult<&mut Blocking> {
        match self.mode() {
            Mode::Blocking(blocking) => Ok(blocking),
            Mode::Closed => Err(client_err(ClientError::Closed)),
            Mode::Loop(_) => Err(PyTypeError::new_err(
                "this client lives on an event loop; await its verbs",
            )),
        }
    }

    /// Run one verb: `op` on the client, and `convert` on what it yields.
    fn run<T, O, C>(slf: &Bound<'_, Self>, op: O, convert: C) -> PyResult<Py<PyAny>>
    where
        T: Send + 'static,
        O: for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, Result<Sent<T>, GnitzSqlError>> + Send + 'static,
        C: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        match slf.try_borrow_mut()?.mode() {
            Mode::Closed => Err(client_err(ClientError::Closed)),
            Mode::Blocking(blocking) => blocking.run(slf, op, convert),
            Mode::Loop(handle) => handle.submit(slf.py(), op, convert),
        }
    }

    /// Answer out of the client's memory. A blocking client does so in place,
    /// GIL held; one on an event loop is reached in its turn, like any call.
    fn peek<T, R, C>(slf: &Bound<'_, Self>, read: R, convert: C) -> PyResult<Py<PyAny>>
    where
        T: Send + 'static,
        R: FnOnce(&mut GnitzClient) -> T + Send + 'static,
        C: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        if let Mode::Blocking(blocking) = slf.try_borrow_mut()?.mode() {
            return convert(slf.py(), read(&mut blocking.client));
        }
        Self::run(slf, whole!(|c| read(c)), convert)
    }

    fn schema_name(slf: &Bound<'_, Self>) -> PyResult<String> {
        Ok(slf.try_borrow()?.schema.clone())
    }

    /// The relation a verb's `name` or `schema.name` argument names.
    fn relation(slf: &Bound<'_, Self>, text: &str) -> PyResult<RelName> {
        RelName::parse(&slf.try_borrow()?.schema, text).map_err(|e| client_err(e.into()))
    }
}

/// The handle is gone: a client on an event loop closes once it owes nothing.
impl Drop for PyClient {
    fn drop(&mut self) {
        if let Mode::Loop(handle) = self.mode() {
            Python::attach(|py| handle.release(py));
        }
    }
}

/// The spec of a delta read that names none: the view whole.
fn whole_view() -> Vec<u8> {
    gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None).encode()
}

/// A poll's `wait`, given in seconds.
fn poll_wait(seconds: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(seconds)
        .map_err(|_| PyValueError::new_err(format!("wait must be a non-negative number of seconds, not {seconds}")))
}

fn none(py: Python<'_>, _: ()) -> PyResult<Py<PyAny>> {
    Ok(py.None())
}

fn scanned(py: Python<'_>, reply: ScanReply) -> PyResult<Py<PyAny>> {
    Ok(scan_result(py, reply)?.into_any())
}

fn delta(py: Python<'_>, (reply, cursor): (ScanReply, DeltaCursor)) -> PyResult<Py<PyAny>> {
    (scan_result(py, reply)?, cursor.pair()).into_py_any(py)
}

#[pymethods]
impl PyClient {
    /// Request frames this connection has written — what a test asserts a
    /// mirrored read does not move. Answers on a poisoned store.
    #[getter]
    fn requests_sent(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::peek(slf, |c| c.requests_sent(), |py, n| n.into_py_any(py))
    }

    // ----- DDL -----

    fn create_schema(slf: &Bound<'_, Self>, name: String) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.create_schema(&name).await?), |py, id| {
            id.into_py_any(py)
        })
    }

    fn drop_schema(slf: &Bound<'_, Self>, name: String) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.drop_schema(&name).await?), none)
    }

    /// create_table(table_name, schema). Partitioned, default distribution; no
    /// inline UNIQUE surface.
    fn create_table(slf: &Bound<'_, Self>, table_name: String, schema: PySchema) -> PyResult<Py<PyAny>> {
        let table = Self::relation(slf, &table_name)?;
        Self::run(
            slf,
            whole!(|c| {
                c.create_table(&table, &schema.rust, &[], TableProps::default(), &[])
                    .await?
            }),
            |py, tid| tid.into_py_any(py),
        )
    }

    fn drop_table(slf: &Bound<'_, Self>, table_name: String) -> PyResult<Py<PyAny>> {
        let table = Self::relation(slf, &table_name)?;
        Self::run(slf, whole!(|c| c.drop_table(&[table], false).await?), none)
    }

    // ----- DML -----

    /// push(target_id, batch, mode="update") -> ingest_lsn: int.
    ///
    /// `"update"` silently upserts on a PK conflict (DBSP z-set retraction
    /// semantics); `"error"` rejects the batch, as SQL `INSERT` does. Inside a
    /// transaction the batch is buffered and the returned LSN is 0. The batch
    /// is pushed as it is at the call: appending to it afterwards changes
    /// nothing already sent.
    #[pyo3(signature = (target_id, batch, mode = "update"))]
    fn push(slf: &Bound<'_, Self>, target_id: u64, batch: PyRef<'_, PyZSetBatch>, mode: &str) -> PyResult<Py<PyAny>> {
        let mode: WireConflictMode = mode.parse().map_err(|e: String| PyValueError::new_err(e))?;
        let (schema, rows) = (Arc::clone(&batch.schema), Arc::clone(&batch.batch));
        drop(batch);
        Self::run(slf, single!(|c| c.push(target_id, &schema, &*rows, mode)), |py, lsn| {
            lsn.into_py_any(py)
        })
    }

    /// delete(target_id, schema, pks) — `pks` is a list where each element is the key's value
    /// (single-column PK) or a tuple of its column values (compound PK).
    fn delete(
        slf: &Bound<'_, Self>,
        target_id: u64,
        schema: PySchema,
        pks: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let batch = retraction_batch(&schema.rust, py_pks_to_column(&schema.rust, &pks)?);
        Self::run(
            slf,
            single!(|c| c.push(target_id, &schema.rust, batch, WireConflictMode::Update)),
            |py, _lsn| Ok(py.None()),
        )
    }

    /// A context manager around one transaction of this client's: a clean exit
    /// commits it under one durable zone LSN, an exception discards it.
    ///
    /// ```python
    /// with client.transaction() as txn:
    ///     txn.push(orders_tid, orders_batch)
    ///     txn.delete(carts_tid, cart_schema, [pk])
    /// ```
    ///
    /// On an `AsyncGnitzClient` it is entered with `async with`.
    fn transaction(slf: &Bound<'_, Self>) -> PyTxn {
        PyTxn { client: slf.clone().unbind() }
    }

    // ----- Views -----

    /// create_view(view_name, source_name) — a passthrough view, whose schema
    /// is its source's.
    fn create_view(slf: &Bound<'_, Self>, view_name: String, source_name: String) -> PyResult<Py<PyAny>> {
        let view = Self::relation(slf, &view_name)?;
        let source = Self::relation(slf, &source_name)?;
        Self::run(
            slf,
            whole!(|c| {
                let source = c.resolve_relation(&source).await?;
                c.create_view(&view, &source, ViewProps::default()).await?
            }),
            |py, vid| vid.into_py_any(py),
        )
    }

    fn drop_view(slf: &Bound<'_, Self>, view_name: String) -> PyResult<Py<PyAny>> {
        let view = Self::relation(slf, &view_name)?;
        Self::run(slf, whole!(|c| c.drop_view(&[view], false).await?), none)
    }

    /// resolve_table(table_name) -> (tid: int, schema: Schema)
    fn resolve_table(slf: &Bound<'_, Self>, table_name: String) -> PyResult<Py<PyAny>> {
        let table = Self::relation(slf, &table_name)?;
        Self::run(slf, whole!(|c| c.resolve_relation(&table).await?), |py, rel| {
            (rel.tid, PySchema { rust: Arc::clone(&rel.schema) }).into_py_any(py)
        })
    }

    /// scan(target_id, schema) -> ScanResult
    ///
    /// Every row of the relation in `schema`'s layout, off this client's local
    /// copy if it mirrors one. `lsn` is `None` for a local answer; a copy's
    /// freshness is `cursor(view_id)`.
    fn scan(slf: &Bound<'_, Self>, target_id: u64, schema: PySchema) -> PyResult<Py<PyAny>> {
        let spec = ReadSpec::all_rows(ReadBound::None);
        Self::run(
            slf,
            single!(|c| c.scan_spec_local_first(target_id, spec, &schema.rust)),
            scanned,
        )
    }

    /// subscription(sql) -> (view_id, schema, spec)
    ///
    /// Plan `sql`, one `SELECT` over one view with a delta feed, as what
    /// `delta_bootstrap` and `delta_poll` take to answer with only the rows and
    /// columns it keeps: the view's id, the schema the replies come in, and the
    /// spec to pass. The view's key rides along, hidden where the `SELECT` does
    /// not name it. An aggregate, `DISTINCT`, `ORDER BY` and `LIMIT` are refused.
    fn subscription(slf: &Bound<'_, Self>, sql: String) -> PyResult<Py<PyAny>> {
        let sn = Self::schema_name(slf)?;
        Self::run(
            slf,
            whole!(|c| gnitz_sql::plan_subscription(c, &sn, &sql).await?),
            |py, s| {
                (
                    s.upstream.tid,
                    PySchema { rust: s.schema },
                    pyo3::types::PyBytes::new(py, &s.spec),
                )
                    .into_py_any(py)
            },
        )
    }

    /// delta_bootstrap(view_id, schema, spec=None) -> (rows, cursor)
    ///
    /// The view's whole current value, and the cursor `delta_poll` continues
    /// from. Under a `subscription`'s `spec` and `schema` it is what the
    /// subscription keeps of the view; every later `delta_poll` of that copy
    /// takes the same two.
    #[pyo3(signature = (view_id, schema, spec = None))]
    fn delta_bootstrap(
        slf: &Bound<'_, Self>,
        view_id: u64,
        schema: PySchema,
        spec: Option<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let spec = spec.unwrap_or_else(whole_view);
        Self::run(slf, single!(|c| c.delta_bootstrap(view_id, &schema.rust, &spec)), delta)
    }

    /// delta_poll(view_id, schema, cursor, spec=None) -> (rows, cursor)
    ///
    /// The view's deltas since `cursor`, and the cursor past them: every push
    /// acknowledged before the call is in them. Raises
    /// `GnitzDeltaExpiredError` for a cursor whose rounds are gone, or that is
    /// another boot's, another relation's or another `spec`'s: bootstrap again.
    #[pyo3(signature = (view_id, schema, cursor, spec = None))]
    fn delta_poll(
        slf: &Bound<'_, Self>,
        view_id: u64,
        schema: PySchema,
        cursor: (u64, u64),
        spec: Option<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cursor = DeltaCursor::from_pair(cursor.0, cursor.1)
            .ok_or_else(|| PyValueError::new_err("a delta cursor at tick 0 continues no round; bootstrap"))?;
        let spec = spec.unwrap_or_else(whole_view);
        Self::run(
            slf,
            single!(|c| c.delta_poll(view_id, cursor, &schema.rust, &spec)),
            delta,
        )
    }

    /// subscribe(view_id, schema, cursor, spec=None) -> int
    ///
    /// Subscribe this connection to the view's delta feed from `cursor`, which
    /// `delta_bootstrap` or `delta_poll` handed out under the same `spec`, and
    /// return the subscription's id. `sync_pushed` then answers with the
    /// deltas pushed for it. It ends with its connection.
    #[pyo3(signature = (view_id, schema, cursor, spec = None))]
    fn subscribe(
        slf: &Bound<'_, Self>,
        view_id: u64,
        schema: PySchema,
        cursor: (u64, u64),
        spec: Option<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cursor = DeltaCursor::from_pair(cursor.0, cursor.1)
            .ok_or_else(|| PyValueError::new_err("a delta cursor at tick 0 continues no round; bootstrap"))?;
        let spec = spec.unwrap_or_else(whole_view);
        Self::run(
            slf,
            whole!(|c| c.subscribe(view_id, cursor, &schema.rust, &spec)?),
            |py, id| id.into_py_any(py),
        )
    }

    /// unsubscribe(sub) -> None
    ///
    /// End a subscription. An id this client does not hold is ignored.
    fn unsubscribe(slf: &Bound<'_, Self>, sub: u64) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.unsubscribe(sub)), none)
    }

    /// sync_pushed(wait=0.0) -> list[Pushed]
    ///
    /// One entry per subscription of this client, in the order they were
    /// made: the deltas pushed for it since the last sync and the cursor past
    /// them, every push acknowledged before the call among them. With nothing
    /// to report, the server holds the reply until a round leaves a
    /// subscription a row it keeps, or `wait` seconds pass.
    ///
    /// A subscription that ended carries its exception in `error` and is
    /// gone: `delta_poll` from its last cursor, and subscribe again from there.
    #[pyo3(signature = (wait = 0.0))]
    fn sync_pushed(slf: &Bound<'_, Self>, wait: f64) -> PyResult<Py<PyAny>> {
        let wait = poll_wait(wait)?;
        let convert = |py: Python<'_>, pushed: Vec<Pushed>| {
            let results: PyResult<Vec<PyPushed>> = pushed.into_iter().map(|p| PyPushed::new(py, p)).collect();
            results?.into_py_any(py)
        };
        // On an event loop the client is free while the server holds the
        // sync.
        if let Mode::Loop(handle) = slf.try_borrow_mut()?.mode() {
            return handle.submit_then(
                slf.py(),
                move |c| Ok(c.begin_sync_pushed(wait)),
                |c, mark, synced| Box::pin(async move { Ok(Sent::ready(Ok(c.finish_sync_pushed(mark, synced)))) }),
                convert,
            );
        }
        Self::run(slf, whole!(|c| c.sync_pushed(wait).await?), convert)
    }

    /// scan_many(pairs) -> list[ScanResult]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, in request
    /// order: an atomic multi-table transaction is never observed torn across it.
    /// `pairs` is a list of `(table_id, schema)`.
    fn scan_many(slf: &Bound<'_, Self>, pairs: Vec<(u64, PySchema)>) -> PyResult<Py<PyAny>> {
        let relations = pairs.into_iter().map(|(tid, s)| (tid, s.rust)).collect();
        Self::run(slf, single!(|c| c.scan_many(relations)), |py, replies| {
            let results: PyResult<Vec<_>> = replies.into_iter().map(|reply| scan_result(py, reply)).collect();
            results?.into_py_any(py)
        })
    }

    /// seek(table_id, schema, pk) -> ScanResult.
    ///
    /// The rows keyed `pk` — a single-column key's value, or a compound key's
    /// tuple of column values in PK order — read where `scan` reads.
    fn seek(slf: &Bound<'_, Self>, table_id: u64, schema: PySchema, pk: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let spec = pk_point_spec(&schema.rust, &pk)?;
        Self::run(
            slf,
            single!(|c| c.scan_spec_local_first(table_id, spec, &schema.rust)),
            scanned,
        )
    }

    /// seek_by_index(table_id, schema, col_indices, key_vals) -> ScanResult.
    ///
    /// The rows whose `col_indices` columns hold `key_vals`, read where `scan`
    /// reads. `key_vals` may stop short of the last column where every column
    /// past it is NOT NULL.
    fn seek_by_index(
        slf: &Bound<'_, Self>,
        table_id: u64,
        schema: PySchema,
        col_indices: Vec<u32>,
        key_vals: Bound<'_, PyList>,
    ) -> PyResult<Py<PyAny>> {
        let cols = PkColList::checked(&col_indices, schema.rust.columns.len()).map_err(|rule| {
            PyValueError::new_err(format!(
                "seek_by_index: {}",
                rule.for_role(gnitz_wire::PkListRole::ColumnList)
            ))
        })?;
        if !(1..=col_indices.len()).contains(&key_vals.len()) {
            return Err(PyValueError::new_err(format!(
                "seek_by_index: key value count {} must be in 1..={}",
                key_vals.len(),
                col_indices.len()
            )));
        }
        let keys = col_indices
            .iter()
            .zip(key_vals.iter())
            .map(|(&c, v)| py_key_image(&schema.rust.columns[c as usize], &v))
            .collect::<PyResult<Vec<u128>>>()?;
        let (&last, eq) = keys.split_last().expect("at least one key value");
        let range = KeyRange::point(cols, eq, last);
        if !range.is_exact(|c| schema.rust.columns[c as usize].is_nullable) {
            return Err(PyValueError::new_err(
                "seek_by_index: the key values stop short of a nullable column, whose NULL rows no walk reaches",
            ));
        }
        let spec = ReadSpec::all_rows(ReadBound::Range(range));
        Self::run(
            slf,
            single!(|c| c.scan_spec_local_first(table_id, spec, &schema.rust)),
            scanned,
        )
    }

    /// execute_sql(sql) -> list of result dicts
    ///
    /// A `SELECT` (and the `EXPLAIN` of one) over a view this client mirrors is
    /// planned and answered against the local copy; every other statement, and
    /// every read of a relation the copy does not hold, runs on the connection.
    fn execute_sql(slf: &Bound<'_, Self>, sql: String) -> PyResult<Py<PyAny>> {
        let sn = Self::schema_name(slf)?;
        Self::run(
            slf,
            whole!(|c| gnitz_sql::execute(c, &sn, &sql).await?),
            sql_results_to_py,
        )
    }

    // ----- Mirroring -----

    /// mirror_at(base_dir)
    ///
    /// Open (or resume) the local copy directory at `base_dir` and read this
    /// client's mirrored views through it. One store per directory, and one per
    /// client: a second call is refused. `close_mirror()` releases it, and so
    /// does closing the client.
    fn mirror_at(slf: &Bound<'_, Self>, base_dir: String) -> PyResult<Py<PyAny>> {
        Self::run(
            slf,
            whole!(|c| {
                let store = c
                    .offload(move || Mirror::open(&base_dir, MirrorConfig::from_env()))
                    .await?
                    .map_err(ClientError::from)?;
                c.attach_mirror(store)?
            }),
            none,
        )
    }

    /// mirror_view(name) -> PollResult
    ///
    /// Register the view and bring its copy up to date; idempotent, and the
    /// result says whether this was a first registration or a reopen. Only a
    /// view created `WITH (delta = '<size>')` can be mirrored.
    ///
    /// A mirrored read answers at the last poll, not at what the server holds
    /// now.
    fn mirror_view(slf: &Bound<'_, Self>, name: String) -> PyResult<Py<PyAny>> {
        let view = Self::relation(slf, &name)?;
        Self::run(slf, whole!(|c| c.mirror_view(&view).await?), |py, outcome| {
            PyPollResult::new(py, outcome).into_py_any(py)
        })
    }

    /// mirror_subscription(alias, sql) -> PollResult
    ///
    /// Mirror what `sql` keeps of one view — a `SELECT` as `subscription` takes
    /// it — as the local relation `_local.<alias>`, and bring its copy up to
    /// date; idempotent as `mirror_view` is. The server sends only those rows
    /// and columns, and the copy carries each of the view's indexes whose
    /// columns the `SELECT` keeps.
    ///
    /// A read of `_local.<alias>` is answered off the copy and never upstream;
    /// the view itself is still read upstream, whole. Several aliases may read
    /// one view. `poll` advances an alias with the mirrored views, and may
    /// plan `sql` again.
    fn mirror_subscription(slf: &Bound<'_, Self>, alias: String, sql: String) -> PyResult<Py<PyAny>> {
        let sn = Self::schema_name(slf)?;
        Self::run(
            slf,
            whole!(|c| gnitz_sql::mirror_subscription(c, &sn, &alias, &sql).await?),
            |py, outcome| PyPollResult::new(py, outcome).into_py_any(py),
        )
    }

    /// forget_view(view_id) — stop mirroring the relation. The copy goes, and a
    /// later read of it is delegated upstream.
    fn forget_view(slf: &Bound<'_, Self>, view_id: u64) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.forget_view(view_id).await?), none)
    }

    /// poll(wait=0.0) -> list[PollResult]
    ///
    /// Advance every registered view and alias by one poll each — one entry per
    /// copy, whatever happened to it. A view that failed carries its exception in
    /// `error`; the others went on. It raises only for a failure of the call
    /// itself: no store attached, a poisoned one, or a `KeyboardInterrupt`.
    ///
    /// The copies then hold every push acknowledged before the call. With
    /// nothing to report, the server holds the reply until a round leaves a
    /// copy a row it keeps, or `wait` seconds pass.
    #[pyo3(signature = (wait = 0.0))]
    fn poll(slf: &Bound<'_, Self>, wait: f64) -> PyResult<Py<PyAny>> {
        let wait = poll_wait(wait)?;
        let convert = |py: Python<'_>, outcomes: Vec<PollOutcome>| {
            let results: Vec<PyPollResult> = outcomes.into_iter().map(|o| PyPollResult::new(py, o)).collect();
            results.into_py_any(py)
        };
        // On an event loop the client is free while the server holds the
        // poll's one request.
        if let Mode::Loop(handle) = slf.try_borrow_mut()?.mode() {
            return handle.submit_then(
                slf.py(),
                move |c| c.begin_poll_mirror(wait),
                |c, poll, synced| {
                    Box::pin(async move { Ok(Sent::ready(Ok(c.finish_poll_mirror(poll, synced).await?))) })
                },
                convert,
            );
        }
        Self::run(slf, whole!(|c| c.poll_mirror(wait).await?), convert)
    }

    /// Make every copy and its cursor durable. A failure leaves the store
    /// usable, so a retry is sound.
    fn checkpoint(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.checkpoint_mirror().await?), none)
    }

    /// close_mirror()
    ///
    /// Checkpoint (unless poisoned) and release the store, reporting the final
    /// checkpoint rather than letting the destructor swallow it. The connection
    /// stays open and `mirror_at` may be called again. It is also the only
    /// recovery from a poisoned copy.
    fn close_mirror(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.close_mirror().await?), none)
    }

    /// Every registration this client holds, valid copy or not — wider than the
    /// views `cursor` answers for by the ones a poll has yet to seed.
    fn mirrored_ids(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::peek(slf, |c| c.mirrored_ids(), |py, ids| ids.into_py_any(py))
    }

    /// cursor(view_id) -> (tag, tick) | None
    ///
    /// The round a local read of `view_id` answers at, or `None` when there is no
    /// valid copy. The tick is the master's global round counter, shared by every
    /// relation, so it advances over rounds that carried this view nothing —
    /// whether a copy changed is `PollResult.reseeded`, not this.
    fn cursor(slf: &Bound<'_, Self>, view_id: u64) -> PyResult<Py<PyAny>> {
        Self::peek(
            slf,
            move |c| c.cursor_of(view_id).map(DeltaCursor::pair),
            |py, cursor| cursor.into_py_any(py),
        )
    }

    /// The message that poisoned this client's copy, or `None`. Answers on a
    /// poisoned store — diagnosing one is what it is for.
    #[getter]
    fn mirror_poisoned(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::peek(slf, |c| c.mirror_poisoned(), |py, why| why.into_py_any(py))
    }

    /// reconnect(target)
    ///
    /// Replace the connection, keeping every mirrored copy — what a host does
    /// after a server restart. Refused inside a transaction. Every copy stops
    /// answering reads until the next poll, so a read before it goes upstream.
    fn reconnect(slf: &Bound<'_, Self>, target: String) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.reconnect(&target).await?), none)
    }
}

/// One `SqlResult` per statement as a dict — the shape every SQL entry point
/// hands back.
fn sql_results_to_py(py: Python<'_>, results: Vec<SqlResult>) -> PyResult<Py<PyAny>> {
    let k_type = pyo3::intern!(py, "type");
    let dicts = results.into_iter().map(|r| {
        let d = PyDict::new(py);
        match r {
            SqlResult::Ddl => {
                d.set_item(k_type, pyo3::intern!(py, "Ddl"))?;
            }
            SqlResult::RowsAffected { count } => {
                d.set_item(k_type, pyo3::intern!(py, "RowsAffected"))?;
                d.set_item(pyo3::intern!(py, "count"), count)?;
            }
            SqlResult::Rows(reply) => {
                d.set_item(k_type, pyo3::intern!(py, "Rows"))?;
                d.set_item(pyo3::intern!(py, "rows"), scan_result(py, reply)?)?;
            }
            SqlResult::TransactionStarted => {
                d.set_item(k_type, pyo3::intern!(py, "TransactionStarted"))?;
            }
            SqlResult::TransactionCommitted { lsn } => {
                d.set_item(k_type, pyo3::intern!(py, "TransactionCommitted"))?;
                d.set_item(pyo3::intern!(py, "lsn"), lsn)?;
            }
            SqlResult::TransactionRolledBack => {
                d.set_item(k_type, pyo3::intern!(py, "TransactionRolledBack"))?;
            }
        }
        Ok(d)
    });
    dicts.collect::<PyResult<Vec<_>>>()?.into_py_any(py)
}

// ---------------------------------------------------------------------------
// The blocking class
// ---------------------------------------------------------------------------

/// A connection whose verbs return once they are answered.
#[pyclass(name = "GnitzClient", extends = PyClient)]
pub struct PyGnitzClient;

#[pymethods]
impl PyGnitzClient {
    #[new]
    #[pyo3(signature = (target, schema = "public"))]
    fn new(py: Python<'_>, target: &str, schema: &str) -> PyResult<PyClassInitializer<Self>> {
        let base = PyClient {
            mode: Mutex::new(Mode::Blocking(Box::new(Blocking::connect(py, target)?))),
            schema: schema.to_string(),
        };
        Ok(PyClassInitializer::from(base).add_subclass(PyGnitzClient))
    }

    /// Close the connection, checkpointing and releasing the mirror store first,
    /// and raise if that final checkpoint failed; the connection closes either
    /// way. Idempotent. The GIL goes down for it: the checkpoint is fsync-bound.
    fn close(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let base: &mut PyClient = slf.as_super();
        match std::mem::replace(base.mode(), Mode::Closed) {
            Mode::Blocking(mut blocking) => blocking.call(py, async |c| c.close_mirror().await),
            Mode::Closed | Mode::Loop(_) => Ok(()),
        }
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        _exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<bool> {
        Self::close(slf, py)?;
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
// The event-loop class
// ---------------------------------------------------------------------------

/// A connection on the running asyncio event loop: every verb submits when
/// called and returns a future of its result. It is connected when built, so
/// awaiting it yields itself, and `async with` closes it on exit.
#[pyclass(name = "AsyncGnitzClient", extends = PyClient)]
pub struct PyAsyncClient;

impl PyAsyncClient {
    fn on_loop<R>(slf: &Bound<'_, Self>, f: impl FnOnce(&LoopHandle) -> PyResult<R>) -> PyResult<R> {
        match slf.as_super().try_borrow_mut()?.mode() {
            Mode::Loop(handle) => f(handle),
            Mode::Closed | Mode::Blocking(_) => unreachable!("this class is built on an event loop and stays there"),
        }
    }
}

#[pymethods]
impl PyAsyncClient {
    /// Connect on the calling thread, and serve on the running loop.
    #[new]
    #[pyo3(signature = (target, schema = "public"))]
    fn new(py: Python<'_>, target: &str, schema: &str) -> PyResult<PyClassInitializer<Self>> {
        let base = PyClient {
            mode: Mutex::new(Mode::Loop(LoopHandle::connect(py, target)?)),
            schema: schema.to_string(),
        };
        Ok(PyClassInitializer::from(base).add_subclass(PyAsyncClient))
    }

    /// Checkpoint and release the mirror store behind every call already
    /// made, then close the connection, whether or not that checkpoint failed.
    /// Awaits to `None`; idempotent.
    fn aclose(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::on_loop(slf, |handle| {
            handle.finish(slf.py(), whole!(|c| c.close_mirror().await?))
        })
    }

    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::on_loop(slf, |handle| handle.settled(slf.py(), slf.clone().into_any().unbind()))
    }

    /// `await connect(p)`, beside `async with connect(p)`.
    fn __await__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::__aenter__(slf)?.call_method0(slf.py(), pyo3::intern!(slf.py(), "__await__"))
    }

    /// `aclose()`, whose `None` lets the block's exception through.
    fn __aexit__(
        slf: &Bound<'_, Self>,
        _exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        Self::aclose(slf)
    }
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

/// The context manager `transaction()` returns: entering it opens the client's
/// transaction and hands back the client.
#[pyclass(name = "Txn", frozen, generic)]
pub struct PyTxn {
    client: Py<PyClient>,
}

#[pymethods]
impl PyTxn {
    fn __enter__(&self, py: Python<'_>) -> PyResult<Py<PyClient>> {
        let mut client = self.client.bind(py).try_borrow_mut()?;
        client.blocking()?.client.txn_begin().map_err(client_err)?;
        Ok(self.client.clone_ref(py))
    }

    /// Ends the transaction and lets the block's exception through.
    fn __exit__(&self, py: Python<'_>, exc_type: Py<PyAny>, _exc_val: Py<PyAny>, _exc_tb: Py<PyAny>) -> PyResult<bool> {
        let mut client = self.client.bind(py).try_borrow_mut()?;
        let clean = exc_type.is_none(py);
        match client.blocking() {
            Ok(blocking) => blocking.call(py, async |c| end_txn(c, clean).await)?,
            // The block's own exception outranks a client it left unusable.
            Err(_) if !clean => {}
            Err(e) => return Err(e),
        }
        Ok(false)
    }

    fn __aenter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let client = self.client.bind(py);
        self.on_loop(client)?;
        let entered = self.client.clone_ref(py);
        PyClient::run(client, whole!(|c| c.txn_begin()?), move |_, ()| Ok(entered.into_any()))
    }

    /// As `__exit__`, awaited.
    fn __aexit__(
        &self,
        py: Python<'_>,
        exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let client = self.client.bind(py);
        self.on_loop(client)?;
        let clean = exc_type.is_none(py);
        PyClient::run(client, whole!(|c| end_txn(c, clean).await?), |py, ()| {
            false.into_py_any(py)
        })
    }
}

/// How a transaction block ends: a clean exit commits, and an exception
/// discards whatever transaction is still open.
async fn end_txn(c: &mut GnitzClient, clean: bool) -> Result<(), ClientError> {
    if clean {
        c.txn_commit().await?;
    } else if c.txn_active() {
        c.txn_rollback()?;
    }
    Ok(())
}

impl PyTxn {
    /// `async with` is for a client on an event loop.
    fn on_loop(&self, client: &Bound<'_, PyClient>) -> PyResult<()> {
        match client.try_borrow_mut()?.mode() {
            Mode::Blocking(_) => Err(PyTypeError::new_err(
                "this client blocks; enter its transaction with `with`",
            )),
            Mode::Closed | Mode::Loop(_) => Ok(()),
        }
    }
}
