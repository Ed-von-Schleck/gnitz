//! The blocking client: the `GnitzClient` pyclass and its verbs, the `Txn`
//! context manager, and the `PollResult` a mirror poll reports through.
//!
//! Every schema, key and row it hands to the wire is encoded by `write`, and
//! every reply it hands back is decoded by `read`; nothing here dispatches on a
//! column type.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use gnitz_core::{ClientError, DeltaCursor, GnitzClient, PollOutcome, PollResult, ScanReply, Schema};
use gnitz_mirror::{Mirror, MirrorConfig};
use gnitz_sql::SqlResult;
use gnitz_wire::{KeyRange, PkColList, ReadBound, ReadSpec};
use gnitz_wire::{TableProps, ViewProps, WireConflictMode};

use crate::read::{scan_result, PyDeltaReply, PyScanResult};
use crate::schema::{resolve_py_schema, scan_pairs, PySchema};
use crate::write::{pk_point_spec, py_key_image, py_pks_to_column, PyZSetBatch};
use crate::{client_err, connect_client, sql_err};

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
            cursor: o.cursor.map(|c| (c.tag, c.tick.get())),
            error: match o.result {
                PollResult::Failed(e) => Some(client_err(e).into_value(py).into_any()),
                _ => None,
            },
        }
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

#[pyclass(name = "GnitzClient")]
pub struct PyGnitzClient {
    /// A `Sync` shim: `#[pyclass]` demands `Sync` and a mirroring client is
    /// `Send` only. Never locked — [`Self::slot`] is the only way in.
    inner: Mutex<Option<GnitzClient>>,
    /// The schema this connection's names resolve in: every SQL statement, and
    /// every verb that takes a relation name.
    #[pyo3(get, set)]
    schema: String,
}

impl PyGnitzClient {
    /// The client slot, empty once `close()` has run.
    fn slot(&mut self) -> &mut Option<GnitzClient> {
        self.inner.get_mut().expect("the mutex is never locked")
    }

    /// The still-open client, or a `GnitzConnectionError` if `close()` already ran.
    fn live(&mut self) -> PyResult<&mut GnitzClient> {
        self.slot().as_mut().ok_or_else(|| client_err(ClientError::Closed))
    }

    /// Run one blocking client call: check the client is open, and drop the GIL
    /// across it. What every blocking method takes.
    fn call<T: Send>(
        &mut self,
        py: Python<'_>,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, ClientError> + Send,
    ) -> PyResult<T> {
        let c = self.live()?;
        py.detach(move || f(c)).map_err(client_err)
    }

    /// One relation read under `spec`, off this client's copy when it mirrors `tid`.
    fn read(&mut self, py: Python<'_>, tid: u64, spec: ReadSpec, schema: Arc<Schema>) -> PyResult<Py<PyScanResult>> {
        let reply = self.call(py, |c| c.scan_spec_local_first(tid, spec, &schema))?;
        scan_result(py, reply)
    }
}

#[pymethods]
impl PyGnitzClient {
    #[new]
    #[pyo3(signature = (target, schema = "public"))]
    pub fn new(py: Python<'_>, target: &str, schema: &str) -> PyResult<Self> {
        Ok(PyGnitzClient {
            inner: Mutex::new(Some(connect_client(py, target)?)),
            schema: schema.to_string(),
        })
    }

    /// Request frames this connection has written — what a test asserts a
    /// mirrored read does not move. Answers on a poisoned store.
    #[getter]
    pub fn requests_sent(&mut self) -> PyResult<u64> {
        Ok(self.live()?.requests_sent())
    }

    /// Close the connection, checkpointing and releasing the mirror store first,
    /// and raise if that final checkpoint failed; the connection closes either
    /// way. Idempotent. The GIL goes down for it: the checkpoint is fsync-bound.
    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let taken = self.slot().take();
        py.detach(move || match taken {
            Some(mut client) => client.close_mirror(),
            None => Ok(()),
        })
        .map_err(client_err)
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    pub fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }

    // ----- DDL -----

    pub fn create_schema(&mut self, py: Python<'_>, name: &str) -> PyResult<u64> {
        self.call(py, |c| c.create_schema(name))
    }

    pub fn drop_schema(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        self.call(py, |c| c.drop_schema(name))
    }

    /// create_table(table_name, schema). Partitioned, default distribution; no
    /// inline UNIQUE surface.
    pub fn create_table(
        &mut self,
        py: Python<'_>,
        table_name: &str,
        #[pyo3(from_py_with = resolve_py_schema)] schema: Arc<Schema>,
    ) -> PyResult<u64> {
        let sn = self.schema.clone();
        self.call(py, |c| {
            c.create_table(&sn, table_name, &schema, &[], TableProps::default(), &[])
        })
    }

    pub fn drop_table(&mut self, py: Python<'_>, table_name: &str) -> PyResult<()> {
        let sn = self.schema.clone();
        self.call(py, |c| c.drop_table(&sn, &[table_name], false))
    }

    // ----- DML -----

    /// push(target_id, batch, mode="update") -> ingest_lsn: int.
    ///
    /// `"update"` silently upserts on a PK conflict (DBSP z-set retraction
    /// semantics); `"error"` rejects the batch, as SQL `INSERT` does.
    #[pyo3(signature = (target_id, batch, mode = "update"))]
    pub fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>, mode: &str) -> PyResult<u64> {
        let m: WireConflictMode = mode.parse().map_err(|e: String| PyValueError::new_err(e))?;
        // Hold the `PyRef` guard here (it is `!Ungil`) and pass only the plain
        // `&Schema`/`&ZSetBatch` into the closure, so the GIL is free during
        // the blocking push without cloning the batch.
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        self.call(py, move |c| c.push(target_id, schema, b, m))
    }

    /// delete(target_id, schema, pks) — `pks` is a list where each element is the key's value
    /// (single-column PK) or a tuple of its column values (compound PK).
    pub fn delete(
        &mut self,
        py: Python<'_>,
        target_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] schema: Arc<Schema>,
        pks: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let pk_col = py_pks_to_column(&schema, &pks)?;
        self.call(py, move |c| c.delete(target_id, &schema, pk_col))
    }

    /// A context manager around one transaction of this client's: a clean exit
    /// commits it under one durable zone LSN, an exception discards it.
    ///
    /// ```python
    /// with client.transaction() as txn:
    ///     txn.push(orders_tid, orders_batch)
    ///     txn.delete(carts_tid, cart_schema, [pk])
    /// ```
    pub fn transaction(slf: Py<Self>) -> PyTxn {
        PyTxn { client: slf }
    }

    // ----- Views -----

    /// create_view(view_name, source_table_id) — a passthrough view, whose
    /// schema is its source's.
    pub fn create_view(&mut self, py: Python<'_>, view_name: &str, source_table_id: u64) -> PyResult<u64> {
        let sn = self.schema.clone();
        self.call(py, |c| {
            c.create_view(&sn, view_name, source_table_id, ViewProps::default())
        })
    }

    pub fn drop_view(&mut self, py: Python<'_>, view_name: &str) -> PyResult<()> {
        let sn = self.schema.clone();
        self.call(py, |c| c.drop_view(&sn, &[view_name], false))
    }

    /// resolve_table(table_name) -> (tid: int, schema: Schema)
    pub fn resolve_table(&mut self, py: Python<'_>, table_name: &str) -> PyResult<(u64, PySchema)> {
        let sn = self.schema.clone();
        let rel = self.call(py, |c| c.resolve_relation(&sn, table_name))?;
        Ok((rel.tid, PySchema { rust: Arc::clone(&rel.schema) }))
    }

    /// scan(target_id, schema) -> ScanResult
    ///
    /// Every row of the relation in `schema`'s layout, off this client's local
    /// copy if it mirrors one. `lsn` is `None` for a local answer; a copy's
    /// freshness is `cursor(view_id)`.
    pub fn scan(
        &mut self,
        py: Python<'_>,
        target_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] schema: Arc<Schema>,
    ) -> PyResult<Py<PyScanResult>> {
        self.read(py, target_id, ReadSpec::all_rows(ReadBound::None), schema)
    }

    /// delta_bootstrap(view_id, view_schema) -> DeltaReply
    ///
    /// The view's whole current value, in the view's own schema, plus the cursor
    /// to poll from. Costs what a scan of the view costs. Apply it to a fresh
    /// copy: it replaces state, it does not add to it.
    pub fn delta_bootstrap(
        &mut self,
        py: Python<'_>,
        view_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] view_schema: Arc<Schema>,
    ) -> PyResult<Py<PyDeltaReply>> {
        let (reply, cursor) = self.call(py, |c| c.delta_bootstrap(view_id, &view_schema))?;
        PyDeltaReply::new(py, reply, cursor)
    }

    /// delta_poll(view_id, view_schema, cursor) -> DeltaReply
    ///
    /// Every delta the view emitted since `cursor`, in the view's own schema,
    /// weights and all. `cursor` is the `(tag, tick)` a previous reply handed
    /// back; a tick of 0 is refused. Apply what comes
    /// back and keep the new cursor; there is nothing to filter and nothing to
    /// reconcile.
    ///
    /// A cursor whose rounds are gone, or that names a different boot or
    /// relation, is refused with `GnitzDeltaExpiredError` rather than answered
    /// with the wrong relation's rows. Bootstrap again.
    pub fn delta_poll(
        &mut self,
        py: Python<'_>,
        view_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] view_schema: Arc<Schema>,
        cursor: (u64, u64),
    ) -> PyResult<Py<PyDeltaReply>> {
        let tick = NonZeroU64::new(cursor.1)
            .ok_or_else(|| PyValueError::new_err("a delta cursor at tick 0 continues no round; bootstrap"))?;
        let cursor = DeltaCursor { tag: cursor.0, tick };
        let (reply, cursor) = self.call(py, |c| c.delta_poll(view_id, cursor, &view_schema))?;
        PyDeltaReply::new(py, reply, cursor)
    }

    /// scan_many(pairs) -> list[ScanResult]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, in request
    /// order: an atomic multi-table transaction is never observed torn across it.
    /// `pairs` is a list of `(table_id, schema)`.
    pub fn scan_many(
        &mut self,
        py: Python<'_>,
        pairs: Vec<(u64, Bound<'_, PyAny>)>,
    ) -> PyResult<Vec<Py<PyScanResult>>> {
        let rels = scan_pairs(&pairs)?;
        let results = self.call(py, |c| c.scan_many(rels))?;
        results.into_iter().map(|reply| scan_result(py, reply)).collect()
    }

    /// seek(table_id, schema, pk) -> ScanResult.
    ///
    /// The rows keyed `pk` — a single-column key's value, or a compound key's
    /// tuple of column values in PK order — read where `scan` reads.
    pub fn seek(
        &mut self,
        py: Python<'_>,
        table_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] schema: Arc<Schema>,
        pk: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyScanResult>> {
        let spec = pk_point_spec(&schema, &pk)?;
        self.read(py, table_id, spec, schema)
    }

    /// seek_by_index(table_id, schema, col_indices, key_vals) -> ScanResult.
    ///
    /// The rows whose `col_indices` columns hold `key_vals`, which may stop
    /// short of the last column — read where `scan` reads.
    pub fn seek_by_index(
        &mut self,
        py: Python<'_>,
        table_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] schema: Arc<Schema>,
        col_indices: Vec<u32>,
        key_vals: Bound<'_, PyList>,
    ) -> PyResult<Py<PyScanResult>> {
        gnitz_wire::validate_pk_col_list(&col_indices, schema.columns.len())
            .map_err(|e| PyValueError::new_err(format!("seek_by_index: {e}")))?;
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
            .map(|(&c, v)| py_key_image(&schema.columns[c as usize], &v))
            .collect::<PyResult<Vec<u128>>>()?;
        let (&last, eq) = keys.split_last().expect("at least one key value");
        let range = KeyRange::point(PkColList::from_slice(&col_indices), eq, last);
        self.read(py, table_id, ReadSpec::all_rows(ReadBound::Range(range)), schema)
    }

    /// execute_sql(sql) -> list of result dicts
    ///
    /// A `SELECT` (and the `EXPLAIN` of one) over a view this client mirrors is
    /// planned and answered against the local copy; every other statement, and
    /// every read of a relation the copy does not hold, runs on the connection.
    pub fn execute_sql<'py>(&mut self, py: Python<'py>, sql: &str) -> PyResult<Vec<Bound<'py, PyDict>>> {
        // Plan + execute (all wire I/O, no Python) with the GIL released.
        let sn = self.schema.clone();
        let c = self.live()?;
        let results = py.detach(|| gnitz_sql::execute(c, &sn, sql)).map_err(sql_err)?;
        sql_results_to_py(py, results)
    }

    // ----- Mirroring -----

    /// mirror_at(base_dir)
    ///
    /// Open (or resume) the local copy directory at `base_dir` and read this
    /// client's mirrored views through it. One store per directory, and one per
    /// client: a second call is refused. `close_mirror()` releases it, and so
    /// does closing the client.
    pub fn mirror_at(&mut self, py: Python<'_>, base_dir: &str) -> PyResult<()> {
        let base_dir = base_dir.to_string();
        self.call(py, move |c| {
            c.attach_mirror(Mirror::open(&base_dir, MirrorConfig::from_env())?)
        })
    }

    /// mirror_view(name) -> PollResult
    ///
    /// Register the view and bring its copy up to date; idempotent, and the
    /// result says whether this was a first registration or a reopen. Only a
    /// view created `WITH (delta = '<size>')` can be mirrored.
    ///
    /// A mirrored read answers at the last poll, not at what the server holds
    /// now.
    pub fn mirror_view(&mut self, py: Python<'_>, name: &str) -> PyResult<PyPollResult> {
        let sn = self.schema.clone();
        let outcome = self.call(py, |c| c.mirror_view(&sn, name))?;
        Ok(PyPollResult::new(py, outcome))
    }

    /// forget_view(view_id) — stop mirroring the relation. The copy goes, and a
    /// later read of it is delegated upstream.
    pub fn forget_view(&mut self, py: Python<'_>, view_id: u64) -> PyResult<()> {
        self.call(py, |c| c.forget_view(view_id))
    }

    /// poll() -> list[PollResult]
    ///
    /// Advance every registered view by one poll each — one entry per view,
    /// whatever happened to it. A view that failed carries its exception in
    /// `error`; the others went on. It raises only for a failure of the call
    /// itself: no store attached, a poisoned one, or a `KeyboardInterrupt`.
    ///
    /// A poll drives no tick server-side.
    pub fn poll(&mut self, py: Python<'_>) -> PyResult<Vec<PyPollResult>> {
        let outcomes = self.call(py, GnitzClient::poll_mirror)?;
        Ok(outcomes.into_iter().map(|o| PyPollResult::new(py, o)).collect())
    }

    /// Make every copy and its cursor durable. A failure leaves the store
    /// usable, so a retry is sound.
    pub fn checkpoint(&mut self, py: Python<'_>) -> PyResult<()> {
        self.call(py, GnitzClient::checkpoint_mirror)
    }

    /// close_mirror()
    ///
    /// Checkpoint (unless poisoned) and release the store, reporting the final
    /// checkpoint rather than letting the destructor swallow it. The connection
    /// stays open and `mirror_at` may be called again. It is also the only
    /// recovery from a poisoned copy.
    pub fn close_mirror(&mut self, py: Python<'_>) -> PyResult<()> {
        self.call(py, GnitzClient::close_mirror)
    }

    /// Whether a read of `view_id` is answered locally. Answers on a poisoned
    /// store.
    pub fn mirrors(&mut self, view_id: u64) -> PyResult<bool> {
        Ok(self.live()?.mirrors(view_id))
    }

    /// Every registration this client holds, valid copy or not — wider than
    /// `mirrors` by the ones a poll has yet to seed.
    pub fn mirrored_ids(&mut self) -> PyResult<Vec<u64>> {
        Ok(self.live()?.mirrored_ids())
    }

    /// cursor(view_id) -> (tag, tick) | None
    ///
    /// The round a local read of `view_id` answers at, or `None` when there is no
    /// valid copy. The tick is the master's global round counter, shared by every
    /// relation, so it advances over rounds that carried this view nothing —
    /// whether a copy changed is `PollResult.reseeded`, not this.
    pub fn cursor(&mut self, view_id: u64) -> PyResult<Option<(u64, u64)>> {
        Ok(self.live()?.cursor_of(view_id).map(|c| (c.tag, c.tick.get())))
    }

    /// The message that poisoned this client's copy, or `None`. Answers on a
    /// poisoned store — diagnosing one is what it is for.
    #[getter]
    pub fn mirror_poisoned(&mut self) -> PyResult<Option<String>> {
        Ok(self.live()?.mirror_poisoned().map(Into::into))
    }

    /// reconnect(target)
    ///
    /// Replace the connection, keeping every mirrored copy — what a host does
    /// after a server restart. Refused inside a transaction. Every cursor is
    /// dropped, so a read before the next poll goes upstream and that poll
    /// reports a reseed for every view.
    pub fn reconnect(&mut self, py: Python<'_>, target: &str) -> PyResult<()> {
        self.call(py, |c| c.reconnect(target))
    }
}

/// One `SqlResult` per statement as a dict — the shape every SQL entry point
/// hands back.
fn sql_results_to_py(py: Python<'_>, results: Vec<SqlResult>) -> PyResult<Vec<Bound<'_, PyDict>>> {
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
            SqlResult::Rows { schema, batch } => {
                d.set_item(k_type, pyo3::intern!(py, "Rows"))?;
                d.set_item(
                    pyo3::intern!(py, "rows"),
                    scan_result(py, ScanReply { schema, batch, lsn: None })?,
                )?;
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
    dicts.collect()
}

/// The context manager `GnitzClient.transaction()` returns: `__enter__` opens the
/// client's transaction and hands back the client.
#[pyclass(name = "Txn", frozen)]
pub struct PyTxn {
    client: Py<PyGnitzClient>,
}

#[pymethods]
impl PyTxn {
    fn __enter__(&self, py: Python<'_>) -> PyResult<Py<PyGnitzClient>> {
        self.client
            .bind(py)
            .try_borrow_mut()?
            .live()?
            .txn_begin()
            .map_err(client_err)?;
        Ok(self.client.clone_ref(py))
    }

    /// Commit on a clean exit. If the block raised, discard whatever transaction is
    /// still open and let the exception through.
    fn __exit__(&self, py: Python<'_>, exc_type: Py<PyAny>, _exc_val: Py<PyAny>, _exc_tb: Py<PyAny>) -> PyResult<bool> {
        let mut client = self.client.bind(py).try_borrow_mut()?;
        if exc_type.is_none(py) {
            client.call(py, |c| c.txn_commit().map(|_| ()))?;
        } else if let Some(c) = client.slot().as_mut().filter(|c| c.txn_active()) {
            c.txn_rollback().map_err(client_err)?;
        }
        Ok(false)
    }
}
