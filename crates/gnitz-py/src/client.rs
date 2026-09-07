//! The blocking client: the `GnitzClient` pyclass and its verbs, the `Txn`
//! context manager over an open transaction, and the `PollResult` a mirror poll
//! reports through.
//!
//! Every schema, key and row it hands to the wire is encoded by `write`, and
//! every reply it hands back is decoded by `read`; nothing here dispatches on a
//! column type.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use gnitz_core::{
    ClientError, DeltaCursor, GnitzClient, PollOutcome, PollResult, Schema, TableProps, WireConflictMode,
};
use gnitz_mirror::Mirror;
use gnitz_sql::{SqlPlanner, SqlResult};

use crate::read::{batch_to_lazy, delta_reply_to_py, triple_to_lazy, PyDeltaReply, PyScanResult};
use crate::schema::{resolve_py_schema, rust_schema_to_py};
use crate::write::{extract_uuid_or_u128, pk_key_from_py, py_pks_to_column, PyZSetBatch};
use crate::{build_pylist, client_err, connect_client, gnitz_err, sql_err, GnitzError};

/// What one view's poll did. `PollOutcome` flattened for Python, which has no
/// cheap payload-carrying enum.
#[pyclass(name = "PollResult", frozen, get_all)]
pub struct PyPollResult {
    /// The relation's server id, which a recreated view moves.
    view_id: u64,
    /// The copy was discarded and re-read whole — the discontinuity a subscriber
    /// has to react to, which no cursor carries.
    reseeded: bool,
    /// Why this one view's poll failed, or `None`. The other views went on; the
    /// recovery is `forget_view(view_id)`.
    error: Option<String>,
    /// `(tag, tick)`, or `None` when the view has no valid copy.
    cursor: Option<(u64, u64)>,
}

impl From<PollOutcome> for PyPollResult {
    fn from(o: PollOutcome) -> PyPollResult {
        PyPollResult {
            view_id: o.view_id,
            reseeded: o.result.reseeded(),
            error: match &o.result {
                PollResult::Failed(e) => Some(e.to_string()),
                _ => None,
            },
            cursor: o.cursor.map(|c| (c.tag, c.tick)),
        }
    }
}

#[pymethods]
impl PyPollResult {
    /// Rendered the way Python prints the same values, not the way Rust does.
    fn __repr__(&self) -> String {
        let cursor = self
            .cursor
            .map_or("None".to_string(), |(tag, tick)| format!("({tag}, {tick})"));
        let error = self.error.as_ref().map_or("None".to_string(), |m| format!("{m:?}"));
        format!(
            "PollResult(view_id={}, reseeded={}, cursor={cursor}, error={error})",
            self.view_id,
            if self.reseeded { "True" } else { "False" },
        )
    }
}

#[pyclass(name = "GnitzClient")]
pub struct PyGnitzClient {
    /// A `Sync` shim: `#[pyclass]` demands `Sync` and a mirroring client is
    /// `Send` only. Never locked — [`Self::slot`] is the only way in.
    inner: Mutex<Option<GnitzClient>>,
}

impl PyGnitzClient {
    /// The client slot, empty once `close()` has run.
    fn slot(&mut self) -> &mut Option<GnitzClient> {
        self.inner.get_mut().expect("the mutex is never locked")
    }

    /// The still-open client, or a `GnitzError` if `close()` already ran.
    fn live(&mut self) -> PyResult<&mut GnitzClient> {
        self.slot()
            .as_mut()
            .ok_or_else(|| GnitzError::new_err("client already closed"))
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
}

#[pymethods]
impl PyGnitzClient {
    #[new]
    pub fn new(py: Python<'_>, socket_path: &str) -> PyResult<Self> {
        Ok(PyGnitzClient {
            inner: Mutex::new(Some(connect_client(py, socket_path)?)),
        })
    }

    /// The client's current OCC basis (the running max of observed server
    /// watermarks, seeded from the HELLO ACK at connect). Read-only, for tests.
    #[getter]
    fn last_seen_lsn(&mut self) -> PyResult<u64> {
        Ok(self.live()?.last_seen_lsn())
    }

    /// Request frames this connection has written — what a test asserts a
    /// mirrored read does not move. Answers on a poisoned store.
    #[getter]
    pub fn requests_sent(&mut self) -> PyResult<u64> {
        Ok(self.live()?.requests_sent())
    }

    /// Close the connection, checkpointing and releasing the mirror store first.
    /// Idempotent. The GIL goes down for it: the checkpoint is fsync-bound.
    pub fn close(&mut self, py: Python<'_>) {
        let taken = self.slot().take();
        py.detach(move || {
            if let Some(mut client) = taken {
                // A `NoMirrorStore` error is the no-store case.
                let _ = client.close_mirror();
                drop(client);
            }
        });
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    pub fn __exit__(&mut self, py: Python<'_>, _exc_type: Py<PyAny>, _exc_val: Py<PyAny>, _exc_tb: Py<PyAny>) -> bool {
        self.close(py);
        false
    }

    // ----- DDL -----

    pub fn create_schema(&mut self, py: Python<'_>, name: &str) -> PyResult<u64> {
        self.call(py, |c| c.create_schema(name))
    }

    pub fn drop_schema(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        self.call(py, |c| c.drop_schema(name))
    }

    /// create_table(schema_name, table_name, columns) — `columns` is a `Schema`
    /// or a list of `ColumnDef`. Partitioned, default distribution; no inline
    /// UNIQUE surface.
    pub fn create_table(
        &mut self,
        py: Python<'_>,
        schema_name: &str,
        table_name: &str,
        #[pyo3(from_py_with = resolve_py_schema)] columns: Arc<Schema>,
    ) -> PyResult<u64> {
        let (cols, pk) = (&columns.columns, &columns.pk_cols);
        self.call(py, move |c| {
            c.create_table(schema_name, table_name, cols, pk, TableProps::default(), &[])
        })
    }

    pub fn drop_table(&mut self, py: Python<'_>, schema_name: &str, table_name: &str) -> PyResult<()> {
        self.call(py, |c| c.drop_table(schema_name, table_name, false))
    }

    // ----- DML -----

    /// push(target_id, batch, mode="update") -> ingest_lsn: int.
    ///
    /// `"update"` silently upserts on a PK conflict (DBSP z-set retraction
    /// semantics); `"error"` rejects the batch, as SQL `INSERT` does.
    #[pyo3(signature = (target_id, batch, mode = "update"))]
    pub fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>, mode: &str) -> PyResult<u64> {
        let m: WireConflictMode = mode.parse().map_err(gnitz_err)?;
        // Hold the `PyRef` guard here (it is `!Ungil`) and pass only the plain
        // `&Schema`/`&ZSetBatch` into the closure, so the GIL is free during
        // the blocking push without cloning the batch.
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        self.call(py, move |c| c.push_with_mode(target_id, schema, b, m))
    }

    /// delete(target_id, schema, pks) — `schema` may be a `Schema` or a list of
    /// `ColumnDef`; `pks` is a list where each element is either an int
    /// (single-column PK) or bytes (a packed compound PK).
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

    /// Open an atomic write-batch transaction as a context manager. A clean
    /// `with`-block exit commits the bundle under one durable zone LSN; an
    /// exception discards it, and nothing was sent.
    ///
    /// ```python
    /// with client.transaction() as txn:
    ///     txn.push(orders_tid, orders_batch)
    ///     txn.delete(carts_tid, cart_schema, [pk])
    /// ```
    pub fn transaction(slf: Bound<'_, PyGnitzClient>) -> PyResult<PyTxn> {
        slf.borrow_mut().live()?.txn_begin().map_err(client_err)?;
        Ok(PyTxn { open: true, client: slf.unbind() })
    }

    // ----- Views -----

    /// `output_schema` may be a `Schema` or a list of `ColumnDef`.
    pub fn create_view(
        &mut self,
        py: Python<'_>,
        schema_name: &str,
        view_name: &str,
        source_table_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] output_schema: Arc<Schema>,
    ) -> PyResult<u64> {
        let cols = &output_schema.columns;
        self.call(py, move |c| {
            c.create_view(schema_name, view_name, source_table_id, cols)
        })
    }

    pub fn drop_view(&mut self, py: Python<'_>, schema_name: &str, view_name: &str) -> PyResult<()> {
        self.call(py, |c| c.drop_view(schema_name, view_name, false))
    }

    /// resolve_table(schema_name, table_name) -> (tid: int, schema: Schema)
    pub fn resolve_table(&mut self, py: Python<'_>, schema_name: &str, table_name: &str) -> PyResult<Py<PyAny>> {
        let (tid, schema) = self.call(py, |c| c.resolve_table_or_view_id(schema_name, table_name))?;
        let py_schema = rust_schema_to_py(py, &schema)?.into_any();
        let tid_obj = tid.into_pyobject(py)?.into_any().unbind();
        Ok(PyTuple::new(py, [tid_obj, py_schema])?.into_any().unbind())
    }

    /// scan(target_id, include_hidden=False) -> ScanResult
    ///
    /// Every row of the relation, off this client's local copy if it mirrors one
    /// and from the server if it does not. `lsn` is `None` for a local answer;
    /// a copy's freshness is `cursor(view_id)`.
    #[pyo3(signature = (target_id, include_hidden = false))]
    pub fn scan(&mut self, py: Python<'_>, target_id: u64, include_hidden: bool) -> PyResult<Py<PyScanResult>> {
        let (schema, batch, lsn) = self.call(py, |c| c.scan_local_first(target_id))?;
        batch_to_lazy(py, schema, batch, lsn, include_hidden)
    }

    /// delta_bootstrap(view_id, view_schema, include_hidden=False) -> DeltaReply
    ///
    /// The view's whole current value, in the view's own schema, plus the cursor
    /// to poll from. Costs what a scan of the view costs. Apply it to a fresh
    /// copy: it replaces state, it does not add to it.
    #[pyo3(signature = (view_id, view_schema, include_hidden = false))]
    pub fn delta_bootstrap(
        &mut self,
        py: Python<'_>,
        view_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] view_schema: Arc<Schema>,
        include_hidden: bool,
    ) -> PyResult<Py<PyDeltaReply>> {
        let out = self.call(py, |c| c.delta_bootstrap(view_id, &view_schema))?;
        delta_reply_to_py(py, view_schema, out, include_hidden)
    }

    /// delta_poll(view_id, reply_schema, cursor, include_hidden=False) -> DeltaReply
    ///
    /// Every delta the view emitted since `cursor`, in `delta_reply_schema`'s
    /// shape. `cursor` is the `(tag, tick)` a previous reply handed back. Apply
    /// what comes back and keep the new cursor; there is nothing to filter and
    /// nothing to reconcile.
    ///
    /// A cursor whose rounds are gone, or that names a different boot or
    /// relation, is refused with `GnitzDeltaExpiredError` rather than answered
    /// with the wrong relation's rows. Bootstrap again.
    #[pyo3(signature = (view_id, reply_schema, cursor, include_hidden = false))]
    pub fn delta_poll(
        &mut self,
        py: Python<'_>,
        view_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] reply_schema: Arc<Schema>,
        cursor: (u64, u64),
        include_hidden: bool,
    ) -> PyResult<Py<PyDeltaReply>> {
        let cursor = DeltaCursor { tag: cursor.0, tick: cursor.1 };
        let out = self.call(py, |c| c.delta_poll(view_id, cursor, &reply_schema))?;
        delta_reply_to_py(py, reply_schema, out, include_hidden)
    }

    /// scan_many(target_ids, include_hidden=False) -> list[ScanResult]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, in request
    /// order: an atomic multi-table transaction is never observed torn across it.
    #[pyo3(signature = (target_ids, include_hidden = false))]
    pub fn scan_many(
        &mut self,
        py: Python<'_>,
        target_ids: Vec<u64>,
        include_hidden: bool,
    ) -> PyResult<Vec<Py<PyScanResult>>> {
        let results = self.call(py, |c| c.scan_many(&target_ids))?;
        results
            .into_iter()
            .map(|triple| triple_to_lazy(py, triple, include_hidden))
            .collect()
    }

    /// seek(table_id, pk=0, include_hidden=False) -> ScanResult.
    /// `pk` may be an `int` (narrow single-PK tables) or `bytes` (compound or
    /// wide-byte PKs).
    #[pyo3(signature = (table_id, pk = None, include_hidden = false))]
    pub fn seek(
        &mut self,
        py: Python<'_>,
        table_id: u64,
        pk: Option<Bound<'_, PyAny>>,
        include_hidden: bool,
    ) -> PyResult<Py<PyScanResult>> {
        let (low, extra) = match pk {
            Some(ref obj) => pk_key_from_py(obj)?,
            None => (0, Vec::new()),
        };
        let triple = self.call(py, move |c| c.seek(table_id, low, &extra))?;
        triple_to_lazy(py, triple, include_hidden)
    }

    /// seek_by_index(table_id, col_indices, key_vals, include_hidden=False) -> ScanResult.
    ///
    /// `col_indices` is the index's FULL declared column list (the server matches
    /// the circuit by exact list); `key_vals` supplies the leading key values, and
    /// may be shorter for a leading-prefix seek.
    #[pyo3(signature = (table_id, col_indices, key_vals, include_hidden = false))]
    pub fn seek_by_index(
        &mut self,
        py: Python<'_>,
        table_id: u64,
        col_indices: Vec<u32>,
        key_vals: Bound<'_, PyList>,
        include_hidden: bool,
    ) -> PyResult<Py<PyScanResult>> {
        let mut keys: Vec<u128> = Vec::with_capacity(key_vals.len());
        for item in key_vals.iter() {
            keys.push(extract_uuid_or_u128(&item)?);
        }
        let triple = self.call(py, move |c| c.seek_by_index(table_id, &col_indices, &keys))?;
        triple_to_lazy(py, triple, include_hidden)
    }

    /// execute_sql(sql, schema_name="public") -> list of result dicts
    ///
    /// A `SELECT` (and the `EXPLAIN` of one) over a view this client mirrors is
    /// planned and answered against the local copy; every other statement, and
    /// every read of a relation the copy does not hold, runs on the connection.
    #[pyo3(signature = (sql, schema_name = "public"))]
    pub fn execute_sql(&mut self, py: Python<'_>, sql: &str, schema_name: &str) -> PyResult<Py<PyAny>> {
        // Plan + execute (all wire I/O, no Python) with the GIL released.
        let c = self.live()?;
        let results = py
            .detach(|| SqlPlanner::new(c, schema_name).execute(sql))
            .map_err(sql_err)?;
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
        self.call(py, move |c| c.attach_mirror(Mirror::open(&base_dir)?))
    }

    /// mirror_view(schema_name, name) -> PollResult
    ///
    /// Register the view and bring its copy up to date; idempotent, and the
    /// result says whether this was a first registration or a reopen. Only a
    /// view created `WITH (delta = '<size>')` can be mirrored.
    ///
    /// A mirrored read answers at the last poll, not at what the server holds
    /// now.
    pub fn mirror_view(&mut self, py: Python<'_>, schema_name: &str, name: &str) -> PyResult<Py<PyPollResult>> {
        let outcome = self.call(py, |c| c.mirror_view(schema_name, name))?;
        Py::new(py, PyPollResult::from(outcome))
    }

    /// forget_view(view_id) — stop mirroring the relation. The copy goes, and a
    /// later read of it is delegated upstream.
    pub fn forget_view(&mut self, py: Python<'_>, view_id: u64) -> PyResult<()> {
        self.call(py, |c| c.forget_view(view_id))
    }

    /// poll() -> list[PollResult]
    ///
    /// Advance every registered view by one poll each — one entry per view,
    /// whatever happened to it. A view that failed carries its message in
    /// `error`; the others went on. It raises only for a failure of the call
    /// itself: no store attached, a poisoned one, or a `KeyboardInterrupt`.
    ///
    /// A poll drives no tick server-side, so a drain is "read the view against
    /// the server, then poll once".
    pub fn poll(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyPollResult>>> {
        let outcomes = self.call(py, GnitzClient::poll_mirror)?;
        outcomes
            .into_iter()
            .map(|o| Py::new(py, PyPollResult::from(o)))
            .collect()
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
    pub fn cursor(&mut self, py: Python<'_>, view_id: u64) -> PyResult<Py<PyAny>> {
        match self.live()?.cursor_of(view_id) {
            None => Ok(py.None()),
            Some(DeltaCursor { tag, tick }) => Ok(PyTuple::new(py, [tag, tick])?.into_any().unbind()),
        }
    }

    /// The message that poisoned this client's copy, or `None`. Answers on a
    /// poisoned store — diagnosing one is what it is for.
    #[getter]
    pub fn mirror_poisoned(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.live()?.mirror_poisoned() {
            None => Ok(py.None()),
            Some(m) => Ok(m.into_pyobject(py)?.into_any().unbind()),
        }
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
fn sql_results_to_py(py: Python<'_>, results: Vec<SqlResult>) -> PyResult<Py<PyAny>> {
    // Interned throughout: `r["type"] == "Rows"` on the Python side then settles
    // on pointer identity rather than a string compare.
    let k_type = pyo3::intern!(py, "type");
    let dicts = results.into_iter().map(|r| {
        let d = PyDict::new(py);
        match r {
            SqlResult::TableCreated { table_id } => {
                d.set_item(k_type, pyo3::intern!(py, "TableCreated"))?;
                d.set_item(pyo3::intern!(py, "table_id"), table_id)?;
            }
            SqlResult::ViewCreated { view_id } => {
                d.set_item(k_type, pyo3::intern!(py, "ViewCreated"))?;
                d.set_item(pyo3::intern!(py, "view_id"), view_id)?;
            }
            SqlResult::IndexCreated { index_id } => {
                d.set_item(k_type, pyo3::intern!(py, "IndexCreated"))?;
                d.set_item(pyo3::intern!(py, "index_id"), index_id)?;
            }
            SqlResult::Dropped => {
                d.set_item(k_type, pyo3::intern!(py, "Dropped"))?;
            }
            SqlResult::Altered { object, name } => {
                d.set_item(k_type, pyo3::intern!(py, "Altered"))?;
                d.set_item(pyo3::intern!(py, "object"), object)?;
                d.set_item(pyo3::intern!(py, "name"), name)?;
            }
            SqlResult::RowsAffected { count } => {
                d.set_item(k_type, pyo3::intern!(py, "RowsAffected"))?;
                d.set_item(pyo3::intern!(py, "count"), count)?;
            }
            SqlResult::Rows { schema, batch } => {
                d.set_item(k_type, pyo3::intern!(py, "Rows"))?;
                d.set_item(
                    pyo3::intern!(py, "rows"),
                    batch_to_lazy(py, Some(Arc::new(schema)), Some(batch), None, false)?,
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
    Ok(build_pylist(py, dicts)?.into_any().unbind())
}

/// An RAII handle on the client's open transaction. Its `push`/`delete` are the
/// client's own, which buffer while a transaction is open — as a SQL `INSERT`
/// between `BEGIN` and `COMMIT` does.
#[pyclass(name = "Txn")]
pub struct PyTxn {
    /// False once `__exit__` has committed or discarded the transaction.
    open: bool,
    client: Py<PyGnitzClient>,
}

#[pymethods]
impl PyTxn {
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Buffer a push of `batch` into `target_id` — the client's own `push`,
    /// which buffers because this transaction is open.
    #[pyo3(signature = (target_id, batch, mode = "update"))]
    pub fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>, mode: &str) -> PyResult<()> {
        self.client(py)?.push(py, target_id, batch, mode).map(|_| ())
    }

    /// Buffer a delete of `pks` from `target_id` — the client's own `delete`.
    pub fn delete(
        &mut self,
        py: Python<'_>,
        target_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] schema: Arc<Schema>,
        pks: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.client(py)?.delete(py, target_id, schema, pks)
    }

    /// Commit the bundle on a clean exit; discard it (rollback) if the block
    /// raised. Returns `False` so an in-block exception is never suppressed.
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<bool> {
        if !self.open {
            return Ok(false);
        }
        let clean = exc_type.is_none(py);
        let r = self.client(py)?.call(py, |c| {
            if clean {
                c.txn_commit().map(|_| ())
            } else {
                c.txn_rollback()
            }
        });
        // Consumed either way — a failed COMMIT has already closed it client-side.
        self.open = false;
        r?;
        Ok(false)
    }
}

impl PyTxn {
    /// The client this transaction is open on, or an error if `__exit__` has
    /// already committed or discarded it.
    fn client<'py>(&self, py: Python<'py>) -> PyResult<PyRefMut<'py, PyGnitzClient>> {
        if !self.open {
            return Err(GnitzError::new_err("transaction already committed or discarded"));
        }
        Ok(self.client.bind(py).borrow_mut())
    }
}
