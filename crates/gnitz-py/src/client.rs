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
use crate::write::{extract_uuid_or_u128, pk_tuple_from_py, py_pks_to_column, PyZSetBatch};
use crate::{client_err, connect_client, gnitz_err, sql_err, to_py_err, GnitzError};

/// What one view's poll did. The round its copy now answers at is `cursor`; the
/// `reseeded` flag is the discontinuity a subscriber has to react to, and no
/// cursor carries it — an expiry-driven reseed inside one boot keeps the tag and
/// moves the tick forward, exactly as an ordinary advance does.
///
/// The flattening is this pyclass's, not the Rust type's: `PollResult` stays an
/// enum where its invariant matters.
#[pyclass(name = "PollResult", frozen, get_all)]
pub struct PyPollResult {
    /// The relation's server id, which a recreated view moves.
    view_id: u64,
    /// The copy was discarded and re-read whole. True for a first registration
    /// and for every recovery; false for a poll that applied deltas, for a reopen
    /// that resumed from its persisted cursor, and for a view whose poll failed —
    /// which is the right answer to "did this view reseed".
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
    /// `#[pyclass]` demands `Sync`; a client that mirrors holds a live engine and
    /// is `Send` only, and `Mutex<T>: Sync` needs just `T: Send`. The lock is what
    /// makes that sound — it is the only way to reach the client from a `&self`.
    /// Nothing takes it: every user here holds `&mut self` and goes through
    /// `get_mut`, and what refuses a second caller is pyo3's borrow flag, with a
    /// `RuntimeError` rather than by queueing.
    inner: Mutex<Option<GnitzClient>>,
}

impl PyGnitzClient {
    /// The still-open client, or a `GnitzError` if `close()` already ran.
    fn live(&mut self) -> PyResult<&mut GnitzClient> {
        self.inner
            .get_mut()
            .expect("nothing locks this mutex, so it cannot be poisoned")
            .as_mut()
            .ok_or_else(|| GnitzError::new_err("client already closed"))
    }

    /// Run one blocking client call: check the client is open, drop the GIL
    /// across it, and map the failure with `map` — [`client_err`] unless the
    /// path raises a class of its own.
    fn call_with<T: Send, E: Send>(
        &mut self,
        py: Python<'_>,
        map: impl FnOnce(E) -> PyErr,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, E> + Send,
    ) -> PyResult<T> {
        let c = self.live()?;
        py.detach(move || f(c)).map_err(map)
    }

    /// [`Self::call_with`] under the default error mapping (a retryable OCC
    /// conflict to `GnitzConflictError`). What a blocking method takes.
    fn call<T: Send>(
        &mut self,
        py: Python<'_>,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, ClientError> + Send,
    ) -> PyResult<T> {
        self.call_with(py, client_err, f)
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

    /// Close the connection: closing checkpoints the mirror store, then releases
    /// it. Calling it twice is fine.
    ///
    /// The GIL goes down for it: the checkpoint is fsync-bound and unbounded in
    /// the copy's size, and the drop is what releases the store's directory lock.
    pub fn close(&mut self, py: Python<'_>) {
        let taken = self
            .inner
            .get_mut()
            .expect("nothing locks this mutex, so it cannot be poisoned")
            .take();
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

    /// create_table(schema_name, table_name, columns).
    /// `columns` may be a `Schema` or a list of `ColumnDef` — resolved at the
    /// parameter through [`resolve_py_schema`], so the PK columns come from the
    /// same rule every other schema surface applies.
    /// Partitioned, default distribution; no inline UNIQUE surface.
    pub fn create_table(
        &mut self,
        py: Python<'_>,
        schema_name: &str,
        table_name: &str,
        #[pyo3(from_py_with = resolve_py_schema)] columns: Arc<Schema>,
    ) -> PyResult<u64> {
        let pk: Vec<u32> = columns.pk_indices().iter().map(|&i| i as u32).collect();
        self.call(py, move |c| {
            c.create_table(
                schema_name,
                table_name,
                &columns.columns,
                &pk,
                TableProps::default(),
                &[],
            )
        })
    }

    pub fn drop_table(&mut self, py: Python<'_>, schema_name: &str, table_name: &str) -> PyResult<()> {
        self.call(py, |c| c.drop_table(schema_name, table_name, false))
    }

    // ----- DML -----

    /// push(target_id, batch) -> ingest_lsn: int. Silent-upsert on PK conflict
    /// (DBSP z-set retraction semantics); SQL-standard rejection is reached via
    /// `INSERT` through `execute_sql`.
    pub fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>) -> PyResult<u64> {
        // Hold the `PyRef` guard here (it is `!Ungil`) and pass only the plain
        // `&Schema`/`&ZSetBatch` into the closure, so the GIL is free during
        // the blocking push without cloning the batch.
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        self.call(py, move |c| c.push(target_id, schema, b))
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

    /// Open an atomic write-batch transaction as a context manager. Buffer
    /// writes with `txn.push` / `txn.delete`; a clean `with`-block exit commits
    /// the whole bundle atomically under one durable zone LSN, while an
    /// exception discards it (rollback — nothing was sent).
    ///
    /// ```python
    /// with client.transaction() as txn:
    ///     txn.push(orders_tid, orders_batch)
    ///     txn.delete(carts_tid, cart_schema, [pk])
    /// ```
    pub fn transaction(slf: Bound<'_, PyGnitzClient>) -> PyResult<PyTxn> {
        to_py_err(slf.borrow_mut().live()?.txn_begin())?;
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
    /// and from the server if it does not. `lsn` is `None` for a local answer: a
    /// served LSN is a server-side counter, and a copy's freshness is a feed
    /// round — `cursor(view_id)` is where a host reads it.
    #[pyo3(signature = (target_id, include_hidden = false))]
    pub fn scan(&mut self, py: Python<'_>, target_id: u64, include_hidden: bool) -> PyResult<Py<PyScanResult>> {
        let (schema, batch, lsn) = self.call(py, |c| c.scan_local_first(target_id))?;
        batch_to_lazy(py, schema, batch, lsn, include_hidden)
    }

    /// delta_bootstrap(view_id, view_schema, include_hidden=False) -> DeltaReply
    ///
    /// The view's whole current value, in the view's own schema, plus the cursor
    /// to poll from. `Delta { after_tick: 0 }` is the sum of every delta after
    /// round 0 — the view's entire history — which is precisely what its output
    /// store holds, so this costs exactly what a scan of the view costs. Apply it
    /// to a fresh copy: it replaces state, it does not add to it.
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

    /// delta_poll(view_id, reply_schema, tag, tick, include_hidden=False) -> DeltaReply
    ///
    /// Every delta the view emitted in `(tick, T]`, in `delta_reply_schema`'s
    /// shape. Apply what comes back and store the returned cursor; there is
    /// nothing to filter and nothing to reconcile.
    ///
    /// A `tag` that does not match the one the reply carries names a different
    /// boot or a different relation — a restart, or a `DROP VIEW` /
    /// `CREATE VIEW` of the same name. That is refused with
    /// `GnitzDeltaExpiredError`, not answered with rows: a foreign cursor draws
    /// the other relation's recent deltas, which are unsafe to apply. The
    /// recovery is the one that error always calls for — bootstrap again.
    /// A `tick` of 0 gets the same error for the same reason: it names no copy to
    /// continue, and a bootstrap comes back in the view's schema rather than in
    /// this one.
    #[pyo3(signature = (view_id, reply_schema, tag, tick, include_hidden = false))]
    pub fn delta_poll(
        &mut self,
        py: Python<'_>,
        view_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] reply_schema: Arc<Schema>,
        tag: u64,
        tick: u64,
        include_hidden: bool,
    ) -> PyResult<Py<PyDeltaReply>> {
        let cursor = gnitz_core::DeltaCursor { tag, tick };
        let out = self.call(py, |c| c.delta_poll(view_id, cursor, &reply_schema))?;
        delta_reply_to_py(py, reply_schema, out, include_hidden)
    }

    /// scan_many(target_ids, include_hidden=False) -> list[ScanResult]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, returned
    /// in request order. An atomic multi-table transaction is never observed
    /// torn across the result list. Same row decoding as `scan`.
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
        let t = match pk {
            Some(ref obj) => pk_tuple_from_py(obj)?,
            None => gnitz_core::PkTuple::from_u128_narrow(0),
        };
        let triple = self.call(py, move |c| c.seek(table_id, &t))?;
        triple_to_lazy(py, triple, include_hidden)
    }

    /// seek_by_index(table_id, col_indices, key_vals, include_hidden=False) -> ScanResult.
    ///
    /// `col_indices` is the index's FULL declared column list (the server matches
    /// the circuit by exact list); `key_vals` supplies the leading native key
    /// values (`len(key_vals)` may be `< len(col_indices)` for a leading-prefix
    /// seek). Arity is validated once inside `GnitzClient::seek_by_index` (the
    /// single choke point for every binding), so no validation is duplicated here.
    /// Key values are decoded through `extract_uuid_or_u128`, so a UUID-keyed seek
    /// accepts the same `uuid.UUID` / hex-string forms the insert paths do.
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
        let results = self.call_with(py, sql_err, |c| SqlPlanner::new(c, schema_name).execute(sql))?;
        sql_results_to_py(py, results)
    }

    // ----- Mirroring -----
    //
    // Every method that does work drops the GIL for all of it — the round trips,
    // and the engine and disk work between them. Only the metadata getters, which
    // answer out of memory, hold it.

    /// mirror_at(base_dir)
    ///
    /// Open (or resume) a local copy directory at `base_dir` and read this
    /// client's mirrored views through it. The directory is this client's alone —
    /// **one store per directory**, in this process or any other, and the engine's
    /// own lock on it refuses a second.
    ///
    /// A second call on the same client is refused, naming the path it holds.
    /// `close_mirror()` releases one; so does closing the client.
    pub fn mirror_at(&mut self, py: Python<'_>, base_dir: &str) -> PyResult<()> {
        let base_dir = base_dir.to_string();
        self.call(py, move |c| c.attach_mirror(Mirror::open(&base_dir)?))
    }

    /// mirror_view(schema_name, name) -> PollResult
    ///
    /// Register the view and bring its copy up to date. Idempotent, and the same
    /// call whether this is a first registration or a reopen: the result says
    /// which it was.
    ///
    /// Only a view with a delta feed can be mirrored — create it
    /// `WITH (delta = '<size>')`.
    ///
    /// **A mirrored read answers at the last poll**, not at what the server holds
    /// now: not read-your-own-writes, and two mirrored views are no consistent
    /// cut.
    pub fn mirror_view(&mut self, py: Python<'_>, schema_name: &str, name: &str) -> PyResult<Py<PyPollResult>> {
        let outcome = self.call(py, |c| c.mirror_view(schema_name, name))?;
        Py::new(py, PyPollResult::from(outcome))
    }

    /// forget_view(view_id)
    ///
    /// Stop mirroring the relation: the copy and its directory go, and a later
    /// read of it is delegated upstream.
    pub fn forget_view(&mut self, py: Python<'_>, view_id: u64) -> PyResult<()> {
        self.call(py, |c| c.forget_view(view_id))
    }

    /// poll() -> list[PollResult]
    ///
    /// Advance every registered view by one poll each, and report what each one
    /// did — **one entry per view, whatever happened to it**. A view that failed
    /// carries its message in `error`; the others went on.
    ///
    /// A poll drives no tick server-side, so a drain is "read the view against
    /// the server, then poll once".
    ///
    /// It raises only for a failure of the call rather than of a view: no store
    /// attached, a poisoned one, and a `KeyboardInterrupt`, which stops at the
    /// view it interrupted.
    pub fn poll(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyPollResult>>> {
        let outcomes = self.call(py, GnitzClient::poll_mirror)?;
        outcomes
            .into_iter()
            .map(|o| Py::new(py, PyPollResult::from(o)))
            .collect()
    }

    /// Make every copy and its cursor durable.
    ///
    /// A failure is reported, not fatal: the flush writes shards and publishes
    /// manifests, neither of which mutates what a copy holds, so the store stays
    /// usable and a retry is sound.
    pub fn checkpoint(&mut self, py: Python<'_>) -> PyResult<()> {
        self.call(py, GnitzClient::checkpoint_mirror)
    }

    /// close_mirror()
    ///
    /// Checkpoint (unless poisoned) and release the store, reporting the final
    /// checkpoint rather than letting the destructor swallow it. The connection
    /// stays open and `mirror_at` may be called again.
    ///
    /// It is also the **only** recovery from a poisoned copy, which refuses every
    /// call that touches it and cannot be cleared.
    pub fn close_mirror(&mut self, py: Python<'_>) -> PyResult<()> {
        self.call(py, GnitzClient::close_mirror)
    }

    /// Whether a read of `view_id` is answered locally. Answers on a poisoned
    /// store.
    pub fn mirrors(&mut self, view_id: u64) -> PyResult<bool> {
        Ok(self.live()?.mirrors(view_id))
    }

    /// Every registration this client holds, whether or not the copy behind it is
    /// valid — wider than `mirrors` by the ones a poll has yet to seed.
    pub fn mirrored_ids(&mut self) -> PyResult<Vec<u64>> {
        Ok(self.live()?.mirrored_ids())
    }

    /// cursor(view_id) -> (tag, tick) | None
    ///
    /// The round a local read of `view_id` answers at, or `None` when there is no
    /// valid copy to read one off.
    ///
    /// The tick is the master's global round counter, shared by every relation,
    /// so it advances over rounds that carried this view nothing. Whether a copy
    /// changed is `PollResult.reseeded`, not this.
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
    /// Replace the connection, keeping every mirrored copy. What a host does
    /// after a server restart: the restart kills the socket, while the copies —
    /// durable, and resumable — survive it. Refused inside a transaction.
    ///
    /// Every cursor is dropped, so a read between the reconnect and the next poll
    /// goes upstream; the poll that follows reports a reseed for every view. A
    /// client that mirrors nothing gains the same recovery.
    pub fn reconnect(&mut self, py: Python<'_>, target: &str) -> PyResult<()> {
        self.call(py, |c| c.reconnect(target))?;
        // The park hook rides along with the reconnect, so a Ctrl-C still
        // interrupts; nothing is reinstalled here.
        Ok(())
    }
}

/// One `SqlResult` per statement as the list of dicts every SQL entry point
/// hands back — the client's and the mirror's alike, so a statement run through
/// either comes back in the same shape.
fn sql_results_to_py(py: Python<'_>, results: Vec<SqlResult>) -> PyResult<Py<PyAny>> {
    let py_list = PyList::empty(py);
    // Interned keys: `r["type"]` on the Python side hits on pointer identity
    // against its own source literal, where a `PyUnicode` freshly built per
    // key per result makes every lookup a string compare.
    let k_type = pyo3::intern!(py, "type");
    for r in results {
        let d = PyDict::new(py);
        match r {
            SqlResult::TableCreated { table_id } => {
                d.set_item(k_type, "TableCreated")?;
                d.set_item(pyo3::intern!(py, "table_id"), table_id)?;
            }
            SqlResult::ViewCreated { view_id } => {
                d.set_item(k_type, "ViewCreated")?;
                d.set_item(pyo3::intern!(py, "view_id"), view_id)?;
            }
            SqlResult::IndexCreated { index_id } => {
                d.set_item(k_type, "IndexCreated")?;
                d.set_item(pyo3::intern!(py, "index_id"), index_id)?;
            }
            SqlResult::Dropped => {
                d.set_item(k_type, "Dropped")?;
            }
            SqlResult::Altered { object, name } => {
                d.set_item(k_type, "Altered")?;
                d.set_item(pyo3::intern!(py, "object"), object)?;
                d.set_item(pyo3::intern!(py, "name"), name)?;
            }
            SqlResult::RowsAffected { count } => {
                d.set_item(k_type, "RowsAffected")?;
                d.set_item(pyo3::intern!(py, "count"), count)?;
            }
            SqlResult::Rows { schema, batch } => {
                d.set_item(k_type, "Rows")?;
                d.set_item(
                    pyo3::intern!(py, "rows"),
                    batch_to_lazy(py, Some(Arc::new(schema)), Some(batch), None, false)?,
                )?;
            }
            SqlResult::TransactionStarted => {
                d.set_item(k_type, "TransactionStarted")?;
            }
            SqlResult::TransactionCommitted { lsn } => {
                d.set_item(k_type, "TransactionCommitted")?;
                d.set_item(pyo3::intern!(py, "lsn"), lsn)?;
            }
            SqlResult::TransactionRolledBack => {
                d.set_item(k_type, "TransactionRolledBack")?;
            }
        }
        py_list.append(d)?;
    }
    Ok(py_list.into_any().unbind())
}

/// Atomic write-batch transaction context manager: an RAII handle on the
/// client's open transaction. `push`/`delete` are the client's own write methods
/// — they buffer because a transaction is open, exactly as a SQL `INSERT` between
/// `BEGIN` and `COMMIT` does. Nothing reaches the server until the `with`-block
/// exits cleanly.
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

    /// Buffer a push of `batch` into `target_id` under conflict mode `mode`
    /// (`"update"` — the default — or `"error"`).
    #[pyo3(signature = (target_id, batch, mode = "update"))]
    pub fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>, mode: &str) -> PyResult<()> {
        let m: WireConflictMode = mode.parse().map_err(gnitz_err)?;
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        self.with_client(py, move |c| c.push_with_mode(target_id, schema, b, m).map(|_| ()))
    }

    /// Buffer a delete of `pks` from `target_id` (same schema and PK forms as
    /// `GnitzClient.delete`).
    pub fn delete(
        &mut self,
        py: Python<'_>,
        target_id: u64,
        #[pyo3(from_py_with = resolve_py_schema)] schema: Arc<Schema>,
        pks: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let pk_col = py_pks_to_column(&schema, &pks)?;
        self.with_client(py, move |c| c.delete(target_id, &schema, pk_col))
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
        let r = self.with_client(py, |c| {
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
    /// Run `f` against the transaction's client, once the transaction is known
    /// to still be open. The call itself goes through the client's own
    /// [`PyGnitzClient::call`], so a buffered write and a `txn_commit` release
    /// the GIL and classify their errors exactly as the non-transactional
    /// writes do.
    fn with_client(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut GnitzClient) -> Result<(), ClientError> + Send,
    ) -> PyResult<()> {
        if !self.open {
            return Err(GnitzError::new_err("transaction already committed or discarded"));
        }
        self.client.bind(py).borrow_mut().call(py, f)
    }
}
