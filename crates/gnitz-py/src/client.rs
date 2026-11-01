//! Every verb, written once on the `_Client` base, and transactions.
//!
//! A verb is `op`, what it does with the client, and `convert`, what turns the
//! client's answer into its Python value under the GIL. How it waits is the
//! class's: `blocking` runs it on the calling thread, `aio` on an asyncio event
//! loop. Every key and row it hands the wire is encoded by `write`, and every
//! row it gets back decoded by `read`; nothing here dispatches on a column type.

use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use pyo3::IntoPyObjectExt;

use gnitz_core::{BoxFut, ClientError, DeltaCursor, GnitzClient, RelName, Sent, SubId, TableDef, ZSetBatch};
use gnitz_mirror::{Mirror, MirrorConfig};
use gnitz_sql::GnitzSqlError;
use gnitz_wire::{KeyRange, PkColList, PkListRole, ReadBound, ReadSpec, WireConflictMode};

use crate::aio::{LoopCore, LoopHandle};
use crate::blocking::Blocking;
use crate::read::scan_result;
use crate::reply::{delta, none, polled, scanned, sql_results, synced, value};
use crate::schema::PySchema;
use crate::write::{py_key_image, py_pks_to_column, PyZSetBatch};
use crate::{client_err, lock};

/// How a client waits, which is the whole difference between its two classes.
pub(crate) enum Mode {
    /// `None` once closed. Held by the verb running on it: one that finds it
    /// held — from another thread, or from Python the running verb ran — is
    /// refused.
    Blocking(Mutex<Option<Box<Blocking>>>),
    Loop(LoopHandle),
}

/// The verbs both client classes inherit. A verb's value is its result on a
/// `GnitzClient`, a future of it on an `AsyncGnitzClient`, and a `Pending`
/// inside `GnitzClient.pipeline()`.
#[pyclass(name = "_Client", subclass, frozen, module = "gnitz._native")]
pub struct PyClient {
    mode: Mode,
    /// The schema of a relation named without one, in SQL and in every verb
    /// that takes a name.
    schema: Mutex<Arc<str>>,
}

/// A verb the client finishes itself: `body` runs with the client `c` to
/// itself, and its value is the verb's.
macro_rules! whole {
    (|$c:ident| $body:expr) => {
        move |$c| Box::pin(async move { Ok(Sent::ready(Ok($body))) })
    };
}
pub(crate) use whole;

/// A verb that is one request: `pending` is submitted, and its reply awaited
/// with the client already free for the next call.
macro_rules! single {
    (|$c:ident| $pending:expr) => {
        move |$c| Box::pin(async move { Ok($pending.detach()) })
    };
}

impl PyClient {
    /// A client that waits as `mode` does.
    pub(crate) fn init(mode: Mode, schema: &str) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyClient {
            mode,
            schema: Mutex::new(Arc::from(schema)),
        })
    }

    /// The blocking client's slot: an error on a client that lives on an event
    /// loop, and on one a verb is running on.
    fn blocking_slot(&self) -> PyResult<MutexGuard<'_, Option<Box<Blocking>>>> {
        let Mode::Blocking(slot) = &self.mode else {
            return Err(PyTypeError::new_err(
                "this client lives on an event loop; await its verbs",
            ));
        };
        match slot.try_lock() {
            Ok(slot) => Ok(slot),
            Err(TryLockError::Poisoned(slot)) => Ok(slot.into_inner()),
            Err(TryLockError::WouldBlock) => Err(PyRuntimeError::new_err("this client is running another call")),
        }
    }

    /// Run `f` on the blocking client: an error on a closed one, as
    /// [`Self::blocking_slot`] gives.
    pub(crate) fn blocking<R>(&self, f: impl FnOnce(&mut Blocking) -> PyResult<R>) -> PyResult<R> {
        match self.blocking_slot()?.as_deref_mut() {
            Some(blocking) => f(blocking),
            None => Err(client_err(ClientError::Closed)),
        }
    }

    /// Close a blocking client: its driver, which the caller drops. `None`
    /// once closed.
    pub(crate) fn take_blocking(&self) -> PyResult<Option<Box<Blocking>>> {
        Ok(self.blocking_slot()?.take())
    }

    /// The event loop's half of a client on one: an error on a blocking client.
    pub(crate) fn on_loop(&self) -> PyResult<&LoopCore> {
        match &self.mode {
            Mode::Loop(handle) => Ok(handle.get()),
            Mode::Blocking(_) => Err(PyTypeError::new_err("this client blocks: use `with`, not `async with`")),
        }
    }

    /// Run one verb: `op` on the client, and `convert` on what it yields.
    fn run<T, O, C>(slf: &Bound<'_, Self>, op: O, convert: C) -> PyResult<Py<PyAny>>
    where
        T: Send + 'static,
        O: for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, Result<Sent<T>, GnitzSqlError>> + Send + 'static,
        C: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        match &slf.get().mode {
            Mode::Blocking(_) => slf.get().blocking(|blocking| blocking.run(slf, op, convert)),
            Mode::Loop(handle) => handle.get().submit(slf.py(), op, convert),
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
        match &slf.get().mode {
            Mode::Blocking(_) => slf
                .get()
                .blocking(|blocking| convert(slf.py(), read(&mut blocking.client))),
            Mode::Loop(_) => Self::run(slf, whole!(|c| read(c)), convert),
        }
    }

    fn schema_name(slf: &Bound<'_, Self>) -> Arc<str> {
        Arc::clone(&lock(&slf.get().schema))
    }

    /// The relation a verb's `name` or `schema.name` argument names, split at
    /// its first `.`; a bare name is in the client's current schema.
    fn relation(slf: &Bound<'_, Self>, text: &str) -> PyResult<RelName> {
        match text.split_once('.') {
            Some((schema, name)) => RelName::new(schema, name),
            None => RelName::unqualified(&lock(&slf.get().schema), text),
        }
        .map_err(|e| client_err(e.into()))
    }

    /// The rows `spec` selects of relation `tid` in `schema`'s layout, off this
    /// client's local copy if it mirrors one.
    fn read(slf: &Bound<'_, Self>, tid: u64, schema: PySchema, spec: ReadSpec) -> PyResult<Py<PyAny>> {
        Self::run(
            slf,
            single!(|c| c.scan_spec_local_first(tid, spec, &schema.rust)),
            scanned,
        )
    }
}

/// A delta read's `cursor` and `spec` arguments: `(tag, tick)` or none, and
/// the view whole where no spec is given.
fn delta_args(cursor: Option<(u64, u64)>, spec: Option<Vec<u8>>) -> PyResult<(Option<DeltaCursor>, Vec<u8>)> {
    let cursor = cursor
        .map(|(tag, tick)| {
            DeltaCursor::from_pair(tag, tick).ok_or_else(|| {
                PyValueError::new_err("a delta cursor at tick 0 continues no round; read the view whole")
            })
        })
        .transpose()?;
    Ok((
        cursor,
        spec.unwrap_or_else(|| ReadSpec::all_rows(ReadBound::None).encode()),
    ))
}

/// A sync's `wait`, given in seconds.
fn sync_wait(seconds: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(seconds)
        .map_err(|_| PyValueError::new_err(format!("wait must be a non-negative number of seconds, not {seconds}")))
}

#[pymethods]
impl PyClient {
    #[getter]
    fn schema(slf: &Bound<'_, Self>) -> String {
        lock(&slf.get().schema).to_string()
    }

    #[setter]
    fn set_schema(&self, schema: String) {
        *lock(&self.schema) = schema.into();
    }

    /// Request frames this connection has written — what a test asserts a
    /// mirrored read does not move. Answers on a poisoned store.
    #[getter]
    fn requests_sent(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::peek(slf, |c| c.requests_sent(), value)
    }

    // ----- DDL -----

    fn create_schema(slf: &Bound<'_, Self>, name: String) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.create_schema(&name, false).await?), value)
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
            whole!(|c| c.create_table(&TableDef::new(table, (*schema.rust).clone())).await?),
            value,
        )
    }

    fn drop_table(slf: &Bound<'_, Self>, table_name: String) -> PyResult<Py<PyAny>> {
        let table = Self::relation(slf, &table_name)?;
        Self::run(slf, whole!(|c| c.drop_table(&[table], false).await?), none)
    }

    fn drop_view(slf: &Bound<'_, Self>, view_name: String) -> PyResult<Py<PyAny>> {
        let view = Self::relation(slf, &view_name)?;
        Self::run(slf, whole!(|c| c.drop_view(&[view], false).await?), none)
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
        let (schema, rows) = (Arc::clone(batch.schema()), Arc::clone(&batch.batch));
        drop(batch);
        Self::run(
            slf,
            single!(|c| c.push(target_id, &schema, Arc::clone(&rows), mode)),
            value,
        )
    }

    /// delete(target_id, schema, pks) — `pks` is a sequence where each element
    /// is the key's value (single-column PK) or a tuple of its column values
    /// (compound PK).
    fn delete(
        slf: &Bound<'_, Self>,
        target_id: u64,
        schema: PySchema,
        pks: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let batch = ZSetBatch::retraction(&schema.rust, py_pks_to_column(&schema.rust, &pks)?);
        Self::run(
            slf,
            single!(|c| c.push(target_id, &schema.rust, batch, WireConflictMode::Update)),
            none,
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

    // ----- Reads -----

    /// resolve_table(table_name) -> (tid: int, schema: Schema)
    fn resolve_table(slf: &Bound<'_, Self>, table_name: String) -> PyResult<Py<PyAny>> {
        let table = Self::relation(slf, &table_name)?;
        Self::run(slf, whole!(|c| c.resolve_relation(&table).await?), |py, rel| {
            value(py, (rel.tid, PySchema { rust: Arc::clone(&rel.schema) }))
        })
    }

    /// scan(target_id, schema) -> ScanResult
    ///
    /// Every row of the relation in `schema`'s layout, off this client's local
    /// copy if it mirrors one. `lsn` is `None` for a local answer; a copy's
    /// freshness is `cursor(view_id)`.
    fn scan(slf: &Bound<'_, Self>, target_id: u64, schema: PySchema) -> PyResult<Py<PyAny>> {
        Self::read(slf, target_id, schema, ReadSpec::all_rows(ReadBound::None))
    }

    /// scan_many(pairs) -> list[ScanResult]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, in request
    /// order: an atomic multi-table transaction is never observed torn across it.
    /// `pairs` is a list of `(table_id, schema)`.
    fn scan_many(slf: &Bound<'_, Self>, pairs: Vec<(u64, PySchema)>) -> PyResult<Py<PyAny>> {
        let spec = ReadSpec::all_rows(ReadBound::None);
        Self::run(
            slf,
            single!(|c| {
                let scans: Vec<_> = pairs.iter().map(|(tid, s)| ((*tid).into(), &spec, &s.rust)).collect();
                c.scan_many(&scans)
            }),
            |py, replies| {
                let results = replies.into_iter().map(|reply| scan_result(py, reply));
                value(py, results.collect::<PyResult<Vec<_>>>()?)
            },
        )
    }

    /// seek(table_id, schema, pk) -> ScanResult.
    ///
    /// The rows keyed `pk` — a single-column key's value, or a compound key's
    /// tuple of column values in PK order — read where `scan` reads.
    fn seek(slf: &Bound<'_, Self>, table_id: u64, schema: PySchema, pk: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let keys = py_pks_to_column(&schema.rust, std::slice::from_ref(&pk))?.into_keys();
        Self::read(slf, table_id, schema, ReadSpec::all_rows(ReadBound::PkSet(keys)))
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
        let columns = schema.rust.columns();
        let cols = PkColList::checked(&col_indices, columns.len()).map_err(|rule| {
            PyValueError::new_err(format!("seek_by_index: {}", rule.for_role(PkListRole::ColumnList)))
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
            .map(|(&c, v)| py_key_image(&columns[c as usize], &v))
            .collect::<PyResult<Vec<u128>>>()?;
        let range = KeyRange::point(cols, &keys);
        if !range.is_exact(|c| columns[c as usize].is_nullable) {
            return Err(PyValueError::new_err(
                "seek_by_index: the key values stop short of a nullable column, whose NULL rows no walk reaches",
            ));
        }
        Self::read(slf, table_id, schema, ReadSpec::all_rows(ReadBound::Range(range)))
    }

    /// execute_sql(sql) -> list of result dicts
    ///
    /// A `SELECT` (and the `EXPLAIN` of one) over a view this client mirrors is
    /// planned and answered against the local copy; every other statement, and
    /// every read of a relation the copy does not hold, runs on the connection.
    fn execute_sql(slf: &Bound<'_, Self>, sql: String) -> PyResult<Py<PyAny>> {
        let sn = Self::schema_name(slf);
        Self::run(slf, whole!(|c| gnitz_sql::execute(c, &sn, &sql).await?), sql_results)
    }

    // ----- Delta feeds -----

    /// subscription(sql) -> (view_id, schema, spec)
    ///
    /// Plan `sql`, one `SELECT` over one view with a delta feed, as what
    /// `delta_read` and `subscribe` take to answer with only the rows and
    /// columns it keeps: the view's id, the schema the replies come in, and the
    /// spec to pass. The view's key rides along, hidden where the `SELECT` does
    /// not name it. An aggregate, `DISTINCT`, `ORDER BY` and `LIMIT` are refused.
    fn subscription(slf: &Bound<'_, Self>, sql: String) -> PyResult<Py<PyAny>> {
        let sn = Self::schema_name(slf);
        Self::run(
            slf,
            whole!(|c| gnitz_sql::plan_subscription(c, &sn, &sql).await?),
            |py, s| (s.upstream.tid, PySchema { rust: s.schema }, PyBytes::new(py, &s.spec)).into_py_any(py),
        )
    }

    /// delta_read(view_id, schema, cursor=None, spec=None) -> (rows, cursor)
    ///
    /// With no `cursor`, the view's whole current value; with one, the view's
    /// deltas since it, which carry every push acknowledged before the call.
    /// Either way, the cursor past them, which the next read continues from.
    /// Under a `subscription`'s `spec` and `schema` it is what the subscription
    /// keeps of the view; every later read of that copy takes the same two.
    /// Raises `GnitzDeltaExpiredError` for a cursor whose rounds are gone, or
    /// that is another boot's, another relation's or another `spec`'s: read
    /// the view whole again.
    #[pyo3(signature = (view_id, schema, cursor = None, spec = None))]
    fn delta_read(
        slf: &Bound<'_, Self>,
        view_id: u64,
        schema: PySchema,
        cursor: Option<(u64, u64)>,
        spec: Option<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let (cursor, spec) = delta_args(cursor, spec)?;
        Self::run(
            slf,
            single!(|c| c.delta_read(view_id, cursor, &schema.rust, &spec)),
            delta,
        )
    }

    /// subscribe(view_id, schema, cursor=None, spec=None) -> (int, ScanResult, (int, int))
    ///
    /// A delta read after `cursor` — whole with none — that stays subscribed
    /// from the cursor it answers with. `sync` answers with the deltas since.
    #[pyo3(signature = (view_id, schema, cursor = None, spec = None))]
    fn subscribe(
        slf: &Bound<'_, Self>,
        view_id: u64,
        schema: PySchema,
        cursor: Option<(u64, u64)>,
        spec: Option<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let (cursor, spec) = delta_args(cursor, spec)?;
        Self::run(
            slf,
            whole!(|c| c.subscribe(view_id, cursor, &schema.rust, &spec).await?),
            |py, (id, rows, cursor)| (id.get(), scan_result(py, rows)?, cursor.pair()).into_py_any(py),
        )
    }

    /// unsubscribe(sub) -> None
    ///
    /// End a subscription: no `sync` reports it again, and the next one tells
    /// the server. An id this client does not hold is ignored.
    fn unsubscribe(slf: &Bound<'_, Self>, sub: u64) -> PyResult<Py<PyAny>> {
        let sub = SubId::new(sub);
        Self::run(slf, whole!(|c| sub.map_or((), |id| c.unsubscribe(id))), none)
    }

    /// sync(wait=0.0) -> Synced
    ///
    /// Bring every subscription of this connection up to every push
    /// acknowledged before the call. With nothing to report, the server holds
    /// the reply for up to `wait` seconds. On an event loop the client's other
    /// calls are answered meanwhile, and leave the sync held; a sync cancelled
    /// before its answer arrives loses nothing, which the next sync brings.
    ///
    /// A subscription that ended and a view that failed carry the exception
    /// in their entry's `error`; on a poisoned store every mirrored view's is
    /// a `GnitzMirrorPoisonedError`. The call raises only for a
    /// `KeyboardInterrupt`.
    #[pyo3(signature = (wait = 0.0))]
    fn sync(slf: &Bound<'_, Self>, wait: f64) -> PyResult<Py<PyAny>> {
        let wait = sync_wait(wait)?;
        match &slf.get().mode {
            Mode::Loop(handle) => handle.get().sync(slf.py(), wait),
            Mode::Blocking(_) => Self::run(slf, whole!(|c| c.sync(wait).await?), synced),
        }
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
            whole!(|c| c
                .attach_mirror(move || Mirror::open(&base_dir, MirrorConfig::from_env()))
                .await?),
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
        Self::run(slf, whole!(|c| c.mirror_view(&view).await?), polled)
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
    /// one view. `sync` advances an alias with the mirrored views, and may
    /// plan `sql` again.
    fn mirror_subscription(slf: &Bound<'_, Self>, alias: String, sql: String) -> PyResult<Py<PyAny>> {
        let sn = Self::schema_name(slf);
        Self::run(
            slf,
            whole!(|c| gnitz_sql::mirror_subscription(c, &sn, &alias, &sql).await?),
            polled,
        )
    }

    /// forget_view(view_id) — stop mirroring the relation. The copy goes, and a
    /// later read of it is delegated upstream.
    fn forget_view(slf: &Bound<'_, Self>, view_id: u64) -> PyResult<Py<PyAny>> {
        Self::run(slf, whole!(|c| c.forget_view(view_id).await?), none)
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
        Self::peek(slf, |c| c.mirrored_ids(), value)
    }

    /// cursor(view_id) -> (tag, tick) | None
    ///
    /// The round a local read of `view_id` answers at, or `None` when there is no
    /// valid copy. A discarded copy is told by `PollResult.reseeded`, never by
    /// its cursor.
    fn cursor(slf: &Bound<'_, Self>, view_id: u64) -> PyResult<Py<PyAny>> {
        Self::peek(slf, move |c| c.cursor_of(view_id).map(DeltaCursor::pair), value)
    }

    /// Whether this client's store is poisoned. Answers on a poisoned store —
    /// diagnosing one is what it is for.
    #[getter]
    fn mirror_poisoned(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::peek(slf, |c| c.mirror_poisoned(), value)
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

/// The server a connection dials: `target`, or `GNITZ_TARGET` where it is not
/// given.
pub(crate) fn resolve_target(target: Option<String>) -> PyResult<String> {
    match target {
        Some(target) => Ok(target),
        None => gnitz_core::env_target().map_err(PyValueError::new_err)?.ok_or_else(|| {
            PyValueError::new_err("no target: pass the server's socket path or tls:// target, or set GNITZ_TARGET")
        }),
    }
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

/// The context manager `transaction()` returns: entering it opens the client's
/// transaction and hands back the client.
#[pyclass(name = "Txn", frozen, generic, module = "gnitz._native")]
pub struct PyTxn {
    client: Py<PyClient>,
}

#[pymethods]
impl PyTxn {
    fn __enter__(&self, py: Python<'_>) -> PyResult<Py<PyClient>> {
        self.client
            .get()
            .blocking(|blocking| blocking.client.txn_begin().map_err(client_err))?;
        Ok(self.client.clone_ref(py))
    }

    /// Ends the transaction and lets the block's exception through.
    fn __exit__(&self, py: Python<'_>, exc_type: Py<PyAny>, _exc_val: Py<PyAny>, _exc_tb: Py<PyAny>) -> PyResult<bool> {
        let clean = exc_type.is_none(py);
        let ended = self
            .client
            .get()
            .blocking(|blocking| Ok(blocking.call(py, async |c| end_txn(c, clean).await)));
        match ended {
            Ok(ended) => ended?,
            // The block's own exception outranks a client it left unusable.
            Err(_) if !clean => {}
            Err(e) => return Err(e),
        }
        Ok(false)
    }

    fn __aenter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let entered = self.client.clone_ref(py);
        self.client
            .get()
            .on_loop()?
            .submit(py, whole!(|c| c.txn_begin()?), move |_, ()| Ok(entered.into_any()))
    }

    /// As `__exit__`, awaited.
    fn __aexit__(
        &self,
        py: Python<'_>,
        exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let core = self.client.get().on_loop()?;
        let clean = exc_type.is_none(py);
        // The block's own exception outranks a client it closed, whose
        // transaction closed with it.
        if !clean && core.closing() {
            return core.settled(py, false.into_py_any(py)?);
        }
        core.submit(py, whole!(|c| end_txn(c, clean).await?), |py, ()| value(py, false))
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

/// A verb's request: runs on the client and yields the verb's answer. Spelled
/// out on [`PyClient::run`] itself, where a closure's signature is inferred.
pub(crate) trait VerbOp<T>:
    for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, Result<Sent<T>, GnitzSqlError>> + Send + 'static
{
}

impl<T, F> VerbOp<T> for F where
    F: for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, Result<Sent<T>, GnitzSqlError>> + Send + 'static
{
}

/// What turns a verb's answer into its Python value.
pub(crate) trait Convert<T>: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static {}

impl<T, F> Convert<T> for F where F: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static {}
