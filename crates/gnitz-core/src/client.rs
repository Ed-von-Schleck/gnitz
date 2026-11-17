//! The client: one session and the host it waits through, with the verbs
//! every other `impl GnitzClient` file builds on — waiting, ids, requests and
//! relation resolution.

use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::os::fd::AsFd;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use gnitz_wire::multi_frame::BLIND;
use gnitz_wire::{ReadSpec, WireConflictMode};

use crate::connection::{DeltaCursor, RelDescriptor, Request, ScanReply, Session, SubId, Target};
use crate::error::{ClientError, not_found};
use crate::host::{BlockingHost, Host, Op, Pending, Sent, offload, serve};
use crate::txn::TxnBuffer;
use crate::{PushBatch, RelName, Schema, ZSetBatch};

/// The ids one SERIAL reservation takes when the statement asks for fewer. Each
/// reservation is a round trip to the master.
const SERIAL_RANGE_SIZE: u64 = 64;

pub struct GnitzClient {
    pub(crate) host: Box<dyn Host>,
    pub(crate) session: Session,
    /// Per table, the SERIAL ids a reservation drew and no INSERT has taken. A
    /// disconnect discards them (an intentional, PostgreSQL-style gap): a server
    /// that crashed may hand the ids no committed row took to another client.
    serial_cache: HashMap<u64, std::ops::Range<u64>>,
    /// Open transaction, if any; `None` is autocommit. Every user-table write
    /// buffers here while it is open, and dropping it is ROLLBACK.
    pub(crate) txn: Option<TxnBuffer>,
    /// The local copy this client reads through, if a host attached one. Boxed,
    /// so a client that never mirrors pays one `None` and no allocation.
    pub(crate) mirror: Option<Box<crate::mirror::MirrorState>>,
    /// A reader's subscriptions with what syncs brought them and no
    /// [`crate::Delivery`] handed over yet, and the reseeds of deliveries
    /// dropped untaken. A mirror keeps its own subscriptions with the views
    /// they feed.
    pub(crate) received: Arc<Mutex<crate::pushed::Received>>,
    /// Qualified name → the descriptor its last RESOLVE answered, for
    /// [`Self::kept_desc`]. This client's own DDL drops the relations it wrote.
    pub(crate) kept: HashMap<RelName, Arc<RelDescriptor>>,
    /// The next subscription id this client asks for. One id names one
    /// subscription for the client's whole life: a reader still holding an id
    /// of a connection since replaced is told that one ended, never handed
    /// another's trains.
    pub(crate) next_sub: SubId,
    /// Ops a reply queued, as a sync's second half is when its answer
    /// arrives. A [`serve`] loop runs them ahead of the calls it is handed.
    pub(crate) resumed: Arc<Mutex<VecDeque<Op>>>,
}

// `gnitz-py` runs these with the GIL released and `gnitz-tokio` spawns them,
// and both need `Send`. Asserted here, a breach names the field that caused it.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    fn assert_send_value<T: Send>(_: T) {}
    assert_send::<GnitzClient>();
    assert_send::<Sent<ScanReply>>();
    let _ = |mut c: GnitzClient, schema: &Arc<Schema>, batch: &ZSetBatch| {
        assert_send_value(c.sync(Duration::ZERO));
        assert_send_value(c.create_schema("", false));
        assert_send_value(c.push(0, schema, batch, WireConflictMode::Update));
        assert_send_value(serve(c, |_| Poll::Ready(None)));
    };
    assert_send::<ZSetBatch>();
    assert_send::<ClientError>();
    // Handed back as `Arc<Schema>` by the scan path, and `Arc<T>: Send` requires
    // `T: Send + Sync`, so this one is the stricter bound.
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Schema>();
};

/// The server `GNITZ_TARGET` names: `None` when it is unset or empty, and a
/// refusal when it is not UTF-8.
pub fn env_target() -> Result<Option<String>, String> {
    match std::env::var("GNITZ_TARGET") {
        Ok(target) => Ok((!target.is_empty()).then_some(target)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err("GNITZ_TARGET is not UTF-8".to_string()),
    }
}

impl GnitzClient {
    /// A blocking client: its verbs' futures complete under [`block_on`](crate::block_on).
    pub fn connect(target: &str) -> Result<Self, ClientError> {
        Self::connect_on(target, Box::new(BlockingHost::default()))
    }

    /// A client that waits the way `host` does. The connect itself blocks the
    /// calling thread, a TLS handshake included; a driver whose thread must not
    /// block runs this where blocking is allowed.
    pub fn connect_on(target: &str, host: Box<dyn Host>) -> Result<Self, ClientError> {
        Self::over(Session::connect(target)?, host)
    }

    /// [`Self::connect_on`] with the connect run as a job on `host`, where it
    /// may block — for a host whose own thread must not, as [`Self::reconnect`]
    /// does. A panic in the connect resumes here; a job the host dropped is
    /// `Closed`.
    pub async fn connect_with(target: &str, mut host: Box<dyn Host>) -> Result<Self, ClientError> {
        let target = target.to_owned();
        let session = offload(&mut *host, move || Session::connect(&target)).await??;
        Self::over(session, host)
    }

    /// A client over an already-connected session.
    pub(crate) fn over(session: Session, mut host: Box<dyn Host>) -> Result<GnitzClient, ClientError> {
        host.attach(session.as_fd().try_clone_to_owned()?)?;
        Ok(GnitzClient {
            host,
            session,
            serial_cache: HashMap::new(),
            txn: None,
            mirror: None,
            received: Default::default(),
            kept: HashMap::new(),
            next_sub: SubId::FIRST,
            resumed: Default::default(),
        })
    }

    /// Replace the connection and keep the copies — what a host does after a
    /// server restart, which kills the socket while the copies survive it.
    ///
    /// Refused while a transaction is open. A failed connect, or a host that
    /// refuses the new socket, leaves this client exactly as it was.
    ///
    /// Every copy keeps its cursor and answers no read until the next poll: the
    /// new connection may be a different server. A poisoned store crosses
    /// unchanged.
    pub async fn reconnect(&mut self, target: &str) -> Result<(), ClientError> {
        if self.txn_active() {
            return Err(ClientError::from(
                "reconnect inside a transaction; commit or roll back first",
            ));
        }
        let target = target.to_owned();
        let fresh = offload(&mut *self.host, move || Session::connect(&target)).await??;
        // Every field, so one added later is decided here too.
        let GnitzClient {
            host,
            session,
            serial_cache,
            txn: _,
            mirror,
            received: _,
            kept,
            next_sub: _,
            resumed: _,
        } = self;
        host.attach(fresh.as_fd().try_clone_to_owned()?)?;
        *session = fresh;
        serial_cache.clear();
        kept.clear();
        if let Some(m) = mirror.as_deref_mut() {
            m.connection_replaced();
        }
        Ok(())
    }

    /// Requests submitted on the current connection — one per request, however
    /// its bytes were batched; HELLO is not counted. Exposed for round-trip-count
    /// assertions.
    pub fn requests_sent(&self) -> u64 {
        self.session.requests_sent()
    }

    /// Whether the connection has ended, so no request on it can be answered.
    pub fn is_closed(&self) -> bool {
        self.session.is_closed()
    }

    // ── Waiting ────────────────────────────────────────────────────────────

    pub(crate) fn ack(&mut self, req: Request<'_>) -> Pending<'_, u64> {
        let sent = self.session.submit(req);
        self.wait(sent)
    }

    /// Step this client's session until `sent` is answered. Replies arrive in
    /// request order, so every reply detached before it is answered by then;
    /// a sync's answer is no reply, and arrives in the order of syncs alone.
    pub fn wait<T>(&mut self, sent: Sent<T>) -> Pending<'_, T> {
        Pending { client: self, sent }
    }

    /// Step until the connection is under its in-flight caps, at which a
    /// request is refused.
    pub(crate) async fn make_room(&mut self) -> Result<(), ClientError> {
        poll_fn(|cx| {
            self.session
                .poll_until(&mut *self.host, cx, |s| (!s.at_capacity()).then_some(()))
        })
        .await
    }

    // ── Catalog and SERIAL ids ─────────────────────────────────────────────

    /// Allocate `n` contiguous catalog object ids (schemas, relations or
    /// indexes) and return the first.
    pub(crate) async fn alloc_ids(&mut self, n: u64) -> Result<u64, ClientError> {
        self.ack(Request::AllocIds(n)).await
    }

    /// Reserve `count` contiguous SERIAL ids for `table` and return the first,
    /// so an INSERT that knows its row count pays one round trip rather than
    /// `ceil(count / SERIAL_RANGE_SIZE)`. An abandoned tail — the old
    /// range's, or this reservation's — is the intentional PostgreSQL-style gap.
    pub async fn reserve_serial_ids(&mut self, table: &RelDescriptor, count: u64) -> Result<u64, ClientError> {
        match self.serial_cache.get_mut(&table.tid) {
            Some(r) if r.end - r.start >= count => {
                let base = r.start;
                r.start += count;
                Ok(base)
            }
            // Refill, abandoning whatever tail the old range still held.
            _ => {
                let want = count.max(SERIAL_RANGE_SIZE);
                let base = self
                    .ack(Request::AllocSerial { table: table.into(), count: want })
                    .await?;
                self.serial_cache.insert(table.tid, base + count..base + want);
                Ok(base)
            }
        }
    }

    /// Reserve exactly `count` SERIAL ids of `table` and return the first,
    /// leaving this client's own reservation untouched: what puts a restored
    /// table's sequence where its dump left it.
    pub async fn reserve_serial_exact(&mut self, table: &RelDescriptor, count: u64) -> Result<u64, ClientError> {
        self.ack(Request::AllocSerial { table: table.into(), count }).await
    }

    // ── Requests ───────────────────────────────────────────────────────────

    /// Push `batch` into `target` under `mode`.
    ///
    /// Inside an open transaction the batch is buffered instead of sent, and the
    /// returned LSN is `0` — nothing is durable until `txn_commit`, which returns
    /// the one zone LSN covering the whole bundle. A batch passed by value moves
    /// into the buffer; a shared one is held as it is until a later push to the
    /// same family extends it, which copies it if the caller still holds it; a
    /// borrowed one is copied into the buffer once.
    pub fn push<'a>(
        &mut self,
        target: impl Into<Target>,
        schema: &Arc<Schema>,
        batch: impl Into<PushBatch<'a>>,
        mode: WireConflictMode,
    ) -> Pending<'_, u64> {
        let (target, batch) = (target.into(), batch.into());
        if let Some(txn) = &mut self.txn {
            let buffered = txn.push(target, schema, batch, mode, BLIND);
            return self.wait(Sent::ready(buffered.map(|()| 0)));
        }
        let batch = &*batch;
        self.ack(Request::Push { target, schema, batch, mode })
    }

    /// Run a parameterized bounded read, replied in `reply_schema`'s layout.
    pub fn scan_spec(
        &mut self,
        target: impl Into<Target>,
        spec: &ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Pending<'_, ScanReply> {
        let target = target.into();
        let sent = self.session.submit_scan(target, spec, reply_schema);
        self.wait(sent)
    }

    /// N [`Self::scan_spec`]s as one request, answered in request order: no
    /// commit is in some of the replies and not in others.
    pub fn scan_many(&mut self, scans: &[(Target, &ReadSpec, &Arc<Schema>)]) -> Pending<'_, Vec<ScanReply>> {
        let sent = self.session.submit_scan_multi(scans);
        self.wait(sent)
    }

    /// Read a view's delta feed under `spec`, with the cursor to poll from
    /// next. The reply comes back in `reply_schema`, weights and all.
    ///
    /// With no `from` it bootstraps: the view's whole current value at its true
    /// net weights, which replaces a copy's state rather than adding to it.
    /// After `from` it is every delta the view emitted since: apply it and store
    /// the new cursor; there is nothing to reconcile.
    ///
    /// `spec` is an encoded `ReadSpec` forwarding rows with no cut: the reply is
    /// that spec applied to the view, in `reply_schema`, which is the view's own
    /// under `ReadSpec::all_rows` of no bound. Every later poll of the copy
    /// carries the same `spec`.
    ///
    /// A cursor the feed does not continue — its rounds dropped, or handed out
    /// by another boot, relation or `spec` — is refused as `DeltaExpired`:
    /// discard the copy and bootstrap again.
    ///
    /// It carries every push acknowledged before it, and is answered at once:
    /// only a sync waits for a view to change.
    pub fn delta_read(
        &mut self,
        view: impl Into<Target>,
        from: Option<DeltaCursor>,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
    ) -> Pending<'_, (ScanReply, DeltaCursor)> {
        let sent = self
            .session
            .submit_delta_read(view.into(), from, reply_schema, spec, None);
        self.wait(sent)
    }

    // ── Reads, by the freshness asked for ──────────────────────────────────
    //
    // `resolve` and `scan_spec` ask the server; `mirrored_desc` and
    // `scan_spec_local_first`, with the mirror's other verbs, consult the copy
    // first, and `kept_desc` the last RESOLVE's answer. **The gate is what the
    // copy holds, never whether a store is attached**, so a client with one
    // reads exactly like a client without for every relation the copy does
    // not hold.

    /// The descriptor for `name` as the server answers it now, or `None` when
    /// no such relation exists; `Err` is a missing schema or a decode error.
    pub async fn resolve(&mut self, name: &RelName) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let sent = self.session.submit_resolve(name.key());
        let found = self.wait(sent).await?;
        match &found {
            Some(desc) => self.kept.insert(name.clone(), Arc::clone(desc)),
            None => self.kept.remove(name),
        };
        Ok(found)
    }

    /// [`Self::resolve`], with a missing relation an error.
    pub async fn resolve_relation(&mut self, name: &RelName) -> Result<Arc<RelDescriptor>, ClientError> {
        self.resolve(name).await?.ok_or_else(|| not_found("relation", name))
    }

    /// What the last RESOLVE answered for `name`. Only a request
    /// carrying its token finds out whether it is stale; a planning error or an
    /// answer that needs no request is the caller's to repeat from
    /// [`Self::resolve`].
    pub fn kept_desc(&self, name: &RelName) -> Option<Arc<RelDescriptor>> {
        self.kept.get(name).cloned()
    }
}
