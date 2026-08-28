//! The Rust async client: a [`Connection`] future over tokio's reactor, and
//! the [`AsyncClient`] handle that feeds it.
//!
//! `gnitz-core`'s `Session` is the spine and owns all the protocol, down to
//! resolving each reply against the request that asked for it. This crate owns
//! how it waits.
//!
//! Off the surface deliberately: `execute_sql`, transactions, DDL and id
//! allocation. Each is an interleaved read/compute/write sequence over
//! `GnitzClient` state rather than a wire verb, so serving one here would mean
//! blocking the driver task. They stay on the blocking client.
//!
//! **Mirroring is served by owning a blocking client that mirrors**, so the
//! reconciliation state machine and every ordering rule its correctness rests on
//! exist once and this crate runs that code instead of a second copy of it.
//! Driving the feed on the async connection instead is not a smaller change:
//! `Op` has no `ScanSpec` variant, so a delta read cannot be expressed on it at
//! all, and adding one would buy the right to rewrite the state machine as async
//! beside the blocking one. The crate still links no engine — it takes an opened
//! store, exactly as the blocking client does.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use gnitz_core::{
    qualified_name, ClientError, DeltaCursor, GnitzClient, Interest, LocalScanReply, MirrorStore, PkTuple, PollOutcome,
    RelDescriptor, RelTarget, Reply, Request, ScanReply, Schema, Session, SlotId, WireConflictMode, ZSetBatch,
    MAX_IN_FLIGHT, MAX_QUEUED_BYTES,
};
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};
use tokio::sync::{mpsc, oneshot};

/// Depth of the request channel. `MAX_IN_FLIGHT` is the real in-flight bound;
/// this only keeps a burst from round-tripping through the scheduler.
const REQUEST_CHANNEL_DEPTH: usize = 256;

fn closed() -> ClientError {
    ClientError::ServerError("connection closed".into())
}

/// One request as it crosses the channel — owned, because `Request<'_>`
/// borrows and the borrow ends at `submit`.
enum Op {
    Push {
        tid: u64,
        schema: Arc<Schema>,
        batch: ZSetBatch,
    },
    Scan(u64),
    Seek(u64, PkTuple),
    ScanMany(Vec<u64>),
    /// The canonical `"schema.name"`.
    Resolve(String),
}

struct Submission {
    op: Op,
    reply: oneshot::Sender<Result<Reply, ClientError>>,
}

/// A channel sender and a client id, plus the second client a copy lives inside
/// once one is attached: `Send + Sync + Clone`. Every method takes `&self`, so
/// sharing one is a clone.
#[derive(Clone)]
pub struct AsyncClient {
    tx: mpsc::Sender<Submission>,
    client_id: u64,
    /// What [`AsyncClient::attach_mirror`] connects the feed's own client to.
    target: Arc<str>,
    /// The blocking client the copy lives inside, once a host attaches a store.
    /// `Arc` so a clone sees an install on any other; `Mutex<Option<_>>` so the
    /// `&self` methods every verb takes can install into it and reach it.
    mirror: Arc<Mutex<Option<GnitzClient>>>,
}

// The handle is shared by cloning, so this is what it promises; the mirror field
// is the one thing that could take it away silently.
const _: fn() = || {
    fn assert_send_sync_clone<T: Send + Sync + Clone>() {}
    assert_send_sync_clone::<AsyncClient>();
};

/// The mirror slot, recovering a poisoned lock: what it guards is a client whose
/// own poison state is the real verdict, and a panicking task must not take
/// every later mirror call down with it.
fn lock(slot: &Mutex<Option<GnitzClient>>) -> MutexGuard<'_, Option<GnitzClient>> {
    slot.lock().unwrap_or_else(|e| e.into_inner())
}

/// Run `f` on a blocking pool thread. A panic in it reaches the caller as an
/// error rather than aborting the runtime.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, ClientError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ClientError::ServerError(format!("mirror task failed: {e}")))
}

/// The refusal every mirror verb gives on a handle that never attached a store.
fn no_mirror() -> ClientError {
    ClientError::ServerError("this handle mirrors nothing; attach a store before mirroring a view".to_string())
}

/// Connect to `target` and hand back the handle paired with the driver that
/// serves it. Nothing reaches the connection until the [`Connection`] is
/// polled.
///
/// The TCP connect and TLS handshake block — a dual-stack `tls://` host can
/// spend tens of seconds in them — so they run on `spawn_blocking`.
pub async fn connect(target: &str) -> Result<(AsyncClient, Connection), ClientError> {
    let handle_target: Arc<str> = Arc::from(target);
    let target = target.to_string();
    let session = tokio::task::spawn_blocking(move || Session::connect(&target))
        .await
        .map_err(|e| ClientError::ServerError(format!("connect task failed: {e}")))?
        .map(|(session, _published_lsn)| session)?;
    let client_id = session.client_id;
    // A `dup`, so the reactor deregisters a descriptor whose life it owns
    // rather than a number the session may already have closed and the kernel
    // handed out again.
    let fd = AsyncFd::new(session.try_clone_fd()?)?;
    let (tx, rx) = mpsc::channel(REQUEST_CHANNEL_DEPTH);
    Ok((
        AsyncClient {
            tx,
            client_id,
            target: handle_target,
            mirror: Arc::new(Mutex::new(None)),
        },
        Connection {
            session,
            fd,
            rx,
            pending: HashMap::new(),
        },
    ))
}

impl AsyncClient {
    /// This connection's client id, minted from the same generator the blocking
    /// client uses, so a process holding both cannot mint one twice.
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    async fn call(&self, op: Op) -> Result<Reply, ClientError> {
        let (reply, rx) = oneshot::channel();
        // A full channel suspends here: `Connection` drains only while under
        // `MAX_IN_FLIGHT`, so back-pressure is a wait, never the cap's error.
        self.tx.send(Submission { op, reply }).await.map_err(|_| closed())?;
        rx.await.map_err(|_| closed())?
    }

    /// Push a batch and resolve to its ingest LSN.
    pub async fn push(&self, tid: u64, schema: Arc<Schema>, batch: ZSetBatch) -> Result<u64, ClientError> {
        match self.call(Op::Push { tid, schema, batch }).await? {
            Reply::Lsn(lsn) => Ok(lsn),
            _ => unreachable!("a push completes as Reply::Lsn"),
        }
    }

    pub async fn scan(&self, tid: u64) -> Result<ScanReply, ClientError> {
        self.read(Op::Scan(tid)).await
    }

    pub async fn seek(&self, tid: u64, pk: PkTuple) -> Result<ScanReply, ClientError> {
        self.read(Op::Seek(tid, pk)).await
    }

    async fn read(&self, op: Op) -> Result<ScanReply, ClientError> {
        match self.call(op).await? {
            Reply::Scan(r) => Ok(r),
            _ => unreachable!("a correlated read completes as Reply::Scan"),
        }
    }

    /// Snapshot N relations at one server-side SAL cut, in request order.
    pub async fn scan_many(&self, tids: &[u64]) -> Result<Vec<ScanReply>, ClientError> {
        match self.call(Op::ScanMany(tids.to_vec())).await? {
            Reply::Multi(r) => Ok(r),
            _ => unreachable!("a scan_multi completes as Reply::Multi"),
        }
    }

    /// Describe one relation. Always a round trip — an async handle has no
    /// statement bracket to scope a catalog snapshot to. On the surface because
    /// every other verb takes a `tid` and nothing else here can produce one.
    pub async fn resolve(&self, schema_name: &str, name: &str) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        match self.call(Op::Resolve(qualified_name(schema_name, name))).await? {
            Reply::Resolve(d) => {
                Ok(d.map(|(tid, schema, blob)| Arc::new(RelDescriptor::from_resolve(tid, schema, blob))))
            }
            _ => unreachable!("a resolve completes as Reply::Resolve"),
        }
    }

    // ── Mirroring ──────────────────────────────────────────────────────────
    //
    // Every method here is `async` through one `spawn_blocking`, the metadata
    // accessors included: the lock is held across a whole poll, so a synchronous
    // `mirrors(tid)` would block a reactor thread for that poll's duration. Two
    // costs a host takes on: a mirroring handle holds a second connection — the
    // feed's, taking a second server-side client slot — and every mirror call
    // occupies a blocking-pool thread.

    /// Read a local copy of one or more views through `store`.
    ///
    /// Refused if this handle, or any clone of it, already mirrors — the store
    /// passed in is dropped, releasing its `flock`. Opening a store is itself
    /// blocking work, so a host should do that off the reactor too.
    ///
    /// The lock spans the whole connect-and-install, and has to: two clones
    /// calling this at once would otherwise both see no store, both connect, and
    /// one install would silently drop the other's client — a checkpoint and a
    /// released `flock`.
    pub async fn attach_mirror(&self, store: impl MirrorStore + 'static) -> Result<(), ClientError> {
        let slot = Arc::clone(&self.mirror);
        let target = Arc::clone(&self.target);
        blocking(move || {
            let mut held = lock(&slot);
            if held.is_some() {
                return Err(ClientError::ServerError(
                    "this handle already mirrors; close_mirror before attaching another store".to_string(),
                ));
            }
            let mut client = GnitzClient::connect(&target)?;
            client.attach_mirror(store)?;
            *held = Some(client);
            Ok(())
        })
        .await?
    }

    /// Checkpoint (unless poisoned) and release the store, reporting the final
    /// checkpoint. The handle can attach again afterwards.
    ///
    /// **A mirroring host must call it**: otherwise the last clone's `Drop` runs
    /// the store's own — an fsync unbounded in the copy's size — on whatever
    /// thread happens to drop it, which in a reactor task is a reactor thread.
    /// The lock is released before that fsync, so a close stalls no other clone.
    pub async fn close_mirror(&self) -> Result<(), ClientError> {
        let slot = Arc::clone(&self.mirror);
        blocking(move || {
            let mut taken = lock(&slot).take().ok_or_else(no_mirror)?;
            taken.close_mirror()
        })
        .await?
    }

    /// Mirror `schema_name.name` and bring its copy up to date.
    pub async fn mirror_view(&self, schema_name: &str, name: &str) -> Result<PollOutcome, ClientError> {
        let (schema_name, name) = (schema_name.to_string(), name.to_string());
        self.on_mirror(move |c| c.mirror_view(&schema_name, &name)).await
    }

    /// Advance every mirrored view by one poll each, one entry per view.
    ///
    /// One whole advance runs inside one `spawn_blocking` under the one lock and
    /// spans no `await`, so two clones polling the same view serialize instead of
    /// both reading cursor `c`, both fetching `(c, …]` and both applying it —
    /// which would double every weight in the interval while the row set stayed
    /// identical.
    pub async fn poll_mirror(&self) -> Result<Vec<PollOutcome>, ClientError> {
        self.on_mirror(GnitzClient::poll_mirror).await
    }

    /// Stop mirroring `table_id`.
    pub async fn forget_view(&self, table_id: u64) -> Result<(), ClientError> {
        self.on_mirror(move |c| c.forget_view(table_id)).await
    }

    /// Make every copy and its cursor durable.
    pub async fn checkpoint_mirror(&self) -> Result<(), ClientError> {
        self.on_mirror(GnitzClient::checkpoint_mirror).await
    }

    /// Whether a read of `table_id` is answered off the copy.
    pub async fn mirrors(&self, table_id: u64) -> Result<bool, ClientError> {
        self.on_mirror(move |c| Ok(c.mirrors(table_id))).await
    }

    /// The round a local read of `table_id` answers at.
    pub async fn cursor_of(&self, table_id: u64) -> Result<Option<DeltaCursor>, ClientError> {
        self.on_mirror(move |c| Ok(c.cursor_of(table_id))).await
    }

    /// Every registration the copy holds.
    pub async fn mirrored_ids(&self) -> Result<Vec<u64>, ClientError> {
        self.on_mirror(|c| Ok(c.mirrored_ids())).await
    }

    /// The message that poisoned the copy, if any.
    pub async fn mirror_poisoned(&self) -> Result<Option<String>, ClientError> {
        self.on_mirror(|c| Ok(c.mirror_poisoned())).await
    }

    /// Run `f` on the installed client, under the lock, on a blocking thread.
    async fn on_mirror<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, ClientError> + Send + 'static,
    ) -> Result<T, ClientError> {
        let slot = Arc::clone(&self.mirror);
        blocking(move || {
            let mut held = lock(&slot);
            f(held.as_mut().ok_or_else(no_mirror)?)
        })
        .await?
    }

    /// [`Self::scan`], answered off the copy when it holds `tid`.
    ///
    /// A handle that never attached takes the wire path with no thread hop:
    /// `try_lock` sees an empty uncontended slot without blocking a reactor
    /// thread. Otherwise one `spawn_blocking` asks the copy, which answers "not
    /// held" itself, so the **delegation runs on the async path** and keeps the
    /// driver's back-pressure and its reply's LSN — at the price of a hop that
    /// can wait behind an in-flight poll, because which relations the copy holds
    /// is itself store state.
    ///
    /// [`Self::seek`] and [`Self::scan_many`] stay wire-only: they take raw ids
    /// and a copy holds views alone.
    pub async fn scan_local_first(&self, tid: u64) -> Result<LocalScanReply, ClientError> {
        // A contended slot means an installed store, so it takes the hop rather
        // than delegating on a lock it could not read. The guard drops at the end
        // of this statement, which is what keeps the blocking hop below from
        // waiting on a lock this task holds.
        let unattached = matches!(self.mirror.try_lock().as_deref(), Ok(None));
        if !unattached {
            let slot = Arc::clone(&self.mirror);
            let local = blocking(move || match lock(&slot).as_mut() {
                Some(c) => c.scan_local(tid),
                None => Ok(None),
            })
            .await??;
            if let Some((schema, batch)) = local {
                return Ok((Some(schema), Some(batch), None));
            }
        }
        let (schema, batch, lsn) = self.scan(tid).await?;
        Ok((schema, batch, Some(lsn)))
    }
}

/// Owns the connection, its `AsyncFd` and the request channel; drains, steps,
/// resolves. Poll it to completion — as a spawned task, or joined beside the
/// work that feeds it. It completes with `Ok(())` once every [`AsyncClient`] is
/// dropped **and** the connection has quiesced.
///
/// Dropping a verb's future is not cancellation: the frame is written and the
/// server commits it; the driver just drops a result nobody is left to receive.
pub struct Connection {
    session: Session,
    fd: AsyncFd<OwnedFd>,
    rx: mpsc::Receiver<Submission>,
    pending: HashMap<SlotId, oneshot::Sender<Result<Reply, ClientError>>>,
}

impl Connection {
    /// Encode every submission the channel holds; `true` when the last handle
    /// is gone. At either cap the channel is left unpolled, which is what turns
    /// them into back-pressure on `AsyncClient::call` rather than the error
    /// `submit` would raise. Queued bytes always arm `write`, so the next step
    /// brings the loop back round.
    fn drain_channel(&mut self, cx: &mut Context<'_>) -> bool {
        while self.pending.len() < MAX_IN_FLIGHT && self.session.queued_bytes() < MAX_QUEUED_BYTES {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(sub)) => self.submit(sub),
                Poll::Ready(None) => return true,
                Poll::Pending => break,
            }
        }
        false
    }

    /// Encode one submission and register its slot. A request the spine refuses
    /// fails that one future and reaches no wire.
    fn submit(&mut self, sub: Submission) {
        let Submission { op, reply } = sub;
        let registered = match &op {
            Op::Push { tid, schema, batch } => self.session.submit(Request::Push {
                target_id: *tid,
                schema,
                batch,
                mode: WireConflictMode::Update,
            }),
            Op::Scan(tid) => self.session.submit(Request::scan(*tid)),
            Op::Seek(tid, pk) => self.session.submit(Request::seek(*tid, pk)),
            Op::ScanMany(tids) => self.session.submit(Request::ScanMulti(tids)),
            Op::Resolve(qname) => self.session.submit(Request::Resolve(RelTarget::Name(qname))),
        };
        match registered {
            Ok(id) => {
                self.pending.insert(id, reply);
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Abandon every pending slot with `cause`, then hand it back as this
    /// driver's own result. `ClientError` is not `Clone`, so each slot gets a
    /// rendering of it.
    fn abort(&mut self, cause: ClientError) -> ClientError {
        self.session.close();
        let text = cause.to_string();
        for (_, reply) in self.pending.drain() {
            let _ = reply.send(Err(ClientError::ServerError(text.clone())));
        }
        cause
    }
}

/// The read and write readiness guards of one poll.
type Guards<'a> = (
    Option<AsyncFdReadyGuard<'a, OwnedFd>>,
    Option<AsyncFdReadyGuard<'a, OwnedFd>>,
);

/// Give a direction's readiness back, but only if the step `consumed` it.
/// `AsyncFd` is edge-triggered, so clearing one the step did not exhaust waits
/// for an edge that never comes.
fn give_back(guard: Option<AsyncFdReadyGuard<'_, OwnedFd>>, consumed: bool) {
    if let (Some(mut g), true) = (guard, consumed) {
        g.clear_ready();
    }
}

/// Poll only the directions `want` names; one that did not fire registers its
/// waker and stays `None`. Stepping a direction that did not fire would cost a
/// syscall per wakeup to discover it was not ready.
fn poll_ready<'a>(fd: &'a AsyncFd<OwnedFd>, want: Interest, cx: &mut Context<'_>) -> io::Result<Guards<'a>> {
    Ok((
        if want.read {
            fired(fd.poll_read_ready(cx))?
        } else {
            None
        },
        if want.write {
            fired(fd.poll_write_ready(cx))?
        } else {
            None
        },
    ))
}

fn fired(
    polled: Poll<io::Result<AsyncFdReadyGuard<'_, OwnedFd>>>,
) -> io::Result<Option<AsyncFdReadyGuard<'_, OwnedFd>>> {
    match polled {
        Poll::Ready(Ok(g)) => Ok(Some(g)),
        Poll::Ready(Err(e)) => Err(e),
        Poll::Pending => Ok(None),
    }
}

impl Future for Connection {
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let handles_gone = this.drain_channel(cx);

            let want = this.session.interest();
            if want.is_empty() {
                // Quiesced: either no handle is left and this is done, or the
                // drain above registered the channel's waker.
                return if handles_gone {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                };
            }

            let (read, write) = match poll_ready(&this.fd, want, cx) {
                Ok(guards) => guards,
                Err(e) => return Poll::Ready(Err(this.abort(e.into()))),
            };
            let ready = Interest {
                read: read.is_some(),
                write: write.is_some(),
            };
            if ready.is_empty() {
                return Poll::Pending;
            }

            let stepped = this.session.step(ready);
            // A step drains the read source and flushes last, so bytes still
            // queued after one are exactly what the fd refused.
            give_back(read, true);
            give_back(write, this.session.interest().write);

            match stepped {
                Ok(done) => {
                    for (id, result) in done {
                        // A slot whose receiver is gone — its verb's future was
                        // dropped — resolves to nothing.
                        if let Some(reply) = this.pending.remove(&id) {
                            let _ = reply.send(result);
                        }
                    }
                }
                // The byte stream's framing is no longer trustworthy.
                Err(e) => return Poll::Ready(Err(this.abort(e))),
            }
        }
    }
}
