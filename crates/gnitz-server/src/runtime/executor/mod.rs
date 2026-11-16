//! Server executor: the serving loop (accept, the signal loop), the `Shared`
//! state, the request router, and the reply-frame vocabulary. Each child
//! module states its own scope.
//!
//! The master owns one `Reactor`: [`serve`] spawns its tasks on it and runs the
//! signal loop until a shutdown signal; `bootstrap` races it against a worker's
//! death.
//!
//! The router writes a request's last frame when it is the ACK naming the
//! request's own `target_id`, or a fault of the request as a whole. A scan, a
//! delta poll, a resolve and a sync write theirs.
//!
//! One connection's replies leave in request order, written by its own task.
//! The one thing another task sends a client is a pushed train, and it only
//! queues it: the connection's task ships it between two replies.

mod backup;
mod committer;
mod ddl;
mod delta;
mod family;
mod read;
mod snapshot;
mod tick;
mod validate;
mod write;

use std::cell::{Cell, Ref};
use std::os::fd::{AsFd, OwnedFd};
use std::rc::Rc;
use std::time::Duration;

use self::committer::{CommitRequests, Committer};
use self::ddl::{allocate_ids, handle_ddl_txn, reserve_serial_range};
use self::delta::{Feeds, Subscriptions, handle_delta_poll};
use self::snapshot::HeldSnapshots;
use self::tick::{TickRequests, Ticks};
use self::write::{Writes, handle_push, handle_push_txn};
use crate::catalog::CatalogEngine;
use crate::runtime::frame::WireMsg;
use crate::runtime::listen::ClientListener;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::reactor::{Peer, RecvBuf, SendBody, Train};
use crate::runtime::signals;
use crate::runtime::sync::{AsyncRwLock, Either, WriteGuard, chan, oneshot, select2};
use crate::runtime::tls::{Served, TlsListener};
use gnitz_store::relation::{Relation, RelationKind};
use gnitz_wire::{ClientVerb, Target, WireFault, WireStatus};

/// Park in 1 ms reactor ticks until `ready`, or until `polls` elapse. The shape
/// every hold-seam needs: it holds no lock while it waits, so the event it waits
/// for can actually happen, and the bound releases it if that event never comes
/// instead of wedging the node. It logs when it parks and which way it released,
/// so a test waits for the park and asserts the release rather than passing on a
/// hold that never engaged.
async fn park_until(shared: &Shared, polls: u32, what: &str, ready: impl Fn() -> bool) {
    gnitz_note!("{}: parked", what);
    for _ in 0..polls {
        if ready() {
            gnitz_note!("{}: released by its event", what);
            return;
        }
        shared.dispatcher.reactor().sleep(Duration::from_millis(1)).await;
    }
    gnitz_warn!("{}: seam armed but the event never arrived; releasing", what);
}

/// The tag of the catalog lock: a guard of it orders a task against DDL.
enum CatalogLock {}

/// Shared executor state held by every task.
///
/// Lock order: the catalog, then a table lock, then the apply lock, then the
/// SAL writer.
struct Shared {
    dispatcher: MasterDispatcher,
    committer: Committer,
    catalog_rwlock: AsyncRwLock<CatalogLock>,
    ticks: Ticks,
    writes: Writes,
    feeds: Feeds,
    /// How long a connection may take from accept to its HELLO, handshake
    /// included (`GNITZ_HELLO_TIMEOUT_MS`). The default outlasts the client's own
    /// `CONNECT_TIMEOUT`, so only a client that has already given up is reaped.
    hello_timeout: Duration,
    /// The last snapshot id handed out.
    last_snapshot: Cell<u64>,
    /// The directory BACKUP writes its entries into (`--backup-dir`); `None`
    /// refuses BACKUP.
    backup_dir: Option<String>,
}

impl Shared {
    /// The state of an executor over `dispatcher`, and the request queues of
    /// its committer, its tick loop and its delta pump.
    fn new(
        dispatcher: MasterDispatcher,
        hello_timeout: Duration,
        backup_dir: Option<String>,
    ) -> (
        Rc<Shared>,
        CommitRequests,
        TickRequests,
        chan::Receiver<oneshot::Sender<()>>,
    ) {
        let boot_seed = dispatcher.sal().watermark();
        let (committer, committer_rx) = Committer::new();
        let (ticks, tick_rx) = Ticks::new();
        let (feeds, asks) = Feeds::new();
        let shared = Rc::new(Shared {
            dispatcher,
            committer,
            catalog_rwlock: AsyncRwLock::default(),
            ticks,
            writes: Writes::new(boot_seed),
            feeds,
            hello_timeout,
            last_snapshot: Cell::new(0),
            backup_dir,
        });
        (shared, committer_rx, tick_rx, asks)
    }

    fn cat(&self) -> Ref<'_, CatalogEngine> {
        self.dispatcher.cat()
    }

    /// Drop the per-relation state of a relation whose DROP is durable: every
    /// per-relation state a drop reclaims is cleared here. Every table lock is
    /// taken under a catalog read guard, so the write guard proves no task holds
    /// or awaits the lock being removed.
    fn forget_relation(&self, _catalog_write: &WriteGuard<CatalogLock>, id: u64) {
        self.writes.forget(id);
        self.dispatcher.forget_relation(id);
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Serve `listeners` until a shutdown signal, having run the final checkpoint.
pub(super) async fn serve(dispatcher: MasterDispatcher, backup_dir: Option<String>, listeners: Vec<ClientListener>) {
    let reactor = dispatcher.reactor().clone();
    let hello_timeout = Duration::from_millis(gnitz_foundation::env::env_num(
        "GNITZ_HELLO_TIMEOUT_MS",
        (gnitz_wire::CONNECT_TIMEOUT * 3 / 2).as_millis() as u64,
    ));
    let (shared, committer_rx, tick_rx, asks) = Shared::new(dispatcher, hello_timeout, backup_dir);

    let signals = signals::Signals::open();
    // Past the last env knob the boot reads, so one it refuses stops the
    // server before it reports ready.
    gnitz_note!("{}", gnitz_foundation::log::READY_LINE);

    let tls = listeners.iter().find_map(|l| l.tls.clone());
    let syncs = Rc::clone(&shared);
    reactor.spawn(async move { syncs.dispatcher.sal().run_syncs(syncs.dispatcher.reactor()).await });
    reactor.spawn(committer::run(committer_rx, Rc::clone(&shared)));
    for listener in listeners {
        reactor.spawn(accept_loop(Rc::clone(&shared), listener));
    }
    reactor.spawn(tick::tick_loop(Rc::clone(&shared), tick_rx));
    reactor.spawn(delta::pump(Rc::clone(&shared), asks));

    serve_until_signalled(&shared, tls.as_deref(), signals).await
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

async fn accept_loop(shared: Rc<Shared>, listener: ClientListener) {
    let ClientListener { fd, tls } = listener;
    // Leaked: the loop never ends, and its accept SQEs name the fd throughout.
    let fd: &'static OwnedFd = Box::leak(Box::new(fd));
    let reactor = shared.dispatcher.reactor().clone();
    loop {
        let accepted = reactor.accept(fd.as_fd()).await;
        let served = tls.as_ref().map(|t| t.current());
        let peer = reactor.connect(accepted, served.as_ref().map(|s| s.session()));
        reactor.spawn(connection_loop(peer, Rc::clone(&shared), served));
    }
}

async fn connection_loop(mut peer: Peer, shared: Rc<Shared>, served: Option<Rc<Served>>) {
    serve_connection(&mut peer, &shared, served).await;
    // The one exit: ship what is corked, a refusal included, then retire the fd.
    let _ = peer.flush_egress().await;
    peer.close().await;
}

/// One message is handled to completion before the next is received, so
/// replies leave in request order, which is how a client pairs each with its
/// request. A sync's answer is no reply, and leaves between two.
///
/// Returns when the peer is gone or refused; the caller closes.
async fn serve_connection(peer: &mut Peer, shared: &Rc<Shared>, served: Option<Rc<Served>>) {
    let deadline = shared.dispatcher.reactor().sleep(shared.hello_timeout);
    match select2(handshake(peer, served), deadline).await {
        Either::A(true) => peer.admit_full_frames(),
        Either::A(false) => return,
        // The deadline may have cut a session's handshake flight short.
        Either::B(()) => return peer.abort(),
    }

    let mut subs = Subscriptions::default();
    let mut snapshots = HeldSnapshots::default();
    loop {
        // Ahead of the next request: a client that always has one ready is
        // still answered its sync.
        if let Some(waiting) = subs.sync_wait(shared) {
            let Ok(synced) = peer.until_request(waiting.as_mut()).await else {
                break;
            };
            if let Some(synced) = synced {
                subs.answer_sync(peer, synced).await;
                continue;
            }
        }
        let Some(buf) = peer.next_request().await else { break };
        handle_message(peer, &mut subs, &mut snapshots, buf, shared).await;
        // Behind the reply, where a pushed train splits none. A client that
        // is gone is found out by the next request.
        let _ = subs.ship(peer).await;
    }
    subs.leave(&shared.feeds, |_| true);
    snapshot::release_all(shared, snapshots).await;
}

/// Answer the client's HELLO, its verdict on the access token it carries; true
/// once the client is admitted.
async fn handshake(peer: &mut Peer, served: Option<Rc<Served>>) -> bool {
    let Some(hello) = peer.next_request().await else {
        return false;
    };
    let Ok(token) = gnitz_wire::read_client_hello(hello.as_slice()) else {
        // This build's HELLO, so a client of another version reports the
        // mismatch, and its release, which the client names.
        peer.cork_frame(&gnitz_wire::refusal_hello());
        return false;
    };
    let admitted = match served.as_deref().and_then(|s| s.tokens.as_ref()) {
        None => true,
        Some(tokens) => match tokens.check(token) {
            Some(name) => {
                gnitz_debug!("a connection authenticated with access token {name:?}");
                true
            }
            None => {
                gnitz_info!(
                    "refused the connection from {}: it holds no access token the server does",
                    peer.peer_addr()
                        .map_or_else(|| "a client".to_string(), |a| a.to_string())
                );
                false
            }
        },
    };
    peer.cork_frame(&gnitz_wire::server_hello(if admitted {
        gnitz_wire::AuthStatus::Accepted
    } else {
        gnitz_wire::AuthStatus::Refused
    }));
    admitted
}

// ---------------------------------------------------------------------------
// Signal loop: TLS reload and the final checkpoint
// ---------------------------------------------------------------------------

fn reload_tls(tls: Option<&TlsListener>) {
    let Some(tls) = tls else {
        gnitz_note!("SIGHUP: no TLS listener, nothing to reload");
        return;
    };
    match tls.reload() {
        Ok(()) => gnitz_note!("SIGHUP: reloaded the TLS listener: {}", tls.describe()),
        Err(e) => gnitz_error!("SIGHUP: the TLS reload failed, the previous setup is still served: {e}"),
    }
}

/// Until SIGTERM/SIGINT, read off `signals`: reload the TLS listener on each
/// SIGHUP. Then close the committer, which runs one final checkpoint behind
/// every write it admitted.
async fn serve_until_signalled(shared: &Shared, tls: Option<&TlsListener>, mut signals: signals::Signals) {
    loop {
        match signals.next(shared.dispatcher.reactor()).await {
            libc::SIGHUP => reload_tls(tls),
            _ => {
                gnitz_info!("shutdown signal received; draining, checkpointing, and stopping");
                // The final checkpoint ticks every push no tick has taken yet.
                shared.committer.close().await;
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(
    peer: &mut Peer,
    subs: &mut Subscriptions,
    snapshots: &mut HeldSnapshots,
    buf: RecvBuf,
    shared: &Shared,
) {
    // One control-header parse for the whole request.
    let ctrl = match gnitz_wire::peek_control_block(buf.as_slice()) {
        Ok(c) => c,
        Err(e) => {
            cork_fault(peer, 0, &decode_fault(e));
            return;
        }
    };
    let target_id = ctrl.hdr.target_id;
    if let Err(f) = dispatch_request(peer, subs, snapshots, buf, ctrl, shared).await {
        cork_fault(peer, target_id, &f);
    }
}

/// Run the verb `ctrl` names and end its reply. An arm that yields a value is
/// answered with the request's ACK carrying it; an arm that returns has written
/// its last frame itself, a fault confined to one position of its reply
/// included. An `Err` is answered by [`handle_message`] with one fault frame,
/// which fails the whole request at the client.
async fn dispatch_request(
    peer: &mut Peer,
    subs: &mut Subscriptions,
    snapshots: &mut HeldSnapshots,
    buf: RecvBuf,
    ctrl: gnitz_wire::DecodedControl,
    shared: &Shared,
) -> Result<(), WireFault> {
    let data = buf.as_slice();
    let target_id = ctrl.hdr.target_id;
    let ack = match ctrl.client_verb()? {
        // The multi-item frames name no single relation in `target_id`; each
        // decodes its items from the frame's body.
        ClientVerb::DdlTxn => handle_ddl_txn(shared, &data[ctrl.body]).await?,
        ClientVerb::PushTxn => handle_push_txn(shared, buf, ctrl).await?,
        ClientVerb::ScanMulti => {
            let scans = gnitz_wire::txn_frame::decode_scan_multi(&data[ctrl.body]).map_err(decode_fault)?;
            return read::handle_scan(shared, peer, &scans).await;
        }
        ClientVerb::DeltaPoll => return handle_delta_poll(shared, peer, subs, &ctrl.hdr, &data[ctrl.body]).await,

        // `target_id` is the sequence key (= the owning table's id).
        ClientVerb::AllocSerialRange => reserve_serial_range(shared, ctrl.hdr.target(), ctrl.hdr.arg0).await?,

        // An id allocation names no relation, so `target_id` is only echoed.
        ClientVerb::AllocIds => allocate_ids(shared, ctrl.hdr.arg0).await?,

        ClientVerb::Resolve => return read::handle_resolve(shared, peer, &data[ctrl.blob.clone()]).await,

        ClientVerb::Push => handle_push(shared, buf, ctrl).await?,

        ClientVerb::SyncPushed => {
            // Refused or not, it is answered as a sync is: out of the order of replies.
            let held = gnitz_wire::txn_frame::decode_held(&data[ctrl.blob.clone()]).map_err(decode_fault);
            subs.ask_sync(shared, held, ctrl.hdr.arg0);
            return Ok(());
        }

        // A snapshot and a backup name no relation of the live catalog in
        // `target_id`: a page names one of the snapshot's.
        ClientVerb::SnapshotOpen => snapshot::open(shared, snapshots).await?,
        ClientVerb::SnapshotPage => {
            snapshot::page(shared, peer, snapshots, &ctrl.hdr, &data[ctrl.blob.clone()]).await?;
            0
        }
        ClientVerb::SnapshotClose => {
            snapshot::close(shared, snapshots, ctrl.hdr.arg0).await?;
            0
        }
        ClientVerb::Backup => {
            backup::backup(shared, &data[ctrl.blob.clone()]).await?;
            0
        }
    };
    cork_ack(peer, target_id, ack);
    Ok(())
}

/// The fault a request whose frame does not decode is refused with.
pub(super) fn decode_fault(e: impl std::fmt::Display) -> WireFault {
    format!("decode error: {e}").into()
}

fn not_found(tid: u64) -> WireFault {
    WireFault {
        status: WireStatus::NotFound,
        text: format!("relation {tid} not found"),
    }
}

/// Which end of a relation a request wants — the discriminator of [`target_kind`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    /// Any read, a system catalog family included.
    Read,
    Write,
    /// A write inside a `PUSH_TXN`. Refusing a stream keeps every family
    /// inside the zone, so a transaction always opens one.
    TxnWrite,
}

/// Resolve `target`'s kind, rejecting a request whose descriptor token is
/// stale and a target that cannot serve `access`. The token comes first: a stale
/// one names a relation whose absence or kind is not the client's error. `0` is
/// a request that carries none.
///
/// A stream holds no rows, so it may be written (outside a transaction) but not
/// read. A view may be read
/// but not written: a push would commit rows its circuit never produced.
///
/// Enforced here even though the SQL binder refuses both: a client can push to
/// or read a relation id without going through the binder.
///
/// **Only the absent-relation arm carries a status of its own**
/// ([`WireStatus::NotFound`]): the arms below name a relation that exists, which a
/// client must not recover from the way it recovers from a vanished one.
fn target_kind(cat: &CatalogEngine, target: Target, access: Access) -> Result<RelationKind, WireFault> {
    cat.check_token(target)?;
    let target_id = target.tid;
    let Some(kind) = cat.registry.relation(target_id).map(Relation::kind) else {
        return Err(not_found(target_id));
    };
    match access {
        Access::Read if kind == RelationKind::Stream => {
            Err(format!("table {target_id} is a stream: a stream holds no rows and cannot be read").into())
        }
        Access::Write | Access::TxnWrite if !kind.is_ingestion_point() => {
            Err(format!("table {target_id} is not writable: pushes must target a base table or a stream").into())
        }
        Access::TxnWrite if kind == RelationKind::Stream => {
            Err(format!("table {target_id} is a stream: a stream cannot be written inside a transaction").into())
        }
        _ => Ok(kind),
    }
}

// ---------------------------------------------------------------------------
// Wire-protocol response helpers
// ---------------------------------------------------------------------------

/// Forward every frame of `train` that carries rows to `peer`, workers in ascending
/// order. Stops at the first failed send, which closes the peer; `Err` on the first
/// worker fault, leaving the rest undrained.
async fn forward_scan(peer: &mut Peer, train: Train<'_>) -> Result<(), WireFault> {
    while let Some(f) = train.next().await? {
        if peer.send(SendBody::Slot(f.slot)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// Cork `msg` for the client; the connection loop ships it. Every frame the
/// master mints is a bare head or a fault, whose text [`WireMsg::fault`] cuts,
/// but a RESOLVE answer, sized before it is corked, and [`cork_train`]'s.
fn cork_msg(peer: &mut Peer, msg: WireMsg<'_>) {
    debug_assert!(
        msg.check_size().is_ok(),
        "an unchecked reply frame is over MAX_FRAME_PAYLOAD"
    );
    msg.encode_framed(peer.corked())
}

/// Cork `rows` as a train of `target_id` read off the master's own copy: frames
/// of at most `MAX_FRAME_PAYLOAD`, the last flagged `scan_last`, cut by
/// [`WireMsg::next_train_frame`] as a worker's are. The ACK that follows the
/// train is [`read::handle_scan`]'s for a scan and the router's for a snapshot
/// page. `Err` is a row too wide for any frame, the frames before it corked.
fn cork_train(peer: &mut Peer, target_id: u64, rows: &gnitz_zset::repr::Batch) -> Result<(), WireFault> {
    let mut next = 0;
    loop {
        let (msg, n) = WireMsg::next_train_frame(target_id, rows, next, gnitz_wire::MAX_FRAME_PAYLOAD, true)?;
        cork_msg(peer, msg);
        if msg.flags.scan_last {
            return Ok(());
        }
        next = n;
    }
}

/// A control-only ACK: the request's own `target_id`, and its one value — a
/// write's LSN, an allocation's base id, or the watermark of the read whose
/// train it ends (see [`tick::read_watermark`]) — in `arg0`.
fn cork_ack(peer: &mut Peer, target_id: u64, value: u64) {
    cork_msg(
        peer,
        WireMsg {
            target_id,
            arg0: value,
            ..Default::default()
        },
    )
}

/// A failure carrying its own status, master-minted or forwarded from a worker.
/// A failure describes no rows, so it carries no schema block.
fn cork_fault(peer: &mut Peer, target_id: u64, fault: &WireFault) {
    cork_msg(peer, WireMsg { target_id, ..WireMsg::fault(fault) })
}

#[cfg(test)]
#[path = "fuzz/executor.rs"]
mod fuzz;

#[cfg(test)]
#[path = "tests/executor.rs"]
mod tests;
