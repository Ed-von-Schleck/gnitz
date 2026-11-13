//! Snapshots: every base table and system family pinned as one point of the
//! log holds them, and read back a page at a time while writes go on.
//!
//! A pin is a read written to every worker at one SAL cut, so every worker pins
//! after exactly the commits ahead of it; the master pins its system families
//! inside the same cut. Nothing is copied: a pin keeps runs and shard mappings
//! alive past the folds and compactions that replace them, so what a snapshot
//! costs is that memory and disk come back only when it is released.
//!
//! A snapshot is its connection's: refused to any other, released by
//! SNAPSHOT_CLOSE or when the connection ends.

use super::{cork_train, decode_fault, Shared};
use crate::runtime::reactor::{Peer, SendBody};
use crate::runtime::sal::Read;
use gnitz_store::relation::Snapshot;
use gnitz_wire::snapshot::PageRequest;
use gnitz_wire::{ControlHeader, WireFault};

/// The snapshots one connection may hold at a time.
const MAX_SNAPSHOTS_PER_CONNECTION: usize = 4;

/// The snapshots one connection holds, by id, each with the master's own pin.
#[derive(Default)]
pub(super) struct HeldSnapshots(Vec<(u64, Snapshot)>);

/// SNAPSHOT_OPEN: pin, and hold the snapshot for this connection.
pub(super) async fn open(shared: &Shared, held: &mut HeldSnapshots) -> Result<u64, WireFault> {
    if held.0.len() >= MAX_SNAPSHOTS_PER_CONNECTION {
        return Err(format!("this connection holds {MAX_SNAPSHOTS_PER_CONNECTION} snapshots; close one first").into());
    }
    let id = shared.last_snapshot.get() + 1;
    shared.last_snapshot.set(id);
    // No DDL moves the catalog between the master's pin and the workers'.
    let catalog = shared.catalog_rwlock.read().await;
    let mut pinned = None;
    let cut = shared
        .dispatcher
        .scan_cut(|cut| {
            cut.read(Read::Pin { snapshot: id })?;
            pinned = Some(shared.cat().registry.pin());
            Ok(())
        })
        .await;
    drop(catalog);
    let mut lease = cut?;
    if let Err(e) = lease.next_train().await.finish().await {
        release(shared, id).await;
        return Err(e);
    }
    held.0.push((id, pinned.expect("an accepted cut pinned")));
    Ok(id)
}

/// SNAPSHOT_PAGE: a page of `hdr.target_id` as snapshot `hdr.arg0` holds it.
/// The master answers a relation its own pin holds — a system family — and
/// every worker the rest, each worker's train forwarded whole in rank order,
/// its row-less frames and its terminal included, so the client reads one
/// `scan_last` frame per worker and reads each worker's last key by its
/// position. The router's ACK follows the trains and ends the page.
pub(super) async fn page(
    shared: &Shared,
    peer: &mut Peer,
    held: &HeldSnapshots,
    hdr: &ControlHeader,
    blob: &[u8],
) -> Result<(), WireFault> {
    let (id, tid) = (hdr.arg0, hdr.target_id);
    let Some((_, pin)) = held.0.iter().find(|(h, _)| *h == id) else {
        return Err(format!("snapshot {id} is not this connection's").into());
    };
    let request = PageRequest::decode(blob).map_err(decode_fault)?;
    if pin.holds(tid) {
        let rows = pin.page(tid, &request)?;
        cork_train(peer, tid, &rows)?;
    } else {
        let read = Read::SnapshotPage { snapshot: id, tid, page: blob };
        let mut lease = shared.dispatcher.scan(read).await?;
        let train = lease.next_train().await;
        'forward: for w in train.workers().iter() {
            while let Some((slot, _)) = train.frame_of(w).await? {
                if peer.send(SendBody::Slot(slot)).await.is_err() {
                    break 'forward;
                }
            }
        }
    }
    Ok(())
}

/// SNAPSHOT_CLOSE: release snapshot `id` this connection holds.
pub(super) async fn close(shared: &Shared, held: &mut HeldSnapshots, id: u64) -> Result<(), WireFault> {
    let Some(at) = held.0.iter().position(|(h, _)| *h == id) else {
        return Err(format!("snapshot {id} is not this connection's").into());
    };
    held.0.swap_remove(at);
    release(shared, id).await;
    Ok(())
}

/// Release every snapshot a connection that has ended held.
pub(super) async fn release_all(shared: &Shared, held: HeldSnapshots) {
    for (id, pin) in held.0 {
        drop(pin);
        release(shared, id).await;
    }
}

/// Release snapshot `id` on every worker. A worker that cannot answer
/// the release has left the cluster, which ends the server; the one failure
/// returned is the SAL refusing the Unpin group, after which the workers keep
/// the pin until restart, so it is logged rather than fail-stopped.
async fn release(shared: &Shared, id: u64) {
    let released = shared.dispatcher.command(Read::Unpin { snapshot: id }).await;
    if let Err(e) = released {
        gnitz_warn!("snapshot {id}: release failed: {}", e.text);
    }
}
