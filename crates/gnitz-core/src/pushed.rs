//! The subscriptions one connection holds, and the verbs of a reader that
//! keeps its own copy: subscribe from a cursor a delta read handed out, then
//! sync, which answers with the deltas the server pushed since.
//!
//! A mirror's subscriptions and a reader's share the connection: one sync
//! brings both up to date, and each takes what was pushed for its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use gnitz_wire::txn_frame::DeltaPollItem;

use crate::client::GnitzClient;
use crate::connection::{DeltaCursor, Request, ScanReply, Sent, Session, Target};
use crate::error::ClientError;
use crate::protocol::wal_block::decode_wal_block_into;
use crate::{Schema, ZSetBatch};

/// A reader's subscription: the layout its trains decode under, and the
/// cursor past everything it has been handed.
struct Held {
    schema: Arc<Schema>,
    cursor: DeltaCursor,
}

/// The ids a client's subscriptions are asked for under, and a reader's own.
/// What was pushed for each is its [`Session`]'s, which a replaced connection
/// starts without: so an id is never asked for twice.
pub(crate) struct Subscriptions {
    /// The id the next subscription is asked for under.
    next: u64,
    /// A reader's own, by id. A mirror keeps its with the views they feed.
    held: HashMap<u64, Held>,
}

/// Where a client's subscriptions stood when a sync was sent: the ones asked
/// for by then, which its answer speaks for, are the ids below this.
pub struct SyncMark(pub(crate) u64);

impl Default for Subscriptions {
    fn default() -> Self {
        Subscriptions { next: 1, held: HashMap::new() }
    }
}

impl Subscriptions {
    /// Subscribe to each of `items` in one request nothing waits for: item `i`
    /// under the id returned plus `i`.
    pub(crate) fn ask(&mut self, session: &mut Session, items: &[DeltaPollItem]) -> Result<u64, ClientError> {
        let first = self.next;
        session.subscribe(first, items)?;
        self.next += items.len() as u64;
        Ok(first)
    }
}

/// What a sync hands a reader for one of its subscriptions.
pub struct Pushed {
    /// The id [`GnitzClient::subscribe`] returned.
    pub sub: u64,
    /// The deltas pushed since the last sync — none, when nothing reached the
    /// subscription — and the cursor past them. An `Err` is the end of the
    /// subscription: continue the copy with a delta read from its last cursor,
    /// which finds out why, and subscribe again from there.
    pub result: Result<(ScanReply, DeltaCursor), ClientError>,
}

/// What was pushed for `held`, the subscription `sub`, since it was last
/// handed any, and its cursor moved past that. `synced` is the answer of a
/// sync sent after `sub` was asked for.
fn drain(
    session: &mut Session,
    sub: u64,
    held: &mut Held,
    synced: &Result<u64, ClientError>,
) -> Result<(ScanReply, DeltaCursor), ClientError> {
    let (blocks, cursor) = session.take_pushed(sub, held.cursor, synced.clone()?)?;
    let mut batch = ZSetBatch::new(&held.schema);
    for block in &blocks {
        decode_wal_block_into(&mut batch, block.block(), &held.schema)?;
    }
    held.cursor = cursor;
    let schema = Arc::clone(&held.schema);
    Ok((ScanReply { schema, batch, lsn: None }, cursor))
}

impl GnitzClient {
    /// Subscribe this connection to a view's delta feed from `cursor`, which a
    /// delta read of the same view under the same `spec` handed out. Returns
    /// the subscription's id, at once: [`Self::sync_pushed`] reports a
    /// subscription the server refused as one that ended.
    ///
    /// The deltas come back in `reply_schema`, weights and all.
    pub fn subscribe(
        &mut self,
        view: impl Into<Target>,
        cursor: DeltaCursor,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
    ) -> Result<u64, ClientError> {
        let item = DeltaCursor::item(Some(cursor), view.into(), reply_schema, spec);
        let id = self.subs.ask(&mut self.session, &[item])?;
        let schema = Arc::clone(reply_schema);
        self.subs.held.insert(id, Held { schema, cursor });
        Ok(id)
    }

    /// End subscription `id`. Nothing waits for the answer — a train of it
    /// still on its way is dropped — and an id this client does not hold is
    /// ignored.
    pub fn unsubscribe(&mut self, id: u64) {
        if self.subs.held.remove(&id).is_some() {
            self.session.unsubscribe([id]);
        }
    }

    /// Bring this client's subscriptions up to every push acknowledged before
    /// the call, and hand back what was pushed for each. With nothing to
    /// report, the server holds the reply until a round leaves one of the
    /// connection's subscriptions a row it keeps, or `wait` passes.
    ///
    /// `Err` is an interrupt; any other failure is each subscription's own.
    pub async fn sync_pushed(&mut self, wait: Duration) -> Result<Vec<Pushed>, ClientError> {
        let (mark, synced) = self.begin_sync_pushed(wait);
        let synced = self.wait(synced).await;
        if let Err(e @ ClientError::Interrupted(_)) = synced {
            return Err(e);
        }
        Ok(self.finish_sync_pushed(mark, synced))
    }

    /// The first half of [`Self::sync_pushed`]: its one request, sent. Its
    /// reply is the second half's, and the client is free until it arrives — a
    /// request made meanwhile ends the hold.
    pub fn begin_sync_pushed(&mut self, wait: Duration) -> (SyncMark, Sent<u64>) {
        self.begin_sync(!self.subs.held.is_empty(), wait)
    }

    /// One SYNC_PUSHED, sent — none with nothing `subscribed`, which is
    /// answered as one that held no subscription — and where this client's
    /// subscriptions stood.
    pub(crate) fn begin_sync(&mut self, subscribed: bool, wait: Duration) -> (SyncMark, Sent<u64>) {
        let mark = SyncMark(self.subs.next);
        let synced = match subscribed {
            true => self.ack(Request::SyncPushed(wait)).detach(),
            false => Sent::ready(Ok(0)),
        };
        (mark, synced)
    }

    /// The second half of [`Self::sync_pushed`], given the reply to the
    /// first's request: one entry for each subscription held when that was
    /// sent, in the order they were made. One made since is the next sync's.
    pub fn finish_sync_pushed(&mut self, mark: SyncMark, synced: Result<u64, ClientError>) -> Vec<Pushed> {
        let GnitzClient { session, subs, .. } = self;
        let mut ids: Vec<u64> = subs.held.keys().copied().filter(|id| *id < mark.0).collect();
        ids.sort_unstable();
        let mut pushed = Vec::with_capacity(ids.len());
        for sub in ids {
            let held = subs.held.get_mut(&sub).expect("just listed");
            let result = drain(session, sub, held, &synced);
            if result.is_err() {
                subs.held.remove(&sub);
            }
            pushed.push(Pushed { sub, result });
        }
        // One whose sync failed may still be held by the server.
        session.unsubscribe(pushed.iter().filter(|p| p.result.is_err()).map(|p| p.sub));
        pushed
    }
}
