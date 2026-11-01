//! A connection's sync, and the verbs of a reader that keeps its own copy:
//! subscribe, then sync, which answers with the view's deltas since and
//! advances every mirrored view.

use std::sync::{Arc, PoisonError};
use std::time::Duration;

use crate::batch::decode_wal_blocks_into;
use crate::client::GnitzClient;
use crate::connection::{DeltaCursor, RawBlock, ScanReply, SubId, Target};
use crate::error::ClientError;
use crate::host::{promise_returning, Promise, Sent};
use crate::mirror::PollOutcome;
use crate::{Schema, ZSetBatch};
use gnitz_wire::txn_frame::kept_as;

/// What a sync hands a reader for one of its subscriptions.
pub struct Pushed {
    /// The id [`GnitzClient::subscribe`] returned.
    pub sub: SubId,
    /// The deltas pushed since the last sync and the cursor past them, or
    /// the end of the subscription.
    pub result: Result<(ScanReply, DeltaCursor), ClientError>,
}

/// What one sync brought a client.
#[must_use]
pub struct Synced {
    /// One entry for each subscription [`GnitzClient::subscribe`] made, in
    /// the order they were made.
    pub pushed: Vec<Pushed>,
    /// One entry for each mirrored view, at the id it is mirrored under now.
    pub mirrored: Vec<PollOutcome>,
}

impl GnitzClient {
    /// A delta read of `view` after `from` — whole with none — that stays
    /// subscribed from the cursor it answers with: the subscription's id, the
    /// rows and that cursor.
    pub async fn subscribe(
        &mut self,
        view: impl Into<Target>,
        from: Option<DeltaCursor>,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
    ) -> Result<(SubId, ScanReply, DeltaCursor), ClientError> {
        let id = self.next_sub;
        self.next_sub = kept_as(id, 1);
        let sent = self
            .session
            .submit_delta_read(view.into(), from, reply_schema, spec, Some(id));
        let (rows, cursor) = self.wait(sent).await?;
        self.readers.insert(id, Arc::clone(reply_schema));
        Ok((id, rows, cursor))
    }

    /// End subscription `id`: no sync reports it again. The next sync tells
    /// the server, and drops what was pushed for `id` until then. An id this
    /// client does not hold is ignored.
    pub fn unsubscribe(&mut self, id: SubId) {
        // A mirror's subscription ids come from the same counter, so only a
        // reader's id is released.
        if self.readers.remove(&id).is_some() {
            self.session.release(id);
        }
        self.reclaim();
        self.unclaimed.retain(|p| p.sub != id);
    }

    /// Bring every subscription of this connection, a reader's and a
    /// mirror's, up to every push acknowledged before the call. With nothing
    /// to report, the server holds the reply for up to `wait`.
    ///
    /// `Err` fails the sync as a whole; what failed one subscription or view
    /// is in its entry.
    pub async fn sync(&mut self, wait: Duration) -> Result<Synced, ClientError> {
        let synced = self.begin_sync(wait);
        let synced = self.wait(synced).await;
        if let Err(e @ ClientError::Interrupted(_)) = synced {
            return Err(e);
        }
        self.finish_sync(synced).await
    }

    /// The first half of [`Self::sync`]: its request, sent. The client is free
    /// until the answer arrives: a request made meanwhile is answered while the
    /// server holds the sync, and leaves it held.
    pub fn begin_sync(&mut self, wait: Duration) -> Sent<()> {
        self.reclaim();
        let wait = if self.unclaimed.is_empty() {
            wait
        } else {
            Duration::ZERO
        };
        let (wait, mirrored) = self.mirror_hold(wait);
        let held: Vec<SubId> = self.readers.keys().copied().chain(mirrored).collect();
        self.session.submit_sync(&held, wait)
    }

    /// The second half of [`Self::sync`], given the reply to the first's
    /// request. A sync that failed is the end of every subscription.
    ///
    /// What it takes from the client is the caller's alone: a driver whose
    /// caller is gone by the time it completes hands it to
    /// [`Self::unclaimed`], or delivers it through a [`Self::sync_reply`] cell.
    pub async fn finish_sync(&mut self, synced: Result<(), ClientError>) -> Result<Synced, ClientError> {
        self.reclaim();
        let mirrored = self.advance_mirror(&synced).await?;
        let mut pushed = self.drain_readers(&synced);
        put_in_front(std::mem::take(&mut self.unclaimed), &mut pushed);
        Ok(Synced { pushed, mirrored })
    }

    /// A cell for [`Self::finish_sync`]'s delivery whose [`Sent`], dropped
    /// with the delivery unread, hands it back as [`Self::unclaimed`] does.
    pub fn sync_reply(&self) -> (Promise<Synced>, Sent<Synced>) {
        let returned = Arc::clone(&self.returned);
        promise_returning(move |synced| returned.lock().unwrap_or_else(PoisonError::into_inner).push(synced))
    }

    /// Hand back a sync's delivery its caller never received: the next sync
    /// reports it in front of what that one brings, and is not held. Each
    /// subscription's rows come before the next sync's, under that sync's
    /// cursor, and an end is reported once; a view reported
    /// [`PollResult::Reseeded`](crate::PollResult::Reseeded) is reported so
    /// again by the next sync that advances it.
    pub fn unclaimed(&mut self, synced: Synced) {
        let Synced { mut pushed, mirrored } = synced;
        if let Some(m) = self.mirror.as_deref_mut() {
            m.owe_reseeds(&mirrored);
        }
        put_in_front(std::mem::take(&mut self.unclaimed), &mut pushed);
        // Rows-free entries carry nothing the next sync's cursor does not.
        pushed.retain(|p| p.result.as_ref().map_or(true, |(rows, _)| !rows.batch.is_empty()));
        self.unclaimed = pushed;
    }

    /// Take up what [`Self::sync_reply`] cells handed back.
    fn reclaim(&mut self) {
        let returned = std::mem::take(&mut *self.returned.lock().unwrap_or_else(PoisonError::into_inner));
        for synced in returned {
            self.unclaimed(synced);
        }
    }

    /// Take what was pushed for each of a reader's subscriptions, and let go
    /// of the ones that ended.
    fn drain_readers(&mut self, synced: &Result<(), ClientError>) -> Vec<Pushed> {
        let GnitzClient { session, readers, .. } = self;
        let mut pushed = Vec::with_capacity(readers.len());
        readers.retain(|&sub, schema| {
            let taken = synced.clone().and_then(|()| session.take_pushed(sub));
            let result = taken.and_then(|(blocks, cursor)| {
                let mut batch = ZSetBatch::new(schema);
                decode_wal_blocks_into(&mut batch, blocks.iter().map(RawBlock::block), schema)?;
                let schema = Arc::clone(schema);
                Ok((ScanReply { schema, batch, lsn: None }, cursor))
            });
            let held = result.is_ok();
            pushed.push(Pushed { sub, result });
            held
        });
        pushed
    }
}

/// Put the deliveries of an earlier sync, `earlier`, in front of a later one's,
/// `later`: a subscription's rows before the later sync's under its cursor,
/// and an end that sync no longer reports kept. A subscription `later` does
/// not report and that had not ended was let go of since.
fn put_in_front(earlier: Vec<Pushed>, later: &mut Vec<Pushed>) {
    for Pushed { sub, result } in earlier {
        match (result, later.iter_mut().find(|p| p.sub == sub)) {
            (Ok((first, _)), Some(Pushed { result: Ok((rows, _)), .. })) => {
                let next = std::mem::replace(&mut rows.batch, first.batch);
                rows.batch.extend_from_owned(next);
            }
            // The later end stands: the reader reads again from the cursor
            // it was last handed, which is before these rows.
            (Ok(_), _) => {}
            (Err(end), Some(p)) => p.result = Err(end),
            (Err(end), None) => later.push(Pushed { sub, result: Err(end) }),
        }
    }
    later.sort_by_key(|p| p.sub);
}
