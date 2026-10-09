//! A connection's sync, and the verbs of a reader that keeps its own copy:
//! subscribe, then sync, which answers with the view's deltas since and
//! advances every mirrored view.

use std::sync::Arc;
use std::time::Duration;

use crate::client::GnitzClient;
use crate::connection::{DeltaCursor, ScanReply, Sent, Target};
use crate::error::ClientError;
use crate::mirror::PollOutcome;
use crate::protocol::wal_block::decode_wal_block_into;
use crate::{Schema, ZSetBatch};

/// What a sync hands a reader for one of its subscriptions.
pub struct Pushed {
    /// The id [`GnitzClient::subscribe`] returned.
    pub sub: u64,
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
    ) -> Result<(u64, ScanReply, DeltaCursor), ClientError> {
        let id = self.next_sub;
        self.next_sub += 1;
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
    pub fn unsubscribe(&mut self, id: u64) {
        self.readers.remove(&id);
    }

    /// Bring every subscription of this connection, a reader's and a
    /// mirror's, up to every push acknowledged before the call. With nothing
    /// to report, the server holds the reply for up to `wait`.
    ///
    /// `Err` fails the sync as a whole; what failed one subscription or view
    /// is in its entry.
    pub async fn sync(&mut self, wait: Duration) -> Result<Synced, ClientError> {
        let synced = self.begin_sync(wait)?;
        let synced = self.wait(synced).await;
        if let Err(e @ ClientError::Interrupted(_)) = synced {
            return Err(e);
        }
        self.finish_sync(synced).await
    }

    /// The first half of [`Self::sync`]: its request, sent. The client is free
    /// until the answer arrives: a request made meanwhile is answered while the
    /// server holds the sync, and leaves it held.
    pub fn begin_sync(&mut self, wait: Duration) -> Result<Sent<()>, ClientError> {
        let wait = self.mirror_hold(wait)?;
        let mirrored = self.mirror.iter().flat_map(|m| m.views.values().filter_map(|v| v.sub));
        let held: Vec<u64> = self.readers.keys().copied().chain(mirrored).collect();
        Ok(self.session.submit_sync(&held, wait))
    }

    /// The second half of [`Self::sync`], given the reply to the first's
    /// request. A sync that failed is the end of every subscription.
    pub async fn finish_sync(&mut self, synced: Result<(), ClientError>) -> Result<Synced, ClientError> {
        let mirrored = self.advance_mirror(&synced).await?;
        let pushed = self.drain_readers(&synced);
        Ok(Synced { pushed, mirrored })
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
                for block in blocks {
                    decode_wal_block_into(&mut batch, block.block(), schema)?;
                }
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
