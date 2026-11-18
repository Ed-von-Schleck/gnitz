//! A connection's sync, and the verbs of a reader that keeps its own copy:
//! subscribe, then sync, which answers with the view's deltas since and
//! advances every mirrored view.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::Schema;
use crate::batch::decode_wal_blocks;
use crate::client::GnitzClient;
use crate::connection::{Held, ScanReply, park, untaken};
use crate::error::ClientError;
use crate::host::{Op, Sent, finished};
use crate::mirror::PollOutcome;
use gnitz_wire::Target;
use gnitz_wire::multi_frame::{DeltaCursor, SubId, kept_as};

/// What a sync hands a reader for one of its subscriptions.
pub struct Pushed {
    /// The id [`GnitzClient::subscribe`] returned.
    pub sub: SubId,
    /// The deltas pushed since the last sync and the cursor past them, or
    /// the end of the subscription.
    pub result: Result<(ScanReply, DeltaCursor), ClientError>,
}

/// A sync's [`Synced`], on its way to a caller who may be gone. What syncs
/// brought a reader's subscriptions stays with the client until a delivery is
/// taken, and a sync while any of it waits is not held. Taking one hands over
/// each subscription's rows so far, in arrival order under the newest cursor,
/// and an end once. Dropped untaken, a delivery hands back only its reseeds:
/// a view it reports [`PollResult::Reseeded`](crate::PollResult::Reseeded) is
/// reported so again by the next sync that advances it.
pub struct Delivery {
    /// `None` once taken.
    mirrored: Option<Vec<PollOutcome>>,
    received: Arc<Mutex<Received>>,
}

impl Delivery {
    pub fn take(mut self) -> Synced {
        let mirrored = self.mirrored.take().unwrap_or_default();
        // Moved out under the lock and decoded after it, so a large train
        // never holds up the client's own use of `received`.
        let taken: Vec<_> = {
            let mut received = lock(&self.received);
            let mut taken = Vec::with_capacity(received.readers.len());
            received.readers.retain(|&sub, Reader { schema, held }| {
                let result = match held {
                    Ok((blocks, cursor)) => Ok((Arc::clone(schema), std::mem::take(blocks), *cursor)),
                    Err(why) => Err(why.clone()),
                };
                let kept = result.is_ok();
                taken.push((sub, result));
                kept
            });
            taken
        };
        let pushed: Vec<Pushed> = taken
            .into_iter()
            .map(|(sub, taken)| Pushed {
                sub,
                result: taken.and_then(|(schema, blocks, cursor)| {
                    let batch = decode_wal_blocks(blocks, None, &schema)?;
                    Ok((ScanReply { schema, batch, lsn: None }, cursor))
                }),
            })
            .collect();
        // A train that does not decode ends its subscription, as an end does.
        let mut received = lock(&self.received);
        for p in pushed.iter().filter(|p| p.result.is_err()) {
            received.readers.remove(&p.sub);
        }
        drop(received);
        Synced { pushed, mirrored }
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if let Some(mirrored) = self.mirrored.take() {
            let reseeded = mirrored.iter().filter(|o| o.result.reseeded()).map(|o| o.view_id);
            lock(&self.received).reseeded.extend(reseeded);
        }
    }
}

/// What a reader's subscriptions were brought and not yet handed over, shared
/// by a client and its [`Delivery`]s.
#[derive(Default)]
pub(crate) struct Received {
    /// Each of a reader's subscriptions by id, in the order they were made.
    readers: BTreeMap<SubId, Reader>,
    /// The views reported reseeded by deliveries dropped untaken, owed their
    /// report again by the next sync.
    reseeded: Vec<u64>,
}

struct Reader {
    /// The layout its trains decode under.
    schema: Arc<Schema>,
    held: Held,
}

fn lock(received: &Mutex<Received>) -> MutexGuard<'_, Received> {
    received.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What one sync brought a client.
#[must_use]
pub struct Synced {
    /// One entry for each subscription [`GnitzClient::subscribe`] made and
    /// held when the delivery was taken, in the order they were made. A
    /// delivery taken late carries the trains of syncs since, under the
    /// newest cursor.
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
        let schema = Arc::clone(reply_schema);
        let held = Ok((Vec::new(), cursor));
        lock(&self.received).readers.insert(id, Reader { schema, held });
        Ok((id, rows, cursor))
    }

    /// End subscription `id`: no sync reports it again. The next sync tells
    /// the server, and drops what was pushed for `id` until then, taken by a
    /// sync or not. An id this client does not hold is ignored.
    pub fn unsubscribe(&mut self, id: SubId) {
        // A mirror's subscription ids come from the same counter, so only a
        // reader's id is released.
        if lock(&self.received).readers.remove(&id).is_some() {
            self.session.release(id);
        }
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
        self.finish_sync(synced).await.map(Delivery::take)
    }

    /// The first half of [`Self::sync`]: its request, sent. The client is free
    /// until the answer arrives: a request made meanwhile is answered while the
    /// server holds the sync, and leaves it held.
    pub fn begin_sync(&mut self, wait: Duration) -> Sent<()> {
        let (wait, readers) = {
            let mut received = lock(&self.received);
            if let Some(m) = self.mirror.as_deref_mut() {
                m.owe_reseeds(&received.reseeded);
            }
            received.reseeded.clear();
            let untaken = received.readers.values().any(|r| untaken(&r.held));
            let held = received.readers.iter().filter(|(_, r)| r.held.is_ok());
            let held: Vec<SubId> = held.map(|(&id, _)| id).collect();
            (if untaken { Duration::ZERO } else { wait }, held)
        };
        let (wait, mirrored) = self.mirror_hold(wait);
        let mut held = readers;
        held.extend(mirrored);
        self.session.submit_sync(&held, wait)
    }

    /// The second half of [`Self::sync`], given the reply to the first's
    /// request. A sync that failed is the end of every subscription.
    ///
    /// What the sync brought a reader's subscriptions stays with the client
    /// until the [`Delivery`] is taken.
    pub async fn finish_sync(&mut self, synced: Result<(), ClientError>) -> Result<Delivery, ClientError> {
        let mirrored = self.advance_mirror(&synced).await?;
        let GnitzClient { session, received, .. } = self;
        for (&sub, reader) in &mut lock(received).readers {
            // An ended one keeps its end until a delivery reports it.
            if reader.held.is_ok() {
                park(&mut reader.held, synced.clone().and_then(|()| session.take_pushed(sub)));
            }
        }
        Ok(Delivery {
            mirrored: Some(mirrored),
            received: Arc::clone(received),
        })
    }

    /// The first parked sync whose answer has arrived, as the op that finishes
    /// it and lands its delivery.
    pub(crate) fn answered_sync(&mut self) -> Option<Op> {
        let (i, reply) = (self.syncs.iter_mut().enumerate()).find_map(|(i, (sent, _))| Some((i, sent.try_take()?)))?;
        let (_, land) = self.syncs.remove(i);
        Some(Box::new(move |client| {
            Box::pin(async move { land(client.finish_sync(reply).await) })
        }))
    }
}

/// Where a [`sync_op`]'s delivery goes.
pub(crate) type Land = Box<dyn FnOnce(Result<Delivery, ClientError>) + Send>;

/// A sync as one queued call of a [`serve`](crate::serve) loop: its request
/// goes out and is parked on the client; once its answer has arrived, `serve`
/// finishes it and hands the [`Delivery`] to `land` before it waits again, and
/// serves on until it has. The sync is finished
/// whether or not its caller is still there; a delivery nobody takes hands back
/// its reseeds, and what the sync brought a reader stays with the client until
/// one is taken.
pub fn sync_op(wait: Duration, land: impl FnOnce(Result<Delivery, ClientError>) + Send + 'static) -> Op {
    Box::new(move |client| {
        let sent = client.begin_sync(wait);
        client.syncs.push((sent, Box::new(land)));
        finished()
    })
}
