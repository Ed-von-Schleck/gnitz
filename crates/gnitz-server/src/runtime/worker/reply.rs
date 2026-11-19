//! The worker's reply queue, over the ring its frames leave on: what the worker
//! owes the master, the one rule that decides when a reply leaves, the sender
//! every train's frames go through, and when a refusal of room asks the master
//! for the ring back.

use std::collections::VecDeque;

use crate::runtime::frame::WireMsg;
use crate::runtime::sal::{KeptRead, Read, ReplyRoute};
use crate::runtime::w2m::{RequestId, W2mWriter};
use gnitz_store::read::{KeySpans, ReadRows};
use gnitz_wire::WireFault;
use gnitz_zset::repr::Batch;

pub(super) struct Replies {
    w2m: W2mWriter,
    /// Replies this worker owes, in the order their requests were read off the
    /// SAL: one lease's are consecutive.
    owed: VecDeque<Owed>,
}

/// One reply this worker owes.
// One per queued reply, which is not the path a one-frame reply takes.
#[allow(clippy::large_enum_variant)]
enum Owed {
    /// A read of a view, received while a drive was writing views. It is kept:
    /// it is answered after the drive, which reads further groups.
    Parked(ReplyRoute, KeptRead),
    Train(Train),
}

impl Owed {
    /// The lease it answers on.
    fn id(&self) -> RequestId {
        match self {
            Owed::Parked(route, _) | Owed::Train(Train { route, .. }) => route.request_id,
        }
    }
}

/// A reply, and how far into it its frames have come.
struct Train {
    route: ReplyRoute,
    /// The rows it answers with, or the fault that is its one frame.
    answer: Result<Rows, WireFault>,
    /// Rows of the batch, or of the current span chunk, already sent.
    next_row: usize,
    /// The release cursor the ring refused its next frame at: until a release
    /// moves it the frame cannot fit, and is not cut again to learn so.
    refused_at: Option<u64>,
}

/// The rows a read answers with.
// One per reply, so the variants' size is immaterial; boxing the batch would
// cost every probe an allocation.
#[allow(clippy::large_enum_variant)]
pub(super) enum Rows {
    /// One batch, cut into frames.
    Batch(ReadRows),
    /// Sorted key spans, a chunk at a time; the first empty chunk ends them.
    Spans(Box<KeySpans>),
    /// A read answered with no rows: its train is one empty last frame.
    Nothing,
}

impl From<ReadRows> for Rows {
    fn from(rows: ReadRows) -> Self {
        Rows::Batch(rows)
    }
}

impl From<Batch> for Rows {
    fn from(rows: Batch) -> Self {
        Rows::Batch(ReadRows::Own(rows))
    }
}

impl Train {
    /// `answer` as the reply to `route`, none of it sent.
    fn new(route: ReplyRoute, answer: Result<Rows, WireFault>) -> Self {
        Train {
            route,
            answer,
            next_row: 0,
            refused_at: None,
        }
    }

    /// Send the next frame, if the ring has room for it now: `Ok(last)`, `last`
    /// once it was the last; `Err` refused, with the release cursor the ring
    /// weighed. A row too wide for any frame, or for the ring, turns the train
    /// into that fault.
    fn emit_next(&mut self, w2m: &mut W2mWriter) -> Result<bool, u64> {
        let id = self.route.request_id;
        loop {
            let (batch, ends) = match &mut self.answer {
                Err(fault) => return w2m.try_send_msg(id, false, &WireMsg::fault(0, fault)).map(|()| true),
                Ok(Rows::Nothing) => {
                    let msg = WireMsg::train_frame(self.route.target_id, true, None);
                    return w2m.try_send_msg(id, false, &msg).map(|()| true);
                }
                Ok(Rows::Batch(batch)) => (&**batch, true),
                Ok(Rows::Spans(spans)) => {
                    if self.next_row == spans.chunk().len() {
                        spans.advance();
                        self.next_row = 0;
                    }
                    // An empty chunk: every span is sent.
                    (spans.chunk(), spans.chunk().is_empty())
                }
            };
            let fault =
                match WireMsg::next_train_frame(self.route.target_id, batch, self.next_row, w2m.frame_budget(), ends) {
                    Ok((msg, _)) if msg.size() > w2m.max_message_bytes() => {
                        format!("a row of {} bytes exceeds what the reply ring holds", msg.size()).into()
                    }
                    Ok((msg, next_row)) => {
                        w2m.try_send_msg(id, false, &msg)?;
                        self.next_row = next_row;
                        return Ok(msg.hdr.flags.scan_last);
                    }
                    Err(fault) => fault,
                };
            self.answer = Err(fault);
        }
    }
}

/// `answer` as the one frame of its reply to `route`, when it is one within
/// `budget`.
fn whole<'a>(route: &ReplyRoute, answer: &'a Result<Rows, WireFault>, budget: usize) -> Option<WireMsg<'a>> {
    match answer {
        Err(fault) => Some(WireMsg::fault(0, fault)),
        Ok(Rows::Spans(_)) => None,
        Ok(Rows::Nothing) => Some(WireMsg::train_frame(route.target_id, true, None)),
        Ok(Rows::Batch(batch)) => {
            let msg = WireMsg::train_frame(route.target_id, true, batch.wire_whole());
            (msg.data.is_none() || msg.size() <= budget).then_some(msg)
        }
    }
}

impl Replies {
    pub(super) fn new(w2m: W2mWriter) -> Self {
        Replies { w2m, owed: VecDeque::new() }
    }

    /// Nothing is owed.
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.owed.is_empty()
    }

    /// Publish that this worker has applied the log through `at`: in the
    /// ring's header, which no owed reply holds up.
    #[inline]
    pub(super) fn applied(&mut self, at: u64) {
        self.w2m.applied(at);
    }

    /// Owe `route` the answer of `read`, once [`Self::answer_parked`] asks for it.
    pub(super) fn park(&mut self, route: ReplyRoute, read: &Read<'_>) {
        self.owed.push_back(Owed::Parked(route, read.keep()));
    }

    /// Answer `route`: now, when the answer is one frame, may pass what is
    /// queued and the ring has room for it; owed at the back of the queue
    /// otherwise. A reply may pass what is queued only when the entry directly
    /// ahead of it answers on another lease, and one that does is published as
    /// passing. Never blocks.
    pub(super) fn reply(&mut self, route: ReplyRoute, answer: Result<Rows, WireFault>) {
        let id = route.request_id;
        if self.owed.back().is_none_or(|ahead| ahead.id() != id)
            && let Some(msg) = whole(&route, &answer, self.w2m.frame_budget())
        {
            let passing = !self.owed.is_empty();
            if self.w2m.try_send_msg(id, passing, &msg).is_ok() {
                return;
            }
            self.refused(id);
        }
        self.owed.push_back(Owed::Train(Train::new(route, answer)));
    }

    /// Answer every parked read where it stands in the queue, by `answer`: the
    /// queue is owed again in its order, each parked read as its reply.
    pub(super) fn answer_parked(&mut self, mut answer: impl FnMut(&Read<'_>) -> Result<Rows, WireFault>) {
        if !self.owed.iter().any(|owed| matches!(owed, Owed::Parked(..))) {
            return;
        }
        for owed in std::mem::take(&mut self.owed) {
            match owed {
                Owed::Parked(route, kept) => self.reply(route, answer(&kept.read())),
                train => self.owed.push_back(train),
            }
        }
    }

    /// Send the next frame of the first [sendable](Self::sendable) reply, if
    /// the ring has room for it now. A reply that ends is removed. Never blocks.
    #[inline]
    pub(super) fn emit_frame(&mut self) {
        if !self.owed.is_empty() {
            self.emit_owed_frame();
        }
    }

    #[inline(never)]
    fn emit_owed_frame(&mut self) {
        let Some(i) = self.sendable().next() else { return };
        let Owed::Train(train) = &mut self.owed[i] else {
            unreachable!("a parked read is not sendable")
        };
        let id = train.route.request_id;
        if train.refused_at == Some(self.w2m.release_cursor()) {
            return self.refused(id);
        }
        match train.emit_next(&mut self.w2m) {
            Ok(last) => {
                if last {
                    self.owed.remove(i);
                }
            }
            Err(at) => {
                train.refused_at = Some(at);
                self.refused(id);
            }
        }
    }

    /// Wait on this worker's park while `idle` and no owed reply can leave
    /// before the master releases room or ends a hold: either wakes the park
    /// too. Returns at once when a reply can leave now.
    pub(super) fn wait_for_master(&self, idle: impl FnOnce() -> bool) {
        if self.waits_on_master() {
            self.w2m.park_for_master(|| idle() && self.waits_on_master());
        }
    }

    /// An owed reply can leave now, without the master releasing room or
    /// ending a hold.
    #[inline]
    pub(super) fn can_leave(&self) -> bool {
        !self.owed.is_empty() && !self.waits_on_master()
    }

    /// No owed reply can leave before the master releases room or ends a hold:
    /// none is sendable, or the ring refused the first sendable one's next frame
    /// at the release cursor as it stands.
    fn waits_on_master(&self) -> bool {
        let release = self.w2m.release_cursor();
        self.sendable()
            .next()
            .is_none_or(|i| matches!(&self.owed[i], Owed::Train(t) if t.refused_at == Some(release)))
    }

    /// The entries a frame may go for now, in queue order: the first of each
    /// lease's run, unless it is a parked read or a train the master holds,
    /// which hold up the rest of their run.
    fn sendable(&self) -> impl Iterator<Item = usize> {
        (0..self.owed.len()).filter(|&i| {
            (i == 0 || self.owed[i - 1].id() != self.owed[i].id())
                && matches!(&self.owed[i], Owed::Train(t) if !self.w2m.held(t.route.request_id))
        })
    }

    /// A publish answering on `id` found no room. A lease's own reader frees
    /// the room its bytes hold, at its own pace; bytes of another lease wait
    /// on another reader, perhaps one that has stopped. So ask the master for
    /// the ring back when the oldest unreleased publish answers on another
    /// lease than a reply that waits: `id`, or that of another sendable entry.
    fn refused(&mut self, id: RequestId) {
        let blocker = self.w2m.oldest_unreleased();
        if blocker != Some(id) || self.sendable().any(|i| Some(self.owed[i].id()) != blocker) {
            self.w2m.name_shortage();
        }
    }
}

#[cfg(test)]
#[path = "tests/reply.rs"]
pub(super) mod tests;
