//! The worker's reply queue, over the ring its frames leave on: what the worker
//! owes the master, the one rule that decides when a reply leaves, the sender
//! every train's frames go through, and when a refusal of room asks the master
//! for the ring back.

use std::collections::VecDeque;

use crate::runtime::frame::WireMsg;
use crate::runtime::sal::{Cut, Read, ReplyRoute};
use crate::runtime::w2m::{RequestId, W2mWriter};
use gnitz_store::read::{KeySpans, ReadRows};
use gnitz_wire::WireFault;
use gnitz_zset::repr::Batch;

pub(super) struct Replies {
    w2m: W2mWriter,
    /// Replies this worker owes, in the order their requests were read off the
    /// SAL: a cut's are consecutive.
    owed: VecDeque<Owed>,
    /// The runs of one cut's publishes the master has not released, oldest
    /// first: where each ends, and the cut whose bytes hold the ring there.
    unreleased: VecDeque<(u64, Cut)>,
}

/// One reply this worker owes.
// One per queued reply, which is not the path a one-frame reply takes.
#[allow(clippy::large_enum_variant)]
enum Owed {
    /// A read of a view, received while a drive was writing views. It owns its
    /// bytes: it is answered after the drive, which reads further groups.
    Parked(ReplyRoute, Read<'static>),
    Train(Train),
}

impl Owed {
    /// The cut of the request it answers.
    fn cut(&self) -> Cut {
        match self {
            Owed::Parked(route, _) | Owed::Train(Train { route, .. }) => route.cut,
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

    /// Send the next frame, if the ring has room for it now: `Some(last)`,
    /// `last` once it was the last; `None` refused. A row too wide for any
    /// frame, or for the ring, turns the train into that fault.
    fn emit_next(&mut self, w2m: &mut W2mWriter) -> Option<bool> {
        let id = self.route.request_id;
        loop {
            let (batch, ends) = match &mut self.answer {
                Err(fault) => return w2m.try_send_msg(id, false, &WireMsg::fault(fault)).then_some(true),
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
                        if !w2m.try_send_msg(id, false, &msg) {
                            return None;
                        }
                        self.next_row = next_row;
                        return Some(msg.flags.scan_last);
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
        Err(fault) => Some(WireMsg::fault(fault)),
        Ok(Rows::Spans(_)) => None,
        Ok(Rows::Batch(batch)) => {
            let msg = WireMsg::train_frame(route.target_id, true, batch.wire_whole());
            (msg.data.is_none() || msg.size() <= budget).then_some(msg)
        }
    }
}

impl Replies {
    pub(super) fn new(w2m: W2mWriter) -> Self {
        Replies {
            w2m,
            owed: VecDeque::new(),
            unreleased: VecDeque::new(),
        }
    }

    /// Nothing is owed.
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.owed.is_empty()
    }

    /// ACK `id`: on the ACK ring, which no owed reply holds up.
    #[inline]
    pub(super) fn ack(&mut self, id: RequestId) {
        self.w2m.ack(id);
    }

    /// Owe `route` the answer of `read`, once [`Self::answer_parked`] asks for it.
    pub(super) fn park(&mut self, route: ReplyRoute, read: Read<'static>) {
        self.owed.push_back(Owed::Parked(route, read));
    }

    /// Answer `route`: now, when the answer is one frame, may pass what is
    /// queued and the ring has room for it; owed at the back of the queue
    /// otherwise. A reply may pass what is queued only when the entry directly
    /// ahead of it is of another cut, and one that does is published as
    /// passing. Never blocks.
    pub(super) fn reply(&mut self, route: ReplyRoute, answer: Result<Rows, WireFault>) {
        if self.owed.back().is_none_or(|ahead| ahead.cut() != route.cut) {
            if let Some(msg) = whole(&route, &answer, self.w2m.frame_budget()) {
                let passing = !self.owed.is_empty();
                if self.w2m.try_send_msg(route.request_id, passing, &msg) {
                    return self.published(route.cut);
                }
                self.refused(route.cut);
            }
        }
        self.owed.push_back(Owed::Train(Train::new(route, answer)));
    }

    /// Answer every parked read where it stands in the queue, by `answer`: the
    /// queue is owed again in its order, each parked read as its reply.
    pub(super) fn answer_parked(&mut self, mut answer: impl FnMut(&Read<'static>) -> Result<Rows, WireFault>) {
        if !self.owed.iter().any(|owed| matches!(owed, Owed::Parked(..))) {
            return;
        }
        for owed in std::mem::take(&mut self.owed) {
            match owed {
                Owed::Parked(route, read) => self.reply(route, answer(&read)),
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
        let cut = train.route.cut;
        let release = self.w2m.release_cursor();
        if train.refused_at != Some(release) {
            let Some(last) = train.emit_next(&mut self.w2m) else {
                train.refused_at = Some(release);
                return self.refused(cut);
            };
            self.published(cut);
            if last {
                self.owed.remove(i);
            }
        } else {
            self.refused(cut);
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

    /// No owed reply can leave before the master releases room or ends a hold.
    fn waits_on_master(&self) -> bool {
        self.w2m.short_of_room() || self.sendable().next().is_none()
    }

    /// The entries a frame may go for now, in queue order: the first of each
    /// cut's run, unless it is a parked read or a train the master holds,
    /// which hold up the rest of their run.
    fn sendable(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.owed.len()).filter(|&i| {
            (i == 0 || self.owed[i - 1].cut() != self.owed[i].cut())
                && matches!(&self.owed[i], Owed::Train(t) if !self.w2m.held(t.route.request_id))
        })
    }

    /// Record a publish of `cut`'s that just fit: the ring bytes below the
    /// write cursor are `cut`'s down to the last publish's end, which extends
    /// the last run when that run is `cut`'s too.
    fn published(&mut self, cut: Cut) {
        self.forget_released();
        let end = self.w2m.write_cursor();
        match self.unreleased.back_mut() {
            Some((last, c)) if *c == cut => *last = end,
            _ => self.unreleased.push_back((end, cut)),
        }
    }

    /// Forget every run the master has released.
    fn forget_released(&mut self) {
        let release = self.w2m.release_cursor();
        while self.unreleased.front().is_some_and(|&(end, _)| end <= release) {
            self.unreleased.pop_front();
        }
    }

    /// A publish of `cut`'s found no room. Its own reader frees the room its
    /// cut's bytes hold, at its own pace; bytes of another cut wait on another
    /// reader, perhaps one that has stopped. So ask the master for the ring
    /// back when the oldest unreleased publish is another cut's than a reply
    /// that waits: `cut`'s, or that of another sendable entry.
    fn refused(&mut self, cut: Cut) {
        self.forget_released();
        let blocker = self.unreleased.front().map(|&(_, c)| c);
        if blocker != Some(cut) || self.sendable().any(|i| Some(self.owed[i].cut()) != blocker) {
            self.w2m.name_shortage();
        }
    }
}

#[cfg(test)]
#[path = "tests/reply.rs"]
pub(super) mod tests;
