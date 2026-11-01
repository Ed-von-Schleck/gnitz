//! The worker's reply queue: what it owes the master, the one rule that decides
//! when a reply leaves, the frame cutter every train is sent through, and when
//! a refusal of room asks the master for the ring back.

use super::*;

use crate::runtime::frame::WireMsg;
use crate::runtime::sal::{Cut, OwnedRead};
use gnitz_store::read::{KeySpans, ReadRows};

/// One reply this worker owes.
// One per queued reply, which is not the path a one-frame reply takes.
#[allow(clippy::large_enum_variant)]
pub(super) enum Owed {
    /// A read of a view, received while a drive was writing views. It owns its
    /// bytes: it is answered after the drive, which may read past a `Flush`.
    Parked(ReplyRoute, OwnedRead),
    Fault(ReplyRoute, WireFault),
    Train(Train),
}

impl Owed {
    /// The cut of the request it answers.
    pub(super) fn cut(&self) -> Cut {
        match self {
            Owed::Parked(route, ..) | Owed::Fault(route, _) | Owed::Train(Train { route, .. }) => route.cut,
        }
    }
}

/// A reply's rows, and how far into them its frames have come.
pub(super) struct Train {
    pub(super) route: ReplyRoute,
    rows: Rows,
    /// Rows of the batch, or of the current span chunk, already sent.
    next_row: usize,
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
    /// Send the next frame, if the ring has room for it now: `Ok(Some(last))`,
    /// `last` once it was the last; `Ok(None)` refused. `Err` is a row too wide
    /// for any frame.
    fn emit_next(&mut self, w2m: &mut W2mWriter, budget: usize) -> Result<Option<bool>, WireFault> {
        let (batch, whole) = match &mut self.rows {
            Rows::Batch(batch) => (&**batch, true),
            Rows::Spans(spans) => {
                if self.next_row == spans.chunk().len() {
                    spans.advance();
                    self.next_row = 0;
                }
                (spans.chunk(), false)
            }
        };
        let head = WireMsg::train_frame(self.route.target_id, false, None).size();
        let rows = batch.wire_rows_within(self.next_row, budget.saturating_sub(head));
        let next_row = self.next_row + rows.map_or(0, |r| r.rows());
        let last = next_row == batch.len() && (whole || batch.is_empty());
        let msg = WireMsg::train_frame(self.route.target_id, last, rows);
        msg.check_size()?;
        if !w2m.try_send_msg(self.route.request_id, false, &msg) {
            return Ok(None);
        }
        self.next_row = next_row;
        Ok(Some(last))
    }
}

impl WorkerProcess<'_> {
    /// Send `answer`, at queue position `at`, now when it is one frame, may pass
    /// what is queued and the ring has room for it; otherwise return the entry it
    /// leaves owed. A reply may pass what is queued only when the entry directly
    /// ahead of it is of another cut, and one that does is published as passing.
    /// Never blocks.
    fn send_or_owe(&mut self, at: usize, route: ReplyRoute, answer: Result<Rows, WireFault>) -> Option<Owed> {
        let passing = at > 0;
        let may_pass = at.checked_sub(1).is_none_or(|p| self.replies[p].cut() != route.cut);
        let sent = match &answer {
            _ if !may_pass => None,
            Err(fault) => Some(
                self.w2m_writer
                    .try_send_msg(route.request_id, passing, &WireMsg::fault(fault)),
            ),
            Ok(Rows::Spans(_)) => None,
            Ok(Rows::Batch(batch)) => self.try_send_whole(route, passing, batch),
        };
        match sent {
            Some(true) => {
                self.published(route.cut);
                return None;
            }
            Some(false) => self.refused(route.cut),
            None => {}
        }
        Some(match answer {
            Err(fault) => Owed::Fault(route, fault),
            Ok(rows) => Owed::Train(Train { route, rows, next_row: 0 }),
        })
    }

    /// `batch` as the one frame of its reply, when it fits the budget: whether
    /// the ring had room for it. `None` past the budget.
    fn try_send_whole(&mut self, route: ReplyRoute, passing: bool, batch: &Batch) -> Option<bool> {
        let msg = WireMsg::train_frame(route.target_id, true, batch.wire_whole());
        (msg.data.is_none() || msg.size() <= self.reply_frame_budget)
            .then(|| self.w2m_writer.try_send_msg(route.request_id, passing, &msg))
    }

    /// Record a publish of `cut`'s that just fit: the ring bytes below the
    /// write cursor are `cut`'s down to the last publish's end, which extends
    /// the last run when that run is `cut`'s too.
    fn published(&mut self, cut: Cut) {
        self.forget_released();
        let end = self.w2m_writer.write_cursor();
        match self.unreleased.back_mut() {
            Some((last, c)) if *c == cut => *last = end,
            _ => self.unreleased.push_back((end, cut)),
        }
    }

    /// Forget every run the master has released.
    fn forget_released(&mut self) {
        let release = self.w2m_writer.release_cursor();
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
        if blocker != Some(cut) || self.sendable().any(|i| Some(self.replies[i].cut()) != blocker) {
            self.w2m_writer.name_shortage();
        }
    }

    /// The entries a frame may go for now, in queue order: the first of each
    /// cut's run, unless it is a parked read or a train the master holds,
    /// which hold up the rest of their run.
    pub(super) fn sendable(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.replies.len()).filter(|&i| {
            (i == 0 || self.replies[i - 1].cut() != self.replies[i].cut())
                && match &self.replies[i] {
                    Owed::Parked(..) => false,
                    Owed::Fault(..) => true,
                    Owed::Train(t) => !self.w2m_writer.held(t.route.request_id),
                }
        })
    }

    /// Answer `route`. Its cut's groups are consecutive in the SAL, so whatever the
    /// cut still owes sits at the back of the queue.
    pub(super) fn reply(&mut self, route: ReplyRoute, answer: Result<Rows, WireFault>) {
        if let Some(owed) = self.send_or_owe(self.replies.len(), route, answer) {
            self.replies.push_back(owed);
        }
    }

    /// Answer every parked read where it stands in the queue.
    pub(super) fn answer_parked(&mut self) {
        let mut i = 0;
        while i < self.replies.len() {
            let Owed::Parked(route, read) = &self.replies[i] else {
                i += 1;
                continue;
            };
            let route = *route;
            let answer = answer(self.catalog, read);
            match self.send_or_owe(i, route, answer) {
                Some(owed) => {
                    self.replies[i] = owed;
                    i += 1;
                }
                None => {
                    self.replies.remove(i);
                }
            }
        }
    }

    /// Send the next frame of the first [sendable](Self::sendable) reply, if
    /// the ring has room for it now. A reply that ends is removed; a train that
    /// faults is answered by the fault in its place. Never blocks.
    pub(super) fn emit_reply_frame(&mut self) {
        let Some(i) = self.sendable().next() else { return };
        let w2m = &mut self.w2m_writer;
        let (route, sent) = match &mut self.replies[i] {
            Owed::Parked(..) => unreachable!("a parked read is not sendable"),
            Owed::Fault(route, fault) => (
                *route,
                Ok(w2m
                    .try_send_msg(route.request_id, false, &WireMsg::fault(fault))
                    .then_some(true)),
            ),
            Owed::Train(train) => (train.route, train.emit_next(w2m, self.reply_frame_budget)),
        };
        match sent {
            Ok(Some(done)) => {
                self.published(route.cut);
                if done {
                    self.replies.remove(i);
                }
            }
            Ok(None) => self.refused(route.cut),
            Err(fault) => {
                // Answered by the fault, sent now or owed in the train's place.
                self.replies[i] = Owed::Fault(route, fault);
                self.emit_reply_frame();
            }
        }
    }
}
