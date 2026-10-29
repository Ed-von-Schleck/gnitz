//! The worker's reply queue: what it owes the master, the one rule that decides
//! when a reply leaves, and the frame cutter every train is sent through.

use super::*;

use crate::runtime::frame::WireMsg;
use crate::runtime::sal::Cut;
use gnitz_store::read::KeySpans;

/// One reply this worker owes.
// One per queued reply, which is not the path a one-frame reply takes.
#[allow(clippy::large_enum_variant)]
pub(super) enum Owed {
    /// A read of a view, received while a drive was writing views.
    Parked(ReplyRoute, Read<'static>),
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
    Own(Batch),
    /// One batch its store holds too.
    Shared(Rc<Batch>),
    /// Sorted key spans, a chunk at a time; the first empty chunk ends them.
    Spans(Box<KeySpans>),
}

impl Train {
    /// Send the next frame; `Ok(true)` once it was the last. `Err` is a row too
    /// wide for any frame.
    fn emit_next(&mut self, w2m: &mut W2mWriter, budget: usize) -> Result<bool, WireFault> {
        let (batch, whole) = match &mut self.rows {
            Rows::Own(batch) => (&*batch, true),
            Rows::Shared(batch) => (&**batch, true),
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
        self.next_row += rows.map_or(0, |r| r.rows());
        let last = self.next_row == batch.len() && (whole || batch.is_empty());
        let msg = WireMsg::train_frame(self.route.target_id, last, rows);
        msg.check_size()?;
        w2m.send_msg(self.route.request_id, &msg);
        Ok(last)
    }
}

impl WorkerProcess<'_> {
    /// Send `answer`, at queue position `at`, now when it is one frame, may pass
    /// what is queued and the ring has room for it; otherwise return the entry it
    /// leaves owed. A reply may pass what is queued only when the entry directly
    /// ahead of it is of another cut. Never blocks.
    fn send_or_owe(&mut self, at: usize, route: ReplyRoute, answer: Result<Rows, WireFault>) -> Option<Owed> {
        let may_pass = at.checked_sub(1).is_none_or(|p| self.replies[p].cut() != route.cut);
        let sent = may_pass
            && match &answer {
                Err(fault) => self.w2m_writer.try_send_msg(route.request_id, &WireMsg::fault(fault)),
                Ok(Rows::Spans(_)) => false,
                Ok(Rows::Own(batch)) => self.try_send_whole(route, batch),
                Ok(Rows::Shared(batch)) => self.try_send_whole(route, batch),
            };
        if sent {
            return None;
        }
        Some(match answer {
            Err(fault) => Owed::Fault(route, fault),
            Ok(rows) => Owed::Train(Train { route, rows, next_row: 0 }),
        })
    }

    /// `batch` as the one frame of its reply, when it fits the budget and the ring.
    fn try_send_whole(&mut self, route: ReplyRoute, batch: &Batch) -> bool {
        let msg = WireMsg::train_frame(route.target_id, true, batch.wire_whole());
        (msg.data.is_none() || msg.size() <= self.reply_frame_budget)
            && self.w2m_writer.try_send_msg(route.request_id, &msg)
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

    /// Send the front reply's next frame, waiting for ring room. A reply that
    /// ends is popped; a train that faults answers its request with the fault.
    pub(super) fn emit_reply_frame(&mut self) {
        let w2m = &mut self.w2m_writer;
        let done = match self.replies.front_mut() {
            None => return,
            Some(Owed::Parked(..)) => unreachable!("a parked read is answered when its drive ends"),
            Some(Owed::Fault(route, fault)) => {
                w2m.send_msg(route.request_id, &WireMsg::fault(fault));
                true
            }
            Some(Owed::Train(train)) => match train.emit_next(w2m, self.reply_frame_budget) {
                Ok(last) => last,
                Err(fault) => {
                    w2m.send_msg(train.route.request_id, &WireMsg::fault(&fault));
                    true
                }
            },
        };
        if done {
            self.replies.pop_front();
        }
    }
}
