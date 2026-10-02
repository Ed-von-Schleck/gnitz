//! The exchange mesh: workers trade a round's partitions directly through one
//! anonymous shared mapping. A round is one or more parts, and each part is one
//! arrival per worker. Every worker runs the same parts in the same order, so a
//! part is complete once the cluster-wide `arrivals` count reaches
//! `(part + 1) * W`. Each worker has two outboxes, alternating by part, so it
//! rewrites one only after every peer has read it. A partition too large for
//! one outbox continues in the round's next part. Nothing here is durable.
//!
//! ```text
//! [0, 8)                         arrivals: AtomicU64 — parts published since boot
//! [8, 16)                        outbox_bytes: u64 — fixed when the region is mapped
//! [HEADER_BYTES + (2w + p) * outbox_bytes, +outbox_bytes)   worker w's outbox p
//!   outbox: [Head, padded to BLOCKS_AT][blocks, 8-aligned]
//! ```
//!
//! A block is one receiver's rows of a part as a WAL block of the round's
//! schema, which every worker already holds from its own partition; an empty
//! [`Span`] sends none. The [`Head`] names the view, and is the divergence check.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::sal::MAX_WORKERS;
use crate::runtime::w2m::SalWake;
use gnitz_foundation::posix_io;
use gnitz_zset::algebra::{op_exchange_gather, ScatterPlan};
use gnitz_zset::repr::{Batch, MemBatch};
use gnitz_zset::schema::SchemaDescriptor;

/// The default and largest virtual size of one outbox.
pub(crate) const OUTBOX_BYTES: usize = 1 << 30;
const _: () = assert!(
    OUTBOX_BYTES <= u32::MAX as usize,
    "a WAL block stores its size as a u32"
);

const PAGE_BYTES: usize = 4096;

/// The page holding `arrivals` and the outbox size; the outboxes start past it.
const HEADER_BYTES: usize = PAGE_BYTES;

/// Where an outbox's blocks start: past its [`Head`].
const BLOCKS_AT: usize = PAGE_BYTES;
const _: () = assert!(size_of::<Head>() <= BLOCKS_AT, "an outbox's head fits its first page");

/// Resident bytes an outbox keeps past its latest part; a part that ends
/// further below the outbox's high-water mark returns the pages beyond it.
const RESIDENT_SLACK_BYTES: usize = 64 << 20;

/// The outbox size: `GNITZ_MESH_OUTBOX_BYTES`, clamped, or [`OUTBOX_BYTES`].
pub(crate) fn outbox_bytes() -> usize {
    let asked = gnitz_foundation::env::env_num("GNITZ_MESH_OUTBOX_BYTES", OUTBOX_BYTES);
    let size = asked.clamp(2 * PAGE_BYTES, OUTBOX_BYTES).next_multiple_of(PAGE_BYTES);
    if size != asked {
        gnitz_info!(
            "GNITZ_MESH_OUTBOX_BYTES={asked} is outside [{}, {OUTBOX_BYTES}] or not page-sized; using {size}",
            2 * PAGE_BYTES
        );
    }
    size
}

/// The mesh region for `nw` workers' outboxes of `outbox_bytes` each, zeroed but
/// for the size it records, shared with every forked child and never unmapped.
pub(crate) fn create_region(nw: usize, outbox_bytes: usize) -> std::io::Result<*mut u8> {
    let base = posix_io::map_anon_shared(HEADER_BYTES + 2 * nw * outbox_bytes)?;
    // SAFETY: the mapping is page-aligned and its first page is the header.
    unsafe { base.add(8).cast::<u64>().write(outbox_bytes as u64) };
    Ok(base)
}

/// Bytes `[at, at + len)` of an outbox; `len == 0` names no block.
#[derive(Clone, Copy)]
#[repr(C)]
struct Span {
    at: u64,
    len: u64,
}

impl Span {
    const EMPTY: Span = Span { at: 0, len: 0 };
}

/// An outbox's first bytes: what the publisher's part is, and each receiver's
/// block of it.
#[repr(C)]
struct Head {
    view: u64,
    /// The publisher's source partition is drained — a backfill stops once every
    /// worker's is.
    drained: u64,
    /// The published partition was consolidated and the round lands where it
    /// folds, so every block is merged on gather.
    consolidated: u64,
    /// The publisher has rows left for another part of this round.
    more: u64,
    blocks: [Span; MAX_WORKERS],
}

pub(crate) struct Mesh {
    base: *mut u8,
    rank: usize,
    outbox_bytes: usize,
    /// Parts gathered since boot — identical on every worker; its parity picks
    /// the outbox.
    part: u64,
    open: Option<Open>,
    /// One per worker, on its W2M header's `sal_park` word.
    wakes: Vec<SalWake>,
    /// Per-receiver row lists of the round being sent, reused across rounds.
    routed: Vec<Vec<u32>>,
    /// How far into each of this worker's outboxes pages may be resident.
    resident: [usize; 2],
    /// This worker's claim that its source is drained, published with every round
    /// until one closes; from then, whether every worker's was.
    drained: bool,
}

/// A round between its [`Mesh::publish`] and the [`Mesh::advance`] that
/// finishes it.
struct Open {
    view: u64,
    schema: SchemaDescriptor,
    /// The published batch was consolidated and the round lands where it folds.
    consolidated: bool,
    /// How far this worker's writing has come; `None` once every row it sends
    /// is written.
    unsent: Option<Cursor>,
    /// This receiver's non-empty blocks from the round's earlier parts, copied
    /// out raw.
    saved: Vec<Vec<u8>>,
    /// Every sender of a saved block was consolidated.
    saved_consolidated: bool,
}

/// How far a round's writing has come through its row lists: at
/// `routed[list][offset..]`.
#[derive(Clone, Copy)]
struct Cursor {
    /// How many lists the round's plan routed into: one per receiver, or the
    /// one list every receiver is sent.
    lists: usize,
    list: usize,
    offset: usize,
}

impl Mesh {
    /// Worker `rank`'s view of the mesh among `wakes.len()` workers.
    ///
    /// # Safety
    /// `base` is a live region [`create_region`] mapped for `wakes.len()` workers,
    /// shared by every one of them.
    pub(crate) unsafe fn new(base: *mut u8, rank: usize, wakes: Vec<SalWake>) -> Self {
        let nw = wakes.len();
        assert!(rank < nw && nw <= MAX_WORKERS, "rank {rank} of {nw} workers");
        Mesh {
            base,
            rank,
            // SAFETY: `create_region` wrote it before any worker was forked.
            outbox_bytes: unsafe { base.add(8).cast::<u64>().read() } as usize,
            part: 0,
            open: None,
            wakes,
            routed: Vec::new(),
            resident: [0; 2],
            drained: false,
        }
    }

    fn nw(&self) -> usize {
        self.wakes.len()
    }

    fn arrivals(&self) -> &AtomicU64 {
        // SAFETY: the region's first 8 bytes, 8-aligned, live for the process.
        unsafe { AtomicU64::from_ptr(self.base.cast()) }
    }

    /// Worker `w`'s outbox for the current part.
    fn outbox(&self, w: usize) -> *mut u8 {
        assert!(w < self.nw(), "worker {w} of {}", self.nw());
        // SAFETY: `w < nw`, so the outbox lies inside the region.
        unsafe {
            self.base
                .add(HEADER_BYTES + (2 * w + self.parity()) * self.outbox_bytes)
        }
    }

    fn parity(&self) -> usize {
        (self.part % 2) as usize
    }

    /// Claim that this worker's source is, or is not, drained.
    pub(crate) fn set_drained(&mut self, own: bool) {
        self.drained = own;
    }

    /// The claim [`Self::set_drained`] made, until a round closes; from then,
    /// whether every worker's source was drained.
    pub(crate) fn drained(&self) -> bool {
        self.drained
    }

    /// Open a round of `view`: send the live rows of `batch` split by `plan` as
    /// the round's first part; `fold` is [`crate::query::DriveHost::exchange`]'s.
    /// Fatal while a round is open, or on a row larger than an outbox.
    pub(crate) fn publish(&mut self, view: u64, batch: &Batch, plan: &ScatterPlan, fold: bool) {
        assert!(self.open.is_none(), "exchange of view {view}: a round is already open");
        let nw = self.nw();
        let lists = plan.route(batch, &mut self.routed, nw).len();
        self.open = Some(Open {
            view,
            schema: *batch.schema(),
            consolidated: fold && batch.is_consolidated(),
            unsent: Some(Cursor { lists, list: 0, offset: 0 }),
            saved: Vec::new(),
            saved_consolidated: true,
        });
        self.send_part(Some(batch));
    }

    /// This worker has rows of the open round still to write, from the batch it
    /// published.
    pub(crate) fn sending(&self) -> bool {
        self.open.as_ref().is_some_and(|open| open.unsent.is_some())
    }

    /// Send the open round's next part: what is left of `batch`'s rows, or
    /// nothing once they are all written.
    fn send_part(&mut self, batch: Option<&Batch>) {
        let open = self.open.as_ref().expect("a part of an open round");
        let mut head = Head {
            view: open.view,
            drained: self.drained as u64,
            consolidated: open.consolidated as u64,
            more: 0,
            blocks: [Span::EMPTY; MAX_WORKERS],
        };
        let mut unsent = open.unsent;
        let mut end = BLOCKS_AT;
        if let Some(at) = &mut unsent {
            let batch = batch.expect("the batch of a round still being sent");
            let more;
            (end, more) = self.write_rows(batch, at, &mut head.blocks);
            if more && end == BLOCKS_AT {
                gnitz_fatal_abort!(
                    "exchange of view {}: a row does not fit the {}-byte outbox",
                    head.view,
                    self.outbox_bytes
                );
            }
            head.more = more as u64;
        }
        self.open.as_mut().expect("still open").unsent = unsent.filter(|_| head.more != 0);
        self.arrive(&head, end);
    }

    /// Write `batch`'s rows from `at` as one block per row list, naming each in
    /// `blocks`, until the lists end or the outbox fills. Returns where the
    /// blocks end, and whether rows are left.
    fn write_rows(&self, batch: &Batch, at: &mut Cursor, blocks: &mut [Span; MAX_WORKERS]) -> (usize, bool) {
        let nw = self.nw();
        let outbox = self.outbox(self.rank);
        let mut end = BLOCKS_AT;
        while at.list < at.lists {
            let rest = &self.routed[at.list][at.offset..];
            if !rest.is_empty() {
                // SAFETY: `[end, outbox_bytes)` lies inside this worker's own
                // outbox, which no peer reads until the arrival that follows.
                let out = unsafe { std::slice::from_raw_parts_mut(outbox.add(end), self.outbox_bytes - end) };
                let Some((n, len)) = batch.encode_scattered_prefix(rest, out) else {
                    return (end, true);
                };
                let span = Span { at: end as u64, len: len as u64 };
                match at.lists {
                    1 => blocks[..nw].fill(span),
                    _ => blocks[at.list] = span,
                }
                end = (end + len).next_multiple_of(8);
                if n < rest.len() {
                    at.offset += n;
                    return (end, true);
                }
            }
            at.list += 1;
            at.offset = 0;
        }
        (end, false)
    }

    /// Publish `head` and the blocks before `end` as this worker's part: count
    /// the arrival, and wake every peer if it completed the part.
    fn arrive(&mut self, head: &Head, end: usize) {
        // SAFETY: the outbox is page-aligned and its first page holds the head;
        // no peer reads it until the arrival below.
        unsafe { std::ptr::copy_nonoverlapping(head, self.outbox(self.rank).cast::<Head>(), 1) }
        self.trim(end);
        let nw = self.nw() as u64;
        // Release: a peer that sees the count sees the outbox writes above.
        if self.arrivals().fetch_add(1, Ordering::AcqRel) == (self.part + 1) * nw - 1 {
            for (w, wake) in self.wakes.iter().enumerate() {
                if w != self.rank {
                    wake.wake();
                }
            }
        }
    }

    /// Return the pages of this part's outbox past `end`, once they exceed
    /// [`RESIDENT_SLACK_BYTES`]. Every peer read what they held two parts ago.
    fn trim(&mut self, end: usize) {
        let p = self.parity();
        let end = end.next_multiple_of(PAGE_BYTES);
        if self.resident[p] > end + RESIDENT_SLACK_BYTES {
            // SAFETY: `[end, resident)` lies inside this worker's own outbox, and
            // every peer is done with what it held.
            unsafe { posix_io::madvise_remove(self.outbox(self.rank).add(end), self.resident[p] - end) };
            self.resident[p] = end;
        }
        self.resident[p] = self.resident[p].max(end);
    }

    /// Every worker has published the current part.
    pub(crate) fn complete(&self) -> bool {
        self.arrivals().load(Ordering::Acquire) >= (self.part + 1) * self.nw() as u64
    }

    /// Take in the completed part and send the next from `batch`, the one
    /// published, which is needed while [`Self::sending`]; or on the round's
    /// last part close it: the rows every peer sent this worker.
    pub(crate) fn advance(&mut self, batch: Option<&Batch>) -> Option<Batch> {
        assert!(self.complete(), "advanced before every worker published");
        let mut open = self.open.take().expect("advance without an open round");
        let view = open.view;
        let (mut drained, mut more, mut consolidated) = (true, false, open.saved_consolidated);
        let mut blocks = Vec::with_capacity(self.nw());
        for peer in 0..self.nw() {
            // SAFETY: the peer wrote its head before the arrival `complete`
            // observed, and rewrites it only after this worker's next arrival.
            let head = unsafe { &*self.outbox(peer).cast::<Head>() };
            if head.view != view {
                gnitz_fatal_abort!(
                    "exchange of view {view}: worker {peer} published view {} — workers diverged",
                    head.view
                );
            }
            drained &= head.drained != 0;
            more |= head.more != 0;
            let block = self.block(peer, head.blocks[self.rank]);
            if !block.is_empty() {
                consolidated &= head.consolidated != 0;
                blocks.push(block);
            }
        }
        if more {
            open.saved.extend(blocks.into_iter().map(<[u8]>::to_vec));
            open.saved_consolidated = consolidated;
            self.open = Some(open);
            self.part += 1;
            self.send_part(batch);
            return None;
        }
        let schema = open.schema;
        let slices: Vec<MemBatch> = open
            .saved
            .iter()
            .map(Vec::as_slice)
            .chain(blocks)
            .map(|block| {
                MemBatch::of_wal_block(block, &schema)
                    .unwrap_or_else(|e| gnitz_fatal_abort!("exchange of view {view}: a peer's block: {e}"))
            })
            .collect();
        let rows = op_exchange_gather(&slices, &schema, consolidated);
        self.part += 1;
        self.drained = drained;
        Some(rows)
    }

    /// The block `span` names in `peer`'s outbox for the current part.
    fn block(&self, peer: usize, span: Span) -> &[u8] {
        if span.len == 0 {
            return &[];
        }
        let inside = span.at >= BLOCKS_AT as u64
            && span
                .at
                .checked_add(span.len)
                .is_some_and(|e| e <= self.outbox_bytes as u64);
        if !inside {
            gnitz_fatal_abort!(
                "exchange: worker {peer}'s head names bytes [{}, +{}) outside its outbox",
                span.at,
                span.len
            );
        }
        // SAFETY: checked above to lie inside the peer's outbox, which it
        // rewrites only after this worker's next arrival.
        unsafe { std::slice::from_raw_parts(self.outbox(peer).add(span.at as usize), span.len as usize) }
    }
}

#[cfg(test)]
#[path = "tests/mesh_fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/mesh.rs"]
mod tests;
