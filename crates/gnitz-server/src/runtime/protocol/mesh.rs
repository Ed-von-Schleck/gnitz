//! The exchange mesh: workers trade a round's partitions directly through one
//! anonymous shared mapping. Every worker runs the same rounds in the same order,
//! so a round is complete once the cluster-wide `arrivals` count reaches
//! `(round + 1) * W`. Each worker has two outboxes, alternating by round, so it
//! rewrites one only after every peer has gathered from it. Nothing here is
//! durable.
//!
//! ```text
//! [0, 64)                        arrivals: AtomicU64 — publishes since boot
//! [HEADER_BYTES + (2w + p) * OUTBOX_BYTES, +OUTBOX_BYTES)   worker w's outbox p
//!   outbox: [Head][dir: (u64 offset, u64 len) × W][pad to BLOCKS_AT][blocks, 8-aligned]
//! ```
//!
//! A block is one receiver's rows as a WAL block of the round's schema, which
//! every worker already holds from its own partition; `len == 0` sends none. The
//! [`Head`] names the round, and is the divergence check.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::sal::MAX_WORKERS;
use crate::runtime::w2m::SalWake;
use crate::runtime::wire::WireData;
use gnitz_foundation::posix_io;
use gnitz_store::ops::{op_exchange_gather, op_exchange_route, ScatterSpec};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::{decode_mem_batch_from_wal_block, Batch, Layout, MAX_BATCH_REGIONS};

/// Virtual bytes of one outbox; each round rewrites it from its start.
const OUTBOX_BYTES: usize = 1 << 30;

const PAGE_BYTES: usize = 4096;

/// The page holding `arrivals`; the outboxes start past it.
const HEADER_BYTES: usize = PAGE_BYTES;

/// Where an outbox's blocks start: past its [`Head`] and its directory, one
/// `(offset, len)` pair per receiver.
const BLOCKS_AT: usize = PAGE_BYTES;
const DIR_AT: usize = size_of::<Head>();
const _: () = assert!(
    DIR_AT + MAX_WORKERS * 16 <= BLOCKS_AT,
    "an outbox's head and directory fit its first page"
);

/// Resident bytes an outbox keeps past its latest round; a round that ends
/// further below the outbox's high-water mark returns the pages beyond it.
const RESIDENT_SLACK_BYTES: usize = 64 << 20;

fn region_bytes(nw: usize) -> usize {
    HEADER_BYTES + 2 * nw * OUTBOX_BYTES
}

/// The mesh region for `nw` workers, zeroed, shared with every forked child and
/// never unmapped.
pub(crate) fn create_region(nw: usize) -> std::io::Result<*mut u8> {
    posix_io::map_anon_shared(region_bytes(nw))
}

/// An outbox's first bytes: the round its blocks belong to.
#[derive(Clone, Copy)]
#[repr(C)]
struct Head {
    view: u64,
    key: u64,
    round: u64,
    /// The publisher's source partition is drained — a backfill stops once every
    /// worker's is.
    drained: u64,
    /// The published partition was consolidated, so every block is.
    consolidated: u64,
}

pub(crate) struct Mesh {
    base: *mut u8,
    rank: usize,
    /// Rounds this worker has gathered — identical on every worker.
    round: u64,
    /// The round this worker published and has not gathered yet.
    open: Option<OpenRound>,
    /// One per worker, on its W2M header's `sal_park` word.
    wakes: Vec<SalWake>,
    /// Per-worker row lists of the round being published, reused.
    routed: Vec<Vec<u32>>,
    /// How far into each of this worker's outboxes pages may be resident.
    resident: [usize; 2],
}

/// What [`Mesh::gather`] checks the peers' heads against and decodes by.
struct OpenRound {
    view: u64,
    key: u64,
    schema: SchemaDescriptor,
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
            round: 0,
            open: None,
            wakes,
            routed: Vec::new(),
            resident: [0; 2],
        }
    }

    fn nw(&self) -> usize {
        self.wakes.len()
    }

    fn arrivals(&self) -> &AtomicU64 {
        // SAFETY: the region's first 8 bytes, 8-aligned, live for the process.
        unsafe { AtomicU64::from_ptr(self.base.cast()) }
    }

    /// Worker `w`'s outbox for the current round.
    fn outbox(&self, w: usize) -> *mut u8 {
        assert!(w < self.nw(), "worker {w} of {}", self.nw());
        // SAFETY: `w < nw`, so the outbox lies inside the region.
        unsafe { self.base.add(HEADER_BYTES + (2 * w + self.parity()) * OUTBOX_BYTES) }
    }

    fn parity(&self) -> usize {
        (self.round % 2) as usize
    }

    /// Open round `(view, key)`: write `batch` into this worker's outbox, split
    /// by `spec` or, with none, whole to every worker; count the arrival, and wake
    /// every peer if it completed the round. Fatal while a round is open, on
    /// oversize, or on a refused route.
    pub(crate) fn publish(&mut self, view: u64, key: u64, drained: bool, batch: &Batch, spec: Option<ScatterSpec<'_>>) {
        assert!(
            self.open.is_none(),
            "exchange of view {view}, key {key}: a round is already open"
        );
        let nw = self.nw();
        let mut out = Outbox {
            base: self.outbox(self.rank),
            end: BLOCKS_AT,
            view,
        };
        out.head(Head {
            view,
            key,
            round: self.round,
            drained: drained as u64,
            consolidated: (batch.layout() == Layout::Consolidated) as u64,
        });
        match spec {
            None => {
                let at = out.put(WireData::Whole(batch));
                (0..nw).for_each(|r| out.direct(r, at));
            }
            Some(spec) => {
                let routed = op_exchange_route(batch, spec, &mut self.routed, nw)
                    .unwrap_or_else(|e| gnitz_fatal_abort!("exchange of view {view}, key {key}: {e}"));
                // A heap-referencing row cannot be framed in place; its subset
                // carries its own heap.
                let heapless = batch.heap_referencing_slots() == 0;
                for (r, rows) in routed.iter().enumerate() {
                    let at = match heapless {
                        true => out.put(WireData::Scattered { batch, indices: rows }),
                        false => out.put(WireData::Whole(&batch.ascending_subset(rows))),
                    };
                    out.direct(r, at);
                }
            }
        }
        self.trim(out.end);
        self.open = Some(OpenRound { view, key, schema: *batch.schema() });
        // Release: a peer that sees the count sees the outbox writes above.
        let last = (self.round + 1) * nw as u64 - 1;
        if self.arrivals().fetch_add(1, Ordering::AcqRel) == last {
            for (w, wake) in self.wakes.iter().enumerate() {
                if w != self.rank {
                    wake.wake();
                }
            }
        }
    }

    /// Return the pages of this round's outbox past `end`, once they exceed
    /// [`RESIDENT_SLACK_BYTES`]. Every peer gathered what they held two rounds ago.
    fn trim(&mut self, end: usize) {
        let p = self.parity();
        let end = end.next_multiple_of(PAGE_BYTES);
        if self.resident[p] > end + RESIDENT_SLACK_BYTES {
            // SAFETY: `[end, resident)` lies inside this worker's own outbox.
            posix_io::madvise_remove(unsafe { self.outbox(self.rank).add(end) }, self.resident[p] - end);
            self.resident[p] = end;
        }
        self.resident[p] = self.resident[p].max(end);
    }

    /// Every worker has published the current round.
    pub(crate) fn complete(&self) -> bool {
        self.arrivals().load(Ordering::Acquire) >= (self.round + 1) * self.nw() as u64
    }

    /// Close the open round: the rows every peer sent this worker, and whether
    /// every peer's partition was drained. Fatal unless the round is complete, or
    /// when a peer's head names another (view, key, round).
    pub(crate) fn gather(&mut self) -> (Batch, bool) {
        let OpenRound { view, key, schema } = self.open.take().expect("gather without an open round");
        assert!(self.complete(), "gathered before every worker published");
        let round = self.round;
        let (mut drained, mut consolidated) = (true, true);
        let mut offsets = vec![[0usize; MAX_BATCH_REGIONS]; self.nw()];
        let mut slices = Vec::with_capacity(self.nw());
        for (peer, offsets) in offsets.iter_mut().enumerate() {
            // SAFETY: the peer wrote its head before the arrival `complete`
            // observed, and rewrites it only after this worker's next publish.
            let head = unsafe { self.outbox(peer).cast::<Head>().read() };
            if (head.view, head.key, head.round) != (view, key, round) {
                gnitz_fatal_abort!(
                    "exchange of (view {view}, key {key}, round {round}): worker {peer} published \
                     (view {}, key {}, round {}) — workers diverged",
                    head.view,
                    head.key,
                    head.round
                );
            }
            drained &= head.drained != 0;
            let block = self.block_from(peer);
            if block.is_empty() {
                continue;
            }
            consolidated &= head.consolidated != 0;
            slices.push(
                decode_mem_batch_from_wal_block(block, &schema, offsets).unwrap_or_else(|e| {
                    gnitz_fatal_abort!("exchange of view {view}, key {key}: worker {peer}'s block: {e}")
                }),
            );
        }
        let rows = op_exchange_gather(&slices, &schema, consolidated);
        self.round += 1;
        (rows, drained)
    }

    /// The block `peer` addressed to this worker in the current round.
    fn block_from(&self, peer: usize) -> &[u8] {
        let outbox = self.outbox(peer);
        // SAFETY: the directory lies inside the peer's outbox, and the peer wrote
        // it before the arrival `complete` observed.
        let (at, len) = unsafe {
            let entry = outbox.add(DIR_AT + 16 * self.rank).cast::<u64>();
            (entry.read() as usize, entry.add(1).read() as usize)
        };
        if at < BLOCKS_AT || len > OUTBOX_BYTES - at {
            gnitz_fatal_abort!("exchange: worker {peer}'s directory names bytes [{at}, +{len}) outside its outbox");
        }
        // SAFETY: checked above to lie inside the peer's outbox, which it
        // rewrites only after this worker's next publish.
        unsafe { std::slice::from_raw_parts(outbox.add(at), len) }
    }
}

/// One outbox being written: its blocks so far end at `end`.
struct Outbox {
    base: *mut u8,
    end: usize,
    view: u64,
}

impl Outbox {
    fn head(&mut self, head: Head) {
        // SAFETY: the outbox is page-aligned and its first page holds the head.
        unsafe { self.base.cast::<Head>().write(head) }
    }

    /// Append `data` as one block, returning its `(offset, len)`; no rows append
    /// none.
    fn put(&mut self, data: WireData) -> (u64, u64) {
        if data.row_count() == 0 {
            return (BLOCKS_AT as u64, 0);
        }
        let (at, size) = (self.end, data.wire_byte_size());
        if size > OUTBOX_BYTES - at {
            gnitz_fatal_abort!(
                "exchange of view {}: a {size}-byte partition does not fit the {OUTBOX_BYTES}-byte outbox",
                self.view
            );
        }
        // SAFETY: `[at, at + size)` lies inside this worker's own outbox, which no
        // peer reads until the arrival that follows.
        let written = data.encode(unsafe { std::slice::from_raw_parts_mut(self.base.add(at), size) });
        debug_assert_eq!(written, size, "the block's encoded size");
        self.end = (at + size).next_multiple_of(8);
        (at as u64, size as u64)
    }

    /// Point receiver `r`'s directory entry at the block `(at, len)`.
    fn direct(&mut self, r: usize, (at, len): (u64, u64)) {
        // SAFETY: `r < MAX_WORKERS`, so the entry lies inside the first page.
        unsafe {
            let entry = self.base.add(DIR_AT + 16 * r).cast::<u64>();
            entry.write(at);
            entry.add(1).write(len);
        }
    }
}

#[cfg(test)]
#[path = "tests/mesh_fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/mesh.rs"]
mod tests;
