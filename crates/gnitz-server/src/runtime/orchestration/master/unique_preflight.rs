//! The DDL-time pre-flight for `CREATE UNIQUE INDEX`: the fan-out
//! (`validate_unique_index_create`), the per-worker sorted-span streams
//! (`PreflightKeyStream`), their k-way merge (`merge_index_scan`) and the
//! per-key accounting it feeds (`PreflightAccumulator`).
//!
//! Nothing here is on the write path — `preflight.rs` holds that — and the only
//! state shared with it is the `UniqueFilter` this pre-flight seeds.

use super::*;

use super::unique_filter::UNIQUE_FILTER_CAP;
use gnitz_store::relation::Relation;

/// The `what` every unique pre-flight frame error is prefixed with.
const OP_UNIQUE_PREFLIGHT: &str = "unique pre-flight";

/// Per-worker state for one sorted-key stream in `merge_index_scan`.
///
/// The frame's key region is an offset into the pinned ring slot rather than a
/// borrowed view, so there is no self-reference and no drop-order contract.
struct PreflightKeyStream {
    /// Worker index, for error attribution only — the merge addresses streams
    /// by reply position.
    w: usize,
    /// Reply index into the scan, for frame pulls.
    reply: usize,
    /// The frame currently being read; pins its ring bytes until replaced.
    slot: Option<W2mSlot>,
    /// Byte offset of the frame's PK region into the slot, and its key count;
    /// zeroed for an empty frame. The stride is `frame_schema.pk_stride()` for
    /// every frame of every train, so it is not recorded here.
    pk_off: usize,
    count: usize,
    /// Cursor into the current frame's keys.
    row: usize,
    /// Current frame is non-terminal: its `scan_last` is clear.
    has_more: bool,
}

impl PreflightKeyStream {
    fn new(w: usize, reply: usize) -> Self {
        PreflightKeyStream {
            w,
            reply,
            slot: None,
            pk_off: 0,
            count: 0,
            row: 0,
            has_more: false,
        }
    }

    /// Install `slot` as the current frame, locating its key region.
    /// A fault/corrupt/undecodable frame is an immediate `Err` — the caller
    /// unwinds to the scan lease's drop, which discards the undrained trains.
    fn attach_frame(&mut self, slot: W2mSlot, frame_schema: &SchemaDescriptor) -> Result<(), WireFault> {
        self.row = 0;
        self.count = 0;
        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        let (has_more, batch) = decode_train_slot(&slot, self.w, OP_UNIQUE_PREFLIGHT, frame_schema, &mut offsets)?;
        self.has_more = has_more;
        let bytes = slot.bytes();
        if let Some(mb) = batch {
            let pk = mb.pk();
            // `pk` points inside `bytes`, which `slot` owns for as long as it is
            // held; record where, then let the view go.
            self.pk_off = pk.as_ptr() as usize - bytes.as_ptr() as usize;
            self.count = mb.len();
        }
        self.slot = Some(slot);
        Ok(())
    }

    /// The key at `row` of the current frame.
    fn key_at(&self, row: usize, pk_stride: usize) -> PkBuf {
        let bytes = self.slot.as_ref().expect("frame attached").bytes();
        let start = self.pk_off + row * pk_stride;
        PkBuf::from_bytes(&bytes[start..start + pk_stride])
    }

    /// Yield this worker's next span, pulling continuation frames on demand.
    /// Returns `Ok(None)` once the train is terminal.
    async fn next_key(
        &mut self,
        frame_schema: &SchemaDescriptor,
        scan: &ScanDispatch,
    ) -> Result<Option<PkBuf>, WireFault> {
        let pk_stride = frame_schema.pk_stride();
        loop {
            if self.row < self.count {
                let key = self.key_at(self.row, pk_stride);
                self.row += 1;
                return Ok(Some(key));
            }
            if !self.has_more {
                self.slot = None; // release the ring slot at the train's end
                return Ok(None);
            }
            let slot = scan.next_frame(self.reply).await;
            self.attach_frame(slot, frame_schema)?;
        }
    }
}

/// Per-key accounting for the pre-flight merge: duplicate verdict + inline
/// seed collection, fed keys in globally-sorted merge order. Split from the
/// frame-pulling loop so the verdict and the all-or-nothing seed rule are
/// directly testable with a small cap.
pub(crate) struct PreflightAccumulator {
    prev: Option<PkBuf>,
    pub(crate) duplicate: bool,
    /// The filter this pre-flight will publish. `insert` owns the cap
    /// discipline: on overflow it drops the set whole and disables itself, so
    /// the seed is never truncated — a truncated seed would publish a warm but
    /// incomplete filter whose "proven absent" answers would let a genuine
    /// duplicate skip the INSERT broadcast. Every span reaching `insert` is
    /// distinct (spans arrive sorted, so duplicates are adjacent and stop at
    /// the `prev` check).
    filter: UniqueFilter,
}

impl PreflightAccumulator {
    pub(crate) fn new(cap: usize) -> Self {
        PreflightAccumulator {
            prev: None,
            duplicate: false,
            filter: UniqueFilter::with_cap(cap),
        }
    }

    /// Offer the next span in globally-sorted merge order. Returns `false`
    /// once a duplicate is found — the verdict is monotonic, so the caller
    /// stops merging useful spans (but still drains every worker's train).
    /// Spans are byte-equal iff value-equal, so equality is a plain compare.
    pub(crate) fn offer(&mut self, key: PkBuf) -> bool {
        if self.duplicate {
            return false;
        }
        if self.prev == Some(key) {
            self.duplicate = true;
            return false;
        }
        self.prev = Some(key);
        self.filter.insert(key.pk_bytes());
        true
    }

    /// The filter holding every distinct span this pre-flight saw, ready to
    /// publish.
    pub(crate) fn into_seed(self) -> UniqueFilter {
        self.filter
    }
}

/// Streaming k-way merge over the per-worker SORTED key streams of a unique
/// pre-flight fan-out. Master memory is `O(num_workers)`; frame bytes the merge
/// has not reached stay in the per-worker W2M rings.
///
/// **The wire is byte-transparent for a span of any width.** A frame's whole PK
/// region IS the OPK leading-key span (`pk_stride == idx_key_size`), read
/// verbatim as a `PkBuf`, and both sides build `frame_schema` from the same
/// inputs — so nothing here decodes a column or takes a catalog lock.
///
/// One adjacent-equal check catches both duplicate classes: two equal keys from
/// one worker pop consecutively out of its sorted run, and the same value held
/// by two workers surfaces as two equal heads.
///
/// Returns on the FIRST error and on the first duplicate without draining the
/// rest — the caller's scan lease drop discards the undrained trains at the
/// ring boundary, as it does for `drain_index_scan`.
async fn merge_index_scan(
    slots: Vec<W2mSlot>,
    scan: &ScanDispatch,
    frame_schema: &SchemaDescriptor,
) -> Result<PreflightAccumulator, WireFault> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let nw = slots.len();
    let mut streams: Vec<PreflightKeyStream> = Vec::with_capacity(nw);

    // Ordered by (span, reply index) — byte-lexicographic via `PkBuf: Ord` —
    // so equal spans pop adjacently whichever workers hold them. The tie-break
    // is the REPLY index, which is also `streams`' index: a scan's lease ids
    // are reply-ordered, so a unicast answers on id 0 whatever worker it was.
    let mut heap: BinaryHeap<Reverse<(PkBuf, usize)>> = BinaryHeap::with_capacity(nw);
    for (i, slot) in slots.into_iter().enumerate() {
        let mut s = PreflightKeyStream::new(scan.worker(i), i);
        s.attach_frame(slot, frame_schema)?;
        if let Some(key) = s.next_key(frame_schema, scan).await? {
            heap.push(Reverse((key, i)));
        }
        streams.push(s);
    }

    let mut acc = PreflightAccumulator::new(UNIQUE_FILTER_CAP);
    while let Some(Reverse((key, i))) = heap.pop() {
        if !acc.offer(key) {
            break;
        } // first duplicate is conclusive
        if let Some(next) = streams[i].next_key(frame_schema, scan).await? {
            heap.push(Reverse((next, i)));
        }
    }
    Ok(acc)
}

impl MasterDispatcher {
    /// Pre-flight global uniqueness check for CREATE UNIQUE INDEX, distributed:
    /// each worker projects its committed partition to the indexed columns' OPK
    /// leading-key spans, sorts them locally (byte-lexicographic), and streams
    /// the SORTED spans back; the master runs a streaming k-way merge
    /// (`merge_index_scan`) whose single adjacent-equal check catches both
    /// within-partition and cross-partition duplicates. It is the ONLY
    /// uniqueness check: `backfill_index` runs no local one — a partition-local
    /// check cannot see a cross-partition duplicate, and its only way to reject
    /// would be to fatally `_exit` the worker. Master memory is
    /// `O(num_workers)` plus the (≤ cap) filter seed — never the table's
    /// distinct-key cardinality.
    ///
    /// On success the index is safe to commit and broadcast and the returned
    /// filter seeds the master's unique-filter cache; on failure the
    /// caller returns a client error and never broadcasts, so no worker
    /// reaches the fatal `DdlSync` backfill path.
    ///
    /// MUST run inside the DDL critical section (committer barrier drained,
    /// catalog write lock held) and BEFORE the IDX_TAB +1 is appended/broadcast,
    /// so the scanned snapshot is exactly the data each worker will later
    /// backfill and no concurrent INSERT can be ordered between the snapshot and
    /// the backfill.
    ///
    /// An unknown table yields an empty set (nothing to validate).
    pub async fn validate_unique_index_create(
        &self,
        owner_id: i64,
        col_indices: &[u32],
    ) -> Result<UniqueFilter, WireFault> {
        let (idx_schema, packed) = {
            let cat = self.cat();
            let owner_schema = match cat.registry().relation(owner_id).map(Relation::schema) {
                Some(s) => s,
                None => return Ok(UniqueFilter::new()),
            };
            // Trivial-uniqueness short-circuit, generalised to the compound PK:
            // a composite index whose columns are the table's enforced-unique PK
            // (set equality — the SQL may list them in another order) can never
            // collide, so the scan is skipped and the seed stays empty. The index
            // key IS the PK here, so a span collision would be a PK collision,
            // which `enforce_unique_pk` already makes impossible.
            let pk = owner_schema.pk_indices();
            if col_indices.len() == pk.len() && pk.iter().all(|p| col_indices.contains(p)) {
                return Ok(UniqueFilter::new());
            }
            // Build the index schema (the circuit is not registered until this
            // pre-flight succeeds) for the merge's reply-frame layout and the
            // promoted per-column widths. Identical inputs to each worker's own
            // build, so the frame schema agrees by construction. `packed` is
            // the column list the worker resolves the seek by.
            (
                gnitz_store::schema::make_index_schema(col_indices, &owner_schema)?,
                gnitz_wire::pack_pk_cols(col_indices),
            )
        };
        let frame_schema = unique_preflight_wire_schema(&idx_schema, col_indices.len());

        // Single-source a REPLICATED owner: under a fan-out each distinct value
        // would arrive `nw` times and pop adjacently in the merge —
        // `PreflightAccumulator::offer` reads that as a duplicate and fails the
        // CREATE on a genuinely unique table. The count-agnostic merge still
        // catches real within-copy duplicates via the same adjacent-equal
        // check, and the seed reflects true cardinality. Hashed owners keep the
        // full fan-out (genuine cross-partition duplicates surface as equal
        // spans from different workers).
        let unicast = read_fanout(self, owner_id);

        // Fan out the pre-flight command (the packed column list rides in
        // seek_col_idx); each worker answers with its sorted-span
        // continuation-frame train. `scan` holds the lease to end of scope: when
        // the merge returns early (error or duplicate verdict) the lease drop
        // discards the undrained trains at the ring boundary.
        let (slots, scan) = dispatch_scan_fanout(self, unicast, |targets| {
            // The worker's `UniquePreflight` arm resolves the owner's schema
            // from its own catalog.
            self.write_group(&DirectGroup {
                template: wire::WireMsg {
                    target_id: owner_id as u64,
                    seek_col_idx: packed,
                    ..Default::default()
                },
                targets,
                ..DirectGroup::new(SalMessageKind::UniquePreflight)
            })
        })
        .await?;

        let merged = merge_index_scan(slots, &scan, &frame_schema).await?;
        if merged.duplicate {
            return Err(self.cat().unique_create_dup_err(owner_id, col_indices).into());
        }
        Ok(merged.into_seed())
    }
}

#[cfg(test)]
#[path = "tests/unique_preflight.rs"]
mod tests;
