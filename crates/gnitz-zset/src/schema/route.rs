//! Worker routing: which worker owns a key, for a cluster of a given size.

use gnitz_wire::{widen_pk_be, KeyRange, NARROW_PK_MAX_BYTES};

use crate::schema::{key, ColumnTable, SchemaDescriptor};

/// Map a 64-bit hash onto `0..num_workers` by multiply-shift.
#[inline(always)]
fn bucket(h: u64, num_workers: usize) -> usize {
    debug_assert!(num_workers >= 1, "worker routing: num_workers must be >= 1");
    ((h as u128 * num_workers as u128) >> 64) as usize
}

/// Which worker this process is, of how many: the one input that decides which
/// `w{k}of{n}` child every store of this process opens.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Slot {
    pub rank: u32,
    pub of: u32,
}

impl Slot {
    /// A one-worker process.
    pub const SOLO: Slot = Slot { rank: 0, of: 1 };

    /// Panics unless `rank < of`.
    pub fn new(rank: u32, of: u32) -> Slot {
        assert!(rank < of, "slot {rank} of {of}");
        Slot { rank, of }
    }
}

/// Where a relation's rows live. Held by the relation, beside its schema.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Placement {
    /// Every worker holds an identical full copy; writes broadcast, reads
    /// single-source worker 0.
    Replicated,
    /// Rows stay on the worker that produced them; a read gathers every worker.
    Local,
    /// A row lives on the worker its leading `dist_stride` OPK key bytes hash to.
    Keyed { dist_stride: u8 },
}

impl Placement {
    /// The worker whose copy of a replicated relation is the counted one: it
    /// alone captures a replicated view's delta feed, and it answers reads.
    pub const REPLICA_OWNER: u32 = 0;

    /// Keyed by the whole PK of `schema`.
    pub fn full_pk(schema: &SchemaDescriptor) -> Placement {
        Placement::Keyed { dist_stride: schema.pk_stride() as u8 }
    }

    /// Keyed by the leading `prefix_cols` PK columns of `schema`. Panics unless
    /// `prefix_cols` is in `1..=` the PK arity.
    pub fn keyed(schema: &SchemaDescriptor, prefix_cols: usize) -> Placement {
        let pk = schema.pk_cols();
        assert!(
            (1..=pk.len()).contains(&prefix_cols),
            "Placement::keyed: prefix {prefix_cols} of a {}-column PK",
            pk.len()
        );
        let dist_stride = pk[..prefix_cols]
            .iter()
            .map(|&c| schema.columns[c as usize].size())
            .sum();
        Placement::Keyed { dist_stride }
    }

    /// True iff worker `rank`'s copy is a counted one: every worker's for a
    /// partitioned relation, [`Self::REPLICA_OWNER`]'s alone for a replicated one.
    #[inline]
    pub const fn counts_on(self, rank: u32) -> bool {
        !self.is_replicated() || rank == Self::REPLICA_OWNER
    }

    /// True iff a row's owning worker is derived from its key.
    #[inline]
    pub const fn is_key_routed(self) -> bool {
        matches!(self, Placement::Keyed { .. })
    }

    /// True iff a full identical copy lives on every worker.
    #[inline]
    pub const fn is_replicated(self) -> bool {
        matches!(self, Placement::Replicated)
    }

    /// The one worker that can answer `range` over `schema`, when one provably
    /// can. Any worker answers an empty range, so worker 0 does.
    pub fn confined_worker(self, schema: &SchemaDescriptor, range: &KeyRange, num_workers: usize) -> Option<usize> {
        if !range.walks_pk(schema.pk_cols()) {
            return None;
        }
        let Some((start, end)) = schema.pk_range_keys(range) else {
            return Some(0);
        };
        let Placement::Keyed { dist_stride } = self else {
            return None;
        };
        key::range_shares_prefix(&start, end.as_ref(), dist_stride as usize)
            .then(|| worker_for_pk_bytes(&start.pk_bytes()[..dist_stride as usize], num_workers))
    }

    /// The worker that stores the row keyed `pk`; `None` unless rows are
    /// key-routed.
    pub fn owner(self, pk: &[u8], num_workers: usize) -> Option<usize> {
        match self {
            Placement::Keyed { dist_stride } => Some(worker_for_pk_bytes(&pk[..dist_stride as usize], num_workers)),
            _ => None,
        }
    }
}

/// Which of `num_workers` workers owns `key`.
#[inline(always)]
pub(crate) fn worker_for_key(pk: u128, num_workers: usize) -> usize {
    let lo = pk as u64;
    let hi = (pk >> 64) as u64;
    bucket(
        lo.wrapping_mul(0x9e3779b97f4a7c15_u64) ^ hi.wrapping_mul(0x6c62272e07bb0142_u64),
        num_workers,
    )
}

/// Which of `num_workers` workers owns an OPK PK region: a narrow one by its
/// `u128` image, a wide one by its hash.
#[inline(always)]
pub(crate) fn worker_for_pk_bytes(bytes: &[u8], num_workers: usize) -> usize {
    if bytes.len() <= NARROW_PK_MAX_BYTES {
        worker_for_key(widen_pk_be(bytes), num_workers)
    } else {
        worker_for_wide_pk(bytes, num_workers)
    }
}

#[inline(never)]
fn worker_for_wide_pk(bytes: &[u8], num_workers: usize) -> usize {
    bucket(gnitz_wire::checksum(bytes), num_workers)
}

/// The worker that owns a global (group-less) aggregate's ground row: where the
/// router sends a row keyed by no columns.
pub fn ground_owner(num_workers: usize) -> usize {
    worker_for_key(gnitz_wire::global_group_key(), num_workers)
}

#[cfg(test)]
#[path = "tests/route.rs"]
mod tests;
