//! The storage LSM — the lifecycle of one store: which runs it holds, and when
//! they flush, compact and publish a manifest. The runs themselves, and the
//! cursor and merges over them, are `gnitz_zset::repr`'s.

mod batch_fsync;
mod bloom;
mod manifest;
mod run_set;
mod shard_index;
mod table;

#[cfg(test)]
mod suites;

pub(crate) use manifest::{
    RecoverySource, link_stores, manifest_path, read_at, read_intact, retire_stores, shard_files,
};
pub(crate) use table::{DEFAULT_RAM_TIER_BYTES, Table, flush_barrier};

/// Which time-prefix of a store's ingests a read sees. A store's runs are its
/// history in order, so a prefix of them is the Z-set the store held then.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cut {
    /// Every ingest.
    Now,
    /// Every ingest up to the last [`Table::seal`]: the rows ingested as
    /// pending since are left out.
    Sealed,
}

/// The **inclusive** OPK ranges `[min, max]` and `[lo, hi]` intersect — whether a
/// run or shard holding `[min, max]` can answer about a key the caller's bound
/// admits. A half-open upper bound passed as `hi` over-approximates, which is
/// the safe direction.
#[inline]
fn pk_ranges_overlap(min: &[u8], max: &[u8], lo: &[u8], hi: &[u8]) -> bool {
    max >= lo && min <= hi
}
