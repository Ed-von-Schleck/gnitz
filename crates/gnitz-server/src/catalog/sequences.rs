//! Catalog object-id allocation and `sequences`: the master scalars (next id,
//! checkpoint generation, topology word), user SERIAL ranges and the checkpoint
//! records.

use gnitz_wire::{SeqTabRow, SysRow};
use gnitz_zset::repr::{Batch, BatchBuilder};

use super::sys_tables::SysFamily;
use super::write_path::{ZoneError, ZoneSink};
use super::CatalogEngine;

/// The next catalog object id to allocate.
const SEQ_ID_NEXT_ID: u64 = 1;
/// Committed checkpoint generation (monotonic).
const SEQ_ID_CHECKPOINT_GEN: u64 = 2;
/// Cluster topology: [`topology_word`].
const SEQ_ID_TOPOLOGY: u64 = 3;

/// Operator-state format version. Bump on any change to an operator-state
/// schema; a mismatch marks every Rederive view invalid at boot. Shard and
/// manifest layout carry their own version words.
const STATE_FORMAT: u32 = 16;

/// The durable topology word recorded in `sequences` ([`SEQ_ID_TOPOLOGY`]):
/// `(worker_count << 32) | STATE_FORMAT`. One packer, shared by the boot-time
/// recorder and the resume-verdict validator.
fn topology_word(worker_count: u32) -> u64 {
    ((worker_count as u64) << 32) | STATE_FORMAT as u64
}

impl CatalogEngine {
    /// Raise the id counter past the id leading each of `batch`'s rows, when
    /// `family` allocates ids. For rows recovered at boot: a live DDL's ids were
    /// allocated, which the precheck holds it to.
    pub(in crate::catalog) fn raise_next_id(&mut self, family: SysFamily, batch: &Batch) {
        if family.allocates_ids() {
            for i in 0..batch.len() {
                let id = gnitz_wire::sys_leading_id(batch.get_pk_bytes(i));
                self.next_id = self.next_id.max(id.saturating_add(1));
            }
        }
    }

    /// Allocate `count` contiguous catalog object ids and return the first.
    pub(crate) fn allocate_ids(&mut self, count: u64) -> Result<u64, String> {
        let base = self.next_id;
        let last = validated_run_last(base, count, gnitz_wire::CATALOG_ID_CEILING)
            .ok_or_else(|| format!("id run length {count} is invalid (base {base})"))?;
        self.next_id = last + 1;
        Ok(base)
    }

    // -- `sequences` -------------------------------------------------------

    /// Whether persisted derived state was written under this boot's topology.
    pub(in crate::catalog) fn topology_matches(&self) -> bool {
        self.sequence_value(SEQ_ID_TOPOLOGY) == Some(topology_word(self.registry.slot().of))
    }

    /// The checkpoint generation, `0` on a fresh database.
    pub(crate) fn durable_generation(&self) -> u64 {
        self.sequence_value(SEQ_ID_CHECKPOINT_GEN).unwrap_or(0)
    }

    /// The live value of `sequences` row `seq_id`, if one is stored.
    pub(crate) fn sequence_value(&self, seq_id: u64) -> Option<u64> {
        let row = self.live_sys_row(SysFamily::Sequence, seq_id)?;
        let (src, ri) = row.source();
        let Ok(stored) = SeqTabRow::read(src, ri);
        Some(stored.next_val)
    }

    /// Reserve the next `count` SERIAL ids of table `seq_id` and answer their
    /// base, laying the `sequences` delta recording them out through `sink`
    /// before it is applied.
    pub(crate) fn reserve_user_sequence(
        &mut self,
        seq_id: u64,
        count: u64,
        sink: &mut ZoneSink<'_>,
    ) -> Result<i64, ZoneError> {
        if !self.caches.relations.get(&seq_id).is_some_and(|e| e.serial) {
            return Err(ZoneError::Refused(
                format!("relation {seq_id} is not a SERIAL table").into(),
            ));
        }
        let invalid = || {
            ZoneError::Refused(format!("SERIAL range of {count} on sequence {seq_id} is invalid or exhausted").into())
        };
        let old = self.sequence_value(seq_id);
        let base = old.unwrap_or(0).checked_add(1).ok_or_else(invalid)?;
        let last = validated_run_last(base, count, i64::MAX as u64).ok_or_else(invalid)?;
        let delta = sequence_delta(seq_id, old, last);
        sink(SysFamily::Sequence, &delta).map_err(ZoneError::Refused)?;
        self.registry
            .ingest(SysFamily::Sequence.id(), delta)
            .map_err(ZoneError::Diverged)?;
        Ok(base as i64)
    }

    /// Move `sequences` row `seq_id` to `value`.
    fn set_sequence(&mut self, seq_id: u64, value: u64) -> Result<(), String> {
        let old = self.sequence_value(seq_id);
        if old == Some(value) {
            return Ok(());
        }
        self.registry
            .ingest(SysFamily::Sequence.id(), sequence_delta(seq_id, old, value))
    }

    /// Write the next catalog id to `sequences`, then flush every system table
    /// in one barrier.
    pub(crate) fn flush_all_system_tables(&mut self) -> Result<(), String> {
        self.set_sequence(SEQ_ID_NEXT_ID, self.next_id)?;
        self.registry.checkpoint_system(self.system_zone)
    }

    /// Load the next catalog id stored in `sequences`, and latch the resume
    /// generation from the checkpoint rows beside it.
    pub(in crate::catalog) fn load_sequence_scalars(&mut self) {
        if let Some(v) = self.sequence_value(SEQ_ID_NEXT_ID) {
            self.next_id = self.next_id.max(v);
        }
        self.resume_generation = self.durable_generation();
    }

    // -- Checkpoint records -------------------------------------------------

    /// Advance the checkpoint generation and flush it durable. Returns it.
    pub(crate) fn advance_durable_generation(&mut self) -> Result<u64, String> {
        let g = self.durable_generation() + 1;
        self.set_sequence(SEQ_ID_CHECKPOINT_GEN, g)?;
        self.flush_all_system_tables()
            .map_err(|e| format!("checkpoint generation flush failed: {e}"))?;
        Ok(g)
    }

    /// The ephemeral checkpoint round: persist the operator traces, output store
    /// and indexes of every view a boot can resume, and every table's indexes, at
    /// `generation`.
    pub(crate) fn flush_ephemeral_round(&mut self, generation: u64) -> Result<(), String> {
        let never = self.never_resumed_views();
        let resumes = |id| !never.contains(&id);
        let states = self.dag.ephemeral_states(&self.registry, resumes)?;
        self.registry.checkpoint_ephemeral(states, generation, resumes)
    }

    /// Record the launched topology. Durable at the next system flush.
    pub(crate) fn record_topology(&mut self, worker_count: u32) -> Result<(), String> {
        self.set_sequence(SEQ_ID_TOPOLOGY, topology_word(worker_count))
    }
}

/// `sequences` row `seq_id` moved from `old` to `value`.
fn sequence_delta(seq_id: u64, old: Option<u64>, value: u64) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Sequence.schema());
    for (next_val, w) in old.map(|v| (v, -1)).into_iter().chain([(value, 1)]) {
        SeqTabRow { seq_id, next_val }.write(&mut bb, w);
    }
    bb.finish()
}

/// The last id of a `count`-long run starting at `base`, or `None` if `count`
/// is zero, overflows against `base`, or reaches `ceiling`.
#[inline]
fn validated_run_last(base: u64, count: u64, ceiling: u64) -> Option<u64> {
    let last = base.checked_add(count.checked_sub(1)?)?;
    (last < ceiling).then_some(last)
}
