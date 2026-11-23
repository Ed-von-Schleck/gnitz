//! Catalog object-id allocation and `sequences`: the master scalars (next id,
//! checkpoint generation) and user SERIAL ranges.

use gnitz_wire::{SeqTabRow, SysRow};
use gnitz_zset::repr::{Batch, BatchBuilder};

use super::CatalogEngine;
use super::sys_tables::{SysFamily, SysFamilyExt};
use super::write_path::{ZoneError, ZoneSink};

/// The next catalog object id to allocate.
const SEQ_ID_NEXT_ID: u64 = 1;
/// The committed checkpoint generation's counter (monotonic).
const SEQ_ID_CHECKPOINT_GEN: u64 = 2;

// A SERIAL row is keyed by its table's id, so a scalar's key sits below every one.
const _: () = assert!(
    SEQ_ID_NEXT_ID < gnitz_wire::FIRST_USER_TABLE_ID && SEQ_ID_CHECKPOINT_GEN < gnitz_wire::FIRST_USER_TABLE_ID
);

/// Operator-state format version: bump when what a trace holds changes in a way
/// its schema does not show. It is part of every [`generation_stamp`], so no
/// store stamped under another format resumes: every view rebuilds at boot and
/// every index refills. A trace's schema needs no bump, since its store's name
/// carries the schema's digest; shard and manifest layout carry their own
/// version words.
const STATE_FORMAT: u64 = 16;

/// The bits of a generation stamp below [`STATE_FORMAT`].
const GENERATION_COUNTER_BITS: u32 = 40;

/// The checkpoint generation as rederived stores are stamped with it: the
/// stored counter under the operator-state format. A stamp is only ever
/// compared for equality, bar one ordering between two of one process.
fn generation_stamp(counter: u64) -> u64 {
    debug_assert!(counter < 1 << GENERATION_COUNTER_BITS);
    STATE_FORMAT << GENERATION_COUNTER_BITS | counter
}

impl CatalogEngine {
    /// The next catalog object id (schema, relation or index) to allocate,
    /// from its `sequences` row. The precheck admits no create at or above it.
    pub(crate) fn next_catalog_id(&self) -> u64 {
        self.sequence_value(SEQ_ID_NEXT_ID)
            .unwrap_or(gnitz_wire::FIRST_USER_TABLE_ID)
    }

    /// Allocate `count` contiguous catalog object ids and answer the first,
    /// laying the `sequences` delta recording the advance out through `sink`
    /// before it is applied.
    pub(crate) fn reserve_catalog_ids(&mut self, count: u64, sink: &mut ZoneSink<'_>) -> Result<u64, ZoneError> {
        let old = self.sequence_value(SEQ_ID_NEXT_ID);
        let base = old.unwrap_or(gnitz_wire::FIRST_USER_TABLE_ID);
        let last = validated_run_last(base, count, gnitz_wire::CATALOG_ID_CEILING).ok_or_else(|| {
            let why = match count {
                0 => "an id run holds at least one id".to_string(),
                _ => format!("the catalog id space cannot hold {count} more ids (next id {base})"),
            };
            ZoneError::Refused(format!("id allocation failed: {why}").into())
        })?;
        self.log_sequence(SEQ_ID_NEXT_ID, old, last + 1, sink)?;
        Ok(base)
    }

    // -- `sequences` -------------------------------------------------------

    /// The checkpoint generation's stamp, of counter `0` on a fresh database.
    pub(crate) fn durable_generation(&self) -> u64 {
        generation_stamp(self.sequence_value(SEQ_ID_CHECKPOINT_GEN).unwrap_or(0))
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
    ) -> Result<u64, ZoneError> {
        if !self.is_serial(seq_id) {
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
        self.log_sequence(seq_id, old, last, sink)?;
        Ok(base)
    }

    /// Move `sequences` row `seq_id` from `old` to `value`, laying the delta out
    /// through `sink` before it is applied.
    fn log_sequence(
        &mut self,
        seq_id: u64,
        old: Option<u64>,
        value: u64,
        sink: &mut ZoneSink<'_>,
    ) -> Result<(), ZoneError> {
        let delta = sequence_delta(seq_id, old, value);
        sink(SysFamily::Sequence, &delta).map_err(ZoneError::Refused)?;
        self.registry
            .ingest(SysFamily::Sequence.id(), delta)
            .map_err(ZoneError::Diverged)
    }

    /// Flush every system table in one barrier.
    pub(crate) fn flush_all_system_tables(&mut self) -> Result<(), String> {
        self.registry.checkpoint_system(self.applied_zone)
    }

    // -- Checkpoint generation -------------------------------------------------

    /// Make the system tables durable ahead of a base round. When `pushes`, the
    /// round moves the base stores past what the checkpointed views integrate,
    /// so the generation is advanced in the same flush.
    pub(crate) fn flush_ahead_of_base_round(&mut self, pushes: bool) -> Result<(), String> {
        if pushes {
            self.advance_durable_generation().map(drop)
        } else {
            self.flush_all_system_tables()
        }
    }

    /// Advance the checkpoint generation and flush it durable. Returns it.
    pub(crate) fn advance_durable_generation(&mut self) -> Result<u64, String> {
        let old = self.sequence_value(SEQ_ID_CHECKPOINT_GEN);
        let counter = old.unwrap_or(0) + 1;
        self.registry.ingest(
            SysFamily::Sequence.id(),
            sequence_delta(SEQ_ID_CHECKPOINT_GEN, old, counter),
        )?;
        self.flush_all_system_tables()
            .map_err(|e| format!("checkpoint generation flush failed: {e}"))?;
        Ok(generation_stamp(counter))
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
