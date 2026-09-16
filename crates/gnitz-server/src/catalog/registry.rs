//! Catalog id-registry: the `sys_columns` → `ColumnDef` readers, name lookups,
//! catalog object-id allocation, and `_sequences` — the master scalars (next id,
//! checkpoint generation, topology word) and user SERIAL ranges.

use super::*;

/// Operator-state format version. Bump on any change to an operator-state
/// schema; a mismatch marks every Rederive view invalid at boot. Shard and
/// manifest layout carry their own version words.
const STATE_FORMAT: u32 = 8;

/// The durable topology word recorded in `_sequences` ([`SEQ_ID_TOPOLOGY`]):
/// `(worker_count << 32) | STATE_FORMAT`. One packer, shared by the boot-time
/// recorder and the resume-verdict validator.
pub(in crate::catalog) fn topology_word(worker_count: u32) -> u64 {
    ((worker_count as u64) << 32) | STATE_FORMAT as u64
}

impl CatalogEngine {
    // -- Read column definitions from sys_columns --------------------------

    /// `owner_id`'s live column records must be keyed 0,1,2,… with no gap or
    /// duplicate: `build_schema_from_col_defs` maps columns positionally.
    pub(in crate::catalog) fn check_column_contiguity(&self, owner_id: i64) -> Result<(), String> {
        let mut keyed = Vec::new();
        self.for_each_row_under(SysFamily::Column, owner_id, |c| {
            keyed.push(gnitz_wire::unpack_pair_pk(c.current_key_narrow()).1)
        });
        match keyed
            .into_iter()
            .zip(0u64..)
            .find(|(actual, expected)| actual != expected)
        {
            Some((actual, expected)) => Err(format!(
                "entity (owner_id={owner_id}): column records are non-contiguous; \
                 expected index {expected}, got {actual}"
            )),
            None => Ok(()),
        }
    }

    /// Column definitions for `owner_id`, in key order, cached until the next
    /// COL_TAB delta.
    pub(crate) fn read_column_defs(&mut self, owner_id: i64) -> Rc<Vec<ColumnDef>> {
        if let Some(defs) = self.caches.col_defs.get(&owner_id) {
            return defs.clone();
        }
        let mut defs = Vec::new();
        self.for_each_row_under(SysFamily::Column, owner_id, |c| {
            let (src, row) = c.current_row_source();
            defs.push(read_col_tab_row(src, row));
        });
        let defs = Rc::new(defs);
        self.caches.col_defs.insert(owner_id, defs.clone());
        defs
    }

    // -- Registry query methods -----------------------------------------------

    pub(crate) fn has_schema(&self, name: &str) -> bool {
        self.caches.schema_by_name.contains_key(name)
    }

    /// Number of live member relations (tables + views) in schema `sid`.
    pub(in crate::catalog) fn schema_member_count(&self, sid: i64) -> usize {
        self.caches.members_by_schema.get(&sid).map_or(0, |s| s.len())
    }

    /// Allocate `count` contiguous catalog object ids and return the first.
    pub(crate) fn allocate_ids(&mut self, count: u64) -> Result<i64, String> {
        let base = self.next_id;
        let last = validated_run_last(base, count, CATALOG_ID_CEILING)
            .ok_or_else(|| format!("id run length {count} is invalid (base {base})"))?;
        self.next_id = last + 1;
        Ok(base)
    }

    /// The qualified `(schema, name)` of `table_id`, or `("?", "?")` when the
    /// catalog has no entry.
    pub(in crate::catalog) fn qualified_name_or_unknown(&self, table_id: i64) -> (&str, &str) {
        self.caches
            .entity_by_id
            .get(&table_id)
            .map_or(("?", "?"), |(s, t)| (s.as_str(), t.as_str()))
    }

    /// The entity id registered under a canonical `"schema.relation"` key.
    pub(crate) fn entity_id_by_qname(&self, qname: &str) -> Option<i64> {
        self.caches.entity_by_qname.get(qname).copied()
    }

    // -- `_sequences` -------------------------------------------------------

    /// The live value of `_sequences` row `seq_id`, if one is stored.
    pub(crate) fn sequence_value(&self, seq_id: i64) -> Option<u64> {
        let row = self.live_sys_row(SysFamily::Sequence, seq_id)?;
        let (src, ri) = row.source();
        Some(payload_u64(src, ri, gnitz_wire::SEQTAB_PAY_VALUE))
    }

    /// The delta moving `_sequences` row `seq_id` from its live value to `new`;
    /// empty when it already holds `new`.
    pub(in crate::catalog) fn sequence_delta(&self, seq_id: i64, new: u64) -> Batch {
        let old = self.sequence_value(seq_id);
        let mut bb = BatchBuilder::new(*SysFamily::Sequence.schema());
        if old != Some(new) {
            for (value, w) in old.map(|v| (v, -1)).into_iter().chain([(new, 1)]) {
                bb.begin_row(seq_id as u128, w);
                bb.put_u64(value);
                bb.end_row();
            }
        }
        bb.finish()
    }

    /// The base of the next `count` SERIAL ids of base table `seq_id`, and the
    /// `_sequences` delta recording them.
    pub(crate) fn reserve_user_sequence(&self, seq_id: i64, count: u64) -> Result<(i64, Batch), String> {
        if !self.registry.relation(seq_id).is_some_and(|r| r.kind().is_base_table()) {
            return Err(format!("sequence {seq_id} is not a base table"));
        }
        let invalid = || format!("SERIAL range of {count} on sequence {seq_id} is invalid or exhausted");
        let high_water = i64::try_from(self.sequence_value(seq_id).unwrap_or(0)).map_err(|_| invalid())?;
        let base = high_water.checked_add(1).ok_or_else(invalid)?;
        let last = validated_run_last(base, count, i64::MAX).ok_or_else(invalid)?;
        Ok((base, self.sequence_delta(seq_id, last as u64)))
    }

    /// The master scalars, by seq id: what [`Self::flush_all_system_tables`]
    /// writes to `_sequences` and [`Self::load_sequence_scalars`] reads back.
    fn sequence_scalars(&self) -> [(i64, u64); 3] {
        [
            (SEQ_ID_NEXT_ID, self.next_id as u64),
            (SEQ_ID_CHECKPOINT_GEN, self.durable_generation),
            (SEQ_ID_TOPOLOGY, self.recorded_topology),
        ]
    }

    /// Write the master scalars to `_sequences`, then flush every system table in
    /// one barrier.
    pub(crate) fn flush_all_system_tables(&mut self) -> Result<(), String> {
        for (seq_id, value) in self.sequence_scalars() {
            let delta = self.sequence_delta(seq_id, value);
            self.registry
                .ingest(SysFamily::Sequence.id(), delta)
                .map_err(|e| format!("sys_sequences ingest (seq {seq_id}) failed: {e}"))?;
        }
        self.registry.checkpoint_system().map_err(|e| e.to_string())
    }

    /// Load the master scalars stored in `_sequences`, and latch the registry's
    /// resume verdict and generation from them.
    pub(in crate::catalog) fn load_sequence_scalars(&mut self) {
        if let Some(v) = self.sequence_value(SEQ_ID_NEXT_ID) {
            self.next_id = v as i64;
        }
        if let Some(v) = self.sequence_value(SEQ_ID_CHECKPOINT_GEN) {
            self.durable_generation = v;
        }
        if let Some(v) = self.sequence_value(SEQ_ID_TOPOLOGY) {
            self.recorded_topology = v;
        }
        self.registry.set_resume_enabled(self.topology_matches());
        self.registry.set_resume_generation(self.durable_generation);
    }

    // -- Checkpoint records -------------------------------------------------

    /// Advance the checkpoint generation and flush it durable, leaving the resume
    /// generation where it is.
    pub(crate) fn advance_durable_generation(&mut self) -> Result<u64, String> {
        self.durable_generation += 1;
        self.flush_all_system_tables()
            .map_err(|e| format!("checkpoint generation flush failed: {e}"))?;
        Ok(self.durable_generation)
    }

    /// [`Self::advance_durable_generation`], then stamp every manifest published
    /// from here on with the new generation. Returns it.
    pub(crate) fn bump_checkpoint_generation(&mut self) -> Result<u64, String> {
        let g = self.advance_durable_generation()?;
        self.registry.set_resume_generation(g);
        Ok(g)
    }

    /// Record the launched topology and latch the registry's resume verdict from
    /// it.
    pub(crate) fn record_topology(&mut self, worker_count: u32) {
        self.recorded_topology = topology_word(worker_count);
        self.registry.set_resume_enabled(self.topology_matches());
    }
}

/// The last id of a `count`-long run starting at `base`, or `None` if `count`
/// is zero, doesn't fit an `i64`, overflows against `base`, or reaches `ceiling`.
#[inline]
fn validated_run_last(base: i64, count: u64, ceiling: i64) -> Option<i64> {
    let count = i64::try_from(count).ok().filter(|&c| c > 0)?;
    let last = base.checked_add(count)?.checked_sub(1)?;
    (last < ceiling).then_some(last)
}

#[cfg(test)]
#[path = "tests/registry.rs"]
mod tests;
