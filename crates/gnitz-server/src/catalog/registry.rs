//! Catalog id-registry: the `sys_columns` → `ColumnDef` readers, name lookups,
//! catalog object-id allocation, and `_sequences` — the master scalars (next id,
//! checkpoint generation, topology word) and user SERIAL ranges.

use super::*;
use gnitz_expr::payload_str;

/// Operator-state format version. Bump on any change to an operator-state
/// schema; a mismatch marks every Rederive view invalid at boot. Shard and
/// manifest layout carry their own version words.
const STATE_FORMAT: u32 = 10;

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

    /// Column definitions for `owner_id`, in key order.
    pub(in crate::catalog) fn read_column_defs(&self, owner_id: i64) -> Vec<ColumnDef> {
        let mut defs = Vec::new();
        self.for_each_row_under(SysFamily::Column, owner_id, |c| {
            let (src, row) = c.current_row_source();
            defs.push(read_col_tab_row(src, row));
        });
        defs
    }

    // -- Registry query methods -----------------------------------------------

    pub(crate) fn has_schema(&self, name: &str) -> bool {
        self.caches.schema_by_name.contains_key(name)
    }

    /// The ids of the live member relations (tables and views) of schema `sid`.
    pub(in crate::catalog) fn schema_members(&self, sid: i64) -> Vec<i64> {
        let mut ids = self.ids_naming(SysFamily::Table, gnitz_wire::RELTAB_PAY_SCHEMA_ID, &[sid]);
        ids.extend(self.ids_naming(SysFamily::View, gnitz_wire::RELTAB_PAY_SCHEMA_ID, &[sid]));
        ids
    }

    /// Allocate `count` contiguous catalog object ids and return the first.
    pub(crate) fn allocate_ids(&mut self, count: u64) -> Result<i64, String> {
        let base = self.next_id;
        let last = validated_run_last(base, count, CATALOG_ID_CEILING)
            .ok_or_else(|| format!("id run length {count} is invalid (base {base})"))?;
        self.next_id = last + 1;
        Ok(base)
    }

    /// `table_id`'s live `sys_tables` or `sys_views` row and the name of the
    /// schema it sits in (`"?"` for none).
    fn relation_name_row(&self, table_id: i64) -> Option<(StoredRow, &str)> {
        let row = self
            .live_sys_row(SysFamily::Table, table_id)
            .or_else(|| self.live_sys_row(SysFamily::View, table_id))?;
        let (src, ri) = row.source();
        let sid = payload_u64(src, ri, gnitz_wire::RELTAB_PAY_SCHEMA_ID) as i64;
        let schema = self.caches.schema_by_id.get(&sid).map_or("?", String::as_str);
        Some((row, schema))
    }

    /// The qualified `(schema, name)` of `table_id`, or `("?", "?")` when the
    /// catalog has no entry.
    pub(crate) fn qualified_name_or_unknown(&self, table_id: i64) -> (String, String) {
        let Some((row, schema)) = self.relation_name_row(table_id) else {
            return ("?".into(), "?".into());
        };
        let (src, ri) = row.source();
        (schema.to_string(), payload_string(src, ri, gnitz_wire::RELTAB_PAY_NAME))
    }

    /// `table_id` as `schema.name`, or `?.?` when the catalog has no entry.
    pub(crate) fn qualified_name(&self, table_id: i64) -> String {
        let Some((row, schema)) = self.relation_name_row(table_id) else {
            return gnitz_wire::qualified_key("?", "?");
        };
        let (src, ri) = row.source();
        gnitz_wire::qualified_key(schema, payload_str(src, ri, gnitz_wire::RELTAB_PAY_NAME))
    }

    /// `table_id`'s column names at `col_indices`, `, `-joined; `?` for one the
    /// catalog does not hold.
    pub(crate) fn column_names(&self, table_id: i64, col_indices: &[u32]) -> String {
        let defs = self.read_column_defs(table_id);
        col_indices
            .iter()
            .map(|&ci| defs.get(ci as usize).map_or("?", |d| d.name.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The entity id registered under a canonical `"schema.relation"` key.
    pub(crate) fn entity_id_by_qname(&self, qname: &str) -> Option<i64> {
        self.caches.entity_by_qname.get(qname).copied()
    }

    // -- `_sequences` -------------------------------------------------------

    /// Whether persisted derived state was written under this boot's topology.
    pub(in crate::catalog) fn topology_matches(&self) -> bool {
        self.sequence_value(SEQ_ID_TOPOLOGY) == Some(topology_word(self.registry.slot().of))
    }

    /// The checkpoint generation, `0` on a fresh database.
    pub(crate) fn durable_generation(&self) -> u64 {
        self.sequence_value(SEQ_ID_CHECKPOINT_GEN).unwrap_or(0)
    }

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

    /// Move `_sequences` row `seq_id` to `value`.
    fn set_sequence(&mut self, seq_id: i64, value: u64) -> Result<(), String> {
        let delta = self.sequence_delta(seq_id, value);
        self.registry
            .ingest(SysFamily::Sequence.id(), delta)
            .map_err(|e| format!("sys_sequences ingest (seq {seq_id}) failed: {e}"))
    }

    /// Write the next catalog id to `_sequences`, then flush every system table
    /// in one barrier.
    pub(crate) fn flush_all_system_tables(&mut self) -> Result<(), String> {
        self.set_sequence(SEQ_ID_NEXT_ID, self.next_id as u64)?;
        Ok(self.registry.checkpoint_system()?)
    }

    /// Load the next catalog id stored in `_sequences`, and latch the registry's
    /// resume verdict and generation from the checkpoint rows beside it.
    pub(in crate::catalog) fn load_sequence_scalars(&mut self) {
        if let Some(v) = self.sequence_value(SEQ_ID_NEXT_ID) {
            self.next_id = v as i64;
        }
        self.registry.set_resume_enabled(self.topology_matches());
        self.registry.set_resume_generation(self.durable_generation());
    }

    // -- Checkpoint records -------------------------------------------------

    /// Advance the checkpoint generation and flush it durable, leaving the resume
    /// generation where it is.
    pub(crate) fn advance_durable_generation(&mut self) -> Result<u64, String> {
        let g = self.durable_generation() + 1;
        self.set_sequence(SEQ_ID_CHECKPOINT_GEN, g)?;
        self.flush_all_system_tables()
            .map_err(|e| format!("checkpoint generation flush failed: {e}"))?;
        Ok(g)
    }

    /// [`Self::advance_durable_generation`], then stamp every manifest published
    /// from here on with the new generation. Returns it.
    pub(crate) fn bump_checkpoint_generation(&mut self) -> Result<u64, String> {
        let g = self.advance_durable_generation()?;
        self.registry.set_resume_generation(g);
        Ok(g)
    }

    /// The ephemeral checkpoint round: persist every view's operator traces and
    /// output stores, and every index, at the registry's resume generation.
    pub(crate) fn flush_ephemeral_round(&mut self) -> Result<(), String> {
        Ok(self.registry.checkpoint_ephemeral(self.dag.collect_ephemeral_state())?)
    }

    /// Unlink the manifest of every store [`Self::flush_ephemeral_round`]
    /// publishes, so the next open erases those stores instead of resuming them.
    pub(crate) fn unlink_derived_manifests(&mut self) {
        self.registry
            .unlink_ephemeral_manifests(self.dag.collect_ephemeral_state());
    }

    /// Record the launched topology and latch the registry's resume verdict from
    /// it. Durable at the next system flush.
    pub(crate) fn record_topology(&mut self, worker_count: u32) -> Result<(), String> {
        self.set_sequence(SEQ_ID_TOPOLOGY, topology_word(worker_count))?;
        self.registry.set_resume_enabled(self.topology_matches());
        Ok(())
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
