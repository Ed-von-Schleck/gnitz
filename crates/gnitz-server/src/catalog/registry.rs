//! Catalog id-registry: the system-table reads (point row, leading-key band,
//! filtered scan), the `sys_columns` → `ColumnDef` readers, name lookups,
//! catalog object-id allocation, and `_sequences` — the master scalars (next id,
//! checkpoint generation, topology word) and user SERIAL ranges.

use super::*;

/// Operator-state format version. Bump on any change to an operator-state
/// schema; a mismatch marks every Rederive view invalid at boot. Shard and
/// manifest layout carry their own version words.
const STATE_FORMAT: u32 = 13;

/// The durable topology word recorded in `_sequences` ([`SEQ_ID_TOPOLOGY`]):
/// `(worker_count << 32) | STATE_FORMAT`. One packer, shared by the boot-time
/// recorder and the resume-verdict validator.
pub(in crate::catalog) fn topology_word(worker_count: u32) -> u64 {
    ((worker_count as u64) << 32) | STATE_FORMAT as u64
}

/// The OPK of a U64 system id — also the leading-column prefix of a pair-keyed
/// family's key. Every system PK column is U64 (asserted beside `SYS_FAMILIES`).
fn sys_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

impl CatalogEngine {
    // -- System-table reads ---------------------------------------------------

    /// This family's relation, from the registry that owns it.
    pub(in crate::catalog) fn sys_relation(&self, family: SysFamily) -> &Relation {
        self.registry
            .relation(family.id())
            .expect("every system family is registered at open")
    }

    /// The live row of single-column-keyed `family` at `id`.
    pub(in crate::catalog) fn live_sys_row(&self, family: SysFamily, id: u64) -> Option<StoredRow> {
        self.sys_relation(family).live_row_at(&sys_key(id)).1
    }

    /// Visit every live row of `family` whose leading key column is `leading`: the
    /// row itself for a single-column key, the owner's whole band for a pair.
    pub(in crate::catalog) fn for_each_row_under(&self, family: SysFamily, leading: u64, f: impl FnMut(&ReadCursor)) {
        self.sys_relation(family)
            .for_each_positive_with_prefix(&sys_key(leading), f)
    }

    /// The live rows of `family` that `keep` selects, in key order.
    pub(in crate::catalog) fn sys_rows_where(&self, family: SysFamily, keep: impl Fn(&Batch, usize) -> bool) -> Batch {
        let scan = self.sys_relation(family).full_scan();
        let hits: Vec<u32> = (0..scan.len() as u32).filter(|&i| keep(&scan, i as usize)).collect();
        scan.ascending_subset(&hits)
    }

    // -- Read column definitions from sys_columns --------------------------

    /// Column definitions for `owner_id`, in key order. Its records must be keyed
    /// 0,1,2,… with no gap or duplicate: every consumer maps columns positionally.
    pub(in crate::catalog) fn read_column_defs(&self, owner_id: u64) -> Result<Vec<ColumnDef>, String> {
        let mut defs = Vec::new();
        let mut err = None;
        self.for_each_row_under(SysFamily::Column, owner_id, |c| {
            if err.is_some() {
                return;
            }
            let col_idx = gnitz_wire::unpack_pair_pk(c.current_key_narrow()).1;
            if col_idx != defs.len() as u64 {
                err = Some(format!(
                    "entity (owner_id={owner_id}): column records are non-contiguous; \
                     expected index {}, got {col_idx}",
                    defs.len()
                ));
                return;
            }
            let (src, row) = c.current_row_source();
            match read_col_tab_row(src, row) {
                Ok(d) => defs.push(d),
                Err(e) => err = Some(format!("entity (owner_id={owner_id}) column {col_idx}: {e}")),
            }
        });
        err.map_or(Ok(defs), Err)
    }

    // -- Registry query methods -----------------------------------------------

    pub(crate) fn has_schema(&self, name: &str) -> bool {
        self.caches.schema_by_name.contains_key(name)
    }

    /// The live rows of relation family `family` (Table or View) in schema `sid`.
    pub(in crate::catalog) fn schema_members(&self, family: SysFamily, sid: u64) -> Batch {
        self.sys_rows_where(family, |s, i| {
            payload_u64(s, i, gnitz_wire::RELTAB_PAY_SCHEMA_ID) == sid
        })
    }

    /// Raise the id counter past the id leading each of `batch`'s `rows`, when
    /// `family` allocates ids.
    pub(in crate::catalog) fn raise_next_id(
        &mut self,
        family: SysFamily,
        batch: &Batch,
        rows: impl Iterator<Item = usize>,
    ) {
        if family.allocates_ids() {
            for i in rows {
                let id = family.leading_id(batch.get_pk(i));
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

    /// The qualified `(schema, name)` of `table_id` from its live `sys_tables` or
    /// `sys_views` row; `"?"` for a part the catalog has no entry for.
    pub(crate) fn qualified_name_or_unknown(&self, table_id: u64) -> (String, String) {
        let Some(row) = self
            .live_sys_row(SysFamily::Table, table_id)
            .or_else(|| self.live_sys_row(SysFamily::View, table_id))
        else {
            return ("?".into(), "?".into());
        };
        let (src, ri) = row.source();
        let sid = payload_u64(src, ri, gnitz_wire::RELTAB_PAY_SCHEMA_ID);
        let schema = self.caches.schema_by_id.get(&sid).map_or("?", String::as_str);
        (schema.to_string(), payload_string(src, ri, gnitz_wire::RELTAB_PAY_NAME))
    }

    /// `table_id` as `schema.name`, or `?.?` when the catalog has no entry.
    pub(crate) fn qualified_name(&self, table_id: u64) -> String {
        let (schema, name) = self.qualified_name_or_unknown(table_id);
        gnitz_wire::qualified_key(&schema, &name)
    }

    /// `table_id`'s column names at `col_indices`, `, `-joined; `?` for one the
    /// catalog does not hold.
    pub(crate) fn column_names(&self, table_id: u64, col_indices: &[u32]) -> String {
        let defs = self.read_column_defs(table_id).unwrap_or_default();
        col_indices
            .iter()
            .map(|&ci| defs.get(ci as usize).map_or("?", |d| d.name.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The entity id registered under a canonical `"schema.relation"` key.
    pub(crate) fn entity_id_by_qname(&self, qname: &str) -> Option<u64> {
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
    pub(crate) fn sequence_value(&self, seq_id: u64) -> Option<u64> {
        let row = self.live_sys_row(SysFamily::Sequence, seq_id)?;
        let (src, ri) = row.source();
        Some(payload_u64(src, ri, gnitz_wire::SEQTAB_PAY_VALUE))
    }

    /// The delta moving `_sequences` row `seq_id` from its live value to `new`;
    /// empty when it already holds `new`.
    pub(in crate::catalog) fn sequence_delta(&self, seq_id: u64, new: u64) -> Batch {
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

    /// The base of the next `count` SERIAL ids of table `seq_id`, and the
    /// `_sequences` delta recording them.
    pub(crate) fn reserve_user_sequence(&self, seq_id: u64, count: u64) -> Result<(i64, Batch), String> {
        if !self.caches.relations.get(&seq_id).is_some_and(|e| e.facts.serial) {
            return Err(format!("relation {seq_id} is not a SERIAL table"));
        }
        let invalid = || format!("SERIAL range of {count} on sequence {seq_id} is invalid or exhausted");
        let base = self
            .sequence_value(seq_id)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(invalid)?;
        let last = validated_run_last(base, count, i64::MAX as u64).ok_or_else(invalid)?;
        Ok((base as i64, self.sequence_delta(seq_id, last)))
    }

    /// Move `_sequences` row `seq_id` to `value`.
    fn set_sequence(&mut self, seq_id: u64, value: u64) -> Result<(), String> {
        let delta = self.sequence_delta(seq_id, value);
        self.registry
            .ingest(SysFamily::Sequence.id(), delta)
            .map_err(|e| format!("sys_sequences ingest (seq {seq_id}) failed: {e}"))
    }

    /// Write the next catalog id to `_sequences`, then flush every system table
    /// in one barrier.
    pub(crate) fn flush_all_system_tables(&mut self) -> Result<(), String> {
        self.set_sequence(SEQ_ID_NEXT_ID, self.next_id)?;
        Ok(self.registry.checkpoint_system(self.system_zone)?)
    }

    /// Load the next catalog id stored in `_sequences`, and latch the registry's
    /// resume generation from the checkpoint rows beside it.
    pub(in crate::catalog) fn load_sequence_scalars(&mut self) {
        if let Some(v) = self.sequence_value(SEQ_ID_NEXT_ID) {
            self.next_id = self.next_id.max(v);
        }
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
    /// output stores, and every index, at `generation` — which every manifest
    /// published after it is stamped with too.
    pub(crate) fn flush_ephemeral_round(&mut self, generation: u64) -> Result<(), String> {
        self.registry.set_resume_generation(generation);
        Ok(self.registry.checkpoint_ephemeral(self.dag.ephemeral_states())?)
    }

    /// Record the launched topology. Durable at the next system flush.
    pub(crate) fn record_topology(&mut self, worker_count: u32) -> Result<(), String> {
        self.set_sequence(SEQ_ID_TOPOLOGY, topology_word(worker_count))
    }
}

/// The last id of a `count`-long run starting at `base`, or `None` if `count`
/// is zero, overflows against `base`, or reaches `ceiling`.
#[inline]
fn validated_run_last(base: u64, count: u64, ceiling: u64) -> Option<u64> {
    let last = base.checked_add(count.checked_sub(1)?)?;
    (last < ceiling).then_some(last)
}

#[cfg(test)]
#[path = "tests/registry.rs"]
mod tests;
