use super::*;
use crate::schema::promote_to_index_key;

impl CatalogEngine {
    // -- FK column validation (pre-create) ---------------------------------

    pub(crate) fn validate_fk_column(
        &self,
        col: &ColumnDef,
        _col_idx: usize,
        self_table_id: i64,
        self_pk_index: u32,
        self_pk_type: u8,
    ) -> Result<(), String> {
        if col.fk_table_id == 0 { return Ok(()); }

        let (target_pk_index, target_pk_type) = if col.fk_table_id == self_table_id {
            (self_pk_index, self_pk_type)
        } else {
            let entry = self.dag.tables.get(&col.fk_table_id)
                .ok_or_else(|| format!("FK references unknown table_id {}", col.fk_table_id))?;
            (entry.schema.pk_index_single(), entry.schema.columns[entry.schema.pk_index_single() as usize].type_code)
        };

        if col.fk_col_idx != target_pk_index {
            return Err("FK must reference target PK".into());
        }

        let promoted = get_index_key_type(col.type_code)?;
        if promoted != target_pk_type {
            return Err(format!(
                "FK type mismatch: promoted code {} vs target {}",
                promoted, target_pk_type
            ));
        }
        Ok(())
    }

    // -- FK inline validation (single-worker) ------------------------------

    pub(crate) fn validate_fk_inline(&self, table_id: i64, batch: &Batch) -> Result<(), String> {
        let constraints = match self.caches.fk_by_child.get(&table_id) {
            Some(c) if !c.is_empty() => c,
            _ => return Ok(()),
        };

        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;
        let schema = entry.schema;

        for constraint in constraints {
            let col_idx = constraint.fk_col_idx;
            let target_id = constraint.target_table_id;

            let target_entry = self.dag.tables.get(&target_id)
                .ok_or_else(|| format!("FK target table {} not found", target_id))?;

            for row in 0..batch.count {
                if batch.get_weight(row) <= 0 { continue; }

                // Check null
                let null_word = batch.get_null_word(row);
                let payload_col = schema.payload_idx(col_idx);
                if null_word & (1u64 << payload_col) != 0 { continue; }

                // Promote column value to PK key
                let col_type = schema.columns[col_idx].type_code;
                let col_size = schema.columns[col_idx].size() as usize;
                let col_data = batch.col_data(payload_col);
                let fk_key = promote_to_index_key(col_data, row * col_size, col_size, col_type);

                // Check if target has this PK
                if !target_entry.handle.has_pk(fk_key) {
                    let (sn, tn) = self.caches.entity_by_id.get(&table_id)
                        .cloned().unwrap_or_default();
                    let (tsn, ttn) = self.caches.entity_by_id.get(&target_id)
                        .cloned().unwrap_or_default();
                    return Err(format!(
                        "Foreign Key violation in '{}.{}': value not found in target '{}.{}'",
                        sn, tn, tsn, ttn
                    ));
                }
            }
        }
        Ok(())
    }

    /// Validate FK RESTRICT on parent DELETE (single-worker path).
    /// For each retraction row, checks whether any child table still references
    /// the PK being deleted via the child's FK index. Returns an error if so.
    pub(crate) fn validate_fk_parent_restrict(&self, table_id: i64, batch: &Batch) -> Result<(), String> {
        let children = self.fk_children_of(table_id);
        if children.is_empty() { return Ok(()); }

        let parent_pk_stride = self.dag.tables.get(&table_id)
            .map(|e| e.schema.pk_stride() as usize)
            .unwrap_or(8);

        for &(child_tid, fk_col_idx) in children {
            let child_entry = match self.dag.tables.get(&child_tid) {
                Some(e) => e,
                None => continue,
            };
            let ic = match child_entry.index_circuits.iter()
                .find(|ic| ic.col_idx == fk_col_idx as u32)
            {
                Some(ic) => ic,
                None => {
                    return Err(format!(
                        "FK RESTRICT check failed: no index on child table {} col {}",
                        child_tid, fk_col_idx,
                    ));
                }
            };
            let idx_key_size = ic.index_schema.columns[0].size() as usize;
            let prefix_len = parent_pk_stride.min(idx_key_size);
            let idx_table = ic.table_mut();
            let mut cursor = idx_table.create_cursor_no_compact();

            for row in 0..batch.count {
                if batch.get_weight(row) >= 0 { continue; }
                // Prefix-scan the child's FK index for any positive-weight
                // entry whose leading column matches this parent's PK.
                let pk_bytes_full = batch.get_pk_bytes(row);
                let prefix = &pk_bytes_full[..prefix_len];
                let referenced = cursor.cursor.seek_first_positive_with_prefix(prefix);

                if referenced {
                    let (sn, tn) = self.caches.entity_by_id.get(&table_id)
                        .cloned().unwrap_or_default();
                    let (csn, ctn) = self.caches.entity_by_id.get(&child_tid)
                        .cloned().unwrap_or_default();
                    return Err(format!(
                        "Foreign Key violation: cannot delete from '{}.{}', row still referenced by '{}.{}'",
                        sn, tn, csn, ctn,
                    ));
                }
            }
        }
        Ok(())
    }

    /// Validate unique index constraints (single-worker path).
    /// For each unique index on this table, checks that no positive-weight row
    /// in the batch introduces a duplicate index key.
    ///
    /// For unique_pk tables, UPSERT rows (PK already exists) get special
    /// handling: the old index entry will be retracted by enforce_unique_pk,
    /// so we only reject if the NEW value collides with a DIFFERENT row's entry.
    pub(crate) fn validate_unique_indices(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;

        // Quick check: any unique index circuits?
        let has_unique = entry.index_circuits.iter().any(|ic| ic.is_unique);
        if !has_unique { return Ok(()); }

        let schema = entry.schema;
        let src_pk_stride = schema.pk_stride() as usize;

        for ic in &entry.index_circuits {
            if !ic.is_unique { continue; }
            let source_col_idx = ic.col_idx as usize;
            let col_type = schema.columns[source_col_idx].type_code;
            let payload_col = schema.payload_idx(source_col_idx);
            let col_size = schema.columns[source_col_idx].size() as usize;

            let idx_key_size = ic.index_schema.columns[0].size() as usize;
            let prefix_len = col_size.min(idx_key_size);
            let take = src_pk_stride.min(16);
            let idx_table = ic.table_mut();
            let mut cursor = idx_table.create_cursor_no_compact();

            // Promoted u128 key uniquely identifies the indexed value for
            // every type we accept (promote_to_index_key widens to ≤16 bytes).
            let mut seen: HashSet<u128> = HashSet::with_capacity(batch.count);

            for row in 0..batch.count {
                if batch.get_weight(row) <= 0 { continue; }

                let null_word = batch.get_null_word(row);
                if null_word & (1u64 << payload_col) != 0 { continue; }

                let row_pk = batch.get_pk(row);
                let is_upsert = entry.unique_pk && entry.handle.has_pk(row_pk);

                let col_data = batch.col_data(payload_col);
                let key_u128 = promote_to_index_key(col_data, row * col_size, col_size, col_type);

                if !seen.insert(key_u128) {
                    let col_names = self.get_column_names(table_id);
                    let cname = col_names.get(source_col_idx).map(|s| s.as_str()).unwrap_or("?");
                    return Err(format!(
                        "Unique index violation on column '{}': duplicate in batch", cname
                    ));
                }

                // Index PK layout: indexed-key field (col_size LE bytes,
                // zero-padded to idx_key_size) followed by the source PK bytes.
                let key_bytes = key_u128.to_le_bytes();
                let prefix = &key_bytes[..prefix_len];
                if !cursor.cursor.seek_first_positive_with_prefix(prefix) { continue; }

                let pk_bytes = cursor.cursor.current_pk_bytes();
                let mut src_pk_buf = [0u8; 16];
                src_pk_buf[..take].copy_from_slice(
                    &pk_bytes[idx_key_size..idx_key_size + take]
                );
                let existing_src_pk = u128::from_le_bytes(src_pk_buf);

                // For UPSERT: the existing entry may be the same row whose
                // index entry will be retracted by enforce_unique_pk.
                if is_upsert && existing_src_pk == row_pk { continue; }

                let col_names = self.get_column_names(table_id);
                let cname = col_names.get(source_col_idx).map(|s| s.as_str()).unwrap_or("?");
                return Err(format!(
                    "Unique index violation on column '{}'", cname
                ));
            }
        }
        Ok(())
    }

}
