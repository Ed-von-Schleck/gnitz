use super::*;
use crate::schema::{pk_native_key, payload_native_key};

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

        // `col.fk_col_idx` here is the PARENT's referenced column index (the
        // planner sets the child column's fk_col_idx to it). The target is a
        // legal reference iff it is the parent's lone PK column, or it carries
        // its own UNIQUE index. Mirrors the production planner gate.
        let target_type = if col.fk_table_id == self_table_id {
            // Self-referential FK: only single-PK self-reference is supported.
            if col.fk_col_idx != self_pk_index {
                return Err("FK must reference target PK".into());
            }
            self_pk_type
        } else {
            let entry = self.dag.tables.get(&col.fk_table_id)
                .ok_or_else(|| format!("FK references unknown table_id {}", col.fk_table_id))?;
            let pk = entry.schema.pk_indices();
            let is_lone_pk = pk.len() == 1 && pk[0] == col.fk_col_idx;
            if !is_lone_pk {
                let has_unique = entry.index_circuits.iter()
                    .any(|ic| ic.col_idx == col.fk_col_idx && ic.is_unique);
                if !has_unique {
                    return Err(
                        "FK must reference the primary key or a UNIQUE-indexed column".into()
                    );
                }
            }
            entry.schema.columns[col.fk_col_idx as usize].type_code
        };

        let promoted = get_index_key_type(col.type_code)?;
        if promoted != target_type {
            return Err(format!(
                "FK type mismatch: promoted code {} vs target {}",
                promoted, target_type
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
            let target_col_idx = constraint.target_col_idx;

            let target_entry = self.dag.tables.get(&target_id)
                .ok_or_else(|| format!("FK target table {} not found", target_id))?;

            // Probe the parent PK when the referenced column is the lone PK;
            // otherwise seek the parent's UNIQUE index circuit for the column.
            let tpk = target_entry.schema.pk_indices();
            let is_lone_pk = tpk.len() == 1 && tpk[0] as usize == target_col_idx;
            let idx_ic = if is_lone_pk {
                None
            } else {
                Some(target_entry.index_circuits.iter()
                    .find(|ic| ic.col_idx as usize == target_col_idx && ic.is_unique)
                    .ok_or_else(|| format!(
                        "FK target {} col {} has no UNIQUE index", target_id, target_col_idx))?)
            };
            let idx_key_size = idx_ic.map(|ic| ic.index_schema.columns[0].size() as usize);

            for row in 0..batch.count {
                if batch.get_weight(row) <= 0 { continue; }

                let null_word = batch.get_null_word(row);
                let payload_col = schema.payload_idx(col_idx);
                if null_word & (1u64 << payload_col) != 0 { continue; }

                let col_type = schema.columns[col_idx].type_code;
                let col_size = schema.columns[col_idx].size() as usize;
                let col_data = batch.col_data(payload_col);
                let fk_key = payload_native_key(col_data, row * col_size, col_size, col_type);

                let found = if is_lone_pk {
                    target_entry.handle.has_pk(fk_key)
                } else {
                    let ic = idx_ic.unwrap();
                    let ks = idx_key_size.unwrap();
                    // OPK-encode the native FK value into the leading index key
                    // column; the index PK is OPK-at-rest, so prefix-match the
                    // whole leading column (idx_key_size), not a source-width LE
                    // prefix.
                    let opk = crate::schema::index_opk_prefix(
                        fk_key, ic.index_schema.columns[0].type_code, ks);
                    ic.table_mut().open_cursor().cursor
                        .seek_first_positive_with_prefix(&opk[..ks])
                };
                if !found {
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
    pub(crate) fn validate_fk_parent_restrict(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        // Owned snapshot so the `seek_family` (&mut self) probe below doesn't
        // alias the borrow of the children list.
        let children: Vec<FkParentRef> = self.fk_children_of(table_id).to_vec();
        if children.is_empty() { return Ok(()); }

        let parent_schema = self.dag.tables.get(&table_id)
            .map(|e| e.schema)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;

        // Removed PKs, collected once. A wide parent PK cannot be packed into a
        // u128, so it goes into a single flat buffer iterated by
        // `chunks_exact(parent_pk_stride)`; the narrow path keeps the u128 vec.
        let wide = parent_schema.pk_is_wide();
        let parent_pk_stride = parent_schema.pk_stride() as usize;
        let mut removed_narrow: Vec<u128> = Vec::new();
        let mut removed_wide: Vec<u8> = Vec::new();
        for row in 0..batch.count {
            if batch.get_weight(row) >= 0 { continue; }
            if wide {
                removed_wide.extend_from_slice(batch.get_pk_bytes(row));
            } else {
                removed_narrow.push(batch.get_pk(row));
            }
        }
        if removed_narrow.is_empty() && removed_wide.is_empty() { return Ok(()); }

        for FkParentRef { child_tid, fk_col_idx, parent_col_idx } in children {
            let col_type = parent_schema.columns[parent_col_idx].type_code;
            let col_size = parent_schema.columns[parent_col_idx].size() as usize;
            let is_pk_col = parent_schema.is_pk_col(parent_col_idx);

            // Resolve the referenced value of each removed row. A PK-column
            // target rides inside the packed PK; a non-PK UNIQUE target is not
            // on the delete wire (the payload is inert filler), so it is read
            // from the parent's committed row. A NULL value can be referenced
            // by no child, so it never blocks the delete.
            let keys: Vec<u128> = if is_pk_col {
                let off = parent_schema.pk_byte_offset(parent_col_idx) as usize;
                if wide {
                    removed_wide.chunks_exact(parent_pk_stride)
                        .map(|pk| pk_native_key(pk, off, col_size, col_type))
                        .collect()
                } else {
                    removed_narrow.iter()
                        .map(|&pk| {
                            // get_pk returned widen_pk_be(region); the low
                            // `parent_pk_stride` bytes of its BE image are the OPK
                            // region, from which pk_native_key decodes the column
                            // back to its native value.
                            let be = pk.to_be_bytes();
                            pk_native_key(&be[16 - parent_pk_stride..], off, col_size, col_type)
                        })
                        .collect()
                }
            } else {
                let payload_col = parent_schema.payload_idx(parent_col_idx);
                let removed_count = if wide {
                    removed_wide.len() / parent_pk_stride
                } else {
                    removed_narrow.len()
                };
                let mut v = Vec::with_capacity(removed_count);
                if wide {
                    for pk in removed_wide.chunks_exact(parent_pk_stride) {
                        if let Some(row) = self.seek_family_bytes(table_id, pk)? {
                            if row.get_null_word(0) & (1u64 << payload_col) != 0 { continue; }
                            v.push(payload_native_key(row.col_data(payload_col), 0, col_size, col_type));
                        }
                    }
                } else {
                    for &pk in &removed_narrow {
                        if let Some(row) = self.seek_family(table_id, pk)? {
                            if row.get_null_word(0) & (1u64 << payload_col) != 0 { continue; }
                            v.push(payload_native_key(row.col_data(payload_col), 0, col_size, col_type));
                        }
                    }
                }
                v
            };
            if keys.is_empty() { continue; }

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
            let idx_key_type = ic.index_schema.columns[0].type_code;
            let idx_table = ic.table_mut();
            let mut cursor = idx_table.open_cursor();

            for fk_key in keys {
                // Prefix-scan the child's FK index for any positive-weight entry
                // whose leading column matches the referenced value. The index PK
                // is OPK-at-rest, so OPK-encode the native value and match the
                // whole leading column.
                let opk = crate::schema::index_opk_prefix(fk_key, idx_key_type, idx_key_size);
                if cursor.cursor.seek_first_positive_with_prefix(&opk[..idx_key_size]) {
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
        let wide = schema.pk_is_wide();
        let src_pk_stride = schema.pk_stride() as usize;

        // One base-table cursor, reused across every row's UPSERT probe on the
        // wide path. The narrow `has_pk(u128)` probe is cheaper (memtable bloom
        // + shard scan, no merge), so narrow keeps it and opens no cursor.
        let mut base_cursor = if entry.unique_pk && wide {
            Some(entry.handle.open_cursor())
        } else {
            None
        };

        for ic in &entry.index_circuits {
            if !ic.is_unique { continue; }
            let source_col_idx = ic.col_idx as usize;
            let col_type = schema.columns[source_col_idx].type_code;
            let col_size = schema.columns[source_col_idx].size() as usize;
            // A unique index may be on a PK column (e.g. a single member of a
            // compound PK). Then the value lives in the packed PK, not a payload
            // column — payload_idx/col_data would panic, and a compound PK must
            // be sliced to the indexed column.
            let is_pk_col = schema.is_pk_col(source_col_idx);
            let pk_field_off = if is_pk_col {
                schema.pk_byte_offset(source_col_idx) as usize
            } else { 0 };
            let payload_col = if is_pk_col {
                usize::MAX
            } else {
                schema.payload_idx(source_col_idx)
            };

            let idx_key_size = ic.index_schema.columns[0].size() as usize;
            let idx_key_type = ic.index_schema.columns[0].type_code;
            let idx_table = ic.table_mut();
            let mut cursor = idx_table.open_cursor();

            // The canonical u128 key uniquely identifies the indexed value for
            // every type we accept (pk_native_key/payload_native_key map to ≤16 bytes).
            let mut seen: HashSet<u128> = HashSet::with_capacity(batch.count);

            for row in 0..batch.count {
                if batch.get_weight(row) <= 0 { continue; }

                // PK columns are non-nullable; only payload columns can be NULL.
                if !is_pk_col {
                    let null_word = batch.get_null_word(row);
                    if null_word & (1u64 << payload_col) != 0 { continue; }
                }

                // UPSERT iff the row's PK already has a live base-table row.
                let is_upsert = if !entry.unique_pk {
                    false
                } else if wide {
                    let row_pk_bytes = batch.get_pk_bytes(row);
                    let cur = base_cursor.as_mut().unwrap();
                    cur.cursor.seek_bytes(row_pk_bytes);
                    cur.cursor.valid
                        && cur.cursor.current_weight > 0
                        && cur.cursor.current_pk_bytes() == row_pk_bytes
                } else {
                    entry.handle.has_pk(batch.get_pk(row))
                };

                // Decode the indexed PK column OPK→native. `get_pk` is the
                // OPK-widened value, which `index_opk_prefix` below would
                // re-OPK-encode — wrong for a signed single PK (double sign flip).
                // `pk_native_key` is correct for all widths and equals `get_pk`
                // for unsigned single PKs.
                let key_u128 = if is_pk_col {
                    pk_native_key(
                        batch.get_pk_bytes(row), pk_field_off, col_size, col_type)
                } else {
                    let col_data = batch.col_data(payload_col);
                    payload_native_key(col_data, row * col_size, col_size, col_type)
                };

                if !seen.insert(key_u128) {
                    let col_names = self.get_column_names(table_id);
                    let cname = col_names.get(source_col_idx).map(|s| s.as_str()).unwrap_or("?");
                    return Err(format!(
                        "Unique index violation on column '{}': duplicate in batch", cname
                    ));
                }

                // Index PK layout: leading indexed-key column (OPK-encoded,
                // idx_key_size bytes) followed by the full source PK bytes —
                // always idx_key_size + src_pk_stride wide. OPK-encode the native
                // value and prefix-match the whole leading column.
                let opk = crate::schema::index_opk_prefix(key_u128, idx_key_type, idx_key_size);
                if !cursor.cursor.seek_first_positive_with_prefix(&opk[..idx_key_size]) {
                    continue;
                }
                let pk_bytes = cursor.cursor.current_pk_bytes();

                // Is the existing index entry the SAME source row (whose old
                // entry enforce_unique_pk retracts under an UPSERT)? Compare the
                // full source PK by raw bytes; at any width truncating to 16
                // bytes would misread two wide PKs sharing a 16-byte prefix as
                // the same row.
                debug_assert!(pk_bytes.len() >= idx_key_size + src_pk_stride);
                let existing_src_pk = &pk_bytes[idx_key_size..idx_key_size + src_pk_stride];
                let matches_existing = existing_src_pk == batch.get_pk_bytes(row);

                if is_upsert && matches_existing { continue; }

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
