use super::*;

impl CatalogEngine {
    // -- FK column validation (pre-create) ---------------------------------

    pub(crate) fn validate_fk_column(
        &self,
        col: &ColumnDef,
        self_table_id: i64,
        self_pk: &[u32],
        self_pk_type: u8,
    ) -> Result<(), String> {
        if col.fk_table_id == 0 {
            return Ok(());
        }

        // `col.fk_col_idx` here is the PARENT's referenced column index (the
        // planner sets the child column's fk_col_idx to it). The target is a
        // legal reference iff it is the parent's lone PK column, or it carries
        // its own UNIQUE index. Mirrors the production planner gate.
        let target_type = if col.fk_table_id == self_table_id {
            // Self-referential FK: the table has no UNIQUE index yet, so the
            // target must be its lone PK column. The downstream probe reads the
            // referenced value out of the packed PK region, which is the whole
            // key only when the PK is a single column.
            if self_pk != [col.fk_col_idx] {
                return Err("FK must reference the primary key or a UNIQUE-indexed column".into());
            }
            self_pk_type
        } else {
            let entry = self
                .dag
                .tables
                .get(&col.fk_table_id)
                .ok_or_else(|| format!("FK references unknown table_id {}", col.fk_table_id))?;
            let pk = entry.schema.pk_indices();
            let is_lone_pk = pk.len() == 1 && pk[0] == col.fk_col_idx;
            if !is_lone_pk {
                // A composite index does not satisfy a single-column FK: a
                // unique (a, b) does not guarantee uniqueness of `a` alone, so
                // match only a single-column unique index on the referenced col.
                let has_unique = entry
                    .index_circuits
                    .iter()
                    .any(|ic| ic.unique_cols() == Some(&[col.fk_col_idx][..]));
                if !has_unique {
                    return Err("FK must reference the primary key or a UNIQUE-indexed column".into());
                }
            }
            entry.schema.columns[col.fk_col_idx as usize].type_code
        };

        // Promote BOTH sides before comparing. `get_index_key_type` maps each
        // ≤8-byte int to its index-key code (signed I8..I64 → I64, unsigned
        // U8..U64 → U64) and is idempotent on the already-promoted widths, so an
        // I64 child column referencing an I64 parent column compares equal — as
        // does the U64-vs-U64 case. Comparing the promoted child against the
        // parent's raw type_code would wrongly reject identical-type FKs once a
        // narrower signed column promotes to I64.
        let promoted = gnitz_wire::index_key_type(col.type_code)?;
        let target_promoted = gnitz_wire::index_key_type(target_type)?;
        if promoted != target_promoted {
            return Err(format!(
                "FK type mismatch: promoted code {promoted} vs target {target_promoted}"
            ));
        }
        Ok(())
    }

    // -- FK inline validation (single-worker) ------------------------------

    /// Single-worker inline FK check, exercised only by the catalog tests;
    /// production FK validation runs distributed on the wire path.
    #[cfg(test)]
    pub(crate) fn validate_fk_inline(&self, table_id: i64, batch: &Batch) -> Result<(), String> {
        let constraints = match self.caches.fk_by_child.get(&table_id) {
            Some(c) if !c.is_empty() => c,
            _ => return Ok(()),
        };

        let entry = self.table_entry(table_id)?;
        let schema = entry.schema;
        let mb = batch.as_mem_batch();

        for constraint in constraints {
            let col_idx = constraint.fk_col_idx;
            let target_id = constraint.target_table_id;
            let target_col_idx = constraint.target_col_idx;

            let target_entry = self
                .dag
                .tables
                .get(&target_id)
                .ok_or_else(|| format!("FK target table {target_id} not found"))?;

            // Probe the parent PK when the referenced column is the lone PK;
            // otherwise seek the parent's UNIQUE index circuit for the column.
            let tpk = target_entry.schema.pk_indices();
            let is_lone_pk = tpk.len() == 1 && tpk[0] as usize == target_col_idx;
            let idx_ic = if is_lone_pk {
                None
            } else {
                // A composite index does not satisfy a single-column FK:
                // match a single-column unique circuit exactly.
                Some(
                    target_entry
                        .index_circuits
                        .iter()
                        .find(|ic| ic.unique_cols() == Some(&[target_col_idx as u32][..]))
                        .ok_or_else(|| format!("FK target {target_id} col {target_col_idx} has no UNIQUE index"))?,
                )
            };
            let idx_key_size = idx_ic.map(|ic| ic.index_schema.columns[0].size() as usize);
            let idx_key_type = idx_ic.map(|ic| ic.index_schema.columns[0].type_code);

            // The child FK column may itself be a PK column, so resolve the
            // PK-vs-payload read once per constraint.
            let loc = schema.locate(col_idx);

            // Open the parent's UNIQUE-index cursor once per constraint and reuse
            // it across rows (the non-lone-PK arm). A fresh open_cursor() per row
            // allocates a loser-tree heap; a reused cursor re-seeks correctly.
            // The lone-PK arm probes has_pk and needs no cursor.
            let mut idx_cursor = idx_ic.map(|ic| ic.table_mut().open_cursor());

            for row in 0..batch.count {
                if batch.get_weight(row) <= 0 {
                    continue;
                }
                if loc.is_null(&mb, row) {
                    continue;
                } // PK never null; payload checks its bit
                let fk_key = loc.native_key(&mb, row);

                let found = if is_lone_pk {
                    target_entry.handle.has_pk(fk_key)
                } else {
                    let ks = idx_key_size.unwrap();
                    // OPK-encode the native FK value (sign-extending from the
                    // child column's width) into the leading index key column;
                    // the index PK is OPK-at-rest, so prefix-match the whole
                    // leading column (idx_key_size), not a source-width LE
                    // prefix.
                    let opk = crate::schema::key::index_opk_prefix(fk_key, loc.type_code(), idx_key_type.unwrap());
                    idx_cursor
                        .as_mut()
                        .unwrap()
                        .seek_first_positive_with_prefix(opk.padded(ks))
                };
                if !found {
                    return Err(self.fk_missing_err(table_id, target_id));
                }
            }
        }
        Ok(())
    }
}
