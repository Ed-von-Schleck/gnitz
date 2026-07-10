use super::*;
use crate::storage::PkBuf;
use rustc_hash::{FxHashMap, FxHashSet};

impl CatalogEngine {
    // -- FK column validation (pre-create) ---------------------------------

    pub(crate) fn validate_fk_column(
        &self,
        col: &ColumnDef,
        self_table_id: i64,
        self_pk_index: u32,
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
            // Self-referential FK: only single-PK self-reference is supported.
            if col.fk_col_idx != self_pk_index {
                return Err("FK must reference target PK".into());
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

        let entry = self
            .dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))?;
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
            // PK-vs-payload read once per constraint. Mirrors the distributed FK
            // gate in master.rs.
            let loc = schema.locate(col_idx);

            // Open the parent's UNIQUE-index cursor once per constraint and reuse
            // it across rows (the non-lone-PK arm). A fresh open_cursor() per row
            // allocates a loser-tree heap; a reused cursor re-seeks correctly,
            // exactly as validate_unique_indices does. The lone-PK arm probes
            // has_pk and needs no cursor.
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
                    let opk = crate::schema::index_opk_prefix(fk_key, loc.type_code(), idx_key_type.unwrap());
                    idx_cursor.as_mut().unwrap().seek_first_positive_with_prefix(&opk[..ks])
                };
                if !found {
                    let (sn, tn) = self.caches.entity_by_id.get(&table_id).cloned().unwrap_or_default();
                    let (tsn, ttn) = self.caches.entity_by_id.get(&target_id).cloned().unwrap_or_default();
                    return Err(format!(
                        "Foreign Key violation in '{sn}.{tn}': value not found in target '{tsn}.{ttn}'"
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
        let entry = self
            .dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))?;

        // Quick check: any unique index circuits?
        let has_unique = entry.index_circuits.iter().any(|ic| ic.is_unique);
        if !has_unique {
            return Ok(());
        }

        let schema = entry.schema;
        let unique_pk = entry.unique_pk();
        let src_pk_stride = schema.pk_stride() as usize;
        // Borrows `batch` (the `&Batch` param), independent of the `&mut self`
        // cache reads below.
        let mb = batch.as_mem_batch();

        // Any retraction in the batch? Computed once: the per-index `retracted`
        // set is populated only when a retraction exists, so insert-only batches
        // pay nothing.
        let has_retractions = (0..batch.count).any(|r| batch.get_weight(r) < 0);

        // PKs the batch upserts: net-positive aggregate weight. On a unique_pk
        // table enforce_unique_pk retracts such a PK's committed row (and its old
        // unique value) at apply, so a committed holder that is itself an upserted
        // PK frees its value — what makes a bulk shift like `UPDATE t SET u = u + 1`
        // valid. The net-positive rule (not "has any +1 row") matches the
        // distributed path's `existing_pks`, so the two validators agree even on
        // an unconsolidated batch that carries both a +1 and a -1 for one PK.
        // Empty on non-unique_pk tables (no enforce_unique_pk), so the implicit
        // exemption never fires there.
        //
        // Insert-only batches (the hot path) carry no `-1`, so every positive row
        // is already net-positive — skip the aggregation map entirely and collect
        // PKs directly; only a mixed-sign batch needs the net pass.
        let mut upserted_pks: FxHashSet<PkBuf> = FxHashSet::default();
        if unique_pk {
            if has_retractions {
                let mut net: FxHashMap<PkBuf, i64> =
                    FxHashMap::with_capacity_and_hasher(batch.count, Default::default());
                for r in 0..batch.count {
                    let w = batch.get_weight(r);
                    if w == 0 {
                        continue;
                    }
                    *net.entry(PkBuf::from_bytes(batch.get_pk_bytes(r))).or_insert(0) += w;
                }
                upserted_pks = net.into_iter().filter(|&(_, w)| w > 0).map(|(pk, _)| pk).collect();
            } else {
                upserted_pks.reserve(batch.count);
                for r in 0..batch.count {
                    if batch.get_weight(r) > 0 {
                        upserted_pks.insert(PkBuf::from_bytes(batch.get_pk_bytes(r)));
                    }
                }
            }
        }

        // (holder source PK, leading-key span) pairs retracted in this batch
        // over the current index's columns. Allocation reused across indices via
        // `clear` (the key is index-specific, so the *contents* are rebuilt per
        // index). The span replaces the old single-`u128` value so a composite
        // `UNIQUE (a, b, …)` whose span exceeds 16 bytes is keyed without
        // truncation.
        let mut retracted: FxHashSet<(PkBuf, PkBuf)> = FxHashSet::default();

        // Reused across indices like `retracted`; cleared per index. The OPK
        // leading-key span identifies the indexed value for every accepted type
        // at any width (byte-equal ⟺ value-equal). Scratch `keybuf` is the
        // reused destination `IndexKeySpec::key_bytes` writes each row's span
        // into — no per-row allocation.
        let mut seen: FxHashSet<PkBuf> = FxHashSet::with_capacity_and_hasher(batch.count, Default::default());
        let mut keybuf = PkBuf::empty(0);

        for ic in &entry.index_circuits {
            let Some(unique_slice) = ic.unique_cols() else { continue };
            seen.clear();
            // Own the column list (`PkColList` is Copy) so it outlives the
            // `&entry` borrow when passed to the `&mut self` error formatters
            // on the return paths below.
            let col_list = gnitz_wire::PkColList::from_slice(unique_slice);
            let cols = col_list.as_slice();

            // Per-circuit read/encode plan, precomputed at registration. A
            // unique index may be on a PK column (a member of a compound PK),
            // whose value lives in the packed PK, not a payload slot — the
            // spec's locators resolve that.
            let spec = ic.key_spec;
            let idx_key_size = spec.key_size();
            let idx_table = ic.table_mut();
            let mut cursor = idx_table.open_cursor();

            // A batch may atomically move a unique value between PKs (transfer)
            // or swap two values; the committed index still holds the old entry
            // (validation is pre-apply), so a collision against a value retracted
            // *by its current holder* is transient. Pairing on the holder PK —
            // not the value alone — stops a forged `P_other/v@-1` naming a
            // non-holder from exempting a real `P2/v@+1`. A row with a NULL in
            // ANY indexed column is not indexed (`key_bytes` → false).
            retracted.clear();
            if has_retractions {
                for row in 0..batch.count {
                    if batch.get_weight(row) >= 0 {
                        continue;
                    }
                    if !spec.key_bytes(&mb, row, &mut keybuf) {
                        continue;
                    }
                    retracted.insert((PkBuf::from_bytes(batch.get_pk_bytes(row)), keybuf));
                }
            }

            for row in 0..batch.count {
                let w = batch.get_weight(row);
                if w <= 0 {
                    continue;
                }
                // PK columns are non-nullable; a row NULL in any indexed column
                // is skipped (NULL-distinct).
                if !spec.key_bytes(&mb, row, &mut keybuf) {
                    continue;
                }

                // One row at weight w is the value w times. On a non-unique_pk
                // table that is w live instances (enforce_unique_pk collapses
                // it to one on unique_pk tables) — the same violation as w
                // separate +1 rows, which `seen` below rejects.
                if !unique_pk && w > 1 {
                    return Err(self.unique_violation_err(table_id, cols, true));
                }

                // UPSERT iff the row's PK is a net-positive PK in this batch AND
                // already has a live base-table row. The net-positive gate (not
                // bare committedness) matches the distributed `existing_pks`: a PK
                // carrying both a +1 and a -1 (net ≤ 0) has an order-dependent
                // surviving state under enforce_unique_pk, so both validators
                // decline to treat it as an upsert. `&&` short-circuits, so the
                // membership test runs before the committed probe and a
                // non-upserted PK pays no seek.
                //
                // Both PK widths probe via `has_pk_bytes` (verbatim OPK bytes —
                // never `get_pk`, which is OPK-widened and would double-encode a
                // signed/compound PK, missing the existing row). This routes the
                // last PK probe through the same distribution-aware
                // `local_index_bytes` funnel as ingest and retraction — mandatory
                // under prefix distribution, where the wide cursor's
                // all-owned-partitions merge would otherwise probe a partition the
                // row was never routed to — and replaces that merge tree with a
                // per-row single-partition bloom + XOR8-gated shard scan.
                let is_upsert = unique_pk
                    && upserted_pks.contains(batch.get_pk_bytes(row))
                    && entry.handle.has_pk_bytes(batch.get_pk_bytes(row));

                if !seen.insert(keybuf) {
                    return Err(self.unique_violation_err(table_id, cols, true));
                }

                // Index PK layout: leading indexed-key columns (OPK-encoded,
                // idx_key_size bytes total) followed by the full source PK bytes —
                // always idx_key_size + src_pk_stride wide. `keybuf` already holds
                // the OPK composite span; prefix-match it whole.
                if !cursor.seek_first_positive_with_prefix(keybuf.pk_bytes()) {
                    continue;
                }
                let pk_bytes = cursor.current_pk_bytes();

                // The committed holder's full source PK. Slice by raw bytes; at
                // any width truncating to 16 bytes would misread two wide PKs
                // sharing a 16-byte prefix as the same row.
                debug_assert!(pk_bytes.len() >= idx_key_size + src_pk_stride);
                let existing_src_pk = &pk_bytes[idx_key_size..idx_key_size + src_pk_stride];

                // `seen` already barred two live rows sharing this value. Exempt
                // the committed collision only when the holder releases the value
                // in this batch: it explicitly retracts (PK, value) here, or — on
                // a unique_pk table and only when this row is itself an upsert —
                // the holder is also an upserted PK, so enforce_unique_pk frees
                // the value at apply. The `upserted_pks` membership test subsumes
                // the same-PK upsert case (the row's own PK is a positive PK).
                if retracted.contains(&(PkBuf::from_bytes(existing_src_pk), keybuf)) {
                    continue;
                }
                if is_upsert && upserted_pks.contains(existing_src_pk) {
                    continue;
                }

                return Err(self.unique_violation_err(table_id, cols, false));
            }
        }
        Ok(())
    }
}
