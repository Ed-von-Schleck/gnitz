use super::*;

/// `<base_dir>/_relations/_preflight_<vid>` — where the master's CREATE VIEW
/// pre-flight compiles. Not the view's own directory, whose rank-0 children are
/// worker 0's; under the relation root, so the sweep reclaims one a crash leaves.
pub(in crate::catalog) fn preflight_dir(base_dir: &str, vid: i64) -> String {
    format!("{}/_preflight_{vid}", relations_dir(base_dir))
}

// ---------------------------------------------------------------------------
// Copy/retract helpers
// ---------------------------------------------------------------------------

/// The OPK image of a **single-column** native system-table PK: a U64 id. A
/// two-column key — COL_TAB's `(owner_id, col_idx)`, the circuit family's
/// `(view_id, node_id)` — has its own [`pair_opk`].
pub(in crate::catalog) fn sys_opk(schema: &SchemaDescriptor, pk: u128) -> gnitz_store::schema::key::PkBuf {
    gnitz_store::schema::key::opk_key_cols(schema, &[pk])
}

/// The OPK image of a pair-keyed system row's compound PK.
pub(in crate::catalog) fn pair_opk(
    schema: &SchemaDescriptor,
    leading: i64,
    trailing: u64,
) -> gnitz_store::schema::key::PkBuf {
    gnitz_store::schema::key::opk_key_cols(schema, &[leading as u64 as u128, trailing as u128])
}

impl CatalogEngine {
    /// Emit a weight −1 batch of every live row of pair-keyed `family` under each of
    /// `leadings` — owners' column records, or views' circuit rows.
    pub(in crate::catalog) fn retract_bands(&self, family: SysFamily, leadings: &[i64]) -> Batch {
        let rel = self.sys_relation(family);
        let mut batch = Batch::with_capacity(&rel.schema(), 0);
        for &leading in leadings {
            let (start, end) = family.band(leading);
            let (mut cursor, _) = rel.range_cursor(start.pk_bytes(), Some(end.pk_bytes()));
            cursor.for_each_positive(|c| c.copy_current_row_into(&mut batch, -1));
        }
        batch
    }

    /// Emit a weight=−1 batch of the live rows of `family` at `ids`; an id with no
    /// live row contributes nothing.
    pub(in crate::catalog) fn retract_pk_list(&self, family: SysFamily, mut ids: Vec<u128>) -> Batch {
        let rel = self.sys_relation(family);
        let schema = rel.schema();
        ids.sort_unstable();
        ids.dedup();
        let mut keys = Vec::with_capacity(ids.len() * schema.pk_stride());
        for &id in &ids {
            keys.extend_from_slice(sys_opk(&schema, id).pk_bytes());
        }
        let mut batch = Batch::with_capacity(&schema, ids.len());
        gnitz_store::storage::PkSetGather::open(keys, schema, |s, e| rel.cursor_in_range(s, e))
            .for_each_live_row(usize::MAX, |c| c.copy_current_row_into(&mut batch, -1));
        batch
    }
}
