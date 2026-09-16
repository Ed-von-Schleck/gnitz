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

/// The OPK image of a **single-column** native system-table PK: a U64 id.
fn sys_opk(schema: &SchemaDescriptor, pk: u128) -> gnitz_store::schema::key::PkBuf {
    gnitz_store::schema::key::opk_key_cols(schema, &[pk])
}

impl CatalogEngine {
    /// The live row of single-column-keyed `family` at `id`.
    pub(in crate::catalog) fn live_sys_row(&self, family: SysFamily, id: i64) -> Option<StoredRow> {
        let key = sys_opk(family.schema(), id as u128);
        self.sys_relation(family).live_row_at(key.pk_bytes()).1
    }

    /// Visit every live row of pair-keyed `family` under `leading` — an owner's
    /// column records, or a view's circuit rows.
    pub(in crate::catalog) fn for_each_row_under(&self, family: SysFamily, leading: i64, f: impl FnMut(&ReadCursor)) {
        self.sys_relation(family)
            .for_each_positive_with_prefix(&(leading as u64).to_be_bytes(), f)
    }

    /// Emit a weight −1 batch of every live row of pair-keyed `family` under each of
    /// `leadings`.
    pub(in crate::catalog) fn retract_bands(&self, family: SysFamily, leadings: &[i64]) -> Batch {
        let mut batch = Batch::with_capacity(family.schema(), 0);
        for &leading in leadings {
            self.for_each_row_under(family, leading, |c| c.copy_current_row_into(&mut batch, -1));
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
