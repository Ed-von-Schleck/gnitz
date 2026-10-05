//! Every read over the system stores: live rows by id or name, an owner's band of
//! rows and the retraction of one, column records, schema and relation names.

use gnitz_store::relation::Relation;
use gnitz_wire::sys_rows::{ColTabRow, SchemaTabRow, SchemaTabSlot};
use gnitz_wire::{payload_bytes, payload_str, payload_u64};
use gnitz_zset::repr::{Batch, ReadCursor, StoredRow};

use super::sys_tables::{CatalogColumn, SysFamily};
use super::CatalogEngine;

/// A set of catalog ids, sorted once so every probe is a binary search: the
/// probes run per row of a client-supplied block bounded only by the frame.
#[derive(Debug, Default)]
pub(in crate::catalog) struct IdSet(Vec<u64>);

impl IdSet {
    pub(in crate::catalog) fn new(ids: impl IntoIterator<Item = u64>) -> Self {
        let mut ids: Vec<u64> = ids.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        IdSet(ids)
    }

    pub(in crate::catalog) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(in crate::catalog) fn contains(&self, id: u64) -> bool {
        self.0.binary_search(&id).is_ok()
    }

    /// The ids, strictly ascending.
    pub(in crate::catalog) fn ids(&self) -> &[u64] {
        &self.0
    }

    /// These ids and `more`.
    pub(in crate::catalog) fn with(self, more: impl IntoIterator<Item = u64>) -> Self {
        IdSet::new(self.0.into_iter().chain(more))
    }
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

    /// The negation of every live row of `family` whose leading key column is one of
    /// `ids` (strictly ascending): each row for a single-column key, each owner's band
    /// for a pair.
    pub(in crate::catalog) fn retract_under(&self, family: SysFamily, ids: &[u64]) -> Batch {
        debug_assert!(ids.windows(2).all(|w| w[0] < w[1]));
        let mut batch = Batch::empty_with_schema(family.schema());
        for &id in ids {
            self.for_each_row_under(family, id, |c| c.copy_current_row_into(&mut batch, -c.current_weight));
        }
        batch
    }

    // -- Read column definitions from sys_columns --------------------------

    /// Column definitions for `owner_id`, in key order. Its records must be keyed
    /// 0,1,2,… with no gap or duplicate: every consumer maps columns positionally.
    pub(in crate::catalog) fn read_column_defs(&self, owner_id: u64) -> Result<Vec<CatalogColumn>, String> {
        let mut defs = Vec::new();
        let mut err = None;
        self.for_each_row_under(SysFamily::Column, owner_id, |c| {
            if err.is_some() {
                return;
            }
            let (src, row) = c.current_row_source();
            let expected = defs.len() as u64;
            let def = ColTabRow::read(src, row).and_then(|r| {
                if r.col_idx != expected {
                    return Err(format!(
                        "column records are non-contiguous; expected index {expected}, got {}",
                        r.col_idx
                    ));
                }
                CatalogColumn::from_row(&r).map_err(|e| format!("column {}: {e}", r.col_idx))
            });
            match def {
                Ok(d) => defs.push(d),
                Err(e) => err = Some(format!("entity (owner_id={owner_id}): {e}")),
            }
        });
        err.map_or(Ok(defs), Err)
    }

    // -- Names ---------------------------------------------------------------

    /// The id of schema `name`, from the live SCHEMA_TAB rows.
    pub(crate) fn schema_id(&self, name: &str) -> Option<u64> {
        let scan = self.sys_relation(SysFamily::Schema).full_scan();
        (0..scan.len())
            .find(|&i| payload_bytes(&*scan, i, SchemaTabSlot::name as usize) == name.as_bytes())
            .map(|i| scan.get_pk(i) as u64)
    }

    /// The id of relation `name` in schema `schema_id`.
    pub(crate) fn relation_id(&self, schema_id: u64, name: &str) -> Option<u64> {
        self.caches.relation_by_name.get(&schema_id)?.get(name).copied()
    }

    /// The name of schema `sid`, from its live SCHEMA_TAB row.
    pub(in crate::catalog) fn schema_name(&self, sid: u64) -> Option<String> {
        let row = self.live_sys_row(SysFamily::Schema, sid)?;
        let (src, ri) = row.source();
        let stored = SchemaTabRow::read(src, ri).expect("a stored schema name passed the precheck");
        Some(stored.name.to_owned())
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
        let schema = self.schema_name(sid).unwrap_or_else(|| "?".into());
        let name =
            payload_str(src, ri, gnitz_wire::RELTAB_PAY_NAME).expect("a stored relation name passed the precheck");
        (schema, name.to_owned())
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
            .map(|&ci| defs.get(ci as usize).map_or("?", |d| d.def.name.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    }
}
