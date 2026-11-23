//! Every read over the system stores: live rows by id or name, an owner's band of
//! rows and the retraction of one, column records, schema and relation names.

use gnitz_store::relation::{Cut, Relation};
use gnitz_trace::runs::{ReadCursor, StoredRow};
use gnitz_wire::{
    CatalogColumn, ColTabRow, PkKeys, SchemaTabRow, SchemaTabSlot, TableTabRow, payload_bytes, payload_str, payload_u64,
};
use gnitz_zset::repr::Batch;

use super::CatalogEngine;
use super::sys_tables::{SysFamily, SysFamilyExt};

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
        self.sys_relation(family).unique_live_row_at(&gnitz_wire::sys_key(id))
    }

    /// Whether `tid` is a SERIAL table, from its live TABLE_TAB row; `false` for
    /// a view or an id no table holds.
    pub(in crate::catalog) fn is_serial(&self, tid: u64) -> bool {
        self.live_sys_row(SysFamily::Table, tid).is_some_and(|row| {
            let (src, ri) = row.source();
            TableTabRow::read(src, ri).is_ok_and(|r| r.serial)
        })
    }

    /// Visit every live row of `family` whose leading key column is `leading`: the
    /// row itself for a single-column key, the owner's whole band for a pair.
    pub(in crate::catalog) fn for_each_row_under(&self, family: SysFamily, leading: u64, f: impl FnMut(&ReadCursor)) {
        let key = gnitz_wire::sys_key(leading);
        self.sys_relation(family)
            .cursor_between(&key, &key, Cut::Now)
            .for_each_positive(f)
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
        let keys = PkKeys::from_sorted(
            8,
            ids.iter().flat_map(|&id| gnitz_wire::sys_key(id)).collect::<Vec<u8>>(),
        );
        let mut batch = Batch::empty_with_schema(family.schema());
        (self.sys_relation(family).gather(keys, Cut::Now, u64::MAX))
            .for_each_positive_capped(usize::MAX, |c| c.copy_current_row_into(&mut batch, -c.current_weight()));
        batch
    }

    // -- Read column definitions from COL_TAB ------------------------------

    /// Column definitions for `owner_id`, in key order.
    pub(in crate::catalog) fn read_column_defs(&self, owner_id: u64) -> Result<Vec<CatalogColumn>, String> {
        let mut defs = Ok(Vec::new());
        self.for_each_row_under(SysFamily::Column, owner_id, |c| {
            let Ok(acc) = &mut defs else { return };
            let (src, row) = c.current_row_source();
            if let Err(e) = ColTabRow::read(src, row)
                .and_then(|r| r.append_to(acc).map_err(|e| format!("column {}: {e}", r.col_idx)))
            {
                defs = Err(format!("entity (owner_id={owner_id}): {e}"));
            }
        });
        defs
    }

    // -- Names ---------------------------------------------------------------

    /// The id of schema `name`, from the live SCHEMA_TAB rows.
    pub(crate) fn schema_id(&self, name: &str) -> Option<u64> {
        let scan = self.sys_relation(SysFamily::Schema).full_scan();
        (0..scan.len())
            .find(|&i| payload_bytes(&*scan, i, SchemaTabSlot::name as usize) == name.as_bytes())
            .map(|i| gnitz_wire::sys_leading_id(scan.get_pk_bytes(i)))
    }

    /// The name of schema `sid`, from its live SCHEMA_TAB row.
    pub(in crate::catalog) fn schema_name(&self, sid: u64) -> Option<String> {
        let row = self.live_sys_row(SysFamily::Schema, sid)?;
        let (src, ri) = row.source();
        let stored = SchemaTabRow::read(src, ri).expect("a stored schema name passed the precheck");
        Some(stored.name.into_owned())
    }

    /// The qualified `(schema, name)` of `table_id` from its live TABLE_TAB or
    /// VIEW_TAB row; `"?"` for a part the catalog has no entry for.
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
        (schema, name.into_owned())
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
            .map(|&ci| defs.get(ci as usize).map_or("?", |d| &*d.def.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `table_id`'s column scales at `col_indices`: a DECIMAL's, else `0`, and
    /// `0` for one the catalog does not hold.
    pub(crate) fn column_scales(&self, table_id: u64, col_indices: &[u32]) -> Vec<u8> {
        let defs = self.read_column_defs(table_id).unwrap_or_default();
        col_indices
            .iter()
            .map(|&ci| defs.get(ci as usize).map_or(0, |d| d.def.ty.scale))
            .collect()
    }
}
