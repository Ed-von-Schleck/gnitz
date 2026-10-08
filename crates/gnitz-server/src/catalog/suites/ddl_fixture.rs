//! Test-only in-process DDL: the direct `create_table` / `drop_view` /
//! `create_index` … entry points the catalog tests drive the applier with.
//!
//! **Not** a production code path. Every real DDL statement (SQL planner,
//! C-API, `gnitz-py`) is built client-side and pushed over the wire as a
//! `DDL_TXN` bundle of system-table deltas, which the executor applies
//! through `apply_bundle`. These wrappers build such a bundle and apply it the
//! same way, without a server.

use super::super::*;
use crate::test_support::{apply_ddl, col_tab_batch, idx_tab_batch, schema_tab_batch};
use gnitz_wire::payload_u64;
use gnitz_wire::sys_rows::{IdxTabSlot, SysRow, TableTabRow};
use gnitz_wire::validate_user_identifier;
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::BatchBuilder;

/// Split `schema.name`, defaulting the schema half. Only these direct entry
/// points take qualified-name strings; the wire path ships schema and entity ids
/// separately.
pub(super) fn parse_qualified_name<'a>(name: &'a str, default_schema: &'a str) -> (&'a str, &'a str) {
    match name.find('.') {
        Some(dot) => (&name[..dot], &name[dot + 1..]),
        None => (default_schema, name),
    }
}

/// Production index names arrive pre-built over the wire; only `create_index`
/// below names one engine-side.
pub(super) fn make_secondary_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{schema_name}__{table_name}__idx_{col_name}")
}

impl CatalogEngine {
    pub(in crate::catalog) fn schema_is_empty(&self, schema_name: &str) -> bool {
        self.schema_id(schema_name)
            .is_none_or(|sid| !self.caches.relation_by_name.contains_key(&sid))
    }

    pub(in crate::catalog) fn get_by_name(&self, schema_name: &str, table_name: &str) -> Option<u64> {
        self.relation_id(self.schema_id(schema_name)?, table_name)
    }

    /// The live rows of relation family `family` (Table or View) in schema `sid`.
    fn schema_members(&self, family: SysFamily, sid: u64) -> Batch {
        self.sys_rows_where(family, |s, i| {
            payload_u64(s, i, gnitz_wire::RELTAB_PAY_SCHEMA_ID) == sid
        })
    }

    /// The id of the live index named `name`.
    fn index_id_by_name(&self, name: &str) -> Option<u64> {
        let rows = self.sys_rows_where(SysFamily::Index, |s, i| {
            gnitz_wire::payload_bytes(s, i, IdxTabSlot::name as usize) == name.as_bytes()
        });
        (!rows.is_empty()).then(|| rows.get_pk(0) as u64)
    }

    pub(in crate::catalog) fn has_index_by_name(&self, name: &str) -> bool {
        self.index_id_by_name(name).is_some()
    }

    /// The ids of the live `sys_indices` rows `owner` owns.
    pub(super) fn index_ids_of(&self, owner: u64) -> Vec<u64> {
        let rows = self.sys_rows_where(SysFamily::Index, |s, i| {
            payload_u64(s, i, IdxTabSlot::owner_id as usize) == owner
        });
        (0..rows.len()).map(|i| rows.get_pk(i) as u64).collect()
    }

    /// Retract the live row at `id` in `family` as a one-block bundle, whose apply
    /// expands the drop cascade.
    pub(super) fn submit_retraction(&mut self, family: SysFamily, id: u64) -> Result<(), String> {
        let batch = self.retract_under(family, &[id]);
        if batch.is_empty() {
            return Err("Entity does not exist in catalog".into());
        }
        apply_ddl(self, [(family, batch)])
    }

    // -- DDL: CREATE/DROP SCHEMA -------------------------------------------

    pub(in crate::catalog) fn create_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        if self.schema_id(name).is_some() {
            return Err(format!("Schema already exists: {name}"));
        }
        let sid = self.allocate_ids(1).unwrap();

        apply_ddl(self, [(SysFamily::Schema, schema_tab_batch(&[(sid, name, 1)]))])
    }

    /// Drop a schema with every table and view in it, as the one all-negative
    /// bundle the client's `drop_schema` commits. The engine has no
    /// `DROP SCHEMA CASCADE`: `precheck_schema_family` rejects a non-empty drop,
    /// and the bundle's reverse apply order retires the members first.
    pub(in crate::catalog) fn drop_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        let sid = self.schema_id(name).ok_or("Schema does not exist")?;
        let mut blocks = vec![(SysFamily::Schema, self.retract_under(SysFamily::Schema, &[sid]))];
        for family in [SysFamily::View, SysFamily::Table] {
            let members = self.schema_members(family, sid);
            if !members.is_empty() {
                blocks.push((family, members.negated()));
            }
        }
        apply_ddl(self, blocks)
    }

    // -- DDL: CREATE/DROP TABLE --------------------------------------------

    /// Build a table directly in the catalog: allocate the id, then apply its
    /// COL_TAB records and TABLE_TAB `+1` as one bundle.
    pub(crate) fn create_table(
        &mut self,
        qualified_name: &str,
        col_defs: &[CatalogColumn],
        pk_cols: &[u32],
    ) -> Result<u64, String> {
        self.create_table_with(qualified_name, col_defs, pk_cols, gnitz_wire::TableProps::default())
    }

    /// A SERIAL table `(id U64 PRIMARY KEY)`.
    pub(crate) fn create_serial_table(&mut self, qualified_name: &str) -> Result<u64, String> {
        let serial = gnitz_wire::TableProps { serial: true, ..Default::default() };
        self.create_table_with(
            qualified_name,
            &[crate::test_support::col_def("id", gnitz_wire::TypeCode::U64)],
            &[0],
            serial,
        )
    }

    /// [`Self::create_table`] under `props`.
    pub(crate) fn create_table_with(
        &mut self,
        qualified_name: &str,
        col_defs: &[CatalogColumn],
        pk_cols: &[u32],
        props: gnitz_wire::TableProps,
    ) -> Result<u64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");

        // Only what the bundle cannot derive for itself: the schema id, and an id
        // for the new table. Every rule this shape must satisfy is the
        // production precheck's — restating one here would let a test asserting
        // that rejection pass against this copy while the production arm was
        // broken.
        let sid = self
            .schema_id(schema_name)
            .ok_or_else(|| format!("Schema does not exist: {schema_name}"))?;
        let tid = self.allocate_ids(1).unwrap();

        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        let row = TableTabRow {
            table_id: tid,
            schema_id: sid,
            name: table_name,
            pk_col_idx: gnitz_wire::PkColList::from_slice(pk_cols).pack(),
            flags: props.pack(),
        };
        row.write(&mut bb, 1);
        let blocks = [
            (SysFamily::Column, col_tab_batch(tid, col_defs, 1)),
            (SysFamily::Table, bb.finish()),
        ];
        apply_ddl(self, blocks)?;
        Ok(tid)
    }

    pub(in crate::catalog) fn drop_table(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");
        validate_user_identifier(schema_name)?;
        validate_user_identifier(table_name)?;

        let tid = self
            .get_by_name(schema_name, table_name)
            .ok_or_else(|| format!("Table does not exist: {schema_name}.{table_name}"))?;

        self.submit_retraction(SysFamily::Table, tid)
    }

    // -- DDL: CREATE/DROP VIEW ---------------------------------------------

    pub(in crate::catalog) fn drop_view(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, view_name) = parse_qualified_name(qualified_name, "public");
        let vid = self
            .get_by_name(schema_name, view_name)
            .ok_or_else(|| format!("View does not exist: {schema_name}.{view_name}"))?;

        self.submit_retraction(SysFamily::View, vid)
    }

    // -- DDL: CREATE/DROP INDEX --------------------------------------------

    pub(crate) fn create_index(
        &mut self,
        qualified_owner: &str,
        col_names: &[&str],
        is_unique: bool,
    ) -> Result<u64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_owner, "public");
        let owner_id = self
            .get_by_name(schema_name, table_name)
            .ok_or_else(|| format!("Table does not exist: {schema_name}.{table_name}"))?;

        // Resolve each column name to its index, in declared order.
        let col_defs = self.read_column_defs(owner_id).unwrap();
        let col_indices: Vec<u32> = col_names
            .iter()
            .map(|name| {
                col_defs
                    .iter()
                    .position(|cd| cd.def.name == *name)
                    .map(|p| p as u32)
                    .ok_or_else(|| format!("Column not found in owner: {name}"))
            })
            .collect::<Result<_, _>>()?;

        let index_name = make_secondary_index_name(schema_name, table_name, &col_names.join("_"));
        let index_id = self.allocate_ids(1).unwrap();

        let batch = idx_tab_batch(index_id, owner_id, &col_indices, &index_name, is_unique, 1);
        apply_ddl(self, [(SysFamily::Index, batch)]).map(|()| index_id)
    }

    pub(crate) fn drop_index(&mut self, index_name: &str) -> Result<(), String> {
        let idx_id = self
            .index_id_by_name(index_name)
            .ok_or_else(|| format!("Index does not exist: {index_name}"))?;

        // precheck_family enforces the FK-target uniqueness guard on the -1;
        // hook_index_register releases the index's claim on its circuit.
        self.submit_retraction(SysFamily::Index, idx_id)
    }

    // -- The worker's path ---------------------------------------------------

    /// Register a base table at a caller-chosen `tid` the way a worker does: the
    /// COL_TAB and TABLE_TAB rows a DDL bundle carries, each through `ddl_sync`,
    /// so the register hooks fire and the relation store is built. `pk` names PK
    /// column indices into `cols`, in key order.
    pub(crate) fn register_table(
        &mut self,
        tid: u64,
        schema_id: u64,
        name: &str,
        cols: &[CatalogColumn],
        pk: &[u32],
    ) -> Result<(), String> {
        let col_batch = col_tab_batch(tid, cols, 1);
        self.ddl_sync(SysFamily::Column.id(), col_batch)?;

        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        let row = TableTabRow {
            table_id: tid,
            schema_id,
            name,
            pk_col_idx: gnitz_wire::PkColList::from_slice(pk).pack(),
            flags: gnitz_wire::TableProps::default().pack(),
        };
        row.write(&mut bb, 1);
        self.ddl_sync(SysFamily::Table.id(), bb.finish())
    }
}
