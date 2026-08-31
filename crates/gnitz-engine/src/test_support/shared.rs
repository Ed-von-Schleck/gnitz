//! The catalog-level test helpers `gnitz-server`'s tests share with this
//! crate's own — the store-level ones are `gnitz-store`'s sibling file.
//!
//! **This file is compiled twice, from one source.** Here it is a `cfg(test)`
//! module of `gnitz-engine`; `gnitz-engine-testkit` compiles the same path as
//! half of its library, which is how `gnitz-server`'s tests reach it.
//!
//! Every path below is spelled `gnitz_engine::`, which resolves through
//! `extern crate self as gnitz_engine` in this crate and to the real dependency
//! in the testkit. So this compilation sees only `gnitz-engine`'s public API,
//! and a helper that reached a crate-internal fails to build here. When that
//! happens the fix is to move the helper to [`super::internal`], not to publish
//! the internal — see this module's parent for what that costs.

use gnitz_engine::catalog::{CatalogEngine, ColumnDef, SysFamily};
use gnitz_store::storage::BatchBuilder;
use gnitz_wire::sys_rows::{write_col_tab_row, write_table_tab_row, ColTabRow, TableTabRow};

// ── Catalog ColumnDef fixtures ────────────────────────────────────────────
//
// `ColumnDef: Default` is the plain column, so each builder names only what it
// varies and a new field costs no construction site anything.

/// A plain non-nullable, non-FK, non-hidden column of the given type.
pub fn col_def(name: &str, type_code: u8) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code,
        ..Default::default()
    }
}

/// Column defs carrying just the names, for a test that needs a *named* schema
/// block. Type and nullability come off the descriptor, so only `name` matters.
pub fn named_col_defs<S: AsRef<str>>(names: &[S]) -> Vec<ColumnDef> {
    names.iter().map(|n| col_def(n.as_ref(), 0)).collect()
}

// ---------------------------------------------------------------------------
// CatalogTestExt — register a relation the way a worker does
// ---------------------------------------------------------------------------

/// In-process table registration for tests that need a real relation store
/// without a server.
///
/// It writes the same COL_TAB and TABLE_TAB rows a DDL bundle carries and hands
/// each to [`CatalogEngine::ddl_sync`], which is the ingest tail the wire path
/// reaches too — so the register hooks fire and the relation store is built.
/// The columns go first: the TABLE_TAB register hook reads them back out of
/// `sys_columns` to derive the schema.
pub trait CatalogTestExt {
    /// Register a base table at a caller-chosen `tid`. `pk` names the PK column
    /// indices into `cols`, in key order.
    ///
    /// A caller-chosen id lifts the engine's allocator past itself (the register
    /// hook raises the id counter to `tid + 1`), so a later engine-side
    /// allocation cannot collide with one chosen here.
    fn register_table(
        &mut self,
        tid: i64,
        schema_id: i64,
        name: &str,
        cols: &[ColumnDef],
        pk: &[u32],
    ) -> Result<(), String>;
}

impl CatalogTestExt for CatalogEngine {
    fn register_table(
        &mut self,
        tid: i64,
        schema_id: i64,
        name: &str,
        cols: &[ColumnDef],
        pk: &[u32],
    ) -> Result<(), String> {
        let mut bb = BatchBuilder::new(SysFamily::Column.schema());
        for (i, cd) in cols.iter().enumerate() {
            write_col_tab_row(
                &mut bb,
                &ColTabRow {
                    owner_id: tid as u64,
                    owner_kind: gnitz_wire::OWNER_KIND_TABLE,
                    col_idx: i as u64,
                    name: &cd.name,
                    type_code: cd.type_code as u64,
                    is_nullable: cd.is_nullable,
                    fk_table_id: cd.fk_table_id as u64,
                    fk_col_idx: cd.fk_col_idx as u64,
                    is_serial: cd.is_serial,
                    is_hidden: cd.is_hidden,
                },
                1,
            )?;
        }
        self.ddl_sync(SysFamily::Column.id(), bb.finish())?;

        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        write_table_tab_row(
            &mut bb,
            &TableTabRow {
                table_id: tid as u64,
                schema_id: schema_id as u64,
                name,
                pk_col_idx: gnitz_wire::pack_pk_cols(pk),
                flags: gnitz_wire::TableProps::default().pack(),
            },
            1,
        );
        self.ddl_sync(SysFamily::Table.id(), bb.finish())
    }
}
