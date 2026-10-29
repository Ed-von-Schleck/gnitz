//! The client's relation schema: column definitions and the layout facts of
//! its PK list.

use gnitz_expr::{SchemaColumn, SchemaDescriptor, SchemaRefusal};
use std::sync::{Arc, OnceLock};

use gnitz_wire::{ColumnDef, PkListRole, PkRule, PK_LIST_MAX_COLS};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    columns: Vec<ColumnDef>,
    layout: SchemaDescriptor,
}

impl Schema {
    /// The one constructor. `Err` for a column list over `MAX_COLUMNS`, an
    /// inadmissible column type, or a PK list that breaks the PK rules at
    /// `PK_LIST_MAX_COLS`, the capacity of a persisted PK list.
    pub fn from_parts(columns: Vec<ColumnDef>, pk_cols: &[u32]) -> Result<Schema, String> {
        if let Some(cd) = columns.iter().find(|cd| !cd.ty.is_admissible()) {
            return Err(format!(
                "column '{}' ({:?}) carries scale {}",
                cd.name, cd.ty.tc, cd.ty.scale
            ));
        }
        let pk_refusal = |rule: PkRule| {
            rule.named(PkListRole::PrimaryKey, |c| {
                columns.get(c as usize).map(|cd| cd.name.as_str())
            })
        };
        if pk_cols.len() > PK_LIST_MAX_COLS {
            return Err(pk_refusal(PkRule::TooManyColumns {
                count: pk_cols.len(),
                max: PK_LIST_MAX_COLS,
            }));
        }
        let cols: Vec<SchemaColumn> = columns
            .iter()
            .map(|cd| SchemaColumn::new(cd.ty.tc, cd.is_nullable))
            .collect();
        let layout = SchemaDescriptor::try_new(&cols, pk_cols).map_err(|refusal| match refusal {
            SchemaRefusal::Pk(rule) => pk_refusal(rule),
            other => other.to_string(),
        })?;
        Ok(Schema { columns, layout })
    }

    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// PK column indices in compound-key order; length >= 1.
    pub fn pk_cols(&self) -> &[u32] {
        self.layout.pk_cols()
    }

    /// The layout facts of this schema's columns and PK list.
    pub fn layout(&self) -> &SchemaDescriptor {
        &self.layout
    }

    /// A name carries no layout.
    pub fn rename_column(&mut self, ci: usize, name: String) {
        self.columns[ci].name = name;
    }

    /// The PK columns in PK-list order, hidden: the leading key of a reply that
    /// carries this schema's key.
    pub fn hidden_key_columns(&self) -> impl Iterator<Item = ColumnDef> + '_ {
        self.pk_cols()
            .iter()
            .map(|&c| self.columns[c as usize].clone().hidden())
    }

    /// The non-PK ("payload") columns as `(payload slot, col_idx, &ColumnDef)`,
    /// in slot order.
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, usize, &ColumnDef)> {
        (0..self.layout.num_payload_cols()).map(|pi| {
            let ci = self.layout.payload_col_idx(pi);
            (pi, ci, &self.columns[ci])
        })
    }

    /// The visible (non-hidden) columns as `(col_idx, &ColumnDef)`. `col_idx` is
    /// the position in the full schema, so a hidden column does not shift the
    /// ones after it.
    #[inline]
    pub fn visible_columns(&self) -> impl Iterator<Item = (usize, &ColumnDef)> {
        self.columns.iter().enumerate().filter(|(_, c)| !c.is_hidden)
    }

    /// This schema's meta-schema record — the bytes a schema-bearing push embeds.
    pub fn to_block(&self) -> Vec<u8> {
        gnitz_wire::schema_block::encode(self.columns.iter().map(ColumnDef::block_col), self.pk_cols())
    }

    /// The schema a meta-schema record describes.
    pub fn from_block(block: &[u8]) -> Result<Schema, String> {
        let mut columns = Vec::new();
        let pk = gnitz_wire::schema_block::decode(block, |c| {
            let name = std::str::from_utf8(c.name).map_err(|e| format!("utf8 in column name: {e}"))?;
            columns.push(ColumnDef {
                name: name.to_owned(),
                ty: c.ty,
                is_nullable: c.nullable,
                is_hidden: c.hidden,
            });
            Ok(())
        })?;
        Schema::from_parts(columns, pk.as_slice())
    }
}

/// The client `Schema` of system table `tid`, built from `gnitz_wire::SYS_FAMILIES`.
///
/// Panics on an id that is not a system table.
pub fn sys_schema(tid: u64) -> &'static Arc<Schema> {
    static INSTANCE: OnceLock<Vec<Arc<Schema>>> = OnceLock::new();
    let schemas = INSTANCE.get_or_init(|| {
        gnitz_wire::SYS_FAMILIES
            .iter()
            .map(|f| {
                let columns = f.column_defs().collect();
                Arc::new(Schema::from_parts(columns, f.pk_cols).expect("a system family is an admissible schema"))
            })
            .collect()
    });
    let idx = gnitz_wire::sys_family_index(tid).unwrap_or_else(|| panic!("not a system table id: {tid}"));
    &schemas[idx]
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
