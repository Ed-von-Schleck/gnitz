//! The client's relation schema: column definitions and the layout facts of
//! its PK list.

use gnitz_expr::{SchemaColumn, SchemaDescriptor, SchemaRefusal};
use gnitz_zset::schema::pk_keys;
use std::sync::{Arc, OnceLock};

use gnitz_wire::{ColumnDef, KeyListRefusal, PkColList, PkListRole, SysFamily};

#[derive(Clone)]
pub struct Schema {
    columns: Vec<ColumnDef>,
    layout: SchemaDescriptor,
    /// The shape record of `columns` and the PK list, encoded on first use.
    shape_record: OnceLock<Arc<[u8]>>,
}

/// Equality is over the columns and the layout; whether the shape record is
/// cached does not enter it.
impl PartialEq for Schema {
    fn eq(&self, o: &Schema) -> bool {
        self.columns == o.columns && self.layout == o.layout
    }
}

impl Eq for Schema {}

impl std::fmt::Debug for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Schema")
            .field("columns", &self.columns)
            .field("layout", &self.layout)
            .finish()
    }
}

impl Schema {
    /// The constructor every other one goes through. `Err` for a column list over `MAX_COLUMNS`, an
    /// inadmissible column type, or a PK list that is no [`PkColList`] over these
    /// columns or breaks the PK rules.
    pub fn from_parts(columns: Vec<ColumnDef>, pk_cols: &[u32]) -> Result<Schema, String> {
        if let Some(cd) = columns.iter().find(|cd| !cd.ty.is_admissible()) {
            return Err(format!(
                "column '{}' ({:?}) carries scale {}",
                cd.name, cd.ty.tc, cd.ty.scale
            ));
        }
        let pk_refusal =
            |rule: KeyListRefusal| rule.named(PkListRole::PrimaryKey, |c| columns.get(c as usize).map(|cd| &*cd.name));
        PkColList::checked(pk_cols, columns.len()).map_err(pk_refusal)?;
        let layout = SchemaDescriptor::try_new(&columns.iter().map(SchemaColumn::of_def).collect::<Vec<_>>(), pk_cols)
            .map_err(|refusal| match refusal {
                SchemaRefusal::Pk(rule) => pk_refusal(rule),
                other => other.to_string(),
            })?;
        Ok(Schema {
            columns,
            layout,
            shape_record: OnceLock::new(),
        })
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

    /// The key set of the keys `natives` lists, each one native value per PK column in
    /// PK-list order: a read's `PkSet`, or the keys of a retraction.
    pub fn pk_keys<N: AsRef<[u128]>>(&self, natives: impl IntoIterator<Item = N>) -> gnitz_wire::PkKeys {
        let keys: Vec<gnitz_wire::PkBuf> = (natives.into_iter())
            .map(|n| {
                // Hard, not debug-only: a short list would encode a short key.
                assert_eq!(
                    n.as_ref().len(),
                    self.pk_cols().len(),
                    "pk_keys: one native value per key column"
                );
                self.layout.opk_key_cols(n.as_ref())
            })
            .collect();
        pk_keys(self.layout.pk_stride(), keys.iter().map(|k| k.pk_bytes()))
    }

    /// The PK columns in PK-list order, hidden: the leading key of a reply that
    /// carries this schema's key.
    pub fn hidden_key_columns(&self) -> impl Iterator<Item = ColumnDef> {
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

    /// This schema's shape record: the nameless meta-schema record a push
    /// carries as its blob, with every hidden flag clear. [`Self::from_record`]
    /// of it is not this schema, whose names it drops. Encoded once per schema;
    /// a clone shares it.
    pub fn shape_record(&self) -> &[u8] {
        self.shape_record.get_or_init(|| {
            gnitz_wire::schema_record::encode(self.columns.iter().map(ColumnDef::shape_col), self.pk_cols()).into()
        })
    }

    /// The schema a meta-schema record describes.
    pub fn from_record(record: &[u8]) -> Result<Schema, String> {
        let mut columns = Vec::new();
        let pk = gnitz_wire::schema_record::decode(record, |c| {
            columns.push(ColumnDef {
                name: Arc::from(c.name),
                ty: c.ty,
                is_nullable: c.nullable,
                is_hidden: c.hidden,
            });
            Ok(())
        })?;
        Schema::from_parts(columns, pk.as_slice())
    }
}

/// The client `Schema` of system family `family`.
pub fn sys_schema(family: SysFamily) -> &'static Arc<Schema> {
    static INSTANCE: OnceLock<[Arc<Schema>; SysFamily::COUNT]> = OnceLock::new();
    let schemas = INSTANCE.get_or_init(|| {
        SysFamily::ALL.map(|f| {
            let (columns, pk) = (f.def().column_defs().collect(), f.def().pk_cols);
            Arc::new(Schema::from_parts(columns, pk).expect("a system family is an admissible schema"))
        })
    });
    &schemas[family.index()]
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
