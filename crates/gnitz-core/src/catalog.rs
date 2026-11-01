//! The catalog as a client reads it: the system families at one cut, each row
//! decoded by the `gnitz_wire` row reader the engine decodes it with.
//!
//! A chain segment is not a relation here. It is part of the user view that owns
//! it: the relations a view [`reads`](CatalogRelation::reads) are those its whole
//! chain scans.

use std::collections::HashMap;
use std::fmt;

use gnitz_wire::{
    qualified_key, CatalogColumn, Circuit, CircuitRow, ColTabRow, ColumnDef, FkRef, IdxTabRow, PkColList, RelClass,
    SchemaTabRow, TableProps, TableTabRow, ViewParts, ViewProps, ViewTabRow, SYSTEM_SCHEMA,
};

use crate::{ProtocolError, RelName, ZSetBatch};

/// The families a [`Catalog`] is decoded from, in the order
/// [`Catalog::decode`] takes their rows.
pub(crate) const CATALOG_FAMILIES: [u64; 6] = [
    gnitz_wire::SCHEMA_TAB,
    gnitz_wire::TABLE_TAB,
    gnitz_wire::VIEW_TAB,
    gnitz_wire::COL_TAB,
    gnitz_wire::IDX_TAB,
    gnitz_wire::CIRCUIT_TAB,
];

/// One live secondary index, as its IDX_TAB row states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRow {
    /// The id of the relation it indexes: a table or a view.
    pub owner: u64,
    pub name: String,
    pub cols: PkColList,
    pub is_unique: bool,
}

/// What a relation's own row states about it beyond its name and key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelProps {
    /// A TABLE_TAB row: a base table, a stream or a system family.
    Table(TableProps),
    /// A user view's VIEW_TAB row.
    View(ViewProps),
}

/// One table, stream, user view or system family.
#[derive(Clone, Debug)]
pub struct CatalogRelation {
    pub id: u64,
    pub schema: String,
    pub name: String,
    pub props: RelProps,
    /// Every column in schema order, hidden ones included.
    pub columns: Vec<CatalogColumn>,
    pub pk: PkColList,
    /// The secondary indexes declared on it.
    pub indexes: Vec<IndexRow>,
    /// The relations a view's chain scans, ascending; empty for any other
    /// relation.
    pub reads: Vec<u64>,
    /// A user view's encoded definition as its row stores it, undecoded so one
    /// unreadable definition costs only its own view; empty for every other
    /// relation.
    pub definition: Vec<u8>,
}

impl CatalogRelation {
    fn new(id: u64, schema: String, name: String, props: RelProps, pk: PkColList) -> Self {
        CatalogRelation {
            id,
            schema,
            name,
            props,
            columns: Vec::new(),
            pk,
            indexes: Vec::new(),
            reads: Vec::new(),
            definition: Vec::new(),
        }
    }

    /// What this relation is, as a RESOLVE of it answers.
    pub fn class(&self) -> RelClass {
        match self.props {
            RelProps::Table(_) if self.schema == SYSTEM_SCHEMA => RelClass::Catalog,
            RelProps::Table(props) if props.stream => RelClass::Stream,
            RelProps::Table(_) => RelClass::Table,
            RelProps::View(props) => props.budget.into(),
        }
    }

    /// `schema.name`, as the catalog spells it.
    pub fn qualified(&self) -> String {
        qualified_key(&self.schema, &self.name)
    }

    /// The columns a client reads, in schema order.
    pub fn visible_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().map(|c| &c.def).filter(|c| !c.is_hidden)
    }

    /// The names of the columns `cols` lists — its [`pk`](Self::pk) or an
    /// index's — in list order.
    pub fn column_names(&self, cols: &PkColList) -> Vec<&str> {
        cols.as_slice()
            .iter()
            .filter_map(|&ci| self.columns.get(ci as usize))
            .map(|c| &*c.def.name)
            .collect()
    }
}

/// The catalog at one cut.
#[derive(Clone, Debug)]
pub struct Catalog {
    /// Every schema's name, ascending.
    pub schemas: Vec<String>,
    /// Every relation, ordered by schema, then name.
    pub relations: Vec<CatalogRelation>,
}

impl Catalog {
    /// The relation `name` names.
    pub fn get(&self, name: &RelName) -> Option<&CatalogRelation> {
        self.relations
            .iter()
            .find(|r| r.schema == name.schema() && r.name == name.name())
    }

    /// The relation whose id is `id`.
    pub fn relation(&self, id: u64) -> Option<&CatalogRelation> {
        self.relations.iter().find(|r| r.id == id)
    }

    /// The views that read relation `id`.
    pub fn readers(&self, id: u64) -> impl Iterator<Item = &CatalogRelation> {
        self.relations.iter().filter(move |r| r.reads.contains(&id))
    }

    /// The relations `rel` reads, ascending by id.
    pub fn reads_of<'a>(&'a self, rel: &'a CatalogRelation) -> impl Iterator<Item = &'a CatalogRelation> {
        rel.reads.iter().filter_map(|&id| self.relation(id))
    }

    /// The table and column `fk` references.
    pub fn fk_target(&self, fk: FkRef) -> Option<(&CatalogRelation, &CatalogColumn)> {
        let table = self.relation(fk.table_id)?;
        Some((table, table.columns.get(fk.col as usize)?))
    }

    /// The tables with a FOREIGN KEY referencing table `id`.
    pub fn referencing(&self, id: u64) -> impl Iterator<Item = &CatalogRelation> {
        self.relations
            .iter()
            .filter(move |r| r.columns.iter().any(|c| c.fk.is_some_and(|fk| fk.table_id == id)))
    }

    /// Decode the live rows of the [`CATALOG_FAMILIES`], read at one cut and in
    /// that order.
    pub(crate) fn decode(families: &[ZSetBatch; CATALOG_FAMILIES.len()]) -> Result<Catalog, ProtocolError> {
        let [schemas, tables, views, columns, indices, circuits] = families;

        let mut schema_names: HashMap<u64, String> = HashMap::new();
        for i in schemas.live_rows() {
            let r = SchemaTabRow::read(schemas, i).map_err(|e| bad(format_args!("schema row {i}"), e))?;
            schema_names.insert(r.schema_id, r.name.into_owned());
        }
        let schema_of = |id: u64| schema_names.get(&id).cloned().unwrap_or_default();

        let mut relations = Vec::new();
        for i in tables.live_rows() {
            let r = TableTabRow::read(tables, i).map_err(|e| bad(format_args!("table row {i}"), e))?;
            let props = r.props().map_err(|e| bad(format_args!("table '{}'", r.name), e))?;
            relations.push(CatalogRelation::new(
                r.table_id,
                schema_of(r.schema_id),
                r.name.into_owned(),
                RelProps::Table(props),
                r.pk_col_idx,
            ));
        }
        // Each chain member's owner: itself for a user view.
        let mut chain_of: HashMap<u64, u64> = HashMap::new();
        for i in views.live_rows() {
            let r = ViewTabRow::read(views, i).map_err(|e| bad(format_args!("view row {i}"), e))?;
            let ViewParts { props, owner, .. } = r.parts().map_err(|e| bad(format_args!("view '{}'", r.name), e))?;
            chain_of.insert(r.view_id, owner.unwrap_or(r.view_id));
            if owner.is_none() {
                let mut rel = CatalogRelation::new(
                    r.view_id,
                    schema_of(r.schema_id),
                    r.name.into_owned(),
                    RelProps::View(props),
                    r.pk_col_idx,
                );
                rel.definition = r.definition.into_owned();
                relations.push(rel);
            }
        }
        let slot_of: HashMap<u64, usize> = relations.iter().enumerate().map(|(at, r)| (r.id, at)).collect();

        // Key order is `(owner_id, col_idx)`, so each owner's columns arrive in
        // schema order, every position once.
        for i in columns.live_rows() {
            let r = ColTabRow::read(columns, i).map_err(|e| bad(format_args!("column row {i}"), e))?;
            let what = format_args!("column {} of {}", r.col_idx, r.owner_id);
            if let Some(&at) = slot_of.get(&r.owner_id) {
                r.append_to(&mut relations[at].columns).map_err(|e| bad(what, e))?;
            }
        }
        for (_, index) in idx_rows(indices)? {
            if let Some(&at) = slot_of.get(&index.owner) {
                relations[at].indexes.push(index);
            }
        }
        for i in circuits.live_rows() {
            let Ok(r) = CircuitRow::read(circuits, i);
            let circuit =
                Circuit::decode(&r.circuit).map_err(|e| bad(format_args!("circuit of view {}", r.view_id), e))?;
            let Some(&view) = chain_of.get(&r.view_id) else {
                continue;
            };
            let Some(&at) = slot_of.get(&view) else {
                continue;
            };
            // A member scanning another member of its own chain reads nothing the
            // user view does not own.
            let outside = circuit.sources().filter(|s| chain_of.get(s) != Some(&view));
            relations[at].reads.extend(outside);
        }
        for r in &mut relations {
            r.reads.sort_unstable();
            r.reads.dedup();
        }

        relations.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        let mut schemas: Vec<String> = schema_names.into_values().collect();
        schemas.sort();
        Ok(Catalog { schemas, relations })
    }
}

/// The live rows of `batch`, a read of IDX_TAB, each with its index in `batch`.
pub(crate) fn idx_rows(batch: &ZSetBatch) -> Result<Vec<(usize, IndexRow)>, ProtocolError> {
    batch
        .live_rows()
        .map(|i| {
            let r = IdxTabRow::read(batch, i).map_err(|e| bad(format_args!("index row {i}"), e))?;
            let index = IndexRow {
                owner: r.owner_id,
                name: r.name.into_owned(),
                cols: r.source_col_idx,
                is_unique: r.is_unique,
            };
            Ok((i, index))
        })
        .collect()
}

/// A catalog row `what` that failed to decode with `e`.
fn bad(what: fmt::Arguments<'_>, e: String) -> ProtocolError {
    ProtocolError::DecodeError(format!("{what}: {e}"))
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
