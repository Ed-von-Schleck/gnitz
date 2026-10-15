//! A snapshot of the server's catalog, read from the `_system` families in one
//! cut: what completion offers and what `\d` lists.

use std::collections::HashMap;

use gnitz_core::{block_on, ClientError, GnitzClient, RelName};
use gnitz_wire::sys_rows::{ColTabRow, IdxTabRow, SchemaTabRow, TableTabRow, ViewTabRow};
use gnitz_wire::{
    ColumnDef, PkColList, PkListRole, ReadBound, ReadSpec, RelClass, TableProps, ViewBudget, COL_TAB, IDX_TAB,
    SCHEMA_TAB, SYSTEM_SCHEMA, TABLE_TAB, VIEW_TAB,
};

#[derive(Clone, Debug)]
pub struct IndexInfo {
    pub name: String,
    pub unique: bool,
    /// The indexed columns' names, in index order.
    pub columns: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Relation {
    pub id: u64,
    pub schema: String,
    pub name: String,
    pub class: RelClass,
    /// A view's `WITH (capacity | delta = …)`.
    pub budget: ViewBudget,
    /// In column order.
    pub columns: Vec<ColumnDef>,
    pub pk: Vec<String>,
    pub indexes: Vec<IndexInfo>,
}

impl Relation {
    pub fn qualified(&self) -> String {
        gnitz_wire::qualified_key(&self.schema, &self.name)
    }
}

#[derive(Clone, Debug, Default)]
pub struct Catalog {
    pub schemas: Vec<String>,
    pub relations: Vec<Relation>,
}

/// The names of `list`'s columns of `columns`.
fn column_names(columns: &[ColumnDef], list: PkColList) -> Vec<String> {
    list.as_slice()
        .iter()
        .filter_map(|&ci| columns.get(ci as usize).map(|c| c.name.clone()))
        .collect()
}

impl Catalog {
    /// The relation `name` names, read in `schema` when it is unqualified.
    pub fn find(&self, schema: &str, name: &str) -> Option<&Relation> {
        let name = RelName::parse(schema, name).ok()?;
        self.relations
            .iter()
            .find(|r| r.schema == name.schema() && r.name == name.name())
    }

    /// Read the catalog through `client`.
    pub fn load(client: &mut GnitzClient) -> Result<Catalog, ClientError> {
        let all = ReadSpec::all_rows(ReadBound::None);
        let [schemas, tables, views, columns, indices] = block_on(client.sys_cut([
            (SCHEMA_TAB, &all),
            (TABLE_TAB, &all),
            (VIEW_TAB, &all),
            (COL_TAB, &all),
            (IDX_TAB, &all),
        ]))?;

        let mut schema_names: HashMap<u64, String> = HashMap::new();
        for i in schemas.live_rows() {
            let r = SchemaTabRow::read(&schemas, i)?;
            schema_names.insert(r.schema_id, r.name.into_owned());
        }
        let schema_of = |id: u64| schema_names.get(&id).cloned().unwrap_or_default();

        let mut relations: Vec<Relation> = Vec::new();
        let mut pk_words: Vec<u64> = Vec::new();
        let mut push = |id, schema: String, name: String, budget: ViewBudget, class, pk_word| {
            relations.push(Relation {
                id,
                schema,
                name,
                class,
                budget,
                columns: Vec::new(),
                pk: Vec::new(),
                indexes: Vec::new(),
            });
            pk_words.push(pk_word);
        };
        for i in tables.live_rows() {
            let r = TableTabRow::read(&tables, i)?;
            let schema = schema_of(r.schema_id);
            let class = if schema == SYSTEM_SCHEMA {
                RelClass::Catalog
            } else if TableProps::from_flags(r.flags)?.stream {
                RelClass::Stream
            } else {
                RelClass::Table
            };
            push(
                r.table_id,
                schema,
                r.name.into_owned(),
                ViewBudget::Plain,
                class,
                r.pk_col_idx,
            );
        }
        for i in views.live_rows() {
            let r = ViewTabRow::read(&views, i)?;
            // A chain segment is the internal part of a user view.
            if r.owner_view_id != 0 {
                continue;
            }
            let budget = ViewBudget::from_row(r.capacity_bytes, r.delta_bytes)?;
            let schema = schema_of(r.schema_id);
            push(
                r.view_id,
                schema,
                r.name.into_owned(),
                budget,
                budget.into(),
                r.pk_col_idx,
            );
        }

        let by_id: HashMap<u64, usize> = relations.iter().enumerate().map(|(i, r)| (r.id, i)).collect();
        let mut cols: Vec<(u64, u64, ColumnDef)> = Vec::new();
        for i in columns.live_rows() {
            let r = ColTabRow::read(&columns, i)?;
            if by_id.contains_key(&r.owner_id) {
                cols.push((r.owner_id, r.col_idx, r.column()?.0));
            }
        }
        cols.sort_by_key(|&(owner, idx, _)| (owner, idx));
        for (owner, _, col) in cols {
            relations[by_id[&owner]].columns.push(col);
        }
        for (rel, word) in relations.iter_mut().zip(pk_words) {
            let list = PkColList::unpack(word).map_err(|rule| rule.for_role(PkListRole::PrimaryKey))?;
            rel.pk = column_names(&rel.columns, list);
        }
        for i in indices.live_rows() {
            let r = IdxTabRow::read(&indices, i)?;
            let (owner, list, unique) = r.parts()?;
            let Some(&at) = by_id.get(&owner) else {
                continue;
            };
            let columns = column_names(&relations[at].columns, list);
            relations[at].indexes.push(IndexInfo {
                name: r.name.into_owned(),
                unique,
                columns,
            });
        }

        relations.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        let mut schemas: Vec<String> = schema_names.into_values().collect();
        schemas.sort();
        Ok(Catalog { schemas, relations })
    }
}
