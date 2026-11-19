//! The catalog written as the statements that create it: what `\d+` shows of a
//! view and what a dump replays. Every relation name is written qualified, and
//! a view's body is its recorded text with the names the catalog holds now
//! standing where the original ones stood.

use gnitz_core::{Catalog, CatalogRelation, RelProps};
use gnitz_wire::{FkAction, TableDistribution};

use super::options::with_options;
use super::view_def::ViewDefinition;
use crate::names::{column_text, relation_text};
use crate::types::serial_type_name;

/// `CREATE SCHEMA <name>`.
pub fn create_schema_statement(name: &str) -> String {
    format!("CREATE SCHEMA {}", relation_text(name))
}

/// `rel`'s name as a statement names it: schema-qualified.
pub fn qualified_name(rel: &CatalogRelation) -> String {
    format!("{}.{}", relation_text(&rel.schema), relation_text(&rel.name))
}

/// ` WITH (…)` declaring `props`, or nothing where every option is the default.
fn with_clause(props: &RelProps) -> String {
    let options = with_options(props);
    match options.is_empty() {
        true => String::new(),
        false => format!(" WITH ({})", options.join(", ")),
    }
}

/// The `CREATE TABLE` that declares base table or stream `rel`: its visible
/// columns, key, foreign keys, every unique index as a named constraint, and its
/// distribution and options. The table's other indexes are
/// [`create_index_statements`]'.
pub fn create_table_statement(catalog: &Catalog, rel: &CatalogRelation) -> Result<String, String> {
    let RelProps::Table(props) = rel.props else {
        return Err(format!("{} is not a table", rel.qualified()));
    };
    let pk = rel.pk.as_slice();
    let mut lines = Vec::new();
    for (ci, col) in rel.columns.iter().enumerate() {
        let c = &col.def;
        if c.is_hidden {
            continue;
        }
        let mut line = format!("{} ", column_text(&c.name));
        if props.serial && pk == [ci as u32] {
            let serial = serial_type_name(c.ty.tc)
                .ok_or_else(|| format!("{}: a SERIAL column of type {}", rel.qualified(), c.ty.tc))?;
            line.push_str(serial);
        } else {
            line.push_str(&c.ty.to_string());
            if !c.is_nullable {
                line.push_str(" NOT NULL");
            }
        }
        if let Some(fk) = col.fk {
            let (parent, parent_col) = catalog
                .fk_target(fk)
                .ok_or_else(|| format!("{}.{}: its foreign key names no column", rel.qualified(), c.name))?;
            line.push_str(&format!(
                " REFERENCES {} ({})",
                qualified_name(parent),
                column_text(&parent_col.def.name)
            ));
            if fk.on_delete == FkAction::Cascade {
                line.push_str(" ON DELETE CASCADE");
            }
        }
        lines.push(line);
    }
    lines.push(format!("PRIMARY KEY ({})", column_list(rel.column_names(&rel.pk))));
    for index in rel.indexes.iter().filter(|i| i.is_unique) {
        lines.push(format!(
            "CONSTRAINT {} UNIQUE ({})",
            column_text(&index.name),
            column_list(rel.column_names(&index.source_col_idx))
        ));
    }
    let mut out = format!(
        "CREATE TABLE {} (\n    {}\n)",
        qualified_name(rel),
        lines.join(",\n    ")
    );
    out.push_str(&with_clause(&rel.props));
    if let TableDistribution::Keyed { prefix_len } = props.distribution
        && prefix_len > 0
    {
        let prefix = &rel.column_names(&rel.pk)[..usize::from(prefix_len).min(pk.len())];
        out.push_str(&format!(" CLUSTER BY {}", column_list(prefix.iter().copied())));
    }
    Ok(out)
}

/// The `CREATE INDEX` statements of every index of `rel` that its own `CREATE`
/// statement does not declare: a view's every index, a table's non-unique ones.
pub fn create_index_statements(rel: &CatalogRelation) -> impl Iterator<Item = String> {
    let view = matches!(rel.props, RelProps::View(_));
    rel.indexes
        .iter()
        .filter(move |i| view || !i.is_unique)
        .map(move |index| {
            format!(
                "CREATE {}INDEX {} ON {} ({})",
                if index.is_unique { "UNIQUE " } else { "" },
                column_text(&index.name),
                qualified_name(rel),
                column_list(rel.column_names(&index.source_col_idx))
            )
        })
}

/// The `CREATE VIEW` that declares user view `rel` as it stands now.
pub fn create_view_statement(catalog: &Catalog, rel: &CatalogRelation) -> Result<String, String> {
    let RelProps::View(_) = rel.props else {
        return Err(format!("{} is not a view", rel.qualified()));
    };
    let def = ViewDefinition::decode(&rel.definition).map_err(|e| format!("{}: {e}", rel.qualified()))?;
    let mut out = format!("CREATE VIEW {}", qualified_name(rel));
    let names: Vec<&str> = rel.visible_columns().map(|c| &*c.name).collect();
    if def.body_columns != names {
        out.push_str(&format!(" ({})", column_list(names.iter().copied())));
    }
    out.push_str(&with_clause(&rel.props));
    out.push_str(" AS\n");
    out.push_str(&view_body(catalog, &def)?);
    Ok(out)
}

/// `def`'s body under the names `catalog` holds: each relation it names written
/// qualified, read by the name the body reads it by, and — where its columns
/// were renamed since — under its old column names.
pub(crate) fn view_body(catalog: &Catalog, def: &ViewDefinition) -> Result<String, String> {
    let q = &def.query;
    let mut out = String::with_capacity(q.len());
    let mut at = 0;
    for r in &def.refs {
        let (start, end) = (r.name.start as usize, r.name.end as usize);
        out.push_str(&q[at..start]);
        let rel = catalog
            .relation(r.tid)
            .ok_or_else(|| format!("the view reads relation {}, which the catalog no longer holds", r.tid))?;
        out.push_str(&qualified_name(rel));
        let now: Vec<&str> = rel.visible_columns().map(|c| &*c.name).collect();
        let old_columns = r.columns.as_ref().filter(|old| **old != now);
        let renames = old_columns.map(|old| format!("({})", column_list(old.iter().map(String::as_str))));
        match r.alias_end {
            // The written alias follows; only a column list is owed after it.
            Some(alias_end) => {
                out.push_str(&q[end..alias_end as usize]);
                out.extend(renames);
                at = alias_end as usize;
            }
            None => {
                if renames.is_some() || !rel.name.eq_ignore_ascii_case(&r.alias) {
                    out.push_str(" AS ");
                    out.push_str(&relation_text(&r.alias));
                    out.extend(renames);
                }
                at = end;
            }
        }
    }
    out.push_str(&q[at..]);
    Ok(out)
}

fn column_list<'a>(names: impl IntoIterator<Item = &'a str>) -> String {
    names.into_iter().map(column_text).collect::<Vec<_>>().join(", ")
}
