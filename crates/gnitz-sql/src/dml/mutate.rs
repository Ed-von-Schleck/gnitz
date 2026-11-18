//! UPDATE and DELETE, planned: the WHERE through the access-path ladder
//! (`crate::access::bound_and_predicate`), and UPDATE's SET list.

use super::set::{SetClause, bind_set_list};
use super::write::{Write, WritePlan};
use crate::access::bound_and_predicate;
use crate::ast_util::{FromShape, classify_from};
use crate::bind::bind_single_table;
use crate::catalog::Catalog;
use crate::error::{GnitzSqlError, reject_if};
use crate::names::{TableRef, extract_table_ref};
use crate::rules::{ClassWant, require_class};
use sqlparser::ast::{Assignment, Delete, Expr, FromTable, TableWithJoins, Update};

pub(super) fn plan_update(update: &Update, cat: &dyn Catalog) -> Result<WritePlan, GnitzSqlError> {
    let (table, selection, assignments) = reject_unhonored_update_clauses(update)?;
    plan_mutation(std::slice::from_ref(table), selection, Some(assignments), cat)
}

pub(super) fn plan_delete(del: &Delete, cat: &dyn Catalog) -> Result<WritePlan, GnitzSqlError> {
    let (from, selection) = reject_unhonored_delete_clauses(del)?;
    plan_mutation(from, selection, None, cat)
}

/// Reject every `UPDATE` clause `plan_mutation` does not consume, and hand back
/// the table, WHERE and SET list it does. Each rejected clause would otherwise be
/// dropped: a dropped `FROM` binds SET/WHERE against the wrong relations.
fn reject_unhonored_update_clauses(
    update: &Update,
) -> Result<(&TableWithJoins, Option<&Expr>, &[Assignment]), GnitzSqlError> {
    const CTX: &str = "UPDATE";
    let Update {
        // `table` whole: `plan_mutation` classifies its FROM shape, so a join
        // written there is rejected.
        table,
        assignments,
        selection,
        // Inert: the `UPDATE` token and advisory-only comment hints.
        update_token: _,
        optimizer_hints: _,
        // Rejected: each is a clause `plan_mutation` does not implement.
        from,
        returning,
        output,
        or,
        order_by,
        limit,
        on_cluster,
        settings,
    } = update;
    reject_if(from.is_some(), CTX, "FROM (join-update)")?;
    reject_if(returning.is_some(), CTX, "RETURNING")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(or.is_some(), CTX, "OR (conflict clause)")?;
    reject_if(!order_by.is_empty(), CTX, "ORDER BY")?;
    reject_if(limit.is_some(), CTX, "LIMIT")?;
    reject_if(on_cluster.is_some(), CTX, "ON CLUSTER")?;
    reject_if(settings.is_some(), CTX, "SETTINGS")?;
    Ok((table, selection.as_ref(), assignments))
}

/// Reject every `DELETE` clause `plan_mutation` does not consume, and hand back
/// the FROM list and WHERE it does. Each rejected clause would otherwise be
/// dropped: a dropped `LIMIT` deletes every matched row, a dropped `USING` binds
/// WHERE against the wrong relations.
fn reject_unhonored_delete_clauses(del: &Delete) -> Result<(&[TableWithJoins], Option<&Expr>), GnitzSqlError> {
    const CTX: &str = "DELETE";
    let Delete {
        from,
        selection,
        // Inert: the `DELETE` token and advisory-only comment hints.
        delete_token: _,
        optimizer_hints: _,
        tables,
        using,
        returning,
        output,
        order_by,
        limit,
        on_cluster,
        settings,
    } = del;
    reject_if(!tables.is_empty(), CTX, "multi-table delete")?;
    reject_if(using.is_some(), CTX, "USING (join-delete)")?;
    reject_if(returning.is_some(), CTX, "RETURNING")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(!order_by.is_empty(), CTX, "ORDER BY")?;
    reject_if(limit.is_some(), CTX, "LIMIT")?;
    reject_if(on_cluster.is_some(), CTX, "ON CLUSTER")?;
    reject_if(settings.is_some(), CTX, "SETTINGS")?;
    let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) = from;
    Ok((from, selection.as_ref()))
}

fn plan_mutation(
    from: &[TableWithJoins],
    selection: Option<&Expr>,
    set: Option<&[Assignment]>,
    cat: &dyn Catalog,
) -> Result<WritePlan, GnitzSqlError> {
    let verb = if set.is_some() { "UPDATE" } else { "DELETE" };
    // `UPDATE a JOIN b ON … SET v = 1` parses; honoring only the relation would
    // update all of `a`.
    let FromShape::SinglePlainRelation(factor) = classify_from(from) else {
        return Err(GnitzSqlError::rejected(format!(
            "{verb}: exactly one simple FROM table required"
        )));
    };
    // The statement writes this table, whose columns keep their own names.
    let TableRef { name: table_name, alias, columns, .. } = extract_table_ref(factor, cat.schema_name(), verb)?;
    reject_if(
        !columns.is_empty(),
        verb,
        "positional column aliases on the table it writes",
    )?;
    let target = cat.probe_relation(&table_name)?;
    require_class(&target, &table_name, ClassWant::BaseTable, verb)?;
    let schema = &target.schema;
    let set = set
        .map(|raw| bind_set_list(raw, schema, &alias, SetClause::Update))
        .transpose()?;
    let conjuncts = match selection {
        Some(we) => bind_single_table(we, schema, &alias)?.conjuncts(),
        None => Vec::new(),
    };
    let (bound, predicate) = bound_and_predicate(schema, &conjuncts, &target.indexes)?;
    let write = match set {
        Some(set) => Write::Update { bound, predicate, set },
        None => Write::Delete { bound, predicate },
    };
    Ok(WritePlan { target, write })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/mutate.rs"]
mod tests;
