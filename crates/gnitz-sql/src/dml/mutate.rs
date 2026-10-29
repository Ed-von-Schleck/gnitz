//! UPDATE and DELETE, as one read-then-write flow: plan the WHERE through the
//! access-path ladder (`crate::access::bound_and_predicate`), then, as one
//! read-modify-write, read the rows it matches and write the rewritten rows
//! (UPDATE) or the retraction of their keys (DELETE). Planning reads only the
//! catalog, so a refused statement issues no request.

use super::set::{apply_set, bind_set_list, SetClause, SetCol};
use crate::access::bound_and_predicate;
use crate::ast_util::{classify_from, FromShape};
use crate::bind::bind_single_table;
use crate::catalog::Catalog;
use crate::error::{reject_if, GnitzSqlError};
use crate::names::{extract_table_ref, TableRef};
use crate::rules::{require_class, ClassWant};
use crate::SqlResult;
use gnitz_core::{GnitzClient, RelDescriptor, Schema, ZSetBatch};
use gnitz_wire::ReadBound;
use sqlparser::ast::{Assignment, Delete, Expr, FromTable, TableWithJoins, Update};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// UPDATE / DELETE
// ---------------------------------------------------------------------------

/// A single-table UPDATE (`set` present) or DELETE, planned: the read that
/// finds its rows and what to write for them.
pub(crate) struct MutationPlan {
    target: Arc<RelDescriptor>,
    bound: ReadBound,
    predicate: Vec<u8>,
    set: Option<Vec<SetCol>>,
}

pub(crate) fn plan_update(update: &Update, cat: &dyn Catalog) -> Result<MutationPlan, GnitzSqlError> {
    let (table, selection, assignments) = reject_unhonored_update_clauses(update)?;
    plan_mutation(std::slice::from_ref(table), selection, Some(assignments), cat)
}

pub(crate) fn plan_delete(del: &Delete, cat: &dyn Catalog) -> Result<MutationPlan, GnitzSqlError> {
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
    } = update;
    reject_if(from.is_some(), CTX, "FROM (join-update)")?;
    reject_if(returning.is_some(), CTX, "RETURNING")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(or.is_some(), CTX, "OR (conflict clause)")?;
    reject_if(!order_by.is_empty(), CTX, "ORDER BY")?;
    reject_if(limit.is_some(), CTX, "LIMIT")?;
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
    } = del;
    reject_if(!tables.is_empty(), CTX, "multi-table delete")?;
    reject_if(using.is_some(), CTX, "USING (join-delete)")?;
    reject_if(returning.is_some(), CTX, "RETURNING")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(!order_by.is_empty(), CTX, "ORDER BY")?;
    reject_if(limit.is_some(), CTX, "LIMIT")?;
    let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) = from;
    Ok((from, selection.as_ref()))
}

fn plan_mutation(
    from: &[TableWithJoins],
    selection: Option<&Expr>,
    set: Option<&[Assignment]>,
    cat: &dyn Catalog,
) -> Result<MutationPlan, GnitzSqlError> {
    let verb = if set.is_some() { "UPDATE" } else { "DELETE" };
    // `UPDATE a JOIN b ON … SET v = 1` parses; honoring only the relation would
    // update all of `a`.
    let FromShape::SinglePlainRelation(factor) = classify_from(from) else {
        return Err(GnitzSqlError::Rejected(format!(
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
    Ok(MutationPlan { target, bound, predicate, set })
}

/// Read the rows the WHERE matches, then write [`delta`] of them, tagged
/// UPDATE under a SET list and DELETE without one.
pub(crate) async fn execute_mutation(client: &mut GnitzClient, plan: MutationPlan) -> Result<SqlResult, GnitzSqlError> {
    let MutationPlan { target, bound, predicate, mut set } = plan;
    let keys_only = set.is_none();
    let tag = if keys_only { "DELETE" } else { "UPDATE" };
    let count = client
        .read_modify_write(&target, bound, predicate, keys_only, |rows| {
            delta(set.as_deref_mut(), rows, &target.schema)
        })
        .await?;
    Ok(SqlResult::RowsAffected { tag, count })
}

/// What a mutation writes for the `rows` it read: under a SET list the rewritten
/// rows (UPDATE), without one the retraction of their keys (DELETE).
fn delta(set: Option<&mut [SetCol]>, rows: ZSetBatch, schema: &Schema) -> Result<ZSetBatch, GnitzSqlError> {
    match set {
        Some(set) => apply_set(set, rows, None, schema),
        None => Ok(ZSetBatch::retraction(schema, rows.pks)),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/mutate.rs"]
mod tests;
