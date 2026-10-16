//! A view's definition as `CREATE VIEW` and `ALTER VIEW` record it: the body's
//! text and every place it names a catalog relation, relative to that text.

use gnitz_core::{RelationRef, ViewDefinition};

use crate::error::GnitzSqlError;
use crate::hir::LoweredView;
use crate::source::StatementText;
use sqlparser::tokenizer::Location;

/// The definition of the view `text` declares, from what binding its body found.
pub(crate) fn record_definition(
    text: StatementText<'_>,
    lowered: &LoweredView,
) -> Result<ViewDefinition, GnitzSqlError> {
    let unrecorded = |why: &str| GnitzSqlError::Rejected(format!("cannot record the view's definition: {why}"));
    let body = text
        .body_after_as()
        .ok_or_else(|| unrecorded("no AS ahead of the body"))?;
    let query = text.script[body.clone()].to_string();
    // A location in the script, as an offset into the body.
    let in_body = |at: Location| -> Result<u32, GnitzSqlError> {
        text.offset(at)
            .and_then(|at| at.checked_sub(body.start))
            .filter(|&at| at <= query.len())
            .and_then(|at| u32::try_from(at).ok())
            .ok_or_else(|| unrecorded("a relation reference lies outside the body"))
    };
    let mut refs = Vec::with_capacity(lowered.refs.len());
    for r in &lowered.refs {
        refs.push(RelationRef {
            tid: r.tid,
            name: in_body(r.name_span.start)?..in_body(r.name_span.end)?,
            alias_end: r.alias_span.map(|s| in_body(s.end)).transpose()?,
            alias: r.alias.clone(),
            columns: r.columns.clone(),
        });
    }
    // A body bound twice over one stretch of text names each relation once.
    refs.sort_by_key(|r| r.name.start);
    refs.dedup_by_key(|r| r.name.start);
    let def = ViewDefinition {
        query,
        body_columns: lowered.body_columns.clone(),
        refs,
    };
    // Held to exactly the rules a reader holds it to.
    def.check().map_err(|e| unrecorded(&e))?;
    Ok(def)
}
