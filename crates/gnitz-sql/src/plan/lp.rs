//! Bound logical IR (`Rel`) and AST→IR lowering.
//!
//! Lowering resolves every name, validates every clause, and classifies every
//! predicate up front, so an `Rel` node carries only resolved data — no consumer
//! ever needs the `AliasMap` again.

use crate::ast_util::{extract_relation_name, is_wildcard_projection};
use crate::bind::{bind_single_table, Binder};
use crate::codec::project_schema::{build_projection, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::plan::validate::{reject_duplicate_column_names, reject_unhonored_select_clauses, HonoredClauses};
use gnitz_core::{ColumnDef, GnitzClient, Schema};
use sqlparser::ast::{Select, SelectItem, WildcardAdditionalOptions};
use std::rc::Rc;

/// Bound logical IR node. Every reference is a resolved *logical* column index
/// (0-based within the node's visible column space); the emitters map logical →
/// physical.
pub(crate) enum Rel {
    /// A base table or committed/hidden view. `schema` is the full registered
    /// schema; a later hidden-view input will restrict its visible range, but a
    /// base table or user view is fully visible.
    ///
    /// `bound` narrows the initial backfill scan to a secondary-index range. A
    /// physical access hint carried as data — extracted where catalog provenance is
    /// known (`lower_linear`, which holds the client, the binder and the raw AST) and
    /// consumed by the emitter, which has none of those.
    Source {
        tid: u64,
        schema: Rc<Schema>,
        bound: Option<gnitz_wire::ScanBound>,
    },
    /// Linear filter; `pred` is bound against `input`'s (source) schema.
    Filter { input: Box<Rel>, pred: BoundExpr },
    /// Linear projection; `items`/`out_cols` as in `codec::project_schema`. The
    /// leading `pk_arity` output columns are the view's physical PK (passed
    /// through from the source in PK order).
    Project {
        input: Box<Rel>,
        items: Vec<ProjItem>,
        out_cols: Vec<ColumnDef>,
        pk_arity: usize,
    },
}

/// Lower a linear (filter/map) CREATE VIEW body to `Project(Filter?(Source))`:
/// clause rejection, single-table resolution, WHERE binding against the source
/// schema, and projection building. The circuit is built by `simple::emit_linear`.
pub(crate) fn lower_linear(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    select: &Select,
) -> Result<Rel, GnitzSqlError> {
    // Filter/map views consume only WHERE + projection; reject the rest (GROUP BY,
    // HAVING, PREWHERE, TOP, …) so a dropped clause is a clean error.
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
        },
        "CREATE VIEW",
    )?;
    let table_name = extract_relation_name(&select.from[0].relation, "CREATE VIEW")?;
    let (source_tid, source_schema) = binder.resolve(client, &table_name)?;

    // Extracted here, not inside the WHERE block below: that block only *wraps*
    // `rel` in a `Rel::Filter`, so there is no `Source` left to mutate by then.
    let bound = crate::plan::index_bound::scan_bound_for_input(
        client,
        select.selection.as_ref(),
        (source_tid, &source_schema),
        binder.is_catalog_relation(&table_name),
    )?;
    let mut rel = Rel::Source {
        tid: source_tid,
        schema: Rc::clone(&source_schema),
        bound,
    };

    // WHERE — bound against the source schema (the only schema in scope).
    if let Some(where_expr) = &select.selection {
        let pred = bind_single_table(where_expr, &source_schema)?;
        rel = Rel::Filter {
            input: Box::new(rel),
            pred,
        };
    }

    // Projection. `build_projection` prepends the k PK columns, so `out_cols` is
    // `[pk…, projected payload…]` and the view PK is `0..k`. A `SELECT *` over a
    // dup-named source carries the duplicates through positionally (the wildcard
    // contract); only an explicit dup projection errors.
    let is_wildcard = is_wildcard_projection(&select.projection);
    let (items, out_cols) = build_projection(&select.projection, &source_schema)?;
    if !is_wildcard {
        reject_duplicate_column_names(&out_cols, "CREATE VIEW projection")?;
    }
    let pk_arity = source_schema.pk_count();
    rel = Rel::Project {
        input: Box::new(rel),
        items,
        out_cols,
        pk_arity,
    };
    Ok(rel)
}

/// Build an identity (pass-through) linear `Rel` over an already-resolved relation: a
/// `Project(Source)` carrying every column through unchanged (PK pinned to the front,
/// §6) — the `SELECT *` expansion of `build_projection`. Emitted as a distinct-source
/// hidden view so a repeated relation (self-join) gets its own source id — the two
/// join inputs are then never the same source.
pub(crate) fn passthrough_rel(tid: u64, schema: Rc<Schema>) -> Result<Rel, GnitzSqlError> {
    let wildcard = [SelectItem::Wildcard(WildcardAdditionalOptions::default())];
    let (items, out_cols) = build_projection(&wildcard, &schema)?;
    let pk_arity = schema.pk_count();
    Ok(Rel::Project {
        input: Box::new(Rel::Source {
            tid,
            schema: Rc::clone(&schema),
            // A pass-through wrapper has no WHERE, so no conjunct to bound on.
            bound: None,
        }),
        items,
        out_cols,
        pk_arity,
    })
}
