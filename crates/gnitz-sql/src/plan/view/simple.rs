//! The filter/map single-table view: an optional WHERE filter plus a projection
//! (pure column reorder/subset, or an expr-map when the projection derives or
//! duplicates a PK column).

use crate::ast_util::{extract_table_factor_name, is_wildcard_projection};
use crate::bind::Binder;
use crate::codec::project_schema::{build_projection, ProjItem};
use crate::error::GnitzSqlError;
use crate::lower::compile_bound_expr;
use crate::plan::validate::reject_duplicate_column_names;
use crate::SqlResult;
use gnitz_core::{CircuitBuilder, ExprBuilder, GnitzClient};

pub(crate) fn execute_create_simple_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    select: &sqlparser::ast::Select,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = extract_table_factor_name(&select.from[0].relation, "CREATE VIEW")?;

    let (source_tid, source_schema) = binder.resolve(client, &table_name)?;

    // Build filter expression (if any)
    let expr_prog = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        Some(eb.build(result_reg))
    } else {
        None
    };

    // Build projection column list
    let is_wildcard = is_wildcard_projection(&select.projection);
    let (items, out_cols) = build_projection(&select.projection, &source_schema, binder)?;

    // The pass-through generalization can now emit the same name twice (SELECT
    // name, name; or an alias landing on the auto-prepended PK). This keys on
    // output *names*, so a duplicate PK *value* under distinct names (SELECT id,
    // id AS id2) is unaffected and accepted.
    //
    // A `SELECT *` over a dup-named source (a join view surfacing both sides'
    // `id`) legitimately carries the duplicate names through positionally — the
    // same wildcard contract as `execute_create_join_view`. Gate the guard on
    // `!is_wildcard` so only an *explicit* projection that produces a duplicate
    // (`SELECT name, name`) errors.
    if !is_wildcard {
        reject_duplicate_column_names(out_cols.iter().map(|c| c.name.as_str()), "CREATE VIEW projection")?;
    }

    // Slots 0..k are the view's physical PK (carried verbatim by commit_row). A
    // payload slot (>= k) that is a PK PassThrough is a duplicate PK value; a
    // Computed is a derived column. Either forces the expr-map, which trusts the
    // planner's declared schema (node_schema = out_schema) and writes/derives
    // each payload slot explicitly — the pure projection path would silently drop
    // a PK value requested as a payload column.
    let k = source_schema.pk_count();
    let needs_expr_map = items[k..].iter().any(|item| match item {
        ProjItem::Computed { .. } => true,
        ProjItem::PassThrough { src_col } => source_schema.is_pk_col(*src_col),
    });

    // Allocate view_id
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;

    // Build circuit
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, Some(p)),
        None => inp,
    };

    let out_node = if needs_expr_map {
        // Emit only the payload slots (k..); the k physical PK columns are carried
        // by commit_row and must not appear in the program. payload_idx is the
        // dense output payload position, matching out_cols[k + payload_idx].
        let mut eb = ExprBuilder::new();
        for (payload_idx, item) in items[k..].iter().enumerate() {
            let payload_idx = payload_idx as u32;
            match item {
                ProjItem::PassThrough { src_col } => {
                    let tc = source_schema.columns[*src_col].type_code as u32;
                    eb.copy_col(tc, *src_col as u32, payload_idx);
                }
                ProjItem::Computed { bound_expr, .. } => {
                    let (reg, _) = compile_bound_expr(bound_expr, &source_schema, &mut eb)?;
                    // EMIT writes the raw register bits via append_int — correct for float.
                    eb.emit_col(reg, payload_idx);
                }
            }
        }
        let program = eb.build(0); // result_reg unused — EMIT/COPY_COL write directly
        cb.map_expr(filtered, program)
    } else if items.len() < source_schema.columns.len()
        || items.iter().enumerate().any(|(i, item)| match item {
            ProjItem::PassThrough { src_col } => *src_col != i,
            _ => false,
        })
    {
        // Pure column reorder/subset — every payload item is a non-PK
        // pass-through, so build_map_output_schema reproduces out_cols (PK region
        // in pk_indices() order, then non-PK cols in projection order). Pass only
        // the payload items (`items[k..]`): the k PK slots are inherited verbatim
        // by execute_map's bulk PK copy / build_map_output_schema's PK prepend.
        // Including them would emit one ColMove per PK index with dst_payload set
        // to the enumeration index, shifting every payload destination out of range
        // (single-PK: payload OOB; compound-PK: SENTINEL stride past num_payload).
        let cols: Vec<usize> = items[k..]
            .iter()
            .filter_map(|i| match i {
                ProjItem::PassThrough { src_col } => Some(*src_col),
                _ => None,
            })
            .collect();
        cb.map(filtered, &cols)
    } else {
        // Identity — PK already at front, full width, no map needed.
        filtered
    };

    cb.sink(out_node);
    let circuit = cb.build();

    // The view's physical PK is the leading k columns (the source PK passed
    // through in pk_indices() order). Persist exactly that.
    let view_pk: Vec<u32> = (0..k as u32).collect();
    client
        .create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols, &view_pk)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}
