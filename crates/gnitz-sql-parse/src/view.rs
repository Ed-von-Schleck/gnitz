//! View compile — the heart of Phase 1B chunk 3.
//!
//! Takes a parsed SELECT query plus a `CatalogResolver`-backed `Binder`
//! and returns a `CircuitGraph` ready to be bincoded into a migration
//! row's `compiled_circuits`. Mirrors the arms in `gnitz_sql::planner`
//! exactly — the differences are surface-level:
//!
//! * Table-id allocation is out of line (`view_id` is passed in).
//! * No `GnitzClient`; resolution goes through the binder.
//! * Returns `CircuitGraph` instead of calling `client.create_view_…`.
//!
//! The client-side planner is left alone in Phase 1; Phase 4/5 is where
//! the client arms collapse into thin `execute_sql` forwarders.

use std::collections::HashMap;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, Query, SelectItem, SetExpr, SetOperator,
    SetQuantifier, TableFactor,
};

use gnitz_protocol::{ColumnDef, Schema, TypeCode};
use gnitz_wire::{AGG_COUNT, AGG_COUNT_NON_NULL, AGG_MAX, AGG_MIN, AGG_SUM};

use crate::binder::{
    resolve_qualified_column, resolve_unqualified_column, Binder,
};
use crate::circuit::{CircuitBuilder, CircuitGraph};
use crate::error::{extract_table_factor_name, GnitzSqlParseError};
use crate::expr_compile::compile_bound_expr;
use crate::expr_program::ExprBuilder;
use crate::plan_types::{AggFunc, BinOp, BoundExpr};
use crate::resolver::CatalogResolver;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Compile a view's SELECT query into a `CircuitGraph`.
pub fn compile_view_query<R: CatalogResolver + ?Sized>(
    view_id:  u64,
    query:    &Query,
    sql_text: &str,
    binder:   &mut Binder<R>,
) -> Result<CircuitGraph, GnitzSqlParseError> {
    if query.order_by.is_some() {
        return Err(GnitzSqlParseError::Unsupported("ORDER BY not supported".into()));
    }

    // Inline CTEs into the binder's alias cache.
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(GnitzSqlParseError::Unsupported(
                "recursive CTEs not supported".into(),
            ));
        }
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.clone();
            let cte_select = match cte.query.body.as_ref() {
                SetExpr::Select(s) => s,
                _ => return Err(GnitzSqlParseError::Unsupported(
                    format!("CTE '{}': only SELECT supported", cte_name),
                )),
            };
            if cte_select.from.len() != 1 || !cte_select.from[0].joins.is_empty() {
                return Err(GnitzSqlParseError::Unsupported(
                    format!("CTE '{}': only single table without JOINs", cte_name),
                ));
            }
            let cte_table_name = extract_table_factor_name(
                &cte_select.from[0].relation,
                &format!("CTE '{}'", cte_name),
            )?;
            let resolved = binder.resolve(&cte_table_name)?;
            binder.cache_alias(cte_name, resolved);
        }
    }

    match query.body.as_ref() {
        SetExpr::SetOperation { op, set_quantifier, left, right } => {
            compile_set_op_view(
                view_id, *op, *set_quantifier, left, right, binder,
            )
        }
        SetExpr::Select(_) => {
            let select = match query.body.as_ref() {
                SetExpr::Select(s) => s,
                _ => unreachable!(),
            };
            if select.distinct.is_some() {
                compile_distinct_view(view_id, select, binder)
            } else if select.from.len() != 1 {
                Err(GnitzSqlParseError::Unsupported(
                    "CREATE VIEW: only single FROM item supported".into(),
                ))
            } else if !select.from[0].joins.is_empty() {
                compile_join_view(view_id, sql_text, select, binder, query)
            } else {
                let has_group_by = matches!(
                    &select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty(),
                );
                if has_group_by {
                    compile_group_by_view(view_id, select, binder)
                } else {
                    compile_plain_view(view_id, select, binder)
                }
            }
        }
        _ => Err(GnitzSqlParseError::Unsupported(
            "CREATE VIEW only supports SELECT".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Plain single-table view (filter + projection)
// ---------------------------------------------------------------------------

fn compile_plain_view<R: CatalogResolver + ?Sized>(
    view_id: u64,
    select:  &sqlparser::ast::Select,
    binder:  &mut Binder<R>,
) -> Result<CircuitGraph, GnitzSqlParseError> {
    let table_name = extract_table_factor_name(&select.from[0].relation, "CREATE VIEW")?;
    let (source_tid, source_schema) = binder.resolve(&table_name)?;

    let expr_prog = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        Some(eb.build(result_reg))
    } else {
        None
    };

    let (items, out_cols) = build_projection(&select.projection, &source_schema, binder)?;
    let has_computed = items.iter().any(|i| matches!(i, ProjectionItem::Computed { .. }));

    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, Some(p)),
        None    => inp,
    };

    let out_node = if has_computed {
        let mut eb = ExprBuilder::new();
        let mut payload_idx = 0u32;
        for item in &items {
            if is_pk_item(item, &source_schema) { continue; }
            match item {
                ProjectionItem::PassThrough { src_col } => {
                    let tc = source_schema.columns[*src_col].type_code as u32;
                    eb.copy_col(tc, *src_col as u32, payload_idx);
                }
                ProjectionItem::Computed { bound_expr, .. } => {
                    let (reg, _is_float) = compile_bound_expr(bound_expr, &source_schema, &mut eb)?;
                    eb.emit_col(reg, payload_idx);
                }
            }
            payload_idx += 1;
        }
        let program = eb.build(0);
        cb.map_expr(filtered, program)
    } else if items.len() < source_schema.columns.len()
           || items.iter().enumerate().any(|(i, item)| match item {
               ProjectionItem::PassThrough { src_col } => *src_col != i,
               _ => false,
           })
    {
        let cols: Vec<usize> = items.iter().filter_map(|i| match i {
            ProjectionItem::PassThrough { src_col } => Some(*src_col),
            _ => None,
        }).collect();
        cb.map(filtered, &cols)
    } else {
        filtered
    };

    cb.sink(out_node);
    Ok(cb.build(col_defs_to_pairs(&out_cols)))
}

// ---------------------------------------------------------------------------
// Projection helpers
// ---------------------------------------------------------------------------

enum ProjectionItem {
    PassThrough { src_col: usize },
    Computed    { bound_expr: BoundExpr, _out_type: TypeCode },
}

fn is_pk_item(item: &ProjectionItem, schema: &Schema) -> bool {
    matches!(item, ProjectionItem::PassThrough { src_col } if *src_col == schema.pk_index)
}

fn build_projection<R: CatalogResolver + ?Sized>(
    projection:    &[SelectItem],
    source_schema: &Schema,
    binder:        &Binder<R>,
) -> Result<(Vec<ProjectionItem>, Vec<ColumnDef>), GnitzSqlParseError> {
    let is_wildcard = projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    if is_wildcard {
        let items: Vec<ProjectionItem> = (0..source_schema.columns.len())
            .map(|i| ProjectionItem::PassThrough { src_col: i })
            .collect();
        return Ok((items, source_schema.columns.clone()));
    }

    let mut items:    Vec<ProjectionItem> = Vec::new();
    let mut out_cols: Vec<ColumnDef>      = Vec::new();

    for (idx, item) in projection.iter().enumerate() {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let col_idx = source_schema.columns.iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                    .ok_or_else(|| GnitzSqlParseError::Bind(
                        format!("column '{}' not found", ident.value),
                    ))?;
                items.push(ProjectionItem::PassThrough { src_col: col_idx });
                out_cols.push(source_schema.columns[col_idx].clone());
            }
            SelectItem::Wildcard(_) => {
                for i in 0..source_schema.columns.len() {
                    items.push(ProjectionItem::PassThrough { src_col: i });
                    out_cols.push(source_schema.columns[i].clone());
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let bound = binder.bind_expr(expr, source_schema)?;
                let out_type = bound.infer_type(source_schema);
                items.push(ProjectionItem::Computed { bound_expr: bound, _out_type: out_type });
                out_cols.push(ColumnDef {
                    name: alias.value.clone(), type_code: out_type,
                    is_nullable: true, fk_table_id: 0, fk_col_idx: 0,
                });
            }
            SelectItem::UnnamedExpr(expr) => {
                let bound = binder.bind_expr(expr, source_schema)?;
                let out_type = bound.infer_type(source_schema);
                let col_name = format!("_expr{}", idx);
                items.push(ProjectionItem::Computed { bound_expr: bound, _out_type: out_type });
                out_cols.push(ColumnDef {
                    name: col_name, type_code: out_type,
                    is_nullable: true, fk_table_id: 0, fk_col_idx: 0,
                });
            }
            _ => return Err(GnitzSqlParseError::Unsupported(
                "unsupported SELECT item in CREATE VIEW projection".into(),
            )),
        }
    }

    // Ensure PK is present (as PassThrough) and first.
    let pk = source_schema.pk_index;
    let pk_pos = items.iter()
        .position(|i| matches!(i, ProjectionItem::PassThrough { src_col } if *src_col == pk));
    match pk_pos {
        Some(0) => { }
        Some(pos) => {
            items.swap(0, pos);
            out_cols.swap(0, pos);
        }
        None => {
            let pk_computed = items.iter().any(|i| {
                if let ProjectionItem::Computed { bound_expr, .. } = i {
                    matches!(bound_expr, BoundExpr::ColRef(c) if *c == pk)
                } else { false }
            });
            if pk_computed {
                return Err(GnitzSqlParseError::Unsupported(
                    "PK column cannot be a computed expression in CREATE VIEW".into(),
                ));
            }
            items.insert(0, ProjectionItem::PassThrough { src_col: pk });
            out_cols.insert(0, source_schema.columns[pk].clone());
        }
    }
    Ok((items, out_cols))
}

fn col_defs_to_pairs(cols: &[ColumnDef]) -> Vec<(String, u8)> {
    cols.iter().map(|c| (c.name.clone(), c.type_code as u8)).collect()
}

// ---------------------------------------------------------------------------
// Equijoin view
// ---------------------------------------------------------------------------

fn compile_join_view<R: CatalogResolver + ?Sized>(
    view_id: u64,
    _sql_text: &str,
    select:  &sqlparser::ast::Select,
    binder:  &mut Binder<R>,
    _query:  &Query,
) -> Result<CircuitGraph, GnitzSqlParseError> {
    let left_name = extract_table_factor_name(&select.from[0].relation, "CREATE VIEW JOIN")?;
    let left_alias = match &select.from[0].relation {
        TableFactor::Table { alias: Some(a), .. } => a.name.value.clone(),
        _ => left_name.clone(),
    };

    if select.from[0].joins.len() != 1 {
        return Err(GnitzSqlParseError::Unsupported(
            "CREATE VIEW: only single JOIN supported".into(),
        ));
    }
    let join = &select.from[0].joins[0];

    let (right_name, right_alias) = match &join.relation {
        TableFactor::Table { name, alias, .. } => {
            let n = name.0.last()
                .and_then(|p| p.as_ident()).map(|i| i.value.clone())
                .ok_or_else(|| GnitzSqlParseError::Bind("empty right table name".into()))?;
            let a = alias.as_ref().map(|al| al.name.value.clone()).unwrap_or_else(|| n.clone());
            (n, a)
        }
        _ => return Err(GnitzSqlParseError::Unsupported(
            "CREATE VIEW JOIN: only simple table reference".into(),
        )),
    };

    let (on_expr, is_left_join) = match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr))
        | JoinOperator::Join (JoinConstraint::On(expr)) => (expr, false),
        JoinOperator::LeftOuter(JoinConstraint::On(expr))
        | JoinOperator::Left     (JoinConstraint::On(expr)) => (expr, true),
        _ => return Err(GnitzSqlParseError::Unsupported(
            "CREATE VIEW: only INNER JOIN / LEFT JOIN ... ON supported".into(),
        )),
    };

    let (left_tid,  left_schema)  = binder.resolve(&left_name)?;
    let (right_tid, right_schema) = binder.resolve(&right_name)?;

    let mut alias_map: HashMap<String, (u64, Schema, usize)> = HashMap::new();
    alias_map.insert(left_alias.to_lowercase(),  (left_tid,  (*left_schema).clone(), 0));
    alias_map.insert(right_alias.to_lowercase(), (right_tid, (*right_schema).clone(), left_schema.columns.len()));

    let (left_join_col, right_join_col) = extract_equijoin_keys(
        on_expr, &left_schema, &alias_map,
    )?;

    let left_reindex_prog  = build_reindex_program(&left_schema);
    let right_reindex_prog = build_reindex_program(&right_schema);

    let mut cb = CircuitBuilder::new(view_id, 0);
    let input_a = cb.input_delta_tagged(left_tid);
    let input_b = cb.input_delta_tagged(right_tid);
    let reindex_a = cb.map_reindex(input_a, left_join_col,  left_reindex_prog);
    let reindex_b = cb.map_reindex(input_b, right_join_col, right_reindex_prog);
    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b);
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a);

    let left_n  = left_schema.columns.len();
    let right_n = right_schema.columns.len();

    let proj_ab: Vec<usize> = (1..1 + left_n + right_n).collect();
    let proj_ab_node = cb.map(join_ab, &proj_ab);

    let mut proj_ba: Vec<usize> = Vec::new();
    for i in 0..left_n  { proj_ba.push(1 + right_n + i); }
    for i in 0..right_n { proj_ba.push(1 + i); }
    let proj_ba_node = cb.map(join_ba, &proj_ba);

    let inner_merged = cb.union(proj_ab_node, proj_ba_node);

    let merged = if is_left_join {
        let right_col_tcs: Vec<u64> = right_schema.columns.iter()
            .map(|c| c.type_code as u64).collect();

        let key_only_b = cb.map_key_only(reindex_b);
        let distinct_b = cb.distinct(key_only_b);
        let trace_db   = cb.integrate_trace(distinct_b);

        let antijoin_a    = cb.anti_join_with_trace_node(reindex_a, trace_db);
        let null_filled_a = cb.null_extend(antijoin_a, &right_col_tcs);

        let correction_raw        = cb.join_with_trace_node(distinct_b, trace_a);
        let correction            = cb.negate(correction_raw);
        let null_filled_correction = cb.null_extend(correction, &right_col_tcs);

        let all_null_fills = cb.union(null_filled_a, null_filled_correction);
        cb.union(inner_merged, all_null_fills)
    } else {
        inner_merged
    };

    let mut out_cols: Vec<ColumnDef> = Vec::new();
    out_cols.push(ColumnDef {
        name: "_join_pk".into(), type_code: TypeCode::U128,
        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
    });
    for col in &left_schema.columns {
        out_cols.push(col.clone());
    }
    for col in &right_schema.columns {
        let mut c = col.clone();
        if is_left_join { c.is_nullable = true; }
        out_cols.push(c);
    }

    let is_wildcard = select.projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    let (final_cols, final_projection) = if is_wildcard {
        let proj: Vec<usize> = (1..1 + left_n + right_n).collect();
        (out_cols, proj)
    } else {
        let mut cols = Vec::new();
        let mut proj = Vec::new();
        cols.push(out_cols[0].clone());
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let idx = resolve_unqualified_column(&ident.value, &alias_map)?;
                    cols.push(out_cols[1 + idx].clone());
                    proj.push(idx + 1);
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                    let idx = resolve_qualified_column(
                        &parts[0].value, &parts[1].value, &alias_map,
                    )?;
                    cols.push(out_cols[1 + idx].clone());
                    proj.push(idx + 1);
                }
                SelectItem::Wildcard(_) => {
                    for i in 1..out_cols.len() {
                        cols.push(out_cols[i].clone());
                        proj.push(i);
                    }
                }
                _ => return Err(GnitzSqlParseError::Unsupported(
                    "unsupported SELECT item in JOIN view".into(),
                )),
            }
        }
        (cols, proj)
    };

    let is_identity = final_projection.len() == left_n + right_n
        && final_projection.iter().enumerate().all(|(i, &p)| p == i + 1);
    let sink_input = if is_identity { merged } else { cb.map(merged, &final_projection) };
    cb.sink(sink_input);
    Ok(cb.build(col_defs_to_pairs(&final_cols)))
}

fn build_reindex_program(schema: &Schema) -> crate::expr_program::ExprProgram {
    let mut eb = ExprBuilder::new();
    let mut payload_idx = 0u32;
    for (ci, col) in schema.columns.iter().enumerate() {
        let tc = col.type_code as u32;
        eb.copy_col(tc, ci as u32, payload_idx);
        payload_idx += 1;
    }
    eb.build(0)
}

fn extract_equijoin_keys(
    on_expr:     &Expr,
    left_schema: &Schema,
    alias_map:   &HashMap<String, (u64, Schema, usize)>,
) -> Result<(usize, usize), GnitzSqlParseError> {
    match on_expr {
        Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Eq, right } => {
            let l_col = resolve_join_col_ref(left,  alias_map)?;
            let r_col = resolve_join_col_ref(right, alias_map)?;
            let left_n = left_schema.columns.len();
            let (left_idx, right_idx) = if l_col < left_n && r_col >= left_n {
                (l_col, r_col - left_n)
            } else if r_col < left_n && l_col >= left_n {
                (r_col, l_col - left_n)
            } else {
                return Err(GnitzSqlParseError::Bind(
                    "JOIN ON: each side of = must reference a different table".into(),
                ));
            };
            Ok((left_idx, right_idx))
        }
        _ => Err(GnitzSqlParseError::Unsupported(
            "JOIN ON: only simple equijoin (col = col) supported".into(),
        )),
    }
}

fn resolve_join_col_ref(
    expr: &Expr, alias_map: &HashMap<String, (u64, Schema, usize)>,
) -> Result<usize, GnitzSqlParseError> {
    match expr {
        Expr::Identifier(ident)
            => resolve_unqualified_column(&ident.value, alias_map),
        Expr::CompoundIdentifier(parts) if parts.len() == 2
            => resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map),
        _ => Err(GnitzSqlParseError::Unsupported(
            "JOIN ON: only column references supported".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// GROUP BY view
// ---------------------------------------------------------------------------

struct AggMapping {
    specs_start:  usize,
    specs_count:  usize,
    is_avg:       bool,
    output_name:  String,
    output_type:  TypeCode,
}

enum GroupBySelectItem {
    GroupCol { src_col: usize, name: String },
    Aggregate { agg_idx: usize },
}

fn compile_group_by_view<R: CatalogResolver + ?Sized>(
    view_id: u64,
    select:  &sqlparser::ast::Select,
    binder:  &mut Binder<R>,
) -> Result<CircuitGraph, GnitzSqlParseError> {
    let table_name = extract_table_factor_name(&select.from[0].relation, "GROUP BY")?;
    let (source_tid, source_schema) = binder.resolve(&table_name)?;

    let group_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs,
        _ => return Err(GnitzSqlParseError::Unsupported(
            "GROUP BY: only expression list supported".into(),
        )),
    };
    let mut group_col_indices: Vec<usize> = Vec::new();
    for ge in group_exprs {
        match ge {
            Expr::Identifier(id) => {
                let idx = source_schema.columns.iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&id.value))
                    .ok_or_else(|| GnitzSqlParseError::Bind(
                        format!("GROUP BY column '{}' not found", id.value),
                    ))?;
                group_col_indices.push(idx);
            }
            _ => return Err(GnitzSqlParseError::Unsupported(
                "GROUP BY: only simple column references supported".into(),
            )),
        }
    }

    let mut agg_mappings: Vec<AggMapping> = Vec::new();
    let mut select_items: Vec<GroupBySelectItem> = Vec::new();
    let mut agg_specs: Vec<(u64, usize)> = Vec::new();

    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::UnnamedExpr(expr)             => (expr, None),
            _ => return Err(GnitzSqlParseError::Unsupported(
                "GROUP BY: unsupported SELECT item".into(),
            )),
        };
        let bound = binder.bind_expr(expr, &source_schema)?;
        match &bound {
            BoundExpr::ColRef(col_idx) => {
                if !group_col_indices.contains(col_idx) {
                    return Err(GnitzSqlParseError::Plan(format!(
                        "column '{}' must appear in GROUP BY or an aggregate function",
                        source_schema.columns[*col_idx].name,
                    )));
                }
                let name = alias.unwrap_or_else(|| source_schema.columns[*col_idx].name.clone());
                select_items.push(GroupBySelectItem::GroupCol { src_col: *col_idx, name });
            }
            BoundExpr::AggCall { func, arg } => {
                let src_col = match arg {
                    Some(a) => match a.as_ref() {
                        BoundExpr::ColRef(c) => Some(*c),
                        _ => return Err(GnitzSqlParseError::Unsupported(
                            "aggregate on computed expression not supported".into(),
                        )),
                    },
                    None => None,
                };
                let agg_idx = agg_mappings.len();
                let start   = agg_specs.len();
                let (out_name, out_type, is_avg) = match func {
                    AggFunc::Count => {
                        agg_specs.push((AGG_COUNT as u64, 0));
                        (alias.unwrap_or_else(|| format!("_count{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::CountNonNull => {
                        agg_specs.push((AGG_COUNT_NON_NULL as u64, src_col.unwrap()));
                        (alias.unwrap_or_else(|| format!("_count{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::Sum => {
                        agg_specs.push((AGG_SUM as u64, src_col.unwrap()));
                        (alias.unwrap_or_else(|| format!("_sum{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::Min => {
                        agg_specs.push((AGG_MIN as u64, src_col.unwrap()));
                        (alias.unwrap_or_else(|| format!("_min{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::Max => {
                        agg_specs.push((AGG_MAX as u64, src_col.unwrap()));
                        (alias.unwrap_or_else(|| format!("_max{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::Avg => {
                        let col = src_col.unwrap();
                        agg_specs.push((AGG_SUM as u64, col));
                        agg_specs.push((AGG_COUNT_NON_NULL as u64, col));
                        (alias.unwrap_or_else(|| format!("_avg{}", idx)), TypeCode::F64, true)
                    }
                };
                let count = agg_specs.len() - start;
                agg_mappings.push(AggMapping {
                    specs_start: start, specs_count: count, is_avg,
                    output_name: out_name, output_type: out_type,
                });
                select_items.push(GroupBySelectItem::Aggregate { agg_idx });
            }
            _ => return Err(GnitzSqlParseError::Plan(
                "GROUP BY SELECT: only column refs and aggregates supported".into(),
            )),
        }
    }

    let use_natural_pk = group_col_indices.len() == 1 && matches!(
        source_schema.columns[group_col_indices[0]].type_code,
        TypeCode::U64 | TypeCode::U128,
    );
    let agg_col_offset = if use_natural_pk { 1 } else { 1 + group_col_indices.len() };

    let mut reduce_schema_cols: Vec<ColumnDef> = Vec::new();
    if use_natural_pk {
        reduce_schema_cols.push(source_schema.columns[group_col_indices[0]].clone());
    } else {
        reduce_schema_cols.push(ColumnDef {
            name: "_group_pk".into(), type_code: TypeCode::U128,
            is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        });
        for &gi in &group_col_indices {
            reduce_schema_cols.push(source_schema.columns[gi].clone());
        }
    }
    for _ in 0..agg_specs.len() {
        reduce_schema_cols.push(ColumnDef {
            name: "_agg".into(), type_code: TypeCode::I64,
            is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        });
    }
    let reduce_schema = Schema { columns: reduce_schema_cols, pk_index: 0 };

    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();

    let filtered = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        let prog = eb.build(result_reg);
        cb.filter(inp, Some(prog))
    } else {
        inp
    };

    let reduced = cb.reduce_multi(filtered, &group_col_indices, &agg_specs);

    let mut post_map_eb = ExprBuilder::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    let mut payload_idx: u32 = 0;

    out_cols.push(reduce_schema.columns[0].clone());

    for si in &select_items {
        match si {
            GroupBySelectItem::GroupCol { src_col, name } => {
                let reduce_col = if use_natural_pk {
                    0
                } else {
                    1 + group_col_indices.iter().position(|&gi| gi == *src_col).unwrap()
                };
                let tc = reduce_schema.columns[reduce_col].type_code;
                post_map_eb.copy_col(tc as u32, reduce_col as u32, payload_idx);
                out_cols.push(ColumnDef {
                    name: name.clone(), type_code: tc,
                    is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
                });
                payload_idx += 1;
            }
            GroupBySelectItem::Aggregate { agg_idx } => {
                let m = &agg_mappings[*agg_idx];
                if m.is_avg {
                    let sum_col = (agg_col_offset + m.specs_start) as u32;
                    let cnt_col = (agg_col_offset + m.specs_start + 1) as u32;
                    let sum_reg = post_map_eb.load_col_int(sum_col as usize);
                    let cnt_reg = post_map_eb.load_col_int(cnt_col as usize);
                    let sum_f   = post_map_eb.int_to_float(sum_reg);
                    let cnt_f   = post_map_eb.int_to_float(cnt_reg);
                    let avg_reg = post_map_eb.float_div(sum_f, cnt_f);
                    post_map_eb.emit_col(avg_reg, payload_idx);
                    out_cols.push(ColumnDef {
                        name: m.output_name.clone(), type_code: TypeCode::F64,
                        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
                    });
                    payload_idx += 1;
                } else {
                    let agg_col = (agg_col_offset + m.specs_start) as u32;
                    let tc      = reduce_schema.columns[agg_col as usize].type_code;
                    post_map_eb.copy_col(tc as u32, agg_col, payload_idx);
                    out_cols.push(ColumnDef {
                        name: m.output_name.clone(), type_code: m.output_type,
                        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
                    });
                    payload_idx += 1;
                }
                let _ = m.specs_count;
            }
        }
    }

    let post_map_prog = post_map_eb.build(0);
    let mapped = cb.map_expr(reduced, post_map_prog);

    let post_map_schema = Schema { columns: out_cols.clone(), pk_index: 0 };

    let having_input = if let Some(having_expr) = &select.having {
        let bound = bind_having_expr(
            having_expr, &post_map_schema, &source_schema,
            &group_col_indices, &agg_mappings, &select_items, use_natural_pk,
        )?;
        let mut heb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &post_map_schema, &mut heb)?;
        let prog = heb.build(result_reg);
        cb.filter(mapped, Some(prog))
    } else {
        mapped
    };

    cb.sink(having_input);
    Ok(cb.build(col_defs_to_pairs(&out_cols)))
}

fn bind_having_expr(
    expr:             &Expr,
    post_map_schema:  &Schema,
    source_schema:    &Schema,
    group_col_indices: &[usize],
    agg_mappings:     &[AggMapping],
    select_items:     &[GroupBySelectItem],
    use_natural_pk:   bool,
) -> Result<BoundExpr, GnitzSqlParseError> {
    match expr {
        Expr::Identifier(ident) => {
            let col_name = &ident.value;
            let idx = post_map_schema.columns.iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| GnitzSqlParseError::Bind(
                    format!("HAVING: column '{}' not found in output", col_name),
                ))?;
            Ok(BoundExpr::ColRef(idx))
        }
        Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            let arg_col = extract_func_arg_col(func, source_schema)?;
            let agg_func = match name.as_str() {
                "count" => if arg_col.is_none() { AggFunc::Count } else { AggFunc::CountNonNull },
                "sum" => AggFunc::Sum, "min" => AggFunc::Min,
                "max" => AggFunc::Max, "avg" => AggFunc::Avg,
                _ => return Err(GnitzSqlParseError::Unsupported(
                    format!("HAVING: function '{}' not supported", name),
                )),
            };
            for si in select_items {
                if let GroupBySelectItem::Aggregate { agg_idx } = si {
                    let m = &agg_mappings[*agg_idx];
                    let matches = match (agg_func, m.is_avg) {
                        (AggFunc::Avg, true)  => true,
                        (AggFunc::Count, false) if m.output_type == TypeCode::I64 => m.specs_count == 1,
                        _ => !m.is_avg,
                    };
                    if matches {
                        let out_idx = post_map_schema.columns.iter()
                            .position(|c| c.name == m.output_name).unwrap();
                        return Ok(BoundExpr::ColRef(out_idx));
                    }
                }
            }
            Err(GnitzSqlParseError::Bind(format!(
                "HAVING: aggregate {}({}) not found in SELECT", name,
                arg_col.map_or("*".to_string(), |c| source_schema.columns[c].name.clone()),
            )))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = bind_having_expr(left, post_map_schema, source_schema,
                group_col_indices, agg_mappings, select_items, use_natural_pk)?;
            let r = bind_having_expr(right, post_map_schema, source_schema,
                group_col_indices, agg_mappings, select_items, use_natural_pk)?;
            let bop = match op {
                sqlparser::ast::BinaryOperator::Plus  => BinOp::Add,
                sqlparser::ast::BinaryOperator::Minus => BinOp::Sub,
                sqlparser::ast::BinaryOperator::Eq    => BinOp::Eq,
                sqlparser::ast::BinaryOperator::NotEq => BinOp::Ne,
                sqlparser::ast::BinaryOperator::Gt    => BinOp::Gt,
                sqlparser::ast::BinaryOperator::GtEq  => BinOp::Ge,
                sqlparser::ast::BinaryOperator::Lt    => BinOp::Lt,
                sqlparser::ast::BinaryOperator::LtEq  => BinOp::Le,
                sqlparser::ast::BinaryOperator::And   => BinOp::And,
                sqlparser::ast::BinaryOperator::Or    => BinOp::Or,
                op => return Err(GnitzSqlParseError::Unsupported(
                    format!("HAVING: operator {:?} not supported", op),
                )),
            };
            Ok(BoundExpr::BinOp(Box::new(l), bop, Box::new(r)))
        }
        Expr::Value(vws) => match &vws.value {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() { Ok(BoundExpr::LitInt(i)) }
                else if let Ok(f) = n.parse::<f64>() { Ok(BoundExpr::LitFloat(f)) }
                else { Err(GnitzSqlParseError::Bind(format!("HAVING: invalid number {}", n))) }
            }
            _ => Err(GnitzSqlParseError::Unsupported("HAVING: unsupported value type".into())),
        },
        Expr::Nested(inner) => bind_having_expr(
            inner, post_map_schema, source_schema,
            group_col_indices, agg_mappings, select_items, use_natural_pk,
        ),
        _ => Err(GnitzSqlParseError::Unsupported(
            format!("HAVING: unsupported expression {:?}", expr),
        )),
    }
}

fn extract_func_arg_col(
    func: &sqlparser::ast::Function, schema: &Schema,
) -> Result<Option<usize>, GnitzSqlParseError> {
    match &func.args {
        FunctionArguments::List(list) if list.args.len() == 1 => match &list.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                let idx = schema.columns.iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&id.value))
                    .ok_or_else(|| GnitzSqlParseError::Bind(
                        format!("column '{}' not found", id.value),
                    ))?;
                Ok(Some(idx))
            }
            _ => Err(GnitzSqlParseError::Unsupported(
                "HAVING: unsupported function argument".into(),
            )),
        },
        _ => Err(GnitzSqlParseError::Unsupported(
            "HAVING: function requires one argument".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Set operations
// ---------------------------------------------------------------------------

fn compile_set_op_side<R: CatalogResolver + ?Sized>(
    select: &sqlparser::ast::Select,
    binder: &mut Binder<R>,
    cb:     &mut CircuitBuilder,
) -> Result<(u64, Vec<ColumnDef>), GnitzSqlParseError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlParseError::Unsupported(
            "set operation: each side must be a simple SELECT from one table".into(),
        ));
    }
    let table_name = extract_table_factor_name(&select.from[0].relation, "set operation")?;
    let (source_tid, source_schema) = binder.resolve(&table_name)?;

    let inp = cb.input_delta_tagged(source_tid);

    let filtered = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        let prog = eb.build(result_reg);
        cb.filter(inp, Some(prog))
    } else {
        inp
    };

    let prog = build_reindex_program(&source_schema);
    let reindexed = cb.map_reindex(filtered, source_schema.pk_index, prog);

    Ok((reindexed, source_schema.columns.clone()))
}

fn compile_set_op_view<R: CatalogResolver + ?Sized>(
    view_id:        u64,
    op:             SetOperator,
    set_quantifier: SetQuantifier,
    left:           &SetExpr,
    right:          &SetExpr,
    binder:         &mut Binder<R>,
) -> Result<CircuitGraph, GnitzSqlParseError> {
    let left_select = match left {
        SetExpr::Select(s) => s,
        _ => return Err(GnitzSqlParseError::Unsupported(
            "set operation: left side must be a SELECT".into(),
        )),
    };
    let right_select = match right {
        SetExpr::Select(s) => s,
        _ => return Err(GnitzSqlParseError::Unsupported(
            "set operation: right side must be a SELECT".into(),
        )),
    };

    let mut cb = CircuitBuilder::new(view_id, 0);
    let (left_node,  left_cols)  = compile_set_op_side(left_select,  binder, &mut cb)?;
    let (right_node, right_cols) = compile_set_op_side(right_select, binder, &mut cb)?;

    if left_cols.len() != right_cols.len() {
        return Err(GnitzSqlParseError::Plan(format!(
            "set operation: column count mismatch ({} vs {})",
            left_cols.len(), right_cols.len(),
        )));
    }

    let out_node = match (op, set_quantifier) {
        (SetOperator::Union, SetQuantifier::All) => cb.union(left_node, right_node),
        (SetOperator::Union, _) => {
            let merged = cb.union(left_node, right_node);
            cb.distinct(merged)
        }
        (SetOperator::Intersect, _) => {
            let trace_l = cb.integrate_trace(left_node);
            let trace_r = cb.integrate_trace(right_node);
            let semi_lr = cb.semi_join_with_trace_node(left_node,  trace_r);
            let semi_rl = cb.semi_join_with_trace_node(right_node, trace_l);
            cb.union(semi_lr, semi_rl)
        }
        (SetOperator::Except, _) => {
            let trace_l = cb.integrate_trace(left_node);
            let trace_r = cb.integrate_trace(right_node);
            let except_lr  = cb.anti_join_with_trace_node(left_node, trace_r);
            let semi_rl    = cb.semi_join_with_trace_node(right_node, trace_l);
            let correction = cb.negate(semi_rl);
            cb.union(except_lr, correction)
        }
        _ => return Err(GnitzSqlParseError::Unsupported(
            format!("set operation {:?} not supported", op),
        )),
    };

    cb.sink(out_node);

    let mut out_cols_final: Vec<ColumnDef> = Vec::new();
    out_cols_final.push(ColumnDef {
        name: "_set_pk".into(), type_code: TypeCode::U128,
        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
    });
    for col in &left_cols { out_cols_final.push(col.clone()); }

    Ok(cb.build(col_defs_to_pairs(&out_cols_final)))
}

// ---------------------------------------------------------------------------
// SELECT DISTINCT
// ---------------------------------------------------------------------------

fn compile_distinct_view<R: CatalogResolver + ?Sized>(
    view_id: u64,
    select:  &sqlparser::ast::Select,
    binder:  &mut Binder<R>,
) -> Result<CircuitGraph, GnitzSqlParseError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlParseError::Unsupported(
            "SELECT DISTINCT: only single table without JOINs".into(),
        ));
    }
    let table_name = extract_table_factor_name(&select.from[0].relation, "SELECT DISTINCT")?;
    let (source_tid, source_schema) = binder.resolve(&table_name)?;

    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();

    let filtered = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        let prog = eb.build(result_reg);
        cb.filter(inp, Some(prog))
    } else {
        inp
    };

    let prog = build_reindex_program(&source_schema);
    let reindexed = cb.map_reindex(filtered, source_schema.pk_index, prog);
    let distinct_node = cb.distinct(reindexed);

    cb.sink(distinct_node);

    let mut out_cols: Vec<ColumnDef> = Vec::new();
    out_cols.push(ColumnDef {
        name: "_distinct_pk".into(), type_code: TypeCode::U128,
        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
    });
    for col in &source_schema.columns { out_cols.push(col.clone()); }

    Ok(cb.build(col_defs_to_pairs(&out_cols)))
}

/// Parse a view body (a SELECT) and compile it. Convenience entry
/// point for `gnitz-engine::sqlc`.
pub fn compile_view_from_sql<R: CatalogResolver + ?Sized>(
    view_id:  u64,
    sql:      &str,
    binder:   &mut Binder<R>,
) -> Result<CircuitGraph, GnitzSqlParseError> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use sqlparser::ast::Statement;

    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql)
        .map_err(GnitzSqlParseError::Parse)?;

    // view.sql in DesiredState is produced via `query.to_string()`, so
    // sqlparser should round-trip into a single Statement::Query.
    if stmts.len() != 1 {
        return Err(GnitzSqlParseError::Unsupported(format!(
            "view body must be exactly one SELECT, found {}", stmts.len(),
        )));
    }
    let query = match stmts.remove(0) {
        Statement::Query(q) => q,
        other => return Err(GnitzSqlParseError::Unsupported(format!(
            "view body must be a SELECT, got {:?}", other,
        ))),
    };
    compile_view_query(view_id, &query, sql, binder)
}

// ---------------------------------------------------------------------------
// Also expose a standalone filter-program compiler (used for future
// CHECK clauses / partial indices — Phase 1 unused, API-parity with the
// client planner).
// ---------------------------------------------------------------------------

pub fn compile_filter_program<R: CatalogResolver + ?Sized>(
    where_expr: &Expr,
    schema:     &Schema,
    binder:     &Binder<R>,
) -> Result<crate::expr_program::ExprProgram, GnitzSqlParseError> {
    let bound = binder.bind_expr(where_expr, schema)?;
    let mut eb = ExprBuilder::new();
    let (result_reg, _) = compile_bound_expr(&bound, schema, &mut eb)?;
    Ok(eb.build(result_reg))
}
