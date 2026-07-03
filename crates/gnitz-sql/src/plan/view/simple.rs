//! The filter/map linear segment emitter: an optional WHERE filter plus a
//! projection (pure column reorder/subset, or an expr-map when the projection
//! derives or duplicates a PK column). Consumes the `Project(Filter?(Source))`
//! IR that `lp::lower_linear` produces; the front-end (clause rejection,
//! resolution, binding) lives in lowering.

use crate::codec::project_schema::ProjItem;
use crate::error::GnitzSqlError;
use crate::lower::{compile_bound_expr, compile_bound_expr_to_program};
use crate::plan::lp::Rel;
use crate::plan::view::EmitPieces;
use gnitz_core::{CircuitBuilder, ExprBuilder};

/// Emit a linear segment's circuit from its lowered `Rel`. Returns
/// `(circuit, output_columns, pk_cols)`; the view's physical PK is the leading
/// `k` source-PK columns (`pk_cols == 0..k`). `view_id` is the segment's
/// allocated id (pre-allocated so a chain's downstream segment can reference it).
pub(crate) fn emit_linear(view_id: u64, rel: Rel) -> Result<EmitPieces, GnitzSqlError> {
    // A lowered linear view is rooted at a Project (lowering appends one
    // unconditionally), over an optional Filter, over one Source.
    let Rel::Project {
        input,
        items,
        out_cols,
        pk_arity: k,
    } = rel
    else {
        unreachable!("a lowered linear view is rooted at a Project");
    };
    let (filter, src) = match *input {
        Rel::Filter { input, pred } => (Some(pred), *input),
        other => (None, other),
    };
    let Rel::Source {
        tid: source_tid,
        schema: source_schema,
    } = src
    else {
        unreachable!("a lowered linear view terminates in one Source");
    };

    // Filter program (if any), compiled against the source schema.
    let expr_prog = filter
        .as_ref()
        .map(|pred| compile_bound_expr_to_program(pred, &source_schema))
        .transpose()?;

    // Slots 0..k are the view's physical PK (carried verbatim by commit_row). A
    // payload slot (>= k) that is a PK PassThrough is a duplicate PK value; a
    // Computed is a derived column. Either forces the expr-map, which trusts the
    // planner's declared schema and writes/derives each payload slot explicitly —
    // the pure projection path would silently drop a PK value requested as a
    // payload column.
    let needs_expr_map = items[k..].iter().any(|item| match item {
        ProjItem::Computed { .. } => true,
        ProjItem::PassThrough { src_col } => source_schema.is_pk_col(*src_col),
    });

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
                    let reg = compile_bound_expr(bound_expr, &source_schema, &mut eb)?;
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
        // by evaluate_map_batch's bulk PK copy / build_map_output_schema's PK prepend.
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
    // through in pk_indices() order).
    let view_pk: Vec<u32> = (0..k as u32).collect();
    Ok((circuit, out_cols, view_pk))
}
