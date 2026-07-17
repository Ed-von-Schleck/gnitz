//! The filter/map linear segment emitter: an optional WHERE filter plus a
//! projection (pure column reorder/subset, or an expr-map when the projection
//! derives or duplicates a PK column). Consumes the `Project(Filter?(Source))`
//! IR that `lp::lower_linear` produces; the front-end (clause rejection,
//! resolution, binding) lives in lowering.

use crate::codec::project_schema::{compile_projection_map, ProjItem};
use crate::error::GnitzSqlError;
use crate::lower::compile_filter_program;
use crate::plan::lp::Rel;
use crate::plan::view::EmitPieces;
use gnitz_core::CircuitBuilder;

/// Emit a linear segment's circuit from its lowered `Rel`. Returns
/// `(circuit, output_columns, pk_cols)`; the view's physical PK is the leading
/// `k` source-PK columns (`pk_cols == 0..k`). `view_id` is the segment's
/// allocated id (pre-allocated so a chain's downstream segment can reference it).
pub(crate) fn emit_linear(view_id: u64, rel: Rel) -> Result<EmitPieces, GnitzSqlError> {
    emit_linear_opts(view_id, rel, false)
}

/// [`emit_linear`] with an optional trailing identity `ExchangeShard` on the
/// view's PK. A plain filter/map over a *base* table is backfilled inline from
/// committed data; but a linear view whose source is an in-bundle **exchange**
/// segment (a hidden join/reduce, e.g. the scalar-subquery `final` over `H`)
/// must be backfilled by the dependency-ordered `fan_out_backfill` *after* that
/// source is filled — and the master only fans a view out when its circuit
/// carries an exchange (`view_seeds_exchange_backfill`). The shard is a same-PK
/// identity (its source already partitions by this PK, so no row moves), added
/// solely to route the view through the ordered distributed backfill.
pub(crate) fn emit_linear_sharded(view_id: u64, rel: Rel) -> Result<EmitPieces, GnitzSqlError> {
    emit_linear_opts(view_id, rel, true)
}

fn emit_linear_opts(view_id: u64, rel: Rel, shard: bool) -> Result<EmitPieces, GnitzSqlError> {
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
        bound,
    } = src
    else {
        unreachable!("a lowered linear view terminates in one Source");
    };

    // Filter program (if any), compiled against the source schema. A predicate
    // that bound to a true constant compiles to no filter at all.
    let expr_prog = match &filter {
        Some(pred) => compile_filter_program(pred, &source_schema)?,
        None => None,
    };

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
    // The `Filter` below is emitted verbatim whether or not a bound rode in: the
    // bound narrows the backfill scan, never the predicate.
    let inp = cb.input_delta_bounded(bound);
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, Some(p)),
        None => inp,
    };

    let out_node = if needs_expr_map {
        // Emit only the payload slots (k..); the k physical PK columns are carried
        // by commit_row and must not appear in the program. payload_idx is the
        // dense output payload position, matching out_cols[k + payload_idx].
        let program = compile_projection_map(&items[k..], &source_schema)?;
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

    let sink_input = if shard {
        cb.shard(out_node, &(0..k).collect::<Vec<_>>())
    } else {
        out_node
    };
    cb.sink(sink_input);
    let circuit = cb.build();

    // The view's physical PK is the leading k columns (the source PK passed
    // through in pk_indices() order).
    let view_pk: Vec<u32> = (0..k as u32).collect();
    Ok((circuit, out_cols, view_pk))
}
