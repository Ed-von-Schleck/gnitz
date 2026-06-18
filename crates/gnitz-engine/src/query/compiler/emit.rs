//! Instruction emission: per-node `emit_*`, the expression/scalar-func
//! constructors, and `build_plan` (one plan, pre or post exchange).

use super::*;

// ---------------------------------------------------------------------------
// Expression + scalar function construction helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
pub(super) fn create_expr_predicate(
    code: Vec<i64>,
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
    schema: &SchemaDescriptor,
    _owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let prog = ExprProgram::new(code, num_regs, result_reg, const_strings);
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_predicate(prog, schema)));
    let ptr = &*func as *const ScalarFuncKind;
    owned_funcs.push(func);
    ptr
}

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
pub(super) fn create_expr_map(
    code: Vec<i64>,
    num_regs: u32,
    const_strings: Vec<Vec<u8>>,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    _owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let prog = ExprProgram::new(code, num_regs, 0, const_strings);
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_map(prog, in_schema, out_schema)));
    let ptr = &*func as *const ScalarFuncKind;
    owned_funcs.push(func);
    ptr
}

#[allow(clippy::vec_box, clippy::ptr_arg)]
pub(super) fn create_universal_projection(
    src_indices: &[i32],
    src_types: &[u8],
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let indices: Vec<u32> = src_indices.iter().map(|&i| i as u32).collect();
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_projection(
        &indices, src_types, in_schema, out_schema,
    )));
    let ptr = &*func as *const ScalarFuncKind;
    owned_funcs.push(func);
    ptr
}

pub(super) fn null_func_ptr() -> *const ScalarFuncKind {
    std::ptr::null()
}

/// Path of a per-worker scratch directory under `view_dir`. Rank-stamped because
/// forked workers share `view_dir`; an un-stamped path would have every worker
/// open the same directory and clobber each other's shard files. Single source
/// of truth for the convention — `create_child_table` and the nested GI dir both
/// derive their paths from it, so the rank stamp can never drift between them.
pub(super) fn child_scratch_dir(view_dir: &str, child_name: &str) -> String {
    format!("{}/scratch_{}_w{}", view_dir, child_name, worker_rank())
}

/// Create a child table in a subdirectory of the view's directory.
pub(super) fn create_child_table(
    state: &mut EmitState,
    view_dir: &str,
    child_name: &str,
    schema: SchemaDescriptor,
    table_id: u32,
) -> Result<Table, crate::storage::StorageError> {
    let child_dir = child_scratch_dir(view_dir, child_name);
    // Track the path before creating so cleanup also removes a partially
    // created directory if Table::new fails.
    state.scratch_dirs.push(child_dir.clone());
    Table::new(&child_dir, child_name, schema, table_id, 256 * 1024, Persistence::Ephemeral)
}

// ---------------------------------------------------------------------------
// Instruction emission — per-node handler
// ---------------------------------------------------------------------------

pub(super) fn is_join_trace_side(loaded: &LoadedCircuit, nid: i32) -> bool {
    // `.all()` (not `.any()`): a ScanTrace node feeding a join on PORT_TRACE
    // skips its delta-register allocation, leaving no `out_reg_of` entry. If the
    // SAME node also feeds a normal-port consumer (Filter/Map/Union on PORT_IN),
    // an `.any()` test would still skip, and the normal consumer's `in_regs`
    // lookup would miss and fall back to register 0 — reading unrelated payload.
    // The skip is only safe when EVERY outgoing edge is a join trace side.
    loaded.outgoing.get(&nid).map(|outs| {
        !outs.is_empty() && outs.iter().all(|&(dst, port)| {
            port == PORT_TRACE
                && matches!(
                    loaded.nodes.get(&dst),
                    Some(gnitz_wire::OpNode::Join(_))
                    | Some(gnitz_wire::OpNode::AntiJoin(_))
                    | Some(gnitz_wire::OpNode::SemiJoin(_))
                    | Some(gnitz_wire::OpNode::SeekTrace)
                )
        })
    }).unwrap_or(false)
}

pub(super) struct EmitState {
    next_extra_reg: i32,
    sink_reg_id: i32,
    input_delta_reg_id: i32,
    emit_failed: bool,
    // Scratch directories created via `create_child_table` during this build.
    // On a compile failure they are removed (see `build_plan`/`compile_view`) so
    // probing unsupported queries can't permanently leak inodes; on success the
    // VM's owned tables keep them alive.
    scratch_dirs: Vec<String>,
}

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
pub(super) fn emit_node(
    loaded: &LoadedCircuit,
    rw: &Rewrites,
    nid: i32,
    reg_id: i32,
    builder: &mut ProgramBuilder,
    state: &mut EmitState,
    out_reg_of: &mut HashMap<i32, i32>,
    reg_schemas: &mut Vec<SchemaDescriptor>,
    reg_kinds: &mut Vec<u8>,
    // Owned resources
    owned_tables: &mut Vec<Box<Table>>,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
    owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_trace_regs: &mut Vec<(u16, usize)>,
    // External tables
    ext_tables: &[ExternalTable],
    ext_trace_regs: &mut Vec<(u16, i64)>,
    source_reg_map: &mut HashMap<i64, i32>,
    // View info
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
) {
    let in_regs = compute_in_regs(loaded, nid, out_reg_of);
    let op = match loaded.nodes.get(&nid) {
        Some(op) => op,
        None => return,
    };

    match op {
        gnitz_wire::OpNode::ScanDelta(tid) => {
            if let Some(ext) = ext_tables.iter().find(|t| t.table_id == *tid as i64) {
                reg_schemas[reg_id as usize] = ext.schema;
                reg_kinds[reg_id as usize] = 0;
                source_reg_map.insert(*tid as i64, reg_id);
            }
        }

        gnitz_wire::OpNode::ScanTrace(tid) => {
            if let Some(ext) = ext_tables.iter().find(|t| t.table_id == *tid as i64) {
                reg_schemas[reg_id as usize] = ext.schema;
                reg_kinds[reg_id as usize] = 1;
                ext_trace_regs.push((reg_id as u16, *tid as i64));

                if !is_join_trace_side(loaded, nid) {
                    // Overwriting out_reg_of below points this node at the
                    // cursorless delta register. out_reg_of holds one register
                    // per node, so a consumer reading this node's PORT_TRACE
                    // would resolve to that single delta register and read an
                    // empty trace — silently emitting empty output. Routing both
                    // would require emitting two output registers for the node.
                    // The graph builder emits trace and delta scans as separate
                    // nodes, so a ScanTrace never has both a PORT_TRACE join
                    // consumer and a non-join consumer; assert it rather than
                    // corrupt results if that ever changes.
                    assert!(
                        loaded.outgoing.get(&nid).is_none_or(|outs| {
                            !outs.iter().any(|&(dst, port)| {
                                port == PORT_TRACE
                                    && matches!(
                                        loaded.nodes.get(&dst),
                                        Some(gnitz_wire::OpNode::Join(_))
                                            | Some(gnitz_wire::OpNode::AntiJoin(_))
                                            | Some(gnitz_wire::OpNode::SemiJoin(_))
                                            | Some(gnitz_wire::OpNode::SeekTrace)
                                    )
                            })
                        }),
                        "ScanTrace node {nid} has mixed consumers: a PORT_TRACE join consumer would \
                         misroute to the cursorless delta register"
                    );
                    let out_delta_id = state.next_extra_reg;
                    state.next_extra_reg += 1;
                    reg_schemas[out_delta_id as usize] = ext.schema;
                    reg_kinds[out_delta_id as usize] = 0;
                    out_reg_of.insert(nid, out_delta_id);
                    builder.add_scan_trace(reg_id as u16, out_delta_id as u16, 0);
                }
            }
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_schema = reg_schemas[in_reg as usize];
            reg_schemas[reg_id as usize] = in_schema;
            reg_kinds[reg_id as usize] = 0;
            let func_ptr = if let Some(blob) = blob {
                match decode_expr_blob(blob) {
                    Some(dep) => {
                        let code: Vec<i64> = dep.code.iter().map(|&w| w as i64).collect();
                        create_expr_predicate(code, dep.num_regs, dep.result_reg, dep.const_strings,
                            &in_schema, owned_expr_progs, owned_funcs)
                    }
                    // A present-but-corrupt blob is catalog corruption. Falling
                    // back to null_func_ptr (pass-all) would silently turn a
                    // WHERE into WHERE TRUE; abort the compile instead.
                    None => { state.emit_failed = true; return; }
                }
            } else {
                // Absent blob = no WHERE clause; pass-all is intentional.
                null_func_ptr()
            };
            builder.add_filter(in_reg as u16, reg_id as u16, func_ptr);
        }

        gnitz_wire::OpNode::Map(mk) => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];
            match mk {
                gnitz_wire::MapKind::Expression { program, reindex_cols, reindex_target_tcs } => {
                    // A corrupt MAP blob would otherwise be skipped, leaving the
                    // output register at the default empty schema and silently
                    // producing wrong/empty downstream results. Abort the compile.
                    let dep = match decode_expr_blob(program) {
                        Some(d) => d,
                        None => { state.emit_failed = true; return; }
                    };
                    // Identity MAP: if no reindex and schemas match, skip if sequential copy.
                    if reindex_cols.is_empty()
                        && schemas_physically_identical(&in_reg_schema, &loaded.out_schema)
                    {
                        let code: Vec<i64> = dep.code.iter().map(|&w| w as i64).collect();
                        let prog = ExprProgram::new(code, dep.num_regs, 0, Vec::new());
                        // Elide only when the block copy skips exactly the inherited
                        // PK region (`base == pk_count`): the MAP carries the PK
                        // region verbatim and copies payload columns 1:1.
                        if prog.sequential_copy_base() == Some(in_reg_schema.pk_indices().len()) {
                            out_reg_of.insert(nid, in_reg);
                            return;
                        }
                    }
                    // Folded MAP (absorbed into upstream REDUCE's finalize program).
                    if let Some(&reduce_nid) = rw.folded_maps.get(&nid) {
                        out_reg_of.insert(nid, *out_reg_of.get(&reduce_nid).unwrap_or(&0));
                        return;
                    }
                    // The reindex output schema prepends `pk_n` synthetic PK
                    // columns to every input column, written into a fixed
                    // [_; MAX_COLUMNS] / [_; MAX_PK_COLUMNS] array. A hand-assembled
                    // or planner-built list could exceed those bounds, or name a
                    // column >= num_columns() (which `columns[c]` would read as a
                    // zeroed slot — a silently wrong key). Fail the compile cleanly.
                    // The byte stride needs no separate MAX_PK_BYTES check: each
                    // output PK column is reindex_output_type_code(tc).wire_stride()
                    // <= 16 bytes, so pk_n <= MAX_PK_COLUMNS (5) bounds the packed
                    // stride at 5 × 16 = 80 = MAX_PK_BYTES (ReindexPacker::new's
                    // assert is the tripwire).
                    let pk_n = reindex_cols.len();
                    if pk_n > crate::schema::MAX_PK_COLUMNS
                        || pk_n + in_reg_schema.num_columns() > crate::schema::MAX_COLUMNS
                        || reindex_cols.iter().any(|&c| c as usize >= in_reg_schema.num_columns())
                    {
                        state.emit_failed = true;
                        return;
                    }
                    // A carried target `t` must be exactly the promotion the planner
                    // derives for a key of this source type. Rather than re-deriving
                    // the sign/width ladder by hand (and drifting from the planner),
                    // validate against the single shared rule: `t` is a value-
                    // preserving promotion of `src` iff `join_key_common_type` maps
                    // the pair `(src, t)` back to `t`. The promotion is idempotent —
                    // promoting a source against its own carried target is a no-op —
                    // so this is exactly the planner's `carried_reindex_tc` contract
                    // read back (a `#[test]` pins the idempotency). It also screens
                    // PK-ineligible targets for free, since the function only ever
                    // yields PK-eligible types. A mismatch means a corrupt/forged
                    // catalog, so abort the compile cleanly rather than panic in
                    // encode_pk_column_promoted (a narrowing target would slice
                    // scratch[src_width..target_width] with src_width > target_width).
                    // Runs after the in-bounds column check above, so columns[c] is
                    // always in range here.
                    let promotion_invalid = reindex_cols.iter().enumerate().any(|(i, &c)| {
                        let t = reindex_target_tcs.get(i).copied().unwrap_or(0);
                        t != 0 && {
                            let src = in_reg_schema.columns[c as usize].type_code;
                            gnitz_wire::join_key_common_type(src, t) != Some(t)
                        }
                    });
                    if promotion_invalid {
                        state.emit_failed = true;
                        return;
                    }
                    let code: Vec<i64> = dep.code.iter().map(|&w| w as i64).collect();
                    let node_schema = if !reindex_cols.is_empty() {
                        reindex_output_schema(&in_reg_schema, reindex_cols, reindex_target_tcs)
                    } else {
                        loaded.out_schema
                    };
                    let fp = create_expr_map(code, dep.num_regs, dep.const_strings,
                        &in_reg_schema, &node_schema, owned_expr_progs, owned_funcs);
                    reg_schemas[reg_id as usize] = node_schema;
                    reg_kinds[reg_id as usize] = 0;
                    let cols_u32: Vec<u32> = reindex_cols.iter().map(|&c| c as u32).collect();
                    builder.add_map(in_reg as u16, reg_id as u16, fp, node_schema,
                        &cols_u32, reindex_target_tcs, false, 0);
                }

                gnitz_wire::MapKind::HashRow(proj_cols, branch_id) => {
                    // Keep the listed columns as payload (positions 0..k), like a
                    // Projection, but prepend a synthetic U128 PK that op_map sets
                    // to a hash of those payload columns (reindex_hash path).
                    let src_indices: Vec<i32> = proj_cols.iter().map(|&c| c as i32).collect();
                    let src_types: Vec<u8> = src_indices.iter()
                        .map(|&i| in_reg_schema.columns[i as usize].type_code)
                        .collect();
                    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
                    cols[0] = SchemaColumn::new(type_code::U128, 0);
                    for (j, &i) in src_indices.iter().enumerate() {
                        cols[1 + j] = in_reg_schema.columns[i as usize];
                    }
                    let node_schema = SchemaDescriptor::new(&cols[..1 + src_indices.len()], &[0]);
                    let fp = create_universal_projection(
                        &src_indices, &src_types, &in_reg_schema, &node_schema, owned_funcs,
                    );
                    reg_schemas[reg_id as usize] = node_schema;
                    reg_kinds[reg_id as usize] = 0;
                    builder.add_map(in_reg as u16, reg_id as u16, fp, node_schema,
                        &[], &[], true, *branch_id);
                }

                gnitz_wire::MapKind::Projection(cols) => {
                    let src_indices: Vec<i32> = cols.iter().map(|&c| c as i32).collect();
                    let src_types: Vec<u8> = src_indices.iter()
                        .map(|&i| in_reg_schema.columns[i as usize].type_code)
                        .collect();
                    let schema = build_map_output_schema(&in_reg_schema, &src_indices);
                    let fp = create_universal_projection(
                        &src_indices, &src_types, &in_reg_schema, &schema, owned_funcs,
                    );
                    reg_schemas[reg_id as usize] = schema;
                    reg_kinds[reg_id as usize] = 0;
                    builder.add_map(in_reg as u16, reg_id as u16, fp, schema, &[], &[], false, 0);
                }

                gnitz_wire::MapKind::KeyOnly => {
                    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_PK_COLUMNS];
                    let mut pk_idx = [0u32; crate::schema::MAX_PK_COLUMNS];
                    let pk_len = copy_pk_columns_into(&in_reg_schema, &mut cols, &mut pk_idx);
                    let s = SchemaDescriptor::new(&cols[..pk_len], &pk_idx[..pk_len]);
                    let fp = create_universal_projection(
                        &[], &[], &in_reg_schema, &s, owned_funcs,
                    );
                    reg_schemas[reg_id as usize] = s;
                    reg_kinds[reg_id as usize] = 0;
                    builder.add_map(in_reg as u16, reg_id as u16, fp, s, &[], &[], false, 0);
                }
            }
        }

        gnitz_wire::OpNode::Negate => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[in_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            builder.add_negate(in_reg as u16, reg_id as u16);
        }

        gnitz_wire::OpNode::Union => {
            let in_a = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[in_a as usize];
            reg_kinds[reg_id as usize] = 0;
            let has_b = in_regs.contains_key(&PORT_IN_B);
            let in_b = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
            builder.add_union(in_a as u16, in_b as u16, has_b, reg_id as u16);
        }

        gnitz_wire::OpNode::Delay => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let state_reg = state.next_extra_reg;
            state.next_extra_reg += 1;
            let in_schema = reg_schemas[in_reg as usize];
            reg_schemas[state_reg as usize] = in_schema;
            reg_kinds[state_reg as usize] = 2;
            reg_schemas[reg_id as usize] = in_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_delay(in_reg as u16, state_reg as u16, reg_id as u16);
        }

        gnitz_wire::OpNode::Distinct => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];
            if rw.skip_nodes.contains(&nid) {
                out_reg_of.insert(nid, in_reg);
                return;
            }
            let child_name = format!("_hist_{view_id}_{nid}");
            let hist_table = match create_child_table(
                state, view_dir, &child_name, in_reg_schema, view_table_id,
            ) {
                Ok(t) => t,
                Err(_) => { state.emit_failed = true; return; }
            };
            let table_idx = owned_tables.len();
            owned_tables.push(Box::new(hist_table));
            let hist_table_ptr = &*owned_tables[table_idx] as *const Table as *mut Table;
            reg_schemas[reg_id as usize] = in_reg_schema;
            reg_kinds[reg_id as usize] = 1;
            owned_trace_regs.push((reg_id as u16, table_idx));
            let out_delta_id = state.next_extra_reg;
            state.next_extra_reg += 1;
            reg_schemas[out_delta_id as usize] = in_reg_schema;
            reg_kinds[out_delta_id as usize] = 0;
            out_reg_of.insert(nid, out_delta_id);
            builder.add_distinct(in_reg as u16, reg_id as u16, out_delta_id as u16, hist_table_ptr);
        }

        gnitz_wire::OpNode::Reduce { group_cols, agg } => {
            emit_reduce(
                loaded, rw, nid, reg_id, group_cols, agg, &in_regs,
                builder, state, out_reg_of, reg_schemas, reg_kinds,
                owned_tables, owned_funcs, owned_expr_progs, owned_trace_regs,
                view_dir, view_table_id, view_id,
            );
        }

        gnitz_wire::OpNode::Join(kind) => {
            // PORT_IN_A == 0 (delta side); PORT_TRACE == PORT_IN_B == 1 (trace/right side).
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            let a_schema = reg_schemas[a_reg as usize];
            let b_schema = reg_schemas[b_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            match kind {
                gnitz_wire::JoinKind::DeltaTrace => {
                    reg_schemas[reg_id as usize] = merge_schemas_for_join(&a_schema, &b_schema);
                    builder.add_join_dt(a_reg as u16, b_reg as u16, reg_id as u16, b_schema);
                }
                gnitz_wire::JoinKind::DeltaTraceOuter => {
                    reg_schemas[reg_id as usize] = merge_schemas_for_join_outer(&a_schema, &b_schema);
                    builder.add_join_dt_outer(a_reg as u16, b_reg as u16, reg_id as u16, b_schema);
                }
                gnitz_wire::JoinKind::DeltaDelta => {
                    reg_schemas[reg_id as usize] = merge_schemas_for_join(&a_schema, &b_schema);
                    builder.add_join_dd(a_reg as u16, b_reg as u16, reg_id as u16, b_schema);
                }
                gnitz_wire::JoinKind::DeltaTraceRange { n_eq, rel } => {
                    // Same output layout as the equi delta-trace join; only the
                    // probe differs. `n_eq`/`rel` ride to the op so it can derive
                    // the eq-prefix / range-slot split and the cut direction.
                    reg_schemas[reg_id as usize] = merge_schemas_for_join(&a_schema, &b_schema);
                    builder.add_join_dt_range(
                        a_reg as u16, b_reg as u16, reg_id as u16, b_schema, *n_eq, *rel);
                }
            }
        }

        gnitz_wire::OpNode::AntiJoin(kind) => {
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[a_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            match kind {
                gnitz_wire::JoinKind::DeltaTrace | gnitz_wire::JoinKind::DeltaTraceOuter => {
                    builder.add_anti_join_dt(a_reg as u16, b_reg as u16, reg_id as u16);
                }
                gnitz_wire::JoinKind::DeltaDelta => {
                    builder.add_anti_join_dd(a_reg as u16, b_reg as u16, reg_id as u16);
                }
                gnitz_wire::JoinKind::DeltaTraceRange { .. } =>
                    unreachable!("no anti-join range variant; planner rejects LEFT/anti + range"),
            }
        }

        gnitz_wire::OpNode::SemiJoin(kind) => {
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[a_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            match kind {
                gnitz_wire::JoinKind::DeltaTrace | gnitz_wire::JoinKind::DeltaTraceOuter => {
                    builder.add_semi_join_dt(a_reg as u16, b_reg as u16, reg_id as u16);
                }
                gnitz_wire::JoinKind::DeltaDelta => {
                    builder.add_semi_join_dd(a_reg as u16, b_reg as u16, reg_id as u16);
                }
                gnitz_wire::JoinKind::DeltaTraceRange { .. } =>
                    unreachable!("no semi-join range variant; planner rejects semi + range"),
            }
        }

        gnitz_wire::OpNode::IntegrateSink => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            state.sink_reg_id = in_reg;
            emit_simple_integrate(builder, in_reg as u16, std::ptr::null_mut());
        }

        gnitz_wire::OpNode::IntegrateTrace => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];
            let child_name = format!("_int_{view_id}_{nid}");
            match create_child_table(state, view_dir, &child_name, in_reg_schema, view_table_id) {
                Ok(t) => {
                    let table_idx = owned_tables.len();
                    owned_tables.push(Box::new(t));
                    let table_ptr = &*owned_tables[table_idx] as *const Table as *mut Table;
                    reg_schemas[reg_id as usize] = in_reg_schema;
                    reg_kinds[reg_id as usize] = 1;
                    owned_trace_regs.push((reg_id as u16, table_idx));
                    emit_simple_integrate(builder, in_reg as u16, table_ptr);
                }
                // Must fail the compile: emitting the view without the Integrate
                // would compile a view that never persists its differential
                // state, leaving its output permanently empty.
                Err(_) => { state.emit_failed = true; }
            }
        }

        gnitz_wire::OpNode::ExchangeShard { .. } => {}

        gnitz_wire::OpNode::PartitionFilter => {
            // Pass-through schema; drops the rows this worker does not own before
            // they reach `integrate_trace`. Worker identity is the compile-time
            // `(worker_rank, num_workers)` of this process (default `(0, 1)` =
            // keep-all for single-process / unit tests).
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[in_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            builder.add_partition_filter(
                in_reg as u16, reg_id as u16, worker_rank(), num_workers());
        }

        gnitz_wire::OpNode::ExchangeGather => {
            if let Some(&in_reg) = in_regs.get(&PORT_IN) {
                reg_schemas[reg_id as usize] = reg_schemas[in_reg as usize];
                reg_kinds[reg_id as usize] = reg_kinds[in_reg as usize];
                // ExchangeGather is a logical passthrough: the exchange mechanism
                // injects gathered data directly into the exchange-input register.
                // Redirect downstream reads to that register; reg_id is never written.
                out_reg_of.insert(nid, in_reg);
            }
        }

        gnitz_wire::OpNode::NullExtend { type_codes } => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_schema = reg_schemas[in_reg as usize];
            assert!(
                type_codes.len() < crate::schema::MAX_COLUMNS,
                "NULL_EXTEND n_cols={} would overflow schema array (max {})",
                type_codes.len(),
                crate::schema::MAX_COLUMNS - 1,
            );
            let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
            cols[0] = SchemaColumn::new(type_code::U128, 0); // dummy PK
            for (i, &tc) in type_codes.iter().enumerate() {
                cols[i + 1] = SchemaColumn::new(tc, 1);
            }
            let right = SchemaDescriptor::new(&cols[..type_codes.len() + 1], &[0]);
            let out_schema = merge_schemas_for_join_outer(&in_schema, &right);
            reg_schemas[reg_id as usize] = out_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_null_extend(in_reg as u16, reg_id as u16, right);
        }

        gnitz_wire::OpNode::GatherReduce => {
            let raw_cols = loaded.gather_reduce_cols.get(&nid).map(|v| v.as_slice()).unwrap_or(&[]);
            emit_gather_reduce(
                raw_cols, nid, reg_id, &in_regs,
                builder, state, out_reg_of, reg_schemas, reg_kinds,
                owned_tables, owned_trace_regs,
                view_dir, view_table_id, view_id,
            );
        }

        gnitz_wire::OpNode::SeekTrace => {
            let trace_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            let key_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            builder.add_seek_trace(trace_reg as u16, key_reg as u16);
        }

        gnitz_wire::OpNode::ClearDeltas => {
            builder.add_clear_deltas();
        }
    }
}

pub(super) fn compute_in_regs(loaded: &LoadedCircuit, nid: i32, out_reg_of: &HashMap<i32, i32>) -> HashMap<i32, i32> {
    let mut in_regs = HashMap::new();
    if let Some(in_edges) = loaded.incoming.get(&nid) {
        for &(src, port) in in_edges {
            if let Some(&reg) = out_reg_of.get(&src) {
                in_regs.insert(port, reg);
            }
        }
    }
    in_regs
}

pub(super) fn emit_simple_integrate(builder: &mut ProgramBuilder, in_reg: u16, table_ptr: *mut Table) {
    builder.add_integrate(
        in_reg,
        table_ptr,
        std::ptr::null_mut(), 0, // no GI
        std::ptr::null_mut(), false, 0, &[], 0, // no AVI
    );
}

// ---------------------------------------------------------------------------
// REDUCE emission
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
pub(super) fn emit_reduce(
    loaded: &LoadedCircuit,
    rw: &Rewrites,
    nid: i32,
    reg_id: i32,
    group_cols: &[u16],
    agg: &gnitz_wire::AggKind,
    in_regs: &HashMap<i32, i32>,
    builder: &mut ProgramBuilder,
    state: &mut EmitState,
    out_reg_of: &mut HashMap<i32, i32>,
    reg_schemas: &mut Vec<SchemaDescriptor>,
    reg_kinds: &mut Vec<u8>,
    owned_tables: &mut Vec<Box<Table>>,
    _owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
    owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_trace_regs: &mut Vec<(u16, usize)>,
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
) {
    let in_reg_id = in_regs.get(&PORT_IN).copied().unwrap_or(0);
    let in_reg_schema = reg_schemas[in_reg_id as usize];

    let gcols: Vec<i32> = group_cols.iter().map(|&c| c as i32).collect();
    let gcols_u32: Vec<u32> = group_cols.iter().map(|&c| c as u32).collect();

    let mut agg_descs: Vec<AggDescriptor> = Vec::new();
    let mut agg_func_id: AggOp = AggOp::Null;
    let mut agg_col_idx: u32 = 0;

    match agg {
        gnitz_wire::AggKind::Null => {
            agg_descs.push(AggDescriptor {
                col_idx: 0, agg_op: AggOp::Null, col_type_code: TypeCode::U64, _pad: [0; 2],
            });
        }
        gnitz_wire::AggKind::Specs(specs) => {
            for &(ref func, col_idx) in specs {
                let agg_op = AggOp::from(*func);
                let col_type_code = TypeCode::from_validated_u8(
                    in_reg_schema.columns[col_idx as usize].type_code,
                );
                agg_descs.push(AggDescriptor {
                    col_idx: col_idx as u32, agg_op, col_type_code, _pad: [0; 2],
                });
            }
            if agg_descs.len() == 1 {
                agg_func_id = agg_descs[0].agg_op;
                agg_col_idx = agg_descs[0].col_idx;
            }
        }
    }

    let reduce_out_schema = build_reduce_output_schema(&in_reg_schema, &gcols, &agg_descs);

    let trace_table = match create_child_table(
        state, view_dir, &format!("_reduce_{view_id}_{nid}"), reduce_out_schema, view_table_id,
    ) {
        Ok(t) => t,
        Err(_) => { state.emit_failed = true; return; }
    };
    let trace_table_idx = owned_tables.len();
    owned_tables.push(Box::new(trace_table));
    let trace_table_ptr = &*owned_tables[trace_table_idx] as *const Table as *mut Table;

    reg_schemas[reg_id as usize] = reduce_out_schema;
    reg_kinds[reg_id as usize] = 1;
    owned_trace_regs.push((reg_id as u16, trace_table_idx));

    let raw_delta_id = state.next_extra_reg;
    state.next_extra_reg += 1;
    reg_schemas[raw_delta_id as usize] = reduce_out_schema;
    reg_kinds[raw_delta_id as usize] = 0;

    let finalize_prog_idx = rw.fold_finalize.get(&nid).copied();
    let mut fin_delta_id: i32 = -1;
    if finalize_prog_idx.is_some() {
        fin_delta_id = state.next_extra_reg;
        state.next_extra_reg += 1;
        reg_schemas[fin_delta_id as usize] = loaded.out_schema;
        reg_kinds[fin_delta_id as usize] = 0;
        out_reg_of.insert(nid, fin_delta_id);
    } else {
        out_reg_of.insert(nid, raw_delta_id);
    }

    let all_linear = agg_descs.iter().all(|a| a.agg_op.is_linear());
    // No nullable check on agg_col_idx: NULL aggregate values never reach the
    // AVI. The reduce accumulator skips NULL inputs (ops/reduce/agg.rs) and AVI
    // integration skips NULL aggregate values before encoding the index key
    // (ops/index.rs), whose value column is a non-nullable PK. Moving either
    // filter without revisiting this would write a zeroed key and corrupt
    // MIN/MAX.
    let will_use_avi = agg_descs.len() == 1
        && matches!(agg_func_id, AggOp::Min | AggOp::Max)
        && agg_value_idx_eligible(
            TypeCode::from_validated_u8(in_reg_schema.columns[agg_col_idx as usize].type_code),
        )
        && avi_group_key_eligible(&in_reg_schema, &gcols_u32);

    let mut tr_in_reg_id: i32 = -1;
    let mut tr_in_table_ptr: *mut Table = std::ptr::null_mut();
    let mut tr_in_table_idx: Option<usize> = None;

    // Name of the trace-input scratch table; reused below to nest the GI under
    // the very same scratch dir so the two paths can't drift apart.
    let tr_in_name = format!("_reduce_in_{view_id}_{nid}");

    if let Some(&existing) = in_regs.get(&PORT_TRACE) {
        tr_in_reg_id = existing;
    } else if !all_linear && !will_use_avi {
        let tr_in = match create_child_table(
            state, view_dir, &tr_in_name, in_reg_schema, view_table_id,
        ) {
            Ok(t) => t,
            Err(_) => { state.emit_failed = true; return; }
        };
        let idx = owned_tables.len();
        owned_tables.push(Box::new(tr_in));
        tr_in_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
        tr_in_table_idx = Some(idx);

        tr_in_reg_id = state.next_extra_reg;
        state.next_extra_reg += 1;
        reg_schemas[tr_in_reg_id as usize] = in_reg_schema;
        reg_kinds[tr_in_reg_id as usize] = 1;
        owned_trace_regs.push((tr_in_reg_id as u16, idx));
    }

    let mut gi_table_ptr: *mut Table = std::ptr::null_mut();
    let mut gi_col_idx: u32 = 0;

    if tr_in_table_idx.is_some() && gcols.len() == 1 {
        let gc_col_idx = gcols[0] as usize;
        let gc_raw = in_reg_schema.columns[gc_col_idx].type_code;
        let gc_tc = TypeCode::from_validated_u8(gc_raw);
        // A nullable group column makes the GI unsound: NULL rows are skipped at
        // population, but the reduce GI path extracts gc=0 from a NULL group's
        // zero-filled slot and would collide it with a real group 0. Fall back to
        // the predicate-filtered full trace scan, which distinguishes NULL from 0.
        let gc_nullable = in_reg_schema.columns[gc_col_idx].nullable != 0;
        if !gc_nullable && matches!(gc_tc,
            TypeCode::U8  | TypeCode::I8  | TypeCode::U16 | TypeCode::I16 |
            TypeCode::U32 | TypeCode::I32 | TypeCode::U64 | TypeCode::I64
        ) {
            // Nest the GI under the trace-input table's own scratch dir, built
            // from the same `child_scratch_dir` convention so the rank stamp can
            // never drift between the two. Nesting folds the GI into the parent's
            // cleanup — the parent dir is the one registered in
            // `state.scratch_dirs`, and `remove_dir_all` is recursive.
            let gi_dir = format!("{}/_gidx", child_scratch_dir(view_dir, &tr_in_name));
            if let Ok(gi_table) = Table::new(
                &gi_dir, "_gidx", crate::ops::make_gi_schema(&in_reg_schema), 0, 1024 * 1024, Persistence::Ephemeral,
            ) {
                let idx = owned_tables.len();
                owned_tables.push(Box::new(gi_table));
                gi_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
                gi_col_idx = gc_col_idx as u32;
            }
        }
    }

    let mut avi_table_ptr: *mut Table = std::ptr::null_mut();
    let mut avi_for_max = false;
    let mut avi_agg_col_type_code: u8 = 0;
    let mut avi_group_cols: Vec<u32> = Vec::new();
    let mut avi_agg_col_idx: u32 = 0;

    if will_use_avi {
        avi_for_max = agg_func_id == AggOp::Max;
        avi_agg_col_type_code = in_reg_schema.columns[agg_col_idx as usize].type_code;
        avi_agg_col_idx = agg_col_idx;
        avi_group_cols = gcols_u32.clone();
        let avi_child = format!("_avidx_{view_id}_{nid}");
        if let Ok(av_table) = create_child_table(
            state, view_dir, &avi_child,
            crate::ops::make_avi_schema(&in_reg_schema, &gcols_u32),
            view_table_id,
        ) {
            let idx = owned_tables.len();
            owned_tables.push(Box::new(av_table));
            avi_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
        }
    }

    if !avi_table_ptr.is_null() {
        builder.add_integrate(
            in_reg_id as u16,
            std::ptr::null_mut(),
            std::ptr::null_mut(), 0,
            avi_table_ptr, avi_for_max, avi_agg_col_type_code,
            &avi_group_cols, avi_agg_col_idx,
        );
    }

    let fin_prog_ptr: *const ExprProgram = if let Some(idx) = finalize_prog_idx {
        // Finalize prog reads from the raw reduce output, so resolve its
        // column operands against reduce_out_schema. Idempotent.
        owned_expr_progs[idx].resolve_column_indices(&reduce_out_schema);
        &*owned_expr_progs[idx] as *const ExprProgram
    } else {
        std::ptr::null()
    };
    let fin_schema_ptr: *const SchemaDescriptor = if finalize_prog_idx.is_some() {
        &loaded.out_schema as *const SchemaDescriptor
    } else {
        std::ptr::null()
    };

    builder.add_reduce(
        in_reg_id as u16,
        tr_in_reg_id as i16,
        reg_id as u16,
        raw_delta_id as u16,
        fin_delta_id as i16,
        &agg_descs,
        &gcols_u32,
        reduce_out_schema,
        avi_table_ptr, avi_for_max, avi_agg_col_type_code,
        avi_agg_col_idx,
        gi_table_ptr, gi_col_idx,
        fin_prog_ptr,
        fin_schema_ptr,
    );

    if !tr_in_table_ptr.is_null() {
        builder.add_integrate(
            in_reg_id as u16,
            tr_in_table_ptr,
            gi_table_ptr, gi_col_idx,
            avi_table_ptr, avi_for_max, avi_agg_col_type_code,
            &avi_group_cols, avi_agg_col_idx,
        );
    }

    emit_simple_integrate(builder, raw_delta_id as u16, trace_table_ptr);
}

// ---------------------------------------------------------------------------
// GATHER_REDUCE emission
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
/// Build the per-aggregate descriptors for a GATHER_REDUCE node from its
/// partial-aggregate schema. The aggregate columns occupy the final
/// `agg_specs.len()` columns of the partial schema (the leading columns are the
/// group key). Each descriptor's `col_idx` must point at the aggregate column's
/// position in that partial schema (`agg_col_in_partial`) — using 0 would make
/// `Accumulator::new` derive a PK offset and corrupt gather-reduce results if
/// the gather path ever calls `step_from_batch`.
pub(super) fn build_gather_agg_descs(
    partial_schema: &SchemaDescriptor,
    agg_specs: &[(u64, u64)],
) -> Vec<AggDescriptor> {
    let num_out_cols = partial_schema.num_columns();
    let agg_count = agg_specs.len();
    let mut agg_descs: Vec<AggDescriptor> = Vec::new();
    if agg_count > 0 {
        assert!(
            num_out_cols >= agg_count,
            "GATHER_REDUCE: agg_count ({agg_count}) exceeds partial schema column count ({num_out_cols})",
        );
        for (ai, &(func_id, _)) in agg_specs.iter().enumerate() {
            let agg_op = AggOp::try_from(func_id as u8)
                .unwrap_or_else(|v| panic!("invalid agg_op {v} from wire protocol"));
            let agg_col_in_partial = num_out_cols - agg_count + ai;
            let col_type = TypeCode::from_validated_u8(
                partial_schema.columns[agg_col_in_partial].type_code,
            );
            agg_descs.push(AggDescriptor {
                col_idx: agg_col_in_partial as u32, agg_op, col_type_code: col_type, _pad: [0; 2],
            });
        }
    } else {
        agg_descs.push(AggDescriptor {
            col_idx: 0, agg_op: AggOp::Null, col_type_code: TypeCode::U64, _pad: [0; 2],
        });
    }
    agg_descs
}

// Vecs are grown in place (push); `Vec<Box<Table>>` is load-bearing — a raw
// pointer is taken into an element below, so the Box keeps the Table at a stable
// address across Vec reallocation. Same signature shape as the sibling emit_*
// helpers above.
#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
pub(super) fn emit_gather_reduce(
    raw_cols: &[(u64, u16, u64, u64)],
    nid: i32,
    reg_id: i32,
    in_regs: &HashMap<i32, i32>,
    builder: &mut ProgramBuilder,
    state: &mut EmitState,
    out_reg_of: &mut HashMap<i32, i32>,
    reg_schemas: &mut Vec<SchemaDescriptor>,
    reg_kinds: &mut Vec<u8>,
    owned_tables: &mut Vec<Box<Table>>,
    owned_trace_regs: &mut Vec<(u16, usize)>,
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
) {
    let in_reg_id = in_regs.get(&PORT_IN).copied().unwrap_or(0);
    let partial_schema = reg_schemas[in_reg_id as usize];

    let agg_specs: Vec<(u64, u64)> = raw_cols.iter()
        .filter(|(k, _, _, _)| *k == gnitz_wire::NODE_COL_KIND_AGG_SPEC)
        .map(|(_, _, v1, v2)| (*v1, *v2))
        .collect();
    let agg_descs = build_gather_agg_descs(&partial_schema, &agg_specs);

    let trace_table = match create_child_table(
        state, view_dir, &format!("_gather_{view_id}_{nid}"), partial_schema, view_table_id,
    ) {
        Ok(t) => t,
        Err(_) => { state.emit_failed = true; return; }
    };
    let table_idx = owned_tables.len();
    owned_tables.push(Box::new(trace_table));
    let trace_table_ptr = &*owned_tables[table_idx] as *const Table as *mut Table;

    reg_schemas[reg_id as usize] = partial_schema;
    reg_kinds[reg_id as usize] = 1;
    owned_trace_regs.push((reg_id as u16, table_idx));

    let raw_delta_id = state.next_extra_reg;
    state.next_extra_reg += 1;
    reg_schemas[raw_delta_id as usize] = partial_schema;
    reg_kinds[raw_delta_id as usize] = 0;
    out_reg_of.insert(nid, raw_delta_id);

    builder.add_gather_reduce(in_reg_id as u16, reg_id as u16, raw_delta_id as u16, &agg_descs);
    emit_simple_integrate(builder, raw_delta_id as u16, trace_table_ptr);
}


#[allow(clippy::too_many_arguments, clippy::vec_box)]
pub(super) fn build_plan(
    loaded: &LoadedCircuit,
    rw: &Rewrites,
    ordered: &[i32],
    ext_tables: &[ExternalTable],
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
    output_node_id: Option<i32>,
    exchange_inputs: &[(i32, SchemaDescriptor)],
    pre_built_expr_progs: Vec<Box<ExprProgram>>,
) -> Option<PlanBuildResult> {
    let mut out_reg_of: HashMap<i32, i32> = HashMap::new();
    let mut next_reg: i32 = 0;
    for &nid in ordered {
        out_reg_of.insert(nid, next_reg);
        next_reg += 1;
    }

    // Destructive-register ordering invariant. `Union`, `Distinct`, and `Delay`
    // empty their PORT_IN register in place (std::mem::replace/swap with an empty
    // batch, to avoid allocation); the trace-absent `AntiJoin(DeltaTrace)` branch
    // does the same to its PORT_IN_A delta input. Every node has one output register
    // shared by all its consumers, so a register that fans into both a
    // non-destructive reader (e.g. integrate_trace) and a destructive consumer is
    // correct only if the destructive op runs LAST among that register's consumers —
    // otherwise an earlier-scheduled co-consumer reads an emptied batch. The Kahn
    // scheduler orders them that way today; nothing else enforces it. Reject any
    // graph that violates it, in release too, rather than silently dropping a
    // reader's batch.
    //
    // skip_nodes holds Distincts elided by opt_distinct (input already distinct):
    // such a node aliases its input register and emits no instruction, so it neither
    // empties a register nor destructively reads one. Exclude it from both roles.

    // Schedule position within THIS compiled slice (exchange views compile
    // sub-slices, so index `ordered`, not loaded.ordered).
    let pos: HashMap<i32, usize> =
        ordered.iter().copied().enumerate().map(|(i, n)| (n, i)).collect();

    for &nid in ordered {
        if rw.skip_nodes.contains(&nid) {
            continue;
        }
        // Ops that destructively empty an input register, and the port they empty.
        // (Union's in_a and AntiJoinDT's delta side are both PORT_IN_A == PORT_IN.)
        let dtor_port = match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::Distinct)
            | Some(gnitz_wire::OpNode::Union)
            | Some(gnitz_wire::OpNode::Delay) => Some(PORT_IN),
            Some(gnitz_wire::OpNode::AntiJoin(gnitz_wire::JoinKind::DeltaTrace)) => Some(PORT_IN_A),
            _ => None,
        };
        let Some(port) = dtor_port else { continue };
        let Some(in_edges) = loaded.incoming.get(&nid) else { continue };
        let Some(&(pred, _)) = in_edges.iter().find(|&&(_, p)| p == port) else { continue };
        let Some(co_readers) = loaded.consumers.get(&pred) else { continue };
        let Some(&dtor_pos) = pos.get(&nid) else { continue };
        for &other in co_readers {
            if other == nid || rw.skip_nodes.contains(&other) {
                continue;
            }
            if let Some(&other_pos) = pos.get(&other) {
                if other_pos >= dtor_pos {
                    gnitz_warn!(
                        "build_plan: destructive op {nid} empties node {pred}'s register, but \
                         co-consumer {other} is scheduled at or after it ({other_pos} >= {dtor_pos}); \
                         every other consumer of a destructively-read register — a non-destructive \
                         reader, or a second destructive consumer that would find it already empty — \
                         must precede it",
                    );
                    return None;
                }
            }
        }
    }

    let mut extra_regs = 0;
    for &nid in ordered {
        let op = loaded.nodes.get(&nid);
        if matches!(op, Some(gnitz_wire::OpNode::Distinct)) && !rw.skip_nodes.contains(&nid) {
            extra_regs += 1;
        } else if matches!(op, Some(gnitz_wire::OpNode::Reduce { .. })) {
            extra_regs += 2; // raw_delta + optional tr_in (safe to over-allocate)
            if rw.fold_finalize.contains_key(&nid) {
                extra_regs += 1;
            }
        } else if matches!(op, Some(gnitz_wire::OpNode::GatherReduce)) {
            extra_regs += 1;
        } else if matches!(op, Some(gnitz_wire::OpNode::ScanTrace(_))) {
            if !is_join_trace_side(loaded, nid) {
                extra_regs += 1;
            }
        } else if matches!(op, Some(gnitz_wire::OpNode::Delay)) {
            extra_regs += 1;
        }
    }

    // One seed register per exchange input (the post phase of an exchange view
    // reads each side's relayed batch from its own register).
    let mut exchange_input_regs: Vec<(i32, i32)> = Vec::with_capacity(exchange_inputs.len());
    let first_exchange_input_reg_id: i32 = if exchange_inputs.is_empty() { -1 } else { next_reg };
    for (ex_nid, _) in exchange_inputs {
        exchange_input_regs.push((*ex_nid, next_reg));
        next_reg += 1;
    }

    let num_regs = (next_reg + extra_regs) as usize;
    // ProgramBuilder addresses registers with u16. A view complex enough to
    // need > 65535 registers would wrap the cast and fire a hard assert in
    // ProgramBuilder::build; reject it as a clean compile failure instead.
    if num_regs > u16::MAX as usize {
        return None;
    }
    let mut reg_schemas = vec![SchemaDescriptor::default(); num_regs];
    let mut reg_kinds = vec![0u8; num_regs];

    for ((ex_nid, ex_schema), &(_, reg)) in exchange_inputs.iter().zip(&exchange_input_regs) {
        out_reg_of.insert(*ex_nid, reg);
        reg_schemas[reg as usize] = *ex_schema;
        reg_kinds[reg as usize] = 0;
    }

    let mut owned_tables: Vec<Box<Table>> = Vec::new();
    let mut owned_funcs: Vec<Box<ScalarFuncKind>> = Vec::new();
    let mut owned_expr_progs: Vec<Box<ExprProgram>> = pre_built_expr_progs;
    let mut owned_trace_regs: Vec<(u16, usize)> = Vec::new();
    let mut ext_trace_regs: Vec<(u16, i64)> = Vec::new();
    let mut source_reg_map: HashMap<i64, i32> = HashMap::new();

    let mut builder = ProgramBuilder::new(num_regs as u16);
    let mut state = EmitState {
        next_extra_reg: next_reg,
        sink_reg_id: -1,
        input_delta_reg_id: first_exchange_input_reg_id,
        emit_failed: false,
        scratch_dirs: Vec::new(),
    };

    for &nid in ordered {
        if matches!(loaded.nodes.get(&nid), Some(gnitz_wire::OpNode::ExchangeShard { .. })) {
            continue;
        }
        let reg_id = *out_reg_of.get(&nid).unwrap();
        emit_node(
            loaded, rw, nid, reg_id,
            &mut builder, &mut state, &mut out_reg_of,
            &mut reg_schemas, &mut reg_kinds,
            &mut owned_tables, &mut owned_funcs, &mut owned_expr_progs, &mut owned_trace_regs,
            ext_tables, &mut ext_trace_regs, &mut source_reg_map,
            view_dir, view_table_id, view_id,
        );
    }

    // On any post-emit failure, remove the scratch directories created during
    // this build so a rejected compile leaks no inodes. On success the dirs are
    // handed to the caller in PlanBuildResult (kept alive by the VM's tables).
    let cleanup = |dirs: &[String]| {
        for d in dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    };

    if state.emit_failed {
        cleanup(&state.scratch_dirs);
        return None;
    }

    builder.add_halt();

    let mut input_delta_reg_id = state.input_delta_reg_id;
    if input_delta_reg_id == -1 && !source_reg_map.is_empty() {
        input_delta_reg_id = *source_reg_map.values().next().unwrap();
    }

    let mut sink_reg = state.sink_reg_id;
    if sink_reg == -1 {
        if let Some(out_nid) = output_node_id {
            if let Some(&reg) = out_reg_of.get(&out_nid) {
                sink_reg = reg;
            }
        }
    }

    if input_delta_reg_id == -1 {
        cleanup(&state.scratch_dirs);
        return None;
    }
    if sink_reg == -1 {
        cleanup(&state.scratch_dirs);
        return None;
    }

    if output_node_id.is_none() && sink_reg >= 0 {
        let sink_schema = &reg_schemas[sink_reg as usize];
        let out_schema = &loaded.out_schema;
        // A column-count match is not enough: two schemas with equal column
        // counts but mismatched types (e.g. I64 vs German-string) let the client
        // read a 16-byte string descriptor out of 8-byte integer storage.
        if sink_schema.num_columns() > 0 && !schemas_physically_identical(sink_schema, out_schema) {
            cleanup(&state.scratch_dirs);
            return None;
        }
    }

    let vm = builder.build_with_owned(
        &reg_schemas, &reg_kinds,
        owned_tables, owned_funcs, owned_expr_progs, owned_trace_regs,
    );

    Some(PlanBuildResult {
        vm,
        num_regs: num_regs as u32,
        in_reg: input_delta_reg_id,
        out_reg: sink_reg,
        ext_trace_regs,
        source_reg_map,
        exchange_input_regs,
        scratch_dirs: std::mem::take(&mut state.scratch_dirs),
    })
}

// ---------------------------------------------------------------------------
// Top-level compile_view entry point
// ---------------------------------------------------------------------------

/// All nodes reachable backwards from `start` (inclusive) via incoming edges —
/// i.e. the sub-pipeline that produces `start`'s value. Used to carve out each
/// set-op side's independent single-source pipeline.
pub(super) fn ancestors_inclusive(loaded: &LoadedCircuit, start: i32) -> HashSet<i32> {
    let mut set = HashSet::new();
    let mut queue = VecDeque::from([start]);
    while let Some(cur) = queue.pop_front() {
        if !set.insert(cur) { continue; }
        if let Some(ins) = loaded.incoming.get(&cur) {
            for &(src, _port) in ins { queue.push_back(src); }
        }
    }
    set
}

/// The node feeding an `ExchangeShard` on `PORT_IN` (the value to repartition).
pub(super) fn exchange_input_node(loaded: &LoadedCircuit, ex_nid: i32) -> i32 {
    loaded.incoming.get(&ex_nid)
        .and_then(|ins| ins.iter().find(|&&(_, port)| port == PORT_IN))
        .map(|&(src, _)| src)
        .unwrap_or(-1)
}

/// `Rewrites` restricted to the nodes of one plan phase. Set-op / distinct
/// views carry no fold rewrites, so only `skip_nodes` need partitioning;
/// `fold_finalize`/`folded_maps` index `owned_expr_progs` and are left empty.
pub(super) fn phase_rewrites(rw: &Rewrites, nids: &HashSet<i32>) -> Rewrites {
    Rewrites {
        skip_nodes: rw.skip_nodes.iter().filter(|n| nids.contains(n)).copied().collect(),
        fold_finalize: HashMap::new(),
        folded_maps: HashMap::new(),
    }
}

/// Narrow a plan's `source_table_id → register` map to the `u16` register width
/// the runtime dispatch (`source_reg_map`) uses.
pub(super) fn source_reg_map_u16(m: &HashMap<i64, i32>) -> HashMap<i64, u16> {
    m.iter().map(|(&tid, &reg)| (tid, reg as u16)).collect()
}

