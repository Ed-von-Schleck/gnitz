//! Instruction emission: per-node `emit_*`, the expression/scalar-func
//! constructors, and `build_plan` (one plan, pre or post exchange).

use super::*;

// ---------------------------------------------------------------------------
// Expression + scalar function construction helpers
// ---------------------------------------------------------------------------

#[allow(clippy::vec_box)]
pub(super) fn create_expr_predicate(
    code: &[u32],
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
    schema: &SchemaDescriptor,
    owned_funcs: &mut Vec<Box<ScalarFunc>>,
) -> *const ScalarFunc {
    let prog = LogicalProgram::from_wire(code, num_regs, result_reg, const_strings);
    let func = Box::new(ScalarFunc::from_predicate(prog, schema));
    let ptr = &*func as *const ScalarFunc;
    owned_funcs.push(func);
    ptr
}

#[allow(clippy::vec_box)]
pub(super) fn create_expr_map(
    code: &[u32],
    num_regs: u32,
    const_strings: Vec<Vec<u8>>,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    owned_funcs: &mut Vec<Box<ScalarFunc>>,
) -> *const ScalarFunc {
    let prog = LogicalProgram::from_wire(code, num_regs, 0, const_strings);
    let func = Box::new(ScalarFunc::from_map(prog, in_schema, out_schema));
    let ptr = &*func as *const ScalarFunc;
    owned_funcs.push(func);
    ptr
}

#[allow(clippy::vec_box, clippy::ptr_arg)]
pub(super) fn create_universal_projection(
    src_indices: &[i32],
    src_types: &[u8],
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    owned_funcs: &mut Vec<Box<ScalarFunc>>,
) -> *const ScalarFunc {
    let indices: Vec<u32> = src_indices.iter().map(|&i| i as u32).collect();
    let func = Box::new(ScalarFunc::from_projection(&indices, src_types, in_schema, out_schema));
    let ptr = &*func as *const ScalarFunc;
    owned_funcs.push(func);
    ptr
}

pub(super) fn null_func_ptr() -> *const ScalarFunc {
    std::ptr::null()
}

/// Both inputs of a `Union` share a physical layout (equal type codes, sizes, and
/// PK indices); only per-column nullability may differ. Return `a`'s schema with
/// each column's nullability OR-ed with `b`'s, so a null-carrying side forces the
/// null-aware `Generic` row comparator instead of the null-blind `FixedIntNonnull`
/// fast path (which orders by raw payload bytes and would fail to coalesce two
/// logically-NULL rows carrying non-zero bytes under the null bit).
pub(super) fn union_nullability_merge(a: &SchemaDescriptor, b: &SchemaDescriptor) -> SchemaDescriptor {
    debug_assert_eq!(a.num_columns(), b.num_columns(), "union inputs must share a layout");
    debug_assert_eq!(a.pk_indices(), b.pk_indices(), "union inputs must share a layout");
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    for (c, col) in cols[..a.num_columns()].iter_mut().enumerate() {
        let ac = a.columns[c];
        let bc = b.columns[c];
        debug_assert_eq!(ac.type_code, bc.type_code, "union inputs must share a layout");
        *col = SchemaColumn::new(ac.type_code, ac.nullable | bc.nullable);
    }
    SchemaDescriptor::new(&cols[..a.num_columns()], a.pk_indices())
}

/// Folded-finalize programs threaded through plan emission. `logical` holds the
/// unresolved programs indexed by `fold_finalize` idx — each is `.take()`n and
/// resolved exactly once against its reduce node's output schema. `resolved` is
/// the kept-alive store the reduce ops hold raw `*const ResolvedProgram` into;
/// it is moved into the VM at the end of `build_plan`.
// `Box` is load-bearing, not incidental: the reduce ops hold raw
// `*const ResolvedProgram` into `resolved`, so each program's heap address must
// stay stable as the vec grows (a bare `Vec<ResolvedProgram>` would move
// elements on reallocation and dangle those pointers).
#[allow(clippy::vec_box)]
pub(super) struct FinalizePrograms {
    pub(super) logical: Vec<Option<Box<LogicalProgram>>>,
    pub(super) resolved: Vec<Box<ResolvedProgram>>,
}

/// Path of a per-worker scratch directory under `view_dir`. Rank-stamped because
/// forked workers share `view_dir`; an un-stamped path would have every worker
/// open the same directory and clobber each other's shard files. Single source
/// of truth for the convention — `create_child_table` derives its path from it,
/// so the rank stamp can never drift between child tables.
pub(super) fn child_scratch_dir(view_dir: &str, child_name: &str) -> String {
    format!("{}/scratch_{}_w{}", view_dir, child_name, worker_rank())
}

/// True iff `name` is one of THIS worker's rank-stamped scratch dirs — the
/// inverse of the `child_scratch_dir` convention above, kept adjacent so the
/// recovery reset sweep can never drift from the naming it must match.
pub(crate) fn is_worker_scratch_dir_name(name: &str) -> bool {
    name.starts_with("scratch_") && name.ends_with(&format!("_w{}", worker_rank()))
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
    // `RederiveCheckpointed`: the ephemeral checkpoint round force-persists
    // these view operator-trace tables with generation-stamped manifests.
    // `create_child_table` is reached only from view-plan compilation, never
    // index-circuit compilation (index tables stay plain `Rederive`).
    Table::new(
        &child_dir,
        child_name,
        schema,
        table_id,
        256 * 1024,
        RecoverySource::RederiveCheckpointed,
    )
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
    loaded
        .outgoing
        .get(&nid)
        .map(|outs| {
            !outs.is_empty()
                && outs.iter().all(|&(dst, port)| {
                    port == PORT_TRACE
                        && matches!(
                            loaded.nodes.get(&dst),
                            Some(gnitz_wire::OpNode::Join(_)) | Some(gnitz_wire::OpNode::SeekTrace)
                        )
                })
        })
        .unwrap_or(false)
}

pub(super) struct EmitState {
    sink_reg_id: i32,
    input_delta_reg_id: i32,
    emit_failed: bool,
    // Scratch directories created via `create_child_table` during this build.
    // On a compile failure they are removed (see `build_plan`/`compile_view`) so
    // probing unsupported queries can't permanently leak inodes; on success the
    // VM's owned tables keep them alive.
    scratch_dirs: Vec<String>,
    // True iff every `ScanDelta` source of this view is a replicated base table.
    // Consumed by the `PartitionFilter` emit arm to bake the trim as a keep-all
    // identity (an all-replicated view runs correct-local on every worker).
    all_sources_replicated: bool,
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
    reg_meta: &mut Vec<RegisterMeta>,
    // Owned resources
    owned_tables: &mut Vec<Box<Table>>,
    owned_funcs: &mut Vec<Box<ScalarFunc>>,
    fin_progs: &mut FinalizePrograms,
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
                reg_meta[reg_id as usize] = RegisterMeta::delta(ext.schema);
                source_reg_map.insert(*tid as i64, reg_id);
            }
        }

        gnitz_wire::OpNode::ScanTrace(tid) => {
            if let Some(ext) = ext_tables.iter().find(|t| t.table_id == *tid as i64) {
                reg_meta[reg_id as usize] = RegisterMeta::trace(ext.schema);
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
                                        Some(gnitz_wire::OpNode::Join(_)) | Some(gnitz_wire::OpNode::SeekTrace)
                                    )
                            })
                        }),
                        "ScanTrace node {nid} has mixed consumers: a PORT_TRACE join consumer would \
                         misroute to the cursorless delta register"
                    );
                    let out_delta_id = reg_meta.len() as i32;
                    reg_meta.push(RegisterMeta::delta(ext.schema));
                    out_reg_of.insert(nid, out_delta_id);
                    builder.add_scan_trace(reg_id as u16, out_delta_id as u16, 0);
                }
            }
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_schema = reg_meta[in_reg as usize].schema;
            reg_meta[reg_id as usize] = RegisterMeta::delta(in_schema);
            let func_ptr = if let Some(blob) = blob {
                match decode_expr_blob(blob) {
                    Some(dep) => create_expr_predicate(
                        &dep.code,
                        dep.num_regs,
                        dep.result_reg,
                        dep.const_strings,
                        &in_schema,
                        owned_funcs,
                    ),
                    // A present-but-corrupt blob is catalog corruption. Falling
                    // back to null_func_ptr (pass-all) would silently turn a
                    // WHERE into WHERE TRUE; abort the compile instead.
                    None => {
                        state.emit_failed = true;
                        return;
                    }
                }
            } else {
                // Absent blob = no WHERE clause; pass-all is intentional.
                null_func_ptr()
            };
            builder.add_filter(in_reg as u16, reg_id as u16, func_ptr);
        }

        gnitz_wire::OpNode::Map(mk) => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_meta[in_reg as usize].schema;
            match mk {
                gnitz_wire::MapKind::Expression {
                    program,
                    reindex_cols,
                    reindex_target_tcs,
                } => {
                    // A corrupt MAP blob would otherwise be skipped, leaving the
                    // output register at the default empty schema and silently
                    // producing wrong/empty downstream results. Abort the compile.
                    let dep = match decode_expr_blob(program) {
                        Some(d) => d,
                        None => {
                            state.emit_failed = true;
                            return;
                        }
                    };
                    // Identity MAP: if no reindex and schemas match, skip if sequential copy.
                    if reindex_cols.is_empty() && in_reg_schema.same_physical_layout(&loaded.out_schema) {
                        let prog = LogicalProgram::from_wire(&dep.code, dep.num_regs, 0, Vec::new());
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
                    // columns to the kept payload columns, written into a fixed
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
                    let node_schema = if !reindex_cols.is_empty() {
                        // The output payload is exactly what the program copies: a
                        // reindex program is one `COPY_COL(tc, src, out)` per kept
                        // payload column with dense outs `0..n` (the planner's
                        // `build_reindex_program`), so the kept-column list is read
                        // straight off the blob — the single source of truth, with no
                        // separate wire list to drift out of sync. A program of any
                        // other shape falls back to "all input columns" (the
                        // pre-derivation structural assumption; the range-join
                        // `nf_rekey` copies at an offset and lands here). An
                        // out-of-range source column means a corrupt/forged catalog —
                        // `columns[c]` would read a zeroed slot — so fail the compile.
                        let prog = LogicalProgram::from_wire(&dep.code, dep.num_regs, 0, Vec::new());
                        let n_in = in_reg_schema.num_columns();
                        let payload_cols: Vec<u16> = match prog.payload_copy_srcs() {
                            Some(srcs) => {
                                if srcs.iter().any(|&c| c as usize >= n_in) {
                                    state.emit_failed = true;
                                    return;
                                }
                                srcs.iter().map(|&c| c as u16).collect()
                            }
                            None => (0..n_in as u16).collect(),
                        };
                        if pk_n + payload_cols.len() > crate::schema::MAX_COLUMNS {
                            state.emit_failed = true;
                            return;
                        }
                        reindex_output_schema(&in_reg_schema, reindex_cols, reindex_target_tcs, &payload_cols)
                    } else {
                        loaded.out_schema
                    };
                    let fp = create_expr_map(
                        &dep.code,
                        dep.num_regs,
                        dep.const_strings,
                        &in_reg_schema,
                        &node_schema,
                        owned_funcs,
                    );
                    reg_meta[reg_id as usize] = RegisterMeta::delta(node_schema);
                    let cols_u32: Vec<u32> = reindex_cols.iter().map(|&c| c as u32).collect();
                    builder.add_map(
                        in_reg as u16,
                        reg_id as u16,
                        fp,
                        node_schema,
                        &cols_u32,
                        reindex_target_tcs,
                        false,
                        0,
                    );
                }

                gnitz_wire::MapKind::HashRow(proj_cols, target_tcs, branch_id) => {
                    // Keep the listed columns as payload (positions 0..k), like a
                    // Projection, but prepend a synthetic U128 PK that op_map sets
                    // to a hash of those payload columns (reindex_hash path).
                    // `target_tcs[j] != 0` promotes payload column j to that
                    // ≤8-byte integer type (cross-width set-op coercion): the node
                    // schema declares the wider slot and `from_projection` widens the
                    // source into it, so both set-op sides hash one physical layout.
                    //
                    // Same trust-boundary guards as the Expression reindex arm above:
                    // an out-of-range column would read a zeroed schema slot (a
                    // silently wrong hash), and a carried target must be exactly the
                    // planner's value-preserving promotion of its source —
                    // `join_key_common_type(src, t) == Some(t)` — restricted to the
                    // ≤8-byte fixed-int domain the payload widen supports. A
                    // violation means a corrupt/forged catalog; fail the compile
                    // cleanly rather than truncate in `copy_column`.
                    if 1 + proj_cols.len() > crate::schema::MAX_COLUMNS
                        || proj_cols.iter().any(|&c| c as usize >= in_reg_schema.num_columns())
                    {
                        state.emit_failed = true;
                        return;
                    }
                    let promotion_invalid = proj_cols.iter().enumerate().any(|(j, &c)| {
                        let t = target_tcs.get(j).copied().unwrap_or(0);
                        t != 0 && {
                            let src = in_reg_schema.columns[c as usize].type_code;
                            !gnitz_wire::is_fixed_int(t) || gnitz_wire::join_key_common_type(src, t) != Some(t)
                        }
                    });
                    if promotion_invalid {
                        state.emit_failed = true;
                        return;
                    }
                    let src_indices: Vec<i32> = proj_cols.iter().map(|&c| c as i32).collect();
                    let src_types: Vec<u8> = src_indices
                        .iter()
                        .map(|&i| in_reg_schema.columns[i as usize].type_code)
                        .collect();
                    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
                    cols[0] = SchemaColumn::new(type_code::U128, 0);
                    for (j, &i) in src_indices.iter().enumerate() {
                        let src = in_reg_schema.columns[i as usize];
                        let tgt = target_tcs.get(j).copied().unwrap_or(0);
                        let out_tc = if tgt != 0 { tgt } else { src.type_code };
                        // `new` re-derives size/signedness for the promoted type;
                        // keep THIS SIDE's nullability. Per-side, not the
                        // operator-merged view nullability: an INTERSECT/EXCEPT leaf
                        // is `distinct`-ed on its own before the tuple-tightening
                        // combine, so its row comparator must classify by what this
                        // side can actually emit.
                        cols[1 + j] = SchemaColumn::new(out_tc, src.nullable);
                    }
                    let node_schema = SchemaDescriptor::new(&cols[..1 + src_indices.len()], &[0]);
                    let fp = create_universal_projection(
                        &src_indices,
                        &src_types,
                        &in_reg_schema,
                        &node_schema,
                        owned_funcs,
                    );
                    reg_meta[reg_id as usize] = RegisterMeta::delta(node_schema);
                    builder.add_map(
                        in_reg as u16,
                        reg_id as u16,
                        fp,
                        node_schema,
                        &[],
                        &[],
                        true,
                        *branch_id,
                    );
                }

                gnitz_wire::MapKind::Projection(cols) => {
                    let src_indices: Vec<i32> = cols.iter().map(|&c| c as i32).collect();
                    let src_types: Vec<u8> = src_indices
                        .iter()
                        .map(|&i| in_reg_schema.columns[i as usize].type_code)
                        .collect();
                    let schema = build_map_output_schema(&in_reg_schema, &src_indices);
                    let fp =
                        create_universal_projection(&src_indices, &src_types, &in_reg_schema, &schema, owned_funcs);
                    reg_meta[reg_id as usize] = RegisterMeta::delta(schema);
                    builder.add_map(in_reg as u16, reg_id as u16, fp, schema, &[], &[], false, 0);
                }
            }
        }

        gnitz_wire::OpNode::Negate => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            reg_meta[reg_id as usize] = RegisterMeta::delta(reg_meta[in_reg as usize].schema);
            builder.add_negate(in_reg as u16, reg_id as u16);
        }

        gnitz_wire::OpNode::Union => {
            let in_a = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let has_b = in_regs.contains_key(&PORT_IN_B);
            let in_b = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
            let a_schema = reg_meta[in_a as usize].schema;
            let out_schema = if has_b {
                union_nullability_merge(&a_schema, &reg_meta[in_b as usize].schema)
            } else {
                // O(1) identity pass-through (op_union returns PORT_IN_A unchanged); no B
                // schema to merge.
                a_schema
            };
            reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
            builder.add_union(in_a as u16, in_b as u16, has_b, reg_id as u16);
        }

        gnitz_wire::OpNode::Distinct | gnitz_wire::OpNode::PositivePart => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_meta[in_reg as usize].schema;
            // `distinct` is the only one the optimizer elides (its input is already
            // distinct); `positive_part` is never seeded into `is_distinct_at`, so
            // it is never in `skip_nodes` — this check is simply false for it.
            if rw.skip_nodes.contains(&nid) {
                out_reg_of.insert(nid, in_reg);
                return;
            }
            // Set-membership clamp `[-1, 1]` for distinct; bag clamp `[0, i64::MAX]`
            // (negative part only) for positive_part. The two presets are the sole
            // difference between the operators; both emit one `WeightClamp` instr.
            let (lo, hi) = if matches!(op, gnitz_wire::OpNode::PositivePart) {
                (0, i64::MAX)
            } else {
                (-1, 1)
            };
            let child_name = format!("_hist_{view_id}_{nid}");
            let hist_table = match create_child_table(state, view_dir, &child_name, in_reg_schema, view_table_id) {
                Ok(t) => t,
                Err(_) => {
                    state.emit_failed = true;
                    return;
                }
            };
            let table_idx = owned_tables.len();
            owned_tables.push(Box::new(hist_table));
            let hist_table_ptr = &*owned_tables[table_idx] as *const Table as *mut Table;
            reg_meta[reg_id as usize] = RegisterMeta::trace(in_reg_schema);
            owned_trace_regs.push((reg_id as u16, table_idx));
            let out_delta_id = reg_meta.len() as i32;
            reg_meta.push(RegisterMeta::delta(in_reg_schema));
            out_reg_of.insert(nid, out_delta_id);
            builder.add_weight_clamp(
                in_reg as u16,
                reg_id as u16,
                out_delta_id as u16,
                hist_table_ptr,
                lo,
                hi,
            );
        }

        gnitz_wire::OpNode::Reduce {
            group_cols,
            agg,
            global_ground,
        } => {
            emit_reduce(
                loaded,
                rw,
                nid,
                reg_id,
                group_cols,
                agg,
                *global_ground,
                &in_regs,
                builder,
                state,
                out_reg_of,
                reg_meta,
                owned_tables,
                fin_progs,
                owned_trace_regs,
                view_dir,
                view_table_id,
                view_id,
            );
        }

        gnitz_wire::OpNode::Join(kind) => {
            // PORT_IN_A == 0 (delta side); PORT_TRACE == PORT_IN_B == 1 (trace/right side).
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            let a_schema = reg_meta[a_reg as usize].schema;
            let b_schema = reg_meta[b_reg as usize].schema;
            match kind {
                gnitz_wire::JoinKind::DeltaTrace => {
                    reg_meta[reg_id as usize] =
                        RegisterMeta::delta(merge_schemas_for_join(&a_schema, &b_schema, JoinNullFill::None));
                    builder.add_join_dt(a_reg as u16, b_reg as u16, reg_id as u16, b_schema);
                }
                gnitz_wire::JoinKind::DeltaTraceRange { n_eq, rel } => {
                    // Same output layout as the equi delta-trace join; only the
                    // probe differs. `n_eq`/`rel` ride to the op so it can derive
                    // the eq-prefix / range-slot split and the cut direction.
                    reg_meta[reg_id as usize] =
                        RegisterMeta::delta(merge_schemas_for_join(&a_schema, &b_schema, JoinNullFill::None));
                    builder.add_join_dt_range(a_reg as u16, b_reg as u16, reg_id as u16, b_schema, *n_eq, *rel);
                }
            }
        }

        gnitz_wire::OpNode::IntegrateSink => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            state.sink_reg_id = in_reg;
            emit_simple_integrate(builder, in_reg as u16, std::ptr::null_mut());
        }

        gnitz_wire::OpNode::IntegrateTrace => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_meta[in_reg as usize].schema;
            let child_name = format!("_int_{view_id}_{nid}");
            match create_child_table(state, view_dir, &child_name, in_reg_schema, view_table_id) {
                Ok(t) => {
                    let table_idx = owned_tables.len();
                    owned_tables.push(Box::new(t));
                    let table_ptr = &*owned_tables[table_idx] as *const Table as *mut Table;
                    reg_meta[reg_id as usize] = RegisterMeta::trace(in_reg_schema);
                    owned_trace_regs.push((reg_id as u16, table_idx));
                    emit_simple_integrate(builder, in_reg as u16, table_ptr);
                }
                // Must fail the compile: emitting the view without the Integrate
                // would compile a view that never persists its differential
                // state, leaving its output permanently empty.
                Err(_) => {
                    state.emit_failed = true;
                }
            }
        }

        gnitz_wire::OpNode::ExchangeShard { .. } => {}

        gnitz_wire::OpNode::PartitionFilter => {
            // Pass-through schema; drops the rows this worker does not own before
            // they reach `integrate_trace`. Worker identity is the compile-time
            // `(worker_rank, num_workers)` of this process (default `(0, 1)` =
            // keep-all for single-process / unit tests). An all-replicated view runs
            // correct-local over the full broadcast on every worker, so it bakes
            // `(0, 1)` too — `op_partition_filter` degenerates to a keep-all identity
            // at `num_workers <= 1`, integrating the full input instead of trimming
            // away the rows this worker does not own.
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            reg_meta[reg_id as usize] = RegisterMeta::delta(reg_meta[in_reg as usize].schema);
            let (wid, nw) = if state.all_sources_replicated {
                (0, 1)
            } else {
                (worker_rank(), num_workers())
            };
            builder.add_partition_filter(in_reg as u16, reg_id as u16, wid, nw);
        }

        gnitz_wire::OpNode::ExchangeGather => {
            if let Some(&in_reg) = in_regs.get(&PORT_IN) {
                reg_meta[reg_id as usize] = reg_meta[in_reg as usize];
                // ExchangeGather is a logical passthrough: the exchange mechanism
                // injects gathered data directly into the exchange-input register.
                // Redirect downstream reads to that register; reg_id is never written.
                out_reg_of.insert(nid, in_reg);
            }
        }

        gnitz_wire::OpNode::NullExtend { type_codes } => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_schema = reg_meta[in_reg as usize].schema;
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
            let out_schema = merge_schemas_for_join(&in_schema, &right, JoinNullFill::RightNullable);
            reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
            builder.add_null_extend(in_reg as u16, reg_id as u16, right);
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
    builder.add_integrate(in_reg, table_ptr, std::ptr::null_mut(), &[], &[]);
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
    global_ground: bool,
    in_regs: &HashMap<i32, i32>,
    builder: &mut ProgramBuilder,
    state: &mut EmitState,
    out_reg_of: &mut HashMap<i32, i32>,
    reg_meta: &mut Vec<RegisterMeta>,
    owned_tables: &mut Vec<Box<Table>>,
    fin_progs: &mut FinalizePrograms,
    owned_trace_regs: &mut Vec<(u16, usize)>,
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
) {
    let in_reg_id = in_regs.get(&PORT_IN).copied().unwrap_or(0);
    let in_reg_schema = reg_meta[in_reg_id as usize].schema;

    let gcols: Vec<i32> = group_cols.iter().map(|&c| c as i32).collect();
    let gcols_u32: Vec<u32> = group_cols.iter().map(|&c| c as u32).collect();

    let mut agg_descs: Vec<AggDescriptor> = Vec::new();

    match agg {
        gnitz_wire::AggKind::Null => {
            agg_descs.push(AggDescriptor {
                col_idx: 0,
                agg_op: AggOp::Null,
                col_type_code: TypeCode::U64,
                _pad: [0; 2],
            });
        }
        gnitz_wire::AggKind::Specs(specs) => {
            for &(ref func, col_idx) in specs {
                let agg_op = AggOp::from(*func);
                let col_type_code = TypeCode::from_validated_u8(in_reg_schema.columns[col_idx as usize].type_code);
                agg_descs.push(AggDescriptor {
                    col_idx: col_idx as u32,
                    agg_op,
                    col_type_code,
                    _pad: [0; 2],
                });
            }
        }
    }

    let reduce_out_schema = build_reduce_output_schema(&in_reg_schema, &gcols, &agg_descs);

    let trace_table = match create_child_table(
        state,
        view_dir,
        &format!("_reduce_{view_id}_{nid}"),
        reduce_out_schema,
        view_table_id,
    ) {
        Ok(t) => t,
        Err(_) => {
            state.emit_failed = true;
            return;
        }
    };
    let trace_table_idx = owned_tables.len();
    owned_tables.push(Box::new(trace_table));
    let trace_table_ptr = &*owned_tables[trace_table_idx] as *const Table as *mut Table;

    reg_meta[reg_id as usize] = RegisterMeta::trace(reduce_out_schema);
    owned_trace_regs.push((reg_id as u16, trace_table_idx));

    let raw_delta_id = reg_meta.len() as i32;
    reg_meta.push(RegisterMeta::delta(reduce_out_schema));

    let finalize_prog_idx = rw.fold_finalize.get(&nid).copied();
    let mut fin_delta_id: i32 = -1;
    if finalize_prog_idx.is_some() {
        fin_delta_id = reg_meta.len() as i32;
        reg_meta.push(RegisterMeta::delta(loaded.out_schema));
        out_reg_of.insert(nid, fin_delta_id);
    } else {
        out_reg_of.insert(nid, raw_delta_id);
    }

    let all_linear = agg_descs.iter().all(|a| a.agg_op.is_linear());
    let has_value_indexed = agg_descs.iter().any(|a| a.agg_op.uses_value_index());
    // Serve every MIN/MAX aggregate (grouped or global) from one combined value
    // index, keyed `group_cols ‖ ordinal ‖ av_encoded`, when every value-indexed
    // aggregate is order-encodable and the group key is byte-form-eligible (the
    // ordinal column is accounted for in `avi_group_key_eligible`'s budget). The
    // empty global key is eligible, so a global MIN/MAX always resolves via the
    // index; nothing value-indexed is left on the trace-scan fallback below.
    //
    // No nullable check on the aggregate columns: NULL aggregate values never
    // reach the AVI. The reduce accumulator skips NULL inputs (ops/reduce/agg.rs)
    // and AVI population skips a NULL aggregate value before encoding the index
    // key (ops/index.rs), whose value column is a non-nullable PK. Moving either
    // filter without revisiting this would write a zeroed key and corrupt MIN/MAX.
    let all_value_indexed_eligible = agg_descs
        .iter()
        .filter(|a| a.agg_op.uses_value_index())
        .all(|a| agg_value_idx_eligible(a.col_type_code));
    // MIN/MAX require an order-encodable (≤8-byte int/float) aggregate column:
    // both the accumulator and the AVI key on `encode_ordered`, which has no
    // monotone key for a STRING / U128 / UUID / BLOB source. Fail the compile here
    // (→ `build_plan` returns `None`, so the view compiles to nothing and produces
    // no output) rather than letting the trace-scan fallback reach
    // `encode_ordered`'s unreachable arm and panic the worker at execution. The
    // SQL binder already rejects MIN/MAX over these types with a clear error; this
    // is the defensive guard for the low-level CircuitBuilder path that bypasses
    // the binder.
    if has_value_indexed && !all_value_indexed_eligible {
        state.emit_failed = true;
        return;
    }
    // Past the guard above, every value-indexed aggregate is order-encodable, so
    // AVI use turns only on the group key being byte-form-eligible.
    let use_avi = has_value_indexed && avi_group_key_eligible(&in_reg_schema, &gcols_u32);

    let mut tr_in_reg_id: i32 = -1;
    let mut tr_in_table_ptr: *mut Table = std::ptr::null_mut();

    let tr_in_name = format!("_reduce_in_{view_id}_{nid}");

    if let Some(&existing) = in_regs.get(&PORT_TRACE) {
        tr_in_reg_id = existing;
    } else if !all_linear && !use_avi {
        let tr_in = match create_child_table(state, view_dir, &tr_in_name, in_reg_schema, view_table_id) {
            Ok(t) => t,
            Err(_) => {
                state.emit_failed = true;
                return;
            }
        };
        let idx = owned_tables.len();
        owned_tables.push(Box::new(tr_in));
        tr_in_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;

        tr_in_reg_id = reg_meta.len() as i32;
        reg_meta.push(RegisterMeta::trace(in_reg_schema));
        owned_trace_regs.push((tr_in_reg_id as u16, idx));
    }

    // One combined value index per reduce: a single table keyed
    // `group_cols ‖ ordinal ‖ av_encoded`, serving every MIN/MAX aggregate.
    // `avi_aggs` is the value-indexed subset of `agg_descs`, in descriptor order
    // (ordinal = position) — the same order, selected by the same predicate, that
    // the reduce read side walks into its seek prefix. One table → one table_id,
    // one scratch dir, one compaction-filename namespace, so per-aggregate entries
    // cannot collide on a memory-pressure flush.
    let mut avi_table_ptr: *mut Table = std::ptr::null_mut();
    let mut avi_aggs: Vec<AggDescriptor> = Vec::new();

    if use_avi {
        avi_aggs = agg_descs
            .iter()
            .filter(|d| d.agg_op.uses_value_index())
            .copied()
            .collect();
        let avi_child = format!("_avidx_{view_id}_{nid}");
        if let Ok(av_table) = create_child_table(
            state,
            view_dir,
            &avi_child,
            crate::ops::make_avi_schema(&in_reg_schema, &gcols_u32),
            view_table_id,
        ) {
            let idx = owned_tables.len();
            owned_tables.push(Box::new(av_table));
            avi_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
        }
    }

    // The combined index integrates BEFORE the reduce reads it, so a prefix seek
    // returns the post-delta extreme directly. (The trace_in integrate below runs
    // after the reduce and carries no index.)
    if !avi_table_ptr.is_null() {
        builder.add_integrate(
            in_reg_id as u16,
            std::ptr::null_mut(),
            avi_table_ptr,
            &gcols_u32,
            &avi_aggs,
        );
    }

    let fin_prog_ptr: *const ResolvedProgram = if let Some(idx) = finalize_prog_idx {
        // Finalize prog reads from the raw reduce output: take the unresolved
        // logical program (each idx is resolved exactly once) and resolve its
        // column operands against reduce_out_schema in map context. The resolved
        // program is kept alive in `fin_progs.resolved` for the reduce op's
        // raw pointer.
        let logical = fin_progs.logical[idx]
            .take()
            .expect("finalize prog resolved exactly once");
        let resolved = logical.resolve(&reduce_out_schema, /* is_filter = */ false);
        fin_progs.resolved.push(Box::new(resolved));
        &**fin_progs.resolved.last().unwrap() as *const ResolvedProgram
    } else {
        std::ptr::null()
    };
    let fin_schema_ptr: *const SchemaDescriptor = if finalize_prog_idx.is_some() {
        &loaded.out_schema as *const SchemaDescriptor
    } else {
        std::ptr::null()
    };

    // Bake worker ownership of the global-aggregate seed exactly as the
    // `PartitionFilter` arm bakes `(worker_rank(), num_workers())`. A reduce whose
    // input has no upstream `ExchangeShard` is **replicated** (`reduce_multi_local`
    // — the full source is on every worker), so every worker is its own owner and
    // seeds its local copy; the load-bearing disjunct, since a replicated view is
    // single-source-read from worker 0, not `worker_for_partition(V₀)`. A sharded
    // global aggregate (`reduce_multi`) funnels all rows to partition
    // `partition_for_key(V₀)`'s owner, so only that worker seeds. Checked against
    // the static `loaded.incoming` graph (which retains the `ExchangeShard →
    // Reduce` edge across the post-phase split), NOT the post-phase register wiring
    // (the ExchangeShard node emits no instruction). `i_am_owner` is meaningful
    // only when `global_ground`; left `false` otherwise so a grouped reduce never
    // pays the bake.
    let i_am_owner = global_ground && {
        let replicated = !loaded.incoming.get(&nid).is_some_and(|ins| {
            ins.iter().any(|&(src, port)| {
                port == PORT_IN && matches!(loaded.nodes.get(&src), Some(gnitz_wire::OpNode::ExchangeShard { .. }))
            })
        });
        replicated
            || worker_rank() as usize
                == crate::ops::worker_for_partition(
                    crate::storage::partition_for_key(crate::ops::global_group_key()),
                    num_workers() as usize,
                )
    };

    builder.add_reduce(
        in_reg_id as u16,
        (tr_in_reg_id >= 0).then_some(tr_in_reg_id as u16),
        reg_id as u16,
        raw_delta_id as u16,
        (fin_delta_id >= 0).then_some(fin_delta_id as u16),
        &agg_descs,
        &gcols_u32,
        reduce_out_schema,
        avi_table_ptr,
        fin_prog_ptr,
        fin_schema_ptr,
        global_ground,
        i_am_owner,
    );

    // The trace_in integrate (non-linear, non-AVI fallback) carries no value
    // index — tr_in and the AVI are mutually exclusive (the tr_in gate is
    // `!all_linear && !use_avi`).
    if !tr_in_table_ptr.is_null() {
        emit_simple_integrate(builder, in_reg_id as u16, tr_in_table_ptr);
    }

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
    pre_built_expr_progs: Vec<Box<LogicalProgram>>,
) -> Option<PlanBuildResult> {
    let mut out_reg_of: HashMap<i32, i32> = HashMap::new();
    let mut next_reg: i32 = 0;
    for &nid in ordered {
        out_reg_of.insert(nid, next_reg);
        next_reg += 1;
    }

    // Destructive-register ordering invariant. `Union`, `Distinct`, and
    // `PositivePart` empty their PORT_IN register in place (std::mem::replace/swap
    // with an empty batch, to avoid allocation). Every node has one output register
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
    let pos: HashMap<i32, usize> = ordered.iter().copied().enumerate().map(|(i, n)| (n, i)).collect();

    for &nid in ordered {
        if rw.skip_nodes.contains(&nid) {
            continue;
        }
        // Ops that destructively empty an input register, and the port they empty.
        let dtor_port = match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::Distinct)
            | Some(gnitz_wire::OpNode::PositivePart)
            | Some(gnitz_wire::OpNode::Union) => Some(PORT_IN),
            _ => None,
        };
        let Some(port) = dtor_port else { continue };
        let Some(in_edges) = loaded.incoming.get(&nid) else {
            continue;
        };
        let Some(&(pred, _)) = in_edges.iter().find(|&&(_, p)| p == port) else {
            continue;
        };
        let Some(co_readers) = loaded.consumers.get(&pred) else {
            continue;
        };
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

    // One seed register per exchange input (the post phase of an exchange view
    // reads each side's relayed batch from its own register).
    let mut exchange_input_regs: Vec<(i32, i32)> = Vec::with_capacity(exchange_inputs.len());
    let first_exchange_input_reg_id: i32 = if exchange_inputs.is_empty() { -1 } else { next_reg };
    for (ex_nid, _) in exchange_inputs {
        exchange_input_regs.push((*ex_nid, next_reg));
        next_reg += 1;
    }

    // Register ids are u16 instruction fields. `reg_meta` is sized to the base
    // register per node plus the exchange seeds here; the emitters push the extras
    // on demand — ScanTrace/Distinct push 1, Reduce up to 3
    // (raw_delta + finalize + trace-in). Each node pushes at most 3, so reserving
    // `next_reg + 3 * ordered.len()` holds the whole program in a single
    // allocation, and that same bound — rejected here before the emit loop creates
    // any scratch tables — guarantees the final `reg_meta.len()` can never exceed
    // u16, so no register id truncates when cast.
    let reg_cap = next_reg as usize + 3 * ordered.len();
    if reg_cap > u16::MAX as usize {
        return None;
    }
    let mut reg_meta = Vec::with_capacity(reg_cap);
    reg_meta.resize(next_reg as usize, RegisterMeta::delta(SchemaDescriptor::default()));

    for ((ex_nid, ex_schema), &(_, reg)) in exchange_inputs.iter().zip(&exchange_input_regs) {
        out_reg_of.insert(*ex_nid, reg);
        reg_meta[reg as usize] = RegisterMeta::delta(*ex_schema);
    }

    let mut owned_tables: Vec<Box<Table>> = Vec::new();
    let mut owned_funcs: Vec<Box<ScalarFunc>> = Vec::new();
    // Folded-finalize programs: the pre-built logical programs go in `logical`
    // (taken + resolved once each during emission); `resolved` is filled as the
    // reduce nodes are emitted and handed to the VM to keep alive.
    let mut fin_progs = FinalizePrograms {
        logical: pre_built_expr_progs.into_iter().map(Some).collect(),
        resolved: Vec::new(),
    };
    let mut owned_trace_regs: Vec<(u16, usize)> = Vec::new();
    let mut ext_trace_regs: Vec<(u16, i64)> = Vec::new();
    let mut source_reg_map: HashMap<i64, i32> = HashMap::new();

    // Are all of this view's `ScanDelta` sources replicated base tables? The
    // `ScanDelta` set is exactly `circuit.dependencies()` (`ScanTrace` excluded),
    // the same source set `DagEngine::view_all_sources_replicated` reads from the
    // dep map — so this compile-time flag equals the runtime decision that
    // short-circuits the view to the local epoch path, and cannot drift from it.
    // `loaded` is the whole circuit in every phase, so the flag is phase-independent.
    let mut has_source = false;
    let all_sources_replicated = loaded
        .nodes
        .values()
        .filter_map(|op| match op {
            gnitz_wire::OpNode::ScanDelta(tid) => Some(*tid as i64),
            _ => None,
        })
        .all(|tid| {
            has_source = true;
            ext_tables
                .iter()
                .find(|t| t.table_id == tid)
                .is_some_and(|t| t.schema.replicated())
        })
        && has_source;

    let mut builder = ProgramBuilder::new();
    let mut state = EmitState {
        sink_reg_id: -1,
        input_delta_reg_id: first_exchange_input_reg_id,
        emit_failed: false,
        scratch_dirs: Vec::new(),
        all_sources_replicated,
    };

    for &nid in ordered {
        if matches!(loaded.nodes.get(&nid), Some(gnitz_wire::OpNode::ExchangeShard { .. })) {
            continue;
        }
        let reg_id = *out_reg_of.get(&nid).unwrap();
        emit_node(
            loaded,
            rw,
            nid,
            reg_id,
            &mut builder,
            &mut state,
            &mut out_reg_of,
            &mut reg_meta,
            &mut owned_tables,
            &mut owned_funcs,
            &mut fin_progs,
            &mut owned_trace_regs,
            ext_tables,
            &mut ext_trace_regs,
            &mut source_reg_map,
            view_dir,
            view_table_id,
            view_id,
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
        let sink_schema = &reg_meta[sink_reg as usize].schema;
        let out_schema = &loaded.out_schema;
        // A column-count match is not enough: two schemas with equal column
        // counts but mismatched types (e.g. I64 vs German-string) let the client
        // read a 16-byte string descriptor out of 8-byte integer storage.
        if sink_schema.num_columns() > 0 && !sink_schema.same_physical_layout(out_schema) {
            cleanup(&state.scratch_dirs);
            return None;
        }
    }

    // Final register count, now that the emitters have pushed every extra. The
    // pre-loop bound already guaranteed `reg_meta.len()` could not exceed u16
    // during emission. `reg_meta` is moved into `build_with_owned` below.
    let num_regs = reg_meta.len();
    debug_assert!(num_regs <= u16::MAX as usize);

    let vm = builder.build_with_owned(
        reg_meta,
        owned_tables,
        owned_funcs,
        fin_progs.resolved,
        owned_trace_regs,
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
        if !set.insert(cur) {
            continue;
        }
        if let Some(ins) = loaded.incoming.get(&cur) {
            for &(src, _port) in ins {
                queue.push_back(src);
            }
        }
    }
    set
}

/// The node feeding an `ExchangeShard` on `PORT_IN` (the value to repartition),
/// or `None` if the exchange has no such input edge (a malformed circuit).
pub(super) fn exchange_input_node(loaded: &LoadedCircuit, ex_nid: i32) -> Option<i32> {
    loaded
        .incoming
        .get(&ex_nid)
        .and_then(|ins| ins.iter().find(|&&(_, port)| port == PORT_IN))
        .map(|&(src, _)| src)
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
