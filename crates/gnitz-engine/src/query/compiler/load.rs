//! Circuit loading: read the system tables into a `LoadedCircuit`, topo-sort it,
//! and the scan/reindex/range-key circuit queries the DAG consults at runtime.

use super::*;
use gnitz_wire::col_index_in as cidx;

// ---------------------------------------------------------------------------
// System table reading
// ---------------------------------------------------------------------------

// Circuit sys-table column offsets, derived by name from the canonical wire
// arrays (`gnitz_wire::CIRCUIT_*_COLS`) — the same arrays `from_wire_cols` uses
// to build the schemas — so there is one source of truth. PK is columns [0, 1]
// (view_id, sub); the denormalised data columns follow. A renamed/reordered wire
// column shifts these automatically or fails `col_index_in`'s const `panic!`.
const NODES_COL_NODE_ID: usize = cidx(gnitz_wire::CIRCUIT_NODES_COLS, "node_id");
const NODES_COL_OPCODE_NEW: usize = cidx(gnitz_wire::CIRCUIT_NODES_COLS, "opcode");
const NODES_COL_SOURCE_TABLE: usize = cidx(gnitz_wire::CIRCUIT_NODES_COLS, "source_table");
const NODES_COL_REINDEX_COL: usize = cidx(gnitz_wire::CIRCUIT_NODES_COLS, "reindex_col");
const NODES_COL_EXPR_PROGRAM: usize = cidx(gnitz_wire::CIRCUIT_NODES_COLS, "expr_program");
const EDGES_COL_DST_NODE: usize = cidx(gnitz_wire::CIRCUIT_EDGES_COLS, "dst_node");
const EDGES_COL_DST_PORT: usize = cidx(gnitz_wire::CIRCUIT_EDGES_COLS, "dst_port");
const EDGES_COL_SRC_NODE: usize = cidx(gnitz_wire::CIRCUIT_EDGES_COLS, "src_node");
const NODECOL_COL_NODE_ID: usize = cidx(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, "node_id");
const NODECOL_COL_KIND: usize = cidx(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, "kind");
const NODECOL_COL_POSITION: usize = cidx(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, "position");
const NODECOL_COL_VALUE1: usize = cidx(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, "value1");
const NODECOL_COL_VALUE2: usize = cidx(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, "value2");

/// Open a cursor for a system table. Returns None if the table handle is null.
/// Positioning is done by the caller via `seek_first_positive_with_prefix` on
/// the `view_id` prefix (the circuit tables use a compound `(view_id, sub)` PK).
fn open_system_cursor(table: *mut Table) -> Option<CursorHandle> {
    if table.is_null() {
        return None;
    }
    let t = unsafe { &*table };
    Some(t.open_cursor())
}

pub(crate) fn load_circuit(
    sys_nodes: *mut Table,
    sys_edges: *mut Table,
    sys_node_cols: *mut Table,
    view_id: u64,
    out_schema: SchemaDescriptor,
) -> Option<LoadedCircuit> {
    let mut nodes: HashMap<i32, gnitz_wire::OpNode> = HashMap::new();
    let mut edges: Vec<(i32, i32, i32)> = Vec::new();
    let mut gather_reduce_cols: HashMap<i32, Vec<(u64, u16, u64, u64)>> = HashMap::new();

    // Phase 1: read CircuitNodeColumns, sorted by (kind, position) per node.
    let mut cols_by_node: HashMap<i32, Vec<gnitz_wire::CircuitNodeColumn>> = HashMap::new();
    {
        let prefix = view_id.to_be_bytes();
        let mut ch = open_system_cursor(sys_node_cols)?;
        let mut hit = ch.cursor.seek_first_positive_with_prefix(&prefix);
        while hit {
            let node_id = ch.cursor.read_i64(NODECOL_COL_NODE_ID) as i32;
            let kind = ch.cursor.read_i64(NODECOL_COL_KIND) as u64;
            let position = ch.cursor.read_i64(NODECOL_COL_POSITION) as u16;
            let v1 = ch.cursor.read_i64(NODECOL_COL_VALUE1) as u64;
            let v2 = ch.cursor.read_i64(NODECOL_COL_VALUE2) as u64;
            cols_by_node
                .entry(node_id)
                .or_default()
                .push(gnitz_wire::CircuitNodeColumn {
                    kind,
                    position,
                    value1: v1,
                    value2: v2,
                });
            ch.cursor.advance();
            hit = ch.cursor.walk_to_positive_with_prefix(&prefix);
        }
    }
    // Sort each node's cols by (kind, position) so decode_op_node sees ordered slices.
    for v in cols_by_node.values_mut() {
        v.sort_by_key(|c| (c.kind, c.position));
    }

    // Phase 2: read CircuitNodes; call decode_op_node for each.
    {
        let prefix = view_id.to_be_bytes();
        let mut ch = open_system_cursor(sys_nodes)?;
        let mut hit = ch.cursor.seek_first_positive_with_prefix(&prefix);
        while hit {
            let node_id = ch.cursor.read_i64(NODES_COL_NODE_ID) as i32;
            let opcode = ch.cursor.read_i64(NODES_COL_OPCODE_NEW) as u64;

            let src_tab: Option<u64> = if ch.cursor.col_is_null(NODES_COL_SOURCE_TABLE) {
                None
            } else {
                Some(ch.cursor.read_i64(NODES_COL_SOURCE_TABLE) as u64)
            };
            let reindex: Option<u16> = if ch.cursor.col_is_null(NODES_COL_REINDEX_COL) {
                None
            } else {
                Some(ch.cursor.read_i64(NODES_COL_REINDEX_COL) as u16)
            };
            let expr_blob: Option<Vec<u8>> = if ch.cursor.col_is_null(NODES_COL_EXPR_PROGRAM) {
                None
            } else {
                let b = ch.cursor.read_german_bytes(NODES_COL_EXPR_PROGRAM);
                if b.is_empty() {
                    None
                } else {
                    Some(b)
                }
            };

            let cols = cols_by_node.get(&node_id).map(|v| v.as_slice()).unwrap_or(&[]);
            // A node that fails to decode must abort the whole load: silently
            // skipping it leaves any edge referencing it dangling, which yields
            // an invalid topological order or silent output corruption.
            let op = gnitz_wire::decode_op_node(opcode, src_tab, reindex, expr_blob, cols).ok()?;
            if matches!(op, gnitz_wire::OpNode::GatherReduce) {
                if let Some(c) = cols_by_node.get(&node_id) {
                    // GatherReduce stays on the legacy tuple path (emit_gather_reduce)
                    // until OpNode::GatherReduce gains a typed `agg` field.
                    gather_reduce_cols.insert(
                        node_id,
                        c.iter().map(|c| (c.kind, c.position, c.value1, c.value2)).collect(),
                    );
                }
            }
            nodes.insert(node_id, op);
            ch.cursor.advance();
            hit = ch.cursor.walk_to_positive_with_prefix(&prefix);
        }
    }

    // Phase 3: read CircuitEdges.
    {
        let prefix = view_id.to_be_bytes();
        let mut ch = open_system_cursor(sys_edges)?;
        let mut hit = ch.cursor.seek_first_positive_with_prefix(&prefix);
        while hit {
            let dst = ch.cursor.read_i64(EDGES_COL_DST_NODE) as i32;
            let port = ch.cursor.read_i64(EDGES_COL_DST_PORT) as i32;
            let src = ch.cursor.read_i64(EDGES_COL_SRC_NODE) as i32;
            edges.push((src, dst, port));
            ch.cursor.advance();
            hit = ch.cursor.walk_to_positive_with_prefix(&prefix);
        }
    }

    // Every edge must reference nodes that exist in the circuit. A dangling
    // endpoint (node failed to decode, or a partial schema flush) would create
    // phantom in_degree entries in topo_sort: a missing src strands its dst
    // (never emitted), a missing dst reaches degree 0 and emit_node is called
    // with an absent node ID. Surface the inconsistency as a clean load failure.
    for &(src, dst, _port) in &edges {
        if !nodes.contains_key(&src) || !nodes.contains_key(&dst) {
            return None;
        }
    }

    Some(LoadedCircuit {
        out_schema,
        nodes,
        edges,
        gather_reduce_cols,
        ..LoadedCircuit::empty()
    })
}

// ---------------------------------------------------------------------------
// Topological sort (Kahn's algorithm)
// ---------------------------------------------------------------------------

pub(crate) fn topo_sort(loaded: &mut LoadedCircuit) -> Result<(), CompileError> {
    let mut in_degree: HashMap<i32, i32> = HashMap::new();
    for &nid in loaded.nodes.keys() {
        in_degree.insert(nid, 0);
        loaded.outgoing.entry(nid).or_default();
        loaded.incoming.entry(nid).or_default();
        loaded.consumers.entry(nid).or_default();
    }

    for &(src, dst, port) in &loaded.edges {
        loaded.outgoing.entry(src).or_default().push((dst, port));
        loaded.incoming.entry(dst).or_default().push((src, port));
        loaded.consumers.entry(src).or_default().push(dst);
        *in_degree.entry(dst).or_insert(0) += 1;
    }

    let mut init: Vec<i32> = loaded
        .nodes
        .keys()
        .filter(|&&nid| *in_degree.get(&nid).unwrap_or(&0) == 0)
        .copied()
        .collect();
    init.sort_unstable(); // deterministic order for tied sources
    let mut queue: VecDeque<i32> = init.into();

    let mut ordered = Vec::with_capacity(loaded.nodes.len());
    while let Some(nid) = queue.pop_front() {
        ordered.push(nid);
        if let Some(outs) = loaded.outgoing.get(&nid) {
            let mut next_batch: Vec<i32> = Vec::new();
            for &(dst, _) in outs {
                let deg = in_degree.get_mut(&dst).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    next_batch.push(dst);
                }
            }
            next_batch.sort_unstable();
            queue.extend(next_batch);
        }
    }

    if ordered.len() != loaded.nodes.len() {
        return Err(CompileError::Cycle);
    }
    loaded.ordered = ordered;
    Ok(())
}

// ---------------------------------------------------------------------------
// Annotation passes
// ---------------------------------------------------------------------------

/// Build the join-shard map by inspecting OpNode variants directly.
/// Maps source_tid → reindex_cols for ScanDelta → Map(Expression{reindex_cols})
/// chains (equijoin pre-indexing).
/// Walk forward from `scan_nid` through `Filter` nodes and collect the reindex
/// columns of every `Map(Expression { reindex_cols })` reached. The SQL planner
/// emits `Filter → Map(reindex)` chains for PK-redistribution views, so a
/// one-hop `scan → Map` lookup would miss the reindex behind a Filter and treat
/// the join as local-only. A table that participates in multiple joins on
/// different keys (`t JOIN t1 ON t.a = t1.x JOIN t2 ON t.b = t2.y`) fans out
/// into several reindex Maps; returning only the first would let
/// `compute_co_partitioned` approve a co-partition valid for one key but not the
/// other, so collect them all. Uses `loaded.outgoing` (populated by `topo_sort`)
/// for O(V+E) traversal instead of rescanning the flat edge list per node.
/// Shared by `compute_join_shard_map` and `DagEngine::get_join_shard_cols`.
pub(crate) fn reindex_cols_through_filters(loaded: &LoadedCircuit, scan_nid: i32) -> Vec<(i32, u8)> {
    // In a JOIN view the input scatter key is defined by the reindex that feeds the
    // trace/probe (IntegrateTrace or a join node). An input may carry OTHER reindexes
    // off the same scan that re-key the already-scattered rows in place and must NOT
    // contribute to the scatter key — notably the band LEFT join's `a.pk` re-key for
    // the null-fill set difference (keyed differently from the eq-prefix join key).
    // Those feed a plain Map → Distinct, never the trace/probe, so we drop them. The
    // nullable-key null/not-null sibling reindexes carry the SAME key as the match
    // reindex and remain (deduped). Non-join views (PK-redistribution, GROUP BY) have
    // no join node, so the guard is off and every reindex is collected as before.
    let is_join_view = loaded.nodes.values().any(|op| {
        matches!(
            op,
            gnitz_wire::OpNode::Join(_) | gnitz_wire::OpNode::AntiJoin(_) | gnitz_wire::OpNode::SemiJoin(_)
        )
    });
    let feeds_trace_or_join = |nid: i32| {
        loaded.outgoing.get(&nid).is_some_and(|outs| {
            outs.iter().any(|&(d, _)| {
                matches!(
                    loaded.nodes.get(&d),
                    Some(
                        gnitz_wire::OpNode::IntegrateTrace
                            | gnitz_wire::OpNode::Join(_)
                            | gnitz_wire::OpNode::AntiJoin(_)
                            | gnitz_wire::OpNode::SemiJoin(_)
                    )
                )
            })
        })
    };

    let mut queue = VecDeque::from([scan_nid]);
    // `visited` bounds the walk to O(nodes): without it a Filter diamond (two
    // edge paths reaching the same Filter) would re-push and re-expand nodes.
    let mut visited = HashSet::new();
    // Each reindex Map contributes its full key sequence. Distinct sequences are
    // concatenated; an identical sequence reached again (the null/not-null sibling
    // Maps of a nullable LEFT-join key) is added once. Within a sequence, duplicate
    // columns are PRESERVED (an overlapping key `a.x = b.p AND a.x = b.q` reindexes
    // `[x, x]`, possibly with distinct per-slot promotion targets), so the result
    // mirrors the trace-side ReindexPacker slot-for-slot.
    let mut seqs: Vec<Vec<(i32, u8)>> = Vec::new();
    while let Some(cur) = queue.pop_front() {
        if !visited.insert(cur) {
            continue;
        }
        if let Some(outs) = loaded.outgoing.get(&cur) {
            for &(dst, _port) in outs {
                match loaded.nodes.get(&dst) {
                    // A non-empty reindex Map is a join (or group) reindex; an empty
                    // reindex_cols is a plain projection Map — skip it. In a join view
                    // a reindex only defines the scatter key if it feeds the trace/probe.
                    Some(gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                        reindex_cols,
                        reindex_target_tcs,
                        ..
                    })) if !reindex_cols.is_empty() && (!is_join_view || feeds_trace_or_join(dst)) => {
                        let seq: Vec<(i32, u8)> = reindex_cols
                            .iter()
                            .enumerate()
                            .map(|(i, &rc)| (rc as i32, reindex_target_tcs.get(i).copied().unwrap_or(0)))
                            .collect();
                        if !seqs.contains(&seq) {
                            seqs.push(seq);
                        }
                    }
                    Some(gnitz_wire::OpNode::Filter(_)) => queue.push_back(dst),
                    _ => {}
                }
            }
        }
    }
    seqs.into_iter().flatten().collect()
}

/// Walk back from an `ExchangeShard` (`enid`) through `Filter` nodes to the source
/// scan (`ScanDelta` for SQL planner views, `ScanTrace` for Python API joins),
/// returning its table id — or `None` on a fan-in (≠ 1 incoming edge) or any
/// non-`Filter`, non-scan node. `Filter` is the only operator transparent to the
/// shard key: row-selective, never re-keys the PK region, never moves a row
/// off-worker; Map/Reduce/Distinct/join change the key or its distribution, and a
/// `PartitionFilter` (range-join broadcast input) is not a `Filter` either. The
/// backward dual of `reindex_cols_through_filters`, reading the same `loaded.incoming`
/// adjacency as `ancestors_inclusive` / `exchange_input_node`. The chain is acyclic —
/// both `compile_view` and `load_meta_circuit` reject a malformed cyclic circuit (the
/// latter presents it as empty, so this walk is never entered on one) — and each hop
/// has exactly one incoming edge, so it terminates without a visited guard.
pub(crate) fn scan_tid_through_filters(loaded: &LoadedCircuit, enid: i32) -> Option<i64> {
    let mut cur = enid;
    loop {
        let ins = loaded.incoming.get(&cur)?;
        // Bail on a fan-in (≠ 1 incoming edge): a multi-input node (Union, set op)
        // draws from more than one source, so no single table's distribution prefix
        // governs the shard key and it can never co-partition. (0 edges = root.)
        if ins.len() != 1 {
            return None;
        }
        let src_nid = ins[0].0;
        match loaded.nodes.get(&src_nid) {
            Some(gnitz_wire::OpNode::ScanDelta(t)) | Some(gnitz_wire::OpNode::ScanTrace(t)) => return Some(*t as i64),
            Some(gnitz_wire::OpNode::Filter(_)) => cur = src_nid,
            _ => return None,
        }
    }
}

/// The equality-conjunct count `n_eq` of a non-equi (range / band) join, or
/// `None` if the loaded meta-circuit is not one — read straight off its
/// `Join(DeltaTraceRange { n_eq, .. })` node (both bilinear terms carry the same
/// `n_eq`). `Some` is the precise range-join discriminator for the dag driver
/// branch and the input relay; the `n_eq` value tells the relay whether to
/// eq-prefix-scatter (`n_eq ≥ 1`, band join) or broadcast (`n_eq == 0`, pure
/// range join) without re-deriving it from the reindex key.
///
/// It is deliberately NOT `has_join_shard && has_exchange`: that predicate is
/// also true for every GROUP BY / reduce / single-sided set-op view (a group
/// reindex matches `reindex_cols_through_filters`, and the view has an output
/// `ExchangeShard`), so keying on it would divert those views into the relay
/// path and corrupt them. Shared by `DagEngine::view_range_join_n_eq` and its
/// unit test.
pub(crate) fn circuit_range_join_n_eq(loaded: &LoadedCircuit) -> Option<u8> {
    loaded.nodes.values().find_map(|op| match op {
        gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTraceRange { n_eq, .. }) => Some(*n_eq),
        _ => None,
    })
}
