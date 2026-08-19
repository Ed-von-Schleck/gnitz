//! Circuit loading: read the system tables into a `LoadedCircuit`, topo-sort it,
//! and the scan/reindex/range-key circuit queries the DAG consults at runtime.

use super::*;
use gnitz_wire::{
    CIRCEDGES_COL_DST_NODE, CIRCEDGES_COL_DST_PORT, CIRCEDGES_COL_SRC_NODE, CIRCNCOL_COL_KIND, CIRCNCOL_COL_NODE_ID,
    CIRCNCOL_COL_POSITION, CIRCNCOL_COL_VALUE1, CIRCNCOL_COL_VALUE2, CIRCNODES_COL_EXPR_PROGRAM, CIRCNODES_COL_NODE_ID,
    CIRCNODES_COL_OPCODE, CIRCNODES_COL_SOURCE_TABLE,
};

// ---------------------------------------------------------------------------
// System table reading
// ---------------------------------------------------------------------------

/// Open a cursor for a system table. Returns None if the table handle is null.
/// Positioning is done by the caller via `seek_first_positive_with_prefix` on
/// the `view_id` prefix (the circuit tables use a compound `(view_id, sub)` PK).
fn open_system_cursor(table: *mut Table) -> Option<ReadCursor> {
    if table.is_null() {
        return None;
    }
    let t = unsafe { &*table };
    Some(t.open_cursor())
}

/// Visit every `(view_id, source_table)` scan edge in the CircuitNodes store —
/// one call per `ScanDelta` node carrying a source, the same rows `load_circuit`
/// turns into `OpNode::ScanDelta`. Repeats are not filtered; the caller dedups.
pub(crate) fn for_each_scan_edge(sys_nodes: *mut Table, mut f: impl FnMut(i64, i64)) {
    let Some(mut cur) = open_system_cursor(sys_nodes) else {
        return;
    };
    // No prefix — every view's nodes, in view_id order.
    cur.for_each_positive_with_prefix(&[], |ch| {
        if ch.read_i64(CIRCNODES_COL_OPCODE) != gnitz_wire::OPCODE_SCAN_DELTA as i64
            || ch.col_is_null(CIRCNODES_COL_SOURCE_TABLE)
        {
            return;
        }
        let source = ch.read_i64(CIRCNODES_COL_SOURCE_TABLE);
        if source <= 0 {
            return;
        }
        // Compound PK `(view_id, node_id)`: view_id is the leading big-endian
        // 8 bytes of the 16-byte PK region.
        let view_id = u64::from_be_bytes(ch.current_pk_bytes()[0..8].try_into().unwrap()) as i64;
        f(view_id, source);
    });
}

/// Non-negative-`i32` node-id gate: an id that would truncate into a colliding
/// engine key is rejected (returns `None`, aborting the load) rather than
/// silently wrapped. A durable sys-table id is always in range.
#[inline]
fn node_id_i32(v: i64) -> Option<i32> {
    if (0..=i32::MAX as i64).contains(&v) {
        Some(v as i32)
    } else {
        None
    }
}

/// Read the three circuit system tables (filtered by the `view_id` OPK prefix)
/// into a `LoadedCircuit`: the node-column `(kind, position)` sort, the
/// `decode_op_node` calls, the node-id `i32` reject, and the edge-validity check.
pub(crate) fn load_circuit(
    sys: SysTableRefs,
    view_id: u64,
    out_schema: SchemaDescriptor,
) -> Result<LoadedCircuit, CompileError> {
    let fail = || CompileError::LoadFailed;
    let mut nodes_cur = open_system_cursor(sys.nodes).ok_or_else(fail)?;
    let mut edges_cur = open_system_cursor(sys.edges).ok_or_else(fail)?;
    let mut node_cols_cur = open_system_cursor(sys.node_columns).ok_or_else(fail)?;
    let mut nodes: HashMap<i32, gnitz_wire::OpNode> = HashMap::new();
    let mut edges: Vec<(i32, i32, i32)> = Vec::new();

    // The circuit tables share a compound `(view_id, sub)` PK; the OPK image of
    // the leading unsigned view_id column is its big-endian bytes.
    let prefix = view_id.to_be_bytes();
    // One abort flag for every malformed-row shape (an out-of-range id, a node
    // that fails to decode): each must abort the WHOLE load — silently skipping
    // a row leaves edges dangling, yielding an invalid topological order or
    // silent output corruption.
    let mut invalid = false;

    // Phase 1: read CircuitNodeColumns, sorted by (kind, position) per node.
    let mut cols_by_node: HashMap<i32, Vec<gnitz_wire::CircuitNodeColumn>> = HashMap::new();
    node_cols_cur.for_each_positive_with_prefix(&prefix, |ch| match node_id_i32(ch.read_i64(CIRCNCOL_COL_NODE_ID)) {
        Some(nid) => cols_by_node
            .entry(nid)
            .or_default()
            .push(gnitz_wire::CircuitNodeColumn {
                kind: ch.read_i64(CIRCNCOL_COL_KIND) as u64,
                position: ch.read_i64(CIRCNCOL_COL_POSITION) as u16,
                value1: ch.read_i64(CIRCNCOL_COL_VALUE1) as u64,
                value2: ch.read_i64(CIRCNCOL_COL_VALUE2) as u64,
            }),
        None => invalid = true,
    });
    // Phase 2: read CircuitNodes; call decode_op_node for each.
    nodes_cur.for_each_positive_with_prefix(&prefix, |ch| {
        let Some(node_id) = node_id_i32(ch.read_i64(CIRCNODES_COL_NODE_ID)) else {
            invalid = true;
            return;
        };
        let opcode = ch.read_i64(CIRCNODES_COL_OPCODE) as u64;

        let src_tab: Option<u64> = if ch.col_is_null(CIRCNODES_COL_SOURCE_TABLE) {
            None
        } else {
            Some(ch.read_i64(CIRCNODES_COL_SOURCE_TABLE) as u64)
        };
        // `None` is a NULL cell only. An empty cell is a damaged blob, and each
        // opcode already judges one: Filter rejects, a ScanDelta bound degrades.
        let expr_blob: Option<Vec<u8>> =
            (!ch.col_is_null(CIRCNODES_COL_EXPR_PROGRAM)).then(|| ch.read_german_bytes(CIRCNODES_COL_EXPR_PROGRAM));

        let cols = cols_by_node.get(&node_id).map(|v| v.as_slice()).unwrap_or(&[]);
        match gnitz_wire::decode_op_node(opcode, src_tab, expr_blob, cols) {
            Ok(op) => {
                nodes.insert(node_id, op);
            }
            Err(_) => invalid = true,
        }
    });
    if invalid {
        return Err(fail());
    }

    // Phase 3: read CircuitEdges. A truncating endpoint id aborts the load.
    edges_cur.for_each_positive_with_prefix(&prefix, |ch| {
        match (
            node_id_i32(ch.read_i64(CIRCEDGES_COL_SRC_NODE)),
            node_id_i32(ch.read_i64(CIRCEDGES_COL_DST_NODE)),
        ) {
            (Some(src), Some(dst)) => edges.push((src, dst, ch.read_i64(CIRCEDGES_COL_DST_PORT) as i32)),
            _ => invalid = true,
        }
    });
    if invalid {
        return Err(fail());
    }

    // Every edge must reference nodes that exist in the circuit. A dangling
    // endpoint (node failed to decode, or a partial schema flush) would create
    // phantom in_degree entries in topo_sort: a missing src strands its dst
    // (never emitted), a missing dst reaches degree 0 and emit_node is called
    // with an absent node ID. Surface the inconsistency as a clean load failure.
    for &(src, dst, _port) in &edges {
        if !nodes.contains_key(&src) || !nodes.contains_key(&dst) {
            return Err(fail());
        }
    }

    topo_sorted(out_schema, nodes, edges)
}

// ---------------------------------------------------------------------------
// Topological sort (Kahn's algorithm)
// ---------------------------------------------------------------------------

/// Assemble the sorted circuit — the only form a `LoadedCircuit` exists in, so
/// no caller can read `ordered` or the adjacency maps before they are populated.
pub(super) fn topo_sorted(
    out_schema: SchemaDescriptor,
    nodes: HashMap<i32, gnitz_wire::OpNode>,
    edges: Vec<(i32, i32, i32)>,
) -> Result<LoadedCircuit, CompileError> {
    let mut outgoing: HashMap<i32, Vec<(i32, i32)>> = HashMap::new();
    let mut incoming: HashMap<i32, Vec<(i32, i32)>> = HashMap::new();
    let mut in_degree: HashMap<i32, i32> = HashMap::new();
    for &nid in nodes.keys() {
        in_degree.insert(nid, 0);
        outgoing.entry(nid).or_default();
        incoming.entry(nid).or_default();
    }

    for &(src, dst, port) in &edges {
        outgoing.entry(src).or_default().push((dst, port));
        incoming.entry(dst).or_default().push((src, port));
        *in_degree.entry(dst).or_insert(0) += 1;
    }

    let mut init: Vec<i32> = nodes
        .keys()
        .filter(|&&nid| *in_degree.get(&nid).unwrap_or(&0) == 0)
        .copied()
        .collect();
    init.sort_unstable(); // deterministic order for tied sources
    let mut queue: VecDeque<i32> = init.into();

    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some(nid) = queue.pop_front() {
        ordered.push(nid);
        if let Some(outs) = outgoing.get(&nid) {
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

    if ordered.len() != nodes.len() {
        return Err(CompileError::Cycle);
    }
    Ok(LoadedCircuit {
        out_schema,
        nodes,
        ordered,
        outgoing,
        incoming,
    })
}

// ---------------------------------------------------------------------------
// Annotation passes
// ---------------------------------------------------------------------------

/// The scatter key of the source scanned at `scan_nid`: the reindex columns of
/// every `ScatterKey` `Map` reachable forward through `Filter`s, and whether the
/// walk passed an `Auxiliary` one without finding any `ScatterKey`.
///
/// The planner marks which reindex is a source's join/group key
/// ([`gnitz_wire::ReindexRole`]) — it holds that fact at the call site, where the
/// engine could only infer it from graph shape. A source that participates in
/// several joins on different keys (`t JOIN t1 ON t.a = t1.x JOIN t2 ON t.b =
/// t2.y`) fans out into several `ScatterKey` Maps and their sequences
/// concatenate. `Filter` is walked through because the planner emits
/// `Filter → Map(reindex)` chains for PK-redistribution views.
///
/// The second return is the shape a planner call site that forgot its role
/// produces; `compile_view` rejects on it, which turns the likelier of the two
/// possible mistakes into a failed compile instead of silently-unscattered rows.
pub(super) fn scatter_key_of_scan(loaded: &LoadedCircuit, scan_nid: i32) -> (Vec<Vec<(u32, u8)>>, bool) {
    let mut queue = VecDeque::from([scan_nid]);
    // `visited` bounds the walk to O(nodes): without it a Filter diamond (two
    // edge paths reaching the same Filter) would re-push and re-expand nodes.
    let mut visited = HashSet::new();
    // Each `ScatterKey` Map contributes its full key sequence. Distinct sequences
    // are concatenated; an identical sequence reached again (the null/not-null
    // sibling Maps of a nullable LEFT-join key) is added once. Within a sequence,
    // duplicate columns are PRESERVED (an overlapping key `a.x = b.p AND a.x =
    // b.q` reindexes `[x, x]`, possibly with distinct per-slot promotion targets),
    // so the result mirrors the trace-side ReindexPacker slot-for-slot.
    let mut seqs: Vec<Vec<(u32, u8)>> = Vec::new();
    let mut saw_auxiliary = false;
    while let Some(cur) = queue.pop_front() {
        if !visited.insert(cur) {
            continue;
        }
        let Some(outs) = loaded.outgoing.get(&cur) else {
            continue;
        };
        for &(dst, _port) in outs {
            match loaded.nodes.get(&dst) {
                Some(gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                    reindex_cols,
                    reindex_target_tcs,
                    role,
                    ..
                })) if !reindex_cols.is_empty() => {
                    if *role != gnitz_wire::ReindexRole::ScatterKey {
                        saw_auxiliary = true;
                        continue;
                    }
                    let seq: Vec<(u32, u8)> = reindex_cols
                        .iter()
                        .enumerate()
                        .map(|(i, &rc)| (rc as u32, reindex_target_tcs.get(i).copied().unwrap_or(0)))
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
    let orphaned = seqs.is_empty() && saw_auxiliary;
    (seqs, orphaned)
}

/// One scan's `ScatterKey` sequences concatenated — the projection
/// [`super::optimize::compute_scatter_routing`] builds the co-partition prefix
/// test from, isolated to one scan node so the tests can pin the walk itself.
#[cfg(test)]
pub(super) fn co_partition_keys(loaded: &LoadedCircuit, scan_nid: i32) -> Vec<(u32, u8)> {
    scatter_key_of_scan(loaded, scan_nid).0.into_iter().flatten().collect()
}

/// True iff some scan reaches reindex `Map`s and none of them is a `ScatterKey`.
///
/// Deliberately not conditioned on the circuit carrying a `Join`: a GROUP BY or
/// PK-redistribution circuit's group reindex is equally load-bearing for routing,
/// and conditioning on `Join` would leave exactly those silently unscattered.
pub(super) fn scan_reaches_only_unflagged_reindexes(loaded: &LoadedCircuit) -> bool {
    loaded.ordered.iter().any(|&nid| {
        matches!(loaded.nodes.get(&nid), Some(gnitz_wire::OpNode::ScanDelta { .. }))
            && scatter_key_of_scan(loaded, nid).1
    })
}

/// Walk back from an `ExchangeShard` (`enid`) through `Filter` nodes to the
/// source `ScanDelta`, returning its table id — or `None` on a fan-in (≠ 1
/// incoming edge) or any non-`Filter`, non-scan node. `Filter` is the only
/// operator transparent to the shard key: row-selective, never re-keys the PK
/// region, never moves a row
/// off-worker; Map/Reduce/Distinct/join change the key or its distribution, and a
/// `WorkerFilter` (range-join broadcast input) is not a `Filter` either. The
/// backward dual of `reindex_cols_through_filters`, reading the same `loaded.incoming`
/// adjacency as `ancestors_inclusive` / `exchange_input_node`. The chain is acyclic —
/// both `compile_view` and `load_meta_circuit` reject a malformed cyclic circuit (the
/// latter presents it as empty, so this walk is never entered on one) — and each hop
/// has exactly one incoming edge, so it terminates without a visited guard.
pub(super) fn scan_tid_through_filters(loaded: &LoadedCircuit, enid: i32) -> Option<i64> {
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
            Some(gnitz_wire::OpNode::ScanDelta { source: t, .. }) => return Some(*t as i64),
            Some(gnitz_wire::OpNode::Filter(_)) => cur = src_nid,
            _ => return None,
        }
    }
}

/// The output `ExchangeShard`'s `(node id, shard columns)` — the sink-nearest
/// one in topo order — or `None` when the circuit carries no shard at all.
///
/// `loaded` MUST be `topo_sort`ed. A circuit may carry several shards (a
/// two-sided set-op emits one per side), so picking by `nodes` hash order would
/// return an arbitrary one; every caller wants the one whose key the output
/// carries.
pub(crate) fn output_exchange_shard(loaded: &LoadedCircuit) -> Option<(i32, Vec<u32>)> {
    loaded
        .ordered
        .iter()
        .rev()
        .find_map(|&nid| match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::ExchangeShard { shard_cols }) => {
                Some((nid, shard_cols.iter().map(|&c| c as u32).collect()))
            }
            _ => None,
        })
}

/// The first node in topological order for which `f` yields a value. Topological
/// order rather than `nodes` iteration order because a `HashMap` walk is keyed by
/// a hasher, so which of several matching nodes wins would be an implementation
/// detail of the map rather than a property of the circuit.
fn find_in_topo_order<T>(loaded: &LoadedCircuit, f: impl Fn(&gnitz_wire::OpNode) -> Option<T>) -> Option<T> {
    loaded.ordered.iter().find_map(|nid| loaded.nodes.get(nid).and_then(&f))
}

/// The equality-conjunct count `n_eq` of a non-equi (range / band) join, or
/// `None` if the loaded meta-circuit is not one — read straight off its
/// `Join(DeltaTraceRange { n_eq, .. })` node (both bilinear terms carry the same
/// `n_eq`, so which one the walk reaches is not observable). `Some` is the
/// precise range-join discriminator for the dag driver
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
    find_in_topo_order(loaded, |op| match op {
        gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTraceRange { n_eq, .. }) => Some(*n_eq),
        _ => None,
    })
}

/// The `(source table id, bound)` the planner pushed onto a `ScanDelta`'s
/// backfill scan. At most ONE exists by construction — only the builder's
/// primary source can carry a bound (`input_delta_bounded`); tagged secondary
/// inputs never do — so the type says so. A **hint**: `None` means "full-scan",
/// which is always correct, so a hand-crafted circuit with several bounded scans
/// is harmless — it takes the topologically first and the rest degrade to full
/// scans, the same way on every worker.
pub(super) fn circuit_source_bound(loaded: &LoadedCircuit) -> Option<(i64, gnitz_wire::ScanBound)> {
    find_in_topo_order(loaded, |op| match op {
        gnitz_wire::OpNode::ScanDelta { source, bound: Some(b) } => Some((*source as i64, *b)),
        _ => None,
    })
}
