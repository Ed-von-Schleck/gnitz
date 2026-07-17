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
fn open_system_cursor(table: *mut Table) -> Option<ReadCursor> {
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
    let nodes_cur = open_system_cursor(sys_nodes)?;
    let edges_cur = open_system_cursor(sys_edges)?;
    let node_cols_cur = open_system_cursor(sys_node_cols)?;
    assemble_circuit(nodes_cur, edges_cur, node_cols_cur, view_id, out_schema)
}

/// Build a `LoadedCircuit` from the three circuit families delivered **in a
/// `FLAG_RUN_TRANSIENT` frame** — fresh, untrusted, per-request wire input,
/// reusing the exact assembly the sys-table path uses. Each already-decoded
/// family batch is wrapped in a standalone `ReadCursor` (`from_owned`) — no
/// `Table`, no scratch dir, no `mkdir` — and the same `(view_id, sub)` OPK-prefix
/// filter applies (the client packs the provisional `view_id` in the compound
/// PK's high half exactly as CREATE VIEW does). The node-id `i32` reject in
/// `assemble_circuit` guards the trust boundary.
pub(crate) fn build_loaded_from_batches(
    nodes_batch: std::rc::Rc<crate::storage::Batch>,
    edges_batch: std::rc::Rc<crate::storage::Batch>,
    node_cols_batch: std::rc::Rc<crate::storage::Batch>,
    view_id: u64,
    out_schema: SchemaDescriptor,
) -> Option<LoadedCircuit> {
    // The circuit-family schemas come from the shared wire-column builder
    // (`schema::from_wire_cols`, compound `(view_id, sub)` PK) — the same
    // builder the catalog's sys-table `SCHEMAS` statics use, so the cursor
    // reads the client-encoded batch with byte-identical layout.
    let fam = |cols| crate::schema::from_wire_cols(cols, gnitz_wire::CIRCUIT_FAMILY_PK);
    let nodes_cur = ReadCursor::from_owned(&[nodes_batch], fam(gnitz_wire::CIRCUIT_NODES_COLS));
    let edges_cur = ReadCursor::from_owned(&[edges_batch], fam(gnitz_wire::CIRCUIT_EDGES_COLS));
    let node_cols_cur = ReadCursor::from_owned(&[node_cols_batch], fam(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS));
    assemble_circuit(nodes_cur, edges_cur, node_cols_cur, view_id, out_schema)
}

/// Non-negative-`i32` node-id gate. A durable sys-table id is always in range;
/// a transient's frame is untrusted, so an id that would truncate into a
/// colliding engine key is rejected (returns `None`, aborting the load) rather
/// than silently wrapped.
#[inline]
fn node_id_i32(v: i64) -> Option<i32> {
    if (0..=i32::MAX as i64).contains(&v) {
        Some(v as i32)
    } else {
        None
    }
}

/// Read three positioned circuit cursors (filtered by the `view_id` OPK prefix)
/// into a `LoadedCircuit` — the shared body behind both `load_circuit` (sys
/// tables) and `build_loaded_from_batches` (transient frame), so the
/// node-column `(kind, position)` sort, the `decode_op_node` calls, the node-id
/// `i32` reject, and the edge-validity check exist exactly once.
fn assemble_circuit(
    mut nodes_cur: ReadCursor,
    mut edges_cur: ReadCursor,
    mut node_cols_cur: ReadCursor,
    view_id: u64,
    out_schema: SchemaDescriptor,
) -> Option<LoadedCircuit> {
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
    node_cols_cur.for_each_positive_with_prefix(&prefix, |ch| match node_id_i32(ch.read_i64(NODECOL_COL_NODE_ID)) {
        Some(nid) => cols_by_node
            .entry(nid)
            .or_default()
            .push(gnitz_wire::CircuitNodeColumn {
                kind: ch.read_i64(NODECOL_COL_KIND) as u64,
                position: ch.read_i64(NODECOL_COL_POSITION) as u16,
                value1: ch.read_i64(NODECOL_COL_VALUE1) as u64,
                value2: ch.read_i64(NODECOL_COL_VALUE2) as u64,
            }),
        None => invalid = true,
    });
    // Sort each node's cols by (kind, position) so decode_op_node sees ordered slices.
    for v in cols_by_node.values_mut() {
        v.sort_by_key(|c| (c.kind, c.position));
    }

    // Phase 2: read CircuitNodes; call decode_op_node for each.
    nodes_cur.for_each_positive_with_prefix(&prefix, |ch| {
        let Some(node_id) = node_id_i32(ch.read_i64(NODES_COL_NODE_ID)) else {
            invalid = true;
            return;
        };
        let opcode = ch.read_i64(NODES_COL_OPCODE_NEW) as u64;

        let src_tab: Option<u64> = if ch.col_is_null(NODES_COL_SOURCE_TABLE) {
            None
        } else {
            Some(ch.read_i64(NODES_COL_SOURCE_TABLE) as u64)
        };
        let expr_blob: Option<Vec<u8>> = if ch.col_is_null(NODES_COL_EXPR_PROGRAM) {
            None
        } else {
            let b = ch.read_german_bytes(NODES_COL_EXPR_PROGRAM);
            if b.is_empty() {
                None
            } else {
                Some(b)
            }
        };

        let cols = cols_by_node.get(&node_id).map(|v| v.as_slice()).unwrap_or(&[]);
        match gnitz_wire::decode_op_node(opcode, src_tab, expr_blob, cols) {
            Ok(op) => {
                nodes.insert(node_id, op);
            }
            Err(_) => invalid = true,
        }
    });
    if invalid {
        return None;
    }

    // Phase 3: read CircuitEdges. A truncating endpoint id aborts the load.
    edges_cur.for_each_positive_with_prefix(&prefix, |ch| {
        match (
            node_id_i32(ch.read_i64(EDGES_COL_SRC_NODE)),
            node_id_i32(ch.read_i64(EDGES_COL_DST_NODE)),
        ) {
            (Some(src), Some(dst)) => edges.push((src, dst, ch.read_i64(EDGES_COL_DST_PORT) as i32)),
            _ => invalid = true,
        }
    });
    if invalid {
        return None;
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
        ..Default::default()
    })
}

/// The distinct scan-source table ids of a loaded circuit. With
/// `include_trace: false`, only `ScanDelta` — the same set
/// `Circuit::dependencies()` computes client-side, and the sequence the master
/// drives a transient's sources in (`ScanTrace` is a read-only `ext_trace`
/// lookup that no drive seeds, and a SQL-planner circuit never emits one). With
/// `include_trace: true`, `ScanTrace` targets too — the boot invalid-view
/// verdict's transitive check needs those as well.
///
/// Nodes are walked in ascending node-id order, NOT `loaded.nodes` iteration
/// order: `nodes` is a `HashMap`, so iterating it directly would return the
/// sources in a per-process-random sequence and make the drive order
/// irreproducible across runs of the same query. Node ids are assigned by the
/// client's circuit builder in construction order, so ascending id tracks the
/// circuit's own build order, and the walk needs no `topo_sort` precondition
/// (unlike reading `loaded.ordered`, which is empty until one runs — a silent
/// "no sources" footgun).
///
/// The order is deterministic but NOT correctness-load-bearing: the master
/// drives one source per epoch, and each epoch's delta joins against the other
/// sources' already-accumulated traces (the symmetric 2-term bilinear form), so
/// whichever source is driven last sees every other side in full. Every order
/// yields the same Z-set, each cross term emitted exactly once.
pub(crate) fn scan_source_ids(loaded: &LoadedCircuit, include_trace: bool) -> Vec<i64> {
    let mut nids: Vec<i32> = loaded.nodes.keys().copied().collect();
    nids.sort_unstable();
    let mut out: Vec<i64> = Vec::new();
    for nid in nids {
        let t = match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::ScanDelta { source, .. }) => *source as i64,
            Some(gnitz_wire::OpNode::ScanTrace(t)) if include_trace => *t as i64,
            _ => continue,
        };
        if !out.contains(&t) {
            out.push(t);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Topological sort (Kahn's algorithm)
// ---------------------------------------------------------------------------

pub(crate) fn topo_sort(loaded: &mut LoadedCircuit) -> Result<(), CompileError> {
    // Rebuild the derived fields from scratch so the sort is idempotent — a
    // second run must not append duplicate adjacency entries.
    loaded.ordered.clear();
    loaded.outgoing.clear();
    loaded.incoming.clear();
    let mut in_degree: HashMap<i32, i32> = HashMap::new();
    for &nid in loaded.nodes.keys() {
        in_degree.insert(nid, 0);
        loaded.outgoing.entry(nid).or_default();
        loaded.incoming.entry(nid).or_default();
    }

    for &(src, dst, port) in &loaded.edges {
        loaded.outgoing.entry(src).or_default().push((dst, port));
        loaded.incoming.entry(dst).or_default().push((src, port));
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
    //
    // Two probe-support consumers count like the trace/probe itself (they appear only
    // in pure-range circuits, where the reindex may reach the trace/probe through no
    // other edge — the pure-range EXISTS circuit has exactly these two shapes):
    //   - a `PartitionFilter` whose own consumer is the trace/probe (the broadcast
    //     trimmer between a scattered reindex and its integrate);
    //   - a `Map(HashRow)` (the head of the inline `m = MAX/MIN(b.range)` threshold
    //     reduce over the broadcast side).
    // Neither re-classifies any other reindex: the pure-range LEFT join's sibling
    // reindexes already qualify directly (same key, deduped), and the null-key /
    // null-fill re-keys feed a PartitionFilter-into-Union or a plain Map, not these.
    let is_join_view = loaded
        .nodes
        .values()
        .any(|op| matches!(op, gnitz_wire::OpNode::Join(_)));
    let is_trace_or_join = |nid: i32| {
        matches!(
            loaded.nodes.get(&nid),
            Some(gnitz_wire::OpNode::IntegrateTrace | gnitz_wire::OpNode::Join(_))
        )
    };
    let feeds_trace_or_join = |nid: i32| {
        loaded.outgoing.get(&nid).is_some_and(|outs| {
            outs.iter().any(|&(d, _)| {
                is_trace_or_join(d)
                    || match loaded.nodes.get(&d) {
                        Some(gnitz_wire::OpNode::PartitionFilter) => loaded
                            .outgoing
                            .get(&d)
                            .is_some_and(|outs2| outs2.iter().any(|&(d2, _)| is_trace_or_join(d2))),
                        Some(gnitz_wire::OpNode::Map(gnitz_wire::MapKind::HashRow(..))) => true,
                        _ => false,
                    }
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
            Some(gnitz_wire::OpNode::ScanDelta { source: t, .. }) | Some(gnitz_wire::OpNode::ScanTrace(t)) => {
                return Some(*t as i64)
            }
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

/// The `(source table id, bound)` the planner pushed onto a `ScanDelta`'s
/// backfill scan. At most ONE exists by construction — only the builder's
/// primary source can carry a bound (`input_delta_bounded`); tagged secondary
/// inputs never do — so the type says so. A **hint**: `None` means "full-scan",
/// which is always correct, and a hand-crafted circuit with several bounded
/// scans yields an arbitrary one (harmless — the rest degrade to full scans).
pub(crate) fn circuit_source_bound(loaded: &LoadedCircuit) -> Option<(i64, gnitz_wire::ScanBound)> {
    loaded.nodes.values().find_map(|op| match op {
        gnitz_wire::OpNode::ScanDelta { source, bound: Some(b) } => Some((*source as i64, *b)),
        _ => None,
    })
}
