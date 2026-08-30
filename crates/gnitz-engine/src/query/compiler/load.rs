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
/// `decode_op_node` calls and the node-id `i32` reject. The edge set is then
/// `topo_sorted`'s to validate.
pub(crate) fn load_circuit(sys: SysTableRefs, view_id: u64) -> Result<LoadedCircuit, CompileError> {
    let fail = || CompileError::Rejected("circuit load failed");
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

    topo_sorted(nodes, edges)
}

// ---------------------------------------------------------------------------
// Topological sort (Kahn's algorithm)
// ---------------------------------------------------------------------------

/// Assemble the sorted circuit — the only form a `LoadedCircuit` exists in, so
/// no caller can read `ordered` or the adjacency maps before they are populated.
pub(super) fn topo_sorted(
    nodes: HashMap<i32, gnitz_wire::OpNode>,
    edges: Vec<(i32, i32, i32)>,
) -> Result<LoadedCircuit, CompileError> {
    let mut outgoing: HashMap<i32, Vec<(i32, i32)>> = HashMap::new();
    let mut incoming: HashMap<i32, Vec<(i32, i32)>> = HashMap::new();
    for &nid in nodes.keys() {
        outgoing.entry(nid).or_default();
        incoming.entry(nid).or_default();
    }

    for &(src, dst, port) in &edges {
        // A dangling endpoint — a node that failed to decode, a partial schema
        // flush, a hand-built fixture — would strand its partner rather than fail,
        // so it is rejected before it can become a phantom adjacency entry.
        let (Some(outs), Some(ins)) = (outgoing.get_mut(&src), incoming.get_mut(&dst)) else {
            return Err(CompileError::Rejected("edge endpoint is not a node of the circuit"));
        };
        outs.push((dst, port));
        ins.push((src, port));
    }

    let inputs = resolve_inputs(&nodes, &incoming)?;

    let mut in_degree: HashMap<i32, i32> = incoming.iter().map(|(&nid, ins)| (nid, ins.len() as i32)).collect();

    let mut init: Vec<i32> = nodes.keys().filter(|nid| in_degree[nid] == 0).copied().collect();
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
        return Err(CompileError::Rejected("circuit graph has a cycle"));
    }
    Ok(LoadedCircuit {
        nodes,
        ordered,
        outgoing,
        inputs,
    })
}

/// Hold every node's incoming edges to the port set its operator declares, and
/// return them as [`NodeInputs`].
///
/// Rejecting a duplicate port, a missing one and an out-of-range one in one place
/// is what lets the emit layer destructure an operand instead of asking whether it
/// exists. The durable form already enforces uniqueness — the `CircuitEdges` PK is
/// `(view_id, dst_node, dst_port)` — but a hand-built circuit does not, so the
/// check lives here, in the only constructor.
fn resolve_inputs(
    nodes: &HashMap<i32, gnitz_wire::OpNode>,
    incoming: &HashMap<i32, Vec<(i32, i32)>>,
) -> Result<HashMap<i32, NodeInputs>, CompileError> {
    let malformed = || CompileError::Rejected("node's input edges do not match its operator's ports");
    nodes
        .iter()
        .map(|(&nid, op)| {
            // Ports are 0 and 1 (`PORT_IN == PORT_IN_A`, `PORT_TRACE == PORT_IN_B`),
            // so a port beyond the set and a duplicate one are the same rejection.
            let mut by_port: [Option<i32>; 2] = [None, None];
            for &(src, port) in incoming.get(&nid).into_iter().flatten() {
                match by_port.get_mut(port as usize) {
                    Some(slot @ None) => *slot = Some(src),
                    _ => return Err(malformed()),
                }
            }
            // Matching the operator's arity against the filled slots IS the
            // port-set equality: a missing port leaves its slot empty.
            let resolved = match (op.ports(), by_port) {
                ([], [None, None]) => NodeInputs::Source,
                ([_], [Some(src), None]) => NodeInputs::Unary(src),
                ([_, _], [Some(a), Some(b)]) => NodeInputs::Binary { a, b },
                _ => return Err(malformed()),
            };
            Ok((nid, resolved))
        })
        .collect()
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
                Some(gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
                    reindex_cols,
                    reindex_target_tcs,
                    role,
                    ..
                })) => {
                    if *role != gnitz_wire::ReindexRole::ScatterKey {
                        saw_auxiliary = true;
                        continue;
                    }
                    let seq: Vec<(u32, u8)> = reindex_cols
                        .iter()
                        .enumerate()
                        .map(|(i, &rc)| (rc, reindex_target_tcs.get(i).copied().unwrap_or(0)))
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

/// Walk back from an `ExchangeShard` (`enid`) through `Filter` nodes to the
/// source `ScanDelta`, returning its table id — or `None` on a fan-in (≠ 1
/// incoming edge) or any non-`Filter`, non-scan node. `Filter` is the only
/// operator transparent to the shard key: row-selective, never re-keys the PK
/// region, never moves a row
/// off-worker; Map/Reduce/Distinct/join change the key or its distribution, and a
/// `WorkerFilter` (range-join broadcast input) is not a `Filter` either. The
/// backward dual of `scatter_key_of_scan`, reading the same `loaded.incoming`
/// adjacency as `ancestors_inclusive`. The chain is acyclic —
/// both `compile_view` and `load_meta_circuit` reject a malformed cyclic circuit (the
/// latter presents it as empty, so this walk is never entered on one) — and each hop
/// has exactly one incoming edge, so it terminates without a visited guard.
pub(super) fn scan_tid_through_filters(loaded: &LoadedCircuit, enid: i32) -> Option<i64> {
    let mut cur = enid;
    loop {
        // Bail on a fan-in: a multi-input node (Union, set op) draws from more
        // than one source, so no single table's distribution prefix governs the
        // shard key and it can never co-partition.
        let NodeInputs::Unary(src_nid) = *loaded.inputs(cur) else {
            return None;
        };
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
pub(super) fn output_exchange_shard(loaded: &LoadedCircuit) -> Option<(i32, Vec<u32>)> {
    loaded
        .ordered
        .iter()
        .rev()
        .find_map(|&nid| match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::ExchangeShard { shard_cols }) => Some((nid, shard_cols.clone())),
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
/// reindex matches `scatter_key_of_scan`, and the view has an output
/// `ExchangeShard`), so keying on it would divert those views into the relay
/// path and corrupt them. Read once into `ViewMeta::range_join_n_eq`, which is
/// what every runtime caller consults.
pub(super) fn circuit_range_join_n_eq(loaded: &LoadedCircuit) -> Option<u8> {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::type_code;

    #[test]
    fn test_topo_sort_simple() {
        let nodes = HashMap::from([
            (0, scan_delta(0)),
            (1, gnitz_wire::OpNode::Filter(None)),
            (2, gnitz_wire::OpNode::IntegrateSink),
        ]);
        let loaded = loaded_for_test(nodes, vec![(0, 1, 0), (1, 2, 0)]);
        assert_eq!(loaded.ordered, vec![0, 1, 2]);
    }

    #[test]
    fn test_topo_sort_cycle() {
        let nodes = HashMap::from([
            (0, gnitz_wire::OpNode::Filter(None)),
            (1, gnitz_wire::OpNode::Filter(None)),
        ]);
        assert!(matches!(
            load::topo_sorted(nodes, vec![(0, 1, 0), (1, 0, 0)]),
            Err(CompileError::Rejected("circuit graph has a cycle"))
        ));
    }

    // ── Items 16 & 28: load_circuit robustness (real system tables) ─────────

    /// The three circuit system tables `load_circuit` reads, on one tempdir.
    /// All three must be live: `load_circuit` opens a cursor over each up front
    /// and returns `None` on a null one, which would pass these assertions
    /// vacuously. Their schemas differ (6/5/7 columns), so one cannot stand in
    /// for another.
    struct CircuitTables {
        _tmp: tempfile::TempDir,
        nodes: Table,
        edges: Table,
        cols: Table,
    }

    impl CircuitTables {
        const VIEW_ID: u64 = 1;

        /// Match `pack_view_pk`: view_id in the high half, so its at-rest OPK
        /// (big-endian) image leads the PK region where `load_circuit` seeks.
        fn pk(sub: u64) -> u128 {
            ((Self::VIEW_ID as u128) << 64) | (sub as u128)
        }

        fn schema(cols: &[gnitz_wire::WireSysCol]) -> SchemaDescriptor {
            crate::schema::from_wire_cols(cols, gnitz_wire::CIRCUIT_FAMILY_PK)
        }

        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let open = |name: &str, cols: &[gnitz_wire::WireSysCol]| {
                // The `TempDir` outlives these: each `ShardIndex` holds the path.
                Table::new(
                    &format!("{}/{name}", tmp.path().to_str().unwrap()),
                    Self::schema(cols),
                    0,
                    RecoverySource::Rederive { resume_at: None },
                )
                .unwrap()
            };
            let nodes = open("nodes", gnitz_wire::CIRCUIT_NODES_COLS);
            let edges = open("edges", gnitz_wire::CIRCUIT_EDGES_COLS);
            let cols = open("cols", gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS);
            Self {
                _tmp: tmp,
                nodes,
                edges,
                cols,
            }
        }

        fn fill(tab: &mut Table, cols: &[gnitz_wire::WireSysCol], f: impl FnOnce(&mut crate::storage::BatchBuilder)) {
            let mut bb = crate::storage::BatchBuilder::new(Self::schema(cols));
            f(&mut bb);
            tab.ingest_owned_batch(bb.finish()).unwrap();
        }

        fn put_nodes(&mut self, f: impl FnOnce(&mut crate::storage::BatchBuilder)) -> &mut Self {
            Self::fill(&mut self.nodes, gnitz_wire::CIRCUIT_NODES_COLS, f);
            self
        }

        fn put_edges(&mut self, f: impl FnOnce(&mut crate::storage::BatchBuilder)) -> &mut Self {
            Self::fill(&mut self.edges, gnitz_wire::CIRCUIT_EDGES_COLS, f);
            self
        }

        fn load(&mut self) -> Result<LoadedCircuit, CompileError> {
            load_circuit(
                SysTableRefs {
                    nodes: &mut self.nodes,
                    edges: &mut self.edges,
                    node_columns: &mut self.cols,
                },
                Self::VIEW_ID,
            )
        }
    }

    #[test]
    fn test_load_circuit_aborts_on_undecodable_node() {
        // An opcode `decode_op_node` rejects must abort the whole load, not be
        // skipped into a partial circuit.
        let mut c = CircuitTables::new();
        c.put_nodes(|bb| {
            bb.begin_row(CircuitTables::pk(1), 1);
            bb.put_u64(1); // node_id
            bb.put_u64(9999); // opcode — unknown → decode_op_node Err
            bb.put_null(); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
        });
        assert!(c.load().is_err(), "an undecodable node must abort load_circuit");
    }

    #[test]
    fn test_load_circuit_keeps_empty_expr_blob_present() {
        // A non-NULL expr_program that reads back empty is a damaged blob. The
        // load must hand it on as `Some`, since `None` is how an absent program
        // is spelled and would turn this Filter into `WHERE TRUE`; rejecting the
        // undecodable blob is the compile's job (see the corrupt-blob tests).
        let mut c = CircuitTables::new();
        c.put_nodes(|bb| {
            bb.begin_row(CircuitTables::pk(0), 1);
            bb.put_u64(0); // node_id
            bb.put_u64(gnitz_wire::OPCODE_SCAN_DELTA);
            bb.put_u64(10); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
            bb.begin_row(CircuitTables::pk(1), 1);
            bb.put_u64(1); // node_id
            bb.put_u64(gnitz_wire::OPCODE_FILTER);
            bb.put_null(); // source_table
            bb.put_blob(&[]); // expr_program — non-NULL, zero length
            bb.end_row();
        })
        .put_edges(|bb| {
            bb.begin_row(CircuitTables::pk(0), 1);
            bb.put_u64(1); // dst_node
            bb.put_u64(PORT_IN as u64);
            bb.put_u64(0); // src_node
            bb.end_row();
        });
        let loaded = c.load().expect("a damaged blob is not a load failure");
        assert!(
            matches!(loaded.nodes.get(&1), Some(gnitz_wire::OpNode::Filter(Some(b))) if b.is_empty()),
            "an empty blob must stay present, not collapse to a pass-all filter"
        );
    }

    #[test]
    fn test_load_circuit_aborts_on_orphan_edge() {
        // An edge whose dst does not exist must abort rather than create a
        // phantom node.
        let mut c = CircuitTables::new();
        c.put_nodes(|bb| {
            // node 0: ScanDelta(source 99)
            bb.begin_row(CircuitTables::pk(0), 1);
            bb.put_u64(0);
            bb.put_u64(gnitz_wire::OPCODE_SCAN_DELTA);
            bb.put_u64(99); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
            // node 1: IntegrateSink
            bb.begin_row(CircuitTables::pk(1), 1);
            bb.put_u64(1);
            bb.put_u64(gnitz_wire::OPCODE_INTEGRATE);
            bb.put_null(); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
        })
        .put_edges(|bb| {
            // Edge 0 → 7, but node 7 does not exist.
            bb.begin_row(CircuitTables::pk(0), 1);
            bb.put_u64(7); // dst_node (orphan)
            bb.put_u64(PORT_IN as u64);
            bb.put_u64(0); // src_node
            bb.end_row();
        });
        assert!(
            c.load().is_err(),
            "an edge to a non-existent node must abort load_circuit"
        );
    }

    /// A reduce owns its trace-in. `reduce_node` wires `PORT_IN` only and
    /// `PORT_TRACE` is written solely by `binary_join`, but the circuit families
    /// carry no catalog precheck, so a forged bundle can land the edge here.
    /// Honouring it would make the reduce's trace-in a *delta* register with no
    /// `Integrate` behind it, and `cursor_mut!` hard-asserts on the null cursor —
    /// Every node's edge set must be exactly the port set its operator declares.
    /// One load-time check stands in for the arity test each operand read used to
    /// carry, so these four shapes must never reach emission: a `Reduce` given a
    /// forged trace edge would be handed a delta register with no `Integrate`
    /// behind it and abort a worker on the null cursor, and an input-less
    /// `ExchangeShard` has nothing to repartition.
    #[test]
    fn port_set_violations_fail_at_load() {
        use gnitz_wire::OpNode;
        let load = |dst: OpNode, edges: Vec<(i32, i32, i32)>| {
            let nodes = HashMap::from([(0, scan_delta(10)), (1, scan_delta(11)), (2, dst)]);
            load::topo_sorted(nodes, edges)
        };
        let rejected = |r: Result<LoadedCircuit, CompileError>, what: &str| {
            assert!(
                matches!(
                    r,
                    Err(CompileError::Rejected(
                        "node's input edges do not match its operator's ports"
                    ))
                ),
                "{what} must fail at load",
            );
        };
        assert!(load(OpNode::Filter(None), vec![(0, 2, PORT_IN)]).is_ok(), "control");

        rejected(
            load(OpNode::Filter(None), vec![(0, 2, PORT_IN), (1, 2, PORT_IN)]),
            "two edges into one port",
        );
        rejected(load(OpNode::Filter(None), vec![(0, 2, 2)]), "a port beyond the arity");
        rejected(
            load(
                OpNode::Reduce {
                    group_cols: vec![0],
                    agg: vec![(gnitz_wire::AggFunc::Count, 1)],
                    global_ground: false,
                    out_key: crate::schema::ReduceOutKey::PkPermutation,
                },
                vec![(0, 2, PORT_IN), (1, 2, PORT_TRACE)],
            ),
            "a forged Reduce trace edge",
        );
        rejected(
            load(OpNode::ExchangeShard { shard_cols: vec![0] }, vec![]),
            "an input-less ExchangeShard",
        );
        rejected(
            load(OpNode::Union, vec![(0, 2, PORT_IN_A)]),
            "a Union wired on one port",
        );
    }

    #[test]
    fn test_circuit_range_join_n_eq_discriminator() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, JoinKind, MapKind, OpNode};

        // A GROUP BY view: ScanDelta → Map(reindex) → ExchangeShard → Reduce →
        // IntegrateSink. It has BOTH a reindex Map (has_join_shard) AND an
        // ExchangeShard (has_exchange), so the wrong discriminator
        // `has_join_shard && has_exchange` would (incorrectly) call it a range
        // join. circuit_range_join_n_eq must return None — no DeltaTraceRange node.
        let mut gb = HashMap::new();
        gb.insert(0, scan_delta(7));
        gb.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        gb.insert(2, OpNode::ExchangeShard { shard_cols: vec![1] });
        gb.insert(
            3,
            OpNode::Reduce {
                group_cols: vec![1],
                // Only exercises range-join classification, never the reduce
                // output schema/validation; the spec is immaterial here.
                agg: vec![(AggFunc::Count, 0)],
                global_ground: false,
                out_key: ReduceOutKey::SyntheticFold,
            },
        );
        gb.insert(4, OpNode::IntegrateSink);
        let gb_edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN), (3, 4, PORT_IN)];
        let gb_loaded = loaded_for_test(gb, gb_edges);
        assert_eq!(
            load::circuit_range_join_n_eq(&gb_loaded),
            None,
            "GROUP BY view must NOT be classified as a range join"
        );

        // A range join: a Join(DeltaTraceRange) node makes it Some, carrying n_eq.
        let mut rj = HashMap::new();
        rj.insert(0, scan_delta(7));
        rj.insert(1, OpNode::IntegrateTrace);
        rj.insert(
            2,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 2,
                rel: gnitz_wire::RangeRel::Lt,
            }),
        );
        rj.insert(3, OpNode::IntegrateSink);
        rj.insert(4, scan_delta(8));
        let rj_edges = vec![(0, 2, PORT_IN_A), (4, 1, PORT_IN), (1, 2, PORT_TRACE), (2, 3, PORT_IN)];
        let rj_loaded = loaded_for_test(rj, rj_edges);
        assert_eq!(
            load::circuit_range_join_n_eq(&rj_loaded),
            Some(2),
            "a Join(DeltaTraceRange) node classifies the view as a range join, carrying its n_eq"
        );
    }

    #[test]
    fn test_co_partition_keys_with_worker_filter_after_map() {
        use gnitz_wire::{MapKind, OpNode};
        // A route-key Map followed by a WorkerFilter: the walk reaches the Map and
        // returns its cols, then stops — a WorkerFilter is not a Filter, so it is
        // never stepped through. What the Map *feeds* does not enter into it.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(99));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::WorkerFilter);
        nodes.insert(3, OpNode::IntegrateTrace);
        let edges = vec![
            (0, 1, PORT_IN), // ScanDelta → reindex Map
            (1, 2, PORT_IN), // Map → WorkerFilter
            (2, 3, PORT_IN), // WorkerFilter → IntegrateTrace
        ];
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(
            load::scatter_key_of_scan(&loaded, 0).0.concat(),
            vec![(2, 0)],
            "WorkerFilter after the reindex Map must not change the walk result"
        );
    }

    /// Real pure-range-join shape (planner.rs, `n_eq == 0`): the reindex Map feeds
    /// the `Join(DeltaTraceRange)` node DIRECTLY as the delta term AND feeds a
    /// `WorkerFilter → IntegrateTrace` toward the trace term. Its key is collected
    /// once, from the flag — the fan-out is not a second contribution. This is what
    /// keeps the join-shard map non-empty for a pure range join (hence
    /// `prepare_relay`'s `is_join` / `range_n_eq` and the broadcast routing).
    #[test]
    fn test_co_partition_keys_range_join_feeds_join_directly() {
        use gnitz_wire::{JoinKind, MapKind, OpNode};
        // ScanDelta(99) ─► Map(reindex=[2]) ─┬─► Join(DeltaTraceRange)  [delta, PORT_IN_A]
        //                                     └─► WorkerFilter ─► IntegrateTrace ─► Join  [PORT_TRACE]
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(99));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::WorkerFilter);
        nodes.insert(3, OpNode::IntegrateTrace);
        nodes.insert(
            4,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 0,
                rel: gnitz_wire::RangeRel::Le,
            }),
        );
        let edges = vec![
            (0, 1, PORT_IN),    // ScanDelta → reindex Map
            (1, 4, PORT_IN_A),  // reindex Map → Join (delta term, DIRECT edge)
            (1, 2, PORT_IN),    // reindex Map → WorkerFilter (toward the trace)
            (2, 3, PORT_IN),    // WorkerFilter → IntegrateTrace
            (3, 4, PORT_TRACE), // IntegrateTrace → Join (trace term)
        ];
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(
            load::scatter_key_of_scan(&loaded, 0).0.concat(),
            vec![(2, 0)],
            "the reindex Map is reached whether it feeds the Join directly or \
             through a WorkerFilter toward the trace"
        );
    }

    #[test]
    fn test_reindex_col_through_filters_trivial_and_absent() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // Trivial: ScanDelta → Map(reindex) directly (no Filter).
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![3],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(load::scatter_key_of_scan(&loaded, 0).0.concat(), vec![(3, 0)]);

        // Absent: ScanDelta → Map with no reindex columns.
        let mut nodes2 = HashMap::new();
        nodes2.insert(0, scan_delta(7));
        nodes2.insert(
            1,
            OpNode::Map(MapKind::Compute {
                program: dummy_blob,
                out_cols: vec![],
            }),
        );
        let loaded2 = loaded_for_test(nodes2, vec![(0, 1, PORT_IN)]);
        assert!(load::scatter_key_of_scan(&loaded2, 0).0.concat().is_empty());
    }

    /// Multi-join: a single ScanDelta fans out through two reindex Maps on
    /// different columns. Both column IDs must be collected, not just the first.
    #[test]
    fn test_co_partition_keys_multi_join() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(0) ──► Map(reindex_col=2)
        //              └──► Filter ──► Map(reindex_col=5)
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(42));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            3,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![5],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        let edges = vec![(0, 1, PORT_IN), (0, 2, PORT_IN), (2, 3, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        let mut got = load::scatter_key_of_scan(&loaded, 0).0.concat();
        got.sort_unstable();
        assert_eq!(got, vec![(2, 0), (5, 0)], "both reindex columns must be collected");
    }

    /// An overlapping key (`a.x = b.p AND a.x = b.q`) reindexes `[x, x]`, possibly
    /// with distinct per-slot promotion targets. The sequence must survive
    /// VERBATIM — duplicates and all — so the scatter packer mirrors the trace-side
    /// ReindexPacker slot-for-slot; column-level dedup would collapse it to one.
    #[test]
    fn test_co_partition_keys_overlapping_key_verbatim() {
        use gnitz_wire::{MapKind, OpNode};
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(42));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![3, 3],
                reindex_target_tcs: vec![0, type_code::I64],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(
            load::scatter_key_of_scan(&loaded, 0).0.concat(),
            vec![(3, 0), (3, type_code::I64)],
            "overlapping key sequence must survive verbatim, not be deduplicated"
        );
    }

    /// A nullable LEFT-join key fans its source to two sibling reindex Maps (the
    /// not-null match side and the null-key bypass) carrying an IDENTICAL sequence.
    /// They must collapse to ONE copy, never be concatenated (which would double
    /// the key columns and diverge from the trace).
    #[test]
    fn test_co_partition_keys_sibling_maps_collapse() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(7) ──► Filter(not-null) ──► Map(reindex [2])
        //              └──► Filter(is-null)  ──► Map(reindex [2])  (identical seq)
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![2],
                reindex_target_tcs: vec![0],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(3, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            4,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![2],
                reindex_target_tcs: vec![0],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (0, 3, PORT_IN), (3, 4, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(
            load::scatter_key_of_scan(&loaded, 0).0.concat(),
            vec![(2, 0)],
            "identical sibling sequences must collapse to one, not concatenate"
        );
    }

    /// Band LEFT join shape: the left scan feeds BOTH the join reindex (`[eq, range]`)
    /// AND an auxiliary `a.pk` re-key for the null-fill. Only the join reindex defines
    /// the input scatter key, and the planner says so by flagging one and not the
    /// other — concatenating both would corrupt the eq-prefix scatter.
    #[test]
    fn test_co_partition_keys_ignores_aux_rekey_in_join_view() {
        use gnitz_wire::{JoinKind, MapKind, OpNode, RangeRel};
        // ScanDelta(10) ──► Map(reindex [1,2]) ──► Join(DeltaTraceRange)
        //              │                       └─► IntegrateTrace
        //              └──► Map(reindex [0]) ──► Map(Projection) ──► Distinct   (a_all → proj_a → D)
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![1, 2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(
            2,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 1,
                rel: RangeRel::Le,
            }),
        );
        nodes.insert(3, OpNode::IntegrateTrace);
        nodes.insert(
            4,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![0],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::Auxiliary,
            }),
        );
        nodes.insert(5, OpNode::Map(MapKind::Projection(vec![])));
        nodes.insert(6, OpNode::Distinct);
        let edges = vec![
            (0, 1, PORT_IN),
            (1, 2, PORT_IN_A),
            (1, 3, PORT_IN), // join reindex → its own integral
            (3, 2, PORT_TRACE),
            (0, 4, PORT_IN),
            (4, 5, PORT_IN),
            (5, 6, PORT_IN), // aux a.pk re-key → proj → distinct
        ];
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(
            load::scatter_key_of_scan(&loaded, 0).0.concat(),
            vec![(1, 0), (2, 0)],
            "only the trace/probe-feeding reindex defines the scatter key; the a.pk re-key is ignored"
        );
    }

    // ── scan_tid_through_filters: the backward (shard → scan) Filter walk ───────
    //
    // The view exchange-skip detector's source resolution. The skip itself has no
    // observable "fired" signal at the E2E layer (exchanging is also correct), so
    // these unit tests are what pin that the walk engages exactly when it should:
    // through Filter chains, never across a re-keying Map / WorkerFilter / fan-in.

    /// A bare `ScanDelta → ExchangeShard` (the no-`WHERE` case) resolves to the source
    /// tid; `ScanDelta → Filter → ExchangeShard` (filtered `GROUP BY prefix`) does too,
    /// as does a chain of Filters.
    #[test]
    fn test_scan_tid_through_filters_filter_chain() {
        use gnitz_wire::OpNode;
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(7) → ExchangeShard, no Filter: the zero-hop base case (no `WHERE`),
        // which already co-partitioned before the walk reached through Filters.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 1),
            Some(7),
            "a bare scan feeding the shard resolves on the first hop (no-`WHERE` case)"
        );

        // ScanDelta(7) → Filter → ExchangeShard.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            Some(7),
            "one Filter between scan and shard is transparent to the shard key"
        );

        // ScanDelta(8) → Filter → Filter → ExchangeShard.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(8));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(2, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            Some(8),
            "a chain of Filters is transparent to the shard key"
        );
    }

    /// A re-keying `Map` rewrites the PK region, so the walk must bail there — even
    /// with a Filter below it (the DISTINCT / set-op `HashRow` reindex shape).
    #[test]
    fn test_scan_tid_through_filters_stops_at_map() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(7) → Map(reindex) → ExchangeShard.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            None,
            "a reindex Map re-keys the PK; the walk must bail rather than cross it"
        );

        // ScanDelta(7) → Map(reindex) → Filter → ExchangeShard: a Filter below the
        // Map does not rescue it — the walk still reaches the Map and bails.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                keep: vec![0],
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            None,
            "a Filter below a reindex Map does not make the Map transparent"
        );
    }

    /// A `WorkerFilter` (range-join broadcast input) is a distinct OpNode variant,
    /// not a `Filter`, so it is never crossed — the same exclusion
    /// `scatter_key_of_scan` makes on the forward walk.
    #[test]
    fn test_scan_tid_through_filters_worker_filter() {
        use gnitz_wire::OpNode;
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, OpNode::WorkerFilter);
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            None,
            "WorkerFilter is not a Filter; the walk must bail"
        );
    }

    /// A fan-in (≠ 1 incoming edge) is not a linear chain — bail. Tested at the shard
    /// itself (two scans feeding it) and one hop in (two scans feeding a Filter).
    #[test]
    fn test_scan_tid_through_filters_fan_in() {
        use gnitz_wire::OpNode;
        let dummy_blob = dummy_expr_blob();
        // A Union of two scans feeds the ExchangeShard directly.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, scan_delta(8));
        nodes.insert(2, OpNode::Union);
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            None,
            "a Union draws from two sources, so no one distribution prefix governs the shard key"
        );

        // The same fan-in one hop further in: the walk must bail at the Union, not
        // follow the Filter it passed through into an arbitrary branch.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, scan_delta(8));
        nodes.insert(2, OpNode::Union);
        nodes.insert(3, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(4, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(
            nodes,
            vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B), (2, 3, PORT_IN), (3, 4, PORT_IN)],
        );
        assert_eq!(
            scan_tid_through_filters(&loaded, 4),
            None,
            "a Filter is transparent to the shard key, so the walk reaches the Union and bails there"
        );
    }

    // ── Finding 2: load_circuit must return None for null system-table pointers ──

    /// Null system-table pointers are a programming error; the engine always supplies
    /// valid handles. `load_circuit` must fail so callers get an explicit error
    /// rather than silently reading an incomplete circuit and producing wrong results.
    #[test]
    fn test_load_circuit_fails_for_null_system_tables() {
        let result = load_circuit(SysTableRefs::null(), 0);
        assert!(
            matches!(result, Err(CompileError::Rejected("circuit load failed"))),
            "null system-table pointers must fail the load, not yield a silently empty circuit"
        );
    }
}
