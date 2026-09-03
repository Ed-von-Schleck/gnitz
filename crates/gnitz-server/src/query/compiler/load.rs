//! Circuit loading: read the system tables into a `LoadedCircuit`, topo-sort it,
//! and the scan/reindex/range-key circuit queries the DAG consults at runtime.

use super::*;
use gnitz_store::storage::{payload_bytes, payload_is_null, payload_u64};
use gnitz_wire::{
    CIRCEDGES_PAY_DST_NODE, CIRCEDGES_PAY_DST_PORT, CIRCEDGES_PAY_SRC_NODE, CIRCNCOL_PAY_KIND, CIRCNCOL_PAY_NODE_ID,
    CIRCNCOL_PAY_POSITION, CIRCNCOL_PAY_VALUE1, CIRCNCOL_PAY_VALUE2, CIRCNODES_PAY_EXPR_PROGRAM, CIRCNODES_PAY_NODE_ID,
    CIRCNODES_PAY_OPCODE, CIRCNODES_PAY_SOURCE_TABLE,
};

// ---------------------------------------------------------------------------
// System table reading
// ---------------------------------------------------------------------------

/// Visit every `(view_id, source_table)` scan edge in the CircuitNodes store —
/// one call per `ScanDelta` node carrying a source, the same rows `load_circuit`
/// turns into `OpNode::ScanDelta`. Repeats are not filtered; the caller dedups.
pub(in crate::query) fn for_each_scan_edge(host: &dyn SchemaSource, mut f: impl FnMut(i64, i64)) {
    let Some(mut cur) = host.open_sys_cursor(gnitz_wire::CIRCUIT_NODES_TAB as i64) else {
        return;
    };
    // No prefix — every view's nodes, in view_id order.
    cur.for_each_positive_with_prefix(&[], |ch| {
        let (src, row) = ch.current_row_source();
        if payload_u64(src, row, CIRCNODES_PAY_OPCODE) != gnitz_wire::OPCODE_SCAN_DELTA
            || payload_is_null(src, row, CIRCNODES_PAY_SOURCE_TABLE)
        {
            return;
        }
        let source = payload_u64(src, row, CIRCNODES_PAY_SOURCE_TABLE) as i64;
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
/// into a `LoadedCircuit`: the per-node column gather, the `decode_op_node`
/// calls and the node-id `i32` reject. The edge set is then `topo_sorted`'s to
/// validate.
pub(super) fn load_circuit(host: &dyn SchemaSource, view_id: u64) -> Result<LoadedCircuit, CompileError> {
    let unopened = || CompileError::Rejected("circuit system tables are not open");
    let open = |tid: u64| host.open_sys_cursor(tid as i64).ok_or_else(unopened);
    let mut nodes_cur = open(gnitz_wire::CIRCUIT_NODES_TAB)?;
    let mut edges_cur = open(gnitz_wire::CIRCUIT_EDGES_TAB)?;
    let mut node_cols_cur = open(gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB)?;
    let mut nodes: HashMap<i32, gnitz_wire::OpNode> = HashMap::new();
    let mut edges: Vec<(i32, i32, i32)> = Vec::new();

    // The circuit tables share a compound `(view_id, sub)` PK; the OPK image of
    // the leading unsigned view_id column is its big-endian bytes.
    let prefix = view_id.to_be_bytes();
    // The first malformed row's reason, naming which check fired. Every shape
    // here aborts the WHOLE load — silently skipping a row leaves edges dangling,
    // yielding an invalid topological order or silent output corruption.
    let mut invalid: Option<&'static str> = None;

    // Phase 1: read CircuitNodeColumns, gathered per node. `decode_op_node`
    // orders them by `position` within a kind.
    let mut cols_by_node: HashMap<i32, Vec<gnitz_wire::CircuitNodeColumn>> = HashMap::new();
    node_cols_cur.for_each_positive_with_prefix(&prefix, |ch| {
        let (src, row) = ch.current_row_source();
        match node_id_i32(payload_u64(src, row, CIRCNCOL_PAY_NODE_ID) as i64) {
            Some(nid) => cols_by_node
                .entry(nid)
                .or_default()
                .push(gnitz_wire::CircuitNodeColumn {
                    kind: payload_u64(src, row, CIRCNCOL_PAY_KIND),
                    position: payload_u64(src, row, CIRCNCOL_PAY_POSITION) as u16,
                    value1: payload_u64(src, row, CIRCNCOL_PAY_VALUE1),
                    value2: payload_u64(src, row, CIRCNCOL_PAY_VALUE2),
                }),
            None => drop(invalid.get_or_insert("circuit node id out of range")),
        }
    });
    // Phase 2: read CircuitNodes; call decode_op_node for each.
    nodes_cur.for_each_positive_with_prefix(&prefix, |ch| {
        let (src, row) = ch.current_row_source();
        let Some(node_id) = node_id_i32(payload_u64(src, row, CIRCNODES_PAY_NODE_ID) as i64) else {
            invalid.get_or_insert("circuit node id out of range");
            return;
        };
        let opcode = payload_u64(src, row, CIRCNODES_PAY_OPCODE);

        let src_tab: Option<u64> = (!payload_is_null(src, row, CIRCNODES_PAY_SOURCE_TABLE))
            .then(|| payload_u64(src, row, CIRCNODES_PAY_SOURCE_TABLE));
        // `None` is a NULL cell only. An empty cell is a damaged blob, and each
        // opcode already judges one: Filter rejects, a ScanDelta bound degrades.
        let expr_blob: Option<Vec<u8>> = (!payload_is_null(src, row, CIRCNODES_PAY_EXPR_PROGRAM))
            .then(|| payload_bytes(src, row, CIRCNODES_PAY_EXPR_PROGRAM).to_vec());

        let cols = cols_by_node.get(&node_id).map(|v| v.as_slice()).unwrap_or(&[]);
        match gnitz_wire::decode_op_node(opcode, src_tab, expr_blob, cols) {
            Ok(op) => {
                nodes.insert(node_id, op);
            }
            Err(_) => drop(invalid.get_or_insert("circuit node failed to decode")),
        }
    });

    // Phase 3: read CircuitEdges. A truncating endpoint id aborts the load.
    edges_cur.for_each_positive_with_prefix(&prefix, |ch| {
        let (row_src, row) = ch.current_row_source();
        match (
            node_id_i32(payload_u64(row_src, row, CIRCEDGES_PAY_SRC_NODE) as i64),
            node_id_i32(payload_u64(row_src, row, CIRCEDGES_PAY_DST_NODE) as i64),
        ) {
            (Some(src), Some(dst)) => edges.push((src, dst, payload_u64(row_src, row, CIRCEDGES_PAY_DST_PORT) as i32)),
            _ => drop(invalid.get_or_insert("circuit edge endpoint id out of range")),
        }
    });
    if let Some(reason) = invalid {
        return Err(CompileError::Rejected(reason));
    }

    topo_sorted(nodes, edges)
}

// ---------------------------------------------------------------------------
// Topological sort (Kahn's algorithm)
// ---------------------------------------------------------------------------

/// Assemble the sorted circuit — the only form a `LoadedCircuit` exists in, so
/// no caller can read `ordered`, the adjacency maps or the elision set before
/// they are populated.
pub(super) fn topo_sorted(
    nodes: HashMap<i32, gnitz_wire::OpNode>,
    edges: Vec<(i32, i32, i32)>,
) -> Result<LoadedCircuit, CompileError> {
    let malformed = || CompileError::Rejected("node's input edges do not match its operator's ports");
    let mut outgoing: HashMap<i32, Vec<(i32, i32)>> = HashMap::new();
    // Each node's producer per port, filled straight from the edge list. Ports
    // are 0 and 1 (`PORT_IN == PORT_IN_A`, `PORT_TRACE == PORT_IN_B`), so a port
    // beyond the set and a duplicate one are the same rejection.
    let mut by_port: HashMap<i32, [Option<i32>; 2]> = HashMap::new();
    for &nid in nodes.keys() {
        outgoing.entry(nid).or_default();
        by_port.entry(nid).or_default();
    }

    for &(src, dst, port) in &edges {
        // A dangling endpoint — a node that failed to decode, a partial schema
        // flush, a hand-built fixture — would strand its partner rather than fail,
        // so it is rejected before it can become a phantom adjacency entry.
        let (Some(outs), Some(ports)) = (outgoing.get_mut(&src), by_port.get_mut(&dst)) else {
            return Err(CompileError::Rejected("edge endpoint is not a node of the circuit"));
        };
        match ports.get_mut(port as usize) {
            Some(slot @ None) => *slot = Some(src),
            _ => return Err(malformed()),
        }
        outs.push((dst, port));
    }

    // Hold every node's filled port slots to the set its operator declares, and
    // keep them as [`NodeInputs`]. Rejecting a duplicate port, a missing one and
    // an out-of-range one in one place is what lets the emit layer destructure an
    // operand instead of asking whether it exists. The durable form already
    // enforces uniqueness — the `CircuitEdges` PK is `(view_id, dst_node,
    // dst_port)` — but a hand-built circuit does not, so the check lives here, in
    // the only constructor.
    let inputs: HashMap<i32, NodeInputs> = nodes
        .iter()
        .map(|(&nid, op)| {
            // Matching the operator's arity against the filled slots IS the
            // port-set equality: a missing port leaves its slot empty.
            let resolved = match (op.ports(), by_port[&nid]) {
                ([], [None, None]) => NodeInputs::Source,
                ([_], [Some(src), None]) => NodeInputs::Unary(src),
                ([_, _], [Some(a), Some(b)]) => NodeInputs::Binary { a, b },
                _ => return Err(malformed()),
            };
            Ok((nid, resolved))
        })
        .collect::<Result<_, _>>()?;

    let mut in_degree: HashMap<i32, i32> = inputs.iter().map(|(&nid, i)| (nid, i.iter().count() as i32)).collect();

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
        skip_nodes: compute_skip_nodes(&nodes, &ordered, &inputs),
        nodes,
        ordered,
        outgoing,
        inputs,
    })
}

/// Distinct nodes elided because their input is already distinct. One forward
/// pass along the topological order, maintaining the set of nodes whose output
/// is known distinct: a Reduce or Distinct establishes it; a Filter preserves
/// it; a Map preserves it unless it re-keys the PK (an equijoin pre-index
/// reindex or a full-row HashRow), which invalidates upstream distinctness.
fn compute_skip_nodes(
    nodes: &HashMap<i32, gnitz_wire::OpNode>,
    ordered: &[i32],
    inputs: &HashMap<i32, NodeInputs>,
) -> HashSet<i32> {
    let mut distinct_at: HashSet<i32> = HashSet::new();
    let mut skip = HashSet::new();
    for &nid in ordered {
        // Every arm below is a unary operator, so its one input is where the
        // property it preserves or establishes comes from.
        let input_distinct = |nid: i32| distinct_at.contains(&inputs[&nid].unary());
        match &nodes[&nid] {
            gnitz_wire::OpNode::Reduce { .. } => {
                distinct_at.insert(nid);
            }
            gnitz_wire::OpNode::Distinct => {
                if input_distinct(nid) {
                    skip.insert(nid);
                }
                distinct_at.insert(nid);
            }
            gnitz_wire::OpNode::Filter(_) => {
                if input_distinct(nid) {
                    distinct_at.insert(nid);
                }
            }
            gnitz_wire::OpNode::Map(mk) => {
                let re_keys = matches!(
                    mk,
                    gnitz_wire::MapKind::Reindex { .. } | gnitz_wire::MapKind::HashRow(..)
                );
                if !re_keys && input_distinct(nid) {
                    distinct_at.insert(nid);
                }
            }
            _ => {}
        }
    }
    skip
}

// ---------------------------------------------------------------------------
// Circuit queries
// ---------------------------------------------------------------------------

/// The scatter key of the source scanned at `scan_nid`: one sequence per
/// `ScatterKey` reindex `Map` reachable forward through `Filter`s.
///
/// Which reindex is a source's join/group key is the planner's to state
/// ([`gnitz_wire::ReindexRole`]); the engine could only guess it from graph
/// shape. The second return says the walk found only `Auxiliary` ones — a
/// planner call site that forgot its role, which `ViewMeta::derive` turns
/// into a failed compile rather than silently-unscattered rows.
pub(super) fn scatter_key_of_scan(loaded: &LoadedCircuit, scan_nid: i32) -> (Vec<Vec<(u32, u8)>>, bool) {
    let mut queue = VecDeque::from([scan_nid]);
    // `visited` bounds the walk to O(nodes): without it a Filter diamond (two
    // edge paths reaching the same Filter) would re-push and re-expand nodes.
    let mut visited = HashSet::new();
    // One entry per distinct key sequence — an identical one reached again (the
    // null/not-null sibling Maps of a nullable LEFT-join key) is added once.
    // Duplicate columns WITHIN a sequence are preserved, so the result mirrors the
    // trace-side `ReindexPacker` slot-for-slot.
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
            match loaded.op(dst) {
                gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
                    reindex_cols,
                    reindex_target_tcs,
                    role,
                    ..
                }) => {
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
                gnitz_wire::OpNode::Filter(_) => queue.push_back(dst),
                _ => {}
            }
        }
    }
    let orphaned = seqs.is_empty() && saw_auxiliary;
    (seqs, orphaned)
}

/// Walk back from an `ExchangeShard` (`enid`) through `Filter` nodes to the
/// source `ScanDelta`, returning its table id — or `None` on a fan-in or any
/// other node. `Filter` is the only operator transparent to the shard key: it is
/// row-selective, never re-keys the PK region and never moves a row off-worker.
/// Terminates without a visited guard: `topo_sorted` rejects a cycle, and each
/// hop takes the one incoming edge.
pub(super) fn scan_tid_through_filters(loaded: &LoadedCircuit, enid: i32) -> Option<i64> {
    let mut cur = enid;
    loop {
        // Bail on a fan-in: a multi-input node (Union, set op) draws from more
        // than one source, so no single table's distribution prefix governs the
        // shard key and it can never co-partition.
        let NodeInputs::Unary(src_nid) = *loaded.inputs(cur) else {
            return None;
        };
        match loaded.op(src_nid) {
            gnitz_wire::OpNode::ScanDelta { source: t, .. } => return Some(*t as i64),
            gnitz_wire::OpNode::Filter(_) => cur = src_nid,
            _ => return None,
        }
    }
}

/// The sink-nearest `ExchangeShard`'s `(node id, shard columns)`, or `None` when
/// the circuit carries none. A two-sided set-op emits one shard per side, and
/// every caller wants the one whose key the output carries.
pub(super) fn output_exchange_shard(loaded: &LoadedCircuit) -> Option<(i32, Vec<u32>)> {
    loaded.ops().rev().find_map(|(nid, op)| match op {
        gnitz_wire::OpNode::ExchangeShard { shard_cols } => Some((nid, shard_cols.clone())),
        _ => None,
    })
}

/// How a view's join, if it has one, needs its inputs placed — what the master
/// relay routes a source's delta by, and whether any source can skip the
/// scatter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum JoinRelay {
    /// Route by the whole reindex key: an equi join, whose matches share a key,
    /// and every circuit without a join (a GROUP BY routes by its group key the
    /// same way).
    WholeKey,
    /// A band join: route by the `n_eq` leading equality slots alone, dropping
    /// the range slot, so equal eq-values co-partition and the range probe
    /// stays partition-local.
    EqPrefix { n_eq: u8 },
    /// A pure range join or a cross join: the matches spread over the whole
    /// other side, so every worker needs the full delta and a `WorkerFilter`
    /// trims what it integrates.
    Broadcast,
}

/// The join relay a circuit's `Join` node calls for, read off the first one —
/// both bilinear terms carry the same kind, so which one the walk reaches is
/// not observable.
pub(super) fn circuit_join_relay(loaded: &LoadedCircuit) -> JoinRelay {
    use gnitz_wire::{JoinKind, OpNode};
    loaded
        .ops()
        .find_map(|(_, op)| match op {
            OpNode::Join(kind) => Some(match kind {
                JoinKind::DeltaTrace => JoinRelay::WholeKey,
                JoinKind::DeltaTraceRange { n_eq: 0, .. } | JoinKind::DeltaTraceCross => JoinRelay::Broadcast,
                JoinKind::DeltaTraceRange { n_eq, .. } => JoinRelay::EqPrefix { n_eq: *n_eq },
            }),
            _ => None,
        })
        .unwrap_or(JoinRelay::WholeKey)
}

/// The `(source table id, bound)` the planner pushed onto a `ScanDelta`'s
/// backfill scan. At most one exists by construction — only the primary source
/// can carry a bound. A hint: `None` means "full-scan", which is always correct,
/// so a hand-crafted circuit with several takes the topologically first and lets
/// the rest degrade, the same way on every worker.
pub(super) fn circuit_source_bound(loaded: &LoadedCircuit) -> Option<(i64, gnitz_wire::ScanBound)> {
    loaded.ops().find_map(|(_, op)| match op {
        gnitz_wire::OpNode::ScanDelta { source, bound: Some(b) } => Some((*source as i64, *b)),
        _ => None,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/load.rs"]
mod tests;
