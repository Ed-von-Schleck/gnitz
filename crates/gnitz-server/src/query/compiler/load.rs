//! Circuit loading: read one view's `CircuitNodes` rows into a topologically
//! sorted `LoadedCircuit`.

use super::*;
use gnitz_store::storage::{payload_bytes, payload_is_null, payload_u64, ReadCursor};
use gnitz_wire::{
    CIRCNODES_PAY_INPUT_0, CIRCNODES_PAY_INPUT_1, CIRCNODES_PAY_OPCODE, CIRCNODES_PAY_PARAMS,
    CIRCNODES_PAY_SOURCE_TABLE,
};

// ---------------------------------------------------------------------------
// System table reading
// ---------------------------------------------------------------------------

/// A cursor over the `CircuitNodes` system table, if the registry holds it.
fn circuit_nodes_cursor(registry: &RelationRegistry) -> Option<ReadCursor> {
    registry
        .relation(gnitz_wire::CIRCUIT_NODES_TAB as i64)
        .map(Relation::cursor)
}

/// Visit every `(view_id, source_table)` scan edge in the CircuitNodes store —
/// one call per `ScanDelta` node carrying a source, the same rows `load_circuit`
/// turns into `OpNode::ScanDelta`. Repeats are not filtered; the caller dedups.
pub(in crate::query) fn for_each_scan_edge(registry: &RelationRegistry, mut f: impl FnMut(i64, i64)) {
    let Some(mut cur) = circuit_nodes_cursor(registry) else {
        return;
    };
    // Every view's nodes, in view_id order.
    cur.for_each_positive(|ch| {
        let (src, row) = ch.current_row_source();
        if payload_u64(src, row, CIRCNODES_PAY_OPCODE) != gnitz_wire::Opcode::ScanDelta.as_wire()
            || payload_is_null(src, row, CIRCNODES_PAY_SOURCE_TABLE)
        {
            return;
        }
        let source = payload_u64(src, row, CIRCNODES_PAY_SOURCE_TABLE) as i64;
        if source <= 0 {
            return;
        }
        let view_id = gnitz_wire::unpack_pair_pk(ch.current_key_narrow()).0 as i64;
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

/// Read the `CircuitNodes` rows of one view (filtered by the `view_id` OPK
/// prefix) into a `LoadedCircuit`: the `decode_op_node` calls, the node-id `i32`
/// reject, and the per-node input slots. Holding those slots to each operator's
/// arity is then `topo_sorted`'s.
pub(super) fn load_circuit(registry: &RelationRegistry, view_id: u64) -> Result<LoadedCircuit, String> {
    let mut nodes_cur = circuit_nodes_cursor(registry).ok_or("the circuit system table is not open")?;
    let mut nodes: FxHashMap<i32, gnitz_wire::OpNode> = FxHashMap::default();
    let mut inputs: FxHashMap<i32, [Option<i32>; 2]> = FxHashMap::default();

    // The circuit table has a compound `(view_id, node_id)` PK; the OPK image of
    // the leading unsigned view_id column is its big-endian bytes.
    let prefix = view_id.to_be_bytes();
    // The first malformed row's reason, naming which check fired. Every shape
    // here aborts the WHOLE load — silently skipping a row leaves an input
    // dangling, yielding an invalid topological order or silent output corruption.
    let mut invalid: Option<String> = None;

    nodes_cur.for_each_positive_with_prefix(&prefix, |ch| {
        let (src, row) = ch.current_row_source();
        let node_id_raw = gnitz_wire::unpack_pair_pk(ch.current_key_narrow()).1 as i64;
        let Some(node_id) = node_id_i32(node_id_raw) else {
            invalid.get_or_insert_with(|| "circuit node id out of range".to_string());
            return;
        };
        let opcode = payload_u64(src, row, CIRCNODES_PAY_OPCODE);

        let nullable_u64 = |pay: usize| (!payload_is_null(src, row, pay)).then(|| payload_u64(src, row, pay));
        let src_tab = nullable_u64(CIRCNODES_PAY_SOURCE_TABLE);
        // `None` is a NULL cell only; a present but empty cell is a damaged blob,
        // which `decode_op_node` rejects.
        let params: Option<&[u8]> =
            (!payload_is_null(src, row, CIRCNODES_PAY_PARAMS)).then(|| payload_bytes(src, row, CIRCNODES_PAY_PARAMS));

        let mut slots = [None; 2];
        for (slot, pay) in slots.iter_mut().zip([CIRCNODES_PAY_INPUT_0, CIRCNODES_PAY_INPUT_1]) {
            let Some(raw) = nullable_u64(pay) else { continue };
            match node_id_i32(raw as i64) {
                Some(producer) => *slot = Some(producer),
                None => drop(invalid.get_or_insert_with(|| "circuit node id out of range".to_string())),
            }
        }

        match gnitz_wire::decode_op_node(opcode, src_tab, params) {
            Ok(op) => {
                nodes.insert(node_id, op);
                inputs.insert(node_id, slots);
            }
            // `decode_op_node` names which trust-boundary check fired and
            // interpolates the offending value; that string is what
            // `preflight_compile` renders into the `CREATE VIEW` error.
            Err(why) => drop(invalid.get_or_insert(why)),
        }
    });
    if let Some(reason) = invalid {
        return Err(reason);
    }

    topo_sorted(nodes, inputs)
}

// ---------------------------------------------------------------------------
// Topological sort (Kahn's algorithm)
// ---------------------------------------------------------------------------

/// Assemble the sorted circuit — the only form a `LoadedCircuit` exists in, so
/// no caller can read `ordered` or the adjacency maps before they are
/// populated.
pub(super) fn topo_sorted(
    nodes: FxHashMap<i32, gnitz_wire::OpNode>,
    by_slot: FxHashMap<i32, [Option<i32>; 2]>,
) -> Result<LoadedCircuit, String> {
    // The only constructor every plan passes through, so the one place the cap
    // has to hold.
    if nodes.len() > MAX_CIRCUIT_NODES {
        return Err("circuit exceeds the node limit".into());
    }
    let malformed = || "node's inputs do not match its operator's arity".to_string();
    let mut outgoing: FxHashMap<i32, Vec<i32>> = FxHashMap::default();
    for &nid in nodes.keys() {
        outgoing.entry(nid).or_default();
    }

    // Rejecting a missing, surplus or dangling operand here, in the only
    // constructor, is what lets the emit layer destructure an operand instead of
    // asking whether it exists. A duplicate or out-of-range port needs no reject:
    // the slot index *is* the port.
    let mut inputs: FxHashMap<i32, NodeInputs> = FxHashMap::default();
    for (&nid, op) in &nodes {
        let slots = by_slot.get(&nid).copied().unwrap_or([None; 2]);
        for producer in slots.into_iter().flatten() {
            let Some(outs) = outgoing.get_mut(&producer) else {
                return Err("a node's input is not a node of the circuit".into());
            };
            outs.push(nid);
        }
        // Matching the operator's arity against the filled slots IS the port-set
        // equality: a missing port leaves its slot empty.
        let resolved = match (op.arity(), slots) {
            (0, [None, None]) => NodeInputs::Source,
            (1, [Some(src), None]) => NodeInputs::Unary(src),
            (2, [Some(a), Some(b)]) => NodeInputs::Binary { a, b },
            _ => return Err(malformed()),
        };
        inputs.insert(nid, resolved);
    }
    // `nodes` is a HashMap, so the walk above fills each consumer list in the
    // hasher's order, which differs between the master and each worker. Forward
    // walks pick the first match they like, so the order must not.
    for outs in outgoing.values_mut() {
        outs.sort_unstable();
    }

    let mut in_degree: FxHashMap<i32, i32> = inputs.iter().map(|(&nid, i)| (nid, i.iter().count() as i32)).collect();

    let mut init: Vec<i32> = nodes.keys().filter(|nid| in_degree[nid] == 0).copied().collect();
    init.sort_unstable(); // deterministic order for tied sources
    let mut queue: VecDeque<i32> = init.into();

    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some(nid) = queue.pop_front() {
        ordered.push(nid);
        if let Some(outs) = outgoing.get(&nid) {
            let mut next_batch: Vec<i32> = Vec::new();
            for &dst in outs {
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
        return Err("circuit graph has a cycle".into());
    }
    Ok(LoadedCircuit { nodes, ordered, outgoing, inputs })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/load.rs"]
mod tests;
