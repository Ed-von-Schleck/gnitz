//! Circuit loading: read one view's `CircuitNodes` rows into a `LoadedCircuit`.

use super::*;
use gnitz_store::storage::{payload_bytes, payload_is_null, payload_u64};
use gnitz_wire::{
    CIRCNODES_PAY_INPUT_0, CIRCNODES_PAY_INPUT_1, CIRCNODES_PAY_OPCODE, CIRCNODES_PAY_PARAMS,
    CIRCNODES_PAY_SOURCE_TABLE,
};

// ---------------------------------------------------------------------------
// System table reading
// ---------------------------------------------------------------------------

/// The `CircuitNodes` system table, if the registry holds it.
fn circuit_nodes(registry: &RelationRegistry) -> Option<&Relation> {
    registry.relation(gnitz_wire::CIRCUIT_NODES_TAB as i64)
}

/// Visit every `(view_id, source_table)` scan edge in the CircuitNodes store —
/// one call per `ScanDelta` node carrying a source, the same rows `load_circuit`
/// turns into `OpNode::ScanDelta`. Repeats are not filtered; the caller dedups.
pub(in crate::query) fn for_each_scan_edge(registry: &RelationRegistry, mut f: impl FnMut(i64, i64)) {
    let Some(mut cur) = circuit_nodes(registry).map(Relation::cursor) else {
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

/// Read the `CircuitNodes` rows of one view (filtered by the `view_id` OPK
/// prefix) into a `LoadedCircuit`, pushing each row in `node_id` order through
/// `Circuit::push` — so a sparse id, an input naming a later node and an arity
/// mismatch each abort the load.
pub(super) fn load_circuit(registry: &RelationRegistry, view_id: u64) -> Result<LoadedCircuit, String> {
    let nodes = circuit_nodes(registry).ok_or("the circuit system table is not open")?;
    let mut circuit = gnitz_wire::Circuit::default();

    // The circuit table has a compound `(view_id, node_id)` PK; the OPK image of
    // the leading unsigned view_id column is its big-endian bytes, and rows arrive
    // in `node_id` order behind it.
    let prefix = view_id.to_be_bytes();
    // The first malformed row's reason, naming which check fired. Every shape
    // here aborts the WHOLE load.
    let mut invalid: Option<String> = None;

    nodes.for_each_positive_with_prefix(&prefix, |ch| {
        if invalid.is_some() {
            return;
        }
        let (src, row) = ch.current_row_source();
        let node_id = gnitz_wire::unpack_pair_pk(ch.current_key_narrow()).1;
        if node_id != circuit.nodes().len() as u64 {
            invalid = Some("circuit node ids are not dense from 0".to_string());
            return;
        }
        let nullable_u64 = |pay: usize| (!payload_is_null(src, row, pay)).then(|| payload_u64(src, row, pay));
        // `None` is a NULL cell only; a present but empty cell is a damaged blob,
        // which `decode_op_node` rejects.
        let params: Option<&[u8]> =
            (!payload_is_null(src, row, CIRCNODES_PAY_PARAMS)).then(|| payload_bytes(src, row, CIRCNODES_PAY_PARAMS));
        // `decode_op_node` names which trust-boundary check fired and interpolates
        // the offending value; that string is what `preflight_compile` renders
        // into the `CREATE VIEW` error.
        let pushed = gnitz_wire::decode_op_node(
            payload_u64(src, row, CIRCNODES_PAY_OPCODE),
            nullable_u64(CIRCNODES_PAY_SOURCE_TABLE),
            params,
        )
        .and_then(|op| {
            let inputs =
                NodeInputs::from_slots([nullable_u64(CIRCNODES_PAY_INPUT_0), nullable_u64(CIRCNODES_PAY_INPUT_1)])?;
            circuit.push(op, inputs)
        });
        if let Err(why) = pushed {
            invalid = Some(why);
        }
    });
    if let Some(reason) = invalid {
        return Err(reason);
    }
    LoadedCircuit::new(circuit)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/load.rs"]
mod tests;
