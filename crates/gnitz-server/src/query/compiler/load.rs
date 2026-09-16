//! Circuit loading: read one view's `CircuitNodes` rows into a `LoadedCircuit`.

use super::*;
use gnitz_expr::RowSource;
use gnitz_store::storage::{payload_bytes, payload_is_null, payload_u64};
use gnitz_wire::sys_rows::CircuitNodeRow;
use gnitz_wire::{
    CIRCNODES_PAY_INPUT_0, CIRCNODES_PAY_INPUT_1, CIRCNODES_PAY_OPCODE, CIRCNODES_PAY_PARAMS,
    CIRCNODES_PAY_SOURCE_TABLE,
};

/// One `CircuitNodes` row off any row source — `write_circuit_node_row`'s inverse.
/// A NULL cell reads as `None`; a present but empty `params` cell stays present.
pub(in crate::query) fn read_circuit_node_row<S: RowSource>(src: &S, row: usize) -> CircuitNodeRow<'_> {
    let (view_id, node_id) = gnitz_wire::unpack_pair_pk(gnitz_wire::widen_pk_be(src.get_pk_bytes(row)));
    let present = |pi: usize| !payload_is_null(src, row, pi);
    let nullable_u64 = |pi: usize| present(pi).then(|| payload_u64(src, row, pi));
    CircuitNodeRow {
        view_id,
        node_id,
        opcode: payload_u64(src, row, CIRCNODES_PAY_OPCODE),
        source_table: nullable_u64(CIRCNODES_PAY_SOURCE_TABLE),
        inputs: [nullable_u64(CIRCNODES_PAY_INPUT_0), nullable_u64(CIRCNODES_PAY_INPUT_1)],
        params: present(CIRCNODES_PAY_PARAMS).then(|| payload_bytes(src, row, CIRCNODES_PAY_PARAMS)),
    }
}

/// Read the `CircuitNodes` rows under `view_id`, in `node_id` order, into a
/// `LoadedCircuit`. The first row `Circuit::push_row` refuses aborts the whole load.
pub(super) fn load_circuit(registry: &RelationRegistry, view_id: u64) -> Result<LoadedCircuit, String> {
    let mut circuit = gnitz_wire::Circuit::default();
    let mut pushed = Ok(0);
    registry
        .relation(gnitz_wire::CIRCUIT_NODES_TAB as i64)
        .expect("the catalog registers every system family at open")
        .for_each_positive_with_prefix(&view_id.to_be_bytes(), |c| {
            if pushed.is_ok() {
                let (src, row) = c.current_row_source();
                pushed = circuit.push_row(&read_circuit_node_row(src, row));
            }
        });
    pushed?;
    LoadedCircuit::new(circuit)
}

#[cfg(test)]
#[path = "tests/load.rs"]
mod tests;
