//! Circuit loading: decode one view's `CIRCUIT_TAB` cell into a `LoadedCircuit`.

use super::*;
use gnitz_expr::payload_bytes;

/// Decode `view_id`'s circuit cell.
pub(in crate::query) fn load_circuit(registry: &RelationRegistry, view_id: u64) -> Result<LoadedCircuit, String> {
    let mut circuit = Err(format!("view {view_id} has no circuit"));
    registry
        .relation(gnitz_wire::CIRCUIT_TAB)
        .expect("the catalog registers every system family at open")
        .for_each_positive_with_prefix(&view_id.to_be_bytes(), |c| {
            let (src, row) = c.current_row_source();
            circuit = gnitz_wire::Circuit::decode(payload_bytes(src, row, gnitz_wire::CIRCTAB_PAY_CIRCUIT));
        });
    circuit.map(LoadedCircuit)
}

#[cfg(test)]
#[path = "tests/load.rs"]
mod tests;
