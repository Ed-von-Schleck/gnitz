//! Circuit loading: decode one view's `CIRCUIT_TAB` cell into a `LoadedCircuit`.

use super::*;
use gnitz_expr::payload_bytes;

/// Decode `view_id`'s circuit cell.
pub(in crate::query) fn load_circuit(registry: &RelationRegistry, view_id: u64) -> Result<LoadedCircuit, String> {
    let (_, row) = registry
        .relation(gnitz_wire::CIRCUIT_TAB)
        .expect("the catalog registers every system family at open")
        .live_row_at(&view_id.to_be_bytes());
    let row = row.ok_or_else(|| format!("view {view_id} has no circuit"))?;
    let (src, ri) = row.source();
    gnitz_wire::Circuit::decode(payload_bytes(src, ri, gnitz_wire::CIRCTAB_PAY_CIRCUIT)).map(LoadedCircuit)
}

#[cfg(test)]
#[path = "tests/load.rs"]
mod tests;
