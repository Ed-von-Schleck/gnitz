use super::*;
use crate::catalog::SysFamily;
use crate::query::compiler::fixtures::*;
use crate::test_support::circuit_nodes_batch;
use gnitz_store::relation::{RelationKind, RelationSpec, StoreConfig};
use gnitz_wire::sys_rows::{write_circuit_node_row, CircuitNodeRow};
use gnitz_wire::{Circuit, KeyRange, PkColList, ReadBound};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::Slot;

const VIEW_ID: u64 = 1;

/// `load_circuit(VIEW_ID)` off a `CircuitNodes` system table holding `rows`.
fn load(rows: Batch) -> Result<LoadedCircuit, String> {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::new(tmp.path().to_str().unwrap(), Slot::SOLO, StoreConfig::default());
    registry
        .register(RelationSpec {
            id: gnitz_wire::CIRCUIT_NODES_TAB,
            kind: RelationKind::SystemCatalog,
            schema: *SysFamily::CircuitNodes.schema(),
            placement: gnitz_zset::schema::Placement::Replicated,
        })
        .unwrap();
    registry.ingest(gnitz_wire::CIRCUIT_NODES_TAB, rows).unwrap();
    load_circuit(&registry, VIEW_ID)
}

/// One input-less row carrying `opcode`, which no `OpNode` encodes to.
fn undecodable_row(bb: &mut BatchBuilder, view_id: u64, node_id: u64, opcode: u64) {
    let row = CircuitNodeRow {
        view_id,
        node_id,
        opcode,
        source_table: None,
        inputs: [None, None],
        params: None,
    };
    write_circuit_node_row(bb, &row, 1);
}

/// The load reports the first row it refuses. Reading on would refuse the next
/// row too, for the gap the first left, and report that instead.
#[test]
fn the_first_refused_row_aborts_the_load_and_is_the_one_reported() {
    let mut bb = BatchBuilder::new(*SysFamily::CircuitNodes.schema());
    undecodable_row(&mut bb, VIEW_ID, 0, 9999);
    undecodable_row(&mut bb, VIEW_ID, 1, 8888);
    assert_eq!(rejection(load(bb.finish())), "circuit params: unknown opcode 9999");
}

/// The load returns the circuit the client wrote, cell for cell — a scan's bound,
/// a second operand, and a `Filter` program that does not decode, which is the
/// compile's to refuse — and none of another view's rows.
#[test]
fn the_load_returns_one_views_circuit_as_written() {
    let mut c = Circuit::default();
    let bound = ReadBound::Range(KeyRange::point(PkColList::from_slice(&[0]), &[], 7));
    let bounded = c.input_delta(10, bound);
    let filtered = c.filter(bounded, vec![0xff]);
    let other = scan(&mut c, 11);
    let both = c.union(filtered, other);
    c.sink(both);

    let mut rows = circuit_nodes_batch(VIEW_ID, &c);
    // Undecodable, so a load that ignored the view prefix would fail outright.
    let mut foreign = BatchBuilder::new(*SysFamily::CircuitNodes.schema());
    undecodable_row(&mut foreign, VIEW_ID + 1, 0, 9999);
    rows.append_batch(&foreign.finish());

    assert_eq!(load(rows).expect("one view's rows").0, c);
}
