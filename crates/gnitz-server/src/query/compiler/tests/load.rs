use super::*;
use crate::catalog::SysFamily;
use crate::query::compiler::fixtures::*;
use crate::test_support::{circuit_batch, circuit_cell_batch};
use gnitz_store::relation::{RelationKind, RelationSpec, StoreConfig};
use gnitz_wire::{Circuit, KeyRange, PkColList, ReadBound};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::Slot;

const VIEW_ID: u64 = 1;

/// `load_circuit(VIEW_ID)` off a `CIRCUIT_TAB` system table holding `rows`.
fn load(rows: Batch) -> Result<LoadedCircuit, String> {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::new(tmp.path().to_str().unwrap(), Slot::SOLO, StoreConfig::default());
    registry
        .register(RelationSpec {
            id: gnitz_wire::CIRCUIT_TAB,
            kind: RelationKind::SystemCatalog,
            schema: *SysFamily::Circuit.schema(),
            placement: gnitz_zset::schema::Placement::Replicated,
            pk_repeats: false,
        })
        .unwrap();
    registry.ingest(gnitz_wire::CIRCUIT_TAB, rows).unwrap();
    load_circuit(&registry, VIEW_ID)
}

/// One node under an opcode no operator has.
const UNDECODABLE: &[u8] = &[1, 0, 200];

/// A view with no circuit row, and one whose cell does not decode, are refused
/// under the cell's own error.
#[test]
fn a_missing_or_undecodable_cell_is_refused() {
    let empty = BatchBuilder::new(SysFamily::Circuit.schema()).finish();
    assert_eq!(rejection(load(empty)), "view 1 has no circuit");
    for (cell, want) in [
        (UNDECODABLE, "circuit: unknown Opcode 200"),
        (&[][..], "circuit: truncated (need 2 bytes at offset 0, 0 remain)"),
    ] {
        assert_eq!(rejection(load(circuit_cell_batch(VIEW_ID, cell))), want);
    }
}

/// The load returns the circuit the client wrote — a scan's bound, a second
/// operand, and a `Filter` program that does not decode, which is the compile's
/// to refuse — and not another view's.
#[test]
fn the_load_returns_one_views_circuit_as_written() {
    let mut c = Circuit::default();
    let bound = ReadBound::Range(KeyRange::point(PkColList::from_slice(&[0]), &[], 7));
    let bounded = c.input_delta(10, bound);
    let filtered = c.filter(bounded, vec![0xff]);
    let other = scan(&mut c, 11);
    let both = c.union(filtered, other);
    c.sink(both);

    let mut rows = circuit_batch(VIEW_ID, &c);
    // Undecodable, so a load that ignored the view prefix would fail outright.
    rows.append_batch(&circuit_cell_batch(VIEW_ID + 1, UNDECODABLE));

    assert_eq!(load(rows).expect("one view's row").0, c);
}
