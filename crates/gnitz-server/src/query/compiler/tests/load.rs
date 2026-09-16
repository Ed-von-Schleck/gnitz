use super::*;
use crate::query::compiler::fixtures::*;
use gnitz_store::relation::{OnRegister, RelationKind, RelationSpec, StoreConfig};
use gnitz_store::storage::Slot;
use gnitz_wire::OpNode;
use gnitz_wire::ViewProps;

// ── load_circuit against the real system tables ─────────────────────────

/// The `CircuitNodes` system table `load_circuit` reads, on one tempdir.
struct CircuitTables {
    _tmp: tempfile::TempDir,
    registry: RelationRegistry,
}

impl CircuitTables {
    const VIEW_ID: u64 = 1;

    fn schema() -> SchemaDescriptor {
        // The catalog family's own schema, not a rebuild of it: the loader reads
        // the real CIRCUIT_NODES table, so a fixture that derived its own layout
        // could drift from the one production writes.
        *crate::catalog::SysFamily::CircuitNodes.schema()
    }

    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let mut registry = RelationRegistry::new(Slot::SOLO, StoreConfig::default());
        // A `SystemCatalog` registration homes the store flat under its own
        // directory, the shape `bootstrap.rs` gives every family.
        registry
            .register(
                RelationSpec {
                    id: gnitz_wire::CIRCUIT_NODES_TAB as i64,
                    kind: RelationKind::SystemCatalog,
                    schema: Self::schema(),
                    directory: format!("{}/nodes", tmp.path().to_str().unwrap()),
                    props: ViewProps::default(),
                },
                OnRegister::Live,
            )
            .unwrap();
        Self { registry, _tmp: tmp }
    }

    fn put_nodes(&mut self, f: impl FnOnce(&mut gnitz_store::storage::BatchBuilder)) -> &mut Self {
        let mut bb = gnitz_store::storage::BatchBuilder::new(Self::schema());
        f(&mut bb);
        self.registry
            .ingest(gnitz_wire::CIRCUIT_NODES_TAB as i64, bb.finish())
            .unwrap();
        self
    }

    /// One `CircuitNodes` row under `view_id`, through the shared row codec — so
    /// a fixture cannot disagree with what the client lays down. `input` is the
    /// producer feeding slot 0.
    fn node_row(
        bb: &mut gnitz_store::storage::BatchBuilder,
        view_id: u64,
        node_id: u64,
        op: OpNode,
        input: Option<u64>,
    ) {
        let (opcode, source_table, params) = gnitz_wire::encode_op_node(&op);
        Self::raw_row(
            bb,
            view_id,
            node_id,
            opcode.as_wire(),
            source_table,
            input,
            params.as_deref(),
        );
    }

    /// [`Self::node_row`] with the row fields spelled out, for the shapes no
    /// `OpNode` encodes to (an unknown opcode, a damaged parameter cell).
    fn raw_row(
        bb: &mut gnitz_store::storage::BatchBuilder,
        view_id: u64,
        node_id: u64,
        opcode: u64,
        source_table: Option<u64>,
        input: Option<u64>,
        params: Option<&[u8]>,
    ) {
        gnitz_wire::sys_rows::write_circuit_node_row(
            bb,
            &gnitz_wire::sys_rows::CircuitNodeRow {
                view_id,
                node_id,
                opcode,
                source_table,
                inputs: [input, None],
                params,
            },
            1,
        );
    }

    /// Loaded straight off the registry the engine loads through, so the fixture
    /// exercises the same lookup production does.
    fn load(&mut self) -> Result<LoadedCircuit, String> {
        load_circuit(&self.registry, Self::VIEW_ID)
    }
}

/// Every malformed-row shape must abort the WHOLE load and say which check
/// fired: skipping a row would leave an input dangling.
#[test]
fn a_malformed_row_aborts_the_load_and_names_the_check() {
    // An opcode `decode_op_node` rejects. Its own reason is what surfaces —
    // interpolated value and all — not a bare "failed to decode".
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| CircuitTables::raw_row(bb, CircuitTables::VIEW_ID, 0, 9999, None, None, None));
    assert_eq!(
        rejection(c.load()),
        "unknown opcode 9999",
        "the decoder's own rejection must reach the caller",
    );

    // A node naming a producer that is not a node of the circuit: honouring it
    // would create a phantom node.
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| {
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 0, scan_delta(99), None);
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 1, OpNode::IntegrateSink, Some(7));
    });
    assert_eq!(rejection(c.load()), "a node's input is not an earlier node");

    // A gap in the ids: node 1 is missing, so node 2's index would not be its id.
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| {
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 0, scan_delta(99), None);
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 2, OpNode::IntegrateSink, Some(0));
    });
    assert_eq!(rejection(c.load()), "circuit node ids are not dense from 0");
}

/// The load is filtered to one view's rows by the `view_id` OPK prefix, and a
/// `Filter` whose program is present but undecodable stays present: rejecting
/// the program is the compile's job, not the load's. A NULL cell is how an
/// absent program is spelled and would turn the Filter into `WHERE TRUE`, so the
/// two must not collapse.
#[test]
fn the_load_takes_one_views_rows_and_keeps_a_damaged_program_present() {
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| {
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 0, scan_delta(10), None);
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 1, OpNode::Filter(vec![0xff]), Some(0));
        // A second view's nodes, which this load must not see. An undecodable
        // opcode, so a load that ignored the prefix would fail outright.
        CircuitTables::raw_row(bb, CircuitTables::VIEW_ID + 1, 2, 9999, None, None, None);
    });
    let loaded = c.load().expect("a damaged program is not a load failure");
    assert_eq!(loaded.len(), 2, "only this view's nodes are loaded");
    assert!(
        matches!(loaded.op(1), OpNode::Filter(b) if b == &[0xff]),
        "an undecodable program must stay present, not collapse to a pass-all filter"
    );
}
