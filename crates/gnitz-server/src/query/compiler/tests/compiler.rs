use super::fixtures::*;
use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{col_def, pk_only_schema, scratch_dir, write_identity_circuit};
use gnitz_store::schema::SchemaColumn;
use gnitz_wire::{type_code, OpNode};

// ── carve: the exchange shape, decided on the graph ─────────────────────

/// The guard `carve` rejected the circuit `nodes`/`edges` with.
fn carve_rejection(nodes: Vec<(NodeId, OpNode)>, edges: Vec<(NodeId, NodeId, usize)>) -> String {
    rejection(loaded_for_test(nodes, edges).carve().map(drop))
}

#[test]
fn more_than_two_exchange_shards_are_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, scan_delta(11)),
                (2, scan_delta(12)),
                (3, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (4, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (5, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (6, OpNode::Union),
                (7, OpNode::Union),
                (8, OpNode::IntegrateSink),
            ],
            vec![
                (0, 3, SLOT_IN),
                (1, 4, SLOT_IN),
                (2, 5, SLOT_IN),
                (3, 6, SLOT_IN),
                (4, 6, SLOT_TRACE),
                (6, 7, SLOT_IN),
                (5, 7, SLOT_TRACE),
                (7, 8, SLOT_IN),
            ],
        ),
        "more than two exchange nodes"
    );
}

/// A pair side relays under its source, routed by the view's one shard key, so
/// two sides keyed differently would bounds-check one side by the other's key.
#[test]
fn exchange_sides_sharding_on_different_keys_are_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, scan_delta(11)),
                (2, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (3, OpNode::ExchangeShard { shard_cols: vec![1] }),
                (4, OpNode::Union),
                (5, OpNode::IntegrateSink),
            ],
            vec![
                (0, 2, SLOT_IN),
                (1, 3, SLOT_IN),
                (2, 4, SLOT_IN),
                (3, 4, SLOT_TRACE),
                (4, 5, SLOT_IN)
            ],
        ),
        "exchange sides shard on different keys"
    );
}

/// A node in two sides would open one scratch child twice, under two
/// unsynchronized shard indexes.
#[test]
fn exchange_sides_sharing_an_ancestor_are_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (2, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (3, OpNode::Union),
                (4, OpNode::IntegrateSink),
            ],
            vec![
                (0, 1, SLOT_IN),
                (0, 2, SLOT_IN),
                (1, 3, SLOT_IN),
                (2, 3, SLOT_TRACE),
                (3, 4, SLOT_IN)
            ],
        ),
        "exchange sides share a node"
    );
}

/// A shard upstream of another lies in both ancestor sets, so it never reaches
/// a plan's node list — where emitting it would abort a worker. No planner path
/// emits the shape; a circuit hand-built through `Circuit` can.
#[test]
fn a_chained_exchange_is_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (2, OpNode::Negate),
                (3, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (4, OpNode::IntegrateSink),
            ],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN), (3, 4, SLOT_IN)],
        ),
        "exchange sides share a node"
    );
}

/// A delta is routed to the side scanning its source, so a scan in the post
/// phase of an exchanged plan would receive nothing.
#[test]
fn a_post_phase_scan_in_an_exchanged_plan_is_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, scan_delta(11)),
                (2, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (3, OpNode::Union),
                (4, OpNode::IntegrateSink),
            ],
            vec![(0, 2, SLOT_IN), (2, 3, SLOT_IN), (1, 3, SLOT_TRACE), (3, 4, SLOT_IN)],
        ),
        "an exchanged plan scans a relation outside every exchange side"
    );
}

/// Each side is its shard's ancestors without the shard; the post phase is every
/// other node, both shards excluded and the sink included.
#[test]
fn the_carve_splits_sides_from_the_post_phase() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, scan_delta(11)),
            (2, OpNode::Negate),
            (3, OpNode::ExchangeShard { shard_cols: vec![0] }),
            (4, OpNode::ExchangeShard { shard_cols: vec![0] }),
            (5, OpNode::Union),
            (6, OpNode::Distinct),
            (7, OpNode::IntegrateSink),
        ],
        vec![
            (0, 2, SLOT_IN),
            (2, 3, SLOT_IN),
            (1, 4, SLOT_IN),
            (3, 5, SLOT_IN),
            (4, 5, SLOT_TRACE),
            (5, 6, SLOT_IN),
            (6, 7, SLOT_IN),
        ],
    );
    let carve = loaded.carve().expect("a well-formed set-op");
    let mut sides: Vec<(NodeId, Vec<NodeId>)> = carve.sides.iter().map(|s| (s.shard, s.nodes.clone())).collect();
    sides.sort();
    assert_eq!(sides, vec![(3, vec![0, 2]), (4, vec![1])]);
    assert_eq!(carve.post, vec![5, 6, 7]);

    let unexchanged = loaded_for_test(
        [(0, scan_delta(10)), (1, OpNode::Negate), (2, OpNode::IntegrateSink)],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
    );
    let carve = unexchanged.carve().expect("an exchange-free circuit");
    assert!(carve.sides.is_empty());
    assert_eq!(carve.post, vec![0, 1, 2], "the post phase is the whole circuit");
}

// ── sink: exactly one ───────────────────────────────────────────────────

#[test]
fn a_circuit_holds_exactly_one_sink() {
    assert_eq!(
        rejection(loaded_for_test([(0, scan_delta(10))], vec![]).sink()),
        "circuit has no IntegrateSink"
    );
    assert_eq!(
        rejection(loaded_for_test([], vec![]).sink()),
        "circuit has no IntegrateSink",
        "an empty circuit"
    );
    let two = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, OpNode::IntegrateSink),
            (2, OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (0, 2, SLOT_IN)],
    );
    assert_eq!(rejection(two.sink()), "circuit has more than one IntegrateSink");
    let one = loaded_for_test([(0, scan_delta(10)), (1, OpNode::IntegrateSink)], vec![(0, 1, SLOT_IN)]);
    assert_eq!(one.sink(), Ok(1));
}

// ── compile_view: the sink contract ─────────────────────────────────────

/// The sink must match the view schema's physical layout, not just its width.
#[test]
fn a_sink_schema_unequal_to_the_view_schema_is_rejected() {
    let dir = scratch_dir("compiler", "sink_schema");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let u64_only = engine
        .create_table("public.u64_only", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let u64_i64 = engine
        .create_table(
            "public.u64_i64",
            &[col_def("id", type_code::U64), col_def("v", type_code::I64)],
            &[0],
        )
        .unwrap();
    let u64_str = engine
        .create_table(
            "public.u64_str",
            &[col_def("id", type_code::U64), col_def("s", type_code::STRING)],
            &[0],
        )
        .unwrap();
    // One identity view per source, `ScanDelta(source) → IntegrateSink`.
    let view_over = |engine: &mut CatalogEngine, source: i64| {
        let vid = engine.allocate_table_id().unwrap();
        write_identity_circuit(engine, vid, source, gnitz_wire::ReadBound::None);
        vid
    };
    let (v_u64_only, v_u64_i64, v_u64_str) = (
        view_over(&mut engine, u64_only),
        view_over(&mut engine, u64_i64),
        view_over(&mut engine, u64_str),
    );

    let view_schema = pk_only_schema(&[type_code::U64]);
    let string_payload = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    let against = |view_schema: &SchemaDescriptor, vid: i64| {
        let site = ViewSite {
            dir: &dir,
            id: vid as u64,
            registry: engine.registry(),
        };
        compile_view(site, view_schema, false).map(drop)
    };
    assert!(against(&view_schema, v_u64_only).is_ok(), "an equal pair compiles");
    for vid in [v_u64_i64, v_u64_str] {
        assert_eq!(
            rejection(against(&view_schema, vid)),
            "sink schema does not match view output schema",
        );
    }
    // Equal column counts, different types: the same guard, on the sharper input.
    assert_eq!(
        rejection(against(&string_payload, v_u64_i64)),
        "sink schema does not match view output schema",
    );
    engine.close();
}
