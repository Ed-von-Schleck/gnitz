use super::*;
use crate::query::compiler::fixtures::*;
use crate::test_support::{make_schema_u64_i64, pk_payload_schema};
use gnitz_store::schema::Placement;
use gnitz_wire::TypeCode;
use gnitz_wire::{JoinKind, MapKind, OpNode, RangeRel};
use std::collections::HashMap;

/// `derive`'s routing metadata, placed as a one-column-PK view.
fn derive(loaded: &LoadedCircuit, registry: &RelationRegistry) -> Result<ViewMeta, String> {
    ViewMeta::derive(loaded, registry, 1).map(|(meta, _)| meta)
}

/// A U64 PK and five I64 payload columns: every key a fixture names is payload.
fn wide_schema() -> SchemaDescriptor {
    let mut cols = vec![gnitz_store::schema::SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..5).map(|_| gnitz_store::schema::SchemaColumn::new(TypeCode::I64, false)));
    SchemaDescriptor::new(&cols, &[0])
}

/// 3-column compound PK `(U32, U64, U64)` + one payload, so a `CLUSTER BY`
/// prefix has more than one proper-prefix width to be tested at.
fn three_col_pk_schema(dist_k: u8) -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U32, TypeCode::U64, TypeCode::U64])
        .with_placement(Placement::Keyed { prefix_len: dist_k })
}

/// The shard key indexes the output layout of whatever the back-walk crossed,
/// the distribution prefix the source's own columns, so the two are reconciled
/// before the elision predicate sees them.
#[test]
fn the_output_exchange_skip_reads_the_shard_key_in_the_sources_columns() {
    let skips = |schema: SchemaDescriptor, shard_cols: Vec<u32>| {
        let loaded = loaded_for_test(
            [(0, scan_delta(7)), (1, OpNode::ExchangeShard { shard_cols })],
            vec![(0, 1, SLOT_IN)],
        );
        derive(&loaded, &sources([(7, schema)])).unwrap().skips_exchange
    };
    assert!(
        skips(three_col_pk_schema(1), vec![0]),
        "unmapped, the shard key is already in the source's columns"
    );
    assert!(!skips(three_col_pk_schema(1), vec![1]), "not the leading column");

    // Behind a PK-preserving map the shard columns index the map's output, whose
    // leading slots are the source PK — here the source's column 2.
    let behind_a_map = |shard_cols: Vec<u32>| {
        let schema = SchemaDescriptor::new(
            &[
                gnitz_store::schema::SchemaColumn::new(TypeCode::I64, false),
                gnitz_store::schema::SchemaColumn::new(TypeCode::I64, false),
                gnitz_store::schema::SchemaColumn::new(TypeCode::U64, false),
            ],
            &[2],
        )
        .with_placement(Placement::Keyed { prefix_len: 1 });
        let loaded = loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, OpNode::Map(MapKind::Projection(vec![0, 1]))),
                (2, OpNode::ExchangeShard { shard_cols }),
            ],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        );
        derive(&loaded, &sources([(7, schema)])).unwrap().skips_exchange
    };
    assert!(behind_a_map(vec![0]), "the map's slot 0 is the source PK");
    assert!(
        !behind_a_map(vec![1]),
        "a payload slot is never the distribution prefix"
    );
    // The relay a split side's partials take to their combine.
    assert!(!skips(three_col_pk_schema(1), vec![]), "an empty key");
    assert!(!skips(three_col_pk_schema(0), vec![]), "an empty key");
}

/// The exchange is skipped where the join key is exactly a source's distribution
/// prefix, and — separately — wherever any participant is replicated.
#[test]
fn co_partitioning_needs_the_exact_pk_sequence_or_a_replicated_participant() {
    // Compound PK (a, b) at columns 0, 1; column 2 is payload.
    let compound = || pk_payload_schema(&[TypeCode::U64; 2]);
    let both = |key: &[u32], ext: RelationRegistry| {
        let meta = join_meta_in(JoinKind::Equi, key, [false; 2], ext);
        (meta.source_route(7).is_none(), meta.source_route(9).is_none())
    };
    assert_eq!(
        both(&[0, 1], sources([(7, compound()), (9, compound())])),
        (true, true),
        "the key [pk0, pk1] is pk_cols(): both sides already sit where it routes"
    );
    assert_eq!(
        both(&[1, 0], sources([(7, compound()), (9, compound())])),
        (false, false),
        "permuted [pk1, pk0] is not the distribution prefix order"
    );

    // Two single-PK (U64) join sides keyed on a NON-PK payload column, so neither
    // side's key matches its distribution prefix — the only reason to skip is
    // replication.
    let base = make_schema_u64_i64;
    let replicated = base().with_placement(Placement::Replicated);
    assert_eq!(
        both(&[1], sources([(7, base()), (9, base())])),
        (false, false),
        "two partitioned sides on a non-PK key both go through the exchange"
    );
    // A partitioned fact skips too when its partner is replicated: it stays in
    // its own PK partitioning and joins the full local dim copy.
    assert_eq!(
        both(&[1], sources([(7, replicated), (9, base())])),
        (true, true),
        "a replicated dim lets both sides skip"
    );
    assert_eq!(
        both(&[1], sources([(7, replicated), (9, replicated)])),
        (true, true),
        "replicated ⋈ replicated"
    );
}

/// A partitioned side clamped to a set needs every row of a key on one worker,
/// so it scatters. A replicated side clamps its whole copy on every worker.
#[test]
fn a_set_fed_partitioned_side_scatters_beside_a_skipping_replicated_partner() {
    let base = make_schema_u64_i64;
    let ext = || sources([(7, base().with_placement(Placement::Replicated)), (9, base())]);
    let skips = |distinct: [bool; 2]| {
        let meta = join_meta_in(JoinKind::Equi, &[1], distinct, ext());
        (meta.source_route(7).is_none(), meta.source_route(9).is_none())
    };
    assert_eq!(skips([false, false]), (true, true), "neither side is clamped");
    assert_eq!(skips([false, true]), (true, false), "the partitioned side is clamped");
    assert_eq!(skips([true, false]), (true, true), "the replicated side is clamped");
}

/// `derive` reads "used outside a join" off the circuit: the replicated source 7
/// and the partitioned source 8 meet in an equi join, and in the second fixture
/// 7's reindexed delta also feeds a `Union` beside the join — the shape of an
/// outer or semi join's preserved side.
#[test]
fn a_replicated_delta_feeding_more_than_its_join_scatters_every_source() {
    let scatters = |also_union: bool| {
        let mut nodes = vec![
            (0, scan_delta(7)),
            (1, scatter_reindex(7, &[1])),
            (2, scan_delta(8)),
            (3, scatter_reindex(8, &[1])),
            (
                4,
                OpNode::Join {
                    kind: JoinKind::Equi,
                    delta_is_right: false,
                },
            ),
        ];
        let mut edges = vec![(0, 1, SLOT_IN), (2, 3, SLOT_IN), (1, 4, SLOT_IN), (3, 4, SLOT_B)];
        if also_union {
            nodes.extend([(5, OpNode::Union), (6, OpNode::IntegrateSink)]);
            edges.extend([(1, 5, SLOT_IN), (4, 5, SLOT_B), (5, 6, SLOT_IN)]);
        } else {
            nodes.push((5, OpNode::IntegrateSink));
            edges.push((4, 5, SLOT_IN));
        }
        let ext = sources([
            (7, make_schema_u64_i64().with_placement(Placement::Replicated)),
            (8, make_schema_u64_i64()),
        ]);
        let meta = derive(&loaded_for_test(nodes, edges), &ext).unwrap();
        (meta.source_route(7).is_some(), meta.source_route(8).is_some())
    };
    assert_eq!(scatters(false), (false, false), "join terms alone: both skip");
    assert_eq!(
        scatters(true),
        (true, true),
        "the replicated delta used directly: both scatter"
    );
}

/// An owner-trimmed source cannot take the replicated-partner skip: its own
/// filter drops every row the relay did not place.
///
///   ScanDelta(7) → Map(reindex [1]) → WorkerFilter → IntegrateTrace ─┐
///   ScanDelta(9) → Map(reindex [1]) ────────────────────────────────→ Join(Equi)
#[test]
fn an_owner_trimmed_source_is_refused_the_replicated_partner_skip() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(7)),
            (1, scatter_reindex(7, &[1])),
            (2, OpNode::WorkerFilter),
            (3, OpNode::IntegrateTrace),
            (4, scan_delta(9)),
            (5, scatter_reindex(9, &[1])),
            (
                6,
                OpNode::Join {
                    kind: JoinKind::Equi,
                    delta_is_right: false,
                },
            ),
            (7, OpNode::IntegrateSink),
        ],
        vec![
            (0, 1, SLOT_IN),
            (1, 2, SLOT_IN),
            (2, 3, SLOT_IN),
            (4, 5, SLOT_IN),
            (5, 6, SLOT_IN),
            (3, 6, SLOT_B),
            (6, 7, SLOT_IN),
        ],
    );
    let ext = sources([
        (7, make_schema_u64_i64()),
        (9, make_schema_u64_i64().with_placement(Placement::Replicated)),
    ]);
    let meta = derive(&loaded, &ext).unwrap();
    assert_eq!(
        join_cols(meta.source_route(7)),
        Some(vec![1]),
        "the trimmed source relays by the whole key it states"
    );
    assert!(
        meta.source_route(9).is_none(),
        "its replicated partner still skips: every worker holds its whole copy"
    );
}

/// Two scans of one source resolve identically in every process — to the one key
/// when they agree, and to a refusal when they do not, because scattering by
/// either key alone would leave the trace side keyed by the other.
#[test]
fn a_source_reached_by_two_scans_routes_by_one_key_or_refuses() {
    let two_scans = |key_a: &[u32], key_b: &[u32]| -> Result<ViewMeta, String> {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(10)),
                (1, scatter_reindex(10, key_a)),
                (2, scan_delta(10)),
                (3, scatter_reindex(10, key_b)),
                (
                    4,
                    OpNode::Join {
                        kind: JoinKind::Equi,
                        delta_is_right: false,
                    },
                ),
                (5, OpNode::IntegrateSink),
            ],
            vec![
                (0, 1, SLOT_IN),
                (2, 3, SLOT_IN),
                (1, 4, SLOT_IN),
                (3, 4, SLOT_B),
                (4, 5, SLOT_IN),
            ],
        );
        derive(&loaded, &sources([(10, wide_schema())]))
    };

    let agreed = two_scans(&[1], &[1]).expect("one key reached twice routes by that key");
    assert_eq!(join_cols(agreed.source_route(10)), Some(vec![1]));
    assert!(
        two_scans(&[1], &[2]).is_err(),
        "two distinct keys must refuse the compile, not pick one or concatenate them"
    );
}

/// A source that feeds no join states no route at all: its rows are already
/// where the view needs them. Source 20's rows reach the sink through a `Union`
/// beside the join's output, which is not a join operand.
#[test]
fn a_source_outside_every_join_states_no_route() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, scatter_reindex(10, &[2])),
            (2, OpNode::IntegrateTrace),
            (
                3,
                OpNode::Join {
                    kind: JoinKind::Equi,
                    delta_is_right: false,
                },
            ),
            (4, scan_delta(20)),
            (5, OpNode::Union),
            (6, OpNode::IntegrateSink),
        ],
        vec![
            (0, 1, SLOT_IN),
            (1, 2, SLOT_IN),
            (1, 3, SLOT_IN),
            (2, 3, SLOT_B),
            (3, 5, SLOT_IN),
            (4, 5, SLOT_B),
            (5, 6, SLOT_IN),
        ],
    );
    let meta = derive(&loaded, &sources([(10, wide_schema()), (20, wide_schema())])).unwrap();
    assert_eq!(join_cols(meta.source_route(10)), Some(vec![2]));
    assert!(
        meta.source_route(20).is_none(),
        "a source outside every join has no route"
    );
}

/// A join operand with no stated route would be integrated wherever it was
/// produced while the trace side is scattered by the key, so the compile is
/// refused.
#[test]
fn a_join_operand_whose_source_states_no_route_is_rejected() {
    let aux = OpNode::Map(MapKind::Reindex {
        keep: vec![0],
        key: vec![(1, None)],
        role: gnitz_wire::ReindexRole::Auxiliary,
        nulls: gnitz_wire::NullKeys::Keep,
    });
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, aux),
            (2, OpNode::IntegrateTrace),
            (
                3,
                OpNode::Join {
                    kind: JoinKind::Equi,
                    delta_is_right: false,
                },
            ),
            (4, OpNode::IntegrateSink),
        ],
        vec![
            (0, 1, SLOT_IN),
            (1, 2, SLOT_IN),
            (1, 3, SLOT_IN),
            (2, 3, SLOT_B),
            (3, 4, SLOT_IN),
        ],
    );
    assert!(
        rejection(derive(&loaded, &sources([(10, wide_schema())]))).contains("states no scatter key"),
        "an auxiliary-only join operand must fail the compile"
    );
}

/// A `Map` between the scan and the reindex moves the key, so the route is read
/// as the source's own columns: reading the node's own key would scatter source
/// 7 by column 2 of a relation whose join column is 5.
#[test]
fn the_route_is_the_stated_one_not_the_reindex_nodes_own_key() {
    let reindex = OpNode::Map(MapKind::Reindex {
        keep: vec![0],
        key: vec![(2, None)],
        role: gnitz_wire::ReindexRole::ScatterKey { source: 7, source_key: vec![(5, None)] },
        nulls: gnitz_wire::NullKeys::Keep,
    });
    let loaded = loaded_for_test(
        [
            (0, scan_delta(7)),
            (1, OpNode::Map(MapKind::Projection(vec![5]))),
            (2, reindex),
            (3, OpNode::IntegrateTrace),
            (
                4,
                OpNode::Join {
                    kind: JoinKind::Equi,
                    delta_is_right: false,
                },
            ),
            (5, OpNode::IntegrateSink),
        ],
        vec![
            (0, 1, SLOT_IN),
            (1, 2, SLOT_IN),
            (2, 3, SLOT_IN),
            (2, 4, SLOT_IN),
            (3, 4, SLOT_B),
            (4, 5, SLOT_IN),
        ],
    );
    let meta = derive(&loaded, &sources([(7, wide_schema())])).unwrap();
    assert_eq!(join_cols(meta.source_route(7)), Some(vec![5]));
}

/// Workers scatter by the stated key mid-round, where a refusal is fatal. A
/// circuit is client-supplied, so a key the relation cannot route by —
/// here a column it has not got — is refused at derive instead.
#[test]
fn a_stated_route_the_relation_cannot_route_by_is_rejected() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(7)),
            (1, scatter_reindex(7, &[9])),
            (2, OpNode::IntegrateTrace),
            (
                3,
                OpNode::Join {
                    kind: JoinKind::Equi,
                    delta_is_right: false,
                },
            ),
            (4, OpNode::IntegrateSink),
        ],
        vec![
            (0, 1, SLOT_IN),
            (1, 2, SLOT_IN),
            (1, 3, SLOT_IN),
            (2, 3, SLOT_B),
            (3, 4, SLOT_IN),
        ],
    );
    assert!(
        rejection(derive(&loaded, &sources([(7, make_schema_u64_i64())]))).contains("source 7 scatter key"),
        "a key naming a column the relation has not got must fail the compile"
    );
}

/// A band join naming more equality slots than its source's reindex key holds.
/// The circuit is client-supplied catalog data, so the route is refused rather
/// than sliced — and `derive` runs on the master and on every worker, ahead of
/// any compile that could have caught it.
#[test]
fn a_band_join_whose_n_eq_overruns_the_reindex_key_is_rejected() {
    let nodes: HashMap<NodeId, OpNode> = HashMap::from([
        (0, scan_delta(7)),
        (1, scatter_reindex(7, &[1, 2])),
        (2, scan_delta(9)),
        (3, scatter_reindex(9, &[1, 2])),
        (
            4,
            OpNode::Join {
                kind: JoinKind::Range { n_eq: 5, rel: RangeRel::Lt },
                delta_is_right: false,
            },
        ),
        (5, OpNode::IntegrateSink),
    ]);
    let edges = vec![
        (0, 1, SLOT_IN),
        (1, 4, SLOT_IN),
        (2, 3, SLOT_IN),
        (3, 4, SLOT_B),
        (4, 5, SLOT_IN),
    ];
    assert_eq!(
        rejection(derive(
            &loaded_for_test(nodes, edges),
            &sources([(7, wide_schema()), (9, wide_schema())])
        )),
        "band join: n_eq does not match the source's reindex key arity",
    );
}

/// One shape covering every route: sources 7 and 9 are the join's two operands,
/// each optionally clamped to a set on the way in, and source 0 the output relay.
///
///   ScanDelta(7) → [Distinct] → Map(reindex key_cols) ─┐
///   ScanDelta(9) → [Distinct] → Map(reindex key_cols) ─→ Join(kind)
///                                        → ExchangeShard([1]) → IntegrateSink
fn join_meta_in(kind: JoinKind, key_cols: &[u32], distinct: [bool; 2], ext: RelationRegistry) -> ViewMeta {
    let mut nodes: Vec<(NodeId, OpNode)> = Vec::new();
    let mut edges: Vec<(NodeId, NodeId, usize)> = Vec::new();
    let mut operands = Vec::new();
    for (i, &source) in [7u64, 9].iter().enumerate() {
        let mut cur = nodes.len();
        nodes.push((cur, scan_delta(source)));
        if distinct[i] {
            let d = nodes.len();
            nodes.push((d, OpNode::Distinct));
            edges.push((cur, d, SLOT_IN));
            cur = d;
        }
        let rekey = nodes.len();
        nodes.push((rekey, scatter_reindex(source, key_cols)));
        edges.push((cur, rekey, SLOT_IN));
        operands.push(rekey);
    }
    let join = nodes.len();
    nodes.push((join, OpNode::Join { kind, delta_is_right: false }));
    edges.push((operands[0], join, SLOT_IN));
    edges.push((operands[1], join, SLOT_B));
    let shard = nodes.len();
    nodes.push((shard, OpNode::ExchangeShard { shard_cols: vec![1] }));
    edges.push((join, shard, SLOT_IN));
    let sink = nodes.len();
    nodes.push((sink, OpNode::IntegrateSink));
    edges.push((shard, sink, SLOT_IN));
    derive(&loaded_for_test(nodes, edges), &ext).expect("fixture routes")
}

/// [`join_meta_in`] over sources whose keys are all payload, so nothing skips.
fn join_meta(kind: JoinKind, key_cols: &[u32]) -> ViewMeta {
    join_meta_in(
        kind,
        key_cols,
        [false; 2],
        sources([(7, wide_schema()), (9, wide_schema())]),
    )
}

fn pure_range(n_eq: u8) -> JoinKind {
    JoinKind::Range { n_eq, rel: RangeRel::Lt }
}

/// The `JoinKey` scatter's columns, or `None` when there is no such route.
fn join_cols(route: Option<&RelayRoute>) -> Option<Vec<u32>> {
    match route? {
        RelayRoute::JoinKey(slots) => Some(slots.iter().map(|&(c, _)| c).collect()),
        _ => None,
    }
}

/// A pure-range join (`n_eq == 0`) and a cross join spread their matches over
/// the whole key space, so the keyed source's delta must be broadcast. The
/// output relay (`source_id == 0`) is not a join relay and still scatters.
#[test]
fn the_keyed_source_of_a_keyless_join_broadcasts_and_the_output_relay_does_not() {
    for kind in [pure_range(0), JoinKind::Cross] {
        let meta = join_meta(kind, &[1]);
        assert!(
            matches!(meta.source_route(7), Some(RelayRoute::Broadcast)),
            "{kind:?}: the keyed input relay must broadcast"
        );
        assert_eq!(
            meta.output_shard_cols(),
            &[1u32][..],
            "{kind:?}: source 0 is not a join relay — it routes by the view's shard cols"
        );
    }
}

/// A band join (`n_eq >= 1`) scatters by the equality prefix, dropping the
/// trailing range slot: equal eq-values then co-partition both sides and the
/// range probe stays partition-local. The trace-side key is `[eq…, range]`,
/// so the relay's columns are one shorter than it — where an equi-join, whose
/// relay is `WholeKey`, routes by the whole key.
#[test]
fn a_band_join_routes_by_the_equality_prefix_and_an_equi_join_by_the_whole_key() {
    let band = join_meta(pure_range(1), &[3, 4]);
    assert_eq!(
        join_cols(band.source_route(7)),
        Some(vec![3]),
        "the trailing range slot must not route"
    );

    let equi = join_meta(JoinKind::Equi, &[3, 4]);
    assert_eq!(join_cols(equi.source_route(7)), Some(vec![3, 4]));
}

/// A band join's relay scatters by the truncated `[eq…]` key, so its skip turns
/// on whether THAT key is the source's distribution prefix.
#[test]
fn a_band_join_skips_the_relay_where_its_equality_prefix_is_the_distribution_key() {
    let skips = |dist_k: u8| {
        let schema = pk_payload_schema(&[TypeCode::U64; 2]).with_placement(Placement::Keyed { prefix_len: dist_k });
        join_meta_in(pure_range(1), &[0, 1], [false; 2], sources([(7, schema), (9, schema)]))
            .source_route(7)
            .is_none()
    };
    assert!(skips(1), "CLUSTER BY (pk0) is exactly the prefix the relay scatters by");
    assert!(
        !skips(2),
        "the full-PK distribution is wider than the eq prefix, so the relay must run"
    );
}

/// A band join takes the replicated-partner skip too: the replicated trace is
/// whole on every worker, so the range probe finds every match locally.
#[test]
fn a_band_join_skips_beside_a_replicated_partner() {
    let base = || pk_payload_schema(&[TypeCode::U64; 2]);
    let ext = sources([(7, base().with_placement(Placement::Replicated)), (9, base())]);
    let meta = join_meta_in(pure_range(1), &[0, 1], [false; 2], ext);
    assert!(meta.source_route(7).is_none(), "the replicated side");
    assert!(meta.source_route(9).is_none(), "and its partitioned partner");
}

/// A broadcast join relays its keyed source even where that source's distribution
/// IS the key it reindexes on: its matches spread over the whole other side, so
/// co-partitioning places none of them. The equi-join over the same fixture is
/// the control.
#[test]
fn a_broadcast_join_relays_a_co_partitioned_source_that_an_equi_join_would_skip() {
    let keyed_by_pk = |kind| {
        let ext = sources([(7u64, make_schema_u64_i64()), (9, make_schema_u64_i64())]);
        join_meta_in(kind, &[0], [false; 2], ext).source_route(7).is_some()
    };
    assert!(!keyed_by_pk(JoinKind::Equi), "the equi join skips the relay");
    assert!(keyed_by_pk(pure_range(0)), "the range join must relay anyway");
    assert!(keyed_by_pk(JoinKind::Cross), "the cross join must relay anyway");
}

/// A source whose PK re-key only an owner filter reads routes by the whole key
/// it states, whatever relay the circuit's joins call for, and skips the relay
/// where its distribution already places it there.
///
///   ScanDelta(7) → Map(reindex pk) → WorkerFilter → IntegrateTrace ─┐
///   ScanDelta(9) → Map(reindex [1]) ──────────────────────────────→ Join(range, n_eq 0)
#[test]
fn an_owner_trimmed_source_routes_by_its_pk_under_a_broadcast_join() {
    let meta = |schema: SchemaDescriptor| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, scatter_reindex(7, &[0])),
                (2, OpNode::WorkerFilter),
                (3, OpNode::IntegrateTrace),
                (4, scan_delta(9)),
                (5, scatter_reindex(9, &[1])),
                (
                    6,
                    OpNode::Join {
                        kind: pure_range(0),
                        delta_is_right: false,
                    },
                ),
                (7, OpNode::IntegrateSink),
            ],
            vec![
                (0, 1, SLOT_IN),
                (1, 2, SLOT_IN),
                (2, 3, SLOT_IN),
                (4, 5, SLOT_IN),
                (5, 6, SLOT_IN),
                (3, 6, SLOT_B),
                (6, 7, SLOT_IN),
            ],
        );
        derive(&loaded, &sources([(7, schema), (9, make_schema_u64_i64())])).unwrap()
    };
    let keyed = meta(make_schema_u64_i64());
    assert!(keyed.source_route(7).is_none(), "already on its PK's owner");
    assert!(matches!(keyed.source_route(9), Some(RelayRoute::Broadcast)));

    let replicated = meta(make_schema_u64_i64().with_placement(Placement::Replicated));
    assert_eq!(
        join_cols(replicated.source_route(7)),
        Some(vec![0]),
        "a replicated copy is relayed by key from one worker"
    );
}

/// `pk_source` names the relation whose PK region the view's output PK region
/// is. Each shape that breaks that identity — an output shard, a bare equi-join,
/// a re-keying `Map` with neither — is asserted on a circuit carrying it ALONE.
#[test]
fn pk_source_names_only_the_relation_whose_pk_region_survives_to_the_sink() {
    let pk_source_of = |nodes, edges| pk_source(&loaded_for_test(nodes, edges));
    assert_eq!(
        pk_source_of(
            HashMap::from([(0, scan_delta(7)), (1, OpNode::IntegrateSink)]),
            vec![(0, 1, SLOT_IN)],
        ),
        Some(7),
        "a bare scan re-emits its source's PK region"
    );
    assert_eq!(
        pk_source_of(
            HashMap::from([
                (0, scan_delta(7)),
                (1, OpNode::ExchangeShard { shard_cols: vec![] }),
                (2, OpNode::IntegrateSink),
            ]),
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        ),
        None,
        "a global aggregate shards on the empty key — the rows move"
    );
    assert_eq!(
        pk_source_of(
            HashMap::from([
                (0, scan_delta(7)),
                (1, scatter_reindex(7, &[1])),
                (2, scan_delta(9)),
                (3, scatter_reindex(9, &[1])),
                (
                    4,
                    OpNode::Join {
                        kind: JoinKind::Equi,
                        delta_is_right: false
                    }
                ),
                (5, OpNode::IntegrateSink),
            ]),
            vec![
                (0, 1, SLOT_IN),
                (1, 4, SLOT_IN),
                (2, 3, SLOT_IN),
                (3, 4, SLOT_B),
                (4, 5, SLOT_IN),
            ],
        ),
        None,
        "an equi-join carries no ExchangeShard and still re-keys"
    );
    assert_eq!(
        pk_source_of(
            HashMap::from([
                (0, scan_delta(7)),
                (1, scatter_reindex(7, &[1])),
                (2, OpNode::IntegrateSink),
            ]),
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        ),
        None,
        "a bare Reindex exchanges nothing and still replaces the PK region"
    );
}

/// The join relay is read off the `Join` node's kind, never off
/// `has_join_shard && has_exchange`: that predicate is also true of every GROUP
/// BY view (a group reindex plus an output shard), so keying on it would divert
/// those views into the broadcast relay and corrupt them. A circuit without a
/// join routes by the whole key, and each join kind names its own relay.
#[test]
fn the_join_relay_follows_the_join_kind_and_a_group_by_routes_by_the_whole_key() {
    let relay_of_circuit = |loaded: &LoadedCircuit| source_uses(loaded).map(|(_, relay)| relay);
    let loaded = loaded_for_test(
        [
            (0, scan_delta(7)),
            (1, scatter_reindex(7, &[1])),
            (2, OpNode::ExchangeShard { shard_cols: vec![1] }),
            (
                3,
                OpNode::Reduce {
                    group_cols: vec![1],
                    agg: vec![gnitz_wire::AggDescriptor {
                        agg_op: gnitz_wire::AggFunc::Count,
                        col_idx: 0,
                    }],
                    global_ground: false,
                },
            ),
            (4, OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN), (3, 4, SLOT_IN)],
    );
    assert_eq!(relay_of_circuit(&loaded), Ok(None));

    let joined = |kind: JoinKind| {
        loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, scan_delta(9)),
                (2, OpNode::IntegrateTrace),
                (3, OpNode::Join { kind, delta_is_right: false }),
                (4, OpNode::IntegrateSink),
            ],
            vec![(0, 3, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_B), (3, 4, SLOT_IN)],
        )
    };
    for (kind, want) in [
        (JoinKind::Equi, JoinRelay::WholeKey),
        (pure_range(2), JoinRelay::EqPrefix { n_eq: 2 }),
        (pure_range(0), JoinRelay::Broadcast),
        (JoinKind::Cross, JoinRelay::Broadcast),
    ] {
        assert_eq!(relay_of_circuit(&joined(kind)), Ok(Some(want)), "{kind:?}");
    }
}

/// A view's sources are routed by one relay, so two joins calling for different
/// relays are refused rather than routed by whichever the walk reaches first.
#[test]
fn joins_calling_for_different_relays_are_rejected() {
    let two_joins = |second: JoinKind| {
        loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, scan_delta(9)),
                (2, OpNode::IntegrateTrace),
                (3, OpNode::IntegrateTrace),
                (
                    4,
                    OpNode::Join {
                        kind: JoinKind::Equi,
                        delta_is_right: false,
                    },
                ),
                (5, OpNode::Join { kind: second, delta_is_right: false }),
                (6, OpNode::Union),
                (7, OpNode::IntegrateSink),
            ],
            vec![
                (0, 2, SLOT_IN),
                (1, 3, SLOT_IN),
                (0, 4, SLOT_IN),
                (3, 4, SLOT_B),
                (1, 5, SLOT_IN),
                (2, 5, SLOT_B),
                (4, 6, SLOT_IN),
                (5, 6, SLOT_B),
                (6, 7, SLOT_IN),
            ],
        )
    };
    assert_eq!(
        source_uses(&two_joins(JoinKind::Equi)).map(|(_, relay)| relay),
        Ok(Some(JoinRelay::WholeKey)),
        "both terms of one join carry the same kind"
    );
    assert_eq!(
        rejection(source_uses(&two_joins(JoinKind::Cross)).map(drop)),
        "circuit joins need different relays"
    );
    assert_eq!(
        rejection(derive(&two_joins(JoinKind::Cross), &sources([])).map(drop)),
        "circuit joins need different relays",
        "the compile is refused"
    );
}

// ── scan_through_row_local: the backward (shard → scan) walk ────────────
//
// The view exchange-skip detector's source resolution. Exchanging is also
// correct, so the skip has no observable "fired" signal at the E2E layer; these
// are what pin that the walk engages exactly when it should.

#[test]
fn the_shard_walk_crosses_row_local_nodes_and_nothing_else() {
    let chain = |mids: Vec<OpNode>| {
        let shard = mids.len() + 1;
        let mut nodes = HashMap::from([
            (0, scan_delta(7)),
            (shard, OpNode::ExchangeShard { shard_cols: vec![0] }),
        ]);
        for (i, op) in mids.into_iter().enumerate() {
            nodes.insert(i + 1, op);
        }
        let edges = (0..shard).map(|i| (i, i + 1, SLOT_IN)).collect();
        scan_through_row_local(&loaded_for_test(nodes, edges), shard)
    };
    let filter = || OpNode::Filter(dummy_expr_blob());
    let rekey = || scatter_reindex(7, &[2]);
    let copy = || OpNode::Map(MapKind::Projection(vec![1]));
    let compute = || {
        OpNode::Map(MapKind::Compute(gnitz_wire::ComputeMap {
            program: dummy_expr_blob(),
            out_cols: vec![],
        }))
    };
    assert_eq!(chain(vec![]), Some((7, false)), "a bare scan resolves on the first hop");
    assert_eq!(chain(vec![filter()]), Some((7, false)));
    assert_eq!(
        chain(vec![filter(), filter()]),
        Some((7, false)),
        "a Filter chain is transparent"
    );
    assert_eq!(
        chain(vec![copy()]),
        Some((7, true)),
        "a copy list carries the PK region"
    );
    assert_eq!(
        chain(vec![compute()]),
        Some((7, true)),
        "an expression map carries the PK region"
    );
    assert_eq!(
        chain(vec![filter(), compute(), filter()]),
        Some((7, true)),
        "filters and maps interleave"
    );
    assert_eq!(chain(vec![rekey()]), None, "a reindex Map re-keys the PK");
    assert_eq!(
        chain(vec![rekey(), filter()]),
        None,
        "a Filter below the Map does not rescue it"
    );
    assert_eq!(chain(vec![OpNode::WorkerFilter]), None, "WorkerFilter is not a Filter");
}

/// A fan-in draws from more than one source, so no single table's distribution
/// prefix governs the shard key. The walk must bail at it — whether it is the
/// shard's own input or one `Filter` further in.
#[test]
fn the_shard_walk_bails_at_a_fan_in() {
    let union_then = |tail: Vec<OpNode>| {
        let shard = tail.len() + 3;
        let mut nodes = HashMap::from([
            (0, scan_delta(7)),
            (1, scan_delta(8)),
            (2, OpNode::Union),
            (shard, OpNode::ExchangeShard { shard_cols: vec![0] }),
        ]);
        for (i, op) in tail.into_iter().enumerate() {
            nodes.insert(i + 3, op);
        }
        let mut edges = vec![(0, 2, SLOT_IN), (1, 2, SLOT_B)];
        edges.extend((2..shard).map(|i| (i, i + 1, SLOT_IN)));
        scan_through_row_local(&loaded_for_test(nodes, edges), shard)
    };
    assert_eq!(union_then(vec![]), None, "the Union feeds the shard directly");
    assert_eq!(
        union_then(vec![OpNode::Filter(dummy_expr_blob())]),
        None,
        "a Filter is transparent, so the walk reaches the Union and bails there"
    );
}

/// A backfill bound survives per source scanned once; a source scanned twice
/// shares one backfill cursor, so it keeps none even where one scan is bounded.
#[test]
fn a_source_scanned_once_keeps_its_backfill_bound() {
    let bound = |col: u32| gnitz_wire::KeyRange::point(gnitz_wire::PkColList::from_slice(&[col]), &[], 0);
    let scan = |source: u64, b: Option<gnitz_wire::KeyRange>| OpNode::ScanDelta {
        source,
        bound: b.map_or(gnitz_wire::ReadBound::None, gnitz_wire::ReadBound::Range),
    };
    let bounds_of = |a: OpNode, b: OpNode| {
        let loaded = loaded_for_test(
            [(0, a), (1, b), (2, OpNode::Union), (3, OpNode::IntegrateSink)],
            vec![(0, 2, SLOT_IN), (1, 2, SLOT_B), (2, 3, SLOT_IN)],
        );
        let meta = derive(&loaded, &sources([(10, wide_schema()), (11, wide_schema())])).unwrap();
        let mut got: Vec<(u64, Vec<u32>)> = meta
            .source_bounds
            .iter()
            .map(|(&s, b)| match b {
                gnitz_wire::ReadBound::Range(r) => (s, r.cols().as_slice().to_vec()),
                other => panic!("source {s}: unexpected bound {other:?}"),
            })
            .collect();
        got.sort();
        got
    };
    assert_eq!(
        bounds_of(scan(10, Some(bound(2))), scan(11, Some(bound(3)))),
        vec![(10, vec![2]), (11, vec![3])],
        "two sources scanned once each keep their own bound"
    );
    assert_eq!(
        bounds_of(scan(10, Some(bound(2))), scan(10, None)),
        vec![],
        "a source scanned twice has none"
    );
    assert_eq!(
        bounds_of(scan(10, None), scan(11, None)),
        vec![],
        "an unbounded scan has none"
    );
}

#[test]
fn a_scatter_key_over_an_unscanned_source_is_rejected() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(7)),
            (1, scatter_reindex(9, &[1])),
            (2, OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
    );
    assert_eq!(
        rejection(derive(&loaded, &sources([(7, wide_schema()), (9, wide_schema())]))),
        "source 9 states a scatter key but is not scanned"
    );
}

#[test]
fn a_scan_of_an_unregistered_relation_is_rejected() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(7)),
            (1, scan_delta(8)),
            (2, OpNode::Union),
            (3, OpNode::IntegrateSink),
        ],
        vec![(0, 2, SLOT_IN), (1, 2, SLOT_B), (2, 3, SLOT_IN)],
    );
    assert_eq!(
        rejection(derive(&loaded, &sources([(7, wide_schema())]))),
        "relation 8 is not registered"
    );
}
