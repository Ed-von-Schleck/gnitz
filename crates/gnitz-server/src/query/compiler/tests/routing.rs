use super::*;
use crate::query::compiler::fixtures::*;
use crate::test_support::{make_schema_u64_i64, pk_payload_schema};
use gnitz_wire::type_code;
use gnitz_wire::{JoinKind, MapKind, OpNode, RangeRel, ReindexRole, TypeCode};
use std::collections::HashMap;

/// 3-column compound PK `(U32, U64, U64)` + one payload, so a `CLUSTER BY`
/// prefix has more than one proper-prefix width to be tested at.
fn three_col_pk_schema(dist_k: u8) -> SchemaDescriptor {
    three_col_placed(Placement::Keyed { prefix_len: dist_k })
}

fn three_col_placed(placement: Placement) -> SchemaDescriptor {
    pk_payload_schema(&[type_code::U32, type_code::U64, type_code::U64]).with_placement(placement)
}

/// The distribution prefix must match EXACTLY: a super-prefix would let the two
/// sides of a join skip their exchange at different widths, hashing equal join
/// keys to different workers and silently dropping matches.
#[test]
fn shard_cols_match_dist_key_is_exact_prefix() {
    // `Keyed { k }` matches `pk_indices()[..k]` and nothing else.
    for (k, want) in [(1u8, &[0u32][..]), (2, &[0, 1]), (0, &[0, 1, 2])] {
        let s = three_col_pk_schema(k); // k = 0 is the default: the whole PK
        for cand in [&[][..], &[0], &[0, 1], &[0, 1, 2], &[1]] {
            assert_eq!(
                shard_cols_match_dist_key(&s, cand),
                cand == want,
                "k={k}: {cand:?} against dist key {want:?}"
            );
        }
    }
    // A relation whose rows are not placed by `worker_for_pk` has no shard key
    // naming where they already are, whatever its PK columns look like.
    for p in [Placement::Replicated, Placement::Local] {
        let s = three_col_placed(p);
        for cand in [&[][..], &[0], &[0, 1, 2]] {
            assert!(!shard_cols_match_dist_key(&s, cand), "{p:?}: {cand:?}");
        }
    }
}

/// The output IPC is elided only where the output's own route key reproduces the
/// input prefix's: at `k == 1` (one column, same `widen_pk_be` value) and at
/// `k == |PK|` (the source PK re-emitted verbatim). In between, a multi-column
/// key folds through Xxh3 into a u128 unrelated to the prefix's, so the output
/// would land on another worker and the addressing store would lose it.
#[test]
fn the_output_exchange_skips_only_at_the_two_ends_of_the_prefix() {
    let skips = |schema: SchemaDescriptor, shard_cols: Vec<u32>| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, gnitz_wire::OpNode::ExchangeShard { shard_cols }),
            ],
            vec![(0, 1, SLOT_IN)],
        );
        let ext = sources([(7, schema)]);
        ViewMeta::derive(&loaded, &ext).unwrap().skips_exchange
    };
    assert!(
        skips(three_col_pk_schema(1), vec![0]),
        "k == 1: one column routes alike"
    );
    assert!(
        !skips(three_col_pk_schema(2), vec![0, 1]),
        "1 < k < |PK|: the fold is unrelated to the prefix, so the shard must run"
    );
    assert!(skips(three_col_pk_schema(0), vec![0, 1, 2]), "k == |PK|: PK re-emitted");
    // The key must still BE the distribution prefix.
    assert!(!skips(three_col_pk_schema(1), vec![1]), "not the leading column");
    assert!(!skips(three_col_placed(Placement::Local), vec![0, 1, 2]), "unkeyed");

    // Behind a PK-preserving map the shard columns index the map's output, whose
    // leading slots are the source PK — here the source's column 2.
    let behind_a_map = |shard_cols: Vec<u32>| {
        let schema = SchemaDescriptor::new(
            &[
                gnitz_store::schema::SchemaColumn::new(type_code::I64, 0),
                gnitz_store::schema::SchemaColumn::new(type_code::I64, 0),
                gnitz_store::schema::SchemaColumn::new(type_code::U64, 0),
            ],
            &[2],
        )
        .with_placement(Placement::Keyed { prefix_len: 1 });
        let loaded = loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Projection(vec![0, 1]))),
                (2, gnitz_wire::OpNode::ExchangeShard { shard_cols }),
            ],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        );
        let ext = sources([(7, schema)]);
        ViewMeta::derive(&loaded, &ext).unwrap().skips_exchange
    };
    assert!(behind_a_map(vec![0]), "the map's slot 0 is the source PK");
    assert!(
        !behind_a_map(vec![1]),
        "a payload slot is never the distribution prefix"
    );
}

/// The exchange is skipped where the join key is exactly a source's distribution
/// prefix, and — separately — wherever any participant is replicated.
#[test]
fn co_partitioning_needs_the_exact_pk_sequence_or_a_replicated_participant() {
    // Compound PK (a, b) at columns 0, 1; column 2 is payload.
    let compound = pk_payload_schema(&[type_code::U64; 2]);
    let ext = sources([(7, compound)]);
    let none = FxHashSet::default();
    let co = |cols: Vec<gnitz_wire::ReindexSlot>| {
        compute_co_partitioned(&JoinShardMap::from_iter([(7i64, cols)]), &ext, &none, &none).contains(&7)
    };
    assert!(co(vec![(0, None), (1, None)]), "shard [pk0, pk1] equals pk_indices()");
    assert!(
        !co(vec![(1, None), (0, None)]),
        "permuted [pk1, pk0] != pk_indices() order"
    );
    // A promoted key (a carried target) never co-partitions: native PK
    // partitions are at the source width, not the T-wide trace key.
    assert!(
        !co(vec![(0, Some(TypeCode::I64)), (1, None)]),
        "a promoted PK slot must go through the exchange"
    );

    // Two single-PK (U64) join sides keyed on a NON-PK payload column, so neither
    // side's key matches its distribution prefix — the only reason to skip is
    // replication.
    let base = make_schema_u64_i64;
    let replicated = base().with_placement(Placement::Replicated);
    let join_on_payload = JoinShardMap::from_iter([(7i64, vec![(1u32, None)]), (8i64, vec![(1u32, None)])]);
    let skips = |ext: RelationRegistry, outside_joins: &[i64], set_fed: &[i64]| {
        let co = compute_co_partitioned(
            &join_on_payload,
            &ext,
            &outside_joins.iter().copied().collect(),
            &set_fed.iter().copied().collect(),
        );
        (co.contains(&7), co.contains(&8))
    };
    let both_skip = |ext: RelationRegistry, outside_joins: &[i64]| skips(ext, outside_joins, &[]);
    assert_eq!(
        both_skip(sources([(7, base()), (8, base())]), &[]),
        (false, false),
        "two partitioned sides on a non-PK key both go through the exchange"
    );
    // A partitioned fact skips too when its partner is replicated: it stays in
    // its own PK partitioning and joins the full local dim copy.
    assert_eq!(
        both_skip(sources([(7, replicated), (8, base())]), &[]),
        (true, true),
        "a replicated dim lets both sides skip"
    );
    assert_eq!(
        both_skip(sources([(7, replicated), (8, base())]), &[8]),
        (true, true),
        "the partitioned side's own delta is on one worker either way"
    );
    // A replicated delta consumed outside the join — an outer join's preserved
    // side under its clamp — would count once per worker.
    assert_eq!(
        both_skip(sources([(7, replicated), (8, base())]), &[7]),
        (false, false),
        "a replicated side used outside its join terms makes both sides scatter"
    );
    assert_eq!(
        both_skip(sources([(7, replicated), (8, replicated)]), &[]),
        (true, true),
        "replicated ⋈ replicated"
    );
    // A partitioned side clamped to a set needs every row of a key on one worker,
    // so it scatters by the key while its replicated partner still skips.
    assert_eq!(
        skips(sources([(7, replicated), (8, base())]), &[], &[8]),
        (true, false),
        "a partitioned set-fed side scatters beside a skipping replicated partner"
    );
    assert_eq!(
        skips(sources([(7, replicated), (8, base())]), &[], &[7]),
        (true, true),
        "a replicated side clamps its whole copy on every worker, so it keeps its skip"
    );
    // The write broadcast already put a replicated source's full trace on every
    // worker, so the promotion gate that blocks a partitioned source does not
    // apply to it.
    let ext_r = sources([(7, replicated)]);
    assert!(
        compute_co_partitioned(
            &JoinShardMap::from_iter([(7i64, vec![(0u32, Some(TypeCode::I64))])]),
            &ext_r,
            &FxHashSet::default(),
            &FxHashSet::default()
        )
        .contains(&7),
        "replicated source skips regardless of carried type-promotion"
    );
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
            (1, scatter_reindex(&[1])),
            (2, scan_delta(8)),
            (3, scatter_reindex(&[1])),
            (4, OpNode::Join(JoinKind::Equi)),
        ];
        let mut edges = vec![(0, 1, SLOT_IN), (2, 3, SLOT_IN), (1, 4, SLOT_IN), (3, 4, SLOT_TRACE)];
        if also_union {
            nodes.extend([(5, OpNode::Union), (6, OpNode::IntegrateSink)]);
            edges.extend([(1, 5, SLOT_IN), (4, 5, SLOT_TRACE), (5, 6, SLOT_IN)]);
        } else {
            nodes.push((5, OpNode::IntegrateSink));
            edges.push((4, 5, SLOT_IN));
        }
        let ext = sources([
            (7, make_schema_u64_i64().with_placement(Placement::Replicated)),
            (8, make_schema_u64_i64()),
        ]);
        let meta = ViewMeta::derive(&loaded_for_test(nodes, edges), &ext).unwrap();
        (meta.scatters(7), meta.scatters(8))
    };
    assert_eq!(scatters(false), (false, false), "join terms alone: both skip");
    assert_eq!(
        scatters(true),
        (true, true),
        "the replicated delta used directly: both scatter"
    );
}

/// Two scans of one source resolve identically in every process — to the one key
/// when they agree, and to a refusal when they do not, because scattering by
/// either key alone would leave the trace side keyed by the other.
#[test]
fn a_source_reached_by_two_scans_routes_by_one_key_or_refuses() {
    let two_scans = |key_a: &[u32], key_b: &[u32]| -> ViewMeta {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(10)),
                (1, scatter_reindex(key_a)),
                (2, scan_delta(10)),
                (3, scatter_reindex(key_b)),
                (4, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::Equi)),
                (5, gnitz_wire::OpNode::IntegrateSink),
            ],
            vec![
                (0, 1, SLOT_IN),
                (2, 3, SLOT_IN),
                (1, 4, SLOT_IN),
                (3, 4, SLOT_TRACE),
                (4, 5, SLOT_IN),
            ],
        );
        ViewMeta::derive(&loaded, &sources([])).unwrap()
    };

    let agreed = two_scans(&[1], &[1]);
    assert_eq!(
        join_cols(agreed.relay_route(10)),
        Some(vec![1]),
        "one key reached twice routes by that key"
    );
    let conflicting = two_scans(&[1], &[2]);
    assert!(
        matches!(conflicting.relay_route(10), RelayRoute::NoSingleKey),
        "two distinct keys must refuse the round, not pick one"
    );
}

/// A source whose path to the join carries no reindex Map has no route key of its
/// own, so it takes the view's shard columns — and never the join scatter.
#[test]
fn a_source_with_no_reindex_map_takes_the_view_shard_route() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, scan_delta(20)),
            (2, scatter_reindex(&[2])),
            (3, gnitz_wire::OpNode::IntegrateTrace),
            (4, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::Equi)),
            (5, gnitz_wire::OpNode::IntegrateSink),
        ],
        vec![
            (0, 2, SLOT_IN),
            (1, 3, SLOT_IN), // ScanDelta(20) → IntegrateTrace, no reindex
            (3, 4, SLOT_TRACE),
            (2, 4, SLOT_IN),
            (4, 5, SLOT_IN),
        ],
    );
    let meta = ViewMeta::derive(&loaded, &sources([])).unwrap();
    assert_eq!(join_cols(meta.relay_route(10)), Some(vec![2]));
    assert!(
        meta.scatters(10),
        "a keyed source with no matching distribution scatters"
    );
    assert_eq!(
        group_key_cols(meta.relay_route(20)),
        Some(vec![]),
        "a keyless source routes by the view's shard cols — this fixture has none"
    );
    assert!(!meta.scatters(20), "a keyless source is not in the scatter set");
}

/// A scan reaching only `Auxiliary` reindex maps is what a planner call site that
/// forgot its role produces. Honouring it would scatter the delta by nothing
/// while the trace side is keyed, so the compile is refused instead.
#[test]
fn a_scan_whose_reindex_maps_are_all_auxiliary_is_rejected() {
    let aux = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
        keep: vec![0],
        key: vec![(1, None)],
        role: gnitz_wire::ReindexRole::Auxiliary,
    });
    let loaded = loaded_for_test(
        [(0, scan_delta(10)), (1, aux), (2, gnitz_wire::OpNode::IntegrateSink)],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
    );
    assert_eq!(
        rejection(ViewMeta::derive(&loaded, &sources([]))),
        "a source's reindex maps carry no route key, so its delta cannot be scattered",
        "an orphaned reindex must fail the compile"
    );
}

/// One shape covering every route: source 7 is the join relay, source 9 the
/// keyless one, source 0 the output relay.
///
///   ScanDelta(7) → Map(reindex key_cols) ─┐
///   ScanDelta(9) ─────────────────────────→ Join(kind)
///                                           → ExchangeShard([1]) → IntegrateSink
fn join_meta(kind: JoinKind, key_cols: &[u32]) -> ViewMeta {
    join_meta_in(kind, key_cols, sources([]))
}

/// [`join_meta`] against a host that knows the sources' schemas, so
/// co-partitioning is decidable.
fn join_meta_in(kind: JoinKind, key_cols: &[u32], ext: RelationRegistry) -> ViewMeta {
    let nodes: HashMap<NodeId, OpNode> = HashMap::from([
        (0, scan_delta(7)),
        (1, scatter_reindex(key_cols)),
        (2, scan_delta(9)),
        (3, OpNode::Join(kind)),
        (4, OpNode::ExchangeShard { shard_cols: vec![1] }),
        (5, OpNode::IntegrateSink),
    ]);
    let edges = vec![
        (0, 1, SLOT_IN),
        (1, 3, SLOT_IN),
        (2, 3, SLOT_TRACE),
        (3, 4, SLOT_IN),
        (4, 5, SLOT_IN),
    ];
    ViewMeta::derive(&loaded_for_test(nodes, edges), &ext).expect("fixture routes")
}

fn pure_range(n_eq: u8) -> JoinKind {
    JoinKind::Range { n_eq, rel: RangeRel::Lt }
}

/// The `GroupKey` scatter's columns, or `None` when the route is not one.
fn group_key_cols(route: &RelayRoute) -> Option<Vec<u32>> {
    match route {
        RelayRoute::GroupKey(cols) => Some(cols.to_vec()),
        _ => None,
    }
}

/// The `JoinKey` scatter's columns, or `None` when the route is not one.
fn join_cols(route: &RelayRoute) -> Option<Vec<u32>> {
    match route {
        RelayRoute::JoinKey(slots) => Some(slots.iter().map(|&(c, _)| c).collect()),
        _ => None,
    }
}

/// A pure-range join (`n_eq == 0`) must broadcast the keyed source's delta:
/// its matches spread over the whole key space, so a scatter would leave each
/// worker probing a slice it does not own and drop rows silently.
///
/// The join relay governs a JOIN relay ONLY: neither the output relay
/// (`source_id == 0`) nor a source whose scans reach no reindex key (9) is
/// one, and reading either as one broadcasts what must scatter.
#[test]
fn only_the_keyed_source_of_a_pure_range_join_broadcasts() {
    let meta = join_meta(pure_range(0), &[1]);
    assert!(
        matches!(meta.relay_route(7), RelayRoute::Broadcast),
        "the keyed input relay of a pure-range join must broadcast"
    );
    non_join_relays_route_by_shard_cols(&meta);
}

/// A cross join has no key at all, so its keyed source broadcasts exactly as a
/// pure-range join's does.
#[test]
fn a_cross_join_broadcasts_like_a_pure_range_join() {
    let meta = join_meta(JoinKind::Cross, &[1]);
    assert!(
        matches!(meta.relay_route(7), RelayRoute::Broadcast),
        "the keyed input relay of a cross join must broadcast"
    );
    non_join_relays_route_by_shard_cols(&meta);
}

/// Shared tail of the two broadcast tests: the relays that are NOT the join's
/// keyed input route by the view's shard cols, exactly as under an equi join.
fn non_join_relays_route_by_shard_cols(meta: &ViewMeta) {
    assert_eq!(
        group_key_cols(meta.relay_route(0)).as_deref(),
        Some(&[1u32][..]),
        "source 0 is not a join relay: it routes by the view's shard cols"
    );
    assert_eq!(
        group_key_cols(meta.relay_route(9)).as_deref(),
        Some(&[1u32][..]),
        "a source carrying no reindex key routes by the view's shard cols"
    );
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
        join_cols(band.relay_route(7)),
        Some(vec![3]),
        "the trailing range slot must not route"
    );

    let equi = join_meta(JoinKind::Equi, &[3, 4]);
    assert_eq!(join_cols(equi.relay_route(7)), Some(vec![3, 4]));
}

/// A broadcast join relays its keyed source even where that source's distribution
/// IS the key it reindexes on: its matches spread over the whole other side, so
/// co-partitioning places none of them. The equi-join over the same fixture is
/// the control.
#[test]
fn a_broadcast_join_relays_a_co_partitioned_source_that_an_equi_join_would_skip() {
    let keyed_by_pk = |kind| {
        let ext = sources([(7i64, make_schema_u64_i64())]);
        join_meta_in(kind, &[0], ext).scatters(7)
    };
    assert!(!keyed_by_pk(JoinKind::Equi), "the equi join skips the relay");
    assert!(keyed_by_pk(pure_range(0)), "the range join must relay anyway");
    assert!(keyed_by_pk(JoinKind::Cross), "the cross join must relay anyway");
}

/// A source whose PK re-key only an owner filter reads needs its rows on that PK's
/// owner and nowhere else, so under a broadcast join relay it routes by the PK and
/// skips the relay where its distribution already places it there. The partner
/// feeding the join directly still broadcasts.
///
///   ScanDelta(7) → Map(reindex pk) → WorkerFilter → IntegrateTrace ─┐
///   ScanDelta(9) → Map(reindex [1]) ──────────────────────────────→ Join(range, n_eq 0)
#[test]
fn an_owner_trimmed_source_routes_by_its_pk_under_a_broadcast_join() {
    let meta = |schema: SchemaDescriptor| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, scatter_reindex(&[0])),
                (2, OpNode::WorkerFilter),
                (3, OpNode::IntegrateTrace),
                (4, scan_delta(9)),
                (5, scatter_reindex(&[1])),
                (6, OpNode::Join(pure_range(0))),
                (7, OpNode::IntegrateSink),
            ],
            vec![
                (0, 1, SLOT_IN),
                (1, 2, SLOT_IN),
                (2, 3, SLOT_IN),
                (4, 5, SLOT_IN),
                (5, 6, SLOT_IN),
                (3, 6, SLOT_TRACE),
                (6, 7, SLOT_IN),
            ],
        );
        ViewMeta::derive(&loaded, &sources([(7, schema), (9, make_schema_u64_i64())])).unwrap()
    };
    let keyed = meta(make_schema_u64_i64());
    assert_eq!(join_cols(keyed.relay_route(7)), Some(vec![0]), "routed by its PK");
    assert!(!keyed.scatters(7), "already on its PK's owner");
    assert!(matches!(keyed.relay_route(9), RelayRoute::Broadcast));
    assert!(keyed.scatters(9));

    let replicated = meta(make_schema_u64_i64().with_placement(Placement::Replicated));
    assert_eq!(join_cols(replicated.relay_route(7)), Some(vec![0]));
    assert!(
        replicated.scatters(7),
        "a replicated copy is relayed by key from one worker"
    );
}

/// `repartitions` is the single bit `source_placement` reads before letting a
/// view inherit its source's placement, so each of its two terms is asserted on a
/// circuit carrying that term ALONE — a fixture carrying both would pass with
/// either term deleted. The join term is what an `ExchangeShard` test misses: an
/// equi-join repartitions its inputs through the runtime join-shard scatter and
/// emits no shard node to see it by.
#[test]
fn repartitions_covers_the_output_shard_and_a_bare_join() {
    let repartitions_of = |nodes, edges| {
        ViewMeta::derive(&loaded_for_test(nodes, edges), &sources([]))
            .unwrap()
            .repartitions
    };
    assert!(
        !repartitions_of(
            HashMap::from([(0, scan_delta(7)), (1, OpNode::IntegrateSink)]),
            vec![(0, 1, SLOT_IN)],
        ),
        "a bare scan re-emits its source's PK region"
    );
    assert!(
        repartitions_of(
            HashMap::from([
                (0, scan_delta(7)),
                (1, OpNode::ExchangeShard { shard_cols: vec![] }),
                (2, OpNode::IntegrateSink),
            ]),
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        ),
        "a global aggregate shards on the empty key — still a repartition"
    );
    assert!(
        repartitions_of(
            HashMap::from([
                (0, scan_delta(7)),
                (1, scatter_reindex(&[1])),
                (2, scan_delta(9)),
                (3, OpNode::Join(JoinKind::Equi)),
                (4, OpNode::IntegrateSink),
            ]),
            vec![(0, 1, SLOT_IN), (1, 3, SLOT_IN), (2, 3, SLOT_TRACE), (3, 4, SLOT_IN)],
        ),
        "an equi-join carries no ExchangeShard and still repartitions"
    );
}

/// The join relay is read off the `Join` node's kind, never off
/// `has_join_shard && has_exchange`: that predicate is also true of every GROUP
/// BY view (a group reindex plus an output shard), so keying on it would divert
/// those views into the broadcast relay and corrupt them. A circuit without a
/// join routes by the whole key, and each join kind names its own relay.
#[test]
fn the_join_relay_follows_the_join_kind_and_a_group_by_routes_by_the_whole_key() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(7)),
            (1, scatter_reindex(&[1])),
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
    assert_eq!(circuit_join_relay(&loaded), Ok(None));

    let joined = |kind: gnitz_wire::JoinKind| {
        loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, scan_delta(9)),
                (2, OpNode::IntegrateTrace),
                (3, OpNode::Join(kind)),
                (4, OpNode::IntegrateSink),
            ],
            vec![(0, 3, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_TRACE), (3, 4, SLOT_IN)],
        )
    };
    let range = |n_eq| gnitz_wire::JoinKind::Range { n_eq, rel: gnitz_wire::RangeRel::Lt };
    for (kind, want) in [
        (gnitz_wire::JoinKind::Equi, JoinRelay::WholeKey),
        (range(2), JoinRelay::EqPrefix { n_eq: 2 }),
        (range(0), JoinRelay::Broadcast),
        (gnitz_wire::JoinKind::Cross, JoinRelay::Broadcast),
    ] {
        assert_eq!(circuit_join_relay(&joined(kind)), Ok(Some(want)), "{kind:?}");
    }
}

/// A view's sources are routed by one relay, so two joins calling for different
/// relays are refused rather than routed by whichever the walk reaches first.
#[test]
fn joins_calling_for_different_relays_are_rejected() {
    let two_joins = |second: gnitz_wire::JoinKind| {
        loaded_for_test(
            [
                (0, scan_delta(7)),
                (1, scan_delta(9)),
                (2, OpNode::IntegrateTrace),
                (3, OpNode::IntegrateTrace),
                (4, OpNode::Join(gnitz_wire::JoinKind::Equi)),
                (5, OpNode::Join(second)),
                (6, OpNode::Union),
                (7, OpNode::IntegrateSink),
            ],
            vec![
                (0, 2, SLOT_IN),
                (1, 3, SLOT_IN),
                (0, 4, SLOT_IN),
                (3, 4, SLOT_TRACE),
                (1, 5, SLOT_IN),
                (2, 5, SLOT_TRACE),
                (4, 6, SLOT_IN),
                (5, 6, SLOT_TRACE),
                (6, 7, SLOT_IN),
            ],
        )
    };
    assert_eq!(
        circuit_join_relay(&two_joins(gnitz_wire::JoinKind::Equi)),
        Ok(Some(JoinRelay::WholeKey)),
        "both terms of one join carry the same kind"
    );
    assert_eq!(
        rejection(circuit_join_relay(&two_joins(gnitz_wire::JoinKind::Cross))),
        "circuit joins need different relays"
    );
    assert_eq!(
        rejection(ViewMeta::derive(&two_joins(gnitz_wire::JoinKind::Cross), &sources([])).map(drop)),
        "circuit joins need different relays",
        "the compile is refused"
    );
}

// ── scatter_key_of_scan: the forward (scan → reindex Map) walk ──────────

/// The pure-range-join shape the planner emits at `n_eq == 0`: the reindex Map
/// feeds the `Join(Range)` DIRECTLY as the delta term AND feeds a
/// `WorkerFilter → IntegrateTrace` toward the trace term. Its key is collected
/// once, from the flag — the fan-out is not a second contribution — and the
/// `WorkerFilter` is not a `Filter`, so the walk never steps through it.
#[test]
fn the_scatter_key_is_collected_once_however_the_reindex_map_fans_out() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(99)),
            (1, scatter_reindex(&[2])),
            (2, OpNode::WorkerFilter),
            (3, OpNode::IntegrateTrace),
            (
                4,
                OpNode::Join(gnitz_wire::JoinKind::Range { n_eq: 0, rel: gnitz_wire::RangeRel::Le }),
            ),
        ],
        vec![
            (0, 1, SLOT_IN),
            (1, 4, SLOT_IN), // reindex Map → Join (delta term, DIRECT edge)
            (1, 2, SLOT_IN), // reindex Map → WorkerFilter (toward the trace)
            (2, 3, SLOT_IN),
            (3, 4, SLOT_TRACE),
        ],
    );
    assert_eq!(scatter_key_of_scan(&loaded, 0).0, vec![vec![(2, None)]]);
}

/// A source joined on two different keys (`t ⋈ t1 ON t.a = t1.x ⋈ t2 ON t.b =
/// t2.y`) fans into two reindex Maps, and both sequences must be collected as
/// SEPARATE sequences: it is the sequence count, not the column count, that
/// decides whether one pack key can route the source at all.
#[test]
fn a_scan_fanning_into_two_reindex_maps_yields_two_sequences() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(42)),
            (1, scatter_reindex(&[2])),
            (2, OpNode::Filter(dummy_expr_blob())),
            (3, scatter_reindex(&[5])),
        ],
        // The second reindex sits behind a Filter, which the walk steps through.
        vec![(0, 1, SLOT_IN), (0, 2, SLOT_IN), (2, 3, SLOT_IN)],
    );
    assert_eq!(
        scatter_key_of_scan(&loaded, 0).0,
        vec![vec![(2, None)], vec![(5, None)]],
        "two distinct keys stay two sequences"
    );
}

/// Within one sequence, duplicate columns are PRESERVED: an overlapping key
/// (`a.x = b.p AND a.x = b.q`) reindexes `[x, x]`, possibly with distinct
/// per-slot promotion targets, and the scatter packer must mirror the trace-side
/// `ReindexPacker` slot-for-slot. Across sibling Maps carrying an IDENTICAL
/// sequence it collapses to one, which concatenating would double.
#[test]
fn a_key_sequence_survives_verbatim_but_identical_siblings_collapse() {
    let overlapping = loaded_for_test(
        [
            (0, scan_delta(42)),
            (
                1,
                OpNode::Map(MapKind::Reindex {
                    keep: vec![0],
                    key: vec![(3, None), (3, Some(gnitz_wire::TypeCode::I64))],
                    role: ReindexRole::ScatterKey,
                }),
            ),
        ],
        vec![(0, 1, SLOT_IN)],
    );
    assert_eq!(
        scatter_key_of_scan(&overlapping, 0).0,
        vec![vec![(3, None), (3, Some(gnitz_wire::TypeCode::I64))]],
        "duplicate slots and their promotion targets survive"
    );

    let siblings = loaded_for_test(
        [
            (0, scan_delta(7)),
            (1, OpNode::Filter(dummy_expr_blob())),
            (2, scatter_reindex(&[2])),
            (3, OpNode::Filter(dummy_expr_blob())),
            (4, scatter_reindex(&[2])),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (0, 3, SLOT_IN), (3, 4, SLOT_IN)],
    );
    assert_eq!(
        scatter_key_of_scan(&siblings, 0).0,
        vec![vec![(2, None)]],
        "identical sibling sequences collapse to one"
    );
}

/// Only the join reindex defines a source's scatter key, and the planner says
/// which one it is by flagging the role. A band LEFT join's left scan also feeds
/// an `Auxiliary` `a.pk` re-key for the null-fill; concatenating that would
/// corrupt the eq-prefix scatter. A scan reaching ONLY auxiliary maps is instead
/// a planner call site that forgot its role, and is reported as orphaned.
#[test]
fn an_auxiliary_reindex_never_contributes_the_scatter_key() {
    let aux_rekey = || {
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(0, None)],
            role: ReindexRole::Auxiliary,
        })
    };
    let circuit = |join_reindex: OpNode, aux_rekey: OpNode| {
        loaded_for_test(
            [
                (0, scan_delta(10)),
                (1, join_reindex),
                (2, OpNode::IntegrateTrace),
                (
                    3,
                    OpNode::Join(gnitz_wire::JoinKind::Range { n_eq: 1, rel: gnitz_wire::RangeRel::Le }),
                ),
                (4, aux_rekey),
                (5, OpNode::Map(MapKind::Projection(vec![]))),
                (6, OpNode::Distinct),
            ],
            vec![
                (0, 1, SLOT_IN),
                (1, 3, SLOT_IN),
                (1, 2, SLOT_IN), // join reindex → its own integral
                (2, 3, SLOT_TRACE),
                (0, 4, SLOT_IN),
                (4, 5, SLOT_IN),
                (5, 6, SLOT_IN), // aux a.pk re-key → proj → distinct
            ],
        )
    };
    assert_eq!(
        scatter_key_of_scan(&circuit(scatter_reindex(&[1, 2]), aux_rekey()), 0),
        (vec![vec![(1, None), (2, None)]], false, false),
        "only the trace/probe-feeding reindex defines the scatter key"
    );
    assert_eq!(
        scatter_key_of_scan(&circuit(aux_rekey(), aux_rekey()), 0),
        (vec![], true, true),
        "a scan reaching no ScatterKey map at all is orphaned"
    );
}

/// A `Map` that does not re-key contributes nothing to the scatter.
#[test]
fn a_non_reindex_map_contributes_no_scatter_key() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(7)),
            (
                1,
                OpNode::Map(MapKind::Compute(gnitz_wire::ComputeMap {
                    program: dummy_expr_blob(),
                    out_cols: vec![],
                })),
            ),
        ],
        vec![(0, 1, SLOT_IN)],
    );
    assert!(scatter_key_of_scan(&loaded, 0).0.is_empty());
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
    let rekey = || scatter_reindex(&[2]);
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
        let mut edges = vec![(0, 2, SLOT_IN), (1, 2, SLOT_TRACE)];
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
    let bound = |col: u32| gnitz_wire::IndexBound {
        idx_cols: gnitz_wire::PkColList::from_slice(&[col]),
        desc: gnitz_wire::RangeDescriptor::new(&[], gnitz_wire::Cut::Before(0), gnitz_wire::Cut::After(0)),
    };
    let scan = |source: u64, b: Option<gnitz_wire::IndexBound>| OpNode::ScanDelta { source, bound: b };
    let bounds_of = |a: OpNode, b: OpNode| {
        let loaded = loaded_for_test(
            [(0, a), (1, b), (2, OpNode::Union), (3, OpNode::IntegrateSink)],
            vec![(0, 2, SLOT_IN), (1, 2, SLOT_TRACE), (2, 3, SLOT_IN)],
        );
        let meta = ViewMeta::derive(&loaded, &sources([])).unwrap();
        let mut got: Vec<(i64, Vec<u32>)> = meta
            .source_bounds
            .iter()
            .map(|(&s, b)| (s, b.idx_cols.as_slice().to_vec()))
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
