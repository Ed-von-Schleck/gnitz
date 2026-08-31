use super::*;
use crate::test_support::{make_schema_u64_i64, pk_payload_schema};
use gnitz_store::schema::type_code;
use gnitz_wire::{JoinKind, OpNode, RangeRel};

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
            HashMap::from([
                (0, scan_delta(7)),
                (1, gnitz_wire::OpNode::ExchangeShard { shard_cols }),
            ]),
            vec![(0, 1, PORT_IN)],
        );
        let ext: ExtTables = HashMap::from([(7, schema)]);
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
}

/// The exchange is skipped where the join key is exactly a source's distribution
/// prefix, and — separately — wherever any participant is replicated.
#[test]
fn co_partitioning_needs_the_exact_pk_sequence_or_a_replicated_participant() {
    // Compound PK (a, b) at columns 0, 1; column 2 is payload.
    let compound = pk_payload_schema(&[type_code::U64; 2]);
    let ext: ExtTables = HashMap::from([(7, compound)]);
    let co = |cols: Vec<(u32, u8)>| compute_co_partitioned(&HashMap::from([(7i64, cols)]), &ext).contains(&7);
    assert!(co(vec![(0, 0), (1, 0)]), "shard [pk0, pk1] equals pk_indices()");
    assert!(!co(vec![(1, 0), (0, 0)]), "permuted [pk1, pk0] != pk_indices() order");
    // A promoted key (non-zero carried tc) never co-partitions: native PK
    // partitions are at the source width, not the T-wide trace key.
    assert!(
        !co(vec![(0, type_code::I64), (1, 0)]),
        "a promoted PK slot must go through the exchange"
    );

    // Two single-PK (U64) join sides keyed on a NON-PK payload column, so neither
    // side's key matches its distribution prefix — the only reason to skip is
    // replication.
    let base = make_schema_u64_i64;
    let replicated = base().with_placement(Placement::Replicated);
    let join_on_payload = HashMap::from([(7i64, vec![(1u32, 0u8)]), (8i64, vec![(1u32, 0u8)])]);
    let both_skip = |ext: ExtTables| {
        let co = compute_co_partitioned(&join_on_payload, &ext);
        (co.contains(&7), co.contains(&8))
    };
    assert_eq!(
        both_skip(HashMap::from([(7, base()), (8, base())])),
        (false, false),
        "two partitioned sides on a non-PK key both go through the exchange"
    );
    // A partitioned fact skips too when its partner is replicated: it stays in
    // its own PK partitioning and joins the full local dim copy.
    assert_eq!(
        both_skip(HashMap::from([(7, replicated), (8, base())])),
        (true, true),
        "a replicated dim lets both sides skip"
    );
    assert_eq!(
        both_skip(HashMap::from([(7, replicated), (8, replicated)])),
        (true, true),
        "replicated ⋈ replicated"
    );
    // The write broadcast already put a replicated source's full trace on every
    // worker, so the tc-promotion gate that blocks a partitioned source does not
    // apply to it.
    let ext_r: ExtTables = HashMap::from([(7, replicated)]);
    assert!(
        compute_co_partitioned(&HashMap::from([(7i64, vec![(0u32, type_code::I64)])]), &ext_r).contains(&7),
        "replicated source skips regardless of carried type-promotion"
    );
}

/// Two scans of one source resolve identically in every process — to the one key
/// when they agree, and to a refusal when they do not, because scattering by
/// either key alone would leave the trace side keyed by the other.
#[test]
fn a_source_reached_by_two_scans_routes_by_one_key_or_refuses() {
    let two_scans = |key_a: &[u32], key_b: &[u32]| -> ViewMeta {
        let loaded = loaded_for_test(
            HashMap::from([
                (0, scan_delta(10)),
                (1, scatter_reindex(key_a)),
                (2, scan_delta(10)),
                (3, scatter_reindex(key_b)),
                (4, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace)),
                (5, gnitz_wire::OpNode::IntegrateSink),
            ]),
            vec![
                (0, 1, PORT_IN),
                (2, 3, PORT_IN),
                (1, 4, PORT_IN_A),
                (3, 4, PORT_TRACE),
                (4, 5, PORT_IN),
            ],
        );
        ViewMeta::derive(&loaded, &ExtTables::default()).unwrap()
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
        HashMap::from([
            (0, scan_delta(10)),
            (1, scan_delta(20)),
            (2, scatter_reindex(&[2])),
            (3, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace)),
            (4, gnitz_wire::OpNode::IntegrateSink),
            (5, gnitz_wire::OpNode::IntegrateTrace),
        ]),
        vec![
            (0, 2, PORT_IN),
            (1, 5, PORT_IN), // ScanDelta(20) → IntegrateTrace, no reindex
            (5, 3, PORT_TRACE),
            (2, 3, PORT_IN_A),
            (3, 4, PORT_IN),
        ],
    );
    let meta = ViewMeta::derive(&loaded, &ExtTables::default()).unwrap();
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
        reindex_cols: vec![1],
        reindex_target_tcs: vec![],
        role: gnitz_wire::ReindexRole::Auxiliary,
    });
    let loaded = loaded_for_test(
        HashMap::from([(0, scan_delta(10)), (1, aux), (2, gnitz_wire::OpNode::IntegrateSink)]),
        vec![(0, 1, PORT_IN), (1, 2, PORT_IN)],
    );
    assert!(
        matches!(
            ViewMeta::derive(&loaded, &ExtTables::default()),
            Err(CompileError::Rejected(
                "a source's reindex maps carry no route key, so its delta cannot be scattered"
            ))
        ),
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
    let nodes: HashMap<i32, OpNode> = HashMap::from([
        (0, scan_delta(7)),
        (1, scatter_reindex(key_cols)),
        (2, scan_delta(9)),
        (3, OpNode::Join(kind)),
        (4, OpNode::ExchangeShard { shard_cols: vec![1] }),
        (5, OpNode::IntegrateSink),
    ]);
    let edges = vec![
        (0, 1, PORT_IN),
        (1, 3, PORT_IN_A),
        (2, 3, PORT_TRACE),
        (3, 4, PORT_IN),
        (4, 5, PORT_IN),
    ];
    ViewMeta::derive(&loaded_for_test(nodes, edges), &ExtTables::default()).expect("fixture routes")
}

fn pure_range(n_eq: u8) -> JoinKind {
    JoinKind::DeltaTraceRange {
        n_eq,
        rel: RangeRel::Lt,
    }
}

/// The `GroupKey` scatter's columns, or `None` when the route is not one.
fn group_key_cols(route: &RelayRoute) -> Option<Vec<u32>> {
    match route {
        RelayRoute::Scatter {
            cols,
            mode: RouteMode::GroupKey,
            ..
        } => Some(cols.to_vec()),
        _ => None,
    }
}

/// The `JoinPromote` scatter's columns, or `None` when the route is not one.
fn join_cols(route: &RelayRoute) -> Option<Vec<u32>> {
    match route {
        RelayRoute::Scatter {
            cols,
            mode: RouteMode::JoinPromote,
            ..
        } => Some(cols.to_vec()),
        _ => None,
    }
}

/// A pure-range join (`n_eq == 0`) must broadcast the keyed source's delta:
/// its matches spread over the whole key space, so a scatter would leave each
/// worker probing a slice it does not own and drop rows silently.
///
/// `range_join_n_eq` governs a JOIN relay ONLY: neither the output relay
/// (`source_id == 0`) nor a source whose scans reach no reindex key (9) is
/// one, and reading either as one broadcasts what must scatter.
#[test]
fn only_the_keyed_source_of_a_pure_range_join_broadcasts() {
    let meta = join_meta(pure_range(0), &[1]);
    assert_eq!(meta.range_join_n_eq, Some(0));
    assert!(
        matches!(meta.relay_route(7), RelayRoute::Broadcast),
        "the keyed input relay of a pure-range join must broadcast"
    );
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
/// so the relay's columns are one shorter than it — where an equi-join, which
/// carries no `range_join_n_eq`, routes by the whole key.
#[test]
fn a_band_join_routes_by_the_equality_prefix_and_an_equi_join_by_the_whole_key() {
    let band = join_meta(pure_range(1), &[3, 4]);
    assert_eq!(band.range_join_n_eq, Some(1));
    assert_eq!(
        join_cols(band.relay_route(7)),
        Some(vec![3]),
        "the trailing range slot must not route"
    );

    let equi = join_meta(JoinKind::DeltaTrace, &[3, 4]);
    assert_eq!(equi.range_join_n_eq, None);
    assert_eq!(join_cols(equi.relay_route(7)), Some(vec![3, 4]));
}

/// A circuit that could not be read routes everything by `∅` — every row to
/// partition 0's owner — rather than by a graph that is not there.
#[test]
fn nothing_special_routes_every_source_by_the_empty_key() {
    let meta = ViewMeta::nothing_special();
    assert_eq!(group_key_cols(meta.relay_route(0)), Some(vec![]));
    assert_eq!(group_key_cols(meta.relay_route(42)), Some(vec![]));
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
        ViewMeta::derive(&loaded_for_test(nodes, edges), &ExtTables::default())
            .unwrap()
            .repartitions
    };
    assert!(
        !repartitions_of(
            HashMap::from([(0, scan_delta(7)), (1, OpNode::IntegrateSink)]),
            vec![(0, 1, PORT_IN)],
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
            vec![(0, 1, PORT_IN), (1, 2, PORT_IN)],
        ),
        "a global aggregate shards on the empty key — still a repartition"
    );
    assert!(
        repartitions_of(
            HashMap::from([
                (0, scan_delta(7)),
                (1, scatter_reindex(&[1])),
                (2, scan_delta(9)),
                (3, OpNode::Join(JoinKind::DeltaTrace)),
                (4, OpNode::IntegrateSink),
            ]),
            vec![(0, 1, PORT_IN), (1, 3, PORT_IN_A), (2, 3, PORT_TRACE), (3, 4, PORT_IN)],
        ),
        "an equi-join carries no ExchangeShard and still repartitions"
    );
}
