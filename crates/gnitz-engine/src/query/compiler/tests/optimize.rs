use super::*;
use gnitz_store::schema::{type_code, SchemaColumn};

/// 3-column compound PK `(U32, U64, U64)` + one payload, so a `CLUSTER BY`
/// prefix has more than one proper-prefix width to be tested at.
fn three_col_pk_schema(dist_k: u8) -> SchemaDescriptor {
    three_col_placed(Placement::Keyed { prefix_len: dist_k })
}

fn three_col_placed(placement: Placement) -> SchemaDescriptor {
    SchemaDescriptor::new_with_placement(
        &[
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2],
        placement,
    )
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
        CircuitFacts::derive(&loaded, &ext).unwrap().skips_exchange
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
    let compound = crate::test_support::pk_payload_schema(&[type_code::U64; 2]);
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
    const COLS: [SchemaColumn; 2] = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
    ];
    let base = || SchemaDescriptor::new(&COLS, &[0]);
    let replicated = SchemaDescriptor::new_with_placement(&COLS, &[0], Placement::Replicated);
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

/// The relay's pack key is derived once, over `loaded.ordered`, because the
/// master and the worker derive their halves in separate processes: a walk keyed
/// by `nodes`' per-process hash order could answer differently on each. Two scans
/// of one source therefore resolve the same way everywhere — to the one key when
/// they agree, and to a refusal when they do not.
#[test]
fn a_source_reached_by_two_scans_resolves_to_one_key_or_refuses() {
    let two_scans = |key_a: &[u32], key_b: &[u32]| {
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
        CircuitFacts::derive(&loaded, &ExtTables::default())
            .unwrap()
            .keys
            .remove(&10)
    };
    assert_eq!(two_scans(&[1], &[1]), Some(Some(vec![(1, 0)])), "one key reached twice");
    assert_eq!(
        two_scans(&[1], &[2]),
        Some(Some(None)).flatten(),
        "two distinct keys must refuse, not pick one"
    );
}

/// A source whose path to the join carries no reindex Map has no route key of its
/// own, so it stays out of the map entirely and takes the view's shard columns.
#[test]
fn a_source_with_no_reindex_map_is_absent_from_the_routing_map() {
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
    let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;
    assert_eq!(keys.get(&10), Some(&Some(vec![(2, 0)])));
    assert!(
        !keys.contains_key(&20),
        "a keyless source must not be in the routing map"
    );
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
            CircuitFacts::derive(&loaded, &ExtTables::default()),
            Err(CompileError::Rejected(
                "a source's reindex maps carry no route key, so its delta cannot be scattered"
            ))
        ),
        "an orphaned reindex must fail the compile"
    );
}
