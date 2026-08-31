use super::*;
use crate::schema::{type_code, SchemaColumn};

/// 3-column compound PK `(U32, U64, U64)` + one payload, so the columns have
/// distinct widths and a prefix stride is unambiguous.
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

#[test]
fn shard_cols_match_dist_key_is_exact_prefix() {
    let k1 = three_col_pk_schema(1); // CLUSTER BY col0
                                     // Exact prefix at k=1 matches; the full PK and a super-prefix do not.
    assert!(shard_cols_match_dist_key(&k1, &[0]));
    assert!(!shard_cols_match_dist_key(&k1, &[0, 1]), "super-prefix must NOT match");
    assert!(!shard_cols_match_dist_key(&k1, &[0, 1, 2]));
    assert!(!shard_cols_match_dist_key(&k1, &[1]), "non-leading column");
    assert!(!shard_cols_match_dist_key(&k1, &[]));

    // Default (full-PK) schema: dist key is the whole PK, exactly.
    let full = three_col_pk_schema(0);
    assert!(shard_cols_match_dist_key(&full, &[0, 1, 2]));
    assert!(
        !shard_cols_match_dist_key(&full, &[0]),
        "a single component is not the full key"
    );
    assert!(!shard_cols_match_dist_key(&full, &[0, 1]));

    // k=2 matches exactly [0,1], not [0] and not [0,1,2].
    let k2 = three_col_pk_schema(2);
    assert!(shard_cols_match_dist_key(&k2, &[0, 1]));
    assert!(!shard_cols_match_dist_key(&k2, &[0]));
    assert!(!shard_cols_match_dist_key(&k2, &[0, 1, 2]));
}

/// A relation whose rows are not placed by `worker_for_pk` has no shard
/// key that names where they already are — whatever its PK columns look
/// like. This is what stops a co-partition/exchange elision from firing onto
/// an unkeyed source.
#[test]
fn shard_cols_never_match_an_unkeyed_placement() {
    for p in [Placement::Replicated, Placement::Local] {
        let s = three_col_placed(p);
        assert!(!shard_cols_match_dist_key(&s, &[0, 1, 2]), "{p:?}: full PK");
        assert!(!shard_cols_match_dist_key(&s, &[0]), "{p:?}: leading column");
        assert!(!shard_cols_match_dist_key(&s, &[]), "{p:?}: empty key");
    }
}

#[test]
fn test_compute_co_partitioned_strict_full_pk_sequence() {
    // Compound PK (a, b) at columns 0, 1; column 2 is payload.
    let compound = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    let ext: ExtTables = HashMap::from([(7, compound)]);
    let co = |cols: Vec<(u32, u8)>| {
        let mut m = HashMap::new();
        m.insert(7i64, cols);
        compute_co_partitioned(&m, &ext).contains(&7)
    };
    // Only the exact PK sequence in schema order co-partitions.
    assert!(
        co(vec![(0, 0), (1, 0)]),
        "shard [pk0, pk1] equals pk_indices() → co-partitioned"
    );
    assert!(!co(vec![(0, 0)]), "shard [pk0] alone is not the full PK");
    assert!(!co(vec![(1, 0)]), "shard [pk1] alone is not the full PK");
    assert!(!co(vec![(1, 0), (0, 0)]), "permuted [pk1, pk0] != pk_indices() order");
    // A promoted key (non-zero carried tc) never co-partitions: native PK
    // partitions are at the source width, not the T-wide trace key.
    assert!(
        !co(vec![(0, type_code::I64), (1, 0)]),
        "a promoted PK slot must go through the exchange"
    );

    // Single-PK source: [pk] stays co-partitioned (no regression).
    let single = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let ext1: ExtTables = HashMap::from([(9, single)]);
    let mut m = HashMap::new();
    m.insert(9i64, vec![(0, 0)]);
    assert!(
        compute_co_partitioned(&m, &ext1).contains(&9),
        "single-PK shard [pk] stays co-partitioned"
    );
}

#[test]
fn test_compute_co_partitioned_replicated() {
    // Two single-PK (U64) join sides; the join key is a NON-PK payload column
    // (col 1), so neither side's shard key matches its distribution prefix —
    // the only reason to skip the exchange is replication.
    const COLS: [SchemaColumn; 2] = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
    ];
    let base = || SchemaDescriptor::new(&COLS, &[0]);
    let replicated = SchemaDescriptor::new_with_placement(&COLS, &[0], crate::schema::Placement::Replicated);
    let join_on_payload = || {
        let mut m = HashMap::new();
        m.insert(7i64, vec![(1u32, 0u8)]); // dim  shards on payload col 1
        m.insert(8i64, vec![(1u32, 0u8)]); // fact shards on payload col 1
        m
    };

    // partitioned ⋈ partitioned on a non-PK key: neither side skips.
    let ext_pp: ExtTables = HashMap::from([(7, base()), (8, base())]);
    let co = compute_co_partitioned(&join_on_payload(), &ext_pp);
    assert!(
        !co.contains(&7) && !co.contains(&8),
        "two partitioned sides on a non-PK key both go through the exchange"
    );

    // partitioned fact ⋈ REPLICATED dim: BOTH skip — the dim because it is
    // replicated, the fact because its join partner is replicated (it stays in
    // its own PK partitioning and joins the full local dim copy).
    let ext_pr: ExtTables = HashMap::from([(7, replicated), (8, base())]);
    let co = compute_co_partitioned(&join_on_payload(), &ext_pr);
    assert!(co.contains(&7), "a replicated source always skips its exchange");
    assert!(
        co.contains(&8),
        "a partitioned fact skips when its partner is replicated"
    );

    // replicated ⋈ replicated: both skip (output is replicated; single-sourced on read).
    let ext_rr: ExtTables = HashMap::from([(7, replicated), (8, replicated)]);
    let co = compute_co_partitioned(&join_on_payload(), &ext_rr);
    assert!(
        co.contains(&7) && co.contains(&8),
        "replicated ⋈ replicated: both sides skip"
    );

    // A replicated source skips even with a promoted (non-zero tc) key: the
    // write broadcast already placed its full trace on every worker, so the
    // tc-promotion exchange gate (which blocks a partitioned source) does not apply.
    let ext_r: ExtTables = HashMap::from([(7, replicated)]);
    let mut promoted = HashMap::new();
    promoted.insert(7i64, vec![(0u32, type_code::I64)]);
    assert!(
        compute_co_partitioned(&promoted, &ext_r).contains(&7),
        "replicated source skips regardless of carried type-promotion"
    );
}

// ── compute_scatter_routing covers ScanDelta (SQL-planner join pattern) ──

/// The routing walk must find ScanDelta → Map(reindex) chains.
#[test]
fn test_scatter_routing_scan_delta() {
    use gnitz_wire::{MapKind, OpNode};

    // Minimal two-sided SQL join circuit skeleton:
    //   ScanDelta(left_tid=10) → Map(reindex_col=1) → Join → IntegrateSink
    //   ScanDelta(right_tid=20) → Map(reindex_col=0) → Join
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(
        1,
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            reindex_cols: vec![1],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        }),
    );
    nodes.insert(2, scan_delta(20));
    nodes.insert(
        3,
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            reindex_cols: vec![0],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        }),
    );
    nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
    nodes.insert(5, OpNode::IntegrateSink);
    let edges = vec![
        (0, 1, PORT_IN),
        (2, 3, PORT_IN),
        (1, 4, PORT_IN_A),
        (3, 4, PORT_TRACE),
        (4, 5, PORT_IN),
    ];
    let loaded = loaded_for_test(nodes, edges);

    let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;

    assert_eq!(
        keys.get(&10),
        Some(&Some(vec![(1, 0)])),
        "left side (source 10) must map to reindex_col=1"
    );
    assert_eq!(
        keys.get(&20),
        Some(&Some(vec![(0, 0)])),
        "right side (source 20) must map to reindex_col=0"
    );
}

#[test]
fn test_scatter_routing_through_filter() {
    use gnitz_wire::{MapKind, OpNode};
    // ScanDelta(42) → Filter → Map(reindex_col=1) → Join → IntegrateSink.
    // The reindex Map is two hops from the scan (a Filter sits between),
    // so the one-hop lookup misses it; BFS through Filter must find it.
    let dummy_blob = dummy_expr_blob();
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(42));
    nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
    nodes.insert(
        2,
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            reindex_cols: vec![1],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        }),
    );
    nodes.insert(3, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
    nodes.insert(4, OpNode::IntegrateSink);
    nodes.insert(5, OpNode::IntegrateTrace);
    let edges = vec![
        (0, 1, PORT_IN), // ScanDelta → Filter
        (1, 2, PORT_IN), // Filter → reindex Map
        (2, 3, PORT_IN_A),
        (2, 5, PORT_IN), // reindex Map → its own integral
        (5, 3, PORT_TRACE),
        (3, 4, PORT_IN),
    ];
    let loaded = loaded_for_test(nodes, edges);

    let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;
    assert_eq!(
        keys.get(&42),
        Some(&Some(vec![(1, 0)])),
        "ScanDelta → Filter → Map(reindex) must map source 42 to col 1"
    );
}

/// Two `ScanDelta` nodes on ONE source with DIFFERENT scatter keys. Under the
/// old `loaded.nodes` walk, last-writer-wins over a per-process `RandomState`
/// order picked one of the two at random — and the master and the worker
/// derive their halves of the routing in separate processes. Accumulating
/// over `loaded.ordered` makes the answer the same everywhere: two sequences,
/// so no single pack key routes the source and the relay refuses.
#[test]
fn test_scatter_routing_two_keys_one_source_is_deterministic() {
    use gnitz_wire::{MapKind, OpNode};

    let reindex = |col: u32| {
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            reindex_cols: vec![col],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        })
    };
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, reindex(1));
    nodes.insert(2, scan_delta(10));
    nodes.insert(3, reindex(2));
    nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
    nodes.insert(5, OpNode::IntegrateSink);
    let edges = vec![
        (0, 1, PORT_IN),
        (2, 3, PORT_IN),
        (1, 4, PORT_IN_A),
        (3, 4, PORT_TRACE),
        (4, 5, PORT_IN),
    ];
    let loaded = loaded_for_test(nodes, edges);

    let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;
    assert_eq!(
        keys.get(&10),
        Some(&None),
        "two distinct keys on one source must refuse, not pick one at random"
    );
}

/// The same shape with the SAME key on both scans must still route. Today's
/// last-writer-wins resolves it to the one correct sequence, so a naive
/// accumulation that skipped the cross-node dedup would newly produce two
/// sequences and refuse a round that works — a regression the rewrite must
/// not introduce.
#[test]
fn test_scatter_routing_repeated_key_one_source_still_routes() {
    use gnitz_wire::{MapKind, OpNode};

    let reindex = || {
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            reindex_cols: vec![1],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        })
    };
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, reindex());
    nodes.insert(2, scan_delta(10));
    nodes.insert(3, reindex());
    nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
    nodes.insert(5, OpNode::IntegrateSink);
    let edges = vec![
        (0, 1, PORT_IN),
        (2, 3, PORT_IN),
        (1, 4, PORT_IN_A),
        (3, 4, PORT_TRACE),
        (4, 5, PORT_IN),
    ];
    let loaded = loaded_for_test(nodes, edges);

    let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;
    assert_eq!(
        keys.get(&10),
        Some(&Some(vec![(1, 0)])),
        "one key reached twice is still one key"
    );
}

/// A source whose path to the join carries no reindex Map stays out of the map.
#[test]
fn test_scatter_routing_unreindexed_trace_side_absent() {
    use gnitz_wire::{MapKind, OpNode};

    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, scan_delta(20));
    nodes.insert(
        2,
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            reindex_cols: vec![2],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        }),
    );
    nodes.insert(3, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
    nodes.insert(4, OpNode::IntegrateSink);
    nodes.insert(5, OpNode::IntegrateTrace);
    let edges = vec![
        (0, 2, PORT_IN),    // ScanDelta → reindex Map
        (1, 5, PORT_IN),    // ScanDelta(20) → IntegrateTrace (no reindex)
        (5, 3, PORT_TRACE), // trace → join trace port
        (2, 3, PORT_IN_A),
        (3, 4, PORT_IN),
    ];
    let loaded = loaded_for_test(nodes, edges);

    let map = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;

    // ScanDelta(10) → Map(reindex_col=2) must be found.
    assert_eq!(
        map.get(&10),
        Some(&Some(vec![(2, 0)])),
        "ScanDelta source must be in the routing map"
    );
    // Source 20 has no downstream reindex Map — must NOT appear.
    assert!(
        !map.contains_key(&20),
        "a source with no reindex Map must not be in the routing map"
    );
}
