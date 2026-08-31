use super::*;
use crate::query::compiler::{scan_delta, scatter_reindex, PORT_IN, PORT_IN_A, PORT_TRACE};
use gnitz_wire::{JoinKind, OpNode, RangeRel};
use std::collections::HashMap;

/// A two-sided join over source 7 (reindexed on `key_cols`, so it carries a
/// scatter key) and source 9 (no reindex at all), with an output
/// `ExchangeShard` on column 1:
///
///   ScanDelta(7) → Map(reindex key_cols) ─┐
///   ScanDelta(9) ─────────────────────────→ Join(kind)
///                                           → ExchangeShard([1]) → IntegrateSink
///
/// One shape covers every route: source 7 is the join relay, source 9 the
/// keyless one, and source 0 the output relay.
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
    let loaded = compiler::loaded_for_test(nodes, edges);
    let facts = compiler::CircuitFacts::derive(&loaded, &compiler::ExtTables::default()).expect("fixture routes");
    ViewMeta::from_facts(facts)
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
            mode: ops::RouteMode::GroupKey,
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
            mode: ops::RouteMode::JoinPromote,
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
