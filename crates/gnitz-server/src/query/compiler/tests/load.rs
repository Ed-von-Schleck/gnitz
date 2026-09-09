use super::*;
use gnitz_store::relation::{OnRegister, RelationKind, RelationSpec, StoreConfig, ViewBudgets};
use gnitz_store::storage::Slot;
use gnitz_wire::{MapKind, OpNode, ReindexRole};
use std::collections::HashMap;

#[test]
fn a_cyclic_circuit_is_rejected() {
    let nodes = HashMap::from([(0, OpNode::Negate), (1, OpNode::Negate)]);
    assert!(matches!(
        load::topo_sorted(
            nodes.into_iter().collect(),
            wire_slots(&[(0, 1, SLOT_IN), (1, 0, SLOT_IN)])
        ),
        Err(CompileError::Rejected("circuit graph has a cycle"))
    ));
}

// ── load_circuit against the real system tables ─────────────────────────

/// The `CircuitNodes` system table `load_circuit` reads, on one tempdir. It must
/// be live: `load_circuit` opens a cursor over it up front and fails on a null
/// one, which would pass these assertions vacuously.
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
                    budgets: ViewBudgets::default(),
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
        let (opcode, source_table, params) = gnitz_wire::encode_op_node(op);
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

    /// Loaded straight off the registry: it is the `SchemaSource` the engine
    /// loads through, so the fixture exercises the same lookup production does.
    fn load(&mut self) -> Result<LoadedCircuit, CompileError> {
        load_circuit(&self.registry, Self::VIEW_ID)
    }
}

/// Every malformed-row shape must abort the WHOLE load and say which check
/// fired: skipping a row would leave an input dangling and silently corrupt the
/// topological order.
#[test]
fn a_malformed_row_aborts_the_load_and_names_the_check() {
    // An opcode `decode_op_node` rejects. Its own reason is what surfaces —
    // interpolated value and all — not a bare "failed to decode".
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| CircuitTables::raw_row(bb, CircuitTables::VIEW_ID, 1, 9999, None, None, None));
    assert!(
        matches!(c.load(), Err(CompileError::RejectedNode(why)) if why == "unknown opcode 9999"),
        "the decoder's own rejection must reach the caller",
    );

    // A node naming a producer that is not a node of the circuit: honouring it
    // would create a phantom node.
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| {
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 0, scan_delta(99), None);
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 1, OpNode::IntegrateSink, Some(7));
    });
    assert!(matches!(
        c.load(),
        Err(CompileError::Rejected("a node's input is not a node of the circuit"))
    ));
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
    assert_eq!(loaded.nodes.len(), 2, "only this view's nodes are loaded");
    assert!(
        matches!(loaded.nodes.get(&1), Some(OpNode::Filter(b)) if b == &[0xff]),
        "an undecodable program must stay present, not collapse to a pass-all filter"
    );
}

/// A host holding no circuit table must fail the load rather than
/// yield a silently empty circuit — a compile attempted against one is a caller
/// error the load has to report.
#[test]
fn a_load_from_unopened_system_tables_fails() {
    assert!(matches!(
        load_circuit(&ext_tables([]), 0),
        Err(CompileError::Rejected("the circuit system table is not open"))
    ));
}

/// Slot 1 is filled solely by a binary join, but the circuit family carries no
/// catalog precheck, so a forged bundle can fill it anywhere — handing a reduce a
/// delta register with no `Integrate` behind it, and aborting a worker on the
/// null cursor. This one load-time check stands in for the arity test each
/// operand read would otherwise carry.
#[test]
fn arity_violations_fail_at_load() {
    let load = |dst: OpNode, edges: &[(i32, i32, usize)]| {
        let nodes = HashMap::from([(0, scan_delta(10)), (1, scan_delta(11)), (2, dst)]);
        load::topo_sorted(nodes.into_iter().collect(), wire_slots(edges))
    };
    let rejected = |r: Result<LoadedCircuit, CompileError>, what: &str| {
        assert!(
            matches!(
                r,
                Err(CompileError::Rejected(
                    "node's inputs do not match its operator's arity"
                ))
            ),
            "{what} must fail at load",
        );
    };
    assert!(load(OpNode::Negate, &[(0, 2, SLOT_IN)]).is_ok(), "control");

    rejected(
        load(
            OpNode::Reduce {
                group_cols: vec![0],
                agg: vec![gnitz_wire::AggDescriptor {
                    agg_op: gnitz_wire::AggFunc::Count,
                    col_idx: 1,
                }],
                global_ground: false,
            },
            &[(0, 2, SLOT_IN), (1, 2, SLOT_TRACE)],
        ),
        "a forged Reduce trace input",
    );
    rejected(
        load(OpNode::ExchangeShard { shard_cols: vec![0] }, &[]),
        "an input-less ExchangeShard",
    );
    rejected(load(OpNode::Union, &[(0, 2, SLOT_IN)]), "a Union wired on one slot");
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
    assert_eq!(load::circuit_join_relay(&loaded), load::JoinRelay::WholeKey);

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
        (gnitz_wire::JoinKind::Equi, load::JoinRelay::WholeKey),
        (range(2), load::JoinRelay::EqPrefix { n_eq: 2 }),
        (range(0), load::JoinRelay::Broadcast),
        (gnitz_wire::JoinKind::Cross, load::JoinRelay::Broadcast),
    ] {
        assert_eq!(load::circuit_join_relay(&joined(kind)), want, "{kind:?}");
    }
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
    assert_eq!(load::scatter_key_of_scan(&loaded, 0).0, vec![vec![(2, None)]]);
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
        load::scatter_key_of_scan(&loaded, 0).0,
        vec![vec![(2, None)], vec![(5, None)]],
        "two distinct keys stay two sequences"
    );
}

/// Within one sequence, duplicate columns are PRESERVED: an overlapping key
/// (`a.x = b.p AND a.x = b.q`) reindexes `[x, x]`, possibly with distinct
/// per-slot promotion targets, and the scatter packer must mirror the trace-side
/// `ReindexPacker` slot-for-slot. Across sibling Maps carrying an IDENTICAL
/// sequence (the not-null / null-key branches of a nullable LEFT-join key) it
/// collapses to one, which concatenating would double.
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
        load::scatter_key_of_scan(&overlapping, 0).0,
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
        load::scatter_key_of_scan(&siblings, 0).0,
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
                (
                    2,
                    OpNode::Join(gnitz_wire::JoinKind::Range { n_eq: 1, rel: gnitz_wire::RangeRel::Le }),
                ),
                (3, OpNode::IntegrateTrace),
                (4, aux_rekey),
                (5, OpNode::Map(MapKind::Projection(vec![]))),
                (6, OpNode::Distinct),
            ],
            vec![
                (0, 1, SLOT_IN),
                (1, 2, SLOT_IN),
                (1, 3, SLOT_IN), // join reindex → its own integral
                (3, 2, SLOT_TRACE),
                (0, 4, SLOT_IN),
                (4, 5, SLOT_IN),
                (5, 6, SLOT_IN), // aux a.pk re-key → proj → distinct
            ],
        )
    };
    assert_eq!(
        load::scatter_key_of_scan(&circuit(scatter_reindex(&[1, 2]), aux_rekey()), 0),
        (vec![vec![(1, None), (2, None)]], false),
        "only the trace/probe-feeding reindex defines the scatter key"
    );
    assert_eq!(
        load::scatter_key_of_scan(&circuit(aux_rekey(), aux_rekey()), 0),
        (vec![], true),
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
    assert!(load::scatter_key_of_scan(&loaded, 0).0.is_empty());
}

// ── scan_tid_through_filters: the backward (shard → scan) walk ──────────
//
// The view exchange-skip detector's source resolution. Exchanging is also
// correct, so the skip has no observable "fired" signal at the E2E layer; these
// are what pin that the walk engages exactly when it should.

/// `Filter` is the only operator transparent to the shard key: it is
/// row-selective, never re-keys the PK region and never moves a row off-worker.
/// A re-keying `Map` rewrites the PK, and a `WorkerFilter` is a different
/// variant altogether — neither is crossed, and a `Filter` below either does not
/// rescue it.
#[test]
fn the_shard_walk_crosses_filters_and_nothing_else() {
    let chain = |mids: Vec<OpNode>| {
        let shard = mids.len() as i32 + 1;
        let mut nodes = HashMap::from([
            (0, scan_delta(7)),
            (shard, OpNode::ExchangeShard { shard_cols: vec![0] }),
        ]);
        for (i, op) in mids.into_iter().enumerate() {
            nodes.insert(i as i32 + 1, op);
        }
        let edges = (0..shard).map(|i| (i, i + 1, SLOT_IN)).collect();
        scan_tid_through_filters(&loaded_for_test(nodes, edges), shard)
    };
    let filter = || OpNode::Filter(dummy_expr_blob());
    let rekey = || scatter_reindex(&[2]);
    assert_eq!(chain(vec![]), Some(7), "a bare scan resolves on the first hop");
    assert_eq!(chain(vec![filter()]), Some(7));
    assert_eq!(
        chain(vec![filter(), filter()]),
        Some(7),
        "a Filter chain is transparent"
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
        let shard = tail.len() as i32 + 3;
        let mut nodes = HashMap::from([
            (0, scan_delta(7)),
            (1, scan_delta(8)),
            (2, OpNode::Union),
            (shard, OpNode::ExchangeShard { shard_cols: vec![0] }),
        ]);
        for (i, op) in tail.into_iter().enumerate() {
            nodes.insert(i as i32 + 3, op);
        }
        let mut edges = vec![(0, 2, SLOT_IN), (1, 2, SLOT_TRACE)];
        edges.extend((2..shard).map(|i| (i, i + 1, SLOT_IN)));
        scan_tid_through_filters(&loaded_for_test(nodes, edges), shard)
    };
    assert_eq!(union_then(vec![]), None, "the Union feeds the shard directly");
    assert_eq!(
        union_then(vec![OpNode::Filter(dummy_expr_blob())]),
        None,
        "a Filter is transparent, so the walk reaches the Union and bails there"
    );
}

/// The node cap is what keeps every downstream `u16` id — registers, tables —
/// in range without any per-plan arithmetic, so it is enforced in the one
/// constructor every plan passes through rather than at each plan build.
#[test]
fn a_circuit_over_the_node_limit_is_rejected() {
    let n = MAX_CIRCUIT_NODES as i32 + 1;
    let mut nodes = HashMap::from([(0, scan_delta(10))]);
    let mut edges = Vec::new();
    for nid in 1..n {
        nodes.insert(nid, OpNode::Negate);
        edges.push((nid - 1, nid, SLOT_IN));
    }
    assert_eq!(
        load::topo_sorted(nodes.clone().into_iter().collect(), wire_slots(&edges))
            .map(|_| "a circuit")
            .expect_err("over the limit")
            .to_string(),
        "circuit exceeds the node limit"
    );
    nodes.remove(&(n - 1));
    edges.pop();
    assert!(
        load::topo_sorted(nodes.into_iter().collect(), wire_slots(&edges)).is_ok(),
        "exactly MAX_CIRCUIT_NODES is accepted"
    );
}
