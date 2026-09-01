use super::*;
use gnitz_store::schema::type_code;
use gnitz_wire::{MapKind, OpNode, ReindexRole};

#[test]
fn a_cyclic_circuit_is_rejected() {
    let nodes = HashMap::from([(0, OpNode::Filter(None)), (1, OpNode::Filter(None))]);
    assert!(matches!(
        load::topo_sorted(nodes, vec![(0, 1, 0), (1, 0, 0)]),
        Err(CompileError::Rejected("circuit graph has a cycle"))
    ));
}

// ── load_circuit against the real system tables ─────────────────────────

/// The three circuit system tables `load_circuit` reads, on one tempdir.
/// All three must be live: `load_circuit` opens a cursor over each up front and
/// fails on a null one, which would pass these assertions vacuously. Their
/// schemas differ (6/5/7 columns), so one cannot stand in for another.
struct CircuitTables {
    _tmp: tempfile::TempDir,
    nodes: Table,
    edges: Table,
    cols: Table,
}

impl CircuitTables {
    const VIEW_ID: u64 = 1;

    fn schema(cols: &[gnitz_wire::WireSysCol]) -> SchemaDescriptor {
        gnitz_store::schema::from_wire_cols(cols, gnitz_wire::CIRCUIT_FAMILY_PK)
    }

    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let open = |name: &str, cols: &[gnitz_wire::WireSysCol]| {
            // Each `Table` owns its path, so it outlives the `TempDir` binding.
            Table::new(
                &format!("{}/{name}", tmp.path().to_str().unwrap()),
                Self::schema(cols),
                0,
                RecoverySource::Rederive { resume_at: None },
            )
            .unwrap()
        };
        Self {
            nodes: open("nodes", gnitz_wire::CIRCUIT_NODES_COLS),
            edges: open("edges", gnitz_wire::CIRCUIT_EDGES_COLS),
            cols: open("cols", gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS),
            _tmp: tmp,
        }
    }

    fn fill(tab: &mut Table, cols: &[gnitz_wire::WireSysCol], f: impl FnOnce(&mut gnitz_store::storage::BatchBuilder)) {
        let mut bb = gnitz_store::storage::BatchBuilder::new(Self::schema(cols));
        f(&mut bb);
        tab.ingest_owned_batch(bb.finish()).unwrap();
    }

    fn put_nodes(&mut self, f: impl FnOnce(&mut gnitz_store::storage::BatchBuilder)) -> &mut Self {
        Self::fill(&mut self.nodes, gnitz_wire::CIRCUIT_NODES_COLS, f);
        self
    }

    fn put_edges(&mut self, f: impl FnOnce(&mut gnitz_store::storage::BatchBuilder)) -> &mut Self {
        Self::fill(&mut self.edges, gnitz_wire::CIRCUIT_EDGES_COLS, f);
        self
    }

    /// One `CircuitNodes` row under `view_id`, through the shared row codec — so
    /// a fixture cannot disagree with what the client lays down.
    fn node_row(
        bb: &mut gnitz_store::storage::BatchBuilder,
        view_id: u64,
        node_id: u64,
        opcode: u64,
        source: Option<u64>,
    ) {
        Self::node_row_with_blob(bb, view_id, node_id, opcode, source, None);
    }

    /// [`Self::node_row`] carrying an expr/param blob.
    fn node_row_with_blob(
        bb: &mut gnitz_store::storage::BatchBuilder,
        view_id: u64,
        node_id: u64,
        opcode: u64,
        source: Option<u64>,
        expr_program: Option<&[u8]>,
    ) {
        gnitz_wire::sys_rows::write_circuit_node_row(
            bb,
            &gnitz_wire::sys_rows::CircuitNodeRow {
                view_id,
                node_id,
                opcode,
                source_table: source,
                expr_program,
            },
            1,
        )
        .unwrap();
    }

    /// One `CircuitEdges` row under `view_id`: `src_node → (dst_node, PORT_IN)`.
    fn edge_row(bb: &mut gnitz_store::storage::BatchBuilder, view_id: u64, src_node: u64, dst_node: u64) {
        gnitz_wire::sys_rows::write_circuit_edge_row(
            bb,
            &gnitz_wire::sys_rows::CircuitEdgeRow {
                view_id,
                dst_node,
                dst_port: PORT_IN as u64,
                src_node,
            },
            1,
        )
        .unwrap();
    }

    fn load(&mut self) -> Result<LoadedCircuit, CompileError> {
        load_circuit(self, Self::VIEW_ID)
    }
}

/// The three circuit tables answer a load in place, the way the registry does
/// for the engine. No relation schemas — nothing under test here resolves one.
impl SchemaSource for CircuitTables {
    fn schema_of(&self, _tid: i64) -> Option<SchemaDescriptor> {
        None
    }

    fn open_sys_cursor(&self, tid: i64) -> Option<ReadCursor> {
        Some(match tid as u64 {
            gnitz_wire::CIRCUIT_NODES_TAB => self.nodes.open_cursor(),
            gnitz_wire::CIRCUIT_EDGES_TAB => self.edges.open_cursor(),
            gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB => self.cols.open_cursor(),
            _ => return None,
        })
    }
}

/// Every malformed-row shape must abort the WHOLE load and say which check
/// fired: skipping a row would leave edges dangling and silently corrupt the
/// topological order.
#[test]
fn a_malformed_row_aborts_the_load_and_names_the_check() {
    // An opcode `decode_op_node` rejects.
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 1, 9999, None));
    assert!(matches!(
        c.load(),
        Err(CompileError::Rejected("circuit node failed to decode"))
    ));

    // An edge whose dst is not a node of the circuit: honouring it would create
    // a phantom node.
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| {
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 0, gnitz_wire::OPCODE_SCAN_DELTA, Some(99));
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 1, gnitz_wire::OPCODE_INTEGRATE, None);
    })
    .put_edges(|bb| {
        // dst_node 7 — no such node.
        CircuitTables::edge_row(bb, CircuitTables::VIEW_ID, 0, 7);
    });
    assert!(matches!(
        c.load(),
        Err(CompileError::Rejected("edge endpoint is not a node of the circuit"))
    ));
}

/// The load is filtered to one view's rows by the `view_id` OPK prefix, and a
/// non-NULL expr blob that reads back empty stays present: `None` is how an
/// absent program is spelled and would turn a Filter into `WHERE TRUE`.
/// Rejecting the undecodable blob is the compile's job, not the load's.
#[test]
fn the_load_takes_one_views_rows_and_keeps_a_damaged_blob_present() {
    let mut c = CircuitTables::new();
    c.put_nodes(|bb| {
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID, 0, gnitz_wire::OPCODE_SCAN_DELTA, Some(10));
        // expr_program non-NULL, zero length.
        CircuitTables::node_row_with_blob(
            bb,
            CircuitTables::VIEW_ID,
            1,
            gnitz_wire::OPCODE_FILTER,
            None,
            Some(&[]),
        );
        // A second view's nodes, which this load must not see. An undecodable
        // opcode, so a load that ignored the prefix would fail outright.
        CircuitTables::node_row(bb, CircuitTables::VIEW_ID + 1, 2, 9999, None);
    })
    .put_edges(|bb| {
        CircuitTables::edge_row(bb, CircuitTables::VIEW_ID, 0, 1);
    });
    let loaded = c.load().expect("a damaged blob is not a load failure");
    assert_eq!(loaded.nodes.len(), 2, "only this view's nodes are loaded");
    assert!(
        matches!(loaded.nodes.get(&1), Some(OpNode::Filter(Some(b))) if b.is_empty()),
        "an empty blob must stay present, not collapse to a pass-all filter"
    );
}

/// A host holding none of the circuit tables must fail the load rather than
/// yield a silently empty circuit — a compile attempted against one is a caller
/// error the load has to report.
#[test]
fn a_load_from_unopened_system_tables_fails() {
    assert!(matches!(
        load_circuit(&ExtTables::new(), 0),
        Err(CompileError::Rejected("circuit system tables are not open"))
    ));
}

/// A reduce owns its trace-in: `PORT_TRACE` is written solely by a binary join,
/// but the circuit families carry no catalog precheck, so a forged bundle can
/// land the edge anywhere. Honouring one would hand the reduce a delta register
/// with no `Integrate` behind it and abort a worker on the null cursor. One
/// load-time check stands in for the arity test each operand read would
/// otherwise carry, so every node's edge set must be exactly its operator's
/// port set.
#[test]
fn port_set_violations_fail_at_load() {
    let load = |dst: OpNode, edges: Vec<(i32, i32, i32)>| {
        let nodes = HashMap::from([(0, scan_delta(10)), (1, scan_delta(11)), (2, dst)]);
        load::topo_sorted(nodes, edges)
    };
    let rejected = |r: Result<LoadedCircuit, CompileError>, what: &str| {
        assert!(
            matches!(
                r,
                Err(CompileError::Rejected(
                    "node's input edges do not match its operator's ports"
                ))
            ),
            "{what} must fail at load",
        );
    };
    assert!(load(OpNode::Filter(None), vec![(0, 2, PORT_IN)]).is_ok(), "control");

    rejected(
        load(OpNode::Filter(None), vec![(0, 2, PORT_IN), (1, 2, PORT_IN)]),
        "two edges into one port",
    );
    rejected(load(OpNode::Filter(None), vec![(0, 2, 2)]), "a port beyond the arity");
    rejected(
        load(
            OpNode::Reduce {
                group_cols: vec![0],
                agg: vec![(gnitz_wire::AggFunc::Count, 1)],
                global_ground: false,
                out_key: gnitz_store::schema::ReduceOutKey::PkPermutation,
            },
            vec![(0, 2, PORT_IN), (1, 2, PORT_TRACE)],
        ),
        "a forged Reduce trace edge",
    );
    rejected(
        load(OpNode::ExchangeShard { shard_cols: vec![0] }, vec![]),
        "an input-less ExchangeShard",
    );
    rejected(
        load(OpNode::Union, vec![(0, 2, PORT_IN_A)]),
        "a Union wired on one port",
    );
}

/// A range join is discriminated by its `Join(DeltaTraceRange)` node, never by
/// `has_join_shard && has_exchange`: that predicate is also true of every GROUP
/// BY view (a group reindex plus an output shard), so keying on it would divert
/// those views into the relay path and corrupt them.
#[test]
fn a_group_by_view_is_not_classified_as_a_range_join() {
    let loaded = loaded_for_test(
        HashMap::from([
            (0, scan_delta(7)),
            (1, scatter_reindex(&[1])),
            (2, OpNode::ExchangeShard { shard_cols: vec![1] }),
            (
                3,
                OpNode::Reduce {
                    group_cols: vec![1],
                    agg: vec![(gnitz_wire::AggFunc::Count, 0)],
                    global_ground: false,
                    out_key: gnitz_store::schema::ReduceOutKey::SyntheticFold,
                },
            ),
            (4, OpNode::IntegrateSink),
        ]),
        vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN), (3, 4, PORT_IN)],
    );
    assert_eq!(load::circuit_range_join_n_eq(&loaded), None);
}

// ── scatter_key_of_scan: the forward (scan → reindex Map) walk ──────────

/// The pure-range-join shape the planner emits at `n_eq == 0`: the reindex Map
/// feeds the `Join(DeltaTraceRange)` DIRECTLY as the delta term AND feeds a
/// `WorkerFilter → IntegrateTrace` toward the trace term. Its key is collected
/// once, from the flag — the fan-out is not a second contribution — and the
/// `WorkerFilter` is not a `Filter`, so the walk never steps through it.
#[test]
fn the_scatter_key_is_collected_once_however_the_reindex_map_fans_out() {
    let loaded = loaded_for_test(
        HashMap::from([
            (0, scan_delta(99)),
            (1, scatter_reindex(&[2])),
            (2, OpNode::WorkerFilter),
            (3, OpNode::IntegrateTrace),
            (
                4,
                OpNode::Join(gnitz_wire::JoinKind::DeltaTraceRange {
                    n_eq: 0,
                    rel: gnitz_wire::RangeRel::Le,
                }),
            ),
        ]),
        vec![
            (0, 1, PORT_IN),
            (1, 4, PORT_IN_A), // reindex Map → Join (delta term, DIRECT edge)
            (1, 2, PORT_IN),   // reindex Map → WorkerFilter (toward the trace)
            (2, 3, PORT_IN),
            (3, 4, PORT_TRACE),
        ],
    );
    assert_eq!(load::scatter_key_of_scan(&loaded, 0).0, vec![vec![(2, 0)]]);
}

/// A source joined on two different keys (`t ⋈ t1 ON t.a = t1.x ⋈ t2 ON t.b =
/// t2.y`) fans into two reindex Maps, and both sequences must be collected as
/// SEPARATE sequences: it is the sequence count, not the column count, that
/// decides whether one pack key can route the source at all.
#[test]
fn a_scan_fanning_into_two_reindex_maps_yields_two_sequences() {
    let loaded = loaded_for_test(
        HashMap::from([
            (0, scan_delta(42)),
            (1, scatter_reindex(&[2])),
            (2, OpNode::Filter(Some(dummy_expr_blob()))),
            (3, scatter_reindex(&[5])),
        ]),
        // The second reindex sits behind a Filter, which the walk steps through.
        vec![(0, 1, PORT_IN), (0, 2, PORT_IN), (2, 3, PORT_IN)],
    );
    assert_eq!(
        load::scatter_key_of_scan(&loaded, 0).0,
        vec![vec![(2, 0)], vec![(5, 0)]],
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
        HashMap::from([
            (0, scan_delta(42)),
            (
                1,
                OpNode::Map(MapKind::Reindex {
                    keep: vec![0],
                    reindex_cols: vec![3, 3],
                    reindex_target_tcs: vec![0, type_code::I64],
                    role: ReindexRole::ScatterKey,
                }),
            ),
        ]),
        vec![(0, 1, PORT_IN)],
    );
    assert_eq!(
        load::scatter_key_of_scan(&overlapping, 0).0,
        vec![vec![(3, 0), (3, type_code::I64)]],
        "duplicate slots and their promotion targets survive"
    );

    let siblings = loaded_for_test(
        HashMap::from([
            (0, scan_delta(7)),
            (1, OpNode::Filter(Some(dummy_expr_blob()))),
            (2, scatter_reindex(&[2])),
            (3, OpNode::Filter(Some(dummy_expr_blob()))),
            (4, scatter_reindex(&[2])),
        ]),
        vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (0, 3, PORT_IN), (3, 4, PORT_IN)],
    );
    assert_eq!(
        load::scatter_key_of_scan(&siblings, 0).0,
        vec![vec![(2, 0)]],
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
            reindex_cols: vec![0],
            reindex_target_tcs: vec![],
            role: ReindexRole::Auxiliary,
        })
    };
    let circuit = |join_reindex: OpNode, aux_rekey: OpNode| {
        loaded_for_test(
            HashMap::from([
                (0, scan_delta(10)),
                (1, join_reindex),
                (
                    2,
                    OpNode::Join(gnitz_wire::JoinKind::DeltaTraceRange {
                        n_eq: 1,
                        rel: gnitz_wire::RangeRel::Le,
                    }),
                ),
                (3, OpNode::IntegrateTrace),
                (4, aux_rekey),
                (5, OpNode::Map(MapKind::Projection(vec![]))),
                (6, OpNode::Distinct),
            ]),
            vec![
                (0, 1, PORT_IN),
                (1, 2, PORT_IN_A),
                (1, 3, PORT_IN), // join reindex → its own integral
                (3, 2, PORT_TRACE),
                (0, 4, PORT_IN),
                (4, 5, PORT_IN),
                (5, 6, PORT_IN), // aux a.pk re-key → proj → distinct
            ],
        )
    };
    assert_eq!(
        load::scatter_key_of_scan(&circuit(scatter_reindex(&[1, 2]), aux_rekey()), 0),
        (vec![vec![(1, 0), (2, 0)]], false),
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
        HashMap::from([
            (0, scan_delta(7)),
            (
                1,
                OpNode::Map(MapKind::Compute {
                    program: dummy_expr_blob(),
                    out_cols: vec![],
                }),
            ),
        ]),
        vec![(0, 1, PORT_IN)],
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
        let edges = (0..shard).map(|i| (i, i + 1, PORT_IN)).collect();
        scan_tid_through_filters(&loaded_for_test(nodes, edges), shard)
    };
    let filter = || OpNode::Filter(Some(dummy_expr_blob()));
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
        let mut edges = vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B)];
        edges.extend((2..shard).map(|i| (i, i + 1, PORT_IN)));
        scan_tid_through_filters(&loaded_for_test(nodes, edges), shard)
    };
    assert_eq!(union_then(vec![]), None, "the Union feeds the shard directly");
    assert_eq!(
        union_then(vec![OpNode::Filter(Some(dummy_expr_blob()))]),
        None,
        "a Filter is transparent, so the walk reaches the Union and bails there"
    );
}
