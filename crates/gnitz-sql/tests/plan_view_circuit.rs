//! The circuit shape a view body compiles to, read straight off `plan_view`.
//!
//! Each row pins only what CLAUDE.md §3 and the placement contracts require:
//! segment count, where the exchanges sit and what they shard on, the two join
//! terms of the symmetric bilinear form, the reduce / clamp / null-fill /
//! worker-filter / filter node counts. Projection lists, map counts, union
//! counts and node numbering are free to change.

use gnitz_core::{CatalogSnapshot, OpNode, TypeCode};
use gnitz_sql::PlannedChain;
use gnitz_wire::{JoinKind, MapKind};

mod pure;
use pure::*;

/// One row of the shape matrix: body, segment count, the exchanges (shard cols
/// per `ExchangeShard`, any order), and the node kinds the body must compile to,
/// with their counts. A kind a row omits must not appear at all, so a new kind
/// touches [`Node`] and only the rows that carry it.
type Row = (&'static str, usize, &'static [&'static [u32]], &'static [(Node, usize)]);

/// A node kind the shape matrix counts — the CLAUDE.md §3 and placement
/// contracts. Projections, maps, unions and node numbering are not pinned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Node {
    EquiJoin,
    RangeJoin,
    CrossJoin,
    Reduce,
    GlobalGround,
    Distinct,
    PositivePart,
    WorkerFilter,
    NullExtend,
    Filter,
}
use Node::*;

impl Node {
    const ALL: &'static [Node] = &[
        EquiJoin,
        RangeJoin,
        CrossJoin,
        Reduce,
        GlobalGround,
        Distinct,
        PositivePart,
        WorkerFilter,
        NullExtend,
        Filter,
    ];

    fn matches(self, op: &OpNode) -> bool {
        match self {
            EquiJoin => matches!(op, OpNode::Join(JoinKind::Equi)),
            RangeJoin => matches!(op, OpNode::Join(JoinKind::Range { .. })),
            CrossJoin => matches!(op, OpNode::Join(JoinKind::Cross)),
            Reduce => matches!(op, OpNode::Reduce { .. }),
            GlobalGround => matches!(op, OpNode::Reduce { global_ground: true, .. }),
            Distinct => matches!(op, OpNode::Distinct),
            PositivePart => matches!(op, OpNode::PositivePart),
            WorkerFilter => matches!(op, OpNode::WorkerFilter),
            NullExtend => matches!(op, OpNode::NullExtend { .. }),
            Filter => matches!(op, OpNode::Filter(_)),
        }
    }
}

/// [`pure::base`] plus `m`, a second table with nullable join keys.
fn cat() -> CatalogSnapshot {
    let mut cat = base();
    let i = TypeCode::I64;
    cat.insert(
        SN,
        "m",
        Some(table(30, vec![col("id", i), ncol("k", i), ncol("v", i)], vec![0])),
    );
    cat
}

/// `pred` over every node of every segment.
fn total(chain: &PlannedChain, pred: impl Fn(&OpNode) -> bool) -> usize {
    chain.views.iter().map(|pv| count(&pv.circuit, &pred)).sum()
}

fn exchanges(chain: &PlannedChain) -> Vec<Vec<u32>> {
    let mut out: Vec<Vec<u32>> = chain
        .views
        .iter()
        .flat_map(|pv| pv.circuit.nodes().iter().map(|n| &n.op))
        .filter_map(|op| match op {
            OpNode::ExchangeShard { shard_cols } => Some(shard_cols.clone()),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

#[rustfmt::skip]
const SHAPES: &[Row] = &[
    // Linear: no exchange, one filter per WHERE.
    ("SELECT id, v FROM t", 1, &[], &[]),
    ("SELECT id, v * 2 AS d FROM t WHERE v > 5", 1, &[], &[(Filter, 1)]),
    // Equi join: two join terms, no exchange; the null-fill is a clamp and a
    // null-extend per preserved side, never a distinct.
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k", 1, &[], &[(EquiJoin, 2)]),
    ("SELECT a.id AS aid, b.w FROM a LEFT JOIN b ON a.k = b.k", 1, &[], &[(EquiJoin, 2), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.w FROM a RIGHT JOIN b ON a.k = b.k", 1, &[], &[(EquiJoin, 2), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.w FROM a FULL JOIN b ON a.k = b.k", 1, &[], &[(EquiJoin, 2), (PositivePart, 2), (NullExtend, 2)]),
    // A residual or WHERE is one filter; a nullable key is one filter per side.
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k AND a.v <> b.w", 1, &[], &[(EquiJoin, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k AND (a.v > 5 OR b.w < 3)", 1, &[], &[(EquiJoin, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k WHERE a.v > 5", 1, &[], &[(EquiJoin, 2), (Filter, 1)]),
    ("SELECT n.id AS nid, b.w FROM n JOIN b ON n.k = b.k", 1, &[], &[(EquiJoin, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, n.v FROM a JOIN n ON a.k = n.k", 1, &[], &[(EquiJoin, 2), (Filter, 1)]),
    ("SELECT n.id AS nid, b.w FROM n LEFT JOIN b ON n.k = b.k", 1, &[], &[(EquiJoin, 2), (PositivePart, 1), (NullExtend, 1), (Filter, 1)]),
    ("SELECT n.id AS nid, m.v FROM n LEFT JOIN m ON n.k = m.k", 1, &[], &[(EquiJoin, 2), (PositivePart, 1), (NullExtend, 1), (Filter, 2)]),
    // An ON conjunct over the null-supplying side filters that input.
    ("SELECT a.id AS aid, b.w FROM a LEFT JOIN b ON a.k = b.k AND b.w > 3", 1, &[], &[(EquiJoin, 2), (PositivePart, 1), (NullExtend, 1), (Filter, 1)]),
    // A filtered derived-table input fuses into the join, cutting no segment.
    ("SELECT a.id AS aid, d.w FROM a JOIN (SELECT k, w FROM b WHERE w > 3) d ON a.k = d.k", 1, &[], &[(EquiJoin, 2), (Filter, 1)]),
    // Band join: two range terms, one pair-PK output exchange, no worker filter.
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[&[0, 1]], &[(RangeJoin, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[&[0, 1]], &[(RangeJoin, 2), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a RIGHT JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[&[0, 1]], &[(RangeJoin, 2), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[&[0, 1]], &[(RangeJoin, 2), (PositivePart, 2), (NullExtend, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w WHERE a.id > 5", 1, &[&[0, 1]], &[(RangeJoin, 2), (PositivePart, 1), (NullExtend, 1), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v < b.w AND a.id > b.id", 1, &[&[0, 1]], &[(RangeJoin, 2), (Filter, 1)]),
    // Pure range: broadcast trimmed by one worker filter per side; LEFT derives
    // its null-fill from a threshold reduce, so no clamp.
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w", 1, &[&[0, 1]], &[(RangeJoin, 2), (WorkerFilter, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.v < b.w", 1, &[&[0, 1]], &[(RangeJoin, 4), (Reduce, 1), (WorkerFilter, 2), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w AND a.id <> b.id", 1, &[&[0, 1]], &[(RangeJoin, 2), (WorkerFilter, 2), (Filter, 1)]),
    // Keyless (cross) join: two cross terms, one worker filter per side, the
    // pair-PK output shard; a residual — a constant `ON 1 = 1` included — or a
    // WHERE is one filter. A self product wraps the second copy in a segment,
    // and a third relation is one more keyless step over the cut product.
    ("SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b", 1, &[&[0, 1]], &[(CrossJoin, 2), (WorkerFilter, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a, b", 1, &[&[0, 1]], &[(CrossJoin, 2), (WorkerFilter, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON 1 = 1", 1, &[&[0, 1]], &[(CrossJoin, 2), (WorkerFilter, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v <> b.w", 1, &[&[0, 1]], &[(CrossJoin, 2), (WorkerFilter, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a, b WHERE a.v <> b.w", 1, &[&[0, 1]], &[(CrossJoin, 2), (WorkerFilter, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b WHERE a.v > 3", 1, &[&[0, 1]], &[(CrossJoin, 2), (WorkerFilter, 2), (Filter, 1)]),
    ("SELECT c.v AS cv, b.id AS bid FROM c CROSS JOIN b", 1, &[&[0, 1, 2]], &[(CrossJoin, 2), (WorkerFilter, 2)]),
    ("SELECT c.v AS cv, ty.id AS tid FROM c NATURAL JOIN ty", 1, &[&[0, 1, 2]], &[(CrossJoin, 2), (WorkerFilter, 2)]),
    ("SELECT x.id AS xa, y.id AS yb FROM a x, a y", 2, &[&[0, 1]], &[(CrossJoin, 2), (WorkerFilter, 2)]),
    ("SELECT a.id AS aid, b.id AS bid, u.id AS uid FROM a, b, u", 2, &[&[0, 1], &[0, 1, 2]], &[(CrossJoin, 4), (WorkerFilter, 4)]),
    // Semi / anti / mark / IN: a join plus clamp arithmetic, no exchange, no distinct.
    ("SELECT a.v FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)", 1, &[], &[(EquiJoin, 2), (PositivePart, 1)]),
    ("SELECT a.v FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)", 1, &[], &[(EquiJoin, 2), (PositivePart, 1)]),
    ("SELECT id FROM a WHERE k IN (SELECT k FROM b)", 1, &[], &[(EquiJoin, 2), (PositivePart, 1)]),
    ("SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AS flag FROM a", 1, &[], &[(EquiJoin, 2), (PositivePart, 1)]),
    // Two marks: the lower one is cut, carrying its mark to the upper one, whose
    // two branches each filter on the WHERE reading both.
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR EXISTS (SELECT 1 FROM b WHERE b.w = a.v)", 2, &[], &[(EquiJoin, 4), (PositivePart, 2), (Filter, 2)]),
    ("SELECT id FROM n WHERE EXISTS (SELECT 1 FROM m WHERE m.k = n.k)", 1, &[], &[(EquiJoin, 2), (PositivePart, 1), (Filter, 2)]),
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w < a.v)", 1, &[&[0]], &[(RangeJoin, 2), (PositivePart, 1)]),
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", 1, &[&[0]], &[(RangeJoin, 2), (Reduce, 1), (WorkerFilter, 1)]),
    ("SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", 1, &[&[0]], &[(RangeJoin, 2), (Reduce, 1), (WorkerFilter, 1)]),
    // Reduce: grouped shards on the group columns; a global aggregate funnels
    // through an empty-key exchange with a ground row, two-phase when linear;
    // a replicated source needs no exchange at all.
    ("SELECT g, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY g", 1, &[&[1]], &[(Reduce, 1)]),
    ("SELECT a AS ka, b AS kb, COUNT(*) AS n, SUM(v) AS s FROM c GROUP BY a, b", 1, &[&[0, 1]], &[(Reduce, 1)]),
    ("SELECT g, SUM(v) AS s FROM t GROUP BY g HAVING SUM(v) > 10", 1, &[&[1]], &[(Reduce, 1), (Filter, 1)]),
    ("SELECT g + v AS k, SUM(g * v) AS s FROM t GROUP BY g + v", 1, &[&[1]], &[(Reduce, 1)]),
    ("SELECT SUM(v) AS s, MIN(v) AS mn, MAX(v) AS mx FROM t", 1, &[&[]], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT SUM(v) AS s, COUNT(*) AS c FROM t", 1, &[&[]], &[(Reduce, 2), (GlobalGround, 1)]),
    ("SELECT AVG(i32c) AS s FROM ty", 1, &[&[]], &[(Reduce, 2), (GlobalGround, 1)]),
    ("SELECT COUNT(k) AS c FROM n", 1, &[&[]], &[(Reduce, 2), (GlobalGround, 1)]),
    // A float SUM, and an AVG over one, would reassociate by worker count: funnel.
    ("SELECT SUM(f) AS s FROM ty", 1, &[&[]], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT AVG(f) AS s FROM ty", 1, &[&[]], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT v, COUNT(*) AS n FROM r GROUP BY v", 1, &[], &[(Reduce, 1)]),
    // DISTINCT and set operations: content-hashed leaves behind an exchange on
    // the hash key, then weight clamps; never a join.
    ("SELECT DISTINCT g FROM t", 1, &[&[0]], &[(Distinct, 1)]),
    ("SELECT g FROM t UNION SELECT g FROM u", 1, &[&[0], &[0]], &[(Distinct, 1)]),
    ("SELECT g FROM t UNION ALL SELECT g FROM u", 1, &[&[0], &[0]], &[]),
    ("SELECT g FROM t EXCEPT SELECT g FROM u", 1, &[&[0], &[0]], &[(Distinct, 2), (PositivePart, 1)]),
    ("SELECT g FROM t INTERSECT SELECT g FROM u", 1, &[&[0], &[0]], &[(Distinct, 2), (PositivePart, 1)]),
    ("SELECT g FROM t EXCEPT ALL SELECT g FROM u", 1, &[&[0], &[0]], &[(PositivePart, 1)]),
    ("SELECT g FROM t INTERSECT ALL SELECT g FROM u", 1, &[&[0], &[0]], &[(PositivePart, 1)]),
    // Segments: a non-trivial CTE and a subquery cut; a derived table over one
    // relation, a computed group key and a projection over a join do not.
    ("SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a", 2, &[&[1]], &[(EquiJoin, 2), (Reduce, 1), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)", 2, &[&[], &[0, 1]], &[(RangeJoin, 2), (Reduce, 1), (GlobalGround, 1), (WorkerFilter, 2), (Filter, 1)]),
    ("WITH agg AS (SELECT k, SUM(v) AS total FROM a GROUP BY k) SELECT b.w AS nm, agg.total AS tot FROM agg JOIN b ON agg.k = b.k", 2, &[&[1]], &[(EquiJoin, 2), (Reduce, 1)]),
    ("SELECT d.id FROM (SELECT id, v FROM t WHERE v > 2) d", 1, &[], &[(Filter, 1)]),
    ("WITH c AS (SELECT id, v FROM t WHERE v > 1) SELECT id FROM c", 2, &[], &[(Filter, 1)]),
    // A linear final over a grouped CTE: the segment's reduce keeps its exchange,
    // and the final — which neither re-keys nor redistributes — adds none.
    ("WITH c AS (SELECT g, SUM(v) AS s FROM t GROUP BY g) SELECT g FROM c WHERE s > 10", 2, &[&[1]], &[(Reduce, 1), (Filter, 1)]),
    ("WITH c AS (SELECT * FROM t) SELECT g FROM c WHERE v = 5", 1, &[], &[(Filter, 1)]),
    ("SELECT g, SUM(v * 2) AS s FROM t WHERE v > 5 GROUP BY g", 1, &[&[1]], &[(Reduce, 1), (Filter, 1)]),
    ("SELECT g + v AS k, SUM(g * v) AS s FROM t GROUP BY g + v HAVING SUM(g * v) > 3", 1, &[&[1]], &[(Reduce, 1), (Filter, 1)]),
    ("SELECT k, COUNT(*) AS n FROM (SELECT id, g AS k FROM t) d GROUP BY k", 1, &[&[1]], &[(Reduce, 1)]),
    ("SELECT DISTINCT g + 1 AS g1, v FROM t", 1, &[&[0]], &[(Distinct, 1)]),
    ("SELECT v * 2 AS x FROM t EXCEPT SELECT g FROM u", 1, &[&[0], &[0]], &[(Distinct, 2), (PositivePart, 1)]),
];

#[test]
fn every_shape_has_its_contract_nodes() {
    let cat = cat();
    let mut mismatches = Vec::new();
    for &(body, segments, exch, nodes) in SHAPES {
        let chain = view(&cat, body);
        let got: Vec<(Node, usize)> = Node::ALL
            .iter()
            .map(|&k| (k, total(&chain, move |op| k.matches(op))))
            .filter(|&(_, n)| n > 0)
            .collect();
        let mut want: Vec<(Node, usize)> = nodes.to_vec();
        want.sort();
        let mut want_exch: Vec<Vec<u32>> = exch.iter().map(|e| e.to_vec()).collect();
        want_exch.sort();
        if (chain.views.len(), exchanges(&chain), &got) != (segments, want_exch.clone(), &want) {
            mismatches.push(format!(
                "`{body}`\n     got {:?} {:?} {got:?}\n    want {segments:?} {want_exch:?} {want:?}",
                chain.views.len(),
                exchanges(&chain),
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "(segments, exchanges, nodes)\n{}",
        mismatches.join("\n")
    );
}

/// A keyless step's output leads with the hidden pair-PK: one slot per source
/// PK column, both sides, at the sources' own arity — the node counts are in
/// the shape table above.
#[test]
fn a_keyless_step_keys_its_output_by_the_hidden_pair_pk() {
    let cat = cat();
    let hidden = |name: &str| (name.to_string(), true, false);
    let shown = |name: &str| (name.to_string(), false, false);
    assert_eq!(
        output_shape(&view(&cat, "SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b")),
        vec![hidden("_pair_pk_0"), hidden("_pair_pk_1"), shown("aid"), shown("bid")]
    );
    assert_eq!(
        output_shape(&view(&cat, "SELECT c.v AS cv, b.id AS bid FROM c, b")),
        vec![
            hidden("_pair_pk_0"),
            hidden("_pair_pk_1"),
            hidden("_pair_pk_2"),
            shown("cv"),
            shown("bid")
        ]
    );
}

/// `UNION ALL` keeps the two sides apart by branch id so identical rows add up;
/// the deduplicating set operations hash both sides identically.
#[test]
fn union_all_tags_its_branches() {
    let cat = cat();
    for (body, want) in [
        ("SELECT g FROM t UNION ALL SELECT g FROM u", vec![0u8, 1]),
        ("SELECT g FROM t UNION SELECT g FROM u", vec![0, 0]),
        ("SELECT g FROM t EXCEPT SELECT g FROM u", vec![0, 0]),
    ] {
        let chain = view(&cat, body);
        let mut branches: Vec<u8> = chain
            .views
            .iter()
            .flat_map(|pv| pv.circuit.nodes().iter().map(|n| &n.op))
            .filter_map(|op| match op {
                OpNode::Map(MapKind::HashRow { branch_id, .. }) => Some(*branch_id),
                _ => None,
            })
            .collect();
        branches.sort_unstable();
        assert_eq!(branches, want, "`{body}`");
    }
}

/// `t(pk, g, ind, other)` with `u(pk, val)`; `indexes` lists `t`'s secondary
/// indexes by column.
fn indexed(indexes: &[&[u32]]) -> CatalogSnapshot {
    let i = TypeCode::I64;
    catalog(vec![
        (
            "t",
            rel(
                40,
                gnitz_core::RelClass::Table,
                false,
                vec![col("pk", i), col("g", i), col("ind", i), col("other", i)],
                vec![0],
                indexes,
            ),
        ),
        ("u", table(41, vec![col("pk", i), col("val", i)], vec![0])),
    ])
}

/// The backfill-scan bound a body's `ScanDelta` carries: the index columns it
/// ranges over, or none. A bound narrows only the initial scan; the filter
/// stays, so a body that cannot bound still computes the same view.
#[test]
fn indexed_predicates_bound_the_backfill_scan() {
    let on_ind = indexed(&[&[2]]);
    let on_ind_other = indexed(&[&[2, 3]]);
    let unindexed = indexed(&[]);
    #[rustfmt::skip]
    let rows: &[(&CatalogSnapshot, &str, &[&[u32]])] = &[
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 GROUP BY g", &[&[2]]),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE ind BETWEEN 5 AND 9 GROUP BY g", &[&[2]]),
        (&on_ind_other, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 AND other > 10 GROUP BY g", &[&[2, 3]]),
        (&on_ind, "SELECT g, other FROM t WHERE ind = 5", &[&[2]]),
        (&on_ind, "WITH c AS (SELECT * FROM t) SELECT g, COUNT(*) AS n FROM c WHERE ind = 5 GROUP BY g", &[&[2]]),
        (&on_ind, "SELECT g, COUNT(*) AS n FROM (SELECT * FROM t) d WHERE ind = 5 GROUP BY g", &[&[2]]),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE other = 5 GROUP BY g", &[]),
        (&on_ind, "SELECT DISTINCT g FROM t WHERE ind = 5", &[&[2]]),
        (&on_ind, "SELECT g FROM t WHERE ind = 5 EXCEPT SELECT val FROM u", &[&[2]]),
        // Each scan carries its bound; the engine drops a twice-scanned source's.
        (&on_ind, "SELECT g FROM t WHERE ind = 5 UNION ALL SELECT g FROM t WHERE ind = 6", &[&[2], &[2]]),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE pk = 5 GROUP BY g", &[]),
        // A WHERE placed into a join input bounds that input's scan.
        (&on_ind, "SELECT t.g, COUNT(*) AS c FROM t JOIN u ON t.pk = u.pk WHERE t.ind = 5 GROUP BY t.g", &[&[2]]),
        (&unindexed, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 GROUP BY g", &[]),
    ];
    for &(cat, body, want) in rows {
        let chain = view(cat, body);
        let bounds: Vec<Vec<u32>> = chain
            .views
            .iter()
            .flat_map(|pv| pv.circuit.nodes().iter().map(|n| &n.op))
            .filter_map(|op| match op {
                OpNode::ScanDelta { bound: Some(b), .. } => Some(b.idx_cols.as_slice().to_vec()),
                _ => None,
            })
            .collect();
        let want: Vec<Vec<u32>> = want.iter().map(|w| w.to_vec()).collect();
        assert_eq!(bounds, want, "`{body}`");
    }
}

/// A HAVING with no GROUP BY and no aggregate is still a grouped body: it filters
/// the one whole-relation group, `Reduce([], [])`. The two surfaces reach that
/// shape by different mechanisms, so one query pins both.
#[test]
fn a_having_without_a_group_by_is_the_whole_relation_group_on_both_surfaces() {
    let cat = base();
    const BODY: &str = "SELECT 1 AS one FROM t HAVING 1 = 1";

    // Ad-hoc: the fold sink, over no group columns and no aggregate.
    assert_eq!(
        gnitz_sql::explain_lines(&read(&cat, &format!("EXPLAIN {BODY}")).unwrap())[3],
        "fold: global aggregate: ; HAVING applied client-side"
    );

    // View: every reduce groups on nothing, one seeds the ground row, and the
    // only aggregates are the cardinality COUNT and the SUM_ZERO its combine
    // folds that COUNT with — no user aggregate.
    let chain = view(&cat, BODY);
    let reduces: Vec<(Vec<u32>, Vec<gnitz_wire::AggFunc>, bool)> = chain
        .views
        .iter()
        .flat_map(|pv| pv.circuit.nodes().iter().map(|n| &n.op))
        .filter_map(|op| match op {
            OpNode::Reduce { group_cols, agg, global_ground, .. } => Some((
                group_cols.as_slice().to_vec(),
                agg.iter().map(|d| d.agg_op).collect(),
                *global_ground,
            )),
            _ => None,
        })
        .collect();
    assert!(!reduces.is_empty(), "the view body compiles to a reduce");
    for (group_cols, ops, _) in &reduces {
        assert!(group_cols.is_empty(), "{reduces:?}");
        for op in ops {
            assert!(
                matches!(op, gnitz_wire::AggFunc::Count | gnitz_wire::AggFunc::SumZero),
                "{reduces:?}"
            );
        }
    }
    assert_eq!(
        reduces.iter().filter(|(_, _, ground)| *ground).count(),
        1,
        "{reduces:?}"
    );

    // And the output columns agree with the ad-hoc reply.
    assert_eq!(
        output_shape(&chain)
            .into_iter()
            .filter(|(_, hidden, _)| !hidden)
            .map(|(n, _, _)| n)
            .collect::<Vec<_>>(),
        ["one"]
    );
}
