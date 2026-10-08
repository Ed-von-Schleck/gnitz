//! The circuit shape a view body compiles to, read straight off the view planner.
//!
//! Each row pins only what CLAUDE.md §3 and the placement contracts require:
//! segment count, where the exchanges sit and what reads them, the bilinear
//! joins, the reduce / clamp / null-fill /
//! worker-filter / filter node counts. Projection lists, map counts, union
//! counts and node numbering are free to change.

use gnitz_wire::{Circuit, OpNode, TypeCode};
use gnitz_wire::{ClampKind, JoinKind, ReadBound};

use super::*;

/// What an `ExchangeShard` co-locates by: the group columns of the reduce or
/// top-N reading it, or [`PK`] where no reader is one.
type Exchange = Option<&'static [u32]>;
const PK: Exchange = None;

/// One row of the shape matrix: body, segment count, the exchanges (one
/// [`Exchange`] per `ExchangeShard`, any order), and the node kinds the body must
/// compile to, with their counts. A kind a row omits must not appear at all, so a
/// new kind touches [`Node`] and only the rows that carry it.
type Row = (&'static str, usize, &'static [Exchange], &'static [(Node, usize)]);

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
    TopN,
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
        TopN,
    ];

    fn matches(self, circuit: &Circuit, node: &gnitz_wire::Node) -> bool {
        let op = &node.op;
        match self {
            EquiJoin => matches!(op, OpNode::Join { kind: JoinKind::Equi, .. }),
            RangeJoin => matches!(op, OpNode::Join { kind: JoinKind::Range { .. }, .. }),
            CrossJoin => matches!(op, OpNode::Join { kind: JoinKind::Cross, .. }),
            Reduce => matches!(op, OpNode::Reduce { .. }),
            GlobalGround => owes_ground(circuit, node),
            Distinct => matches!(op, OpNode::WeightClamp(ClampKind::Distinct)),
            PositivePart => matches!(op, OpNode::WeightClamp(ClampKind::PositivePart)),
            WorkerFilter => matches!(op, OpNode::WorkerFilter),
            NullExtend => matches!(op, OpNode::NullExtend { .. }),
            Filter => matches!(op, OpNode::Filter(_)),
            TopN => matches!(op, OpNode::TopN { .. }),
        }
    }
}

/// [`base`] plus `m`, a second table with nullable join keys, and `rv`, a
/// registered reduce view `(g, n)` keyed by its one group column.
fn cat() -> TestCatalog {
    let cat = base();
    let i = TypeCode::I64;
    cat.insert(
        &in_sn("m"),
        table(30, vec![col("id", i), ncol("k", i), ncol("v", i)], vec![0]),
    );
    let rv = view(&cat, "SELECT g, COUNT(*) AS n FROM t GROUP BY g");
    register(&cat, "rv", 31, gnitz_wire::RelClass::View, final_view(&rv));
    cat
}

/// A reduce over no group columns behind an exchange: the one that owes a row
/// over an empty input.
fn owes_ground(circuit: &Circuit, node: &gnitz_wire::Node) -> bool {
    matches!(&node.op, OpNode::Reduce { group_cols, .. } if group_cols.is_empty())
        && matches!(circuit.nodes()[node.inputs()[0]].op, OpNode::ExchangeShard)
}

/// How many nodes of every segment are a `kind`.
fn total(chain: &PlannedChain, kind: Node) -> usize {
    all_views(chain)
        .map(|pv| {
            pv.circuit
                .nodes()
                .iter()
                .filter(|n| kind.matches(&pv.circuit, n))
                .count()
        })
        .sum()
}

/// Every exchange of every segment, by the group columns of the reduce or top-N
/// reading it.
fn exchanges(chain: &PlannedChain) -> Vec<Option<Vec<u32>>> {
    let mut out: Vec<Option<Vec<u32>>> = all_views(chain)
        .flat_map(|pv| {
            let nodes = pv.circuit.nodes();
            (0..nodes.len())
                .filter(|&at| matches!(nodes[at].op, OpNode::ExchangeShard))
                .map(|at| {
                    nodes
                        .iter()
                        .filter(|n| n.inputs().contains(&at))
                        .find_map(|n| match &n.op {
                            OpNode::Reduce { group_cols, .. } | OpNode::TopN { group_cols, .. } => {
                                Some(group_cols.clone())
                            }
                            _ => None,
                        })
                })
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
    // Equi join: one join, no exchange; a null-fill against a non-unique
    // side subtracts the preserved rows joined (one more join) against the other
    // side's distinct key set, then null-extends.
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k", 1, &[], &[(EquiJoin, 1)]),
    ("SELECT a.id AS aid, b.w FROM a LEFT JOIN b ON a.k = b.k", 1, &[], &[(EquiJoin, 2), (Distinct, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.w FROM a RIGHT JOIN b ON a.k = b.k", 1, &[], &[(EquiJoin, 2), (Distinct, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.w FROM a FULL JOIN b ON a.k = b.k", 1, &[], &[(EquiJoin, 3), (Distinct, 2), (NullExtend, 2)]),
    // A null-fill against a side unique on the key subtracts without a clamp.
    ("SELECT a.id AS aid, b.w FROM a LEFT JOIN b ON a.k = b.id", 1, &[], &[(EquiJoin, 1), (NullExtend, 1)]),
    // A residual or WHERE is one filter; a nullable key is none, its re-key
    // dropping NULL-keyed rows itself.
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k AND a.v <> b.w", 1, &[], &[(EquiJoin, 1), (Filter, 1)]),
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k AND (a.v > 5 OR b.w < 3)", 1, &[], &[(EquiJoin, 1), (Filter, 1)]),
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k WHERE a.v > 5", 1, &[], &[(EquiJoin, 1), (Filter, 1)]),
    ("SELECT n.id AS nid, b.w FROM n JOIN b ON n.k = b.k", 1, &[], &[(EquiJoin, 1)]),
    ("SELECT a.id AS aid, n.v FROM a JOIN n ON a.k = n.k", 1, &[], &[(EquiJoin, 1)]),
    ("SELECT n.id AS nid, b.w FROM n LEFT JOIN b ON n.k = b.k", 1, &[], &[(EquiJoin, 2), (Distinct, 1), (NullExtend, 1)]),
    ("SELECT n.id AS nid, m.v FROM n LEFT JOIN m ON n.k = m.k", 1, &[], &[(EquiJoin, 2), (Distinct, 1), (NullExtend, 1)]),
    // A composite key mixing a NOT NULL and a nullable column.
    ("SELECT n.id AS nid, b.w FROM n LEFT JOIN b ON n.k = b.k AND n.id = b.w", 1, &[], &[(EquiJoin, 2), (Distinct, 1), (NullExtend, 1)]),
    // An ON conjunct over the null-supplying side filters that input.
    ("SELECT a.id AS aid, b.w FROM a LEFT JOIN b ON a.k = b.k AND b.w > 3", 1, &[], &[(EquiJoin, 2), (Distinct, 1), (NullExtend, 1), (Filter, 1)]),
    // A filtered derived-table input fuses into the join, cutting no segment.
    ("SELECT a.id AS aid, d.w FROM a JOIN (SELECT k, w FROM b WHERE w > 3) d ON a.k = d.k", 1, &[], &[(EquiJoin, 1), (Filter, 1)]),
    // Band join: one range join, one output exchange on the pair-PK, no worker filter.
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[PK], &[(RangeJoin, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a RIGHT JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 2), (NullExtend, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w WHERE a.id > 5", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 1), (NullExtend, 1), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v < b.w AND a.id > b.id", 1, &[PK], &[(RangeJoin, 1), (Filter, 1)]),
    // Pure range: broadcast trimmed by one worker filter per side; LEFT derives
    // its null-fill from a threshold reduce, so no clamp.
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w", 1, &[PK], &[(RangeJoin, 1), (WorkerFilter, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.v < b.w", 1, &[PK], &[(RangeJoin, 2), (Reduce, 1), (WorkerFilter, 2), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w AND a.id <> b.id", 1, &[PK], &[(RangeJoin, 1), (WorkerFilter, 2), (Filter, 1)]),
    // Keyless (cross) join: one cross join, one worker filter per side, the
    // pair-PK output shard; a residual — a constant `ON 1 = 1` included — or a
    // WHERE is one filter. A self product wraps the second copy in a segment,
    // and a third relation is one more keyless step over the cut product.
    ("SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b", 1, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a, b", 1, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON 1 = 1", 1, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v <> b.w", 1, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a, b WHERE a.v <> b.w", 1, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b WHERE a.v > 3", 1, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2), (Filter, 1)]),
    ("SELECT c.v AS cv, b.id AS bid FROM c CROSS JOIN b", 1, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2)]),
    ("SELECT c.v AS cv, ty.id AS tid FROM c NATURAL JOIN ty", 1, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2)]),
    ("SELECT x.id AS xa, y.id AS yb FROM a x, a y", 2, &[PK], &[(CrossJoin, 1), (WorkerFilter, 2)]),
    ("SELECT a.id AS aid, b.id AS bid, u.id AS uid FROM a, b, u", 2, &[PK, PK], &[(CrossJoin, 2), (WorkerFilter, 4)]),
    // Semi / anti / mark / IN: a join against B's key set, no exchange.
    ("SELECT a.v FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)", 1, &[], &[(EquiJoin, 1), (Distinct, 1)]),
    ("SELECT a.v FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)", 1, &[], &[(EquiJoin, 1), (Distinct, 1)]),
    ("SELECT id FROM a WHERE k IN (SELECT k FROM b)", 1, &[], &[(EquiJoin, 1), (Distinct, 1)]),
    ("SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AS flag FROM a", 1, &[], &[(EquiJoin, 1), (Distinct, 1)]),
    // A B unique on the key already is a set.
    ("SELECT id FROM a WHERE k IN (SELECT id FROM b)", 1, &[], &[(EquiJoin, 1)]),
    // Two marks: the lower one is cut, carrying its mark to the upper one, whose
    // matched branch folds the WHERE reading both to true.
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR EXISTS (SELECT 1 FROM b WHERE b.w = a.v)", 2, &[], &[(EquiJoin, 2), (Distinct, 2), (Filter, 1)]),
    ("SELECT id FROM n WHERE EXISTS (SELECT 1 FROM m WHERE m.k = n.k)", 1, &[], &[(EquiJoin, 1), (Distinct, 1)]),
    ("SELECT id FROM n WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = n.k AND b.w = n.id)", 1, &[], &[(EquiJoin, 1), (Distinct, 1)]),
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w < a.v)", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 1)]),
    // Pure range: A is owned before the join, so its output needs no exchange.
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", 1, &[], &[(RangeJoin, 1), (Reduce, 1), (WorkerFilter, 1)]),
    ("SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", 1, &[], &[(RangeJoin, 1), (Reduce, 1), (WorkerFilter, 1)]),
    // Reduce: an exchange on the group columns — for a global aggregate an empty
    // key, and a ground row.
    ("SELECT g, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY g", 1, &[Some(&[1])], &[(Reduce, 1)]),
    ("SELECT a AS ka, b AS kb, COUNT(*) AS n, SUM(v) AS s FROM c GROUP BY a, b", 1, &[Some(&[0, 1])], &[(Reduce, 1)]),
    ("SELECT g, SUM(v) AS s FROM t GROUP BY g HAVING SUM(v) > 10", 1, &[Some(&[1])], &[(Reduce, 1), (Filter, 1)]),
    ("SELECT g + v AS k, SUM(g * v) AS s FROM t GROUP BY g + v", 1, &[Some(&[1])], &[(Reduce, 1)]),
    ("SELECT SUM(v) AS s, MIN(v) AS mn, MAX(v) AS mx FROM t", 1, &[Some(&[])], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT SUM(v) AS s, COUNT(*) AS c FROM t", 1, &[Some(&[])], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT AVG(i32c) AS s FROM ty", 1, &[Some(&[])], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT COUNT(k) AS c FROM n", 1, &[Some(&[])], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT SUM(f) AS s FROM ty", 1, &[Some(&[])], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT AVG(f) AS s FROM ty", 1, &[Some(&[])], &[(Reduce, 1), (GlobalGround, 1)]),
    ("SELECT v, COUNT(*) AS n FROM r GROUP BY v", 1, &[Some(&[1])], &[(Reduce, 1)]),
    // DISTINCT and set operations: content-hashed leaves, each behind an exchange
    // on its hash PK, then weight clamps; never a join.
    ("SELECT DISTINCT g FROM t", 1, &[PK], &[(Distinct, 1)]),
    ("SELECT g FROM t UNION SELECT g FROM u", 1, &[PK, PK], &[(Distinct, 1)]),
    ("SELECT g FROM t UNION ALL SELECT g FROM u", 1, &[PK, PK], &[]),
    // Only the left side is clamped: the right's weights are never negative.
    ("SELECT g FROM t EXCEPT SELECT g FROM u", 1, &[PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    ("SELECT g FROM t INTERSECT SELECT g FROM u", 1, &[PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    ("SELECT g FROM t EXCEPT ALL SELECT g FROM u", 1, &[PK, PK], &[(PositivePart, 1)]),
    ("SELECT g FROM t INTERSECT ALL SELECT g FROM u", 1, &[PK, PK], &[(PositivePart, 1)]),
    // A side that is already a set needs no clamp: EXCEPT's left, either of
    // INTERSECT's; a DISTINCT over one is the input itself.
    ("SELECT id, g FROM t EXCEPT SELECT id, g FROM u", 1, &[PK, PK], &[(PositivePart, 1)]),
    ("SELECT g FROM t INTERSECT SELECT id FROM u", 1, &[PK, PK], &[(PositivePart, 1)]),
    ("SELECT DISTINCT id, g FROM t", 1, &[], &[]),
    // A view whose PK does not repeat is a set as a table is: a DISTINCT over its
    // key is the input, and a null-fill against it subtracts without a clamp.
    ("SELECT DISTINCT g, n FROM rv", 1, &[], &[]),
    ("SELECT a.id AS aid, rv.n FROM a LEFT JOIN rv ON a.k = rv.g", 1, &[], &[(EquiJoin, 1), (NullExtend, 1)]),
    // A tree of directly nested set operations is one circuit, one exchange per
    // leaf; a UNION DISTINCT clamps once over all of its leaves.
    ("SELECT g FROM t UNION SELECT g FROM u UNION SELECT v FROM a", 1, &[PK, PK, PK], &[(Distinct, 1)]),
    ("SELECT g FROM t UNION ALL SELECT g FROM u INTERSECT SELECT v FROM a", 1, &[PK, PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    // Segments: a non-trivial CTE and a subquery cut; a derived table over one
    // relation, a computed group key and a projection over a join do not.
    ("SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1), (NullExtend, 1)]),
    ("SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)", 2, &[PK, Some(&[])], &[(RangeJoin, 1), (Reduce, 1), (GlobalGround, 1), (WorkerFilter, 2)]),
    ("WITH agg AS (SELECT k, SUM(v) AS total FROM a GROUP BY k) SELECT b.w AS nm, agg.total AS tot FROM agg JOIN b ON agg.k = b.k", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1)]),
    ("SELECT d.id FROM (SELECT id, v FROM t WHERE v > 2) d", 1, &[], &[(Filter, 1)]),
    ("WITH c AS (SELECT id, v FROM t WHERE v > 1) SELECT id FROM c", 2, &[], &[(Filter, 1)]),
    // A linear final over a grouped CTE: the segment's reduce keeps its exchange,
    // and the final — which neither re-keys nor redistributes — adds none.
    ("WITH c AS (SELECT g, SUM(v) AS s FROM t GROUP BY g) SELECT g FROM c WHERE s > 10", 2, &[Some(&[1])], &[(Reduce, 1), (Filter, 1)]),
    ("WITH c AS (SELECT * FROM t) SELECT g FROM c WHERE v = 5", 1, &[], &[(Filter, 1)]),
    ("SELECT g, SUM(v * 2) AS s FROM t WHERE v > 5 GROUP BY g", 1, &[Some(&[1])], &[(Reduce, 1), (Filter, 1)]),
    ("SELECT g + v AS k, SUM(g * v) AS s FROM t GROUP BY g + v HAVING SUM(g * v) > 3", 1, &[Some(&[1])], &[(Reduce, 1), (Filter, 1)]),
    ("SELECT k, COUNT(*) AS n FROM (SELECT id, g AS k FROM t) d GROUP BY k", 1, &[Some(&[1])], &[(Reduce, 1)]),
    ("SELECT DISTINCT g + 1 AS g1, v FROM t", 1, &[PK], &[(Distinct, 1)]),
    ("SELECT v * 2 AS x FROM t EXCEPT SELECT g FROM u", 1, &[PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    // Window: a whole-partition frame joins the source to one reduce over it; a
    // cumulative frame folds over a band self-join first.
    ("SELECT id, SUM(v) OVER (PARTITION BY g) AS s FROM t", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1)]),
    ("SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM t", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1)]),
    ("SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s FROM t", 5, &[PK, Some(&[1, 2]), Some(&[2, 3])], &[(EquiJoin, 1), (RangeJoin, 1), (Reduce, 2)]),
    // An order key a whole-partition frame drops is not computed.
    ("SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY v + 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM t", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1)]),
    // A QUALIFY bounding an unprojected ROW_NUMBER is a top-N per partition,
    // with no band join — cutting the outer side of whatever else the body
    // joins in; a projected one is the ranking desugar. The body's projection
    // and filter run in the top-N's own circuit.
    ("SELECT id, g FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) <= 2", 1, &[Some(&[1])], &[(TopN, 1)]),
    ("SELECT id, g FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) < 3", 1, &[Some(&[1])], &[(TopN, 1)]),
    ("SELECT id, g FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) <= 2 AND v > 0", 1, &[Some(&[1])], &[(TopN, 1), (Filter, 1)]),
    ("SELECT id, SUM(v) OVER (PARTITION BY g) AS s FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) = 1", 3, &[Some(&[1]), Some(&[2])], &[(TopN, 1), (EquiJoin, 1), (Reduce, 1)]),
    ("SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn FROM t QUALIFY rn <= 2", 5, &[PK, Some(&[1, 2, 0]), Some(&[2, 3, 4])], &[(EquiJoin, 1), (RangeJoin, 1), (Reduce, 2), (Filter, 2)]),
];

#[test]
fn every_shape_has_its_contract_nodes() {
    let cat = cat();
    let mut mismatches = Vec::new();
    for &(body, segments, exch, nodes) in SHAPES {
        let chain = view(&cat, body);
        let got: Vec<(Node, usize)> = Node::ALL
            .iter()
            .map(|&k| (k, total(&chain, k)))
            .filter(|&(_, n)| n > 0)
            .collect();
        let mut want: Vec<(Node, usize)> = nodes.to_vec();
        want.sort();
        let mut want_exch: Vec<Option<Vec<u32>>> = exch.iter().map(|e| e.map(<[u32]>::to_vec)).collect();
        want_exch.sort();
        if (view_count(&chain), exchanges(&chain), &got) != (segments, want_exch.clone(), &want) {
            mismatches.push(format!(
                "`{body}`\n     got {:?} {:?} {got:?}\n    want {segments:?} {want_exch:?} {want:?}",
                view_count(&chain),
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

/// `t(pk, g, ind, other)` with `u(pk, val)`; `indexes` lists `t`'s secondary
/// indexes by column.
fn indexed(indexes: &[&[u32]]) -> TestCatalog {
    let i = TypeCode::I64;
    catalog(vec![
        (
            "t",
            rel(
                40,
                gnitz_wire::RelClass::Table,
                vec![col("pk", i), col("g", i), col("ind", i), col("other", i)],
                vec![0],
                indexes.iter().map(|cols| ix(cols)).collect(),
            ),
        ),
        ("u", table(41, vec![col("pk", i), col("val", i)], vec![0])),
    ])
}

/// The backfill-scan bound each `ScanDelta` of a body carries. A bound narrows only
/// the initial scan; the filter stays, so a body that cannot bound computes the
/// same view.
#[test]
fn indexed_predicates_bound_the_backfill_scan() {
    let on_ind = indexed(&[&[2]]);
    let on_ind_other = indexed(&[&[2, 3]]);
    let unindexed = indexed(&[]);
    #[rustfmt::skip]
    let rows: &[(&TestCatalog, &str, &[&str])] = &[
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 GROUP BY g", &["[2]"]),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE ind BETWEEN 5 AND 9 GROUP BY g", &["[2]"]),
        (&on_ind_other, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 AND other > 10 GROUP BY g", &["[2, 3]"]),
        (&on_ind, "SELECT g, other FROM t WHERE ind = 5", &["[2]"]),
        (&on_ind, "WITH c AS (SELECT * FROM t) SELECT g, COUNT(*) AS n FROM c WHERE ind = 5 GROUP BY g", &["[2]"]),
        (&on_ind, "SELECT g, COUNT(*) AS n FROM (SELECT * FROM t) d WHERE ind = 5 GROUP BY g", &["[2]"]),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE other = 5 GROUP BY g", &[]),
        (&on_ind, "SELECT DISTINCT g FROM t WHERE ind = 5", &["[2]"]),
        (&on_ind, "SELECT g FROM t WHERE ind = 5 EXCEPT SELECT val FROM u", &["[2]"]),
        // Each scan carries its bound; the engine drops a twice-scanned source's.
        (&on_ind, "SELECT g FROM t WHERE ind = 5 UNION ALL SELECT g FROM t WHERE ind = 6", &["[2]", "[2]"]),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE pk = 5 GROUP BY g", &["pk set 1"]),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE pk BETWEEN 1 AND 9 GROUP BY g", &["pk range"]),
        // An index keeps the backfill over a PK range.
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE pk > 3 AND ind = 5 GROUP BY g", &["[2]"]),
        // A WHERE placed into a join input bounds that input's scan.
        (&on_ind, "SELECT t.g, COUNT(*) AS c FROM t JOIN u ON t.pk = u.pk WHERE t.ind = 5 GROUP BY t.g", &["[2]"]),
        (&unindexed, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 GROUP BY g", &[]),
    ];
    for &(cat, body, want) in rows {
        let chain = view(cat, body);
        let bounds: Vec<String> = all_views(&chain)
            .flat_map(|pv| pv.circuit.nodes().iter().map(|n| &n.op))
            .filter_map(|op| match op {
                OpNode::ScanDelta { bound, .. } => match bound {
                    ReadBound::None => None,
                    ReadBound::Range(r) if r.walks_pk(&[0]) => Some("pk range".to_string()),
                    ReadBound::PkSet(keys) => Some(format!("pk set {}", keys.len())),
                    ReadBound::Range(r) => Some(format!("{:?}", r.cols().as_slice())),
                },
                _ => None,
            })
            .collect();
        let want: Vec<String> = want.iter().map(|w| w.to_string()).collect();
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
        crate::dml::explain_lines(&read(&cat, &format!("EXPLAIN {BODY}")).unwrap(), false)[3],
        "fold: global aggregate; HAVING applied client-side"
    );

    // View: every reduce groups on nothing, one seeds the ground row, and none
    // carries a user aggregate.
    let chain = view(&cat, BODY);
    let reduces: Vec<(Vec<u32>, Vec<gnitz_wire::AggFunc>, bool)> = all_views(&chain)
        .flat_map(|pv| pv.circuit.nodes().iter().map(|n| (&pv.circuit, n)))
        .filter_map(|(circuit, node)| match &node.op {
            OpNode::Reduce { group_cols, agg } => Some((
                group_cols.as_slice().to_vec(),
                agg.iter().map(|d| d.agg_op).collect(),
                owes_ground(circuit, node),
            )),
            _ => None,
        })
        .collect();
    assert!(!reduces.is_empty(), "the view body compiles to a reduce");
    for (group_cols, ops, _) in &reduces {
        assert!(group_cols.is_empty(), "{reduces:?}");
        for op in ops {
            assert!(
                matches!(op, gnitz_wire::AggFunc::Count | gnitz_wire::AggFunc::Sum),
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

/// GROUP BY and PARTITION BY are unordered, so a permutation of leading PK
/// columns is planned in PK order; any other group list keeps the order written.
#[test]
fn a_permutation_of_leading_pk_columns_groups_in_pk_order() {
    let cat = base();
    let i = TypeCode::I64;
    cat.insert(
        &in_sn("ck3"),
        table(
            30,
            vec![col("k1", i), col("k2", i), col("k3", i), col("v", i)],
            vec![0, 1, 2],
        ),
    );
    for (body, want) in [
        ("SELECT k2, k1, COUNT(*) AS n FROM ck3 GROUP BY k2, k1", &[0u32, 1][..]),
        ("SELECT k1, k2, COUNT(*) AS n FROM ck3 GROUP BY k1, k2", &[0, 1]),
        (
            "SELECT k1, k2, k3, COUNT(*) AS n FROM ck3 GROUP BY k3, k1, k2",
            &[0, 1, 2],
        ),
        ("SELECT k3, k2, COUNT(*) AS n FROM ck3 GROUP BY k3, k2", &[2, 1]),
        (
            "SELECT k1, k2, k3, v FROM ck3 QUALIFY ROW_NUMBER() OVER (PARTITION BY k2, k1 ORDER BY v) <= 2",
            &[0, 1],
        ),
    ] {
        let chain = view(&cat, body);
        let groups: Vec<&[u32]> = all_views(&chain)
            .flat_map(|pv| pv.circuit.nodes().iter().map(|n| &n.op))
            .filter_map(|op| match op {
                OpNode::Reduce { group_cols, .. } | OpNode::TopN { group_cols, .. } => Some(group_cols.as_slice()),
                _ => None,
            })
            .collect();
        assert!(!groups.is_empty(), "{body}: no reduce or top-N");
        assert!(groups.iter().all(|g| *g == want), "{body}: {groups:?}");
    }
}

/// Every scatter key of the final view, as `(source, the columns it states)`, in
/// circuit order: each re-key walked back to the scan it reads.
fn scatter_keys(chain: &PlannedChain) -> Vec<(u64, Vec<u32>)> {
    use gnitz_wire::{MapKind, ReindexRole};
    let nodes = final_view(chain).circuit.nodes();
    nodes
        .iter()
        .filter_map(|node| {
            let OpNode::Map(MapKind::Reindex {
                role: ReindexRole::ScatterKey { source_cols },
                ..
            }) = &node.op
            else {
                return None;
            };
            let mut at = node;
            loop {
                match at.op {
                    OpNode::ScanDelta { source, .. } => return Some((source, source_cols.clone())),
                    _ => at = &nodes[at.inputs()[0]],
                }
            }
        })
        .collect()
}

const A: u64 = 18;
const B: u64 = 19;

/// A side states the slots its delta is routed by: an equi join's whole key, a
/// band's equality prefix, none for a pure range or a cross join — and the whole
/// source PK for the outer side of an EXISTS over a pure range, which is owned
/// before it is joined.
#[test]
fn a_join_side_states_the_slots_its_delta_is_routed_by() {
    let cat = cat();
    #[rustfmt::skip]
    let rows: &[(&str, &[u32], &[u32])] = &[
        ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k", &[1], &[1]),
        ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k AND a.v = b.w", &[1, 2], &[1, 2]),
        ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v <= b.w", &[1], &[1]),
        ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w", &[], &[]),
        ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.v < b.w", &[], &[]),
        ("SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b", &[], &[]),
        ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", &[0], &[]),
    ];
    for &(body, a, b) in rows {
        let keys = scatter_keys(&view(&cat, body));
        for (source, want) in [(A, a), (B, b)] {
            let stated: Vec<&[u32]> = keys.iter().filter(|k| k.0 == source).map(|k| k.1.as_slice()).collect();
            assert!(!stated.is_empty(), "`{body}`: source {source} states no key");
            assert!(
                stated.iter().all(|s| *s == want),
                "`{body}`: source {source} states {stated:?}"
            );
        }
    }
}

/// A preserved side's NULL-keyed rows are unmatched rows, so it is re-keyed a
/// second time keeping them — only where a key column can be NULL at all.
#[test]
fn the_null_keeping_rekey_is_emitted_only_over_a_nullable_key() {
    let cat = cat();
    let n = 20;
    let rekeys = |body: &str, source: u64| scatter_keys(&view(&cat, body)).iter().filter(|k| k.0 == source).count();
    let not_null = "SELECT a.id AS aid, b.w FROM a LEFT JOIN b ON a.k = b.k";
    assert_eq!([rekeys(not_null, A), rekeys(not_null, B)], [1, 1]);
    let nullable = "SELECT n.id AS nid, b.w FROM n LEFT JOIN b ON n.k = b.k";
    assert_eq!([rekeys(nullable, n), rekeys(nullable, B)], [2, 1]);
    // The null-supplying side's unmatched rows are no output.
    let supplying = "SELECT a.id AS aid, n.v FROM a LEFT JOIN n ON a.k = n.k";
    assert_eq!([rekeys(supplying, A), rekeys(supplying, n)], [1, 1]);
}

/// A backfill feeds a view's sources in scan order, the first against nothing:
/// the one side that outputs its unmatched rows is scanned second.
#[test]
fn the_side_that_alone_outputs_its_unmatched_rows_is_scanned_second() {
    let cat = cat();
    let (t, u) = (16, 17);
    #[rustfmt::skip]
    let rows: &[(&str, [u64; 2])] = &[
        ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k", [A, B]),
        ("SELECT a.id AS aid, b.w FROM a LEFT JOIN b ON a.k = b.k", [B, A]),
        ("SELECT a.id AS aid, b.w FROM a RIGHT JOIN b ON a.k = b.k", [A, B]),
        // Whichever side goes first is null-filled whole.
        ("SELECT a.id AS aid, b.w FROM a FULL JOIN b ON a.k = b.k", [A, B]),
        ("SELECT a.v FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)", [B, A]),
        ("SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AS flag FROM a", [B, A]),
        ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w", [B, A]),
        ("SELECT g FROM t EXCEPT SELECT g FROM u", [u, t]),
        ("SELECT g FROM t INTERSECT SELECT g FROM u", [u, t]),
        ("SELECT g FROM t UNION SELECT g FROM u", [t, u]),
    ];
    for &(body, want) in rows {
        let scanned: Vec<u64> = final_view(&view(&cat, body)).circuit.sources().collect();
        assert_eq!(scanned, want, "`{body}`");
    }
}
