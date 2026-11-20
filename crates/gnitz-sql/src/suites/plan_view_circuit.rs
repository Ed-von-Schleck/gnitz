//! The circuit shape a view body compiles to, read straight off the view planner.
//!
//! Each row pins only what CLAUDE.md §3 and the placement contracts require:
//! segment count, where the exchanges sit and what reads them, the bilinear
//! joins, the reduce / clamp / null-fill / filter node counts. Projection lists, map counts, union
//! counts and node numbering are free to change.

use gnitz_wire::{ClampKind, JoinKind, ReadBound};
use gnitz_wire::{OpNode, TypeCode};

use super::*;

/// What an exchange co-locates by: the group columns of the reduce, top-N or
/// window that reads its input so, or [`PK`] for an `ExchangeShard`.
type Exchange = Option<&'static [u32]>;
const PK: Exchange = None;

/// One row of the shape matrix: body, segment count, the exchanges (one
/// [`Exchange`] per exchanging node, any order), and the node kinds the body must
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
    NullExtend,
    Filter,
    TopN,
    Window,
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
        NullExtend,
        Filter,
        TopN,
        Window,
    ];

    fn matches(self, op: &OpNode) -> bool {
        match self {
            EquiJoin => matches!(op, OpNode::Join(JoinKind::Equi)),
            RangeJoin => matches!(op, OpNode::Join(JoinKind::Range { .. })),
            CrossJoin => matches!(op, OpNode::Join(JoinKind::Cross)),
            Reduce => matches!(op, OpNode::Reduce { .. }),
            GlobalGround => op.owes_ground_row(),
            Distinct => matches!(op, OpNode::WeightClamp(ClampKind::Distinct)),
            PositivePart => matches!(op, OpNode::WeightClamp(ClampKind::PositivePart)),
            NullExtend => matches!(op, OpNode::NullExtend { .. }),
            Filter => matches!(op, OpNode::Filter(_)),
            TopN => matches!(op, OpNode::TopN { .. }),
            Window => matches!(op, OpNode::Window { .. }),
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

/// How many nodes of every segment are a `kind`.
fn total(chain: &ViewBundle, kind: Node) -> usize {
    all_views(chain)
        .map(|pv| pv.circuit.ops().filter(|&(_, op)| kind.matches(op)).count())
        .sum()
}

/// The columns of its input `op` groups by, for an operator that groups.
fn group_cols(op: &OpNode) -> Option<&[u32]> {
    match op {
        OpNode::Reduce { group, .. } | OpNode::Window { group, .. } => Some(&group.group_cols),
        OpNode::TopN { group_cols, .. } => Some(group_cols),
        _ => None,
    }
}

/// Every exchange of every segment, by the group columns of the reduce, top-N
/// or window making it.
fn exchanges(chain: &ViewBundle) -> Vec<Option<Vec<u32>>> {
    let mut out: Vec<Option<Vec<u32>>> = all_views(chain)
        .flat_map(|pv| pv.circuit.ops().map(|(_, op)| op))
        .filter(|op| op.exchanges())
        .map(|op| group_cols(op).map(<[u32]>::to_vec))
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
    // Band join: one range join, one output exchange on the pair-PK.
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[PK], &[(RangeJoin, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a RIGHT JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 2), (NullExtend, 2)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w WHERE a.id > 5", 1, &[PK], &[(RangeJoin, 1), (PositivePart, 1), (NullExtend, 1), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v < b.w AND a.id > b.id", 1, &[PK], &[(RangeJoin, 1), (Filter, 1)]),
    // Pure range: broadcast trimmed by one PK exchange per side; LEFT derives
    // its null-fill from a threshold reduce, so no clamp.
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w", 1, &[PK, PK, PK], &[(RangeJoin, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.v < b.w", 1, &[PK, PK, PK], &[(RangeJoin, 2), (Reduce, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w AND a.id <> b.id", 1, &[PK, PK, PK], &[(RangeJoin, 1), (Filter, 1)]),
    // Keyless (cross) join: one cross join, one PK exchange per side, the
    // pair-PK output shard; a residual — a constant `ON 1 = 1` included — or a
    // WHERE is one filter. A self product wraps the second copy in a segment,
    // and a third relation is one more keyless step over the cut product.
    ("SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b", 1, &[PK, PK, PK], &[(CrossJoin, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a, b", 1, &[PK, PK, PK], &[(CrossJoin, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON 1 = 1", 1, &[PK, PK, PK], &[(CrossJoin, 1), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v <> b.w", 1, &[PK, PK, PK], &[(CrossJoin, 1), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a, b WHERE a.v <> b.w", 1, &[PK, PK, PK], &[(CrossJoin, 1), (Filter, 1)]),
    ("SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b WHERE a.v > 3", 1, &[PK, PK, PK], &[(CrossJoin, 1), (Filter, 1)]),
    ("SELECT c.v AS cv, b.id AS bid FROM c CROSS JOIN b", 1, &[PK, PK, PK], &[(CrossJoin, 1)]),
    ("SELECT c.v AS cv, ty.id AS tid FROM c NATURAL JOIN ty", 1, &[PK, PK, PK], &[(CrossJoin, 1)]),
    ("SELECT x.id AS xa, y.id AS yb FROM a x, a y", 2, &[PK, PK, PK], &[(CrossJoin, 1)]),
    // A self-join with a computed key cuts the computed side alone: its segment is a
    // second source.
    ("SELECT x.id AS xid, y.v FROM (SELECT id, v + 1 AS s FROM a) x JOIN a y ON x.s = y.k", 2, &[], &[(EquiJoin, 1)]),
    ("SELECT x.id AS xid, y.v FROM (SELECT id, v + 1 AS s FROM a) x LEFT JOIN a y ON x.s = y.k", 2, &[], &[(EquiJoin, 2), (Distinct, 1), (NullExtend, 1)]),
    ("SELECT a.id AS aid, b.id AS bid, u.id AS uid FROM a, b, u", 2, &[PK, PK, PK, PK, PK, PK], &[(CrossJoin, 2)]),
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
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", 1, &[], &[(RangeJoin, 1), (Reduce, 1)]),
    ("SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", 1, &[], &[(RangeJoin, 1), (Reduce, 1)]),
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
    // DISTINCT and set operations: leaves keyed by their element, each behind an
    // exchange on that key, then weight clamps; never a join.
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
    // A clamp only its sign is read through is left out: under EXCEPT's right,
    // under INTERSECT beside a set, under a UNION's clamp.
    ("SELECT g FROM t EXCEPT (SELECT g FROM u UNION SELECT v FROM a)", 1, &[PK, PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    ("SELECT g FROM t INTERSECT SELECT g FROM u UNION SELECT v FROM a", 1, &[PK, PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    // Beside a relaxed operand the other keeps its clamp.
    ("SELECT DISTINCT g FROM t INTERSECT ALL SELECT DISTINCT g FROM u", 1, &[PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    // An ALL operation over a set is a set.
    ("(SELECT id FROM t EXCEPT ALL SELECT id FROM u) EXCEPT SELECT id FROM a", 1, &[PK, PK, PK], &[(PositivePart, 2)]),
    // A DISTINCT operand, or a set operation under a projection, is in the circuit.
    ("SELECT DISTINCT g FROM t UNION ALL SELECT g FROM u", 1, &[PK, PK], &[(Distinct, 1)]),
    ("SELECT g FROM t EXCEPT SELECT DISTINCT g FROM u", 1, &[PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    ("SELECT * FROM (SELECT g FROM t UNION SELECT g FROM u) d WHERE g > 1", 1, &[PK, PK], &[(Distinct, 1), (Filter, 1)]),
    // Segments: a subquery and a grouped join side cut; a CTE or derived table
    // over one relation, a computed group key and a projection over a join do not.
    ("SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1), (NullExtend, 1)]),
    // Against a side of at most one row, the other side stays on its PK owner: only
    // the one-row side is broadcast, and neither integral is exchanged. Every pair
    // is made on the owner of its left PK, so a one-row right side needs no output
    // exchange either.
    ("SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)", 2, &[Some(&[])], &[(RangeJoin, 1), (Reduce, 1), (GlobalGround, 1)]),
    // A window's partition exists only while it holds a row, so its reduce owes
    // no ground row.
    ("SELECT a.id, SUM(a.v) OVER () AS s FROM a", 2, &[Some(&[])], &[(CrossJoin, 1), (Reduce, 1)]),
    ("SELECT a.id AS aid, s.m FROM a CROSS JOIN (SELECT MAX(w) AS m FROM b) s", 2, &[Some(&[])], &[(CrossJoin, 1), (Reduce, 1), (GlobalGround, 1)]),
    ("SELECT s.m, a.id AS aid FROM (SELECT MAX(w) AS m FROM b) s CROSS JOIN a", 2, &[PK, Some(&[])], &[(CrossJoin, 1), (Reduce, 1), (GlobalGround, 1)]),
    ("WITH agg AS (SELECT k, SUM(v) AS total FROM a GROUP BY k) SELECT b.w AS nm, agg.total AS tot FROM agg JOIN b ON agg.k = b.k", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1)]),
    ("SELECT d.id FROM (SELECT id, v FROM t WHERE v > 2) d", 1, &[], &[(Filter, 1)]),
    ("WITH c AS (SELECT id, v FROM t WHERE v > 1) SELECT id FROM c", 1, &[], &[(Filter, 1)]),
    // The WHERE levels a projection does not separate are one filter.
    ("WITH c AS (SELECT id, v FROM t WHERE v > 2) SELECT id FROM c WHERE id > 1", 1, &[], &[(Filter, 1)]),
    // A grouped CTE read once is its reader's own reduce, as its derived-table
    // spelling is.
    ("WITH c AS (SELECT g, SUM(v) AS s FROM t GROUP BY g) SELECT g FROM c WHERE s > 10", 1, &[Some(&[1])], &[(Reduce, 1), (Filter, 1)]),
    ("WITH c AS (SELECT * FROM t) SELECT g FROM c WHERE v = 5", 1, &[], &[(Filter, 1)]),
    ("SELECT g, SUM(v * 2) AS s FROM t WHERE v > 5 GROUP BY g", 1, &[Some(&[1])], &[(Reduce, 1), (Filter, 1)]),
    ("SELECT g + v AS k, SUM(g * v) AS s FROM t GROUP BY g + v HAVING SUM(g * v) > 3", 1, &[Some(&[1])], &[(Reduce, 1), (Filter, 1)]),
    ("SELECT k, COUNT(*) AS n FROM (SELECT id, g AS k FROM t) d GROUP BY k", 1, &[Some(&[1])], &[(Reduce, 1)]),
    ("SELECT DISTINCT g + 1 AS g1, v FROM t", 1, &[PK], &[(Distinct, 1)]),
    ("SELECT v * 2 AS x FROM t EXCEPT SELECT g FROM u", 1, &[PK, PK], &[(Distinct, 1), (PositivePart, 1)]),
    // Window: a whole-partition frame joins the source to one reduce over it; a
    // cumulative frame is one window over the source.
    ("SELECT id, SUM(v) OVER (PARTITION BY g) AS s FROM t", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1)]),
    ("SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM t", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1)]),
    ("SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s FROM t", 1, &[Some(&[2])], &[(Window, 1)]),
    ("SELECT id, SUM(v) OVER (ORDER BY v) AS s, RANK() OVER (ORDER BY v) AS r FROM t", 1, &[Some(&[])], &[(Window, 1)]),
    // One window per specification, each over the last one's rows; a
    // whole-partition frame beside them joins its reduce onto those rows.
    ("SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s, RANK() OVER (ORDER BY id) AS r FROM t", 2, &[Some(&[]), Some(&[2])], &[(Window, 2)]),
    ("SELECT id, RANK() OVER (PARTITION BY g ORDER BY v) AS r, SUM(v) OVER (PARTITION BY g) AS s FROM t", 3, &[Some(&[1]), Some(&[1])], &[(EquiJoin, 1), (Reduce, 1), (Window, 1)]),
    // An order key a whole-partition frame drops is not computed.
    ("SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY v + 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM t", 2, &[Some(&[1])], &[(EquiJoin, 1), (Reduce, 1)]),
    // A QUALIFY bounding an unprojected ROW_NUMBER is a top-N per partition,
    // cutting the outer side of whatever else the body joins in; a projected
    // one is a window's rank. The body's projection and filter run in the
    // top-N's own circuit, as a window's do in its.
    ("SELECT id, g FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) <= 2", 1, &[Some(&[1])], &[(TopN, 1)]),
    ("SELECT id, g FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) <= 2 AND v > 0", 1, &[Some(&[1])], &[(TopN, 1), (Filter, 1)]),
    ("SELECT id, SUM(v) OVER (PARTITION BY g) AS s FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) = 1", 3, &[Some(&[1]), Some(&[2])], &[(TopN, 1), (EquiJoin, 1), (Reduce, 1)]),
    ("SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn FROM t QUALIFY rn <= 2", 1, &[Some(&[1])], &[(Window, 1), (Filter, 1)]),
    // A cut beside a running value cuts the window's rows.
    ("SELECT id, RANK() OVER (PARTITION BY g ORDER BY v) AS r FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) <= 2", 2, &[Some(&[0]), Some(&[1])], &[(TopN, 1), (Window, 1)]),
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
        // A source's one scan keeps a bound its readers share and widens differing ones.
        (&on_ind, "SELECT g FROM t WHERE ind = 5 UNION ALL SELECT g FROM t WHERE ind = 6", &[]),
        (&on_ind, "SELECT g FROM t WHERE ind = 5 UNION ALL SELECT other FROM t WHERE ind = 5", &["[2]"]),
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
            .flat_map(|pv| pv.circuit.ops().map(|(_, op)| op))
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
        .flat_map(|pv| pv.circuit.ops().map(|(_, op)| op))
        .filter_map(|op| match op {
            OpNode::Reduce { group: g, .. } => Some((
                g.group_cols.clone(),
                g.aggs.iter().map(|d| d.agg_op).collect(),
                op.owes_ground_row(),
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
            .flat_map(|pv| pv.circuit.ops().map(|(_, op)| op))
            .filter_map(group_cols)
            .collect();
        assert!(!groups.is_empty(), "{body}: no reduce or top-N");
        assert!(groups.iter().all(|g| *g == want), "{body}: {groups:?}");
    }
}

/// Every scatter key of the final view, as `(source, the columns it states)`, in
/// circuit order.
fn scatter_keys(chain: &ViewBundle) -> Vec<(u64, Vec<u32>)> {
    (final_view(chain).circuit.ops())
        .filter_map(|(_, op)| match op {
            OpNode::ScanDelta { source, scatter: Some(key), .. } => Some((*source, key.iter().map(|s| s.0).collect())),
            _ => None,
        })
        .collect()
}

const A: u64 = 18;
const B: u64 = 19;

/// A side states the slots its delta is routed by, in its source's own columns:
/// an equi join's whole key, a band's equality prefix, none for a pure range or
/// a cross join — and the whole source PK for the outer side of an EXISTS over a
/// pure range, which is owned before it is joined, and for a side against one of
/// at most one row, which alone is broadcast, on whichever side it sits.
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
        // A derived side whose `Map` moves the key off its source slot.
        ("SELECT x.k AS k, x.s AS s, b.w FROM (SELECT v + 1 AS s, k FROM a) x JOIN b ON x.k = b.k", &[1], &[1]),
        ("SELECT x.k AS k, x.k2 AS k2, b.w FROM (SELECT k, k AS k2 FROM a) x JOIN b ON x.k2 = b.k", &[1], &[1]),
        // The one-row side reads a segment of `b`.
        ("SELECT s.m, a.id AS aid FROM (SELECT MAX(w) AS m FROM b) s CROSS JOIN a", &[0], &[]),
        ("SELECT s.m, a.id AS aid FROM (SELECT MAX(w) AS m FROM b) s JOIN a ON s.m > a.v", &[0], &[]),
        ("SELECT a.id AS aid, s.m FROM a CROSS JOIN (SELECT MAX(w) AS m FROM b) s", &[0], &[]),
        ("SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)", &[0], &[]),
    ];
    for &(body, a, other) in rows {
        let keys = scatter_keys(&view(&cat, body));
        let (on_a, on_other): (Vec<_>, Vec<_>) = keys.iter().partition(|k| k.0 == A);
        for (side, stated, want) in [("a", on_a, a), ("the other side", on_other, other)] {
            assert!(!stated.is_empty(), "`{body}`: {side} states no key");
            assert!(stated.iter().all(|k| k.1 == want), "`{body}`: {side} states {stated:?}");
        }
    }
}

/// A preserved side's NULL-keyed rows are unmatched rows, so it is re-keyed a
/// second time keeping them — only where a key column can be NULL at all.
#[test]
fn the_null_keeping_rekey_is_emitted_only_over_a_nullable_key() {
    let cat = cat();
    let n = 20;
    // The re-keys of `source`'s rows as scanned, through filters and projections.
    let rekeys = |body: &str, source: u64| {
        use gnitz_wire::MapKind;
        let chain = view(&cat, body);
        let circuit = &final_view(&chain).circuit;
        let scanned = |mut at: gnitz_wire::NodeId| loop {
            match circuit.op(at) {
                OpNode::ScanDelta { source, .. } => return Some(*source),
                OpNode::Filter(_) | OpNode::Map(MapKind::Compute(_)) => at = circuit.inputs(at)[0],
                _ => return None,
            }
        };
        (circuit.ops())
            .filter(|(at, op)| {
                matches!(op, OpNode::Map(MapKind::Reindex { .. })) && scanned(circuit.inputs(*at)[0]) == Some(source)
            })
            .count()
    };
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

/// A set operation is keyed by the PK its leaves carry where that keeps the
/// result's uniqueness, else by its columns where they form a narrow PK, else by a hash.
#[test]
fn a_set_op_is_keyed_by_the_key_its_leaves_carry() {
    let cat = cat();
    let maps = |chain: &ViewBundle, kind: fn(&gnitz_wire::MapKind) -> bool| {
        all_views(chain).any(|pv| pv.circuit.ops().any(|(_, op)| matches!(op, OpNode::Map(m) if kind(m))))
    };
    let hashes = |chain: &ViewBundle| maps(chain, |m| matches!(m, gnitz_wire::MapKind::HashRow { .. }));
    let reindexes = |chain: &ViewBundle| maps(chain, |m| matches!(m, gnitz_wire::MapKind::Reindex { .. }));
    // (body, key columns of the view with no hidden key, PK repeats)
    #[rustfmt::skip]
    let carried: &[(&str, &[u32], bool)] = &[
        ("SELECT * FROM t EXCEPT SELECT * FROM u", &[0], false),
        ("SELECT id, g FROM t INTERSECT ALL SELECT id, g FROM u", &[0], false),
        ("SELECT * FROM t EXCEPT ALL SELECT * FROM u", &[0], false),
        ("SELECT * FROM t UNION ALL SELECT * FROM u", &[0], true),
        // The whole result is the key.
        ("SELECT id FROM t UNION SELECT id FROM u", &[0], false),
        // The key need not lead the SELECT list; a filter keeps the source's rows.
        ("SELECT g, id FROM t INTERSECT SELECT g, id FROM u WHERE v > 1", &[1], false),
        ("SELECT * FROM t WHERE v > 1 EXCEPT ALL SELECT * FROM t WHERE v < 9", &[0], false),
        ("SELECT * FROM c EXCEPT SELECT * FROM c WHERE v > 3", &[0, 1], false),
        ("SELECT * FROM t EXCEPT SELECT * FROM u EXCEPT SELECT * FROM t WHERE g = 2", &[0], false),
        // A second copy of a key column is a payload column.
        ("SELECT id, id AS j FROM t EXCEPT SELECT id, id AS j FROM u", &[0], false),
        // Any source carries its key: a view, a system table.
        ("SELECT * FROM t EXCEPT SELECT * FROM tv", &[0], false),
        ("SELECT * FROM t INTERSECT SELECT * FROM sys", &[0], false),
    ];
    for &(body, pk, repeats) in carried {
        let chain = view(&cat, body);
        let v = final_view(&chain);
        assert!(!hashes(&chain) && !reindexes(&chain), "`{body}` re-keys");
        assert!(v.schema.columns().iter().all(|c| !c.is_hidden), "`{body}`");
        assert_eq!(v.schema.pk_cols(), pk, "`{body}`");
        assert_eq!(v.pk_repeats, repeats, "`{body}`");
    }
    // No leaf carries its PK, not all of it, or not at one position or type; the
    // columns form a narrow key.
    for body in [
        "SELECT g FROM t EXCEPT SELECT g FROM u",
        "SELECT id, g FROM t EXCEPT SELECT g, id FROM u",
        "SELECT a, v FROM c EXCEPT SELECT a, v FROM c WHERE v > 3",
        // A widened column.
        "SELECT id, i32c FROM ty EXCEPT SELECT id, v FROM t",
    ] {
        let chain = view(&cat, body);
        let v = final_view(&chain);
        assert!(
            reindexes(&chain) && !hashes(&chain),
            "`{body}` is not keyed by its columns"
        );
        assert!(v.schema.columns().iter().all(|c| !c.is_hidden), "`{body}`");
        assert_eq!(v.schema.pk_cols().len(), v.schema.columns().len(), "`{body}`");
        assert!(!v.pk_repeats, "`{body}`");
    }
    for body in [
        // Two elements under one carried key would leave the result's key
        // repeating, and the columns are too wide a key.
        "SELECT * FROM t UNION SELECT * FROM u",
        // A nullable column, a STRING, a UUID.
        "SELECT k FROM n EXCEPT SELECT g FROM t",
        "SELECT s FROM ty EXCEPT SELECT s FROM ty WHERE id > 1",
        "SELECT uid FROM ty UNION SELECT uid FROM ty WHERE id > 1",
    ] {
        let chain = view(&cat, body);
        assert!(hashes(&chain), "`{body}` is not hashed");
        assert!(!final_view(&chain).pk_repeats, "`{body}`");
    }
}
