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

/// One row of the shape matrix, in column order:
/// body, segments, exchanges (shard cols per `ExchangeShard`, any order),
/// equi joins, range joins, reduces, of which global-ground, distinct,
/// positive_part, worker_filter, null_extend, filter.
type Row = (
    &'static str,
    usize,
    &'static [&'static [u32]],
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
);

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
        .flat_map(|pv| pv.circuit.nodes.values())
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
    ("SELECT id, v FROM t", 1, &[], 0, 0, 0, 0, 0, 0, 0, 0, 0),
    ("SELECT id, v * 2 AS d FROM t WHERE v > 5", 1, &[], 0, 0, 0, 0, 0, 0, 0, 0, 1),
    // Equi join: two join terms, no exchange; the null-fill is a clamp and a
    // null-extend per preserved side, never a distinct.
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k", 1, &[], 2, 0, 0, 0, 0, 0, 0, 0, 0),
    ("SELECT a.id AS aid, b.w FROM a LEFT JOIN b ON a.k = b.k", 1, &[], 2, 0, 0, 0, 0, 1, 0, 1, 0),
    ("SELECT a.id AS aid, b.w FROM a RIGHT JOIN b ON a.k = b.k", 1, &[], 2, 0, 0, 0, 0, 1, 0, 1, 0),
    ("SELECT a.id AS aid, b.w FROM a FULL JOIN b ON a.k = b.k", 1, &[], 2, 0, 0, 0, 0, 2, 0, 2, 0),
    // A residual or WHERE is one filter; a nullable key is one filter per side.
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k AND a.v <> b.w", 1, &[], 2, 0, 0, 0, 0, 0, 0, 0, 1),
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k AND (a.v > 5 OR b.w < 3)", 1, &[], 2, 0, 0, 0, 0, 0, 0, 0, 1),
    ("SELECT a.id AS aid, b.w FROM a JOIN b ON a.k = b.k WHERE a.v > 5", 1, &[], 2, 0, 0, 0, 0, 0, 0, 0, 1),
    ("SELECT n.id AS nid, b.w FROM n JOIN b ON n.k = b.k", 1, &[], 2, 0, 0, 0, 0, 0, 0, 0, 1),
    ("SELECT a.id AS aid, n.v FROM a JOIN n ON a.k = n.k", 1, &[], 2, 0, 0, 0, 0, 0, 0, 0, 1),
    ("SELECT n.id AS nid, b.w FROM n LEFT JOIN b ON n.k = b.k", 1, &[], 2, 0, 0, 0, 0, 1, 0, 1, 1),
    ("SELECT n.id AS nid, m.v FROM n LEFT JOIN m ON n.k = m.k", 1, &[], 2, 0, 0, 0, 0, 1, 0, 1, 2),
    // Band join: two range terms, one pair-PK output exchange, no worker filter.
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[&[0, 1]], 0, 2, 0, 0, 0, 0, 0, 0, 0),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[&[0, 1]], 0, 2, 0, 0, 0, 1, 0, 1, 0),
    ("SELECT a.id AS aid, b.id AS bid FROM a RIGHT JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[&[0, 1]], 0, 2, 0, 0, 0, 1, 0, 1, 0),
    ("SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.k = b.k AND a.v <= b.w", 1, &[&[0, 1]], 0, 2, 0, 0, 0, 2, 0, 2, 0),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.k = b.k AND a.v <= b.w WHERE a.id > 5", 1, &[&[0, 1]], 0, 2, 0, 0, 0, 1, 0, 1, 1),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v < b.w AND a.id > b.id", 1, &[&[0, 1]], 0, 2, 0, 0, 0, 0, 0, 0, 1),
    // Pure range: broadcast trimmed by one worker filter per side; LEFT derives
    // its null-fill from a threshold reduce, so no clamp.
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w", 1, &[&[0, 1]], 0, 2, 0, 0, 0, 0, 2, 0, 0),
    ("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.v < b.w", 1, &[&[0, 1]], 0, 4, 1, 0, 0, 0, 2, 1, 0),
    ("SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v < b.w AND a.id <> b.id", 1, &[&[0, 1]], 0, 2, 0, 0, 0, 0, 2, 0, 1),
    // Semi / anti / mark / IN: a join plus clamp arithmetic, no exchange, no distinct.
    ("SELECT a.v FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)", 1, &[], 2, 0, 0, 0, 0, 1, 0, 0, 0),
    ("SELECT a.v FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)", 1, &[], 2, 0, 0, 0, 0, 1, 0, 0, 0),
    ("SELECT id FROM a WHERE k IN (SELECT k FROM b)", 1, &[], 2, 0, 0, 0, 0, 1, 0, 0, 0),
    ("SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AS flag FROM a", 1, &[], 2, 0, 0, 0, 0, 1, 0, 0, 0),
    ("SELECT id FROM n WHERE EXISTS (SELECT 1 FROM m WHERE m.k = n.k)", 1, &[], 2, 0, 0, 0, 0, 1, 0, 0, 2),
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w < a.v)", 1, &[&[0]], 0, 2, 0, 0, 0, 1, 0, 0, 0),
    ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", 1, &[&[0]], 0, 2, 1, 0, 0, 0, 1, 0, 0),
    ("SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", 1, &[&[0]], 0, 2, 1, 0, 0, 0, 1, 0, 0),
    // Reduce: grouped shards on the group columns; a global aggregate funnels
    // through an empty-key exchange with a ground row, two-phase when linear;
    // a replicated source needs no exchange at all.
    ("SELECT g, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY g", 1, &[&[1]], 0, 0, 1, 0, 0, 0, 0, 0, 0),
    ("SELECT a AS ka, b AS kb, COUNT(*) AS n, SUM(v) AS s FROM c GROUP BY a, b", 1, &[&[0, 1]], 0, 0, 1, 0, 0, 0, 0, 0, 0),
    ("SELECT g, SUM(v) AS s FROM t GROUP BY g HAVING SUM(v) > 10", 1, &[&[1]], 0, 0, 1, 0, 0, 0, 0, 0, 1),
    ("SELECT g + v AS k, SUM(g * v) AS s FROM t GROUP BY g + v", 1, &[&[3]], 0, 0, 1, 0, 0, 0, 0, 0, 0),
    ("SELECT SUM(v) AS s, MIN(v) AS mn, MAX(v) AS mx FROM t", 1, &[&[]], 0, 0, 1, 1, 0, 0, 0, 0, 0),
    ("SELECT SUM(v) AS s, COUNT(*) AS c FROM t", 1, &[&[]], 0, 0, 2, 1, 0, 0, 0, 0, 0),
    ("SELECT v, COUNT(*) AS n FROM r GROUP BY v", 1, &[], 0, 0, 1, 0, 0, 0, 0, 0, 0),
    // DISTINCT and set operations: content-hashed leaves behind an exchange on
    // the hash key, then weight clamps; never a join.
    ("SELECT DISTINCT g FROM t", 1, &[&[0]], 0, 0, 0, 0, 1, 0, 0, 0, 0),
    ("SELECT g FROM t UNION SELECT g FROM u", 1, &[&[0], &[0]], 0, 0, 0, 0, 1, 0, 0, 0, 0),
    ("SELECT g FROM t UNION ALL SELECT g FROM u", 1, &[&[0], &[0]], 0, 0, 0, 0, 0, 0, 0, 0, 0),
    ("SELECT g FROM t EXCEPT SELECT g FROM u", 1, &[&[0], &[0]], 0, 0, 0, 0, 2, 1, 0, 0, 0),
    ("SELECT g FROM t INTERSECT SELECT g FROM u", 1, &[&[0], &[0]], 0, 0, 0, 0, 2, 1, 0, 0, 0),
    ("SELECT g FROM t EXCEPT ALL SELECT g FROM u", 1, &[&[0], &[0]], 0, 0, 0, 0, 0, 1, 0, 0, 0),
    ("SELECT g FROM t INTERSECT ALL SELECT g FROM u", 1, &[&[0], &[0]], 0, 0, 0, 0, 0, 1, 0, 0, 0),
    // Segments: a derived table, a non-trivial CTE and a subquery each cut a
    // materialized segment; a computed group key or aggregate argument does not.
    ("SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a", 3, &[&[1]], 2, 0, 1, 0, 0, 1, 0, 1, 0),
    ("SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)", 2, &[&[], &[0, 1]], 0, 2, 1, 1, 0, 0, 2, 0, 1),
    ("WITH agg AS (SELECT k, SUM(v) AS total FROM a GROUP BY k) SELECT b.w AS nm, agg.total AS tot FROM agg JOIN b ON agg.k = b.k", 2, &[&[1]], 2, 0, 1, 0, 0, 0, 0, 0, 0),
    ("SELECT d.id FROM (SELECT id, v FROM t WHERE v > 2) d", 2, &[], 0, 0, 0, 0, 0, 0, 0, 0, 1),
    ("WITH c AS (SELECT id, v FROM t WHERE v > 1) SELECT id FROM c", 2, &[], 0, 0, 0, 0, 0, 0, 0, 0, 1),
    ("WITH c AS (SELECT * FROM t) SELECT g FROM c WHERE v = 5", 1, &[], 0, 0, 0, 0, 0, 0, 0, 0, 1),
    ("SELECT g, SUM(v * 2) AS s FROM t WHERE v > 5 GROUP BY g", 1, &[&[1]], 0, 0, 1, 0, 0, 0, 0, 0, 1),
    ("SELECT g + v AS k, SUM(g * v) AS s FROM t GROUP BY g + v HAVING SUM(g * v) > 3", 1, &[&[3]], 0, 0, 1, 0, 0, 0, 0, 0, 1),
];

#[test]
fn every_shape_has_its_contract_nodes() {
    let cat = cat();
    let mut mismatches = Vec::new();
    for &(body, segments, exch, joins, range_joins, reduces, ground, distinct, pos, wf, null_ext, filter) in SHAPES {
        let chain = view(&cat, body);
        let n = |pred: fn(&OpNode) -> bool| total(&chain, pred);
        let got = (
            chain.views.len(),
            exchanges(&chain),
            n(|op| matches!(op, OpNode::Join(JoinKind::DeltaTrace))),
            n(|op| matches!(op, OpNode::Join(JoinKind::DeltaTraceRange { .. }))),
            n(|op| matches!(op, OpNode::Reduce { .. })),
            n(|op| {
                matches!(
                    op,
                    OpNode::Reduce {
                        global_ground: true,
                        ..
                    }
                )
            }),
            n(|op| matches!(op, OpNode::Distinct)),
            n(|op| matches!(op, OpNode::PositivePart)),
            n(|op| matches!(op, OpNode::WorkerFilter)),
            n(|op| matches!(op, OpNode::NullExtend { .. })),
            n(|op| matches!(op, OpNode::Filter(_))),
        );
        let mut want_exch: Vec<Vec<u32>> = exch.iter().map(|e| e.to_vec()).collect();
        want_exch.sort();
        let want = (
            segments,
            want_exch,
            joins,
            range_joins,
            reduces,
            ground,
            distinct,
            pos,
            wf,
            null_ext,
            filter,
        );
        if got != want {
            mismatches.push(format!("`{body}`\n     got {got:?}\n    want {want:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "(segments, exchanges, joins, range_joins, reduces, global_ground, distinct, positive_part, \
         worker_filter, null_extend, filter)\n{}",
        mismatches.join("\n")
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
            .flat_map(|pv| pv.circuit.nodes.values())
            .filter_map(|op| match op {
                OpNode::Map(MapKind::HashRow(_, _, branch)) => Some(*branch),
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
    let rows: &[(&CatalogSnapshot, &str, Option<&[u32]>)] = &[
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 GROUP BY g", Some(&[2])),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE ind BETWEEN 5 AND 9 GROUP BY g", Some(&[2])),
        (&on_ind_other, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 AND other > 10 GROUP BY g", Some(&[2, 3])),
        (&on_ind, "SELECT g, other FROM t WHERE ind = 5", Some(&[2])),
        (&on_ind, "WITH c AS (SELECT * FROM t) SELECT g, COUNT(*) AS n FROM c WHERE ind = 5 GROUP BY g", Some(&[2])),
        (&on_ind, "SELECT g, COUNT(*) AS n FROM (SELECT * FROM t) d WHERE ind = 5 GROUP BY g", None),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE other = 5 GROUP BY g", None),
        (&on_ind, "SELECT DISTINCT g FROM t WHERE ind = 5", None),
        (&on_ind, "SELECT g, COUNT(*) AS c FROM t WHERE pk = 5 GROUP BY g", None),
        (&on_ind, "SELECT t.g, COUNT(*) AS c FROM t JOIN u ON t.pk = u.pk WHERE t.ind = 5 GROUP BY t.g", None),
        (&unindexed, "SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 GROUP BY g", None),
    ];
    for &(cat, body, want) in rows {
        let chain = view(cat, body);
        let bounds: Vec<Vec<u32>> = chain
            .views
            .iter()
            .flat_map(|pv| pv.circuit.nodes.values())
            .filter_map(|op| match op {
                OpNode::ScanDelta { bound: Some(b), .. } => Some(b.idx_cols.as_slice().to_vec()),
                _ => None,
            })
            .collect();
        let want: Vec<Vec<u32>> = want.into_iter().map(|w| w.to_vec()).collect();
        assert_eq!(bounds, want, "`{body}`");
    }
}
