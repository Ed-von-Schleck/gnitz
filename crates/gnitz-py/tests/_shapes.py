"""One catalogue of view shapes, for the sweeps that hold the shape fixed and
vary *when* a view meets its rows: created over rows already there
(`schema_lifetime/test_view_creation_order.py`), or maintained and then carried
across a process boundary (`state_lifetime/test_view_rebuild.py`).

Each shape is `(tables, rows, views)`: the `CREATE TABLE`s, the statements that
fill them, and `{view: (body, columns read back, expected bag)}` in creation
order. Every expectation is a weighted bag, recomputed here from the rows the
shape writes: the failures these sweeps exist for — a backfill and a deferred
tick both delivering a row, an exchange closure re-derived once per (view,
source) pair, a broadcast replayed per worker — leave the row set right and only
the weights wrong.

Row counts stay small on purpose: a few dozen rows over four workers is a
handful per partition, which is enough for unequal partitions and for every
aggregate to be non-trivial.

A shape names only its own tables, and the sweeps give each a schema of its
own. That isolation matters for `join`: neither of its bases roots any other
exchange view, which is what once left an `ExchangeShard`-only classifier with
an empty base set and the join a deterministic per-key prefix at W>1.
"""
from collections import Counter

from _sql import values


def _grouped(rows, key, *vals):
    """`{(key, *sums): 1}` — what a GROUP BY view must equal, each `vals` entry
    a row -> number summed over the group."""
    acc = {}
    for r in rows:
        acc.setdefault(key(r), [0] * len(vals))
        for i, v in enumerate(vals):
            acc[key(r)][i] += v(r)
    return {(k, *v): 1 for k, v in acc.items()}


def setop(op, left, right):
    """`op` over two Counters of tuples: the bag operators are Counter's own
    arithmetic, the deduplicating ones decide membership on positive weight."""
    return {
        "UNION ALL": left + right,
        "EXCEPT ALL": left - right,
        "INTERSECT ALL": left & right,
        "UNION": dict.fromkeys(left | right, 1),
        "INTERSECT": dict.fromkeys(left & right, 1),
        "EXCEPT": dict.fromkeys(left.keys() - right.keys(), 1),
    }[op]


# Every operator in both quantifiers, by the suffix its view carries.
ONE_SOURCE_OPS = {
    "ex": "EXCEPT", "in": "INTERSECT", "ua": "UNION ALL", "u": "UNION",
    "ea": "EXCEPT ALL", "ia": "INTERSECT ALL",
}


def _table(name, cols, rows):
    """`(CREATE TABLE, INSERT)` for `name(id BIGINT PK, cols... BIGINT NOT NULL)`."""
    ddl = f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY" + "".join(
        f", {c} BIGINT NOT NULL" for c in cols) + ")"
    return ddl, f"INSERT INTO {name} VALUES {values(rows)}"


def _over(*tables):
    """The `tables` and `rows` slots for several `_table`s."""
    return "; ".join(t[0] for t in tables), "; ".join(t[1] for t in tables)


# `a(id, k, av)`: 30 rows over 5 keys. `b(id, bv)`: one row per key.
_A = [(i, i % 5, i * 10) for i in range(30)]
_B = [(j, j * 100) for j in range(5)]
_TA, _TB = _table("a", ("k", "av"), _A), _table("b", ("bv",), _B)
_BV = dict(_B)


def _band(i):
    return i * 10 < (i % 5) * 100


def _a(views):
    return (*_over(_TA), views)


def _ab(views):
    return (*_over(_TA, _TB), views)


_PAIR = "SELECT a.id AS aid, b.id AS bid FROM a"

_SB = [(i, i % 6, i + 1) for i in range(24)]
_GG = [(i, i % 2, (i % 2) + 1) for i in range(20)]
_GP = [(i, i % 5) for i in range(40)]
_PG = [(i, i % 3, i + 1) for i in range(24)]
_SIB = [(i, i % 4, i) for i in range(24)]
_NEST = [(i, i * 10) for i in range(12)]
_DIA = [(i, i % 4, i + 1) for i in range(24)]
_DIA_SUM = {g: s for (g, s) in _grouped(_DIA, lambda r: r[1], lambda r: r[2])}
# pk 0, group 0's MIN holder, is deleted again.
_MM = [(i, i % 6, (i * 7) % 50) for i in range(24)]
_MM_LIVE = _MM[1:]
# Both carriers of b.val = 15 are deleted again.
_SA = [(i, i % 20) for i in range(40)]
_SB_SET = [(i, (i % 20) + 10) for i in range(40) if i not in (5, 25)]
_VA, _VB = Counter((v,) for _, v in _SA), Counter((v,) for _, v in _SB_SET)
_DT = [(i, i % 7) for i in range(42)]
_CB = [(a, b, a * 100 + b) for a in range(1, 9) for b in range(1, 5)]
_FT = [(i, i % 4, (100 if i <= 18 else 5000)) for i in range(1, 25)]
_GT = "CREATE TABLE gt (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)"


def _ks(keep):
    return Counter((k,) for _, k, _ in _A if keep(k))


def _by_group(rows):
    out = {}
    for _, g, v in rows:
        out.setdefault(g, []).append(v)
    return out


SHAPES = {
    "proj": _a({"v": ("SELECT id, av + 1 AS p FROM a", ("id", "p"), {(i, av + 1): 1 for i, _, av in _A})}),

    # ── joins ───────────────────────────────────────────────────────────────
    # The minimal two-base equi-join, on bases nothing else reads.
    "join": _ab({"v": ("SELECT a.id AS aid, a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.id",
                       ("aid", "av", "bv"), {(i, av, _BV[k]): 1 for i, k, av in _A})}),
    "joins_sharing_a_base": (
        *_over(_TA, _TB, _table("c", ("cv",), _B)),
        {"x": ("SELECT a.id AS aid, a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.id",
               ("aid", "av", "bv"), {(i, av, _BV[k]): 1 for i, k, av in _A}),
         "y": ("SELECT a.id AS aid, a.av AS av, c.cv AS cv FROM a JOIN c ON a.k = c.id",
               ("aid", "av", "cv"), {(i, av, _BV[k]): 1 for i, k, av in _A})}),
    "band": _ab({"v": (f"{_PAIR} JOIN b ON a.k = b.id AND a.av < b.bv",
                       ("aid", "bid"), {(i, k): 1 for i, k, _ in _A if _band(i)})}),
    "band_left": _ab({"v": (f"{_PAIR} LEFT JOIN b ON a.k = b.id AND a.av < b.bv",
                            ("aid", "bid"), {(i, k if _band(i) else None): 1 for i, k, _ in _A})}),
    # Every `k = 4` row is past the threshold, so the LEFT form null-fills six rows.
    "range": _ab({"v": (f"{_PAIR} JOIN b ON a.k < b.id",
                        ("aid", "bid"), {(i, j): 1 for i, k, _ in _A for j, _ in _B if k < j})}),
    "range_left": _ab({"v": (f"{_PAIR} LEFT JOIN b ON a.k < b.id", ("aid", "bid"),
                             {(i, j): 1 for i, k, _ in _A for j, _ in _B if k < j}
                             | {(i, None): 1 for i, k, _ in _A if k == 4})}),
    # A keyless step broadcasts its delta instead of exchanging it, so a
    # broadcast replayed per worker returns a W-fold duplication.
    "cross": _ab({"v": (f"{_PAIR} CROSS JOIN b", ("aid", "bid"), {(i, j): 1 for i, *_ in _A for j, _ in _B})}),
    # The repeated occurrence is wrapped in a pass-through that must seed first.
    "self_join": _a({"v": ("SELECT x.id AS xid, y.id AS yid FROM a x JOIN a y ON x.k = y.id",
                           ("xid", "yid"), {(i, k): 1 for i, k, _ in _A})}),
    # `x` reaches `a` by two paths; within one drive it is evaluated once per
    # incoming edge, which is correct multi-input behaviour and not a re-run.
    "diamond": (
        *_over(_table("a", ("g", "val"), _DIA)),
        {"v1": ("SELECT g, SUM(val) AS s FROM a GROUP BY g", ("g", "s"), dict.fromkeys(_DIA_SUM.items(), 1)),
         "x": ("SELECT a.id AS id, a.g AS g, v1.s AS s FROM a JOIN v1 ON a.g = v1.g",
               ("id", "g", "s"), {(i, g, _DIA_SUM[g]): 1 for i, g, _ in _DIA})}),

    # ── subqueries ──────────────────────────────────────────────────────────
    "exists": _ab({"v": ("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.bv = a.av)",
                         ("id",), {(i,): 1 for i in (0, 10, 20)})}),
    "not_exists": _ab({"v": ("SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.bv = a.av)",
                             ("id",), {(i,): 1 for i, *_ in _A if i not in (0, 10, 20)})}),
    "correlated_scalar": _ab({"v": ("SELECT b.id AS id, (SELECT COUNT(*) FROM a WHERE a.k = b.id) AS c FROM b",
                                    ("id", "c"), {(j, 6): 1 for j, _ in _B})}),
    "uncorrelated_scalar": _ab({"v": ("SELECT id FROM b WHERE b.bv < (SELECT MAX(av) FROM a)",
                                      ("id",), {(0,): 1, (1,): 1, (2,): 1})}),

    # ── reduces ─────────────────────────────────────────────────────────────
    # A base feeding several exchange views must be driven once, not once per
    # view — the doubling lands as weight 2 on the same group row.
    "shared_base_groupby": (
        *_over(_table("t", ("g", "val"), _SB)),
        {"vx1": ("SELECT g, SUM(val) AS s FROM t GROUP BY g", ("g", "s"),
                 _grouped(_SB, lambda r: r[1], lambda r: r[2])),
         "vx2": ("SELECT g, SUM(id) AS s FROM t GROUP BY g", ("g", "s"),
                 _grouped(_SB, lambda r: r[1], lambda r: r[0]))}),
    # An exchange view over another: driving the base fills the chain
    # transitively, and driving the intermediate as a source too would
    # double-fill the top view in a hashmap-order-dependent way. The decoys
    # perturb that order.
    "groupby_over_groupby": (
        *_over(_table("a", ("grp", "val"), _GG)),
        {"decoy1": ("SELECT grp, COUNT(*) AS c FROM a GROUP BY grp", ("grp", "c"), {(0, 10): 1, (1, 10): 1}),
         "v": ("SELECT grp, SUM(val) AS s FROM a GROUP BY grp", ("grp", "s"), {(0, 10): 1, (1, 20): 1}),
         "decoy2": ("SELECT s, COUNT(*) AS c FROM v GROUP BY s", ("s", "c"), {(10, 1): 1, (20, 1): 1}),
         "g": ("SELECT s, COUNT(*) AS c FROM v GROUP BY s", ("s", "c"), {(10, 1): 1, (20, 1): 1})}),
    # `n` is cascade-reachable through `x`, so a worker's non-exchange pass must
    # skip it: weight exactly 1 per row is what an inline open-time backfill
    # doubles.
    "groupby_over_projection": (
        *_over(_table("a", ("val",), _GP)),
        {"n": ("SELECT id, val + 1 AS p1 FROM a", ("id", "p1"), {(i, v + 1): 1 for i, v in _GP}),
         "x": ("SELECT p1, COUNT(*) AS c FROM n GROUP BY p1", ("p1", "c"), {(v + 1, 8): 1 for v in range(5)})}),
    "projection_over_groupby": (
        *_over(_table("t", ("g", "val"), _PG)),
        {"vx": ("SELECT g, SUM(val) AS s FROM t GROUP BY g", ("g", "s"),
                _grouped(_PG, lambda r: r[1], lambda r: r[2])),
         "vn": ("SELECT g, s + 1 AS s1 FROM vx", ("g", "s1"),
                {(g, s + 1): 1 for g, s in _grouped(_PG, lambda r: r[1], lambda r: r[2])})}),
    "projection_sibling_of_groupby": (
        *_over(_table("t", ("g", "val"), _SIB)),
        {"vn": ("SELECT id, val + 1 AS plus1 FROM t", ("id", "plus1"), {(i, v + 1): 1 for i, _, v in _SIB}),
         "vx": ("SELECT g, SUM(val) AS s FROM t GROUP BY g", ("g", "s"),
                _grouped(_SIB, lambda r: r[1], lambda r: r[2]))}),
    # Both views are cascade-unreachable, so the depth sort must fill the inner
    # one first or the outer reads an empty source.
    "nested_projections": (
        *_over(_table("t", ("val",), _NEST)),
        {"v1": ("SELECT id, val + 1 AS a FROM t", ("id", "a"), {(i, v + 1): 1 for i, v in _NEST}),
         "v2": ("SELECT id, a + 1 AS b FROM v1", ("id", "b"), {(i, v + 2): 1 for i, v in _NEST})}),
    # The DELETE removes group 0's MIN holder, so the live trace carries a
    # MIN-retraction recompute. AVG is one exact division, so it is bit-stable.
    "groupby_min_max_avg": (
        _table("t", ("g", "a"), _MM)[0],
        _table("t", ("g", "a"), _MM)[1] + "; DELETE FROM t WHERE id = 0",
        {"v": ("SELECT g, MIN(a) AS lo, MAX(a) AS hi, AVG(a) AS av, COUNT(*) AS c FROM t GROUP BY g",
               ("g", "lo", "hi", "av", "c"),
               {(g, min(vs), max(vs), sum(vs) / len(vs), len(vs)): 1 for g, vs in _by_group(_MM_LIVE).items()})}),
    # One join-output key spans six groups, so each group's MIN must isolate.
    "min_over_fanout_join": _ab({"v": (
        "SELECT a.av AS g, MIN(a.id) AS lo, MAX(b.bv) AS hi FROM b JOIN a ON b.id = a.k GROUP BY a.av",
        ("g", "lo", "hi"), {(av, i, _BV[k]): 1 for i, k, av in _A})}),
    "reduce_into_join": _ab({"v": (
        "WITH s AS (SELECT k, SUM(av) AS t FROM a GROUP BY k) SELECT b.bv AS bv, s.t AS t FROM s JOIN b ON s.k = b.id",
        ("bv", "t"), {(_BV[k], t): 1 for k, t in _grouped(_A, lambda r: r[1], lambda r: r[2])})}),
    # A contiguous low-id range fails the predicate on every worker's first
    # chunks, so the exchanged pre-phase payload for those rounds is empty.
    # Inferring "done" from an empty round truncates the view.
    "filtered_groupby": (
        *_over(_table("ft", ("category", "amount"), _FT)),
        {"fv": ("SELECT category, COUNT(*) AS cnt, SUM(amount) AS total FROM ft WHERE amount > 1000 GROUP BY category",
                ("category", "cnt", "total"),
                _grouped([r for r in _FT if r[2] > 1000], lambda r: r[1], lambda r: 1, lambda r: r[2]))}),
    # A linear final whose delta source is an in-bundle hidden segment that
    # exchanges — grouped, joined, or a linear segment between two grouped ones —
    # so the segment must be filled before the final reads it.
    "grouped_cte": (
        *_over(_table("t", ("v",), [(1, 20), (2, 5), (3, 30)])),
        {"v": ("WITH c AS (SELECT id, SUM(v) AS sv FROM t GROUP BY id) SELECT id FROM c WHERE sv > 10",
               ("id",), {(1,): 1, (3,): 1})}),
    "join_cte": (
        *_over(_table("ja", ("k",), [(1, 10), (2, 20), (3, 10)]), _table("jb", ("x",), [(10, 5), (20, 0)])),
        {"v": ("WITH c AS (SELECT ja.id AS id, jb.x AS x FROM ja JOIN jb ON ja.k = jb.id) "
               "SELECT id FROM c WHERE x > 0", ("id",), {(1,): 1, (3,): 1})}),
    "linear_between": (
        *_over(_table("t", ("v",), [(1, 20), (2, 5), (3, 20), (4, 30)])),
        {"v": ("WITH g AS (SELECT id, SUM(v) AS sv FROM t GROUP BY id), "
               "l AS (SELECT id, sv FROM g WHERE sv > 10) SELECT sv, COUNT(*) AS c FROM l GROUP BY sv",
               ("sv", "c"), {(20, 2): 1, (30, 1): 1})}),

    # A global (ungrouped) aggregate's ground row: exactly one row, on the
    # partition the constant key routes to rather than on every worker —
    # over a source never populated, one emptied again, and a replicated one,
    # which elects its ground-row owner without the partition route.
    "global_agg_never_populated": (
        _GT, "", {"gv": ("SELECT COUNT(*) AS cnt, SUM(a) AS total FROM gt", ("cnt", "total"), {(0, None): 1})}),
    "global_agg_emptied": (
        _GT, "INSERT INTO gt VALUES (1, 5), (2, 8); DELETE FROM gt",
        {"gv": ("SELECT COUNT(*) AS cnt, MIN(a) AS lo FROM gt", ("cnt", "lo"), {(0, None): 1})}),
    "global_agg_replicated": (
        _GT + " WITH (replicated = true)", "",
        {"gv": ("SELECT COUNT(*) AS cnt, SUM(a) AS total FROM gt", ("cnt", "total"), {(0, None): 1})}),

    # ── set operations and DISTINCT ─────────────────────────────────────────
    # Four set-ops over one overlapping pair: UNION ALL's weight-2 duplicates,
    # the deduplicating ops' boundary state, and EXCEPT/INTERSECT's distinct and
    # positive_part integrals. The DELETE removes both carriers of b.val = 15,
    # so 15 re-enters EXCEPT and leaves INTERSECT.
    "set_ops": (
        "; ".join(f"CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)" for t in ("sa", "sb")),
        f"INSERT INTO sa VALUES {values(_SA)}; "
        f"INSERT INTO sb VALUES {values((i, (i % 20) + 10) for i in range(40))}; "
        "DELETE FROM sb WHERE id IN (5, 25)",
        {f"v_{n}": (f"SELECT val FROM sa {op} SELECT val FROM sb", ("val",), setop(op, _VA, _VB))
         for n, op in (("all", "UNION ALL"), ("union", "UNION"), ("isect", "INTERSECT"), ("exc", "EXCEPT"))}),
    "set_op_chain": _ab({"v": ("SELECT k FROM a UNION SELECT id FROM b UNION SELECT bv FROM b",
                               ("k",), {(x,): 1 for x in (0, 1, 2, 3, 4, 100, 200, 300, 400)})}),
    # Both sides off one source, whole and split by a filter: one push drives
    # every leaf in one epoch.
    "one_source_set_ops": _a({
        **{f"same_{n}": (f"SELECT k FROM a {op} SELECT k FROM a", ("k",),
                         setop(op, _ks(lambda k: True), _ks(lambda k: True)))
           for n, op in ONE_SOURCE_OPS.items()},
        **{f"split_{n}": (f"SELECT k FROM a WHERE k > 0 {op} SELECT k FROM a WHERE k < 3", ("k",),
                          setop(op, _ks(lambda k: k > 0), _ks(lambda k: k < 3)))
           for n, op in ONE_SOURCE_OPS.items()}}),
    "grouped_set_op_side": _ab({"v": (
        "SELECT k AS x, COUNT(*) AS n FROM a GROUP BY k UNION ALL SELECT id, bv FROM b",
        ("x", "n"), {(k, 6): 1 for k in range(5)} | {(j, bv): 1 for j, bv in _B})}),
    # Every surviving value keeps at least one carrier, so the deduplicated set
    # is stable across the DELETE.
    "distinct": (
        _table("dt", ("g",), _DT)[0], _table("dt", ("g",), _DT)[1] + "; DELETE FROM dt WHERE id = 0",
        {"dv": ("SELECT DISTINCT g FROM dt", ("g",), {(g,): 1 for g in range(7)})}),
    "distinct_over_join": _ab({"v": ("SELECT DISTINCT b.bv AS bv FROM a JOIN b ON a.k = b.id",
                                     ("bv",), {(bv,): 1 for _, bv in _B})}),

    # A linear view over a CLUSTER BY proper prefix sits on the worker owning the
    # source's distribution prefix. That placement is folded at registration and
    # written to no wire format, so boot re-derives it; re-deriving the full-PK
    # default instead addresses the resumed rows by a key naming a different
    # worker, and the view comes back a per-key prefix of itself.
    "cluster_by_prefix": (
        "CREATE TABLE cbt (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b)) CLUSTER BY a",
        f"INSERT INTO cbt VALUES {values(_CB)}",
        {"cbv": ("SELECT a, b, v FROM cbt WHERE v > 0", ("a", "b", "v"), dict.fromkeys(_CB, 1))}),
}


def views_ddl(views):
    return "; ".join(f"CREATE VIEW {name} AS {body}" for name, (body, _, _) in views.items())
