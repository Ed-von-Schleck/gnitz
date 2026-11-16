"""The reference semantics every view expectation is computed with — grouping,
aggregates, comparison and ordering under SQL's NULL rules, the set operators —
and one catalogue of view shapes, for the sweeps that hold the shape fixed and
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
import operator
from collections import Counter

from _sql import values


def groups(rows, key):
    """`{key(r): [the rows with that key]}`, in first-seen order."""
    out = {}
    for r in rows:
        out.setdefault(key(r), []).append(r)
    return out


def grouped(rows, key, *vals):
    """`{(key, *sums): 1}` — what a GROUP BY view must equal, each `vals` entry
    a row -> number summed over the group."""
    return {(k, *(sum(v(r) for r in rs) for v in vals)): 1 for k, rs in groups(rows, key).items()}


def agg(kind, vals):
    """One SQL aggregate over `vals`: COUNT* counts every value, the rest skip
    NULLs, and each but COUNT is NULL once none is left."""
    if kind == "COUNT*":
        return len(vals)
    live = [v for v in vals if v is not None]
    if kind == "COUNT":
        return len(live)
    if not live:
        return None
    return {"SUM": sum, "MIN": min, "MAX": max, "AVG": lambda v: sum(v) / len(v)}[kind](live)


def known(op):
    """`op` under SQL 3VL: a comparison against NULL admits nothing."""
    return lambda l, r: l is not None and r is not None and op(l, r)


# Each range comparison, by the suffix its view carries.
RANGE_OPS = {"lt": ("<", operator.lt), "le": ("<=", operator.le), "gt": (">", operator.gt),
             "ge": (">=", operator.ge)}


def sorted_by(rows, keys):
    """`rows` (dicts) ordered by `keys` — `(column, desc, nulls_first)` each —
    ties in ascending `id`, the identity a cut breaks them by."""
    out = sorted(rows, key=lambda r: r["id"])
    for col, desc, nulls_first in reversed(keys):
        nulls = [r for r in out if r[col] is None]
        # A reversed sort still keeps equal keys in their order.
        vals = sorted((r for r in out if r[col] is not None), key=lambda r: r[col], reverse=desc)
        out = nulls + vals if nulls_first else vals + nulls
    return out


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
_DIA_SUM = {g: s for (g, s) in grouped(_DIA, lambda r: r[1], lambda r: r[2])}
# pk 0, group 0's MIN holder, is deleted again.
_MM = [(i, i % 6, (i * 7) % 50) for i in range(24)]
_MM_LIVE = _MM[1:]
# Both carriers of b.val = 15 are deleted again.
_SA = [(i, i % 20) for i in range(40)]
_SB_SET = [(i, (i % 20) + 10) for i in range(40) if i not in (5, 25)]
_VA, _VB = Counter((v,) for _, v in _SA), Counter((v,) for _, v in _SB_SET)
_DT = [(i, i % 7) for i in range(42)]
_SO = [(pk, pk % 4, pk * 3) for pk in range(24)]
_CB = [(a, b, a * 100 + b) for a in range(1, 9) for b in range(1, 5)]
_FT = [(i, i % 4, (100 if i <= 18 else 5000)) for i in range(1, 25)]
_GT = "CREATE TABLE gt (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)"
# `t` varies within a `k` group on both sides, so each group holds matches and
# non-matches rather than the whole product.
_BP = [(k, b, k * 10 + b) for k in range(1, 7) for b in range(1, 4)]
_BQ = [(k, b, k * 10 + 2 * b) for k in range(1, 7) for b in range(1, 4)]
# `dt`'s join key `k` is neither its PK nor equal to it; `du` row 1003 is
# deleted again.
_DT_ROWS = [(i, 1000 + i, i * 10) for i in range(1, 25)]
_DU = [(1000 + i, i * 100 + 1) for i in range(1, 25)]
_DT_LIVE = [r for r in _DT_ROWS if r[1] != 1003]


# `kb` overlaps `ka`, agreeing on a third of the shared ids; `kc` and `kd` repeat
# them under a compound key, and `kr` is a replicated part of `ka`.
_KA0 = [(i, i % 7) for i in range(30)]
_KA = [(i, 3 if i == 4 else v) for i, v in _KA0]
_KB0 = [(i, i % 7 if i % 3 == 0 else 100 + i) for i in range(10, 40)]
_KB = [r for r in _KB0 if r[0] != 12]
_KC = [(i % 6, i, v) for i, v in _KA]
_KD = [(i % 6, i, v) for i, v in _KB]
_KR = [r for r in _KA if r[0] % 2 == 0]


def _bag(rows, keep=lambda *r: True, order=None):
    return Counter(tuple(r[i] for i in order) if order else tuple(r) for r in rows if keep(*r))


def _ks(keep):
    return Counter((k,) for _, k, _ in _A if keep(k))


def _band_over(cluster):
    """A band join of two `(k, b)`-keyed tables placed by `cluster`."""
    ddl = "; ".join(f"CREATE TABLE {t} (k BIGINT NOT NULL, b BIGINT NOT NULL, t BIGINT NOT NULL, "
                    f"PRIMARY KEY (k, b)){cluster}" for t in ("p", "q"))
    return (ddl, f"INSERT INTO p VALUES {values(_BP)}; INSERT INTO q VALUES {values(_BQ)}",
            {"v": ("SELECT p.k AS k, p.b AS pb, q.b AS qb FROM p JOIN q ON p.k = q.k AND p.t < q.t",
                   ("k", "pb", "qb"),
                   {(pk, pb, qb): 1 for pk, pb, pt in _BP for qk, qb, qt in _BQ if pk == qk and pt < qt})})


_U64 = [0, 1, 2**63, 2**64 - 1]


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
    # The band relays by its equality prefix alone: skipped where that prefix is
    # the tables' distribution key, run where the whole PK places their rows.
    "band_cluster_by_k": _band_over(" CLUSTER BY k"),
    "band_whole_pk": _band_over(""),
    # A derived side whose `Map` moves the join key off the slot `dt` holds it
    # in: computed beside it, duplicated, or computed itself. The master must
    # scatter `dt`'s delta by the source column the key came from.
    "derived_join_side": (
        "CREATE TABLE dt (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL); "
        "CREATE TABLE du (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)",
        f"INSERT INTO dt VALUES {values(_DT_ROWS)}; INSERT INTO du VALUES {values(_DU)}; "
        "DELETE FROM du WHERE id = 1003",
        {"computed": ("SELECT x.k AS k, x.s AS s, du.b AS b FROM (SELECT a + 1 AS s, k FROM dt) x "
                      "JOIN du ON x.k = du.id",
                      ("k", "s", "b"), {(k, a + 1, i * 100 + 1): 1 for i, k, a in _DT_LIVE}),
         "duplicated": ("SELECT x.k AS k, x.k2 AS k2, du.b AS b FROM (SELECT k, k AS k2 FROM dt) x "
                        "JOIN du ON x.k2 = du.id",
                        ("k", "k2", "b"), {(k, k, i * 100 + 1): 1 for i, k, _ in _DT_LIVE}),
         # `a * 10 + 1` is `du.b` on every row.
         "computed_key": ("SELECT x.id AS id, du.b AS b FROM (SELECT id, a * 10 + 1 AS s FROM dt) x "
                          "JOIN du ON x.s = du.b",
                          ("id", "b"), {(i, a * 10 + 1): 1 for i, _, a in _DT_LIVE})}),
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
    "self_join_computed": _a({"v": ("SELECT x.id AS xid, y.id AS yid FROM (SELECT id, k + 1 AS s FROM a) x JOIN a y ON x.s = y.id",
                                    ("xid", "yid"), {(i, k + 1): 1 for i, k, _ in _A})}),
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
                 grouped(_SB, lambda r: r[1], lambda r: r[2])),
         "vx2": ("SELECT g, SUM(id) AS s FROM t GROUP BY g", ("g", "s"),
                 grouped(_SB, lambda r: r[1], lambda r: r[0]))}),
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
                grouped(_PG, lambda r: r[1], lambda r: r[2])),
         "vn": ("SELECT g, s + 1 AS s1 FROM vx", ("g", "s1"),
                {(g, s + 1): 1 for g, s in grouped(_PG, lambda r: r[1], lambda r: r[2])})}),
    "projection_sibling_of_groupby": (
        *_over(_table("t", ("g", "val"), _SIB)),
        {"vn": ("SELECT id, val + 1 AS plus1 FROM t", ("id", "plus1"), {(i, v + 1): 1 for i, _, v in _SIB}),
         "vx": ("SELECT g, SUM(val) AS s FROM t GROUP BY g", ("g", "s"),
                grouped(_SIB, lambda r: r[1], lambda r: r[2]))}),
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
               {(g, *(agg(f, [r[2] for r in rs]) for f in ("MIN", "MAX", "AVG", "COUNT"))): 1
                for g, rs in groups(_MM_LIVE, lambda r: r[1]).items()})}),
    # One join-output key spans six groups, so each group's MIN must isolate.
    "min_over_fanout_join": _ab({"v": (
        "SELECT a.av AS g, MIN(a.id) AS lo, MAX(b.bv) AS hi FROM b JOIN a ON b.id = a.k GROUP BY a.av",
        ("g", "lo", "hi"), {(av, i, _BV[k]): 1 for i, k, av in _A})}),
    "reduce_into_join": _ab({"v": (
        "WITH s AS (SELECT k, SUM(av) AS t FROM a GROUP BY k) SELECT b.bv AS bv, s.t AS t FROM s JOIN b ON s.k = b.id",
        ("bv", "t"), {(_BV[k], t): 1 for k, t in grouped(_A, lambda r: r[1], lambda r: r[2])})}),
    # A contiguous low-id range fails the predicate on every worker's first
    # chunks, so the exchanged pre-phase payload for those rounds is empty.
    # Inferring "done" from an empty round truncates the view.
    "filtered_groupby": (
        *_over(_table("ft", ("category", "amount"), _FT)),
        {"fv": ("SELECT category, COUNT(*) AS cnt, SUM(amount) AS total FROM ft WHERE amount > 1000 GROUP BY category",
                ("category", "cnt", "total"),
                grouped([r for r in _FT if r[2] > 1000], lambda r: r[1], lambda r: 1, lambda r: r[2]))}),
    # A linear final whose delta source is an in-bundle hidden segment that
    # exchanges — a grouped or joined CTE read twice, or a linear segment between
    # two grouped ones — so the segment must be filled before the final reads it.
    "grouped_cte": (
        *_over(_table("t", ("v",), [(1, 20), (2, 5), (3, 30)])),
        {"v": ("WITH c AS (SELECT id, SUM(v) AS sv FROM t GROUP BY id) "
               "SELECT id FROM c WHERE sv > 10 UNION ALL SELECT id FROM c WHERE sv < 10",
               ("id",), {(1,): 1, (2,): 1, (3,): 1})}),
    "join_cte": (
        *_over(_table("ja", ("k",), [(1, 10), (2, 20), (3, 10)]), _table("jb", ("x",), [(10, 5), (20, 0)])),
        {"v": ("WITH c AS (SELECT ja.id AS id, jb.x AS x FROM ja JOIN jb ON ja.k = jb.id) "
               "SELECT id FROM c WHERE x > 0 UNION ALL SELECT id FROM c WHERE x = 0",
               ("id",), {(1,): 1, (2,): 1, (3,): 1})}),
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
    # Sides that output their base table's whole PK, keyed by it.
    "source_keyed_set_ops": (
        "; ".join(f"CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL){w}"
                  for t, w in (("ka", ""), ("kb", ""), ("kr", " WITH (replicated = true)"))) + "; "
        "CREATE TABLE kc (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b)) "
        "CLUSTER BY a; "
        "CREATE TABLE kd (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
        f"INSERT INTO ka VALUES {values(_KA0)}; INSERT INTO kb VALUES {values(_KB0)}; "
        f"INSERT INTO kc VALUES {values(_KC)}; INSERT INTO kd VALUES {values(_KD)}; "
        f"INSERT INTO kr VALUES {values(_KR)}; "
        "UPDATE ka SET val = 3 WHERE id = 4; DELETE FROM kb WHERE id = 12",
        {**{f"k_{n}": (f"SELECT * FROM ka {op} SELECT * FROM kb", ("id", "val"), setop(op, _bag(_KA), _bag(_KB)))
            for n, op in ONE_SOURCE_OPS.items()},
         "k_filtered": ("SELECT id, val FROM ka WHERE val > 1 EXCEPT SELECT id, val FROM kb WHERE val < 6",
                        ("id", "val"), setop("EXCEPT", _bag(_KA, lambda i, v: v > 1), _bag(_KB, lambda i, v: v < 6))),
         "k_reordered": ("SELECT val, id FROM ka INTERSECT SELECT val, id FROM kb", ("val", "id"),
                         setop("INTERSECT", _bag(_KA, order=(1, 0)), _bag(_KB, order=(1, 0)))),
         "k_chain": ("SELECT * FROM ka EXCEPT SELECT * FROM kb EXCEPT SELECT * FROM kr", ("id", "val"),
                     setop("EXCEPT", Counter(setop("EXCEPT", _bag(_KA), _bag(_KB))), _bag(_KR))),
         "k_self": ("SELECT * FROM ka WHERE val > 2 EXCEPT ALL SELECT * FROM ka WHERE val < 5", ("id", "val"),
                    setop("EXCEPT ALL", _bag(_KA, lambda i, v: v > 2), _bag(_KA, lambda i, v: v < 5))),
         "k_prefix": ("SELECT * FROM kc INTERSECT SELECT * FROM kd", ("a", "b", "v"),
                      setop("INTERSECT", _bag(_KC), _bag(_KD))),
         "k_replicated": ("SELECT * FROM ka EXCEPT SELECT * FROM kr", ("id", "val"),
                          setop("EXCEPT", _bag(_KA), _bag(_KR)))}),
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

    # Views whose key is not their first column: the store holds the circuit's
    # regions under the SELECT list's numbering.
    "select_order": (
        "CREATE TABLE so (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        f"INSERT INTO so VALUES {values(_SO)}",
        {"sol": ("SELECT b, id, a FROM so", ("b", "id", "a"), {(b, i, a): 1 for i, a, b in _SO}),
         "sog": ("SELECT COUNT(*) AS n, a FROM so GROUP BY a", ("n", "a"), {(6, a): 1 for a in range(4)}),
         "sot": ("SELECT b, id FROM so ORDER BY b DESC LIMIT 3", ("b", "id"),
                 {(69, 23): 1, (66, 22): 1, (63, 21): 1}),
         "soq": ("SELECT a, id FROM so QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY id) = 1",
                 ("a", "id"), {(a, a): 1 for a in range(4)})}),

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

    # A bound outside the key's type: a backfill walks the key bytes and a
    # delta passes the filter, so both must read `-1` alike.
    "key_bound_outside_its_type": (
        "CREATE TABLE ub (k BIGINT UNSIGNED NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        f"INSERT INTO ub VALUES {values((k, i) for i, k in enumerate(_U64))}",
        {"none": ("SELECT k FROM ub WHERE k <= -1", ("k",), {}),
         "every": ("SELECT k FROM ub WHERE k > -1", ("k",), {(k,): 1 for k in _U64})}),
}


def views_ddl(views):
    """The `CREATE VIEW`s of `{view: (body, ...)}`, in its order."""
    return "; ".join(f"CREATE VIEW {name} AS {body}" for name, (body, *_) in views.items())
