"""A join side the view reads nothing of carries no payload into the join.

Its trace element is then the join key alone, at the summed weight of every row
under it, and each kind still owes exact weights: an inner join multiplies, an
outer null-fill or anti row is `w_P · [S = 0]` per key (a linear difference, so
no row identity is needed to subtract on), and a mark tags each outer row. The
preserved side is bag-valued (`UNION ALL` of two tables), NULL keys never match,
and the PK-keyed shapes read the source's own store instead of a trace.

Run with GNITZ_WORKERS=4: the fill must cancel per worker.
"""
from collections import Counter

import gnitz

from _read import bag, scanned
from _sql import churn, inserting


def _left(A, B):
    """(a, b) pairs of A LEFT JOIN B ON k, a bag over A rows (k, tag)."""
    out = []
    for ak, at in A:
        m = [bid for bid, (bk, _) in B.items() if ak is not None and bk == ak]
        out += [(ak, at, bid) for bid in m] or [(ak, at, None)]
    return out


def test_a_side_read_for_nothing_keeps_exact_weights(client):
    client.execute_sql(
        "CREATE TABLE a1 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, t BIGINT NOT NULL); "
        "CREATE TABLE a2 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, t BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, y BIGINT); "
        "CREATE VIEW a AS SELECT k, t FROM a1 UNION ALL SELECT k, t FROM a2; "
        "CREATE VIEW l_none AS SELECT b.id AS bid FROM a LEFT JOIN b ON a.k = b.k; "
        "CREATE VIEW r_none AS SELECT a.t AS at FROM b RIGHT JOIN a ON a.k = b.k; "
        "CREATE VIEW f_none AS SELECT 1 AS one FROM a FULL JOIN b ON a.k = b.k; "
        "CREATE VIEW i_none AS SELECT 1 AS one FROM a JOIN b ON a.k = b.k; "
        "CREATE VIEW anti_none AS SELECT 1 AS one FROM a1 WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a1.k); "
        "CREATE VIEW mark_none AS SELECT EXISTS (SELECT 1 FROM b WHERE b.k = a1.k) AS e FROM a1; "
        "CREATE VIEW g_none AS SELECT b.y AS yy, COUNT(*) AS n FROM a LEFT JOIN b ON a.k = b.k GROUP BY b.y")
    steps = [
        inserting("a1", {1: (0, 1), 2: (None, 2), 3: (5, 3), 4: (5, 3)}),
        inserting("a2", {1: (0, 1), 2: (None, 2), 3: (7, 9)}),
        inserting("b", {1: (0, 10)}),
        inserting("b", {2: (5, 20), 3: (5, 30)}),
        ("DELETE FROM b WHERE id = 1", "b", {1: None}),
        inserting("b", {4: (None, 40), 5: (7, 50)}),
        ("DELETE FROM a1 WHERE id = 3", "a1", {3: None}),
        ("DELETE FROM b WHERE id = 2", "b", {2: None}),
        ("DELETE FROM b WHERE id = 3", "b", {3: None}),
    ]
    state = {"a1": {}, "a2": {}, "b": {}}
    for sql in churn(client, state, steps):
        A = list(state["a1"].values()) + list(state["a2"].values())
        B = state["b"]
        L = _left(A, B)
        assert bag(scanned(client, "l_none"), "bid") == dict(Counter((bid,) for _, _, bid in L)), sql
        assert bag(scanned(client, "r_none"), "at") == dict(Counter((t,) for _, t, _ in L)), sql
        matched_b = {bid for _, _, bid in L if bid is not None}
        n_full = len(L) + sum(1 for bid in B if bid not in matched_b)
        assert bag(scanned(client, "f_none"), "one") == ({(1,): n_full} if n_full else {}), sql
        n_inner = sum(1 for _, _, bid in L if bid is not None)
        assert bag(scanned(client, "i_none"), "one") == ({(1,): n_inner} if n_inner else {}), sql
        A1 = list(state["a1"].values())
        bk = {k for k, _ in B.values() if k is not None}
        n_anti = sum(1 for k, _ in A1 if k is None or k not in bk)
        assert bag(scanned(client, "anti_none"), "one") == ({(1,): n_anti} if n_anti else {}), sql
        assert bag(scanned(client, "mark_none"), "e") == dict(
            Counter((k is not None and k in bk,) for k, _ in A1)), sql
        ys = Counter(B[bid][1] if bid is not None else None for _, _, bid in L)
        assert bag(scanned(client, "g_none"), "yy", "n") == {(y, n): 1 for y, n in ys.items()}, sql


_TABLES = (
    "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, t BIGINT NOT NULL); "
    "CREATE TABLE q (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, y BIGINT); "
)
_VIEWS = {
    # p preserved, keeps nothing, re-keyed onto its own PK (a source-walked trace).
    "pk_left": "SELECT q.y AS y FROM p LEFT JOIN q ON p.id = q.k",
    # q unique on its PK key: the matched half is a projection of the inner join.
    "pk_left_u": "SELECT 1 AS one FROM p LEFT JOIN q ON p.k = q.id",
    "pk_full": "SELECT 1 AS one FROM p FULL JOIN q ON p.id = q.id",
    "pk_inner": "SELECT 1 AS one FROM p JOIN q ON p.id = q.k",
    "pk_anti": "SELECT 1 AS one FROM p WHERE NOT EXISTS (SELECT 1 FROM q WHERE q.k = p.id)",
    "pk_semi": "SELECT 1 AS one FROM p WHERE p.k IN (SELECT q.k FROM q)",
    "pk_mark": "SELECT EXISTS (SELECT 1 FROM q WHERE q.id = p.k) AS e FROM p",
    "cnt_left": "SELECT COUNT(*) AS n FROM p LEFT JOIN q ON p.k = q.k",
}


def _ref(P, Q):
    def left(pk, qk):
        out = []
        for pid, (pkk, pt) in P.items():
            pv = {"id": pid, "k": pkk}[pk]
            m = [qid for qid, (qkk, qy) in Q.items() if pv is not None and {"id": qid, "k": qkk}[qk] == pv]
            out += [(pid, qid) for qid in m] or [(pid, None)]
        return out

    def c(xs):
        return dict(Counter(xs))

    def one(n):
        return {(1,): n} if n else {}

    L1 = left("id", "k")
    Lu = left("k", "id")
    Lf = left("id", "id")
    n_full = len(Lf) + sum(1 for qid in Q if qid not in P)
    qk = {k for k, _ in Q.values() if k is not None}
    n_left = len(left("k", "k"))
    return {
        "pk_left": ("y", c((Q[q][1] if q is not None else None,) for _, q in L1)),
        "pk_left_u": ("one", one(len(Lu))),
        "pk_full": ("one", one(n_full)),
        "pk_inner": ("one", one(sum(1 for _, q in L1 if q is not None))),
        "pk_anti": ("one", one(sum(1 for pid in P if pid not in qk))),
        "pk_semi": ("one", one(sum(1 for k, _ in P.values() if k is not None and k in qk))),
        "pk_mark": ("e", c((k is not None and k in Q,) for k, _ in P.values())),
        "cnt_left": ("n", {(n_left,): 1}),
    }


def _check(conn, P, Q, suffix, what):
    for name, (col, want) in _ref(P, Q).items():
        assert bag(scanned(conn, name + suffix), col) == want, (name + suffix, what)


def test_a_pk_keyed_side_read_for_nothing_survives_backfill_and_restart(own_server):
    own_server.extra_env.update({"GNITZ_RAM_TIER_BYTES": "4096"})
    own_server.start(workers=4)
    P, Q = {}, {}
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(_TABLES + " ".join(f"CREATE VIEW {n} AS {b};" for n, b in _VIEWS.items()))
        for i in range(1, 301):
            P[i] = (i % 17 if i % 5 else None, i)
        conn.execute_sql("INSERT INTO p VALUES " + ", ".join(f"({i}, {'NULL' if k is None else k}, {t})" for i, (k, t) in P.items()))
        for i in range(1, 120, 2):
            Q[i] = (i % 13, i * 10)
        conn.execute_sql("INSERT INTO q VALUES " + ", ".join(f"({i}, {k}, {y})" for i, (k, y) in Q.items()))
        _check(conn, P, Q, "", "maintained")
        conn.execute_sql("DELETE FROM q WHERE id < 40")
        Q = {i: v for i, v in Q.items() if i >= 40}
        conn.execute_sql("DELETE FROM p WHERE id > 250")
        P = {i: v for i, v in P.items() if i <= 250}
        _check(conn, P, Q, "", "after deletes")
        conn.execute_sql(" ".join(f"CREATE VIEW {n}_bf AS {b};" for n, b in _VIEWS.items()))
        _check(conn, P, Q, "_bf", "backfill")
    own_server.restart(graceful=True)
    with gnitz.connect(own_server.target) as conn:
        _check(conn, P, Q, "", "after graceful restart")
        _check(conn, P, Q, "_bf", "backfill after graceful restart")
        conn.execute_sql("INSERT INTO q VALUES (1001, 3, 7), (1002, 3, 8), (1003, NULL, 9), (5, 5, 50)")
        Q.update({1001: (3, 7), 1002: (3, 8), 1003: (None, 9), 5: (5, 50)})
        _check(conn, P, Q, "", "post-restart delta")
        _check(conn, P, Q, "_bf", "post-restart delta bf")
    own_server.restart(graceful=False)
    with gnitz.connect(own_server.target) as conn:
        _check(conn, P, Q, "", "after crash restart")
        _check(conn, P, Q, "_bf", "bf after crash restart")
