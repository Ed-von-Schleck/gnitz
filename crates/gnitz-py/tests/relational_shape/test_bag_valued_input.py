"""A bag-valued input: one identity at weight 2 through every join kind.

A `UNION ALL` view whose branches project to one identity is a relation whose
elements carry weight > 1, and each join kind owes it a different weight: an
inner or keyless join multiplies (`w_A · w_B`), a semi-join keeps `w_A` while
anything matches, and an anti-join or an outer null-fill keeps `w_A` while
nothing does. The null-fill contract is weight-exact for a bag-valued preserved
side: it subtracts the raw matched multiplicity and clamps the result, where
clamping the match witness to 1 would leak a weight-`w−1` fill beside the
matched rows and a second match would multiply a semi-join to 4.

Pruning manufactures the same case from a base table: two rows differing only
in a column no view reads coincide once it is dropped from the join payload.

Run with GNITZ_WORKERS=4: the fill must cancel per worker.
"""
import operator
from collections import Counter

from _read import bag, scanned

_ID = "SELECT uv.k AS k, uv.x AS x, b.id AS bid"

# view -> (SQL body, what the view owes an identity (k, x) of weight w, given
# the b rows it matches, as (bid, weight) pairs).
_KINDS = {
    "inner": (f"{_ID} FROM uv JOIN b ON uv.k = b.k", "k", "product"),
    "left": (f"{_ID} FROM uv LEFT JOIN b ON uv.k = b.k", "k", "left"),
    "right": (f"{_ID} FROM b RIGHT JOIN uv ON b.k = uv.k", "k", "left"),
    "band_left": (f"{_ID} FROM uv LEFT JOIN b ON uv.k = b.k AND uv.x <= b.y", "band", "left"),
    "range_left": (f"{_ID} FROM uv LEFT JOIN b ON uv.x < b.y", "range", "left"),
    "cross": (f"{_ID} FROM uv CROSS JOIN b", "all", "product"),
    "semi": ("SELECT k, x FROM uv WHERE EXISTS (SELECT 1 FROM b WHERE b.k = uv.k)", "k", "semi"),
    "anti": ("SELECT k, x FROM uv WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = uv.k)", "k", "anti"),
}

_MATCH = {
    "k": lambda k, x, bk, y: k == bk,
    "band": lambda k, x, bk, y: k == bk and x <= y,
    "range": lambda k, x, bk, y: operator.lt(x, y),
    "all": lambda k, x, bk, y: True,
}


def test_each_join_kind_owes_a_weight_two_identity_its_own_weight(client, schema_name):
    """Two matches, one, and none, for every kind over one weight-2 identity
    that matches and one that never does."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE s1 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL); "
        "CREATE TABLE s2 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, y BIGINT NOT NULL); "
        "CREATE TABLE l (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, kept BIGINT NOT NULL, "
        "dropped BIGINT NOT NULL); "
        "CREATE VIEW uv AS SELECT k, x FROM s1 UNION ALL SELECT k, x FROM s2; "
        + "; ".join(f"CREATE VIEW {name} AS {body}" for name, (body, _, _) in _KINDS.items()) + "; "
        "CREATE VIEW pruned AS SELECT l.kept, b.y FROM l LEFT JOIN b ON l.k = b.k; "
        # (5, 10) and (9, 99) each arrive from both branches.
        "INSERT INTO s1 VALUES (1, 5, 10), (2, 9, 99); "
        "INSERT INTO s2 VALUES (1, 5, 10), (2, 9, 99); "
        "INSERT INTO l VALUES (1, 5, 9, 100), (2, 5, 9, 200)", schema_name=sn)
    uv = {(5, 10): 2, (9, 99): 2}

    b = {}
    for sql, changes in [
        ("SELECT 1", {}),
        ("INSERT INTO b VALUES (1, 5, 50)", {1: (5, 50)}),
        ("INSERT INTO b VALUES (2, 5, 60)", {2: (5, 60)}),
        ("DELETE FROM b", {1: None, 2: None}),
    ]:
        client.execute_sql(sql, schema_name=sn)
        for pk, row in changes.items():
            if row is None:
                del b[pk]
            else:
                b[pk] = row

        for name, (_, match, owes) in _KINDS.items():
            want = Counter()
            for (k, x), w in uv.items():
                hits = [bid for bid, (bk, y) in b.items() if _MATCH[match](k, x, bk, y)]
                if owes in ("product", "left"):
                    want.update({(k, x, bid): w for bid in hits})
                if owes == "left" and not hits:
                    want[(k, x, None)] += w
                if (owes == "semi" and hits) or (owes == "anti" and not hits):
                    want[(k, x)] += w
            cols = ("k", "x") if owes in ("semi", "anti") else ("k", "x", "bid")
            assert bag(scanned(client, sn, name), *cols) == want, (sql, name)

        # Both `l` rows coincide on (k, kept) once `dropped` is pruned.
        assert bag(scanned(client, sn, "pruned"), "kept", "y") == (
            {(9, y): 2 for _, y in b.values()} if b else {(9, None): 2}), sql
