"""How a scalar-expression test states its subject over table `t`: each
expression against its declared type and its value on every row, and each
predicate against the keys it admits — held alike by every view of it and by an
ad-hoc read of the same body."""

from _read import bag, rows, scanned

# Expressions per projection view, so a table of them stays inside one
# program's register file.
_PER_VIEW = 12


def declared(client, name):
    """`{column name: (type code, scale)}` of relation `name`. The scale is
    asserted beside the type because a value cannot carry it: Python compares
    `Decimal("37.50")` and `Decimal("37.50000")` equal."""
    return {c.name: (c.type_code, c.scale) for c in client.resolve_table(name)[1].columns}


def projected(client, seed, exprs, key="id"):
    """Assert `exprs`, `{expression: ((type code, scale), [value per row of t,
    in key order])}`, after running `seed`.

    One copy of the projection exists before `seed`, so each value is a delta a
    circuit computed; a second is created after it, a pure backfill. Both must
    equal the ad-hoc read and the table, at the declared type. `DELETE FROM t`
    must then leave both empty: an insert and its retraction re-derive each
    value independently, and cancel only if every computed cell matches byte for
    byte."""
    items = list(exprs.items())
    parts = [items[k:k + _PER_VIEW] for k in range(0, len(items), _PER_VIEW)]
    bodies = [f"SELECT {key}, " + ", ".join(f"{e} AS c{n}" for n, (e, _) in enumerate(part))
              + " FROM t" for part in parts]
    views = [(f"m{n}", f"b{n}") for n in range(len(parts))]

    client.execute_sql("; ".join(f"CREATE VIEW {m} AS {b}" for (m, _), b in zip(views, bodies)))
    client.execute_sql(seed)
    client.execute_sql("; ".join(f"CREATE VIEW {b} AS {s}" for (_, b), s in zip(views, bodies)))

    keys = sorted(k for (k,) in bag(scanned(client, "t"), key))
    for (m, b), part, body in zip(views, parts, bodies):
        assert list(declared(client, m).values())[1:] == [ty for _, (ty, _) in part], body
        want = dict.fromkeys(zip(keys, *(vs for _, (_, vs) in part)), 1)
        assert bag(scanned(client, m)) == want, body
        assert bag(scanned(client, b)) == want, body
        assert bag(rows(client, body)) == want, body

    client.execute_sql("DELETE FROM t")
    for m, b in views:
        assert bag(scanned(client, m)) == bag(scanned(client, b)) == {}


def filtered(client, preds, key="id"):
    """One maintained `SELECT key FROM t WHERE p` per predicate, and the read
    of them all: `held()` is `{predicate: the keys its view holds}`, asserting
    on the way that each key is held at weight 1 and that each view equals the
    ad-hoc read of its WHERE. `held(p, ...)` reads only the views named."""
    views = {p: f"f{n}" for n, p in enumerate(preds)}
    client.execute_sql("; ".join(
        f"CREATE VIEW {v} AS SELECT {key} FROM t WHERE {p}" for p, v in views.items()))

    def held(*only):
        got = {}
        for p in only or views:
            b = bag(scanned(client, views[p]))
            assert b == bag(rows(client, f"SELECT {key} FROM t WHERE {p}")), p
            assert set(b.values()) <= {1}, p
            got[p] = {k for (k,) in b}
        return got

    return held
