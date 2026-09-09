"""How a test observes a read: the SELECT accessor, the Z-set bag, and the
plan's access line.

`bag` rather than a row list is the default here on purpose. A Z-set engine
defines correctness as weights, so a row count, a `sorted(pks)` list or a
`{pk: val}` dict all read a duplicated reply frame, a delta applied twice and a
weight-0 ghost as correct.
"""


def rows(client, sn, q):
    """The rows of a direct SELECT, in result order."""
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return list(res["rows"])


def bag(rows, *cols):
    """`{(values of cols): summed weight}` over any row iterable — a SELECT
    result or a scan — with ghosts dropped.

    Summing rather than collecting is what makes the answer invariant to how a
    multiplicity is split across workers and entries, so the same expectation
    holds at every worker count.
    """
    acc = {}
    for r in rows:
        d = r._asdict()
        k = tuple(d[c] for c in cols) if cols else tuple(r)
        acc[k] = acc.get(k, 0) + r.weight
    return {k: w for k, w in sorted(acc.items(), key=repr) if w != 0}


def access(client, sn, q):
    """EXPLAIN's `access:` line for `q` — which walk the plan chose. Found by
    prefix rather than by row position, so adding a plan line cannot silently
    make this read a different fact."""
    lines = [r[0] for r in rows(client, sn, "EXPLAIN " + q)]
    got = [ln for ln in lines if ln.startswith("access: ")]
    assert len(got) == 1, lines
    return got[0]


def scanned(client, sn, name):
    """The rows of a full scan of a relation named in `sn` — the scan-side
    counterpart of `rows`, so both compose with `bag`."""
    return list(client.scan(client.resolve_table(sn, name)[0]))
