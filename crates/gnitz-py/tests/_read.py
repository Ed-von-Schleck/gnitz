"""How a test observes a read: the SELECT accessor, the Z-set bag, and the
plan's access line.

`bag` rather than a row list is the default here on purpose. A Z-set engine
defines correctness as weights, so a row count, a `sorted(pks)` list or a
`{pk: val}` dict all read a duplicated reply frame, a delta applied twice and a
weight-0 ghost as correct.
"""

import math


def rows(client, q):
    """The rows of a one-statement SELECT, in result order. `client` is anything
    with a blocking `execute_sql` — a connection or a mirroring client."""
    return result_rows(client.execute_sql(q))


def result_rows(results):
    """`rows` over the results `execute_sql` already returned — an awaited
    call's, or a `Pending`'s."""
    return list(reply(results))


def reply(results):
    """The one `ScanResult` of a one-statement SELECT's `results`: what `rows`
    lists, with the names, schema and LSN the result carries beside them."""
    assert len(results) == 1, f"expected one statement result, got {len(results)}"
    res = results[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return res["rows"]


def bag(rows, *cols):
    """`{(values of cols): summed weight}` over any row iterable — a SELECT
    result or a scan — with ghosts dropped.

    Summing rather than collecting is what makes the answer invariant to how a
    multiplicity is split across workers and entries, so the same expectation
    holds at every worker count.

    A NaN is folded onto `math.nan` itself, so an expectation spelling
    `math.nan` matches it: tuple equality tries identity before `==`, and by
    `==` a NaN equals nothing — not even the NaN a retraction must cancel.
    """
    acc = {}
    for r in rows:
        d = r._asdict()
        k = tuple(d[c] for c in cols) if cols else tuple(r)
        k = tuple(math.nan if v != v else v for v in k)
        acc[k] = acc.get(k, 0) + r._weight
    return {k: w for k, w in sorted(acc.items(), key=repr) if w != 0}


def ordered(rows, *cols):
    """`[(values of cols, weight)]` in result order, one entry per run of equal
    rows — `bag`'s counterpart for a result whose order is the thing under test.
    With no `cols`, each row is its `(name, value)` pairs.

    Summing a run rather than listing entries makes the answer invariant to how
    a multiplicity is split across entries, as `bag` is."""
    out = []
    for r in rows:
        d = r._asdict()
        k = tuple(d[c] for c in cols) if cols else tuple(d.items())
        if out and out[-1][0] == k:
            out[-1] = (k, out[-1][1] + r._weight)
        else:
            out.append((k, r._weight))
    return out


def plan_line(client, q, label):
    """What EXPLAIN's `label:` line says of `q`. Found by prefix rather than by
    row position, so adding a plan line cannot silently make this read a
    different fact."""
    lines = [r[0] for r in rows(client, "EXPLAIN " + q)]
    got = [ln.removeprefix(label + ": ") for ln in lines if ln.startswith(label + ": ")]
    assert len(got) == 1, lines
    return got[0]


def access(client, q):
    """Which walk the plan chose for `q`."""
    return plan_line(client, q, "access")


def scanned(client, name, *, hidden=False):
    """The rows of a full scan of relation `name`, hidden columns included when
    `hidden` — the scan-side counterpart of `rows`, so both
    compose with `bag`."""
    result = client.scan(*client.resolve_table(name))
    return list(result.including_hidden() if hidden else result)
