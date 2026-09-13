"""What a binary push is: a batch folded by PK into a base table, and concurrent
pushes grouped into shared commits.

A base table upserts by PK: within one push the last weight-positive row for a
PK wins, a retraction matches on the PK alone, and a weight-0 row is no row.
Every read is a Z-set bag of the table and of a passthrough view over it: the
table runs `enforce_unique_pk` and the view does not, so a weight the table
clamps shows up in the view at whatever weight was actually emitted.
"""

import threading

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import join_or_fail

_INT = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64, is_nullable=True)]
_STR = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.STRING, is_nullable=True)]

# A STRING table's push takes its own encoding and relocation path, so every
# case runs over both; the four values are indexed by the cases below. The
# string leg spans the empty string, the exact 12-byte inline limit, a heap
# string and NULL.
_LEGS = {
    "int": (_INT, [10, 20, 99, None]),
    "string": (_STR, ["", "abcdefghijkl", "this_is_a_longer_string_value", None]),
}

# (committed (pk, value index), pushed (pk, value index, weight) or
# ("delete", keys), surviving (pk, value index)). A retraction carries filler
# payload, as `delete` ships.
_FOLD = {
    "every value": ([], [(1, 0, 1), (2, 1, 1), (3, 2, 1), (4, 3, 1)],
                    [(1, 0), (2, 1), (3, 2), (4, 3)]),
    "last write wins": ([], [(1, 0, 1), (1, 1, 1)], [(1, 1)]),
    "insert then retract": ([], [(1, 0, 1), (1, 2, -1)], []),
    "retract then insert": ([(1, 0)], [(1, 2, -1), (1, 1, 1)], [(1, 1)]),
    "weight 3 is one row": ([], [(1, 0, 3)], [(1, 0)]),
    "weight 0 is no row": ([], [(1, 0, 0), (2, 1, 1), (3, 2, 0)], [(2, 1)]),
    "all weight 0": ([(1, 0)], [(2, 1, 0), (3, 2, 0)], [(1, 0)]),
    "independent keys": ([(1, 0), (2, 1), (3, 2)], [(2, 2, 1)], [(1, 0), (2, 2), (3, 2)]),
    "delete by key, absent key": ([(1, 0), (2, 1)], ("delete", [2, 999]), [(1, 0)]),
}


@pytest.mark.parametrize("leg", _LEGS)
@pytest.mark.parametrize("committed,pushed,survivors", _FOLD.values(), ids=_FOLD.keys())
def test_a_push_folds_by_pk(client, schema_name, leg, committed, pushed, survivors):
    cols, vals = _LEGS[leg]
    sn = schema_name
    tid = client.create_table(sn, "t", cols)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=sn)
    schema = gnitz.Schema(cols)

    def batch(rows):
        return gnitz.ZSetBatch(schema).extend(
            [{"pk": pk, "val": vals[i], "_weight": w} for pk, i, w in rows])

    if committed:
        client.push(tid, batch([(pk, i, 1) for pk, i in committed]))
    if pushed[0] == "delete":
        client.delete(tid, schema, pushed[1])
    else:
        client.push(tid, batch(pushed))

    want = {(pk, vals[i]): 1 for pk, i in survivors}
    assert bag(scanned(client, sn, "t")) == bag(scanned(client, sn, "v")) == want


def test_an_empty_push_writes_nothing_and_keeps_the_connection_aligned(client, schema_name):
    """An empty batch ACKs with the "nothing written" LSN 0 — never a scan,
    whose streamed dump of the table's rows would desync the one-frame push
    reply. The pushes around it return real LSNs and land whole."""
    sn = schema_name
    tid = client.create_table(sn, "t", _INT)
    schema = gnitz.Schema(_INT)
    bulk = gnitz.ZSetBatch(schema).extend([{"pk": i, "val": i * 10} for i in range(1, 1001)])
    assert client.push(tid, bulk) > 0
    assert client.push(tid, gnitz.ZSetBatch(schema)) == 0
    assert client.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": 1001, "val": 0}])) > 0
    assert bag(scanned(client, sn, "t")) == {(i, i * 10): 1 for i in range(1, 1001)} | {(1001, 0): 1}


def test_concurrent_pushes_coalesce_into_shared_commits(server, client, schema_name):
    """Eight connections push at once. Each batch carries four rows under keys
    no other connection writes, alternating inline and heap strings, plus one
    row under the key every connection writes.

    A push that validates against no committed state holds its table lock
    shared, so concurrent pushes reach the committer together and fold into one
    merged batch under one SAL zone: fewer distinct LSNs come back than pushes
    were made. Eight connections is what makes that fold happen at all — the
    committer drains only what is already queued when it wakes. The merge
    relocates every string into the merged batch's own heap, which a row count
    cannot check, and leaves the shared key as exactly one pushed row at
    weight 1."""
    conns, rounds, per = 8, 30, 4
    sn = schema_name
    tid = client.create_table(sn, "t", _STR)
    schema = gnitz.Schema(_STR)

    def value(pk):
        return f"v{pk}" if pk % 2 == 0 else f"long-payload-for-row-{pk}"

    lsns = [[] for _ in range(conns)]
    errors = []

    def worker(w):
        try:
            with gnitz.connect(server) as c:
                for r in range(rounds):
                    base = 1 + (w * rounds + r) * per
                    batch = gnitz.ZSetBatch(schema).extend(
                        [{"pk": pk, "val": value(pk)} for pk in range(base, base + per)]
                        + [{"pk": 0, "val": f"w{w}r{r}"}])
                    lsns[w].append(c.push(tid, batch))
        except Exception as e:  # noqa: BLE001 — re-raised below
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(conns)]
    for t in threads:
        t.start()
    join_or_fail("a concurrent push hung", *threads)
    assert not errors, errors

    got = bag(scanned(client, sn, "t"))
    [shared] = [k for k in got if k[0] == 0]
    assert shared[1] in {f"w{w}r{r}" for w in range(conns) for r in range(rounds)}
    assert got == {(pk, value(pk)): 1 for pk in range(1, 1 + conns * rounds * per)} | {shared: 1}

    # Deliberately loose: that pushes coalesce at all, not how far.
    seen = [lsn for per_conn in lsns for lsn in per_conn]
    assert len(set(seen)) <= 0.75 * len(seen), (
        f"{len(set(seen))} zones for {len(seen)} pushes: pushes did not coalesce")
