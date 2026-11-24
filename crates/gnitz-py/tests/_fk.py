"""What the foreign-key suites share: seeding a schema, writing to it, reading
it back as bags, and the refusal an FK violation raises."""

import contextlib

import pytest
import gnitz
from _read import bag, scanned
from _sql import insert


def seed(client, ddl, rows_by_table):
    client.execute_sql(ddl)
    for table, rows in rows_by_table.items():
        if rows:
            insert(client, table, rows)


def write(client, write):
    """A SQL string, or a binary `("push", table, rows)` / `("delete", table, keys)`."""
    if isinstance(write, str):
        client.execute_sql(write)
        return
    verb, table, payload = write
    tid, schema = client.resolve_table(table)
    if verb == "push":
        client.push(tid, gnitz.ZSetBatch(schema).extend(payload))
    else:
        client.delete(tid, schema, payload)


def held(client, relations):
    """Each named relation's bag."""
    return {r: bag(scanned(client, r)) for r in relations}


def want(rows_by_table):
    return {t: dict.fromkeys(rows, 1) for t, rows in rows_by_table.items()}


_VIOLATION = "(?i)foreign key"


def refused(match=_VIOLATION):
    return pytest.raises(gnitz.GnitzIntegrityError, match=match)


def check(client, ddl, seed_rows, op, after):
    """Seed, write, and hold the relations to `after`: the rows each relation
    named holds once the write lands, or — `None`, or a pattern the refusal
    matches — the write is refused and every seeded table keeps its seed."""
    seed(client, ddl, seed_rows)
    landed = isinstance(after, dict)
    with contextlib.nullcontext() if landed else refused(after or _VIOLATION):
        write(client, op)
    expected = after if landed else seed_rows
    assert held(client, expected) == want(expected)
