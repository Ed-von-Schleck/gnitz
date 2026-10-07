"""What the foreign-key suites share: seeding a schema, reading it back as
bags, and the refusal an FK violation raises."""

import pytest
import gnitz
from _read import bag, scanned
from _sql import insert


def seed(client, ddl, rows_by_table):
    client.execute_sql(ddl)
    for table, rows in rows_by_table.items():
        if rows:
            insert(client, table, rows)


def held(client, relations):
    """Each named relation's bag."""
    return {r: bag(scanned(client, r)) for r in relations}


def want(rows_by_table):
    return {t: dict.fromkeys(rows, 1) for t, rows in rows_by_table.items()}


def refused():
    return pytest.raises(gnitz.GnitzIntegrityError, match="(?i)foreign key")
