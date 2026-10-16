"""How a test writes: one literal rule for every INSERT it builds, and one
replay rule for a scenario that keeps its own copy of what it wrote."""

from datetime import date


def lit(v):
    """One SQL literal: `None` is NULL, a `str`, `date` or `datetime` is quoted
    in its ISO spelling, anything else its Python spelling."""
    if v is None:
        return "NULL"
    if isinstance(v, (str, date)):
        return "'" + str(v).replace("'", "''") + "'"
    return str(v)


def values(rows):
    """`(a, b), (c, d)` for a VALUES clause, each value a `lit`."""
    return ", ".join("(" + ", ".join(lit(v) for v in r) + ")" for r in rows)


def insert(client, table, rows):
    """One multi-row INSERT of `rows` into `table`."""
    client.execute_sql(f"INSERT INTO {table} VALUES {values(rows)}")


def inserting(table, rows):
    """The `churn` step inserting `rows`, `{pk: rest of the row}`, into `table`;
    a scalar pk or row is one column."""
    def cols(v):
        return v if isinstance(v, tuple) else (v,)
    return f"INSERT INTO {table} VALUES {values((*cols(k), *cols(r)) for k, r in rows.items())}", table, rows


def churn(client, state, steps):
    """Run each `(statement, table, {pk: row})` step and replay it into
    `state[table]` — a `None` row deletes the pk — yielding the statement once
    both hold the step, for the caller to assert against. A step naming no table
    replays into `state` itself; a statement may be a callable taking the client,
    for a write SQL does not spell, or `None`, for a step that only observes."""
    for stmt, *table, changes in steps:
        if callable(stmt):
            stmt(client)
        elif stmt is not None:
            client.execute_sql(stmt)
        rows = state[table[0]] if table else state
        for pk, row in changes.items():
            if row is None:
                del rows[pk]
            else:
                rows[pk] = row
        yield stmt
