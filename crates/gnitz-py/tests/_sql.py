"""How a test writes: one literal rule for every INSERT it builds, and one
replay rule for a scenario that keeps its own copy of what it wrote."""


def _lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def values(rows):
    """`(a, b), (c, d)` for a VALUES clause: `None` is NULL, a `str` is a quoted
    string, anything else its Python spelling."""
    return ", ".join("(" + ", ".join(_lit(v) for v in r) + ")" for r in rows)


def insert(client, table, rows):
    """One multi-row INSERT of `rows` into `table`."""
    client.execute_sql(f"INSERT INTO {table} VALUES {values(rows)}")


def churn(client, state, steps):
    """Run each `(statement, table, {pk: row})` step and replay it into
    `state[table]` — a `None` row deletes the pk — yielding the statement once
    both hold the step, for the caller to assert against. A step naming no table
    replays into `state` itself, and a statement may be a callable taking the
    client, for a write SQL does not spell."""
    for stmt, *table, changes in steps:
        if callable(stmt):
            stmt(client)
        else:
            client.execute_sql(stmt)
        rows = state[table[0]] if table else state
        for pk, row in changes.items():
            if row is None:
                del rows[pk]
            else:
                rows[pk] = row
        yield stmt
