"""How a test spells rows as SQL: one literal rule for every INSERT it builds."""


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


def insert(client, sn, table, rows):
    """One multi-row INSERT of `rows` into `table`."""
    client.execute_sql(f"INSERT INTO {table} VALUES {values(rows)}", schema_name=sn)
