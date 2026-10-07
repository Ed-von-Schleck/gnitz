"""How a test reads the catalog through SQL: the system families as the
relations of `_system`."""

from _read import bag, rows


def schema_id(client, name=None):
    """The id of schema `name`, the client's own by default."""
    name = name or client.schema
    (row,) = rows(client, f"SELECT schema_id FROM _system.schemas WHERE name = '{name}'")
    return row.schema_id


def family(client, name, *cols, where=None):
    """The bag of `cols` over family `name`, read ad hoc — what a view over the
    same rows must equal at every point."""
    q = f"SELECT {', '.join(cols)} FROM _system.{name}"
    if where:
        q += f" WHERE {where}"
    return bag(rows(client, q), *cols)
