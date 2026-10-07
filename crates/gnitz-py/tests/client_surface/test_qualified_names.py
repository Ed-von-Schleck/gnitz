"""A relation named with its schema, from a connection whose own schema is
another: every verb that takes a relation name, and SQL."""
import pytest

import gnitz
from gnitz import aio
from _read import bag, rows
from _schemas import KV


@pytest.fixture
def items(client):
    """The qualified name of a two-row `(pk, val)` table in `client`'s schema."""
    tid = client.create_table("items", KV)
    client.push(tid, gnitz.ZSetBatch(KV).extend([{"pk": 1, "val": 10}, {"pk": 2, "val": 20}]))
    return f"{client.schema}.items"


_ITEMS = {(1, 10): 1, (2, 20): 1}


def test_a_qualified_name_reaches_a_relation_of_another_schema(server, client, items):
    """A connection in `public` resolves, reads, writes, views and drops a
    relation of another schema by its qualified name, and an unqualified name
    is still `public`'s."""
    with gnitz.connect(server) as public:
        assert public.schema == "public"
        tid, schema = public.resolve_table(items)
        assert tid == client.resolve_table("items")[0]
        assert bag(public.scan(tid, schema)) == _ITEMS
        assert bag(rows(public, f"SELECT pk, val FROM {items}")) == _ITEMS
        with pytest.raises(gnitz.GnitzNotFoundError, match="relation 'items' not found"):
            public.resolve_table("items")

        public.execute_sql(f"INSERT INTO {items} VALUES (3, 30)")
        public.execute_sql(f"UPDATE {items} SET val = 11 WHERE pk = 1")
        public.execute_sql(f"DELETE FROM {items} WHERE pk = 2")
        want = {(1, 11): 1, (3, 30): 1}
        assert bag(client.scan(tid, schema)) == want

        # A view placed beside its source, by a connection in neither's schema.
        view = f"{client.schema}.v"
        vid = public.create_view(view, items)
        assert client.resolve_table("v")[0] == vid
        assert bag(rows(client, "SELECT pk, val FROM v")) == want
        public.drop_view(view)
        public.drop_table(items)
        with pytest.raises(gnitz.GnitzNotFoundError):
            client.resolve_table("items")


@pytest.mark.parametrize("name,exc,needle", [
    ("a.b.c", gnitz.GnitzRefusedError, "Identifier contains invalid characters: b.c"),
    ("nosuchschema.items", gnitz.GnitzNotFoundError, "schema 'nosuchschema' not found"),
])
def test_a_malformed_or_unplaced_name_is_refused_as_itself(client, name, exc, needle):
    """A name the client cannot place is refused as what it is, not looked up
    in the connection's own schema."""
    with pytest.raises(exc, match=needle):
        client.resolve_table(name)


@pytest.mark.asyncio
async def test_an_event_loop_connection_names_another_schema(server, client, items):
    """The same from a `gnitz.aio` connection: one in the default schema reads
    another's relation by name, and one opened in that schema needs no
    qualifier."""
    async with aio.connect(server) as public:
        tid, schema = await public.resolve_table(items)
        assert bag(await public.scan(tid, schema)) == _ITEMS
        (res,) = await public.execute_sql(f"SELECT pk, val FROM {items}")
        assert bag(res["rows"]) == _ITEMS
    async with aio.connect(server, schema=client.schema) as conn:
        assert (await conn.resolve_table("items"))[0] == tid
