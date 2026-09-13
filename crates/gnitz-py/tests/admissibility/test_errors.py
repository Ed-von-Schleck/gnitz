"""What the catalog and the wire refuse from a client binding: a pushed schema
that disagrees with its target, a write aimed at a view, a system relation, a
base table under a live view, a reserved name, and a relation that is not
there.

The engine — not the SQL planner — has to hold these, because every path below
is a shipped client binding that validates nothing on the way in.
"""

import pytest
import gnitz
from _read import bag
from _uid import uid

_PK = gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)
_COLS = [_PK, gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
_NOT_WRITABLE = "is not writable"


@pytest.mark.parametrize("warm", [False, True], ids=["cold", "warm"])
def test_a_push_whose_pk_type_disagrees_is_rejected(client, schema_name, warm):
    """`validate_schema_match` runs on every push carrying a schema descriptor;
    which mismatches it distinguishes is pinned in Rust. What only an end-to-end
    push can show is that the descriptor reaches it on the cold path AND on the
    warm path, where a cached schema once let the push skip validation and
    reinterpret a U64-encoded PK as I64, corrupting at rest."""
    tid = client.create_table(schema_name, "t", _COLS)
    if warm:
        client.scan(tid)   # caches the table schema client-side
    signed_pk = gnitz.Schema([gnitz.ColumnDef("pk", gnitz.TypeCode.I64, primary_key=True), _COLS[1]])
    with pytest.raises(gnitz.GnitzError):
        client.push(tid, gnitz.ZSetBatch(signed_pk).extend([{"pk": 1, "val": 42}]))
    assert bag(client.scan(tid)) == {}


def test_writes_to_a_view_are_rejected_and_change_nothing(client, schema_name):
    """A view's tid lives in the same id space as a base table's, so a raw push
    or delete addressed to one would commit rows its circuit never produced. SQL
    refuses a view target in the binder; the raw API reaches only the server,
    since the client's schema cache carries no relation kind. It is one guard on
    the cold path, the warm path (schema omitted after a scan) and the
    empty-batch arm alike — so a client bug producing an empty batch fails
    instead of being masked by the no-op ACK a base table still gives."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE VIEW v AS SELECT * FROM t WHERE val > 50; "
        "INSERT INTO t VALUES (1, 100), (2, 10)", schema_name=sn)
    tid, t_schema = client.resolve_table(sn, "t")
    vid, v_schema = client.resolve_table(sn, "v")
    batch = gnitz.ZSetBatch(v_schema).extend([{"pk": 999, "val": 999}])

    with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
        client.push(vid, batch)
    assert bag(client.scan(vid)) == {(1, 100): 1}
    # Warm path: the scan above cached the view schema client-side.
    for write in (lambda: client.push(vid, batch),
                  lambda: client.delete(vid, v_schema, [1]),
                  lambda: client.push(vid, gnitz.ZSetBatch(v_schema))):
        with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
            write()
    assert bag(client.scan(vid)) == {(1, 100): 1}

    # The same empty batch against the base table is the ordinary no-op ACK.
    assert client.push(tid, gnitz.ZSetBatch(t_schema)) == 0


def test_an_absent_relation_is_a_miss_not_a_writability_failure(client, schema_name):
    """A genuinely absent tid reports 'not found' rather than 'not writable',
    and a name the client itself cannot resolve raises a catchable class, so a
    caller branches on absence without matching prose."""
    batch = gnitz.ZSetBatch(gnitz.Schema([_PK])).extend([{"pk": 1}])
    with pytest.raises(gnitz.GnitzError, match="not found"):
        client.push(99999999, batch)
    with pytest.raises(gnitz.GnitzError):
        client.scan(99999999)

    with pytest.raises(gnitz.GnitzNotFoundError) as ei:
        client.drop_table(schema_name, "nope")
    assert f"{schema_name}.nope" in str(ei.value)
    # Still a GnitzError, so an existing broad handler keeps working.
    assert isinstance(ei.value, gnitz.GnitzError)

    with pytest.raises(gnitz.GnitzError):
        client.drop_schema("nonexistent_schema_xyz")
    with pytest.raises(gnitz.GnitzError):
        client.create_table("nonexistent_schema_xyz", "t", _COLS)


def test_create_schema_rejects_a_leading_underscore(client):
    """`_` is the system prefix; `create_schema` is the one naming surface with
    no SQL planner in front of the engine's rule."""
    with pytest.raises(gnitz.GnitzError, match="cannot start with '_'"):
        client.create_schema("_reserved")


def test_system_relations_survive_a_rejected_drop(client):
    """Neither a system table nor the system schema may be dropped, and the
    rejected cascade must not have retracted a single member on its way to the
    row it was rejected on."""
    def sys_ids():
        return {r["table_id"] for r in client.scan(gnitz.TABLE_TAB)
                if r["table_id"] < gnitz.FIRST_USER_TABLE_ID}

    before = sys_ids()
    assert before, "bootstrap registered no system relations"

    with pytest.raises(gnitz.GnitzError):
        client.drop_table("_system", "_tables")
    assert sys_ids() == before

    with pytest.raises(gnitz.GnitzError):
        client.drop_schema("_system")
    assert sys_ids() == before


def test_a_live_view_blocks_dropping_its_base_even_from_another_schema(client, schema_name):
    """The dependency graph the engine derives from a view's circuit gates DROP
    TABLE and the schema cascade that would reach the table, whichever schema
    the view lives in. The rejected drops leave the table intact, and dropping
    the view lifts both."""
    other = "s" + uid()
    client.create_schema(other)
    tid = client.create_table(other, "t", _COLS)
    client.create_view(schema_name, "v", tid, gnitz.Schema(_COLS))

    with pytest.raises(gnitz.GnitzError):
        client.drop_table(other, "t")
    with pytest.raises(gnitz.GnitzError):
        client.drop_schema(other)
    assert client.resolve_table(other, "t")[0] == tid

    client.drop_view(schema_name, "v")
    client.drop_schema(other)


@pytest.mark.parametrize("tables,body,base", [
    ("t", "SELECT x.a AS xa, y.b AS yb FROM t x JOIN t y ON x.b = y.id", "t"),
    ("tuw", "SELECT t.a AS ta, w.b AS wb FROM t JOIN u ON t.b = u.id JOIN w ON u.b = w.id", "u"),
    ("tu", "WITH agg AS (SELECT a, SUM(b) AS s FROM t GROUP BY a) "
           "SELECT u.b AS ub, agg.s AS s FROM agg JOIN u ON agg.a = u.id", "t"),
], ids=["self-join-pass-through", "join-segment", "reduce-segment"])
def test_a_generated_relation_holds_its_base_until_the_view_drops(client, schema_name, tables, body, base):
    """Whatever the lowering generates — a collision pass-through, a join
    segment, a reduce segment — is a node of the dependency graph like any view:
    it holds a reference on its source, so the base refuses to drop under the
    live view, and `DROP VIEW` retires the whole bundle, so the base is free
    straight after. An orphaned generated relation would keep refusing. In the
    two segment cases `base` is reached only through the generated relation."""
    client.execute_sql(
        "; ".join(f"CREATE TABLE {n} (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
                  "b BIGINT NOT NULL)" for n in tables)
        + f"; CREATE VIEW v AS {body}", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="dependen"):
        client.execute_sql(f"DROP TABLE {base}", schema_name=schema_name)
    client.execute_sql(f"DROP VIEW v; DROP TABLE {base}", schema_name=schema_name)
