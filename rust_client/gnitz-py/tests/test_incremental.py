"""Retraction and incremental correctness tests.

Uses SQL views where possible; CircuitBuilder for operators SQL doesn't expose.
"""

import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _push(client, tid, schema, rows, weight=1):
    """Push [(col_kwargs...), ...] — rows is list of dicts or tuples for (pk, val)."""
    batch = gnitz.ZSetBatch(schema)
    for row in rows:
        if isinstance(row, dict):
            batch.append(weight=weight, **row)
        else:
            pk, val = row
            batch.append(pk=pk, val=val, weight=weight)
    client.push(tid, batch)


def _scan_positive(client, target_id):
    """Return all positive-weight rows sorted by pk."""
    return sorted(
        (row.pk, row.val)
        for row in client.scan(target_id)
        if row.weight > 0
    )


def _scan_reduce(client, vid):
    """Scan a reduce view; return {group_val: agg_val} for positive-weight rows.

    Reduce output schema: (pk U128, group I64, agg I64) at positions [0,1,2].
    """
    return {row[1]: row[2] for row in client.scan(vid) if row.weight > 0}


# ---------------------------------------------------------------------------
# TestFilterRetraction
# ---------------------------------------------------------------------------

class TestFilterRetraction:

    def test_delete_propagates_through_filter(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 50",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)
            assert len([r for r in client.scan(vid) if r.weight > 0]) == 1

            # Retract the row
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            schema = gnitz.Schema(cols)
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=100, weight=-1)
            client.push(tid, batch)

            assert len([r for r in client.scan(vid) if r.weight > 0]) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_delete_outside_filter_no_effect(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 50",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
            assert len([r for r in client.scan(vid) if r.weight > 0]) == 0

            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            schema = gnitz.Schema(cols)
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=10, weight=-1)
            client.push(tid, batch)

            assert len([r for r in client.scan(vid) if r.weight > 0]) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestJoinRetraction
# ---------------------------------------------------------------------------

class TestJoinRetraction:

    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT a.pk, a.val, b.label FROM a JOIN b ON a.pk = b.pk",
            schema_name=sn,
        )
        a_tid, _ = client.resolve_table(sn, "a")
        b_tid, _ = client.resolve_table(sn, "b")
        vid, _ = client.resolve_table(sn, "v")
        return a_tid, b_tid, vid

    def test_delete_propagates_through_join(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            a_tid, b_tid, vid = self._setup(client, sn)

            client.execute_sql("INSERT INTO a VALUES (1, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 999)", schema_name=sn)
            assert len([r for r in client.scan(vid) if r.weight > 0]) == 1

            a_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                      gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            a_schema = gnitz.Schema(a_cols)
            batch = gnitz.ZSetBatch(a_schema)
            batch.append(pk=1, val=100, weight=-1)
            client.push(a_tid, batch)

            assert len([r for r in client.scan(vid) if r.weight > 0]) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_trace_update_removes_join_output(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            a_tid, b_tid, vid = self._setup(client, sn)

            client.execute_sql("INSERT INTO a VALUES (1, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 999)", schema_name=sn)
            assert len([r for r in client.scan(vid) if r.weight > 0]) == 1

            b_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                      gnitz.ColumnDef("label", gnitz.TypeCode.I64)]
            b_schema = gnitz.Schema(b_cols)
            batch = gnitz.ZSetBatch(b_schema)
            batch.append(pk=1, label=999, weight=-1)
            client.push(b_tid, batch)

            assert len([r for r in client.scan(vid) if r.weight > 0]) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestReduceSumRetraction (CircuitBuilder)
# ---------------------------------------------------------------------------

def _make_grp_table(client, sn, unique_pk=False):
    """Create (pk U64 PK, grp I64, val I64) table. Returns (tid, schema, tname)."""
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tname = "t_" + _uid()
    tid = client.create_table(sn, tname, cols, unique_pk=unique_pk)
    return tid, schema, tname


def _make_reduce_view(client, sn, tid, agg_func_id, vname=None):
    """Create a reduce view: group_by=[1](grp), agg=[2](val). Returns vid."""
    if vname is None:
        vname = "v_" + _uid()
    cb = client.circuit_builder(source_table_id=tid)
    inp = cb.input_delta()
    red = cb.reduce(inp, group_by_cols=[1], agg_func_id=agg_func_id, agg_col_idx=2)
    cb.sink(red)
    circuit = cb.build()
    out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
                gnitz.ColumnDef("agg", gnitz.TypeCode.I64)]
    vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)
    return vid, vname


def _push_grp(client, tid, schema, rows, weight=1):
    """Push [(pk, grp, val), ...] with given weight."""
    batch = gnitz.ZSetBatch(schema)
    for pk, grp, val in rows:
        batch.append(pk=pk, grp=grp, val=val, weight=weight)
    client.push(tid, batch)


class TestReduceSumRetraction:

    def test_sum_retraction(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=2)  # SUM

            _push_grp(client, tid, schema, [(1, 1, 10), (2, 1, 20)])
            assert _scan_reduce(client, vid) == {1: 30}

            _push_grp(client, tid, schema, [(2, 1, 20)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 10}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_sum_full_group_elimination(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=2)

            _push_grp(client, tid, schema, [(1, 1, 10)])
            assert _scan_reduce(client, vid) == {1: 10}

            _push_grp(client, tid, schema, [(1, 1, 10)], weight=-1)
            result = _scan_reduce(client, vid)
            assert 1 not in result or result[1] == 0
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_sum_multi_tick(self, client):
        """7 ticks: exercises view store L0 compaction (fires at tick 5)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=2)

            # Tick 1: sum = 10 + 20 + 30 = 60
            _push_grp(client, tid, schema, [(1, 1, 10), (2, 1, 20), (3, 1, 30)])
            assert _scan_reduce(client, vid) == {1: 60}

            # Tick 2: sum = 60 + 25 + 25 = 110
            _push_grp(client, tid, schema, [(4, 1, 25), (5, 1, 25)])
            assert _scan_reduce(client, vid) == {1: 110}

            # Tick 3: retract pk=2,3,4,5 (sum reduction = 20+30+25+25=100); sum=10
            _push_grp(client, tid, schema,
                      [(2, 1, 20), (3, 1, 30), (4, 1, 25), (5, 1, 25)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 10}

            # Tick 4: push (6,1,15),(7,1,5) → sum = 10+15+5 = 30
            _push_grp(client, tid, schema, [(6, 1, 15), (7, 1, 5)])
            assert _scan_reduce(client, vid) == {1: 30}

            # Tick 5: retract (1,1,10) → sum = 20  [compaction fires here]
            _push_grp(client, tid, schema, [(1, 1, 10)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 20}

            # Tick 6: push (8,1,50) → sum = 70
            _push_grp(client, tid, schema, [(8, 1, 50)])
            assert _scan_reduce(client, vid) == {1: 70}

            # Tick 7: retract (6,1,15),(8,1,50) → sum = 5
            _push_grp(client, tid, schema, [(6, 1, 15), (8, 1, 50)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 5}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestReduceMinRetraction (CircuitBuilder)
# ---------------------------------------------------------------------------

class TestReduceMinRetraction:

    def test_min_retract_non_min(self, client):
        """Retract a non-min row — MIN stays the same."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=3)  # MIN

            _push_grp(client, tid, schema, [(1, 1, 3), (2, 1, 7), (3, 1, 9)])
            assert _scan_reduce(client, vid) == {1: 3}

            _push_grp(client, tid, schema, [(2, 1, 7)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 3}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_min_retract_current_min(self, client):
        """Retract the current MIN — forces history replay; new MIN = 7."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=3)

            _push_grp(client, tid, schema, [(1, 1, 3), (2, 1, 7), (3, 1, 9)])
            assert _scan_reduce(client, vid) == {1: 3}

            _push_grp(client, tid, schema, [(1, 1, 3)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 7}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_min_new_minimum(self, client):
        """Push a smaller value — MIN updates."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=3)

            _push_grp(client, tid, schema, [(1, 1, 5), (2, 1, 8)])
            assert _scan_reduce(client, vid) == {1: 5}

            _push_grp(client, tid, schema, [(3, 1, 2)])
            assert _scan_reduce(client, vid) == {1: 2}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_min_group_elimination(self, client):
        """Retract all rows — group disappears."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=3)

            _push_grp(client, tid, schema, [(1, 1, 5)])
            assert 1 in _scan_reduce(client, vid)

            _push_grp(client, tid, schema, [(1, 1, 5)], weight=-1)
            result = _scan_reduce(client, vid)
            assert 1 not in result or result[1] == 0
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_min_evolving_minimum(self, client):
        """7 ticks, each pushing a strictly lower value: exercises view store L0 compaction."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=3)  # MIN

            # Tick 1: min=100
            _push_grp(client, tid, schema, [(1, 1, 100)])
            assert _scan_reduce(client, vid) == {1: 100}

            # Tick 2: min=90
            _push_grp(client, tid, schema, [(2, 1, 90)])
            assert _scan_reduce(client, vid) == {1: 90}

            # Tick 3: min=80
            _push_grp(client, tid, schema, [(3, 1, 80)])
            assert _scan_reduce(client, vid) == {1: 80}

            # Tick 4: min=70
            _push_grp(client, tid, schema, [(4, 1, 70)])
            assert _scan_reduce(client, vid) == {1: 70}

            # Tick 5: min=60  [compaction fires here]
            _push_grp(client, tid, schema, [(5, 1, 60)])
            assert _scan_reduce(client, vid) == {1: 60}

            # Tick 6: min=50
            _push_grp(client, tid, schema, [(6, 1, 50)])
            assert _scan_reduce(client, vid) == {1: 50}

            # Tick 7: min=40
            _push_grp(client, tid, schema, [(7, 1, 40)])
            assert _scan_reduce(client, vid) == {1: 40}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestReduceMaxRetraction (CircuitBuilder)
# ---------------------------------------------------------------------------

class TestReduceMaxRetraction:

    def test_max_retract_non_max(self, client):
        """Retract a non-max row — MAX stays the same."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=4)  # MAX

            _push_grp(client, tid, schema, [(1, 1, 3), (2, 1, 7), (3, 1, 9)])
            assert _scan_reduce(client, vid) == {1: 9}

            _push_grp(client, tid, schema, [(1, 1, 3)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 9}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_max_retract_current_max(self, client):
        """Retract the current MAX — forces history replay; new MAX = 7."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=4)

            _push_grp(client, tid, schema, [(1, 1, 3), (2, 1, 7), (3, 1, 9)])
            assert _scan_reduce(client, vid) == {1: 9}

            _push_grp(client, tid, schema, [(3, 1, 9)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 7}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_max_group_elimination(self, client):
        """Retract all rows — group disappears."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=4)

            _push_grp(client, tid, schema, [(1, 1, 5)])
            assert 1 in _scan_reduce(client, vid)

            _push_grp(client, tid, schema, [(1, 1, 5)], weight=-1)
            result = _scan_reduce(client, vid)
            assert 1 not in result or result[1] == 0
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_max_evolving_maximum(self, client):
        """7 ticks, each pushing a strictly higher value: exercises view store L0 compaction."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=4)  # MAX

            # Tick 1: max=10
            _push_grp(client, tid, schema, [(1, 1, 10)])
            assert _scan_reduce(client, vid) == {1: 10}

            # Tick 2: max=20
            _push_grp(client, tid, schema, [(2, 1, 20)])
            assert _scan_reduce(client, vid) == {1: 20}

            # Tick 3: max=30
            _push_grp(client, tid, schema, [(3, 1, 30)])
            assert _scan_reduce(client, vid) == {1: 30}

            # Tick 4: max=40
            _push_grp(client, tid, schema, [(4, 1, 40)])
            assert _scan_reduce(client, vid) == {1: 40}

            # Tick 5: max=50  [compaction fires here]
            _push_grp(client, tid, schema, [(5, 1, 50)])
            assert _scan_reduce(client, vid) == {1: 50}

            # Tick 6: max=60
            _push_grp(client, tid, schema, [(6, 1, 60)])
            assert _scan_reduce(client, vid) == {1: 60}

            # Tick 7: max=70
            _push_grp(client, tid, schema, [(7, 1, 70)])
            assert _scan_reduce(client, vid) == {1: 70}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestViewCascade
# ---------------------------------------------------------------------------

class TestViewCascade:

    def test_cascade_insert(self, client):
        """T → filter view V1 → passthrough view V2; push matching row; verify both."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            v1_id, v1_schema = client.resolve_table(sn, "v1")

            # V2 is a passthrough view on top of V1
            v2_id = client.create_view(sn, "v2", v1_id, v1_schema)

            client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)

            assert len([r for r in client.scan(v1_id) if r.weight > 0]) == 1
            assert len([r for r in client.scan(v2_id) if r.weight > 0]) == 1
        finally:
            for obj in ["v2", "v1", "t"]:
                try:
                    if obj.startswith("v"):
                        client.execute_sql(f"DROP VIEW {obj}", schema_name=sn)
                    else:
                        client.execute_sql(f"DROP TABLE {obj}", schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_cascade_delete(self, client):
        """T → V1 (filter) → V2 (passthrough); retract row; both views empty."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            v1_id, v1_schema = client.resolve_table(sn, "v1")
            v2_id = client.create_view(sn, "v2", v1_id, v1_schema)

            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)
            assert len([r for r in client.scan(v1_id) if r.weight > 0]) == 1

            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            schema = gnitz.Schema(cols)
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=100, weight=-1)
            client.push(tid, batch)

            assert len([r for r in client.scan(v1_id) if r.weight > 0]) == 0
            assert len([r for r in client.scan(v2_id) if r.weight > 0]) == 0
        finally:
            for obj in ["v2", "v1", "t"]:
                try:
                    if obj.startswith("v"):
                        client.execute_sql(f"DROP VIEW {obj}", schema_name=sn)
                    else:
                        client.execute_sql(f"DROP TABLE {obj}", schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestNullThroughOperators
# ---------------------------------------------------------------------------

class TestNullThroughOperators:

    def test_null_through_filter(self, client):
        """IS NOT NULL filter excludes null-val rows."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val IS NOT NULL",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)",
                               schema_name=sn)

            positive = [r for r in client.scan(vid) if r.weight > 0]
            assert len(positive) == 2
            pks = sorted(r.pk for r in positive)
            assert pks == [1, 3]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_null_through_join(self, client):
        """Nullable column in left table is preserved in join output."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, score BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.pk, a.val, b.score FROM a JOIN b ON a.pk = b.pk",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO a VALUES (1, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 42)", schema_name=sn)

            positive = [r for r in client.scan(vid) if r.weight > 0]
            assert len(positive) == 1
            assert positive[0]["val"] is None
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_count_vs_sum_with_nulls(self, client):
        """COUNT(*) = 3, SUM(val) = sum of non-null values when one val is NULL."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_count AS SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_sum AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
                schema_name=sn,
            )
            count_vid, _ = client.resolve_table(sn, "v_count")
            sum_vid, _ = client.resolve_table(sn, "v_sum")

            # 3 rows: val=10, val=20, val=NULL
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 1, NULL)",
                schema_name=sn,
            )

            count_rows = [r for r in client.scan(count_vid) if r.weight > 0]
            assert len(count_rows) == 1
            # row[2] is the agg column (cnt)
            assert count_rows[0][2] == 3

            sum_rows = [r for r in client.scan(sum_vid) if r.weight > 0]
            assert len(sum_rows) == 1
            assert sum_rows[0][2] == 30  # 10+20, null excluded
        finally:
            for sql in ["DROP VIEW v_count", "DROP VIEW v_sum", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestAntiJoinSemantics (CircuitBuilder)
# ---------------------------------------------------------------------------

def _make_pk_val_table(client, sn, unique_pk=True):
    """Create (pk U64 PK, val I64) table. Returns (tid, schema, tname)."""
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tname = "t_" + _uid()
    tid = client.create_table(sn, tname, cols, unique_pk=unique_pk)
    return tid, schema, tname


class TestAntiJoinSemantics:

    def test_anti_join_basic(self, client):
        """delta [1,2,3], trace has [2]; anti-join output is [1,3]."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            delta_tid, d_schema, d_tname = _make_pk_val_table(client, sn)
            trace_tid, t_schema, t_tname = _make_pk_val_table(client, sn)

            # Pre-populate trace BEFORE creating the circuit
            batch = gnitz.ZSetBatch(t_schema)
            batch.append(pk=2, val=2)
            client.push(trace_tid, batch)

            cb = client.circuit_builder(source_table_id=delta_tid)
            inp = cb.input_delta()
            out = cb.anti_join(inp, trace_tid)
            cb.sink(out)
            circuit = cb.build()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, "vaj", circuit, out_cols)

            batch = gnitz.ZSetBatch(d_schema)
            for pk in [1, 2, 3]:
                batch.append(pk=pk, val=pk)
            client.push(delta_tid, batch)

            positive = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
            assert positive == [1, 3]
        finally:
            try:
                client.drop_view(sn, "vaj")
            except Exception:
                pass
            for tname in (d_tname, t_tname):
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_anti_join_all_matched(self, client):
        """delta rows = trace rows; output empty."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            delta_tid, d_schema, d_tname = _make_pk_val_table(client, sn)
            trace_tid, t_schema, t_tname = _make_pk_val_table(client, sn)

            for pk in [1, 2, 3]:
                batch = gnitz.ZSetBatch(t_schema)
                batch.append(pk=pk, val=pk)
                client.push(trace_tid, batch)

            cb = client.circuit_builder(source_table_id=delta_tid)
            inp = cb.input_delta()
            out = cb.anti_join(inp, trace_tid)
            cb.sink(out)
            circuit = cb.build()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, "vaj", circuit, out_cols)

            batch = gnitz.ZSetBatch(d_schema)
            for pk in [1, 2, 3]:
                batch.append(pk=pk, val=pk)
            client.push(delta_tid, batch)

            positive = [r for r in client.scan(vid) if r.weight > 0]
            assert len(positive) == 0
        finally:
            try:
                client.drop_view(sn, "vaj")
            except Exception:
                pass
            for tname in (d_tname, t_tname):
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_anti_join_none_matched(self, client):
        """trace empty; all delta rows pass through."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            delta_tid, d_schema, d_tname = _make_pk_val_table(client, sn)
            trace_tid, t_schema, t_tname = _make_pk_val_table(client, sn)

            cb = client.circuit_builder(source_table_id=delta_tid)
            inp = cb.input_delta()
            out = cb.anti_join(inp, trace_tid)
            cb.sink(out)
            circuit = cb.build()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, "vaj", circuit, out_cols)

            batch = gnitz.ZSetBatch(d_schema)
            for pk in [1, 2, 3]:
                batch.append(pk=pk, val=pk)
            client.push(delta_tid, batch)

            positive = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
            assert positive == [1, 2, 3]
        finally:
            try:
                client.drop_view(sn, "vaj")
            except Exception:
                pass
            for tname in (d_tname, t_tname):
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_anti_join_trace_update(self, client):
        """After updating the trace, subsequent delta pushes see updated trace."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            delta_tid, d_schema, d_tname = _make_pk_val_table(client, sn)
            trace_tid, t_schema, t_tname = _make_pk_val_table(client, sn)

            cb = client.circuit_builder(source_table_id=delta_tid)
            inp = cb.input_delta()
            out = cb.anti_join(inp, trace_tid)
            cb.sink(out)
            circuit = cb.build()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, "vaj", circuit, out_cols)

            # Tick 1: empty trace, delta [1,2] → output [1,2]
            batch = gnitz.ZSetBatch(d_schema)
            batch.append(pk=1, val=1)
            batch.append(pk=2, val=2)
            client.push(delta_tid, batch)
            positive = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
            assert positive == [1, 2]

            # Add [1] to trace
            t_batch = gnitz.ZSetBatch(t_schema)
            t_batch.append(pk=1, val=1)
            client.push(trace_tid, t_batch)

            # Tick 2: push new delta [3] — circuit fires, trace now has [1]
            # [3] is NOT in trace, so only [3] is added
            batch2 = gnitz.ZSetBatch(d_schema)
            batch2.append(pk=3, val=3)
            client.push(delta_tid, batch2)

            # The new delta row 3 should pass through (not in trace)
            positive2 = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
            assert 3 in positive2
        finally:
            try:
                client.drop_view(sn, "vaj")
            except Exception:
                pass
            for tname in (d_tname, t_tname):
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_anti_join_compacts_after_many_ticks(self, client):
        """7 ticks, one new delta row per tick, empty trace: exercises view store L0 compaction."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            delta_tid, d_schema, d_tname = _make_pk_val_table(client, sn)
            trace_tid, _t_schema, t_tname = _make_pk_val_table(client, sn)

            cb = client.circuit_builder(source_table_id=delta_tid)
            inp = cb.input_delta()
            out = cb.anti_join(inp, trace_tid)
            cb.sink(out)
            circuit = cb.build()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, "vaj2", circuit, out_cols)

            # Push one row per tick; trace stays empty so every row passes through.
            for pk in range(1, 8):
                batch = gnitz.ZSetBatch(d_schema)
                batch.append(pk=pk, val=pk)
                client.push(delta_tid, batch)

                positive = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
                assert pk in positive, f"tick {pk}: pk={pk} missing from anti-join output"

            # After 7 ticks, all PKs 1–7 must be present (compaction fired at tick 5).
            positive_final = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
            assert positive_final == list(range(1, 8))
        finally:
            try:
                client.drop_view(sn, "vaj2")
            except Exception:
                pass
            for tname in (d_tname, t_tname):
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestSemiJoinSemantics (CircuitBuilder)
# ---------------------------------------------------------------------------

class TestSemiJoinSemantics:

    def test_semi_join_basic(self, client):
        """delta [1,2,3], trace has [2]; semi-join output is [2]."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            delta_tid, d_schema, d_tname = _make_pk_val_table(client, sn)
            trace_tid, t_schema, t_tname = _make_pk_val_table(client, sn)

            batch = gnitz.ZSetBatch(t_schema)
            batch.append(pk=2, val=2)
            client.push(trace_tid, batch)

            cb = client.circuit_builder(source_table_id=delta_tid)
            inp = cb.input_delta()
            out = cb.semi_join(inp, trace_tid)
            cb.sink(out)
            circuit = cb.build()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, "vsj", circuit, out_cols)

            batch = gnitz.ZSetBatch(d_schema)
            for pk in [1, 2, 3]:
                batch.append(pk=pk, val=pk)
            client.push(delta_tid, batch)

            positive = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
            assert positive == [2]
        finally:
            try:
                client.drop_view(sn, "vsj")
            except Exception:
                pass
            for tname in (d_tname, t_tname):
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_semi_join_complement(self, client):
        """anti_join(delta,trace) ∪ semi_join(delta,trace) == delta."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            delta_tid, d_schema, d_tname = _make_pk_val_table(client, sn)
            trace_tid, t_schema, t_tname = _make_pk_val_table(client, sn)

            batch = gnitz.ZSetBatch(t_schema)
            batch.append(pk=2, val=2)
            client.push(trace_tid, batch)

            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]

            cb_aj = client.circuit_builder(source_table_id=delta_tid)
            inp_aj = cb_aj.input_delta()
            out_aj = cb_aj.anti_join(inp_aj, trace_tid)
            cb_aj.sink(out_aj)
            aj_circuit = cb_aj.build()
            aj_vid = client.create_view_with_circuit(sn, "vaj2", aj_circuit, out_cols)

            cb_sj = client.circuit_builder(source_table_id=delta_tid)
            inp_sj = cb_sj.input_delta()
            out_sj = cb_sj.semi_join(inp_sj, trace_tid)
            cb_sj.sink(out_sj)
            sj_circuit = cb_sj.build()
            sj_vid = client.create_view_with_circuit(sn, "vsj2", sj_circuit, out_cols)

            batch = gnitz.ZSetBatch(d_schema)
            for pk in [1, 2, 3]:
                batch.append(pk=pk, val=pk)
            client.push(delta_tid, batch)

            aj_pks = sorted(r.pk for r in client.scan(aj_vid) if r.weight > 0)
            sj_pks = sorted(r.pk for r in client.scan(sj_vid) if r.weight > 0)
            combined = sorted(set(aj_pks) | set(sj_pks))
            assert combined == [1, 2, 3]
            # No overlap
            assert set(aj_pks) & set(sj_pks) == set()
        finally:
            for vname in ("vaj2", "vsj2"):
                try:
                    client.drop_view(sn, vname)
                except Exception:
                    pass
            for tname in (d_tname, t_tname):
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestDistinctOperator (CircuitBuilder)
# ---------------------------------------------------------------------------

class TestDistinctOperator:

    def test_distinct_dedup(self, client):
        """Push same row twice (w=+1 each); distinct view shows weight=1."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            schema = gnitz.Schema(cols)
            tname = "t_" + _uid()
            tid = client.create_table(sn, tname, cols, unique_pk=False)

            cb = client.circuit_builder(source_table_id=tid)
            inp = cb.input_delta()
            out = cb.distinct(inp)
            cb.sink(out)
            circuit = cb.build()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, "vd", circuit, out_cols)

            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=10, weight=1)
            client.push(tid, batch)

            batch2 = gnitz.ZSetBatch(schema)
            batch2.append(pk=1, val=10, weight=1)
            client.push(tid, batch2)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
            assert rows[0].weight == 1
        finally:
            try:
                client.drop_view(sn, "vd")
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_distinct_cancellation(self, client):
        """Push row w=+1 then w=-1; distinct view shows row absent."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            schema = gnitz.Schema(cols)
            tname = "t_" + _uid()
            tid = client.create_table(sn, tname, cols, unique_pk=False)

            cb = client.circuit_builder(source_table_id=tid)
            inp = cb.input_delta()
            out = cb.distinct(inp)
            cb.sink(out)
            circuit = cb.build()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, "vd", circuit, out_cols)

            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=10, weight=1)
            client.push(tid, batch)

            batch2 = gnitz.ZSetBatch(schema)
            batch2.append(pk=1, val=10, weight=-1)
            client.push(tid, batch2)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 0
        finally:
            try:
                client.drop_view(sn, "vd")
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestStringThroughPipeline
# ---------------------------------------------------------------------------

class TestStringThroughPipeline:

    def test_string_through_join(self, client):
        """String name column is preserved in join output."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE left_t (pk BIGINT NOT NULL PRIMARY KEY, name VARCHAR NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE right_t (pk BIGINT NOT NULL PRIMARY KEY, score BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT left_t.pk, left_t.name, right_t.score "
                "FROM left_t JOIN right_t ON left_t.pk = right_t.pk",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO left_t VALUES (1, 'Alice')", schema_name=sn)
            client.execute_sql("INSERT INTO right_t VALUES (1, 100)", schema_name=sn)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE left_t", "DROP TABLE right_t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_string_then_reduce(self, client):
        """Join STRING-bearing trace, then SUM(val) GROUP BY grp via CircuitBuilder."""
        sn = "s" + _uid()
        client.create_schema(sn)
        # left: (pk U64, grp I64, val I64)
        left_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                     gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
                     gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        left_schema = gnitz.Schema(left_cols)
        # right: (pk U64, label STRING)
        right_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                      gnitz.ColumnDef("label", gnitz.TypeCode.STRING, is_nullable=True)]
        right_schema = gnitz.Schema(right_cols)

        l_tname = "lt_" + _uid()
        r_tname = "rt_" + _uid()
        l_tid = client.create_table(sn, l_tname, left_cols)
        r_tid = client.create_table(sn, r_tname, right_cols)
        try:
            # Push right (trace) rows first
            r_batch = gnitz.ZSetBatch(right_schema)
            r_batch.append(pk=1, label="A")
            r_batch.append(pk=2, label="B")
            client.push(r_tid, r_batch)

            # Circuit: join left with right, then reduce SUM(val) GROUP BY grp
            # join output: (pk, grp, val, label) → col 1=grp, col 2=val, col 3=label
            cb = client.circuit_builder(source_table_id=l_tid)
            inp = cb.input_delta()
            j = cb.join(inp, r_tid)
            red = cb.reduce(j, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(red)
            circuit = cb.build()

            vname = "v_" + _uid()
            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                        gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
                        gnitz.ColumnDef("agg", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

            l_batch = gnitz.ZSetBatch(left_schema)
            l_batch.append(pk=1, grp=10, val=100)
            l_batch.append(pk=2, grp=10, val=200)
            client.push(l_tid, l_batch)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
            # group 10, sum = 300
            assert rows[0][2] == 300
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, l_tname)
            except Exception:
                pass
            try:
                client.drop_table(sn, r_tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestChainedJoin
# ---------------------------------------------------------------------------

class TestChainedJoin:

    def test_chained_join(self, client):
        """3 tables A, B, C; circuit A → join(B) → join(C); verify merged output."""
        sn = "s" + _uid()
        client.create_schema(sn)
        # A: (pk U64, a_val I64), B: (pk U64, b_val I64), C: (pk U64, c_val I64)
        def _make_t(name):
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid = client.create_table(sn, name, cols)
            return tid, gnitz.Schema(cols)

        a_tname = "a_" + _uid()
        b_tname = "b_" + _uid()
        c_tname = "c_" + _uid()
        a_tid, a_schema = _make_t(a_tname)
        b_tid, b_schema = _make_t(b_tname)
        c_tid, c_schema = _make_t(c_tname)
        vname = "v_" + _uid()
        try:
            # Pre-populate B and C (traces)
            for tid, schema in [(b_tid, b_schema), (c_tid, c_schema)]:
                batch = gnitz.ZSetBatch(schema)
                batch.append(pk=1, val=0)
                client.push(tid, batch)

            # Circuit: join A with B → (pk, a_val, b_val), then join result with C
            # join A×B output: (pk, a_val, b_val) — 3 cols
            # join (A×B)×C output: (pk, a_val, b_val, c_val) — 4 cols
            cb = client.circuit_builder(source_table_id=a_tid)
            inp = cb.input_delta()
            j1 = cb.join(inp, b_tid)
            j2 = cb.join(j1, c_tid)
            cb.sink(j2)
            circuit = cb.build()

            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("a_val", gnitz.TypeCode.I64),
                        gnitz.ColumnDef("b_val", gnitz.TypeCode.I64),
                        gnitz.ColumnDef("c_val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

            batch = gnitz.ZSetBatch(a_schema)
            batch.append(pk=1, val=10)
            client.push(a_tid, batch)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            for tname in (a_tname, b_tname, c_tname):
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestLargeBatch
# ---------------------------------------------------------------------------

class TestLargeBatch:

    def test_large_batch_through_join(self, client):
        """1000 rows in each table; join output has 1000 matching rows."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, a_val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, b_val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.pk, a.a_val, b.b_val "
                "FROM a JOIN b ON a.pk = b.pk",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            n = 1000
            a_vals = ",".join(f"({i}, {i * 10})" for i in range(1, n + 1))
            b_vals = ",".join(f"({i}, {i * 100})" for i in range(1, n + 1))
            client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)
            client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == n
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestReduceRetraction (additional coverage via SQL)
# ---------------------------------------------------------------------------

class TestReduceRetraction:

    def test_count_retraction(self, client):
        """Push rows in a group, retract one, verify COUNT decrements."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=1)  # COUNT

            _push_grp(client, tid, schema, [(1, 1, 10), (2, 1, 20), (3, 1, 30)])
            assert _scan_reduce(client, vid) == {1: 3}

            _push_grp(client, tid, schema, [(2, 1, 20)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 2}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_multi_group_reduce(self, client):
        """Two groups, retract from group 1, verify group 2 unaffected."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=2)  # SUM

            _push_grp(client, tid, schema, [
                (1, 1, 10), (2, 1, 20),
                (3, 2, 100), (4, 2, 200),
            ])
            result = _scan_reduce(client, vid)
            assert result == {1: 30, 2: 300}

            # Retract from group 1 only
            _push_grp(client, tid, schema, [(1, 1, 10)], weight=-1)
            result = _scan_reduce(client, vid)
            assert result[1] == 20
            assert result[2] == 300  # group 2 unaffected
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_reinsertion_restores_aggregate(self, client):
        """Push, retract, re-push same row, verify SUM returns to original."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_table(client, sn)
            vid, vname = _make_reduce_view(client, sn, tid, agg_func_id=2)  # SUM

            _push_grp(client, tid, schema, [(1, 1, 10), (2, 1, 20)])
            assert _scan_reduce(client, vid) == {1: 30}

            # Retract pk=2
            _push_grp(client, tid, schema, [(2, 1, 20)], weight=-1)
            assert _scan_reduce(client, vid) == {1: 10}

            # Re-push pk=2
            _push_grp(client, tid, schema, [(2, 1, 20)])
            assert _scan_reduce(client, vid) == {1: 30}
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestReduceStringGIPath (CircuitBuilder) — GI-path regression
# ---------------------------------------------------------------------------

def _make_grp_str_table(client, sn):
    """Create (pk U64 PK, grp I64, val STRING nullable) table. unique_pk=False."""
    cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
        gnitz.ColumnDef("val", gnitz.TypeCode.STRING, is_nullable=True),
    ]
    schema = gnitz.Schema(cols)
    tname = "t_" + _uid()
    tid = client.create_table(sn, tname, cols, unique_pk=False)
    return tid, schema, tname


def _make_reduce_str_view(client, sn, tid, agg_func_id, vname=None):
    """Reduce view: group_by=[1](grp), agg=[2](val STRING). Returns (vid, vname).

    The output agg column is I64 (not STRING): the accumulator stores only the
    first 8 bytes of the German string as a comparison key (output_column_type()
    returns TYPE_I64 for MIN/MAX regardless of the input column type).
    """
    if vname is None:
        vname = "v_" + _uid()
    cb = client.circuit_builder(source_table_id=tid)
    inp = cb.input_delta()
    red = cb.reduce(inp, group_by_cols=[1], agg_func_id=agg_func_id, agg_col_idx=2)
    cb.sink(red)
    circuit = cb.build()
    out_cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
        gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
        gnitz.ColumnDef("agg", gnitz.TypeCode.I64, is_nullable=True),
    ]
    vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)
    return vid, vname


def _push_str_grp(client, tid, schema, rows, weight=1):
    """Push [(pk, grp, val_str), ...] with given weight."""
    batch = gnitz.ZSetBatch(schema)
    for pk, grp, val in rows:
        batch.append(pk=pk, grp=grp, val=val, weight=weight)
    client.push(tid, batch)


def _scan_str_reduce(client, vid):
    """Scan reduce view; return {grp_val: agg_str} for positive-weight rows."""
    return {row[1]: row[2] for row in client.scan(vid) if row.weight > 0}


class TestReduceStringGIPath:

    def test_max_string_gi_path_same_pk_multiple_payloads(self, client):
        """MAX on STRING col (GI path): history replay must loop over all payloads at each PK.

        unique_pk=False allows (PK=1, grp=1, val="apple") and
        (PK=1, grp=1, val="zebra") to coexist.  On tick 2 we retract
        "apple" from PK=1.  The GI path must seek trace_in to PK=1 and
        advance past ALL matching (PK, payload) entries — not just the first.

        Bug (if instead of while): only "apple" is collected from PK=1;
        replay = {apple+1, apple−1} → empty → MAX changes to "mango"'s
        compare key (from PK=2).

        Fix (while + advance): both "apple" and "zebra" are collected;
        replay = {apple+1, zebra+1, apple−1} → {zebra+1} → MAX unchanged.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid, schema, tname = _make_grp_str_table(client, sn)
            vid, vname = _make_reduce_str_view(client, sn, tid, agg_func_id=4)  # MAX

            # Tick 1: push apple and zebra at PK=1, mango at PK=2.
            # The output agg is the I64 compare key of whichever string wins MAX.
            _push_str_grp(client, tid, schema,
                          [(1, 1, "apple"), (1, 1, "zebra"), (2, 1, "mango")])
            r1 = _scan_str_reduce(client, vid)
            assert 1 in r1, "group 1 must have an aggregate after tick 1"

            # Tick 2: retract "apple" from PK=1.
            # GI loop must collect both apple and zebra from trace_in at PK=1,
            # then subtract apple (from delta), leaving zebra as the new MAX.
            # The MAX compare key must NOT change vs. tick 1.
            _push_str_grp(client, tid, schema, [(1, 1, "apple")], weight=-1)
            r2 = _scan_str_reduce(client, vid)
            assert r1 == r2, (
                "MAX must be unchanged after retracting apple: "
                "zebra still present at PK=1 but GI bug drops it from replay"
            )
        finally:
            try:
                client.execute_sql("DROP VIEW " + vname, schema_name=sn)
            except Exception:
                pass
            try:
                client.execute_sql("DROP TABLE " + tname, schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)
