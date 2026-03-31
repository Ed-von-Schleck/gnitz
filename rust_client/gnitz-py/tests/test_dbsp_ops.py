import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _create_table(client, sn, name=None):
    """(pk U64, val I64). Returns (tid, schema, name)."""
    if name is None:
        name = "t_" + _uid()
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, name, cols)
    return tid, schema, name


def _push(client, tid, schema, rows):
    """rows: [(pk, val, weight)]"""
    batch = gnitz.ZSetBatch(schema)
    for pk, val, w in rows:
        batch.append(pk=pk, val=val, weight=w)
    client.push(tid, batch)


def _scan_dict(client, target_id):
    """Return {pk: (val, weight)} for all rows with weight > 0."""
    result_scan = client.scan(target_id)
    result = {}
    for row in result_scan:
        if row.weight == 0:
            continue
        result[row.pk] = (row.val, row.weight)
    return result


class TestFilterView:

    def test_filter_gt_50(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, schema, tname = _create_table(client, sn)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()

        eb = gnitz.ExprBuilder()
        r0 = eb.load_col_int(1)   # col 1 = val
        r1 = eb.load_const(50)
        cond = eb.cmp_gt(r0, r1)
        prog = eb.build(result_reg=cond)

        filt = cb.filter(inp, prog)
        cb.sink(filt)
        circuit = cb.build()

        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        view_id = client.create_view_with_circuit(sn, "vf", circuit, out_cols)
        assert view_id > 0

        _push(client, tid, schema, [(1, 30, 1), (2, 70, 1), (3, 100, 1)])
        data = _scan_dict(client, view_id)
        assert 1 not in data            # 30 <= 50, filtered out
        assert 2 in data and data[2][0] == 70
        assert 3 in data and data[3][0] == 100

        client.drop_view(sn, "vf")
        client.drop_table(sn, tname)
        client.drop_schema(sn)


class TestReduceView:

    def test_sum_reduce(self, client):
        """Verify reduce view accumulates inserted rows, grouping by a non-pk I64 column."""
        sn = "s" + _uid()
        client.create_schema(sn)

        # Table: (pk U64, group_id I64, val I64) — group_by col 1 (I64)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("group_id", gnitz.TypeCode.I64),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        # reduce: group_by=[col 1 = group_id], agg_col=2 (val), agg_func=1 (count)
        red = cb.reduce(inp, group_by_cols=[1], agg_func_id=1, agg_col_idx=2)
        cb.sink(red)
        circuit = cb.build()

        # Output schema for reduce: (U128 hash pk, I64 group_id, I64 agg)
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                    gnitz.ColumnDef("gk", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("agg", gnitz.TypeCode.I64)]
        view_id = client.create_view_with_circuit(sn, "vr", circuit, out_cols)

        # Push 4 rows: 2 in group 1, 2 in group 2
        batch = gnitz.ZSetBatch(schema)
        for pk, gid, val in [(1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40)]:
            batch.append(pk=pk, group_id=gid, val=val)
        client.push(tid, batch)

        result = client.scan(view_id)
        assert len(result) > 0
        # Should have 2 groups
        assert len(result) == 2
        by_group = {row[1]: row[2] for row in result if row.weight > 0}
        assert by_group[1] == 2
        assert by_group[2] == 2

        client.drop_view(sn, "vr")
        client.drop_table(sn, tname)
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestMapOperator
# ---------------------------------------------------------------------------

class TestMapOperator:

    def test_map_projection_reorder(self, client):
        """Map view with projection=[2, 1] swaps a/b payload columns."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("a", gnitz.TypeCode.I64),
                gnitz.ColumnDef("b", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        out = cb.map(inp, projection=[2, 1])  # swap a and b (1-indexed payload cols)
        cb.sink(out)
        circuit = cb.build()

        # Output: (pk, b, a)
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("b", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("a", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, "vm", circuit, out_cols)

        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, a=10, b=20)
        client.push(tid, batch)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 1
        # b=20 now at position 1, a=10 at position 2
        assert rows[0][1] == 20  # b
        assert rows[0][2] == 10  # a

        client.drop_view(sn, "vm")
        client.drop_table(sn, tname)
        client.drop_schema(sn)

    def test_map_projection_subset(self, client):
        """Map view with projection=[1] keeps only first payload column."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("a", gnitz.TypeCode.I64),
                gnitz.ColumnDef("b", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        out = cb.map(inp, projection=[1])  # only a
        cb.sink(out)
        circuit = cb.build()

        # Output: (pk, a)
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("a", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, "vm2", circuit, out_cols)

        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, a=10, b=99)
        client.push(tid, batch)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 1
        assert rows[0].a == 10

        client.drop_view(sn, "vm2")
        client.drop_table(sn, tname)
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestNegateOperator
# ---------------------------------------------------------------------------

class TestNegateOperator:

    def test_negate_flips_weights(self, client):
        """Negate maps w=-1 retraction to w=+1 (visible in scan)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols, unique_pk=False)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        out = cb.negate(inp)
        cb.sink(out)
        circuit = cb.build()

        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, "vn", circuit, out_cols)

        # Push w=-1 (retraction): negate produces w=+1, visible in scan
        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, val=10, weight=-1)
        client.push(tid, batch)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 1
        assert rows[0].pk == 1

        client.drop_view(sn, "vn")
        client.drop_table(sn, tname)
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestJoinOperator
# ---------------------------------------------------------------------------

class TestJoinOperator:

    def test_join_output_columns(self, client):
        """Join two tables; output has left PK + left payload + right payload."""
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
                "CREATE VIEW v AS SELECT a.pk, a.a_val, b.b_val FROM a JOIN b ON a.pk = b.pk",
                schema_name=sn,
            )
            vid, v_schema = client.resolve_table(sn, "v")
            # _join_pk (U128 synthetic) + pk + a_val + b_val = 4 columns
            assert len(v_schema.columns) == 4

            client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 20)", schema_name=sn)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
            row = rows[0]  # schema: _join_pk (U128), pk, a_val, b_val
            assert row[2] == 10  # a_val from table a
            assert row[3] == 20  # b_val from table b
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_join_no_matches(self, client):
        """Push disjoint PKs; join output empty."""
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
                "CREATE VIEW v AS SELECT a.pk, a.a_val, b.b_val FROM a JOIN b ON a.pk = b.pk",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (2, 20)", schema_name=sn)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestOperatorCombinations
# ---------------------------------------------------------------------------

class TestOperatorCombinations:

    def test_filter_then_map(self, client):
        """Filter (val > 10) then map (reorder cols); only matching rows in correct order."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("a", gnitz.TypeCode.I64),
                gnitz.ColumnDef("b", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols)

        eb = gnitz.ExprBuilder()
        r0 = eb.load_col_int(1)   # a is col 1
        r1 = eb.load_const(10)
        cond = eb.cmp_gt(r0, r1)
        prog = eb.build(result_reg=cond)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        filt = cb.filter(inp, prog)
        mapped = cb.map(filt, projection=[2, 1])  # swap a and b
        cb.sink(mapped)
        circuit = cb.build()

        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("b", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("a", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, "vfm", circuit, out_cols)

        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, a=5, b=50)   # filtered out (a=5 <= 10)
        batch.append(pk=2, a=20, b=200)  # passes (a=20 > 10)
        client.push(tid, batch)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 1
        assert rows[0].pk == 2
        assert rows[0][1] == 200  # b at position 1
        assert rows[0][2] == 20   # a at position 2

        client.drop_view(sn, "vfm")
        client.drop_table(sn, tname)
        client.drop_schema(sn)

    def test_join_then_reduce(self, client):
        """Join two tables, then SUM(val) GROUP BY group_id via CircuitBuilder."""
        sn = "s" + _uid()
        client.create_schema(sn)
        a_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                  gnitz.ColumnDef("group_id", gnitz.TypeCode.I64),
                  gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        b_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                  gnitz.ColumnDef("extra", gnitz.TypeCode.I64)]
        a_schema = gnitz.Schema(a_cols)
        b_schema = gnitz.Schema(b_cols)
        a_tname = "a_" + _uid()
        b_tname = "b_" + _uid()
        a_tid = client.create_table(sn, a_tname, a_cols)
        b_tid = client.create_table(sn, b_tname, b_cols)
        vname = "v_" + _uid()
        try:
            # Pre-populate b (trace)
            b_batch = gnitz.ZSetBatch(b_schema)
            b_batch.append(pk=1, extra=0)
            b_batch.append(pk=2, extra=0)
            b_batch.append(pk=3, extra=0)
            client.push(b_tid, b_batch)

            # join output: (pk, group_id, val, extra) → col 1=group_id, col 2=val
            cb = client.circuit_builder(source_table_id=a_tid)
            inp = cb.input_delta()
            j = cb.join(inp, b_tid)
            red = cb.reduce(j, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(red)
            circuit = cb.build()

            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                        gnitz.ColumnDef("group_id", gnitz.TypeCode.I64),
                        gnitz.ColumnDef("total", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

            a_batch = gnitz.ZSetBatch(a_schema)
            a_batch.append(pk=1, group_id=10, val=100)
            a_batch.append(pk=2, group_id=10, val=200)
            a_batch.append(pk=3, group_id=20, val=50)
            client.push(a_tid, a_batch)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            totals = {row[1]: row[2] for row in rows}
            # group 10: 100+200=300, group 20: 50
            assert totals[10] == 300
            assert totals[20] == 50
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            for tname in [a_tname, b_tname]:
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_two_views_same_table(self, client):
        """Two separate circuits over same table; both get pushed data."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols)

        out_cols = cols

        # View 1: filter val > 10
        eb1 = gnitz.ExprBuilder()
        r0 = eb1.load_col_int(1)
        r1 = eb1.load_const(10)
        cond1 = eb1.cmp_gt(r0, r1)
        prog1 = eb1.build(result_reg=cond1)

        cb1 = client.circuit_builder(source_table_id=tid)
        filt1 = cb1.filter(cb1.input_delta(), prog1)
        cb1.sink(filt1)
        vid1 = client.create_view_with_circuit(sn, "v1", cb1.build(), out_cols)

        # View 2: filter val > 50
        eb2 = gnitz.ExprBuilder()
        r0b = eb2.load_col_int(1)
        r1b = eb2.load_const(50)
        cond2 = eb2.cmp_gt(r0b, r1b)
        prog2 = eb2.build(result_reg=cond2)

        cb2 = client.circuit_builder(source_table_id=tid)
        filt2 = cb2.filter(cb2.input_delta(), prog2)
        cb2.sink(filt2)
        vid2 = client.create_view_with_circuit(sn, "v2", cb2.build(), out_cols)

        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, val=5)
        batch.append(pk=2, val=30)
        batch.append(pk=3, val=100)
        client.push(tid, batch)

        v1_pks = sorted(r.pk for r in client.scan(vid1) if r.weight > 0)
        v2_pks = sorted(r.pk for r in client.scan(vid2) if r.weight > 0)

        assert v1_pks == [2, 3]   # val > 10
        assert v2_pks == [3]       # val > 50

        client.drop_view(sn, "v1")
        client.drop_view(sn, "v2")
        client.drop_table(sn, tname)
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestFilterRetraction
# ---------------------------------------------------------------------------

class TestFilterRetraction:

    def test_filter_retraction(self, client):
        """Push rows, verify filter, retract a passing row, verify it disappears."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols, unique_pk=False)

        eb = gnitz.ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(50)
        cond = eb.cmp_gt(r0, r1)
        prog = eb.build(result_reg=cond)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        filt = cb.filter(inp, prog)
        cb.sink(filt)
        circuit = cb.build()

        vid = client.create_view_with_circuit(sn, "vfr", circuit, cols)

        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, val=30, weight=1)
        batch.append(pk=2, val=70, weight=1)
        batch.append(pk=3, val=100, weight=1)
        client.push(tid, batch)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        pks = sorted(r.pk for r in rows)
        assert pks == [2, 3]

        # Retract pk=2 (val=70) — should disappear from the view
        batch2 = gnitz.ZSetBatch(schema)
        batch2.append(pk=2, val=70, weight=-1)
        client.push(tid, batch2)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        pks = sorted(r.pk for r in rows)
        assert pks == [3]

        client.drop_view(sn, "vfr")
        client.drop_table(sn, tname)
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestDistinctOperator
# ---------------------------------------------------------------------------

class TestDistinctOperator:

    def test_distinct_operator(self, client):
        """Push row with weight=1 twice, verify distinct output has weight=1.
        Retract once, verify still positive. Retract again, verify gone."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        # unique_pk=False so pushes accumulate weight instead of upserting
        tid = client.create_table(sn, tname, cols, unique_pk=False)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        out = cb.distinct(inp)
        cb.sink(out)
        circuit = cb.build()

        vid = client.create_view_with_circuit(sn, "vd", circuit, cols)

        # Push weight=+1
        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, val=10, weight=1)
        client.push(tid, batch)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 1

        # Push another weight=+1 (same row, accumulates to w=2 in base)
        batch2 = gnitz.ZSetBatch(schema)
        batch2.append(pk=1, val=10, weight=1)
        client.push(tid, batch2)

        # Distinct still shows weight=1
        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 1

        # Retract once (base goes to w=1, distinct still positive)
        batch3 = gnitz.ZSetBatch(schema)
        batch3.append(pk=1, val=10, weight=-1)
        client.push(tid, batch3)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 1

        # Retract again (base goes to w=0, distinct should vanish)
        batch4 = gnitz.ZSetBatch(schema)
        batch4.append(pk=1, val=10, weight=-1)
        client.push(tid, batch4)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 0

        client.drop_view(sn, "vd")
        client.drop_table(sn, tname)
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestUnionOperator
# ---------------------------------------------------------------------------


class TestUnionOperator:

    def test_union_same_pk_different_payload(self, client):
        """Union must preserve (PK, payload) sort order when same PK appears in both branches."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [
            gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("a", gnitz.TypeCode.I64),
            gnitz.ColumnDef("b", gnitz.TypeCode.I64),
        ]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        # Multiple rows with same PK but different payloads are needed to exercise the bug.
        tid = client.create_table(sn, tname, cols, unique_pk=False)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        left = cb.map(inp, projection=[2, 1])  # (pk, b, a) — swapped payloads
        unioned = cb.union(left, inp)
        cb.sink(unioned)
        circuit = cb.build()

        out_cols = [
            gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("a", gnitz.TypeCode.I64),
            gnitz.ColumnDef("b", gnitz.TypeCode.I64),
        ]
        vid = client.create_view_with_circuit(sn, "vu", circuit, out_cols)

        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, a=10, b=20, weight=1)
        client.push(tid, batch)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 2
        payloads = {(r[1], r[2]) for r in rows}
        assert (10, 20) in payloads
        assert (20, 10) in payloads

        retract = gnitz.ZSetBatch(schema)
        retract.append(pk=1, a=10, b=20, weight=-1)
        client.push(tid, retract)

        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 0

        client.drop_view(sn, "vu")
        client.drop_table(sn, tname)
        client.drop_schema(sn)
