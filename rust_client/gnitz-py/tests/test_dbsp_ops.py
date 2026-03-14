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
    client.push(tid, schema, batch)


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
        cb.sink(filt, cb._view_id)
        circuit = cb.build()

        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        view_id = client.create_view_with_circuit(sn, "vf", circuit, out_cols)
        assert view_id == circuit._view_id

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
        cb.sink(red, cb._view_id)
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
        client.push(tid, schema, batch)

        result = client.scan(view_id)
        assert len(result) > 0
        # Should have 2 groups
        assert len(result) == 2

        client.drop_view(sn, "vr")
        client.drop_table(sn, tname)
        client.drop_schema(sn)
