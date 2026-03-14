import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _create_table(client, sn, name=None):
    """(pk U64, val I64). Returns (tid, schema, name)."""
    if name is None:
        name = "t_" + _uid()
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, name, cols, pk_col_idx=0)
    return tid, schema, name


def _push(client, tid, schema, rows):
    """rows: [(pk, val, weight)]"""
    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend(r[0] for r in rows)
    batch.pk_hi.extend([0] * len(rows))
    batch.weights.extend(r[2] for r in rows)
    batch.nulls.extend([0] * len(rows))
    batch.columns[1].extend(r[1] for r in rows)
    client.push(tid, schema, batch)


def _scan_dict(client, target_id):
    """Return {pk: (val, weight)} for all rows with weight > 0."""
    _, batch = client.scan(target_id)
    if batch is None or len(batch.pk_lo) == 0:
        return {}
    result = {}
    for i in range(len(batch.pk_lo)):
        if batch.weights[i] == 0:
            continue
        pk = batch.pk_lo[i]
        val = batch.columns[1][i]
        result[pk] = (val, batch.weights[i])
    return result


class TestFilterView:

    def test_filter_gt_50(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, schema, tname = _create_table(client, sn)

        vid = client.allocate_table_id()
        cb = gnitz.CircuitBuilder(vid, tid)
        inp = cb.input_delta()

        eb = gnitz.ExprBuilder()
        r0 = eb.load_col_int(1)   # col 1 = val
        r1 = eb.load_const(50)
        cond = eb.cmp_gt(r0, r1)
        prog = eb.build(result_reg=cond)

        filt = cb.filter(inp, prog)
        cb.sink(filt, vid)
        circuit = cb.build()

        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        out_schema = gnitz.Schema(out_cols, pk_index=0)
        view_id = client.create_view_with_circuit(sn, "vf", circuit, out_schema)
        assert view_id == vid

        _push(client, tid, schema, [(1, 30, 1), (2, 70, 1), (3, 100, 1)])
        data = _scan_dict(client, vid)
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
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                gnitz.ColumnDef("group_id", gnitz.TypeCode.I64),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols, pk_index=0)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols, pk_col_idx=0)

        vid = client.allocate_table_id()
        cb = gnitz.CircuitBuilder(vid, tid)
        inp = cb.input_delta()
        # reduce: group_by=[col 1 = group_id], agg_col=2 (val), agg_func=1 (count)
        red = cb.reduce(inp, group_by_cols=[1], agg_func_id=1, agg_col_idx=2)
        cb.sink(red, vid)
        circuit = cb.build()

        # Output schema for reduce: (U128 hash pk, I64 group_id, I64 agg)
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128),
                    gnitz.ColumnDef("gk", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("agg", gnitz.TypeCode.I64)]
        out_schema = gnitz.Schema(out_cols, pk_index=0)
        client.create_view_with_circuit(sn, "vr", circuit, out_schema)

        # Push 4 rows: 2 in group 1, 2 in group 2
        batch = gnitz.ZSetBatch(schema)
        rows = [(1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40)]
        batch.pk_lo.extend(r[0] for r in rows)
        batch.pk_hi.extend([0] * len(rows))
        batch.weights.extend([1] * len(rows))
        batch.nulls.extend([0] * len(rows))
        batch.columns[1].extend(r[1] for r in rows)
        batch.columns[2].extend(r[2] for r in rows)
        client.push(tid, schema, batch)

        _, result = client.scan(vid)
        assert result is not None and len(result.pk_lo) > 0
        # Should have 2 groups
        assert len(result.pk_lo) == 2

        client.drop_view(sn, "vr")
        client.drop_table(sn, tname)
        client.drop_schema(sn)
