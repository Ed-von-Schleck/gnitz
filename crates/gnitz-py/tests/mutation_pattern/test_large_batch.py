"""One push carrying a thousand rows.
"""

import gnitz
from _uid import uid as _uid


class TestLargeBatch:

    def _setup(self, client, sn):
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tn = "t" + _uid()
        tid = client.create_table(sn, tn, cols)
        return tid, tn, schema

    def test_large_batch(self, client):
        """1000 rows pushed in a single batch all appear in scan."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, schema = self._setup(client, sn)
        try:
            batch = gnitz.ZSetBatch(schema)
            for i in range(1, 1001):
                batch.append(pk=i, val=i * 10)
            client.push(tid, batch)
            rows = list(client.scan(tid))
            assert len(rows) == 1000
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)


# ===========================================================================
