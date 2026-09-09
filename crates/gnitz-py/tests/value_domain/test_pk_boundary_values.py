"""PK and payload values at the edges of their domain: 0, u64::MAX, and negative
payloads.

A PK is an order-preserving byte string, so 0 and u64::MAX are the two ends of
its encoding, and a negative payload is where a sign-extended comparison and a
raw one disagree.
"""

import gnitz
from _uid import uid as _uid


class TestPkBoundaryValues:

    def _setup(self, client, sn):
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tn = "t" + _uid()
        tid = client.create_table(sn, tn, cols)
        return tid, tn, schema

    def test_insert_zero_pk(self, client):
        """PK value of 0 is valid and round-trips correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, schema = self._setup(client, sn)
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=0, val=42)
            client.push(tid, batch)
            rows = list(client.scan(tid))
            assert len(rows) == 1
            assert rows[0].pk == 0
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_insert_max_u64_pk(self, client):
        """PK value of 2^64 - 1 round-trips correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, schema = self._setup(client, sn)
        max_u64 = (1 << 64) - 1
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=max_u64, val=99)
            client.push(tid, batch)
            rows = list(client.scan(tid))
            assert len(rows) == 1
            assert rows[0].pk == max_u64
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_insert_negative_values(self, client):
        """Negative I64 payload values round-trip correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, schema = self._setup(client, sn)
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=-100)
            batch.append(pk=2, val=-(2**62))
            client.push(tid, batch)
            vals = {r.pk: r.val for r in list(client.scan(tid))}
            assert vals[1] == -100
            assert vals[2] == -(2**62)
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)
