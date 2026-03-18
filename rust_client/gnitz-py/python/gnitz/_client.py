import gnitz._native as _native
from gnitz._types import Schema, _to_native_col, _to_native_schema, _from_native_schema
from gnitz._batch import ScanResult, ZSetBatch
from gnitz._builders import CircuitBuilder


class Connection:
    def __init__(self, socket_path: str):
        self._client = _native.GnitzClient(socket_path)

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
        return False

    # DDL

    def create_schema(self, name):
        return self._client.create_schema(name)

    def drop_schema(self, name):
        self._client.drop_schema(name)

    def create_table(self, schema_name, table_name, columns, unique_pk=True):
        pk_idx = next((i for i, c in enumerate(columns) if c.primary_key), 0)
        return self._client.create_table(
            schema_name, table_name,
            [_to_native_col(c) for c in columns],
            pk_col_idx=pk_idx,
            unique_pk=unique_pk,
        )

    def drop_table(self, schema_name, table_name):
        self._client.drop_table(schema_name, table_name)

    # DML

    def push(self, target_id, batch):
        self._client.push(target_id, _to_native_schema(batch._schema), batch._raw)

    def scan(self, target_id):
        native_schema, batch = self._client.scan(target_id)
        if native_schema is None:
            return ScanResult(None, None)
        return ScanResult(_from_native_schema(native_schema), batch)

    def delete(self, target_id, schema, pks):
        self._client.delete(target_id, _to_native_schema(schema), pks)

    # Views

    def circuit_builder(self, source_table_id):
        return CircuitBuilder(source_table_id)

    def create_view(self, schema_name, view_name, source_table_id, output_schema):
        return self._client.create_view(
            schema_name, view_name, source_table_id,
            _to_native_schema(output_schema),
        )

    def create_view_with_circuit(self, schema_name, view_name, circuit, columns):
        if isinstance(columns, Schema):
            s = _to_native_schema(columns)
        else:
            s = _to_native_schema(Schema(columns))
        return self._client.create_view_with_circuit(
            schema_name, view_name, circuit._graph, s,
        )

    def drop_view(self, schema_name, view_name):
        self._client.drop_view(schema_name, view_name)

    def resolve_table(self, schema_name, table_name):
        tid, ns = self._client.resolve_table_id(schema_name, table_name)
        return tid, _from_native_schema(ns)

    def resolve_table_id(self, schema_name, table_name):
        return self.resolve_table(schema_name, table_name)

    def seek(self, table_id: int, pk: int = 0) -> "ScanResult":
        pk_lo = pk & 0xFFFFFFFFFFFFFFFF
        pk_hi = (pk >> 64) & 0xFFFFFFFFFFFFFFFF
        native_schema, batch = self._client.seek(table_id, pk_lo, pk_hi)
        if native_schema is None:
            return ScanResult(None, None)
        return ScanResult(_from_native_schema(native_schema), batch)

    def seek_by_index(self, table_id: int, col_idx: int, key: int = 0) -> "ScanResult":
        key_lo = key & 0xFFFFFFFFFFFFFFFF
        key_hi = (key >> 64) & 0xFFFFFFFFFFFFFFFF
        native_schema, batch = self._client.seek_by_index(table_id, col_idx, key_lo, key_hi)
        if native_schema is None:
            return ScanResult(None, None)
        return ScanResult(_from_native_schema(native_schema), batch)

    def execute_sql(self, sql: str, schema_name: str = "public") -> list:
        results = self._client.execute_sql(schema_name, sql)
        for r in results:
            if isinstance(r, dict) and r.get("type") == "Rows":
                r["rows"] = ScanResult(_from_native_schema(r.pop("schema")), r.pop("batch"))
        return results

    # Low-level

    def allocate_table_id(self):
        return self._client.allocate_table_id()

    def allocate_schema_id(self):
        return self._client.allocate_schema_id()
