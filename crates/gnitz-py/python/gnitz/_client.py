import gnitz._native as _native
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
        if isinstance(columns, type) and hasattr(columns, '_columns'):
            columns = columns._columns
        pk_idx = next((i for i, c in enumerate(columns) if c.primary_key), 0)
        return self._client.create_table(
            schema_name, table_name,
            list(columns),
            pk_col_idx=pk_idx,
            unique_pk=unique_pk,
        )

    def drop_table(self, schema_name, table_name):
        self._client.drop_table(schema_name, table_name)

    # Migrations

    def push_migration(self, parent_hash, desired_state_sql, author, message):
        """Submit a schema migration and return the committed hash.

        parent_hash=0 is the genesis commit; otherwise pass the hash
        returned by the previous successful push_migration. Supports
        CREATE/DROP TABLE and CREATE/DROP INDEX; CREATE/DROP VIEW in
        migration SQL is rejected (use the non-migration create_view
        path for views).
        """
        return self._client.push_migration(
            parent_hash, desired_state_sql, author, message,
        )

    # DML

    def push(self, target_id, batch):
        return self._client.push(target_id, batch._raw)

    def scan(self, target_id):
        return self._client.scan_lazy(target_id)

    def delete(self, target_id, schema, pks):
        self._client.delete(target_id, schema, pks)

    # Views

    def circuit_builder(self, source_table_id):
        return CircuitBuilder(source_table_id)

    def create_view(self, schema_name, view_name, source_table_id, output_schema):
        return self._client.create_view(
            schema_name, view_name, source_table_id,
            output_schema,
        )

    def create_view_with_circuit(self, schema_name, view_name, circuit, columns):
        if isinstance(columns, type) and hasattr(columns, '_schema'):
            columns = columns._schema
        elif not isinstance(columns, _native.Schema):
            columns = _native.Schema(list(columns))
        return self._client.create_view_with_circuit(
            schema_name, view_name, circuit._graph, columns,
        )

    def drop_view(self, schema_name, view_name):
        self._client.drop_view(schema_name, view_name)

    def resolve_table(self, schema_name, table_name):
        tid, ns = self._client.resolve_table_id(schema_name, table_name)
        return tid, ns

    def resolve_table_id(self, schema_name, table_name):
        return self.resolve_table(schema_name, table_name)

    def seek(self, table_id: int, pk: int = 0):
        pk_lo = pk & 0xFFFFFFFFFFFFFFFF
        pk_hi = (pk >> 64) & 0xFFFFFFFFFFFFFFFF
        return self._client.seek_lazy(table_id, pk_lo, pk_hi)

    def seek_by_index(self, table_id: int, col_idx: int, key: int = 0):
        key_lo = key & 0xFFFFFFFFFFFFFFFF
        key_hi = (key >> 64) & 0xFFFFFFFFFFFFFFFF
        return self._client.seek_by_index_lazy(table_id, col_idx, key_lo, key_hi)

    def execute_sql(self, sql: str, schema_name: str = "public") -> list:
        return self._client.execute_sql(schema_name, sql)

    def submit_sql(self, sql: str, schema_name: str = "public") -> None:
        """Submit raw SQL to the server for atomic DDL application.

        Routes through FLAG_EXECUTE_SQL → server-side parse + migration
        apply. Phase 2 supports DDL (CREATE/DROP TABLE / INDEX) only;
        DML and mixed DDL+DML are rejected. Phase 4 will collapse the
        client-side `execute_sql` into this path and rename.
        """
        return self._client.submit_sql(schema_name, sql)

    # Low-level

    def allocate_table_id(self):
        return self._client.allocate_table_id()

    def allocate_schema_id(self):
        return self._client.allocate_schema_id()
