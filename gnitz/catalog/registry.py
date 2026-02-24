# gnitz/catalog/registry.py

from rpython.rlib.objectmodel import newlist_hint
from gnitz.catalog.system_tables import (
    FIRST_USER_SCHEMA_ID,
    FIRST_USER_TABLE_ID,
)


class SystemTables(object):
    """
    Container for the seven core system tables.
    Used during bootstrap and registry rebuilding.
    """

    def __init__(self, schemas, tables, views, columns, indices, view_deps, sequences):
        self.schemas = schemas  # PersistentTable
        self.tables = tables
        self.views = views
        self.columns = columns
        self.indices = indices
        self.view_deps = view_deps
        self.sequences = sequences

    def close(self):
        self.schemas.close()
        self.tables.close()
        self.views.close()
        self.columns.close()
        self.indices.close()
        self.view_deps.close()
        self.sequences.close()


class TableFamily(object):
    """
    A logical table entity. Phase A is a thin wrapper around a single 
    PersistentTable, but carries the metadata required to emit exact 
    Z-Set retractions during DROP TABLE operations.
    """

    _immutable_fields_ = [
        "schema_name",
        "table_name",
        "table_id",
        "schema_id",
        "directory",
        "pk_col_idx",
        "primary",
        "schema",
    ]

    def __init__(
        self,
        schema_name,
        table_name,
        table_id,
        schema_id,
        directory,
        pk_col_idx,
        primary,
    ):
        self.schema_name = schema_name
        self.table_name = table_name
        self.table_id = table_id
        self.schema_id = schema_id
        self.directory = directory
        self.pk_col_idx = pk_col_idx
        self.created_lsn = 0  # Phase A: logic for LSN tracking is deferred
        self.primary = primary  # PersistentTable
        self.schema = primary.schema

    def get_schema(self):
        return self.schema

    def create_cursor(self):
        return self.primary.create_cursor()

    def ingest_batch(self, batch):
        self.primary.ingest_batch(batch)

    def flush(self):
        return self.primary.flush()

    def close(self):
        self.primary.close()


class EntityRegistry(object):
    """
    Central directory for mapping names and IDs to catalog entities.
    Tracks schema and table ID sequences to prevent collisions.
    """

    def __init__(self):
        # Maps "schema.table" -> TableFamily
        self._by_name = {}
        # Maps table_id (int) -> TableFamily
        self._by_id = {}
        # Schema name -> schema_id (int)
        self._schema_name_to_id = {}
        # Schema_id (int) -> schema name
        self._schema_id_to_name = {}

        # ID allocators
        self._next_schema_id = FIRST_USER_SCHEMA_ID
        self._next_table_id = FIRST_USER_TABLE_ID

    # ── Schema management ────────────────────────────────────────────────

    def register_schema(self, schema_id, name):
        self._schema_name_to_id[name] = schema_id
        self._schema_id_to_name[schema_id] = name
        # Keep counter ahead of known IDs to support restarts
        if schema_id >= self._next_schema_id:
            self._next_schema_id = schema_id + 1

    def unregister_schema(self, name, schema_id):
        if name in self._schema_name_to_id:
            del self._schema_name_to_id[name]
        if schema_id in self._schema_id_to_name:
            del self._schema_id_to_name[schema_id]

    def has_schema(self, name):
        return name in self._schema_name_to_id

    def get_schema_id(self, name):
        # RPython: explicit check instead of .get(k, default) for type proof
        if name in self._schema_name_to_id:
            return self._schema_name_to_id[name]
        return -1

    def get_schema_name(self, schema_id):
        if schema_id in self._schema_id_to_name:
            return self._schema_id_to_name[schema_id]
        return ""

    def allocate_schema_id(self):
        sid = self._next_schema_id
        self._next_schema_id += 1
        return sid

    # ── Table management ─────────────────────────────────────────────────

    def register(self, family):
        qualified = family.schema_name + "." + family.table_name
        self._by_name[qualified] = family
        self._by_id[family.table_id] = family
        # Advance counter if this ID is >= current pointer
        if family.table_id >= self._next_table_id:
            self._next_table_id = family.table_id + 1

    def unregister(self, schema_name, table_name):
        qualified = schema_name + "." + table_name
        if qualified in self._by_name:
            family = self._by_name[qualified]
            del self._by_name[qualified]
            tid = family.table_id
            if tid in self._by_id:
                del self._by_id[tid]

    def has(self, schema_name, table_name):
        qualified = schema_name + "." + table_name
        return qualified in self._by_name

    def get(self, schema_name, table_name):
        qualified = schema_name + "." + table_name
        return self._by_name[qualified]

    def get_by_id(self, table_id):
        return self._by_id[table_id]

    def has_id(self, table_id):
        return table_id in self._by_id

    def allocate_table_id(self):
        tid = self._next_table_id
        self._next_table_id += 1
        return tid

    def schema_is_empty(self, schema_name):
        """
        Returns True if no TableFamily exists in this schema.
        Iterates over keys; safe in Phase A (single-threaded).
        """
        for k in self._by_name:
            family = self._by_name[k]
            if family.schema_name == schema_name:
                return False
        return True
