# gnitz/catalog/loader.py

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.batch import ZSetBatch
from gnitz.catalog import system_tables as sys
from gnitz.catalog.metadata import read_column_defs

# Dispatch constants to fix RPython TyperError with bound methods
KIND_SCHEMA = 1
KIND_TABLE = 2
KIND_VIEW = 3
KIND_INDEX = 4


class CatalogBootstrapper(object):
    """
    Coordinates the transition from raw disk files to a live in-memory Registry.

    Unifies the startup sequence by replaying the durable System Z-Sets
    through the same reactive handlers used at runtime, ensuring symmetry
    and fixing the 'Resurrection Bug' by filtering out tombstone records.
    """

    _immutable_fields_ = ["registry", "sys_tables", "base_dir"]

    def __init__(self, registry, sys_tables, base_dir):
        self.registry = registry
        self.sys_tables = sys_tables
        self.base_dir = base_dir

    def recover_system_state(self):
        """
        Phase 1: Recovers sequence counters and registers core system tables.
        This must be performed before the reactive replay.
        """
        # 1. Recover sequence high-water marks (Counters)
        cursor = self.sys_tables.sequences.create_cursor()
        try:
            while cursor.is_valid():
                if cursor.weight() > 0:
                    acc = cursor.get_accessor()
                    seq_id = intmask(r_uint64(cursor.key()))
                    val = intmask(acc.get_int(sys.SeqTab.COL_VALUE))

                    if seq_id == sys.SEQ_ID_SCHEMAS:
                        self.registry._next_schema_id = val + 1
                    elif seq_id == sys.SEQ_ID_TABLES:
                        self.registry._next_table_id = val + 1
                    elif seq_id == sys.SEQ_ID_INDICES:
                        self.registry._next_index_id = val + 1
                    elif seq_id == sys.SEQ_ID_PROGRAMS:
                        if val + 1 > self.registry._next_table_id:
                            self.registry._next_table_id = val + 1
                cursor.advance()
        finally:
            cursor.close()

        # 2. Register Foundation System Tables into the Registry.
        # This allows handlers to find the tables they need (like ColTab)
        # during the replay phase.
        sys_dir = self.base_dir + "/" + sys.SYS_CATALOG_DIRNAME

        from gnitz.catalog.registry import TableFamily

        for pt, t in [
            (self.sys_tables.schemas, sys.SchemaTab),
            (self.sys_tables.tables, sys.TableTab),
            (self.sys_tables.views, sys.ViewTab),
            (self.sys_tables.columns, sys.ColTab),
            (self.sys_tables.indices, sys.IdxTab),
            (self.sys_tables.view_deps, sys.DepTab),
            (self.sys_tables.sequences, sys.SeqTab),
            (self.sys_tables.functions, sys.FuncTab),
            (self.sys_tables.subscriptions, sys.SubTab),
            (self.sys_tables.circuit_nodes, sys.CircuitNodesTab),
            (self.sys_tables.circuit_edges, sys.CircuitEdgesTab),
            (self.sys_tables.circuit_sources, sys.CircuitSourcesTab),
            (self.sys_tables.circuit_params, sys.CircuitParamsTab),
            (self.sys_tables.circuit_group_cols, sys.CircuitGroupColsTab),
        ]:
            self.registry.register(
                TableFamily(
                    "_system",
                    t.NAME,
                    t.ID,
                    sys.SYSTEM_SCHEMA_ID,
                    sys_dir + "/" + t.SUBDIR,
                    0,
                    pt,
                )
            )

    def replay_catalog(self, handlers):
        # Strict ordering to respect relational dependencies (Schemas first).
        self._replay_store(self.sys_tables.schemas, handlers, KIND_SCHEMA)
        self._replay_store(self.sys_tables.tables, handlers, KIND_TABLE)
        self._replay_store(self.sys_tables.views, handlers, KIND_VIEW)
        self._replay_store(self.sys_tables.indices, handlers, KIND_INDEX)

    def _replay_store(self, store, handlers, kind):
        """Scans a system store and feeds active records into the reactive loop."""
        cursor = store.create_cursor()
        schema = store.get_schema()

        batch = ZSetBatch(schema)
        try:
            while cursor.is_valid():
                if cursor.weight() > 0:
                    batch.append_from_accessor(
                        cursor.key(),
                        cursor.weight(),
                        cursor.get_accessor(),
                    )

                    if batch.length() >= 512:
                        self._dispatch(handlers, kind, batch)
                        batch.free()
                        batch = ZSetBatch(schema)

                cursor.advance()

            if batch.length() > 0:
                self._dispatch(handlers, kind, batch)
        finally:
            batch.free()
            cursor.close()

    def _dispatch(self, handlers, kind, batch):
        """
        Explicit dispatcher to satisfy RPython's monomorphism constraints.
        Bound methods of the same class cannot be treated as anonymous callables.
        """
        if kind == KIND_SCHEMA:
            handlers.on_schema_delta(batch)
        elif kind == KIND_TABLE:
            handlers.on_table_delta(batch)
        elif kind == KIND_VIEW:
            handlers.on_view_delta(batch)
        elif kind == KIND_INDEX:
            handlers.on_index_delta(batch)
