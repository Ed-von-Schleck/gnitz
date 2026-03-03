# gnitz/catalog/loader.py

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.batch import ZSetBatch
from gnitz.catalog import system_tables as sys
from gnitz.catalog.metadata import read_column_defs
from gnitz.catalog.registry import TableFamily

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
        # 1. Recover sequence high-water marks.
        #
        # Collect all four values in a single cursor pass, then hand them to
        # the registry via the public API.  Sentinel -1 means "no record found"
        # (e.g. fresh database before bootstrap completes); recover_sequences
        # treats -1 + 1 = 0 as below any FIRST_USER_* floor and ignores it.
        schema_hwm = -1
        table_hwm = -1
        index_hwm = -1
        programs_hwm = -1

        cursor = self.sys_tables.sequences.create_cursor()
        try:
            while cursor.is_valid():
                if cursor.weight() > 0:
                    acc = cursor.get_accessor()
                    seq_id = intmask(r_uint64(cursor.key()))
                    val = intmask(acc.get_int(sys.SeqTab.COL_VALUE))
                    if seq_id == sys.SEQ_ID_SCHEMAS:
                        schema_hwm = val
                    elif seq_id == sys.SEQ_ID_TABLES:
                        table_hwm = val
                    elif seq_id == sys.SEQ_ID_INDICES:
                        index_hwm = val
                    elif seq_id == sys.SEQ_ID_PROGRAMS:
                        programs_hwm = val
                cursor.advance()
        finally:
            cursor.close()

        self.registry.recover_sequences(schema_hwm, table_hwm, index_hwm, programs_hwm)

        # 2. Register Foundation System Tables directly into the Registry so
        #    that handlers can locate them (e.g. ColTab) during replay.
        sys_dir = self.base_dir + "/" + sys.SYS_CATALOG_DIRNAME

        self.registry.register(TableFamily(
            "_system", sys.SchemaTab.NAME, sys.SchemaTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.SchemaTab.SUBDIR, 0,
            self.sys_tables.schemas,
        ))
        self.registry.register(TableFamily(
            "_system", sys.TableTab.NAME, sys.TableTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.TableTab.SUBDIR, 0,
            self.sys_tables.tables,
        ))
        self.registry.register(TableFamily(
            "_system", sys.ViewTab.NAME, sys.ViewTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.ViewTab.SUBDIR, 0,
            self.sys_tables.views,
        ))
        self.registry.register(TableFamily(
            "_system", sys.ColTab.NAME, sys.ColTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.ColTab.SUBDIR, 0,
            self.sys_tables.columns,
        ))
        self.registry.register(TableFamily(
            "_system", sys.IdxTab.NAME, sys.IdxTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.IdxTab.SUBDIR, 0,
            self.sys_tables.indices,
        ))
        self.registry.register(TableFamily(
            "_system", sys.DepTab.NAME, sys.DepTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.DepTab.SUBDIR, 0,
            self.sys_tables.view_deps,
        ))
        self.registry.register(TableFamily(
            "_system", sys.SeqTab.NAME, sys.SeqTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.SeqTab.SUBDIR, 0,
            self.sys_tables.sequences,
        ))
        self.registry.register(TableFamily(
            "_system", sys.FuncTab.NAME, sys.FuncTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.FuncTab.SUBDIR, 0,
            self.sys_tables.functions,
        ))
        self.registry.register(TableFamily(
            "_system", sys.SubTab.NAME, sys.SubTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.SubTab.SUBDIR, 0,
            self.sys_tables.subscriptions,
        ))
        self.registry.register(TableFamily(
            "_system", sys.CircuitNodesTab.NAME, sys.CircuitNodesTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.CircuitNodesTab.SUBDIR, 0,
            self.sys_tables.circuit_nodes,
        ))
        self.registry.register(TableFamily(
            "_system", sys.CircuitEdgesTab.NAME, sys.CircuitEdgesTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.CircuitEdgesTab.SUBDIR, 0,
            self.sys_tables.circuit_edges,
        ))
        self.registry.register(TableFamily(
            "_system", sys.CircuitSourcesTab.NAME, sys.CircuitSourcesTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.CircuitSourcesTab.SUBDIR, 0,
            self.sys_tables.circuit_sources,
        ))
        self.registry.register(TableFamily(
            "_system", sys.CircuitParamsTab.NAME, sys.CircuitParamsTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.CircuitParamsTab.SUBDIR, 0,
            self.sys_tables.circuit_params,
        ))
        self.registry.register(TableFamily(
            "_system", sys.CircuitGroupColsTab.NAME, sys.CircuitGroupColsTab.ID,
            sys.SYSTEM_SCHEMA_ID, sys_dir + "/" + sys.CircuitGroupColsTab.SUBDIR, 0,
            self.sys_tables.circuit_group_cols,
        ))

    def replay_catalog(self, handlers):
        # Strict ordering to respect relational dependencies (Schemas first).
        self._replay_store(self.sys_tables.schemas, handlers, KIND_SCHEMA)
        self._replay_store(self.sys_tables.tables, handlers, KIND_TABLE)
        self._replay_store(self.sys_tables.views, handlers, KIND_VIEW)
        self._replay_store(self.sys_tables.indices, handlers, KIND_INDEX)

    def _replay_store(self, store, handlers, kind):
        """Scans a system store and feeds active records into the reactive loop."""
        schema = store.get_schema()
        cursor = store.create_cursor()
        try:
            # Nest the batch lifecycle inside the cursor try so that a
            # ZSetBatch allocation failure cannot leak the already-open cursor.
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
        finally:
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
