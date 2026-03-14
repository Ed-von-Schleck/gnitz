# gnitz/catalog/loader.py

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.batch import ArenaZSetBatch
from gnitz.catalog import system_tables as sys
from gnitz.catalog.metadata import read_column_defs
from gnitz.catalog.registry import TableFamily


class CatalogBootstrapper(object):
    """
    Coordinates the transition from raw disk files to a live in-memory Registry.

    Unifies the startup sequence by replaying the durable System Z-Sets
    through the post-ingestion hooks on TableFamily, ensuring symmetry
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
                    seq_id = intmask(cursor.key())
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
        #    that hooks can locate them (e.g. ColTab) during replay.
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

    def replay_catalog(self):
        """Replays system stores through post-ingestion hooks in dependency order."""
        self._replay_family("_system", sys.SchemaTab.NAME)
        self._replay_family("_system", sys.TableTab.NAME)
        self._replay_family("_system", sys.ViewTab.NAME)
        self._replay_family("_system", sys.IdxTab.NAME)

    def _replay_family(self, schema_name, table_name):
        """Scans a system store and feeds active records through family hooks."""
        family = self.registry.get(schema_name, table_name)
        if len(family.post_ingest_hooks) == 0:
            return
        schema = family.store.get_schema()
        cursor = family.store.create_cursor()
        try:
            batch = ArenaZSetBatch(schema)
            try:
                while cursor.is_valid():
                    if cursor.weight() > 0:
                        batch.append_from_accessor(
                            cursor.key(),
                            cursor.weight(),
                            cursor.get_accessor(),
                        )

                        if batch.length() >= 512:
                            self._fire_hooks(family, batch)
                            batch.free()
                            batch = ArenaZSetBatch(schema)

                    cursor.advance()

                if batch.length() > 0:
                    self._fire_hooks(family, batch)
            finally:
                batch.free()
        finally:
            cursor.close()

    def _fire_hooks(self, family, batch):
        """Fires all post-ingestion hooks on a family."""
        for h_idx in range(len(family.post_ingest_hooks)):
            family.post_ingest_hooks[h_idx].on_delta(batch)
