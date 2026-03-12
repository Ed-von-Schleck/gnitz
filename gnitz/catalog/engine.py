# gnitz/catalog/engine.py

import os
from rpython.rlib import rposix, rposix_stat
from rpython.rlib.rarithmetic import r_uint64, intmask, r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.types import TableSchema, ColumnDefinition
from gnitz.core.errors import LayoutError
from gnitz.core.batch import ZSetBatch
from gnitz.catalog.program_cache import ProgramCache

from gnitz.catalog.identifiers import validate_user_identifier, parse_qualified_name
from gnitz.catalog import system_tables as sys
from gnitz.catalog.registry import EntityRegistry, ingest_to_family
from gnitz.catalog.index_circuit import (
    get_index_key_type,
    make_secondary_index_name,
)
from gnitz.catalog.metadata import (
    ensure_dir,
    make_system_tables,
    bootstrap_system_tables,
)
from gnitz.catalog.loader import CatalogBootstrapper
from gnitz.catalog.hooks import wire_catalog_hooks


def open_engine(base_dir, memtable_arena_size=1 * 1024 * 1024):
    """
    Public factory to open a GnitzDB instance.
    """
    os.write(1, " -> 1. ensure_dir(base_dir)\n")
    ensure_dir(base_dir)

    sys_dir = base_dir + "/" + sys.SYS_CATALOG_DIRNAME
    os.write(1, " -> 2. ensure_dir(sys_dir)\n")
    ensure_dir(sys_dir)

    os.write(1, " -> 3. make_system_tables\n")
    sys_tables = make_system_tables(base_dir)

    tmp_cursor = sys_tables.tables.create_cursor()
    is_new = not tmp_cursor.is_valid()
    tmp_cursor.close()

    if is_new:
        os.write(1, " -> 4. bootstrap_system_tables (Fresh Instance)\n")
        bootstrap_system_tables(sys_tables, base_dir)

    registry = EntityRegistry()
    program_cache = ProgramCache(registry)
    bootstrapper = CatalogBootstrapper(registry, sys_tables, base_dir)

    os.write(1, " -> 5. recover_system_state (Counters)\n")
    bootstrapper.recover_system_state()

    table_hook = wire_catalog_hooks(registry, sys_tables, base_dir, program_cache)

    engine = Engine(base_dir, sys_tables, registry, program_cache)

    os.write(1, " -> 6. replay_catalog (Logical Recovery)\n")
    bootstrapper.replay_catalog()

    # Enable cascading effects now that replay is complete
    table_hook.cascade_enabled = True

    return engine


def _validate_fk_column(
    col, col_idx, registry, self_table_id=0, self_pk_index=0, self_pk_type=None
):
    """Ensures a column's FK metadata is logically sound before ingestion."""
    if col.fk_table_id == 0:
        return

    if col.fk_table_id == self_table_id:
        target_pk_index = self_pk_index
        target_pk_type = self_pk_type
    else:
        if not registry.has_id(col.fk_table_id):
            raise LayoutError("FK references unknown table_id %d" % col.fk_table_id)
        target_family = registry.get_by_id(col.fk_table_id)
        target_pk_index = target_family.schema.pk_index
        target_pk_type = target_family.schema.get_pk_column().field_type

    if col.fk_col_idx != target_pk_index:
        raise LayoutError("FK must reference target PK")

    promoted_type = get_index_key_type(col.field_type)
    if promoted_type.code != target_pk_type.code:
        raise LayoutError(
            "FK type mismatch: promoted code %d vs target %d"
            % (promoted_type.code, target_pk_type.code)
        )


def _col_defs_from_graph(graph):
    """
    Converts a CircuitGraph's output_col_defs list into a list of ColumnDefinition objects.
    """
    result = newlist_hint(len(graph.output_col_defs))
    for name, type_code in graph.output_col_defs:
        result.append(
            ColumnDefinition(
                sys.type_code_to_field_type(type_code),
                is_nullable=False,
                name=name,
            )
        )
    return result


class Engine(object):
    """
    The Supervisor for catalog mutations.
    Writes durable intent to System Z-Sets; side-effects fire automatically
    via post-ingestion hooks on TableFamily.
    """

    _immutable_fields_ = [
        "base_dir", "sys", "registry", "program_cache",
        "_schemas_family", "_tables_family", "_views_family", "_indices_family",
    ]

    def __init__(self, base_dir, sys_tables, registry, program_cache):
        self.base_dir = base_dir
        self.sys = sys_tables
        self.registry = registry
        self.program_cache = program_cache
        # Cache family references for system tables with hooks
        self._schemas_family = registry.get("_system", sys.SchemaTab.NAME)
        self._tables_family = registry.get("_system", sys.TableTab.NAME)
        self._views_family = registry.get("_system", sys.ViewTab.NAME)
        self._indices_family = registry.get("_system", sys.IdxTab.NAME)

    # -- Public Intent API ----------------------------------------------------

    def create_schema(self, name):
        validate_user_identifier(name)
        if self.registry.has_schema(name):
            raise LayoutError("Schema already exists: %s" % name)
        sid = self.registry.allocate_schema_id()
        self._write_schema_record(sid, name)
        self._advance_sequence(sys.SEQ_ID_SCHEMAS, sid - 1, sid)

    def drop_schema(self, name):
        validate_user_identifier(name)
        if not self.registry.has_schema(name):
            raise LayoutError("Schema does not exist")
        sid = self.registry.get_schema_id(name)
        self._retract_schema_record(sid, name)

    def create_table(self, qualified_name, columns, pk_col_idx):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        validate_user_identifier(schema_name)
        validate_user_identifier(table_name)

        if not self.registry.has_schema(schema_name):
            raise LayoutError("Schema does not exist")
        if self.registry.has(schema_name, table_name):
            raise LayoutError("Table already exists")

        if len(columns) > 64:
            raise LayoutError("Maximum 64 columns supported")
        if pk_col_idx < 0 or pk_col_idx >= len(columns):
            raise LayoutError("Primary Key index out of bounds")

        tid = self.registry.allocate_table_id()
        sid = self.registry.get_schema_id(schema_name)
        self_pk_type = columns[pk_col_idx].field_type

        for col_idx in range(len(columns)):
            _validate_fk_column(
                columns[col_idx], col_idx, self.registry, tid, pk_col_idx, self_pk_type
            )

        directory = (
            self.base_dir + "/" + schema_name + "/" + table_name + "_" + str(tid)
        )

        # Columns are written first so that the table handler can read them.
        self._write_column_records(tid, sys.OWNER_KIND_TABLE, columns)
        self._write_table_record(tid, sid, table_name, directory, pk_col_idx, 0)

        self._advance_sequence(sys.SEQ_ID_TABLES, tid - 1, tid)
        family = self.registry.get(schema_name, table_name)
        return family

    def drop_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        validate_user_identifier(schema_name)
        validate_user_identifier(table_name)
        if not self.registry.has(schema_name, table_name):
            raise LayoutError("Table does not exist: %s.%s" % (schema_name, table_name))
        family = self.registry.get(schema_name, table_name)
        tid = family.table_id
        self._check_for_dependencies(tid, table_name)

        self._retract_table_record(
            tid, family.schema_id, table_name, family.directory, family.pk_col_idx, 0
        )
        self._retract_column_records(tid, sys.OWNER_KIND_TABLE, family.schema.columns)

    def create_view(self, qualified_name, graph, sql_definition=""):
        schema_name, view_name = parse_qualified_name(qualified_name, "public")
        if self.registry.has(schema_name, view_name):
            raise LayoutError("View/Table already exists")

        # Validate graph structure (sink presence, DAG properties) before persistence.
        self.program_cache.validate_graph_structure(graph)

        vid = self.registry.allocate_table_id()
        sid = self.registry.get_schema_id(schema_name)
        directory = (
            self.base_dir
            + "/"
            + schema_name
            + "/view_"
            + view_name
            + "_"
            + str(vid)
        )

        # 1. Column records (Durable + Visible to view handler)
        col_defs = _col_defs_from_graph(graph)
        self._write_column_records(vid, sys.OWNER_KIND_VIEW, col_defs)
        self.sys.columns.flush()

        # 2. View dependency records
        self._write_view_deps(vid, graph.dependencies)
        self.sys.view_deps.flush()

        # 3. Circuit graph records
        self._write_circuit_graph(vid, graph)
        self.sys.circuit_nodes.flush()
        self.sys.circuit_edges.flush()
        self.sys.circuit_sources.flush()
        self.sys.circuit_params.flush()
        self.sys.circuit_group_cols.flush()

        # 4. View record (Invokes reactive handler)
        self._write_view_record(vid, sid, view_name, sql_definition, directory, 0)
        self.sys.views.flush()

        self._advance_sequence(sys.SEQ_ID_TABLES, vid - 1, vid)
        return self.registry.get(schema_name, view_name)

    def drop_view(self, qualified_name):
        schema_name, view_name = parse_qualified_name(qualified_name, "public")
        family = self.registry.get(schema_name, view_name)
        vid = family.table_id
        self._check_for_dependencies(vid, view_name)

        deps = self._read_view_deps(vid)

        # Evict plan first
        self.program_cache.invalidate(vid)

        self._retract_view_record(
            vid, family.schema_id, view_name, "", family.directory, 0
        )
        self._retract_circuit_graph(vid)
        self._retract_view_deps(vid, deps)
        self._retract_column_records(vid, sys.OWNER_KIND_VIEW, family.schema.columns)

    def create_index(self, qualified_owner_name, col_name, is_unique=False):
        schema_name, table_name = parse_qualified_name(qualified_owner_name, "public")
        family = self.get_table(qualified_owner_name)

        col_idx = -1
        for i in range(len(family.schema.columns)):
            if family.schema.columns[i].name == col_name:
                col_idx = i
                break
        if col_idx == -1:
            raise LayoutError("Column not found in owner")

        index_name = make_secondary_index_name(schema_name, table_name, col_name)
        index_id = self.registry.allocate_index_id()
        self._write_index_record(
            index_id, family.table_id, sys.OWNER_KIND_TABLE, col_idx, index_name, int(is_unique), ""
        )
        self._advance_sequence(sys.SEQ_ID_INDICES, index_id - 1, index_id)
        return self.registry.get_index_by_name(index_name)

    def drop_index(self, index_name):
        circuit = self.registry.get_index_by_name(index_name)
        self._retract_index_record(
            circuit.index_id,
            circuit.owner_id,
            sys.OWNER_KIND_TABLE,
            circuit.source_col_idx,
            circuit.name,
            int(circuit.is_unique),
            circuit.cache_dir,
        )

    def get_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        return self.registry.get(schema_name, table_name)

    def close(self):
        for family in self.registry.iter_families():
            if family.table_id >= sys.FIRST_USER_TABLE_ID:
                family.close()
        self.sys.close()

    def _check_for_dependencies(self, tid, name):
        """Validates that no entities depend on the given ID before dropping it."""
        for referencing in self.registry.iter_families():
            if referencing.table_id == tid:
                continue
            for col in referencing.schema.columns:
                if col.fk_table_id == tid:
                    raise LayoutError("FK dependency: table '%s'" % name)

        cursor = self.sys.view_deps.create_cursor()
        try:
            while cursor.is_valid():
                if cursor.weight() > 0:
                    acc = cursor.get_accessor()
                    if intmask(acc.get_int(sys.DepTab.COL_DEP_TABLE_ID)) == tid or \
                       intmask(acc.get_int(sys.DepTab.COL_DEP_VIEW_ID)) == tid:
                        raise LayoutError("View dependency: entity '%s'" % name)
                cursor.advance()
        finally:
            cursor.close()

    def _read_view_deps(self, vid):
        from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_int64
        start_key = r_uint128(r_uint64(vid)) << 64
        end_key = r_uint128(r_uint64(vid + 1)) << 64
        cursor = self.sys.view_deps.create_cursor()
        res = newlist_hint(4)
        try:
            cursor.seek(start_key)
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > r_int64(0):
                    acc = cursor.get_accessor()
                    res.append(
                        (
                            intmask(acc.get_int(sys.DepTab.COL_DEP_VIEW_ID)),
                            intmask(acc.get_int(sys.DepTab.COL_DEP_TABLE_ID)),
                        )
                    )
                cursor.advance()
        finally:
            cursor.close()
        return res

    # -- Circuit Graph Write Helpers ------------------------------------------

    def _write_circuit_graph(self, vid, graph):
        """Ingests all five circuit table payloads from a CircuitGraph."""
        s, batch = self._begin_write(sys.CircuitNodesTab)
        for node_id, opcode in graph.nodes:
            sys.CircuitNodesTab.append(batch, s, vid, node_id, opcode)
        self._finish_write_sys(self.sys.circuit_nodes, batch)

        s, batch = self._begin_write(sys.CircuitEdgesTab)
        for edge_id, src, dst, port in graph.edges:
            sys.CircuitEdgesTab.append(batch, s, vid, edge_id, src, dst, port)
        self._finish_write_sys(self.sys.circuit_edges, batch)

        s, batch = self._begin_write(sys.CircuitSourcesTab)
        for node_id, table_id in graph.sources:
            sys.CircuitSourcesTab.append(batch, s, vid, node_id, table_id)
        self._finish_write_sys(self.sys.circuit_sources, batch)

        s, batch = self._begin_write(sys.CircuitParamsTab)
        for node_id, slot, value in graph.params:
            sys.CircuitParamsTab.append(batch, s, vid, node_id, slot, value)
        self._finish_write_sys(self.sys.circuit_params, batch)

        s, batch = self._begin_write(sys.CircuitGroupColsTab)
        for node_id, col_idx in graph.group_cols:
            sys.CircuitGroupColsTab.append(batch, s, vid, node_id, col_idx)
        self._finish_write_sys(self.sys.circuit_group_cols, batch)

    def _retract_circuit_graph(self, vid):
        start_key = r_uint128(r_uint64(vid)) << 64
        end_key = r_uint128(r_uint64(vid + 1)) << 64

        # Nodes
        s, batch = self._begin_write(sys.CircuitNodesTab)
        cursor = self.sys.circuit_nodes.create_cursor()
        try:
            cursor.seek(start_key)
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > 0:
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    node_id = intmask(r_uint64(pk))
                    opcode = intmask(acc.get_int(sys.CircuitNodesTab.COL_OPCODE))
                    sys.CircuitNodesTab.retract(batch, s, vid, node_id, opcode)
                cursor.advance()
        finally:
            cursor.close()
        self._finish_write_sys(self.sys.circuit_nodes, batch)

        # Edges
        s, batch = self._begin_write(sys.CircuitEdgesTab)
        cursor = self.sys.circuit_edges.create_cursor()
        try:
            cursor.seek(start_key)
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > 0:
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    edge_id = intmask(r_uint64(pk))
                    src = intmask(acc.get_int(sys.CircuitEdgesTab.COL_SRC_NODE))
                    dst = intmask(acc.get_int(sys.CircuitEdgesTab.COL_DST_NODE))
                    port = intmask(acc.get_int(sys.CircuitEdgesTab.COL_DST_PORT))
                    sys.CircuitEdgesTab.retract(batch, s, vid, edge_id, src, dst, port)
                cursor.advance()
        finally:
            cursor.close()
        self._finish_write_sys(self.sys.circuit_edges, batch)

        # Sources
        s, batch = self._begin_write(sys.CircuitSourcesTab)
        cursor = self.sys.circuit_sources.create_cursor()
        try:
            cursor.seek(start_key)
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > 0:
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    node_id = intmask(r_uint64(pk))
                    table_id = intmask(acc.get_int(sys.CircuitSourcesTab.COL_TABLE_ID))
                    sys.CircuitSourcesTab.retract(batch, s, vid, node_id, table_id)
                cursor.advance()
        finally:
            cursor.close()
        self._finish_write_sys(self.sys.circuit_sources, batch)

        # Params
        s, batch = self._begin_write(sys.CircuitParamsTab)
        cursor = self.sys.circuit_params.create_cursor()
        try:
            cursor.seek(start_key)
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > 0:
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    lo64 = r_uint64(pk)
                    node_id = intmask(lo64 >> 8)
                    slot = intmask(lo64 & r_uint64(0xFF))
                    value = intmask(acc.get_int(sys.CircuitParamsTab.COL_VALUE))
                    sys.CircuitParamsTab.retract(batch, s, vid, node_id, slot, value)
                cursor.advance()
        finally:
            cursor.close()
        self._finish_write_sys(self.sys.circuit_params, batch)

        # Group cols
        s, batch = self._begin_write(sys.CircuitGroupColsTab)
        cursor = self.sys.circuit_group_cols.create_cursor()
        try:
            cursor.seek(start_key)
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > 0:
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    lo64 = r_uint64(pk)
                    node_id = intmask(lo64 >> 16)
                    col_idx = intmask(lo64 & r_uint64(0xFFFF))
                    sys.CircuitGroupColsTab.retract(batch, s, vid, node_id, col_idx)
                cursor.advance()
        finally:
            cursor.close()
        self._finish_write_sys(self.sys.circuit_group_cols, batch)

    # -- Private Write Helpers ------------------------------------------------

    def _begin_write(self, tab_class):
        s = tab_class.schema()
        return s, ZSetBatch(s)

    def _finish_write(self, family, batch):
        ingest_to_family(family, batch)
        batch.free()

    def _finish_write_sys(self, sys_table, batch):
        sys_table.ingest_batch(batch)
        batch.free()

    def _write_schema_record(self, sid, name):
        s, batch = self._begin_write(sys.SchemaTab)
        sys.SchemaTab.append(batch, s, sid, name)
        self._finish_write(self._schemas_family, batch)

    def _retract_schema_record(self, sid, name):
        s, batch = self._begin_write(sys.SchemaTab)
        sys.SchemaTab.retract(batch, s, sid, name)
        self._finish_write(self._schemas_family, batch)

    def _write_table_record(self, tid, sid, name, directory, pk_idx, lsn):
        s, batch = self._begin_write(sys.TableTab)
        sys.TableTab.append(batch, s, tid, sid, name, directory, pk_idx, lsn)
        self._finish_write(self._tables_family, batch)

    def _retract_table_record(self, tid, sid, name, directory, pk_idx, lsn):
        s, batch = self._begin_write(sys.TableTab)
        sys.TableTab.retract(batch, s, tid, sid, name, directory, pk_idx, lsn)
        self._finish_write(self._tables_family, batch)

    def _write_view_record(self, vid, sid, name, sql_def, directory, lsn):
        s, batch = self._begin_write(sys.ViewTab)
        sys.ViewTab.append(batch, s, vid, sid, name, sql_def, directory, lsn)
        self._finish_write(self._views_family, batch)

    def _retract_view_record(self, vid, sid, name, sql_def, directory, lsn):
        s, batch = self._begin_write(sys.ViewTab)
        sys.ViewTab.retract(batch, s, vid, sid, name, sql_def, directory, lsn)
        self._finish_write(self._views_family, batch)

    def _write_view_deps(self, vid, dep_ids):
        s, batch = self._begin_write(sys.DepTab)
        for dep_tid in dep_ids:
            sys.DepTab.append(batch, s, vid, 0, dep_tid)
        self._finish_write_sys(self.sys.view_deps, batch)

    def _retract_view_deps(self, vid, deps_info):
        s, batch = self._begin_write(sys.DepTab)
        for dvid, dtid in deps_info:
            sys.DepTab.retract(batch, s, vid, dvid, dtid)
        self._finish_write_sys(self.sys.view_deps, batch)

    def _write_column_records(self, oid, kind, columns):
        s, batch = self._begin_write(sys.ColTab)
        for i in range(len(columns)):
            col = columns[i]
            sys.ColTab.append(
                batch, s, oid, kind, i, col.name, col.field_type.code,
                int(col.is_nullable), col.fk_table_id, col.fk_col_idx,
            )
        self._finish_write_sys(self.sys.columns, batch)

    def _retract_column_records(self, oid, kind, columns):
        s, batch = self._begin_write(sys.ColTab)
        for i in range(len(columns)):
            col = columns[i]
            sys.ColTab.retract(
                batch, s, oid, kind, i, col.name, col.field_type.code,
                int(col.is_nullable), col.fk_table_id, col.fk_col_idx,
            )
        self._finish_write_sys(self.sys.columns, batch)

    def _write_index_record(self, idx_id, oid, kind, s_idx, name, unique, c_dir):
        s, batch = self._begin_write(sys.IdxTab)
        sys.IdxTab.append(batch, s, idx_id, oid, kind, s_idx, name, unique, c_dir)
        self._finish_write(self._indices_family, batch)

    def _retract_index_record(self, idx_id, oid, kind, s_idx, name, unique, c_dir):
        s, batch = self._begin_write(sys.IdxTab)
        sys.IdxTab.retract(batch, s, idx_id, oid, kind, s_idx, name, unique, c_dir)
        self._finish_write(self._indices_family, batch)

    def _advance_sequence(self, seq_id, old_val, new_val):
        s, batch = self._begin_write(sys.SeqTab)
        sys.SeqTab.retract(batch, s, seq_id, old_val)
        sys.SeqTab.append(batch, s, seq_id, new_val)
        self._finish_write_sys(self.sys.sequences, batch)
