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
from gnitz.catalog.registry import EntityRegistry
from gnitz.catalog.index_circuit import (
    get_index_key_type,
    make_fk_index_name,
    make_secondary_index_name,
)
from gnitz.catalog.metadata import (
    ensure_dir,
    make_system_tables,
    bootstrap_system_tables,
)
from gnitz.catalog.loader import CatalogBootstrapper
from gnitz.catalog.handlers import CatalogHandlers, CatalogReactiveStore


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
    handlers = CatalogHandlers(registry, sys_tables, base_dir, program_cache)
    bootstrapper = CatalogBootstrapper(registry, sys_tables, base_dir)

    os.write(1, " -> 5. recover_system_state (Counters)\n")
    bootstrapper.recover_system_state()

    engine = Engine(base_dir, sys_tables, registry, handlers, program_cache)

    os.write(1, " -> 6. wire_reactivity\n")
    engine._wire_reactivity()

    os.write(1, " -> 7. replay_catalog (Logical Recovery)\n")
    bootstrapper.replay_catalog(handlers)

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
    Converts a CircuitGraph's output_col_defs list of (name, type_code) tuples
    into a list of ColumnDefinition objects suitable for _write_column_records.
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
    Writes durable intent to System Z-Sets; Handlers react to perform side-effects.
    """

    _immutable_fields_ = ["base_dir", "sys", "registry", "handlers", "program_cache"]

    def __init__(self, base_dir, sys_tables, registry, handlers, program_cache):
        self.base_dir = base_dir
        self.sys = sys_tables
        self.registry = registry
        self.handlers = handlers
        self.program_cache = program_cache

    def _wire_reactivity(self):
        """Links System Z-Sets to the reactive handlers."""
        h = self.handlers
        self.sys.schemas = CatalogReactiveStore(self.sys.schemas, h, "schema")
        self.sys.tables = CatalogReactiveStore(self.sys.tables, h, "table")
        self.sys.views = CatalogReactiveStore(self.sys.views, h, "view")
        self.sys.indices = CatalogReactiveStore(self.sys.indices, h, "index")

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

        self._write_column_records(tid, sys.OWNER_KIND_TABLE, columns)
        self._write_table_record(tid, sid, table_name, directory, pk_col_idx, 0)

        self._advance_sequence(sys.SEQ_ID_TABLES, tid - 1, tid)
        family = self.registry.get(schema_name, table_name)
        self._create_fk_indices_for_family(family)
        return family

    def drop_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        family = self.registry.get(schema_name, table_name)
        tid = family.table_id
        self._check_for_dependencies(tid, table_name)

        self._retract_table_record(
            tid, family.schema_id, table_name, family.directory, family.pk_col_idx, 0
        )
        self._retract_column_records(tid, sys.OWNER_KIND_TABLE, family.schema.columns)

    def create_view(self, qualified_name, graph, sql_definition=""):
        """
        Creates a reactive view from a CircuitGraph produced by CircuitBuilder.build().

        graph: CircuitGraph — carries output_col_defs, dependencies, and the
               five circuit table payloads (nodes, edges, sources, params,
               group_cols).
        """
        schema_name, view_name = parse_qualified_name(qualified_name, "public")
        if self.registry.has(schema_name, view_name):
            raise LayoutError("View/Table already exists")

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

        # 1. Column records — must precede the view record so the reactive
        #    handler can reconstruct the TableFamily schema on ingestion.
        col_defs = _col_defs_from_graph(graph)
        self._write_column_records(vid, sys.OWNER_KIND_VIEW, col_defs)
        self.sys.columns.flush()

        # 2. View dependency records — must precede the view record so that
        #    _compute_view_depth can resolve upstream depths immediately.
        self._write_view_deps(vid, graph.dependencies)
        self.sys.view_deps.flush()

        # 3. Circuit graph records (five tables).
        self._write_circuit_graph(vid, graph)
        self.sys.circuit_nodes.flush()
        self.sys.circuit_edges.flush()
        self.sys.circuit_sources.flush()
        self.sys.circuit_params.flush()
        self.sys.circuit_group_cols.flush()

        # 4. View record — triggers the reactive handler which registers the
        #    TableFamily in the EntityRegistry.
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

        # Evict the compiled plan before retracting metadata so that any
        # concurrent get_program call that races with drop cannot receive a
        # stale plan pointing at freed resources.
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

    def _create_fk_indices_for_family(self, family):
        columns = family.schema.columns
        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if col.fk_table_id == 0 or col_idx == family.schema.pk_index:
                continue
            index_name = make_fk_index_name(
                family.schema_name, family.table_name, col.name
            )
            if self.registry.has_index_by_name(index_name):
                continue
            index_id = self.registry.allocate_index_id()
            self._write_index_record(
                index_id, family.table_id, sys.OWNER_KIND_TABLE, col_idx, index_name, 0, ""
            )
            self._advance_sequence(sys.SEQ_ID_INDICES, index_id - 1, index_id)

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
        for k in self.registry._by_name:
            family = self.registry._by_name[k]
            if family.table_id >= sys.FIRST_USER_TABLE_ID:
                family.close()
        self.sys.close()

    def _check_for_dependencies(self, tid, name):
        """Validates that no entities depend on the given ID before dropping it."""
        for k in self.registry._by_name:
            referencing = self.registry._by_name[k]
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
        cursor = self.sys.view_deps.create_cursor()
        res = newlist_hint(4)
        try:
            while cursor.is_valid():
                acc = cursor.get_accessor()
                if intmask(acc.get_int(sys.DepTab.COL_VIEW_ID)) == vid and cursor.weight() > 0:
                    res.append(
                        (
                            intmask(r_uint64(cursor.key())),
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
        s = self.sys.circuit_nodes.schema
        batch = ZSetBatch(s)
        for node_id, opcode in graph.nodes:
            sys.CircuitNodesTab.append(batch, s, vid, node_id, opcode)
        self.sys.circuit_nodes.ingest_batch(batch)
        batch.free()

        s = self.sys.circuit_edges.schema
        batch = ZSetBatch(s)
        for edge_id, src, dst, port in graph.edges:
            sys.CircuitEdgesTab.append(batch, s, vid, edge_id, src, dst, port)
        self.sys.circuit_edges.ingest_batch(batch)
        batch.free()

        s = self.sys.circuit_sources.schema
        batch = ZSetBatch(s)
        for node_id, table_id in graph.sources:
            sys.CircuitSourcesTab.append(batch, s, vid, node_id, table_id)
        self.sys.circuit_sources.ingest_batch(batch)
        batch.free()

        s = self.sys.circuit_params.schema
        batch = ZSetBatch(s)
        for node_id, slot, value in graph.params:
            sys.CircuitParamsTab.append(batch, s, vid, node_id, slot, value)
        self.sys.circuit_params.ingest_batch(batch)
        batch.free()

        s = self.sys.circuit_group_cols.schema
        batch = ZSetBatch(s)
        for node_id, col_idx in graph.group_cols:
            sys.CircuitGroupColsTab.append(batch, s, vid, node_id, col_idx)
        self.sys.circuit_group_cols.ingest_batch(batch)
        batch.free()

    def _retract_circuit_graph(self, vid):
        """
        Scans all five circuit tables for the given view_id and retracts every
        record found.  Uses the same range-scan convention as ProgramCache's
        loaders: [vid << 64, (vid+1) << 64).
        """
        start_key = r_uint128(r_uint64(vid)) << 64
        end_key = r_uint128(r_uint64(vid + 1)) << 64

        # Nodes
        s = self.sys.circuit_nodes.schema
        batch = ZSetBatch(s)
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
        self.sys.circuit_nodes.ingest_batch(batch)
        batch.free()

        # Edges
        s = self.sys.circuit_edges.schema
        batch = ZSetBatch(s)
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
        self.sys.circuit_edges.ingest_batch(batch)
        batch.free()

        # Sources
        s = self.sys.circuit_sources.schema
        batch = ZSetBatch(s)
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
        self.sys.circuit_sources.ingest_batch(batch)
        batch.free()

        # Params
        s = self.sys.circuit_params.schema
        batch = ZSetBatch(s)
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
        self.sys.circuit_params.ingest_batch(batch)
        batch.free()

        # Group cols
        s = self.sys.circuit_group_cols.schema
        batch = ZSetBatch(s)
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
        self.sys.circuit_group_cols.ingest_batch(batch)
        batch.free()

    # -- Private Write Helpers ------------------------------------------------

    def _write_schema_record(self, sid, name):
        s = self.sys.schemas.schema
        batch = ZSetBatch(s)
        sys.SchemaTab.append(batch, s, sid, name)
        self.sys.schemas.ingest_batch(batch)
        batch.free()

    def _retract_schema_record(self, sid, name):
        s = self.sys.schemas.schema
        batch = ZSetBatch(s)
        sys.SchemaTab.retract(batch, s, sid, name)
        self.sys.schemas.ingest_batch(batch)
        batch.free()

    def _write_table_record(self, tid, sid, name, directory, pk_idx, lsn):
        s = self.sys.tables.schema
        batch = ZSetBatch(s)
        sys.TableTab.append(batch, s, tid, sid, name, directory, pk_idx, lsn)
        self.sys.tables.ingest_batch(batch)
        batch.free()

    def _retract_table_record(self, tid, sid, name, directory, pk_idx, lsn):
        s = self.sys.tables.schema
        batch = ZSetBatch(s)
        sys.TableTab.retract(batch, s, tid, sid, name, directory, pk_idx, lsn)
        self.sys.tables.ingest_batch(batch)
        batch.free()

    def _write_view_record(self, vid, sid, name, sql_def, directory, lsn):
        s = self.sys.views.schema
        batch = ZSetBatch(s)
        sys.ViewTab.append(batch, s, vid, sid, name, sql_def, directory, lsn)
        self.sys.views.ingest_batch(batch)
        batch.free()

    def _retract_view_record(self, vid, sid, name, sql_def, directory, lsn):
        s = self.sys.views.schema
        batch = ZSetBatch(s)
        sys.ViewTab.retract(batch, s, vid, sid, name, sql_def, directory, lsn)
        self.sys.views.ingest_batch(batch)
        batch.free()

    def _write_view_deps(self, vid, dep_ids):
        s = self.sys.view_deps.schema
        batch = ZSetBatch(s)
        for dep_tid in dep_ids:
            dep_id = intmask(r_uint64(vid) ^ r_uint64(dep_tid))
            sys.DepTab.append(batch, s, dep_id, vid, 0, dep_tid)
        self.sys.view_deps.ingest_batch(batch)
        batch.free()

    def _retract_view_deps(self, vid, deps_info):
        s = self.sys.view_deps.schema
        batch = ZSetBatch(s)
        for d_id, dvid, dtid in deps_info:
            sys.DepTab.retract(batch, s, d_id, vid, dvid, dtid)
        self.sys.view_deps.ingest_batch(batch)
        batch.free()

    def _write_column_records(self, oid, kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            sys.ColTab.append(
                batch, s, oid, kind, i, col.name, col.field_type.code,
                int(col.is_nullable), col.fk_table_id, col.fk_col_idx,
            )
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _retract_column_records(self, oid, kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            sys.ColTab.retract(
                batch, s, oid, kind, i, col.name, col.field_type.code,
                int(col.is_nullable), col.fk_table_id, col.fk_col_idx,
            )
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _write_index_record(self, idx_id, oid, kind, s_idx, name, unique, c_dir):
        s = self.sys.indices.schema
        batch = ZSetBatch(s)
        sys.IdxTab.append(batch, s, idx_id, oid, kind, s_idx, name, unique, c_dir)
        self.sys.indices.ingest_batch(batch)
        batch.free()

    def _retract_index_record(self, idx_id, oid, kind, s_idx, name, unique, c_dir):
        s = self.sys.indices.schema
        batch = ZSetBatch(s)
        sys.IdxTab.retract(batch, s, idx_id, oid, kind, s_idx, name, unique, c_dir)
        self.sys.indices.ingest_batch(batch)
        batch.free()

    def _advance_sequence(self, seq_id, old_val, new_val):
        s = self.sys.sequences.schema
        batch = ZSetBatch(s)
        sys.SeqTab.retract(batch, s, seq_id, old_val)
        sys.SeqTab.append(batch, s, seq_id, new_val)
        self.sys.sequences.ingest_batch(batch)
        batch.free()
