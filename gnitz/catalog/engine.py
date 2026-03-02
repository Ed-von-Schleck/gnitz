# gnitz/catalog/engine.py

import os
from rpython.rlib import rposix, rposix_stat
from rpython.rlib.rarithmetic import r_uint64, intmask, r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.types import TableSchema
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

    # Robust Freshness Check: If the internal tables catalog is empty, 
    # we must perform the initial bootstrap injection.
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
        raise LayoutError("FK type mismatch: promoted code %d vs target %d" % 
                          (promoted_type.code, target_pk_type.code))


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
        self.sys.instructions = CatalogReactiveStore(self.sys.instructions, h, "instr")

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
            _validate_fk_column(columns[col_idx], col_idx, self.registry, 
                               tid, pk_col_idx, self_pk_type)

        directory = self.base_dir + "/" + schema_name + "/" + table_name + "_" + str(tid)

        # CRITICAL: Column metadata must be flushed to disk BEFORE the table record.
        # This ensures that when the reactive handler for the table record runs,
        # it can successfully read the column definitions from the storage layer.
        self._write_column_records(tid, sys.OWNER_KIND_TABLE, columns)
        self.sys.columns.flush()

        self._write_table_record(tid, sid, table_name, directory, pk_col_idx, 0)
        self.sys.tables.flush()

        self._advance_sequence(sys.SEQ_ID_TABLES, tid - 1, tid)
        family = self.registry.get(schema_name, table_name)
        self._create_fk_indices_for_family(family)
        return family

    def drop_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        family = self.registry.get(schema_name, table_name)
        tid = family.table_id
        self._check_for_dependencies(tid, table_name)

        self._retract_table_record(tid, family.schema_id, table_name, family.directory, family.pk_col_idx, 0)
        self.sys.tables.flush()
        self._retract_column_records(tid, sys.OWNER_KIND_TABLE, family.schema.columns)
        self.sys.columns.flush()

    def create_view(self, qualified_name, query_builder, sql_definition=""):
        schema_name, view_name = parse_qualified_name(qualified_name, "public")
        if self.registry.has(schema_name, view_name):
            raise LayoutError("View/Table already exists")

        vid = self.registry.allocate_table_id()
        sid = self.registry.get_schema_id(schema_name)
        out_schema = query_builder.registers[query_builder.current_reg_idx].vm_schema.table_schema
        directory = self.base_dir + "/" + schema_name + "/view_" + view_name + "_" + str(vid)

        # Metadata ordering as in create_table
        self._write_column_records(vid, sys.OWNER_KIND_VIEW, out_schema.columns)
        self.sys.columns.flush()

        deps = self._extract_builder_dependencies(query_builder)
        self._write_view_deps(vid, deps)
        self.sys.view_deps.flush()

        self._write_instruction_records(vid, query_builder.instructions)
        self.sys.instructions.flush()

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
        instrs = self._read_view_instructions(vid)

        self._retract_view_record(vid, family.schema_id, view_name, "", family.directory, 0)
        self.sys.views.flush()
        self._retract_instruction_records(vid, instrs)
        self.sys.instructions.flush()
        self._retract_view_deps(vid, deps)
        self.sys.view_deps.flush()
        self._retract_column_records(vid, sys.OWNER_KIND_VIEW, family.schema.columns)
        self.sys.columns.flush()

    def create_index(self, qualified_owner_name, col_name, is_unique=False):
        schema_name, table_name = parse_qualified_name(qualified_owner_name, "public")
        family = self.get_table(qualified_owner_name)

        col_idx = -1
        for i in range(len(family.schema.columns)):
            if family.schema.columns[i].name == col_name:
                col_idx = i
                break
        if col_idx == -1: raise LayoutError("Column not found in owner")

        index_name = make_secondary_index_name(schema_name, table_name, col_name)
        index_id = self.registry.allocate_index_id()
        self._write_index_record(index_id, family.table_id, sys.OWNER_KIND_TABLE, 
                                 col_idx, index_name, int(is_unique), "")
        self.sys.indices.flush()
        self._advance_sequence(sys.SEQ_ID_INDICES, index_id - 1, index_id)
        return self.registry.get_index_by_name(index_name)

    def _create_fk_indices_for_family(self, family):
        columns = family.schema.columns
        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if col.fk_table_id == 0 or col_idx == family.schema.pk_index:
                continue
            index_name = make_fk_index_name(family.schema_name, family.table_name, col.name)
            if self.registry.has_index_by_name(index_name): continue
            index_id = self.registry.allocate_index_id()
            self._write_index_record(index_id, family.table_id, sys.OWNER_KIND_TABLE, col_idx, index_name, 0, "")
            self.sys.indices.flush()
            self._advance_sequence(sys.SEQ_ID_INDICES, index_id - 1, index_id)

    def drop_index(self, index_name):
        circuit = self.registry.get_index_by_name(index_name)
        self._retract_index_record(circuit.index_id, circuit.owner_id, sys.OWNER_KIND_TABLE, 
                                   circuit.source_col_idx, circuit.name, int(circuit.is_unique), 
                                   circuit.cache_dir)
        self.sys.indices.flush()

    def get_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        return self.registry.get(schema_name, table_name)

    def close(self):
        for k in self.registry._by_name:
            family = self.registry._by_name[k]
            if family.table_id >= sys.FIRST_USER_TABLE_ID: family.close()
        self.sys.close()

    def _check_for_dependencies(self, tid, name):
        """Validates that no entities depend on the given ID before dropping it."""
        for k in self.registry._by_name:
            referencing = self.registry._by_name[k]
            if referencing.table_id == tid: continue
            for col in referencing.schema.columns:
                if col.fk_table_id == tid: raise LayoutError("FK dependency: table '%s'" % name)

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

    def _extract_builder_dependencies(self, builder):
        deps = newlist_hint(len(builder.registers))
        for reg in builder.registers:
            if reg is not None and reg.is_trace() and reg.table is not None:
                tid = reg.table.table_id
                if tid > 0 and tid not in deps: deps.append(tid)
        return deps

    def _read_view_deps(self, vid):
        cursor = self.sys.view_deps.create_cursor()
        res = newlist_hint(4)
        try:
            while cursor.is_valid():
                acc = cursor.get_accessor()
                if intmask(acc.get_int(sys.DepTab.COL_VIEW_ID)) == vid and cursor.weight() > 0:
                    res.append((intmask(r_uint64(cursor.key())), 
                                intmask(acc.get_int(sys.DepTab.COL_DEP_VIEW_ID)),
                                intmask(acc.get_int(sys.DepTab.COL_DEP_TABLE_ID))))
                cursor.advance()
        finally:
            cursor.close()
        return res

    def _read_view_instructions(self, vid):
        cursor = self.sys.instructions.create_cursor()
        res = newlist_hint(16)
        try:
            start_key = r_uint128(r_uint64(vid)) << 64
            cursor.seek(start_key)
            while cursor.is_valid():
                key = cursor.key()
                if intmask(r_uint64(key >> 64)) != vid: break
                if cursor.weight() > 0:
                    acc = cursor.get_accessor()
                    payload = newlist_hint(20)
                    for j in range(1, 14 + 1): payload.append(intmask(acc.get_int(j)))
                    payload.append(intmask(acc.get_int(15)))
                    payload.append(intmask(acc.get_int(16)))
                    payload.append(sys.read_string(acc, 17))
                    payload.append(intmask(acc.get_int(18)))
                    payload.append(intmask(acc.get_int(19)))
                    payload.append(intmask(acc.get_int(20)))
                    res.append((key, payload))
                cursor.advance()
        finally:
            cursor.close()
        return res

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

    def _extract_tid_from_instr(self, instr):
        """
        Derives the table_id context for a VM instruction.
        Essential for re-hydrating the VM Program Cache after a restart.
        """
        if instr.target_table: 
            return instr.target_table.table_id
        
        # Inspection of associated registers
        if instr.reg_trace and instr.reg_trace.table: 
            return instr.reg_trace.table.table_id
        if instr.reg_trace_in and instr.reg_trace_in.table: 
            return instr.reg_trace_in.table.table_id
        if instr.reg_trace_out and instr.reg_trace_out.table: 
            return instr.reg_trace_out.table.table_id
        return 0

    def _write_instruction_records(self, vid, instructions):
        s = self.sys.instructions.schema
        batch = ZSetBatch(s)
        v_hi = r_uint64(vid)
        for i in range(len(instructions)):
            instr = instructions[i]
            ipk = (r_uint128(v_hi) << 64) | r_uint128(r_uint64(i))
            
            # Deep inspection of instruction context for persistence
            tid = self._extract_tid_from_instr(instr)

            sys.InstrTab.append(batch, s, ipk, instr.opcode,
                instr.reg_in.reg_id if instr.reg_in else 0,
                instr.reg_out.reg_id if instr.reg_out else 0,
                instr.reg_in_a.reg_id if instr.reg_in_a else 0,
                instr.reg_in_b.reg_id if instr.reg_in_b else 0,
                instr.reg_trace.reg_id if instr.reg_trace else 0,
                instr.reg_trace_in.reg_id if instr.reg_trace_in else 0,
                instr.reg_trace_out.reg_id if instr.reg_trace_out else 0,
                instr.reg_delta.reg_id if instr.reg_delta else 0,
                instr.reg_history.reg_id if instr.reg_history else 0,
                instr.reg_a.reg_id if instr.reg_a else 0,
                instr.reg_b.reg_id if instr.reg_b else 0,
                instr.reg_key.reg_id if instr.reg_key else 0,
                tid, 0, 0, "", 
                instr.chunk_limit, instr.jump_target, instr.yield_reason)
        self.sys.instructions.ingest_batch(batch)
        batch.free()

    def _retract_instruction_records(self, vid, records):
        s = self.sys.instructions.schema
        batch = ZSetBatch(s)
        for ipk, p in records:
            sys.InstrTab.retract(batch, s, ipk, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19])
        self.sys.instructions.ingest_batch(batch)
        batch.free()

    def _write_column_records(self, oid, kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            sys.ColTab.append(batch, s, oid, kind, i, col.name, col.field_type.code, int(col.is_nullable), col.fk_table_id, col.fk_col_idx)
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _retract_column_records(self, oid, kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            sys.ColTab.retract(batch, s, oid, kind, i, col.name, col.field_type.code, int(col.is_nullable), col.fk_table_id, col.fk_col_idx)
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
