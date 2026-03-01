# gnitz/catalog/engine.py

import errno
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import r_uint64, r_int64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi
from rpython.rlib import rposix_stat

from gnitz.core.types import TableSchema, TYPE_U64, TYPE_STRING
from gnitz.core.errors import LayoutError
from gnitz.core.batch import ZSetBatch
from gnitz.storage import mmap_posix
from gnitz.catalog.program_cache import ProgramCache

from gnitz.catalog.identifiers import validate_user_identifier, parse_qualified_name
from gnitz.catalog.system_tables import (
    SYS_TABLE_SCHEMAS,
    SYS_TABLE_TABLES,
    SYS_TABLE_COLUMNS,
    SYS_TABLE_INDICES,
    SYS_TABLE_SEQUENCES,
    SYSTEM_SCHEMA_ID,
    FIRST_USER_TABLE_ID,
    OWNER_KIND_TABLE,
    SEQ_ID_SCHEMAS,
    SEQ_ID_TABLES,
    SEQ_ID_INDICES,
    SYS_CATALOG_DIRNAME,
)
from gnitz.catalog.registry import (
    TableFamily,
    EntityRegistry,
    ReactiveStore,
    _wire_fk_constraints_for_family,
)
from gnitz.catalog.system_records import (
    _read_string,
    _append_schema_record,
    _retract_schema_record,
    _append_table_record,
    _retract_table_record,
    _append_column_record,
    _retract_column_record,
    _append_index_record,
    _retract_index_record,
    _append_sequence_record,
    _retract_sequence_record,
)
from gnitz.catalog.index_circuit import (
    get_index_key_type,
    _make_index_circuit,
    _backfill_index,
    make_fk_index_name,
    make_secondary_index_name,
)
from gnitz.catalog.loader import (
    _make_system_tables,
    _bootstrap_system_tables,
    _rebuild_registry,
    _read_column_defs,
)


def open_engine(base_dir, memtable_arena_size=1 * 1024 * 1024):
    """
    Public factory to open a GnitzDB instance.
    Handles directory creation, bootstrapping, and metadata recovery.
    """
    _ensure_dir(base_dir)
    sys_dir = base_dir + "/" + SYS_CATALOG_DIRNAME
    is_new = not os_path_exists(sys_dir)

    registry = EntityRegistry()
    sys_tables = _make_system_tables(base_dir)

    if is_new:
        _bootstrap_system_tables(sys_tables, base_dir)

    _rebuild_registry(sys_tables, base_dir, registry)

    engine = Engine(base_dir, sys_tables, registry)
    
    # Wire reactivity AFTER metadata recovery to avoid re-triggering DDL 
    # side-effects for existing records during the loader phase.
    engine._wire_reactivity()
    
    return engine


def os_path_exists(path):
    """RPython helper for checking path existence via stat."""
    try:
        rposix_stat.stat(path)
        return True
    except OSError:
        return False


def _ensure_dir(path):
    """Ensures a directory exists, ignoring EEXIST."""
    try:
        rposix.mkdir(path, 0o755)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _remove_circuit_from_family(circuit, family):
    """
    Rebuilds the index_circuits list excluding the target.
    Must use newlist_hint to avoid mr-poisoning.
    """
    old = family.index_circuits
    new_list = newlist_hint(len(old))
    for c in old:
        if c.index_id != circuit.index_id:
            new_list.append(c)
    family.index_circuits = new_list


def _validate_fk_column(
    col, col_idx, registry, self_table_id=0, self_pk_index=0, self_pk_type=None
):
    """
    Validates a single ColumnDefinition's FK annotation.
    """
    if col.fk_table_id == 0:
        return

    # Resolve target: Self-referential or external
    if col.fk_table_id == self_table_id:
        target_pk_index = self_pk_index
        target_pk_type = self_pk_type
        target_name = "<self>"
    else:
        if not registry.has_id(col.fk_table_id):
            raise LayoutError(
                "FK on column '%s' (idx %d) references unknown table_id %d"
                % (col.name, col_idx, col.fk_table_id)
            )
        target_family = registry.get_by_id(col.fk_table_id)
        target_pk_index = target_family.schema.pk_index
        target_pk_type = target_family.schema.get_pk_column().field_type
        target_name = target_family.table_name

    if col.fk_col_idx != target_pk_index:
        raise LayoutError(
            "FK on column '%s' must reference the PK column of target table '%s'"
            % (col.name, target_name)
        )

    promoted_type = get_index_key_type(col.field_type)
    if promoted_type.code != target_pk_type.code:
        raise LayoutError(
            "FK column '%s' promoted type (code %d) is incompatible with "
            "target PK type (code %d)"
            % (col.name, promoted_type.code, target_pk_type.code)
        )
        
        
class SchemaReactiveStore(ReactiveStore):
    def __init__(self, inner, engine):
        ReactiveStore.__init__(self, inner)
        self.engine = engine
    def on_ingest(self, batch):
        self.engine._on_schema_delta(batch)

class TableReactiveStore(ReactiveStore):
    def __init__(self, inner, engine):
        ReactiveStore.__init__(self, inner)
        self.engine = engine
    def on_ingest(self, batch):
        self.engine._on_table_delta(batch)

class InstructionReactiveStore(ReactiveStore):
    def __init__(self, inner, engine):
        ReactiveStore.__init__(self, inner)
        self.engine = engine
    def on_ingest(self, batch):
        # Reactively purge the plan cache when instructions change
        self.engine.program_cache.invalidate_all()


class Engine(object):
    """
    The reactive DDL supervisor for GnitzDB.
    """
    _immutable_fields_ = ["base_dir", "sys", "registry", "program_cache"]

    def __init__(self, base_dir, sys_tables, registry):
        self.base_dir = base_dir
        self.sys = sys_tables
        self.registry = registry
        self.program_cache = ProgramCache(registry)

    def _wire_reactivity(self):
        self.sys.schemas = SchemaReactiveStore(self.sys.schemas, self)
        self.sys.tables = TableReactiveStore(self.sys.tables, self)
        self.sys.instructions = InstructionReactiveStore(self.sys.instructions, self)

    # ── Reactive Callbacks ───────────────────────────────────────────────────

    def _on_schema_delta(self, batch):
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            batch.bind_raw_accessor(i, acc)
            
            sid = intmask(r_uint64(batch.get_pk(i)))
            name = _read_string(acc, 0)
            
            if weight > 0:
                if self.registry.has_schema(name):
                    continue
                
                path = self.base_dir + "/" + name
                _ensure_dir(path)
                mmap_posix.fsync_dir(self.base_dir)
                self.registry.register_schema(sid, name)
            else:
                if self.registry.has_schema(name):
                    if not self.registry.schema_is_empty(name):
                        raise LayoutError("Cannot drop non-empty schema: %s" % name)
                    self.registry.unregister_schema(name, sid)

    def _on_table_delta(self, batch):
        acc = batch.get_accessor(0)
        from gnitz.storage.table import PersistentTable

        for i in range(batch.length()):
            weight = batch.get_weight(i)
            tid = intmask(r_uint64(batch.get_pk(i)))
            batch.bind_raw_accessor(i, acc)
            
            sid = intmask(acc.get_int(0))
            name = _read_string(acc, 1)
            directory = _read_string(acc, 2)
            pk_col_idx = intmask(acc.get_int(3))

            if weight > 0:
                if self.registry.has_id(tid):
                    continue

                col_defs = _read_column_defs(self.sys.columns, tid)
                if len(col_defs) == 0:
                    raise LayoutError(
                        "Cannot create table '%s': column definitions missing" % name
                    )

                tbl_schema = TableSchema(col_defs, pk_col_idx)
                # Physical creation of directory and metadata happens in PersistentTable
                pt = PersistentTable(directory, name, tbl_schema, table_id=tid)
                
                # fsync parent schema directory
                schema_name = self.registry.get_schema_name(sid)
                mmap_posix.fsync_dir(self.base_dir + "/" + schema_name)

                family = TableFamily(
                    schema_name, name, tid, sid, directory, pk_col_idx, pt
                )
                self.registry.register(family)
                _wire_fk_constraints_for_family(family, self.registry)
            else:
                if self.registry.has_id(tid):
                    family = self.registry.get_by_id(tid)
                    
                    # Ref integrity check: Is anyone pointing at us?
                    for k in self.registry._by_name:
                        referencing = self.registry._by_name[k]
                        if referencing.table_id == tid: continue
                        for col in referencing.schema.columns:
                            if col.fk_table_id == tid:
                                raise LayoutError("Referential integrity: FK points to '%s'" % name)

                    family.close()
                    self.registry.unregister(family.schema_name, family.table_name)

    def _on_index_delta(self, batch):
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            idx_id = intmask(r_uint64(batch.get_pk(i)))
            batch.bind_raw_accessor(i, acc)
            
            owner_id = intmask(acc.get_int(0))
            source_col_idx = intmask(acc.get_int(2))
            name = _read_string(acc, 3)
            is_unique = acc.get_int(4) != 0
            cache_dir = _read_string(acc, 5)

            if weight > 0:
                if self.registry.has_index_by_name(name):
                    continue
                
                family = self.registry.get_by_id(owner_id)
                col = family.schema.columns[source_col_idx]
                source_pk_type = family.schema.get_pk_column().field_type
                idx_dir = family.directory + "/idx_" + str(idx_id)
                
                circuit = _make_index_circuit(
                    idx_id, owner_id, source_col_idx, col.field_type,
                    source_pk_type, idx_dir, name, is_unique, cache_dir
                )
                
                # Critical: Backfill can raise LayoutError on unique violation.
                # If it fails, the reactive transaction aborts.
                _backfill_index(circuit, family)
                
                family.index_circuits.append(circuit)
                self.registry.register_index(idx_id, name, circuit)
            else:
                if self.registry.has_index_by_name(name):
                    circuit = self.registry.get_index_by_name(name)
                    if "__fk_" in name:
                        raise LayoutError("Cannot drop internal FK index: %s" % name)
                    
                    family = self.registry.get_by_id(circuit.owner_id)
                    _remove_circuit_from_family(circuit, family)
                    self.registry.unregister_index(name, idx_id)
                    circuit.close()

    # ── Public Intent API ────────────────────────────────────────────────────

    def create_schema(self, name):
        validate_user_identifier(name)
        if self.registry.has_schema(name):
            raise LayoutError("Schema already exists: %s" % name)

        sid = self.registry.allocate_schema_id()
        self._write_schema_record(sid, name)
        self._advance_sequence(SEQ_ID_SCHEMAS, sid - 1, sid)

    def drop_schema(self, name):
        validate_user_identifier(name)
        if not self.registry.has_schema(name):
            raise LayoutError("Schema does not exist: %s" % name)
        if name == "_system":
            raise LayoutError("Cannot drop system schema")

        sid = self.registry.get_schema_id(name)
        self._retract_schema_record(sid, name)

    def create_table(self, qualified_name, columns, pk_col_idx):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        validate_user_identifier(schema_name)
        validate_user_identifier(table_name)

        if not self.registry.has_schema(schema_name):
            raise LayoutError("Schema does not exist: %s" % schema_name)
        if self.registry.has(schema_name, table_name):
            raise LayoutError("Table already exists: %s" % qualified_name)

        tid = self.registry.allocate_table_id()
        sid = self.registry.get_schema_id(schema_name)
        self_pk_type = columns[pk_col_idx].field_type
        
        for col_idx in range(len(columns)):
            _validate_fk_column(
                columns[col_idx], col_idx, self.registry,
                self_table_id=tid, self_pk_index=pk_col_idx, self_pk_type=self_pk_type,
            )

        directory = self.base_dir + "/" + schema_name + "/" + table_name + "_" + str(tid)

        # 1. Write columns first (required for reactive schema reconstruction)
        self._write_column_records(tid, OWNER_KIND_TABLE, columns)
        self.sys.columns.flush()
        
        # 2. Write table record (triggers reactive _on_table_delta)
        self._write_table_record(tid, sid, table_name, directory, pk_col_idx, 0)
        self.sys.tables.flush()

        self._advance_sequence(SEQ_ID_TABLES, tid - 1, tid)
        
        family = self.registry.get(schema_name, table_name)
        self._create_fk_indices_for_family(family)
        return family

    def drop_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        if not self.registry.has(schema_name, table_name):
            raise LayoutError("Table does not exist: %s" % qualified_name)

        family = self.registry.get(schema_name, table_name)
        tid = family.table_id

        # Retract table record (triggers reactive teardown)
        self._retract_table_record(
            tid, family.schema_id, table_name, family.directory, family.pk_col_idx, 0
        )
        self.sys.tables.flush()

        # Retract columns
        self._retract_column_records(tid, OWNER_KIND_TABLE, family.schema.columns)
        self.sys.columns.flush()

    def create_index(self, qualified_owner_name, col_name, is_unique=False):
        schema_name, table_name = parse_qualified_name(qualified_owner_name, "public")
        family = self.get_table(qualified_owner_name)
        
        col_idx = -1
        for i in range(len(family.schema.columns)):
            if family.schema.columns[i].name == col_name:
                col_idx = i; break
        if col_idx == -1: raise LayoutError("Column not found")

        index_name = make_secondary_index_name(schema_name, table_name, col_name)
        if self.registry.has_index_by_name(index_name):
            raise LayoutError("Index already exists")

        index_id = self.registry.allocate_index_id()
        self._write_index_record(
            index_id, family.table_id, OWNER_KIND_TABLE, col_idx, index_name, int(is_unique), ""
        )
        self.sys.indices.flush()
        self._advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id)
        
        return self.registry.get_index_by_name(index_name)

    def _create_fk_indices_for_family(self, family):
        schema_name = family.schema_name
        table_name = family.table_name
        columns = family.schema.columns

        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if col.fk_table_id == 0 or col_idx == family.schema.pk_index:
                continue

            index_name = make_fk_index_name(schema_name, table_name, col.name)
            already_exists = False
            for existing in family.index_circuits:
                if existing.source_col_idx == col_idx:
                    already_exists = True; break
            if already_exists: continue

            index_id = self.registry.allocate_index_id()
            self._write_index_record(
                index_id, family.table_id, OWNER_KIND_TABLE, col_idx, index_name, 0, ""
            )
            self.sys.indices.flush()
            self._advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id)

    def drop_index(self, index_name):
        circuit = self.registry.get_index_by_name(index_name)
        self._retract_index_record(
            circuit.index_id, circuit.owner_id, OWNER_KIND_TABLE,
            circuit.source_col_idx, circuit.name, int(circuit.is_unique), circuit.cache_dir
        )
        self.sys.indices.flush()

    def get_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        if not self.registry.has(schema_name, table_name):
            raise KeyError(qualified_name)
        return self.registry.get(schema_name, table_name)

    def close(self):
        keys = newlist_hint(len(self.registry._by_name))
        for k in self.registry._by_name: keys.append(k)
        for k in keys:
            family = self.registry._by_name[k]
            if family.table_id >= FIRST_USER_TABLE_ID: family.close()
        self.sys.close()

    # ── Private Write Helpers ────────────────────────────────────────────────

    def _write_schema_record(self, schema_id, name):
        s = self.sys.schemas.schema
        batch = ZSetBatch(s)
        _append_schema_record(batch, s, schema_id, name)
        self.sys.schemas.ingest_batch(batch)
        batch.free()

    def _retract_schema_record(self, schema_id, name):
        s = self.sys.schemas.schema
        batch = ZSetBatch(s)
        _retract_schema_record(batch, s, schema_id, name)
        self.sys.schemas.ingest_batch(batch)
        batch.free()

    def _write_table_record(
        self, table_id, schema_id, name, directory, pk_col_idx, created_lsn
    ):
        s = self.sys.tables.schema
        batch = ZSetBatch(s)
        _append_table_record(
            batch, s, table_id, schema_id, name, directory, pk_col_idx, created_lsn
        )
        self.sys.tables.ingest_batch(batch)
        batch.free()

    def _retract_table_record(
        self, table_id, schema_id, name, directory, pk_col_idx, created_lsn
    ):
        s = self.sys.tables.schema
        batch = ZSetBatch(s)
        _retract_table_record(
            batch, s, table_id, schema_id, name, directory, pk_col_idx, created_lsn
        )
        self.sys.tables.ingest_batch(batch)
        batch.free()

    def _write_column_records(self, owner_id, owner_kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            _append_column_record(
                batch, s, owner_id, owner_kind, i, col.name, col.field_type.code,
                int(col.is_nullable), col.fk_table_id, col.fk_col_idx
            )
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _retract_column_records(self, owner_id, owner_kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            _retract_column_record(
                batch, s, owner_id, owner_kind, i, col.name, col.field_type.code,
                int(col.is_nullable), col.fk_table_id, col.fk_col_idx
            )
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _write_index_record(
        self, index_id, owner_id, owner_kind, source_col_idx, name, is_unique, cache_directory
    ):
        s = self.sys.indices.schema
        batch = ZSetBatch(s)
        _append_index_record(
            batch, s, index_id, owner_id, owner_kind, source_col_idx, name, is_unique, cache_directory
        )
        self.sys.indices.ingest_batch(batch)
        batch.free()

    def _retract_index_record(
        self, index_id, owner_id, owner_kind, source_col_idx, name, is_unique, cache_directory
    ):
        s = self.sys.indices.schema
        batch = ZSetBatch(s)
        _retract_index_record(
            batch, s, index_id, owner_id, owner_kind, source_col_idx, name, is_unique, cache_directory
        )
        self.sys.indices.ingest_batch(batch)
        batch.free()

    def _advance_sequence(self, seq_id, old_val, new_val):
        s = self.sys.sequences.schema
        batch = ZSetBatch(s)
        _retract_sequence_record(batch, s, seq_id, old_val)
        _append_sequence_record(batch, s, seq_id, new_val)
        self.sys.sequences.ingest_batch(batch)
        batch.free()
