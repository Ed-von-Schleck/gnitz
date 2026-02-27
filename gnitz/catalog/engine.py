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
    _wire_fk_constraints_for_family,
)
from gnitz.catalog.system_records import (
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

    return Engine(base_dir, sys_tables, registry)


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
    Must use newlist_hint to avoid mr-poisoning (Appendix A).
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
    Raises LayoutError if any constraint is violated.
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

    # FK must target the Primary Key column
    if col.fk_col_idx != target_pk_index:
        raise LayoutError(
            "FK on column '%s' must reference the PK column of target table '%s'"
            % (col.name, target_name)
        )

    # Promoted FK key type must match target PK type (e.g. U32 promotes to U64)
    promoted_type = get_index_key_type(col.field_type)
    if promoted_type.code != target_pk_type.code:
        raise LayoutError(
            "FK column '%s' promoted type (code %d) is incompatible with "
            "target PK type (code %d)"
            % (col.name, promoted_type.code, target_pk_type.code)
        )


class Engine(object):
    """
    The public DDL API for GnitzDB.
    Orchestrates metadata persistence in system tables and in-memory registry state.
    """

    def __init__(self, base_dir, sys_tables, registry):
        self.base_dir = base_dir
        self.sys = sys_tables
        self.registry = registry

    def create_schema(self, name):
        validate_user_identifier(name)
        if self.registry.has_schema(name):
            raise LayoutError("Schema already exists: %s" % name)

        _ensure_dir(self.base_dir + "/" + name)

        sid = self.registry.allocate_schema_id()
        self._write_schema_record(sid, name)
        self._advance_sequence(SEQ_ID_SCHEMAS, sid - 1, sid)
        self.registry.register_schema(sid, name)

    def drop_schema(self, name):
        validate_user_identifier(name)
        if not self.registry.has_schema(name):
            raise LayoutError("Schema does not exist: %s" % name)
        if name == "_system":
            raise LayoutError("Cannot drop system schema")
        if not self.registry.schema_is_empty(name):
            raise LayoutError("Cannot drop non-empty schema '%s'" % name)

        sid = self.registry.get_schema_id(name)
        self._retract_schema_record(sid, name)
        self.registry.unregister_schema(name, sid)

    def create_table(self, qualified_name, columns, pk_col_idx):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        validate_user_identifier(schema_name)
        validate_user_identifier(table_name)

        if not self.registry.has_schema(schema_name):
            raise LayoutError("Schema does not exist: %s" % schema_name)
        if self.registry.has(schema_name, table_name):
            raise LayoutError("Table already exists: %s" % qualified_name)
        if pk_col_idx < 0 or pk_col_idx >= len(columns):
            raise LayoutError("Primary Key index out of bounds")

        # Pre-allocate ID to allow self-referential FK validation
        tid = self.registry.allocate_table_id()
        sid = self.registry.get_schema_id(schema_name)

        # Validate all columns (including FK constraints)
        self_pk_type = columns[pk_col_idx].field_type
        for col_idx in range(len(columns)):
            _validate_fk_column(
                columns[col_idx],
                col_idx,
                self.registry,
                self_table_id=tid,
                self_pk_index=pk_col_idx,
                self_pk_type=self_pk_type,
            )

        schema_dir = self.base_dir + "/" + schema_name
        _ensure_dir(schema_dir)
        directory = schema_dir + "/" + table_name + "_" + str(tid)

        tbl_schema = TableSchema(columns, pk_col_idx)
        from gnitz.storage.table import PersistentTable

        pt = PersistentTable(directory, table_name, tbl_schema, table_id=tid)

        self._write_column_records(tid, OWNER_KIND_TABLE, columns)
        self._write_table_record(tid, sid, table_name, directory, pk_col_idx, 0)
        self._advance_sequence(SEQ_ID_TABLES, tid - 1, tid)

        family = TableFamily(
            schema_name, table_name, tid, sid, directory, pk_col_idx, pt
        )
        self.registry.register(family)

        # Auto-create indices for FK columns
        self._create_fk_indices_for_family(family)

        # Wire constraints into the memory model
        _wire_fk_constraints_for_family(family, self.registry)

        return family

    def drop_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        validate_user_identifier(schema_name)
        validate_user_identifier(table_name)

        if not self.registry.has(schema_name, table_name):
            raise LayoutError("Table does not exist: %s" % qualified_name)

        family = self.registry.get(schema_name, table_name)
        tid = family.table_id

        # Protect against referenced Foreign Keys (Phase C Step 8)
        for k in self.registry._by_name:
            referencing = self.registry._by_name[k]
            if referencing.table_id == tid:
                continue
            for col in referencing.schema.columns:
                if col.fk_table_id == tid:
                    raise LayoutError(
                        "Cannot drop table '%s': table '%s.%s' has a foreign key referencing it"
                        % (
                            qualified_name,
                            referencing.schema_name,
                            referencing.table_name,
                        )
                    )

        tbl_schema = family.schema

        # 1. Drop indices
        # Use a temporary snapshot to avoid list mutation issues during iteration
        circuits_to_drop = newlist_hint(len(family.index_circuits))
        for c in family.index_circuits:
            circuits_to_drop.append(c)

        for circuit in circuits_to_drop:
            self._retract_index_record(
                circuit.index_id,
                circuit.owner_id,
                OWNER_KIND_TABLE,
                circuit.source_col_idx,
                circuit.name,
                int(circuit.is_unique),
                circuit.cache_dir,
            )
            self.registry.unregister_index(circuit.name, circuit.index_id)
            circuit.close()

        family.index_circuits = newlist_hint(0)

        # 2. Retract table metadata
        self._retract_table_record(
            tid,
            family.schema_id,
            table_name,
            family.directory,
            family.pk_col_idx,
            family.created_lsn,
        )
        self._retract_column_records(tid, OWNER_KIND_TABLE, tbl_schema.columns)

        family.close()
        self.registry.unregister(schema_name, table_name)

    def create_index(self, qualified_owner_name, col_name, is_unique=False):
        schema_name, table_name = parse_qualified_name(qualified_owner_name, "public")
        validate_user_identifier(schema_name)
        validate_user_identifier(table_name)

        if not self.registry.has(schema_name, table_name):
            raise LayoutError("Table does not exist: %s" % qualified_owner_name)

        family = self.registry.get(schema_name, table_name)

        col_idx = -1
        for i in range(len(family.schema.columns)):
            if family.schema.columns[i].name == col_name:
                col_idx = i
                break

        if col_idx == -1:
            raise LayoutError(
                "Column '%s' does not exist on table %s"
                % (col_name, qualified_owner_name)
            )

        if col_idx == family.schema.pk_index:
            raise LayoutError("Cannot create index on PK column")

        col = family.schema.columns[col_idx]
        get_index_key_type(col.field_type)

        for existing in family.index_circuits:
            if existing.source_col_idx == col_idx:
                raise LayoutError("Index already exists on this column")

        index_name = make_secondary_index_name(schema_name, table_name, col_name)
        if self.registry.has_index_by_name(index_name):
            raise LayoutError("Index name collision: %s" % index_name)

        index_id = self.registry.allocate_index_id()
        source_pk_type = family.schema.get_pk_column().field_type
        idx_dir = family.directory + "/idx_" + str(index_id)
        
        circuit = _make_index_circuit(
            index_id,
            family.table_id,
            col_idx,
            col.field_type,
            source_pk_type,
            idx_dir,
            index_name,
            is_unique,
            "",
        )
        
        # Populations uses Direct Injection Kernel (upsert_index_row)
        _backfill_index(circuit, family)

        self._write_index_record(
            index_id,
            family.table_id,
            OWNER_KIND_TABLE,
            col_idx,
            index_name,
            int(is_unique),
            "",
        )
        self._advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id)

        family.index_circuits.append(circuit)
        self.registry.register_index(index_id, index_name, circuit)

        return circuit

    def _create_fk_indices_for_family(self, family):
        """
        Auto-creates indices for all Foreign Key columns (Phase C Step 6).
        Called during create_table.
        """
        schema_name = family.schema_name
        table_name = family.table_name
        columns = family.schema.columns

        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if col.fk_table_id == 0 or col_idx == family.schema.pk_index:
                continue

            index_name = make_fk_index_name(schema_name, table_name, col.name)

            # Avoid duplication if a user index already exists
            already_exists = False
            for existing in family.index_circuits:
                if existing.source_col_idx == col_idx:
                    already_exists = True
                    break
            if already_exists:
                continue

            index_id = self.registry.allocate_index_id()
            source_pk_type = family.schema.get_pk_column().field_type
            idx_dir = family.directory + "/idx_" + str(index_id)

            circuit = _make_index_circuit(
                index_id,
                family.table_id,
                col_idx,
                col.field_type,
                source_pk_type,
                idx_dir,
                index_name,
                False,  # is_unique = False
                "",
            )
            _backfill_index(circuit, family)

            self._write_index_record(
                index_id,
                family.table_id,
                OWNER_KIND_TABLE,
                col_idx,
                index_name,
                0,  # is_unique = False
                "",
            )
            self._advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id)

            family.index_circuits.append(circuit)
            self.registry.register_index(index_id, index_name, circuit)

    def drop_index(self, index_name):
        circuit = self.registry.get_index_by_name(index_name)
        
        # Guard against manual drop of auto-generated FK indices (Phase C Step 11)
        if "__fk_" in index_name:
            raise LayoutError(
                "Index '%s' is FK-implied and cannot be dropped directly. "
                "Drop the referencing table or its FK column instead." % index_name
            )

        family = self.registry.get_by_id(circuit.owner_id)

        self._retract_index_record(
            circuit.index_id,
            circuit.owner_id,
            OWNER_KIND_TABLE,
            circuit.source_col_idx,
            circuit.name,
            int(circuit.is_unique),
            circuit.cache_dir,
        )

        _remove_circuit_from_family(circuit, family)
        self.registry.unregister_index(circuit.name, circuit.index_id)
        circuit.close()

    def get_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        if not self.registry.has(schema_name, table_name):
            raise KeyError(qualified_name)
        return self.registry.get(schema_name, table_name)

    def close(self):
        # Create a snapshot of keys to avoid modification issues during close
        keys = newlist_hint(len(self.registry._by_name))
        for k in self.registry._by_name:
            keys.append(k)

        for k in keys:
            family = self.registry._by_name[k]
            if family.table_id >= FIRST_USER_TABLE_ID:
                family.close()

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
                batch,
                s,
                owner_id,
                owner_kind,
                i,
                col.name,
                col.field_type.code,
                int(col.is_nullable),
                col.fk_table_id,
                col.fk_col_idx,
            )
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _retract_column_records(self, owner_id, owner_kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            _retract_column_record(
                batch,
                s,
                owner_id,
                owner_kind,
                i,
                col.name,
                col.field_type.code,
                int(col.is_nullable),
                col.fk_table_id,
                col.fk_col_idx,
            )
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _write_index_record(
        self,
        index_id,
        owner_id,
        owner_kind,
        source_col_idx,
        name,
        is_unique,
        cache_directory,
    ):
        s = self.sys.indices.schema
        batch = ZSetBatch(s)
        _append_index_record(
            batch,
            s,
            index_id,
            owner_id,
            owner_kind,
            source_col_idx,
            name,
            is_unique,
            cache_directory,
        )
        self.sys.indices.ingest_batch(batch)
        batch.free()

    def _retract_index_record(
        self,
        index_id,
        owner_id,
        owner_kind,
        source_col_idx,
        name,
        is_unique,
        cache_directory,
    ):
        s = self.sys.indices.schema
        batch = ZSetBatch(s)
        _retract_index_record(
            batch,
            s,
            index_id,
            owner_id,
            owner_kind,
            source_col_idx,
            name,
            is_unique,
            cache_directory,
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
