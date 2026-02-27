# gnitz/catalog/registry.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128

from gnitz.catalog.system_tables import (
    FIRST_USER_SCHEMA_ID,
    FIRST_USER_TABLE_ID,
    FIRST_USER_INDEX_ID,
)
from gnitz.core.errors import LayoutError, MemTableFullError
from gnitz.core.types import TableSchema, ColumnDefinition
from gnitz.core.batch import ZSetBatch
from gnitz.catalog.index_circuit import promote_to_index_key
from gnitz.storage.comparator import RawWALAccessor


def _ingest_into_index(circuit, idx_batch):
    """
    Internal helper to handle index ingestion.
    """
    try:
        circuit.table.ingest_batch(idx_batch)
    except MemTableFullError:
        circuit.table.flush()
        circuit.table.ingest_batch(idx_batch)


def _wire_fk_constraints_for_family(family, registry):
    """
    Iterates all FK columns of `family` and wires active FKConstraint objects.
    """
    columns = family.schema.columns
    for col_idx in range(len(columns)):
        col = columns[col_idx]
        if col.fk_table_id == 0:
            continue
        if col.fk_table_id == family.table_id:
            target_family = family
        elif registry.has_id(col.fk_table_id):
            target_family = registry.get_by_id(col.fk_table_id)
        else:
            continue
        family._add_fk_constraint(col_idx, target_family)


class FKConstraint(object):
    """
    An active FK constraint on a single column of a TableFamily.
    """
    _immutable_fields_ = ["fk_col_idx", "target_family"]

    def __init__(self, fk_col_idx, target_family):
        self.fk_col_idx = fk_col_idx
        self.target_family = target_family


class SystemTables(object):
    def __init__(self, schemas, tables, views, columns, indices, view_deps, sequences):
        self.schemas = schemas
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
    A logical table entity managing primary storage and secondary indices.
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
        self.created_lsn = 0
        self.primary = primary
        self.schema = primary.schema
        self.index_circuits = newlist_hint(4)
        self.fk_constraints = newlist_hint(0)

    def _add_fk_constraint(self, col_idx, target_family):
        self.fk_constraints.append(FKConstraint(col_idx, target_family))

    def get_schema(self):
        return self.schema

    def create_cursor(self):
        return self.primary.create_cursor()

    def ingest_batch(self, batch):
        """
        Durable ingestion with incremental FK enforcement.
        """
        n_constraints = len(self.fk_constraints)
        if n_constraints > 0:
            n_records = batch.length()
            for c_idx in range(n_constraints):
                constraint = self.fk_constraints[c_idx]
                col_idx = constraint.fk_col_idx
                target = constraint.target_family
                col_type = self.schema.columns[col_idx].field_type

                for i in range(n_records):
                    if batch.get_weight(i) <= 0:
                        continue
                    acc = batch.get_accessor(i)
                    if acc.is_null(col_idx):
                        continue

                    fk_key = promote_to_index_key(acc, col_idx, col_type)
                    if not target.primary.has_pk(fk_key):
                        raise LayoutError(
                            "FK violation: column '%s' value not found in '%s.%s'"
                            % (self.schema.columns[col_idx].name, 
                               target.schema_name, target.table_name)
                        )

        self.primary.ingest_batch(batch)

        n = len(self.index_circuits)
        for i in range(n):
            circuit = self.index_circuits[i]
            idx_batch = circuit.compute_index_delta(batch)
            if not idx_batch.is_empty():
                _ingest_into_index(circuit, idx_batch)

    def bulk_validate_all_constraints(self, chunk_size=131072):
        """
        High-performance bulk validation using the zero-allocation batch system.
        Used for full-table audits and after ALTER TABLE.
        """
        n_constraints = len(self.fk_constraints)
        if n_constraints == 0:
            return

        for c_idx in range(n_constraints):
            constraint = self.fk_constraints[c_idx]
            self._bulk_validate_single_fk(constraint, chunk_size)

    def _bulk_validate_single_fk(self, constraint, chunk_size):
        col_idx = constraint.fk_col_idx
        target = constraint.target_family
        col_type = self.schema.columns[col_idx].field_type

        # Create a projection schema: just the FK column (as the PK)
        proj_schema = TableSchema([self.schema.columns[col_idx]], pk_index=0)
        proj_batch = ZSetBatch(proj_schema, initial_capacity=chunk_size)
        
        source_cursor = self.primary.create_cursor()
        target_cursor = target.primary.create_cursor()
        
        # Zero-allocation row (reused for every insertion into proj_batch)
        dummy_row = make_payload_row(proj_schema)

        while source_cursor.is_valid():
            # 1. Chunked Projection
            while source_cursor.is_valid() and proj_batch.length() < chunk_size:
                if source_cursor.weight() > 0:
                    acc = source_cursor.get_accessor()
                    if not acc.is_null(col_idx):
                        fk_key = promote_to_index_key(acc, col_idx, col_type)
                        # We only care about unique FK presence.
                        # weight=1 is sufficient for presence validation.
                        proj_batch.append(fk_key, r_int64(1), dummy_row)
                source_cursor.advance()

            if proj_batch.is_empty():
                break

            # 2. Optimized Sort/Consolidate (Fixed-width fast path)
            proj_batch.sort()
            proj_batch.consolidate()

            # 3. Monotonic target validation
            n_unique = proj_batch.length()
            for i in range(n_unique):
                fk_key = proj_batch.get_pk(i)
                target_cursor.seek(fk_key)
                
                if not target_cursor.is_valid() or target_cursor.key() != fk_key:
                    proj_batch.free()
                    source_cursor.close()
                    target_cursor.close()
                    raise LayoutError(
                        "Referential integrity violation in '%s.%s': "
                        "value not found in target table '%s.%s'"
                        % (self.schema_name, self.table_name,
                           target.schema_name, target.table_name)
                    )
            
            proj_batch.clear()

        proj_batch.free()
        source_cursor.close()
        target_cursor.close()

    def flush(self):
        return self.primary.flush()

    def close(self):
        self.primary.close()
        for circuit in self.index_circuits:
            circuit.close()


class EntityRegistry(object):
    def __init__(self):
        self._by_name = {}
        self._by_id = {}
        self._schema_name_to_id = {}
        self._schema_id_to_name = {}
        self._index_by_name = {}
        self._index_by_id = {}
        self._next_schema_id = FIRST_USER_SCHEMA_ID
        self._next_table_id = FIRST_USER_TABLE_ID
        self._next_index_id = FIRST_USER_INDEX_ID

    def register_schema(self, schema_id, name):
        self._schema_name_to_id[name] = schema_id
        self._schema_id_to_name[schema_id] = name
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

    def register(self, family):
        qualified = family.schema_name + "." + family.table_name
        self._by_name[qualified] = family
        self._by_id[family.table_id] = family
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
        for k in self._by_name:
            family = self._by_name[k]
            if family.schema_name == schema_name:
                return False
        return True

    def allocate_index_id(self):
        iid = self._next_index_id
        self._next_index_id += 1
        return iid

    def register_index(self, index_id, name, circuit):
        self._index_by_name[name] = circuit
        self._index_by_id[index_id] = circuit
        if index_id >= self._next_index_id:
            self._next_index_id = index_id + 1

    def unregister_index(self, name, index_id):
        if name in self._index_by_name:
            del self._index_by_name[name]
        if index_id in self._index_by_id:
            del self._index_by_id[index_id]

    def has_index_by_name(self, name):
        return name in self._index_by_name

    def get_index_by_name(self, name):
        if name in self._index_by_name:
            return self._index_by_name[name]
        raise LayoutError("Index does not exist: %s" % name)

    def is_joinable(self, owner_id, col_idx):
        if not self.has_id(owner_id):
            return False
        family = self.get_by_id(owner_id)
        if col_idx == family.schema.pk_index:
            return True
        for circuit in family.index_circuits:
            if circuit.source_col_idx == col_idx:
                return True
        return False
