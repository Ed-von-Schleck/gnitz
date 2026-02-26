# gnitz/catalog/registry.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi

from gnitz.catalog.system_tables import (
    FIRST_USER_SCHEMA_ID,
    FIRST_USER_TABLE_ID,
    FIRST_USER_INDEX_ID,
)
from gnitz.core.errors import LayoutError, MemTableFullError
from gnitz.catalog.index_circuit import promote_to_index_key


def _ingest_into_index(circuit, idx_batch):
    """
    Internal helper to handle index ingestion.
    In translated mode, EphemeralTable may raise MemTableFullError,
    requiring a flush before retrying the ingestion.
    """
    try:
        circuit.table.ingest_batch(idx_batch)
    except MemTableFullError:
        circuit.table.flush()
        circuit.table.ingest_batch(idx_batch)


def _wire_fk_constraints_for_family(family, registry):
    """
    Iterates all FK columns of `family` and wires active FKConstraint objects.
    Self-referential FKs are handled: if fk_table_id == family.table_id, the
    target is family itself.
    Called after the family and all its potential FK targets are registered.
    """
    columns = family.schema.columns
    for col_idx in range(len(columns)):
        col = columns[col_idx]
        if col.fk_table_id == 0:
            continue
        if col.fk_table_id == family.table_id:
            target_family = family  # Self-referential FK.
        elif registry.has_id(col.fk_table_id):
            target_family = registry.get_by_id(col.fk_table_id)
        else:
            # Orphaned FK annotation: target was dropped. Skip silently;
            # the drop_table guard prevents this in normal operation.
            continue
        family._add_fk_constraint(col_idx, target_family)


class FKConstraint(object):
    """
    An active FK constraint on a single column of a TableFamily.
    `target_family` is a direct reference to the referenced TableFamily.
    Constraints are wired during create_table and _rebuild_registry; they are
    in-memory objects derived from the persisted _system._columns records.
    """

    _immutable_fields_ = ["fk_col_idx", "target_family"]

    def __init__(self, fk_col_idx, target_family):
        self.fk_col_idx = fk_col_idx
        self.target_family = target_family


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
    A logical table entity. Manages a primary PersistentTable and
    a collection of soft-state secondary indices.
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
        self.primary = primary  # PersistentTable
        self.schema = primary.schema
        # Soft-state secondary indices
        self.index_circuits = newlist_hint(4)
        # Active Foreign Key constraints
        self.fk_constraints = newlist_hint(0)

    def _add_fk_constraint(self, col_idx, target_family):
        """
        Registers an active FK constraint for column `col_idx`.
        Called once per FK column during create_table and _rebuild_registry.
        """
        self.fk_constraints.append(FKConstraint(col_idx, target_family))

    def get_schema(self):
        return self.schema

    def create_cursor(self):
        return self.primary.create_cursor()

    def ingest_batch(self, batch):
        """
        Durable ingestion into primary table followed by soft-state
        fan-out to all registered secondary indices.
        """
        # FK Enforcement Pre-commit Check
        n_constraints = len(self.fk_constraints)
        if n_constraints > 0:
            n_records = batch.length()
            for c_idx in range(n_constraints):
                constraint = self.fk_constraints[c_idx]
                col_idx = constraint.fk_col_idx
                target = constraint.target_family

                # Hoist cursor creation to batch-level
                target_cursor = target.primary.create_cursor()
                try:
                    for i in range(n_records):
                        weight = batch.get_weight(i)
                        if weight <= 0:
                            continue  # Skip retractions/annihilations

                        acc = batch.get_accessor(i)
                        if acc.is_null(col_idx):
                            continue  # NULL values permitted in FK columns

                        # Correct key promotion for signed/unsigned comparison
                        col_type = self.schema.columns[col_idx].field_type
                        fk_key = promote_to_index_key(acc, col_idx, col_type)

                        # O(log N) existence check
                        target_cursor.seek(fk_key)
                        found = (
                            target_cursor.is_valid()
                            and target_cursor.key() == fk_key
                            and target_cursor.weight() > 0
                        )

                        if not found:
                            raise LayoutError(
                                "Referential integrity violation: FK column '%s' value not "
                                "found in target table '%s.%s'"
                                % (
                                    self.schema.columns[col_idx].name,
                                    target.schema_name,
                                    target.table_name,
                                )
                            )
                finally:
                    target_cursor.close()

        # Primary Ingestion
        self.primary.ingest_batch(batch)

        # Index propagation
        n = len(self.index_circuits)
        for i in range(n):
            circuit = self.index_circuits[i]
            idx_batch = circuit.compute_index_delta(batch)
            if not idx_batch.is_empty():
                _ingest_into_index(circuit, idx_batch)
            idx_batch.free()

    def flush(self):
        # Flushing primary is sufficient for hard-state durability.
        return self.primary.flush()

    def close(self):
        self.primary.close()
        for circuit in self.index_circuits:
            circuit.close()


class EntityRegistry(object):
    """
    Central directory for mapping names and IDs to catalog entities.
    Tracks schema, table, and index ID sequences to prevent collisions.
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

        # Index Tracking
        self._index_by_name = {}  # str -> IndexCircuit
        self._index_by_id = {}  # int -> IndexCircuit

        # ID allocators
        self._next_schema_id = FIRST_USER_SCHEMA_ID
        self._next_table_id = FIRST_USER_TABLE_ID
        self._next_index_id = FIRST_USER_INDEX_ID

    # ── Schema management ────────────────────────────────────────────────

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

    # ── Table management ─────────────────────────────────────────────────

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

    # ── Index management ─────────────────────────────────────────────────

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
        """Returns True if col_idx is the PK or has a secondary index."""
        if not self.has_id(owner_id):
            return False
        family = self.get_by_id(owner_id)
        if col_idx == family.schema.pk_index:
            return True
        for circuit in family.index_circuits:
            if circuit.source_col_idx == col_idx:
                return True
        return False
