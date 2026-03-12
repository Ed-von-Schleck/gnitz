# gnitz/catalog/registry.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
)

from gnitz.catalog.system_tables import (
    FIRST_USER_SCHEMA_ID,
    FIRST_USER_TABLE_ID,
    FIRST_USER_INDEX_ID,
)
from gnitz.core.errors import LayoutError
from gnitz.core.keys import promote_to_index_key
from gnitz.core.batch import ZSetBatch


def _find_seen(lo_list, hi_list, pk_lo, pk_hi):
    """Linear scan of seen-PK lists. Returns index, or -1 if not found."""
    for i in range(len(lo_list)):
        if lo_list[i] == pk_lo and hi_list[i] == pk_hi:
            return i
    return -1


def _retract_from_out(out, schema, src_idx):
    """Copy out[src_idx] into a temp batch with w=-1, then append to out."""
    tmp = ZSetBatch(schema)
    tmp._direct_append_row(out, src_idx, r_int64(-1))
    out.append_batch(tmp, 0, tmp.length())
    tmp.free()


def _remove_seen(lo_list, hi_list, idx_list, i):
    """RPython-safe removal: overwrite slot i with last element, then shrink."""
    last = len(lo_list) - 1
    lo_list[i] = lo_list[last]
    hi_list[i] = hi_list[last]
    idx_list[i] = idx_list[last]
    del lo_list[last]
    del hi_list[last]
    del idx_list[last]


def _enforce_unique_pk(family, batch):
    """
    Transform batch for a unique_pk=True table into UPSERT/DELETE-by-PK semantics.

    - w > 0: auto-retract any stored row with same PK, then insert new row.
    - w < 0: emit retraction using stored payload (ignore incoming payload).

    Returns a new ZSetBatch. The caller owns it and must free it when done.
    """
    store = family.store
    schema = family.schema
    n = batch.length()
    out = ZSetBatch(schema)
    seen_lo = []    # r_uint64: lower 64 bits of seen PKs
    seen_hi = []    # r_uint64: upper 64 bits of seen PKs
    seen_out_idx = []   # int: index in out of the last +w row for this PK

    for i in range(n):
        pk = batch.get_pk(i)
        weight = batch.get_weight(i)
        pk_lo = r_uint64(pk)
        pk_hi = r_uint64(pk >> 64)

        if weight > r_int64(0):
            intra_idx = _find_seen(seen_lo, seen_hi, pk_lo, pk_hi)
            if intra_idx >= 0:
                # Cancel the previous in-batch insertion for this PK
                _retract_from_out(out, schema, seen_out_idx[intra_idx])
                seen_out_idx[intra_idx] = out.length()
            else:
                # Retract any existing stored row for this PK
                store.retract_pk(pk, out)
                seen_lo.append(pk_lo)
                seen_hi.append(pk_hi)
                seen_out_idx.append(out.length())
            out._direct_append_row(batch, i, weight)

        elif weight < r_int64(0):
            intra_idx = _find_seen(seen_lo, seen_hi, pk_lo, pk_hi)
            if intra_idx >= 0:
                # Cancel the pending in-batch insert (no net change)
                _retract_from_out(out, schema, seen_out_idx[intra_idx])
                _remove_seen(seen_lo, seen_hi, seen_out_idx, intra_idx)
            else:
                # Delete from storage by PK (ignore incoming payload)
                store.retract_pk(pk, out)

    return out


def wire_fk_constraints_for_family(family, registry):
    """
    Iterates all FK columns of `family` and wires active FKConstraint objects.
    Enables incremental validation during ingestion.
    """
    columns = family.schema.columns
    for col_idx in range(len(columns)):
        col = columns[col_idx]
        if col.fk_table_id == 0:
            continue

        target_family = None
        if col.fk_table_id == family.table_id:
            target_family = family
        elif registry.has_id(col.fk_table_id):
            target_family = registry.get_by_id(col.fk_table_id)

        if target_family:
            family._add_fk_constraint(col_idx, target_family)


class FKConstraint(object):
    """
    An active Foreign Key constraint tracking a source column and its target table.
    """

    _immutable_fields_ = ["fk_col_idx", "target_family"]

    def __init__(self, fk_col_idx, target_family):
        self.fk_col_idx = fk_col_idx
        self.target_family = target_family


def ingest_to_family(family, batch):
    """
    The external ingestion pipeline for user-table data.
    Enforces unique-PK and FK constraints, writes to storage, and updates
    secondary indices.

    Returns the effective batch (may be a new transformed batch when
    unique_pk=True, or the same object otherwise). Callers may use the
    returned batch for downstream DAG evaluation.

    For internal writes (VM operator state, catalog bootstrap, index backfill),
    call family.store.ingest_batch(batch) directly to bypass enforcement.
    """
    n_records = batch.length()
    if n_records == 0:
        return batch

    # --- Stage 0: Unique PK Enforcement ---
    if family.unique_pk:
        batch = _enforce_unique_pk(family, batch)
        n_records = batch.length()
        if n_records == 0:
            return batch

    # --- Stage 1: Foreign Key Enforcement ---
    n_constraints = len(family.fk_constraints)
    if n_constraints > 0:
        acc = batch.get_accessor(0)

        for i in range(n_records):
            # We only validate insertions (positive weights)
            if batch.get_weight(i) <= 0:
                continue

            batch.bind_accessor(i, acc)

            for c_idx in range(n_constraints):
                constraint = family.fk_constraints[c_idx]
                col_idx = constraint.fk_col_idx
                if acc.is_null(col_idx):
                    continue

                # Extract value and promote to the target PK's index key type
                fk_key = promote_to_index_key(
                    acc, col_idx, family.schema.columns[col_idx].field_type
                )

                if not constraint.target_family.store.has_pk(fk_key):
                    raise LayoutError(
                        "Foreign Key violation in '%s.%s': value for column '%s' "
                        "not found in target '%s.%s'"
                        % (
                            family.schema_name,
                            family.table_name,
                            family.schema.columns[col_idx].name,
                            constraint.target_family.schema_name,
                            constraint.target_family.table_name,
                        )
                    )

    # --- Stage 2: Primary Ingestion (Durable WAL + MemTable) ---
    family.store.ingest_batch(batch)

    # --- Stage 3: Secondary Index Projection ---
    n_indices = len(family.index_circuits)
    if n_indices > 0:
        for idx_num in range(n_indices):
            circuit = family.index_circuits[idx_num]
            # Delegating projection to the EphemeralTable storage of the index
            circuit.table.ingest_projection(
                batch,
                circuit.source_col_idx,
                circuit.source_col_type,
                circuit._index_payload_accessor,
                circuit.is_unique,
            )

    # --- Stage 4: Post-Ingestion Effect Hooks ---
    for h_idx in range(len(family.post_ingest_hooks)):
        family.post_ingest_hooks[h_idx].on_delta(batch)

    return batch


class TableFamily(object):
    """
    Relational metadata container for a table or view.
    Manages a ZSetStore (PersistentTable or EphemeralTable) and
    associated secondary IndexCircuits.
    """

    _immutable_fields_ = [
        "schema_name",
        "table_name",
        "table_id",
        "schema_id",
        "directory",
        "pk_col_idx",
        "store",
        "schema",
        "unique_pk",
    ]

    def __init__(
        self,
        schema_name,
        table_name,
        table_id,
        schema_id,
        directory,
        pk_col_idx,
        store,
        unique_pk=False,
    ):
        self.schema_name = schema_name
        self.table_name = table_name
        self.table_id = table_id
        self.schema_id = schema_id
        self.directory = directory
        self.pk_col_idx = pk_col_idx
        self.store = store
        self.unique_pk = unique_pk
        self.schema = store.get_schema()
        self.index_circuits = newlist_hint(4)
        self.fk_constraints = newlist_hint(0)
        self.post_ingest_hooks = newlist_hint(0)
        self.depth = 0
        self.created_lsn = 0

    def _add_fk_constraint(self, col_idx, target_family):
        self.fk_constraints.append(FKConstraint(col_idx, target_family))

    def bulk_validate_all_constraints(self):
        """Performs a full audit of referential integrity."""
        for c_idx in range(len(self.fk_constraints)):
            self._bulk_validate_single_fk(self.fk_constraints[c_idx])

    def _bulk_validate_single_fk(self, constraint):
        """Optimized bulk validation via supporting index scan."""
        supporting_circuit = None
        for circuit in self.index_circuits:
            if circuit.source_col_idx == constraint.fk_col_idx:
                supporting_circuit = circuit
                break

        if supporting_circuit is None:
            raise LayoutError(
                "No supporting index found for FK on column '%s'"
                % self.schema.columns[constraint.fk_col_idx].name
            )

        idx_cursor = supporting_circuit.create_cursor()
        try:
            while idx_cursor.is_valid():
                if idx_cursor.weight() > 0:
                    fk_val = idx_cursor.key()
                    if not constraint.target_family.store.has_pk(fk_val):
                        idx_cursor.close()
                        raise LayoutError(
                            "Referential integrity violation: value in '%s.%s' "
                            "not found in target '%s.%s'"
                            % (
                                self.schema_name,
                                self.table_name,
                                constraint.target_family.schema_name,
                                constraint.target_family.table_name,
                            )
                        )
                idx_cursor.advance()
        finally:
            idx_cursor.close()

    def close(self):
        self.store.close()
        for circuit in self.index_circuits:
            circuit.close()


class EntityRegistry(object):
    """
    Central hub for name resolution and metadata mapping.
    """

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
        # Partition range for user-table creation. Default: all 256.
        # Workers narrow this after fork; master sets (0, 0) to skip.
        self.active_part_start = 0
        self.active_part_end = 256

    def recover_sequences(self, schema_hwm, table_hwm, index_hwm, programs_hwm):
        """
        Advances sequence counters to the high-water marks recovered from
        durable storage during engine startup.
        """
        next_schema = schema_hwm + 1
        if next_schema > self._next_schema_id:
            self._next_schema_id = next_schema

        next_table = table_hwm + 1
        if next_table > self._next_table_id:
            self._next_table_id = next_table

        next_index = index_hwm + 1
        if next_index > self._next_index_id:
            self._next_index_id = next_index

        next_programs = programs_hwm + 1
        if next_programs > self._next_table_id:
            self._next_table_id = next_programs

    def iter_families(self):
        """
        Returns a snapshot list of all registered TableFamily objects.
        """
        result = newlist_hint(len(self._by_name))
        for k in self._by_name:
            result.append(self._by_name[k])
        return result

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
        if qualified not in self._by_name:
            raise LayoutError("Unknown entity: %s" % qualified)
        return self._by_name[qualified]

    def get_by_id(self, table_id):
        if table_id not in self._by_id:
            raise LayoutError("Unknown table_id %d" % table_id)
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

    def get_depth(self, view_id):
        if view_id in self._by_id:
            return self._by_id[view_id].depth
        return 0
