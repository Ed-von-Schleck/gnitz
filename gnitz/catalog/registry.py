# gnitz/catalog/registry.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)

from gnitz.catalog.system_tables import (
    FIRST_USER_SCHEMA_ID,
    FIRST_USER_TABLE_ID,
    FIRST_USER_INDEX_ID,
)
from gnitz.core.errors import LayoutError
from gnitz.core.keys import promote_to_index_key
from gnitz.core.batch import ArenaZSetBatch


def _retract_from_out(out, schema, src_idx):
    """Copy out[src_idx] into a temp batch with w=-1, then append to out."""
    tmp = ArenaZSetBatch(schema)
    tmp._direct_append_row(out, src_idx, r_int64(-1))
    out.append_batch(tmp, 0, tmp.length())
    tmp.free()


def _enforce_unique_pk(family, batch):
    """
    Transform batch for a unique_pk=True table into UPSERT/DELETE-by-PK semantics.

    - w > 0: auto-retract any stored row with same PK, then insert new row.
    - w < 0: emit retraction using stored payload (ignore incoming payload).

    Returns a new ArenaZSetBatch. The caller owns it and must free it when done.
    """
    store = family.store
    schema = family.schema
    n = batch.length()
    out = ArenaZSetBatch(schema)
    seen_dict = {}   # {int: {int: int}}  intmask(pk_lo) -> {intmask(pk_hi) -> out_idx}

    for i in range(n):
        pk_lo = r_uint64(batch._read_pk_lo(i))
        pk_hi = r_uint64(batch._read_pk_hi(i))
        weight = batch.get_weight(i)
        lo_key = intmask(pk_lo)
        hi_key = intmask(pk_hi)

        if weight > r_int64(0):
            intra_out_idx = -1
            if lo_key in seen_dict:
                inner = seen_dict[lo_key]
                if hi_key in inner:
                    intra_out_idx = inner[hi_key]
            if intra_out_idx >= 0:
                # Cancel the previous in-batch insertion for this PK
                _retract_from_out(out, schema, intra_out_idx)
                seen_dict[lo_key][hi_key] = out.length()
            else:
                # Retract any existing stored row for this PK
                pk128 = (r_uint128(pk_hi) << 64) | r_uint128(pk_lo)
                store.retract_pk(pk128, out)
                if lo_key not in seen_dict:
                    seen_dict[lo_key] = {}
                seen_dict[lo_key][hi_key] = out.length()
            out._direct_append_row(batch, i, weight)

        elif weight < r_int64(0):
            intra_out_idx = -1
            if lo_key in seen_dict:
                inner = seen_dict[lo_key]
                if hi_key in inner:
                    intra_out_idx = inner[hi_key]
            if intra_out_idx >= 0:
                # Cancel the pending in-batch insert (no net change)
                _retract_from_out(out, schema, intra_out_idx)
                del seen_dict[lo_key][hi_key]
            else:
                # Delete from storage by PK (ignore incoming payload)
                pk128 = (r_uint128(pk_hi) << 64) | r_uint128(pk_lo)
                store.retract_pk(pk128, out)

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


def _build_pk_check_batch(schema, lo_list, hi_list):
    """Build a batch containing the given PK (lo,hi) pairs, columns zeroed."""
    n = len(lo_list)
    batch = ArenaZSetBatch(schema, initial_capacity=max(n, 1))
    num_cols = len(schema.columns)
    for k in range(n):
        batch.pk_lo_buf.put_u64(lo_list[k])
        batch.pk_hi_buf.put_u64(hi_list[k])
        batch.weight_buf.put_i64(r_int64(1))
        # Payload columns are intentionally null-masked. This batch is PK-only:
        # only pk_lo_buf / pk_hi_buf matter for has_pk() checks. All null bits are
        # set so that accidental column reads return NULL instead of garbage bytes.
        batch.null_buf.put_u64(r_uint64(0xFFFFFFFFFFFFFFFF))
        for ci in range(num_cols):
            if ci == schema.pk_index:
                continue
            stride = schema.columns[ci].field_type.size
            dest = batch.col_bufs[ci].alloc(
                stride, alignment=schema.columns[ci].field_type.alignment
            )
            for b in range(stride):
                dest[b] = "\x00"
        batch._count += 1
    return batch


def validate_fk_distributed(family, batch, dispatcher):
    """Validate FK constraints via distributed PK lookups through the dispatcher.

    Called by the master before fan_out_push. For each FK constraint, collects
    unique FK values, builds a check batch, and asks workers if those PKs exist
    in the target table. Raises LayoutError on violation.
    """
    n_constraints = len(family.fk_constraints)
    if n_constraints == 0:
        return

    n_records = batch.length()
    acc = batch.get_accessor(0)

    for c_idx in range(n_constraints):
        constraint = family.fk_constraints[c_idx]
        col_idx = constraint.fk_col_idx
        target_family = constraint.target_family
        target_id = target_family.table_id
        target_schema = target_family.schema

        # Collect unique FK values as (lo, hi) pairs — avoid r_uint128 lists
        seen_lo = []
        seen_hi = []
        seen_dict = {}   # {int: {int: int}}  dedup sentinel

        for i in range(n_records):
            if batch.get_weight(i) <= r_int64(0):
                continue
            batch.bind_accessor(i, acc)
            if acc.is_null(col_idx):
                continue

            fk_key = promote_to_index_key(
                acc, col_idx, family.schema.columns[col_idx].field_type
            )
            fk_lo = r_uint64(intmask(fk_key))
            fk_hi = r_uint64(intmask(fk_key >> 64))

            # Dedup via O(1) dict lookup
            lo_key = intmask(fk_lo)
            hi_key = intmask(fk_hi)
            if lo_key not in seen_dict or hi_key not in seen_dict[lo_key]:
                if lo_key not in seen_dict:
                    seen_dict[lo_key] = {}
                seen_dict[lo_key][hi_key] = 1
                seen_lo.append(fk_lo)
                seen_hi.append(fk_hi)

        if len(seen_lo) == 0:
            continue

        check_batch = _build_pk_check_batch(target_schema, seen_lo, seen_hi)
        any_missing = dispatcher.check_fk_batch(
            target_id, check_batch, target_schema
        )
        check_batch.free()

        if any_missing:
            raise LayoutError(
                "Foreign Key violation in '%s.%s': value for column '%s' "
                "not found in target '%s.%s'"
                % (
                    family.schema_name,
                    family.table_name,
                    family.schema.columns[col_idx].name,
                    target_family.schema_name,
                    target_family.table_name,
                )
            )


def validate_fk_inline(family, batch):
    """
    Single-process FK validation. Checks foreign key values against the local
    PartitionedTable store, which owns all 256 partitions.

    ONLY call this in single-process (no dispatcher) mode, where the local store
    is complete. In multi-worker mode, FK validation is done at the master via
    validate_fk_distributed() before fan_out_push — workers receive pre-validated
    batches and must NOT call this function.

    Called by: executor.handle_push (single-process path) before ingest_to_family.
    Counterpart: validate_fk_distributed (master, multi-worker path).
    """
    n_constraints = len(family.fk_constraints)
    if n_constraints == 0:
        return

    n_records = batch.length()
    acc = batch.get_accessor(0)

    for i in range(n_records):
        if batch.get_weight(i) <= r_int64(0):
            continue

        batch.bind_accessor(i, acc)

        for c_idx in range(n_constraints):
            constraint = family.fk_constraints[c_idx]
            col_idx = constraint.fk_col_idx
            if acc.is_null(col_idx):
                continue

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


def validate_unique_indices_distributed(family, batch, dispatcher):
    """Validate unique index constraints via distributed PK lookups.

    Called by the master before fan_out_push. For each unique index, collects
    index keys from positive-weight rows, checks batch-internal duplicates,
    then broadcasts to ALL workers (index stores are partitioned by main-table PK,
    not index key). Raises LayoutError on violation.

    For unique_pk tables, UPSERT rows (PKs already in the table) are skipped —
    the worker-level check in ingest_projection handles those after
    _enforce_unique_pk retracts the old indexed value.
    """
    # Quick check: any unique index circuits?
    has_unique = False
    for ic in range(len(family.index_circuits)):
        if family.index_circuits[ic].is_unique:
            has_unique = True
            break
    if not has_unique:
        return

    n = batch.length()
    acc = batch.get_accessor(0)

    # For unique_pk tables, determine which PKs already exist (UPSERT rows)
    existing_pks = None
    if family.unique_pk:
        pk_lo_list = []
        pk_hi_list = []
        for i in range(n):
            if batch.get_weight(i) <= r_int64(0):
                continue
            pk_lo_list.append(r_uint64(batch._read_pk_lo(i)))
            pk_hi_list.append(r_uint64(batch._read_pk_hi(i)))
        if len(pk_lo_list) > 0:
            pk_check = _build_pk_check_batch(family.schema, pk_lo_list, pk_hi_list)
            existing_pks = dispatcher.check_pk_existence(
                family.table_id, pk_check, family.schema
            )
            pk_check.free()

    # Check each unique index
    for ic_idx in range(len(family.index_circuits)):
        circuit = family.index_circuits[ic_idx]
        if not circuit.is_unique:
            continue

        seen_lo = []
        seen_hi = []
        seen_dict = {}   # {int: {int: int}}

        for i in range(n):
            if batch.get_weight(i) <= r_int64(0):
                continue
            batch.bind_accessor(i, acc)
            if acc.is_null(circuit.source_col_idx):
                continue

            # Skip UPSERT rows (worker handles after _enforce_unique_pk)
            if existing_pks is not None:
                pk_lo = intmask(r_uint64(batch._read_pk_lo(i)))
                pk_hi = intmask(r_uint64(batch._read_pk_hi(i)))
                if pk_lo in existing_pks and pk_hi in existing_pks[pk_lo]:
                    continue

            idx_key = promote_to_index_key(
                acc, circuit.source_col_idx, circuit.source_col_type
            )
            lo = r_uint64(intmask(idx_key))
            hi = r_uint64(intmask(idx_key >> 64))
            lo_key = intmask(lo)
            hi_key = intmask(hi)

            if lo_key in seen_dict and hi_key in seen_dict[lo_key]:
                raise LayoutError(
                    "Unique index violation on column '%s': duplicate in batch"
                    % family.schema.columns[circuit.source_col_idx].name
                )
            if lo_key not in seen_dict:
                seen_dict[lo_key] = {}
            seen_dict[lo_key][hi_key] = 1
            seen_lo.append(lo)
            seen_hi.append(hi)

        if len(seen_lo) == 0:
            continue

        idx_schema = circuit.table.get_schema()
        check_batch = _build_pk_check_batch(idx_schema, seen_lo, seen_hi)
        any_exists = dispatcher.check_pk_exists_broadcast(
            family.table_id, circuit.source_col_idx, check_batch, idx_schema
        )
        check_batch.free()

        if any_exists:
            raise LayoutError(
                "Unique index violation on column '%s'"
                % family.schema.columns[circuit.source_col_idx].name
            )


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
    The ZSet ingestion pipeline for a table family.

    Stages:
      0 — Unique-PK dedup (if family.unique_pk): transforms batch into effective
          batch with auto-retract semantics. Returns new batch when unique_pk=True;
          returns same batch object when unique_pk=False.
      2 — Storage ingest (WAL + memtable). Does NOT flush.
      3 — Secondary index projection for all family.index_circuits.
      4 — Post-ingestion hooks (e.g. DDL effect hooks on system tables).

    FK constraints are NOT enforced here. FK validation is a partition-global
    concern and belongs at the request layer:
      - Single-process:  call validate_fk_inline(family, batch) BEFORE this.
      - Multi-worker:    master calls validate_fk_distributed() before fan_out_push.
                         Workers receive pre-validated batches and call this directly.

    Flush is NOT called. Callers are responsible for calling family.store.flush()
    after this function when durability is required.

    Return value: the effective batch (same object as batch when unique_pk=False;
    a newly allocated batch when unique_pk=True). When unique_pk=True, the caller
    must free the returned batch if it is not the same object as the input.
    Use `if effective is not batch: effective.free()` after use.
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
