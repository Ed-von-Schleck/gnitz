# gnitz/catalog/hooks.py

import os
from rpython.rlib.rarithmetic import r_uint64, r_int64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core.errors import LayoutError
from gnitz.core.types import TableSchema
from gnitz.storage.owned_batch import ArenaZSetBatch
from gnitz.storage import mmap_posix
from gnitz.storage.partitioned_table import (
    make_partitioned_persistent,
    make_partitioned_ephemeral,
    get_num_partitions,
)
from gnitz.catalog import system_tables as sys
from gnitz.catalog.metadata import ensure_dir, read_column_defs
from gnitz.catalog.registry import (
    TableFamily,
    wire_fk_constraints_for_family,
    ingest_to_family,
)
from gnitz.catalog.index_circuit import (
    _make_index_circuit,
    _backfill_index,
    make_fk_index_name,
)
from gnitz.storage import engine_ffi


def _view_needs_exchange_dag(engine, vid):
    """True if the view requires worker exchange; local backfill is unsafe."""
    return engine_ffi._dag_view_needs_exchange(engine.dag_handle, vid) != 0


def _backfill_view(engine, vid):
    """Scan each source family and feed all live rows through the view's plan."""
    if not engine_ffi._dag_ensure_compiled(engine.dag_handle, vid):
        return
    view_family = engine.registry.get_by_id(vid)
    out_buf = lltype.malloc(rffi.LONGLONGP.TO, 64, flavor="raw")
    try:
        n = engine_ffi._dag_get_source_ids(engine.dag_handle, vid, out_buf, 64)
        source_ids = [intmask(out_buf[i]) for i in range(intmask(n))]
    finally:
        lltype.free(out_buf, flavor="raw")
    for source_id in source_ids:
        if not engine.registry.has_id(source_id):
            continue
        src_family = engine.registry.get_by_id(source_id)
        scan_batch = ArenaZSetBatch(src_family.schema)
        cursor = src_family.store.create_cursor()
        try:
            while cursor.is_valid():
                w = cursor.weight()
                if w > r_int64(0):
                    acc = cursor.get_accessor()
                    scan_batch.append_from_accessor(cursor.key_lo(), cursor.key_hi(), w, acc)
                cursor.advance()
        finally:
            cursor.close()
        if scan_batch.length() == 0:
            scan_batch.free()
            continue
        out_handle = engine_ffi._dag_execute_epoch(
            engine.dag_handle, vid, scan_batch._handle, source_id)
        # scan_batch._handle ownership was transferred to _dag_execute_epoch;
        # prevent double-free by setting _handle to null before free().
        scan_batch._handle = lltype.nullptr(rffi.VOIDP.TO)
        scan_batch.free()
        if out_handle:
            result = ArenaZSetBatch._wrap_handle(
                view_family.schema, out_handle, False, False)
            if result.length() > 0:
                ingest_to_family(view_family, result)
                view_family.store.flush()
            result.free()


def _remove_circuit_from_family(circuit, family):
    """
    RPython-safe removal of a circuit from a family's resizable list.
    """
    old = family.index_circuits
    new_list = newlist_hint(len(old))
    for c in old:
        if c.index_id != circuit.index_id:
            new_list.append(c)
    family.index_circuits = new_list


class DeltaHook(object):
    """
    Base class for post-ingestion effect hooks on TableFamily.
    Subclasses override on_delta to react to Z-Set batch changes.
    """

    def on_delta(self, batch):
        raise NotImplementedError


class SchemaEffectHook(DeltaHook):
    """Reacts to additions or removals of database schemas."""

    _immutable_fields_ = ["engine"]

    def __init__(self, engine):
        self.engine = engine

    def on_delta(self, batch):
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            batch.bind_accessor(i, acc)

            sid = intmask(r_uint64(batch.get_pk(i)))
            name = sys.read_string(acc, sys.SchemaTab.COL_NAME)

            if weight > 0:
                if self.engine.registry.has_schema(name):
                    continue

                path = self.engine.base_dir + "/" + name
                ensure_dir(path)
                mmap_posix.fsync_dir(self.engine.base_dir)

                self.engine.registry.register_schema(sid, name)
            else:
                if self.engine.registry.has_schema(name):
                    if name == "_system":
                        raise LayoutError("Forbidden: cannot drop system schema")
                    if not self.engine.registry.schema_is_empty(name):
                        raise LayoutError("Cannot drop non-empty schema: %s" % name)
                    self.engine.registry.unregister_schema(name, sid)


class TableEffectHook(DeltaHook):
    """Reacts to additions or removals of persistent user tables."""

    _immutable_fields_ = ["engine"]

    def __init__(self, engine):
        self.engine = engine
        self.cascade_enabled = False

    def on_delta(self, batch):
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            tid = intmask(r_uint64(batch.get_pk(i)))
            batch.bind_accessor(i, acc)

            sid = intmask(acc.get_int(sys.TableTab.COL_SCHEMA_ID))
            name = sys.read_string(acc, sys.TableTab.COL_NAME)
            pk_col_idx = intmask(acc.get_int(sys.TableTab.COL_PK_COL_IDX))
            flags = intmask(acc.get_int(sys.TableTab.COL_FLAGS))
            unique_pk = (flags & sys.TableTab.FLAG_UNIQUE_PK) != 0

            if weight > 0:
                if self.engine.registry.has_id(tid):
                    continue

                col_defs = read_column_defs(self.engine.sys.columns, tid)
                if len(col_defs) == 0:
                    os.write(
                        2,
                        " [ERROR] Cannot register table '%s': columns not found\n"
                        % name,
                    )
                    continue

                schema_name = self.engine.registry.get_schema_name(sid)
                directory = (
                    self.engine.base_dir + "/" + schema_name + "/" + name + "_" + str(tid)
                )

                tbl_schema = TableSchema(col_defs, pk_col_idx)
                n = get_num_partitions(tid)
                pt = make_partitioned_persistent(
                    directory, name, tbl_schema, tid, n,
                    part_start=self.engine.registry.active_part_start,
                    part_end=self.engine.registry.active_part_end,
                )

                mmap_posix.fsync_dir(self.engine.base_dir + "/" + schema_name)

                family = TableFamily(
                    schema_name, name, tid, sid, directory, pk_col_idx, pt,
                    unique_pk=unique_pk,
                )
                self.engine.registry.register(family)
                wire_fk_constraints_for_family(family, self.engine.registry)
                # Register with DagEngine
                packed = engine_ffi.pack_schema(family.schema)
                with rffi.scoped_str2charp(directory) as dir_c:
                    engine_ffi._dag_register_table(
                        self.engine.dag_handle, tid,
                        family.store._handle,
                        rffi.cast(rffi.VOIDP, packed),
                        family.depth, int(unique_pk), 1, dir_c)
                lltype.free(packed, flavor="raw")

                if self.cascade_enabled:
                    self._create_fk_indices(family)
            else:
                if self.engine.registry.has_id(tid):
                    family = self.engine.registry.get_by_id(tid)
                    for referencing in self.engine.registry.iter_families():
                        if referencing.table_id == tid:
                            continue
                        for col in referencing.schema.columns:
                            if col.fk_table_id == tid:
                                raise LayoutError(
                                    "Integrity violation: table referenced by '%s'"
                                    % (
                                        referencing.schema_name
                                        + "."
                                        + referencing.table_name
                                    )
                                )

                    engine_ffi._dag_unregister_table(self.engine.dag_handle, tid)
                    family.close()
                    self.engine.registry.unregister(family.schema_name, family.table_name)

    def _create_fk_indices(self, family):
        columns = family.schema.columns
        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if col.fk_table_id == 0 or col_idx == family.schema.pk_index:
                continue
            index_name = make_fk_index_name(
                family.schema_name, family.table_name, col.name
            )
            if self.engine.registry.has_index_by_name(index_name):
                continue
            index_id = self.engine.registry.allocate_index_id()
            s = self.engine.sys.indices.schema
            idx_batch = ArenaZSetBatch(s)
            sys.IdxTab.append(
                idx_batch, s, index_id, family.table_id,
                sys.OWNER_KIND_TABLE, col_idx, index_name, 0, ""
            )
            indices_family = self.engine.registry.get("_system", sys.IdxTab.NAME)
            ingest_to_family(indices_family, idx_batch)
            idx_batch.free()
            self._advance_sequence(sys.SEQ_ID_INDICES, index_id - 1, index_id)

    def _advance_sequence(self, seq_id, old_val, new_val):
        s = self.engine.sys.sequences.schema
        batch = ArenaZSetBatch(s)
        sys.SeqTab.retract(batch, s, seq_id, old_val)
        sys.SeqTab.append(batch, s, seq_id, new_val)
        self.engine.sys.sequences.ingest_batch(batch)
        batch.free()


class ViewEffectHook(DeltaHook):
    """Reacts to additions or removals of reactive views."""

    _immutable_fields_ = ["engine"]

    def __init__(self, engine):
        self.engine = engine

    def on_delta(self, batch):
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            vid = intmask(r_uint64(batch.get_pk(i)))
            batch.bind_accessor(i, acc)

            sid = intmask(acc.get_int(sys.ViewTab.COL_SCHEMA_ID))
            name = sys.read_string(acc, sys.ViewTab.COL_NAME)

            if weight > 0:
                if self.engine.registry.has_id(vid):
                    continue

                col_defs = read_column_defs(self.engine.sys.columns, vid)
                if len(col_defs) == 0:
                    os.write(
                        2,
                        " [ERROR] Cannot register view '%s': columns not found\n"
                        % name,
                    )
                    continue

                schema_name = self.engine.registry.get_schema_name(sid)
                directory = (
                    self.engine.base_dir + "/" + schema_name + "/view_" + name + "_" + str(vid)
                )

                tbl_schema = TableSchema(col_defs, pk_index=0)
                n = get_num_partitions(vid)
                et = make_partitioned_ephemeral(
                    directory, name, tbl_schema, vid, n,
                    part_start=self.engine.registry.active_part_start,
                    part_end=self.engine.registry.active_part_end,
                )

                family = TableFamily(schema_name, name, vid, sid, directory, 0, et)

                max_depth = 0
                out_buf = lltype.malloc(rffi.LONGLONGP.TO, 64, flavor="raw")
                try:
                    n_src = engine_ffi._dag_get_source_ids(
                        self.engine.dag_handle, vid, out_buf, 64)
                    for si in range(intmask(n_src)):
                        src_id = intmask(out_buf[si])
                        if self.engine.registry.has_id(src_id):
                            d = self.engine.registry.get_by_id(src_id).depth + 1
                            if d > max_depth:
                                max_depth = d
                finally:
                    lltype.free(out_buf, flavor="raw")
                family.depth = max_depth

                self.engine.registry.register(family)
                # Register with DagEngine
                packed = engine_ffi.pack_schema(family.schema)
                with rffi.scoped_str2charp(directory) as dir_c:
                    engine_ffi._dag_register_table(
                        self.engine.dag_handle, vid,
                        family.store._handle,
                        rffi.cast(rffi.VOIDP, packed),
                        family.depth, 0, 1, dir_c)
                lltype.free(packed, flavor="raw")

                # Only compile and backfill on processes that own data partitions.
                if self.engine.registry.active_part_start != self.engine.registry.active_part_end:
                    if engine_ffi._dag_ensure_compiled(self.engine.dag_handle, vid):
                        if not _view_needs_exchange_dag(self.engine, vid):
                            _backfill_view(self.engine, vid)
            else:
                if self.engine.registry.has_id(vid):
                    family = self.engine.registry.get_by_id(vid)
                    engine_ffi._dag_unregister_table(self.engine.dag_handle, vid)
                    family.close()
                    self.engine.registry.unregister(family.schema_name, family.table_name)


class DepTabEffectHook(DeltaHook):
    """Invalidates the cached dep_map whenever DepTab is modified."""

    _immutable_fields_ = ["engine"]

    def __init__(self, engine):
        self.engine = engine

    def on_delta(self, batch):
        engine_ffi._dag_invalidate_dep_map(self.engine.dag_handle)


class IndexEffectHook(DeltaHook):
    """Reacts to additions or removals of secondary index circuits."""

    _immutable_fields_ = ["engine"]

    def __init__(self, engine):
        self.engine = engine

    def on_delta(self, batch):
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            idx_id = intmask(r_uint64(batch.get_pk(i)))
            batch.bind_accessor(i, acc)

            owner_id = intmask(acc.get_int(sys.IdxTab.COL_OWNER_ID))
            source_col_idx = intmask(acc.get_int(sys.IdxTab.COL_SOURCE_COL_IDX))
            name = sys.read_string(acc, sys.IdxTab.COL_NAME)
            is_unique = acc.get_int(sys.IdxTab.COL_IS_UNIQUE) != 0
            cache_dir = sys.read_string(acc, sys.IdxTab.COL_CACHE_DIRECTORY)

            if weight > 0:
                if self.engine.registry.has_index_by_name(name):
                    continue

                family = self.engine.registry.get_by_id(owner_id)
                col = family.schema.columns[source_col_idx]
                source_pk_type = family.schema.get_pk_column().field_type
                idx_dir = family.directory + "/idx_" + str(idx_id)

                circuit = _make_index_circuit(
                    idx_id,
                    owner_id,
                    source_col_idx,
                    col.field_type,
                    source_pk_type,
                    idx_dir,
                    name,
                    is_unique,
                    cache_dir,
                )

                _backfill_index(circuit, family)
                family.index_circuits.append(circuit)
                self.engine.registry.register_index(idx_id, name, circuit)
            else:
                if self.engine.registry.has_index_by_name(name):
                    circuit = self.engine.registry.get_index_by_name(name)
                    if "__fk_" in name:
                        raise LayoutError("Forbidden: cannot drop internal FK index")

                    family = self.engine.registry.get_by_id(circuit.owner_id)
                    _remove_circuit_from_family(circuit, family)
                    self.engine.registry.unregister_index(name, idx_id)
                    circuit.close()


def wire_catalog_hooks(engine):
    """
    Attaches effect hooks to system table families.
    Must be called after recover_system_state (families exist)
    and before replay_catalog (hooks must be in place for replay).
    """
    schemas_family = engine.registry.get("_system", sys.SchemaTab.NAME)
    schemas_family.post_ingest_hooks.append(SchemaEffectHook(engine))

    tables_family = engine.registry.get("_system", sys.TableTab.NAME)
    table_hook = TableEffectHook(engine)
    tables_family.post_ingest_hooks.append(table_hook)

    views_family = engine.registry.get("_system", sys.ViewTab.NAME)
    views_family.post_ingest_hooks.append(ViewEffectHook(engine))

    indices_family = engine.registry.get("_system", sys.IdxTab.NAME)
    indices_family.post_ingest_hooks.append(IndexEffectHook(engine))

    deps_family = engine.registry.get("_system", sys.DepTab.NAME)
    deps_family.post_ingest_hooks.append(DepTabEffectHook(engine))

    return table_hook
