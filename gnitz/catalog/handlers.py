# gnitz/catalog/handlers.py

import os
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.errors import LayoutError
from gnitz.core.types import TableSchema
from gnitz.storage import mmap_posix
from gnitz.storage.table import PersistentTable
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.catalog import system_tables as sys
from gnitz.catalog.metadata import ensure_dir, read_column_defs
from gnitz.catalog.registry import (
    TableFamily,
    wire_fk_constraints_for_family,
)
from gnitz.catalog.index_circuit import (
    _make_index_circuit,
    _backfill_index,
)


def _compute_view_depth(vid, view_deps_store, registry):
    """
    Traverses the view dependency graph to determine the topological rank.
    view_deps_store is a ZSetStore (PersistentTable).
    """
    max_depth = 0
    cursor = view_deps_store.create_cursor()
    try:
        while cursor.is_valid():
            if cursor.weight() > 0:
                acc = cursor.get_accessor()
                v_id = intmask(acc.get_int(sys.DepTab.COL_VIEW_ID))
                if v_id == vid:
                    dep_vid = intmask(acc.get_int(sys.DepTab.COL_DEP_VIEW_ID))
                    if dep_vid > 0 and registry.has_id(dep_vid):
                        candidate = registry.get_by_id(dep_vid).depth + 1
                        if candidate > max_depth:
                            max_depth = candidate
            cursor.advance()
    finally:
        cursor.close()
    return max_depth


class CatalogHandlers(object):
    """
    The reactive 'Operator' for GnitzDB catalog state.

    Translates durable Z-Set deltas into in-memory EntityRegistry updates
    and physical side-effects (directory creation, index backfilling).
    """

    _immutable_fields_ = ["registry", "sys_tables", "base_dir", "program_cache"]

    def __init__(self, registry, sys_tables, base_dir, program_cache):
        self.registry = registry
        self.sys_tables = sys_tables
        self.base_dir = base_dir
        self.program_cache = program_cache

    def on_schema_delta(self, batch):
        """Reacts to additions or removals of database schemas."""
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            batch.bind_raw_accessor(i, acc)

            sid = intmask(r_uint64(batch.get_pk(i)))
            name = sys.read_string(acc, sys.SchemaTab.COL_NAME)

            if weight > 0:
                if self.registry.has_schema(name):
                    continue

                path = self.base_dir + "/" + name
                ensure_dir(path)
                mmap_posix.fsync_dir(self.base_dir)

                self.registry.register_schema(sid, name)
            else:
                if self.registry.has_schema(name):
                    if name == "_system":
                        raise LayoutError("Forbidden: cannot drop system schema")
                    if not self.registry.schema_is_empty(name):
                        raise LayoutError("Cannot drop non-empty schema: %s" % name)
                    self.registry.unregister_schema(name, sid)

    def on_table_delta(self, batch):
        """Reacts to additions or removals of persistent user tables."""
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            tid = intmask(r_uint64(batch.get_pk(i)))
            batch.bind_raw_accessor(i, acc)

            sid = intmask(acc.get_int(sys.TableTab.COL_SCHEMA_ID))
            name = sys.read_string(acc, sys.TableTab.COL_NAME)
            directory = sys.read_string(acc, sys.TableTab.COL_DIRECTORY)
            pk_col_idx = intmask(acc.get_int(sys.TableTab.COL_PK_COL_IDX))

            if weight > 0:
                if self.registry.has_id(tid):
                    continue

                col_defs = read_column_defs(self.sys_tables.columns, tid)
                if len(col_defs) == 0:
                    os.write(
                        1,
                        " [ERROR] Cannot register table '%s': columns not found\n"
                        % name,
                    )
                    continue

                tbl_schema = TableSchema(col_defs, pk_col_idx)
                pt = PersistentTable(directory, name, tbl_schema, table_id=tid)

                schema_name = self.registry.get_schema_name(sid)
                mmap_posix.fsync_dir(self.base_dir + "/" + schema_name)

                # positional arg 'pt' becomes 'family.store'
                family = TableFamily(
                    schema_name, name, tid, sid, directory, pk_col_idx, pt
                )
                self.registry.register(family)
                wire_fk_constraints_for_family(family, self.registry)
            else:
                if self.registry.has_id(tid):
                    family = self.registry.get_by_id(tid)
                    for referencing in self.registry.iter_families():
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

                    family.close()
                    self.registry.unregister(family.schema_name, family.table_name)

    def on_view_delta(self, batch):
        """Reacts to additions or removals of reactive views."""
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            vid = intmask(r_uint64(batch.get_pk(i)))
            batch.bind_raw_accessor(i, acc)

            sid = intmask(acc.get_int(sys.ViewTab.COL_SCHEMA_ID))
            name = sys.read_string(acc, sys.ViewTab.COL_NAME)
            directory = sys.read_string(acc, sys.ViewTab.COL_CACHE_DIRECTORY)

            if weight > 0:
                if self.registry.has_id(vid):
                    continue

                col_defs = read_column_defs(self.sys_tables.columns, vid)
                if len(col_defs) == 0:
                    os.write(
                        1,
                        " [ERROR] Cannot register view '%s': columns not found\n"
                        % name,
                    )
                    continue

                tbl_schema = TableSchema(col_defs, pk_index=0)
                et = EphemeralTable(directory, name, tbl_schema, table_id=vid)
                schema_name = self.registry.get_schema_name(sid)

                # positional arg 'et' becomes 'family.store'
                family = TableFamily(schema_name, name, vid, sid, directory, 0, et)

                family.depth = _compute_view_depth(
                    vid, self.sys_tables.view_deps, self.registry
                )

                self.registry.register(family)
            else:
                if self.registry.has_id(vid):
                    family = self.registry.get_by_id(vid)
                    family.close()
                    self.registry.unregister(family.schema_name, family.table_name)

    def on_index_delta(self, batch):
        """Reacts to additions or removals of secondary index circuits."""
        acc = batch.get_accessor(0)
        for i in range(batch.length()):
            weight = batch.get_weight(i)
            idx_id = intmask(r_uint64(batch.get_pk(i)))
            batch.bind_raw_accessor(i, acc)

            owner_id = intmask(acc.get_int(sys.IdxTab.COL_OWNER_ID))
            source_col_idx = intmask(acc.get_int(sys.IdxTab.COL_SOURCE_COL_IDX))
            name = sys.read_string(acc, sys.IdxTab.COL_NAME)
            is_unique = acc.get_int(sys.IdxTab.COL_IS_UNIQUE) != 0
            cache_dir = sys.read_string(acc, sys.IdxTab.COL_CACHE_DIRECTORY)

            if weight > 0:
                if self.registry.has_index_by_name(name):
                    continue

                family = self.registry.get_by_id(owner_id)
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
                self.registry.register_index(idx_id, name, circuit)
            else:
                if self.registry.has_index_by_name(name):
                    circuit = self.registry.get_index_by_name(name)
                    if "__fk_" in name:
                        raise LayoutError("Forbidden: cannot drop internal FK index")

                    family = self.registry.get_by_id(circuit.owner_id)
                    _remove_circuit_from_family(circuit, family)
                    self.registry.unregister_index(name, idx_id)
                    circuit.close()


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
