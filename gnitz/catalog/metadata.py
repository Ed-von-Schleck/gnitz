# gnitz/catalog/metadata.py

import errno
import os
from rpython.rlib import rposix, jit
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask, r_int64
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.jit import unrolling_iterable

from gnitz.core.types import ColumnDefinition, TYPE_U64, TYPE_STRING, TYPE_U128
from gnitz.core.errors import LayoutError
from gnitz.core.batch import ZSetBatch
from gnitz.storage.table import PersistentTable
from gnitz.catalog import system_tables as sys

# -----------------------------------------------------------------------------
# System Table Registry Container
# -----------------------------------------------------------------------------

class SystemTables(object):
    """
    Physical container for the core catalog tables. 
    Defined here to break the circular dependency with registry.py.
    """
    _immutable_fields_ = [
        "schemas", "tables", "views", "columns", "indices", 
        "view_deps", "sequences", "instructions", "functions", "subscriptions"
    ]

    def __init__(self, schemas, tables, views, columns, indices, view_deps, 
                 sequences, instructions, functions, subscriptions):
        self.schemas = schemas
        self.tables = tables
        self.views = views
        self.columns = columns
        self.indices = indices
        self.view_deps = view_deps
        self.sequences = sequences
        self.instructions = instructions
        self.functions = functions
        self.subscriptions = subscriptions

    def close(self):
        self.schemas.close()
        self.tables.close()
        self.views.close()
        self.columns.close()
        self.indices.close()
        self.view_deps.close()
        self.sequences.close()
        self.instructions.close()
        self.functions.close()
        self.subscriptions.close()


# -----------------------------------------------------------------------------
# Filesystem and Schema Recovery Helpers
# -----------------------------------------------------------------------------

@jit.dont_look_inside
def ensure_dir(path):
    """
    Ensures a directory exists, ignoring EEXIST. 
    Crucial for RPython posix compliance and bootstrap ordering.
    """
    try:
        rposix.mkdir(path, 0o755)
    except OSError as e:
        if e.errno != errno.EEXIST:
            # We raise here because parent-missing (ENOENT) should be 
            # caught by the bootstrap coordinator.
            raise e

def read_column_defs(cols_pt, owner_id):
    """
    Fetches and reconstructs ColumnDefinitions for a given table or view owner.
    """
    cursor = cols_pt.create_cursor()
    try:
        # Columns are indexed by (owner_id << 9 | col_idx)
        start_key = r_uint128(sys.pack_column_id(owner_id, 0))
        end_key = r_uint128(sys.pack_column_id(owner_id + 1, 0))
        cursor.seek(start_key)

        col_defs = newlist_hint(8)
        while cursor.is_valid() and cursor.key() < end_key:
            # Only include columns with positive net weight (Active)
            if cursor.weight() > 0:
                acc = cursor.get_accessor()
                col_defs.append(
                    ColumnDefinition(
                        sys.type_code_to_field_type(intmask(acc.get_int(sys.ColTab.COL_TYPE_CODE))),
                        is_nullable=intmask(acc.get_int(sys.ColTab.COL_IS_NULLABLE)) != 0,
                        name=sys.read_string(acc, sys.ColTab.COL_NAME),
                        fk_table_id=intmask(acc.get_int(sys.ColTab.COL_FK_TABLE_ID)),
                        fk_col_idx=intmask(acc.get_int(sys.ColTab.COL_FK_COL_IDX)),
                    )
                )
            cursor.advance()
            
        # PROBE: Log recovery result for diagnostics
        if len(col_defs) == 0:
            os.write(1, " [INFO] No columns found for owner_id %d\n" % owner_id)
        else:
            os.write(1, " [INFO] Recovered %d columns for owner_id %d\n" % (len(col_defs), owner_id))
            
        return col_defs
    finally:
        cursor.close()


# -----------------------------------------------------------------------------
# System Table Factories and Initial Injections
# -----------------------------------------------------------------------------

SYSTEM_TAB_LIST = unrolling_iterable([
    sys.SchemaTab, sys.TableTab, sys.ViewTab, sys.ColTab, sys.IdxTab,
    sys.DepTab, sys.SeqTab, sys.InstrTab, sys.FuncTab, sys.SubTab,
])

def _create_sys_table(sys_dir, tab_class):
    """Physical factory for persistent system tables."""
    directory = sys_dir + "/" + tab_class.SUBDIR
    # Note: PersistentTable constructor calls ensure_dir(directory)
    return PersistentTable(
        directory, tab_class.NAME, tab_class.schema(), table_id=tab_class.ID
    )

def make_system_tables(base_dir):
    """
    Coordinates the physical creation/opening of the system catalog.
    """
    sys_dir = base_dir + "/" + sys.SYS_CATALOG_DIRNAME
    return SystemTables(
        schemas=_create_sys_table(sys_dir, sys.SchemaTab),
        tables=_create_sys_table(sys_dir, sys.TableTab),
        views=_create_sys_table(sys_dir, sys.ViewTab),
        columns=_create_sys_table(sys_dir, sys.ColTab),
        indices=_create_sys_table(sys_dir, sys.IdxTab),
        view_deps=_create_sys_table(sys_dir, sys.DepTab),
        sequences=_create_sys_table(sys_dir, sys.SeqTab),
        instructions=_create_sys_table(sys_dir, sys.InstrTab),
        functions=_create_sys_table(sys_dir, sys.FuncTab),
        subscriptions=_create_sys_table(sys_dir, sys.SubTab),
    )

def bootstrap_system_tables(sys_tables, base_dir):
    """
    Initial injection of foundational records for a fresh database instance.
    """
    sys_dir = base_dir + "/" + sys.SYS_CATALOG_DIRNAME

    # 1. Core Schema records
    schemas_batch = ZSetBatch(sys_tables.schemas.schema)
    sys.SchemaTab.append(schemas_batch, sys_tables.schemas.schema, sys.SYSTEM_SCHEMA_ID, "_system")
    sys.SchemaTab.append(schemas_batch, sys_tables.schemas.schema, sys.PUBLIC_SCHEMA_ID, "public")
    sys_tables.schemas.ingest_batch(schemas_batch)
    schemas_batch.free()

    # 2. Table records (Self-registration)
    tables_batch = ZSetBatch(sys_tables.tables.schema)
    for t in SYSTEM_TAB_LIST:
        sys.TableTab.append(
            tables_batch, sys_tables.tables.schema, t.ID, sys.SYSTEM_SCHEMA_ID,
            t.NAME, sys_dir + "/" + t.SUBDIR, 0, 0
        )
    sys_tables.tables.ingest_batch(tables_batch)
    tables_batch.free()

    # 3. Column records (Logical Schemas for Catalog)
    cols_batch = ZSetBatch(sys_tables.columns.schema)
    
    sys.ColTab.append_system(cols_batch, sys_tables.columns.schema, sys.SchemaTab.ID,
        [("schema_id", TYPE_U64), ("name", TYPE_STRING)])
        
    sys.ColTab.append_system(cols_batch, sys_tables.columns.schema, sys.TableTab.ID,
        [("table_id", TYPE_U64), ("schema_id", TYPE_U64), ("name", TYPE_STRING),
         ("directory", TYPE_STRING), ("pk_col_idx", TYPE_U64), ("created_lsn", TYPE_U64)])
         
    sys.ColTab.append_system(cols_batch, sys_tables.columns.schema, sys.ColTab.ID,
        [("column_id", TYPE_U64), ("owner_id", TYPE_U64), ("owner_kind", TYPE_U64),
         ("col_idx", TYPE_U64), ("name", TYPE_STRING), ("type_code", TYPE_U64),
         ("is_nullable", TYPE_U64), ("fk_table_id", TYPE_U64), ("fk_col_idx", TYPE_U64)])

    sys.ColTab.append_system(cols_batch, sys_tables.columns.schema, sys.IdxTab.ID,
        [("index_id", TYPE_U64), ("owner_id", TYPE_U64), ("owner_kind", TYPE_U64),
         ("source_col_idx", TYPE_U64), ("name", TYPE_STRING), ("is_unique", TYPE_U64),
         ("cache_directory", TYPE_STRING)])

    sys.ColTab.append_system(cols_batch, sys_tables.columns.schema, sys.ViewTab.ID,
        [("view_id", TYPE_U64), ("schema_id", TYPE_U64), ("name", TYPE_STRING),
         ("sql_definition", TYPE_STRING), ("cache_directory", TYPE_STRING), ("created_lsn", TYPE_U64)])

    sys.ColTab.append_system(cols_batch, sys_tables.columns.schema, sys.SeqTab.ID,
        [("seq_id", TYPE_U64), ("next_val", TYPE_U64)])

    sys.ColTab.append_system(cols_batch, sys_tables.columns.schema, sys.InstrTab.ID,
        [("instr_pk", TYPE_U128), ("opcode", TYPE_U64), ("reg_in", TYPE_U64), ("reg_out", TYPE_U64),
         ("reg_in_a", TYPE_U64), ("reg_in_b", TYPE_U64), ("reg_trace", TYPE_U64),
         ("reg_trace_in", TYPE_U64), ("reg_trace_out", TYPE_U64), ("reg_delta", TYPE_U64),
         ("reg_history", TYPE_U64), ("reg_a", TYPE_U64), ("reg_b", TYPE_U64), ("reg_key", TYPE_U64),
         ("target_table_id", TYPE_U64), ("func_id", TYPE_U64), ("agg_func_id", TYPE_U64),
         ("group_by_cols", TYPE_STRING), ("chunk_limit", TYPE_U64), ("jump_target", TYPE_U64),
         ("yield_reason", TYPE_U64)])
         
    sys_tables.columns.ingest_batch(cols_batch)
    cols_batch.free()

    # 4. Sequence High-Water Marks
    seq_batch = ZSetBatch(sys_tables.sequences.schema)
    sys.SeqTab.append(seq_batch, sys_tables.sequences.schema, sys.SEQ_ID_SCHEMAS, sys.FIRST_USER_SCHEMA_ID - 1)
    sys.SeqTab.append(seq_batch, sys_tables.sequences.schema, sys.SEQ_ID_TABLES, sys.FIRST_USER_TABLE_ID - 1)
    sys.SeqTab.append(seq_batch, sys_tables.sequences.schema, sys.SEQ_ID_INDICES, sys.FIRST_USER_INDEX_ID - 1)
    sys.SeqTab.append(seq_batch, sys_tables.sequences.schema, sys.SEQ_ID_PROGRAMS, sys.FIRST_USER_TABLE_ID - 1)
    sys_tables.sequences.ingest_batch(seq_batch)
    seq_batch.free()

    # Flush all foundational metadata to disk
    sys_tables.schemas.flush()
    sys_tables.tables.flush()
    sys_tables.columns.flush()
    sys_tables.sequences.flush()
