# gnitz/catalog/loader.py

from rpython.rlib.rarithmetic import (
    r_uint64,
    r_int64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi

from gnitz.core.types import ColumnDefinition, TableSchema, TYPE_U64, TYPE_STRING
from gnitz.core.batch import ZSetBatch
from gnitz.core.values import make_payload_row
from gnitz.storage.table import PersistentTable

from gnitz.catalog.system_tables import (
    SYSTEM_SCHEMA_ID,
    PUBLIC_SCHEMA_ID,
    FIRST_USER_SCHEMA_ID,
    SYS_TABLE_SCHEMAS,
    SYS_TABLE_TABLES,
    SYS_TABLE_VIEWS,
    SYS_TABLE_COLUMNS,
    SYS_TABLE_INDICES,
    SYS_TABLE_VIEW_DEPS,
    SYS_TABLE_SEQUENCES,
    FIRST_USER_TABLE_ID,
    FIRST_USER_INDEX_ID,
    OWNER_KIND_TABLE,
    SEQ_ID_SCHEMAS,
    SEQ_ID_TABLES,
    SEQ_ID_INDICES,
    SYS_CATALOG_DIRNAME,
    SYS_SUBDIR_SCHEMAS,
    SYS_SUBDIR_TABLES,
    SYS_SUBDIR_VIEWS,
    SYS_SUBDIR_COLUMNS,
    SYS_SUBDIR_INDICES,
    SYS_SUBDIR_VIEW_DEPS,
    SYS_SUBDIR_SEQUENCES,
    make_schemas_schema,
    make_tables_schema,
    make_views_schema,
    make_columns_schema,
    make_indices_schema,
    make_view_deps_schema,
    make_sequences_schema,
    pack_column_id,
    type_code_to_field_type,
)
from gnitz.catalog.registry import (
    SystemTables,
    TableFamily,
    _wire_fk_constraints_for_family,
)
from gnitz.catalog.system_records import (
    _read_string,
    _append_schema_record,
    _append_table_record,
    _append_column_record,
    _append_system_cols,
    _append_sequence_record,
)
from gnitz.catalog.index_circuit import _make_index_circuit, _backfill_index


def _create_sys_table(sys_dir, subdir, name, schema, table_id):
    directory = sys_dir + "/" + subdir
    return PersistentTable(directory, name, schema, table_id=table_id)


def _make_system_tables(sys_dir):
    return SystemTables(
        schemas=_create_sys_table(
            sys_dir,
            SYS_SUBDIR_SCHEMAS,
            "_schemas",
            make_schemas_schema(),
            SYS_TABLE_SCHEMAS,
        ),
        tables=_create_sys_table(
            sys_dir,
            SYS_SUBDIR_TABLES,
            "_tables",
            make_tables_schema(),
            SYS_TABLE_TABLES,
        ),
        views=_create_sys_table(
            sys_dir, SYS_SUBDIR_VIEWS, "_views", make_views_schema(), SYS_TABLE_VIEWS
        ),
        columns=_create_sys_table(
            sys_dir,
            SYS_SUBDIR_COLUMNS,
            "_columns",
            make_columns_schema(),
            SYS_TABLE_COLUMNS,
        ),
        indices=_create_sys_table(
            sys_dir,
            SYS_SUBDIR_INDICES,
            "_indices",
            make_indices_schema(),
            SYS_TABLE_INDICES,
        ),
        view_deps=_create_sys_table(
            sys_dir,
            SYS_SUBDIR_VIEW_DEPS,
            "_view_deps",
            make_view_deps_schema(),
            SYS_TABLE_VIEW_DEPS,
        ),
        sequences=_create_sys_table(
            sys_dir,
            SYS_SUBDIR_SEQUENCES,
            "_sequences",
            make_sequences_schema(),
            SYS_TABLE_SEQUENCES,
        ),
    )


def _bootstrap_system_tables(sys_tables, base_dir):
    sys_dir = base_dir + "/" + SYS_CATALOG_DIRNAME

    schemas_schema = sys_tables.schemas.schema
    tables_schema = sys_tables.tables.schema
    cols_schema = sys_tables.columns.schema
    seq_schema = sys_tables.sequences.schema

    # 1. Schema records
    schemas_batch = ZSetBatch(schemas_schema)
    _append_schema_record(schemas_batch, schemas_schema, SYSTEM_SCHEMA_ID, "_system")
    _append_schema_record(schemas_batch, schemas_schema, PUBLIC_SCHEMA_ID, "public")
    sys_tables.schemas.ingest_batch(schemas_batch)
    schemas_batch.free()

    # 2. Table records
    tables_batch = ZSetBatch(tables_schema)
    table_configs = [
        (SYS_TABLE_SCHEMAS, "_schemas", SYS_SUBDIR_SCHEMAS),
        (SYS_TABLE_TABLES, "_tables", SYS_SUBDIR_TABLES),
        (SYS_TABLE_VIEWS, "_views", SYS_SUBDIR_VIEWS),
        (SYS_TABLE_COLUMNS, "_columns", SYS_SUBDIR_COLUMNS),
        (SYS_TABLE_INDICES, "_indices", SYS_SUBDIR_INDICES),
        (SYS_TABLE_VIEW_DEPS, "_view_deps", SYS_SUBDIR_VIEW_DEPS),
        (SYS_TABLE_SEQUENCES, "_sequences", SYS_SUBDIR_SEQUENCES),
    ]

    for tid, name, subdir in table_configs:
        _append_table_record(
            tables_batch,
            tables_schema,
            tid,
            SYSTEM_SCHEMA_ID,
            name,
            sys_dir + "/" + subdir,
            0,
            0,
        )
    sys_tables.tables.ingest_batch(tables_batch)
    tables_batch.free()

    # 3. Column records
    cols_batch = ZSetBatch(cols_schema)

    _append_system_cols(
        cols_batch,
        cols_schema,
        SYS_TABLE_SCHEMAS,
        [("schema_id", TYPE_U64), ("name", TYPE_STRING)],
    )
    _append_system_cols(
        cols_batch,
        cols_schema,
        SYS_TABLE_TABLES,
        [
            ("table_id", TYPE_U64),
            ("schema_id", TYPE_U64),
            ("name", TYPE_STRING),
            ("directory", TYPE_STRING),
            ("pk_col_idx", TYPE_U64),
            ("created_lsn", TYPE_U64),
        ],
    )
    _append_system_cols(
        cols_batch,
        cols_schema,
        SYS_TABLE_VIEWS,
        [
            ("view_id", TYPE_U64),
            ("schema_id", TYPE_U64),
            ("name", TYPE_STRING),
            ("sql_definition", TYPE_STRING),
            ("cache_directory", TYPE_STRING),
            ("created_lsn", TYPE_U64),
        ],
    )
    _append_system_cols(
        cols_batch,
        cols_schema,
        SYS_TABLE_COLUMNS,
        [
            ("column_id", TYPE_U64),
            ("owner_id", TYPE_U64),
            ("owner_kind", TYPE_U64),
            ("col_idx", TYPE_U64),
            ("name", TYPE_STRING),
            ("type_code", TYPE_U64),
            ("is_nullable", TYPE_U64),
            ("fk_table_id", TYPE_U64),
            ("fk_col_idx", TYPE_U64),
        ],
    )
    _append_system_cols(
        cols_batch,
        cols_schema,
        SYS_TABLE_INDICES,
        [
            ("index_id", TYPE_U64),
            ("owner_id", TYPE_U64),
            ("owner_kind", TYPE_U64),
            ("source_col_idx", TYPE_U64),
            ("name", TYPE_STRING),
            ("is_unique", TYPE_U64),
            ("cache_directory", TYPE_STRING),
        ],
    )
    _append_system_cols(
        cols_batch,
        cols_schema,
        SYS_TABLE_VIEW_DEPS,
        [
            ("dep_id", TYPE_U64),
            ("view_id", TYPE_U64),
            ("dep_view_id", TYPE_U64),
            ("dep_table_id", TYPE_U64),
        ],
    )
    _append_system_cols(
        cols_batch,
        cols_schema,
        SYS_TABLE_SEQUENCES,
        [("seq_id", TYPE_U64), ("next_val", TYPE_U64)],
    )

    sys_tables.columns.ingest_batch(cols_batch)
    cols_batch.free()

    # 4. Sequence high-water marks
    seq_batch = ZSetBatch(seq_schema)
    _append_sequence_record(
        seq_batch, seq_schema, SEQ_ID_SCHEMAS, FIRST_USER_SCHEMA_ID - 1
    )
    _append_sequence_record(
        seq_batch, seq_schema, SEQ_ID_TABLES, FIRST_USER_TABLE_ID - 1
    )
    _append_sequence_record(
        seq_batch, seq_schema, SEQ_ID_INDICES, FIRST_USER_INDEX_ID - 1
    )

    sys_tables.sequences.ingest_batch(seq_batch)
    seq_batch.free()

    # Flush all for hard-state durability
    sys_tables.schemas.flush()
    sys_tables.tables.flush()
    sys_tables.views.flush()
    sys_tables.columns.flush()
    sys_tables.indices.flush()
    sys_tables.view_deps.flush()
    sys_tables.sequences.flush()


def _read_column_defs(cols_pt, owner_id):
    cursor = cols_pt.create_cursor()
    start_key = r_uint128(pack_column_id(owner_id, 0))
    end_key = r_uint128(pack_column_id(owner_id + 1, 0))

    cursor.seek(start_key)

    col_defs = newlist_hint(8)
    while cursor.is_valid() and cursor.key() < end_key:
        acc = cursor.get_accessor()
        col_name = _read_string(acc, 4)
        type_code = intmask(acc.get_int(5))
        is_nullable_val = intmask(acc.get_int(6))
        # Recover Foreign Key metadata
        fk_table_id = intmask(acc.get_int(7))
        fk_col_idx = intmask(acc.get_int(8))

        field_type = type_code_to_field_type(type_code)
        is_nullable = is_nullable_val != 0
        col_defs.append(
            ColumnDefinition(
                field_type,
                is_nullable=is_nullable,
                name=col_name,
                fk_table_id=fk_table_id,
                fk_col_idx=fk_col_idx,
            )
        )
        cursor.advance()

    cursor.close()
    return col_defs


def _register_system_table(registry, pt, tbl_name, table_id, subdir, sys_dir):
    directory = sys_dir + "/" + subdir
    family = TableFamily(
        "_system", tbl_name, table_id, SYSTEM_SCHEMA_ID, directory, 0, pt
    )
    registry.register(family)


def _rebuild_registry(sys_tables, base_dir, registry):
    # 1. Schemas
    cursor = sys_tables.schemas.create_cursor()
    while cursor.is_valid():
        acc = cursor.get_accessor()
        sid = intmask(r_uint64(cursor.key()))
        name = _read_string(acc, 1)
        registry.register_schema(sid, name)
        cursor.advance()
    cursor.close()

    # 2. System Table Families
    sys_dir = base_dir + "/" + SYS_CATALOG_DIRNAME
    _register_system_table(
        registry,
        sys_tables.schemas,
        "_schemas",
        SYS_TABLE_SCHEMAS,
        SYS_SUBDIR_SCHEMAS,
        sys_dir,
    )
    _register_system_table(
        registry,
        sys_tables.tables,
        "_tables",
        SYS_TABLE_TABLES,
        SYS_SUBDIR_TABLES,
        sys_dir,
    )
    _register_system_table(
        registry, sys_tables.views, "_views", SYS_TABLE_VIEWS, SYS_SUBDIR_VIEWS, sys_dir
    )
    _register_system_table(
        registry,
        sys_tables.columns,
        "_columns",
        SYS_TABLE_COLUMNS,
        SYS_SUBDIR_COLUMNS,
        sys_dir,
    )
    _register_system_table(
        registry,
        sys_tables.indices,
        "_indices",
        SYS_TABLE_INDICES,
        SYS_SUBDIR_INDICES,
        sys_dir,
    )
    _register_system_table(
        registry,
        sys_tables.view_deps,
        "_view_deps",
        SYS_TABLE_VIEW_DEPS,
        SYS_SUBDIR_VIEW_DEPS,
        sys_dir,
    )
    _register_system_table(
        registry,
        sys_tables.sequences,
        "_sequences",
        SYS_TABLE_SEQUENCES,
        SYS_SUBDIR_SEQUENCES,
        sys_dir,
    )

    # 3. Sequences (Counters)
    cursor = sys_tables.sequences.create_cursor()
    while cursor.is_valid():
        acc = cursor.get_accessor()
        seq_id = intmask(r_uint64(cursor.key()))
        val = intmask(acc.get_int(1))
        if seq_id == SEQ_ID_SCHEMAS:
            registry._next_schema_id = val + 1
        elif seq_id == SEQ_ID_TABLES:
            registry._next_table_id = val + 1
        elif seq_id == SEQ_ID_INDICES:
            registry._next_index_id = val + 1
        cursor.advance()
    cursor.close()

    # Migration: Phase A databases lack SEQ_ID_INDICES — seed it now.
    seq2_cursor = sys_tables.sequences.create_cursor()
    found_indices_seq = False
    while seq2_cursor.is_valid():
        if intmask(r_uint64(seq2_cursor.key())) == SEQ_ID_INDICES:
            found_indices_seq = True
            break
        seq2_cursor.advance()
    seq2_cursor.close()

    if not found_indices_seq:
        seq_schema_m = sys_tables.sequences.schema
        mig_batch = ZSetBatch(seq_schema_m)
        _append_sequence_record(
            mig_batch, seq_schema_m, SEQ_ID_INDICES, FIRST_USER_INDEX_ID - 1
        )
        sys_tables.sequences.ingest_batch(mig_batch)
        mig_batch.free()
        registry._next_index_id = FIRST_USER_INDEX_ID

    # 4. User Tables
    cursor = sys_tables.tables.create_cursor()
    while cursor.is_valid():
        tid = intmask(r_uint64(cursor.key()))
        if tid >= FIRST_USER_TABLE_ID:
            acc = cursor.get_accessor()
            sid = intmask(acc.get_int(1))
            tbl_name = _read_string(acc, 2)
            directory = _read_string(acc, 3)
            pk_col_idx = intmask(acc.get_int(4))

            col_defs = _read_column_defs(sys_tables.columns, tid)
            tbl_schema = TableSchema(col_defs, pk_col_idx)
            pt = PersistentTable(directory, tbl_name, tbl_schema, table_id=tid)

            schema_name = registry.get_schema_name(sid)
            family = TableFamily(
                schema_name, tbl_name, tid, sid, directory, pk_col_idx, pt
            )
            registry.register(family)
        cursor.advance()
    cursor.close()

    # Pass 4b: Wire Foreign Key constraints now that all user families are registered.
    for k in registry._by_name:
        family = registry._by_name[k]
        if family.table_id >= FIRST_USER_TABLE_ID:
            _wire_fk_constraints_for_family(family, registry)

    # 5. User Table Indices (soft state rebuild from source)
    idx_cursor = sys_tables.indices.create_cursor()
    while idx_cursor.is_valid():
        idx_id = intmask(r_uint64(idx_cursor.key()))
        acc = idx_cursor.get_accessor()
        owner_id = intmask(acc.get_int(1))
        owner_kind = intmask(acc.get_int(2))
        source_col_idx = intmask(acc.get_int(3))
        idx_name = _read_string(acc, 4)
        is_unique = intmask(acc.get_int(5)) != 0
        cache_dir = _read_string(acc, 6)

        # Orphaned record check: skip if owner table was dropped but index record remains
        if owner_kind == OWNER_KIND_TABLE and registry.has_id(owner_id):
            family = registry.get_by_id(owner_id)
            if source_col_idx < len(family.schema.columns):
                col = family.schema.columns[source_col_idx]
                source_col_type = col.field_type
                source_pk_type = family.schema.get_pk_column().field_type
                idx_dir = family.directory + "/idx_" + str(idx_id)

                circuit = _make_index_circuit(
                    idx_id,
                    owner_id,
                    source_col_idx,
                    source_col_type,
                    source_pk_type,
                    idx_dir,
                    idx_name,
                    is_unique,
                    cache_dir,
                )
                _backfill_index(circuit, family)
                family.index_circuits.append(circuit)
                registry.register_index(idx_id, idx_name, circuit)

        idx_cursor.advance()
    idx_cursor.close()
