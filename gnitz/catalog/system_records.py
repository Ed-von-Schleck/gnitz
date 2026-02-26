# gnitz/catalog/system_records.py

from rpython.rlib.rarithmetic import r_uint64, r_int64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core.values import make_payload_row
from gnitz.core import strings as string_logic
from gnitz.catalog.system_tables import pack_column_id, OWNER_KIND_TABLE


def _read_string(accessor, col_idx):
    """
    Extracts a Python str from a RowAccessor. Handles both MemTable and Shards.
    """
    length, prefix, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(col_idx)
    if py_string is not None:
        return py_string
    null_ptr = lltype.nullptr(rffi.CCHARP.TO)
    if struct_ptr == null_ptr:
        return ""
    return string_logic.unpack_string(struct_ptr, heap_ptr)


# ── Schema Records ───────────────────────────────────────────────────────────


def _append_schema_record(batch, schema, schema_id, name):
    row = make_payload_row(schema)
    row.append_string(name)
    batch.append(r_uint128(r_uint64(schema_id)), r_int64(1), row)


def _retract_schema_record(batch, schema, schema_id, name):
    row = make_payload_row(schema)
    row.append_string(name)
    batch.append(r_uint128(r_uint64(schema_id)), r_int64(-1), row)


# ── Table Records ────────────────────────────────────────────────────────────


def _append_table_record(
    batch, schema, table_id, schema_id, name, directory, pk_col_idx, created_lsn
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(schema_id)))
    row.append_string(name)
    row.append_string(directory)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(pk_col_idx)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(created_lsn)))
    batch.append(r_uint128(r_uint64(table_id)), r_int64(1), row)


def _retract_table_record(
    batch, schema, table_id, schema_id, name, directory, pk_col_idx, created_lsn
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(schema_id)))
    row.append_string(name)
    row.append_string(directory)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(pk_col_idx)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(created_lsn)))
    batch.append(r_uint128(r_uint64(table_id)), r_int64(-1), row)


# ── Column Records ───────────────────────────────────────────────────────────


def _append_column_record(
    batch,
    schema,
    owner_id,
    owner_kind,
    col_idx,
    name,
    type_code,
    is_nullable,
    fk_table_id,
    fk_col_idx,
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_kind)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(col_idx)))
    row.append_string(name)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(type_code)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(is_nullable)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_table_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_col_idx)))
    pk_val = pack_column_id(owner_id, col_idx)
    batch.append(r_uint128(pk_val), r_int64(1), row)


def _retract_column_record(
    batch,
    schema,
    owner_id,
    owner_kind,
    col_idx,
    name,
    type_code,
    is_nullable,
    fk_table_id,
    fk_col_idx,
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_kind)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(col_idx)))
    row.append_string(name)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(type_code)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(is_nullable)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_table_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_col_idx)))
    pk = pack_column_id(owner_id, col_idx)
    batch.append(r_uint128(pk), r_int64(-1), row)


def _append_system_cols(batch, schema, table_id, column_defs):
    """
    Helper for bootstrap to append multiple columns for a system table.
    """
    for i in range(len(column_defs)):
        col_name, field_type = column_defs[i]
        _append_column_record(
            batch,
            schema,
            table_id,
            OWNER_KIND_TABLE,
            i,
            col_name,
            field_type.code,
            0,
            0,
            0,
        )


# ── Index Records ────────────────────────────────────────────────────────────


def _append_index_record(
    batch,
    schema,
    index_id,
    owner_id,
    owner_kind,
    source_col_idx,
    name,
    is_unique,
    cache_directory,
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_kind)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(source_col_idx)))
    row.append_string(name)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(is_unique)))
    row.append_string(cache_directory)
    batch.append(r_uint128(r_uint64(index_id)), r_int64(1), row)


def _retract_index_record(
    batch,
    schema,
    index_id,
    owner_id,
    owner_kind,
    source_col_idx,
    name,
    is_unique,
    cache_directory,
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_kind)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(source_col_idx)))
    row.append_string(name)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(is_unique)))
    row.append_string(cache_directory)
    batch.append(r_uint128(r_uint64(index_id)), r_int64(-1), row)


# ── Sequence Records ─────────────────────────────────────────────────────────


def _append_sequence_record(batch, schema, seq_id, val):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(val)))
    batch.append(r_uint128(r_uint64(seq_id)), r_int64(1), row)


def _retract_sequence_record(batch, schema, seq_id, val):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(val)))
    batch.append(r_uint128(r_uint64(seq_id)), r_int64(-1), row)
