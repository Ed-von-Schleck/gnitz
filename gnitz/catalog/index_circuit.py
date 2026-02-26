# gnitz/catalog/index_circuit.py

from rpython.rlib.rarithmetic import (
    r_uint64,
    r_int64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi

from gnitz.core.types import (
    TYPE_U8,
    TYPE_I8,
    TYPE_U16,
    TYPE_I16,
    TYPE_U32,
    TYPE_I32,
    TYPE_U64,
    TYPE_I64,
    TYPE_F32,
    TYPE_F64,
    TYPE_STRING,
    TYPE_U128,
    ColumnDefinition,
    TableSchema,
)
from gnitz.core.errors import LayoutError, MemTableFullError
from gnitz.core.batch import ZSetBatch
from gnitz.core.values import make_payload_row
from gnitz.storage.ephemeral_table import EphemeralTable


def get_index_key_type(field_type):
    """
    Returns the index PK type for a source column type, or raises LayoutError.
    Numeric types are promoted to U64 or U128.
    """
    code = field_type.code
    if code == TYPE_U128.code:
        return TYPE_U128
    if code == TYPE_U64.code:
        return TYPE_U64

    # Using explicit 'or' comparisons to avoid RPython TyperError: contains() on non-const tuple
    if (
        code == TYPE_I64.code
        or code == TYPE_U32.code
        or code == TYPE_I32.code
        or code == TYPE_U16.code
        or code == TYPE_I16.code
        or code == TYPE_U8.code
        or code == TYPE_I8.code
    ):
        return TYPE_U64

    if code == TYPE_F32.code or code == TYPE_F64.code or code == TYPE_STRING.code:
        raise LayoutError(
            "Secondary index on column type %d not supported in Phase B" % code
        )
    raise LayoutError("Unknown column type code: %d" % code)


def make_index_schema(index_key_type, source_pk_type):
    """
    Creates a Z-Set schema for the index: index_key (PK) -> source_pk (Payload).
    """
    cols = newlist_hint(2)
    cols.append(ColumnDefinition(index_key_type, is_nullable=False, name="index_key"))
    cols.append(ColumnDefinition(source_pk_type, is_nullable=False, name="source_pk"))
    return TableSchema(cols, pk_index=0)


def promote_to_index_key(accessor, col_idx, source_col_type):
    """
    Extracts a value from the source table and promotes it to a uniform
    r_uint128 key suitable for the index. Signed types are bit-reinterpreted.
    """
    code = source_col_type.code
    if code == TYPE_U128.code:
        return accessor.get_u128(col_idx)
    if code == TYPE_U64.code:
        return r_uint128(accessor.get_int(col_idx))

    if code == TYPE_U32.code or code == TYPE_U16.code or code == TYPE_U8.code:
        return r_uint128(accessor.get_int(col_idx))  # zero-extend

    if code == TYPE_I64.code:
        # get_int returns r_uint64 — same bit pattern, zero-extended to r_uint128
        return r_uint128(accessor.get_int(col_idx))

    if code == TYPE_I32.code or code == TYPE_I16.code or code == TYPE_I8.code:
        # Sign-extend to 64 bits, then interpret as unsigned
        signed_64 = accessor.get_int_signed(col_idx)  # r_int64
        return r_uint128(rffi.cast(rffi.ULONGLONG, signed_64))

    raise LayoutError("Cannot promote column type %d to index key" % code)
    
    
def make_fk_index_name(schema_name, table_name, col_name):
    """
    Computes the auto-generated name for a Foreign Key index.
    Convention: schema__table__fk_column
    """
    return schema_name + "__" + table_name + "__fk_" + col_name


def make_secondary_index_name(schema_name, table_name, col_name):
    """
    Computes the auto-generated name for a standard secondary index.
    Convention: schema__table__idx_column
    """
    return schema_name + "__" + table_name + "__idx_" + col_name


class IndexCircuit(object):
    """
    A soft-state component that transforms deltas from a source table
    into deltas for a secondary index Z-Set.
    """

    _immutable_fields_ = [
        "index_id",
        "owner_id",
        "source_col_idx",
        "source_col_type",
        "index_key_type",
        "source_pk_type",
        "name",
        "is_unique",
        "cache_dir",
        "table",
    ]

    def __init__(
        self,
        index_id,
        owner_id,
        source_col_idx,
        source_col_type,
        index_key_type,
        source_pk_type,
        name,
        is_unique,
        cache_dir,
        table,
    ):
        self.index_id = index_id  # int
        self.owner_id = owner_id  # int (table_id of source table)
        self.source_col_idx = source_col_idx  # int
        self.source_col_type = source_col_type  # FieldType
        self.index_key_type = index_key_type  # FieldType: TYPE_U64 or TYPE_U128
        self.source_pk_type = source_pk_type  # FieldType: TYPE_U64 or TYPE_U128
        self.name = name  # str
        self.is_unique = is_unique  # bool
        self.cache_dir = cache_dir  # str
        self.table = table  # EphemeralTable (the index store)

    def compute_index_delta(self, source_batch):
        """
        Transforms a source Z-Set batch into an index Z-Set batch.
        Records where the indexed column is NULL are skipped.
        """
        idx_schema = self.table.schema
        idx_batch = ZSetBatch(idx_schema)
        n = source_batch.length()
        for i in range(n):
            acc = source_batch.get_accessor(i)
            if acc.is_null(self.source_col_idx):
                continue

            source_pk = source_batch.get_pk(i)  # r_uint128
            weight = source_batch.get_weight(i)  # r_int64
            index_key = promote_to_index_key(
                acc, self.source_col_idx, self.source_col_type
            )

            row = make_payload_row(idx_schema)
            if self.source_pk_type.code == TYPE_U128.code:
                row.append_u128(r_uint64(source_pk), r_uint64(source_pk >> 64))
            else:
                row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(source_pk)))

            idx_batch.append(index_key, weight, row)
        return idx_batch

    def create_cursor(self):
        return self.table.create_cursor()

    def close(self):
        self.table.close()


# ── Circuit Factory and Lifecycle ────────────────────────────────────────────


def _make_index_circuit(
    index_id,
    owner_id,
    source_col_idx,
    source_col_type,
    source_pk_type,
    index_dir,
    index_name,
    is_unique,
    cache_dir,
):
    """
    Creates the directory and EphemeralTable for an index and returns the circuit.
    """
    index_key_type = get_index_key_type(source_col_type)
    idx_schema = make_index_schema(index_key_type, source_pk_type)
    idx_table = EphemeralTable(
        index_dir, "_idx_" + str(index_id), idx_schema, table_id=index_id
    )
    return IndexCircuit(
        index_id,
        owner_id,
        source_col_idx,
        source_col_type,
        index_key_type,
        source_pk_type,
        index_name,
        is_unique,
        cache_dir,
        idx_table,
    )


BACKFILL_BATCH_SIZE = 1000


def _backfill_index(circuit, source_family):
    """
    Performs the initial population of a secondary index by scanning the
    source table. Handles MemTableFullError by flushing ephemeral shards.
    """
    idx_schema = circuit.table.schema
    src_cursor = source_family.primary.create_cursor()
    batch = ZSetBatch(idx_schema)

    while src_cursor.is_valid():
        acc = src_cursor.get_accessor()
        weight = src_cursor.weight()

        # Ghost Property: UnifiedCursor already filters zeros, but check again.
        if weight != 0 and not acc.is_null(circuit.source_col_idx):
            source_pk = src_cursor.key()
            index_key = promote_to_index_key(
                acc, circuit.source_col_idx, circuit.source_col_type
            )

            row = make_payload_row(idx_schema)
            if circuit.source_pk_type.code == TYPE_U128.code:
                row.append_u128(r_uint64(source_pk), r_uint64(source_pk >> 64))
            else:
                row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(source_pk)))

            batch.append(index_key, weight, row)

            if batch.length() >= BACKFILL_BATCH_SIZE:
                try:
                    circuit.table.ingest_batch(batch)
                except MemTableFullError:
                    circuit.table.flush()
                    circuit.table.ingest_batch(batch)
                batch.free()
                batch = ZSetBatch(idx_schema)

        src_cursor.advance()

    if not batch.is_empty():
        try:
            circuit.table.ingest_batch(batch)
        except MemTableFullError:
            circuit.table.flush()
            circuit.table.ingest_batch(batch)

    batch.free()
    src_cursor.close()
