# gnitz/catalog/index_circuit.py

from rpython.rlib.rarithmetic import (
    r_uint64,
    r_int64,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

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
from gnitz.core.errors import LayoutError
from gnitz.core import comparator as core_comparator
from gnitz.core.keys import promote_to_index_key
from gnitz.storage.ephemeral_table import EphemeralTable


class IndexPayloadAccessor(core_comparator.RowAccessor):
    """
    Internal accessor for index projection.
    Mocks a row where the single payload column is a Source PK.
    Stores the PK in a small raw buffer so get_col_ptr can return a pointer.
    """

    def __init__(self):
        self.pk_lo = r_uint64(0)
        self.pk_hi = r_uint64(0)
        # 16-byte buffer: room for u128 (or u64 in first 8 bytes)
        self._buf = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")

    def set_pk(self, pk_lo, pk_hi):
        self.pk_lo = pk_lo
        self.pk_hi = pk_hi
        rffi.cast(rffi.ULONGLONGP, self._buf)[0] = rffi.cast(
            rffi.ULONGLONG, pk_lo)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self._buf, 8))[0] = rffi.cast(
            rffi.ULONGLONG, pk_hi)

    def is_null(self, col_idx):
        return False

    def get_int(self, col_idx):
        return self.pk_lo

    def get_int_signed(self, col_idx):
        return rffi.cast(rffi.LONGLONG, self.pk_lo)

    def get_u128_lo(self, col_idx):
        return self.pk_lo

    def get_u128_hi(self, col_idx):
        return self.pk_hi

    def get_float(self, col_idx):
        return 0.0

    def get_str_struct(self, col_idx):
        return (0, r_int64(0), lltype.nullptr(rffi.CCHARP.TO), lltype.nullptr(rffi.CCHARP.TO), "")

    def get_col_ptr(self, col_idx):
        return self._buf


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


def make_fk_index_name(schema_name, table_name, col_name):
    """Convention: schema__table__fk_column"""
    return schema_name + "__" + table_name + "__fk_" + col_name


def make_secondary_index_name(schema_name, table_name, col_name):
    """Convention: schema__table__idx_column"""
    return schema_name + "__" + table_name + "__idx_" + col_name


class IndexCircuit(object):
    """
    A descriptor and state-holder for a secondary index.
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
        "_index_payload_accessor",
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
        self.index_id = index_id
        self.owner_id = owner_id
        self.source_col_idx = source_col_idx
        self.source_col_type = source_col_type
        self.index_key_type = index_key_type
        self.source_pk_type = source_pk_type
        self.name = name
        self.is_unique = is_unique
        self.cache_dir = cache_dir
        self.table = table  # EphemeralTable
        self._index_payload_accessor = IndexPayloadAccessor()

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
        name=index_name,
        is_unique=is_unique,
        cache_dir=cache_dir,
        table=idx_table,
    )


def _backfill_index(circuit, source_family):
    """
    Initial population of a secondary index by scanning the source table.
    Uses the ZSetStore interface and the optimized ingest_one kernel.
    """
    src_cursor = source_family.store.create_cursor()

    while src_cursor.is_valid():
        acc = src_cursor.get_accessor()
        weight = src_cursor.weight()

        if weight != r_int64(0) and not acc.is_null(circuit.source_col_idx):
            source_pk_lo = src_cursor.key_lo()
            source_pk_hi = src_cursor.key_hi()
            index_key = promote_to_index_key(
                acc, circuit.source_col_idx, circuit.source_col_type
            )
            index_key_lo = r_uint64(intmask(index_key))
            index_key_hi = r_uint64(intmask(index_key >> 64))

            if circuit.is_unique and circuit.table.has_pk(index_key_lo, index_key_hi):
                src_cursor.close()
                raise LayoutError(
                    "Unique index violation for '%s' during backfill" % circuit.name
                )

            # Set PK components for the alignment-safe accessor
            acc_inj = circuit._index_payload_accessor
            acc_inj.set_pk(source_pk_lo, source_pk_hi)

            # EphemeralTable.ingest_one handles MemTableFullError internally.
            circuit.table.ingest_one(index_key_lo, index_key_hi, weight, acc_inj)

        src_cursor.advance()

    src_cursor.close()
