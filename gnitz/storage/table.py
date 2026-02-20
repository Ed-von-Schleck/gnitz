# gnitz/storage/table.py
#
# PersistentTable: durability and indexing layer for a single GnitzDB table.
#
# Migration note (PayloadRow)
# ----------------------------
# ``PersistentTable.insert`` previously accepted ``List`` as its
# row parameter.  It now accepts ``PayloadRow`` directly.  There is no
# conversion layer; ``TaggedValue`` is removed from the codebase.

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rtyper.lltypesystem.llmemory import raw_malloc, raw_free, raw_memcopy

from gnitz.core import types
from gnitz.core.values import PayloadRow
from gnitz.storage import wal_format


# ---------------------------------------------------------------------------
# WAL record sizing
# ---------------------------------------------------------------------------

def _compute_record_size(schema, row):
    """
    Compute the total byte size required to serialise ``row`` into a WAL
    record, including the fixed header, fixed-width payload columns, and the
    variable-length heap section for string blobs.
    """
    fixed_size = wal_format._HDR_PAYLOAD_BASE + schema.memtable_stride
    heap_size = 0
    payload_col = 0
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
        col_type = schema.columns[i].field_type
        if col_type.code == types.TYPE_STRING.code:
            if not row.is_null(payload_col):
                heap_size += len(row.get_str(payload_col))
        payload_col += 1
    return fixed_size, heap_size


# ---------------------------------------------------------------------------
# MemTable (in-memory write buffer)
# ---------------------------------------------------------------------------

class MemTableEntry(object):
    """
    A single entry in the in-memory write buffer.
    """
    _immutable_fields_ = ["pk", "weight", "row"]

    def __init__(self, pk, weight, row):
        self.pk = pk        # r_uint128
        self.weight = weight    # r_int64
        self.row = row       # PayloadRow


class MemTable(object):
    """
    In-memory unsorted write buffer for one table.
    """

    def __init__(self, schema):
        self._schema = schema
        self._entries = []      # List, appended in order
        self._byte_estimate = 0

    def append(self, pk, weight, row):
        """Append a record to the write buffer."""
        entry = MemTableEntry(pk, weight, row)
        self._entries.append(entry)
        # Rough byte estimate for flush-threshold decisions.
        n_payload = len(row._lo)
        self._byte_estimate += 32 + n_payload * 8

    def entry_count(self):
        return len(self._entries)

    def byte_estimate(self):
        return self._byte_estimate

    def get_entries(self):
        """Return the entry list (caller must not mutate)."""
        return self._entries

    def clear(self):
        """Reset the buffer after a successful flush."""
        self._entries = []
        self._byte_estimate = 0


# ---------------------------------------------------------------------------
# WalWriter
# ---------------------------------------------------------------------------

class WalWriter(object):
    """
    Serialises ``(pk, weight, PayloadRow)`` triples into a WAL segment file.
    """

    def __init__(self, schema, fd):
        self._schema = schema
        self._fd = fd       # OS file descriptor (int)
        self._offset = 0        # bytes written to this segment so far

    def write_record(self, pk, weight, row):
        """
        Serialise one record and write it to the segment file.
        """
        fixed_size, heap_size = _compute_record_size(self._schema, row)
        total_size = fixed_size + heap_size

        # raw_malloc returns an Address
        buf = raw_malloc(total_size)
        wal_format.write_wal_record(
            self._schema,
            pk,
            weight,
            row,
            rffi.cast(rffi.CCHARP, buf),
            0,           # base: start of buffer
            fixed_size,  # heap_base: immediately after the fixed payload
        )

        _write_all(self._fd, buf, total_size)
        raw_free(buf)
        self._offset += total_size
        return self._offset

    def offset(self):
        return self._offset


def _write_all(fd, buf, size):
    """Write exactly ``size`` bytes from ``buf`` to file descriptor ``fd``."""
    import os as _os
    written = 0
    while written < size:
        # buf is an llmemory.Address; use address arithmetic
        ptr = buf + written
        chunk = rffi.cast(rffi.CCHARP, ptr)
        n = _os.write(fd, rffi.charpsize2str(chunk, size - written))
        if n <= 0:
            raise IOError("WAL write failed")
        written += n


# ---------------------------------------------------------------------------
# PersistentTable
# ---------------------------------------------------------------------------

class PersistentTable(object):
    """
    Durability and indexing layer for a single GnitzDB table.
    """

    _immutable_fields_ = ["_schema"]

    def __init__(self, schema, wal_writer=None):
        self._schema = schema
        self._memtable = MemTable(schema)
        self._wal_writer = wal_writer   # WalWriter or None

    def insert(self, pk, row):
        self._insert_weighted(pk, row, r_int64(1))

    def delete(self, pk, row):
        self._insert_weighted(pk, row, r_int64(-1))

    def insert_weighted(self, pk, row, weight):
        self._insert_weighted(pk, row, weight)

    def _insert_weighted(self, pk, row, weight):
        self._memtable.append(pk, weight, row)
        if self._wal_writer is not None:
            self._wal_writer.write_record(pk, weight, row)

    def flush_memtable(self):
        entries = self._memtable.get_entries()
        self._memtable.clear()
        return entries

    def memtable_byte_estimate(self):
        return self._memtable.byte_estimate()

    def memtable_entry_count(self):
        return self._memtable.entry_count()

    def replay_wal_record(self, buf, base, heap_base):
        """
        Decode one WAL record and insert it directly into the memtable.
        """
        pk, weight, row = wal_format.decode_wal_record(
            self._schema, buf, base, heap_base
        )
        self._memtable.append(pk, weight, row)
        return pk, weight, row
