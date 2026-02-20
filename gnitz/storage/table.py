# gnitz/storage/table.py
#
# PersistentTable: durability and indexing layer for a single GnitzDB table.
#
# Migration note (PayloadRow)
# ----------------------------
# ``PersistentTable.insert`` previously accepted ``List`` as its
# row parameter.  It now accepts ``PayloadRow`` directly.  There is no
# conversion layer; ``TaggedValue`` is removed from the codebase.
#
# The signature change is:
#
#   Before:  insert(self, pk, row)  where  row: List
#   After:   insert(self, pk, row)  where  row: PayloadRow
#
# All callers must be updated atomically (see §10.1 of the design plan).
# The WAL write path delegates to ``wal_format.write_wal_block``, which
# already reads from ``PayloadRow`` via schema dispatch.

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rawstorage import raw_malloc, raw_free, raw_memcopy

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

    This must be called before allocating the write buffer so the caller can
    allocate exactly the right amount of space (or a pre-sized ring buffer
    slot).

    Parameters
    ----------
    schema : TableSchema
    row : PayloadRow

    Returns
    -------
    (fixed_size, heap_size) : (int, int)
        ``fixed_size`` is the size of the header + fixed payload.
        ``heap_size`` is the total size of all string blob bytes.
        The total record size is ``fixed_size + heap_size``.
    """
    fixed_size = wal_format._HDR_PAYLOAD_BASE + schema.memtable_stride
    heap_size  = 0
    payload_col = 0
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
        col_type = schema.columns.field_type
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

    Holds the primary key, algebraic weight, and payload row for one
    pending write.  Entries are flushed to disk as part of a WAL segment
    and then incorporated into immutable shards during compaction.
    """
    _immutable_fields_ =

    def __init__(self, pk, weight, row):
        self.pk     = pk        # r_uint128
        self.weight = weight    # r_int64
        self.row    = row       # PayloadRow


class MemTable(object):
    """
    In-memory unsorted write buffer for one table.

    Records are appended in insertion order.  The buffer is sorted and
    deduplicated when flushed to a WAL segment and subsequently to an
    immutable shard.  Sorting uses the PK as the primary key.

    This class holds ``List``, not ``List[List]``.
    The ``PayloadRow`` reference stored in each entry is the canonical payload
    representation from the VM layer; no conversion is needed.
    """

    def __init__(self, schema):
        self._schema  = schema
        self._entries = []      # List, appended in order
        self._byte_estimate = 0

    def append(self, pk, weight, row):
        """Append a record to the write buffer."""
        entry = MemTableEntry(pk, weight, row)
        self._entries.append(entry)
        # Rough byte estimate for flush-threshold decisions.
        # Header (32) + one word per payload column (8) + string lengths.
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
        self._entries =[]
        self._byte_estimate = 0


# ---------------------------------------------------------------------------
# WalWriter
# ---------------------------------------------------------------------------

class WalWriter(object):
    """
    Serialises ``(pk, weight, PayloadRow)`` triples into a WAL segment file.

    Each call to ``write_record`` appends one record.  ``flush`` syncs the
    file descriptor and returns the file offset at which the next record will
    be written.

    This is a minimal write-path object.  The full WAL manager (segment
    rotation, recovery, checksumming) lives in ``gnitz/storage/wal.py``.
    """

    def __init__(self, schema, fd):
        self._schema = schema
        self._fd     = fd       # OS file descriptor (int)
        self._offset = 0        # bytes written to this segment so far

    def write_record(self, pk, weight, row):
        """
        Serialise one record and write it to the segment file.

        Allocates a temporary C buffer, calls ``wal_format.write_wal_block``,
        writes the buffer to the file descriptor, then frees the buffer.
        """
        fixed_size, heap_size = _compute_record_size(self._schema, row)
        total_size = fixed_size + heap_size

        buf = raw_malloc(total_size)
        wal_format.write_wal_record(
            self._schema,
            pk, weight, row,
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
    # RPython's os.write binding works on str; we use rffi to write raw bytes.
    import os as _os
    written = 0
    while written < size:
        ptr   = rffi.ptradd(buf, written)
        chunk = rffi.cast(rffi.CCHARP, ptr)
        n     = _os.write(fd, rffi.charpsize2str(chunk, size - written))
        if n <= 0:
            raise IOError("WAL write failed")
        written += n


# ---------------------------------------------------------------------------
# PersistentTable
# ---------------------------------------------------------------------------

class PersistentTable(object):
    """
    Durability and indexing layer for a single GnitzDB table.

    Manages an in-memory write buffer (``MemTable``) and a WAL segment
    writer.  A ``PayloadRow`` produced by the VM layer is inserted via
    ``insert``; the table appends it to the memtable and (if the WAL is
    enabled) writes it to the WAL segment.

    Schema-level dispatch
    ---------------------
    ``PersistentTable`` never inspects ``TaggedValue.tag``.  All column-type
    dispatch is on ``schema.columns.field_type.code``, consistent with the
    rest of the post-migration storage layer.

    Thread safety
    -------------
    Not thread-safe.  External callers must hold a table-level lock.
    """

    _immutable_fields_ =

    def __init__(self, schema, wal_writer=None):
        """
        Parameters
        ----------
        schema : TableSchema
        wal_writer : WalWriter or None
            If ``None``, inserts are accepted into the memtable but not
            persisted to a WAL (useful for ephemeral / read-replica tables).
        """
        self._schema     = schema
        self._memtable   = MemTable(schema)
        self._wal_writer = wal_writer   # WalWriter or None

    # ------------------------------------------------------------------
    # Public insertion API
    # ------------------------------------------------------------------

    def insert(self, pk, row):
        """
        Insert a row with algebraic weight +1.

        Parameters
        ----------
        pk : r_uint128
            Primary key value.  Must not equal the PK column value already
            present in ``row`` (the PK column slot is excluded from
            ``PayloadRow``'s arrays by construction).
        row : PayloadRow
            Non-PK column values constructed via ``make_payload_row(schema)``
            followed by one ``append_*`` call per non-PK column in schema
            order.  Passing a ``List`` here is a type error;
            ``TaggedValue`` has been removed from the codebase.
        """
        self._insert_weighted(pk, row, r_int64(1))

    def delete(self, pk, row):
        """
        Insert a row with algebraic weight -1 (logical deletion).

        The ``row`` must match the stored payload exactly so that the DBSP
        accumulator resolves to zero.
        """
        self._insert_weighted(pk, row, r_int64(-1))

    def insert_weighted(self, pk, row, weight):
        """
        Insert a row with an explicit algebraic weight.

        Primarily used during WAL replay and compaction merge.
        """
        self._insert_weighted(pk, row, weight)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _insert_weighted(self, pk, row, weight):
        """
        Core insertion path: append to memtable and (optionally) WAL.

        The caller is responsible for schema validation; this method
        performs no per-column type checking at runtime.
        """
        self._memtable.append(pk, weight, row)
        if self._wal_writer is not None:
            self._wal_writer.write_record(pk, weight, row)

    def flush_memtable(self):
        """
        Return the current memtable entries and reset the buffer.

        Called by the shard writer when the memtable has grown past its
        flush threshold.  The returned list of ``MemTableEntry`` objects
        must be processed (sorted, serialised to a shard) before the
        next flush.
        """
        entries = self._memtable.get_entries()
        self._memtable.clear()
        return entries

    def memtable_byte_estimate(self):
        return self._memtable.byte_estimate()

    def memtable_entry_count(self):
        return self._memtable.entry_count()

    # ------------------------------------------------------------------
    # WAL recovery
    # ------------------------------------------------------------------

    def replay_wal_record(self, buf, base, heap_base):
        """
        Decode one WAL record and insert it directly into the memtable,
        bypassing the WAL writer (to avoid re-writing records during replay).

        Parameters
        ----------
        buf : rffi.CCHARP
        base : int
        heap_base : int

        Returns
        -------
        pk : r_uint128
        weight : r_int64
        row : PayloadRow
        """
        pk, weight, row = wal_format.decode_wal_block(
            self._schema, buf, base, heap_base
        )
        self._memtable.append(pk, weight, row)
        return pk, weight, row
