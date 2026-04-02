# gnitz/core/store.py

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from gnitz.core.comparator import RowAccessor


class AbstractCursor(object):
    """
    The complete read interface the VM requires from any persistent Z-Set.
    Implementations (MemTable, Shard, Unified) must adhere to this protocol.
    """

    def seek(self, key_lo, key_hi):
        """Positions the cursor at the first record >= (key_hi, key_lo)."""
        raise NotImplementedError

    def advance(self):
        """Moves the cursor to the next record in the sorted sequence."""
        raise NotImplementedError

    def is_valid(self):
        """Returns True if the cursor is positioned at a valid record."""
        # -> bool
        raise NotImplementedError

    def is_exhausted(self):
        """Returns True if no more records remain."""
        return not self.is_valid()

    def key_lo(self):
        """Returns the low 64 bits of the Primary Key of the current record."""
        raise NotImplementedError

    def key_hi(self):
        """Returns the high 64 bits of the Primary Key of the current record."""
        raise NotImplementedError

    def key(self):
        """Returns the Primary Key as r_uint128. Backward-compat assembler."""
        return (r_uint128(self.key_hi()) << 64) | r_uint128(self.key_lo())

    def peek_key_lo(self):
        """Returns key_lo without consuming. Default: same as key_lo()."""
        return self.key_lo()

    def peek_key_hi(self):
        """Returns key_hi without consuming. Default: same as key_hi()."""
        return self.key_hi()

    def peek_key(self):
        """Returns the key without consuming the record. Default: same as key()."""
        return (r_uint128(self.peek_key_hi()) << 64) | r_uint128(self.peek_key_lo())

    def weight(self):
        """Returns the net algebraic weight of the current record."""
        # -> r_int64
        raise NotImplementedError

    def get_accessor(self):
        """Returns a RowAccessor for reading the payload of the current record."""
        # -> RowAccessor
        raise NotImplementedError

    def estimated_length(self):
        """Upper bound on live records. Permitted to over-count; must not under-count."""
        return 0

    def seek_key_exact(self, key_lo, key_hi):
        """Seek to (key_lo, key_hi). Returns True iff cursor is now positioned exactly there."""
        self.seek(key_lo, key_hi)
        return self.is_valid() and self.key_lo() == key_lo and self.key_hi() == key_hi

    def close(self):
        """Releases any resources held by the cursor."""
        raise NotImplementedError


class ZSetStore(object):
    """
    The unified storage interface (Z-Store).
    Implemented by TableFamily (Catalog), PersistentTable, and EphemeralTable.
    """
    _immutable_fields_ = ["schema"]

    def get_schema(self):
        """Returns the TableSchema defining the physical layout."""
        # -> gnitz.core.types.TableSchema
        raise NotImplementedError

    def ingest_batch(self, batch):
        """
        Ingests a batch of records.
        batch: gnitz.storage.owned_batch.ArenaZSetBatch
        """
        raise NotImplementedError

    def create_cursor(self):
        """Creates a cursor for reading the current net state of the store."""
        # -> AbstractCursor
        raise NotImplementedError

    def has_pk(self, key_lo, key_hi):
        """
        Returns True if the Primary Key exists with a non-zero weight.
        key_lo, key_hi: r_uint64 -> bool
        """
        raise NotImplementedError

    def get_weight(self, key_lo, key_hi, accessor):
        """
        Returns the net algebraic weight for a specific record.
        key_lo, key_hi: r_uint64, accessor: RowAccessor -> r_int64
        """
        raise NotImplementedError

    def create_child(self, name, schema):
        """
        Creates a compatible internal store for VM operator state or indices.
        name: str, schema: TableSchema -> ZSetStore
        """
        raise NotImplementedError

    def retract_pk(self, key_lo, key_hi, out_batch):
        """If PK exists with positive net weight, append retraction to out_batch.
        Returns True if retraction was emitted.
        key_lo, key_hi: r_uint64, out_batch: ArenaZSetBatch -> bool
        """
        raise NotImplementedError

    def flush(self):
        """Forces pending mutations to persistent storage."""
        # -> str (path to created shard or empty)
        raise NotImplementedError

    def close(self):
        """Closes the store and releases memory/file handles."""
        raise NotImplementedError

    def compact_if_needed(self):
        """Trigger compaction if shard count exceeds threshold. No-op by default."""
        pass

    def ingest_batch_memonly(self, batch):
        """Ingest into memtable only, bypassing the WAL. Worker DDL sync use."""
        self.ingest_batch(batch)

    def close_partitions_outside(self, start, end):
        """Close partitions not in [start, end). Only meaningful for PartitionedTable."""
        raise NotImplementedError

    def close_all_partitions(self):
        """Close all partitions. Only meaningful for PartitionedTable."""
        raise NotImplementedError
