# gnitz/core/store.py

from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from gnitz.core.comparator import RowAccessor


class AbstractCursor(object):
    """
    The complete read interface the VM requires from any persistent Z-Set.
    Implementations (MemTable, Shard, Unified) must adhere to this protocol.
    """

    def seek(self, key):
        """Positions the cursor at the first record >= key."""
        # key: r_uint128
        raise NotImplementedError

    def advance(self):
        """Moves the cursor to the next record in the sorted sequence."""
        raise NotImplementedError

    def is_valid(self):
        """Returns True if the cursor is positioned at a valid record."""
        # -> bool
        raise NotImplementedError

    def key(self):
        """Returns the Primary Key of the current record."""
        # -> r_uint128
        raise NotImplementedError

    def weight(self):
        """Returns the net algebraic weight of the current record."""
        # -> r_int64
        raise NotImplementedError

    def get_accessor(self):
        """Returns a RowAccessor for reading the payload of the current record."""
        # -> RowAccessor
        raise NotImplementedError

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
        batch: gnitz.core.batch.ArenaZSetBatch
        """
        raise NotImplementedError

    def create_cursor(self):
        """Creates a cursor for reading the current net state of the store."""
        # -> AbstractCursor
        raise NotImplementedError

    def has_pk(self, key):
        """
        Returns True if the Primary Key exists with a non-zero weight.
        key: r_uint128 -> bool
        """
        raise NotImplementedError

    def get_weight(self, key, accessor):
        """
        Returns the net algebraic weight for a specific record.
        key: r_uint128, accessor: RowAccessor -> r_int64
        """
        raise NotImplementedError

    def create_child(self, name, schema):
        """
        Creates a compatible internal store for VM operator state or indices.
        name: str, schema: TableSchema -> ZSetStore
        """
        raise NotImplementedError

    def flush(self):
        """Forces pending mutations to persistent storage."""
        # -> str (path to created shard or empty)
        raise NotImplementedError

    def close(self):
        """Closes the store and releases memory/file handles."""
        raise NotImplementedError
