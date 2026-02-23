# gnitz/backend/table.py
from gnitz.backend.cursor import AbstractCursor
from gnitz.core.types import TableSchema
from gnitz.core.batch import ZSetBatch
from gnitz.core.comparator import RowAccessor
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128

class AbstractTable(object):
    """
    The complete write+read interface the VM requires from any persistent Z-Set.
    
    Confinement: This interface is now PayloadRow-free. All data entering
    or being queried through this interface must use Accessors or Batches.
    """

    def get_schema(self):
        """Returns the TableSchema defining the physical layout."""
        # -> TableSchema
        raise NotImplementedError

    def create_cursor(self):
        """Creates a cursor for reading the current net state of the table."""
        # -> AbstractCursor
        raise NotImplementedError

    def create_scratch_table(self, name, schema):
        """
        Creates a compatible internal table for VM operator state (Traces).
        The implementation decides where/how to persist it (e.g. EphemeralTable).
        """
        # -> AbstractTable
        raise NotImplementedError

    def ingest_batch(self, batch):
        """
        Ingests a batch of records into the table. 
        Implementations should perform optimized batch IO (e.g., WAL 
        group-fsync and MemTable fast-path ingestion).
        
        batch: ArenaZSetBatch (from gnitz.vm.batch)
        """
        raise NotImplementedError

    def get_weight(self, key, accessor):
        """
        Returns the net algebraic weight for a specific record.
        
        Zero-allocation: the caller provides a RowAccessor which the 
        implementation uses for SkipList hashing and record comparison.
        
        key: r_uint128, accessor: RowAccessor -> r_int64
        """
        raise NotImplementedError
