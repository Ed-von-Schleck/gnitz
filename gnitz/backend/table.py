# gnitz/backend/table.py
from gnitz.backend.cursor import AbstractCursor
from gnitz.core.types import TableSchema
from gnitz.core.values import PayloadRow
from gnitz.core.batch import ZSetBatch
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128

class AbstractTable(object):
    """
    The complete write+read interface the VM requires from any persistent Z-Set.
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

    def ingest(self, key, weight, payload):
        """
        Ingests a single record into the table.
        key: r_uint128, weight: r_int64, payload: PayloadRow
        """
        raise NotImplementedError

    def ingest_batch(self, batch):
        """
        Ingests a batch of records into the table. 
        Implementations should override this to perform optimized batch 
        IO (e.g., single-LSN WAL blocks and group fsync).
        
        batch: ZSetBatch (from gnitz.core.batch)
        """
        # Default implementation for backends not yet optimized for batching.
        # Iterates over the batch using the public core.batch API.
        for i in range(batch.length()):
            self.ingest(
                batch.get_pk(i), 
                batch.get_weight(i), 
                batch.get_row(i)
            )

    def get_weight(self, key, payload):
        """
        Returns the net algebraic weight for a specific record.
        key: r_uint128, payload: PayloadRow -> r_int64
        """
        raise NotImplementedError
