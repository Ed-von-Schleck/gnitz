# gnitz/backend/table.py
from gnitz.backend.cursor import AbstractCursor
from gnitz.core.types import TableSchema

class AbstractTable(object):
    """
    The complete write+read interface the VM requires from any persistent Z-Set.
    """
    def get_schema(self):
        # → TableSchema
        raise NotImplementedError

    def create_cursor(self):
        # → AbstractCursor
        raise NotImplementedError

    def create_scratch_table(self, name, schema):
        # → AbstractTable
        # Creates a compatible internal table for VM operator state.
        # The implementation decides where/how to persist it.
        raise NotImplementedError

    def ingest(self, key, weight, payload):
        # key: r_uint128, weight: r_int64, payload: List[TaggedValue]
        raise NotImplementedError

    def get_weight(self, key, payload):
        # → r_int64
        raise NotImplementedError
