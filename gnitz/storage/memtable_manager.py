from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage.memtable import MemTable
from gnitz.storage.wal_format import WALRecord

class MemTableManager(object):
    _immutable_fields_ = ['schema', 'capacity', 'table_id']
    def __init__(self, schema, capacity, wal_writer=None, table_id=None):
        self.schema = schema
        self.table_id = table_id
        self.capacity = capacity
        self.wal_writer = wal_writer
        self.active_table = MemTable(self.schema, self.capacity)
        self.current_lsn = r_uint64(1)
        self.starting_lsn = r_uint64(1)

    def put(self, key, weight, field_values):
        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)
        if self.wal_writer:
            # FIXED: Use the factory method to handle 128-bit key splitting safely
            self.wal_writer.append_block(
                lsn, 
                self.table_id, 
                [WALRecord.from_key(key, weight, field_values)]
            )
        self.active_table.upsert(r_uint128(key), weight, field_values)

    def flush_and_rotate(self, filename):
        self.active_table.flush(filename)
        self.active_table.free()
        self.active_table = MemTable(self.schema, self.capacity)

    def close(self):
        if self.active_table: self.active_table.free()
