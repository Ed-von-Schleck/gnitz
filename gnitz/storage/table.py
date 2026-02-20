# gnitz/storage/table.py
#
# PersistentTable: durability and indexing layer for a single GnitzDB table.

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix

from gnitz.core import types, strings
from gnitz.core.values import PayloadRow
from gnitz.core.row_logic import PayloadRowComparator
from gnitz.storage import wal_format, mmap_posix, buffer


# ---------------------------------------------------------------------------
# MemTable (In-memory row-oriented write buffer)
# ---------------------------------------------------------------------------

class MemTableEntry(object):
    """
    A single entry in the write buffer.
    """
    _immutable_fields_ = ["pk", "weight", "row"]

    def __init__(self, pk, weight, row):
        self.pk = r_uint128(pk)
        self.weight = weight
        self.row = row

    def compare_to(self, other, row_cmp):
        """Total ordering: PK first, then Payload."""
        if self.pk < other.pk: return -1
        if self.pk > other.pk: return 1
        return row_cmp.compare(self.row, other.row)


class MemTable(object):
    """
    Simple insertion-ordered buffer. Used by PersistentTable to bridge 
    unit tests and integration logic.
    """

    def __init__(self, schema):
        self.schema = schema
        self._entries = []
        self._byte_estimate = 0

    def append(self, pk, weight, row):
        self._entries.append(MemTableEntry(pk, weight, row))
        # Overhead estimate
        self._byte_estimate += 128 + (len(row._lo) * 8)

    def entry_count(self):
        return len(self._entries)

    def byte_estimate(self):
        return self._byte_estimate

    def get_entries(self):
        return self._entries

    def clear(self):
        self._entries = []
        self._byte_estimate = 0


# ---------------------------------------------------------------------------
# WalWriter
# ---------------------------------------------------------------------------

class WalWriter(object):
    def __init__(self, schema, fd):
        self._schema = schema
        self._fd = fd
        self._offset = 0

    def write_record(self, pk, weight, row):
        fixed_size, heap_size = wal_format.compute_record_size(self._schema, row)
        total_size = fixed_size + heap_size
        buf = lltype.malloc(rffi.CCHARP.TO, total_size, flavor='raw')
        try:
            wal_format.write_wal_record(
                self._schema, r_uint128(pk), weight, row, buf, 0, fixed_size
            )
            _write_all(self._fd, buf, total_size)
        finally:
            lltype.free(buf, flavor='raw')
        self._offset += total_size
        return self._offset

    def close(self):
        if self._fd != -1:
            rposix.close(self._fd)
            self._fd = -1


def _write_all(fd, buf, size):
    written = 0
    while written < size:
        ptr = rffi.ptradd(buf, written)
        n = mmap_posix.write_c(fd, ptr, size - written)
        n_signed = rffi.cast(lltype.Signed, n)
        if n_signed <= 0: raise IOError("WAL write failed")
        written += n_signed


# ---------------------------------------------------------------------------
# PersistentTable
# ---------------------------------------------------------------------------

class PersistentTable(object):
    _immutable_fields_ = ["_schema", "_directory", "_name", "_row_cmp"]

    def __init__(self, directory, name, schema, wal_writer=None):
        self._schema = schema
        self._directory = directory
        self._name = name
        self._memtable = MemTable(schema)
        self._row_cmp = PayloadRowComparator(schema)
        
        if wal_writer is not None:
            self._wal_writer = wal_writer
        elif directory is not None and name is not None:
            path = os.path.join(directory, name + ".wal")
            fd = rposix.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            self._wal_writer = WalWriter(schema, fd)
        else:
            self._wal_writer = None

    def insert(self, pk, row):
        self._insert_weighted(pk, row, r_int64(1))

    def delete(self, pk, row):
        self._insert_weighted(pk, row, r_int64(-1))

    def insert_weighted(self, pk, row, weight):
        self._insert_weighted(pk, row, weight)

    def _insert_weighted(self, pk, row, weight):
        self._memtable.append(pk, weight, row)
        if self._wal_writer: self._wal_writer.write_record(pk, weight, row)

    def flush_memtable(self):
        entries = self._memtable.get_entries()
        self._memtable.clear()
        return entries

    def flush(self):
        """
        Transmutes the MemTable to a Columnar Shard.
        Performs manual serialization to ensure German String offset stability.
        """
        from gnitz.storage.writer_table import TableShardWriter
        
        shard_filename = os.path.join(self._directory, self._name + ".db")
        writer = TableShardWriter(self._schema, 1)
        
        entries = self._memtable.get_entries()
        if not entries:
            self._memtable.clear()
            return shard_filename
        
        # Total sort (PK + Payload)
        def sort_cmp(a, b): return a.compare_to(b, self._row_cmp)
        entries.sort(cmp=sort_cmp)
        
        stable_buffers = []
        stride = self._schema.memtable_stride
        
        i = 0
        n = len(entries)
        while i < n:
            base_entry = entries[i]
            net_weight = base_entry.weight
            next_i = i + 1
            while next_i < n:
                other = entries[next_i]
                if other.pk == base_entry.pk and self._row_cmp.compare(other.row, base_entry.row) == 0:
                    net_weight += other.weight
                    next_i += 1
                else: break
            
            if net_weight != 0:
                # 1. Calculate blob size
                heap_sz = 0
                for col_idx in range(len(self._schema.columns)):
                    if col_idx == self._schema.pk_index: continue
                    if self._schema.columns[col_idx].field_type == types.TYPE_STRING:
                        s = base_entry.row.get_str(col_idx if col_idx < self._schema.pk_index else col_idx - 1)
                        if len(s) > strings.SHORT_STRING_THRESHOLD: heap_sz += len(s)

                # 2. Serialize AoS Row [Stride] + [Blobs]
                rec_buf = lltype.malloc(rffi.CCHARP.TO, stride + heap_sz, flavor='raw')
                stable_buffers.append(rec_buf)
                
                # Zero out stride
                for k in range(stride): rec_buf[k] = '\x00'
                
                curr_blob_off = 0
                for col_idx in range(len(self._schema.columns)):
                    if col_idx == self._schema.pk_index: continue
                    
                    p_idx = col_idx if col_idx < self._schema.pk_index else col_idx - 1
                    col_def = self._schema.columns[col_idx]
                    dest_ptr = rffi.ptradd(rec_buf, self._schema.get_column_offset(col_idx))
                    
                    if col_def.field_type == types.TYPE_STRING:
                        s_val = base_entry.row.get_str(p_idx)
                        # Offset is relative to start of rec_buf
                        strings.pack_string(dest_ptr, s_val, stride + curr_blob_off)
                        if len(s_val) > strings.SHORT_STRING_THRESHOLD:
                            blob_dest = rffi.ptradd(rec_buf, stride + curr_blob_off)
                            for k in range(len(s_val)): blob_dest[k] = s_val[k]
                            curr_blob_off += len(s_val)
                    elif col_def.field_type == types.TYPE_U128:
                        val = base_entry.row.get_u128(p_idx)
                        rffi.cast(rffi.ULONGLONGP, dest_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(val))
                        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(dest_ptr, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(val >> 64))
                    elif col_def.field_type in (types.TYPE_F64, types.TYPE_F32):
                        rffi.cast(rffi.DOUBLEP, dest_ptr)[0] = base_entry.row.get_float(p_idx)
                    else:
                        sz = col_def.field_type.size
                        v = base_entry.row.get_int(p_idx)
                        if sz == 8: rffi.cast(rffi.ULONGLONGP, dest_ptr)[0] = v
                        elif sz == 4: rffi.cast(rffi.UINTP, dest_ptr)[0] = rffi.cast(rffi.UINT, v)
                        elif sz == 2: rffi.cast(rffi.USHORTP, dest_ptr)[0] = rffi.cast(rffi.USHORT, v)
                        else: dest_ptr[0] = rffi.cast(rffi.UCHAR, v)

                # 3. Add to writer
                writer.add_row(base_entry.pk, net_weight, rec_buf, rec_buf)
            i = next_i

        writer.finalize(shard_filename)
        for b in stable_buffers: lltype.free(b, flavor='raw')
        self._memtable.clear()
        return shard_filename

    def memtable_byte_estimate(self): return self._memtable.byte_estimate()
    def memtable_entry_count(self): return self._memtable.entry_count()
    def close(self):
        if self._wal_writer: self._wal_writer.close()
