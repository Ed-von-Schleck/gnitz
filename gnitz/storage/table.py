# gnitz/storage/table.py

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix

from gnitz.core import types, strings, errors
from gnitz.core.values import PayloadRow
from gnitz.core.row_logic import PayloadRowComparator
from gnitz.storage import wal_format, mmap_posix, buffer, manifest, refcount


# ---------------------------------------------------------------------------
# MemTable (In-memory row-oriented write buffer)
# ---------------------------------------------------------------------------

class MemTableEntry(object):
    _immutable_fields_ = ["pk", "weight", "row"]

    def __init__(self, pk, weight, row):
        self.pk = r_uint128(pk)
        self.weight = weight
        self.row = row

    def compare_to(self, other, row_cmp):
        if self.pk < other.pk: return -1
        if self.pk > other.pk: return 1
        return row_cmp.compare(self.row, other.row)


class MemTable(object):
    def __init__(self, schema):
        self.schema = schema
        self._entries = []
        self._byte_estimate = 0

    def append(self, pk, weight, row):
        self._entries.append(MemTableEntry(pk, weight, row))
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
    _immutable_fields_ = ["_schema", "_directory", "_name", "_row_cmp", "table_id"]

    def __init__(self, directory, name, schema, wal_writer=None, table_id=0, 
                 cache_size=1048576, read_only=False, validate_checksums=False):
        self._schema = schema
        self._directory = directory
        self._name = name
        self.table_id = table_id
        self.validate_checksums = validate_checksums
        self.is_closed = False
        
        self._memtable = MemTable(schema)
        self._row_cmp = PayloadRowComparator(schema)
        self.ref_counter = refcount.RefCounter()
        
        # Ensure directory exists and initialize manifest
        if directory is not None:
            if not os.path.exists(directory):
                os.makedirs(directory)
            manifest_path = os.path.join(directory, "MANIFEST")
            self.manifest_manager = manifest.ManifestManager(manifest_path)
        else:
            self.manifest_manager = None

        # WAL Setup
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
        if self.is_closed:
            raise errors.StorageError("Table is closed")
        self._memtable.append(pk, weight, row)
        if self._wal_writer:
            self._wal_writer.write_record(pk, weight, row)

    def get_weight(self, key, payload):
        total_w = r_int64(0)
        key_u128 = r_uint128(key)
        
        # 1. Search MemTable
        for entry in self._memtable.get_entries():
            if entry.pk == key_u128 and self._row_cmp.compare(entry.row, payload) == 0:
                total_w += entry.weight
        
        # 2. Search Shards
        if self.manifest_manager and self.manifest_manager.exists():
            from gnitz.storage.shard_table import TableShardView
            reader = self.manifest_manager.load_current()
            try:
                is_pk128 = self._schema.get_pk_column().field_type.size == 16
                for entry in reader.iterate_entries():
                    if entry.get_min_key() <= key_u128 <= entry.get_max_key():
                        view = TableShardView(entry.shard_filename, self._schema, 
                                              validate_checksums=self.validate_checksums)
                        try:
                            idx = view.find_lower_bound(key_u128)
                            while idx < view.count:
                                mid_key = view.get_pk_u128(idx) if is_pk128 else r_uint128(view.get_pk_u64(idx))
                                if mid_key != key_u128:
                                    break
                                if self._compare_shard_row_to_payload(view, idx, payload):
                                    total_w += view.get_weight(idx)
                                idx += 1
                        finally:
                            view.close()
            finally:
                reader.close()
        return total_w

    def _compare_shard_row_to_payload(self, view, shard_idx, payload):
        from gnitz.storage.comparator import SoAAccessor
        from gnitz.core.comparator import PayloadRowAccessor, compare_rows
        
        soa = SoAAccessor(self._schema)
        soa.set_row(view, shard_idx)
        val = PayloadRowAccessor(self._schema)
        val.set_row(payload)
        return compare_rows(self._schema, soa, val) == 0

    def flush_memtable(self):
        entries = self._memtable.get_entries()
        self._memtable.clear()
        return entries

    def _trigger_compaction(self):
        pass

    def flush(self):
        from gnitz.storage.writer_table import TableShardWriter
        
        entries = self._memtable.get_entries()
        if not entries:
            self._memtable.clear()
            return ""
        
        # Sort to ensure PK ordering in shard and to identify min/max PK
        def sort_cmp(a, b): return a.compare_to(b, self._row_cmp)
        entries.sort(cmp=sort_cmp)
        
        shard_name = "shard_%d.db" % intmask(r_uint64(entries[0].pk))
        shard_path = os.path.join(self._directory, shard_name)
        writer = TableShardWriter(self._schema, self.table_id)
        
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
                heap_sz = 0
                for col_idx in range(len(self._schema.columns)):
                    if col_idx == self._schema.pk_index: continue
                    if self._schema.columns[col_idx].field_type == types.TYPE_STRING:
                        p_idx = col_idx if col_idx < self._schema.pk_index else col_idx - 1
                        s = base_entry.row.get_str(p_idx)
                        if len(s) > strings.SHORT_STRING_THRESHOLD: heap_sz += len(s)

                rec_buf = lltype.malloc(rffi.CCHARP.TO, stride + heap_sz, flavor='raw')
                stable_buffers.append(rec_buf)
                for k in range(stride + heap_sz): rec_buf[k] = '\x00'
                
                curr_blob_off = 0
                for col_idx in range(len(self._schema.columns)):
                    if col_idx == self._schema.pk_index: continue
                    p_idx = col_idx if col_idx < self._schema.pk_index else col_idx - 1
                    col_def = self._schema.columns[col_idx]
                    dest_ptr = rffi.ptradd(rec_buf, self._schema.get_column_offset(col_idx))
                    
                    if col_def.field_type == types.TYPE_STRING:
                        s_val = base_entry.row.get_str(p_idx)
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
                        if sz == 8: 
                            rffi.cast(rffi.ULONGLONGP, dest_ptr)[0] = rffi.cast(rffi.ULONGLONG, v)
                        elif sz == 4: 
                            rffi.cast(rffi.UINTP, dest_ptr)[0] = rffi.cast(rffi.UINT, v)
                        elif sz == 2:
                            dest_ptr[0] = chr(intmask(v) & 0xFF)
                            dest_ptr[1] = chr((intmask(v) >> 8) & 0xFF)
                        else: 
                            dest_ptr[0] = chr(intmask(v) & 0xFF)

                writer.add_row(base_entry.pk, net_weight, rec_buf, rec_buf)
            i = next_i

        # If after annihilation no rows remain, writer.count is 0.
        # Manifest update should only happen if writer.count > 0.
        if writer.count > 0:
            writer.finalize(shard_path)
            # The first and last surviving entries define the PK range for the manifest
            # Since we only wrote rows with net_weight != 0, we need the range of THOSE rows.
            # However, for simplicity in this baseline, we use the sort order.
            self._update_manifest_with_shard(shard_path, entries[0].pk, entries[-1].pk)
        else:
            writer.close()
            shard_path = ""
        
        for b in stable_buffers: lltype.free(b, flavor='raw')
        self._memtable.clear()
        return shard_path

    def _update_manifest_with_shard(self, path, min_pk, max_pk):
        if not self.manifest_manager: return
        entries = []
        if self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            for e in reader.iterate_entries(): entries.append(e)
            reader.close()
        
        new_entry = manifest.ManifestEntry(self.table_id, path, min_pk, max_pk, 0, 0)
        entries.append(new_entry)
        self.manifest_manager.publish_new_version(entries)

    def memtable_byte_estimate(self): return self._memtable.byte_estimate()
    def memtable_entry_count(self): return self._memtable.entry_count()

    def close(self):
        if self.is_closed: return
        if self._wal_writer: self._wal_writer.close()
        self.is_closed = True
