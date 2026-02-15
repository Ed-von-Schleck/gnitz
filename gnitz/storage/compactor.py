import os
from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask
from gnitz.storage import shard_table, writer_table, tournament_tree, comparator
from gnitz.storage.shard_table import TableShardView
from gnitz.storage import manifest
from gnitz.storage.spine import ShardHandle
from gnitz.storage.shard_registry import ShardMetadata

class CompactionPolicy(object):
    def __init__(self, registry):
        self.registry = registry

    def should_compact(self, table_id):
        return self.registry.mark_for_compaction(table_id)


def merge_row_contributions(active_cursors, schema):
    """
    Groups inputs by Semantic Row Payload and sums weights.
    
    In DBSP, compaction is a pure Z-Set merge: 
    Q(A + B) = Q(A) + Q(B).
    """
    n = len(active_cursors)
    results = []
    
    processed = []
    for _ in range(n):
        processed.append(False)
    
    for i in range(n):
        if processed[i]: 
            continue
        
        base_cursor = active_cursors[i]
        total_weight = base_cursor.view.get_weight(base_cursor.position)
        processed[i] = True
        
        for j in range(i + 1, n):
            if processed[j]: 
                continue
            
            other_cursor = active_cursors[j]
            
            if comparator.compare_soa_rows(schema, base_cursor.view, base_cursor.position, 
                                           other_cursor.view, other_cursor.position) == 0:
                total_weight += other_cursor.view.get_weight(other_cursor.position)
                processed[j] = True
        
        if total_weight != 0:
            results.append((total_weight, i)) 
            
    return results


def compact_shards(input_files, output_file, schema, table_id=0, validate_checksums=False):
    """
    Executes an N-way merge compaction of overlapping shards.
    Utilizes a tournament tree for Primary Key alignment and 
    Semantic Row Payload grouping for Z-Set algebraic summation.
    """
    num_inputs = len(input_files)
    views = []
    cursors = []
    
    tree = None
    writer = None
    
    try:
        i = 0
        while i < num_inputs:
            filename = input_files[i]
            view = shard_table.TableShardView(filename, schema, validate_checksums=validate_checksums)
            views.append(view)
            cursors.append(tournament_tree.StreamCursor(view))
            i += 1
        
        tree = tournament_tree.TournamentTree(cursors)
        writer = writer_table.TableShardWriter(schema, table_id)
        
        stride = schema.memtable_stride
        tmp_row = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        
        while not tree.is_exhausted():
            min_key = tree.get_min_key()
            active_cursors = tree.get_all_cursors_at_min()
            
            merged = merge_row_contributions(active_cursors, schema)
            
            m_idx = 0
            num_merged = len(merged)
            while m_idx < num_merged:
                weight, exemplar_idx = merged[m_idx]
                exemplar_cursor = active_cursors[exemplar_idx]
                row_idx = exemplar_cursor.position
                
                for k in range(stride):
                    tmp_row[k] = '\x00'
                
                num_cols = len(schema.columns)
                for col_idx in range(num_cols):
                    if col_idx == schema.pk_index: 
                        continue
                    
                    col_ptr = exemplar_cursor.view.get_col_ptr(row_idx, col_idx)
                    dest_off = schema.get_column_offset(col_idx)
                    dest_ptr = rffi.ptradd(tmp_row, dest_off)
                    
                    sz = schema.columns[col_idx].field_type.size
                    for b in range(sz):
                        dest_ptr[b] = col_ptr[b]
                
                writer.add_row(min_key, weight, tmp_row, exemplar_cursor.view.blob_buf.ptr)
                m_idx += 1
            
            tree.advance_min_cursors(min_key)
            
        writer.finalize(output_file)
        lltype.free(tmp_row, flavor='raw')
    finally:
        if tree: 
            tree.close()
        
        for v in views:
            if v: v.close()


def execute_compaction(table_id, policy, manifest_mgr, ref_counter, schema, output_dir=".", spine_obj=None, validate_checksums=False):
    """
    High-level orchestrator for table-scoped compaction.
    """
    shards = policy.registry.get_shards_for_table(table_id)
    if not shards: 
        return None
    
    input_files = []
    
    true_min_lsn = shards[0].min_lsn
    true_max_lsn = shards[0].max_lsn
    
    for s in shards:
        input_files.append(s.filename)
        if s.min_lsn < true_min_lsn: true_min_lsn = s.min_lsn
        if s.max_lsn > true_max_lsn: true_max_lsn = s.max_lsn
    
    lsn_tag = intmask(true_max_lsn)
    out_filename = os.path.join(output_dir, "compacted_%d_%d.db" % (table_id, lsn_tag))
    
    for fn in input_files:
        ref_counter.acquire(fn)
        
    try:
        compact_shards(input_files, out_filename, schema, table_id, validate_checksums=validate_checksums)
        
        reader = manifest_mgr.load_current()
        new_entries = []
        for entry in reader.iterate_entries():
            keep = True
            for in_fn in input_files:
                if entry.shard_filename == in_fn:
                    keep = False
                    break
            if keep: 
                new_entries.append(entry)
        reader.close()
        
        v = TableShardView(out_filename, schema, validate_checksums=validate_checksums)
        try:
            is_u128 = schema.get_pk_column().field_type.size == 16
            if v.count > 0:
                min_k = v.get_pk_u128(0) if is_u128 else r_uint128(v.get_pk_u64(0))
                max_k = v.get_pk_u128(v.count-1) if is_u128 else r_uint128(v.get_pk_u64(v.count-1))
            else:
                min_k = shards[0].get_min_key()
                max_k = shards[len(shards)-1].get_max_key()

            new_entry = manifest.ManifestEntry(
                table_id, out_filename, min_k, max_k, 
                true_min_lsn, true_max_lsn
            )
            new_entries.append(new_entry)
            manifest_mgr.publish_new_version(new_entries, true_max_lsn)
            
            if spine_obj:
                new_handle = ShardHandle(out_filename, schema, true_max_lsn, validate_checksums=validate_checksums)
                spine_obj.replace_handles(input_files, new_handle)
            
            for f_path in input_files:
                policy.registry.unregister_shard(f_path)
                ref_counter.mark_for_deletion(f_path)
            
            policy.registry.register_shard(
                ShardMetadata(out_filename, table_id, min_k, max_k, true_min_lsn, true_max_lsn)
            )
            policy.registry.clear_compaction_flag(table_id)
        finally: 
            v.close()
            
    except Exception as e:
        if os.path.exists(out_filename):
            os.unlink(out_filename)
        raise e
    finally:
        for fn in input_files:
            ref_counter.release(fn)
        ref_counter.try_cleanup()
    
    return out_filename
