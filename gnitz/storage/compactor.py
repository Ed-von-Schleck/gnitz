import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, intmask
from gnitz.storage import shard_table, writer_table, tournament_tree, compaction_logic

class CompactionPolicy(object):
    def __init__(self, registry):
        self.registry = registry
    def should_compact(self, table_id):
        return self.registry.mark_for_compaction(table_id)

def compact_shards(input_files, output_file, schema, table_id=0, validate_checksums=False):
    num_inputs = len(input_files)
    views = [None] * num_inputs
    cursors = [None] * num_inputs
    tree = None
    writer = None
    
    try:
        for i in range(num_inputs):
            filename = input_files[i]
            view = shard_table.TableShardView(filename, schema, validate_checksums=validate_checksums)
            views[i] = view
            cursors[i] = tournament_tree.StreamCursor(view)
        
        tree = tournament_tree.TournamentTree(cursors)
        writer = writer_table.TableShardWriter(schema, table_id)
        
        stride = schema.memtable_stride
        tmp_row = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        
        while not tree.is_exhausted():
            min_key = tree.get_min_key()
            active_cursors = tree.get_all_cursors_at_min()
            merged = compaction_logic.merge_row_contributions(active_cursors, schema)
            
            for weight, exemplar_idx in merged:
                exemplar_cursor = active_cursors[exemplar_idx]
                row_idx = exemplar_cursor.position
                for k in range(stride): tmp_row[k] = '\x00'
                for i in range(len(schema.columns)):
                    if i == schema.pk_index: continue
                    col_ptr = exemplar_cursor.view.get_col_ptr(row_idx, i)
                    dest_off = schema.get_column_offset(i)
                    dest_ptr = rffi.ptradd(tmp_row, dest_off)
                    sz = schema.columns[i].field_type.size
                    for b in range(sz): dest_ptr[b] = col_ptr[b]
                writer.add_row(min_key, weight, tmp_row, exemplar_cursor.view.blob_buf.ptr)
            tree.advance_min_cursors(min_key)
            
        writer.finalize(output_file)
        lltype.free(tmp_row, flavor='raw')
    finally:
        if tree: tree.close()
        for v in views:
            if v: v.close()

def execute_compaction(table_id, policy, manifest_mgr, ref_counter, schema, output_dir=".", spine_obj=None, validate_checksums=False):
    shards = policy.registry.get_shards_for_table(table_id)
    if not shards: return None
    
    input_files = [s.filename for s in shards]
    # Fixed: Use intmask for string formatting to satisfy RPython annotator
    lsn_val = intmask(shards[0].min_lsn)
    out_filename = os.path.join(output_dir, "compacted_%d_%d.db" % (table_id, lsn_val))
    
    for f in input_files: ref_counter.acquire(f)
    try:
        compact_shards(input_files, out_filename, schema, table_id, validate_checksums=validate_checksums)
        
        reader = manifest_mgr.load_current()
        # Manifest entries might need to be resizable, but for now we try to filter carefully
        old_entries = []
        for entry in reader.iterate_entries():
            old_entries.append(entry)
        reader.close()
        
        new_entries = []
        for entry in old_entries:
            keep = True
            for old in input_files:
                if entry.shard_filename == old:
                    keep = False; break
            if keep: new_entries.append(entry)
        
        from gnitz.storage.shard_table import TableShardView
        v = TableShardView(out_filename, schema, validate_checksums=validate_checksums)
        try:
            is_u128 = schema.get_pk_column().field_type.size == 16
            if v.count > 0:
                min_k = v.get_pk_u128(0) if is_u128 else r_uint128(v.get_pk_u64(0))
                max_k = v.get_pk_u128(v.count-1) if is_u128 else r_uint128(v.get_pk_u64(v.count-1))
            else:
                min_k = shards[0].min_key
                max_k = shards[-1].max_key
                
            from gnitz.storage import manifest
            new_entry = manifest.ManifestEntry(table_id, out_filename, min_k, max_k, shards[0].min_lsn, shards[-1].max_lsn)
            new_entries.append(new_entry)
            manifest_mgr.publish_new_version(new_entries, shards[-1].max_lsn)
            
            if spine_obj:
                from gnitz.storage.spine import ShardHandle
                new_handle = ShardHandle(out_filename, schema, shards[-1].max_lsn, validate_checksums=validate_checksums)
                spine_obj.replace_handles(input_files, new_handle)
            
            for f in input_files:
                policy.registry.unregister_shard(f)
                ref_counter.mark_for_deletion(f)
            
            from gnitz.storage.shard_registry import ShardMetadata
            policy.registry.register_shard(ShardMetadata(out_filename, table_id, min_k, max_k, shards[0].min_lsn, shards[-1].max_lsn))
            policy.registry.clear_compaction_flag(table_id)
        finally: v.close()
    finally:
        # Crucial: Release references BEFORE trying to cleanup/delete files
        for f in input_files: ref_counter.release(f)
        ref_counter.try_cleanup()
    
    return out_filename
