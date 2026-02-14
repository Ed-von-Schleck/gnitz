import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask
from gnitz.storage import shard_table, writer_table, tournament_tree, compaction_logic
from gnitz.storage.shard_table import TableShardView
from gnitz.storage import manifest
from gnitz.storage.spine import ShardHandle
from gnitz.storage.shard_registry import ShardMetadata

class CompactionPolicy(object):
    def __init__(self, registry):
        self.registry = registry

    def should_compact(self, table_id):
        return self.registry.mark_for_compaction(table_id)


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
            
            # Pure Z-Set Merge: Coalesce rows by semantic equality across shards
            merged = compaction_logic.merge_row_contributions(active_cursors, schema)
            
            m_idx = 0
            num_merged = len(merged)
            while m_idx < num_merged:
                weight, exemplar_idx = merged[m_idx]
                exemplar_cursor = active_cursors[exemplar_idx]
                row_idx = exemplar_cursor.position
                
                # Zero out the scratch buffer for the packed row payload
                k = 0
                while k < stride: 
                    tmp_row[k] = '\x00'
                    k += 1
                
                # Copy individual column data from the source shard
                col_idx = 0
                num_cols = len(schema.columns)
                while col_idx < num_cols:
                    if col_idx == schema.pk_index: 
                        col_idx += 1
                        continue
                    
                    col_ptr = exemplar_cursor.view.get_col_ptr(row_idx, col_idx)
                    dest_off = schema.get_column_offset(col_idx)
                    dest_ptr = rffi.ptradd(tmp_row, dest_off)
                    
                    sz = schema.columns[col_idx].field_type.size
                    b = 0
                    while b < sz:
                        dest_ptr[b] = col_ptr[b]
                        b += 1
                    col_idx += 1
                
                writer.add_row(min_key, weight, tmp_row, exemplar_cursor.view.blob_buf.ptr)
                m_idx += 1
            
            # Advance all cursors that contributed to this Primary Key
            tree.advance_min_cursors(min_key)
            
        writer.finalize(output_file)
        lltype.free(tmp_row, flavor='raw')
    finally:
        if tree: 
            tree.close()
        
        v_idx = 0
        while v_idx < len(views):
            v = views[v_idx]
            if v: v.close()
            v_idx += 1


def execute_compaction(table_id, policy, manifest_mgr, ref_counter, schema, output_dir=".", spine_obj=None, validate_checksums=False):
    """
    High-level orchestrator for table-scoped compaction.
    
    FIXED [Absolute Bounds Pattern]: 
    Manually calculates min/max LSN bounds because the registry list 
    is sorted by Primary Key, not temporal LSN sequence.
    """
    shards = policy.registry.get_shards_for_table(table_id)
    if not shards: 
        return None
    
    input_files = []
    
    # 1. Initialize absolute temporal bounds
    true_min_lsn = shards[0].min_lsn
    true_max_lsn = shards[0].max_lsn
    
    for s_idx in range(len(shards)):
        s = shards[s_idx]
        input_files.append(s.filename)
        
        # 2. Aggregating the union of LSN ranges
        if s.min_lsn < true_min_lsn:
            true_min_lsn = s.min_lsn
        if s.max_lsn > true_max_lsn:
            true_max_lsn = s.max_lsn
    
    # Generate filename using the highest LSN in the compacted set
    lsn_tag = intmask(true_max_lsn)
    out_filename = os.path.join(output_dir, "compacted_%d_%d.db" % (table_id, lsn_tag))
    
    for f_idx in range(len(input_files)):
        ref_counter.acquire(input_files[f_idx])
        
    try:
        compact_shards(input_files, out_filename, schema, table_id, validate_checksums=validate_checksums)
        
        reader = manifest_mgr.load_current()
        old_entries = []
        it = reader.iterate_entries()
        while True:
            try:
                entry = it.next()
                old_entries.append(entry)
            except StopIteration:
                break
        reader.close()
        
        new_entries = []
        for e_idx in range(len(old_entries)):
            entry = old_entries[e_idx]
            keep = True
            for in_idx in range(len(input_files)):
                if entry.shard_filename == input_files[in_idx]:
                    keep = False
                    break
            if keep: 
                new_entries.append(entry)
        
        v = TableShardView(out_filename, schema, validate_checksums=validate_checksums)
        try:
            is_u128 = schema.get_pk_column().field_type.size == 16
            if v.count > 0:
                min_k = v.get_pk_u128(0) if is_u128 else r_uint128(v.get_pk_u64(0))
                max_k = v.get_pk_u128(v.count-1) if is_u128 else r_uint128(v.get_pk_u64(v.count-1))
            else:
                # Fallback to key bounds of participant shards if new shard is empty (Ghost Property)
                # Note: Registry sort order ensures shards[0] is min PK and shards[-1] is max PK
                min_k = shards[0].get_min_key()
                max_k = shards[len(shards)-1].get_max_key()

            # Create new entry with absolute temporal bounds
            new_entry = manifest.ManifestEntry(
                table_id, out_filename, min_k, max_k, 
                true_min_lsn, true_max_lsn
            )
            new_entries.append(new_entry)
            
            # Publish updated manifest with absolute max LSN
            manifest_mgr.publish_new_version(new_entries, true_max_lsn)
            
            if spine_obj:
                new_handle = ShardHandle(out_filename, schema, true_max_lsn, validate_checksums=validate_checksums)
                spine_obj.replace_handles(input_files, new_handle)
            
            for cleanup_idx in range(len(input_files)):
                f_path = input_files[cleanup_idx]
                policy.registry.unregister_shard(f_path)
                ref_counter.mark_for_deletion(f_path)
            
            policy.registry.register_shard(
                ShardMetadata(out_filename, table_id, min_k, max_k, true_min_lsn, true_max_lsn)
            )
            policy.registry.clear_compaction_flag(table_id)
        finally: 
            v.close()
            
    finally:
        for release_idx in range(len(input_files)):
            ref_counter.release(input_files[release_idx])
        ref_counter.try_cleanup()
    
    return out_filename
