# gnitz/storage/compactor.py

import os
from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask
from gnitz.storage import shard_table, writer_table, tournament_tree, comparator, manifest, index
from gnitz.core import types

def merge_row_contributions(active_cursors, schema):
    """
    Groups inputs by Semantic Row Payload and sums weights.
    Uses the unified RowAccessor interface for comparison.
    """
    n = len(active_cursors)
    results = []
    processed = [False] * n
    
    # Create reusable accessors to avoid allocation in the loop
    acc_left = comparator.SoAAccessor(schema)
    acc_right = comparator.SoAAccessor(schema)
    
    for i in range(n):
        if processed[i]: 
            continue
        
        base_cursor = active_cursors[i]
        total_weight = base_cursor.view.get_weight(base_cursor.position)
        processed[i] = True
        
        # Set the left accessor once for the outer loop iteration
        acc_left.set_row(base_cursor.view, base_cursor.position)
        
        for j in range(i + 1, n):
            if processed[j]: 
                continue
            
            other_cursor = active_cursors[j]
            # Set the right accessor for the inner loop iteration
            acc_right.set_row(other_cursor.view, other_cursor.position)
            
            # Use the new unified comparison function
            if comparator.compare_rows(schema, acc_left, acc_right) == 0:
                total_weight += other_cursor.view.get_weight(other_cursor.position)
                processed[j] = True
        
        # Only preserve records with a non-zero net weight (Ghost Property)
        if total_weight != 0:
            results.append((total_weight, i)) 
            
    return results

def compact_shards(input_files, output_file, schema, table_id=0, validate_checksums=False):
    """
    Executes an N-way merge compaction of overlapping shards.
    """
    num_inputs = len(input_files)
    views = []
    cursors = []
    
    tree = None
    writer = None
    
    try:
        # 1. Initialize cursors for all overlapping shards
        i = 0
        while i < num_inputs:
            filename = input_files[i]
            view = shard_table.TableShardView(filename, schema, validate_checksums=validate_checksums)
            views.append(view)
            cursors.append(tournament_tree.StreamCursor(view))
            i += 1
        
        # 2. Setup N-way merge via Tournament Tree
        tree = tournament_tree.TournamentTree(cursors)
        writer = writer_table.TableShardWriter(schema, table_id)
        
        stride = schema.memtable_stride
        tmp_row = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        
        # 3. Process records in PK order
        while not tree.is_exhausted():
            min_key = tree.get_min_key()
            active_cursors = tree.get_all_cursors_at_min()
            
            # Coalesce weights for all payloads matching this PK
            merged = merge_row_contributions(active_cursors, schema)
            
            m_idx = 0
            num_merged = len(merged)
            while m_idx < num_merged:
                weight, exemplar_idx = merged[m_idx]
                exemplar_cursor = active_cursors[exemplar_idx]
                row_idx = exemplar_cursor.position
                
                # Materialize columnar row into temporary AoS buffer for writing
                for k in range(stride): tmp_row[k] = '\x00'
                
                num_cols = len(schema.columns)
                for col_idx in range(num_cols):
                    if col_idx == schema.pk_index: continue
                    
                    col_ptr = exemplar_cursor.view.get_col_ptr(row_idx, col_idx)
                    dest_off = schema.get_column_offset(col_idx)
                    dest_ptr = rffi.ptradd(tmp_row, dest_off)
                    
                    sz = schema.columns[col_idx].field_type.size
                    for b in range(sz): dest_ptr[b] = col_ptr[b]
                
                writer.add_row(min_key, weight, tmp_row, exemplar_cursor.view.blob_buf.ptr)
                m_idx += 1
            
            tree.advance_min_cursors(min_key)
            
        writer.finalize(output_file)
        lltype.free(tmp_row, flavor='raw')
    finally:
        if tree: tree.close()
        for v in views:
            if v: v.close()

def execute_compaction(shard_index, manifest_mgr, output_dir=".", validate_checksums=False):
    """
    High-level orchestrator for table-scoped compaction using the unified ShardIndex.
    """
    handles = shard_index.handles
    if not handles: return None
    
    table_id = shard_index.table_id
    schema = shard_index.schema
    input_files = []
    
    # Track the aggregate LSN range for the new Guard Shard
    true_min_lsn = handles[0].min_lsn
    true_max_lsn = handles[0].lsn
    
    for h in handles:
        input_files.append(h.filename)
        if h.min_lsn < true_min_lsn: true_min_lsn = h.min_lsn
        if h.lsn > true_max_lsn: true_max_lsn = h.lsn
    
    lsn_tag = intmask(true_max_lsn)
    out_filename = os.path.join(output_dir, "compacted_%d_%d.db" % (table_id, lsn_tag))
    
    try:
        # 1. Perform physical merge
        compact_shards(input_files, out_filename, schema, table_id, validate_checksums=validate_checksums)
        
        # 2. Create new handle for the resulting Guard Shard
        new_handle = index.ShardHandle(out_filename, schema, true_min_lsn, true_max_lsn, validate_checksums=validate_checksums)
        
        # 3. Update the Index (Atomically replaces handles, closes mmaps, releases locks)
        shard_index.replace_handles(input_files, new_handle)
        
        # 4. Update the Manifest Authority
        meta_list = shard_index.get_metadata_list()
        manifest_entries = []
        for meta in meta_list:
            manifest_entries.append(manifest.ManifestEntry(
                meta.table_id, meta.filename, meta.get_min_key(), meta.get_max_key(),
                meta.min_lsn, meta.max_lsn
            ))
        manifest_mgr.publish_new_version(manifest_entries, true_max_lsn)
        
        # 5. Cleanup physical files
        for f_path in input_files:
            shard_index.ref_counter.mark_for_deletion(f_path)
        shard_index.ref_counter.try_cleanup()
            
    except Exception as e:
        if os.path.exists(out_filename):
            os.unlink(out_filename)
        raise e
    
    return out_filename
