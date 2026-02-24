# gnitz/storage/compactor.py

import os
from rpython.rlib import jit
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask
from gnitz.storage import shard_table, writer_table, tournament_tree, index, cursor
from gnitz.core import types, comparator as core_comparator


NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


def merge_row_contributions(active_cursors, schema):
    """
    Groups inputs by Semantic Row Payload and sums weights.
    Uses the unified get_row_accessor() interface to remain storage-agnostic.
    """
    n = len(active_cursors)
    results = newlist_hint(n)

    processed = newlist_hint(n)
    for _ in range(n):
        processed.append(False)

    i = 0
    while i < n:
        if processed[i]:
            i += 1
            continue

        base_cursor = active_cursors[i]
        total_weight = base_cursor.weight()
        processed[i] = True

        acc_left = base_cursor.get_row_accessor()

        j = i + 1
        while j < n:
            if processed[j]:
                j += 1
                continue

            other_cursor = active_cursors[j]
            acc_right = other_cursor.get_row_accessor()

            # Lexicographical comparison of non-PK columns
            if core_comparator.compare_rows(schema, acc_left, acc_right) == 0:
                total_weight += other_cursor.weight()
                processed[j] = True
            j += 1

        # Only preserve records with a non-zero net weight (Ghost Property)
        if total_weight != 0:
            results.append((total_weight, i))
        i += 1

    return results


def compact_shards(
    input_files, output_file, schema, table_id=0, validate_checksums=False
):
    """
    Executes an N-way merge compaction of overlapping shards.
    """
    num_inputs = len(input_files)
    views = newlist_hint(num_inputs)
    cursors = newlist_hint(num_inputs)

    tree = None
    writer = None

    try:
        # 1. Initialize cursors for all overlapping shards
        idx = 0
        while idx < num_inputs:
            filename = input_files[idx]
            view = shard_table.TableShardView(
                filename, schema, validate_checksums=validate_checksums
            )
            views.append(view)
            cursors.append(cursor.ShardCursor(view))
            idx += 1

        # 2. Setup N-way merge via Tournament Tree
        tree = tournament_tree.TournamentTree(cursors)
        writer = writer_table.TableShardWriter(schema, table_id)

        stride = schema.memtable_stride
        tmp_row = lltype.malloc(rffi.CCHARP.TO, stride, flavor="raw")

        # 3. Process records in PK order
        while not tree.is_exhausted():
            min_key = tree.get_min_key()
            active_cursors = tree.get_all_cursors_at_min()

            # Coalesce weights for all payloads matching this PK
            merged = merge_row_contributions(active_cursors, schema)

            num_cols = len(schema.columns)
            m_idx = 0
            num_merged = len(merged)
            while m_idx < num_merged:
                weight, exemplar_idx = merged[m_idx]
                exemplar_cursor = active_cursors[exemplar_idx]
                acc = exemplar_cursor.get_row_accessor()

                # Zero out the temporary AoS buffer
                k = 0
                while k < stride:
                    tmp_row[k] = "\x00"
                    k += 1

                # Materialize columnar row into temporary AoS buffer for writing
                heap_ptr = NULL_PTR
                col_idx = 0
                while col_idx < num_cols:
                    if col_idx == schema.pk_index:
                        col_idx += 1
                        continue

                    col_def = schema.columns[col_idx]
                    col_ptr = acc.get_col_ptr(col_idx)
                    dest_off = schema.get_column_offset(col_idx)
                    dest_ptr = rffi.ptradd(tmp_row, dest_off)

                    sz = col_def.field_type.size
                    b = 0
                    while b < sz:
                        dest_ptr[b] = col_ptr[b]
                        b += 1

                    # Extract heap pointer if this is a string column to allow relocation
                    if col_def.field_type == types.TYPE_STRING:
                        _, _, _, h_ptr, _ = acc.get_str_struct(col_idx)
                        heap_ptr = h_ptr
                    col_idx += 1

                writer.add_row(min_key, weight, tmp_row, heap_ptr)
                m_idx += 1

            tree.advance_min_cursors(min_key)

        writer.finalize(output_file)
        lltype.free(tmp_row, flavor="raw")
    finally:
        if tree:
            tree.close()
        v_idx = 0
        while v_idx < len(views):
            v = views[v_idx]
            if v:
                v.close()
            v_idx += 1


def execute_compaction(
    shard_index, manifest_mgr, output_dir=".", validate_checksums=False
):
    """
    High-level orchestrator for table-scoped compaction using the unified ShardIndex.
    """
    handles = shard_index.handles
    if not handles:
        return None

    table_id = shard_index.table_id
    schema = shard_index.schema
    num_h = len(handles)
    input_files = newlist_hint(num_h)

    # Track the aggregate LSN range for the new Guard Shard
    true_min_lsn = handles[0].min_lsn
    true_max_lsn = handles[0].lsn

    h_idx = 0
    while h_idx < num_h:
        h = handles[h_idx]
        input_files.append(h.filename)
        if h.min_lsn < true_min_lsn:
            true_min_lsn = h.min_lsn
        if h.lsn > true_max_lsn:
            true_max_lsn = h.lsn
        h_idx += 1

    lsn_tag = intmask(true_max_lsn)
    out_filename = os.path.join(output_dir, "compacted_%d_%d.db" % (table_id, lsn_tag))

    try:
        # 1. Perform physical merge
        compact_shards(
            input_files,
            out_filename,
            schema,
            table_id,
            validate_checksums=validate_checksums,
        )

        # 2. Create new handle for the resulting Guard Shard
        new_handle = index.ShardHandle(
            out_filename,
            schema,
            true_min_lsn,
            true_max_lsn,
            validate_checksums=validate_checksums,
        )

        # 3. Update the Index (Replaces handles and releases locks)
        shard_index.replace_handles(input_files, new_handle)

        # 4. Update the Manifest Authority
        manifest_mgr.publish_new_version(shard_index.get_metadata_list(), true_max_lsn)

        # 5. Cleanup physical files
        f_idx = 0
        while f_idx < len(input_files):
            shard_index.ref_counter.mark_for_deletion(input_files[f_idx])
            f_idx += 1
        shard_index.ref_counter.try_cleanup()

    except Exception as e:
        if os.path.exists(out_filename):
            os.unlink(out_filename)
        raise e

    return out_filename
