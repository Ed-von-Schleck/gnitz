# gnitz/storage/compactor.py

import os
from rpython.rlib import jit
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from gnitz.storage import shard_table, writer_table, tournament_tree, index, cursor
from gnitz.core import types, comparator as core_comparator


NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


def compact_shards(
    input_files, output_file, schema, table_id=0, validate_checksums=False
):
    """
    Executes an N-way merge compaction of overlapping shards.

    This implementation uses the payload-aware TournamentTree to automatically
    group identical (PrimaryKey, Payload) records across all input shards.
    This eliminates the need for manual pairwise comparisons in the hot loop.
    """
    num_inputs = len(input_files)
    views = newlist_hint(num_inputs)
    cursors = newlist_hint(num_inputs)

    tree = None
    writer = None
    tmp_row = NULL_PTR

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
        # The tree now strictly requires the schema to perform (PK, Payload) sorting.
        tree = tournament_tree.TournamentTree(cursors, schema)
        writer = writer_table.TableShardWriter(schema, table_id)

        stride = schema.memtable_stride
        tmp_row = lltype.malloc(rffi.CCHARP.TO, stride, flavor="raw")

        # 3. Process records in (PrimaryKey, Payload) order
        while not tree.is_exhausted():
            min_key = tree.get_min_key()

            # get_all_indices_at_min() returns all cursors matching BOTH
            # Primary Key and Payload. This replaces the O(N^2) pairwise
            # payload scanning logic previously found in merge_row_contributions.
            indices = tree.get_all_indices_at_min()

            # Coalesce weights for this specific (PK, Payload) group
            net_weight = r_int64(0)
            i = 0
            num_indices = len(indices)
            while i < num_indices:
                net_weight += cursors[indices[i]].weight()
                i += 1

            # Only preserve records with a non-zero net weight (Ghost Property)
            if net_weight != 0:
                exemplar_cursor = cursors[indices[0]]
                acc = exemplar_cursor.get_row_accessor()

                # Zero out the temporary AoS buffer
                k = 0
                while k < stride:
                    tmp_row[k] = "\x00"
                    k += 1

                # Materialize columnar row into temporary AoS buffer for writing
                heap_ptr = NULL_PTR
                num_cols = len(schema.columns)
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

                    # Extract heap pointer for string columns to enable zero-copy relocation.
                    # All strings in a single shard share the same blob buffer pointer.
                    if col_def.field_type.code == types.TYPE_STRING.code:
                        _, _, _, h_ptr, _ = acc.get_str_struct(col_idx)
                        heap_ptr = h_ptr
                    col_idx += 1

                writer.add_row(min_key, net_weight, tmp_row, heap_ptr)

            # Advance all cursors in the processed group
            i = 0
            while i < num_indices:
                tree.advance_cursor_by_index(indices[i])
                i += 1

        writer.finalize(output_file)
    finally:
        if tmp_row:
            lltype.free(tmp_row, flavor="raw")
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
