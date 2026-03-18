# gnitz/storage/compactor.py

import os
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_int64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import shard_table, writer_table, tournament_tree, cursor


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
        idx = 0
        while idx < num_inputs:
            filename = input_files[idx]
            view = shard_table.TableShardView(
                filename, schema, validate_checksums=validate_checksums
            )
            views.append(view)
            cursors.append(cursor.ShardCursor(view))
            idx += 1

        tree = tournament_tree.TournamentTree(cursors, schema)
        writer = writer_table.TableShardWriter(schema, table_id)

        while not tree.is_exhausted():
            min_key = tree.get_min_key()
            num_indices = tree.get_all_indices_at_min()

            net_weight = r_int64(0)
            i = 0
            while i < num_indices:
                net_weight += cursors[tree._min_indices[i]].weight()
                i += 1

            if net_weight != 0:
                exemplar_cursor = cursors[tree._min_indices[0]]
                acc = exemplar_cursor.get_accessor()
                writer.add_row_from_accessor(min_key, net_weight, acc)

            i = 0
            while i < num_indices:
                tree.advance_cursor_by_index(tree._min_indices[i])
                i += 1

        writer.finalize(output_file)
    finally:
        if tree:
            tree.close()
        v_idx = 0
        while v_idx < len(views):
            v = views[v_idx]
            if v:
                v.close()
            v_idx += 1


def _find_guard_for_key(guard_keys, key):
    """Binary search: index of the rightmost guard with guard_key <= key.
    Rows below the first guard key always map to index 0."""
    n = len(guard_keys)
    lo, hi = 0, n - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        gk_lo, gk_hi = guard_keys[mid]
        gk = (r_uint128(gk_hi) << 64) | r_uint128(gk_lo)
        if gk <= key:
            lo = mid
        else:
            hi = mid - 1
    return lo


def _merge_and_route(input_files, guard_keys, output_dir, table_id, level_num, lsn_tag,
                     schema, validate_checksums=False):
    """N-way merge of input_files, routing each row to the appropriate guard writer."""
    num_inputs = len(input_files)
    num_guards = len(guard_keys)

    views = newlist_hint(num_inputs)
    cursors_list = newlist_hint(num_inputs)
    writers = newlist_hint(num_guards)
    out_filenames = newlist_hint(num_guards)

    i = 0
    while i < num_guards:
        fn = output_dir + "/shard_%d_%d_L%d_G%d.db" % (table_id, lsn_tag, level_num, i)
        out_filenames.append(fn)
        writers.append(writer_table.TableShardWriter(schema, table_id))
        i += 1

    tree = None
    try:
        i = 0
        while i < num_inputs:
            view = shard_table.TableShardView(
                input_files[i], schema, validate_checksums=validate_checksums
            )
            views.append(view)
            cursors_list.append(cursor.ShardCursor(view))
            i += 1

        tree = tournament_tree.TournamentTree(cursors_list, schema)

        while not tree.is_exhausted():
            min_key = tree.get_min_key()
            num_indices = tree.get_all_indices_at_min()

            net_weight = r_int64(0)
            i = 0
            while i < num_indices:
                net_weight += cursors_list[tree._min_indices[i]].weight()
                i += 1

            if net_weight != 0:
                guard_idx = _find_guard_for_key(guard_keys, min_key)
                exemplar_cursor = cursors_list[tree._min_indices[0]]
                acc = exemplar_cursor.get_accessor()
                writers[guard_idx].add_row_from_accessor(min_key, net_weight, acc)

            i = 0
            while i < num_indices:
                tree.advance_cursor_by_index(tree._min_indices[i])
                i += 1
    finally:
        if tree is not None:
            tree.close()
        v_idx = 0
        while v_idx < len(views):
            v = views[v_idx]
            if v:
                v.close()
            v_idx += 1

    guard_outputs = newlist_hint(num_guards)
    i = 0
    while i < num_guards:
        w = writers[i]
        if w.count > 0:
            w.finalize(out_filenames[i])
            if os.path.exists(out_filenames[i]):
                gk_lo, gk_hi = guard_keys[i]
                guard_outputs.append((gk_lo, gk_hi, out_filenames[i]))
        else:
            w.close()
        i += 1

    return guard_outputs


def compact_l0_to_l1(flsm_index, output_dir, schema, table_id, lsn_tag,
                     validate_checksums=False):
    """Route L0 handles into L1 guards via tournament-tree merge."""
    if len(flsm_index.levels) > 0 and len(flsm_index.levels[0].guards) > 0:
        l1 = flsm_index.levels[0]
        ng = len(l1.guards)
        guard_keys = newlist_hint(ng)
        gi = 0
        while gi < ng:
            g = l1.guards[gi]
            guard_keys.append((g.guard_key_lo, g.guard_key_hi))
            gi += 1
    else:
        n_handles = len(flsm_index.handles)
        guard_keys = newlist_hint(n_handles)
        hi_idx = 0
        while hi_idx < n_handles:
            h = flsm_index.handles[hi_idx]
            if hi_idx == 0:
                guard_keys.append((h.pk_min_lo, h.pk_min_hi))
            else:
                prev = flsm_index.handles[hi_idx - 1]
                if h.pk_min_lo != prev.pk_min_lo or h.pk_min_hi != prev.pk_min_hi:
                    guard_keys.append((h.pk_min_lo, h.pk_min_hi))
            hi_idx += 1

    n_inputs = len(flsm_index.handles)
    input_files = newlist_hint(n_inputs)
    fi = 0
    while fi < n_inputs:
        input_files.append(flsm_index.handles[fi].filename)
        fi += 1

    return _merge_and_route(input_files, guard_keys, output_dir, table_id, 1, lsn_tag,
                            schema, validate_checksums)
