# gnitz/storage/compactor.py
#
# Shard compaction backed by Rust (libgnitz_engine).

import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import errors
from gnitz.storage import engine_ffi


def _pack_schema(schema):
    """Pack a TableSchema into a flat C buffer matching Rust SchemaDescriptor."""
    buf = lltype.malloc(rffi.CCHARP.TO, engine_ffi.SCHEMA_DESC_SIZE, flavor="raw")
    # Zero it
    i = 0
    while i < engine_ffi.SCHEMA_DESC_SIZE:
        buf[i] = '\x00'
        i += 1
    # num_columns (u32 at offset 0)
    num_cols = len(schema.columns)
    rffi.cast(rffi.UINTP, buf)[0] = rffi.cast(rffi.UINT, num_cols)
    # pk_index (u32 at offset 4)
    rffi.cast(rffi.UINTP, rffi.ptradd(buf, 4))[0] = rffi.cast(rffi.UINT, schema.pk_index)
    # columns: 4 bytes each (type_code, size, nullable, pad) starting at offset 8
    ci = 0
    while ci < num_cols:
        col = schema.columns[ci]
        base = 8 + ci * 4
        buf[base] = chr(col.field_type.code)
        buf[base + 1] = chr(col.field_type.size)
        buf[base + 2] = chr(1 if col.is_nullable else 0)
        ci += 1
    return buf


def _pack_filenames(file_list):
    """Pack a list of RPython strings into a C array of char pointers."""
    n = len(file_list)
    ptrs = lltype.malloc(rffi.CCHARPP.TO, n, flavor="raw")
    strs = newlist_hint(n)
    i = 0
    while i < n:
        s = rffi.str2charp(file_list[i])
        strs.append(s)
        ptrs[i] = s
        i += 1
    return ptrs, strs, n


def _free_filenames(ptrs, strs, n):
    """Free the packed filename arrays."""
    i = 0
    while i < n:
        rffi.free_charp(strs[i])
        i += 1
    lltype.free(ptrs, flavor="raw")


def compact_shards(
    input_files, output_file, schema, table_id=0, validate_checksums=False
):
    """Executes an N-way merge compaction of overlapping shards."""
    schema_buf = _pack_schema(schema)
    in_ptrs, in_strs, n_inputs = _pack_filenames(input_files)
    out_str = rffi.str2charp(output_file)
    try:
        rc = engine_ffi._compact_shards(
            in_ptrs,
            rffi.cast(rffi.UINT, n_inputs),
            out_str,
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.UINT, table_id),
        )
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError("Compaction failed (error %d)" % rc_int)
    finally:
        rffi.free_charp(out_str)
        _free_filenames(in_ptrs, in_strs, n_inputs)
        lltype.free(schema_buf, flavor="raw")


def _merge_and_route(input_files, guard_keys, output_dir, table_id, level_num, lsn_tag,
                     schema, validate_checksums=False):
    """N-way merge of input_files, routing each row to the appropriate guard writer."""
    num_guards = len(guard_keys)
    schema_buf = _pack_schema(schema)
    in_ptrs, in_strs, n_inputs = _pack_filenames(input_files)
    dir_str = rffi.str2charp(output_dir)

    # Pack guard keys into flat u64 array: [lo0, hi0, lo1, hi1, ...]
    gk_buf = lltype.malloc(rffi.ULONGLONGP.TO, num_guards * 2, flavor="raw")
    i = 0
    while i < num_guards:
        gk_lo, gk_hi = guard_keys[i]
        gk_buf[i * 2] = rffi.cast(rffi.ULONGLONG, gk_lo)
        gk_buf[i * 2 + 1] = rffi.cast(rffi.ULONGLONG, gk_hi)
        i += 1

    # Allocate output results buffer
    max_results = num_guards
    results_buf = lltype.malloc(
        rffi.CCHARP.TO, max_results * engine_ffi.GUARD_RESULT_SIZE, flavor="raw"
    )

    try:
        rc = engine_ffi._merge_and_route(
            in_ptrs,
            rffi.cast(rffi.UINT, n_inputs),
            dir_str,
            gk_buf,
            rffi.cast(rffi.UINT, num_guards),
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.UINT, table_id),
            rffi.cast(rffi.UINT, level_num),
            rffi.cast(rffi.ULONGLONG, lsn_tag),
            rffi.cast(rffi.VOIDP, results_buf),
            rffi.cast(rffi.UINT, max_results),
        )
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError("Merge and route failed (error %d)" % rc_int)

        # Unpack results
        guard_outputs = newlist_hint(rc_int)
        ri = 0
        while ri < rc_int:
            base = rffi.ptradd(results_buf, ri * engine_ffi.GUARD_RESULT_SIZE)
            gk_lo = r_uint64(rffi.cast(rffi.ULONGLONGP, base)[0])
            gk_hi = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 8))[0])
            # Read filename (null-terminated, starting at offset 16)
            fn_chars = newlist_hint(256)
            j = 0
            while j < 256:
                ch = base[16 + j]
                if ch == '\x00':
                    break
                fn_chars.append(ch)
                j += 1
            guard_outputs.append((gk_lo, gk_hi, "".join(fn_chars)))
            ri += 1
        return guard_outputs
    finally:
        rffi.free_charp(dir_str)
        _free_filenames(in_ptrs, in_strs, n_inputs)
        lltype.free(schema_buf, flavor="raw")
        lltype.free(gk_buf, flavor="raw")
        lltype.free(results_buf, flavor="raw")


def _find_guard_for_key(guard_keys, key):
    """Binary search: index of the rightmost guard with guard_key <= key.
    Rows below the first guard key always map to index 0."""
    from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
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


def compact_guard_horizontal(guard, output_path, schema, table_id, validate_checksums=False):
    """Merges all files within a single guard into one output file. Zero-weight rows dropped."""
    input_files = newlist_hint(len(guard.handles))
    for h in guard.handles:
        input_files.append(h.filename)
    compact_shards(input_files, output_path, schema, table_id, validate_checksums)


def compact_guard_vertical(src_guard, dest_guards, output_dir, schema, table_id,
                           dest_level_num, lsn_tag, validate_checksums=False):
    """Merges src_guard's files with overlapping dest_guards' files, routing to dest boundaries."""
    all_input_files = newlist_hint(len(src_guard.handles))
    for h in src_guard.handles:
        all_input_files.append(h.filename)
    for dg in dest_guards:
        for h in dg.handles:
            all_input_files.append(h.filename)

    if len(dest_guards) > 0:
        guard_keys = newlist_hint(len(dest_guards))
        for dg in dest_guards:
            guard_keys.append((dg.guard_key_lo, dg.guard_key_hi))
    else:
        guard_keys = newlist_hint(1)
        guard_keys.append((src_guard.guard_key_lo, src_guard.guard_key_hi))

    return _merge_and_route(all_input_files, guard_keys, output_dir, table_id,
                            dest_level_num, lsn_tag, schema, validate_checksums)


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
