# gnitz/catalog/engine.py
#
# Thin wrapper around the Rust CatalogEngine (opaque FFI handle).
# All catalog logic — DDL, hooks, entity registry, bootstrap/recovery,
# FK validation, view/index backfill — lives in Rust.

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import intmask

from gnitz.core.errors import LayoutError
from gnitz.storage import engine_ffi


class CatalogHandle(object):
    """Thin wrapper around Rust CatalogEngine opaque handle."""

    _immutable_fields_ = ["_handle", "dag_handle", "base_dir"]

    def __init__(self, handle, dag_handle, base_dir):
        self._handle = handle
        self.dag_handle = dag_handle
        self.base_dir = base_dir

    def close(self):
        engine_ffi._catalog_close(self._handle)


def open_engine(base_dir, memtable_arena_size=1 * 1024 * 1024):
    """Open or create a GnitzDB instance via Rust CatalogEngine."""
    buf = rffi.str2charp(base_dir)
    try:
        handle = engine_ffi._catalog_open(
            rffi.cast(rffi.VOIDP, buf),
            rffi.cast(rffi.UINT, len(base_dir)),
        )
    finally:
        rffi.free_charp(buf)
    if not handle:
        raise LayoutError("Failed to open catalog engine")
    dag_handle = engine_ffi._catalog_get_dag_handle(handle)
    return CatalogHandle(handle, dag_handle, base_dir)


# ---------------------------------------------------------------------------
# DDL helpers — called from executor.py
# ---------------------------------------------------------------------------

def _check_ffi_result(rc, context=""):
    """Raise LayoutError if a catalog FFI call returned an error."""
    if intmask(rc) < 0:
        out_len = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
        try:
            err_ptr = engine_ffi._catalog_last_error(out_len)
            elen = intmask(out_len[0])
            if elen > 0 and err_ptr:
                msg = rffi.charpsize2str(rffi.cast(rffi.CCHARP, err_ptr), elen)
            else:
                msg = context or "catalog FFI error"
        finally:
            lltype.free(out_len, flavor="raw")
        raise LayoutError(msg)


def catalog_create_schema(handle, name):
    buf = rffi.str2charp(name)
    try:
        rc = engine_ffi._catalog_create_schema(
            handle, rffi.cast(rffi.VOIDP, buf), rffi.cast(rffi.UINT, len(name)),
        )
    finally:
        rffi.free_charp(buf)
    _check_ffi_result(rc, "create_schema")


def catalog_drop_schema(handle, name):
    buf = rffi.str2charp(name)
    try:
        rc = engine_ffi._catalog_drop_schema(
            handle, rffi.cast(rffi.VOIDP, buf), rffi.cast(rffi.UINT, len(name)),
        )
    finally:
        rffi.free_charp(buf)
    _check_ffi_result(rc, "drop_schema")


def catalog_create_table(handle, qualified_name, columns, pk_col_idx, unique_pk):
    """Serialize ColumnDefinitions and call Rust create_table."""
    # Serialize col_defs: [name_len(u32) + name(bytes) + type_code(u8)
    #                      + is_nullable(u8) + fk_table_id(i64) + fk_col_idx(u32)]
    import struct
    parts = []
    for col in columns:
        name_bytes = col.name.encode("utf-8") if isinstance(col.name, unicode) else col.name
        parts.append(struct.pack("<I", len(name_bytes)))
        parts.append(name_bytes)
        parts.append(struct.pack("<BB", col.field_type.code, int(col.is_nullable)))
        parts.append(struct.pack("<qI", col.fk_table_id, col.fk_col_idx))
    col_buf = "".join(parts)

    name_buf = rffi.str2charp(qualified_name)
    col_raw = rffi.str2charp(col_buf)
    try:
        tid = engine_ffi._catalog_create_table(
            handle,
            rffi.cast(rffi.VOIDP, name_buf),
            rffi.cast(rffi.UINT, len(qualified_name)),
            rffi.cast(rffi.VOIDP, col_raw),
            rffi.cast(rffi.UINT, len(col_buf)),
            rffi.cast(rffi.UINT, len(columns)),
            rffi.cast(rffi.UINT, pk_col_idx),
            rffi.cast(rffi.INT, int(unique_pk)),
        )
    finally:
        rffi.free_charp(name_buf)
        rffi.free_charp(col_raw)
    if intmask(tid) < 0:
        _check_ffi_result(rffi.cast(rffi.INT, -1), "create_table")
    return intmask(tid)


def catalog_drop_table(handle, qualified_name):
    buf = rffi.str2charp(qualified_name)
    try:
        rc = engine_ffi._catalog_drop_table(
            handle, rffi.cast(rffi.VOIDP, buf), rffi.cast(rffi.UINT, len(qualified_name)),
        )
    finally:
        rffi.free_charp(buf)
    _check_ffi_result(rc, "drop_table")


def catalog_create_view(handle, qualified_name, graph, sql_definition):
    """Serialize CircuitGraph and call Rust create_view."""
    import struct

    name_buf = rffi.str2charp(qualified_name)
    sql_buf = rffi.str2charp(sql_definition)

    # Nodes: [nid, opcode] pairs as i32 flat array
    nodes_data = lltype.nullptr(rffi.INTP.TO)
    nodes_count = len(graph.nodes)
    if nodes_count > 0:
        nodes_data = lltype.malloc(rffi.INTP.TO, nodes_count * 2, flavor="raw")
        for i in range(nodes_count):
            nid, opcode = graph.nodes[i]
            nodes_data[i * 2] = rffi.cast(rffi.INT, nid)
            nodes_data[i * 2 + 1] = rffi.cast(rffi.INT, opcode)

    # Edges: [eid, src, dst, port] quads
    edges_data = lltype.nullptr(rffi.INTP.TO)
    edges_count = len(graph.edges)
    if edges_count > 0:
        edges_data = lltype.malloc(rffi.INTP.TO, edges_count * 4, flavor="raw")
        for i in range(edges_count):
            eid, src, dst, port = graph.edges[i]
            edges_data[i * 4] = rffi.cast(rffi.INT, eid)
            edges_data[i * 4 + 1] = rffi.cast(rffi.INT, src)
            edges_data[i * 4 + 2] = rffi.cast(rffi.INT, dst)
            edges_data[i * 4 + 3] = rffi.cast(rffi.INT, port)

    # Sources: [nid, table_id] pairs as i64
    sources_data = lltype.nullptr(rffi.LONGLONGP.TO)
    sources_count = len(graph.sources)
    if sources_count > 0:
        sources_data = lltype.malloc(rffi.LONGLONGP.TO, sources_count * 2, flavor="raw")
        for i in range(sources_count):
            nid, tid = graph.sources[i]
            sources_data[i * 2] = rffi.cast(rffi.LONGLONG, nid)
            sources_data[i * 2 + 1] = rffi.cast(rffi.LONGLONG, tid)

    # Params: [nid, slot, value] triples as i64
    params_data = lltype.nullptr(rffi.LONGLONGP.TO)
    params_count = len(graph.params)
    if params_count > 0:
        params_data = lltype.malloc(rffi.LONGLONGP.TO, params_count * 3, flavor="raw")
        for i in range(params_count):
            nid, slot, val = graph.params[i]
            params_data[i * 3] = rffi.cast(rffi.LONGLONG, nid)
            params_data[i * 3 + 1] = rffi.cast(rffi.LONGLONG, slot)
            params_data[i * 3 + 2] = rffi.cast(rffi.LONGLONG, val)

    # Group cols: [nid, col_idx] pairs as i32
    gcols_data = lltype.nullptr(rffi.INTP.TO)
    gcols_count = len(graph.group_cols)
    if gcols_count > 0:
        gcols_data = lltype.malloc(rffi.INTP.TO, gcols_count * 2, flavor="raw")
        for i in range(gcols_count):
            nid, cidx = graph.group_cols[i]
            gcols_data[i * 2] = rffi.cast(rffi.INT, nid)
            gcols_data[i * 2 + 1] = rffi.cast(rffi.INT, cidx)

    # Output col defs: serialized as [name_len(u32) + name(bytes) + type_code(u8)]
    ocd_parts = []
    for col_name, type_code in graph.output_col_defs:
        name_bytes = col_name.encode("utf-8") if isinstance(col_name, unicode) else col_name
        ocd_parts.append(struct.pack("<I", len(name_bytes)))
        ocd_parts.append(name_bytes)
        ocd_parts.append(struct.pack("<B", type_code))
    ocd_buf_str = "".join(ocd_parts)
    ocd_buf = rffi.str2charp(ocd_buf_str)
    ocd_count = len(graph.output_col_defs)

    # Dependencies
    deps_data = lltype.nullptr(rffi.LONGLONGP.TO)
    deps_count = len(graph.dependencies)
    if deps_count > 0:
        deps_data = lltype.malloc(rffi.LONGLONGP.TO, deps_count, flavor="raw")
        for i in range(deps_count):
            deps_data[i] = rffi.cast(rffi.LONGLONG, graph.dependencies[i])

    try:
        vid = engine_ffi._catalog_create_view(
            handle,
            rffi.cast(rffi.VOIDP, name_buf), rffi.cast(rffi.UINT, len(qualified_name)),
            rffi.cast(rffi.VOIDP, sql_buf), rffi.cast(rffi.UINT, len(sql_definition)),
            nodes_data, rffi.cast(rffi.UINT, nodes_count),
            edges_data, rffi.cast(rffi.UINT, edges_count),
            sources_data, rffi.cast(rffi.UINT, sources_count),
            params_data, rffi.cast(rffi.UINT, params_count),
            gcols_data, rffi.cast(rffi.UINT, gcols_count),
            rffi.cast(rffi.VOIDP, ocd_buf), rffi.cast(rffi.UINT, len(ocd_buf_str)), rffi.cast(rffi.UINT, ocd_count),
            deps_data, rffi.cast(rffi.UINT, deps_count),
        )
    finally:
        rffi.free_charp(name_buf)
        rffi.free_charp(sql_buf)
        rffi.free_charp(ocd_buf)
        if nodes_data:
            lltype.free(nodes_data, flavor="raw")
        if edges_data:
            lltype.free(edges_data, flavor="raw")
        if sources_data:
            lltype.free(sources_data, flavor="raw")
        if params_data:
            lltype.free(params_data, flavor="raw")
        if gcols_data:
            lltype.free(gcols_data, flavor="raw")
        if deps_data:
            lltype.free(deps_data, flavor="raw")

    if intmask(vid) < 0:
        _check_ffi_result(rffi.cast(rffi.INT, -1), "create_view")
    return intmask(vid)


def catalog_drop_view(handle, qualified_name):
    buf = rffi.str2charp(qualified_name)
    try:
        rc = engine_ffi._catalog_drop_view(
            handle, rffi.cast(rffi.VOIDP, buf), rffi.cast(rffi.UINT, len(qualified_name)),
        )
    finally:
        rffi.free_charp(buf)
    _check_ffi_result(rc, "drop_view")


def catalog_create_index(handle, qualified_owner, col_name, is_unique):
    owner_buf = rffi.str2charp(qualified_owner)
    col_buf = rffi.str2charp(col_name)
    try:
        iid = engine_ffi._catalog_create_index(
            handle,
            rffi.cast(rffi.VOIDP, owner_buf), rffi.cast(rffi.UINT, len(qualified_owner)),
            rffi.cast(rffi.VOIDP, col_buf), rffi.cast(rffi.UINT, len(col_name)),
            rffi.cast(rffi.INT, int(is_unique)),
        )
    finally:
        rffi.free_charp(owner_buf)
        rffi.free_charp(col_buf)
    if intmask(iid) < 0:
        _check_ffi_result(rffi.cast(rffi.INT, -1), "create_index")
    return intmask(iid)


def catalog_drop_index(handle, index_name):
    buf = rffi.str2charp(index_name)
    try:
        rc = engine_ffi._catalog_drop_index(
            handle, rffi.cast(rffi.VOIDP, buf), rffi.cast(rffi.UINT, len(index_name)),
        )
    finally:
        rffi.free_charp(buf)
    _check_ffi_result(rc, "drop_index")


def catalog_advance_sequence(handle, seq_id, old_val, new_val):
    engine_ffi._catalog_advance_sequence(
        handle,
        rffi.cast(rffi.LONGLONG, seq_id),
        rffi.cast(rffi.LONGLONG, old_val),
        rffi.cast(rffi.LONGLONG, new_val),
    )
