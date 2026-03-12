"""High-level GnitzDB client over IPC v2."""

from gnitz_client.protocol import (
    MAGIC_V2, HEADER_SIZE, ALIGNMENT, STATUS_OK, STATUS_ERROR,
    IPC_STRING_STRIDE, FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID,
    align_up, pack_header, unpack_header,
)
from gnitz_client.types import (
    TypeCode, ColumnDef, Schema, META_SCHEMA,
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, DEP_TAB, SEQ_TAB,
    SEQ_ID_SCHEMAS, SEQ_ID_TABLES,
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    SCHEMA_TAB_SCHEMA, TABLE_TAB_SCHEMA, VIEW_TAB_SCHEMA,
    COL_TAB_SCHEMA, DEP_TAB_SCHEMA, SEQ_TAB_SCHEMA,
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_SOURCES_TAB,
    CIRCUIT_PARAMS_TAB, CIRCUIT_GROUP_COLS_TAB,
    CIRCUIT_NODES_SCHEMA, CIRCUIT_EDGES_SCHEMA, CIRCUIT_SOURCES_SCHEMA,
    CIRCUIT_PARAMS_SCHEMA, CIRCUIT_GROUP_COLS_SCHEMA,
    OPCODE_FILTER, OPCODE_MAP, OPCODE_INTEGRATE, OPCODE_SCAN_TRACE,
    PORT_IN, PORT_TRACE,
    TYPE_STRIDES,
)
from gnitz_client.batch import (
    ZSetBatch, encode_zset_section, decode_zset_section,
    schema_to_batch, batch_to_schema,
)
from gnitz_client.circuit import CircuitGraph
from gnitz_client import transport


class GnitzError(Exception):
    """Raised when the server returns STATUS_ERROR."""
    pass


def _pack_column_id(owner_id: int, col_idx: int) -> int:
    """Synthetic PK for ColTab: (owner_id << 9) | col_idx."""
    return (owner_id << 9) | col_idx


class GnitzClient:
    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self.sock = transport.connect(socket_path)

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def _send(
        self,
        target_id: int,
        schema: Schema | None = None,
        batch: ZSetBatch | None = None,
        client_id: int = 0,
        flags: int = 0,
        status: int = STATUS_OK,
        error_msg: str = "",
    ):
        err_bytes = error_msg.encode("utf-8") if error_msg else b""
        err_len = len(err_bytes)

        # Encode schema section
        schema_section = b""
        schema_count = 0
        schema_blob_sz = 0
        if schema is not None and batch is not None and len(batch.pk_lo) > 0:
            schema_batch = schema_to_batch(schema)
            schema_section, schema_blob_sz = encode_zset_section(META_SCHEMA, schema_batch)
            schema_count = len(schema_batch.pk_lo)

        # Encode data section
        data_section = b""
        data_count = 0
        data_blob_sz = 0
        data_pk_index = 0
        if schema is not None and batch is not None and len(batch.pk_lo) > 0:
            data_section, data_blob_sz = encode_zset_section(schema, batch)
            data_count = len(batch.pk_lo)
            data_pk_index = schema.pk_index

        # Build header
        header = pack_header(
            magic=MAGIC_V2,
            status=status,
            err_len=err_len,
            target_id=target_id,
            client_id=client_id,
            schema_count=schema_count,
            schema_blob_sz=schema_blob_sz,
            data_count=data_count,
            data_blob_sz=data_blob_sz,
            data_pk_index=data_pk_index,
            flags=flags,
        )

        # Assemble message
        body_start = align_up(HEADER_SIZE + err_len, ALIGNMENT)
        err_padding = body_start - HEADER_SIZE - err_len

        schema_section_padded = b""
        if schema_section:
            data_section_start = align_up(body_start + len(schema_section), ALIGNMENT)
            schema_pad = data_section_start - body_start - len(schema_section)
            schema_section_padded = schema_section + b'\x00' * schema_pad
        else:
            data_section_start = body_start

        msg = header + err_bytes + b'\x00' * err_padding + schema_section_padded + data_section

        # Pad to at least HEADER_SIZE
        if len(msg) < HEADER_SIZE:
            msg = msg + b'\x00' * (HEADER_SIZE - len(msg))

        transport.send_memfd(self.sock, msg)

    def _receive(self) -> tuple[dict, Schema | None, ZSetBatch | None]:
        data = transport.recv_memfd(self.sock)
        if len(data) < HEADER_SIZE:
            raise GnitzError("Response too small for header")

        hdr = unpack_header(data)
        if hdr["magic"] != MAGIC_V2:
            raise GnitzError("Invalid magic in response")

        err_len = hdr["err_len"]
        if hdr["status"] == STATUS_ERROR:
            error_msg = data[HEADER_SIZE:HEADER_SIZE + err_len].decode("utf-8")
            raise GnitzError(error_msg)

        body_start = align_up(HEADER_SIZE + err_len, ALIGNMENT)

        # Parse schema section
        wire_schema = None
        schema_section_end = body_start
        schema_count = hdr["schema_count"]

        if schema_count > 0:
            schema_batch = decode_zset_section(
                data, body_start, META_SCHEMA, schema_count, hdr["schema_blob_sz"],
            )
            wire_schema = batch_to_schema(schema_batch)
            # Compute schema section size to find data section
            schema_section_size = _compute_section_size(META_SCHEMA, schema_count, hdr["schema_blob_sz"])
            schema_section_end = align_up(body_start + schema_section_size, ALIGNMENT)

        # Parse data section
        zbatch = None
        data_count = hdr["data_count"]
        if data_count > 0 and wire_schema is not None:
            zbatch = decode_zset_section(
                data, schema_section_end, wire_schema, data_count, hdr["data_blob_sz"],
            )

        return hdr, wire_schema, zbatch

    def scan(self, target_id: int) -> tuple[Schema | None, ZSetBatch | None]:
        """Send empty request, receive snapshot."""
        self._send(target_id)
        hdr, schema, batch = self._receive()
        return schema, batch

    def push(self, target_id: int, schema: Schema, batch: ZSetBatch):
        """Push data to a target, check for errors."""
        self._send(target_id, schema=schema, batch=batch)
        hdr, resp_schema, resp_batch = self._receive()
        return resp_schema, resp_batch

    def allocate_table_id(self) -> int:
        """Ask server to atomically allocate a table/view ID."""
        self._send(target_id=0, flags=FLAG_ALLOCATE_TABLE_ID)
        hdr, _, _ = self._receive()
        return hdr["target_id"]

    def allocate_schema_id(self) -> int:
        """Ask server to atomically allocate a schema ID."""
        self._send(target_id=0, flags=FLAG_ALLOCATE_SCHEMA_ID)
        hdr, _, _ = self._receive()
        return hdr["target_id"]

    def create_schema(self, name: str) -> int:
        """Create a schema by pushing to system tables."""
        new_sid = self.allocate_schema_id()

        schema_s = SCHEMA_TAB_SCHEMA
        b = ZSetBatch(
            schema=schema_s,
            pk_lo=[new_sid],
            pk_hi=[0],
            weights=[1],
            nulls=[0],
            columns=[[], [name]],  # col0=pk(empty), col1=name
        )
        self.push(SCHEMA_TAB, schema_s, b)
        return new_sid

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        columns: list[ColumnDef],
        pk_col_idx: int = 0,
    ) -> int:
        """Create a table by pushing columns + table record to system tables."""
        new_tid = self.allocate_table_id()

        # 2. Find schema_id by scanning SchemaTab
        _, schema_batch = self.scan(SCHEMA_TAB)
        schema_id = self._find_schema_id(schema_batch, schema_name)

        # 3. Push column records to ColTab
        col_schema = COL_TAB_SCHEMA
        col_batch = ZSetBatch(schema=col_schema)
        for i, col in enumerate(columns):
            col_pk = _pack_column_id(new_tid, i)
            col_batch.pk_lo.append(col_pk)
            col_batch.pk_hi.append(0)
            col_batch.weights.append(1)
            col_batch.nulls.append(0)

        # Build column data lists - 9 columns, pk_index=0
        # col0 = pk (empty), col1=owner_id, col2=owner_kind, col3=col_idx,
        # col4=name, col5=type_code, col6=is_nullable, col7=fk_table_id, col8=fk_col_idx
        pk_col: list = []
        owner_ids = []
        owner_kinds = []
        col_idxs = []
        names = []
        type_codes = []
        is_nullables = []
        fk_table_ids = []
        fk_col_idxs = []
        for i, col in enumerate(columns):
            pk_col.append(None)
            owner_ids.append(new_tid)
            owner_kinds.append(OWNER_KIND_TABLE)
            col_idxs.append(i)
            names.append(col.name)
            type_codes.append(col.type_code)
            is_nullables.append(1 if col.is_nullable else 0)
            fk_table_ids.append(0)
            fk_col_idxs.append(0)
        col_batch.columns = [pk_col, owner_ids, owner_kinds, col_idxs, names,
                             type_codes, is_nullables, fk_table_ids, fk_col_idxs]
        self.push(COL_TAB, col_schema, col_batch)

        # 4. Push table record to TableTab
        # Directory is derived server-side from base_dir; client sends empty.
        directory = ""

        table_s = TABLE_TAB_SCHEMA
        tb = ZSetBatch(
            schema=table_s,
            pk_lo=[new_tid],
            pk_hi=[0],
            weights=[1],
            nulls=[0],
            columns=[[], [schema_id], [table_name], [directory], [pk_col_idx], [0]],
        )
        self.push(TABLE_TAB, table_s, tb)

        return new_tid

    def drop_schema(self, name: str):
        """Drop a schema by retracting from SchemaTab."""
        _, schema_batch = self.scan(SCHEMA_TAB)
        schema_id = self._find_schema_id(schema_batch, name)

        schema_s = SCHEMA_TAB_SCHEMA
        b = ZSetBatch(
            schema=schema_s,
            pk_lo=[schema_id],
            pk_hi=[0],
            weights=[-1],
            nulls=[0],
            columns=[[], [name]],
        )
        self.push(SCHEMA_TAB, schema_s, b)

    def drop_table(self, schema_name: str, table_name: str):
        """Drop a table by retracting from TableTab and ColTab."""
        # Find table record
        _, tbl_batch = self.scan(TABLE_TAB)
        tid, sid, directory, pk_col_idx, created_lsn = self._find_table_record(
            tbl_batch, schema_name, table_name
        )

        # Find and retract column records
        _, col_batch = self.scan(COL_TAB)
        self._retract_columns_for_owner(col_batch, tid, OWNER_KIND_TABLE)

        # Retract table record
        table_s = TABLE_TAB_SCHEMA
        tb = ZSetBatch(
            schema=table_s,
            pk_lo=[tid],
            pk_hi=[0],
            weights=[-1],
            nulls=[0],
            columns=[[], [sid], [table_name], [directory], [pk_col_idx], [created_lsn]],
        )
        self.push(TABLE_TAB, table_s, tb)

    def create_view(
        self,
        schema_name: str,
        view_name: str,
        source_table_id: int,
        output_columns: list[ColumnDef],
    ) -> int:
        """Create a minimal passthrough view on a source table.

        This builds the simplest possible circuit graph:
          node 0: SCAN_TRACE (primary input, table_id=0)
          node 1: INTEGRATE (sink, target=view_id)
          edge 0: node 0 -> node 1, port 0
        """
        vid = self.allocate_table_id()

        # Find schema_id
        _, schema_batch = self.scan(SCHEMA_TAB)
        sid = self._find_schema_id(schema_batch, schema_name)

        # 1. Column records
        col_schema = COL_TAB_SCHEMA
        col_batch = ZSetBatch(schema=col_schema)
        for i, col in enumerate(output_columns):
            col_pk = _pack_column_id(vid, i)
            col_batch.pk_lo.append(col_pk)
            col_batch.pk_hi.append(0)
            col_batch.weights.append(1)
            col_batch.nulls.append(0)
        pk_col: list = []
        owner_ids = []
        owner_kinds = []
        col_idxs = []
        names = []
        type_codes = []
        is_nullables = []
        fk_table_ids = []
        fk_col_idxs = []
        for i, col in enumerate(output_columns):
            pk_col.append(None)
            owner_ids.append(vid)
            owner_kinds.append(OWNER_KIND_VIEW)
            col_idxs.append(i)
            names.append(col.name)
            type_codes.append(col.type_code)
            is_nullables.append(1 if col.is_nullable else 0)
            fk_table_ids.append(0)
            fk_col_idxs.append(0)
        col_batch.columns = [pk_col, owner_ids, owner_kinds, col_idxs, names,
                             type_codes, is_nullables, fk_table_ids, fk_col_idxs]
        self.push(COL_TAB, col_schema, col_batch)

        # 2. Dependency records — U128 PK: pk_hi=vid, pk_lo=dep_tid
        dep_s = DEP_TAB_SCHEMA
        dep_batch = ZSetBatch(
            schema=dep_s,
            pk_lo=[source_table_id],
            pk_hi=[vid],
            weights=[1],
            nulls=[0],
            columns=[[], [vid], [0], [source_table_id]],
        )
        self.push(DEP_TAB, dep_s, dep_batch)

        # 3. Circuit graph: 2 nodes, 1 edge, 1 source, 1 param
        # U128 PKs: pk_hi = view_id (high 64), pk_lo = node_id (low 64)
        nodes_s = CIRCUIT_NODES_SCHEMA
        nodes_batch = ZSetBatch(
            schema=nodes_s,
            pk_lo=[0, 1],       # node_id 0, 1
            pk_hi=[vid, vid],   # view_id in high 64 bits
            weights=[1, 1],
            nulls=[0, 0],
            columns=[[], [OPCODE_SCAN_TRACE, OPCODE_INTEGRATE]],
        )
        self.push(CIRCUIT_NODES_TAB, nodes_s, nodes_batch)

        edges_s = CIRCUIT_EDGES_SCHEMA
        edges_batch = ZSetBatch(
            schema=edges_s,
            pk_lo=[0],       # edge_id 0
            pk_hi=[vid],
            weights=[1],
            nulls=[0],
            columns=[[], [0], [1], [PORT_IN]],
        )
        self.push(CIRCUIT_EDGES_TAB, edges_s, edges_batch)

        sources_s = CIRCUIT_SOURCES_SCHEMA
        sources_batch = ZSetBatch(
            schema=sources_s,
            pk_lo=[0],       # node_id 0
            pk_hi=[vid],
            weights=[1],
            nulls=[0],
            columns=[[], [0]],  # table_id=0 means primary input
        )
        self.push(CIRCUIT_SOURCES_TAB, sources_s, sources_batch)

        # 4. View record (triggers hook)
        view_s = VIEW_TAB_SCHEMA
        vb = ZSetBatch(
            schema=view_s,
            pk_lo=[vid],
            pk_hi=[0],
            weights=[1],
            nulls=[0],
            columns=[[], [sid], [view_name], [""], [""], [0]],
        )
        self.push(VIEW_TAB, view_s, vb)

        return vid

    def create_view_with_circuit(
        self,
        schema_name: str,
        view_name: str,
        circuit: CircuitGraph,
        output_columns: list[ColumnDef],
    ) -> int:
        """Create a view with an arbitrary circuit graph.

        The circuit's view_id must already be allocated (via allocate_table_id).
        """
        vid = circuit.view_id

        # Find schema_id
        _, schema_batch = self.scan(SCHEMA_TAB)
        sid = self._find_schema_id(schema_batch, schema_name)

        # 1. Column records (same pattern as create_view)
        col_schema = COL_TAB_SCHEMA
        col_batch = ZSetBatch(schema=col_schema)
        for i, col in enumerate(output_columns):
            col_pk = _pack_column_id(vid, i)
            col_batch.pk_lo.append(col_pk)
            col_batch.pk_hi.append(0)
            col_batch.weights.append(1)
            col_batch.nulls.append(0)
        pk_col: list = []
        owner_ids = []
        owner_kinds = []
        col_idxs = []
        names = []
        type_codes = []
        is_nullables = []
        fk_table_ids = []
        fk_col_idxs = []
        for i, col in enumerate(output_columns):
            pk_col.append(None)
            owner_ids.append(vid)
            owner_kinds.append(OWNER_KIND_VIEW)
            col_idxs.append(i)
            names.append(col.name)
            type_codes.append(col.type_code)
            is_nullables.append(1 if col.is_nullable else 0)
            fk_table_ids.append(0)
            fk_col_idxs.append(0)
        col_batch.columns = [pk_col, owner_ids, owner_kinds, col_idxs, names,
                             type_codes, is_nullables, fk_table_ids, fk_col_idxs]
        self.push(COL_TAB, col_schema, col_batch)

        # 2. Dependency records — U128 PK: pk_hi=vid, pk_lo=dep_tid
        dep_s = DEP_TAB_SCHEMA
        if circuit.dependencies:
            dep_batch = ZSetBatch(schema=dep_s)
            dep_pks = []
            dep_vids = []
            dep_view_ids = []
            dep_table_ids = []
            for dep_tid in circuit.dependencies:
                dep_batch.pk_lo.append(dep_tid)
                dep_batch.pk_hi.append(vid)
                dep_batch.weights.append(1)
                dep_batch.nulls.append(0)
                dep_pks.append(None)
                dep_vids.append(vid)
                dep_view_ids.append(0)
                dep_table_ids.append(dep_tid)
            dep_batch.columns = [dep_pks, dep_vids, dep_view_ids, dep_table_ids]
            self.push(DEP_TAB, dep_s, dep_batch)

        # 3. Circuit nodes
        if circuit.nodes:
            nodes_s = CIRCUIT_NODES_SCHEMA
            nodes_batch = ZSetBatch(schema=nodes_s)
            opcodes_col = []
            for node_id, opcode in circuit.nodes:
                nodes_batch.pk_lo.append(node_id)
                nodes_batch.pk_hi.append(vid)
                nodes_batch.weights.append(1)
                nodes_batch.nulls.append(0)
                opcodes_col.append(opcode)
            nodes_batch.columns = [[], opcodes_col]
            self.push(CIRCUIT_NODES_TAB, nodes_s, nodes_batch)

        # 4. Circuit edges
        if circuit.edges:
            edges_s = CIRCUIT_EDGES_SCHEMA
            edges_batch = ZSetBatch(schema=edges_s)
            src_col = []
            dst_col = []
            port_col = []
            for edge_id, src, dst, port in circuit.edges:
                edges_batch.pk_lo.append(edge_id)
                edges_batch.pk_hi.append(vid)
                edges_batch.weights.append(1)
                edges_batch.nulls.append(0)
                src_col.append(src)
                dst_col.append(dst)
                port_col.append(port)
            edges_batch.columns = [[], src_col, dst_col, port_col]
            self.push(CIRCUIT_EDGES_TAB, edges_s, edges_batch)

        # 5. Circuit sources
        if circuit.sources:
            sources_s = CIRCUIT_SOURCES_SCHEMA
            sources_batch = ZSetBatch(schema=sources_s)
            table_id_col = []
            for node_id, table_id in circuit.sources:
                sources_batch.pk_lo.append(node_id)
                sources_batch.pk_hi.append(vid)
                sources_batch.weights.append(1)
                sources_batch.nulls.append(0)
                table_id_col.append(table_id)
            sources_batch.columns = [[], table_id_col]
            self.push(CIRCUIT_SOURCES_TAB, sources_s, sources_batch)

        # 6. Circuit params
        if circuit.params:
            params_s = CIRCUIT_PARAMS_SCHEMA
            params_batch = ZSetBatch(schema=params_s)
            value_col = []
            for node_id, slot, value in circuit.params:
                param_lo = (node_id << 8) | slot
                params_batch.pk_lo.append(param_lo)
                params_batch.pk_hi.append(vid)
                params_batch.weights.append(1)
                params_batch.nulls.append(0)
                value_col.append(value)
            params_batch.columns = [[], value_col]
            self.push(CIRCUIT_PARAMS_TAB, params_s, params_batch)

        # 7. Circuit group cols
        if circuit.group_cols:
            gcols_s = CIRCUIT_GROUP_COLS_SCHEMA
            gcols_batch = ZSetBatch(schema=gcols_s)
            col_idx_col = []
            for node_id, col_idx in circuit.group_cols:
                gcol_lo = (node_id << 16) | col_idx
                gcols_batch.pk_lo.append(gcol_lo)
                gcols_batch.pk_hi.append(vid)
                gcols_batch.weights.append(1)
                gcols_batch.nulls.append(0)
                col_idx_col.append(col_idx)
            gcols_batch.columns = [[], col_idx_col]
            self.push(CIRCUIT_GROUP_COLS_TAB, gcols_s, gcols_batch)

        # 8. View record (triggers hook — must be last)
        view_s = VIEW_TAB_SCHEMA
        vb = ZSetBatch(
            schema=view_s,
            pk_lo=[vid],
            pk_hi=[0],
            weights=[1],
            nulls=[0],
            columns=[[], [sid], [view_name], [""], [""], [0]],
        )
        self.push(VIEW_TAB, view_s, vb)

        return vid

    def drop_view(self, schema_name: str, view_name: str):
        """Drop a view by retracting from ViewTab, circuit tables, DepTab, ColTab."""
        # Find view record
        _, view_batch = self.scan(VIEW_TAB)
        vid, sid, sql_def, cache_dir, created_lsn = self._find_view_record(
            view_batch, schema_name, view_name
        )

        # Retract view record
        view_s = VIEW_TAB_SCHEMA
        vb = ZSetBatch(
            schema=view_s,
            pk_lo=[vid],
            pk_hi=[0],
            weights=[-1],
            nulls=[0],
            columns=[[], [sid], [view_name], [sql_def], [cache_dir], [created_lsn]],
        )
        self.push(VIEW_TAB, view_s, vb)

        # Retract circuit graph records
        self._retract_circuit_graph(vid)

        # Retract dep records
        self._retract_deps_for_view(vid)

        # Retract column records
        _, col_batch = self.scan(COL_TAB)
        self._retract_columns_for_owner(col_batch, vid, OWNER_KIND_VIEW)

    def _find_table_record(
        self, batch: ZSetBatch | None, schema_name: str, table_name: str
    ) -> tuple[int, int, str, int, int]:
        """Find a table record by schema + name in TableTab scan."""
        if batch is None:
            raise GnitzError(f"Table '{schema_name}.{table_name}' not found")
        # We need schema_id for the schema_name
        _, schema_batch = self.scan(SCHEMA_TAB)
        sid = self._find_schema_id(schema_batch, schema_name)

        for i in range(len(batch.pk_lo)):
            if batch.weights[i] > 0:
                if batch.columns[1][i] == sid and batch.columns[2][i] == table_name:
                    tid = batch.pk_lo[i]
                    directory = batch.columns[3][i]
                    pk_col_idx = batch.columns[4][i]
                    created_lsn = batch.columns[5][i]
                    return tid, sid, directory, pk_col_idx, created_lsn
        raise GnitzError(f"Table '{schema_name}.{table_name}' not found")

    def _find_view_record(
        self, batch: ZSetBatch | None, schema_name: str, view_name: str
    ) -> tuple[int, int, str, str, int]:
        """Find a view record by schema + name in ViewTab scan."""
        if batch is None:
            raise GnitzError(f"View '{schema_name}.{view_name}' not found")
        _, schema_batch = self.scan(SCHEMA_TAB)
        sid = self._find_schema_id(schema_batch, schema_name)

        for i in range(len(batch.pk_lo)):
            if batch.weights[i] > 0:
                if batch.columns[1][i] == sid and batch.columns[2][i] == view_name:
                    vid = batch.pk_lo[i]
                    sql_def = batch.columns[3][i]
                    cache_dir = batch.columns[4][i]
                    created_lsn = batch.columns[5][i]
                    return vid, sid, sql_def, cache_dir, created_lsn
        raise GnitzError(f"View '{schema_name}.{view_name}' not found")

    def _retract_columns_for_owner(self, col_batch: ZSetBatch | None, owner_id: int, owner_kind: int):
        """Retract all column records for a given owner from ColTab."""
        if col_batch is None:
            return
        retract = ZSetBatch(schema=COL_TAB_SCHEMA)
        for i in range(len(col_batch.pk_lo)):
            if col_batch.weights[i] > 0 and col_batch.columns[1][i] == owner_id and col_batch.columns[2][i] == owner_kind:
                retract.pk_lo.append(col_batch.pk_lo[i])
                retract.pk_hi.append(0)
                retract.weights.append(-1)
                retract.nulls.append(0)
        if len(retract.pk_lo) == 0:
            return
        # Build column lists
        cols = [[] for _ in range(9)]
        for i in range(len(col_batch.pk_lo)):
            if col_batch.weights[i] > 0 and col_batch.columns[1][i] == owner_id and col_batch.columns[2][i] == owner_kind:
                for c in range(9):
                    if c == 0:
                        cols[c].append(None)
                    else:
                        cols[c].append(col_batch.columns[c][i])
        retract.columns = cols
        self.push(COL_TAB, COL_TAB_SCHEMA, retract)

    def _retract_circuit_graph(self, vid: int):
        """Retract all circuit table records for a view."""
        # Nodes
        _, nodes_batch = self.scan(CIRCUIT_NODES_TAB)
        if nodes_batch:
            self._retract_records_for_view_u128(
                CIRCUIT_NODES_TAB, CIRCUIT_NODES_SCHEMA, nodes_batch, vid
            )
        # Edges
        _, edges_batch = self.scan(CIRCUIT_EDGES_TAB)
        if edges_batch:
            self._retract_records_for_view_u128(
                CIRCUIT_EDGES_TAB, CIRCUIT_EDGES_SCHEMA, edges_batch, vid
            )
        # Sources
        _, src_batch = self.scan(CIRCUIT_SOURCES_TAB)
        if src_batch:
            self._retract_records_for_view_u128(
                CIRCUIT_SOURCES_TAB, CIRCUIT_SOURCES_SCHEMA, src_batch, vid
            )
        # Params
        _, params_batch = self.scan(CIRCUIT_PARAMS_TAB)
        if params_batch:
            self._retract_records_for_view_u128(
                CIRCUIT_PARAMS_TAB, CIRCUIT_PARAMS_SCHEMA, params_batch, vid
            )
        # Group cols
        _, gcols_batch = self.scan(CIRCUIT_GROUP_COLS_TAB)
        if gcols_batch:
            self._retract_records_for_view_u128(
                CIRCUIT_GROUP_COLS_TAB, CIRCUIT_GROUP_COLS_SCHEMA, gcols_batch, vid
            )

    def _retract_records_for_view_u128(
        self, table_id: int, schema: Schema, batch: ZSetBatch, vid: int
    ):
        """Retract all records whose U128 PK has view_id in high 64 bits (pk_hi)."""
        retract = ZSetBatch(schema=schema)
        for i in range(len(batch.pk_lo)):
            pk_view_id = batch.pk_hi[i]  # high 64 bits = view_id
            if batch.weights[i] > 0 and pk_view_id == vid:
                retract.pk_lo.append(batch.pk_lo[i])
                retract.pk_hi.append(batch.pk_hi[i])
                retract.weights.append(-1)
                retract.nulls.append(0)
        if len(retract.pk_lo) == 0:
            return
        ncols = len(schema.columns)
        cols = [[] for _ in range(ncols)]
        for i in range(len(batch.pk_lo)):
            pk_view_id = batch.pk_hi[i]
            if batch.weights[i] > 0 and pk_view_id == vid:
                for c in range(ncols):
                    if c == schema.pk_index:
                        cols[c].append(None)
                    else:
                        cols[c].append(batch.columns[c][i])
        retract.columns = cols
        self.push(table_id, schema, retract)

    def _retract_deps_for_view(self, vid: int):
        """Retract all dependency records for a view."""
        _, dep_batch = self.scan(DEP_TAB)
        if dep_batch is None:
            return
        retract = ZSetBatch(schema=DEP_TAB_SCHEMA)
        for i in range(len(dep_batch.pk_lo)):
            if dep_batch.weights[i] > 0 and dep_batch.columns[1][i] == vid:
                retract.pk_lo.append(dep_batch.pk_lo[i])
                retract.pk_hi.append(dep_batch.pk_hi[i])
                retract.weights.append(-1)
                retract.nulls.append(0)
        if len(retract.pk_lo) == 0:
            return
        cols = [[] for _ in range(4)]
        for i in range(len(dep_batch.pk_lo)):
            if dep_batch.weights[i] > 0 and dep_batch.columns[1][i] == vid:
                cols[0].append(None)
                cols[1].append(dep_batch.columns[1][i])
                cols[2].append(dep_batch.columns[2][i])
                cols[3].append(dep_batch.columns[3][i])
        retract.columns = cols
        self.push(DEP_TAB, DEP_TAB_SCHEMA, retract)

    def _advance_seq(self, seq_id: int, old_val: int, new_val: int):
        """Retract old HWM and insert new HWM in SeqTab."""
        seq_s = SEQ_TAB_SCHEMA
        b = ZSetBatch(
            schema=seq_s,
            pk_lo=[seq_id, seq_id],
            pk_hi=[0, 0],
            weights=[-1, 1],
            nulls=[0, 0],
            columns=[[], [old_val, new_val]],
        )
        self.push(SEQ_TAB, seq_s, b)

    def _find_seq_val(self, batch: ZSetBatch | None, seq_id: int) -> int:
        """Find the current value of a sequence in a SeqTab scan result."""
        if batch is None:
            return 0
        for i in range(len(batch.pk_lo)):
            if batch.pk_lo[i] == seq_id and batch.weights[i] > 0:
                return batch.columns[1][i]  # next_val column
        return 0

    def _find_schema_id(self, batch: ZSetBatch | None, name: str) -> int:
        """Find schema_id by name in a SchemaTab scan result."""
        if batch is None:
            raise GnitzError(f"Schema '{name}' not found (empty scan)")
        for i in range(len(batch.pk_lo)):
            if batch.weights[i] > 0 and batch.columns[1][i] == name:
                return batch.pk_lo[i]
        raise GnitzError(f"Schema '{name}' not found")


def _compute_section_size(schema: Schema, count: int, blob_sz: int) -> int:
    """Compute the total wire bytes for a ZSet section."""
    cur = 0
    struct_sz = count * 8

    # 4 structural buffers
    for _ in range(4):
        cur = align_up(cur + struct_sz, ALIGNMENT)

    # Payload columns
    for ci, col in enumerate(schema.columns):
        if ci == schema.pk_index:
            continue
        if col.type_code == TypeCode.STRING:
            cur = align_up(cur + count * IPC_STRING_STRIDE, ALIGNMENT)
        else:
            stride = TYPE_STRIDES[col.type_code]
            cur = align_up(cur + stride * count, ALIGNMENT)

    cur += blob_sz
    return cur
