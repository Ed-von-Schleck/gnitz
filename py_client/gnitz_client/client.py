"""High-level GnitzDB client over IPC v2."""

from gnitz_client.protocol import (
    MAGIC_V2, HEADER_SIZE, ALIGNMENT, STATUS_OK, STATUS_ERROR,
    IPC_STRING_STRIDE,
    align_up, pack_header, unpack_header,
)
from gnitz_client.types import (
    TypeCode, ColumnDef, Schema, META_SCHEMA,
    SCHEMA_TAB, TABLE_TAB, COL_TAB, SEQ_TAB,
    SEQ_ID_SCHEMAS, SEQ_ID_TABLES, OWNER_KIND_TABLE,
    SCHEMA_TAB_SCHEMA, TABLE_TAB_SCHEMA, COL_TAB_SCHEMA, SEQ_TAB_SCHEMA,
    TYPE_STRIDES,
)
from gnitz_client.batch import (
    ZSetBatch, encode_zset_section, decode_zset_section,
    schema_to_batch, batch_to_schema,
)
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

    def create_schema(self, name: str) -> int:
        """Create a schema by pushing to system tables."""
        # 1. Read current schema HWM from SeqTab
        _, seq_batch = self.scan(SEQ_TAB)
        old_hwm = self._find_seq_val(seq_batch, SEQ_ID_SCHEMAS)
        new_sid = old_hwm + 1

        # 2. Push schema record to SchemaTab
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

        # 3. Advance sequence
        self._advance_seq(SEQ_ID_SCHEMAS, old_hwm, new_sid)
        return new_sid

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        columns: list[ColumnDef],
        pk_col_idx: int = 0,
    ) -> int:
        """Create a table by pushing columns + table record to system tables."""
        # 1. Read current table HWM from SeqTab
        _, seq_batch = self.scan(SEQ_TAB)
        old_hwm = self._find_seq_val(seq_batch, SEQ_ID_TABLES)
        new_tid = old_hwm + 1

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

        # 5. Advance sequence
        self._advance_seq(SEQ_ID_TABLES, old_hwm, new_tid)
        return new_tid

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
