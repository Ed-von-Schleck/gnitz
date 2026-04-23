//! Wire protocol: IPC message codec, schema conversion, encode/decode.

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, type_size, encode_german_string, decode_german_string};
use crate::storage::Batch;
use crate::util::align8;

// ---------------------------------------------------------------------------
// Constants re-exported from gnitz_wire
// ---------------------------------------------------------------------------

pub use gnitz_wire::{
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA, IPC_CONTROL_TID,
    STATUS_OK, STATUS_ERROR, META_FLAG_NULLABLE, META_FLAG_IS_PK,
    FLAG_CONFLICT_MODE_PRESENT as FLAG_CONFLICT_MODE_PRESENT_U64,
    WireConflictMode,
};

/// Engine-side u32 alias for the cross-crate conflict-mode marker bit.
pub const FLAG_CONFLICT_MODE_PRESENT: u32 = FLAG_CONFLICT_MODE_PRESENT_U64 as u32;

#[inline]
pub fn decode_conflict_mode(flags: u64, seek_col_idx: u64) -> WireConflictMode {
    if flags & FLAG_CONFLICT_MODE_PRESENT_U64 != 0 {
        WireConflictMode::from_u8((seek_col_idx & 0xFF) as u8)
    } else {
        WireConflictMode::Update
    }
}

pub const FLAG_BATCH_SORTED: u64 = 1 << 50;
pub const FLAG_BATCH_CONSOLIDATED: u64 = 1 << 51;

// ---------------------------------------------------------------------------
// Internal schema descriptors for the wire control and schema blocks
// ---------------------------------------------------------------------------

const ZERO_COL: SchemaColumn = SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 };
const U64_COL: SchemaColumn = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
const STR_COL: SchemaColumn = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };
const STR_COL_NULL: SchemaColumn = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 1, _pad: 0 };

pub(crate) const META_SCHEMA_DESC: SchemaDescriptor = {
    let mut sd = SchemaDescriptor { num_columns: 4, pk_index: 0, columns: [ZERO_COL; 64] };
    sd.columns[0] = U64_COL;
    sd.columns[1] = U64_COL;
    sd.columns[2] = U64_COL;
    sd.columns[3] = STR_COL;
    sd
};

const _: () = assert!(
    META_SCHEMA_DESC.num_columns == 4,
    "META_SCHEMA layout changed; update schema_to_batch and decode_schema_block",
);

pub(crate) const CONTROL_SCHEMA_DESC: SchemaDescriptor = {
    let mut sd = SchemaDescriptor {
        num_columns: gnitz_wire::control::NUM_COLUMNS as u32,
        pk_index: 0,
        columns: [ZERO_COL; 64],
    };
    sd.columns[0] = U64_COL; sd.columns[1] = U64_COL; sd.columns[2] = U64_COL;
    sd.columns[3] = U64_COL; sd.columns[4] = U64_COL; sd.columns[5] = U64_COL;
    sd.columns[6] = U64_COL; sd.columns[7] = U64_COL; sd.columns[8] = U64_COL;
    sd.columns[9] = STR_COL_NULL;
    sd
};

// ---------------------------------------------------------------------------
// Private WAL-sizing helpers (replaces wal::block_size / schema_wal_block_size)
// ---------------------------------------------------------------------------

fn wal_block_size(num_regions: usize, region_sizes: &[u32]) -> usize {
    let mut pos = gnitz_wire::WAL_HEADER_SIZE + num_regions * 8;
    for i in 0..num_regions {
        pos = align8(pos);
        pos += region_sizes[i] as usize;
    }
    pos
}

fn schema_wal_block_size(schema: &SchemaDescriptor, row_count: usize, blob_size: usize) -> usize {
    let pk_idx = schema.pk_index as usize;
    let num_payload = schema.num_columns as usize - 1;
    let num_regions = 4 + num_payload + 1;
    let mut sizes = [0u32; 128];
    for i in 0..4 { sizes[i] = (8 * row_count) as u32; }
    let mut pi = 0;
    for ci in 0..schema.num_columns as usize {
        if ci == pk_idx { continue; }
        sizes[4 + pi] = (schema.columns[ci].size as usize * row_count) as u32;
        pi += 1;
    }
    sizes[4 + pi] = blob_size as u32;
    wal_block_size(num_regions, &sizes[..num_regions])
}

// ---------------------------------------------------------------------------
// Schema ↔ batch conversion
// ---------------------------------------------------------------------------

pub(crate) fn schema_to_batch(schema: &SchemaDescriptor, col_names: &[&[u8]]) -> Batch {
    let ncols = schema.num_columns as usize;
    let meta = META_SCHEMA_DESC;
    let mut batch = Batch::with_schema(meta, ncols);

    for ci in 0..ncols {
        let col = &schema.columns[ci];
        let mut flags: u64 = 0;
        if col.nullable != 0 { flags |= META_FLAG_NULLABLE; }
        if ci == schema.pk_index as usize { flags |= META_FLAG_IS_PK; }

        let type_code_val = col.type_code as u64;
        let name = if ci < col_names.len() { col_names[ci] } else { b"" };
        let name_st = encode_german_string(name, &mut batch.blob);

        batch.extend_pk_lo(&(ci as u64).to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &type_code_val.to_le_bytes());
        batch.extend_col(1, &flags.to_le_bytes());
        batch.extend_col(2, &name_st);
        batch.count += 1;
    }
    batch
}

#[cfg(test)]
pub(crate) fn batch_to_schema(batch: &Batch) -> Result<(SchemaDescriptor, Vec<Vec<u8>>), &'static str> {
    if batch.count == 0 { return Err("empty schema batch"); }
    if batch.count > 64 { return Err("schema exceeds 64-column limit"); }
    let mut sd = SchemaDescriptor {
        num_columns: batch.count as u32,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    let mut names = Vec::with_capacity(batch.count);
    let mut pk_found = false;
    for i in 0..batch.count {
        let off8 = i * 8;
        let type_code_val = u64::from_le_bytes(
            batch.col_data(0)[off8..off8 + 8].try_into().unwrap()
        ) as u8;
        let flags_val = u64::from_le_bytes(
            batch.col_data(1)[off8..off8 + 8].try_into().unwrap()
        );
        let off16 = i * 16;
        let mut st = [0u8; 16];
        st.copy_from_slice(&batch.col_data(2)[off16..off16 + 16]);
        names.push(decode_german_string(&st, &batch.blob));
        let is_nullable = (flags_val & META_FLAG_NULLABLE) != 0;
        let is_pk = (flags_val & META_FLAG_IS_PK) != 0;
        let col_size = type_size(type_code_val);
        sd.columns[i] = SchemaColumn {
            type_code: type_code_val,
            size: col_size,
            nullable: if is_nullable { 1 } else { 0 },
            _pad: 0,
        };
        if is_pk {
            if pk_found { return Err("multiple PK columns"); }
            sd.pk_index = i as u32;
            pk_found = true;
        }
    }
    if !pk_found { return Err("no PK column"); }
    Ok((sd, names))
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// Compute total encoded wire size without allocating.
pub fn wire_size(
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
) -> usize {
    let has_data = data_batch.map(|b| b.count > 0).unwrap_or(false);
    let has_schema = has_data || (schema.is_some() && status == STATUS_OK);

    let ctrl_blob = if error_msg.len() > gnitz_wire::SHORT_STRING_THRESHOLD {
        error_msg.len()
    } else { 0 };
    let mut total = schema_wal_block_size(&CONTROL_SCHEMA_DESC, 1, ctrl_blob);

    if has_schema {
        let s = schema.unwrap_or_else(|| data_batch.unwrap().schema.as_ref().unwrap());
        let names = col_names.unwrap_or(&[]);
        let ncols = s.num_columns as usize;
        let schema_blob: usize = names.iter().take(ncols)
            .map(|n| if n.len() > gnitz_wire::SHORT_STRING_THRESHOLD { n.len() } else { 0 })
            .sum();
        total += schema_wal_block_size(&META_SCHEMA_DESC, ncols, schema_blob);
    }

    if has_data {
        total += data_batch.unwrap().wire_byte_size(0);
    }
    total
}

/// Encode a full IPC wire message. Returns heap-allocated Vec<u8>.
#[cfg(test)]
pub fn encode_wire(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk_lo: u64,
    seek_pk_hi: u64,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
) -> Vec<u8> {
    let sz = wire_size(status, error_msg, schema, col_names, data_batch);
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf, 0, target_id, client_id, flags,
        seek_pk_lo, seek_pk_hi, seek_col_idx, request_id,
        status, error_msg, schema, col_names, data_batch,
    );
    buf
}

/// Encode a full IPC wire message into a caller-provided buffer.
/// Returns bytes written. Panics if the buffer is too small.
pub fn encode_wire_into(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk_lo: u64,
    seek_pk_hi: u64,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
) -> usize {
    let has_data = data_batch.map(|b| b.count > 0).unwrap_or(false);
    let has_schema = has_data || (schema.is_some() && status == STATUS_OK);

    let mut wire_flags = flags;
    if has_schema { wire_flags |= FLAG_HAS_SCHEMA; }
    if has_data {
        wire_flags |= FLAG_HAS_DATA;
        let b = data_batch.unwrap();
        if b.sorted { wire_flags |= FLAG_BATCH_SORTED; }
        if b.consolidated { wire_flags |= FLAG_BATCH_CONSOLIDATED; }
    }

    let ctrl_batch = {
        use gnitz_wire::control as ctrl;
        let cs = CONTROL_SCHEMA_DESC;
        let mut b = Batch::with_schema(cs, 1);
        let has_error = !error_msg.is_empty();
        let null_word: u64 = if has_error { 0 } else { ctrl::NULL_BIT_ERROR_MSG };
        b.extend_pk_lo(&0u64.to_le_bytes());
        b.extend_pk_hi(&0u64.to_le_bytes());
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_STATUS,       &(status as u64).to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_CLIENT_ID,    &client_id.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_TARGET_ID,    &target_id.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_FLAGS,        &wire_flags.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_SEEK_PK_LO,   &seek_pk_lo.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_SEEK_PK_HI,   &seek_pk_hi.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_SEEK_COL_IDX, &seek_col_idx.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_REQUEST_ID,   &request_id.to_le_bytes());
        let error_st = if has_error {
            encode_german_string(error_msg, &mut b.blob)
        } else {
            [0u8; 16]
        };
        b.extend_col(ctrl::PAYLOAD_ERROR_MSG, &error_st);
        b.count = 1;
        b
    };
    let written = ctrl_batch.encode_to_wire(IPC_CONTROL_TID as u32, out, offset);
    let mut pos = offset + written;

    if has_schema {
        let eff_schema = if let Some(s) = schema { s } else {
            data_batch.unwrap().schema.as_ref().unwrap()
        };
        let names = col_names.unwrap_or(&[]);
        let schema_batch = schema_to_batch(eff_schema, names);
        let written = schema_batch.encode_to_wire(target_id as u32, out, pos);
        pos += written;
    }

    if has_data {
        let written = data_batch.unwrap().encode_to_wire(target_id as u32, out, pos);
        pos += written;
    }

    pos - offset
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Decoded control fields from a wire message.
pub struct DecodedControl {
    pub status: u32,
    pub client_id: u64,
    pub target_id: u64,
    pub flags: u64,
    pub seek_pk_lo: u64,
    pub seek_pk_hi: u64,
    pub seek_col_idx: u64,
    pub request_id: u64,
    pub error_msg: Vec<u8>,
}

/// Full decoded wire message.
pub struct DecodedWire {
    pub control: DecodedControl,
    pub schema: Option<SchemaDescriptor>,
    pub data_batch: Option<Batch>,
    /// Backing buffer keeping `data_batch`'s region pointers live (W2M path).
    pub batch_backing: Option<Vec<u8>>,
}

fn decode_control_block(data: &[u8]) -> Result<DecodedControl, &'static str> {
    use gnitz_wire::control as ctrl;

    let (batch, _) = Batch::decode_from_wal_block(data, &CONTROL_SCHEMA_DESC)
        .map_err(|_| "control block invalid")?;
    if batch.count != 1 {
        return Err("control block must have 1 row");
    }

    let read_u64_col = |pi: usize| -> u64 {
        u64::from_le_bytes(batch.col_data(pi)[0..8].try_into().unwrap())
    };

    let null_bmp     = batch.get_null_word(0);
    let status       = read_u64_col(ctrl::PAYLOAD_STATUS) as u32;
    let client_id    = read_u64_col(ctrl::PAYLOAD_CLIENT_ID);
    let target_id    = read_u64_col(ctrl::PAYLOAD_TARGET_ID);
    let flags        = read_u64_col(ctrl::PAYLOAD_FLAGS);
    let seek_pk_lo   = read_u64_col(ctrl::PAYLOAD_SEEK_PK_LO);
    let seek_pk_hi   = read_u64_col(ctrl::PAYLOAD_SEEK_PK_HI);
    let seek_col_idx = read_u64_col(ctrl::PAYLOAD_SEEK_COL_IDX);
    let request_id   = read_u64_col(ctrl::PAYLOAD_REQUEST_ID);

    let error_is_null = (null_bmp & ctrl::NULL_BIT_ERROR_MSG) != 0;
    let error_msg = if error_is_null {
        Vec::new()
    } else {
        let mut st = [0u8; 16];
        st.copy_from_slice(&batch.col_data(ctrl::PAYLOAD_ERROR_MSG)[0..16]);
        decode_german_string(&st, &batch.blob)
    };

    Ok(DecodedControl {
        status, client_id, target_id, flags,
        seek_pk_lo, seek_pk_hi, seek_col_idx, request_id, error_msg,
    })
}

fn schemas_layout_equal(a: &SchemaDescriptor, b: &SchemaDescriptor) -> bool {
    if a.num_columns != b.num_columns || a.pk_index != b.pk_index { return false; }
    for i in 0..a.num_columns as usize {
        if a.columns[i].type_code != b.columns[i].type_code
            || a.columns[i].nullable != b.columns[i].nullable
        {
            return false;
        }
    }
    true
}

fn decode_schema_block(data: &[u8]) -> Result<SchemaDescriptor, &'static str> {
    let (batch, _) = Batch::decode_from_wal_block(data, &META_SCHEMA_DESC)
        .map_err(|_| "schema block invalid")?;
    if batch.count == 0 { return Err("empty schema block"); }
    if batch.count > 64 { return Err("schema exceeds 64-column limit"); }

    let n = batch.count;
    let type_data  = batch.col_data(0);
    let flags_data = batch.col_data(1);

    let mut sd = SchemaDescriptor {
        num_columns: n as u32,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    let mut pk_found = false;

    for i in 0..n {
        let off8 = i * 8;
        let tc = u64::from_le_bytes(type_data[off8..off8+8].try_into().unwrap()) as u8;
        let fl = u64::from_le_bytes(flags_data[off8..off8+8].try_into().unwrap());
        let is_nullable = (fl & META_FLAG_NULLABLE) != 0;
        let is_pk       = (fl & META_FLAG_IS_PK)       != 0;
        sd.columns[i] = SchemaColumn {
            type_code: tc,
            size: type_size(tc),
            nullable: if is_nullable { 1 } else { 0 },
            _pad: 0,
        };
        if is_pk {
            if pk_found { return Err("multiple PK columns"); }
            sd.pk_index = i as u32;
            pk_found = true;
        }
    }
    if !pk_found { return Err("no PK column"); }
    Ok(sd)
}

/// Peek at just the `target_id` from a wire message's control block.
pub fn peek_target_id(data: &[u8]) -> Result<i64, &'static str> {
    if data.len() < gnitz_wire::WAL_HEADER_SIZE {
        return Err("IPC payload too small");
    }
    let ctrl_size = u32::from_le_bytes(
        data[16..20].try_into().map_err(|_| "bad control size")?
    ) as usize;
    if ctrl_size > data.len() {
        return Err("control block truncated");
    }
    let ctrl = decode_control_block(&data[..ctrl_size])?;
    Ok(ctrl.target_id as i64)
}

/// Decode a wire message using a caller-provided schema.
pub fn decode_wire_with_schema(data: &[u8], schema: &SchemaDescriptor) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, Some(schema))
}

/// Decode a full IPC wire message from raw bytes.
pub fn decode_wire(data: &[u8]) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, None)
}

fn decode_wire_impl(
    data: &[u8],
    schema_hint: Option<&SchemaDescriptor>,
) -> Result<DecodedWire, &'static str> {
    if data.len() < gnitz_wire::WAL_HEADER_SIZE {
        return Err("IPC payload too small");
    }

    let ctrl_size = u32::from_le_bytes(
        data[16..20].try_into().map_err(|_| "bad control size")?
    ) as usize;
    if ctrl_size > data.len() {
        return Err("control block truncated");
    }

    let control = decode_control_block(&data[..ctrl_size])?;
    let flags = control.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data   = (flags & FLAG_HAS_DATA)   != 0;

    if has_data && !has_schema {
        return Err("FLAG_HAS_DATA without FLAG_HAS_SCHEMA");
    }

    let mut off = ctrl_size;
    let mut wire_schema: Option<SchemaDescriptor> = None;

    if has_schema {
        if off + gnitz_wire::WAL_HEADER_SIZE > data.len() {
            return Err("schema block truncated");
        }
        let schema_size = u32::from_le_bytes(
            data[off + 16..off + 20].try_into().map_err(|_| "bad schema size")?
        ) as usize;
        if off + schema_size > data.len() {
            return Err("schema block truncated");
        }
        let parsed = decode_schema_block(&data[off..off + schema_size])?;
        if let Some(hint) = schema_hint {
            if !schemas_layout_equal(&parsed, hint) {
                return Err("schema mismatch: client schema differs from server schema");
            }
            wire_schema = Some(*hint);
        } else {
            wire_schema = Some(parsed);
        }
        off += schema_size;
    }

    let data_batch = if has_data {
        let eff_schema = wire_schema.as_ref().ok_or("no schema for data block")?;
        if off + gnitz_wire::WAL_HEADER_SIZE > data.len() {
            return Err("data block truncated");
        }
        let data_size = u32::from_le_bytes(
            data[off + 16..off + 20].try_into().map_err(|_| "bad data size")?
        ) as usize;
        if off + data_size > data.len() {
            return Err("data block truncated");
        }
        let (mut batch, _) = Batch::decode_from_wal_block(&data[off..off + data_size], eff_schema)?;
        if (flags & FLAG_BATCH_SORTED) != 0 { batch.mark_sorted(); }
        if (flags & FLAG_BATCH_CONSOLIDATED) != 0 { batch.mark_consolidated(); }
        Some(batch)
    } else {
        None
    };

    Ok(DecodedWire { control, schema: wire_schema, data_batch, batch_backing: None })
}
