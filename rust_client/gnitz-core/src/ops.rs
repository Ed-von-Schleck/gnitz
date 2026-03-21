use gnitz_protocol::{Schema, ZSetBatch, FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_INDEX_ID, batch_to_schema};
use crate::connection::Connection;
use crate::error::ClientError;
use gnitz_protocol::Message;

pub const SCHEMA_TAB:           u64 = 1;
pub const TABLE_TAB:            u64 = 2;
pub const VIEW_TAB:             u64 = 3;
pub const COL_TAB:              u64 = 4;
pub const IDX_TAB:              u64 = 5;
pub const DEP_TAB:              u64 = 6;
pub const SEQ_TAB:              u64 = 7;
pub const FIRST_USER_TABLE_ID:  u64 = 16;
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

pub fn alloc_table_id(conn: &Connection) -> Result<u64, ClientError> {
    let msg = conn.roundtrip(0, FLAG_ALLOCATE_TABLE_ID, None, None)?;
    Ok(msg.target_id)
}

pub fn alloc_schema_id(conn: &Connection) -> Result<u64, ClientError> {
    let msg = conn.roundtrip(0, FLAG_ALLOCATE_SCHEMA_ID, None, None)?;
    Ok(msg.target_id)
}

pub fn alloc_index_id(conn: &Connection) -> Result<u64, ClientError> {
    let msg = conn.roundtrip(0, FLAG_ALLOCATE_INDEX_ID, None, None)?;
    Ok(msg.target_id)
}

fn decode_scan_response(msg: Message) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
    let view_lsn = msg.seek_pk_lo;
    let schema = msg.schema_batch
        .as_ref()
        .map(batch_to_schema)
        .transpose()
        .map_err(ClientError::Protocol)?;
    Ok((schema, msg.data_batch, view_lsn))
}

pub fn seek_by_index(
    conn:     &Connection,
    table_id: u64,
    col_idx:  u64,
    key_lo:   u64,
    key_hi:   u64,
) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
    let msg = conn.roundtrip_seek_by_index(table_id, col_idx, key_lo, key_hi)?;
    decode_scan_response(msg)
}

pub fn push(
    conn:      &Connection,
    target_id: u64,
    schema:    &Schema,
    batch:     &ZSetBatch,
) -> Result<u64, ClientError> {
    let msg = conn.roundtrip(target_id, 0, Some(schema), Some(batch))?;
    Ok(msg.seek_pk_lo)   // ingest_lsn from server ACK
}

pub fn scan(
    conn:      &Connection,
    target_id: u64,
) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
    let msg = conn.roundtrip(target_id, 0, None, None)?;
    decode_scan_response(msg)
}

pub fn seek(
    conn:      &Connection,
    target_id: u64,
    pk_lo:     u64,
    pk_hi:     u64,
) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
    let msg = conn.roundtrip_seek(target_id, pk_lo, pk_hi)?;
    decode_scan_response(msg)
}
