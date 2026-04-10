use std::os::unix::io::RawFd;
use gnitz_protocol::{
    Message, Schema, ZSetBatch,
    STATUS_ERROR, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
    FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_INDEX_ID,
    FLAG_CONFLICT_MODE_PRESENT, WireConflictMode,
    send_message, recv_message,
    connect as proto_connect, close_fd,
};
use crate::error::ClientError;

pub use gnitz_wire::{
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, IDX_TAB, DEP_TAB, SEQ_TAB,
    FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID,
};

fn check_response(msg: Message) -> Result<Message, ClientError> {
    if msg.status == STATUS_ERROR {
        return Err(ClientError::ServerError(
            msg.error_text.unwrap_or_else(|| "unknown server error".into())
        ));
    }
    Ok(msg)
}

pub struct Connection {
    sock_fd:       RawFd,
    pub client_id: u64,
}

impl Connection {
    pub fn connect(socket_path: &str) -> Result<Self, ClientError> {
        let sock_fd = proto_connect(socket_path)?;
        Ok(Connection { sock_fd, client_id: std::process::id() as u64 })
    }

    pub fn close(self) {
        close_fd(self.sock_fd);
    }

    pub fn alloc_table_id(&self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_TABLE_ID, None, None)?;
        Ok(msg.target_id)
    }

    pub fn alloc_schema_id(&self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_SCHEMA_ID, None, None)?;
        Ok(msg.target_id)
    }

    pub fn alloc_index_id(&self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_INDEX_ID, None, None)?;
        Ok(msg.target_id)
    }

    /// Default push: silent-upsert (`WireConflictMode::Update`).
    /// Callers that need SQL-standard rejection use `push_with_mode`.
    pub fn push(
        &self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
    ) -> Result<u64, ClientError> {
        self.push_with_mode(target_id, schema, batch, WireConflictMode::Update)
    }

    pub fn push_with_mode(
        &self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
        mode:      WireConflictMode,
    ) -> Result<u64, ClientError> {
        let msg = self.roundtrip_push(target_id, schema, batch, mode)?;
        Ok(msg.seek_pk_lo)
    }

    pub fn scan(
        &self,
        target_id: u64,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let msg = self.roundtrip(target_id, 0, None, None)?;
        Ok((msg.schema, msg.data_batch, msg.seek_pk_lo))
    }

    pub fn seek(
        &self,
        target_id: u64,
        pk_lo:     u64,
        pk_hi:     u64,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let msg = self.roundtrip_seek(target_id, pk_lo, pk_hi)?;
        Ok((msg.schema, msg.data_batch, msg.seek_pk_lo))
    }

    pub fn seek_by_index(
        &self,
        table_id: u64,
        col_idx:  u64,
        key_lo:   u64,
        key_hi:   u64,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let msg = self.roundtrip_seek_by_index(table_id, col_idx, key_lo, key_hi)?;
        Ok((msg.schema, msg.data_batch, msg.seek_pk_lo))
    }

    fn roundtrip(
        &self,
        target_id: u64,
        flags:     u64,
        schema:    Option<&Schema>,
        data:      Option<&ZSetBatch>,
    ) -> Result<Message, ClientError> {
        send_message(self.sock_fd, target_id, self.client_id, flags, 0, 0, 0, schema, data)?;
        let msg = recv_message(self.sock_fd, None)?;
        check_response(msg)
    }

    /// Push path: encodes the requested `WireConflictMode` into the
    /// control block by setting `FLAG_CONFLICT_MODE_PRESENT` and
    /// packing the mode byte into the low byte of `seek_col_idx`.
    fn roundtrip_push(
        &self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
        mode:      WireConflictMode,
    ) -> Result<Message, ClientError> {
        let flags = FLAG_CONFLICT_MODE_PRESENT;
        let seek_col_idx = mode.as_u8() as u64;
        send_message(
            self.sock_fd, target_id, self.client_id, flags,
            0, 0, seek_col_idx,
            Some(schema), Some(batch),
        )?;
        let msg = recv_message(self.sock_fd, None)?;
        check_response(msg)
    }

    fn roundtrip_seek_by_index(
        &self,
        table_id: u64,
        col_idx:  u64,
        key_lo:   u64,
        key_hi:   u64,
    ) -> Result<Message, ClientError> {
        send_message(self.sock_fd, table_id, self.client_id, FLAG_SEEK_BY_INDEX, key_lo, key_hi, col_idx, None, None)?;
        let msg = recv_message(self.sock_fd, None)?;
        check_response(msg)
    }

    fn roundtrip_seek(
        &self,
        target_id: u64,
        pk_lo:     u64,
        pk_hi:     u64,
    ) -> Result<Message, ClientError> {
        send_message(self.sock_fd, target_id, self.client_id, FLAG_SEEK, pk_lo, pk_hi, 0, None, None)?;
        let msg = recv_message(self.sock_fd, None)?;
        check_response(msg)
    }
}
