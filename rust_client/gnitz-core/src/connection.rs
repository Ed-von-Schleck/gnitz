use std::os::unix::io::RawFd;
use gnitz_protocol::{
    Message, Schema, ZSetBatch,
    STATUS_ERROR, FLAG_SEEK, FLAG_SEEK_BY_INDEX, send_message, recv_message,
    connect as proto_connect, close_fd,
};
use crate::error::ClientError;

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

    pub fn roundtrip(
        &self,
        target_id: u64,
        flags:     u64,
        schema:    Option<&Schema>,
        data:      Option<&ZSetBatch>,
    ) -> Result<Message, ClientError> {
        send_message(self.sock_fd, target_id, self.client_id, flags, 0, 0, 0, schema, data)?;
        let msg = recv_message(self.sock_fd, None)?;

        if msg.status == STATUS_ERROR {
            return Err(ClientError::ServerError(
                msg.error_text.unwrap_or_else(|| "unknown server error".into())
            ));
        }
        Ok(msg)
    }

    pub fn roundtrip_seek_by_index(
        &self,
        table_id: u64,
        col_idx:  u64,
        key_lo:   u64,
        key_hi:   u64,
    ) -> Result<Message, ClientError> {
        send_message(self.sock_fd, table_id, self.client_id, FLAG_SEEK_BY_INDEX, key_lo, key_hi, col_idx, None, None)?;
        let msg = recv_message(self.sock_fd, None)?;
        if msg.status == STATUS_ERROR {
            return Err(ClientError::ServerError(
                msg.error_text.unwrap_or_else(|| "unknown server error".into())
            ));
        }
        Ok(msg)
    }

    pub fn roundtrip_seek(
        &self,
        target_id: u64,
        pk_lo:     u64,
        pk_hi:     u64,
    ) -> Result<Message, ClientError> {
        send_message(self.sock_fd, target_id, self.client_id, FLAG_SEEK, pk_lo, pk_hi, 0, None, None)?;
        let msg = recv_message(self.sock_fd, None)?;
        if msg.status == STATUS_ERROR {
            return Err(ClientError::ServerError(
                msg.error_text.unwrap_or_else(|| "unknown server error".into())
            ));
        }
        Ok(msg)
    }
}
