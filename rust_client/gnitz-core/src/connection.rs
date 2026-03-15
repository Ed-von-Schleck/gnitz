use std::os::unix::io::RawFd;
use gnitz_protocol::{
    Header, Message, Schema, ZSetBatch,
    STATUS_ERROR, FLAG_SEEK, send_message, recv_message,
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
        let mut hdr = Header::default();
        hdr.target_id = target_id;
        hdr.client_id = self.client_id;
        hdr.flags     = flags;

        send_message(self.sock_fd, hdr, schema, None, data)?;
        let msg = recv_message(self.sock_fd, None)?;

        if msg.header.status == STATUS_ERROR {
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
        let mut hdr = Header::default();
        hdr.target_id  = target_id;
        hdr.client_id  = self.client_id;
        hdr.flags      = FLAG_SEEK;
        hdr.seek_pk_lo = pk_lo;
        hdr.seek_pk_hi = pk_hi;
        send_message(self.sock_fd, hdr, None, None, None)?;
        let msg = recv_message(self.sock_fd, None)?;
        if msg.header.status == STATUS_ERROR {
            return Err(ClientError::ServerError(
                msg.error_text.unwrap_or_else(|| "unknown server error".into())
            ));
        }
        Ok(msg)
    }
}
