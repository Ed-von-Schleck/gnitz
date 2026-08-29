use super::codec::{encode_schema_block, schema_from_block};
use super::error::ProtocolError;
use super::transport::ClientTransport;
use super::types::{PkTuple, Schema, ZSetBatch};
use super::wal_block::{decode_wal_block, encode_wal_block};
use super::WAL_BLOCK_HEADER_SIZE;
use super::{
    wire_flags_get_schema_version, Header, WireConflictMode, FLAG_HAS_DATA, FLAG_HAS_SCHEMA, STATUS_ERROR, STATUS_OK,
};
use crate::types::sys_schema;

#[derive(Debug, Default)]
pub struct Message {
    pub status: u32,
    pub target_id: u64,
    pub flags: u64,
    pub seek_pk: u128,
    /// The schema, `Some` iff a schema block was physically in the frame (an
    /// `Arc` so a cache absorb is a refcount bump, not a deep copy). On a
    /// hint-only continuation frame it is `None` — the caller supplied the
    /// schema out of band.
    pub schema: Option<std::sync::Arc<Schema>>,
    pub data_batch: Option<ZSetBatch>,
    pub error_text: Option<String>, // Some(_) when status == STATUS_ERROR
    /// The control block's arbitrary-length BLOB cell. On a RESOLVE reply it
    /// carries the relation-descriptor blob (`gnitz_wire::RelDescriptorBlob`);
    /// every other reply leaves it empty.
    pub seek_pk_extra: Vec<u8>,
}

// ── Control block ─────────────────────────────────────────────────────────────
//
// The control-block wire codec (layout, template encoder, directory-driven
// decoder) lives in `gnitz_wire::control` — the one implementation both the
// client and the engine run. The wrappers here adapt it to the client types:
// `Header` in/out, UTF-8 validated error text, and the checksum stamp every
// client TCP frame carries.

/// Encode a `Header` + optional error message + optional wide-PK extra bytes
/// into a control WAL block. When `error_msg` is empty the error_msg column
/// is NULL; when `seek_pk_extra` is empty the seek_pk_extra column is NULL.
pub(crate) fn encode_control_block(header: &Header, error_msg: &str, seek_pk_extra: &[u8]) -> Vec<u8> {
    let total = gnitz_wire::control::ctrl_block_size(error_msg.len(), seek_pk_extra.len());
    let mut buf = vec![0u8; total];
    // Client frames carry a body checksum, matching `encode_wal_block`.
    gnitz_wire::control::encode_ctrl_block(&mut buf, 0, header, error_msg.as_bytes(), seek_pk_extra, true);
    buf
}

/// Decode a control WAL block, returning `(Header, error_msg, seek_pk_extra)`.
/// `error_msg` is empty when the null bit for error_msg is set;
/// `seek_pk_extra` is empty when the null bit for seek_pk_extra is set.
pub fn decode_control_block(data: &[u8]) -> Result<(Header, String, Vec<u8>), ProtocolError> {
    let dc = gnitz_wire::control::peek_control_block_ipc(data).map_err(|e| ProtocolError::DecodeError(e.into()))?;
    let header = dc.header();
    let error_msg =
        String::from_utf8(dc.error_msg).map_err(|e| ProtocolError::DecodeError(format!("utf8 in error_msg: {e}")))?;
    Ok((header, error_msg, dc.seek_pk_extra))
}

/// An encoded wire message as its constituent WAL blocks — control, optional
/// schema, data (empty when no data block) — kept separate so send paths can
/// hand them to a vectored write (one length prefix over the concatenation)
/// instead of flattening into one contiguous buffer.
pub struct MessageParts {
    pub ctrl: Vec<u8>,
    pub schema: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

impl MessageParts {
    /// The blocks in wire order, for a vectored send. Empty segments are
    /// skipped by the transport.
    pub fn segments(&self) -> [&[u8]; 3] {
        [&self.ctrl, self.schema.as_deref().unwrap_or(&[]), &self.data]
    }

    /// Flatten into one contiguous payload. Tests use this to inspect a frame
    /// the send path would hand to a vectored write unflattened.
    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<u8> {
        let [a, b, c] = self.segments();
        let mut out = Vec::with_capacity(a.len() + b.len() + c.len());
        out.extend_from_slice(a);
        out.extend_from_slice(b);
        out.extend_from_slice(c);
        out
    }
}

/// The one frame encoder: control block + optional schema block + optional data
/// block, without the 4-byte frame header. `FLAG_HAS_SCHEMA` / `FLAG_HAS_DATA`
/// are derived here, so no caller sets them.
///
/// `seek_pk` / `seek_col_idx` address a SEEK; `seek_pk_extra` is the control
/// block's arbitrary-length BLOB cell. `schema_block` is what rides in the
/// frame; `data` names the schema its rows are encoded against, which a
/// warm-cache push does not ship.
#[allow(clippy::too_many_arguments)]
fn encode_parts(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    seek_pk_extra: &[u8],
    schema_block: Option<Vec<u8>>,
    data: Option<(&Schema, &ZSetBatch)>,
) -> MessageParts {
    let data = data.filter(|(_, b)| !b.is_empty());
    let mut flags_out = flags;
    if schema_block.is_some() {
        flags_out |= FLAG_HAS_SCHEMA;
    }
    if data.is_some() {
        flags_out |= FLAG_HAS_DATA;
    }
    let ctrl_hdr = Header {
        status: STATUS_OK,
        target_id,
        client_id,
        flags: flags_out,
        seek_pk,
        seek_col_idx,
        request_id: 0,
    };
    MessageParts {
        ctrl: encode_control_block(&ctrl_hdr, "", seek_pk_extra),
        schema: schema_block,
        data: data.map_or_else(Vec::new, |(s, b)| encode_wal_block(s, target_id as u32, b)),
    }
}

/// Encode a request/response carrying `schema` in the frame. Pass the parts to
/// `send_framed_iov`, or hand them to the outbound queue, for framing.
///
/// `seek_pk` carries the seek key for `FLAG_SEEK` / `FLAG_SEEK_BY_INDEX` frames;
/// pass `&PkTuple::EMPTY` for non-seek frames. The wire-level
/// `(seek_pk: u128, seek_pk_extra: BLOB)` split is performed here via
/// `PkTuple::split_wire`, so callers never handle it.
///
/// `data` pairs the rows with the schema they were encoded against, so a data
/// block without its schema is unrepresentable. The schema block is derived
/// before the empty-batch filter, so an empty Z-set delta still ships one.
pub fn encode_message_parts(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: &PkTuple,
    seek_col_idx: u64,
    data: Option<(&Schema, &ZSetBatch)>,
) -> MessageParts {
    let (seek_pk_lo, seek_pk_extra) = seek_pk.split_wire();
    encode_parts(
        target_id,
        client_id,
        flags,
        seek_pk_lo,
        seek_col_idx,
        seek_pk_extra,
        data.map(|(s, _)| encode_schema_block(s, target_id as u32)),
        data,
    )
}

/// Like [`encode_message_parts`] but omits the schema block from the frame.
/// The data block is still encoded using `data_schema`; the server
/// reconstructs the schema from its catalog (guided by the schema version in
/// `flags`). Used by warm-cache PUSH paths where the server already knows
/// the schema (version embedded in `flags` bits 24-39).
pub fn encode_message_noschema_parts(
    target_id: u64,
    client_id: u64,
    flags: u64,
    data_schema: &Schema,
    data_batch: &ZSetBatch,
) -> MessageParts {
    encode_parts(
        target_id,
        client_id,
        flags,
        0,
        0,
        &[],
        None,
        Some((data_schema, data_batch)),
    )
}

/// Encode an atomic user-table push transaction frame (`FLAG_PUSH_TXN`) into
/// wire bytes (without the 4-byte frame header). Adapts the client's
/// `(&Schema, &ZSetBatch)` families to the pre-encoded blocks the shared
/// `gnitz_wire::txn_frame` codec bundles — the frame layout itself lives there,
/// so the server cannot walk a different one.
pub fn encode_push_txn(
    client_id: u64,
    families: &[(u64, &Schema, &ZSetBatch, WireConflictMode)],
    preconditions: &[(u64, u64)],
) -> Vec<u8> {
    let blocks: Vec<(u8, Vec<u8>, Vec<u8>)> = families
        .iter()
        .map(|(tid, schema, batch, mode)| {
            (
                mode.as_wire(),
                encode_schema_block(schema, *tid as u32),
                encode_wal_block(schema, *tid as u32, batch),
            )
        })
        .collect();
    let refs: Vec<(u8, &[u8], &[u8])> = blocks.iter().map(|(m, s, d)| (*m, &s[..], &d[..])).collect();
    gnitz_wire::txn_frame::encode_push_txn(client_id, &refs, preconditions)
}

/// Encode a `FLAG_SCAN_MULTI` request frame into wire bytes (without the 4-byte
/// frame header): a consistent snapshot of N relations at one server-side SAL
/// cut. The server answers with N reply trains in this exact order; the caller
/// reads them positionally.
pub fn encode_scan_multi(client_id: u64, relations: &[(u64, u16)]) -> Vec<u8> {
    gnitz_wire::txn_frame::encode_scan_multi(client_id, relations)
}

/// Encode an atomic DDL transaction frame (`FLAG_DDL_TXN`) into wire bytes
/// (without the 4-byte frame header). Every system-table write — a `CREATE`'s N
/// family batches, a `DROP`/`CREATE INDEX`/`CREATE SCHEMA`'s single batch — is
/// carried by one such frame so the server ingests the whole bundle under one
/// durable SAL zone.
///
/// Each family is named by its system table id alone: the schema its batch is
/// encoded against is that id's, derived here through [`sys_schema`], so a
/// caller cannot pair one family's id with another's shape.
pub fn encode_ddl_txn(client_id: u64, families: &[(u64, ZSetBatch)]) -> Vec<u8> {
    let blocks: Vec<Vec<u8>> = families
        .iter()
        .map(|(tid, batch)| encode_wal_block(sys_schema(*tid), *tid as u32, batch))
        .collect();
    let refs: Vec<&[u8]> = blocks.iter().map(|b| &b[..]).collect();
    gnitz_wire::txn_frame::encode_ddl_txn(client_id, &refs)
}

/// Encode one control-only frame: no schema block, no data block.
///
/// The seek key arrives as its two raw wire halves, not as a `PkTuple`, because
/// `PkTuple::split_wire` caps the extra region at 64 bytes (`MAX_PK_BYTES - 16`)
/// and this channel also carries the SEEK_BY_INDEX_RANGE `RangeDescriptor`, up
/// to 82 bytes at max arity. The control block's BLOB column has no such cap.
pub(crate) fn encode_control_frame(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    seek_pk_extra: &[u8],
) -> Vec<u8> {
    encode_parts(
        target_id,
        client_id,
        flags,
        seek_pk,
        seek_col_idx,
        seek_pk_extra,
        None,
        None,
    )
    .ctrl
}

/// Send one control-only frame ([`encode_control_frame`]), blocking until it
/// is on the wire.
pub fn send_control(
    t: &mut ClientTransport,
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    seek_pk_extra: &[u8],
) -> Result<(), ProtocolError> {
    t.send_framed(&encode_control_frame(
        target_id,
        client_id,
        flags,
        seek_pk,
        seek_col_idx,
        seek_pk_extra,
    ))
}

/// Parse a wire payload (without 4-byte frame header) into a `Message`.
/// The payload is what `recv_framed()` returns.
///
/// `schema_hint` is `Some((schema, cached_version))` when the client has a
/// cached schema for the responding table. On continuation frames (no schema
/// block), the hint is used to decode the data block; a version mismatch is a
/// hard protocol error. Pass `None` for the initial frame or when no cache
/// entry exists.
pub fn parse_response(buf: &[u8], schema_hint: Option<(&Schema, u16)>) -> Result<Message, ProtocolError> {
    let parsed = parse_response_frame(buf, schema_hint)?;
    let data_batch = match parsed.data_block {
        Some(range) => {
            let eff: &Schema = match parsed.message.schema.as_deref() {
                Some(s) => s,
                None => schema_hint
                    .map(|(s, _)| s)
                    .ok_or_else(|| ProtocolError::DecodeError("no schema for data block".into()))?,
            };
            Some(decode_wal_block(&buf[range], eff)?.0)
        }
        None => None,
    };
    Ok(Message {
        data_batch,
        ..parsed.message
    })
}

/// A parsed reply frame whose data block has been located but not decoded.
///
/// `message.data_batch` is always `None` here: locating the block is all this
/// does, and decoding it is what [`parse_response`] adds.
pub(crate) struct ParsedFrame {
    pub(crate) message: Message,
    /// Where the data block sits in the frame buffer, or `None` when the frame
    /// carries no data.
    pub(crate) data_block: Option<std::ops::Range<usize>>,
}

/// Everything [`parse_response`] does except decoding the data block: frame
/// bounds, control block, error text, schema block and the schema-version check.
///
/// The split exists so a caller that wants the block's *bytes* — a subscriber
/// feeding them into a store, which would otherwise pay an OPK→native decode
/// and a native→OPK re-encode of every key to rebuild what the socket already
/// delivered — reaches them through the same validation the decoding entry
/// runs, rather than a second copy of it that could drift on what it checked.
pub(crate) fn parse_response_frame(
    buf: &[u8],
    schema_hint: Option<(&Schema, u16)>,
) -> Result<ParsedFrame, ProtocolError> {
    if buf.len() < WAL_BLOCK_HEADER_SIZE {
        return Err(ProtocolError::DecodeError("message too small".into()));
    }

    let ctrl = gnitz_wire::wal::block_slice_at(buf, 0)?;
    let ctrl_size = ctrl.len();
    let (ctrl_header, error_msg, seek_pk_extra) = decode_control_block(ctrl)?;

    let flags = ctrl_header.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data = (flags & FLAG_HAS_DATA) != 0;

    let mut off = ctrl_size;
    let mut wire_schema: Option<Schema> = None;

    if has_schema {
        let block = gnitz_wire::wal::block_slice_at(buf, off)?;
        off += block.len();
        wire_schema = Some(schema_from_block(block)?);
    } else if has_data {
        match schema_hint {
            None => {
                return Err(ProtocolError::DecodeError(
                    "FLAG_HAS_DATA without FLAG_HAS_SCHEMA and no cached schema".into(),
                ))
            }
            Some((_hint_schema, cached_version)) => {
                let server_version = wire_flags_get_schema_version(flags);
                if server_version != cached_version {
                    return Err(ProtocolError::DecodeError(format!(
                        "schema version mismatch: cached={cached_version} server={server_version}"
                    )));
                }
                // hint-only frame: decode against borrowed hint below; no schema carried in Message
            }
        }
    }

    let data_block = if has_data {
        let block = gnitz_wire::wal::block_slice_at(buf, off)?;
        Some(off..off + block.len())
    } else {
        None
    };

    // Every non-OK status the server emits rides a control-only frame, so both
    // blocks are already absent and there is nothing to suppress.
    let error_text = (ctrl_header.status == STATUS_ERROR).then_some(error_msg);

    Ok(ParsedFrame {
        message: Message {
            status: ctrl_header.status,
            target_id: ctrl_header.target_id,
            flags: ctrl_header.flags,
            seek_pk: ctrl_header.seek_pk,
            schema: wire_schema.map(std::sync::Arc::new),
            data_batch: None,
            error_text,
            seek_pk_extra,
        },
        data_block,
    })
}

#[cfg(test)]
#[path = "tests/message.rs"]
mod tests;
