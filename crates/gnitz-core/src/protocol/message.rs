use super::codec::{encode_schema_block, schema_from_block};
use super::error::ProtocolError;
use super::types::{Schema, ZSetBatch};
use super::wal_block::encode_wal_block;
use super::{WireConflictMode, WireFlags, WireStatus};
use gnitz_wire::control::{ctrl_block_size, encode_ctrl_block, frame_blocks, peek_control_block, ControlHeader};
use gnitz_wire::txn_frame::WalBlock;

/// One batch's region list, held for as long as the frame encoder needs it. A
/// transaction frame is sized from every family's regions before any is written,
/// so all of them are live at once.
struct Regioned<'a> {
    table_id: u32,
    entry_count: u32,
    regions: Vec<&'a [u8]>,
}

impl<'a> Regioned<'a> {
    fn new(table_id: u64, batch: &ZSetBatch, regions: Vec<&'a [u8]>) -> Self {
        Regioned {
            table_id: table_id as u32,
            entry_count: batch.len() as u32,
            regions,
        }
    }

    fn wal(&self) -> WalBlock<'_> {
        WalBlock {
            table_id: self.table_id,
            entry_count: self.entry_count,
            regions: &self.regions,
        }
    }
}

#[derive(Debug, Default)]
pub struct Message {
    pub hdr: ControlHeader,
    /// The schema, `Some` iff a schema block was physically in the frame (an
    /// `Arc` so a cache absorb is a refcount bump, not a deep copy). On a
    /// hint-only continuation frame it is `None` — the caller supplied the
    /// schema out of band.
    pub schema: Option<std::sync::Arc<Schema>>,
    /// The blob under a non-`Ok` status.
    pub error_text: Option<String>,
    /// The blob under an `Ok` status.
    pub blob: Vec<u8>,
}

/// An encoded wire message as its constituent blocks — control header, optional
/// schema, data (empty when no data block) — kept separate so send paths can
/// hand them to a vectored write (one length prefix over the concatenation)
/// instead of flattening into one contiguous buffer.
pub struct MessageParts {
    pub ctrl: Vec<u8>,
    /// Empty when the frame carries no schema block, as for `data`. A block
    /// that is present is never empty: it always carries a WAL header.
    pub schema: Vec<u8>,
    pub data: Vec<u8>,
}

impl MessageParts {
    /// One block as a whole frame: no schema, no data. What every pre-encoded
    /// transaction payload is queued as.
    pub fn single(ctrl: Vec<u8>) -> Self {
        MessageParts {
            ctrl,
            schema: Vec::new(),
            data: Vec::new(),
        }
    }

    /// The blocks in wire order, for a vectored send. Empty segments are
    /// skipped by the transport.
    pub fn segments(&self) -> [&[u8]; 3] {
        [&self.ctrl, &self.schema, &self.data]
    }

    /// The frame's payload length: the segments summed.
    pub fn byte_len(&self) -> usize {
        self.segments().iter().map(|s| s.len()).sum()
    }

    /// Flatten into one contiguous payload. Tests use this to inspect a frame
    /// the send path would hand to a vectored write unflattened.
    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.byte_len());
        for s in self.segments() {
            out.extend_from_slice(s);
        }
        out
    }
}

/// Control header + optional schema block + optional data block, without the 4-byte
/// frame header. The schema block is built before the empty-batch filter, so an
/// empty Z-set delta still ships one.
pub fn encode_frame(
    hdr: ControlHeader,
    blob: &[u8],
    schema: Option<&Schema>,
    data: Option<&ZSetBatch>,
) -> MessageParts {
    let schema = schema.map_or_else(Vec::new, |s| encode_schema_block(s, hdr.target_id as u32));
    let data = data.filter(|b| !b.is_empty());
    let hdr = ControlHeader {
        flags: WireFlags {
            has_schema: !schema.is_empty(),
            has_data: data.is_some(),
            ..hdr.flags
        },
        ..hdr
    };
    let mut ctrl = vec![0u8; ctrl_block_size(blob.len())];
    encode_ctrl_block(&mut ctrl, &hdr, blob);
    MessageParts {
        ctrl,
        schema,
        data: data.map_or_else(Vec::new, |b| encode_wal_block(hdr.target_id as u32, b)),
    }
}

/// Encode an atomic user-table push transaction frame (`ClientVerb::PushTxn`) into
/// wire bytes (without the 4-byte frame header). Adapts the client's
/// `(&Schema, &ZSetBatch)` families to the pre-encoded blocks the shared
/// `gnitz_wire::txn_frame` codec bundles — the frame layout itself lives there,
/// so the server cannot walk a different one.
pub fn encode_push_txn(
    families: &[(u64, &Schema, &ZSetBatch, WireConflictMode)],
    preconditions: &[(u64, u64)],
) -> Vec<u8> {
    // Every family's regions are live at once: the frame is sized from all of
    // them and then each batch is framed straight into it, so a batch is copied
    // once instead of once into a per-family block and again into the frame.
    let parts: Vec<(WireConflictMode, Vec<u8>, Regioned<'_>)> = families
        .iter()
        .map(|(tid, schema, batch, mode)| {
            (
                *mode,
                encode_schema_block(schema, *tid as u32),
                Regioned::new(*tid, batch, super::regions::regions(batch)),
            )
        })
        .collect();
    let refs: Vec<(WireConflictMode, &[u8], WalBlock<'_>)> =
        parts.iter().map(|(m, sb, r)| (*m, &sb[..], r.wal())).collect();
    gnitz_wire::txn_frame::encode_push_txn(&refs, preconditions)
}

/// Encode an atomic DDL transaction frame (`ClientVerb::DdlTxn`) into wire bytes
/// (without the 4-byte frame header). Every system-table write — a `CREATE`'s N
/// family batches, a `DROP`/`CREATE INDEX`/`CREATE SCHEMA`'s single batch — is
/// carried by one such frame so the server ingests the whole bundle under one
/// durable SAL zone.
///
/// Each family is named by its system table id alone; its batch carries its own
/// layout, which `Session::submit` validates against that id's system schema.
pub fn encode_ddl_txn(families: &[(u64, ZSetBatch)]) -> Vec<u8> {
    let regioned: Vec<Regioned<'_>> = families
        .iter()
        .map(|(tid, batch)| Regioned::new(*tid, batch, super::regions::regions(batch)))
        .collect();
    let refs: Vec<WalBlock<'_>> = regioned.iter().map(Regioned::wal).collect();
    gnitz_wire::txn_frame::encode_ddl_txn(&refs)
}

/// Parse a wire payload (without 4-byte frame header) into a `Message`.
/// The payload is what `recv_framed()` returns.
///
/// `schema_hint` is `Some((schema, cached_version))` when the client has a
/// cached schema for the responding table. On continuation frames (no schema
/// block), the hint is used to decode the data block; a version mismatch is a
/// hard protocol error. Pass `None` for the initial frame or when no cache
/// entry exists.
#[cfg(any(test, feature = "integration"))]
pub fn parse_response(
    buf: &[u8],
    schema_hint: Option<(&Schema, u16)>,
) -> Result<(Message, Option<ZSetBatch>), ProtocolError> {
    let mut parsed = parse_response_frame(buf, schema_hint.map(|(_, v)| v))?;
    let data_batch = match parsed.data_block.take() {
        Some(range) => {
            let eff = parsed
                .effective(schema_hint.map(|(s, _)| s))
                .ok_or_else(|| ProtocolError::DecodeError("no schema for data block".into()))?;
            Some(super::wal_block::decode_wal_block(&buf[range], eff)?.0)
        }
        None => None,
    };
    Ok((parsed.message, data_batch))
}

/// A parsed reply frame whose data block has been located but not decoded.
pub(crate) struct ParsedFrame {
    pub(crate) message: Message,
    /// Where the data block sits in the frame buffer, or `None` when the frame
    /// carries no data.
    pub(crate) data_block: Option<std::ops::Range<usize>>,
}

impl ParsedFrame {
    /// The schema this frame's data block decodes under: the block it carried,
    /// else the caller's hint. One rule, so the two decode entries cannot
    /// disagree about which schema a hint-only continuation frame uses.
    pub(crate) fn effective<'s>(&'s self, hint: Option<&'s Schema>) -> Option<&'s Schema> {
        self.message.schema.as_deref().or(hint)
    }
}

/// Everything [`parse_response`] does except decoding the data block: frame
/// bounds, control header, error text, schema block and the schema-version check.
///
/// The split exists so a caller that wants the block's *bytes* — a subscriber
/// feeding them into a store, which would otherwise pay an OPK→native decode
/// and a native→OPK re-encode of every key to rebuild what the socket already
/// delivered — reaches them through the same validation the decoding entry
/// runs, rather than a second copy of it that could drift on what it checked.
pub(crate) fn parse_response_frame(buf: &[u8], cached_version: Option<u16>) -> Result<ParsedFrame, ProtocolError> {
    let ctrl = peek_control_block(buf).map_err(|e| ProtocolError::DecodeError(e.into()))?;
    let blocks = frame_blocks(buf, &ctrl)?;

    let wire_schema = match &blocks.schema {
        Some(r) => Some(schema_from_block(&buf[r.clone()])?),
        None => None,
    };
    if blocks.schema.is_none() && blocks.data.is_some() {
        // A hint-only frame: the caller decodes it against the schema it already
        // holds, so all this leg checks is that the stamp still matches.
        let Some(cached) = cached_version else {
            return Err(ProtocolError::DecodeError(
                "a data block without a schema block and no cached schema".into(),
            ));
        };
        let server_version = ctrl.hdr.flags.schema_version;
        if server_version != cached {
            return Err(ProtocolError::DecodeError(format!(
                "schema version mismatch: cached={cached} server={server_version}"
            )));
        }
    }

    let (error_text, blob) = if ctrl.hdr.status != WireStatus::Ok && !ctrl.blob.is_empty() {
        let text =
            String::from_utf8(ctrl.blob).map_err(|e| ProtocolError::DecodeError(format!("utf8 in error text: {e}")))?;
        (Some(text), Vec::new())
    } else {
        (None, ctrl.blob)
    };

    Ok(ParsedFrame {
        message: Message {
            hdr: ctrl.hdr,
            schema: wire_schema.map(std::sync::Arc::new),
            error_text,
            blob,
        },
        data_block: blocks.data,
    })
}

#[cfg(test)]
#[path = "tests/message.rs"]
mod tests;
