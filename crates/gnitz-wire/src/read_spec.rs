// ---------------------------------------------------------------------------
// ReadSpec — the parameterized-scan descriptor for an ad-hoc bounded SELECT.
//
// One wire descriptor carries bound extraction, predicate evaluation,
// projection, and ORDER BY / LIMIT top-k out to the workers, executed
// single-pass over each worker's merged partition cursor. Both client
// (gnitz-core) and engine (gnitz-engine worker) share this encoder/decoder —
// the same drift-safety rule `RangeDescriptor` follows. The master forwards the
// encoded blob verbatim (it never decodes the bound); the worker decodes it at
// the trust boundary and rejects any malformed frame.
// ---------------------------------------------------------------------------

use crate::range::RangeDescriptor;

/// ORDER BY keys apply in sequence; a spec carries at most this many.
pub const MAX_ORDER_KEYS: usize = 16;
/// A `pk IN (…)` list larger than this rejects cleanly at bind time.
pub const MAX_PK_SET_KEYS: usize = 65_536;
/// Decode-side ceiling on a full encoded `ReadSpec` blob.
pub const MAX_READ_SPEC_BYTES: usize = 2 << 20;

const VERSION: u8 = 1;

const BOUND_NONE: u8 = 0;
const BOUND_PK_RANGE: u8 = 1;
const BOUND_INDEX_RANGE: u8 = 2;
const BOUND_PK_SET: u8 = 3;

const ORDER_DESC: u8 = 1 << 0;
const ORDER_NULLS_FIRST: u8 = 1 << 1;

/// One ORDER BY key. `col` is a **full reply-schema column index** (not a dense
/// payload index): the worker's top-k sink and the client's finish both resolve
/// it through the same `is_pk_col` / `payload_idx` split, so a projected PK
/// column and a hidden appended order column each land in the right window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderKey {
    pub col: u16,
    pub desc: bool,
    pub nulls_first: bool,
}

/// The bound a `ReadSpec` walks before predicate/projection. `RangeDescriptor`
/// carries **native** values (packed LE `u128`); the worker is the sole OPK
/// encoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadBound {
    /// Full merged cursor — a plain scan with a server-side predicate.
    None,
    /// Range over the PK column list (`RangeDescriptor` semantics). A full-PK
    /// point lookup is `n_eq = pk_count − 1` with degenerate cuts. Exact — no
    /// residual is needed for the bound itself.
    PkRange(RangeDescriptor),
    /// Secondary-index range: the packed index column list and the descriptor.
    /// The worker decides the walk from the range column's type: a wide-int
    /// (U128/UUID) index — the one index-eligible family the predicate VM
    /// cannot express, whose conjunct the SQL layer therefore strips — runs an
    /// un-gated byte-exact `BoundedIndexCursor`; a ≤8-byte-integer index is
    /// selectivity-gated (may fall back to a full cursor) and the conjunct
    /// stays in the predicate. Both sides derive the split from the same
    /// column type, so a stripped conjunct is always re-imposed by the walk.
    IndexRange { idx_cols: u64, desc: RangeDescriptor },
    /// `pk IN (…)` for a single-column PK. Values are raw native keys widened to
    /// `u128` (`FixedInt::pack`), **deduplicated**; wire order is irrelevant —
    /// the worker OPK-sorts the keys before its forward gather.
    PkSet(Vec<u128>),
}

impl ReadBound {
    const fn kind(&self) -> u8 {
        match self {
            ReadBound::None => BOUND_NONE,
            ReadBound::PkRange(_) => BOUND_PK_RANGE,
            ReadBound::IndexRange { .. } => BOUND_INDEX_RANGE,
            ReadBound::PkSet(_) => BOUND_PK_SET,
        }
    }
}

/// A parameterized bounded scan: bound → predicate → projection → ORDER BY /
/// LIMIT, executed on the workers over one merged partition cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSpec {
    pub bound: ReadBound,
    /// Compiled predicate over the SOURCE schema ("EXPR" blob). Empty = the
    /// bound is exact and no residual filter runs.
    pub predicate: Vec<u8>,
    /// Compiled projection map program over the SOURCE schema → reply payload
    /// columns. Empty = identity (reply schema == source schema).
    pub projection: Vec<u8>,
    /// ORDER BY keys applied in sequence; `col` indices refer to the REPLY
    /// schema. `len ≤ MAX_ORDER_KEYS`.
    pub order: Vec<OrderKey>,
    /// OFFSET + LIMIT in logical rows (summed weight). 0 = unbounded. (`LIMIT 0`
    /// never reaches the wire — the SQL layer short-circuits an empty window.)
    pub limit_k: u64,
}

/// Pack a ScanSpec request's control-block `seek_pk_extra` blob: the encoded
/// `ReadSpec` followed by the raw reply-schema wire block, each `u32`-length-
/// prefixed. Bundling both in the arbitrary-length `seek_pk_extra` BLOB keeps
/// the master a pure forwarder (it never decodes the spec) and hands the worker
/// the raw reply-schema bytes to **echo** verbatim — the engine
/// `SchemaDescriptor` drops the hidden flags, so the worker must never rebuild
/// the block.
pub fn pack_scan_spec_extra(spec: &[u8], reply_block: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + spec.len() + reply_block.len());
    out.extend_from_slice(&(spec.len() as u32).to_le_bytes());
    out.extend_from_slice(spec);
    out.extend_from_slice(&(reply_block.len() as u32).to_le_bytes());
    out.extend_from_slice(reply_block);
    out
}

/// Split a ScanSpec `seek_pk_extra` blob back into `(spec_bytes, reply_block)`
/// at the trust boundary — rejecting a truncated / trailing-byte frame.
pub fn unpack_scan_spec_extra(extra: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let spec_len = extra.get(0..4).ok_or("scan_spec extra: truncated spec length")?;
    let spec_len = u32::from_le_bytes(spec_len.try_into().unwrap()) as usize;
    let spec_end = 4usize
        .checked_add(spec_len)
        .ok_or("scan_spec extra: spec length overflow")?;
    let block_len_bytes = extra
        .get(spec_end..spec_end + 4)
        .ok_or("scan_spec extra: truncated block length")?;
    let block_len = u32::from_le_bytes(block_len_bytes.try_into().unwrap()) as usize;
    let block_start = spec_end + 4;
    let block_end = block_start
        .checked_add(block_len)
        .ok_or("scan_spec extra: block length overflow")?;
    if extra.len() != block_end {
        return Err(format!("scan_spec extra: {} bytes, expected {block_end}", extra.len()));
    }
    Ok((&extra[4..spec_end], &extra[block_start..block_end]))
}

/// A bounds-checked forward reader over the encoded blob — every field access
/// is a `take` that rejects a truncated frame rather than panicking.
struct Reader<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, off: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.off.checked_add(n).ok_or("read_spec: length overflow")?;
        if end > self.buf.len() {
            return Err(format!(
                "read_spec: truncated (need {n} bytes at offset {}, {} remain)",
                self.off,
                self.buf.len() - self.off
            ));
        }
        let s = &self.buf[self.off..end];
        self.off = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    /// The next byte without consuming it — used to compute a variable-length
    /// `RangeDescriptor`'s span from its leading `n_eq`.
    fn peek_u8(&self) -> Result<u8, String> {
        self.buf
            .get(self.off)
            .copied()
            .ok_or_else(|| "read_spec: truncated reading descriptor length".to_string())
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }
}

/// Read an embedded `RangeDescriptor`: peek its `n_eq` to learn its span, slice
/// exactly that many bytes, and defer full validation to `RangeDescriptor::decode`.
/// (`n_eq` is one byte, so `encoded_len` cannot overflow; a pathological value
/// exceeds `remaining` and `take` rejects it, and a valid one is re-validated —
/// arity, flags, exact length — by the descriptor decoder.)
fn read_range_descriptor(r: &mut Reader) -> Result<RangeDescriptor, String> {
    let n_eq = r.peek_u8()? as usize;
    let bytes = r.take(RangeDescriptor::encoded_len(n_eq))?;
    RangeDescriptor::decode(bytes)
}

impl ReadSpec {
    /// Serialise to a version-prefixed LE byte sequence (§ layout below).
    pub fn encode(&self) -> Vec<u8> {
        // Header: version | bound_kind | n_order_keys | reserved | u64 limit_k.
        let mut out = vec![VERSION, self.bound.kind(), self.order.len() as u8, 0u8];
        out.extend_from_slice(&self.limit_k.to_le_bytes());

        match &self.bound {
            ReadBound::None => {}
            ReadBound::PkRange(desc) => out.extend_from_slice(&desc.encode()),
            ReadBound::IndexRange { idx_cols, desc } => {
                out.extend_from_slice(&idx_cols.to_le_bytes());
                out.extend_from_slice(&desc.encode());
            }
            ReadBound::PkSet(keys) => {
                out.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                for k in keys {
                    out.extend_from_slice(&k.to_le_bytes());
                }
            }
        }

        for key in &self.order {
            out.extend_from_slice(&key.col.to_le_bytes());
            let mut flags = 0u8;
            if key.desc {
                flags |= ORDER_DESC;
            }
            if key.nulls_first {
                flags |= ORDER_NULLS_FIRST;
            }
            out.push(flags);
            out.push(0u8); // reserved
        }

        out.extend_from_slice(&(self.predicate.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.predicate);
        out.extend_from_slice(&(self.projection.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.projection);
        out
    }

    /// Decode and validate at the trust boundary. Rejects: an over-cap blob,
    /// unknown version/kind, order-key or PkSet count over cap, a raw-u128
    /// duplicate PkSet key (dedup is type-agnostic), an unknown order flag bit,
    /// length overflows/truncation, and trailing bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        if buf.len() > MAX_READ_SPEC_BYTES {
            return Err(format!(
                "read_spec: {} bytes exceeds cap {MAX_READ_SPEC_BYTES}",
                buf.len()
            ));
        }
        let mut r = Reader::new(buf);
        let version = r.u8()?;
        if version != VERSION {
            return Err(format!("read_spec: unknown version {version}"));
        }
        let bound_kind = r.u8()?;
        let n_order = r.u8()? as usize;
        let _reserved = r.u8()?;
        let limit_k = r.u64()?;

        if n_order > MAX_ORDER_KEYS {
            return Err(format!("read_spec: {n_order} order keys exceeds cap {MAX_ORDER_KEYS}"));
        }

        let bound = match bound_kind {
            BOUND_NONE => ReadBound::None,
            BOUND_PK_RANGE => ReadBound::PkRange(read_range_descriptor(&mut r)?),
            BOUND_INDEX_RANGE => {
                let idx_cols = r.u64()?;
                let desc = read_range_descriptor(&mut r)?;
                ReadBound::IndexRange { idx_cols, desc }
            }
            BOUND_PK_SET => {
                let count = r.u32()? as usize;
                if count > MAX_PK_SET_KEYS {
                    return Err(format!("read_spec: PkSet count {count} exceeds cap {MAX_PK_SET_KEYS}"));
                }
                let mut keys = Vec::with_capacity(count);
                let mut seen = std::collections::HashSet::with_capacity(count);
                for _ in 0..count {
                    let k = r.u128()?;
                    if !seen.insert(k) {
                        return Err(format!("read_spec: PkSet duplicate key {k}"));
                    }
                    keys.push(k);
                }
                ReadBound::PkSet(keys)
            }
            other => return Err(format!("read_spec: unknown bound kind {other}")),
        };

        let mut order = Vec::with_capacity(n_order);
        for _ in 0..n_order {
            let col = r.u16()?;
            let flags = r.u8()?;
            if flags & !(ORDER_DESC | ORDER_NULLS_FIRST) != 0 {
                return Err(format!("read_spec: order key has unknown flag bits {flags:#04x}"));
            }
            let _rsv = r.u8()?;
            order.push(OrderKey {
                col,
                desc: flags & ORDER_DESC != 0,
                nulls_first: flags & ORDER_NULLS_FIRST != 0,
            });
        }

        let pred_len = r.u32()? as usize;
        let predicate = r.take(pred_len)?.to_vec();
        let proj_len = r.u32()? as usize;
        let projection = r.take(proj_len)?.to_vec();

        if r.remaining() != 0 {
            return Err(format!("read_spec: {} trailing bytes", r.remaining()));
        }

        Ok(ReadSpec {
            bound,
            predicate,
            projection,
            order,
            limit_k,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range::Cut::{After, Before};

    fn sample_order() -> Vec<OrderKey> {
        vec![
            OrderKey {
                col: 3,
                desc: false,
                nulls_first: true,
            },
            OrderKey {
                col: 0,
                desc: true,
                nulls_first: false,
            },
        ]
    }

    #[test]
    fn roundtrips_every_bound() {
        let specs = [
            ReadSpec {
                bound: ReadBound::None,
                predicate: vec![1, 2, 3, 4],
                projection: vec![],
                order: sample_order(),
                limit_k: 42,
            },
            ReadSpec {
                bound: ReadBound::PkRange(RangeDescriptor::new(&[], After(10), After(u64::MAX as u128))),
                predicate: vec![],
                projection: vec![9, 9, 9],
                order: vec![],
                limit_k: 0,
            },
            ReadSpec {
                bound: ReadBound::IndexRange {
                    idx_cols: 0x0000_0002_0000_0001,
                    desc: RangeDescriptor::new(&[7], Before(1), Before(u128::MAX)),
                },
                predicate: vec![5, 5],
                projection: vec![6],
                order: sample_order(),
                limit_k: 100,
            },
            ReadSpec {
                bound: ReadBound::PkSet(vec![5, 10, 3, 99]),
                predicate: vec![],
                projection: vec![],
                order: vec![OrderKey {
                    col: 7,
                    desc: false,
                    nulls_first: false,
                }],
                limit_k: 8,
            },
        ];
        for spec in specs {
            let bytes = spec.encode();
            assert!(bytes.len() <= MAX_READ_SPEC_BYTES);
            assert_eq!(ReadSpec::decode(&bytes), Ok(spec.clone()), "{spec:?}");
        }
    }

    #[test]
    fn accepts_unsorted_pk_set_including_widened_signed() {
        // `pk IN (-1, 5)` on a signed PK widens to `[0xFFFF…FF, 5]` — typed order
        // is increasing but raw-u128 order is DECREASING. The decoder must accept
        // this without a sortedness check.
        let spec = ReadSpec {
            bound: ReadBound::PkSet(vec![u64::MAX as u128, 5]),
            predicate: vec![],
            projection: vec![],
            order: vec![],
            limit_k: 0,
        };
        let bytes = spec.encode();
        assert_eq!(ReadSpec::decode(&bytes), Ok(spec));
    }

    #[test]
    fn decode_rejects_bad_version() {
        let mut bytes = ReadSpec {
            bound: ReadBound::None,
            predicate: vec![],
            projection: vec![],
            order: vec![],
            limit_k: 0,
        }
        .encode();
        bytes[0] = 2;
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("version"));
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        let mut bytes = ReadSpec {
            bound: ReadBound::None,
            predicate: vec![],
            projection: vec![],
            order: vec![],
            limit_k: 0,
        }
        .encode();
        bytes[1] = 9;
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("bound kind"));
    }

    #[test]
    fn decode_rejects_order_key_over_cap() {
        let mut bytes = ReadSpec {
            bound: ReadBound::None,
            predicate: vec![],
            projection: vec![],
            order: vec![],
            limit_k: 0,
        }
        .encode();
        bytes[2] = (MAX_ORDER_KEYS + 1) as u8;
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("order keys exceeds cap"));
    }

    #[test]
    fn decode_rejects_pk_set_over_cap() {
        // Hand-build a header + PkSet count exceeding the cap (the count is read
        // before any per-key bytes, so no keys are needed to trip it).
        let mut bytes = vec![VERSION, BOUND_PK_SET, 0, 0];
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&((MAX_PK_SET_KEYS + 1) as u32).to_le_bytes());
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("PkSet count"));
    }

    #[test]
    fn decode_rejects_duplicate_pk_set_key() {
        let spec = ReadSpec {
            bound: ReadBound::PkSet(vec![5, 5]),
            predicate: vec![],
            projection: vec![],
            order: vec![],
            limit_k: 0,
        };
        assert!(ReadSpec::decode(&spec.encode()).unwrap_err().contains("duplicate"));
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut bytes = ReadSpec {
            bound: ReadBound::None,
            predicate: vec![],
            projection: vec![],
            order: vec![],
            limit_k: 0,
        }
        .encode();
        bytes.push(0);
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("trailing"));
    }

    #[test]
    fn decode_rejects_truncation() {
        let bytes = ReadSpec {
            bound: ReadBound::PkSet(vec![1, 2, 3]),
            predicate: vec![],
            projection: vec![],
            order: vec![],
            limit_k: 0,
        }
        .encode();
        // Chop the last key's bytes off.
        assert!(ReadSpec::decode(&bytes[..bytes.len() - 4]).is_err());
    }

    #[test]
    fn decode_rejects_oversized_blob() {
        let big = vec![0u8; MAX_READ_SPEC_BYTES + 1];
        assert!(ReadSpec::decode(&big).unwrap_err().contains("exceeds cap"));
    }

    #[test]
    fn scan_spec_extra_roundtrips() {
        let spec = vec![1u8, 2, 3, 4, 5];
        let block = vec![9u8; 40];
        let packed = pack_scan_spec_extra(&spec, &block);
        assert_eq!(unpack_scan_spec_extra(&packed), Ok((spec.as_slice(), block.as_slice())));
        // Empty block is valid (identity projection carries a real block anyway,
        // but the codec must not choke on either side being empty).
        let packed = pack_scan_spec_extra(&[], &block);
        assert_eq!(unpack_scan_spec_extra(&packed), Ok(([].as_slice(), block.as_slice())));
    }

    #[test]
    fn scan_spec_extra_rejects_truncation_and_trailing() {
        let packed = pack_scan_spec_extra(&[1, 2, 3], &[4, 5]);
        assert!(unpack_scan_spec_extra(&packed[..packed.len() - 1]).is_err());
        let mut long = packed.clone();
        long.push(0);
        assert!(unpack_scan_spec_extra(&long).is_err());
        assert!(unpack_scan_spec_extra(&[]).is_err());
    }
}
