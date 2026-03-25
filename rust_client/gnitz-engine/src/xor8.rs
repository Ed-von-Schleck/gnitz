use crate::xxh;
use xorf::{Filter, Xor8};

const MAGIC: &[u8; 4] = b"GXF1";

/// Build an Xor8 filter from parallel arrays of (pk_lo, pk_hi).
/// Returns None if the input is empty.
pub fn build(pk_lo: &[u64], pk_hi: &[u64]) -> Option<Xor8> {
    let n = pk_lo.len().min(pk_hi.len());
    if n == 0 {
        return None;
    }
    let mut keys: Vec<u64> = pk_lo[..n]
        .iter()
        .zip(pk_hi[..n].iter())
        .map(|(&lo, &hi)| xxh::hash_u128(lo, hi))
        .collect();
    // Dedup hash keys for xor8 construction only — the xorf crate panics
    // on duplicate inputs. Z-Set shards can contain the same PK multiple
    // times (different weights), but the filter only needs distinct keys.
    keys.sort_unstable();
    keys.dedup();
    Some(Xor8::from(keys.as_slice()))
}

/// Check if a key may be present in the filter.
pub fn may_contain(filter: &Xor8, key_lo: u64, key_hi: u64) -> bool {
    let h = xxh::hash_u128(key_lo, key_hi);
    filter.contains(&h)
}

/// Serialize an Xor8 filter to bytes.
/// Format: [magic:4B "GXF1"][seed:u64 LE][block_length:u32 LE][fp_count:u32 LE][fingerprints]
pub fn serialize(filter: &Xor8) -> Vec<u8> {
    let fp_count = filter.fingerprints.len() as u32;
    let total = 4 + 8 + 4 + 4 + filter.fingerprints.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&filter.seed.to_le_bytes());
    buf.extend_from_slice(&(filter.block_length as u32).to_le_bytes());
    buf.extend_from_slice(&fp_count.to_le_bytes());
    buf.extend_from_slice(&filter.fingerprints);
    buf
}

/// Deserialize an Xor8 filter from bytes.
/// Returns None if the buffer is too short, has wrong magic, or is internally inconsistent.
pub fn deserialize(buf: &[u8]) -> Option<Xor8> {
    // Minimum: 4 (magic) + 8 (seed) + 4 (block_length) + 4 (fp_count) = 20 bytes
    if buf.len() < 20 {
        return None;
    }
    if &buf[..4] != MAGIC {
        return None;
    }
    let seed = u64::from_le_bytes(buf[4..12].try_into().ok()?);
    let block_length = u32::from_le_bytes(buf[12..16].try_into().ok()?) as usize;
    let fp_count = u32::from_le_bytes(buf[16..20].try_into().ok()?) as usize;

    if block_length == 0 {
        return None;
    }
    // Validate: fp_count should be 3 * block_length for a standard xor8
    if fp_count != 3 * block_length {
        return None;
    }
    if buf.len() < 20 + fp_count {
        return None;
    }
    let fingerprints = buf[20..20 + fp_count].to_vec().into_boxed_slice();
    Some(Xor8 {
        seed,
        block_length,
        fingerprints,
    })
}

/// Returns the serialized size of a filter.
pub fn serialized_size(filter: &Xor8) -> usize {
    4 + 8 + 4 + 4 + filter.fingerprints.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_query_no_false_negatives() {
        let lo: Vec<u64> = (0..1000).collect();
        let hi: Vec<u64> = vec![0; 1000];
        let filter = build(&lo, &hi).unwrap();
        for i in 0..1000u64 {
            assert!(may_contain(&filter, i, 0), "false negative for key {}", i);
        }
    }

    #[test]
    fn false_positive_rate() {
        let lo: Vec<u64> = (0..2000).collect();
        let hi: Vec<u64> = vec![0; 2000];
        let filter = build(&lo, &hi).unwrap();
        let mut fp = 0u32;
        for i in 10_000u64..20_000 {
            if may_contain(&filter, i, 0) {
                fp += 1;
            }
        }
        // Xor8 theoretical FPR ~0.39%. Allow up to 2%.
        assert!(fp < 200, "FPR too high: {}/10000", fp);
    }

    #[test]
    fn small_set() {
        let filter = build(&[100, 200], &[0, 0]).unwrap();
        assert!(may_contain(&filter, 100, 0));
        assert!(may_contain(&filter, 200, 0));
    }

    #[test]
    fn empty_returns_none() {
        assert!(build(&[], &[]).is_none());
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let lo: Vec<u64> = (0..500).collect();
        let hi: Vec<u64> = vec![42; 500];
        let filter = build(&lo, &hi).unwrap();
        let bytes = serialize(&filter);
        let restored = deserialize(&bytes).unwrap();

        // Verify all original keys found in restored filter
        for i in 0..500u64 {
            assert!(
                may_contain(&restored, i, 42),
                "roundtrip false negative for key {}",
                i
            );
        }
    }

    #[test]
    fn deserialize_old_format_returns_none() {
        // Old format starts with seed (u64), no magic
        let mut old_buf = vec![0u8; 32];
        old_buf[..8].copy_from_slice(&1u64.to_le_bytes()); // seed = 1
        assert!(deserialize(&old_buf).is_none());
    }

    #[test]
    fn deserialize_truncated_returns_none() {
        assert!(deserialize(&[]).is_none());
        assert!(deserialize(b"GXF1").is_none());
        assert!(deserialize(&[0u8; 19]).is_none());
    }

    #[test]
    fn serialized_size_matches() {
        let filter = build(&[1, 2, 3], &[0, 0, 0]).unwrap();
        let bytes = serialize(&filter);
        assert_eq!(bytes.len(), serialized_size(&filter));
    }
}
