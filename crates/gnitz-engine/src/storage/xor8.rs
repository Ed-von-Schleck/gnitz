use crate::xxh;
use xorf::{Filter, Xor8};

const MAGIC: &[u8; 4] = b"GXF1";
const HEADER_SIZE: usize = 4 + 8 + 4 + 4; // magic + seed + block_length + fp_count

/// Build an Xor8 filter from a slice of u128 PKs. Deduplicates before building.
/// Returns None if the input is empty.
///
/// Duplicate PKs are deduplicated before building — shard files may contain
/// retraction rows where the same PK appears with different payloads/weights.
pub fn build(pks: &[u128]) -> Option<Xor8> {
    if pks.is_empty() {
        return None;
    }
    let mut keys: Vec<u64> = pks.iter().map(|&k| xxh::hash_u128(k)).collect();
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return None;
    }
    Some(Xor8::from(keys.as_slice()))
}

/// Check if a key may be present in the filter.
pub fn may_contain(filter: &Xor8, key: u128) -> bool {
    let h = xxh::hash_u128(key);
    filter.contains(&h)
}

/// Serialize an Xor8 filter to bytes.
/// Format: [magic:4B "GXF1"][seed:u64 LE][block_length:u32 LE][fp_count:u32 LE][fingerprints]
pub fn serialize(filter: &Xor8) -> Vec<u8> {
    let fp_count = u32::try_from(filter.fingerprints.len()).expect("xor8 filter too large to serialize");
    let block_length = u32::try_from(filter.block_length).expect("xor8 block_length too large to serialize");
    let mut buf = Vec::with_capacity(serialized_size(filter));
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&filter.seed.to_le_bytes());
    buf.extend_from_slice(&block_length.to_le_bytes());
    buf.extend_from_slice(&fp_count.to_le_bytes());
    buf.extend_from_slice(&filter.fingerprints);
    buf
}

/// Deserialize an Xor8 filter from bytes.
/// Returns None if the buffer is too short, has wrong magic, or is internally inconsistent.
pub fn deserialize(buf: &[u8]) -> Option<Xor8> {
    if buf.len() < HEADER_SIZE {
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
    if fp_count != block_length.checked_mul(3)? {
        return None;
    }
    if buf.len() < HEADER_SIZE + fp_count {
        return None;
    }
    let fingerprints = buf[HEADER_SIZE..HEADER_SIZE + fp_count].to_vec().into_boxed_slice();
    Some(Xor8 {
        seed,
        block_length,
        fingerprints,
    })
}

/// Returns the serialized size of a filter.
pub fn serialized_size(filter: &Xor8) -> usize {
    HEADER_SIZE + filter.fingerprints.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_query_no_false_negatives() {
        let pks: Vec<u128> = (0u128..1000).collect();
        let filter = build(&pks).unwrap();
        for &pk in &pks {
            assert!(may_contain(&filter, pk), "false negative for key {}", pk);
        }
    }

    #[test]
    fn false_positive_rate() {
        let pks: Vec<u128> = (0u128..2000).collect();
        let filter = build(&pks).unwrap();
        let mut fp = 0u32;
        for i in 10_000u128..20_000 {
            if may_contain(&filter, i) {
                fp += 1;
            }
        }
        // Xor8 theoretical FPR ~0.39%. Allow up to 2%.
        assert!(fp < 200, "FPR too high: {}/10000", fp);
    }

    #[test]
    fn small_set() {
        let filter = build(&[100u128, 200]).unwrap();
        assert!(may_contain(&filter, 100));
        assert!(may_contain(&filter, 200));
    }

    #[test]
    fn empty_returns_none() {
        assert!(build(&[]).is_none());
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let pks: Vec<u128> = (0u128..500)
            .map(|i| crate::util::make_pk(i as u64, 42))
            .collect();
        let filter = build(&pks).unwrap();
        let bytes = serialize(&filter);
        let restored = deserialize(&bytes).unwrap();

        for &pk in &pks {
            assert!(
                may_contain(&restored, pk),
                "roundtrip false negative for key {:#034x}", pk,
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
        let filter = build(&[1u128, 2, 3]).unwrap();
        let bytes = serialize(&filter);
        assert_eq!(bytes.len(), serialized_size(&filter));
    }

    fn make_header(block_length: u32, fp_count: u32, extra_bytes: usize) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER_SIZE + extra_bytes];
        buf[..4].copy_from_slice(MAGIC);
        // seed stays zero
        buf[12..16].copy_from_slice(&block_length.to_le_bytes());
        buf[16..20].copy_from_slice(&fp_count.to_le_bytes());
        buf
    }

    #[test]
    fn deserialize_rejects_zero_block_length() {
        let buf = make_header(0, 0, 0);
        assert!(deserialize(&buf).is_none());
    }

    #[test]
    fn deserialize_rejects_inconsistent_fp_count() {
        let buf = make_header(2, 8, 8);
        assert!(deserialize(&buf).is_none());
    }

    #[test]
    fn deserialize_rejects_short_body() {
        let buf = make_header(1, 3, 1);
        assert!(deserialize(&buf).is_none());
    }

    #[test]
    fn build_crossing_u64_boundary() {
        let pks: [u128; 5] = [
            0,
            1,
            u64::MAX as u128,
            (u64::MAX as u128) + 1,
            u128::MAX,
        ];
        let filter = build(&pks).unwrap();
        for &pk in &pks {
            assert!(may_contain(&filter, pk), "false negative for pk {:#034x}", pk);
        }
    }
}
