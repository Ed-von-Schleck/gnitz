//! Shard image encoding and writing, reached through `Batch::write_as_shard`.

use std::borrow::Cow;
use std::fs::OpenOptions;
use std::os::unix::fs::{FileExt, OpenOptionsExt};

use super::batch::{Batch, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::layout::*;
use super::shard_filter;
use crate::repr::error::StorageError;
use crate::schema::key::probe_key;
use crate::schema::SchemaDescriptor;
use gnitz_wire::write_u64_le;
use xorf::BinaryFuse8;

/// Fixed-width region `i`'s on-disk encoding and image. `pack_ints` admits FoR
/// on fixed-int payload regions.
fn encode_region<'a>(
    schema: &SchemaDescriptor,
    i: usize,
    src: &'a [u8],
    n: usize,
    pack_ints: bool,
) -> (Encoding, Cow<'a, [u8]>) {
    let width = src.len() / n;
    // Every element equals the next exactly when the region equals itself
    // shifted by one element: one `bcmp` for the whole region.
    if src[width..] == src[..src.len() - width] {
        return (Encoding::Constant, Cow::Borrowed(&src[..width]));
    }
    let packed = if i == REG_WEIGHT {
        two_value_encode(src).map(|image| (Encoding::TwoValue, image))
    } else if pack_ints && i >= REG_PAYLOAD_START {
        let col = &schema.columns[schema.payload_col_idx(i - REG_PAYLOAD_START)];
        col.fixed_int()
            .and_then(|fi| for_encode(src, fi))
            .map(|image| (Encoding::For, image))
    } else {
        None
    };
    match packed {
        Some((encoding, image)) => (encoding, Cow::Owned(image)),
        None => (Encoding::Raw, Cow::Borrowed(src)),
    }
}

/// The `(size, encoding)` projection of a region's [`DirEntry`], for the
/// shard-format assertions here and in the compaction / shard-reader test
/// modules. The entry shape itself lives in `layout`.
#[cfg(test)]
pub(crate) fn region_dir(image: &[u8], i: usize) -> (usize, Encoding) {
    let e = DirEntry::read(image, i);
    (e.size, Encoding::from_byte(e.encoding).unwrap())
}

fn build_shard_filter_from_pk_region(pk_bytes: &[u8], stride: usize) -> Option<BinaryFuse8> {
    // One hashed key per distinct PK. The PK region is sorted, so rows that
    // share a PK but differ in payload (valid under (PK, payload) element
    // identity) are adjacent — skipping chunks byte-equal to their predecessor
    // is an allocation-free O(n) pre-shrink that bounds `build`'s sort at the
    // number of *distinct* PKs in the region rather than its row count.
    // `probe_key` is the derivation the probe side must match exactly.
    let mut keys: Vec<u64> = Vec::with_capacity(pk_bytes.len() / stride);
    let mut prev: Option<&[u8]> = None;
    for chunk in pk_bytes.chunks_exact(stride) {
        if prev == Some(chunk) {
            continue;
        }
        prev = Some(chunk);
        keys.push(probe_key(chunk));
    }
    shard_filter::build(keys)
}

/// Per-call policy for [`Batch::write_as_shard`].
#[derive(Clone, Copy, Default)]
pub struct ShardWriteOpts {
    /// FoR on fixed-int payload regions.
    pub pack_ints: bool,
    /// Stamp [`SHARD_FLAG_SKELETON`].
    pub skeleton: bool,
    /// Write no PK filter — named for what it turns off, so `Default` builds one.
    pub skip_pk_filter: bool,
}

impl Batch {
    /// Write this batch as a new, unsynced shard at `path`; `Err` if `path` exists.
    pub fn write_as_shard(&self, path: &str, opts: ShardWriteOpts) -> Result<(), StorageError> {
        let schema = self.schema();
        let n = self.count;
        assert!(n > 0, "every writer skips an empty output");
        // A shard carries no dead heap bytes.
        if self.dead_heap != 0 && super::string_heap::measure_dead_heap(&self.as_mem_batch(), schema) != 0 {
            return self.compacted().write_as_shard(path, opts);
        }
        self.debug_verify_dead_heap();
        let regions = self.wire_regions();
        let npc = schema.num_payload_cols();
        self.debug_verify_consolidated();
        debug_assert!(
            !opts.skeleton || npc == 0,
            "a skeleton shard must be written under the PK-only projection of its relation's schema",
        );
        // A store nothing point-probes writes no filter.
        let filter = (!opts.skip_pk_filter)
            .then(|| build_shard_filter_from_pk_region(regions[REG_PK], schema.pk_stride()))
            .flatten()
            .map(|f| shard_filter::serialize(&f));
        // Blob and filter are Raw; every other region is fixed-width.
        let (blob, fixed) = regions.split_last().unwrap();
        let images = fixed
            .iter()
            .enumerate()
            .map(|(i, &src)| encode_region(schema, i, src, n, opts.pack_ints))
            .chain([
                (Encoding::Raw, Cow::Borrowed(*blob)),
                (Encoding::Raw, Cow::Borrowed(filter.as_deref().unwrap_or(&[]))),
            ]);

        static PAD: [u8; ALIGNMENT] = [0; ALIGNMENT];
        let file = OpenOptions::new().write(true).create_new(true).mode(0o644).open(path)?;
        let mut header = vec![0u8; desc_len(npc)];
        let mut body = gnitz_wire::RowHasher::default();
        let mut end = header.len();
        for (i, (encoding, image)) in images.enumerate() {
            for bytes in [&PAD[..region_start(end) - end], &image] {
                body.update(bytes);
                file.write_all_at(bytes, end as u64)?;
                end += bytes.len();
            }
            DirEntry {
                size: image.len(),
                encoding: encoding as u8,
            }
            .write(&mut header, i);
        }

        ShardHeader {
            row_count: n,
            file_npc: npc,
            skeleton: opts.skeleton,
            body_checksum: body.digest(),
        }
        .write(&mut header);
        let desc = desc_digest(path, &header);
        write_u64_le(&mut header, OFF_DESC_CHECKSUM, desc);
        file.write_all_at(&header, 0)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/shard_file.rs"]
mod tests;
