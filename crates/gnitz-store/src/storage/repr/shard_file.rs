//! Shard image encoding and atomic writing, reached through `Batch::write_as_shard`.

use std::borrow::Cow;
use std::os::unix::fs::FileExt;

use super::super::error::StorageError;
use super::super::StagedFile;
use super::batch::{Batch, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::layout::*;
use super::shard_filter;
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
    // `probe_key` owns the narrow/wide derivation the probe side must
    // match exactly.
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
pub(crate) struct ShardWriteOpts {
    /// FoR on fixed-int payload regions.
    pub pack_ints: bool,
    /// Stamp [`SHARD_FLAG_SKELETON`].
    pub skeleton: bool,
    /// Write no PK filter — named for what it turns off, so `Default` builds one.
    pub skip_pk_filter: bool,
}

impl ShardWriteOpts {
    /// The compaction write policy: FoR-packed integer payload regions. The
    /// differential-test oracles reuse it so they cannot drift from the
    /// production write; the shard index overrides `skip_pk_filter` per store.
    pub(crate) const COMPACTION: Self = ShardWriteOpts {
        pack_ints: true,
        skeleton: false,
        skip_pk_filter: false,
    };
}

/// A single-I64-payload shard of `(opk_bytes, weight, payload)` rows, written by
/// the production [`Batch::write_as_shard`]. Several I64 payload columns go
/// through [`write_i64_shard`].
#[cfg(test)]
pub(in crate::storage) fn write_test_shard(
    path: &std::path::Path,
    schema: &SchemaDescriptor,
    rows: &[(Vec<u8>, i64, i64)],
    opts: ShardWriteOpts,
) -> String {
    let path = path.to_str().unwrap().to_owned();
    let rows: Vec<(&[u8], i64, i64)> = rows.iter().map(|(pk, w, v)| (pk.as_slice(), *w, *v)).collect();
    crate::test_support::make_batch_opk(schema, &rows)
        .write_as_shard(&path, opts)
        .unwrap();
    path
}

/// [`write_test_shard`] at any number of I64 payload columns, with explicit null
/// words and a raw blob heap. Rows are `(opk_bytes, weight, null_word, payload)`.
#[cfg(test)]
pub(in crate::storage) fn write_i64_shard(
    path: &str,
    schema: &SchemaDescriptor,
    rows: &[(Vec<u8>, i64, u64, Vec<i64>)],
    blob: &[u8],
    opts: ShardWriteOpts,
) {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    b.blob.extend_from_slice(blob);
    for (pk, w, null_word, cols) in rows {
        debug_assert_eq!(cols.len(), schema.num_payload_cols());
        b.begin_row(pk, *w);
        for (pi, v) in cols.iter().enumerate() {
            b.extend_col(pi, &v.to_le_bytes());
        }
        b.commit_row(*null_word);
    }
    b.write_as_shard(path, opts).unwrap();
}

impl Batch {
    /// Write this batch as an unsynced shard at `path`, through a [`StagedFile`].
    pub(crate) fn write_as_shard(&self, path: &str, opts: ShardWriteOpts) -> Result<(), StorageError> {
        let schema = self.schema();
        let n = self.count;
        assert!(n > 0, "every writer skips an empty output");
        let mut regions = gnitz_wire::Regions::new();
        self.wire_regions(&mut regions);
        let npc = schema.num_payload_cols();
        #[cfg(debug_assertions)]
        self.debug_verify_consolidated(schema);
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
        let (staged, file) = StagedFile::create(path)?;
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
        staged.commit()
    }
}

#[cfg(test)]
#[path = "tests/shard_file.rs"]
mod tests;
