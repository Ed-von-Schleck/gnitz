//! The read rung — the `ReadSpec` executor and the store read verbs, as
//! `impl RelationRegistry` and `impl Relation` blocks over `relation`.
//!
//! What is decided here is which source serves a bound — the store, or an index
//! over it — and who hydrates a skeleton row. The filter `scan_spec` drives is
//! `gnitz-expr`'s, the map and the sink are `gnitz_zset::algebra`'s — the same
//! kernels a circuit's linear operators dispatch to — so a read here and a view
//! there compute a row the same way.
//!
//! Recomputing a capacity-bounded view's skeleton row needs the view's compiled
//! program; that edge is injected as [`SkeletonHydrator`], and a host that
//! maintains no circuit passes `None`.

mod delta_read;
mod key_spans;
mod pinned;
mod scan_spec;
mod store_io;

pub use key_spans::KeySpans;
pub use store_io::Probe;

use crate::relation::RelationRegistry;
use gnitz_wire::PkKeys;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

/// Refuses a reply in `produced` unless its layout digest is the client's
/// `reply_layout`: a reply in any other layout would ship its regions under the
/// client's strides.
fn check_layout(reply_layout: u64, produced: &SchemaDescriptor) -> Result<(), String> {
    match reply_layout == produced.layout_digest() {
        true => Ok(()),
        false => Err("reply schema does not match the output layout".to_string()),
    }
}

/// Refuses `keys` unless each is `stride` bytes, the key of the store `verb` reads
/// of relation `id`: a key list of another width names none of its keys.
fn check_stride(verb: &str, id: u64, keys: &PkKeys, stride: usize) -> Result<(), String> {
    match keys.stride() == stride {
        true => Ok(()),
        false => Err(format!(
            "{verb}: key stride {} != the read store's {stride} (relation {id})",
            keys.stride()
        )),
    }
}

/// Recomputes a capacity-bounded view's rows at its skeleton keys, from the view's
/// own maintained state. A host that maintains no circuit has none.
pub trait SkeletonHydrator {
    /// Every row of `view_id` at `keys` — at the view's `pk_stride`, each held by
    /// the view's store as a skeleton row — consolidated, each key's weights summing
    /// to its skeleton row's weight.
    fn hydrate_keys(&mut self, registry: &RelationRegistry, view_id: u64, keys: PkKeys) -> Result<Batch, String>;
}
