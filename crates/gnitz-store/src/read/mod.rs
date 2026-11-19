//! The read rung — the `ReadSpec` executor and the store read verbs, as
//! `impl RelationRegistry`, `impl Relation` and `impl Snapshot` blocks over
//! `relation`.
//!
//! What is decided here is which source serves a bound — the store, or an index
//! over it — and who hydrates a skeleton row. The filter `scan_spec` drives is
//! `gnitz-expr`'s, the map and the sink are `gnitz_zset::algebra`'s — the same
//! kernels a circuit's linear operators dispatch to — so a read here and a view
//! there compute a row the same way.
//!
//! Recomputing a capacity-bounded view's skeleton row needs the view's compiled
//! program; that edge is injected as `scan_spec`'s hydrate closure, and a host
//! that maintains no circuit passes `None`.

mod delta_read;
mod key_spans;
mod pinned;
mod scan_spec;
mod source;
mod store_io;

pub use key_spans::KeySpans;
pub use source::SourceCursor;
pub use store_io::Probe;

use std::ops::Deref;
use std::rc::Rc;

use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

/// The rows a read answers with: a batch the store holds too, or one built for
/// this read alone.
// One per read; boxing the owned batch would cost every read an allocation.
#[allow(clippy::large_enum_variant)]
pub enum ReadRows {
    /// The store's cached snapshot, or a feed round as it was captured.
    Shared(Rc<Batch>),
    Own(Batch),
}

impl Deref for ReadRows {
    type Target = Batch;

    fn deref(&self) -> &Batch {
        match self {
            ReadRows::Shared(rows) => rows,
            ReadRows::Own(rows) => rows,
        }
    }
}

impl ReadRows {
    /// The rows as a batch of their own: a shared one is copied unless this was its last
    /// holder.
    pub fn into_batch(self) -> Batch {
        match self {
            ReadRows::Shared(rows) => Rc::unwrap_or_clone(rows),
            ReadRows::Own(rows) => rows,
        }
    }
}

/// Refuses a reply in `produced` unless its layout digest is the client's
/// `reply_layout`: a reply in any other layout would ship its regions under the
/// client's strides.
fn check_layout(reply_layout: u64, produced: &SchemaDescriptor) -> Result<(), String> {
    match reply_layout == produced.layout_digest() {
        true => Ok(()),
        false => Err("reply schema does not match the output layout".to_string()),
    }
}

/// Refuses keys of `width` bytes unless it is `stride`, the key of the store
/// `verb` reads of relation `id`: a key of another width names none of its keys.
fn check_stride(verb: &str, id: u64, width: usize, stride: usize) -> Result<(), String> {
    match width == stride {
        true => Ok(()),
        false => Err(format!(
            "{verb}: key stride {width} != the read store's {stride} (relation {id})"
        )),
    }
}
