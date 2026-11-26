//! The format a server's data directory is stamped with, and the refusal of a
//! directory stamped with another.

use gnitz_store::relation::{Foreign, STORE_FORMAT};
use gnitz_wire::{RELEASE, format_digest};

use super::sal::SAL_FORMAT;

/// The format of everything a server keeps under its data directory: the
/// stores and the SAL. `data_format_is_pinned` fails until a change to it is
/// pinned, so a release that moves it is known to need a dump and restore.
pub(crate) const DATA_FORMAT: u64 = format_digest(&[STORE_FORMAT, SAL_FORMAT]);

/// Why the data directory at `dir` is refused, and how its data is carried
/// over to this build.
pub(crate) fn refusal(dir: &str, foreign: &Foreign) -> String {
    let unchanged = "nothing in it was changed";
    match foreign {
        Foreign::Stamped { release } if release == RELEASE => format!(
            "data directory '{dir}' was written by another build of gnitz {RELEASE}, in a storage format \
             this build does not read; {unchanged}. Open it with the build that wrote it, or dump it with \
             that build and restore the dump with this one into a new data directory"
        ),
        Foreign::Stamped { release } => format!(
            "data directory '{dir}' was written by gnitz {release}, whose storage format gnitz {RELEASE} \
             does not read; {unchanged}. To upgrade it, dump it with gnitz {release} \
             (gnitz --data {dir} -c '\\dump DUMP_DIR'), then restore the dump with gnitz {RELEASE} into a \
             new data directory (gnitz --data NEW_DIR -c '\\restore DUMP_DIR')"
        ),
        Foreign::Unstamped => format!(
            "data directory '{dir}' was written by a gnitz build from before data directories were stamped \
             with their format, which this build does not read; {unchanged}. No released gnitz of that age \
             can dump it: open it with the build that wrote it, or start a new data directory"
        ),
    }
}

#[cfg(test)]
#[path = "tests/data_format.rs"]
mod tests;
