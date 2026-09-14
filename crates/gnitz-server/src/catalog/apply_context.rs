//! The catalog applier's execution context, in its own module so the mode moves
//! only through the transitions below.

use std::num::NonZeroU64;

/// Which of the two ways a catalog row can reach the applier is running now.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::catalog) enum ApplyMode {
    /// Boot shard replay: every row arrives as a lone `+1` at its current value,
    /// so no row carries a transition.
    Replay,
    /// Every application after boot replay: live DDL, worker `ddl_sync`, SAL
    /// recovery and compensation.
    Live,
}

/// The apply mode and the DDL-zone LSN.
pub(in crate::catalog) struct ApplyContext {
    /// `Replay` while `open()` replays boot shards, then `Live` for good.
    mode: ApplyMode,
    /// LSN every write in the open DDL zone is pinned to; `None` outside a zone.
    ddl_zone_lsn: Option<NonZeroU64>,
}

impl ApplyContext {
    pub(super) fn new() -> Self {
        Self {
            mode: ApplyMode::Replay,
            ddl_zone_lsn: None,
        }
    }
    #[inline]
    pub(super) fn mode(&self) -> ApplyMode {
        self.mode
    }
    #[inline]
    pub(super) fn go_live(&mut self) {
        self.mode = ApplyMode::Live;
    }
    #[inline]
    pub(super) fn ddl_zone_lsn(&self) -> Option<NonZeroU64> {
        self.ddl_zone_lsn
    }

    /// Pin every system-table write until [`Self::close_ddl_zone`] to `lsn`.
    #[inline]
    pub(crate) fn open_ddl_zone(&mut self, lsn: NonZeroU64) {
        self.ddl_zone_lsn = Some(lsn);
    }

    /// Close the DDL zone after it is durably committed (or rolled back).
    #[inline]
    pub(crate) fn close_ddl_zone(&mut self) {
        self.ddl_zone_lsn = None;
    }
}
