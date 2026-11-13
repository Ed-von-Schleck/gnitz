//! The futex park every transport sleeps through: [`Park`], one word a side
//! sleeps on and its peer wakes, [`WorkerParks`], the mapping where each
//! worker sleeps for a SAL group or an exchange part, and [`WorkerSet`], a
//! subset of the workers.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use gnitz_foundation::posix_io;
use gnitz_wire::{low_bits_mask, BitIter, MAX_WORKERS};

// ---------------------------------------------------------------------------
// Futex wait/wake
// ---------------------------------------------------------------------------

/// One v1 futex call on `word`, with no `FUTEX_PRIVATE_FLAG` — a park word is
/// shared: `Err` holds the errno.
#[inline]
fn futex(word: *const AtomicU32, op: libc::c_int, val: u32) -> Result<(), i32> {
    let rc = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const libc::c_void,
            op,
            val as libc::c_int,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        )
    };
    match rc < 0 {
        // `libc::syscall` returns -1, not -errno.
        true => Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0)),
        false => Ok(()),
    }
}

/// Wake the waiter parked on `word`. Aborts rather than reporting: a lost wake
/// leaves the peer blocked on a queue that has work in it, so there is no
/// caller-side answer to distinguish. The abort names the caller's location.
#[inline]
#[track_caller]
fn futex_wake(word: *const AtomicU32) {
    if let Err(e) = futex(word, libc::FUTEX_WAKE, 1) {
        futex_failed(e);
    }
}

/// Outlined so the caller's location is not spilled onto the stack on the
/// paths that never abort.
#[cold]
#[inline(never)]
#[track_caller]
fn futex_failed(errno: i32) -> ! {
    gnitz_fatal_abort!("{}: futex failed: errno={errno}", std::panic::Location::caller());
}

// ---------------------------------------------------------------------------
// Park
// ---------------------------------------------------------------------------

/// Bit 63 of a park word: the parker is armed. Outside the low half a futex compares.
const ARMED: u64 = 1 << 63;

/// One side's sleep, in one word: a value in bits 0..63 that only grows, and the
/// parker's arm in bit 63. The parker arms, re-tests what it waits for, and
/// sleeps on the value the arm returned; a peer wakes it by moving that value.
/// The two kinds of wake order against the arm differently.
///
/// - **An RMW wake** ([`Self::publish_waking`], [`Self::bump`]): the arm and every
///   advance are RMWs on the one atomic, so they are totally ordered: an advance
///   behind the arm sees it and wakes, and an arm behind the advance reads
///   everything the advance released, so the parker's re-test sees it.
/// - **A load wake** ([`Self::take_arm`]): the peer stores the word the parker
///   waits on and then loads the park; the parker arms and then loads that word.
///   All four are SeqCst, so in each pair at least one side sees the other: the
///   peer sees the arm and takes it, or the parker's re-test sees the stored word
///   and does not sleep. A wake that finds no arm costs no RMW.
///
/// A park is alone on its cache line.
#[repr(C, align(64))]
pub(crate) struct Park(AtomicU64);

impl Park {
    /// `n` parks in one fresh shared mapping, never unmapped.
    pub(super) fn map(n: usize) -> std::io::Result<&'static [Park]> {
        let base = posix_io::map_shared(None, (n * size_of::<Park>()).max(1))?;
        // SAFETY: a fresh page-aligned shared mapping of at least `n` parks, never
        // unmapped; zeroed is a `Park` nobody sleeps on.
        Ok(unsafe { std::slice::from_raw_parts(base.cast(), n) })
    }

    #[inline]
    pub(super) fn value(&self) -> u64 {
        self.0.load(Ordering::Acquire) & !ARMED
    }

    /// Drop the arm. The result is deliberately unused: binding it would force a
    /// `lock cmpxchg` retry loop where a discarded one lowers to a `lock and`.
    #[inline]
    fn disarm(&self) {
        self.0.fetch_and(!ARMED, Ordering::SeqCst);
    }

    /// Take the arm, if set, and wake the parker: the load wake, called after the
    /// caller's SeqCst store of the word the parker re-tests.
    ///
    /// One CAS clears the arm and advances the value by one, so an arm has one
    /// taker, and a take ahead of the parker's futex wait still moves the value
    /// it compares. A failed CAS read a value later than the load's: another
    /// take, which woke the parker, or a disarm or a newer arm, which is ordered
    /// after the load and so followed by a re-test that sees the caller's word.
    /// So a failed CAS neither retries nor wakes.
    #[inline]
    #[track_caller]
    pub(super) fn take_arm(&self) {
        let cur = self.0.load(Ordering::SeqCst);
        if cur & ARMED != 0
            && self
                .0
                .compare_exchange(cur, (cur + 1) & !ARMED, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
        {
            futex_wake(self.futex_word());
        }
    }

    /// The value's one writer stores `v`, waking `parker` if this word was armed:
    /// a word whose parker sleeps on a park of its own. The swap takes the arm,
    /// so later publishes wake nobody until a re-arm.
    #[inline]
    pub(super) fn publish_waking(&self, v: u64, parker: &Park) {
        debug_assert_eq!(v & ARMED, 0, "a value reached the armed bit");
        if self.0.swap(v, Ordering::SeqCst) & ARMED != 0 {
            parker.bump();
        }
    }

    /// Ask for a wake on the next publish without waiting here: the parker of a
    /// [`Self::publish_waking`] word, ahead of its own park's arm and re-test.
    #[inline]
    pub(super) fn arm(&self) {
        self.0.fetch_or(ARMED, Ordering::SeqCst);
    }

    /// One of any number of writers advances the value by one, waking an armed
    /// parker. The arm stays the parker's to clear: cleared here in a second RMW,
    /// it could erase an arm taken after the wake.
    #[inline]
    #[track_caller]
    pub(super) fn bump(&self) {
        if self.0.fetch_add(1, Ordering::AcqRel) & ARMED != 0 {
            futex_wake(self.futex_word());
        }
    }

    /// Arm unless `still_waiting`, tested behind the arm, says the wait is over:
    /// the futex word and the value a wait on it compares, with the arm left
    /// set; or `None`, disarmed.
    #[inline]
    pub(super) fn arm_for(&self, still_waiting: impl FnOnce() -> bool) -> Option<(*const AtomicU32, u32)> {
        let at = self.0.fetch_or(ARMED, Ordering::SeqCst) & !ARMED;
        if still_waiting() {
            return Some((self.futex_word(), at as u32));
        }
        self.disarm();
        None
    }

    /// Arm, then sleep until a wake unless `still_waiting`, tested behind the arm,
    /// says otherwise. Disarms either way.
    #[track_caller]
    pub(crate) fn park(&self, still_waiting: impl FnOnce() -> bool) {
        if let Some((word, at)) = self.arm_for(still_waiting) {
            // A wake, a moved value and a signal all return; anything else aborts.
            if let Err(e) = futex(word, libc::FUTEX_WAIT, at) {
                if e != libc::EAGAIN && e != libc::EINTR {
                    futex_failed(e);
                }
            }
            self.disarm();
        }
    }

    /// The 32-bit futex word aliasing the low half. The words a futex waits on
    /// are moved only by a bump or a take, each by one, so every advance moves
    /// the half a futex compares.
    #[inline]
    fn futex_word(&self) -> *const AtomicU32 {
        &self.0 as *const AtomicU64 as *const AtomicU32
    }
}

// ---------------------------------------------------------------------------
// WorkerSet
// ---------------------------------------------------------------------------

/// A subset of the workers. `ALL` is unbounded; every other set holds only
/// launched workers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct WorkerSet(pub(super) u64);

const _: () = assert!(MAX_WORKERS <= u64::BITS as usize);

impl WorkerSet {
    pub(crate) const EMPTY: Self = WorkerSet(0);
    pub(crate) const ALL: Self = WorkerSet(u64::MAX);

    pub(crate) const fn one(w: usize) -> Self {
        WorkerSet(1 << w)
    }

    pub(crate) const fn with(self, w: usize) -> Self {
        WorkerSet(self.0 | 1 << w)
    }

    pub(crate) const fn without(self, w: usize) -> Self {
        WorkerSet(self.0 & !(1 << w))
    }

    pub(crate) const fn within(self, nw: usize) -> Self {
        WorkerSet(self.0 & low_bits_mask(nw))
    }

    pub(crate) const fn contains(self, w: usize) -> bool {
        self.0 >> w & 1 == 1
    }

    pub(crate) const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    pub(crate) const fn union(self, other: Self) -> Self {
        WorkerSet(self.0 | other.0)
    }

    /// `w`'s position among the members, ascending; `None` when it is not one.
    pub(crate) const fn position(self, w: usize) -> Option<usize> {
        match self.contains(w) {
            true => Some((self.0 & low_bits_mask(w)).count_ones() as usize),
            false => None,
        }
    }

    pub(crate) fn iter(self) -> BitIter {
        BitIter(self.0)
    }
}

impl FromIterator<usize> for WorkerSet {
    fn from_iter<I: IntoIterator<Item = usize>>(workers: I) -> Self {
        workers.into_iter().fold(WorkerSet::EMPTY, WorkerSet::with)
    }
}

// ---------------------------------------------------------------------------
// WorkerParks
// ---------------------------------------------------------------------------

/// Every worker's park: where a worker sleeps for a SAL group or an exchange part,
/// and what the master and its peers wake. Mapped once before the fork, never unmapped.
#[derive(Clone, Copy)]
pub(crate) struct WorkerParks(&'static [Park]);

impl WorkerParks {
    pub(crate) fn create(nw: usize) -> std::io::Result<Self> {
        assert!(nw <= MAX_WORKERS, "{nw} workers: a WorkerSet holds at most MAX_WORKERS");
        Ok(WorkerParks(Park::map(nw)?))
    }

    pub(super) fn len(self) -> usize {
        self.0.len()
    }

    /// Wake every worker of `set` that sleeps; members past the launched
    /// workers are none.
    #[track_caller]
    pub(super) fn wake(self, set: WorkerSet) {
        for w in set.within(self.0.len()).iter() {
            self.0[w].bump();
        }
    }

    /// Worker `w`'s park: where it sleeps for a SAL group or an exchange part; its
    /// value counts the wakes `w` was sent. No timeout: `PR_SET_PDEATHSIG` kills
    /// the worker when the master dies.
    pub(crate) fn park(self, w: usize) -> &'static Park {
        &self.0[w]
    }
}

#[cfg(test)]
#[path = "tests/park.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/park.rs"]
mod bench;
