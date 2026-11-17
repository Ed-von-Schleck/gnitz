//! Where a client on an event loop runs work that blocks — its connect, and
//! its mirror store's disk work: one pool every such client in the process
//! shares. A job runs on an idle thread, or on one started for it; a thread
//! idle for [`IDLE`] ends, so a process whose clients block on nothing keeps
//! no thread here.

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Duration;

use gnitz_core::Job;

use crate::lock;

/// How long a thread waits for its next job before it ends.
const IDLE: Duration = Duration::from_secs(10);

struct Queue {
    jobs: VecDeque<Job>,
    /// Threads waiting for a job.
    idle: usize,
}

static QUEUE: Mutex<Queue> = Mutex::new(Queue { jobs: VecDeque::new(), idle: 0 });
static QUEUED: Condvar = Condvar::new();

/// Run `job` on the pool. A job no thread can be started for is dropped.
pub(crate) fn spawn(job: Job) {
    let mut queue = lock(&QUEUE);
    queue.jobs.push_back(job);
    // Each idle thread takes one queued job.
    if queue.idle >= queue.jobs.len() {
        drop(queue);
        QUEUED.notify_one();
        return;
    }
    drop(queue);
    if std::thread::Builder::new()
        .name("gnitz-jobs".into())
        .spawn(work)
        .is_err()
    {
        drop(lock(&QUEUE).jobs.pop_back());
    }
}

/// A pool thread: run queued jobs until none comes for [`IDLE`].
fn work() {
    let mut queue = lock(&QUEUE);
    loop {
        if let Some(job) = queue.jobs.pop_front() {
            drop(queue);
            job();
            queue = lock(&QUEUE);
            continue;
        }
        queue.idle += 1;
        let (woken, waited) = QUEUED.wait_timeout(queue, IDLE).unwrap_or_else(PoisonError::into_inner);
        queue = woken;
        queue.idle -= 1;
        if waited.timed_out() && queue.jobs.is_empty() {
            return;
        }
    }
}
