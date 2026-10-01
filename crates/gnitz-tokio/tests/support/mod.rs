//! What both suites drive a verb with.

use std::future::Future;
use std::task::Poll;

/// `verb` polled once, which takes it through its submit. Verbs primed in
/// sequence from one task therefore reach the driver in call order — while the
/// sequence stays within the request channel's depth and tokio's per-poll
/// budget, past either of which a first poll suspends before it sends.
pub async fn submitted<F: Future>(verb: F) -> impl Future<Output = F::Output> {
    let mut verb = Box::pin(verb);
    // A driver on another thread can have the reply back within that one poll.
    let early = std::future::poll_fn(|cx| {
        Poll::Ready(match verb.as_mut().poll(cx) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        })
    })
    .await;
    async move {
        match early {
            Some(output) => output,
            None => verb.await,
        }
    }
}
