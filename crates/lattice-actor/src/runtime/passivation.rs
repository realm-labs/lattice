use std::future::pending;

use tokio::time::sleep;

use super::PassivationPolicy;

/// Waits for the configured idle period while the Actor runtime is ready to
/// receive its next command.
///
/// The caller cancels this future as soon as a mailbox command becomes ready
/// and constructs a fresh one after that command has finished. Consequently,
/// startup and handler execution time do not count as idle time.
pub(super) async fn wait_for_idle_timeout(passivation: PassivationPolicy) {
    match passivation {
        PassivationPolicy::Disabled => pending().await,
        PassivationPolicy::IdleTimeout(timeout) => sleep(timeout).await,
    }
}
