use std::future::Future;

use tracing::warn;

pub(crate) async fn default_shutdown_signal() {
    #[cfg(unix)]
    {
        let ctrl_c = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                warn!(%error, "failed to listen for ctrl-c shutdown signal");
            }
        };
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                first_shutdown_signal(ctrl_c, async move {
                    let _ = sigterm.recv().await;
                })
                .await;
            }
            Err(error) => {
                warn!(%error, "failed to listen for sigterm shutdown signal");
                ctrl_c.await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to listen for ctrl-c shutdown signal");
        }
    }
}

#[cfg_attr(not(unix), allow(dead_code))]
pub(crate) async fn first_shutdown_signal<C, T>(ctrl_c: C, terminate: T)
where
    C: Future<Output = ()>,
    T: Future<Output = ()>,
{
    tokio::pin!(ctrl_c);
    tokio::pin!(terminate);
    tokio::select! {
        () = &mut ctrl_c => {}
        () = &mut terminate => {}
    }
}
