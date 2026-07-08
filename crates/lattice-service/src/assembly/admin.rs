use std::net::SocketAddr;

use lattice_core::kind::ActorKind;
use lattice_ops::ops_config::AdminHttpConfig;
use tokio::net::TcpListener;

use crate::error::LatticeServiceError;
use crate::runtime::admin::AdminHttpServer;

pub(crate) async fn build_admin_http(
    config: Option<AdminHttpConfig>,
    actor_kinds: Vec<ActorKind>,
) -> Result<Option<AdminHttpServer>, LatticeServiceError> {
    let Some(config) = config else {
        return Ok(None);
    };
    let bind = config
        .bind
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));
    let listener = TcpListener::bind(bind).await?;
    Ok(Some(AdminHttpServer {
        listener,
        auth: config.build_auth(),
        actor_kinds,
    }))
}
