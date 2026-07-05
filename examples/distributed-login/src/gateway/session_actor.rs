use async_trait::async_trait;
use lattice_actor::{Actor, ActorContext, ActorError, Handler};
use lattice_core::ActorRef;
use lattice_gateway::ClientFrame;
use lattice_rpc::Rpc;
use tokio::sync::{mpsc, oneshot};

use crate::game::{PushToClientReply, PushToClientRequest};

pub(super) struct GatewaySessionActor {
    session_id: String,
    tx: mpsc::Sender<ClientFrame>,
    self_ref_tx: Option<oneshot::Sender<ActorRef>>,
}

impl GatewaySessionActor {
    pub(super) fn new(
        session_id: String,
        tx: mpsc::Sender<ClientFrame>,
        self_ref_tx: oneshot::Sender<ActorRef>,
    ) -> Self {
        Self {
            session_id,
            tx,
            self_ref_tx: Some(self_ref_tx),
        }
    }
}

#[async_trait]
impl Actor for GatewaySessionActor {
    type Error = ActorError;
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(self_ref_tx) = self.self_ref_tx.take() {
            let _ = self_ref_tx.send(ctx.require_self_ref()?.clone());
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Rpc<PushToClientRequest>> for GatewaySessionActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<PushToClientRequest>,
    ) -> Result<PushToClientReply, ActorError> {
        if msg.req.session_id != self.session_id {
            return Ok(PushToClientReply {
                ok: false,
                message: format!(
                    "gateway session actor {} cannot serve {}",
                    self.session_id, msg.req.session_id
                ),
            });
        }

        self.tx
            .send(ClientFrame {
                msg_id: msg.req.msg_id,
                payload: msg.req.payload,
            })
            .await
            .map_err(|_| ActorError::new("client writer task is closed"))?;
        Ok(PushToClientReply {
            ok: true,
            message: "pushed by gateway session actor".to_string(),
        })
    }
}
