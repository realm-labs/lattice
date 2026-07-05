use lattice_actor::{Actor, ActorCallError, ActorHandle, Handler};
use lattice_core::Epoch;
use tonic::{Request, Response, Status};
use tracing::{Instrument, debug, warn};

use crate::dedup::{RequestDedupKey, RequestDeduplicator};
use crate::metadata::metadata_status;
use crate::security::security_status;
use crate::security::{PeerIdentity, RpcSecurityPolicy};
use crate::{RoutedRequest, Rpc, RpcContext, RpcRequest};

#[derive(Debug)]
pub struct ActorRpcAdapter<A: Actor> {
    handle: ActorHandle<A>,
    owner_epoch: Option<Epoch>,
}

impl<A: Actor> Clone for ActorRpcAdapter<A> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            owner_epoch: self.owner_epoch,
        }
    }
}

impl<A: Actor> ActorRpcAdapter<A> {
    pub fn new(handle: ActorHandle<A>) -> Self {
        Self {
            handle,
            owner_epoch: None,
        }
    }

    pub fn with_owner_epoch(mut self, owner_epoch: Epoch) -> Self {
        self.owner_epoch = Some(owner_epoch);
        self
    }

    pub async fn unary<Req>(&self, request: Request<Req>) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let ctx = RpcContext::from_metadata(request.metadata()).map_err(metadata_status)?;
        self.validate_owner_epoch(&ctx)?;

        let req = request.into_inner();
        self.dispatch(req, ctx).await
    }

    pub async fn unary_secure<Req>(
        &self,
        request: Request<Req>,
        policy: &RpcSecurityPolicy,
        peer: Option<&PeerIdentity>,
    ) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let ctx = RpcContext::from_metadata(request.metadata()).map_err(metadata_status)?;
        self.validate_owner_epoch(&ctx)?;
        policy.validate(&ctx, peer).map_err(security_status)?;

        let req = request.into_inner();
        self.dispatch(req, ctx).await
    }

    async fn dispatch<Req>(&self, req: Req, ctx: RpcContext) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        self.dispatch_raw(req, ctx).await.map_err(actor_call_status)
    }

    async fn dispatch_raw<Req>(
        &self,
        req: Req,
        ctx: RpcContext,
    ) -> Result<Response<Req::Reply>, ActorCallError>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let actor_kind = req.actor_kind();
        let route_key = req.route_key();
        let span = tracing::info_span!(
            "rpc.server",
            otel.kind = "server",
            rpc.method = Req::METHOD,
            actor.kind = actor_kind.as_str(),
            route.key = ?route_key,
            request.id = ctx.request_id.as_str(),
            source.service = ctx.source_service.as_str(),
            source.instance = ctx.source_instance.as_str()
        );
        async {
            debug!(
                rpc.method = Req::METHOD,
                request.id = ctx.request_id.as_str(),
                "dispatching rpc request to actor"
            );
            match self.handle.call(Rpc { req, ctx }).await {
                Ok(reply) => {
                    debug!(rpc.method = Req::METHOD, "rpc request handled by actor");
                    Ok(Response::new(reply))
                }
                Err(error) => {
                    warn!(
                        rpc.method = Req::METHOD,
                        %error,
                        "actor failed to handle rpc request"
                    );
                    Err(error)
                }
            }
        }
        .instrument(span)
        .await
    }

    pub async fn unary_dedup<Req>(
        &self,
        request: Request<Req>,
        deduplicator: &RequestDeduplicator,
    ) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let ctx = RpcContext::from_metadata(request.metadata()).map_err(metadata_status)?;
        self.validate_owner_epoch(&ctx)?;
        self.dispatch_dedup(request, ctx, deduplicator).await
    }

    pub async fn unary_dedup_secure<Req>(
        &self,
        request: Request<Req>,
        policy: &RpcSecurityPolicy,
        peer: Option<&PeerIdentity>,
        deduplicator: &RequestDeduplicator,
    ) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let ctx = RpcContext::from_metadata(request.metadata()).map_err(metadata_status)?;
        self.validate_owner_epoch(&ctx)?;
        policy.validate(&ctx, peer).map_err(security_status)?;
        self.dispatch_dedup(request, ctx, deduplicator).await
    }

    async fn dispatch_dedup<Req>(
        &self,
        request: Request<Req>,
        ctx: RpcContext,
        deduplicator: &RequestDeduplicator,
    ) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let key = RequestDedupKey::new(Req::METHOD, &ctx.request_id);
        if !deduplicator.begin(&key) {
            return Err(Status::already_exists("duplicate request id"));
        }

        let req = request.into_inner();
        match self.dispatch_raw(req, ctx).await {
            Ok(response) => Ok(response),
            Err(error) => {
                if should_release_dedup_key(&error) {
                    deduplicator.forget(&key);
                }
                Err(actor_call_status(error))
            }
        }
    }

    fn validate_owner_epoch(&self, ctx: &RpcContext) -> Result<(), Status> {
        if let (Some(expected), Some(actual)) = (ctx.route_epoch, self.owner_epoch)
            && expected != actual
        {
            return Err(Status::failed_precondition("route epoch mismatch"));
        }
        Ok(())
    }
}

fn should_release_dedup_key(error: &ActorCallError) -> bool {
    matches!(
        error,
        ActorCallError::MailboxFull | ActorCallError::MailboxClosed
    )
}

fn actor_call_status(error: ActorCallError) -> Status {
    match error {
        ActorCallError::MailboxFull => Status::resource_exhausted(error.to_string()),
        ActorCallError::MailboxClosed | ActorCallError::ResponseDropped => {
            Status::unavailable(error.to_string())
        }
        ActorCallError::Handler(_) => Status::internal(error.to_string()),
    }
}
