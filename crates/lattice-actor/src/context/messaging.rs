//! Typed messaging performed from inside an Actor turn.
//!
//! Targets own their delivery capability. The local runtime therefore does
//! not need to know whether an integration implements a target with a mailbox,
//! a network route, or another transport.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use super::ActorContext;
use crate::{
    error::{ActorCallError, ActorTellError},
    handle::ActorHandle,
    state_machine::Accepts,
    traits::{Actor, Handler, Message, Request, Responder},
};

/// A self-contained, statically typed destination for a one-way message.
pub trait TellTarget<M: Message>: Sync {
    type Error;

    fn tell(&self, message: M) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A self-contained, statically typed destination for a request.
pub trait AskTarget<R: Request>: Sync {
    type Error;

    fn invalid_timeout_error() -> Self::Error;

    fn ask_until(
        &self,
        request: R,
        deadline: Instant,
    ) -> impl Future<Output = Result<R::Response, Self::Error>> + Send;
}

impl<B, M> TellTarget<M> for ActorHandle<B>
where
    B: Actor + Handler<M>,
    B::Behavior: Accepts<M>,
    M: Message,
{
    type Error = ActorTellError<M>;

    fn tell(&self, message: M) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ActorHandle::tell(self, message)
    }
}

impl<B, R> AskTarget<R> for ActorHandle<B>
where
    B: Actor + Responder<R>,
    B::Behavior: Accepts<R>,
    R: Request,
{
    type Error = ActorCallError;

    fn invalid_timeout_error() -> Self::Error {
        ActorCallError::InvalidTimeout
    }

    fn ask_until(
        &self,
        request: R,
        deadline: Instant,
    ) -> impl Future<Output = Result<R::Response, Self::Error>> + Send {
        ActorHandle::ask_until(self, request, deadline)
    }
}

impl<A: Actor> ActorContext<A> {
    pub fn tell<'a, T, M>(
        &self,
        target: &'a T,
        message: M,
    ) -> impl Future<Output = Result<(), T::Error>> + Send + 'a
    where
        T: TellTarget<M> + 'a,
        M: Message,
    {
        target.tell(message)
    }

    /// Sends a request while preserving the current request deadline.
    pub async fn ask<T, R>(
        &self,
        target: &T,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response, T::Error>
    where
        T: AskTarget<R>,
        R: Request,
    {
        let requested_deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(T::invalid_timeout_error)?;
        let deadline = self
            .current_deadline
            .map_or(requested_deadline, |parent| parent.min(requested_deadline));
        target.ask_until(request, deadline).await
    }
}
