//! Bound, location-transparent Actor messaging capabilities.

use std::{fmt, time::Instant};

use lattice_actor::{
    context::{AskTarget, TellTarget},
    traits::{Message, Request},
    watch::WatchTarget,
};
use lattice_core::actor_address::{
    ActorAddress, EntityAddress, ProtocolTag, RecipientAddress, SingletonAddress,
};

use crate::{
    protocol::{Protocol, SupportsAsk, SupportsTell},
    recipient::{ActorSystem, RecipientError, WatchSubscription},
};

macro_rules! bound_reference {
    ($name:ident, $address:ident) => {
        #[derive(Clone)]
        pub struct $name<P: ProtocolTag> {
            address: $address<P>,
            actor_system: ActorSystem,
        }

        impl<P: ProtocolTag> $name<P> {
            pub(crate) fn new(address: $address<P>, actor_system: ActorSystem) -> Self {
                Self {
                    address,
                    actor_system,
                }
            }

            pub fn address(&self) -> &$address<P> {
                &self.address
            }

            pub fn into_address(self) -> $address<P> {
                self.address
            }

            pub async fn tell<M>(&self, message: M) -> Result<(), RecipientError>
            where
                P: Protocol + SupportsTell<M>,
                M: Message,
            {
                self.actor_system.tell(&self.address, message).await
            }

            pub async fn ask<R>(
                &self,
                request: R,
                timeout: std::time::Duration,
            ) -> Result<R::Response, RecipientError>
            where
                P: Protocol + SupportsAsk<R>,
                R: Request,
            {
                self.actor_system.ask(&self.address, request, timeout).await
            }

            pub async fn watch(&self) -> Result<WatchSubscription, RecipientError>
            where
                P: Protocol,
            {
                self.actor_system.watch(&self.address).await
            }
        }

        impl<P: ProtocolTag> fmt::Debug for $name<P> {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("address", &self.address)
                    .finish_non_exhaustive()
            }
        }

        impl<P, M> TellTarget<M> for $name<P>
        where
            P: Protocol + SupportsTell<M>,
            M: Message,
        {
            type Error = RecipientError;

            fn tell(
                &self,
                message: M,
            ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
                $name::tell(self, message)
            }
        }

        impl<P, R> AskTarget<R> for $name<P>
        where
            P: Protocol + SupportsAsk<R>,
            R: Request,
        {
            type Error = RecipientError;

            fn invalid_timeout_error() -> Self::Error {
                RecipientError::InvalidTimeout
            }

            fn ask_until(
                &self,
                request: R,
                deadline: Instant,
            ) -> impl std::future::Future<Output = Result<R::Response, Self::Error>> + Send {
                self.actor_system
                    .ask_until(self.address.clone().into(), request, deadline)
            }
        }

        impl<P> WatchTarget for $name<P>
        where
            P: Protocol,
        {
            type Error = RecipientError;
            type Subscription = WatchSubscription;

            fn watch(
                &self,
            ) -> impl std::future::Future<Output = Result<Self::Subscription, Self::Error>> + Send
            {
                $name::watch(self)
            }
        }
    };
}

bound_reference!(ActorRef, ActorAddress);
bound_reference!(EntityRef, EntityAddress);
bound_reference!(SingletonRef, SingletonAddress);

#[derive(Clone, Debug)]
pub enum Recipient<P: ProtocolTag> {
    Actor(ActorRef<P>),
    Entity(EntityRef<P>),
    Singleton(SingletonRef<P>),
}

impl<P: ProtocolTag> From<ActorRef<P>> for Recipient<P> {
    fn from(value: ActorRef<P>) -> Self {
        Self::Actor(value)
    }
}

impl<P: ProtocolTag> From<EntityRef<P>> for Recipient<P> {
    fn from(value: EntityRef<P>) -> Self {
        Self::Entity(value)
    }
}

impl<P: ProtocolTag> From<SingletonRef<P>> for Recipient<P> {
    fn from(value: SingletonRef<P>) -> Self {
        Self::Singleton(value)
    }
}

impl<P: ProtocolTag> Recipient<P> {
    pub fn address(&self) -> RecipientAddress<P> {
        match self {
            Self::Actor(reference) => reference.address().clone().into(),
            Self::Entity(reference) => reference.address().clone().into(),
            Self::Singleton(reference) => reference.address().clone().into(),
        }
    }

    pub async fn tell<M>(&self, message: M) -> Result<(), RecipientError>
    where
        P: Protocol + SupportsTell<M>,
        M: Message,
    {
        match self {
            Self::Actor(target) => target.tell(message).await,
            Self::Entity(target) => target.tell(message).await,
            Self::Singleton(target) => target.tell(message).await,
        }
    }

    pub async fn ask<R>(
        &self,
        request: R,
        timeout: std::time::Duration,
    ) -> Result<R::Response, RecipientError>
    where
        P: Protocol + SupportsAsk<R>,
        R: Request,
    {
        match self {
            Self::Actor(target) => target.ask(request, timeout).await,
            Self::Entity(target) => target.ask(request, timeout).await,
            Self::Singleton(target) => target.ask(request, timeout).await,
        }
    }

    pub async fn watch(&self) -> Result<WatchSubscription, RecipientError>
    where
        P: Protocol,
    {
        match self {
            Self::Actor(target) => target.watch().await,
            Self::Entity(target) => target.watch().await,
            Self::Singleton(target) => target.watch().await,
        }
    }
}

impl<P, M> TellTarget<M> for Recipient<P>
where
    P: Protocol + SupportsTell<M>,
    M: Message,
{
    type Error = RecipientError;

    fn tell(
        &self,
        message: M,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let target = self.clone();
        async move { target.tell(message).await }
    }
}

impl<P, R> AskTarget<R> for Recipient<P>
where
    P: Protocol + SupportsAsk<R>,
    R: Request,
{
    type Error = RecipientError;

    fn invalid_timeout_error() -> Self::Error {
        RecipientError::InvalidTimeout
    }

    fn ask_until(
        &self,
        request: R,
        deadline: Instant,
    ) -> impl std::future::Future<Output = Result<R::Response, Self::Error>> + Send {
        let target = self.clone();
        async move {
            match target {
                Self::Actor(target) => AskTarget::ask_until(&target, request, deadline).await,
                Self::Entity(target) => AskTarget::ask_until(&target, request, deadline).await,
                Self::Singleton(target) => AskTarget::ask_until(&target, request, deadline).await,
            }
        }
    }
}

impl<P> WatchTarget for Recipient<P>
where
    P: Protocol,
{
    type Error = RecipientError;
    type Subscription = WatchSubscription;

    fn watch(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Subscription, Self::Error>> + Send {
        let target = self.clone();
        async move { target.watch().await }
    }
}
