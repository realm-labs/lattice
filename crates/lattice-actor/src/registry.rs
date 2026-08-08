use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    num::NonZeroU64,
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use dashmap::{DashMap, mapref::entry::Entry};
use lattice_core::{
    actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    },
    id::ActorId,
    kind::ActorKind,
    service_context::ServiceContext,
};
use thiserror::Error;
use tokio::sync::{Semaphore, watch};

use crate::{
    directory::ActivationDirectory,
    error::{ActorActivationError, ActorAdminError, ActorError},
    handle::{ActorHandle, StopFailureRecord},
    mailbox::MailboxConfig,
    observation::ActorObserverHandle,
    protocol::{ActorProtocolBinding, Protocol},
    recipient::ActorSystem,
    runtime::{
        ActorSpawnContext, ActorSpawnOptions, PassivationPolicy, ShardMigrationPolicy,
        spawn_actor_with_self_ref, spawner::ActorSpawner,
    },
    traits::{Actor, ActorLifecycleState, EntityActivationState, PassivationReason, StopReason},
    watch::LocalActorRef,
};

mod quarantine;

#[derive(Debug, Clone)]
pub struct ActorRegistryConfig {
    pub mailbox: MailboxConfig,
    pub passivation: PassivationPolicy,
    pub shard_migration: ShardMigrationPolicy,
    pub waiter_capacity: usize,
    pub waiter_timeout: Duration,
    pub quarantine_capacity: usize,
    pub actor_ref: Option<ActorRefConfig>,
    pub service: ServiceContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorRefConfig {
    pub cluster_id: ClusterId,
    pub node_address: NodeAddress,
    pub node_incarnation: NodeIncarnation,
}

impl Default for ActorRegistryConfig {
    fn default() -> Self {
        Self {
            mailbox: MailboxConfig::default(),
            passivation: PassivationPolicy::Disabled,
            shard_migration: ShardMigrationPolicy::BlockRunningActors,
            waiter_capacity: 1024,
            waiter_timeout: Duration::from_secs(5),
            quarantine_capacity: 1024,
            actor_ref: None,
            service: ServiceContext::empty(),
        }
    }
}

pub struct ActorRegistry<A: Actor> {
    kind: ActorKind,
    config: ActorRegistryConfig,
    protocol_id: Option<ProtocolId>,
    entries: Arc<DashMap<ActorId, RegistryEntry<A>>>,
    exact_entries: Arc<DashMap<ActivationId, ExactRegistryEntry<A>>>,
    quarantined: Arc<DashMap<LocalActorRef, QuarantinedEntry<A>>>,
    actor_system: Arc<OnceLock<ActorSystem>>,
    fencing_token_resolvers: Arc<RwLock<BTreeMap<String, ActorFencingTokenResolver>>>,
    observer: ActorObserverHandle,
    spawner: ActorSpawner,
}

type ActorFencingTokenResolver = Arc<dyn Fn(&ActorId) -> Option<u64> + Send + Sync + 'static>;

/// Monotonic authority generation attached to one placement-managed Actor activation.
///
/// The token is produced by the framework authority layer and consumed by storage integrations;
/// application Actors do not allocate or increment it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorFencingToken(NonZeroU64);

impl ActorFencingToken {
    fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Proof that one external authority generation was validated by this Registry.
///
/// Only framework authority adapters can obtain this proof. Application callers use
/// [`ActorRegistry::get_or_load`], which resolves authority automatically.
#[doc(hidden)]
pub struct ValidatedActorAuthority {
    actor_id: ActorId,
    fencing_token: ActorFencingToken,
    registry_identity: usize,
}

struct ExactRegistryEntry<A: Actor> {
    handle: ActorHandle<A>,
    local_ref: LocalActorRef,
}

impl<A: Actor> fmt::Debug for ActorRegistry<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorRegistry")
            .field("kind", &self.kind)
            .field("config", &self.config)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ActorCreateContext {
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
    pub service: ServiceContext,
    fencing_token: Option<ActorFencingToken>,
}

impl ActorCreateContext {
    /// Returns the placement authority generation for this activation, when it is managed by an
    /// external authority such as a shard or singleton placement slot.
    pub const fn fencing_token(&self) -> Option<ActorFencingToken> {
        self.fencing_token
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedActorFailure {
    pub actor_id: ActorId,
    pub local_ref: LocalActorRef,
    pub failure: StopFailureRecord,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegistryDrainResult {
    pub requested: usize,
    pub stopped: usize,
    pub retained_failures: Vec<RetainedActorFailure>,
    pub request_failures: Vec<ActorId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineDiagnostics {
    pub actor_id: ActorId,
    pub local_ref: LocalActorRef,
    pub actor_ref: Option<ActorRef>,
    pub failure: StopFailureRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorCellDiagnostics {
    pub actor_id: ActorId,
    pub local_ref: LocalActorRef,
    pub lifecycle: ActorLifecycleState,
    pub quarantined: bool,
    pub stop_failure: Option<StopFailureRecord>,
}

struct QuarantinedEntry<A: Actor> {
    actor_id: ActorId,
    handle: ActorHandle<A>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorRegistryMetricsSnapshot {
    pub retained_stop_failures: usize,
    pub quarantine_used: usize,
    pub quarantine_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorQuarantineError {
    #[error("actor is not retained in StopFailed")]
    NotRetained,
    #[error("actor quarantine capacity {capacity} is exhausted")]
    Capacity { capacity: usize },
    #[error(transparent)]
    Admin(#[from] ActorAdminError),
}

impl RegistryDrainResult {
    pub fn completed(&self) -> bool {
        self.retained_failures.is_empty() && self.request_failures.is_empty()
    }
}

#[async_trait]
pub trait ActorFactory<A>: Clone + Send + Sync + 'static
where
    A: Actor,
{
    async fn create(&self, ctx: ActorCreateContext) -> Result<A, A::Error>;
}

#[async_trait]
pub trait ActorLoader<A>: Clone + Send + Sync + 'static
where
    A: Actor,
{
    async fn load(&self, ctx: ActorCreateContext) -> Result<A, A::Error>;
}

impl<A: Actor> ActorRegistry<A> {
    pub fn new(kind: ActorKind, config: ActorRegistryConfig) -> Self {
        assert!(
            config.actor_ref.is_none(),
            "registries with exact ActorRefs must be constructed with ActorRegistry::new_bound"
        );
        assert!(
            config.quarantine_capacity > 0,
            "quarantine capacity must be nonzero"
        );
        Self {
            kind,
            config,
            protocol_id: None,
            entries: Arc::new(DashMap::new()),
            exact_entries: Arc::new(DashMap::new()),
            quarantined: Arc::new(DashMap::new()),
            actor_system: Arc::new(OnceLock::new()),
            fencing_token_resolvers: Arc::new(RwLock::new(BTreeMap::new())),
            observer: ActorObserverHandle::default(),
            spawner: ActorSpawner::task_per_actor(),
        }
    }

    /// Constructs a registry whose exact activation references are bound to
    /// the supplied server protocol. The reference protocol ID is derived from
    /// the binding and cannot drift from the registered dispatcher.
    pub fn new_bound<P: Protocol>(
        kind: ActorKind,
        config: ActorRegistryConfig,
        protocol: &ActorProtocolBinding<A, P>,
    ) -> Self {
        assert!(
            config.quarantine_capacity > 0,
            "quarantine capacity must be nonzero"
        );
        Self {
            kind,
            config,
            protocol_id: Some(protocol.protocol_id()),
            entries: Arc::new(DashMap::new()),
            exact_entries: Arc::new(DashMap::new()),
            quarantined: Arc::new(DashMap::new()),
            actor_system: Arc::new(OnceLock::new()),
            fencing_token_resolvers: Arc::new(RwLock::new(BTreeMap::new())),
            observer: ActorObserverHandle::default(),
            spawner: ActorSpawner::task_per_actor(),
        }
    }

    pub fn with_observer(mut self, observer: ActorObserverHandle) -> Self {
        self.observer = observer;
        self
    }

    #[doc(hidden)]
    pub fn install_actor_system(&self, actor_system: ActorSystem) -> Result<(), ActorSystem> {
        self.actor_system.set(actor_system)
    }

    pub fn kind(&self) -> &ActorKind {
        &self.kind
    }

    /// Installs an authority resolver used by direct registry activations.
    ///
    /// A Registry may host several placement routes, so differently named resolvers are composed.
    /// Reinstalling the same name replaces its previous authority view. Returning `None` means
    /// this resolver does not own the Actor ID; if no installed resolver owns it, a new activation
    /// is rejected before its loader runs.
    #[doc(hidden)]
    pub fn install_fencing_token_resolver<F>(&self, resolver_name: impl Into<String>, resolver: F)
    where
        F: Fn(&ActorId) -> Option<u64> + Send + Sync + 'static,
    {
        self.fencing_token_resolvers
            .write()
            .expect("actor fencing token resolver poisoned")
            .insert(resolver_name.into(), Arc::new(resolver));
    }

    pub fn protocol_id(&self) -> Option<ProtocolId> {
        self.protocol_id
    }

    pub fn shard_migration_policy(&self) -> ShardMigrationPolicy {
        self.config.shard_migration
    }

    pub fn running_actor_ids(&self) -> Vec<ActorId> {
        self.entries
            .iter()
            .filter_map(|entry| match entry.value() {
                RegistryEntry::Running(handle)
                    if is_business_admitted(handle.lifecycle_state()) =>
                {
                    Some(entry.key().clone())
                }
                RegistryEntry::Running(_) => None,
                RegistryEntry::Activating(_) => None,
            })
            .collect()
    }

    /// Returns every nonterminal activation still owned by the active registry.
    ///
    /// This intentionally includes Starting, Passivating, Stopping, and StopFailed
    /// cells so an external authority fence cannot miss an in-flight stop.
    pub fn active_actor_ids(&self) -> Vec<ActorId> {
        self.entries
            .iter()
            .filter_map(|entry| match entry.value() {
                RegistryEntry::Running(handle)
                    if !is_terminal(handle.lifecycle_state())
                        && handle.lifecycle_state() != ActorLifecycleState::Quarantined =>
                {
                    Some(entry.key().clone())
                }
                RegistryEntry::Running(_) | RegistryEntry::Activating(_) => None,
            })
            .collect()
    }

    pub fn activation_state(&self, actor_id: &ActorId) -> EntityActivationState {
        match self.entries.get(actor_id).as_deref() {
            Some(RegistryEntry::Running(_)) => EntityActivationState::Active,
            Some(RegistryEntry::Activating(activation)) => activation.state(),
            None => EntityActivationState::Absent,
        }
    }

    pub fn get_running(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        if self.resolve_fencing_token(actor_id).is_err() {
            return None;
        }
        match self.entries.get(actor_id).as_deref() {
            Some(RegistryEntry::Running(handle))
                if is_business_admitted(handle.lifecycle_state()) =>
            {
                Some(handle.clone())
            }
            Some(RegistryEntry::Running(_)) => None,
            Some(RegistryEntry::Activating(_)) | None => None,
        }
    }

    pub fn get_exact(&self, actor_ref: &ActorRef) -> Option<ActorHandle<A>> {
        if self.config.actor_ref.as_ref().is_none_or(|config| {
            config.cluster_id != *actor_ref.cluster_id()
                || config.node_address != *actor_ref.node_address()
                || config.node_incarnation != actor_ref.node_incarnation()
                || self.protocol_id != Some(actor_ref.protocol_id())
        }) {
            return None;
        }
        if let Some(directory) = self.config.service.extension::<ActivationDirectory>()
            && let Some(handle) = directory.resolve(actor_ref)
        {
            return Some(handle);
        }
        let exact = self.exact_entries.get(&actor_ref.activation_id())?;
        if is_business_admitted(exact.handle.lifecycle_state())
            && exact
                .handle
                .actor_ref()
                .is_some_and(|current| current.same_activation(actor_ref))
        {
            Some(exact.handle.clone())
        } else {
            None
        }
    }

    pub async fn remove(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        let handle = self.entry_handle(actor_id)?;
        if is_business_admitted(handle.lifecycle_state()) {
            let _ = handle.stop(StopReason::Requested).await;
        }
        Some(handle)
    }

    pub async fn drain(&self) -> RegistryDrainResult {
        let actor_ids = self
            .entries
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        self.drain_actor_ids(actor_ids).await
    }

    /// Performs a destructive process-shutdown cleanup after first giving every
    /// active Actor its normal persistence opportunity.
    pub async fn force_shutdown(&self, reason: &str, ticket: &str) -> Vec<ActorCellDiagnostics> {
        let drained = self.drain().await;
        for retained in drained.retained_failures {
            if let Some(handle) = self.entry_handle(&retained.actor_id)
                && handle.local_ref() == retained.local_ref
            {
                let _ = handle
                    .force_stop(reason.to_owned(), ticket.to_owned())
                    .await;
            }
        }
        let quarantined = self
            .quarantined
            .iter()
            .map(|entry| entry.value().handle.clone())
            .chain(self.entries.iter().filter_map(|entry| match entry.value() {
                RegistryEntry::Running(handle)
                    if handle.lifecycle_state() == ActorLifecycleState::Quarantined =>
                {
                    Some(handle.clone())
                }
                RegistryEntry::Running(_) | RegistryEntry::Activating(_) => None,
            }))
            .collect::<Vec<_>>();
        for handle in quarantined {
            if matches!(
                handle.lifecycle_state(),
                ActorLifecycleState::StopFailed | ActorLifecycleState::Quarantined
            ) {
                let _ = handle
                    .force_stop(reason.to_owned(), ticket.to_owned())
                    .await;
            }
        }
        self.live_cells()
    }

    pub async fn drain_actor_ids<I>(&self, actor_ids: I) -> RegistryDrainResult
    where
        I: IntoIterator<Item = ActorId>,
    {
        let mut result = RegistryDrainResult::default();
        for actor_id in actor_ids {
            let Some(handle) = self.entry_handle(&actor_id) else {
                continue;
            };
            let mut lifecycle = handle.subscribe_lifecycle();
            match handle.lifecycle_state() {
                ActorLifecycleState::StopFailed => {
                    if let Some(failure) = handle.inspect_stop_failure() {
                        result.retained_failures.push(RetainedActorFailure {
                            actor_id,
                            local_ref: handle.local_ref(),
                            failure,
                        });
                    }
                    continue;
                }
                ActorLifecycleState::Stopped | ActorLifecycleState::Quarantined => continue,
                ActorLifecycleState::Starting | ActorLifecycleState::Running => {
                    result.requested += 1;
                    if handle
                        .stop(StopReason::Passivated(PassivationReason::Drain))
                        .await
                        .is_err()
                    {
                        result.request_failures.push(actor_id);
                        continue;
                    }
                }
                ActorLifecycleState::Passivating | ActorLifecycleState::Stopping => {}
            }
            let terminal = tokio::time::timeout(self.config.waiter_timeout, async {
                loop {
                    match *lifecycle.borrow() {
                        ActorLifecycleState::Stopped => return true,
                        ActorLifecycleState::StopFailed => return false,
                        _ => {}
                    }
                    if lifecycle.changed().await.is_err() {
                        return false;
                    }
                }
            })
            .await
            .unwrap_or(false);
            if terminal {
                result.stopped += 1;
            } else if let Some(failure) = handle.inspect_stop_failure() {
                result.retained_failures.push(RetainedActorFailure {
                    actor_id,
                    local_ref: handle.local_ref(),
                    failure,
                });
            } else {
                result.request_failures.push(actor_id);
            }
        }
        result
    }

    /// Waits until the selected active Actor cells are actually gone.
    /// StopFailed and Quarantined are deliberately nonterminal and keep this future pending.
    pub async fn wait_actor_ids_terminal<I>(&self, actor_ids: I)
    where
        I: IntoIterator<Item = ActorId>,
    {
        let mut lifecycles = actor_ids
            .into_iter()
            .filter_map(|actor_id| self.entry_handle(&actor_id))
            .map(|handle| handle.subscribe_lifecycle())
            .collect::<Vec<_>>();
        for lifecycle in &mut lifecycles {
            while *lifecycle.borrow() != ActorLifecycleState::Stopped {
                if lifecycle.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    pub async fn passivate_actor_ids<I>(&self, actor_ids: I, reason: PassivationReason) -> usize
    where
        I: IntoIterator<Item = ActorId>,
    {
        let mut passivated = 0;
        for actor_id in actor_ids {
            if let Some(handle) = self.entry_handle(&actor_id)
                && is_business_admitted(handle.lifecycle_state())
            {
                let mut lifecycle = handle.subscribe_lifecycle();
                if handle.stop(StopReason::Passivated(reason)).await.is_err() {
                    continue;
                }
                let stopped = tokio::time::timeout(self.config.waiter_timeout, async {
                    while *lifecycle.borrow() != ActorLifecycleState::Stopped {
                        if *lifecycle.borrow() == ActorLifecycleState::StopFailed
                            || lifecycle.changed().await.is_err()
                        {
                            return false;
                        }
                    }
                    true
                })
                .await
                .unwrap_or(false);
                if stopped {
                    passivated += 1;
                }
            }
        }
        passivated
    }

    pub async fn start(
        &self,
        actor_id: ActorId,
        actor: A,
    ) -> Result<ActorHandle<A>, ActorActivationError> {
        self.remove_stopped_running_entry(&actor_id);
        match self.entries.entry(actor_id.clone()) {
            Entry::Occupied(_) => Err(ActorActivationError::AlreadyExists),
            Entry::Vacant(entry) => {
                let handle = self
                    .spawn_actor(actor_id.clone(), actor)
                    .map_err(ActorActivationError::ActivationFailed)?;
                entry.insert(RegistryEntry::Running(handle.clone()));
                self.register_exact(&handle);
                if handle.terminal_cleanup_started() || is_terminal(handle.lifecycle_state()) {
                    self.remove_stopped_running_entry(&actor_id);
                }
                Ok(handle)
            }
        }
    }

    pub async fn get_or_activate<F, Fut>(
        &self,
        actor_id: ActorId,
        activate: F,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<A, A::Error>>,
    {
        self.remove_stopped_running_entry(&actor_id);
        let lookup = match self.entries.entry(actor_id.clone()) {
            Entry::Occupied(entry) => match entry.get() {
                RegistryEntry::Running(handle)
                    if handle.lifecycle_state() == ActorLifecycleState::StopFailed =>
                {
                    return Err(ActorActivationError::RetainedStopFailure);
                }
                RegistryEntry::Running(handle)
                    if handle.lifecycle_state() == ActorLifecycleState::Quarantined =>
                {
                    return Err(ActorActivationError::Quarantined);
                }
                RegistryEntry::Running(handle) => return Ok(handle.clone()),
                RegistryEntry::Activating(activation) => RegistryLookup::Wait(activation.clone()),
            },
            Entry::Vacant(entry) => {
                let activation = ActivationState::new(self.config.waiter_capacity);
                entry.insert(RegistryEntry::Activating(activation.clone()));
                RegistryLookup::Activate(activation)
            }
        };

        let activation = match lookup {
            RegistryLookup::Wait(activation) => {
                return self.wait_for_activation(activation).await;
            }
            RegistryLookup::Activate(activation) => activation,
        };

        activation.set_loading();

        let result = match activate().await {
            Ok(actor) => {
                let spawned = match self.entries.entry(actor_id.clone()) {
                    Entry::Occupied(mut entry) if matches!(entry.get(), RegistryEntry::Activating(existing) if Arc::ptr_eq(existing, &activation)) => {
                        match self.spawn_actor(actor_id.clone(), actor) {
                            Ok(handle) => {
                                entry.insert(RegistryEntry::Running(handle.clone()));
                                Ok(handle)
                            }
                            Err(error) => {
                                entry.remove();
                                Err(ActorActivationError::ActivationFailed(error))
                            }
                        }
                    }
                    _ => Err(ActorActivationError::ActivationFailed(ActorError::new(
                        "actor registry entry removed during activation",
                    ))),
                };
                match spawned {
                    Ok(handle) => {
                        self.register_exact(&handle);
                        if handle.terminal_cleanup_started()
                            || is_terminal(handle.lifecycle_state())
                        {
                            self.remove_stopped_running_entry(&actor_id);
                        }
                        Ok(handle)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => {
                self.entries.remove_if(&actor_id, |_, entry| {
                    matches!(entry, RegistryEntry::Activating(existing) if Arc::ptr_eq(existing, &activation))
                });
                Err(ActorActivationError::ActivationFailed(
                    ActorError::from_error(error),
                ))
            }
        };

        activation.publish(result.clone());
        result
    }

    pub async fn get_or_create<F>(
        &self,
        actor_id: ActorId,
        factory: F,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        F: ActorFactory<A>,
    {
        let ctx = self.create_context(&actor_id)?;
        self.get_or_activate(actor_id, || async move { factory.create(ctx).await })
            .await
    }

    pub async fn get_or_load<L>(
        &self,
        actor_id: ActorId,
        loader: L,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        L: ActorLoader<A>,
    {
        let ctx = self.create_context(&actor_id)?;
        self.get_or_activate(actor_id, || async move { loader.load(ctx).await })
            .await
    }

    /// Validates the exact authority generation carried by a framework routing target.
    #[doc(hidden)]
    pub fn validate_actor_authority(
        &self,
        actor_id: ActorId,
        expected_generation: u64,
    ) -> Result<ValidatedActorAuthority, ActorActivationError> {
        let fencing_token = self
            .resolve_fencing_token(&actor_id)?
            .ok_or_else(|| authority_error("actor registry has no external authority resolver"))?;
        if fencing_token.get() != expected_generation {
            return Err(authority_error(
                "actor routing generation does not match current placement authority",
            ));
        }
        Ok(ValidatedActorAuthority {
            actor_id,
            fencing_token,
            registry_identity: self.registry_identity(),
        })
    }

    /// Loads an Actor using an authority proof issued by this Registry.
    #[doc(hidden)]
    pub async fn load_with_validated_authority<L>(
        &self,
        authority: ValidatedActorAuthority,
        loader: L,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        L: ActorLoader<A>,
    {
        if authority.registry_identity != self.registry_identity() {
            return Err(authority_error(
                "actor authority proof belongs to a different registry",
            ));
        }
        let actor_id = authority.actor_id;
        let ctx = self.create_context_with_token(&actor_id, authority.fencing_token);
        self.get_or_activate(actor_id, || async move { loader.load(ctx).await })
            .await
    }

    fn create_context(
        &self,
        actor_id: &ActorId,
    ) -> Result<ActorCreateContext, ActorActivationError> {
        let fencing_token = self.resolve_fencing_token(actor_id)?;
        Ok(self.create_context_optional(actor_id, fencing_token))
    }

    fn create_context_with_token(
        &self,
        actor_id: &ActorId,
        fencing_token: ActorFencingToken,
    ) -> ActorCreateContext {
        self.create_context_optional(actor_id, Some(fencing_token))
    }

    fn create_context_optional(
        &self,
        actor_id: &ActorId,
        fencing_token: Option<ActorFencingToken>,
    ) -> ActorCreateContext {
        ActorCreateContext {
            actor_kind: self.kind.clone(),
            actor_id: actor_id.clone(),
            service: self.config.service.clone(),
            fencing_token,
        }
    }

    fn resolve_fencing_token(
        &self,
        actor_id: &ActorId,
    ) -> Result<Option<ActorFencingToken>, ActorActivationError> {
        let resolvers = self
            .fencing_token_resolvers
            .read()
            .expect("actor fencing token resolver poisoned")
            .clone();
        if resolvers.is_empty() {
            return Ok(None);
        }
        let mut generation = None;
        for resolver in resolvers.into_values() {
            let Some(candidate) = resolver(actor_id) else {
                continue;
            };
            if generation.is_some_and(|current| current != candidate) {
                return Err(authority_error(
                    "actor activation matched conflicting placement authorities",
                ));
            }
            generation = Some(candidate);
        }
        let generation = generation
            .ok_or_else(|| authority_error("actor activation has no live placement authority"))?;
        ActorFencingToken::new(generation)
            .map(Some)
            .ok_or_else(|| authority_error("actor placement authority generation is zero"))
    }

    fn registry_identity(&self) -> usize {
        Arc::as_ptr(&self.entries) as usize
    }

    async fn wait_for_activation(
        &self,
        activation: Arc<ActivationState<A>>,
    ) -> Result<ActorHandle<A>, ActorActivationError> {
        let permit = activation
            .waiter_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ActorActivationError::WaiterCapacityExceeded)?;
        let mut result_rx = activation.result_tx.subscribe();

        if let Some(result) = result_rx.borrow().clone() {
            drop(permit);
            return result;
        }

        let wait = async {
            loop {
                result_rx.changed().await.map_err(|_| {
                    ActorActivationError::ActivationFailed(ActorError::new(
                        "activation result channel closed",
                    ))
                })?;
                if let Some(result) = result_rx.borrow().clone() {
                    return result;
                }
            }
        };

        let result = tokio::time::timeout(self.config.waiter_timeout, wait)
            .await
            .map_err(|_| ActorActivationError::WaiterTimeout {
                timeout: self.config.waiter_timeout,
            })?;
        drop(permit);
        result
    }

    fn remove_stopped_running_entry(&self, actor_id: &ActorId) {
        let removed = self.entries.remove_if(actor_id, |_, entry| {
            matches!(
                entry,
                RegistryEntry::Running(handle)
                    if is_terminal(handle.lifecycle_state())
            )
        });
        if let Some((_, RegistryEntry::Running(handle))) = removed {
            self.remove_exact(&handle);
            if let Some(directory) = self.config.service.extension::<ActivationDirectory>()
                && let Some(reference) = handle.actor_ref()
            {
                directory.remove(&reference.erase());
            }
        }
    }

    fn register_exact(&self, handle: &ActorHandle<A>) {
        if let Some(reference) = handle.actor_ref() {
            self.exact_entries.insert(
                reference.activation_id(),
                ExactRegistryEntry {
                    handle: handle.clone(),
                    local_ref: handle.local_ref(),
                },
            );
        }
    }

    fn remove_exact(&self, handle: &ActorHandle<A>) {
        if let Some(reference) = handle.actor_ref() {
            self.exact_entries
                .remove_if(&reference.activation_id(), |_, entry| {
                    entry.local_ref == handle.local_ref()
                });
        }
    }

    fn entry_handle(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        match self.entries.get(actor_id).as_deref() {
            Some(RegistryEntry::Running(handle)) => Some(handle.clone()),
            Some(RegistryEntry::Activating(_)) | None => None,
        }
    }

    fn local_handle(&self, local_ref: LocalActorRef) -> Option<ActorHandle<A>> {
        self.entries
            .iter()
            .find_map(|entry| match entry.value() {
                RegistryEntry::Running(handle) if handle.local_ref() == local_ref => {
                    Some(handle.clone())
                }
                RegistryEntry::Running(_) | RegistryEntry::Activating(_) => None,
            })
            .or_else(|| {
                self.quarantined
                    .get(&local_ref)
                    .map(|entry| entry.handle.clone())
            })
    }

    fn spawn_actor(&self, actor_id: ActorId, actor: A) -> Result<ActorHandle<A>, ActorError> {
        let self_ref = self
            .actor_ref_for(actor_id.clone())
            .map(|actor_ref| actor_ref.erase());
        let entries = self.entries.clone();
        let exact_entries = self.exact_entries.clone();
        let quarantined = self.quarantined.clone();
        let terminal_actor_id = actor_id.clone();
        let directory = self.config.service.extension::<ActivationDirectory>();
        let terminal_reference = self_ref.clone();
        let terminal_activation = self_ref.as_ref().map(ActorRef::activation_id);
        let terminal_hook = Box::new(move |local_ref| {
            entries.remove_if(&terminal_actor_id, |_, entry| {
                matches!(entry, RegistryEntry::Running(handle) if handle.local_ref() == local_ref)
            });
            quarantined.remove_if(&local_ref, |_, entry| {
                entry.actor_id == terminal_actor_id && entry.handle.local_ref() == local_ref
            });
            if let Some(activation_id) = terminal_activation {
                exact_entries.remove_if(&activation_id, |_, entry| entry.local_ref == local_ref);
            }
            if let (Some(directory), Some(reference)) = (&directory, &terminal_reference) {
                directory.remove(reference);
            }
        });
        let handle = spawn_actor_with_self_ref(
            actor,
            ActorSpawnContext {
                options: ActorSpawnOptions {
                    mailbox: self.config.mailbox,
                    execution: None,
                    scheduler_key: None,
                    passivation: self.config.passivation,
                    self_ref,
                    service: self.config.service.clone(),
                },
                actor_system: Some(self.actor_system.clone()),
                observer: self.observer.clone(),
                terminal_hook: Some(terminal_hook),
                spawner: self.spawner.clone(),
            },
        )
        .map_err(|error| ActorError::new(error.to_string()))?;
        if let Some(directory) = self.config.service.extension::<ActivationDirectory>()
            && let Err(error) = directory.register(&handle)
        {
            let _ = handle.try_stop_internal(StopReason::StartFailed);
            return Err(ActorError::new(error.to_string()));
        }
        if is_terminal(handle.lifecycle_state())
            && let Some(directory) = self.config.service.extension::<ActivationDirectory>()
            && let Some(reference) = handle.actor_ref()
        {
            directory.remove(&reference.erase());
        }
        Ok(handle)
    }

    fn actor_ref_for(&self, actor_id: ActorId) -> Option<ActorRef> {
        let config = self.config.actor_ref.as_ref()?;
        let protocol_id = self.protocol_id?;
        let path = ActorPath::user([
            "user".to_owned(),
            encode_segment(self.kind.as_str().as_bytes()),
            encode_actor_id(&actor_id),
        ])
        .inspect_err(|error| {
            tracing::warn!(
                actor_kind = self.kind.as_str(),
                %error,
                "actor identity does not fit an addressable actor path; the activation stays node-local"
            );
        })
        .ok()?;
        ActorRef::new(
            config.cluster_id.clone(),
            config.node_address.clone(),
            config.node_incarnation,
            path,
            crate::runtime::next_activation_id(config.node_incarnation),
            protocol_id,
        )
        .ok()
    }
}

fn encode_actor_id(actor_id: &ActorId) -> String {
    match actor_id {
        ActorId::Str(value) => format!("s-{}", encode_segment(value.as_bytes())),
        ActorId::U64(value) => format!("u-{value}"),
        ActorId::I64(value) => format!("i-{value}"),
        ActorId::Bytes(value) => format!("b-{}", encode_segment(value)),
    }
}

fn encode_segment(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

fn is_terminal(state: ActorLifecycleState) -> bool {
    state == ActorLifecycleState::Stopped
}

fn authority_error(message: &'static str) -> ActorActivationError {
    ActorActivationError::ActivationFailed(ActorError::new(message))
}

fn is_business_admitted(state: ActorLifecycleState) -> bool {
    matches!(
        state,
        ActorLifecycleState::Starting | ActorLifecycleState::Running
    )
}

enum RegistryEntry<A: Actor> {
    Running(ActorHandle<A>),
    Activating(Arc<ActivationState<A>>),
}

enum RegistryLookup<A: Actor> {
    Activate(Arc<ActivationState<A>>),
    Wait(Arc<ActivationState<A>>),
}

struct ActivationState<A: Actor> {
    result_tx: watch::Sender<Option<Result<ActorHandle<A>, ActorActivationError>>>,
    waiter_slots: Arc<Semaphore>,
    state: AtomicU8,
}

impl<A: Actor> ActivationState<A> {
    fn new(waiter_capacity: usize) -> Arc<Self> {
        let (result_tx, _result_rx) = watch::channel(None);
        Arc::new(Self {
            result_tx,
            waiter_slots: Arc::new(Semaphore::new(waiter_capacity)),
            state: AtomicU8::new(0),
        })
    }

    fn publish(&self, result: Result<ActorHandle<A>, ActorActivationError>) {
        self.result_tx.send_replace(Some(result));
    }

    fn set_loading(&self) {
        self.state.store(1, Ordering::Release);
    }

    fn state(&self) -> EntityActivationState {
        match self.state.load(Ordering::Acquire) {
            0 => EntityActivationState::Activating,
            _ => EntityActivationState::Loading,
        }
    }
}
