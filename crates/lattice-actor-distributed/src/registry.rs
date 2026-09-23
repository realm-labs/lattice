use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    marker::PhantomData,
    num::NonZeroU64,
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use dashmap::{DashMap, mapref::entry::Entry};
use lattice_model::{
    ModelError,
    actor::{ActivationId, ActorAddress, ActorPath, ProtocolId, ProtocolTag},
    cluster::{ClusterId, NodeEndpoint, NodeIncarnation},
};
use thiserror::Error;

use lattice_actor::{
    attachments::ActorRuntimeAttachments,
    environment::ActorEnvironment,
    error::{ActorAdminError, ActorFailure},
    handle::{ActorHandle, StopFailureRecord},
    mailbox::MailboxConfig,
    observation::ActorObserverHandle,
    runtime::{ActorSpawnOptions, PassivationPolicy, spawner::ActorSpawner},
    traits::{Actor, ActorLifecycleState, PassivationReason, StopReason},
    watch::LocalActorRef,
};

use crate::{
    activation::{DistributedActorIdentity, DistributedActorRuntime},
    directory::ActivationDirectory,
    entity::EntityActivationState,
    protocol::{ActorProtocolBinding, Protocol},
    recipient::ActorSystem,
};

mod activation;
pub mod definition;
mod quarantine;

pub use definition::ActorDefinition;
pub use lattice_model::actor::ActorId;

use activation::{ActivationCleanup, ActivationState};

#[derive(Debug, Clone, Error)]
pub enum ActorActivationError {
    #[error("actor is already running or activating")]
    AlreadyExists,
    #[error("activation waiter capacity exceeded")]
    WaiterCapacityExceeded,
    #[error("timed out waiting {timeout:?} for actor activation")]
    WaiterTimeout { timeout: Duration },
    #[error("actor activation failed: {0}")]
    ActivationFailed(ActorFailure),
    #[error("actor activation is retained after stopping persistence failed")]
    RetainedStopFailure,
    #[error("actor activation was cancelled before publication")]
    Cancelled,
}

static NEXT_ACTIVATION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ShardMigrationPolicy {
    #[default]
    BlockRunningActors,
    PassivateRunningActors,
}

#[derive(Debug, Clone)]
pub struct ActorRegistryConfig {
    pub mailbox: MailboxConfig,
    pub passivation: PassivationPolicy,
    pub shard_migration: ShardMigrationPolicy,
    pub waiter_capacity: usize,
    pub waiter_timeout: Duration,
    pub quarantine_capacity: usize,
    pub address: Option<ActorAddressConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorAddressConfig {
    pub cluster_id: ClusterId,
    pub node_address: NodeEndpoint,
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
            address: None,
        }
    }
}

/// Activation registry using an externally owned Actor runtime.
///
/// Retain the runtime that supplied the spawner for the lifetime of these Actors.
/// Multiple registries may share one spawner and its execution resources. The
/// registry never creates executors or shuts them down; drain it before shutting
/// down the owner. Loader and Actor environments come from that same spawner.
pub struct ActorRegistry<D: ActorDefinition, A: Actor> {
    definition: PhantomData<fn() -> D>,
    config: ActorRegistryConfig,
    protocol_id: Option<ProtocolId>,
    entries: Arc<DashMap<ActorId, RegistryEntry<A>>>,
    exact_entries: Arc<DashMap<ActivationId, ExactRegistryEntry<A>>>,
    quarantined: Arc<DashMap<LocalActorRef, QuarantinedEntry<A>>>,
    actor_system: Arc<OnceLock<ActorSystem>>,
    fencing_token_resolvers: Arc<RwLock<BTreeMap<String, ActorFencingTokenResolver>>>,
    spawner: ActorSpawner,
}

type ActorFencingTokenResolver =
    Arc<dyn Fn(&ActorId, &mut dyn FnMut(Option<u64>)) + Send + Sync + 'static>;

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
    reference: ActorAddress,
    handle: ActorHandle<A>,
    local_ref: LocalActorRef,
}

impl<D: ActorDefinition, A: Actor> fmt::Debug for ActorRegistry<D, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorRegistry")
            .field("definition", &D::NAME)
            .field("config", &self.config)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ActorCreateContext {
    pub actor_name: &'static str,
    pub actor_id: ActorId,
    pub environment: ActorEnvironment,
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
    pub actor_address: Option<ActorAddress>,
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

impl<D: ActorDefinition, A: Actor> ActorRegistry<D, A> {
    /// Creates a node-local registry using the supplied execution entry point.
    pub fn new(spawner: ActorSpawner, config: ActorRegistryConfig) -> Self {
        assert!(
            config.address.is_none(),
            "registries with exact ActorAddresses must be constructed with ActorRegistry::new_bound"
        );
        assert!(
            config.quarantine_capacity > 0,
            "quarantine capacity must be nonzero"
        );
        let actor_system = Arc::new(OnceLock::new());
        Self {
            definition: PhantomData,
            config,
            protocol_id: None,
            entries: Arc::new(DashMap::new()),
            exact_entries: Arc::new(DashMap::new()),
            quarantined: Arc::new(DashMap::new()),
            actor_system,
            fencing_token_resolvers: Arc::new(RwLock::new(BTreeMap::new())),
            spawner,
        }
    }

    /// Constructs a registry whose exact activation addresses are bound to
    /// the supplied server protocol. The address protocol ID is derived from
    /// the binding and cannot drift from the registered dispatcher.
    pub fn new_bound<P: Protocol>(
        spawner: ActorSpawner,
        config: ActorRegistryConfig,
        protocol: &ActorProtocolBinding<A, P>,
    ) -> Self
    where
        D: ActorDefinition<Protocol = P>,
    {
        assert!(
            config.quarantine_capacity > 0,
            "quarantine capacity must be nonzero"
        );
        let actor_system = Arc::new(OnceLock::new());
        Self {
            definition: PhantomData,
            config,
            protocol_id: Some(protocol.protocol_id()),
            entries: Arc::new(DashMap::new()),
            exact_entries: Arc::new(DashMap::new()),
            quarantined: Arc::new(DashMap::new()),
            actor_system,
            fencing_token_resolvers: Arc::new(RwLock::new(BTreeMap::new())),
            spawner,
        }
    }

    pub fn with_observer(mut self, observer: ActorObserverHandle) -> Self {
        self.spawner = self.spawner.with_observer(observer);
        self
    }

    #[doc(hidden)]
    pub fn install_actor_system(&self, actor_system: ActorSystem) -> Result<(), ActorSystem> {
        self.actor_system.set(actor_system)
    }

    pub fn name(&self) -> &'static str {
        D::NAME
    }

    pub(crate) fn address_namespace(&self) -> String {
        encode_segment(D::NAME.as_bytes())
    }

    /// Installs an authority resolver used by direct registry activations.
    ///
    /// A Registry may host several placement routes, so differently named resolvers are composed.
    /// Reinstalling the same name replaces its previous authority view. The resolver must invoke
    /// the callback exactly once while holding the lock that serializes authority changes. Pass
    /// `None` when the resolver has no live authority for the Actor ID. The callback may publish
    /// a Registry entry; it never awaits a loader or invokes Actor lifecycle/business callbacks.
    #[doc(hidden)]
    pub fn install_fencing_token_resolver<F>(&self, resolver_name: impl Into<String>, resolver: F)
    where
        F: Fn(&ActorId, &mut dyn FnMut(Option<u64>)) + Send + Sync + 'static,
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
                RegistryEntry::Running(handle, _)
                    if is_business_admitted(handle.lifecycle_state()) =>
                {
                    Some(entry.key().clone())
                }
                RegistryEntry::Running(_, _) => None,
                RegistryEntry::Activating(_) => None,
            })
            .collect()
    }

    /// Returns every nonterminal activation still owned by the active registry.
    ///
    /// This includes loading placeholders and Starting, Passivating, Stopping, and StopFailed
    /// cells so drain and authority fencing cannot miss in-flight work.
    pub fn active_actor_ids(&self) -> Vec<ActorId> {
        self.entries
            .iter()
            .filter_map(|entry| match entry.value() {
                RegistryEntry::Running(handle, _) if !is_terminal(handle.lifecycle_state()) => {
                    Some(entry.key().clone())
                }
                RegistryEntry::Activating(_) => Some(entry.key().clone()),
                RegistryEntry::Running(_, _) => None,
            })
            .collect()
    }

    pub fn activation_state(&self, actor_id: &ActorId) -> EntityActivationState {
        match self.entries.get(actor_id).as_deref() {
            Some(RegistryEntry::Running(_, _)) => EntityActivationState::Active,
            Some(RegistryEntry::Activating(activation)) => activation.state(),
            None => EntityActivationState::Absent,
        }
    }

    pub fn get_running(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        let token = self.resolve_fencing_token(actor_id).ok()?;
        self.with_actor_authority(actor_id, token, || {
            Ok(match self.entries.get(actor_id).as_deref() {
                Some(RegistryEntry::Running(handle, current))
                    if *current == token && is_business_admitted(handle.lifecycle_state()) =>
                {
                    Some(handle.clone())
                }
                Some(RegistryEntry::Running(_, _)) => None,
                Some(RegistryEntry::Activating(_)) | None => None,
            })
        })
        .ok()
        .flatten()
    }

    pub fn exact_address(&self, actor_id: &ActorId) -> Option<ActorAddress> {
        let handle = self.get_running(actor_id)?;
        self.exact_reference(&handle)
    }

    pub fn address<P: ProtocolTag>(
        &self,
        actor_id: &ActorId,
    ) -> Result<Option<ActorAddress<P>>, ModelError> {
        self.exact_address(actor_id)
            .map(|address| address.try_typed::<P>())
            .transpose()
    }

    pub fn get_exact(&self, address: &ActorAddress) -> Option<ActorHandle<A>> {
        if address.actor_path().segments().nth(1) != Some(self.address_namespace().as_str()) {
            return None;
        }
        if self.config.address.as_ref().is_none_or(|config| {
            config.cluster_id != *address.cluster_id()
                || config.node_address != *address.node_address()
                || config.node_incarnation != address.node_incarnation()
                || self.protocol_id != Some(address.protocol_id())
        }) {
            return None;
        }
        if let Some(directory) = self.spawner.environment().get::<ActivationDirectory>()
            && let Some(handle) = directory.resolve(address)
        {
            return Some(handle);
        }
        let exact = self.exact_entries.get(&address.activation_id())?;
        if is_business_admitted(exact.handle.lifecycle_state())
            && exact.reference.same_activation(address)
        {
            Some(exact.handle.clone())
        } else {
            None
        }
    }

    pub async fn remove(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        let handle = self.cancel_loading_or_handle(actor_id)??;
        if is_business_admitted(handle.lifecycle_state()) {
            let _ = handle.stop(StopReason::Requested);
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
            .collect::<Vec<_>>();
        for handle in quarantined {
            if handle.lifecycle_state() == ActorLifecycleState::StopFailed {
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
            let Some(entry) = self.cancel_loading_or_handle(&actor_id) else {
                continue;
            };
            let Some(handle) = entry else {
                result.requested += 1;
                result.stopped += 1;
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
                ActorLifecycleState::Stopped => continue,
                ActorLifecycleState::Starting | ActorLifecycleState::Running => {
                    result.requested += 1;
                    if handle
                        .stop(StopReason::Passivated(PassivationReason::Drain))
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
    /// StopFailed is deliberately nonterminal and keeps this future pending.
    pub async fn wait_actor_ids_terminal<I>(&self, actor_ids: I)
    where
        I: IntoIterator<Item = ActorId>,
    {
        let entries = actor_ids
            .into_iter()
            .filter_map(|actor_id| {
                self.entries
                    .get(&actor_id)
                    .map(|entry| match entry.value() {
                        RegistryEntry::Running(handle, _) => Ok(handle.clone()),
                        RegistryEntry::Activating(activation) => Err(activation.clone()),
                    })
            })
            .collect::<Vec<_>>();
        for entry in entries {
            let handle = match entry {
                Ok(handle) => handle,
                Err(activation) => match activation.result().await {
                    Ok(handle) => handle,
                    Err(_) => continue,
                },
            };
            let mut lifecycle = handle.subscribe_lifecycle();
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
            let entry = self.cancel_loading_or_handle(&actor_id);
            if matches!(entry, Some(None)) {
                passivated += 1;
            }
            if let Some(Some(handle)) = entry
                && is_business_admitted(handle.lifecycle_state())
            {
                let mut lifecycle = handle.subscribe_lifecycle();
                if handle.stop(StopReason::Passivated(reason)).is_err() {
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
        let fencing_token = self.resolve_fencing_token(&actor_id)?;
        let handle = self.with_actor_authority(&actor_id, fencing_token, || {
            match self.entries.entry(actor_id.clone()) {
                Entry::Occupied(_) => Err(ActorActivationError::AlreadyExists),
                Entry::Vacant(entry) => {
                    let handle = self
                        .spawn_actor(actor_id.clone(), actor)
                        .map_err(ActorActivationError::ActivationFailed)?;
                    entry.insert(RegistryEntry::Running(handle.clone(), fencing_token));
                    Ok(handle)
                }
            }
        })?;
        self.remove_stopped_running_entry(&actor_id);
        Ok(handle)
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
        let fencing_token = self.resolve_fencing_token(&actor_id)?;
        self.get_or_activate_with_token(actor_id, fencing_token, activate)
            .await
    }

    async fn get_or_activate_with_token<F, Fut>(
        &self,
        actor_id: ActorId,
        fencing_token: Option<ActorFencingToken>,
        activate: F,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<A, A::Error>>,
    {
        self.remove_stopped_running_entry(&actor_id);
        let lookup = self.with_actor_authority(&actor_id, fencing_token, || {
            self.lookup_activation(&actor_id, fencing_token)
        })?;

        let activation = match lookup {
            RegistryLookup::Running(handle) => return Ok(handle),
            RegistryLookup::Wait(activation) => {
                let handle = self.wait_for_activation(activation).await?;
                return self.with_actor_authority(&actor_id, fencing_token, || {
                    if self.entries.get(&actor_id).is_some_and(|entry| {
                        matches!(entry.value(), RegistryEntry::Running(current, token)
                            if *token == fencing_token && current.local_ref() == handle.local_ref()
                                && is_business_admitted(current.lifecycle_state())
                                && !current.business_admission_fenced())
                    }) {
                        Ok(handle)
                    } else {
                        Err(ActorActivationError::Cancelled)
                    }
                });
            }
            RegistryLookup::Activate(activation) => activation,
        };

        let _cleanup = ActivationCleanup {
            entries: self.entries.clone(),
            actor_id: actor_id.clone(),
            activation: activation.clone(),
        };
        activation.set_loading();

        let loaded = tokio::select! {
            biased;
            result = activation.result() => return result,
            loaded = async { activate().await } => loaded,
        };
        let result = match loaded {
            Ok(actor) => {
                let spawned = self.with_actor_authority(&actor_id, fencing_token, || match self.entries.entry(actor_id.clone()) {
                    Entry::Occupied(mut entry) if matches!(entry.get(), RegistryEntry::Activating(existing) if Arc::ptr_eq(existing, &activation)) => {
                        match self.spawn_actor(actor_id.clone(), actor) {
                            Ok(handle) => {
                                entry.insert(RegistryEntry::Running(handle.clone(), fencing_token));
                                Ok(handle)
                            }
                            Err(error) => {
                                entry.remove();
                                Err(ActorActivationError::ActivationFailed(error))
                            }
                        }
                    }
                    _ => Err(ActorActivationError::ActivationFailed(ActorFailure::new(
                        "actor registry entry removed during activation",
                    ))),
                });
                match spawned {
                    Ok(handle) => {
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
                    ActorFailure::from_error(error),
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
        self.get_or_activate_with_token(actor_id, ctx.fencing_token, || async move {
            factory.create(ctx).await
        })
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
        self.get_or_activate_with_token(actor_id, ctx.fencing_token, || async move {
            loader.load(ctx).await
        })
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
        self.get_or_activate_with_token(actor_id, ctx.fencing_token, || async move {
            loader.load(ctx).await
        })
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
            actor_name: D::NAME,
            actor_id: actor_id.clone(),
            environment: self.spawner.environment().clone(),
            fencing_token,
        }
    }

    fn resolve_fencing_token(
        &self,
        actor_id: &ActorId,
    ) -> Result<Option<ActorFencingToken>, ActorActivationError> {
        self.resolve_authority(actor_id).map(|(token, _)| token)
    }

    fn resolve_authority(
        &self,
        actor_id: &ActorId,
    ) -> Result<(Option<ActorFencingToken>, Option<ActorFencingTokenResolver>), ActorActivationError>
    {
        let resolvers = self
            .fencing_token_resolvers
            .read()
            .expect("actor fencing token resolver poisoned")
            .clone();
        if resolvers.is_empty() {
            return Ok((None, None));
        }
        let mut generation = None;
        let mut selected = None;
        for resolver in resolvers.into_values() {
            let mut candidate = None;
            resolver(actor_id, &mut |value| candidate = value);
            let Some(candidate) = candidate else {
                continue;
            };
            if generation.is_some_and(|current| current != candidate) {
                return Err(authority_error(
                    "actor activation matched conflicting placement authorities",
                ));
            }
            generation = Some(candidate);
            selected = Some(resolver);
        }
        let generation = generation
            .ok_or_else(|| authority_error("actor activation has no live placement authority"))?;
        ActorFencingToken::new(generation)
            .map(|token| (Some(token), selected))
            .ok_or_else(|| authority_error("actor placement authority generation is zero"))
    }

    fn with_actor_authority<R>(
        &self,
        actor_id: &ActorId,
        expected: Option<ActorFencingToken>,
        operation: impl FnOnce() -> Result<R, ActorActivationError>,
    ) -> Result<R, ActorActivationError> {
        let (token, resolver) = self.resolve_authority(actor_id)?;
        if token != expected {
            return Err(authority_error(
                "actor placement authority changed during activation",
            ));
        }
        let Some(resolver) = resolver else {
            return operation();
        };
        let mut operation = Some(operation);
        let mut result = None;
        resolver(actor_id, &mut |generation| {
            result = Some(if generation != expected.map(ActorFencingToken::get) {
                Err(authority_error(
                    "actor placement authority changed during activation",
                ))
            } else {
                operation
                    .take()
                    .expect("authority resolver invoked callback more than once")()
            });
        });
        result.unwrap_or_else(|| {
            Err(authority_error(
                "actor authority resolver did not validate publication",
            ))
        })
    }

    fn registry_identity(&self) -> usize {
        Arc::as_ptr(&self.entries) as usize
    }

    fn remove_stopped_running_entry(&self, actor_id: &ActorId) {
        let removed = self.entries.remove_if(actor_id, |_, entry| {
            matches!(
                entry,
                RegistryEntry::Running(handle, _)
                    if is_terminal(handle.lifecycle_state())
            )
        });
        if let Some((_, RegistryEntry::Running(handle, _))) = removed
            && let Some(reference) = self.remove_exact(&handle)
            && let Some(directory) = self.spawner.environment().get::<ActivationDirectory>()
        {
            directory.remove(&reference);
        }
    }

    fn remove_exact(&self, handle: &ActorHandle<A>) -> Option<ActorAddress> {
        let activation_id = self
            .exact_entries
            .iter()
            .find_map(|entry| (entry.local_ref == handle.local_ref()).then_some(*entry.key()))?;
        self.exact_entries
            .remove_if(&activation_id, |_, entry| {
                entry.local_ref == handle.local_ref()
            })
            .map(|(_, entry)| entry.reference)
    }

    fn exact_reference(&self, handle: &ActorHandle<A>) -> Option<ActorAddress> {
        self.exact_entries.iter().find_map(|entry| {
            (entry.local_ref == handle.local_ref()).then(|| entry.reference.clone())
        })
    }

    fn entry_handle(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        match self.entries.get(actor_id).as_deref() {
            Some(RegistryEntry::Running(handle, _)) => Some(handle.clone()),
            Some(RegistryEntry::Activating(_)) | None => None,
        }
    }

    fn local_handle(&self, local_ref: LocalActorRef) -> Option<ActorHandle<A>> {
        self.entries
            .iter()
            .find_map(|entry| match entry.value() {
                RegistryEntry::Running(handle, _) if handle.local_ref() == local_ref => {
                    Some(handle.clone())
                }
                RegistryEntry::Running(_, _) | RegistryEntry::Activating(_) => None,
            })
            .or_else(|| {
                self.quarantined
                    .get(&local_ref)
                    .map(|entry| entry.handle.clone())
            })
    }

    fn spawn_actor(&self, actor_id: ActorId, actor: A) -> Result<ActorHandle<A>, ActorFailure> {
        let self_address = self
            .actor_address_for(actor_id.clone())
            .map(|address| address.erase());
        let entries = self.entries.clone();
        let exact_entries = self.exact_entries.clone();
        let quarantined = self.quarantined.clone();
        let terminal_actor_id = actor_id.clone();
        let directory = self.spawner.environment().get::<ActivationDirectory>();
        let terminal_reference = self_address.clone();
        let terminal_activation = self_address.as_ref().map(ActorAddress::activation_id);
        let terminal_hook = Box::new(move |local_ref| {
            entries.remove_if(&terminal_actor_id, |_, entry| {
                matches!(entry, RegistryEntry::Running(handle, _) if handle.local_ref() == local_ref)
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
        let mut runtime_attachments = ActorRuntimeAttachments::builder();
        runtime_attachments
            .insert(DistributedActorRuntime::new(self.actor_system.clone()))
            .map_err(|error| ActorFailure::new(error.to_string()))?;
        runtime_attachments
            .insert(DistributedActorIdentity::new(self_address.clone()))
            .map_err(|error| ActorFailure::new(error.to_string()))?;
        let handle = self
            .spawner
            .spawn_managed_actor(
                actor,
                ActorSpawnOptions {
                    mailbox: self.config.mailbox,
                    execution: None,
                    scheduler_key: None,
                    passivation: self.config.passivation,
                },
                runtime_attachments.build(),
                Some(terminal_hook),
            )
            .map_err(|error| ActorFailure::new(error.to_string()))?;
        if let (Some(directory), Some(reference)) = (
            self.spawner.environment().get::<ActivationDirectory>(),
            self_address.as_ref(),
        ) && let Err(error) = directory.register(reference, &handle)
        {
            let _ = handle.try_stop_internal(StopReason::StartFailed);
            return Err(ActorFailure::new(error.to_string()));
        }
        if is_terminal(handle.lifecycle_state())
            && let Some(directory) = self.spawner.environment().get::<ActivationDirectory>()
            && let Some(reference) = self_address.as_ref()
        {
            directory.remove(reference);
        }
        if !is_terminal(handle.lifecycle_state())
            && let Some(reference) = self_address
        {
            self.exact_entries.insert(
                reference.activation_id(),
                ExactRegistryEntry {
                    reference,
                    handle: handle.clone(),
                    local_ref: handle.local_ref(),
                },
            );
        }
        Ok(handle)
    }

    fn actor_address_for(&self, actor_id: ActorId) -> Option<ActorAddress> {
        let config = self.config.address.as_ref()?;
        let protocol_id = self.protocol_id?;
        let path = ActorPath::user([
            "user".to_owned(),
            encode_segment(D::NAME.as_bytes()),
            encode_actor_id(&actor_id),
        ])
        .inspect_err(|error| {
            tracing::warn!(
                actor_kind = D::NAME,
                %error,
                "actor identity does not fit an addressable actor path; the activation stays node-local"
            );
        })
        .ok()?;
        ActorAddress::new(
            config.cluster_id.clone(),
            config.node_address.clone(),
            path,
            next_activation_id(config.node_incarnation),
            protocol_id,
        )
        .ok()
    }
}

fn encode_actor_id(actor_id: &ActorId) -> String {
    format!("b-{}", encode_segment(actor_id.as_bytes()))
}

fn next_activation_id(node_incarnation: NodeIncarnation) -> ActivationId {
    let sequence = NEXT_ACTIVATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    ActivationId::new(node_incarnation, sequence).expect("process activation sequence is nonzero")
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
    ActorActivationError::ActivationFailed(ActorFailure::new(message))
}

fn is_business_admitted(state: ActorLifecycleState) -> bool {
    matches!(
        state,
        ActorLifecycleState::Starting | ActorLifecycleState::Running
    )
}

enum RegistryEntry<A: Actor> {
    Running(ActorHandle<A>, Option<ActorFencingToken>),
    Activating(Arc<ActivationState<A>>),
}

enum RegistryLookup<A: Actor> {
    Running(ActorHandle<A>),
    Activate(Arc<ActivationState<A>>),
    Wait(Arc<ActivationState<A>>),
}
