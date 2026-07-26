//! Lease-aware process-wide distributed ID generation.

use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    snowflake::{SnowflakeConfig, SnowflakeError, SnowflakeState},
    worker::{
        WorkerId, WorkerIdAcquisition, WorkerIdLease, WorkerIdLeaseStore, WorkerIdOwner,
        WorkerIdRange, WorkerIdStoreError,
    },
};

const RUNTIME_EXIT_MESSAGE: &str = "distributed ID runtime stopped unexpectedly";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedIdConfig {
    pub snowflake: SnowflakeConfig,
    pub worker_range: WorkerIdRange,
    pub lease_ttl: Duration,
    pub renew_interval: Duration,
    pub lease_safety_margin: Duration,
    pub maximum_clock_skew: Duration,
    pub reacquire_backoff_initial: Duration,
    pub reacquire_backoff_max: Duration,
}

impl Default for DistributedIdConfig {
    fn default() -> Self {
        let snowflake = SnowflakeConfig::default();
        Self {
            snowflake,
            worker_range: WorkerIdRange::new(0, snowflake.max_worker_id())
                .expect("default Snowflake range is valid"),
            lease_ttl: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
            lease_safety_margin: Duration::from_secs(2),
            maximum_clock_skew: Duration::from_secs(5),
            reacquire_backoff_initial: Duration::from_millis(250),
            reacquire_backoff_max: Duration::from_secs(5),
        }
    }
}

impl DistributedIdConfig {
    pub fn validate(&self) -> Result<(), DistributedIdError> {
        if self.lease_ttl.is_zero()
            || self.renew_interval.is_zero()
            || self.lease_safety_margin.is_zero()
            || self.maximum_clock_skew.is_zero()
            || self.reacquire_backoff_initial.is_zero()
            || self.reacquire_backoff_max.is_zero()
        {
            return Err(DistributedIdError::InvalidConfiguration {
                message: "durations must be nonzero".to_string(),
            });
        }
        if self.worker_range.end_inclusive().get() > self.snowflake.max_worker_id() {
            return Err(DistributedIdError::InvalidConfiguration {
                message: "worker range exceeds the Snowflake worker bits".to_string(),
            });
        }
        let usable = self
            .lease_ttl
            .checked_sub(self.lease_safety_margin)
            .ok_or_else(|| DistributedIdError::InvalidConfiguration {
                message: "lease safety margin must be shorter than the TTL".to_string(),
            })?;
        if self.renew_interval >= usable {
            return Err(DistributedIdError::InvalidConfiguration {
                message: "renew interval must precede the safe lease deadline".to_string(),
            });
        }
        if self.reacquire_backoff_initial > self.reacquire_backoff_max {
            return Err(DistributedIdError::InvalidConfiguration {
                message: "reacquire backoff bounds are reversed".to_string(),
            });
        }
        self.reuse_cooldown()?;
        Ok(())
    }

    pub fn reuse_cooldown(&self) -> Result<Duration, DistributedIdError> {
        self.maximum_clock_skew
            .checked_mul(2)
            .and_then(|duration| duration.checked_add(Duration::from_millis(1)))
            .ok_or_else(|| DistributedIdError::InvalidConfiguration {
                message: "maximum clock skew overflows the reuse cooldown".to_string(),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistributedIdState {
    Active {
        worker_id: WorkerId,
        lease_generation: u64,
    },
    CoolingDown {
        worker_id: WorkerId,
        cooldown: Duration,
    },
    Reacquiring {
        previous_worker_id: Option<WorkerId>,
        attempt: u64,
        last_error: Option<String>,
    },
    Failed {
        message: String,
    },
    Stopped,
}

#[derive(Debug, thiserror::Error)]
pub enum DistributedIdError {
    #[error("distributed ID configuration is invalid: {message}")]
    InvalidConfiguration { message: String },
    #[error("the worker ID lease is temporarily unavailable")]
    LeaseUnavailable,
    #[error("the distributed ID service is stopped")]
    Stopped,
    #[error("the distributed ID runtime failed and cannot recover: {message}")]
    RuntimeFailed { message: String },
    #[error("system time is before the Unix epoch or cannot fit in milliseconds")]
    SystemClock,
    #[error(transparent)]
    Snowflake(#[from] SnowflakeError),
    #[error(transparent)]
    LeaseStore(#[from] WorkerIdStoreError),
    #[error("distributed ID runtime task failed")]
    RuntimeTask(#[source] tokio::task::JoinError),
}

#[derive(Clone)]
pub struct DistributedIdGenerator {
    gate: Arc<GenerationGate>,
}

impl fmt::Debug for DistributedIdGenerator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DistributedIdGenerator")
            .field("active", &self.gate.is_active())
            .finish_non_exhaustive()
    }
}

impl DistributedIdGenerator {
    pub fn try_next_id(&self) -> Result<u64, DistributedIdError> {
        let version = self.gate.begin_generation()?;
        let now_ms = unix_time_ms()?;
        let worker_id = self.gate.worker_id.load(Ordering::Acquire);
        let generated = self
            .gate
            .snowflake
            .next_at(self.gate.config, worker_id, now_ms);
        if !self.gate.generation_is_current(version) {
            return Err(self.gate.unavailable_error());
        }
        generated
            .map_err(|error| DistributedIdError::Snowflake(self.gate.observe(worker_id, error)))
    }

    /// Counts generation attempts rejected because the system clock moved
    /// backwards. A nonzero and growing value means wall-clock time regressed
    /// and every generation fails until it catches up.
    pub fn clock_regressions(&self) -> u64 {
        self.gate.clock_regressions.load(Ordering::Relaxed)
    }

    pub async fn next_id(&self) -> Result<u64, DistributedIdError> {
        loop {
            match self.try_next_id() {
                Err(DistributedIdError::Snowflake(SnowflakeError::SequenceExhausted(_))) => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                result => return result,
            }
        }
    }
}

pub struct DistributedIdService {
    store: Arc<dyn WorkerIdLeaseStore>,
    generator: DistributedIdGenerator,
    state_rx: watch::Receiver<DistributedIdState>,
    state_tx: watch::Sender<DistributedIdState>,
    shutdown_tx: watch::Sender<bool>,
    lease: Arc<AsyncMutex<Option<WorkerIdLease>>>,
    task: Option<JoinHandle<()>>,
}

impl fmt::Debug for DistributedIdService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DistributedIdService")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl DistributedIdService {
    pub async fn start(
        store: Arc<dyn WorkerIdLeaseStore>,
        owner: WorkerIdOwner,
        config: DistributedIdConfig,
    ) -> Result<Self, DistributedIdError> {
        config.validate()?;
        let requested_at = Instant::now();
        let acquisition = store
            .acquire(&owner, config.worker_range, config.lease_ttl)
            .await?;
        if let Err(error) = validate_acquisition(&acquisition, &owner, &config) {
            let _ = store.release(acquisition.lease()).await;
            return Err(error);
        }
        let Some(safe_until) = lease_safe_deadline(
            requested_at,
            acquisition.lease(),
            config.lease_safety_margin,
        ) else {
            let _ = store.release(acquisition.lease()).await;
            return Err(DistributedIdError::LeaseUnavailable);
        };

        let gate = Arc::new(GenerationGate::new(config.snowflake));
        let initial_state = if acquisition.is_reused() {
            DistributedIdState::CoolingDown {
                worker_id: acquisition.lease().id(),
                cooldown: config.reuse_cooldown()?,
            }
        } else {
            let generation = gate.activate(acquisition.lease().id(), safe_until)?;
            DistributedIdState::Active {
                worker_id: acquisition.lease().id(),
                lease_generation: generation,
            }
        };
        let (state_tx, state_rx) = watch::channel(initial_state);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let lease = Arc::new(AsyncMutex::new(Some(acquisition.lease().clone())));
        let runtime = Runtime {
            store: store.clone(),
            owner,
            config,
            gate: gate.clone(),
            state_tx: state_tx.clone(),
            lease: lease.clone(),
        };
        let runtime_guard = RuntimeGuard {
            gate: gate.clone(),
            state_tx: state_tx.clone(),
        };
        let task = tokio::spawn(async move {
            let _runtime_guard = runtime_guard;
            runtime.run(acquisition, requested_at, shutdown_rx).await;
        });
        Ok(Self {
            store,
            generator: DistributedIdGenerator { gate },
            state_rx,
            state_tx,
            shutdown_tx,
            lease,
            task: Some(task),
        })
    }

    pub fn generator(&self) -> DistributedIdGenerator {
        self.generator.clone()
    }

    pub fn state(&self) -> DistributedIdState {
        self.state_rx.borrow().clone()
    }

    pub fn subscribe_state(&self) -> watch::Receiver<DistributedIdState> {
        self.state_rx.clone()
    }

    pub async fn wait_until_active(&self) -> Result<WorkerId, DistributedIdError> {
        let mut state = self.subscribe_state();
        loop {
            match state.borrow_and_update().clone() {
                DistributedIdState::Active { worker_id, .. } => return Ok(worker_id),
                DistributedIdState::Stopped => return Err(DistributedIdError::Stopped),
                DistributedIdState::Failed { message } => {
                    return Err(DistributedIdError::RuntimeFailed { message });
                }
                DistributedIdState::CoolingDown { .. } | DistributedIdState::Reacquiring { .. } => {
                }
            }
            state
                .changed()
                .await
                .map_err(|_| DistributedIdError::Stopped)?;
        }
    }

    pub async fn shutdown(mut self) -> Result<bool, DistributedIdError> {
        self.generator.gate.stop();
        self.state_tx.send_replace(DistributedIdState::Stopped);
        self.shutdown_tx.send_replace(true);
        let task_result = match self.task.take() {
            Some(task) => task.await.map_err(DistributedIdError::RuntimeTask),
            None => Ok(()),
        };
        let lease = self.lease.lock().await.take();
        let release_result = match lease {
            Some(lease) => self.store.release(&lease).await.map_err(Into::into),
            None => Ok(false),
        };
        task_result?;
        release_result
    }
}

impl Drop for DistributedIdService {
    fn drop(&mut self) {
        self.generator.gate.stop();
        self.state_tx.send_replace(DistributedIdState::Stopped);
        self.shutdown_tx.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct GenerationGate {
    config: SnowflakeConfig,
    snowflake: SnowflakeState,
    worker_id: AtomicU64,
    lease_generation: AtomicU64,
    safe_until_ms: AtomicU64,
    clock_regressions: AtomicU64,
    base: Instant,
    stopped: AtomicBool,
    failed: AtomicBool,
    transition: Mutex<()>,
}

impl GenerationGate {
    fn new(config: SnowflakeConfig) -> Self {
        Self {
            config,
            snowflake: SnowflakeState::new(),
            worker_id: AtomicU64::new(0),
            lease_generation: AtomicU64::new(0),
            safe_until_ms: AtomicU64::new(0),
            clock_regressions: AtomicU64::new(0),
            base: Instant::now(),
            stopped: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            transition: Mutex::new(()),
        }
    }

    fn activate(
        &self,
        worker_id: WorkerId,
        safe_until: Instant,
    ) -> Result<u64, DistributedIdError> {
        let _transition = self.transition.lock().expect("ID generation gate poisoned");
        if self.stopped.load(Ordering::Acquire) {
            return Err(DistributedIdError::Stopped);
        }
        let current = self.lease_generation.load(Ordering::Acquire);
        if current & 1 == 1 {
            self.store_safe_until(safe_until);
            return Ok(current);
        }
        self.worker_id.store(worker_id.get(), Ordering::Release);
        self.store_safe_until(safe_until);
        let next =
            current
                .checked_add(1)
                .ok_or_else(|| DistributedIdError::InvalidConfiguration {
                    message: "lease generation exhausted".to_string(),
                })?;
        self.lease_generation.store(next, Ordering::Release);
        Ok(next)
    }

    fn extend(&self, safe_until: Instant) {
        let _transition = self.transition.lock().expect("ID generation gate poisoned");
        if self.lease_generation.load(Ordering::Acquire) & 1 == 1 {
            self.store_safe_until(safe_until);
        }
    }

    fn store_safe_until(&self, safe_until: Instant) {
        self.safe_until_ms
            .store(self.millis_since_base(safe_until), Ordering::Release);
    }

    fn millis_since_base(&self, instant: Instant) -> u64 {
        u64::try_from(instant.saturating_duration_since(self.base).as_millis()).unwrap_or(u64::MAX)
    }

    fn fail(&self) {
        let _transition = self.transition.lock().expect("ID generation gate poisoned");
        self.failed.store(true, Ordering::Release);
        let current = self.lease_generation.load(Ordering::Acquire);
        if current & 1 == 1 {
            self.lease_generation
                .store(current.wrapping_add(1), Ordering::Release);
        }
    }

    fn observe(&self, worker_id: u64, error: SnowflakeError) -> SnowflakeError {
        if let SnowflakeError::ClockMovedBackwards { last_ms, now_ms } = error {
            self.clock_regressions.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                worker_id,
                last_ms,
                now_ms,
                "system clock moved backwards; distributed ID generation fails until wall clock time catches up"
            );
        }
        error
    }

    fn deactivate(&self) {
        let _transition = self.transition.lock().expect("ID generation gate poisoned");
        let current = self.lease_generation.load(Ordering::Acquire);
        if current & 1 == 1 {
            self.lease_generation
                .store(current.wrapping_add(1), Ordering::Release);
        }
    }

    fn stop(&self) {
        let _transition = self.transition.lock().expect("ID generation gate poisoned");
        self.stopped.store(true, Ordering::Release);
        let current = self.lease_generation.load(Ordering::Acquire);
        if current & 1 == 1 {
            self.lease_generation
                .store(current.wrapping_add(1), Ordering::Release);
        }
    }

    fn begin_generation(&self) -> Result<u64, DistributedIdError> {
        let version = self.lease_generation.load(Ordering::Acquire);
        if version & 1 == 0 {
            return Err(self.unavailable_error());
        }
        if self.millis_since_base(Instant::now()) > self.safe_until_ms.load(Ordering::Acquire) {
            return Err(self.unavailable_error());
        }
        Ok(version)
    }

    fn generation_is_current(&self, version: u64) -> bool {
        let current = self.lease_generation.load(Ordering::Acquire);
        current == version && current & 1 == 1
    }

    fn is_active(&self) -> bool {
        self.lease_generation.load(Ordering::Acquire) & 1 == 1
    }

    fn unavailable_error(&self) -> DistributedIdError {
        if self.stopped.load(Ordering::Acquire) {
            DistributedIdError::Stopped
        } else if self.failed.load(Ordering::Acquire) {
            DistributedIdError::RuntimeFailed {
                message: RUNTIME_EXIT_MESSAGE.to_string(),
            }
        } else {
            DistributedIdError::LeaseUnavailable
        }
    }
}

struct Runtime {
    store: Arc<dyn WorkerIdLeaseStore>,
    owner: WorkerIdOwner,
    config: DistributedIdConfig,
    gate: Arc<GenerationGate>,
    state_tx: watch::Sender<DistributedIdState>,
    lease: Arc<AsyncMutex<Option<WorkerIdLease>>>,
}

struct RuntimeGuard {
    gate: Arc<GenerationGate>,
    state_tx: watch::Sender<DistributedIdState>,
}

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        if self.gate.stopped.load(Ordering::Acquire) {
            return;
        }
        self.gate.fail();
        tracing::error!("{RUNTIME_EXIT_MESSAGE}");
        self.state_tx.send_replace(DistributedIdState::Failed {
            message: RUNTIME_EXIT_MESSAGE.to_string(),
        });
    }
}

impl Runtime {
    async fn run(
        self,
        mut acquisition: WorkerIdAcquisition,
        mut requested_at: Instant,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            match self.hold(acquisition, requested_at, &mut shutdown).await {
                HoldResult::Shutdown => return,
                HoldResult::Lost(previous_worker_id) => {
                    self.gate.deactivate();
                    *self.lease.lock().await = None;
                    let Some((next, next_requested_at)) =
                        self.reacquire(previous_worker_id, &mut shutdown).await
                    else {
                        return;
                    };
                    acquisition = next;
                    requested_at = next_requested_at;
                }
            }
        }
    }

    async fn hold(
        &self,
        acquisition: WorkerIdAcquisition,
        requested_at: Instant,
        shutdown: &mut watch::Receiver<bool>,
    ) -> HoldResult {
        let reused = acquisition.is_reused();
        let mut current = acquisition.into_lease();
        let worker_id = current.id();
        *self.lease.lock().await = Some(current.clone());
        let mut safe_until =
            match lease_safe_deadline(requested_at, &current, self.config.lease_safety_margin) {
                Some(deadline) => deadline,
                None => return HoldResult::Lost(worker_id),
            };
        let mut next_renew = requested_at + self.config.renew_interval;
        let cooldown_deadline = reused
            .then(|| Instant::now() + self.config.reuse_cooldown().expect("validated cooldown"));
        let mut active = !reused;

        loop {
            let cooldown_wait = async {
                match cooldown_deadline.filter(|_| !active) {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return HoldResult::Shutdown;
                    }
                }
                () = tokio::time::sleep_until(next_renew) => {
                    match self.renew_until(&current, safe_until, shutdown).await {
                        RenewResult::Renewed(renewed, renewed_at) => {
                            current = renewed;
                            *self.lease.lock().await = Some(current.clone());
                            let Some(deadline) = lease_safe_deadline(
                                renewed_at,
                                &current,
                                self.config.lease_safety_margin,
                            ) else {
                                return HoldResult::Lost(worker_id);
                            };
                            safe_until = deadline;
                            self.gate.extend(safe_until);
                            next_renew = renewed_at + self.config.renew_interval;
                        }
                        RenewResult::Lost => return HoldResult::Lost(worker_id),
                        RenewResult::Shutdown => return HoldResult::Shutdown,
                    }
                }
                () = cooldown_wait => {
                    if Instant::now() >= safe_until {
                        return HoldResult::Lost(worker_id);
                    }
                    match self.gate.activate(worker_id, safe_until) {
                        Ok(generation) => {
                            active = true;
                            self.state_tx.send_replace(DistributedIdState::Active {
                                worker_id,
                                lease_generation: generation,
                            });
                        }
                        Err(_) => return HoldResult::Shutdown,
                    }
                }
            }
        }
    }

    async fn renew_until(
        &self,
        lease: &WorkerIdLease,
        safe_deadline: Instant,
        shutdown: &mut watch::Receiver<bool>,
    ) -> RenewResult {
        let mut backoff = self.config.reacquire_backoff_initial;
        loop {
            let requested_at = Instant::now();
            let renewal = tokio::time::timeout_at(
                safe_deadline,
                self.store.renew(lease, self.config.lease_ttl),
            );
            let result = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return RenewResult::Shutdown;
                    }
                    continue;
                }
                result = renewal => result,
            };
            match result {
                Ok(Ok(Some(renewed))) => {
                    if validate_renewal(lease, &renewed, &self.config) {
                        return RenewResult::Renewed(renewed, requested_at);
                    }
                    tracing::error!(
                        worker_id = %lease.id(),
                        "worker ID store returned an invalid renewal; closing the generation gate"
                    );
                    return RenewResult::Lost;
                }
                Ok(Ok(None)) | Err(_) => return RenewResult::Lost,
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, worker_id = %lease.id(), "worker ID renewal failed; retrying before the safe deadline");
                    let now = Instant::now();
                    if now >= safe_deadline {
                        return RenewResult::Lost;
                    }
                    let delay = backoff.min(safe_deadline.saturating_duration_since(now));
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return RenewResult::Shutdown;
                            }
                        }
                        () = tokio::time::sleep(delay) => {}
                    }
                    backoff = backoff
                        .saturating_mul(2)
                        .min(self.config.reacquire_backoff_max);
                }
            }
        }
    }

    async fn reacquire(
        &self,
        previous_worker_id: WorkerId,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Option<(WorkerIdAcquisition, Instant)> {
        let mut attempt = 0_u64;
        let mut backoff = self.config.reacquire_backoff_initial;
        loop {
            attempt = attempt.saturating_add(1);
            self.state_tx.send_replace(DistributedIdState::Reacquiring {
                previous_worker_id: Some(previous_worker_id),
                attempt,
                last_error: None,
            });
            let requested_at = Instant::now();
            let result = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return None;
                    }
                    continue;
                }
                result = self.store.acquire(
                    &self.owner,
                    self.config.worker_range,
                    self.config.lease_ttl,
                ) => result,
            };
            match result {
                Ok(acquisition) => {
                    if let Err(error) =
                        validate_acquisition(&acquisition, &self.owner, &self.config)
                    {
                        let _ = self.store.release(acquisition.lease()).await;
                        self.state_tx.send_replace(DistributedIdState::Reacquiring {
                            previous_worker_id: Some(previous_worker_id),
                            attempt,
                            last_error: Some(error.to_string()),
                        });
                    } else {
                        let Some(safe_until) = lease_safe_deadline(
                            requested_at,
                            acquisition.lease(),
                            self.config.lease_safety_margin,
                        ) else {
                            let _ = self.store.release(acquisition.lease()).await;
                            return None;
                        };
                        let state = if acquisition.is_reused() {
                            DistributedIdState::CoolingDown {
                                worker_id: acquisition.lease().id(),
                                cooldown: self.config.reuse_cooldown().expect("validated cooldown"),
                            }
                        } else {
                            match self.gate.activate(acquisition.lease().id(), safe_until) {
                                Ok(generation) => DistributedIdState::Active {
                                    worker_id: acquisition.lease().id(),
                                    lease_generation: generation,
                                },
                                Err(_) => {
                                    let _ = self.store.release(acquisition.lease()).await;
                                    return None;
                                }
                            }
                        };
                        self.state_tx.send_replace(state);
                        return Some((acquisition, requested_at));
                    }
                }
                Err(error) => {
                    self.state_tx.send_replace(DistributedIdState::Reacquiring {
                        previous_worker_id: Some(previous_worker_id),
                        attempt,
                        last_error: Some(error.to_string()),
                    });
                }
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return None;
                    }
                }
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff
                .saturating_mul(2)
                .min(self.config.reacquire_backoff_max);
        }
    }
}

enum HoldResult {
    Lost(WorkerId),
    Shutdown,
}

enum RenewResult {
    Renewed(WorkerIdLease, Instant),
    Lost,
    Shutdown,
}

fn validate_acquisition(
    acquisition: &WorkerIdAcquisition,
    owner: &WorkerIdOwner,
    config: &DistributedIdConfig,
) -> Result<(), DistributedIdError> {
    let lease = acquisition.lease();
    if lease.owner() != owner || !config.worker_range.contains(lease.id()) {
        return Err(DistributedIdError::InvalidConfiguration {
            message: "lease store returned an owner or worker ID outside the request".to_string(),
        });
    }
    if lease.valid_for() <= config.lease_safety_margin {
        return Err(DistributedIdError::InvalidConfiguration {
            message: "lease store returned no usable validity window".to_string(),
        });
    }
    Ok(())
}

fn validate_renewal(
    previous: &WorkerIdLease,
    renewed: &WorkerIdLease,
    config: &DistributedIdConfig,
) -> bool {
    renewed.id() == previous.id()
        && renewed.owner() == previous.owner()
        && renewed.token() == previous.token()
        && renewed.valid_for() > config.lease_safety_margin
}

fn lease_safe_deadline(
    requested_at: Instant,
    lease: &WorkerIdLease,
    margin: Duration,
) -> Option<Instant> {
    lease
        .valid_for()
        .checked_sub(margin)
        .map(|usable| requested_at + usable)
}

fn unix_time_ms() -> Result<i64, DistributedIdError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DistributedIdError::SystemClock)?;
    i64::try_from(duration.as_millis()).map_err(|_| DistributedIdError::SystemClock)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use lattice_core::actor_ref::{ClusterId, NodeIncarnation};
    use tokio::sync::Notify;

    use super::{
        DistributedIdConfig, DistributedIdError, DistributedIdService, DistributedIdState,
        unix_time_ms,
    };
    use crate::{
        snowflake::{SnowflakeConfig, SnowflakeError},
        worker::{
            InMemoryWorkerIdLeaseStore, WorkerIdAcquisition, WorkerIdLease, WorkerIdLeaseStore,
            WorkerIdOwner, WorkerIdRange, WorkerIdStoreError,
        },
    };

    fn owner(incarnation: u128) -> WorkerIdOwner {
        WorkerIdOwner::for_node(
            ClusterId::new("id-service-test").unwrap(),
            format!("node-{incarnation}"),
            NodeIncarnation::new(incarnation).unwrap(),
        )
        .unwrap()
    }

    fn config() -> DistributedIdConfig {
        let snowflake = SnowflakeConfig::new(1_000, 48, 2, 4).unwrap();
        DistributedIdConfig {
            snowflake,
            worker_range: WorkerIdRange::new(0, 3).unwrap(),
            lease_ttl: Duration::from_millis(500),
            renew_interval: Duration::from_millis(100),
            lease_safety_margin: Duration::from_millis(50),
            maximum_clock_skew: Duration::from_millis(10),
            reacquire_backoff_initial: Duration::from_millis(5),
            reacquire_backoff_max: Duration::from_millis(20),
        }
    }

    #[tokio::test]
    async fn service_generates_and_shutdown_fails_closed() {
        let store = Arc::new(InMemoryWorkerIdLeaseStore::default());
        let service = DistributedIdService::start(store, owner(1), config())
            .await
            .unwrap();
        assert!(matches!(service.state(), DistributedIdState::Active { .. }));
        let generator = service.generator();
        assert!(generator.try_next_id().is_ok());
        assert!(service.shutdown().await.unwrap());
        assert!(matches!(
            generator.try_next_id(),
            Err(DistributedIdError::Stopped)
        ));
    }

    #[tokio::test]
    async fn unexpected_runtime_exit_fails_closed_and_terminates_waiters() {
        let store = Arc::new(InMemoryWorkerIdLeaseStore::default());
        let mut service = DistributedIdService::start(store, owner(1), config())
            .await
            .unwrap();
        let generator = service.generator();
        let task = service.task.take().unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::task::yield_now().await;

        assert!(matches!(
            generator.try_next_id(),
            Err(DistributedIdError::RuntimeFailed { .. })
        ));
        assert!(matches!(service.state(), DistributedIdState::Failed { .. }));
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(250), service.wait_until_active())
                .await
                .expect("a failed runtime must not hang its waiters"),
            Err(DistributedIdError::RuntimeFailed { .. })
        ));
        assert!(service.shutdown().await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn lease_deadlines_are_anchored_at_the_request_instead_of_the_response() {
        let store = Arc::new(SlowAcquireStore::new(Duration::from_millis(400)));
        let started = tokio::time::Instant::now();
        let service = DistributedIdService::start(store, owner(1), config())
            .await
            .unwrap();
        let mut states = service.subscribe_state();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(
                    &*states.borrow_and_update(),
                    DistributedIdState::Reacquiring { .. }
                ) {
                    break;
                }
                states.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
        assert!(elapsed < Duration::from_millis(600), "{elapsed:?}");
    }

    #[tokio::test]
    async fn a_starved_runtime_still_closes_the_gate_at_the_lease_deadline() {
        let store = Arc::new(InMemoryWorkerIdLeaseStore::default());
        let service = DistributedIdService::start(store, owner(1), config())
            .await
            .unwrap();
        let generator = service.generator();
        assert!(generator.try_next_id().is_ok());

        std::thread::sleep(Duration::from_millis(700));

        assert!(matches!(
            generator.try_next_id(),
            Err(DistributedIdError::LeaseUnavailable)
        ));
        let _ = service.shutdown().await;
    }

    #[tokio::test]
    async fn backwards_clock_movement_is_counted_for_operators() {
        let store = Arc::new(InMemoryWorkerIdLeaseStore::default());
        let service = DistributedIdService::start(store, owner(1), config())
            .await
            .unwrap();
        let generator = service.generator();
        let gate = &generator.gate;
        gate.snowflake
            .next_at(
                gate.config,
                gate.worker_id.load(Ordering::Acquire),
                unix_time_ms().unwrap() + 60_000,
            )
            .unwrap();

        assert!(matches!(
            generator.try_next_id(),
            Err(DistributedIdError::Snowflake(
                SnowflakeError::ClockMovedBackwards { .. }
            ))
        ));
        assert_eq!(generator.clock_regressions(), 1);
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reused_worker_waits_for_the_clock_skew_cooldown() {
        let store = Arc::new(InMemoryWorkerIdLeaseStore::default());
        let mut single = config();
        single.worker_range = WorkerIdRange::new(0, 0).unwrap();
        let first = DistributedIdService::start(store.clone(), owner(1), single.clone())
            .await
            .unwrap();
        first.shutdown().await.unwrap();
        let second = DistributedIdService::start(store, owner(2), single)
            .await
            .unwrap();
        assert!(matches!(
            second.state(),
            DistributedIdState::CoolingDown { .. }
        ));
        assert!(matches!(
            second.generator().try_next_id(),
            Err(DistributedIdError::LeaseUnavailable)
        ));
        tokio::time::timeout(Duration::from_millis(100), second.wait_until_active())
            .await
            .unwrap()
            .unwrap();
        assert!(second.generator().try_next_id().is_ok());
        second.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn lease_loss_fails_closed_then_reacquires_for_the_same_generator() {
        let store = Arc::new(LoseOnceStore::default());
        let mut test_config = config();
        test_config.renew_interval = Duration::from_millis(20);
        let service = DistributedIdService::start(store.clone(), owner(1), test_config)
            .await
            .unwrap();
        let generator = service.generator();
        let initial_worker = service.wait_until_active().await.unwrap();

        let mut states = service.subscribe_state();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    &*states.borrow_and_update(),
                    DistributedIdState::Reacquiring { .. }
                ) {
                    break;
                }
                states.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            generator.try_next_id(),
            Err(DistributedIdError::LeaseUnavailable)
        ));

        store.allow_reacquire.notify_one();
        let recovered_worker =
            tokio::time::timeout(Duration::from_secs(1), service.wait_until_active())
                .await
                .unwrap()
                .unwrap();
        assert_ne!(initial_worker, recovered_worker);
        assert!(generator.try_next_id().is_ok());
        service.shutdown().await.unwrap();
    }

    #[derive(Debug)]
    struct SlowAcquireStore {
        inner: InMemoryWorkerIdLeaseStore,
        acquire_delay: Duration,
        acquisitions: AtomicUsize,
    }

    impl SlowAcquireStore {
        fn new(acquire_delay: Duration) -> Self {
            Self {
                inner: InMemoryWorkerIdLeaseStore::default(),
                acquire_delay,
                acquisitions: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl WorkerIdLeaseStore for SlowAcquireStore {
        async fn acquire(
            &self,
            owner: &WorkerIdOwner,
            range: WorkerIdRange,
            ttl: Duration,
        ) -> Result<WorkerIdAcquisition, WorkerIdStoreError> {
            tokio::time::sleep(self.acquire_delay).await;
            if self.acquisitions.fetch_add(1, Ordering::AcqRel) > 0 {
                return Err(WorkerIdStoreError::unavailable(owner, range));
            }
            self.inner.acquire(owner, range, ttl).await
        }

        async fn renew(
            &self,
            _lease: &WorkerIdLease,
            _ttl: Duration,
        ) -> Result<Option<WorkerIdLease>, WorkerIdStoreError> {
            Err(WorkerIdStoreError::Backend {
                message: "renewal is unreachable".to_string(),
            })
        }

        async fn release(&self, lease: &WorkerIdLease) -> Result<bool, WorkerIdStoreError> {
            self.inner.release(lease).await
        }
    }

    #[derive(Debug, Default)]
    struct LoseOnceStore {
        inner: InMemoryWorkerIdLeaseStore,
        acquisitions: AtomicUsize,
        lose_renewal: AtomicBool,
        allow_reacquire: Notify,
    }

    #[async_trait]
    impl WorkerIdLeaseStore for LoseOnceStore {
        async fn acquire(
            &self,
            owner: &WorkerIdOwner,
            range: WorkerIdRange,
            ttl: Duration,
        ) -> Result<WorkerIdAcquisition, WorkerIdStoreError> {
            let attempt = self.acquisitions.fetch_add(1, Ordering::AcqRel);
            if attempt > 0 {
                self.allow_reacquire.notified().await;
            } else {
                self.lose_renewal.store(true, Ordering::Release);
            }
            self.inner.acquire(owner, range, ttl).await
        }

        async fn renew(
            &self,
            lease: &WorkerIdLease,
            ttl: Duration,
        ) -> Result<Option<WorkerIdLease>, WorkerIdStoreError> {
            if self.lose_renewal.swap(false, Ordering::AcqRel) {
                return Ok(None);
            }
            self.inner.renew(lease, ttl).await
        }

        async fn release(&self, lease: &WorkerIdLease) -> Result<bool, WorkerIdStoreError> {
            self.inner.release(lease).await
        }
    }
}
