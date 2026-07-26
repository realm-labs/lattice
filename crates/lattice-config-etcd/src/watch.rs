use std::time::{Duration, Instant};

use async_trait::async_trait;
use lattice_config::store::ConfigStoreError;
use tokio::sync::watch;

pub const WATCH_TARGET: &str = "lattice.config.etcd";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigStaleness {
    stale: bool,
    stale_since: Option<Instant>,
    disconnects: u64,
}

impl ConfigStaleness {
    pub(crate) const fn fresh() -> Self {
        Self {
            stale: false,
            stale_since: None,
            disconnects: 0,
        }
    }

    pub const fn is_stale(&self) -> bool {
        self.stale
    }

    pub const fn stale_since(&self) -> Option<Instant> {
        self.stale_since
    }

    pub const fn disconnects(&self) -> u64 {
        self.disconnects
    }
}

#[derive(Debug, Clone)]
pub struct ConfigStalenessWatch {
    rx: watch::Receiver<ConfigStaleness>,
}

impl ConfigStalenessWatch {
    pub(crate) const fn new(rx: watch::Receiver<ConfigStaleness>) -> Self {
        Self { rx }
    }

    pub fn current(&self) -> ConfigStaleness {
        *self.rx.borrow()
    }

    pub async fn changed(&mut self) -> Result<ConfigStaleness, ConfigStoreError> {
        self.rx
            .changed()
            .await
            .map_err(|_| ConfigStoreError::WatchClosed)?;
        Ok(*self.rx.borrow())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WatchRetryPolicy {
    pub(crate) initial: Duration,
    pub(crate) max: Duration,
    pub(crate) multiplier: f64,
    pub(crate) jitter: f64,
}

impl Default for WatchRetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: 0.25,
        }
    }
}

pub(crate) struct RetryBackoff {
    policy: WatchRetryPolicy,
    current: Duration,
    sequence: u64,
}

impl RetryBackoff {
    pub(crate) fn new(policy: WatchRetryPolicy) -> Self {
        Self {
            current: policy.initial,
            policy,
            sequence: 0,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.current = self.policy.initial;
        self.sequence = 0;
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        self.sequence = self.sequence.wrapping_add(1);
        let unit = (self.sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 11) as f64
            / ((1_u64 << 53) as f64);
        let factor = 1.0 + ((unit * 2.0) - 1.0) * self.policy.jitter;
        let delay = self.current.mul_f64(factor).min(self.policy.max);
        self.current = self
            .current
            .mul_f64(self.policy.multiplier)
            .min(self.policy.max);
        delay
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EtcdSnapshot {
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) revision: i64,
}

#[async_trait]
pub(crate) trait EtcdWatchSession: Send + 'static {
    async fn next(&mut self) -> Result<Vec<Option<Vec<u8>>>, ConfigStoreError>;
    async fn cancel(&mut self);
}

#[async_trait]
pub(crate) trait EtcdWatchBackend: Clone + Send + Sync + 'static {
    type Session: EtcdWatchSession;

    async fn snapshot(&self, key: &str) -> Result<EtcdSnapshot, ConfigStoreError>;
    async fn watch_from(
        &self,
        key: &str,
        start_revision: i64,
    ) -> Result<Self::Session, ConfigStoreError>;
}

pub(crate) struct RawConfigWatch {
    pub(crate) values: watch::Receiver<Option<Vec<u8>>>,
    pub(crate) staleness: watch::Receiver<ConfigStaleness>,
}

pub(crate) fn spawn_config_watch<B>(
    backend: B,
    key: String,
    snapshot: EtcdSnapshot,
    policy: WatchRetryPolicy,
) -> RawConfigWatch
where
    B: EtcdWatchBackend,
{
    let (values_tx, values_rx) = watch::channel(snapshot.value);
    let (staleness_tx, staleness_rx) = watch::channel(ConfigStaleness::fresh());

    tokio::spawn(run_config_watch(
        backend,
        key,
        snapshot.revision,
        values_tx,
        staleness_tx,
        policy,
    ));

    RawConfigWatch {
        values: values_rx,
        staleness: staleness_rx,
    }
}

enum SessionOutcome {
    ConsumerGone,
    Interrupted {
        progressed: bool,
        error: ConfigStoreError,
    },
}

async fn run_config_watch<B>(
    backend: B,
    key: String,
    initial_revision: i64,
    values: watch::Sender<Option<Vec<u8>>>,
    staleness: watch::Sender<ConfigStaleness>,
    policy: WatchRetryPolicy,
) where
    B: EtcdWatchBackend,
{
    let mut backoff = RetryBackoff::new(policy);
    let mut known_revision = Some(initial_revision);

    loop {
        let revision = match known_revision.take() {
            Some(revision) => revision,
            None => {
                let resync = tokio::select! {
                    () = values.closed() => return,
                    result = backend.snapshot(&key) => result,
                };
                match resync {
                    Ok(snapshot) => {
                        values.send_if_modified(|current| {
                            if *current == snapshot.value {
                                false
                            } else {
                                *current = snapshot.value;
                                true
                            }
                        });
                        snapshot.revision
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: WATCH_TARGET,
                            key = %key,
                            error = %error,
                            "config watch resync failed",
                        );
                        mark_stale(&staleness);
                        if !delay_until_closed(&values, backoff.next_delay()).await {
                            return;
                        }
                        continue;
                    }
                }
            }
        };

        let registration = tokio::select! {
            () = values.closed() => return,
            result = backend.watch_from(&key, revision + 1) => result,
        };
        let mut session = match registration {
            Ok(session) => session,
            Err(error) => {
                tracing::warn!(
                    target: WATCH_TARGET,
                    key = %key,
                    start_revision = revision + 1,
                    error = %error,
                    "config watch registration failed",
                );
                mark_stale(&staleness);
                if !delay_until_closed(&values, backoff.next_delay()).await {
                    return;
                }
                continue;
            }
        };
        mark_fresh(&staleness);

        let outcome = pump_session(&mut session, &values).await;
        session.cancel().await;

        match outcome {
            SessionOutcome::ConsumerGone => return,
            SessionOutcome::Interrupted { progressed, error } => {
                if progressed {
                    backoff.reset();
                }
                tracing::warn!(
                    target: WATCH_TARGET,
                    key = %key,
                    error = %error,
                    "config watch interrupted; reconnecting",
                );
                mark_stale(&staleness);
                if !delay_until_closed(&values, backoff.next_delay()).await {
                    return;
                }
            }
        }
    }
}

async fn pump_session<S>(session: &mut S, values: &watch::Sender<Option<Vec<u8>>>) -> SessionOutcome
where
    S: EtcdWatchSession,
{
    let mut progressed = false;
    loop {
        let batch = tokio::select! {
            () = values.closed() => return SessionOutcome::ConsumerGone,
            result = session.next() => result,
        };
        match batch {
            Ok(updates) => {
                progressed = true;
                for value in updates {
                    values.send_replace(value);
                }
            }
            Err(error) => return SessionOutcome::Interrupted { progressed, error },
        }
    }
}

async fn delay_until_closed(values: &watch::Sender<Option<Vec<u8>>>, delay: Duration) -> bool {
    tokio::select! {
        () = values.closed() => false,
        () = tokio::time::sleep(delay) => true,
    }
}

fn mark_stale(staleness: &watch::Sender<ConfigStaleness>) {
    staleness.send_if_modified(|current| {
        if current.stale {
            return false;
        }
        current.stale = true;
        current.stale_since = Some(Instant::now());
        current.disconnects = current.disconnects.saturating_add(1);
        true
    });
}

fn mark_fresh(staleness: &watch::Sender<ConfigStaleness>) {
    staleness.send_if_modified(|current| {
        if !current.stale {
            return false;
        }
        current.stale = false;
        current.stale_since = None;
        true
    });
}
