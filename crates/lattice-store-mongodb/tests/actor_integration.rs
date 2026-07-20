use lattice_actor::context::HandlerContext;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorError, ActorStopError};
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::runtime::spawn_actor;
use lattice_actor::traits::{Actor, ActorLifecycleState, Handler, StopReason};
use lattice_store_mongodb::document::LoadedDocument;
use lattice_store_mongodb::document::tracked::Tracked;
use lattice_store_mongodb::error::MongoStoreError;
use lattice_store_mongodb::persistence::actor::{
    CompletionStatus, MongoFlushCompleted, PersistenceStatus,
};
use lattice_store_mongodb::persistence::coordinator::MongoPersistenceCoordinator;
use lattice_store_mongodb::persistence::coordinator::drain::{MongoDrainOptions, MongoDrainReport};
use lattice_store_mongodb::persistence::request::{
    DocumentWriteOutcome, FlushGeneration, FlushOutcome, PreparedDocumentWrite, PreparedWriteStore,
};
use lattice_store_mongodb::persistence::types::MongoDocumentKey;
use lattice_store_mongodb::scan::ScanBudget;
use lattice_store_mongodb::{MongoDocument, MongoScan};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "actor_persistence_test")]
struct TestDocument {
    #[mongo(id)]
    id: u64,
    value: String,
}

#[derive(Clone)]
struct AcknowledgingStore;

#[async_trait]
impl PreparedWriteStore for AcknowledgingStore {
    async fn flush(
        &self,
        writes: Vec<PreparedDocumentWrite>,
    ) -> Result<FlushOutcome, MongoStoreError> {
        Ok(FlushOutcome {
            documents: writes
                .into_iter()
                .map(|write| {
                    (
                        write.token,
                        DocumentWriteOutcome::Applied {
                            previous_version: write.expected_version,
                            new_version: write.expected_version + 1,
                            updated_at_ms: 88,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        })
    }
}

#[derive(Clone)]
struct BlockingStore {
    entered: Arc<Semaphore>,
    dropped: Arc<Semaphore>,
}

struct DropSignal(Arc<Semaphore>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

#[async_trait]
impl PreparedWriteStore for BlockingStore {
    async fn flush(
        &self,
        _writes: Vec<PreparedDocumentWrite>,
    ) -> Result<FlushOutcome, MongoStoreError> {
        let _drop_signal = DropSignal(self.dropped.clone());
        self.entered.add_permits(1);
        std::future::pending().await
    }
}

#[derive(Clone)]
struct BlockingThenAcknowledgingStore {
    attempts: Arc<AtomicUsize>,
    entered: Arc<Semaphore>,
    dropped: Arc<Semaphore>,
}

#[async_trait]
impl PreparedWriteStore for BlockingThenAcknowledgingStore {
    async fn flush(
        &self,
        writes: Vec<PreparedDocumentWrite>,
    ) -> Result<FlushOutcome, MongoStoreError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            let _drop_signal = DropSignal(self.dropped.clone());
            self.entered.add_permits(1);
            std::future::pending().await
        } else {
            Ok(FlushOutcome {
                documents: writes
                    .into_iter()
                    .map(|write| {
                        (
                            write.token,
                            DocumentWriteOutcome::Applied {
                                previous_version: write.expected_version,
                                new_version: write.expected_version + 1,
                                updated_at_ms: 101,
                            },
                        )
                    })
                    .collect(),
            })
        }
    }
}

#[derive(Debug, lattice_actor::Message)]
struct Persist;

#[derive(Debug, lattice_actor::Message)]
struct AbortPersist;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Observed {
    Completed {
        status: CompletionStatus,
        metadata: Option<(i64, i64)>,
    },
    Rejected {
        retry_attempt: u32,
    },
}

struct PersistenceActor {
    coordinator: MongoPersistenceCoordinator,
    document: Tracked<TestDocument>,
    store: Arc<dyn PreparedWriteStore>,
    observed: Arc<Mutex<Option<Observed>>>,
    signal: Arc<Semaphore>,
    generation: Option<FlushGeneration>,
}

impl Actor for PersistenceActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Handler<Persist> for PersistenceActor {
    async fn handle(
        &mut self,
        context: &mut HandlerContext<'_, Self>,
        _message: Persist,
    ) -> Result<(), Self::Error> {
        let prepared = self
            .coordinator
            .prepare(ScanBudget::generous(), |preparation| {
                preparation.scan_tracked(&self.document)
            })
            .map_err(ActorError::from_error)?;
        self.generation = prepared.request.as_ref().map(|request| request.generation);
        match self
            .coordinator
            .dispatch_prepared(context, self.store.clone(), prepared)
        {
            Ok(PersistenceStatus::InFlight) => Ok(()),
            Ok(other) => Err(ActorError::new(format!(
                "expected in-flight persistence, got {other:?}"
            ))),
            Err(_) => {
                *self.observed.lock().expect("result mutex poisoned") = Some(Observed::Rejected {
                    retry_attempt: self.coordinator.retry_attempt(),
                });
                self.signal.add_permits(1);
                Ok(())
            }
        }
    }
}

impl Handler<AbortPersist> for PersistenceActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        _message: AbortPersist,
    ) -> Result<(), Self::Error> {
        let generation = self
            .generation
            .take()
            .ok_or_else(|| ActorError::new("no persistence generation to abort"))?;
        self.coordinator
            .abort_in_flight_as_unknown(generation, "operator intervention")
            .map_err(ActorError::from_error)?;
        Ok(())
    }
}

impl Handler<MongoFlushCompleted> for PersistenceActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        completion: MongoFlushCompleted,
    ) -> Result<(), Self::Error> {
        let status = self
            .coordinator
            .apply_completion(completion)
            .map_err(ActorError::from_error)?;
        let key = MongoDocumentKey::for_document::<TestDocument>(&42)
            .expect("test document ID should encode");
        *self.observed.lock().expect("result mutex poisoned") = Some(Observed::Completed {
            status,
            metadata: self.coordinator.document_meta(&key),
        });
        self.signal.add_permits(1);
        Ok(())
    }
}

fn actor(
    observed: Arc<Mutex<Option<Observed>>>,
    signal: Arc<Semaphore>,
    store: Arc<dyn PreparedWriteStore>,
) -> PersistenceActor {
    let old = TestDocument {
        id: 42,
        value: "old".to_owned(),
    };
    let mut coordinator = MongoPersistenceCoordinator::new(5);
    let mut document = coordinator
        .track_loaded(LoadedDocument {
            version: 2,
            updated_at_ms: 7,
            value: old,
        })
        .expect("fixture document should attach");
    document.write().value = "new".to_owned();
    PersistenceActor {
        coordinator,
        document,
        store,
        observed,
        signal,
        generation: None,
    }
}

struct StoppingDrainActor {
    coordinator: MongoPersistenceCoordinator,
    document: Tracked<TestDocument>,
    store: Arc<dyn PreparedWriteStore>,
    report: Arc<Mutex<Option<MongoDrainReport>>>,
    drained: Arc<Semaphore>,
    drain_options: MongoDrainOptions,
}

impl Actor for StoppingDrainActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn stopping(
        &mut self,
        _context: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        let report = self
            .coordinator
            .drain(self.store.as_ref(), self.drain_options, |preparation| {
                preparation.scan_tracked(&self.document)
            })
            .await?;
        *self.report.lock().expect("drain report mutex poisoned") = Some(report);
        self.drained.add_permits(1);
        Ok(())
    }
}

impl Handler<Persist> for StoppingDrainActor {
    async fn handle(
        &mut self,
        context: &mut HandlerContext<'_, Self>,
        _message: Persist,
    ) -> Result<(), Self::Error> {
        let prepared = self
            .coordinator
            .prepare(ScanBudget::generous(), |preparation| {
                preparation.scan_tracked(&self.document)
            })
            .map_err(ActorError::from_error)?;
        self.coordinator
            .dispatch_prepared(context, self.store.clone(), prepared)
            .map_err(ActorError::from_error)?;
        Ok(())
    }
}

impl Handler<MongoFlushCompleted> for StoppingDrainActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        completion: MongoFlushCompleted,
    ) -> Result<(), Self::Error> {
        self.coordinator
            .apply_completion(completion)
            .map_err(ActorError::from_error)?;
        Ok(())
    }
}

fn stopping_drain_actor(
    store: Arc<dyn PreparedWriteStore>,
    report: Arc<Mutex<Option<MongoDrainReport>>>,
    drained: Arc<Semaphore>,
    drain_options: MongoDrainOptions,
) -> StoppingDrainActor {
    let mut coordinator = MongoPersistenceCoordinator::new(11);
    let mut document = coordinator
        .track_loaded(LoadedDocument {
            version: 4,
            updated_at_ms: 8,
            value: TestDocument {
                id: 42,
                value: "old".to_owned(),
            },
        })
        .expect("fixture document should attach");
    document.write().value = "new".to_owned();
    StoppingDrainActor {
        coordinator,
        document,
        store,
        report,
        drained,
        drain_options,
    }
}

#[tokio::test]
async fn prepared_flush_returns_through_pipe_to_self_and_commits_metadata() {
    let observed = Arc::new(Mutex::new(None));
    let signal = Arc::new(Semaphore::new(0));
    let handle = spawn_actor(
        actor(
            observed.clone(),
            signal.clone(),
            Arc::new(AcknowledgingStore),
        ),
        MailboxConfig::bounded(8),
    );
    handle
        .tell(Persist)
        .await
        .expect("persist tell should enqueue");
    tokio::time::timeout(Duration::from_secs(2), signal.acquire())
        .await
        .expect("completion should arrive")
        .expect("signal should remain open")
        .forget();
    assert_eq!(
        observed.lock().expect("result mutex poisoned").clone(),
        Some(Observed::Completed {
            status: CompletionStatus::Applied(
                lattice_store_mongodb::persistence::coordinator::PersistenceReport {
                    clean: 0,
                    applied: 1,
                    failed: 0,
                    conflicts: 0,
                }
            ),
            metadata: Some((3, 88)),
        })
    );
}

#[tokio::test]
async fn pipe_capacity_rejection_rolls_back_in_flight_and_schedules_retry() {
    let observed = Arc::new(Mutex::new(None));
    let signal = Arc::new(Semaphore::new(0));
    let handle = spawn_actor(
        actor(
            observed.clone(),
            signal.clone(),
            Arc::new(AcknowledgingStore),
        ),
        MailboxConfig::bounded(8).with_deferred_capacity(0),
    );
    handle
        .tell(Persist)
        .await
        .expect("persist tell should enqueue");
    tokio::time::timeout(Duration::from_secs(2), signal.acquire())
        .await
        .expect("rejection should be observed")
        .expect("signal should remain open")
        .forget();
    assert_eq!(
        observed.lock().expect("result mutex poisoned").clone(),
        Some(Observed::Rejected { retry_attempt: 1 })
    );
}

#[tokio::test]
async fn stopping_actor_cancels_in_flight_store_future() {
    let observed = Arc::new(Mutex::new(None));
    let signal = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(Semaphore::new(0));
    let handle = spawn_actor(
        actor(
            observed,
            signal,
            Arc::new(BlockingStore {
                entered: entered.clone(),
                dropped: dropped.clone(),
            }),
        ),
        MailboxConfig::bounded(8),
    );
    handle
        .tell(Persist)
        .await
        .expect("persist tell should enqueue");
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .expect("store future should start")
        .expect("entry signal should remain open")
        .forget();
    handle
        .stop(StopReason::Requested)
        .await
        .expect("actor stop should enqueue");
    tokio::time::timeout(Duration::from_secs(2), dropped.acquire())
        .await
        .expect("actor stop should cancel the pipe future")
        .expect("drop signal should remain open")
        .forget();
}

#[tokio::test]
async fn aborting_in_flight_marks_unknown_and_cancels_pipe_task() {
    let observed = Arc::new(Mutex::new(None));
    let signal = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(Semaphore::new(0));
    let handle = spawn_actor(
        actor(
            observed.clone(),
            signal.clone(),
            Arc::new(BlockingStore {
                entered: entered.clone(),
                dropped: dropped.clone(),
            }),
        ),
        MailboxConfig::bounded(8),
    );
    handle.tell(Persist).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .expect("store future should start")
        .expect("entry signal should remain open")
        .forget();

    handle.tell(AbortPersist).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), dropped.acquire())
        .await
        .expect("manual abort should cancel the store future")
        .expect("drop signal should remain open")
        .forget();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        observed.lock().expect("result mutex poisoned").clone(),
        None,
        "aborting the pipe task must not synthesize a completion"
    );
}

#[tokio::test]
async fn actor_stopping_replays_and_awaits_the_cancelled_in_flight_flush() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(Semaphore::new(0));
    let drained = Arc::new(Semaphore::new(0));
    let report = Arc::new(Mutex::new(None));
    let store = Arc::new(BlockingThenAcknowledgingStore {
        attempts: attempts.clone(),
        entered: entered.clone(),
        dropped: dropped.clone(),
    });
    let handle = spawn_actor(
        stopping_drain_actor(
            store,
            report.clone(),
            drained.clone(),
            MongoDrainOptions {
                timeout: Duration::from_secs(2),
                ..MongoDrainOptions::default()
            },
        ),
        MailboxConfig::bounded(8),
    );

    handle.tell(Persist).await.expect("persist should enqueue");
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .expect("first flush should start")
        .expect("entry signal should remain open")
        .forget();
    handle
        .stop(StopReason::Requested)
        .await
        .expect("stop should enqueue");
    tokio::time::timeout(Duration::from_secs(2), dropped.acquire())
        .await
        .expect("stopping should cancel the actor-dispatched flush")
        .expect("drop signal should remain open")
        .forget();
    tokio::time::timeout(Duration::from_secs(2), drained.acquire())
        .await
        .expect("stopping should finish the direct drain")
        .expect("drain signal should remain open")
        .forget();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let report = report
        .lock()
        .expect("drain report mutex poisoned")
        .expect("drain report should be retained");
    assert_eq!(report.recovered_in_flight, 1);
    assert_eq!(report.flush_attempts, 1);
    assert_eq!(report.persistence.applied, 1);
}

#[tokio::test]
async fn retry_stop_resumes_a_timed_out_drain_without_losing_dirty_state() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(Semaphore::new(0));
    let drained = Arc::new(Semaphore::new(0));
    let report = Arc::new(Mutex::new(None));
    let store = Arc::new(BlockingThenAcknowledgingStore {
        attempts: attempts.clone(),
        entered: entered.clone(),
        dropped: dropped.clone(),
    });
    let handle = spawn_actor(
        stopping_drain_actor(
            store,
            report.clone(),
            drained.clone(),
            MongoDrainOptions {
                timeout: Duration::from_millis(20),
                ..MongoDrainOptions::default()
            },
        ),
        MailboxConfig::bounded(8),
    );
    let mut lifecycle = handle.subscribe_lifecycle();

    handle
        .stop(StopReason::Requested)
        .await
        .expect("stop should enqueue");
    tokio::time::timeout(Duration::from_secs(2), async {
        while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
            lifecycle
                .changed()
                .await
                .expect("actor should remain alive");
        }
    })
    .await
    .expect("timed out drain should enter StopFailed");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(
        report
            .lock()
            .expect("drain report mutex poisoned")
            .is_none()
    );

    handle
        .retry_stop()
        .await
        .expect("retry_stop should replay retained work");
    tokio::time::timeout(Duration::from_secs(2), drained.acquire())
        .await
        .expect("retry should finish the direct drain")
        .expect("drain signal should remain open")
        .forget();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let report = report
        .lock()
        .expect("drain report mutex poisoned")
        .expect("successful retry should retain a report");
    assert_eq!(report.recovered_in_flight, 1);
    assert_eq!(report.persistence.applied, 1);
}
