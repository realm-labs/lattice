use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::runtime::spawn_actor;
use lattice_actor::traits::{Actor, Handler, StopReason};
use lattice_store_mongodb::actor::{CompletionStatus, MongoFlushCompleted, PersistenceStatus};
use lattice_store_mongodb::coordinator::MongoPersistenceCoordinator;
use lattice_store_mongodb::document::LoadedDocument;
use lattice_store_mongodb::error::MongoStoreError;
use lattice_store_mongodb::mongo::MongoDocumentKey;
use lattice_store_mongodb::prepared::{
    DocumentWriteOutcome, FlushOutcome, PreparedDocumentWrite, PreparedWriteStore,
};
use lattice_store_mongodb::scan::ScanBudget;
use lattice_store_mongodb::tracked::Tracked;
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

#[derive(Debug, lattice_actor::Message)]
struct Persist;

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
}

#[async_trait]
impl Actor for PersistenceActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<Persist> for PersistenceActor {
    async fn handle(
        &mut self,
        context: &mut ActorContext<Self>,
        _message: Persist,
    ) -> Result<(), Self::Error> {
        let prepared = self
            .coordinator
            .prepare(ScanBudget::generous(), |preparation| {
                preparation.scan_tracked(&self.document)
            })
            .map_err(ActorError::from_error)?;
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

#[async_trait]
impl Handler<MongoFlushCompleted> for PersistenceActor {
    async fn handle(
        &mut self,
        _context: &mut ActorContext<Self>,
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
                lattice_store_mongodb::coordinator::PersistenceReport {
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
