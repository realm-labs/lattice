use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lattice_store_mongodb::document::{LoadedDocument, tracked::Tracked};
use lattice_store_mongodb::error::MongoStoreError;
use lattice_store_mongodb::persistence::coordinator::MongoPersistenceCoordinator;
use lattice_store_mongodb::persistence::coordinator::drain::{MongoDrainOptions, MongoDrainReport};
use lattice_store_mongodb::persistence::request::{
    DocumentWriteOutcome, FlushOutcome, PreparedDocumentWrite, PreparedWriteStore,
};
use lattice_store_mongodb::scan::ScanBudget;
use lattice_store_mongodb::{MongoDocument, MongoScan};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "persistence_benchmark")]
struct PersistenceDocument {
    #[mongo(id)]
    id: u64,
    revision: u64,
    payload: String,
    #[mongo(scan = "map")]
    attributes: BTreeMap<String, u64>,
}

#[derive(Clone, Copy)]
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
                            updated_at_ms: 1,
                        },
                    )
                })
                .collect(),
        })
    }
}

struct PersistenceFixture {
    coordinator: MongoPersistenceCoordinator,
    documents: Vec<Tracked<PersistenceDocument>>,
    next_revision: u64,
}

impl PersistenceFixture {
    fn new(document_count: usize, payload_bytes: usize) -> Self {
        let loaded = (0..document_count)
            .map(|index| LoadedDocument {
                version: 1,
                updated_at_ms: 1,
                value: PersistenceDocument {
                    id: index as u64 + 1,
                    revision: 0,
                    payload: "x".repeat(payload_bytes),
                    attributes: BTreeMap::from([("first".to_owned(), 1), ("second".to_owned(), 2)]),
                },
            })
            .collect();
        let mut coordinator = MongoPersistenceCoordinator::new(1);
        let documents = coordinator
            .track_loaded_many(loaded)
            .expect("benchmark documents must attach");
        Self {
            coordinator,
            documents,
            next_revision: 1,
        }
    }

    fn mutate_all(&mut self) {
        let revision = self.next_revision;
        self.next_revision = self.next_revision.wrapping_add(1);
        for document in &mut self.documents {
            let value = document.write();
            value.revision = revision;
            value.attributes.insert("first".to_owned(), revision);
        }
    }

    async fn flush_dirty(&mut self, store: &dyn PreparedWriteStore) {
        let documents = &self.documents;
        let prepared = self
            .coordinator
            .prepare(ScanBudget::generous(), |preparation| {
                for document in documents {
                    preparation.scan_tracked(document)?;
                }
                Ok(())
            })
            .expect("benchmark preparation");
        let request = prepared.request.expect("mutated documents must write");
        let generation = request.generation;
        self.coordinator
            .begin_flush(prepared.commit)
            .expect("benchmark begin flush");
        let outcome = store.flush(request.writes).await.expect("benchmark flush");
        let report = self
            .coordinator
            .complete(generation, outcome)
            .expect("benchmark completion");
        assert_eq!(report.applied, self.documents.len());
    }

    async fn drain_dirty(&mut self, store: &dyn PreparedWriteStore) -> MongoDrainReport {
        let documents = &self.documents;
        self.coordinator
            .drain(
                store,
                MongoDrainOptions {
                    timeout: Duration::from_secs(30),
                    max_documents_per_pass: usize::MAX,
                    max_fields_per_pass: usize::MAX,
                    max_scan_duration: Duration::from_secs(30),
                },
                |preparation| {
                    for document in documents {
                        preparation.scan_tracked(document)?;
                    }
                    Ok(())
                },
            )
            .await
            .expect("benchmark drain")
    }
}

fn persistence_pipeline(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let store = AcknowledgingStore;
    let mut group = criterion.benchmark_group("mongo_persistence_pipeline");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    for document_count in [1_usize, 100, 1_000] {
        let fixture = Arc::new(tokio::sync::Mutex::new(PersistenceFixture::new(
            document_count,
            128,
        )));
        group.throughput(Throughput::Elements(document_count as u64));
        group.bench_with_input(
            BenchmarkId::new("prepare_flush_complete", document_count),
            &document_count,
            |bencher, _| {
                let fixture = fixture.clone();
                bencher.to_async(&runtime).iter_custom(move |iterations| {
                    let fixture = fixture.clone();
                    async move {
                        let mut fixture = fixture.lock().await;
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            fixture.mutate_all();
                            let started = Instant::now();
                            fixture.flush_dirty(&store).await;
                            elapsed += started.elapsed();
                        }
                        elapsed
                    }
                });
            },
        );
    }
    group.finish();
}

fn persistence_drain(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let store = AcknowledgingStore;
    let mut group = criterion.benchmark_group("mongo_persistence_drain");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    for document_count in [1_usize, 100, 1_000] {
        let fixture = Arc::new(tokio::sync::Mutex::new(PersistenceFixture::new(
            document_count,
            128,
        )));
        group.throughput(Throughput::Elements(document_count as u64));
        group.bench_with_input(
            BenchmarkId::new("dirty_shutdown", document_count),
            &document_count,
            |bencher, _| {
                let fixture = fixture.clone();
                bencher.to_async(&runtime).iter_custom(move |iterations| {
                    let fixture = fixture.clone();
                    async move {
                        let mut fixture = fixture.lock().await;
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            fixture.mutate_all();
                            let started = Instant::now();
                            let report = fixture.drain_dirty(&store).await;
                            elapsed += started.elapsed();
                            assert_eq!(report.persistence.applied, document_count);
                        }
                        elapsed
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, persistence_pipeline, persistence_drain);
criterion_main!(benches);
