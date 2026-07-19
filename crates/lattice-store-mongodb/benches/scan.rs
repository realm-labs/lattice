use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lattice_store_mongodb::scan::{MongoScan as _, ScanBudget, ScanCursor};
use lattice_store_mongodb::{MongoDocument, MongoScan};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "scan_benchmark_whole")]
struct WholeDocument {
    #[mongo(id)]
    id: u64,
    payload: String,
}

#[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "scan_benchmark_map")]
struct MapDocument {
    #[mongo(id)]
    id: u64,
    #[mongo(scan = "map")]
    entries: BTreeMap<String, String>,
}

fn scan_budget() -> ScanBudget {
    ScanBudget::new(usize::MAX, usize::MAX, usize::MAX, Duration::from_secs(60))
}

fn whole_document_scans(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mongo_scan_whole_document");
    for size in [1_024_usize, 10_240, 1_048_576] {
        let baseline_value = WholeDocument {
            id: 1,
            payload: "a".repeat(size),
        };
        let baseline = baseline_value.capture().expect("benchmark baseline");
        let changed = WholeDocument {
            id: 1,
            payload: "b".repeat(size),
        };
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("diff", size), &size, |bencher, _| {
            bencher.iter(|| {
                changed
                    .diff(
                        black_box(&baseline),
                        ScanCursor::default(),
                        &mut scan_budget(),
                    )
                    .expect("benchmark diff")
            });
        });
    }
    group.finish();
}

fn map_document_scans(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mongo_scan_map_document");
    for entries in [1_000_usize, 10_000] {
        let values = (0..entries)
            .map(|index| (format!("key-{index:08}"), format!("value-{index}")))
            .collect::<BTreeMap<_, _>>();
        let baseline_value = MapDocument {
            id: 1,
            entries: values.clone(),
        };
        let baseline = baseline_value.capture().expect("benchmark baseline");
        let mut changed = MapDocument {
            id: 1,
            entries: values,
        };
        changed
            .entries
            .insert("key-00000000".to_owned(), "changed".to_owned());
        group.throughput(Throughput::Elements(entries as u64));
        group.bench_with_input(
            BenchmarkId::new("single_entry_diff", entries),
            &entries,
            |bencher, _| {
                bencher.iter(|| {
                    changed
                        .diff(
                            black_box(&baseline),
                            ScanCursor::default(),
                            &mut scan_budget(),
                        )
                        .expect("benchmark diff")
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, whole_document_scans, map_document_scans);
criterion_main!(benches);
