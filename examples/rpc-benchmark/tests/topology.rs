use rpc_benchmark::topology::{BenchmarkConfig, BenchmarkTopology};
use rpc_benchmark::workload::{WorkloadConfig, run_cross_service_chain, run_routed_rpc_fanout};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_starts_and_shuts_down_cleanly() {
    let config = BenchmarkConfig::test_default();
    let topology = BenchmarkTopology::start(&config).await.unwrap();

    topology.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routed_rpc_fanout_exercises_multiple_actors() {
    let config = BenchmarkConfig::test_default();
    let topology = BenchmarkTopology::start(&config).await.unwrap();
    let report = run_routed_rpc_fanout(&topology, &WorkloadConfig::from(&config))
        .await
        .unwrap();

    assert_eq!(report.successes, config.requests);
    assert_eq!(report.errors, 0);
    assert!(report.observed_actor_ids.len() > 1);

    topology.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_service_chain_exercises_multiple_actors() {
    let config = BenchmarkConfig::test_default();
    let topology = BenchmarkTopology::start(&config).await.unwrap();
    let report = run_cross_service_chain(&topology, &WorkloadConfig::from(&config))
        .await
        .unwrap();

    assert_eq!(report.successes, config.requests);
    assert_eq!(report.errors, 0);
    assert!(report.observed_actor_ids.len() > 1);

    topology.shutdown().await.unwrap();
}
