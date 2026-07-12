use remoting_benchmark::{BenchmarkConfig, RemotingTopology};

#[tokio::test]
async fn bounded_multi_lane_association_admits_bulk_tells() {
    let config = BenchmarkConfig::test_default();
    let topology = RemotingTopology::start(&config).unwrap();
    let report = topology
        .run_bulk_tell(config.requests, config.payload_bytes)
        .await
        .unwrap();
    assert_eq!(report.successes, config.requests);
    assert_eq!(report.errors, 0);
    topology.shutdown().await;
}
