use std::process::{Command, Output};

use lattice_coordination::storage::{
    ClusterLifecycleStore, CoordinatorLeaseStore,
    candidates::CandidateStore,
    etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
    records::DurableStorageLimits,
};
use lattice_model::{
    cluster::CoordinatorScope,
    run::{RunCompletion, RunPhase},
};

fn invoke(endpoints: &str, namespace: &str, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lattice-admin"))
        .args([
            "--endpoints",
            endpoints,
            "--namespace",
            namespace,
            "--epoch",
            "1",
            "--allow-insecure-local",
        ])
        .args(arguments)
        .env_remove("LATTICE_ADMIN_ETCD_USER")
        .env_remove("LATTICE_ADMIN_ETCD_PASSWORD")
        .output()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires isolated loopback etcd via LATTICE_ETCD_ENDPOINTS"]
async fn offline_cli_defaults_to_read_only_and_requires_exact_confirmation() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS").expect("isolated etcd");
    // The test process ID and time distinguish its dedicated namespace without
    // adding a UUID dependency to the operational CLI.
    let namespace = format!(
        "/lattice-admin-test/{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.split(',').map(str::to_owned).collect(),
        cluster_prefix: namespace.clone(),
        list_page_size: 4,
        limits: DurableStorageLimits {
            maximum_slots: 8,
            maximum_plans: 8,
            maximum_members: 8,
            maximum_admin_operations: 8,
            maximum_entity_configs: 8,
            maximum_singleton_configs: 8,
        },
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_framework().await.unwrap();
    let original = store.lifecycle().await.unwrap();
    let preview_candidate = [
        "candidate",
        "--node-id",
        "coordinator-a",
        "--expected-revision",
        "0",
        "--operation",
        "enable-a",
    ];
    assert!(
        invoke(&endpoints, &namespace, &preview_candidate)
            .status
            .success()
    );
    assert_eq!(
        store
            .candidate_set(&CoordinatorScope::Cluster)
            .await
            .unwrap()
            .eligible_count,
        0
    );
    let mut apply_candidate = preview_candidate.to_vec();
    apply_candidate.push("--execute");
    let changed = invoke(&endpoints, &namespace, &apply_candidate);
    assert!(changed.status.success(), "{changed:?}");
    assert!(
        invoke(&endpoints, &namespace, &apply_candidate)
            .status
            .success()
    );
    assert_eq!(
        store
            .candidate_set(&CoordinatorScope::Cluster)
            .await
            .unwrap()
            .eligible_count,
        1
    );
    let inspection = invoke(&endpoints, &namespace, &["candidates"]);
    assert!(inspection.status.success(), "{inspection:?}");
    assert!(String::from_utf8_lossy(&inspection.stdout).contains("coordinator-a"));
    let last_removal = invoke(
        &endpoints,
        &namespace,
        &[
            "candidate",
            "--node-id",
            "coordinator-a",
            "--expected-revision",
            "1",
            "--operation",
            "remove-a",
            "--disable",
            "--execute",
        ],
    );
    assert!(!last_removal.status.success());
    assert_eq!(
        store
            .candidate_set(&CoordinatorScope::Cluster)
            .await
            .unwrap()
            .eligible_count,
        1
    );

    assert!(
        invoke(&endpoints, &namespace, &["inspect"])
            .status
            .success()
    );
    let preview = invoke(
        &endpoints,
        &namespace,
        &["reset", "--operation", "cli-reset"],
    );
    assert!(preview.status.success(), "{preview:?}");
    assert!(String::from_utf8_lossy(&preview.stdout).contains("DRY RUN"));
    assert_eq!(store.lifecycle().await.unwrap(), original);
    let rejected = invoke(
        &endpoints,
        &namespace,
        &["reset", "--operation", "cli-reset", "--execute"],
    );
    assert!(!rejected.status.success());
    assert_eq!(store.lifecycle().await.unwrap(), original);
    let confirmation = format!("{namespace}@1");
    let reset = invoke(
        &endpoints,
        &namespace,
        &[
            "reset",
            "--operation",
            "cli-reset",
            "--execute",
            "--confirm-stopped",
            &confirmation,
        ],
    );
    assert!(reset.status.success(), "{reset:?}");
    assert!(matches!(
        store.lifecycle().await.unwrap().phase,
        RunPhase::Closed {
            completion: RunCompletion::Reset,
            ..
        }
    ));
    assert!(
        invoke(&endpoints, &namespace, &["start-run"])
            .status
            .success()
    );
    assert_eq!(store.lifecycle().await.unwrap().epoch.get(), 1);
    let next = invoke(&endpoints, &namespace, &["start-run", "--execute"]);
    assert!(next.status.success(), "{next:?}");
    assert_eq!(store.lifecycle().await.unwrap().epoch.get(), 2);
    assert!(
        !invoke(
            &endpoints,
            &namespace,
            &[
                "reset",
                "--operation",
                "cli-reset",
                "--execute",
                "--confirm-stopped",
                &confirmation
            ]
        )
        .status
        .success()
    );
}
