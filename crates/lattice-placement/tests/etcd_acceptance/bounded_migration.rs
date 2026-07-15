use super::*;

#[tokio::test]
async fn real_etcd_migration_chunks_large_config_and_domain_finalization_sets() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let mut raw = Client::connect(endpoints.clone(), None).await.unwrap();
    let prefix = format!(
        "/lattice-migration-bounded-transactions/{}",
        uuid::Uuid::new_v4().simple()
    );
    raw.put(format!("{prefix}/schema_generation"), "4", None)
        .await
        .unwrap();
    let mut mapping = MigrationDomainMapping {
        entity_types: BTreeMap::new(),
        singleton_kinds: BTreeMap::new(),
    };
    for index in 0..40 {
        let entity = EntityType::new(format!("entity-{index:02}")).unwrap();
        let target_domain = PlacementDomainId::new(format!("domain-{index:02}")).unwrap();
        let legacy = EntityConfig::new(
            domain(),
            entity.clone(),
            ProtocolId::new(10_000 + index).unwrap(),
            8,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .unwrap();
        raw.put(
            format!("{prefix}/entity_types/{}", entity.as_str()),
            serde_json::to_vec(&legacy).unwrap(),
            None,
        )
        .await
        .unwrap();
        mapping
            .entity_types
            .insert(entity.as_str().to_owned(), target_domain);
    }
    let backup_dir = tempfile::tempdir().unwrap();
    let report = migrate(
        MigrationMode::Apply,
        MigrationConfig {
            endpoints,
            cluster_prefix: prefix.clone(),
            page_size: 3,
            limits: limits(64),
            backup_path: Some(backup_dir.path().join("generation-4.json")),
            mapping,
        },
    )
    .await
    .unwrap();
    assert!(report.completed);
    assert_eq!(report.entity_configs, 40);
    assert_eq!(
        raw.get(format!("{prefix}/schema_generation"), None)
            .await
            .unwrap()
            .kvs()[0]
            .value(),
        b"5"
    );
    let migrated = raw
        .get(
            format!("{prefix}/domains/"),
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await
        .unwrap();
    assert_eq!(
        migrated
            .kvs()
            .iter()
            .filter(|record| record.key_str().unwrap().contains("/entity_types/"))
            .count(),
        40
    );
    assert!(migrated.kvs().iter().any(|record| {
        record
            .key_str()
            .unwrap()
            .ends_with("/domain-39/state_revision")
    }));
}
