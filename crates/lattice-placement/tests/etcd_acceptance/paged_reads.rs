use super::*;

use lattice_placement::storage::page::PageCursor;

const PAGED_ENTITY: &str = "paged-entity";
const PAGED_SINGLETON: &str = "paged-singleton";

fn paged_slot(key: PlacementSlotKey) -> Vec<u8> {
    let slot = PlacementSlot {
        key,
        config_fingerprint: ConfigFingerprint::new([5; 32]),
        owner: Some(node("paged-owner", 70, 29270)),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(4).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    serde_json::to_vec(&slot).unwrap()
}

fn shard_key(shard: u32) -> PlacementSlotKey {
    PlacementSlotKey::Shard {
        domain: domain(),
        entity_type: EntityType::new(PAGED_ENTITY).unwrap(),
        shard_id: ShardId::new(shard),
    }
}

fn singleton_key() -> PlacementSlotKey {
    PlacementSlotKey::Singleton {
        domain: domain(),
        kind: SingletonKind::new(PAGED_SINGLETON).unwrap(),
    }
}

fn shard_record_key(prefix: &str, shard: u32) -> String {
    format!(
        "{prefix}/domains/{}/shards/{PAGED_ENTITY}/{shard}",
        domain().as_str()
    )
}

async fn seed_shard(raw: &mut Client, prefix: &str, shard: u32) {
    raw.put(
        shard_record_key(prefix, shard),
        paged_slot(shard_key(shard)),
        None,
    )
    .await
    .unwrap();
}

fn slot_name(slot: &PlacementSlot) -> String {
    match &slot.key {
        PlacementSlotKey::Shard { shard_id, .. } => format!("shard-{}", shard_id.get()),
        PlacementSlotKey::Singleton { kind, .. } => format!("singleton-{}", kind.as_str()),
    }
}

async fn sweep(
    store: &EtcdPlacementStore,
    limit: usize,
    from: Option<PageCursor>,
) -> (Vec<String>, Vec<usize>) {
    let mut cursor = from;
    let mut seen = Vec::new();
    let mut remaining = Vec::new();
    loop {
        let page = store
            .list_slots_page(&domain(), &[], cursor.as_ref(), limit)
            .await
            .unwrap();
        assert!(page.records.len() <= limit);
        seen.extend(page.records.iter().map(slot_name));
        remaining.push(page.remaining);
        let Some(next) = page.next_cursor else {
            return (seen, remaining);
        };
        cursor = Some(next);
    }
}

/// A page has to be a bounded etcd range and not a full prefix scan sliced in memory, so the sweep
/// is asserted through what a range can report: never more records than the limit, an exact count
/// of what is still to come, and a cursor that walks both slot prefixes in one key order.
#[tokio::test]
async fn real_etcd_slot_pages_are_bounded_ranges_over_both_slot_prefixes() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = format!("/lattice-paged-tests/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdPlacementStore::connect(EtcdPlacementConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 2,
        limits: limits(64),
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_schema_generation().await.unwrap();
    let mut raw = Client::connect(endpoints, None).await.unwrap();
    for shard in 0..6 {
        seed_shard(&mut raw, &prefix, shard).await;
    }
    raw.put(
        format!(
            "{prefix}/domains/{}/singletons/{PAGED_SINGLETON}",
            domain().as_str()
        ),
        paged_slot(singleton_key()),
        None,
    )
    .await
    .unwrap();

    let (seen, remaining) = sweep(&store, 2, None).await;
    assert_eq!(
        seen,
        vec![
            "shard-0".to_owned(),
            "shard-1".to_owned(),
            "shard-2".to_owned(),
            "shard-3".to_owned(),
            "shard-4".to_owned(),
            "shard-5".to_owned(),
            format!("singleton-{PAGED_SINGLETON}"),
        ]
    );
    assert_eq!(remaining, vec![5, 3, 1, 0]);

    assert!(matches!(
        store.list_slots_page(&domain(), &[], None, 0).await,
        Err(StorageError::BackendArgument)
    ));
    let filtered = store
        .list_slots_page(&domain(), &[PlacementSlotState::Fenced], None, 3)
        .await
        .unwrap();
    assert!(filtered.records.is_empty());
    assert_eq!(filtered.remaining, 4);

    raw.delete(
        prefix,
        Some(etcd_client::DeleteOptions::new().with_prefix()),
    )
    .await
    .unwrap();
}

async fn cursor_fixture(
    endpoints: Vec<String>,
    name: &str,
    shards: &[u32],
) -> (EtcdPlacementStore, Client, String) {
    let prefix = format!("/lattice-paged-{name}/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdPlacementStore::connect(EtcdPlacementConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 2,
        limits: limits(64),
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_schema_generation().await.unwrap();
    let mut raw = Client::connect(endpoints, None).await.unwrap();
    for shard in shards {
        seed_shard(&mut raw, &prefix, *shard).await;
    }
    (store, raw, prefix)
}

async fn drop_prefix(raw: &mut Client, prefix: String) {
    raw.delete(
        prefix,
        Some(etcd_client::DeleteOptions::new().with_prefix()),
    )
    .await
    .unwrap();
}

/// The cursor contract the in-memory store is held to, asserted against a real etcd range: a record
/// deleted behind the cursor moves no later record, where an offset resume would skip the record
/// that slid down into the prefix the sweep already consumed.
#[tokio::test]
async fn real_etcd_removing_a_record_behind_the_cursor_skips_no_later_record() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let (store, mut raw, prefix) =
        cursor_fixture(endpoints, "removed-cursor", &[1, 2, 4, 5, 6]).await;
    let first = store
        .list_slots_page(&domain(), &[], None, 2)
        .await
        .unwrap();
    assert_eq!(
        first.records.iter().map(slot_name).collect::<Vec<_>>(),
        vec!["shard-1".to_owned(), "shard-2".to_owned()]
    );
    raw.delete(shard_record_key(&prefix, 1), None)
        .await
        .unwrap();
    let (seen, _) = sweep(&store, 2, first.next_cursor).await;
    assert_eq!(
        seen,
        vec![
            "shard-4".to_owned(),
            "shard-5".to_owned(),
            "shard-6".to_owned()
        ]
    );
    drop_prefix(&mut raw, prefix).await;
}

/// The mirror image: a record inserted behind the cursor is not handed back, where an offset resume
/// would repeat the record the insert pushed up into the next slice.
#[tokio::test]
async fn real_etcd_inserting_a_record_behind_the_cursor_repeats_no_later_record() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let (store, mut raw, prefix) =
        cursor_fixture(endpoints, "inserted-cursor", &[1, 2, 4, 5, 6]).await;
    let first = store
        .list_slots_page(&domain(), &[], None, 2)
        .await
        .unwrap();
    assert_eq!(
        first.records.iter().map(slot_name).collect::<Vec<_>>(),
        vec!["shard-1".to_owned(), "shard-2".to_owned()]
    );
    seed_shard(&mut raw, &prefix, 0).await;
    let (seen, _) = sweep(&store, 2, first.next_cursor).await;
    assert_eq!(
        seen,
        vec![
            "shard-4".to_owned(),
            "shard-5".to_owned(),
            "shard-6".to_owned()
        ]
    );
    drop_prefix(&mut raw, prefix).await;
}
