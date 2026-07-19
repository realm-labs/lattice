use std::collections::{BTreeMap, HashMap, HashSet};

use lattice_store_mongodb::coordinator::{MongoPersistenceCoordinator, PersistenceError};
use lattice_store_mongodb::document::{
    LoadedDocument, MongoDocument as _, decode_flat_document, encode_flat_document,
};
use lattice_store_mongodb::mongo_store::MongoStore;
use lattice_store_mongodb::scan::{FieldChange, MongoScan as _, ScanBudget, ScanCursor};
use lattice_store_mongodb::tracked::Tracked;
use lattice_store_mongodb::{
    MongoDocument, MongoDocumentCollection, MongoDocumentSet, MongoLazyCollection,
    MongoLazyDocument, MongoLazyTable, MongoScan, MongoStoreError, MongoTableSpec,
    MongoUnloadableCollection, MongoUnloadableDocument, MongoUnloadableTable,
};
use mongodb::bson::{doc, to_bson};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "macro_docs")]
struct MacroDoc {
    #[mongo(id)]
    #[serde(rename = "document_id")]
    id: u64,
    #[serde(rename = "display_name")]
    name: String,
    items: BTreeMap<String, Item>,
    #[mongo(scan = "whole")]
    small_map: BTreeMap<String, i32>,
    #[serde(skip)]
    cache: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Item {
    count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "macro_secondary_docs")]
struct SecondaryDoc {
    #[mongo(id)]
    id: u64,
    enabled: bool,
}

#[derive(Debug, MongoDocumentSet)]
#[mongo(id = u64)]
struct MacroDocuments {
    primary: Tracked<MacroDoc>,
    secondary: Tracked<SecondaryDoc>,
    #[mongo(skip)]
    transient_counter: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct WorldMemberId {
    world_id: u64,
    member_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "macro_world_members")]
struct WorldMemberDocument {
    #[mongo(id)]
    id: WorldMemberId,
    alliance_id: u64,
    contribution: i64,
}

#[derive(Debug)]
struct AllianceMembers {
    by_member: HashMap<u64, Tracked<WorldMemberDocument>>,
    members_by_alliance: HashMap<u64, HashSet<u64>>,
    alliance_by_member: HashMap<u64, u64>,
}

impl MongoDocumentCollection<u64> for AllianceMembers {
    type Document = WorldMemberDocument;

    fn load_filter(owner_id: &u64) -> Result<mongodb::bson::Document, MongoStoreError> {
        Ok(doc! {
            "_id.world_id": to_bson(owner_id)
                .map_err(|error| MongoStoreError::encode("encode world owner ID", error))?,
        })
    }

    fn owner_id(document: &Self::Document) -> &u64 {
        &document.id.world_id
    }

    fn from_documents(documents: Vec<Tracked<Self::Document>>) -> Result<Self, PersistenceError> {
        let mut by_member = HashMap::new();
        let mut members_by_alliance = HashMap::<u64, HashSet<u64>>::new();
        let mut alliance_by_member = HashMap::new();
        for document in documents {
            let member_id = document.id.member_id;
            let alliance_id = document.alliance_id;
            by_member.insert(member_id, document);
            members_by_alliance
                .entry(alliance_id)
                .or_default()
                .insert(member_id);
            alliance_by_member.insert(member_id, alliance_id);
        }
        Ok(Self {
            by_member,
            members_by_alliance,
            alliance_by_member,
        })
    }

    fn documents(&self) -> impl Iterator<Item = &Tracked<Self::Document>> {
        self.by_member.values()
    }
}

#[derive(Debug, MongoDocumentSet)]
#[mongo(id = u64)]
struct WorldDocuments {
    primary: Tracked<MacroDoc>,
    #[mongo(many)]
    alliance_members: AllianceMembers,
    #[mongo(skip)]
    transient_counter: usize,
}

struct WorldMemberTable;

impl MongoTableSpec<u64> for WorldMemberTable {
    type Key = u64;
    type Document = WorldMemberDocument;

    const PAGE_KEY_FIELD: &'static str = "_id.member_id";

    fn document_id(owner_id: &u64, key: &Self::Key) -> WorldMemberId {
        WorldMemberId {
            world_id: *owner_id,
            member_id: *key,
        }
    }

    fn owner_id(document: &Self::Document) -> &u64 {
        &document.id.world_id
    }

    fn key(document: &Self::Document) -> &Self::Key {
        &document.id.member_id
    }

    fn owner_filter(owner_id: &u64) -> Result<mongodb::bson::Document, MongoStoreError> {
        Ok(doc! {
            "_id.world_id": to_bson(owner_id)
                .map_err(|error| MongoStoreError::encode("encode world owner ID", error))?,
        })
    }
}

#[derive(MongoDocumentSet)]
#[mongo(id = u64)]
struct MixedLoadingDocuments {
    eager: Tracked<MacroDoc>,
    #[mongo(lazy)]
    lazy_singleton: MongoLazyDocument<SecondaryDoc>,
    #[mongo(lazy_unload = "10m")]
    unloadable_singleton: MongoUnloadableDocument<SecondaryDoc>,
    #[mongo(lazy)]
    lazy_collection: MongoLazyCollection<u64, AllianceMembers>,
    #[mongo(lazy_unload = "1h")]
    unloadable_collection: MongoUnloadableCollection<u64, AllianceMembers>,
    #[mongo(lazy)]
    lazy_rows: MongoLazyTable<u64, WorldMemberTable>,
    #[mongo(lazy_unload = "30s")]
    unloadable_rows: MongoUnloadableTable<u64, WorldMemberTable>,
}

#[test]
fn derives_typed_document_identity_from_the_annotated_entity_field() {
    assert_eq!(MacroDoc::COLLECTION, "macro_docs");
    let _: <MacroDoc as lattice_store_mongodb::document::MongoDocument>::Id = 42_u64;
    let document = MacroDoc {
        id: 42,
        name: String::new(),
        items: BTreeMap::new(),
        small_map: BTreeMap::new(),
        cache: 0,
    };
    assert_eq!(document.id(), &42);
}

#[test]
fn document_identity_field_maps_to_mongo_id_and_round_trips_into_the_entity() {
    let value = MacroDoc {
        id: 42,
        name: "Ada".to_owned(),
        items: BTreeMap::new(),
        small_map: BTreeMap::new(),
        cache: 0,
    };
    let encoded = encode_flat_document(&value, 3, 7).expect("entity should encode");
    assert_eq!(encoded.get_i64("_id"), Ok(42));
    assert!(!encoded.contains_key("document_id"));

    let loaded = decode_flat_document::<MacroDoc>(encoded).expect("entity should decode");
    assert_eq!(loaded.id(), &42);
    assert_eq!(loaded.value.name, "Ada");
    assert_eq!(loaded.version, 3);
    assert_eq!(loaded.updated_at_ms, 7);
}

#[test]
fn scan_infers_maps_and_respects_serde_and_exception_overrides() {
    let mut value = MacroDoc {
        id: 42,
        name: "old".to_owned(),
        items: BTreeMap::from([("1".to_owned(), Item { count: 1 })]),
        small_map: BTreeMap::from([("a".to_owned(), 1)]),
        cache: 1,
    };
    let baseline = value.capture().expect("macro document should capture");
    value.name = "new".to_owned();
    value
        .items
        .get_mut("1")
        .expect("fixture item should exist")
        .count = 2;
    value.small_map.insert("b".to_owned(), 2);
    value.cache = 99;

    let delta = value
        .diff(
            &baseline,
            ScanCursor::default(),
            &mut ScanBudget::generous(),
        )
        .expect("macro document should diff");
    let paths = delta
        .changes
        .iter()
        .map(|change| match change {
            FieldChange::Set { path, .. } | FieldChange::Unset { path } => path.0.as_str(),
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, ["display_name", "items.1", "small_map"]);
}

fn loaded_documents(id: u64) -> LoadedMacroDocuments {
    LoadedMacroDocuments {
        primary: LoadedDocument {
            version: 3,
            updated_at_ms: 11,
            value: MacroDoc {
                id,
                name: "old".to_owned(),
                items: BTreeMap::new(),
                small_map: BTreeMap::new(),
                cache: 0,
            },
        },
        secondary: LoadedDocument {
            version: 7,
            updated_at_ms: 13,
            value: SecondaryDoc { id, enabled: true },
        },
    }
}

#[test]
fn document_set_derives_loading_registration_and_tracked_scanning() {
    assert_eq!(MacroDocuments::DOCUMENT_COUNT, 2);
    let mut coordinator = MongoPersistenceCoordinator::new(9);
    let mut documents = coordinator
        .attach_loaded_set::<MacroDocuments>(&42, loaded_documents(42))
        .expect("document set should attach");
    assert_eq!(documents.primary.name, "old");
    assert!(documents.secondary.enabled);
    assert_eq!(documents.transient_counter, 0);

    let unchanged = coordinator
        .prepare_set(ScanBudget::generous(), &documents)
        .expect("unchanged set should prepare");
    assert!(unchanged.request.is_none());

    documents.primary.write().name = "new".to_owned();
    let changed = coordinator
        .prepare_set(ScanBudget::generous(), &documents)
        .expect("changed set should prepare");
    let request = changed.request.expect("changed set should write");
    assert_eq!(request.writes.len(), 1);
    assert_eq!(request.writes[0].key.collection, "macro_docs");
}

#[test]
fn document_set_keeps_lazy_fields_out_of_the_startup_loaded_shape() {
    assert_eq!(MixedLoadingDocuments::DOCUMENT_COUNT, 7);
    let mut coordinator = MongoPersistenceCoordinator::new(9);
    let mut documents = coordinator
        .attach_loaded_set::<MixedLoadingDocuments>(
            &42,
            LoadedMixedLoadingDocuments {
                eager: LoadedDocument {
                    version: 3,
                    updated_at_ms: 11,
                    value: MacroDoc {
                        id: 42,
                        name: "Ada".to_owned(),
                        items: BTreeMap::new(),
                        small_map: BTreeMap::new(),
                        cache: 0,
                    },
                },
            },
        )
        .expect("mixed document set should attach its eager subset");

    assert_eq!(documents.eager.name, "Ada");
    assert!(!documents.lazy_singleton.is_loaded());
    assert!(!documents.unloadable_singleton.is_loaded());
    assert!(!documents.lazy_collection.is_loaded());
    assert!(!documents.unloadable_collection.is_loaded());
    assert_eq!(documents.lazy_rows.loaded_len(), 0);
    assert_eq!(documents.unloadable_rows.loaded_len(), 0);

    let unchanged = coordinator
        .prepare_set(ScanBudget::generous(), &documents)
        .expect("unloaded lazy fields should be ignored by scanning");
    assert!(unchanged.request.is_none());

    documents.eager.write().name = "Grace".to_owned();
    let changed = coordinator
        .prepare_set(ScanBudget::generous(), &documents)
        .expect("eager field should retain synchronous tracked semantics");
    assert_eq!(
        changed.request.expect("eager field changed").writes.len(),
        1
    );
}

#[allow(dead_code)]
async fn lazy_access_returns_direct_references<'a>(
    documents: &'a mut MixedLoadingDocuments,
    store: &MongoStore,
    coordinator: &mut MongoPersistenceCoordinator,
) -> Result<&'a mut SecondaryDoc, PersistenceError> {
    documents.lazy_singleton.get_mut(store, coordinator).await
}

#[test]
fn document_set_rejects_foreign_ids_before_registering_any_document() {
    let mut coordinator = MongoPersistenceCoordinator::new(10);
    let error = coordinator
        .attach_loaded_set::<MacroDocuments>(&42, loaded_documents(7))
        .expect_err("foreign IDs must be rejected");
    assert!(matches!(
        error,
        PersistenceError::DocumentIdMismatch {
            expected,
            actual,
            ..
        } if expected == "42" && actual == "7"
    ));

    coordinator
        .attach_loaded_set::<MacroDocuments>(&42, loaded_documents(42))
        .expect("ID validation must happen before registration");
}

fn loaded_world_member(
    world_id: u64,
    member_id: u64,
    alliance_id: u64,
) -> LoadedDocument<WorldMemberDocument> {
    LoadedDocument {
        version: 2,
        updated_at_ms: 17,
        value: WorldMemberDocument {
            id: WorldMemberId {
                world_id,
                member_id,
            },
            alliance_id,
            contribution: 10,
        },
    }
}

#[test]
fn document_set_builds_and_scans_runtime_sized_collections() {
    assert_eq!(WorldDocuments::DOCUMENT_COUNT, 2);
    assert_eq!(
        AllianceMembers::load_filter(&42).expect("filter should encode"),
        doc! { "_id.world_id": 42_i64 },
    );

    let mut coordinator = MongoPersistenceCoordinator::new(11);
    let mut documents = coordinator
        .attach_loaded_set::<WorldDocuments>(
            &42,
            LoadedWorldDocuments {
                primary: loaded_documents(42).primary,
                alliance_members: vec![
                    loaded_world_member(42, 1, 7),
                    loaded_world_member(42, 2, 7),
                    loaded_world_member(42, 3, 8),
                ],
            },
        )
        .expect("singleton and collection documents should attach");

    assert_eq!(documents.alliance_members.by_member.len(), 3);
    assert_eq!(
        documents
            .alliance_members
            .members_by_alliance
            .get(&7)
            .expect("alliance index should exist"),
        &HashSet::from([1, 2]),
    );
    assert_eq!(
        documents.alliance_members.alliance_by_member.get(&3),
        Some(&8),
    );
    assert_eq!(documents.transient_counter, 0);

    let unchanged = coordinator
        .prepare_set(ScanBudget::generous(), &documents)
        .expect("unchanged collection should prepare");
    assert!(unchanged.request.is_none());

    documents
        .alliance_members
        .by_member
        .get_mut(&2)
        .expect("member should exist")
        .write()
        .contribution = 99;
    let changed = coordinator
        .prepare_set(ScanBudget::generous(), &documents)
        .expect("changed collection should prepare");
    let request = changed.request.expect("member change should write");
    assert_eq!(request.writes.len(), 1);
    assert_eq!(request.writes[0].key.collection, "macro_world_members");
}

#[test]
fn document_set_rejects_foreign_collection_owners_before_registration() {
    let mut coordinator = MongoPersistenceCoordinator::new(12);
    let error = coordinator
        .attach_loaded_set::<WorldDocuments>(
            &42,
            LoadedWorldDocuments {
                primary: loaded_documents(42).primary,
                alliance_members: vec![loaded_world_member(7, 1, 9)],
            },
        )
        .expect_err("foreign collection owner must be rejected");
    assert!(matches!(
        error,
        PersistenceError::DocumentIdMismatch {
            expected,
            actual,
            ..
        } if expected == "42" && actual == "7"
    ));

    coordinator
        .attach_loaded_set::<WorldDocuments>(
            &42,
            LoadedWorldDocuments {
                primary: loaded_documents(42).primary,
                alliance_members: vec![loaded_world_member(42, 1, 9)],
            },
        )
        .expect("owner validation must happen before any registration");
}
