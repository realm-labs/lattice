use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use lattice_store_mongodb::document::set::{MongoDocumentCollection, MongoDocumentSet};
use lattice_store_mongodb::document::tracked::Tracked;
use lattice_store_mongodb::document::{
    LoadedDocument, MongoDocument as _, decode_flat_document, encode_flat_document,
};
use lattice_store_mongodb::error::MongoStoreError;
use lattice_store_mongodb::loading::collection::{MongoLazyCollection, MongoUnloadableCollection};
use lattice_store_mongodb::loading::document::{MongoLazyDocument, MongoUnloadableDocument};
use lattice_store_mongodb::loading::table::{MongoLazyTable, MongoTableSpec, MongoUnloadableTable};
use lattice_store_mongodb::persistence::coordinator::{
    ConflictPolicy, MongoPersistenceCoordinator, PersistenceError,
};
use lattice_store_mongodb::scan::{
    FieldChange, MongoMapScanAdapter, MongoScan as _, ScanBudget, ScanCursor, ScanError,
};
use lattice_store_mongodb::store::MongoStore;
use lattice_store_mongodb::{MongoDocument, MongoDocumentSet, MongoScan};
use mongodb::bson::{Bson, doc, to_bson};
use serde::{Deserialize, Serialize};

static MAP_CONTAINER_SERIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static CUSTOM_MAP_SERIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static DEFERRED_VALUE_SERIALIZATIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct CountingMap(BTreeMap<String, i32>);

impl Serialize for CountingMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        MAP_CONTAINER_SERIALIZATIONS.fetch_add(1, Ordering::Relaxed);
        self.0.serialize(serializer)
    }
}

impl<'a> IntoIterator for &'a CountingMap {
    type Item = (&'a String, &'a i32);
    type IntoIter = std::collections::btree_map::Iter<'a, String, i32>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct DeferredValue(i32);

impl Serialize for DeferredValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        DEFERRED_VALUE_SERIALIZATIONS.fetch_add(1, Ordering::Relaxed);
        self.0.serialize(serializer)
    }
}

mod prefixed_string_map {
    use std::collections::BTreeMap;
    use std::sync::atomic::Ordering;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::CUSTOM_MAP_SERIALIZATIONS;

    pub fn serialize<S>(value: &BTreeMap<String, i32>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        CUSTOM_MAP_SERIALIZATIONS.fetch_add(1, Ordering::Relaxed);
        value
            .iter()
            .map(|(key, value)| (format!("key_{key}"), value.to_string()))
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<String, i32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::<String, String>::deserialize(deserializer)?
            .into_iter()
            .map(|(key, value)| {
                let key = key.strip_prefix("key_").ok_or_else(|| {
                    serde::de::Error::custom("custom map key is missing the key_ prefix")
                })?;
                let value = value.parse().map_err(serde::de::Error::custom)?;
                Ok((key.to_owned(), value))
            })
            .collect()
    }
}

struct PrefixedStringMapAdapter;

impl MongoMapScanAdapter<String, i32> for PrefixedStringMapAdapter {
    fn encode_key(key: &String) -> Result<String, ScanError> {
        Ok(format!("key_{key}"))
    }

    fn encode_value(value: &i32) -> Result<Bson, ScanError> {
        Ok(Bson::String(value.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "macro_docs")]
struct MacroDoc {
    #[mongo(id)]
    #[serde(rename = "document_id")]
    id: u64,
    #[serde(rename = "display_name")]
    name: String,
    #[mongo(scan = "map")]
    items: BTreeMap<String, Item>,
    small_map: BTreeMap<String, i32>,
    #[serde(skip)]
    cache: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Item {
    count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "macro_secondary_docs", conflict = "quarantine")]
struct SecondaryDoc {
    #[mongo(id)]
    id: u64,
    enabled: bool,
}

#[test]
fn document_conflict_policy_is_explicit_and_defaults_safe() {
    assert_eq!(
        <MacroDoc as lattice_store_mongodb::document::MongoDocument>::CONFLICT_POLICY,
        ConflictPolicy::BlockCoordinator
    );
    assert_eq!(
        <SecondaryDoc as lattice_store_mongodb::document::MongoDocument>::CONFLICT_POLICY,
        ConflictPolicy::QuarantineDocument
    );
}

mod u64_as_string {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FlattenedFields {
    region_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "serde_shape_docs")]
#[serde(rename_all = "camelCase")]
struct SerdeShapeDoc {
    #[mongo(id)]
    document_id: u64,
    display_name: String,
    #[serde(with = "u64_as_string")]
    score: u64,
    #[serde(flatten)]
    flattened: FlattenedFields,
    #[serde(skip_serializing_if = "Option::is_none")]
    nickname: Option<String>,
    #[mongo(scan = "map")]
    inventory_items: BTreeMap<String, Item>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "path_key_docs")]
struct PathKeyDoc {
    #[mongo(id)]
    id: u64,
    #[serde(with = "lattice_store_mongodb::document::bson_serde::path_key_map")]
    #[mongo(scan = "map")]
    values: HashMap<String, i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "unsafe_path_key_docs")]
struct UnsafePathKeyDoc {
    #[mongo(id)]
    id: u64,
    #[mongo(scan = "map")]
    values: HashMap<String, i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "streaming_map_docs")]
struct StreamingMapDoc {
    #[mongo(id)]
    id: u64,
    #[mongo(scan = "map")]
    values: CountingMap,
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "field_budget_docs")]
struct FieldBudgetDoc {
    #[mongo(id)]
    id: u64,
    first: i32,
    deferred: DeferredValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "custom_map_adapter_docs")]
struct CustomMapAdapterDoc {
    #[mongo(id)]
    id: u64,
    #[serde(with = "prefixed_string_map")]
    #[mongo(scan = "map", adapter = PrefixedStringMapAdapter)]
    values: BTreeMap<String, i32>,
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
fn map_scans_are_explicit_and_respect_serde_overrides() {
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

#[test]
fn scan_uses_the_exact_serde_business_bson_shape() {
    let mut value = SerdeShapeDoc {
        document_id: 42,
        display_name: "old".to_owned(),
        score: 7,
        flattened: FlattenedFields {
            region_code: "eu".to_owned(),
        },
        nickname: Some("first".to_owned()),
        inventory_items: BTreeMap::from([("one".to_owned(), Item { count: 1 })]),
    };
    let encoded = encode_flat_document(&value, 1, 0).expect("serde-shaped document should encode");
    assert_eq!(encoded.get_i64("_id"), Ok(42));
    assert!(!encoded.contains_key("documentId"));
    assert_eq!(encoded.get_str("score"), Ok("7"));
    assert_eq!(encoded.get_str("regionCode"), Ok("eu"));

    let baseline = value
        .capture()
        .expect("serde-shaped baseline should capture");
    value.display_name = "new".to_owned();
    value.score = 8;
    value.flattened.region_code = "us".to_owned();
    value.nickname = None;
    value
        .inventory_items
        .get_mut("one")
        .expect("inventory item exists")
        .count = 2;
    let delta = value
        .diff(
            &baseline,
            ScanCursor::default(),
            &mut ScanBudget::generous(),
        )
        .expect("serde-shaped document should diff");
    let changes = delta
        .changes
        .iter()
        .map(|change| match change {
            FieldChange::Set { path, value } => (path.0.as_str(), Some(value)),
            FieldChange::Unset { path } => (path.0.as_str(), None),
        })
        .collect::<Vec<_>>();
    assert!(changes.iter().any(|(path, value)| {
        *path == "displayName" && *value == Some(&mongodb::bson::Bson::String("new".to_owned()))
    }));
    assert!(changes.iter().any(|(path, value)| {
        *path == "score" && *value == Some(&mongodb::bson::Bson::String("8".to_owned()))
    }));
    assert!(changes.iter().any(|(path, value)| {
        *path == "regionCode" && *value == Some(&mongodb::bson::Bson::String("us".to_owned()))
    }));
    assert!(
        changes
            .iter()
            .any(|(path, value)| *path == "nickname" && value.is_none())
    );
    assert!(
        changes
            .iter()
            .any(|(path, _)| *path == "inventoryItems.one")
    );
    assert!(
        !changes.iter().any(|(path, _)| {
            matches!(*path, "display_name" | "region_code" | "inventory_items")
        })
    );
}

#[test]
fn explicit_map_scan_uses_the_same_path_key_encoding_as_full_documents() {
    let mut value = PathKeyDoc {
        id: 42,
        values: HashMap::from([
            ("a.b".to_owned(), 1),
            ("a%2Eb".to_owned(), 2),
            (String::new(), 3),
        ]),
    };
    let encoded = encode_flat_document(&value, 1, 0).expect("path keys should encode");
    let values = encoded
        .get_document("values")
        .expect("path-key map should be stored as a document");
    assert_eq!(values.get_i32("a%2Eb"), Ok(1));
    assert_eq!(values.get_i32("a%252Eb"), Ok(2));
    assert_eq!(values.get_i32("%EMPTY"), Ok(3));
    assert_eq!(
        decode_flat_document::<PathKeyDoc>(encoded)
            .expect("path-key document should decode")
            .value,
        value,
    );

    let baseline = value.capture().expect("path-key baseline should capture");
    value.values.insert("a.b".to_owned(), 4);
    value.values.remove("");
    let paths = value
        .diff(
            &baseline,
            ScanCursor::default(),
            &mut ScanBudget::generous(),
        )
        .expect("path-key map should diff")
        .changes
        .into_iter()
        .map(|change| match change {
            FieldChange::Set { path, .. } | FieldChange::Unset { path } => path.0,
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, ["values.%EMPTY", "values.a%2Eb"]);
}

#[test]
fn explicit_map_scan_rejects_unencoded_mongodb_path_keys() {
    let value = UnsafePathKeyDoc {
        id: 42,
        values: HashMap::from([("a.b".to_owned(), 1)]),
    };
    let error = value
        .capture()
        .expect_err("unencoded path key must not enter an incremental baseline");
    assert!(matches!(
        error,
        lattice_store_mongodb::scan::ScanError::InvalidMapKey(key) if key == "a.b"
    ));
}

#[test]
fn map_scan_encodes_entries_without_serializing_the_map_container() {
    MAP_CONTAINER_SERIALIZATIONS.store(0, Ordering::Relaxed);
    let mut value = StreamingMapDoc {
        id: 42,
        values: CountingMap(BTreeMap::from([
            ("one".to_owned(), 1),
            ("two".to_owned(), 2),
        ])),
    };
    let baseline = value.capture().expect("streaming map should capture");
    assert_eq!(MAP_CONTAINER_SERIALIZATIONS.load(Ordering::Relaxed), 0);

    value.values.0.insert("one".to_owned(), 10);
    value.values.0.remove("two");
    value.values.0.insert("three".to_owned(), 3);
    let delta = value
        .diff(
            &baseline,
            ScanCursor::default(),
            &mut ScanBudget::generous(),
        )
        .expect("streaming map should diff");

    assert_eq!(MAP_CONTAINER_SERIALIZATIONS.load(Ordering::Relaxed), 0);
    let changes = delta
        .changes
        .into_iter()
        .map(|change| match change {
            FieldChange::Set { path, value } => (path.0, Some(value)),
            FieldChange::Unset { path } => (path.0, None),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        changes,
        [
            ("values.one".to_owned(), Some(10_i32.into())),
            ("values.three".to_owned(), Some(3_i32.into())),
            ("values.two".to_owned(), None),
        ]
    );
}

#[test]
fn custom_map_adapter_matches_custom_serde_without_serializing_the_container() {
    CUSTOM_MAP_SERIALIZATIONS.store(0, Ordering::Relaxed);
    let mut value = CustomMapAdapterDoc {
        id: 42,
        values: BTreeMap::from([("one".to_owned(), 1), ("two".to_owned(), 2)]),
    };

    let baseline = value.capture().expect("adapter baseline should capture");
    assert_eq!(CUSTOM_MAP_SERIALIZATIONS.load(Ordering::Relaxed), 0);

    let Bson::Document(encoded) = to_bson(&value).expect("full document should serialize") else {
        panic!("custom Map document should encode as BSON");
    };
    assert_eq!(
        encoded.get_document("values").expect("encoded Map"),
        &doc! { "key_one": "1", "key_two": "2" }
    );
    assert_eq!(CUSTOM_MAP_SERIALIZATIONS.load(Ordering::Relaxed), 1);

    let loaded_baseline = CustomMapAdapterDoc::capture_bson(&encoded)
        .expect("serialized BSON should produce the same baseline shape");
    value.values.insert("one".to_owned(), 10);
    value.values.remove("two");

    CUSTOM_MAP_SERIALIZATIONS.store(0, Ordering::Relaxed);
    for baseline in [&baseline, &loaded_baseline] {
        let delta = value
            .diff(baseline, ScanCursor::default(), &mut ScanBudget::generous())
            .expect("custom adapter should diff");
        assert_eq!(
            delta.changes,
            [
                FieldChange::Set {
                    path: lattice_store_mongodb::persistence::types::MongoFieldPath::new(
                        "values.key_one",
                    ),
                    value: Bson::String("10".to_owned()),
                },
                FieldChange::Unset {
                    path: lattice_store_mongodb::persistence::types::MongoFieldPath::new(
                        "values.key_two",
                    ),
                },
            ]
        );
    }
    assert_eq!(CUSTOM_MAP_SERIALIZATIONS.load(Ordering::Relaxed), 0);
}

#[test]
fn field_budget_does_not_serialize_deferred_fields() {
    let mut value = FieldBudgetDoc {
        id: 42,
        first: 1,
        deferred: DeferredValue(1),
    };
    let mut baseline = value
        .capture()
        .expect("field-budget document should capture");
    value.first = 2;
    value.deferred = DeferredValue(2);
    DEFERRED_VALUE_SERIALIZATIONS.store(0, Ordering::Relaxed);

    let mut first_budget = ScanBudget::new(1, 1, Duration::from_secs(1));
    let first = value
        .diff(&baseline, ScanCursor::default(), &mut first_budget)
        .expect("first field batch should scan");
    assert!(!first.complete);
    assert_eq!(DEFERRED_VALUE_SERIALIZATIONS.load(Ordering::Relaxed), 0);
    let cursor = baseline
        .apply(first.commit)
        .expect("first field baseline should advance");

    let second = value
        .diff(&baseline, cursor, &mut ScanBudget::generous())
        .expect("deferred field batch should scan");
    assert!(second.complete);
    assert_eq!(DEFERRED_VALUE_SERIALIZATIONS.load(Ordering::Relaxed), 1);
    assert!(
        second.changes.iter().any(|change| {
            matches!(change, FieldChange::Set { path, .. } if path.0 == "deferred")
        })
    );
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
