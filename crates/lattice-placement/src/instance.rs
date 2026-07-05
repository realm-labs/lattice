use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use http::Uri;
use lattice_core::instance::InstanceCapacity;
use lattice_core::{InstanceId, ServiceKind};
use serde::{Deserialize, Serialize};

use crate::PlacementError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceState {
    Starting,
    Ready,
    Draining,
    Stopping,
    Dead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceRecord {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    #[serde(with = "uri_serde")]
    pub advertised_endpoint: Uri,
    #[serde(with = "uri_serde")]
    pub control_endpoint: Uri,
    pub version: String,
    pub state: InstanceState,
    pub capacity: InstanceCapacity,
    pub labels: BTreeMap<String, String>,
}

mod uri_serde {
    use std::str::FromStr;

    use http::Uri;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Uri, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Uri, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Uri::from_str(&value).map_err(serde::de::Error::custom)
    }
}

#[async_trait]
pub trait InstanceRegistry: Clone + Send + Sync + 'static {
    async fn upsert(&self, record: InstanceRecord) -> Result<(), PlacementError>;
    async fn get(&self, instance_id: &InstanceId)
    -> Result<Option<InstanceRecord>, PlacementError>;
    async fn list(&self, service_kind: &ServiceKind)
    -> Result<Vec<InstanceRecord>, PlacementError>;

    async fn list_ready(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        let records = self.list(service_kind).await?;
        Ok(records
            .into_iter()
            .filter(|record| record.state == InstanceState::Ready)
            .collect())
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryInstanceRegistry {
    records: Arc<std::sync::Mutex<HashMap<InstanceId, InstanceRecord>>>,
}

impl InMemoryInstanceRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl InstanceRegistry for InMemoryInstanceRegistry {
    async fn upsert(&self, record: InstanceRecord) -> Result<(), PlacementError> {
        self.records
            .lock()
            .expect("instance registry mutex poisoned")
            .insert(record.instance_id.clone(), record);
        Ok(())
    }

    async fn get(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        Ok(self
            .records
            .lock()
            .expect("instance registry mutex poisoned")
            .get(instance_id)
            .cloned())
    }

    async fn list(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        Ok(self
            .records
            .lock()
            .expect("instance registry mutex poisoned")
            .values()
            .filter(|record| &record.service_kind == service_kind)
            .cloned()
            .collect())
    }
}
