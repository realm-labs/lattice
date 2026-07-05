use async_trait::async_trait;
use etcd_client::{Client, EventType};
use lattice_config::ConfigStoreError;
use tokio::sync::watch;

use crate::codec::etcd_error;

#[async_trait]
pub(crate) trait EtcdConfigClient: Clone + Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigStoreError>;
    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), ConfigStoreError>;
    async fn watch(&self, key: &str) -> Result<watch::Receiver<Option<Vec<u8>>>, ConfigStoreError>;
}

#[derive(Clone)]
pub(crate) struct RealEtcdConfigClient {
    client: Client,
}

impl RealEtcdConfigClient {
    pub(crate) async fn connect(endpoints: Vec<String>) -> Result<Self, ConfigStoreError> {
        let client = Client::connect(endpoints, None).await.map_err(etcd_error)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl EtcdConfigClient for RealEtcdConfigClient {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigStoreError> {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(etcd_error)?;
        Ok(response.kvs().first().map(|kv| kv.value().to_vec()))
    }

    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), ConfigStoreError> {
        let mut client = self.client.clone();
        client.put(key, value, None).await.map_err(etcd_error)?;
        Ok(())
    }

    async fn watch(&self, key: &str) -> Result<watch::Receiver<Option<Vec<u8>>>, ConfigStoreError> {
        let initial = self.get(key).await?;
        let (tx, rx) = watch::channel(initial);
        let watch_key = key.to_string();
        let mut client = self.client.clone();
        let mut stream = client.watch(watch_key, None).await.map_err(etcd_error)?;

        tokio::spawn(async move {
            while let Ok(Some(response)) = stream.message().await {
                for event in response.events() {
                    let value = match event.event_type() {
                        EventType::Put => event.kv().map(|kv| kv.value().to_vec()),
                        EventType::Delete => None,
                    };
                    tx.send_replace(value);
                }
            }
        });

        Ok(rx)
    }
}
