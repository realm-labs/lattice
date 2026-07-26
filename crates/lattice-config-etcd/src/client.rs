use async_trait::async_trait;
use etcd_client::{Client, ConnectOptions, EventType, ResponseHeader, WatchOptions, WatchStream};
use lattice_config::store::ConfigStoreError;

use crate::codec::{backend_error, etcd_error};
use crate::watch::{
    EtcdSnapshot, EtcdWatchBackend, EtcdWatchSession, RawConfigWatch, WatchRetryPolicy,
    spawn_config_watch,
};

#[async_trait]
pub(crate) trait EtcdConfigClient: Clone + Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigStoreError>;
    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), ConfigStoreError>;
    async fn watch(&self, key: &str) -> Result<RawConfigWatch, ConfigStoreError>;
}

#[derive(Clone)]
pub(crate) struct RealEtcdConfigClient {
    client: Client,
    retry: WatchRetryPolicy,
}

impl RealEtcdConfigClient {
    pub(crate) async fn connect(
        endpoints: Vec<String>,
        options: Option<ConnectOptions>,
    ) -> Result<Self, ConfigStoreError> {
        let client = Client::connect(endpoints, options)
            .await
            .map_err(etcd_error)?;
        Ok(Self {
            client,
            retry: WatchRetryPolicy::default(),
        })
    }
}

#[async_trait]
impl EtcdConfigClient for RealEtcdConfigClient {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigStoreError> {
        Ok(self.snapshot(key).await?.value)
    }

    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), ConfigStoreError> {
        let mut client = self.client.clone();
        client.put(key, value, None).await.map_err(etcd_error)?;
        Ok(())
    }

    async fn watch(&self, key: &str) -> Result<RawConfigWatch, ConfigStoreError> {
        let snapshot = self.snapshot(key).await?;
        Ok(spawn_config_watch(
            self.clone(),
            key.to_string(),
            snapshot,
            self.retry,
        ))
    }
}

#[async_trait]
impl EtcdWatchBackend for RealEtcdConfigClient {
    type Session = RealEtcdWatchSession;

    async fn snapshot(&self, key: &str) -> Result<EtcdSnapshot, ConfigStoreError> {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(etcd_error)?;
        let revision = response
            .header()
            .map(ResponseHeader::revision)
            .unwrap_or_default();
        Ok(EtcdSnapshot {
            value: response.kvs().first().map(|kv| kv.value().to_vec()),
            revision,
        })
    }

    async fn watch_from(
        &self,
        key: &str,
        start_revision: i64,
    ) -> Result<Self::Session, ConfigStoreError> {
        let mut client = self.client.clone();
        let stream = client
            .watch(
                key,
                Some(WatchOptions::new().with_start_revision(start_revision)),
            )
            .await
            .map_err(etcd_error)?;
        Ok(RealEtcdWatchSession {
            stream,
            watch_id: None,
        })
    }
}

pub(crate) struct RealEtcdWatchSession {
    stream: WatchStream,
    watch_id: Option<i64>,
}

#[async_trait]
impl EtcdWatchSession for RealEtcdWatchSession {
    async fn next(&mut self) -> Result<Vec<Option<Vec<u8>>>, ConfigStoreError> {
        let Some(response) = self.stream.message().await.map_err(etcd_error)? else {
            return Err(backend_error("etcd closed the config watch stream"));
        };
        if response.created() {
            self.watch_id = Some(response.watch_id());
        }
        let compact_revision = response.compact_revision();
        if compact_revision > 0 {
            self.watch_id = None;
            return Err(backend_error(format_args!(
                "etcd compacted the config watch below revision {compact_revision}"
            )));
        }
        if response.canceled() {
            self.watch_id = None;
            return Err(backend_error(format_args!(
                "etcd canceled the config watch: {}",
                response.cancel_reason()
            )));
        }
        Ok(response
            .events()
            .iter()
            .map(|event| match event.event_type() {
                EventType::Put => event.kv().map(|kv| kv.value().to_vec()),
                EventType::Delete => None,
            })
            .collect())
    }

    async fn cancel(&mut self) {
        let Some(watch_id) = self.watch_id.take() else {
            return;
        };
        if let Err(error) = self.stream.cancel(watch_id).await {
            tracing::debug!(
                target: crate::watch::WATCH_TARGET,
                watch_id,
                error = %error,
                "cancelling the etcd config watcher failed",
            );
        }
    }
}
