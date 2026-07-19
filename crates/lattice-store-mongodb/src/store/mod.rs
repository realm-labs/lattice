use std::future::IntoFuture;
use std::time::Duration;

use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::{Client, Database};
use tracing::info;

use crate::error::MongoStoreError;

mod read;
mod write;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoStoreConfig {
    pub uri: String,
    pub database: String,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
}

#[derive(Clone)]
pub struct MongoStore {
    database: Database,
    operation_timeout: Duration,
}

impl MongoStore {
    pub async fn connect(config: MongoStoreConfig) -> Result<Self, MongoStoreError> {
        if config.uri.trim().is_empty() {
            return Err(MongoStoreError::invalid_config("uri", "cannot be empty"));
        }
        if config.database.trim().is_empty() {
            return Err(MongoStoreError::invalid_config(
                "database",
                "cannot be empty",
            ));
        }
        if config.connect_timeout.is_zero() {
            return Err(MongoStoreError::invalid_config(
                "connect_timeout",
                "must be positive",
            ));
        }
        if config.operation_timeout.is_zero() {
            return Err(MongoStoreError::invalid_config(
                "operation_timeout",
                "must be positive",
            ));
        }

        info!(
            database = %config.database,
            uri = %redact_mongo_uri(&config.uri),
            "mongo.connect.start"
        );

        let mut options = ClientOptions::parse(&config.uri)
            .await
            .map_err(store_error("parse mongo uri"))?;
        options.connect_timeout = Some(config.connect_timeout);
        options.server_selection_timeout = Some(config.connect_timeout);
        let client = Client::with_options(options).map_err(store_error("create mongo client"))?;
        let database = client.database(&config.database);
        mongo_timeout(
            config.operation_timeout,
            "ping mongo",
            database.run_command(doc! { "ping": 1 }),
        )
        .await?;

        info!(database = %config.database, "mongo.connect.success");
        Ok(Self {
            database,
            operation_timeout: config.operation_timeout,
        })
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }
}

async fn mongo_timeout<F, T, E>(
    duration: Duration,
    context: &'static str,
    future: F,
) -> Result<T, MongoStoreError>
where
    F: IntoFuture<Output = Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(duration, future.into_future())
        .await
        .map_err(|_| MongoStoreError::timeout(context, duration))?
        .map_err(store_error(context))
}

fn store_error<E>(context: &'static str) -> impl FnOnce(E) -> MongoStoreError
where
    E: std::error::Error + Send + Sync + 'static,
{
    move |error| MongoStoreError::operation(context, error)
}

pub fn redact_mongo_uri(uri: &str) -> String {
    if let Some((scheme, rest)) = uri.split_once("://")
        && let Some((_, host_and_path)) = rest.rsplit_once('@')
    {
        return format!("{scheme}://<redacted>@{host_and_path}");
    }
    uri.to_owned()
}
