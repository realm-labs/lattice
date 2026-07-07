pub mod cache;
pub mod control;
pub mod coordinator;
pub mod endpoint;
pub mod error;
pub mod etcd;
pub mod instance;
pub mod route;
pub mod singleton;
pub mod static_resolver;
pub mod store;
pub mod vshard;

#[cfg(test)]
mod tests;
