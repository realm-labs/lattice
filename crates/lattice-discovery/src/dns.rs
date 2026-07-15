use std::collections::BTreeMap;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::Stream;
use hickory_resolver::TokioResolver;
use hickory_resolver::proto::rr::{RData, RecordType};
use lattice_core::actor_ref::NodeAddress;
use lattice_core::coordinator::CoordinatorScope;

use crate::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
    DiscoverySource, DiscoveryTarget,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsMode {
    Srv { service: String },
    Host { hostname: String, port: u16 },
}

#[derive(Debug, Clone)]
pub struct DnsDiscoveryConfig {
    pub scope: CoordinatorScope,
    pub mode: DnsMode,
    pub min_refresh: Duration,
    pub max_refresh: Duration,
    pub retry_delay: Duration,
}

impl DnsDiscoveryConfig {
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        let name_valid = match &self.mode {
            DnsMode::Srv { service } => !service.is_empty(),
            DnsMode::Host { hostname, port } => !hostname.is_empty() && *port != 0,
        };
        if !name_valid
            || self.min_refresh.is_zero()
            || self.max_refresh < self.min_refresh
            || self.retry_delay.is_zero()
        {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "DNS names, ports, refresh bounds and retry delay must be valid"
                    .to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct DnsDiscovery {
    resolver: Arc<dyn DnsResolver>,
    config: DnsDiscoveryConfig,
}

impl std::fmt::Debug for DnsDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DnsDiscovery")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DnsDiscovery {
    pub fn from_system(config: DnsDiscoveryConfig) -> Result<Self, DiscoveryError> {
        config.validate()?;
        let resolver = TokioResolver::builder_tokio()
            .map_err(dns_configuration_error)?
            .build()
            .map_err(dns_configuration_error)?;
        Ok(Self {
            resolver: Arc::new(HickoryDnsResolver { resolver }),
            config,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_resolver(
        config: DnsDiscoveryConfig,
        resolver: Arc<dyn DnsResolver>,
    ) -> Result<Self, DiscoveryError> {
        config.validate()?;
        Ok(Self { resolver, config })
    }
}

impl CoordinatorDiscovery for DnsDiscovery {
    fn scope(&self) -> &CoordinatorScope {
        &self.config.scope
    }

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        let scope = self.config.scope.clone();
        Box::pin(async_stream::stream! {
            let mut generation = 0_u64;
            let mut emitted_initial = false;
            loop {
                match resolve_once(self.resolver.as_ref(), &self.config).await {
                    Ok((targets, ttl)) if !targets.is_empty() => {
                        generation += 1;
                        emitted_initial = true;
                        yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation, targets });
                        tokio::time::sleep(ttl.clamp(self.config.min_refresh, self.config.max_refresh)).await;
                    }
                    Ok(_) => {
                        if !emitted_initial {
                            generation += 1;
                            emitted_initial = true;
                            yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation, targets: Vec::new() });
                        }
                        yield Err(provider_error("DNS lookup returned no reachable targets"));
                        tokio::time::sleep(self.config.retry_delay).await;
                    }
                    Err(error) => {
                        if !emitted_initial {
                            generation += 1;
                            emitted_initial = true;
                            yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation, targets: Vec::new() });
                        }
                        yield Err(error);
                        tokio::time::sleep(self.config.retry_delay).await;
                    }
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DnsLookup<T> {
    pub(crate) records: Vec<T>,
    pub(crate) ttl: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct SrvRecord {
    pub(crate) target: String,
    pub(crate) port: u16,
    pub(crate) priority: u16,
    pub(crate) weight: u16,
}

#[async_trait]
pub(crate) trait DnsResolver: Send + Sync {
    async fn lookup_ip(&self, hostname: &str) -> Result<DnsLookup<IpAddr>, String>;
    async fn lookup_srv(&self, service: &str) -> Result<DnsLookup<SrvRecord>, String>;
}

struct HickoryDnsResolver {
    resolver: TokioResolver,
}

#[async_trait]
impl DnsResolver for HickoryDnsResolver {
    async fn lookup_ip(&self, hostname: &str) -> Result<DnsLookup<IpAddr>, String> {
        let lookup = self
            .resolver
            .lookup_ip(hostname)
            .await
            .map_err(|error| error.to_string())?;
        Ok(DnsLookup {
            records: lookup.iter().collect(),
            ttl: remaining_ttl(lookup.valid_until()),
        })
    }

    async fn lookup_srv(&self, service: &str) -> Result<DnsLookup<SrvRecord>, String> {
        let lookup = self
            .resolver
            .lookup(service, RecordType::SRV)
            .await
            .map_err(|error| error.to_string())?;
        let records = lookup
            .answers()
            .iter()
            .filter_map(|record| match &record.data {
                RData::SRV(srv) if srv.port != 0 && srv.target.to_utf8() != "." => {
                    Some(SrvRecord {
                        target: srv.target.to_utf8().trim_end_matches('.').to_string(),
                        port: srv.port,
                        priority: srv.priority,
                        weight: srv.weight,
                    })
                }
                _ => None,
            })
            .collect();
        Ok(DnsLookup {
            records,
            ttl: remaining_ttl(lookup.valid_until()),
        })
    }
}

async fn resolve_once(
    resolver: &dyn DnsResolver,
    config: &DnsDiscoveryConfig,
) -> Result<(Vec<DiscoveryTarget>, Duration), DiscoveryError> {
    let mut targets = BTreeMap::<NodeAddress, DiscoveryTarget>::new();
    let mut ttl = config.max_refresh;
    match &config.mode {
        DnsMode::Host { hostname, port } => {
            let lookup = resolver.lookup_ip(hostname).await.map_err(provider_error)?;
            ttl = ttl.min(lookup.ttl);
            for ip in lookup.records {
                insert_dns_target(&mut targets, ip, *port, 0, hostname, hostname, 0)?;
            }
        }
        DnsMode::Srv { service } => {
            let lookup = resolver.lookup_srv(service).await.map_err(provider_error)?;
            ttl = ttl.min(lookup.ttl);
            for record in lookup.records {
                let addresses = resolver
                    .lookup_ip(&record.target)
                    .await
                    .map_err(provider_error)?;
                ttl = ttl.min(addresses.ttl);
                for ip in addresses.records {
                    insert_dns_target(
                        &mut targets,
                        ip,
                        record.port,
                        record.priority,
                        service,
                        &record.target,
                        record.weight,
                    )?;
                }
            }
        }
    }
    Ok((targets.into_values().collect(), ttl))
}

fn insert_dns_target(
    targets: &mut BTreeMap<NodeAddress, DiscoveryTarget>,
    ip: IpAddr,
    port: u16,
    priority: u16,
    query: &str,
    server_name: &str,
    weight: u16,
) -> Result<(), DiscoveryError> {
    let address = NodeAddress::new(ip.to_string(), port)
        .map_err(|error| provider_error(error.to_string()))?;
    let source = DiscoverySource::single(DiscoveryOrigin::Dns {
        query: query.to_string(),
        server_name: server_name.to_string(),
        weight,
    });
    match targets.get_mut(&address) {
        Some(current) => {
            current.priority = current.priority.min(priority);
            current.source.merge(&source);
        }
        None => {
            targets.insert(
                address.clone(),
                DiscoveryTarget {
                    address,
                    expected_node_id: None,
                    source,
                    priority,
                },
            );
        }
    }
    Ok(())
}

fn remaining_ttl(valid_until: Instant) -> Duration {
    valid_until.saturating_duration_since(Instant::now())
}

fn dns_configuration_error(error: impl std::fmt::Display) -> DiscoveryError {
    DiscoveryError::InvalidConfiguration {
        message: format!("cannot initialize system DNS resolver: {error}"),
    }
}

fn provider_error(error: impl std::fmt::Display) -> DiscoveryError {
    DiscoveryError::Provider {
        provider: "dns",
        message: error.to_string(),
    }
}
