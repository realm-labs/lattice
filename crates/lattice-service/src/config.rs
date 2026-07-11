use std::net::IpAddr;
use std::time::Duration;

use lattice_core::direct_link::target::DirectLinkEndpoint;
use lattice_core::instance::{InstanceId, InstanceIncarnation};
use lattice_direct_link::transport::DirectLinkListenConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceConfig {
    pub instance_id: InstanceId,
    pub incarnation: InstanceIncarnation,
    pub advertised_endpoint: Option<http::Uri>,
}

impl InstanceConfig {
    pub fn new(instance_id: InstanceId) -> Self {
        Self {
            instance_id,
            incarnation: InstanceIncarnation::generate(),
            advertised_endpoint: None,
        }
    }

    pub fn with_incarnation(mut self, incarnation: InstanceIncarnation) -> Self {
        self.incarnation = incarnation;
        self
    }

    pub fn with_advertised_endpoint(mut self, endpoint: http::Uri) -> Self {
        self.advertised_endpoint = Some(endpoint);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectLinkConfig {
    bind_endpoint: String,
    max_frame_size: usize,
    maintenance_interval: Duration,
    bind_policy: DirectLinkBindPolicy,
    max_connections: Option<usize>,
    max_active_links: Option<usize>,
    max_open_links_per_second: Option<usize>,
    max_messages_per_second: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectLinkBindPolicy {
    LoopbackOnly,
    External,
}

impl DirectLinkConfig {
    pub fn enabled(bind_endpoint: impl Into<String>) -> Self {
        Self {
            bind_endpoint: bind_endpoint.into(),
            max_frame_size: 256 * 1024,
            maintenance_interval: Duration::from_secs(1),
            bind_policy: DirectLinkBindPolicy::LoopbackOnly,
            max_connections: None,
            max_active_links: None,
            max_open_links_per_second: None,
            max_messages_per_second: None,
        }
    }

    pub fn max_frame_size(mut self, max_frame_size: usize) -> Self {
        if max_frame_size > 0 {
            self.max_frame_size = max_frame_size;
        }
        self
    }

    pub fn maintenance_interval(mut self, interval: Duration) -> Self {
        if !interval.is_zero() {
            self.maintenance_interval = interval;
        }
        self
    }

    pub fn bind_policy(mut self, policy: DirectLinkBindPolicy) -> Self {
        self.bind_policy = policy;
        self
    }

    pub fn max_connections(mut self, max_connections: usize) -> Self {
        if max_connections > 0 {
            self.max_connections = Some(max_connections);
        }
        self
    }

    pub fn max_active_links(mut self, max_active_links: usize) -> Self {
        if max_active_links > 0 {
            self.max_active_links = Some(max_active_links);
        }
        self
    }

    pub fn max_open_links_per_second(mut self, max_open_links: usize) -> Self {
        if max_open_links > 0 {
            self.max_open_links_per_second = Some(max_open_links);
        }
        self
    }

    pub fn max_messages_per_second(mut self, max_messages: usize) -> Self {
        if max_messages > 0 {
            self.max_messages_per_second = Some(max_messages);
        }
        self
    }

    pub(crate) fn maintenance_interval_config(&self) -> Duration {
        self.maintenance_interval
    }

    pub(crate) fn max_connections_config(&self) -> Option<usize> {
        self.max_connections
    }

    pub(crate) fn max_active_links_config(&self) -> Option<usize> {
        self.max_active_links
    }

    pub(crate) fn max_open_links_per_second_config(&self) -> Option<usize> {
        self.max_open_links_per_second
    }

    pub(crate) fn max_messages_per_second_config(&self) -> Option<usize> {
        self.max_messages_per_second
    }

    pub(crate) fn listen_config(&self) -> Result<DirectLinkListenConfig, String> {
        let endpoint = if self.bind_endpoint.contains("://") {
            self.bind_endpoint.clone()
        } else {
            format!("tcp://{}", self.bind_endpoint)
        };
        let uri = endpoint
            .parse()
            .map_err(|error| format!("invalid direct-link bind endpoint {endpoint}: {error}"))?;
        validate_bind_policy(&uri, self.bind_policy)?;
        Ok(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new(uri),
            max_frame_size: self.max_frame_size,
        })
    }
}

fn validate_bind_policy(uri: &http::Uri, policy: DirectLinkBindPolicy) -> Result<(), String> {
    if policy == DirectLinkBindPolicy::External {
        return Ok(());
    }

    let host = uri
        .host()
        .ok_or_else(|| format!("direct-link bind endpoint {uri} has no host"))?;
    if host.eq_ignore_ascii_case("localhost") {
        return Ok(());
    }
    let host = host.trim_matches(['[', ']']);
    let address: IpAddr = host.parse().map_err(|_| {
        format!(
            "direct-link bind endpoint {uri} is not loopback; call bind_policy(DirectLinkBindPolicy::External) to allow external binds"
        )
    })?;
    if address.is_loopback() {
        Ok(())
    } else {
        Err(format!(
            "direct-link bind endpoint {uri} is not loopback; call bind_policy(DirectLinkBindPolicy::External) to allow external binds"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_config_generates_a_new_boot_incarnation_and_allows_explicit_certificate_binding() {
        let first = InstanceConfig::new(InstanceId::new("world-a"));
        let second = InstanceConfig::new(InstanceId::new("world-a"));
        assert_ne!(first.incarnation, second.incarnation);
        assert_eq!(first.incarnation.as_str().len(), 32);

        let explicit = InstanceIncarnation::new("certificate-boot-a");
        assert_eq!(
            InstanceConfig::new(InstanceId::new("world-a"))
                .with_incarnation(explicit.clone())
                .incarnation,
            explicit
        );
    }

    #[test]
    fn direct_link_bind_policy_allows_loopback_by_default() {
        for endpoint in ["127.0.0.1:0", "tcp://localhost:0", "tcp://[::1]:0"] {
            assert!(
                DirectLinkConfig::enabled(endpoint).listen_config().is_ok(),
                "{endpoint} should be accepted by loopback policy"
            );
        }
    }

    #[test]
    fn direct_link_bind_policy_rejects_external_binds_without_opt_in() {
        for endpoint in ["0.0.0.0:0", "192.0.2.10:9000", "tcp://[::]:0"] {
            let error = DirectLinkConfig::enabled(endpoint)
                .listen_config()
                .unwrap_err();
            assert!(
                error.contains("DirectLinkBindPolicy::External"),
                "{endpoint} produced unexpected error: {error}"
            );
        }
    }

    #[test]
    fn direct_link_bind_policy_allows_external_binds_with_explicit_opt_in() {
        assert!(
            DirectLinkConfig::enabled("0.0.0.0:0")
                .bind_policy(DirectLinkBindPolicy::External)
                .listen_config()
                .is_ok()
        );
    }

    #[test]
    fn direct_link_connection_limit_ignores_zero_and_records_positive_values() {
        assert_eq!(
            DirectLinkConfig::enabled("127.0.0.1:0")
                .max_connections(0)
                .max_connections_config(),
            None
        );
        assert_eq!(
            DirectLinkConfig::enabled("127.0.0.1:0")
                .max_connections(8)
                .max_connections_config(),
            Some(8)
        );
    }

    #[test]
    fn direct_link_active_link_limit_ignores_zero_and_records_positive_values() {
        assert_eq!(
            DirectLinkConfig::enabled("127.0.0.1:0")
                .max_active_links(0)
                .max_active_links_config(),
            None
        );
        assert_eq!(
            DirectLinkConfig::enabled("127.0.0.1:0")
                .max_active_links(4)
                .max_active_links_config(),
            Some(4)
        );
    }

    #[test]
    fn direct_link_rate_limits_ignore_zero_and_record_positive_values() {
        let disabled = DirectLinkConfig::enabled("127.0.0.1:0")
            .max_open_links_per_second(0)
            .max_messages_per_second(0);
        assert_eq!(disabled.max_open_links_per_second_config(), None);
        assert_eq!(disabled.max_messages_per_second_config(), None);

        let enabled = DirectLinkConfig::enabled("127.0.0.1:0")
            .max_open_links_per_second(2)
            .max_messages_per_second(64);
        assert_eq!(enabled.max_open_links_per_second_config(), Some(2));
        assert_eq!(enabled.max_messages_per_second_config(), Some(64));
    }
}
