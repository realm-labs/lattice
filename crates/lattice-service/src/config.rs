use lattice_core::{DirectLinkEndpoint, InstanceId};
use lattice_direct_link::DirectLinkListenConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceConfig {
    pub instance_id: InstanceId,
    pub advertised_endpoint: Option<http::Uri>,
}

impl InstanceConfig {
    pub fn new(instance_id: InstanceId) -> Self {
        Self {
            instance_id,
            advertised_endpoint: None,
        }
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
}

impl DirectLinkConfig {
    pub fn enabled(bind_endpoint: impl Into<String>) -> Self {
        Self {
            bind_endpoint: bind_endpoint.into(),
            max_frame_size: 256 * 1024,
        }
    }

    pub fn max_frame_size(mut self, max_frame_size: usize) -> Self {
        if max_frame_size > 0 {
            self.max_frame_size = max_frame_size;
        }
        self
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
        Ok(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new(uri),
            max_frame_size: self.max_frame_size,
        })
    }
}
