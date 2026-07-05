use lattice_core::InstanceId;

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
